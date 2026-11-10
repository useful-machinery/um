//! `um workflow run list` (REQ-LOCALRUN-188): every run under the `um` home, read without
//! taking a run lock or changing a directory.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::path::Path;

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
use serde_json::Value;
use um_execution::{
    ListedRun, LocalRunDirectoryError, RunState, WaitRecord, WorkflowHome, WorkflowRunRecord,
    list_runs, normalize_terminal_path, normalize_terminal_scalar, position_path, read_definition,
};

use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "List the local workflow runs under the um home";
const COMMAND: &str = "um workflow run list";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, help = "List only runs that wait")]
    waiting: bool,

    #[arg(
        long,
        help = "List only live runs behind their definition's current revision"
    )]
    drift: bool,

    #[arg(
        long,
        value_name = "OWNER",
        help = "List only the runs of OWNER; `me` is the invoking user"
    )]
    owner: Option<String>,

    #[arg(
        long,
        value_name = "ID",
        help = "List only the runs of the definition ID"
    )]
    definition: Option<String>,

    #[command(flatten)]
    output: super::control::JsonOutput,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        let home = match WorkflowHome::from_environment() {
            Ok(home) => home,
            Err(error) => {
                return super::control::write_refusal(
                    COMMAND,
                    None,
                    &super::control::Refusal::new(
                        error.code(),
                        "The um home cannot be located: neither UM_HOME nor HOME is set.",
                    ),
                    self.output.json,
                );
            }
        };
        let listing = list_runs(&home)
            .map_err(|error| anyhow!(error).context("list the runs under the um home"))?;
        write_unreadable(&listing.unreadable).with_context(|| format!("write {COMMAND} output"))?;
        let owner = self.owner.map(|owner| {
            if owner == "me" {
                super::super::login_name()
            } else {
                owner
            }
        });
        let runs = listing
            .runs
            .iter()
            .filter(|run| !self.waiting || run.record.state == RunState::Waiting)
            .filter(|run| {
                owner
                    .as_ref()
                    .is_none_or(|owner| run.record.owner == *owner)
            })
            .filter(|run| {
                self.definition
                    .as_ref()
                    .is_none_or(|id| run.record.definition.as_ref() == Some(id))
            })
            .collect::<Vec<_>>();
        let runs = if self.drift {
            let currents = current_revisions(&home, &runs)
                .with_context(|| format!("write {COMMAND} output"))?;
            runs.into_iter()
                .filter_map(|run| {
                    let current = *currents.get(run.record.definition.as_deref()?)?;
                    run.drifted(current).then_some((run, Some(current)))
                })
                .collect::<Vec<_>>()
        } else {
            runs.into_iter().map(|run| (run, None)).collect()
        };
        if self.output.json {
            super::super::super::write_pretty_json(&ListDocument {
                schema_version: 1,
                command: COMMAND,
                runs: runs
                    .iter()
                    .map(|(run, current)| listed(run, *current))
                    .collect(),
            })
        } else {
            write_plain(&runs)
        }
        .with_context(|| format!("write {COMMAND} output"))?;
        Ok(ExitCode::Success)
    }
}

/// The run's position: the wait's step path while it waits, else the record's `position`
/// joined by `.`, or `None` when it has none.
pub(super) fn position(record: &WorkflowRunRecord, wait: Option<&WaitRecord>) -> Option<String> {
    if let Some(wait) = wait.filter(|_| record.state == RunState::Waiting) {
        return Some(wait.step_path());
    }
    (!record.position.is_empty()).then(|| position_path(&record.position))
}

/// The closed code an unreadable `runs/` entry is named with: `run_record_missing` for a
/// directory without `run.json`, `run_directory_invalid` otherwise.
fn unreadable_code(error: &LocalRunDirectoryError) -> &'static str {
    match error {
        LocalRunDirectoryError::File { path, source, .. }
            if source.kind() == io::ErrorKind::NotFound
                && path.file_name().is_some_and(|name| name == "run.json") =>
        {
            "run_record_missing"
        }
        _ => "run_directory_invalid",
    }
}

fn write_unreadable(unreadable: &[(std::path::PathBuf, LocalRunDirectoryError)]) -> io::Result<()> {
    if unreadable.is_empty() {
        return Ok(());
    }
    let mut stderr = io::stderr().lock();
    for (path, error) in unreadable {
        writeln!(
            stderr,
            "! {}: {}",
            normalize_terminal_path(path),
            unreadable_code(error)
        )?;
    }
    stderr.flush()
}

/// The current revision of each definition `runs` names, each record read once
/// (REQ-LOCALRUN-190); a definition whose record cannot be read is named on stderr with its
/// code and left out, so its runs are omitted.
fn current_revisions(
    home: &WorkflowHome,
    runs: &[&ListedRun],
) -> io::Result<BTreeMap<String, u64>> {
    let definitions = runs
        .iter()
        .filter_map(|run| run.record.definition.as_deref())
        .collect::<BTreeSet<_>>();
    let mut currents = BTreeMap::new();
    let mut stderr = io::stderr().lock();
    for id in definitions {
        match read_definition(home, id) {
            Ok(definition) => {
                currents.insert(id.to_owned(), definition.record.current);
            }
            Err(error) => writeln!(
                stderr,
                "! {}: {}",
                normalize_terminal_path(&home.definition_directory(id)),
                error.code()
            )?,
        }
    }
    stderr.flush()?;
    Ok(currents)
}

/// `value` made safe for a terminal, or `-` for none.
pub(super) fn or_dash(value: Option<&str>) -> String {
    value.map_or_else(
        || "-".to_owned(),
        |value| normalize_terminal_scalar(value.as_bytes()),
    )
}

fn write_plain(runs: &[(&ListedRun, Option<u64>)]) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    if runs.is_empty() {
        writeln!(stdout, "No workflow runs.")?;
        return stdout.flush();
    }
    for (run, current) in runs {
        let record = &run.record;
        let definition = match (&record.definition, record.revision) {
            (Some(definition), Some(revision)) => {
                format!(
                    "{}@{revision}",
                    normalize_terminal_scalar(definition.as_bytes())
                )
            }
            _ => "path".to_owned(),
        };
        write!(
            stdout,
            "{} · {definition} · {} · {} · {} · {} · {} · {}",
            normalize_terminal_scalar(record.id.as_bytes()),
            or_dash(record.subject.as_deref()),
            state_name(record.state),
            or_dash(position(record, run.wait.as_ref()).as_deref()),
            or_dash(run.wait.as_ref().map(WaitRecord::kind_name)),
            normalize_terminal_scalar(record.owner.as_bytes()),
            or_dash(record.outcome.as_deref()),
        )?;
        for (label, run) in [("pred", &record.predecessor), ("succ", &record.successor)] {
            if let Some(run) = run {
                write!(
                    stdout,
                    " · {label} {}",
                    normalize_terminal_scalar(run.as_bytes())
                )?;
            }
        }
        match current {
            Some(current) => writeln!(stdout, " · current {current}")?,
            None => writeln!(stdout)?,
        }
    }
    stdout.flush()
}

pub(super) use super::super::state_name;

fn listed(run: &ListedRun, current: Option<u64>) -> Listed<'_> {
    let record = &run.record;
    Listed {
        id: &record.id,
        directory: &run.directory,
        definition: record.definition.as_deref(),
        revision: record.revision,
        subject: record.subject.as_deref(),
        workflow: &record.workflow,
        owner: &record.owner,
        state: record.state,
        outcome: record.outcome.as_deref(),
        predecessor: record.predecessor.as_deref(),
        successor: record.successor.as_deref(),
        position: position(record, run.wait.as_ref()),
        wait: run.wait.as_ref().map(|wait| ListedWait {
            kind: wait.kind_name(),
            since: &wait.since,
            step: &wait.step,
        }),
        attempts: record.attempts,
        started_at: &record.started_at,
        current,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListDocument<'a> {
    schema_version: u8,
    command: &'static str,
    runs: Vec<Listed<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Listed<'a> {
    id: &'a str,
    directory: &'a Path,
    definition: Option<&'a str>,
    revision: Option<u64>,
    subject: Option<&'a str>,
    workflow: &'a str,
    owner: &'a str,
    state: RunState,
    outcome: Option<&'a str>,
    predecessor: Option<&'a str>,
    successor: Option<&'a str>,
    position: Option<String>,
    wait: Option<ListedWait<'a>>,
    attempts: u64,
    started_at: &'a str,
    /// The definition's current revision, under `--drift` only.
    #[serde(skip_serializing_if = "Option::is_none")]
    current: Option<u64>,
}

#[derive(Serialize)]
struct ListedWait<'a> {
    kind: &'static str,
    since: &'a str,
    step: &'a [Value],
}
