//! `um workflow definition show` (REQ-WFDEF-26).

use std::io::{self, Write};

use clap::Args;
use serde::Serialize;

use super::report::{display, write_json};
use um_execution::{
    BindingsSnapshot, DefinitionError, ListedRun, RevisionRecord, RunState, StoredDefinition,
    WorkflowHome, list_runs, read_current_bindings, read_definition,
};

pub(super) const ABOUT: &str = "Show a local workflow definition and its revisions";
const COMMAND_NAME: &str = "um workflow definition show";
const REFUSED: &str = "Show refused.";
const TREE_ROOT_FILE: &str = "workflow.yaml";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "ID", help = super::ID_HELP)]
    id: String,

    #[command(flatten)]
    output: super::super::super::JsonArgs<super::super::super::WorkflowJson>,
}

/// The record, whether each revision's tree copy is under the home, newest first, the
/// current revision's snapshot, and the definition's live runs in id order.
struct Shown {
    definition: StoredDefinition,
    retained: Vec<bool>,
    bindings: BindingsSnapshot,
    runs: Vec<ListedRun>,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        let result = WorkflowHome::from_environment().and_then(|home| shown(&home, &self.id));
        super::finish(
            COMMAND_NAME,
            REFUSED,
            result,
            self.output.json,
            |shown, json| {
                if json {
                    write_json(&show_report(shown), "workflow definition")
                } else {
                    write_human(shown)
                }
            },
        )
    }
}

fn shown(home: &WorkflowHome, id: &str) -> Result<Shown, DefinitionError> {
    let definition = read_definition(home, id)?;
    let bindings = read_current_bindings(&definition)?;
    let retained = definition
        .record
        .revisions
        .iter()
        .map(|revision| {
            home.tree_directory(&revision.tree)
                .is_some_and(|tree| tree.join(TREE_ROOT_FILE).is_file())
        })
        .collect();
    let runs = list_runs(home)
        .map_err(|error| DefinitionError::StoreUnavailable {
            path: home.runs_directory(id),
            operation: "list runs",
            source: io::Error::other(error),
        })?
        .runs
        .into_iter()
        .filter(|run| {
            run.record.definition.as_deref() == Some(id)
                && matches!(run.record.state, RunState::Running | RunState::Waiting)
        })
        .collect();
    Ok(Shown {
        definition,
        retained,
        bindings,
        runs,
    })
}

fn write_human(shown: &Shown) -> anyhow::Result<()> {
    let record = &shown.definition.record;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "definition: {}", display(&record.id))?;
    writeln!(stdout, "name: {}", display(&record.name))?;
    writeln!(stdout, "enabled: {}", super::yes_no(record.enabled))?;
    writeln!(stdout, "current: {}", record.current)?;
    writeln!(
        stdout,
        "cloud id: {}",
        record
            .cloud_id
            .as_deref()
            .map_or_else(|| "none".to_owned(), display)
    )?;
    writeln!(stdout, "── revisions ──")?;
    for (revision, retained) in record.revisions.iter().zip(&shown.retained) {
        writeln!(
            stdout,
            "{}{} · tree {} · bindings {} · published at {} · by {} · tree copy {}",
            revision.n,
            if revision.n == record.current {
                " (current)"
            } else {
                ""
            },
            display(&revision.tree),
            display(&revision.bindings),
            display(&revision.published_at),
            display(&revision.by),
            if *retained { "present" } else { "missing" }
        )?;
    }
    writeln!(stdout, "── bindings ──")?;
    for (key, bound) in shown.bindings.bindings() {
        writeln!(
            stdout,
            "{}: {} ({})",
            display(key),
            display(&bound.value),
            bound.kind.as_str()
        )?;
    }
    writeln!(stdout, "── live runs ──")?;
    if shown.runs.is_empty() {
        writeln!(stdout, "No live runs.")?;
    }
    for run in shown.runs.iter().map(|run| live_run(run, record.current)) {
        writeln!(
            stdout,
            "{} · revision {} · {} · {}{}",
            display(run.id),
            run.revision,
            run.subject.map_or_else(|| "-".to_owned(), display),
            run.state,
            if run.drifted { " · drifted" } else { "" }
        )?;
    }
    Ok(())
}

/// `run` as `show` reports it, drifted when behind `current` (REQ-LOCALRUN-190).
fn live_run(run: &ListedRun, current: u64) -> JsonRun<'_> {
    let record = &run.record;
    JsonRun {
        id: &record.id,
        revision: record.revision.unwrap_or_default(),
        subject: record.subject.as_deref(),
        state: super::super::state_name(record.state),
        drifted: run.drifted(current),
    }
}

fn show_report(shown: &Shown) -> JsonShown<'_> {
    let definition = &shown.definition;
    let record = &definition.record;
    JsonShown {
        schema_version: 1,
        command: COMMAND_NAME,
        definition: JsonDefinition {
            id: &record.id,
            name: &record.name,
            enabled: record.enabled,
            current: record.current,
            cloud_id: record.cloud_id.as_deref(),
            path: definition.path.to_string_lossy().into_owned(),
        },
        revisions: record
            .revisions
            .iter()
            .zip(&shown.retained)
            .map(|(revision, retained)| JsonRevision {
                revision,
                tree_retained: *retained,
            })
            .collect(),
        bindings: &shown.bindings,
        runs: shown
            .runs
            .iter()
            .map(|run| live_run(run, record.current))
            .collect(),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonShown<'a> {
    schema_version: u8,
    command: &'static str,
    definition: JsonDefinition<'a>,
    revisions: Vec<JsonRevision<'a>>,
    bindings: &'a BindingsSnapshot,
    runs: Vec<JsonRun<'a>>,
}

/// A live run of the definition: `id`, `revision`, `subject`, `state` and `drifted`.
#[derive(Serialize)]
struct JsonRun<'a> {
    id: &'a str,
    revision: u64,
    subject: Option<&'a str>,
    state: &'static str,
    drifted: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonDefinition<'a> {
    id: &'a str,
    name: &'a str,
    enabled: bool,
    current: u64,
    cloud_id: Option<&'a str>,
    path: String,
}

/// A revision as the record holds it (`n`, `tree`, `bindings`, `publishedAt`, `by`) and
/// whether its tree copy is present.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonRevision<'a> {
    #[serde(flatten)]
    revision: &'a RevisionRecord,
    tree_retained: bool,
}
