//! `um workflow run show` (REQ-LOCALRUN-189): one run from its records, read as `status`
//! reads it, without taking its lock or changing its directory, with its mirror writes
//! (REQ-LOCALRUN-193).

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;
use serde::Serialize;
use serde_json::{Map, Value};
use um_execution::{
    ChildRun, ControlEvent, LocalStatusAttempt, LocalStatusResult, MirrorAction, MirrorSkipReason,
    MirrorWrite, MirrorWriteOutcome, WaitRecord, WorkflowRunRecord, child_runs,
    normalize_terminal_path, normalize_terminal_scalar, read_local_run_status,
    read_numbered_controls, read_numbered_mirror_writes,
};

use super::control::{Refusal, write_refusal};
use super::list::{or_dash, position, state_name};
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str =
    "Show a local workflow run with its wait, children, controls and mirror writes";
const COMMAND: &str = "um workflow run show";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[command(flatten)]
    output: super::control::JsonOutput,
}

/// Everything `show` prints about one run.
struct Shown {
    run_directory: PathBuf,
    record: WorkflowRunRecord,
    wait: Option<WaitRecord>,
    attempts: Vec<Attempt>,
    children: Vec<ChildRun>,
    controls: Vec<(u64, ControlEvent)>,
    mirror: Vec<(u64, MirrorWrite)>,
}

/// An attempt with the continuation `reason` and `edge` and the `imported` source run its
/// `state.json` entry records, when it has them.
struct Attempt {
    attempt: LocalStatusAttempt,
    reason: Option<Value>,
    edge: Option<Value>,
    imported: Option<Value>,
}

/// A run `show` refuses, with the run directory once `RUN` resolved.
type Refused = (Option<PathBuf>, Refusal);

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        super::super::super::execute_read_only_with_signals("workflow run show", move |control| {
            let json = self.output.json;
            let shown = self.read();
            super::super::super::complete_read_only_output(control, || match shown {
                Ok(shown) => write_shown(&shown, json),
                Err((run_directory, refusal)) => {
                    write_refusal(COMMAND, run_directory.as_deref(), &refusal, json)
                }
            })
        })
    }

    fn read(&self) -> Result<Shown, Refused> {
        let run_directory = self
            .run
            .resolve()
            .map_err(|error| (None, Refusal::new(error.code, error.message)))?;
        let snapshot = read_local_run_status(&run_directory).map_err(|error| {
            (
                Some(error.run_directory.unwrap_or_else(|| run_directory.clone())),
                Refusal::new("run_directory_invalid", error.code.message()),
            )
        })?;
        let run_directory = snapshot.run_directory;
        let invalid = |message: String| {
            (
                Some(run_directory.clone()),
                Refusal::new("run_directory_invalid", message),
            )
        };
        let record = snapshot.record.ok_or_else(|| {
            (
                Some(run_directory.clone()),
                Refusal::new(
                    "run_record_missing",
                    "The run has no run record; only a run admitted from a tree or a definition has one.",
                ),
            )
        })?;
        let record: WorkflowRunRecord =
            serde_json::from_value(record).map_err(|error| invalid(error.to_string()))?;
        let children = child_runs(&run_directory).map_err(|error| invalid(error.to_string()))?;
        let controls =
            read_numbered_controls(&run_directory).map_err(|error| invalid(error.to_string()))?;
        let mirror = read_numbered_mirror_writes(&run_directory)
            .map_err(|error| invalid(error.to_string()))?;
        let attempts = snapshot
            .attempts
            .into_iter()
            .map(|attempt| {
                let entry = state_attempt(&snapshot.state, attempt.attempt_number);
                let field = |name| {
                    entry
                        .and_then(|entry| entry["continuation"].get(name))
                        .cloned()
                };
                Attempt {
                    reason: field("reason"),
                    edge: field("edge"),
                    imported: entry.and_then(|entry| entry.get("imported")).cloned(),
                    attempt,
                }
            })
            .collect();
        Ok(Shown {
            record,
            wait: snapshot.wait,
            attempts,
            children,
            controls,
            mirror,
            run_directory,
        })
    }
}

/// The `state.json` entry for attempt `number`, when it has one.
fn state_attempt(state: &Value, number: u64) -> Option<&Value> {
    state["attempts"]
        .as_array()?
        .iter()
        .find(|attempt| attempt["attemptNumber"] == number)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShowDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    run_directory: &'a Path,
    record: &'a WorkflowRunRecord,
    position: Option<String>,
    wait: Option<&'a WaitRecord>,
    attempts: Vec<ShownAttempt<'a>>,
    children: Vec<ShownChild<'a>>,
    controls: Vec<ShownControl<'a>>,
    mirror: Vec<ShownMirror<'a>>,
}

#[derive(Serialize)]
struct ShownAttempt<'a> {
    number: u64,
    trigger: &'static str,
    state: &'static str,
    reason: Option<&'a Value>,
    edge: Option<&'a Value>,
    /// The run a successor imported this attempt from (REQ-LOCALRUN-195), when it did.
    #[serde(skip_serializing_if = "Option::is_none")]
    imported: Option<&'a Value>,
    result: ShownResult<'a>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ShownResult<'a> {
    NotPublished { reason: &'static str },
    Published { directory: &'a str },
    PublicationFailed { phase: &'static str },
}

impl<'a> From<&'a LocalStatusResult> for ShownResult<'a> {
    fn from(result: &'a LocalStatusResult) -> Self {
        match result {
            LocalStatusResult::NotPublished { reason } => Self::NotPublished { reason },
            LocalStatusResult::Published { relative_directory } => Self::Published {
                directory: relative_directory,
            },
            LocalStatusResult::PublicationFailed { phase } => Self::PublicationFailed { phase },
        }
    }
}

#[derive(Serialize)]
struct ShownChild<'a> {
    step: &'a str,
    item: Option<u64>,
    ordinal: u64,
    id: &'a str,
    state: &'static str,
    outcome: Option<&'a str>,
    directory: &'a Path,
}

#[derive(Serialize)]
struct ShownControl<'a> {
    ordinal: u64,
    event: &'a str,
    by: &'a str,
    at: &'a str,
    wait: Option<&'a str>,
    payload: Option<&'a Map<String, Value>>,
}

/// A mirror write with the ordinal of its file name.
#[derive(Serialize)]
struct ShownMirror<'a> {
    ordinal: u64,
    #[serde(flatten)]
    write: &'a MirrorWrite,
}

fn write_shown(shown: &Shown, json: bool) -> super::super::super::CommandResult {
    if json {
        super::super::super::write_pretty_json(&document(shown))
    } else {
        write_plain(shown)
    }
    .with_context(|| format!("write {COMMAND} output"))?;
    Ok(ExitCode::Success)
}

fn document(shown: &Shown) -> ShowDocument<'_> {
    ShowDocument {
        schema_version: 1,
        command: COMMAND,
        outcome: "shown",
        exit_status: ExitCode::Success.as_u8(),
        run_directory: &shown.run_directory,
        record: &shown.record,
        position: position(&shown.record, shown.wait.as_ref()),
        wait: shown.wait.as_ref(),
        attempts: shown
            .attempts
            .iter()
            .map(|shown| ShownAttempt {
                number: shown.attempt.attempt_number,
                trigger: shown.attempt.trigger,
                state: shown.attempt.state,
                reason: shown.reason.as_ref(),
                edge: shown.edge.as_ref(),
                imported: shown.imported.as_ref(),
                result: (&shown.attempt.result).into(),
            })
            .collect(),
        children: shown
            .children
            .iter()
            .map(|child| ShownChild {
                step: &child.step,
                item: child.item,
                ordinal: child.ordinal,
                id: &child.record.id,
                state: state_name(child.record.state),
                outcome: child.record.outcome.as_deref(),
                directory: &child.directory,
            })
            .collect(),
        controls: shown
            .controls
            .iter()
            .map(|(ordinal, control)| ShownControl {
                ordinal: *ordinal,
                event: &control.event,
                by: &control.by,
                at: &control.at,
                wait: control.wait.as_deref(),
                payload: control.payload.as_ref(),
            })
            .collect(),
        mirror: shown
            .mirror
            .iter()
            .map(|(ordinal, write)| ShownMirror {
                ordinal: *ordinal,
                write,
            })
            .collect(),
    }
}

fn text(value: &str) -> String {
    normalize_terminal_scalar(value.as_bytes())
}

fn compact(value: &impl Serialize) -> String {
    serde_json::to_string(value).map_or_else(|_| "-".to_owned(), |value| text(&value))
}

/// The edge an attempt took as `<step>.<outcome> -> <target>`, `(counted)` when it counted
/// against the edge's limit, or its JSON when it has another shape.
fn edge_text(edge: &Value) -> String {
    match (
        edge["step"].as_str(),
        edge["outcome"].as_str(),
        edge["target"].as_str(),
    ) {
        (Some(step), Some(outcome), Some(target)) => format!(
            "{}.{} -> {}{}",
            text(step),
            text(outcome),
            text(target),
            if edge["counted"] == true {
                " (counted)"
            } else {
                ""
            }
        ),
        _ => compact(edge),
    }
}

/// A mirror write as `<hook> · comment <body>|state <value> · <result>`, the result
/// `written`, `skipped <reason>` or `failed <code>`; a state whose bound value could not be
/// read is `-`.
fn mirror_text(write: &MirrorWrite) -> String {
    let action = match &write.action {
        MirrorAction::Comment { body } => format!("comment {}", text(body)),
        MirrorAction::State { state, .. } => format!("state {}", or_dash(state.as_deref())),
    };
    let result = match write.outcome {
        MirrorWriteOutcome::Written => "written".to_owned(),
        MirrorWriteOutcome::Skipped => match write.reason {
            Some(MirrorSkipReason::GuardHeld) => "skipped guard_held".to_owned(),
            None => "skipped -".to_owned(),
        },
        MirrorWriteOutcome::Failed => format!("failed {}", or_dash(write.code.as_deref())),
    };
    format!("{} · {action} · {result}", write.hook.as_str())
}

/// The run's edge counts as `<step>.<outcome>=<n>`, or `-` when it took none.
fn edges_text(record: &WorkflowRunRecord) -> String {
    if record.edges.is_empty() {
        return "-".to_owned();
    }
    record
        .edges
        .iter()
        .map(|(edge, count)| format!("{}={count}", text(edge)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn write_plain(shown: &Shown) -> io::Result<()> {
    let record = &shown.record;
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "run: {}", text(&record.id))?;
    writeln!(
        stdout,
        "directory: {}",
        normalize_terminal_path(&shown.run_directory)
    )?;
    match (&record.definition, record.revision) {
        (Some(definition), Some(revision)) => {
            writeln!(stdout, "definition: {}@{revision}", text(definition))?;
        }
        _ => writeln!(stdout, "definition: -")?,
    }
    writeln!(stdout, "workflow: {}", text(&record.workflow))?;
    writeln!(stdout, "subject: {}", or_dash(record.subject.as_deref()))?;
    writeln!(stdout, "owner: {}", text(&record.owner))?;
    writeln!(stdout, "state: {}", state_name(record.state))?;
    writeln!(stdout, "outcome: {}", or_dash(record.outcome.as_deref()))?;
    writeln!(stdout, "started: {}", text(&record.started_at))?;
    writeln!(stdout, "attempts: {}", record.attempts)?;
    writeln!(
        stdout,
        "predecessor: {}",
        or_dash(record.predecessor.as_deref())
    )?;
    writeln!(
        stdout,
        "successor: {}",
        or_dash(record.successor.as_deref())
    )?;
    writeln!(stdout, "edges: {}", edges_text(record))?;
    writeln!(
        stdout,
        "position: {}",
        or_dash(position(record, shown.wait.as_ref()).as_deref())
    )?;
    match &shown.wait {
        Some(wait) => {
            writeln!(
                stdout,
                "wait: {} at step {} since {}",
                wait.kind_name(),
                text(&wait.step_path()),
                text(&wait.since)
            )?;
            writeln!(stdout, "  rule: {}", compact(&wait.rule))?;
            writeln!(stdout, "  payload: {}", compact(&wait.payload))?;
        }
        None => writeln!(stdout, "wait: none")?,
    }
    writeln!(stdout)?;
    writeln!(stdout, "attempts:")?;
    for Attempt {
        attempt,
        reason,
        edge,
        imported,
    } in &shown.attempts
    {
        write!(
            stdout,
            "  {}  {}  {}  {}",
            attempt.attempt_number,
            attempt.trigger,
            attempt.state,
            match &attempt.result {
                LocalStatusResult::NotPublished { reason } => format!("not_published ({reason})"),
                LocalStatusResult::Published { relative_directory } => {
                    format!("published ({})", text(relative_directory))
                }
                LocalStatusResult::PublicationFailed { phase } => {
                    format!("publication_failed ({phase})")
                }
            }
        )?;
        if let Some(reason) = reason {
            write!(stdout, "  reason {}", compact(reason))?;
        }
        if let Some(edge) = edge {
            write!(stdout, "  edge {}", edge_text(edge))?;
        }
        if let Some(run) = imported
            .as_ref()
            .and_then(|imported| imported["run"].as_str())
        {
            write!(stdout, "  imported from {}", text(run))?;
        }
        writeln!(stdout)?;
    }
    writeln!(stdout)?;
    writeln!(stdout, "children:")?;
    for child in &shown.children {
        writeln!(
            stdout,
            "  {} {} {:06}  {}  {}  {}  {}",
            text(&child.step),
            child
                .item
                .map_or_else(|| "-".to_owned(), |item| item.to_string()),
            child.ordinal,
            text(&child.record.id),
            state_name(child.record.state),
            or_dash(child.record.outcome.as_deref()),
            normalize_terminal_path(&child.directory)
        )?;
    }
    writeln!(stdout)?;
    writeln!(stdout, "controls:")?;
    for (ordinal, control) in &shown.controls {
        write!(
            stdout,
            "  {ordinal:04}  {}  by {}  at {}  wait {}",
            text(&control.event),
            text(&control.by),
            text(&control.at),
            or_dash(control.wait.as_deref())
        )?;
        if let Some(payload) = &control.payload {
            write!(stdout, "  {}", compact(payload))?;
        }
        writeln!(stdout)?;
    }
    writeln!(stdout)?;
    if shown.mirror.is_empty() {
        writeln!(stdout, "mirror: none")?;
    } else {
        writeln!(stdout, "mirror:")?;
        for (ordinal, write) in &shown.mirror {
            writeln!(stdout, "  {ordinal:04} · {}", mirror_text(write))?;
        }
    }
    stdout.flush()
}
