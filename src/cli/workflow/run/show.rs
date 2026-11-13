//! `um workflow run show` (REQ-LOCALRUN-189): one run from its records, read as `status`
//! reads it, without taking its lock or changing its directory, with its mirror writes
//! (REQ-LOCALRUN-193) and each running child's attempt and in-flight steps, read from the
//! child's records the same way; with `--cloud ORGANIZATION`, the Cloud run `RUN` names
//! instead.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;
use serde::Serialize;
use serde_json::{Map, Value};
use um_execution::{
    ChildRun, ControlEvent, LocalStatusAttempt, LocalStatusProgress, LocalStatusResult,
    LocalStatusStart, MirrorAction, MirrorSkipReason, MirrorWrite, MirrorWriteOutcome, RunState,
    WaitRecord, WorkflowRunRecord, child_runs, human_duration, normalize_terminal_path,
    normalize_terminal_scalar, read_local_run_status, read_numbered_controls,
    read_numbered_mirror_writes,
};

use super::control::{Refusal, write_refusal};
use super::list::{or_dash, position, state_name};
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str =
    "Show a local workflow run from its records, or a Cloud run with --cloud";
pub(super) const AFTER_HELP: &str = "With --cloud ORGANIZATION, RUN is a Cloud run ID: the command reads that run with its definition, subject claim, workflow state and outcome, latest progress, exits and newest trigger deliveries. It reads and writes no local run.";
const COMMAND: &str = "um workflow run show";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[command(flatten)]
    output: super::control::JsonOutput,

    #[command(flatten)]
    cloud: super::CloudSelector,
}

/// Everything `show` prints about one run.
struct Shown {
    run_directory: PathBuf,
    record: WorkflowRunRecord,
    wait: Option<WaitRecord>,
    attempts: Vec<Attempt>,
    children: Vec<Child>,
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

/// A child run with its progress while its record is `running`.
struct Child {
    run: ChildRun,
    progress: Option<ChildProgress>,
}

/// A running child's current attempt, read from its own records (REQ-LOCALRUN-189).
struct ChildProgress {
    attempt: u64,
    state: &'static str,
    recovery: &'static str,
    progress: LocalStatusProgress,
}

/// A run `show` refuses, with the run directory once `RUN` resolved.
type Refused = (Option<PathBuf>, Refusal);

impl Command {
    pub(super) fn execute(mut self) -> super::super::super::CommandResult {
        if let Some(organization) = self.cloud.organization.take() {
            return super::cloud::CloudRun::new(
                self.run,
                organization,
                self.cloud,
                self.output.json,
            )?
            .show();
        }
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
        let now = um_support::utc_now();
        let children = child_runs(&run_directory)
            .map_err(|error| invalid(error.to_string()))?
            .into_iter()
            .map(|run| {
                let progress = (run.record.state == RunState::Running)
                    .then(|| child_progress(&run, now))
                    .transpose()
                    .map_err(invalid)?;
                Ok(Child { run, progress })
            })
            .collect::<Result<_, Refused>>()?;
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

/// A running child's current attempt and its in-flight nodes, with the time since each start
/// at `now`, or the reason its records cannot be read, naming the child: the refusal's run
/// directory is the parent's.
fn child_progress(child: &ChildRun, now: time::OffsetDateTime) -> Result<ChildProgress, String> {
    let unreadable = |reason: &dyn std::fmt::Display| {
        format!(
            "Child run {}: {reason}",
            normalize_terminal_path(&child.directory)
        )
    };
    let snapshot = read_local_run_status(&child.directory)
        .map_err(|error| unreadable(&error.code.message()))?;
    let progress = snapshot
        .status_state
        .in_flight(now)
        .map_err(|error| unreadable(&error))?;
    Ok(ChildProgress {
        attempt: snapshot.current_attempt_number,
        state: snapshot.current_attempt_state,
        recovery: snapshot.recovery.as_str(),
        progress,
    })
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
    progress: Option<ShownProgress<'a>>,
}

/// A running child's progress: the recorded start times, never an elapsed time.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShownProgress<'a> {
    attempt: u64,
    state: &'static str,
    started_at: Option<&'a str>,
    recovery: &'static str,
    steps: Vec<ShownNode<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShownNode<'a> {
    id: &'a str,
    role: &'a str,
    state: &'a str,
    started_at: Option<&'a str>,
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
            .map(|Child { run, progress }| ShownChild {
                step: &run.step,
                item: run.item,
                ordinal: run.ordinal,
                id: &run.record.id,
                state: state_name(run.record.state),
                outcome: run.record.outcome.as_deref(),
                directory: &run.directory,
                progress: progress.as_ref().map(shown_progress),
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

fn started_at(start: Option<&LocalStatusStart>) -> Option<&str> {
    start.map(|start| start.at.as_str())
}

fn shown_progress(child: &ChildProgress) -> ShownProgress<'_> {
    ShownProgress {
        attempt: child.attempt,
        state: child.state,
        started_at: started_at(child.progress.started.as_ref()),
        recovery: child.recovery,
        steps: child
            .progress
            .nodes
            .iter()
            .map(|node| ShownNode {
                id: &node.id,
                role: &node.role,
                state: &node.state,
                started_at: started_at(node.started.as_ref()),
            })
            .collect(),
    }
}

/// The time since a recorded start (REQ-LOCALRUN-124), `-` when the state records none.
fn elapsed_text(start: Option<&LocalStatusStart>) -> String {
    start.map_or_else(|| "-".to_owned(), |start| human_duration(start.elapsed))
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
    for Child { run, progress } in &shown.children {
        writeln!(
            stdout,
            "  {} {} {:06}  {}  {}  {}  {}",
            text(&run.step),
            run.item
                .map_or_else(|| "-".to_owned(), |item| item.to_string()),
            run.ordinal,
            text(&run.record.id),
            state_name(run.record.state),
            or_dash(run.record.outcome.as_deref()),
            normalize_terminal_path(&run.directory)
        )?;
        if let Some(child) = progress {
            writeln!(
                stdout,
                "    attempt {} · {} · {}",
                child.attempt,
                child.recovery,
                elapsed_text(child.progress.started.as_ref())
            )?;
            for node in &child.progress.nodes {
                writeln!(
                    stdout,
                    "    {} · {} · {}",
                    text(&node.id),
                    node.state,
                    elapsed_text(node.started.as_ref())
                )?;
            }
        }
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
