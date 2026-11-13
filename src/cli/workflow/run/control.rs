//! What every control command shares (REQ-LOCALRUN-181): opening the run `RUN` names,
//! appending one control event, and the recorded and refused documents.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use serde::Serialize;
use serde_json::{Map, Value};
use um_execution::{
    ControlEvent, LocalRunDirectoryError, NoConditions, RunState, WaitInspection, WaitKind,
    WaitRecord, WorkflowRunRecord, append_control, inspect_wait, normalize_terminal_path,
    normalize_terminal_scalar, read_local_run_status,
};

use crate::exit_code::ExitCode;

/// A control command's `--json` option.
pub(super) type JsonOutput = super::super::super::JsonArgs<super::super::super::WorkflowJson>;

/// A run a control command acts on: its record and its wait as `inspect_wait` found them
/// without taking the run lock. A control command observes no condition (`NoConditions`), so
/// an `external` wait reads as held whatever its provider would answer.
pub(super) struct ControlTarget {
    pub(super) run_directory: PathBuf,
    pub(super) record: WorkflowRunRecord,
    /// The run's `state.json`, as `um workflow run status` reads it.
    pub(super) state: Value,
    pub(super) inspection: WaitInspection,
}

/// A control that does not apply, refused before anything is written: one closed `code`.
#[derive(Debug)]
pub(super) struct Refusal {
    pub(super) code: &'static str,
    pub(super) message: String,
}

impl Refusal {
    pub(super) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Why a control command did not act: a refusal, or an operational failure.
pub(super) enum ControlFailure {
    Refused(Refusal),
    Failed(anyhow::Error),
}

impl From<Refusal> for ControlFailure {
    fn from(refusal: Refusal) -> Self {
        Self::Refused(refusal)
    }
}

impl From<LocalRunDirectoryError> for ControlFailure {
    fn from(error: LocalRunDirectoryError) -> Self {
        Self::Failed(anyhow::Error::new(error))
    }
}

impl From<anyhow::Error> for ControlFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Failed(error)
    }
}

/// The control event a command appended, as its document reports it.
#[derive(Serialize)]
pub(super) struct RecordedControl {
    pub(super) ordinal: u64,
    pub(super) event: String,
    /// The control file relative to the run directory, `controls/<NNNN>-<event>.json`.
    pub(super) path: String,
    pub(super) wait: Option<String>,
}

/// What a control command did: the control it appended, the run's wait afterwards, and the
/// document's `outcome` (`recorded`, the outcome the command ended the run with, or
/// `interrupted`).
pub(super) struct Acted {
    pub(super) control: RecordedControl,
    pub(super) wait: Option<WaitRecord>,
    pub(super) outcome: &'static str,
}

impl Acted {
    pub(super) fn recorded(control: RecordedControl, wait: Option<WaitRecord>) -> Self {
        Self {
            control,
            wait,
            outcome: "recorded",
        }
    }
}

impl ControlTarget {
    /// The run's current wait, whether or not a control already resolves it.
    pub(super) fn wait(&self) -> Option<&WaitRecord> {
        inspected_wait(&self.inspection)
    }

    /// The wait of `kind` a resolving command (`answer`, `approve`, `hand-back`) acts on:
    /// refused while a live process holds the run, when it does not wait, when it waits on
    /// another kind, and when a recorded control already resolves the wait.
    pub(super) fn wait_to_resolve(&self, kind: WaitKind) -> Result<&WaitRecord, Refusal> {
        let wait = match &self.inspection {
            WaitInspection::Locked(_) => return Err(run_locked()),
            WaitInspection::NoWait { .. } | WaitInspection::Continued { .. } => {
                return Err(Refusal::new(
                    "run_not_waiting",
                    "The run does not wait, so there is nothing to resolve.",
                ));
            }
            WaitInspection::Held { wait }
            | WaitInspection::Satisfied { wait, .. }
            | WaitInspection::RuleInvalid { wait, .. }
            | WaitInspection::ConditionUnavailable { wait, .. } => wait,
        };
        if wait.kind != kind {
            return Err(wait_kind_mismatch(wait));
        }
        if matches!(self.inspection, WaitInspection::Satisfied { .. }) {
            return Err(wait_resolved());
        }
        Ok(wait)
    }

    /// The ids of the steps of the run's latest attempt.
    pub(super) fn step_ids(&self) -> Vec<&str> {
        self.state["attempts"]
            .as_array()
            .and_then(|attempts| attempts.last())
            .and_then(|attempt| attempt["progress"]["steps"].as_array())
            .into_iter()
            .flatten()
            .filter_map(|step| step["id"].as_str())
            .collect()
    }

    /// Appends the control `event` naming `wait` with `payload`, by the invoking user.
    pub(super) fn record(
        &self,
        event: &str,
        wait: Option<String>,
        payload: Option<Map<String, Value>>,
    ) -> Result<RecordedControl, ControlFailure> {
        let control = ControlEvent::new(
            event,
            &self.record.id,
            &super::super::login_name(),
            wait,
            payload,
        )?;
        let path = append_control(&self.run_directory, &control)?;
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("appended control file name is not UTF-8"))?;
        let ordinal = name
            .split_once('-')
            .and_then(|(ordinal, _)| ordinal.parse().ok())
            .ok_or_else(|| anyhow!("appended control file name has no ordinal"))?;
        Ok(RecordedControl {
            ordinal,
            event: control.event,
            path: format!("controls/{name}"),
            wait: control.wait,
        })
    }
}

/// The wait `inspection` found, whether or not a control already resolves it.
fn inspected_wait(inspection: &WaitInspection) -> Option<&WaitRecord> {
    match inspection {
        WaitInspection::Held { wait }
        | WaitInspection::Satisfied { wait, .. }
        | WaitInspection::RuleInvalid { wait, .. }
        | WaitInspection::ConditionUnavailable { wait, .. } => Some(wait),
        WaitInspection::Locked(_)
        | WaitInspection::NoWait { .. }
        | WaitInspection::Continued { .. } => None,
    }
}

/// The current wait of the run at `run_directory`, inspected again without taking the run
/// lock, for a command that reports where a run it signalled stopped (REQ-LOCALRUN-202).
pub(super) fn current_wait(run_directory: &Path) -> Result<Option<WaitRecord>, ControlFailure> {
    let probe = inspect_wait(run_directory, &NoConditions)?;
    Ok(inspected_wait(&probe.inspection).cloned())
}

pub(super) fn run_locked() -> Refusal {
    Refusal::new(
        "run_locked",
        "A live process holds the run; wait for it to finish or cancel it.",
    )
}

/// A live run that already holds a withdrawal its process has not yet honored
/// (REQ-LOCALRUN-186).
pub(super) fn withdraw_pending() -> Refusal {
    Refusal::new(
        "withdraw_pending",
        "A withdrawal is already recorded; the run ends once its running stage finishes.",
    )
}

pub(super) fn wait_kind_mismatch(wait: &WaitRecord) -> Refusal {
    Refusal::new(
        "wait_kind_mismatch",
        format!(
            "The run waits on {}, which this command does not resolve.",
            wait.kind_name()
        ),
    )
}

pub(super) fn wait_resolved() -> Refusal {
    Refusal::new(
        "wait_resolved",
        "A recorded control already resolves the run's wait; resume the run instead.",
    )
}

/// Opens the run at `run_directory` (REQ-LOCALRUN-181): a run without a run record is
/// `run_record_missing`, and a directory that is not a run `run_directory_invalid`.
pub(super) fn open(run_directory: &Path) -> Result<ControlTarget, Refusal> {
    fn invalid<E>(_: E) -> Refusal {
        Refusal::new(
            "run_directory_invalid",
            "The directory is not a readable workflow run directory.",
        )
    }
    let snapshot = read_local_run_status(run_directory).map_err(invalid)?;
    let record = snapshot.record.ok_or_else(|| {
        Refusal::new(
            "run_record_missing",
            "The run has no run record; only a run admitted from a tree or a definition takes controls.",
        )
    })?;
    let record: WorkflowRunRecord = serde_json::from_value(record).map_err(invalid)?;
    let probe = inspect_wait(run_directory, &NoConditions).map_err(invalid)?;
    Ok(ControlTarget {
        run_directory: probe.run_directory,
        record,
        state: snapshot.state,
        inspection: probe.inspection,
    })
}

/// The run at `run_directory` opened as `open` does when it waits, or `None` when it does not
/// wait or cannot be probed, for a command that acts on a waiting run and otherwise does what
/// it did before the run could wait (`retry`, REQ-LOCALRUN-184).
pub(super) fn open_if_waiting(run_directory: &Path) -> Option<Result<ControlTarget, Refusal>> {
    let probe = inspect_wait(run_directory, &NoConditions).ok()?;
    matches!(
        probe.inspection,
        WaitInspection::Held { .. }
            | WaitInspection::Satisfied { .. }
            | WaitInspection::RuleInvalid { .. }
            | WaitInspection::ConditionUnavailable { .. }
    )
    .then(|| open(run_directory))
}

/// Runs control command `command` on `RUN` and prints its document: `act` appends the
/// control or refuses, after `RUN` resolves and the run opens.
pub(super) fn execute(
    command: &'static str,
    run: &super::ExistingLocalRun,
    json: bool,
    act: impl FnOnce(&ControlTarget) -> Result<Acted, ControlFailure>,
) -> super::super::super::CommandResult {
    let run_directory = match run.resolve() {
        Ok(run_directory) => run_directory,
        Err(error) => {
            return write_refusal(
                command,
                None,
                &Refusal::new(error.code, error.message),
                json,
            );
        }
    };
    let target = match open(&run_directory) {
        Ok(target) => target,
        Err(refusal) => return write_refusal(command, Some(&run_directory), &refusal, json),
    };
    match act(&target) {
        Ok(acted) => write_recorded(command, &target, &acted, json),
        Err(ControlFailure::Refused(refusal)) => {
            write_refusal(command, Some(&target.run_directory), &refusal, json)
        }
        Err(ControlFailure::Failed(error)) => Err(error
            .context(format!(
                "{command} {}",
                normalize_terminal_path(&target.run_directory)
            ))
            .into()),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RecordedDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    run_directory: &'a str,
    run: &'a str,
    control: &'a RecordedControl,
    wait: Option<&'a WaitRecord>,
}

fn write_recorded(
    command: &'static str,
    target: &ControlTarget,
    acted: &Acted,
    json: bool,
) -> super::super::super::CommandResult {
    if json {
        let run_directory = target
            .run_directory
            .to_str()
            .ok_or_else(|| anyhow!("normalized run path is not UTF-8"))?;
        super::super::super::write_pretty_json(&RecordedDocument {
            schema_version: 1,
            command,
            outcome: acted.outcome,
            exit_status: ExitCode::Success.as_u8(),
            run_directory,
            run: &target.record.id,
            control: &acted.control,
            wait: acted.wait.as_ref(),
        })
        .with_context(|| format!("write {command} output"))?;
    } else {
        let mut stdout = io::stdout().lock();
        write!(
            stdout,
            "recorded {} for {} as {}",
            acted.control.event,
            normalize_terminal_scalar(target.record.id.as_bytes()),
            acted.control.path
        )
        .and_then(|()| match acted.outcome {
            "recorded" => Ok(()),
            outcome => write!(stdout, " · run {outcome}"),
        })
        .and_then(|()| writeln!(stdout))
        .and_then(|()| stdout.flush())
        .with_context(|| format!("write {command} output"))?;
    }
    Ok(ExitCode::Success)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RefusedDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_directory: Option<&'a str>,
    diagnostics: [RefusalDiagnostic<'a>; 1],
}

#[derive(Serialize)]
struct RefusalDiagnostic<'a> {
    code: &'static str,
    message: &'a str,
}

/// Prints `refusal` of `command` and exits 1: with `--json` the `refused` document, naming
/// the run directory once one resolved, and otherwise a stderr line.
pub(super) fn write_refusal(
    command: &'static str,
    run_directory: Option<&Path>,
    refusal: &Refusal,
    json: bool,
) -> super::super::super::CommandResult {
    if json {
        super::super::super::write_pretty_json(&RefusedDocument {
            schema_version: 1,
            command,
            outcome: "refused",
            exit_status: ExitCode::GeneralFailure.as_u8(),
            run_directory: run_directory.and_then(Path::to_str),
            diagnostics: [RefusalDiagnostic {
                code: refusal.code,
                message: &refusal.message,
            }],
        })
        .with_context(|| format!("write {command} output"))?;
    } else {
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "{command} refused: {}: {}",
            refusal.code, refusal.message
        )
        .and_then(|()| stderr.flush())
        .with_context(|| format!("write {command} output"))?;
    }
    Ok(ExitCode::GeneralFailure)
}

/// Whether the run's record reads `exited`.
pub(super) fn exited(target: &ControlTarget) -> bool {
    target.record.state == RunState::Exited
}
