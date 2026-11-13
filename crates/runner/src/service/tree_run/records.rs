//! Reading the local run and wait records a tree run leaves (REQ-LOCALRUN-175,
//! REQ-LOCALRUN-178) and the terminal report they map to (REQ-RUNNERENROLL-99): the
//! transport result is `succeeded` unless the record's outcome is the engine's `failed`
//! (REQ-WFEXEC-57), or `canceled` or `withdrawn` (`cancelled`), the record's outcome
//! travels as `chosenOutcome`, its run's mirror writes as `mirror` and its own confirmed
//! departure as `claimRelease`. An adopted run (REQ-RUNNERENROLL-89) whose record
//! already read `exited` `failed` reports the failure its current attempt recorded, and
//! one whose first `resume --from` the runtime refused ends `failed` with the refusal's
//! code, as does a run whose `create` the runtime refused (REQ-RUNNERENROLL-91).

use std::path::Path;

use serde_json::{Value, json};
use um_execution::{
    CancellationReason, LocalRunDirectoryError, MirrorAction, MirrorHook, MirrorSkipReason,
    MirrorWrite, MirrorWriteOutcome, read_durable_record, wait_identity,
};
use um_runner_protocol::TreeExit;
use um_support::is_diagnostic_slug;

/// The most mirror results one finish carries, the protocol's bound.
const MAXIMUM_MIRROR_RESULTS: usize = 1024;

/// Reads `<run directory>/<name>` as JSON through the engine's own reader of its durable
/// records (`um_execution::read_durable_record`): a regular file opened without following
/// a symbolic link and without waiting on a FIFO, of at most the engine's bound; `None`
/// for a file that is absent, of another kind, larger or not JSON.
fn read_json(run_directory: &Path, name: &str) -> Option<Value> {
    let bytes = read_durable_record(run_directory, name).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// What `run.json` reads: its state, outcome, attempt count and position, and the run's
/// mirror writes once the resume loop has ended on it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct TreeRecord {
    pub(in crate::service) state: RecordState,
    pub(in crate::service) outcome: Option<String>,
    pub(in crate::service) attempts: u64,
    pub(in crate::service) position: Vec<Value>,
    /// The run's `mirror/` writes in order (REQ-LOCALRUN-193), read only for the record
    /// the loop ends on; empty while the loop observes the record.
    pub(in crate::service) mirror: Vec<MirrorWrite>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum RecordState {
    Running,
    Waiting,
    Exited,
}

/// Reads `<run directory>/run.json`; `None` when it is absent, unreadable as `read_json`
/// reads it, or not a run record.
pub(in crate::service) fn read_record(run_directory: &Path) -> Option<TreeRecord> {
    let record = read_json(run_directory, "run.json")?;
    let state = match record["state"].as_str()? {
        "running" => RecordState::Running,
        "waiting" => RecordState::Waiting,
        "exited" => RecordState::Exited,
        _ => return None,
    };
    let outcome = match &record["outcome"] {
        Value::Null => None,
        Value::String(outcome) => Some(outcome.clone()),
        _ => return None,
    };
    Some(TreeRecord {
        state,
        outcome,
        attempts: record["attempts"]
            .as_u64()
            .filter(|attempts| *attempts >= 1)?,
        position: record["position"].as_array()?.clone(),
        mirror: Vec::new(),
    })
}

/// The class of a failure to read a run's `mirror/`, which the run event records beside the
/// exit that reports no writes for it (REQ-RUNNERENROLL-99): a file there that is not a
/// write, or a repeated ordinal; a `mirror/` or a file in it of the wrong kind or size; a
/// file operation that failed; or any other refusal of the run directory.
pub(in crate::service) fn mirror_read_failure(error: &LocalRunDirectoryError) -> &'static str {
    match error {
        LocalRunDirectoryError::StateFile { source, .. } => match **source {
            LocalRunDirectoryError::MirrorWriteInvalid => "mirror_write_invalid",
            _ => "mirror_layout_invalid",
        },
        LocalRunDirectoryError::File { .. } => "mirror_file_unavailable",
        _ => "mirror_directory_unavailable",
    }
}

/// The class recorded when the blocking worker reading a run's `mirror/` did not return.
pub(in crate::service) const MIRROR_READER_LOST: &str = "mirror_reader_lost";

/// What `wait.json` reads: the wait's kind, start and step, and its rule's source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct TreeWait {
    pub(in crate::service) kind: String,
    pub(in crate::service) since: String,
    pub(in crate::service) step: Vec<Value>,
    pub(in crate::service) source: String,
}

impl TreeWait {
    /// The wait's identity `<kind>@<since>` (REQ-LOCALRUN-181).
    pub(in crate::service) fn identity(&self) -> String {
        wait_identity(&self.kind, &self.since)
    }

    /// Whether only a recorded control resolves it: a local kind's derived rule names
    /// the source `um`; any other source is an external condition (REQ-WFEXEC-61).
    pub(in crate::service) fn awaits_control(&self) -> bool {
        self.source == "um"
    }
}

/// Reads `<run directory>/wait.json`; `None` when it is absent, unreadable as `read_json`
/// reads it, or not a wait record.
pub(in crate::service) fn read_wait(run_directory: &Path) -> Option<TreeWait> {
    let wait = read_json(run_directory, "wait.json")?;
    Some(TreeWait {
        kind: wait["kind"].as_str()?.to_owned(),
        since: wait["since"].as_str()?.to_owned(),
        step: wait["step"].as_array()?.clone(),
        source: wait["rule"]["source"].as_str()?.to_owned(),
    })
}

/// How the attempt ends on the wire.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::service) enum TreeConclusion {
    /// A transport result, its gate transitions in order, and the root's exit.
    Terminal {
        transitions: Vec<Value>,
        result: TransportResult,
        exit: TreeExit,
    },
    /// No truthful result can be reported; the attempt is aborted with this cause.
    Unreportable(&'static str),
}

#[derive(Clone, Debug, PartialEq)]
pub(in crate::service) enum TransportResult {
    Succeeded,
    Failed {
        primary_issue: Value,
        /// The runner's safe facts about a failure it classified itself.
        diagnostic: Option<Value>,
    },
    Cancelled {
        reason: CancellationReason,
    },
    /// The runner's own shutdown or lease loss ended the run.
    Interrupted {
        reason: CancellationReason,
    },
}

impl TransportResult {
    /// The closed `WorkflowTerminalOutcome` the finish or interruption carries.
    pub(in crate::service) fn outcome(&self) -> Value {
        match self {
            Self::Succeeded => json!({ "outcome": "succeeded", "forceAbort": null }),
            Self::Failed { primary_issue, .. } => json!({
                "outcome": "failed",
                "primaryIssue": primary_issue,
                "forceAbort": null,
            }),
            Self::Cancelled { reason } | Self::Interrupted { reason } => json!({
                "outcome": "cancelled",
                "reason": reason.as_str(),
                "forceAbort": null,
            }),
        }
    }
}

/// Maps the record the resume loop ended on, the last `um` command's `--json` document
/// and the runner's applied cancellation to the attempt's report. The loop ends on a
/// record that does not read `exited`, or before `create` wrote one, only when the
/// runner's shutdown or lease loss stopped it, which is an interruption without an
/// outcome; any other unsettled or missing record has no truthful result.
pub(in crate::service) fn conclude(
    record: Option<&TreeRecord>,
    document: Option<&Value>,
    cancellation: Option<CancellationReason>,
) -> TreeConclusion {
    let record = match record {
        Some(record) if record.state == RecordState::Exited => record,
        // No exited record: the runner's shutdown or lease loss interrupted the attempt,
        // before `create` wrote the record or with the record as it stands.
        unsettled => {
            return match cancellation {
                Some(
                    reason @ (CancellationReason::RunnerShutdown
                    | CancellationReason::ExecutionLeaseExpired),
                ) => {
                    let result = TransportResult::Interrupted { reason };
                    TreeConclusion::Terminal {
                        transitions: gate_transitions(&result),
                        exit: unchanged_exit(),
                        result,
                    }
                }
                _ => TreeConclusion::Unreportable(match unsettled {
                    Some(_) => "tree_run_record_unsettled",
                    None => "tree_run_record_unavailable",
                }),
            };
        }
    };
    let result = match record.outcome.as_deref() {
        Some("failed") => {
            let Some(primary_issue) = document
                .map(|document| &document["result"]["primaryIssue"])
                .filter(|issue| issue.is_object())
            else {
                return TreeConclusion::Unreportable("tree_run_failure_unclassified");
            };
            TransportResult::Failed {
                primary_issue: primary_issue.clone(),
                diagnostic: None,
            }
        }
        Some("canceled" | "withdrawn") => cancelled_result(cancellation),
        _ => TransportResult::Succeeded,
    };
    TreeConclusion::Terminal {
        transitions: gate_transitions(&result),
        exit: recorded_exit(record),
        result,
    }
}

/// The result of a run the runner's `cancellation` ended: an interruption for the runner's
/// shutdown or lease loss, otherwise a cancellation, by force when the abort was forced.
fn cancelled_result(cancellation: Option<CancellationReason>) -> TransportResult {
    match cancellation {
        Some(
            reason @ (CancellationReason::RunnerShutdown
            | CancellationReason::ExecutionLeaseExpired),
        ) => TransportResult::Interrupted { reason },
        Some(CancellationReason::ForceAbort) => TransportResult::Cancelled {
            reason: CancellationReason::ForceAbort,
        },
        // A cancellation the runner did not apply came from the host itself.
        _ => TransportResult::Cancelled {
            reason: CancellationReason::UserRequest,
        },
    }
}

/// The exit of an attempt that changed nothing of its run: no chosen outcome, no mirror
/// write and no release request.
fn unchanged_exit() -> TreeExit {
    TreeExit {
        chosen_outcome: None,
        mirror: Vec::new(),
        claim_release: false,
    }
}

/// The exit `record` reports: its outcome as the chosen outcome, its run's mirror writes
/// and whether they confirm the runner's own departure.
fn recorded_exit(record: &TreeRecord) -> TreeExit {
    TreeExit {
        chosen_outcome: record.outcome.clone(),
        mirror: mirror_results(&record.mirror),
        claim_release: record.mirror.iter().any(confirms_departure),
    }
}

/// The writes as `MirrorResult`s, each its file without `schemaVersion`: the latest
/// `MAXIMUM_MIRROR_RESULTS` of them, so the outcome's write is always among them.
fn mirror_results(writes: &[MirrorWrite]) -> Vec<Value> {
    writes[writes.len().saturating_sub(MAXIMUM_MIRROR_RESULTS)..]
        .iter()
        .filter_map(|write| {
            let mut result = serde_json::to_value(write).ok()?;
            result.as_object_mut()?.remove("schemaVersion");
            Some(result)
        })
        .collect()
}

/// The runner's own confirmed departure (REQ-RUNNERENROLL-99): the outcome hook's state
/// write landed, or was skipped because its guard held, the issue already being elsewhere.
/// A failed write, whose effect is unknown, confirms nothing.
fn confirms_departure(write: &MirrorWrite) -> bool {
    write.hook == MirrorHook::Outcome
        && matches!(write.action, MirrorAction::State { .. })
        && match write.outcome {
            MirrorWriteOutcome::Written => true,
            MirrorWriteOutcome::Skipped => write.reason == Some(MirrorSkipReason::GuardHeld),
            MirrorWriteOutcome::Failed => false,
        }
}

/// How a run ended when the runner classifies the end itself rather than concluding the
/// record and the last command's document: an adopted run's (REQ-RUNNERENROLL-89), or a
/// created run's whose `create` the runtime refused (REQ-RUNNERENROLL-91).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) enum ClassifiedEnd {
    /// The adopted record already read `exited` `failed` and no command ran: the issue
    /// its current attempt recorded on its failed node, when it recorded one.
    RecordedFailure { primary_issue: Option<Value> },
    /// The runtime refused `command`, the run's first, with its first diagnostic's `code`,
    /// so `step` did not start: the first named step of a `resume --from`, or the root
    /// workflow's first step for a `create`.
    Refused {
        command: RefusedCommand,
        step: String,
        code: Option<String>,
    },
    /// `cancellation` reached the first `resume --from` before it started an engine
    /// attempt: the record still reads `exited` as the earlier attempt left it.
    Unstarted { cancellation: CancellationReason },
}

/// The run's first command the runtime refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum RefusedCommand {
    /// `um workflow run create`, which made no run.
    Create,
    /// An adopted run's `um workflow run resume --from`.
    Resume,
}

impl RefusedCommand {
    /// The stage of the refusal's diagnostic.
    fn stage(self) -> &'static str {
        match self {
            Self::Create => "tree_run_create_refused",
            Self::Resume => "tree_run_continuation_refused",
        }
    }
}

/// Concludes a classified `end`. A recorded failure is the attempt's own issue with
/// diagnostic stage `tree_run_adopted_failure`, its exit the record's as `conclude` reports
/// it, mirror writes and release request included; without an issue, or without the record,
/// it has no truthful result. A refused first command is the failure of its step to start,
/// with diagnostic stage `tree_run_create_refused` for a `create` and
/// `tree_run_continuation_refused` for a first resume and the runtime's code as
/// `declineCause`, no chosen outcome and no release request: the run did not change, or was
/// never made. Neither did one whose first resume a cancellation reached before any attempt
/// started: it is cancelled or interrupted as that cancellation reads, and nothing of the
/// record, which is the earlier attempt's, is reported.
pub(in crate::service) fn conclude_classified(
    end: &ClassifiedEnd,
    record: Option<&TreeRecord>,
) -> TreeConclusion {
    let (result, exit) = match end {
        ClassifiedEnd::RecordedFailure { primary_issue } => {
            let (Some(primary_issue), Some(record)) = (primary_issue, record) else {
                return TreeConclusion::Unreportable("tree_run_failure_unclassified");
            };
            (
                TransportResult::Failed {
                    primary_issue: primary_issue.clone(),
                    diagnostic: Some(json!({ "stage": "tree_run_adopted_failure" })),
                },
                recorded_exit(record),
            )
        }
        ClassifiedEnd::Refused {
            command,
            step,
            code,
        } => {
            let mut diagnostic = json!({ "stage": command.stage() });
            if let Some(code) = code.as_deref().filter(|code| is_diagnostic_slug(code)) {
                diagnostic["declineCause"] = json!(code);
            }
            (
                TransportResult::Failed {
                    primary_issue: json!({
                        "node": { "id": step, "role": "step" },
                        "state": "failed",
                        "detail": { "phase": "start", "code": "step_unavailable" },
                    }),
                    diagnostic: Some(diagnostic),
                },
                unchanged_exit(),
            )
        }
        ClassifiedEnd::Unstarted { cancellation } => {
            (cancelled_result(Some(*cancellation)), unchanged_exit())
        }
    };
    TreeConclusion::Terminal {
        transitions: gate_transitions(&result),
        exit,
        result,
    }
}

/// The issue `<run directory>/state.json` records for the current attempt: its first
/// node in state `failed` with a detail, as the primary issue a document would carry;
/// `None` when it records none or is unreadable as `read_json` reads it.
pub(in crate::service) fn read_failed_issue(run_directory: &Path) -> Option<Value> {
    let state = read_json(run_directory, "state.json")?;
    let current = state["currentAttemptNumber"].as_u64()?;
    let attempt = state["attempts"]
        .as_array()?
        .iter()
        .find(|attempt| attempt["attemptNumber"].as_u64() == Some(current))?;
    attempt["progress"]["steps"]
        .as_array()?
        .iter()
        .find(|step| {
            step["state"] == "failed"
                && step["id"].is_string()
                && matches!(step["role"].as_str(), Some("step" | "finalizer"))
                && step["detail"].is_object()
        })
        .map(|step| {
            json!({
                "node": { "id": step["id"], "role": step["role"] },
                "state": "failed",
                "detail": step["detail"],
            })
        })
}

/// The root's workflow-state transitions from the open gate to the terminal state the
/// result names, numbered from 1; a tree run reports no step transition.
fn gate_transitions(result: &TransportResult) -> Vec<Value> {
    let open = json!({ "state": "executing", "gate": "open" });
    let states = match result {
        TransportResult::Succeeded => {
            vec![open, json!({ "state": "succeeded", "forceAbort": null })]
        }
        TransportResult::Failed { primary_issue, .. } => vec![
            open,
            json!({ "state": "executing", "gate": "failure_stopped", "primaryIssue": primary_issue }),
            json!({ "state": "failed", "primaryIssue": primary_issue, "forceAbort": null }),
        ],
        TransportResult::Cancelled { reason } | TransportResult::Interrupted { reason } => vec![
            open,
            json!({ "state": "executing", "gate": "cancelling", "reason": reason.as_str() }),
            json!({ "state": "cancelled", "reason": reason.as_str(), "forceAbort": null }),
        ],
    };
    states
        .windows(2)
        .zip(1_u64..)
        .map(|(pair, sequence)| {
            json!({
                "eventVersion": 1,
                "eventType": "workflow_state_changed",
                "transitionSequence": sequence,
                "from": pair[0],
                "to": pair[1],
            })
        })
        .collect()
}
