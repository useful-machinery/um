//! Reading the local run and wait records a tree run leaves (REQ-LOCALRUN-175,
//! REQ-LOCALRUN-178) and the terminal report they map to (REQ-RUNNERENROLL-99): the
//! transport result is `cancelled` when the record's outcome is the engine's `canceled` or
//! `withdrawn`, and otherwise the outcome of the engine's published result (REQ-WFEXEC-15),
//! read from the last `--json` document or, when that carries none, from the run
//! directory's record of the current attempt, a failure there with the primary issue of the
//! `result.json` that attempt published and never a step the runner selects itself; the
//! record's outcome, the workflow's choice, travels as `chosenOutcome` and never selects
//! the result, its run's mirror writes, every attempt's, as `mirror` and the departure the
//! attempt's own outcome write confirmed as `claimRelease`. An adopted run
//! (REQ-RUNNERENROLL-89) whose record already read `exited` is concluded by the result its
//! current attempt recorded, and one whose first `resume --from` the runtime refused ends
//! `failed` with the refusal's code, as does a run whose `create` the runtime refused
//! (REQ-RUNNERENROLL-91).

use std::path::Path;

use serde_json::{Map, Value, json};
use useful_execution::{
    CancellationReason, FailingNodeState, LocalRunDirectoryError, MirrorAction, MirrorHook,
    MirrorSkipReason, MirrorWrite, MirrorWriteOutcome, WaitQuestions, WorkflowNodeRole,
    read_durable_record, wait_failed_child, wait_identity, wait_questions,
};
use useful_runner_protocol::{
    TreeExit, TreeExport, TreeProgressFailedChild, TreeProgressFailedChildRole,
    TreeProgressFailedChildState, TreeProgressWaitQuestion, TreeProgressWaitQuestions,
};
use useful_support::is_diagnostic_slug;

/// The most mirror results one finish carries, the protocol's bound.
const MAXIMUM_MIRROR_RESULTS: usize = 1024;

/// Reads `<run directory>/<name>` as JSON through the engine's own reader of its durable
/// records (`useful_execution::read_durable_record`), `name` a path beneath the run directory:
/// a regular file opened, as each directory on the way to it, without following a symbolic
/// link and without waiting on a FIFO, of at most the engine's bound; `None` for a file
/// that is absent, behind a link, of another kind, larger or not JSON.
pub(super) fn read_json(run_directory: &Path, name: &str) -> Option<Value> {
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
    /// The run's `mirror/` writes in order (REQ-LOCALRUN-193), every attempt's, read only
    /// for the record the loop ends on; empty while the loop observes the record.
    pub(in crate::service) mirror: Vec<MirrorWrite>,
    /// How many leading writes of `mirror` an earlier attempt of the run made: those
    /// `mirror/` held when the runner took the run, or all of them when that count could
    /// not be read (REQ-RUNNERENROLL-99). Zero while the loop observes the record.
    pub(in crate::service) earlier_writes: usize,
    /// The root's text exports the exit lists (REQ-RUNNERENROLL-131), read only for the
    /// record the loop ends on, when it exits with a workflow outcome the runner reports
    /// from the record; empty while the loop observes the record.
    pub(in crate::service) exports: Vec<TreeExport>,
}

impl TreeRecord {
    /// Whether the record's outcome is the engine's own cancellation, `canceled` or
    /// `withdrawn`, which no workflow can declare (REQ-WFFORMAT-87).
    pub(in crate::service) fn reads_cancellation(&self) -> bool {
        matches!(self.outcome.as_deref(), Some("canceled" | "withdrawn"))
    }
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
        earlier_writes: 0,
        exports: Vec::new(),
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

/// What `wait.json` reads: the wait's kind, start and step, its rule's source, for an
/// `answer` wait the questions it reports (REQ-RUNNERENROLL-119) and, for a wait of any
/// kind, the failed child it reports (REQ-RUNNERENROLL-125).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct TreeWait {
    pub(in crate::service) kind: String,
    pub(in crate::service) since: String,
    pub(in crate::service) step: Vec<Value>,
    pub(in crate::service) source: String,
    /// `None` when the wait reports no questions: another kind, a payload that is not an
    /// object, or questions outside the bounds Cloud admits.
    pub(in crate::service) questions: Option<TreeProgressWaitQuestions>,
    /// `None` when the wait reports no failed child: a payload that is not an object, holds
    /// no `failedChild` or one that does not read as the engine's record, or a record outside
    /// the bounds Cloud admits.
    pub(in crate::service) failed_child: Option<TreeProgressFailedChild>,
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
/// reads it, or not a wait record. Neither a question nor a failed child makes it `None`.
pub(in crate::service) fn read_wait(run_directory: &Path) -> Option<TreeWait> {
    let wait = read_json(run_directory, "wait.json")?;
    let kind = wait["kind"].as_str()?.to_owned();
    let payload = wait["payload"].as_object();
    let questions = if kind == "answer" {
        payload.and_then(answer_questions)
    } else {
        None
    };
    let failed_child = payload.and_then(reported_failed_child);
    Some(TreeWait {
        kind,
        since: wait["since"].as_str()?.to_owned(),
        step: wait["step"].as_array()?.clone(),
        source: wait["rule"]["source"].as_str()?.to_owned(),
        questions,
        failed_child,
    })
}

/// The questions an `answer` wait's `payload` asks, read as `useful workflow run answer` reads
/// them (REQ-RUNNERENROLL-119): the listed questions, or the one question with id `1` that a
/// payload without a list asks; `None` when they break a bound Cloud admits, so the wait is
/// reported without them and nothing is truncated.
fn answer_questions(payload: &Map<String, Value>) -> Option<TreeProgressWaitQuestions> {
    let questions = match wait_questions(payload) {
        WaitQuestions::Listed(questions) => questions
            .into_iter()
            .map(|question| TreeProgressWaitQuestion {
                id: question.id,
                text: question.text,
            })
            .collect(),
        WaitQuestions::Unlisted { text } => vec![TreeProgressWaitQuestion {
            id: "1".to_owned(),
            text,
        }],
    };
    TreeProgressWaitQuestions::new(questions)
}

/// The failed child a wait's `payload` records, read as `useful workflow run status` reads
/// it (REQ-RUNNERENROLL-125) and reduced to the outcome, the failing node's `id` and role,
/// its state and, for a failed node, its phase and code; `None` when the payload records
/// none or the reduction breaks a bound Cloud admits, so the wait is reported without it
/// and nothing is truncated.
fn reported_failed_child(payload: &Map<String, Value>) -> Option<TreeProgressFailedChild> {
    let record = wait_failed_child(payload)?;
    let node = record.failing_node();
    let role = match node.role {
        WorkflowNodeRole::Step => TreeProgressFailedChildRole::Step,
        WorkflowNodeRole::Finalizer => TreeProgressFailedChildRole::Finalizer,
    };
    let (state, phase, code) = match node.state {
        FailingNodeState::Failed { phase, code } => (
            TreeProgressFailedChildState::Failed,
            Some(phase.to_owned()),
            code,
        ),
        FailingNodeState::Blocked => (TreeProgressFailedChildState::Blocked, None, None),
    };
    TreeProgressFailedChild::new(
        record.outcome().to_owned(),
        node.id,
        role,
        state,
        phase,
        code,
    )
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

/// The engine's published result for the attempt that exited the run (REQ-WFEXEC-15), as
/// the runner read it, and whether it read it from the run directory's record of that
/// attempt rather than from a command's `--json` document.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::service) struct EngineResult {
    pub(in crate::service) outcome: EngineOutcome,
    pub(in crate::service) recorded: bool,
}

/// The settled outcome of an engine result.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::service) enum EngineOutcome {
    Succeeded,
    /// A failure, with its primary issue when the result names one.
    Failed {
        primary_issue: Option<Value>,
    },
    Cancelled,
}

impl EngineResult {
    /// The settled result a command's `--json` `document` carries (REQ-LOCALRUN-127): its
    /// `result.outcome` `succeeded`, `failed`, with `result.primaryIssue` when that is an
    /// object, or `cancelled`; `None` for a `waiting` or `continued` result, a refusal, or a
    /// document with no result.
    pub(in crate::service) fn from_document(document: &Value) -> Option<Self> {
        Some(Self {
            outcome: settled_outcome(&document["result"])?,
            recorded: false,
        })
    }
}

/// The settled outcome an engine result reads, whether a document carries it or the run
/// directory holds it as an attempt's `result.json`: its `outcome` `succeeded`, `failed`,
/// with its `primaryIssue` when that is an object, or `cancelled`; `None` for any other
/// outcome.
fn settled_outcome(result: &Value) -> Option<EngineOutcome> {
    Some(match result["outcome"].as_str()? {
        "succeeded" => EngineOutcome::Succeeded,
        "failed" => EngineOutcome::Failed {
            primary_issue: Some(&result["primaryIssue"])
                .filter(|issue| issue.is_object())
                .cloned(),
        },
        "cancelled" => EngineOutcome::Cancelled,
        _ => return None,
    })
}

/// The result `<run directory>/state.json` records for the run's current attempt, counted
/// only once the attempt's result is recorded `published`, which the engine does before the
/// run's record reads `exited`: its state `succeeded`, `workflow_failed`, a failure whose
/// primary issue is that of the result the attempt published (REQ-WFEXEC-12), as a
/// document would carry it, or `cancelled`. `None` for any other state, a result not
/// recorded published, or a file `read_json` cannot read.
pub(in crate::service) fn read_recorded_result(run_directory: &Path) -> Option<EngineResult> {
    let state = read_json(run_directory, "state.json")?;
    let attempt = published_attempt(&state)?;
    let outcome = match attempt["state"].as_str()? {
        "succeeded" => EngineOutcome::Succeeded,
        "workflow_failed" => EngineOutcome::Failed {
            primary_issue: published_issue(run_directory, &attempt["result"]),
        },
        "cancelled" => EngineOutcome::Cancelled,
        _ => return None,
    };
    Some(EngineResult {
        outcome,
        recorded: true,
    })
}

/// The run's current attempt as `state` records it, the attempt whose `attemptNumber` is
/// `currentAttemptNumber`, once its result is recorded `published`; `None` otherwise.
pub(super) fn published_attempt(state: &Value) -> Option<&Value> {
    let current = state["currentAttemptNumber"].as_u64()?;
    state["attempts"]
        .as_array()?
        .iter()
        .find(|attempt| attempt["attemptNumber"].as_u64() == Some(current))
        .filter(|attempt| attempt["result"]["status"] == "published")
}

/// The primary issue of the result an attempt recorded `published`: the `primaryIssue` of
/// `<relativeDirectory>/result.json` beneath the run directory when that result reads
/// `failed`, read as a document's result is read. `None` with no `relativeDirectory`, a
/// file `read_json` cannot read, another outcome or no issue: the runner selects no issue
/// from the attempt's nodes.
fn published_issue(run_directory: &Path, published: &Value) -> Option<Value> {
    let directory = published["relativeDirectory"].as_str()?;
    let result = read_json(run_directory, &format!("{directory}/result.json"))?;
    match settled_outcome(&result)? {
        EngineOutcome::Failed { primary_issue } => primary_issue,
        EngineOutcome::Succeeded | EngineOutcome::Cancelled => None,
    }
}

/// Maps the record the resume loop ended on, the engine's result for the attempt that
/// exited it and the runner's applied cancellation to the attempt's report. An exited
/// record whose outcome is the engine's `canceled` or `withdrawn` is a cancellation whatever
/// any result reads; otherwise the engine's result is the transport result and the record's
/// outcome only the chosen one: `succeeded`, `failed` with its primary issue, carrying
/// diagnostic stage `tree_run_adopted_failure` when it was read from the run's record, or
/// `cancelled`. No engine result, or a failure without its issue, has no truthful result.
/// The loop ends on a record that does not read `exited`, or before `create` wrote one,
/// only when the runner's shutdown or lease loss stopped it, which is an interruption
/// without an outcome; any other unsettled or missing record has no truthful result.
pub(in crate::service) fn conclude(
    record: Option<&TreeRecord>,
    engine: Option<&EngineResult>,
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
    let result = if record.reads_cancellation() {
        cancelled_result(cancellation)
    } else {
        match engine.map(|engine| (&engine.outcome, engine.recorded)) {
            Some((EngineOutcome::Succeeded, _)) => TransportResult::Succeeded,
            Some((
                EngineOutcome::Failed {
                    primary_issue: Some(primary_issue),
                },
                recorded,
            )) => TransportResult::Failed {
                primary_issue: primary_issue.clone(),
                diagnostic: recorded.then(|| json!({ "stage": "tree_run_adopted_failure" })),
            },
            Some((EngineOutcome::Cancelled, _)) => cancelled_result(cancellation),
            Some((
                EngineOutcome::Failed {
                    primary_issue: None,
                },
                _,
            ))
            | None => return TreeConclusion::Unreportable("tree_run_failure_unclassified"),
        }
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
/// write, no release request and no export.
fn unchanged_exit() -> TreeExit {
    TreeExit {
        chosen_outcome: None,
        mirror: Vec::new(),
        claim_release: false,
        exports: Vec::new(),
    }
}

/// The exit `record` reports: its outcome as the chosen outcome, its run's mirror writes,
/// every attempt's, whether a write past the earlier attempts' confirms the runner's own
/// departure, and the root's text exports read for it (REQ-RUNNERENROLL-131).
fn recorded_exit(record: &TreeRecord) -> TreeExit {
    TreeExit {
        chosen_outcome: record.outcome.clone(),
        mirror: mirror_results(&record.mirror),
        claim_release: record
            .mirror
            .iter()
            .skip(record.earlier_writes)
            .any(confirms_departure),
        exports: record.exports.clone(),
    }
}

/// The writes as `MirrorResult`s, each its file without `schemaVersion` and without a
/// `reason` the protocol does not name (`travels`): the latest `MAXIMUM_MIRROR_RESULTS` of
/// them, so the outcome's write is always among them.
fn mirror_results(writes: &[MirrorWrite]) -> Vec<Value> {
    writes[writes.len().saturating_sub(MAXIMUM_MIRROR_RESULTS)..]
        .iter()
        .filter_map(|write| {
            let mut result = serde_json::to_value(write).ok()?;
            let result_object = result.as_object_mut()?;
            result_object.remove("schemaVersion");
            if !write.reason.is_none_or(travels) {
                result_object.remove("reason");
            }
            Some(result)
        })
        .collect()
}

/// Whether the runner protocol's `MirrorResult` names `reason`, so that it travels with its
/// write (REQ-RUNNERENROLL-99): a reason the protocol does not name would have Cloud refuse
/// the frame, so its write travels `skipped` with no `reason`.
const fn travels(reason: MirrorSkipReason) -> bool {
    match reason {
        MirrorSkipReason::GuardHeld => true,
        MirrorSkipReason::StartAnnounced => false,
    }
}

/// Whether `write` confirms a departure (REQ-RUNNERENROLL-99): the outcome hook's state
/// write landed, or was skipped because its guard held, the issue already being elsewhere.
/// A failed write, whose effect is unknown, confirms nothing, and only a write the attempt
/// itself made confirms the runner's own departure (`recorded_exit`).
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
/// record and the engine's result: an adopted run's (REQ-RUNNERENROLL-89), or a created
/// run's whose `create` the runtime refused (REQ-RUNNERENROLL-91).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) enum ClassifiedEnd {
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
    /// `useful workflow run create`, which made no run.
    Create,
    /// An adopted run's `useful workflow run resume --from`.
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

/// Concludes a classified `end`, whatever any engine result reads. A refused first command
/// is the failure of its step to start, with diagnostic stage `tree_run_create_refused` for
/// a `create` and `tree_run_continuation_refused` for a first resume and the runtime's code
/// as `declineCause`, no chosen outcome and no release request: the run did not change, or
/// was never made. Neither did one whose first resume a cancellation reached before any
/// attempt started: it is cancelled or interrupted as that cancellation reads, and nothing
/// of the record, which is the earlier attempt's, is reported.
pub(in crate::service) fn conclude_classified(end: &ClassifiedEnd) -> TreeConclusion {
    let (result, exit) = match end {
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
