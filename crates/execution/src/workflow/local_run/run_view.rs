//! A run read whole for a viewer: the run, its newest attempt's steps and finalizers as the
//! workflow declares them, and one step's output (REQ-RUNNERENROLL-144,
//! REQ-RUNNERENROLL-145). A tree run is read from its directory, with a settled step's
//! retained output; a run in this process without one is projected from the run interface's
//! model, with a step's newest records.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use time::OffsetDateTime;

use super::super::document::{FailurePolicy, Output};
use super::super::presentation_feed::{
    AgentPresentationHarness, WorkflowPresentationDefinition, WorkflowPresentationStep,
};
use super::super::publication::RecoveryDiagnosticKindV1;
use super::super::run_record::{RunState, WorkflowRunRecord};
use super::super::run_timing::ObservationClock;
use super::super::run_view_model::{
    WorkflowRunOutputDisposition, WorkflowRunStepLog, WorkflowRunStepView, WorkflowRunViewModel,
};
use super::super::runtime::WorkflowState;
use super::super::schema_common::{parse_canonical_utc_timestamp, utc_timestamp};
use super::super::validated::{WorkflowNodeRole, WorkflowValueType};
use super::{
    AttemptFinalizationV1, AttemptNodeRoleV1, AttemptStepStateV1, ChildRunDirectory,
    DurableInvocationDiagnosticV1, DurableInvocationStateV1, DurableInvocationV1, LocalAttemptV1,
    LocalRunDirectoryError, LocalStatusError, RUN_RECORD_FILE, RetainedOutputV1,
    RetainedReadBudget, attempt_state_name, attempt_state_of, child_run_directories,
    load_attempt_retained_execution_with_budget, read_durable_record, read_optional_run_record,
    read_optional_wait_record, read_run_file_tail, read_stable_local_run_snapshot,
};

/// One level of a child run's path below the run asked for: the `workflow` step, the
/// one-based fan-out element for a fan-out's child, and the child's ordinal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunViewPathElement {
    pub step: String,
    pub item: Option<u64>,
    pub ordinal: u64,
}

/// What `read_run_view` reads: the run at the end of `path` below the run directory, the
/// step whose retained output is wanted, the time durations are measured to, and the bounds
/// on the steps listed, the child runs listed per `workflow` step and the bytes of each output
/// window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunViewRequest {
    pub path: Vec<RunViewPathElement>,
    pub step: Option<String>,
    pub now: OffsetDateTime,
    pub maximum_steps: usize,
    pub maximum_child_runs: usize,
    pub maximum_output_bytes: u64,
}

/// A run read whole. The run record gives `workflow_path`, `state`, `attempts` and
/// `outcome`; the newest attempt gives `attempt_number`, `attempt_state` and the steps; the
/// root wait record gives `wait` while the run waits. Every vocabulary is the word the
/// engine writes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunView {
    pub workflow_path: String,
    pub state: String,
    pub attempts: u64,
    pub outcome: Option<String>,
    pub attempt_number: u64,
    pub attempt_state: String,
    pub wait: Option<RunViewWait>,
    /// The steps then the finalizers of the newest attempt in the definition's presentation
    /// order, the first `maximum_steps` of them.
    pub steps: Vec<RunViewStep>,
    pub steps_omitted: usize,
    pub output: RunViewStepOutput,
}

/// The root wait record's kind, start and waiting step position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunViewWait {
    pub kind: String,
    pub since: String,
    pub step: Vec<Value>,
}

/// One step or finalizer of the newest attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunViewStep {
    pub id: String,
    /// `step` or `finalizer`.
    pub role: String,
    pub state: String,
    pub failure_policy: String,
    /// The step's direct dependencies.
    pub needs: Vec<String>,
    /// The time the step first left `pending` in this attempt, as recorded.
    pub started_at: Option<String>,
    /// The time since `started_at` for a step in flight; for a settled one the time to the
    /// last finish among its invocations; none for a step that never started or has no
    /// finished invocation.
    pub duration: Option<Duration>,
    pub work: RunViewWork,
    /// Each declared output with its kind and whether the attempt committed it.
    pub outputs: Vec<RunViewOutput>,
}

/// What a step runs, as the workflow declares it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunViewWork {
    /// A `cmd` step: its argv and its declared `cwd`, never resolved against the host.
    Command {
        argv: Vec<String>,
        cwd: Option<String>,
    },
    /// An `agent` step. `effort` is the harness's level word: Pi's `thinking`, Claude Code's
    /// or Codex's `effort`.
    Agent {
        profile: String,
        harness: String,
        model: String,
        effort: String,
    },
    /// A `workflow` step: its file and its child runs in the engine's order (element, then
    /// ordinal), the last `maximum_child_runs` of them.
    Workflow {
        file: String,
        child_runs: Vec<RunViewChildRun>,
        child_runs_omitted: usize,
    },
}

impl RunViewWork {
    /// The step's kind as the workflow format spells it: `cmd`, `agent` or `workflow`.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Command { .. } => "cmd",
            Self::Agent { .. } => "agent",
            Self::Workflow { .. } => "workflow",
        }
    }
}

/// One child run of a `workflow` step: its ordinal, its fan-out element, and its record's
/// state and outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunViewChildRun {
    pub ordinal: u64,
    pub item: Option<u64>,
    pub state: String,
    pub outcome: Option<String>,
}

/// A declared output: its name, its kind, and whether the attempt committed it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunViewOutput {
    pub name: String,
    pub kind: String,
    pub committed: bool,
}

/// The output of the step the request names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunViewStepOutput {
    /// No step was named, or the step is not a step of the run; read from a run directory,
    /// also a step pending or in flight.
    NotGiven,
    /// The step is settled and no invocation of it retained output.
    SettledWithNone,
    Retained(RunViewRetainedOutput),
    /// A step's newest records as the run interface's model holds them, for a step of a run
    /// projected from the model, in any state.
    Observed(RunViewObservedOutput),
}

/// A step's newest whole records in the order observed, within the request's byte bound, and
/// the step's older records and their payload bytes, those the model discarded included.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunViewObservedOutput {
    pub records: Vec<RunViewRecord>,
    pub omitted_records: u64,
    pub omitted_bytes: u64,
}

/// One record of a step's output: its source's word (`stdout`, `stderr`, or an agent
/// observation's kind), its payload made visible, and whether it continues a record the
/// output was split into.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunViewRecord {
    pub source: String,
    pub text: String,
    pub continuation: bool,
}

/// The output of a settled step's newest ended invocation that retained any.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunViewRetainedOutput {
    pub invocation_id: u64,
    pub stdout: RunViewStream,
    pub stderr: RunViewStream,
    /// The step's earlier invocations that retained output.
    pub earlier_invocations_omitted: usize,
}

/// One retained stream: the record's retained and discarded byte counts, the last
/// `maximum_output_bytes` of the retained file, and the bytes of the file before them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunViewStream {
    pub retained_bytes: u64,
    pub discarded_bytes: u64,
    pub window: Vec<u8>,
    pub omitted_bytes: u64,
}

/// Where a read of a run directory failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunViewStage {
    ChildPath,
    RunRecord,
    WaitRecord,
    Definition,
    ChildRuns,
    ChildRunRecord,
    StepOutput,
}

impl RunViewStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ChildPath => "child_path",
            Self::RunRecord => "run_record",
            Self::WaitRecord => "wait_record",
            Self::Definition => "definition",
            Self::ChildRuns => "child_runs",
            Self::ChildRunRecord => "child_run_record",
            Self::StepOutput => "step_output",
        }
    }
}

#[derive(Debug)]
pub enum RunViewError {
    /// The run's `local-run.json` and `state.json` gave no stable snapshot.
    Snapshot(LocalStatusError),
    /// A file of the run could not be read.
    Read {
        stage: RunViewStage,
        source: LocalRunDirectoryError,
    },
    /// The run has no `run.json`: it was not admitted from a tree.
    RunRecordMissing { run_directory: PathBuf },
    /// The run's state records no attempt.
    AttemptMissing { run_directory: PathBuf },
    /// The path's element at zero-based `depth` names no child run of the run above it.
    ChildRunMissing { depth: usize },
}

impl fmt::Display for RunViewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(error) => formatter.write_str(error.code.message()),
            Self::Read { stage, source } => write!(formatter, "{}: {source}", stage.as_str()),
            Self::RunRecordMissing { .. } => formatter.write_str("the run has no run record"),
            Self::AttemptMissing { .. } => formatter.write_str("the run has no attempt"),
            Self::ChildRunMissing { depth } => {
                write!(formatter, "no child run at path element {depth}")
            }
        }
    }
}

impl std::error::Error for RunViewError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn at(stage: RunViewStage) -> impl FnOnce(LocalRunDirectoryError) -> RunViewError {
    move |source| RunViewError::Read { stage, source }
}

/// Reads the run at `request.path` below `run_directory`, running or ended, into a view, as
/// a reader outside the engine reads a run its own steps can write: through the engine's
/// bounded readers, following no symbolic link and waiting on no FIFO, taking no run lock
/// and writing nothing.
///
/// Call it only from a process that does not own the run. The run lock is a
/// process-associated `fcntl` lock that closing any descriptor of `run.lock` in the holding
/// process releases, and the read opens and closes one.
pub fn read_run_view(
    run_directory: &Path,
    request: &RunViewRequest,
) -> Result<RunView, RunViewError> {
    let directory = resolve_child_path(run_directory, &request.path)?;
    let snapshot = read_stable_local_run_snapshot(&directory).map_err(RunViewError::Snapshot)?;
    let record = read_optional_run_record(&snapshot.root)
        .map_err(at(RunViewStage::RunRecord))?
        .ok_or_else(|| RunViewError::RunRecordMissing {
            run_directory: snapshot.run_directory.clone(),
        })?;
    let wait = match record.state {
        RunState::Waiting => read_optional_wait_record(&snapshot.root)
            .map_err(at(RunViewStage::WaitRecord))?
            .map(|wait| RunViewWait {
                kind: wait.kind.name().to_owned(),
                since: wait.since,
                step: wait.step,
            }),
        RunState::Running | RunState::Exited => None,
    };
    let attempt = snapshot
        .state
        .attempts
        .last()
        .ok_or_else(|| RunViewError::AttemptMissing {
            run_directory: snapshot.run_directory.clone(),
        })?;
    let (workflow, _, _) = load_attempt_retained_execution_with_budget(
        &snapshot.root,
        &snapshot.run,
        &snapshot.state,
        attempt,
        &mut RetainedReadBudget::with_bytes(snapshot.retained_json_bytes)
            .map_err(at(RunViewStage::Definition))?,
    )
    .map_err(at(RunViewStage::Definition))?;
    let definition = WorkflowPresentationDefinition::from_workflow(&workflow);

    let listed = definition
        .presentation_order
        .iter()
        .take(request.maximum_steps)
        .collect::<Vec<_>>();
    let children = if listed.iter().any(|id| {
        matches!(
            definition.steps.get(*id),
            Some(WorkflowPresentationStep::Workflow { .. })
        )
    }) {
        child_run_directories(&snapshot.run_directory).map_err(at(RunViewStage::ChildRuns))?
    } else {
        Vec::new()
    };
    let mut steps = Vec::with_capacity(listed.len());
    for id in listed {
        let (Some(step), Some(role)) = (definition.steps.get(id), definition.node_roles.get(id))
        else {
            continue;
        };
        steps.push(step_entry(attempt, id, *role, step, &children, request)?);
    }
    let output = match &request.step {
        Some(id) => match definition.node_roles.get(id) {
            Some(role) => step_output(&snapshot.run_directory, attempt, id, *role, request)?,
            None => RunViewStepOutput::NotGiven,
        },
        None => RunViewStepOutput::NotGiven,
    };
    Ok(RunView {
        workflow_path: record.workflow,
        state: run_state_name(record.state).to_owned(),
        attempts: record.attempts,
        outcome: record.outcome,
        attempt_number: attempt.attempt_number,
        attempt_state: attempt_state_name(attempt.state).to_owned(),
        wait,
        steps_omitted: definition
            .presentation_order
            .len()
            .saturating_sub(steps.len()),
        steps,
        output,
    })
}

/// The directory of the child run `path` names below `run_directory`, one level at a time
/// through the engine's listing of each run's `children/`.
fn resolve_child_path(
    run_directory: &Path,
    path: &[RunViewPathElement],
) -> Result<PathBuf, RunViewError> {
    let mut directory = run_directory.to_owned();
    for (depth, element) in path.iter().enumerate() {
        directory = child_run_directories(&directory)
            .map_err(at(RunViewStage::ChildPath))?
            .into_iter()
            .find(|child| {
                child.step == element.step
                    && child.item == element.item
                    && child.ordinal == element.ordinal
            })
            .map(|child| child.directory)
            .ok_or(RunViewError::ChildRunMissing { depth })?;
    }
    Ok(directory)
}

/// A node's progress in an attempt: its state, recorded start and retained outputs.
struct NodeProgress<'a> {
    state: AttemptStepStateV1,
    started_at: Option<&'a str>,
    outputs: &'a [RetainedOutputV1],
}

/// `id`'s progress in `attempt`: a step's entry, a finalizer's while finalization is in
/// progress, a completed finalizer's state with no start, and `pending` for a node with no
/// entry yet.
fn node_progress<'a>(
    attempt: &'a LocalAttemptV1,
    id: &str,
    role: WorkflowNodeRole,
) -> NodeProgress<'a> {
    let entry = match (role, &attempt.finalization) {
        (WorkflowNodeRole::Step, _) => attempt.progress.steps.iter().find(|step| step.id == id),
        (WorkflowNodeRole::Finalizer, Some(AttemptFinalizationV1::Progress(progress))) => {
            progress.finalizers.iter().find(|step| step.id == id)
        }
        (WorkflowNodeRole::Finalizer, Some(AttemptFinalizationV1::Complete(complete))) => {
            return complete
                .finalizers
                .iter()
                .find(|finalizer| finalizer.id == id)
                .map_or(PENDING, |finalizer| NodeProgress {
                    state: finalizer.state,
                    started_at: None,
                    outputs: finalizer.outputs.as_deref().unwrap_or_default(),
                });
        }
        (WorkflowNodeRole::Finalizer, None) => None,
    };
    entry.map_or(PENDING, |step| NodeProgress {
        state: step.state,
        started_at: step.started_at.as_deref(),
        outputs: step.outputs.as_deref().unwrap_or_default(),
    })
}

const PENDING: NodeProgress<'static> = NodeProgress {
    state: AttemptStepStateV1::Pending,
    started_at: None,
    outputs: &[],
};

fn step_entry(
    attempt: &LocalAttemptV1,
    id: &str,
    role: WorkflowNodeRole,
    step: &WorkflowPresentationStep,
    children: &[ChildRunDirectory],
    request: &RunViewRequest,
) -> Result<RunViewStep, RunViewError> {
    let progress = node_progress(attempt, id, role);
    let (child_runs, child_runs_omitted) = match step {
        WorkflowPresentationStep::Workflow { .. } => child_runs(id, children, request)?,
        WorkflowPresentationStep::Command { .. } | WorkflowPresentationStep::Agent { .. } => {
            (Vec::new(), 0)
        }
    };
    let facts = StepFacts {
        state: step_state_name(progress.state),
        started_at: progress.started_at.map(str::to_owned),
        duration: duration(attempt, id, role, &progress, request.now),
        child_runs,
        child_runs_omitted,
    };
    Ok(step_view(id, role, step, facts, |name| {
        progress
            .outputs
            .iter()
            .any(|retained| retained.name() == name)
    }))
}

/// What a step's entry takes from where its progress was read: its state word, its start and
/// duration, and a `workflow` step's child runs.
struct StepFacts {
    state: &'static str,
    started_at: Option<String>,
    duration: Option<Duration>,
    child_runs: Vec<RunViewChildRun>,
    child_runs_omitted: usize,
}

/// A step's entry: the declared step `id` with the `facts` of its progress, each declared
/// output marked committed when `committed` says so.
fn step_view(
    id: &str,
    role: WorkflowNodeRole,
    step: &WorkflowPresentationStep,
    facts: StepFacts,
    committed: impl Fn(&str) -> bool,
) -> RunViewStep {
    let work = match step {
        WorkflowPresentationStep::Command { argv, cwd, .. } => RunViewWork::Command {
            argv: argv.clone(),
            cwd: cwd.clone(),
        },
        WorkflowPresentationStep::Agent {
            profile, harness, ..
        } => {
            let (harness, model, effort) = match harness {
                AgentPresentationHarness::Pi { model, thinking } => {
                    ("pi", model, thinking.as_str())
                }
                AgentPresentationHarness::ClaudeCode { model, effort } => {
                    ("claude_code", model, effort.as_str())
                }
                AgentPresentationHarness::Codex { model, effort } => {
                    ("codex", model, effort.as_str())
                }
            };
            RunViewWork::Agent {
                profile: profile.clone(),
                harness: harness.to_owned(),
                model: model.clone(),
                effort: effort.to_owned(),
            }
        }
        WorkflowPresentationStep::Workflow { file, .. } => RunViewWork::Workflow {
            file: file.clone(),
            child_runs: facts.child_runs,
            child_runs_omitted: facts.child_runs_omitted,
        },
    };
    RunViewStep {
        id: id.to_owned(),
        role: match role {
            WorkflowNodeRole::Step => "step",
            WorkflowNodeRole::Finalizer => "finalizer",
        }
        .to_owned(),
        state: facts.state.to_owned(),
        failure_policy: match step.failure_policy() {
            FailurePolicy::Required => "required",
            FailurePolicy::Advisory => "advisory",
        }
        .to_owned(),
        needs: step.direct_dependencies().to_vec(),
        started_at: facts.started_at,
        duration: facts.duration,
        work,
        outputs: step
            .outputs()
            .iter()
            .map(|(name, output)| RunViewOutput {
                name: name.clone(),
                kind: output_kind(output).to_owned(),
                committed: committed(name),
            })
            .collect(),
    }
}

/// The last `maximum_child_runs` child runs of the step `id` in the listing's order, each
/// with its record, and the number of earlier ones left out.
fn child_runs(
    id: &str,
    children: &[ChildRunDirectory],
    request: &RunViewRequest,
) -> Result<(Vec<RunViewChildRun>, usize), RunViewError> {
    let own = children
        .iter()
        .filter(|child| child.step == id)
        .collect::<Vec<_>>();
    let omitted = own.len().saturating_sub(request.maximum_child_runs);
    let runs = own
        .into_iter()
        .skip(omitted)
        .map(|child| {
            let bytes = read_durable_record(&child.directory, RUN_RECORD_FILE)
                .map_err(at(RunViewStage::ChildRunRecord))?;
            let record = serde_json::from_slice::<WorkflowRunRecord>(&bytes).map_err(|source| {
                RunViewError::Read {
                    stage: RunViewStage::ChildRunRecord,
                    source: LocalRunDirectoryError::Json {
                        operation: "decode child run record",
                        source,
                    },
                }
            })?;
            Ok(RunViewChildRun {
                ordinal: child.ordinal,
                item: child.item,
                state: run_state_name(record.state).to_owned(),
                outcome: record.outcome,
            })
        })
        .collect::<Result<Vec<_>, RunViewError>>()?;
    Ok((runs, omitted))
}

/// The time since a node's start at `now` while it is in flight, and for a settled node the
/// time from its start to the last finish among its invocations.
fn duration(
    attempt: &LocalAttemptV1,
    id: &str,
    role: WorkflowNodeRole,
    progress: &NodeProgress<'_>,
    now: OffsetDateTime,
) -> Option<Duration> {
    let started = parse_canonical_utc_timestamp(progress.started_at?)?;
    let end = if progress.state.is_in_flight() {
        now
    } else if progress.state == AttemptStepStateV1::Pending {
        return None;
    } else {
        invocations_of(attempt, id, role)
            .filter_map(|invocation| {
                parse_canonical_utc_timestamp(invocation.finished_at.as_deref()?)
            })
            .max()?
    };
    Some(Duration::try_from(end - started).unwrap_or(Duration::ZERO))
}

fn invocations_of<'a>(
    attempt: &'a LocalAttemptV1,
    id: &'a str,
    role: WorkflowNodeRole,
) -> impl Iterator<Item = &'a DurableInvocationV1> + 'a {
    let role = match role {
        WorkflowNodeRole::Step => AttemptNodeRoleV1::Step,
        WorkflowNodeRole::Finalizer => AttemptNodeRoleV1::Finalizer,
    };
    attempt
        .progress
        .invocations
        .iter()
        .filter(move |invocation| invocation.step_id == id && invocation.node_role == role)
}

/// The retained output of the node `id` when it is settled: its newest ended invocation that
/// retained any, read through the bounded tail reader, with its earlier ones counted.
fn step_output(
    run_directory: &Path,
    attempt: &LocalAttemptV1,
    id: &str,
    role: WorkflowNodeRole,
    request: &RunViewRequest,
) -> Result<RunViewStepOutput, RunViewError> {
    let state = node_progress(attempt, id, role).state;
    if state == AttemptStepStateV1::Pending || state.is_in_flight() {
        return Ok(RunViewStepOutput::NotGiven);
    }
    let mut retained = invocations_of(attempt, id, role)
        .filter(|invocation| {
            invocation.state != DurableInvocationStateV1::Active
                && !invocation.diagnostics.is_empty()
        })
        .collect::<Vec<_>>();
    retained.sort_by_key(|invocation| invocation.invocation_id);
    let Some(newest) = retained.pop() else {
        return Ok(RunViewStepOutput::SettledWithNone);
    };
    let stream = |kinds: [RecoveryDiagnosticKindV1; 2]| {
        newest
            .diagnostics
            .iter()
            .find(|diagnostic| kinds.contains(&diagnostic.kind))
            .map(|diagnostic| read_stream(run_directory, diagnostic, request))
            .transpose()
            .map(Option::unwrap_or_default)
    };
    Ok(RunViewStepOutput::Retained(RunViewRetainedOutput {
        invocation_id: newest.invocation_id,
        stdout: stream([
            RecoveryDiagnosticKindV1::CommandStdout,
            RecoveryDiagnosticKindV1::AgentHarnessStdout,
        ])?,
        stderr: stream([
            RecoveryDiagnosticKindV1::CommandStderr,
            RecoveryDiagnosticKindV1::AgentHarnessStderr,
        ])?,
        earlier_invocations_omitted: retained.len(),
    }))
}

fn read_stream(
    run_directory: &Path,
    diagnostic: &DurableInvocationDiagnosticV1,
    request: &RunViewRequest,
) -> Result<RunViewStream, RunViewError> {
    let tail = read_run_file_tail(
        run_directory,
        &diagnostic.reference,
        request.maximum_output_bytes,
    )
    .map_err(at(RunViewStage::StepOutput))?;
    Ok(RunViewStream {
        retained_bytes: diagnostic.retained_bytes,
        discarded_bytes: diagnostic.discarded_bytes,
        window: tail.bytes,
        omitted_bytes: tail.omitted_bytes,
    })
}

/// What `live_run_view` projects: the step whose records are wanted and the bounds on the
/// steps listed and on the bytes of the records given.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveRunViewRequest {
    pub step: Option<String>,
    pub maximum_steps: usize,
    pub maximum_output_bytes: u64,
}

/// A run in this process projected from the run interface's model into the view a run
/// directory's read answers: `running` until the engine has ended the run and `exited`
/// after, one attempt, its state the engine's word for where the run is, no wait and no
/// outcome, and each step as the model has observed it. A named step of the run gives its
/// newest records; no step and a step the run does not have give none
/// (REQ-RUNNERENROLL-144, REQ-RUNNERENROLL-145).
///
/// The model's lock is held once, to copy the snapshot, and the snapshot shares the ring of
/// the named step alone; it is dropped before this returns.
pub fn live_run_view<Clock: ObservationClock>(
    model: &WorkflowRunViewModel<Clock>,
    request: &LiveRunViewRequest,
) -> RunView {
    let (snapshot, selected) = model.snapshot_for_step(request.step.as_deref());
    let output = selected.and_then(|index| snapshot.steps.get(index)).map_or(
        RunViewStepOutput::NotGiven,
        |step| {
            RunViewStepOutput::Observed(observed_output(&step.log, request.maximum_output_bytes))
        },
    );
    let steps = snapshot
        .steps
        .iter()
        .take(request.maximum_steps)
        .map(live_step_entry)
        .collect::<Vec<_>>();
    let state = match snapshot.workflow {
        WorkflowState::Executing { .. } | WorkflowState::Finalizing { .. } => "running",
        WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. }
        | WorkflowState::Waiting { .. }
        | WorkflowState::Continued { .. } => "exited",
    };
    let attempt_state = attempt_state_name(attempt_state_of(&snapshot.workflow));
    let steps_omitted = snapshot.steps.len().saturating_sub(steps.len());
    let workflow_path = snapshot.workflow_path;
    RunView {
        workflow_path,
        state: state.to_owned(),
        attempts: 1,
        outcome: None,
        attempt_number: 1,
        attempt_state: attempt_state.to_owned(),
        wait: None,
        steps,
        steps_omitted,
        output,
    }
}

/// The steps in flight of a run in this process (REQ-RUNNERENROLL-134).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LiveRunningSteps {
    /// The first steps in flight in the definition's presentation order.
    pub steps: Vec<LiveRunningStep>,
    pub omitted: usize,
}

/// A step in flight: its ID and its state's word.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveRunningStep {
    pub id: String,
    pub state: String,
}

/// The first `maximum` of the model's steps that are `starting`, `running`,
/// `capturing_outputs`, `recovering` or `cancelling`, in presentation order, and the number
/// of the others, copied under the model's lock without any step's definition.
pub fn live_running_steps<Clock: ObservationClock>(
    model: &WorkflowRunViewModel<Clock>,
    maximum: usize,
) -> LiveRunningSteps {
    let (steps, omitted) = model.steps_in_flight(maximum);
    LiveRunningSteps {
        steps: steps
            .into_iter()
            .map(|(id, state)| LiveRunningStep {
                id,
                state: state.as_str().to_owned(),
            })
            .collect(),
        omitted,
    }
}

/// A step as the model has observed it: its start in canonical UTC and its elapsed time,
/// none before it started; an output committed when the step committed it. A `workflow`
/// step lists no child runs.
fn live_step_entry(step: &WorkflowRunStepView) -> RunViewStep {
    let facts = StepFacts {
        state: step.state.as_str(),
        started_at: step
            .timing
            .as_ref()
            .and_then(|timing| utc_timestamp(timing.started_at).ok()),
        duration: step.timing.as_ref().map(|timing| timing.duration),
        child_runs: Vec::new(),
        child_runs_omitted: 0,
    };
    step_view(&step.id, step.role, &step.definition, facts, |name| {
        step.outputs.get(name) == Some(&WorkflowRunOutputDisposition::Committed)
    })
}

/// The newest whole records of `log` whose payloads total at most `maximum_bytes`, with the
/// older records the model discarded or this leaves out counted.
fn observed_output(log: &WorkflowRunStepLog, maximum_bytes: u64) -> RunViewObservedOutput {
    let mut given_bytes = 0_u64;
    let given = log
        .records
        .iter()
        .rev()
        .take_while(|record| {
            let bytes = u64::try_from(record.payload.len()).unwrap_or(u64::MAX);
            let total = given_bytes.saturating_add(bytes);
            let fits = total <= maximum_bytes;
            if fits {
                given_bytes = total;
            }
            fits
        })
        .count();
    let left_out = log.records.len().saturating_sub(given);
    RunViewObservedOutput {
        records: log
            .records
            .iter()
            .skip(left_out)
            .map(|record| RunViewRecord {
                source: record.source.word().to_owned(),
                text: record.payload.as_ref().to_owned(),
                continuation: record.continuation,
            })
            .collect(),
        omitted_records: log
            .discarded_records
            .saturating_add(u64::try_from(left_out).unwrap_or(u64::MAX)),
        omitted_bytes: log
            .discarded_bytes
            .saturating_add(log.retained_bytes.saturating_sub(given_bytes)),
    }
}

const fn run_state_name(state: RunState) -> &'static str {
    match state {
        RunState::Running => "running",
        RunState::Waiting => "waiting",
        RunState::Exited => "exited",
    }
}

/// A node state as `state.json` spells it.
const fn step_state_name(state: AttemptStepStateV1) -> &'static str {
    match state {
        AttemptStepStateV1::Pending => "pending",
        AttemptStepStateV1::Starting => "starting",
        AttemptStepStateV1::Running => "running",
        AttemptStepStateV1::CapturingOutputs => "capturing_outputs",
        AttemptStepStateV1::Cancelling => "cancelling",
        AttemptStepStateV1::Succeeded => "succeeded",
        AttemptStepStateV1::Inherited => "inherited",
        AttemptStepStateV1::Failed => "failed",
        AttemptStepStateV1::Blocked => "blocked",
        AttemptStepStateV1::Skipped => "skipped",
        AttemptStepStateV1::NotRun => "not_run",
        AttemptStepStateV1::Cancelled => "cancelled",
    }
}

/// A declared output's kind as the workflow format spells it.
const fn output_kind(output: &Output) -> &'static str {
    let value_type = match output {
        Output::TextPath { .. } | Output::TextAgentResponse | Output::ChildOutcome => {
            WorkflowValueType::Text
        }
        Output::JsonPath { .. } | Output::JsonAgentResult { .. } => WorkflowValueType::Json,
        Output::FilePath { .. } => WorkflowValueType::File,
        Output::GitBranchWorkspace => WorkflowValueType::GitBranch,
        Output::ChildExport { value_type } => *value_type,
    };
    match value_type {
        WorkflowValueType::Text => "text",
        WorkflowValueType::Json => "json",
        WorkflowValueType::File => "file",
        WorkflowValueType::GitBranch => "git_branch",
        WorkflowValueType::AttachmentCollection => "attachments",
    }
}
