use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, ready};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use time::OffsetDateTime;
use tokio::sync::watch;
use unicode_width::UnicodeWidthStr;

use super::admission::CancellationReason;
use super::archived_attempt::presented_step_state;
use super::evidence::FailedChild;
use super::observation::{
    CommandOutputSource, ExecutionObservation, ExecutionObserver, ObservedStepTransition,
};
use super::presentation_feed::{
    AcceptedRecordOrder, AgentPresentationObservationKind, DisplayDeadline,
    MAX_NORMALIZED_CHILD_RECORD_BYTES, NormalizedAgentObservation, NormalizedChildOutput,
    PresentationRecord, PresentationRecordKind, PresentationTransition, WorkflowPresentationFeed,
    WorkflowPresentationStep,
};
use super::publication::{
    LocalPublicationError, LocalPublicationFailureKind, LocalPublicationPhase,
    WorkflowRunFinalization, WorkflowRunResult, WorkflowRunTiming, WorkflowStepTiming,
};
use super::resolution::ResolvedWorkflow;
use super::run_timing::{
    ObservationClock, ObservationTime, RunTimingObservation, RunTimingSnapshot,
};
use super::runtime::{
    ExecutionSeed, InheritedDisposition, RunOutcome, SchedulingGate, StepState, StepStateKind,
    TransitionEvent, WorkflowState,
};
use super::validated::WorkflowNodeRole;

const MAXIMUM_LOG_RECORDS_PER_STEP: usize = 4_096;
const RUN_LOG_RECORD_BUDGET: usize = 262_144;

/// The bounds on what a model keeps of its steps' output: each step keeps at most
/// `step_records` records of at most `step_bytes` payload bytes, and the run at most
/// `run_records` and `run_bytes` over its steps and finalizers (REQ-LOCALRUN-38,
/// REQ-LOCALRUN-39, REQ-RUNNERENROLL-147).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunLogBudget {
    pub step_bytes: usize,
    pub run_bytes: usize,
    pub step_records: usize,
    pub run_records: usize,
}

impl RunLogBudget {
    /// The budget of `useful workflow run`'s interface.
    fn base() -> Self {
        Self {
            step_bytes: usize::try_from(super::MAXIMUM_RETAINED_BYTES_PER_STREAM)
                .unwrap_or(usize::MAX),
            run_bytes: usize::try_from(super::RUN_LOG_BYTE_BUDGET).unwrap_or(usize::MAX),
            step_records: MAXIMUM_LOG_RECORDS_PER_STEP,
            run_records: RUN_LOG_RECORD_BUDGET,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StepLogCapacity {
    maximum_records: usize,
    maximum_bytes: usize,
}

impl StepLogCapacity {
    #[cfg(test)]
    pub(crate) fn new(maximum_records: usize, maximum_bytes: usize) -> Option<Self> {
        (maximum_records != 0 && maximum_bytes >= MAX_NORMALIZED_CHILD_RECORD_BYTES).then_some(
            Self {
                maximum_records,
                maximum_bytes,
            },
        )
    }

    /// A step's capacity under the base budget.
    pub(crate) fn for_step_count(step_count: usize) -> Self {
        Self::for_budget(RunLogBudget::base(), step_count)
    }

    /// A step's share of `budget` among `step_count` steps: the smaller of the step bound and
    /// the run bound over the step count, for records and for bytes, and never under one
    /// record.
    fn for_budget(budget: RunLogBudget, step_count: usize) -> Self {
        let step_count = step_count.max(1);
        Self {
            maximum_records: budget
                .step_records
                .min(budget.run_records / step_count)
                .max(1),
            maximum_bytes: budget
                .step_bytes
                .min(budget.run_bytes / step_count)
                .max(MAX_NORMALIZED_CHILD_RECORD_BYTES),
        }
    }

    pub(crate) const fn maximum_records(self) -> usize {
        self.maximum_records
    }

    pub(crate) const fn maximum_bytes(self) -> usize {
        self.maximum_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunLogSource {
    Command(CommandOutputSource),
    Agent(AgentPresentationObservationKind),
}

const AGENT_OBSERVATION_KINDS: [AgentPresentationObservationKind; 10] = [
    AgentPresentationObservationKind::Assistant,
    AgentPresentationObservationKind::Reasoning,
    AgentPresentationObservationKind::ToolCall,
    AgentPresentationObservationKind::ToolResult,
    AgentPresentationObservationKind::Diagnostic,
    AgentPresentationObservationKind::Usage,
    AgentPresentationObservationKind::Model,
    AgentPresentationObservationKind::Lifecycle,
    AgentPresentationObservationKind::ValueRejected,
    AgentPresentationObservationKind::HarnessEvent,
];

impl WorkflowRunLogSource {
    /// The source's word: `stdout` or `stderr` for a command's record and the kind's word for
    /// an agent's (REQ-RUNNERENROLL-145).
    pub(crate) const fn word(self) -> &'static str {
        match self {
            Self::Command(CommandOutputSource::StandardOutput) => "stdout",
            Self::Command(CommandOutputSource::StandardError) => "stderr",
            Self::Agent(kind) => kind.as_str(),
        }
    }

    /// The source `word` spells, `None` for a word that spells none.
    pub(crate) fn from_word(word: &str) -> Option<Self> {
        match word {
            "stdout" => Some(Self::Command(CommandOutputSource::StandardOutput)),
            "stderr" => Some(Self::Command(CommandOutputSource::StandardError)),
            _ => AGENT_OBSERVATION_KINDS
                .into_iter()
                .find(|kind| kind.as_str() == word)
                .map(Self::Agent),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunLogRecord {
    pub(crate) accepted_order: AcceptedRecordOrder,
    pub(crate) observed_at: OffsetDateTime,
    pub(crate) invocation: super::runtime::ActionId,
    pub(crate) source: WorkflowRunLogSource,
    pub(crate) source_sequence: u64,
    pub(crate) payload: Arc<str>,
    pub(crate) display_width: usize,
    pub(crate) continuation: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunStepLog {
    pub(crate) records: Arc<VecDeque<WorkflowRunLogRecord>>,
    pub(crate) observed_records: u64,
    pub(crate) retained_records: u64,
    pub(crate) retained_bytes: u64,
    pub(crate) discarded_records: u64,
    pub(crate) discarded_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunOutputUnavailableReason {
    Failed,
    Blocked,
    Skipped,
    NotRun,
    Cancelled,
    /// The step succeeded without committing the output, as a `workflow` step leaves an
    /// export its child left unavailable (REQ-WFEXEC-60).
    NotCommitted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunOutputDisposition {
    Pending,
    Committed,
    Unavailable(WorkflowRunOutputUnavailableReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunElapsed {
    pub(crate) started_at: OffsetDateTime,
    pub(crate) duration: Duration,
    pub(crate) frozen: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunStepView {
    pub(crate) id: String,
    pub(crate) role: WorkflowNodeRole,
    pub(crate) definition: WorkflowPresentationStep,
    pub(crate) state: StepStateKind,
    pub(crate) fact: Option<ObservedStepTransition>,
    pub(crate) inherited: Option<super::evidence::InheritedDetail>,
    pub(crate) timing: Option<WorkflowRunElapsed>,
    pub(crate) outputs: BTreeMap<String, WorkflowRunOutputDisposition>,
    pub(crate) log: WorkflowRunStepLog,
    /// The failed child a `workflow` step succeeded on (REQ-WFEXEC-77); `None` for any
    /// other step. The step's `state` stays `Succeeded`.
    pub(crate) failed_child: Option<FailedChild>,
}

impl WorkflowRunStepView {
    /// The state the interface presents the step with (REQ-LOCALRUN-221).
    pub(crate) fn presented_state(&self) -> StepStateKind {
        presented_step_state(self.state, self.failed_child.as_ref())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowRunCancellationView {
    pub(crate) reason: CancellationReason,
    pub(crate) force_stop_deadline: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRunPublicationFailure {
    pub(crate) phase: LocalPublicationPhase,
    pub(crate) kind: LocalPublicationFailureKind,
    pub(crate) export: Option<String>,
}

impl From<&LocalPublicationError> for WorkflowRunPublicationFailure {
    fn from(error: &LocalPublicationError) -> Self {
        Self {
            phase: error.phase(),
            kind: error.kind(),
            export: error.export().map(str::to_owned),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowRunPublicationResult {
    Succeeded { result_directory: String },
    Failed(WorkflowRunPublicationFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunPublicationState {
    NotStarted,
    Publishing,
    Completed(WorkflowRunPublicationResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowRunCleanupResult {
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkflowRunCleanupState {
    NotStarted,
    Cleaning,
    Completed(WorkflowRunCleanupResult),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRunViewSnapshot {
    pub(crate) generation: u64,
    pub(crate) workflow_path: String,
    pub(crate) maximum_parallel_steps: usize,
    pub(crate) workflow: WorkflowState<OffsetDateTime>,
    pub(crate) timing: WorkflowRunElapsed,
    pub(crate) steps: Vec<WorkflowRunStepView>,
    pub(crate) finalization_start: Option<usize>,
    pub(crate) cancellation: Option<WorkflowRunCancellationView>,
    pub(crate) force_abort: Option<super::runtime::ForceAbortEvidence>,
    pub(crate) finalization: Option<WorkflowRunFinalization>,
    pub(crate) authoritative_result: bool,
    pub(crate) quiescent: bool,
    pub(crate) publication: WorkflowRunPublicationState,
    pub(crate) cleanup: WorkflowRunCleanupState,
    pub quit_eligible: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowRunViewModelError {
    AlreadyReconciled,
    InvalidTerminalResult,
}

pub struct WorkflowRunViewModel<Clock> {
    inner: Arc<Mutex<WorkflowRunViewState>>,
    timing: RunTimingObservation,
    clock: Clock,
    changes: watch::Sender<u64>,
}

impl<Clock: Clone> Clone for WorkflowRunViewModel<Clock> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            timing: self.timing.clone(),
            clock: self.clock.clone(),
            changes: self.changes.clone(),
        }
    }
}

impl<Clock> WorkflowRunViewModel<Clock>
where
    Clock: ObservationClock,
{
    pub fn new(
        workflow: &ResolvedWorkflow,
        maximum_parallel_steps: usize,
        timing: RunTimingObservation,
        clock: Clock,
    ) -> Self {
        let log_capacity = StepLogCapacity::for_step_count(node_count(workflow));
        Self::with_log_capacity(
            workflow,
            maximum_parallel_steps,
            timing,
            clock,
            log_capacity,
        )
    }

    /// A model whose steps keep their output within `budget` (REQ-RUNNERENROLL-147).
    pub fn with_log_budget(
        workflow: &ResolvedWorkflow,
        maximum_parallel_steps: usize,
        timing: RunTimingObservation,
        clock: Clock,
        budget: RunLogBudget,
    ) -> Self {
        let log_capacity = StepLogCapacity::for_budget(budget, node_count(workflow));
        Self::with_log_capacity(
            workflow,
            maximum_parallel_steps,
            timing,
            clock,
            log_capacity,
        )
    }

    fn with_log_capacity(
        workflow: &ResolvedWorkflow,
        maximum_parallel_steps: usize,
        timing: RunTimingObservation,
        clock: Clock,
        log_capacity: StepLogCapacity,
    ) -> Self {
        let feed = WorkflowPresentationFeed::new(workflow);
        let steps = feed
            .definition()
            .presentation_order
            .iter()
            .filter_map(|id| {
                feed.definition()
                    .steps
                    .get(id)
                    .cloned()
                    .zip(feed.definition().node_roles.get(id).copied())
                    .map(|(definition, role)| {
                        WorkflowRunStepViewState::new(id.clone(), role, definition)
                    })
            })
            .collect::<Vec<_>>();
        let step_indexes = steps
            .iter()
            .enumerate()
            .map(|(index, step)| (step.id.clone(), index))
            .collect();
        let finalization_start = feed.definition().finalization_start;
        let (changes, _) = watch::channel(0);
        Self {
            inner: Arc::new(Mutex::new(WorkflowRunViewState {
                workflow_path: feed.definition().workflow_path.clone(),
                maximum_parallel_steps,
                feed,
                step_indexes,
                steps,
                finalization_start,
                workflow: WorkflowState::Executing {
                    gate: SchedulingGate::Open,
                },
                cancellation: None,
                force_abort: None,
                finalization: None,
                authoritative_result: false,
                terminal_timing: None,
                quiescent: false,
                publication: WorkflowRunPublicationState::NotStarted,
                cleanup: WorkflowRunCleanupState::NotStarted,
                adapter_lifecycle_completed: false,
                log_capacity,
                last_accepted_order: None,
                generation: 0,
            })),
            timing,
            clock,
            changes,
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub fn timing_observation(&self) -> RunTimingObservation {
        self.timing.clone()
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn snapshot(&self) -> WorkflowRunViewSnapshot {
        self.snapshot_with_logs(None)
    }

    pub(crate) fn snapshot_for_render(&self, selected_step: usize) -> WorkflowRunViewSnapshot {
        self.snapshot_with_logs(Some(selected_step))
    }

    fn snapshot_with_logs(&self, selected_step: Option<usize>) -> WorkflowRunViewSnapshot {
        let now = self.clock.sample();
        let state = lock_state(&self.inner);
        self.snapshot_of(&state, now, selected_step)
    }

    /// A snapshot holding the records of the step `step` names alone, and that step's index:
    /// the step is resolved and the snapshot taken in one hold of the lock. With no step
    /// named, or one the model does not have, the snapshot holds no step's records.
    pub(crate) fn snapshot_for_step(
        &self,
        step: Option<&str>,
    ) -> (WorkflowRunViewSnapshot, Option<usize>) {
        let now = self.clock.sample();
        let state = lock_state(&self.inner);
        let index = step.and_then(|id| state.step_indexes.get(id).copied());
        let selected = index.unwrap_or(state.steps.len());
        (self.snapshot_of(&state, now, Some(selected)), index)
    }

    /// The IDs and states of the first `maximum` steps in flight in presentation order, and
    /// the number of the others, copied in one hold of the lock (REQ-RUNNERENROLL-134).
    pub(crate) fn steps_in_flight(&self, maximum: usize) -> (Vec<(String, StepStateKind)>, usize) {
        let state = lock_state(&self.inner);
        let mut steps = Vec::new();
        let mut omitted = 0_usize;
        for step in state.steps.iter().filter(|step| is_in_flight(step.state)) {
            if steps.len() < maximum {
                steps.push((step.id.clone(), step.state));
            } else {
                omitted = omitted.saturating_add(1);
            }
        }
        (steps, omitted)
    }

    /// Shows the model the steps `seed` inherits, which a seeded engine start settles with no
    /// transition: each is `inherited` with the seed's detail and outputs.
    pub fn observe_seed<Output>(&self, seed: &ExecutionSeed<Output>) {
        self.observe_inherited(|id| {
            seed.inherited_step(id).map(|step| InheritedStepFacts {
                detail: &step.detail,
                disposition: step.disposition,
                outputs: step.outputs.keys().map(String::as_str).collect(),
            })
        });
    }

    /// Settles each step of the model `inherited` answers facts for as `inherited`.
    pub(crate) fn observe_inherited<'seed>(
        &self,
        inherited: impl Fn(&str) -> Option<InheritedStepFacts<'seed>>,
    ) {
        self.update(|state| {
            let mut changed = false;
            for step in &mut state.steps {
                if let Some(facts) = inherited(&step.id) {
                    step.inherit(&facts);
                    changed = true;
                }
            }
            changed
        });
    }

    fn snapshot_of(
        &self,
        state: &WorkflowRunViewState,
        now: ObservationTime,
        selected_step: Option<usize>,
    ) -> WorkflowRunViewSnapshot {
        let timing = self.timing.snapshot();
        let workflow_timing = state.terminal_timing.as_ref().map_or_else(
            || observed_run_elapsed(&timing, now),
            |terminal| WorkflowRunElapsed {
                started_at: terminal.started_at,
                duration: terminal.duration,
                frozen: true,
            },
        );
        let steps = state
            .steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                step.snapshot(
                    &timing,
                    now,
                    state.authoritative_result,
                    selected_step.is_none_or(|selected| selected == index),
                )
            })
            .collect();
        WorkflowRunViewSnapshot {
            generation: state.generation,
            workflow_path: state.workflow_path.clone(),
            maximum_parallel_steps: state.maximum_parallel_steps,
            workflow: state.workflow.clone(),
            timing: workflow_timing,
            steps,
            finalization_start: state.finalization_start,
            cancellation: state.cancellation.clone(),
            force_abort: state.force_abort,
            finalization: state.finalization.clone(),
            authoritative_result: state.authoritative_result,
            quiescent: state.quiescent,
            publication: state.publication.clone(),
            cleanup: state.cleanup,
            quit_eligible: state.adapter_lifecycle_completed,
        }
    }

    pub fn reconcile_terminal_result(
        &self,
        run: &WorkflowRunResult,
    ) -> Result<(), WorkflowRunViewModelError> {
        let generation = {
            let mut state = lock_state(&self.inner);
            if state.authoritative_result {
                return Err(WorkflowRunViewModelError::AlreadyReconciled);
            }
            validate_terminal_result(&state, run)?;
            let closing_records = state.feed.finish_child_streams(run.timing.finished_at);
            for record in closing_records {
                state.apply_record(record);
            }

            state.workflow = workflow_state_from_outcome(&run.outcome);
            state.cancellation =
                run.cancellation
                    .as_ref()
                    .map(|cancellation| WorkflowRunCancellationView {
                        reason: cancellation.reason,
                        force_stop_deadline: cancellation.force_stop_deadline,
                    });
            state.force_abort = run.force_abort;
            for (view, terminal) in state.steps.iter_mut().zip(run_nodes(run)) {
                view.reconcile(terminal);
            }
            state.finalization = run.finalization.clone();
            state.terminal_timing = Some(run.timing.clone());
            state.authoritative_result = true;
            state.advance_generation()
        };
        self.changes.send_replace(generation);
        Ok(())
    }

    pub fn mark_quiescent(&self) {
        let generation = {
            let mut state = lock_state(&self.inner);
            if state.quiescent {
                return;
            }
            let observed_at = self.clock.sample();
            self.timing.mark_quiesced(observed_at);
            state.quiescent = true;
            state.advance_generation()
        };
        self.changes.send_replace(generation);
    }

    pub fn begin_publication(&self) {
        self.update(|state| {
            if state.publication != WorkflowRunPublicationState::NotStarted {
                return false;
            }
            state.publication = WorkflowRunPublicationState::Publishing;
            true
        });
    }

    pub fn complete_publication(&self, result: WorkflowRunPublicationResult) {
        self.update(|state| {
            if state.publication != WorkflowRunPublicationState::Publishing {
                return false;
            }
            state.publication = WorkflowRunPublicationState::Completed(result);
            true
        });
    }

    pub fn begin_cleanup(&self) {
        self.update(|state| {
            if state.cleanup != WorkflowRunCleanupState::NotStarted {
                return false;
            }
            state.cleanup = WorkflowRunCleanupState::Cleaning;
            true
        });
    }

    pub fn complete_cleanup(&self, result: WorkflowRunCleanupResult) {
        self.update(|state| {
            if state.cleanup != WorkflowRunCleanupState::Cleaning {
                return false;
            }
            state.cleanup = WorkflowRunCleanupState::Completed(result);
            true
        });
    }

    pub fn mark_adapter_lifecycle_completed(&self) {
        self.update(|state| {
            if state.adapter_lifecycle_completed {
                return false;
            }
            state.adapter_lifecycle_completed = true;
            true
        });
    }

    fn update(&self, update: impl FnOnce(&mut WorkflowRunViewState) -> bool) {
        let generation = {
            let mut state = lock_state(&self.inner);
            update(&mut state).then(|| state.advance_generation())
        };
        if let Some(generation) = generation {
            self.changes.send_replace(generation);
        }
    }
}

impl<Clock, Deadline> ExecutionObserver<Deadline> for WorkflowRunViewModel<Clock>
where
    Clock: ObservationClock,
    Deadline: DisplayDeadline,
{
    fn observe(
        &self,
        observation: ExecutionObservation<Deadline>,
    ) -> impl Future<Output = ()> + Send {
        let generation = {
            let mut state = lock_state(&self.inner);
            let observed_at = self.clock.sample();
            self.timing.record(&observation, observed_at);
            let records = state.feed.accept(observed_at.utc, observation);
            if records.is_empty() {
                None
            } else {
                for record in records {
                    state.apply_record(record);
                }
                Some(state.advance_generation())
            }
        };
        if let Some(generation) = generation {
            self.changes.send_replace(generation);
        }
        ready(())
    }
}

/// What the model needs of a step a continued attempt inherits: the seed's detail, its
/// disposition and the outputs it holds.
pub(crate) struct InheritedStepFacts<'seed> {
    pub(crate) detail: &'seed super::evidence::InheritedDetail,
    pub(crate) disposition: InheritedDisposition,
    pub(crate) outputs: Vec<&'seed str>,
}

struct WorkflowRunViewState {
    workflow_path: String,
    maximum_parallel_steps: usize,
    feed: WorkflowPresentationFeed,
    step_indexes: BTreeMap<String, usize>,
    steps: Vec<WorkflowRunStepViewState>,
    finalization_start: Option<usize>,
    workflow: WorkflowState<OffsetDateTime>,
    cancellation: Option<WorkflowRunCancellationView>,
    force_abort: Option<super::runtime::ForceAbortEvidence>,
    finalization: Option<WorkflowRunFinalization>,
    authoritative_result: bool,
    terminal_timing: Option<WorkflowRunTiming>,
    quiescent: bool,
    publication: WorkflowRunPublicationState,
    cleanup: WorkflowRunCleanupState,
    adapter_lifecycle_completed: bool,
    log_capacity: StepLogCapacity,
    last_accepted_order: Option<AcceptedRecordOrder>,
    generation: u64,
}

impl WorkflowRunViewState {
    fn apply_record(&mut self, record: PresentationRecord) {
        self.last_accepted_order = Some(record.accepted_order);
        match record.kind {
            PresentationRecordKind::Transition(transition) => {
                self.apply_transition(*transition);
            }
            PresentationRecordKind::ChildOutput(output) => {
                let Some(index) = self.step_indexes.get(&output.step).copied() else {
                    return;
                };
                self.steps[index].log.push_command(
                    record.accepted_order,
                    record.observed_at,
                    output,
                    self.log_capacity,
                );
            }
            PresentationRecordKind::AgentObservation(observation) => {
                let Some(index) = self.step_indexes.get(&observation.step).copied() else {
                    return;
                };
                self.steps[index].log.push_agent(
                    record.accepted_order,
                    record.observed_at,
                    observation,
                    self.log_capacity,
                );
            }
        }
    }

    fn apply_transition(&mut self, transition: PresentationTransition) {
        match transition.event {
            TransitionEvent::Step { step, to, .. } => {
                let Some(index) = self.step_indexes.get(&step).copied() else {
                    return;
                };
                self.steps[index].apply_transition(to, transition.step);
            }
            TransitionEvent::Workflow { to, .. } => {
                self.workflow = *to;
            }
            TransitionEvent::CancellationAccepted {
                reason, deadline, ..
            } => {
                let prior_issue = match &self.workflow {
                    WorkflowState::Executing {
                        gate: SchedulingGate::FailureStopped { primary_issue },
                    }
                    | WorkflowState::Executing {
                        gate:
                            SchedulingGate::Cancelling {
                                prior_issue: Some(primary_issue),
                                ..
                            },
                    } => Some(primary_issue.clone()),
                    WorkflowState::Executing { .. }
                    | WorkflowState::Finalizing { .. }
                    | WorkflowState::Succeeded
                    | WorkflowState::Failed { .. }
                    | WorkflowState::Cancelled { .. }
                    | WorkflowState::Waiting { .. }
                    | WorkflowState::Continued { .. } => None,
                };
                self.workflow = WorkflowState::Executing {
                    gate: SchedulingGate::Cancelling {
                        reason,
                        prior_issue,
                    },
                };
                self.cancellation = Some(WorkflowRunCancellationView {
                    reason,
                    force_stop_deadline: deadline,
                });
            }
            TransitionEvent::FinalizationCancellationAccepted {
                reason, deadline, ..
            } => {
                let WorkflowState::Finalizing {
                    trigger,
                    primary_issue,
                    ..
                } = &self.workflow
                else {
                    return;
                };
                self.workflow = WorkflowState::Finalizing {
                    trigger: *trigger,
                    gate: super::runtime::FinalizationGate::Cancelling {
                        reason,
                        deadline: Some(deadline),
                        force_abort: false,
                    },
                    primary_issue: primary_issue.clone(),
                };
            }
            TransitionEvent::ForceAbortAccepted { reason, phase, .. } => {
                self.force_abort = Some(super::runtime::ForceAbortEvidence { reason, phase });
                match (&self.workflow, phase) {
                    (
                        WorkflowState::Executing { gate },
                        super::runtime::RunCancellationPhase::Ordinary,
                    ) => {
                        let (effective_reason, prior_issue) = match gate {
                            SchedulingGate::Open => (reason, None),
                            SchedulingGate::FailureStopped { primary_issue } => {
                                (reason, Some(primary_issue.clone()))
                            }
                            SchedulingGate::Cancelling {
                                reason,
                                prior_issue,
                            } => (*reason, prior_issue.clone()),
                        };
                        self.workflow = WorkflowState::Executing {
                            gate: SchedulingGate::Cancelling {
                                reason: effective_reason,
                                prior_issue,
                            },
                        };
                    }
                    (
                        WorkflowState::Finalizing {
                            trigger,
                            gate,
                            primary_issue,
                        },
                        super::runtime::RunCancellationPhase::Finalization,
                    ) => {
                        let deadline = match gate {
                            super::runtime::FinalizationGate::Open => None,
                            super::runtime::FinalizationGate::Cancelling { deadline, .. } => {
                                *deadline
                            }
                        };
                        let effective_reason = match gate {
                            super::runtime::FinalizationGate::Open => reason,
                            super::runtime::FinalizationGate::Cancelling { reason, .. } => *reason,
                        };
                        self.workflow = WorkflowState::Finalizing {
                            trigger: *trigger,
                            gate: super::runtime::FinalizationGate::Cancelling {
                                reason: effective_reason,
                                deadline,
                                force_abort: true,
                            },
                            primary_issue: primary_issue.clone(),
                        };
                    }
                    _ => {}
                }
            }
        }
    }

    fn advance_generation(&mut self) -> u64 {
        self.generation = self.generation.saturating_add(1);
        self.generation
    }
}

struct WorkflowRunStepViewState {
    id: String,
    role: WorkflowNodeRole,
    definition: WorkflowPresentationStep,
    state: StepStateKind,
    fact: Option<ObservedStepTransition>,
    inherited: Option<super::evidence::InheritedDetail>,
    outputs: BTreeMap<String, WorkflowRunOutputDisposition>,
    log: StepLogRing,
    terminal_timing: Option<WorkflowStepTiming>,
    failed_child: Option<FailedChild>,
}

impl WorkflowRunStepViewState {
    fn new(id: String, role: WorkflowNodeRole, definition: WorkflowPresentationStep) -> Self {
        let outputs = definition
            .outputs()
            .keys()
            .map(|name| (name.clone(), WorkflowRunOutputDisposition::Pending))
            .collect();
        Self {
            id,
            role,
            definition,
            state: StepStateKind::Pending,
            fact: None,
            inherited: None,
            outputs,
            log: StepLogRing::default(),
            terminal_timing: None,
            failed_child: None,
        }
    }

    fn apply_transition(&mut self, state: StepStateKind, detail: Option<ObservedStepTransition>) {
        self.state = state;
        self.fact = match &detail {
            Some(ObservedStepTransition::OutputsCommitted { .. }) | None => None,
            Some(_) => detail.clone(),
        };
        self.failed_child = match (state, &detail) {
            (
                StepStateKind::Succeeded,
                Some(ObservedStepTransition::OutputsCommitted { failed_child, .. }),
            ) => failed_child.clone(),
            _ => None,
        };
        match state {
            StepStateKind::Succeeded => {
                if let Some(ObservedStepTransition::OutputsCommitted { outputs, .. }) = detail {
                    for (name, disposition) in &mut self.outputs {
                        *disposition = if outputs.contains(name) {
                            WorkflowRunOutputDisposition::Committed
                        } else {
                            WorkflowRunOutputDisposition::Unavailable(
                                WorkflowRunOutputUnavailableReason::NotCommitted,
                            )
                        };
                    }
                }
            }
            StepStateKind::Inherited => {}
            StepStateKind::Failed => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Failed);
            }
            StepStateKind::Blocked => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Blocked);
            }
            StepStateKind::Skipped => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Skipped);
            }
            StepStateKind::NotRun => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::NotRun);
            }
            StepStateKind::Cancelled => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Cancelled);
            }
            StepStateKind::Pending
            | StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Recovering
            | StepStateKind::Cancelling => {}
        }
    }

    fn reconcile(&mut self, terminal: &super::publication::WorkflowRunStep) {
        self.state = terminal_state_kind(&terminal.state);
        self.fact = terminal_step_fact(&terminal.state);
        self.inherited = match &terminal.state {
            StepState::Inherited { detail, .. } => Some(detail.clone()),
            _ => None,
        };
        self.failed_child = match &terminal.state {
            StepState::Succeeded { .. } => terminal.failed_child.clone(),
            _ => None,
        };
        match &terminal.state {
            StepState::Succeeded { outputs } => {
                for (name, disposition) in &mut self.outputs {
                    *disposition = if outputs.contains_key(name) {
                        WorkflowRunOutputDisposition::Committed
                    } else {
                        WorkflowRunOutputDisposition::Unavailable(
                            WorkflowRunOutputUnavailableReason::NotCommitted,
                        )
                    };
                }
            }
            StepState::Inherited {
                disposition,
                outputs,
                ..
            } => self.inherit_outputs(*disposition, |name| outputs.contains_key(name)),
            StepState::Failed { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Failed);
            }
            StepState::Blocked { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Blocked);
            }
            StepState::Skipped { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Skipped);
            }
            StepState::NotRun { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::NotRun);
            }
            StepState::Cancelled { .. } => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Cancelled);
            }
            StepState::Pending
            | StepState::Starting
            | StepState::Running
            | StepState::CapturingOutputs
            | StepState::Recovering { .. }
            | StepState::Cancelling { .. } => {}
        }
        self.terminal_timing = terminal.timing.clone();
    }

    fn inherit(&mut self, facts: &InheritedStepFacts<'_>) {
        self.state = StepStateKind::Inherited;
        self.fact = None;
        self.inherited = Some(facts.detail.clone());
        self.failed_child = None;
        self.inherit_outputs(facts.disposition, |name| facts.outputs.contains(&name));
    }

    /// An inherited step's outputs: the ones it holds committed and the others pending when
    /// it inherited a success, and every one unavailable when it inherited a skip.
    fn inherit_outputs(&mut self, disposition: InheritedDisposition, holds: impl Fn(&str) -> bool) {
        match disposition {
            InheritedDisposition::Succeeded => {
                for (name, output_disposition) in &mut self.outputs {
                    *output_disposition = if holds(name) {
                        WorkflowRunOutputDisposition::Committed
                    } else {
                        WorkflowRunOutputDisposition::Pending
                    };
                }
            }
            InheritedDisposition::Skipped => {
                self.make_outputs_unavailable(WorkflowRunOutputUnavailableReason::Skipped);
            }
        }
    }

    fn make_outputs_unavailable(&mut self, reason: WorkflowRunOutputUnavailableReason) {
        for disposition in self.outputs.values_mut() {
            *disposition = WorkflowRunOutputDisposition::Unavailable(reason);
        }
    }

    fn snapshot(
        &self,
        timing: &RunTimingSnapshot,
        now: ObservationTime,
        authoritative_result: bool,
        include_log_records: bool,
    ) -> WorkflowRunStepView {
        let step_timing = if authoritative_result {
            self.terminal_timing
                .as_ref()
                .map(|terminal| WorkflowRunElapsed {
                    started_at: terminal.started_at,
                    duration: terminal.duration,
                    frozen: true,
                })
        } else {
            observed_step_elapsed(timing, &self.id, now)
        };
        WorkflowRunStepView {
            id: self.id.clone(),
            role: self.role,
            definition: self.definition.clone(),
            state: self.state,
            fact: self.fact.clone(),
            inherited: self.inherited.clone(),
            timing: step_timing,
            outputs: self.outputs.clone(),
            log: self.log.snapshot(include_log_records),
            failed_child: self.failed_child.clone(),
        }
    }
}

#[derive(Default)]
struct StepLogRing {
    records: Arc<VecDeque<WorkflowRunLogRecord>>,
    retained_bytes: u64,
    observed_records: u64,
    discarded_records: u64,
    discarded_bytes: u64,
}

impl StepLogRing {
    fn push_command(
        &mut self,
        accepted_order: AcceptedRecordOrder,
        observed_at: OffsetDateTime,
        output: NormalizedChildOutput,
        capacity: StepLogCapacity,
    ) {
        self.push(
            accepted_order,
            observed_at,
            output.invocation,
            WorkflowRunLogSource::Command(output.source),
            output.source_sequence.get(),
            output.payload,
            output.continuation,
            capacity,
        );
    }

    fn push_agent(
        &mut self,
        accepted_order: AcceptedRecordOrder,
        observed_at: OffsetDateTime,
        observation: NormalizedAgentObservation,
        capacity: StepLogCapacity,
    ) {
        self.push(
            accepted_order,
            observed_at,
            observation.invocation,
            WorkflowRunLogSource::Agent(observation.kind),
            observation.observation_sequence,
            observation.payload,
            observation.continuation,
            capacity,
        );
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the bounded log record keeps normalized source identity explicit"
    )]
    fn push(
        &mut self,
        accepted_order: AcceptedRecordOrder,
        observed_at: OffsetDateTime,
        invocation: super::runtime::ActionId,
        source: WorkflowRunLogSource,
        source_sequence: u64,
        payload: String,
        continuation: bool,
        capacity: StepLogCapacity,
    ) {
        let payload_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let maximum_bytes = u64::try_from(capacity.maximum_bytes()).unwrap_or(u64::MAX);
        while !self.records.is_empty()
            && (self.records.len() >= capacity.maximum_records()
                || self.retained_bytes.saturating_add(payload_bytes) > maximum_bytes)
        {
            self.discard_oldest();
        }

        self.observed_records = self.observed_records.saturating_add(1);
        self.retained_bytes = self.retained_bytes.saturating_add(payload_bytes);
        let display_width = UnicodeWidthStr::width(payload.as_str());
        Arc::make_mut(&mut self.records).push_back(WorkflowRunLogRecord {
            accepted_order,
            observed_at,
            invocation,
            source,
            source_sequence,
            payload: Arc::from(payload),
            display_width,
            continuation,
        });
    }

    fn discard_oldest(&mut self) {
        let Some(discarded) = Arc::make_mut(&mut self.records).pop_front() else {
            return;
        };
        let discarded_bytes = u64::try_from(discarded.payload.len()).unwrap_or(u64::MAX);
        self.retained_bytes = self.retained_bytes.saturating_sub(discarded_bytes);
        self.discarded_records = self.discarded_records.saturating_add(1);
        self.discarded_bytes = self.discarded_bytes.saturating_add(discarded_bytes);
    }

    fn snapshot(&self, include_records: bool) -> WorkflowRunStepLog {
        WorkflowRunStepLog {
            records: if include_records {
                Arc::clone(&self.records)
            } else {
                Arc::new(VecDeque::new())
            },
            observed_records: self.observed_records,
            retained_records: u64::try_from(self.records.len()).unwrap_or(u64::MAX),
            retained_bytes: self.retained_bytes,
            discarded_records: self.discarded_records,
            discarded_bytes: self.discarded_bytes,
        }
    }
}

/// The steps and finalizers of `workflow`, among which the run's log budget is shared.
fn node_count(workflow: &ResolvedWorkflow) -> usize {
    workflow.definition.presentation_order.len()
        + workflow.definition.finalizer_presentation_order.len()
}

/// Whether a step in `state` has started and not settled (REQ-RUNNERENROLL-134).
const fn is_in_flight(state: StepStateKind) -> bool {
    matches!(
        state,
        StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Recovering
            | StepStateKind::Cancelling
    )
}

fn validate_terminal_result(
    state: &WorkflowRunViewState,
    run: &WorkflowRunResult,
) -> Result<(), WorkflowRunViewModelError> {
    let nodes = run_nodes(run).collect::<Vec<_>>();
    if run.workflow_path != state.workflow_path
        || nodes.len() != state.steps.len()
        || run.finalization.is_some() != state.finalization_start.is_some()
    {
        return Err(WorkflowRunViewModelError::InvalidTerminalResult);
    }
    for (view, terminal) in state.steps.iter().zip(nodes) {
        if view.id != terminal.id
            || view.role != terminal.role
            || terminal.failure_policy != view.definition.failure_policy()
            || !terminal_step_is_valid(view, terminal)
        {
            return Err(WorkflowRunViewModelError::InvalidTerminalResult);
        }
    }
    Ok(())
}

fn run_nodes(
    run: &WorkflowRunResult,
) -> impl Iterator<Item = &super::publication::WorkflowRunStep> {
    run.steps.iter().chain(
        run.finalization
            .iter()
            .flat_map(|finalization| &finalization.finalizers),
    )
}

fn terminal_step_is_valid(
    view: &WorkflowRunStepViewState,
    terminal: &super::publication::WorkflowRunStep,
) -> bool {
    match &terminal.state {
        // A `workflow` step commits the exports its child left available, a subset of the
        // ones it declares (REQ-WFEXEC-60); any other step commits every declared output.
        StepState::Succeeded { outputs } => match view.definition {
            WorkflowPresentationStep::Workflow { .. } => outputs
                .keys()
                .all(|output| view.outputs.contains_key(output)),
            WorkflowPresentationStep::Command { .. } | WorkflowPresentationStep::Agent { .. } => {
                outputs.keys().eq(view.outputs.keys())
            }
        },
        StepState::Inherited {
            disposition: InheritedDisposition::Succeeded,
            outputs,
            ..
        } => outputs
            .keys()
            .all(|output| view.outputs.contains_key(output)),
        StepState::Inherited {
            disposition: InheritedDisposition::Skipped,
            ..
        }
        | StepState::Failed { .. }
        | StepState::Blocked { .. }
        | StepState::Skipped { .. }
        | StepState::NotRun { .. }
        | StepState::Cancelled { .. } => true,
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => false,
    }
}

fn terminal_state_kind(state: &StepState<super::value::CapturedValue>) -> StepStateKind {
    match state {
        StepState::Pending => StepStateKind::Pending,
        StepState::Starting => StepStateKind::Starting,
        StepState::Running => StepStateKind::Running,
        StepState::CapturingOutputs => StepStateKind::CapturingOutputs,
        StepState::Recovering { .. } => StepStateKind::Recovering,
        StepState::Cancelling { .. } => StepStateKind::Cancelling,
        StepState::Succeeded { .. } => StepStateKind::Succeeded,
        StepState::Inherited { .. } => StepStateKind::Inherited,
        StepState::Failed { .. } => StepStateKind::Failed,
        StepState::Blocked { .. } => StepStateKind::Blocked,
        StepState::Skipped { .. } => StepStateKind::Skipped,
        StepState::NotRun { .. } => StepStateKind::NotRun,
        StepState::Cancelled { .. } => StepStateKind::Cancelled,
    }
}

fn terminal_step_fact(
    state: &StepState<super::value::CapturedValue>,
) -> Option<ObservedStepTransition> {
    match state {
        StepState::Inherited { .. } => None,
        StepState::Failed { detail } => Some(ObservedStepTransition::Failed {
            detail: detail.clone(),
        }),
        StepState::Blocked { detail } => Some(ObservedStepTransition::Blocked {
            detail: detail.clone(),
        }),
        StepState::Skipped { detail } => Some(ObservedStepTransition::Skipped {
            detail: detail.clone(),
        }),
        StepState::NotRun { detail } => Some(ObservedStepTransition::NotRun { detail: *detail }),
        StepState::Cancelling { detail } => {
            Some(ObservedStepTransition::Cancelling { detail: *detail })
        }
        StepState::Cancelled { detail } => {
            Some(ObservedStepTransition::Cancelled { detail: *detail })
        }
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Succeeded { .. } => None,
    }
}

fn workflow_state_from_outcome(outcome: &RunOutcome) -> WorkflowState<OffsetDateTime> {
    match outcome {
        RunOutcome::Succeeded => WorkflowState::Succeeded,
        RunOutcome::Failed {
            primary_issue,
            later_cancellation,
        } => WorkflowState::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: *later_cancellation,
        },
        RunOutcome::Cancelled { reason } => WorkflowState::Cancelled { reason: *reason },
        RunOutcome::Waiting { wait } => WorkflowState::Waiting { wait: wait.clone() },
        RunOutcome::Continued { edge } => WorkflowState::Continued { edge: edge.clone() },
    }
}

fn observed_run_elapsed(timing: &RunTimingSnapshot, now: ObservationTime) -> WorkflowRunElapsed {
    let Some(started) = timing.execution_started else {
        return WorkflowRunElapsed {
            started_at: timing.presentation_opened.utc,
            duration: Duration::ZERO,
            frozen: true,
        };
    };
    let finished = timing.terminal.or(timing.quiesced);
    let finished_at = finished.map_or(now.monotonic, |point| point.monotonic);
    WorkflowRunElapsed {
        started_at: started.utc,
        duration: finished_at.saturating_duration_since(started.monotonic),
        frozen: finished.is_some(),
    }
}

fn observed_step_elapsed(
    timing: &RunTimingSnapshot,
    step: &str,
    now: ObservationTime,
) -> Option<WorkflowRunElapsed> {
    let observed = timing.steps.get(step)?;
    let finished = observed
        .finished
        .or_else(|| timing.quiesced.map(|point| point.monotonic));
    Some(WorkflowRunElapsed {
        started_at: observed.started.utc,
        duration: finished
            .unwrap_or(now.monotonic)
            .saturating_duration_since(observed.started.monotonic),
        frozen: finished.is_some(),
    })
}

fn lock_state(state: &Mutex<WorkflowRunViewState>) -> MutexGuard<'_, WorkflowRunViewState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests;
