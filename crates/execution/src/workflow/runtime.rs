use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::admission::{
    AdmittedWorkflow, CancellationOperationId, CancellationReason, ResolvedInput,
};
use super::condition::{
    self, ConditionDispositions, ConditionEvaluation, ConditionValues, ResolvedPredicate,
    TerminalDisposition,
};
use super::document::{FailurePolicy, FinalizationTrigger, WaitKind, WaitRule};
pub(crate) use super::evidence::FailurePhase;
use super::evidence::{
    BlockedDetail, CancellationDetail, ConditionFalseDetail, EvaluatedPredicateEvidence,
    FailureDetail, InheritedDetail, NodeFailureSource, NonExecutionCode, NonExecutionDetail,
    Prerequisite, PrimaryIssue, PrimaryIssueDetail,
};
use super::finalization_context::{
    self, FinalizationContext, OrdinaryIssue, OrdinaryIssueDisposition,
};
use super::step_runtime::StepFailureKind;
use super::validated::{
    ResolvedDirectPrerequisite, ResolvedValueSource, Scheduling, ValidatedEdgeTarget,
    ValidatedFinalizer, ValidatedMessageSource, ValidatedRecoveryHandler, ValidatedStep,
    ValidatedStepRecovery, ValidatedWorkflow, WorkflowNode, WorkflowNodeRole,
};
use super::validation::CHILD_OUTCOME_OUTPUT;
use super::value::CapturedValue;

pub(crate) trait ConditionOutput {
    fn condition_text(&self) -> Option<&str>;
    fn condition_json(&self) -> Option<&serde_json::Value>;
}

pub(crate) trait ProvisionalStepResources {
    fn requires_release(&self) -> bool;

    /// How a `workflow` step's child ended, which the reducer routes on when the step's
    /// completion arrives under an interruption (REQ-WFEXEC-70); `None` for any other step.
    fn child_ending(&self) -> Option<ChildEnding> {
        None
    }
}

#[cfg(test)]
impl ProvisionalStepResources for String {
    fn requires_release(&self) -> bool {
        false
    }

    // `child:<outcome>` names a child's ending in the reducer's tests, and
    // `child:<outcome>:<step>` one an interruption with `user_request` failed at `step`.
    fn child_ending(&self) -> Option<ChildEnding> {
        let mut parts = self.strip_prefix("child:")?.splitn(2, ':');
        Some(ChildEnding {
            outcome: parts.next()?.to_owned(),
            interruption: parts.next().map(|step| ChildInterruption {
                reason: CancellationReason::UserRequest,
                step: step.to_owned(),
            }),
        })
    }
}

/// The interruption that failed a `workflow` step's child (REQ-WFEXEC-70): its reason and
/// the child's interrupted step, the primary issue of the child's result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildInterruption {
    pub reason: CancellationReason,
    pub step: String,
}

/// What a `workflow` step's child reported with the step's completion: the outcome it
/// recorded and, when an interruption failed it, that interruption.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChildEnding {
    pub(crate) outcome: String,
    pub(crate) interruption: Option<ChildInterruption>,
}

impl ConditionOutput for CapturedValue {
    fn condition_text(&self) -> Option<&str> {
        match self {
            CapturedValue::Text(value) => Some(value.as_str()),
            CapturedValue::Json(_) | CapturedValue::File(_) | CapturedValue::GitBranch(_) => None,
        }
    }

    fn condition_json(&self) -> Option<&serde_json::Value> {
        match self {
            CapturedValue::Json(value) => Some(value.value()),
            CapturedValue::Text(_) | CapturedValue::File(_) | CapturedValue::GitBranch(_) => None,
        }
    }
}

#[cfg(test)]
impl ConditionOutput for String {
    fn condition_text(&self) -> Option<&str> {
        Some(self)
    }

    fn condition_json(&self) -> Option<&serde_json::Value> {
        None
    }
}

pub(crate) type OutputSet<Output> = BTreeMap<String, Output>;
pub(crate) type ExportSet<Output> = BTreeMap<String, ExportValue<Output>>;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct TransitionSequence(pub u64);

impl TransitionSequence {
    pub const fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ActionId {
    // Kept as the serialized scalar while allocation is no longer restricted to
    // transition identities. Force-containment actions use the disjoint high range.
    pub transition_sequence: TransitionSequence,
}

impl ActionId {
    fn for_transition(transition_sequence: TransitionSequence) -> Self {
        Self {
            transition_sequence,
        }
    }

    fn for_force_abort(operation: CancellationOperationId, index: usize) -> Self {
        let operation = operation.get() & 0x7fff_ffff;
        let index = u64::try_from(index).unwrap_or(u64::MAX) & 0xffff_ffff;
        Self {
            transition_sequence: TransitionSequence((1_u64 << 63) | (operation << 32) | index),
        }
    }

    fn is_force_abort(self) -> bool {
        self.transition_sequence.get() & (1_u64 << 63) != 0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TargetExecutionNumber(u8);

impl TargetExecutionNumber {
    pub(crate) const FIRST: Self = Self(1);

    pub const fn get(self) -> u8 {
        self.0
    }

    pub const fn fixture(value: u8) -> Self {
        Self(value)
    }

    fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RecoveryRoundNumber(u8);

// Recovery rounds and target executions deliberately remain distinct typed ordinals even while
// V1 keeps their values equal; sharing one type would erase the versioned ABI invariant.
impl RecoveryRoundNumber {
    pub const fn get(self) -> u8 {
        self.0
    }

    pub const fn fixture(value: u8) -> Self {
        Self(value)
    }

    #[cfg(test)]
    pub(crate) fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryHandlerKind {
    Command,
    Agent,
}

impl RecoveryHandlerKind {
    fn from_validated(handler: &ValidatedRecoveryHandler) -> Self {
        match handler {
            ValidatedRecoveryHandler::Command { .. } => Self::Command,
            ValidatedRecoveryHandler::Agent { .. } => Self::Agent,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryHandlerActivity {
    Starting,
    Running,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActiveStepInvocation {
    Target {
        execution_number: TargetExecutionNumber,
    },
    RecoveryHandler {
        round: RecoveryRoundNumber,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProvisionalTargetFailure<Cause> {
    pub(crate) execution_number: TargetExecutionNumber,
    pub(crate) invocation: ActionId,
    pub(crate) phase: FailurePhase,
    pub(crate) detail: FailureDetail,
    pub(crate) cause: Cause,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryDecisionKind {
    Recheck,
    GaveUp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryDecision {
    pub(crate) kind: RecoveryDecisionKind,
    pub(crate) summary: String,
    pub(crate) reason: String,
}

impl RecoveryDecision {
    #[cfg(test)]
    pub(crate) fn recheck(summary: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: RecoveryDecisionKind::Recheck,
            summary: summary.into(),
            reason: reason.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn gave_up(summary: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: RecoveryDecisionKind::GaveUp,
            summary: summary.into(),
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryHandlerFailurePhase {
    Start,
    Execution,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryHandlerOutcome<Cause> {
    Starting,
    Running,
    Recheck {
        summary: String,
        reason: String,
    },
    GaveUp {
        summary: String,
        reason: String,
    },
    Failed {
        phase: RecoveryHandlerFailurePhase,
        cause: Cause,
    },
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryHandlerRecord<Cause> {
    pub(crate) kind: RecoveryHandlerKind,
    pub(crate) invocation: ActionId,
    pub(crate) outcome: RecoveryHandlerOutcome<Cause>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryRoundRecord<Cause> {
    pub(crate) number: RecoveryRoundNumber,
    pub(crate) failed_execution: ProvisionalTargetFailure<Cause>,
    pub(crate) handler: Option<RecoveryHandlerRecord<Cause>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryTerminalDisposition {
    Recovered {
        execution_number: TargetExecutionNumber,
    },
    Exhausted {
        execution_number: TargetExecutionNumber,
    },
    GaveUp {
        round: RecoveryRoundNumber,
    },
    HandlerFailed {
        round: RecoveryRoundNumber,
        phase: RecoveryHandlerFailurePhase,
    },
    Cancelled {
        round: RecoveryRoundNumber,
        active: ActiveStepInvocation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepRecoveryState<Cause> {
    pub(crate) configured_rounds: u8,
    pub(crate) handler_kind: Option<RecoveryHandlerKind>,
    pub(crate) rounds: Vec<RecoveryRoundRecord<Cause>>,
    pub(crate) terminal_disposition: Option<RecoveryTerminalDisposition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulingGate {
    Open,
    FailureStopped {
        primary_issue: PrimaryIssue,
    },
    Cancelling {
        reason: CancellationReason,
        prior_issue: Option<PrimaryIssue>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalizationGate<Deadline = ()> {
    Open,
    Cancelling {
        reason: CancellationReason,
        deadline: Option<Deadline>,
        force_abort: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowState<Deadline = ()> {
    Executing {
        gate: SchedulingGate,
    },
    Finalizing {
        trigger: FinalizationTrigger,
        gate: FinalizationGate<Deadline>,
        primary_issue: Option<PrimaryIssue>,
    },
    Succeeded,
    Failed {
        primary_issue: PrimaryIssue,
        later_cancellation: Option<CancellationReason>,
    },
    Cancelled {
        reason: CancellationReason,
    },
    /// The edge-scheduled walk entered a wait and the attempt ended on it (REQ-WFEXEC-61,
    /// REQ-WFEXEC-62, INV-ENGINE-20).
    Waiting {
        wait: EnteredWait,
    },
    /// The edge-scheduled walk took a backward or limited edge and the attempt ended on it;
    /// the run continues from the edge's target in a new attempt (REQ-WFEXEC-64,
    /// INV-ENGINE-20, INV-ENGINE-22).
    Continued {
        edge: TakenEdgeRecord,
    },
}

/// The continuing edge an attempt ended on (REQ-WFEXEC-64, REQ-CONTINUE-77): the step that
/// took it, the recorded child outcome, the step the run continues from and whether the
/// taking counts against the edge's limit (a limited edge's `then`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TakenEdgeRecord {
    pub step: String,
    pub outcome: String,
    pub target: String,
    pub counted: bool,
}

impl TakenEdgeRecord {
    /// The edge's key in the run record's `edges` counts (REQ-WFDEF-4).
    pub fn key(&self) -> String {
        edge_key(&self.step, &self.outcome)
    }
}

/// `<step>.<outcome>`, the key an edge is counted under (REQ-WFDEF-4).
pub(crate) fn edge_key(step: &str, outcome: &str) -> String {
    format!("{step}.{outcome}")
}

/// The wait an attempt ended on (REQ-WFEXEC-61, REQ-WFEXEC-62): the step it waits at, the
/// recorded child outcome whose edge was the wait (`None` for a `stop` wait, which no edge
/// enters), its kind, the target the run continues at once it resolves, for an approval the
/// step a hand-back continues at and, for an `external` wait, its authored rule as the
/// format spells it (REQ-WFFORMAT-97), which the wait record resolves (REQ-WFEXEC-65).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnteredWait {
    pub(crate) step: String,
    pub(crate) outcome: Option<String>,
    pub(crate) kind: WaitKind,
    pub(crate) then: String,
    pub(crate) on_hand_back: Option<String>,
    pub(crate) rule: Option<WaitRule>,
    /// The interruption that failed the child whose `repair` edge this wait is
    /// (REQ-WFEXEC-70); `None` for any other wait.
    pub(crate) interrupted: Option<ChildInterruption>,
    /// The reason of the interruption that ended the attempt on the wait it continued from,
    /// kept rather than entered (REQ-WFEXEC-72, INV-ENGINE-32); `None` for an entered wait.
    pub(crate) kept: Option<CancellationReason>,
}

impl EnteredWait {
    pub fn step(&self) -> &str {
        &self.step
    }

    pub fn outcome(&self) -> Option<&str> {
        self.outcome.as_deref()
    }

    pub const fn kind(&self) -> WaitKind {
        self.kind
    }

    /// The kind's name as the format spells it (REQ-WFFORMAT-92).
    pub const fn kind_name(&self) -> &'static str {
        self.kind.name()
    }

    pub fn then(&self) -> &str {
        &self.then
    }

    pub fn on_hand_back(&self) -> Option<&str> {
        self.on_hand_back.as_deref()
    }

    pub fn rule(&self) -> Option<&WaitRule> {
        self.rule.as_ref()
    }

    pub fn interrupted(&self) -> Option<&ChildInterruption> {
        self.interrupted.as_ref()
    }

    /// The interruption's reason when the attempt kept the wait it continued from instead
    /// of entering one (REQ-WFEXEC-72): its record is the one the attempt continued from.
    pub const fn kept(&self) -> Option<CancellationReason> {
        self.kept
    }
}

impl<Deadline> WorkflowState<Deadline> {
    pub fn map_deadline<Mapped>(
        self,
        map: impl FnOnce(Deadline) -> Mapped,
    ) -> WorkflowState<Mapped> {
        match self {
            Self::Executing { gate } => WorkflowState::Executing { gate },
            Self::Finalizing {
                trigger,
                gate,
                primary_issue,
            } => WorkflowState::Finalizing {
                trigger,
                gate: match gate {
                    FinalizationGate::Open => FinalizationGate::Open,
                    FinalizationGate::Cancelling {
                        reason,
                        deadline,
                        force_abort,
                    } => FinalizationGate::Cancelling {
                        reason,
                        deadline: deadline.map(map),
                        force_abort,
                    },
                },
                primary_issue,
            },
            Self::Succeeded => WorkflowState::Succeeded,
            Self::Failed {
                primary_issue,
                later_cancellation,
            } => WorkflowState::Failed {
                primary_issue,
                later_cancellation,
            },
            Self::Cancelled { reason } => WorkflowState::Cancelled { reason },
            Self::Waiting { wait } => WorkflowState::Waiting { wait },
            Self::Continued { edge } => WorkflowState::Continued { edge },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InheritedDisposition {
    Succeeded,
    Skipped,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputProducer {
    pub(crate) attempt_id: String,
    pub(crate) attempt_number: u64,
    pub(crate) node: String,
    pub(crate) output: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InheritedStepSeed<Output> {
    pub(crate) detail: InheritedDetail,
    pub(crate) disposition: InheritedDisposition,
    pub(crate) outputs: OutputSet<Output>,
    pub(crate) producers: BTreeMap<String, OutputProducer>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionSeed<Output> {
    steps: BTreeMap<String, InheritedStepSeed<Output>>,
    /// The step a continuation of an edge-scheduled workflow starts at instead of the
    /// definition's entry (REQ-CONTINUE-3, REQ-WFEXEC-58).
    entry: Option<String>,
    /// The run's limited-edge counts by `<step>.<outcome>` (REQ-WFEXEC-64, INV-ENGINE-22).
    edge_counts: BTreeMap<String, u64>,
    /// The wait an edge-scheduled continuation continues from, which an interruption before
    /// any of its steps has started keeps (REQ-WFEXEC-72, INV-ENGINE-32).
    resumed_wait: Option<EnteredWait>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionSeedError {
    UnknownStep,
    Finalizer,
    InvalidDetail,
    InvalidOutputs,
    InvalidProducer,
}

impl<Output> ExecutionSeed<Output> {
    pub(crate) fn empty() -> Self {
        Self {
            steps: BTreeMap::new(),
            entry: None,
            edge_counts: BTreeMap::new(),
            resumed_wait: None,
        }
    }

    /// Names the step an edge-scheduled continuation starts at; a dataflow workflow has no
    /// entry and ignores it (REQ-WFEXEC-58, INV-ENGINE-18).
    #[must_use]
    pub fn with_entry(mut self, step: impl Into<String>) -> Self {
        self.entry = Some(step.into());
        self
    }

    /// Carries the run record's edge counts, which decide a limited edge's `then` or `else`
    /// (REQ-WFEXEC-64, INV-ENGINE-22).
    #[must_use]
    pub fn with_edge_counts(mut self, counts: BTreeMap<String, u64>) -> Self {
        self.edge_counts = counts;
        self
    }

    /// Carries the wait the continuation continues from: an interruption accepted before any
    /// step of the attempt has started ends it on this wait, kept (REQ-WFEXEC-72,
    /// INV-ENGINE-32). A dataflow workflow ignores it.
    #[must_use]
    pub fn with_resumed_wait(mut self, wait: EnteredWait) -> Self {
        self.resumed_wait = Some(wait);
        self
    }

    pub(crate) fn new(
        admitted: &AdmittedWorkflow,
        steps: BTreeMap<String, InheritedStepSeed<Output>>,
    ) -> Result<Self, ExecutionSeedError> {
        for (id, seed) in &steps {
            let disposition = match seed.detail.prior_state {
                super::evidence::InheritedPriorState::Succeeded => InheritedDisposition::Succeeded,
                super::evidence::InheritedPriorState::Skipped => InheritedDisposition::Skipped,
                super::evidence::InheritedPriorState::Inherited => seed.disposition,
            };
            if disposition != seed.disposition {
                return Err(ExecutionSeedError::InvalidDetail);
            }
            if !admitted.workflow().definition.steps.contains_key(id) {
                return Err(
                    if admitted.workflow().definition.finalizers.contains_key(id) {
                        ExecutionSeedError::Finalizer
                    } else {
                        ExecutionSeedError::UnknownStep
                    },
                );
            }
            if seed.detail.prior_attempt_id.is_empty() || seed.detail.prior_attempt_number == 0 {
                return Err(ExecutionSeedError::InvalidDetail);
            }
            if (disposition == InheritedDisposition::Skipped && !seed.outputs.is_empty())
                || seed.producers.keys().ne(seed.outputs.keys())
            {
                return Err(ExecutionSeedError::InvalidOutputs);
            }
            if seed.producers.iter().any(|(output, producer)| {
                producer.attempt_id.is_empty()
                    || producer.attempt_number == 0
                    || producer.attempt_number > seed.detail.prior_attempt_number
                    || producer.node != *id
                    || producer.output != *output
            }) {
                return Err(ExecutionSeedError::InvalidProducer);
            }
        }
        Ok(Self {
            steps,
            entry: None,
            edge_counts: BTreeMap::new(),
            resumed_wait: None,
        })
    }
}

#[cfg(test)]
impl<Output> ExecutionSeed<Output> {
    pub(crate) fn inherited_step(&self, id: &str) -> Option<&InheritedStepSeed<Output>> {
        self.steps.get(id)
    }
}

impl<Output> Default for ExecutionSeed<Output> {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StepState<Output> {
    Pending,
    Starting,
    Running,
    CapturingOutputs,
    Recovering {
        round: RecoveryRoundNumber,
        handler: RecoveryHandlerActivity,
    },
    Cancelling {
        detail: CancellationDetail,
    },
    Succeeded {
        outputs: OutputSet<Output>,
    },
    Inherited {
        detail: InheritedDetail,
        disposition: InheritedDisposition,
        outputs: OutputSet<Output>,
    },
    Failed {
        detail: FailureDetail,
    },
    Blocked {
        detail: BlockedDetail,
    },
    Skipped {
        detail: ConditionFalseDetail,
    },
    NotRun {
        detail: NonExecutionDetail,
    },
    Cancelled {
        detail: CancellationDetail,
    },
}

impl<Output> StepState<Output> {
    fn kind(&self) -> StepStateKind {
        match self {
            Self::Pending => StepStateKind::Pending,
            Self::Starting => StepStateKind::Starting,
            Self::Running => StepStateKind::Running,
            Self::CapturingOutputs => StepStateKind::CapturingOutputs,
            Self::Recovering { .. } => StepStateKind::Recovering,
            Self::Cancelling { .. } => StepStateKind::Cancelling,
            Self::Succeeded { .. } => StepStateKind::Succeeded,
            Self::Inherited { .. } => StepStateKind::Inherited,
            Self::Failed { .. } => StepStateKind::Failed,
            Self::Blocked { .. } => StepStateKind::Blocked,
            Self::Skipped { .. } => StepStateKind::Skipped,
            Self::NotRun { .. } => StepStateKind::NotRun,
            Self::Cancelled { .. } => StepStateKind::Cancelled,
        }
    }

    fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Starting
                | Self::Running
                | Self::CapturingOutputs
                | Self::Recovering { .. }
                | Self::Cancelling { .. }
        )
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded { .. }
                | Self::Inherited { .. }
                | Self::Failed { .. }
                | Self::Blocked { .. }
                | Self::Skipped { .. }
                | Self::NotRun { .. }
                | Self::Cancelled { .. }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StepStateKind {
    Pending,
    Starting,
    Running,
    CapturingOutputs,
    Recovering,
    Cancelling,
    Succeeded,
    Inherited,
    Failed,
    Blocked,
    Skipped,
    NotRun,
    Cancelled,
}

impl StepStateKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::CapturingOutputs => "capturing_outputs",
            Self::Recovering => "recovering",
            Self::Cancelling => "cancelling",
            Self::Succeeded => "succeeded",
            Self::Inherited => "inherited",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Skipped => "skipped",
            Self::NotRun => "not_run",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepRuntimeState<Cause, Output> {
    pub(crate) state: StepState<Output>,
    pub(crate) current_action: Option<ActionId>,
    pub(crate) target_execution: Option<TargetExecutionNumber>,
    // Output capture replaces current_action, so retain the target's original invocation identity.
    pub(crate) target_invocation: Option<ActionId>,
    pub(crate) active_invocation: Option<ActiveStepInvocation>,
    pub(crate) recovery: Option<StepRecoveryState<Cause>>,
    pub(crate) condition_passed: bool,
    // The kind of the failure that settled the target without a recovery round, which the
    // terminal transition reports once the cause itself is no longer retained.
    pub(crate) failure_kind: Option<StepFailureKind>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeRecovery {
    retries: u8,
    handler_kind: Option<RecoveryHandlerKind>,
}

/// The action input carrying a `forEach` step's list to the child-run driver: not an
/// identifier, so no child input name collides with it.
pub(crate) const FOR_EACH_INPUT: &str = "@forEach";

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeStep {
    role: WorkflowNodeRole,
    is_agent: bool,
    failure_policy: FailurePolicy,
    condition: Option<ResolvedPredicate>,
    condition_values: BTreeMap<String, ResolvedValueSource>,
    prerequisites: Arc<[ResolvedDirectPrerequisite]>,
    evidence_prerequisites: Arc<[Prerequisite]>,
    inputs: BTreeMap<String, ResolvedValueSource>,
    declared_outputs: BTreeSet<String>,
    recovery: Option<RuntimeRecovery>,
    when: BTreeSet<FinalizationTrigger>,
    /// A `workflow` step's edges by child outcome; `None` for every other step.
    edges: Option<Arc<BTreeMap<String, RuntimeEdgeTarget>>>,
}

/// How the ordinary steps are selected (REQ-WFFORMAT-93): by satisfied prerequisites, or
/// along the edges of an edge-scheduled workflow from its entry (REQ-WFEXEC-58).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeScheduling {
    Dataflow,
    Edges { entry: String },
}

/// Where a settled `workflow` step's recorded outcome leads: a step, an exit, a limited
/// edge, or a wait the run enters (REQ-WFEXEC-58, REQ-WFEXEC-61, REQ-WFEXEC-64). A wait's
/// `then` is its target as the format spells it, a step id or `exit.<name>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeEdgeTarget {
    Step(String),
    Exit(String),
    Limited {
        then: String,
        limit: u32,
        else_: Box<RuntimeEdgeTarget>,
    },
    Wait {
        kind: WaitKind,
        then: String,
        on_hand_back: Option<String>,
        rule: Option<WaitRule>,
    },
}

impl RuntimeEdgeTarget {
    fn from_validated(target: &ValidatedEdgeTarget) -> Self {
        match target {
            ValidatedEdgeTarget::Step(step) => Self::Step(step.clone()),
            ValidatedEdgeTarget::Exit(name) => Self::Exit(name.clone()),
            ValidatedEdgeTarget::Limited { then, limit, else_ } => Self::Limited {
                then: then.clone(),
                limit: *limit,
                else_: Box::new(Self::from_validated(else_)),
            },
            ValidatedEdgeTarget::Wait {
                kind,
                then,
                on_hand_back,
                rule,
            } => Self::Wait {
                kind: *kind,
                then: edge_target_text(then),
                on_hand_back: on_hand_back.clone(),
                rule: rule.clone(),
            },
        }
    }
}

fn edge_target_text(target: &ValidatedEdgeTarget) -> String {
    match target {
        ValidatedEdgeTarget::Step(step) | ValidatedEdgeTarget::Limited { then: step, .. } => {
            step.clone()
        }
        ValidatedEdgeTarget::Exit(name) => format!("exit.{name}"),
        ValidatedEdgeTarget::Wait { then, .. } => edge_target_text(then),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeExport {
    step: String,
    output: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeDefinition {
    steps: BTreeMap<String, RuntimeStep>,
    ordinary_ids: BTreeSet<String>,
    finalizer_ids: BTreeSet<String>,
    finalizer_presentation_order: Vec<String>,
    exports: BTreeMap<String, RuntimeExport>,
    maximum_parallel_steps: NonZeroUsize,
    maximum_transitions: u64,
    text_inputs: BTreeMap<String, Arc<str>>,
    json_inputs: BTreeMap<String, Arc<serde_json::Value>>,
    scheduling: RuntimeScheduling,
    /// The ordinary steps in document order; an edge to a step not later than its producer
    /// continues the run (REQ-WFEXEC-64).
    step_order: Vec<String>,
    /// The run's limited-edge counts, from the seed (REQ-WFEXEC-64, INV-ENGINE-22).
    edge_counts: BTreeMap<String, u64>,
}

impl RuntimeDefinition {
    fn from_admitted(admitted: &AdmittedWorkflow) -> Self {
        let mut definition = Self::from_workflow(
            &admitted.workflow().definition,
            admitted.execution().limits().maximum_parallel_steps(),
            admitted.capacity().maximum_transitions,
        );
        definition.text_inputs = admitted
            .inputs()
            .values()
            .iter()
            .filter_map(|(name, input)| match input {
                ResolvedInput::Text(value) => Some((name.clone(), Arc::clone(value))),
                ResolvedInput::Json(_) | ResolvedInput::File(_) | ResolvedInput::Attachments(_) => {
                    None
                }
            })
            .collect();
        definition.json_inputs = admitted
            .inputs()
            .values()
            .iter()
            .filter_map(|(name, input)| match input {
                ResolvedInput::Json(value) => Some((name.clone(), value.value_arc())),
                ResolvedInput::Text(_) | ResolvedInput::File(_) | ResolvedInput::Attachments(_) => {
                    None
                }
            })
            .collect();
        definition
    }

    fn from_workflow(
        workflow: &ValidatedWorkflow,
        maximum_parallel_steps: NonZeroUsize,
        maximum_transitions: u64,
    ) -> Self {
        let ordinary_ids = workflow.steps.keys().cloned().collect();
        let finalizer_ids = workflow.finalizers.keys().cloned().collect();
        let steps = workflow
            .steps
            .iter()
            .map(|(id, step)| {
                (
                    id.clone(),
                    runtime_step(
                        WorkflowNodeRole::Step,
                        step,
                        workflow.recoveries.get(id).and_then(Option::as_ref),
                        BTreeSet::new(),
                    ),
                )
            })
            .chain(
                workflow
                    .finalizers
                    .iter()
                    .map(|(id, finalizer)| (id.clone(), runtime_finalizer(finalizer))),
            )
            .collect();
        let exports = workflow
            .exports
            .iter()
            .map(|(name, source)| {
                (
                    name.clone(),
                    RuntimeExport {
                        step: source.node.id.clone(),
                        output: source.output.clone(),
                    },
                )
            })
            .collect();
        Self {
            steps,
            ordinary_ids,
            finalizer_ids,
            finalizer_presentation_order: workflow.finalizer_presentation_order.clone(),
            exports,
            maximum_parallel_steps,
            maximum_transitions,
            text_inputs: BTreeMap::new(),
            json_inputs: BTreeMap::new(),
            scheduling: match &workflow.scheduling {
                Scheduling::Dataflow => RuntimeScheduling::Dataflow,
                Scheduling::Edges { entry } => RuntimeScheduling::Edges {
                    entry: entry.clone(),
                },
            },
            step_order: workflow.source_order.clone(),
            edge_counts: BTreeMap::new(),
        }
    }
}

fn runtime_finalizer(finalizer: &ValidatedFinalizer) -> RuntimeStep {
    runtime_step(
        WorkflowNodeRole::Finalizer,
        &finalizer.body,
        None,
        finalizer.when.clone(),
    )
}

fn runtime_step(
    role: WorkflowNodeRole,
    step: &ValidatedStep,
    recovery: Option<&ValidatedStepRecovery>,
    when: BTreeSet<FinalizationTrigger>,
) -> RuntimeStep {
    let common = step.common();
    RuntimeStep {
        role,
        is_agent: matches!(step, ValidatedStep::Agent(_)),
        failure_policy: common.failure_policy,
        condition: common.condition.clone(),
        condition_values: common.condition_values.clone(),
        prerequisites: Arc::from(common.prerequisites.clone()),
        evidence_prerequisites: Arc::from(common.evidence_prerequisites.clone()),
        inputs: match step {
            ValidatedStep::Command(command) => command
                .inputs
                .iter()
                .map(|(name, reference)| (name.clone(), reference.source.clone()))
                .collect(),
            // A fan-out's list reaches the child-run driver as the action input `@forEach`,
            // a key no child input can take (REQ-WFEXEC-66).
            ValidatedStep::Workflow(workflow) => workflow
                .inputs
                .iter()
                .map(|(name, input)| (name.clone(), input.reference.source.clone()))
                .chain(
                    workflow
                        .for_each
                        .as_ref()
                        .map(|for_each| (FOR_EACH_INPUT.to_owned(), for_each.items.source.clone())),
                )
                .collect(),
            ValidatedStep::Agent(agent) => agent
                .agent
                .message
                .text
                .iter()
                .chain(&agent.agent.message.attachments)
                .filter_map(|source| match source {
                    ValidatedMessageSource::Reference { source, .. } => match source {
                        ResolvedValueSource::Output(output) => {
                            Some((output.reference(), source.clone()))
                        }
                        ResolvedValueSource::FinalizationContext => Some((
                            "finalization.context".to_owned(),
                            ResolvedValueSource::FinalizationContext,
                        )),
                        ResolvedValueSource::Input(_)
                        | ResolvedValueSource::Wait(_)
                        | ResolvedValueSource::ControlsAnswers
                        | ResolvedValueSource::Item { .. } => None,
                    },
                    ValidatedMessageSource::File { .. } => None,
                })
                .collect(),
        },
        declared_outputs: common.outputs.keys().cloned().collect(),
        recovery: recovery.map(|recovery| RuntimeRecovery {
            retries: recovery.retries,
            handler_kind: recovery
                .handler
                .as_ref()
                .map(RecoveryHandlerKind::from_validated),
        }),
        when,
        edges: match step {
            ValidatedStep::Workflow(workflow) => Some(Arc::new(
                workflow
                    .edges
                    .iter()
                    .map(|(outcome, target)| {
                        (outcome.clone(), RuntimeEdgeTarget::from_validated(target))
                    })
                    .collect(),
            )),
            ValidatedStep::Command(_) | ValidatedStep::Agent(_) => None,
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExportUnavailableReason {
    Failed,
    Blocked,
    Skipped,
    NotRun,
    TriggerNotSelected,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExportValue<Output> {
    Available { output: Output },
    Unavailable { reason: ExportUnavailableReason },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OrdinaryOutcome {
    Succeeded,
    Failed {
        primary_issue: PrimaryIssue,
        later_cancellation: Option<CancellationReason>,
    },
    Cancelled {
        reason: CancellationReason,
    },
    Waiting {
        wait: EnteredWait,
    },
    Continued {
        edge: TakenEdgeRecord,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunCancellationPhase {
    Ordinary,
    Finalization,
}

impl RunCancellationPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Finalization => "finalization",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ForceAbortEvidence {
    pub reason: CancellationReason,
    pub phase: RunCancellationPhase,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizationCancellation<Deadline = ()> {
    pub reason: CancellationReason,
    pub deadline: Option<Deadline>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizerResult {
    pub finalizer: String,
    pub failure_policy: FailurePolicy,
    pub disposition: StepState<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizationSummary<Deadline = ()> {
    pub trigger: FinalizationTrigger,
    pub finalizers: Vec<FinalizerResult>,
    pub cancellation: Option<FinalizationCancellation<Deadline>>,
    pub force_abort: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FinalizationRuntime<Deadline> {
    trigger: FinalizationTrigger,
    context: Arc<[u8]>,
    ordinary_outcome: OrdinaryOutcome,
    cancellation: Option<FinalizationCancellation<Deadline>>,
    force_abort: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeState<Cause, Output, Deadline = ()> {
    definition: Arc<RuntimeDefinition>,
    pub(crate) finalization_identity: Option<(String, String)>,
    pub(crate) workflow: WorkflowState<Deadline>,
    // One namespace and one reducer map; role is retained in the definition and events.
    pub(crate) steps: BTreeMap<String, StepRuntimeState<Cause, Output>>,
    pub(crate) output_producers: BTreeMap<(String, String), OutputProducer>,
    pub(crate) exports: Option<ExportSet<Output>>,
    pub(crate) finalization_summary: Option<FinalizationSummary<Deadline>>,
    finalization: Option<FinalizationRuntime<Deadline>>,
    pub(crate) force_abort: Option<ForceAbortEvidence>,
    pub(crate) last_cancellation_operation: Option<CancellationOperationId>,
    /// Whether the ordinary stop was an interruption: its stopped steps settle `failed`
    /// with `interrupted` instead of `cancelled` (REQ-WFEXEC-69).
    pub(crate) interrupted: bool,
    /// The `workflow` steps whose child had not started when the interruption was accepted:
    /// stopped, they fail at `start` (REQ-WFEXEC-70).
    unstarted_children: BTreeSet<String>,
    /// The `workflow` step whose child's completion arrived under the interruption and is
    /// routed on its edge, with the interruption that failed the child (REQ-WFEXEC-70,
    /// INV-ENGINE-29): the walk may then end while the gate is `Cancelling`.
    routed_child: Option<(String, Option<ChildInterruption>)>,
    pub(crate) last_transition_sequence: TransitionSequence,
    transition_capacity_exceeded: bool,
    /// The exit an edge-scheduled walk took (REQ-WFEXEC-58); `None` until then, and after
    /// a walk that ended without one.
    pub(crate) exit_outcome: Option<String>,
    /// The wait the walk entered (REQ-WFEXEC-61, REQ-WFEXEC-62); once set no step is
    /// authorized and the attempt ends `waiting` (INV-ENGINE-20).
    pub(crate) wait: Option<EnteredWait>,
    /// The continuing edge the walk took (REQ-WFEXEC-64); once set no step is authorized and
    /// the attempt ends `continued` (INV-ENGINE-20).
    pub(crate) continued: Option<TakenEdgeRecord>,
    /// The wait an edge-scheduled continuation continues from (`ExecutionSeed::
    /// with_resumed_wait`), which an interruption before any step of the attempt has started
    /// keeps (REQ-WFEXEC-72, INV-ENGINE-32).
    resumed_wait: Option<EnteredWait>,
}

impl<Cause, Output, Deadline> RuntimeState<Cause, Output, Deadline> {
    pub(crate) const fn transition_capacity_exceeded(&self) -> bool {
        self.transition_capacity_exceeded
    }

    pub(crate) fn invocation_is_agent(&self, id: &str, handler: bool) -> bool {
        self.definition.steps.get(id).is_some_and(|step| {
            if handler {
                step.recovery
                    .as_ref()
                    .and_then(|recovery| recovery.handler_kind)
                    == Some(RecoveryHandlerKind::Agent)
            } else {
                step.is_agent
            }
        })
    }
}

#[cfg(test)]
impl<Cause, Output, Deadline> RuntimeState<Cause, Output, Deadline> {
    pub(crate) fn admitted_transition_ceiling(&self) -> u64 {
        self.definition.maximum_transitions
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CancellationRequest<Deadline> {
    pub(crate) reason: CancellationReason,
    pub(crate) deadline: Deadline,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Occurrence<Provisional, Cause, Output, Deadline> {
    StepStarted {
        step: String,
        action: ActionId,
    },
    StepStartFailed {
        step: String,
        action: ActionId,
        cause: Cause,
    },
    StepExecutionCompleted {
        step: String,
        action: ActionId,
        provisional: Provisional,
    },
    StepExecutionFailed {
        step: String,
        action: ActionId,
        cause: Cause,
    },
    OutputsCaptured {
        step: String,
        action: ActionId,
        outputs: OutputSet<Output>,
    },
    OutputCaptureFailed {
        step: String,
        action: ActionId,
        cause: Cause,
    },
    RecoveryHandlerStarted {
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
    },
    RecoveryHandlerStartFailed {
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        cause: Cause,
    },
    RecoveryHandlerCompleted {
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        decision: RecoveryDecision,
    },
    RecoveryHandlerExecutionFailed {
        step: String,
        round: RecoveryRoundNumber,
        action: ActionId,
        cause: Cause,
    },
    CancellationOperationRequested {
        operation: CancellationOperationId,
        reason: CancellationReason,
        deadline: Deadline,
    },
    OrdinaryCancellationOperationRequested {
        operation: CancellationOperationId,
        reason: CancellationReason,
        deadline: Deadline,
    },
    /// The local CLI's interruption (REQ-WFEXEC-69): stops like a cancellation during
    /// ordinary execution and acts as one in any other phase.
    InterruptionOperationRequested {
        operation: CancellationOperationId,
        reason: CancellationReason,
        deadline: Deadline,
    },
    ForceAbortRequested {
        operation: CancellationOperationId,
        deadline: Deadline,
    },
    StepQuiesced {
        step: String,
        action: ActionId,
    },
    /// A stop control in force for a `workflow` step the reducer authorized: the step does
    /// not start and the run enters a `stop` wait at it (REQ-WFEXEC-62).
    StopEntered {
        step: String,
        action: ActionId,
    },
}

// Runtime and terminal outcomes intentionally remain distinct: the former exists only while
// crossing the phase boundary, while the latter is the adapter-facing finish contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunOutcome {
    Succeeded,
    Failed {
        primary_issue: PrimaryIssue,
        later_cancellation: Option<CancellationReason>,
    },
    Cancelled {
        reason: CancellationReason,
    },
    /// The attempt ended on a wait (REQ-WFEXEC-15, REQ-WFEXEC-61): no finalizer ran.
    Waiting {
        wait: EnteredWait,
    },
    /// The attempt took a backward or limited edge (REQ-WFEXEC-15, REQ-WFEXEC-64): no
    /// finalizer ran and the run continues from the edge's target.
    Continued {
        edge: TakenEdgeRecord,
    },
}

impl RunOutcome {
    /// The interruption that failed the attempt (REQ-WFEXEC-69): a `Failed` outcome whose
    /// primary issue is a step that failed `interrupted`, with the reason the result records
    /// as its cancellation. A child run reports it to its parent's step (REQ-WFEXEC-70).
    pub fn interruption(&self) -> Option<ChildInterruption> {
        let Self::Failed {
            primary_issue,
            later_cancellation: Some(reason),
        } = self
        else {
            return None;
        };
        let PrimaryIssueDetail::Failed(detail) = &primary_issue.detail else {
            return None;
        };
        detail.is_interrupted().then(|| ChildInterruption {
            reason: *reason,
            step: primary_issue.node.id.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ActionInput<Output> {
    WorkflowInput,
    Output(Output),
    FinalizationContext(Arc<[u8]>),
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Action<Provisional, Cause, Output, Deadline> {
    StartStep {
        step: String,
        execution_number: TargetExecutionNumber,
        inputs: BTreeMap<String, ActionInput<Output>>,
    },
    StartRecoveryHandler {
        step: String,
        round: RecoveryRoundNumber,
        kind: RecoveryHandlerKind,
        history: Vec<RecoveryRoundRecord<Cause>>,
    },
    CaptureOutputs {
        step: String,
        provisional: Provisional,
    },
    ReleaseStepResources {
        provisional: Provisional,
    },
    CancelStep {
        step: String,
        active: ActiveStepInvocation,
        reason: CancellationReason,
        deadline: Deadline,
    },
    ForceAbortStep {
        step: String,
        active: ActiveStepInvocation,
        reason: CancellationReason,
        deadline: Deadline,
    },
    FinishRun {
        outcome: RunOutcome,
        exports: ExportSet<Output>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestedAction<Provisional, Cause, Output, Deadline> {
    pub(crate) id: ActionId,
    pub(crate) action: Action<Provisional, Cause, Output, Deadline>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionEvent<Deadline> {
    Step {
        sequence: TransitionSequence,
        step: String,
        role: WorkflowNodeRole,
        failure_policy: FailurePolicy,
        from: StepStateKind,
        to: StepStateKind,
    },
    Workflow {
        sequence: TransitionSequence,
        from: WorkflowState<Deadline>,
        to: Box<WorkflowState<Deadline>>,
    },
    CancellationAccepted {
        sequence: TransitionSequence,
        reason: CancellationReason,
        deadline: Deadline,
    },
    FinalizationCancellationAccepted {
        sequence: TransitionSequence,
        reason: CancellationReason,
        deadline: Deadline,
    },
    ForceAbortAccepted {
        sequence: TransitionSequence,
        reason: CancellationReason,
        phase: RunCancellationPhase,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Reduction<Provisional, Cause, Output, Deadline> {
    pub(crate) state: RuntimeState<Cause, Output, Deadline>,
    pub(crate) events: Vec<TransitionEvent<Deadline>>,
    pub(crate) actions: Vec<RequestedAction<Provisional, Cause, Output, Deadline>>,
    pub(crate) occurrence_accepted: bool,
}

#[cfg(test)]
pub(crate) fn initialize<Provisional, Cause, Output, Deadline>(
    admitted: &AdmittedWorkflow,
    initial_cancellation: Option<CancellationRequest<Deadline>>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Cause: Clone,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    initialize_seeded_with_operation(
        admitted,
        ExecutionSeed::empty(),
        initial_cancellation.map(|request| InitialCancellation::Graceful {
            request,
            operation: None,
        }),
    )
}

pub(super) enum InitialCancellation<Deadline> {
    Graceful {
        request: CancellationRequest<Deadline>,
        operation: Option<CancellationOperationId>,
    },
    Interruption {
        request: CancellationRequest<Deadline>,
        operation: Option<CancellationOperationId>,
    },
    ForceAbort {
        operation: CancellationOperationId,
        deadline: Deadline,
    },
}

pub(super) fn initialize_seeded_with_operation<Provisional, Cause, Output, Deadline>(
    admitted: &AdmittedWorkflow,
    seed: ExecutionSeed<Output>,
    initial_cancellation: Option<InitialCancellation<Deadline>>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Cause: Clone,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    initialize_seeded_with_identity(admitted, seed, initial_cancellation, None)
}

pub(super) fn initialize_seeded_with_identity<Provisional, Cause, Output, Deadline>(
    admitted: &AdmittedWorkflow,
    seed: ExecutionSeed<Output>,
    initial_cancellation: Option<InitialCancellation<Deadline>>,
    identity: Option<(String, String)>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Cause: Clone,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    initialize_seeded_definition(
        ExecutionStart {
            definition: RuntimeDefinition::from_admitted(admitted),
            initial_cancellation,
            identity,
        },
        seed,
    )
}

pub(in crate::workflow) struct ExecutionStart<Deadline> {
    pub(in crate::workflow) definition: RuntimeDefinition,
    pub(in crate::workflow) initial_cancellation: Option<InitialCancellation<Deadline>>,
    pub(in crate::workflow) identity: Option<(String, String)>,
}

pub(in crate::workflow) fn initialize_seeded_definition<Provisional, Cause, Output, Deadline>(
    mut start: ExecutionStart<Deadline>,
    seed: ExecutionSeed<Output>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Cause: Clone,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    // A continuation of an edge-scheduled workflow starts the step it names, so that step
    // is the walk's entry for this attempt (REQ-CONTINUE-3, INV-ENGINE-18).
    if let (RuntimeScheduling::Edges { entry }, Some(named)) =
        (&mut start.definition.scheduling, &seed.entry)
        && start.definition.ordinary_ids.contains(named)
    {
        entry.clone_from(named);
    }
    start.definition.edge_counts.clone_from(&seed.edge_counts);
    let output_producers = seed
        .steps
        .iter()
        .flat_map(|(step, seed)| {
            seed.producers
                .iter()
                .map(move |(output, producer)| ((step.clone(), output.clone()), producer.clone()))
        })
        .collect();
    let steps = start
        .definition
        .steps
        .iter()
        .map(|(step, definition)| {
            let inherited = seed.steps.get(step);
            (
                step.clone(),
                StepRuntimeState {
                    state: inherited.map_or(StepState::Pending, |seed| StepState::Inherited {
                        detail: seed.detail.clone(),
                        disposition: seed.disposition,
                        outputs: seed.outputs.clone(),
                    }),
                    current_action: None,
                    target_execution: None,
                    target_invocation: None,
                    active_invocation: None,
                    recovery: inherited
                        .is_none()
                        .then_some(definition.recovery)
                        .flatten()
                        .map(|recovery| StepRecoveryState {
                            configured_rounds: recovery.retries,
                            handler_kind: recovery.handler_kind,
                            rounds: Vec::with_capacity(usize::from(recovery.retries)),
                            terminal_disposition: None,
                        }),
                    condition_passed: false,
                    failure_kind: None,
                },
            )
        })
        .collect();
    let mut reduction = Reduction {
        state: RuntimeState {
            definition: Arc::new(start.definition),
            finalization_identity: start.identity,
            workflow: WorkflowState::Executing {
                gate: SchedulingGate::Open,
            },
            steps,
            output_producers,
            exports: None,
            finalization_summary: None,
            finalization: None,
            force_abort: None,
            last_cancellation_operation: None,
            interrupted: false,
            unstarted_children: BTreeSet::new(),
            routed_child: None,
            last_transition_sequence: TransitionSequence::default(),
            transition_capacity_exceeded: false,
            exit_outcome: None,
            wait: None,
            continued: None,
            resumed_wait: seed.resumed_wait,
        },
        events: Vec::new(),
        actions: Vec::new(),
        occurrence_accepted: true,
    };
    if let Some(cancellation) = start.initial_cancellation {
        match cancellation {
            InitialCancellation::Graceful { request, operation } => {
                apply_cancellation(&mut reduction, request, operation);
            }
            InitialCancellation::Interruption { request, operation } => {
                apply_interruption(&mut reduction, request, operation);
            }
            InitialCancellation::ForceAbort {
                operation,
                deadline,
            } => {
                apply_force_abort(&mut reduction, operation, deadline);
            }
        }
    }
    stabilize(&mut reduction);
    reduction
}

pub(crate) fn reduce<Provisional, Cause, Output, Deadline>(
    current: &RuntimeState<Cause, Output, Deadline>,
    occurrence: Occurrence<Provisional, Cause, Output, Deadline>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Provisional: ProvisionalStepResources,
    Cause: Clone + NodeFailureSource,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    if matches!(
        &current.workflow,
        WorkflowState::Succeeded
            | WorkflowState::Failed { .. }
            | WorkflowState::Cancelled { .. }
            | WorkflowState::Waiting { .. }
            | WorkflowState::Continued { .. }
    ) {
        return discard_occurrence(current, occurrence);
    }
    let mut reduction = Reduction {
        state: current.clone(),
        events: Vec::new(),
        actions: Vec::new(),
        occurrence_accepted: false,
    };
    if !apply_occurrence(&mut reduction, occurrence) {
        return reduction;
    }
    reduction.occurrence_accepted = true;
    stabilize(&mut reduction);
    reduction
}

pub(crate) fn discard_occurrence<Provisional, Cause, Output, Deadline>(
    current: &RuntimeState<Cause, Output, Deadline>,
    occurrence: Occurrence<Provisional, Cause, Output, Deadline>,
) -> Reduction<Provisional, Cause, Output, Deadline>
where
    Provisional: ProvisionalStepResources,
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    Reduction {
        state: current.clone(),
        events: Vec::new(),
        actions: occurrence_resource_release(occurrence)
            .into_iter()
            .collect(),
        occurrence_accepted: false,
    }
}

pub(crate) fn occurrence_resource_release<Provisional, Cause, Output, Deadline>(
    occurrence: Occurrence<Provisional, Cause, Output, Deadline>,
) -> Option<RequestedAction<Provisional, Cause, Output, Deadline>>
where
    Provisional: ProvisionalStepResources,
{
    let Occurrence::StepExecutionCompleted {
        action,
        provisional,
        ..
    } = occurrence
    else {
        return None;
    };
    provisional.requires_release().then_some(RequestedAction {
        id: action,
        action: Action::ReleaseStepResources { provisional },
    })
}

fn apply_occurrence<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    occurrence: Occurrence<Provisional, Cause, Output, Deadline>,
) -> bool
where
    Provisional: ProvisionalStepResources,
    Cause: Clone + NodeFailureSource,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    match occurrence {
        Occurrence::StepStarted { step, action } => {
            if !step_accepts(&reduction.state, &step, StepStateKind::Starting, action) {
                return false;
            }
            transition_step(reduction, &step, StepState::Running, Some(action));
        }
        Occurrence::StepStartFailed {
            step,
            action,
            cause,
        } => {
            return apply_step_failure(
                reduction,
                step,
                action,
                StepStateKind::Starting,
                FailurePhase::Start,
                cause,
            );
        }
        Occurrence::StepExecutionCompleted {
            step,
            action,
            provisional,
        } => {
            let routed = match interrupted_completion(&reduction.state, &step, action, &provisional)
            {
                InterruptedCompletion::Ordinary => false,
                InterruptedCompletion::Route(interruption) => {
                    reduction.state.routed_child = Some((step.clone(), interruption));
                    true
                }
                InterruptedCompletion::Fail => {
                    if provisional.requires_release() {
                        reduction.actions.push(RequestedAction {
                            id: action,
                            action: Action::ReleaseStepResources { provisional },
                        });
                    }
                    settle_interrupted(reduction, &step, FailureDetail::interrupted_in_execution());
                    return true;
                }
            };
            if !routed && !step_accepts(&reduction.state, &step, StepStateKind::Running, action) {
                if provisional.requires_release() {
                    reduction.actions.push(RequestedAction {
                        id: action,
                        action: Action::ReleaseStepResources { provisional },
                    });
                }
                return false;
            }
            let sequence = transition_step(reduction, &step, StepState::CapturingOutputs, None);
            if step_declares_outputs(&reduction.state, &step) {
                let capture_action = ActionId::for_transition(sequence);
                set_current_action(&mut reduction.state, &step, capture_action);
                reduction.actions.push(RequestedAction {
                    id: capture_action,
                    action: Action::CaptureOutputs { step, provisional },
                });
            } else {
                let release_resources = provisional.requires_release().then(|| RequestedAction {
                    id: ActionId::for_transition(sequence),
                    action: Action::ReleaseStepResources { provisional },
                });
                mark_recovered(reduction, &step);
                transition_step(
                    reduction,
                    &step,
                    StepState::Succeeded {
                        outputs: BTreeMap::new(),
                    },
                    None,
                );
                clear_active_invocation(&mut reduction.state, &step);
                if let Some(release_resources) = release_resources {
                    // Resource release must precede any dependent starts appended by stabilize.
                    reduction.actions.push(release_resources);
                }
            }
        }
        Occurrence::StepExecutionFailed {
            step,
            action,
            cause,
        } => {
            return apply_step_failure(
                reduction,
                step,
                action,
                StepStateKind::Running,
                FailurePhase::Execution,
                cause,
            );
        }
        Occurrence::OutputsCaptured {
            step,
            action,
            outputs,
        } => {
            if !step_accepts(
                &reduction.state,
                &step,
                StepStateKind::CapturingOutputs,
                action,
            ) || !outputs_match_declaration(&reduction.state, &step, &outputs)
            {
                return false;
            }
            mark_recovered(reduction, &step);
            transition_step(reduction, &step, StepState::Succeeded { outputs }, None);
            clear_active_invocation(&mut reduction.state, &step);
        }
        Occurrence::OutputCaptureFailed {
            step,
            action,
            cause,
        } => {
            return apply_step_failure(
                reduction,
                step,
                action,
                StepStateKind::CapturingOutputs,
                FailurePhase::OutputCapture,
                cause,
            );
        }
        Occurrence::RecoveryHandlerStarted {
            step,
            round,
            action,
        } => {
            if !handler_accepts(
                &reduction.state,
                &step,
                round,
                RecoveryHandlerActivity::Starting,
                action,
            ) {
                return false;
            }
            set_handler_outcome(
                &mut reduction.state,
                &step,
                round,
                RecoveryHandlerOutcome::Running,
            );
            transition_step(
                reduction,
                &step,
                StepState::Recovering {
                    round,
                    handler: RecoveryHandlerActivity::Running,
                },
                Some(action),
            );
        }
        Occurrence::RecoveryHandlerStartFailed {
            step,
            round,
            action,
            cause,
        } => {
            return apply_handler_failure(
                reduction,
                step,
                round,
                action,
                RecoveryHandlerActivity::Starting,
                RecoveryHandlerFailurePhase::Start,
                cause,
            );
        }
        Occurrence::RecoveryHandlerCompleted {
            step,
            round,
            action,
            decision,
        } => {
            return apply_handler_decision(reduction, step, round, action, decision);
        }
        Occurrence::RecoveryHandlerExecutionFailed {
            step,
            round,
            action,
            cause,
        } => {
            return apply_handler_failure(
                reduction,
                step,
                round,
                action,
                RecoveryHandlerActivity::Running,
                RecoveryHandlerFailurePhase::Execution,
                cause,
            );
        }
        Occurrence::CancellationOperationRequested {
            operation,
            reason,
            deadline,
        } => {
            if reason == CancellationReason::ForceAbort
                || stale_operation(&reduction.state, operation)
            {
                return false;
            }
            return apply_cancellation(
                reduction,
                CancellationRequest { reason, deadline },
                Some(operation),
            );
        }
        Occurrence::OrdinaryCancellationOperationRequested {
            operation,
            reason,
            deadline,
        } => {
            return apply_ordinary_only_cancellation(
                reduction,
                CancellationRequest { reason, deadline },
                operation,
            );
        }
        Occurrence::InterruptionOperationRequested {
            operation,
            reason,
            deadline,
        } => {
            if reason == CancellationReason::ForceAbort
                || stale_operation(&reduction.state, operation)
            {
                return false;
            }
            return apply_interruption(
                reduction,
                CancellationRequest { reason, deadline },
                Some(operation),
            );
        }
        Occurrence::ForceAbortRequested {
            operation,
            deadline,
        } => {
            if stale_operation(&reduction.state, operation) {
                return false;
            }
            return apply_force_abort(reduction, operation, deadline);
        }
        Occurrence::StepQuiesced { step, action } => {
            let Some((cancelling_reason, active)) =
                cancelling_step(&reduction.state, &step, action)
            else {
                return false;
            };
            let terminal_reason = if action.is_force_abort() {
                CancellationReason::ForceAbort
            } else {
                cancelling_reason
            };
            mark_recovery_cancelled(&mut reduction.state, &step, active);
            if reduction.state.interrupted
                && !action.is_force_abort()
                && step_role(&reduction.state, &step) == Some(WorkflowNodeRole::Step)
            {
                // A continuation's entry `workflow` step whose child had not started keeps the
                // wait the attempt continues from (REQ-WFEXEC-72, INV-ENGINE-32).
                if reduction.state.unstarted_children.contains(&step)
                    && entry_step(&reduction.state) == Some(step.as_str())
                    && let Some(kept) = kept_resumed_wait(&reduction.state)
                {
                    settle_unstarted_on_wait(reduction, &step, kept);
                    return true;
                }
                // Any other `workflow` step whose child had not started fails at `start`
                // (REQ-WFEXEC-70).
                let detail = if reduction.state.unstarted_children.contains(&step) {
                    FailureDetail::interrupted_before_start()
                } else {
                    FailureDetail::interrupted_in_execution()
                };
                settle_interrupted(reduction, &step, detail);
            } else {
                transition_step(
                    reduction,
                    &step,
                    StepState::Cancelled {
                        detail: CancellationDetail::new(terminal_reason),
                    },
                    None,
                );
                clear_active_invocation(&mut reduction.state, &step);
            }
        }
        Occurrence::StopEntered { step, action } => {
            return enter_stop_wait(reduction, step, action);
        }
    }
    true
}

// A stop is entered instead of starting a `workflow` step the walk authorized: the step
// never started, so it drops its target invocation and becomes `not_reached` with every
// other unstarted step, and the attempt ends on a `stop` wait whose `then` is the step
// (REQ-WFEXEC-62, INV-ENGINE-20).
fn enter_stop_wait<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    action: ActionId,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let edge_step = matches!(
        reduction.state.definition.scheduling,
        RuntimeScheduling::Edges { .. }
    ) && reduction
        .state
        .definition
        .steps
        .get(&step)
        .is_some_and(|definition| definition.edges.is_some());
    if !edge_step
        || reduction.state.wait.is_some()
        || reduction.state.continued.is_some()
        || reduction.state.exit_outcome.is_some()
        || !matches!(
            &reduction.state.workflow,
            WorkflowState::Executing {
                gate: SchedulingGate::Open
            }
        )
        || !step_accepts(&reduction.state, &step, StepStateKind::Starting, action)
    {
        return false;
    }
    let wait = EnteredWait {
        step: step.clone(),
        outcome: None,
        kind: WaitKind::Stop,
        then: step.clone(),
        on_hand_back: None,
        rule: None,
        interrupted: None,
        kept: None,
    };
    settle_unstarted_on_wait(reduction, &step, wait);
    true
}

// A step that never started drops its target invocation and becomes `not_reached` with every
// other unstarted step, and the attempt ends on `wait`: the `stop` wait entered before it
// (REQ-WFEXEC-62) or the wait an interruption kept (REQ-WFEXEC-72).
fn settle_unstarted_on_wait<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: &str,
    wait: EnteredWait,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if let Some(runtime) = reduction.state.steps.get_mut(step) {
        runtime.target_execution = None;
        runtime.target_invocation = None;
        runtime.active_invocation = None;
    }
    transition_step(
        reduction,
        step,
        StepState::NotRun {
            detail: NonExecutionDetail::not_reached(),
        },
        None,
    );
    reduction.state.wait = Some(wait);
    mark_unstarted_not_reached(reduction);
}

/// The step an edge-scheduled walk starts at (REQ-WFEXEC-58), a continuation's included
/// (REQ-CONTINUE-3); `None` for a dataflow workflow.
fn entry_step<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<&str> {
    match &state.definition.scheduling {
        RuntimeScheduling::Edges { entry } => Some(entry),
        RuntimeScheduling::Dataflow => None,
    }
}

/// The wait the attempt continues from, marked kept with the interruption's reason, when an
/// interruption accepted with no issue recorded stops an edge-scheduled continuation of a
/// wait before its walk ended (REQ-WFEXEC-72, INV-ENGINE-32); the caller establishes that no
/// step of the attempt has started.
fn kept_resumed_wait<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<EnteredWait> {
    let WorkflowState::Executing {
        gate:
            SchedulingGate::Cancelling {
                reason,
                prior_issue: None,
            },
    } = &state.workflow
    else {
        return None;
    };
    if !state.interrupted
        || entry_step(state).is_none()
        || state.wait.is_some()
        || state.continued.is_some()
        || state.exit_outcome.is_some()
    {
        return None;
    }
    state.resumed_wait.as_ref().map(|wait| EnteredWait {
        kept: Some(*reason),
        ..wait.clone()
    })
}

fn apply_ordinary_only_cancellation<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    cancellation: CancellationRequest<Deadline>,
    operation: CancellationOperationId,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if cancellation.reason == CancellationReason::ForceAbort
        || stale_operation(&reduction.state, operation)
    {
        return false;
    }
    if !matches!(&reduction.state.workflow, WorkflowState::Executing { .. }) {
        reduction.state.last_cancellation_operation = Some(operation);
        return matches!(&reduction.state.workflow, WorkflowState::Finalizing { .. });
    }
    apply_ordinary_cancellation(reduction, cancellation, Some(operation), false)
}

fn stale_operation<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    operation: CancellationOperationId,
) -> bool {
    state
        .last_cancellation_operation
        .is_some_and(|last| operation <= last)
}

// Ordinary and finalization cancellation deliberately retain separate gates and events;
// their matching dispatch shapes make the phase boundary explicit rather than generic.
fn apply_cancellation<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    cancellation: CancellationRequest<Deadline>,
    operation: Option<CancellationOperationId>,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    match &reduction.state.workflow {
        WorkflowState::Executing { .. } => {
            apply_ordinary_cancellation(reduction, cancellation, operation, false)
        }
        WorkflowState::Finalizing {
            gate: FinalizationGate::Open,
            ..
        } => apply_finalization_cancellation(reduction, cancellation, operation),
        WorkflowState::Finalizing {
            gate: FinalizationGate::Cancelling { .. },
            ..
        }
        | WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. }
        | WorkflowState::Waiting { .. }
        | WorkflowState::Continued { .. } => false,
    }
}

// An interruption accepted during ordinary execution stops exactly as a cancellation does
// and differs only in how its stopped steps settle; in any other phase, or once a stop was
// accepted, it is that cancellation (REQ-WFEXEC-69, REQ-WFFINALIZE-15).
fn apply_interruption<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    cancellation: CancellationRequest<Deadline>,
    operation: Option<CancellationOperationId>,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    match &reduction.state.workflow {
        WorkflowState::Executing { .. } => {
            apply_ordinary_cancellation(reduction, cancellation, operation, true)
        }
        _ => apply_cancellation(reduction, cancellation, operation),
    }
}

fn apply_ordinary_cancellation<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    cancellation: CancellationRequest<Deadline>,
    operation: Option<CancellationOperationId>,
    interruption: bool,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let prior_issue = match &reduction.state.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => None,
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { primary_issue },
        } => Some(primary_issue.clone()),
        WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        } => return false,
        _ => return false,
    };
    if let Some(operation) = operation {
        reduction.state.last_cancellation_operation = Some(operation);
    }
    reduction.state.workflow = WorkflowState::Executing {
        gate: SchedulingGate::Cancelling {
            reason: cancellation.reason,
            prior_issue,
        },
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction
        .events
        .push(TransitionEvent::CancellationAccepted {
            sequence,
            reason: cancellation.reason,
            deadline: cancellation.deadline.clone(),
        });
    // While a `workflow` step's child settles under an interruption, its edge may still end
    // the walk and mark the unstarted steps `not_reached`, so they stay pending until the
    // step settles (REQ-WFEXEC-70).
    let mut keep_pending = false;
    if interruption {
        reduction.state.interrupted = true;
        settle_idle_interruption(reduction);
        reduction.state.unstarted_children = active_workflow_steps(&reduction.state)
            .filter(|(_, kind)| *kind == StepStateKind::Starting)
            .map(|(step, _)| step.clone())
            .collect();
        keep_pending = active_workflow_steps(&reduction.state).next().is_some();
    }
    cancel_nodes(
        reduction,
        WorkflowNodeRole::Step,
        cancellation.reason,
        Some(cancellation.deadline),
        false,
        operation,
        keep_pending,
    );
    true
}

/// The ordinary `workflow` steps, those with edges, that are starting or running.
fn active_workflow_steps<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> impl Iterator<Item = (&String, StepStateKind)> {
    state.definition.ordinary_ids.iter().filter_map(|id| {
        state.definition.steps.get(id)?.edges.as_ref()?;
        let kind = state.steps.get(id)?.state.kind();
        matches!(kind, StepStateKind::Starting | StepStateKind::Running).then_some((id, kind))
    })
}

fn apply_finalization_cancellation<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    cancellation: CancellationRequest<Deadline>,
    operation: Option<CancellationOperationId>,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if let Some(operation) = operation {
        reduction.state.last_cancellation_operation = Some(operation);
    }
    let from = reduction.state.workflow.clone();
    let WorkflowState::Finalizing {
        trigger,
        primary_issue,
        ..
    } = &from
    else {
        return false;
    };
    let to = WorkflowState::Finalizing {
        trigger: *trigger,
        gate: FinalizationGate::Cancelling {
            reason: cancellation.reason,
            deadline: Some(cancellation.deadline.clone()),
            force_abort: false,
        },
        primary_issue: primary_issue.clone(),
    };
    reduction.state.workflow = to;
    if let Some(finalization) = reduction.state.finalization.as_mut() {
        finalization.cancellation = Some(FinalizationCancellation {
            reason: cancellation.reason,
            deadline: Some(cancellation.deadline.clone()),
        });
    }
    let sequence = next_sequence(&mut reduction.state);
    reduction
        .events
        .push(TransitionEvent::FinalizationCancellationAccepted {
            sequence,
            reason: cancellation.reason,
            deadline: cancellation.deadline.clone(),
        });
    cancel_nodes(
        reduction,
        WorkflowNodeRole::Finalizer,
        cancellation.reason,
        Some(cancellation.deadline),
        false,
        operation,
        false,
    );
    true
}

fn apply_force_abort<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    operation: CancellationOperationId,
    deadline: Deadline,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if reduction.state.force_abort.is_some() {
        return false;
    }
    let phase = match reduction.state.workflow {
        WorkflowState::Executing { .. } => RunCancellationPhase::Ordinary,
        WorkflowState::Finalizing { .. } => RunCancellationPhase::Finalization,
        WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. }
        | WorkflowState::Waiting { .. }
        | WorkflowState::Continued { .. } => return false,
    };
    reduction.state.force_abort = Some(ForceAbortEvidence {
        reason: CancellationReason::ForceAbort,
        phase,
    });
    reduction.state.last_cancellation_operation = Some(operation);

    match reduction.state.workflow.clone() {
        WorkflowState::Executing { gate } => {
            let (reason, prior_issue) = match gate {
                SchedulingGate::FailureStopped { primary_issue } => {
                    (CancellationReason::ForceAbort, Some(primary_issue))
                }
                SchedulingGate::Cancelling {
                    reason,
                    prior_issue,
                } => (reason, prior_issue),
                SchedulingGate::Open => (CancellationReason::ForceAbort, None),
            };
            reduction.state.workflow = WorkflowState::Executing {
                gate: SchedulingGate::Cancelling {
                    reason,
                    prior_issue,
                },
            };
        }
        WorkflowState::Finalizing {
            trigger,
            gate,
            primary_issue,
        } => {
            let (reason, phase_deadline) = match gate {
                FinalizationGate::Open => (CancellationReason::ForceAbort, None),
                FinalizationGate::Cancelling {
                    reason, deadline, ..
                } => (reason, deadline),
            };
            reduction.state.workflow = WorkflowState::Finalizing {
                trigger,
                gate: FinalizationGate::Cancelling {
                    reason,
                    deadline: phase_deadline,
                    force_abort: true,
                },
                primary_issue,
            };
            if let Some(finalization) = reduction.state.finalization.as_mut() {
                finalization.force_abort = true;
                if finalization.cancellation.is_none() {
                    finalization.cancellation = Some(FinalizationCancellation {
                        reason: CancellationReason::ForceAbort,
                        deadline: None,
                    });
                }
            }
        }
        WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. }
        | WorkflowState::Waiting { .. }
        | WorkflowState::Continued { .. } => return false,
    }

    let sequence = next_sequence(&mut reduction.state);
    reduction.events.push(TransitionEvent::ForceAbortAccepted {
        sequence,
        reason: CancellationReason::ForceAbort,
        phase,
    });
    let role = match phase {
        RunCancellationPhase::Ordinary => WorkflowNodeRole::Step,
        RunCancellationPhase::Finalization => WorkflowNodeRole::Finalizer,
    };
    cancel_nodes(
        reduction,
        role,
        CancellationReason::ForceAbort,
        Some(deadline),
        true,
        Some(operation),
        false,
    );
    true
}

fn cancel_pending_nodes<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    role: WorkflowNodeRole,
    reason: CancellationReason,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let pending = reduction
        .state
        .definition
        .steps
        .iter()
        .filter(|(_, definition)| definition.role == role)
        .filter(|(id, _)| {
            reduction
                .state
                .steps
                .get(*id)
                .is_some_and(|runtime| matches!(runtime.state, StepState::Pending))
        })
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    for step in pending {
        transition_step(
            reduction,
            &step,
            StepState::Cancelled {
                detail: CancellationDetail::new(reason),
            },
            None,
        );
    }
}

fn cancel_nodes<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    role: WorkflowNodeRole,
    reason: CancellationReason,
    deadline: Option<Deadline>,
    force: bool,
    operation: Option<CancellationOperationId>,
    keep_pending: bool,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let force_context = match (force, operation, deadline.clone()) {
        (true, Some(operation), Some(deadline)) => Some((operation, deadline)),
        (true, _, _) => return,
        (false, _, _) => None,
    };
    let graceful_deadline = if force { None } else { deadline };
    let nodes = reduction
        .state
        .definition
        .steps
        .iter()
        .filter(|(_, definition)| definition.role == role)
        .filter_map(|(id, _)| {
            reduction
                .state
                .steps
                .get(id)
                .map(|runtime| (id.clone(), runtime.state.kind()))
        })
        .collect::<Vec<_>>();
    let mut containment_index = 1;
    for (step, state) in nodes {
        match state {
            StepStateKind::Pending if keep_pending => {}
            StepStateKind::Pending => {
                transition_step(
                    reduction,
                    &step,
                    StepState::Cancelled {
                        detail: CancellationDetail::new(reason),
                    },
                    None,
                );
            }
            StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Recovering => {
                let Some(active) = reduction
                    .state
                    .steps
                    .get(&step)
                    .and_then(|runtime| runtime.active_invocation)
                else {
                    continue;
                };
                mark_handler_cancelled(&mut reduction.state, &step, active);
                let sequence = transition_step(
                    reduction,
                    &step,
                    StepState::Cancelling {
                        detail: CancellationDetail::new(reason),
                    },
                    None,
                );
                let action = force_context.as_ref().map_or_else(
                    || ActionId::for_transition(sequence),
                    |(operation, _)| ActionId::for_force_abort(*operation, containment_index),
                );
                containment_index += 1;
                set_current_action(&mut reduction.state, &step, action);
                let requested = match &force_context {
                    Some((_, deadline)) => Action::ForceAbortStep {
                        step,
                        active,
                        reason,
                        deadline: deadline.clone(),
                    },
                    None => {
                        let Some(deadline) = graceful_deadline.clone() else {
                            continue;
                        };
                        Action::CancelStep {
                            step,
                            active,
                            reason,
                            deadline,
                        }
                    }
                };
                reduction.actions.push(RequestedAction {
                    id: action,
                    action: requested,
                });
            }
            StepStateKind::Cancelling if force => {
                let Some((operation, deadline)) = &force_context else {
                    continue;
                };
                let action = ActionId::for_force_abort(*operation, containment_index);
                containment_index += 1;
                set_current_action(&mut reduction.state, &step, action);
                let Some(active) = reduction
                    .state
                    .steps
                    .get(&step)
                    .and_then(|runtime| runtime.active_invocation)
                else {
                    continue;
                };
                reduction.actions.push(RequestedAction {
                    id: action,
                    action: Action::ForceAbortStep {
                        step,
                        active,
                        reason,
                        deadline: deadline.clone(),
                    },
                });
            }
            StepStateKind::Cancelling
            | StepStateKind::Succeeded
            | StepStateKind::Inherited
            | StepStateKind::Failed
            | StepStateKind::Blocked
            | StepStateKind::Skipped
            | StepStateKind::NotRun
            | StepStateKind::Cancelled => {}
        }
    }
}

fn step_role<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
) -> Option<WorkflowNodeRole> {
    state
        .definition
        .steps
        .get(step)
        .map(|definition| definition.role)
}

// An interruption that finds no ordinary step active ends a continuation whose entry is still
// pending on the wait it continues from, kept, with every unstarted step `not_reached`
// (REQ-WFEXEC-72). Otherwise it fails the first pending step in document order at `start`, so
// the attempt still names the step it stopped at; every other pending step is then cancelled
// (REQ-WFEXEC-69).
fn settle_idle_interruption<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let state = &reduction.state;
    let ordinary = |id: &&String| state.definition.ordinary_ids.contains(*id);
    if state
        .steps
        .iter()
        .any(|(id, runtime)| ordinary(&id) && runtime.state.is_active())
    {
        return;
    }
    let entry_pending = entry_step(state)
        .and_then(|entry| state.steps.get(entry))
        .is_some_and(|runtime| matches!(runtime.state, StepState::Pending));
    if entry_pending && let Some(kept) = kept_resumed_wait(state) {
        reduction.state.wait = Some(kept);
        mark_unstarted_not_reached(reduction);
        return;
    }
    let Some(step) = state
        .definition
        .step_order
        .iter()
        .filter(ordinary)
        .find(|id| {
            state
                .steps
                .get(*id)
                .is_some_and(|runtime| matches!(runtime.state, StepState::Pending))
        })
        .cloned()
    else {
        return;
    };
    settle_interrupted(reduction, &step, FailureDetail::interrupted_before_start());
}

// An interrupted ordinary step fails with no outputs and no recovery round; the first one
// becomes the run's primary issue unless an issue was already recorded, whatever its
// failure policy (REQ-WFEXEC-69, REQ-WFEXEC-17).
fn settle_interrupted<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: &str,
    detail: FailureDetail,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let Some(role) = step_role(&reduction.state, step) else {
        return;
    };
    let primary_issue = PrimaryIssue::failed(
        WorkflowNode {
            id: step.to_owned(),
            role,
        },
        detail.clone(),
    );
    transition_step(reduction, step, StepState::Failed { detail }, None);
    clear_active_invocation(&mut reduction.state, step);
    record_stopped_issue(reduction, primary_issue);
}

// The first step that fails while the interruption stops the run becomes its primary issue
// unless one was already recorded, and the steps a settling `workflow` step kept pending are
// cancelled (REQ-WFEXEC-69, REQ-WFEXEC-70).
fn record_stopped_issue<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    primary_issue: PrimaryIssue,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let WorkflowState::Executing {
        gate: SchedulingGate::Cancelling {
            reason,
            prior_issue,
        },
    } = &reduction.state.workflow
    else {
        return;
    };
    let reason = *reason;
    if prior_issue.is_none() {
        let from = reduction.state.workflow.clone();
        let to = WorkflowState::Executing {
            gate: SchedulingGate::Cancelling {
                reason,
                prior_issue: Some(primary_issue),
            },
        };
        let sequence = next_sequence(&mut reduction.state);
        reduction.state.workflow = to.clone();
        reduction.events.push(TransitionEvent::Workflow {
            sequence,
            from,
            to: Box::new(to),
        });
    }
    cancel_pending_nodes(reduction, WorkflowNodeRole::Step, reason);
}

/// How the reducer takes a `workflow` step's completion that arrives under an
/// interruption (REQ-WFEXEC-70, INV-ENGINE-29).
enum InterruptedCompletion {
    /// Not such a completion: it is accepted only as any completion is.
    Ordinary,
    /// The step commits the child's outcome and the walk ends on its edge.
    Route(Option<ChildInterruption>),
    /// The edge is not taken and the step fails `interrupted`.
    Fail,
}

// While an interruption stops a `workflow` step, the completion its child reported is routed
// before any output is committed: an interrupted child only to a `repair` wait, a child that
// exited on its own on any edge, which `end_edge_walk` then ends the attempt on. The
// completion may carry the step's start action, when it was claimed before the stop reached
// the driver, or arrive after it.
fn interrupted_completion<Provisional, Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
    action: ActionId,
    provisional: &Provisional,
) -> InterruptedCompletion
where
    Provisional: ProvisionalStepResources,
{
    let Some(runtime) = state.steps.get(step) else {
        return InterruptedCompletion::Ordinary;
    };
    let Some(edges) = state
        .definition
        .steps
        .get(step)
        .and_then(|definition| definition.edges.as_ref())
    else {
        return InterruptedCompletion::Ordinary;
    };
    if !state.interrupted
        || runtime.state.kind() != StepStateKind::Cancelling
        || (runtime.current_action != Some(action) && runtime.target_invocation != Some(action))
    {
        return InterruptedCompletion::Ordinary;
    }
    let Some(ending) = provisional.child_ending() else {
        return InterruptedCompletion::Ordinary;
    };
    let prior_issue = matches!(
        &state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Cancelling {
                prior_issue: Some(_),
                ..
            }
        }
    );
    match (edges.get(&ending.outcome), ending.interruption) {
        (None, _) => InterruptedCompletion::Fail,
        _ if prior_issue => InterruptedCompletion::Fail,
        (
            Some(RuntimeEdgeTarget::Wait {
                kind: WaitKind::Repair,
                ..
            }),
            Some(interruption),
        ) => InterruptedCompletion::Route(Some(interruption)),
        (Some(_), Some(_)) => InterruptedCompletion::Fail,
        (Some(_), None) => InterruptedCompletion::Route(None),
    }
}

fn cancelling_step<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
    action: ActionId,
) -> Option<(CancellationReason, ActiveStepInvocation)> {
    let runtime = state.steps.get(step)?;
    match &runtime.state {
        StepState::Cancelling { detail } if runtime.current_action == Some(action) => {
            Some((detail.code, runtime.active_invocation?))
        }
        _ => None,
    }
}

fn apply_step_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    action: ActionId,
    expected_state: StepStateKind,
    phase: FailurePhase,
    cause: Cause,
) -> bool
where
    Cause: Clone + NodeFailureSource,
    Output: Clone,
    Deadline: Clone,
{
    if !step_accepts(&reduction.state, &step, expected_state, action) {
        return false;
    }
    let Some(definition) = reduction.state.definition.steps.get(&step).cloned() else {
        return false;
    };
    let Ok(detail) = cause.node_failure_detail(phase) else {
        return false;
    };
    if definition.role == WorkflowNodeRole::Step
        && activate_recovery_round(reduction, &step, phase, detail.clone(), cause.clone())
    {
        return true;
    }
    if definition.recovery.is_some() {
        mark_recovery_exhausted(&mut reduction.state, &step);
    }
    if let Some(runtime) = reduction.state.steps.get_mut(&step) {
        runtime.failure_kind = Some(cause.failure_kind());
    }
    settle_target_failure(reduction, step, definition, detail);
    true
}

fn activate_recovery_round<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: &str,
    phase: FailurePhase,
    detail: FailureDetail,
    cause: Cause,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let Some(runtime) = reduction.state.steps.get(step) else {
        return false;
    };
    let Some(execution_number) = runtime.target_execution else {
        return false;
    };
    let Some(invocation) = runtime.target_invocation else {
        return false;
    };
    let Some(recovery) = runtime.recovery.as_ref() else {
        return false;
    };
    if recovery.rounds.len() >= usize::from(recovery.configured_rounds) {
        return false;
    }
    let Some(round_value) = u8::try_from(recovery.rounds.len())
        .ok()
        .and_then(|round| round.checked_add(1))
    else {
        return false;
    };
    let round = RecoveryRoundNumber(round_value);
    if round.get() != execution_number.get() {
        return false;
    }
    let handler_kind = recovery.handler_kind;
    let failed_execution = ProvisionalTargetFailure {
        execution_number,
        invocation,
        phase,
        detail,
        cause,
    };
    let Some(recovery) = reduction
        .state
        .steps
        .get_mut(step)
        .and_then(|runtime| runtime.recovery.as_mut())
    else {
        return false;
    };
    recovery.rounds.push(RecoveryRoundRecord {
        number: round,
        failed_execution,
        handler: None,
    });

    match handler_kind {
        Some(kind) => {
            if let Some(runtime) = reduction.state.steps.get_mut(step) {
                runtime.active_invocation = Some(ActiveStepInvocation::RecoveryHandler { round });
            }
            let sequence = transition_step(
                reduction,
                step,
                StepState::Recovering {
                    round,
                    handler: RecoveryHandlerActivity::Starting,
                },
                None,
            );
            let action = ActionId::for_transition(sequence);
            let Some(round_record) = reduction
                .state
                .steps
                .get_mut(step)
                .and_then(|runtime| runtime.recovery.as_mut())
                .and_then(|recovery| recovery.rounds.last_mut())
            else {
                return false;
            };
            round_record.handler = Some(RecoveryHandlerRecord {
                kind,
                invocation: action,
                outcome: RecoveryHandlerOutcome::Starting,
            });
            set_current_action(&mut reduction.state, step, action);
            let history = reduction
                .state
                .steps
                .get(step)
                .and_then(|runtime| runtime.recovery.as_ref())
                .map(|recovery| recovery.rounds.clone())
                .unwrap_or_default();
            reduction.actions.push(RequestedAction {
                id: action,
                action: Action::StartRecoveryHandler {
                    step: step.to_owned(),
                    round,
                    kind,
                    history,
                },
            });
        }
        None => {
            let Some(next_execution) = execution_number.next() else {
                return false;
            };
            let Some(definition) = reduction.state.definition.steps.get(step).cloned() else {
                return false;
            };
            let inputs = resolved_action_inputs(&reduction.state, &definition);
            authorize_target(reduction, step.to_owned(), next_execution, inputs);
        }
    }
    true
}

fn apply_handler_decision<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    round: RecoveryRoundNumber,
    action: ActionId,
    decision: RecoveryDecision,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if !handler_accepts(
        &reduction.state,
        &step,
        round,
        RecoveryHandlerActivity::Running,
        action,
    ) {
        return false;
    }
    let outcome = match decision.kind {
        RecoveryDecisionKind::Recheck => RecoveryHandlerOutcome::Recheck {
            summary: decision.summary,
            reason: decision.reason,
        },
        RecoveryDecisionKind::GaveUp => RecoveryHandlerOutcome::GaveUp {
            summary: decision.summary,
            reason: decision.reason,
        },
    };
    set_handler_outcome(&mut reduction.state, &step, round, outcome);
    match decision.kind {
        RecoveryDecisionKind::Recheck => {
            let Some(execution_number) = latest_target_failure(&reduction.state, &step)
                .and_then(|failure| failure.execution_number.next())
            else {
                return false;
            };
            let Some(definition) = reduction.state.definition.steps.get(&step).cloned() else {
                return false;
            };
            let inputs = resolved_action_inputs(&reduction.state, &definition);
            authorize_target(reduction, step, execution_number, inputs);
        }
        RecoveryDecisionKind::GaveUp => {
            set_recovery_disposition(
                &mut reduction.state,
                &step,
                RecoveryTerminalDisposition::GaveUp { round },
            );
            settle_latest_target_failure(reduction, step);
        }
    }
    true
}

fn apply_handler_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    round: RecoveryRoundNumber,
    action: ActionId,
    expected: RecoveryHandlerActivity,
    phase: RecoveryHandlerFailurePhase,
    cause: Cause,
) -> bool
where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if !handler_accepts(&reduction.state, &step, round, expected, action) {
        return false;
    }
    set_handler_outcome(
        &mut reduction.state,
        &step,
        round,
        RecoveryHandlerOutcome::Failed { phase, cause },
    );
    set_recovery_disposition(
        &mut reduction.state,
        &step,
        RecoveryTerminalDisposition::HandlerFailed { round, phase },
    );
    settle_latest_target_failure(reduction, step);
    true
}

fn settle_latest_target_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let Some(failure) = latest_target_failure(&reduction.state, &step).cloned() else {
        return;
    };
    let Some(definition) = reduction.state.definition.steps.get(&step).cloned() else {
        return;
    };
    settle_target_failure(reduction, step, definition, failure.detail);
}

fn settle_target_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    definition: RuntimeStep,
    detail: FailureDetail,
) where
    Cause: Clone,
    Deadline: Clone,
{
    let primary_issue = PrimaryIssue::failed(
        WorkflowNode {
            id: step.clone(),
            role: definition.role,
        },
        detail.clone(),
    );
    transition_step(reduction, &step, StepState::Failed { detail }, None);
    clear_active_invocation(&mut reduction.state, &step);
    commit_required_issue(reduction, &definition, primary_issue);
}

fn commit_required_issue<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    definition: &RuntimeStep,
    primary_issue: PrimaryIssue,
) where
    Cause: Clone,
    Deadline: Clone,
{
    if definition.failure_policy == FailurePolicy::Required {
        match definition.role {
            WorkflowNodeRole::Step => close_ordinary_gate_for_failure(reduction, primary_issue),
            WorkflowNodeRole::Finalizer => select_finalizer_primary_issue(reduction, primary_issue),
        }
    }
}

fn latest_target_failure<'a, Cause, Output, Deadline>(
    state: &'a RuntimeState<Cause, Output, Deadline>,
    step: &str,
) -> Option<&'a ProvisionalTargetFailure<Cause>> {
    state
        .steps
        .get(step)?
        .recovery
        .as_ref()?
        .rounds
        .last()
        .map(|round| &round.failed_execution)
}

fn handler_accepts<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
    round: RecoveryRoundNumber,
    expected: RecoveryHandlerActivity,
    action: ActionId,
) -> bool {
    state.steps.get(step).is_some_and(|runtime| {
        runtime.current_action == Some(action)
            && runtime.active_invocation == Some(ActiveStepInvocation::RecoveryHandler { round })
            && matches!(
                &runtime.state,
                StepState::Recovering {
                    round: active_round,
                    handler,
                } if *active_round == round && *handler == expected
            )
    })
}

fn set_handler_outcome<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    round: RecoveryRoundNumber,
    outcome: RecoveryHandlerOutcome<Cause>,
) {
    if let Some(handler) = state
        .steps
        .get_mut(step)
        .and_then(|runtime| runtime.recovery.as_mut())
        .and_then(|recovery| recovery.rounds.last_mut())
        .filter(|record| record.number == round)
        .and_then(|record| record.handler.as_mut())
    {
        handler.outcome = outcome;
    }
}

fn set_recovery_disposition<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    disposition: RecoveryTerminalDisposition,
) {
    if let Some(recovery) = state
        .steps
        .get_mut(step)
        .and_then(|runtime| runtime.recovery.as_mut())
    {
        recovery.terminal_disposition = Some(disposition);
    }
}

fn mark_recovery_exhausted<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
) {
    if let Some(execution_number) = activated_recovery_execution(state, step) {
        set_recovery_disposition(
            state,
            step,
            RecoveryTerminalDisposition::Exhausted { execution_number },
        );
    }
}

fn mark_recovered<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: &str,
) {
    if let Some(execution_number) = activated_recovery_execution(&reduction.state, step) {
        set_recovery_disposition(
            &mut reduction.state,
            step,
            RecoveryTerminalDisposition::Recovered { execution_number },
        );
    }
}

fn activated_recovery_execution<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
) -> Option<TargetExecutionNumber> {
    let runtime = state.steps.get(step)?;
    runtime
        .recovery
        .as_ref()
        .is_some_and(|recovery| !recovery.rounds.is_empty())
        .then_some(runtime.target_execution)
        .flatten()
}

fn mark_handler_cancelled<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    active: ActiveStepInvocation,
) {
    if let ActiveStepInvocation::RecoveryHandler { round } = active {
        set_handler_outcome(state, step, round, RecoveryHandlerOutcome::Cancelled);
    }
}

fn mark_recovery_cancelled<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    active: ActiveStepInvocation,
) {
    let Some(round) = state
        .steps
        .get(step)
        .and_then(|runtime| runtime.recovery.as_ref())
        .and_then(|recovery| recovery.rounds.last())
        .map(|round| round.number)
    else {
        return;
    };
    set_recovery_disposition(
        state,
        step,
        RecoveryTerminalDisposition::Cancelled { round, active },
    );
}

fn clear_active_invocation<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
) {
    if let Some(runtime) = state.steps.get_mut(step) {
        runtime.active_invocation = None;
    }
}

// Primary issue selection is phase-specific even though both transitions carry the same
// surrounding workflow fields; keeping them separate makes ordinary precedence explicit.
fn close_ordinary_gate_for_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    primary_issue: PrimaryIssue,
) where
    Cause: Clone,
    Deadline: Clone,
{
    if !matches!(
        &reduction.state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::Open
        }
    ) {
        return;
    }
    let from = reduction.state.workflow.clone();
    let to = WorkflowState::Executing {
        gate: SchedulingGate::FailureStopped { primary_issue },
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction.events.push(TransitionEvent::Workflow {
        sequence,
        from,
        to: Box::new(to),
    });
}

fn select_finalizer_primary_issue<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    primary_issue: PrimaryIssue,
) where
    Cause: Clone,
    Deadline: Clone,
{
    let WorkflowState::Finalizing {
        trigger,
        gate: FinalizationGate::Open,
        primary_issue: None,
    } = &reduction.state.workflow
    else {
        return;
    };
    if *trigger != FinalizationTrigger::Succeeded {
        return;
    }
    let from = reduction.state.workflow.clone();
    let to = WorkflowState::Finalizing {
        trigger: *trigger,
        gate: FinalizationGate::Open,
        primary_issue: Some(primary_issue),
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction.events.push(TransitionEvent::Workflow {
        sequence,
        from,
        to: Box::new(to),
    });
}

fn stabilize<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    loop {
        let before = reduction.state.last_transition_sequence;
        match &reduction.state.workflow {
            WorkflowState::Executing { .. } => {
                propagate_ordinary_pending_dispositions(reduction);
                evaluate_ready_conditions(reduction, WorkflowNodeRole::Step);
                propagate_ordinary_pending_dispositions(reduction);
                end_edge_walk(reduction);
                select_ready_nodes(reduction, WorkflowNodeRole::Step);
                enter_finalization_or_finish(reduction);
            }
            WorkflowState::Finalizing { .. } => {
                let suppress_selected = reduction
                    .state
                    .force_abort
                    .is_some_and(|force_abort| force_abort.phase == RunCancellationPhase::Ordinary);
                if suppress_selected {
                    propagate_finalizer_trigger_dispositions(reduction);
                    cancel_pending_nodes(
                        reduction,
                        WorkflowNodeRole::Finalizer,
                        CancellationReason::ForceAbort,
                    );
                } else {
                    propagate_finalizer_dispositions(reduction);
                }
                evaluate_ready_conditions(reduction, WorkflowNodeRole::Finalizer);
                if !suppress_selected {
                    propagate_finalizer_dispositions(reduction);
                }
                select_ready_nodes(reduction, WorkflowNodeRole::Finalizer);
                finish_finalization_if_terminal(reduction);
            }
            WorkflowState::Succeeded
            | WorkflowState::Failed { .. }
            | WorkflowState::Cancelled { .. }
            | WorkflowState::Waiting { .. }
            | WorkflowState::Continued { .. } => {}
        }
        if reduction.state.last_transition_sequence == before {
            break;
        }
    }
}

fn evaluate_ready_conditions<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    role: WorkflowNodeRole,
) where
    Cause: Clone,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    loop {
        let candidate = condition_ids(&reduction.state, role)
            .find(|id| condition_gate_ready(&reduction.state, id))
            .cloned();
        let Some(id) = candidate else { break };
        let Some(definition) = reduction.state.definition.steps.get(&id).cloned() else {
            break;
        };
        let Some(predicate) = definition.condition.as_ref() else {
            break;
        };
        match evaluate_condition(&reduction.state, &definition, predicate) {
            ConditionEvaluation::Passed => {
                if let Some(runtime) = reduction.state.steps.get_mut(&id) {
                    runtime.condition_passed = true;
                }
            }
            ConditionEvaluation::False {
                evaluated_predicates,
            } => {
                let entries = evaluated_predicates
                    .iter()
                    .map(|entry| EvaluatedPredicateEvidence {
                        path: entry.path.as_str().to_owned(),
                        result: entry.result,
                    });
                let Ok(detail) = ConditionFalseDetail::new(entries) else {
                    break;
                };
                transition_step(reduction, &id, StepState::Skipped { detail }, None);
            }
            ConditionEvaluation::Failed {
                canonical_ref,
                pointer,
            } => {
                let Ok(detail) =
                    FailureDetail::json_pointer_missing(canonical_ref.as_ref(), pointer.as_ref())
                else {
                    break;
                };
                settle_condition_failure(reduction, id, definition, detail);
            }
            ConditionEvaluation::Unavailable { .. } => break,
        }
    }
}

fn condition_ids<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    role: WorkflowNodeRole,
) -> impl Iterator<Item = &String> {
    match role {
        WorkflowNodeRole::Step => state.definition.ordinary_ids.iter(),
        WorkflowNodeRole::Finalizer => state.definition.finalizer_ids.iter(),
    }
}

fn condition_gate_ready<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    id: &str,
) -> bool {
    let Some(definition) = state.definition.steps.get(id) else {
        return false;
    };
    let Some(runtime) = state.steps.get(id) else {
        return false;
    };
    if condition_cleared(definition, runtime)
        || !matches!(runtime.state, StepState::Pending)
        || !condition_phase_gate_open(state, definition)
        || !condition_blockers(state, definition).is_empty()
    {
        return false;
    }
    let prerequisites_ready = definition.prerequisites.iter().all(|prerequisite| {
        let Some(producer) = state.steps.get(&prerequisite.producer) else {
            return false;
        };
        let terminal = producer.state.is_terminal();
        let succeeded = step_data_available(&producer.state);
        match definition.role {
            WorkflowNodeRole::Step => {
                (!prerequisite.control || ordinary_control_satisfied(state, prerequisite))
                    && (!prerequisite.disposition_control || terminal)
                    && (!prerequisite.condition_data || succeeded)
            }
            WorkflowNodeRole::Finalizer => {
                (!(prerequisite.control || prerequisite.disposition_control) || terminal)
                    && (!prerequisite.condition_data || succeeded)
            }
        }
    });
    prerequisites_ready && condition_sources_available(state, definition)
}

fn condition_phase_gate_open<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    definition: &RuntimeStep,
) -> bool {
    match (&state.workflow, definition.role) {
        (
            WorkflowState::Executing {
                gate: SchedulingGate::Open,
            },
            WorkflowNodeRole::Step,
        ) => true,
        (
            WorkflowState::Finalizing {
                trigger,
                gate: FinalizationGate::Open,
                ..
            },
            WorkflowNodeRole::Finalizer,
        ) => definition.when.contains(trigger),
        _ => false,
    }
}

fn condition_sources_available<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    definition: &RuntimeStep,
) -> bool {
    definition
        .condition_values
        .values()
        .all(|source| match source {
            ResolvedValueSource::Input(name) => {
                state.definition.text_inputs.contains_key(name)
                    || state.definition.json_inputs.contains_key(name)
            }
            ResolvedValueSource::FinalizationContext => state.finalization.is_some(),
            // A condition never reads a control value (REQ-WFFORMAT-95).
            ResolvedValueSource::Wait(_)
            | ResolvedValueSource::ControlsAnswers
            | ResolvedValueSource::Item { .. } => false,
            ResolvedValueSource::Output(source) => state
                .steps
                .get(&source.node.id)
                .is_some_and(|runtime| step_data_available(&runtime.state)),
        })
}

fn evaluate_condition<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    definition: &RuntimeStep,
    predicate: &ResolvedPredicate,
) -> ConditionEvaluation
where
    Output: ConditionOutput,
{
    let context = state.finalization.as_ref().and_then(|finalization| {
        serde_json::from_slice::<serde_json::Value>(&finalization.context).ok()
    });
    let mut values = ConditionValues::default();
    for (canonical, source) in &definition.condition_values {
        match source {
            ResolvedValueSource::Input(name) => {
                if let Some(text) = state.definition.text_inputs.get(name) {
                    values.insert_text_value(canonical.as_str(), text);
                } else if let Some(json) = state.definition.json_inputs.get(name) {
                    values.insert_json_value(canonical.as_str(), json);
                }
            }
            ResolvedValueSource::FinalizationContext => {
                if let Some(context) = context.as_ref() {
                    values.insert_json_value(canonical.as_str(), context);
                }
            }
            ResolvedValueSource::Wait(_)
            | ResolvedValueSource::ControlsAnswers
            | ResolvedValueSource::Item { .. } => {}
            ResolvedValueSource::Output(source) => {
                let Some(output) = state
                    .steps
                    .get(&source.node.id)
                    .and_then(|runtime| step_outputs(&runtime.state))
                    .and_then(|outputs| outputs.get(&source.output))
                else {
                    continue;
                };
                if let Some(text) = output.condition_text() {
                    values.insert_text_value(canonical.as_str(), text);
                } else if let Some(json) = output.condition_json() {
                    values.insert_json_value(canonical.as_str(), json);
                }
            }
        }
    }
    let mut dispositions = ConditionDispositions::default();
    for (id, runtime) in &state.steps {
        if let Some(disposition) = terminal_disposition(&runtime.state) {
            let role = state
                .definition
                .steps
                .get(id)
                .map_or(WorkflowNodeRole::Step, |definition| definition.role);
            dispositions.insert(
                WorkflowNode {
                    id: id.clone(),
                    role,
                },
                disposition,
            );
        }
    }
    condition::evaluate(predicate, &values, &dispositions)
}

fn step_data_available<Output>(state: &StepState<Output>) -> bool {
    matches!(state, StepState::Succeeded { .. })
        || matches!(
            state,
            StepState::Inherited {
                disposition: InheritedDisposition::Succeeded,
                ..
            }
        )
}

pub(crate) fn step_outputs<Output>(state: &StepState<Output>) -> Option<&OutputSet<Output>> {
    match state {
        StepState::Succeeded { outputs }
        | StepState::Inherited {
            disposition: InheritedDisposition::Succeeded,
            outputs,
            ..
        } => Some(outputs),
        StepState::Inherited {
            disposition: InheritedDisposition::Skipped,
            ..
        }
        | StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. }
        | StepState::Failed { .. }
        | StepState::Blocked { .. }
        | StepState::Skipped { .. }
        | StepState::NotRun { .. }
        | StepState::Cancelled { .. } => None,
    }
}

fn terminal_disposition<Output>(state: &StepState<Output>) -> Option<TerminalDisposition> {
    match state {
        StepState::Succeeded { .. } => Some(TerminalDisposition::Succeeded),
        StepState::Inherited { .. } => Some(TerminalDisposition::Inherited),
        StepState::Failed { .. } => Some(TerminalDisposition::Failed),
        StepState::Skipped { .. } => Some(TerminalDisposition::Skipped),
        StepState::Blocked { .. } => Some(TerminalDisposition::Blocked),
        StepState::NotRun { .. } => Some(TerminalDisposition::NotRun),
        StepState::Cancelled { .. } => Some(TerminalDisposition::Cancelled),
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => None,
    }
}

fn settle_blocked<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    id: String,
    detail: BlockedDetail,
) where
    Cause: Clone,
    Deadline: Clone,
{
    let Some(definition) = reduction.state.definition.steps.get(&id).cloned() else {
        return;
    };
    let primary_issue = PrimaryIssue::blocked(
        WorkflowNode {
            id: id.clone(),
            role: definition.role,
        },
        detail.clone(),
    );
    transition_step(reduction, &id, StepState::Blocked { detail }, None);
    commit_required_issue(reduction, &definition, primary_issue);
}

fn settle_condition_failure<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    id: String,
    definition: RuntimeStep,
    detail: FailureDetail,
) where
    Cause: Clone,
    Deadline: Clone,
{
    settle_target_failure(reduction, id, definition, detail);
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PendingDisposition {
    Blocked(BlockedDetail),
    NotRun,
}

fn propagate_ordinary_pending_dispositions<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    while let Some((step, disposition)) = next_ordinary_pending_disposition(&reduction.state) {
        match disposition {
            PendingDisposition::Blocked(detail) => settle_blocked(reduction, step, detail),
            PendingDisposition::NotRun => {
                transition_step(
                    reduction,
                    &step,
                    StepState::NotRun {
                        detail: NonExecutionDetail::failure_stop(),
                    },
                    None,
                );
            }
        }
    }
}

fn next_ordinary_pending_disposition<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<(String, PendingDisposition)> {
    let failure_stopped = matches!(
        &state.workflow,
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { .. }
        }
    );
    state.definition.ordinary_ids.iter().find_map(|step_id| {
        let definition = state.definition.steps.get(step_id)?;
        let runtime = state.steps.get(step_id)?;
        if !matches!(runtime.state, StepState::Pending) {
            return None;
        }
        let blockers =
            ordinary_unsatisfied_prerequisites(state, step_id, definition, failure_stopped);
        if !blockers.is_empty() {
            return BlockedDetail::new(blockers)
                .ok()
                .map(|detail| (step_id.clone(), PendingDisposition::Blocked(detail)));
        }
        (failure_stopped
            && definition
                .prerequisites
                .iter()
                .all(|prerequisite| ordinary_prerequisite_satisfied(state, prerequisite)))
        .then(|| (step_id.clone(), PendingDisposition::NotRun))
    })
}

fn condition_blockers<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    definition: &RuntimeStep,
) -> Vec<Prerequisite> {
    let mut blockers = Vec::new();
    for prerequisite in definition.prerequisites.iter() {
        let Some(producer) = state.steps.get(&prerequisite.producer) else {
            continue;
        };
        if !producer.state.is_terminal() {
            continue;
        }
        if definition.role == WorkflowNodeRole::Step
            && prerequisite.control
            && !ordinary_control_satisfied(state, prerequisite)
            && let Ok(blocker) = Prerequisite::control(prerequisite.producer.clone())
        {
            blockers.push(blocker);
        }
        if prerequisite.condition_data && !step_data_available(&producer.state) {
            blockers.extend(
                definition
                    .evidence_prerequisites
                    .iter()
                    .filter(|descriptor| {
                        matches!(descriptor, Prerequisite::Condition { r#ref }
                            if output_reference_producer(r#ref)
                                == Some(prerequisite.producer.as_str()))
                    })
                    .cloned(),
            );
        }
    }
    for source in definition.condition_values.values() {
        let ResolvedValueSource::Output(source) = source else {
            continue;
        };
        if state.steps.get(&source.node.id).is_some_and(|producer| {
            producer.state.is_terminal() && !step_data_available(&producer.state)
        }) && let Ok(blocker) = Prerequisite::condition(source.reference())
        {
            blockers.push(blocker);
        }
    }
    blockers.sort();
    blockers.dedup();
    blockers
}

// While scheduling is open, an unevaluated condition owns readiness: a false condition skips
// the step even when its body inputs are unavailable. After a failure stop no condition is
// evaluated again, so body and control prerequisites must also block or the step would remain
// pending forever.
fn ordinary_unsatisfied_prerequisites<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    id: &str,
    definition: &RuntimeStep,
    failure_stopped: bool,
) -> Vec<Prerequisite> {
    let condition_pending = state
        .steps
        .get(id)
        .is_some_and(|runtime| !condition_cleared(definition, runtime));
    let mut blockers = if condition_pending {
        condition_blockers(state, definition)
    } else {
        Vec::new()
    };
    if condition_pending && !failure_stopped {
        return blockers;
    }
    for prerequisite in definition.prerequisites.iter() {
        let Some(producer) = state.steps.get(&prerequisite.producer) else {
            continue;
        };
        if !producer.state.is_terminal() {
            continue;
        }
        let succeeded = step_data_available(&producer.state);
        let control_satisfied = ordinary_control_satisfied(state, prerequisite);
        if prerequisite.control
            && !control_satisfied
            && let Ok(blocker) = Prerequisite::control(prerequisite.producer.clone())
            && !blockers.contains(&blocker)
        {
            blockers.push(blocker);
        }
        if prerequisite.data && !succeeded {
            blockers.extend(
                definition
                    .evidence_prerequisites
                    .iter()
                    .filter(|descriptor| {
                        matches!(descriptor, Prerequisite::Body { r#ref }
                            if output_reference_producer(r#ref)
                                == Some(prerequisite.producer.as_str()))
                    })
                    .cloned(),
            );
        }
    }
    blockers
}

fn output_reference_producer(reference: &str) -> Option<&str> {
    let mut segments = reference.split('.');
    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (Some("outputs"), Some(producer), Some(_), None) => Some(producer),
        _ => None,
    }
}

fn ordinary_prerequisite_satisfied<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    prerequisite: &ResolvedDirectPrerequisite,
) -> bool {
    let Some(producer) = state.steps.get(&prerequisite.producer) else {
        return false;
    };
    let succeeded = step_data_available(&producer.state);
    (!prerequisite.control || ordinary_control_satisfied(state, prerequisite))
        && (!prerequisite.data || succeeded)
        && (!prerequisite.disposition_control || producer.state.is_terminal())
}

fn ordinary_control_satisfied<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    prerequisite: &ResolvedDirectPrerequisite,
) -> bool {
    state
        .steps
        .get(&prerequisite.producer)
        .is_some_and(|producer| {
            matches!(
                producer.state,
                StepState::Succeeded { .. }
                    | StepState::Skipped { .. }
                    | StepState::Inherited { .. }
            ) || (state
                .definition
                .steps
                .get(&prerequisite.producer)
                .is_some_and(|definition| definition.failure_policy == FailurePolicy::Advisory)
                && matches!(
                    producer.state,
                    StepState::Failed { .. } | StepState::Blocked { .. }
                ))
        })
}

fn propagate_finalizer_dispositions<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    propagate_finalizer_trigger_dispositions(reduction);
    while let Some((finalizer, detail)) = next_finalizer_condition_block(&reduction.state) {
        settle_blocked(reduction, finalizer, detail);
    }

    while let Some((finalizer, references)) = next_input_unavailable(&reduction.state) {
        let prerequisites = references
            .into_iter()
            .filter_map(|reference| Prerequisite::body(reference).ok());
        let Ok(detail) = BlockedDetail::new(prerequisites) else {
            continue;
        };
        settle_blocked(reduction, finalizer, detail);
    }
}

fn propagate_finalizer_trigger_dispositions<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let Some(trigger) = reduction
        .state
        .finalization
        .as_ref()
        .map(|finalization| finalization.trigger)
    else {
        return;
    };
    let not_selected = reduction
        .state
        .definition
        .finalizer_ids
        .iter()
        .filter(|id| {
            reduction
                .state
                .steps
                .get(*id)
                .is_some_and(|runtime| matches!(runtime.state, StepState::Pending))
                && reduction
                    .state
                    .definition
                    .steps
                    .get(*id)
                    .is_some_and(|definition| !definition.when.contains(&trigger))
        })
        .cloned()
        .collect::<Vec<_>>();
    for finalizer in not_selected {
        transition_step(
            reduction,
            &finalizer,
            StepState::NotRun {
                detail: NonExecutionDetail::finalizer_trigger_not_selected(),
            },
            None,
        );
    }
}

fn next_finalizer_condition_block<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<(String, BlockedDetail)> {
    state.definition.finalizer_ids.iter().find_map(|id| {
        let definition = state.definition.steps.get(id)?;
        let runtime = state.steps.get(id)?;
        if condition_cleared(definition, runtime)
            || !matches!(runtime.state, StepState::Pending)
            || !condition_phase_gate_open(state, definition)
        {
            return None;
        }
        BlockedDetail::new(condition_blockers(state, definition))
            .ok()
            .map(|detail| (id.clone(), detail))
    })
}

fn next_input_unavailable<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<(String, Vec<String>)> {
    state.definition.finalizer_ids.iter().find_map(|id| {
        let definition = state.definition.steps.get(id)?;
        if !finalizer_ready(state, id, definition) {
            return None;
        }
        let references = unavailable_references(state, definition);
        (!references.is_empty()).then(|| (id.clone(), references))
    })
}

fn unavailable_references<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    definition: &RuntimeStep,
) -> Vec<String> {
    definition
        .evidence_prerequisites
        .iter()
        .filter_map(|descriptor| match descriptor {
            Prerequisite::Body { r#ref }
                if output_reference_producer(r#ref).is_some_and(|producer| {
                    !state
                        .steps
                        .get(producer)
                        .is_some_and(|runtime| step_data_available(&runtime.state))
                }) =>
            {
                Some(r#ref.clone())
            }
            Prerequisite::Control { .. }
            | Prerequisite::Condition { .. }
            | Prerequisite::Body { .. } => None,
        })
        .collect()
}

fn select_ready_nodes<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    role: WorkflowNodeRole,
) where
    Cause: Clone,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    let gate_open = matches!(
        (&reduction.state.workflow, role),
        (
            WorkflowState::Executing {
                gate: SchedulingGate::Open,
            },
            WorkflowNodeRole::Step,
        ) | (
            WorkflowState::Finalizing {
                gate: FinalizationGate::Open,
                ..
            },
            WorkflowNodeRole::Finalizer,
        )
    );
    if !gate_open {
        return;
    }
    let active = reduction
        .state
        .steps
        .values()
        .filter(|step| step.state.is_active())
        .count();
    let available_slots = reduction
        .state
        .definition
        .maximum_parallel_steps
        .get()
        .saturating_sub(active);
    let ids = match role {
        WorkflowNodeRole::Step => &reduction.state.definition.ordinary_ids,
        WorkflowNodeRole::Finalizer => &reduction.state.definition.finalizer_ids,
    };
    let selected = ids
        .iter()
        .filter_map(|id| {
            let definition = reduction.state.definition.steps.get(id)?;
            let ready = match role {
                WorkflowNodeRole::Step => ordinary_ready(&reduction.state, id, definition),
                WorkflowNodeRole::Finalizer => {
                    finalizer_ready(&reduction.state, id, definition)
                        && unavailable_references(&reduction.state, definition).is_empty()
                }
            };
            ready.then(|| {
                (
                    id.clone(),
                    resolved_action_inputs(&reduction.state, definition),
                )
            })
        })
        .take(available_slots)
        .collect::<Vec<_>>();
    for (step, inputs) in selected {
        authorize_target(reduction, step, TargetExecutionNumber::FIRST, inputs);
    }
}

fn authorize_target<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: String,
    execution_number: TargetExecutionNumber,
    inputs: BTreeMap<String, ActionInput<Output>>,
) {
    if let Some(runtime) = reduction.state.steps.get_mut(&step) {
        runtime.target_execution = Some(execution_number);
    }
    let sequence = transition_step(reduction, &step, StepState::Starting, None);
    let action_id = ActionId::for_transition(sequence);
    if let Some(runtime) = reduction.state.steps.get_mut(&step) {
        runtime.target_invocation = Some(action_id);
        runtime.active_invocation = Some(ActiveStepInvocation::Target { execution_number });
    }
    set_current_action(&mut reduction.state, &step, action_id);
    reduction.actions.push(RequestedAction {
        id: action_id,
        action: Action::StartStep {
            step,
            execution_number,
            inputs,
        },
    });
}

fn resolved_action_inputs<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    definition: &RuntimeStep,
) -> BTreeMap<String, ActionInput<Output>>
where
    Output: Clone,
{
    definition
        .inputs
        .iter()
        .map(|(input, source)| {
            let value = match source {
                ResolvedValueSource::Input(_) => ActionInput::WorkflowInput,
                // The child-run driver resolves the control values (REQ-WFEXEC-63) and each
                // fan-out element's `item` values (REQ-WFEXEC-67) itself.
                ResolvedValueSource::Wait(_)
                | ResolvedValueSource::ControlsAnswers
                | ResolvedValueSource::Item { .. } => ActionInput::Unavailable,
                ResolvedValueSource::FinalizationContext => state
                    .finalization
                    .as_ref()
                    .map(|finalization| {
                        ActionInput::FinalizationContext(Arc::clone(&finalization.context))
                    })
                    .unwrap_or(ActionInput::Unavailable),
                ResolvedValueSource::Output(source) => state
                    .steps
                    .get(&source.node.id)
                    .and_then(|producer| step_outputs(&producer.state))
                    .and_then(|outputs| outputs.get(&source.output))
                    .cloned()
                    .map_or(ActionInput::Unavailable, ActionInput::Output),
            };
            (input.clone(), value)
        })
        .collect()
}

// A node with a condition is selected to start only after that condition passed in the current
// attempt (INV-ENGINE-16). An evaluation the reducer cannot settle leaves the node pending.
fn condition_cleared<Cause, Output>(
    definition: &RuntimeStep,
    runtime: &StepRuntimeState<Cause, Output>,
) -> bool {
    definition.condition.is_none() || runtime.condition_passed
}

fn ordinary_ready<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step_id: &str,
    definition: &RuntimeStep,
) -> bool
where
    Output: ConditionOutput,
{
    state.steps.get(step_id).is_some_and(|step| {
        matches!(step.state, StepState::Pending) && condition_cleared(definition, step)
    }) && definition
        .prerequisites
        .iter()
        .all(|prerequisite| ordinary_prerequisite_satisfied(state, prerequisite))
        && edge_authorized(state, step_id)
}

// In an edge-scheduled workflow the entry, or the step a continuation's seed names, is
// authorized from the start and any other step only as the `Step` target of the edge a
// `workflow` step that settled in this attempt took; an inherited step routes nothing, and
// nothing is authorized once the walk has exited, entered a wait or taken a continuing edge
// (REQ-WFEXEC-58, REQ-WFEXEC-64, INV-ENGINE-18, INV-ENGINE-20). A step leaves `Pending` when
// it starts, so each taken edge starts its target once; a continuing edge's target starts in
// the next attempt, never in this one.
fn edge_authorized<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step_id: &str,
) -> bool
where
    Output: ConditionOutput,
{
    match &state.definition.scheduling {
        RuntimeScheduling::Dataflow => true,
        RuntimeScheduling::Edges { entry } => {
            state.exit_outcome.is_none()
                && state.wait.is_none()
                && state.continued.is_none()
                && (entry == step_id
                    || state.definition.ordinary_ids.iter().any(|producer| {
                        taken_edge(state, producer)
                            .is_some_and(|(_, edge)| edge == TakenEdge::Step(step_id))
                    }))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TakenEdge<'a> {
    Step(&'a str),
    Exit(&'a str),
    /// The edge is a wait the run enters (REQ-WFEXEC-61).
    Wait(&'a RuntimeEdgeTarget),
    /// The edge names the producer itself or an earlier step, or is a limited edge's `then`
    /// under its count: the run continues from `target` in a new attempt (REQ-WFEXEC-64).
    Continue {
        target: &'a str,
        counted: bool,
    },
    // The recorded outcome names no edge.
    Unrouted,
}

/// The recorded outcome of a `workflow` step that succeeded in this attempt and the edge it
/// took; `None` for any other step or state, an inherited step among them (REQ-WFEXEC-58).
fn taken_edge<'a, Cause, Output, Deadline>(
    state: &'a RuntimeState<Cause, Output, Deadline>,
    producer: &str,
) -> Option<(Option<&'a str>, TakenEdge<'a>)>
where
    Output: ConditionOutput,
{
    let edges = state.definition.steps.get(producer)?.edges.as_ref()?;
    let StepState::Succeeded { outputs } = &state.steps.get(producer)?.state else {
        return None;
    };
    let outcome = outputs
        .get(CHILD_OUTCOME_OUTPUT)
        .and_then(ConditionOutput::condition_text);
    let edge = match outcome.and_then(|outcome| Some((outcome, edges.get(outcome)?))) {
        // The count is compared before the taking: `limit: 2` continues twice and takes
        // `else` the third time (REQ-WFEXEC-64).
        Some((outcome, RuntimeEdgeTarget::Limited { then, limit, else_ })) => {
            let count = state
                .definition
                .edge_counts
                .get(&edge_key(producer, outcome))
                .copied()
                .unwrap_or(0);
            if count < u64::from(*limit) {
                TakenEdge::Continue {
                    target: then,
                    counted: true,
                }
            } else {
                ordinary_edge(&state.definition, producer, else_)
            }
        }
        Some((_, target)) => ordinary_edge(&state.definition, producer, target),
        None => TakenEdge::Unrouted,
    };
    Some((outcome, edge))
}

/// An edge target taken as the format spells it: a step later in document order than the
/// producer starts in this attempt, the producer itself or an earlier step continues the run
/// (REQ-WFEXEC-58, REQ-WFEXEC-64). Validation admits no limited edge inside another.
/// Whether `target` is a step later than `producer` in `order`, the document order: the
/// comparison that makes a `Step` edge start its target in the attempt rather than continue
/// (REQ-WFEXEC-58) and a fan-out go on to its next element (REQ-WFEXEC-66).
pub(crate) fn later_step(order: &[String], producer: &str, target: &str) -> bool {
    let position = |id: &str| order.iter().position(|step| step == id);
    matches!(
        (position(producer), position(target)),
        (Some(producer), Some(target)) if target > producer
    )
}

fn ordinary_edge<'a>(
    definition: &RuntimeDefinition,
    producer: &str,
    target: &'a RuntimeEdgeTarget,
) -> TakenEdge<'a> {
    match target {
        RuntimeEdgeTarget::Step(step) if later_step(&definition.step_order, producer, step) => {
            TakenEdge::Step(step)
        }
        RuntimeEdgeTarget::Step(step) => TakenEdge::Continue {
            target: step,
            counted: false,
        },
        RuntimeEdgeTarget::Exit(name) => TakenEdge::Exit(name),
        RuntimeEdgeTarget::Wait { .. } => TakenEdge::Wait(target),
        RuntimeEdgeTarget::Limited { .. } => TakenEdge::Unrouted,
    }
}

// A taken edge that is not a `Step` ends the walk, and every step that has not started
// becomes `not_run` `not_reached`: an `Exit` records the exit outcome and ends the ordinary
// phase as a success (REQ-WFEXEC-58), a `Wait` records the wait the attempt ends on
// (REQ-WFEXEC-61), and a `Continue` records the edge the attempt ends `continued` on
// (REQ-WFEXEC-64, INV-ENGINE-22). Validation leaves no outcome without an edge, so the remaining case is
// logged and ends the walk with no exit outcome, which the run record reports as `failed`.
fn end_edge_walk<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone + ConditionOutput,
    Deadline: Clone,
{
    // Under an interruption only the routed `workflow` step's edge ends the walk, and an
    // edge that would start a step ends the attempt `continued` instead (REQ-WFEXEC-70).
    let routed = match &reduction.state.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => None,
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_issue: None,
                },
        } => match reduction.state.routed_child.clone() {
            Some(routed) => Some((routed, *reason)),
            None => return,
        },
        _ => return,
    };
    if !matches!(
        reduction.state.definition.scheduling,
        RuntimeScheduling::Edges { .. }
    ) || reduction.state.exit_outcome.is_some()
        || reduction.state.wait.is_some()
        || reduction.state.continued.is_some()
    {
        return;
    }
    // A routed step whose capture failed routes nothing and fails the run.
    if let Some(((step, _), _)) = &routed
        && let Some(StepState::Failed { detail }) = reduction
            .state
            .steps
            .get(step)
            .map(|runtime| &runtime.state)
    {
        let primary_issue = PrimaryIssue::failed(
            WorkflowNode {
                id: step.clone(),
                role: WorkflowNodeRole::Step,
            },
            detail.clone(),
        );
        record_stopped_issue(reduction, primary_issue);
        return;
    }
    let interrupted = routed
        .as_ref()
        .and_then(|((_, interruption), _)| interruption.clone());
    let ended = reduction
        .state
        .definition
        .ordinary_ids
        .iter()
        .filter(|producer| {
            routed
                .as_ref()
                .is_none_or(|((step, _), _)| step == *producer)
        })
        .find_map(|producer| {
            let (outcome, edge) = taken_edge(&reduction.state, producer)?;
            let ending = match edge {
                TakenEdge::Step(target) if routed.is_some() => {
                    WalkEnding::Continue(TakenEdgeRecord {
                        step: producer.clone(),
                        outcome: outcome.unwrap_or_default().to_owned(),
                        target: target.to_owned(),
                        counted: false,
                    })
                }
                TakenEdge::Step(_) => return None,
                TakenEdge::Exit(name) => WalkEnding::Exit(name.to_owned()),
                TakenEdge::Continue { target, counted } => WalkEnding::Continue(TakenEdgeRecord {
                    step: producer.clone(),
                    outcome: outcome.unwrap_or_default().to_owned(),
                    target: target.to_owned(),
                    counted,
                }),
                TakenEdge::Wait(RuntimeEdgeTarget::Wait {
                    kind,
                    then,
                    on_hand_back,
                    rule,
                }) => WalkEnding::Wait(EnteredWait {
                    step: producer.clone(),
                    outcome: outcome.map(str::to_owned),
                    kind: *kind,
                    then: then.clone(),
                    on_hand_back: on_hand_back.clone(),
                    rule: rule.clone(),
                    interrupted: interrupted.clone(),
                    kept: None,
                }),
                TakenEdge::Wait(_) | TakenEdge::Unrouted => WalkEnding::Unrouted(producer.clone()),
            };
            Some(ending)
        });
    match ended {
        None => return,
        Some(WalkEnding::Exit(name)) => reduction.state.exit_outcome = Some(name),
        Some(WalkEnding::Wait(wait)) => reduction.state.wait = Some(wait),
        Some(WalkEnding::Continue(edge)) => reduction.state.continued = Some(edge),
        Some(WalkEnding::Unrouted(producer)) => {
            tracing::error!(event = "edge_walk_unrouted", step = %producer);
        }
    }
    mark_unstarted_not_reached(reduction);
}

enum WalkEnding {
    Exit(String),
    Wait(EnteredWait),
    Continue(TakenEdgeRecord),
    Unrouted(String),
}

fn mark_unstarted_not_reached<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let pending = reduction
        .state
        .definition
        .ordinary_ids
        .iter()
        .filter(|id| {
            reduction
                .state
                .steps
                .get(*id)
                .is_some_and(|runtime| matches!(runtime.state, StepState::Pending))
        })
        .cloned()
        .collect::<Vec<_>>();
    for step in pending {
        transition_step(
            reduction,
            &step,
            StepState::NotRun {
                detail: NonExecutionDetail::not_reached(),
            },
            None,
        );
    }
}

fn finalizer_ready<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    id: &str,
    definition: &RuntimeStep,
) -> bool {
    let Some(finalization) = &state.finalization else {
        return false;
    };
    state.steps.get(id).is_some_and(|step| {
        matches!(step.state, StepState::Pending) && condition_cleared(definition, step)
    }) && definition.when.contains(&finalization.trigger)
        && definition.prerequisites.iter().all(|prerequisite| {
            state
                .steps
                .get(&prerequisite.producer)
                .is_some_and(|producer| producer.state.is_terminal())
        })
}

fn enter_finalization_or_finish<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if !matches!(&reduction.state.workflow, WorkflowState::Executing { .. })
        || !reduction.state.definition.ordinary_ids.iter().all(|id| {
            reduction
                .state
                .steps
                .get(id)
                .is_some_and(|step| step.state.is_terminal())
        })
    {
        return;
    }
    let Some(ordinary_outcome) = ordinary_outcome(&reduction.state) else {
        return;
    };
    if reduction.state.definition.finalizer_ids.is_empty() {
        finish_run(reduction, ordinary_outcome, None);
        return;
    }
    // A wait or a continuing edge ends the attempt without finalization: every finalizer is
    // `not_run` with `finalizer_trigger_not_selected` and the attempt has no finalization
    // summary (REQ-WFEXEC-61, REQ-WFEXEC-64, INV-ENGINE-20).
    if matches!(
        ordinary_outcome,
        OrdinaryOutcome::Waiting { .. } | OrdinaryOutcome::Continued { .. }
    ) {
        let finalizers = reduction
            .state
            .definition
            .finalizer_presentation_order
            .iter()
            .filter(|id| {
                reduction
                    .state
                    .steps
                    .get(*id)
                    .is_some_and(|runtime| matches!(runtime.state, StepState::Pending))
            })
            .cloned()
            .collect::<Vec<_>>();
        for finalizer in finalizers {
            transition_step(
                reduction,
                &finalizer,
                StepState::NotRun {
                    detail: NonExecutionDetail::finalizer_trigger_not_selected(),
                },
                None,
            );
        }
        finish_run(reduction, ordinary_outcome, None);
        return;
    }
    let trigger = match &ordinary_outcome {
        OrdinaryOutcome::Succeeded
        | OrdinaryOutcome::Waiting { .. }
        | OrdinaryOutcome::Continued { .. } => FinalizationTrigger::Succeeded,
        OrdinaryOutcome::Failed { .. } => FinalizationTrigger::Failed,
        OrdinaryOutcome::Cancelled { .. } => FinalizationTrigger::Cancelled,
    };
    let ordinary_issues = ordinary_issues(&reduction.state);
    let primary_issue_step_id = match &ordinary_outcome {
        OrdinaryOutcome::Failed { primary_issue, .. } => Some(primary_issue.node.id.as_str()),
        OrdinaryOutcome::Succeeded
        | OrdinaryOutcome::Cancelled { .. }
        | OrdinaryOutcome::Waiting { .. }
        | OrdinaryOutcome::Continued { .. } => None,
    };
    let ordinary_cancellation = match &ordinary_outcome {
        OrdinaryOutcome::Failed {
            later_cancellation, ..
        } => *later_cancellation,
        OrdinaryOutcome::Cancelled { reason } => Some(*reason),
        OrdinaryOutcome::Succeeded
        | OrdinaryOutcome::Waiting { .. }
        | OrdinaryOutcome::Continued { .. } => None,
    };
    let (run_id, attempt_id) = reduction
        .state
        .finalization_identity
        .as_ref()
        .map_or(("unavailable", "unavailable"), |(run, attempt)| {
            (run.as_str(), attempt.as_str())
        });
    let context = finalization_context::serialize(FinalizationContext {
        run_id,
        attempt_id,
        trigger,
        primary_issue_step_id,
        cancellation_reason: ordinary_cancellation,
        ordinary_issues: &ordinary_issues,
    });
    let primary_issue = match &ordinary_outcome {
        OrdinaryOutcome::Failed { primary_issue, .. } => Some(primary_issue.clone()),
        OrdinaryOutcome::Succeeded
        | OrdinaryOutcome::Cancelled { .. }
        | OrdinaryOutcome::Waiting { .. }
        | OrdinaryOutcome::Continued { .. } => None,
    };
    let force_abort = reduction
        .state
        .force_abort
        .is_some_and(|force_abort| force_abort.phase == RunCancellationPhase::Ordinary);
    let (gate, cancellation) = if force_abort {
        (
            FinalizationGate::Cancelling {
                reason: CancellationReason::ForceAbort,
                deadline: None,
                force_abort: true,
            },
            Some(FinalizationCancellation {
                reason: CancellationReason::ForceAbort,
                deadline: None,
            }),
        )
    } else {
        (FinalizationGate::Open, None)
    };
    let from = reduction.state.workflow.clone();
    let to = WorkflowState::Finalizing {
        trigger,
        gate,
        primary_issue,
    };
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction.state.finalization = Some(FinalizationRuntime {
        trigger,
        context,
        ordinary_outcome,
        cancellation,
        force_abort,
    });
    reduction.events.push(TransitionEvent::Workflow {
        sequence,
        from,
        to: Box::new(to),
    });
}

// Finalization entry and ordinary outcome composition both enumerate the closed ordinary
// outcomes, but they own different state changes and should not share a partial mapper.
fn ordinary_outcome<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<OrdinaryOutcome>
where
    Cause: Clone,
{
    Some(match &state.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => match (&state.wait, &state.continued) {
            (Some(wait), _) => OrdinaryOutcome::Waiting { wait: wait.clone() },
            (None, Some(edge)) => OrdinaryOutcome::Continued { edge: edge.clone() },
            (None, None) => OrdinaryOutcome::Succeeded,
        },
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { primary_issue },
        } => OrdinaryOutcome::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: None,
        },
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_issue: Some(primary_issue),
                },
        } => OrdinaryOutcome::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: Some(*reason),
        },
        // A walk a routed `workflow` step ended under an interruption wins over the stop
        // (REQ-WFEXEC-70).
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_issue: None,
                },
        } => match (
            &state.routed_child,
            &state.wait,
            &state.continued,
            &state.exit_outcome,
        ) {
            (Some(_), Some(wait), _, _) => OrdinaryOutcome::Waiting { wait: wait.clone() },
            // So does the wait an interruption kept before any step of the continuation
            // started (REQ-WFEXEC-72).
            (None, Some(wait), _, _) if wait.kept.is_some() => {
                OrdinaryOutcome::Waiting { wait: wait.clone() }
            }
            (Some(_), None, Some(edge), _) => OrdinaryOutcome::Continued { edge: edge.clone() },
            (Some(_), None, None, Some(_)) => OrdinaryOutcome::Succeeded,
            _ => OrdinaryOutcome::Cancelled { reason: *reason },
        },
        WorkflowState::Finalizing { .. }
        | WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. }
        | WorkflowState::Waiting { .. }
        | WorkflowState::Continued { .. } => return None,
    })
}

fn ordinary_issues<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Vec<OrdinaryIssue> {
    state
        .definition
        .ordinary_ids
        .iter()
        .filter_map(|id| {
            let runtime = state.steps.get(id)?;
            let (disposition, failure, blocked_code) = match &runtime.state {
                StepState::Failed { detail } => {
                    (OrdinaryIssueDisposition::Failed, Some(detail.clone()), None)
                }
                StepState::Blocked { detail } => {
                    (OrdinaryIssueDisposition::Blocked, None, Some(detail.code))
                }
                _ => return None,
            };
            Some(OrdinaryIssue {
                step_id: id.clone(),
                failure_policy: state.definition.steps.get(id)?.failure_policy,
                disposition,
                failure,
                blocked_code,
            })
        })
        .collect()
}

// Finalization and ordinary completion retain distinct quiescence predicates; the similar
// guards are clearer than an abstraction parameterized by role and phase.
fn finish_finalization_if_terminal<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    if !matches!(&reduction.state.workflow, WorkflowState::Finalizing { .. })
        || !reduction.state.definition.finalizer_ids.iter().all(|id| {
            reduction
                .state
                .steps
                .get(id)
                .is_some_and(|step| step.state.is_terminal())
        })
    {
        return;
    }
    let Some(finalization) = reduction.state.finalization.clone() else {
        return;
    };
    let summary = finalization_summary(&reduction.state, &finalization);
    let outcome = compose_final_outcome(&reduction.state, &finalization);
    finish_run(reduction, outcome, Some(summary));
}

fn finalization_summary<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    finalization: &FinalizationRuntime<Deadline>,
) -> FinalizationSummary<Deadline>
where
    Cause: Clone,
    Deadline: Clone,
{
    let finalizers = state
        .definition
        .finalizer_presentation_order
        .iter()
        .filter_map(|id| {
            let runtime = state.steps.get(id)?;
            let disposition = erase_outputs(&runtime.state)?;
            Some(FinalizerResult {
                finalizer: id.clone(),
                failure_policy: state.definition.steps.get(id)?.failure_policy,
                disposition,
            })
        })
        .collect();
    FinalizationSummary {
        trigger: finalization.trigger,
        finalizers,
        cancellation: finalization.cancellation.clone(),
        force_abort: finalization.force_abort,
    }
}

fn erase_outputs<Output>(state: &StepState<Output>) -> Option<StepState<()>> {
    Some(match state {
        StepState::Succeeded { .. } => StepState::Succeeded {
            outputs: BTreeMap::new(),
        },
        StepState::Inherited { .. } => return None,
        StepState::Failed { detail } => StepState::Failed {
            detail: detail.clone(),
        },
        StepState::Blocked { detail } => StepState::Blocked {
            detail: detail.clone(),
        },
        StepState::Skipped { detail } => StepState::Skipped {
            detail: detail.clone(),
        },
        StepState::NotRun { detail } => StepState::NotRun { detail: *detail },
        StepState::Cancelled { detail } => StepState::Cancelled { detail: *detail },
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => return None,
    })
}

fn compose_final_outcome<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    finalization: &FinalizationRuntime<Deadline>,
) -> OrdinaryOutcome
where
    Cause: Clone,
{
    match &finalization.ordinary_outcome {
        OrdinaryOutcome::Failed {
            primary_issue,
            later_cancellation,
        } => OrdinaryOutcome::Failed {
            primary_issue: primary_issue.clone(),
            later_cancellation: later_cancellation.or_else(|| {
                finalization
                    .cancellation
                    .as_ref()
                    .map(|record| record.reason)
            }),
        },
        OrdinaryOutcome::Cancelled { reason } => OrdinaryOutcome::Cancelled { reason: *reason },
        OrdinaryOutcome::Waiting { wait } => OrdinaryOutcome::Waiting { wait: wait.clone() },
        OrdinaryOutcome::Continued { edge } => OrdinaryOutcome::Continued { edge: edge.clone() },
        OrdinaryOutcome::Succeeded => match &state.workflow {
            WorkflowState::Finalizing {
                primary_issue: Some(primary_issue),
                ..
            } => OrdinaryOutcome::Failed {
                primary_issue: primary_issue.clone(),
                later_cancellation: finalization
                    .cancellation
                    .as_ref()
                    .map(|record| record.reason),
            },
            WorkflowState::Finalizing {
                gate: FinalizationGate::Cancelling { reason, .. },
                primary_issue: None,
                ..
            } => OrdinaryOutcome::Cancelled { reason: *reason },
            _ => OrdinaryOutcome::Succeeded,
        },
    }
}

fn finish_run<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    ordinary_outcome: OrdinaryOutcome,
    summary: Option<FinalizationSummary<Deadline>>,
) where
    Cause: Clone,
    Output: Clone,
    Deadline: Clone,
{
    let Some(exports) = derive_exports(&reduction.state) else {
        return;
    };
    let (to, outcome) = match ordinary_outcome {
        OrdinaryOutcome::Succeeded => (WorkflowState::Succeeded, RunOutcome::Succeeded),
        OrdinaryOutcome::Failed {
            primary_issue,
            later_cancellation,
        } => (
            WorkflowState::Failed {
                primary_issue: primary_issue.clone(),
                later_cancellation,
            },
            RunOutcome::Failed {
                primary_issue,
                later_cancellation,
            },
        ),
        OrdinaryOutcome::Cancelled { reason } => (
            WorkflowState::Cancelled { reason },
            RunOutcome::Cancelled { reason },
        ),
        OrdinaryOutcome::Waiting { wait } => (
            WorkflowState::Waiting { wait: wait.clone() },
            RunOutcome::Waiting { wait },
        ),
        OrdinaryOutcome::Continued { edge } => (
            WorkflowState::Continued { edge: edge.clone() },
            RunOutcome::Continued { edge },
        ),
    };
    let from = reduction.state.workflow.clone();
    let sequence = next_sequence(&mut reduction.state);
    reduction.state.workflow = to.clone();
    reduction.state.exports = Some(exports.clone());
    reduction.state.finalization_summary = summary.clone();
    reduction.events.push(TransitionEvent::Workflow {
        sequence,
        from,
        to: Box::new(to),
    });
    reduction.actions.push(RequestedAction {
        id: ActionId::for_transition(sequence),
        action: Action::FinishRun { outcome, exports },
    });
}

fn derive_exports<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
) -> Option<ExportSet<Output>>
where
    Output: Clone,
{
    state
        .definition
        .exports
        .iter()
        .map(|(name, source)| {
            let step = state.steps.get(&source.step)?;
            let value = match &step.state {
                StepState::Succeeded { outputs } => match outputs.get(&source.output) {
                    Some(output) => ExportValue::Available {
                        output: output.clone(),
                    },
                    // A `workflow` step commits only the child exports that were available,
                    // so an export of one it left out is unavailable, as for an inherited
                    // step whose output was not retained.
                    None if state
                        .definition
                        .steps
                        .get(&source.step)
                        .is_some_and(|definition| definition.edges.is_some()) =>
                    {
                        ExportValue::Unavailable {
                            reason: ExportUnavailableReason::Failed,
                        }
                    }
                    None => return None,
                },
                StepState::Inherited {
                    disposition: InheritedDisposition::Succeeded,
                    outputs,
                    ..
                } => outputs.get(&source.output).map_or(
                    ExportValue::Unavailable {
                        reason: ExportUnavailableReason::Failed,
                    },
                    |output| ExportValue::Available {
                        output: output.clone(),
                    },
                ),
                StepState::Inherited {
                    disposition: InheritedDisposition::Skipped,
                    ..
                } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::Skipped,
                },
                StepState::Failed { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::Failed,
                },
                StepState::Blocked { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::Blocked,
                },
                StepState::Skipped { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::Skipped,
                },
                StepState::NotRun {
                    detail:
                        NonExecutionDetail {
                            code: NonExecutionCode::FinalizerTriggerNotSelected,
                        },
                } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::TriggerNotSelected,
                },
                StepState::NotRun { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::NotRun,
                },
                StepState::Cancelled { .. } => ExportValue::Unavailable {
                    reason: ExportUnavailableReason::Cancelled,
                },
                StepState::Pending
                | StepState::Starting
                | StepState::Running
                | StepState::CapturingOutputs
                | StepState::Recovering { .. }
                | StepState::Cancelling { .. } => return None,
            };
            Some((name.clone(), value))
        })
        .collect()
}

fn step_accepts<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
    expected_state: StepStateKind,
    action: ActionId,
) -> bool {
    state.steps.get(step).is_some_and(|runtime| {
        runtime.state.kind() == expected_state && runtime.current_action == Some(action)
    })
}

fn step_declares_outputs<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
) -> bool {
    state
        .definition
        .steps
        .get(step)
        .is_some_and(|definition| !definition.declared_outputs.is_empty())
}

// A `workflow` step commits the subset of its derived outputs its child made available, which
// always includes the text `outcome` its edges route on (REQ-WFEXEC-58); every other step
// commits exactly its declared outputs.
fn outputs_match_declaration<Cause, Output, Deadline>(
    state: &RuntimeState<Cause, Output, Deadline>,
    step: &str,
    outputs: &OutputSet<Output>,
) -> bool
where
    Output: ConditionOutput,
{
    state
        .definition
        .steps
        .get(step)
        .is_some_and(|definition| match &definition.edges {
            None => outputs.keys().eq(definition.declared_outputs.iter()),
            Some(_) => {
                outputs
                    .keys()
                    .all(|name| definition.declared_outputs.contains(name))
                    && outputs
                        .get(CHILD_OUTCOME_OUTPUT)
                        .and_then(ConditionOutput::condition_text)
                        .is_some()
            }
        })
}

fn set_current_action<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
    step: &str,
    action: ActionId,
) {
    if let Some(runtime) = state.steps.get_mut(step) {
        runtime.current_action = Some(action);
    }
}

fn transition_step<Provisional, Cause, Output, Deadline>(
    reduction: &mut Reduction<Provisional, Cause, Output, Deadline>,
    step: &str,
    to: StepState<Output>,
    current_action: Option<ActionId>,
) -> TransitionSequence {
    let sequence = next_sequence(&mut reduction.state);
    let definition = reduction.state.definition.steps.get(step);
    let failure_policy = definition
        .map(|definition| definition.failure_policy)
        .unwrap_or_default();
    let role = definition
        .map(|definition| definition.role)
        .unwrap_or(WorkflowNodeRole::Step);
    if let Some(runtime) = reduction.state.steps.get_mut(step) {
        let from = runtime.state.kind();
        let to_kind = to.kind();
        runtime.state = to;
        runtime.current_action = current_action;
        reduction.events.push(TransitionEvent::Step {
            sequence,
            step: step.to_owned(),
            role,
            failure_policy,
            from,
            to: to_kind,
        });
    }
    sequence
}

fn next_sequence<Cause, Output, Deadline>(
    state: &mut RuntimeState<Cause, Output, Deadline>,
) -> TransitionSequence {
    let sequence = state.last_transition_sequence.next();
    if sequence.get() > state.definition.maximum_transitions {
        state.transition_capacity_exceeded = true;
        return state.last_transition_sequence;
    }
    state.last_transition_sequence = sequence;
    sequence
}

#[cfg(test)]
mod tests;
