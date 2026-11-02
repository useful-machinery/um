use std::future::{Future, ready};
use std::sync::Arc;

use super::admission::CancellationReason;
use super::agent::AgentObservationEnvelope;
use super::evidence::{
    BlockedDetail, CancellationDetail, ConditionFalseDetail, FailureDetail, NonExecutionDetail,
};
use super::runtime::{
    ActionId, ActiveStepInvocation, RecoveryDecisionKind, RecoveryHandlerActivity,
    RecoveryHandlerKind, TransitionEvent,
};
use super::step_runtime::{RecoveryFailureKind, StepFailureKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandOutputSource {
    StandardOutput,
    StandardError,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SourceSequence(u64);

impl SourceSequence {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    pub(super) const fn first() -> Self {
        Self(1)
    }

    pub(super) fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandOutputObservation {
    pub(crate) step: String,
    pub(crate) invocation: ActionId,
    pub(crate) source: CommandOutputSource,
    pub(crate) sequence: SourceSequence,
    pub(crate) bytes: Arc<[u8]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandOutputClosedObservation {
    pub(crate) step: String,
    pub(crate) invocation: ActionId,
    pub(crate) source: CommandOutputSource,
    pub(crate) sequence: SourceSequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedStepTransition {
    Recovery {
        active: ActiveStepInvocation,
        active_invocation_id: ActionId,
        settled_invocation: Option<(ActionId, ActiveStepInvocation)>,
        configured_rounds: u8,
        handler_kind: Option<RecoveryHandlerKind>,
        handler_state: Option<RecoveryHandlerActivity>,
        decision: Option<RecoveryDecisionKind>,
    },
    OutputsCommitted {
        outputs: Vec<String>,
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
    Cancelling {
        detail: CancellationDetail,
    },
    Cancelled {
        detail: CancellationDetail,
    },
}

/// How one physical target, finalizer or recovery-handler execution ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionSettlement {
    /// The execution completed, a handler that decided either way included.
    Succeeded,
    Failed(StepFailureKind),
    Cancelled(CancellationReason),
}

/// The physical execution a step transition settles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SettledExecution {
    pub invocation: ActionId,
    pub role: ActiveStepInvocation,
    pub settlement: ExecutionSettlement,
}

/// The physical execution a step transition starts; a recheck names the kind of the failure
/// it follows, the kind a handler's context names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartedExecution {
    pub invocation: ActionId,
    pub role: ActiveStepInvocation,
    pub recovery_failure_kind: Option<RecoveryFailureKind>,
}

/// The physical executions one committed transition settles and starts; empty for every
/// transition that neither ends nor begins a target, finalizer or recovery-handler execution.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObservedExecutions {
    pub settled: Option<SettledExecution>,
    pub started: Option<StartedExecution>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionObservation<Deadline> {
    pub event: TransitionEvent<Deadline>,
    pub step: Option<ObservedStepTransition>,
    pub executions: ObservedExecutions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionObservation<Deadline> {
    Transition(Box<TransitionObservation<Deadline>>),
    CommandOutput(CommandOutputObservation),
    CommandOutputClosed(CommandOutputClosedObservation),
    Agent(AgentObservationEnvelope),
}

pub trait ExecutionObserver<Deadline>: Clone + Send + Sync + 'static {
    fn observe(
        &self,
        observation: ExecutionObservation<Deadline>,
    ) -> impl Future<Output = ()> + Send;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NoopExecutionObserver;

impl<Deadline> ExecutionObserver<Deadline> for NoopExecutionObserver {
    fn observe(
        &self,
        _observation: ExecutionObservation<Deadline>,
    ) -> impl Future<Output = ()> + Send {
        ready(())
    }
}
