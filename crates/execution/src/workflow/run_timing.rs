use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use time::OffsetDateTime;

use super::admission::CancellationReason;
use super::evidence::FailurePhase;
use super::observation::{ExecutionObservation, ObservedStepTransition};
use super::presentation_feed::DisplayDeadline;
use super::runtime::{StepStateKind, TransitionEvent, WorkflowState};

#[derive(Clone, Copy, Debug)]
pub struct ObservationTime {
    pub utc: OffsetDateTime,
    pub monotonic: Instant,
}

pub trait ObservationClock: Clone + Send + Sync + 'static {
    fn sample(&self) -> ObservationTime;
}

#[derive(Clone, Copy, Debug)]
pub struct ObservedStepTiming {
    pub started: ObservationTime,
    pub finished: Option<Instant>,
}

#[derive(Clone, Debug)]
pub struct RunTimingSnapshot {
    pub(crate) presentation_opened: ObservationTime,
    pub execution_started: Option<ObservationTime>,
    pub steps: BTreeMap<String, ObservedStepTiming>,
    pub cancellation: Option<(CancellationReason, OffsetDateTime)>,
    pub terminal: Option<ObservationTime>,
    pub(crate) quiesced: Option<ObservationTime>,
}

#[derive(Clone)]
pub struct RunTimingObservation {
    state: Arc<Mutex<RunTimingSnapshot>>,
}

impl RunTimingObservation {
    pub fn new(presentation_opened: ObservationTime) -> Self {
        Self {
            state: Arc::new(Mutex::new(RunTimingSnapshot {
                presentation_opened,
                execution_started: None,
                steps: BTreeMap::new(),
                cancellation: None,
                terminal: None,
                quiesced: None,
            })),
        }
    }

    pub fn mark_execution_started(&self, observed_at: ObservationTime) {
        lock_timing(&self.state)
            .execution_started
            .get_or_insert(observed_at);
    }

    pub fn observe<Deadline: DisplayDeadline>(
        &self,
        observation: &ExecutionObservation<Deadline>,
        clock: &impl ObservationClock,
    ) {
        if observation_needs_timing_sample(observation) {
            self.record(observation, clock.sample());
        }
    }

    pub fn record<Deadline: DisplayDeadline>(
        &self,
        observation: &ExecutionObservation<Deadline>,
        observed_at: ObservationTime,
    ) {
        let ExecutionObservation::Transition(transition) = observation else {
            return;
        };
        let mut timing = lock_timing(&self.state);
        match &transition.event {
            TransitionEvent::Step { step, to, .. } if *to == StepStateKind::Starting => {
                timing
                    .steps
                    .entry(step.clone())
                    .or_insert(ObservedStepTiming {
                        started: observed_at,
                        finished: None,
                    });
            }
            // A step an interruption fails before it starts never passes through `starting`,
            // yet every failure outside `condition` publishes timing (REQ-WFEXEC-40), so it
            // starts and finishes at the failure (REQ-WFEXEC-69).
            TransitionEvent::Step {
                step,
                to: StepStateKind::Failed,
                ..
            } if !timing.steps.contains_key(step) && failure_is_timed(transition.step.as_ref()) => {
                timing.steps.insert(
                    step.clone(),
                    ObservedStepTiming {
                        started: observed_at,
                        finished: Some(observed_at.monotonic),
                    },
                );
            }
            TransitionEvent::Step {
                step,
                to:
                    StepStateKind::Succeeded
                    | StepStateKind::Failed
                    | StepStateKind::Blocked
                    | StepStateKind::NotRun
                    | StepStateKind::Cancelled,
                ..
            } => {
                if let Some(step) = timing.steps.get_mut(step) {
                    step.finished.get_or_insert(observed_at.monotonic);
                }
            }
            TransitionEvent::CancellationAccepted {
                reason, deadline, ..
            } => {
                timing
                    .cancellation
                    .get_or_insert((*reason, deadline.deadline_utc()));
            }
            TransitionEvent::Workflow { to, .. }
                if matches!(
                    to.as_ref(),
                    WorkflowState::Succeeded
                        | WorkflowState::Failed { .. }
                        | WorkflowState::Cancelled { .. }
                        | WorkflowState::Waiting { .. }
                        | WorkflowState::Continued { .. }
                ) =>
            {
                timing.terminal.get_or_insert(observed_at);
            }
            TransitionEvent::Step { .. }
            | TransitionEvent::Workflow { .. }
            | TransitionEvent::FinalizationCancellationAccepted { .. }
            | TransitionEvent::ForceAbortAccepted { .. } => {}
        }
    }

    pub(crate) fn mark_quiesced(&self, observed_at: ObservationTime) {
        lock_timing(&self.state).quiesced.get_or_insert(observed_at);
    }

    pub fn snapshot(&self) -> RunTimingSnapshot {
        lock_timing(&self.state).clone()
    }
}

fn failure_is_timed(step: Option<&ObservedStepTransition>) -> bool {
    matches!(
        step,
        Some(ObservedStepTransition::Failed { detail })
            if !matches!(detail.phase, FailurePhase::Condition)
    )
}

fn observation_needs_timing_sample<Deadline>(observation: &ExecutionObservation<Deadline>) -> bool {
    let ExecutionObservation::Transition(transition) = observation else {
        return false;
    };
    match &transition.event {
        TransitionEvent::Step {
            to:
                StepStateKind::Starting
                | StepStateKind::Succeeded
                | StepStateKind::Failed
                | StepStateKind::Blocked
                | StepStateKind::NotRun
                | StepStateKind::Cancelled,
            ..
        }
        | TransitionEvent::CancellationAccepted { .. }
        | TransitionEvent::FinalizationCancellationAccepted { .. }
        | TransitionEvent::ForceAbortAccepted { .. } => true,
        TransitionEvent::Workflow { to, .. } => matches!(
            to.as_ref(),
            WorkflowState::Succeeded
                | WorkflowState::Failed { .. }
                | WorkflowState::Cancelled { .. }
                | WorkflowState::Waiting { .. }
                | WorkflowState::Continued { .. }
        ),
        TransitionEvent::Step { .. } => false,
    }
}

fn lock_timing(timing: &Mutex<RunTimingSnapshot>) -> MutexGuard<'_, RunTimingSnapshot> {
    timing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::super::document::FailurePolicy;
    use super::super::evidence::FailureDetail;
    use super::super::observation::{ObservedExecutions, TransitionObservation};
    use super::super::runtime::TransitionSequence;
    use super::super::validated::WorkflowNodeRole;
    use super::*;

    fn failed(step: &str, detail: FailureDetail) -> ExecutionObservation<OffsetDateTime> {
        ExecutionObservation::Transition(Box::new(TransitionObservation {
            event: TransitionEvent::Step {
                sequence: TransitionSequence(1),
                step: step.to_owned(),
                role: WorkflowNodeRole::Step,
                failure_policy: FailurePolicy::Required,
                from: StepStateKind::Pending,
                to: StepStateKind::Failed,
            },
            step: Some(ObservedStepTransition::Failed { detail }),
            executions: ObservedExecutions::default(),
        }))
    }

    // REQ-WFEXEC-69, REQ-WFEXEC-40: a step an interruption fails before it starts is timed
    // at that instant, while a `condition` failure, which never ran, stays untimed.
    #[test]
    fn a_failure_without_a_start_is_timed_unless_its_phase_is_condition() {
        let observed_at = ObservationTime {
            utc: OffsetDateTime::UNIX_EPOCH,
            monotonic: um_support::monotonic_now(),
        };
        let timing = RunTimingObservation::new(observed_at);
        timing.record(
            &failed("interrupted", FailureDetail::interrupted_before_start()),
            observed_at,
        );
        timing.record(
            &failed(
                "condition",
                FailureDetail::json_pointer_missing("outputs.upstream.ready", "/ready").unwrap(),
            ),
            observed_at,
        );

        let snapshot = timing.snapshot();
        let interrupted = snapshot.steps["interrupted"];
        assert_eq!(interrupted.started.monotonic, observed_at.monotonic);
        assert_eq!(interrupted.finished, Some(observed_at.monotonic));
        assert!(!snapshot.steps.contains_key("condition"));
    }
}
