use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::future::{Future, ready};
use std::path::PathBuf;

use super::admission::AdmittedWorkflow;
use super::artifact::ArtifactStaging;
use super::coordinator::{CommitPort, CommittedReduction, CoordinationError, CoordinatorClock};
use super::diagnostic::StepDiagnosticLog;
use super::input::InputStaging;
use super::observation::{
    ExecutionObservation, ExecutionObserver, ExecutionSettlement, ObservedExecutions,
    ObservedStepTransition, SettledExecution, StartedExecution, TransitionObservation,
};
use super::resolution::{WorkflowContentDigest, WorkflowSourceProvenance};
use super::runtime::{
    ActionId, ActiveStepInvocation, ExportSet, OutputProducer, RecoveryTerminalDisposition,
    RunOutcome, RuntimeState, StepState, StepStateKind, TransitionEvent, WorkflowState,
};
use super::step_runtime::{
    AgentExecution, RecoveryFailureKind, StepFailureCause, StepFailureKind,
    WorkflowAgentDispatcher, WorkflowCommitPort, WorkflowExecutionStart, execute_workflow_observed,
};
use super::value::CapturedValue;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowExecutionResult<Deadline = ()> {
    pub outcome: RunOutcome,
    pub steps: BTreeMap<String, StepState<CapturedValue>>,
    pub recoveries: BTreeMap<String, Option<super::runtime::StepRecoveryState<StepFailureCause>>>,
    pub output_producers: BTreeMap<(String, String), OutputProducer>,
    pub finalization_summary: Option<super::runtime::FinalizationSummary<Deadline>>,
    pub force_abort: Option<super::runtime::ForceAbortEvidence>,
    pub exports: ExportSet<CapturedValue>,
    pub provenance: WorkflowSourceProvenance,
    pub content_digest: WorkflowContentDigest,
}

pub struct NoopCommitPort;

/// Cloud's step commit runs before observer success and before dependent actions.
/// The filesystem work must not block the current-thread runner executor.
pub struct CloudStepCommitPort {
    private: PathBuf,
    attempt_id: String,
    attempt_number: u64,
    artifacts: ArtifactStaging,
    prior: Option<(PathBuf, String, u64)>,
    committed: BTreeSet<String>,
}

impl CloudStepCommitPort {
    pub fn new(
        private: PathBuf,
        attempt_id: String,
        attempt_number: u64,
        artifacts: ArtifactStaging,
        prior: Option<(PathBuf, String, u64)>,
    ) -> Self {
        Self {
            private,
            attempt_id,
            attempt_number,
            artifacts,
            prior,
            committed: BTreeSet::new(),
        }
    }
}

impl<Deadline: Send> CommitPort<CommittedReduction<StepFailureCause, CapturedValue, Deadline>>
    for CloudStepCommitPort
{
    type Error = super::local_run::LocalRunDirectoryError;

    fn commit(
        &mut self,
        commit: CommittedReduction<StepFailureCause, CapturedValue, Deadline>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        let steps: BTreeMap<_, _> = commit
            .state
            .steps
            .iter()
            .filter(|(node, runtime)| {
                !self.committed.contains(*node)
                    && matches!(
                        runtime.state,
                        StepState::Succeeded { .. }
                            | StepState::Skipped { .. }
                            | StepState::Inherited { .. }
                    )
            })
            .map(|(node, runtime)| (node.clone(), runtime.state.clone()))
            .collect();
        self.committed.extend(steps.keys().cloned());
        let private = self.private.clone();
        let attempt_id = self.attempt_id.clone();
        let attempt_number = self.attempt_number;
        let artifacts = self.artifacts.clone();
        let prior = self.prior.clone();
        let producers = commit.state.output_producers.clone();
        async move {
            if steps.is_empty() {
                return Ok(());
            }
            let retained = tokio::task::spawn_blocking(move || {
                super::local_run::retain_cloud_completed_steps(
                    &private,
                    &attempt_id,
                    attempt_number,
                    &steps,
                    &producers,
                    &artifacts,
                    prior
                        .as_ref()
                        .map(|(path, id, number)| (path.as_path(), id.as_str(), *number)),
                )
            })
            .await
            .map_err(|_| {
                tracing::error!(
                    event = "cloud_step_checkpoint_failed",
                    cause = "worker_join_failed"
                );
                super::local_run::LocalRunDirectoryError::StateWriteUnavailable
            })?;
            if let Err(error) = &retained {
                // Never emit private paths or carrier contents from the OS error.
                let cause = match error {
                    super::local_run::LocalRunDirectoryError::StateInvalid => "invalid_state",
                    super::local_run::LocalRunDirectoryError::StateConflict => "state_conflict",
                    super::local_run::LocalRunDirectoryError::StateWriteUnavailable => {
                        "state_write_unavailable"
                    }
                    super::local_run::LocalRunDirectoryError::StagingUnavailable => {
                        "staging_unavailable"
                    }
                    super::local_run::LocalRunDirectoryError::CarrierInvalid => "carrier_invalid",
                    super::local_run::LocalRunDirectoryError::Artifact { .. } => {
                        "artifact_unavailable"
                    }
                    super::local_run::LocalRunDirectoryError::File { source, .. } => {
                        tracing::error!(event = "cloud_step_checkpoint_failed", cause = "file_io", kind = ?source.kind());
                        return retained;
                    }
                    _ => "retention_unavailable",
                };
                tracing::error!(event = "cloud_step_checkpoint_failed", cause);
            }
            retained
        }
    }
}

impl<Commit> CommitPort<Commit> for NoopCommitPort {
    type Error = Infallible;

    fn commit(&mut self, _commit: Commit) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
    }
}

struct DurableObserverCommitPort<Commits, Observer> {
    commits: Commits,
    observer: Observer,
}

impl<Deadline, Commits, Observer>
    CommitPort<CommittedReduction<StepFailureCause, CapturedValue, Deadline>>
    for DurableObserverCommitPort<Commits, Observer>
where
    Deadline: Clone + Send + 'static,
    Commits: CommitPort<CommittedReduction<StepFailureCause, CapturedValue, Deadline>>,
    Observer: ExecutionObserver<Deadline>,
{
    type Error = Commits::Error;

    fn commit(
        &mut self,
        commit: CommittedReduction<StepFailureCause, CapturedValue, Deadline>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        let state = commit.state.clone();
        let events = commit.events.clone();
        let committed = self.commits.commit(commit);
        let observer = self.observer.clone();
        async move {
            committed.await?;
            for event in events {
                let step = observed_step_transition(&event, &state);
                let executions = observed_executions(&event, &state);
                observer
                    .observe(ExecutionObservation::Transition(Box::new(
                        TransitionObservation {
                            event,
                            step,
                            executions,
                        },
                    )))
                    .await;
            }
            Ok(())
        }
    }
}

fn observed_step_transition<Deadline>(
    event: &TransitionEvent<Deadline>,
    state: &RuntimeState<StepFailureCause, CapturedValue, Deadline>,
) -> Option<ObservedStepTransition> {
    let TransitionEvent::Step { step, from, to, .. } = event else {
        return None;
    };
    let runtime = state.steps.get(step)?;
    if matches!(
        to,
        StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Recovering
    ) && let Some(recovery) = runtime
        .recovery
        .as_ref()
        .filter(|recovery| !recovery.rounds.is_empty())
        && let Some(active) = runtime.active_invocation
        && let Some(active_invocation_id) = match active {
            super::runtime::ActiveStepInvocation::Target { .. } => runtime.target_invocation,
            super::runtime::ActiveStepInvocation::RecoveryHandler { .. } => runtime.current_action,
        }
    {
        let settled_invocation = if matches!(
            from,
            StepStateKind::Running | StepStateKind::CapturingOutputs
        ) {
            recovery.rounds.last().map(|round| {
                (
                    round.failed_execution.invocation,
                    super::runtime::ActiveStepInvocation::Target {
                        execution_number: round.failed_execution.execution_number,
                    },
                )
            })
        } else {
            None
        };
        let handler_state = match runtime.state {
            StepState::Recovering { handler, .. } => Some(handler),
            _ => None,
        };
        let decision = recovery.rounds.last().and_then(|round| {
            round
                .handler
                .as_ref()
                .and_then(|handler| match handler.outcome {
                    super::runtime::RecoveryHandlerOutcome::Recheck { .. } => {
                        Some(super::runtime::RecoveryDecisionKind::Recheck)
                    }
                    super::runtime::RecoveryHandlerOutcome::GaveUp { .. } => {
                        Some(super::runtime::RecoveryDecisionKind::GaveUp)
                    }
                    super::runtime::RecoveryHandlerOutcome::Starting
                    | super::runtime::RecoveryHandlerOutcome::Running
                    | super::runtime::RecoveryHandlerOutcome::Failed { .. }
                    | super::runtime::RecoveryHandlerOutcome::Cancelled => None,
                })
        });
        return Some(ObservedStepTransition::Recovery {
            active,
            active_invocation_id,
            settled_invocation,
            configured_rounds: recovery.configured_rounds,
            handler_kind: recovery.handler_kind,
            handler_state,
            decision,
        });
    }
    match (to, &runtime.state) {
        (StepStateKind::Succeeded, StepState::Succeeded { outputs }) => {
            Some(ObservedStepTransition::OutputsCommitted {
                outputs: outputs.keys().cloned().collect(),
            })
        }
        (StepStateKind::Failed, StepState::Failed { detail }) => {
            Some(ObservedStepTransition::Failed {
                detail: detail.clone(),
            })
        }
        (StepStateKind::Blocked, StepState::Blocked { detail }) => {
            Some(ObservedStepTransition::Blocked {
                detail: detail.clone(),
            })
        }
        (StepStateKind::Skipped, StepState::Skipped { detail }) => {
            Some(ObservedStepTransition::Skipped {
                detail: detail.clone(),
            })
        }
        (StepStateKind::NotRun, StepState::NotRun { detail }) => {
            Some(ObservedStepTransition::NotRun { detail: *detail })
        }
        (StepStateKind::Cancelling, StepState::Cancelling { detail }) => {
            Some(ObservedStepTransition::Cancelling { detail: *detail })
        }
        (StepStateKind::Cancelled, StepState::Cancelled { detail }) => {
            Some(ObservedStepTransition::Cancelled { detail: *detail })
        }
        _ => None,
    }
}

/// The physical target, finalizer or recovery-handler execution a committed step transition
/// settles and the one it starts, read from the post-commit step state (REQ-TELEMETRY-41).
fn observed_executions<Deadline>(
    event: &TransitionEvent<Deadline>,
    state: &RuntimeState<StepFailureCause, CapturedValue, Deadline>,
) -> ObservedExecutions {
    let TransitionEvent::Step { step, from, to, .. } = event else {
        return ObservedExecutions::default();
    };
    let Some(runtime) = state.steps.get(step) else {
        return ObservedExecutions::default();
    };
    let settles = |(invocation, role): (ActionId, ActiveStepInvocation), settlement| {
        Some(SettledExecution {
            invocation,
            role,
            settlement,
        })
    };
    let target = runtime.target_execution.zip(runtime.target_invocation).map(
        |(execution_number, invocation)| {
            (
                invocation,
                ActiveStepInvocation::Target { execution_number },
            )
        },
    );
    let recovery = runtime.recovery.as_ref();
    let round = recovery.and_then(|recovery| recovery.rounds.last());
    let handler = round.and_then(|round| {
        round.handler.as_ref().map(|handler| {
            (
                handler.invocation,
                ActiveStepInvocation::RecoveryHandler {
                    round: round.number,
                },
            )
        })
    });
    let failed_target = round.and_then(|round| {
        let failed = &round.failed_execution;
        settles(
            (
                failed.invocation,
                ActiveStepInvocation::Target {
                    execution_number: failed.execution_number,
                },
            ),
            ExecutionSettlement::Failed(StepFailureKind::of(&failed.cause)),
        )
    });
    let (settled, started) = match to {
        StepStateKind::Starting => {
            let recovery_failure_kind = runtime
                .target_execution
                .filter(|execution| execution.get() > 1)
                .and(round)
                .map(|round| {
                    RecoveryFailureKind::classify(
                        round.failed_execution.phase,
                        &round.failed_execution.cause,
                    )
                });
            let settled = match from {
                StepStateKind::Pending => None,
                StepStateKind::Recovering => {
                    handler.and_then(|handler| settles(handler, ExecutionSettlement::Succeeded))
                }
                _ => failed_target
                    .filter(|failed| Some(failed.invocation) != runtime.target_invocation),
            };
            let started = target.map(|(invocation, role)| StartedExecution {
                invocation,
                role,
                recovery_failure_kind,
            });
            (settled, started)
        }
        StepStateKind::Recovering if *from != StepStateKind::Recovering => (
            failed_target,
            handler.map(|(invocation, role)| StartedExecution {
                invocation,
                role,
                recovery_failure_kind: None,
            }),
        ),
        StepStateKind::Succeeded => (
            target.and_then(|target| settles(target, ExecutionSettlement::Succeeded)),
            None,
        ),
        StepStateKind::Failed => {
            let settled = match recovery.and_then(|recovery| recovery.terminal_disposition) {
                Some(RecoveryTerminalDisposition::GaveUp { .. }) => {
                    handler.and_then(|handler| settles(handler, ExecutionSettlement::Succeeded))
                }
                Some(RecoveryTerminalDisposition::HandlerFailed { .. }) => {
                    handler.and_then(|handler| {
                        settles(
                            handler,
                            ExecutionSettlement::Failed(StepFailureKind::RecoveryHandler),
                        )
                    })
                }
                _ => target.and_then(|target| {
                    settles(
                        target,
                        ExecutionSettlement::Failed(
                            runtime.failure_kind.unwrap_or(StepFailureKind::Execution),
                        ),
                    )
                }),
            };
            (settled, None)
        }
        StepStateKind::Cancelled => {
            let StepState::Cancelled { detail } = &runtime.state else {
                return ObservedExecutions::default();
            };
            let open = match recovery.and_then(|recovery| recovery.terminal_disposition) {
                Some(RecoveryTerminalDisposition::Cancelled {
                    active: ActiveStepInvocation::RecoveryHandler { .. },
                    ..
                }) => handler,
                _ => target,
            };
            (
                open.and_then(|open| settles(open, ExecutionSettlement::Cancelled(detail.code))),
                None,
            )
        }
        _ => (None, None),
    };
    ObservedExecutions { settled, started }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the local adapter additionally supplies durable process-guard registration"
)]
pub async fn execute_workflow<Clock, Commits, Observer, Dispatcher>(
    admitted: AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    inputs: &InputStaging,
    diagnostics: &StepDiagnosticLog,
    agents: AgentExecution<Dispatcher>,
    clock: Clock,
    commits: Commits,
    observer: Observer,
    start: impl Into<WorkflowExecutionStart>,
) -> Result<WorkflowExecutionResult<Clock::Instant>, CoordinationError>
// This result projection intentionally repeats the shared runtime's generic port
// constraints so it can preserve its distinct domain result.
where
    Clock: CoordinatorClock,
    Clock::Instant: Sync,
    Commits: WorkflowCommitPort<Clock>,
    Observer: ExecutionObserver<Clock::Instant>,
    Dispatcher: WorkflowAgentDispatcher<Clock::Instant, Observer>,
{
    let provenance = admitted.workflow().source.clone();
    let content_digest = admitted.workflow().content_digest.clone();
    let start = start.into();
    let coordinated = execute_workflow_observed(
        admitted,
        artifacts,
        inputs,
        diagnostics,
        clock,
        DurableObserverCommitPort {
            commits,
            observer: observer.clone(),
        },
        observer,
        agents,
        start,
    )
    .await?;
    let outcome = match coordinated.state.workflow {
        WorkflowState::Succeeded => RunOutcome::Succeeded,
        WorkflowState::Failed {
            primary_issue,
            later_cancellation,
        } => RunOutcome::Failed {
            primary_issue,
            later_cancellation,
        },
        WorkflowState::Cancelled { reason } => RunOutcome::Cancelled { reason },
        WorkflowState::Executing { .. } | WorkflowState::Finalizing { .. } => {
            return Err(CoordinationError::ReducerStateUnavailable);
        }
    };
    let (steps, recoveries) = coordinated
        .state
        .steps
        .into_iter()
        .map(|(step, runtime)| ((step.clone(), runtime.state), (step, runtime.recovery)))
        .unzip();
    let output_producers = coordinated.state.output_producers;
    let finalization_summary = coordinated.state.finalization_summary;
    let force_abort = coordinated.state.force_abort;
    let exports = coordinated
        .state
        .exports
        .ok_or(CoordinationError::ReducerStateUnavailable)?;
    Ok(WorkflowExecutionResult {
        outcome,
        steps,
        recoveries,
        output_producers,
        finalization_summary,
        force_abort,
        exports,
        provenance,
        content_digest,
    })
}

#[cfg(test)]
mod tests;
