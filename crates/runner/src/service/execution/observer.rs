use super::*;

#[derive(Clone)]
pub(super) struct RunnerExecutionObserver {
    assignment_id: String,
    attempt_id: String,
    transition_budget: usize,
    outbox: ObservationOutbox,
    post_stop_fence: PostStopFence,
    cancellation: CancellationSource,
    invocation_evidence: RunnerInvocationEvidence,
    state: Arc<Mutex<ObserverState>>,
}

#[derive(Clone, Default)]
pub(super) struct RunnerInvocationEvidence {
    pub(super) diagnostics: StepDiagnosticLog,
    pub(super) accounting: InvocationAccountingLog,
    pub(super) agent_steps: BTreeSet<String>,
    pub(super) recovery_agent_steps: BTreeSet<String>,
}

pub(super) struct ObserverState {
    transition_count: usize,
    last_sequence: u64,
    terminal_sequence: Option<u64>,
    terminal_state: Option<WorkflowState>,
    force_abort: Option<ForceAbortEvidence>,
    cancellation: Option<(CancellationReason, RunnerExecutionInstant)>,
    step_timings: BTreeMap<String, RunnerStepTiming>,
    active_invocations: BTreeMap<String, RunnerActiveInvocation>,
    settled_invocations: BTreeMap<u64, (String, RecoveryInvocationV1)>,
    fault: Option<ObserverFault>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ObserverFault {
    TransitionCapacityExceeded,
    SequenceExhausted,
    DuplicateTerminal,
    InvocationSettlementFailed,
    AmbiguousAgentInvocation,
    AgentInvocationTimingMissing,
    InvocationEvidenceInvalid,
    Outbox(OutboxFailure),
    DuplicateCancellation,
}

impl ObserverFault {
    pub(super) fn cause(self) -> &'static str {
        match self {
            Self::TransitionCapacityExceeded => "observer_transition_capacity_exceeded",
            Self::SequenceExhausted => "observer_sequence_exhausted",
            Self::DuplicateTerminal => "observer_duplicate_terminal",
            Self::InvocationSettlementFailed => "observer_invocation_settlement_failed",
            Self::AmbiguousAgentInvocation => "observer_ambiguous_agent_invocation",
            Self::AgentInvocationTimingMissing => "observer_agent_invocation_timing_missing",
            Self::InvocationEvidenceInvalid => "observer_invocation_evidence_invalid",
            Self::Outbox(OutboxFailure::Encoding) => "transition_observation_encoding_failed",
            Self::Outbox(OutboxFailure::Capacity) => "transition_observation_capacity_exceeded",
            Self::Outbox(OutboxFailure::Sequence) => "transition_observation_sequence_exhausted",
            Self::DuplicateCancellation => "observer_duplicate_cancellation",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct RunnerActiveInvocation {
    pub(super) id: ActionId,
    pub(super) role: ActiveStepInvocation,
    pub(super) started_at: RunnerExecutionInstant,
}

#[derive(Clone, Copy)]
pub(super) struct RunnerStepTiming {
    pub(super) started_at: RunnerExecutionInstant,
    finished_at: Option<RunnerExecutionInstant>,
}

impl RunnerExecutionObserver {
    pub(super) fn new(
        assignment_id: String,
        attempt_id: String,
        transition_budget: usize,
        outbox: ObservationOutbox,
        post_stop_fence: PostStopFence,
        cancellation: CancellationSource,
        invocation_evidence: RunnerInvocationEvidence,
    ) -> Self {
        Self {
            assignment_id,
            attempt_id,
            transition_budget,
            outbox,
            post_stop_fence,
            cancellation,
            invocation_evidence,
            state: Arc::new(Mutex::new(ObserverState {
                transition_count: 0,
                last_sequence: 0,
                terminal_sequence: None,
                terminal_state: None,
                force_abort: None,
                cancellation: None,
                step_timings: BTreeMap::new(),
                active_invocations: BTreeMap::new(),
                settled_invocations: BTreeMap::new(),
                fault: None,
            })),
        }
    }

    pub(super) fn last_sequence(&self) -> u64 {
        self.lock().last_sequence
    }

    pub(super) fn terminal_sequence(&self) -> Option<u64> {
        self.lock().terminal_sequence
    }

    pub(super) fn terminal_state(&self) -> Option<WorkflowState> {
        self.lock().terminal_state.clone()
    }

    pub(super) fn force_abort(&self) -> Option<ForceAbortEvidence> {
        self.lock().force_abort
    }

    pub(super) fn fault(&self) -> Option<ObserverFault> {
        self.lock().fault
    }

    pub(super) fn cancellation(&self) -> Option<(CancellationReason, RunnerExecutionInstant)> {
        self.lock().cancellation
    }

    pub(super) fn invocations_for_step(&self, step: &str) -> Vec<RecoveryInvocationV1> {
        self.lock()
            .settled_invocations
            .values()
            .filter(|(settled_step, _)| settled_step == step)
            .map(|(_, invocation)| invocation.clone())
            .collect()
    }

    pub(super) fn step_timing(&self, step: &str) -> Option<WorkflowStepTiming> {
        let timing = *self.lock().step_timings.get(step)?;
        let finished_at = timing.finished_at?;
        Some(WorkflowStepTiming {
            started_at: timing.started_at.utc,
            duration: finished_at
                .monotonic
                .saturating_duration_since(timing.started_at.monotonic),
        })
    }

    pub(super) fn invocation_evidence(
        &self,
        step: &str,
        invocation: RunnerActiveInvocation,
        finished_at: RunnerExecutionInstant,
        cancelled: bool,
    ) -> Option<RecoveryInvocationV1> {
        let diagnostic = self
            .invocation_evidence
            .diagnostics
            .get_invocation(step, invocation.id);
        self.invocation_evidence_with_diagnostic(
            step,
            invocation,
            finished_at,
            cancelled,
            diagnostic,
        )
    }

    pub(super) fn invocation_evidence_with_diagnostic(
        &self,
        step: &str,
        invocation: RunnerActiveInvocation,
        finished_at: RunnerExecutionInstant,
        cancelled: bool,
        diagnostic: Option<StepDiagnostic>,
    ) -> Option<RecoveryInvocationV1> {
        let usage = self
            .invocation_evidence
            .accounting
            .usage(invocation.id)
            .unwrap_or_default();
        let native = self
            .invocation_evidence
            .accounting
            .native_session(invocation.id);
        let configured_agent = match invocation.role {
            ActiveStepInvocation::Target { .. } => {
                self.invocation_evidence.agent_steps.contains(step)
            }
            ActiveStepInvocation::RecoveryHandler { .. } => {
                self.invocation_evidence.recovery_agent_steps.contains(step)
            }
        };
        let diagnostics = diagnostic
            .and_then(|diagnostic| command_output_v1(&diagnostic).ok())
            .map(|output| {
                let (stdout_kind, stderr_kind) = if configured_agent || native.is_some() {
                    (
                        RecoveryDiagnosticKindV1::AgentHarnessStdout,
                        RecoveryDiagnosticKindV1::AgentHarnessStderr,
                    )
                } else {
                    (
                        RecoveryDiagnosticKindV1::CommandStdout,
                        RecoveryDiagnosticKindV1::CommandStderr,
                    )
                };
                vec![
                    RecoveryInvocationDiagnosticV1 {
                        kind: stdout_kind,
                        reference: format!(
                            "runner/invocations/{}/stdout",
                            invocation.id.transition_sequence.get()
                        ),
                        stream: output.stdout,
                    },
                    RecoveryInvocationDiagnosticV1 {
                        kind: stderr_kind,
                        reference: format!(
                            "runner/invocations/{}/stderr",
                            invocation.id.transition_sequence.get()
                        ),
                        stream: output.stderr,
                    },
                ]
            })
            .unwrap_or_default();
        let diagnostic_reference =
            native.map(|session| format!("runner/native-sessions/{}", session.diagnostic_identity));
        let (role, target_execution, recovery_round) = match invocation.role {
            ActiveStepInvocation::Target { execution_number } => (
                RecoveryInvocationRoleV1::Target,
                Some(execution_number.get()),
                None,
            ),
            ActiveStepInvocation::RecoveryHandler { round } => (
                RecoveryInvocationRoleV1::RecoveryHandler,
                None,
                Some(round.get()),
            ),
        };
        Some(RecoveryInvocationV1 {
            invocation_id: invocation.id.transition_sequence.get(),
            role,
            target_execution,
            recovery_round,
            state: if cancelled {
                RecoveryInvocationStateV1::Cancelled
            } else {
                RecoveryInvocationStateV1::Settled
            },
            started_at: format_utc(invocation.started_at.utc),
            finished_at: format_utc(finished_at.utc),
            duration_milliseconds: u64::try_from(
                finished_at
                    .monotonic
                    .saturating_duration_since(invocation.started_at.monotonic)
                    .as_millis(),
            )
            .ok()?,
            usage: RecoveryInvocationUsageV1 {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
            },
            diagnostics,
            diagnostic_reference,
        })
    }

    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, ObserverState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(super) fn settle_runner_invocation(
    observer: &RunnerExecutionObserver,
    state: &mut ObserverState,
    step: &str,
    invocation: RunnerActiveInvocation,
    finished_at: RunnerExecutionInstant,
    cancelled: bool,
    evidence: &mut Option<RecoveryInvocationV1>,
) -> bool {
    let Some(settled) = observer.invocation_evidence(step, invocation, finished_at, cancelled)
    else {
        return false;
    };
    if let Some((known_step, known)) = state.settled_invocations.get(&settled.invocation_id) {
        return known_step == step && known == &settled;
    }
    if evidence.is_some() {
        return false;
    }
    *evidence = Some(settled.clone());
    state
        .settled_invocations
        .insert(settled.invocation_id, (step.to_owned(), settled));
    true
}

impl ExecutionObserver<RunnerExecutionInstant> for RunnerExecutionObserver {
    fn observe(
        &self,
        observation: ExecutionObservation<RunnerExecutionInstant>,
    ) -> impl Future<Output = ()> + Send {
        let observer = self.clone();
        async move {
            let ExecutionObservation::Transition(transition) = observation else {
                // Invocation-level command streams and agent transcript activity remain local.
                return;
            };
            let observed_at = RunnerExecutionClock.now();
            let phase_cancellation = match &transition.event {
                TransitionEvent::Workflow { to, .. }
                    if matches!(to.as_ref(), WorkflowState::Finalizing { .. }) =>
                {
                    observer
                        .cancellation
                        .cancellation_reason()
                        .filter(|reason| {
                            matches!(
                                reason,
                                CancellationReason::RunnerShutdown
                                    | CancellationReason::ExecutionLeaseExpired
                            )
                        })
                }
                _ => None,
            };
            let fence = observer.post_stop_fence.lock();
            if *fence && !is_lease_loss_terminal_transition(&transition) {
                drop(fence);
                if let Some(reason) = phase_cancellation {
                    observer.cancellation.request_cancellation(reason);
                }
                return;
            }
            let mut state = observer.lock();
            if state.fault.is_some() {
                return;
            }
            if state.transition_count == observer.transition_budget {
                state.fault = Some(ObserverFault::TransitionCapacityExceeded);
                return;
            }
            let Some(sequence) = state.last_sequence.checked_add(1) else {
                state.fault = Some(ObserverFault::SequenceExhausted);
                return;
            };
            let cancellation = match &transition.event {
                TransitionEvent::CancellationAccepted {
                    reason, deadline, ..
                } => Some((*reason, *deadline)),
                _ => None,
            };
            let terminal = match &transition.event {
                TransitionEvent::Workflow { to, .. }
                    if matches!(
                        to.as_ref(),
                        WorkflowState::Succeeded
                            | WorkflowState::Failed { .. }
                            | WorkflowState::Cancelled { .. }
                    ) =>
                {
                    Some(to.as_ref().clone())
                }
                _ => None,
            };
            if terminal.is_some() && state.terminal_sequence.is_some() {
                state.fault = Some(ObserverFault::DuplicateTerminal);
                return;
            }
            let mut invocation_evidence = None;
            if let TransitionEvent::Step { step, to, .. } = &transition.event {
                let cancelled = *to == StepStateKind::Cancelled;
                if let Some(ObservedStepTransition::Recovery {
                    active,
                    active_invocation_id,
                    settled_invocation,
                    ..
                }) = &transition.step
                {
                    if let Some(previous) = state.active_invocations.get(step).copied()
                        && previous.id != *active_invocation_id
                    {
                        state.active_invocations.remove(step);
                        if !settle_runner_invocation(
                            &observer,
                            &mut state,
                            step,
                            previous,
                            observed_at,
                            false,
                            &mut invocation_evidence,
                        ) {
                            state.fault = Some(ObserverFault::InvocationSettlementFailed);
                            return;
                        }
                    }
                    if let Some((id, role)) = settled_invocation
                        && !state
                            .settled_invocations
                            .contains_key(&id.transition_sequence.get())
                    {
                        let started_at = state
                            .step_timings
                            .get(step)
                            .map_or(observed_at, |timing| timing.started_at);
                        if !settle_runner_invocation(
                            &observer,
                            &mut state,
                            step,
                            RunnerActiveInvocation {
                                id: *id,
                                role: *role,
                                started_at,
                            },
                            observed_at,
                            false,
                            &mut invocation_evidence,
                        ) {
                            state.fault = Some(ObserverFault::InvocationSettlementFailed);
                            return;
                        }
                    }
                    state.active_invocations.entry(step.clone()).or_insert(
                        RunnerActiveInvocation {
                            id: *active_invocation_id,
                            role: *active,
                            started_at: observed_at,
                        },
                    );
                }
                if matches!(
                    to,
                    StepStateKind::Succeeded | StepStateKind::Failed | StepStateKind::Cancelled
                ) && let Some(active) = state.active_invocations.remove(step)
                    && !settle_runner_invocation(
                        &observer,
                        &mut state,
                        step,
                        active,
                        observed_at,
                        cancelled,
                        &mut invocation_evidence,
                    )
                {
                    state.fault = Some(ObserverFault::InvocationSettlementFailed);
                    return;
                }
            }
            if let TransitionEvent::Step { step, to, .. } = &transition.event
                && matches!(
                    to,
                    StepStateKind::Succeeded | StepStateKind::Failed | StepStateKind::Cancelled
                )
                && observer.invocation_evidence.agent_steps.contains(step)
                && invocation_evidence.is_none()
                && !state.settled_invocations.values().any(|(id, _)| id == step)
            {
                let ids = observer
                    .invocation_evidence
                    .diagnostics
                    .invocation_ids(step);
                if ids.len() > 1 {
                    state.fault = Some(ObserverFault::AmbiguousAgentInvocation);
                    return;
                }
                if let Some(id) = ids.first() {
                    let Some(started_at) =
                        state.step_timings.get(step).map(|timing| timing.started_at)
                    else {
                        state.fault = Some(ObserverFault::AgentInvocationTimingMissing);
                        return;
                    };
                    let active = RunnerActiveInvocation {
                        id: *id,
                        role: ActiveStepInvocation::Target {
                            execution_number: TargetExecutionNumber::fixture(1),
                        },
                        started_at,
                    };
                    let Some(evidence) = observer.invocation_evidence(
                        step,
                        active,
                        observed_at,
                        *to == StepStateKind::Cancelled,
                    ) else {
                        state.fault = Some(ObserverFault::InvocationEvidenceInvalid);
                        return;
                    };
                    state
                        .settled_invocations
                        .insert(evidence.invocation_id, (step.clone(), evidence.clone()));
                    invocation_evidence = Some(evidence);
                }
            }
            if let TransitionEvent::ForceAbortAccepted { reason, phase, .. } = &transition.event {
                state.force_abort = Some(ForceAbortEvidence {
                    reason: *reason,
                    phase: *phase,
                });
            }
            let workflow_event =
                workflow_event(&transition, invocation_evidence.as_ref(), state.force_abort);
            let diagnostic = (workflow_event["to"] == "failed")
                .then(|| workflow_event.get("stepId").and_then(Value::as_str))
                .flatten()
                .zip(workflow_event.get("detail"))
                .and_then(|(step, detail)| {
                    git_capture_diagnostic(step, detail, &observer.invocation_evidence.diagnostics)
                });
            let enqueued = observer.outbox.enqueue(AssignmentObservation::Execution {
                assignment_id: observer.assignment_id.clone(),
                attempt_id: observer.attempt_id.clone(),
                report: ExecutionReport::Transition {
                    execution_event_sequence: sequence,
                    workflow_event,
                    diagnostic,
                },
            });
            if let Err(error) = enqueued {
                state.fault = Some(ObserverFault::Outbox(error));
                return;
            }
            match &transition.event {
                TransitionEvent::Step {
                    step,
                    to: StepStateKind::Starting,
                    ..
                } => {
                    state
                        .step_timings
                        .entry(step.clone())
                        .or_insert(RunnerStepTiming {
                            started_at: observed_at,
                            finished_at: None,
                        });
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
                    if let Some(timing) = state.step_timings.get_mut(step) {
                        timing.finished_at.get_or_insert(observed_at);
                    }
                }
                TransitionEvent::Step { .. }
                | TransitionEvent::Workflow { .. }
                | TransitionEvent::CancellationAccepted { .. }
                | TransitionEvent::FinalizationCancellationAccepted { .. }
                | TransitionEvent::ForceAbortAccepted { .. } => {}
            }
            state.transition_count += 1;
            state.last_sequence = sequence;
            if let Some(terminal) = terminal {
                state.terminal_sequence = Some(sequence);
                state.terminal_state = Some(terminal.map_deadline(|_| ()));
            }
            if let Some(cancellation) = cancellation
                && state.cancellation.replace(cancellation).is_some()
            {
                state.fault = Some(ObserverFault::DuplicateCancellation);
            }
            drop(state);
            if let Some(reason) = phase_cancellation {
                observer.cancellation.request_cancellation(reason);
            }
        }
    }
}
