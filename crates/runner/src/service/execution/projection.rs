use super::*;

pub(super) fn is_lease_loss_terminal_transition(
    transition: &TransitionObservation<RunnerExecutionInstant>,
) -> bool {
    matches!(
        &transition.event,
        TransitionEvent::Workflow { to, .. }
            if matches!(
                to.as_ref(),
                WorkflowState::Cancelled {
                    reason: CancellationReason::ExecutionLeaseExpired,
                }
            )
    )
}

pub(super) fn terminal_recovery_summaries(
    recoveries: &BTreeMap<String, Option<StepRecoveryState<StepFailureCause>>>,
) -> Option<Value> {
    let summaries = recoveries
        .iter()
        .filter_map(|(step, recovery)| {
            step_recovery_summary_v1(recovery.as_ref())
                .ok()
                .flatten()
                .and_then(|summary| serde_json::to_value(summary).ok())
                .map(|summary| (step.clone(), summary))
        })
        .collect::<serde_json::Map<_, _>>();
    (!summaries.is_empty()).then_some(Value::Object(summaries))
}

pub(super) fn terminal_outcome(
    outcome: &str,
    primary_issue: Option<Value>,
    reason: Option<&str>,
    finalization: Option<Value>,
    force_abort: Option<ForceAbortEvidence>,
    recovery_summaries: Option<Value>,
) -> Value {
    let mut object = serde_json::Map::from_iter([
        ("outcome".to_owned(), json!(outcome)),
        ("forceAbort".to_owned(), json!(force_abort)),
    ]);
    if let Some(primary_issue) = primary_issue {
        object.insert("primaryIssue".to_owned(), primary_issue);
    }
    if let Some(reason) = reason {
        object.insert("reason".to_owned(), json!(reason));
    }
    if let Some(finalization) = finalization {
        object.insert("finalization".to_owned(), finalization);
    }
    if let Some(recovery_summaries) = recovery_summaries {
        object.insert("recoverySummaries".to_owned(), recovery_summaries);
    }
    Value::Object(object)
}

pub(super) fn finalization_summary(summary: &FinalizationSummary<RunnerExecutionInstant>) -> Value {
    let finalizers = summary
        .finalizers
        .iter()
        .map(finalizer_result)
        .collect::<Vec<_>>();
    let issues = summary
        .finalizers
        .iter()
        .filter(|result| {
            matches!(
                result.disposition,
                StepState::Failed { .. } | StepState::Blocked { .. }
            )
        })
        .map(|result| {
            json!({
                "node": { "id": result.finalizer, "role": "finalizer" },
                "impact": result.failure_policy,
            })
        })
        .collect::<Vec<_>>();
    let mut object = serde_json::Map::from_iter([
        ("trigger".to_owned(), json!(summary.trigger.as_str())),
        ("finalizers".to_owned(), Value::Array(finalizers)),
        ("issues".to_owned(), Value::Array(issues)),
        ("forceAbort".to_owned(), json!(summary.force_abort)),
    ]);
    if let Some(cancellation) = &summary.cancellation {
        let mut value = serde_json::Map::from_iter([(
            "reason".to_owned(),
            json!(cancellation_reason(cancellation.reason)),
        )]);
        if let Some(deadline) = cancellation.deadline {
            value.insert(
                "forceStopDeadline".to_owned(),
                json!(format_utc(deadline.utc)),
            );
        }
        object.insert("cancellation".to_owned(), Value::Object(value));
    }
    Value::Object(object)
}

pub(super) fn finalizer_result(result: &FinalizerResult) -> Value {
    let mut object = serde_json::Map::from_iter([
        ("id".to_owned(), json!(result.finalizer)),
        ("role".to_owned(), json!("finalizer")),
        ("failurePolicy".to_owned(), json!(result.failure_policy)),
    ]);
    match &result.disposition {
        StepState::Succeeded { .. } => {
            object.insert("state".to_owned(), json!("succeeded"));
        }
        StepState::Failed { detail } => {
            object.insert("state".to_owned(), json!("failed"));
            object.insert("detail".to_owned(), json!(detail));
        }
        StepState::Blocked { detail } => {
            object.insert("state".to_owned(), json!("blocked"));
            object.insert("detail".to_owned(), json!(detail));
        }
        StepState::Skipped { detail } => {
            object.insert("state".to_owned(), json!("skipped"));
            object.insert("detail".to_owned(), json!(detail));
        }
        StepState::NotRun { detail } => {
            object.insert("state".to_owned(), json!("not_run"));
            object.insert("detail".to_owned(), json!(detail));
        }
        StepState::Cancelled { detail } => {
            object.insert("state".to_owned(), json!("cancelled"));
            object.insert("detail".to_owned(), json!(detail));
        }
        StepState::Pending
        | StepState::Inherited { .. }
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => {
            object.insert("state".to_owned(), json!("incomplete"));
        }
    }
    Value::Object(object)
}

pub(super) fn distributed_invocation_evidence(invocation: &RecoveryInvocationV1) -> Option<Value> {
    let mut value = serde_json::to_value(invocation).ok()?;
    let object = value.as_object_mut()?;
    let Some(diagnostics) = object.get_mut("diagnostics") else {
        return Some(value);
    };
    for diagnostic in diagnostics.as_array_mut()? {
        let stream = diagnostic
            .as_object_mut()?
            .get_mut("stream")?
            .as_object_mut()?;
        let encoded = stream.remove("data")?.as_str()?.to_owned();
        stream.remove("encoding")?;
        let bytes = BASE64_STANDARD.decode(encoded).ok()?;
        let digest = digest(&SHA256, &bytes);
        let value = digest
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        stream.insert(
            "digest".to_owned(),
            json!({ "algorithm": "sha256", "value": value }),
        );
    }
    Some(value)
}

pub(super) fn workflow_event(
    transition: &TransitionObservation<RunnerExecutionInstant>,
    invocation_evidence: Option<&RecoveryInvocationV1>,
    force_abort: Option<ForceAbortEvidence>,
) -> Value {
    let mut event = match &transition.event {
        TransitionEvent::Step {
            sequence,
            step,
            role,
            failure_policy,
            from,
            to,
        } => {
            let mut event = serde_json::Map::from_iter([
                ("eventVersion".to_owned(), json!(1)),
                ("eventType".to_owned(), json!("step_state_changed")),
                ("transitionSequence".to_owned(), json!(sequence.get())),
                ("stepId".to_owned(), json!(step)),
                ("role".to_owned(), json!(node_role(*role))),
                ("failurePolicy".to_owned(), json!(failure_policy)),
                ("from".to_owned(), json!(step_state_name(*from))),
                ("to".to_owned(), json!(step_state_name(*to))),
            ]);
            if let Some(observed) = &transition.step {
                match observed {
                    ObservedStepTransition::Recovery {
                        active,
                        active_invocation_id,
                        configured_rounds,
                        handler_kind,
                        handler_state,
                        decision,
                        ..
                    } => {
                        let mut progress = serde_json::Map::from_iter([
                            ("configuredRetries".to_owned(), json!(configured_rounds)),
                            (
                                "activeInvocationId".to_owned(),
                                json!(active_invocation_id.transition_sequence.get()),
                            ),
                        ]);
                        match active {
                            ActiveStepInvocation::Target { execution_number } => {
                                progress.insert("activeRole".to_owned(), json!("target"));
                                progress.insert(
                                    "targetExecution".to_owned(),
                                    json!(execution_number.get()),
                                );
                            }
                            ActiveStepInvocation::RecoveryHandler { round } => {
                                progress.insert("activeRole".to_owned(), json!("recovery_handler"));
                                progress.insert("recoveryRound".to_owned(), json!(round.get()));
                            }
                        }
                        if let Some(kind) = handler_kind {
                            progress.insert(
                                "handlerKind".to_owned(),
                                json!(match kind {
                                    RecoveryHandlerKind::Command => "cmd",
                                    RecoveryHandlerKind::Agent => "agent",
                                }),
                            );
                        }
                        if let Some(handler_state) = handler_state {
                            progress.insert(
                                "handlerState".to_owned(),
                                json!(match handler_state {
                                    RecoveryHandlerActivity::Starting => "starting",
                                    RecoveryHandlerActivity::Running => "running",
                                }),
                            );
                        }
                        if let Some(decision) = decision {
                            progress.insert(
                                "decision".to_owned(),
                                json!(match decision {
                                    RecoveryDecisionKind::Recheck => "recheck",
                                    RecoveryDecisionKind::GaveUp => "gave_up",
                                }),
                            );
                        }
                        event.insert("recoveryProgress".to_owned(), Value::Object(progress));
                    }
                    ObservedStepTransition::OutputsCommitted { .. } => {}
                    ObservedStepTransition::Failed { detail } => {
                        event.insert("detail".to_owned(), json!(detail));
                    }
                    ObservedStepTransition::Blocked { detail } => {
                        event.insert("detail".to_owned(), json!(detail));
                    }
                    ObservedStepTransition::Skipped { detail } => {
                        event.insert("detail".to_owned(), json!(detail));
                    }
                    ObservedStepTransition::NotRun { detail } => {
                        event.insert("detail".to_owned(), json!(detail));
                    }
                    ObservedStepTransition::Cancelling { detail }
                    | ObservedStepTransition::Cancelled { detail } => {
                        event.insert("detail".to_owned(), json!(detail));
                    }
                }
            }
            Value::Object(event)
        }
        TransitionEvent::Workflow { sequence, from, to } => json!({
            "eventVersion": 1,
            "eventType": "workflow_state_changed",
            "transitionSequence": sequence.get(),
            "from": workflow_state(from),
            "to": workflow_state(to),
        }),
        TransitionEvent::CancellationAccepted {
            sequence,
            reason,
            deadline,
        } => json!({
            "eventVersion": 1,
            "eventType": "cancellation_accepted",
            "transitionSequence": sequence.get(),
            "reason": cancellation_reason(*reason),
            "deadline": format_utc(deadline.utc),
        }),
        TransitionEvent::FinalizationCancellationAccepted {
            sequence,
            reason,
            deadline,
        } => json!({
            "eventVersion": 1,
            "eventType": "finalization_cancellation_accepted",
            "transitionSequence": sequence.get(),
            "reason": cancellation_reason(*reason),
            "deadline": format_utc(deadline.utc),
        }),
        TransitionEvent::ForceAbortAccepted {
            sequence,
            reason,
            phase,
        } => json!({
            "eventVersion": 1,
            "eventType": "force_abort_accepted",
            "transitionSequence": sequence.get(),
            "reason": cancellation_reason(*reason),
            "phase": phase.as_str(),
        }),
    };
    if matches!(
        &transition.event,
        TransitionEvent::Workflow { to, .. }
            if matches!(
                to.as_ref(),
                WorkflowState::Succeeded
                    | WorkflowState::Failed { .. }
                    | WorkflowState::Cancelled { .. }
            )
    ) && let Value::Object(object) = &mut event
        && let Some(Value::Object(to)) = object.get_mut("to")
    {
        to.insert("forceAbort".to_owned(), json!(force_abort));
    }
    if let Some(invocation_evidence) = invocation_evidence
        && let Value::Object(object) = &mut event
    {
        object.insert(
            "invocationEvidence".to_owned(),
            distributed_invocation_evidence(invocation_evidence).unwrap_or(Value::Null),
        );
    }
    event
}

pub(super) fn workflow_state(state: &WorkflowState<RunnerExecutionInstant>) -> Value {
    match state {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => json!({ "state": "executing", "gate": "open" }),
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { primary_issue },
        } => json!({
            "state": "executing",
            "gate": "failure_stopped",
            "primaryIssue": workflow_issue(primary_issue),
        }),
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_issue: None,
                },
        } => json!({
            "state": "executing",
            "gate": "cancelling",
            "reason": cancellation_reason(*reason),
        }),
        WorkflowState::Executing {
            gate:
                SchedulingGate::Cancelling {
                    reason,
                    prior_issue: Some(prior_issue),
                },
        } => json!({
            "state": "executing",
            "gate": "cancelling",
            "reason": cancellation_reason(*reason),
            "priorIssue": workflow_issue(prior_issue),
        }),
        WorkflowState::Finalizing {
            trigger,
            gate,
            primary_issue,
        } => {
            let mut object = serde_json::Map::from_iter([
                ("state".to_owned(), json!("finalizing")),
                ("trigger".to_owned(), json!(trigger.as_str())),
            ]);
            match gate {
                FinalizationGate::Open => {
                    object.insert("gate".to_owned(), json!("open"));
                }
                FinalizationGate::Cancelling {
                    reason,
                    deadline,
                    force_abort,
                } => {
                    object.insert("gate".to_owned(), json!("cancelling"));
                    object.insert("reason".to_owned(), json!(cancellation_reason(*reason)));
                    object.insert("forceAbort".to_owned(), json!(force_abort));
                    if let Some(deadline) = deadline {
                        object.insert(
                            "forceStopDeadline".to_owned(),
                            json!(format_utc(deadline.utc)),
                        );
                    }
                }
            }
            if let Some(primary_issue) = primary_issue {
                object.insert("primaryIssue".to_owned(), workflow_issue(primary_issue));
            }
            Value::Object(object)
        }
        WorkflowState::Succeeded => json!({ "state": "succeeded" }),
        WorkflowState::Failed {
            primary_issue,
            later_cancellation: None,
        } => json!({
            "state": "failed",
            "primaryIssue": workflow_issue(primary_issue),
        }),
        WorkflowState::Failed {
            primary_issue,
            later_cancellation: Some(later_cancellation),
        } => json!({
            "state": "failed",
            "primaryIssue": workflow_issue(primary_issue),
            "laterCancellation": cancellation_reason(*later_cancellation),
        }),
        WorkflowState::Cancelled { reason } => json!({
            "state": "cancelled",
            "reason": cancellation_reason(*reason),
        }),
    }
}

pub(super) fn workflow_issue(issue: &PrimaryIssue) -> Value {
    json!(issue)
}

pub(super) fn git_capture_diagnostic(
    step: &str,
    detail: &Value,
    diagnostics: &StepDiagnosticLog,
) -> Option<Value> {
    (detail.get("phase")?.as_str()? == "output_capture"
        && (detail.get("code")?.as_str()?.starts_with("git_")
            || detail.get("code")?.as_str()? == "output_staging_unavailable"))
        .then(|| diagnostics.git_capture_failure(step))
        .flatten()
}

// The portable result may reject inconsistent step metadata. Keep the original
// failure and its bounded command diagnostic in the runner-private retained
// workspace so that a second failure cannot erase the first one.
pub(super) fn retain_result_publication_failure(
    private_root: &Path,
    run_outcome: &RunOutcome,
    steps: &[WorkflowRunStep],
    finalization: Option<&WorkflowRunFinalization>,
    publication: (&str, &str, Option<&str>),
) -> std::io::Result<()> {
    let (outcome, primary_issue) = match run_outcome {
        RunOutcome::Succeeded => ("succeeded", None),
        RunOutcome::Failed { primary_issue, .. } => ("failed", Some(workflow_issue(primary_issue))),
        RunOutcome::Cancelled { .. } => ("cancelled", None),
    };
    let failed_node = primary_issue.as_ref().and_then(|issue| issue.get("node"));
    let step = failed_node.and_then(|node| {
        let id = node.get("id")?.as_str()?;
        let role = node.get("role")?.as_str()?;
        steps
            .iter()
            .chain(finalization.iter().flat_map(|summary| &summary.finalizers))
            .find(|step| step.id == id && node_role(step.role) == role)
    });
    let command_output = step
        .and_then(|step| step.command_output.as_ref())
        .and_then(|diagnostic| command_output_v1(diagnostic).ok());
    let record = json!({
        "schemaVersion": 1,
        "outcome": outcome,
        "primaryIssue": primary_issue,
        "publicationFailure": {
            "phase": publication.0,
            "kind": publication.1,
            "invariant": publication.2,
        },
        "stepMetadata": steps.iter()
            .chain(finalization.iter().flat_map(|summary| &summary.finalizers))
            .map(|step| json!({
                "id": step.id,
                "role": node_role(step.role),
                "state": run_step_state_name(&step.state),
                "timingPresent": step.timing.is_some(),
                "commandOutputPresent": step.command_output.is_some(),
                "recoveryPresent": step.recovery.is_some(),
                "invocationCount": step.invocations.len(),
            }))
            .collect::<Vec<_>>(),
        "failedNode": step.map(|step| json!({
            "id": step.id,
            "role": node_role(step.role),
            "kind": match step.kind {
                WorkflowRunStepKind::Command => "cmd",
                WorkflowRunStepKind::Agent => "agent",
            },
            "timingPresent": step.timing.is_some(),
            "commandOutputPresent": step.command_output.is_some(),
            "recovery": step.recovery,
            "invocations": step.invocations,
        })),
        "failedCommandOutput": command_output,
    });
    let bytes = serde_json::to_vec(&record).map_err(std::io::Error::other)?;
    let path = private_root.join("result-publication-failure.json");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()
}

pub(super) fn run_step_state_name<Output>(state: &StepState<Output>) -> &'static str {
    match state {
        StepState::Pending => "pending",
        StepState::Starting => "starting",
        StepState::Running => "running",
        StepState::CapturingOutputs => "capturing_outputs",
        StepState::Recovering { .. } => "recovering",
        StepState::Cancelling { .. } => "cancelling",
        StepState::Succeeded { .. } => "succeeded",
        StepState::Inherited { .. } => "inherited",
        StepState::Failed { .. } => "failed",
        StepState::Blocked { .. } => "blocked",
        StepState::Skipped { .. } => "skipped",
        StepState::NotRun { .. } => "not_run",
        StepState::Cancelled { .. } => "cancelled",
    }
}

pub(super) fn node_role(role: WorkflowNodeRole) -> &'static str {
    match role {
        WorkflowNodeRole::Step => "step",
        WorkflowNodeRole::Finalizer => "finalizer",
    }
}

pub(super) fn outbox_cause(error: OutboxFailure, terminal: bool) -> &'static str {
    match (terminal, error) {
        (false, OutboxFailure::Encoding) => "start_observation_encoding_failed",
        (false, OutboxFailure::Capacity) => "start_observation_capacity_exceeded",
        (false, OutboxFailure::Sequence) => "start_observation_sequence_exhausted",
        (true, OutboxFailure::Encoding) => "terminal_observation_encoding_failed",
        (true, OutboxFailure::Capacity) => "terminal_observation_capacity_exceeded",
        (true, OutboxFailure::Sequence) => "terminal_observation_sequence_exhausted",
    }
}

pub(super) fn artifact_staging_cause(error: ArtifactStagingFailure) -> &'static str {
    match error {
        ArtifactStagingFailure::ExecutionRootUnavailable => "artifact_execution_root_unavailable",
        ArtifactStagingFailure::StagingParentUnavailable => "artifact_staging_parent_unavailable",
        ArtifactStagingFailure::StagingParentExposed => "artifact_staging_parent_exposed",
        ArtifactStagingFailure::IdentityUnavailable => "artifact_staging_identity_unavailable",
    }
}

pub(super) fn input_staging_cause(error: InputStagingFailure) -> &'static str {
    match error {
        InputStagingFailure::ExecutionRootUnavailable => "input_execution_root_unavailable",
        InputStagingFailure::StagingParentUnavailable => "input_staging_parent_unavailable",
        InputStagingFailure::StagingParentExposed => "input_staging_parent_exposed",
        InputStagingFailure::IdentityUnavailable => "input_staging_identity_unavailable",
    }
}

pub(super) fn agent_input_staging_cause(error: AgentInputStagingFailure) -> &'static str {
    match error {
        AgentInputStagingFailure::ExecutionRootUnavailable => {
            "agent_input_execution_root_unavailable"
        }
        AgentInputStagingFailure::StagingParentUnavailable => {
            "agent_input_staging_parent_unavailable"
        }
        AgentInputStagingFailure::StagingParentExposed => "agent_input_staging_parent_exposed",
        AgentInputStagingFailure::IdentityUnavailable => "agent_input_staging_identity_unavailable",
    }
}

pub(super) fn diagnostic_open_cause(error: &std::io::Error) -> &'static str {
    match error.kind() {
        std::io::ErrorKind::NotFound => "diagnostic_directory_missing",
        std::io::ErrorKind::PermissionDenied => "diagnostic_directory_permission_denied",
        _ => "diagnostic_directory_io_failure",
    }
}

pub(super) fn dispatcher_cause(error: &std::io::Error) -> &'static str {
    match error.kind() {
        std::io::ErrorKind::NotFound => "agent_dispatcher_resource_missing",
        std::io::ErrorKind::PermissionDenied => "agent_dispatcher_permission_denied",
        std::io::ErrorKind::OutOfMemory => "agent_dispatcher_out_of_memory",
        _ => "agent_dispatcher_io_failure",
    }
}

pub(super) fn coordination_cause(error: CoordinationError) -> &'static str {
    match error {
        CoordinationError::ArtifactStagingMismatch => "artifact_staging_mismatch",
        CoordinationError::InputStagingMismatch => "input_staging_mismatch",
        CoordinationError::AgentInputStagingMismatch => "agent_input_staging_mismatch",
        CoordinationError::AgentRuntimeUnavailable => "agent_runtime_unavailable",
        CoordinationError::CommitFailed => "coordination_commit_failed",
        CoordinationError::OccurrenceChannelClosed => "occurrence_channel_closed",
        CoordinationError::OccurrenceConflict => "occurrence_conflict",
        CoordinationError::OccurrenceIdentityCapacityExceeded => {
            "occurrence_identity_capacity_exceeded"
        }
        CoordinationError::OccurrenceOrdinalExhausted => "occurrence_ordinal_exhausted",
        CoordinationError::ReducerStateUnavailable => "reducer_state_unavailable",
        CoordinationError::TransitionCapacityExceeded => "transition_capacity_exceeded",
    }
}

pub(super) fn terminal_result_agrees(
    terminal: Option<&WorkflowState>,
    outcome: &RunOutcome,
) -> bool {
    match (terminal, outcome) {
        (Some(WorkflowState::Succeeded), RunOutcome::Succeeded) => true,
        (
            Some(WorkflowState::Failed {
                primary_issue: left_failure,
                later_cancellation: left_cancellation,
            }),
            RunOutcome::Failed {
                primary_issue: right_failure,
                later_cancellation: right_cancellation,
            },
        ) => left_failure == right_failure && left_cancellation == right_cancellation,
        (
            Some(WorkflowState::Cancelled { reason: left }),
            RunOutcome::Cancelled { reason: right },
        ) => left == right,
        _ => false,
    }
}

pub(super) fn step_state_name(state: StepStateKind) -> &'static str {
    state.as_str()
}

pub(super) fn cancellation_reason(reason: CancellationReason) -> &'static str {
    reason.as_str()
}

pub(super) fn format_utc(value: OffsetDateTime) -> String {
    value
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

// Runner Serve's transport-independent clock intentionally stays separate from the
// local command's publication-aware execution clock.
