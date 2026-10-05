use super::*;

impl ExecutionJob {
    pub(super) async fn run_workflow(
        &mut self,
        assignment_id: &str,
        attempt_id: &str,
        run_id: &str,
    ) -> ExecutionCompletion {
        let post_stop_fence =
            PostStopFence::with_workflow_git(Some(self.accepted.workflow_git.clone()));
        let cancellation = self
            .accepted
            .admitted
            .execution()
            .cancellation()
            .source()
            .clone();
        if let Err(completion) = self
            .ensure_execution_authority(&cancellation, &post_stop_fence, assignment_id, attempt_id)
            .await
        {
            return completion;
        }
        if self
            .enqueue(assignment_id, attempt_id, ExecutionReport::Started)
            .is_none()
        {
            return self.abort_retained(assignment_id, attempt_id, 0, "runner_internal_failure");
        }
        if let Err(completion) = self
            .wait_for_start_authority(&cancellation, &post_stop_fence, assignment_id, attempt_id)
            .await
        {
            return completion;
        }
        if !self.activate_workflow_git().await
            && cancellation.cancellation_reason() != Some(CancellationReason::RunnerShutdown)
        {
            return self.execution_environment_lost(
                assignment_id,
                attempt_id,
                "workflow_git_activation_failed",
                "workflow_git_activation",
            );
        }
        if let Err(completion) = self
            .ensure_execution_authority(&cancellation, &post_stop_fence, assignment_id, attempt_id)
            .await
        {
            return completion;
        }
        let initial_authority = self.authority_updates.borrow().clone();
        let initial_wait = match self
            .lease_clock
            .start_wait(initial_authority.renewal_request)
        {
            Ok(wait) => wait,
            Err(error) => {
                self.lease_clock_failure(error);
                return self
                    .fail_before_execution(
                        &cancellation,
                        &post_stop_fence,
                        assignment_id,
                        attempt_id,
                    )
                    .await;
            }
        };
        let artifacts = match self.stage_or_abort(
            ArtifactStaging::create(
                self.accepted.admitted.execution(),
                &self.accepted.root.private,
            ),
            artifact_staging_cause,
            "artifact_staging",
            assignment_id,
            attempt_id,
        ) {
            Ok(staging) => staging,
            Err(completion) => return *completion,
        };
        let inputs = match self.stage_or_abort(
            InputStaging::create(
                self.accepted.admitted.execution(),
                &self.accepted.root.private,
            ),
            input_staging_cause,
            "input_staging",
            assignment_id,
            attempt_id,
        ) {
            Ok(staging) => staging,
            Err(completion) => return *completion,
        };
        let recovery_agent_steps: BTreeSet<String> = self
            .accepted
            .admitted
            .workflow()
            .definition
            .recoveries
            .iter()
            .filter_map(|(step, recovery)| {
                recovery.as_ref().and_then(|recovery| {
                    matches!(
                        recovery.handler,
                        Some(ValidatedRecoveryHandler::Agent { .. })
                    )
                    .then(|| step.clone())
                })
            })
            .collect();
        let agent_staging = if self.accepted.admitted.agent_steps().is_empty() {
            None
        } else {
            match AgentInputStaging::create(
                self.accepted.admitted.execution(),
                &self.accepted.root.private,
            ) {
                Ok(staging) => Some(PreserveOnDrop::new(staging)),
                Err(error) => {
                    return self.execution_environment_lost(
                        assignment_id,
                        attempt_id,
                        agent_input_staging_cause(error),
                        "agent_input_staging",
                    );
                }
            }
        };

        let agent_diagnostic_sessions = if agent_staging.is_some() {
            let attempt_handle = match std::fs::File::open(&self.accepted.root.private) {
                Ok(handle) => OwnedFd::from(handle),
                Err(error) => {
                    return self.execution_environment_lost(
                        assignment_id,
                        attempt_id,
                        diagnostic_open_cause(&error),
                        "diagnostic_sessions",
                    );
                }
            };
            match AgentDiagnosticSessionStore::create_transient(
                &attempt_handle,
                &self.accepted.root.private,
            ) {
                Ok(sessions) => Some(sessions),
                Err(_) => {
                    return self.execution_environment_lost(
                        assignment_id,
                        attempt_id,
                        "diagnostic_session_creation_failed",
                        "diagnostic_sessions",
                    );
                }
            }
        } else {
            None
        };

        if let Err(completion) = self
            .ensure_execution_authority(&cancellation, &post_stop_fence, assignment_id, attempt_id)
            .await
        {
            return completion;
        }

        let started_at = RunnerExecutionClock.now();
        let diagnostics = StepDiagnosticLog::default();
        let accounting = InvocationAccountingLog::default();
        let observer = RunnerExecutionObserver::new(
            assignment_id.to_owned(),
            attempt_id.to_owned(),
            self.accepted.transition_budget,
            self.outbox.clone(),
            post_stop_fence.clone(),
            cancellation.clone(),
            RunnerInvocationEvidence {
                diagnostics: diagnostics.clone(),
                accounting: accounting.clone(),
                agent_steps: self
                    .accepted
                    .admitted
                    .agent_steps()
                    .keys()
                    .cloned()
                    .collect(),
                recovery_agent_steps,
            },
        );
        let process_guard_registry = self
            .accepted
            .process_guards
            .registry(self.accepted.guard_processes);
        let execution = if let (Some(agent_staging), Some(diagnostic_sessions)) =
            (&agent_staging, agent_diagnostic_sessions)
        {
            let maximum_log_bytes = self
                .accepted
                .admitted
                .execution()
                .limits()
                .maximum_step_log_bytes();
            let dispatcher = production_agent_dispatcher(
                diagnostics.clone(),
                maximum_log_bytes,
                RunnerExecutionClock,
                observer.clone(),
                &self.accepted.execution_version,
            );
            let dispatcher = match dispatcher {
                Ok(dispatcher) => dispatcher,
                Err(error) => {
                    let cause = dispatcher_cause(&error);
                    self.collapse("runner_internal_failure", cause, "harness_start");
                    return self.abort_retained(
                        assignment_id,
                        attempt_id,
                        observer.last_sequence(),
                        "runner_internal_failure",
                    );
                }
            };
            let agents = AgentExecution::enabled_with_accounting(
                WorkflowRunId::from(Arc::from(run_id)),
                (**agent_staging).clone(),
                diagnostic_sessions,
                dispatcher,
                accounting.clone(),
            );
            // Enabled and disabled execution carry distinct static dispatcher types;
            // keeping each engine call explicit avoids a dynamic adapter boundary.
            // jscpd:ignore-start
            let result = mark_engine_terminal(
                run_under_lease(
                    execute_workflow(
                        self.accepted.admitted.clone(),
                        &artifacts,
                        &inputs,
                        &diagnostics,
                        agents,
                        RunnerExecutionClock,
                        NoopCommitPort,
                        observer.clone(),
                        process_guard_registry,
                    ),
                    &cancellation,
                    &self.lease_clock,
                    self.authority_updates.clone(),
                    self.infrastructure_interruption.clone(),
                    Some((initial_authority.sequence, initial_wait)),
                    &self.causal_lease,
                    &self.outbox,
                    assignment_id,
                    attempt_id,
                    &post_stop_fence,
                    &self.accepted.process_guards,
                ),
                Arc::clone(&self.engine_terminal),
            )
            .await;
            // jscpd:ignore-end
            result
        } else {
            // See the enabled branch: the no-agent dispatcher is intentionally a different type.
            // jscpd:ignore-start
            let result = mark_engine_terminal(
                run_under_lease(
                    execute_workflow(
                        self.accepted.admitted.clone(),
                        &artifacts,
                        &inputs,
                        &diagnostics,
                        AgentExecution::disabled(),
                        RunnerExecutionClock,
                        NoopCommitPort,
                        observer.clone(),
                        process_guard_registry,
                    ),
                    &cancellation,
                    &self.lease_clock,
                    self.authority_updates.clone(),
                    self.infrastructure_interruption.clone(),
                    Some((initial_authority.sequence, initial_wait)),
                    &self.causal_lease,
                    &self.outbox,
                    assignment_id,
                    attempt_id,
                    &post_stop_fence,
                    &self.accepted.process_guards,
                ),
                Arc::clone(&self.engine_terminal),
            )
            .await;
            // jscpd:ignore-end
            result
        };
        self.accepted.workflow_git.disable();

        let (result, infrastructure_interruption) = match execution {
            LeaseExecution::Completed {
                output: Ok(result),
                infrastructure_interruption,
            } => (result, infrastructure_interruption),
            LeaseExecution::Completed {
                output: Err(error), ..
            } => {
                self.collapse(
                    "runner_internal_failure",
                    coordination_cause(error),
                    "harness_execution",
                );
                return self
                    .abort_unless_fenced(
                        &post_stop_fence,
                        assignment_id,
                        attempt_id,
                        observer.last_sequence(),
                        "runner_internal_failure",
                    )
                    .await;
            }
            LeaseExecution::ContainmentDeadline => {
                return ExecutionCompletion::fenced(None);
            }
            LeaseExecution::LeaseClockFailed { quiescent, error } => {
                self.lease_clock_failure(error);
                let report = quiescent.then(|| {
                    self.abort(
                        assignment_id,
                        attempt_id,
                        observer.last_sequence(),
                        "runner_internal_failure",
                    )
                });
                return ExecutionCompletion::lease_clock_failed(report.flatten());
            }
        };
        if let Some(fault) = observer.fault() {
            self.collapse(
                "runner_internal_failure",
                fault.cause(),
                "harness_execution",
            );
            return self
                .abort_unless_fenced(
                    &post_stop_fence,
                    assignment_id,
                    attempt_id,
                    observer.last_sequence(),
                    "runner_internal_failure",
                )
                .await;
        }
        let last_sequence = observer.last_sequence();
        let has_finalizers = !self
            .accepted
            .admitted
            .workflow()
            .definition
            .finalizers
            .is_empty();
        let inconsistency = if last_sequence == 0 {
            Some("terminal_sequence_missing")
        } else if observer.terminal_sequence() != Some(last_sequence) {
            Some("terminal_sequence_mismatch")
        } else if !terminal_result_agrees(observer.terminal_state().as_ref(), &result.outcome) {
            Some("terminal_outcome_mismatch")
        } else if observer.force_abort() != result.force_abort {
            Some("force_abort_mismatch")
        } else if has_finalizers != result.finalization_summary.is_some() {
            Some("finalization_shape_mismatch")
        } else {
            None
        };
        if let Some(cause) = inconsistency {
            self.collapse("engine_result_inconsistent", cause, "harness_execution");
            return self
                .abort_unless_fenced(
                    &post_stop_fence,
                    assignment_id,
                    attempt_id,
                    last_sequence,
                    "engine_result_inconsistent",
                )
                .await;
        }

        let finished_at = RunnerExecutionClock.now();
        let prepared = match self.runner_result(
            &diagnostics,
            result.clone(),
            &observer,
            started_at,
            finished_at,
        ) {
            Ok(run) => match prepare_cloud_workflow_result(
                &run,
                self.accepted.project_id().to_owned(),
                self.accepted.repository_connection_id().to_owned(),
                self.accepted.source_object_format().to_owned(),
                self.accepted.source_commit_oid().to_owned(),
                self.accepted.source_display_snapshot().map(|snapshot| {
                    CloudSourceDisplaySnapshotV1 {
                        organization_display_name: snapshot.organization_display_name.clone(),
                        project_name: snapshot.project_name.clone(),
                        repository: CloudSourceDisplayRepositoryV1 {
                            provider_kind: snapshot.repository.provider_kind.clone(),
                            full_name: snapshot.repository.full_name.clone(),
                        },
                    }
                }),
            ) {
                Ok(prepared) => Some(prepared),
                Err(error) => {
                    let (phase, kind, invariant) = error.diagnostic_codes();
                    let private_root = self.accepted.root.private.path().to_path_buf();
                    let retained = tokio::task::spawn_blocking(move || {
                        retain_result_publication_failure(
                            &private_root,
                            &run.outcome,
                            &run.steps,
                            run.finalization.as_ref(),
                            (phase, kind, invariant),
                        )
                    })
                    .await;
                    if !matches!(retained, Ok(Ok(()))) {
                        self.record_preparation_failure(
                            "diagnostic_retention",
                            "diagnostic_retention_failed",
                            Vec::new(),
                        );
                    }
                    let mut details = vec![
                        KeyValue::new(telemetry::attribute::ARTIFACT_PUBLICATION_PHASE, phase),
                        KeyValue::new(telemetry::attribute::ARTIFACT_PUBLICATION_KIND, kind),
                    ];
                    if let Some(invariant) = invariant {
                        details.push(KeyValue::new(
                            telemetry::attribute::ARTIFACT_RESULT_INVARIANT,
                            invariant,
                        ));
                    }
                    self.record_preparation_failure(
                        "result_publication",
                        "publication_failed",
                        details,
                    );
                    None
                }
            },
            Err(failure) => {
                self.collapse("runner_internal_failure", failure.code, "bundle_generation");
                let mut details = Vec::new();
                if let Some(node) = failure.node {
                    details.push(KeyValue::new(telemetry::attribute::ARTIFACT_NODE_ID, node));
                }
                self.record_preparation_failure("runner_result", failure.code, details);
                None
            }
        };
        let carriers_ready = match &prepared {
            Some(prepared) => match verify_prepared_carriers(&artifacts, prepared).await {
                Ok(()) => true,
                Err(failure) => {
                    self.record_preparation_failure(
                        "carrier_verification",
                        failure.code,
                        failure
                            .member_index
                            .map(|index| {
                                KeyValue::new(
                                    telemetry::attribute::ARTIFACT_MEMBER_INDEX,
                                    telemetry::integer(index),
                                )
                            })
                            .into_iter()
                            .collect(),
                    );
                    false
                }
            },
            None => false,
        };
        let delivery = match (prepared, carriers_ready) {
            (Some(prepared), true) => {
                self.deliver_artifacts(assignment_id, attempt_id, &artifacts, prepared)
                    .await
            }
            _ => Ok(internal_delivery_failure("preparation")),
        };
        let delivery = match delivery {
            Ok(delivery) => delivery,
            Err(error) => {
                self.lease_clock_failure(error);
                post_stop_fence.fence();
                self.accepted.process_guards.begin_forced_containment();
                return ExecutionCompletion::lease_clock_failed(self.abort(
                    assignment_id,
                    attempt_id,
                    last_sequence,
                    "runner_internal_failure",
                ));
            }
        };
        if delivery == ArtifactDeliveryOutcome::AuthorityLost {
            return ExecutionCompletion::fenced(None);
        }
        let infrastructure_interruption = infrastructure_interruption.or(match &result.outcome {
            RunOutcome::Cancelled {
                reason: CancellationReason::RunnerShutdown,
            } => Some(InfrastructureInterruption::RunnerShutdown),
            RunOutcome::Cancelled {
                reason: CancellationReason::ExecutionLeaseExpired,
            } => Some(InfrastructureInterruption::ExecutionLeaseExpired),
            RunOutcome::Succeeded | RunOutcome::Failed { .. } | RunOutcome::Cancelled { .. } => {
                None
            }
        });
        let workspace_disposition = match (&result.outcome, &delivery) {
            (RunOutcome::Succeeded, ArtifactDeliveryOutcome::Prepared { .. }) => {
                WorkspaceDisposition::Remove
            }
            (RunOutcome::Succeeded, _) => {
                WorkspaceDisposition::Retain(RetentionReason::ArtifactDeliveryFailed)
            }
            (RunOutcome::Failed { .. }, _) => WorkspaceDisposition::Retain(RetentionReason::Failed),
            (RunOutcome::Cancelled { .. }, _) if infrastructure_interruption.is_some() => {
                WorkspaceDisposition::Retain(RetentionReason::Interrupted)
            }
            (
                RunOutcome::Cancelled {
                    reason:
                        CancellationReason::RunnerShutdown
                        | CancellationReason::UserRequest
                        | CancellationReason::ForceAbort,
                },
                _,
            ) => WorkspaceDisposition::Retain(RetentionReason::Cancelled),
            (RunOutcome::Cancelled { .. }, _) => {
                WorkspaceDisposition::Retain(RetentionReason::Failed)
            }
        };
        let artifact_delivery = artifact_delivery_result(&delivery);

        let finalization = result
            .finalization_summary
            .as_ref()
            .map(finalization_summary);
        let recovery_summaries = terminal_recovery_summaries(&result.recoveries);
        let report = match result.outcome {
            RunOutcome::Succeeded => ExecutionReport::Finished {
                final_execution_event_sequence: last_sequence,
                outcome: terminal_outcome(
                    "succeeded",
                    None,
                    None,
                    finalization,
                    result.force_abort,
                    recovery_summaries.clone(),
                ),
                artifact_delivery,
            },
            RunOutcome::Failed { primary_issue, .. } => ExecutionReport::Finished {
                final_execution_event_sequence: last_sequence,
                outcome: terminal_outcome(
                    "failed",
                    Some(workflow_issue(&primary_issue)),
                    None,
                    finalization,
                    result.force_abort,
                    recovery_summaries,
                ),
                artifact_delivery,
            },
            RunOutcome::Cancelled { reason } => {
                let report = if let Some(interruption) = infrastructure_interruption {
                    ExecutionReport::Interrupted {
                        final_execution_event_sequence: last_sequence,
                        reason: interruption.report_reason().to_owned(),
                        terminal_outcome: terminal_outcome(
                            "cancelled",
                            None,
                            Some(reason.as_str()),
                            finalization,
                            result.force_abort,
                            recovery_summaries,
                        ),
                        artifact_delivery,
                    }
                } else if matches!(
                    reason,
                    CancellationReason::UserRequest | CancellationReason::ForceAbort
                ) {
                    ExecutionReport::Finished {
                        final_execution_event_sequence: last_sequence,
                        outcome: terminal_outcome(
                            "cancelled",
                            None,
                            Some(reason.as_str()),
                            finalization,
                            result.force_abort,
                            recovery_summaries,
                        ),
                        artifact_delivery,
                    }
                } else {
                    self.collapse(
                        "runner_internal_failure",
                        "unexpected_cancellation_reason",
                        "harness_execution",
                    );
                    return self
                        .abort_unless_fenced(
                            &post_stop_fence,
                            assignment_id,
                            attempt_id,
                            last_sequence,
                            "runner_internal_failure",
                        )
                        .await;
                };
                if matches!(
                    reason,
                    CancellationReason::UserRequest | CancellationReason::ForceAbort
                ) {
                    return ExecutionCompletion::containment_gated(report, workspace_disposition);
                }
                report
            }
        };
        ExecutionCompletion::containment_gated(report, workspace_disposition)
    }
}
