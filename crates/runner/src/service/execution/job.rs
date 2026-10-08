use super::*;

impl ExecutionJob {
    pub(in crate::service) fn new(
        accepted: AcceptedAssignment,
        outbox: ObservationOutbox,
        artifact_delivery: ArtifactDeliveryBroker,
        manager_events: tokio::sync::mpsc::UnboundedSender<ManagerEvent>,
        engine_terminal: Arc<AtomicBool>,
        run_event: RunEvent,
        authority: ExecutionAuthority,
    ) -> Self {
        Self {
            accepted,
            outbox,
            artifact_delivery,
            manager_events,
            engine_terminal,
            containment_clock: authority.lease_clock.clone(),
            lease_clock: authority.lease_clock,
            causal_lease: authority.causal_lease,
            authority_updates: authority.updates,
            start_authority: authority.start_authority,
            infrastructure_interruption: authority.infrastructure_interruption,
            workspace_release_reported: AtomicBool::new(false),
            run_event,
        }
    }

    #[cfg(test)]
    pub(in crate::service) fn register_stubborn_fixture(
        &self,
        kill_succeeds: bool,
    ) -> Arc<FixtureGuardProcessControl> {
        let control = Arc::new(FixtureGuardProcessControl {
            alive: AtomicBool::new(true),
            kill_succeeds,
            kill_count: std::sync::atomic::AtomicUsize::new(0),
        });
        let guards = &self.accepted.process_guards;
        guards.use_control(control.clone());
        guards
            .registry(true)
            .register("fixture", 1, &stubborn_guard_identity())
            .expect("register guard");
        control
    }

    #[cfg(test)]
    pub(in crate::service) fn use_containment_clock(&mut self, clock: LeaseClock) {
        self.containment_clock = clock;
    }

    #[cfg(test)]
    pub(in crate::service) fn finish_success_fixture(mut self, clock: LeaseClock) {
        self.containment_clock = clock;
        tokio::spawn(self.finish_execution(ExecutionCompletion::containment_gated(
            ExecutionReport::Finished {
                diagnostic: None,
                final_execution_event_sequence: 1,
                outcome: terminal_outcome("succeeded", None, None, None, None, None),
                artifact_delivery: json!({"outcome": "prepared", "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc"}),
            },
            WorkspaceDisposition::Remove,
        )));
    }

    // the guard store separately owns the injected containment observation.
    #[cfg(test)]
    pub(in crate::service) fn use_quiescence_fixture(
        &self,
        quiescent: Arc<std::sync::atomic::AtomicBool>,
    ) {
        self.accepted
            .process_guards
            .use_quiescence_fixture(quiescent);
    }

    pub(in crate::service) fn spawn(self) {
        let assignment_id = self.accepted.assignment_id().to_owned();
        let root = self.accepted.root.clone();
        let process_guards = self.accepted.process_guards.clone();
        let workflow_git = self.accepted.workflow_git.clone();
        let manager_events = self.manager_events.clone();
        let outbox = self.outbox.clone();
        let run_event = self.run_event.clone();
        tokio::spawn(async move {
            if std::panic::AssertUnwindSafe(self.run())
                .catch_unwind()
                .await
                .is_err()
            {
                run_event.result("aborted");
                run_event.set(KeyValue::new(
                    telemetry::attribute::FAILURE_CAUSE_TYPE,
                    "execution_panic",
                ));
                run_event.set(KeyValue::new(
                    telemetry::attribute::EXECUTOR_FAULT_REASON,
                    "runner_internal_failure",
                ));
                run_event.set(KeyValue::new(
                    telemetry::attribute::DIAGNOSTIC_STAGE,
                    "harness_execution",
                ));
                let guards = process_guards.clone();
                let _ =
                    tokio::task::spawn_blocking(move || guards.begin_forced_containment()).await;
                let check = process_guards.clone();
                let snapshot = tokio::task::spawn_blocking(move || check.quiescence_snapshot())
                    .await
                    .ok();
                let quiescence = if snapshot.as_ref().is_some_and(|(proven, _)| *proven) {
                    super::super::workspace::ProcessQuiescence::Proven
                } else {
                    super::super::workspace::ProcessQuiescence::Failed
                };
                let quiescence_failure = snapshot
                    .filter(|(proven, surviving)| !proven && !surviving.is_empty())
                    .map(|(_, surviving)| surviving);
                workflow_git.disable();
                let release = root
                    .release_workspace_pending(
                        quiescence,
                        WorkspaceDisposition::Retain(RetentionReason::Failed),
                    )
                    .wait_async()
                    .await;
                let _ = manager_events.send(ManagerEvent::WorkspaceReleased {
                    assignment_id: assignment_id.clone(),
                    result: release,
                });
                let _ = manager_events.send(ManagerEvent::Finished {
                    assignment_id,
                    final_observation_id: None,
                    final_delivery_deadline: None,
                    lease_clock_failed: false,
                    fenced: false,
                    retained_root: Some(Box::new(root)),
                    quiescence,
                    quiescence_failure,
                    workspace_disposition: WorkspaceDisposition::Retain(RetentionReason::Failed),
                });
                outbox.wake();
            }
        });
    }

    pub(super) async fn run(mut self) {
        let assignment_id = self.accepted.assignment_id().to_owned();
        let attempt_id = self.accepted.attempt_id().to_owned();
        let run_id = self.accepted.run_id().to_owned();
        // Keep the large admitted workflow future off the caller's stack. In
        // particular a retained continuation carries an inherited seed into
        // both the enabled and disabled agent execution branches.
        let completion = Box::pin(self.run_workflow(&assignment_id, &attempt_id, &run_id)).await;
        self.finish_execution(completion).await;
    }

    pub(super) async fn finish_execution(self, mut completion: ExecutionCompletion) {
        let assignment_id = self.accepted.assignment_id().to_owned();
        let attempt_id = self.accepted.attempt_id().to_owned();
        let guards = self.accepted.process_guards.clone();
        let mut snapshot = tokio::task::spawn_blocking({
            let guards = guards.clone();
            move || guards.quiescence_snapshot()
        })
        .await
        .ok();
        if !snapshot.as_ref().is_some_and(|(proven, _)| *proven)
            || matches!(
                completion.workspace_disposition,
                WorkspaceDisposition::Retain(_)
            )
        {
            // Kill while the authenticated leader is still observable. A TERM grace
            // could let that leader exit, making later group signals unsafe.
            let force = guards.clone();
            let _ = tokio::task::spawn_blocking(move || force.begin_forced_containment()).await;
            for _ in 0..80 {
                let check = guards.clone();
                if let Ok(observed) =
                    tokio::task::spawn_blocking(move || check.quiescence_snapshot()).await
                {
                    let proven = observed.0;
                    snapshot = Some(observed);
                    if proven {
                        break;
                    }
                }
                if !self.wait_for_containment_poll().await {
                    break;
                }
            }
            // The last wait can be the one during which the group exits.
            let check = guards.clone();
            if let Ok(observed) =
                tokio::task::spawn_blocking(move || check.quiescence_snapshot()).await
            {
                snapshot = Some(observed);
            }
        }
        let proven = snapshot.as_ref().is_some_and(|(proven, _)| *proven);
        let quiescence = if proven {
            super::super::workspace::ProcessQuiescence::Proven
        } else {
            super::super::workspace::ProcessQuiescence::Failed
        };
        let quiescence_failure = snapshot
            .filter(|(proven, surviving)| !proven && !surviving.is_empty())
            .map(|(_, surviving)| surviving);
        if let Some(mut report) = completion.deferred_containment_report.take() {
            // Cancellation is confirmed only after containment. A failed check
            // must not publish a cancelled outcome, even with failure details.
            let reportable_failure = quiescence_failure.is_some()
                && matches!(&report, ExecutionReport::Finished { outcome, .. }
                    if outcome["outcome"] != "cancelled");
            if reportable_failure
                && let Some(surviving) = &quiescence_failure
                && let ExecutionReport::Finished { outcome, .. } = &mut report
            {
                outcome["quiescenceFailure"] = json!({
                    "reason": "process_quiescence_failed", "survivingGuards": surviving
                });
            }
            if quiescence == super::super::workspace::ProcessQuiescence::Proven
                || reportable_failure
            {
                completion.final_observation_id = self.enqueue(&assignment_id, &attempt_id, report);
            }
        }
        if completion.final_observation_id.is_some() && !completion.lease_clock_failed {
            match self.terminal_report_deadline() {
                Ok(deadline) => completion.final_delivery_deadline = Some(deadline),
                Err(error) => {
                    self.lease_clock_failure(error);
                    completion.lease_clock_failed = true;
                }
            }
        }
        let _ = self
            .release_workspace(quiescence, completion.workspace_disposition)
            .await;
        let retained_root = self.accepted.root;
        let _ = self.manager_events.send(ManagerEvent::Finished {
            assignment_id,
            final_observation_id: completion.final_observation_id,
            final_delivery_deadline: completion.final_delivery_deadline,
            lease_clock_failed: completion.lease_clock_failed,
            fenced: completion.fenced,
            retained_root: Some(Box::new(retained_root)),
            quiescence,
            quiescence_failure,
            workspace_disposition: completion.workspace_disposition,
        });
        self.outbox.wake();
    }

    pub(super) async fn wait_for_containment_poll(&self) -> bool {
        let wait = self
            .containment_clock
            .now()
            .and_then(|now| now.checked_add(Duration::from_millis(25)))
            .and_then(|deadline| self.containment_clock.start_wait(deadline));
        match wait {
            Ok(wait) => wait.wait(&LeaseWaitCancellation::default()).await.is_ok(),
            Err(_) => false,
        }
    }

    pub(super) async fn activate_workflow_git(&self) -> bool {
        let workflow_git = self.accepted.workflow_git.clone();
        let lease_clock = self.lease_clock.clone();
        let authority = self.authority_updates.clone();
        tokio::task::spawn_blocking(move || workflow_git.activate(lease_clock, authority).is_ok())
            .await
            .unwrap_or(false)
    }

    pub(super) async fn release_workspace(
        &self,
        quiescence: super::super::workspace::ProcessQuiescence,
        disposition: WorkspaceDisposition,
    ) -> super::super::workspace::CleanupResult {
        self.accepted.workflow_git.disable();
        let result = self
            .accepted
            .root
            .release_workspace_pending(quiescence, disposition)
            .wait_async()
            .await;
        if !self.workspace_release_reported.swap(true, Ordering::AcqRel) {
            let _ = self.manager_events.send(ManagerEvent::WorkspaceReleased {
                assignment_id: self.accepted.assignment_id().to_owned(),
                result,
            });
            self.outbox.wake();
        }
        result
    }

    pub(super) fn collapse(&self, code: &'static str, cause: &'static str, stage: &'static str) {
        self.run_event.result("aborted");
        for (key, value) in [
            (telemetry::attribute::EXECUTOR_FAULT_REASON, code),
            (telemetry::attribute::FAILURE_CAUSE_TYPE, cause),
            (telemetry::attribute::DIAGNOSTIC_STAGE, stage),
        ] {
            self.run_event.set(KeyValue::new(key, value));
        }
    }

    pub(super) fn lease_clock_failure(&self, error: LeaseClockError) {
        self.collapse(
            "runner_internal_failure",
            lease_clock_cause(error),
            "execution_root",
        );
    }

    pub(super) fn abort_retained(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        last_execution_event_sequence: u64,
        reason: &str,
    ) -> ExecutionCompletion {
        ExecutionCompletion::retained(
            self.abort(
                assignment_id,
                attempt_id,
                last_execution_event_sequence,
                reason,
            ),
            RetentionReason::Failed,
        )
    }

    pub(super) fn stage_or_abort<T: PreservableStaging, E>(
        &self,
        staging: Result<T, E>,
        classify: fn(E) -> &'static str,
        stage: &'static str,
        assignment_id: &str,
        attempt_id: &str,
    ) -> Result<PreserveOnDrop<T>, Box<ExecutionCompletion>> {
        staging.map(PreserveOnDrop::new).map_err(|error| {
            Box::new(self.execution_environment_lost(
                assignment_id,
                attempt_id,
                classify(error),
                stage,
            ))
        })
    }

    pub(super) fn execution_environment_lost(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        cause: &'static str,
        stage: &'static str,
    ) -> ExecutionCompletion {
        self.collapse("execution_environment_lost", cause, stage);
        self.abort_retained(assignment_id, attempt_id, 0, "execution_environment_lost")
    }
}
impl ExecutionJob {
    pub(super) fn record_preparation_failure(
        &self,
        stage: &'static str,
        code: &'static str,
        details: Vec<KeyValue>,
    ) {
        self.artifact_delivery.record_preparation_failure(
            self.accepted.run_id(),
            self.accepted.assignment_id(),
            self.accepted.attempt_id(),
            [
                KeyValue::new(telemetry::attribute::ARTIFACT_PREPARATION_STAGE, stage),
                KeyValue::new(telemetry::attribute::ARTIFACT_FAILURE_CODE, code),
            ]
            .into_iter()
            .chain(details),
        );
    }

    pub(super) fn runner_result(
        &self,
        diagnostics: &StepDiagnosticLog,
        execution: WorkflowExecutionResult<RunnerExecutionInstant>,
        observer: &RunnerExecutionObserver,
        started_at: RunnerExecutionInstant,
        finished_at: RunnerExecutionInstant,
    ) -> Result<WorkflowRunResult, RunnerResultFailure> {
        let workflow = self.accepted.admitted.workflow();
        let cancellation =
            observed_workflow_cancellation(&execution.outcome, observer.cancellation())
                .ok_or_else(|| RunnerResultFailure::new("cancellation_inconsistent"))?;
        let mut states = execution.steps;
        let mut recoveries = execution.recoveries;
        let mut steps = Vec::with_capacity(states.len());
        for id in &workflow.definition.presentation_order {
            let state = states
                .remove(id)
                .ok_or_else(|| RunnerResultFailure::for_node("step_state_missing", id))?;
            let recovery_state = recoveries
                .remove(id)
                .ok_or_else(|| RunnerResultFailure::for_node("step_recovery_missing", id))?;
            let recovery = step_recovery_summary_v1(recovery_state.as_ref())
                .map_err(|_| RunnerResultFailure::for_node("recovery_summary_invalid", id))?;
            let (kind, failure_policy) =
                workflow_step_kind_policy(
                    workflow.definition.steps.get(id).ok_or_else(|| {
                        RunnerResultFailure::for_node("step_definition_missing", id)
                    })?,
                );
            steps.push(WorkflowRunStep {
                id: id.clone(),
                role: WorkflowNodeRole::Step,
                kind,
                failure_policy,
                state,
                timing: observer.step_timing(id),
                command_output: (kind == WorkflowRunStepKind::Command)
                    .then(|| diagnostics.get(id))
                    .flatten(),
                recovery,
                invocations: observer.invocations_for_step(id),
            });
        }
        let finalization = match (
            workflow.definition.finalizers.is_empty(),
            execution.finalization_summary,
        ) {
            (true, None) => None,
            (false, Some(summary)) => {
                let mut summarized = summary
                    .finalizers
                    .into_iter()
                    .map(|result| (result.finalizer.clone(), result))
                    .collect::<BTreeMap<_, _>>();
                let mut finalizers = Vec::with_capacity(summarized.len());
                for id in &workflow.definition.finalizer_presentation_order {
                    let state = states.remove(id).ok_or_else(|| {
                        RunnerResultFailure::for_node("finalizer_state_missing", id)
                    })?;
                    let summary = summarized.remove(id).ok_or_else(|| {
                        RunnerResultFailure::for_node("finalizer_summary_missing", id)
                    })?;
                    let finalizer = workflow.definition.finalizers.get(id).ok_or_else(|| {
                        RunnerResultFailure::for_node("finalizer_definition_missing", id)
                    })?;
                    let (kind, failure_policy) = workflow_step_kind_policy(&finalizer.body);
                    if summary.failure_policy != failure_policy
                        || !summary_disposition_matches(&summary.disposition, &state)
                    {
                        return Err(RunnerResultFailure::for_node(
                            "finalizer_disposition_mismatch",
                            id,
                        ));
                    }
                    if recoveries
                        .remove(id)
                        .ok_or_else(|| {
                            RunnerResultFailure::for_node("finalizer_recovery_missing", id)
                        })?
                        .is_some()
                    {
                        return Err(RunnerResultFailure::for_node(
                            "finalizer_recovery_unexpected",
                            id,
                        ));
                    }
                    finalizers.push(WorkflowRunStep {
                        id: id.clone(),
                        role: WorkflowNodeRole::Finalizer,
                        kind,
                        failure_policy,
                        state,
                        timing: observer.step_timing(id),
                        command_output: (kind == WorkflowRunStepKind::Command)
                            .then(|| diagnostics.get(id))
                            .flatten(),
                        recovery: None,
                        invocations: observer.invocations_for_step(id),
                    });
                }
                if !summarized.is_empty() {
                    return Err(RunnerResultFailure::new("finalizer_summary_unconsumed"));
                }
                Some(WorkflowRunFinalization {
                    trigger: summary.trigger,
                    finalizers,
                    cancellation: summary.cancellation.map(|cancellation| {
                        WorkflowRunFinalizationCancellation {
                            reason: cancellation.reason,
                            force_stop_deadline: cancellation.deadline.map(|deadline| deadline.utc),
                        }
                    }),
                    force_abort: summary.force_abort,
                })
            }
            (true, Some(_)) | (false, None) => {
                return Err(RunnerResultFailure::new("finalization_shape_mismatch"));
            }
        };
        if !states.is_empty() || !recoveries.is_empty() {
            return Err(RunnerResultFailure::new("step_state_unconsumed"));
        }
        let continuation = self.continuation_record()?;
        Ok(WorkflowRunResult {
            run_directory: self.accepted.root.private.path().to_owned(),
            attempt_number: self.accepted.attempt_number,
            continuation,
            output_producers: execution.output_producers.into_iter().fold(
                BTreeMap::new(),
                |mut producers, ((node, output), producer)| {
                    producers.entry(node).or_default().insert(output, producer);
                    producers
                },
            ),
            workflow_path: execution.provenance.workflow_path,
            source_root: execution.provenance.source_root,
            content_digest: execution.content_digest,
            execution_root: self.accepted.admitted.execution().root().to_owned(),
            maximum_parallel_steps: self
                .accepted
                .admitted
                .execution()
                .limits()
                .maximum_parallel_steps(),
            maximum_retained_bytes_per_stream: self
                .accepted
                .admitted
                .execution()
                .limits()
                .maximum_step_log_bytes()
                .get(),
            cloud_capacity: Some(cloud_execution_capacity(&self.accepted.admitted)),
            maximum_result_bytes: self
                .accepted
                .admitted
                .workflow()
                .capacity
                .requirements
                .portable_result_bytes,
            timing: WorkflowRunTiming {
                started_at: started_at.utc,
                finished_at: finished_at.utc,
                duration: finished_at
                    .monotonic
                    .saturating_duration_since(started_at.monotonic),
            },
            outcome: execution.outcome,
            cancellation,
            force_abort: execution.force_abort,
            steps,
            finalization,
            exports: execution.exports,
            export_sources: workflow.definition.exports.clone(),
            export_presentation: workflow.definition.export_presentation.clone(),
        })
    }

    pub(super) fn continuation_record(
        &self,
    ) -> Result<Option<um_execution::ContinuationRecordV1>, RunnerResultFailure> {
        self.accepted
            .continuation
            .as_ref()
            .map(|offer| {
                let root = &self.accepted.root;
                let snapshot = root
                    .continuation_snapshot
                    .as_ref()
                    .ok_or_else(|| RunnerResultFailure::new("continuation_snapshot_missing"))?;
                let proof = root
                    .retained_quiescence
                    .ok_or_else(|| RunnerResultFailure::new("continuation_quiescence_missing"))?;
                let proven_at = root
                    .continuation_proven_at
                    .as_ref()
                    .ok_or_else(|| RunnerResultFailure::new("continuation_proof_time_missing"))?;
                let current_digest = &offer.effective_capacity.source_closure_digest;
                let prior_digest = &offer.prior_manifest_digest;
                cloud_continuation_record(
                    offer.request.clone(),
                    offer.reexecuted_steps.clone(),
                    offer.inherited_steps.clone(),
                    DigestV1 {
                        algorithm: current_digest.algorithm.clone(),
                        value: current_digest.value.clone(),
                    },
                    DigestV1 {
                        algorithm: prior_digest.algorithm.clone(),
                        value: prior_digest.value.clone(),
                    },
                    CloudContinuationEvidence {
                        execution_root: root.execution.to_string_lossy().into_owned(),
                        prior_execution_root: offer.execution_root.clone(),
                        start_snapshot: snapshot.start_snapshot.clone(),
                        prior_settlement_snapshot: offer.prior_settlement_snapshot.clone(),
                        modified: snapshot.modified.clone(),
                        quiescence: json!({
                            "groupsRecorded": proof.recorded,
                            "groupsTerminated": proof.terminated,
                            "groupsAbsent": proof.absent,
                            "provenAt": proven_at,
                        }),
                    },
                )
                .map_err(|_| RunnerResultFailure::new("continuation_provenance_invalid"))
            })
            .transpose()
    }

    pub(super) async fn deliver_artifacts(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        artifacts: &ArtifactStaging,
        prepared: PreparedCloudWorkflowResult,
    ) -> Result<ArtifactDeliveryOutcome, LeaseClockError> {
        for carrier in prepared.carriers {
            let delivery = ArtifactDeliverySpec::cloud_carrier(
                assignment_id.to_owned(),
                attempt_id.to_owned(),
                artifacts,
                carrier,
            );
            let outcome = self.await_delivery(assignment_id, delivery).await?;
            if !matches!(outcome, ArtifactDeliveryOutcome::Delivered { .. }) {
                return Ok(outcome);
            }
        }
        let result = match prepared.result_file {
            Some(file) => ArtifactDeliverySpec::result_file(
                assignment_id.to_owned(),
                attempt_id.to_owned(),
                file,
                prepared.result_size_bytes,
                prepared.result_sha256,
            ),
            None => ArtifactDeliverySpec::result(
                assignment_id.to_owned(),
                attempt_id.to_owned(),
                prepared.result_json,
            ),
        };
        self.await_delivery(assignment_id, result).await
    }

    pub(super) async fn await_delivery(
        &self,
        assignment_id: &str,
        delivery: ArtifactDeliverySpec,
    ) -> Result<ArtifactDeliveryOutcome, LeaseClockError> {
        if !self
            .authority_updates
            .borrow()
            .permits_artifact_delivery(self.lease_clock.now()?)?
        {
            return Ok(ArtifactDeliveryOutcome::AuthorityLost);
        }
        let Ok(mut completion) = self.artifact_delivery.start(delivery) else {
            return Ok(internal_delivery_failure("registration"));
        };
        let mut authority_updates = self.authority_updates.clone();
        loop {
            let authority = authority_updates.borrow_and_update().clone();
            let now = self.lease_clock.now()?;
            if !authority.permits_artifact_delivery(now)? {
                self.artifact_delivery.cancel_assignment(assignment_id);
                return Ok(ArtifactDeliveryOutcome::AuthorityLost);
            }
            if !matches!(
                now.checked_cmp(authority.renewal_request)?,
                std::cmp::Ordering::Less
            ) {
                match self.causal_lease.request_renewal(
                    authority.sequence,
                    assignment_id,
                    self.accepted.attempt_id(),
                    &self.lease_clock,
                    &self.outbox,
                ) {
                    Ok(()) => {}
                    Err(RenewalRequestFailure::LeaseClock) => {
                        return Err(LeaseClockError::ClockUnavailable);
                    }
                    Err(RenewalRequestFailure::Outbox) => {
                        self.record_preparation_failure(
                            "delivery_wait",
                            "lease_renewal_outbox_failed",
                            Vec::new(),
                        );
                        return Ok(internal_delivery_failure("preparation"));
                    }
                    Err(RenewalRequestFailure::Sequence) => {
                        self.record_preparation_failure(
                            "delivery_wait",
                            "lease_renewal_sequence_failed",
                            Vec::new(),
                        );
                        return Ok(internal_delivery_failure("preparation"));
                    }
                }
                tokio::select! {
                    result = &mut completion => return Ok(self.delivery_completion(result)),
                    changed = authority_updates.changed() => {
                        if changed.is_err() {
                            self.artifact_delivery.cancel_assignment(assignment_id);
                            return Ok(ArtifactDeliveryOutcome::AuthorityLost);
                        }
                    }
                    result = wait_for_lease_deadline(&self.lease_clock, authority.local_expiry) => {
                        result?;
                        self.artifact_delivery.cancel_assignment(assignment_id);
                        return Ok(ArtifactDeliveryOutcome::AuthorityLost);
                    }
                }
                continue;
            }
            tokio::select! {
                result = &mut completion => return Ok(self.delivery_completion(result)),
                changed = authority_updates.changed() => {
                    if changed.is_err() {
                        self.artifact_delivery.cancel_assignment(assignment_id);
                        return Ok(ArtifactDeliveryOutcome::AuthorityLost);
                    }
                }
                result = wait_for_lease_deadline(&self.lease_clock, authority.renewal_request) => {
                    result?;
                }
            }
        }
    }

    pub(super) fn delivery_completion(
        &self,
        result: Result<ArtifactDeliveryOutcome, tokio::sync::oneshot::error::RecvError>,
    ) -> ArtifactDeliveryOutcome {
        result.unwrap_or_else(|_| {
            self.record_preparation_failure(
                "delivery_wait",
                "delivery_completion_lost",
                Vec::new(),
            );
            internal_delivery_failure("preparation")
        })
    }

    pub(super) async fn wait_for_start_authority(
        &mut self,
        cancellation: &CancellationSource,
        post_stop_fence: &PostStopFence,
        assignment_id: &str,
        attempt_id: &str,
    ) -> Result<(), ExecutionCompletion> {
        loop {
            if *self.start_authority.borrow() {
                return self
                    .ensure_execution_authority(
                        cancellation,
                        post_stop_fence,
                        assignment_id,
                        attempt_id,
                    )
                    .await;
            }
            if let Some(reason) = cancellation.cancellation_reason() {
                return Err(cancellation_before_start_completion(reason));
            }
            self.ensure_execution_authority(
                cancellation,
                post_stop_fence,
                assignment_id,
                attempt_id,
            )
            .await?;
            let cancellation_start = self.authority_updates.borrow().cancellation_start;
            tokio::select! {
                biased;
                reason = cancellation.wait_for_cancellation() => {
                    return Err(cancellation_before_start_completion(reason));
                }
                changed = self.start_authority.changed() => {
                    if changed.is_err() {
                        return Err(ExecutionCompletion::retained(None, RetentionReason::OutcomeUnknown));
                    }
                }
                changed = self.authority_updates.changed() => {
                    if changed.is_err() {
                        return Err(ExecutionCompletion::retained(None, RetentionReason::OutcomeUnknown));
                    }
                }
                elapsed = wait_for_lease_deadline(&self.lease_clock, cancellation_start) => {
                    if let Err(error) = elapsed {
                        self.lease_clock_failure(error);
                        return Err(self
                            .fail_before_execution(
                                cancellation,
                                post_stop_fence,
                                assignment_id,
                                attempt_id,
                            )
                            .await);
                    }
                    begin_forced_containment(
                        cancellation,
                        post_stop_fence,
                        &self.accepted.process_guards,
                    );
                    return Err(ExecutionCompletion::fenced(None));
                }
            }
        }
    }

    pub(super) async fn ensure_execution_authority(
        &self,
        cancellation: &CancellationSource,
        post_stop_fence: &PostStopFence,
        assignment_id: &str,
        attempt_id: &str,
    ) -> Result<(), ExecutionCompletion> {
        match self.has_execution_authority() {
            Ok(true) => Ok(()),
            Ok(false) => Err(ExecutionCompletion::fenced(None)),
            Err(error) => {
                self.lease_clock_failure(error);
                Err(self
                    .fail_before_execution(cancellation, post_stop_fence, assignment_id, attempt_id)
                    .await)
            }
        }
    }

    pub(super) async fn fail_before_execution(
        &self,
        cancellation: &CancellationSource,
        post_stop_fence: &PostStopFence,
        assignment_id: &str,
        attempt_id: &str,
    ) -> ExecutionCompletion {
        begin_forced_containment(cancellation, post_stop_fence, &self.accepted.process_guards);
        ExecutionCompletion::lease_clock_failed(self.abort(
            assignment_id,
            attempt_id,
            0,
            "runner_internal_failure",
        ))
    }

    pub(super) fn has_execution_authority(&self) -> Result<bool, LeaseClockError> {
        let authority = self.authority_updates.borrow();
        Ok(!authority.revoked
            && matches!(
                self.lease_clock
                    .now()?
                    .checked_cmp(authority.cancellation_start)?,
                std::cmp::Ordering::Less
            ))
    }

    pub(super) fn terminal_report_deadline(&self) -> Result<LeaseInstant, LeaseClockError> {
        let selected_at = self.lease_clock.now()?;
        let authority = self.authority_updates.borrow();
        let budget_end = selected_at.checked_add(authority.terminal_report_delivery_budget)?;
        match budget_end.checked_cmp(authority.local_expiry)? {
            std::cmp::Ordering::Greater => Ok(authority.local_expiry),
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal => Ok(budget_end),
        }
    }

    pub(super) async fn abort_unless_fenced(
        &self,
        post_stop_fence: &PostStopFence,
        assignment_id: &str,
        attempt_id: &str,
        last_execution_event_sequence: u64,
        reason: &str,
    ) -> ExecutionCompletion {
        if post_stop_fence.is_fenced() {
            ExecutionCompletion::fenced(None)
        } else {
            self.abort_retained(
                assignment_id,
                attempt_id,
                last_execution_event_sequence,
                reason,
            )
        }
    }

    pub(super) fn enqueue(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        report: ExecutionReport,
    ) -> Option<u64> {
        let terminal = report.is_terminal();
        if terminal {
            self.describe_report(&report);
        }
        let enqueued = self.outbox.enqueue(AssignmentObservation::Execution {
            assignment_id: assignment_id.to_owned(),
            attempt_id: attempt_id.to_owned(),
            report,
        });
        if let Err(error) = enqueued {
            self.collapse(
                "runner_internal_failure",
                outbox_cause(error, terminal),
                "execution_root",
            );
        }
        enqueued.ok()
    }

    pub(super) fn describe_report(&self, report: &ExecutionReport) {
        match report {
            ExecutionReport::Finished { outcome, .. } => {
                match outcome["outcome"].as_str() {
                    Some("succeeded") => self.run_event.result("succeeded"),
                    Some("failed") => {
                        self.run_event.result("failed");
                        for (key, value) in [
                            (
                                telemetry::attribute::FAILURE_PHASE,
                                outcome["primaryIssue"]["detail"]["phase"].as_str(),
                            ),
                            (
                                telemetry::attribute::FAILURE_CODE,
                                outcome["primaryIssue"]["detail"]["code"].as_str(),
                            ),
                        ] {
                            if let Some(value) = value {
                                self.run_event.set(KeyValue::new(key, value.to_owned()));
                            }
                        }
                    }
                    Some("cancelled") => self.run_event.result("cancelled"),
                    _ => self.run_event.result("aborted"),
                }
                if let Some(reason) = outcome["reason"].as_str() {
                    self.run_event.set(KeyValue::new(
                        telemetry::attribute::INTERRUPTION_CAUSE,
                        reason.to_owned(),
                    ));
                }
            }
            ExecutionReport::Interrupted { reason, .. }
            | ExecutionReport::AssignmentInterrupted { reason } => {
                self.run_event.result("interrupted");
                self.run_event.set(KeyValue::new(
                    telemetry::attribute::INTERRUPTION_CAUSE,
                    reason.clone(),
                ));
            }
            ExecutionReport::Aborted { reason, .. } => {
                self.run_event.result("aborted");
                self.run_event.set(KeyValue::new(
                    telemetry::attribute::EXECUTOR_FAULT_REASON,
                    reason.clone(),
                ));
            }
            ExecutionReport::Started | ExecutionReport::Transition { .. } => {}
        }
    }

    pub(super) fn abort(
        &self,
        assignment_id: &str,
        attempt_id: &str,
        last_execution_event_sequence: u64,
        reason: &str,
    ) -> Option<u64> {
        self.enqueue(
            assignment_id,
            attempt_id,
            ExecutionReport::Aborted {
                last_execution_event_sequence,
                reason: reason.to_owned(),
            },
        )
    }
}
