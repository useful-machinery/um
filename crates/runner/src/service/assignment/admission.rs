use super::*;

impl AssignmentManager {
    pub(in crate::service) fn new(
        config: &Config,
        lease_clock: LeaseClock,
        dependencies: AssignmentDependencies,
    ) -> Self {
        let AssignmentDependencies {
            work_root,
            root_preparer,
            sleeper,
            source_broker,
            input_broker,
            execution_version,
            recorder,
            guard_processes,
        } = dependencies;
        let (event_sender, events) = mpsc::unbounded_channel();
        let outbox = ObservationOutbox::new();
        let allow_insecure_artifact_uploads =
            config.endpoint().scheme() == "ws" && crate::is_loopback(config.endpoint());
        let artifact_delivery = ArtifactDeliveryBroker::new(
            outbox.clone(),
            Arc::clone(&sleeper),
            allow_insecure_artifact_uploads,
            recorder.clone(),
        );
        let root_preparation_worker = AssignmentRootPreparationWorker::new();
        Self {
            work_root,
            root_preparer,
            root_preparation_worker,
            pi_installation: config.pi_installation().cloned(),
            claude_code_installation: config.claude_code_installation().cloned(),
            codex_installation: config.codex_installation().cloned(),
            environment: EnvironmentSnapshot::new(std::env::vars_os()),
            execution_version,
            lease_clock,
            sleeper,
            source_broker,
            input_broker,
            recorder,
            runner_id: config.credential().runner_id().to_owned(),
            run_events: BTreeMap::new(),
            lease_policy: None,
            slot: None,
            reporting: None,
            decisions: VecDeque::new(),
            cancellations: VecDeque::new(),
            releases: VecDeque::new(),
            outbox,
            artifact_delivery,
            events,
            event_sender,
            shutting_down: false,
            shutdown_cleanup_deadline: None,
            lease_clock_failed: false,
            lease_clock_failure_report: None,
            cleanup_failed: false,
            cleanup_failure_report: None,
            quiescence_failure: None,
            deferred_successor: None,
            deferred_offer_failure: None,
            fenced_final_graces: BTreeSet::new(),
            guard_processes,
        }
    }

    pub(in crate::service) fn retain_lease_policy(
        &mut self,
        policy: &ExecutionLeasePolicy,
    ) -> Result<(), WelcomePolicyFailure> {
        validate_lease_policy(policy)?;
        match &self.lease_policy {
            Some(retained) if retained != policy => Err(WelcomePolicyFailure::Changed),
            Some(_) => Ok(()),
            None => {
                self.lease_policy = Some(policy.clone());
                Ok(())
            }
        }
    }

    pub(in crate::service) fn handle_offer(
        &mut self,
        offer: AssignmentOffer,
    ) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();
        self.handle_offer_after_drain(offer)
    }

    pub(super) fn handle_offer_after_drain(
        &mut self,
        offer: AssignmentOffer,
    ) -> Result<(), AssignmentManagerFailure> {
        if self.cancellation_uses_effect_id(&offer.effect_id) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(LocalSlot::Preparing(preparing)) = &self.slot {
            if preparing.offer.assignment_id == offer.assignment_id {
                return if same_assignment(&preparing.offer, &offer) {
                    Ok(())
                } else {
                    Err(AssignmentManagerFailure::ConflictingOffer)
                };
            }
            if preparing.offer.effect_id == offer.effect_id {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
        }
        if let Some(index) = self
            .decisions
            .iter()
            .position(|decision| decision.offer.assignment_id == offer.assignment_id)
        {
            if !same_assignment(&self.decisions[index].offer, &offer) {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
            return Ok(());
        }
        if self.decisions.iter().any(|decision| {
            decision.offer.effect_id == offer.effect_id && !same_assignment(&decision.offer, &offer)
        }) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(deferred) = &self.deferred_successor {
            if deferred.assignment_id == offer.assignment_id {
                return if same_assignment(deferred, &offer) {
                    Ok(())
                } else {
                    Err(AssignmentManagerFailure::ConflictingOffer)
                };
            }
            if deferred.effect_id == offer.effect_id {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
        }
        if let Some(cancelled) = self.cancellations.iter().find(|retained| {
            retained.application.is_some() && retained.command.assignment_id == offer.assignment_id
        }) {
            return if cancelled.command.run_id == offer.run_id
                && cancelled.command.attempt_id == offer.attempt_id
            {
                Ok(())
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }

        if self.cleanup_failed {
            self.make_decision_room()?;
            let response = rejected(&offer, environment_unavailable());
            return self.retain_decision(offer, response);
        }
        if matches!(self.slot, Some(LocalSlot::Releasing(_))) {
            if self.deferred_successor.is_none() {
                self.deferred_successor = Some(offer);
                return Ok(());
            }
            self.make_decision_room()?;
            let response = rejected(&offer, AssignmentDecline::CapacityUnavailable);
            return self.retain_decision(offer, response);
        }
        if matches!(
            &self.slot,
            Some(LocalSlot::Finishing(finishing))
                if finishing.identity.assignment_id != offer.assignment_id
        ) {
            let Some(LocalSlot::Finishing(finishing)) = self.slot.take() else {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            };
            self.retire_assignment_observations(&finishing.identity.assignment_id);
            if let Some(root) = finishing.root {
                self.deferred_successor = Some(offer);
                self.begin_assignment_finalization(
                    finishing.identity.assignment_id,
                    root,
                    ProcessQuiescence::Proven,
                    finishing.workspace_disposition,
                    ReleaseAfter::Idle,
                );
                return Ok(());
            }
            // The root is already released. Admission may proceed, but the old
            // grace event must not restore reporting after successor fencing.
            self.fenced_final_graces.insert(FencedFinalGrace {
                assignment_id: finishing.identity.assignment_id,
                final_observation_id: finishing.final_observation_id,
            });
        }

        if self.shutting_down {
            self.make_decision_room()?;
            let response = rejected(&offer, AssignmentDecline::CapacityUnavailable);
            return self.retain_decision(offer, response);
        }

        self.apply_successor_fences(&offer.assignment_id);
        self.make_decision_room()?;
        if self.slot.is_some() {
            let response = rejected(&offer, AssignmentDecline::CapacityUnavailable);
            return self.retain_decision(offer, response);
        }

        if let Err(decline) = self.validate_admission_prerequisites(&offer) {
            let response = rejected(&offer, decline);
            return self.retain_decision(offer, response);
        }

        let root_offer = offer.clone();
        let root_preparation = Arc::new(AssignmentRootPreparationHandoff::new());
        self.slot = Some(LocalSlot::Preparing(Box::new(PreparingAssignment {
            offer,
            cancellation: CaptureCancellation::default(),
            root_preparation: Some(Arc::clone(&root_preparation)),
            root: None,
            prepare_effect_id: None,
            preparation_event: None,
        })));
        if self
            .begin_workspace_preparation(root_offer, root_preparation)
            .is_err()
        {
            let Some(LocalSlot::Preparing(preparing)) = self.slot.take() else {
                return Err(AssignmentManagerFailure::DecisionCapacity);
            };
            let offer = preparing.offer;
            let response = rejected(&offer, environment_unavailable());
            return self.retain_decision(offer, response);
        }
        Ok(())
    }

    pub(in crate::service) fn handle_prepare(
        &mut self,
        prepare: AssignmentPrepare,
    ) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();
        if self.cancellation_uses_effect_id(&prepare.effect_id) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        let Some(LocalSlot::Preparing(mut preparing)) = self.slot.take() else {
            return Ok(());
        };
        if prepare.assignment_id != preparing.offer.assignment_id
            || prepare.run_id != preparing.offer.run_id
            || prepare.attempt_id != preparing.offer.attempt_id
            || prepare.execution_spec_id != preparing.offer.execution_spec.execution_spec_id
        {
            self.slot = Some(LocalSlot::Preparing(preparing));
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(effect_id) = &preparing.prepare_effect_id {
            let replay = effect_id == &prepare.effect_id;
            self.slot = Some(LocalSlot::Preparing(preparing));
            return if replay {
                Ok(())
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }
        let Some(root) = preparing.root.take() else {
            self.slot = Some(LocalSlot::Preparing(preparing));
            return Err(AssignmentManagerFailure::ConflictingOffer);
        };
        let offer = preparing.offer.clone();
        let Some(deadline) = PreparationDeadline::from_wire(
            &prepare.preparation_expires_at,
            self.sleeper.utc_now(),
            self.sleeper.now(),
        ) else {
            preparing.cancellation.cancel();
            self.begin_assignment_cleanup(
                offer.assignment_id,
                root,
                ProcessQuiescence::Proven,
                ReleaseAfter::Idle,
            );
            return Ok(());
        };
        preparing.preparation_event = self.recorder.as_ref().map(|recorder| {
            recorder.start(
                "runner.assignment_preparation",
                [
                    opentelemetry::KeyValue::new(
                        crate::telemetry::attribute::ASSIGNMENT_ID,
                        offer.assignment_id.clone(),
                    ),
                    opentelemetry::KeyValue::new(
                        crate::telemetry::attribute::RUN_ID,
                        offer.run_id.clone(),
                    ),
                    opentelemetry::KeyValue::new(
                        crate::telemetry::attribute::ASSIGNMENT_PREPARATION_PHASE,
                        "source_materialization",
                    ),
                ],
            )
        });
        let progress = AssignmentObservation::PreparationProgress {
            assignment_id: offer.assignment_id.clone(),
            attempt_id: offer.attempt_id.clone(),
            preparation_sequence: 1,
            phase: "source_materialization".to_owned(),
        };
        if self.outbox.enqueue(progress).is_err() {
            self.slot = Some(LocalSlot::Preparing(preparing));
            self.begin_assignment_cleanup(
                offer.assignment_id,
                root,
                ProcessQuiescence::Proven,
                ReleaseAfter::Idle,
            );
            return Err(AssignmentManagerFailure::DecisionCapacity);
        }
        preparing.prepare_effect_id = Some(prepare.effect_id.clone());
        self.slot = Some(LocalSlot::Preparing(preparing));
        self.begin_source_admission(offer, root, prepare.effect_id, deadline)
    }

    pub(super) fn begin_workspace_preparation(
        &self,
        offer: AssignmentOffer,
        handoff: Arc<AssignmentRootPreparationHandoff>,
    ) -> Result<(), ()> {
        self.root_preparation_worker
            .prepare(AssignmentRootPreparation {
                offer,
                recorder: self.recorder.clone(),
                root_preparer: Arc::clone(&self.root_preparer),
                handoff,
                event_sender: self.event_sender.clone(),
                wake: self.outbox.clone(),
            })
    }

    pub(super) fn finish_preparation_event(&mut self, outcome: TelemetryOutcome) {
        if let Some(LocalSlot::Preparing(preparing)) = &mut self.slot
            && let Some(event) = preparing.preparation_event.take()
        {
            event.finish(outcome);
        }
    }

    pub(super) fn reject_after_prepare_and_cleanup(
        &mut self,
        offer: AssignmentOffer,
        root: AssignmentRoot,
        prepare_effect_id: String,
        decline: AssignmentDecline,
    ) -> Result<(), AssignmentManagerFailure> {
        self.finish_preparation_event(TelemetryOutcome::Failure);
        let response = AssignmentDecision::Rejected {
            effect_id: prepare_effect_id,
            assignment_id: offer.assignment_id.clone(),
            decline,
        };
        self.retain_decision(offer.clone(), response)?;
        self.begin_assignment_cleanup(
            offer.assignment_id,
            root,
            ProcessQuiescence::Proven,
            ReleaseAfter::Idle,
        );
        Ok(())
    }

    pub(super) fn begin_source_admission(
        &mut self,
        offer: AssignmentOffer,
        root: AssignmentRoot,
        prepare_effect_id: String,
        deadline: PreparationDeadline,
    ) -> Result<(), AssignmentManagerFailure> {
        let source =
            super::super::source::MaterializationRequest::from_validated(&offer.execution_spec);
        let Some(broker) = self.source_broker.clone() else {
            let decline =
                AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable);
            return self.reject_after_prepare_and_cleanup(offer, root, prepare_effect_id, decline);
        };

        let cancellation = CaptureCancellation::default();
        let sleeper = Arc::clone(&self.sleeper);
        let worker_cancellation = cancellation.clone();
        let worker_offer = offer.clone();
        let environment = self.environment.clone();
        let input_broker = self.input_broker.clone();
        let runtime = self.admission_runtime();
        let sender = self.event_sender.clone();
        let wake = self.outbox.clone();
        if let Some(LocalSlot::Preparing(preparing)) = &mut self.slot {
            preparing.cancellation = cancellation;
        }
        tokio::task::spawn_blocking(move || {
            let preparation_fence = match PreparationFence::arm(
                deadline,
                worker_cancellation.clone(),
                Arc::clone(&sleeper),
            ) {
                Ok(fence) => fence,
                Err(()) => {
                    let _ = sender.send(ManagerEvent::Prepared {
                        offer: Box::new(worker_offer),
                        prepare_effect_id,
                        deadline,
                        admission: Box::new(Err(Box::new((root, environment_unavailable())))),
                    });
                    wake.wake();
                    return;
                }
            };
            let _preparation_fence = preparation_fence;
            let mut root = root;
            let workspace_path = root.workspace.path();
            let preparation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let checkout = super::super::source::checkout(
                    Arc::clone(&broker),
                    &environment,
                    &worker_offer.assignment_id,
                    &source,
                    &worker_cancellation,
                    &workspace_path,
                    &root.private,
                )
                .map_err(materialization_decline)?;
                runtime.progress(&worker_offer, 2, "input_download")?;
                let inputs = super::super::run_inputs::materialize(
                    input_broker.as_deref(),
                    &worker_offer.assignment_id,
                    &worker_offer.execution_spec.execution_spec_id,
                    worker_offer.execution_spec.run_inputs.as_ref(),
                    deadline,
                    &worker_cancellation,
                    &root.private,
                )
                .map_err(run_input_decline)?;
                runtime.progress(&worker_offer, 3, "workflow_admission")?;
                let materialized =
                    super::super::source::resolve_checkout(checkout, &worker_cancellation)
                        .map_err(materialization_decline)?;
                Ok::<_, AssignmentDecline>((materialized, inputs))
            }));
            let admission = match preparation {
                Ok(Ok((materialized, inputs))) => {
                    root.execution = materialized.execution_root;
                    let workflow_git = std::env::current_exe()
                        .map_err(anyhow::Error::from)
                        .and_then(|helper_executable| {
                            WorkflowGitAuthority::install(WorkflowGitInstall {
                                broker: Arc::clone(&broker),
                                assignment_id: &worker_offer.assignment_id,
                                origin: materialized.origin,
                                workspace: &root.execution,
                                private_root: &root.private,
                                environment: &environment,
                                helper_executable: &helper_executable,
                                clock: Arc::clone(&sleeper),
                                recorder: runtime.recorder.clone(),
                                cancellation: &worker_cancellation,
                            })
                        });
                    match workflow_git {
                        Ok(workflow_git) => {
                            root.install_workflow_git(workflow_git.clone());
                            runtime.finish(
                                &worker_offer,
                                root,
                                materialized.workflow,
                                inputs,
                                materialized.git_capture,
                                PreparationAuthority {
                                    deadline,
                                    cancellation: &worker_cancellation,
                                    monotonic_now: sleeper.now(),
                                },
                            )
                        }
                        Err(_) => Err(Box::new((root, environment_unavailable()))),
                    }
                }
                Ok(Err(decline)) => Err(Box::new((root, decline))),
                Err(_) => Err(Box::new((root, environment_unavailable()))),
            };
            let _ = sender.send(ManagerEvent::Prepared {
                offer: Box::new(worker_offer),
                prepare_effect_id,
                deadline,
                admission: Box::new(admission),
            });
            wake.wake();
        });
        Ok(())
    }

    pub(super) fn cancellation_uses_effect_id(&self, effect_id: &str) -> bool {
        self.cancellations
            .iter()
            .any(|retained| retained.command.effect_id == effect_id)
    }

    pub(super) fn release_uses_effect_id(&self, effect_id: &str) -> bool {
        self.releases
            .iter()
            .any(|release| release.effect_id == effect_id)
    }

    pub(super) fn retain_release(&mut self, release: AssignmentRelease) {
        if self.releases.len() == MAXIMUM_RETAINED_DECISIONS {
            self.releases.pop_front();
        }
        self.releases.push_back(release);
    }

    pub(in crate::service) fn handle_cancel(
        &mut self,
        cancel: AssignmentCancel,
    ) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();

        if let Some(index) = self
            .cancellations
            .iter()
            .position(|retained| retained.command.request_id == cancel.request_id)
        {
            if self.cancellations[index].command != cancel {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
            if self.cancellations[index].ready
                && self.cancellations[index].observation_id.is_none()
                && self.cancellations[index].application.is_some()
            {
                self.emit_cancellation_application(index)?;
            }
            return Ok(());
        }
        if self.cancellation_uses_effect_id(&cancel.effect_id)
            || self.release_uses_effect_id(&cancel.effect_id)
            || self.decisions.iter().any(|decision| {
                decision.offer.effect_id == cancel.effect_id
                    || match &decision.response {
                        AssignmentDecision::Accepted { effect_id, .. }
                        | AssignmentDecision::Rejected { effect_id, .. } => {
                            effect_id == &cancel.effect_id
                        }
                    }
                    || decision
                        .start
                        .as_ref()
                        .is_some_and(|start| start.effect_id == cancel.effect_id)
                    || decision
                        .start_authorization
                        .as_ref()
                        .is_some_and(|authorization| authorization.effect_id == cancel.effect_id)
                    || decision.renewals.contains_key(&cancel.effect_id)
                    || decision.rejected_renewals.contains_key(&cancel.effect_id)
            })
            || matches!(&self.slot,
                Some(LocalSlot::Preparing(preparing))
                    if preparing.offer.effect_id == cancel.effect_id
                        || preparing.prepare_effect_id.as_ref() == Some(&cancel.effect_id))
            || self
                .deferred_successor
                .as_ref()
                .is_some_and(|offer| offer.effect_id == cancel.effect_id)
        {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if self.cancellations.iter().any(|retained| {
            retained.command.assignment_id == cancel.assignment_id
                && (retained.command.run_id != cancel.run_id
                    || retained.command.attempt_id != cancel.attempt_id)
        }) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(LocalSlot::Preparing(preparing)) = &self.slot
            && preparing.offer.assignment_id == cancel.assignment_id
        {
            if preparing.offer.run_id != cancel.run_id
                || preparing.offer.attempt_id != cancel.attempt_id
            {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
            let effective_mode = self.effective_cancellation_mode(&cancel);
            let application = cancellation_application(
                &cancel,
                effective_mode,
                cancellation_disposition(cancel.mode, effective_mode, false),
            );
            self.retain_cancellation(cancel, Some(application), false)?;
            let Some(LocalSlot::Preparing(preparing)) = &mut self.slot else {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            };
            preparing.cancellation.cancel();
            if let Some(event) = preparing.preparation_event.take() {
                event.finish(TelemetryOutcome::Cancelled);
            }
            if preparing.root.is_some() && preparing.prepare_effect_id.is_none() {
                let Some(LocalSlot::Preparing(mut preparing)) = self.slot.take() else {
                    return Err(AssignmentManagerFailure::ConflictingOffer);
                };
                let Some(root) = preparing.root.take() else {
                    self.slot = Some(LocalSlot::Preparing(preparing));
                    return Err(AssignmentManagerFailure::ConflictingOffer);
                };
                let identity = AssignmentIdentity::from_offer(&preparing.offer);
                self.begin_assignment_finalization(
                    identity.assignment_id.clone(),
                    root,
                    ProcessQuiescence::Proven,
                    WorkspaceDisposition::Retain(RetentionReason::Cancelled),
                    ReleaseAfter::PreExecutionCancellation(Box::new(identity)),
                );
            }
            return Ok(());
        }

        let matching_decision = self.matching_decision_index(
            &cancel.assignment_id,
            &cancel.run_id,
            &cancel.attempt_id,
        )?;
        if let Some(LocalSlot::Releasing(releasing)) = &self.slot
            && releasing.assignment_id == cancel.assignment_id
            && let ReleaseAfter::PreExecutionCancellation(identity) = &releasing.after
            && (identity.run_id != cancel.run_id || identity.attempt_id != cancel.attempt_id)
        {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        let accepted_target = matching_decision.is_some_and(|index| {
            matches!(
                self.decisions[index].response,
                AssignmentDecision::Accepted { .. }
            )
        });
        let exact_slot = match &self.slot {
            Some(LocalSlot::Accepted(accepted)) => {
                accepted.identity.assignment_id == cancel.assignment_id
            }
            Some(LocalSlot::Running(running)) => {
                running.identity.assignment_id == cancel.assignment_id
            }
            Some(LocalSlot::Finishing(finishing)) => {
                finishing.identity.assignment_id == cancel.assignment_id
            }
            Some(LocalSlot::Releasing(releasing)) => {
                releasing.assignment_id == cancel.assignment_id
                    && (accepted_target
                        || matches!(&releasing.after, ReleaseAfter::PreExecutionCancellation(_)))
            }
            Some(LocalSlot::Preparing(_)) | None => false,
        };
        if !exact_slot {
            if accepted_target {
                let effective_mode = self.effective_cancellation_mode(&cancel);
                let application = cancellation_application(
                    &cancel,
                    effective_mode,
                    cancellation_disposition(cancel.mode, effective_mode, true),
                );
                let index = self.retain_cancellation(cancel, Some(application), true)?;
                self.emit_cancellation_application(index)?;
            } else {
                self.retain_cancellation(cancel, None, true)?;
            }
            return Ok(());
        }

        let effective_mode = self.effective_cancellation_mode(&cancel);
        let superseded =
            cancel.mode == CancellationMode::Graceful && effective_mode == CancellationMode::Force;
        self.make_cancellation_room()?;
        match self.slot.take() {
            Some(LocalSlot::Accepted(accepted)) => {
                let identity = accepted.identity.clone();
                let application = cancellation_application(
                    &cancel,
                    effective_mode,
                    cancellation_disposition(cancel.mode, effective_mode, false),
                );
                self.retain_cancellation(cancel, Some(application), false)?;
                self.begin_assignment_finalization(
                    identity.assignment_id.clone(),
                    accepted.root,
                    ProcessQuiescence::Proven,
                    WorkspaceDisposition::Retain(RetentionReason::Cancelled),
                    ReleaseAfter::PreExecutionCancellation(Box::new(identity)),
                );
            }
            Some(LocalSlot::Running(running)) => {
                let awaiting_start_authority = !*running.start_authority.borrow();
                let disposition = if superseded {
                    CancellationApplicationDisposition::Superseded
                } else if running.engine_terminal.load(Ordering::Acquire) {
                    CancellationApplicationDisposition::ExecutionTerminal
                } else {
                    match cancel.mode {
                        CancellationMode::Force => {
                            running.cancellation.request_force_abort();
                            if awaiting_start_authority {
                                CancellationApplicationDisposition::PreExecutionStopped
                            } else {
                                CancellationApplicationDisposition::ForceCancelling
                            }
                        }
                        CancellationMode::Graceful => {
                            let application = running
                                .cancellation
                                .request_ordinary_cancellation(CancellationReason::UserRequest);
                            if awaiting_start_authority {
                                CancellationApplicationDisposition::PreExecutionStopped
                            } else if application
                                == OrdinaryCancellationRequestResult::FinalizersPreserved
                            {
                                CancellationApplicationDisposition::FinalizersPreserved
                            } else {
                                CancellationApplicationDisposition::OrdinaryCancelling
                            }
                        }
                    }
                };
                let application = cancellation_application(&cancel, effective_mode, disposition);
                let ready =
                    running.engine_terminal.load(Ordering::Acquire) || !awaiting_start_authority;
                let index = self.retain_cancellation(cancel, Some(application), ready)?;
                self.slot = Some(LocalSlot::Running(running));
                if self.cancellations[index].ready {
                    self.emit_cancellation_application(index)?;
                }
            }
            Some(LocalSlot::Finishing(finishing)) => {
                let application = cancellation_application(
                    &cancel,
                    effective_mode,
                    cancellation_disposition(cancel.mode, effective_mode, true),
                );
                let index = self.retain_cancellation(cancel, Some(application), true)?;
                self.slot = Some(LocalSlot::Finishing(finishing));
                self.emit_cancellation_application(index)?;
            }
            Some(LocalSlot::Releasing(releasing)) => {
                let pre_execution =
                    matches!(&releasing.after, ReleaseAfter::PreExecutionCancellation(_));
                let application = cancellation_application(
                    &cancel,
                    effective_mode,
                    cancellation_disposition(cancel.mode, effective_mode, !pre_execution),
                );
                let index = self.retain_cancellation(cancel, Some(application), !pre_execution)?;
                self.slot = Some(LocalSlot::Releasing(releasing));
                if self.cancellations[index].ready {
                    self.emit_cancellation_application(index)?;
                }
            }
            slot => {
                self.slot = slot;
                self.retain_cancellation(cancel, None, true)?;
            }
        }
        Ok(())
    }
}
