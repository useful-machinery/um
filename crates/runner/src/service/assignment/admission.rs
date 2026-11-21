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
            tree_run,
            execution_version,
            recorder,
            guard_processes,
            events: runner_events,
        } = dependencies;
        let (event_sender, events) = mpsc::unbounded_channel();
        let outbox = ObservationOutbox::new().with_events(Arc::clone(&runner_events));
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
            agent_login_directory: config
                .state_access()
                .map(|state| crate::agent_login::directory::LoginDirectory::beside(state.path())),
            agent_login_setup: None,
            grant_token_refresh: None,
            execution_version,
            lease_clock,
            sleeper,
            source_broker,
            input_broker,
            tree_run,
            recorder,
            runner_events,
            runner_id: config.credential().runner_id().to_owned(),
            run_events: BTreeMap::new(),
            offer_facts: BTreeMap::new(),
            recent_assignments: Ring::new(RECENT_ASSIGNMENTS),
            lease_policy: None,
            held: BTreeMap::new(),
            reporting: BTreeMap::new(),
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

    /// Makes each later admission set agent logins up for itself with
    /// `setup` before it offers them (REQ-GRANT-49). The setup is built
    /// after the manager, since its gate holds the manager.
    pub(in crate::service) fn set_up_agent_logins_with(
        &mut self,
        setup: super::super::control::AdmissionAgentLogins,
    ) {
        self.agent_login_setup = Some(setup);
    }

    /// Makes each later admission keep the token files of its grant tokens
    /// current while its assignment runs, waiting on `sleeper` and
    /// publishing each readiness a refresh records in `status`
    /// (REQ-GRANT-89).
    pub(in crate::service) fn refresh_grant_tokens_with(
        &mut self,
        sleeper: Arc<dyn Sleeper>,
        status: Arc<super::super::control::LiveStatus>,
    ) {
        self.grant_token_refresh = Some(GrantTokenRefreshing { sleeper, status });
    }

    /// Accepts a connection's `welcome`: retains its lease policy (`retain_lease_policy`),
    /// then asks Cloud again at once for the next lease of every running assignment whose
    /// request Cloud acknowledged and has not answered (REQ-RUNNERENROLL-151,
    /// INV-RUNNER-48). A request still in the outbox is replayed by the outbox, so a second
    /// welcome on one connection adds nothing.
    pub(in crate::service) fn accept_welcome(
        &mut self,
        policy: &ExecutionLeasePolicy,
    ) -> Result<(), WelcomePolicyFailure> {
        self.retain_lease_policy(policy)?;
        self.drain_events();
        let clock_failure = self.held.values().find_map(|held| {
            let HeldAssignment::Running(running) = held else {
                return None;
            };
            running
                .causal_lease
                .repeat_renewal_request(
                    running.current_grant.sequence,
                    &running.identity.assignment_id,
                    &running.identity.attempt_id,
                    &self.lease_clock,
                    &self.outbox,
                )
                .err()
                .map(|error| (running.identity.assignment_id.clone(), error))
        });
        if let Some((assignment_id, error)) = clock_failure {
            // The connection loop observes the failure as it does one met in a renewal.
            self.fail_lease_clock(&assignment_id, error);
        }
        Ok(())
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
        for held in self.held.values() {
            let HeldAssignment::Preparing(preparing) = held else {
                continue;
            };
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
        if self.releasing_any() {
            if self.deferred_successor.is_none() {
                self.deferred_successor = Some(offer);
                return Ok(());
            }
            self.make_decision_room()?;
            let response = rejected(&offer, AssignmentDecline::CapacityUnavailable);
            return self.retain_decision(offer, response);
        }
        // The offer proves Cloud has settled an ended assignment only when that assignment
        // never reported a wait; one that did reports on beside the successor
        // (INV-RUNNER-40).
        let fenced: Vec<String> = self
            .held
            .values()
            .filter(|held| {
                matches!(held, HeldAssignment::Finishing(finishing) if !finishing.reported_wait)
                    && held.assignment_id() != offer.assignment_id
            })
            .map(|held| held.assignment_id().to_owned())
            .collect();
        let mut releasing_fenced = false;
        for assignment_id in fenced {
            let Some(finishing) = self.take_held(&assignment_id, HeldAssignment::finishing) else {
                continue;
            };
            self.retire_assignment_observations(&assignment_id);
            if let Some(root) = finishing.root {
                releasing_fenced = true;
                self.begin_assignment_finalization(
                    assignment_id,
                    root,
                    ProcessQuiescence::Proven,
                    finishing.workspace_disposition,
                    ReleaseAfter::Idle,
                );
                continue;
            }
            // The root is already released. Admission may proceed, but the old
            // grace event must not restore reporting after successor fencing.
            self.fenced_final_graces.insert(FencedFinalGrace {
                assignment_id,
                final_observation_id: finishing.final_observation_id,
            });
        }
        if releasing_fenced {
            self.deferred_successor = Some(offer);
            return Ok(());
        }

        if self.shutting_down {
            self.make_decision_room()?;
            let response = rejected(&offer, AssignmentDecline::CapacityUnavailable);
            return self.retain_decision(offer, response);
        }

        self.apply_successor_fences(&offer.assignment_id);
        self.make_decision_room()?;
        // The runner takes an offer only while every assignment it holds waits, and holds
        // at most `MAXIMUM_HELD_ASSIGNMENTS` (INV-RUNNER-40).
        if self.held.values().any(HeldAssignment::is_working)
            || self.held.len() >= MAXIMUM_HELD_ASSIGNMENTS
        {
            let response = rejected(&offer, AssignmentDecline::CapacityUnavailable);
            return self.retain_decision(offer, response);
        }

        if let Err(decline) = self.validate_admission_prerequisites(&offer) {
            let response = rejected(&offer, decline);
            return self.retain_decision(offer, response);
        }

        let root_offer = offer.clone();
        let root_preparation = Arc::new(AssignmentRootPreparationHandoff::new());
        let run_event = RunEvent::default();
        if let Some(recorder) = &self.recorder {
            run_event.start(
                recorder,
                &AssignmentIdentity::from_offer(&offer),
                &self.runner_id,
            );
        }
        let assignment_id = offer.assignment_id.clone();
        self.run_events
            .insert(offer.assignment_id.clone(), run_event.clone());
        self.keep_offer_facts(&offer);
        self.hold(HeldAssignment::Preparing(Box::new(PreparingAssignment {
            offer,
            cancellation: CaptureCancellation::default(),
            root_preparation: Some(Arc::clone(&root_preparation)),
            root: None,
            prepare_effect_id: None,
            preparation_event: None,
            run_event,
        })));
        if self
            .begin_workspace_preparation(root_offer, root_preparation)
            .is_err()
        {
            let Some(preparing) = self.take_held(&assignment_id, HeldAssignment::preparing) else {
                return Err(AssignmentManagerFailure::DecisionCapacity);
            };
            let offer = preparing.offer;
            let decline = environment_unavailable();
            self.reject_offered_run(&offer.assignment_id, &preparing.run_event, &decline);
            let response = rejected(&offer, decline);
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
        let Some(mut preparing) = self.take_held(&prepare.assignment_id, HeldAssignment::preparing)
        else {
            // A prepare of an assignment not in preparation conflicts with one that is.
            return if self
                .held
                .values()
                .any(|held| matches!(held, HeldAssignment::Preparing(_)))
            {
                Err(AssignmentManagerFailure::ConflictingOffer)
            } else {
                Ok(())
            };
        };
        if prepare.run_id != preparing.offer.run_id
            || prepare.attempt_id != preparing.offer.attempt_id
            || prepare.execution_spec_id != preparing.offer.execution_spec.execution_spec_id
        {
            self.hold(HeldAssignment::Preparing(preparing));
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(effect_id) = &preparing.prepare_effect_id {
            let replay = effect_id == &prepare.effect_id;
            self.hold(HeldAssignment::Preparing(preparing));
            return if replay {
                Ok(())
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }
        let Some(root) = preparing.root.take() else {
            self.hold(HeldAssignment::Preparing(preparing));
            return Err(AssignmentManagerFailure::ConflictingOffer);
        };
        let offer = preparing.offer.clone();
        let Some(deadline) = PreparationDeadline::from_wire(
            &prepare.preparation_expires_at,
            self.sleeper.utc_now(),
            self.sleeper.now(),
        ) else {
            preparing.cancellation.cancel();
            self.finish_offered_run(&offer.assignment_id, &preparing.run_event, "aborted");
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
            if let Some(event) = preparing.preparation_event.take() {
                event.finish(TelemetryOutcome::Failure);
            }
            self.finish_offered_run(&offer.assignment_id, &preparing.run_event, "aborted");
            self.begin_assignment_cleanup(
                offer.assignment_id,
                root,
                ProcessQuiescence::Proven,
                ReleaseAfter::Idle,
            );
            return Err(AssignmentManagerFailure::DecisionCapacity);
        }
        // Checkout begins once Cloud can see its progress.
        preparing.run_event.enter(RunPhase::Checkout);
        preparing.prepare_effect_id = Some(prepare.effect_id.clone());
        self.hold(HeldAssignment::Preparing(preparing));
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

    pub(super) fn finish_preparation_event(
        &mut self,
        assignment_id: &str,
        outcome: TelemetryOutcome,
    ) {
        if let Some(preparing) = self.preparing_mut(assignment_id)
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
        self.finish_preparation_event(&offer.assignment_id, TelemetryOutcome::Failure);
        let run_event = self
            .preparing(&offer.assignment_id)
            .map_or_else(RunEvent::default, |preparing| preparing.run_event.clone());
        run_event.leave(PhaseEnd::Failed);
        self.reject_offered_run(&offer.assignment_id, &run_event, &decline);
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

    /// The other tree runs the manager holds, as the agent-login pass of the admission of
    /// `assignment_id` reads them without the manager's lock (REQ-GRANT-49, INV-RUNNER-15):
    /// the activity of each `Running` tree run but that assignment. The offer rule took the
    /// offer only while each held assignment waited, and none is taken while this one is
    /// prepared, so the set only shrinks; an ended assignment that only reports or releases
    /// holds no process and is not in it.
    fn runs_beside(&self, assignment_id: &str) -> HeldRuns {
        HeldRuns::Waiting(
            self.held
                .iter()
                .filter(|(id, _)| id.as_str() != assignment_id)
                .filter_map(|(_, held)| match held {
                    HeldAssignment::Running(running) => running.activity.clone(),
                    _ => None,
                })
                .collect(),
        )
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
        let tree_run = self.tree_run.clone();
        let mut runtime = self.admission_runtime(&offer.assignment_id);
        let agent_login_directory = self.agent_login_directory.clone();
        let agent_login_setup = self.agent_login_setup.clone();
        let runs_beside = self.runs_beside(&offer.assignment_id);
        let grant_token_refresh = self.grant_token_refresh.clone();
        let sender = self.event_sender.clone();
        let wake = self.outbox.clone();
        if let Some(preparing) = self.preparing_mut(&offer.assignment_id) {
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
            // The admission sets agent logins up for itself first, a
            // continuation's included, so it is offered a replaced generation
            // and nothing of a revoked login; nothing has been offered yet,
            // and nothing is installed or removed once it has. Beside other
            // runs it asks Cloud only while each waits, and installs or
            // removes only while none also has a command in flight, holding
            // their activities and never the manager's lock; otherwise it is
            // offered what is on disk (REQ-GRANT-49, INV-RUNNER-15). Then it
            // asks Cloud for its assignment's grant tokens, whatever the
            // gate answered, now that Cloud counts the assignment as
            // preparing, as for its source credential, and keeps them
            // until the assignment ends (REQ-GRANT-86, INV-RUNNER-28),
            // keeping each file current while it runs (REQ-GRANT-89).
            // Whatever the setup meets, a panic included, the admission goes
            // on with what is on disk. The worker is the hold's only owner
            // until it publishes the admission's outcome, which carries it.
            let grant_tokens = agent_login_setup.as_ref().and_then(|setup| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    setup.set_up(
                        &runs_beside,
                        &worker_offer.run_id,
                        &worker_offer.assignment_id,
                    )
                }))
                .ok()
                .flatten()
                .map(|mut tokens| {
                    if let Some(refreshing) = &grant_token_refresh {
                        refreshing.start(&mut tokens);
                    }
                    tokens
                })
            });
            // The steps draw from the process environment with the agent
            // logins the runner keeps now, looked at for this admission
            // (REQ-GRANT-37, REQ-GRANT-44), and a tree run's `useful` commands
            // with those it keeps as each starts (REQ-GRANT-49), where the
            // service has none of its own; where the runner keeps
            // none either, nothing is added or withheld (REQ-GRANT-51,
            // INV-RUNNER-17). The admission's grant tokens and their files
            // are offered too; it holds none of a provider whose names the
            // service carries, which keeps the service's values, and a
            // provider neither provides has its names unset (REQ-GRANT-87).
            let offered = crate::agent_login::offer_with_grant_tokens(
                &runtime.environment,
                agent_login_directory.as_ref(),
                grant_tokens.as_ref(),
            );
            // The admission reports where each provider's credential comes
            // from to Cloud once, before any step starts, from what the
            // offer settled, each token service Cloud listed that has a
            // source included (REQ-GRANT-93, REQ-INTEGRATION-12,
            // INV-RUNNER-31). Whatever Cloud
            // answers, a panic included, the admission goes on with the
            // offer as it is; a report Cloud does not take is the setup's
            // own record (REQ-TELEMETRY-48).
            if let Some(setup) = &agent_login_setup {
                let sources = crate::agent_login::reported_sources(&offered, grant_tokens.as_ref());
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    setup.report_sources(&worker_offer.assignment_id, sources);
                }));
            }
            runtime.environment = offered.environment;
            // Each command of a tree run builds its own environment from the same process
            // environment and directory when it starts, keeping the token variables this
            // offer added (REQ-GRANT-49, REQ-GRANT-87).
            runtime.command_environment = offered.commands;
            // How the provider answers each agent step given a login the
            // runner added is noted for the next pass, never for a variable
            // the service carried (REQ-GRANT-52).
            runtime.harness_login_observer = agent_login_setup
                .as_ref()
                .and_then(|setup| setup.observer(offered.offered));
            if let Some(tree) = &worker_offer.execution_spec.workflow_tree {
                let mut root = root;
                let preparation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let admission = TreeAdmission {
                        runtime: &runtime,
                        tree_run: &tree_run,
                        broker: Arc::clone(&broker),
                        input_broker: input_broker.as_deref(),
                        environment: &environment,
                        offer: &worker_offer,
                        tree,
                        deadline,
                        cancellation: &worker_cancellation,
                        sleeper: Arc::clone(&sleeper),
                    };
                    match &worker_offer.continuation {
                        Some(continuation) => admission.adopt(&mut root, continuation),
                        None => admission.prepare(&mut root),
                    }
                }));
                let admission = match preparation {
                    Ok(Ok(prepared)) => runtime.finish_tree(
                        &worker_offer,
                        root,
                        prepared,
                        PreparationAuthority {
                            deadline,
                            cancellation: &worker_cancellation,
                            monotonic_now: sleeper.now(),
                        },
                    ),
                    Ok(Err(decline)) => Err(Box::new((root, decline))),
                    Err(_) => Err(Box::new((root, environment_unavailable()))),
                };
                let _ = sender.send(ManagerEvent::Prepared {
                    offer: Box::new(worker_offer),
                    prepare_effect_id,
                    deadline,
                    admission: Box::new(handing_over(admission, grant_tokens)),
                });
                wake.wake();
                return;
            }
            let mut root = root;
            let workspace_path = root.workspace.path();
            let preparation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Some(continuation) = &worker_offer.continuation {
                    runtime.prove_continuation(
                        &worker_offer,
                        continuation,
                        &mut root,
                        deadline,
                        &worker_cancellation,
                        sleeper.as_ref(),
                    )?;
                }
                let separate_definition = worker_offer.execution_spec.schema_version == 3;
                let clone_root = if worker_offer.continuation.is_some() || separate_definition {
                    let path = root.private.join("replacement-definition");
                    std::fs::create_dir(&path).map_err(|error| {
                        runtime.preparation_failure(
                            &worker_offer,
                            "replacement_definition_root",
                            io_error_kind(&error),
                        );
                        environment_unavailable()
                    })?;
                    path
                } else {
                    workspace_path.clone()
                };
                let checkout = super::super::source::checkout(
                    Arc::clone(&broker),
                    &environment,
                    &worker_offer.assignment_id,
                    &source,
                    &worker_cancellation,
                    &clone_root,
                    &root.private,
                )
                .map_err(|failure| {
                    if worker_offer.continuation.is_some() {
                        runtime.preparation_failure(
                            &worker_offer,
                            "continuation_source_checkout",
                            materialization_failure_name(failure),
                        );
                    }
                    materialization_decline(failure)
                })?;
                if separate_definition && worker_offer.continuation.is_none() {
                    let workspace_source = source.for_workspace(
                        &worker_offer
                            .execution_spec
                            .primary_workspace_source
                            .commit_oid,
                    );
                    super::super::source::checkout(
                        Arc::clone(&broker),
                        &environment,
                        &worker_offer.assignment_id,
                        &workspace_source,
                        &worker_cancellation,
                        &workspace_path,
                        &root.private,
                    )
                    .map_err(materialization_decline)?;
                }
                runtime.run_event.enter(RunPhase::Preparation);
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
                let mut materialized =
                    super::super::source::resolve_checkout(checkout, &worker_cancellation)
                        .map_err(materialization_decline)?;
                if separate_definition {
                    // Both initial admission and continuation resolve immutable
                    // definition bytes in private, then execute only in the claimed
                    // primary workspace. Never execute a resumed run in the
                    // disposable definition checkout.
                    std::fs::remove_dir_all(&clone_root).map_err(|_| environment_unavailable())?;
                    materialized.execution_root = workspace_path.clone();
                    materialized.git_capture =
                        materialized.workflow.requires_git_capture().then(|| {
                            CloudGitCaptureProjection::new(
                                Arc::from(
                                    worker_offer
                                        .execution_spec
                                        .primary_workspace_source
                                        .commit_oid
                                        .as_str(),
                                ),
                                Arc::from(
                                    worker_offer
                                        .execution_spec
                                        .workflow_definition_source
                                        .workflow_source_closure_digest
                                        .value
                                        .as_str(),
                                ),
                                worker_cancellation.clone(),
                            )
                        });
                }
                Ok::<_, AssignmentDecline>((materialized, inputs))
            }));
            let admission = match preparation {
                Ok(Ok((mut materialized, inputs))) => {
                    if worker_offer.continuation.is_some() {
                        // The fetched effective definition has its own source root;
                        // only execution adopts the retained primary work tree.
                        materialized.execution_root = root.execution.clone();
                        materialized.git_capture =
                            materialized.workflow.requires_git_capture().then(|| {
                                CloudGitCaptureProjection::for_continuation(
                                    Arc::from(
                                        worker_offer
                                            .execution_spec
                                            .primary_workspace_source
                                            .commit_oid
                                            .as_str(),
                                    ),
                                    Arc::from(materialized.workflow.content_digest.value.as_str()),
                                    worker_cancellation.clone(),
                                )
                            });
                    } else {
                        root.execution = materialized.execution_root;
                    }
                    let workflow_git = std::env::current_exe()
                        .map_err(anyhow::Error::from)
                        .and_then(|helper_executable| {
                            WorkflowGitAuthority::install(WorkflowGitInstall {
                                broker: Arc::clone(&broker),
                                assignment_id: &worker_offer.assignment_id,
                                origin: materialized.origin,
                                canonical_origin: materialized.canonical_origin,
                                replace_origin: worker_offer.continuation.is_some(),
                                previous_private_root: worker_offer
                                    .continuation
                                    .as_ref()
                                    .and_then(|offer| {
                                        root.retained_private_roots.get(&offer.prior_attempt_id)
                                    })
                                    .map(|path| path.as_path()),
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
                            if worker_offer.continuation.is_none() {
                                // Ordinary execution uses the clone as its workspace;
                                // workflow-Git teardown now owns origin restoration.
                                if let Some(mut cleanup) = materialized.origin_cleanup.take() {
                                    cleanup.disarm();
                                }
                            }
                            // A continuation fetched its definition into private
                            // replacement-definition, not the retained workspace.
                            // Its preparation guard must restore that clone's
                            // origin; workflow-Git teardown owns only the retained
                            // primary worktree's newly installed relay wiring.
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
                admission: Box::new(handing_over(admission, grant_tokens)),
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
            || self.held.values().any(|held| {
                matches!(held,
                HeldAssignment::Preparing(preparing)
                    if preparing.offer.effect_id == cancel.effect_id
                        || preparing.prepare_effect_id.as_ref() == Some(&cancel.effect_id))
            })
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
        if let Some(preparing) = self.preparing(&cancel.assignment_id) {
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
            let assignment_id = cancel.assignment_id.clone();
            self.retain_cancellation(cancel, Some(application), false)?;
            let Some(preparing) = self.preparing_mut(&assignment_id) else {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            };
            preparing.cancellation.cancel();
            if let Some(event) = preparing.preparation_event.take() {
                event.finish(TelemetryOutcome::Cancelled);
            }
            if preparing.root.is_some() && preparing.prepare_effect_id.is_none() {
                let Some(mut preparing) = self.take_held(&assignment_id, HeldAssignment::preparing)
                else {
                    return Err(AssignmentManagerFailure::ConflictingOffer);
                };
                let Some(root) = preparing.root.take() else {
                    self.hold(HeldAssignment::Preparing(preparing));
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
        if let Some(HeldAssignment::Releasing(releasing)) = self.held.get(&cancel.assignment_id)
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
        let held = match self.held.get(&cancel.assignment_id) {
            Some(
                HeldAssignment::Accepted(_)
                | HeldAssignment::Running(_)
                | HeldAssignment::Finishing(_),
            ) => true,
            Some(HeldAssignment::Releasing(releasing)) => {
                accepted_target
                    || matches!(&releasing.after, ReleaseAfter::PreExecutionCancellation(_))
            }
            Some(HeldAssignment::Preparing(_)) | None => false,
        };
        if !held {
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
        match self.held.remove(&cancel.assignment_id) {
            Some(HeldAssignment::Accepted(accepted)) => {
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
            Some(HeldAssignment::Running(running)) => {
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
                self.hold(HeldAssignment::Running(running));
                if self.cancellations[index].ready {
                    self.emit_cancellation_application(index)?;
                }
            }
            Some(HeldAssignment::Finishing(finishing)) => {
                let application = cancellation_application(
                    &cancel,
                    effective_mode,
                    cancellation_disposition(cancel.mode, effective_mode, true),
                );
                let index = self.retain_cancellation(cancel, Some(application), true)?;
                self.hold(HeldAssignment::Finishing(finishing));
                self.emit_cancellation_application(index)?;
            }
            Some(HeldAssignment::Releasing(releasing)) => {
                let pre_execution =
                    matches!(&releasing.after, ReleaseAfter::PreExecutionCancellation(_));
                let application = cancellation_application(
                    &cancel,
                    effective_mode,
                    cancellation_disposition(cancel.mode, effective_mode, !pre_execution),
                );
                let index = self.retain_cancellation(cancel, Some(application), !pre_execution)?;
                self.hold(HeldAssignment::Releasing(releasing));
                if self.cancellations[index].ready {
                    self.emit_cancellation_application(index)?;
                }
            }
            held => {
                if let Some(held) = held {
                    self.hold(held);
                }
                self.retain_cancellation(cancel, None, true)?;
            }
        }
        Ok(())
    }
}

/// `admission`, as an admission's worker publishes it, with the grant token
/// hold the worker alone owned until then: handed to the accepted assignment,
/// or ended with a declined one. Once the outcome is published nothing of the
/// worker holds the tokens, so the manager letting go of the assignment ends
/// its refresh and removes its directory whatever the worker still does
/// (INV-RUNNER-28).
fn handing_over(
    admission: Result<AcceptedAssignment, Box<(AssignmentRoot, AssignmentDecline)>>,
    grant_tokens: Option<AssignmentGrantTokens>,
) -> Result<AcceptedAssignment, Box<(AssignmentRoot, AssignmentDecline)>> {
    match admission {
        Ok(accepted) => Ok(AcceptedAssignment {
            grant_tokens,
            ..accepted
        }),
        Err(declined) => {
            drop(grant_tokens);
            Err(declined)
        }
    }
}

impl AdmissionRuntime {
    /// Proves the retained workspace a continuation offer names (REQ-CONTINUE-47 to 49),
    /// on the calling blocking worker: the offer's boot is this one, the retained chain
    /// is claimed for the new attempt and quiesced, the workspace is snapshotted, and
    /// `continuation_ready` is sent and acknowledged before preparation goes on.
    fn prove_continuation(
        &self,
        offer: &AssignmentOffer,
        continuation: &ContinuationOffer,
        root: &mut AssignmentRoot,
        deadline: PreparationDeadline,
        cancellation: &CaptureCancellation,
        sleeper: &dyn Sleeper,
    ) -> Result<(), AssignmentDecline> {
        if self.work_root.boot_id() != Some(continuation.required_runner_boot_id.as_str()) {
            self.preparation_failure(offer, "continuation_boot_identity", "boot_mismatch");
            return Err(environment_unavailable());
        }
        self.work_root
            .claim_retained_for_attempt(
                root,
                super::super::workspace::RetainedClaimRequest {
                    assignment_id: &offer.assignment_id,
                    run_id: &offer.run_id,
                    attempt_id: &offer.attempt_id,
                    prior_assignment_id: &continuation.prior_assignment_id,
                    prior_attempt_id: &continuation.prior_attempt_id,
                    recorded_root: Path::new(&continuation.execution_root),
                },
            )
            .inspect_err(|&error| {
                self.preparation_failure(
                    offer,
                    "continuation_retained_claim",
                    match error {
                        AssignmentRootCreationError::OwnershipUnproven => "ownership_unproven",
                        AssignmentRootCreationError::Unavailable => "unavailable",
                        AssignmentRootCreationError::CleanupFailed => "cleanup_failed",
                        AssignmentRootCreationError::RetainedClaimCleanup(site) => site,
                    },
                );
            })
            .map_err(|error| match error {
                AssignmentRootCreationError::OwnershipUnproven => {
                    AssignmentDecline::RunnerUnable(RunnerUnableReason::OwnershipUnproven)
                }
                AssignmentRootCreationError::Unavailable => AssignmentDecline::RunnerUnable(
                    RunnerUnableReason::RetainedWorkspaceUnavailable,
                ),
                _ => environment_unavailable(),
            })?;
        // Snapshot after the durable claim, on this blocking worker.
        // An unavailable snapshot is explicit evidence, not a reason
        // to substitute null or to read the unclaimed workspace.
        root.continuation_snapshot = Some(
            useful_execution::capture_cloud_continuation_snapshot(
                &root.execution,
                continuation.prior_settlement_snapshot.as_ref(),
            )
            .map_err(|_| {
                self.preparation_failure(offer, "continuation_snapshot", "capture_failed");
                environment_unavailable()
            })?,
        );
        let proof = root.retained_quiescence.ok_or_else(|| {
            self.preparation_failure(offer, "continuation_quiescence", "proof_absent");
            environment_unavailable()
        })?;
        let snapshot = root
            .continuation_snapshot
            .as_ref()
            .ok_or_else(environment_unavailable)?;
        let proven_at = sleeper
            .utc_now()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| {
                self.preparation_failure(offer, "continuation_clock", "format_failed");
                environment_unavailable()
            })?;
        root.continuation_proven_at = Some(proven_at.clone());
        let id = self
            .outbox
            .enqueue(AssignmentObservation::ContinuationReady {
                assignment_id: offer.assignment_id.clone(),
                attempt_id: offer.attempt_id.clone(),
                start_snapshot: snapshot.start_snapshot.clone(),
                quiescence: serde_json::json!({
                    "groupsRecorded": proof.recorded,
                    "groupsTerminated": proof.terminated,
                    "groupsAbsent": proof.absent,
                    "provenAt": proven_at,
                }),
                modified: snapshot.modified.clone(),
            })
            .map_err(|_| {
                self.preparation_failure(offer, "continuation_ready_outbox", "enqueue_failed");
                environment_unavailable()
            })?;
        tokio::runtime::Handle::current().block_on(self.outbox.wait_for_continuation_ready(
            id,
            deadline,
            cancellation,
            sleeper,
        ))?;
        Ok(())
    }
}

/// The preparation of a tree run (REQ-RUNNERENROLL-91): the workspace checkout and run
/// inputs as for any run, then the published tree in place of the Git definition
/// checkout and workflow admission and, on a GitHub project, the workspace's Git relay
/// (REQ-RUNNERENROLL-121); or, for a continuation, the adoption of the run an earlier
/// attempt created (REQ-RUNNERENROLL-89).
struct TreeAdmission<'a> {
    runtime: &'a AdmissionRuntime,
    tree_run: &'a TreeRunDependencies,
    broker: Arc<dyn SourceCredentialBroker>,
    input_broker: Option<&'a dyn RunInputBroker>,
    /// The service environment the checkout uses; each of the tree's `useful` commands runs
    /// with the environment `runtime.command_environment` builds when it starts.
    environment: &'a EnvironmentSnapshot,
    offer: &'a AssignmentOffer,
    tree: &'a useful_runner_protocol::WorkflowTreeV1RunnerProjection,
    deadline: PreparationDeadline,
    cancellation: &'a CaptureCancellation,
    sleeper: Arc<dyn Sleeper>,
}

impl TreeAdmission<'_> {
    fn prepare(
        self,
        root: &mut AssignmentRoot,
    ) -> Result<super::super::tree_run::TreeRun, AssignmentDecline> {
        let offer = self.offer;
        let spec = &offer.execution_spec;
        let workspace = root.workspace.path();
        let source = super::super::source::MaterializationRequest::from_validated(spec)
            .for_workspace(&spec.primary_workspace_source.commit_oid);
        let checkout = super::super::source::checkout(
            Arc::clone(&self.broker),
            self.environment,
            &offer.assignment_id,
            &source,
            self.cancellation,
            &workspace,
            &root.private,
        )
        .map_err(materialization_decline)?;
        // A GitHub checkout's origin is the repository, which the runtime's steps read
        // through the runner's relay (REQ-RUNNERENROLL-121). Dropping a GitLab checkout
        // restores the clone's canonical origin, and its steps use the host's own Git
        // access (REQ-WORKSPACE-42).
        let relay_origin = checkout.relay_origin();
        drop(checkout);
        root.execution.clone_from(&workspace);
        self.runtime.run_event.enter(RunPhase::Preparation);
        self.runtime.progress(offer, 2, "input_download")?;
        let inputs = super::super::run_inputs::materialize(
            self.input_broker,
            &offer.assignment_id,
            &spec.execution_spec_id,
            spec.run_inputs.as_ref(),
            self.deadline,
            self.cancellation,
            &root.private,
        )
        .map_err(run_input_decline)?;
        self.runtime.progress(offer, 3, "workflow_admission")?;
        let prepared = super::super::tree_run::prepare(
            self.tree_run,
            &super::super::tree_run::TreePreparation {
                assignment_id: &offer.assignment_id,
                execution_spec: spec,
                tree: self.tree,
                private: &root.private,
                workspace: &workspace,
                environment: &self.runtime.command_environment,
                deadline: self.deadline,
                cancellation: self.cancellation,
                sleeper: Arc::clone(&self.sleeper),
            },
            &inputs,
        )?;
        let Some(origin) = relay_origin else {
            return Ok(prepared);
        };
        // Installed once the tree is published, as the engine path installs after
        // `resolve_checkout`; a continuation, which clones nothing, reads the origin back.
        super::super::tree_run::record_origin(&root.private, &origin).map_err(|error| {
            self.runtime.preparation_failure(
                offer,
                "workflow_git_origin_record",
                io_error_kind(&error),
            );
            environment_unavailable()
        })?;
        self.install_relay(root, origin)?;
        Ok(prepared)
    }

    /// Installs the workspace's Git relay for `origin` in the execution root
    /// (REQ-RUNNERENROLL-121, REQ-RUNNERENROLL-122), leaving the workspace's remotes as they
    /// read; `finish_tree` carries it and `run_tree` activates it.
    fn install_relay(
        &self,
        root: &mut AssignmentRoot,
        origin: Arc<str>,
    ) -> Result<(), AssignmentDecline> {
        let workflow_git = self
            .tree_run
            .helper_executable()
            .map_err(anyhow::Error::from)
            .and_then(|helper_executable| {
                WorkflowGitAuthority::install(WorkflowGitInstall {
                    broker: Arc::clone(&self.broker),
                    assignment_id: &self.offer.assignment_id,
                    origin,
                    canonical_origin: None,
                    replace_origin: false,
                    previous_private_root: None,
                    workspace: &root.execution,
                    private_root: &root.private,
                    environment: self.environment,
                    helper_executable: &helper_executable,
                    clock: Arc::clone(&self.sleeper),
                    recorder: self.runtime.recorder.clone(),
                    cancellation: self.cancellation,
                })
            })
            .map_err(|_| {
                self.runtime.preparation_failure(
                    self.offer,
                    "workflow_git_install",
                    "install_failed",
                );
                environment_unavailable()
            })?;
        root.install_workflow_git(workflow_git);
        Ok(())
    }

    /// Proves the retained workspace as for any continuation, then adopts the run in the
    /// run-scoped home of the one attempt of the retained chain that holds one, the
    /// attempt that created the run: no checkout, no run input and no tree download. The
    /// run executes in the retained workspace its record names, which the claim proved,
    /// with the workspace's Git relay for the origin that attempt recorded, when it
    /// recorded one (REQ-RUNNERENROLL-122).
    fn adopt(
        self,
        root: &mut AssignmentRoot,
        continuation: &ContinuationOffer,
    ) -> Result<super::super::tree_run::TreeRun, AssignmentDecline> {
        let offer = self.offer;
        self.runtime.prove_continuation(
            offer,
            continuation,
            root,
            self.deadline,
            self.cancellation,
            self.sleeper.as_ref(),
        )?;
        self.runtime.run_event.enter(RunPhase::Preparation);
        self.runtime.progress(offer, 2, "workflow_admission")?;
        let creators = root
            .retained_private_roots
            .values()
            .filter(|private| {
                std::fs::symlink_metadata(super::super::tree_run::tree_home(private))
                    .is_ok_and(|home| home.is_dir())
            })
            .collect::<Vec<_>>();
        let [creator] = creators.as_slice() else {
            self.runtime.preparation_failure(
                offer,
                "continuation_tree_home",
                if creators.is_empty() {
                    "absent"
                } else {
                    "ambiguous"
                },
            );
            return Err(AssignmentDecline::RunnerUnable(
                RunnerUnableReason::RetainedWorkspaceUnavailable,
            )
            .diagnosed("workflow_tree_adoption", "tree_run_home_unavailable"));
        };
        // Only a GitHub project's first attempt records its origin. The workspace's own
        // remotes are not read: a step may have rewritten or added them.
        let origin = super::super::tree_run::recorded_origin(creator).map_err(|error| {
            self.runtime.preparation_failure(
                offer,
                "continuation_tree_origin",
                io_error_kind(&error),
            );
            environment_unavailable()
        })?;
        let run = super::super::tree_run::adopt(
            self.tree_run,
            &super::super::tree_run::TreeAdoption {
                assignment_id: &offer.assignment_id,
                home: &super::super::tree_run::tree_home(creator),
                private: &root.private,
                environment: &self.runtime.command_environment,
                execution_spec: &offer.execution_spec,
                sleeper: Arc::clone(&self.sleeper),
                steps: &continuation.reexecuted_steps,
                pending_deliveries: &self.tree.pending_deliveries,
            },
        )?;
        if let Some(origin) = origin {
            self.install_relay(root, origin)?;
        }
        Ok(run)
    }
}
