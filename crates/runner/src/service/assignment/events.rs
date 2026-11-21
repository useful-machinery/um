use super::*;

// Decide the terminal path without inspecting mutable manager state or reading a clock.
// The caller samples and clamps the deadline only when containment is proven and a
// report has been selected; failed containment has its own acknowledgement path.
#[derive(Debug, PartialEq)]
enum FinishedDisposition {
    ContainmentFailed,
    PreExecutionCancellation,
    WithoutReport,
    RetainAfterClockFailure(Option<LeaseClockError>),
    AwaitAcknowledgement(u64, LeaseInstant),
}

fn finished_disposition(
    quiescence: ProcessQuiescence,
    report: Option<u64>,
    lease_clock_failed: bool,
    pending_pre_execution_cancellation: bool,
    deadline: Option<Result<(LeaseInstant, bool), LeaseClockError>>,
) -> FinishedDisposition {
    if quiescence == ProcessQuiescence::Failed {
        return FinishedDisposition::ContainmentFailed;
    }
    let Some(id) = report else {
        return if !lease_clock_failed && pending_pre_execution_cancellation {
            FinishedDisposition::PreExecutionCancellation
        } else {
            FinishedDisposition::WithoutReport
        };
    };
    if lease_clock_failed {
        return FinishedDisposition::RetainAfterClockFailure(None);
    }
    match deadline {
        Some(Ok((deadline, true))) => FinishedDisposition::AwaitAcknowledgement(id, deadline),
        Some(Err(error)) => FinishedDisposition::RetainAfterClockFailure(Some(error)),
        Some(Ok((_, false))) | None => FinishedDisposition::WithoutReport,
    }
}

impl AssignmentManager {
    pub(super) fn drain_events(&mut self) {
        self.artifact_delivery.drain_uploads();
        while let Ok(event) = self.events.try_recv() {
            match event {
                ManagerEvent::WorkspacePrepared {
                    assignment_id,
                    root,
                } => {
                    let root = root.map(|root| *root);
                    let Some(mut preparing) =
                        self.take_held(&assignment_id, HeldAssignment::preparing)
                    else {
                        if let Ok(root) = root {
                            release_unclaimed_assignment_root(root);
                        }
                        continue;
                    };
                    preparing.root_preparation = None;
                    match root {
                        Ok(root) if preparing.cancellation.is_cancelled() => {
                            let identity = AssignmentIdentity::from_offer(&preparing.offer);
                            if self.has_pending_pre_execution_cancellation(&assignment_id) {
                                self.begin_assignment_finalization(
                                    assignment_id,
                                    root,
                                    ProcessQuiescence::Proven,
                                    WorkspaceDisposition::Retain(RetentionReason::Cancelled),
                                    ReleaseAfter::PreExecutionCancellation(Box::new(identity)),
                                );
                            } else {
                                self.finish_offered_run(
                                    &assignment_id,
                                    &preparing.run_event,
                                    "aborted",
                                );
                                self.begin_assignment_cleanup(
                                    assignment_id,
                                    root,
                                    ProcessQuiescence::Proven,
                                    ReleaseAfter::Idle,
                                );
                            }
                        }
                        Ok(root) => {
                            let preparation = AssignmentObservation::Preparing {
                                effect_id: preparing.offer.effect_id.clone(),
                                assignment_id: assignment_id.clone(),
                                offered_execution_spec_id: preparing
                                    .offer
                                    .execution_spec
                                    .execution_spec_id
                                    .clone(),
                            };
                            if self.outbox.enqueue(preparation).is_err() {
                                self.finish_offered_run(
                                    &assignment_id,
                                    &preparing.run_event,
                                    "aborted",
                                );
                                self.begin_assignment_cleanup(
                                    assignment_id,
                                    root,
                                    ProcessQuiescence::Proven,
                                    ReleaseAfter::Idle,
                                );
                            } else {
                                preparing.root = Some(root);
                                self.hold(HeldAssignment::Preparing(preparing));
                            }
                        }
                        Err(error) => {
                            let cleanup_failed =
                                error == AssignmentRootCreationError::CleanupFailed;
                            if cleanup_failed {
                                self.cleanup_failed = true;
                            }
                            if !preparing.cancellation.is_cancelled() {
                                let offer = preparing.offer;
                                let decline = match (&offer.continuation, error) {
                                    (Some(_), AssignmentRootCreationError::OwnershipUnproven) => {
                                        AssignmentDecline::RunnerUnable(
                                            RunnerUnableReason::OwnershipUnproven,
                                        )
                                    }
                                    (Some(_), AssignmentRootCreationError::Unavailable) => {
                                        AssignmentDecline::RunnerUnable(
                                            RunnerUnableReason::RetainedWorkspaceUnavailable,
                                        )
                                    }
                                    _ => environment_unavailable(),
                                };
                                self.reject_offered_run(
                                    &assignment_id,
                                    &preparing.run_event,
                                    &decline,
                                );
                                let response = rejected(&offer, decline);
                                if let Err(failure) = self.retain_decision(offer, response) {
                                    self.lease_clock_failed |=
                                        failure == AssignmentManagerFailure::LeaseClock;
                                }
                            } else if !cleanup_failed
                                && self.has_pending_pre_execution_cancellation(&assignment_id)
                            {
                                self.complete_pre_execution_cancellation(
                                    AssignmentIdentity::from_offer(&preparing.offer),
                                );
                            } else {
                                self.finish_offered_run(
                                    &assignment_id,
                                    &preparing.run_event,
                                    "aborted",
                                );
                            }
                            self.outbox.wake();
                        }
                    }
                }
                ManagerEvent::Prepared {
                    offer,
                    prepare_effect_id,
                    deadline,
                    admission,
                } => {
                    let Some(mut preparing) =
                        self.take_held(&offer.assignment_id, HeldAssignment::preparing)
                    else {
                        continue;
                    };
                    if !same_assignment(&preparing.offer, &offer) {
                        self.hold(HeldAssignment::Preparing(preparing));
                        continue;
                    }
                    if preparing.cancellation.is_cancelled() || deadline.remaining().is_none() {
                        if let Some(event) = preparing.preparation_event.take() {
                            event.finish(TelemetryOutcome::Cancelled);
                        }
                        // The deadline fence cancels the same capture as a
                        // gateway cancellation or a shutdown; only those two
                        // leave state of their own behind. A release has
                        // already ended the run, so its leave finds nothing.
                        if self.has_pending_pre_execution_cancellation(&offer.assignment_id)
                            || self.shutting_down
                        {
                            preparing.run_event.leave(PhaseEnd::Cancelled);
                        } else {
                            preparing.run_event.leave(PhaseEnd::TimedOut);
                        }
                        let root = match *admission {
                            Ok(accepted) => accepted.root,
                            Err(failure) => failure.0,
                        };
                        let identity = AssignmentIdentity::from_offer(&offer);
                        if self.has_pending_pre_execution_cancellation(&offer.assignment_id) {
                            self.begin_assignment_finalization(
                                offer.assignment_id.clone(),
                                root,
                                ProcessQuiescence::Proven,
                                WorkspaceDisposition::Retain(RetentionReason::Cancelled),
                                ReleaseAfter::PreExecutionCancellation(Box::new(identity)),
                            );
                        } else {
                            self.finish_offered_run(
                                &offer.assignment_id,
                                &preparing.run_event,
                                "aborted",
                            );
                            self.begin_assignment_cleanup(
                                offer.assignment_id.clone(),
                                root,
                                ProcessQuiescence::Proven,
                                ReleaseAfter::Idle,
                            );
                        }
                        continue;
                    }
                    let end = if admission.is_ok() {
                        PhaseEnd::Succeeded
                    } else {
                        PhaseEnd::Failed
                    };
                    if let Some(event) = preparing.preparation_event.take() {
                        event.finish(end.outcome());
                    }
                    preparing.run_event.leave(end);
                    match *admission {
                        Ok(accepted) => {
                            self.hold(HeldAssignment::Accepted(Box::new(accepted)));
                            if offer.continuation.is_some() {
                                self.runner_events.record(LoggedEvent::RunContinued {
                                    run_id: offer.run_id.clone(),
                                    assignment_id: offer.assignment_id.clone(),
                                });
                            }
                            let response = AssignmentDecision::Accepted {
                                effect_id: prepare_effect_id.clone(),
                                assignment_id: offer.assignment_id.clone(),
                                offered_execution_spec_id: offer
                                    .execution_spec
                                    .execution_spec_id
                                    .clone(),
                            };
                            if let Err(failure) = self.retain_decision(*offer, response) {
                                self.lease_clock_failed |=
                                    failure == AssignmentManagerFailure::LeaseClock;
                                if let Some(accepted) = self.take_held(
                                    &preparing.offer.assignment_id,
                                    HeldAssignment::accepted,
                                ) {
                                    self.finish_offered_run(
                                        accepted.assignment_id(),
                                        &preparing.run_event,
                                        "aborted",
                                    );
                                    self.begin_assignment_cleanup(
                                        accepted.identity.assignment_id.clone(),
                                        accepted.root,
                                        ProcessQuiescence::Proven,
                                        ReleaseAfter::Idle,
                                    );
                                }
                            }
                        }
                        Err(failure) => {
                            let (root, decline) = *failure;
                            self.reject_offered_run(
                                &offer.assignment_id,
                                &preparing.run_event,
                                &decline,
                            );
                            let response = AssignmentDecision::Rejected {
                                effect_id: prepare_effect_id,
                                assignment_id: offer.assignment_id.clone(),
                                decline,
                            };
                            if let Err(failure) = self.retain_decision(*offer, response) {
                                self.lease_clock_failed |=
                                    failure == AssignmentManagerFailure::LeaseClock;
                            }
                            self.begin_assignment_cleanup(
                                preparing.offer.assignment_id,
                                root,
                                ProcessQuiescence::Proven,
                                ReleaseAfter::Idle,
                            );
                        }
                    }
                }
                ManagerEvent::WorkspaceReleased {
                    assignment_id,
                    result,
                } => {
                    if let Some(running) = self.running_mut(&assignment_id) {
                        running.workspace_release = Some(result);
                    }
                }
                ManagerEvent::Finished {
                    assignment_id,
                    final_observation_id,
                    final_delivery_deadline,
                    lease_clock_failed,
                    fenced,
                    retained_root,
                    quiescence,
                    quiescence_failure,
                    workspace_disposition,
                } => {
                    // A delayed completion must not discard the assignment in another
                    // phase of its lifecycle, nor another assignment.
                    let Some(running) = self.take_held(&assignment_id, HeldAssignment::running)
                    else {
                        continue;
                    };
                    let reported_wait = running.reported_wait();
                    self.keep_settled_run(&running);
                    if running
                        .run_event
                        .interrupted_for(CancellationReason::ExecutionLeaseExpired.as_str())
                    {
                        self.record_lease_lost(&running.identity);
                    }
                    let identity = running.identity;
                    let retained_root = retained_root.map(|root| *root);
                    let deadline_status = if quiescence != ProcessQuiescence::Failed
                        && final_observation_id.is_some()
                        && !lease_clock_failed
                    {
                        final_delivery_deadline.map(|deadline| {
                            self.clamp_to_shutdown_cleanup_deadline(deadline)
                                .and_then(|deadline| {
                                    self.lease_clock.now().and_then(|now| {
                                        now.checked_cmp(deadline).map(|order| {
                                            (deadline, order == std::cmp::Ordering::Less)
                                        })
                                    })
                                })
                        })
                    } else {
                        None
                    };
                    let disposition = finished_disposition(
                        quiescence,
                        final_observation_id,
                        lease_clock_failed,
                        self.has_pending_pre_execution_cancellation(&assignment_id),
                        deadline_status,
                    );
                    match disposition {
                        FinishedDisposition::ContainmentFailed => {
                            // Unproven process containment permanently fences this boot from
                            // admitting another assignment, even after its roots are retained.
                            self.cleanup_failed = true;
                            self.quiescence_failure = quiescence_failure;
                            // A transport send only marks the terminal frame encoded. Keep
                            // the fenced boot alive for its Cloud acknowledgement (and
                            // reconnect/replay) until the existing delivery deadline.
                            if let (Some(id), Some(deadline)) =
                                (final_observation_id, final_delivery_deadline)
                                && !lease_clock_failed
                            {
                                let pending_deadline = self
                                    .clamp_to_shutdown_cleanup_deadline(deadline)
                                    .ok()
                                    .filter(|deadline| {
                                        self.lease_clock
                                            .now()
                                            .and_then(|now| now.checked_cmp(*deadline))
                                            .is_ok_and(|order| order == std::cmp::Ordering::Less)
                                    });
                                if let Some(deadline) = pending_deadline {
                                    self.cleanup_failure_report = Some(id);
                                    if self
                                        .start_final_grace(
                                            assignment_id.clone(),
                                            id,
                                            deadline,
                                            false,
                                        )
                                        .is_err()
                                    {
                                        self.cleanup_failure_report = None;
                                        self.lease_clock_failed = true;
                                        self.retire_assignment_observations(&assignment_id);
                                    }
                                } else {
                                    self.retire_assignment_observations(&assignment_id);
                                }
                            }
                            let after = if final_observation_id.is_some() {
                                ReleaseAfter::Reporting(Box::new(ReportingAssignment {
                                    identity,
                                    reported_wait,
                                }))
                            } else {
                                self.finish_run_event(
                                    &assignment_id,
                                    Some(if fenced { "fenced" } else { "aborted" }),
                                );
                                self.retire_assignment_observations(&assignment_id);
                                ReleaseAfter::Idle
                            };
                            if let Some(root) = retained_root {
                                self.begin_assignment_finalization(
                                    assignment_id,
                                    root,
                                    ProcessQuiescence::Failed,
                                    workspace_disposition,
                                    after,
                                );
                            } else if let ReleaseAfter::Reporting(identity) = after {
                                self.report(*identity);
                            }
                        }
                        FinishedDisposition::PreExecutionCancellation => {
                            if let Some(root) = retained_root {
                                self.begin_assignment_finalization(
                                    assignment_id,
                                    root,
                                    quiescence,
                                    workspace_disposition,
                                    ReleaseAfter::PreExecutionCancellation(Box::new(identity)),
                                );
                            } else {
                                self.complete_pre_execution_cancellation(identity);
                            }
                        }
                        FinishedDisposition::WithoutReport => {
                            self.lease_clock_failed |= lease_clock_failed;
                            self.finish_without_reporting(
                                assignment_id,
                                retained_root,
                                quiescence,
                                workspace_disposition,
                                fenced,
                            );
                        }
                        FinishedDisposition::RetainAfterClockFailure(error) => {
                            if let Some(error) = error {
                                self.classify_lease_clock(
                                    &assignment_id,
                                    error,
                                    "terminal_acknowledgement",
                                );
                            }
                            // Only a selected report can enter this disposition.
                            if let Some(id) = final_observation_id {
                                self.retain_after_lease_clock_failure(
                                    assignment_id,
                                    retained_root,
                                    quiescence,
                                    workspace_disposition,
                                    ReportingAssignment {
                                        identity,
                                        reported_wait,
                                    },
                                    id,
                                );
                            }
                        }
                        FinishedDisposition::AwaitAcknowledgement(id, deadline) => {
                            self.hold(HeldAssignment::Finishing(Box::new(FinishingAssignment {
                                identity: identity.clone(),
                                final_observation_id: id,
                                root: retained_root,
                                workspace_disposition,
                                reported_wait,
                            })));
                            if self
                                .start_final_grace(identity.assignment_id, id, deadline, false)
                                .is_err()
                            {
                                self.lease_clock_failed = true;
                            }
                        }
                    }
                }
                ManagerEvent::FinalGraceElapsed {
                    assignment_id,
                    final_observation_id,
                    continue_reporting,
                } => {
                    if self.cleanup_failure_report == Some(final_observation_id) {
                        self.cleanup_failure_report = None;
                        self.retire_assignment_observations(&assignment_id);
                        self.reporting.remove(&assignment_id);
                        continue;
                    }
                    if self.fenced_final_graces.remove(&FencedFinalGrace {
                        assignment_id: assignment_id.clone(),
                        final_observation_id,
                    }) {
                        // The terminal report was fenced on successor admission;
                        // this timer cannot resume predecessor reporting.
                        continue;
                    }
                    // Acknowledgement can release the assignment before its grace timer
                    // fires. Preserve whatever is held.
                    let Some(finishing) = self.take_held(&assignment_id, HeldAssignment::finishing)
                    else {
                        continue;
                    };
                    if finishing.final_observation_id == final_observation_id {
                        let workspace_disposition = finishing.workspace_disposition;
                        let after = if continue_reporting {
                            ReleaseAfter::Reporting(Box::new(ReportingAssignment {
                                identity: finishing.identity,
                                reported_wait: finishing.reported_wait,
                            }))
                        } else {
                            self.finish_run_event(&assignment_id, Some("aborted"));
                            self.retire_assignment_observations(&assignment_id);
                            ReleaseAfter::Idle
                        };
                        if let Some(root) = finishing.root {
                            self.begin_assignment_finalization(
                                assignment_id,
                                root,
                                ProcessQuiescence::Proven,
                                workspace_disposition,
                                after,
                            );
                        } else if let ReleaseAfter::Reporting(identity) = after {
                            self.report(*identity);
                        }
                    } else {
                        self.hold(HeldAssignment::Finishing(finishing));
                    }
                }
                ManagerEvent::CleanupFinished {
                    assignment_id,
                    result,
                } => self.finish_cleanup(assignment_id, result),
                ManagerEvent::LeaseClockFailed {
                    assignment_id,
                    error,
                } => {
                    self.classify_lease_clock(&assignment_id, error, "terminal_acknowledgement");
                    self.lease_clock_failed = true;
                    if let Some(id) = self.cleanup_failure_report.take() {
                        let assignment_id = {
                            self.outbox
                                .lock()
                                .entries
                                .iter()
                                .find(|entry| entry.id == id)
                                .map(|entry| entry.observation.assignment_id().to_owned())
                        };
                        if let Some(assignment_id) = assignment_id {
                            self.retire_assignment_observations(&assignment_id);
                        }
                    }
                    // The held assignments share the lease clock that failed.
                    self.revoke_every_running_authority();
                }
            }
        }
        self.interrupt_held_after_cleanup_failure();
        self.forget_ended_offer_facts();
    }

    pub(super) fn has_pending_pre_execution_cancellation(&self, assignment_id: &str) -> bool {
        self.cancellations.iter().any(|retained| {
            retained.command.assignment_id == assignment_id
                && retained.application.is_some()
                && !retained.ready
        })
    }

    pub(super) fn clamp_to_shutdown_cleanup_deadline(
        &self,
        deadline: LeaseInstant,
    ) -> Result<LeaseInstant, LeaseClockError> {
        let Some(cleanup_deadline) = self.shutdown_cleanup_deadline else {
            return Ok(deadline);
        };
        match deadline.checked_cmp(cleanup_deadline)? {
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal => Ok(deadline),
            std::cmp::Ordering::Greater => Ok(cleanup_deadline),
        }
    }

    pub(super) fn start_final_grace(
        &self,
        assignment_id: String,
        final_observation_id: u64,
        deadline: LeaseInstant,
        continue_reporting: bool,
    ) -> Result<(), LeaseClockError> {
        let wait = self
            .lease_clock
            .start_wait(deadline)
            .inspect_err(|&error| {
                self.classify_lease_clock(&assignment_id, error, "terminal_acknowledgement");
            })?;
        let sender = self.event_sender.clone();
        let outbox = self.outbox.clone();
        tokio::spawn(async move {
            let cancellation = LeaseWaitCancellation::default();
            let event = match wait.wait(&cancellation).await {
                Ok(_) => ManagerEvent::FinalGraceElapsed {
                    assignment_id,
                    final_observation_id,
                    continue_reporting,
                },
                Err(error) => ManagerEvent::LeaseClockFailed {
                    assignment_id,
                    error,
                },
            };
            let _ = sender.send(event);
            outbox.wake();
        });
        Ok(())
    }

    pub(super) fn apply_successor_fences(&mut self, successor_assignment_id: &str) {
        // Only a predecessor that never reported a wait is settled by the successor's offer
        // (INV-RUNNER-40).
        let predecessors: Vec<String> = self
            .reporting
            .iter()
            .filter(|(assignment_id, reporting)| {
                *assignment_id != successor_assignment_id && !reporting.reported_wait
            })
            .map(|(assignment_id, _)| assignment_id.clone())
            .collect();
        for predecessor in predecessors {
            self.retire_assignment_observations(&predecessor);
            self.reporting.remove(&predecessor);
        }
        let rejected_assignments: Vec<_> = self
            .decisions
            .iter()
            .filter(|decision| {
                decision.offer.assignment_id != successor_assignment_id
                    && matches!(decision.response, AssignmentDecision::Rejected { .. })
            })
            .map(|decision| decision.offer.assignment_id.clone())
            .collect();
        for assignment_id in rejected_assignments {
            self.retire_assignment_observations(&assignment_id);
        }
    }

    // Ends the assignment's `runner.run` record once and lists the assignment as
    // recent; a later call finds nothing and writes nothing.
    pub(super) fn finish_run_event(&mut self, assignment_id: &str, result: Option<&'static str>) {
        if let Some(event) = self.run_events.remove(assignment_id) {
            let end = event.finish(result);
            self.keep_recent_assignment(assignment_id, end);
        }
    }

    // Ends `run`, the run opened for an offer the runner is giving up, and
    // forgets it and lists the assignment as recent only while it is still the
    // assignment's recorded run: a declined duplicate offer must not end a live
    // run of the same assignment.
    pub(super) fn finish_offered_run(
        &mut self,
        assignment_id: &str,
        run: &RunEvent,
        result: &'static str,
    ) {
        let recorded = self
            .run_events
            .get(assignment_id)
            .is_some_and(|recorded| recorded.is(run));
        if recorded {
            self.run_events.remove(assignment_id);
        }
        let end = run.finish(Some(result));
        if recorded {
            self.keep_recent_assignment(assignment_id, end);
        }
    }

    /// Ends the run opened for an offer the runner answers with `decline`: names the
    /// decline on its record, then finishes it `rejected` (REQ-TELEMETRY-47). Every
    /// rejected ending goes through here.
    pub(super) fn reject_offered_run(
        &mut self,
        assignment_id: &str,
        run: &RunEvent,
        decline: &AssignmentDecline,
    ) {
        run.decline(decline);
        self.finish_offered_run(assignment_id, run, "rejected");
    }

    pub(super) fn retire_assignment_observations(&mut self, assignment_id: &str) {
        self.finish_run_event(assignment_id, Some("fenced"));
        self.outbox.fence_assignment(assignment_id);
        self.artifact_delivery.cancel_assignment(assignment_id);
        for decision in &mut self.decisions {
            if decision.offer.assignment_id == assignment_id
                && decision
                    .response_observation_id
                    .is_some_and(|id| !self.outbox.contains(id))
            {
                decision.response_observation_id = None;
            }
        }
        for cancellation in &mut self.cancellations {
            if cancellation.command.assignment_id == assignment_id
                && cancellation
                    .observation_id
                    .is_some_and(|id| !self.outbox.contains(id))
            {
                cancellation.observation_id = None;
            }
        }
    }

    pub(super) fn make_decision_room(&mut self) -> Result<(), AssignmentManagerFailure> {
        if self.decisions.len() < MAXIMUM_RETAINED_DECISIONS {
            return Ok(());
        }
        // Never the decision of an assignment that is accepted, running or finishing.
        let Some(index) = self.decisions.iter().position(|decision| {
            decision.response_observation_id.is_none()
                && !matches!(
                    self.held.get(&decision.offer.assignment_id),
                    Some(
                        HeldAssignment::Accepted(_)
                            | HeldAssignment::Running(_)
                            | HeldAssignment::Finishing(_)
                    )
                )
        }) else {
            return Err(AssignmentManagerFailure::DecisionCapacity);
        };
        self.decisions.remove(index);
        Ok(())
    }

    pub(super) fn retain_decision(
        &mut self,
        offer: AssignmentOffer,
        response: AssignmentDecision,
    ) -> Result<(), AssignmentManagerFailure> {
        let causal_lease = if matches!(response, AssignmentDecision::Accepted { .. }) {
            let basis = self
                .lease_clock
                .now()
                .map_err(|_| AssignmentManagerFailure::LeaseClock)?;
            Some(CausalLease::new(basis))
        } else {
            None
        };
        let response_observation_id = self
            .outbox
            .enqueue(AssignmentObservation::Decision(response.clone()))
            .map_err(|_| AssignmentManagerFailure::DecisionCapacity)?;
        self.decisions.push_back(RetainedDecision {
            offer,
            response,
            response_observation_id: Some(response_observation_id),
            causal_lease,
            start: None,
            start_authorization: None,
            renewals: BTreeMap::new(),
            rejected_renewals: BTreeMap::new(),
        });
        Ok(())
    }

    pub(super) fn validate_admission_prerequisites(
        &self,
        offer: &AssignmentOffer,
    ) -> Result<(), AssignmentDecline> {
        if offer.continuation.is_some() {
            validate_effective_spec(&offer.execution_spec, true)?;
        } else {
            validate_execution_spec(&offer.execution_spec)?;
        }
        if let Some(continuation) = &offer.continuation
            && (continuation.definition_source != offer.execution_spec.workflow_definition_source
                || continuation.effective_capacity != offer.execution_spec.capacity)
        {
            return Err(AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch,
            ));
        }
        // The continuation root preparer authenticates the pinned retained
        // workspace before preparation; an ordinary offer must never substitute
        // its fresh execution tree for this requested baseline.
        Ok(())
    }

    pub(super) fn admission_runtime(&self, assignment_id: &str) -> AdmissionRuntime {
        let (preparation_event, run_event) = match self.preparing(assignment_id) {
            Some(preparing) => (
                preparing.preparation_event.clone(),
                preparing.run_event.clone(),
            ),
            None => (None, RunEvent::default()),
        };
        AdmissionRuntime {
            pi_installation: self.pi_installation.clone(),
            claude_code_installation: self.claude_code_installation.clone(),
            codex_installation: self.codex_installation.clone(),
            environment: self.environment.clone(),
            command_environment: crate::agent_login::CommandEnvironment::process(
                self.environment.clone(),
            ),
            harness_login_observer: None,
            execution_version: Arc::clone(&self.execution_version),
            outbox: self.outbox.clone(),
            guard_processes: self.guard_processes,
            recorder: self.recorder.clone(),
            preparation_event,
            run_event,
            work_root: Arc::clone(&self.work_root),
        }
    }

    pub(super) fn classify_lease_clock(
        &self,
        assignment_id: &str,
        error: LeaseClockError,
        stage: &'static str,
    ) {
        if let Some(event) = self.run_events.get(assignment_id) {
            event.lease_clock_failure(error, stage);
        }
    }

    /// Fails the lease clock, which the renewal of `assignment_id` met: records the failure
    /// on that assignment's run and revokes the authority of every running assignment, since
    /// they share the clock.
    pub(super) fn fail_lease_clock(
        &mut self,
        assignment_id: &str,
        error: LeaseClockError,
    ) -> AssignmentManagerFailure {
        if let Some(running) = self.running(assignment_id) {
            running
                .run_event
                .lease_clock_failure(error, "lease_renewal");
        }
        self.revoke_every_running_authority();
        self.lease_clock_failed = true;
        AssignmentManagerFailure::LeaseClock
    }

    fn revoke_every_running_authority(&mut self) {
        for held in self.held.values_mut() {
            if let HeldAssignment::Running(running) = held {
                revoke_authority(running);
            }
        }
    }

    pub(super) fn begin_lease_clock_failure_reporting(&mut self, final_observation_id: u64) {
        self.lease_clock_failed = true;
        self.lease_clock_failure_report = Some(final_observation_id);
        self.outbox.retain_only(final_observation_id);
        self.outbox.wake();
    }

    pub(super) fn validate_grant(
        &self,
        grant: &ExecutionLeaseGrant,
        expected_sequence: u64,
        cancellation_grace: Duration,
        causal_lease: &CausalLease,
    ) -> Result<LeaseAuthority, GrantValidationFailure> {
        if grant.sequence != expected_sequence {
            return Err(GrantValidationFailure::MissingBasis);
        }
        let policy = self
            .lease_policy
            .as_ref()
            .ok_or(GrantValidationFailure::MissingBasis)?;
        let basis = causal_lease
            .basis(expected_sequence)
            .ok_or(GrantValidationFailure::MissingBasis)?;
        LeaseAuthority::derive(expected_sequence, basis, policy, cancellation_grace)
            .map_err(GrantValidationFailure::Arithmetic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finished_paths_are_selected_without_mutating_manager_state() {
        let (clock, _, _) = super::super::super::lease_clock::controlled_lease_clock();
        let deadline = clock.now().expect("controlled lease instant");
        let pending = Some(Ok((deadline, true)));
        assert_eq!(
            finished_disposition(ProcessQuiescence::Failed, Some(7), true, true, pending),
            FinishedDisposition::ContainmentFailed
        );
        assert_eq!(
            finished_disposition(ProcessQuiescence::Proven, None, false, true, pending),
            FinishedDisposition::PreExecutionCancellation
        );
        assert_eq!(
            finished_disposition(ProcessQuiescence::Proven, None, true, true, pending),
            FinishedDisposition::WithoutReport
        );
        assert_eq!(
            finished_disposition(ProcessQuiescence::Proven, Some(7), true, false, pending),
            FinishedDisposition::RetainAfterClockFailure(None)
        );
        assert_eq!(
            finished_disposition(ProcessQuiescence::Proven, Some(7), false, false, None),
            FinishedDisposition::WithoutReport
        );
        assert_eq!(
            finished_disposition(
                ProcessQuiescence::Proven,
                Some(7),
                false,
                false,
                Some(Ok((deadline, false)))
            ),
            FinishedDisposition::WithoutReport
        );
        assert_eq!(
            finished_disposition(ProcessQuiescence::Proven, Some(7), false, false, pending),
            FinishedDisposition::AwaitAcknowledgement(7, deadline)
        );
        assert_eq!(
            finished_disposition(
                ProcessQuiescence::Proven,
                Some(7),
                false,
                false,
                Some(Err(LeaseClockError::ClockUnavailable))
            ),
            FinishedDisposition::RetainAfterClockFailure(Some(LeaseClockError::ClockUnavailable))
        );
    }
}
