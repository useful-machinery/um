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
                    let Some(LocalSlot::Preparing(mut preparing)) = self.slot.take() else {
                        if let Ok(root) = root {
                            release_unclaimed_assignment_root(root);
                        }
                        continue;
                    };
                    if preparing.offer.assignment_id != assignment_id {
                        self.slot = Some(LocalSlot::Preparing(preparing));
                        if let Ok(root) = root {
                            release_unclaimed_assignment_root(root);
                        }
                        continue;
                    }
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
                                self.begin_assignment_cleanup(
                                    assignment_id,
                                    root,
                                    ProcessQuiescence::Proven,
                                    ReleaseAfter::Idle,
                                );
                            } else {
                                preparing.root = Some(root);
                                self.slot = Some(LocalSlot::Preparing(preparing));
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
                    let Some(LocalSlot::Preparing(mut preparing)) = self.slot.take() else {
                        continue;
                    };
                    if !same_assignment(&preparing.offer, &offer) {
                        self.slot = Some(LocalSlot::Preparing(preparing));
                        continue;
                    }
                    if preparing.cancellation.is_cancelled() || deadline.remaining().is_none() {
                        if let Some(event) = preparing.preparation_event.take() {
                            event.finish(TelemetryOutcome::Cancelled);
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
                            self.begin_assignment_cleanup(
                                offer.assignment_id.clone(),
                                root,
                                ProcessQuiescence::Proven,
                                ReleaseAfter::Idle,
                            );
                        }
                        continue;
                    }
                    if let Some(event) = preparing.preparation_event.take() {
                        event.finish(if admission.is_ok() {
                            TelemetryOutcome::Success
                        } else {
                            TelemetryOutcome::Failure
                        });
                    }
                    match *admission {
                        Ok(accepted) => {
                            self.slot = Some(LocalSlot::Accepted(Box::new(accepted)));
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
                                if let Some(LocalSlot::Accepted(accepted)) = self.slot.take() {
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
                    if let Some(LocalSlot::Running(running)) = &mut self.slot
                        && running.identity.assignment_id == assignment_id
                    {
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
                    let running = match self.slot.take() {
                        Some(LocalSlot::Running(running)) => running,
                        slot => {
                            // A delayed completion must not discard a successor
                            // in another phase of its lifecycle.
                            self.slot = slot;
                            continue;
                        }
                    };
                    if running.identity.assignment_id != assignment_id {
                        self.slot = Some(LocalSlot::Running(running));
                        continue;
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
                                ReleaseAfter::Reporting(Box::new(identity))
                            } else {
                                if let Some(event) = self.run_events.remove(&assignment_id) {
                                    event.finish(Some(if fenced { "fenced" } else { "aborted" }));
                                }
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
                                self.reporting = Some(*identity);
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
                                    identity,
                                    id,
                                );
                            }
                        }
                        FinishedDisposition::AwaitAcknowledgement(id, deadline) => {
                            self.slot = Some(LocalSlot::Finishing(Box::new(FinishingAssignment {
                                identity: identity.clone(),
                                final_observation_id: id,
                                root: retained_root,
                                workspace_disposition,
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
                        self.reporting = None;
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
                    let finishing = match self.slot.take() {
                        Some(LocalSlot::Finishing(finishing)) => finishing,
                        slot => {
                            // Acknowledgement can release the predecessor before
                            // its grace timer fires. Preserve any successor slot.
                            self.slot = slot;
                            continue;
                        }
                    };
                    if finishing.identity.assignment_id == assignment_id
                        && finishing.final_observation_id == final_observation_id
                    {
                        let workspace_disposition = finishing.workspace_disposition;
                        let after = if continue_reporting {
                            ReleaseAfter::Reporting(Box::new(finishing.identity))
                        } else {
                            if let Some(event) = self.run_events.remove(&assignment_id) {
                                event.finish(Some("aborted"));
                            }
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
                            self.reporting = Some(*identity);
                        }
                    } else {
                        self.slot = Some(LocalSlot::Finishing(finishing));
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
                    if let Some(LocalSlot::Running(running)) = &mut self.slot {
                        revoke_authority(running);
                    }
                }
            }
        }
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
        let predecessor = self
            .reporting
            .as_ref()
            .filter(|identity| identity.assignment_id != successor_assignment_id)
            .cloned();
        if let Some(predecessor) = predecessor {
            self.retire_assignment_observations(&predecessor.assignment_id);
            self.reporting = None;
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

    pub(super) fn retire_assignment_observations(&mut self, assignment_id: &str) {
        if let Some(event) = self.run_events.remove(assignment_id) {
            event.finish(Some("fenced"));
        }
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
        let active_assignment = match &self.slot {
            Some(LocalSlot::Accepted(accepted)) => Some(accepted.identity.assignment_id.as_str()),
            Some(LocalSlot::Running(running)) => Some(running.identity.assignment_id.as_str()),
            Some(LocalSlot::Finishing(finishing)) => {
                Some(finishing.identity.assignment_id.as_str())
            }
            Some(LocalSlot::Preparing(_) | LocalSlot::Releasing(_)) | None => None,
        };
        let Some(index) = self.decisions.iter().position(|decision| {
            decision.response_observation_id.is_none()
                && active_assignment != Some(decision.offer.assignment_id.as_str())
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

    pub(super) fn admission_runtime(&self) -> AdmissionRuntime {
        let preparation_event = match &self.slot {
            Some(LocalSlot::Preparing(preparing)) => preparing.preparation_event.clone(),
            _ => None,
        };
        AdmissionRuntime {
            pi_installation: self.pi_installation.clone(),
            claude_code_installation: self.claude_code_installation.clone(),
            codex_installation: self.codex_installation.clone(),
            environment: self.environment.clone(),
            execution_version: Arc::clone(&self.execution_version),
            outbox: self.outbox.clone(),
            guard_processes: self.guard_processes,
            recorder: self.recorder.clone(),
            preparation_event,
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

    pub(super) fn fail_lease_clock(&mut self, error: LeaseClockError) -> AssignmentManagerFailure {
        if let Some(LocalSlot::Running(running)) = &self.slot {
            running
                .run_event
                .lease_clock_failure(error, "lease_renewal");
        }
        if let Some(LocalSlot::Running(running)) = &mut self.slot {
            revoke_authority(running);
        }
        self.lease_clock_failed = true;
        AssignmentManagerFailure::LeaseClock
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
