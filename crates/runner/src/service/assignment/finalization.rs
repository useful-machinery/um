use super::*;

impl AssignmentManager {
    pub(super) fn finish_without_reporting(
        &mut self,
        assignment_id: String,
        root: Option<AssignmentRoot>,
        quiescence: ProcessQuiescence,
        disposition: WorkspaceDisposition,
        fenced: bool,
    ) {
        if let Some(event) = self.run_events.remove(&assignment_id) {
            event.finish(Some(if fenced { "fenced" } else { "aborted" }));
        }
        self.retire_assignment_observations(&assignment_id);
        self.cleanup_retained_root(
            assignment_id,
            root,
            quiescence,
            disposition,
            ReleaseAfter::Idle,
        );
        self.outbox.wake();
    }

    pub(super) fn retain_after_lease_clock_failure(
        &mut self,
        assignment_id: String,
        root: Option<AssignmentRoot>,
        quiescence: ProcessQuiescence,
        disposition: WorkspaceDisposition,
        identity: AssignmentIdentity,
        final_observation_id: u64,
    ) {
        self.begin_lease_clock_failure_reporting(final_observation_id);
        self.cleanup_retained_root(
            assignment_id,
            root,
            quiescence,
            disposition,
            ReleaseAfter::Reporting(Box::new(identity)),
        );
    }

    pub(super) fn cleanup_retained_root(
        &mut self,
        assignment_id: String,
        root: Option<AssignmentRoot>,
        quiescence: ProcessQuiescence,
        disposition: WorkspaceDisposition,
        after: ReleaseAfter,
    ) {
        if let Some(root) = root {
            self.begin_assignment_finalization(assignment_id, root, quiescence, disposition, after);
        } else if let ReleaseAfter::Reporting(identity) = after {
            self.reporting = Some(*identity);
        }
    }

    pub(super) fn begin_assignment_cleanup(
        &mut self,
        assignment_id: String,
        root: AssignmentRoot,
        quiescence: ProcessQuiescence,
        after: ReleaseAfter,
    ) {
        self.begin_assignment_finalization(
            assignment_id,
            root,
            quiescence,
            WorkspaceDisposition::Retain(RetentionReason::Failed),
            after,
        );
    }

    pub(super) fn begin_assignment_finalization(
        &mut self,
        assignment_id: String,
        root: AssignmentRoot,
        quiescence: ProcessQuiescence,
        disposition: WorkspaceDisposition,
        after: ReleaseAfter,
    ) {
        let retention_report = root.retention_report_identity();
        self.slot = Some(LocalSlot::Releasing(ReleasingAssignment {
            assignment_id: assignment_id.clone(),
            after,
            retention_report,
        }));
        let sender = self.event_sender.clone();
        let wake = self.outbox.clone();
        // Run the synchronous release chain on the blocking pool, not the manager caller.
        tokio::task::spawn_blocking(move || {
            let result = root.release_pending(quiescence, disposition).wait();
            // The root itself still holds the work-root lease through its trees
            // and cleanup engine. Release it before waking the manager: a settled
            // slot may immediately let the next boot acquire the same work root.
            drop(root);
            let _ = sender.send(ManagerEvent::CleanupFinished {
                assignment_id,
                result,
            });
            wake.wake();
        });
    }

    pub(super) fn finish_cleanup(&mut self, assignment_id: String, result: CleanupResult) {
        let Some(LocalSlot::Releasing(releasing)) = self.slot.take() else {
            return;
        };
        if releasing.assignment_id != assignment_id {
            self.slot = Some(LocalSlot::Releasing(releasing));
            return;
        }
        match result {
            CleanupResult::Released | CleanupResult::Retained => {
                if result == CleanupResult::Retained
                    && let Some((assignment_id, attempt_id, run_id, execution_root, snapshot)) =
                        releasing.retention_report.as_ref()
                    && self
                        .outbox
                        .enqueue(AssignmentObservation::WorkspaceRetention {
                            assignment_id: assignment_id.clone(),
                            attempt_id: attempt_id.clone(),
                            run_id: run_id.clone(),
                            execution_root: execution_root.clone(),
                            state: "retained".to_owned(),
                            settlement_snapshot: snapshot.clone(),
                        })
                        .is_err()
                {
                    self.cleanup_failed = true;
                }
                match releasing.after {
                    ReleaseAfter::Idle => {}
                    ReleaseAfter::Reporting(identity) => self.reporting = Some(*identity),
                    ReleaseAfter::PreExecutionCancellation(identity) => {
                        self.complete_pre_execution_cancellation(*identity);
                    }
                }
                if let Some(successor) = self.deferred_successor.take()
                    && let Err(failure) = self.handle_offer_after_drain(successor)
                {
                    self.record_deferred_offer_failure(failure);
                }
            }
            CleanupResult::Quarantined(_) | CleanupResult::Preempted => {
                self.cleanup_failed = true;
                if let Some(successor) = self.deferred_successor.take() {
                    let response = rejected(&successor, environment_unavailable());
                    if let Err(failure) = self.retain_decision(successor, response) {
                        self.record_deferred_offer_failure(failure);
                    }
                }
            }
        }
        self.outbox.wake();
    }

    pub(in crate::service) fn record_deferred_offer_failure(
        &mut self,
        failure: AssignmentManagerFailure,
    ) {
        if failure == AssignmentManagerFailure::LeaseClock {
            self.lease_clock_failed = true;
        } else {
            self.deferred_offer_failure = Some(failure);
        }
        self.outbox.wake();
    }

    pub(in crate::service) fn take_deferred_offer_failure(
        &mut self,
    ) -> Option<AssignmentManagerFailure> {
        self.drain_events();
        self.deferred_offer_failure.take()
    }

    pub(super) fn complete_pre_execution_cancellation(&mut self, identity: AssignmentIdentity) {
        let pending: Vec<_> = self
            .cancellations
            .iter()
            .enumerate()
            .filter(|(_, retained)| {
                retained.command.assignment_id == identity.assignment_id
                    && retained.application.is_some()
                    && !retained.ready
            })
            .map(|(index, _)| index)
            .collect();
        for index in pending {
            self.cancellations[index].ready = true;
            if self.emit_cancellation_application(index).is_err() {
                self.cleanup_failed = true;
                self.retire_assignment_observations(&identity.assignment_id);
                return;
            }
        }
        let reason = if self.cancellations.iter().any(|retained| {
            retained.command.assignment_id == identity.assignment_id
                && retained.application.as_ref().is_some_and(|application| {
                    application.effective_mode == CancellationMode::Force
                })
        }) {
            "force_abort"
        } else {
            "user_request"
        };
        if self
            .enqueue_assignment_interruption(&identity, reason)
            .is_err()
        {
            self.cleanup_failed = true;
            self.retire_assignment_observations(&identity.assignment_id);
            return;
        }
        self.reporting = Some(identity);
        self.outbox.wake();
    }

    pub(super) fn enqueue_assignment_interruption(
        &self,
        identity: &AssignmentIdentity,
        reason: &str,
    ) -> Result<u64, OutboxFailure> {
        self.outbox.enqueue(AssignmentObservation::Execution {
            assignment_id: identity.assignment_id.clone(),
            attempt_id: identity.attempt_id.clone(),
            report: ExecutionReport::AssignmentInterrupted {
                reason: reason.to_owned(),
            },
        })
    }

    pub(super) fn finish_before_execution(
        &mut self,
        identity: AssignmentIdentity,
        root: AssignmentRoot,
        reason: &str,
    ) -> Result<(), AssignmentManagerFailure> {
        let final_observation_id = self
            .enqueue_assignment_interruption(&identity, reason)
            .map_err(|_| AssignmentManagerFailure::DecisionCapacity)?;
        let workspace_disposition = WorkspaceDisposition::Retain(match reason {
            "graceful_shutdown" => RetentionReason::Cancelled,
            "execution_lease_expired" => RetentionReason::Interrupted,
            _ => RetentionReason::Failed,
        });
        self.slot = Some(LocalSlot::Finishing(Box::new(FinishingAssignment {
            identity: identity.clone(),
            final_observation_id,
            root: Some(root),
            workspace_disposition,
        })));
        let deadline = self
            .lease_clock
            .now()
            .and_then(|now| now.checked_add(FINAL_ACKNOWLEDGEMENT_GRACE))
            .and_then(|deadline| self.clamp_to_shutdown_cleanup_deadline(deadline))
            .map_err(|_| AssignmentManagerFailure::LeaseClock)?;
        self.start_final_grace(identity.assignment_id, final_observation_id, deadline, true)
            .map_err(|_| AssignmentManagerFailure::LeaseClock)?;
        Ok(())
    }

    pub(in crate::service) fn lease_clock_has_failed(&mut self) -> bool {
        self.drain_events();
        self.lease_clock_failed
    }

    pub(in crate::service) fn pending_lease_clock_failure_report(&mut self) -> Option<u64> {
        self.drain_events();
        self.lease_clock_failure_report
            .filter(|id| !self.outbox.is_encoded(*id))
    }

    pub(in crate::service) fn lease_clock_failure_ready_to_exit(&mut self) -> bool {
        self.drain_events();
        self.lease_clock_failed
            && !matches!(self.slot, Some(LocalSlot::Releasing(_)))
            && self
                .lease_clock_failure_report
                .is_none_or(|id| self.outbox.is_encoded(id) || !self.outbox.contains(id))
    }

    pub(in crate::service) fn cleanup_failure_ready_to_exit(&mut self) -> bool {
        self.drain_events();
        self.cleanup_failed
            && !matches!(self.slot, Some(LocalSlot::Releasing(_)))
            && self.deferred_successor.is_none()
            && self.cleanup_failure_report.is_none()
            && self.outbox.replay_obligations_dispatched()
    }

    pub(in crate::service) fn quiescence_failure(&self) -> Option<Vec<String>> {
        self.quiescence_failure.clone()
    }

    pub(in crate::service) fn shutdown_complete(&mut self) -> bool {
        self.drain_events();
        self.slot.is_none()
            && self.reporting.is_none()
            && self.outbox.replay_obligations_dispatched()
    }

    pub(in crate::service) fn shutdown_waiting_only_for_cleanup(&mut self) -> bool {
        self.drain_events();
        self.shutting_down
            && matches!(self.slot, Some(LocalSlot::Releasing(_)))
            && self.outbox.replay_obligations_dispatched()
    }

    pub(in crate::service) fn status_counts(&mut self) -> AssignmentCounts {
        self.drain_events();
        let mut counts = AssignmentCounts::default();
        match &self.slot {
            Some(LocalSlot::Preparing(_)) => counts.preparing = 1,
            Some(LocalSlot::Accepted(_)) => counts.accepted = 1,
            Some(LocalSlot::Running(_)) => counts.running = 1,
            Some(LocalSlot::Finishing(_) | LocalSlot::Releasing(_)) => counts.finishing = 1,
            None if self.reporting.is_some() => counts.reporting = 1,
            None => {}
        }
        counts
    }

    pub(in crate::service) fn notification(&self) -> Arc<Notify> {
        self.outbox.notification()
    }
}
