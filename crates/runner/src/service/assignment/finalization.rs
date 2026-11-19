use super::*;
use crate::control_protocol::{HeldRun, HeldRunState};

impl AssignmentManager {
    pub(super) fn finish_without_reporting(
        &mut self,
        assignment_id: String,
        root: Option<AssignmentRoot>,
        quiescence: ProcessQuiescence,
        disposition: WorkspaceDisposition,
        fenced: bool,
    ) {
        self.finish_run_event(
            &assignment_id,
            Some(if fenced { "fenced" } else { "aborted" }),
        );
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
        ended: ReportingAssignment,
        final_observation_id: u64,
    ) {
        self.begin_lease_clock_failure_reporting(final_observation_id);
        self.cleanup_retained_root(
            assignment_id,
            root,
            quiescence,
            disposition,
            ReleaseAfter::Reporting(Box::new(ended)),
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
        } else {
            self.tree_run.forget(&assignment_id);
            if let ReleaseAfter::Reporting(identity) = after {
                self.report(*identity);
            }
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
        // The assignment has ended: its tree run takes no more deliveries or controls.
        self.tree_run.forget(&assignment_id);
        self.hold(HeldAssignment::Releasing(Box::new(ReleasingAssignment {
            assignment_id: assignment_id.clone(),
            after,
            retention_report,
        })));
        let sender = self.event_sender.clone();
        let wake = self.outbox.clone();
        // Run the synchronous release chain on the blocking pool, not the manager caller.
        tokio::task::spawn_blocking(move || {
            let result = root.release_pending(quiescence, disposition).wait();
            // The root itself still holds the work-root lease through its trees
            // and cleanup engine. Release it before waking the manager: a settled
            // manager may immediately let the next boot acquire the same work root.
            drop(root);
            let _ = sender.send(ManagerEvent::CleanupFinished {
                assignment_id,
                result,
            });
            wake.wake();
        });
    }

    pub(super) fn finish_cleanup(&mut self, assignment_id: String, result: CleanupResult) {
        let Some(releasing) = self.take_held(&assignment_id, HeldAssignment::releasing) else {
            return;
        };
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
                    ReleaseAfter::Reporting(identity) => self.report(*identity),
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
        // Cancelled before it ran, the assignment never reported a wait.
        self.report(ReportingAssignment {
            identity,
            reported_wait: false,
        });
        self.outbox.wake();
    }

    pub(super) fn enqueue_assignment_interruption(
        &self,
        identity: &AssignmentIdentity,
        reason: &str,
    ) -> Result<u64, OutboxFailure> {
        let id = self.outbox.enqueue(AssignmentObservation::Execution {
            assignment_id: identity.assignment_id.clone(),
            attempt_id: identity.attempt_id.clone(),
            report: ExecutionReport::AssignmentInterrupted {
                reason: reason.to_owned(),
            },
        })?;
        if let Some(event) = self.run_events.get(&identity.assignment_id) {
            event.result("interrupted");
            event.set(KeyValue::new(
                crate::telemetry::attribute::INTERRUPTION_CAUSE,
                reason.to_owned(),
            ));
        }
        Ok(id)
    }

    pub(super) fn finish_before_execution(
        &mut self,
        identity: AssignmentIdentity,
        root: AssignmentRoot,
        reason: &str,
    ) -> Result<(), AssignmentManagerFailure> {
        let Ok(final_observation_id) = self.enqueue_assignment_interruption(&identity, reason)
        else {
            // No report will end the run, and the assignment is given up.
            self.finish_run_event(&identity.assignment_id, Some("aborted"));
            return Err(AssignmentManagerFailure::DecisionCapacity);
        };
        let workspace_disposition = WorkspaceDisposition::Retain(match reason {
            "graceful_shutdown" => RetentionReason::Cancelled,
            "execution_lease_expired" => RetentionReason::Interrupted,
            _ => RetentionReason::Failed,
        });
        // Ended before it ran, the assignment never reported a wait.
        self.hold(HeldAssignment::Finishing(Box::new(FinishingAssignment {
            identity: identity.clone(),
            final_observation_id,
            root: Some(root),
            workspace_disposition,
            reported_wait: false,
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
            && !self.releasing_any()
            && self
                .lease_clock_failure_report
                .is_none_or(|id| self.outbox.is_encoded(id) || !self.outbox.contains(id))
    }

    /// Whether a boot a failed cleanup fenced may exit: once it holds no assignment, so the
    /// exit ends no run's processes (REQ-RUNNERENROLL-71), and each report it owes has been
    /// sent. An assignment that only reports does not hold the exit.
    pub(in crate::service) fn cleanup_failure_ready_to_exit(&mut self) -> bool {
        self.drain_events();
        self.cleanup_failed
            && self.held.is_empty()
            && self.deferred_successor.is_none()
            && self.cleanup_failure_report.is_none()
            && self.outbox.replay_obligations_dispatched()
    }

    pub(in crate::service) fn quiescence_failure(&self) -> Option<Vec<String>> {
        self.quiescence_failure.clone()
    }

    pub(in crate::service) fn shutdown_complete(&mut self) -> bool {
        self.drain_events();
        self.held.is_empty()
            && self.reporting.is_empty()
            && self.outbox.replay_obligations_dispatched()
    }

    pub(in crate::service) fn shutdown_waiting_only_for_cleanup(&mut self) -> bool {
        self.drain_events();
        self.shutting_down
            && !self.held.is_empty()
            && self
                .held
                .values()
                .all(|held| matches!(held, HeldAssignment::Releasing(_)))
            && self.outbox.replay_obligations_dispatched()
    }

    /// The assignment counts and the held runs of `useful runner status`, taken in one read
    /// (REQ-RUNNERENROLL-42, REQ-RUNNERENROLL-80). A tree run whose loop last reported a
    /// wait, by the read the offer rule makes, is counted `waiting` and listed `waiting`; an
    /// assignment that has ended is counted `finishing` or `reporting` and not listed. The
    /// working runs come first, and each group follows the assignment IDs, which Cloud
    /// mints in time order as it offers, so the order the runner took them.
    pub(in crate::service) fn status_assignments(&mut self) -> (AssignmentCounts, Vec<HeldRun>) {
        self.drain_events();
        let mut counts = AssignmentCounts {
            reporting: u64::try_from(self.reporting.len()).unwrap_or(u64::MAX),
            ..AssignmentCounts::default()
        };
        let mut working = Vec::new();
        let mut waiting = Vec::new();
        for held in self.held.values() {
            let (count, run_id) = match held {
                HeldAssignment::Preparing(preparing) => {
                    (&mut counts.preparing, &preparing.offer.run_id)
                }
                HeldAssignment::Accepted(accepted) => {
                    (&mut counts.accepted, &accepted.identity.run_id)
                }
                HeldAssignment::Running(running) if held.is_working() => {
                    (&mut counts.running, &running.identity.run_id)
                }
                HeldAssignment::Running(running) => (&mut counts.waiting, &running.identity.run_id),
                HeldAssignment::Finishing(_) | HeldAssignment::Releasing(_) => {
                    counts.finishing = counts.finishing.saturating_add(1);
                    continue;
                }
            };
            *count = count.saturating_add(1);
            let run_id = run_id.clone();
            if held.is_working() {
                working.push(HeldRun {
                    run_id,
                    state: HeldRunState::Working,
                });
            } else {
                waiting.push(HeldRun {
                    run_id,
                    state: HeldRunState::Waiting,
                });
            }
        }
        working.append(&mut waiting);
        (counts, working)
    }

    /// What the runner holds, as agent-login setup reads it (INV-RUNNER-15): nothing, only
    /// tree runs whose loops last reported a wait, or an assignment that is working. An
    /// assignment being prepared, accepted, finished or released, one still reporting and a
    /// run on the embedded engine are working.
    pub(in crate::service) fn held_runs(&mut self) -> HeldRuns {
        self.drain_events();
        if self.held.is_empty() && self.reporting.is_empty() {
            return HeldRuns::Nothing;
        }
        if !self.reporting.is_empty() {
            return HeldRuns::Working;
        }
        let mut waiting = Vec::with_capacity(self.held.len());
        for held in self.held.values() {
            match held {
                HeldAssignment::Running(running) => match &running.activity {
                    Some(activity) if activity.is_waiting() => {
                        waiting.push(Arc::clone(activity));
                    }
                    _ => return HeldRuns::Working,
                },
                _ => return HeldRuns::Working,
            }
        }
        HeldRuns::Waiting(waiting)
    }

    pub(in crate::service) fn notification(&self) -> Arc<Notify> {
        self.outbox.notification()
    }
}
