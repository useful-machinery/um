use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) enum ExecutionReport {
    AssignmentInterrupted {
        reason: String,
    },
    Started,
    Transition {
        execution_event_sequence: u64,
        workflow_event: Value,
        diagnostic: Option<Value>,
    },
    /// A tree run's root progress between and during its engine attempts, numbered by
    /// the assignment's own progress sequence from 1 (REQ-RUNNERENROLL-99). `steps_only`
    /// marks a report that restates the root to carry step observations alone, which is
    /// owed to no run (REQ-RUNNERENROLL-88).
    TreeProgress {
        progress_sequence: u64,
        progress: um_runner_protocol::TreeRootProgress,
        steps_only: bool,
    },
    /// A tree run's application of one trigger delivery (REQ-TRIGGER-136).
    TriggerDeliveryApplied {
        application: um_runner_protocol::TriggerDeliveryApplication,
    },
    /// A tree run's application of one authorized Cloud control (REQ-RUNNERENROLL-90).
    RunControlApplied {
        application: um_runner_protocol::RunControlApplication,
    },
    Finished {
        final_execution_event_sequence: u64,
        outcome: Value,
        artifact_delivery: Value,
        diagnostic: Option<Value>,
    },
    /// A tree run's finish, carrying the root's exit beside its transport outcome
    /// (REQ-RUNNERENROLL-99) and the safe facts of a failure the runner classified
    /// itself (REQ-RUNNERENROLL-89).
    TreeFinished {
        final_execution_event_sequence: u64,
        outcome: Value,
        artifact_delivery: Value,
        diagnostic: Option<Value>,
        exit: um_runner_protocol::TreeExit,
    },
    Interrupted {
        final_execution_event_sequence: u64,
        reason: String,
        terminal_outcome: Value,
        artifact_delivery: Value,
    },
    Aborted {
        last_execution_event_sequence: u64,
        reason: String,
    },
}

impl ExecutionReport {
    pub(in crate::service) fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::AssignmentInterrupted { .. }
                | Self::Finished { .. }
                | Self::TreeFinished { .. }
                | Self::Interrupted { .. }
                | Self::Aborted { .. }
        )
    }

    /// Whether this is a progress report for step observations alone.
    pub(in crate::service) fn is_steps_only(&self) -> bool {
        matches!(
            self,
            Self::TreeProgress {
                steps_only: true,
                ..
            }
        )
    }

    pub(super) fn is_condition_evidence_transition(&self) -> bool {
        matches!(
            self,
            Self::Transition { workflow_event, .. }
                if is_condition_evidence_workflow_event(workflow_event)
        )
    }

    pub(in crate::service) fn runner_frame(
        &self,
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
    ) -> RunnerFrame {
        match self {
            Self::AssignmentInterrupted { reason } => RunnerFrame::AssignmentInterrupted {
                envelope,
                assignment_id,
                attempt_id,
                reason: reason.clone(),
            },
            Self::Started => RunnerFrame::ExecutionStarted {
                envelope,
                assignment_id,
                attempt_id,
            },
            Self::Transition {
                execution_event_sequence,
                workflow_event,
                diagnostic,
            } => RunnerFrame::ExecutionTransition {
                envelope,
                assignment_id,
                attempt_id,
                execution_event_sequence: *execution_event_sequence,
                workflow_event: workflow_event.clone(),
                diagnostic: diagnostic.clone(),
            },
            Self::TreeProgress {
                progress_sequence,
                progress,
                ..
            } => RunnerFrame::TreeProgress {
                envelope,
                assignment_id,
                attempt_id,
                progress_sequence: *progress_sequence,
                progress: progress.clone(),
            },
            Self::TriggerDeliveryApplied { application } => RunnerFrame::TriggerDeliveryApplied {
                envelope,
                assignment_id,
                attempt_id,
                application: application.clone(),
            },
            Self::RunControlApplied { application } => RunnerFrame::RunControlApplied {
                envelope,
                assignment_id,
                attempt_id,
                application: application.clone(),
            },
            Self::Finished {
                final_execution_event_sequence,
                outcome,
                artifact_delivery,
                diagnostic,
            } => RunnerFrame::ExecutionFinished {
                envelope,
                assignment_id,
                attempt_id,
                final_execution_event_sequence: *final_execution_event_sequence,
                outcome: outcome.clone(),
                artifact_delivery: artifact_delivery.clone(),
                diagnostic: diagnostic.clone(),
                tree_exit: None,
            },
            Self::TreeFinished {
                final_execution_event_sequence,
                outcome,
                artifact_delivery,
                diagnostic,
                exit,
            } => RunnerFrame::ExecutionFinished {
                envelope,
                assignment_id,
                attempt_id,
                final_execution_event_sequence: *final_execution_event_sequence,
                outcome: outcome.clone(),
                artifact_delivery: artifact_delivery.clone(),
                diagnostic: diagnostic.clone(),
                tree_exit: Some(exit.clone()),
            },
            Self::Interrupted {
                final_execution_event_sequence,
                reason,
                terminal_outcome,
                artifact_delivery,
            } => RunnerFrame::ExecutionInterrupted {
                envelope,
                assignment_id,
                attempt_id,
                final_execution_event_sequence: *final_execution_event_sequence,
                reason: reason.clone(),
                terminal_outcome: terminal_outcome.clone(),
                artifact_delivery: artifact_delivery.clone(),
            },
            Self::Aborted {
                last_execution_event_sequence,
                reason,
            } => RunnerFrame::ExecutionAborted {
                envelope,
                assignment_id,
                attempt_id,
                last_execution_event_sequence: *last_execution_event_sequence,
                reason: reason.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) enum ArtifactRequest {
    RegisterCarrier {
        assignment_id: String,
        attempt_id: String,
        portable_owner_path: String,
        media_type: String,
        size_bytes: u64,
        sha256: String,
        idempotency_key: String,
    },
    ConfirmCarrier {
        assignment_id: String,
        attempt_id: String,
        artifact_set_id: String,
        carrier_id: String,
    },
    RegisterResult {
        assignment_id: String,
        attempt_id: String,
        size_bytes: u64,
        sha256: String,
    },
    ConfirmResult {
        assignment_id: String,
        attempt_id: String,
        artifact_set_id: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum ArtifactRequestKind {
    RegisterCarrier,
    ConfirmCarrier,
    RegisterResult,
    ConfirmResult,
}

impl ArtifactRequest {
    pub(super) fn kind(&self) -> ArtifactRequestKind {
        match self {
            Self::RegisterCarrier { .. } => ArtifactRequestKind::RegisterCarrier,
            Self::ConfirmCarrier { .. } => ArtifactRequestKind::ConfirmCarrier,
            Self::RegisterResult { .. } => ArtifactRequestKind::RegisterResult,
            Self::ConfirmResult { .. } => ArtifactRequestKind::ConfirmResult,
        }
    }

    pub(in crate::service) fn assignment_id(&self) -> &str {
        match self {
            Self::RegisterCarrier { assignment_id, .. }
            | Self::ConfirmCarrier { assignment_id, .. }
            | Self::RegisterResult { assignment_id, .. }
            | Self::ConfirmResult { assignment_id, .. } => assignment_id,
        }
    }

    pub(in crate::service) fn runner_frame(&self, envelope: RunnerEnvelope) -> RunnerFrame {
        match self {
            Self::RegisterCarrier {
                assignment_id,
                attempt_id,
                portable_owner_path,
                media_type,
                size_bytes,
                sha256,
                idempotency_key,
            } => RunnerFrame::ArtifactCarrierRegister {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                portable_owner_path: portable_owner_path.clone(),
                media_type: media_type.clone(),
                size_bytes: *size_bytes,
                sha256: sha256.clone(),
                idempotency_key: idempotency_key.clone(),
            },
            Self::ConfirmCarrier {
                assignment_id,
                attempt_id,
                artifact_set_id,
                carrier_id,
            } => RunnerFrame::ArtifactCarrierConfirm {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                artifact_set_id: artifact_set_id.clone(),
                carrier_id: carrier_id.clone(),
            },
            Self::RegisterResult {
                assignment_id,
                attempt_id,
                size_bytes,
                sha256,
            } => RunnerFrame::ArtifactResultRegister {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                size_bytes: *size_bytes,
                sha256: sha256.clone(),
            },
            Self::ConfirmResult {
                assignment_id,
                attempt_id,
                artifact_set_id,
            } => RunnerFrame::ArtifactResultConfirm {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                artifact_set_id: artifact_set_id.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) enum AssignmentObservation {
    Preparing {
        effect_id: String,
        assignment_id: String,
        offered_execution_spec_id: String,
    },
    PreparationProgress {
        assignment_id: String,
        attempt_id: String,
        preparation_sequence: u64,
        phase: String,
    },
    ContinuationReady {
        assignment_id: String,
        attempt_id: String,
        start_snapshot: Value,
        quiescence: Value,
        modified: Value,
    },
    Decision(AssignmentDecision),
    CancellationApplied(AssignmentCancellationApplication),
    LeaseRenewalRequested {
        assignment_id: String,
        attempt_id: String,
        current_lease_sequence: u64,
    },
    Execution {
        assignment_id: String,
        attempt_id: String,
        report: ExecutionReport,
    },
    WorkspaceRegistration {
        assignment_id: String,
        payload: serde_json::Value,
    },
    DiscoveryInventory {
        payload: serde_json::Value,
    },
    DiscoveryComplete {
        work_root_id: String,
    },
    WorkspaceRetention {
        assignment_id: String,
        attempt_id: String,
        run_id: String,
        execution_root: String,
        state: String,
        settlement_snapshot: Option<serde_json::Value>,
    },
    Artifact {
        delivery_id: u64,
        request: ArtifactRequest,
    },
}

impl AssignmentObservation {
    pub(in crate::service) fn assignment_id(&self) -> &str {
        match self {
            Self::Preparing { assignment_id, .. }
            | Self::PreparationProgress { assignment_id, .. }
            | Self::ContinuationReady { assignment_id, .. } => assignment_id,
            Self::Decision(decision) => decision.assignment_id(),
            Self::CancellationApplied(application) => &application.assignment_id,
            Self::LeaseRenewalRequested { assignment_id, .. }
            | Self::Execution { assignment_id, .. }
            | Self::WorkspaceRetention { assignment_id, .. }
            | Self::WorkspaceRegistration { assignment_id, .. } => assignment_id,
            Self::Artifact { request, .. } => request.assignment_id(),
            Self::DiscoveryInventory { .. } | Self::DiscoveryComplete { .. } => "",
        }
    }

    pub(in crate::service) fn is_terminal(&self) -> bool {
        matches!(self, Self::Execution { report, .. } if report.is_terminal())
    }

    /// The boot's scan of its work root: replayed on every session, but owed to no
    /// run. A boot that ends before reporting it leaves nothing behind, since the
    /// next boot scans and reports again (REQ-RUNNERENROLL-84, REQ-RUNNERENROLL-85).
    pub(super) fn is_boot_inventory(&self) -> bool {
        matches!(
            self,
            Self::DiscoveryInventory { .. } | Self::DiscoveryComplete { .. }
        )
    }

    pub(super) fn is_condition_evidence_transition(&self) -> bool {
        matches!(self, Self::Execution { report, .. } if report.is_condition_evidence_transition())
    }

    /// Whether the observation is owed to no run and so may be refused rather than use
    /// the slots and bytes reserved for mandatory execution observations: registration,
    /// discovery, and a progress report for steps alone (REQ-RUNNERENROLL-88).
    pub(super) fn is_optional(&self) -> bool {
        match self {
            Self::WorkspaceRegistration { .. }
            | Self::DiscoveryInventory { .. }
            | Self::DiscoveryComplete { .. } => true,
            Self::Execution { report, .. } => report.is_steps_only(),
            _ => false,
        }
    }

    pub(in crate::service) fn runner_frame(&self, envelope: RunnerEnvelope) -> RunnerFrame {
        match self {
            Self::Preparing {
                effect_id,
                assignment_id,
                offered_execution_spec_id,
            } => RunnerFrame::AssignmentPreparing {
                envelope,
                effect_id: effect_id.clone(),
                assignment_id: assignment_id.clone(),
                offered_execution_spec_id: offered_execution_spec_id.clone(),
            },
            Self::PreparationProgress {
                assignment_id,
                attempt_id,
                preparation_sequence,
                phase,
            } => RunnerFrame::AssignmentPreparationProgress {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                preparation_sequence: *preparation_sequence,
                phase: phase.clone(),
            },
            Self::ContinuationReady {
                assignment_id,
                attempt_id,
                start_snapshot,
                quiescence,
                modified,
            } => RunnerFrame::ContinuationReady {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                start_snapshot: start_snapshot.clone(),
                quiescence: quiescence.clone(),
                modified: modified.clone(),
            },
            Self::Decision(decision) => decision.runner_frame(envelope),
            Self::CancellationApplied(application) => RunnerFrame::AssignmentCancellationApplied {
                envelope,
                effect_id: application.effect_id.clone(),
                request_id: application.request_id.clone(),
                assignment_id: application.assignment_id.clone(),
                attempt_id: application.attempt_id.clone(),
                mode: application.mode,
                effective_mode: application.effective_mode,
                disposition: application.disposition,
            },
            Self::LeaseRenewalRequested {
                assignment_id,
                attempt_id,
                current_lease_sequence,
            } => RunnerFrame::ExecutionLeaseRenewalRequested {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                current_lease_sequence: *current_lease_sequence,
            },
            Self::Execution {
                assignment_id,
                attempt_id,
                report,
            } => report.runner_frame(envelope, assignment_id.clone(), attempt_id.clone()),
            Self::WorkspaceRegistration { payload, .. } | Self::DiscoveryInventory { payload } => {
                RunnerFrame::WorkspaceInventory {
                    envelope,
                    payload: payload.clone(),
                }
            }
            Self::DiscoveryComplete { work_root_id } => RunnerFrame::WorkspaceInventoryComplete {
                envelope,
                work_root_id: work_root_id.clone(),
                inventory_through_sequence: 0, // replaced at the serialized emission boundary
            },
            Self::WorkspaceRetention {
                assignment_id,
                attempt_id,
                run_id,
                execution_root,
                state,
                settlement_snapshot,
            } => RunnerFrame::WorkspaceRetentionReport {
                envelope,
                assignment_id: assignment_id.clone(),
                attempt_id: attempt_id.clone(),
                run_id: run_id.clone(),
                execution_root: execution_root.clone(),
                state: state.clone(),
                settlement_snapshot: settlement_snapshot.clone(),
            },
            Self::Artifact { request, .. } => request.runner_frame(envelope),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct RetainedObservationFrame {
    pub(in crate::service) envelope: RunnerEnvelope,
    pub(in crate::service) encoded: Arc<str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct PendingAssignmentObservation {
    pub(in crate::service) id: u64,
    pub(in crate::service) observation: AssignmentObservation,
    pub(in crate::service) retained_frame: Option<RetainedObservationFrame>,
}

impl PendingAssignmentObservation {
    pub(in crate::service) fn artifact_request(&self) -> Option<(u64, ArtifactRequestKind)> {
        match &self.observation {
            AssignmentObservation::Artifact {
                delivery_id,
                request,
            } => Some((*delivery_id, request.kind())),
            _ => None,
        }
    }
}

/// A `trigger_delivery` effect: one control delivery for the run an assignment holds
/// (REQ-TRIGGER-136).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct AssignmentTriggerDelivery {
    pub(in crate::service) effect_id: String,
    pub(in crate::service) assignment_id: String,
    pub(in crate::service) run_id: String,
    pub(in crate::service) attempt_id: String,
    pub(in crate::service) delivery: um_runner_protocol::TriggerDelivery,
}

/// A `run_control` effect: one authorized Cloud control for the run an assignment holds
/// (REQ-RUNNERENROLL-90).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct AssignmentRunControl {
    pub(in crate::service) effect_id: String,
    pub(in crate::service) assignment_id: String,
    pub(in crate::service) run_id: String,
    pub(in crate::service) attempt_id: String,
    pub(in crate::service) control: um_runner_protocol::RunControl,
}

impl AssignmentManager {
    /// Hands a delivery for the assignment this manager holds to its tree run, which applies
    /// it with `um workflow run event` and reports the application (REQ-TRIGGER-136).
    pub(in crate::service) fn handle_trigger_delivery(
        &mut self,
        delivery: AssignmentTriggerDelivery,
    ) -> Result<(), AssignmentManagerFailure> {
        if self.holds_for_tree_run(
            &delivery.assignment_id,
            &delivery.run_id,
            &delivery.attempt_id,
        )? {
            self.tree_run
                .deliver(&delivery.assignment_id, delivery.delivery);
        }
        Ok(())
    }

    /// Hands a control for the assignment this manager holds to its tree run, which applies
    /// it to the run's current wait with the `um workflow run` control command it names and
    /// reports the application (REQ-RUNNERENROLL-90).
    pub(in crate::service) fn handle_run_control(
        &mut self,
        control: AssignmentRunControl,
    ) -> Result<(), AssignmentManagerFailure> {
        if self.holds_for_tree_run(&control.assignment_id, &control.run_id, &control.attempt_id)? {
            self.tree_run
                .control(&control.assignment_id, control.control);
        }
        Ok(())
    }

    /// Whether an effect for the tree run of `assignment_id` reaches it. Cloud fences
    /// deliveries and controls by the active assignment, so one for an assignment held no
    /// longer, or never, is acknowledged and applies to nothing here; one naming the held
    /// assignment with another run or attempt conflicts as a cancellation would.
    fn holds_for_tree_run(
        &mut self,
        assignment_id: &str,
        run_id: &str,
        attempt_id: &str,
    ) -> Result<bool, AssignmentManagerFailure> {
        self.drain_events();
        let held = match &self.slot {
            Some(LocalSlot::Preparing(preparing)) => (
                &preparing.offer.assignment_id,
                &preparing.offer.run_id,
                &preparing.offer.attempt_id,
            ),
            Some(LocalSlot::Accepted(accepted)) => (
                &accepted.identity.assignment_id,
                &accepted.identity.run_id,
                &accepted.identity.attempt_id,
            ),
            Some(LocalSlot::Running(running)) => (
                &running.identity.assignment_id,
                &running.identity.run_id,
                &running.identity.attempt_id,
            ),
            Some(LocalSlot::Finishing(_) | LocalSlot::Releasing(_)) | None => return Ok(false),
        };
        if held.0 != assignment_id {
            return Ok(false);
        }
        if held.1 != run_id || held.2 != attempt_id {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        Ok(true)
    }
}
