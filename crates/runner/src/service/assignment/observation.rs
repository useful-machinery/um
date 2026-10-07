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
    },
    Finished {
        final_execution_event_sequence: u64,
        outcome: Value,
        artifact_delivery: Value,
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
                | Self::Interrupted { .. }
                | Self::Aborted { .. }
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
            } => RunnerFrame::ExecutionTransition {
                envelope,
                assignment_id,
                attempt_id,
                execution_event_sequence: *execution_event_sequence,
                workflow_event: workflow_event.clone(),
            },
            Self::Finished {
                final_execution_event_sequence,
                outcome,
                artifact_delivery,
            } => RunnerFrame::ExecutionFinished {
                envelope,
                assignment_id,
                attempt_id,
                final_execution_event_sequence: *final_execution_event_sequence,
                outcome: outcome.clone(),
                artifact_delivery: artifact_delivery.clone(),
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
            | Self::WorkspaceRetention { assignment_id, .. } => assignment_id,
            Self::Artifact { request, .. } => request.assignment_id(),
        }
    }

    pub(in crate::service) fn is_terminal(&self) -> bool {
        matches!(self, Self::Execution { report, .. } if report.is_terminal())
    }

    pub(super) fn is_condition_evidence_transition(&self) -> bool {
        matches!(self, Self::Execution { report, .. } if report.is_condition_evidence_transition())
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
