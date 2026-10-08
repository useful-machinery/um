use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use serde_json::Value;
use tokio::sync::{Notify, mpsc};

use super::Sleeper;
use super::artifact_delivery::{
    ArtifactCloudResponse, ArtifactDeliveryBroker, ArtifactDeliveryProtocolFailure,
};
use super::config::Config;
use super::execution::{
    AssignmentProcessGuards, ExecutionAuthority, ExecutionJob, InfrastructureInterruption,
};
use super::lease_clock::{LeaseClock, LeaseClockError, LeaseInstant, LeaseWaitCancellation};
use super::run_inputs::{HttpRunInputBroker, PreparationDeadline, RunInputBroker, RunInputFailure};
use super::source::{HttpSourceCredentialBroker, MaterializationFailure, SourceCredentialBroker};
use super::workflow_git::{WorkflowGitAuthority, WorkflowGitInstall};
use super::workspace::{
    AssignmentRoot, AssignmentRootCreationError, CleanupResult, ProcessQuiescence, RetentionReason,
    WorkRootLease, WorkspaceDisposition,
};
use crate::control_protocol::AssignmentCounts;
use crate::telemetry::{Event as TelemetryEvent, Outcome as TelemetryOutcome};
#[cfg(test)]
use um_execution::RUNNER_TERMINAL_FRAME_BYTES;
use um_execution::{
    AdmissionFailure, AdmissionFailureKind, AdmittedWorkflow, CancellationPolicy,
    CancellationReason, CancellationSource, CaptureCancellation, CloudGitCaptureProjection,
    ConditionCapacityBounds, EnvironmentSnapshot, ExecutionContext, MAXIMUM_CANCELLATION_GRACE,
    MAXIMUM_ENCODED_OUTBOX_BYTES, MAXIMUM_PARALLEL_STEPS, MINIMUM_CANCELLATION_GRACE,
    OrdinaryCancellationRequestResult, ResolvedInputs, ResolvedWorkflow, SourceRevisionProvenance,
    ValidatedClaudeCodeInstallation, ValidatedCodexInstallation, ValidatedPiInstallation,
    WorkflowCapacityBudget, admit_runner_workflow, default_execution_policy_limits,
    valid_condition_capacity,
};
use um_runner_protocol::{
    AssignmentDecline, CancellationApplicationDisposition, CancellationMode, ContinuationOffer,
    ExecutionLeaseGrant, ExecutionLeasePolicy, ExecutionSpecInvalidReason,
    ExecutionSpecV1RunnerProjection, MAXIMUM_CONDITION_TRANSITION_FRAME_BYTES,
    MAXIMUM_ORDINARY_FRAME_BYTES, MAXIMUM_TERMINAL_FRAME_BYTES,
    PrimaryWorkspaceSourceV1RunnerProjection, RunnerEnvelope, RunnerFrame, RunnerUnableReason,
    SourceDisplaySnapshotV1RunnerProjection, WorkflowDefinitionSourceV1RunnerProjection,
    encode_runner_frame, is_condition_evidence_workflow_event,
};

mod admission;
mod causal_lease;
mod commands;
mod events;
mod finalization;
mod lease_authority;
mod observation;
mod outbox;
mod root_preparation;
mod spec_validation;

pub(super) use causal_lease::*;
pub(super) use commands::*;
pub(super) use lease_authority::*;
pub(super) use observation::*;
pub(super) use outbox::*;
use root_preparation::*;
use spec_validation::*;
const MAXIMUM_RETAINED_DECISIONS: usize = 256;
pub(super) const MAXIMUM_SERVICE_OBSERVATIONS: usize = 1_344;
pub(super) const OBSERVATION_RESERVE_BASE: usize = 64;
const FINAL_ACKNOWLEDGEMENT_GRACE: Duration = Duration::from_secs(10);
const MINIMUM_RENEWAL_HEADROOM: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AssignmentOffer {
    pub(super) effect_id: String,
    pub(super) assignment_id: String,
    pub(super) run_id: String,
    pub(super) project_id: String,
    pub(super) attempt_id: String,
    pub(super) attempt_number: u64,
    pub(super) execution_spec: ExecutionSpecV1RunnerProjection,
    pub(super) continuation: Option<Box<ContinuationOffer>>,
}

pub(super) trait AssignmentRootPreparer: Send + Sync {
    fn prepare(
        &self,
        offer: &AssignmentOffer,
        recorder: Option<Arc<crate::telemetry::Recorder>>,
    ) -> Result<AssignmentRoot, AssignmentRootCreationError>;
}

impl AssignmentRootPreparer for WorkRootLease {
    fn prepare(
        &self,
        offer: &AssignmentOffer,
        recorder: Option<Arc<crate::telemetry::Recorder>>,
    ) -> Result<AssignmentRoot, AssignmentRootCreationError> {
        // An offer only allocates the new assignment's private directory.
        // A retained root may be inspected and claimed only after the bounded
        // assignment-prepare command, never merely upon receiving an offer.
        self.create_assignment_for_attempt(
            &offer.assignment_id,
            &offer.run_id,
            &offer.attempt_id,
            recorder,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WelcomePolicyFailure {
    Invalid,
    Changed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AssignmentManagerFailure {
    ConflictingOffer,
    DecisionCapacity,
    LeaseClock,
}

struct RetainedDecision {
    offer: AssignmentOffer,
    response: AssignmentDecision,
    response_observation_id: Option<u64>,
    causal_lease: Option<CausalLease>,
    start: Option<AssignmentStart>,
    start_authorization: Option<AssignmentStartAuthorization>,
    renewals: BTreeMap<String, AssignmentRenewal>,
    rejected_renewals: BTreeMap<String, AssignmentRenewal>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AssignmentIdentity {
    assignment_id: String,
    run_id: String,
    project_id: String,
    attempt_id: String,
    execution_spec_id: String,
    source_branch: String,
    source_display_snapshot: Option<SourceDisplaySnapshotV1RunnerProjection>,
    repository_connection_id: String,
    source_object_format: String,
    source_commit_oid: String,
}

impl AssignmentIdentity {
    fn from_offer(offer: &AssignmentOffer) -> Self {
        Self {
            assignment_id: offer.assignment_id.clone(),
            run_id: offer.run_id.clone(),
            project_id: offer.project_id.clone(),
            attempt_id: offer.attempt_id.clone(),
            execution_spec_id: offer.execution_spec.execution_spec_id.clone(),
            source_branch: offer.execution_spec.source_branch.clone(),
            source_display_snapshot: offer.execution_spec.source_display_snapshot.clone(),
            repository_connection_id: offer
                .execution_spec
                .primary_workspace_source
                .repository_connection_id
                .clone(),
            source_object_format: offer
                .execution_spec
                .primary_workspace_source
                .object_format
                .clone(),
            source_commit_oid: offer
                .execution_spec
                .primary_workspace_source
                .commit_oid
                .clone(),
        }
    }
}

pub(super) struct AcceptedAssignment {
    identity: AssignmentIdentity,
    pub(super) attempt_number: u64,
    pub(super) continuation: Option<Box<ContinuationOffer>>,
    pub(super) root: AssignmentRoot,
    pub(super) admitted: AdmittedWorkflow,
    pub(super) transition_budget: usize,
    pub(super) execution_version: Arc<str>,
    pub(super) process_guards: AssignmentProcessGuards,
    pub(super) guard_processes: bool,
    pub(super) workflow_git: WorkflowGitAuthority,
}

impl AcceptedAssignment {
    pub(super) fn assignment_id(&self) -> &str {
        &self.identity.assignment_id
    }

    pub(super) fn attempt_id(&self) -> &str {
        &self.identity.attempt_id
    }

    pub(super) fn run_id(&self) -> &str {
        &self.identity.run_id
    }

    pub(super) fn project_id(&self) -> &str {
        &self.identity.project_id
    }

    pub(super) fn repository_connection_id(&self) -> &str {
        &self.identity.repository_connection_id
    }

    pub(super) fn source_object_format(&self) -> &str {
        &self.identity.source_object_format
    }

    pub(super) fn source_commit_oid(&self) -> &str {
        &self.identity.source_commit_oid
    }

    pub(super) fn source_display_snapshot(
        &self,
    ) -> Option<&SourceDisplaySnapshotV1RunnerProjection> {
        self.identity.source_display_snapshot.as_ref()
    }
}

// Shared with the execution job: the manager owns completion at the durable
// acknowledgement/fence boundary, not at the end of the engine future.
#[derive(Clone, Default)]
pub(super) struct RunEvent(Arc<Mutex<RunEventState>>);

#[derive(Default)]
struct RunEventState {
    event: Option<TelemetryEvent>,
    result: Option<&'static str>,
}

pub(super) fn lease_clock_cause(error: LeaseClockError) -> &'static str {
    match error {
        #[cfg(not(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64"))))]
        LeaseClockError::UnsupportedPlatform => "lease_platform_unsupported",
        LeaseClockError::ClockUnavailable => "lease_clock_unavailable",
        LeaseClockError::TimerUnavailable => "lease_timer_unavailable",
        LeaseClockError::TimerWaitFailed => "lease_timer_wait_failed",
        LeaseClockError::ArithmeticOverflow => "lease_arithmetic_overflow",
        LeaseClockError::IncompatibleInstant => "lease_incompatible_instant",
    }
}

impl RunEvent {
    fn start(
        &self,
        recorder: &crate::telemetry::Recorder,
        identity: &AssignmentIdentity,
        runner_id: &str,
    ) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.event.is_none() {
            state.event = Some(recorder.start(
                "runner.run",
                [
                    KeyValue::new(crate::telemetry::attribute::RUN_ID, identity.run_id.clone()),
                    KeyValue::new(
                        crate::telemetry::attribute::ASSIGNMENT_ID,
                        identity.assignment_id.clone(),
                    ),
                    KeyValue::new(
                        crate::telemetry::attribute::ATTEMPT_ID,
                        identity.attempt_id.clone(),
                    ),
                    KeyValue::new(crate::telemetry::attribute::RUNNER_ID, runner_id.to_owned()),
                    KeyValue::new(
                        crate::telemetry::attribute::RUNNER_BOOT_ID,
                        recorder.boot_id().to_owned(),
                    ),
                ],
            ));
        }
    }

    pub(super) fn set(&self, attribute: KeyValue) {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(event) = &state.event {
            event.set(attribute);
        }
    }

    fn lease_clock_failure(&self, error: LeaseClockError, stage: &'static str) {
        self.result("aborted");
        for (key, value) in [
            (
                crate::telemetry::attribute::FAILURE_CAUSE_TYPE,
                lease_clock_cause(error),
            ),
            (crate::telemetry::attribute::DIAGNOSTIC_STAGE, stage),
            (
                crate::telemetry::attribute::EXECUTOR_FAULT_REASON,
                "runner_internal_failure",
            ),
        ] {
            self.set(KeyValue::new(key, value));
        }
    }

    pub(super) fn result(&self, result: &'static str) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.result = Some(result);
        if let Some(event) = &state.event {
            event.set(KeyValue::new(
                crate::telemetry::attribute::RUN_RESULT,
                result,
            ));
        }
    }

    fn finish(&self, override_result: Option<&'static str>) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = override_result.or(state.result).unwrap_or("aborted");
        if let Some(event) = state.event.take() {
            event.set(KeyValue::new(
                crate::telemetry::attribute::RUN_RESULT,
                result,
            ));
            event.finish(match result {
                "succeeded" => TelemetryOutcome::Success,
                "cancelled" | "interrupted" => TelemetryOutcome::Cancelled,
                _ => TelemetryOutcome::Failure,
            });
        }
    }
}

struct RunningAssignment {
    identity: AssignmentIdentity,
    cancellation: CancellationSource,
    cancellation_grace: Duration,
    current_grant: ExecutionLeaseGrant,
    causal_lease: CausalLease,
    authority_updates: tokio::sync::watch::Sender<LeaseAuthority>,
    start_authority: tokio::sync::watch::Sender<bool>,
    infrastructure_interruption: tokio::sync::watch::Sender<Option<InfrastructureInterruption>>,
    workflow_git: WorkflowGitAuthority,
    engine_terminal: Arc<AtomicBool>,
    workspace_release: Option<CleanupResult>,
    run_event: RunEvent,
}

struct PreparingAssignment {
    offer: AssignmentOffer,
    cancellation: CaptureCancellation,
    root_preparation: Option<Arc<AssignmentRootPreparationHandoff>>,
    root: Option<AssignmentRoot>,
    prepare_effect_id: Option<String>,
    preparation_event: Option<TelemetryEvent>,
}

struct PreparationFence {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl PreparationFence {
    fn arm(
        deadline: PreparationDeadline,
        cancellation: CaptureCancellation,
        sleeper: Arc<dyn Sleeper>,
    ) -> Result<Self, ()> {
        let remaining = deadline.remaining_at(sleeper.now()).ok_or(())?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|_| ())?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (ready, readiness) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("runner-preparation-deadline".to_owned())
            .spawn(move || {
                runtime.block_on(async move {
                    let sleep = sleeper.sleep(remaining);
                    tokio::pin!(sleep);
                    let pending = std::future::poll_fn(|context| {
                        std::task::Poll::Ready(
                            std::future::Future::poll(sleep.as_mut(), context)
                                == std::task::Poll::Pending,
                        )
                    })
                    .await;
                    let _ = ready.send(());
                    if pending {
                        tokio::select! {
                            () = &mut sleep => cancellation.cancel(),
                            _ = stopped => {}
                        }
                    } else {
                        cancellation.cancel();
                    }
                });
            })
            .map_err(|_| ())?;
        readiness.recv().map_err(|_| ())?;
        Ok(Self {
            stop: Some(stop),
            worker: Some(worker),
        })
    }
}

impl Drop for PreparationFence {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct FinishingAssignment {
    identity: AssignmentIdentity,
    final_observation_id: u64,
    root: Option<AssignmentRoot>,
    workspace_disposition: WorkspaceDisposition,
}

// A successor has fenced the predecessor's terminal report, but its grace timer
// may still deliver an event. Keep these obligations separate from the successor slot.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct FencedFinalGrace {
    assignment_id: String,
    final_observation_id: u64,
}

enum ReleaseAfter {
    Idle,
    Reporting(Box<AssignmentIdentity>),
    PreExecutionCancellation(Box<AssignmentIdentity>),
}

struct ReleasingAssignment {
    assignment_id: String,
    after: ReleaseAfter,
    retention_report: Option<(String, String, String, String, Option<serde_json::Value>)>,
}

enum LocalSlot {
    Preparing(Box<PreparingAssignment>),
    Accepted(Box<AcceptedAssignment>),
    Running(Box<RunningAssignment>),
    Finishing(Box<FinishingAssignment>),
    Releasing(ReleasingAssignment),
}

pub(super) enum ManagerEvent {
    WorkspacePrepared {
        assignment_id: String,
        root: Result<Box<AssignmentRoot>, AssignmentRootCreationError>,
    },
    Prepared {
        offer: Box<AssignmentOffer>,
        prepare_effect_id: String,
        deadline: PreparationDeadline,
        admission: Box<Result<AcceptedAssignment, Box<(AssignmentRoot, AssignmentDecline)>>>,
    },
    WorkspaceReleased {
        assignment_id: String,
        result: CleanupResult,
    },
    Finished {
        assignment_id: String,
        final_observation_id: Option<u64>,
        final_delivery_deadline: Option<LeaseInstant>,
        lease_clock_failed: bool,
        fenced: bool,
        retained_root: Option<Box<AssignmentRoot>>,
        quiescence: ProcessQuiescence,
        quiescence_failure: Option<Vec<String>>,
        workspace_disposition: WorkspaceDisposition,
    },
    FinalGraceElapsed {
        assignment_id: String,
        final_observation_id: u64,
        continue_reporting: bool,
    },
    CleanupFinished {
        assignment_id: String,
        result: CleanupResult,
    },
    LeaseClockFailed {
        assignment_id: String,
        error: LeaseClockError,
    },
}

#[derive(Clone, Copy)]
struct PreparationAuthority<'a> {
    deadline: PreparationDeadline,
    cancellation: &'a CaptureCancellation,
    monotonic_now: Instant,
}

impl PreparationAuthority<'_> {
    fn ensure_current(self) -> Result<(), AssignmentDecline> {
        if self.cancellation.is_cancelled()
            || self.deadline.remaining_at(self.monotonic_now).is_none()
        {
            Err(AssignmentDecline::RunnerUnable(
                RunnerUnableReason::InputServiceUnavailable,
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
struct AdmissionRuntime {
    pi_installation: Option<ValidatedPiInstallation>,
    claude_code_installation: Option<ValidatedClaudeCodeInstallation>,
    codex_installation: Option<ValidatedCodexInstallation>,
    environment: EnvironmentSnapshot,
    execution_version: Arc<str>,
    outbox: ObservationOutbox,
    guard_processes: bool,
    recorder: Option<Arc<crate::telemetry::Recorder>>,
    preparation_event: Option<TelemetryEvent>,
    work_root: Arc<WorkRootLease>,
}

impl AdmissionRuntime {
    fn progress(
        &self,
        offer: &AssignmentOffer,
        preparation_sequence: u64,
        phase: &str,
    ) -> Result<(), AssignmentDecline> {
        if let Some(event) = &self.preparation_event {
            event.set(opentelemetry::KeyValue::new(
                crate::telemetry::attribute::ASSIGNMENT_PREPARATION_PHASE,
                phase.to_owned(),
            ));
        }
        self.outbox
            .enqueue(AssignmentObservation::PreparationProgress {
                assignment_id: offer.assignment_id.clone(),
                attempt_id: offer.attempt_id.clone(),
                preparation_sequence,
                phase: phase.to_owned(),
            })
            .map(|_| ())
            .map_err(|_| environment_unavailable())
    }

    fn finish(
        &self,
        offer: &AssignmentOffer,
        root: AssignmentRoot,
        workflow: ResolvedWorkflow,
        inputs: ResolvedInputs,
        git_capture: Option<CloudGitCaptureProjection>,
        authority: PreparationAuthority<'_>,
    ) -> Result<AcceptedAssignment, Box<(AssignmentRoot, AssignmentDecline)>> {
        let prepared = (|| {
            authority.ensure_current()?;
            validate_carried_capacity(&offer.execution_spec, &workflow)?;
            let cloud_git_capture = git_capture.is_some();
            let context = build_execution_context(
                &offer.execution_spec,
                &root.execution,
                git_capture,
                &self.environment,
                self.pi_installation.as_ref(),
                self.claude_code_installation.as_ref(),
                self.codex_installation.as_ref(),
            )?;
            let carried = &offer.execution_spec.capacity;
            let transition_budget = self.outbox.reserve(
                usize::try_from(carried.selected_maximum_transitions)
                    .map_err(|_| environment_unavailable())?,
                carried.encoded_outbox_bytes,
            )?;
            let admitted = admit_runner_workflow(workflow, inputs, context)
                .map_err(|failure| admission_decline(failure, cloud_git_capture))?;
            authority.ensure_current()?;
            if admitted.capacity().maximum_transitions != carried.selected_maximum_transitions {
                return Err(capacity_binding_invalid());
            }
            Ok((admitted, transition_budget))
        })();
        let (admitted, transition_budget) = match prepared {
            Ok(prepared) => prepared,
            Err(decline) => return Err(Box::new((root, decline))),
        };
        let Some(workflow_git) = root.workflow_git() else {
            return Err(Box::new((root, environment_unavailable())));
        };
        let process_guards = if self.guard_processes {
            match AssignmentProcessGuards::durable(&root.private) {
                Ok(guards) => guards,
                Err(_) => return Err(Box::new((root, environment_unavailable()))),
            }
        } else {
            AssignmentProcessGuards::new()
        };
        Ok(AcceptedAssignment {
            identity: AssignmentIdentity::from_offer(offer),
            attempt_number: offer.attempt_number,
            continuation: offer.continuation.clone(),
            root,
            admitted,
            transition_budget,
            execution_version: Arc::clone(&self.execution_version),
            process_guards,
            guard_processes: self.guard_processes,
            workflow_git,
        })
    }
}

type ProductionBrokers = (Arc<dyn SourceCredentialBroker>, Arc<dyn RunInputBroker>);

pub(super) struct AssignmentDependencies {
    work_root: Arc<WorkRootLease>,
    root_preparer: Arc<dyn AssignmentRootPreparer>,
    sleeper: Arc<dyn Sleeper>,
    source_broker: Option<Arc<dyn SourceCredentialBroker>>,
    input_broker: Option<Arc<dyn RunInputBroker>>,
    execution_version: Arc<str>,
    recorder: Option<Arc<crate::telemetry::Recorder>>,
    guard_processes: bool,
}

impl AssignmentDependencies {
    pub(super) fn new(
        work_root: Arc<WorkRootLease>,
        sleeper: Arc<dyn Sleeper>,
        source_broker: Option<Arc<dyn SourceCredentialBroker>>,
        input_broker: Option<Arc<dyn RunInputBroker>>,
        execution_version: Arc<str>,
        recorder: Option<Arc<crate::telemetry::Recorder>>,
        guard_processes: bool,
    ) -> Self {
        let root_preparer: Arc<dyn AssignmentRootPreparer> = work_root.clone();
        Self {
            work_root,
            root_preparer,
            sleeper,
            source_broker,
            input_broker,
            execution_version,
            recorder,
            guard_processes,
        }
    }

    pub(super) fn production(
        config: &Config,
        boot_id: &str,
        sleeper: Arc<dyn Sleeper>,
        work_root: Arc<WorkRootLease>,
        recorder: Arc<crate::telemetry::Recorder>,
        source_override: Option<Arc<dyn SourceCredentialBroker>>,
    ) -> Result<Self, super::ServiceError> {
        let (source_broker, input_broker) = Self::production_brokers(
            config.endpoint(),
            config.credential(),
            boot_id,
            config.repository_url_policy(),
            recorder.clone(),
            source_override,
        )?;
        Ok(Self::new(
            work_root,
            sleeper,
            Some(source_broker),
            Some(input_broker),
            Arc::from(recorder.service_version()),
            Some(recorder),
            true,
        ))
    }

    fn production_brokers(
        endpoint: &url::Url,
        credential: &crate::credential::Credential,
        boot_id: &str,
        repository_url_policy: super::config::RepositoryUrlPolicy,
        recorder: Arc<crate::telemetry::Recorder>,
        source_override: Option<Arc<dyn SourceCredentialBroker>>,
    ) -> Result<ProductionBrokers, super::ServiceError> {
        let source_broker = match source_override {
            Some(source) => source,
            None => Arc::new(
                HttpSourceCredentialBroker::new(
                    endpoint,
                    credential,
                    boot_id,
                    repository_url_policy,
                )
                .map_err(|_| super::ServiceError::SourceBrokerConfiguration(endpoint.clone()))?
                .with_recorder(recorder),
            ),
        };
        let input_broker = Arc::new(
            HttpRunInputBroker::new(endpoint, credential, boot_id)
                .map_err(|_| super::ServiceError::InputBrokerConfiguration(endpoint.clone()))?,
        );
        Ok((source_broker, input_broker))
    }
}

pub(super) struct AssignmentManager {
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the test fixture asserts work-root cleanup through this lease"
        )
    )]
    work_root: Arc<WorkRootLease>,
    root_preparer: Arc<dyn AssignmentRootPreparer>,
    root_preparation_worker: AssignmentRootPreparationWorker,
    pi_installation: Option<ValidatedPiInstallation>,
    claude_code_installation: Option<ValidatedClaudeCodeInstallation>,
    codex_installation: Option<ValidatedCodexInstallation>,
    environment: EnvironmentSnapshot,
    execution_version: Arc<str>,
    lease_clock: LeaseClock,
    sleeper: Arc<dyn Sleeper>,
    source_broker: Option<Arc<dyn SourceCredentialBroker>>,
    input_broker: Option<Arc<dyn RunInputBroker>>,
    recorder: Option<Arc<crate::telemetry::Recorder>>,
    runner_id: String,
    run_events: BTreeMap<String, RunEvent>,
    lease_policy: Option<ExecutionLeasePolicy>,
    slot: Option<LocalSlot>,
    reporting: Option<AssignmentIdentity>,
    decisions: VecDeque<RetainedDecision>,
    cancellations: VecDeque<RetainedCancellation>,
    releases: VecDeque<AssignmentRelease>,
    outbox: ObservationOutbox,
    artifact_delivery: ArtifactDeliveryBroker,
    events: mpsc::UnboundedReceiver<ManagerEvent>,
    event_sender: mpsc::UnboundedSender<ManagerEvent>,
    shutting_down: bool,
    shutdown_cleanup_deadline: Option<LeaseInstant>,
    lease_clock_failed: bool,
    lease_clock_failure_report: Option<u64>,
    cleanup_failed: bool,
    cleanup_failure_report: Option<u64>,
    quiescence_failure: Option<Vec<String>>,
    deferred_successor: Option<AssignmentOffer>,
    deferred_offer_failure: Option<AssignmentManagerFailure>,
    fenced_final_graces: BTreeSet<FencedFinalGrace>,
    guard_processes: bool,
}

impl Drop for AssignmentManager {
    fn drop(&mut self) {
        for event in self.run_events.values() {
            event.finish(Some("aborted"));
        }
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;

    pub(in crate::service) fn manager(
        config: &Config,
        boot_id: String,
        lease_clock: LeaseClock,
    ) -> AssignmentManager {
        manager_with_dependencies(
            config,
            boot_id,
            lease_clock,
            super::super::test_support::fixture_sleeper(),
            None,
            None,
            false,
        )
    }

    pub(in crate::service) fn manager_with_dependencies(
        config: &Config,
        boot_id: String,
        lease_clock: LeaseClock,
        sleeper: Arc<dyn Sleeper>,
        recorder: Option<Arc<crate::telemetry::Recorder>>,
        source_broker: Option<Arc<dyn SourceCredentialBroker>>,
        guard_processes: bool,
    ) -> AssignmentManager {
        let work_root = WorkRootLease::acquire_for_test(config.assignment().work_root(), &boot_id)
            .unwrap_or_else(|error| panic!("acquire isolated test work root: {error}"));
        let default_source_broker = HttpSourceCredentialBroker::new(
            config.endpoint(),
            config.credential(),
            &boot_id,
            config.repository_url_policy(),
        )
        .ok()
        .map(|broker| match &recorder {
            Some(recorder) => broker.with_recorder(Arc::clone(recorder)),
            None => broker,
        })
        .map(|broker| Arc::new(broker) as Arc<dyn SourceCredentialBroker>);
        let input_broker =
            HttpRunInputBroker::new(config.endpoint(), config.credential(), &boot_id)
                .ok()
                .map(|broker| Arc::new(broker) as Arc<dyn RunInputBroker>);
        let dependencies = AssignmentDependencies::new(
            work_root,
            sleeper,
            source_broker.or(default_source_broker),
            input_broker,
            Arc::from(crate::telemetry::TEST_SERVICE_VERSION),
            recorder,
            guard_processes,
        );
        AssignmentManager::new(config, lease_clock, dependencies)
    }

    pub(in crate::service) struct RenewalTimingFixture {
        clock: LeaseClock,
        control: super::super::lease_clock::ControlledLeaseClock,
        waits: tokio::sync::mpsc::UnboundedReceiver<(
            Duration,
            super::super::lease_clock::LeaseTimerRelease,
        )>,
        authority: tokio::sync::watch::Receiver<LeaseAuthority>,
        causal_lease: CausalLease,
        cancellation: CancellationSource,
        identity: AssignmentIdentity,
        initial_cancellation_headroom: Duration,
    }

    impl RenewalTimingFixture {
        pub(in crate::service) async fn start_execution_supervisor(
            &mut self,
        ) -> super::super::execution::test_support::LiveLeaseExecution {
            let execution = super::super::execution::test_support::supervise_assignment_lease(
                self.clock.clone(),
                self.authority.clone(),
                self.causal_lease.clone(),
                self.cancellation.clone(),
                self.identity.assignment_id.clone(),
                self.identity.attempt_id.clone(),
            );
            let (renewal_wait, renewal_release) =
                super::super::test_support::with_watchdog(self.waits.recv())
                    .await
                    .expect("initial renewal timer was not armed")
                    .expect("controlled lease clock closed before initial renewal timer");
            assert_eq!(renewal_wait, Duration::ZERO);
            renewal_release.release();
            let (cancellation_wait, _cancellation_release) =
                super::super::test_support::with_watchdog(self.waits.recv())
                    .await
                    .expect("initial cancellation timer was not armed")
                    .expect("controlled lease clock closed before initial cancellation timer");
            assert_eq!(cancellation_wait, self.initial_cancellation_headroom);
            execution
        }

        pub(in crate::service) async fn wait_until_execution_observes_renewal(&mut self) {
            let expected = self
                .authority
                .borrow()
                .renewal_request
                .checked_duration_since(self.clock.now().expect("read fixture lease clock"))
                .expect("renewed authority request remains in the future");
            let (renewal_wait, _renewal_release) =
                super::super::test_support::with_watchdog(self.waits.recv())
                    .await
                    .expect("renewed authority timer was not armed")
                    .expect("controlled lease clock closed before renewed authority timer");
            assert_eq!(renewal_wait, expected);
        }

        pub(in crate::service) fn advance(&self, duration: Duration) {
            self.control.advance(duration);
        }

        pub(in crate::service) fn authority_sequence(&self) -> u64 {
            self.authority.borrow().sequence
        }

        pub(in crate::service) fn cancellation_headroom(&self) -> Option<Duration> {
            self.authority
                .borrow()
                .cancellation_start
                .checked_duration_since(self.clock.now().ok()?)
                .ok()
        }

        pub(in crate::service) fn cancellation_started(&self) -> bool {
            self.cancellation.cancellation_reason().is_some()
        }
    }

    pub(in crate::service) fn install_running_renewal_fixture(
        manager: &mut AssignmentManager,
        offer: AssignmentOffer,
    ) -> RenewalTimingFixture {
        install_running_renewal_fixture_after_request(manager, offer, Duration::from_secs(29))
    }

    fn install_running_renewal_fixture_after_request(
        manager: &mut AssignmentManager,
        offer: AssignmentOffer,
        elapsed_since_request: Duration,
    ) -> RenewalTimingFixture {
        let policy = ExecutionLeasePolicy {
            schema_version: 2,
            force_stop_and_reap_budget_milliseconds: 5000,
            terminal_report_delivery_budget_milliseconds: 5000,
            renewal_delivery_budget_milliseconds: 5000,
            lease_duration_milliseconds: 371_000,
            fencing_margin_milliseconds: 11_000,
        };
        manager
            .retain_lease_policy(&policy)
            .expect("retain fixture lease policy");
        let (clock, control, waits) = super::super::lease_clock::controlled_lease_clock();
        manager.lease_clock = clock.clone();
        let initial_basis = clock.now().expect("read fixture lease clock");
        let cancellation_grace = Duration::from_secs(
            offer
                .execution_spec
                .execution_limits
                .cancellation_grace_seconds,
        );
        let initial_authority =
            LeaseAuthority::derive(1, initial_basis, &policy, cancellation_grace)
                .expect("derive initial fixture authority");
        control.advance(
            initial_authority
                .renewal_request
                .checked_duration_since(initial_basis)
                .expect("measure fixture renewal schedule"),
        );
        let renewal_basis = clock.now().expect("read fixture renewal basis");
        let causal_lease = CausalLease::new(initial_basis);
        {
            let mut state = causal_lease.lock();
            state.bases.insert(2, renewal_basis);
            state.renewal_requests.insert(2);
        }
        let remaining = initial_authority
            .cancellation_start
            .checked_duration_since(renewal_basis)
            .expect("measure fixture cancellation headroom");
        let initial_cancellation_headroom = remaining
            .checked_sub(elapsed_since_request)
            .expect("fixture delay remains before cancellation");
        control.advance(elapsed_since_request);

        let identity = AssignmentIdentity::from_offer(&offer);
        let response = AssignmentDecision::Accepted {
            effect_id: offer.effect_id.clone(),
            assignment_id: offer.assignment_id.clone(),
            offered_execution_spec_id: offer.execution_spec.execution_spec_id.clone(),
        };
        let start = AssignmentStart {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5aby".to_owned(),
            assignment_id: offer.assignment_id.clone(),
            run_id: offer.run_id.clone(),
            attempt_id: offer.attempt_id.clone(),
            execution_spec_id: offer.execution_spec.execution_spec_id.clone(),
            lease: ExecutionLeaseGrant { sequence: 1 },
        };
        manager.decisions.push_back(RetainedDecision {
            offer,
            response,
            response_observation_id: None,
            causal_lease: Some(causal_lease.clone()),
            start: Some(start),
            start_authorization: None,
            renewals: BTreeMap::new(),
            rejected_renewals: BTreeMap::new(),
        });
        let cancellation = CancellationSource::new();
        let (authority_updates, authority) = tokio::sync::watch::channel(initial_authority);
        let (start_authority, _start_authority) = tokio::sync::watch::channel(true);
        let (infrastructure_interruption, _infrastructure_interruption) =
            tokio::sync::watch::channel(None);
        let broker = manager
            .source_broker
            .clone()
            .expect("fixture assignment source broker");
        let workflow_git = super::super::workflow_git::test_support::lease_authority_fixture(
            &identity.assignment_id,
            broker,
            Arc::clone(&manager.sleeper),
        );
        manager.slot = Some(LocalSlot::Running(Box::new(RunningAssignment {
            identity: identity.clone(),
            cancellation: cancellation.clone(),
            cancellation_grace,
            current_grant: ExecutionLeaseGrant { sequence: 1 },
            causal_lease: causal_lease.clone(),
            authority_updates,
            start_authority,
            infrastructure_interruption,
            workflow_git,
            engine_terminal: Arc::new(AtomicBool::new(false)),
            workspace_release: None,
            run_event: RunEvent::default(),
        })));
        RenewalTimingFixture {
            clock,
            control,
            waits,
            authority,
            causal_lease,
            cancellation,
            identity,
            initial_cancellation_headroom,
        }
    }

    pub(in crate::service) fn install_root_preparer(
        manager: &mut AssignmentManager,
        root_preparer: Arc<dyn AssignmentRootPreparer>,
    ) {
        manager.root_preparer = root_preparer;
    }

    pub(in crate::service) fn observation_retained(manager: &AssignmentManager, id: u64) -> bool {
        manager.outbox.contains(id)
    }

    pub(in crate::service) fn artifact_delivery(
        manager: &AssignmentManager,
    ) -> ArtifactDeliveryBroker {
        manager.artifact_delivery.clone()
    }

    pub(in crate::service) fn cleanup_complete(manager: &mut AssignmentManager) -> bool {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Releasing(_)))
    }

    pub(in crate::service) fn enqueue_lease_clock_failure_report(manager: &mut AssignmentManager) {
        let final_observation_id = manager
            .outbox
            .enqueue(AssignmentObservation::Execution {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                report: ExecutionReport::Aborted {
                    last_execution_event_sequence: 40,
                    reason: "runner_internal_failure".to_owned(),
                },
            })
            .expect("enqueue fixture lease clock failure report");
        manager.begin_lease_clock_failure_reporting(final_observation_id);
    }

    pub(in crate::service) fn enqueue_transitions(manager: &AssignmentManager, count: u64) {
        for sequence in 1..=count {
            manager
                .outbox
                .enqueue(AssignmentObservation::Execution {
                    assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    report: ExecutionReport::Transition {
                        diagnostic: None,
                        execution_event_sequence: sequence,
                        workflow_event: serde_json::json!({
                            "eventVersion": 1,
                            "eventType": "step_state_changed",
                            "transitionSequence": sequence,
                            "stepId": "fixture",
                            "role": "step",
                            "failurePolicy": "required",
                            "from": "pending",
                            "to": "starting",
                        }),
                    },
                })
                .unwrap();
        }
    }

    pub(in crate::service) fn enqueue_finalization_terminal(manager: &AssignmentManager) {
        manager
            .outbox
            .enqueue(AssignmentObservation::Execution {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                report: ExecutionReport::Finished {
                    diagnostic: None,
                    final_execution_event_sequence: 1,
                    outcome: serde_json::json!({
                        "outcome": "succeeded",
                        "forceAbort": null,
                        "finalization": {
                            "trigger": "succeeded",
                            "finalizers": [{
                                "id": "cleanup",
                                "role": "finalizer",
                                "failurePolicy": "required",
                                "state": "succeeded",
                            }],
                            "issues": [],
                            "forceAbort": false,
                        },
                    }),
                    artifact_delivery: serde_json::json!({
                        "outcome": "prepared",
                        "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
                    }),
                },
            })
            .unwrap();
    }

    pub(in crate::service) fn active_step_count(manager: &AssignmentManager) -> Option<usize> {
        match &manager.slot {
            Some(LocalSlot::Accepted(accepted)) => {
                Some(accepted.admitted.workflow().definition.steps.len())
            }
            _ => None,
        }
    }

    // jscpd:ignore-start -- Test offers and production result metadata use distinct protocol projections.
    pub(in crate::service) fn align_fixture_capacity(
        execution_spec: &mut ExecutionSpecV1RunnerProjection,
        workflow: &ResolvedWorkflow,
    ) {
        let digest = &workflow.capacity.source_closure_digest;
        let requirements = workflow.capacity.requirements;
        let projected_digest = um_runner_protocol::WorkflowSourceClosureDigestV1RunnerProjection {
            algorithm: digest.algorithm.as_str().to_owned(),
            value: digest.value.clone(),
        };
        execution_spec
            .workflow_definition_source
            .workflow_source_closure_digest = projected_digest.clone();
        execution_spec.capacity = um_runner_protocol::ExecutionCapacityV1RunnerProjection {
            execution_contract: "workflow_v1_cloud_inputs_artifacts@1".to_owned(),
            source_closure_digest: projected_digest,
            general_maximum_transitions: requirements.general_maximum_transitions,
            selected_maximum_transitions: requirements.cloud_maximum_transitions,
            maximum_invocations: requirements.maximum_invocations,
            maximum_retained_bytes_per_invocation: requirements
                .maximum_retained_bytes_per_invocation,
            diagnostic_retention_bytes: requirements.diagnostic_retention_bytes,
            native_session_retention_bytes: requirements.native_session_retention_bytes,
            aggregate_retention_bytes: requirements.aggregate_retention_bytes,
            condition_transition_count: requirements.condition_transition_count,
            aggregate_condition_transition_bytes: requirements.aggregate_condition_transition_bytes,
            terminal_result_structure_bytes: requirements.terminal_result_structure_bytes,
            presentation_result_bytes: requirements.presentation_result_bytes,
            portable_result_bytes: requirements.portable_result_bytes,
            encoded_outbox_bytes: requirements.encoded_outbox_bytes,
        };
    }
}
// jscpd:ignore-end

fn validate_carried_capacity(
    execution_spec: &ExecutionSpecV1RunnerProjection,
    workflow: &ResolvedWorkflow,
) -> Result<(), AssignmentDecline> {
    let carried = &execution_spec.capacity;
    let resolved = workflow.capacity.requirements;
    let digest = &workflow.capacity.source_closure_digest;
    if carried.source_closure_digest.algorithm != digest.algorithm.as_str()
        || carried.source_closure_digest.value != digest.value
        || carried.source_closure_digest
            != execution_spec
                .workflow_definition_source
                .workflow_source_closure_digest
    {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch,
        ));
    }
    if carried.execution_contract != "workflow_v1_cloud_inputs_artifacts@1"
        || carried.general_maximum_transitions != resolved.general_maximum_transitions
        || carried.selected_maximum_transitions != resolved.cloud_maximum_transitions
        || carried.maximum_invocations != resolved.maximum_invocations
        || carried.maximum_retained_bytes_per_invocation
            != resolved.maximum_retained_bytes_per_invocation
        || carried.diagnostic_retention_bytes != resolved.diagnostic_retention_bytes
        || carried.native_session_retention_bytes != resolved.native_session_retention_bytes
        || carried.aggregate_retention_bytes != resolved.aggregate_retention_bytes
        || carried.condition_transition_count != resolved.condition_transition_count
        || carried.aggregate_condition_transition_bytes
            != resolved.aggregate_condition_transition_bytes
        || carried.terminal_result_structure_bytes != resolved.terminal_result_structure_bytes
        || carried.presentation_result_bytes != resolved.presentation_result_bytes
        || carried.portable_result_bytes != resolved.portable_result_bytes
        || carried.encoded_outbox_bytes != resolved.encoded_outbox_bytes
    {
        return Err(capacity_binding_invalid());
    }
    Ok(())
}

fn capacity_binding_invalid() -> AssignmentDecline {
    AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::WorkflowAdmissionInvalid)
}

fn build_execution_context(
    execution_spec: &ExecutionSpecV1RunnerProjection,
    root: &Path,
    git_capture: Option<CloudGitCaptureProjection>,
    environment: &EnvironmentSnapshot,
    pi_installation: Option<&ValidatedPiInstallation>,
    claude_code_installation: Option<&ValidatedClaudeCodeInstallation>,
    codex_installation: Option<&ValidatedCodexInstallation>,
) -> Result<ExecutionContext, AssignmentDecline> {
    let maximum_parallel_steps =
        usize::try_from(execution_spec.execution_limits.maximum_parallel_steps)
            .map_err(|_| invalid_execution_limits())?;
    let cancellation_grace =
        Duration::from_secs(execution_spec.execution_limits.cancellation_grace_seconds);
    let capacity = &execution_spec.capacity;
    let context = ExecutionContext::new(
        root.to_owned(),
        default_execution_policy_limits(maximum_parallel_steps),
        environment.without_managed_runner_credentials_and_helpers(),
        CancellationPolicy::new(CancellationSource::new(), cancellation_grace),
    )
    .with_source_revision(SourceRevisionProvenance::new(
        execution_spec.source_branch.as_str(),
        execution_spec.primary_workspace_source.commit_oid.as_str(),
    ))
    .with_capacity_budget(WorkflowCapacityBudget {
        maximum_invocations: capacity.maximum_invocations,
        diagnostic_retention_bytes: capacity.diagnostic_retention_bytes,
        native_session_retention_bytes: capacity.native_session_retention_bytes,
        aggregate_retention_bytes: capacity.aggregate_retention_bytes,
        encoded_outbox_bytes: capacity.encoded_outbox_bytes,
    });
    let context = match git_capture {
        Some(projection) => context.with_cloud_git_capture(projection),
        None => context,
    };
    let context = match pi_installation {
        Some(installation) => context.with_pi_installation(installation.clone()),
        None => context,
    };
    let context = match claude_code_installation {
        Some(installation) => context.with_claude_code_installation(installation.clone()),
        None => context,
    };
    Ok(match codex_installation {
        Some(installation) => context.with_codex_installation(installation.clone()),
        None => context,
    })
}

fn revoke_authority(running: &mut RunningAssignment) {
    disable_workflow_git_off_thread(&running.workflow_git);
    running.authority_updates.send_modify(|authority| {
        authority.revoked = true;
    });
    running
        .cancellation
        .request_cancellation(CancellationReason::ExecutionLeaseExpired);
}

fn disable_workflow_git_off_thread(workflow_git: &WorkflowGitAuthority) {
    if workflow_git.fence_without_wake() {
        let workflow_git = workflow_git.clone();
        tokio::task::spawn_blocking(move || workflow_git.wake());
    }
}

fn run_input_decline(failure: RunInputFailure) -> AssignmentDecline {
    let cause = match failure {
        RunInputFailure::AssignmentFenced => "assignment_fenced",
        RunInputFailure::ServiceUnavailable => "input_service_unavailable",
        RunInputFailure::EnvironmentUnavailable => "execution_root_unavailable",
        _ => "admission_invalid",
    };
    run_input_decline_code(failure).diagnosed("input_materialization", cause)
}

fn run_input_decline_code(failure: RunInputFailure) -> AssignmentDecline {
    match failure {
        RunInputFailure::ServiceUnavailable => {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::InputServiceUnavailable)
        }
        RunInputFailure::AssignmentFenced => {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::InputServiceUnavailable)
        }
        RunInputFailure::EnvironmentUnavailable => environment_unavailable(),
        RunInputFailure::InvalidProjection => AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InvalidInputProjection,
        ),
        RunInputFailure::ManifestMismatch => AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InputManifestMismatch,
        ),
        RunInputFailure::ContentUnavailable => AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InputContentUnavailable,
        ),
        RunInputFailure::ContentMismatch => AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InputContentMismatch,
        ),
        RunInputFailure::TextInvalid => {
            AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::InputTextInvalid)
        }
        RunInputFailure::JsonInvalid => {
            AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::InputJsonInvalid)
        }
    }
}

fn materialization_decline(failure: MaterializationFailure) -> AssignmentDecline {
    let cause = match failure {
        MaterializationFailure::ProviderUnavailable => "source_provider_unavailable",
        MaterializationFailure::RepositoryUnavailable => "source_repository_unavailable",
        MaterializationFailure::AssignmentFenced => "assignment_fenced",
        MaterializationFailure::WorkflowUnavailable
        | MaterializationFailure::WorkflowDigestMismatch => "workflow_source_unavailable",
        MaterializationFailure::EnvironmentUnavailable => "execution_root_unavailable",
        _ => "admission_invalid",
    };
    materialization_decline_code(failure).diagnosed("source_materialization", cause)
}

fn materialization_decline_code(failure: MaterializationFailure) -> AssignmentDecline {
    let reason = match failure {
        MaterializationFailure::UnsupportedObjectFormat => {
            return AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::UnsupportedSourceObjectFormat,
            );
        }
        MaterializationFailure::CommitUnavailable => {
            ExecutionSpecInvalidReason::SourceCommitUnavailable
        }
        MaterializationFailure::CommitMismatch => ExecutionSpecInvalidReason::SourceCommitMismatch,
        MaterializationFailure::DirtyCheckout => ExecutionSpecInvalidReason::SourceCheckoutDirty,
        MaterializationFailure::WorkflowUnavailable => {
            ExecutionSpecInvalidReason::WorkflowSourceInvalid
        }
        MaterializationFailure::WorkflowDigestMismatch => {
            ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch
        }
        MaterializationFailure::ProviderUnavailable
        | MaterializationFailure::RepositoryUnavailable
        | MaterializationFailure::AssignmentFenced => {
            return AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable);
        }
        MaterializationFailure::EnvironmentUnavailable => return environment_unavailable(),
    };
    AssignmentDecline::ExecutionSpecInvalid(reason)
}

fn invalid_execution_limits() -> AssignmentDecline {
    AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::InvalidExecutionLimits)
}

fn admission_decline(failure: AdmissionFailure, cloud_git_capture: bool) -> AssignmentDecline {
    let kind = failure.kind();
    let (stage, cause) = admission_diagnostic(kind);
    admission_decline_code(failure, cloud_git_capture).diagnosed(stage, cause)
}

fn admission_diagnostic(kind: AdmissionFailureKind) -> (&'static str, &'static str) {
    match kind {
        AdmissionFailureKind::GitContextUnavailable => ("admission", "git_context_unavailable"),
        AdmissionFailureKind::GitContextNotRepository => ("admission", "git_context_invalid"),
        AdmissionFailureKind::GitContextExecutionRootMismatch => {
            ("execution_root", "execution_root_unavailable")
        }
        _ if kind.is_execution_root_failure() => ("execution_root", "execution_root_unavailable"),
        _ => ("admission", "admission_invalid"),
    }
}

fn admission_decline_code(failure: AdmissionFailure, cloud_git_capture: bool) -> AssignmentDecline {
    let kind = failure.kind();
    if cloud_git_capture {
        match kind {
            AdmissionFailureKind::GitObjectFormatUnsupported => {
                return AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::UnsupportedSourceObjectFormat,
                );
            }
            AdmissionFailureKind::GitBaselineUnavailable => {
                return AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::SourceCommitMismatch,
                );
            }
            AdmissionFailureKind::GitInitialWorkspaceDirty => {
                return AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::SourceCheckoutDirty,
                );
            }
            AdmissionFailureKind::GitWorkflowDigestMismatch => {
                return AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch,
                );
            }
            AdmissionFailureKind::GitContextUnavailable
            | AdmissionFailureKind::GitContextNotRepository
            | AdmissionFailureKind::GitContextExecutionRootMismatch => {
                return environment_unavailable();
            }
            _ => {}
        }
    }
    if kind.is_execution_root_failure() {
        environment_unavailable()
    } else if kind.is_projected_execution_limit_failure() {
        invalid_execution_limits()
    } else if kind == AdmissionFailureKind::AgentStepRuntimeUnsupported {
        AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowEnvironmentUnsupported)
    } else {
        AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::WorkflowAdmissionInvalid,
        )
    }
}

fn release_unclaimed_assignment_root(root: AssignmentRoot) {
    tokio::task::spawn_blocking(move || {
        let _ = root
            .release_pending(
                ProcessQuiescence::Proven,
                WorkspaceDisposition::Retain(RetentionReason::Failed),
            )
            .wait();
    });
}

fn environment_unavailable() -> AssignmentDecline {
    AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
}

fn cancellation_application(
    cancel: &AssignmentCancel,
    effective_mode: CancellationMode,
    disposition: CancellationApplicationDisposition,
) -> AssignmentCancellationApplication {
    AssignmentCancellationApplication {
        effect_id: cancel.effect_id.clone(),
        request_id: cancel.request_id.clone(),
        assignment_id: cancel.assignment_id.clone(),
        attempt_id: cancel.attempt_id.clone(),
        mode: cancel.mode,
        effective_mode,
        disposition,
    }
}

fn cancellation_disposition(
    requested_mode: CancellationMode,
    effective_mode: CancellationMode,
    terminal: bool,
) -> CancellationApplicationDisposition {
    if requested_mode == CancellationMode::Graceful && effective_mode == CancellationMode::Force {
        CancellationApplicationDisposition::Superseded
    } else if terminal {
        CancellationApplicationDisposition::ExecutionTerminal
    } else {
        CancellationApplicationDisposition::PreExecutionStopped
    }
}

fn rejected(offer: &AssignmentOffer, decline: AssignmentDecline) -> AssignmentDecision {
    AssignmentDecision::Rejected {
        effect_id: offer.effect_id.clone(),
        assignment_id: offer.assignment_id.clone(),
        decline,
    }
}

fn same_assignment(left: &AssignmentOffer, right: &AssignmentOffer) -> bool {
    left.assignment_id == right.assignment_id
        && left.run_id == right.run_id
        && left.project_id == right.project_id
        && left.attempt_id == right.attempt_id
        && left.attempt_number == right.attempt_number
        && left.execution_spec == right.execution_spec
        && left.continuation == right.continuation
}

fn start_matches_offer(start: &AssignmentStart, offer: &AssignmentOffer) -> bool {
    start.assignment_id == offer.assignment_id
        && start.run_id == offer.run_id
        && start.attempt_id == offer.attempt_id
        && start.execution_spec_id == offer.execution_spec.execution_spec_id
}

#[cfg(feature = "test-fixtures")]
#[path = "../../tests/support/nested_workflow_scenario.rs"]
mod nested_workflow_scenario;
#[cfg(feature = "test-fixtures")]
pub(crate) use nested_workflow_scenario::run_nested_workflow_delivery_failure_scenario;

#[cfg(test)]
mod tests;
