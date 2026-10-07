use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::{Read as _, Write as _};
use std::ops::Add;
use std::os::fd::OwnedFd;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::FutureExt as _;
use opentelemetry::KeyValue;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use ring::digest::{SHA256, digest};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::artifact_delivery::{
    ArtifactDeliveryBroker, ArtifactDeliveryOutcome, ArtifactDeliverySpec,
    ClosedArtifactDeliveryFailure,
};
use super::assignment::{
    AcceptedAssignment, AssignmentObservation, CausalLease, ExecutionReport, LeaseAuthority,
    ManagerEvent, ObservationOutbox, OutboxFailure, RenewalRequestFailure, RunEvent,
    lease_clock_cause,
};
use super::lease_clock::{
    LeaseClock, LeaseClockError, LeaseInstant, LeaseWait, LeaseWaitCancellation,
};
use super::workspace::{RetentionReason, WorkspaceDisposition};
use crate::telemetry;
use um_execution::{
    ActionId, ActiveStepInvocation, AdmittedWorkflow, AgentDiagnosticSessionStore, AgentExecution,
    AgentInputStaging, AgentInputStagingFailure, ArtifactStaging, ArtifactStagingFailure,
    AuthenticatedProcessGroup, AuthenticatedSignalResult, CancellationReason, CancellationSource,
    CloudCarrierBody, CloudContinuationEvidence, CloudExecutionCapacityV1,
    CloudSourceDisplayRepositoryV1, CloudSourceDisplaySnapshotV1, CoordinationError,
    CoordinatorClock, DigestV1, DurableProcessGuardStore, ExecutionObservation, ExecutionObserver,
    FailurePolicy, FinalizationGate, FinalizationSummary, FinalizerResult, ForceAbortEvidence,
    InputStaging, InputStagingFailure, InvocationAccountingLog, NoopCommitPort,
    ObservedStepTransition, PreparedCloudWorkflowResult, PrimaryIssue, ProcessGuardRegistry,
    ProcessGuardStoreError, ProcessIdentityInspector, ProcessIdentityObservation,
    RecoveryDecisionKind, RecoveryDiagnosticKindV1, RecoveryHandlerActivity, RecoveryHandlerKind,
    RecoveryInvocationDiagnosticV1, RecoveryInvocationRoleV1, RecoveryInvocationStateV1,
    RecoveryInvocationUsageV1, RecoveryInvocationV1, RunOutcome, SchedulingGate, StepDiagnostic,
    StepDiagnosticLog, StepFailureCause, StepRecoveryState, StepState, StepStateKind,
    SystemProcessIdentityInspector, TargetExecutionNumber, TransitionEvent, TransitionObservation,
    ValidatedRecoveryHandler, ValidatedStep, WorkflowExecutionResult, WorkflowExecutionStart,
    WorkflowNodeRole, WorkflowRunCancellation, WorkflowRunFinalization,
    WorkflowRunFinalizationCancellation, WorkflowRunId, WorkflowRunResult, WorkflowRunStep,
    WorkflowRunStepKind, WorkflowRunTiming, WorkflowState, WorkflowStepTiming,
    cloud_continuation_record, command_output_v1, execute_workflow, load_cloud_continuation_seed,
    prepare_cloud_workflow_result, production_agent_dispatcher, step_recovery_summary_v1,
    summary_disposition_matches, terminate_authenticated_process_group,
};
#[cfg(test)]
use um_execution::{
    BlockedDetail, FinalizationTrigger, Prerequisite, RecoveryRoundNumber, TransitionSequence,
    spawn_isolated_command_launch,
};

mod job;
mod lease_supervision;
mod observer;
mod process_guards;
mod projection;
mod run;

use lease_supervision::*;
use observer::*;
#[cfg(test)]
use process_guards::GuardProcessControl;
pub(super) use process_guards::{AssignmentProcessGuards, RetainedQuiescence};
use projection::*;

#[derive(Clone)]
struct PostStopFence {
    fenced: Arc<Mutex<bool>>,
    workflow_git: Option<super::workflow_git::WorkflowGitAuthority>,
}

impl PostStopFence {
    fn with_workflow_git(authority: Option<super::workflow_git::WorkflowGitAuthority>) -> Self {
        Self {
            fenced: Arc::new(Mutex::new(false)),
            workflow_git: authority,
        }
    }

    fn fence(&self) {
        if let Some(authority) = &self.workflow_git {
            authority.disable();
        }
        *self.lock() = true;
    }

    fn is_fenced(&self) -> bool {
        *self.lock()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, bool> {
        self.fenced
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InfrastructureInterruption {
    RunnerShutdown,
    ExecutionLeaseExpired,
}

impl InfrastructureInterruption {
    const fn report_reason(self) -> &'static str {
        match self {
            Self::RunnerShutdown => "graceful_shutdown",
            Self::ExecutionLeaseExpired => "execution_lease_expired",
        }
    }
}

struct ExecutionCompletion {
    final_observation_id: Option<u64>,
    deferred_containment_report: Option<ExecutionReport>,
    final_delivery_deadline: Option<LeaseInstant>,
    lease_clock_failed: bool,
    fenced: bool,
    workspace_disposition: WorkspaceDisposition,
}

impl ExecutionCompletion {
    fn retained(final_observation_id: Option<u64>, reason: RetentionReason) -> Self {
        Self {
            final_observation_id,
            deferred_containment_report: None,
            final_delivery_deadline: None,
            lease_clock_failed: false,
            fenced: false,
            workspace_disposition: WorkspaceDisposition::Retain(reason),
        }
    }

    fn fenced(final_observation_id: Option<u64>) -> Self {
        let mut completion = Self::retained(final_observation_id, RetentionReason::Interrupted);
        completion.fenced = true;
        completion
    }

    fn containment_gated(
        report: ExecutionReport,
        workspace_disposition: WorkspaceDisposition,
    ) -> Self {
        Self {
            final_observation_id: None,
            deferred_containment_report: Some(report),
            final_delivery_deadline: None,
            lease_clock_failed: false,
            fenced: false,
            workspace_disposition,
        }
    }

    fn lease_clock_failed(final_observation_id: Option<u64>) -> Self {
        Self {
            final_observation_id,
            deferred_containment_report: None,
            final_delivery_deadline: None,
            lease_clock_failed: true,
            fenced: false,
            workspace_disposition: WorkspaceDisposition::Retain(RetentionReason::OutcomeUnknown),
        }
    }
}

pub(super) struct ExecutionAuthority {
    pub(super) lease_clock: LeaseClock,
    pub(super) causal_lease: CausalLease,
    pub(super) updates: tokio::sync::watch::Receiver<LeaseAuthority>,
    pub(super) start_authority: tokio::sync::watch::Receiver<bool>,
    pub(super) infrastructure_interruption:
        tokio::sync::watch::Receiver<Option<InfrastructureInterruption>>,
}

trait PreservableStaging {
    fn preserve(&self);
    fn preserve_on_drop(&self);
}

impl PreservableStaging for ArtifactStaging {
    fn preserve(&self) {
        ArtifactStaging::preserve(self);
    }

    fn preserve_on_drop(&self) {
        ArtifactStaging::preserve_on_drop(self);
    }
}

impl PreservableStaging for InputStaging {
    fn preserve(&self) {
        InputStaging::preserve(self);
    }

    fn preserve_on_drop(&self) {
        InputStaging::preserve_on_drop(self);
    }
}

impl PreservableStaging for AgentInputStaging {
    fn preserve(&self) {
        AgentInputStaging::preserve(self);
    }

    fn preserve_on_drop(&self) {
        AgentInputStaging::preserve_on_drop(self);
    }
}

struct PreserveOnDrop<T: PreservableStaging> {
    staging: T,
}

impl<T: PreservableStaging> PreserveOnDrop<T> {
    fn new(staging: T) -> Self {
        staging.preserve_on_drop();
        Self { staging }
    }
}

impl<T: PreservableStaging> std::ops::Deref for PreserveOnDrop<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.staging
    }
}

impl<T: PreservableStaging> Drop for PreserveOnDrop<T> {
    fn drop(&mut self) {
        self.staging.preserve();
    }
}

async fn mark_engine_terminal<Output>(
    execution: impl Future<Output = Output>,
    engine_terminal: Arc<AtomicBool>,
) -> Output {
    execution
        .map(move |output| {
            engine_terminal.store(true, Ordering::Release);
            output
        })
        .await
}

pub(super) struct ExecutionJob {
    accepted: AcceptedAssignment,
    outbox: ObservationOutbox,
    artifact_delivery: ArtifactDeliveryBroker,
    manager_events: tokio::sync::mpsc::UnboundedSender<ManagerEvent>,
    engine_terminal: Arc<AtomicBool>,
    lease_clock: LeaseClock,
    containment_clock: LeaseClock,
    causal_lease: CausalLease,
    pub(super) authority_updates: tokio::sync::watch::Receiver<LeaseAuthority>,
    start_authority: tokio::sync::watch::Receiver<bool>,
    infrastructure_interruption: tokio::sync::watch::Receiver<Option<InfrastructureInterruption>>,
    workspace_release_reported: AtomicBool,
    run_event: RunEvent,
}

struct RunnerResultFailure {
    code: &'static str,
    node: Option<String>,
}

impl RunnerResultFailure {
    fn new(code: &'static str) -> Self {
        Self { code, node: None }
    }

    fn for_node(code: &'static str, node: &str) -> Self {
        Self {
            code,
            node: Some(node.to_owned()),
        }
    }
}

#[cfg(test)]
fn stubborn_guard_identity() -> AuthenticatedProcessGroup {
    AuthenticatedProcessGroup::new(
        rustix::process::Pid::from_raw(41).expect("fixture pid"),
        "stubborn-fixture".to_owned(),
    )
    .expect("fixture identity")
}

#[cfg(test)]
pub(super) struct FixtureGuardProcessControl {
    alive: AtomicBool,
    kill_succeeds: bool,
    pub(super) kill_count: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl FixtureGuardProcessControl {
    pub(super) fn mark_absent(&self) {
        self.alive.store(false, Ordering::Release);
    }
}

#[cfg(test)]
impl GuardProcessControl for FixtureGuardProcessControl {
    fn observe(&self, identity: &AuthenticatedProcessGroup) -> ProcessIdentityObservation {
        if *identity != stubborn_guard_identity() {
            return SystemProcessIdentityInspector.observe(identity);
        }
        if self.alive.load(Ordering::Acquire) {
            ProcessIdentityObservation::Exact {
                leader: um_execution::LeaderState::Running,
            }
        } else {
            ProcessIdentityObservation::Absent
        }
    }

    fn terminate(&self, identity: &AuthenticatedProcessGroup) -> AuthenticatedSignalResult {
        if *identity != stubborn_guard_identity() {
            return terminate_authenticated_process_group(identity);
        }
        self.kill_count.fetch_add(1, Ordering::AcqRel);
        if self.kill_succeeds {
            self.alive.store(false, Ordering::Release);
            AuthenticatedSignalResult::Signalled
        } else {
            AuthenticatedSignalResult::Unavailable
        }
    }
}

fn cancellation_before_start_completion(reason: CancellationReason) -> ExecutionCompletion {
    match reason {
        CancellationReason::UserRequest | CancellationReason::ForceAbort => {
            ExecutionCompletion::retained(None, RetentionReason::Cancelled)
        }
        CancellationReason::TerminationRequest
        | CancellationReason::CallerOutputFailure
        | CancellationReason::RunnerShutdown
        | CancellationReason::ExecutionLeaseExpired => ExecutionCompletion::fenced(None),
    }
}

pub(super) fn cloud_execution_capacity(admitted: &AdmittedWorkflow) -> CloudExecutionCapacityV1 {
    let capacity = admitted.capacity();
    let requirements = capacity.resolved.requirements;
    let digest = &capacity.resolved.source_closure_digest;
    CloudExecutionCapacityV1 {
        execution_contract: capacity.execution_contract.as_str().to_owned(),
        source_closure_digest: DigestV1 {
            algorithm: digest.algorithm.as_str().to_owned(),
            value: digest.value.clone(),
        },
        general_maximum_transitions: requirements.general_maximum_transitions,
        selected_maximum_transitions: capacity.maximum_transitions,
        maximum_invocations: requirements.maximum_invocations,
        maximum_retained_bytes_per_invocation: requirements.maximum_retained_bytes_per_invocation,
        diagnostic_retention_bytes: requirements.diagnostic_retention_bytes,
        native_session_retention_bytes: requirements.native_session_retention_bytes,
        aggregate_retention_bytes: requirements.aggregate_retention_bytes,
        condition_transition_count: requirements.condition_transition_count,
        aggregate_condition_transition_bytes: requirements.aggregate_condition_transition_bytes,
        terminal_result_structure_bytes: requirements.terminal_result_structure_bytes,
        presentation_result_bytes: requirements.presentation_result_bytes,
        portable_result_bytes: requirements.portable_result_bytes,
        encoded_outbox_bytes: requirements.encoded_outbox_bytes,
    }
}

fn observed_workflow_cancellation(
    outcome: &RunOutcome,
    cancellation: Option<(CancellationReason, RunnerExecutionInstant)>,
) -> Option<Option<WorkflowRunCancellation>> {
    match (cancellation, outcome) {
        (
            None,
            RunOutcome::Succeeded
            | RunOutcome::Failed {
                later_cancellation: None,
                ..
            }
            // Direct force records phased force evidence, not an ordinary
            // cancellation deadline. Do not fabricate one here.
            | RunOutcome::Cancelled {
                reason: CancellationReason::ForceAbort,
            },
        ) => Some(None),
        (
            Some((reason, deadline)),
            RunOutcome::Failed {
                later_cancellation: Some(later),
                ..
            },
        ) if reason == *later => Some(Some(WorkflowRunCancellation {
            reason,
            force_stop_deadline: deadline.utc,
        })),
        (
            Some((reason, deadline)),
            RunOutcome::Cancelled {
                reason: outcome_reason,
            },
        ) if reason == *outcome_reason => Some(Some(WorkflowRunCancellation {
            reason,
            force_stop_deadline: deadline.utc,
        })),
        _ => None,
    }
}

fn internal_delivery_failure(phase: &str) -> ArtifactDeliveryOutcome {
    ArtifactDeliveryOutcome::Failed(ClosedArtifactDeliveryFailure {
        phase: phase.to_owned(),
        code: "delivery_internal_failure".to_owned(),
    })
}

fn workflow_step_kind_policy(step: &ValidatedStep) -> (WorkflowRunStepKind, FailurePolicy) {
    match step {
        ValidatedStep::Command(command) => {
            (WorkflowRunStepKind::Command, command.common.failure_policy)
        }
        ValidatedStep::Agent(agent) => (WorkflowRunStepKind::Agent, agent.common.failure_policy),
    }
}

fn artifact_delivery_result(delivery: &ArtifactDeliveryOutcome) -> Value {
    match delivery {
        ArtifactDeliveryOutcome::Prepared { artifact_set_id } => json!({
            "outcome": "prepared",
            "artifactSetId": artifact_set_id,
        }),
        ArtifactDeliveryOutcome::Failed(failure) => json!({
            "outcome": "failed",
            "phase": failure.phase,
            "code": failure.code,
        }),
        ArtifactDeliveryOutcome::Delivered { .. } | ArtifactDeliveryOutcome::AuthorityLost => {
            json!({
                "outcome": "failed",
                "phase": "confirmation",
                "code": "delivery_internal_failure",
            })
        }
    }
}

async fn verify_prepared_carriers(
    artifacts: &ArtifactStaging,
    prepared: &PreparedCloudWorkflowResult,
) -> Result<(), CarrierVerificationFailure> {
    let artifacts = artifacts.clone();
    let carriers = prepared.carriers.clone();
    tokio::task::spawn_blocking(move || {
        for (index, carrier) in carriers.iter().enumerate() {
            let failure = |code| CarrierVerificationFailure {
                code,
                member_index: Some(u64::try_from(index).unwrap_or(u64::MAX)),
            };
            match &carrier.body {
                CloudCarrierBody::Staged(staged) => {
                    let mut file = artifacts
                        .open_artifact(staged.handle())
                        .map_err(|_| failure("open_failed"))?;
                    let mut context = ring::digest::Context::new(&SHA256);
                    let mut size = 0_u64;
                    let mut buffer = [0_u8; 64 * 1024];
                    loop {
                        let read = file.read(&mut buffer).map_err(|_| failure("read_failed"))?;
                        if read == 0 {
                            break;
                        }
                        let read_size =
                            u64::try_from(read).map_err(|_| failure("size_overflow"))?;
                        size = size
                            .checked_add(read_size)
                            .ok_or_else(|| failure("size_overflow"))?;
                        context.update(&buffer[..read]);
                    }
                    if size != carrier.size_bytes {
                        return Err(failure("size_mismatch"));
                    }
                    if !digest_matches(&carrier.sha256, context.finish().as_ref()) {
                        return Err(failure("digest_mismatch"));
                    }
                }
                CloudCarrierBody::Bytes(bytes) => {
                    if u64::try_from(bytes.len()) != Ok(carrier.size_bytes) {
                        return Err(failure("size_mismatch"));
                    }
                    if !digest_matches(&carrier.sha256, digest(&SHA256, bytes).as_ref()) {
                        return Err(failure("digest_mismatch"));
                    }
                }
            }
        }
        Ok(())
    })
    .await
    .unwrap_or(Err(CarrierVerificationFailure {
        code: "verification_task_failed",
        member_index: None,
    }))
}

struct CarrierVerificationFailure {
    code: &'static str,
    member_index: Option<u64>,
}

fn digest_matches(expected: &str, digest: &[u8]) -> bool {
    if expected.len() != digest.len().saturating_mul(2) {
        return false;
    }
    expected
        .as_bytes()
        .chunks_exact(2)
        .zip(digest)
        .all(|(encoded, byte)| {
            hex_nibble(encoded[0])
                .zip(hex_nibble(encoded[1]))
                .is_some_and(|(high, low)| high << 4 | low == *byte)
        })
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

// jscpd:ignore-start
#[derive(Clone, Copy, Debug)]
struct RunnerExecutionInstant {
    monotonic: Instant,
    utc: OffsetDateTime,
}

impl Add<Duration> for RunnerExecutionInstant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self {
            monotonic: self.monotonic + duration,
            utc: self.utc + duration,
        }
    }
}
// jscpd:ignore-end

#[derive(Clone, Copy)]
struct RunnerExecutionClock;

impl CoordinatorClock for RunnerExecutionClock {
    type Instant = RunnerExecutionInstant;

    #[expect(
        clippy::disallowed_methods,
        reason = "RunnerExecutionClock is the Runner Serve workflow clock boundary"
    )]
    fn now(&mut self) -> Self::Instant {
        RunnerExecutionInstant {
            monotonic: Instant::now(),
            utc: OffsetDateTime::now_utc(),
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "RunnerExecutionClock is the Runner Serve deadline wait boundary"
    )]
    fn wait_until(&self, deadline: Self::Instant) -> impl Future<Output = ()> + Send {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.monotonic))
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    pub(in crate::service) struct LiveLeaseExecution {
        completion: tokio::sync::oneshot::Sender<&'static str>,
        task: tokio::task::JoinHandle<LeaseExecution<&'static str>>,
        cancellation: CancellationSource,
        fence: PostStopFence,
        guards: AssignmentProcessGuards,
        invocations: Arc<AtomicUsize>,
        infrastructure_interruption: tokio::sync::watch::Sender<Option<InfrastructureInterruption>>,
    }

    impl LiveLeaseExecution {
        pub(in crate::service) async fn complete(self) {
            let Self {
                completion,
                task,
                cancellation,
                fence,
                guards,
                invocations,
                infrastructure_interruption,
            } = self;
            completion
                .send("completed-after-renewal")
                .expect("live lease execution ended before completion");
            assert!(matches!(
                crate::service::test_support::with_watchdog(task)
                    .await
                    .expect("live lease execution supervision timed out")
                    .expect("live lease execution supervision task failed"),
                LeaseExecution::Completed {
                    output: "completed-after-renewal",
                    ..
                }
            ));
            assert_eq!(invocations.load(Ordering::Acquire), 1);
            assert_eq!(cancellation.cancellation_reason(), None);
            assert!(!fence.is_fenced());
            assert!(!guards.forced_containment_started());
            drop(infrastructure_interruption);
        }
    }

    pub(in crate::service) fn supervise_assignment_lease(
        lease_clock: LeaseClock,
        authority_updates: tokio::sync::watch::Receiver<LeaseAuthority>,
        causal_lease: CausalLease,
        cancellation: CancellationSource,
        assignment_id: String,
        attempt_id: String,
    ) -> LiveLeaseExecution {
        let observed_cancellation = cancellation.clone();
        let outbox = ObservationOutbox::new();
        let fence = PostStopFence::with_workflow_git(None);
        let observed_fence = fence.clone();
        let guards = AssignmentProcessGuards::new();
        let observed_guards = guards.clone();
        let (infrastructure_interruption, infrastructure_updates) =
            tokio::sync::watch::channel(None);
        let (completion, completed) = tokio::sync::oneshot::channel();
        let invocations = Arc::new(AtomicUsize::new(0));
        let observed_invocations = Arc::clone(&invocations);
        let task = tokio::spawn(async move {
            run_under_lease(
                async {
                    observed_invocations.fetch_add(1, Ordering::Release);
                    completed.await.expect("live lease execution completion")
                },
                &cancellation,
                &lease_clock,
                authority_updates,
                infrastructure_updates,
                None,
                &causal_lease,
                &outbox,
                &assignment_id,
                &attempt_id,
                &fence,
                &guards,
            )
            .await
        });
        LiveLeaseExecution {
            completion,
            task,
            cancellation: observed_cancellation,
            fence: observed_fence,
            guards: observed_guards,
            invocations,
            infrastructure_interruption,
        }
    }
}

#[cfg(test)]
mod tests;
