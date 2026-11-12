#![cfg_attr(
    test,
    allow(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used,
        reason = "unit tests use Cargo-provided fixture paths and panic shortcuts"
    )
)]

mod claude_code;
mod codex;
mod harness_installation;
mod owned_tree;
mod pi;
mod process;
mod workflow;

pub use claude_code::{
    CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION, CLAUDE_CODE_STREAM_JSON_V1_SUPPORTED_RANGE,
    ClaudeCodeIncompatibility, ClaudeCodeInstallationFailure, ClaudeCodeProbe,
    ValidatedClaudeCodeInstallation, discover_and_validate_claude_code_installation,
};
pub use codex::{
    CODEX_APP_SERVER_V1_QUALIFICATION_VERSION, CODEX_APP_SERVER_V1_SUPPORTED_RANGE,
    CodexIncompatibility, CodexInstallationFailure, CodexProbe, ValidatedCodexInstallation,
    discover_and_validate_codex_installation,
};
pub use owned_tree::{RemovalError, open_directory_at, open_regular_file_at, remove_open_tree_at};
pub use pi::{
    PI_JSON_V1_QUALIFICATION_VERSION, PI_JSON_V1_SUPPORTED_RANGE, PiIncompatibility,
    PiInstallationFailure, PiProbe, ValidatedPiInstallation, discover_and_validate_pi_installation,
};
pub use workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM;
pub use workflow::admission::admit_workflow;
pub use workflow::admission::{
    AdmissionFailure, AdmissionFailureKind, AdmittedWorkflow, CancellationPolicy,
    CancellationReason, CancellationSource, EnvironmentSnapshot, ExecutionContext,
    OrdinaryCancellationRequestResult, ResolvedAttachment, ResolvedFile, ResolvedInput,
    ResolvedInputs, ResolvedJsonInput, SourceRevisionProvenance, WorkflowCapacityBudget,
    admit_local_continuation_workflow, admit_local_workflow, admit_runner_workflow,
    completed_inputs_match, default_execution_policy_limits, local_continuation_input_failures,
    untested_harness_advisories,
};
pub use workflow::agent::WorkflowRunId;
pub use workflow::agent::dispatch::{ProductionAgentDispatcher, production_agent_dispatcher};
pub use workflow::agent_diagnostics::AgentDiagnosticSessionStore;
pub use workflow::agent_input::AgentInputStaging;
pub use workflow::archived_attempt::{
    ArchivedAttemptLoadError, load_local_archived_attempt, reconcile_current_result_publication,
};
pub use workflow::archived_presentation::safe_path as normalize_terminal_path;
pub use workflow::archived_presentation::{
    ArchivedViewOutput, ineligibility_code, operational_error_code,
};
pub use workflow::artifact::{ArtifactStaging, CaptureCancellation, StagedCarrier};
pub use workflow::cancellation::{MAXIMUM_CANCELLATION_GRACE, MINIMUM_CANCELLATION_GRACE};
pub use workflow::capacity::{
    ConditionCapacityBounds, MAXIMUM_ENCODED_OUTBOX_BYTES, RUNNER_TERMINAL_FRAME_BYTES,
    valid_condition_capacity,
};
#[cfg(feature = "test-fixtures")]
pub use workflow::child_guard::unsupplied_internal_worker_executable;
pub use workflow::child_guard::{
    internal_worker_requested as child_guard_worker_requested,
    run_internal_worker as run_child_guard_worker,
};
pub use workflow::continuation::{
    AdmissionViolation as ContinuationAdmissionViolation, PriorState as ContinuationPriorState,
};
pub use workflow::coordinator::{
    ClassifiedCommitError, CommitFailure, CommitFailureClass, CoordinationError, CoordinatorClock,
};
pub use workflow::definition_store::{
    BindingsFile, BindingsSnapshot, BoundBinding, DefinitionError, DefinitionRecord, EnabledChange,
    PublishTarget, Published, RevisionRecord, StoredDefinition, WorkflowHome, list_definitions,
    publish, read_current_bindings, read_definition, read_revision_bindings, set_enabled,
};
pub use workflow::diagnostic::StepDiagnosticLog;
pub use workflow::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
pub use workflow::document::BindingKind;
pub use workflow::document::FailurePolicy;
pub use workflow::document::FinalizationTrigger;
pub use workflow::evidence::{BlockedDetail, Prerequisite};
pub use workflow::evidence::{
    FailureDetail, InheritedDetail, InheritedPriorState, NodeDetail, PrimaryIssue,
};
pub use workflow::execution::{
    CloudStepCommitPort, NoopCommitPort, WorkflowExecutionResult, chosen_outcome, execute_workflow,
    execute_workflow_with_children, failure_outcome,
};
pub use workflow::git_capture::CloudGitCaptureProjection;
pub use workflow::input::InputStaging;
pub use workflow::invocation_accounting::InvocationAccountingLog;
pub use workflow::local_run::{
    AttemptControls, CANCELED_OUTCOME, CarryMiss, ChildCommitFailure, ChildRun, ChildRunParent,
    ContinuationDefinitionMode, DurableDeadline, DurableInvocationStateV1, DurableInvocationV1,
    FailedAttemptRestart, InitialLocalRun, ListedRun, LocalAttemptOwner,
    LocalAttemptOwnershipReleased, LocalContinuationOpen, LocalRecoveryStatus,
    LocalRetryBeginError, LocalRetryEligibility, LocalRetryOpen, LocalRetryRejection,
    LocalRunStatusSnapshot, LocalStatusError, LocalStatusResult, PendingLocalContinuation,
    PublicationFailurePhaseV1, ResolvedBy, RetryIneligibilityReason, RunListing,
    SUPERSEDED_OUTCOME, SuccessorCarry, TreeCopyMismatch, WITHDRAWN_OUTCOME, WaitChild,
    WaitInspection, WaitProbe, acquire_local_continuation, acquire_local_retry, attempt_controls,
    await_child_start_gate, await_run_lock_gate, bind_cloud_continuation_context, child_runs,
    inspect_wait, is_run_id, latest_attempt_failed, latest_child_run, list_runs,
    load_cloud_continuation_seed, locate_run, lock_holder, positioned_element,
    read_local_run_status, retain_cloud_continuation_evidence, retain_cloud_workflow_evidence,
    wait_child,
};
pub use workflow::observation::{
    ExecutionObservation, ExecutionObserver, ExecutionSettlement, ObservedExecutions,
    ObservedStepTransition, SettledExecution, StartedExecution, TransitionObservation,
};
pub use workflow::portable_artifact::{
    ArtifactDiagnostic, ArtifactValidationSummary, PortableArtifactValidation,
    PortableArtifactValidationFailure, validate_portable_artifact_set,
};
pub use workflow::presentation::{
    ChildRunPresentation, ColorChoice, PresentationConfig, PresentationFailure,
    PresentationFailureOperation, PresentationMode, PublicationPresentation,
    RequestedPresentationMode, SystemObservationClock, TerminalCapabilities, WorkflowRunOutput,
    WorkflowRunPresentation, WorkflowRunPresentationResult, human_duration, styled_terminal_text,
    superseded_rejection_message, visible_text, write_untested_harness_advisories,
};
pub use workflow::presentation_feed::{DisplayDeadline, normalize_terminal_scalar};
pub use workflow::process_group::{
    AuthenticatedProcessGroup, DurableProcessGuardStore, ProcessGuardRegistry,
    ProcessGuardStoreError, ProcessIdentityInspector, ProcessIdentityObservation,
    SystemProcessIdentityInspector, terminate_authenticated_process_group,
};
pub use workflow::publication::{
    AgentProfileReplacementV1, CloudCarrierBody, CloudContinuationEvidence,
    CloudExecutionCapacityV1, CloudResultCarrier, CloudSourceDisplayRepositoryV1,
    CloudSourceDisplaySnapshotV1, ContinuationRecordV1, DigestV1, LocalPublicationError,
    LocalPublicationPhase, PreparedCloudWorkflowResult, RecoveryDiagnosticKindV1,
    RecoveryInvocationDiagnosticV1, RecoveryInvocationRoleV1, RecoveryInvocationStateV1,
    RecoveryInvocationUsageV1, RecoveryInvocationV1, ReplacedAgentProfileV1, ReplacedHarnessV1,
    WorkflowResultV1, WorkflowRunCancellation, WorkflowRunFinalization,
    WorkflowRunFinalizationCancellation, WorkflowRunResult, WorkflowRunStep, WorkflowRunStepKind,
    WorkflowRunTerminalResultV1, WorkflowRunTiming, WorkflowStepTiming, cloud_continuation_record,
    command_output_v1, prepare_attempt_result_destination, prepare_cloud_workflow_result,
    publish_prepared_workflow_result, step_recovery_summary_v1, summary_disposition_matches,
};
pub use workflow::rejection::{RejectionDiagnostic, human_resolution_remedy};
pub use workflow::resolution::{
    ResolutionFailure, ResolvedWorkflow, resolve, resolve_with_agent_profiles,
    resolve_workflow_file, resolve_workflow_file_with_agent_profiles,
};
pub use workflow::result_validation::{
    internal_worker_requested as result_validation_worker_requested,
    run_internal_worker as run_result_validation_worker,
};
pub use workflow::run_record::{RunParent, RunState, WorkflowRunRecord};
pub use workflow::run_timing::ObservationTime;
pub use workflow::run_timing::{ObservationClock, RunTimingObservation, RunTimingSnapshot};
pub use workflow::run_view_model::{
    WorkflowRunCleanupResult, WorkflowRunPublicationResult, WorkflowRunViewModel,
};
pub use workflow::runtime::ExportValue;
pub use workflow::runtime::{
    ActionId, ActiveStepInvocation, EnteredWait, ExecutionSeed, FinalizationGate,
    FinalizationSummary, FinalizerResult, ForceAbortEvidence, InheritedDisposition, OutputProducer,
    RecoveryDecisionKind, RecoveryHandlerActivity, RecoveryHandlerKind, RunOutcome, SchedulingGate,
    StepRecoveryState, StepState, StepStateKind, TakenEdgeRecord, TransitionEvent,
    TransitionSequence, WorkflowState,
};
pub use workflow::runtime::{RecoveryRoundNumber, TargetExecutionNumber};
pub use workflow::step_runtime::spawn_isolated_command_launch;
pub use workflow::step_runtime::{
    AgentExecution, RecoveryFailureKind, StepFailureCause, StepFailureKind, WorkflowExecutionStart,
};
pub use workflow::subject_index::{
    SlotHolder, SubjectClaim, SubjectError, check as check_subject, claim as claim_subject,
    is_subject, slot as subject_slot,
};
pub use workflow::terminal_host::archived::{
    ArchivedTerminalHostExit, ArchivedWorkflowTerminalHost,
};
pub use workflow::terminal_host::{
    HostInteraction, TerminalBoundary, TerminalHostExit, TerminalInputEvent,
    TerminalLifecycleEvent, TerminalRect, WorkflowTerminalBoundary, WorkflowTerminalHost,
};
pub use workflow::validated::{
    Scheduling, ValidatedHarness, ValidatedRecoveryHandler, ValidatedStep, WorkflowNodeRole,
    WorkflowValueType,
};
pub use workflow::value::CapturedValue;
pub use workflow::wait::{
    CONTROL_ANSWER, CONTROL_APPROVE, CONTROL_CANCEL, CONTROL_HAND_BACK, CONTROL_INTERRUPT,
    CONTROL_LIFT, CONTROL_RELEASE, CONTROL_RETRY, CONTROL_STOP, CONTROL_WITHDRAW, ConditionError,
    ConditionSource, ControlEvent, Event, ExternalEvent, NoConditions, RuleError, WaitKind,
    WaitRecord, WaitRule, append_control, evaluate, event_literal, position_path, read_controls,
    read_numbered_controls, stop_in_force, wait_payload,
};
pub use workflow::workspace_snapshot::{
    CloudContinuationSnapshot, capture_cloud_continuation_snapshot,
    capture_cloud_settlement_snapshot,
};
pub use workflow::{
    MAXIMUM_PARALLEL_STEPS, STRUCTURAL_SCHEMA, is_input_name, is_lowercase_hex,
    is_valid_input_display_name, is_valid_media_type, lowercase_hex,
};
pub use workflow::{
    Mirror, MirrorAction, MirrorBlock, MirrorFailure, MirrorGuard, MirrorGuardError,
    MirrorGuardOperator, MirrorHook, MirrorSkipReason, MirrorState, MirrorTarget, MirrorWrite,
    MirrorWriteOutcome, Template, TemplateError, TemplateSegment, hook_variables,
    read_mirror_writes, read_numbered_mirror_writes,
};
pub use workflow::{
    TreeDiagnostic, TreeFile, TreeLocation, TreeTriggerKind, ValidatedTree, is_workflow_tree,
    validate_workflow_tree,
};

pub use claude_code::ClaudeCodeCapability;
pub use codex::CodexCapability;
pub use codex::CodexInstallationIdentity;
pub use harness_installation::StableVersion;
pub use harness_installation::{HarnessQualification, UntestedHarnessAdvisory};
pub use pi::PiCapability;
pub use workflow::ClaudeCodeConfig;
pub use workflow::PiConfig;
pub use workflow::admission::AdmittedExecutionContext;
pub use workflow::admission::AdmittedHarness;
pub use workflow::admission::AdmittedTree;
pub use workflow::admission::AdmittedWorkflowCapacity;
pub use workflow::admission::ExecutionLimits;
pub use workflow::admission::ExecutionPolicyLimits;
pub use workflow::admission::ResolvedJsonInputError;
pub use workflow::admission::WorkflowExecutionContract;
pub use workflow::agent_diagnostics::AgentDiagnosticSessionError;
pub use workflow::agent_input::AgentInputStagingFailure;
pub use workflow::agent_input::AgentInputStagingReleaseFailure;
pub use workflow::archived_attempt::ArchivedAttemptIneligibilityReason;
pub use workflow::archived_attempt::ArchivedAttemptIneligible;
pub use workflow::archived_attempt::ArchivedAttemptOperationalError;
pub use workflow::archived_attempt::ArchivedAttemptOperationalErrorCode;
pub use workflow::archived_attempt::LoadedLocalArchivedAttempt;
pub use workflow::archived_attempt::LocalArchivedAttempt;
pub use workflow::archived_attempt::ProjectionInvariant;
pub use workflow::archived_attempt::adopted_child_outcome;
pub use workflow::archived_presentation::ArchivedViewOutputFailure;
pub use workflow::artifact::ArtifactReadFailure;
pub use workflow::artifact::ArtifactReleaseFailure;
pub use workflow::artifact::ArtifactStagingFailure;
pub use workflow::capacity::ComputedWorkflowCapacity;
pub use workflow::codex::CodexConfig;
pub use workflow::evidence::CancellationDetail;
pub use workflow::evidence::ConditionFalseDetail;
pub use workflow::evidence::EvidenceError;
pub use workflow::evidence::NonExecutionDetail;
pub use workflow::git_capture::GitCaptureContext;
pub use workflow::git_capture::LocalGitBaseline;
pub use workflow::input::InputStagingFailure;
pub use workflow::input::InputStagingReleaseFailure;
pub use workflow::invocation_accounting::InvocationUsage;
pub use workflow::invocation_accounting::NativeSessionFact;
pub use workflow::local_run::AttemptPrivateStaging;
pub use workflow::local_run::DurableInvocationDiagnosticV1;
pub use workflow::local_run::LocalRunCommitPort;
pub use workflow::local_run::LocalRunDirectoryError;
pub use workflow::local_run::LocalStatusAttempt;
pub use workflow::local_run::LocalStatusErrorCode;
pub use workflow::local_run::OwnershipUnprovenReason;
pub use workflow::local_run::PendingLocalRetry;
pub use workflow::local_run::{
    LocalStatusNode, LocalStatusProgress, LocalStatusStart, LocalStatusStateView,
};
pub use workflow::process_group::AuthenticatedSignalResult;
pub use workflow::process_group::ProcessGuardRegistration;
pub use workflow::publication::CommandOutputV1;
pub use workflow::publication::PreparedResultDestination;
pub use workflow::publication::StepRecoverySummaryV1;
pub use workflow::publication::WorkflowStepV1;
pub use workflow::publication::{ContinuationEdgeV1, ContinuationReasonV1};
pub use workflow::rejection::RejectionLocation;
pub use workflow::resolution::ContentDigestAlgorithm;
pub use workflow::resolution::WorkflowContentDigest;
pub use workflow::resolution::WorkflowSourceProvenance;
pub use workflow::run_timing::ObservedStepTiming;
pub use workflow::run_view_model::WorkflowRunPublicationFailure;
pub use workflow::run_view_model::WorkflowRunViewModelError;
pub use workflow::run_view_model::WorkflowRunViewSnapshot;
pub use workflow::runtime::FinalizationCancellation;
pub use workflow::runtime::RunCancellationPhase;
pub use workflow::step_runtime::NoAgentDispatcher;
pub use workflow::step_runtime::{
    ChildInputValue, ChildRunControl, ChildRunDispatcher, ChildRunOutcome, ChildRunRequest,
    ChildRunStarted, ChildRuns, ControlContext, NoChildRunDispatcher,
};
pub use workflow::terminal_host::archived::ArchivedTerminalExitRequest;
pub use workflow::validated::ResolvedOutputSource;
pub use workflow::validated::ValidatedAgent;
pub use workflow::validated::ValidatedAgentStep;
pub use workflow::validated::ValidatedCommandStep;
pub use workflow::validated::ValidatedCommonStep;
pub use workflow::validated::ValidatedFinalizer;
pub use workflow::validated::ValidatedStepRecovery;
pub use workflow::validated::ValidatedWorkflow;
pub use workflow::{
    AGENT_PROFILE_REPLACEMENT_SCHEMA, AgentProfileReplacementDocument,
    MAXIMUM_AGENT_PROFILE_REPLACEMENT_BYTES, ResolvedAgentProfileReplacement,
};

pub use claude_code::ClaudeCodeStreamJsonV1Capabilities;
pub use codex::CodexAppServerV1Capabilities;
pub use pi::PiJsonV1Capabilities;
pub use process::{
    CommandOutput, CommandProbeError, CommandRequest, CommandRunner, ManagedProcessGroup,
    SystemCommandRunner,
};
pub use workflow::CanonicalJsonError;
pub use workflow::admission::ClaudeCodeStreamJsonV1Admission;
pub use workflow::admission::CodexAppServerV1Admission;
pub use workflow::admission::PiJsonV1Admission;
pub use workflow::agent::AgentInvocation;
pub use workflow::agent::AgentObservationEnvelope;
pub use workflow::agent::AgentObservationSink;
pub use workflow::agent::AgentStartCallback;
pub use workflow::agent::dispatch::AgentInvocationDispatcher;
pub use workflow::agent::{AgentCompatibilityProfile, AgentFailure};
pub use workflow::agent_input::AgentInputStartFailure;
pub use workflow::artifact::ArtifactHandle;
pub use workflow::artifact::CaptureCandidateSet;
pub use workflow::artifact::CaptureFailure;
pub use workflow::artifact::CapturedArtifact;
pub use workflow::artifact::CapturedGitBranch;
pub use workflow::capacity::WorkflowCapacity;
pub use workflow::claude_code::ClaudeCodeStreamJsonV1ProtocolLimits;
pub use workflow::codex::CodexAppServerV1ProtocolLimits;
pub use workflow::coordinator::CommitPort;
pub use workflow::coordinator::CommittedReduction;
pub use workflow::git_capture::GitCaptureFailure;
pub use workflow::git_capture::GitCommandTimeout;
pub use workflow::input::InputPreparationFailure;
pub use workflow::observation::CommandOutputClosedObservation;
pub use workflow::observation::CommandOutputObservation;
pub use workflow::pi::PiJsonV1ProtocolLimits;
pub use workflow::process_group::LeaderState;
pub use workflow::publication::DiagnosticStreamV1;
pub use workflow::publication::RunResultInvariant;
pub use workflow::recovery::RecoveryDecisionFailureKind;
pub use workflow::recovery::RecoveryHandlerFailure;
pub use workflow::runtime::ExportUnavailableReason;
pub use workflow::step_runtime::AgentExecutionObservationSink;
pub use workflow::step_runtime::CommandExecutionFailure;
pub use workflow::step_runtime::CommandLaunchFailure;
pub use workflow::step_runtime::CommandPreparationFailure;
pub use workflow::step_runtime::OutputCaptureFailure;
pub use workflow::step_runtime::StepExecutionFailure;
pub use workflow::step_runtime::StepStartFailure;
pub use workflow::step_runtime::WorkflowAgentDispatcher;
pub use workflow::step_runtime::WorkflowCommitPort;
pub use workflow::step_runtime::WorkingDirectoryFailure;
pub use workflow::value::CapturedJson;
pub use workflow::value::CapturedText;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOutcome {
    Succeeded,
    Failed,
    Interrupted,
    Terminated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentHarnessInstallationFailure {
    Pi(PiInstallationFailure),
    ClaudeCode(ClaudeCodeInstallationFailure),
    Codex(CodexInstallationFailure),
}
