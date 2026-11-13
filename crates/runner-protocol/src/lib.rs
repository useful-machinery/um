use std::{fmt, sync::OnceLock};

use jsonschema::Validator;
use serde_json::{Value, json};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};

// cargo-typify emits public declarations; keep its output reproducible and contain
// the binary crate's visibility exception to this generated module.
#[allow(
    dead_code,
    unreachable_pub,
    clippy::unwrap_used,
    clippy::large_enum_variant,
    clippy::enum_variant_names,
    reason = "decode_cloud_frame and encode_runner_frame call generated protocol codecs while cargo-typify retains the full schema surface"
)]
mod generated;

const PROTOCOL_SCHEMA: &str = include_str!("schema/runner-protocol-v1.schema.json");
pub const MAXIMUM_ORDINARY_FRAME_BYTES: usize = 262_144;
pub const MAXIMUM_CONDITION_TRANSITION_FRAME_BYTES: usize = 256 * 1024 * 1024;
pub const MAXIMUM_TERMINAL_FRAME_BYTES: usize = 512 * 1024 * 1024;
const PROTOCOL_VERSION: i64 = 1;
const PAYLOAD_VERSION: i64 = 1;
const RUNNER_TO_CLOUD: &str = "runner_to_cloud";
const CLOUD_TO_RUNNER: &str = "cloud_to_runner";

static PROTOCOL_VALIDATOR: OnceLock<Result<Validator, ()>> = OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerEnvelope {
    pub message_id: String,
    pub runner_id: String,
    pub boot_id: String,
    pub sequence: u64,
    pub sent_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerFrame {
    Hello {
        envelope: RunnerEnvelope,
        runner_version: String,
        recovery_root: Option<Value>,
        /// Advertises `treeExecution: 1`: this runner executes published trees
        /// (REQ-RUNNERENROLL-91).
        tree_execution: bool,
        /// Advertises `treeContinuation: 1`: this runner adopts a published
        /// tree an earlier attempt left in its retained workspace, so Cloud may
        /// offer it a definition-backed run's continuation (REQ-COMPAT-18).
        tree_continuation: bool,
    },
    WorkspaceInventoryComplete {
        envelope: RunnerEnvelope,
        work_root_id: String,
        inventory_through_sequence: u64,
    },
    WorkspaceInventory {
        envelope: RunnerEnvelope,
        payload: Value,
    },
    WorkspaceRetentionReport {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        run_id: String,
        execution_root: String,
        state: String,
        settlement_snapshot: Option<serde_json::Value>,
    },
    EffectAcknowledged {
        envelope: RunnerEnvelope,
        effect_id: String,
    },
    AssignmentPreparing {
        envelope: RunnerEnvelope,
        effect_id: String,
        assignment_id: String,
        offered_execution_spec_id: String,
    },
    AssignmentPreparationProgress {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        preparation_sequence: u64,
        phase: String,
    },
    ContinuationReady {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        start_snapshot: Value,
        quiescence: Value,
        modified: Value,
    },
    AssignmentAccepted {
        envelope: RunnerEnvelope,
        effect_id: String,
        assignment_id: String,
        offered_execution_spec_id: String,
    },
    AssignmentRejected {
        envelope: RunnerEnvelope,
        effect_id: String,
        assignment_id: String,
        decline: AssignmentDecline,
    },
    AssignmentCancellationApplied {
        envelope: RunnerEnvelope,
        effect_id: String,
        request_id: String,
        assignment_id: String,
        attempt_id: String,
        mode: CancellationMode,
        effective_mode: CancellationMode,
        disposition: CancellationApplicationDisposition,
    },
    AssignmentInterrupted {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        reason: String,
    },
    ExecutionLeaseRenewalRequested {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        current_lease_sequence: u64,
    },
    ExecutionStarted {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
    },
    ExecutionTransition {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        execution_event_sequence: u64,
        workflow_event: Value,
        diagnostic: Option<Value>,
    },
    /// A tree run's ordered root progress between engine attempts (REQ-RUN-88,
    /// REQ-RUNNERENROLL-99). `progress_sequence` numbers the assignment's
    /// progress reports from 1, apart from `execution_event_sequence`.
    TreeProgress {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        progress_sequence: u64,
        progress: TreeRootProgress,
    },
    /// The runner's application of one trigger delivery to the run its assignment holds
    /// (REQ-TRIGGER-136).
    TriggerDeliveryApplied {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        application: TriggerDeliveryApplication,
    },
    /// The runner's application of one authorized Cloud control to the run its assignment
    /// holds (REQ-RUNNERENROLL-90).
    RunControlApplied {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        application: RunControlApplication,
    },
    ExecutionFinished {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        final_execution_event_sequence: u64,
        outcome: Value,
        artifact_delivery: Value,
        diagnostic: Option<Value>,
        /// The root's exit, carried only by a tree run's finish (REQ-RUNNERENROLL-99).
        tree_exit: Option<TreeExit>,
    },
    ExecutionInterrupted {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        final_execution_event_sequence: u64,
        reason: String,
        terminal_outcome: Value,
        artifact_delivery: Value,
    },
    ExecutionAborted {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        last_execution_event_sequence: u64,
        reason: String,
    },
    ArtifactCarrierRegister {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        portable_owner_path: String,
        media_type: String,
        size_bytes: u64,
        sha256: String,
        idempotency_key: String,
    },
    ArtifactCarrierConfirm {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        artifact_set_id: String,
        carrier_id: String,
    },
    ArtifactResultRegister {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        size_bytes: u64,
        sha256: String,
    },
    ArtifactResultConfirm {
        envelope: RunnerEnvelope,
        assignment_id: String,
        attempt_id: String,
        artifact_set_id: String,
    },
}

/// The exit of a tree run's root beside its transport outcome (REQ-RUNNERENROLL-99).
/// The encoder always writes `mirror` and `claimRelease`, so Cloud records an exit
/// even for a finish that names no outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeExit {
    pub chosen_outcome: Option<String>,
    /// The run's mirror writes in order, each a closed `MirrorResult`: its
    /// `mirror/<NNNN>-<hook>.json` without `schemaVersion`.
    pub mirror: Vec<Value>,
    /// Whether the runner requests release of the run's subject claim.
    pub claim_release: bool,
}

/// One root progress report of a tree run: the internal engine attempt ordinal
/// (from 1), the record's `position` (step identifiers and `{"item": n}`), and
/// the pending wait. The frame's state is `waiting` exactly when `wait` is
/// present, and `running` otherwise. Timestamps are RFC 3339 UTC with `Z`.
/// `steps` are the report's step observations; the encoder writes the optional
/// `steps` member only when the list is non-empty (REQ-COMPAT-18), and Cloud
/// refuses a frame that names one `(run, run_attempt, step)` twice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeRootProgress {
    pub attempt: u64,
    pub position: Vec<Value>,
    pub wait: Option<TreeProgressWait>,
    pub reported_at: String,
    pub steps: Vec<TreeStepObservation>,
}

/// The `{kind, since, step}` of a waiting tree run's `wait.json`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeProgressWait {
    pub kind: String,
    pub since: String,
    pub step: Vec<Value>,
}

/// One step observation of a tree run (REQ-COMPAT-18): the run inside the tree
/// it belongs to (`run`, empty for the root), that run's engine attempt (from
/// 1), the step identifier and its latest state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeStepObservation {
    pub run: Vec<TreeStepRunElement>,
    pub run_attempt: u64,
    pub step: String,
    pub state: TreeStepState,
}

/// One level of child workflow on the path to an observation's run: the parent
/// step, the one-based fan-out element when that step fans out, and the child
/// run's one-based ordinal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeStepRunElement {
    pub step: String,
    pub item: Option<u64>,
    pub ordinal: u64,
}

/// A step's state in the shared runtime's attempt record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TreeStepState {
    Pending,
    Starting,
    Running,
    CapturingOutputs,
    Cancelling,
    Succeeded,
    Inherited,
    Failed,
    Blocked,
    Skipped,
    NotRun,
    Cancelled,
}

impl TreeStepState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::CapturingOutputs => "capturing_outputs",
            Self::Cancelling => "cancelling",
            Self::Succeeded => "succeeded",
            Self::Inherited => "inherited",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Skipped => "skipped",
            Self::NotRun => "not_run",
            Self::Cancelled => "cancelled",
        }
    }
}

fn tree_step_observation_value(observation: &TreeStepObservation) -> Value {
    let run = observation
        .run
        .iter()
        .map(|element| {
            let mut value = json!({ "step": element.step, "ordinal": element.ordinal });
            if let Some(item) = element.item {
                value["item"] = json!(item);
            }
            value
        })
        .collect::<Vec<_>>();
    json!({
        "run": run,
        "runAttempt": observation.run_attempt,
        "step": observation.step,
        "state": observation.state.as_str(),
    })
}

/// A control delivery Cloud pinned to a run's subject claim (REQ-TRIGGER-134), carried by a
/// `trigger_delivery` effect or, for a delivery accepted while the run was queued, in the
/// offer's `workflowTree.pendingDeliveries`. `event` is the shared event `{source, event,
/// at, payload}` and `claim` is `{connection, issue, observation?}`, each as Cloud sent it,
/// for `um workflow run event`'s `--event` and `--claim` files (REQ-TRIGGER-136).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TriggerDelivery {
    pub delivery_id: String,
    pub claim_id: String,
    pub event: Value,
    pub claim: Value,
}

/// The action an applied delivery took on the run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TriggerDeliveryAction {
    Cancel,
    Withdraw,
}

impl TriggerDeliveryAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::Withdraw => "withdraw",
        }
    }
}

/// How `um workflow run event` answered a delivery: applied with its action, or refused with
/// its code (`^[a-z][a-z0-9_]{0,63}$`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TriggerDeliveryOutcome {
    Applied(TriggerDeliveryAction),
    Refused { code: String },
}

/// The fresh issue read an application acted on, in the claim observation's shape; the
/// provider may report no team or update time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TriggerIssueObservation {
    pub team: Option<String>,
    pub state: String,
    pub updated_at: Option<String>,
}

/// One delivery's application, reported as `trigger_delivery_applied`; `observation` is
/// absent when the command refused before reading the issue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TriggerDeliveryApplication {
    pub delivery_id: String,
    pub outcome: TriggerDeliveryOutcome,
    pub observation: Option<TriggerIssueObservation>,
}

/// Bounds one answer's text in UTF-8 bytes; the schema can bound only its characters.
pub const MAXIMUM_RUN_CONTROL_ANSWER_BYTES: usize = 65_536;

/// An authorized Cloud control for the run an assignment holds, carried by a `run_control`
/// effect (REQ-RUNNERENROLL-90).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunControl {
    pub control_id: String,
    pub action: RunControlAction,
}

/// What a control asks of the run, each kind with only its own members: the `um workflow
/// run` command it names (`answer`, `approve`, `hand-back`, `lift`, `stop` or `retry`) and
/// the wait it resolves, when it names one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunControlAction {
    Answer {
        wait: RunControlWait,
        answers: Vec<RunControlAnswer>,
    },
    Approve {
        wait: RunControlWait,
    },
    HandBack {
        wait: RunControlWait,
        notes: String,
    },
    Lift {
        wait: Option<RunControlWait>,
        before: Option<String>,
    },
    Stop {
        before: String,
        reason: Option<String>,
    },
    Retry {
        wait: RunControlWait,
    },
}

/// The wait a control resolves: the `kind` and `since` of the pending wait as
/// `tree_progress` reported it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunControlWait {
    pub kind: String,
    pub since: String,
}

/// One answer to a question the waiting run asks, by the question's id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunControlAnswer {
    pub question: String,
    pub text: String,
}

/// Why the runner skipped a control without running its command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunControlSkip {
    /// The run no longer waits on the wait the control names.
    WaitNotCurrent,
    /// The assignment ended before the control was applied.
    AssignmentEnded,
}

impl RunControlSkip {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WaitNotCurrent => "wait_not_current",
            Self::AssignmentEnded => "assignment_ended",
        }
    }
}

/// How the runner answered a control: applied with the command's own outcome (1 to 64
/// characters), refused with the runtime's refusal code (`^[a-z][a-z0-9_]{0,63}$`), or
/// skipped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunControlOutcome {
    Applied { action_outcome: String },
    Refused { code: String },
    Skipped(RunControlSkip),
}

/// One control's application, reported as `run_control_applied`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunControlApplication {
    pub control_id: String,
    pub outcome: RunControlOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerUnableReason {
    ExecutionEnvironmentUnavailable,
    SourceServiceUnavailable,
    InputServiceUnavailable,
    WorkflowEnvironmentUnsupported,
    OwnershipUnproven,
    RetainedWorkspaceUnavailable,
}

impl RunnerUnableReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionEnvironmentUnavailable => "execution_environment_unavailable",
            Self::SourceServiceUnavailable => "source_service_unavailable",
            Self::InputServiceUnavailable => "input_service_unavailable",
            Self::WorkflowEnvironmentUnsupported => "workflow_environment_unsupported",
            Self::OwnershipUnproven => "ownership_unproven",
            Self::RetainedWorkspaceUnavailable => "retained_workspace_unavailable",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionSpecInvalidReason {
    UnsupportedSchemaVersion,
    InvalidExecutionLimits,
    InvalidSourceProjection,
    UnsupportedSourceObjectFormat,
    SourceCommitMismatch,
    SourceCommitUnavailable,
    SourceCheckoutDirty,
    WorkflowSourceDigestMismatch,
    WorkflowSourceInvalid,
    #[allow(
        dead_code,
        reason = "retained in the protocol taxonomy; workflow validation now rejects the local node-count case during source resolution"
    )]
    WorkflowContractInvalid,
    WorkflowAdmissionInvalid,
    InvalidInputProjection,
    InputManifestMismatch,
    InputContentUnavailable,
    InputContentMismatch,
    InputTextInvalid,
    InputJsonInvalid,
}

impl ExecutionSpecInvalidReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedSchemaVersion => "unsupported_schema_version",
            Self::InvalidExecutionLimits => "invalid_execution_limits",
            Self::InvalidSourceProjection => "invalid_source_projection",
            Self::UnsupportedSourceObjectFormat => "unsupported_source_object_format",
            Self::SourceCommitMismatch => "source_commit_mismatch",
            Self::SourceCommitUnavailable => "source_commit_unavailable",
            Self::SourceCheckoutDirty => "source_checkout_dirty",
            Self::WorkflowSourceDigestMismatch => "workflow_source_digest_mismatch",
            Self::WorkflowSourceInvalid => "workflow_source_invalid",
            Self::WorkflowContractInvalid => "workflow_contract_invalid",
            Self::WorkflowAdmissionInvalid => "workflow_admission_invalid",
            Self::InvalidInputProjection => "invalid_input_projection",
            Self::InputManifestMismatch => "input_manifest_mismatch",
            Self::InputContentUnavailable => "input_content_unavailable",
            Self::InputContentMismatch => "input_content_mismatch",
            Self::InputTextInvalid => "input_text_invalid",
            Self::InputJsonInvalid => "input_json_invalid",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AssignmentDecline {
    CapacityUnavailable,
    RunnerUnable(RunnerUnableReason),
    ExecutionSpecInvalid(ExecutionSpecInvalidReason),
    Diagnosed {
        decline: Box<Self>,
        stage: &'static str,
        cause: &'static str,
    },
}

impl AssignmentDecline {
    pub fn diagnosed(self, stage: &'static str, cause: &'static str) -> Self {
        Self::Diagnosed {
            decline: Box::new(self),
            stage,
            cause,
        }
    }

    pub fn protocol_type_and_reason(&self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::CapacityUnavailable => ("capacity_unavailable", None),
            Self::RunnerUnable(reason) => ("runner_unable", Some(reason.as_str())),
            Self::ExecutionSpecInvalid(reason) => ("execution_spec_invalid", Some(reason.as_str())),
            Self::Diagnosed { decline, .. } => decline.protocol_type_and_reason(),
        }
    }

    fn diagnostic(&self) -> Option<Value> {
        match self {
            Self::Diagnosed { stage, cause, .. } => Some(json!({
                "stage": stage,
                "declineCause": cause,
            })),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionLimitsV1RunnerProjection {
    pub maximum_parallel_steps: u64,
    pub cancellation_grace_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowSourceClosureDigestV1RunnerProjection {
    pub algorithm: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowDefinitionSourceV1RunnerProjection {
    pub repository_connection_id: String,
    pub object_format: String,
    pub commit_oid: String,
    pub workflow_path: String,
    pub workflow_source_closure_digest: WorkflowSourceClosureDigestV1RunnerProjection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryWorkspaceSourceV1RunnerProjection {
    pub kind: String,
    pub provider_kind: String,
    pub repository_connection_id: String,
    pub object_format: String,
    pub commit_oid: String,
    pub materialization_contract: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionCapacityV1RunnerProjection {
    pub execution_contract: String,
    pub source_closure_digest: WorkflowSourceClosureDigestV1RunnerProjection,
    // Runner admission consumes a closed wire projection rather than the resolver's
    // computed type so protocol decoding cannot accidentally become capacity authority.
    pub general_maximum_transitions: u64,
    pub selected_maximum_transitions: u64,
    pub maximum_invocations: u64,
    pub maximum_retained_bytes_per_invocation: u64,
    pub diagnostic_retention_bytes: u64,
    pub native_session_retention_bytes: u64,
    pub aggregate_retention_bytes: u64,
    pub condition_transition_count: u64,
    pub aggregate_condition_transition_bytes: u64,
    pub terminal_result_structure_bytes: u64,
    #[serde(default)]
    pub presentation_result_bytes: u64,
    pub portable_result_bytes: u64,
    pub encoded_outbox_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunInputProjectionV1 {
    pub input_set_id: String,
    pub manifest_digest: WorkflowSourceClosureDigestV1RunnerProjection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDisplayRepositoryV1RunnerProjection {
    pub provider_kind: String,
    pub instance_url: Option<String>,
    pub full_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDisplaySnapshotV1RunnerProjection {
    pub organization_display_name: String,
    pub project_name: String,
    pub repository: SourceDisplayRepositoryV1RunnerProjection,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceWitness {
    #[serde(rename = "ref")]
    pub reference: String,
    pub tip_oid: String,
    pub relationship: String,
    pub observed_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceChangeRequest {
    pub provider_id: String,
    pub number: u64,
    pub head_repository_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceMembership {
    pub repository_connection_id: String,
    pub provider_kind: String,
    pub provider_repository_id: String,
    pub object_format: String,
    pub workspace_commit_oid: String,
    pub witness: WorkspaceWitness,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_request: Option<WorkspaceChangeRequest>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionSpecV1RunnerProjection {
    pub execution_spec_id: String,
    pub schema_version: u64,
    pub execution_limits: ExecutionLimitsV1RunnerProjection,
    pub source_branch: String,
    pub workflow_definition_source: WorkflowDefinitionSourceV1RunnerProjection,
    pub primary_workspace_source: PrimaryWorkspaceSourceV1RunnerProjection,
    pub source_display_snapshot: Option<SourceDisplaySnapshotV1RunnerProjection>,
    pub workspace_branch: Option<String>,
    pub workspace_membership: Option<WorkspaceMembership>,
    pub capacity: ExecutionCapacityV1RunnerProjection,
    pub run_inputs: Option<RunInputProjectionV1>,
    /// The published tree a definition-backed run executes in place of `workflowPath`.
    pub workflow_tree: Option<WorkflowTreeV1RunnerProjection>,
}

/// The `workflowTree` block of a definition-backed run's spec: the revision's sealed
/// files, its bindings snapshot and resolution and the start's owner, stop step and Cloud
/// run page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTreeV1RunnerProjection {
    /// The tree digest, `sha256:<hex>`.
    pub digest: String,
    pub definition_id: String,
    pub revision: u64,
    pub workflow_path: String,
    pub files: Vec<WorkflowTreeFileV1RunnerProjection>,
    /// The bindings snapshot, an object in the bindings file's shape.
    pub bindings: Value,
    /// The revision's resolution by binding key: each binding's kind and value and, for a
    /// Linear team or state, the provider id and display name Cloud resolved.
    pub resolution: Value,
    pub owner: Option<String>,
    pub stop_before: Option<String>,
    /// The deliveries accepted while the run was queued, applied before its first step.
    pub pending_deliveries: Vec<TriggerDelivery>,
    pub run_url: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTreeFileV1RunnerProjection {
    pub path: String,
    pub sha256: String,
    pub size: u64,
    pub executable: bool,
}

/// The pinned continuation evidence is carried separately from the immutable
/// run execution specification. The runner verifies its source and capacity
/// against the resolved closure before it accepts an assignment.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContinuationOffer {
    pub prior_assignment_id: String,
    pub prior_attempt_id: String,
    pub required_runner_boot_id: String,
    pub execution_root: String,
    pub definition_source: WorkflowDefinitionSourceV1RunnerProjection,
    pub effective_capacity: ExecutionCapacityV1RunnerProjection,
    pub prior_manifest_digest: WorkflowSourceClosureDigestV1RunnerProjection,
    pub request: Value,
    pub reexecuted_steps: Vec<String>,
    pub inherited_steps: Vec<Value>,
    pub prior_settlement_snapshot: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionLeasePolicy {
    pub schema_version: u64,
    pub force_stop_and_reap_budget_milliseconds: i64,
    pub terminal_report_delivery_budget_milliseconds: i64,
    pub renewal_delivery_budget_milliseconds: i64,
    pub lease_duration_milliseconds: u64,
    pub fencing_margin_milliseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionLeaseGrant {
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationMode {
    Graceful,
    Force,
}

impl CancellationMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Graceful => "graceful",
            Self::Force => "force",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancellationApplicationDisposition {
    PreExecutionStopped,
    OrdinaryCancelling,
    FinalizersPreserved,
    ForceCancelling,
    ExecutionTerminal,
    Superseded,
}

impl CancellationApplicationDisposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PreExecutionStopped => "pre_execution_stopped",
            Self::OrdinaryCancelling => "ordinary_cancelling",
            Self::FinalizersPreserved => "finalizers_preserved",
            Self::ForceCancelling => "force_cancelling",
            Self::ExecutionTerminal => "execution_terminal",
            Self::Superseded => "superseded",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudEnvelope {
    pub message_id: String,
    pub sent_at: String,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ArtifactUploadCapability {
    pub url: String,
    pub content_length: String,
    pub content_type: String,
    pub if_none_match: String,
    pub checksum_sha256: String,
    pub expires_at: String,
}

impl fmt::Debug for ArtifactUploadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ArtifactUploadCapability(<redacted>)")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactRegistrationOutcome {
    Succeeded {
        artifact_set_id: String,
        carrier_id: String,
        upload_capability: ArtifactUploadCapability,
    },
    Retryable,
    Failed {
        code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactRegistrationResponse {
    pub request_message_id: String,
    pub outcome: ArtifactRegistrationOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactConfirmationOutcome {
    Confirmed {
        artifact_set_id: String,
        carrier_id: String,
    },
    Absent {
        artifact_set_id: String,
        carrier_id: String,
        upload_capability: ArtifactUploadCapability,
    },
    Retryable {
        artifact_set_id: String,
        carrier_id: String,
    },
    Failed {
        artifact_set_id: String,
        carrier_id: String,
        code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactConfirmationResponse {
    pub request_message_id: String,
    pub outcome: ArtifactConfirmationOutcome,
}

// Result freeze responses remain distinct from carrier responses because their
// deadline participates in a separate result-finalization state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactResultRegistrationOutcome {
    Succeeded {
        artifact_set_id: String,
        finalization_deadline: String,
        upload_capability: ArtifactUploadCapability,
    },
    Retryable,
    Failed {
        code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactResultRegistrationResponse {
    pub request_message_id: String,
    pub outcome: ArtifactResultRegistrationOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactResultConfirmationOutcome {
    Confirmed {
        artifact_set_id: String,
    },
    Pending {
        artifact_set_id: String,
    },
    Absent {
        artifact_set_id: String,
        upload_capability: ArtifactUploadCapability,
    },
    Retryable {
        artifact_set_id: String,
    },
    Failed {
        artifact_set_id: String,
        phase: String,
        code: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactResultConfirmationResponse {
    pub request_message_id: String,
    pub outcome: ArtifactResultConfirmationOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CloudFrame {
    Welcome {
        envelope: CloudEnvelope,
        session_id: String,
        ping_interval_seconds: u64,
        pong_timeout_seconds: u64,
        lease_policy: ExecutionLeasePolicy,
    },
    ObservationAck {
        envelope: CloudEnvelope,
        acknowledged_message_id: String,
        acknowledged_sequence: u64,
    },
    WorkspaceRegistration {
        envelope: CloudEnvelope,
        acknowledged_message_id: String,
        acknowledged_sequence: u64,
        work_root_id: String,
        workspace_id: String,
        rejection: Option<String>,
    },
    ArtifactCarrierRegistration {
        envelope: CloudEnvelope,
        response: ArtifactRegistrationResponse,
    },
    ArtifactCarrierConfirmation {
        envelope: CloudEnvelope,
        response: ArtifactConfirmationResponse,
    },
    ArtifactResultRegistration {
        envelope: CloudEnvelope,
        response: ArtifactResultRegistrationResponse,
    },
    ArtifactResultConfirmation {
        envelope: CloudEnvelope,
        response: ArtifactResultConfirmationResponse,
    },
    AssignmentOffer {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        project_id: String,
        attempt_id: String,
        attempt_number: u64,
        execution_spec: Box<ExecutionSpecV1RunnerProjection>,
        continuation: Option<Box<ContinuationOffer>>,
    },
    AssignmentPrepare {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        execution_spec_id: String,
        preparation_expires_at: String,
    },
    AssignmentStart {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        execution_spec_id: String,
        lease: ExecutionLeaseGrant,
    },
    AssignmentCancel {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        request_id: String,
        mode: CancellationMode,
    },
    ExecutionStartAuthorized {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
    },
    AssignmentLeaseRenewed {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        lease: ExecutionLeaseGrant,
    },
    AssignmentRelease {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        reason: String,
    },
    /// A Gateway-originated effect carrying one delivery for the run the assignment holds
    /// (REQ-TRIGGER-136).
    TriggerDelivery {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        delivery: TriggerDelivery,
    },
    /// A Gateway-originated effect carrying one authorized control for the run the
    /// assignment holds (REQ-RUNNERENROLL-90).
    RunControl {
        envelope: CloudEnvelope,
        effect_id: String,
        assignment_id: String,
        run_id: String,
        attempt_id: String,
        control: RunControl,
    },
}

#[derive(Debug)]
pub enum DecodeError {
    InvalidJson,
    InvalidFrame(&'static str),
    RunnerDirectedFrame,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson => formatter.write_str("runner protocol frame is not valid JSON"),
            Self::InvalidFrame(field) => {
                write!(formatter, "runner protocol frame has an invalid {field}")
            }
            Self::RunnerDirectedFrame => {
                formatter.write_str("runner protocol frame has runner-to-cloud direction")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Debug)]
pub enum EncodeError {
    InvalidFrame(&'static str),
    Serialization,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFrame(field) => {
                write!(formatter, "runner protocol frame has an invalid {field}")
            }
            Self::Serialization => formatter.write_str("encode runner protocol frame"),
        }
    }
}

impl std::error::Error for EncodeError {}

pub fn valid_assignment_id(value: &str) -> bool {
    value.parse::<generated::AssignmentId>().is_ok()
}

pub fn valid_attempt_id(value: &str) -> bool {
    value.parse::<generated::AttemptId>().is_ok()
}

pub fn valid_boot_id(value: &str) -> bool {
    value.parse::<generated::BootId>().is_ok()
}

pub fn valid_repository_connection_id(value: &str) -> bool {
    value.parse::<generated::RepositoryConnectionId>().is_ok()
}

pub fn valid_run_id(value: &str) -> bool {
    value.parse::<generated::RunId>().is_ok()
}

pub fn valid_run_input_set_id(value: &str) -> bool {
    value
        .parse::<generated::RunInputProjectionInputSetId>()
        .is_ok()
}

pub fn decode_cloud_frame(bytes: &[u8]) -> Result<CloudFrame, DecodeError> {
    match decode_frame(bytes)? {
        ValidatedFrame::Cloud(frame) => Ok(*frame),
        ValidatedFrame::Runner => Err(DecodeError::RunnerDirectedFrame),
    }
}

pub fn encode_runner_frame(frame: &RunnerFrame) -> Result<Vec<u8>, EncodeError> {
    let value = match frame {
        RunnerFrame::Hello {
            envelope,
            runner_version,
            recovery_root,
            tree_execution,
            tree_continuation,
        } => {
            let mut payload = json!({ "runnerVersion": runner_version });
            if *tree_execution {
                payload["treeExecution"] = json!(1);
            }
            if *tree_continuation {
                payload["treeContinuation"] = json!(1);
            }
            if let Some(root) = recovery_root {
                payload["workspaceRecoveryVersion"] = json!(1);
                payload["workspaceRecoveryRoot"] = root.clone();
            }
            runner_frame_value(envelope, "hello", payload)
        }
        RunnerFrame::WorkspaceInventoryComplete {
            envelope,
            work_root_id,
            inventory_through_sequence,
        } => runner_frame_value(
            envelope,
            "workspace_inventory_complete",
            json!({ "schemaVersion": 1, "workRootId": work_root_id, "inventoryThroughSequence": inventory_through_sequence }),
        ),
        RunnerFrame::WorkspaceInventory { envelope, payload } => {
            runner_frame_value(envelope, "workspace_inventory", payload.clone())
        }
        RunnerFrame::WorkspaceRetentionReport {
            envelope,
            assignment_id,
            attempt_id,
            run_id,
            execution_root,
            state,
            settlement_snapshot,
        } => {
            let mut payload = json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "runId": run_id,
                "executionRoot": execution_root,
                "state": state,
            });
            if let Some(snapshot) = settlement_snapshot {
                payload["settlementSnapshot"] = snapshot.clone();
            }
            runner_frame_value(envelope, "workspace_retention_report", payload)
        }
        RunnerFrame::EffectAcknowledged {
            envelope,
            effect_id,
        } => runner_frame_value(
            envelope,
            "effect_acknowledged",
            json!({ "effectId": effect_id }),
        ),
        RunnerFrame::AssignmentPreparing {
            envelope,
            effect_id,
            assignment_id,
            offered_execution_spec_id,
        } => runner_frame_value(
            envelope,
            "assignment_preparing",
            json!({
                "effectId": effect_id,
                "assignmentId": assignment_id,
                "offeredExecutionSpecId": offered_execution_spec_id,
            }),
        ),
        RunnerFrame::AssignmentPreparationProgress {
            envelope,
            assignment_id,
            attempt_id,
            preparation_sequence,
            phase,
        } => runner_frame_value(
            envelope,
            "assignment_preparation_progress",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "preparationSequence": preparation_sequence,
                "phase": phase,
            }),
        ),
        RunnerFrame::ContinuationReady {
            envelope,
            assignment_id,
            attempt_id,
            start_snapshot,
            quiescence,
            modified,
        } => runner_frame_value(
            envelope,
            "continuation_ready",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "startSnapshot": start_snapshot,
                "quiescence": quiescence,
                "modified": modified,
            }),
        ),
        RunnerFrame::AssignmentAccepted {
            envelope,
            effect_id,
            assignment_id,
            offered_execution_spec_id,
        } => runner_frame_value(
            envelope,
            "assignment_accepted",
            json!({
                "effectId": effect_id,
                "assignmentId": assignment_id,
                "offeredExecutionSpecId": offered_execution_spec_id,
            }),
        ),
        RunnerFrame::AssignmentRejected {
            envelope,
            effect_id,
            assignment_id,
            decline,
        } => {
            let (decline_type, decline_reason) = decline.protocol_type_and_reason();
            let decline_value = match decline_reason {
                Some(reason) => json!({
                    "type": decline_type,
                    "reason": reason,
                }),
                None => json!({ "type": decline_type }),
            };
            let mut decline_value = decline_value;
            if let Some(diagnostic) = decline.diagnostic() {
                decline_value["diagnostic"] = diagnostic;
            }
            let payload = json!({
                "effectId": effect_id,
                "assignmentId": assignment_id,
                "decline": decline_value,
            });
            runner_frame_value(envelope, "assignment_rejected", payload)
        }
        RunnerFrame::AssignmentCancellationApplied {
            envelope,
            effect_id,
            request_id,
            assignment_id,
            attempt_id,
            mode,
            effective_mode,
            disposition,
        } => runner_frame_value(
            envelope,
            "assignment_cancellation_applied",
            json!({
                "effectId": effect_id,
                "requestId": request_id,
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "mode": mode.as_str(),
                "effectiveMode": effective_mode.as_str(),
                "disposition": disposition.as_str(),
            }),
        ),
        RunnerFrame::AssignmentInterrupted {
            envelope,
            assignment_id,
            attempt_id,
            reason,
        } => runner_frame_value(
            envelope,
            "assignment_interrupted",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "reason": reason,
            }),
        ),
        RunnerFrame::ExecutionLeaseRenewalRequested {
            envelope,
            assignment_id,
            attempt_id,
            current_lease_sequence,
        } => runner_frame_value(
            envelope,
            "execution_lease_renewal_requested",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "currentLeaseSequence": current_lease_sequence,
            }),
        ),
        RunnerFrame::ExecutionStarted {
            envelope,
            assignment_id,
            attempt_id,
        } => runner_frame_value(
            envelope,
            "execution_started",
            json!({ "assignmentId": assignment_id, "attemptId": attempt_id }),
        ),
        RunnerFrame::ExecutionTransition {
            envelope,
            assignment_id,
            attempt_id,
            execution_event_sequence,
            workflow_event,
            diagnostic,
        } => runner_frame_value(
            envelope,
            "execution_transition",
            with_runner_diagnostic(
                json!({
                    "assignmentId": assignment_id,
                    "attemptId": attempt_id,
                    "executionEventSequence": execution_event_sequence,
                    "workflowEvent": workflow_event,
                }),
                diagnostic,
            ),
        ),
        RunnerFrame::TreeProgress {
            envelope,
            assignment_id,
            attempt_id,
            progress_sequence,
            progress,
        } => {
            let mut payload = json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "progressSequence": progress_sequence,
                "attempt": progress.attempt,
                "state": if progress.wait.is_some() { "waiting" } else { "running" },
                "position": progress.position,
                "wait": progress.wait.as_ref().map(|wait| json!({
                    "kind": wait.kind,
                    "since": wait.since,
                    "step": wait.step,
                })),
                "reportedAt": progress.reported_at,
            });
            if !progress.steps.is_empty() {
                payload["steps"] = progress
                    .steps
                    .iter()
                    .map(tree_step_observation_value)
                    .collect();
            }
            runner_frame_value(envelope, "tree_progress", payload)
        }
        RunnerFrame::TriggerDeliveryApplied {
            envelope,
            assignment_id,
            attempt_id,
            application,
        } => {
            let mut payload = json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "deliveryId": application.delivery_id,
                "observation": application.observation.as_ref().map(|observation| json!({
                    "team": observation.team,
                    "state": observation.state,
                    "updatedAt": observation.updated_at,
                })),
            });
            match &application.outcome {
                TriggerDeliveryOutcome::Applied(action) => {
                    payload["outcome"] = json!("applied");
                    payload["action"] = json!(action.as_str());
                }
                TriggerDeliveryOutcome::Refused { code } => {
                    payload["outcome"] = json!("refused");
                    payload["code"] = json!(code);
                }
            }
            runner_frame_value(envelope, "trigger_delivery_applied", payload)
        }
        RunnerFrame::RunControlApplied {
            envelope,
            assignment_id,
            attempt_id,
            application,
        } => {
            let mut payload = json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "controlId": application.control_id,
            });
            match &application.outcome {
                RunControlOutcome::Applied { action_outcome } => {
                    payload["outcome"] = json!("applied");
                    payload["actionOutcome"] = json!(action_outcome);
                }
                RunControlOutcome::Refused { code } => {
                    payload["outcome"] = json!("refused");
                    payload["code"] = json!(code);
                }
                RunControlOutcome::Skipped(skip) => {
                    payload["outcome"] = json!("skipped");
                    payload["code"] = json!(skip.as_str());
                }
            }
            runner_frame_value(envelope, "run_control_applied", payload)
        }
        RunnerFrame::ExecutionFinished {
            envelope,
            assignment_id,
            attempt_id,
            final_execution_event_sequence,
            outcome,
            artifact_delivery,
            diagnostic,
            tree_exit,
        } => {
            let mut payload = json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "finalExecutionEventSequence": final_execution_event_sequence,
                "outcome": outcome,
                "artifactDelivery": artifact_delivery,
            });
            if let Some(exit) = tree_exit {
                if let Some(chosen) = &exit.chosen_outcome {
                    payload["chosenOutcome"] = json!(chosen);
                }
                payload["mirror"] = Value::Array(exit.mirror.clone());
                payload["claimRelease"] = json!(exit.claim_release);
            }
            runner_frame_value(
                envelope,
                "execution_finished",
                with_runner_diagnostic(payload, diagnostic),
            )
        }
        RunnerFrame::ExecutionInterrupted {
            envelope,
            assignment_id,
            attempt_id,
            final_execution_event_sequence,
            reason,
            terminal_outcome,
            artifact_delivery,
        } => runner_frame_value(
            envelope,
            "execution_interrupted",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "finalExecutionEventSequence": final_execution_event_sequence,
                "reason": reason,
                "terminalOutcome": terminal_outcome,
                "artifactDelivery": artifact_delivery,
            }),
        ),
        RunnerFrame::ExecutionAborted {
            envelope,
            assignment_id,
            attempt_id,
            last_execution_event_sequence,
            reason,
        } => runner_frame_value(
            envelope,
            "execution_aborted",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "lastExecutionEventSequence": last_execution_event_sequence,
                "reason": reason,
            }),
        ),
        RunnerFrame::ArtifactCarrierRegister {
            envelope,
            assignment_id,
            attempt_id,
            portable_owner_path,
            media_type,
            size_bytes,
            sha256,
            idempotency_key,
        } => runner_frame_value(
            envelope,
            "artifact_carrier_register",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "portableOwnerPath": portable_owner_path,
                "mediaType": media_type,
                "sizeBytes": size_bytes,
                "sha256": sha256,
                "idempotencyKey": idempotency_key,
            }),
        ),
        RunnerFrame::ArtifactCarrierConfirm {
            envelope,
            assignment_id,
            attempt_id,
            artifact_set_id,
            carrier_id,
        } => runner_frame_value(
            envelope,
            "artifact_carrier_confirm",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "artifactSetId": artifact_set_id,
                "carrierId": carrier_id,
            }),
        ),
        RunnerFrame::ArtifactResultRegister {
            envelope,
            assignment_id,
            attempt_id,
            size_bytes,
            sha256,
        } => runner_frame_value(
            envelope,
            "artifact_result_register",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "sizeBytes": size_bytes,
                "sha256": sha256,
            }),
        ),
        RunnerFrame::ArtifactResultConfirm {
            envelope,
            assignment_id,
            attempt_id,
            artifact_set_id,
        } => runner_frame_value(
            envelope,
            "artifact_result_confirm",
            json!({
                "assignmentId": assignment_id,
                "attemptId": attempt_id,
                "artifactSetId": artifact_set_id,
            }),
        ),
    };
    let encoded = serde_json::to_vec(&value).map_err(|_| EncodeError::Serialization)?;

    match decode_frame(&encoded) {
        Ok(ValidatedFrame::Runner) => Ok(encoded),
        Ok(ValidatedFrame::Cloud(_)) => Err(EncodeError::InvalidFrame("direction")),
        Err(DecodeError::InvalidFrame(field)) => Err(EncodeError::InvalidFrame(field)),
        Err(DecodeError::InvalidJson | DecodeError::RunnerDirectedFrame) => {
            Err(EncodeError::Serialization)
        }
    }
}

fn with_runner_diagnostic(mut payload: Value, diagnostic: &Option<Value>) -> Value {
    if let Some(diagnostic) = diagnostic {
        payload["diagnostic"] = diagnostic.clone();
    }
    payload
}

fn runner_frame_value(envelope: &RunnerEnvelope, frame_type: &str, payload: Value) -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "direction": RUNNER_TO_CLOUD,
        "messageId": envelope.message_id,
        "runnerId": envelope.runner_id,
        "bootId": envelope.boot_id,
        "sequence": envelope.sequence,
        "sentAt": envelope.sent_at,
        "type": frame_type,
        "payloadVersion": PAYLOAD_VERSION,
        "payload": payload,
    })
}

enum ValidatedFrame {
    Runner,
    Cloud(Box<CloudFrame>),
}

fn cloud(frame: CloudFrame) -> ValidatedFrame {
    ValidatedFrame::Cloud(Box::new(frame))
}

// cargo-typify generates distinct cloud structs with the same envelope fields.
// This macro keeps their validation and projection on one shared path without
// introducing wrappers around generated types.
macro_rules! validated_runner_frame {
    ($frame:expr) => {
        validate_runner_frame(
            &$frame.protocol_version,
            &$frame.payload_version,
            &$frame.direction,
            $frame.sent_at,
        )
    };
}

macro_rules! validated_cloud_envelope {
    ($frame:expr) => {
        cloud_envelope(
            &$frame.protocol_version,
            &$frame.payload_version,
            &$frame.direction,
            $frame.message_id,
            $frame.sent_at,
        )
    };
}

fn decode_frame(bytes: &[u8]) -> Result<ValidatedFrame, DecodeError> {
    if bytes.is_empty() || bytes.len() > MAXIMUM_TERMINAL_FRAME_BYTES {
        return Err(DecodeError::InvalidFrame("size"));
    }
    let value: Value = serde_json::from_slice(bytes).map_err(|_| DecodeError::InvalidJson)?;
    let runner_to_cloud = value.get("direction").and_then(Value::as_str) == Some(RUNNER_TO_CLOUD);
    let frame_type = value.get("type").and_then(Value::as_str);
    let large_terminal = runner_to_cloud
        && matches!(
            frame_type,
            Some("execution_finished" | "execution_interrupted")
        );
    let large_condition = runner_to_cloud
        && frame_type == Some("execution_transition")
        && bytes.len() <= MAXIMUM_CONDITION_TRANSITION_FRAME_BYTES
        && condition_evidence_transition(&value);
    if bytes.len() > MAXIMUM_ORDINARY_FRAME_BYTES && !large_terminal && !large_condition {
        return Err(DecodeError::InvalidFrame("sizeClass"));
    }
    validate_protocol_schema(&value)?;
    // The canonical schema admits retained V1 recovery frames, but the
    // unactivated runtime must not silently project their V2 fields away.
    if unactivated_recovery_frame(&value) {
        return Err(DecodeError::InvalidFrame("workspaceRecoveryVersion"));
    }
    validate_closed_shape(&value)?;
    let generated = generated_frame(value)?;

    match generated {
        generated::RunnerProtocolVersion1::RunnerHello(frame) => validate_runner_frame(
            &frame.protocol_version,
            &frame.payload_version,
            &frame.direction,
            frame.sent_at,
        ),
        generated::RunnerProtocolVersion1::RunnerWorkspaceInventory(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerWorkspaceInventoryComplete(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerWorkspaceRetentionReport(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerEffectAcknowledged(frame) => {
            validate_runner_frame(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                frame.sent_at,
            )
        }
        generated::RunnerProtocolVersion1::RunnerAssignmentPreparing(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerAssignmentPreparationProgress(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerContinuationReady(frame) => {
            if frame.payload.schema_version.is_some() || frame.payload.recovery.is_some() {
                return Err(DecodeError::InvalidFrame("workspaceRecoveryVersion"));
            }
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerAssignmentAccepted(frame) => {
            validate_runner_frame(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                frame.sent_at,
            )
        }
        generated::RunnerProtocolVersion1::RunnerAssignmentRejected(frame) => {
            validate_runner_frame(
                &frame.protocol_version,
                &frame.payload_version,
                &frame.direction,
                frame.sent_at,
            )
        }
        generated::RunnerProtocolVersion1::RunnerAssignmentCancellationApplied(frame) => {
            validate_cancellation_application(
                cancellation_mode(frame.payload.mode),
                cancellation_mode(frame.payload.effective_mode),
                cancellation_application_disposition(frame.payload.disposition),
            )?;
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerAssignmentInterrupted(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionLeaseRenewalRequested(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionStarted(frame) => {
            if frame.payload.schema_version.is_some() || frame.payload.workspace_recovery.is_some()
            {
                return Err(DecodeError::InvalidFrame("workspaceRecoveryVersion"));
            }
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionTransition(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerTreeProgress(frame) => {
            validate_timestamp(&frame.payload.reported_at)
                .map_err(|_| DecodeError::InvalidFrame("reportedAt"))?;
            if let Some(wait) = &frame.payload.wait {
                validate_timestamp(&wait.since).map_err(|_| DecodeError::InvalidFrame("since"))?;
            }
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionFinished(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerTriggerDeliveryApplied(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerRunControlApplied(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionInterrupted(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerExecutionAborted(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerArtifactCarrierRegister(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerArtifactCarrierConfirm(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerArtifactResultRegister(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::RunnerArtifactResultConfirm(frame) => {
            validated_runner_frame!(frame)
        }
        generated::RunnerProtocolVersion1::CloudWelcome(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let ping_interval_seconds = frame.payload.ping_interval_seconds.get();
            let pong_timeout_seconds = u64::try_from(frame.payload.pong_timeout_seconds)
                .map_err(|_| DecodeError::InvalidFrame("pongTimeoutSeconds"))?;
            if pong_timeout_seconds < ping_interval_seconds.saturating_mul(2) {
                return Err(DecodeError::InvalidFrame("pongTimeoutSeconds"));
            }
            let session_id = frame.payload.session_id.to_string();
            let policy = frame.payload.lease_policy;
            let schema_version = policy
                .schema_version
                .as_u64()
                .ok_or(DecodeError::InvalidFrame("schemaVersion"))?;
            Ok(cloud(CloudFrame::Welcome {
                envelope,
                session_id,
                ping_interval_seconds,
                pong_timeout_seconds,
                lease_policy: ExecutionLeasePolicy {
                    schema_version,
                    force_stop_and_reap_budget_milliseconds: policy
                        .force_stop_and_reap_budget_milliseconds
                        .0,
                    terminal_report_delivery_budget_milliseconds: policy
                        .terminal_report_delivery_budget_milliseconds
                        .0,
                    renewal_delivery_budget_milliseconds: policy
                        .renewal_delivery_budget_milliseconds
                        .0,
                    lease_duration_milliseconds: policy.lease_duration_milliseconds.0.get(),
                    fencing_margin_milliseconds: policy.fencing_margin_milliseconds.0.get(),
                },
            }))
        }
        generated::RunnerProtocolVersion1::CloudWorkspaceRegistered(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(cloud(CloudFrame::WorkspaceRegistration {
                envelope,
                acknowledged_message_id: frame.payload.acknowledged_message_id.to_string(),
                acknowledged_sequence: frame.payload.acknowledged_sequence.0.get(),
                work_root_id: frame.payload.work_root_id.to_string(),
                workspace_id: frame.payload.workspace_id.to_string(),
                rejection: None,
            }))
        }
        generated::RunnerProtocolVersion1::CloudWorkspaceRegistrationRejected(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(cloud(CloudFrame::WorkspaceRegistration {
                envelope,
                acknowledged_message_id: frame.payload.acknowledged_message_id.to_string(),
                acknowledged_sequence: frame.payload.acknowledged_sequence.0.get(),
                work_root_id: frame.payload.work_root_id.to_string(),
                workspace_id: frame.payload.workspace_id.to_string(),
                rejection: Some(frame.payload.reason.to_string()),
            }))
        }
        generated::RunnerProtocolVersion1::CloudObservationAck(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(cloud(CloudFrame::ObservationAck {
                envelope,
                acknowledged_message_id: frame.payload.acknowledged_message_id.to_string(),
                acknowledged_sequence: frame.payload.acknowledged_sequence.0.get(),
            }))
        }
        generated::RunnerProtocolVersion1::CloudArtifactCarrierRegistration(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let response = artifact_registration_response(frame.payload)?;
            Ok(cloud(CloudFrame::ArtifactCarrierRegistration {
                envelope,
                response,
            }))
        }
        generated::RunnerProtocolVersion1::CloudArtifactCarrierConfirmation(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let response = artifact_confirmation_response(frame.payload)?;
            Ok(cloud(CloudFrame::ArtifactCarrierConfirmation {
                envelope,
                response,
            }))
        }
        generated::RunnerProtocolVersion1::CloudArtifactResultRegistration(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let response = artifact_result_registration_response(frame.payload)?;
            Ok(cloud(CloudFrame::ArtifactResultRegistration {
                envelope,
                response,
            }))
        }
        generated::RunnerProtocolVersion1::CloudArtifactResultConfirmation(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let response = artifact_result_confirmation_response(frame.payload)?;
            Ok(cloud(CloudFrame::ArtifactResultConfirmation {
                envelope,
                response,
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentOffer(frame) => {
            if frame
                .payload
                .continuation
                .as_ref()
                .is_some_and(|continuation| {
                    continuation.schema_version.is_some() || continuation.recovery.is_some()
                })
            {
                return Err(DecodeError::InvalidFrame("workspaceRecoveryVersion"));
            }
            let envelope = validated_cloud_envelope!(frame)?;
            let execution_spec = frame.payload.execution_spec;
            let schema_version = match *execution_spec.schema_version {
                1.0 => 1,
                2.0 => 2,
                3.0 => 3,
                _ => return Err(DecodeError::InvalidFrame("executionSpec.schemaVersion")),
            };
            let maximum_parallel_steps =
                u64::try_from(execution_spec.execution_limits.maximum_parallel_steps.0)
                    .map_err(|_| DecodeError::InvalidFrame("maximumParallelSteps"))?;
            let cancellation_grace_seconds = execution_spec
                .execution_limits
                .cancellation_grace_seconds
                .get();
            let workflow_definition_source = WorkflowDefinitionSourceV1RunnerProjection {
                repository_connection_id: execution_spec
                    .workflow_definition_source
                    .repository_connection_id
                    .to_string(),
                object_format: execution_spec
                    .workflow_definition_source
                    .object_format
                    .as_str()
                    .ok_or(DecodeError::InvalidFrame(
                        "workflowDefinitionSource.objectFormat",
                    ))?
                    .to_owned(),
                commit_oid: execution_spec
                    .workflow_definition_source
                    .commit_oid
                    .to_string(),
                workflow_path: execution_spec
                    .workflow_definition_source
                    .workflow_path
                    .to_string(),
                workflow_source_closure_digest: WorkflowSourceClosureDigestV1RunnerProjection {
                    algorithm: execution_spec
                        .workflow_definition_source
                        .workflow_source_closure_digest
                        .algorithm
                        .as_str()
                        .ok_or(DecodeError::InvalidFrame(
                            "workflowDefinitionSource.workflowSourceClosureDigest.algorithm",
                        ))?
                        .to_owned(),
                    value: execution_spec
                        .workflow_definition_source
                        .workflow_source_closure_digest
                        .value
                        .to_string(),
                },
            };
            let primary_workspace_source = PrimaryWorkspaceSourceV1RunnerProjection {
                kind: execution_spec
                    .primary_workspace_source
                    .kind
                    .as_str()
                    .ok_or(DecodeError::InvalidFrame("primaryWorkspaceSource.kind"))?
                    .to_owned(),
                provider_kind: execution_spec
                    .primary_workspace_source
                    .provider_kind
                    .to_string(),
                repository_connection_id: execution_spec
                    .primary_workspace_source
                    .repository_connection_id
                    .to_string(),
                object_format: execution_spec
                    .primary_workspace_source
                    .object_format
                    .as_str()
                    .ok_or(DecodeError::InvalidFrame(
                        "primaryWorkspaceSource.objectFormat",
                    ))?
                    .to_owned(),
                commit_oid: execution_spec
                    .primary_workspace_source
                    .commit_oid
                    .to_string(),
                materialization_contract: execution_spec
                    .primary_workspace_source
                    .materialization_contract
                    .as_str()
                    .ok_or(DecodeError::InvalidFrame(
                        "primaryWorkspaceSource.materializationContract",
                    ))?
                    .to_owned(),
            };
            let source_display_snapshot = execution_spec.source_display_snapshot.map(|snapshot| {
                let repository = match snapshot.repository {
                    generated::SourceDisplayRepository::HubSourceDisplayRepository(value) => {
                        SourceDisplayRepositoryV1RunnerProjection {
                            provider_kind: "github".to_owned(),
                            instance_url: None,
                            full_name: value.full_name.to_string(),
                        }
                    }
                    generated::SourceDisplayRepository::LabSourceDisplayRepository(value) => {
                        SourceDisplayRepositoryV1RunnerProjection {
                            provider_kind: "gitlab".to_owned(),
                            instance_url: Some("https://gitlab.com".to_owned()),
                            full_name: value.full_name.to_string(),
                        }
                    }
                };
                SourceDisplaySnapshotV1RunnerProjection {
                    organization_display_name: snapshot.organization_display_name.to_string(),
                    project_name: snapshot.project_name.to_string(),
                    repository,
                }
            });
            match (schema_version, source_display_snapshot.is_some()) {
                (1, false) | (2 | 3, true) => {}
                _ => {
                    return Err(DecodeError::InvalidFrame(
                        "executionSpec.sourceDisplaySnapshot",
                    ));
                }
            }
            let workspace_branch = execution_spec
                .workspace_branch
                .map(|branch| branch.to_string());
            let workspace_membership = execution_spec
                .workspace_membership
                .map(|membership| {
                    serde_json::to_value(membership)
                        .map_err(|_| DecodeError::InvalidFrame("workspaceMembership"))
                        .and_then(|value| {
                            serde_json::from_value(value)
                                .map_err(|_| DecodeError::InvalidFrame("workspaceMembership"))
                        })
                })
                .transpose()?;
            let run_inputs = execution_spec
                .run_inputs
                .map(|inputs| RunInputProjectionV1 {
                    input_set_id: inputs.input_set_id.to_string(),
                    manifest_digest: WorkflowSourceClosureDigestV1RunnerProjection {
                        algorithm: inputs
                            .manifest_digest
                            .algorithm
                            .as_str()
                            .unwrap_or("sha256")
                            .to_owned(),
                        value: inputs.manifest_digest.value.to_string(),
                    },
                });
            let workflow_tree = execution_spec
                .workflow_tree
                .map(workflow_tree_projection)
                .transpose()?;
            let capacity = execution_spec.capacity;
            let condition_transition_count = u64::try_from(capacity.condition_transition_count)
                .map_err(|_| DecodeError::InvalidFrame("conditionTransitionCount"))?;
            let aggregate_condition_transition_bytes =
                u64::try_from(capacity.aggregate_condition_transition_bytes)
                    .map_err(|_| DecodeError::InvalidFrame("aggregateConditionTransitionBytes"))?;
            let terminal_result_structure_bytes =
                u64::try_from(capacity.terminal_result_structure_bytes)
                    .map_err(|_| DecodeError::InvalidFrame("terminalResultStructureBytes"))?;
            let presentation_result_bytes =
                u64::try_from(capacity.presentation_result_bytes.unwrap_or(0))
                    .map_err(|_| DecodeError::InvalidFrame("presentationResultBytes"))?;
            let portable_result_bytes = u64::try_from(capacity.portable_result_bytes)
                .map_err(|_| DecodeError::InvalidFrame("portableResultBytes"))?;
            let encoded_outbox_bytes = u64::try_from(capacity.encoded_outbox_bytes)
                .map_err(|_| DecodeError::InvalidFrame("encodedOutboxBytes"))?;
            let capacity = ExecutionCapacityV1RunnerProjection {
                execution_contract: capacity
                    .execution_contract
                    .as_str()
                    .ok_or(DecodeError::InvalidFrame("executionContract"))?
                    .to_owned(),
                source_closure_digest: WorkflowSourceClosureDigestV1RunnerProjection {
                    algorithm: capacity
                        .source_closure_digest
                        .algorithm
                        .as_str()
                        .ok_or(DecodeError::InvalidFrame("sourceClosureDigest.algorithm"))?
                        .to_owned(),
                    value: capacity.source_closure_digest.value.to_string(),
                },
                general_maximum_transitions: capacity.general_maximum_transitions.get(),
                selected_maximum_transitions: capacity.selected_maximum_transitions.get(),
                maximum_invocations: capacity.maximum_invocations.get(),
                maximum_retained_bytes_per_invocation: capacity
                    .maximum_retained_bytes_per_invocation
                    .get(),
                diagnostic_retention_bytes: capacity.diagnostic_retention_bytes.get(),
                native_session_retention_bytes: capacity.native_session_retention_bytes.get(),
                aggregate_retention_bytes: capacity.aggregate_retention_bytes.get(),
                condition_transition_count,
                aggregate_condition_transition_bytes,
                terminal_result_structure_bytes,
                presentation_result_bytes,
                portable_result_bytes,
                encoded_outbox_bytes,
            };
            let continuation = frame
                .payload
                .continuation
                .map(|value| {
                    serde_json::to_value(value)
                        .map_err(|_| DecodeError::InvalidFrame("continuation"))
                        .and_then(|value| {
                            serde_json::from_value(value)
                                .map_err(|_| DecodeError::InvalidFrame("continuation"))
                        })
                })
                .transpose()?
                .map(Box::new);
            Ok(cloud(CloudFrame::AssignmentOffer {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                project_id: frame.payload.project_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                attempt_number: frame.payload.attempt_number.get(),
                execution_spec: Box::new(ExecutionSpecV1RunnerProjection {
                    execution_spec_id: execution_spec.execution_spec_id.to_string(),
                    schema_version,
                    execution_limits: ExecutionLimitsV1RunnerProjection {
                        maximum_parallel_steps,
                        cancellation_grace_seconds,
                    },
                    source_branch: execution_spec.source_branch.to_string(),
                    workflow_definition_source,
                    primary_workspace_source,
                    source_display_snapshot,
                    workspace_branch,
                    workspace_membership,
                    capacity,
                    run_inputs,
                    workflow_tree,
                }),
                continuation,
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentPrepare(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(cloud(CloudFrame::AssignmentPrepare {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                execution_spec_id: frame.payload.execution_spec_id.to_string(),
                preparation_expires_at: frame.payload.preparation_expires_at.to_string(),
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentStart(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let lease = frame.payload.lease;
            Ok(cloud(CloudFrame::AssignmentStart {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                execution_spec_id: frame.payload.execution_spec_id.to_string(),
                lease: ExecutionLeaseGrant {
                    sequence: lease.lease_sequence.get(),
                },
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentCancel(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(cloud(CloudFrame::AssignmentCancel {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                request_id: frame.payload.request_id.to_string(),
                mode: cancellation_mode(frame.payload.mode),
            }))
        }
        generated::RunnerProtocolVersion1::CloudExecutionStartAuthorized(frame) => {
            if frame.payload.schema_version.is_some() || frame.payload.workspace_recovery.is_some()
            {
                return Err(DecodeError::InvalidFrame("workspaceRecoveryVersion"));
            }
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(cloud(CloudFrame::ExecutionStartAuthorized {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentLeaseRenewed(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let lease = frame.payload.lease;
            let sequence = u64::try_from(lease.lease_sequence)
                .map_err(|_| DecodeError::InvalidFrame("leaseSequence"))?;
            Ok(cloud(CloudFrame::AssignmentLeaseRenewed {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                lease: ExecutionLeaseGrant { sequence },
            }))
        }
        generated::RunnerProtocolVersion1::CloudAssignmentRelease(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            Ok(cloud(CloudFrame::AssignmentRelease {
                envelope,
                effect_id: frame.payload.effect_id.to_string(),
                assignment_id: frame.payload.assignment_id.to_string(),
                run_id: frame.payload.run_id.to_string(),
                attempt_id: frame.payload.attempt_id.to_string(),
                reason: frame.payload.reason.to_string(),
            }))
        }
        generated::RunnerProtocolVersion1::CloudTriggerDelivery(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let payload = frame.payload;
            Ok(cloud(CloudFrame::TriggerDelivery {
                envelope,
                effect_id: payload.effect_id.to_string(),
                assignment_id: payload.assignment_id.to_string(),
                run_id: payload.run_id.to_string(),
                attempt_id: payload.attempt_id.to_string(),
                delivery: trigger_delivery(
                    payload.delivery_id,
                    payload.claim_id,
                    &payload.event,
                    &payload.claim,
                )?,
            }))
        }
        generated::RunnerProtocolVersion1::CloudRunControl(frame) => {
            let envelope = validated_cloud_envelope!(frame)?;
            let payload = frame.payload;
            Ok(cloud(CloudFrame::RunControl {
                envelope,
                effect_id: payload.effect_id.to_string(),
                assignment_id: payload.assignment_id.to_string(),
                run_id: payload.run_id.to_string(),
                attempt_id: payload.attempt_id.to_string(),
                control: RunControl {
                    control_id: payload.control_id.to_string(),
                    action: run_control_action(payload.control)?,
                },
            }))
        }
    }
}

/// The action a `run_control` carries. The schema admits each kind's members only with that
/// kind, so a member the kind does not carry is absent here; an answer's byte bound and the
/// wait's instant are checked beside it.
fn run_control_action(control: generated::RunControl) -> Result<RunControlAction, DecodeError> {
    let wait = control
        .wait
        .map(|wait| {
            Ok(RunControlWait {
                kind: wait.kind.to_string(),
                since: validate_timestamp(&wait.since)
                    .map_err(|_| DecodeError::InvalidFrame("wait.since"))?,
            })
        })
        .transpose()?;
    let required = |wait: Option<RunControlWait>| wait.ok_or(DecodeError::InvalidFrame("wait"));
    Ok(match control.kind {
        generated::RunControlKind::Answer => RunControlAction::Answer {
            wait: required(wait)?,
            answers: control
                .answers
                .into_iter()
                .map(|answer| {
                    let text = answer.text.to_string();
                    if text.len() > MAXIMUM_RUN_CONTROL_ANSWER_BYTES {
                        return Err(DecodeError::InvalidFrame("answers.text"));
                    }
                    Ok(RunControlAnswer {
                        question: answer.question.to_string(),
                        text,
                    })
                })
                .collect::<Result<_, _>>()?,
        },
        generated::RunControlKind::Approve => RunControlAction::Approve {
            wait: required(wait)?,
        },
        generated::RunControlKind::HandBack => RunControlAction::HandBack {
            wait: required(wait)?,
            notes: control
                .notes
                .ok_or(DecodeError::InvalidFrame("notes"))?
                .to_string(),
        },
        generated::RunControlKind::Lift => RunControlAction::Lift {
            wait,
            before: control.before.map(|step| step.to_string()),
        },
        generated::RunControlKind::Stop => RunControlAction::Stop {
            before: control
                .before
                .ok_or(DecodeError::InvalidFrame("before"))?
                .to_string(),
            reason: control.reason.map(|reason| reason.to_string()),
        },
        generated::RunControlKind::Retry => RunControlAction::Retry {
            wait: required(wait)?,
        },
    })
}

/// The delivery a `trigger_delivery` effect or a pending delivery carries, its event and
/// claim kept as the JSON Cloud sent.
fn trigger_delivery(
    delivery_id: generated::TriggerDeliveryId,
    claim_id: generated::SubjectClaimId,
    event: &generated::TriggerDeliveryEvent,
    claim: &generated::TriggerDeliveryClaim,
) -> Result<TriggerDelivery, DecodeError> {
    Ok(TriggerDelivery {
        delivery_id: delivery_id.to_string(),
        claim_id: claim_id.to_string(),
        event: serde_json::to_value(event).map_err(|_| DecodeError::InvalidFrame("event"))?,
        claim: serde_json::to_value(claim).map_err(|_| DecodeError::InvalidFrame("claim"))?,
    })
}

/// The generated frame `value` declares by its `type`, which the schema gives each frame
/// alone. Deserializing only that variant, rather than the untagged enum that tries each in
/// turn, keeps the decoder's stack to the one variant: a debug build of the enum's derived
/// deserializer reserves a result for every variant, which an encode deep in a workflow's
/// observer cannot afford.
fn generated_frame(value: Value) -> Result<generated::RunnerProtocolVersion1, DecodeError> {
    use generated::RunnerProtocolVersion1 as Frame;
    fn variant<T: serde::de::DeserializeOwned>(
        value: Value,
        frame: fn(T) -> Frame,
    ) -> Result<Frame, DecodeError> {
        serde_json::from_value(value)
            .map(frame)
            .map_err(|_| DecodeError::InvalidJson)
    }
    let frame_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match frame_type {
        "hello" => variant(value, Frame::RunnerHello),
        "workspace_retention_report" => variant(value, Frame::RunnerWorkspaceRetentionReport),
        "workspace_inventory" => variant(value, Frame::RunnerWorkspaceInventory),
        "workspace_inventory_complete" => variant(value, Frame::RunnerWorkspaceInventoryComplete),
        "effect_acknowledged" => variant(value, Frame::RunnerEffectAcknowledged),
        "assignment_preparing" => variant(value, Frame::RunnerAssignmentPreparing),
        "assignment_preparation_progress" => {
            variant(value, Frame::RunnerAssignmentPreparationProgress)
        }
        "continuation_ready" => variant(value, Frame::RunnerContinuationReady),
        "tree_progress" => variant(value, Frame::RunnerTreeProgress),
        "trigger_delivery_applied" => variant(value, Frame::RunnerTriggerDeliveryApplied),
        "run_control_applied" => variant(value, Frame::RunnerRunControlApplied),
        "assignment_accepted" => variant(value, Frame::RunnerAssignmentAccepted),
        "assignment_rejected" => variant(value, Frame::RunnerAssignmentRejected),
        "assignment_cancellation_applied" => {
            variant(value, Frame::RunnerAssignmentCancellationApplied)
        }
        "assignment_interrupted" => variant(value, Frame::RunnerAssignmentInterrupted),
        "execution_lease_renewal_requested" => {
            variant(value, Frame::RunnerExecutionLeaseRenewalRequested)
        }
        "execution_started" => variant(value, Frame::RunnerExecutionStarted),
        "execution_transition" => variant(value, Frame::RunnerExecutionTransition),
        "execution_finished" => variant(value, Frame::RunnerExecutionFinished),
        "execution_interrupted" => variant(value, Frame::RunnerExecutionInterrupted),
        "execution_aborted" => variant(value, Frame::RunnerExecutionAborted),
        "artifact_carrier_register" => variant(value, Frame::RunnerArtifactCarrierRegister),
        "artifact_carrier_confirm" => variant(value, Frame::RunnerArtifactCarrierConfirm),
        "artifact_result_register" => variant(value, Frame::RunnerArtifactResultRegister),
        "artifact_result_confirm" => variant(value, Frame::RunnerArtifactResultConfirm),
        "welcome" => variant(value, Frame::CloudWelcome),
        "observation_ack" => variant(value, Frame::CloudObservationAck),
        "workspace_registered" => variant(value, Frame::CloudWorkspaceRegistered),
        "workspace_registration_rejected" => {
            variant(value, Frame::CloudWorkspaceRegistrationRejected)
        }
        "artifact_carrier_registration" => variant(value, Frame::CloudArtifactCarrierRegistration),
        "artifact_carrier_confirmation" => variant(value, Frame::CloudArtifactCarrierConfirmation),
        "artifact_result_registration" => variant(value, Frame::CloudArtifactResultRegistration),
        "artifact_result_confirmation" => variant(value, Frame::CloudArtifactResultConfirmation),
        "assignment_offer" => variant(value, Frame::CloudAssignmentOffer),
        "assignment_prepare" => variant(value, Frame::CloudAssignmentPrepare),
        "assignment_start" => variant(value, Frame::CloudAssignmentStart),
        "assignment_cancel" => variant(value, Frame::CloudAssignmentCancel),
        "execution_start_authorized" => variant(value, Frame::CloudExecutionStartAuthorized),
        "assignment_lease_renewed" => variant(value, Frame::CloudAssignmentLeaseRenewed),
        "assignment_release" => variant(value, Frame::CloudAssignmentRelease),
        "trigger_delivery" => variant(value, Frame::CloudTriggerDelivery),
        "run_control" => variant(value, Frame::CloudRunControl),
        _ => Err(DecodeError::InvalidFrame("type")),
    }
}

fn workflow_tree_projection(
    tree: generated::WorkflowTreeExecution,
) -> Result<WorkflowTreeV1RunnerProjection, DecodeError> {
    let files = tree
        .files
        .into_iter()
        .map(|file| {
            Ok(WorkflowTreeFileV1RunnerProjection {
                path: file.path.to_string(),
                sha256: file.sha256.to_string(),
                size: u64::try_from(file.size)
                    .map_err(|_| DecodeError::InvalidFrame("workflowTree.files.size"))?,
                executable: file.executable,
            })
        })
        .collect::<Result<Vec<_>, DecodeError>>()?;
    let bindings = serde_json::to_value(&tree.bindings)
        .map_err(|_| DecodeError::InvalidFrame("workflowTree.bindings"))?;
    let resolution = serde_json::to_value(&tree.resolution)
        .map_err(|_| DecodeError::InvalidFrame("workflowTree.resolution"))?;
    let pending_deliveries = tree
        .pending_deliveries
        .into_iter()
        .map(|pending| {
            trigger_delivery(
                pending.delivery_id,
                pending.claim_id,
                &pending.event,
                &pending.claim,
            )
        })
        .collect::<Result<Vec<_>, DecodeError>>()?;
    Ok(WorkflowTreeV1RunnerProjection {
        digest: tree.digest.to_string(),
        definition_id: tree.definition_id.to_string(),
        revision: tree.revision.get(),
        workflow_path: tree.workflow_path.to_string(),
        files,
        bindings,
        resolution,
        owner: tree.owner.map(|owner| owner.display_name.to_string()),
        stop_before: tree.stop_before.map(|step| step.to_string()),
        pending_deliveries,
        run_url: tree.run_url,
    })
}

fn cancellation_mode(value: generated::CancellationMode) -> CancellationMode {
    match value {
        generated::CancellationMode::Graceful => CancellationMode::Graceful,
        generated::CancellationMode::Force => CancellationMode::Force,
    }
}

fn cancellation_application_disposition(
    value: generated::CancellationApplicationDisposition,
) -> CancellationApplicationDisposition {
    match value {
        generated::CancellationApplicationDisposition::PreExecutionStopped => {
            CancellationApplicationDisposition::PreExecutionStopped
        }
        generated::CancellationApplicationDisposition::OrdinaryCancelling => {
            CancellationApplicationDisposition::OrdinaryCancelling
        }
        generated::CancellationApplicationDisposition::FinalizersPreserved => {
            CancellationApplicationDisposition::FinalizersPreserved
        }
        generated::CancellationApplicationDisposition::ForceCancelling => {
            CancellationApplicationDisposition::ForceCancelling
        }
        generated::CancellationApplicationDisposition::ExecutionTerminal => {
            CancellationApplicationDisposition::ExecutionTerminal
        }
        generated::CancellationApplicationDisposition::Superseded => {
            CancellationApplicationDisposition::Superseded
        }
    }
}

fn validate_cancellation_application(
    mode: CancellationMode,
    effective_mode: CancellationMode,
    disposition: CancellationApplicationDisposition,
) -> Result<(), DecodeError> {
    let valid = match (mode, effective_mode) {
        (CancellationMode::Force, CancellationMode::Graceful) => false,
        (CancellationMode::Graceful, CancellationMode::Force) => {
            disposition == CancellationApplicationDisposition::Superseded
        }
        (CancellationMode::Force, CancellationMode::Force) => matches!(
            disposition,
            CancellationApplicationDisposition::PreExecutionStopped
                | CancellationApplicationDisposition::ForceCancelling
                | CancellationApplicationDisposition::ExecutionTerminal
        ),
        (CancellationMode::Graceful, CancellationMode::Graceful) => matches!(
            disposition,
            CancellationApplicationDisposition::PreExecutionStopped
                | CancellationApplicationDisposition::OrdinaryCancelling
                | CancellationApplicationDisposition::FinalizersPreserved
                | CancellationApplicationDisposition::ExecutionTerminal
        ),
    };
    if valid {
        Ok(())
    } else {
        Err(DecodeError::InvalidFrame("cancellation application"))
    }
}

fn artifact_registration_response(
    payload: generated::CloudArtifactCarrierRegistrationPayload,
) -> Result<ArtifactRegistrationResponse, DecodeError> {
    use generated::CloudArtifactCarrierRegistrationPayload as Payload;

    let (request_message_id, outcome) = match payload {
        Payload::Succeeded {
            artifact_set_id,
            carrier_id,
            request_message_id,
            upload_capability,
        } => (
            request_message_id.to_string(),
            ArtifactRegistrationOutcome::Succeeded {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
                upload_capability: artifact_upload_capability(upload_capability)?,
            },
        ),
        Payload::Retryable { request_message_id } => (
            request_message_id.to_string(),
            ArtifactRegistrationOutcome::Retryable,
        ),
        Payload::Failed {
            code,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactRegistrationOutcome::Failed {
                code: code.to_string(),
            },
        ),
    };
    Ok(ArtifactRegistrationResponse {
        request_message_id,
        outcome,
    })
}

fn artifact_confirmation_response(
    payload: generated::CloudArtifactCarrierConfirmationPayload,
) -> Result<ArtifactConfirmationResponse, DecodeError> {
    use generated::CloudArtifactCarrierConfirmationPayload as Payload;

    let (request_message_id, outcome) = match payload {
        Payload::Confirmed {
            artifact_set_id,
            carrier_id,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactConfirmationOutcome::Confirmed {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
            },
        ),
        Payload::Absent {
            artifact_set_id,
            carrier_id,
            request_message_id,
            upload_capability,
        } => (
            request_message_id.to_string(),
            ArtifactConfirmationOutcome::Absent {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
                upload_capability: artifact_upload_capability(upload_capability)?,
            },
        ),
        Payload::Retryable {
            artifact_set_id,
            carrier_id,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactConfirmationOutcome::Retryable {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
            },
        ),
        Payload::Failed {
            artifact_set_id,
            carrier_id,
            code,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactConfirmationOutcome::Failed {
                artifact_set_id: artifact_set_id.to_string(),
                carrier_id: carrier_id.to_string(),
                code: code.to_string(),
            },
        ),
    };
    Ok(ArtifactConfirmationResponse {
        request_message_id,
        outcome,
    })
}

fn artifact_result_registration_response(
    payload: generated::CloudArtifactResultRegistrationPayload,
) -> Result<ArtifactResultRegistrationResponse, DecodeError> {
    use generated::CloudArtifactResultRegistrationPayload as Payload;

    let (request_message_id, outcome) = match payload {
        Payload::Succeeded {
            artifact_set_id,
            finalization_deadline,
            request_message_id,
            upload_capability,
        } => (
            request_message_id.to_string(),
            ArtifactResultRegistrationOutcome::Succeeded {
                artifact_set_id: artifact_set_id.to_string(),
                finalization_deadline: validate_timestamp(&finalization_deadline)?,
                upload_capability: artifact_upload_capability(upload_capability)?,
            },
        ),
        Payload::Retryable { request_message_id } => (
            request_message_id.to_string(),
            ArtifactResultRegistrationOutcome::Retryable,
        ),
        Payload::Failed {
            code,
            request_message_id,
        } => (
            request_message_id.to_string(),
            ArtifactResultRegistrationOutcome::Failed {
                code: code.to_string(),
            },
        ),
    };
    Ok(ArtifactResultRegistrationResponse {
        request_message_id,
        outcome,
    })
}

fn artifact_result_confirmation_response(
    payload: generated::CloudArtifactResultConfirmationPayload,
) -> Result<ArtifactResultConfirmationResponse, DecodeError> {
    let value = serde_json::to_value(payload)
        .map_err(|_| DecodeError::InvalidFrame("artifact result confirmation"))?;
    let field = |name| {
        value
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(DecodeError::InvalidFrame("artifact result confirmation"))
    };
    let request_message_id = field("requestMessageId")?;
    let artifact_set_id = field("artifactSetId")?;
    let outcome = match field("outcome")?.as_str() {
        "confirmed" => ArtifactResultConfirmationOutcome::Confirmed { artifact_set_id },
        "pending" => ArtifactResultConfirmationOutcome::Pending { artifact_set_id },
        "absent" => {
            let capability = value
                .get("uploadCapability")
                .cloned()
                .ok_or(DecodeError::InvalidFrame("artifact upload capability"))?;
            let capability = serde_json::from_value::<generated::UploadCapability>(capability)
                .map_err(|_| DecodeError::InvalidFrame("artifact upload capability"))?;
            ArtifactResultConfirmationOutcome::Absent {
                artifact_set_id,
                upload_capability: artifact_upload_capability(capability)?,
            }
        }
        "retryable" => ArtifactResultConfirmationOutcome::Retryable { artifact_set_id },
        "failed" => ArtifactResultConfirmationOutcome::Failed {
            artifact_set_id,
            phase: field("phase")?,
            code: field("code")?,
        },
        _ => return Err(DecodeError::InvalidFrame("artifact result confirmation")),
    };
    Ok(ArtifactResultConfirmationResponse {
        request_message_id,
        outcome,
    })
}

fn artifact_upload_capability(
    capability: generated::UploadCapability,
) -> Result<ArtifactUploadCapability, DecodeError> {
    let expires_at = validate_timestamp(&capability.expires_at)?;
    let if_none_match = capability
        .headers
        .if_none_match
        .as_str()
        .ok_or(DecodeError::InvalidFrame("If-None-Match"))?
        .to_owned();
    Ok(ArtifactUploadCapability {
        url: capability.url,
        content_length: capability.headers.content_length.to_string(),
        content_type: capability.headers.content_type.to_string(),
        if_none_match,
        checksum_sha256: capability.headers.x_amz_checksum_sha256.to_string(),
        expires_at,
    })
}

fn condition_evidence_transition(value: &Value) -> bool {
    value
        .pointer("/payload/workflowEvent")
        .is_some_and(is_condition_evidence_workflow_event)
}

pub fn is_condition_evidence_workflow_event(event: &Value) -> bool {
    if event.get("eventType").and_then(Value::as_str) == Some("workflow_state_changed") {
        return ["from", "to"].into_iter().any(|side| {
            ["primaryIssue", "priorIssue"].into_iter().any(|field| {
                event
                    .get(side)
                    .and_then(|state| state.get(field))
                    .is_some_and(|issue| {
                        issue.get("state").and_then(Value::as_str) == Some("failed")
                            && is_condition_pointer_failure(&issue["detail"])
                    })
            })
        });
    }
    if event.get("eventType").and_then(Value::as_str) != Some("step_state_changed") {
        return false;
    }
    match event.get("to").and_then(Value::as_str) {
        Some("skipped") => {
            event.pointer("/detail/code").and_then(Value::as_str) == Some("condition_false")
        }
        Some("failed") => is_condition_pointer_failure(&event["detail"]),
        _ => false,
    }
}

fn is_condition_pointer_failure(detail: &Value) -> bool {
    detail.get("phase").and_then(Value::as_str) == Some("condition")
        && detail.get("code").and_then(Value::as_str) == Some("json_pointer_missing")
}

fn validate_protocol_schema(value: &Value) -> Result<(), DecodeError> {
    let validator = PROTOCOL_VALIDATOR
        .get_or_init(|| {
            let schema = serde_json::from_str::<Value>(PROTOCOL_SCHEMA).map_err(|_| ())?;
            jsonschema::draft202012::new(&schema).map_err(|_| ())
        })
        .as_ref()
        .map_err(|_| DecodeError::InvalidFrame("schema"))?;
    if validator.is_valid(value) {
        Ok(())
    } else {
        Err(DecodeError::InvalidFrame("schema"))
    }
}

fn unactivated_recovery_frame(value: &Value) -> bool {
    let kind = value.get("type").and_then(Value::as_str);
    let payload = value.get("payload");
    match kind {
        Some("assignment_offer") => payload
            .and_then(|body| body.get("continuation"))
            .is_some_and(|continuation| continuation.get("schemaVersion").is_some()),
        Some("continuation_ready" | "execution_started" | "execution_start_authorized") => {
            payload.is_some_and(|body| body.get("schemaVersion").is_some())
        }
        _ => false,
    }
}

fn validate_closed_shape(value: &Value) -> Result<(), DecodeError> {
    let object = value
        .as_object()
        .ok_or(DecodeError::InvalidFrame("envelope"))?;
    let direction = object
        .get("direction")
        .and_then(Value::as_str)
        .ok_or(DecodeError::InvalidFrame("direction"))?;
    let frame_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or(DecodeError::InvalidFrame("type"))?;
    let envelope_keys: &[&str] = if direction == RUNNER_TO_CLOUD {
        &[
            "protocolVersion",
            "direction",
            "messageId",
            "runnerId",
            "bootId",
            "sequence",
            "sentAt",
            "type",
            "payloadVersion",
            "payload",
        ]
    } else {
        &[
            "protocolVersion",
            "direction",
            "messageId",
            "sentAt",
            "type",
            "payloadVersion",
            "payload",
        ]
    };
    if object.len() != envelope_keys.len()
        || !envelope_keys.iter().all(|key| object.contains_key(*key))
    {
        return Err(DecodeError::InvalidFrame("envelope"));
    }
    let payload = object
        .get("payload")
        .and_then(Value::as_object)
        .ok_or(DecodeError::InvalidFrame("payload"))?;
    let payload_keys: &[&str] = match frame_type {
        "hello" => &["runnerVersion"],
        "workspace_inventory_complete" => {
            &["schemaVersion", "workRootId", "inventoryThroughSequence"]
        }
        "workspace_inventory" => &[
            "schemaVersion",
            "workRootId",
            "workspaceId",
            "runId",
            "priorAttemptId",
            "priorAssignmentId",
            "originRunnerBootId",
            "originAssignmentId",
            "originAttemptId",
            "executionRoot",
            "observedClaim",
            "lastClaimGeneration",
            "presence",
            "recoveryReason",
        ],
        "workspace_registered" => &[
            "schemaVersion",
            "acknowledgedMessageId",
            "acknowledgedSequence",
            "workRootId",
            "workspaceId",
            "runId",
            "originRunnerBootId",
            "originAssignmentId",
            "originAttemptId",
            "executionRoot",
        ],
        "workspace_registration_rejected" => &[
            "schemaVersion",
            "acknowledgedMessageId",
            "acknowledgedSequence",
            "workRootId",
            "workspaceId",
            "reason",
        ],
        "workspace_retention_report" => &[
            "assignmentId",
            "attemptId",
            "runId",
            "executionRoot",
            "state",
        ],
        "effect_acknowledged" => &["effectId"],
        "assignment_preparing" => &["effectId", "assignmentId", "offeredExecutionSpecId"],
        "assignment_preparation_progress" => {
            &["assignmentId", "attemptId", "preparationSequence", "phase"]
        }
        "continuation_ready" => &[
            "assignmentId",
            "attemptId",
            "startSnapshot",
            "quiescence",
            "modified",
        ],
        "assignment_accepted" => &["effectId", "assignmentId", "offeredExecutionSpecId"],
        "assignment_rejected" => &["effectId", "assignmentId", "decline"],
        "assignment_cancellation_applied" => &[
            "effectId",
            "requestId",
            "assignmentId",
            "attemptId",
            "mode",
            "effectiveMode",
            "disposition",
        ],
        "assignment_interrupted" => &["assignmentId", "attemptId", "reason"],
        "execution_lease_renewal_requested" => {
            &["assignmentId", "attemptId", "currentLeaseSequence"]
        }
        "execution_started" => &["assignmentId", "attemptId"],
        "execution_transition" => &[
            "assignmentId",
            "attemptId",
            "executionEventSequence",
            "workflowEvent",
        ],
        "tree_progress" => &[
            "assignmentId",
            "attemptId",
            "progressSequence",
            "attempt",
            "state",
            "position",
            "wait",
            "reportedAt",
        ],
        "trigger_delivery_applied" => &[
            "assignmentId",
            "attemptId",
            "deliveryId",
            "outcome",
            "observation",
        ],
        "run_control_applied" => &["assignmentId", "attemptId", "controlId", "outcome"],
        "execution_finished" => &[
            "assignmentId",
            "attemptId",
            "finalExecutionEventSequence",
            "outcome",
            "artifactDelivery",
        ],
        "execution_interrupted" => &[
            "assignmentId",
            "attemptId",
            "finalExecutionEventSequence",
            "reason",
            "terminalOutcome",
            "artifactDelivery",
        ],
        "execution_aborted" => &[
            "assignmentId",
            "attemptId",
            "lastExecutionEventSequence",
            "reason",
        ],
        "welcome" => &[
            "sessionId",
            "pingIntervalSeconds",
            "pongTimeoutSeconds",
            "leasePolicy",
        ],
        "observation_ack" => &["acknowledgedMessageId", "acknowledgedSequence"],
        "artifact_carrier_register" => &[
            "assignmentId",
            "attemptId",
            "portableOwnerPath",
            "mediaType",
            "sizeBytes",
            "sha256",
            "idempotencyKey",
        ],
        "artifact_carrier_confirm" => &["assignmentId", "attemptId", "artifactSetId", "carrierId"],
        "artifact_result_register" => &["assignmentId", "attemptId", "sizeBytes", "sha256"],
        "artifact_result_confirm" => &["assignmentId", "attemptId", "artifactSetId"],
        "artifact_carrier_registration"
        | "artifact_carrier_confirmation"
        | "artifact_result_registration"
        | "artifact_result_confirmation" => return Ok(()),
        "assignment_offer" => &[
            "effectId",
            "assignmentId",
            "runId",
            "projectId",
            "attemptId",
            "attemptNumber",
            "executionSpec",
        ],
        "assignment_prepare" => &[
            "effectId",
            "assignmentId",
            "runId",
            "attemptId",
            "executionSpecId",
            "preparationExpiresAt",
        ],
        "assignment_start" => &[
            "effectId",
            "assignmentId",
            "runId",
            "attemptId",
            "executionSpecId",
            "lease",
        ],
        "assignment_cancel" => &[
            "effectId",
            "assignmentId",
            "runId",
            "attemptId",
            "requestId",
            "mode",
        ],
        "execution_start_authorized" => &["effectId", "assignmentId", "runId", "attemptId"],
        "assignment_lease_renewed" => &["effectId", "assignmentId", "runId", "attemptId", "lease"],
        "assignment_release" => &["effectId", "assignmentId", "runId", "attemptId", "reason"],
        "trigger_delivery" => &[
            "effectId",
            "assignmentId",
            "runId",
            "attemptId",
            "deliveryId",
            "claimId",
            "event",
            "claim",
        ],
        "run_control" => &[
            "effectId",
            "assignmentId",
            "runId",
            "attemptId",
            "controlId",
            "control",
        ],
        _ => return Err(DecodeError::InvalidFrame("type")),
    };
    let optional_keys: &[&str] = match frame_type {
        "hello" => &[
            "workspaceRecoveryVersion",
            "workspaceRecoveryRoot",
            "treeExecution",
            "treeContinuation",
        ],
        "workspace_inventory" => &["discovery", "diagnostic"],
        "execution_transition" | "execution_interrupted" => &["diagnostic"],
        "execution_finished" => &["diagnostic", "chosenOutcome", "mirror", "claimRelease"],
        "tree_progress" => &["steps"],
        "assignment_offer" => &["continuation"],
        "workspace_retention_report" => &["settlementSnapshot"],
        "trigger_delivery_applied" => &["action", "code"],
        "run_control_applied" => &["code", "actionOutcome"],
        _ => &[],
    };
    if !payload_keys.iter().all(|key| payload.contains_key(*key))
        || !payload.keys().all(|key| {
            payload_keys.contains(&key.as_str()) || optional_keys.contains(&key.as_str())
        })
    {
        return Err(DecodeError::InvalidFrame("payload"));
    }
    Ok(())
}

fn validate_runner_frame(
    protocol_version: &Value,
    payload_version: &Value,
    direction: &Value,
    sent_at: generated::UtcTimestamp,
) -> Result<ValidatedFrame, DecodeError> {
    validate_constants(
        protocol_version,
        payload_version,
        direction,
        RUNNER_TO_CLOUD,
    )?;
    validate_timestamp(&sent_at)?;
    Ok(ValidatedFrame::Runner)
}

fn cloud_envelope(
    protocol_version: &Value,
    payload_version: &Value,
    direction: &Value,
    message_id: generated::CloudMessageId,
    sent_at: generated::UtcTimestamp,
) -> Result<CloudEnvelope, DecodeError> {
    validate_constants(
        protocol_version,
        payload_version,
        direction,
        CLOUD_TO_RUNNER,
    )?;
    Ok(CloudEnvelope {
        message_id: message_id.to_string(),
        sent_at: validate_timestamp(&sent_at)?,
    })
}

fn validate_constants(
    protocol_version: &Value,
    payload_version: &Value,
    direction: &Value,
    expected_direction: &str,
) -> Result<(), DecodeError> {
    if protocol_version.as_i64() != Some(PROTOCOL_VERSION) {
        return Err(DecodeError::InvalidFrame("protocolVersion"));
    }
    if payload_version.as_i64() != Some(PAYLOAD_VERSION) {
        return Err(DecodeError::InvalidFrame("payloadVersion"));
    }
    if direction.as_str() != Some(expected_direction) {
        return Err(DecodeError::InvalidFrame("direction"));
    }
    Ok(())
}

fn validate_timestamp(timestamp: &generated::UtcTimestamp) -> Result<String, DecodeError> {
    let value = timestamp.to_string();
    let parsed =
        OffsetDateTime::parse(&value, &Rfc3339).map_err(|_| DecodeError::InvalidFrame("sentAt"))?;
    if parsed.offset() != UtcOffset::UTC || !value.ends_with('Z') {
        return Err(DecodeError::InvalidFrame("sentAt"));
    }
    Ok(value)
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "protocol unit tests use Rust test assertions and fixture extraction"
)]
mod tests {
    use super::*;

    const VALID_FIXTURES: &[&[u8]] = &[
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-hello.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-effect-acknowledged.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-welcome.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-observation-ack.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-source-display.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-fresh-hello.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-assignment-accepted.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-assignment-rejected.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-continuation-ready.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-assignment-cancellation-applied.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-assignment-interrupted.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-start.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-cancel.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-execution-start-authorized.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-lease-renewal-requested.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-started.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-transition.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-transition-agent-failure.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-transition-output-failure.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-transition-recovery.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/execution-transition-agent-finalizer-succeeded.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/execution-transition-agent-finalizer-failed.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/execution-transition-agent-finalizer-cancelled.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-finished.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-finished-recovery.json"
        )),
        // REQ-WFRECOVERY-62
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-finished-recovery-handler-launch.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-finished-user-cancelled.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-finished-delivery-failure.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-finished-delivery-internal-failure.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-interrupted.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-execution-aborted.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-lease-renewed.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-release.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-artifact-carrier-register.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-artifact-carrier-confirm.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-artifact-carrier-registration.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-artifact-carrier-confirmation.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-artifact-result-register.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-artifact-result-register-maximum.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-artifact-result-confirm.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-artifact-result-registration.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-artifact-result-confirmation.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-artifact-result-pending.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/hello-tree-execution.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-workflow-tree.json"
        )),
        // REQ-COMPAT-18
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/hello-tree-continuation.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-tree-continuation.json"
        )),
        // REQ-RUNNERENROLL-99
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/execution-finished-chosen-outcome.json"
        )),
        // REQ-RUN-88, REQ-RUNNERENROLL-99
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/tree-progress-waiting.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/tree-progress-running.json"
        )),
        // REQ-COMPAT-18
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/tree-progress-steps.json"
        )),
        // REQ-TRIGGER-136
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-trigger-delivery.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/trigger-delivery-applied.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/trigger-delivery-refused.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-pending-deliveries.json"
        )),
        // REQ-RUNNERENROLL-90
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-run-control-answer.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-run-control-stop.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-run-control-lift.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/run-control-applied.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/run-control-refused.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/run-control-skipped.json"
        )),
    ];

    const INVALID_FIXTURES: &[&[u8]] = &[
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/unknown-type.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/runner-artifact-result-register-over-maximum.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/wrong-direction.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/unsupported-protocol-version.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/extra-envelope-field.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/extra-payload-field.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/invalid-runner-id.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/sequence-zero.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/non-utc-timestamp.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/execution-finished-zero-sequence.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/execution-interrupted-reason-mismatch.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/execution-aborted-inconsistent-zero.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/delivery-code-phase-mismatch.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/delivery-open-diagnostic.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/runner-execution-finished-recovery-version.json"
        )),
        // REQ-WFRECOVERY-62
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/runner-execution-finished-recovery-handler-launch-missing-launch-failure.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/execution-transition-agent-finalizer-recovery-handler.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/execution-transition-agent-finalizer-recovery-progress.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/cloud-assignment-offer-v2-missing-source-display.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/hello-tree-execution-wrong-version.json"
        )),
        // REQ-COMPAT-18
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/hello-tree-continuation-unknown.json"
        )),
        // REQ-RUNNERENROLL-99
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/execution-finished-chosen-outcome-empty.json"
        )),
        // REQ-RUNNERENROLL-99
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/tree-progress-unknown-state.json"
        )),
        // REQ-COMPAT-18
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/tree-progress-steps-unknown-state.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/tree-progress-steps-too-many.json"
        )),
        // REQ-TRIGGER-136
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/cloud-trigger-delivery-without-claim.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/trigger-delivery-applied-without-action.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/trigger-delivery-refused-with-action.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/cloud-assignment-offer-pending-delivery-without-event.json"
        )),
        // REQ-RUNNERENROLL-90
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/cloud-run-control-answer-without-wait.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/cloud-run-control-approve-null-wait.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/cloud-run-control-stop-with-wait.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/run-control-applied-with-code.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/run-control-refused-without-code.json"
        )),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/invalid/run-control-skipped-with-refusal-code.json"
        )),
    ];

    // INV-CONTRACT-7
    // INV-RUNNER-1
    #[test]
    fn generated_types_and_handwritten_validation_accept_every_valid_fixture() {
        for (index, fixture) in VALID_FIXTURES.iter().enumerate() {
            let parsed = serde_json::from_slice::<generated::RunnerProtocolVersion1>(fixture);
            assert!(
                parsed.is_ok(),
                "generated types rejected valid fixture {index}: {}",
                parsed.unwrap_err()
            );
            assert!(
                decode_frame(fixture).is_ok(),
                "validation rejected valid fixture {index}"
            );
        }
    }

    // INV-RUNNER-1
    #[test]
    fn discovery_wire_fixtures_are_live_without_enabling_admission() {
        for fixture in [
            include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/runner-protocol/v1/valid/runner-recovery-hello.json")).as_slice(),
            include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/runner-protocol/v1/valid/runner-workspace-inventory-complete.json")).as_slice(),
        ] {
            assert!(serde_json::from_slice::<generated::RunnerProtocolVersion1>(fixture).is_ok());
            let result = decode_frame(fixture);
            assert!(matches!(result, Ok(ValidatedFrame::Runner)), "{:?}", result.err());
        }
    }

    // INV-CONTRACT-7
    // INV-RUNNER-1
    #[test]
    fn initial_inventory_fixture_is_live() {
        let fixture = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-workspace-inventory-present.json"
        ));
        assert!(matches!(decode_frame(fixture), Ok(ValidatedFrame::Runner)));
    }

    #[test]
    fn generated_types_and_handwritten_validation_reject_every_invalid_fixture() {
        for (index, fixture) in INVALID_FIXTURES.iter().enumerate() {
            let result = serde_json::from_slice::<generated::RunnerProtocolVersion1>(fixture)
                .ok()
                .and_then(|frame| decode_frame(fixture).ok().map(|_| frame));
            assert!(result.is_none(), "invalid fixture {index} was accepted");
        }
    }

    #[test]
    fn cancellation_application_rejects_inconsistent_modes() {
        let fixture = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/runner-assignment-cancellation-applied.json"
        ));
        let mut frame: Value = serde_json::from_slice(fixture).unwrap();
        frame["payload"]["mode"] = json!("force");
        frame["payload"]["effectiveMode"] = json!("graceful");
        let encoded = serde_json::to_vec(&frame).unwrap();
        assert!(matches!(
            decode_frame(&encoded),
            Err(DecodeError::InvalidFrame("cancellation application"))
        ));
    }

    #[test]
    fn generated_artifact_media_type_uses_the_canonical_parameter_grammar() {
        for valid in [
            "application/octet-stream;version=1",
            "application/octet-stream ; version=1",
            "application/octet-stream\t;\tversion=1",
            "application/octet-stream ;\tversion=雪",
        ] {
            assert!(
                valid.parse::<generated::ArtifactMediaType>().is_ok(),
                "valid media type: {valid:?}"
            );
        }
        for control in ['\u{000b}', '\u{000c}', '\u{007f}'] {
            let invalid = format!("application/octet-stream;version=one{control}two");
            assert!(
                invalid.parse::<generated::ArtifactMediaType>().is_err(),
                "media type with U+{:04X} in a parameter value accepted",
                u32::from(control)
            );
        }
    }

    #[test]
    fn result_pending_decodes_as_nonterminal_set_identity() {
        let frame = decode_cloud_frame(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-artifact-result-pending.json"
        )))
        .unwrap();
        let CloudFrame::ArtifactResultConfirmation { response, .. } = frame else {
            panic!("wrong cloud frame kind");
        };
        assert!(
            matches!(response.outcome, ArtifactResultConfirmationOutcome::Pending {
            artifact_set_id
        } if artifact_set_id == "ats_01k0z6r1w8f4jy2m7q9v3x5ac0")
        );
    }

    #[test]
    fn decode_cloud_frame_rejects_runner_directed_frames() {
        assert!(matches!(
            decode_cloud_frame(VALID_FIXTURES[0]),
            Err(DecodeError::RunnerDirectedFrame)
        ));
    }

    #[test]
    fn origin_registration_replies_decode_as_correlated_closed_frames() {
        for (fixture, rejected) in [
            (include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/runner-protocol/v1/valid/cloud-workspace-registered.json")).as_slice(), false),
            (include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/runner-protocol/v1/valid/cloud-workspace-registration-rejected.json")).as_slice(), true),
        ] {
            let CloudFrame::WorkspaceRegistration { acknowledged_message_id, acknowledged_sequence, rejection, .. } = decode_cloud_frame(fixture).unwrap() else {
                panic!("wrong registration reply");
            };
            assert_eq!(acknowledged_message_id, "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc");
            assert_eq!(acknowledged_sequence, 2);
            assert_eq!(rejection.is_some(), rejected);
        }
    }

    #[test]
    fn encode_runner_frame_round_trips_through_generated_validation() {
        let envelope = RunnerEnvelope {
            message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            sequence: 1,
            sent_at: "2026-07-23T00:00:00Z".to_owned(),
        };
        let frame = RunnerFrame::Hello {
            envelope: envelope.clone(),
            runner_version: "0.2.0".to_owned(),
            recovery_root: None,
            tree_execution: true,
            tree_continuation: false,
        };

        let encoded = encode_runner_frame(&frame).unwrap();
        assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));
        // REQ-RUNNERENROLL-91
        let value: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(value["payload"]["treeExecution"], 1);
        // REQ-COMPAT-18
        assert!(value["payload"].get("treeContinuation").is_none());

        let continuing = RunnerFrame::Hello {
            envelope,
            runner_version: "0.2.0".to_owned(),
            recovery_root: None,
            tree_execution: true,
            tree_continuation: true,
        };
        let encoded = encode_runner_frame(&continuing).unwrap();
        assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));
        let value: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(value["payload"]["treeContinuation"], 1);
    }

    // REQ-COMPAT-18
    #[test]
    fn tree_continuation_offer_with_an_empty_partition_decodes() {
        let CloudFrame::AssignmentOffer {
            execution_spec,
            continuation,
            ..
        } = decode_cloud_frame(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-tree-continuation.json"
        )))
        .unwrap()
        else {
            panic!("expected an assignment offer");
        };
        assert!(execution_spec.workflow_tree.is_some());
        let continuation = continuation.expect("continuation block");
        assert!(continuation.reexecuted_steps.is_empty());
        assert!(continuation.inherited_steps.is_empty());
    }

    // REQ-RUNNERENROLL-99
    #[test]
    fn tree_finish_always_writes_its_exit_fields() {
        let mut frame = maximal_recovery_terminal_frame();
        let RunnerFrame::ExecutionFinished {
            outcome, tree_exit, ..
        } = &mut frame
        else {
            panic!("expected terminal frame");
        };
        *outcome = json!({"outcome": "succeeded", "forceAbort": null});
        *tree_exit = Some(TreeExit {
            chosen_outcome: None,
            mirror: Vec::new(),
            claim_release: false,
        });
        let encoded = encode_runner_frame(&frame).unwrap();
        assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));
        let payload = serde_json::from_slice::<Value>(&encoded).unwrap()["payload"].take();
        assert_eq!(payload["mirror"], json!([]));
        assert_eq!(payload["claimRelease"], json!(false));
        assert!(payload.get("chosenOutcome").is_none());

        let RunnerFrame::ExecutionFinished { tree_exit, .. } = &mut frame else {
            panic!("expected terminal frame");
        };
        if let Some(exit) = tree_exit {
            exit.chosen_outcome = Some("planned".to_owned());
        }
        let encoded = encode_runner_frame(&frame).unwrap();
        let payload = serde_json::from_slice::<Value>(&encoded).unwrap()["payload"].take();
        assert_eq!(payload["chosenOutcome"], "planned");

        let RunnerFrame::ExecutionFinished { tree_exit, .. } = &mut frame else {
            panic!("expected terminal frame");
        };
        *tree_exit = None;
        let encoded = encode_runner_frame(&frame).unwrap();
        let payload = serde_json::from_slice::<Value>(&encoded).unwrap()["payload"].take();
        for field in ["chosenOutcome", "mirror", "claimRelease"] {
            assert!(payload.get(field).is_none(), "{field} on a non-tree finish");
        }
    }

    // REQ-RUN-88, REQ-RUNNERENROLL-99
    #[test]
    fn tree_progress_encodes_the_contract_fixtures_and_requires_a_wait_while_waiting() {
        let fixture = |name: &str| -> Value {
            let path = format!(
                "{}/../../tests/fixtures/runner-protocol/v1/valid/{name}",
                env!("CARGO_MANIFEST_DIR")
            );
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
        };
        let progress =
            |message_id: &str, sequence, progress_sequence, progress| RunnerFrame::TreeProgress {
                envelope: RunnerEnvelope {
                    message_id: message_id.to_owned(),
                    runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    sequence,
                    sent_at: format!("2026-07-23T00:00:0{sequence}Z"),
                },
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                progress_sequence,
                progress,
            };
        let waiting = progress(
            "rmsg_01k0z6r1w8f4jy2m7q9v3x5atp",
            6,
            2,
            TreeRootProgress {
                attempt: 2,
                position: vec![json!("merge")],
                wait: Some(TreeProgressWait {
                    kind: "external".to_owned(),
                    since: "2026-07-23T00:00:05Z".to_owned(),
                    step: vec![json!("merge")],
                }),
                reported_at: "2026-07-23T00:00:06Z".to_owned(),
                steps: Vec::new(),
            },
        );
        let encoded = encode_runner_frame(&waiting).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).unwrap(),
            fixture("tree-progress-waiting.json")
        );
        let running = progress(
            "rmsg_01k0z6r1w8f4jy2m7q9v3x5atq",
            5,
            1,
            TreeRootProgress {
                attempt: 1,
                position: vec![json!("implement"), json!({"item": 2})],
                wait: None,
                reported_at: "2026-07-23T00:00:05Z".to_owned(),
                steps: Vec::new(),
            },
        );
        let encoded = encode_runner_frame(&running).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).unwrap(),
            fixture("tree-progress-running.json")
        );

        let mut unwaiting = fixture("tree-progress-waiting.json");
        unwaiting["payload"]["wait"] = Value::Null;
        assert!(decode_frame(&serde_json::to_vec(&unwaiting).unwrap()).is_err());
    }

    // REQ-COMPAT-18
    #[test]
    fn tree_progress_writes_steps_only_when_it_has_an_observation() {
        let fixture: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/tree-progress-steps.json"
        )))
        .unwrap();
        let mut frame = RunnerFrame::TreeProgress {
            envelope: RunnerEnvelope {
                message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5ats".to_owned(),
                runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                sequence: 7,
                sent_at: "2026-07-23T00:00:07Z".to_owned(),
            },
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            progress_sequence: 3,
            progress: TreeRootProgress {
                attempt: 1,
                position: vec![json!("implement"), json!({"item": 2})],
                wait: None,
                reported_at: "2026-07-23T00:00:07Z".to_owned(),
                steps: vec![
                    TreeStepObservation {
                        run: Vec::new(),
                        run_attempt: 1,
                        step: "implement".to_owned(),
                        state: TreeStepState::Running,
                    },
                    TreeStepObservation {
                        run: vec![TreeStepRunElement {
                            step: "implement".to_owned(),
                            item: Some(2),
                            ordinal: 1,
                        }],
                        run_attempt: 1,
                        step: "build".to_owned(),
                        state: TreeStepState::Succeeded,
                    },
                ],
            },
        };
        let encoded = encode_runner_frame(&frame).unwrap();
        assert!(
            decode_frame(&encoded).is_ok(),
            "the schema refused the written steps"
        );
        assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), fixture);

        let RunnerFrame::TreeProgress { progress, .. } = &mut frame else {
            panic!("expected a progress frame");
        };
        progress.steps.clear();
        let encoded = encode_runner_frame(&frame).unwrap();
        assert!(decode_frame(&encoded).is_ok());
        let payload = serde_json::from_slice::<Value>(&encoded).unwrap()["payload"].take();
        assert!(
            payload.get("steps").is_none(),
            "steps written without an observation"
        );
    }

    // REQ-RUNNERENROLL-91
    #[test]
    fn tree_offer_projects_the_files_bindings_and_run_page() {
        let CloudFrame::AssignmentOffer { execution_spec, .. } =
            decode_cloud_frame(include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-workflow-tree.json"
            )))
            .unwrap()
        else {
            panic!("expected an assignment offer");
        };
        let tree = execution_spec.workflow_tree.expect("workflow tree");
        assert_eq!(tree.definition_id, "def_01k0z6r1w8f4jy2m7q9v3x5abr");
        assert_eq!(tree.revision, 3);
        assert_eq!(tree.workflow_path, "workflows/build.yaml");
        assert_eq!(
            tree.files
                .iter()
                .map(|file| (file.path.as_str(), file.size, file.executable))
                .collect::<Vec<_>>(),
            [
                ("workflows/build.yaml", 812, false),
                ("workflows/scripts/check.sh", 96, true),
            ]
        );
        assert_eq!(
            tree.bindings["reviewers"],
            json!({"kind": "members", "value": ["prn_01k0z6r1w8f4jy2m7q9v3x5abs"]})
        );
        assert_eq!(
            tree.resolution["linearTeam"],
            json!({"kind": "linear.team", "value": "ENG", "id": "team_8c1b", "name": "Engineering"})
        );
        assert_eq!(tree.owner.as_deref(), Some("Ada Lovelace"));
        assert_eq!(tree.stop_before, None);
        assert_eq!(
            tree.run_url.as_deref(),
            Some("https://usefulmachinery.com/acme/runs/run_01k0z6r1w8f4jy2m7q9v3x5abp")
        );
    }

    // REQ-TRIGGER-136
    #[test]
    fn trigger_delivery_decodes_its_event_and_claim_and_the_application_encodes_the_fixtures() {
        let fixture = |name: &str| -> Value {
            let path = format!(
                "{}/../../tests/fixtures/runner-protocol/v1/valid/{name}",
                env!("CARGO_MANIFEST_DIR")
            );
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
        };
        let delivered = fixture("cloud-trigger-delivery.json");
        let CloudFrame::TriggerDelivery {
            effect_id,
            assignment_id,
            run_id,
            attempt_id,
            delivery,
            ..
        } = decode_cloud_frame(&serde_json::to_vec(&delivered).unwrap()).unwrap()
        else {
            panic!("expected a trigger delivery");
        };
        assert_eq!(
            (
                effect_id.as_str(),
                assignment_id.as_str(),
                run_id.as_str(),
                attempt_id.as_str()
            ),
            (
                "eff_01k0z6r1w8f4jy2m7q9v3x5atd",
                "asn_01k0z6r1w8f4jy2m7q9v3x5abn",
                "run_01k0z6r1w8f4jy2m7q9v3x5abp",
                "atm_01k0z6r1w8f4jy2m7q9v3x5abc"
            )
        );
        assert_eq!(delivery.delivery_id, "wtd_01k0z6r1w8f4jy2m7q9v3x5abt");
        assert_eq!(delivery.claim_id, "wsc_01k0z6r1w8f4jy2m7q9v3x5abv");
        // The event and claim reach the runner as Cloud sent them, for `um workflow run event`.
        assert_eq!(delivery.event, delivered["payload"]["event"]);
        assert_eq!(delivery.claim, delivered["payload"]["claim"]);

        let applied = |message_id: &str, sequence, outcome, observation| {
            RunnerFrame::TriggerDeliveryApplied {
                envelope: RunnerEnvelope {
                    message_id: message_id.to_owned(),
                    runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    sequence,
                    sent_at: "2026-07-23T00:00:10Z".to_owned(),
                },
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                application: TriggerDeliveryApplication {
                    delivery_id: "wtd_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned(),
                    outcome,
                    observation,
                },
            }
        };
        let encoded = encode_runner_frame(&applied(
            "rmsg_01k0z6r1w8f4jy2m7q9v3x5ata",
            7,
            TriggerDeliveryOutcome::Applied(TriggerDeliveryAction::Cancel),
            Some(TriggerIssueObservation {
                team: Some("team_8c1b".to_owned()),
                state: "state_canceled".to_owned(),
                updated_at: Some("2026-07-23T00:00:07.512Z".to_owned()),
            }),
        ))
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).unwrap(),
            fixture("trigger-delivery-applied.json")
        );
        let encoded = encode_runner_frame(&applied(
            "rmsg_01k0z6r1w8f4jy2m7q9v3x5atb",
            8,
            TriggerDeliveryOutcome::Refused {
                code: "event_not_later".to_owned(),
            },
            None,
        ))
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).unwrap(),
            fixture("trigger-delivery-refused.json")
        );
    }

    // REQ-RUNNERENROLL-90
    #[test]
    fn run_control_decodes_its_kind_and_the_application_encodes_the_fixtures() {
        let fixture = |name: &str| -> Value {
            let path = format!(
                "{}/../../tests/fixtures/runner-protocol/v1/valid/{name}",
                env!("CARGO_MANIFEST_DIR")
            );
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
        };
        let control = |document: &Value| {
            let CloudFrame::RunControl {
                effect_id,
                assignment_id,
                run_id,
                attempt_id,
                control,
                ..
            } = decode_cloud_frame(&serde_json::to_vec(document).unwrap()).unwrap()
            else {
                panic!("expected a run control");
            };
            assert_eq!(
                (assignment_id.as_str(), run_id.as_str(), attempt_id.as_str()),
                (
                    "asn_01k0z6r1w8f4jy2m7q9v3x5abn",
                    "run_01k0z6r1w8f4jy2m7q9v3x5abp",
                    "atm_01k0z6r1w8f4jy2m7q9v3x5abc"
                )
            );
            assert_eq!(control.control_id, "rct_01k0z6r1w8f4jy2m7q9v3x5acr");
            (effect_id, control.action)
        };
        let wait = RunControlWait {
            kind: "answer".to_owned(),
            since: "2026-07-23T00:00:05.250Z".to_owned(),
        };
        assert_eq!(
            control(&fixture("cloud-run-control-answer.json")),
            (
                "eff_01k0z6r1w8f4jy2m7q9v3x5acr".to_owned(),
                RunControlAction::Answer {
                    wait,
                    answers: vec![
                        RunControlAnswer {
                            question: "scope".to_owned(),
                            text: "Only the API.".to_owned(),
                        },
                        RunControlAnswer {
                            question: "2".to_owned(),
                            text: "Yes.".to_owned(),
                        },
                    ],
                }
            )
        );
        assert_eq!(
            control(&fixture("cloud-run-control-stop.json")).1,
            RunControlAction::Stop {
                before: "merge".to_owned(),
                reason: Some("Hold the merge until the release freeze ends.".to_owned()),
            }
        );
        assert_eq!(
            control(&fixture("cloud-run-control-lift.json")).1,
            RunControlAction::Lift {
                wait: None,
                before: Some("merge".to_owned()),
            }
        );
        // An answer within the schema's 65,536 characters but over 65,536 UTF-8 bytes is
        // refused, as is a wait whose `since` is no real instant.
        let mut oversized = fixture("cloud-run-control-answer.json");
        oversized["payload"]["control"]["answers"][0]["text"] = json!("é".repeat(65_536));
        assert!(matches!(
            decode_cloud_frame(&serde_json::to_vec(&oversized).unwrap()),
            Err(DecodeError::InvalidFrame("answers.text"))
        ));
        let mut impossible = fixture("cloud-run-control-answer.json");
        impossible["payload"]["control"]["wait"]["since"] = json!("2026-02-30T00:00:05Z");
        assert!(decode_cloud_frame(&serde_json::to_vec(&impossible).unwrap()).is_err());

        let applied = |message_id: &str, sequence, outcome| RunnerFrame::RunControlApplied {
            envelope: RunnerEnvelope {
                message_id: message_id.to_owned(),
                runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                sequence,
                sent_at: "2026-07-23T00:00:13Z".to_owned(),
            },
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            application: RunControlApplication {
                control_id: "rct_01k0z6r1w8f4jy2m7q9v3x5acr".to_owned(),
                outcome,
            },
        };
        for (message_id, sequence, outcome, name) in [
            (
                "rmsg_01k0z6r1w8f4jy2m7q9v3x5acr",
                9,
                RunControlOutcome::Applied {
                    action_outcome: "recorded".to_owned(),
                },
                "run-control-applied.json",
            ),
            (
                "rmsg_01k0z6r1w8f4jy2m7q9v3x5acs",
                10,
                RunControlOutcome::Refused {
                    code: "question_unknown".to_owned(),
                },
                "run-control-refused.json",
            ),
            (
                "rmsg_01k0z6r1w8f4jy2m7q9v3x5act",
                11,
                RunControlOutcome::Skipped(RunControlSkip::WaitNotCurrent),
                "run-control-skipped.json",
            ),
        ] {
            let encoded = encode_runner_frame(&applied(message_id, sequence, outcome)).unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&encoded).unwrap(),
                fixture(name)
            );
        }
        // A refusal code the contract does not admit is not encoded.
        assert!(
            encode_runner_frame(&applied(
                "rmsg_01k0z6r1w8f4jy2m7q9v3x5acs",
                10,
                RunControlOutcome::Refused {
                    code: "Not A Slug".to_owned(),
                },
            ))
            .is_err()
        );
    }

    // REQ-TRIGGER-136
    #[test]
    fn tree_offer_projects_its_pending_deliveries_in_order() {
        let offer = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-pending-deliveries.json"
        ));
        let document: Value = serde_json::from_slice(offer).unwrap();
        let CloudFrame::AssignmentOffer { execution_spec, .. } = decode_cloud_frame(offer).unwrap()
        else {
            panic!("expected an assignment offer");
        };
        let pending = execution_spec
            .workflow_tree
            .expect("workflow tree")
            .pending_deliveries;
        let wire = &document["payload"]["executionSpec"]["workflowTree"]["pendingDeliveries"];
        assert_eq!(
            pending
                .iter()
                .map(|delivery| (
                    delivery.delivery_id.as_str(),
                    delivery.claim_id.as_str(),
                    &delivery.event,
                    &delivery.claim
                ))
                .collect::<Vec<_>>(),
            [
                (
                    "wtd_01k0z6r1w8f4jy2m7q9v3x5abt",
                    "wsc_01k0z6r1w8f4jy2m7q9v3x5abv",
                    &wire[0]["event"],
                    &wire[0]["claim"]
                ),
                (
                    "wtd_01k0z6r1w8f4jy2m7q9v3x5abx",
                    "wsc_01k0z6r1w8f4jy2m7q9v3x5aby",
                    &wire[1]["event"],
                    &wire[1]["claim"]
                ),
            ]
        );
        // A claim from a manual start carries no admission observation.
        assert!(pending[1].claim.get("observation").is_none());
    }

    #[test]
    fn diagnosed_decline_keeps_closed_reason_and_sibling_safe_facts() {
        let frame = RunnerFrame::AssignmentRejected {
            envelope: maximal_envelope(),
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abh".to_owned(),
            decline: AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable)
                .diagnosed("source_materialization", "source_repository_unavailable"),
        };
        let encoded = encode_runner_frame(&frame).unwrap();
        let value: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            value["payload"]["decline"],
            json!({
                "type": "runner_unable",
                "reason": "source_service_unavailable",
                "diagnostic": {
                    "stage": "source_materialization",
                    "declineCause": "source_repository_unavailable",
                },
            })
        );
        assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));
    }

    #[test]
    fn retained_workspace_report_carries_only_owner_snapshots() {
        for settlement_snapshot in [
            None,
            Some(json!({
                "algorithm":"git_worktree_sha256_v1",
                "value":"a".repeat(64),
                "takenAt":"2026-10-01T00:00:00Z",
                "settledBy":"engine"
            })),
            Some(json!({
                "algorithm":"git_worktree_sha256_v1",
                "unavailable":"io_unavailable",
                "settledBy":"engine"
            })),
        ] {
            let frame = RunnerFrame::WorkspaceRetentionReport {
                envelope: maximal_envelope(),
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abh".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned(),
                run_id: "run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                execution_root: "/retained/run".to_owned(),
                state: "retained".to_owned(),
                settlement_snapshot: settlement_snapshot.clone(),
            };
            let encoded = encode_runner_frame(&frame).unwrap();
            let value: Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(
                value["payload"].get("settlementSnapshot"),
                settlement_snapshot.as_ref()
            );
            assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));
        }
    }

    #[test]
    fn result_registration_encodes_the_workflow_capacity_range() {
        for size_bytes in [202_027_693, 428_876_460, 1_127_929_176, 1_127_929_177] {
            let frame = RunnerFrame::ArtifactResultRegister {
                envelope: maximal_envelope(),
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abh".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned(),
                size_bytes,
                sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                    .to_owned(),
            };
            let encoded = encode_runner_frame(&frame);
            if size_bytes > 1_127_929_176 {
                assert!(encoded.is_err());
            } else {
                let encoded = encoded.unwrap();
                assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));
            }
        }
    }

    #[test]
    fn assignment_decisions_encode_only_the_closed_wire_vocabulary() {
        const EFFECT_ID: &str = "eff_01k0z6r1w8f4jy2m7q9v3x5abg";
        const ASSIGNMENT_ID: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abh";
        const EXECUTION_SPEC_ID: &str = "xsp_01k0z6r1w8f4jy2m7q9v3x5abj";
        let envelope = || RunnerEnvelope {
            message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            sequence: 1,
            sent_at: "2026-07-23T00:00:00Z".to_owned(),
        };
        let rejected = |decline| RunnerFrame::AssignmentRejected {
            envelope: envelope(),
            effect_id: EFFECT_ID.to_owned(),
            assignment_id: ASSIGNMENT_ID.to_owned(),
            decline,
        };
        let rejected_payload = |decline| {
            json!({
                "effectId": EFFECT_ID,
                "assignmentId": ASSIGNMENT_ID,
                "decline": decline,
            })
        };
        let cases = [
            (
                RunnerFrame::AssignmentAccepted {
                    envelope: envelope(),
                    effect_id: EFFECT_ID.to_owned(),
                    assignment_id: ASSIGNMENT_ID.to_owned(),
                    offered_execution_spec_id: EXECUTION_SPEC_ID.to_owned(),
                },
                json!({
                    "effectId": EFFECT_ID,
                    "assignmentId": ASSIGNMENT_ID,
                    "offeredExecutionSpecId": EXECUTION_SPEC_ID,
                }),
            ),
            (
                rejected(AssignmentDecline::CapacityUnavailable),
                rejected_payload(json!({ "type": "capacity_unavailable" })),
            ),
            (
                rejected(AssignmentDecline::RunnerUnable(
                    RunnerUnableReason::SourceServiceUnavailable,
                )),
                rejected_payload(json!({
                    "type": "runner_unable",
                    "reason": "source_service_unavailable",
                })),
            ),
            (
                rejected(AssignmentDecline::RunnerUnable(
                    RunnerUnableReason::InputServiceUnavailable,
                )),
                rejected_payload(json!({
                    "type": "runner_unable",
                    "reason": "input_service_unavailable",
                })),
            ),
            (
                rejected(AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::InvalidExecutionLimits,
                )),
                rejected_payload(json!({
                    "type": "execution_spec_invalid",
                    "reason": "invalid_execution_limits",
                })),
            ),
            (
                rejected(AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::InvalidInputProjection,
                )),
                rejected_payload(json!({
                    "type": "execution_spec_invalid",
                    "reason": "invalid_input_projection",
                })),
            ),
            (
                rejected(AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::InputManifestMismatch,
                )),
                rejected_payload(json!({
                    "type": "execution_spec_invalid",
                    "reason": "input_manifest_mismatch",
                })),
            ),
            (
                rejected(AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::InputContentUnavailable,
                )),
                rejected_payload(json!({
                    "type": "execution_spec_invalid",
                    "reason": "input_content_unavailable",
                })),
            ),
            (
                rejected(AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::InputContentMismatch,
                )),
                rejected_payload(json!({
                    "type": "execution_spec_invalid",
                    "reason": "input_content_mismatch",
                })),
            ),
            (
                rejected(AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::InputTextInvalid,
                )),
                rejected_payload(json!({
                    "type": "execution_spec_invalid",
                    "reason": "input_text_invalid",
                })),
            ),
            (
                rejected(AssignmentDecline::ExecutionSpecInvalid(
                    ExecutionSpecInvalidReason::SourceCommitUnavailable,
                )),
                rejected_payload(json!({
                    "type": "execution_spec_invalid",
                    "reason": "source_commit_unavailable",
                })),
            ),
        ];

        for (index, (frame, expected_payload)) in cases.into_iter().enumerate() {
            let encoded = encode_runner_frame(&frame)
                .unwrap_or_else(|error| panic!("assignment decision {index}: {error}"));
            let value: Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(value["payload"], expected_payload);
        }
    }

    #[test]
    fn retry_offer_keeps_its_authoritative_attempt_number() {
        let mut offer: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer.json"
        )))
        .unwrap();
        offer["payload"]["attemptNumber"] = json!(2);
        let decoded = decode_cloud_frame(&serde_json::to_vec(&offer).unwrap()).unwrap();
        assert!(matches!(
            decoded,
            CloudFrame::AssignmentOffer {
                attempt_number: 2,
                ..
            }
        ));
    }

    #[test]
    fn continuation_offer_carries_pinned_evidence_without_changing_an_ordinary_offer() {
        let mut offer: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-source-display.json"
        )))
        .unwrap();
        let original = decode_cloud_frame(&serde_json::to_vec(&offer).unwrap()).unwrap();
        assert!(matches!(
            original,
            CloudFrame::AssignmentOffer {
                continuation: None,
                ..
            }
        ));
        offer["payload"]["attemptNumber"] = json!(2);
        offer["payload"]["continuation"] = json!({
            "priorAssignmentId": "asn_01k0z6r1w8f4jy2m7q9v3x5abd",
            "priorAttemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abd",
            "requiredRunnerBootId": "rbt_01k0z6r1w8f4jy2m7q9v3x5abc",
            "executionRoot": "/retained/assignment/workspace",
            "definitionSource": offer["payload"]["executionSpec"]["workflowDefinitionSource"].clone(),
            "effectiveCapacity": offer["payload"]["executionSpec"]["capacity"].clone(),
            "priorManifestDigest": offer["payload"]["executionSpec"]["workflowDefinitionSource"]["workflowSourceClosureDigest"].clone(),
            "request": {"fromSteps": ["build"], "definition": "inherited"},
            "reexecutedSteps": ["build"],
            "inheritedSteps": [],
            "priorSettlementSnapshot": null,
        });
        let _: generated::ContinuationOffer =
            serde_json::from_value(offer["payload"]["continuation"].clone())
                .expect("canonical continuation payload");
        let decoded = decode_cloud_frame(&serde_json::to_vec(&offer).unwrap())
            .expect("versioned continuation offer");
        assert!(matches!(
            decoded,
            CloudFrame::AssignmentOffer {
                attempt_number: 2,
                continuation: Some(_),
                ..
            }
        ));
        offer["payload"]["continuation"]["unrecognizedClaim"] = json!(true);
        assert!(decode_cloud_frame(&serde_json::to_vec(&offer).unwrap()).is_err());
    }

    #[test]
    fn gitlab_offer_round_trips_and_rejects_mismatched_or_missing_snapshot() {
        let mut offer: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-source-display.json"
        )))
        .unwrap();
        let spec = &mut offer["payload"]["executionSpec"];
        spec["primaryWorkspaceSource"]["providerKind"] = json!("gitlab");
        spec["sourceDisplaySnapshot"]["repository"] = json!({
            "providerKind": "gitlab",
            "instanceUrl": "https://gitlab.com",
            "fullName": format!("group/{}project", "nested/".repeat(40))
        });
        let bytes = serde_json::to_vec(&offer).unwrap();
        let frame = decode_cloud_frame(&bytes).expect("GitLab offer must decode");
        let CloudFrame::AssignmentOffer { execution_spec, .. } = frame else {
            panic!("expected offer");
        };
        assert_eq!(
            execution_spec.primary_workspace_source.provider_kind,
            "gitlab"
        );
        assert_eq!(
            execution_spec
                .source_display_snapshot
                .unwrap()
                .repository
                .instance_url
                .as_deref(),
            Some("https://gitlab.com")
        );
        let accepted = offer.clone();
        offer["payload"]["executionSpec"]["sourceDisplaySnapshot"] = Value::Null;
        assert!(decode_cloud_frame(&serde_json::to_vec(&offer).unwrap()).is_err());
        offer = accepted;
        offer["payload"]["executionSpec"]["sourceDisplaySnapshot"]["repository"]["providerKind"] =
            json!("github");
        assert!(decode_cloud_frame(&serde_json::to_vec(&offer).unwrap()).is_err());
    }

    #[test]
    fn zero_execution_limit_reaches_semantic_admission() {
        let mut offer: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer.json"
        )))
        .unwrap();
        offer["payload"]["executionSpec"]["executionLimits"]["maximumParallelSteps"] = json!(0);
        let encoded = serde_json::to_vec(&offer).unwrap();

        let decoded = decode_cloud_frame(&encoded)
            .expect("invalid execution limits must receive a semantic rejection");

        assert!(matches!(
            decoded,
            CloudFrame::AssignmentOffer { execution_spec, .. }
                if execution_spec.execution_limits.maximum_parallel_steps == 0
        ));
    }

    #[test]
    fn malformed_source_values_are_rejected_by_protocol_decoding() {
        let mut offer: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer.json"
        )))
        .unwrap();
        offer["payload"]["executionSpec"]["workflowDefinitionSource"]["commitOid"] =
            json!("not-an-oid");
        offer["payload"]["executionSpec"]["primaryWorkspaceSource"]["providerKind"] =
            json!("unknown");
        let encoded = serde_json::to_vec(&offer).unwrap();

        assert!(decode_cloud_frame(&encoded).is_err());
    }

    fn maximal_envelope() -> RunnerEnvelope {
        RunnerEnvelope {
            message_id: "rmsg_07zzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            runner_id: "rnr_07zzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            boot_id: "rbt_07zzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            sequence: i64::MAX as u64,
            sent_at: "9999-12-31T23:59:59.999999999Z".to_owned(),
        }
    }

    fn maximal_diagnostic(kind: &str) -> Value {
        json!({
            "kind": kind,
            "reference": "r".repeat(128),
            "stream": {
                "retainedBytes": 4194304,
                "discardedBytes": 9223372036854775807_u64,
                "truncated": true,
                "fullyDrained": true,
                "digest": { "algorithm": "sha256", "value": "f".repeat(64) },
            }
        })
    }

    fn maximal_recovery_terminal_frame() -> RunnerFrame {
        let escaped = "\u{0001}".repeat(4_096);
        let mut remaining_rounds = 232_u64;
        let mut summaries = serde_json::Map::new();
        let mut invocation_id = 1_u64;
        for step_index in 0..24_u64 {
            let rounds_for_step = remaining_rounds.min(10);
            remaining_rounds -= rounds_for_step;
            let rounds = (1..=rounds_for_step)
                .map(|round| {
                    let target_invocation = invocation_id;
                    invocation_id += 1;
                    let handler_invocation = invocation_id;
                    invocation_id += 1;
                    json!({
                        "number": round,
                        "failedExecution": {
                            "executionNumber": round,
                            "invocationId": target_invocation,
                            "failure": {
                                "phase": "execution",
                                "cause": { "code": "command_exit", "exitCode": 2147483647 }
                            }
                        },
                        "handler": {
                            "kind": "cmd",
                            "invocationId": handler_invocation,
                            "outcome": "recheck",
                            "summary": escaped,
                            "reason": escaped,
                        }
                    })
                })
                .collect::<Vec<_>>();
            summaries.insert(
                format!("step{step_index}"),
                json!({
                    "schemaVersion": 1,
                    "configuredRetries": rounds_for_step,
                    "handlerKind": "cmd",
                    "rounds": rounds,
                    "termination": {
                        "kind": "exhausted",
                        "executionNumber": rounds_for_step + 1,
                    }
                }),
            );
        }
        assert_eq!(remaining_rounds, 0);
        RunnerFrame::ExecutionFinished {
            diagnostic: None,
            envelope: maximal_envelope(),
            assignment_id: "asn_07zzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            attempt_id: "atm_07zzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            final_execution_event_sequence: i64::MAX as u64,
            outcome: json!({
                "outcome": "failed",
                "forceAbort": null,
                "primaryIssue": {
                    "node": { "id": "step23", "role": "step" },
                    "state": "failed",
                    "detail": {
                        "phase": "execution",
                        "code": "command_exit",
                        "exitCode": 2147483647,
                    }
                },
                "recoverySummaries": summaries,
            }),
            artifact_delivery: json!({
                "outcome": "prepared",
                "artifactSetId": "ats_07zzzzzzzzzzzzzzzzzzzzzzzz",
            }),
            tree_exit: None,
        }
    }

    #[test]
    fn failed_node_diagnostic_is_a_validated_sibling() {
        let mut frame = maximal_recovery_terminal_frame();
        let RunnerFrame::ExecutionFinished {
            diagnostic,
            outcome,
            ..
        } = &mut frame
        else {
            panic!("expected terminal frame");
        };
        *outcome = json!({"outcome": "failed", "forceAbort": null,
            "primaryIssue": {"node": {"id": "agent", "role": "step"}, "state": "failed",
                "detail": {"phase": "execution", "code": "harness_failed"}}});
        *diagnostic = Some(json!({"harnessError": "future_value", "httpStatus": 503}));
        let encoded =
            encode_runner_frame(&frame).unwrap_or_else(|error| panic!("{error:?} {frame:?}"));
        assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));
        let value: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            value["payload"]["diagnostic"],
            json!({"harnessError": "future_value", "httpStatus": 503})
        );
        assert!(
            value["payload"]["outcome"]["primaryIssue"]
                .get("diagnostic")
                .is_none()
        );
        let RunnerFrame::ExecutionFinished { diagnostic, .. } = &mut frame else {
            panic!("expected terminal frame");
        };
        *diagnostic = Some(json!({"message": "private sentinel"}));
        assert!(matches!(
            encode_runner_frame(&frame),
            Err(EncodeError::InvalidFrame("schema"))
        ));
    }

    #[test]
    fn terminal_recovery_launch_failure_passes_the_runner_encoder() {
        let mut frame = maximal_recovery_terminal_frame();
        let RunnerFrame::ExecutionFinished { outcome, .. } = &mut frame else {
            panic!("expected terminal frame");
        };
        outcome["recoverySummaries"]["step0"]["rounds"][0]["failedExecution"]["failure"] =
            json!({ "phase": "start", "cause": { "code": "harness_start_failed" } });
        let encoded = encode_runner_frame(&frame).unwrap();
        assert!(decode_frame(&encoded).is_ok());
    }

    #[test]
    fn large_workflow_condition_issues_pass_the_runner_encoder() {
        let cases: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/condition-workflow-transitions.json"
        )))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let mut event = case["workflowEvent"].clone();
            for side in ["from", "to"] {
                for field in ["primaryIssue", "priorIssue"] {
                    if let Some(detail) = event
                        .get_mut(side)
                        .and_then(|state| state.get_mut(field))
                        .and_then(|issue| issue.get_mut("detail"))
                    {
                        detail["pointer"] = json!(format!("/{}", "\u{0007}".repeat(65_536)));
                    }
                }
            }
            let frame = RunnerFrame::ExecutionTransition {
                diagnostic: None,
                envelope: maximal_envelope(),
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                execution_event_sequence: 4,
                workflow_event: event.clone(),
            };
            let encoded = encode_runner_frame(&frame)
                .unwrap_or_else(|error| panic!("{}: {error:?}", case["name"]));
            assert!(encoded.len() > MAXIMUM_ORDINARY_FRAME_BYTES);
            assert!(matches!(decode_frame(&encoded), Ok(ValidatedFrame::Runner)));

            // A schema-valid execution failure does not acquire the condition allowance.
            for side in ["from", "to"] {
                for field in ["primaryIssue", "priorIssue"] {
                    if let Some(issue) = event.get_mut(side).and_then(|state| state.get_mut(field))
                    {
                        issue["detail"] =
                            json!({"phase":"execution","code":"command_exit","exitCode":1});
                    }
                }
            }
            assert!(!is_condition_evidence_workflow_event(&event));
            let mut ordinary: Value = serde_json::from_slice(&encoded).unwrap();
            ordinary["payload"]["workflowEvent"] = event;
            let mut ordinary = serde_json::to_vec(&ordinary).unwrap();
            assert!(decode_frame(&ordinary).is_ok());
            ordinary.resize(MAXIMUM_ORDINARY_FRAME_BYTES + 1, b' ');
            assert!(matches!(
                decode_frame(&ordinary),
                Err(DecodeError::InvalidFrame("sizeClass"))
            ));
        }
    }

    // INV-RUNNER-1
    #[test]
    fn canonical_maximal_recovery_frame_has_published_exact_size() {
        let fixture: Value = serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/fixtures/runner-protocol/v1/maximal-recovery-frame-size.json"
        )))
        .unwrap();
        let fixture_size = |field| usize::try_from(fixture[field].as_u64().unwrap()).unwrap();
        assert_eq!(
            fixture_size("maximumOrdinaryFrameBytes"),
            MAXIMUM_ORDINARY_FRAME_BYTES
        );
        assert_eq!(
            fixture_size("maximumConditionTransitionFrameBytes"),
            MAXIMUM_CONDITION_TRANSITION_FRAME_BYTES
        );
        assert_eq!(
            fixture_size("maximumTerminalFrameBytes"),
            MAXIMUM_TERMINAL_FRAME_BYTES
        );
        let encoded = encode_runner_frame(&maximal_recovery_terminal_frame()).unwrap();
        assert_eq!(encoded.len(), fixture_size("maximalRecoveryFrameBytes"));
        assert!(encoded.len() < MAXIMUM_TERMINAL_FRAME_BYTES);

        let settling = RunnerFrame::ExecutionTransition {
            diagnostic: None,
            envelope: maximal_envelope(),
            assignment_id: "asn_07zzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            attempt_id: "atm_07zzzzzzzzzzzzzzzzzzzzzzzz".to_owned(),
            execution_event_sequence: i64::MAX as u64,
            workflow_event: json!({
                "eventVersion": 1,
                "eventType": "step_state_changed",
                "transitionSequence": 1286,
                "stepId": "step23",
                "role": "step",
                "failurePolicy": "required",
                "from": "running",
                "to": "failed",
                "detail": {
                    "phase": "execution",
                    "code": "command_exit",
                    "exitCode": 2147483647,
                },
                "invocationEvidence": {
                    "invocationId": 1286,
                    "role": "target",
                    "targetExecution": 11,
                    "state": "settled",
                    "startedAt": "9999-12-31T23:59:59.999999998Z",
                    "finishedAt": "9999-12-31T23:59:59.999999999Z",
                    "durationMilliseconds": 9223372036854775807_u64,
                    "usage": {
                        "inputTokens": 9223372036854775807_u64,
                        "outputTokens": 9223372036854775807_u64,
                    },
                    "diagnostics": [
                        maximal_diagnostic("command_stdout"),
                        maximal_diagnostic("command_stderr"),
                    ],
                    "diagnosticReference": "r".repeat(128),
                }
            }),
        };
        let settling = encode_runner_frame(&settling).unwrap();
        assert_eq!(settling.len(), fixture_size("maximalSettlingFrameBytes"));
        assert!(settling.len() <= MAXIMUM_ORDINARY_FRAME_BYTES);

        let mut one_byte_over = encoded;
        one_byte_over.resize(MAXIMUM_TERMINAL_FRAME_BYTES + 1, b' ');
        assert!(matches!(
            decode_frame(&one_byte_over),
            Err(DecodeError::InvalidFrame("size"))
        ));
    }

    // INV-RUNNER-8
    #[test]
    fn decode_cloud_frame_rejects_invalid_welcome_timing_pair() {
        let bytes = br#"{
          "protocolVersion": 1,
          "direction": "cloud_to_runner",
          "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abh",
          "sentAt": "2026-07-23T00:00:00Z",
          "type": "welcome",
          "payloadVersion": 1,
          "payload": {
            "sessionId": "rsn_01k0z6r1w8f4jy2m7q9v3x5abj",
            "pingIntervalSeconds": 10,
            "pongTimeoutSeconds": 19,
            "leasePolicy": {
              "schemaVersion": 2,
              "forceStopAndReapBudgetMilliseconds": 5000,
              "terminalReportDeliveryBudgetMilliseconds": 5000,
              "renewalDeliveryBudgetMilliseconds": 5000,
              "leaseDurationMilliseconds": 371000,
              "fencingMarginMilliseconds": 11000
            }
          }
        }"#;

        assert!(matches!(
            decode_cloud_frame(bytes),
            Err(DecodeError::InvalidFrame("pongTimeoutSeconds"))
        ));
    }
}
