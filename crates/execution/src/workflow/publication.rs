use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use ring::digest::{Context as DigestContext, SHA256};
use rustix::fd::OwnedFd;
use rustix::fs::{
    AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags, flock, fstat, mkdirat, openat,
    renameat_with, statat, unlinkat,
};
use rustix::io::Errno;
use serde::de::Error as _;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use time::OffsetDateTime;

use super::super::ExecutionOutcome;
use super::admission::CancellationReason;
use super::agent::{AgentFailure, AgentFailureCause};
use super::agent_input::AgentInputStartFailure;
use super::artifact::{ArtifactExposeFailure, ArtifactStaging, CaptureFailureKind, StagedCarrier};
use super::artifact_set;
use super::diagnostic::{CapturedDiagnosticStream, StepDiagnostic};
use super::document::{FailurePolicy, FinalizationTrigger};
use super::evidence::{NodeDetail, PrimaryIssue};
use super::execution_root::open_directory;
use super::export_presentation::{ContentReason, Field, resolve_text};
use super::git_capture::GitCaptureFailure;
use super::input::InputPreparationFailureKind;
use super::private_staging::{directory_entry_names, same_file};
use super::resolution::WorkflowContentDigest;
use super::result_metadata;
use super::runtime::{
    ActiveStepInvocation, ExportSet, ExportUnavailableReason, ExportValue, FailurePhase,
    OutputProducer, RecoveryHandlerFailurePhase, RecoveryHandlerKind, RecoveryHandlerOutcome,
    RecoveryTerminalDisposition, RunOutcome, StepRecoveryState, StepState,
};
use super::schema_common::{lowercase_hex, utc_timestamp};
use super::step_runtime::{
    CommandExecutionFailure, CommandLaunchFailure, CommandPreparationFailure, OutputCaptureFailure,
    StepExecutionFailure, StepFailureCause, StepStartFailure, WorkingDirectoryFailure,
};
use super::validated::WorkflowNodeRole;
use super::validated::{
    ResolvedOutputSource, ValidatedExportPresentation, ValidatedPresentationField,
    WorkflowValueType,
};
use super::value::CapturedValue;
use super::workspace_snapshot::WorkspaceSnapshotV1;

const COMMAND: &str = "um workflow run";
const RETRY_COMMAND: &str = "um workflow retry";
const CONTINUE_COMMAND: &str = "um workflow continue";
const RESULT_FILE: &str = "result.json";
const EXPORT_DIRECTORY: &str = "exports";
const STAGING_ATTEMPTS: usize = 16;

#[derive(Clone, Debug)]
pub struct WorkflowRunTiming {
    pub started_at: OffsetDateTime,
    pub finished_at: OffsetDateTime,
    pub duration: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowStepTiming {
    pub started_at: OffsetDateTime,
    pub duration: Duration,
}

#[derive(Clone, Debug)]
pub struct WorkflowRunCancellation {
    pub reason: CancellationReason,
    pub force_stop_deadline: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowRunStepKind {
    Command,
    Agent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRunStep {
    pub id: String,
    pub role: WorkflowNodeRole,
    pub kind: WorkflowRunStepKind,
    pub failure_policy: FailurePolicy,
    pub state: StepState<CapturedValue>,
    pub timing: Option<WorkflowStepTiming>,
    pub command_output: Option<StepDiagnostic>,
    pub recovery: Option<StepRecoverySummaryV1>,
    pub invocations: Vec<RecoveryInvocationV1>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRunFinalization {
    pub trigger: FinalizationTrigger,
    pub finalizers: Vec<WorkflowRunStep>,
    pub cancellation: Option<WorkflowRunFinalizationCancellation>,
    pub force_abort: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRunFinalizationCancellation {
    pub reason: CancellationReason,
    pub force_stop_deadline: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContinuationRecordV1 {
    pub(crate) request: ContinuationRequestV1,
    pub(crate) from_steps: Vec<String>,
    pub(crate) reexecuted_steps: Vec<String>,
    pub(crate) inherited_steps: Vec<ContinuationInheritedStepV1>,
    pub(crate) definition_source: ContinuationDefinitionSourceV1,
    pub(crate) workspace: ContinuationWorkspaceV1,
}

/// Evidence captured after the authenticated retained-workspace claim and
/// acknowledged readiness. This is private runner state, not caller input.
pub struct CloudContinuationEvidence {
    pub execution_root: String,
    pub prior_execution_root: String,
    pub start_snapshot: serde_json::Value,
    pub prior_settlement_snapshot: Option<serde_json::Value>,
    pub modified: serde_json::Value,
    pub quiescence: serde_json::Value,
}

#[derive(Debug)]
pub struct CloudContinuationRecordError;

pub fn cloud_continuation_record(
    request: serde_json::Value,
    reexecuted_steps: Vec<String>,
    inherited_steps: Vec<serde_json::Value>,
    manifest_digest: DigestV1,
    prior_manifest_digest: DigestV1,
    evidence: CloudContinuationEvidence,
) -> Result<ContinuationRecordV1, CloudContinuationRecordError> {
    let request: ContinuationRequestV1 =
        serde_json::from_value(request).map_err(|_| CloudContinuationRecordError)?;
    let from_steps = request.from_steps.clone();
    let inherited_steps = inherited_steps
        .into_iter()
        .map(|step| serde_json::from_value(step).map_err(|_| CloudContinuationRecordError))
        .collect::<Result<_, _>>()?;
    let definition_source = match &request.definition {
        ContinuationRequestedDefinitionV1::Inherited(_) => {
            ContinuationDefinitionSourceV1::Inherited {
                manifest_digest,
                prior_manifest_digest,
            }
        }
        ContinuationRequestedDefinitionV1::Replaced { .. } => {
            ContinuationDefinitionSourceV1::Replaced {
                manifest_digest,
                prior_manifest_digest,
            }
        }
    };
    let workspace: ContinuationWorkspaceV1 = serde_json::from_value(serde_json::json!({
        "executionRoot": evidence.execution_root,
        "priorExecutionRoot": evidence.prior_execution_root,
        "preparation": "ready",
        "startSnapshot": evidence.start_snapshot,
        "priorSettlementSnapshot": evidence.prior_settlement_snapshot,
        "modified": evidence.modified,
        "quiescence": evidence.quiescence,
    }))
    .map_err(|_| CloudContinuationRecordError)?;
    let record = ContinuationRecordV1 {
        request,
        from_steps,
        reexecuted_steps,
        inherited_steps,
        definition_source,
        workspace,
    };
    if super::result_metadata::validate_continuation_record(&record) {
        Ok(record)
    } else {
        Err(CloudContinuationRecordError)
    }
}

impl ContinuationRecordV1 {
    pub fn reexecuted_steps(&self) -> &[String] {
        &self.reexecuted_steps
    }

    pub fn inherited_step_ids(&self) -> impl Iterator<Item = &str> {
        self.inherited_steps.iter().map(|step| step.id.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ContinuationRequestV1 {
    pub(crate) from_steps: Vec<String>,
    pub(crate) definition: ContinuationRequestedDefinitionV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) execution_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) expected_run_version: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ContinuationRequestedDefinitionV1 {
    Inherited(ContinuationInheritedDefinitionV1),
    Replaced {
        replaced: ContinuationReplacementDefinitionSourceV1,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContinuationInheritedDefinitionV1 {
    Inherited,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ContinuationReplacementDefinitionSourceV1 {
    Local(ContinuationLocalDefinitionSourceV1),
    Cloud(ContinuationCloudDefinitionSourceV1),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContinuationLocalDefinitionSourceV1 {
    pub(crate) path: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ContinuationCloudDefinitionSourceV1 {
    pub(crate) commit_oid: String,
    pub(crate) workflow_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ContinuationInheritedStepV1 {
    pub(crate) id: String,
    pub(crate) prior_state: super::evidence::InheritedPriorState,
    pub(crate) definition_changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ContinuationDefinitionSourceV1 {
    Inherited {
        #[serde(rename = "manifestDigest")]
        manifest_digest: DigestV1,
        #[serde(rename = "priorManifestDigest")]
        prior_manifest_digest: DigestV1,
    },
    Replaced {
        #[serde(rename = "manifestDigest")]
        manifest_digest: DigestV1,
        #[serde(rename = "priorManifestDigest")]
        prior_manifest_digest: DigestV1,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ContinuationWorkspaceV1 {
    pub(crate) execution_root: String,
    pub(crate) prior_execution_root: String,
    // Pre-staged local records omitted preparation; they were ready at admission.
    #[serde(default)]
    pub(crate) preparation: ContinuationPreparationV1,
    pub(crate) start_snapshot: Option<WorkspaceSnapshotV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) prior_settlement_snapshot: Option<WorkspaceSnapshotV1>,
    pub(crate) modified: WorkspaceModifiedV1,
    pub(crate) quiescence: Option<ContinuationQuiescenceV1>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContinuationPreparationV1 {
    Pending,
    #[default]
    Ready,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum WorkspaceModifiedV1 {
    Known(bool),
    Unknown(WorkspaceModifiedUnknownV1),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspaceModifiedUnknownV1 {
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ContinuationQuiescenceV1 {
    pub(crate) groups_recorded: u64,
    pub(crate) groups_terminated: u64,
    pub(crate) groups_absent: u64,
    pub(crate) proven_at: String,
}

#[derive(Clone, Debug)]
pub struct WorkflowRunResult {
    pub run_directory: PathBuf,
    pub attempt_number: u64,
    pub continuation: Option<ContinuationRecordV1>,
    pub output_producers: BTreeMap<String, BTreeMap<String, OutputProducer>>,
    pub workflow_path: String,
    pub source_root: PathBuf,
    pub content_digest: WorkflowContentDigest,
    pub execution_root: PathBuf,
    pub maximum_parallel_steps: NonZeroUsize,
    pub maximum_retained_bytes_per_stream: u64,
    pub cloud_capacity: Option<CloudExecutionCapacityV1>,
    pub maximum_result_bytes: u64,
    pub timing: WorkflowRunTiming,
    pub outcome: RunOutcome,
    pub cancellation: Option<WorkflowRunCancellation>,
    pub force_abort: Option<super::runtime::ForceAbortEvidence>,
    pub steps: Vec<WorkflowRunStep>,
    pub finalization: Option<WorkflowRunFinalization>,
    pub exports: ExportSet<CapturedValue>,
    pub export_sources: BTreeMap<String, ResolvedOutputSource>,
    pub export_presentation: BTreeMap<String, super::validated::ValidatedExportPresentation>,
}

#[derive(Clone)]
pub enum CloudCarrierBody {
    Staged(StagedCarrier),
    Bytes(Arc<[u8]>),
}

#[derive(Clone)]
pub struct CloudResultCarrier {
    pub portable_owner_path: String,
    pub idempotency_key: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub body: CloudCarrierBody,
}

pub struct PreparedCloudWorkflowResult {
    // Small results retain the existing in-memory delivery path. Large results
    // remain in a private file until every retryable upload has completed.
    pub result_json: Arc<[u8]>,
    pub result_file: Option<Arc<tempfile::NamedTempFile>>,
    pub result_size_bytes: u64,
    pub result_sha256: String,
    pub carriers: Vec<CloudResultCarrier>,
}

pub fn summary_disposition_matches(
    summarized: &StepState<()>,
    state: &StepState<CapturedValue>,
) -> bool {
    match (summarized, state) {
        (StepState::Succeeded { .. }, StepState::Succeeded { .. }) => true,
        (
            StepState::Inherited {
                detail: left_detail,
                disposition: left_disposition,
                ..
            },
            StepState::Inherited {
                detail: right_detail,
                disposition: right_disposition,
                ..
            },
        ) => left_detail == right_detail && left_disposition == right_disposition,
        (StepState::Failed { detail: left }, StepState::Failed { detail: right }) => left == right,
        (StepState::Blocked { detail: left }, StepState::Blocked { detail: right }) => {
            left == right
        }
        (StepState::Skipped { detail: left }, StepState::Skipped { detail: right }) => {
            left == right
        }
        (StepState::NotRun { detail: left }, StepState::NotRun { detail: right }) => left == right,
        (StepState::Cancelled { detail: left }, StepState::Cancelled { detail: right }) => {
            left == right
        }
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalPublicationPhase {
    TargetValidation,
    Staging,
    ExportCopy,
    Serialization,
    Close,
    Verification,
    Commit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalPublicationFailureKind {
    InvalidResultPath,
    ParentUnavailable,
    DestinationExists,
    StagingUnavailable,
    CarrierHandoffUnavailable,
    ExportWriteUnavailable,
    UnsupportedExport,
    InvalidRunResult,
    ResultConflict,
    SerializationUnavailable,
    VerificationUnavailable,
    AtomicPublicationUnavailable,
    CommittedDurabilityUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunResultInvariant {
    AttemptMetadata,
    Continuation,
    WorkflowMetadata,
    ExecutionMetadata,
    ResultStructure,
    StepMetadata,
    FinalizationMetadata,
    OutcomeMetadata,
    ExportMetadata,
    ExportSources,
    ExportValues,
    GitBranch,
    DiagnosticStream,
    Recovery,
    Failure,
    RetainedPath,
    Timing,
}

impl RunResultInvariant {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AttemptMetadata => "attempt_metadata",
            Self::Continuation => "continuation",
            Self::WorkflowMetadata => "workflow_metadata",
            Self::ExecutionMetadata => "execution_metadata",
            Self::ResultStructure => "result_structure",
            Self::StepMetadata => "step_metadata",
            Self::FinalizationMetadata => "finalization_metadata",
            Self::OutcomeMetadata => "outcome_metadata",
            Self::ExportMetadata => "export_metadata",
            Self::ExportSources => "export_sources",
            Self::ExportValues => "export_values",
            Self::GitBranch => "git_branch",
            Self::DiagnosticStream => "diagnostic_stream",
            Self::Recovery => "recovery",
            Self::Failure => "failure",
            Self::RetainedPath => "retained_path",
            Self::Timing => "timing",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalPublicationError {
    phase: LocalPublicationPhase,
    kind: LocalPublicationFailureKind,
    export: Option<String>,
    invariant: Option<RunResultInvariant>,
}

impl LocalPublicationError {
    pub fn phase(&self) -> LocalPublicationPhase {
        self.phase
    }

    pub(crate) fn kind(&self) -> LocalPublicationFailureKind {
        self.kind
    }

    pub(crate) fn export(&self) -> Option<&str> {
        self.export.as_deref()
    }

    pub fn invariant(&self) -> Option<RunResultInvariant> {
        self.invariant
    }

    /// A rename has made the result visible. The owner must leave the durable
    /// attempt pending so reconciliation can sync it before recording publication.
    pub fn committed(&self) -> bool {
        self.kind == LocalPublicationFailureKind::CommittedDurabilityUnavailable
    }

    /// Stable, content-free details for operator diagnostics. In particular, these
    /// omit the export name, which comes from the workflow definition.
    pub fn diagnostic_codes(&self) -> (&'static str, &'static str, Option<&'static str>) {
        let phase = match self.phase {
            LocalPublicationPhase::TargetValidation => "target_validation",
            LocalPublicationPhase::Staging => "staging",
            LocalPublicationPhase::ExportCopy => "export_copy",
            LocalPublicationPhase::Serialization => "serialization",
            LocalPublicationPhase::Close => "close",
            LocalPublicationPhase::Verification => "verification",
            LocalPublicationPhase::Commit => "commit",
        };
        let kind = match self.kind {
            LocalPublicationFailureKind::InvalidResultPath => "invalid_result_path",
            LocalPublicationFailureKind::ParentUnavailable => "parent_unavailable",
            LocalPublicationFailureKind::DestinationExists => "destination_exists",
            LocalPublicationFailureKind::StagingUnavailable => "staging_unavailable",
            LocalPublicationFailureKind::CarrierHandoffUnavailable => "carrier_handoff_unavailable",
            LocalPublicationFailureKind::ExportWriteUnavailable => "export_write_unavailable",
            LocalPublicationFailureKind::UnsupportedExport => "unsupported_export",
            LocalPublicationFailureKind::InvalidRunResult => "invalid_run_result",
            LocalPublicationFailureKind::ResultConflict => "result_conflict",
            LocalPublicationFailureKind::SerializationUnavailable => "serialization_unavailable",
            LocalPublicationFailureKind::VerificationUnavailable => "verification_unavailable",
            LocalPublicationFailureKind::AtomicPublicationUnavailable => {
                "atomic_publication_unavailable"
            }
            LocalPublicationFailureKind::CommittedDurabilityUnavailable => {
                "committed_durability_unavailable"
            }
        };
        (phase, kind, self.invariant.map(RunResultInvariant::as_str))
    }

    fn new(phase: LocalPublicationPhase, kind: LocalPublicationFailureKind) -> Self {
        Self {
            phase,
            kind,
            export: None,
            invariant: None,
        }
    }

    fn for_export(
        phase: LocalPublicationPhase,
        kind: LocalPublicationFailureKind,
        export: &str,
    ) -> Self {
        Self {
            phase,
            kind,
            export: Some(export.to_owned()),
            invariant: None,
        }
    }

    fn invalid(invariant: RunResultInvariant) -> Self {
        Self {
            phase: LocalPublicationPhase::Serialization,
            kind: LocalPublicationFailureKind::InvalidRunResult,
            export: None,
            invariant: Some(invariant),
        }
    }
}

impl fmt::Display for LocalPublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "local result publication failure during {:?}: {:?}",
            self.phase, self.kind
        )?;
        if let Some(export) = &self.export {
            write!(formatter, " for export {export:?}")?;
        }
        if let Some(invariant) = self.invariant {
            write!(formatter, " ({})", invariant.as_str())?;
        }
        Ok(())
    }
}

impl std::error::Error for LocalPublicationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowOutcomeV1 {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRunTerminalResultV1 {
    schema_version: u8,
    command: &'static str,
    outcome: WorkflowOutcomeV1,
    exit_status: u16,
    #[serde(skip)]
    execution_outcome: ExecutionOutcome,
    run_directory: String,
    attempt_number: u64,
    result_directory: String,
    result: WorkflowResultV1,
}

impl WorkflowRunTerminalResultV1 {
    pub(crate) fn exit_status(&self) -> u16 {
        self.exit_status
    }

    pub(crate) fn execution_outcome(&self) -> ExecutionOutcome {
        self.execution_outcome
    }

    pub fn result_directory(&self) -> &str {
        &self.result_directory
    }

    #[cfg(test)]
    pub(crate) fn result(&self) -> &WorkflowResultV1 {
        &self.result
    }

    pub fn mark_retry(&mut self) {
        self.command = RETRY_COMMAND;
    }

    pub fn mark_continue(&mut self) {
        self.command = CONTINUE_COMMAND;
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowResultV1 {
    pub(crate) schema_version: u8,
    pub(crate) attempt_number: u64,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) continuation: Option<ContinuationRecordV1>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) output_producers: BTreeMap<String, BTreeMap<String, OutputProducer>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) export_sources: BTreeMap<String, ExportSourceV1>,
    pub(crate) workflow: WorkflowIdentityV1,
    pub(crate) execution: WorkflowExecutionV1,
    pub(crate) command_output_policy: CommandOutputPolicyV1,
    pub(crate) outcome: WorkflowOutcomeV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) primary_issue: Option<PrimaryIssue>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) cancellation: Option<CancellationV1>,
    #[serde(deserialize_with = "deserialize_nullable_option")]
    pub(crate) force_abort: Option<ForceAbortV1>,
    pub steps: Vec<WorkflowStepV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) finalization: Option<FinalizationV1>,
    pub(crate) exports: BTreeMap<String, ExportV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowIdentityV1 {
    pub(crate) path: String,
    pub(crate) provenance: WorkflowProvenanceV1,
    pub(crate) digest: DigestV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum WorkflowProvenanceV1 {
    Local {
        #[serde(rename = "sourceRoot")]
        source_root: String,
    },
    Cloud {
        #[serde(rename = "projectId")]
        project_id: String,
        #[serde(rename = "repositoryConnectionId")]
        repository_connection_id: String,
        #[serde(rename = "objectFormat")]
        object_format: String,
        #[serde(rename = "commitOid")]
        commit_oid: String,
        #[serde(
            rename = "sourceDisplaySnapshot",
            default,
            deserialize_with = "deserialize_non_null_option",
            skip_serializing_if = "Option::is_none"
        )]
        source_display_snapshot: Option<CloudSourceDisplaySnapshotV1>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloudSourceDisplaySnapshotV1 {
    pub organization_display_name: String,
    pub project_name: String,
    pub repository: CloudSourceDisplayRepositoryV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloudSourceDisplayRepositoryV1 {
    pub provider_kind: String,
    pub full_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloudExecutionCapacityV1 {
    pub execution_contract: String,
    pub source_closure_digest: DigestV1,
    // Portable results and Runner frames are independently closed schemas; keeping this
    // flat projection explicit prevents either wire contract from becoming the other's ABI.
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
    #[serde(default, skip_serializing_if = "is_zero")]
    pub presentation_result_bytes: u64,
    pub portable_result_bytes: u64,
    pub encoded_outbox_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WorkflowExecutionV1 {
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) execution_root: Option<String>,
    pub(crate) maximum_parallel_steps: usize,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) capacity: Option<CloudExecutionCapacityV1>,
    pub(crate) started_at: String,
    pub(crate) finished_at: String,
    pub(crate) duration_milliseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CommandOutputPolicyV1 {
    pub(crate) encoding: String,
    pub(crate) maximum_retained_bytes_per_stream: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DigestV1 {
    pub algorithm: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CancellationV1 {
    pub(crate) reason: CancellationReasonV1,
    pub(crate) force_stop_deadline: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ForceAbortV1 {
    pub(crate) reason: CancellationReasonV1,
    pub(crate) phase: ForceAbortPhaseV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ForceAbortPhaseV1 {
    Ordinary,
    Finalization,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CancellationReasonV1 {
    UserRequest,
    TerminationRequest,
    CallerOutputFailure,
    RunnerShutdown,
    ExecutionLeaseExpired,
    ForceAbort,
}

// The published result's terminal-only enum must remain closed independently of the
// durable attempt projection, which also admits live states.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowStepStateV1 {
    Succeeded,
    Inherited,
    Failed,
    Blocked,
    Skipped,
    NotRun,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowNodeRoleV1 {
    Step,
    Finalizer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowNodeV1 {
    pub(crate) id: String,
    pub(crate) role: WorkflowNodeRoleV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ExportSourceV1 {
    pub(crate) node: WorkflowNodeV1,
    pub(crate) output: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowStepV1 {
    pub id: String,
    pub(crate) role: WorkflowNodeRoleV1,
    pub(crate) kind: String,
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) state: WorkflowStepStateV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) recovery: Option<StepRecoverySummaryV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) invocations: Vec<RecoveryInvocationV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) started_at: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) duration_milliseconds: Option<u64>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub detail: Option<NodeDetail>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) command_output: Option<CommandOutputV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryHandlerKindV1 {
    Cmd,
    Agent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepRecoverySummaryV1 {
    pub(crate) schema_version: u8,
    pub(crate) configured_retries: u8,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) handler_kind: Option<RecoveryHandlerKindV1>,
    pub(crate) rounds: Vec<RecoveryRoundSummaryV1>,
    pub(crate) termination: RecoveryTerminationV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryRoundSummaryV1 {
    pub(crate) number: u8,
    pub(crate) failed_execution: RecoveryFailedExecutionV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) handler: Option<RecoveryHandlerSummaryV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryFailedExecutionV1 {
    pub(crate) execution_number: u8,
    pub(crate) invocation_id: u64,
    pub(crate) failure: FailureV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryHandlerOutcomeV1 {
    Recheck,
    GaveUp,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryHandlerSummaryV1 {
    pub(crate) kind: RecoveryHandlerKindV1,
    pub(crate) invocation_id: u64,
    pub(crate) outcome: RecoveryHandlerOutcomeV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) summary: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) reason: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) failure: Option<RecoveryHandlerFailureV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RecoveryTerminationV1 {
    Recovered {
        #[serde(rename = "executionNumber")]
        execution_number: u8,
    },
    Exhausted {
        #[serde(rename = "executionNumber")]
        execution_number: u8,
    },
    GaveUp {
        round: u8,
    },
    HandlerFailed {
        round: u8,
        #[serde(rename = "handlerFailure")]
        handler_failure: RecoveryHandlerFailureV1,
    },
    Cancelled {
        round: u8,
        #[serde(rename = "activeRole")]
        active_role: RecoveryInvocationRoleV1,
        #[serde(
            rename = "executionNumber",
            default,
            deserialize_with = "deserialize_non_null_option",
            skip_serializing_if = "Option::is_none"
        )]
        execution_number: Option<u8>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryHandlerFailurePhaseV1 {
    Start,
    Execution,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryHandlerFailureV1 {
    pub(crate) phase: RecoveryHandlerFailurePhaseV1,
    pub(crate) cause: RecoveryHandlerFailureCauseV1,
}

// Recovery handler and target failures are separate closed schemas; keeping each field
// explicit is clearer than a generic cause that could admit fields at the wrong boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryHandlerFailureCauseV1 {
    pub(crate) code: RecoveryHandlerFailureCodeV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) exit_code: Option<i32>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) decision_rejection: Option<RecoveryDecisionRejectionV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryHandlerFailureCodeV1 {
    ContextUnavailable,
    HandlerUnavailable,
    WorkingDirectoryUnavailable,
    CommandPreparationFailed,
    CommandLaunchFailed,
    CommandWaitFailed,
    CommandExitFailed,
    ProcessQuiescenceFailed,
    ResultMissing,
    ResultSymbolicLink,
    ResultNotRegular,
    ResultUnavailable,
    ResultTooLarge,
    DecisionInvalid,
    AgentInputFailed,
    AgentFailed,
    AgentResultMissing,
    AgentResultInvalid,
    SettlementFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryDecisionRejectionV1 {
    InputTooLarge,
    InvalidUtf8,
    InvalidJson,
    DuplicateKey,
    UnknownField,
    UnsupportedSchemaVersion,
    UnknownDecision,
    EmptySummary,
    SummaryTooLong,
    EmptyReason,
    ReasonTooLong,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryInvocationRoleV1 {
    Target,
    RecoveryHandler,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryInvocationStateV1 {
    Settled,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryInvocationV1 {
    pub invocation_id: u64,
    pub role: RecoveryInvocationRoleV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub target_execution: Option<u8>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub recovery_round: Option<u8>,
    pub state: RecoveryInvocationStateV1,
    pub started_at: String,
    pub finished_at: String,
    pub duration_milliseconds: u64,
    pub usage: RecoveryInvocationUsageV1,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<RecoveryInvocationDiagnosticV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub diagnostic_reference: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryInvocationUsageV1 {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

pub(crate) fn total_recovery_usage(
    invocations: &[RecoveryInvocationV1],
) -> RecoveryInvocationUsageV1 {
    invocations
        .iter()
        .fold(RecoveryInvocationUsageV1::default(), |total, invocation| {
            RecoveryInvocationUsageV1 {
                input_tokens: total
                    .input_tokens
                    .saturating_add(invocation.usage.input_tokens),
                output_tokens: total
                    .output_tokens
                    .saturating_add(invocation.usage.output_tokens),
            }
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryDiagnosticKindV1 {
    CommandStdout,
    CommandStderr,
    AgentHarnessStdout,
    AgentHarnessStderr,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryInvocationDiagnosticV1 {
    pub kind: RecoveryDiagnosticKindV1,
    pub reference: String,
    pub stream: DiagnosticStreamV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FinalizationV1 {
    pub(crate) trigger: FinalizationTriggerV1,
    pub(crate) finalizers: Vec<WorkflowStepV1>,
    pub(crate) issues: Vec<FinalizationIssueV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) cancellation: Option<FinalizationCancellationV1>,
    pub(crate) force_abort: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FinalizationTriggerV1 {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FinalizationIssueV1 {
    pub(crate) node: WorkflowNodeV1,
    pub(crate) impact: FailurePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FinalizationCancellationV1 {
    pub(crate) reason: CancellationReasonV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) force_stop_deadline: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FailureV1 {
    pub(crate) phase: FailurePhaseV1,
    pub(crate) cause: FailureCauseV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailurePhaseV1 {
    Start,
    Execution,
    OutputCapture,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailureCodeV1 {
    StepUnavailable,
    PreparationTaskUnavailable,
    InputsUnavailable,
    OutputsUnsupported,
    AgentRuntimeUnavailable,
    AgentStepUnavailable,
    AgentAdmissionUnavailable,
    AgentInputsUnavailable,
    AgentInputMissingUpstream,
    AgentInputTypeMismatch,
    AgentSourceUnavailable,
    AgentSourceTextInvalid,
    AgentResultSchemaUnavailable,
    AgentValueModeInvalid,
    AgentAttachmentCountLimit,
    AgentAttachmentBytesLimit,
    ArtifactStagingMismatch,
    AgentStagingMismatch,
    AgentInputStagingUnavailable,
    HarnessStartFailed,
    HarnessInputTooLarge,
    HarnessFailed,
    HarnessProtocolFailed,
    MissingResponse,
    MissingResult,
    ResultValidationLimitExceeded,
    CapturedValueTooLarge,
    ResultSettlementFailed,
    InputInvalidName,
    InputValueCountLimit,
    InputValueSizeLimit,
    InputTotalSizeLimit,
    InputCollectionOrdinalLimit,
    InputTypeMismatch,
    InputSourceUnavailable,
    InputStagingUnavailable,
    InputLiveLimit,
    ExecutionRootRebound,
    WorkingDirectoryUnavailable,
    WorkingDirectoryEscape,
    WorkingDirectoryNotDirectory,
    CommandArgvInvalid,
    CommandPathUnconfigured,
    ExecutableNotFound,
    ExecutableUnavailable,
    CommandLaunchNotFound,
    CommandLaunchPermissionDenied,
    CommandLaunchInvalidInput,
    CommandLaunchFailed,
    CommandExit,
    CommandWaitFailed,
    ExecutionTaskUnavailable,
    OutputUnsupported,
    CaptureTaskUnavailable,
    OutputPathAbsolute,
    OutputPathEscape,
    OutputPathEmpty,
    OutputMissing,
    OutputSymbolicLink,
    OutputParentNotDirectory,
    OutputNotRegularFile,
    OutputSourceUnavailable,
    OutputInvalidUtf8,
    OutputInvalidJson,
    OutputDuplicateJsonMember,
    OutputJsonSchemaMismatch,
    CapturedFileCountLimit,
    CapturedFileSizeLimit,
    CapturedTotalSizeLimit,
    CapturedGitCarrierCountLimit,
    CapturedGitCarrierSizeLimit,
    CapturedTotalGitCarrierSizeLimit,
    GitExecutionRootRebound,
    GitHeadUnavailable,
    GitBaselineNotAncestor,
    GitCleanlinessUnavailable,
    GitWorkspaceDirty,
    GitTreeUnavailable,
    GitRequiredObjectsUnavailable,
    GitSourceAuthorityChanged,
    GitStructureLimitExceeded,
    GitCommandTimedOut,
    GitBundleGenerationFailed,
    GitBundleProfileInvalid,
    GitBundleVerificationFailed,
    GitWorkspaceChanged,
    GitTemporaryStorageUnavailable,
    OutputStagingUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FailureCauseV1 {
    pub(crate) code: FailureCodeV1,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) input: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) collection_index: Option<usize>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) output: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) exit_code: Option<i32>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn deserialize_nullable_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn deserialize_non_null_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl FailureCauseV1 {
    fn code(code: FailureCodeV1) -> Self {
        Self {
            code,
            input: None,
            collection_index: None,
            output: None,
            exit_code: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandOutputV1 {
    pub stdout: DiagnosticStreamV1,
    pub stderr: DiagnosticStreamV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiagnosticStreamV1 {
    pub(crate) encoding: String,
    pub(crate) data: String,
    pub retained_bytes: u64,
    pub discarded_bytes: u64,
    pub truncated: bool,
    pub fully_drained: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExportV1 {
    Available {
        presentation: Option<ExportPresentationV1>,
        kind: String,
        media_type: String,
        path: String,
        size_bytes: u64,
        digest: DigestV1,
        provenance: Option<ExportProvenanceV1>,
        producer: Option<OutputProducer>,
    },
    GitBranch {
        presentation: Option<ExportPresentationV1>,
        artifact_version: u8,
        object_format: String,
        base_oid: String,
        head_oid: String,
        tree_oid: String,
        carrier: Option<GitBranchCarrierV1>,
        provenance: Option<ExportProvenanceV1>,
        producer: Option<OutputProducer>,
    },
    Unavailable {
        reason: ExportUnavailableReasonV1,
        presentation: Option<ExportPresentationV1>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExportPresentationV1 {
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) title: Option<PresentationFieldV1>,
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) description: Option<PresentationFieldV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PresentationFieldV1 {
    Available {
        value: String,
    },
    Unavailable {
        reason: PresentationUnavailableReasonV1,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PresentationUnavailableReasonV1 {
    SourceFailed,
    SourceBlocked,
    SourceNotRun,
    SourceInputUnavailable,
    SourceTriggerNotSelected,
    SourceCancelled,
    Blank,
    TooLarge,
    ControlCharacter,
    MultilineTitle,
}

impl ExportV1 {
    pub(super) fn same_carrier_metadata(&self, other: &Self) -> bool {
        if self == other {
            return true;
        }
        let without_presentation = |export: &Self| {
            let mut metadata = export.clone();
            match &mut metadata {
                Self::Available { presentation, .. }
                | Self::GitBranch { presentation, .. }
                | Self::Unavailable { presentation, .. } => *presentation = None,
            }
            metadata
        };
        // Presentation belongs to each export alias, independently of its carrier.
        without_presentation(self) == without_presentation(other)
    }

    pub(crate) fn presentation(&self) -> Option<&ExportPresentationV1> {
        match self {
            Self::Available { presentation, .. }
            | Self::GitBranch { presentation, .. }
            | Self::Unavailable { presentation, .. } => presentation.as_ref(),
        }
    }

    fn set_presentation(&mut self, value: ExportPresentationV1) {
        match self {
            Self::Available { presentation, .. }
            | Self::GitBranch { presentation, .. }
            | Self::Unavailable { presentation, .. } => *presentation = Some(value),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum ExportProvenanceV1 {
    Inherited,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GitBranchCarrierV1 {
    pub(crate) path: String,
    pub(crate) media_type: String,
    pub(crate) size_bytes: u64,
    pub(crate) digest: DigestV1,
}

fn serialize_export_origin<State>(
    state: &mut State,
    provenance: Option<&ExportProvenanceV1>,
    producer: Option<&OutputProducer>,
) -> Result<(), State::Error>
where
    State: SerializeStruct,
{
    if let Some(provenance) = provenance {
        state.serialize_field("provenance", provenance)?;
    }
    if let Some(producer) = producer {
        state.serialize_field("producer", producer)?;
    }
    Ok(())
}

impl Serialize for ExportV1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Available {
                kind,
                media_type,
                path,
                size_bytes,
                digest,
                provenance,
                producer,
                presentation,
            } => {
                let mut state = serializer.serialize_struct(
                    "AvailableExportV1",
                    6 + usize::from(provenance.is_some())
                        + usize::from(producer.is_some())
                        + usize::from(presentation.is_some()),
                )?;
                state.serialize_field("state", "available")?;
                state.serialize_field("kind", kind)?;
                state.serialize_field("mediaType", media_type)?;
                state.serialize_field("path", path)?;
                state.serialize_field("sizeBytes", size_bytes)?;
                state.serialize_field("digest", digest)?;
                serialize_export_origin(&mut state, provenance.as_ref(), producer.as_ref())?;
                if let Some(presentation) = presentation {
                    state.serialize_field("presentation", presentation)?;
                }
                state.end()
            }
            Self::GitBranch {
                artifact_version,
                object_format,
                base_oid,
                head_oid,
                tree_oid,
                carrier,
                provenance,
                producer,
                presentation,
            } => {
                let mut state = serializer.serialize_struct(
                    "GitBranchExportV1",
                    7 + usize::from(carrier.is_some())
                        + usize::from(provenance.is_some())
                        + usize::from(producer.is_some())
                        + usize::from(presentation.is_some()),
                )?;
                state.serialize_field("state", "available")?;
                state.serialize_field("kind", "git_branch")?;
                state.serialize_field("artifactVersion", artifact_version)?;
                state.serialize_field("objectFormat", object_format)?;
                state.serialize_field("baseOid", base_oid)?;
                state.serialize_field("headOid", head_oid)?;
                state.serialize_field("treeOid", tree_oid)?;
                if let Some(carrier) = carrier {
                    state.serialize_field("carrier", carrier)?;
                }
                serialize_export_origin(&mut state, provenance.as_ref(), producer.as_ref())?;
                if let Some(presentation) = presentation {
                    state.serialize_field("presentation", presentation)?;
                }
                state.end()
            }
            Self::Unavailable {
                reason,
                presentation,
            } => {
                let mut state = serializer.serialize_struct(
                    "UnavailableExportV1",
                    2 + usize::from(presentation.is_some()),
                )?;
                state.serialize_field("state", "unavailable")?;
                state.serialize_field("reason", reason)?;
                if let Some(presentation) = presentation {
                    state.serialize_field("presentation", presentation)?;
                }
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ExportV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let state = value.get("state").and_then(Value::as_str);
        let kind = value.get("kind").and_then(Value::as_str);
        match (state, kind) {
            (Some("available"), Some("git_branch")) => {
                let wire = serde_json::from_value::<GitBranchExportWire>(value)
                    .map_err(D::Error::custom)?;
                if wire.state != "available" || wire.kind != "git_branch" {
                    return Err(D::Error::custom("invalid Git branch export"));
                }
                Ok(Self::GitBranch {
                    artifact_version: wire.artifact_version,
                    object_format: wire.object_format,
                    base_oid: wire.base_oid,
                    head_oid: wire.head_oid,
                    tree_oid: wire.tree_oid,
                    carrier: wire.carrier,
                    provenance: wire.provenance,
                    producer: wire.producer,
                    presentation: wire.presentation,
                })
            }
            (Some("available"), Some(_)) => {
                let wire = serde_json::from_value::<AvailableExportWire>(value)
                    .map_err(D::Error::custom)?;
                if wire.state != "available" {
                    return Err(D::Error::custom("invalid available export"));
                }
                Ok(Self::Available {
                    kind: wire.kind,
                    media_type: wire.media_type,
                    path: wire.path,
                    size_bytes: wire.size_bytes,
                    digest: wire.digest,
                    provenance: wire.provenance,
                    producer: wire.producer,
                    presentation: wire.presentation,
                })
            }
            (Some("unavailable"), None) => {
                let wire = serde_json::from_value::<UnavailableExportWire>(value)
                    .map_err(D::Error::custom)?;
                if wire.state != "unavailable" {
                    return Err(D::Error::custom("invalid unavailable export"));
                }
                Ok(Self::Unavailable {
                    reason: wire.reason,
                    presentation: wire.presentation,
                })
            }
            _ => Err(D::Error::custom("invalid export state or kind")),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AvailableExportWire {
    state: String,
    kind: String,
    media_type: String,
    path: String,
    size_bytes: u64,
    digest: DigestV1,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    provenance: Option<ExportProvenanceV1>,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    producer: Option<OutputProducer>,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    presentation: Option<ExportPresentationV1>,
}

// Available file-like and Git exports intentionally keep separate closed wire structs;
// flattening their shared optional origin fields would weaken unknown-field rejection.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GitBranchExportWire {
    state: String,
    kind: String,
    artifact_version: u8,
    object_format: String,
    base_oid: String,
    head_oid: String,
    tree_oid: String,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    carrier: Option<GitBranchCarrierV1>,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    provenance: Option<ExportProvenanceV1>,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    producer: Option<OutputProducer>,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    presentation: Option<ExportPresentationV1>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnavailableExportWire {
    state: String,
    reason: ExportUnavailableReasonV1,
    #[serde(default, deserialize_with = "deserialize_non_null_option")]
    presentation: Option<ExportPresentationV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum ExportUnavailableReasonV1 {
    #[serde(rename = "source_failed")]
    Failed,
    #[serde(rename = "source_blocked")]
    Blocked,
    #[serde(rename = "source_input_unavailable")]
    InputUnavailable,
    #[serde(rename = "source_skipped")]
    Skipped,
    #[serde(rename = "source_not_run")]
    NotRun,
    #[serde(rename = "source_trigger_not_selected")]
    TriggerNotSelected,
    #[serde(rename = "source_cancelled")]
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PublicationBoundary {
    StagingCreated,
    BeforeExportMaterialization { export: String },
    AfterExportMaterialization { export: String },
    BeforeSerialization,
    StagingComplete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StagedFile {
    Export { export: String },
    Result,
}

trait PublicationObserver {
    fn observe(&mut self, _boundary: &PublicationBoundary) -> Result<(), ()> {
        Ok(())
    }

    fn close_staged_file(&mut self, file: File, _staged_file: &StagedFile) -> io::Result<()> {
        close_file(file)
    }

    fn sync_committed_directory(&mut self, directory: &OwnedFd) -> io::Result<()> {
        sync_directory(directory)
    }
}

struct NoopPublicationObserver;

impl PublicationObserver for NoopPublicationObserver {}

pub struct PreparedResultDestination {
    target: PublicationTarget,
}

#[cfg(test)]
pub(crate) fn prepare_result_destination(
    destination: &Path,
) -> Result<PreparedResultDestination, LocalPublicationError> {
    PublicationTarget::validate(destination, None, None)
        .map(|target| PreparedResultDestination { target })
}

pub fn prepare_attempt_result_destination(
    destination: &Path,
    private_staging: &Path,
    expected_result_parent: &OwnedFd,
    expected_staging_parent: &OwnedFd,
) -> Result<PreparedResultDestination, LocalPublicationError> {
    PublicationTarget::validate(
        destination,
        Some(private_staging),
        Some((expected_result_parent, expected_staging_parent)),
    )
    .map(|target| PreparedResultDestination { target })
}

pub fn publish_prepared_workflow_result(
    destination: &PreparedResultDestination,
    artifacts: &ArtifactStaging,
    run: &WorkflowRunResult,
) -> Result<WorkflowRunTerminalResultV1, LocalPublicationError> {
    publish_prepared_with_observer(destination, artifacts, run, &mut NoopPublicationObserver)
}

#[cfg(test)]
pub(crate) fn publish_workflow_result(
    destination: &Path,
    artifacts: &ArtifactStaging,
    run: &WorkflowRunResult,
) -> Result<WorkflowRunTerminalResultV1, LocalPublicationError> {
    let destination = prepare_result_destination(destination)?;
    publish_prepared_workflow_result(&destination, artifacts, run)
}

pub fn prepare_cloud_workflow_result(
    run: &WorkflowRunResult,
    project_id: String,
    repository_connection_id: String,
    object_format: String,
    commit_oid: String,
    source_display_snapshot: Option<CloudSourceDisplaySnapshotV1>,
) -> Result<PreparedCloudWorkflowResult, LocalPublicationError> {
    let mut carriers = Vec::new();
    let exports = project_exports(run, |_name, ordinal, source, output, metadata| {
        if let Some(carrier) = cloud_available_export(ordinal, source, output, metadata)? {
            carriers.push(carrier);
        }
        Ok(())
    })?;
    let result = build_result_with_provenance(
        run,
        WorkflowProvenanceV1::Cloud {
            project_id,
            repository_connection_id,
            object_format,
            commit_oid,
            source_display_snapshot,
        },
        exports,
    )?;
    let mut file = tempfile::NamedTempFile::new().map_err(|_| serialization_unavailable())?;
    write_result_json(&result, run.maximum_result_bytes, &mut file)?;
    let size = file
        .as_file()
        .metadata()
        .map_err(|_| serialization_unavailable())?
        .len();
    let mut digest = DigestContext::new(&SHA256);
    let mut reader = file.reopen().map_err(|_| serialization_unavailable())?;
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = reader
            .read(&mut buffer)
            .map_err(|_| serialization_unavailable())?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    let sha256 = lowercase_hex(digest.finish().as_ref());
    if size <= 16 * 1024 * 1024 {
        let mut result_json =
            Vec::with_capacity(usize::try_from(size).map_err(|_| serialization_unavailable())?);
        file.reopen()
            .map_err(|_| serialization_unavailable())?
            .read_to_end(&mut result_json)
            .map_err(|_| serialization_unavailable())?;
        Ok(PreparedCloudWorkflowResult {
            result_json: Arc::from(result_json),
            result_file: None,
            result_size_bytes: size,
            result_sha256: sha256,
            carriers,
        })
    } else {
        Ok(PreparedCloudWorkflowResult {
            result_json: Arc::from([]),
            result_file: Some(Arc::new(file)),
            result_size_bytes: size,
            result_sha256: sha256,
            carriers,
        })
    }
}

fn serialization_unavailable() -> LocalPublicationError {
    LocalPublicationError::new(
        LocalPublicationPhase::Serialization,
        LocalPublicationFailureKind::SerializationUnavailable,
    )
}

struct ResultJsonWriter<'a, W> {
    output: &'a mut W,
    written: u64,
    limit: u64,
}

impl<W: Write> Write for ResultJsonWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let size = u64::try_from(bytes.len()).map_err(io::Error::other)?;
        if self
            .written
            .checked_add(size)
            .is_none_or(|total| total > self.limit)
        {
            return Err(io::Error::other("portable result capacity exceeded"));
        }
        self.output.write_all(bytes)?;
        self.written += size;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

fn write_result_json(
    result: &WorkflowResultV1,
    maximum_result_bytes: u64,
    output: &mut impl Write,
) -> Result<(), LocalPublicationError> {
    result_metadata::validate_with_invariant(result).map_err(invalid_run_result)?;
    let decorated = result
        .exports
        .values()
        .any(|export| export.presentation().is_some());
    let mut buffered = std::io::BufWriter::with_capacity(64 * 1024, output);
    let mut writer = ResultJsonWriter {
        output: &mut buffered,
        written: 0,
        limit: maximum_result_bytes.min(result_metadata::MAXIMUM_RESULT_JSON_BYTES),
    };
    if decorated {
        serde_json::to_writer(&mut writer, result)
    } else {
        serde_json::to_writer_pretty(&mut writer, result)
    }
    .map_err(|_| serialization_unavailable())?;
    writer
        .write_all(b"\n")
        .and_then(|()| writer.flush())
        .map_err(|_| serialization_unavailable())
}

// This projection is shared by local materialization and cloud handoff. Neither
// destination is allowed to reinterpret the kind, digest or carrier presence.
fn export_metadata(
    name: &str,
    ordinal: usize,
    output: &CapturedValue,
) -> Result<ExportV1, LocalPublicationError> {
    let path = format!("{EXPORT_DIRECTORY}/{ordinal:04}");
    let available =
        |kind: &str, media_type: &str, size_bytes: u64, sha256: String| ExportV1::Available {
            kind: kind.to_owned(),
            media_type: media_type.to_owned(),
            path: path.clone(),
            size_bytes,
            digest: DigestV1 {
                algorithm: "sha256".to_owned(),
                value: sha256,
            },
            provenance: None,
            producer: None,
            presentation: None,
        };
    Ok(match output {
        CapturedValue::File(file) => available(
            "file",
            file.media_type(),
            file.size(),
            file.sha256().to_owned(),
        ),
        CapturedValue::Text(text) => {
            let bytes = text.carrier();
            available(
                "text",
                "text/plain; charset=utf-8",
                semantic_export_size(bytes, name)?,
                lowercase_hex(ring::digest::digest(&SHA256, bytes).as_ref()),
            )
        }
        CapturedValue::Json(value) => {
            let bytes = value.carrier();
            available(
                "json",
                "application/json",
                semantic_export_size(bytes, name)?,
                lowercase_hex(ring::digest::digest(&SHA256, bytes).as_ref()),
            )
        }
        CapturedValue::GitBranch(branch) => {
            let metadata = branch.metadata();
            if (metadata.base_oid() != metadata.head_oid()) != branch.carrier().is_some() {
                return Err(invalid_run_result(RunResultInvariant::GitBranch));
            }
            ExportV1::GitBranch {
                artifact_version: metadata.artifact_version(),
                object_format: metadata.object_format().as_str().to_owned(),
                base_oid: metadata.base_oid().to_owned(),
                head_oid: metadata.head_oid().to_owned(),
                tree_oid: metadata.tree_oid().to_owned(),
                carrier: branch.carrier().map(|carrier| GitBranchCarrierV1 {
                    path,
                    media_type: carrier.media_type().to_owned(),
                    size_bytes: carrier.size(),
                    digest: DigestV1 {
                        algorithm: "sha256".to_owned(),
                        value: carrier.sha256().to_owned(),
                    },
                }),
                provenance: None,
                producer: None,
                presentation: None,
            }
        }
    })
}

fn cloud_available_export(
    ordinal: usize,
    source: &ResolvedOutputSource,
    output: &CapturedValue,
    metadata: &ExportV1,
) -> Result<Option<CloudResultCarrier>, LocalPublicationError> {
    let body = match output {
        CapturedValue::File(file) => Some(CloudCarrierBody::Staged(file.carrier().clone())),
        CapturedValue::Text(text) => Some(CloudCarrierBody::Bytes(Arc::from(text.carrier()))),
        CapturedValue::Json(value) => Some(CloudCarrierBody::Bytes(Arc::from(value.carrier()))),
        CapturedValue::GitBranch(branch) => branch
            .carrier()
            .map(|carrier| CloudCarrierBody::Staged(carrier.staged().clone())),
    };
    let carrier = body
        .map(|body| {
            let (media_type, size_bytes, sha256) = match &metadata {
                ExportV1::Available {
                    media_type,
                    size_bytes,
                    digest,
                    ..
                } => (media_type.clone(), *size_bytes, digest.value.clone()),
                ExportV1::GitBranch {
                    carrier: Some(carrier),
                    ..
                } => (
                    carrier.media_type.clone(),
                    carrier.size_bytes,
                    carrier.digest.value.clone(),
                ),
                ExportV1::GitBranch { carrier: None, .. } | ExportV1::Unavailable { .. } => {
                    return Err(invalid_run_result(RunResultInvariant::GitBranch));
                }
            };
            Ok(CloudResultCarrier {
                portable_owner_path: format!("{EXPORT_DIRECTORY}/{ordinal:04}"),
                idempotency_key: format!("capture:{}:{}", source.node.id, source.output),
                media_type,
                size_bytes,
                sha256,
                body,
            })
        })
        .transpose()?;
    Ok(carrier)
}

#[cfg(test)]
fn publish_with_observer(
    destination: &Path,
    artifacts: &ArtifactStaging,
    run: &WorkflowRunResult,
    observer: &mut impl PublicationObserver,
) -> Result<WorkflowRunTerminalResultV1, LocalPublicationError> {
    let destination = prepare_result_destination(destination)?;
    publish_prepared_with_observer(&destination, artifacts, run, observer)
}

fn publish_prepared_with_observer(
    destination: &PreparedResultDestination,
    artifacts: &ArtifactStaging,
    run: &WorkflowRunResult,
    observer: &mut impl PublicationObserver,
) -> Result<WorkflowRunTerminalResultV1, LocalPublicationError> {
    let target = &destination.target;
    let mut staging = StagingDirectory::create(target)?;
    observe(
        observer,
        &PublicationBoundary::StagingCreated,
        LocalPublicationPhase::Staging,
        LocalPublicationFailureKind::StagingUnavailable,
        None,
    )?;

    let exports = project_exports(run, |name, ordinal, _source, output, _metadata| {
        materialize_available_export(&mut staging, observer, artifacts, name, ordinal, output)
    })?;

    observe(
        observer,
        &PublicationBoundary::BeforeSerialization,
        LocalPublicationPhase::Serialization,
        LocalPublicationFailureKind::SerializationUnavailable,
        None,
    )?;
    let result = build_result(run, exports)?;
    let result_file = staging.write_result_streamed(&result, run.maximum_result_bytes)?;
    observer
        .close_staged_file(result_file, &StagedFile::Result)
        .map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Close,
                LocalPublicationFailureKind::SerializationUnavailable,
            )
        })?;
    observe(
        observer,
        &PublicationBoundary::StagingComplete,
        LocalPublicationPhase::Commit,
        LocalPublicationFailureKind::AtomicPublicationUnavailable,
        None,
    )?;
    target.verify_parent()?;
    staging.verify(&result).map_err(|_| {
        LocalPublicationError::new(
            LocalPublicationPhase::Verification,
            LocalPublicationFailureKind::VerificationUnavailable,
        )
    })?;
    match target.existing_publication(&result, &staging.root)? {
        ExistingPublication::Absent => {
            if let Err(error) = staging.commit(target, observer)
                && (error.kind() != LocalPublicationFailureKind::DestinationExists
                    || target.existing_publication(&result, &staging.root)?
                        != ExistingPublication::Identical)
            {
                return Err(
                    if error.kind() == LocalPublicationFailureKind::DestinationExists {
                        result_conflict()
                    } else {
                        error
                    },
                );
            }
        }
        ExistingPublication::Identical => {
            observer
                .sync_committed_directory(&target.parent)
                .map_err(|_| {
                    LocalPublicationError::new(
                        LocalPublicationPhase::Commit,
                        LocalPublicationFailureKind::CommittedDurabilityUnavailable,
                    )
                })?;
        }
        ExistingPublication::Conflict => return Err(result_conflict()),
    }
    drop(staging);

    let outcome = result.outcome;
    let execution_outcome = execution_outcome(run, outcome);
    let terminal = WorkflowRunTerminalResultV1 {
        schema_version: 1,
        command: COMMAND,
        outcome,
        exit_status: exit_status(execution_outcome),
        execution_outcome,
        run_directory: retained_path(&run.run_directory)?,
        attempt_number: run.attempt_number,
        result_directory: target.normalized.clone(),
        result,
    };
    Ok(terminal)
}

// Shared alias resolution, declaration checks and export projection. The
// destination callback only materializes the first carrier for each source.
fn project_exports(
    run: &WorkflowRunResult,
    mut materialize: impl FnMut(
        &str,
        usize,
        &ResolvedOutputSource,
        &CapturedValue,
        &ExportV1,
    ) -> Result<(), LocalPublicationError>,
) -> Result<BTreeMap<String, ExportV1>, LocalPublicationError> {
    validate_export_source_set(run)?;
    let mut exports = BTreeMap::new();
    let mut sources = BTreeMap::<(String, String), SourcePublication>::new();
    for (index, (name, export)) in run.exports.iter().enumerate() {
        let (ordinal, source) = checked_export_source(run, index, name)?;
        let identity = (source.node.id.clone(), source.output.clone());
        let metadata = match export {
            ExportValue::Unavailable { reason } => {
                unavailable_export(&mut sources, identity, source, *reason)?
            }
            ExportValue::Available { output } => {
                if !captured_type_matches(source.value_type, output) {
                    return Err(invalid_run_result(RunResultInvariant::ExportValues));
                }
                match existing_available_export(&sources, &identity, source, output)? {
                    Some(metadata) => metadata,
                    None => {
                        let metadata = export_metadata(name, ordinal, output)?;
                        materialize(name, ordinal, source, output, &metadata)?;
                        insert_available_source(&mut sources, identity, source, output, &metadata);
                        metadata
                    }
                }
            }
        };
        exports.insert(name.clone(), metadata);
    }
    Ok(exports)
}

enum SourcePublication {
    Available {
        source: ResolvedOutputSource,
        output: CapturedValue,
        metadata: Box<ExportV1>,
    },
    Unavailable {
        source: ResolvedOutputSource,
        reason: ExportUnavailableReason,
    },
}

fn insert_available_source(
    sources: &mut BTreeMap<(String, String), SourcePublication>,
    identity: (String, String),
    source: &ResolvedOutputSource,
    output: &CapturedValue,
    metadata: &ExportV1,
) {
    sources.insert(
        identity,
        SourcePublication::Available {
            source: source.clone(),
            output: output.clone(),
            metadata: Box::new(metadata.clone()),
        },
    );
}

fn validate_export_source_set(run: &WorkflowRunResult) -> Result<(), LocalPublicationError> {
    if run.exports.keys().eq(run.export_sources.keys()) {
        Ok(())
    } else {
        Err(invalid_run_result(RunResultInvariant::ExportSources))
    }
}

fn checked_export_source<'a>(
    run: &'a WorkflowRunResult,
    index: usize,
    name: &str,
) -> Result<(usize, &'a ResolvedOutputSource), LocalPublicationError> {
    let ordinal = index
        .checked_add(1)
        .ok_or_else(|| invalid_run_result(RunResultInvariant::ExportSources))?;
    let source = run
        .export_sources
        .get(name)
        .ok_or_else(|| invalid_run_result(RunResultInvariant::ExportSources))?;
    Ok((ordinal, source))
}

fn existing_available_export(
    sources: &BTreeMap<(String, String), SourcePublication>,
    identity: &(String, String),
    source: &ResolvedOutputSource,
    output: &CapturedValue,
) -> Result<Option<ExportV1>, LocalPublicationError> {
    let Some(publication) = sources.get(identity) else {
        return Ok(None);
    };
    let SourcePublication::Available {
        source: owner_source,
        output: owner_output,
        metadata,
    } = publication
    else {
        return Err(invalid_run_result(RunResultInvariant::ExportValues));
    };
    if owner_source != source || owner_output != output {
        return Err(invalid_run_result(RunResultInvariant::ExportValues));
    }
    Ok(Some(metadata.as_ref().clone()))
}

fn unavailable_export(
    sources: &mut BTreeMap<(String, String), SourcePublication>,
    identity: (String, String),
    source: &ResolvedOutputSource,
    reason: ExportUnavailableReason,
) -> Result<ExportV1, LocalPublicationError> {
    match sources.get(&identity) {
        None => {
            sources.insert(
                identity,
                SourcePublication::Unavailable {
                    source: source.clone(),
                    reason,
                },
            );
        }
        Some(SourcePublication::Unavailable {
            source: owner_source,
            reason: owner_reason,
        }) if owner_source == source && *owner_reason == reason => {}
        Some(_) => return Err(invalid_run_result(RunResultInvariant::ExportValues)),
    }
    Ok(ExportV1::Unavailable {
        reason: export_unavailable_reason(reason),
        presentation: None,
    })
}

fn captured_type_matches(value_type: WorkflowValueType, output: &CapturedValue) -> bool {
    value_type == output.value_type()
}

fn materialize_available_export(
    staging: &mut StagingDirectory<'_>,
    observer: &mut impl PublicationObserver,
    artifacts: &ArtifactStaging,
    name: &str,
    ordinal: usize,
    output: &CapturedValue,
) -> Result<(), LocalPublicationError> {
    if let CapturedValue::GitBranch(branch) = output {
        return write_git_branch_export(staging, observer, artifacts, name, ordinal, branch);
    }
    let file_name = format!("{ordinal:04}");
    if let CapturedValue::File(file) = output {
        return expose_available_carrier(
            staging,
            observer,
            artifacts,
            name,
            &file_name,
            file.carrier(),
        );
    }

    if let Some(carrier) = output
        .private_capture_carrier()
        .filter(|carrier| staged_carrier_matches_semantic_bytes(carrier, output))
    {
        return expose_available_carrier(staging, observer, artifacts, name, &file_name, carrier);
    }

    observe(
        observer,
        &PublicationBoundary::BeforeExportMaterialization {
            export: name.to_owned(),
        },
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::ExportWriteUnavailable,
        Some(name),
    )?;
    let expected_size = match output {
        CapturedValue::Text(text) => semantic_export_size(text.carrier(), name)?,
        CapturedValue::Json(value) => semantic_export_size(value.carrier(), name)?,
        CapturedValue::File(_) | CapturedValue::GitBranch(_) => {
            return Err(unsupported_export_error(name));
        }
    };
    let mut destination = staging.create_export(&file_name).map_err(|kind| {
        LocalPublicationError::for_export(LocalPublicationPhase::ExportCopy, kind, name)
    })?;
    let bytes = match output {
        CapturedValue::Text(text) => text.carrier(),
        CapturedValue::Json(value) => value.carrier(),
        CapturedValue::File(_) | CapturedValue::GitBranch(_) => {
            return Err(unsupported_export_error(name));
        }
    };
    destination
        .write_all(bytes)
        .map_err(|_| export_write_error(name))?;
    let written = u64::try_from(bytes.len()).map_err(|_| export_write_error(name))?;
    destination.flush().map_err(|_| export_write_error(name))?;
    if written != expected_size {
        return Err(export_write_error(name));
    }
    observer
        .close_staged_file(
            destination,
            &StagedFile::Export {
                export: name.to_owned(),
            },
        )
        .map_err(|_| {
            LocalPublicationError::for_export(
                LocalPublicationPhase::Close,
                LocalPublicationFailureKind::ExportWriteUnavailable,
                name,
            )
        })?;
    observe(
        observer,
        &PublicationBoundary::AfterExportMaterialization {
            export: name.to_owned(),
        },
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::ExportWriteUnavailable,
        Some(name),
    )?;
    Ok(())
}

fn write_git_branch_export(
    staging: &mut StagingDirectory<'_>,
    observer: &mut impl PublicationObserver,
    artifacts: &ArtifactStaging,
    name: &str,
    ordinal: usize,
    branch: &super::artifact::CapturedGitBranch,
) -> Result<(), LocalPublicationError> {
    let metadata = branch.metadata();
    let has_delta = metadata.base_oid() != metadata.head_oid();
    if has_delta != branch.carrier().is_some() {
        return Err(invalid_run_result(RunResultInvariant::GitBranch));
    }
    if let Some(carrier) = branch.carrier() {
        let file_name = format!("{ordinal:04}");
        expose_staged_carrier(
            staging,
            observer,
            artifacts,
            name,
            &file_name,
            carrier.staged(),
            branch.output_identity(),
        )?;
    }
    Ok(())
}

fn observe(
    observer: &mut impl PublicationObserver,
    boundary: &PublicationBoundary,
    phase: LocalPublicationPhase,
    kind: LocalPublicationFailureKind,
    export: Option<&str>,
) -> Result<(), LocalPublicationError> {
    observer.observe(boundary).map_err(|()| {
        export.map_or_else(
            || LocalPublicationError::new(phase, kind),
            |export| LocalPublicationError::for_export(phase, kind, export),
        )
    })
}

fn expose_staged_carrier(
    staging: &mut StagingDirectory<'_>,
    observer: &mut impl PublicationObserver,
    artifacts: &ArtifactStaging,
    export: &str,
    file_name: &str,
    carrier: &StagedCarrier,
    expected_output_identity: &str,
) -> Result<(), LocalPublicationError> {
    observe(
        observer,
        &PublicationBoundary::BeforeExportMaterialization {
            export: export.to_owned(),
        },
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::CarrierHandoffUnavailable,
        Some(export),
    )?;
    staging
        .expose_export(artifacts, carrier, expected_output_identity, file_name)
        .map_err(|failure| carrier_handoff_error(export, failure))?;
    observe(
        observer,
        &PublicationBoundary::AfterExportMaterialization {
            export: export.to_owned(),
        },
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::CarrierHandoffUnavailable,
        Some(export),
    )
}

fn expose_available_carrier(
    staging: &mut StagingDirectory<'_>,
    observer: &mut impl PublicationObserver,
    artifacts: &ArtifactStaging,
    export: &str,
    file_name: &str,
    carrier: &StagedCarrier,
) -> Result<(), LocalPublicationError> {
    expose_staged_carrier(
        staging,
        observer,
        artifacts,
        export,
        file_name,
        carrier,
        carrier.output_identity(),
    )
}

fn staged_carrier_matches_semantic_bytes(carrier: &StagedCarrier, output: &CapturedValue) -> bool {
    let bytes = match output {
        CapturedValue::Text(text) => text.carrier(),
        CapturedValue::Json(json) => json.carrier(),
        CapturedValue::File(_) | CapturedValue::GitBranch(_) => return false,
    };
    let Ok(size) = u64::try_from(bytes.len()) else {
        return false;
    };
    let mut digest = DigestContext::new(&SHA256);
    digest.update(bytes);
    carrier.size() == size && carrier.sha256() == lowercase_hex(digest.finish().as_ref())
}

fn semantic_export_size(carrier: &[u8], export: &str) -> Result<u64, LocalPublicationError> {
    u64::try_from(carrier.len()).map_err(|_| {
        LocalPublicationError::for_export(
            LocalPublicationPhase::ExportCopy,
            LocalPublicationFailureKind::UnsupportedExport,
            export,
        )
    })
}

fn unsupported_export_error(export: &str) -> LocalPublicationError {
    LocalPublicationError::for_export(
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::UnsupportedExport,
        export,
    )
}

fn export_write_error(export: &str) -> LocalPublicationError {
    LocalPublicationError::for_export(
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::ExportWriteUnavailable,
        export,
    )
}

fn carrier_handoff_error(export: &str, _failure: ArtifactExposeFailure) -> LocalPublicationError {
    LocalPublicationError::for_export(
        LocalPublicationPhase::ExportCopy,
        LocalPublicationFailureKind::CarrierHandoffUnavailable,
        export,
    )
}

fn resolve_presentation_field(
    run: &WorkflowRunResult,
    field: Field,
    source: &ValidatedPresentationField,
) -> Result<PresentationFieldV1, LocalPublicationError> {
    let unavailable = |reason| PresentationFieldV1::Unavailable { reason };
    let text = match source {
        ValidatedPresentationField::Literal(value) => value.as_str(),
        ValidatedPresentationField::Reference(source) => {
            let step = match source.node.role {
                WorkflowNodeRole::Step => run.steps.iter().find(|step| step.id == source.node.id),
                WorkflowNodeRole::Finalizer => run.finalization.as_ref().and_then(|finalization| {
                    finalization
                        .finalizers
                        .iter()
                        .find(|step| step.id == source.node.id)
                }),
            }
            .ok_or_else(|| invalid_run_result(RunResultInvariant::ExportSources))?;
            match &step.state {
                StepState::Succeeded { outputs }
                | StepState::Inherited {
                    outputs,
                    disposition: super::runtime::InheritedDisposition::Succeeded,
                    ..
                } => match outputs.get(&source.output) {
                    Some(CapturedValue::Text(text)) => text.as_str(),
                    _ => return Err(invalid_run_result(RunResultInvariant::ExportSources)),
                },
                StepState::Failed { .. } => {
                    return Ok(unavailable(PresentationUnavailableReasonV1::SourceFailed));
                }
                StepState::Blocked { .. } if source.node.role == WorkflowNodeRole::Finalizer => {
                    return Ok(unavailable(
                        PresentationUnavailableReasonV1::SourceInputUnavailable,
                    ));
                }
                StepState::Blocked { .. } => {
                    return Ok(unavailable(PresentationUnavailableReasonV1::SourceBlocked));
                }
                StepState::Skipped { .. } | StepState::Inherited { .. } => {
                    return Ok(unavailable(PresentationUnavailableReasonV1::SourceNotRun));
                }
                StepState::NotRun { .. } if source.node.role == WorkflowNodeRole::Finalizer => {
                    return Ok(unavailable(
                        PresentationUnavailableReasonV1::SourceTriggerNotSelected,
                    ));
                }
                StepState::NotRun { .. } => {
                    return Ok(unavailable(PresentationUnavailableReasonV1::SourceNotRun));
                }
                StepState::Cancelled { .. } => {
                    return Ok(unavailable(
                        PresentationUnavailableReasonV1::SourceCancelled,
                    ));
                }
                _ => return Err(invalid_run_result(RunResultInvariant::ExportSources)),
            }
        }
    };
    match resolve_text(field, text) {
        Ok(value) => Ok(PresentationFieldV1::Available {
            value: value.to_owned(),
        }),
        Err(_) if matches!(source, ValidatedPresentationField::Literal(_)) => {
            Err(invalid_run_result(RunResultInvariant::ExportSources))
        }
        Err(reason) => Ok(unavailable(match reason {
            ContentReason::Blank => PresentationUnavailableReasonV1::Blank,
            ContentReason::ControlCharacter => PresentationUnavailableReasonV1::ControlCharacter,
            ContentReason::TooLarge => PresentationUnavailableReasonV1::TooLarge,
            ContentReason::MultilineTitle => PresentationUnavailableReasonV1::MultilineTitle,
        })),
    }
}

fn resolve_export_presentation(
    run: &WorkflowRunResult,
    presentation: &ValidatedExportPresentation,
) -> Result<ExportPresentationV1, LocalPublicationError> {
    let title = presentation
        .title
        .as_ref()
        .map(|source| resolve_presentation_field(run, Field::Title, source))
        .transpose()?;
    let description = presentation
        .description
        .as_ref()
        .map(|source| resolve_presentation_field(run, Field::Description, source))
        .transpose()?;
    if title.is_none() && description.is_none() {
        return Err(invalid_run_result(RunResultInvariant::ExportSources));
    }
    Ok(ExportPresentationV1 { title, description })
}

fn build_result(
    run: &WorkflowRunResult,
    exports: BTreeMap<String, ExportV1>,
) -> Result<WorkflowResultV1, LocalPublicationError> {
    let source_root = retained_path(&run.source_root)?;
    let execution_root = retained_path(&run.execution_root)?;
    build_result_with_provenance(run, WorkflowProvenanceV1::Local { source_root }, exports).map(
        |mut result| {
            result.execution.execution_root = Some(execution_root);
            result
        },
    )
}

fn build_result_with_provenance(
    run: &WorkflowRunResult,
    provenance: WorkflowProvenanceV1,
    mut exports: BTreeMap<String, ExportV1>,
) -> Result<WorkflowResultV1, LocalPublicationError> {
    let (outcome, primary_issue) = match &run.outcome {
        RunOutcome::Succeeded => (WorkflowOutcomeV1::Succeeded, None),
        RunOutcome::Failed { primary_issue, .. } => {
            (WorkflowOutcomeV1::Failed, Some(primary_issue.clone()))
        }
        RunOutcome::Cancelled { .. } => (WorkflowOutcomeV1::Cancelled, None),
    };
    let cancellation = run
        .cancellation
        .as_ref()
        .map(|cancellation| {
            Ok(CancellationV1 {
                reason: cancellation_reason(cancellation.reason),
                force_stop_deadline: timestamp(cancellation.force_stop_deadline)?,
            })
        })
        .transpose()?;
    let force_abort = run.force_abort.map(|force_abort| ForceAbortV1 {
        reason: cancellation_reason(force_abort.reason),
        phase: match force_abort.phase {
            super::runtime::RunCancellationPhase::Ordinary => ForceAbortPhaseV1::Ordinary,
            super::runtime::RunCancellationPhase::Finalization => ForceAbortPhaseV1::Finalization,
        },
    });
    let steps = run
        .steps
        .iter()
        .map(step_v1)
        .collect::<Result<Vec<_>, _>>()?;
    let finalization = run.finalization.as_ref().map(finalization_v1).transpose()?;
    let export_sources = run
        .continuation
        .as_ref()
        .map(|_| {
            run.export_sources
                .iter()
                .map(|(name, source)| {
                    (
                        name.clone(),
                        ExportSourceV1 {
                            node: WorkflowNodeV1 {
                                id: source.node.id.clone(),
                                role: workflow_node_role(source.node.role),
                            },
                            output: source.output.clone(),
                        },
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    if run
        .export_presentation
        .keys()
        .any(|name| !exports.contains_key(name))
    {
        return Err(invalid_run_result(RunResultInvariant::ExportSources));
    }
    for (name, export) in &mut exports {
        if let Some(presentation) = run.export_presentation.get(name) {
            export.set_presentation(resolve_export_presentation(run, presentation)?);
        }
        let Some(source) = run.export_sources.get(name) else {
            return Err(invalid_run_result(RunResultInvariant::ExportSources));
        };
        if let Some(producer) = run
            .output_producers
            .get(&source.node.id)
            .and_then(|outputs| outputs.get(&source.output))
        {
            match export {
                ExportV1::Available {
                    provenance: export_provenance,
                    producer: export_producer,
                    ..
                }
                | ExportV1::GitBranch {
                    provenance: export_provenance,
                    producer: export_producer,
                    ..
                } => {
                    *export_provenance = Some(ExportProvenanceV1::Inherited);
                    *export_producer = Some(producer.clone());
                }
                ExportV1::Unavailable { .. } => {
                    return Err(invalid_run_result(RunResultInvariant::ExportValues));
                }
            }
        }
    }

    if run.attempt_number == 0 {
        return Err(invalid_run_result(RunResultInvariant::AttemptMetadata));
    }
    Ok(WorkflowResultV1 {
        schema_version: 1,
        attempt_number: run.attempt_number,
        continuation: run.continuation.clone(),
        output_producers: run.output_producers.clone(),
        export_sources,
        workflow: WorkflowIdentityV1 {
            path: run.workflow_path.clone(),
            provenance,
            digest: DigestV1 {
                algorithm: run.content_digest.algorithm.as_str().to_owned(),
                value: run.content_digest.value.clone(),
            },
        },
        execution: WorkflowExecutionV1 {
            execution_root: None,
            maximum_parallel_steps: run.maximum_parallel_steps.get(),
            capacity: run.cloud_capacity.clone(),
            started_at: timestamp(run.timing.started_at)?,
            finished_at: timestamp(run.timing.finished_at)?,
            duration_milliseconds: duration_milliseconds(run.timing.duration)?,
        },
        command_output_policy: CommandOutputPolicyV1 {
            encoding: "base64".to_owned(),
            maximum_retained_bytes_per_stream: run.maximum_retained_bytes_per_stream,
        },
        outcome,
        primary_issue,
        cancellation,
        force_abort,
        steps,
        finalization,
        exports,
    })
}

fn finalization_v1(
    finalization: &WorkflowRunFinalization,
) -> Result<FinalizationV1, LocalPublicationError> {
    let finalizers = finalization
        .finalizers
        .iter()
        .map(step_v1)
        .collect::<Result<Vec<_>, _>>()?;
    if finalizers
        .iter()
        .any(|finalizer| finalizer.role != WorkflowNodeRoleV1::Finalizer)
    {
        return Err(invalid_run_result(RunResultInvariant::FinalizationMetadata));
    }
    let issues = finalizers
        .iter()
        .filter(|finalizer| {
            matches!(
                finalizer.state,
                WorkflowStepStateV1::Failed | WorkflowStepStateV1::Blocked
            )
        })
        .map(|finalizer| FinalizationIssueV1 {
            node: WorkflowNodeV1 {
                id: finalizer.id.clone(),
                role: WorkflowNodeRoleV1::Finalizer,
            },
            impact: finalizer.failure_policy,
        })
        .collect();
    let cancellation = finalization
        .cancellation
        .as_ref()
        .map(|cancellation| {
            Ok(FinalizationCancellationV1 {
                reason: cancellation_reason(cancellation.reason),
                force_stop_deadline: cancellation
                    .force_stop_deadline
                    .map(timestamp)
                    .transpose()?,
            })
        })
        .transpose()?;
    Ok(FinalizationV1 {
        trigger: finalization_trigger(finalization.trigger),
        finalizers,
        issues,
        cancellation,
        force_abort: finalization.force_abort,
    })
}

fn step_v1(step: &WorkflowRunStep) -> Result<WorkflowStepV1, LocalPublicationError> {
    let (started_at, duration_milliseconds) = match &step.timing {
        Some(timing) => (
            Some(timestamp(timing.started_at)?),
            Some(duration_milliseconds(timing.duration)?),
        ),
        None => (None, None),
    };
    let (state, detail) = match &step.state {
        StepState::Succeeded { .. } => (WorkflowStepStateV1::Succeeded, None),
        StepState::Inherited { detail, .. } => (
            WorkflowStepStateV1::Inherited,
            Some(NodeDetail::Inherited(detail.clone())),
        ),
        StepState::Failed { detail } => (
            WorkflowStepStateV1::Failed,
            Some(NodeDetail::Failed(detail.clone())),
        ),
        StepState::Blocked { detail } => (
            WorkflowStepStateV1::Blocked,
            Some(NodeDetail::Blocked(detail.clone())),
        ),
        StepState::Skipped { detail } => (
            WorkflowStepStateV1::Skipped,
            Some(NodeDetail::Skipped(detail.clone())),
        ),
        StepState::NotRun { detail } => (
            WorkflowStepStateV1::NotRun,
            Some(NodeDetail::NotRun(*detail)),
        ),
        StepState::Cancelled { detail } => (
            WorkflowStepStateV1::Cancelled,
            Some(NodeDetail::Cancellation(*detail)),
        ),
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Cancelling { .. } => {
            return Err(invalid_run_result(RunResultInvariant::StepMetadata));
        }
    };
    let command_output = step
        .command_output
        .as_ref()
        .map(command_output_v1)
        .transpose()?;

    if step.kind == WorkflowRunStepKind::Agent && command_output.is_some() {
        return Err(invalid_run_result(RunResultInvariant::StepMetadata));
    }

    Ok(WorkflowStepV1 {
        id: step.id.clone(),
        role: workflow_node_role(step.role),
        kind: match step.kind {
            WorkflowRunStepKind::Command => "cmd",
            WorkflowRunStepKind::Agent => "agent",
        }
        .to_owned(),
        failure_policy: step.failure_policy,
        state,
        recovery: step.recovery.clone(),
        invocations: step.invocations.clone(),
        started_at,
        duration_milliseconds,
        detail,
        command_output,
    })
}

pub fn command_output_v1(
    diagnostic: &StepDiagnostic,
) -> Result<CommandOutputV1, LocalPublicationError> {
    Ok(CommandOutputV1 {
        stdout: diagnostic_stream_v1(diagnostic.standard_output())?,
        stderr: diagnostic_stream_v1(diagnostic.standard_error())?,
    })
}

fn diagnostic_stream_v1(
    stream: &CapturedDiagnosticStream,
) -> Result<DiagnosticStreamV1, LocalPublicationError> {
    let retained_bytes = u64::try_from(stream.bytes().len())
        .map_err(|_| invalid_run_result(RunResultInvariant::DiagnosticStream))?;
    if retained_bytes > super::MAXIMUM_RETAINED_BYTES_PER_STREAM {
        return Err(invalid_run_result(RunResultInvariant::DiagnosticStream));
    }
    let discarded_bytes = stream
        .truncation()
        .map_or(0, |truncation| truncation.discarded_bytes());
    Ok(DiagnosticStreamV1 {
        encoding: "base64".to_owned(),
        data: BASE64_STANDARD.encode(stream.bytes()),
        retained_bytes,
        discarded_bytes,
        truncated: discarded_bytes != 0,
        fully_drained: stream.fully_drained(),
    })
}

pub fn step_recovery_summary_v1(
    recovery: Option<&StepRecoveryState<StepFailureCause>>,
) -> Result<Option<StepRecoverySummaryV1>, LocalPublicationError> {
    let Some(recovery) = recovery else {
        return Ok(None);
    };
    if recovery.rounds.is_empty() {
        return if recovery.terminal_disposition.is_none() {
            Ok(None)
        } else {
            Err(invalid_run_result(RunResultInvariant::Recovery))
        };
    }
    let rounds = recovery_round_summaries_v1(recovery, true)?;
    let termination = match recovery
        .terminal_disposition
        .ok_or_else(|| invalid_run_result(RunResultInvariant::Recovery))?
    {
        RecoveryTerminalDisposition::Recovered { execution_number } => {
            RecoveryTerminationV1::Recovered {
                execution_number: execution_number.get(),
            }
        }
        RecoveryTerminalDisposition::Exhausted { execution_number } => {
            RecoveryTerminationV1::Exhausted {
                execution_number: execution_number.get(),
            }
        }
        RecoveryTerminalDisposition::GaveUp { round } => {
            RecoveryTerminationV1::GaveUp { round: round.get() }
        }
        RecoveryTerminalDisposition::HandlerFailed { round, phase } => {
            let failure = recovery
                .rounds
                .iter()
                .find(|candidate| candidate.number == round)
                .and_then(|round| round.handler.as_ref())
                .and_then(|handler| match &handler.outcome {
                    RecoveryHandlerOutcome::Failed {
                        phase: retained_phase,
                        cause,
                    } if *retained_phase == phase => Some(cause),
                    _ => None,
                })
                .ok_or_else(|| invalid_run_result(RunResultInvariant::Recovery))?;
            RecoveryTerminationV1::HandlerFailed {
                round: round.get(),
                handler_failure: recovery_handler_failure_v1(phase, failure)?,
            }
        }
        RecoveryTerminalDisposition::Cancelled { round, active } => {
            let (active_role, execution_number) = match active {
                ActiveStepInvocation::Target { execution_number } => (
                    RecoveryInvocationRoleV1::Target,
                    Some(execution_number.get()),
                ),
                ActiveStepInvocation::RecoveryHandler { .. } => {
                    (RecoveryInvocationRoleV1::RecoveryHandler, None)
                }
            };
            RecoveryTerminationV1::Cancelled {
                round: round.get(),
                active_role,
                execution_number,
            }
        }
    };
    Ok(Some(StepRecoverySummaryV1 {
        schema_version: 1,
        configured_retries: recovery.configured_rounds,
        handler_kind: recovery.handler_kind.map(recovery_handler_kind_v1),
        rounds,
        termination,
    }))
}

pub(crate) fn recovery_round_summaries_v1(
    recovery: &StepRecoveryState<StepFailureCause>,
    require_settled_handlers: bool,
) -> Result<Vec<RecoveryRoundSummaryV1>, LocalPublicationError> {
    recovery
        .rounds
        .iter()
        .map(|round| {
            let handler = round
                .handler
                .as_ref()
                .map(|handler| {
                    let (outcome, summary, reason, failure) = match &handler.outcome {
                        RecoveryHandlerOutcome::Recheck { summary, reason } => (
                            RecoveryHandlerOutcomeV1::Recheck,
                            Some(summary.clone()),
                            Some(reason.clone()),
                            None,
                        ),
                        RecoveryHandlerOutcome::GaveUp { summary, reason } => (
                            RecoveryHandlerOutcomeV1::GaveUp,
                            Some(summary.clone()),
                            Some(reason.clone()),
                            None,
                        ),
                        RecoveryHandlerOutcome::Failed { phase, cause } => (
                            RecoveryHandlerOutcomeV1::Failed,
                            None,
                            None,
                            Some(recovery_handler_failure_v1(*phase, cause)?),
                        ),
                        RecoveryHandlerOutcome::Cancelled => {
                            (RecoveryHandlerOutcomeV1::Cancelled, None, None, None)
                        }
                        RecoveryHandlerOutcome::Starting | RecoveryHandlerOutcome::Running
                            if !require_settled_handlers =>
                        {
                            return Ok(None);
                        }
                        RecoveryHandlerOutcome::Starting | RecoveryHandlerOutcome::Running => {
                            return Err(invalid_run_result(RunResultInvariant::Recovery));
                        }
                    };
                    Ok(Some(RecoveryHandlerSummaryV1 {
                        kind: recovery_handler_kind_v1(handler.kind),
                        invocation_id: handler.invocation.transition_sequence.get(),
                        outcome,
                        summary,
                        reason,
                        failure,
                    }))
                })
                .transpose()?
                .flatten();
            Ok(RecoveryRoundSummaryV1 {
                number: round.number.get(),
                failed_execution: RecoveryFailedExecutionV1 {
                    execution_number: round.failed_execution.execution_number.get(),
                    invocation_id: round.failed_execution.invocation.transition_sequence.get(),
                    failure: failure_v1(
                        round.failed_execution.phase,
                        &round.failed_execution.cause,
                    )?,
                },
                handler,
            })
        })
        .collect()
}

pub(crate) fn recovery_handler_kind_v1(kind: RecoveryHandlerKind) -> RecoveryHandlerKindV1 {
    match kind {
        RecoveryHandlerKind::Command => RecoveryHandlerKindV1::Cmd,
        RecoveryHandlerKind::Agent => RecoveryHandlerKindV1::Agent,
    }
}

fn recovery_handler_failure_v1(
    phase: RecoveryHandlerFailurePhase,
    cause: &StepFailureCause,
) -> Result<RecoveryHandlerFailureV1, LocalPublicationError> {
    let StepFailureCause::RecoveryHandler(cause) = cause else {
        return Err(invalid_run_result(RunResultInvariant::Recovery));
    };
    let (code, exit_code, decision_rejection) = match cause {
        super::recovery::RecoveryHandlerFailure::ContextUnavailable => {
            (RecoveryHandlerFailureCodeV1::ContextUnavailable, None, None)
        }
        super::recovery::RecoveryHandlerFailure::HandlerUnavailable => {
            (RecoveryHandlerFailureCodeV1::HandlerUnavailable, None, None)
        }
        super::recovery::RecoveryHandlerFailure::WorkingDirectoryUnavailable => (
            RecoveryHandlerFailureCodeV1::WorkingDirectoryUnavailable,
            None,
            None,
        ),
        super::recovery::RecoveryHandlerFailure::CommandPreparationFailed => (
            RecoveryHandlerFailureCodeV1::CommandPreparationFailed,
            None,
            None,
        ),
        super::recovery::RecoveryHandlerFailure::CommandLaunchFailed => (
            RecoveryHandlerFailureCodeV1::CommandLaunchFailed,
            None,
            None,
        ),
        super::recovery::RecoveryHandlerFailure::CommandWaitFailed => {
            (RecoveryHandlerFailureCodeV1::CommandWaitFailed, None, None)
        }
        super::recovery::RecoveryHandlerFailure::CommandExitFailed { code } => {
            (RecoveryHandlerFailureCodeV1::CommandExitFailed, *code, None)
        }
        super::recovery::RecoveryHandlerFailure::ProcessQuiescenceFailed => (
            RecoveryHandlerFailureCodeV1::ProcessQuiescenceFailed,
            None,
            None,
        ),
        super::recovery::RecoveryHandlerFailure::ResultMissing => {
            (RecoveryHandlerFailureCodeV1::ResultMissing, None, None)
        }
        super::recovery::RecoveryHandlerFailure::ResultSymbolicLink => {
            (RecoveryHandlerFailureCodeV1::ResultSymbolicLink, None, None)
        }
        super::recovery::RecoveryHandlerFailure::ResultNotRegular => {
            (RecoveryHandlerFailureCodeV1::ResultNotRegular, None, None)
        }
        super::recovery::RecoveryHandlerFailure::ResultUnavailable => {
            (RecoveryHandlerFailureCodeV1::ResultUnavailable, None, None)
        }
        super::recovery::RecoveryHandlerFailure::ResultTooLarge => {
            (RecoveryHandlerFailureCodeV1::ResultTooLarge, None, None)
        }
        super::recovery::RecoveryHandlerFailure::DecisionInvalid(rejection) => (
            RecoveryHandlerFailureCodeV1::DecisionInvalid,
            None,
            Some(recovery_decision_rejection_v1(*rejection)),
        ),
        super::recovery::RecoveryHandlerFailure::AgentInputFailed => {
            (RecoveryHandlerFailureCodeV1::AgentInputFailed, None, None)
        }
        super::recovery::RecoveryHandlerFailure::AgentFailed => {
            (RecoveryHandlerFailureCodeV1::AgentFailed, None, None)
        }
        super::recovery::RecoveryHandlerFailure::AgentResultMissing => {
            (RecoveryHandlerFailureCodeV1::AgentResultMissing, None, None)
        }
        super::recovery::RecoveryHandlerFailure::AgentResultInvalid(rejection) => (
            RecoveryHandlerFailureCodeV1::AgentResultInvalid,
            None,
            Some(recovery_decision_rejection_v1(*rejection)),
        ),
        super::recovery::RecoveryHandlerFailure::SettlementFailed => {
            (RecoveryHandlerFailureCodeV1::SettlementFailed, None, None)
        }
    };
    Ok(RecoveryHandlerFailureV1 {
        phase: match phase {
            RecoveryHandlerFailurePhase::Start => RecoveryHandlerFailurePhaseV1::Start,
            RecoveryHandlerFailurePhase::Execution => RecoveryHandlerFailurePhaseV1::Execution,
        },
        cause: RecoveryHandlerFailureCauseV1 {
            code,
            exit_code,
            decision_rejection,
        },
    })
}

fn recovery_decision_rejection_v1(
    rejection: super::recovery::RecoveryDecisionFailureKind,
) -> RecoveryDecisionRejectionV1 {
    use super::recovery::RecoveryDecisionFailureKind as Source;
    match rejection {
        Source::InputTooLarge => RecoveryDecisionRejectionV1::InputTooLarge,
        Source::InvalidUtf8 => RecoveryDecisionRejectionV1::InvalidUtf8,
        Source::InvalidJson => RecoveryDecisionRejectionV1::InvalidJson,
        Source::DuplicateKey => RecoveryDecisionRejectionV1::DuplicateKey,
        Source::UnknownField => RecoveryDecisionRejectionV1::UnknownField,
        Source::UnsupportedSchemaVersion => RecoveryDecisionRejectionV1::UnsupportedSchemaVersion,
        Source::UnknownDecision => RecoveryDecisionRejectionV1::UnknownDecision,
        Source::EmptySummary => RecoveryDecisionRejectionV1::EmptySummary,
        Source::SummaryTooLong => RecoveryDecisionRejectionV1::SummaryTooLong,
        Source::EmptyReason => RecoveryDecisionRejectionV1::EmptyReason,
        Source::ReasonTooLong => RecoveryDecisionRejectionV1::ReasonTooLong,
    }
}

pub(super) fn failure_v1(
    phase: FailurePhase,
    cause: &StepFailureCause,
) -> Result<FailureV1, LocalPublicationError> {
    let (phase, cause) = match (phase, cause) {
        (FailurePhase::Start, StepFailureCause::Start(cause)) => {
            (FailurePhaseV1::Start, start_failure_cause(cause)?)
        }
        (FailurePhase::Execution, StepFailureCause::Execution(cause)) => {
            (FailurePhaseV1::Execution, execution_failure_cause(cause))
        }
        (FailurePhase::OutputCapture, StepFailureCause::OutputCapture(cause)) => (
            FailurePhaseV1::OutputCapture,
            output_capture_failure_cause(cause),
        ),
        _ => return Err(invalid_run_result(RunResultInvariant::Failure)),
    };
    Ok(FailureV1 { phase, cause })
}

fn start_failure_cause(
    failure: &StepStartFailure,
) -> Result<FailureCauseV1, LocalPublicationError> {
    let cause = match failure {
        StepStartFailure::StepUnavailable => FailureCauseV1::code(FailureCodeV1::StepUnavailable),
        StepStartFailure::PreparationTaskUnavailable => {
            FailureCauseV1::code(FailureCodeV1::PreparationTaskUnavailable)
        }
        StepStartFailure::InputsUnavailable => {
            FailureCauseV1::code(FailureCodeV1::InputsUnavailable)
        }
        StepStartFailure::InputPreparation(failure) => {
            let code = match failure.kind() {
                InputPreparationFailureKind::InvalidInputName => FailureCodeV1::InputInvalidName,
                InputPreparationFailureKind::ValueCountLimitExceeded => {
                    FailureCodeV1::InputValueCountLimit
                }
                InputPreparationFailureKind::ValueSizeLimitExceeded => {
                    FailureCodeV1::InputValueSizeLimit
                }
                InputPreparationFailureKind::TotalSizeLimitExceeded => {
                    FailureCodeV1::InputTotalSizeLimit
                }
                InputPreparationFailureKind::CollectionOrdinalLimitExceeded => {
                    FailureCodeV1::InputCollectionOrdinalLimit
                }
                InputPreparationFailureKind::ValueTypeMismatch => FailureCodeV1::InputTypeMismatch,
                InputPreparationFailureKind::SourceUnavailable => {
                    FailureCodeV1::InputSourceUnavailable
                }
                InputPreparationFailureKind::StagingUnavailable => {
                    FailureCodeV1::InputStagingUnavailable
                }
                InputPreparationFailureKind::LiveLimitExceeded => FailureCodeV1::InputLiveLimit,
            };
            let mut cause = FailureCauseV1::code(code);
            cause.input = failure.input_identity().map(str::to_owned);
            cause.collection_index = failure.collection_index();
            cause
        }
        StepStartFailure::AgentInput(failure) => agent_input_failure_cause(failure),
        StepStartFailure::AgentRuntimeUnavailable => {
            FailureCauseV1::code(FailureCodeV1::AgentRuntimeUnavailable)
        }
        StepStartFailure::Agent(failure) => agent_failure_cause(failure),
        StepStartFailure::OutputsUnsupported => {
            FailureCauseV1::code(FailureCodeV1::OutputsUnsupported)
        }
        StepStartFailure::WorkingDirectory(failure) => FailureCauseV1::code(match failure {
            WorkingDirectoryFailure::ExecutionRootRebound => FailureCodeV1::ExecutionRootRebound,
            WorkingDirectoryFailure::Unavailable => FailureCodeV1::WorkingDirectoryUnavailable,
            WorkingDirectoryFailure::EscapesExecutionRoot => FailureCodeV1::WorkingDirectoryEscape,
            WorkingDirectoryFailure::NotDirectory => FailureCodeV1::WorkingDirectoryNotDirectory,
        }),
        StepStartFailure::CommandPreparation(failure) => FailureCauseV1::code(match failure {
            CommandPreparationFailure::InvalidArgv => FailureCodeV1::CommandArgvInvalid,
            CommandPreparationFailure::PathNotConfigured => FailureCodeV1::CommandPathUnconfigured,
            CommandPreparationFailure::ExecutableNotFound => FailureCodeV1::ExecutableNotFound,
            CommandPreparationFailure::ExecutableUnavailable => {
                FailureCodeV1::ExecutableUnavailable
            }
        }),
        StepStartFailure::CommandLaunch(failure) => FailureCauseV1::code(match failure {
            CommandLaunchFailure::NotFound => FailureCodeV1::CommandLaunchNotFound,
            CommandLaunchFailure::PermissionDenied => FailureCodeV1::CommandLaunchPermissionDenied,
            CommandLaunchFailure::InvalidInput => FailureCodeV1::CommandLaunchInvalidInput,
            CommandLaunchFailure::Other => FailureCodeV1::CommandLaunchFailed,
        }),
    };
    Ok(cause)
}

fn agent_input_failure_cause(failure: &AgentInputStartFailure) -> FailureCauseV1 {
    FailureCauseV1::code(match failure {
        AgentInputStartFailure::StepUnavailable => FailureCodeV1::AgentStepUnavailable,
        AgentInputStartFailure::AgentAdmissionUnavailable => {
            FailureCodeV1::AgentAdmissionUnavailable
        }
        AgentInputStartFailure::InputsUnavailable => FailureCodeV1::AgentInputsUnavailable,
        AgentInputStartFailure::MissingUpstreamValue { .. } => {
            FailureCodeV1::AgentInputMissingUpstream
        }
        AgentInputStartFailure::ValueTypeMismatch { .. } => FailureCodeV1::AgentInputTypeMismatch,
        AgentInputStartFailure::RetainedSourceUnavailable { .. } => {
            FailureCodeV1::AgentSourceUnavailable
        }
        AgentInputStartFailure::InvalidRetainedText { .. } => FailureCodeV1::AgentSourceTextInvalid,
        AgentInputStartFailure::ResultSchemaUnavailable { .. } => {
            FailureCodeV1::AgentResultSchemaUnavailable
        }
        AgentInputStartFailure::InvalidValueMode => FailureCodeV1::AgentValueModeInvalid,
        AgentInputStartFailure::AttachmentCountLimitExceeded { .. } => {
            FailureCodeV1::AgentAttachmentCountLimit
        }
        AgentInputStartFailure::AttachmentBytesLimitExceeded { .. } => {
            FailureCodeV1::AgentAttachmentBytesLimit
        }
        AgentInputStartFailure::WorkingDirectory(failure) => match failure {
            WorkingDirectoryFailure::ExecutionRootRebound => FailureCodeV1::ExecutionRootRebound,
            WorkingDirectoryFailure::Unavailable => FailureCodeV1::WorkingDirectoryUnavailable,
            WorkingDirectoryFailure::EscapesExecutionRoot => FailureCodeV1::WorkingDirectoryEscape,
            WorkingDirectoryFailure::NotDirectory => FailureCodeV1::WorkingDirectoryNotDirectory,
        },
        AgentInputStartFailure::ArtifactStagingMismatch => FailureCodeV1::ArtifactStagingMismatch,
        AgentInputStartFailure::AgentStagingMismatch => FailureCodeV1::AgentStagingMismatch,
        AgentInputStartFailure::StagingUnavailable
        | AgentInputStartFailure::DiagnosticSessionUnavailable { .. } => {
            FailureCodeV1::AgentInputStagingUnavailable
        }
    })
}

fn agent_failure_cause(failure: &AgentFailure) -> FailureCauseV1 {
    FailureCauseV1::code(agent_failure_code(failure.cause()))
}

fn agent_failure_code(failure: &AgentFailureCause) -> FailureCodeV1 {
    match failure {
        AgentFailureCause::HarnessStartFailed { .. }
        | AgentFailureCause::HarnessSetupFailed { .. }
        | AgentFailureCause::HarnessSetupRejected { .. } => FailureCodeV1::HarnessStartFailed,
        AgentFailureCause::HarnessInputTooLarge { .. } => FailureCodeV1::HarnessInputTooLarge,
        AgentFailureCause::HarnessFailed { .. } => FailureCodeV1::HarnessFailed,
        AgentFailureCause::HarnessProtocolFailed => FailureCodeV1::HarnessProtocolFailed,
        AgentFailureCause::MissingResponse => FailureCodeV1::MissingResponse,
        AgentFailureCause::MissingResult => FailureCodeV1::MissingResult,
        AgentFailureCause::ResultValidationLimitExceeded { .. } => {
            FailureCodeV1::ResultValidationLimitExceeded
        }
        AgentFailureCause::CapturedValueTooLarge => FailureCodeV1::CapturedValueTooLarge,
        AgentFailureCause::ResultSettlementFailed => FailureCodeV1::ResultSettlementFailed,
    }
}

fn execution_failure_cause(failure: &StepExecutionFailure) -> FailureCauseV1 {
    match failure {
        StepExecutionFailure::Command(CommandExecutionFailure::UnsuccessfulExit { code }) => {
            let mut cause = FailureCauseV1::code(FailureCodeV1::CommandExit);
            cause.exit_code = *code;
            cause
        }
        StepExecutionFailure::Command(CommandExecutionFailure::Wait) => {
            FailureCauseV1::code(FailureCodeV1::CommandWaitFailed)
        }
        StepExecutionFailure::Agent(failure) => agent_failure_cause(failure),
        StepExecutionFailure::TaskUnavailable => {
            FailureCauseV1::code(FailureCodeV1::ExecutionTaskUnavailable)
        }
    }
}

fn output_capture_failure_cause(failure: &OutputCaptureFailure) -> FailureCauseV1 {
    match failure {
        OutputCaptureFailure::StepUnavailable => {
            FailureCauseV1::code(FailureCodeV1::StepUnavailable)
        }
        OutputCaptureFailure::UnsupportedOutput => {
            FailureCauseV1::code(FailureCodeV1::OutputUnsupported)
        }
        OutputCaptureFailure::TaskUnavailable => {
            FailureCauseV1::code(FailureCodeV1::CaptureTaskUnavailable)
        }
        OutputCaptureFailure::Capture(failure) => {
            let code = match failure.kind() {
                CaptureFailureKind::AbsolutePath => FailureCodeV1::OutputPathAbsolute,
                CaptureFailureKind::LexicalEscape => FailureCodeV1::OutputPathEscape,
                CaptureFailureKind::EmptyPath => FailureCodeV1::OutputPathEmpty,
                CaptureFailureKind::Missing => FailureCodeV1::OutputMissing,
                CaptureFailureKind::SymbolicLink => FailureCodeV1::OutputSymbolicLink,
                CaptureFailureKind::NotDirectory => FailureCodeV1::OutputParentNotDirectory,
                CaptureFailureKind::NotRegularFile => FailureCodeV1::OutputNotRegularFile,
                CaptureFailureKind::SourceUnavailable => FailureCodeV1::OutputSourceUnavailable,
                CaptureFailureKind::InvalidTextEncoding => FailureCodeV1::OutputInvalidUtf8,
                CaptureFailureKind::InvalidJson => FailureCodeV1::OutputInvalidJson,
                CaptureFailureKind::DuplicateJsonMember => FailureCodeV1::OutputDuplicateJsonMember,
                CaptureFailureKind::JsonSchemaMismatch => FailureCodeV1::OutputJsonSchemaMismatch,
                CaptureFailureKind::FileCountLimitExceeded => FailureCodeV1::CapturedFileCountLimit,
                CaptureFailureKind::FileSizeLimitExceeded => FailureCodeV1::CapturedFileSizeLimit,
                CaptureFailureKind::TotalSizeLimitExceeded => FailureCodeV1::CapturedTotalSizeLimit,
                CaptureFailureKind::GitCarrierCountLimitExceeded => {
                    FailureCodeV1::CapturedGitCarrierCountLimit
                }
                CaptureFailureKind::GitCarrierSizeLimitExceeded => {
                    FailureCodeV1::CapturedGitCarrierSizeLimit
                }
                CaptureFailureKind::TotalGitCarrierSizeLimitExceeded => {
                    FailureCodeV1::CapturedTotalGitCarrierSizeLimit
                }
                CaptureFailureKind::CarrierProducerUnavailable => {
                    FailureCodeV1::GitBundleGenerationFailed
                }
                CaptureFailureKind::InvalidDeclaration | CaptureFailureKind::StagingUnavailable => {
                    FailureCodeV1::OutputStagingUnavailable
                }
            };
            let mut cause = FailureCauseV1::code(code);
            cause.output = Some(failure.output_identity().to_owned());
            cause
        }
        OutputCaptureFailure::Git { output, failure } => {
            let code = match failure.cause() {
                GitCaptureFailure::CommandFailed(_) => FailureCodeV1::GitRequiredObjectsUnavailable,
                GitCaptureFailure::Cancelled | GitCaptureFailure::Artifact(_) => {
                    FailureCodeV1::OutputStagingUnavailable
                }
                GitCaptureFailure::ExecutionRootRebound => FailureCodeV1::GitExecutionRootRebound,
                GitCaptureFailure::StagingMismatch => FailureCodeV1::OutputStagingUnavailable,
                GitCaptureFailure::HeadUnavailable => FailureCodeV1::GitHeadUnavailable,
                GitCaptureFailure::BaselineNotAncestor => FailureCodeV1::GitBaselineNotAncestor,
                GitCaptureFailure::CleanlinessUnavailable => {
                    FailureCodeV1::GitCleanlinessUnavailable
                }
                GitCaptureFailure::WorkspaceDirty => FailureCodeV1::GitWorkspaceDirty,
                GitCaptureFailure::TreeUnavailable => FailureCodeV1::GitTreeUnavailable,
                GitCaptureFailure::RequiredObjectsUnavailable => {
                    FailureCodeV1::GitRequiredObjectsUnavailable
                }
                GitCaptureFailure::SourceAuthorityChanged => {
                    FailureCodeV1::GitSourceAuthorityChanged
                }
                GitCaptureFailure::GitStructureLimitExceeded => {
                    FailureCodeV1::GitStructureLimitExceeded
                }
                GitCaptureFailure::CommandTimedOut(_) => FailureCodeV1::GitCommandTimedOut,
                GitCaptureFailure::BundleGenerationFailed => {
                    FailureCodeV1::GitBundleGenerationFailed
                }
                GitCaptureFailure::BundleProfileInvalid => FailureCodeV1::GitBundleProfileInvalid,
                GitCaptureFailure::BundleVerificationFailed => {
                    FailureCodeV1::GitBundleVerificationFailed
                }
                GitCaptureFailure::WorkspaceChanged => FailureCodeV1::GitWorkspaceChanged,
                GitCaptureFailure::TemporaryStorageUnavailable => {
                    FailureCodeV1::GitTemporaryStorageUnavailable
                }
            };
            let mut cause = FailureCauseV1::code(code);
            cause.output = Some(output.clone());
            cause
        }
    }
}

fn retained_path(path: &Path) -> Result<String, LocalPublicationError> {
    if !path.is_absolute() {
        return Err(invalid_run_result(RunResultInvariant::RetainedPath));
    }
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid_run_result(RunResultInvariant::RetainedPath))
}

fn timestamp(value: OffsetDateTime) -> Result<String, LocalPublicationError> {
    utc_timestamp(value).map_err(|_| invalid_run_result(RunResultInvariant::Timing))
}

fn duration_milliseconds(duration: Duration) -> Result<u64, LocalPublicationError> {
    u64::try_from(duration.as_millis()).map_err(|_| invalid_run_result(RunResultInvariant::Timing))
}

fn invalid_run_result(invariant: RunResultInvariant) -> LocalPublicationError {
    LocalPublicationError::invalid(invariant)
}

fn workflow_node_role(role: WorkflowNodeRole) -> WorkflowNodeRoleV1 {
    match role {
        WorkflowNodeRole::Step => WorkflowNodeRoleV1::Step,
        WorkflowNodeRole::Finalizer => WorkflowNodeRoleV1::Finalizer,
    }
}

pub(super) fn finalization_trigger(trigger: FinalizationTrigger) -> FinalizationTriggerV1 {
    match trigger {
        FinalizationTrigger::Succeeded => FinalizationTriggerV1::Succeeded,
        FinalizationTrigger::Failed => FinalizationTriggerV1::Failed,
        FinalizationTrigger::Cancelled => FinalizationTriggerV1::Cancelled,
    }
}

pub(super) fn cancellation_reason(reason: CancellationReason) -> CancellationReasonV1 {
    match reason {
        CancellationReason::UserRequest => CancellationReasonV1::UserRequest,
        CancellationReason::TerminationRequest => CancellationReasonV1::TerminationRequest,
        CancellationReason::CallerOutputFailure => CancellationReasonV1::CallerOutputFailure,
        CancellationReason::RunnerShutdown => CancellationReasonV1::RunnerShutdown,
        CancellationReason::ExecutionLeaseExpired => CancellationReasonV1::ExecutionLeaseExpired,
        CancellationReason::ForceAbort => CancellationReasonV1::ForceAbort,
    }
}

fn export_unavailable_reason(reason: ExportUnavailableReason) -> ExportUnavailableReasonV1 {
    match reason {
        ExportUnavailableReason::Failed => ExportUnavailableReasonV1::Failed,
        ExportUnavailableReason::Blocked => ExportUnavailableReasonV1::Blocked,
        ExportUnavailableReason::Skipped => ExportUnavailableReasonV1::Skipped,
        ExportUnavailableReason::NotRun => ExportUnavailableReasonV1::NotRun,
        ExportUnavailableReason::TriggerNotSelected => {
            ExportUnavailableReasonV1::TriggerNotSelected
        }
        ExportUnavailableReason::Cancelled => ExportUnavailableReasonV1::Cancelled,
    }
}

fn execution_outcome(run: &WorkflowRunResult, outcome: WorkflowOutcomeV1) -> ExecutionOutcome {
    if run.force_abort.is_some()
        || run
            .steps
            .iter()
            .chain(
                run.finalization
                    .iter()
                    .flat_map(|finalization| &finalization.finalizers),
            )
            .any(|step| {
                step.command_output.as_ref().is_some_and(|output| {
                    !output.standard_output().fully_drained()
                        || !output.standard_error().fully_drained()
                }) || step.invocations.iter().any(|invocation| {
                    invocation
                        .diagnostics
                        .iter()
                        .any(|diagnostic| !diagnostic.stream.fully_drained)
                })
            })
        || run.finalization.as_ref().is_some_and(|finalization| {
            finalization.force_abort
                || finalization
                    .cancellation
                    .as_ref()
                    .is_some_and(|cancellation| {
                        matches!(
                            cancellation.reason,
                            CancellationReason::CallerOutputFailure
                                | CancellationReason::RunnerShutdown
                                | CancellationReason::ExecutionLeaseExpired
                                | CancellationReason::ForceAbort
                        )
                    })
        })
    {
        return ExecutionOutcome::Failed;
    }
    match outcome {
        WorkflowOutcomeV1::Succeeded => ExecutionOutcome::Succeeded,
        WorkflowOutcomeV1::Failed => ExecutionOutcome::Failed,
        WorkflowOutcomeV1::Cancelled => match &run.outcome {
            RunOutcome::Cancelled {
                reason: CancellationReason::UserRequest,
            } => ExecutionOutcome::Interrupted,
            RunOutcome::Cancelled {
                reason: CancellationReason::TerminationRequest,
            } => ExecutionOutcome::Terminated,
            RunOutcome::Cancelled {
                reason:
                    CancellationReason::CallerOutputFailure
                    | CancellationReason::RunnerShutdown
                    | CancellationReason::ExecutionLeaseExpired
                    | CancellationReason::ForceAbort,
            }
            | RunOutcome::Succeeded
            | RunOutcome::Failed { .. } => ExecutionOutcome::Failed,
        },
    }
}

const fn exit_status(outcome: ExecutionOutcome) -> u16 {
    match outcome {
        ExecutionOutcome::Succeeded => 0,
        ExecutionOutcome::Failed => 1,
        ExecutionOutcome::Interrupted => 130,
        ExecutionOutcome::Terminated => 143,
    }
}

struct PublicationTarget {
    supplied_parent: PathBuf,
    parent: OwnedFd,
    staging_parent: OwnedFd,
    name: OsString,
    normalized: String,
}

impl PublicationTarget {
    fn validate(
        destination: &Path,
        private_staging: Option<&Path>,
        expected_parents: Option<(&OwnedFd, &OwnedFd)>,
    ) -> Result<Self, LocalPublicationError> {
        let name = destination.file_name().ok_or_else(|| {
            LocalPublicationError::new(
                LocalPublicationPhase::TargetValidation,
                LocalPublicationFailureKind::InvalidResultPath,
            )
        })?;
        if name == OsStr::new(".") || name == OsStr::new("..") || name.to_str().is_none() {
            return Err(LocalPublicationError::new(
                LocalPublicationPhase::TargetValidation,
                LocalPublicationFailureKind::InvalidResultPath,
            ));
        }
        let supplied_parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let supplied_parent = if supplied_parent.is_absolute() {
            supplied_parent.to_owned()
        } else {
            std::env::current_dir()
                .map_err(|_| {
                    LocalPublicationError::new(
                        LocalPublicationPhase::TargetValidation,
                        LocalPublicationFailureKind::ParentUnavailable,
                    )
                })?
                .join(supplied_parent)
        };
        let canonical_parent = std::fs::canonicalize(&supplied_parent).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::TargetValidation,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })?;
        let parent = open_directory(&canonical_parent).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::TargetValidation,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })?;
        let normalized = canonical_parent
            .join(name)
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| {
                LocalPublicationError::new(
                    LocalPublicationPhase::TargetValidation,
                    LocalPublicationFailureKind::InvalidResultPath,
                )
            })?;
        let staging_parent = match private_staging {
            Some(path) => {
                let canonical = std::fs::canonicalize(path).map_err(|_| {
                    LocalPublicationError::new(
                        LocalPublicationPhase::TargetValidation,
                        LocalPublicationFailureKind::ParentUnavailable,
                    )
                })?;
                open_directory(&canonical).map_err(|_| {
                    LocalPublicationError::new(
                        LocalPublicationPhase::TargetValidation,
                        LocalPublicationFailureKind::ParentUnavailable,
                    )
                })?
            }
            None => rustix::io::dup(&parent).map_err(|_| {
                LocalPublicationError::new(
                    LocalPublicationPhase::TargetValidation,
                    LocalPublicationFailureKind::ParentUnavailable,
                )
            })?,
        };
        if let Some((expected_parent, expected_staging_parent)) = expected_parents
            && (!same_file(expected_parent, &parent).map_err(|_| invalid_publication_parent())?
                || !same_file(expected_staging_parent, &staging_parent)
                    .map_err(|_| invalid_publication_parent())?)
        {
            return Err(invalid_publication_parent());
        }
        cleanup_abandoned_preflights(&staging_parent)?;
        if !same_file(&staging_parent, &parent).map_err(|_| invalid_publication_parent())? {
            cleanup_abandoned_preflights(&parent)?;
        }
        verify_publication_capability(&staging_parent, &parent)?;
        Ok(Self {
            supplied_parent,
            parent,
            staging_parent,
            name: name.to_owned(),
            normalized,
        })
    }

    fn verify_parent(&self) -> Result<(), LocalPublicationError> {
        let rebound = std::fs::canonicalize(&self.supplied_parent).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })?;
        let rebound = open_directory(&rebound).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })?;
        if !same_file(&self.parent, &rebound).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::ParentUnavailable,
            )
        })? {
            return Err(LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::ParentUnavailable,
            ));
        }
        Ok(())
    }

    fn existing_publication(
        &self,
        expected: &WorkflowResultV1,
        expected_root: &OwnedFd,
    ) -> Result<ExistingPublication, LocalPublicationError> {
        let named = match statat(&self.parent, &self.name, AtFlags::SYMLINK_NOFOLLOW) {
            Err(Errno::NOENT) => return Ok(ExistingPublication::Absent),
            Ok(named) => named,
            Err(_) => return Ok(ExistingPublication::Conflict),
        };
        if FileType::from_raw_mode(named.st_mode) != FileType::Directory {
            return Ok(ExistingPublication::Conflict);
        }
        let root = match openat(
            &self.parent,
            &self.name,
            directory_open_flags(),
            Mode::empty(),
        ) {
            Ok(root) => root,
            Err(_) => return Ok(ExistingPublication::Conflict),
        };
        let opened = fstat(&root).map_err(|_| result_conflict())?;
        if opened.st_dev != named.st_dev || opened.st_ino != named.st_ino {
            return Ok(ExistingPublication::Conflict);
        }
        let retained = match artifact_set::read_and_validate(
            &root,
            result_metadata::MAXIMUM_RESULT_JSON_BYTES,
        ) {
            Ok(retained) => retained,
            Err(_) => return Ok(ExistingPublication::Conflict),
        };
        if retained != *expected {
            return Ok(ExistingPublication::Conflict);
        }
        let Some(mut file) = open_result_file(&root) else {
            return Ok(ExistingPublication::Conflict);
        };
        let Some(mut expected) = open_result_file(expected_root) else {
            return Ok(ExistingPublication::Conflict);
        };
        if fstat(&file).ok().map(|stat| stat.st_size)
            != fstat(&expected).ok().map(|stat| stat.st_size)
        {
            return Ok(ExistingPublication::Conflict);
        }
        let mut left = [0; 64 * 1024];
        let mut right = [0; 64 * 1024];
        loop {
            let count = match file.read(&mut left) {
                Ok(count) => count,
                Err(_) => return Ok(ExistingPublication::Conflict),
            };
            if expected.read_exact(&mut right[..count]).is_err() || left[..count] != right[..count]
            {
                return Ok(ExistingPublication::Conflict);
            }
            if count == 0 {
                return Ok(ExistingPublication::Identical);
            }
        }
    }
}

fn open_result_file(root: &OwnedFd) -> Option<File> {
    openat(
        root,
        RESULT_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .ok()
    .map(File::from)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExistingPublication {
    Absent,
    Identical,
    Conflict,
}

fn result_conflict() -> LocalPublicationError {
    LocalPublicationError::new(
        LocalPublicationPhase::Commit,
        LocalPublicationFailureKind::ResultConflict,
    )
}

fn invalid_publication_parent() -> LocalPublicationError {
    LocalPublicationError::new(
        LocalPublicationPhase::TargetValidation,
        LocalPublicationFailureKind::ParentUnavailable,
    )
}

struct StagingDirectory<'a> {
    parent: &'a OwnedFd,
    identity: String,
    root: OwnedFd,
    exports: Option<OwnedFd>,
    export_files: Vec<String>,
    result_created: bool,
    committed: bool,
}

impl<'a> StagingDirectory<'a> {
    fn create(target: &'a PublicationTarget) -> Result<Self, LocalPublicationError> {
        let (identity, root) = create_staging_root(&target.staging_parent)?;
        let mut staging = Self {
            parent: &target.staging_parent,
            identity,
            root,
            exports: None,
            export_files: Vec::new(),
            result_created: false,
            committed: false,
        };
        mkdirat(&staging.root, EXPORT_DIRECTORY, Mode::RWXU).map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Staging,
                LocalPublicationFailureKind::StagingUnavailable,
            )
        })?;
        let exports = openat(
            &staging.root,
            EXPORT_DIRECTORY,
            directory_open_flags(),
            Mode::empty(),
        )
        .map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Staging,
                LocalPublicationFailureKind::StagingUnavailable,
            )
        })?;
        staging.exports = Some(exports);
        Ok(staging)
    }

    fn create_export(&mut self, name: &str) -> Result<File, LocalPublicationFailureKind> {
        let exports = self
            .exports
            .as_ref()
            .ok_or(LocalPublicationFailureKind::StagingUnavailable)?;
        let file = openat(
            exports,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| LocalPublicationFailureKind::ExportWriteUnavailable)?;
        self.export_files.push(name.to_owned());
        Ok(File::from(file))
    }

    fn expose_export(
        &mut self,
        artifacts: &ArtifactStaging,
        carrier: &StagedCarrier,
        expected_output_identity: &str,
        name: &str,
    ) -> Result<(), ArtifactExposeFailure> {
        let exports = self
            .exports
            .as_ref()
            .ok_or(ArtifactExposeFailure::Unavailable)?;
        self.export_files.push(name.to_owned());
        artifacts.expose_carrier(carrier, expected_output_identity, exports, OsStr::new(name))
    }

    fn write_result_streamed(
        &mut self,
        value: &WorkflowResultV1,
        maximum: u64,
    ) -> Result<File, LocalPublicationError> {
        let mut result = openat(
            &self.root,
            RESULT_FILE,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map(File::from)
        .map_err(|_| {
            LocalPublicationError::new(
                LocalPublicationPhase::Serialization,
                LocalPublicationFailureKind::SerializationUnavailable,
            )
        })?;
        self.result_created = true;
        write_result_json(value, maximum, &mut result)?;
        Ok(result)
    }

    fn verify(&self, result: &WorkflowResultV1) -> Result<(), Errno> {
        let named = statat(self.parent, &self.identity, AtFlags::SYMLINK_NOFOLLOW)?;
        let opened = fstat(&self.root)?;
        if named.st_dev != opened.st_dev
            || named.st_ino != opened.st_ino
            || FileType::from_raw_mode(named.st_mode) != FileType::Directory
        {
            return Err(Errno::IO);
        }
        let root_entries = directory_entries(&self.root)?;
        if root_entries
            != BTreeSet::from([
                RESULT_FILE.as_bytes().to_vec(),
                EXPORT_DIRECTORY.as_bytes().to_vec(),
            ])
        {
            return Err(Errno::IO);
        }
        let named_result = statat(&self.root, RESULT_FILE, AtFlags::SYMLINK_NOFOLLOW)?;
        let named_exports = statat(&self.root, EXPORT_DIRECTORY, AtFlags::SYMLINK_NOFOLLOW)?;
        let exports = self.exports.as_ref().ok_or(Errno::IO)?;
        let opened_exports = fstat(exports)?;
        if FileType::from_raw_mode(named_result.st_mode) != FileType::RegularFile
            || FileType::from_raw_mode(named_exports.st_mode) != FileType::Directory
            || FileType::from_raw_mode(opened_exports.st_mode) != FileType::Directory
            || named_exports.st_dev != opened_exports.st_dev
            || named_exports.st_ino != opened_exports.st_ino
        {
            return Err(Errno::IO);
        }
        let expected_exports = self
            .export_files
            .iter()
            .map(|name| name.as_bytes().to_vec())
            .collect::<BTreeSet<_>>();
        if directory_entries(exports)? != expected_exports {
            return Err(Errno::IO);
        }
        for name in &self.export_files {
            if FileType::from_raw_mode(statat(exports, name, AtFlags::SYMLINK_NOFOLLOW)?.st_mode)
                != FileType::RegularFile
            {
                return Err(Errno::IO);
            }
        }
        let staged =
            artifact_set::read_and_validate(&self.root, result_metadata::MAXIMUM_RESULT_JSON_BYTES)
                .map_err(|_| Errno::IO)?;
        (staged == *result).then_some(()).ok_or(Errno::IO)
    }

    fn commit(
        &mut self,
        target: &PublicationTarget,
        observer: &mut impl PublicationObserver,
    ) -> Result<(), LocalPublicationError> {
        let unavailable = || {
            LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::AtomicPublicationUnavailable,
            )
        };
        let exports = self.exports.as_ref().ok_or_else(unavailable)?;
        for name in &self.export_files {
            let file = openat(
                exports,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(|_| unavailable())?;
            file.sync_all().map_err(|_| unavailable())?;
        }
        open_result_file(&self.root)
            .ok_or_else(unavailable)?
            .sync_all()
            .map_err(|_| unavailable())?;
        sync_directory(exports).map_err(|_| unavailable())?;
        sync_directory(&self.root).map_err(|_| unavailable())?;
        sync_directory(self.parent).map_err(|_| unavailable())?;
        if !same_file(self.parent, &target.parent).map_err(|_| unavailable())? {
            sync_directory(&target.parent).map_err(|_| unavailable())?;
        }
        renameat_with(
            self.parent,
            &self.identity,
            &target.parent,
            &target.name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|failure| {
            let kind = match failure {
                Errno::EXIST | Errno::NOTEMPTY => LocalPublicationFailureKind::DestinationExists,
                _ => LocalPublicationFailureKind::AtomicPublicationUnavailable,
            };
            LocalPublicationError::new(LocalPublicationPhase::Commit, kind)
        })?;
        self.committed = true;
        let committed_unavailable = || {
            LocalPublicationError::new(
                LocalPublicationPhase::Commit,
                LocalPublicationFailureKind::CommittedDurabilityUnavailable,
            )
        };
        observer
            .sync_committed_directory(&target.parent)
            .map_err(|_| committed_unavailable())?;
        if !same_file(self.parent, &target.parent).map_err(|_| committed_unavailable())? {
            observer
                .sync_committed_directory(self.parent)
                .map_err(|_| committed_unavailable())?;
        }
        Ok(())
    }

    fn cleanup(&mut self) {
        if self.committed {
            return;
        }
        let same_root = statat(self.parent, &self.identity, AtFlags::SYMLINK_NOFOLLOW)
            .and_then(|named| {
                let opened = fstat(&self.root)?;
                Ok(named.st_dev == opened.st_dev && named.st_ino == opened.st_ino)
            })
            .unwrap_or(false);
        if !same_root {
            return;
        }
        if let Some(exports) = self.exports.take() {
            for name in &self.export_files {
                let _ = unlinkat(&exports, name, AtFlags::empty());
            }
            drop(exports);
        }
        if self.result_created {
            let _ = unlinkat(&self.root, RESULT_FILE, AtFlags::empty());
        }
        let _ = unlinkat(&self.root, EXPORT_DIRECTORY, AtFlags::REMOVEDIR);
        let _ = unlinkat(self.parent, &self.identity, AtFlags::REMOVEDIR);
    }
}

impl Drop for StagingDirectory<'_> {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn sync_directory(directory: &OwnedFd) -> io::Result<()> {
    let readable = openat(directory, ".", directory_open_flags(), Mode::empty())?;
    File::from(readable).sync_all()
}

// Preflight directories contain no data. Never follow links or remove nonempty entries:
// another publisher may still own a concurrent preflight in the same parent.
fn cleanup_abandoned_preflights(parent: &OwnedFd) -> Result<(), LocalPublicationError> {
    let readable = openat(parent, ".", directory_open_flags(), Mode::empty())
        .map_err(|_| invalid_publication_parent())?;
    flock(&readable, FlockOperation::LockExclusive).map_err(|_| invalid_publication_parent())?;
    for name in directory_entries(&readable).map_err(|_| invalid_publication_parent())? {
        let Some(id) = name.strip_prefix(b".result-preflight-") else {
            continue;
        };
        if id.len() != 26
            || !id
                .iter()
                .all(|byte| b"0123456789abcdefghjkmnpqrstvwxyz".contains(byte))
        {
            continue;
        }
        let Ok(name) = std::str::from_utf8(&name) else {
            continue;
        };
        if let Ok(directory) = openat(parent, name, directory_open_flags(), Mode::empty()) {
            // The publisher holds a lock through preflight cleanup; only an
            // abandoned directory can be removed by a competing attempt.
            match flock(&directory, FlockOperation::NonBlockingLockExclusive) {
                Ok(()) => {}
                Err(Errno::AGAIN | Errno::ACCESS) => continue,
                Err(_) => return Err(invalid_publication_parent()),
            }
            match unlinkat(parent, name, AtFlags::REMOVEDIR) {
                Ok(()) | Err(Errno::NOENT | Errno::NOTEMPTY) => {}
                Err(_) => return Err(invalid_publication_parent()),
            }
        }
    }
    sync_directory(parent).map_err(|_| invalid_publication_parent())
}

fn verify_publication_capability(
    staging_parent: &OwnedFd,
    target_parent: &OwnedFd,
) -> Result<(), LocalPublicationError> {
    let (source, _locked_source) = create_validation_directory(staging_parent)?;
    for _ in 0..STAGING_ATTEMPTS {
        let destination = format!(
            ".result-preflight-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        );
        match renameat_with(
            staging_parent,
            &source,
            target_parent,
            &destination,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => {
                return unlinkat(target_parent, destination, AtFlags::REMOVEDIR).map_err(|_| {
                    LocalPublicationError::new(
                        LocalPublicationPhase::TargetValidation,
                        LocalPublicationFailureKind::ParentUnavailable,
                    )
                });
            }
            Err(Errno::EXIST | Errno::NOTEMPTY) => {}
            Err(_) => {
                let _ = unlinkat(staging_parent, &source, AtFlags::REMOVEDIR);
                return Err(LocalPublicationError::new(
                    LocalPublicationPhase::TargetValidation,
                    LocalPublicationFailureKind::AtomicPublicationUnavailable,
                ));
            }
        }
    }
    let _ = unlinkat(staging_parent, source, AtFlags::REMOVEDIR);
    Err(LocalPublicationError::new(
        LocalPublicationPhase::TargetValidation,
        LocalPublicationFailureKind::AtomicPublicationUnavailable,
    ))
}

fn create_validation_directory(
    parent: &OwnedFd,
) -> Result<(String, OwnedFd), LocalPublicationError> {
    // Serialize the mkdir-to-child-lock window with orphan cleanup in this parent.
    let parent_lock = openat(parent, ".", directory_open_flags(), Mode::empty())
        .map_err(|_| invalid_publication_parent())?;
    flock(&parent_lock, FlockOperation::LockExclusive).map_err(|_| invalid_publication_parent())?;
    for _ in 0..STAGING_ATTEMPTS {
        let identity = format!(
            ".result-preflight-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        );
        match mkdirat(parent, &identity, Mode::RWXU) {
            Ok(()) => {
                let directory = openat(parent, &identity, directory_open_flags(), Mode::empty())
                    .and_then(|directory| {
                        flock(&directory, FlockOperation::NonBlockingLockExclusive)?;
                        Ok(directory)
                    });
                return directory
                    .map(|directory| (identity.clone(), directory))
                    .map_err(|_| {
                        let _ = unlinkat(parent, &identity, AtFlags::REMOVEDIR);
                        invalid_publication_parent()
                    });
            }
            Err(Errno::EXIST) => {}
            Err(_) => {
                return Err(LocalPublicationError::new(
                    LocalPublicationPhase::TargetValidation,
                    LocalPublicationFailureKind::ParentUnavailable,
                ));
            }
        }
    }
    Err(LocalPublicationError::new(
        LocalPublicationPhase::TargetValidation,
        LocalPublicationFailureKind::ParentUnavailable,
    ))
}

fn create_staging_root(parent: &OwnedFd) -> Result<(String, OwnedFd), LocalPublicationError> {
    for _ in 0..STAGING_ATTEMPTS {
        let identity = format!(
            ".result-{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        );
        match mkdirat(parent, &identity, Mode::RWXU) {
            Ok(()) => {
                let root = openat(parent, &identity, directory_open_flags(), Mode::empty());
                return match root {
                    Ok(root) => Ok((identity, root)),
                    Err(_) => {
                        let _ = unlinkat(parent, &identity, AtFlags::REMOVEDIR);
                        Err(LocalPublicationError::new(
                            LocalPublicationPhase::Staging,
                            LocalPublicationFailureKind::StagingUnavailable,
                        ))
                    }
                };
            }
            Err(Errno::EXIST) => {}
            Err(_) => {
                return Err(LocalPublicationError::new(
                    LocalPublicationPhase::Staging,
                    LocalPublicationFailureKind::StagingUnavailable,
                ));
            }
        }
    }
    Err(LocalPublicationError::new(
        LocalPublicationPhase::Staging,
        LocalPublicationFailureKind::StagingUnavailable,
    ))
}

fn directory_open_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

fn close_file(file: File) -> io::Result<()> {
    nix::unistd::close(file).map_err(io::Error::from)
}

fn directory_entries(directory: &OwnedFd) -> Result<BTreeSet<Vec<u8>>, Errno> {
    directory_entry_names(directory)
}

#[cfg(test)]
mod tests;
