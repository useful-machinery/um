mod cloud_retained;
pub(crate) use cloud_retained::retain_cloud_completed_steps;
pub use cloud_retained::{
    bind_cloud_continuation_context, load_cloud_continuation_seed,
    retain_cloud_continuation_evidence, retain_cloud_workflow_evidence,
};

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd as _, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use ring::digest::{Context as DigestContext, SHA256, digest};
use rustix::fs::{
    AtFlags, FileType, FlockOperation, Mode, OFlags, RenameFlags, fchmod, fcntl_lock, fstat,
    linkat, mkdirat, openat, renameat_with, statat, unlinkat,
};
use rustix::io::{Errno, dup};
use rustix::process::{Flock, FlockOffsetType, FlockType, fcntl_getlk};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::admission::{
    AdmittedTree, AdmittedWorkflow, CancellationReason, ResolvedAttachment, ResolvedInput,
    ResolvedInputs, ResolvedJsonInput,
};
use super::agent::AgentCompatibilityProfile;
use super::agent_diagnostics::AgentDiagnosticSessionStore;
use super::archived_presentation::safe_path;
use super::artifact::{
    ArtifactStaging, CaptureCancellation, CaptureCandidateDeclaration, CarrierDestination,
    CarrierProducer, GitBranchCaptureDeclaration, GitBranchMetadata, GitObjectFormat,
    RetainedFileCaptureDeclaration,
};
use super::cancellation::MAXIMUM_CANCELLATION_GRACE;
use super::coordinator::{
    ClassifiedCommitError, CommitFailure, CommitFailureClass, CommitPort, CommittedActionKind,
    CommittedReduction, CoordinationDiagnostic,
};
use super::definition_store::WorkflowHome;
use super::diagnostic::{StepDiagnostic, StepDiagnosticLog};
use super::document::FailurePolicy;
use super::evidence::{NodeDetail, NonExecutionCode};
use super::execution_root::AdmittedExecutionRoot;
use super::force_abort_evidence::{
    FirstForceAbortPhase, finalization_cancellation_matches_force_phase,
    finalization_node_cancellation_matches, ordinary_node_cancellation_matches,
};
use super::git_capture::{GitCaptureContext, GitWorkspaceAdmissionFailure, LocalGitBaseline};
use super::invocation_accounting::InvocationAccountingLog;
use super::private_staging::{
    create_staging_root, directory_entry_names, open_directory_path, remove_staging_root, same_file,
};
use super::process_group::{
    AuthenticatedProcessGroup, AuthenticatedSignalResult, DurableProcessGuardStore,
    ProcessGuardRegistry, ProcessGuardStoreError, ProcessIdentityObservation,
    system_process_identity_observation, terminate_authenticated_process_group,
};
use super::profile_replacement::{
    AgentProfileReplacementDocument, MAXIMUM_AGENT_PROFILE_REPLACEMENT_BYTES,
};
use super::publication::{
    CancellationReasonV1, FailureDiagnosticV1, FinalizationTriggerV1, RunResultInvariant,
    cancellation_reason, finalization_trigger,
};
use super::resolution::{ResolvedWorkflow, resolve_retained};
use super::run_record::{RunState, WorkflowRunRecord};
use super::runtime::{
    ActiveStepInvocation, EnteredWait, ExecutionSeed, FinalizationSummary, InheritedDisposition,
    InheritedStepSeed, StepState, TargetExecutionNumber, WorkflowState,
};
use super::schema_common::{
    is_canonical_absolute_path, is_canonical_relative_path, is_lowercase_hex, lowercase_hex,
    utc_timestamp,
};
use super::step_runtime::StepFailureCause;
use super::tree::{TreeDiagnostic, ValidatedTree, validate_workflow_tree};
use super::tree_copy::{self, FileOperationError, TreeCopyError, file_locator, write_tree_copy};
use super::value::CapturedValue;
use super::wait::{ControlEvent, RuleError, WaitRecord, fan_out_element};
use super::workspace_snapshot::{
    WorkspaceSnapshotSettlementV1, WorkspaceSnapshotV1, capture_settlement_snapshot,
    capture_start_snapshot, compare_continuation_snapshots,
};

const LOCAL_RUN_FILE: &str = "local-run.json";
const RUN_RECORD_FILE: &str = "run.json";
const RUN_RECORD_TEMPORARY_FILE: &str = "run.json.tmp";
const STATE_FILE: &str = "state.json";
const LOCK_FILE: &str = "run.lock";
const WORKFLOW_DIRECTORY: &str = "workflow";
const WORKFLOW_FILES_DIRECTORY: &str = "files";
const WORKFLOW_MANIFEST_FILE: &str = "manifest.json";
const WORKFLOW_TREE_DIRECTORY: &str = "tree";
const ATTEMPTS_DIRECTORY: &str = "attempts";
const INVOCATIONS_DIRECTORY: &str = "invocations";
const VALUES_DIRECTORY: &str = "values";
pub(super) const PRIVATE_DIRECTORY: &str = ".private";
const CHILDREN_DIRECTORY: &str = "children";
/// The wait record of a waiting run (REQ-LOCALRUN-178), at the root and under the attempt.
const WAIT_RECORD_FILE: &str = "wait.json";
const WAIT_RECORD_TEMPORARY_FILE: &str = "wait.json.tmp";
/// The run record's outcome for a run withdrawn from its wait (REQ-CONTINUE-75).
pub const WITHDRAWN_OUTCOME: &str = "withdrawn";
/// The run record's outcome for a run canceled while it waits (REQ-CONTINUE-75).
pub const CANCELED_OUTCOME: &str = "canceled";
/// The run record's outcome for a run its successor superseded (INV-ENGINE-26).
pub const SUPERSEDED_OUTCOME: &str = "superseded";
/// The prefix of a run record's id (REQ-LOCALRUN-175).
const RUN_ID_PREFIX: &str = "run_";
/// The controls appended to the run (REQ-LOCALRUN-178), written by `wait::append_control`.
pub(super) const CONTROLS_DIRECTORY: &str = "controls";
/// The mirror writes of the run (REQ-LOCALRUN-193), written by `mirror::append_write`.
pub(super) const MIRROR_DIRECTORY: &str = "mirror";
const INITIAL_ATTEMPT_NUMBER: u64 = 1;
const INITIAL_ATTEMPT_DIRECTORY: &str = "000001";
const STAGING_ATTEMPTS: usize = 16;
const PRIVATE_STAGING_ATTEMPTS: usize = 16;
const STATUS_SNAPSHOT_ATTEMPTS: usize = 8;
pub(super) const MAXIMUM_DURABLE_JSON_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_RETAINED_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_RETAINED_OUTPUTS: usize = 4_096;
const MAXIMUM_RETAINED_OUTPUT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAXIMUM_RETAINED_SOURCE_CLOSURE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_RETAINED_TEXT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_RETAINED_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_RETAINED_CAPTURED_FILE_BYTES: u64 =
    MAXIMUM_RETAINED_SOURCE_CLOSURE_BYTES + MAXIMUM_RETAINED_INPUT_BYTES;
const MAXIMUM_RETAINED_RUN_JSON_BYTES: u64 =
    3 * MAXIMUM_DURABLE_JSON_BYTES + super::result_metadata::MAXIMUM_RESULT_JSON_BYTES;
// An archived-attempt read covers the immutable workflow/import captures, the base64
// representation of both bounded diagnostic streams, and the run, state, manifest,
// and result JSON envelopes. The complete result allowance already includes both
// diagnostic streams; do not add them a second time. Each term is independently
// enforced while it is read.
const MAXIMUM_RETAINED_TOTAL_BYTES: u64 =
    MAXIMUM_RETAINED_CAPTURED_FILE_BYTES + MAXIMUM_RETAINED_RUN_JSON_BYTES;
const MAXIMUM_DIAGNOSTICS: usize = 256;
const QUIESCENCE_POLL_INTERVAL: Duration = Duration::from_millis(5);
const QUIESCENCE_POLL_ATTEMPTS: usize =
    (MAXIMUM_CANCELLATION_GRACE.as_millis() / QUIESCENCE_POLL_INTERVAL.as_millis()) as usize;
const SHA256_ALGORITHM: &str = "sha256";

#[derive(Debug)]
pub enum LocalRunDirectoryError {
    InvalidPath,
    ParentUnavailable,
    DestinationExists,
    ExecutionRootOverlap,
    StagingUnavailable,
    LockUnavailable,
    IdentityUnavailable,
    HostIdentityUnavailable,
    SerializationUnavailable,
    StateInvalid,
    DocumentFramingInvalid,
    DocumentNullInvalid,
    RecoverySchemaUnsupported,
    StateConflict,
    StateWriteUnavailable,
    PublicationUnavailable,
    File {
        path: PathBuf,
        operation: &'static str,
        source: io::Error,
    },
    StateFile {
        path: PathBuf,
        operation: &'static str,
        source: Box<Self>,
    },
    Json {
        operation: &'static str,
        source: serde_json::Error,
    },
    Artifact {
        path: PathBuf,
        operation: &'static str,
        source: super::artifact::ArtifactReadFailure,
    },
    AttemptNumberInvalid,
    AttemptTriggerInvalid,
    AttemptIdentityInvalid,
    AttemptExecutionRootInvalid,
    AttemptCreatedAtInvalid,
    AttemptStartedAtInvalid,
    AttemptSettledAtInvalid,
    AttemptSettlementInvalid,
    AttemptDefinitionInvalid,
    AttemptSnapshotInvalid,
    AttemptOwnerInvalid,
    AttemptStepsEmpty,
    AttemptStartInvalid,
    AttemptCancellationRecordInvalid,
    AttemptForceAbortInvalid,
    AttemptCancellationMissing,
    AttemptCancellationConfirmationInvalid,
    AttemptInterruptionCancellationInvalid,
    AttemptInterruptionInvalid,
    AttemptRejectionInvalid,
    AttemptStepIdInvalid,
    AttemptStepRoleInvalid,
    AttemptStepDuplicate,
    AttemptStepDetailInvalid,
    AttemptStepOutputsInvalid,
    AttemptStepRecoveryInvalid,
    AttemptStepCancellationInvalid,
    AttemptActionIdInvalid,
    AttemptActionTargetInvalid,
    AttemptActionNodeInvalid,
    AttemptActionInvocationInvalid,
    AttemptTerminalActionsInvalid,
    AttemptGuardIdInvalid,
    AttemptGuardActionInvalid,
    AttemptGuardStepInvalid,
    AttemptGuardHostInvalid,
    AttemptGuardProcessInvalid,
    AttemptContinuationInvalid,
    AttemptInheritedStepInvalid,
    AttemptInheritedOutputInvalid,
    AttemptRecoveryAccountingInvalid,
    AttemptRecoveryInvocationInvalid,
    AttemptRecoveryDiagnosticInvalid,
    AttemptRecoveryRoundInvalid,
    AttemptRecoveryActiveInvalid,
    AttemptFinalizationForceAbortInvalid,
    AttemptFinalizationProgressInvalid,
    AttemptFinalizationCompleteInvalid,
    AttemptFinalizationIssuesInvalid,
    AttemptFinalizationInterruptionInvalid,
    AttemptResultInvalid,
    ManifestDigestInvalid,
    RegularFileInvalid,
    FileSizeInvalid,
    CarrierInvalid,
    StateSchemaInvalid,
    StateIdentityInvalid,
    StateRevisionInvalid,
    StateCurrentAttemptInvalid,
    StateAttemptsEmpty,
    StateAttemptIndexInvalid,
    StateDiagnosticsLimitInvalid,
    WaitRecordInvalid,
    ControlEventInvalid,
    MirrorWriteInvalid,
    /// No run under the `um` home has the id (REQ-LOCALRUN-180): `run_not_found`.
    RunNotFound,
    /// A parent's commit refused because one of its child runs failed to commit its own
    /// state (REQ-LOCALRUN-207, INV-ENGINE-33); it classifies as the child's failure and
    /// reads as the child's own error.
    ChildCommitFailed(ChildCommitFailure),
}

/// A child run's failed state commit as its parent's [`LocalRunCommitPort`] latches it
/// (REQ-LOCALRUN-207, INV-ENGINE-33): the classified failure and, beside it, the text of
/// the child's own error, which names the child's file, for the root command to print.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildCommitFailure {
    pub failure: CommitFailure,
    pub text: Option<String>,
}

impl PartialEq for LocalRunDirectoryError {
    fn eq(&self, other: &Self) -> bool {
        use LocalRunDirectoryError::{Artifact, ChildCommitFailed, File, Json, StateFile};
        match (self, other) {
            (
                File {
                    path: a,
                    operation: op_a,
                    source: a_source,
                },
                File {
                    path: b,
                    operation: op_b,
                    source: b_source,
                },
            ) => {
                a == b
                    && op_a == op_b
                    && a_source.kind() == b_source.kind()
                    && a_source.raw_os_error() == b_source.raw_os_error()
            }
            (
                StateFile {
                    path: a,
                    operation: op_a,
                    source: a_source,
                },
                StateFile {
                    path: b,
                    operation: op_b,
                    source: b_source,
                },
            ) => a == b && op_a == op_b && a_source == b_source,
            (
                Artifact {
                    path: a,
                    operation: op_a,
                    source: a_source,
                },
                Artifact {
                    path: b,
                    operation: op_b,
                    source: b_source,
                },
            ) => a == b && op_a == op_b && a_source == b_source,
            (
                Json {
                    operation: op_a,
                    source: a,
                },
                Json {
                    operation: op_b,
                    source: b,
                },
            ) => {
                op_a == op_b
                    && a.classify() == b.classify()
                    && a.line() == b.line()
                    && a.column() == b.column()
            }
            (ChildCommitFailed(a), ChildCommitFailed(b)) => a == b,
            _ => std::mem::discriminant(self) == std::mem::discriminant(other),
        }
    }
}

impl Eq for LocalRunDirectoryError {}

/// INV-ENGINE-33: a failed state write keeps its class, operation and OS error number,
/// never its path or the operating system's message.
impl ClassifiedCommitError for LocalRunDirectoryError {
    fn commit_failure(&self) -> CommitFailure {
        match self {
            Self::File {
                operation, source, ..
            } => CommitFailure {
                class: match source.kind() {
                    io::ErrorKind::StorageFull => CommitFailureClass::StorageFull,
                    io::ErrorKind::QuotaExceeded => CommitFailureClass::QuotaExceeded,
                    io::ErrorKind::PermissionDenied => CommitFailureClass::PermissionDenied,
                    io::ErrorKind::ReadOnlyFilesystem => CommitFailureClass::ReadOnlyFilesystem,
                    io::ErrorKind::NotFound => CommitFailureClass::NotFound,
                    _ => CommitFailureClass::IoFailure,
                },
                operation: Some(operation),
                os_error: source.raw_os_error(),
            },
            Self::StateFile { source, .. } => source.commit_failure(),
            Self::StateConflict => CommitFailure::of_class(CommitFailureClass::StateConflict),
            Self::StateInvalid => CommitFailure::of_class(CommitFailureClass::StateInvalid),
            Self::ChildCommitFailed(child) => child.failure,
            _ => CommitFailure::of_class(CommitFailureClass::Unclassified),
        }
    }
}

impl fmt::Display for LocalRunDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File {
                path,
                operation,
                source,
            } => write!(formatter, "{operation} {}: {source}", safe_path(path)),
            Self::StateFile {
                path,
                operation,
                source,
            } => write!(formatter, "{operation} {}: {source}", safe_path(path)),
            Self::Json { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::Artifact {
                path,
                operation,
                source,
            } => write!(formatter, "{operation} {}: {source}", safe_path(path)),
            Self::ChildCommitFailed(ChildCommitFailure {
                text: Some(text), ..
            }) => formatter.write_str(text),
            Self::ChildCommitFailed(ChildCommitFailure { failure, .. }) => {
                write!(formatter, "child run state commit failed: {failure}")
            }
            // Variant names are stable, narrow invariant identifiers; split words for CLI prose.
            other => {
                let name = format!("{other:?}");
                let mut previous_lowercase = false;
                for character in name.chars() {
                    if character.is_uppercase() && previous_lowercase {
                        write!(formatter, " ")?;
                    }
                    write!(formatter, "{}", character.to_ascii_lowercase())?;
                    previous_lowercase = character.is_lowercase();
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for LocalRunDirectoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File { source, .. } => Some(source),
            Self::StateFile { source, .. } => Some(source),
            Self::Json { source, .. } => Some(source),
            Self::Artifact { source, .. } => Some(source),
            _ => None,
        }
    }
}

pub(super) fn file_error(
    parent: &OwnedFd,
    name: impl AsRef<std::ffi::OsStr>,
    operation: &'static str,
    source: impl Into<io::Error>,
) -> LocalRunDirectoryError {
    path_error(file_locator(parent, name), operation, source)
}

fn file_operation_error(error: FileOperationError) -> LocalRunDirectoryError {
    path_error(error.path, error.operation, error.source)
}

pub(super) fn path_error(
    path: PathBuf,
    operation: &'static str,
    source: impl Into<io::Error>,
) -> LocalRunDirectoryError {
    LocalRunDirectoryError::File {
        path,
        operation,
        source: source.into(),
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DigestV1 {
    pub(super) algorithm: String,
    pub(super) value: String,
}

impl DigestV1 {
    fn sha256(bytes: &[u8]) -> Self {
        Self {
            algorithm: SHA256_ALGORITHM.to_owned(),
            value: lowercase_hex(digest(&SHA256, bytes).as_ref()),
        }
    }

    fn validate(&self) -> bool {
        self.algorithm == SHA256_ALGORITHM && is_lowercase_hex(&self.value, 64)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct LocalRunV1 {
    pub(super) schema_version: u8,
    pub(super) local_run_id: String,
    pub(super) created_at: String,
    pub(super) workflow_digest: DigestV1,
    pub(super) workflow_manifest_digest: DigestV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    git_baseline: Option<GitBaselineV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum GitBaselineV1 {
    Available {
        #[serde(rename = "objectFormat")]
        object_format: String,
        #[serde(rename = "commitOid")]
        commit_oid: String,
    },
    Unavailable {
        reason: GitBaselineUnavailableReasonV1,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GitBaselineUnavailableReasonV1 {
    Cancelled,
    ExecutionRootRebound,
    GitUnavailable,
    GitTimedOut,
    GitOutputLimitExceeded,
    NotWorkTree,
    ExecutionRootNotWorkTreeRoot,
    UnsupportedObjectFormat,
    BaselineUnavailable,
    InitialWorkspaceDirty,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptDefinitionV1 {
    digest: DigestV1,
    manifest_digest: DigestV1,
    locator: AttemptDefinitionLocatorV1,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AttemptDefinitionLocatorV1 {
    Run,
    Attempt {
        #[serde(rename = "attemptNumber")]
        attempt_number: u64,
    },
    PriorAttempt {
        #[serde(rename = "attemptNumber")]
        attempt_number: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowManifestV1 {
    schema_version: u8,
    workflow_path: String,
    source_root: String,
    maximum_parallel_steps: usize,
    source_files: Vec<ManifestSourceFileV1>,
    inputs: BTreeMap<String, ManifestInputV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_profile_replacement: Option<ManifestFileV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tree: Option<ManifestTreeV1>,
}

/// The run's read-only copy of the tree it was admitted from (REQ-LOCALRUN-170): the
/// copy's directory under `workflow/`, the tree digest (REQ-WFDEF-11) and the root's
/// `programs` and `requires.tools`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestTreeV1 {
    digest: DigestV1,
    relative_directory: String,
    programs: Vec<String>,
    required_tools: Vec<String>,
}

impl ManifestTreeV1 {
    /// The tree digest recorded when the run was created, lowercase hex.
    pub fn digest(&self) -> &str {
        &self.digest.value
    }

    pub fn programs(&self) -> &[String] {
        &self.programs
    }

    pub fn required_tools(&self) -> &[String] {
        &self.required_tools
    }
}

/// A retained tree copy whose recomputed digest differs from the manifest's
/// (REQ-LOCALRUN-171); a copy that no longer validates as a tree has no digest and
/// carries the validator's diagnostics instead.
#[derive(Debug)]
pub struct TreeCopyMismatch {
    expected: String,
    actual: Option<String>,
    diagnostics: Vec<TreeDiagnostic>,
}

impl TreeCopyMismatch {
    pub fn expected_digest(&self) -> &str {
        &self.expected
    }

    /// The copy's digest now, or `None` when the copy no longer validates.
    pub fn actual_digest(&self) -> Option<&str> {
        self.actual.as_deref()
    }

    pub fn diagnostics(&self) -> &[TreeDiagnostic] {
        &self.diagnostics
    }
}

/// Re-validates the run's tree copy (`<run>/workflow/tree`) and compares its digest
/// with the manifest's record (REQ-LOCALRUN-171). Run, retry and continue share it.
pub(crate) fn verify_tree_copy(
    run_directory: &Path,
    record: &ManifestTreeV1,
) -> Result<ValidatedTree, TreeCopyMismatch> {
    let copy = tree_copy_directory(run_directory, record);
    let mismatch = |actual, diagnostics| TreeCopyMismatch {
        expected: record.digest.value.clone(),
        actual,
        diagnostics,
    };
    match validate_workflow_tree(&copy) {
        Ok(tree)
            if tree.digest.algorithm.as_str() == record.digest.algorithm
                && tree.digest.value == record.digest.value =>
        {
            Ok(tree)
        }
        Ok(tree) => Err(mismatch(Some(tree.digest.value), Vec::new())),
        Err(diagnostics) => Err(mismatch(None, diagnostics)),
    }
}

fn tree_copy_directory(run_directory: &Path, record: &ManifestTreeV1) -> PathBuf {
    run_directory
        .join(WORKFLOW_DIRECTORY)
        .join(&record.relative_directory)
}

fn retained_tree_copy<'a>(
    run_directory: &Path,
    record: Option<&'a ManifestTreeV1>,
) -> Option<(PathBuf, &'a ManifestTreeV1)> {
    record.map(|record| (tree_copy_directory(run_directory, record), record))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestSourceFileV1 {
    path: String,
    #[serde(flatten)]
    file: ManifestFileV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestFileV1 {
    ordinal: u64,
    relative_file: String,
    size_bytes: u64,
    digest: DigestV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ManifestInputV1 {
    Text {
        #[serde(flatten)]
        file: ManifestFileV1,
    },
    Json {
        #[serde(flatten)]
        file: ManifestFileV1,
    },
    File {
        #[serde(rename = "mediaType")]
        media_type: String,
        #[serde(flatten)]
        file: ManifestFileV1,
    },
    Attachments {
        items: Vec<ManifestAttachmentV1>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestAttachmentV1 {
    media_type: String,
    #[serde(flatten)]
    file: ManifestFileV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct LocalRunStateV1 {
    pub(super) schema_version: u8,
    pub(super) local_run_id: String,
    pub(super) revision: u64,
    pub(super) current_attempt_number: u64,
    pub(super) attempts: Vec<LocalAttemptV1>,
    diagnostics: Vec<LocalDiagnosticV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct LocalAttemptV1 {
    attempt_id: String,
    pub(super) attempt_number: u64,
    pub(super) trigger: AttemptTriggerV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) prior_attempt_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) definition: Option<AttemptDefinitionV1>,
    pub(super) state: AttemptStateV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) continuation: Option<super::publication::ContinuationRecordV1>,
    /// The backward or limited edge a `continued` attempt took (REQ-WFEXEC-64,
    /// REQ-CONTINUE-78); no other attempt has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) taken_edge: Option<super::publication::ContinuationEdgeV1>,
    /// The run a successor imported this attempt from (REQ-LOCALRUN-195); an attempt the
    /// run made itself has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) imported: Option<ImportedAttemptV1>,
    pub(super) execution_root: String,
    pub(super) created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) settled_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) settlement_snapshot: Option<WorkspaceSnapshotV1>,
    owner: AttemptOwnerV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cancellation: Option<AttemptCancellationV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) force_abort: Option<super::runtime::ForceAbortEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interruption: Option<AttemptInterruptionV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rejection: Option<AttemptRejectionV1>,
    pub(super) progress: AttemptProgressV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) finalization: Option<AttemptFinalizationV1>,
    process_guards: Vec<ProcessGuardV1>,
    pub(super) result: AttemptResultV1,
}

/// Where a successor's imported attempt came from (REQ-LOCALRUN-195): the predecessor's
/// run id.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ImportedAttemptV1 {
    pub(super) run: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AttemptTriggerV1 {
    Initial,
    ExplicitRetry,
    Continuation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AttemptStateV1 {
    Created,
    Running,
    Cancelling,
    Succeeded,
    WorkflowFailed,
    Cancelled,
    Interrupted,
    Rejected,
    /// The attempt ended on a wait the run continues from (REQ-WFEXEC-61).
    Waiting,
    /// The attempt took a backward or limited edge the run continues from (REQ-WFEXEC-64).
    Continued,
}

impl AttemptStateV1 {
    pub(super) fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::WorkflowFailed
                | Self::Cancelled
                | Self::Interrupted
                | Self::Rejected
                | Self::Waiting
                | Self::Continued
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttemptOwnerV1 {
    owner_nonce: String,
    execution_host: ExecutionHostV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecutionHostV1 {
    kind: ExecutionHostKindV1,
    value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionHostKindV1 {
    HostBoot,
    IsolationInstance,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptCancellationV1 {
    pub(super) reason: CancellationReasonV1,
    pub(super) requested_at: String,
    pub(super) force_stop_deadline: String,
    workflow_confirmed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttemptInterruptionV1 {
    cause: InterruptionCauseV1,
    execution_may_have_started: bool,
    cancellation_requested: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InterruptionCauseV1 {
    ExecutorShutdown,
    ExecutionLeaseExpired,
    ExecutionOwnerLost,
    ExecutorFault,
    StatePersistenceFailure,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttemptRejectionV1 {
    code: RejectionCodeV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RejectionCodeV1 {
    ImmutableSpecificationUnusable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptProgressV1 {
    accepted_occurrence_ordinal: u64,
    last_transition_sequence: u64,
    pub(super) steps: Vec<AttemptStepV1>,
    outstanding_actions: Vec<OutstandingActionV1>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) invocations: Vec<DurableInvocationV1>,
    pub(super) accounting: DurableInvocationAccountingV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptStepV1 {
    pub(super) id: String,
    pub(super) role: AttemptNodeRoleV1,
    pub(super) failure_policy: FailurePolicy,
    pub(super) state: AttemptStepStateV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) outputs: Option<Vec<RetainedOutputV1>>,
    #[serde(
        default,
        deserialize_with = "super::evidence::deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(super) detail: Option<NodeDetail>,
    /// A failed node's classified facts, recorded when it failed (REQ-LOCALRUN-198).
    #[serde(
        default,
        deserialize_with = "super::evidence::deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(super) diagnostic: Option<FailureDiagnosticV1>,
    /// The time of the commit that first moved the node out of `pending` in this attempt,
    /// kept through recovery executions and after the node ends (REQ-LOCALRUN-201).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) recovery: Option<DurableStepRecoveryV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RetainedCarrierV1 {
    relative_path: String,
    media_type: String,
    size_bytes: u64,
    digest: DigestV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum RetainedOutputV1 {
    Text {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        producer: Option<super::runtime::OutputProducer>,
        carrier: RetainedCarrierV1,
    },
    Json {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        producer: Option<super::runtime::OutputProducer>,
        carrier: RetainedCarrierV1,
    },
    File {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        producer: Option<super::runtime::OutputProducer>,
        #[serde(rename = "mediaType")]
        media_type: String,
        carrier: RetainedCarrierV1,
    },
    GitBranch {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        producer: Option<super::runtime::OutputProducer>,
        #[serde(rename = "artifactVersion")]
        artifact_version: u8,
        #[serde(rename = "objectFormat")]
        object_format: String,
        #[serde(rename = "baseOid")]
        base_oid: String,
        #[serde(rename = "headOid")]
        head_oid: String,
        #[serde(rename = "treeOid")]
        tree_oid: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        carrier: Option<RetainedCarrierV1>,
    },
}

impl RetainedOutputV1 {
    pub(super) fn name(&self) -> &str {
        match self {
            Self::Text { name, .. }
            | Self::Json { name, .. }
            | Self::File { name, .. }
            | Self::GitBranch { name, .. } => name,
        }
    }

    pub(super) fn producer(&self) -> Option<&super::runtime::OutputProducer> {
        match self {
            Self::Text { producer, .. }
            | Self::Json { producer, .. }
            | Self::File { producer, .. }
            | Self::GitBranch { producer, .. } => producer.as_ref(),
        }
    }

    fn set_producer(&mut self, value: super::runtime::OutputProducer) {
        match self {
            Self::Text { producer, .. }
            | Self::Json { producer, .. }
            | Self::File { producer, .. }
            | Self::GitBranch { producer, .. } => *producer = Some(value),
        }
    }

    fn carrier(&self) -> Option<&RetainedCarrierV1> {
        match self {
            Self::Text { carrier, .. }
            | Self::Json { carrier, .. }
            | Self::File { carrier, .. } => Some(carrier),
            Self::GitBranch { carrier, .. } => carrier.as_ref(),
        }
    }

    fn matches_export(&self, export: &super::publication::ExportV1) -> bool {
        match (self, export) {
            (Self::Text { carrier, .. }, export) => retained_carrier_matches_available_export(
                carrier,
                "text",
                "text/plain; charset=utf-8",
                export,
            ),
            (Self::Json { carrier, .. }, export) => retained_carrier_matches_available_export(
                carrier,
                "json",
                "application/json",
                export,
            ),
            (
                Self::File {
                    media_type,
                    carrier,
                    ..
                },
                export,
            ) => retained_carrier_matches_available_export(carrier, "file", media_type, export),
            (
                Self::GitBranch {
                    artifact_version,
                    object_format,
                    base_oid,
                    head_oid,
                    tree_oid,
                    carrier,
                    ..
                },
                super::publication::ExportV1::GitBranch {
                    artifact_version: export_artifact_version,
                    object_format: export_object_format,
                    base_oid: export_base_oid,
                    head_oid: export_head_oid,
                    tree_oid: export_tree_oid,
                    carrier: export_carrier,
                    ..
                },
            ) => {
                artifact_version == export_artifact_version
                    && object_format == export_object_format
                    && base_oid == export_base_oid
                    && head_oid == export_head_oid
                    && tree_oid == export_tree_oid
                    && match (carrier, export_carrier) {
                        (None, None) => true,
                        (Some(retained), Some(export)) => retained_carrier_matches_metadata(
                            retained,
                            &export.media_type,
                            export.size_bytes,
                            &export.digest,
                        ),
                        (None, Some(_)) | (Some(_), None) => false,
                    }
            }
            (
                Self::GitBranch { .. },
                super::publication::ExportV1::Available { .. }
                | super::publication::ExportV1::Unavailable { .. },
            ) => false,
        }
    }
}

fn retained_carrier_matches_available_export(
    retained: &RetainedCarrierV1,
    expected_kind: &str,
    expected_media_type: &str,
    export: &super::publication::ExportV1,
) -> bool {
    let super::publication::ExportV1::Available {
        kind,
        media_type,
        size_bytes,
        digest,
        ..
    } = export
    else {
        return false;
    };
    kind == expected_kind
        && media_type == expected_media_type
        && retained_carrier_matches_metadata(retained, media_type, *size_bytes, digest)
}

fn retained_carrier_matches_metadata(
    retained: &RetainedCarrierV1,
    media_type: &str,
    size_bytes: u64,
    digest: &super::publication::DigestV1,
) -> bool {
    retained.media_type == media_type
        && retained.size_bytes == size_bytes
        && retained.digest.algorithm == digest.algorithm
        && retained.digest.value == digest.value
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableStepRecoveryV1 {
    pub(super) schema_version: u8,
    pub(super) configured_retries: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) handler_kind: Option<DurableRecoveryHandlerKindV1>,
    pub(super) rounds: Vec<DurableRecoveryRoundV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) active: Option<DurableRecoveryActiveV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) termination: Option<super::publication::RecoveryTerminationV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum DurableRecoveryHandlerKindV1 {
    Cmd,
    Agent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableRecoveryRoundV1 {
    pub(super) number: u8,
    pub(super) failed_execution: super::publication::RecoveryFailedExecutionV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) handler: Option<super::publication::RecoveryHandlerSummaryV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableRecoveryActiveV1 {
    pub(super) role: super::publication::RecoveryInvocationRoleV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) target_execution: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) recovery_round: Option<u8>,
    pub(super) invocation_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) handler_state: Option<DurableRecoveryHandlerStateV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) decision: Option<super::publication::RecoveryHandlerOutcomeV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum DurableRecoveryHandlerStateV1 {
    Starting,
    Running,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableInvocationV1 {
    pub invocation_id: u64,
    pub step_id: String,
    pub(crate) node_role: AttemptNodeRoleV1,
    pub role: super::publication::RecoveryInvocationRoleV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_execution: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_round: Option<u8>,
    pub state: DurableInvocationStateV1,
    pub started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    pub usage: super::publication::RecoveryInvocationUsageV1,
    pub diagnostics: Vec<DurableInvocationDiagnosticV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic_reference: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableInvocationStateV1 {
    Active,
    Settled,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DurableInvocationDiagnosticV1 {
    pub kind: super::publication::RecoveryDiagnosticKindV1,
    pub reference: String,
    pub retained_bytes: u64,
    pub discarded_bytes: u64,
    pub truncated: bool,
    pub fully_drained: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableInvocationAccountingV1 {
    pub(super) maximum_invocations: u64,
    pub(super) observed_invocations: u64,
    pub(super) settled_invocations: u64,
    pub(super) input_tokens: u64,
    pub(super) output_tokens: u64,
    pub(super) retained_diagnostic_bytes: u64,
    pub(super) discarded_diagnostic_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AttemptNodeRoleV1 {
    Step,
    Finalizer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum AttemptFinalizationV1 {
    Progress(AttemptFinalizationProgressV1),
    Complete(AttemptFinalizationCompleteV1),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptFinalizationProgressV1 {
    complete: bool,
    trigger: FinalizationTriggerV1,
    finalizers: Vec<AttemptStepV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cancellation: Option<DurableFinalizationCancellationV1>,
    force_abort: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AttemptFinalizationCompleteV1 {
    pub(super) complete: bool,
    pub(super) trigger: FinalizationTriggerV1,
    pub(super) finalizers: Vec<DurableFinalizerV1>,
    pub(super) issues: Vec<DurableFinalizationIssueV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cancellation: Option<DurableFinalizationCancellationV1>,
    pub(super) force_abort: bool,
}

// Completed finalizers and live attempt nodes remain distinct durable envelopes even
// though both carry the same canonical detail union.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableFinalizerV1 {
    pub(super) id: String,
    pub(super) role: AttemptNodeRoleV1,
    pub(super) failure_policy: FailurePolicy,
    pub(super) state: AttemptStepStateV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) outputs: Option<Vec<RetainedOutputV1>>,
    #[serde(
        default,
        deserialize_with = "super::evidence::deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(super) detail: Option<NodeDetail>,
    /// A failed node's classified facts, recorded when it failed (REQ-LOCALRUN-198).
    #[serde(
        default,
        deserialize_with = "super::evidence::deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub(super) diagnostic: Option<FailureDiagnosticV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableFinalizationIssueV1 {
    pub(super) finalizer_id: String,
    pub(super) impact: FailurePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DurableFinalizationCancellationV1 {
    pub(super) reason: CancellationReasonV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) force_stop_deadline: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AttemptStepStateV1 {
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

impl AttemptStepStateV1 {
    /// Whether a node in this state has started and not ended: the states that record a
    /// node's `startedAt` (REQ-LOCALRUN-201) and that `um workflow run show` reports as a
    /// running child's in-flight nodes (REQ-LOCALRUN-189).
    pub(super) const fn is_in_flight(self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Running | Self::CapturingOutputs | Self::Cancelling
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OutstandingActionV1 {
    action_id: u64,
    kind: OutstandingActionKindV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_role: Option<AttemptNodeRoleV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_execution: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery_round: Option<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OutstandingActionKindV1 {
    StartStep,
    StartRecoveryHandler,
    CaptureOutputs,
    CancelStep,
    FinishRun,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProcessGuardV1 {
    guard_id: String,
    action_id: u64,
    step_id: String,
    node_role: AttemptNodeRoleV1,
    state: ProcessGuardStateV1,
    execution_host: ExecutionHostV1,
    process_group_id: i64,
    liveness: ProcessLivenessV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProcessGuardStateV1 {
    Prepared,
    Released,
    Quiesced,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProcessLivenessV1 {
    kind: ProcessLivenessKindV1,
    value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProcessLivenessKindV1 {
    LeaderStartIdentity,
    GuardHandleIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum AttemptResultV1 {
    NotPublished {
        reason: ResultAbsentReasonV1,
    },
    Published {
        #[serde(rename = "relativeDirectory")]
        relative_directory: String,
    },
    PublicationFailed {
        phase: PublicationFailurePhaseV1,
        #[serde(rename = "resultInvariant", skip_serializing_if = "Option::is_none")]
        result_invariant: Option<RunResultInvariant>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ResultAbsentReasonV1 {
    AttemptNonterminal,
    PublicationPending,
    Interrupted,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationFailurePhaseV1 {
    ExportCopy,
    Serialization,
    Close,
    Verification,
    Rename,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LocalDiagnosticV1 {
    sequence: u64,
    attempt_number: u64,
    code: DiagnosticCodeV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    action_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    guard_id: Option<String>,
    /// A `state_persistence_failure`'s class (REQ-LOCALRUN-205).
    #[serde(skip_serializing_if = "Option::is_none")]
    cause: Option<CommitFailureClass>,
    /// A `state_persistence_failure`'s operating-system error number (REQ-LOCALRUN-205).
    #[serde(skip_serializing_if = "Option::is_none")]
    os_error: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DiagnosticCodeV1 {
    StaleOccurrence,
    StatePersistenceFailure,
    TransitionCapacityExceeded,
    ResultPublicationFailure,
    PrivateCleanupFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalStatusErrorCode {
    RunDirectoryUnavailable,
    RunDirectoryInvalid,
    RecoverySchemaUnsupported,
    LockQueryFailed,
    StatusSnapshotUnstable,
}

impl LocalStatusErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunDirectoryUnavailable => "run_directory_unavailable",
            Self::RunDirectoryInvalid => "run_directory_invalid",
            Self::RecoverySchemaUnsupported => "recovery_schema_unsupported",
            Self::LockQueryFailed => "lock_query_failed",
            Self::StatusSnapshotUnstable => "status_snapshot_unstable",
        }
    }

    pub const fn message(self) -> &'static str {
        match self {
            Self::RunDirectoryUnavailable => "The run directory is unavailable.",
            Self::RunDirectoryInvalid => "The run directory does not contain valid V1 state.",
            Self::RecoverySchemaUnsupported => {
                "The run uses an unsupported recovery summary schema version."
            }
            Self::LockQueryFailed => "The run lock could not be inspected.",
            Self::StatusSnapshotUnstable => {
                "The run state changed too quickly to obtain a stable snapshot."
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalStatusError {
    pub code: LocalStatusErrorCode,
    pub run_directory: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnershipUnprovenReason {
    ExecutionHostIdentityUnavailable,
    ProcessIdentityInspectionUnavailable,
}

impl OwnershipUnprovenReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionHostIdentityUnavailable => "execution_host_identity_unavailable",
            Self::ProcessIdentityInspectionUnavailable => "process_identity_inspection_unavailable",
        }
    }

    pub const fn remedy(self) -> &'static str {
        match self {
            Self::ExecutionHostIdentityUnavailable => {
                "restore execution-host identity inspection or restart the host boundary"
            }
            Self::ProcessIdentityInspectionUnavailable => {
                "restore process identity inspection or restart the host boundary"
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalRecoveryStatus {
    Active,
    Settled,
    Abandoned,
    OwnershipUnproven {
        guard_ids: Vec<String>,
        reason: OwnershipUnprovenReason,
    },
}

impl LocalRecoveryStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Settled => "settled",
            Self::Abandoned => "abandoned",
            Self::OwnershipUnproven { .. } => "ownership_unproven",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryIneligibilityReason {
    RunLocked,
    LatestAttemptSucceeded,
    LatestAttemptRejected,
    OwnershipUnproven,
    /// The run record carries `successor`: a restart superseded the run, whose last attempt
    /// may still read `waiting` (REQ-LOCALRUN-196). Published as the continuation code
    /// `continuation_disposition_ineligible` (REQ-CONTINUE-29, REQ-LOCALRUN-129).
    RunSuperseded,
}

impl RetryIneligibilityReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunLocked => "run_locked",
            Self::LatestAttemptSucceeded => "latest_attempt_succeeded",
            Self::LatestAttemptRejected => "latest_attempt_rejected",
            Self::OwnershipUnproven => "ownership_unproven",
            Self::RunSuperseded => "continuation_disposition_ineligible",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalRetryEligibility {
    Eligible,
    Ineligible(RetryIneligibilityReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalRetryRejection {
    run_directory: PathBuf,
    attempt_number: u64,
    reason: RetryIneligibilityReason,
    guard_ids: Vec<String>,
    ownership_reason: Option<OwnershipUnprovenReason>,
    successor: Option<String>,
}

impl LocalRetryRejection {
    pub fn run_directory(&self) -> &Path {
        &self.run_directory
    }

    pub(crate) const fn attempt_number(&self) -> u64 {
        self.attempt_number
    }

    pub const fn reason(&self) -> RetryIneligibilityReason {
        self.reason
    }

    pub(crate) fn guard_ids(&self) -> &[String] {
        &self.guard_ids
    }

    pub(crate) const fn ownership_reason(&self) -> Option<OwnershipUnprovenReason> {
        self.ownership_reason
    }

    /// The successor's run id when the run record carries one (REQ-LOCALRUN-196).
    pub fn successor(&self) -> Option<&str> {
        self.successor.as_deref()
    }
}

pub enum LocalRetryOpen {
    Acquired(Box<PendingLocalRetry>),
    Rejected(LocalRetryRejection),
}

pub struct PendingLocalRetry {
    normalized: PathBuf,
    root: Arc<OwnedFd>,
    lock: File,
    state: Arc<StateStore>,
    workflow: ResolvedWorkflow,
    inputs: ResolvedInputs,
    maximum_parallel_steps: usize,
    definition: AttemptDefinitionV1,
    git_baseline: Option<LocalGitBaseline>,
    tree: Option<ManifestTreeV1>,
}

impl PendingLocalRetry {
    pub fn run_directory(&self) -> &Path {
        &self.normalized
    }

    /// `LocalAttemptOwner::update_run_record` under the retry's run lock.
    pub fn update_run_record(
        &self,
        state: RunState,
        attempts: u64,
        outcome: Option<&str>,
    ) -> Result<(), LocalRunDirectoryError> {
        update_run_record(&self.root, state, attempts, outcome)
    }

    /// The run's tree copy and its manifest record, for a run admitted from a tree
    /// (REQ-LOCALRUN-170).
    pub fn tree_copy(&self) -> Option<(PathBuf, &ManifestTreeV1)> {
        retained_tree_copy(&self.normalized, self.tree.as_ref())
    }

    /// `verify_tree_copy` for a run admitted from a tree (REQ-LOCALRUN-171).
    pub fn verify_tree_copy(&self) -> Option<Result<ValidatedTree, TreeCopyMismatch>> {
        self.tree
            .as_ref()
            .map(|record| verify_tree_copy(&self.normalized, record))
    }

    pub fn execution_specification(&self) -> (&ResolvedWorkflow, &ResolvedInputs, usize) {
        (&self.workflow, &self.inputs, self.maximum_parallel_steps)
    }

    pub fn git_baseline(&self) -> Option<&LocalGitBaseline> {
        self.git_baseline.as_ref()
    }

    pub fn reused_execution_root_attempts(
        &self,
        admitted: &AdmittedWorkflow,
    ) -> Result<Vec<u64>, LocalRunDirectoryError> {
        let execution_root = admitted
            .execution()
            .root()
            .to_str()
            .ok_or(LocalRunDirectoryError::InvalidPath)?;
        let state = lock_state(&self.state.current)?;
        Ok(state
            .attempts
            .iter()
            .filter(|attempt| attempt.execution_root == execution_root)
            .map(|attempt| attempt.attempt_number)
            .collect())
    }

    pub fn begin(
        self,
        admitted: &AdmittedWorkflow,
    ) -> Result<LocalAttemptOwner, LocalRetryBeginError> {
        begin_local_retry(self, admitted, &SystemLocalRecoveryAuthority)
    }
}

pub enum LocalContinuationOpen {
    Acquired(Box<PendingLocalContinuation>),
    Rejected(LocalRetryRejection),
}

/// The definition a continuation's attempt runs under (REQ-CONTINUE-11, REQ-CONTINUE-79).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuationDefinitionMode {
    /// The definition that governed the prior attempt, which the attempt points at.
    Inherited,
    /// A replacement named for this attempt alone, retained under the attempt's directory.
    Replaced,
    /// The run's own definition: a successor's restart continuation, whose prior attempt is
    /// imported and ran under its predecessor's definition.
    Run,
}

/// What a successor carries from its predecessor beyond the imported attempts and controls
/// (REQ-LOCALRUN-195, REQ-LOCALRUN-197).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SuccessorCarry {
    /// Nothing more: the successor's record reads `running` and its first own attempt
    /// begins at once (`um workflow run restart`).
    Continue,
    /// The predecessor's root wait record, copied as the successor's own: the successor's
    /// record reads `waiting` and its first own attempt is the continuation a control
    /// recorded on it admits (`um workflow run upgrade`).
    Wait,
}

/// One piece of a waiting run's carried state that does not resolve against another
/// workflow (REQ-LOCALRUN-197): `code` is `position_unresolved`, `wait_target_unresolved`,
/// `outcome_unrouted` or `items_unresolved`, and `message` names the step and the outcome
/// or element.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CarryMiss {
    pub code: &'static str,
    pub message: String,
}

/// What `PendingLocalContinuation::failed_attempt_restart` finds (REQ-CONTINUE-81).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FailedAttemptRestart {
    /// The latest attempt did not end `failed`.
    NotFailed,
    /// The ordinary steps the failed attempt left unfinished, in presentation order.
    Steps(Vec<String>),
    /// The failed attempt finished every ordinary step; only a finalizer failed.
    FinalizerOnly,
}

pub struct PendingLocalContinuation {
    normalized: PathBuf,
    root: OwnedFd,
    lock: File,
    run: LocalRunV1,
    state: LocalRunStateV1,
    prior_execution_root: PathBuf,
    prior_workflow: ResolvedWorkflow,
    initial_workflow: ResolvedWorkflow,
    inputs: ResolvedInputs,
    maximum_parallel_steps: usize,
    quiescence: super::publication::ContinuationQuiescenceV1,
    tree: Option<ManifestTreeV1>,
    /// Whether `begin` keeps the root wait record it removes (`keep_resolved_wait`).
    keep_resolved_wait: bool,
}

impl PendingLocalContinuation {
    pub fn run_directory(&self) -> &Path {
        &self.normalized
    }

    /// Declares that the attempt `begin` starts continues from the run's wait, which a
    /// control or an external event resolved (REQ-CONTINUE-75, REQ-CONTINUE-80): `begin`
    /// reads the root wait record before removing it, the returned owner holds it
    /// (`LocalAttemptOwner::resumed_wait_record`) and the attempt's execution seed carries it,
    /// so an interruption before any step of the attempt has started keeps that wait
    /// (REQ-WFEXEC-72, INV-ENGINE-32). `begin` then refuses a run without a root wait record.
    pub fn keep_resolved_wait(&mut self) {
        self.keep_resolved_wait = true;
    }

    /// `LocalAttemptOwner::update_run_record` under the continuation's run lock.
    pub fn update_run_record(
        &self,
        state: RunState,
        attempts: u64,
        outcome: Option<&str>,
    ) -> Result<(), LocalRunDirectoryError> {
        update_run_record(&self.root, state, attempts, outcome)
    }

    /// The run's root wait record (REQ-LOCALRUN-178), or `None` when the run does not wait.
    pub fn wait(&self) -> Result<Option<WaitRecord>, LocalRunDirectoryError> {
        read_optional_wait_record(&self.root)
    }

    /// The first control, in ordinal order, that resolves `wait` for this run, read under
    /// the run lock (REQ-LOCALRUN-197): a command that inspected the run before locking it
    /// asks again here, since a control recorded in between resolves nothing once copied to a
    /// successor (REQ-LOCALRUN-195). `None` when no control does, for a rule on a source other
    /// than `um` (controls never resolve an external wait, REQ-WFEXEC-65), as for a rule
    /// outside the matcher's grammar, and for a run without a record.
    pub fn resolving_control(
        &self,
        wait: &WaitRecord,
    ) -> Result<Option<ControlEvent>, LocalRunDirectoryError> {
        let Some(record) = read_optional_run_record(&self.root)? else {
            return Ok(None);
        };
        Ok(control_resolution(&self.normalized, &record.id, wait)?
            .ok()
            .flatten())
    }

    /// `end_waiting(WITHDRAWN_OUTCOME)`.
    pub fn withdraw(self) -> Result<(), LocalRunDirectoryError> {
        self.end_waiting(WITHDRAWN_OUTCOME)
    }

    /// Ends a waiting run without another attempt (REQ-CONTINUE-75): the run record reads
    /// `exited` with `outcome` (`withdrawn` or `canceled`) and its attempt count unchanged,
    /// the root wait record is removed, and the run lock is released. A run that does not
    /// wait is `StateConflict` and left as it is.
    pub fn end_waiting(self, outcome: &str) -> Result<(), LocalRunDirectoryError> {
        verify_retry_lock_identity(&self.root, &self.lock)?;
        if read_optional_wait_record(&self.root)?.is_none() {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        if let Some(record) = read_optional_run_record(&self.root)? {
            update_run_record(&self.root, RunState::Exited, record.attempts, Some(outcome))?;
        }
        remove_root_wait_record(&self.root)
    }

    /// Supersedes this waiting run by `successor`, a run that already exists
    /// (REQ-LOCALRUN-175, INV-ENGINE-26): the run record reads `exited` with the outcome
    /// `superseded`, `successor` set, its attempt count unchanged and its `position` cleared,
    /// the root wait record is removed, and the run lock is released. No attempt, control or
    /// mirror record is written. A run that does not wait, or has no run record, is
    /// `StateConflict` and left as it is.
    pub fn supersede(self, successor: &str) -> Result<(), LocalRunDirectoryError> {
        verify_retry_lock_identity(&self.root, &self.lock)?;
        if !is_run_id(successor) {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        if read_optional_wait_record(&self.root)?.is_none()
            || read_optional_run_record(&self.root)?.is_none()
        {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        rewrite_run_record(&self.root, |record| {
            record.state = RunState::Exited;
            record.outcome = Some(SUPERSEDED_OUTCOME.to_owned());
            record.successor = Some(successor.to_owned());
            record.position = Vec::new();
            Ok(())
        })?;
        remove_root_wait_record(&self.root)
    }

    /// The run record (REQ-WFDEF-4), or `None` for a run admitted from a workflow file's
    /// closure.
    pub fn record(&self) -> Result<Option<WorkflowRunRecord>, LocalRunDirectoryError> {
        read_optional_run_record(&self.root)
    }

    /// Ends this run without an attempt because its predecessor continued or exited without
    /// being superseded by it (REQ-LOCALRUN-196): the run record reads `exited` `withdrawn`
    /// with its attempt count unchanged, the root wait record is removed when present, and
    /// the run lock is released. No attempt, control or mirror record is written.
    pub fn abandon(self) -> Result<(), LocalRunDirectoryError> {
        verify_retry_lock_identity(&self.root, &self.lock)?;
        if let Some(record) = read_optional_run_record(&self.root)? {
            update_run_record(
                &self.root,
                RunState::Exited,
                record.attempts,
                Some(WITHDRAWN_OUTCOME),
            )?;
        }
        remove_root_wait_record(&self.root)
    }

    /// The run the prior attempt was recorded under when this run is a successor whose first
    /// own attempt has yet to begin (REQ-LOCALRUN-195, REQ-CONTINUE-79); `None` otherwise.
    pub fn imported_prior(&self) -> Option<&str> {
        self.state
            .attempts
            .last()
            .and_then(|prior| prior.imported.as_ref())
            .map(|imported| imported.run.as_str())
    }

    /// The retained child outcome of each `workflow` step the last attempt succeeded or
    /// inherited (REQ-WFEXEC-58, REQ-LOCALRUN-197), by step: its `text` output
    /// `CHILD_OUTCOME_OUTPUT`, read through its carrier and checked against it. A step that
    /// retained no outcome is absent. Nothing is written.
    pub fn inherited_child_outcomes(
        &self,
    ) -> Result<BTreeMap<String, String>, LocalRunDirectoryError> {
        let mut outcomes = BTreeMap::new();
        let Some(prior) = self.state.attempts.last() else {
            return Ok(outcomes);
        };
        for step in &prior.progress.steps {
            let Some(output) =
                completed_step_output(prior, step, super::validation::CHILD_OUTCOME_OUTPUT)
                    .filter(|output| matches!(output, RetainedOutputV1::Text { .. }))
            else {
                continue;
            };
            let bytes = read_retained_carrier(&self.root, &self.state, &output)?;
            let outcome =
                String::from_utf8(bytes).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            outcomes.insert(step.id.clone(), outcome);
        }
        Ok(outcomes)
    }

    /// The length of the list the `forEach` step `step` of `workflow` fans out over, as this
    /// run would read it (REQ-WFFORMAT-98, REQ-LOCALRUN-197): a `json` root input from
    /// `inputs`, or a `json` export of an earlier `workflow` step retained by the last
    /// attempt, read through its carrier. `None` when `step` is not a `forEach` step, or its
    /// list is absent or not an array. Nothing is written.
    pub fn inherited_items_length(
        &self,
        workflow: &ResolvedWorkflow,
        step: &str,
        inputs: &ResolvedInputs,
    ) -> Result<Option<u64>, LocalRunDirectoryError> {
        let Some(super::validated::ValidatedStep::Workflow(
            super::validated::ValidatedWorkflowStep {
                for_each: Some(for_each),
                ..
            },
        )) = workflow.definition.steps.get(step)
        else {
            return Ok(None);
        };
        let length = match &for_each.items.source {
            super::validated::ResolvedValueSource::Input(name) => match inputs.get(name) {
                Some(ResolvedInput::Json(json)) => json.value().as_array().map(Vec::len),
                _ => None,
            },
            super::validated::ResolvedValueSource::Output(source) => {
                let output = self.state.attempts.last().and_then(|prior| {
                    prior
                        .progress
                        .steps
                        .iter()
                        .find(|producer| producer.id == source.node.id)
                        .and_then(|producer| completed_step_output(prior, producer, &source.output))
                        .filter(|output| matches!(output, RetainedOutputV1::Json { .. }))
                });
                match output {
                    Some(output) => {
                        let bytes = read_retained_carrier(&self.root, &self.state, &output)?;
                        serde_json::from_slice::<Value>(&bytes)
                            .map_err(|_| LocalRunDirectoryError::StateInvalid)?
                            .as_array()
                            .map(Vec::len)
                    }
                    None => None,
                }
            }
            _ => None,
        };
        length
            .map(u64::try_from)
            .transpose()
            .map_err(|_| LocalRunDirectoryError::StateInvalid)
    }

    /// What of this waiting run's carried state does not resolve against `workflow`, another
    /// revision's, with `inputs` the run's inputs projected onto it (REQ-LOCALRUN-197): in
    /// this order, a record `position` naming no `workflow` step, or an element of a step
    /// that is not `forEach` (`position_unresolved`); the root wait record's `then`, or its
    /// `onHandBack`, naming no step (`wait_target_unresolved`); the retained outcome of a
    /// `workflow` step the continuation from either target would inherit that is not a key of
    /// its `on` edges (`outcome_unrouted`); and a fan-out wait's element beyond the list its
    /// step now reads, or under a step that is not `forEach` (`items_unresolved`). One miss
    /// per code and step; none when everything resolves. A run without a root wait record is
    /// `StateConflict`. Nothing is written.
    pub fn resolve_carry(
        &self,
        workflow: &ResolvedWorkflow,
        inputs: &ResolvedInputs,
    ) -> Result<Vec<CarryMiss>, LocalRunDirectoryError> {
        let wait = self.wait()?.ok_or(LocalRunDirectoryError::StateConflict)?;
        let record = read_run_record(&self.root)?;
        let steps = &workflow.definition.steps;
        let workflow_step = |id: &str| match steps.get(id) {
            Some(super::validated::ValidatedStep::Workflow(step)) => Some(step),
            _ => None,
        };
        let mut found = BTreeSet::new();
        let mut misses = Vec::new();
        let mut miss = |code: &'static str, step: &str, message: String| {
            if found.insert((code, step.to_owned())) {
                misses.push(CarryMiss { code, message });
            }
        };
        if let Some(step) = record.position.first() {
            let step = step.as_str().unwrap_or_default();
            let element = record
                .position
                .get(1)
                .and_then(|element| element.get("item"))
                .and_then(Value::as_u64);
            match (workflow_step(step), element) {
                (None, _) => miss(
                    "position_unresolved",
                    step,
                    format!(
                        "The run's position names step `{step}`, which is not a workflow step of the current revision."
                    ),
                ),
                (Some(defined), Some(item)) if defined.for_each.is_none() => miss(
                    "position_unresolved",
                    step,
                    format!(
                        "The run's position names element {item} of step `{step}`, which no longer fans out in the current revision."
                    ),
                ),
                _ => {}
            }
        }
        let targets = std::iter::once(&wait.then)
            .chain(wait.on_hand_back.as_ref())
            .collect::<Vec<_>>();
        for target in &targets {
            if !steps.contains_key(target.as_str()) {
                miss(
                    "wait_target_unresolved",
                    target,
                    format!(
                        "The wait continues at step `{target}`, which the current revision does not define."
                    ),
                );
            }
        }
        let outcomes = self.inherited_child_outcomes()?;
        for target in &targets {
            let Some(reexecuted) =
                self.candidate_reexecuted_steps(workflow, std::slice::from_ref(*target))
            else {
                continue;
            };
            for (step, outcome) in &outcomes {
                if reexecuted.contains(step) {
                    continue;
                }
                if workflow_step(step).is_some_and(|defined| !defined.edges.contains_key(outcome)) {
                    miss(
                        "outcome_unrouted",
                        step,
                        format!(
                            "Inherited step `{step}` ended `{outcome}`, which the step's edges in the current revision do not route."
                        ),
                    );
                }
            }
        }
        if let Some((step, item)) = wait.fan_out_element()
            && self
                .inherited_items_length(workflow, step, inputs)?
                .is_none_or(|length| length < item)
        {
            miss(
                "items_unresolved",
                step,
                format!(
                    "The wait is at element {item} of step `{step}`, which the current revision does not fan out over that many elements."
                ),
            );
        }
        Ok(misses)
    }

    /// The edge the prior attempt took when it ended `continued` (REQ-WFEXEC-64,
    /// REQ-CONTINUE-78); `None` for any other prior attempt.
    pub fn continued_edge(&self) -> Option<&super::publication::ContinuationEdgeV1> {
        self.state
            .attempts
            .last()
            .filter(|prior| prior.state == AttemptStateV1::Continued)
            .and_then(|prior| prior.taken_edge.as_ref())
    }

    /// The steps the prior attempt left starting, running or capturing outputs when its
    /// process died (REQ-CONTINUE-76), in the prior attempt's order; none when the prior
    /// attempt is terminal.
    pub fn abandoned_steps(&self) -> Vec<String> {
        self.state
            .attempts
            .last()
            .filter(|prior| !prior.state.is_terminal())
            .into_iter()
            .flat_map(|prior| &prior.progress.steps)
            .filter(|step| {
                matches!(
                    step.state,
                    AttemptStepStateV1::Starting
                        | AttemptStepStateV1::Running
                        | AttemptStepStateV1::CapturingOutputs
                )
            })
            .map(|step| step.id.clone())
            .collect()
    }

    /// The run's tree copy and its manifest record, for a run admitted from a tree
    /// (REQ-LOCALRUN-170).
    pub fn tree_copy(&self) -> Option<(PathBuf, &ManifestTreeV1)> {
        retained_tree_copy(&self.normalized, self.tree.as_ref())
    }

    /// `verify_tree_copy` for a run admitted from a tree (REQ-LOCALRUN-171).
    pub fn verify_tree_copy(&self) -> Option<Result<ValidatedTree, TreeCopyMismatch>> {
        self.tree
            .as_ref()
            .map(|record| verify_tree_copy(&self.normalized, record))
    }

    pub fn prior_attempt_number(&self) -> u64 {
        self.state.current_attempt_number
    }

    pub fn prior_execution_root(&self) -> &Path {
        &self.prior_execution_root
    }

    pub fn maximum_parallel_steps(&self) -> usize {
        self.maximum_parallel_steps
    }

    pub fn git_baseline(&self) -> Option<LocalGitBaseline> {
        self.run.git_baseline.as_ref().and_then(local_git_baseline)
    }

    pub fn previous_definition(&self) -> &ResolvedWorkflow {
        &self.prior_workflow
    }

    /// The run's initial workflow and its resolved inputs, which the root's `mirror` block
    /// and its issue input are read from before `end_waiting` or `withdraw` consume the
    /// continuation.
    pub fn root_specification(&self) -> (&ResolvedWorkflow, &ResolvedInputs) {
        (&self.initial_workflow, &self.inputs)
    }

    pub fn definition_changes(
        &self,
        workflow: &ResolvedWorkflow,
        inherited: &[String],
    ) -> BTreeMap<String, bool> {
        inherited
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    super::continuation::definition_changed(workflow, &self.prior_workflow, id),
                )
            })
            .collect()
    }

    pub fn projected_inputs(
        &self,
        workflow: &ResolvedWorkflow,
    ) -> Result<ResolvedInputs, Vec<String>> {
        super::continuation::project_inputs(
            &workflow.definition,
            &self.initial_workflow.definition,
            &self.inputs,
        )
    }

    pub fn candidate_inputs(&self, workflow: &ResolvedWorkflow) -> ResolvedInputs {
        super::continuation::project_input_candidates(
            &workflow.definition,
            &self.initial_workflow.definition,
            &self.continuation_input_values(workflow),
        )
        .0
    }

    /// An optional null rejected by its original schema can only be the empty value
    /// admission supplied after checking inputs. Preserve that omission for an unchanged
    /// optional contract, so admission completes it again without validating the sentinel.
    fn continuation_input_values(&self, workflow: &ResolvedWorkflow) -> ResolvedInputs {
        let prior = &self.initial_workflow;
        let mut values = self.inputs.values().clone();
        values.retain(|name, value| {
            let ResolvedInput::Json(json) = value else {
                return true;
            };
            !(json.value().is_null()
                && prior.definition.optional_inputs.contains(name)
                && workflow.definition.optional_inputs.contains(name)
                && !prior.definition.input_defaults.contains_key(name)
                && !workflow.definition.input_defaults.contains_key(name)
                && prior.input_json_schema(name).is_some_and(|schema| {
                    !schema.is_valid(json.value())
                        && Some(schema) == workflow.input_json_schema(name)
                }))
        });
        ResolvedInputs::new(values)
    }

    /// A later admission rejection still settles an abandoned owner, without claiming
    /// a new attempt. Already terminal history remains byte-for-byte unchanged.
    pub fn settle_abandoned(self) -> Result<(), LocalRunDirectoryError> {
        settle_abandoned_snapshot(&self.root, &self.state)
    }

    fn state_store(&self) -> Result<Arc<StateStore>, LocalRunDirectoryError> {
        store_from_locked_snapshot(&self.root, &self.state)
    }

    pub fn validate_execution_root(
        &self,
        admitted: &AdmittedWorkflow,
    ) -> Result<(), LocalRunDirectoryError> {
        if paths_overlap(&self.normalized, admitted.execution().root())
            || admitted
                .execution()
                .root_identity()
                .contains_directory(&self.root)
                .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?
        {
            return Err(LocalRunDirectoryError::ExecutionRootOverlap);
        }
        Ok(())
    }

    pub fn quiescence_counts(&self) -> (u64, u64, u64) {
        (
            self.quiescence.groups_recorded,
            self.quiescence.groups_terminated,
            self.quiescence.groups_absent,
        )
    }

    pub fn verify_locked_history(&self) -> Result<(), LocalRunDirectoryError> {
        verify_retry_lock_identity(&self.root, &self.lock)?;
        validate_run_state_pair(&self.run, &self.state)
    }

    /// Graph-only selection remains available when inherited-state admission fails,
    /// so the caller can still collect independent candidate-admission diagnostics.
    pub fn candidate_reexecuted_steps(
        &self,
        workflow: &ResolvedWorkflow,
        from: &[String],
    ) -> Option<Vec<String>> {
        super::continuation::partition(&workflow.definition, from, &self.unfinished_steps())
            .ok()
            .map(|partition| partition.reexecuted)
    }

    /// The mapped inputs this run's continuation, admitted as `admitted`, would give the
    /// child of its `workflow` step `step` for element `item` under `controls`, read before
    /// the continuation begins (REQ-CONTINUE-82): the same mapper the step runs, with every
    /// referenced output read from the prior attempt's retained carrier as the continuation
    /// would inherit it. `None` when the step forms no request for that element.
    pub fn child_request_inputs(
        &self,
        admitted: &AdmittedWorkflow,
        step: &str,
        item: Option<u64>,
        controls: &super::step_runtime::ControlContext,
    ) -> Result<
        Option<BTreeMap<String, super::step_runtime::ChildInputValue>>,
        LocalRunDirectoryError,
    > {
        super::step_runtime::child_preflight_inputs(admitted, step, item, controls, |source| {
            self.retained_output_value(&source.node.id, &source.output)
        })
    }

    /// The `text` or `json` value of `producer`'s output `output` the prior attempt
    /// succeeded or inherited, read through its carrier and checked against it; `None` for
    /// any other output.
    fn retained_output_value(
        &self,
        producer: &str,
        output: &str,
    ) -> Result<Option<super::value::CapturedValue>, LocalRunDirectoryError> {
        let Some(prior) = self.state.attempts.last() else {
            return Ok(None);
        };
        let Some(retained) = prior
            .progress
            .steps
            .iter()
            .find(|step| step.id == producer)
            .and_then(|step| completed_step_output(prior, step, output))
        else {
            return Ok(None);
        };
        match &retained {
            RetainedOutputV1::Text { .. } => {
                let bytes = read_retained_carrier(&self.root, &self.state, &retained)?;
                let text =
                    String::from_utf8(bytes).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
                Ok(Some(super::value::CapturedValue::text(Arc::from(text))))
            }
            RetainedOutputV1::Json { .. } => {
                let bytes = read_retained_carrier(&self.root, &self.state, &retained)?;
                let value = serde_json::from_slice::<Value>(&bytes)
                    .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
                super::value::CapturedJson::unconstrained(Arc::new(value), Arc::from(bytes))
                    .map(super::value::CapturedValue::json)
                    .map(Some)
                    .ok_or(LocalRunDirectoryError::StateInvalid)
            }
            _ => Ok(None),
        }
    }

    /// What a repair continuation of this run would restart (REQ-CONTINUE-81): when the
    /// latest attempt ended `failed`, every ordinary step of its definition, in presentation
    /// order, it left other than `succeeded`, `skipped` or `inherited`. A latest attempt that
    /// did not fail during execution, such as one that succeeded with a declared outcome, is
    /// `NotFailed`; a failed attempt that left no such step, whose failure was a finalizer's,
    /// is `FinalizerOnly`, which names no step to continue from.
    pub fn failed_attempt_restart(&self) -> FailedAttemptRestart {
        let Some(prior) = self
            .state
            .attempts
            .last()
            .filter(|prior| prior.state == AttemptStateV1::WorkflowFailed)
        else {
            return FailedAttemptRestart::NotFailed;
        };
        let steps = self
            .prior_workflow
            .definition
            .presentation_order
            .iter()
            .filter(|id| {
                !prior.progress.steps.iter().any(|step| {
                    step.id == **id
                        && matches!(
                            step.state,
                            AttemptStepStateV1::Succeeded
                                | AttemptStepStateV1::Skipped
                                | AttemptStepStateV1::Inherited
                        )
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if steps.is_empty() {
            FailedAttemptRestart::FinalizerOnly
        } else {
            FailedAttemptRestart::Steps(steps)
        }
    }

    /// The steps the prior attempt left neither succeeded nor inherited, which an
    /// edge-scheduled continuation re-executes (REQ-CONTINUE-3).
    fn unfinished_steps(&self) -> BTreeSet<String> {
        self.state
            .attempts
            .last()
            .into_iter()
            .flat_map(|prior| &prior.progress.steps)
            .filter(|step| {
                !matches!(
                    step.state,
                    AttemptStepStateV1::Succeeded | AttemptStepStateV1::Inherited
                )
            })
            .map(|step| step.id.clone())
            .collect()
    }

    pub fn partition(
        &self,
        workflow: &ResolvedWorkflow,
        from: &[String],
    ) -> Result<(Vec<String>, Vec<String>), Vec<super::continuation::AdmissionViolation>> {
        let partition =
            super::continuation::partition(&workflow.definition, from, &self.unfinished_steps())
                .map_err(|invalid| {
                    let mut violations = if invalid.is_empty() {
                        vec![super::continuation::AdmissionViolation::InvalidFrom {
                            id: String::new(),
                        }]
                    } else {
                        invalid
                            .into_iter()
                            .map(|id| super::continuation::AdmissionViolation::InvalidFrom { id })
                            .collect::<Vec<_>>()
                    };
                    super::continuation::sort_violations(&mut violations, &workflow.definition);
                    violations.dedup();
                    violations
                })?;
        let Some(prior) = self.state.attempts.last() else {
            return Err(vec![super::continuation::AdmissionViolation::InvalidFrom {
                id: String::new(),
            }]);
        };
        let states: BTreeMap<String, super::continuation::PriorState> = prior
            .progress
            .steps
            .iter()
            .map(|step| {
                let state = match step.state {
                    AttemptStepStateV1::Succeeded => super::continuation::PriorState::Succeeded,
                    AttemptStepStateV1::Skipped => super::continuation::PriorState::Skipped,
                    AttemptStepStateV1::Inherited => super::continuation::PriorState::Inherited,
                    _ => super::continuation::PriorState::Unsatisfied,
                };
                (step.id.clone(), state)
            })
            .collect();
        let (effectively_skipped, disposition_failures) = effective_skipped_sources(
            &self.state,
            prior.attempt_number,
            &partition.inherited,
            &states,
        );
        let mut failures = super::continuation::check_inheritance(
            &workflow.definition,
            &self.prior_workflow.definition,
            &partition,
            &states,
            &effectively_skipped,
        );
        let inherited = partition
            .inherited
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        for source in
            super::continuation::referenced_outputs(&workflow.definition, &partition.reexecuted)
        {
            if source.node.role != super::validated::WorkflowNodeRole::Step
                || !inherited.contains(source.node.id.as_str())
                || effectively_skipped.contains(&source.node.id)
            {
                continue;
            }
            let retained = prior
                .progress
                .steps
                .iter()
                .find(|step| step.id == source.node.id)
                .and_then(|step| step.outputs.as_deref())
                .and_then(|outputs| outputs.iter().find(|output| output.name() == source.output));
            let declaration = step_output_declaration(workflow, &source.node.id, &source.output);
            if !matches!((retained, declaration), (Some(retained), Some(declaration)) if retained_output_matches_declaration(retained, declaration))
            {
                failures.push(super::continuation::AdmissionViolation::Reference {
                    reference: source.reference(),
                });
            }
        }
        for violation in disposition_failures {
            if !failures.contains(&violation) {
                failures.push(violation);
            }
        }
        super::continuation::sort_violations(&mut failures, &workflow.definition);
        failures.dedup();
        if !failures.is_empty() {
            return Err(failures);
        }
        Ok((partition.reexecuted, partition.inherited))
    }

    /// Claim only after the caller has admitted the projected inputs and every profile.
    /// Staging is non-authoritative until the single locked state replacement succeeds.
    /// An `edge` continuation records the edge it took and, when the edge is counted, first
    /// rewrites `run.json` with the new attempt count and the edge's count incremented, so a
    /// process that ends before the attempt exists has still taken the count once
    /// (REQ-WFEXEC-64, REQ-LOCALRUN-175, INV-ENGINE-22).
    ///
    /// `mode` names the attempt's definition: `Inherited` requires `admitted` to be the prior
    /// attempt's, `Replaced` retains `admitted` under the new attempt's directory, and `Run`
    /// requires `admitted` to be the run's own and records `definitionSource` `replaced` when
    /// it differs from the prior attempt's (REQ-CONTINUE-79).
    pub fn begin(
        self,
        admitted: &AdmittedWorkflow,
        from: Vec<String>,
        mode: ContinuationDefinitionMode,
        reason: super::publication::ContinuationReasonV1,
        edge: Option<super::publication::ContinuationEdgeV1>,
    ) -> Result<LocalAttemptOwner, LocalRunDirectoryError> {
        verify_retry_lock_identity(&self.root, &self.lock)?;
        // The wait the attempt continues from, read before anything changes and before
        // `remove_root_wait_record` removes it (REQ-WFEXEC-72).
        let resumed_wait = if self.keep_resolved_wait {
            Some(
                read_optional_wait_record(&self.root)?
                    .ok_or(LocalRunDirectoryError::StateConflict)?,
            )
        } else {
            None
        };
        if admitted.workflow().source.source_root != self.initial_workflow.source.source_root
            || admitted.execution().limits().maximum_parallel_steps().get()
                != self.maximum_parallel_steps
            || (mode == ContinuationDefinitionMode::Run
                && admitted_workflow_digest(admitted) != self.run.workflow_digest)
        {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let replacement_path = if mode == ContinuationDefinitionMode::Replaced {
            Some(
                admitted
                    .workflow()
                    .source
                    .source_root
                    .join(&admitted.workflow().source.workflow_path)
                    .to_str()
                    .ok_or(LocalRunDirectoryError::InvalidPath)?
                    .to_owned(),
            )
        } else {
            None
        };
        let (reexecuted, inherited) = self
            .partition(admitted.workflow(), &from)
            .map_err(|_| LocalRunDirectoryError::StateConflict)?;
        if self
            .projected_inputs(admitted.workflow())
            .map_err(|_| LocalRunDirectoryError::StateConflict)?
            != *admitted.inputs()
        {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let executed_git = admitted
            .workflow()
            .definition
            .finalizers
            .values()
            .any(|node| super::admission::has_git_output(&node.body))
            || reexecuted.iter().any(|id| {
                admitted
                    .workflow()
                    .definition
                    .steps
                    .get(id)
                    .is_some_and(super::admission::has_git_output)
            });
        match (executed_git, self.git_baseline(), admitted.git_capture()) {
            (true, Some(baseline), Some(capture)) if capture.uses_local_baseline(&baseline) => {}
            (true, _, _) => return Err(LocalRunDirectoryError::StateConflict),
            (false, _, None) => {}
            (false, _, Some(_)) => return Err(LocalRunDirectoryError::StateConflict),
        }
        self.validate_execution_root(admitted)?;
        let prior = self
            .state
            .attempts
            .last()
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        let next = prior
            .attempt_number
            .checked_add(1)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        let prior_definition = prior
            .definition
            .clone()
            .unwrap_or_else(|| attempt_definition_for_run(&self.run));
        let attempt_directory = create_or_verify_attempt_directory(&self.root, next)?;
        let definition = match mode {
            ContinuationDefinitionMode::Replaced => {
                mkdir(&attempt_directory, WORKFLOW_DIRECTORY)?;
                let workflow_directory = open_directory_at(&attempt_directory, WORKFLOW_DIRECTORY)?;
                mkdir(&workflow_directory, WORKFLOW_FILES_DIRECTORY)?;
                let files = open_directory_at(&workflow_directory, WORKFLOW_FILES_DIRECTORY)?;
                let manifest = retain_execution_specification(&files, admitted)?;
                let bytes = encode_json(&manifest)?;
                write_new_immutable_file(&workflow_directory, WORKFLOW_MANIFEST_FILE, &bytes)?;
                sync_directory(&files)?;
                sync_directory(&workflow_directory)?;
                sync_directory(&attempt_directory)?;
                AttemptDefinitionV1 {
                    digest: admitted_workflow_digest(admitted),
                    manifest_digest: DigestV1::sha256(&bytes),
                    locator: AttemptDefinitionLocatorV1::Attempt {
                        attempt_number: next,
                    },
                }
            }
            ContinuationDefinitionMode::Inherited => {
                if admitted.workflow().content_digest != self.prior_workflow.content_digest {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
                AttemptDefinitionV1 {
                    digest: prior_definition.digest.clone(),
                    manifest_digest: prior_definition.manifest_digest.clone(),
                    locator: AttemptDefinitionLocatorV1::PriorAttempt {
                        attempt_number: prior.attempt_number,
                    },
                }
            }
            // A restart continuation runs under the run's own definition, already retained
            // as its `workflow/` (REQ-CONTINUE-79).
            ContinuationDefinitionMode::Run => attempt_definition_for_run(&self.run),
        };
        let abandoned = !prior.state.is_terminal();
        let abandonment = abandoned.then(|| {
            capture_settlement_snapshot(
                Path::new(&prior.execution_root),
                WorkspaceSnapshotSettlementV1::AbandonmentRecovery,
            )
        });
        let prior_settlement = abandonment
            .clone()
            .or_else(|| prior.settlement_snapshot.clone());
        let start = capture_start_snapshot(admitted.execution().root());
        let changes = self.definition_changes(admitted.workflow(), &inherited);
        let inherited_steps = inherited
            .iter()
            .map(|id| {
                let step = prior
                    .progress
                    .steps
                    .iter()
                    .find(|step| &step.id == id)
                    .ok_or(LocalRunDirectoryError::StateInvalid)?;
                let prior_state = match step.state {
                    AttemptStepStateV1::Succeeded => {
                        super::evidence::InheritedPriorState::Succeeded
                    }
                    AttemptStepStateV1::Skipped => super::evidence::InheritedPriorState::Skipped,
                    AttemptStepStateV1::Inherited => {
                        super::evidence::InheritedPriorState::Inherited
                    }
                    _ => return Err(LocalRunDirectoryError::StateInvalid),
                };
                let changed = changes
                    .get(id)
                    .copied()
                    .ok_or(LocalRunDirectoryError::StateInvalid)?;
                Ok((
                    super::publication::ContinuationInheritedStepV1 {
                        id: id.clone(),
                        prior_state,
                        definition_changed: changed,
                    },
                    step,
                ))
            })
            .collect::<Result<Vec<_>, LocalRunDirectoryError>>()?;
        let portable_digest = |digest: &DigestV1| super::publication::DigestV1 {
            algorithm: digest.algorithm.clone(),
            value: digest.value.clone(),
        };
        let replaced = match mode {
            ContinuationDefinitionMode::Inherited => false,
            ContinuationDefinitionMode::Replaced => true,
            ContinuationDefinitionMode::Run => {
                definition.digest != prior_definition.digest
                    || definition.manifest_digest != prior_definition.manifest_digest
            }
        };
        let source = if replaced {
            super::publication::ContinuationDefinitionSourceV1::Replaced {
                manifest_digest: portable_digest(&definition.manifest_digest),
                prior_manifest_digest: portable_digest(&prior_definition.manifest_digest),
            }
        } else {
            super::publication::ContinuationDefinitionSourceV1::Inherited {
                manifest_digest: portable_digest(&definition.manifest_digest),
                prior_manifest_digest: portable_digest(&prior_definition.manifest_digest),
            }
        };
        // A run-definition continuation that resolves no root wait is a restart's first own
        // attempt whichever command begins it: `restart`, or `resume --from` after a restart
        // interrupted before that attempt (REQ-CONTINUE-79). The continuation of an upgrade's
        // carried wait keeps the reason its control gave it (REQ-CONTINUE-80).
        let reason = if mode == ContinuationDefinitionMode::Run
            && read_optional_wait_record(&self.root)?.is_none()
        {
            super::publication::ContinuationReasonV1::Restart
        } else {
            reason
        };
        let record = super::publication::ContinuationRecordV1 {
            schema_version: None,
            request: super::publication::ContinuationRequestV1 {
                from_steps: from.clone(),
                definition: match replacement_path {
                    Some(path) => super::publication::ContinuationRequestedDefinitionV1::Replaced {
                        replaced:
                            super::publication::ContinuationReplacementDefinitionSourceV1::Local(
                                super::publication::ContinuationLocalDefinitionSourceV1 { path },
                            ),
                    },
                    None => super::publication::ContinuationRequestedDefinitionV1::Inherited(
                        super::publication::ContinuationInheritedDefinitionV1::Inherited,
                    ),
                },
                execution_root: (admitted.execution().root() != Path::new(&prior.execution_root))
                    .then(|| admitted.execution().root().to_string_lossy().into_owned()),
                expected_run_version: None,
            },
            from_steps: from,
            reexecuted_steps: reexecuted,
            inherited_steps: inherited_steps
                .iter()
                .map(|(entry, _)| entry.clone())
                .collect(),
            definition_source: source,
            workspace: super::publication::ContinuationWorkspaceV1 {
                recovery: None,
                execution_root: admitted
                    .execution()
                    .root()
                    .to_str()
                    .ok_or(LocalRunDirectoryError::InvalidPath)?
                    .to_owned(),
                prior_execution_root: prior.execution_root.clone(),
                preparation: super::publication::ContinuationPreparationV1::Ready,
                modified: compare_continuation_snapshots(
                    admitted.execution().root(),
                    Path::new(&prior.execution_root),
                    &start,
                    prior_settlement.as_ref(),
                ),
                start_snapshot: Some(start),
                prior_settlement_snapshot: prior_settlement,
                quiescence: Some(self.quiescence.clone()),
            },
            reason: Some(reason),
            edge: edge.clone(),
        };
        let mut attempt = fresh_attempt(
            admitted,
            next,
            AttemptTriggerV1::Continuation,
            Some(prior.attempt_number),
            definition,
            timestamp(um_support::utc_now())?,
        )?;
        attempt.continuation = Some(record);
        for (entry, original) in inherited_steps {
            let step = attempt
                .progress
                .steps
                .iter_mut()
                .find(|step| step.id == entry.id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            step.state = AttemptStepStateV1::Inherited;
            step.detail = Some(NodeDetail::Inherited(super::evidence::InheritedDetail {
                prior_attempt_id: prior.attempt_id.clone(),
                prior_attempt_number: prior.attempt_number,
                prior_state: entry.prior_state,
                definition_changed: entry.definition_changed,
            }));
            let mut outputs = original
                .outputs
                .clone()
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            if original.state == AttemptStepStateV1::Succeeded {
                for output in &mut outputs {
                    output.set_producer(own_output_producer(prior, &step.id, output.name()));
                }
            }
            step.outputs = Some(outputs);
        }
        if let Some(edge) = edge.as_ref().filter(|edge| edge.counted) {
            count_run_record_edge(&self.root, next, &edge.key())?;
        }
        let store = self.state_store()?;
        store.update(|state| {
            let cause = abandoned_attempt_cause(state, next - 1);
            let previous = current_attempt_mut(state)?;
            if previous.attempt_number != next - 1 {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            if abandoned {
                for guard in &mut previous.process_guards {
                    guard.state = ProcessGuardStateV1::Quiesced;
                }
                let started = previous.started_at.is_some();
                settle_interrupted_attempt(previous, cause, started, abandonment)?;
            }
            state.current_attempt_number = next;
            state.attempts.push(attempt);
            Ok(())
        })?;
        remove_root_wait_record(&self.root)?;
        let name = attempt_directory_name(next).ok_or(LocalRunDirectoryError::StateInvalid)?;
        let finalizers = Arc::from(fresh_finalizer_progress(admitted)?);
        Ok(LocalAttemptOwner {
            normalized: self.normalized.clone(),
            lock: Some(self.lock),
            private_directory: self.normalized.join(PRIVATE_DIRECTORY),
            result_directory: self
                .normalized
                .join(ATTEMPTS_DIRECTORY)
                .join(name)
                .join("result"),
            attempt_directory,
            attempt_number: next,
            finalizers,
            state: store,
            resumed_wait,
        })
    }
}

fn store_from_locked_snapshot(
    root: &OwnedFd,
    state: &LocalRunStateV1,
) -> Result<Arc<StateStore>, LocalRunDirectoryError> {
    let root = Arc::new(dup(root).map_err(|_| LocalRunDirectoryError::StateInvalid)?);
    let private = Arc::new(open_directory_at(&root, PRIVATE_DIRECTORY)?);
    Ok(Arc::new(StateStore {
        root,
        private,
        current: Mutex::new(state.clone()),
        pending_recovery_invocations: Mutex::new(BTreeMap::new()),
    }))
}

fn settle_abandoned_snapshot(
    root: &OwnedFd,
    state: &LocalRunStateV1,
) -> Result<(), LocalRunDirectoryError> {
    let prior = state
        .attempts
        .last()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    if prior.state.is_terminal() {
        return Ok(());
    }
    let snapshot = capture_settlement_snapshot(
        Path::new(&prior.execution_root),
        WorkspaceSnapshotSettlementV1::AbandonmentRecovery,
    );
    store_from_locked_snapshot(root, state)?.update(|state| {
        let cause = abandoned_attempt_cause(state, state.current_attempt_number);
        let prior = current_attempt_mut(state)?;
        for guard in &mut prior.process_guards {
            guard.state = ProcessGuardStateV1::Quiesced;
        }
        let started = prior.started_at.is_some();
        settle_interrupted_attempt(prior, cause, started, Some(snapshot))
    })
}

fn effective_skipped_sources(
    state: &LocalRunStateV1,
    prior_attempt_number: u64,
    inherited: &[String],
    states: &BTreeMap<String, super::continuation::PriorState>,
) -> (
    BTreeSet<String>,
    Vec<super::continuation::AdmissionViolation>,
) {
    let mut skipped = BTreeSet::new();
    let mut violations = Vec::new();
    let index = LocalRunStateIndex::new(state).ok();
    for id in inherited {
        if matches!(
            states.get(id),
            Some(
                super::continuation::PriorState::Skipped
                    | super::continuation::PriorState::Inherited
            )
        ) {
            match index
                .as_ref()
                .and_then(|index| index.disposition(prior_attempt_number, id))
            {
                Some(InheritedDisposition::Skipped) => {
                    skipped.insert(id.clone());
                }
                Some(InheritedDisposition::Succeeded) => {}
                None => violations.push(super::continuation::AdmissionViolation::Node {
                    id: id.clone(),
                    prior: states.get(id).copied(),
                }),
            }
        }
    }
    (skipped, violations)
}

pub enum LocalRetryBeginError {
    Rejected(LocalRetryRejection),
    Operational(LocalRunDirectoryError),
}

impl From<LocalRunDirectoryError> for LocalRetryBeginError {
    fn from(error: LocalRunDirectoryError) -> Self {
        Self::Operational(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalStatusAttempt {
    pub attempt_number: u64,
    pub trigger: &'static str,
    pub state: &'static str,
    pub result: LocalStatusResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalStatusResult {
    NotPublished { reason: &'static str },
    Published { relative_directory: String },
    PublicationFailed { phase: &'static str },
}

mod status_state;
pub use status_state::{
    LocalStatusNode, LocalStatusProgress, LocalStatusStart, LocalStatusStateView,
};

#[derive(Clone, Debug, PartialEq)]
pub struct LocalRunStatusSnapshot {
    pub status_state: LocalStatusStateView,
    pub run_directory: PathBuf,
    pub run: Value,
    /// The run record `run.json` (REQ-LOCALRUN-175), or `None` for a run without one.
    pub record: Option<Value>,
    /// The root wait record `wait.json` while the run waits (REQ-LOCALRUN-146).
    pub wait: Option<WaitRecord>,
    pub state: Value,
    pub current_attempt_number: u64,
    pub current_attempt_state: &'static str,
    pub current_result: LocalStatusResult,
    pub attempts: Vec<LocalStatusAttempt>,
    pub recovery: LocalRecoveryStatus,
    pub retry: LocalRetryEligibility,
    pub continuation: LocalRetryEligibility,
}

pub trait DurableDeadline {
    fn deadline_utc(&self) -> OffsetDateTime;
}

pub struct LocalAttemptOwner {
    normalized: PathBuf,
    lock: Option<File>,
    private_directory: PathBuf,
    attempt_directory: OwnedFd,
    result_directory: PathBuf,
    attempt_number: u64,
    finalizers: Arc<[AttemptStepV1]>,
    state: Arc<StateStore>,
    /// The root wait record `begin` removed for a continuation that keeps it
    /// (`PendingLocalContinuation::keep_resolved_wait`); `None` otherwise.
    resumed_wait: Option<WaitRecord>,
}

pub type InitialLocalRun = LocalAttemptOwner;

pub struct LocalAttemptOwnershipReleased {
    _private: (),
}

impl LocalAttemptOwner {
    pub fn create(
        requested: &Path,
        admitted: &AdmittedWorkflow,
    ) -> Result<Self, LocalRunDirectoryError> {
        Self::create_with_record(requested, admitted, None)
    }

    /// `create`, writing `record` as the run directory's `run.json` (REQ-LOCALRUN-175)
    /// into the staging root, so the directory holds it when it appears.
    pub fn create_with_record(
        requested: &Path,
        admitted: &AdmittedWorkflow,
        record: Option<&WorkflowRunRecord>,
    ) -> Result<Self, LocalRunDirectoryError> {
        create_with_observer(
            requested,
            admitted,
            record,
            &mut NoopInitialPublicationObserver,
        )
    }

    /// Rewrites the run record's `state`, `attempts` and `outcome` (REQ-LOCALRUN-175): the
    /// attempt's chosen outcome once it exits, `None` while it is live. A run without a
    /// record is left as it is.
    pub fn update_run_record(
        &self,
        state: RunState,
        attempts: u64,
        outcome: Option<&str>,
    ) -> Result<(), LocalRunDirectoryError> {
        update_run_record(&self.state.root, state, attempts, outcome)
    }

    /// Creates `requested` as a successor of the waiting run `predecessor` holds
    /// (REQ-LOCALRUN-195, INV-ENGINE-26) and returns its directory, its lock released: as
    /// `create_with_record`, with `admitted`'s `workflow/` and `record` as its `run.json`, but
    /// carrying the predecessor's Git baseline and importing its attempts and controls rather
    /// than staging a first attempt. The successor's first own attempt is a continuation of
    /// the last imported one. `record` must name the predecessor's run as `predecessor` and
    /// count its attempts; the predecessor is left as it is, for `supersede`.
    ///
    /// `carry` says what else the successor holds: with `Continue` its record reads `running`
    /// and it holds no wait; with `Wait` its record reads `waiting` and the predecessor's root
    /// wait record is copied as its own (REQ-LOCALRUN-197). A record whose state disagrees,
    /// or `Wait` for a predecessor without a root wait record, is `StateInvalid` before
    /// anything is staged. The record's `position`, `edges` and `external` are the caller's.
    pub fn create_successor(
        requested: &Path,
        admitted: &AdmittedWorkflow,
        record: &WorkflowRunRecord,
        predecessor: &PendingLocalContinuation,
        carry: SuccessorCarry,
    ) -> Result<PathBuf, LocalRunDirectoryError> {
        verify_retry_lock_identity(&predecessor.root, &predecessor.lock)?;
        let predecessor_id = read_run_record(&predecessor.root)?.id;
        let carried = match carry {
            SuccessorCarry::Continue => record.state == RunState::Running,
            SuccessorCarry::Wait => {
                record.state == RunState::Waiting
                    && read_optional_wait_record(&predecessor.root)?.is_some()
            }
        };
        if !carried
            || record.predecessor.as_deref() != Some(predecessor_id.as_str())
            || u64::try_from(predecessor.state.attempts.len()).ok() != Some(record.attempts)
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        if !predecessor
            .state
            .attempts
            .last()
            .is_some_and(|prior| prior.state.is_terminal())
        {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        publish_run_directory(
            requested,
            admitted,
            Some(record),
            || predecessor.run.git_baseline.clone(),
            |staging| {
                stage_successor_import(
                    predecessor,
                    &predecessor_id,
                    carry == SuccessorCarry::Wait,
                    &staging,
                )
            },
            &mut NoopInitialPublicationObserver,
        )
        .map(|published| published.normalized)
    }

    /// Writes the wait record of the wait this attempt ended on (REQ-LOCALRUN-178,
    /// INV-ENGINE-20): `attempts/<NNNNNN>/wait.json` and then, through
    /// `.private/wait.json.tmp` and a rename, the identical `wait.json` at the run root, both
    /// owner-only and read-only. The caller writes it before the run record reads `waiting`.
    pub fn record_wait(&self, record: &WaitRecord) -> Result<(), LocalRunDirectoryError> {
        if !record.is_valid() {
            return Err(LocalRunDirectoryError::WaitRecordInvalid);
        }
        let bytes = encode_json(record)?;
        write_new_immutable_file(&self.attempt_directory, WAIT_RECORD_FILE, &bytes)?;
        sync_directory(&self.attempt_directory)?;
        write_through_private(
            &self.state.private,
            WAIT_RECORD_TEMPORARY_FILE,
            &self.state.root,
            WAIT_RECORD_FILE,
            &bytes,
            Mode::RUSR,
            RenameFlags::empty(),
        )?;
        sync_directory(&self.state.root)
    }

    /// The record of the wait this attempt continues from, as `begin` removed it, when the
    /// continuation kept it (`PendingLocalContinuation::keep_resolved_wait`). An attempt that
    /// ends on that wait kept (`EnteredWait::kept`, REQ-WFEXEC-72) writes this record back
    /// unchanged with `record_wait` (REQ-LOCALRUN-178).
    pub fn resumed_wait_record(&self) -> Option<&WaitRecord> {
        self.resumed_wait.as_ref()
    }

    pub fn run_directory(&self) -> &Path {
        &self.normalized
    }

    /// Where `create` wrote the run's tree copy for a run admitted from a tree
    /// (REQ-LOCALRUN-170): `<run directory>/workflow/tree`, the root
    /// `AdmittedWorkflow::with_tree_copy` binds.
    pub fn tree_copy_directory(&self) -> PathBuf {
        self.normalized
            .join(WORKFLOW_DIRECTORY)
            .join(WORKFLOW_TREE_DIRECTORY)
    }

    pub fn private_directory(&self) -> &Path {
        &self.private_directory
    }

    pub fn result_directory(&self) -> &Path {
        &self.result_directory
    }

    pub fn attempt_directory_handle(&self) -> &OwnedFd {
        &self.attempt_directory
    }

    pub fn private_directory_handle(&self) -> &OwnedFd {
        &self.state.private
    }

    pub fn create_agent_diagnostic_sessions(
        &self,
    ) -> Result<AgentDiagnosticSessionStore, LocalRunDirectoryError> {
        let state = lock_state(&self.state.current)?;
        if state.current_attempt_number != self.attempt_number {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let local_run_id = Arc::<str>::from(state.local_run_id.as_str());
        drop(state);
        let attempt_name = attempt_directory_name(self.attempt_number)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        let attempt_path = self.normalized.join(ATTEMPTS_DIRECTORY).join(attempt_name);
        AgentDiagnosticSessionStore::create(
            &self.attempt_directory,
            &attempt_path,
            local_run_id,
            self.attempt_number,
        )
        .map_err(|_| LocalRunDirectoryError::StagingUnavailable)
    }

    pub fn create_private_staging(&self) -> Result<AttemptPrivateStaging, LocalRunDirectoryError> {
        let (identity, root) =
            create_staging_root(&self.state.private, "workflow", PRIVATE_STAGING_ATTEMPTS)
                .map_err(|()| LocalRunDirectoryError::StagingUnavailable)?;
        Ok(AttemptPrivateStaging {
            parent: Arc::clone(&self.state.private),
            path: self.private_directory.join(identity.as_ref()),
            identity,
            root,
            released: false,
        })
    }

    pub const fn attempt_number(&self) -> u64 {
        self.attempt_number
    }

    pub fn finalization_identity(&self) -> Result<(String, String), LocalRunDirectoryError> {
        let state = lock_state(&self.state.current)?;
        let attempt = state
            .attempts
            .iter()
            .find(|attempt| attempt.attempt_number == self.attempt_number)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        Ok((state.local_run_id.clone(), attempt.attempt_id.clone()))
    }

    pub fn continuation_record(
        &self,
    ) -> Result<Option<super::publication::ContinuationRecordV1>, LocalRunDirectoryError> {
        let state = lock_state(&self.state.current)?;
        let attempt = state
            .attempts
            .iter()
            .find(|attempt| attempt.attempt_number == self.attempt_number)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        Ok(attempt.continuation.clone())
    }

    pub fn bind_continuation_context(
        &self,
        admitted: AdmittedWorkflow,
    ) -> Result<AdmittedWorkflow, LocalRunDirectoryError> {
        let state = lock_state(&self.state.current)?;
        let attempt = current_attempt(&state)?;
        if attempt.attempt_number != self.attempt_number
            || attempt.trigger != AttemptTriggerV1::Continuation
            || admitted.execution().root().to_str() != Some(attempt.execution_root.as_str())
            || attempt.definition.as_ref().is_none_or(|definition| {
                definition.digest.value != admitted.workflow().content_digest.value
            })
        {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let record = attempt
            .continuation
            .as_ref()
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        let inherited = attempt
            .progress
            .steps
            .iter()
            .filter(|step| step.state == AttemptStepStateV1::Inherited)
            .map(|step| {
                (
                    step.id.as_str(),
                    step.outputs.as_deref().unwrap_or_default(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let bytes = encode_json(&serde_json::json!({
            "continuation": record,
            "inheritedOutputs": inherited,
        }))?;
        const CONTEXT: &str = "continuation-context.json";
        write_new_immutable_file(&self.attempt_directory, CONTEXT, &bytes)?;
        sync_directory(&self.attempt_directory)?;
        let name = attempt_directory_name(self.attempt_number)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        let context_path = self
            .normalized
            .join(ATTEMPTS_DIRECTORY)
            .join(name)
            .join(CONTEXT);
        super::recovery::verify_regular_path_binding(
            &context_path,
            &self.attempt_directory,
            CONTEXT,
        )
        .map_err(|()| LocalRunDirectoryError::StateInvalid)?;
        Ok(admitted.with_continuation_context(&context_path))
    }

    pub fn release(mut self) -> LocalAttemptOwnershipReleased {
        self.release_lock();
        LocalAttemptOwnershipReleased { _private: () }
    }

    pub fn commit_port(
        &self,
        diagnostics: StepDiagnosticLog,
        accounting: InvocationAccountingLog,
        artifacts: ArtifactStaging,
    ) -> LocalRunCommitPort {
        LocalRunCommitPort {
            state: Arc::clone(&self.state),
            finalizers: Arc::clone(&self.finalizers),
            diagnostics,
            accounting,
            artifacts,
            child_failure: Arc::new(OnceLock::new()),
            failure_text: Arc::new(OnceLock::new()),
        }
    }

    pub fn durable_invocations(&self) -> Result<Vec<DurableInvocationV1>, LocalRunDirectoryError> {
        let state = lock_state(&self.state.current)?;
        let attempt = state
            .attempts
            .iter()
            .find(|attempt| attempt.attempt_number == self.attempt_number)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        Ok(attempt.progress.invocations.clone())
    }

    pub fn process_guard_registry(&self) -> ProcessGuardRegistry {
        let state: Arc<dyn DurableProcessGuardStore> = self.state.clone();
        ProcessGuardRegistry::durable(state)
    }

    pub async fn execution_seed(
        &self,
        admitted: AdmittedWorkflow,
        artifacts: ArtifactStaging,
    ) -> Result<ExecutionSeed<CapturedValue>, LocalRunDirectoryError> {
        let state = Arc::clone(&self.state);
        let attempt_number = self.attempt_number;
        let resumed_wait = self.resumed_wait.clone();
        tokio::task::spawn_blocking(move || {
            let seed = load_execution_seed(
                &state.root,
                &state.current,
                attempt_number,
                &admitted,
                &artifacts,
            )?;
            // A continuation of a resolved wait carries it, so an interruption before any
            // step has started keeps it (REQ-WFEXEC-72, INV-ENGINE-32).
            Ok(match resumed_wait {
                Some(record) => seed.with_resumed_wait(resumed_entered_wait(&record)?),
                None => seed,
            })
        })
        .await
        .map_err(|_| LocalRunDirectoryError::StateInvalid)?
    }

    pub fn record_result_published(&self) -> Result<(), LocalRunDirectoryError> {
        self.state.update(|state| {
            let attempt = current_attempt_mut(state)?;
            if !attempt.state.is_terminal() {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            attempt.result = AttemptResultV1::Published {
                relative_directory: attempt_result_relative_path(attempt.attempt_number),
            };
            Ok(())
        })
    }

    pub fn record_result_publication_failed(
        &self,
        phase: PublicationFailurePhaseV1,
        invariant: Option<RunResultInvariant>,
    ) -> Result<(), LocalRunDirectoryError> {
        self.state.update(|state| {
            let attempt_number = state.current_attempt_number;
            let attempt = current_attempt_mut(state)?;
            if !attempt.state.is_terminal() {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            attempt.result = AttemptResultV1::PublicationFailed {
                phase,
                result_invariant: invariant,
            };
            append_diagnostic(
                state,
                attempt_number,
                DiagnosticCodeV1::ResultPublicationFailure,
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn record_executor_fault_before_execution(
        &self,
    ) -> Result<(), LocalRunDirectoryError> {
        record_executor_fault_before_execution(&self.state)
    }

    pub async fn record_executor_fault_before_execution_async(
        &self,
    ) -> Result<(), LocalRunDirectoryError> {
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || record_executor_fault_before_execution(&state))
            .await
            .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?
    }

    /// Appends the `state_persistence_failure` diagnostic of `failure` for the current
    /// attempt and settles nothing (REQ-LOCALRUN-205, INV-ENGINE-33): the attempt stays the
    /// abandoned attempt the next retry or continuation settles.
    pub async fn record_state_persistence_failure_async(
        &self,
        failure: CommitFailure,
    ) -> Result<(), LocalRunDirectoryError> {
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || record_state_persistence_failure(&state, failure))
            .await
            .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?
    }

    /// Whether the attempt reads settled in the last state this owner committed
    /// (REQ-LOCALRUN-206). A state write that failed did not advance it.
    pub fn attempt_settled(&self) -> bool {
        lock_state(&self.state.current).is_ok_and(|state| {
            current_attempt(&state).is_ok_and(|attempt| attempt.state.is_terminal())
        })
    }

    pub fn record_private_cleanup_failure(&self) -> Result<(), LocalRunDirectoryError> {
        self.state.update(|state| {
            append_diagnostic(
                state,
                state.current_attempt_number,
                DiagnosticCodeV1::PrivateCleanupFailure,
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn root_handle(&self) -> &OwnedFd {
        &self.state.root
    }

    fn release_lock(&mut self) {
        if let Some(lock) = self.lock.take() {
            // Closing normally releases the process-associated lock. Unlock explicitly so
            // an orderly owner release is immediately visible to status and retry queries.
            let _ = fcntl_lock(&lock, FlockOperation::Unlock);
        }
    }
}

fn record_executor_fault_before_execution(
    state: &StateStore,
) -> Result<(), LocalRunDirectoryError> {
    let snapshot = settlement_snapshot(state, WorkspaceSnapshotSettlementV1::Engine)?;
    state.update(|state| {
        let attempt = current_attempt_mut(state)?;
        if attempt.state != AttemptStateV1::Created {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        settle_interrupted_attempt(
            attempt,
            InterruptionCauseV1::ExecutorFault,
            false,
            Some(snapshot),
        )
    })
}

fn record_state_persistence_failure(
    state: &StateStore,
    failure: CommitFailure,
) -> Result<(), LocalRunDirectoryError> {
    state.update(|state| {
        append_diagnostic_of(
            state,
            state.current_attempt_number,
            DiagnosticCodeV1::StatePersistenceFailure,
            Some(failure),
        )
    })
}

/// The cause an unsettled `attempt_number` is settled `interrupted` with: the state
/// persistence failure its owner recorded before it stopped, or the owner's loss
/// (REQ-LOCALRUN-205).
fn abandoned_attempt_cause(state: &LocalRunStateV1, attempt_number: u64) -> InterruptionCauseV1 {
    if state.diagnostics.iter().any(|diagnostic| {
        diagnostic.attempt_number == attempt_number
            && diagnostic.code == DiagnosticCodeV1::StatePersistenceFailure
    }) {
        InterruptionCauseV1::StatePersistenceFailure
    } else {
        InterruptionCauseV1::ExecutionOwnerLost
    }
}

fn settlement_snapshot(
    state: &StateStore,
    settled_by: WorkspaceSnapshotSettlementV1,
) -> Result<WorkspaceSnapshotV1, LocalRunDirectoryError> {
    let current = lock_state(&state.current)?;
    let attempt = current_attempt(&current)?;
    Ok(capture_settlement_snapshot(
        Path::new(&attempt.execution_root),
        settled_by,
    ))
}

pub struct AttemptPrivateStaging {
    parent: Arc<OwnedFd>,
    path: PathBuf,
    identity: Arc<str>,
    root: OwnedFd,
    released: bool,
}

impl AttemptPrivateStaging {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn root_handle(&self) -> &OwnedFd {
        &self.root
    }

    pub fn release(mut self) -> Result<(), LocalRunDirectoryError> {
        remove_staging_root(&self.parent, &self.identity, &self.root)
            .map_err(|_| LocalRunDirectoryError::StagingUnavailable)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for AttemptPrivateStaging {
    fn drop(&mut self) {
        if !self.released {
            let _ = remove_staging_root(&self.parent, &self.identity, &self.root);
        }
    }
}

impl Drop for LocalAttemptOwner {
    fn drop(&mut self) {
        self.release_lock();
    }
}

pub struct LocalRunCommitPort {
    state: Arc<StateStore>,
    finalizers: Arc<[AttemptStepV1]>,
    diagnostics: StepDiagnosticLog,
    accounting: InvocationAccountingLog,
    artifacts: ArtifactStaging,
    /// Set by a child run whose own state commit failed; once set, every commit is refused
    /// with it and writes nothing (REQ-LOCALRUN-207, INV-ENGINE-33).
    child_failure: Arc<OnceLock<ChildCommitFailure>>,
    /// The text of the error the first failed commit returned: its write, path and OS
    /// message, which the classified failure never holds (REQ-LOCALRUN-143, INV-ENGINE-33).
    failure_text: Arc<OnceLock<String>>,
}

impl LocalRunCommitPort {
    /// The latch a child run's failed state commit sets, after which this port refuses
    /// every commit with that failure (REQ-LOCALRUN-207, INV-ENGINE-33).
    pub fn child_failure_latch(&self) -> Arc<OnceLock<ChildCommitFailure>> {
        Arc::clone(&self.child_failure)
    }

    /// The cell this port sets to the text of its first failed commit's error, for the
    /// command to print once the engine has consumed the port (REQ-LOCALRUN-143).
    pub fn failure_text(&self) -> Arc<OnceLock<String>> {
        Arc::clone(&self.failure_text)
    }
}

impl<Deadline>
    CommitPort<CommittedReduction<StepFailureCause, super::value::CapturedValue, Deadline>>
    for LocalRunCommitPort
where
    Deadline: DurableDeadline + Send + 'static,
{
    type Error = LocalRunDirectoryError;

    fn commit(
        &mut self,
        commit: CommittedReduction<StepFailureCause, super::value::CapturedValue, Deadline>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        let state = Arc::clone(&self.state);
        let finalizers = Arc::clone(&self.finalizers);
        let diagnostics = self.diagnostics.clone();
        let accounting = self.accounting.clone();
        let artifacts = self.artifacts.clone();
        let child_failure = Arc::clone(&self.child_failure);
        let failure_text = Arc::clone(&self.failure_text);
        async move {
            let result = match child_failure.get() {
                Some(child) => Err(LocalRunDirectoryError::ChildCommitFailed(child.clone())),
                None => tokio::task::spawn_blocking(move || {
                    state.commit_runtime(
                        &commit,
                        &finalizers,
                        &diagnostics,
                        &accounting,
                        &artifacts,
                    )
                })
                .await
                .unwrap_or(Err(LocalRunDirectoryError::StateWriteUnavailable)),
            };
            if let Err(error) = &result {
                failure_text.get_or_init(|| error.to_string());
            }
            result
        }
    }
}

type PendingRecoveryInvocationKey = (u64, String, u8);
type PendingRecoveryInvocation = (u64, String);

struct StateStore {
    root: Arc<OwnedFd>,
    private: Arc<OwnedFd>,
    current: Mutex<LocalRunStateV1>,
    pending_recovery_invocations:
        Mutex<BTreeMap<PendingRecoveryInvocationKey, PendingRecoveryInvocation>>,
}

impl StateStore {
    fn commit_runtime<Deadline>(
        &self,
        commit: &CommittedReduction<StepFailureCause, super::value::CapturedValue, Deadline>,
        finalizers: &[AttemptStepV1],
        diagnostics: &StepDiagnosticLog,
        accounting: &InvocationAccountingLog,
        artifacts: &ArtifactStaging,
    ) -> Result<(), LocalRunDirectoryError>
    where
        Deadline: DurableDeadline,
    {
        let retained_outputs = self.retain_outputs(&commit.state, finalizers, artifacts)?;
        let settlement_snapshot = (commit.diagnostic.is_some()
            || matches!(
                &commit.state.workflow,
                WorkflowState::Succeeded
                    | WorkflowState::Failed { .. }
                    | WorkflowState::Cancelled { .. }
                    | WorkflowState::Waiting { .. }
                    | WorkflowState::Continued { .. }
            ))
        .then(|| {
            let state = lock_state(&self.current)?;
            let attempt = current_attempt(&state)?;
            Ok(capture_settlement_snapshot(
                Path::new(&attempt.execution_root),
                WorkspaceSnapshotSettlementV1::Engine,
            ))
        })
        .transpose()?;
        self.update(|state| {
            let now = timestamp(um_support::utc_now())?;
            let attempt_number = state.current_attempt_number;
            {
                let attempt = current_attempt_mut(state)?;
                if attempt.state.is_terminal() {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
                attempt.started_at.get_or_insert_with(|| now.clone());
                if let Some(diagnostic) = commit.diagnostic {
                    let code = match diagnostic {
                        CoordinationDiagnostic::TransitionCapacityExceeded => {
                            DiagnosticCodeV1::TransitionCapacityExceeded
                        }
                    };
                    settle_interrupted_attempt(
                        attempt,
                        InterruptionCauseV1::ExecutorFault,
                        true,
                        settlement_snapshot.clone(),
                    )?;
                    append_diagnostic(state, attempt_number, code)?;
                    return Ok(());
                }
            }
            let attempt = current_attempt_mut(state)?;
            if commit.occurrence_accepted {
                attempt.progress.accepted_occurrence_ordinal = commit.occurrence_ordinal.get();
            }
            attempt.progress.last_transition_sequence = commit.state.last_transition_sequence.get();
            update_step_progress(attempt, &commit.state, finalizers, &retained_outputs, &now)?;
            update_invocation_ledger(self, attempt, commit, diagnostics, accounting, &now)?;
            update_recovery_progress(
                &mut attempt.progress.steps,
                &attempt.progress.invocations,
                &commit.state.steps,
            )?;
            attempt.progress.outstanding_actions =
                outstanding_actions(attempt, &commit.state.steps)?;
            for requested in &commit.actions {
                if !matches!(requested.kind, CommittedActionKind::FinishRun)
                    && !attempt.progress.outstanding_actions.iter().any(|action| {
                        action.action_id == requested.id.transition_sequence.get()
                            && action.step_id == requested.step
                            && action.node_role
                                == requested
                                    .step
                                    .as_deref()
                                    .and_then(|id| attempt_node_role(attempt, id))
                    })
                {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
            }
            for event in &commit.events {
                if let super::runtime::TransitionEvent::CancellationAccepted {
                    reason,
                    deadline,
                    ..
                } = event
                {
                    attempt.cancellation = Some(AttemptCancellationV1 {
                        reason: cancellation_reason(*reason),
                        requested_at: now.clone(),
                        force_stop_deadline: timestamp(deadline.deadline_utc())?,
                        workflow_confirmed: false,
                    });
                }
            }
            attempt.state = match &commit.state.workflow {
                WorkflowState::Executing {
                    gate: super::runtime::SchedulingGate::Cancelling { .. },
                }
                | WorkflowState::Finalizing {
                    gate: super::runtime::FinalizationGate::Cancelling { .. },
                    ..
                } => AttemptStateV1::Cancelling,
                WorkflowState::Executing { .. }
                | WorkflowState::Finalizing {
                    gate: super::runtime::FinalizationGate::Open,
                    ..
                } => AttemptStateV1::Running,
                WorkflowState::Succeeded => AttemptStateV1::Succeeded,
                WorkflowState::Failed { .. } => AttemptStateV1::WorkflowFailed,
                WorkflowState::Cancelled { .. } => AttemptStateV1::Cancelled,
                WorkflowState::Waiting { .. } => AttemptStateV1::Waiting,
                WorkflowState::Continued { .. } => AttemptStateV1::Continued,
            };
            // A continued attempt keeps the edge it took, so a run whose next attempt never
            // began continues from it (REQ-CONTINUE-78).
            if let WorkflowState::Continued { edge } = &commit.state.workflow {
                attempt.taken_edge = Some(edge.clone().into());
            }
            if attempt.state.is_terminal() {
                attempt.settled_at = Some(now);
                attempt.settlement_snapshot = settlement_snapshot.clone();
                attempt.progress.outstanding_actions.clear();
                attempt.result = AttemptResultV1::NotPublished {
                    reason: ResultAbsentReasonV1::PublicationPending,
                };
                if let Some(cancellation) = &mut attempt.cancellation
                    && matches!(attempt.state, AttemptStateV1::Cancelled)
                {
                    cancellation.workflow_confirmed = true;
                }
            }
            if !commit.occurrence_accepted {
                let attempt_number = attempt.attempt_number;
                append_diagnostic(state, attempt_number, DiagnosticCodeV1::StaleOccurrence)?;
            }
            Ok(())
        })
    }

    fn update(
        &self,
        mutate: impl FnOnce(&mut LocalRunStateV1) -> Result<(), LocalRunDirectoryError>,
    ) -> Result<(), LocalRunDirectoryError> {
        self.update_with_observer(mutate, &mut NoopStateCommitObserver)
    }

    fn update_with_observer(
        &self,
        mutate: impl FnOnce(&mut LocalRunStateV1) -> Result<(), LocalRunDirectoryError>,
        observer: &mut impl StateCommitObserver,
    ) -> Result<(), LocalRunDirectoryError> {
        let mut current = lock_state(&self.current)?;
        // The exclusive run lock and this mutex serialize writers. Disk is decoded
        // when ownership is acquired, not on every commit.
        let mut next = current.clone();
        mutate(&mut next)?;
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        validate_state(&next)?;
        replace_state(&self.root, &self.private, &mut current, next, observer)
    }
}

impl DurableProcessGuardStore for StateStore {
    fn register(
        &self,
        step: &str,
        action_id: u64,
        identity: &AuthenticatedProcessGroup,
    ) -> Result<String, ProcessGuardStoreError> {
        let guard_id = generate_uuid().map_err(|_| ProcessGuardStoreError)?;
        self.update(|state| {
            let attempt = current_attempt_mut(state)?;
            let registered_action = attempt
                .progress
                .outstanding_actions
                .iter()
                .find(|action| {
                    action.action_id == action_id
                        && matches!(
                            action.kind,
                            OutstandingActionKindV1::StartStep
                                | OutstandingActionKindV1::StartRecoveryHandler
                        )
                        && action.step_id.as_deref() == Some(step)
                })
                .and_then(|action| action.node_role)
                .ok_or(LocalRunDirectoryError::StateConflict)?;
            if attempt
                .process_guards
                .iter()
                .any(|guard| guard.action_id == action_id || guard.guard_id == guard_id)
            {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            attempt.process_guards.push(ProcessGuardV1 {
                guard_id: guard_id.clone(),
                action_id,
                step_id: step.to_owned(),
                node_role: registered_action,
                state: ProcessGuardStateV1::Prepared,
                execution_host: attempt.owner.execution_host.clone(),
                process_group_id: i64::from(identity.process_group().as_raw_pid()),
                liveness: ProcessLivenessV1 {
                    kind: ProcessLivenessKindV1::LeaderStartIdentity,
                    value: identity.leader_start_identity().to_owned(),
                },
            });
            Ok(())
        })
        .map_err(|_| ProcessGuardStoreError)?;
        Ok(guard_id)
    }

    fn mark_released(&self, _guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        // Prepared was durably committed before continuation. Until quiescence,
        // recovery treats it exactly like released: the child may have run.
        // The quiesced commit folds both later transitions into one write.
        Ok(())
    }

    fn mark_quiesced(&self, guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        self.update(|state| {
            let guard = current_attempt_mut(state)?
                .process_guards
                .iter_mut()
                .find(|guard| guard.guard_id == guard_id)
                .ok_or(LocalRunDirectoryError::StateConflict)?;
            guard.state = ProcessGuardStateV1::Quiesced;
            Ok(())
        })
        .map_err(|_| ProcessGuardStoreError)
    }
}

fn lock_state(
    state: &Mutex<LocalRunStateV1>,
) -> Result<MutexGuard<'_, LocalRunStateV1>, LocalRunDirectoryError> {
    state
        .lock()
        .map_err(|_| LocalRunDirectoryError::StateConflict)
}

trait InitialPublicationObserver {
    fn published(&mut self, _path: &Path, _lock: &File) -> Result<(), LocalRunDirectoryError> {
        Ok(())
    }
}

struct NoopInitialPublicationObserver;

impl InitialPublicationObserver for NoopInitialPublicationObserver {}

fn create_with_observer(
    requested: &Path,
    admitted: &AdmittedWorkflow,
    record: Option<&WorkflowRunRecord>,
    observer: &mut impl InitialPublicationObserver,
) -> Result<InitialLocalRun, LocalRunDirectoryError> {
    let published = publish_run_directory(
        requested,
        admitted,
        record,
        || Some(capture_initial_git_baseline(admitted)),
        |staging| {
            mkdir(staging.attempts, INITIAL_ATTEMPT_DIRECTORY)?;
            let attempt_directory = open_directory_at(staging.attempts, INITIAL_ATTEMPT_DIRECTORY)?;
            let state = initial_state(
                admitted,
                staging.run,
                staging.run.local_run_id.clone(),
                staging.run.created_at.clone(),
            )?;
            Ok(StagedRunContent {
                state,
                controls: false,
                wait: false,
                kept: attempt_directory,
            })
        },
        observer,
    )?;
    let root = Arc::new(published.root);
    let private = Arc::new(published.private);
    let state = Arc::new(StateStore {
        root: Arc::clone(&root),
        private,
        current: Mutex::new(published.state),
        pending_recovery_invocations: Mutex::new(BTreeMap::new()),
    });
    let private_directory = published.normalized.join(PRIVATE_DIRECTORY);
    let result_directory = published
        .normalized
        .join(ATTEMPTS_DIRECTORY)
        .join(INITIAL_ATTEMPT_DIRECTORY)
        .join("result");
    Ok(LocalAttemptOwner {
        normalized: published.normalized,
        lock: Some(published.lock),
        private_directory,
        attempt_directory: published.kept,
        result_directory,
        attempt_number: INITIAL_ATTEMPT_NUMBER,
        finalizers: Arc::from(fresh_finalizer_progress(admitted)?),
        state,
        resumed_wait: None,
    })
}

/// The staging handles and the `local-run.json` a run's own content is staged against.
struct RunStaging<'a> {
    root: &'a OwnedFd,
    attempts: &'a OwnedFd,
    run: &'a LocalRunV1,
}

/// What one kind of run stages beyond the shared layout: its `state.json`, whether it holds
/// `controls/` and a root `wait.json`, and what its creator keeps of the staging.
struct StagedRunContent<Kept> {
    state: LocalRunStateV1,
    controls: bool,
    wait: bool,
    kept: Kept,
}

/// A run directory `publish_run_directory` published, its lock still held.
struct PublishedRunDirectory<Kept> {
    normalized: PathBuf,
    root: OwnedFd,
    private: OwnedFd,
    lock: File,
    state: LocalRunStateV1,
    kept: Kept,
}

/// Stages a run directory under a sibling `.run-*` root and publishes it with one
/// no-replace rename (INV-ENGINE-19), so it appears complete or not at all: `run.lock` held,
/// `workflow/` retained from `admitted` (REQ-LOCALRUN-170), `attempts/`, `.private/`,
/// `local-run.json` with `git_baseline`'s answer, `run.json` when `record` is given, then
/// what `stage` writes and the `state.json` it answers, the whole verified before the
/// rename. `create` stages a first attempt and `create_successor` imports a predecessor's.
fn publish_run_directory<Kept>(
    requested: &Path,
    admitted: &AdmittedWorkflow,
    record: Option<&WorkflowRunRecord>,
    git_baseline: impl FnOnce() -> Option<GitBaselineV1>,
    stage: impl FnOnce(RunStaging<'_>) -> Result<StagedRunContent<Kept>, LocalRunDirectoryError>,
    observer: &mut impl InitialPublicationObserver,
) -> Result<PublishedRunDirectory<Kept>, LocalRunDirectoryError> {
    if record.is_some_and(|record| !record.is_valid()) {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let target = RunDirectoryTarget::validate(requested, admitted)?;
    let (staging_name, staging_root) =
        create_staging_root(&target.parent, ".run", STAGING_ATTEMPTS)
            .map_err(|()| LocalRunDirectoryError::StagingUnavailable)?;
    let mut staging = InitialStaging {
        parent: &target.parent,
        name: staging_name,
        root: staging_root,
        committed: false,
    };
    let run_staging = create_run_staging(&staging.root, &target.suffix)?;

    let lock = create_file(&run_staging, LOCK_FILE, Mode::RUSR | Mode::WUSR)?;
    fcntl_lock(&lock, FlockOperation::NonBlockingLockExclusive)
        .map_err(|source| file_error(&run_staging, LOCK_FILE, "lock", source))?;

    mkdir(&run_staging, WORKFLOW_DIRECTORY)?;
    let workflow_directory = open_directory_at(&run_staging, WORKFLOW_DIRECTORY)?;
    mkdir(&workflow_directory, WORKFLOW_FILES_DIRECTORY)?;
    let workflow_files = open_directory_at(&workflow_directory, WORKFLOW_FILES_DIRECTORY)?;
    mkdir(&run_staging, ATTEMPTS_DIRECTORY)?;
    let attempts = open_directory_at(&run_staging, ATTEMPTS_DIRECTORY)?;
    mkdir(&run_staging, PRIVATE_DIRECTORY)?;
    let private = open_directory_at(&run_staging, PRIVATE_DIRECTORY)?;

    let mut manifest = retain_execution_specification(&workflow_files, admitted)?;
    manifest.tree = admitted
        .tree()
        .map(|tree| retain_tree_copy(&workflow_directory, tree))
        .transpose()?;
    let manifest_bytes = encode_json(&manifest)?;
    write_new_immutable_file(&workflow_directory, WORKFLOW_MANIFEST_FILE, &manifest_bytes)?;

    let run = LocalRunV1 {
        schema_version: 1,
        local_run_id: generate_uuid()?,
        created_at: timestamp(um_support::utc_now())?,
        workflow_digest: admitted_workflow_digest(admitted),
        workflow_manifest_digest: DigestV1::sha256(&manifest_bytes),
        git_baseline: git_baseline(),
    };
    validate_run(&run)?;
    let run_bytes = encode_json(&run)?;
    write_new_immutable_file(&run_staging, LOCAL_RUN_FILE, &run_bytes)?;
    if let Some(record) = record {
        // Rewritten as attempts start and settle, so not immutable.
        tree_copy::write_new_file_with_final_mode(
            &run_staging,
            RUN_RECORD_FILE,
            &encode_json(record)?,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(file_operation_error)?;
    }

    let content = stage(RunStaging {
        root: &run_staging,
        attempts: &attempts,
        run: &run,
    })?;
    validate_state(&content.state)?;
    let state_bytes = encode_json(&content.state)?;
    decode_state(&state_bytes)?;
    write_new_state_file(&run_staging, &state_bytes)?;
    sync_directory(&workflow_files)?;
    sync_directory(&workflow_directory)?;
    sync_directory(&attempts)?;
    sync_directory(&private)?;
    sync_directory(&run_staging)?;
    sync_directory(&staging.root)?;
    verify_initial_staging(
        &run_staging,
        &run,
        &content.state,
        record,
        content.controls,
        content.wait,
    )?;
    let retained_root =
        dup(&run_staging).map_err(|_| LocalRunDirectoryError::PublicationUnavailable)?;

    target.verify_parent_and_absence()?;
    renameat_with(
        &target.parent,
        staging.name.as_ref(),
        &target.parent,
        &target.name,
        RenameFlags::NOREPLACE,
    )
    .map_err(|failure| match failure {
        Errno::EXIST | Errno::NOTEMPTY => LocalRunDirectoryError::DestinationExists,
        _ => LocalRunDirectoryError::PublicationUnavailable,
    })?;
    staging.committed = true;
    // Process-termination recovery relies on atomic visibility, not host-power-loss
    // durability; a post-rename directory sync cannot revoke the published commit.
    let _ = sync_directory(&target.parent);
    observer.published(&target.normalized, &lock)?;
    Ok(PublishedRunDirectory {
        normalized: target.normalized,
        root: retained_root,
        private,
        lock,
        state: content.state,
        kept: content.kept,
    })
}

/// Stages a successor's import of the waiting run `predecessor` holds (REQ-LOCALRUN-195,
/// INV-ENGINE-26): its `attempts/` copied entire, its root `workflow/` copied under the first
/// attempt that ran under it, its `controls/` copied when it has them, its root `wait.json`
/// copied when `wait` carries it (REQ-LOCALRUN-197), and the state of its attempts
/// re-located and marked imported from `predecessor_id` under a fresh state identity.
fn stage_successor_import(
    predecessor: &PendingLocalContinuation,
    predecessor_id: &str,
    wait: bool,
    staging: &RunStaging<'_>,
) -> Result<StagedRunContent<()>, LocalRunDirectoryError> {
    copy_directory_contents(
        &open_directory_at(&predecessor.root, ATTEMPTS_DIRECTORY)?,
        staging.attempts,
    )?;
    let (attempts, holder) =
        relocate_run_definitions(&predecessor.run, &predecessor.state, predecessor_id)?;
    if let Some(holder) = holder {
        let name = attempt_directory_name(holder).ok_or(LocalRunDirectoryError::StateInvalid)?;
        let attempt = open_directory_at(staging.attempts, &name)?;
        copy_entry(&predecessor.root, WORKFLOW_DIRECTORY, &attempt)?;
        sync_directory(&attempt)?;
    }
    let controls = match statat(
        &predecessor.root,
        CONTROLS_DIRECTORY,
        AtFlags::SYMLINK_NOFOLLOW,
    ) {
        Ok(_) => true,
        Err(Errno::NOENT) => false,
        Err(source) => {
            return Err(file_error(
                &predecessor.root,
                CONTROLS_DIRECTORY,
                "stat",
                source,
            ));
        }
    };
    if controls {
        copy_entry(&predecessor.root, CONTROLS_DIRECTORY, staging.root)?;
    }
    if wait {
        copy_entry(&predecessor.root, WAIT_RECORD_FILE, staging.root)?;
    }
    Ok(StagedRunContent {
        state: LocalRunStateV1 {
            schema_version: 1,
            local_run_id: staging.run.local_run_id.clone(),
            revision: 1,
            current_attempt_number: predecessor.state.current_attempt_number,
            attempts,
            diagnostics: predecessor.state.diagnostics.clone(),
        },
        controls,
        wait,
        kept: (),
    })
}

/// The predecessor's attempts as a successor imports them (REQ-LOCALRUN-195), each marked
/// imported from `predecessor_id` unless it already carries the mark of the run it was
/// recorded under, and the first attempt that ran under the predecessor's own
/// definition: that attempt's definition is re-located to `attempt` itself, where the
/// successor keeps the predecessor's `workflow/`, and each later one that ran under it to
/// `prior_attempt` naming it, both with the predecessor's digests. An attempt that ran under
/// a replacement keeps its own locator.
fn relocate_run_definitions(
    run: &LocalRunV1,
    state: &LocalRunStateV1,
    predecessor_id: &str,
) -> Result<(Vec<LocalAttemptV1>, Option<u64>), LocalRunDirectoryError> {
    let mut holder = None;
    let mut attempts = Vec::with_capacity(state.attempts.len());
    for attempt in &state.attempts {
        let mut imported = attempt.clone();
        if resolved_attempt_definition(run, state, attempt)?.locator
            == AttemptDefinitionLocatorV1::Run
        {
            let attempt_number = *holder.get_or_insert(attempt.attempt_number);
            imported.definition = Some(AttemptDefinitionV1 {
                locator: if attempt_number == attempt.attempt_number {
                    AttemptDefinitionLocatorV1::Attempt { attempt_number }
                } else {
                    AttemptDefinitionLocatorV1::PriorAttempt { attempt_number }
                },
                ..attempt_definition_for_run(run)
            });
        }
        imported.imported.get_or_insert_with(|| ImportedAttemptV1 {
            run: predecessor_id.to_owned(),
        });
        attempts.push(imported);
    }
    Ok((attempts, holder))
}

/// Copies every entry of `from` into the directory `to` as `copy_entry` copies one, then
/// syncs `to`.
fn copy_directory_contents(from: &OwnedFd, to: &OwnedFd) -> Result<(), LocalRunDirectoryError> {
    for name in directory_entries(from)? {
        let name = std::str::from_utf8(&name).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        copy_entry(from, name, to)?;
    }
    sync_directory(to)
}

/// Copies the entry `name` of `from` into `to` under the same name and mode: a regular file's
/// bytes, or a directory and everything under it, its mode set once its contents are written,
/// since a read-only directory refuses new entries. Any other kind of entry is
/// `StateInvalid`.
fn copy_entry(from: &OwnedFd, name: &str, to: &OwnedFd) -> Result<(), LocalRunDirectoryError> {
    let stat = statat(from, name, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|source| file_error(from, name, "stat", source))?;
    let mode = Mode::from_raw_mode(stat.st_mode);
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::Directory => {
            mkdir(to, name)?;
            let source = open_directory_at(from, name)?;
            let copy = open_directory_at(to, name)?;
            copy_directory_contents(&source, &copy)?;
            fchmod(&copy, mode)
                .map_err(|source| file_error(to, name, "set permissions", source))?;
            sync_directory(&copy)
        }
        FileType::RegularFile => {
            let mut source = openat(
                from,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(|source| file_error(from, name, "open", source))?;
            let mut copy = create_file(to, name, Mode::RUSR | Mode::WUSR)?;
            io::copy(&mut source, &mut copy)
                .and_then(|_| copy.sync_all())
                .map_err(|source| file_error(to, name, "copy", source))?;
            fchmod(&copy, mode)
                .map_err(|source| file_error(to, name, "set permissions", source))?;
            copy.sync_all()
                .map_err(|source| file_error(to, name, "sync", source))
        }
        _ => Err(LocalRunDirectoryError::StateInvalid),
    }
}

/// Creates the child run directory `children/<step>/<NNNNNN>/`, or for fan-out element
/// `item` `children/<step>/<n>/<NNNNNN>/`, under `parent`'s run directory with the next
/// ordinal there (REQ-LOCALRUN-177, INV-ENGINE-19), as `create_with_record` creates a run:
/// the tests' entry point to what `ChildRunParent::create_child` does for the CLI's
/// dispatcher. `admitted` is the child's admission from the parent's tree copy, so the
/// child's `workflow/tree/` is written from the pinned bytes with the parent's digest;
/// `record` is `WorkflowRunRecord::child_of` the parent's record for `step` and `item`. A
/// record that does not descend from the parent's `run.json` for `step` and `item`, or whose
/// tree is not the admitted one, is refused before anything is written.
#[cfg(test)]
pub(crate) fn create_child(
    parent: &LocalAttemptOwner,
    step: &str,
    item: Option<u64>,
    admitted: &AdmittedWorkflow,
    record: &WorkflowRunRecord,
) -> Result<InitialLocalRun, LocalRunDirectoryError> {
    create_child_under(
        &parent.state.root,
        &parent.normalized,
        step,
        item,
        admitted,
        record,
    )
}

/// What a running parent attempt lends the child runs its `workflow` steps start
/// (REQ-WFEXEC-59): its run directory and record, without the attempt's ownership, so a
/// dispatcher can create `children/<step>/<NNNNNN>/` while the attempt runs.
#[derive(Clone)]
pub struct ChildRunParent {
    root: Arc<OwnedFd>,
    normalized: PathBuf,
    record: WorkflowRunRecord,
}

impl ChildRunParent {
    /// The parent's `run.json`, which a child's record descends from.
    pub fn record(&self) -> &WorkflowRunRecord {
        &self.record
    }

    /// `create_child` under this parent: the child of `step`, or of its fan-out element
    /// `item`, counted from 1.
    pub fn create_child(
        &self,
        step: &str,
        item: Option<u64>,
        admitted: &AdmittedWorkflow,
        record: &WorkflowRunRecord,
    ) -> Result<InitialLocalRun, LocalRunDirectoryError> {
        create_child_under(&self.root, &self.normalized, step, item, admitted, record)
    }
}

impl LocalAttemptOwner {
    /// The handle a child-run dispatcher creates this run's children through; a run that
    /// holds no `run.json` has none.
    pub fn child_run_parent(&self) -> Result<ChildRunParent, LocalRunDirectoryError> {
        Ok(ChildRunParent {
            root: Arc::clone(&self.state.root),
            normalized: self.normalized.clone(),
            record: read_run_record(&self.state.root)?,
        })
    }
}

/// Creates the child as `create_child` describes. Before the child's directory exists it
/// rewrites the parent's `run.json` `position` to `[<step>]`, or `[<step>, {"item": n}]` for
/// a fan-out element, so the position never trails a child that exists; the parent attempt
/// holds the run lock, which serializes the rewrite as it does `count_run_record_edge`.
fn create_child_under(
    parent_root: &OwnedFd,
    parent_directory: &Path,
    step: &str,
    item: Option<u64>,
    admitted: &AdmittedWorkflow,
    record: &WorkflowRunRecord,
) -> Result<InitialLocalRun, LocalRunDirectoryError> {
    let parent_record = read_run_record(parent_root)?;
    let tree = admitted
        .tree()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let admitted_tree = format!(
        "{}:{}",
        tree.digest().algorithm.as_str(),
        tree.digest().value
    );
    let descends = record.parent.as_ref().is_some_and(|named| {
        named.run == parent_record.id && named.step == step && named.item == item
    }) && record.definition == parent_record.definition
        && record.revision == parent_record.revision
        && record.owner == parent_record.owner
        && record.tree == parent_record.tree
        && record.tree == admitted_tree;
    let element = item
        .map(|item| attempt_directory_name(item).ok_or(LocalRunDirectoryError::StateInvalid))
        .transpose()?;
    if !descends || !record.is_valid() {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let position = std::iter::once(Value::String(step.to_owned()))
        .chain(item.map(super::run_record::item_position_element))
        .collect::<Vec<_>>();
    rewrite_run_record(parent_root, |record| {
        record.position = position;
        Ok(())
    })?;
    let mut child_directory = parent_directory.join(CHILDREN_DIRECTORY).join(step);
    let mut directory = create_or_open_directory(parent_root, CHILDREN_DIRECTORY)
        .and_then(|children| create_or_open_directory(&children, step))?;
    if let Some(element) = element {
        directory = create_or_open_directory(&directory, &element)?;
        child_directory.push(element);
    }
    let name = attempt_directory_name(next_ordinal_in(&directory)?)
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    InitialLocalRun::create_with_record(&child_directory.join(name), admitted, Some(record))
}

/// The ordinal `create_child` gives `step`'s next child run (REQ-LOCALRUN-177): one past
/// the highest ordinal under `children/<step>/`, read from the directory rather than kept
/// in a counter, so `1` before the step has run. The tests' view of `next_ordinal_in`.
#[cfg(test)]
pub(crate) fn next_child_ordinal(
    parent: &LocalAttemptOwner,
    step: &str,
) -> Result<u64, LocalRunDirectoryError> {
    if !super::schema_common::is_identifier(step) {
        return Err(LocalRunDirectoryError::InvalidPath);
    }
    let Some(children) = open_optional_directory(&parent.state.root, CHILDREN_DIRECTORY)? else {
        return Ok(1);
    };
    match open_optional_directory(&children, step)? {
        Some(step_directory) => next_ordinal_in(&step_directory),
        None => Ok(1),
    }
}

/// One past the highest six-digit ordinal entry, ignoring anything else, such as the
/// staging root an interrupted creation left.
fn next_ordinal_in(directory: &OwnedFd) -> Result<u64, LocalRunDirectoryError> {
    ordinals_in(directory)?
        .last()
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(LocalRunDirectoryError::StateInvalid)
}

/// The six-digit ordinal entries of `directory` in ascending order, ignoring anything else.
fn ordinals_in(directory: &OwnedFd) -> Result<Vec<u64>, LocalRunDirectoryError> {
    let mut ordinals = directory_entries(directory)?
        .iter()
        .filter_map(|name| {
            let name = std::str::from_utf8(name).ok()?;
            let ordinal = name
                .bytes()
                .all(|byte| byte.is_ascii_digit())
                .then(|| name.parse::<u64>().ok())??;
            (attempt_directory_name(ordinal).as_deref() == Some(name)).then_some(ordinal)
        })
        .collect::<Vec<_>>();
    ordinals.sort_unstable();
    Ok(ordinals)
}

/// One six-digit entry of `children/<step>/` (REQ-LOCALRUN-177): a plain child run, which
/// holds `run.json`, or the directory of a fan-out element's child runs, which does not.
enum StepChildEntry {
    Child(Box<WorkflowRunRecord>),
    Element(OwnedFd),
}

/// The entries of `children/<step>/` in ordinal order with their names.
fn step_child_entries(
    step_directory: &OwnedFd,
) -> Result<Vec<(u64, String, StepChildEntry)>, LocalRunDirectoryError> {
    ordinals_in(step_directory)?
        .into_iter()
        .map(|ordinal| {
            let name =
                attempt_directory_name(ordinal).ok_or(LocalRunDirectoryError::StateInvalid)?;
            let entry = open_directory_at(step_directory, &name)?;
            let entry = match read_optional_run_record(&entry)? {
                Some(record) => StepChildEntry::Child(Box::new(record)),
                None => StepChildEntry::Element(entry),
            };
            Ok((ordinal, name, entry))
        })
        .collect()
}

/// The highest-ordinal child run in `directory`, its name and record, or `None` when it
/// holds none.
fn latest_child_in(
    directory: &OwnedFd,
) -> Result<Option<(String, WorkflowRunRecord)>, LocalRunDirectoryError> {
    let Some(ordinal) = ordinals_in(directory)?.last().copied() else {
        return Ok(None);
    };
    let name = attempt_directory_name(ordinal).ok_or(LocalRunDirectoryError::StateInvalid)?;
    let record = read_run_record(&open_directory_at(directory, &name)?)?;
    Ok(Some((name, record)))
}

fn open_optional_directory(
    parent: &OwnedFd,
    name: &str,
) -> Result<Option<OwnedFd>, LocalRunDirectoryError> {
    read_if_present(parent, name, |parent| open_directory_at(parent, name))
}

/// `read(parent)` when `name` exists in `parent` and `None` when it does not; a `stat`
/// failing otherwise is the file's error.
fn read_if_present<T>(
    parent: &OwnedFd,
    name: &str,
    read: impl FnOnce(&OwnedFd) -> Result<T, LocalRunDirectoryError>,
) -> Result<Option<T>, LocalRunDirectoryError> {
    match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => read(parent).map(Some),
        Err(Errno::NOENT) => Ok(None),
        Err(source) => Err(file_error(parent, name, "stat", source)),
    }
}

struct RunDirectoryTarget {
    supplied_parent: PathBuf,
    parent: OwnedFd,
    name: OsString,
    suffix: Vec<OsString>,
    normalized: PathBuf,
    execution_root: AdmittedExecutionRoot,
}

impl RunDirectoryTarget {
    fn validate(
        requested: &Path,
        admitted: &AdmittedWorkflow,
    ) -> Result<Self, LocalRunDirectoryError> {
        let requested = if requested.is_absolute() {
            requested.to_owned()
        } else {
            std::env::current_dir()
                .map_err(|source| {
                    path_error(requested.to_owned(), "resolve run directory", source)
                })?
                .join(requested)
        };
        let (supplied_parent, suffix) = nearest_existing_parent(&requested)?;
        let canonical_parent = std::fs::canonicalize(&supplied_parent)
            .map_err(|source| path_error(supplied_parent.clone(), "resolve run parent", source))?;
        let parent = open_directory_path(&canonical_parent)
            .map_err(|source| path_error(canonical_parent.clone(), "open run parent", source))?;
        let name = suffix
            .first()
            .ok_or(LocalRunDirectoryError::DestinationExists)?
            .clone();
        ensure_absent(&parent, &name)?;
        let normalized = suffix
            .iter()
            .fold(canonical_parent, |path, component| path.join(component));
        if normalized.to_str().is_none() {
            return Err(LocalRunDirectoryError::InvalidPath);
        }
        // The tree copy's `programs` directories lead `PATH` (REQ-WFEXEC-53), and a `PATH`
        // entry cannot contain `:`; refuse before anything is written.
        if admitted
            .tree()
            .is_some_and(|tree| !tree.programs().is_empty())
            && normalized.as_os_str().as_encoded_bytes().contains(&b':')
        {
            return Err(LocalRunDirectoryError::InvalidPath);
        }
        let execution_root = admitted.execution().root_identity().clone();
        if run_directory_overlaps_execution_root(
            &normalized,
            admitted.execution().root(),
            &execution_root,
            &parent,
        )? {
            return Err(LocalRunDirectoryError::ExecutionRootOverlap);
        }
        Ok(Self {
            supplied_parent,
            parent,
            name,
            suffix,
            normalized,
            execution_root,
        })
    }

    fn verify_parent_and_absence(&self) -> Result<(), LocalRunDirectoryError> {
        let rebound = std::fs::canonicalize(&self.supplied_parent)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?;
        let rebound =
            open_directory_path(&rebound).map_err(|_| LocalRunDirectoryError::ParentUnavailable)?;
        if !same_file(&self.parent, &rebound)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?
        {
            return Err(LocalRunDirectoryError::ParentUnavailable);
        }
        if run_directory_overlaps_execution_root(
            &self.normalized,
            self.execution_root.provenance_path(),
            &self.execution_root,
            &self.parent,
        )? {
            return Err(LocalRunDirectoryError::ExecutionRootOverlap);
        }
        ensure_absent(&self.parent, &self.name)
    }
}

fn nearest_existing_parent(
    requested: &Path,
) -> Result<(PathBuf, Vec<OsString>), LocalRunDirectoryError> {
    let mut candidate = requested.to_owned();
    let mut suffix = VecDeque::new();
    loop {
        match std::fs::symlink_metadata(&candidate) {
            Ok(_) if suffix.is_empty() => {
                return Err(LocalRunDirectoryError::DestinationExists);
            }
            Ok(_) => break,
            Err(failure) if failure.kind() == io::ErrorKind::NotFound => {
                let component = candidate
                    .file_name()
                    .filter(|component| *component != "." && *component != "..")
                    .ok_or(LocalRunDirectoryError::InvalidPath)?;
                if component.to_str().is_none() {
                    return Err(LocalRunDirectoryError::InvalidPath);
                }
                suffix.push_front(component.to_owned());
                if !candidate.pop() {
                    return Err(LocalRunDirectoryError::ParentUnavailable);
                }
            }
            Err(_) => return Err(LocalRunDirectoryError::ParentUnavailable),
        }
    }
    if suffix.is_empty() {
        return Err(LocalRunDirectoryError::DestinationExists);
    }
    Ok((candidate, suffix.into_iter().collect()))
}

fn create_run_staging(
    staging_root: &OwnedFd,
    suffix: &[OsString],
) -> Result<OwnedFd, LocalRunDirectoryError> {
    let mut current = dup(staging_root).map_err(|_| LocalRunDirectoryError::StagingUnavailable)?;
    for component in suffix.iter().skip(1) {
        mkdir(&current, component)?;
        current = open_directory_at(&current, component)?;
    }
    Ok(current)
}

struct InitialStaging<'a> {
    parent: &'a OwnedFd,
    name: Arc<str>,
    root: OwnedFd,
    committed: bool,
}

impl Drop for InitialStaging<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = remove_staging_root(self.parent, &self.name, &self.root);
        }
    }
}

fn retain_execution_specification(
    files: &OwnedFd,
    admitted: &AdmittedWorkflow,
) -> Result<WorkflowManifestV1, LocalRunDirectoryError> {
    let source_root = admitted
        .workflow()
        .source
        .source_root
        .to_str()
        .ok_or(LocalRunDirectoryError::InvalidPath)?
        .to_owned();
    let mut ordinal = 0_u64;
    let mut source_files = Vec::with_capacity(admitted.workflow().source_closure.len());
    for (path, bytes) in &admitted.workflow().source_closure {
        ordinal = ordinal
            .checked_add(1)
            .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
        let file = retain_file(files, ordinal, bytes)?;
        source_files.push(ManifestSourceFileV1 {
            path: path.clone(),
            file,
        });
    }
    let mut inputs = BTreeMap::new();
    for (name, input) in admitted.inputs().values() {
        let retained = match input {
            ResolvedInput::Text(text) => {
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
                ManifestInputV1::Text {
                    file: retain_file(files, ordinal, text.as_bytes())?,
                }
            }
            ResolvedInput::Json(json) => {
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
                ManifestInputV1::Json {
                    file: retain_file(files, ordinal, json.source())?,
                }
            }
            ResolvedInput::File(value) => {
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
                ManifestInputV1::File {
                    media_type: value.media_type().to_owned(),
                    file: retain_file(files, ordinal, value.bytes())?,
                }
            }
            ResolvedInput::Attachments(attachments) => {
                let mut items = Vec::with_capacity(attachments.len());
                for attachment in attachments.iter() {
                    ordinal = ordinal
                        .checked_add(1)
                        .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
                    items.push(ManifestAttachmentV1 {
                        media_type: attachment.media_type().to_owned(),
                        file: retain_file(files, ordinal, attachment.bytes())?,
                    });
                }
                ManifestInputV1::Attachments { items }
            }
        };
        inputs.insert(name.clone(), retained);
    }
    let agent_profile_replacement = admitted
        .workflow()
        .agent_profile_replacement
        .as_ref()
        .map(|replacement| {
            ordinal = ordinal
                .checked_add(1)
                .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
            retain_file(files, ordinal, replacement.document.bytes())
        })
        .transpose()?;
    let manifest = WorkflowManifestV1 {
        schema_version: 1,
        workflow_path: admitted.workflow().source.workflow_path.clone(),
        source_root,
        maximum_parallel_steps: admitted.execution().limits().maximum_parallel_steps().get(),
        source_files,
        inputs,
        agent_profile_replacement,
        tree: None,
    };
    validate_manifest(&manifest)?;
    Ok(manifest)
}

/// Copies the admitted tree read-only under `workflow/tree/` (REQ-LOCALRUN-170), as
/// `tree_copy::write_tree_copy` writes it.
fn retain_tree_copy(
    workflow_directory: &OwnedFd,
    tree: &AdmittedTree,
) -> Result<ManifestTreeV1, LocalRunDirectoryError> {
    mkdir(workflow_directory, WORKFLOW_TREE_DIRECTORY)?;
    let copy = open_directory_at(workflow_directory, WORKFLOW_TREE_DIRECTORY)?;
    write_tree_copy(
        &copy,
        tree.files()
            .iter()
            .map(|(path, file)| (path.as_str(), file)),
        tree.programs(),
    )
    .map_err(|error| match error {
        TreeCopyError::Layout => LocalRunDirectoryError::SerializationUnavailable,
        TreeCopyError::File(error) => file_operation_error(error),
    })?;
    let record = ManifestTreeV1 {
        digest: DigestV1 {
            algorithm: tree.digest().algorithm.as_str().to_owned(),
            value: tree.digest().value.clone(),
        },
        relative_directory: WORKFLOW_TREE_DIRECTORY.to_owned(),
        programs: tree.programs().to_vec(),
        required_tools: tree.required_tools().to_vec(),
    };
    validate_manifest_tree(&record)?;
    Ok(record)
}

fn retain_file(
    directory: &OwnedFd,
    ordinal: u64,
    bytes: &[u8],
) -> Result<ManifestFileV1, LocalRunDirectoryError> {
    let name = retained_file_name(ordinal)?;
    write_new_immutable_file(directory, &name, bytes)?;
    Ok(ManifestFileV1 {
        ordinal,
        relative_file: format!("{WORKFLOW_FILES_DIRECTORY}/{name}"),
        size_bytes: u64::try_from(bytes.len())
            .map_err(|_| LocalRunDirectoryError::SerializationUnavailable)?,
        digest: DigestV1::sha256(bytes),
    })
}

fn capture_initial_git_baseline(admitted: &AdmittedWorkflow) -> GitBaselineV1 {
    let admitted_baseline = admitted.git_capture().map(GitCaptureContext::baseline);
    let baseline = admitted_baseline.map_or_else(
        || {
            GitCaptureContext::admit_local(
                admitted.execution(),
                &super::artifact::CaptureCancellation::default(),
            )
            .map(|context| context.baseline())
        },
        Ok,
    );
    match baseline {
        Ok(baseline) => GitBaselineV1::Available {
            object_format: baseline.object_format().as_str().to_owned(),
            commit_oid: baseline.commit_oid().to_owned(),
        },
        Err(failure) => GitBaselineV1::Unavailable {
            reason: git_baseline_unavailable_reason(failure),
        },
    }
}

const fn git_baseline_unavailable_reason(
    failure: GitWorkspaceAdmissionFailure,
) -> GitBaselineUnavailableReasonV1 {
    match failure {
        GitWorkspaceAdmissionFailure::Cancelled => GitBaselineUnavailableReasonV1::Cancelled,
        GitWorkspaceAdmissionFailure::ExecutionRootRebound => {
            GitBaselineUnavailableReasonV1::ExecutionRootRebound
        }
        GitWorkspaceAdmissionFailure::GitUnavailable => {
            GitBaselineUnavailableReasonV1::GitUnavailable
        }
        GitWorkspaceAdmissionFailure::GitTimedOut => GitBaselineUnavailableReasonV1::GitTimedOut,
        GitWorkspaceAdmissionFailure::GitOutputLimitExceeded => {
            GitBaselineUnavailableReasonV1::GitOutputLimitExceeded
        }
        GitWorkspaceAdmissionFailure::NotWorkTree => GitBaselineUnavailableReasonV1::NotWorkTree,
        GitWorkspaceAdmissionFailure::ExecutionRootNotWorkTreeRoot => {
            GitBaselineUnavailableReasonV1::ExecutionRootNotWorkTreeRoot
        }
        GitWorkspaceAdmissionFailure::UnsupportedObjectFormat => {
            GitBaselineUnavailableReasonV1::UnsupportedObjectFormat
        }
        GitWorkspaceAdmissionFailure::BaselineUnavailable => {
            GitBaselineUnavailableReasonV1::BaselineUnavailable
        }
        GitWorkspaceAdmissionFailure::InitialWorkspaceDirty => {
            GitBaselineUnavailableReasonV1::InitialWorkspaceDirty
        }
    }
}

/// The admitted workflow's content digest as the run's documents record it.
fn admitted_workflow_digest(admitted: &AdmittedWorkflow) -> DigestV1 {
    DigestV1 {
        algorithm: admitted
            .workflow()
            .content_digest
            .algorithm
            .as_str()
            .to_owned(),
        value: admitted.workflow().content_digest.value.clone(),
    }
}

fn attempt_definition_for_run(run: &LocalRunV1) -> AttemptDefinitionV1 {
    AttemptDefinitionV1 {
        digest: run.workflow_digest.clone(),
        manifest_digest: run.workflow_manifest_digest.clone(),
        locator: AttemptDefinitionLocatorV1::Run,
    }
}

fn initial_state(
    admitted: &AdmittedWorkflow,
    run: &LocalRunV1,
    local_run_id: String,
    created_at: String,
) -> Result<LocalRunStateV1, LocalRunDirectoryError> {
    let attempt = fresh_attempt(
        admitted,
        INITIAL_ATTEMPT_NUMBER,
        AttemptTriggerV1::Initial,
        None,
        attempt_definition_for_run(run),
        created_at,
    )?;
    Ok(LocalRunStateV1 {
        schema_version: 1,
        local_run_id,
        revision: 1,
        current_attempt_number: INITIAL_ATTEMPT_NUMBER,
        attempts: vec![attempt],
        diagnostics: Vec::new(),
    })
}

type RetainedOutputSets = BTreeMap<(AttemptNodeRoleV1, String), Vec<RetainedOutputV1>>;

impl StateStore {
    fn retain_outputs<Deadline>(
        &self,
        runtime: &super::runtime::RuntimeState<
            StepFailureCause,
            super::value::CapturedValue,
            Deadline,
        >,
        finalizers: &[AttemptStepV1],
        artifacts: &ArtifactStaging,
    ) -> Result<RetainedOutputSets, LocalRunDirectoryError> {
        let (attempt_number, durable_state) = {
            let state = lock_state(&self.current)?;
            let attempt = current_attempt(&state)?;
            (attempt.attempt_number, state.clone())
        };
        let attempts = open_directory_at(&self.root, ATTEMPTS_DIRECTORY)?;
        let attempt_name =
            attempt_directory_name(attempt_number).ok_or(LocalRunDirectoryError::StateInvalid)?;
        let attempt = open_directory_at(&attempts, &attempt_name)?;
        let values = create_or_open_directory(&attempt, VALUES_DIRECTORY)?;
        let mut retained = BTreeMap::new();
        for (node, node_runtime) in &runtime.steps {
            let role = if finalizers.iter().any(|finalizer| finalizer.id == *node) {
                AttemptNodeRoleV1::Finalizer
            } else {
                AttemptNodeRoleV1::Step
            };
            let outputs = match &node_runtime.state {
                StepState::Succeeded { outputs } => outputs,
                StepState::Inherited {
                    disposition: super::runtime::InheritedDisposition::Succeeded,
                    outputs,
                    ..
                } => {
                    let descriptors = inherited_retained_outputs(
                        &durable_state,
                        attempt_number,
                        node,
                        outputs,
                        &runtime.output_producers,
                    )?;
                    retained.insert((role, node.clone()), descriptors);
                    continue;
                }
                StepState::Inherited {
                    disposition: super::runtime::InheritedDisposition::Skipped,
                    ..
                } => {
                    retained.insert((role, node.clone()), Vec::new());
                    continue;
                }
                _ => continue,
            };
            let role_directory = create_or_open_directory(&values, retained_role_name(role))?;
            let node_directory = create_or_open_directory(&role_directory, node)?;
            let mut descriptors = Vec::with_capacity(outputs.len());
            for (name, value) in outputs {
                descriptors.push(retain_output_value_with_producer(
                    artifacts,
                    &node_directory,
                    role,
                    node,
                    name,
                    value,
                    None,
                )?);
            }
            sync_directory(&node_directory)?;
            sync_directory(&role_directory)?;
            retained.insert((role, node.clone()), descriptors);
        }
        sync_directory(&values)?;
        sync_directory(&attempt)?;
        Ok(retained)
    }
}

fn inherited_retained_outputs<Output>(
    state: &LocalRunStateV1,
    current_attempt_number: u64,
    node: &str,
    outputs: &super::runtime::OutputSet<Output>,
    producers: &BTreeMap<(String, String), super::runtime::OutputProducer>,
) -> Result<Vec<RetainedOutputV1>, LocalRunDirectoryError> {
    let mut retained = Vec::with_capacity(outputs.len());
    for name in outputs.keys() {
        let producer = producers
            .get(&(node.to_owned(), name.clone()))
            .filter(|producer| {
                producer.node == node
                    && producer.output == *name
                    && producer.attempt_number < current_attempt_number
            })
            .ok_or(LocalRunDirectoryError::StateConflict)?;
        let source_attempt = state
            .attempts
            .iter()
            .find(|attempt| {
                attempt.attempt_number == producer.attempt_number
                    && attempt.attempt_id == producer.attempt_id
            })
            .ok_or(LocalRunDirectoryError::StateConflict)?;
        let source_step = source_attempt
            .progress
            .steps
            .iter()
            .find(|step| step.id == node && step.state == AttemptStepStateV1::Succeeded)
            .ok_or(LocalRunDirectoryError::StateConflict)?;
        let mut descriptor = source_step
            .outputs
            .iter()
            .flatten()
            .find(|output| output.name() == name)
            .cloned()
            .ok_or(LocalRunDirectoryError::StateConflict)?;
        if descriptor
            .producer()
            .is_some_and(|retained_producer| retained_producer != producer)
        {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        descriptor.set_producer(producer.clone());
        retained.push(descriptor);
    }
    Ok(retained)
}

#[cfg(test)]
fn retain_output_value(
    artifacts: &ArtifactStaging,
    directory: &OwnedFd,
    role: AttemptNodeRoleV1,
    node: &str,
    name: &str,
    value: &super::value::CapturedValue,
) -> Result<RetainedOutputV1, LocalRunDirectoryError> {
    retain_output_value_with_producer(artifacts, directory, role, node, name, value, None)
}

fn retain_output_value_with_producer(
    artifacts: &ArtifactStaging,
    directory: &OwnedFd,
    role: AttemptNodeRoleV1,
    node: &str,
    name: &str,
    value: &super::value::CapturedValue,
    producer: Option<super::runtime::OutputProducer>,
) -> Result<RetainedOutputV1, LocalRunDirectoryError> {
    let relative_path = retained_value_relative_path(role, node, name);
    match value {
        super::value::CapturedValue::Text(text) => {
            let carrier = retain_bytes_carrier(
                directory,
                name,
                &relative_path,
                "text/plain; charset=utf-8",
                text.carrier(),
            )?;
            Ok(RetainedOutputV1::Text {
                name: name.to_owned(),
                producer,
                carrier,
            })
        }
        super::value::CapturedValue::Json(json) => {
            let carrier = retain_bytes_carrier(
                directory,
                name,
                &relative_path,
                "application/json",
                json.carrier(),
            )?;
            Ok(RetainedOutputV1::Json {
                name: name.to_owned(),
                producer,
                carrier,
            })
        }
        super::value::CapturedValue::File(file) => {
            if file.output_identity() != name {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            let carrier =
                retain_staged_carrier(artifacts, directory, name, &relative_path, file.carrier())?;
            Ok(RetainedOutputV1::File {
                name: name.to_owned(),
                producer,
                media_type: file.media_type().to_owned(),
                carrier,
            })
        }
        super::value::CapturedValue::GitBranch(branch) => {
            if branch.output_identity() != name {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            let metadata = branch.metadata();
            let carrier = branch
                .carrier()
                .map(|carrier| {
                    retain_staged_carrier(
                        artifacts,
                        directory,
                        name,
                        &relative_path,
                        carrier.staged(),
                    )
                })
                .transpose()?;
            Ok(RetainedOutputV1::GitBranch {
                name: name.to_owned(),
                producer,
                artifact_version: metadata.artifact_version(),
                object_format: metadata.object_format().as_str().to_owned(),
                base_oid: metadata.base_oid().to_owned(),
                head_oid: metadata.head_oid().to_owned(),
                tree_oid: metadata.tree_oid().to_owned(),
                carrier,
            })
        }
    }
}

fn retain_bytes_carrier(
    directory: &OwnedFd,
    name: &str,
    relative_path: &str,
    media_type: &str,
    bytes: &[u8],
) -> Result<RetainedCarrierV1, LocalRunDirectoryError> {
    write_or_verify_immutable_file(directory, name, bytes)?;
    let size_bytes =
        u64::try_from(bytes.len()).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    let carrier = RetainedCarrierV1 {
        relative_path: relative_path.to_owned(),
        media_type: media_type.to_owned(),
        size_bytes,
        digest: DigestV1::sha256(bytes),
    };
    verify_retained_carrier(directory, name, &carrier)?;
    Ok(carrier)
}

fn retain_staged_carrier(
    artifacts: &ArtifactStaging,
    directory: &OwnedFd,
    name: &str,
    relative_path: &str,
    staged: &super::artifact::StagedCarrier,
) -> Result<RetainedCarrierV1, LocalRunDirectoryError> {
    match statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
        Err(Errno::NOENT) => {
            let mut destination = create_file(directory, name, Mode::RUSR | Mode::WUSR)?;
            let copied = artifacts
                .copy_to(staged.handle(), &mut destination)
                .map_err(|source| LocalRunDirectoryError::Artifact {
                    path: file_locator(directory, name),
                    operation: "copy staged carrier",
                    source,
                })?;
            if copied != staged.size() {
                return Err(LocalRunDirectoryError::StateWriteUnavailable);
            }
            destination
                .flush()
                .and_then(|()| destination.sync_all())
                .map_err(|source| file_error(directory, name, "sync staged carrier", source))?;
            fchmod(destination.as_fd(), Mode::RUSR)
                .map_err(|source| file_error(directory, name, "set carrier permissions", source))?;
            destination
                .sync_all()
                .map_err(|source| file_error(directory, name, "sync staged carrier", source))?;
        }
        Ok(metadata) if FileType::from_raw_mode(metadata.st_mode) == FileType::RegularFile => {}
        Ok(_) | Err(_) => return Err(LocalRunDirectoryError::StateConflict),
    }
    let carrier = RetainedCarrierV1 {
        relative_path: relative_path.to_owned(),
        media_type: staged.media_type().to_owned(),
        size_bytes: staged.size(),
        digest: DigestV1 {
            algorithm: SHA256_ALGORITHM.to_owned(),
            value: staged.sha256().to_owned(),
        },
    };
    verify_retained_carrier(directory, name, &carrier)?;
    Ok(carrier)
}

fn verify_retained_carrier(
    directory: &OwnedFd,
    name: &str,
    carrier: &RetainedCarrierV1,
) -> Result<(), LocalRunDirectoryError> {
    verify_retained_carrier_with_sync(directory, name, carrier, true)
}

fn verify_retained_carrier_with_sync(
    directory: &OwnedFd,
    name: &str,
    carrier: &RetainedCarrierV1,
    synchronize: bool,
) -> Result<(), LocalRunDirectoryError> {
    let descriptor = openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|source| file_error(directory, name, "open carrier", source))?;
    let opened =
        fstat(&descriptor).map_err(|source| file_error(directory, name, "stat carrier", source))?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile
        || u64::try_from(opened.st_size) != Ok(carrier.size_bytes)
    {
        return Err(LocalRunDirectoryError::StateFile {
            path: file_locator(directory, name),
            operation: "verify carrier",
            source: Box::new(LocalRunDirectoryError::CarrierInvalid),
        });
    }
    let mut file = File::from(descriptor);
    if synchronize {
        file.sync_all()
            .map_err(|source| file_error(directory, name, "sync carrier", source))?;
    }
    let mut context = DigestContext::new(&SHA256);
    let mut observed = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| file_error(directory, name, "read carrier", source))?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(read).map_err(|_| LocalRunDirectoryError::StateInvalid)?)
            .filter(|bytes| *bytes <= carrier.size_bytes)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        context.update(&buffer[..read]);
    }
    let after =
        fstat(&file).map_err(|source| file_error(directory, name, "stat carrier", source))?;
    let named = statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|source| file_error(directory, name, "stat carrier", source))?;
    if observed != carrier.size_bytes
        || lowercase_hex(context.finish().as_ref()) != carrier.digest.value
        || opened.st_dev != after.st_dev
        || opened.st_ino != after.st_ino
        || opened.st_size != after.st_size
        || opened.st_dev != named.st_dev
        || opened.st_ino != named.st_ino
        || FileType::from_raw_mode(named.st_mode) != FileType::RegularFile
    {
        return Err(LocalRunDirectoryError::StateFile {
            path: file_locator(directory, name),
            operation: "verify carrier",
            source: Box::new(LocalRunDirectoryError::CarrierInvalid),
        });
    }
    Ok(())
}

const fn retained_role_name(role: AttemptNodeRoleV1) -> &'static str {
    match role {
        AttemptNodeRoleV1::Step => "steps",
        AttemptNodeRoleV1::Finalizer => "finalizers",
    }
}

fn update_invocation_ledger<Deadline>(
    store: &StateStore,
    attempt: &mut LocalAttemptV1,
    commit: &CommittedReduction<StepFailureCause, super::value::CapturedValue, Deadline>,
    diagnostics: &StepDiagnosticLog,
    accounting: &InvocationAccountingLog,
    now: &str,
) -> Result<(), LocalRunDirectoryError> {
    for (step_id, runtime) in &commit.state.steps {
        let Some(recovery) = runtime
            .recovery
            .as_ref()
            .filter(|recovery| !recovery.rounds.is_empty())
        else {
            continue;
        };
        for round in &recovery.rounds {
            let invocation_id = round.failed_execution.invocation.transition_sequence.get();
            if attempt
                .progress
                .invocations
                .iter()
                .any(|invocation| invocation.invocation_id == invocation_id)
            {
                continue;
            }
            let execution_number = round.failed_execution.execution_number.get();
            let pending = store
                .pending_recovery_invocations
                .lock()
                .map_err(|_| LocalRunDirectoryError::StateConflict)?
                .remove(&(attempt.attempt_number, step_id.clone(), execution_number));
            let started_at = match pending {
                Some((pending_invocation, started_at)) if pending_invocation == invocation_id => {
                    started_at
                }
                Some(_) => return Err(LocalRunDirectoryError::StateConflict),
                None => attempt.started_at.clone().unwrap_or_else(|| now.to_owned()),
            };
            attempt.progress.invocations.push(DurableInvocationV1 {
                invocation_id,
                step_id: step_id.clone(),
                node_role: AttemptNodeRoleV1::Step,
                role: super::publication::RecoveryInvocationRoleV1::Target,
                target_execution: Some(execution_number),
                recovery_round: None,
                state: DurableInvocationStateV1::Active,
                started_at,
                finished_at: None,
                usage: super::publication::RecoveryInvocationUsageV1::default(),
                diagnostics: Vec::new(),
                diagnostic_reference: None,
            });
        }
    }
    // Ordinary agents have no durable invocation until their result is known. An
    // interrupted attempt must not leave an Active invocation in terminal state.
    // Keep the launch identity in memory, as with a target awaiting recovery.
    let pending_agents = {
        let mut pending = store
            .pending_recovery_invocations
            .lock()
            .map_err(|_| LocalRunDirectoryError::StateConflict)?;
        let ready = pending
            .iter()
            .filter(|((number, step_id, execution_number), _)| {
                *number == attempt.attempt_number
                    && commit.state.invocation_is_agent(step_id, false)
                    && !commit.state.steps.get(step_id).is_some_and(|runtime| {
                        matches!(runtime.active_invocation, Some(ActiveStepInvocation::Target {
                            execution_number: active,
                        }) if active.get() == *execution_number)
                    })
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        for (key, _) in &ready {
            pending.remove(key);
        }
        ready
    };
    for ((_, step_id, execution_number), (invocation_id, started_at)) in pending_agents {
        if attempt
            .progress
            .invocations
            .iter()
            .any(|invocation| invocation.invocation_id == invocation_id)
        {
            continue;
        }
        ensure_invocation_capacity(attempt)?;
        attempt.progress.invocations.push(DurableInvocationV1 {
            invocation_id,
            node_role: attempt_node_role(attempt, &step_id)
                .ok_or(LocalRunDirectoryError::StateConflict)?,
            step_id,
            role: super::publication::RecoveryInvocationRoleV1::Target,
            target_execution: Some(execution_number),
            recovery_round: None,
            state: DurableInvocationStateV1::Active,
            started_at,
            finished_at: None,
            usage: super::publication::RecoveryInvocationUsageV1::default(),
            diagnostics: Vec::new(),
            diagnostic_reference: None,
        });
    }
    for action in &commit.actions {
        let (role, target_execution, recovery_round) = match action.kind {
            CommittedActionKind::StartStep => (
                super::publication::RecoveryInvocationRoleV1::Target,
                action.execution_number.map(TargetExecutionNumber::get),
                None,
            ),
            CommittedActionKind::StartRecoveryHandler => (
                super::publication::RecoveryInvocationRoleV1::RecoveryHandler,
                None,
                action
                    .recovery_round
                    .map(super::runtime::RecoveryRoundNumber::get),
            ),
            CommittedActionKind::CaptureOutputs
            | CommittedActionKind::CancelStep
            | CommittedActionKind::ForceAbortStep
            | CommittedActionKind::FinishRun => continue,
        };
        let step_id = action
            .step
            .as_ref()
            .ok_or(LocalRunDirectoryError::StateConflict)?;
        let runtime_recovery = commit
            .state
            .steps
            .get(step_id)
            .and_then(|runtime| runtime.recovery.as_ref());
        let recovery_active = runtime_recovery.is_some_and(|recovery| !recovery.rounds.is_empty());
        let invocation_id = action.id.transition_sequence.get();
        if !recovery_active
            || (matches!(action.kind, CommittedActionKind::StartStep)
                && commit.state.invocation_is_agent(step_id, false))
        {
            if (runtime_recovery.is_some() || commit.state.invocation_is_agent(step_id, false))
                && let Some(execution_number) = action.execution_number
            {
                let key = (
                    attempt.attempt_number,
                    step_id.clone(),
                    execution_number.get(),
                );
                let mut pending = store
                    .pending_recovery_invocations
                    .lock()
                    .map_err(|_| LocalRunDirectoryError::StateConflict)?;
                if let Some(retained) = pending.get(&key) {
                    if retained.0 != invocation_id {
                        return Err(LocalRunDirectoryError::StateConflict);
                    }
                } else {
                    pending.insert(key, (invocation_id, now.to_owned()));
                }
            }
            continue;
        }
        let node_role =
            attempt_node_role(attempt, step_id).ok_or(LocalRunDirectoryError::StateConflict)?;
        if let Some(existing) = attempt
            .progress
            .invocations
            .iter()
            .find(|invocation| invocation.invocation_id == invocation_id)
        {
            if existing.step_id != *step_id
                || existing.node_role != node_role
                || existing.role != role
                || existing.target_execution != target_execution
                || existing.recovery_round != recovery_round
            {
                return Err(LocalRunDirectoryError::StateConflict);
            }
            continue;
        }
        ensure_invocation_capacity(attempt)?;
        attempt.progress.invocations.push(DurableInvocationV1 {
            invocation_id,
            step_id: step_id.clone(),
            node_role,
            role,
            target_execution,
            recovery_round,
            state: DurableInvocationStateV1::Active,
            started_at: now.to_owned(),
            finished_at: None,
            usage: super::publication::RecoveryInvocationUsageV1::default(),
            diagnostics: Vec::new(),
            diagnostic_reference: None,
        });
    }

    for index in 0..attempt.progress.invocations.len() {
        let should_settle = {
            let invocation = &attempt.progress.invocations[index];
            invocation.state == DurableInvocationStateV1::Active
                && !durable_invocation_is_active(invocation, &commit.state.steps)
        };
        if !should_settle {
            continue;
        }
        let (step_id, invocation_id, role) = {
            let invocation = &attempt.progress.invocations[index];
            (
                invocation.step_id.clone(),
                invocation.invocation_id,
                invocation.role,
            )
        };
        let action = super::runtime::ActionId {
            transition_sequence: super::runtime::TransitionSequence(invocation_id),
        };
        let diagnostic = diagnostics.get_invocation(&step_id, action);
        let agent = accounting.native_session(action);
        let is_agent = commit.state.invocation_is_agent(
            &step_id,
            role == super::publication::RecoveryInvocationRoleV1::RecoveryHandler,
        );
        let retained = diagnostic
            .as_ref()
            .map(|diagnostic| {
                store.retain_invocation_diagnostics(
                    attempt.attempt_number,
                    invocation_id,
                    diagnostic,
                    is_agent,
                )
            })
            .transpose()?;
        let usage = accounting.usage(action).unwrap_or_default();
        let diagnostic_reference = agent.map(|session| {
            format!(
                "diagnostics/{}/{}",
                match session.profile {
                    AgentCompatibilityProfile::PiJsonV1 => "pi-json-v1",
                    AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => {
                        "claude-code-stream-json-v1"
                    }
                    AgentCompatibilityProfile::CodexAppServerV1 => "codex-app-server-v1",
                },
                session.diagnostic_identity
            )
        });
        // An interrupted step was stopped as a cancellation stops it (REQ-WFEXEC-69).
        let cancelled = commit.state.steps.get(&step_id).is_some_and(|runtime| {
            matches!(runtime.state, StepState::Cancelled { .. })
                || matches!(&runtime.state, StepState::Failed { detail } if detail.is_interrupted())
        });
        let invocation = &mut attempt.progress.invocations[index];
        invocation.state = if cancelled {
            DurableInvocationStateV1::Cancelled
        } else {
            DurableInvocationStateV1::Settled
        };
        invocation.finished_at = Some(now.to_owned());
        invocation.usage = super::publication::RecoveryInvocationUsageV1 {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
        };
        invocation.diagnostics = retained.unwrap_or_default();
        invocation.diagnostic_reference = diagnostic_reference;
        if role == super::publication::RecoveryInvocationRoleV1::RecoveryHandler
            && !diagnostics.is_recovery_handler(&step_id, action)
        {
            return Err(LocalRunDirectoryError::StateConflict);
        }
    }
    attempt
        .progress
        .invocations
        .sort_by_key(|invocation| invocation.invocation_id);
    recalculate_invocation_accounting(&mut attempt.progress)
}

fn ensure_invocation_capacity(attempt: &LocalAttemptV1) -> Result<(), LocalRunDirectoryError> {
    if u64::try_from(attempt.progress.invocations.len())
        .ok()
        .is_none_or(|count| count >= attempt.progress.accounting.maximum_invocations)
    {
        Err(LocalRunDirectoryError::StateConflict)
    } else {
        Ok(())
    }
}

fn durable_invocation_is_active<Cause, Output>(
    invocation: &DurableInvocationV1,
    runtime_steps: &BTreeMap<String, super::runtime::StepRuntimeState<Cause, Output>>,
) -> bool {
    let Some(active) = runtime_steps
        .get(&invocation.step_id)
        .and_then(|runtime| runtime.active_invocation)
    else {
        return false;
    };
    match (invocation.role, active) {
        (
            super::publication::RecoveryInvocationRoleV1::Target,
            ActiveStepInvocation::Target { execution_number },
        ) => invocation.target_execution == Some(execution_number.get()),
        (
            super::publication::RecoveryInvocationRoleV1::RecoveryHandler,
            ActiveStepInvocation::RecoveryHandler { round },
        ) => invocation.recovery_round == Some(round.get()),
        _ => false,
    }
}

fn recalculate_invocation_accounting(
    progress: &mut AttemptProgressV1,
) -> Result<(), LocalRunDirectoryError> {
    let mut settled = 0_u64;
    let mut input_tokens = 0_u64;
    let mut output_tokens = 0_u64;
    let mut retained_diagnostic_bytes = 0_u64;
    let mut discarded_diagnostic_bytes = 0_u64;
    for invocation in &progress.invocations {
        if invocation.state != DurableInvocationStateV1::Active {
            settled = settled
                .checked_add(1)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
        }
        input_tokens = input_tokens
            .checked_add(invocation.usage.input_tokens)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        output_tokens = output_tokens
            .checked_add(invocation.usage.output_tokens)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        for diagnostic in &invocation.diagnostics {
            retained_diagnostic_bytes = retained_diagnostic_bytes
                .checked_add(diagnostic.retained_bytes)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            discarded_diagnostic_bytes = discarded_diagnostic_bytes
                .checked_add(diagnostic.discarded_bytes)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
        }
    }
    let observed_invocations = u64::try_from(progress.invocations.len())
        .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if observed_invocations > progress.accounting.maximum_invocations {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    progress.accounting.observed_invocations = observed_invocations;
    progress.accounting.settled_invocations = settled;
    progress.accounting.input_tokens = input_tokens;
    progress.accounting.output_tokens = output_tokens;
    progress.accounting.retained_diagnostic_bytes = retained_diagnostic_bytes;
    progress.accounting.discarded_diagnostic_bytes = discarded_diagnostic_bytes;
    Ok(())
}

impl StateStore {
    fn retain_invocation_diagnostics(
        &self,
        attempt_number: u64,
        invocation_id: u64,
        diagnostic: &StepDiagnostic,
        agent: bool,
    ) -> Result<Vec<DurableInvocationDiagnosticV1>, LocalRunDirectoryError> {
        let attempts = open_directory_at(&self.root, ATTEMPTS_DIRECTORY)?;
        let attempt_name =
            attempt_directory_name(attempt_number).ok_or(LocalRunDirectoryError::StateInvalid)?;
        let attempt = open_directory_at(&attempts, &attempt_name)?;
        let invocations = create_or_open_directory(&attempt, INVOCATIONS_DIRECTORY)?;
        let invocation_name = format!("{invocation_id:020}");
        let invocation = create_or_open_directory(&invocations, &invocation_name)?;
        let (stdout_kind, stderr_kind) = if agent {
            (
                super::publication::RecoveryDiagnosticKindV1::AgentHarnessStdout,
                super::publication::RecoveryDiagnosticKindV1::AgentHarnessStderr,
            )
        } else {
            (
                super::publication::RecoveryDiagnosticKindV1::CommandStdout,
                super::publication::RecoveryDiagnosticKindV1::CommandStderr,
            )
        };
        let mut retained = Vec::with_capacity(2);
        for (name, kind, stream) in [
            ("stdout.bin", stdout_kind, diagnostic.standard_output()),
            ("stderr.bin", stderr_kind, diagnostic.standard_error()),
        ] {
            write_or_verify_immutable_file(&invocation, name, stream.bytes())?;
            let discarded_bytes = stream
                .truncation()
                .map_or(0, |truncation| truncation.discarded_bytes());
            retained.push(DurableInvocationDiagnosticV1 {
                kind,
                reference: format!(
                    "{ATTEMPTS_DIRECTORY}/{attempt_name}/{INVOCATIONS_DIRECTORY}/{invocation_name}/{name}"
                ),
                retained_bytes: u64::try_from(stream.bytes().len())
                    .map_err(|_| LocalRunDirectoryError::StateInvalid)?,
                discarded_bytes,
                truncated: discarded_bytes != 0,
                fully_drained: stream.fully_drained(),
            });
        }
        sync_directory(&invocation)?;
        sync_directory(&invocations)?;
        sync_directory(&attempt)?;
        Ok(retained)
    }
}

pub(super) fn create_or_open_directory(
    parent: &OwnedFd,
    name: &str,
) -> Result<OwnedFd, LocalRunDirectoryError> {
    match mkdirat(parent, name, Mode::RWXU) {
        Ok(()) | Err(Errno::EXIST) => open_directory_at(parent, name),
        Err(_) => Err(LocalRunDirectoryError::StagingUnavailable),
    }
}

fn write_or_verify_immutable_file(
    directory: &OwnedFd,
    name: &str,
    bytes: &[u8],
) -> Result<(), LocalRunDirectoryError> {
    match statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
        Err(Errno::NOENT) => write_new_immutable_file(directory, name, bytes),
        Ok(metadata) if FileType::from_raw_mode(metadata.st_mode) == FileType::RegularFile => {
            let retained = read_regular_file(directory, name)?;
            if retained == bytes {
                Ok(())
            } else {
                Err(LocalRunDirectoryError::StateConflict)
            }
        }
        Ok(_) | Err(_) => Err(LocalRunDirectoryError::StateConflict),
    }
}

fn update_step_progress<Deadline>(
    attempt: &mut LocalAttemptV1,
    runtime: &super::runtime::RuntimeState<StepFailureCause, super::value::CapturedValue, Deadline>,
    finalizers: &[AttemptStepV1],
    retained_outputs: &RetainedOutputSets,
    now: &str,
) -> Result<(), LocalRunDirectoryError>
where
    Deadline: DurableDeadline,
{
    if attempt.progress.steps.len() + finalizers.len() != runtime.steps.len() {
        return Err(LocalRunDirectoryError::StateConflict);
    }
    if let (Some(retained), Some(observed)) = (attempt.force_abort, runtime.force_abort)
        && retained != observed
    {
        return Err(LocalRunDirectoryError::StateConflict);
    }
    attempt.force_abort = runtime.force_abort;
    update_progress_nodes(
        &mut attempt.progress.steps,
        &runtime.steps,
        retained_outputs,
        now,
    )?;

    if let Some(summary) = &runtime.finalization_summary {
        if finalizers.is_empty() {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let mut complete = durable_finalization_summary(summary)?;
        let finalizer_count = complete.finalizers.len();
        let mut retained = complete
            .finalizers
            .into_iter()
            .map(|finalizer| (finalizer.id.clone(), finalizer))
            .collect::<BTreeMap<_, _>>();
        if retained.len() != finalizer_count || retained.len() != finalizers.len() {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        complete.finalizers = finalizers
            .iter()
            .map(|expected| {
                let mut finalizer = retained
                    .remove(&expected.id)
                    .ok_or(LocalRunDirectoryError::StateConflict)?;
                if finalizer.role != expected.role
                    || finalizer.failure_policy != expected.failure_policy
                {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
                finalizer.outputs = retained_outputs
                    .get(&(AttemptNodeRoleV1::Finalizer, finalizer.id.clone()))
                    .cloned()
                    .or_else(|| (finalizer.state != AttemptStepStateV1::Succeeded).then(Vec::new));
                if finalizer.outputs.is_none() {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
                Ok(finalizer)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !retained.is_empty() {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        complete.issues = durable_finalization_issues(&complete.finalizers);
        attempt.finalization = Some(AttemptFinalizationV1::Complete(complete));
        return Ok(());
    }

    let WorkflowState::Finalizing { trigger, gate, .. } = &runtime.workflow else {
        if attempt.finalization.is_some()
            || (!finalizers.is_empty()
                && matches!(
                    runtime.workflow,
                    WorkflowState::Succeeded
                        | WorkflowState::Failed { .. }
                        | WorkflowState::Cancelled { .. }
                ))
        {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        return Ok(());
    };

    if finalizers.is_empty() {
        return Err(LocalRunDirectoryError::StateConflict);
    }
    if attempt.finalization.is_none() {
        attempt.finalization = Some(AttemptFinalizationV1::Progress(
            AttemptFinalizationProgressV1 {
                complete: false,
                trigger: finalization_trigger(*trigger),
                finalizers: finalizers.to_vec(),
                cancellation: None,
                force_abort: false,
            },
        ));
    }
    let Some(AttemptFinalizationV1::Progress(progress)) = &mut attempt.finalization else {
        return Err(LocalRunDirectoryError::StateConflict);
    };
    progress.trigger = finalization_trigger(*trigger);
    match gate {
        super::runtime::FinalizationGate::Open => {
            progress.cancellation = None;
            progress.force_abort = false;
        }
        super::runtime::FinalizationGate::Cancelling {
            reason,
            deadline,
            force_abort,
        } => {
            progress.cancellation = Some(DurableFinalizationCancellationV1 {
                reason: cancellation_reason(*reason),
                force_stop_deadline: deadline
                    .as_ref()
                    .map(|deadline| timestamp(deadline.deadline_utc()))
                    .transpose()?,
            });
            progress.force_abort = *force_abort;
        }
    }
    update_progress_nodes(
        &mut progress.finalizers,
        &runtime.steps,
        retained_outputs,
        now,
    )
}

/// Projects the runtime state onto the durable nodes at a commit made at `now`. A node
/// that becomes active records `now` as its start once and keeps it (REQ-LOCALRUN-201).
fn update_progress_nodes<Cause, Output>(
    nodes: &mut [AttemptStepV1],
    runtime_steps: &BTreeMap<String, super::runtime::StepRuntimeState<Cause, Output>>,
    retained_outputs: &RetainedOutputSets,
    now: &str,
) -> Result<(), LocalRunDirectoryError> {
    for node in nodes {
        let runtime = runtime_steps
            .get(&node.id)
            .ok_or(LocalRunDirectoryError::StateConflict)?;
        let next_state = attempt_step_state(&runtime.state);
        let retained = retained_outputs.get(&(node.role, node.id.clone()));
        node.outputs = match &runtime.state {
            StepState::Inherited { disposition, .. } => {
                let durable = node
                    .outputs
                    .as_deref()
                    .ok_or(LocalRunDirectoryError::StateConflict)?;
                let loaded = retained.ok_or(LocalRunDirectoryError::StateConflict)?;
                if node.state != AttemptStepStateV1::Inherited
                    || (*disposition == InheritedDisposition::Skipped
                        && (!durable.is_empty() || !loaded.is_empty()))
                    || loaded.iter().any(|output| !durable.contains(output))
                {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
                Some(durable.to_vec())
            }
            _ => retained
                .cloned()
                .or_else(|| (next_state != AttemptStepStateV1::Succeeded).then(Vec::new)),
        };
        if next_state.is_in_flight() {
            node.started_at.get_or_insert_with(|| now.to_owned());
        }
        node.state = next_state;
        if node.outputs.is_none() {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        node.detail = attempt_step_detail(&runtime.state);
        node.diagnostic = attempt_step_diagnostic(&runtime.state)?;
    }
    Ok(())
}

fn update_recovery_progress(
    nodes: &mut [AttemptStepV1],
    invocations: &[DurableInvocationV1],
    runtime_steps: &BTreeMap<
        String,
        super::runtime::StepRuntimeState<StepFailureCause, super::value::CapturedValue>,
    >,
) -> Result<(), LocalRunDirectoryError> {
    for node in nodes {
        let runtime = runtime_steps
            .get(&node.id)
            .ok_or(LocalRunDirectoryError::StateConflict)?;
        let Some(recovery) = runtime
            .recovery
            .as_ref()
            .filter(|recovery| !recovery.rounds.is_empty())
        else {
            node.recovery = None;
            continue;
        };
        let termination = if recovery.terminal_disposition.is_some() {
            super::publication::step_recovery_summary_v1(Some(recovery))
                .map_err(|_| LocalRunDirectoryError::SerializationUnavailable)?
                .map(|summary| summary.termination)
        } else {
            None
        };
        let rounds = super::publication::recovery_round_summaries_v1(recovery, false)
            .map_err(|_| LocalRunDirectoryError::SerializationUnavailable)?
            .into_iter()
            .map(|round| DurableRecoveryRoundV1 {
                number: round.number,
                failed_execution: round.failed_execution,
                handler: round.handler,
            })
            .collect();
        let active = runtime.active_invocation.and_then(|active| {
            let (role, target_execution, recovery_round) = match active {
                ActiveStepInvocation::Target { execution_number } => (
                    super::publication::RecoveryInvocationRoleV1::Target,
                    Some(execution_number.get()),
                    None,
                ),
                ActiveStepInvocation::RecoveryHandler { round } => (
                    super::publication::RecoveryInvocationRoleV1::RecoveryHandler,
                    None,
                    Some(round.get()),
                ),
            };
            let invocation = invocations.iter().find(|invocation| {
                invocation.step_id == node.id
                    && invocation.role == role
                    && invocation.target_execution == target_execution
                    && invocation.recovery_round == recovery_round
                    && invocation.state == DurableInvocationStateV1::Active
            })?;
            let (handler_state, decision) = match (&runtime.state, recovery.rounds.last()) {
                (StepState::Recovering { handler, .. }, _) => (
                    Some(match handler {
                        super::runtime::RecoveryHandlerActivity::Starting => {
                            DurableRecoveryHandlerStateV1::Starting
                        }
                        super::runtime::RecoveryHandlerActivity::Running => {
                            DurableRecoveryHandlerStateV1::Running
                        }
                    }),
                    None,
                ),
                (_, Some(round)) => (
                    None,
                    round
                        .handler
                        .as_ref()
                        .and_then(|handler| match handler.outcome {
                            super::runtime::RecoveryHandlerOutcome::Recheck { .. } => {
                                Some(super::publication::RecoveryHandlerOutcomeV1::Recheck)
                            }
                            super::runtime::RecoveryHandlerOutcome::GaveUp { .. } => {
                                Some(super::publication::RecoveryHandlerOutcomeV1::GaveUp)
                            }
                            super::runtime::RecoveryHandlerOutcome::Failed { .. } => {
                                Some(super::publication::RecoveryHandlerOutcomeV1::Failed)
                            }
                            super::runtime::RecoveryHandlerOutcome::Cancelled => {
                                Some(super::publication::RecoveryHandlerOutcomeV1::Cancelled)
                            }
                            super::runtime::RecoveryHandlerOutcome::Starting
                            | super::runtime::RecoveryHandlerOutcome::Running => None,
                        }),
                ),
                (_, None) => (None, None),
            };
            Some(DurableRecoveryActiveV1 {
                role,
                target_execution,
                recovery_round,
                invocation_id: invocation.invocation_id,
                handler_state,
                decision,
            })
        });
        node.recovery = Some(DurableStepRecoveryV1 {
            schema_version: 1,
            configured_retries: recovery.configured_rounds,
            handler_kind: recovery.handler_kind.map(|kind| match kind {
                super::runtime::RecoveryHandlerKind::Command => DurableRecoveryHandlerKindV1::Cmd,
                super::runtime::RecoveryHandlerKind::Agent => DurableRecoveryHandlerKindV1::Agent,
            }),
            rounds,
            active,
            termination,
        });
    }
    Ok(())
}

fn attempt_step_state<Output>(state: &StepState<Output>) -> AttemptStepStateV1 {
    match state {
        StepState::Pending => AttemptStepStateV1::Pending,
        StepState::Starting => AttemptStepStateV1::Starting,
        StepState::Running => AttemptStepStateV1::Running,
        StepState::CapturingOutputs => AttemptStepStateV1::CapturingOutputs,
        StepState::Recovering { .. } => AttemptStepStateV1::Running,
        StepState::Cancelling { .. } => AttemptStepStateV1::Cancelling,
        StepState::Succeeded { .. } => AttemptStepStateV1::Succeeded,
        StepState::Inherited { .. } => AttemptStepStateV1::Inherited,
        StepState::Failed { .. } => AttemptStepStateV1::Failed,
        StepState::Blocked { .. } => AttemptStepStateV1::Blocked,
        StepState::Skipped { .. } => AttemptStepStateV1::Skipped,
        StepState::NotRun { .. } => AttemptStepStateV1::NotRun,
        StepState::Cancelled { .. } => AttemptStepStateV1::Cancelled,
    }
}

// Durable state copies detail while the live view model copies it into observation
// facts; keeping both projections explicit avoids coupling persistence to presentation.
fn attempt_step_detail<Output>(state: &StepState<Output>) -> Option<NodeDetail> {
    match state {
        StepState::Inherited { detail, .. } => Some(NodeDetail::Inherited(detail.clone())),
        StepState::Failed { detail } => Some(NodeDetail::Failed(detail.clone())),
        StepState::Blocked { detail } => Some(NodeDetail::Blocked(detail.clone())),
        StepState::Skipped { detail } => Some(NodeDetail::Skipped(detail.clone())),
        StepState::NotRun { detail } => Some(NodeDetail::NotRun(*detail)),
        StepState::Cancelling { detail } | StepState::Cancelled { detail } => {
            Some(NodeDetail::Cancellation(*detail))
        }
        StepState::Pending
        | StepState::Starting
        | StepState::Running
        | StepState::CapturingOutputs
        | StepState::Recovering { .. }
        | StepState::Succeeded { .. } => None,
    }
}

/// A failed node's classified facts, projected while its failure detail is still in memory
/// (REQ-LOCALRUN-198); an inherited node of a later attempt carries none.
fn attempt_step_diagnostic<Output>(
    state: &StepState<Output>,
) -> Result<Option<FailureDiagnosticV1>, LocalRunDirectoryError> {
    match state {
        StepState::Failed { detail } => FailureDiagnosticV1::project(detail)
            .map_err(|_| LocalRunDirectoryError::SerializationUnavailable),
        _ => Ok(None),
    }
}

fn durable_finalization_summary<Deadline>(
    summary: &FinalizationSummary<Deadline>,
) -> Result<AttemptFinalizationCompleteV1, LocalRunDirectoryError>
where
    Deadline: DurableDeadline,
{
    let finalizers = summary
        .finalizers
        .iter()
        .map(|finalizer| {
            let (state, detail) = match &finalizer.disposition {
                StepState::Succeeded { .. } => (AttemptStepStateV1::Succeeded, None),
                StepState::Inherited { .. } => {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
                StepState::Failed { detail } => (
                    AttemptStepStateV1::Failed,
                    Some(NodeDetail::Failed(detail.clone())),
                ),
                StepState::Blocked { detail } => (
                    AttemptStepStateV1::Blocked,
                    Some(NodeDetail::Blocked(detail.clone())),
                ),
                StepState::Skipped { detail } => (
                    AttemptStepStateV1::Skipped,
                    Some(NodeDetail::Skipped(detail.clone())),
                ),
                StepState::NotRun { detail }
                    if detail.code == NonExecutionCode::FinalizerTriggerNotSelected =>
                {
                    (
                        AttemptStepStateV1::NotRun,
                        Some(NodeDetail::NotRun(*detail)),
                    )
                }
                StepState::Cancelled { detail } => (
                    AttemptStepStateV1::Cancelled,
                    Some(NodeDetail::Cancellation(*detail)),
                ),
                StepState::Pending
                | StepState::Starting
                | StepState::Running
                | StepState::CapturingOutputs
                | StepState::Recovering { .. }
                | StepState::Cancelling { .. }
                | StepState::NotRun { .. } => {
                    return Err(LocalRunDirectoryError::StateConflict);
                }
            };
            Ok(DurableFinalizerV1 {
                id: finalizer.finalizer.clone(),
                role: AttemptNodeRoleV1::Finalizer,
                failure_policy: finalizer.failure_policy,
                state,
                outputs: Some(Vec::new()),
                detail,
                diagnostic: attempt_step_diagnostic(&finalizer.disposition)?,
            })
        })
        .collect::<Result<Vec<_>, LocalRunDirectoryError>>()?;
    let issues = durable_finalization_issues(&finalizers);
    let cancellation = summary
        .cancellation
        .as_ref()
        .map(|cancellation| {
            Ok(DurableFinalizationCancellationV1 {
                reason: cancellation_reason(cancellation.reason),
                force_stop_deadline: cancellation
                    .deadline
                    .as_ref()
                    .map(|deadline| timestamp(deadline.deadline_utc()))
                    .transpose()?,
            })
        })
        .transpose()?;
    Ok(AttemptFinalizationCompleteV1 {
        complete: true,
        trigger: finalization_trigger(summary.trigger),
        finalizers,
        issues,
        cancellation,
        force_abort: summary.force_abort,
    })
}

fn durable_finalization_issues(
    finalizers: &[DurableFinalizerV1],
) -> Vec<DurableFinalizationIssueV1> {
    finalizers
        .iter()
        .filter(|finalizer| {
            matches!(
                finalizer.state,
                AttemptStepStateV1::Failed | AttemptStepStateV1::Blocked
            )
        })
        .map(|finalizer| DurableFinalizationIssueV1 {
            finalizer_id: finalizer.id.clone(),
            impact: finalizer.failure_policy,
        })
        .collect()
}

fn outstanding_actions<Cause, Output>(
    attempt: &LocalAttemptV1,
    runtime_steps: &BTreeMap<String, super::runtime::StepRuntimeState<Cause, Output>>,
) -> Result<Vec<OutstandingActionV1>, LocalRunDirectoryError> {
    let ordered_nodes = attempt.progress.steps.iter().chain(
        attempt
            .finalization
            .iter()
            .filter_map(|finalization| match finalization {
                AttemptFinalizationV1::Progress(progress) => Some(progress.finalizers.as_slice()),
                AttemptFinalizationV1::Complete(_) => None,
            })
            .flatten(),
    );
    let mut actions = ordered_nodes
        .filter_map(|node| {
            let runtime = match runtime_steps.get(&node.id) {
                Some(runtime) => runtime,
                None => return Some(Err(LocalRunDirectoryError::StateConflict)),
            };
            let action = runtime.current_action?;
            let kind = match runtime.state {
                StepState::Starting | StepState::Running => OutstandingActionKindV1::StartStep,
                StepState::Recovering { .. } => OutstandingActionKindV1::StartRecoveryHandler,
                StepState::CapturingOutputs => OutstandingActionKindV1::CaptureOutputs,
                StepState::Cancelling { .. } => OutstandingActionKindV1::CancelStep,
                StepState::Pending
                | StepState::Inherited { .. }
                | StepState::Succeeded { .. }
                | StepState::Failed { .. }
                | StepState::Blocked { .. }
                | StepState::Skipped { .. }
                | StepState::NotRun { .. }
                | StepState::Cancelled { .. } => {
                    return Some(Err(LocalRunDirectoryError::StateConflict));
                }
            };
            let (target_execution, recovery_round) = match runtime.active_invocation {
                Some(ActiveStepInvocation::Target { execution_number }) => {
                    (Some(execution_number.get()), None)
                }
                Some(ActiveStepInvocation::RecoveryHandler { round }) => (None, Some(round.get())),
                None => (None, None),
            };
            Some(Ok(OutstandingActionV1 {
                action_id: action.transition_sequence.get(),
                kind,
                step_id: Some(node.id.clone()),
                node_role: Some(node.role),
                target_execution,
                recovery_round,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    actions.sort_unstable_by_key(|action| action.action_id);
    Ok(actions)
}

fn attempt_node_role(attempt: &LocalAttemptV1, id: &str) -> Option<AttemptNodeRoleV1> {
    if attempt.progress.steps.iter().any(|node| node.id == id) {
        return Some(AttemptNodeRoleV1::Step);
    }
    attempt
        .finalization
        .as_ref()
        .and_then(|finalization| match finalization {
            AttemptFinalizationV1::Progress(progress) => progress
                .finalizers
                .iter()
                .find(|node| node.id == id)
                .map(|node| node.role),
            AttemptFinalizationV1::Complete(complete) => complete
                .finalizers
                .iter()
                .find(|node| node.id == id)
                .map(|node| node.role),
        })
}

fn settle_interrupted_attempt(
    attempt: &mut LocalAttemptV1,
    cause: InterruptionCauseV1,
    execution_may_have_started: bool,
    settlement_snapshot: Option<WorkspaceSnapshotV1>,
) -> Result<(), LocalRunDirectoryError> {
    let cancellation_requested = attempt.cancellation.is_some()
        || attempt.force_abort.is_some()
        || attempt
            .finalization
            .as_ref()
            .is_some_and(|finalization| match finalization {
                AttemptFinalizationV1::Progress(progress) => progress.cancellation.is_some(),
                AttemptFinalizationV1::Complete(complete) => complete.cancellation.is_some(),
            });
    attempt.state = AttemptStateV1::Interrupted;
    attempt.settled_at = Some(timestamp(um_support::utc_now())?);
    attempt.settlement_snapshot = settlement_snapshot;
    attempt.interruption = Some(AttemptInterruptionV1 {
        cause,
        execution_may_have_started,
        cancellation_requested,
    });
    attempt.result = AttemptResultV1::NotPublished {
        reason: ResultAbsentReasonV1::Interrupted,
    };
    attempt.progress.outstanding_actions.clear();
    Ok(())
}

fn append_diagnostic(
    state: &mut LocalRunStateV1,
    attempt_number: u64,
    code: DiagnosticCodeV1,
) -> Result<(), LocalRunDirectoryError> {
    append_diagnostic_of(state, attempt_number, code, None)
}

/// Appends a `code` diagnostic for `attempt_number`, carrying the class and OS error number
/// of the commit `failure` a `state_persistence_failure` names (REQ-LOCALRUN-205).
fn append_diagnostic_of(
    state: &mut LocalRunStateV1,
    attempt_number: u64,
    code: DiagnosticCodeV1,
    failure: Option<CommitFailure>,
) -> Result<(), LocalRunDirectoryError> {
    let sequence = state
        .diagnostics
        .last()
        .map_or(Some(1), |diagnostic| diagnostic.sequence.checked_add(1))
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    state.diagnostics.push(LocalDiagnosticV1 {
        sequence,
        attempt_number,
        code,
        step_id: None,
        action_id: None,
        guard_id: None,
        cause: failure.map(|failure| failure.class),
        os_error: failure
            .and_then(|failure| failure.os_error)
            .filter(|number| *number > 0),
    });
    if state.diagnostics.len() > MAXIMUM_DIAGNOSTICS {
        state.diagnostics.remove(0);
    }
    Ok(())
}

fn current_attempt(state: &LocalRunStateV1) -> Result<&LocalAttemptV1, LocalRunDirectoryError> {
    let attempt = state
        .attempts
        .last()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    if attempt.attempt_number != state.current_attempt_number {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(attempt)
}

fn current_attempt_mut(
    state: &mut LocalRunStateV1,
) -> Result<&mut LocalAttemptV1, LocalRunDirectoryError> {
    let attempt = state
        .attempts
        .last_mut()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    if attempt.attempt_number != state.current_attempt_number {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(attempt)
}

pub(super) fn attempt_result_relative_path(attempt_number: u64) -> String {
    format!(
        "{ATTEMPTS_DIRECTORY}/{}/result",
        attempt_directory_name(attempt_number).unwrap_or_else(|| attempt_number.to_string())
    )
}

pub(crate) fn attempt_directory_name(attempt_number: u64) -> Option<String> {
    if attempt_number == 0 {
        None
    } else if attempt_number < 1_000_000 {
        Some(format!("{attempt_number:06}"))
    } else {
        Some(attempt_number.to_string())
    }
}

fn retained_file_name(ordinal: u64) -> Result<String, LocalRunDirectoryError> {
    if ordinal == 0 {
        Err(LocalRunDirectoryError::SerializationUnavailable)
    } else if ordinal < 10_000 {
        Ok(format!("{ordinal:04}"))
    } else {
        Ok(ordinal.to_string())
    }
}

fn replace_state(
    root: &OwnedFd,
    private: &OwnedFd,
    current: &mut LocalRunStateV1,
    next: LocalRunStateV1,
    observer: &mut impl StateCommitObserver,
) -> Result<(), LocalRunDirectoryError> {
    let bytes = encode_json(&next)?;
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|size| size > MAXIMUM_DURABLE_JSON_BYTES)
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let (temporary_name, mut temporary) = create_state_temporary(private)?;
    let _cleanup = StateTemporary {
        parent: private,
        name: temporary_name.clone(),
    };
    observer
        .write_temporary(&mut temporary, &bytes)
        .map_err(|source| file_error(private, &temporary_name, "write temporary state", source))?;
    temporary
        .flush()
        .and_then(|()| temporary.sync_all())
        .map_err(|source| file_error(private, &temporary_name, "sync temporary state", source))?;
    drop(temporary);
    observer.temporary_complete()?;
    observer
        .exchange(private, &temporary_name, root)
        .map_err(|source| file_error(root, STATE_FILE, "exchange state", source))?;
    // An error after exchange cannot undo the commit. Keep memory aligned with
    // the visible snapshot even when directory sync or the observer fails.
    *current = next;
    sync_directory(root)?;
    observer.replaced()?;
    Ok(())
}

trait StateCommitObserver {
    fn exchange(
        &mut self,
        private: &OwnedFd,
        temporary_name: &str,
        root: &OwnedFd,
    ) -> rustix::io::Result<()> {
        renameat_with(
            private,
            temporary_name,
            root,
            STATE_FILE,
            RenameFlags::EXCHANGE,
        )
    }

    fn write_temporary(&mut self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }

    fn temporary_complete(&mut self) -> Result<(), LocalRunDirectoryError> {
        Ok(())
    }

    fn replaced(&mut self) -> Result<(), LocalRunDirectoryError> {
        Ok(())
    }
}

struct NoopStateCommitObserver;

impl StateCommitObserver for NoopStateCommitObserver {}

struct StateTemporary<'a> {
    parent: &'a OwnedFd,
    name: String,
}

impl Drop for StateTemporary<'_> {
    fn drop(&mut self) {
        let _ = unlinkat(self.parent, &self.name, AtFlags::empty());
    }
}

fn create_state_temporary(parent: &OwnedFd) -> Result<(String, File), LocalRunDirectoryError> {
    for _ in 0..STAGING_ATTEMPTS {
        let name = format!(".state-{}", generate_uuid()?);
        match openat(
            parent,
            &name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(file) => return Ok((name, File::from(file))),
            Err(Errno::EXIST) => {}
            Err(source) => return Err(file_error(parent, &name, "create temporary state", source)),
        }
    }
    Err(LocalRunDirectoryError::StateWriteUnavailable)
}

fn verify_initial_staging(
    root: &OwnedFd,
    expected_run: &LocalRunV1,
    expected_state: &LocalRunStateV1,
    expected_record: Option<&WorkflowRunRecord>,
    expected_controls: bool,
    expected_wait: bool,
) -> Result<(), LocalRunDirectoryError> {
    let mut expected_entries = run_root_entries();
    if expected_record.is_some() {
        expected_entries.insert(RUN_RECORD_FILE.as_bytes().to_vec());
    }
    if expected_controls {
        expected_entries.insert(CONTROLS_DIRECTORY.as_bytes().to_vec());
    }
    if expected_wait {
        expected_entries.insert(WAIT_RECORD_FILE.as_bytes().to_vec());
    }
    if directory_entries(root)? != expected_entries
        || read_run(root)? != *expected_run
        || read_state(root)? != *expected_state
        || expected_record
            .map(|record| read_run_record(root).map(|retained| retained != *record))
            .transpose()?
            .unwrap_or(false)
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(())
}

/// The run's `run.json` (REQ-LOCALRUN-175), refused at its path when it does not parse
/// or validate.
pub(super) fn read_run_record(root: &OwnedFd) -> Result<WorkflowRunRecord, LocalRunDirectoryError> {
    let bytes = read_regular_file(root, RUN_RECORD_FILE)?;
    serde_json::from_slice::<WorkflowRunRecord>(&bytes)
        .ok()
        .filter(WorkflowRunRecord::is_valid)
        .ok_or_else(|| LocalRunDirectoryError::StateFile {
            path: file_locator(root, RUN_RECORD_FILE),
            operation: "validate",
            source: Box::new(LocalRunDirectoryError::StateInvalid),
        })
}

/// The run's `run.json` as `read_run_record` reads it, or `None` for a run without one.
fn read_optional_run_record(
    root: &OwnedFd,
) -> Result<Option<WorkflowRunRecord>, LocalRunDirectoryError> {
    read_if_present(root, RUN_RECORD_FILE, read_run_record)
}

/// The run's `state.json`, or `None` for a run root without one.
fn read_optional_state(root: &OwnedFd) -> Result<Option<LocalRunStateV1>, LocalRunDirectoryError> {
    read_if_present(root, STATE_FILE, read_state)
}

/// Sets the run record's `state`, `attempts` and `outcome` through `.private/run.json.tmp`
/// and a rename over `run.json`, so an interrupted rewrite leaves no entry in the run root,
/// and clears its `position` once the run is `exited` (REQ-WFDEF-4). The caller holds the
/// run lock. A run without `run.json` is left as it is.
fn update_run_record(
    root: &OwnedFd,
    state: RunState,
    attempts: u64,
    outcome: Option<&str>,
) -> Result<(), LocalRunDirectoryError> {
    rewrite_run_record(root, |record| {
        record.state = state;
        record.attempts = attempts;
        record.outcome = outcome.map(str::to_owned);
        if state == RunState::Exited {
            record.position = Vec::new();
        }
        Ok(())
    })
}

/// Applies `change` to the run record and writes it back as `update_run_record` describes.
fn rewrite_run_record(
    root: &OwnedFd,
    change: impl FnOnce(&mut WorkflowRunRecord) -> Result<(), LocalRunDirectoryError>,
) -> Result<(), LocalRunDirectoryError> {
    let Some(mut record) = read_optional_run_record(root)? else {
        return Ok(());
    };
    change(&mut record)?;
    if !record.is_valid() {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let bytes = encode_json(&record)?;
    let private = open_directory_at(root, PRIVATE_DIRECTORY)?;
    write_through_private(
        &private,
        RUN_RECORD_TEMPORARY_FILE,
        root,
        RUN_RECORD_FILE,
        &bytes,
        Mode::RUSR | Mode::WUSR,
        RenameFlags::empty(),
    )?;
    sync_directory(root)
}

/// Rewrites the run record for the attempt a counted edge starts (REQ-LOCALRUN-175,
/// REQ-WFEXEC-64): `running`, `attempts` the new attempt's number and the edge's count in
/// `edges` incremented, in one write under the run lock. A run without `run.json` is left as
/// it is.
fn count_run_record_edge(
    root: &OwnedFd,
    attempts: u64,
    key: &str,
) -> Result<(), LocalRunDirectoryError> {
    rewrite_run_record(root, |record| {
        record.state = RunState::Running;
        record.attempts = attempts;
        record.outcome = None;
        let count = record.edges.entry(key.to_owned()).or_insert(0);
        *count = count
            .checked_add(1)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        Ok(())
    })
}

/// Writes `bytes` whole as `.private/<temporary>` with `mode` and renames it to `name` in
/// `directory` with `flags`, so an interrupted write leaves no entry there; a stale
/// temporary is replaced and a failed one removed. The caller serializes writers and syncs
/// `directory`.
pub(super) fn write_through_private(
    private: &OwnedFd,
    temporary: &str,
    directory: &OwnedFd,
    name: &str,
    bytes: &[u8],
    mode: Mode,
    flags: RenameFlags,
) -> Result<(), LocalRunDirectoryError> {
    match unlinkat(private, temporary, AtFlags::empty()) {
        Ok(()) | Err(Errno::NOENT) => {}
        Err(source) => {
            return Err(file_error(
                private,
                temporary,
                "remove stale temporary",
                source,
            ));
        }
    }
    let replaced = tree_copy::write_new_file_with_final_mode(private, temporary, bytes, mode)
        .map_err(file_operation_error)
        .and_then(|()| {
            renameat_with(private, temporary, directory, name, flags)
                .map_err(|source| file_error(directory, name, "replace", source))
        });
    if replaced.is_err() {
        let _ = unlinkat(private, temporary, AtFlags::empty());
    }
    replaced
}

/// The run's root `wait.json` (REQ-LOCALRUN-178), or `None` while the run does not wait;
/// a record that does not parse or validate is refused at its path.
fn read_optional_wait_record(root: &OwnedFd) -> Result<Option<WaitRecord>, LocalRunDirectoryError> {
    read_if_present(root, WAIT_RECORD_FILE, |root| {
        let bytes = read_regular_file(root, WAIT_RECORD_FILE)?;
        serde_json::from_slice::<WaitRecord>(&bytes)
            .ok()
            .filter(WaitRecord::is_valid)
            .ok_or_else(|| LocalRunDirectoryError::StateFile {
                path: file_locator(root, WAIT_RECORD_FILE),
                operation: "validate",
                source: Box::new(LocalRunDirectoryError::WaitRecordInvalid),
            })
    })
}

/// The wait copy `attempts/<N>/wait.json` of the attempt `attempt_number` of the run whose
/// directory is `root`, or `None` for an attempt that entered no wait (REQ-LOCALRUN-179).
pub(super) fn read_attempt_wait_record(
    root: &OwnedFd,
    attempt_number: u64,
) -> Result<Option<WaitRecord>, LocalRunDirectoryError> {
    let Some(name) = attempt_directory_name(attempt_number) else {
        return Ok(None);
    };
    let Some(attempts) = open_optional_directory(root, ATTEMPTS_DIRECTORY)? else {
        return Ok(None);
    };
    match open_optional_directory(&attempts, &name)? {
        Some(directory) => read_optional_wait_record(&directory),
        None => Ok(None),
    }
}

/// What `um workflow run resume RUN_DIR` without `--from` finds before it takes the run
/// lock (REQ-CONTINUE-74, REQ-CONTINUE-76).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WaitInspection {
    /// A live process holds the run lock.
    Locked(LocalRetryRejection),
    /// The run waits and no control or observed event resolves the wait.
    Held { wait: WaitRecord },
    /// `by` is the first control, in ordinal order, or the first event the condition source
    /// observed that the wait's rule matches.
    Satisfied { wait: WaitRecord, by: ResolvedBy },
    /// The wait's rule is outside the matcher's grammar (`wait_rule_invalid`).
    RuleInvalid { wait: WaitRecord, error: RuleError },
    /// The condition source could not read the condition an `external` wait's rule names
    /// (`wait_condition_unavailable`, REQ-CONTINUE-74); the wait neither holds nor resolves.
    ConditionUnavailable {
        wait: WaitRecord,
        error: super::wait::ConditionError,
    },
    /// The run reads `running`, no process holds its lock and its latest attempt ended
    /// `continued` on `edge`: the process ended before the next attempt began, so the run
    /// continues from the edge's target, uncounted (REQ-CONTINUE-78).
    Continued {
        edge: super::publication::ContinuationEdgeV1,
    },
    /// The run does not wait: `state` is its record's, or `None` for a run without a
    /// record; a record that reads `waiting` without a root wait record is one too.
    /// `successor` is the record's: set once a restart superseded the run
    /// (REQ-LOCALRUN-196), so the refusal can name it (REQ-CONTINUE-29).
    NoWait {
        state: Option<RunState>,
        successor: Option<String>,
    },
}

/// What resolved a wait (REQ-CONTINUE-74): a control under `controls/` for a rule on source
/// `um`, or an event a condition source observed for a rule on any other source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedBy {
    Control(ControlEvent),
    External(super::wait::ExternalEvent),
}

/// A `WaitInspection` with the run's directory and latest attempt number.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitProbe {
    pub run_directory: PathBuf,
    pub attempt_number: u64,
    pub inspection: WaitInspection,
}

/// The first control under `run_directory`'s `controls/`, in ordinal order, that `wait`'s
/// rule matches for the run `run_id` (REQ-CONTINUE-74), `Ok(None)` when none does and
/// `Ok(None)` without reading the controls for a rule on a source other than `um`
/// (REQ-WFEXEC-65); a rule outside the grammar is `Err(RuleError)`.
fn control_resolution(
    run_directory: &Path,
    run_id: &str,
    wait: &WaitRecord,
) -> Result<Result<Option<ControlEvent>, RuleError>, LocalRunDirectoryError> {
    if wait.rule.source != super::wait::SOURCE {
        return Ok(Ok(None));
    }
    let controls = super::wait::read_controls(run_directory)?;
    Ok(super::wait::evaluate(&wait.rule, run_id, wait, &controls).map(|control| control.cloned()))
}

/// Probes the run at `requested` without taking its lock (REQ-CONTINUE-74): a lock a live
/// process holds is `Locked`; otherwise the run record, the root wait record and the events
/// decide: the controls for a rule on source `um`, and for a rule on any other source the
/// events `conditions` observes now, never the controls (REQ-WFEXEC-65). Nothing is written.
pub fn inspect_wait(
    requested: &Path,
    conditions: &dyn super::wait::ConditionSource,
) -> Result<WaitProbe, LocalRunDirectoryError> {
    let (normalized, root) = open_run_root(requested)?;
    if query_run_lock(&root)?.is_some() {
        let snapshot =
            read_local_run_status(requested).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        return Ok(WaitProbe {
            run_directory: normalized,
            attempt_number: snapshot.current_attempt_number,
            inspection: WaitInspection::Locked(retry_rejection_from_snapshot(
                &snapshot,
                RetryIneligibilityReason::RunLocked,
            )),
        });
    }
    let Some(record) = read_optional_run_record(&root)? else {
        let state = read_state(&root)?;
        return Ok(WaitProbe {
            run_directory: normalized,
            attempt_number: state.current_attempt_number,
            inspection: WaitInspection::NoWait {
                state: None,
                successor: None,
            },
        });
    };
    let probe = |inspection| WaitProbe {
        run_directory: normalized.clone(),
        attempt_number: record.attempts,
        inspection,
    };
    if record.state == RunState::Running
        && let Some(edge) = read_state(&root)?
            .attempts
            .last()
            .filter(|attempt| attempt.state == AttemptStateV1::Continued)
            .and_then(|attempt| attempt.taken_edge.clone())
    {
        return Ok(probe(WaitInspection::Continued {
            edge: super::publication::ContinuationEdgeV1 {
                counted: false,
                ..edge
            },
        }));
    }
    if record.state != RunState::Waiting {
        return Ok(probe(WaitInspection::NoWait {
            state: Some(record.state),
            successor: record.successor.clone(),
        }));
    }
    let Some(wait) = read_optional_wait_record(&root)? else {
        return Ok(probe(WaitInspection::NoWait {
            state: Some(RunState::Waiting),
            successor: record.successor.clone(),
        }));
    };
    let resolved = if wait.rule.source == super::wait::SOURCE {
        control_resolution(&normalized, &record.id, &wait)?
            .map(|control| control.map(ResolvedBy::Control))
    } else {
        let events = match conditions.observe(&wait) {
            Ok(events) => events,
            Err(error) => {
                return Ok(probe(WaitInspection::ConditionUnavailable { wait, error }));
            }
        };
        super::wait::evaluate(&wait.rule, &record.id, &wait, &events)
            .map(|event| event.cloned().map(ResolvedBy::External))
    };
    let inspection = match resolved {
        Ok(None) => WaitInspection::Held { wait },
        Ok(Some(by)) => WaitInspection::Satisfied { wait, by },
        Err(error) => WaitInspection::RuleInvalid { wait, error },
    };
    Ok(probe(inspection))
}

/// The control values an attempt of the run at `requested` starts with (REQ-WFEXEC-63,
/// REQ-CONTINUE-75), read from the wait copies of the attempts before `attempt_number` and
/// the run's controls. An imported attempt's copy (REQ-LOCALRUN-195) is resolved by a control
/// naming any run of the lineage: the run it was recorded under, copied with it, a successor
/// that carried the wait (REQ-LOCALRUN-197) and recorded the control, or this run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AttemptControls {
    /// `wait` and `resume_item` from the attempt immediately before, when it entered a wait
    /// a control resolved, so only the attempt that continues the wait reads its control's
    /// fields (REQ-WFEXEC-63); `answers` from every resolved `answer` wait, the latest per
    /// `then` step.
    pub controls: super::step_runtime::ControlContext,
    /// The step a `control.retry` continues at and its `payload.agentProfiles`, the
    /// agent-profile replacement that step's child resolves with, from the latest earlier
    /// resolved wait however many attempts back (REQ-AGENT-59).
    pub replacement: Option<(String, Value)>,
}

pub fn attempt_controls(
    requested: &Path,
    attempt_number: u64,
) -> Result<AttemptControls, LocalRunDirectoryError> {
    let (normalized, root) = open_run_root(requested)?;
    let mut resolved = AttemptControls::default();
    let Some(record) = read_optional_run_record(&root)? else {
        return Ok(resolved);
    };
    let controls = super::wait::read_controls(&normalized)?;
    let state = read_optional_state(&root)?;
    let imported = |number: u64| {
        state.as_ref().is_some_and(|state| {
            state
                .attempts
                .iter()
                .any(|attempt| attempt.attempt_number == number && attempt.imported.is_some())
        })
    };
    let mut latest = None;
    for number in 1..attempt_number {
        let Some(wait) = read_attempt_wait_record(&root, number)? else {
            continue;
        };
        // The copy of a wait an imported attempt entered was resolved under some run of the
        // lineage: the run the attempt was recorded under, or a successor that carried the
        // wait (REQ-LOCALRUN-197). Every control here was recorded on this run or copied
        // along the lineage (REQ-LOCALRUN-195), so each is read as its own run's; a wait
        // this run itself entered is resolved by a control naming this run alone.
        let control = if imported(number) {
            controls.iter().find(|control| {
                super::wait::evaluate(
                    &wait.rule,
                    &control.run,
                    &wait,
                    std::slice::from_ref(*control),
                )
                .ok()
                .flatten()
                .is_some()
            })
        } else {
            super::wait::evaluate(&wait.rule, &record.id, &wait, &controls)
                .ok()
                .flatten()
        }
        .cloned();
        if let Some(control) = &control
            && wait.kind == super::wait::WaitKind::Answer
            && control.event == super::wait::CONTROL_ANSWER
            && let Some(answers) = control
                .payload
                .as_ref()
                .and_then(|payload| payload.get("answers"))
        {
            resolved
                .controls
                .answers
                .insert(wait.then.clone(), Arc::new(answers.clone()));
        }
        latest = Some((number, wait, control));
    }
    if let Some((number, wait, Some(control))) = latest {
        let target = match (control.event.as_str(), wait.on_hand_back) {
            (super::wait::CONTROL_HAND_BACK, Some(step)) => step,
            _ => wait.then,
        };
        let payload = control.payload.unwrap_or_default();
        let fields = payload
            .iter()
            .filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_owned())))
            .collect();
        if control.event == super::wait::CONTROL_RETRY
            && let Some(profiles) = payload.get("agentProfiles")
        {
            resolved.replacement = Some((target.clone(), profiles.clone()));
        }
        // Only the attempt that continues the wait reads its control's fields (REQ-WFEXEC-63)
        // and resumes its fan-out at the recorded element (REQ-WFEXEC-68); one reaching the
        // step after a later attempt ended without a wait (a backward or limited edge, an
        // abandoned attempt) has no `wait.<field>` and starts at the first element.
        if number + 1 != attempt_number {
            return Ok(resolved);
        }
        if let Some((step, item)) = fan_out_element(&wait.step)
            && step == target
        {
            resolved.controls.resume_item = Some((target.clone(), item));
        }
        resolved.controls.wait = Some((target, fields));
    }
    Ok(resolved)
}

/// What a wait record's `payload.child` names (REQ-CONTINUE-82, INV-ENGINE-27).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WaitChild {
    /// The payload names no child.
    Absent,
    /// The waiting step's own child: the canonical `children/<step>/<NNNNNN>` or, for a
    /// fan-out waiting at element n, `children/<step>/<n>/<NNNNNN>`, relative to the run
    /// directory (REQ-LOCALRUN-177).
    Owned { locator: PathBuf, item: Option<u64> },
    /// Anything else: another step's or element's child, a path that is not canonical, or a
    /// value that is not a path.
    Unowned,
}

/// Reads the child `wait`'s payload names against the waiting step and element the record
/// itself holds. The locator is evidence to check, never a path to follow: nothing is opened,
/// and only the exact canonical form of the waiting step's child is `Owned`.
pub fn wait_child(wait: &WaitRecord) -> WaitChild {
    let Some(locator) = wait.payload.get("child") else {
        return WaitChild::Absent;
    };
    let (step, element) = match (wait.step.as_slice(), wait.fan_out_element()) {
        ([Value::String(step)], _) => (step.as_str(), None),
        (_, Some((step, item))) => (step, Some(item)),
        _ => return WaitChild::Unowned,
    };
    let Some(locator) = locator.as_str() else {
        return WaitChild::Unowned;
    };
    if !super::schema_common::is_identifier(step) {
        return WaitChild::Unowned;
    }
    let segments = locator.split('/').collect::<Vec<_>>();
    let expected_element = element.and_then(attempt_directory_name);
    let ordinal = match (segments.as_slice(), expected_element.as_deref()) {
        ([CHILDREN_DIRECTORY, named, ordinal], None) if *named == step => *ordinal,
        ([CHILDREN_DIRECTORY, named, named_element, ordinal], Some(expected))
            if *named == step && *named_element == expected =>
        {
            *ordinal
        }
        _ => return WaitChild::Unowned,
    };
    let canonical = ordinal
        .parse::<u64>()
        .ok()
        .filter(|ordinal| *ordinal < 1_000_000)
        .and_then(attempt_directory_name);
    if canonical.as_deref() != Some(ordinal) {
        return WaitChild::Unowned;
    }
    WaitChild::Owned {
        locator: PathBuf::from(locator),
        item: element,
    }
}

/// Whether the latest attempt of the run at `requested` ended `failed`, read without the run
/// lock: the evidence that selects a repair continuation over a fresh child, which the
/// continuation reads again under the lock (REQ-CONTINUE-81).
pub fn latest_attempt_failed(requested: &Path) -> Result<bool, LocalRunDirectoryError> {
    let (_, root) = open_run_root(requested)?;
    let state = read_state(&root)?;
    Ok(state
        .attempts
        .last()
        .is_some_and(|attempt| attempt.state == AttemptStateV1::WorkflowFailed))
}

/// The latest child run of `step` under the run at `requested` (REQ-LOCALRUN-177), its run
/// record's state and its fan-out element, or `None` before the step, or the element, has
/// run a child. When the run record's `position` is `[<step>, {"item": n}]` it is the
/// highest ordinal under `children/<step>/<n>/`; otherwise the highest ordinal under the
/// highest element's directory, or under `children/<step>/` for a plain step.
pub fn latest_child_run(
    requested: &Path,
    step: &str,
) -> Result<Option<(PathBuf, RunState, Option<u64>)>, LocalRunDirectoryError> {
    if !super::schema_common::is_identifier(step) {
        return Err(LocalRunDirectoryError::InvalidPath);
    }
    let (normalized, root) = open_run_root(requested)?;
    let named = position_element(&root, step)?;
    let Some(children) = open_optional_directory(&root, CHILDREN_DIRECTORY)? else {
        return Ok(None);
    };
    let Some(step_directory) = open_optional_directory(&children, step)? else {
        return Ok(None);
    };
    let step_path = normalized.join(CHILDREN_DIRECTORY).join(step);
    let (element_path, element, item) = match named {
        Some(item) => {
            let name = attempt_directory_name(item).ok_or(LocalRunDirectoryError::StateInvalid)?;
            let Some(element) = open_optional_directory(&step_directory, &name)? else {
                return Ok(None);
            };
            (step_path.join(name), element, item)
        }
        None => match step_child_entries(&step_directory)?.pop() {
            None => return Ok(None),
            Some((_, name, StepChildEntry::Child(record))) => {
                return Ok(Some((step_path.join(name), record.state, None)));
            }
            Some((item, name, StepChildEntry::Element(element))) => {
                (step_path.join(name), element, item)
            }
        },
    };
    Ok(latest_child_in(&element)?
        .map(|(name, record)| (element_path.join(name), record.state, Some(item))))
}

/// The fan-out element of `step` the run record's `position` names (REQ-WFEXEC-68), the
/// element an abandoned fan-out resumes at even before its child exists, or `None` when the
/// position names no element of `step`.
pub fn positioned_element(
    requested: &Path,
    step: &str,
) -> Result<Option<u64>, LocalRunDirectoryError> {
    let (_, root) = open_run_root(requested)?;
    position_element(&root, step)
}

fn position_element(root: &OwnedFd, step: &str) -> Result<Option<u64>, LocalRunDirectoryError> {
    Ok(read_optional_run_record(root)?.and_then(|record| {
        fan_out_element(&record.position).and_then(|(named, item)| (named == step).then_some(item))
    }))
}

/// The lock a live process holds on the run whose root is `root`, read without taking it.
fn query_run_lock(root: &OwnedFd) -> Result<Option<Flock>, LocalRunDirectoryError> {
    let lock = openat(
        root,
        LOCK_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|source| file_error(root, LOCK_FILE, "open", source))?;
    verify_retry_lock_identity(root, &lock)?;
    query_status_lock(&lock).map_err(|source| file_error(root, LOCK_FILE, "query lock", source))
}

/// The process that holds the lock of the run at `requested`, or `None` while no other
/// process does; the lock is only queried, never taken.
pub fn lock_holder(
    requested: &Path,
) -> Result<Option<rustix::process::Pid>, LocalRunDirectoryError> {
    let (_, root) = open_run_root(requested)?;
    Ok(query_run_lock(&root)?.and_then(|lock| lock.pid))
}

/// Whether `value` has the form of a run record's id (REQ-LOCALRUN-175): `run_` and a
/// lowercase ULID.
pub fn is_run_id(value: &str) -> bool {
    super::schema_common::is_prefixed_ulid(value, RUN_ID_PREFIX)
}

/// The canonical directory of the run `id` under `home` (REQ-LOCALRUN-180): the one
/// `workflows/<definition id>/runs/<id>/` whose `run.json` carries `id`. A value that is not
/// a run id is `InvalidPath`, and an id no run under the home has is `RunNotFound`.
pub fn locate_run(home: &WorkflowHome, id: &str) -> Result<PathBuf, LocalRunDirectoryError> {
    if !is_run_id(id) {
        return Err(LocalRunDirectoryError::InvalidPath);
    }
    for definition in home_definitions(home)? {
        let candidate = home.runs_directory(&definition).join(id);
        let Ok((normalized, root)) = open_run_root(&candidate) else {
            continue;
        };
        if read_run_record(&root).is_ok_and(|record| record.id == id) {
            return Ok(normalized);
        }
    }
    Err(LocalRunDirectoryError::RunNotFound)
}

/// One root run under the `um` home, as `list_runs` reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListedRun {
    pub directory: PathBuf,
    pub record: WorkflowRunRecord,
    pub wait: Option<WaitRecord>,
}

impl ListedRun {
    /// Whether the run is drifted (REQ-LOCALRUN-190): its record reads `running` or
    /// `waiting` and its revision is below `current`, its definition's current revision. A
    /// run with no revision and an exited run never drift.
    pub fn drifted(&self, current: u64) -> bool {
        matches!(self.record.state, RunState::Running | RunState::Waiting)
            && self
                .record
                .revision
                .is_some_and(|revision| revision < current)
    }
}

/// Every root run under the `um` home in `id` order, and each `runs/` entry that is not a
/// readable run directory with the reason.
#[derive(Debug)]
pub struct RunListing {
    pub runs: Vec<ListedRun>,
    pub unreadable: Vec<(PathBuf, LocalRunDirectoryError)>,
}

/// Lists every `workflows/<definition id>/runs/<run id>/` under `home` that holds a
/// `run.json` (REQ-LOCALRUN-173), with its root wait record, taking no lock and writing
/// nothing. A missing `workflows/` or `runs/` directory lists nothing.
pub fn list_runs(home: &WorkflowHome) -> Result<RunListing, LocalRunDirectoryError> {
    let mut listing = RunListing {
        runs: Vec::new(),
        unreadable: Vec::new(),
    };
    for definition in home_definitions(home)? {
        let runs = home.runs_directory(&definition);
        for entry in sorted_entries(&runs)? {
            let path = runs.join(entry);
            match read_listed_run(&path) {
                Ok(run) => listing.runs.push(run),
                Err(error) => listing.unreadable.push((path, error)),
            }
        }
    }
    listing
        .runs
        .sort_by(|left, right| left.record.id.cmp(&right.record.id));
    Ok(listing)
}

fn read_listed_run(path: &Path) -> Result<ListedRun, LocalRunDirectoryError> {
    let (directory, root) = open_run_root(path)?;
    Ok(ListedRun {
        record: read_run_record(&root)?,
        wait: read_optional_wait_record(&root)?,
        directory,
    })
}

/// The definition directories under the home's `workflows/`, in name order.
pub(super) fn home_definitions(home: &WorkflowHome) -> Result<Vec<String>, LocalRunDirectoryError> {
    let workflows = home.workflows_directory();
    Ok(sorted_entries(&workflows)?
        .into_iter()
        .filter(|name| {
            std::fs::symlink_metadata(workflows.join(name)).is_ok_and(|entry| entry.is_dir())
        })
        .filter_map(|name| name.into_string().ok())
        .collect())
}

/// The names in `directory` in order, or none when it does not exist.
fn sorted_entries(directory: &Path) -> Result<Vec<OsString>, LocalRunDirectoryError> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(path_error(directory.to_owned(), "list directory", source)),
    };
    let mut names = entries
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| path_error(directory.to_owned(), "list directory", source))?;
    names.sort();
    Ok(names)
}

/// One child run under a run's `children/` (REQ-LOCALRUN-177).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildRun {
    pub step: String,
    /// The one-based fan-out element the child ran for, `None` for a plain step's child.
    pub item: Option<u64>,
    pub ordinal: u64,
    pub directory: PathBuf,
    pub record: WorkflowRunRecord,
}

/// Every child run `children/<step>/<NNNNNN>/` and `children/<step>/<n>/<NNNNNN>/` of the
/// run at `requested`, in step, then element, then ordinal order, each with its run record;
/// none before a step has run a child.
pub fn child_runs(requested: &Path) -> Result<Vec<ChildRun>, LocalRunDirectoryError> {
    let (normalized, root) = open_run_root(requested)?;
    let Some(children) = open_optional_directory(&root, CHILDREN_DIRECTORY)? else {
        return Ok(Vec::new());
    };
    let mut runs = Vec::new();
    for step in directory_entries(&children)? {
        let Some(step) = String::from_utf8(step)
            .ok()
            .filter(|step| super::schema_common::is_identifier(step))
        else {
            continue;
        };
        let Some(step_directory) = open_optional_directory(&children, &step)? else {
            continue;
        };
        let step_path = normalized.join(CHILDREN_DIRECTORY).join(&step);
        for (number, name, entry) in step_child_entries(&step_directory)? {
            match entry {
                StepChildEntry::Child(record) => runs.push(ChildRun {
                    directory: step_path.join(&name),
                    record: *record,
                    step: step.clone(),
                    item: None,
                    ordinal: number,
                }),
                StepChildEntry::Element(element) => {
                    for ordinal in ordinals_in(&element)? {
                        let child = attempt_directory_name(ordinal)
                            .ok_or(LocalRunDirectoryError::StateInvalid)?;
                        runs.push(ChildRun {
                            record: read_run_record(&open_directory_at(&element, &child)?)?,
                            directory: step_path.join(&name).join(child),
                            step: step.clone(),
                            item: Some(number),
                            ordinal,
                        });
                    }
                }
            }
        }
    }
    Ok(runs)
}

/// Removes the run's root `wait.json` when present (REQ-LOCALRUN-178): a later attempt
/// begins or the run is withdrawn. The attempt copies stay as history.
fn remove_root_wait_record(root: &OwnedFd) -> Result<(), LocalRunDirectoryError> {
    match unlinkat(root, WAIT_RECORD_FILE, AtFlags::empty()) {
        Ok(()) => sync_directory(root),
        Err(Errno::NOENT) => Ok(()),
        Err(source) => Err(file_error(root, WAIT_RECORD_FILE, "remove", source)),
    }
}

/// The canonical path of a run directory and its root handle.
pub(super) fn open_run_root(
    requested: &Path,
) -> Result<(PathBuf, OwnedFd), LocalRunDirectoryError> {
    let normalized = std::fs::canonicalize(requested)
        .map_err(|source| path_error(requested.to_owned(), "resolve run directory", source))?;
    let root = open_directory_path(&normalized)
        .map_err(|source| path_error(normalized.clone(), "open run directory", source))?;
    Ok((normalized, root))
}

fn read_run(root: &OwnedFd) -> Result<LocalRunV1, LocalRunDirectoryError> {
    read_run_with_size(root).map(|(run, _)| run)
}

fn read_run_with_size(root: &OwnedFd) -> Result<(LocalRunV1, u64), LocalRunDirectoryError> {
    let bytes = read_regular_file(root, LOCAL_RUN_FILE)?;
    let size = u64::try_from(bytes.len()).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    Ok((
        decode_run(&bytes).map_err(|source| LocalRunDirectoryError::StateFile {
            path: file_locator(root, LOCAL_RUN_FILE),
            operation: "validate",
            source: Box::new(source),
        })?,
        size,
    ))
}

fn read_state(root: &OwnedFd) -> Result<LocalRunStateV1, LocalRunDirectoryError> {
    read_state_with_size(root).map(|(state, _)| state)
}

fn read_state_with_size(root: &OwnedFd) -> Result<(LocalRunStateV1, u64), LocalRunDirectoryError> {
    let bytes = read_regular_file(root, STATE_FILE)?;
    let size = u64::try_from(bytes.len()).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    Ok((
        decode_state(&bytes).map_err(|source| LocalRunDirectoryError::StateFile {
            path: file_locator(root, STATE_FILE),
            operation: "validate",
            source: Box::new(source),
        })?,
        size,
    ))
}

pub(super) fn mark_validated_result_published(
    requested: &Path,
    attempt_number: u64,
) -> Result<bool, LocalRunDirectoryError> {
    mark_validated_result_published_with(requested, attempt_number, sync_directory)
}

fn mark_validated_result_published_with(
    requested: &Path,
    attempt_number: u64,
    sync: impl Fn(&OwnedFd) -> Result<(), LocalRunDirectoryError>,
) -> Result<bool, LocalRunDirectoryError> {
    let (_, root) = open_run_root(requested)?;
    let lock = open_retry_lock(&root)?;
    match fcntl_lock(&lock, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {}
        Err(Errno::AGAIN | Errno::ACCESS) => return Ok(false),
        Err(source) => return Err(file_error(&root, LOCK_FILE, "lock", source)),
    }
    verify_retry_lock_identity(&root, &lock)?;
    verify_existing_run_layout(&root)?;
    let run = read_run(&root)?;
    let state = read_state(&root)?;
    validate_run_state_pair(&run, &state)?;
    let expected = attempt_result_relative_path(attempt_number);
    let attempt = state
        .attempts
        .iter()
        .find(|attempt| attempt.attempt_number == attempt_number)
        .ok_or(LocalRunDirectoryError::StateConflict)?;
    match &attempt.result {
        AttemptResultV1::Published { relative_directory } if *relative_directory == expected => {
            return Ok(true);
        }
        AttemptResultV1::NotPublished {
            reason: ResultAbsentReasonV1::PublicationPending,
        } if attempt.state.is_terminal() => {}
        _ => return Err(LocalRunDirectoryError::StateConflict),
    }

    let attempts = open_directory_at(&root, ATTEMPTS_DIRECTORY)?;
    let attempt_name =
        attempt_directory_name(attempt_number).ok_or(LocalRunDirectoryError::StateInvalid)?;
    let attempt_directory = open_directory_at(&attempts, &attempt_name)?;
    let result = open_directory_at(&attempt_directory, "result")?;
    let private = Arc::new(open_directory_at(&root, PRIVATE_DIRECTORY)?);
    // Publication synced the files and staged directories before the rename.
    // A failed post-rename sync leaves the result visible but not yet durable;
    // persist Published only after both sides of the rename are synced.
    sync(&result)?;
    sync(&attempt_directory)?;
    sync(&private)?;
    let root = Arc::new(root);
    let store = StateStore {
        root,
        private,
        current: Mutex::new(state),
        pending_recovery_invocations: Mutex::new(BTreeMap::new()),
    };
    store.update(|state| {
        let attempt = state
            .attempts
            .iter_mut()
            .find(|attempt| attempt.attempt_number == attempt_number)
            .ok_or(LocalRunDirectoryError::StateConflict)?;
        match attempt.result {
            AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            } if attempt.state.is_terminal() => {
                attempt.result = AttemptResultV1::Published {
                    relative_directory: expected,
                };
                Ok(())
            }
            _ => Err(LocalRunDirectoryError::StateConflict),
        }
    })?;
    Ok(true)
}

pub fn acquire_local_retry(requested: &Path) -> Result<LocalRetryOpen, LocalRunDirectoryError> {
    for _ in 0..STATUS_SNAPSHOT_ATTEMPTS {
        let (normalized, root) = open_run_root(requested)?;
        if normalized.to_str().is_none() {
            return Err(LocalRunDirectoryError::InvalidPath);
        }
        let lock = open_retry_lock(&root)?;
        match fcntl_lock(&lock, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => {
                verify_retry_lock_identity(&root, &lock)?;
                return open_locked_retry(normalized, root, lock);
            }
            Err(Errno::AGAIN | Errno::ACCESS) => {
                let snapshot = read_local_run_status(requested)
                    .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
                if snapshot.retry
                    == LocalRetryEligibility::Ineligible(RetryIneligibilityReason::RunLocked)
                {
                    return Ok(LocalRetryOpen::Rejected(retry_rejection_from_snapshot(
                        &snapshot,
                        RetryIneligibilityReason::RunLocked,
                    )));
                }
            }
            Err(source) => return Err(file_error(&root, LOCK_FILE, "lock", source)),
        }
    }
    Err(LocalRunDirectoryError::StateConflict)
}

/// A seam for the end-to-end tests of a command that inspects a run and then takes its lock
/// (feature `test-fixtures`): when `UM_TEST_RUN_LOCK_GATE` names a path, the command calls
/// this before the lock, which writes `<path>.inspected` and returns once `<path>` exists,
/// so a test can act on the run between the inspection and the lock; a gate not opened within
/// `TEST_GATE_TIMEOUT` is a `File` error, never a hung command. Without the feature, or
/// the variable, it returns at once.
pub fn await_run_lock_gate() -> Result<(), LocalRunDirectoryError> {
    #[cfg(feature = "test-fixtures")]
    await_test_gate(
        RUN_LOCK_GATE,
        "inspected",
        "the run lock gate was not opened in time",
    )?;
    Ok(())
}

/// A seam for the end-to-end tests of an attempt interrupted before its stage's child is
/// prepared (feature `test-fixtures`, REQ-WFEXEC-72): when `UM_TEST_CHILD_START_GATE` names a
/// path, the child dispatcher calls this before it prepares a child, which writes
/// `<path>.starting` and returns once `<path>` exists, with `await_run_lock_gate`'s timeout.
/// Without the feature, or the variable, it returns at once.
pub fn await_child_start_gate() -> Result<(), LocalRunDirectoryError> {
    #[cfg(feature = "test-fixtures")]
    await_test_gate(
        CHILD_START_GATE,
        "starting",
        "the child start gate was not opened in time",
    )?;
    Ok(())
}

/// The gate both test seams share: when `variable` names a path, writes the marker beside it
/// with `extension` and returns once the path exists, or a `File` error after
/// `TEST_GATE_TIMEOUT`.
#[cfg(feature = "test-fixtures")]
fn await_test_gate(
    variable: &str,
    extension: &str,
    timed_out: &'static str,
) -> Result<(), LocalRunDirectoryError> {
    let Some(gate) = std::env::var_os(variable).map(PathBuf::from) else {
        return Ok(());
    };
    let marker = gate.with_extension(extension);
    std::fs::write(&marker, b"").map_err(|source| LocalRunDirectoryError::File {
        path: marker,
        operation: "write",
        source,
    })?;
    let deadline = um_support::monotonic_now() + TEST_GATE_TIMEOUT;
    while !gate.exists() {
        if um_support::monotonic_now() >= deadline {
            return Err(LocalRunDirectoryError::File {
                path: gate,
                operation: "await",
                source: io::Error::new(io::ErrorKind::TimedOut, timed_out),
            });
        }
        um_support::sleep(TEST_GATE_POLL_INTERVAL);
    }
    Ok(())
}

#[cfg(feature = "test-fixtures")]
const RUN_LOCK_GATE: &str = "UM_TEST_RUN_LOCK_GATE";
#[cfg(feature = "test-fixtures")]
const CHILD_START_GATE: &str = "UM_TEST_CHILD_START_GATE";
#[cfg(feature = "test-fixtures")]
const TEST_GATE_POLL_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(feature = "test-fixtures")]
const TEST_GATE_TIMEOUT: Duration = Duration::from_secs(30);

pub fn acquire_local_continuation(
    requested: &Path,
) -> Result<LocalContinuationOpen, LocalRunDirectoryError> {
    // Unlike retry's status-retry loop, continuation fails closed on lock contention.
    let (normalized, root) = open_run_root(requested)?;
    let lock = open_retry_lock(&root)?;
    match fcntl_lock(&lock, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {}
        Err(Errno::AGAIN | Errno::ACCESS) => {
            let snapshot = read_local_run_status(requested)
                .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            return Ok(LocalContinuationOpen::Rejected(
                retry_rejection_from_snapshot(&snapshot, RetryIneligibilityReason::RunLocked),
            ));
        }
        Err(source) => return Err(file_error(&root, LOCK_FILE, "lock", source)),
    }
    verify_retry_lock_identity(&root, &lock)?;
    verify_existing_run_layout(&root)?;
    let run = read_run(&root)?;
    let state = read_state(&root)?;
    validate_run_state_pair(&run, &state)?;
    let prior = state
        .attempts
        .last()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let prior_execution_root = PathBuf::from(&prior.execution_root);
    let recovery = recovery_status(prior, false);
    let successor = run_successor(&root)?;
    if let LocalRetryEligibility::Ineligible(reason) =
        retry_eligibility(prior.state, &recovery, false, successor.is_some())
    {
        return Ok(LocalContinuationOpen::Rejected(retry_rejection(
            normalized,
            prior.attempt_number,
            reason,
            &recovery,
            successor,
        )));
    }
    // Process ownership must be settled before reading retained workspace evidence.
    let quiescence = match quiesce_run(&state, &SystemLocalRecoveryAuthority) {
        Ok(proof) => proof,
        Err((guard_ids, reason)) => {
            return Ok(LocalContinuationOpen::Rejected(LocalRetryRejection {
                run_directory: normalized,
                attempt_number: prior.attempt_number,
                reason: RetryIneligibilityReason::OwnershipUnproven,
                guard_ids,
                ownership_reason: Some(reason),
                successor: None,
            }));
        }
    };
    // The state claim is authoritative. A crash may have left only a prepared
    // next-attempt directory; remove that orphan under the lock after quiescence.
    let retained = (|| {
        cleanup_unclaimed_attempt_directory(&root, &state)?;
        let mut retained_read_budget = RetainedReadBudget::default();
        let (prior_workflow, _, _) = load_attempt_retained_execution_with_budget(
            &root,
            &run,
            &state,
            prior,
            &mut retained_read_budget,
        )?;
        let (initial_workflow, inputs, maximum_parallel_steps, tree) =
            load_retained_execution_with_budget(&root, &run, &mut retained_read_budget)?;
        validate_retained_outputs_against_definition(prior, &prior_workflow)?;
        verify_retained_output_evidence(&root, &state, prior.attempt_number)?;
        authenticate_retained_output_producers(
            &root,
            &run,
            &state,
            prior,
            &initial_workflow,
            &mut retained_read_budget,
        )?;
        Ok::<_, LocalRunDirectoryError>((
            prior_workflow,
            initial_workflow,
            inputs,
            maximum_parallel_steps,
            tree,
        ))
    })();
    let (prior_workflow, initial_workflow, inputs, maximum_parallel_steps, tree) = match retained {
        Ok(retained) => retained,
        Err(error) => {
            settle_abandoned_snapshot(&root, &state)?;
            return Err(error);
        }
    };
    Ok(LocalContinuationOpen::Acquired(Box::new(
        PendingLocalContinuation {
            normalized,
            root,
            lock,
            run,
            state,
            prior_execution_root,
            prior_workflow,
            initial_workflow,
            inputs,
            maximum_parallel_steps,
            quiescence,
            tree,
            keep_resolved_wait: false,
        },
    )))
}

fn open_retry_lock(root: &OwnedFd) -> Result<File, LocalRunDirectoryError> {
    let lock = openat(
        root,
        LOCK_FILE,
        OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|source| file_error(root, LOCK_FILE, "open", source))?;
    verify_retry_lock_identity(root, &lock)?;
    Ok(lock)
}

fn verify_retry_lock_identity(root: &OwnedFd, lock: &File) -> Result<(), LocalRunDirectoryError> {
    let opened = fstat(lock).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    let current = statat(root, LOCK_FILE, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile
        || FileType::from_raw_mode(current.st_mode) != FileType::RegularFile
        || opened.st_dev != current.st_dev
        || opened.st_ino != current.st_ino
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(())
}

fn open_locked_retry(
    normalized: PathBuf,
    root: OwnedFd,
    lock: File,
) -> Result<LocalRetryOpen, LocalRunDirectoryError> {
    verify_existing_run_layout(&root)?;
    let run = read_run(&root)?;
    let state = read_state(&root)?;
    validate_run_state_pair(&run, &state)?;
    let current = state
        .attempts
        .last()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let recovery = recovery_status(current, false);
    let successor = run_successor(&root)?;
    if let LocalRetryEligibility::Ineligible(reason) =
        retry_eligibility(current.state, &recovery, false, successor.is_some())
    {
        return Ok(LocalRetryOpen::Rejected(retry_rejection(
            normalized,
            current.attempt_number,
            reason,
            &recovery,
            successor,
        )));
    }

    let mut retained_read_budget = RetainedReadBudget::default();
    let (current_workflow, _, _) = load_attempt_retained_execution_with_budget(
        &root,
        &run,
        &state,
        current,
        &mut retained_read_budget,
    )?;
    validate_retained_outputs_against_definition(current, &current_workflow)?;
    let (workflow, inputs, maximum_parallel_steps, tree) =
        load_retained_execution_with_budget(&root, &run, &mut retained_read_budget)?;
    verify_retained_output_evidence(&root, &state, current.attempt_number)?;
    cleanup_unreferenced_retained_values(&root, &state, current.attempt_number)?;
    let private = Arc::new(open_directory_at(&root, PRIVATE_DIRECTORY)?);
    let root = Arc::new(root);
    let state = Arc::new(StateStore {
        root: Arc::clone(&root),
        private,
        current: Mutex::new(state),
        pending_recovery_invocations: Mutex::new(BTreeMap::new()),
    });
    Ok(LocalRetryOpen::Acquired(Box::new(PendingLocalRetry {
        normalized,
        root,
        lock,
        state,
        workflow,
        inputs,
        maximum_parallel_steps,
        definition: attempt_definition_for_run(&run),
        git_baseline: run.git_baseline.as_ref().and_then(local_git_baseline),
        tree,
    })))
}

fn cached_producer_workflow(
    workflows: &mut BTreeMap<AttemptDefinitionV1, ResolvedWorkflow>,
    definition: AttemptDefinitionV1,
    load: impl FnOnce() -> Result<ResolvedWorkflow, LocalRunDirectoryError>,
) -> Result<&ResolvedWorkflow, LocalRunDirectoryError> {
    Ok(match workflows.entry(definition) {
        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::btree_map::Entry::Vacant(entry) => entry.insert(load()?),
    })
}

fn authenticate_retained_output_producers(
    root: &OwnedFd,
    run: &LocalRunV1,
    state: &LocalRunStateV1,
    attempt: &LocalAttemptV1,
    initial_workflow: &ResolvedWorkflow,
    budget: &mut RetainedReadBudget,
) -> Result<(), LocalRunDirectoryError> {
    let index = LocalRunStateIndex::new(state)?;
    let mut workflows =
        BTreeMap::from([(attempt_definition_for_run(run), initial_workflow.clone())]);
    let mut verified_attempts = BTreeSet::new();
    for output in attempt_retained_outputs(attempt) {
        let Some(producer) = output.producer() else {
            continue;
        };
        let producer_attempt = index
            .attempt(producer.attempt_number)
            .filter(|candidate| candidate.attempt_id == producer.attempt_id)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        if producer_attempt
            .progress
            .steps
            .iter()
            .find(|step| step.id == producer.node && step.state == AttemptStepStateV1::Succeeded)
            .and_then(|step| step.outputs.as_deref())
            .and_then(|outputs| {
                outputs
                    .iter()
                    .find(|candidate| candidate.name() == producer.output)
            })
            .is_none_or(|source| !retained_output_payload_matches(source, output, producer))
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        let definition = resolved_attempt_definition(run, state, producer_attempt)?;
        let workflow = cached_producer_workflow(&mut workflows, definition, || {
            load_attempt_retained_execution_with_budget(root, run, state, producer_attempt, budget)
                .map(|(workflow, _, _)| workflow)
        })?;
        validate_retained_outputs_against_definition(producer_attempt, workflow)?;
        if verified_attempts.insert(producer.attempt_number) {
            verify_retained_output_evidence(root, state, producer.attempt_number)?;
        }
        if matches!(output, RetainedOutputV1::Json { .. }) {
            let schema = workflow
                .json_schema(&producer.node, output.name())
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let carrier = open_retained_carrier(root, state, output)?;
            let name = carrier
                .name
                .to_str()
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let bytes =
                read_regular_file_bounded(&carrier.parent, name, MAXIMUM_RETAINED_FILE_BYTES)?;
            let value = um_support::strict_json_from_slice(&bytes)
                .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            if !schema.is_valid(&value) {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
        }
    }
    Ok(())
}

fn attempt_retained_outputs(attempt: &LocalAttemptV1) -> Vec<&RetainedOutputV1> {
    let mut outputs = attempt
        .progress
        .steps
        .iter()
        .flat_map(|step| step.outputs.iter().flatten())
        .collect::<Vec<_>>();
    if let Some(finalization) = &attempt.finalization {
        match finalization {
            AttemptFinalizationV1::Progress(progress) => outputs.extend(
                progress
                    .finalizers
                    .iter()
                    .flat_map(|finalizer| finalizer.outputs.iter().flatten()),
            ),
            AttemptFinalizationV1::Complete(complete) => outputs.extend(
                complete
                    .finalizers
                    .iter()
                    .flat_map(|finalizer| finalizer.outputs.iter().flatten()),
            ),
        }
    }
    outputs
}

fn cleanup_unreferenced_retained_values(
    root: &OwnedFd,
    state: &LocalRunStateV1,
    attempt_number: u64,
) -> Result<(), LocalRunDirectoryError> {
    let attempts = open_directory_at(root, ATTEMPTS_DIRECTORY)?;
    let mut found = false;
    for attempt in state
        .attempts
        .iter()
        .filter(|attempt| attempt.attempt_number == attempt_number)
    {
        found = true;
        let expected = attempt_retained_outputs(attempt)
            .into_iter()
            .filter(|output| {
                output.producer().is_none_or(|producer| {
                    producer.attempt_id == attempt.attempt_id
                        && producer.attempt_number == attempt.attempt_number
                })
            })
            .filter_map(RetainedOutputV1::carrier)
            .map(|carrier| carrier.relative_path.clone())
            .collect::<BTreeSet<_>>();
        let attempt_name = attempt_directory_name(attempt.attempt_number)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        let attempt_directory = open_directory_at(&attempts, &attempt_name)?;
        let values = match openat(
            &attempt_directory,
            VALUES_DIRECTORY,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(values) => values,
            Err(Errno::NOENT) => continue,
            Err(_) => return Err(LocalRunDirectoryError::StateInvalid),
        };
        cleanup_unreferenced_tree(&values, VALUES_DIRECTORY, &expected, 0)?;
        sync_directory(&values)?;
        sync_directory(&attempt_directory)?;
    }
    found
        .then_some(())
        .ok_or(LocalRunDirectoryError::StateInvalid)
}

fn cleanup_unreferenced_tree(
    directory: &OwnedFd,
    relative: &str,
    expected: &BTreeSet<String>,
    depth: usize,
) -> Result<(), LocalRunDirectoryError> {
    if depth > 3 {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    for name in directory_entries(directory)? {
        let name_string = std::str::from_utf8(&name).ok();
        let child = name_string.map(|name| format!("{relative}/{name}"));
        let metadata = statat(directory, name.as_slice(), AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        let kind = FileType::from_raw_mode(metadata.st_mode);
        if kind == FileType::RegularFile {
            if child.as_ref().is_some_and(|path| expected.contains(path)) {
                continue;
            }
            unlinkat(directory, name.as_slice(), AtFlags::empty())
                .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
            continue;
        }
        if kind == FileType::Directory {
            let child_directory = openat(
                directory,
                name.as_slice(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            let child_relative = child.as_deref().unwrap_or("");
            cleanup_unreferenced_tree(&child_directory, child_relative, expected, depth + 1)?;
            if directory_entries(&child_directory)?.is_empty() {
                drop(child_directory);
                unlinkat(directory, name.as_slice(), AtFlags::REMOVEDIR)
                    .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
            }
            continue;
        }
        if child.as_ref().is_some_and(|path| expected.contains(path)) {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        unlinkat(directory, name.as_slice(), AtFlags::empty())
            .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)?;
    }
    Ok(())
}

pub(super) fn required_inherited_outputs(
    attempt: &LocalAttemptV1,
    workflow: &ResolvedWorkflow,
) -> Result<BTreeSet<(String, String)>, LocalRunDirectoryError> {
    let inherited = attempt
        .progress
        .steps
        .iter()
        .filter(|step| step.state == AttemptStepStateV1::Inherited)
        .map(|step| step.id.as_str())
        .collect::<BTreeSet<_>>();
    let reexecuted = attempt
        .progress
        .steps
        .iter()
        .filter(|step| step.state != AttemptStepStateV1::Inherited)
        .map(|step| step.id.clone())
        .collect::<Vec<_>>();
    let mut required = super::continuation::referenced_outputs(&workflow.definition, &reexecuted)
        .into_iter()
        .filter(|source| inherited.contains(source.node.id.as_str()))
        .map(|source| (source.node.id, source.output))
        .collect::<BTreeSet<_>>();
    // An inherited `workflow` step routes the continuation from its retained outcome
    // (REQ-WFEXEC-58), which a step that inherited a skip does not have.
    required.extend(
        attempt
            .progress
            .steps
            .iter()
            .filter(|step| {
                step.state == AttemptStepStateV1::Inherited
                    && step
                        .outputs
                        .iter()
                        .flatten()
                        .any(|output| output.name() == super::validation::CHILD_OUTCOME_OUTPUT)
                    && matches!(
                        workflow.definition.steps.get(&step.id),
                        Some(super::validated::ValidatedStep::Workflow(_))
                    )
            })
            .map(|step| {
                (
                    step.id.clone(),
                    super::validation::CHILD_OUTCOME_OUTPUT.to_owned(),
                )
            }),
    );
    Ok(required)
}

fn load_execution_seed(
    root: &OwnedFd,
    current: &Mutex<LocalRunStateV1>,
    attempt_number: u64,
    admitted: &AdmittedWorkflow,
    artifacts: &ArtifactStaging,
) -> Result<ExecutionSeed<CapturedValue>, LocalRunDirectoryError> {
    let state = lock_state(current)?;
    let run = read_run(root)?;
    let index = LocalRunStateIndex::new(&state)?;
    let mut producer_workflows = BTreeMap::new();
    let mut retained_read_budget = RetainedReadBudget::default();
    let attempt = index
        .attempt(attempt_number)
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    validate_retained_outputs_against_definition(attempt, admitted.workflow())?;
    verify_retained_output_evidence(root, &state, attempt_number)?;
    let required_outputs = required_inherited_outputs(attempt, admitted.workflow())?;
    let mut inherited_steps = BTreeMap::new();
    for step in &attempt.progress.steps {
        if step.state != AttemptStepStateV1::Inherited {
            continue;
        }
        let Some(NodeDetail::Inherited(detail)) = &step.detail else {
            return Err(LocalRunDirectoryError::StateInvalid);
        };
        let disposition = index
            .disposition(attempt_number, &step.id)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        let retained_outputs = step.outputs.iter().flatten().collect::<Vec<_>>();
        if disposition == InheritedDisposition::Skipped && !retained_outputs.is_empty() {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        let mut outputs = BTreeMap::new();
        let mut producers = BTreeMap::new();
        for retained in retained_outputs.into_iter().filter(|retained| {
            required_outputs.contains(&(step.id.clone(), retained.name().to_owned()))
        }) {
            let declaration =
                step_output_declaration(admitted.workflow(), &step.id, retained.name())
                    .ok_or(LocalRunDirectoryError::StateInvalid)?;
            if !retained_output_matches_declaration(retained, declaration) {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            let producer = retained
                .producer()
                .cloned()
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let producer_attempt = index
                .attempt(producer.attempt_number)
                .filter(|attempt| attempt.attempt_id == producer.attempt_id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let definition = resolved_attempt_definition(&run, &state, producer_attempt)?;
            let producer_workflow =
                cached_producer_workflow(&mut producer_workflows, definition, || {
                    load_attempt_retained_execution_with_budget(
                        root,
                        &run,
                        &state,
                        producer_attempt,
                        &mut retained_read_budget,
                    )
                    .map(|(workflow, _, _)| workflow)
                })?;
            let name = retained.name().to_owned();
            let value = load_retained_value(
                root,
                &state,
                producer_workflow,
                artifacts,
                &step.id,
                retained,
            )?;
            if outputs.insert(name.clone(), value).is_some()
                || producers.insert(name, producer).is_some()
            {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
        }
        // A child export the inherited `workflow` step left out stays absent, as it was
        // when the step first succeeded (REQ-WFEXEC-58).
        if disposition == InheritedDisposition::Succeeded
            && required_outputs.iter().any(|(node, output)| {
                node == &step.id
                    && !outputs.contains_key(output)
                    && !matches!(
                        step_output_declaration(admitted.workflow(), node, output),
                        Some(super::document::Output::ChildExport { .. })
                    )
            })
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        inherited_steps.insert(
            step.id.clone(),
            InheritedStepSeed {
                detail: detail.clone(),
                disposition,
                outputs,
                producers,
            },
        );
    }
    let seed = ExecutionSeed::new(admitted, inherited_steps)
        .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    let super::validated::Scheduling::Edges { .. } = &admitted.workflow().definition.scheduling
    else {
        return Ok(seed);
    };
    // An edge-scheduled walk decides each limited edge from the run record's counts
    // (REQ-WFEXEC-64, INV-ENGINE-22); a run without a record has none.
    let seed = match read_optional_run_record(root)? {
        Some(record) => seed.with_edge_counts(record.edges),
        None => seed,
    };
    // An edge-scheduled continuation starts the one step it names (REQ-CONTINUE-3).
    Ok(
        match attempt
            .continuation
            .as_ref()
            .map(|record| record.from_steps.as_slice())
        {
            Some([from]) => seed.with_entry(from.clone()),
            _ => seed,
        },
    )
}

/// The wait `record` names as the engine holds it: its kind, its waiting step, `then`,
/// `onHandBack` and the `payload.outcome` it was entered on when that is a string. The record
/// itself, not this, is what a kept wait writes back (REQ-WFEXEC-72).
fn resumed_entered_wait(record: &WaitRecord) -> Result<EnteredWait, LocalRunDirectoryError> {
    let step = record
        .step
        .first()
        .and_then(Value::as_str)
        .ok_or(LocalRunDirectoryError::WaitRecordInvalid)?;
    Ok(EnteredWait {
        step: step.to_owned(),
        outcome: record
            .payload
            .get("outcome")
            .and_then(Value::as_str)
            .map(str::to_owned),
        kind: record.kind,
        then: record.then.clone(),
        on_hand_back: record.on_hand_back.clone(),
        rule: None,
        interrupted: None,
        kept: None,
    })
}

fn step_output_declaration<'a>(
    workflow: &'a ResolvedWorkflow,
    node: &str,
    output: &str,
) -> Option<&'a super::document::Output> {
    let step = workflow.definition.steps.get(node)?;
    step.common()
        .outputs
        .get(output)
        .map(|output| &output.definition)
}

struct LocalRunStateIndex<'a> {
    attempts: &'a [LocalAttemptV1],
    steps: Vec<BTreeMap<&'a str, &'a AttemptStepV1>>,
    dispositions: Vec<BTreeMap<&'a str, InheritedDisposition>>,
}

impl<'a> LocalRunStateIndex<'a> {
    fn new(state: &'a LocalRunStateV1) -> Result<Self, LocalRunDirectoryError> {
        let mut steps = Vec::with_capacity(state.attempts.len());
        let mut dispositions = Vec::with_capacity(state.attempts.len());
        for (index, attempt) in state.attempts.iter().enumerate() {
            let expected_number = u64::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or(LocalRunDirectoryError::AttemptNumberInvalid)?;
            if attempt.attempt_number != expected_number {
                return Err(LocalRunDirectoryError::AttemptNumberInvalid);
            }
            let mut attempt_steps = BTreeMap::new();
            for step in &attempt.progress.steps {
                if attempt_steps.insert(step.id.as_str(), step).is_some() {
                    return Err(LocalRunDirectoryError::AttemptStepDuplicate);
                }
            }
            let mut attempt_dispositions = BTreeMap::new();
            for step in &attempt.progress.steps {
                let disposition = match step.state {
                    AttemptStepStateV1::Succeeded => Some(InheritedDisposition::Succeeded),
                    AttemptStepStateV1::Skipped => Some(InheritedDisposition::Skipped),
                    AttemptStepStateV1::Inherited => {
                        let Some(NodeDetail::Inherited(detail)) = &step.detail else {
                            return Err(LocalRunDirectoryError::AttemptInheritedStepInvalid);
                        };
                        let prior_index = state_attempt_index(detail.prior_attempt_number)
                            .filter(|prior_index| *prior_index < index)
                            .ok_or(LocalRunDirectoryError::AttemptInheritedStepInvalid)?;
                        Some(
                            dispositions
                                .get(prior_index)
                                .and_then(|prior: &BTreeMap<&str, InheritedDisposition>| {
                                    prior.get(step.id.as_str())
                                })
                                .copied()
                                .ok_or(LocalRunDirectoryError::AttemptInheritedStepInvalid)?,
                        )
                    }
                    AttemptStepStateV1::Pending
                    | AttemptStepStateV1::Starting
                    | AttemptStepStateV1::Running
                    | AttemptStepStateV1::CapturingOutputs
                    | AttemptStepStateV1::Cancelling
                    | AttemptStepStateV1::Failed
                    | AttemptStepStateV1::Blocked
                    | AttemptStepStateV1::NotRun
                    | AttemptStepStateV1::Cancelled => None,
                };
                if let Some(disposition) = disposition {
                    attempt_dispositions.insert(step.id.as_str(), disposition);
                }
            }
            steps.push(attempt_steps);
            dispositions.push(attempt_dispositions);
        }
        Ok(Self {
            attempts: &state.attempts,
            steps,
            dispositions,
        })
    }

    fn attempt(&self, attempt_number: u64) -> Option<&'a LocalAttemptV1> {
        let index = state_attempt_index(attempt_number)?;
        self.attempts
            .get(index)
            .filter(|attempt| attempt.attempt_number == attempt_number)
    }

    fn step(&self, attempt_number: u64, node: &str) -> Option<&'a AttemptStepV1> {
        self.steps
            .get(state_attempt_index(attempt_number)?)?
            .get(node)
            .copied()
    }

    fn disposition(&self, attempt_number: u64, node: &str) -> Option<InheritedDisposition> {
        self.dispositions
            .get(state_attempt_index(attempt_number)?)?
            .get(node)
            .copied()
    }
}

fn state_attempt_index(attempt_number: u64) -> Option<usize> {
    usize::try_from(attempt_number.checked_sub(1)?).ok()
}

fn load_retained_value(
    root: &OwnedFd,
    state: &LocalRunStateV1,
    producer_workflow: &ResolvedWorkflow,
    artifacts: &ArtifactStaging,
    node: &str,
    retained: &RetainedOutputV1,
) -> Result<CapturedValue, LocalRunDirectoryError> {
    load_retained_value_from(producer_workflow, artifacts, node, retained, |retained| {
        open_retained_carrier(root, state, retained)
    })
}

fn load_retained_value_from(
    producer_workflow: &ResolvedWorkflow,
    artifacts: &ArtifactStaging,
    node: &str,
    retained: &RetainedOutputV1,
    mut open: impl FnMut(&RetainedOutputV1) -> Result<RetainedCarrierProducer, LocalRunDirectoryError>,
) -> Result<CapturedValue, LocalRunDirectoryError> {
    match retained {
        RetainedOutputV1::Text { carrier, .. } => {
            let mut producer = open(retained)?;
            let value = capture_retained_candidate(
                artifacts,
                retained.name(),
                CaptureCandidateDeclaration::RetainedFile(RetainedFileCaptureDeclaration::text(
                    retained.name(),
                    &mut producer,
                )),
            )?;
            let CapturedValue::Text(text) = &value else {
                return Err(LocalRunDirectoryError::StateInvalid);
            };
            retained_semantic_carrier_matches(text.carrier(), carrier)?;
            Ok(value)
        }
        RetainedOutputV1::Json { carrier, .. } => {
            let schema = producer_workflow
                .json_schema(node, retained.name())
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let mut producer = open(retained)?;
            let value = capture_retained_candidate(
                artifacts,
                retained.name(),
                CaptureCandidateDeclaration::RetainedFile(RetainedFileCaptureDeclaration::json(
                    retained.name(),
                    schema,
                    &mut producer,
                )),
            )?;
            let CapturedValue::Json(json) = &value else {
                return Err(LocalRunDirectoryError::StateInvalid);
            };
            retained_semantic_carrier_matches(json.carrier(), carrier)?;
            Ok(value)
        }
        RetainedOutputV1::File {
            media_type,
            carrier,
            ..
        } => {
            let mut producer = open(retained)?;
            let value = capture_retained_candidate(
                artifacts,
                retained.name(),
                CaptureCandidateDeclaration::RetainedFile(RetainedFileCaptureDeclaration::new(
                    retained.name(),
                    media_type,
                    &mut producer,
                )),
            )?;
            let CapturedValue::File(file) = &value else {
                return Err(LocalRunDirectoryError::StateInvalid);
            };
            if file.size() != carrier.size_bytes || file.sha256() != carrier.digest.value {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            Ok(value)
        }
        RetainedOutputV1::GitBranch {
            artifact_version,
            object_format,
            base_oid,
            head_oid,
            tree_oid,
            carrier,
            ..
        } => {
            if *artifact_version != 1 || object_format != "sha1" {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            let metadata = GitBranchMetadata::new(
                Arc::from(base_oid.as_str()),
                Arc::from(head_oid.as_str()),
                Arc::from(tree_oid.as_str()),
            );
            let branch = match carrier {
                None => super::artifact::CapturedGitBranch::from_retained(
                    Arc::from(retained.name()),
                    metadata,
                    None,
                ),
                Some(carrier) => {
                    let mut producer = open(retained)?;
                    let mut declarations = [CaptureCandidateDeclaration::GitBranch(
                        GitBranchCaptureDeclaration::new(
                            retained.name(),
                            metadata,
                            Some(&mut producer),
                        ),
                    )];
                    let mut captured = artifacts
                        .capture_candidates(&mut declarations, &CaptureCancellation::default())
                        .map_err(|_| LocalRunDirectoryError::StateInvalid)?
                        .commit();
                    let value = captured
                        .remove(retained.name())
                        .ok_or(LocalRunDirectoryError::StateInvalid)?;
                    let CapturedValue::GitBranch(branch) = &value else {
                        return Err(LocalRunDirectoryError::StateInvalid);
                    };
                    let staged = branch
                        .carrier()
                        .ok_or(LocalRunDirectoryError::StateInvalid)?;
                    if staged.size() != carrier.size_bytes
                        || staged.sha256() != carrier.digest.value
                    {
                        return Err(LocalRunDirectoryError::StateInvalid);
                    }
                    return Ok(value);
                }
            };
            Ok(CapturedValue::GitBranch(branch))
        }
    }
}

struct RetainedCarrierProducer {
    source: File,
    parent: OwnedFd,
    name: OsString,
}

impl CarrierProducer for RetainedCarrierProducer {
    fn stream_to(&mut self, destination: &mut CarrierDestination<'_>) -> io::Result<()> {
        io::copy(&mut self.source, destination).map(|_| ())
    }

    fn supports_hard_links(&self) -> bool {
        true
    }

    fn hard_link_to(&mut self, destination: &OwnedFd, name: &std::ffi::OsStr) -> io::Result<()> {
        linkat(
            &self.parent,
            &self.name,
            destination,
            name,
            AtFlags::empty(),
        )
        .map_err(std::io::Error::from)?;
        let identity_matches = fstat(&self.source)
            .and_then(|source| {
                statat(destination, name, AtFlags::SYMLINK_NOFOLLOW).map(|linked| {
                    FileType::from_raw_mode(linked.st_mode) == FileType::RegularFile
                        && source.st_dev == linked.st_dev
                        && source.st_ino == linked.st_ino
                })
            })
            .unwrap_or(false);
        if identity_matches {
            Ok(())
        } else {
            let _ = unlinkat(destination, name, AtFlags::empty());
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "retained carrier identity changed",
            ))
        }
    }
}

fn capture_retained_candidate(
    artifacts: &ArtifactStaging,
    name: &str,
    declaration: CaptureCandidateDeclaration<'_>,
) -> Result<CapturedValue, LocalRunDirectoryError> {
    let mut declarations = [declaration];
    artifacts
        .capture_candidates(&mut declarations, &CaptureCancellation::default())
        .map_err(|_| LocalRunDirectoryError::StateInvalid)?
        .commit()
        .remove(name)
        .ok_or(LocalRunDirectoryError::StateInvalid)
}

fn retained_semantic_carrier_matches(
    bytes: &[u8],
    carrier: &RetainedCarrierV1,
) -> Result<(), LocalRunDirectoryError> {
    (u64::try_from(bytes.len()) == Ok(carrier.size_bytes)
        && lowercase_hex(digest(&SHA256, bytes).as_ref()) == carrier.digest.value)
        .then_some(())
        .ok_or(LocalRunDirectoryError::StateInvalid)
}

/// The retained output `name` of `step`, a step of `attempt` that succeeded or inherited,
/// with its producer: an inherited output names the attempt that produced it, and a
/// succeeded step's is `attempt` itself. `None` for another state or an output not retained.
fn completed_step_output(
    attempt: &LocalAttemptV1,
    step: &AttemptStepV1,
    name: &str,
) -> Option<RetainedOutputV1> {
    if !matches!(
        step.state,
        AttemptStepStateV1::Succeeded | AttemptStepStateV1::Inherited
    ) {
        return None;
    }
    let mut output = step
        .outputs
        .iter()
        .flatten()
        .find(|output| output.name() == name)?
        .clone();
    if output.producer().is_none() && step.state == AttemptStepStateV1::Succeeded {
        output.set_producer(own_output_producer(attempt, &step.id, name));
    }
    Some(output)
}

/// The producer of the output `output` of `node`, a step `attempt` ran itself
/// (REQ-CONTINUE-15): the attempt's ids, the node and the output's name.
fn own_output_producer(
    attempt: &LocalAttemptV1,
    node: &str,
    output: &str,
) -> super::runtime::OutputProducer {
    super::runtime::OutputProducer {
        attempt_id: attempt.attempt_id.clone(),
        attempt_number: attempt.attempt_number,
        node: node.to_owned(),
        output: output.to_owned(),
    }
}

/// The bytes of `output`'s carrier, read through `open_retained_carrier` and checked against
/// the carrier's size and digest; a carrier that does not match is `StateInvalid`.
fn read_retained_carrier(
    root: &OwnedFd,
    state: &LocalRunStateV1,
    output: &RetainedOutputV1,
) -> Result<Vec<u8>, LocalRunDirectoryError> {
    let carrier = output
        .carrier()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let producer = open_retained_carrier(root, state, output)?;
    let mut bytes = Vec::new();
    // One byte past the recorded size, so a longer carrier fails the size check.
    producer
        .source
        .take(carrier.size_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    retained_semantic_carrier_matches(&bytes, carrier)?;
    Ok(bytes)
}

fn open_retained_carrier(
    root: &OwnedFd,
    state: &LocalRunStateV1,
    output: &RetainedOutputV1,
) -> Result<RetainedCarrierProducer, LocalRunDirectoryError> {
    let producer = output
        .producer()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let owner = state
        .attempts
        .iter()
        .find(|attempt| {
            attempt.attempt_number == producer.attempt_number
                && attempt.attempt_id == producer.attempt_id
        })
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let carrier = output
        .carrier()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let attempts = open_directory_at(root, ATTEMPTS_DIRECTORY)?;
    let owner_name =
        attempt_directory_name(owner.attempt_number).ok_or(LocalRunDirectoryError::StateInvalid)?;
    let owner = open_directory_at(&attempts, &owner_name)?;
    let path = Path::new(&carrier.relative_path);
    let name = path
        .file_name()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let mut parent = owner;
    for component in path
        .parent()
        .ok_or(LocalRunDirectoryError::StateInvalid)?
        .components()
    {
        let std::path::Component::Normal(component) = component else {
            return Err(LocalRunDirectoryError::StateInvalid);
        };
        parent = open_directory_at(&parent, component)?;
    }
    let source = openat(
        &parent,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    Ok(RetainedCarrierProducer {
        source,
        parent,
        name: name.to_owned(),
    })
}

pub(super) fn verify_retained_output_evidence(
    root: &OwnedFd,
    state: &LocalRunStateV1,
    attempt_number: u64,
) -> Result<(), LocalRunDirectoryError> {
    let attempts = open_directory_at(root, ATTEMPTS_DIRECTORY)?;
    let mut found = false;
    let mut output_count = 0_usize;
    let mut output_bytes = 0_u64;
    for attempt in state
        .attempts
        .iter()
        .filter(|attempt| attempt.attempt_number == attempt_number)
    {
        found = true;
        for output in attempt_retained_outputs(attempt) {
            output_count = output_count
                .checked_add(1)
                .filter(|count| *count <= MAXIMUM_RETAINED_OUTPUTS)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            if let Some(carrier) = output.carrier() {
                output_bytes = output_bytes
                    .checked_add(carrier.size_bytes)
                    .filter(|bytes| *bytes <= MAXIMUM_RETAINED_OUTPUT_BYTES)
                    .ok_or(LocalRunDirectoryError::StateInvalid)?;
            }
            let owner = match output.producer() {
                Some(producer) => state
                    .attempts
                    .iter()
                    .find(|candidate| {
                        candidate.attempt_number == producer.attempt_number
                            && candidate.attempt_id == producer.attempt_id
                    })
                    .ok_or(LocalRunDirectoryError::StateInvalid)?,
                None => attempt,
            };
            let owner_name = attempt_directory_name(owner.attempt_number)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let owner_directory = open_directory_at(&attempts, &owner_name)?;
            verify_retained_output(&owner_directory, output)?;
        }
    }
    if !found {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(())
}

fn verify_retained_output(
    attempt: &OwnedFd,
    output: &RetainedOutputV1,
) -> Result<(), LocalRunDirectoryError> {
    let Some(carrier) = output.carrier() else {
        return Ok(());
    };
    let path = Path::new(&carrier.relative_path);
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let parent_path = path.parent().ok_or(LocalRunDirectoryError::StateInvalid)?;
    let mut parent = dup(attempt).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    for component in parent_path.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(LocalRunDirectoryError::StateInvalid);
        };
        parent = open_directory_at(&parent, component)?;
    }
    verify_retained_carrier_with_sync(&parent, name, carrier, false)?;
    let bytes = match output {
        RetainedOutputV1::Text { .. } | RetainedOutputV1::Json { .. } => Some(
            read_regular_file_bounded(&parent, name, MAXIMUM_RETAINED_FILE_BYTES)?,
        ),
        RetainedOutputV1::File { .. } | RetainedOutputV1::GitBranch { .. } => None,
    };
    match (output, bytes) {
        (RetainedOutputV1::Text { .. }, Some(bytes)) => {
            std::str::from_utf8(&bytes).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        }
        (RetainedOutputV1::Json { .. }, Some(bytes)) => {
            let value = um_support::strict_json_from_slice(&bytes)
                .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            let canonical = super::canonical_json::to_bounded_bytes(&value, carrier.size_bytes)
                .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            if canonical.as_ref() != bytes {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
        }
        (
            RetainedOutputV1::GitBranch {
                base_oid,
                head_oid,
                tree_oid,
                carrier: Some(_),
                ..
            },
            None,
        ) => {
            use std::sync::atomic::AtomicBool;
            let descriptor = openat(
                &parent,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            let mut descriptor = descriptor;
            super::git_artifact::validate_git_bundle(
                &mut descriptor,
                super::git_artifact::GitArtifactDescriptor {
                    base_oid,
                    head_oid,
                    tree_oid,
                },
                &mut super::git_artifact::GitArtifactValidationBudget::default(),
                &AtomicBool::new(false),
            )
            .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        }
        (RetainedOutputV1::File { .. }, None)
        | (RetainedOutputV1::GitBranch { carrier: None, .. }, None) => {}
        _ => return Err(LocalRunDirectoryError::StateInvalid),
    }
    Ok(())
}

/// The run root holds exactly the entries of `run_root_entries`, with or without the run
/// record `run.json`, the directory `children/` (REQ-LOCALRUN-98, REQ-LOCALRUN-177), the
/// wait record `wait.json` as a regular file, the directory `controls/` (REQ-LOCALRUN-178)
/// and the directory `mirror/` (REQ-LOCALRUN-193).
fn verify_existing_run_layout(root: &OwnedFd) -> Result<(), LocalRunDirectoryError> {
    let mut entries = directory_entries(root)?;
    for name in [
        LOCAL_RUN_FILE,
        STATE_FILE,
        LOCK_FILE,
        WORKFLOW_DIRECTORY,
        ATTEMPTS_DIRECTORY,
        PRIVATE_DIRECTORY,
    ] {
        if !entries.contains(name.as_bytes()) {
            return Err(LocalRunDirectoryError::StateFile {
                path: file_locator(root, name),
                operation: "validate layout",
                source: Box::new(LocalRunDirectoryError::StateInvalid),
            });
        }
    }
    entries.remove(RUN_RECORD_FILE.as_bytes());
    let optional_entries_typed = [
        (CHILDREN_DIRECTORY, FileType::Directory),
        (CONTROLS_DIRECTORY, FileType::Directory),
        (MIRROR_DIRECTORY, FileType::Directory),
        (WAIT_RECORD_FILE, FileType::RegularFile),
    ]
    .into_iter()
    .all(|(name, expected)| {
        !entries.remove(name.as_bytes())
            || statat(root, name, AtFlags::SYMLINK_NOFOLLOW)
                .is_ok_and(|stat| FileType::from_raw_mode(stat.st_mode) == expected)
    });
    if entries != run_root_entries() || !optional_entries_typed {
        return Err(LocalRunDirectoryError::StateFile {
            path: file_locator(root, "."),
            operation: "validate layout",
            source: Box::new(LocalRunDirectoryError::StateInvalid),
        });
    }
    open_directory_at(root, ATTEMPTS_DIRECTORY)?;
    Ok(())
}

fn run_root_entries() -> BTreeSet<Vec<u8>> {
    BTreeSet::from([
        LOCAL_RUN_FILE.as_bytes().to_vec(),
        STATE_FILE.as_bytes().to_vec(),
        LOCK_FILE.as_bytes().to_vec(),
        WORKFLOW_DIRECTORY.as_bytes().to_vec(),
        ATTEMPTS_DIRECTORY.as_bytes().to_vec(),
        PRIVATE_DIRECTORY.as_bytes().to_vec(),
    ])
}

/// The run's retained definition, inputs and parallelism, and its tree record when
/// the run was admitted from a tree (REQ-LOCALRUN-170).
pub(super) fn load_retained_execution_with_budget(
    root: &OwnedFd,
    run: &LocalRunV1,
    budget: &mut RetainedReadBudget,
) -> Result<
    (
        ResolvedWorkflow,
        ResolvedInputs,
        usize,
        Option<ManifestTreeV1>,
    ),
    LocalRunDirectoryError,
> {
    let workflow_directory = open_directory_at(root, WORKFLOW_DIRECTORY)?;
    load_retained_execution_directory(
        workflow_directory,
        &run.workflow_digest,
        &run.workflow_manifest_digest,
        budget,
    )
}

pub(super) fn load_attempt_retained_execution_with_budget(
    root: &OwnedFd,
    run: &LocalRunV1,
    state: &LocalRunStateV1,
    selected: &LocalAttemptV1,
    budget: &mut RetainedReadBudget,
) -> Result<(ResolvedWorkflow, ResolvedInputs, usize), LocalRunDirectoryError> {
    let definition = resolved_attempt_definition(run, state, selected)?;
    let workflow_directory = match definition.locator {
        AttemptDefinitionLocatorV1::Run => open_directory_at(root, WORKFLOW_DIRECTORY)?,
        AttemptDefinitionLocatorV1::Attempt { attempt_number } => {
            let attempts = open_directory_at(root, ATTEMPTS_DIRECTORY)?;
            let name = attempt_directory_name(attempt_number)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let attempt = open_directory_at(&attempts, &name)?;
            open_directory_at(&attempt, WORKFLOW_DIRECTORY)?
        }
        AttemptDefinitionLocatorV1::PriorAttempt { .. } => {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
    };
    load_retained_execution_directory(
        workflow_directory,
        &definition.digest,
        &definition.manifest_digest,
        budget,
    )
    .map(|(workflow, inputs, maximum_parallel_steps, _)| (workflow, inputs, maximum_parallel_steps))
}

fn resolved_attempt_definition(
    run: &LocalRunV1,
    state: &LocalRunStateV1,
    selected: &LocalAttemptV1,
) -> Result<AttemptDefinitionV1, LocalRunDirectoryError> {
    let Some(mut definition) = selected.definition.as_ref() else {
        return Ok(attempt_definition_for_run(run));
    };
    let mut visited = BTreeSet::new();
    loop {
        if !visited.insert(definition.locator.clone()) {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        match definition.locator {
            AttemptDefinitionLocatorV1::Run | AttemptDefinitionLocatorV1::Attempt { .. } => {
                return Ok(definition.clone());
            }
            AttemptDefinitionLocatorV1::PriorAttempt { attempt_number } => {
                let prior = state
                    .attempts
                    .iter()
                    .find(|attempt| attempt.attempt_number == attempt_number)
                    .ok_or(LocalRunDirectoryError::StateInvalid)?;
                let Some(prior_definition) = prior.definition.as_ref() else {
                    if definition.digest != run.workflow_digest
                        || definition.manifest_digest != run.workflow_manifest_digest
                    {
                        return Err(LocalRunDirectoryError::StateInvalid);
                    }
                    return Ok(attempt_definition_for_run(run));
                };
                if definition.digest != prior_definition.digest
                    || definition.manifest_digest != prior_definition.manifest_digest
                {
                    return Err(LocalRunDirectoryError::StateInvalid);
                }
                definition = prior_definition;
            }
        }
    }
}

fn load_retained_execution_directory(
    workflow_directory: OwnedFd,
    expected_workflow_digest: &DigestV1,
    expected_manifest_digest: &DigestV1,
    budget: &mut RetainedReadBudget,
) -> Result<
    (
        ResolvedWorkflow,
        ResolvedInputs,
        usize,
        Option<ManifestTreeV1>,
    ),
    LocalRunDirectoryError,
> {
    let manifest_bytes = read_regular_file(&workflow_directory, WORKFLOW_MANIFEST_FILE)?;
    budget.account(&manifest_bytes)?;
    if DigestV1::sha256(&manifest_bytes) != *expected_manifest_digest {
        return Err(LocalRunDirectoryError::StateFile {
            path: file_locator(&workflow_directory, WORKFLOW_MANIFEST_FILE),
            operation: "validate manifest",
            source: Box::new(LocalRunDirectoryError::ManifestDigestInvalid),
        });
    }
    let manifest: WorkflowManifestV1 = decode_schema_one(&manifest_bytes)?;
    validate_manifest(&manifest).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    // The tree copy is an entry only when the manifest, authenticated by its digest,
    // declares it; `verify_tree_copy` judges the copy's contents (REQ-LOCALRUN-171).
    let mut expected_workflow_entries = BTreeSet::from([
        WORKFLOW_MANIFEST_FILE.as_bytes().to_vec(),
        WORKFLOW_FILES_DIRECTORY.as_bytes().to_vec(),
    ]);
    if manifest.tree.is_some() {
        expected_workflow_entries.insert(WORKFLOW_TREE_DIRECTORY.as_bytes().to_vec());
        open_directory_at(&workflow_directory, WORKFLOW_TREE_DIRECTORY)?;
    }
    if directory_entries(&workflow_directory)? != expected_workflow_entries {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    let files = open_directory_at(&workflow_directory, WORKFLOW_FILES_DIRECTORY)?;
    let expected_entries = (1..retained_manifest_file_count(&manifest)?)
        .map(retained_file_name)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(String::into_bytes)
        .collect::<BTreeSet<_>>();
    if directory_entries(&files)? != expected_entries {
        return Err(LocalRunDirectoryError::StateInvalid);
    }

    let mut captured_file_bytes = 0;
    let mut read_file = |file: &ManifestFileV1| {
        let name = retained_file_name(file.ordinal)?;
        let bytes = read_retained_file(&files, &name)?;
        let size = u64::try_from(bytes.len()).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        account_retained_bytes(
            &mut captured_file_bytes,
            size,
            MAXIMUM_RETAINED_CAPTURED_FILE_BYTES,
        )?;
        budget.account(&bytes)?;
        if size != file.size_bytes || DigestV1::sha256(&bytes) != file.digest {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        Ok(bytes)
    };

    let mut source_closure = BTreeMap::new();
    for source in &manifest.source_files {
        if source_closure
            .insert(
                source.path.clone(),
                Arc::<[u8]>::from(read_file(&source.file)?),
            )
            .is_some()
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
    }
    let mut input_bytes = 0_u64;
    let mut attachment_count = 0_usize;
    let mut inputs = BTreeMap::new();
    for (name, input) in &manifest.inputs {
        let value = match input {
            ManifestInputV1::Text { file } | ManifestInputV1::Json { file } => {
                let bytes = read_file(file)?;
                if file.size_bytes > MAXIMUM_RETAINED_TEXT_BYTES {
                    return Err(LocalRunDirectoryError::StateInvalid);
                }
                account_retained_bytes(
                    &mut input_bytes,
                    file.size_bytes,
                    MAXIMUM_RETAINED_INPUT_BYTES,
                )?;
                if matches!(input, ManifestInputV1::Json { .. }) {
                    ResolvedInput::Json(
                        ResolvedJsonInput::from_source(Arc::from(bytes))
                            .map_err(|_| LocalRunDirectoryError::StateInvalid)?,
                    )
                } else {
                    let text = String::from_utf8(bytes)
                        .map(Arc::<str>::from)
                        .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
                    ResolvedInput::Text(text)
                }
            }
            ManifestInputV1::File { media_type, file } => {
                if file.size_bytes > MAXIMUM_RETAINED_FILE_BYTES
                    || !super::is_valid_media_type(media_type)
                {
                    return Err(LocalRunDirectoryError::StateInvalid);
                }
                account_retained_bytes(
                    &mut input_bytes,
                    file.size_bytes,
                    MAXIMUM_RETAINED_INPUT_BYTES,
                )?;
                ResolvedInput::File(super::admission::ResolvedFile::new(
                    Arc::<str>::from(media_type.as_str()),
                    Arc::<[u8]>::from(read_file(file)?),
                ))
            }
            ManifestInputV1::Attachments { items } => {
                attachment_count = attachment_count
                    .checked_add(items.len())
                    .filter(|count| *count <= 256)
                    .ok_or(LocalRunDirectoryError::StateInvalid)?;
                let mut attachments = Vec::with_capacity(items.len());
                for attachment in items {
                    if attachment.file.size_bytes > MAXIMUM_RETAINED_FILE_BYTES {
                        return Err(LocalRunDirectoryError::StateInvalid);
                    }
                    account_retained_bytes(
                        &mut input_bytes,
                        attachment.file.size_bytes,
                        MAXIMUM_RETAINED_INPUT_BYTES,
                    )?;
                    attachments.push(ResolvedAttachment::new(
                        Arc::<str>::from(attachment.media_type.as_str()),
                        Arc::<[u8]>::from(read_file(&attachment.file)?),
                    ));
                }
                ResolvedInput::Attachments(Arc::from(attachments))
            }
        };
        inputs.insert(name.clone(), value);
    }
    let replacement = manifest
        .agent_profile_replacement
        .as_ref()
        .map(|file| {
            if file.size_bytes > MAXIMUM_AGENT_PROFILE_REPLACEMENT_BYTES {
                return Err(LocalRunDirectoryError::StateInvalid);
            }
            read_file(file).map(AgentProfileReplacementDocument::new)
        })
        .transpose()?;
    let workflow = resolve_retained(
        PathBuf::from(&manifest.source_root),
        &manifest.workflow_path,
        source_closure,
        replacement.as_ref(),
    )
    .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
    if workflow.content_digest.algorithm.as_str() != expected_workflow_digest.algorithm
        || workflow.content_digest.value != expected_workflow_digest.value
        || workflow.declared_inputs().len() != inputs.len()
        || workflow.declared_inputs().iter().any(|(name, kind)| {
            !matches!(
                (kind, inputs.get(name)),
                (
                    super::validated::WorkflowValueType::Text,
                    Some(ResolvedInput::Text(_))
                ) | (
                    super::validated::WorkflowValueType::Json,
                    Some(ResolvedInput::Json(_))
                ) | (
                    super::validated::WorkflowValueType::File,
                    Some(ResolvedInput::File(_))
                ) | (
                    super::validated::WorkflowValueType::AttachmentCollection,
                    Some(ResolvedInput::Attachments(_))
                )
            )
        })
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok((
        workflow,
        ResolvedInputs::new(inputs),
        manifest.maximum_parallel_steps,
        manifest.tree,
    ))
}

fn retained_manifest_file_count(
    manifest: &WorkflowManifestV1,
) -> Result<u64, LocalRunDirectoryError> {
    let input_files = manifest.inputs.values().try_fold(0_usize, |count, input| {
        count.checked_add(match input {
            ManifestInputV1::Text { .. }
            | ManifestInputV1::Json { .. }
            | ManifestInputV1::File { .. } => 1,
            ManifestInputV1::Attachments { items } => items.len(),
        })
    });
    let count = manifest
        .source_files
        .len()
        .checked_add(input_files.ok_or(LocalRunDirectoryError::StateInvalid)?)
        .and_then(|count| {
            count.checked_add(usize::from(manifest.agent_profile_replacement.is_some()))
        })
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    u64::try_from(count)
        .ok()
        .and_then(|count| count.checked_add(1))
        .ok_or(LocalRunDirectoryError::StateInvalid)
}

fn read_retained_file(parent: &OwnedFd, name: &str) -> Result<Vec<u8>, LocalRunDirectoryError> {
    read_regular_file_bounded(parent, name, MAXIMUM_RETAINED_FILE_BYTES)
}

fn retry_rejection_from_snapshot(
    snapshot: &LocalRunStatusSnapshot,
    reason: RetryIneligibilityReason,
) -> LocalRetryRejection {
    retry_rejection(
        snapshot.run_directory.clone(),
        snapshot.current_attempt_number,
        reason,
        &snapshot.recovery,
        snapshot.record.as_ref().and_then(record_value_successor),
    )
}

fn retry_rejection(
    run_directory: PathBuf,
    attempt_number: u64,
    reason: RetryIneligibilityReason,
    recovery: &LocalRecoveryStatus,
    successor: Option<String>,
) -> LocalRetryRejection {
    let (guard_ids, ownership_reason) = match recovery {
        LocalRecoveryStatus::OwnershipUnproven { guard_ids, reason } => {
            (guard_ids.clone(), Some(*reason))
        }
        LocalRecoveryStatus::Active
        | LocalRecoveryStatus::Settled
        | LocalRecoveryStatus::Abandoned => (Vec::new(), None),
    };
    LocalRetryRejection {
        run_directory,
        attempt_number,
        reason,
        guard_ids,
        ownership_reason,
        successor,
    }
}

/// The successor named by the run's `run.json`, or `None` for a run without one or one no
/// restart superseded (REQ-LOCALRUN-196).
fn run_successor(root: &OwnedFd) -> Result<Option<String>, LocalRunDirectoryError> {
    Ok(read_optional_run_record(root)?.and_then(|record| record.successor))
}

/// The `successor` member of a run record read as a status value.
fn record_value_successor(record: &Value) -> Option<String> {
    record
        .get("successor")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

#[derive(Default)]
pub(super) struct RetainedReadBudget {
    total_bytes: u64,
}

impl RetainedReadBudget {
    pub(super) fn with_bytes(total_bytes: u64) -> Result<Self, LocalRunDirectoryError> {
        (total_bytes <= MAXIMUM_RETAINED_TOTAL_BYTES)
            .then_some(Self { total_bytes })
            .ok_or(LocalRunDirectoryError::StateInvalid)
    }

    pub(super) fn account(&mut self, bytes: &[u8]) -> Result<(), LocalRunDirectoryError> {
        let size = u64::try_from(bytes.len()).map_err(|_| LocalRunDirectoryError::StateInvalid)?;
        self.account_size(size)
    }

    pub(super) fn account_size(&mut self, size: u64) -> Result<(), LocalRunDirectoryError> {
        account_retained_bytes(&mut self.total_bytes, size, MAXIMUM_RETAINED_TOTAL_BYTES)
    }
}

fn account_retained_bytes(
    total_bytes: &mut u64,
    size: u64,
    maximum_bytes: u64,
) -> Result<(), LocalRunDirectoryError> {
    *total_bytes = total_bytes
        .checked_add(size)
        .filter(|total| *total <= maximum_bytes)
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    Ok(())
}

pub(super) struct StableLocalRunSnapshot {
    pub(super) run_directory: PathBuf,
    pub(super) root: OwnedFd,
    pub(super) run: LocalRunV1,
    pub(super) state: LocalRunStateV1,
    pub(super) retained_json_bytes: u64,
    pub(super) lock_held: bool,
}

pub(super) fn read_stable_local_run_snapshot(
    requested: &Path,
) -> Result<StableLocalRunSnapshot, LocalStatusError> {
    let normalized = std::fs::canonicalize(requested).map_err(|_| LocalStatusError {
        code: LocalStatusErrorCode::RunDirectoryUnavailable,
        run_directory: None,
    })?;
    let reported_directory = normalized.to_str().map(|_| normalized.clone());
    if reported_directory.is_none() {
        return Err(LocalStatusError {
            code: LocalStatusErrorCode::RunDirectoryUnavailable,
            run_directory: None,
        });
    }
    let root = open_directory_path(&normalized).map_err(|_| LocalStatusError {
        code: LocalStatusErrorCode::RunDirectoryUnavailable,
        run_directory: reported_directory.clone(),
    })?;
    let (run, run_bytes) =
        read_run_with_size(&root).map_err(|_| invalid_status_error(&reported_directory))?;
    let lock = open_status_lock(&root, &reported_directory)?;

    for _ in 0..STATUS_SNAPSHOT_ATTEMPTS {
        let (before, _) = read_state_with_size(&root)
            .map_err(|error| status_state_error(error, &reported_directory))?;
        if validate_run_state_pair(&run, &before).is_err() {
            return Err(invalid_status_error(&reported_directory));
        }
        let lock_held = query_status_lock(&lock)
            .map_err(|_| LocalStatusError {
                code: LocalStatusErrorCode::LockQueryFailed,
                run_directory: reported_directory.clone(),
            })?
            .is_some();
        let (after, state_bytes) = read_state_with_size(&root)
            .map_err(|error| status_state_error(error, &reported_directory))?;
        if validate_run_state_pair(&run, &after).is_err() {
            return Err(invalid_status_error(&reported_directory));
        }
        verify_status_lock_identity(&root, &lock, &reported_directory)?;
        if before.revision != after.revision {
            continue;
        }
        let retained_json_bytes = run_bytes
            .checked_add(state_bytes)
            .ok_or_else(|| invalid_status_error(&reported_directory))?;
        RetainedReadBudget::with_bytes(retained_json_bytes)
            .map_err(|_| invalid_status_error(&reported_directory))?;
        return Ok(StableLocalRunSnapshot {
            run_directory: normalized,
            root,
            run,
            state: after,
            retained_json_bytes,
            lock_held,
        });
    }

    Err(LocalStatusError {
        code: LocalStatusErrorCode::StatusSnapshotUnstable,
        run_directory: reported_directory,
    })
}

pub fn read_local_run_status(requested: &Path) -> Result<LocalRunStatusSnapshot, LocalStatusError> {
    let snapshot = read_stable_local_run_snapshot(requested)?;
    let reported_directory = Some(snapshot.run_directory.clone());
    let run_value = serde_json::to_value(&snapshot.run)
        .map_err(|_| invalid_status_error(&reported_directory))?;
    // The run record (REQ-LOCALRUN-146): only a run admitted from a tree holds one.
    let record = read_optional_run_record(&snapshot.root)
        .map_err(|_| invalid_status_error(&reported_directory))?
        .map(serde_json::to_value)
        .transpose()
        .map_err(|_| invalid_status_error(&reported_directory))?;
    let wait = read_optional_wait_record(&snapshot.root)
        .map_err(|_| invalid_status_error(&reported_directory))?;
    let mut status = status_snapshot(
        snapshot.run_directory,
        run_value,
        record,
        snapshot.state,
        snapshot.lock_held,
    )
    .map_err(|_| invalid_status_error(&reported_directory))?;
    status.wait = wait;
    Ok(status)
}

fn invalid_status_error(run_directory: &Option<PathBuf>) -> LocalStatusError {
    LocalStatusError {
        code: LocalStatusErrorCode::RunDirectoryInvalid,
        run_directory: run_directory.clone(),
    }
}

fn status_state_error(
    error: LocalRunDirectoryError,
    run_directory: &Option<PathBuf>,
) -> LocalStatusError {
    let mut cause = &error;
    while let LocalRunDirectoryError::StateFile { source, .. } = cause {
        cause = source;
    }
    LocalStatusError {
        code: if *cause == LocalRunDirectoryError::RecoverySchemaUnsupported {
            LocalStatusErrorCode::RecoverySchemaUnsupported
        } else {
            LocalStatusErrorCode::RunDirectoryInvalid
        },
        run_directory: run_directory.clone(),
    }
}

fn open_status_lock(
    root: &OwnedFd,
    run_directory: &Option<PathBuf>,
) -> Result<File, LocalStatusError> {
    let metadata = statat(root, LOCK_FILE, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| invalid_status_error(run_directory))?;
    if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
        return Err(invalid_status_error(run_directory));
    }
    let lock = openat(
        root,
        LOCK_FILE,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| LocalStatusError {
        code: LocalStatusErrorCode::LockQueryFailed,
        run_directory: run_directory.clone(),
    })?;
    verify_status_lock_identity(root, &lock, run_directory)?;
    Ok(lock)
}

fn verify_status_lock_identity(
    root: &OwnedFd,
    lock: &File,
    run_directory: &Option<PathBuf>,
) -> Result<(), LocalStatusError> {
    let opened = fstat(lock).map_err(|_| invalid_status_error(run_directory))?;
    let current = statat(root, LOCK_FILE, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| invalid_status_error(run_directory))?;
    if FileType::from_raw_mode(opened.st_mode) != FileType::RegularFile
        || FileType::from_raw_mode(current.st_mode) != FileType::RegularFile
        || opened.st_dev != current.st_dev
        || opened.st_ino != current.st_ino
    {
        return Err(invalid_status_error(run_directory));
    }
    Ok(())
}

/// The lock another process holds on the run lock, if any: an `fcntl` query that takes
/// nothing. A process's own lock never blocks its query.
fn query_status_lock(lock: &File) -> Result<Option<Flock>, Errno> {
    let requested = Flock {
        start: 0,
        length: 0,
        pid: None,
        typ: FlockType::WriteLock,
        offset_type: FlockOffsetType::Set,
    };
    fcntl_getlk(lock, &requested)
}

fn status_snapshot(
    run_directory: PathBuf,
    run: Value,
    record: Option<Value>,
    state: LocalRunStateV1,
    lock_held: bool,
) -> Result<LocalRunStatusSnapshot, ()> {
    let current = state.attempts.last().ok_or(())?;
    let recovery = recovery_status(current, lock_held);
    let superseded = record.as_ref().and_then(record_value_successor).is_some();
    let retry = retry_eligibility(current.state, &recovery, lock_held, superseded);
    let current_result = status_result(&current.result);
    let attempts = state
        .attempts
        .iter()
        .map(|attempt| LocalStatusAttempt {
            attempt_number: attempt.attempt_number,
            trigger: attempt_trigger_name(attempt.trigger),
            state: attempt_state_name(attempt.state),
            result: status_result(&attempt.result),
        })
        .collect();
    let mut state_value = serde_json::to_value(&state).map_err(|_| ())?;
    for attempt in state_value
        .get_mut("attempts")
        .and_then(Value::as_array_mut)
        .ok_or(())?
    {
        attempt
            .as_object_mut()
            .ok_or(())?
            .entry("forceAbort")
            .or_insert(Value::Null);
    }
    Ok(LocalRunStatusSnapshot {
        run_directory,
        run,
        record,
        wait: None,
        state: state_value,
        current_attempt_number: current.attempt_number,
        current_attempt_state: attempt_state_name(current.state),
        current_result,
        attempts,
        recovery,
        retry,
        continuation: retry,
        status_state: LocalStatusStateView { state },
    })
}

trait LocalRecoveryAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()>;

    fn observe_process(&self, guard: &ProcessGuardV1) -> ProcessIdentityObservation;
}

struct SystemLocalRecoveryAuthority;

impl LocalRecoveryAuthority for SystemLocalRecoveryAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
        execution_host().map_err(|_| ())
    }

    fn observe_process(&self, guard: &ProcessGuardV1) -> ProcessIdentityObservation {
        process_identity_observation(guard)
    }
}

trait LocalQuiescenceAuthority: LocalRecoveryAuthority {
    fn terminate_process(&self, guard: &ProcessGuardV1) -> AuthenticatedSignalResult;

    fn wait_for_process_change(&self);
}

impl LocalQuiescenceAuthority for SystemLocalRecoveryAuthority {
    fn terminate_process(&self, guard: &ProcessGuardV1) -> AuthenticatedSignalResult {
        authenticated_process_group(guard)
            .map_or(AuthenticatedSignalResult::Unavailable, |identity| {
                terminate_authenticated_process_group(&identity)
            })
    }

    fn wait_for_process_change(&self) {
        um_support::sleep(QUIESCENCE_POLL_INTERVAL);
    }
}

fn begin_local_retry(
    pending: PendingLocalRetry,
    admitted: &AdmittedWorkflow,
    authority: &impl LocalQuiescenceAuthority,
) -> Result<LocalAttemptOwner, LocalRetryBeginError> {
    verify_retry_lock_identity(&pending.root, &pending.lock)?;
    if admitted.workflow().content_digest.algorithm.as_str()
        != pending.workflow.content_digest.algorithm.as_str()
        || admitted.workflow().content_digest.value != pending.workflow.content_digest.value
    {
        return Err(LocalRunDirectoryError::StateConflict.into());
    }
    if existing_run_overlaps_execution_root(&pending, admitted)? {
        return Err(LocalRunDirectoryError::ExecutionRootOverlap.into());
    }

    let current = lock_state(&pending.state.current)?.clone();
    let prior = current
        .attempts
        .last()
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let recovery = recovery_status_with(prior, false, authority);
    let successor = run_successor(&pending.root)?;
    if let LocalRetryEligibility::Ineligible(reason) =
        retry_eligibility(prior.state, &recovery, false, successor.is_some())
    {
        return Err(LocalRetryBeginError::Rejected(retry_rejection(
            pending.normalized.clone(),
            prior.attempt_number,
            reason,
            &recovery,
            successor,
        )));
    }

    let next_attempt_number = prior
        .attempt_number
        .checked_add(1)
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let next_attempt = retry_attempt(
        admitted,
        next_attempt_number,
        prior.attempt_number,
        pending.definition.clone(),
    )?;
    if let Err((guard_ids, ownership_reason)) = quiesce_attempt(prior, authority) {
        return Err(LocalRetryBeginError::Rejected(LocalRetryRejection {
            run_directory: pending.normalized.clone(),
            attempt_number: prior.attempt_number,
            reason: RetryIneligibilityReason::OwnershipUnproven,
            guard_ids,
            ownership_reason: Some(ownership_reason),
            successor: None,
        }));
    }
    let abandonment_snapshot = (!prior.state.is_terminal()).then(|| {
        capture_settlement_snapshot(
            Path::new(&prior.execution_root),
            WorkspaceSnapshotSettlementV1::AbandonmentRecovery,
        )
    });

    cleanup_unclaimed_attempt_directory(&pending.root, &current)?;
    let attempt_directory = create_or_verify_attempt_directory(&pending.root, next_attempt_number)?;
    pending.state.update(|state| {
        if state.current_attempt_number != prior.attempt_number {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let cause = abandoned_attempt_cause(state, prior.attempt_number);
        let current_attempt = current_attempt_mut(state)?;
        if !current_attempt.state.is_terminal() {
            for guard in &mut current_attempt.process_guards {
                guard.state = ProcessGuardStateV1::Quiesced;
            }
            let execution_may_have_started = current_attempt.started_at.is_some();
            settle_interrupted_attempt(
                current_attempt,
                cause,
                execution_may_have_started,
                abandonment_snapshot.clone(),
            )?;
        }
        state.current_attempt_number = next_attempt_number;
        state.attempts.push(next_attempt.clone());
        Ok(())
    })?;
    remove_root_wait_record(&pending.root)?;

    let attempt_directory_name =
        attempt_directory_name(next_attempt_number).ok_or(LocalRunDirectoryError::StateInvalid)?;
    let private_directory = pending.normalized.join(PRIVATE_DIRECTORY);
    let result_directory = pending
        .normalized
        .join(ATTEMPTS_DIRECTORY)
        .join(attempt_directory_name)
        .join("result");
    Ok(LocalAttemptOwner {
        normalized: pending.normalized,
        lock: Some(pending.lock),
        private_directory,
        attempt_directory,
        result_directory,
        attempt_number: next_attempt_number,
        finalizers: Arc::from(fresh_finalizer_progress(admitted)?),
        state: pending.state,
        resumed_wait: None,
    })
}

fn existing_run_overlaps_execution_root(
    pending: &PendingLocalRetry,
    admitted: &AdmittedWorkflow,
) -> Result<bool, LocalRunDirectoryError> {
    Ok(
        paths_overlap(&pending.normalized, admitted.execution().root())
            || admitted
                .execution()
                .root_identity()
                .contains_directory(&pending.root)
                .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?,
    )
}

fn retry_attempt(
    admitted: &AdmittedWorkflow,
    attempt_number: u64,
    prior_attempt_number: u64,
    definition: AttemptDefinitionV1,
) -> Result<LocalAttemptV1, LocalRunDirectoryError> {
    fresh_attempt(
        admitted,
        attempt_number,
        AttemptTriggerV1::ExplicitRetry,
        Some(prior_attempt_number),
        definition,
        timestamp(um_support::utc_now())?,
    )
}

fn fresh_attempt(
    admitted: &AdmittedWorkflow,
    attempt_number: u64,
    trigger: AttemptTriggerV1,
    prior_attempt_number: Option<u64>,
    definition: AttemptDefinitionV1,
    created_at: String,
) -> Result<LocalAttemptV1, LocalRunDirectoryError> {
    let execution_root = admitted
        .execution()
        .root()
        .to_str()
        .ok_or(LocalRunDirectoryError::InvalidPath)?
        .to_owned();
    let steps = admitted
        .workflow()
        .definition
        .presentation_order
        .iter()
        .map(|id| {
            let node = admitted
                .workflow()
                .definition
                .steps
                .get(id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            fresh_progress_node(id, AttemptNodeRoleV1::Step, node)
        })
        .collect::<Result<Vec<_>, LocalRunDirectoryError>>()?;
    Ok(LocalAttemptV1 {
        attempt_id: generate_uuid()?,
        attempt_number,
        trigger,
        prior_attempt_number,
        definition: Some(definition),
        state: AttemptStateV1::Created,
        continuation: None,
        taken_edge: None,
        imported: None,
        execution_root,
        created_at,
        started_at: None,
        settled_at: None,
        settlement_snapshot: None,
        owner: AttemptOwnerV1 {
            owner_nonce: generate_uuid()?,
            execution_host: execution_host()?,
        },
        cancellation: None,
        force_abort: None,
        interruption: None,
        rejection: None,
        progress: AttemptProgressV1 {
            accepted_occurrence_ordinal: 0,
            last_transition_sequence: 0,
            steps,
            outstanding_actions: Vec::new(),
            invocations: Vec::new(),
            accounting: DurableInvocationAccountingV1 {
                maximum_invocations: admitted
                    .capacity()
                    .resolved
                    .requirements
                    .maximum_invocations,
                observed_invocations: 0,
                settled_invocations: 0,
                input_tokens: 0,
                output_tokens: 0,
                retained_diagnostic_bytes: 0,
                discarded_diagnostic_bytes: 0,
            },
        },
        finalization: None,
        process_guards: Vec::new(),
        result: AttemptResultV1::NotPublished {
            reason: ResultAbsentReasonV1::AttemptNonterminal,
        },
    })
}

fn fresh_finalizer_progress(
    admitted: &AdmittedWorkflow,
) -> Result<Vec<AttemptStepV1>, LocalRunDirectoryError> {
    admitted
        .workflow()
        .definition
        .finalizer_presentation_order
        .iter()
        .map(|id| {
            let node = &admitted
                .workflow()
                .definition
                .finalizers
                .get(id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?
                .body;
            fresh_progress_node(id, AttemptNodeRoleV1::Finalizer, node)
        })
        .collect()
}

fn fresh_progress_node(
    id: &str,
    role: AttemptNodeRoleV1,
    node: &super::validated::ValidatedStep,
) -> Result<AttemptStepV1, LocalRunDirectoryError> {
    let failure_policy = node.common().failure_policy;
    Ok(AttemptStepV1 {
        id: id.to_owned(),
        role,
        failure_policy,
        state: AttemptStepStateV1::Pending,
        outputs: Some(Vec::new()),
        detail: None,
        diagnostic: None,
        started_at: None,
        recovery: None,
    })
}

// Continue must inspect the entire retained history, not just the latest attempt.
// No signal is issued until every same-host identity has been classified.
fn quiesce_run(
    state: &LocalRunStateV1,
    authority: &impl LocalQuiescenceAuthority,
) -> Result<super::publication::ContinuationQuiescenceV1, (Vec<String>, OwnershipUnprovenReason)> {
    let guards = state
        .attempts
        .iter()
        .flat_map(|attempt| &attempt.process_guards)
        .collect::<Vec<_>>();
    let recorded =
        u64::try_from(guards.len()).map_err(|_| process_inspection_unproven(Vec::new()))?;
    if guards.is_empty() {
        let proven_at = timestamp(um_support::utc_now())
            .map_err(|_| process_inspection_unproven(Vec::new()))?;
        return Ok(super::publication::ContinuationQuiescenceV1 {
            groups_recorded: 0,
            groups_terminated: 0,
            groups_absent: 0,
            proven_at,
        });
    }
    let current_host = quiescence_host(&guards, authority)?;
    let mut absent = 0_u64;
    let mut exact = Vec::new();
    let mut unproven = Vec::new();
    for guard in guards {
        if guard.execution_host != current_host {
            absent += 1;
        } else {
            match authority.observe_process(guard) {
                ProcessIdentityObservation::Exact { .. } => exact.push(guard),
                ProcessIdentityObservation::Absent => absent += 1,
                ProcessIdentityObservation::Unavailable => unproven.push(guard.guard_id.clone()),
            }
        }
    }
    if !unproven.is_empty() {
        return Err(process_inspection_unproven(unproven));
    }
    let mut terminated = Vec::new();
    for guard in exact {
        match authority.terminate_process(guard) {
            AuthenticatedSignalResult::Signalled => terminated.push(guard),
            AuthenticatedSignalResult::Absent => absent += 1,
            AuthenticatedSignalResult::Unavailable => unproven.push(guard.guard_id.clone()),
        }
    }
    if !unproven.is_empty() {
        return Err(process_inspection_unproven(unproven));
    }
    prove_groups_absent(&terminated, authority)?;
    let proven_at =
        timestamp(um_support::utc_now()).map_err(|_| process_inspection_unproven(Vec::new()))?;
    Ok(super::publication::ContinuationQuiescenceV1 {
        groups_recorded: recorded,
        groups_terminated: u64::try_from(terminated.len())
            .map_err(|_| process_inspection_unproven(Vec::new()))?,
        groups_absent: absent,
        proven_at,
    })
}

fn quiesce_attempt(
    attempt: &LocalAttemptV1,
    authority: &impl LocalQuiescenceAuthority,
) -> Result<(), (Vec<String>, OwnershipUnprovenReason)> {
    let guards = attempt
        .process_guards
        .iter()
        .filter(|guard| !matches!(guard.state, ProcessGuardStateV1::Quiesced))
        .collect::<Vec<_>>();
    if guards.is_empty() {
        return Ok(());
    }
    let current_host = quiescence_host(&guards, authority)?;
    let matching = guards
        .iter()
        .copied()
        .filter(|guard| guard.execution_host == current_host)
        .collect::<Vec<_>>();
    let mut exact = Vec::new();
    let mut unproven = Vec::new();
    for guard in matching {
        match authority.observe_process(guard) {
            ProcessIdentityObservation::Exact { .. } => exact.push(guard),
            ProcessIdentityObservation::Absent => {}
            ProcessIdentityObservation::Unavailable => unproven.push(guard.guard_id.clone()),
        }
    }
    if !unproven.is_empty() {
        return Err(process_inspection_unproven(unproven));
    }
    for guard in &exact {
        match authority.terminate_process(guard) {
            AuthenticatedSignalResult::Signalled | AuthenticatedSignalResult::Absent => {}
            AuthenticatedSignalResult::Unavailable => unproven.push(guard.guard_id.clone()),
        }
    }
    if !unproven.is_empty() {
        return Err(process_inspection_unproven(unproven));
    }
    prove_groups_absent(&exact, authority)
}

fn quiescence_host(
    guards: &[&ProcessGuardV1],
    authority: &impl LocalQuiescenceAuthority,
) -> Result<ExecutionHostV1, (Vec<String>, OwnershipUnprovenReason)> {
    authority.execution_host().map_err(|()| {
        (
            guards.iter().map(|guard| guard.guard_id.clone()).collect(),
            OwnershipUnprovenReason::ExecutionHostIdentityUnavailable,
        )
    })
}

fn prove_groups_absent(
    guards: &[&ProcessGuardV1],
    authority: &impl LocalQuiescenceAuthority,
) -> Result<(), (Vec<String>, OwnershipUnprovenReason)> {
    for _ in 0..QUIESCENCE_POLL_ATTEMPTS {
        let mut surviving = Vec::new();
        let mut unavailable = Vec::new();
        for guard in guards {
            match authority.observe_process(guard) {
                ProcessIdentityObservation::Absent => {}
                ProcessIdentityObservation::Exact { .. } => surviving.push(guard.guard_id.clone()),
                ProcessIdentityObservation::Unavailable => unavailable.push(guard.guard_id.clone()),
            }
        }
        if !unavailable.is_empty() {
            return Err(process_inspection_unproven(unavailable));
        }
        if surviving.is_empty() {
            return Ok(());
        }
        authority.wait_for_process_change();
    }
    Err(process_inspection_unproven(
        guards.iter().map(|guard| guard.guard_id.clone()).collect(),
    ))
}

fn process_inspection_unproven(guard_ids: Vec<String>) -> (Vec<String>, OwnershipUnprovenReason) {
    (
        guard_ids,
        OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
    )
}

fn cleanup_unclaimed_attempt_directory(
    root: &OwnedFd,
    state: &LocalRunStateV1,
) -> Result<(), LocalRunDirectoryError> {
    let next = state
        .current_attempt_number
        .checked_add(1)
        .ok_or(LocalRunDirectoryError::StateInvalid)?;
    let name = attempt_directory_name(next).ok_or(LocalRunDirectoryError::StateInvalid)?;
    let attempts = open_directory_at(root, ATTEMPTS_DIRECTORY)?;
    match openat(
        &attempts,
        &name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(directory) => {
            crate::owned_tree::remove_open_tree_at(&attempts, &name, &directory)
                .map_err(|_| LocalRunDirectoryError::StateInvalid)?;
            sync_directory(&attempts)?;
        }
        Err(Errno::NOENT) => {}
        Err(_) => return Err(LocalRunDirectoryError::StateInvalid),
    }
    Ok(())
}

fn create_or_verify_attempt_directory(
    root: &OwnedFd,
    attempt_number: u64,
) -> Result<OwnedFd, LocalRunDirectoryError> {
    let attempts = open_directory_at(root, ATTEMPTS_DIRECTORY)?;
    let name =
        attempt_directory_name(attempt_number).ok_or(LocalRunDirectoryError::StateInvalid)?;
    match mkdirat(&attempts, &name, Mode::RWXU) {
        Ok(()) => {
            sync_directory(&attempts)?;
            open_directory_at(&attempts, &name)
        }
        Err(Errno::EXIST) => {
            let attempt = open_directory_at(&attempts, &name)?;
            if directory_entries(&attempt)?.is_empty() {
                Ok(attempt)
            } else {
                Err(LocalRunDirectoryError::StateConflict)
            }
        }
        Err(_) => Err(LocalRunDirectoryError::StagingUnavailable),
    }
}

fn recovery_status(attempt: &LocalAttemptV1, lock_held: bool) -> LocalRecoveryStatus {
    recovery_status_with(attempt, lock_held, &SystemLocalRecoveryAuthority)
}

fn recovery_status_with(
    attempt: &LocalAttemptV1,
    lock_held: bool,
    authority: &impl LocalRecoveryAuthority,
) -> LocalRecoveryStatus {
    if lock_held {
        return LocalRecoveryStatus::Active;
    }
    if attempt.state.is_terminal() {
        return LocalRecoveryStatus::Settled;
    }
    let guards = attempt
        .process_guards
        .iter()
        .filter(|guard| !matches!(guard.state, ProcessGuardStateV1::Quiesced))
        .collect::<Vec<_>>();
    if guards.is_empty() {
        return LocalRecoveryStatus::Abandoned;
    }
    let current_host = match authority.execution_host() {
        Ok(host) => host,
        Err(()) => {
            return LocalRecoveryStatus::OwnershipUnproven {
                guard_ids: guards.iter().map(|guard| guard.guard_id.clone()).collect(),
                reason: OwnershipUnprovenReason::ExecutionHostIdentityUnavailable,
            };
        }
    };
    let guard_ids = guards
        .iter()
        .filter(|guard| guard.execution_host == current_host)
        .filter(|guard| {
            matches!(
                authority.observe_process(guard),
                ProcessIdentityObservation::Unavailable
            )
        })
        .map(|guard| guard.guard_id.clone())
        .collect::<Vec<_>>();
    if guard_ids.is_empty() {
        LocalRecoveryStatus::Abandoned
    } else {
        LocalRecoveryStatus::OwnershipUnproven {
            guard_ids,
            reason: OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
        }
    }
}

/// Retry and continuation eligibility in its closed precedence order: the lock, unproven
/// ownership, a superseded run record (`superseded`: the record carries `successor`, whose
/// last attempt may still read `waiting`, REQ-LOCALRUN-196), then the last attempt's state.
fn retry_eligibility(
    state: AttemptStateV1,
    recovery: &LocalRecoveryStatus,
    lock_held: bool,
    superseded: bool,
) -> LocalRetryEligibility {
    let reason = if lock_held {
        Some(RetryIneligibilityReason::RunLocked)
    } else if matches!(recovery, LocalRecoveryStatus::OwnershipUnproven { .. }) {
        Some(RetryIneligibilityReason::OwnershipUnproven)
    } else if superseded {
        Some(RetryIneligibilityReason::RunSuperseded)
    } else if matches!(state, AttemptStateV1::Succeeded) {
        Some(RetryIneligibilityReason::LatestAttemptSucceeded)
    } else if matches!(state, AttemptStateV1::Rejected) {
        Some(RetryIneligibilityReason::LatestAttemptRejected)
    } else {
        None
    };
    reason.map_or(
        LocalRetryEligibility::Eligible,
        LocalRetryEligibility::Ineligible,
    )
}

fn status_result(result: &AttemptResultV1) -> LocalStatusResult {
    match result {
        AttemptResultV1::NotPublished { reason } => LocalStatusResult::NotPublished {
            reason: result_absent_reason_name(*reason),
        },
        AttemptResultV1::Published { relative_directory } => LocalStatusResult::Published {
            relative_directory: relative_directory.clone(),
        },
        AttemptResultV1::PublicationFailed { phase, .. } => LocalStatusResult::PublicationFailed {
            phase: publication_failure_phase_name(*phase),
        },
    }
}

pub(super) const fn attempt_trigger_name(trigger: AttemptTriggerV1) -> &'static str {
    match trigger {
        AttemptTriggerV1::Initial => "initial",
        AttemptTriggerV1::ExplicitRetry => "explicit_retry",
        AttemptTriggerV1::Continuation => "continuation",
    }
}

const fn attempt_state_name(state: AttemptStateV1) -> &'static str {
    match state {
        AttemptStateV1::Created => "created",
        AttemptStateV1::Running => "running",
        AttemptStateV1::Cancelling => "cancelling",
        AttemptStateV1::Succeeded => "succeeded",
        AttemptStateV1::WorkflowFailed => "workflow_failed",
        AttemptStateV1::Cancelled => "cancelled",
        AttemptStateV1::Interrupted => "interrupted",
        AttemptStateV1::Rejected => "rejected",
        AttemptStateV1::Waiting => "waiting",
        AttemptStateV1::Continued => "continued",
    }
}

const fn result_absent_reason_name(reason: ResultAbsentReasonV1) -> &'static str {
    match reason {
        ResultAbsentReasonV1::AttemptNonterminal => "attempt_nonterminal",
        ResultAbsentReasonV1::PublicationPending => "publication_pending",
        ResultAbsentReasonV1::Interrupted => "interrupted",
        ResultAbsentReasonV1::Rejected => "rejected",
    }
}

const fn publication_failure_phase_name(phase: PublicationFailurePhaseV1) -> &'static str {
    match phase {
        PublicationFailurePhaseV1::ExportCopy => "export_copy",
        PublicationFailurePhaseV1::Serialization => "serialization",
        PublicationFailurePhaseV1::Close => "close",
        PublicationFailurePhaseV1::Verification => "verification",
        PublicationFailurePhaseV1::Rename => "rename",
    }
}

fn process_identity_observation(guard: &ProcessGuardV1) -> ProcessIdentityObservation {
    authenticated_process_group(guard).map_or(ProcessIdentityObservation::Unavailable, |identity| {
        system_process_identity_observation(&identity)
    })
}

fn authenticated_process_group(guard: &ProcessGuardV1) -> Option<AuthenticatedProcessGroup> {
    if !matches!(
        guard.liveness.kind,
        ProcessLivenessKindV1::LeaderStartIdentity
    ) {
        return None;
    }
    let process_group = i32::try_from(guard.process_group_id)
        .ok()
        .and_then(rustix::process::Pid::from_raw)?;
    AuthenticatedProcessGroup::new(process_group, guard.liveness.value.clone())
}

fn decode_run(bytes: &[u8]) -> Result<LocalRunV1, LocalRunDirectoryError> {
    let run: LocalRunV1 = decode_schema_one(bytes)?;
    validate_run(&run)?;
    Ok(run)
}

fn decode_state(bytes: &[u8]) -> Result<LocalRunStateV1, LocalRunDirectoryError> {
    let document = decode_schema_one_value(bytes)?;
    dispatch_durable_recovery_versions(&document)?;
    let state =
        serde_json::from_value(document).map_err(|source| LocalRunDirectoryError::Json {
            operation: "decode state",
            source,
        })?;
    validate_state(&state)?;
    Ok(state)
}

fn dispatch_durable_recovery_versions(document: &Value) -> Result<(), LocalRunDirectoryError> {
    let Some(attempts) = document.get("attempts").and_then(Value::as_array) else {
        return Ok(());
    };
    for attempt in attempts {
        let ordinary = attempt
            .get("progress")
            .and_then(|progress| progress.get("steps"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten();
        let finalizers = attempt
            .get("finalization")
            .and_then(|finalization| finalization.get("finalizers"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten();
        for recovery in ordinary
            .chain(finalizers)
            .filter_map(|step| step.get("recovery"))
        {
            if recovery
                .get("schemaVersion")
                .is_some_and(|version| version.as_u64() != Some(1))
            {
                return Err(LocalRunDirectoryError::RecoverySchemaUnsupported);
            }
        }
    }
    Ok(())
}

fn decode_schema_one<Document>(bytes: &[u8]) -> Result<Document, LocalRunDirectoryError>
where
    Document: for<'de> Deserialize<'de>,
{
    serde_json::from_value(decode_schema_one_value(bytes)?).map_err(|source| {
        LocalRunDirectoryError::Json {
            operation: "decode document",
            source,
        }
    })
}

fn decode_schema_one_value(bytes: &[u8]) -> Result<Value, LocalRunDirectoryError> {
    if bytes.starts_with(&[0xef, 0xbb, 0xbf]) || !bytes.ends_with(b"\n") {
        return Err(LocalRunDirectoryError::DocumentFramingInvalid);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = DuplicateFreeValue::deserialize(&mut deserializer)
        .and_then(|value| deserializer.end().map(|()| value.0))
        .map_err(|source| LocalRunDirectoryError::Json {
            operation: "decode document",
            source,
        })?;
    if contains_null(&value) {
        return Err(LocalRunDirectoryError::DocumentNullInvalid);
    }
    if value
        .get("schemaVersion")
        .and_then(Value::as_u64)
        .is_none_or(|version| version != 1)
    {
        return Err(LocalRunDirectoryError::StateSchemaInvalid);
    }
    Ok(value)
}

struct DuplicateFreeValue(Value);

impl<'de> Deserialize<'de> for DuplicateFreeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(DuplicateFreeValueVisitor)
    }
}

struct DuplicateFreeValueVisitor;

impl<'de> Visitor<'de> for DuplicateFreeValueVisitor {
    type Value = DuplicateFreeValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a duplicate-free JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(DuplicateFreeValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(DuplicateFreeValue(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(DuplicateFreeValue(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(DuplicateFreeValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(DuplicateFreeValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(DuplicateFreeValue(Value::String(value)))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(DuplicateFreeValue(Value::Null))
    }

    fn visit_seq<A>(self, mut values: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut sequence = Vec::new();
        while let Some(value) = values.next_element::<DuplicateFreeValue>()? {
            sequence.push(value.0);
        }
        Ok(DuplicateFreeValue(Value::Array(sequence)))
    }

    fn visit_map<A>(self, mut values: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = serde_json::Map::new();
        while let Some(key) = values.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON object member"));
            }
            let value = values.next_value::<DuplicateFreeValue>()?;
            object.insert(key, value.0);
        }
        Ok(DuplicateFreeValue(Value::Object(object)))
    }
}

fn contains_null(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Array(values) => values.iter().any(contains_null),
        Value::Object(properties) => properties.values().any(contains_null),
        Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

fn validate_run(run: &LocalRunV1) -> Result<(), LocalRunDirectoryError> {
    if run.schema_version != 1
        || !is_canonical_uuid(&run.local_run_id)
        || !valid_timestamp(&run.created_at)
        || !run.workflow_digest.validate()
        || !run.workflow_manifest_digest.validate()
        || run
            .git_baseline
            .as_ref()
            .is_some_and(|baseline| match baseline {
                GitBaselineV1::Available {
                    object_format,
                    commit_oid,
                } => object_format != "sha1" || !is_lowercase_hex(commit_oid, 40),
                GitBaselineV1::Unavailable { .. } => false,
            })
    {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    Ok(())
}

fn local_git_baseline(baseline: &GitBaselineV1) -> Option<LocalGitBaseline> {
    let GitBaselineV1::Available {
        object_format,
        commit_oid,
    } = baseline
    else {
        return None;
    };
    let format = match object_format.as_str() {
        "sha1" => GitObjectFormat::Sha1,
        _ => return None,
    };
    LocalGitBaseline::new(format, Arc::from(commit_oid.as_str()))
}

fn validate_run_state_pair(
    run: &LocalRunV1,
    state: &LocalRunStateV1,
) -> Result<(), LocalRunDirectoryError> {
    if state.local_run_id != run.local_run_id {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    // An imported attempt's definition is re-located to the attempt that holds the
    // predecessor's copy (REQ-LOCALRUN-195), so it is not the run's own.
    for attempt in state
        .attempts
        .iter()
        .filter(|attempt| attempt.imported.is_none())
    {
        let Some(definition) = &attempt.definition else {
            continue;
        };
        if matches!(
            attempt.trigger,
            AttemptTriggerV1::Initial | AttemptTriggerV1::ExplicitRetry
        ) && (definition.digest != run.workflow_digest
            || definition.manifest_digest != run.workflow_manifest_digest
            || definition.locator != AttemptDefinitionLocatorV1::Run)
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
    }
    Ok(())
}

fn validate_manifest(manifest: &WorkflowManifestV1) -> Result<(), LocalRunDirectoryError> {
    if manifest.schema_version != 1
        || !is_canonical_relative_path(&manifest.workflow_path)
        || !is_canonical_absolute_path(&manifest.source_root)
        || !(1..=256).contains(&manifest.maximum_parallel_steps)
        || manifest.source_files.is_empty()
        || manifest.inputs.len() > 256
        || manifest
            .inputs
            .keys()
            .any(|name| !super::is_input_name(name))
    {
        return Err(LocalRunDirectoryError::SerializationUnavailable);
    }
    let mut source_paths = BTreeSet::new();
    let mut previous_source_path: Option<&[u8]> = None;
    for source in &manifest.source_files {
        if !is_canonical_relative_path(&source.path) {
            return Err(LocalRunDirectoryError::SerializationUnavailable);
        }
        if !source_paths.insert(source.path.as_str()) {
            return Err(LocalRunDirectoryError::SerializationUnavailable);
        }
        if previous_source_path.is_some_and(|path| path >= source.path.as_bytes()) {
            return Err(LocalRunDirectoryError::SerializationUnavailable);
        }
        previous_source_path = Some(source.path.as_bytes());
    }
    if !source_paths.contains(manifest.workflow_path.as_str()) {
        return Err(LocalRunDirectoryError::SerializationUnavailable);
    }
    let mut retained_files = manifest
        .source_files
        .iter()
        .map(|source| &source.file)
        .collect::<Vec<_>>();
    let mut input_bytes = 0_u64;
    let mut attachment_count = 0_usize;
    for input in manifest.inputs.values() {
        match input {
            ManifestInputV1::Text { file } | ManifestInputV1::Json { file } => {
                if file.size_bytes > MAXIMUM_RETAINED_TEXT_BYTES {
                    return Err(LocalRunDirectoryError::SerializationUnavailable);
                }
                account_retained_bytes(
                    &mut input_bytes,
                    file.size_bytes,
                    MAXIMUM_RETAINED_INPUT_BYTES,
                )?;
                retained_files.push(file);
            }
            ManifestInputV1::File { media_type, file } => {
                if file.size_bytes > MAXIMUM_RETAINED_FILE_BYTES
                    || !super::is_valid_media_type(media_type)
                {
                    return Err(LocalRunDirectoryError::SerializationUnavailable);
                }
                account_retained_bytes(
                    &mut input_bytes,
                    file.size_bytes,
                    MAXIMUM_RETAINED_INPUT_BYTES,
                )?;
                retained_files.push(file);
            }
            ManifestInputV1::Attachments { items } => {
                attachment_count = attachment_count
                    .checked_add(items.len())
                    .filter(|count| *count <= 256)
                    .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
                for attachment in items {
                    if attachment.file.size_bytes > MAXIMUM_RETAINED_FILE_BYTES
                        || !super::is_valid_media_type(&attachment.media_type)
                    {
                        return Err(LocalRunDirectoryError::SerializationUnavailable);
                    }
                    account_retained_bytes(
                        &mut input_bytes,
                        attachment.file.size_bytes,
                        MAXIMUM_RETAINED_INPUT_BYTES,
                    )?;
                    retained_files.push(&attachment.file);
                }
            }
        }
    }
    if let Some(file) = &manifest.agent_profile_replacement {
        if file.size_bytes > MAXIMUM_AGENT_PROFILE_REPLACEMENT_BYTES {
            return Err(LocalRunDirectoryError::SerializationUnavailable);
        }
        retained_files.push(file);
    }
    if let Some(tree) = &manifest.tree {
        validate_manifest_tree(tree)?;
    }
    let mut expected_ordinal = 1_u64;
    for file in retained_files {
        if file.ordinal != expected_ordinal
            || file.relative_file
                != format!(
                    "{WORKFLOW_FILES_DIRECTORY}/{}",
                    retained_file_name(expected_ordinal)?
                )
            || !file.digest.validate()
        {
            return Err(LocalRunDirectoryError::SerializationUnavailable);
        }
        expected_ordinal = expected_ordinal
            .checked_add(1)
            .ok_or(LocalRunDirectoryError::SerializationUnavailable)?;
    }
    Ok(())
}

fn validate_manifest_tree(tree: &ManifestTreeV1) -> Result<(), LocalRunDirectoryError> {
    if tree.relative_directory != WORKFLOW_TREE_DIRECTORY
        || !tree.digest.validate()
        || tree
            .programs
            .iter()
            .any(|program| !is_canonical_relative_path(program.trim_end_matches('/')))
        || tree.required_tools.iter().any(String::is_empty)
    {
        return Err(LocalRunDirectoryError::SerializationUnavailable);
    }
    Ok(())
}

fn validate_state(state: &LocalRunStateV1) -> Result<(), LocalRunDirectoryError> {
    if state.schema_version != 1 {
        return Err(LocalRunDirectoryError::StateSchemaInvalid);
    }
    if !is_canonical_uuid(&state.local_run_id) {
        return Err(LocalRunDirectoryError::StateIdentityInvalid);
    }
    if state.revision == 0 {
        return Err(LocalRunDirectoryError::StateRevisionInvalid);
    }
    if state.current_attempt_number == 0 {
        return Err(LocalRunDirectoryError::StateCurrentAttemptInvalid);
    }
    if state.attempts.is_empty() {
        return Err(LocalRunDirectoryError::StateAttemptsEmpty);
    }
    if state.attempts.last().map(|attempt| attempt.attempt_number)
        != Some(state.current_attempt_number)
    {
        return Err(LocalRunDirectoryError::StateAttemptIndexInvalid);
    }
    if state.diagnostics.len() > MAXIMUM_DIAGNOSTICS {
        return Err(LocalRunDirectoryError::StateDiagnosticsLimitInvalid);
    }
    let state_index = LocalRunStateIndex::new(state)?;
    for (index, attempt) in state.attempts.iter().enumerate() {
        let number = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        validate_attempt(&state_index, attempt, number)?;
    }
    let mut prior_sequence = 0;
    for diagnostic in &state.diagnostics {
        if diagnostic.sequence == 0
            || diagnostic.sequence <= prior_sequence
            || diagnostic.attempt_number == 0
            || diagnostic.attempt_number > state.current_attempt_number
            || diagnostic.action_id == Some(0)
            || diagnostic
                .guard_id
                .as_deref()
                .is_some_and(|guard| !is_canonical_uuid(guard))
            || diagnostic.os_error.is_some_and(|number| number < 1)
            || ((diagnostic.cause.is_some() || diagnostic.os_error.is_some())
                && diagnostic.code != DiagnosticCodeV1::StatePersistenceFailure)
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
        prior_sequence = diagnostic.sequence;
    }
    Ok(())
}

fn validate_attempt_identity(
    attempt: &LocalAttemptV1,
    expected_number: u64,
) -> Result<(), LocalRunDirectoryError> {
    if attempt.attempt_number != expected_number {
        return Err(LocalRunDirectoryError::AttemptNumberInvalid);
    }
    let valid_trigger = match attempt.trigger {
        AttemptTriggerV1::Initial => expected_number == 1 && attempt.prior_attempt_number.is_none(),
        AttemptTriggerV1::ExplicitRetry | AttemptTriggerV1::Continuation => {
            attempt.prior_attempt_number == Some(expected_number - 1)
        }
    };
    if !valid_trigger {
        return Err(LocalRunDirectoryError::AttemptTriggerInvalid);
    }
    if !is_canonical_uuid(&attempt.attempt_id)
        || attempt
            .imported
            .as_ref()
            .is_some_and(|imported| !is_run_id(&imported.run))
    {
        return Err(LocalRunDirectoryError::AttemptIdentityInvalid);
    }
    if !is_canonical_absolute_path(&attempt.execution_root) {
        return Err(LocalRunDirectoryError::AttemptExecutionRootInvalid);
    }
    Ok(())
}

fn validate_attempt_timeline(attempt: &LocalAttemptV1) -> Result<(), LocalRunDirectoryError> {
    if !valid_timestamp(&attempt.created_at) {
        return Err(LocalRunDirectoryError::AttemptCreatedAtInvalid);
    }
    if attempt
        .started_at
        .as_deref()
        .is_some_and(|value| !valid_timestamp(value))
    {
        return Err(LocalRunDirectoryError::AttemptStartedAtInvalid);
    }
    if attempt
        .settled_at
        .as_deref()
        .is_some_and(|value| !valid_timestamp(value))
    {
        return Err(LocalRunDirectoryError::AttemptSettledAtInvalid);
    }
    if attempt.state.is_terminal() != attempt.settled_at.is_some() {
        return Err(LocalRunDirectoryError::AttemptSettlementInvalid);
    }
    Ok(())
}

fn validate_attempt_definition(attempt: &LocalAttemptV1) -> Result<(), LocalRunDirectoryError> {
    if attempt.definition.as_ref().is_some_and(|definition| {
        !definition.digest.validate()
            || !definition.manifest_digest.validate()
            || match definition.locator {
                AttemptDefinitionLocatorV1::Run => false,
                AttemptDefinitionLocatorV1::Attempt { attempt_number } => {
                    attempt_number == 0 || attempt_number > attempt.attempt_number
                }
                AttemptDefinitionLocatorV1::PriorAttempt { attempt_number } => {
                    attempt_number == 0 || attempt_number >= attempt.attempt_number
                }
            }
    }) {
        return Err(LocalRunDirectoryError::AttemptDefinitionInvalid);
    }
    if attempt
        .settlement_snapshot
        .as_ref()
        .is_some_and(|snapshot| !attempt.state.is_terminal() || !snapshot.validate(true))
        || (attempt.definition.is_some()
            && attempt.state.is_terminal()
            && attempt.settlement_snapshot.is_none())
    {
        return Err(LocalRunDirectoryError::AttemptSnapshotInvalid);
    }
    if !validate_owner(&attempt.owner) {
        return Err(LocalRunDirectoryError::AttemptOwnerInvalid);
    }
    if attempt.progress.steps.is_empty() {
        return Err(LocalRunDirectoryError::AttemptStepsEmpty);
    }
    Ok(())
}

fn validate_attempt_start(attempt: &LocalAttemptV1) -> Result<(), LocalRunDirectoryError> {
    if matches!(attempt.state, AttemptStateV1::Created) && attempt.started_at.is_some() {
        return Err(LocalRunDirectoryError::AttemptStartInvalid);
    }
    if !matches!(
        attempt.state,
        AttemptStateV1::Created | AttemptStateV1::Rejected
    ) && attempt.started_at.is_none()
        && !matches!(
            attempt.interruption,
            Some(AttemptInterruptionV1 {
                execution_may_have_started: false,
                ..
            })
        )
    {
        return Err(LocalRunDirectoryError::AttemptStartInvalid);
    }
    Ok(())
}

fn validate_attempt(
    state_index: &LocalRunStateIndex<'_>,
    attempt: &LocalAttemptV1,
    expected_number: u64,
) -> Result<(), LocalRunDirectoryError> {
    validate_attempt_identity(attempt, expected_number)?;
    validate_attempt_timeline(attempt)?;
    validate_attempt_definition(attempt)?;
    validate_attempt_start(attempt)?;
    validate_attempt_cancellation(attempt)?;
    validate_attempt_outcome(attempt)?;
    let mut step_ids = BTreeSet::new();
    for step in &attempt.progress.steps {
        validate_attempt_step(attempt, step, &mut step_ids)?;
    }
    validate_attempt_continuation(state_index, attempt)?;
    validate_attempt_finalization(attempt, &mut step_ids)?;
    validate_attempt_recovery(attempt, &step_ids)?;
    validate_attempt_actions(attempt)?;
    validate_attempt_guards(attempt, &step_ids)?;
    validate_attempt_result(attempt)
}

fn validate_attempt_step<'a>(
    attempt: &LocalAttemptV1,
    step: &'a AttemptStepV1,
    step_ids: &mut BTreeSet<&'a str>,
) -> Result<(), LocalRunDirectoryError> {
    if step.id.is_empty() {
        return Err(LocalRunDirectoryError::AttemptStepIdInvalid);
    }
    if step.role != AttemptNodeRoleV1::Step {
        return Err(LocalRunDirectoryError::AttemptStepRoleInvalid);
    }
    if !step_ids.insert(step.id.as_str()) {
        return Err(LocalRunDirectoryError::AttemptStepDuplicate);
    }
    if !attempt_step_detail_valid(step.role, step.state, step.detail.as_ref())
        || !attempt_step_diagnostic_valid(step.state, step.diagnostic.as_ref())
        || !attempt_step_started_at_valid(step.state, step.started_at.as_deref())
    {
        return Err(LocalRunDirectoryError::AttemptStepDetailInvalid);
    }
    if !retained_output_set_valid(
        step.role,
        &step.id,
        step.state,
        step.outputs.as_deref(),
        attempt.definition.is_some(),
    ) {
        return Err(LocalRunDirectoryError::AttemptStepOutputsInvalid);
    }
    if step.state == AttemptStepStateV1::Inherited && step.recovery.is_some() {
        return Err(LocalRunDirectoryError::AttemptStepRecoveryInvalid);
    }
    let ordinary_cancellation = attempt
        .cancellation
        .as_ref()
        .map(|cancellation| cancellation.reason);
    let first_force_abort_phase = attempt
        .force_abort
        .map(|force_abort| force_abort.phase.into());
    if step.detail.as_ref().is_some_and(|detail| {
        let NodeDetail::Cancellation(detail) = detail else {
            return false;
        };
        !ordinary_node_cancellation_matches(
            cancellation_reason(detail.code),
            ordinary_cancellation,
            CancellationReasonV1::ForceAbort,
            first_force_abort_phase,
        )
    }) {
        return Err(LocalRunDirectoryError::AttemptStepCancellationInvalid);
    }
    Ok(())
}

fn validate_attempt_cancellation(attempt: &LocalAttemptV1) -> Result<(), LocalRunDirectoryError> {
    let finalization_cancelled =
        attempt
            .finalization
            .as_ref()
            .is_some_and(|finalization| match finalization {
                AttemptFinalizationV1::Progress(progress) => progress.cancellation.is_some(),
                AttemptFinalizationV1::Complete(complete) => complete.cancellation.is_some(),
            });
    if attempt.cancellation.as_ref().is_some_and(|cancellation| {
        cancellation.reason == CancellationReasonV1::ForceAbort
            || !valid_timestamp(&cancellation.requested_at)
            || !valid_timestamp(&cancellation.force_stop_deadline)
    }) {
        return Err(LocalRunDirectoryError::AttemptCancellationRecordInvalid);
    }
    if attempt.force_abort.is_some_and(|force_abort| {
        force_abort.reason != CancellationReason::ForceAbort
            || matches!(
                attempt.state,
                AttemptStateV1::Created | AttemptStateV1::Rejected | AttemptStateV1::Succeeded
            )
            || (force_abort.phase == super::runtime::RunCancellationPhase::Finalization
                && attempt.finalization.is_none())
    }) {
        return Err(LocalRunDirectoryError::AttemptForceAbortInvalid);
    }
    if matches!(
        attempt.state,
        AttemptStateV1::Cancelling | AttemptStateV1::Cancelled
    ) && attempt.cancellation.is_none()
        && attempt.force_abort.is_none()
        && !finalization_cancelled
    {
        return Err(LocalRunDirectoryError::AttemptCancellationMissing);
    }
    if attempt.cancellation.as_ref().is_some_and(|cancellation| {
        cancellation.workflow_confirmed != matches!(attempt.state, AttemptStateV1::Cancelled)
    }) {
        return Err(LocalRunDirectoryError::AttemptCancellationConfirmationInvalid);
    }
    if attempt.interruption.as_ref().is_some_and(|interruption| {
        interruption.cancellation_requested
            != (attempt.cancellation.is_some()
                || attempt.force_abort.is_some()
                || finalization_cancelled)
    }) {
        return Err(LocalRunDirectoryError::AttemptInterruptionCancellationInvalid);
    }
    Ok(())
}

fn validate_attempt_outcome(attempt: &LocalAttemptV1) -> Result<(), LocalRunDirectoryError> {
    if matches!(attempt.state, AttemptStateV1::Interrupted) != attempt.interruption.is_some() {
        return Err(LocalRunDirectoryError::AttemptInterruptionInvalid);
    }
    if matches!(attempt.state, AttemptStateV1::Rejected) != attempt.rejection.is_some() {
        return Err(LocalRunDirectoryError::AttemptRejectionInvalid);
    }
    Ok(())
}

fn validate_attempt_actions(attempt: &LocalAttemptV1) -> Result<(), LocalRunDirectoryError> {
    let mut action_ids = BTreeSet::new();
    let mut prior_action_id = 0;
    for action in &attempt.progress.outstanding_actions {
        let requires_step = !matches!(action.kind, OutstandingActionKindV1::FinishRun);
        if action.action_id == 0
            || action.action_id <= prior_action_id
            || !action_ids.insert(action.action_id)
        {
            return Err(LocalRunDirectoryError::AttemptActionIdInvalid);
        }
        if requires_step != action.step_id.is_some() || requires_step != action.node_role.is_some()
        {
            return Err(LocalRunDirectoryError::AttemptActionTargetInvalid);
        }
        if action
            .step_id
            .as_deref()
            .is_some_and(|id| attempt_node_role(attempt, id) != action.node_role)
        {
            return Err(LocalRunDirectoryError::AttemptActionNodeInvalid);
        }
        if (requires_step && action.target_execution.is_some() == action.recovery_round.is_some())
            || (!requires_step
                && (action.target_execution.is_some() || action.recovery_round.is_some()))
            || (matches!(action.kind, OutstandingActionKindV1::StartRecoveryHandler)
                && action.recovery_round.is_none())
            || (matches!(action.kind, OutstandingActionKindV1::StartStep)
                && action.target_execution.is_none())
        {
            return Err(LocalRunDirectoryError::AttemptActionInvocationInvalid);
        }
        prior_action_id = action.action_id;
    }
    if attempt.state.is_terminal() && !attempt.progress.outstanding_actions.is_empty() {
        return Err(LocalRunDirectoryError::AttemptTerminalActionsInvalid);
    }
    Ok(())
}

fn validate_attempt_guards(
    attempt: &LocalAttemptV1,
    step_ids: &BTreeSet<&str>,
) -> Result<(), LocalRunDirectoryError> {
    let mut guard_ids = BTreeSet::new();
    for guard in &attempt.process_guards {
        if !is_canonical_uuid(&guard.guard_id) || !guard_ids.insert(guard.guard_id.as_str()) {
            return Err(LocalRunDirectoryError::AttemptGuardIdInvalid);
        }
        if guard.action_id == 0 {
            return Err(LocalRunDirectoryError::AttemptGuardActionInvalid);
        }
        if !step_ids.contains(guard.step_id.as_str())
            || attempt_node_role(attempt, &guard.step_id) != Some(guard.node_role)
        {
            return Err(LocalRunDirectoryError::AttemptGuardStepInvalid);
        }
        if !validate_execution_host(&guard.execution_host) {
            return Err(LocalRunDirectoryError::AttemptGuardHostInvalid);
        }
        if guard.process_group_id <= 0
            || guard.liveness.value.is_empty()
            || guard.liveness.value.len() > 256
        {
            return Err(LocalRunDirectoryError::AttemptGuardProcessInvalid);
        }
    }
    Ok(())
}

fn validate_attempt_continuation(
    state_index: &LocalRunStateIndex<'_>,
    attempt: &LocalAttemptV1,
) -> Result<(), LocalRunDirectoryError> {
    let inherited = attempt
        .progress
        .steps
        .iter()
        .filter(|step| step.state == AttemptStepStateV1::Inherited)
        .collect::<Vec<_>>();
    let Some(continuation) = &attempt.continuation else {
        return (attempt.trigger != AttemptTriggerV1::Continuation && inherited.is_empty())
            .then_some(())
            .ok_or(LocalRunDirectoryError::AttemptContinuationInvalid);
    };
    let reexecuted = attempt
        .progress
        .steps
        .iter()
        .filter(|step| step.state != AttemptStepStateV1::Inherited)
        .map(|step| step.id.as_str())
        .collect::<Vec<_>>();
    if attempt.trigger != AttemptTriggerV1::Continuation
        || continuation.workspace.preparation
            != super::publication::ContinuationPreparationV1::Ready
        || !super::result_metadata::validate_continuation_record(continuation)
        || continuation.workspace.execution_root != attempt.execution_root
        || reexecuted
            != continuation
                .reexecuted_steps
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        || continuation.inherited_steps.len() != inherited.len()
        || continuation
            .inherited_steps
            .iter()
            .zip(&inherited)
            .any(|(record, step)| record.id != step.id)
    {
        return Err(LocalRunDirectoryError::AttemptContinuationInvalid);
    }
    let prior = state_index
        .attempt(
            attempt
                .prior_attempt_number
                .ok_or(LocalRunDirectoryError::AttemptContinuationInvalid)?,
        )
        .ok_or(LocalRunDirectoryError::AttemptContinuationInvalid)?;
    let requested_execution_root = continuation
        .request
        .execution_root
        .as_deref()
        .unwrap_or(&prior.execution_root);
    if requested_execution_root != continuation.workspace.execution_root
        || continuation.workspace.prior_execution_root != prior.execution_root
        || continuation.workspace.prior_settlement_snapshot != prior.settlement_snapshot
    {
        return Err(LocalRunDirectoryError::AttemptContinuationInvalid);
    }
    for (record, step) in continuation.inherited_steps.iter().zip(inherited) {
        let Some(NodeDetail::Inherited(detail)) = &step.detail else {
            return Err(LocalRunDirectoryError::AttemptInheritedStepInvalid);
        };
        let disposition = state_index
            .disposition(attempt.attempt_number, &step.id)
            .ok_or(LocalRunDirectoryError::AttemptInheritedStepInvalid)?;
        let prior_step = state_index
            .step(prior.attempt_number, &step.id)
            .ok_or(LocalRunDirectoryError::AttemptInheritedStepInvalid)?;
        let prior_state = match prior_step.state {
            AttemptStepStateV1::Succeeded => super::evidence::InheritedPriorState::Succeeded,
            AttemptStepStateV1::Skipped => super::evidence::InheritedPriorState::Skipped,
            AttemptStepStateV1::Inherited => super::evidence::InheritedPriorState::Inherited,
            AttemptStepStateV1::Pending
            | AttemptStepStateV1::Starting
            | AttemptStepStateV1::Running
            | AttemptStepStateV1::CapturingOutputs
            | AttemptStepStateV1::Failed
            | AttemptStepStateV1::Blocked
            | AttemptStepStateV1::NotRun
            | AttemptStepStateV1::Cancelling
            | AttemptStepStateV1::Cancelled => {
                return Err(LocalRunDirectoryError::AttemptInheritedStepInvalid);
            }
        };
        let outputs = step
            .outputs
            .as_deref()
            .ok_or(LocalRunDirectoryError::AttemptInheritedStepInvalid)?;
        let prior_outputs = prior_step
            .outputs
            .as_deref()
            .ok_or(LocalRunDirectoryError::AttemptInheritedStepInvalid)?;
        if detail.prior_attempt_id != prior.attempt_id
            || detail.prior_attempt_number != prior.attempt_number
            || detail.prior_state != prior_state
            || detail.prior_state != record.prior_state
            || detail.definition_changed != record.definition_changed
            || outputs.len() != prior_outputs.len()
            || (disposition == InheritedDisposition::Skipped && !outputs.is_empty())
        {
            return Err(LocalRunDirectoryError::AttemptInheritedStepInvalid);
        }
        for output in outputs {
            let prior_output = prior_outputs
                .iter()
                .find(|candidate| candidate.name() == output.name())
                .ok_or(LocalRunDirectoryError::AttemptInheritedOutputInvalid)?;
            let expected_producer = match prior_step.state {
                AttemptStepStateV1::Succeeded => {
                    own_output_producer(prior, &step.id, output.name())
                }
                AttemptStepStateV1::Inherited => prior_output
                    .producer()
                    .cloned()
                    .ok_or(LocalRunDirectoryError::AttemptInheritedOutputInvalid)?,
                AttemptStepStateV1::Skipped
                | AttemptStepStateV1::Pending
                | AttemptStepStateV1::Starting
                | AttemptStepStateV1::Running
                | AttemptStepStateV1::CapturingOutputs
                | AttemptStepStateV1::Failed
                | AttemptStepStateV1::Blocked
                | AttemptStepStateV1::NotRun
                | AttemptStepStateV1::Cancelling
                | AttemptStepStateV1::Cancelled => {
                    return Err(LocalRunDirectoryError::AttemptInheritedOutputInvalid);
                }
            };
            if output.producer() != Some(&expected_producer)
                || !retained_output_payload_matches(prior_output, output, &expected_producer)
            {
                return Err(LocalRunDirectoryError::AttemptInheritedOutputInvalid);
            }
            let producer_attempt = state_index
                .attempt(expected_producer.attempt_number)
                .filter(|candidate| candidate.attempt_id == expected_producer.attempt_id)
                .ok_or(LocalRunDirectoryError::AttemptInheritedOutputInvalid)?;
            let source = state_index
                .step(producer_attempt.attempt_number, &step.id)
                .filter(|candidate| candidate.state == AttemptStepStateV1::Succeeded)
                .and_then(|source| source.outputs.as_deref())
                .and_then(|outputs| {
                    outputs
                        .iter()
                        .find(|candidate| candidate.name() == output.name())
                })
                .ok_or(LocalRunDirectoryError::AttemptInheritedOutputInvalid)?;
            if !retained_output_payload_matches(source, output, &expected_producer) {
                return Err(LocalRunDirectoryError::AttemptInheritedOutputInvalid);
            }
        }
    }
    for step in attempt
        .progress
        .steps
        .iter()
        .filter(|step| step.state == AttemptStepStateV1::Succeeded)
    {
        if step.outputs.iter().flatten().any(|output| {
            output.producer().is_some_and(|producer| {
                producer.attempt_id != attempt.attempt_id
                    || producer.attempt_number != attempt.attempt_number
                    || producer.node != step.id
                    || producer.output != output.name()
            })
        }) {
            return Err(LocalRunDirectoryError::AttemptContinuationInvalid);
        }
    }
    Ok(())
}

fn retained_output_payload_matches(
    source: &RetainedOutputV1,
    inherited: &RetainedOutputV1,
    producer: &super::runtime::OutputProducer,
) -> bool {
    let mut source = source.clone();
    let mut inherited = inherited.clone();
    source.set_producer(producer.clone());
    inherited.set_producer(producer.clone());
    source == inherited
}

fn validate_attempt_recovery(
    attempt: &LocalAttemptV1,
    step_ids: &BTreeSet<&str>,
) -> Result<(), LocalRunDirectoryError> {
    if attempt.progress.accounting.maximum_invocations == 0
        || u64::try_from(attempt.progress.invocations.len())
            .ok()
            .is_none_or(|count| count > attempt.progress.accounting.maximum_invocations)
    {
        return Err(LocalRunDirectoryError::AttemptRecoveryAccountingInvalid);
    }
    let mut invocation_ids = BTreeSet::new();
    let mut previous_invocation_id = 0_u64;
    for invocation in &attempt.progress.invocations {
        if invocation.invocation_id == 0
            || invocation.invocation_id <= previous_invocation_id
            || !invocation_ids.insert(invocation.invocation_id)
            || !step_ids.contains(invocation.step_id.as_str())
            || attempt_node_role(attempt, &invocation.step_id) != Some(invocation.node_role)
            || invocation.target_execution.is_some() == invocation.recovery_round.is_some()
            || (invocation.role == super::publication::RecoveryInvocationRoleV1::Target)
                != invocation.target_execution.is_some()
            || !valid_timestamp(&invocation.started_at)
            || invocation
                .finished_at
                .as_deref()
                .is_some_and(|finished| !valid_timestamp(finished))
            || (invocation.state == DurableInvocationStateV1::Active)
                != invocation.finished_at.is_none()
            || invocation
                .diagnostic_reference
                .as_deref()
                .is_some_and(|path| {
                    !is_canonical_relative_path(path) || path.split('/').any(str::is_empty)
                })
        {
            return Err(LocalRunDirectoryError::AttemptRecoveryInvocationInvalid);
        }
        for diagnostic in &invocation.diagnostics {
            if !is_canonical_relative_path(&diagnostic.reference)
                || diagnostic.truncated != (diagnostic.discarded_bytes != 0)
            {
                return Err(LocalRunDirectoryError::AttemptRecoveryDiagnosticInvalid);
            }
        }
        previous_invocation_id = invocation.invocation_id;
    }
    if attempt.state.is_terminal()
        && attempt
            .progress
            .invocations
            .iter()
            .any(|invocation| invocation.state == DurableInvocationStateV1::Active)
    {
        return Err(LocalRunDirectoryError::AttemptRecoveryInvocationInvalid);
    }
    let mut projected = attempt.progress.clone();
    recalculate_invocation_accounting(&mut projected)?;
    if projected.accounting != attempt.progress.accounting {
        return Err(LocalRunDirectoryError::AttemptRecoveryAccountingInvalid);
    }
    for step in &attempt.progress.steps {
        let Some(recovery) = &step.recovery else {
            continue;
        };
        if recovery.schema_version != 1
            || !(1..=10).contains(&recovery.configured_retries)
            || recovery.rounds.is_empty()
            || recovery.rounds.len() > usize::from(recovery.configured_retries)
            || recovery
                .rounds
                .iter()
                .enumerate()
                .any(|(index, round)| round.number != u8::try_from(index + 1).unwrap_or(u8::MAX))
            || recovery.termination.is_some()
                != matches!(
                    step.state,
                    AttemptStepStateV1::Succeeded
                        | AttemptStepStateV1::Failed
                        | AttemptStepStateV1::Cancelled
                )
        {
            return Err(LocalRunDirectoryError::AttemptRecoveryRoundInvalid);
        }
        if let Some(active) = &recovery.active
            && !attempt.progress.invocations.iter().any(|invocation| {
                invocation.invocation_id == active.invocation_id
                    && invocation.step_id == step.id
                    && invocation.role == active.role
                    && invocation.target_execution == active.target_execution
                    && invocation.recovery_round == active.recovery_round
                    && invocation.state == DurableInvocationStateV1::Active
            })
        {
            return Err(LocalRunDirectoryError::AttemptRecoveryActiveInvalid);
        }
    }
    Ok(())
}

fn validate_attempt_finalization<'a>(
    attempt: &'a LocalAttemptV1,
    node_ids: &mut BTreeSet<&'a str>,
) -> Result<(), LocalRunDirectoryError> {
    let Some(finalization) = &attempt.finalization else {
        return Ok(());
    };
    let force_abort = match finalization {
        AttemptFinalizationV1::Progress(progress) => progress.force_abort,
        AttemptFinalizationV1::Complete(complete) => complete.force_abort,
    };
    if force_abort != attempt.force_abort.is_some() {
        return Err(LocalRunDirectoryError::AttemptFinalizationForceAbortInvalid);
    }
    match finalization {
        AttemptFinalizationV1::Progress(progress) => {
            if progress.complete
                || progress.finalizers.is_empty()
                || progress.finalizers.iter().any(|finalizer| {
                    finalizer.role != AttemptNodeRoleV1::Finalizer
                        || finalizer.id.is_empty()
                        || !node_ids.insert(finalizer.id.as_str())
                        || !attempt_step_detail_valid(
                            finalizer.role,
                            finalizer.state,
                            finalizer.detail.as_ref(),
                        )
                        || !attempt_step_diagnostic_valid(
                            finalizer.state,
                            finalizer.diagnostic.as_ref(),
                        )
                        || !attempt_step_started_at_valid(
                            finalizer.state,
                            finalizer.started_at.as_deref(),
                        )
                        || !retained_output_set_valid(
                            finalizer.role,
                            &finalizer.id,
                            finalizer.state,
                            finalizer.outputs.as_deref(),
                            attempt.definition.is_some(),
                        )
                        || !retained_finalization_cancellation_detail_valid(
                            finalizer.detail.as_ref(),
                            progress.cancellation.as_ref(),
                            progress.force_abort,
                        )
                })
            {
                return Err(LocalRunDirectoryError::AttemptFinalizationProgressInvalid);
            }
            if matches!(
                attempt.state,
                AttemptStateV1::Created | AttemptStateV1::Rejected
            ) {
                return Err(LocalRunDirectoryError::AttemptFinalizationProgressInvalid);
            }
            if !valid_finalization_interruption(
                progress.cancellation.as_ref(),
                progress.force_abort,
                attempt
                    .force_abort
                    .map(|force_abort| force_abort.phase.into()),
            ) {
                return Err(LocalRunDirectoryError::AttemptFinalizationInterruptionInvalid);
            }
            if matches!(
                attempt.state,
                AttemptStateV1::Succeeded
                    | AttemptStateV1::WorkflowFailed
                    | AttemptStateV1::Cancelled
                    | AttemptStateV1::Waiting
                    | AttemptStateV1::Continued
            ) {
                return Err(LocalRunDirectoryError::AttemptFinalizationProgressInvalid);
            }
        }
        AttemptFinalizationV1::Complete(complete) => {
            if !complete.complete
                || complete.finalizers.is_empty()
                || !matches!(
                    attempt.state,
                    AttemptStateV1::Succeeded
                        | AttemptStateV1::WorkflowFailed
                        | AttemptStateV1::Cancelled
                )
                // Progress and complete finalizers have distinct durable types and phase
                // invariants, so keeping their local validation explicit is clearer.
                || complete.finalizers.iter().any(|finalizer| {
                    finalizer.role != AttemptNodeRoleV1::Finalizer
                        || finalizer.id.is_empty()
                        || !node_ids.insert(finalizer.id.as_str())
                        || !durable_finalizer_valid(finalizer)
                        || !retained_output_set_valid(
                            finalizer.role,
                            &finalizer.id,
                            finalizer.state,
                            finalizer.outputs.as_deref(),
                            attempt.definition.is_some(),
                        )
                        || !retained_finalization_cancellation_detail_valid(
                            finalizer.detail.as_ref(),
                            complete.cancellation.as_ref(),
                            complete.force_abort,
                        )
                })
            {
                return Err(LocalRunDirectoryError::AttemptFinalizationCompleteInvalid);
            }
            let expected_issues = complete
                .finalizers
                .iter()
                .filter(|finalizer| {
                    matches!(
                        finalizer.state,
                        AttemptStepStateV1::Failed | AttemptStepStateV1::Blocked
                    )
                })
                .map(|finalizer| (finalizer.id.as_str(), finalizer.failure_policy))
                .collect::<Vec<_>>();
            if complete.issues.len() != expected_issues.len()
                || complete
                    .issues
                    .iter()
                    .zip(expected_issues)
                    .any(|(issue, (id, impact))| issue.finalizer_id != id || issue.impact != impact)
            {
                return Err(LocalRunDirectoryError::AttemptFinalizationIssuesInvalid);
            }
            if !valid_finalization_interruption(
                complete.cancellation.as_ref(),
                complete.force_abort,
                attempt
                    .force_abort
                    .map(|force_abort| force_abort.phase.into()),
            ) {
                return Err(LocalRunDirectoryError::AttemptFinalizationInterruptionInvalid);
            }
        }
    }
    Ok(())
}

fn retained_finalization_cancellation_detail_valid(
    detail: Option<&NodeDetail>,
    cancellation: Option<&DurableFinalizationCancellationV1>,
    force_abort: bool,
) -> bool {
    let Some(NodeDetail::Cancellation(detail)) = detail else {
        return true;
    };
    finalization_node_cancellation_matches(
        cancellation_reason(detail.code),
        cancellation.map(|cancellation| cancellation.reason),
        CancellationReasonV1::ForceAbort,
        force_abort,
    )
}

fn valid_finalization_interruption(
    cancellation: Option<&DurableFinalizationCancellationV1>,
    force_abort: bool,
    first_force_abort_phase: Option<FirstForceAbortPhase>,
) -> bool {
    if !finalization_cancellation_matches_force_phase(
        cancellation.map(|cancellation| cancellation.reason),
        CancellationReasonV1::ForceAbort,
        first_force_abort_phase,
    ) {
        return false;
    }
    match (cancellation, force_abort) {
        (None, false) => true,
        (Some(cancellation), false) => {
            cancellation.reason != CancellationReasonV1::ForceAbort
                && cancellation
                    .force_stop_deadline
                    .as_deref()
                    .is_some_and(valid_timestamp)
        }
        (Some(cancellation), true) => {
            (cancellation.reason == CancellationReasonV1::ForceAbort
                && cancellation.force_stop_deadline.is_none())
                || (cancellation.reason != CancellationReasonV1::ForceAbort
                    && cancellation
                        .force_stop_deadline
                        .as_deref()
                        .is_some_and(valid_timestamp))
        }
        (None, true) => false,
    }
}

struct RetainedNodeOutputs<'a> {
    id: &'a str,
    role: AttemptNodeRoleV1,
    state: AttemptStepStateV1,
    outputs: Option<&'a [RetainedOutputV1]>,
}

impl<'a> From<&'a AttemptStepV1> for RetainedNodeOutputs<'a> {
    fn from(node: &'a AttemptStepV1) -> Self {
        Self {
            id: &node.id,
            role: node.role,
            state: node.state,
            outputs: node.outputs.as_deref(),
        }
    }
}

impl<'a> From<&'a DurableFinalizerV1> for RetainedNodeOutputs<'a> {
    fn from(node: &'a DurableFinalizerV1) -> Self {
        Self {
            id: &node.id,
            role: node.role,
            state: node.state,
            outputs: node.outputs.as_deref(),
        }
    }
}

pub(super) fn validate_retained_outputs_against_definition(
    attempt: &LocalAttemptV1,
    workflow: &ResolvedWorkflow,
) -> Result<(), LocalRunDirectoryError> {
    if attempt.definition.is_none() {
        return Ok(());
    }
    if attempt.progress.steps.len() != workflow.definition.presentation_order.len() {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    for (step, expected_id) in attempt
        .progress
        .steps
        .iter()
        .zip(&workflow.definition.presentation_order)
    {
        let definition = workflow
            .definition
            .steps
            .get(expected_id)
            .ok_or(LocalRunDirectoryError::StateInvalid)?;
        if step.id != *expected_id
            || step.role != AttemptNodeRoleV1::Step
            || !retained_outputs_match_declarations(step.state, step.outputs.as_deref(), definition)
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
    }

    let finalizers = match &attempt.finalization {
        None => return Ok(()),
        Some(AttemptFinalizationV1::Progress(progress)) => progress
            .finalizers
            .iter()
            .map(RetainedNodeOutputs::from)
            .collect::<Vec<_>>(),
        Some(AttemptFinalizationV1::Complete(complete)) => complete
            .finalizers
            .iter()
            .map(RetainedNodeOutputs::from)
            .collect::<Vec<_>>(),
    };
    if finalizers.len() != workflow.definition.finalizer_presentation_order.len() {
        return Err(LocalRunDirectoryError::StateInvalid);
    }
    for (finalizer, expected_id) in finalizers
        .into_iter()
        .zip(&workflow.definition.finalizer_presentation_order)
    {
        let definition = &workflow
            .definition
            .finalizers
            .get(expected_id)
            .ok_or(LocalRunDirectoryError::StateInvalid)?
            .body;
        if finalizer.id != expected_id
            || finalizer.role != AttemptNodeRoleV1::Finalizer
            || !retained_outputs_match_declarations(finalizer.state, finalizer.outputs, definition)
        {
            return Err(LocalRunDirectoryError::StateInvalid);
        }
    }
    Ok(())
}

fn retained_outputs_match_declarations(
    state: AttemptStepStateV1,
    retained: Option<&[RetainedOutputV1]>,
    definition: &super::validated::ValidatedStep,
) -> bool {
    let Some(retained) = retained else {
        return false;
    };
    if state == AttemptStepStateV1::Inherited {
        return true;
    }
    if state != AttemptStepStateV1::Succeeded {
        return retained.is_empty();
    }
    let declared = &definition.common().outputs;
    // A `workflow` step commits its `outcome` and only the child exports that were
    // available (REQ-WFEXEC-58), so a child export it left out is absent.
    let optional = |declaration: &super::document::Output| {
        matches!(declaration, super::document::Output::ChildExport { .. })
    };
    let mut names = BTreeSet::new();
    retained.iter().all(|output| {
        names.insert(output.name())
            && declared.get(output.name()).is_some_and(|declaration| {
                retained_output_matches_declaration(output, &declaration.definition)
            })
    }) && declared.iter().all(|(name, declaration)| {
        optional(&declaration.definition) || names.contains(name.as_str())
    })
}

fn retained_output_matches_declaration(
    retained: &RetainedOutputV1,
    declared: &super::document::Output,
) -> bool {
    matches!(
        (retained, declared),
        (
            RetainedOutputV1::Text { .. },
            super::document::Output::TextPath { .. }
                | super::document::Output::TextAgentResponse
                | super::document::Output::ChildOutcome
                | super::document::Output::ChildExport {
                    value_type: super::validated::WorkflowValueType::Text
                }
        ) | (
            RetainedOutputV1::Json { .. },
            super::document::Output::JsonPath { .. }
                | super::document::Output::JsonAgentResult { .. }
                | super::document::Output::ChildExport {
                    value_type: super::validated::WorkflowValueType::Json
                }
        ) | (
            RetainedOutputV1::GitBranch { .. },
            super::document::Output::GitBranchWorkspace
        )
    ) || matches!(
        (retained, declared),
        (
            RetainedOutputV1::File { media_type, .. },
            super::document::Output::FilePath {
                media_type: declared_media_type,
                ..
            }
        ) if media_type == declared_media_type
    )
}

pub(super) fn retained_output_matches_export(
    attempt: &LocalAttemptV1,
    role: AttemptNodeRoleV1,
    node: &str,
    output_name: &str,
    export: &super::publication::ExportV1,
) -> bool {
    retained_node_output(attempt, role, node, output_name)
        .is_some_and(|output| output.matches_export(export))
}

pub(super) fn retained_output_present(
    attempt: &LocalAttemptV1,
    role: AttemptNodeRoleV1,
    node: &str,
    output_name: &str,
) -> bool {
    retained_node_output(attempt, role, node, output_name).is_some()
}

fn retained_node_output<'a>(
    attempt: &'a LocalAttemptV1,
    role: AttemptNodeRoleV1,
    node: &str,
    output_name: &str,
) -> Option<&'a RetainedOutputV1> {
    let outputs = match role {
        AttemptNodeRoleV1::Step => attempt
            .progress
            .steps
            .iter()
            .find(|step| step.id == node)
            .and_then(|step| step.outputs.as_deref()),
        AttemptNodeRoleV1::Finalizer => {
            attempt
                .finalization
                .as_ref()
                .and_then(|finalization| match finalization {
                    AttemptFinalizationV1::Progress(progress) => progress
                        .finalizers
                        .iter()
                        .find(|finalizer| finalizer.id == node)
                        .and_then(|finalizer| finalizer.outputs.as_deref()),
                    AttemptFinalizationV1::Complete(complete) => complete
                        .finalizers
                        .iter()
                        .find(|finalizer| finalizer.id == node)
                        .and_then(|finalizer| finalizer.outputs.as_deref()),
                })
        }
    };
    outputs.and_then(|outputs| outputs.iter().find(|output| output.name() == output_name))
}

fn retained_output_set_valid(
    role: AttemptNodeRoleV1,
    node: &str,
    state: AttemptStepStateV1,
    outputs: Option<&[RetainedOutputV1]>,
    required: bool,
) -> bool {
    let Some(outputs) = outputs else {
        return !required;
    };
    if !matches!(
        state,
        AttemptStepStateV1::Succeeded | AttemptStepStateV1::Inherited
    ) && !outputs.is_empty()
    {
        return false;
    }
    let mut names = BTreeSet::new();
    outputs.iter().all(|output| {
        let name = output.name();
        um_support::is_identifier(name)
            && names.insert(name)
            && retained_output_valid(role, node, output)
    })
}

fn retained_output_valid(role: AttemptNodeRoleV1, node: &str, output: &RetainedOutputV1) -> bool {
    let expected_path = retained_value_relative_path(role, node, output.name());
    if output.producer().is_some_and(|producer| {
        !is_canonical_uuid(&producer.attempt_id)
            || producer.attempt_number == 0
            || producer.node != node
            || producer.output != output.name()
    }) {
        return false;
    }
    let carrier_valid = |carrier: &RetainedCarrierV1, media_type: &str| {
        carrier.relative_path == expected_path
            && carrier.media_type == media_type
            && carrier.size_bytes <= MAXIMUM_RETAINED_FILE_BYTES
            && carrier.digest.validate()
    };
    match output {
        RetainedOutputV1::Text { carrier, .. } => {
            carrier_valid(carrier, "text/plain; charset=utf-8")
        }
        RetainedOutputV1::Json { carrier, .. } => carrier_valid(carrier, "application/json"),
        RetainedOutputV1::File {
            media_type,
            carrier,
            ..
        } => super::is_valid_media_type(media_type) && carrier_valid(carrier, media_type),
        RetainedOutputV1::GitBranch {
            artifact_version,
            object_format,
            base_oid,
            head_oid,
            tree_oid,
            carrier,
            ..
        } => {
            *artifact_version == 1
                && object_format == "sha1"
                && is_lowercase_hex(base_oid, 40)
                && is_lowercase_hex(head_oid, 40)
                && is_lowercase_hex(tree_oid, 40)
                && ((base_oid == head_oid && carrier.is_none())
                    || (base_oid != head_oid
                        && carrier.as_ref().is_some_and(|carrier| {
                            carrier_valid(carrier, "application/vnd.git.bundle")
                        })))
        }
    }
}

fn retained_value_relative_path(role: AttemptNodeRoleV1, node: &str, output: &str) -> String {
    let role = match role {
        AttemptNodeRoleV1::Step => "steps",
        AttemptNodeRoleV1::Finalizer => "finalizers",
    };
    format!("{VALUES_DIRECTORY}/{role}/{node}/{output}")
}

fn durable_finalizer_valid(finalizer: &DurableFinalizerV1) -> bool {
    attempt_step_detail_valid(finalizer.role, finalizer.state, finalizer.detail.as_ref())
        && attempt_step_diagnostic_valid(finalizer.state, finalizer.diagnostic.as_ref())
        && matches!(
            finalizer.state,
            AttemptStepStateV1::Succeeded
                | AttemptStepStateV1::Failed
                | AttemptStepStateV1::Blocked
                | AttemptStepStateV1::Skipped
                | AttemptStepStateV1::NotRun
                | AttemptStepStateV1::Cancelled
        )
}

/// Only a failed node carries a classified diagnostic, and never an empty one
/// (REQ-LOCALRUN-198).
fn attempt_step_diagnostic_valid(
    state: AttemptStepStateV1,
    diagnostic: Option<&FailureDiagnosticV1>,
) -> bool {
    diagnostic.is_none_or(|diagnostic| {
        state == AttemptStepStateV1::Failed
            && super::result_metadata::valid_failure_diagnostic(diagnostic)
    })
}

/// A node that left `pending` in its attempt may carry the canonical UTC time it did;
/// a pending or inherited node never does (REQ-LOCALRUN-201).
fn attempt_step_started_at_valid(state: AttemptStepStateV1, started_at: Option<&str>) -> bool {
    started_at.is_none_or(|started_at| {
        !matches!(
            state,
            AttemptStepStateV1::Pending | AttemptStepStateV1::Inherited
        ) && valid_timestamp(started_at)
    })
}

fn attempt_step_detail_valid(
    role: AttemptNodeRoleV1,
    state: AttemptStepStateV1,
    detail: Option<&NodeDetail>,
) -> bool {
    match (role, state, detail) {
        (
            _,
            AttemptStepStateV1::Pending
            | AttemptStepStateV1::Starting
            | AttemptStepStateV1::Running
            | AttemptStepStateV1::CapturingOutputs
            | AttemptStepStateV1::Succeeded,
            None,
        ) => true,
        (
            AttemptNodeRoleV1::Step,
            AttemptStepStateV1::Inherited,
            Some(NodeDetail::Inherited(detail)),
        ) => detail.prior_attempt_number > 0 && is_canonical_uuid(&detail.prior_attempt_id),
        (_, AttemptStepStateV1::Failed, Some(NodeDetail::Failed(_))) => true,
        (_, AttemptStepStateV1::Blocked, Some(NodeDetail::Blocked(_))) => true,
        (_, AttemptStepStateV1::Skipped, Some(NodeDetail::Skipped(_))) => true,
        (AttemptNodeRoleV1::Step, AttemptStepStateV1::NotRun, Some(NodeDetail::NotRun(detail))) => {
            matches!(
                detail.code,
                NonExecutionCode::FailureStop | NonExecutionCode::NotReached
            )
        }
        (
            AttemptNodeRoleV1::Finalizer,
            AttemptStepStateV1::NotRun,
            Some(NodeDetail::NotRun(detail)),
        ) => detail.code == NonExecutionCode::FinalizerTriggerNotSelected,
        (
            AttemptNodeRoleV1::Step | AttemptNodeRoleV1::Finalizer,
            AttemptStepStateV1::Cancelling | AttemptStepStateV1::Cancelled,
            Some(NodeDetail::Cancellation(_)),
        ) => true,
        _ => false,
    }
}

fn validate_attempt_result(attempt: &LocalAttemptV1) -> Result<(), LocalRunDirectoryError> {
    let valid = match (&attempt.state, &attempt.result) {
        (
            AttemptStateV1::Created | AttemptStateV1::Running | AttemptStateV1::Cancelling,
            AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::AttemptNonterminal,
            },
        ) => true,
        (
            AttemptStateV1::Succeeded
            | AttemptStateV1::WorkflowFailed
            | AttemptStateV1::Cancelled
            | AttemptStateV1::Waiting
            | AttemptStateV1::Continued,
            AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            },
        ) => true,
        (
            AttemptStateV1::Succeeded
            | AttemptStateV1::WorkflowFailed
            | AttemptStateV1::Cancelled
            | AttemptStateV1::Waiting
            | AttemptStateV1::Continued,
            AttemptResultV1::PublicationFailed {
                phase,
                result_invariant,
            },
        ) => result_invariant.is_none() || *phase == PublicationFailurePhaseV1::Serialization,
        (
            AttemptStateV1::Interrupted,
            AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::Interrupted,
            },
        ) => true,
        (
            AttemptStateV1::Rejected,
            AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::Rejected,
            },
        ) => true,
        (_, AttemptResultV1::Published { relative_directory }) => {
            *relative_directory == attempt_result_relative_path(attempt.attempt_number)
                && matches!(
                    attempt.state,
                    AttemptStateV1::Succeeded
                        | AttemptStateV1::WorkflowFailed
                        | AttemptStateV1::Cancelled
                        | AttemptStateV1::Waiting
                        | AttemptStateV1::Continued
                )
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(LocalRunDirectoryError::AttemptResultInvalid)
    }
}

fn validate_owner(owner: &AttemptOwnerV1) -> bool {
    is_canonical_uuid(&owner.owner_nonce) && validate_execution_host(&owner.execution_host)
}

fn validate_execution_host(host: &ExecutionHostV1) -> bool {
    !host.value.is_empty() && host.value.len() <= 256
}

pub(super) fn read_regular_file(
    parent: &OwnedFd,
    name: &str,
) -> Result<Vec<u8>, LocalRunDirectoryError> {
    read_regular_file_bounded(parent, name, MAXIMUM_DURABLE_JSON_BYTES)
}

fn read_regular_file_bounded(
    parent: &OwnedFd,
    name: &str,
    maximum_bytes: u64,
) -> Result<Vec<u8>, LocalRunDirectoryError> {
    let file = openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|source| file_error(parent, name, "open", source))?;
    let metadata = fstat(&file).map_err(|source| file_error(parent, name, "stat", source))?;
    if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile {
        return Err(LocalRunDirectoryError::StateFile {
            path: file_locator(parent, name),
            operation: "validate",
            source: Box::new(LocalRunDirectoryError::RegularFileInvalid),
        });
    }
    if metadata.st_size < 0
        || u64::try_from(metadata.st_size)
            .ok()
            .is_none_or(|size| size > maximum_bytes)
    {
        return Err(invalid_file_size(parent, name));
    }
    let mut file = File::from(file);
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| file_error(parent, name, "read", source))?;
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|size| size > maximum_bytes)
    {
        return Err(invalid_file_size(parent, name));
    }
    Ok(bytes)
}

fn invalid_file_size(parent: &OwnedFd, name: &str) -> LocalRunDirectoryError {
    LocalRunDirectoryError::StateFile {
        path: file_locator(parent, name),
        operation: "validate",
        source: Box::new(LocalRunDirectoryError::FileSizeInvalid),
    }
}

pub(super) fn encode_json(document: &impl Serialize) -> Result<Vec<u8>, LocalRunDirectoryError> {
    let mut bytes = serde_json::to_vec_pretty(document)
        .map_err(|_| LocalRunDirectoryError::SerializationUnavailable)?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(super) fn write_new_immutable_file(
    parent: &OwnedFd,
    name: &str,
    bytes: &[u8],
) -> Result<(), LocalRunDirectoryError> {
    tree_copy::write_new_file_with_final_mode(parent, name, bytes, Mode::RUSR)
        .map_err(file_operation_error)
}

fn write_new_state_file(parent: &OwnedFd, bytes: &[u8]) -> Result<(), LocalRunDirectoryError> {
    let mut file = create_file(parent, STATE_FILE, Mode::RUSR | Mode::WUSR)?;
    file.write_all(bytes)
        .and_then(|()| file.flush())
        .and_then(|()| file.sync_all())
        .map_err(|source| file_error(parent, STATE_FILE, "write and sync", source))
}

fn create_file(parent: &OwnedFd, name: &str, mode: Mode) -> Result<File, LocalRunDirectoryError> {
    tree_copy::create_file(parent, name, mode).map_err(file_operation_error)
}

fn mkdir(
    parent: &OwnedFd,
    name: impl AsRef<std::ffi::OsStr>,
) -> Result<(), LocalRunDirectoryError> {
    tree_copy::mkdir(parent, name).map_err(file_operation_error)
}

pub(super) fn open_directory_at(
    parent: &OwnedFd,
    name: impl AsRef<std::ffi::OsStr>,
) -> Result<OwnedFd, LocalRunDirectoryError> {
    tree_copy::open_directory_at(parent, name).map_err(file_operation_error)
}

pub(super) fn sync_directory(directory: &OwnedFd) -> Result<(), LocalRunDirectoryError> {
    tree_copy::sync_directory(directory).map_err(file_operation_error)
}

fn ensure_absent(parent: &OwnedFd, name: &std::ffi::OsStr) -> Result<(), LocalRunDirectoryError> {
    match statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => Err(LocalRunDirectoryError::DestinationExists),
        Err(Errno::NOENT) => Ok(()),
        Err(_) => Err(LocalRunDirectoryError::ParentUnavailable),
    }
}

fn run_directory_overlaps_execution_root(
    run_directory: &Path,
    execution_path: &Path,
    execution_root: &AdmittedExecutionRoot,
    run_parent: &OwnedFd,
) -> Result<bool, LocalRunDirectoryError> {
    Ok(paths_overlap(run_directory, execution_path)
        || execution_root
            .contains_directory(run_parent)
            .map_err(|_| LocalRunDirectoryError::ParentUnavailable)?)
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

pub(super) fn directory_entries(
    directory: &OwnedFd,
) -> Result<BTreeSet<Vec<u8>>, LocalRunDirectoryError> {
    directory_entry_names(directory)
        .map_err(|source| file_error(directory, ".", "list directory", source))
}

fn timestamp(value: OffsetDateTime) -> Result<String, LocalRunDirectoryError> {
    utc_timestamp(value).map_err(|_| LocalRunDirectoryError::SerializationUnavailable)
}

fn valid_timestamp(value: &str) -> bool {
    value.ends_with('Z') && OffsetDateTime::parse(value, &Rfc3339).is_ok()
}

fn generate_uuid() -> Result<String, LocalRunDirectoryError> {
    super::identity::random_uuid_v4().map_err(|_| LocalRunDirectoryError::IdentityUnavailable)
}

pub(super) fn is_canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
        })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn execution_host() -> Result<ExecutionHostV1, LocalRunDirectoryError> {
    let value = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map_err(|_| LocalRunDirectoryError::HostIdentityUnavailable)?;
    let value = value.trim_end_matches(['\r', '\n']).to_ascii_lowercase();
    validated_execution_host(value)
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "the host-boot identity boundary reads the fixed kern.bootsessionuuid sysctl"
)]
fn execution_host() -> Result<ExecutionHostV1, LocalRunDirectoryError> {
    let mut length = 0_usize;
    // SAFETY: the fixed C string is valid and the null output pointer requests only the size.
    let result = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            std::ptr::null_mut(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 || length == 0 || length > 257 {
        return Err(LocalRunDirectoryError::HostIdentityUnavailable);
    }
    let mut bytes = vec![0_u8; length];
    // SAFETY: sysctl writes at most the reported length into the allocated byte buffer.
    let result = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            bytes.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 {
        return Err(LocalRunDirectoryError::HostIdentityUnavailable);
    }
    bytes.truncate(length);
    if bytes.last() == Some(&0) {
        bytes.pop();
    }
    let value = String::from_utf8(bytes)
        .map_err(|_| LocalRunDirectoryError::HostIdentityUnavailable)?
        .to_ascii_lowercase();
    validated_execution_host(value)
}

fn validated_execution_host(value: String) -> Result<ExecutionHostV1, LocalRunDirectoryError> {
    if !is_canonical_uuid(&value) {
        return Err(LocalRunDirectoryError::HostIdentityUnavailable);
    }
    Ok(ExecutionHostV1 {
        kind: ExecutionHostKindV1::HostBoot,
        value,
    })
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn execution_host() -> Result<ExecutionHostV1, LocalRunDirectoryError> {
    Err(LocalRunDirectoryError::HostIdentityUnavailable)
}

#[cfg(test)]
mod tests;
