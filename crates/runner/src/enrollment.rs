use std::error::Error;
use std::fmt;
use std::fs::{self, DirBuilder, File, Metadata, OpenOptions, Permissions};
use std::io::{self, IsTerminal, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use fs4::{FileExt, TryLockError};
use reqwest::{StatusCode, Url};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;

use um_support::generate_idempotency_key;

use super::service::Backoff;
use super::validation::{valid_secret_syntax, valid_typed_id};

mod home;

pub use home::{PublicationStage, RunnerHome, RunnerHomeError};

const ARTIFACT_LIMIT: u64 = 4096;
const CONFIG_LIMIT: u64 = 16 * 1024;
const STATE_LIMIT: u64 = 64 * 1024;
const RESPONSE_LIMIT: u64 = 64 * 1024;
const DIRECTORY_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;
const JOURNAL_FILE: &str = ".runner-enrollment.json";
const LOCK_FILE: &str = ".runner-state.lock";
/// The temporary files of the runner's protected host documents.
const STATE_TEMPORARY_PREFIX: &str = ".runner-state.tmp";
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const LOCK_RETRY: Duration = Duration::from_millis(25);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
#[expect(
    clippy::cast_possible_wrap,
    reason = "O_NOFOLLOW fits in the signed custom_flags value on supported Unix targets"
)]
const NOFOLLOW_FLAG: i32 = rustix::fs::OFlags::NOFOLLOW.bits() as i32;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ActivationArtifact {
    schema_version: u8,
    activation_url: String,
    #[serde(skip_serializing)]
    activation_token: String,
    runner_id: String,
    expires_at: String,
}

/// Owned activation fields supplied by the caller that received the Cloud
/// response. Translation from API DTOs happens at that boundary so the
/// runner component never references the human API client.
pub struct ActivationArtifactParts {
    pub activation_url: String,
    pub activation_token: String,
    pub runner_id: String,
    pub expires_at: String,
}

impl ActivationArtifact {
    pub fn from_parts(parts: ActivationArtifactParts) -> Self {
        Self {
            schema_version: 1,
            activation_url: parts.activation_url,
            activation_token: parts.activation_token,
            runner_id: parts.runner_id,
            expires_at: parts.expires_at,
        }
    }

    pub fn write_json(&self, output: &mut impl Write) -> Result<(), EnrollmentError> {
        let transferable = TransferableActivationArtifact::from(self);
        serde_json::to_writer_pretty(&mut *output, &transferable)
            .map_err(|_| EnrollmentError::ArtifactWrite)?;
        output
            .write_all(b"\n")
            .map_err(|_| EnrollmentError::ArtifactWrite)
    }

    pub fn runner_id(&self) -> &str {
        &self.runner_id
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TransferableActivationArtifact<'a> {
    schema_version: u8,
    activation_url: &'a str,
    activation_token: &'a str,
    runner_id: &'a str,
    expires_at: &'a str,
}

impl<'a> From<&'a ActivationArtifact> for TransferableActivationArtifact<'a> {
    fn from(artifact: &'a ActivationArtifact) -> Self {
        Self {
            schema_version: artifact.schema_version,
            activation_url: &artifact.activation_url,
            activation_token: &artifact.activation_token,
            runner_id: &artifact.runner_id,
            expires_at: &artifact.expires_at,
        }
    }
}

/// A pool's join key, the OpenAPI `RunnerJoinKeyArtifact`. Its token stays in
/// memory for one start: it is never serialized, so neither the state, the
/// journal nor a diagnostic can carry it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JoinKeyArtifact {
    schema_version: u8,
    join_url: String,
    join_key_id: String,
    join_token: String,
    expires_at: String,
}

impl JoinKeyArtifact {
    /// Reads the artifact as an activation file is read: from a regular file
    /// private to the current user, which is neither removed nor rewritten, or
    /// with `-` from piped standard input.
    pub fn read_file(path: &Path) -> Result<Self, EnrollmentError> {
        let bytes = if path == Path::new("-") {
            if io::stdin().is_terminal() {
                return Err(EnrollmentError::TerminalStdin);
            }
            read_piped_artifact().map_err(|_| EnrollmentError::InvalidJoinKey)?
        } else {
            read_bounded_regular_file(path, ARTIFACT_LIMIT, true)
                .map_err(|_| EnrollmentError::JoinKeyFile)?
        };
        Self::parse(&bytes)
    }

    /// Parses the artifact text, as supplied by the environment.
    pub fn parse(bytes: &[u8]) -> Result<Self, EnrollmentError> {
        if bytes.len() as u64 > ARTIFACT_LIMIT {
            return Err(EnrollmentError::InvalidJoinKey);
        }
        let key: Self =
            serde_json::from_slice(bytes).map_err(|_| EnrollmentError::InvalidJoinKey)?;
        if key.schema_version != 1
            || !valid_typed_id(&key.join_key_id, "rjk_")
            || parse_rfc3339(&key.expires_at).is_none()
            || bearer_capability_id(&key.join_token, "rjk_").as_deref()
                != Some(key.join_key_id.as_str())
        {
            return Err(EnrollmentError::InvalidJoinKey);
        }
        Ok(key)
    }

    pub fn join_key_id(&self) -> &str {
        &self.join_key_id
    }
}

// StagedJoinKey is what a join journal keeps of its key: enough to resend the
// staged request, never the token, which the next start re-reads from the
// still-supplied key.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StagedJoinKey {
    join_url: String,
    join_key_id: String,
    expires_at: String,
}

impl From<&JoinKeyArtifact> for StagedJoinKey {
    fn from(key: &JoinKeyArtifact) -> Self {
        Self {
            join_url: key.join_url.clone(),
            join_key_id: key.join_key_id.clone(),
            expires_at: key.expires_at.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DeploymentMode {
    Production,
    Development,
}

// The runner home writes the document it generates through the same
// definition, so the field names exist once.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct OperatorConfig {
    schema_version: u8,
    deployment_mode: DeploymentMode,
    runner_state_path: PathBuf,
    control_socket_path: PathBuf,
    work_root: PathBuf,
}

// RunnerServiceConfiguration is the validated startup projection shared by
// enrollment and Runner Serve. Secret material remains private to the runner
// domain and is exposed only to construct the outbound Authorization header.
pub(crate) struct RunnerServiceConfiguration {
    pub(super) runner_id: String,
    pub(super) connection_url: String,
    pub(super) credential_id: String,
    pub(super) credential_secret: String,
    pub(super) pending_credential: Option<PendingCredential>,
    pub(super) state_access: RunnerStateAccess,
    pub(super) control_socket_path: PathBuf,
    pub(super) work_root: PathBuf,
}

#[derive(Clone)]
pub(crate) struct RunnerStateAccess {
    path: PathBuf,
    deployment_mode: DeploymentMode,
}

#[derive(Clone)]
pub(crate) struct PendingCredential {
    pub(super) runner_id: String,
    pub(super) connection_url: String,
    pub(super) credential_id: String,
    pub(super) credential_secret: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReloadStateError {
    RegistrationMismatch,
    StateUpdate,
}

// A journal stages either an activation's enrollment or a join with a pool's
// join key; the retained format names exactly one of the two.
#[derive(Clone)]
enum EnrollmentTarget {
    Activation(ActivationArtifact),
    Join(StagedJoinKey),
}

#[derive(Clone, Deserialize)]
#[serde(try_from = "StoredEnrollmentJournal")]
struct EnrollmentJournal {
    schema_version: u8,
    target: EnrollmentTarget,
    credential_secret: String,
    credential_secret_verifier: String,
    idempotency_key: String,
    replace_credential: bool,
    staged_at: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredEnrollmentJournal {
    schema_version: u8,
    #[serde(default)]
    activation_artifact: Option<ActivationArtifact>,
    #[serde(default)]
    join_key: Option<StagedJoinKey>,
    credential_secret: String,
    credential_secret_verifier: String,
    idempotency_key: String,
    replace_credential: bool,
    staged_at: String,
}

impl TryFrom<StoredEnrollmentJournal> for EnrollmentJournal {
    type Error = &'static str;

    fn try_from(stored: StoredEnrollmentJournal) -> Result<Self, Self::Error> {
        let target = match (stored.activation_artifact, stored.join_key) {
            (Some(artifact), None) => EnrollmentTarget::Activation(artifact),
            (None, Some(join_key)) => EnrollmentTarget::Join(join_key),
            _ => {
                return Err(
                    "enrollment journal names neither or both of an activation and a join key",
                );
            }
        };
        Ok(Self {
            schema_version: stored.schema_version,
            target,
            credential_secret: stored.credential_secret,
            credential_secret_verifier: stored.credential_secret_verifier,
            idempotency_key: stored.idempotency_key,
            replace_credential: stored.replace_credential,
            staged_at: stored.staged_at,
        })
    }
}

impl EnrollmentJournal {
    // The path is the journal's own, which an unresolved join names so the
    // operator can find it.
    fn activation_artifact(&self, path: &Path) -> Result<&ActivationArtifact, EnrollmentError> {
        match &self.target {
            EnrollmentTarget::Activation(artifact) => Ok(artifact),
            EnrollmentTarget::Join(staged) => Err(EnrollmentError::UnresolvedJoin {
                journal: path.to_owned(),
                join_key_id: staged.join_key_id.clone(),
            }),
        }
    }

    fn credential_origin(&self) -> Result<CredentialOrigin, EnrollmentError> {
        match &self.target {
            EnrollmentTarget::Activation(artifact) => {
                Ok(CredentialOrigin::Activation(activation_id(artifact)?))
            }
            EnrollmentTarget::Join(join_key) => {
                Ok(CredentialOrigin::JoinKey(join_key.join_key_id.clone()))
            }
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedEnrollmentJournal<'a> {
    schema_version: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    activation_artifact: Option<TransferableActivationArtifact<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    join_key: Option<&'a StagedJoinKey>,
    credential_secret: &'a str,
    credential_secret_verifier: &'a str,
    idempotency_key: &'a str,
    replace_credential: bool,
    staged_at: &'a str,
}

impl<'a> From<&'a EnrollmentJournal> for PersistedEnrollmentJournal<'a> {
    fn from(journal: &'a EnrollmentJournal) -> Self {
        Self {
            schema_version: journal.schema_version,
            activation_artifact: match &journal.target {
                EnrollmentTarget::Activation(artifact) => {
                    Some(TransferableActivationArtifact::from(artifact))
                }
                EnrollmentTarget::Join(_) => None,
            },
            join_key: match &journal.target {
                EnrollmentTarget::Activation(_) => None,
                EnrollmentTarget::Join(join_key) => Some(join_key),
            },
            credential_secret: &journal.credential_secret,
            credential_secret_verifier: &journal.credential_secret_verifier,
            idempotency_key: &journal.idempotency_key,
            replace_credential: journal.replace_credential,
            staged_at: &journal.staged_at,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TerminalReceipt {
    schema_version: u8,
    activation_id: String,
    disposition: TerminalDisposition,
    observed_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum TerminalDisposition {
    Gone,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RunnerState {
    schema_version: u8,
    runner_id: String,
    connection_url: String,
    current_credential: StoredRunnerCredential,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_credential: Option<StoredRunnerCredential>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_promotion: Option<CredentialPromotion>,
    updated_at: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredRunnerCredential {
    id: String,
    #[serde(skip_serializing)]
    secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    activation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    join_key_id: Option<String>,
    enrolled_at: String,
}

// A credential is created either by an activation's enrollment or by a join
// with a pool's join key; its state names exactly one of them.
#[derive(Clone, Eq, PartialEq)]
enum CredentialOrigin {
    Activation(String),
    JoinKey(String),
}

impl StoredRunnerCredential {
    fn new(id: String, secret: String, origin: CredentialOrigin, enrolled_at: String) -> Self {
        let (activation_id, join_key_id) = match origin {
            CredentialOrigin::Activation(activation_id) => (Some(activation_id), None),
            CredentialOrigin::JoinKey(join_key_id) => (None, Some(join_key_id)),
        };
        Self {
            id,
            secret,
            activation_id,
            join_key_id,
            enrolled_at,
        }
    }

    fn origin(&self) -> Option<CredentialOrigin> {
        match (&self.activation_id, &self.join_key_id) {
            (Some(activation_id), None) if valid_typed_id(activation_id, "rna_") => {
                Some(CredentialOrigin::Activation(activation_id.clone()))
            }
            (None, Some(join_key_id)) if valid_typed_id(join_key_id, "rjk_") => {
                Some(CredentialOrigin::JoinKey(join_key_id.clone()))
            }
            _ => None,
        }
    }

    fn activated_by(&self, activation_id: &str) -> bool {
        self.activation_id.as_deref() == Some(activation_id)
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CredentialPromotion {
    credential_id: String,
    activation_id: String,
    promoted_at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedRunnerState<'a> {
    schema_version: u8,
    runner_id: &'a str,
    connection_url: &'a str,
    current_credential: PersistedRunnerCredential<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_credential: Option<PersistedRunnerCredential<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_promotion: Option<&'a CredentialPromotion>,
    updated_at: &'a str,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedRunnerCredential<'a> {
    id: &'a str,
    secret: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    activation_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    join_key_id: Option<&'a str>,
    enrolled_at: &'a str,
}

impl<'a> From<&'a StoredRunnerCredential> for PersistedRunnerCredential<'a> {
    fn from(credential: &'a StoredRunnerCredential) -> Self {
        Self {
            id: &credential.id,
            secret: &credential.secret,
            activation_id: credential.activation_id.as_deref(),
            join_key_id: credential.join_key_id.as_deref(),
            enrolled_at: &credential.enrolled_at,
        }
    }
}

impl<'a> From<&'a RunnerState> for PersistedRunnerState<'a> {
    fn from(state: &'a RunnerState) -> Self {
        Self {
            schema_version: state.schema_version,
            runner_id: &state.runner_id,
            connection_url: &state.connection_url,
            current_credential: PersistedRunnerCredential::from(&state.current_credential),
            pending_credential: state
                .pending_credential
                .as_ref()
                .map(PersistedRunnerCredential::from),
            last_promotion: state.last_promotion.as_ref(),
            updated_at: &state.updated_at,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EnrollmentResponse {
    schema_version: u8,
    runner_id: String,
    runner_name: String,
    organization: EnrollmentOrganization,
    runner_pool: EnrollmentPool,
    credential_id: String,
    connection_url: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EnrollmentOrganization {
    id: String,
    display_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EnrollmentPool {
    id: String,
    name: String,
}

impl EnrollmentResponse {
    pub fn runner_id(&self) -> &str {
        &self.runner_id
    }

    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    pub fn runner_name(&self) -> &str {
        &self.runner_name
    }

    pub fn pool_name(&self) -> &str {
        &self.runner_pool.name
    }
}

#[derive(Debug)]
pub enum EnrollmentOutcome {
    Enrolled {
        response: EnrollmentResponse,
        replacement: bool,
        deployment: String,
    },
    ReplacementCredential {
        runner_id: String,
        credential_id: String,
        deployment: String,
    },
    Gone {
        activation_id: String,
        deployment: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplacementDisposition {
    Current,
    Pending,
    Missing,
}

pub fn write_activation_file(
    destination: &str,
    artifact: &ActivationArtifact,
) -> Result<(), EnrollmentError> {
    if destination == "-" {
        return artifact.write_json(&mut io::stdout().lock());
    }
    let file =
        PrivateArtifactFile::create(Path::new(destination)).map_err(|error| match error {
            PrivateArtifactError::UnsafeDirectory => EnrollmentError::UnsafePath(
                "activation artifact parent must be a non-symbolic-link directory",
            ),
            _ => EnrollmentError::ArtifactWrite,
        })?;
    let mut document = zeroize::Zeroizing::new(Vec::new());
    artifact.write_json(&mut *document)?;
    file.write(&document)
        .map_err(|_| EnrollmentError::ArtifactWrite)
}

/// A new file only the current user can read and write, which holds a
/// secret-bearing artifact that cannot be fetched again: the activation and
/// join key artifacts. `create` makes it empty, so a caller can refuse an
/// unusable destination before the secret is issued; `write` fills it once.
/// A file dropped before its content is written and synced is removed.
pub struct PrivateArtifactFile {
    path: PathBuf,
    file: File,
    written: bool,
}

impl PrivateArtifactFile {
    /// Creates the file at `path`, with mode `0600`, in a directory that is
    /// not a symbolic link. It never follows a symbolic link at `path` and
    /// never replaces an existing path.
    pub fn create(path: &Path) -> Result<Self, PrivateArtifactError> {
        let metadata =
            fs::symlink_metadata(artifact_parent(path)).map_err(PrivateArtifactError::Directory)?;
        if !metadata.file_type().is_dir() {
            return Err(PrivateArtifactError::UnsafeDirectory);
        }
        let file = create_new_private_file(path).map_err(PrivateArtifactError::Create)?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            written: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Writes `bytes` as the file's whole content and syncs the file and its
    /// directory. Once the content is synced the file is kept, even when its
    /// directory cannot be synced, since the secret it holds cannot be
    /// fetched again.
    pub fn write(mut self, bytes: &[u8]) -> Result<(), PrivateArtifactError> {
        self.file
            .write_all(bytes)
            .and_then(|()| self.file.sync_all())
            .map_err(PrivateArtifactError::Write)?;
        self.written = true;
        sync_directory_io(artifact_parent(&self.path)).map_err(PrivateArtifactError::SyncDirectory)
    }
}

impl Drop for PrivateArtifactFile {
    fn drop(&mut self) {
        if !self.written {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// The step at which a `PrivateArtifactFile` failed, with the I/O error
/// beneath it.
#[derive(Debug)]
pub enum PrivateArtifactError {
    Directory(io::Error),
    UnsafeDirectory,
    Create(io::Error),
    Write(io::Error),
    SyncDirectory(io::Error),
}

impl fmt::Display for PrivateArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Directory(_) => "inspect its directory",
            Self::UnsafeDirectory => "its directory must be a directory, not a symbolic link",
            Self::Create(_) => "create it",
            Self::Write(_) => "write it",
            Self::SyncDirectory(_) => "sync its directory",
        })
    }
}

impl Error for PrivateArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Directory(error)
            | Self::Create(error)
            | Self::Write(error)
            | Self::SyncDirectory(error) => Some(error),
            Self::UnsafeDirectory => None,
        }
    }
}

pub fn enroll(
    activation_file: Option<&Path>,
    config_path: &Path,
    replace_credential: bool,
    resume: bool,
) -> Result<EnrollmentOutcome, EnrollmentError> {
    if !resume && activation_file.is_none() {
        return Err(EnrollmentError::InvalidCommand);
    }
    if resume && (activation_file.is_some() || replace_credential) {
        return Err(EnrollmentError::InvalidCommand);
    }
    if activation_file == Some(Path::new("-")) && io::stdin().is_terminal() {
        return Err(EnrollmentError::TerminalStdin);
    }

    let LockedStateDirectory {
        mode,
        state_path,
        journal_path,
        _lock: lock,
    } = LockedStateDirectory::lock(config_path)?;

    let journal = if resume {
        read_journal(&journal_path)?
    } else {
        let artifact = read_activation_artifact_allow_expired(
            activation_file.ok_or(EnrollmentError::InvalidCommand)?,
            mode,
        )?;
        let existing_state = read_runner_state_if_present(&state_path, mode)?;
        match (&existing_state, replace_credential) {
            (Some(_), false) => return Err(EnrollmentError::StateAlreadyEnrolled),
            (None, true) => return Err(EnrollmentError::ReplacementStateMissing),
            (Some(state), true) => {
                if state.runner_id != artifact.runner_id {
                    return Err(EnrollmentError::RunnerMismatch);
                }
                let artifact_activation_id = activation_id(&artifact)?;
                let local_credential = if let Some(pending) = &state.pending_credential {
                    if !pending.activated_by(&artifact_activation_id) {
                        return Err(EnrollmentError::PendingCredentialExists);
                    }
                    Some(pending)
                } else if state.last_promotion.as_ref().is_some_and(|promotion| {
                    promotion.credential_id == state.current_credential.id
                        && promotion.activation_id == artifact_activation_id
                }) {
                    Some(&state.current_credential)
                } else {
                    None
                };
                if let Some(credential) = local_credential {
                    require_resolved_journal(&journal_path)?;
                    let outcome = EnrollmentOutcome::ReplacementCredential {
                        runner_id: state.runner_id.clone(),
                        credential_id: credential.id.clone(),
                        deployment: enrollment_deployment(&artifact.activation_url)?,
                    };
                    drop(lock);
                    return Ok(outcome);
                }
            }
            (None, false) => {}
        }
        require_unexpired(&artifact.expires_at, EnrollmentError::ExpiredArtifact)?;
        stage_journal(
            &journal_path,
            EnrollmentTarget::Activation(artifact),
            replace_credential,
        )?
    };

    let artifact = journal.activation_artifact(&journal_path)?;
    validate_journal(&journal, mode)?;
    if journal.replace_credential {
        validate_replacement_journal_local_state(&state_path, mode, &journal, artifact)?;
    }

    let deployment = enrollment_deployment(&artifact.activation_url)?;
    match send_enrollment(
        &journal,
        &artifact.activation_url,
        &artifact.activation_token,
    )? {
        EnrollmentHTTPOutcome::Success(response) => {
            validate_enrollment_response(&response, Some(&artifact.runner_id), mode)?;
            persist_enrollment_state(&state_path, &journal, &response, mode)?;
            remove_and_sync(&journal_path)?;
            drop(lock);
            Ok(EnrollmentOutcome::Enrolled {
                response,
                replacement: journal.replace_credential,
                deployment,
            })
        }
        EnrollmentHTTPOutcome::Gone => {
            let activation_id = activation_id(artifact)?;
            let receipt = TerminalReceipt {
                schema_version: 1,
                activation_id: activation_id.clone(),
                disposition: TerminalDisposition::Gone,
                observed_at: now_rfc3339()?,
            };
            atomic_write_json(&journal_path, &receipt)?;
            drop(lock);
            Ok(EnrollmentOutcome::Gone {
                activation_id,
                deployment,
            })
        }
    }
}

#[derive(Debug)]
pub enum JoinOutcome {
    /// The host already holds enrolled state, so it sent no join.
    AlreadyEnrolled,
    Joined(Box<EnrollmentResponse>),
}

/// Cloud's definite refusal of a join, a closed category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinRefusal {
    /// 401: the token is malformed or does not verify.
    Unauthorized,
    /// 410: the key is revoked or expired, its issuer may no longer issue a
    /// key for the pool, or the runner its exact repeat made is deleted.
    Gone,
    /// 409: the pool's member is not active, the organization is at its
    /// registration limit, or the idempotency key names another verifier.
    Conflict,
}

impl JoinRefusal {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Unauthorized => "join_key_unauthorized",
            Self::Gone => "join_key_gone",
            Self::Conflict => "join_conflict",
        }
    }
}

/// Why an enrollment or join outcome is ambiguous, a closed category. Cloud
/// may or may not have committed the request, so its journal is kept.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AmbiguousCause {
    /// No complete answer: building the client, sending the request or
    /// reading the response body failed.
    Network,
    /// 429: Cloud is limiting the rate of requests.
    RateLimited,
    /// A server error, with its status.
    CloudUnavailable(u16),
}

impl AmbiguousCause {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::RateLimited => "rate_limited",
            Self::CloudUnavailable(_) => "cloud_unavailable",
        }
    }
}

/// One resend of a join whose outcome was ambiguous: why, and how long the
/// join waits before it is sent again.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JoinRetry {
    pub cause: AmbiguousCause,
    pub delay: Duration,
}

/// Joins the key's pool when the host holds no enrolled state, and otherwise
/// leaves the state as it is and sends nothing. A staged join left by an
/// interrupted start is resent before anything else, and an ambiguous outcome
/// is resent with the service's reconnect backoff, so one host registers at
/// most one runner. Each such resend is handed to `observe_retry` with its
/// cause and delay before the delay is slept, still under the state lock. A
/// definite refusal clears the journal and says whether an earlier send can
/// have reached Cloud, except that a resent join whose token Cloud rejects,
/// whether an earlier start or this one sent it first, keeps it. The key, and
/// with it the token, is dropped when this returns.
pub fn join_unless_enrolled(
    config_path: &Path,
    key: JoinKeyArtifact,
    mut observe_retry: impl FnMut(JoinRetry),
) -> Result<JoinOutcome, EnrollmentError> {
    let host = LockedStateDirectory::lock(config_path)?;
    let mode = host.mode;
    let journal_path = &host.journal_path;

    if let Some(state) = read_runner_state_if_present(&host.state_path, mode)? {
        remove_persisted_join_journal(journal_path, &state)?;
        return Ok(JoinOutcome::AlreadyEnrolled);
    }

    let (journal, mut resent) = match read_private_file_if_present(journal_path, STATE_LIMIT)? {
        Some(bytes) => match serde_json::from_slice::<EnrollmentJournal>(&bytes) {
            Ok(journal) => (journal, true),
            Err(_) => {
                require_terminal_receipt(&bytes)?;
                (stage_join(journal_path, &key, mode)?, false)
            }
        },
        None => (stage_join(journal_path, &key, mode)?, false),
    };
    let EnrollmentTarget::Join(staged) = &journal.target else {
        return Err(EnrollmentError::UnresolvedJournal);
    };
    if staged.join_key_id != key.join_key_id {
        return Err(EnrollmentError::JoinJournalForAnotherKey {
            journal: journal_path.clone(),
            join_key_id: staged.join_key_id.clone(),
        });
    }
    validate_journal(&journal, mode)?;

    let mut backoff = Backoff::new();
    let response = loop {
        let refusal = match send_enrollment(&journal, &staged.join_url, &key.join_token) {
            Ok(EnrollmentHTTPOutcome::Success(response)) => break response,
            Ok(EnrollmentHTTPOutcome::Gone) => JoinRefusal::Gone,
            // An earlier send of this journal may have registered a runner,
            // so the journal stays for the join to be completed or cleaned up
            // deliberately.
            Err(EnrollmentError::Unauthorized) if resent => {
                return Err(EnrollmentError::StagedJoinUnauthorized {
                    journal: journal_path.clone(),
                    join_key_id: staged.join_key_id.clone(),
                });
            }
            Err(EnrollmentError::Unauthorized) => JoinRefusal::Unauthorized,
            Err(EnrollmentError::Conflict) => JoinRefusal::Conflict,
            Err(EnrollmentError::NetworkAmbiguous(cause)) => {
                resent = true;
                let delay = backoff.next_delay();
                observe_retry(JoinRetry { cause, delay });
                um_support::sleep(delay);
                continue;
            }
            Err(error) => return Err(error),
        };
        remove_and_sync(journal_path)?;
        return Err(EnrollmentError::JoinRefused { refusal, resent });
    };
    drop(key);
    validate_enrollment_response(&response, None, mode)?;
    persist_enrollment_state(&host.state_path, &journal, &response, mode)?;
    remove_and_sync(journal_path)?;
    drop(host);
    Ok(JoinOutcome::Joined(Box::new(response)))
}

/// Removes the journal of the host's own completed join, the one whose
/// credential the enrolled state already holds, under the state lock. A start
/// of an enrolled host calls it before serving, so the journal neither keeps a
/// second copy of the credential secret nor blocks a later enrollment. Any
/// other journal, and a host without enrolled state, are left as they are.
pub fn remove_completed_join_journal(config_path: &Path) -> Result<(), EnrollmentError> {
    let host = LockedStateDirectory::lock(config_path)?;
    if let Some(state) = read_runner_state_if_present(&host.state_path, host.mode)? {
        remove_persisted_join_journal(&host.journal_path, &state)?;
    }
    Ok(())
}

/// The operator configuration's state directory, held under its state lock
/// until this is dropped.
struct LockedStateDirectory {
    mode: DeploymentMode,
    state_path: PathBuf,
    journal_path: PathBuf,
    _lock: StateLock,
}

impl LockedStateDirectory {
    fn lock(config_path: &Path) -> Result<Self, EnrollmentError> {
        let config = load_operator_config(config_path)?;
        let state_path = config.runner_state_path;
        let state_directory = state_path
            .parent()
            .filter(|value| !value.as_os_str().is_empty())
            .ok_or(EnrollmentError::UnsafePath(
                "runner state path has no parent directory",
            ))?;
        ensure_private_directory(state_directory)?;
        let lock = acquire_state_lock(&state_directory.join(LOCK_FILE))?;
        Ok(Self {
            mode: config.deployment_mode,
            journal_path: state_directory.join(JOURNAL_FILE),
            state_path,
            _lock: lock,
        })
    }
}

fn stage_join(
    journal_path: &Path,
    key: &JoinKeyArtifact,
    mode: DeploymentMode,
) -> Result<EnrollmentJournal, EnrollmentError> {
    validate_join_url(&key.join_url, &key.join_key_id, mode)?;
    require_unexpired(&key.expires_at, EnrollmentError::ExpiredJoinKey)?;
    stage_journal(
        journal_path,
        EnrollmentTarget::Join(StagedJoinKey::from(key)),
        false,
    )
}

// A start interrupted after it wrote the joined state but before it removed the
// journal leaves a journal whose credential the state already holds; it is
// removed so it neither keeps a second copy of the secret nor blocks a later
// enrollment. Any other journal is left for its own resolution.
fn remove_persisted_join_journal(path: &Path, state: &RunnerState) -> Result<(), EnrollmentError> {
    let persisted = read_private_file_if_present(path, STATE_LIMIT)
        .ok()
        .flatten()
        .and_then(|bytes| serde_json::from_slice::<EnrollmentJournal>(&bytes).ok())
        .is_some_and(|journal| {
            matches!(journal.target, EnrollmentTarget::Join(_))
                && journal.credential_origin().ok() == state.current_credential.origin()
                && journal.credential_secret == state.current_credential.secret
        });
    if persisted {
        remove_and_sync(path)?;
    }
    Ok(())
}

// Validation requires the root-level /v1/runner-enrollments/.../activate or
// /v1/runner-join-keys/.../join path, so this origin is the API base that
// actually received enrollment. The caller's human-auth deployment may differ
// from the issued artifact's target.
fn enrollment_deployment(request_url: &str) -> Result<String, EnrollmentError> {
    let url = Url::parse(request_url).map_err(|_| EnrollmentError::InvalidJournal)?;
    Ok(url.origin().ascii_serialization())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnrollmentRequest<'a> {
    schema_version: u8,
    credential_secret_verifier: &'a str,
}

enum EnrollmentHTTPOutcome {
    Success(EnrollmentResponse),
    Gone,
}

// send_enrollment posts the journal's staged request, the same body for an
// activation and a join, to `url` with the capability `token` as bearer.
fn send_enrollment(
    journal: &EnrollmentJournal,
    url: &str,
    token: &str,
) -> Result<EnrollmentHTTPOutcome, EnrollmentError> {
    um_support::install_provider();
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|_| EnrollmentError::NetworkAmbiguous(AmbiguousCause::Network))?;
    let request = EnrollmentRequest {
        schema_version: 1,
        credential_secret_verifier: &journal.credential_secret_verifier,
    };
    let body = serde_json::to_vec(&request).map_err(|_| EnrollmentError::InvalidJournal)?;
    let mut response = client
        .post(url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Idempotency-Key", &journal.idempotency_key)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .map_err(|_| EnrollmentError::NetworkAmbiguous(AmbiguousCause::Network))?;
    let status = response.status();
    if status == StatusCode::GONE {
        return Ok(EnrollmentHTTPOutcome::Gone);
    }
    if status == StatusCode::UNAUTHORIZED {
        return Err(EnrollmentError::Unauthorized);
    }
    if status == StatusCode::CONFLICT {
        return Err(EnrollmentError::Conflict);
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err(EnrollmentError::NetworkAmbiguous(
            AmbiguousCause::RateLimited,
        ));
    }
    if status.is_server_error() {
        return Err(EnrollmentError::NetworkAmbiguous(
            AmbiguousCause::CloudUnavailable(status.as_u16()),
        ));
    }
    if status != StatusCode::CREATED {
        return Err(EnrollmentError::InvalidResponse);
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut response)
        .take(RESPONSE_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| EnrollmentError::NetworkAmbiguous(AmbiguousCause::Network))?;
    if bytes.len() as u64 > RESPONSE_LIMIT {
        return Err(EnrollmentError::InvalidResponse);
    }
    let decoded = serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidResponse)?;
    Ok(EnrollmentHTTPOutcome::Success(decoded))
}

pub(crate) fn load_runner_service_configuration(
    path: &Path,
) -> Result<RunnerServiceConfiguration, EnrollmentError> {
    let config = load_operator_config(path)?;
    let (lock, state) = load_locked_runner_state(&config)?;
    drop(lock);

    let pending_credential = state
        .pending_credential
        .map(|credential| PendingCredential {
            runner_id: state.runner_id.clone(),
            connection_url: state.connection_url.clone(),
            credential_id: credential.id,
            credential_secret: credential.secret,
        });
    Ok(RunnerServiceConfiguration {
        runner_id: state.runner_id,
        connection_url: state.connection_url,
        credential_id: state.current_credential.id,
        credential_secret: state.current_credential.secret,
        pending_credential,
        state_access: RunnerStateAccess {
            path: config.runner_state_path,
            deployment_mode: config.deployment_mode,
        },
        control_socket_path: config.control_socket_path,
        work_root: config.work_root,
    })
}

pub fn load_control_socket_path(path: &Path) -> Result<PathBuf, EnrollmentError> {
    Ok(load_operator_config(path)?.control_socket_path)
}

/// The protected state path, `runnerStatePath`, from the operator
/// configuration, read without taking the state lock or reading the state.
pub(crate) fn load_runner_state_path(path: &Path) -> Result<PathBuf, EnrollmentError> {
    Ok(load_operator_config(path)?.runner_state_path)
}

/// The work root, `workRoot`, from the operator configuration, read without
/// taking the state lock or reading the state.
pub(crate) fn load_work_root(path: &Path) -> Result<PathBuf, EnrollmentError> {
    Ok(load_operator_config(path)?.work_root)
}

pub fn replacement_disposition(
    config_path: &Path,
    expected_runner_id: &str,
    expected_credential_id: &str,
) -> Result<ReplacementDisposition, EnrollmentError> {
    let config = load_operator_config(config_path)?;
    let (lock, state) = load_locked_runner_state(&config)?;
    if state.runner_id != expected_runner_id {
        return Err(EnrollmentError::RunnerMismatch);
    }
    let disposition = if state.current_credential.id == expected_credential_id {
        ReplacementDisposition::Current
    } else if state
        .pending_credential
        .as_ref()
        .is_some_and(|credential| credential.id == expected_credential_id)
    {
        ReplacementDisposition::Pending
    } else {
        ReplacementDisposition::Missing
    };
    drop(lock);
    Ok(disposition)
}

fn load_locked_runner_state(
    config: &OperatorConfig,
) -> Result<(StateLock, RunnerState), EnrollmentError> {
    let state_directory = config
        .runner_state_path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .ok_or(EnrollmentError::InvalidConfig)?;
    ensure_private_directory(state_directory)?;
    let lock = acquire_state_lock(&state_directory.join(LOCK_FILE))?;
    let state = read_runner_state_if_present(&config.runner_state_path, config.deployment_mode)?
        .ok_or(EnrollmentError::InvalidState)?;
    Ok((lock, state))
}

impl RunnerStateAccess {
    /// The protected state file, `runnerStatePath`.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn load_pending(
        &self,
        expected_runner_id: &str,
    ) -> Result<Option<PendingCredential>, ReloadStateError> {
        let directory = self.path.parent().ok_or(ReloadStateError::StateUpdate)?;
        ensure_private_directory(directory).map_err(|_| ReloadStateError::StateUpdate)?;
        let lock = acquire_state_lock(&directory.join(LOCK_FILE))
            .map_err(|_| ReloadStateError::StateUpdate)?;
        let state = read_runner_state_if_present(&self.path, self.deployment_mode)
            .map_err(|_| ReloadStateError::StateUpdate)?
            .ok_or(ReloadStateError::StateUpdate)?;
        drop(lock);
        if state.runner_id != expected_runner_id {
            return Err(ReloadStateError::RegistrationMismatch);
        }
        Ok(state
            .pending_credential
            .map(|credential| PendingCredential {
                runner_id: state.runner_id,
                connection_url: state.connection_url,
                credential_id: credential.id,
                credential_secret: credential.secret,
            }))
    }

    pub(crate) fn promote(
        &self,
        expected_runner_id: &str,
        expected_current_credential_id: &str,
        expected_pending_credential_id: &str,
    ) -> Result<(), ReloadStateError> {
        let directory = self.path.parent().ok_or(ReloadStateError::StateUpdate)?;
        ensure_private_directory(directory).map_err(|_| ReloadStateError::StateUpdate)?;
        let lock = acquire_state_lock(&directory.join(LOCK_FILE))
            .map_err(|_| ReloadStateError::StateUpdate)?;
        let mut state = read_runner_state_if_present(&self.path, self.deployment_mode)
            .map_err(|_| ReloadStateError::StateUpdate)?
            .ok_or(ReloadStateError::StateUpdate)?;
        if state.runner_id != expected_runner_id {
            return Err(ReloadStateError::RegistrationMismatch);
        }
        if state.current_credential.id != expected_current_credential_id {
            return Err(ReloadStateError::StateUpdate);
        }
        let pending = state
            .pending_credential
            .take()
            .filter(|credential| credential.id == expected_pending_credential_id)
            .ok_or(ReloadStateError::StateUpdate)?;
        let activation_id = pending
            .activation_id
            .clone()
            .ok_or(ReloadStateError::StateUpdate)?;
        let promoted_at = now_rfc3339().map_err(|_| ReloadStateError::StateUpdate)?;
        state.last_promotion = Some(CredentialPromotion {
            credential_id: pending.id.clone(),
            activation_id,
            promoted_at: promoted_at.clone(),
        });
        state.current_credential = pending;
        state.updated_at = promoted_at;
        atomic_write_json(&self.path, &PersistedRunnerState::from(&state))
            .map_err(|_| ReloadStateError::StateUpdate)?;
        drop(lock);
        Ok(())
    }
}

fn load_operator_config(path: &Path) -> Result<OperatorConfig, EnrollmentError> {
    let bytes = read_bounded_regular_file(path, CONFIG_LIMIT, false)
        .map_err(|_| EnrollmentError::InvalidConfig)?;
    let config: OperatorConfig =
        serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidConfig)?;
    if config.schema_version != 1
        || !config.runner_state_path.is_absolute()
        || !config.control_socket_path.is_absolute()
        || !config.work_root.is_absolute()
    {
        return Err(EnrollmentError::InvalidConfig);
    }
    Ok(config)
}

#[cfg(test)]
fn read_activation_artifact(
    path: &Path,
    mode: DeploymentMode,
) -> Result<ActivationArtifact, EnrollmentError> {
    let artifact = read_activation_artifact_allow_expired(path, mode)?;
    require_unexpired(&artifact.expires_at, EnrollmentError::ExpiredArtifact)?;
    Ok(artifact)
}

fn read_activation_artifact_allow_expired(
    path: &Path,
    mode: DeploymentMode,
) -> Result<ActivationArtifact, EnrollmentError> {
    let bytes = if path == Path::new("-") {
        read_piped_artifact().map_err(|_| EnrollmentError::InvalidArtifact)?
    } else {
        read_bounded_regular_file(path, ARTIFACT_LIMIT, true)
            .map_err(|_| EnrollmentError::InvalidArtifact)?
    };
    if bytes.len() as u64 > ARTIFACT_LIMIT {
        return Err(EnrollmentError::InvalidArtifact);
    }
    let artifact = serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidArtifact)?;
    validate_artifact(&artifact, mode)?;
    Ok(artifact)
}

// The bound admits one byte over the limit so the caller can refuse it.
fn read_piped_artifact() -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    io::stdin()
        .lock()
        .take(ARTIFACT_LIMIT + 1)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn require_unexpired(expires_at: &str, expired: EnrollmentError) -> Result<(), EnrollmentError> {
    if parse_rfc3339(expires_at).is_none_or(|expires_at| expires_at <= um_support::utc_now()) {
        return Err(expired);
    }
    Ok(())
}

fn validate_artifact(
    artifact: &ActivationArtifact,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    if artifact.schema_version != 1
        || !valid_typed_id(&artifact.runner_id, "rnr_")
        || parse_rfc3339(&artifact.expires_at).is_none()
    {
        return Err(EnrollmentError::InvalidArtifact);
    }
    let activation_id = activation_id(artifact)?;
    let url = validate_cloud_url(&artifact.activation_url, mode, CloudURLKind::Activation)?;
    if url.path() != format!("/v1/runner-enrollments/{activation_id}/activate") {
        return Err(EnrollmentError::InvalidArtifact);
    }
    Ok(())
}

fn activation_id(artifact: &ActivationArtifact) -> Result<String, EnrollmentError> {
    bearer_capability_id(&artifact.activation_token, "rna_").ok_or(EnrollmentError::InvalidArtifact)
}

// bearer_capability_id returns the typed id of a `<prefix><id>.<secret>`
// capability token whose secret encodes 32 bytes.
fn bearer_capability_id(token: &str, prefix: &str) -> Option<String> {
    if token.len() > 256 {
        return None;
    }
    let (id, secret) = token.split_once('.')?;
    if secret.contains('.') || !valid_typed_id(id, prefix) || !valid_secret(secret) {
        return None;
    }
    Some(id.to_owned())
}

fn validate_join_url(
    join_url: &str,
    join_key_id: &str,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    let url = validate_cloud_url(join_url, mode, CloudURLKind::Activation)?;
    if url.path() != format!("/v1/runner-join-keys/{join_key_id}/join") {
        return Err(EnrollmentError::InvalidJoinKey);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum CloudURLKind {
    Activation,
    Connection,
}

fn validate_cloud_url(
    value: &str,
    mode: DeploymentMode,
    kind: CloudURLKind,
) -> Result<Url, EnrollmentError> {
    let url = Url::parse(value).map_err(|_| EnrollmentError::InvalidURL)?;
    if url_authority_has_user_info(value)
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
        || url.host_str().is_none()
    {
        return Err(EnrollmentError::InvalidURL);
    }
    let (secure, insecure) = match kind {
        CloudURLKind::Activation => ("https", "http"),
        CloudURLKind::Connection => ("wss", "ws"),
    };
    if url.scheme() == secure {
        return Ok(url);
    }
    if mode == DeploymentMode::Development
        && url.scheme() == insecure
        && exact_loopback_host(value, &url)
    {
        return Ok(url);
    }
    Err(EnrollmentError::InvalidURL)
}

fn url_authority_has_user_info(value: &str) -> bool {
    value
        .split_once("://")
        .and_then(|(_, remainder)| remainder.split(['/', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'))
}

fn exact_loopback_host(value: &str, url: &Url) -> bool {
    let Some(raw_host) = raw_url_host(value) else {
        return false;
    };
    match url.host() {
        Some(url::Host::Domain(host)) => host == "localhost" && raw_host == "localhost",
        Some(url::Host::Ipv4(address)) => address.is_loopback() && raw_host == address.to_string(),
        Some(url::Host::Ipv6(address)) => address.is_loopback() && raw_host == "[::1]",
        None => false,
    }
}

fn raw_url_host(value: &str) -> Option<&str> {
    let (_, remainder) = value.split_once("://")?;
    let authority = remainder.split(['/', '?', '#']).next()?;
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    if authority.starts_with('[') {
        let closing = authority.find(']')?;
        return Some(&authority[..=closing]);
    }
    Some(
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host),
    )
}

// A join names no runner in advance, so it expects none.
fn validate_enrollment_response(
    response: &EnrollmentResponse,
    expected_runner_id: Option<&str>,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    if response.schema_version != 1
        || expected_runner_id.is_some_and(|runner_id| response.runner_id != runner_id)
        || !valid_typed_id(&response.runner_id, "rnr_")
        || !valid_typed_id(&response.organization.id, "org_")
        || !valid_typed_id(&response.runner_pool.id, "rpl_")
        || !valid_typed_id(&response.credential_id, "rrc_")
        || !valid_name(&response.runner_name)
        || !valid_name(&response.runner_pool.name)
        || response.organization.display_name.is_empty()
    {
        return Err(EnrollmentError::InvalidResponse);
    }
    let url = validate_cloud_url(&response.connection_url, mode, CloudURLKind::Connection)?;
    if url.path() != "/v1/runner/connect" {
        return Err(EnrollmentError::InvalidResponse);
    }
    Ok(())
}

fn stage_journal(
    path: &Path,
    target: EnrollmentTarget,
    replace_credential: bool,
) -> Result<EnrollmentJournal, EnrollmentError> {
    require_resolved_journal(path)?;
    let mut secret_bytes = [0_u8; 32];
    getrandom::fill(&mut secret_bytes).map_err(|_| EnrollmentError::EntropyUnavailable)?;
    let credential_secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret_bytes);
    let verifier = digest(&SHA256, &secret_bytes);
    let credential_secret_verifier =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(verifier.as_ref());
    let journal = EnrollmentJournal {
        schema_version: 1,
        target,
        credential_secret,
        credential_secret_verifier,
        idempotency_key: generate_idempotency_key()
            .map_err(|_| EnrollmentError::EntropyUnavailable)?,
        replace_credential,
        staged_at: now_rfc3339()?,
    };
    atomic_write_json(path, &PersistedEnrollmentJournal::from(&journal))?;
    Ok(journal)
}

fn require_resolved_journal(path: &Path) -> Result<(), EnrollmentError> {
    let Some(bytes) = read_private_file_if_present(path, STATE_LIMIT)? else {
        return Ok(());
    };
    require_terminal_receipt(&bytes).map_err(|error| {
        match serde_json::from_slice::<EnrollmentJournal>(&bytes) {
            Ok(journal) => journal.activation_artifact(path).err().unwrap_or(error),
            Err(_) => error,
        }
    })
}

fn require_terminal_receipt(bytes: &[u8]) -> Result<(), EnrollmentError> {
    let receipt = serde_json::from_slice::<TerminalReceipt>(bytes)
        .map_err(|_| EnrollmentError::UnresolvedJournal)?;
    if !valid_terminal_receipt(&receipt) {
        return Err(EnrollmentError::UnresolvedJournal);
    }
    Ok(())
}

fn read_journal(path: &Path) -> Result<EnrollmentJournal, EnrollmentError> {
    let bytes = read_private_file_if_present(path, STATE_LIMIT)
        .map_err(|_| EnrollmentError::InvalidJournal)?
        .ok_or(EnrollmentError::MissingJournal)?;
    serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidJournal)
}

fn validate_journal(
    journal: &EnrollmentJournal,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    match &journal.target {
        EnrollmentTarget::Activation(artifact) => validate_artifact(artifact, mode)?,
        EnrollmentTarget::Join(join_key) => {
            if journal.replace_credential
                || !valid_typed_id(&join_key.join_key_id, "rjk_")
                || parse_rfc3339(&join_key.expires_at).is_none()
            {
                return Err(EnrollmentError::InvalidJournal);
            }
            validate_join_url(&join_key.join_url, &join_key.join_key_id, mode)?;
        }
    }
    if journal.schema_version != 1
        || !valid_secret(&journal.credential_secret)
        || !valid_secret(&journal.credential_secret_verifier)
        || !journal_verifier_matches_secret(journal)
        || journal.idempotency_key.is_empty()
        || journal.idempotency_key.len() > 255
        || !journal
            .idempotency_key
            .bytes()
            .all(|byte| byte.is_ascii_graphic())
        || parse_rfc3339(&journal.staged_at).is_none()
    {
        return Err(EnrollmentError::InvalidJournal);
    }
    Ok(())
}

fn validate_replacement_journal_local_state(
    path: &Path,
    mode: DeploymentMode,
    journal: &EnrollmentJournal,
    artifact: &ActivationArtifact,
) -> Result<(), EnrollmentError> {
    let state = read_runner_state_if_present(path, mode)?
        .ok_or(EnrollmentError::ReplacementStateMissing)?;
    if state.runner_id != artifact.runner_id {
        return Err(EnrollmentError::RunnerMismatch);
    }
    let activation_id = activation_id(artifact)?;
    let staged = state.pending_credential.as_ref().or_else(|| {
        state
            .last_promotion
            .as_ref()
            .filter(|promotion| promotion.activation_id == activation_id)
            .map(|_| &state.current_credential)
    });
    if staged.is_some_and(|credential| {
        !credential.activated_by(&activation_id) || credential.secret != journal.credential_secret
    }) {
        return Err(EnrollmentError::PendingCredentialExists);
    }
    Ok(())
}

fn persist_enrollment_state(
    path: &Path,
    journal: &EnrollmentJournal,
    response: &EnrollmentResponse,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    let origin = journal.credential_origin()?;
    let existing = read_runner_state_if_present(path, mode)?;
    if existing
        .as_ref()
        .is_some_and(|state| enrollment_already_persisted(state, journal, response, &origin))
    {
        return Ok(());
    }

    let enrolled_at = now_rfc3339()?;
    let credential = StoredRunnerCredential::new(
        response.credential_id.clone(),
        journal.credential_secret.clone(),
        origin,
        enrolled_at.clone(),
    );
    let state = if journal.replace_credential {
        let mut state = existing.ok_or(EnrollmentError::ReplacementStateMissing)?;
        if state.runner_id != response.runner_id {
            return Err(EnrollmentError::RunnerMismatch);
        }
        if state.pending_credential.is_some() {
            return Err(EnrollmentError::PendingCredentialExists);
        }
        state.connection_url.clone_from(&response.connection_url);
        state.pending_credential = Some(credential);
        state.updated_at = enrolled_at;
        state
    } else {
        if existing.is_some() {
            return Err(EnrollmentError::StateAlreadyEnrolled);
        }
        RunnerState {
            schema_version: 1,
            runner_id: response.runner_id.clone(),
            connection_url: response.connection_url.clone(),
            current_credential: credential,
            pending_credential: None,
            last_promotion: None,
            updated_at: enrolled_at,
        }
    };
    atomic_write_json(path, &PersistedRunnerState::from(&state))
}

fn enrollment_already_persisted(
    state: &RunnerState,
    journal: &EnrollmentJournal,
    response: &EnrollmentResponse,
    origin: &CredentialOrigin,
) -> bool {
    if state.runner_id != response.runner_id || state.connection_url != response.connection_url {
        return false;
    }
    let persisted = if journal.replace_credential {
        state.pending_credential.as_ref().or_else(|| {
            state
                .last_promotion
                .as_ref()
                .filter(|promotion| {
                    promotion.credential_id == response.credential_id
                        && *origin == CredentialOrigin::Activation(promotion.activation_id.clone())
                })
                .map(|_| &state.current_credential)
        })
    } else if state.pending_credential.is_none() {
        Some(&state.current_credential)
    } else {
        None
    };
    persisted.is_some_and(|credential| {
        credential.id == response.credential_id
            && credential.secret == journal.credential_secret
            && credential.origin().as_ref() == Some(origin)
    })
}

fn read_runner_state_if_present(
    path: &Path,
    mode: DeploymentMode,
) -> Result<Option<RunnerState>, EnrollmentError> {
    let Some(bytes) = read_private_file_if_present(path, STATE_LIMIT)
        .map_err(|_| EnrollmentError::InvalidState)?
    else {
        return Ok(None);
    };
    let state: RunnerState =
        serde_json::from_slice(&bytes).map_err(|_| EnrollmentError::InvalidState)?;
    if state.schema_version != 1
        || !valid_typed_id(&state.runner_id, "rnr_")
        || !valid_stored_credential(&state.current_credential)
        || state.pending_credential.as_ref().is_some_and(|credential| {
            !valid_stored_credential(credential) || credential.id == state.current_credential.id
        })
        || state.last_promotion.as_ref().is_some_and(|promotion| {
            promotion.credential_id != state.current_credential.id
                || !state
                    .current_credential
                    .activated_by(&promotion.activation_id)
                || parse_rfc3339(&promotion.promoted_at).is_none()
        })
        || parse_rfc3339(&state.updated_at).is_none()
    {
        return Err(EnrollmentError::InvalidState);
    }
    let url = validate_cloud_url(&state.connection_url, mode, CloudURLKind::Connection)?;
    if url.path() != "/v1/runner/connect" {
        return Err(EnrollmentError::InvalidState);
    }
    Ok(Some(state))
}

fn valid_stored_credential(credential: &StoredRunnerCredential) -> bool {
    valid_typed_id(&credential.id, "rrc_")
        && valid_secret(&credential.secret)
        && credential.origin().is_some()
        && parse_rfc3339(&credential.enrolled_at).is_some()
}

fn journal_verifier_matches_secret(journal: &EnrollmentJournal) -> bool {
    let Ok(secret) =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&journal.credential_secret)
    else {
        return false;
    };
    let verifier = digest(&SHA256, &secret);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(verifier.as_ref())
        == journal.credential_secret_verifier
}

fn valid_terminal_receipt(receipt: &TerminalReceipt) -> bool {
    receipt.schema_version == 1
        && valid_typed_id(&receipt.activation_id, "rna_")
        && matches!(&receipt.disposition, TerminalDisposition::Gone)
        && parse_rfc3339(&receipt.observed_at).is_some()
}

/// Writes one of the runner's protected host documents beside its state.
pub(crate) fn atomic_write_json(
    path: &Path,
    value: &impl Serialize,
) -> Result<(), EnrollmentError> {
    let bytes = json_document(value).map_err(|_| EnrollmentError::StateWrite)?;
    write_private_file_atomically(
        path,
        &bytes,
        STATE_TEMPORARY_PREFIX,
        ensure_destination_safe,
        |_| EnrollmentError::StateWrite,
    )
}

/// `atomic_write_json` through temporary files named from
/// `temporary_prefix`, answering the I/O error that stopped it, for a caller
/// that classifies it: a destination that is not a private file of the
/// effective user is `PermissionDenied`, a value that cannot be encoded
/// `InvalidData`.
pub(crate) fn write_private_json(
    path: &Path,
    value: &impl Serialize,
    temporary_prefix: &str,
) -> io::Result<()> {
    let bytes = json_document(value)?;
    write_private_file_atomically(
        path,
        &bytes,
        temporary_prefix,
        ensure_private_destination,
        |error| error,
    )
}

fn json_document(value: &impl Serialize) -> serde_json::Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Replaces `path` with `bytes` in one step, the private-file write the
/// runner's state documents and an installed agent login share: a new `0600`
/// temporary file in the same directory, created without following a
/// symbolic link and named from `temporary_prefix`, is written and synced,
/// `check_destination` is asked again about the destination, and the file is
/// renamed over it and the directory synced. `check_destination` is asked
/// once before the temporary file exists as well. A failure at any step
/// removes the temporary file, so the destination keeps its earlier content
/// whole; an I/O failure is reported as `write_failure(error)`, a path
/// without a parent as `InvalidInput`.
pub(crate) fn write_private_file_atomically<E>(
    path: &Path,
    bytes: &[u8],
    temporary_prefix: &str,
    check_destination: impl Fn(&Path) -> Result<(), E>,
    write_failure: impl Fn(io::Error) -> E,
) -> Result<(), E> {
    let parent = path
        .parent()
        .ok_or_else(|| write_failure(io::ErrorKind::InvalidInput.into()))?;
    check_destination(path)?;
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(
        "{temporary_prefix}.{}.{sequence}",
        std::process::id()
    ));
    let mut file = create_new_private_file(&temporary).map_err(&write_failure)?;
    let result = (|| {
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(&write_failure)?;
        drop(file);
        check_destination(path)?;
        fs::rename(&temporary, path).map_err(&write_failure)?;
        sync_directory_io(parent).map_err(&write_failure)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// Removes from `directory` the temporary files that writes through
/// `write_private_file_atomically` with one of `prefixes` left when their
/// process was killed between creating the file and the rename: each regular
/// file of the effective user named `<prefix>.<pid>.<sequence>`. A link is
/// never followed or removed, and nothing else is touched. For a caller that
/// is the only writer with those prefixes, while it writes nothing; a file
/// that cannot be removed is left for its next call.
pub(crate) fn remove_leftover_temporary_files(directory: &Path, prefixes: &[&str]) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    let owner = rustix::process::geteuid().as_raw();
    let mut removed = false;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let leftover = name
            .to_str()
            .is_some_and(|name| prefixes.iter().any(|prefix| temporary_of(name, prefix)));
        // `DirEntry::metadata` does not follow a link.
        if leftover
            && entry
                .metadata()
                .is_ok_and(|metadata| metadata.file_type().is_file() && metadata.uid() == owner)
            && fs::remove_file(entry.path()).is_ok()
        {
            removed = true;
        }
    }
    if removed {
        let _ = sync_directory_io(directory);
    }
}

/// Whether `name` is a temporary file name `write_private_file_atomically`
/// makes from `prefix`.
fn temporary_of(name: &str, prefix: &str) -> bool {
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    name.strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('.'))
        .and_then(|rest| rest.split_once('.'))
        .is_some_and(|(pid, sequence)| digits(pid) && digits(sequence))
}

fn remove_and_sync(path: &Path) -> Result<(), EnrollmentError> {
    let parent = path.parent().ok_or(EnrollmentError::StateWrite)?;
    fs::remove_file(path).map_err(|_| EnrollmentError::StateWrite)?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), EnrollmentError> {
    sync_directory_io(path).map_err(|_| EnrollmentError::StateWrite)
}

pub(crate) fn sync_directory_io(path: &Path) -> io::Result<()> {
    File::open(path).and_then(|directory| directory.sync_all())
}

fn acquire_state_lock(path: &Path) -> Result<StateLock, EnrollmentError> {
    let file = open_or_create_private_file(path).map_err(|_| EnrollmentError::StateLock)?;
    let start = um_support::monotonic_now();
    loop {
        match FileExt::try_lock(&file) {
            Ok(()) => return Ok(StateLock { file }),
            Err(TryLockError::WouldBlock) if um_support::elapsed(start) < LOCK_TIMEOUT => {
                um_support::sleep(LOCK_RETRY);
            }
            Err(TryLockError::WouldBlock | TryLockError::Error(_)) => {
                return Err(EnrollmentError::StateLock);
            }
        }
    }
}

struct StateLock {
    file: File,
}

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn artifact_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn ensure_private_directory(path: &Path) -> Result<(), EnrollmentError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_directory(&metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = DirBuilder::new();
            builder.mode(DIRECTORY_MODE);
            builder
                .create(path)
                .map_err(|_| EnrollmentError::UnsafePath("cannot create runner state directory"))?;
            fs::set_permissions(path, Permissions::from_mode(DIRECTORY_MODE)).map_err(|_| {
                EnrollmentError::UnsafePath("cannot protect runner state directory")
            })?;
            let metadata = fs::symlink_metadata(path).map_err(|_| {
                EnrollmentError::UnsafePath("cannot inspect runner state directory")
            })?;
            validate_private_directory(&metadata)
        }
        Err(_) => Err(EnrollmentError::UnsafePath(
            "cannot inspect runner state directory",
        )),
    }
}

fn validate_private_directory(metadata: &Metadata) -> Result<(), EnrollmentError> {
    if !metadata.file_type().is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o7777 != DIRECTORY_MODE
    {
        return Err(EnrollmentError::UnsafePath(
            "runner state directory must be owned by the current user with mode 0700",
        ));
    }
    Ok(())
}

fn validate_private_file(metadata: &Metadata) -> Result<(), EnrollmentError> {
    private_file(metadata).map_err(EnrollmentError::from)
}

fn private_file(metadata: &Metadata) -> Result<(), PrivateFileFailure> {
    if !metadata.file_type().is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o7777 != FILE_MODE
        || metadata.nlink() != 1
    {
        return Err(PrivateFileFailure::refused(
            "runner state file must be owned by the current user with mode 0600",
            io::ErrorKind::PermissionDenied,
        ));
    }
    Ok(())
}

/// A read or check of one of the runner's private files that failed: the
/// requirement enrollment reports, and the I/O error beneath it that the
/// agent-login readiness record classifies. A file that is not a private
/// regular file of the effective user is `PermissionDenied`; one over its
/// size bound is `InvalidData`.
#[derive(Debug)]
pub(crate) struct PrivateFileFailure {
    requirement: &'static str,
    error: io::Error,
}

impl PrivateFileFailure {
    /// The failure of an I/O call that stopped the read, which enrollment
    /// reports as `requirement`.
    fn new(requirement: &'static str) -> impl FnOnce(io::Error) -> Self {
        move |error| Self { requirement, error }
    }

    /// A file the runner refuses to read, as an error of `kind`.
    fn refused(requirement: &'static str, kind: io::ErrorKind) -> Self {
        Self::new(requirement)(kind.into())
    }
}

impl From<PrivateFileFailure> for EnrollmentError {
    fn from(failure: PrivateFileFailure) -> Self {
        Self::UnsafePath(failure.requirement)
    }
}

impl From<PrivateFileFailure> for io::Error {
    fn from(failure: PrivateFileFailure) -> Self {
        failure.error
    }
}

// Enrollment state and human OAuth storage intentionally translate the same
// no-follow private-file primitive into different domain error contracts.
fn create_new_private_file(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(FILE_MODE)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)?;
    file.set_permissions(Permissions::from_mode(FILE_MODE))?;
    Ok(file)
}

fn open_or_create_private_file(path: &Path) -> Result<File, EnrollmentError> {
    match create_new_private_file(path) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path).map_err(|_| EnrollmentError::StateLock)?;
            validate_private_file(&metadata)?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(NOFOLLOW_FLAG)
                .open(path)
                .map_err(|_| EnrollmentError::StateLock)?;
            validate_private_file(&file.metadata().map_err(|_| EnrollmentError::StateLock)?)?;
            Ok(file)
        }
        Err(_) => Err(EnrollmentError::StateLock),
    }
}

fn ensure_destination_safe(path: &Path) -> Result<(), EnrollmentError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_file(&metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(EnrollmentError::StateWrite),
    }
}

fn ensure_private_destination(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => private_file(&metadata).map_err(io::Error::from),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Reads one of the runner's protected host documents, `None` when it is
/// absent.
pub(crate) fn read_private_file_if_present(
    path: &Path,
    limit: u64,
) -> Result<Option<Vec<u8>>, EnrollmentError> {
    read_private_document(path, limit).map_err(EnrollmentError::from)
}

/// `read_private_file_if_present` keeping the I/O error beneath a failure,
/// for a caller that classifies it.
pub(crate) fn read_private_document(
    path: &Path,
    limit: u64,
) -> Result<Option<Vec<u8>>, PrivateFileFailure> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            private_file(&metadata)?;
            let bytes = read_bounded_regular_file(path, limit, true)?;
            Ok(Some(bytes))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(PrivateFileFailure::new("cannot inspect runner state file")(
            error,
        )),
    }
}

fn read_bounded_regular_file(
    path: &Path,
    limit: u64,
    private: bool,
) -> Result<Vec<u8>, PrivateFileFailure> {
    const OVER_LIMIT: &str = "input file exceeds its size limit";
    let metadata =
        fs::symlink_metadata(path).map_err(PrivateFileFailure::new("cannot inspect input file"))?;
    if !metadata.file_type().is_file() {
        return Err(PrivateFileFailure::refused(
            "input must be a regular non-symbolic-link file",
            io::ErrorKind::PermissionDenied,
        ));
    }
    if private {
        private_file(&metadata)?;
    }
    if metadata.len() > limit {
        return Err(PrivateFileFailure::refused(
            OVER_LIMIT,
            io::ErrorKind::InvalidData,
        ));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
        .map_err(PrivateFileFailure::new("cannot open input file"))?;
    let opened = file
        .metadata()
        .map_err(PrivateFileFailure::new("cannot inspect opened input file"))?;
    if !opened.file_type().is_file() || private && private_file(&opened).is_err() {
        return Err(PrivateFileFailure::refused(
            "opened input file is unsafe",
            io::ErrorKind::PermissionDenied,
        ));
    }
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(PrivateFileFailure::new("cannot read input file"))?;
    if bytes.len() as u64 > limit {
        return Err(PrivateFileFailure::refused(
            OVER_LIMIT,
            io::ErrorKind::InvalidData,
        ));
    }
    Ok(bytes)
}

fn valid_secret(value: &str) -> bool {
    valid_secret_syntax(value)
        && base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value)
            .is_ok_and(|decoded| decoded.len() == 32)
}

pub(crate) fn parse_rfc3339(value: &str) -> Option<time::OffsetDateTime> {
    time::OffsetDateTime::parse(value, &Rfc3339).ok()
}

fn valid_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

fn now_rfc3339() -> Result<String, EnrollmentError> {
    um_support::utc_now()
        .format(&Rfc3339)
        .map_err(|_| EnrollmentError::StateWrite)
}

#[derive(Debug)]
pub enum EnrollmentError {
    InvalidCommand,
    TerminalStdin,
    InvalidConfig,
    InvalidArtifact,
    ExpiredArtifact,
    InvalidURL,
    ArtifactWrite,
    UnsafePath(&'static str),
    StateLock,
    UnresolvedJournal,
    MissingJournal,
    InvalidJournal,
    EntropyUnavailable,
    StateAlreadyEnrolled,
    ReplacementStateMissing,
    PendingCredentialExists,
    RunnerMismatch,
    InvalidState,
    StateWrite,
    NetworkAmbiguous(AmbiguousCause),
    Unauthorized,
    Conflict,
    InvalidResponse,
    InvalidJoinKey,
    JoinKeyFile,
    ExpiredJoinKey,
    /// Enrollment found a join journal, which only a start with its key
    /// resolves.
    UnresolvedJoin {
        journal: PathBuf,
        join_key_id: String,
    },
    /// A start's join key is not the one its host's staged join was written
    /// for. The staged request may have registered a runner, so another key
    /// does not supersede it.
    JoinJournalForAnotherKey {
        journal: PathBuf,
        join_key_id: String,
    },
    /// Cloud rejected the token of a resent staged join. An earlier send may
    /// have registered a runner, so the journal is kept for the join to be
    /// completed or cleaned up deliberately.
    StagedJoinUnauthorized {
        journal: PathBuf,
        join_key_id: String,
    },
    /// Cloud refused the join definitely, and the journal is removed.
    /// `resent` says whether an earlier send of the journal, in this start or
    /// an earlier one, can have reached Cloud: only when it is false is it
    /// certain that no runner was registered.
    JoinRefused {
        refusal: JoinRefusal,
        resent: bool,
    },
}

impl fmt::Display for EnrollmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidCommand => "runner enrollment options are inconsistent",
            Self::TerminalStdin => {
                "terminal stdin cannot supply an activation artifact or join key"
            }
            Self::InvalidConfig => "runner operator configuration is invalid",
            Self::InvalidArtifact => "activation artifact is invalid",
            Self::ExpiredArtifact => {
                "activation artifact has expired; issue a different activation"
            }
            Self::InvalidURL => "Cloud-issued runner URL is not permitted by deploymentMode",
            Self::ArtifactWrite => "activation artifact could not be written safely",
            Self::UnsafePath(requirement) => requirement,
            Self::StateLock => "runner state lock is unavailable",
            Self::UnresolvedJournal => "an unresolved enrollment journal already exists",
            Self::MissingJournal => "no unresolved enrollment journal is available to resume",
            Self::InvalidJournal => "enrollment journal is invalid",
            Self::EntropyUnavailable => "secure enrollment randomness is unavailable",
            Self::StateAlreadyEnrolled => "runner state already contains a current credential",
            Self::ReplacementStateMissing => {
                "replacement enrollment requires existing runner state"
            }
            Self::PendingCredentialExists => "runner state already contains a pending credential",
            Self::RunnerMismatch => "activation and local runner registration do not match",
            Self::InvalidState => "protected runner state is invalid",
            Self::StateWrite => "protected runner state could not be committed durably",
            Self::NetworkAmbiguous(cause) => {
                write!(
                    formatter,
                    "enrollment outcome is ambiguous ({}",
                    cause.code()
                )?;
                if let AmbiguousCause::CloudUnavailable(status) = cause {
                    write!(formatter, ", HTTP {status}")?;
                }
                return formatter.write_str("); resume the journal explicitly");
            }
            Self::Unauthorized => {
                "activation authentication was rejected; the journal remains unresolved"
            }
            Self::Conflict => {
                "enrollment request conflicts with Cloud state; the journal remains unresolved"
            }
            Self::InvalidResponse => {
                "enrollment response is invalid; the journal remains unresolved"
            }
            Self::InvalidJoinKey => "join key is invalid",
            Self::JoinKeyFile => {
                "join key file must be a regular file owned by the current user with mode 0600 and at most 4096 bytes"
            }
            Self::ExpiredJoinKey => "join key has expired; issue a different join key",
            Self::UnresolvedJoin {
                journal,
                join_key_id,
            } => {
                formatter.write_str("enrollment cannot resolve a staged join: ")?;
                return write_staged_join_and_resend_remedy(formatter, journal, join_key_id);
            }
            Self::JoinJournalForAnotherKey {
                journal,
                join_key_id,
            } => {
                formatter.write_str("this join key cannot resolve the host's staged join: ")?;
                return write_staged_join_and_resend_remedy(formatter, journal, join_key_id);
            }
            Self::StagedJoinUnauthorized {
                journal,
                join_key_id,
            } => {
                formatter.write_str(
                    "Cloud rejected the join key's token for the host's staged join: ",
                )?;
                return write_staged_join(formatter, journal, join_key_id);
            }
            Self::JoinRefused {
                refusal: JoinRefusal::Unauthorized,
                ..
            } => "Cloud rejected the join key's token",
            Self::JoinRefused {
                refusal: JoinRefusal::Gone,
                resent: false,
            } => {
                "the join key is revoked or expired, or its issuer may no longer issue a key for the pool"
            }
            Self::JoinRefused {
                refusal: JoinRefusal::Gone,
                resent: true,
            } => {
                "the join key is revoked or expired, its issuer may no longer issue a key for the pool, or the runner this host's earlier join registered was deleted"
            }
            Self::JoinRefused {
                refusal: JoinRefusal::Conflict,
                ..
            } => {
                "the pool cannot admit the join: its member is not active or the organization is at its runner limit"
            }
        };
        formatter.write_str(message)
    }
}

impl Error for EnrollmentError {}

// A staged join was written before its request was sent, so the host cannot
// know whether Cloud registered a runner from it. It names the key by id: the
// token is never kept.
fn write_staged_join(
    formatter: &mut fmt::Formatter<'_>,
    journal: &Path,
    join_key_id: &str,
) -> fmt::Result {
    write!(
        formatter,
        "join journal {} was staged for join key {join_key_id}, and Cloud may already have registered a runner from it",
        journal.display()
    )
}

fn write_staged_join_and_resend_remedy(
    formatter: &mut fmt::Formatter<'_>,
    journal: &Path,
    join_key_id: &str,
) -> fmt::Result {
    write_staged_join(formatter, journal, join_key_id)?;
    write!(
        formatter,
        "; {}",
        ResendRemedy {
            journal,
            join_key_id
        }
    )
}

// The remedy for a staged join that a start could not send: resend it with its
// own key, or clear it after checking the pool, so one host makes at most one
// runner. It does not fit a join whose resent token Cloud rejected.
struct ResendRemedy<'a> {
    journal: &'a Path,
    join_key_id: &'a str,
}

impl fmt::Display for ResendRemedy<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            journal,
            join_key_id,
        } = self;
        write!(
            formatter,
            "start um runner serve once more with join key {join_key_id}, which resends the same join, or remove {} after confirming in the pool that no runner joined from this host",
            journal.display()
        )
    }
}

#[cfg(test)]
mod tests;
