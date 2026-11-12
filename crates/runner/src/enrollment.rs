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

use super::validation::{valid_secret_syntax, valid_typed_id};

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum DeploymentMode {
    Production,
    Development,
}

#[derive(Debug, Deserialize)]
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

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EnrollmentJournal {
    schema_version: u8,
    activation_artifact: ActivationArtifact,
    #[serde(skip_serializing)]
    credential_secret: String,
    credential_secret_verifier: String,
    #[serde(skip_serializing)]
    idempotency_key: String,
    replace_credential: bool,
    staged_at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedEnrollmentJournal<'a> {
    schema_version: u8,
    activation_artifact: TransferableActivationArtifact<'a>,
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
            activation_artifact: TransferableActivationArtifact::from(&journal.activation_artifact),
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
    activation_id: String,
    enrolled_at: String,
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
    activation_id: &'a str,
    enrolled_at: &'a str,
}

impl<'a> From<&'a StoredRunnerCredential> for PersistedRunnerCredential<'a> {
    fn from(credential: &'a StoredRunnerCredential) -> Self {
        Self {
            id: &credential.id,
            secret: &credential.secret,
            activation_id: &credential.activation_id,
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
    let path = Path::new(destination);
    let parent = artifact_parent(path);
    let metadata = fs::symlink_metadata(parent).map_err(|_| EnrollmentError::ArtifactWrite)?;
    if !metadata.file_type().is_dir() {
        return Err(EnrollmentError::UnsafePath(
            "activation artifact parent must be a non-symbolic-link directory",
        ));
    }
    let mut file = create_new_private_file(path).map_err(|_| EnrollmentError::ArtifactWrite)?;
    artifact.write_json(&mut file)?;
    file.sync_all()
        .map_err(|_| EnrollmentError::ArtifactWrite)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| EnrollmentError::ArtifactWrite)
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

    let config = load_operator_config(config_path)?;
    let state_path = config.runner_state_path;
    let state_directory = state_path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .ok_or(EnrollmentError::UnsafePath(
            "runner state path has no parent directory",
        ))?;
    ensure_private_directory(state_directory)?;
    let lock_path = state_directory.join(LOCK_FILE);
    let lock = acquire_state_lock(&lock_path)?;
    let journal_path = state_directory.join(JOURNAL_FILE);

    let journal = if resume {
        read_journal(&journal_path)?
    } else {
        let artifact = read_activation_artifact_allow_expired(
            activation_file.ok_or(EnrollmentError::InvalidCommand)?,
            config.deployment_mode,
        )?;
        let existing_state = read_runner_state_if_present(&state_path, config.deployment_mode)?;
        match (&existing_state, replace_credential) {
            (Some(_), false) => return Err(EnrollmentError::StateAlreadyEnrolled),
            (None, true) => return Err(EnrollmentError::ReplacementStateMissing),
            (Some(state), true) => {
                if state.runner_id != artifact.runner_id {
                    return Err(EnrollmentError::RunnerMismatch);
                }
                let artifact_activation_id = activation_id(&artifact)?;
                let local_credential = if let Some(pending) = &state.pending_credential {
                    if pending.activation_id != artifact_activation_id {
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
        require_unexpired_artifact(&artifact)?;
        stage_journal(&journal_path, artifact, replace_credential)?
    };

    validate_journal(&journal, config.deployment_mode)?;
    if journal.replace_credential {
        validate_replacement_journal_local_state(&state_path, config.deployment_mode, &journal)?;
    }

    let request = EnrollmentRequest {
        schema_version: 1,
        credential_secret_verifier: &journal.credential_secret_verifier,
    };
    let deployment = enrollment_deployment(&journal.activation_artifact.activation_url)?;
    match send_enrollment(&journal, &request)? {
        EnrollmentHTTPOutcome::Success(response) => {
            validate_enrollment_response(
                &response,
                &journal.activation_artifact,
                config.deployment_mode,
            )?;
            persist_enrollment_state(&state_path, &journal, &response, config.deployment_mode)?;
            remove_and_sync(&journal_path)?;
            drop(lock);
            Ok(EnrollmentOutcome::Enrolled {
                response,
                replacement: journal.replace_credential,
                deployment,
            })
        }
        EnrollmentHTTPOutcome::Gone => {
            let activation_id = activation_id(&journal.activation_artifact)?;
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

// Validation requires the root-level /v1/runner-enrollments/.../activate path,
// so this origin is the API base that actually received enrollment. The
// caller's human-auth deployment may differ from the issued artifact's target.
fn enrollment_deployment(activation_url: &str) -> Result<String, EnrollmentError> {
    let url = Url::parse(activation_url).map_err(|_| EnrollmentError::InvalidJournal)?;
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

fn send_enrollment(
    journal: &EnrollmentJournal,
    request: &EnrollmentRequest<'_>,
) -> Result<EnrollmentHTTPOutcome, EnrollmentError> {
    um_support::install_provider();
    let client = reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()
        .map_err(|_| EnrollmentError::NetworkAmbiguous)?;
    let body = serde_json::to_vec(request).map_err(|_| EnrollmentError::InvalidJournal)?;
    let mut response = client
        .post(&journal.activation_artifact.activation_url)
        .header(
            "Authorization",
            format!("Bearer {}", journal.activation_artifact.activation_token),
        )
        .header("Idempotency-Key", &journal.idempotency_key)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .map_err(|_| EnrollmentError::NetworkAmbiguous)?;
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
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return Err(EnrollmentError::NetworkAmbiguous);
    }
    if status != StatusCode::CREATED {
        return Err(EnrollmentError::InvalidResponse);
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut response)
        .take(RESPONSE_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| EnrollmentError::NetworkAmbiguous)?;
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
        let promoted_at = now_rfc3339().map_err(|_| ReloadStateError::StateUpdate)?;
        state.last_promotion = Some(CredentialPromotion {
            credential_id: pending.id.clone(),
            activation_id: pending.activation_id.clone(),
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
    require_unexpired_artifact(&artifact)?;
    Ok(artifact)
}

fn read_activation_artifact_allow_expired(
    path: &Path,
    mode: DeploymentMode,
) -> Result<ActivationArtifact, EnrollmentError> {
    let bytes = if path == Path::new("-") {
        let mut bytes = Vec::new();
        io::stdin()
            .lock()
            .take(ARTIFACT_LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| EnrollmentError::InvalidArtifact)?;
        bytes
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

fn require_unexpired_artifact(artifact: &ActivationArtifact) -> Result<(), EnrollmentError> {
    if parse_rfc3339(&artifact.expires_at)
        .is_none_or(|expires_at| expires_at <= um_support::utc_now())
    {
        return Err(EnrollmentError::ExpiredArtifact);
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
    if artifact.activation_token.len() > 256 {
        return Err(EnrollmentError::InvalidArtifact);
    }
    let (activation_id, secret) = artifact
        .activation_token
        .split_once('.')
        .ok_or(EnrollmentError::InvalidArtifact)?;
    if secret.contains('.') || !valid_typed_id(activation_id, "rna_") || !valid_secret(secret) {
        return Err(EnrollmentError::InvalidArtifact);
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(secret)
        .map_err(|_| EnrollmentError::InvalidArtifact)?;
    if decoded.len() != 32 {
        return Err(EnrollmentError::InvalidArtifact);
    }
    Ok(activation_id.to_owned())
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

fn validate_enrollment_response(
    response: &EnrollmentResponse,
    artifact: &ActivationArtifact,
    mode: DeploymentMode,
) -> Result<(), EnrollmentError> {
    if response.schema_version != 1
        || response.runner_id != artifact.runner_id
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
    artifact: ActivationArtifact,
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
        activation_artifact: artifact,
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
    if let Some(bytes) = read_private_file_if_present(path, STATE_LIMIT)? {
        let receipt = serde_json::from_slice::<TerminalReceipt>(&bytes)
            .map_err(|_| EnrollmentError::UnresolvedJournal)?;
        if !valid_terminal_receipt(&receipt) {
            return Err(EnrollmentError::UnresolvedJournal);
        }
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
    validate_artifact(&journal.activation_artifact, mode)?;
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
) -> Result<(), EnrollmentError> {
    let state = read_runner_state_if_present(path, mode)?
        .ok_or(EnrollmentError::ReplacementStateMissing)?;
    if state.runner_id != journal.activation_artifact.runner_id {
        return Err(EnrollmentError::RunnerMismatch);
    }
    let activation_id = activation_id(&journal.activation_artifact)?;
    let staged = state.pending_credential.as_ref().or_else(|| {
        state
            .last_promotion
            .as_ref()
            .filter(|promotion| promotion.activation_id == activation_id)
            .map(|_| &state.current_credential)
    });
    if staged.is_some_and(|credential| {
        credential.activation_id != activation_id || credential.secret != journal.credential_secret
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
    let activation_id = activation_id(&journal.activation_artifact)?;
    let existing = read_runner_state_if_present(path, mode)?;
    if existing
        .as_ref()
        .is_some_and(|state| enrollment_already_persisted(state, journal, response, &activation_id))
    {
        return Ok(());
    }

    let enrolled_at = now_rfc3339()?;
    let credential = StoredRunnerCredential {
        id: response.credential_id.clone(),
        secret: journal.credential_secret.clone(),
        activation_id,
        enrolled_at: enrolled_at.clone(),
    };
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
    activation_id: &str,
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
                        && promotion.activation_id == activation_id
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
            && credential.activation_id == activation_id
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
                || promotion.activation_id != state.current_credential.activation_id
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
        && valid_typed_id(&credential.activation_id, "rna_")
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
    NetworkAmbiguous,
    Unauthorized,
    Conflict,
    InvalidResponse,
}

impl fmt::Display for EnrollmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidCommand => "runner enrollment options are inconsistent",
            Self::TerminalStdin => "terminal stdin cannot supply an activation artifact",
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
            Self::NetworkAmbiguous => {
                "enrollment outcome is ambiguous; resume the journal explicitly"
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
        };
        formatter.write_str(message)
    }
}

impl Error for EnrollmentError {}

#[cfg(test)]
mod tests;
