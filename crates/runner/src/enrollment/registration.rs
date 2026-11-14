//! The host side of a signed-in registration from `um runner serve`
//! (REQ-RUNNERENROLL-117, INV-RUNNER-26): the registration record beside the
//! state, and the operation that registers a runner, issues and consumes its
//! activation and writes the state, resumably. Cloud's side is a
//! `RegistrationAuthority` the caller supplies, so this crate reads no human
//! credential.

use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};

use reqwest::Url;
use serde::{Deserialize, Serialize};

use um_support::generate_idempotency_key;

use super::{
    ActivationArtifact, EnrollmentError, EnrollmentHTTPOutcome, EnrollmentJournal,
    EnrollmentResponse, EnrollmentTarget, LockedStateDirectory, STATE_LIMIT, atomic_write_json,
    now_rfc3339, parse_rfc3339, persist_enrollment_state, read_private_document,
    read_private_file_if_present, read_runner_state_if_present, remove_and_sync,
    remove_persisted_journal, require_terminal_receipt, require_unexpired, send_until_answered,
    stage_journal, valid_idempotency_key, valid_typed_id, validate_artifact,
    validate_enrollment_response, validate_journal, write_terminal_receipt,
};

/// How long after staging a registration is resent. Cloud replays a request
/// under its `Idempotency-Key` for seven days (REQ-API-6), so a resend within
/// six answers with the runner an earlier send made.
const RESEND_WINDOW: time::Duration = time::Duration::days(6);
const RECORD_INVALID: &str = "it is not a valid registration record";

/// Cloud's side of a signed-in registration, as the signed-in person. The
/// operation asks it only for what the record does not already answer, and
/// its failure stays the implementer's own.
pub trait RegistrationAuthority {
    type Failure;

    /// The API address of the deployment the authority sends to.
    fn api_url(&self) -> &str;

    /// The signed-in principal's ID, `prn_...`.
    fn principal_id(&mut self) -> Result<String, Self::Failure>;

    /// The ID, `rpl_...`, of the pool `pool` names in `organization`, by ID
    /// or exact name.
    fn pool_id(&mut self, organization: &str, pool: &str) -> Result<String, Self::Failure>;

    /// Creates a registration in `organization`, in the pool `pool_id` or,
    /// with none, in the signed-in person's personal pool, under
    /// `idempotency_key`.
    fn register(
        &mut self,
        organization: &str,
        pool_id: Option<&str>,
        idempotency_key: &str,
    ) -> Result<RegisteredRunner, Self::Failure>;

    /// Issues an activation for the registration `runner_id`, under a fresh
    /// idempotency key of its own.
    fn issue_activation(
        &mut self,
        organization: &str,
        runner_id: &str,
    ) -> Result<ActivationArtifact, Self::Failure>;

    /// Whether `failure` is Cloud's definite refusal, after which no earlier
    /// send of the same registration can have made a runner.
    fn is_definite_refusal(&self, failure: &Self::Failure) -> bool;
}

/// Cloud's answer to a registration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredRunner {
    pub runner_id: String,
    pub organization_id: String,
    pub pool_id: String,
}

/// The registration mode a start was given, with the pool as typed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistrationTarget {
    Personal,
    Pool(String),
}

#[derive(Debug)]
pub enum RegistrationOutcome {
    /// The host held enrolled state when the lock was taken, so nothing was
    /// sent.
    AlreadyEnrolled,
    Registered(Box<EnrollmentResponse>),
}

/// The step of a registration that failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationStage {
    LockState,
    ReadState,
    ReadRecord,
    ReadJournal,
    Principal,
    ResolvePool,
    WriteRecord,
    Register,
    IssueActivation,
    Enroll,
    WriteState,
    CompleteRecord,
}

impl fmt::Display for RegistrationStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::LockState => "lock the runner state",
            Self::ReadState => "read the runner state",
            Self::ReadRecord => "read the registration record",
            Self::ReadJournal => "read the enrollment journal",
            Self::Principal => "read the signed-in principal",
            Self::ResolvePool => "resolve the pool",
            Self::WriteRecord => "write the registration record",
            Self::Register => "register the runner",
            Self::IssueActivation => "issue the runner's activation",
            Self::Enroll => "enroll the runner",
            Self::WriteState => "write the runner state",
            Self::CompleteRecord => "complete the registration record",
        })
    }
}

/// The closed conflicts of a signed-in registration (REQ-RUNNERENROLL-118).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationConflictKind {
    /// The start names another organization, mode or pool than the record,
    /// or the record names another runner than the host's state, or the
    /// host is enrolled without a record.
    Target,
    /// The host holds a registration or a staged enrollment this start
    /// cannot resume.
    Unresolved,
    /// The record says the registration completed and the state is missing.
    StateMissing,
}

impl RegistrationConflictKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Target => "registration_target_conflict",
            Self::Unresolved => "registration_unresolved",
            Self::StateMissing => "registration_state_missing",
        }
    }
}

/// A conflict, the file an operator would inspect, the runner the record
/// names when it names one (or, on an enrolled host without a record, the
/// host's runner), and the options the record was made with when there is
/// one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistrationConflict {
    pub kind: RegistrationConflictKind,
    pub path: PathBuf,
    pub runner_id: Option<String>,
    pub recorded: Option<RecordedOptions>,
}

/// The organization and mode a registration record was made with, as given.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedOptions {
    pub organization: String,
    pub target: RegistrationTarget,
}

impl fmt::Display for RegistrationConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = self.path.display();
        match self.kind {
            RegistrationConflictKind::Target if self.recorded.is_none() => write!(
                formatter,
                "the host is enrolled without a registration record {path}"
            )?,
            RegistrationConflictKind::Target => write!(
                formatter,
                "registration record {path} names another organization, pool, mode or runner"
            )?,
            RegistrationConflictKind::Unresolved => write!(
                formatter,
                "{path} holds a registration or enrollment this start cannot resume"
            )?,
            RegistrationConflictKind::StateMissing => write!(
                formatter,
                "registration record {path} is complete and the runner state file is missing"
            )?,
        }
        match &self.runner_id {
            Some(runner_id) => write!(formatter, " (runner {runner_id})"),
            None => Ok(()),
        }
    }
}

/// A registration that stopped. `runner_id` is the runner Cloud registered
/// when this start knows it, and `record` the registration record left on
/// the host, which a start with the same options resumes; with neither, no
/// runner was registered.
#[derive(Debug)]
pub enum RegistrationError<F> {
    /// The authority's failure at `stage`, unchanged.
    Authority {
        stage: RegistrationStage,
        failure: F,
        runner_id: Option<String>,
        record: Option<PathBuf>,
    },
    /// The host side failed at `stage`.
    Host {
        stage: RegistrationStage,
        error: EnrollmentError,
        runner_id: Option<String>,
        record: Option<PathBuf>,
    },
    Conflict(RegistrationConflict),
}

impl<F> fmt::Display for RegistrationError<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Authority { stage, .. } | Self::Host { stage, .. } => stage.fmt(formatter),
            Self::Conflict(conflict) => conflict.fmt(formatter),
        }
    }
}

impl<F: fmt::Debug> Error for RegistrationError<F> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Host { error, .. } => Some(error),
            Self::Authority { .. } | Self::Conflict(_) => None,
        }
    }
}

/// Registers, enrolls and records a runner for a host with no state file
/// under the state lock, resuming the registration an earlier start
/// recorded. The record is written and synced before the registration is
/// first sent, and every send carries its key and the same request, so one
/// host registers at most one runner (INV-RUNNER-26).
pub fn register_unless_enrolled<A: RegistrationAuthority>(
    config_path: &Path,
    organization: &str,
    target: &RegistrationTarget,
    authority: &mut A,
) -> Result<RegistrationOutcome, RegistrationError<A::Failure>> {
    let host = LockedStateDirectory::lock(config_path)
        .map_err(Kept::nothing().host(RegistrationStage::LockState))?;
    if read_runner_state_if_present(&host.state_path, host.mode)
        .map_err(Kept::nothing().host(RegistrationStage::ReadState))?
        .is_some()
    {
        return Ok(RegistrationOutcome::AlreadyEnrolled);
    }
    let path = host.registration_path();
    let mut record = match read_registration_record(&path)
        .map_err(|error| Kept::record(&path).host_error(RegistrationStage::ReadRecord, error))?
    {
        Some(record) => resumable(record, &path, organization, target, authority)?,
        None => stage_registration(&host, &path, organization, target, authority)?,
    };
    let runner = match record.runner.clone() {
        Some(runner) => runner,
        None => register(&mut record, &path, authority)?,
    };
    let response = enroll_registered_runner(&host, &path, &record, &runner, authority)?;
    let kept = Kept::of(&path, &record);
    record.enrolled_at =
        Some(now_rfc3339().map_err(kept.clone().host(RegistrationStage::CompleteRecord))?);
    atomic_write_json(&path, &record).map_err(kept.host(RegistrationStage::CompleteRecord))?;
    drop(host);
    Ok(RegistrationOutcome::Registered(Box::new(response)))
}

/// Confirms a signed-in mode against an enrolled host's registration record
/// under the state lock, sending nothing (REQ-RUNNERENROLL-118): the options
/// must name the record's organization and target, and the record the
/// state's runner. A record whose runner the state holds and that is not yet
/// complete is completed, after the activation journal the state consumed is
/// removed. The state itself is never written.
pub fn confirm_registration(
    config_path: &Path,
    organization: &str,
    target: &RegistrationTarget,
) -> Result<(), RegistrationError<Infallible>> {
    let host = LockedStateDirectory::lock(config_path)
        .map_err(Kept::nothing().host(RegistrationStage::LockState))?;
    let state = read_runner_state_if_present(&host.state_path, host.mode)
        .and_then(|state| state.ok_or(EnrollmentError::InvalidState))
        .map_err(Kept::nothing().host(RegistrationStage::ReadState))?;
    let path = host.registration_path();
    let Some(mut record) = read_registration_record(&path)
        .map_err(|error| Kept::record(&path).host_error(RegistrationStage::ReadRecord, error))?
    else {
        return Err(RegistrationError::Conflict(RegistrationConflict {
            kind: RegistrationConflictKind::Target,
            path,
            runner_id: Some(state.runner_id),
            recorded: None,
        }));
    };
    let holds_runner = record
        .runner
        .as_ref()
        .is_some_and(|runner| runner.id == state.runner_id);
    if !holds_runner || !record.matches(organization, target) {
        return Err(RegistrationError::Conflict(
            record.conflict(RegistrationConflictKind::Target, &path),
        ));
    }
    if record.enrolled_at.is_none() {
        let kept = Kept::of(&path, &record);
        remove_persisted_journal(&host.journal_path, &state, true)
            .map_err(kept.clone().host(RegistrationStage::CompleteRecord))?;
        record.enrolled_at =
            Some(now_rfc3339().map_err(kept.clone().host(RegistrationStage::CompleteRecord))?);
        atomic_write_json(&path, &record).map_err(kept.host(RegistrationStage::CompleteRecord))?;
    }
    Ok(())
}

// Steps 4 and 8 for a record an earlier start wrote: a complete record with no
// state is reported, and a record that names no runner is resent only by the
// recorded principal to the recorded deployment within the resend window.
fn resumable<A: RegistrationAuthority>(
    record: RegistrationRecord,
    path: &Path,
    organization: &str,
    target: &RegistrationTarget,
    authority: &mut A,
) -> Result<RegistrationRecord, RegistrationError<A::Failure>> {
    let conflict = |kind, record: &RegistrationRecord| {
        RegistrationError::Conflict(record.conflict(kind, path))
    };
    if record.enrolled_at.is_some() {
        return Err(conflict(RegistrationConflictKind::StateMissing, &record));
    }
    if !record.matches(organization, target) {
        return Err(conflict(RegistrationConflictKind::Target, &record));
    }
    if record.runner.is_none() {
        let stale = parse_rfc3339(&record.staged_at)
            .is_none_or(|staged_at| um_support::utc_now() - staged_at > RESEND_WINDOW);
        if stale || record.deployment != authority.api_url() {
            return Err(conflict(RegistrationConflictKind::Unresolved, &record));
        }
        let principal_id = authority
            .principal_id()
            .map_err(Kept::of(path, &record).authority(RegistrationStage::Principal))?;
        if principal_id != record.principal_id {
            return Err(conflict(RegistrationConflictKind::Unresolved, &record));
        }
    }
    Ok(record)
}

// Step 3: with no record, refuse an enrollment or join another command
// staged, ask the authority for what the record names, and write and sync the
// record before anything is registered.
fn stage_registration<A: RegistrationAuthority>(
    host: &LockedStateDirectory,
    path: &Path,
    organization: &str,
    target: &RegistrationTarget,
    authority: &mut A,
) -> Result<RegistrationRecord, RegistrationError<A::Failure>> {
    let journal = read_private_file_if_present(&host.journal_path, STATE_LIMIT)
        .map_err(Kept::nothing().host(RegistrationStage::ReadJournal))?;
    if journal.is_some_and(|bytes| require_terminal_receipt(&bytes).is_err()) {
        return Err(RegistrationError::Conflict(RegistrationConflict {
            kind: RegistrationConflictKind::Unresolved,
            path: host.journal_path.clone(),
            runner_id: None,
            recorded: None,
        }));
    }
    let principal_id = authority
        .principal_id()
        .map_err(Kept::nothing().authority(RegistrationStage::Principal))?;
    let target = match target {
        RegistrationTarget::Personal => RecordedTarget::Personal {},
        RegistrationTarget::Pool(pool) => RecordedTarget::Pool {
            pool: pool.clone(),
            pool_id: authority
                .pool_id(organization, pool)
                .map_err(Kept::nothing().authority(RegistrationStage::ResolvePool))?,
        },
    };
    let record = RegistrationRecord {
        schema_version: 1,
        deployment: authority.api_url().to_owned(),
        organization: organization.to_owned(),
        target,
        principal_id,
        idempotency_key: generate_idempotency_key().map_err(|_| {
            Kept::nothing().host_error(
                RegistrationStage::WriteRecord,
                EnrollmentError::EntropyUnavailable,
            )
        })?,
        staged_at: now_rfc3339().map_err(Kept::nothing().host(RegistrationStage::WriteRecord))?,
        runner: None,
        enrolled_at: None,
    };
    if !record.valid() {
        return Err(Kept::nothing().host_error(
            RegistrationStage::WriteRecord,
            EnrollmentError::InvalidRegistrationAnswer,
        ));
    }
    atomic_write_json(path, &record)
        .map_err(Kept::nothing().host(RegistrationStage::WriteRecord))?;
    Ok(record)
}

// Step 5: register under the recorded key and pool, and record the runner.
// A definite refusal removes the record, since Cloud's replay rule means no
// earlier send made a runner; anything else keeps it for the next start.
fn register<A: RegistrationAuthority>(
    record: &mut RegistrationRecord,
    path: &Path,
    authority: &mut A,
) -> Result<RecordedRunner, RegistrationError<A::Failure>> {
    let pool_id = match &record.target {
        RecordedTarget::Personal {} => None,
        RecordedTarget::Pool { pool_id, .. } => Some(pool_id.as_str()),
    };
    let registered =
        match authority.register(&record.organization, pool_id, &record.idempotency_key) {
            Ok(registered) => registered,
            Err(failure) if authority.is_definite_refusal(&failure) => {
                let kept = remove_and_sync(path).is_err();
                return Err(RegistrationError::Authority {
                    stage: RegistrationStage::Register,
                    failure,
                    runner_id: None,
                    record: kept.then(|| path.to_owned()),
                });
            }
            Err(failure) => {
                return Err(
                    Kept::of(path, record).authority_error(RegistrationStage::Register, failure)
                );
            }
        };
    let runner = RecordedRunner {
        id: registered.runner_id,
        organization_id: registered.organization_id,
        pool_id: registered.pool_id,
    };
    if !runner.valid() {
        return Err(Kept::of(path, record).host_error(
            RegistrationStage::Register,
            EnrollmentError::InvalidRegistrationAnswer,
        ));
    }
    record.runner = Some(runner.clone());
    atomic_write_json(path, &*record)
        .map_err(Kept::of(path, record).host(RegistrationStage::WriteRecord))?;
    Ok(runner)
}

// Steps 6 and 7: resend an activation an earlier start staged for the
// recorded runner, or issue and stage a new one, send it until Cloud answers
// definitely, and write the state.
fn enroll_registered_runner<A: RegistrationAuthority>(
    host: &LockedStateDirectory,
    path: &Path,
    record: &RegistrationRecord,
    runner: &RecordedRunner,
    authority: &mut A,
) -> Result<EnrollmentResponse, RegistrationError<A::Failure>> {
    let kept = Kept::of(path, record);
    let journal_path = &host.journal_path;
    let mut resent = false;
    if let Some(journal) = staged_activation(journal_path, runner)
        .map_err(kept.clone().host(RegistrationStage::ReadJournal))?
        .map_err(|kind| {
            RegistrationError::Conflict(RegistrationConflict {
                kind,
                path: journal_path.clone(),
                runner_id: Some(runner.id.clone()),
                recorded: None,
            })
        })?
    {
        let EnrollmentTarget::Activation(artifact) = &journal.target else {
            return Err(kept.host_error(
                RegistrationStage::ReadJournal,
                EnrollmentError::UnresolvedJournal,
            ));
        };
        validate_journal(&journal, host.mode)
            .map_err(kept.clone().host(RegistrationStage::Enroll))?;
        match send_until_answered(
            &journal,
            &artifact.activation_url,
            &artifact.activation_token,
            &mut resent,
            |_| {},
        )
        .map_err(kept.clone().host(RegistrationStage::Enroll))?
        {
            EnrollmentHTTPOutcome::Success(response) => {
                return commit(host, &journal, response, runner, &kept);
            }
            EnrollmentHTTPOutcome::Gone => {
                write_terminal_receipt(journal_path, artifact)
                    .map_err(kept.clone().host(RegistrationStage::Enroll))?;
            }
        }
    }

    let artifact = authority
        .issue_activation(&record.organization, &runner.id)
        .map_err(kept.clone().authority(RegistrationStage::IssueActivation))?;
    if artifact.runner_id != runner.id {
        return Err(kept.host_error(
            RegistrationStage::IssueActivation,
            EnrollmentError::RunnerMismatch,
        ));
    }
    validate_artifact(&artifact, host.mode)
        .and_then(|()| require_unexpired(&artifact.expires_at, EnrollmentError::ExpiredArtifact))
        .map_err(kept.clone().host(RegistrationStage::IssueActivation))?;
    let journal = stage_journal(
        journal_path,
        EnrollmentTarget::Activation(artifact.clone()),
        false,
    )
    .and_then(|journal| validate_journal(&journal, host.mode).map(|()| journal))
    .map_err(kept.clone().host(RegistrationStage::Enroll))?;
    match send_until_answered(
        &journal,
        &artifact.activation_url,
        &artifact.activation_token,
        &mut resent,
        |_| {},
    )
    .map_err(kept.clone().host(RegistrationStage::Enroll))?
    {
        EnrollmentHTTPOutcome::Success(response) => commit(host, &journal, response, runner, &kept),
        EnrollmentHTTPOutcome::Gone => {
            write_terminal_receipt(journal_path, &artifact)
                .map_err(kept.clone().host(RegistrationStage::Enroll))?;
            Err(kept.host_error(RegistrationStage::Enroll, EnrollmentError::ActivationGone))
        }
    }
}

// The staged activation journal to resend for `runner`: none when the host
// holds no journal or a terminal receipt, and the unresolved conflict for a
// journal of another runner, a replacement or a join.
fn staged_activation(
    journal_path: &Path,
    runner: &RecordedRunner,
) -> Result<Result<Option<EnrollmentJournal>, RegistrationConflictKind>, EnrollmentError> {
    let Some(bytes) = read_private_file_if_present(journal_path, STATE_LIMIT)? else {
        return Ok(Ok(None));
    };
    if require_terminal_receipt(&bytes).is_ok() {
        return Ok(Ok(None));
    }
    Ok(serde_json::from_slice::<EnrollmentJournal>(&bytes)
        .ok()
        .filter(|journal| {
            !journal.replace_credential
                && matches!(
                    &journal.target,
                    EnrollmentTarget::Activation(artifact) if artifact.runner_id == runner.id
                )
        })
        .map(Some)
        .ok_or(RegistrationConflictKind::Unresolved))
}

// Step 7: the answer must name the recorded runner; the state is written
// before the journal that staged its credential is removed.
fn commit<F>(
    host: &LockedStateDirectory,
    journal: &EnrollmentJournal,
    response: EnrollmentResponse,
    runner: &RecordedRunner,
    kept: &Kept,
) -> Result<EnrollmentResponse, RegistrationError<F>> {
    validate_enrollment_response(&response, Some(&runner.id), host.mode)
        .map_err(kept.clone().host(RegistrationStage::Enroll))?;
    persist_enrollment_state(&host.state_path, journal, &response, host.mode)
        .and_then(|()| remove_and_sync(&host.journal_path))
        .map_err(kept.clone().host(RegistrationStage::WriteState))?;
    Ok(response)
}

/// What a failure leaves on the host: the record, and the runner it names.
#[derive(Clone)]
struct Kept {
    record: Option<PathBuf>,
    runner_id: Option<String>,
}

impl Kept {
    const fn nothing() -> Self {
        Self {
            record: None,
            runner_id: None,
        }
    }

    fn of(path: &Path, record: &RegistrationRecord) -> Self {
        Self {
            record: Some(path.to_owned()),
            runner_id: record.runner.as_ref().map(|runner| runner.id.clone()),
        }
    }

    /// A record that names no runner, or that did not read.
    fn record(path: &Path) -> Self {
        Self {
            record: Some(path.to_owned()),
            runner_id: None,
        }
    }

    fn host_error<F>(
        self,
        stage: RegistrationStage,
        error: EnrollmentError,
    ) -> RegistrationError<F> {
        RegistrationError::Host {
            stage,
            error,
            runner_id: self.runner_id,
            record: self.record,
        }
    }

    fn authority_error<F>(self, stage: RegistrationStage, failure: F) -> RegistrationError<F> {
        RegistrationError::Authority {
            stage,
            failure,
            runner_id: self.runner_id,
            record: self.record,
        }
    }

    fn host<F>(
        self,
        stage: RegistrationStage,
    ) -> impl FnOnce(EnrollmentError) -> RegistrationError<F> {
        move |error| self.host_error(stage, error)
    }

    fn authority<F>(self, stage: RegistrationStage) -> impl FnOnce(F) -> RegistrationError<F> {
        move |failure| self.authority_error(stage, failure)
    }
}

/// `.runner-registration.json`, schema `runner-registration-v1`: closed, no
/// secret, written by `atomic_write_json` under the state lock.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct RegistrationRecord {
    schema_version: u8,
    deployment: String,
    organization: String,
    target: RecordedTarget,
    principal_id: String,
    idempotency_key: String,
    staged_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    runner: Option<RecordedRunner>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enrolled_at: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum RecordedTarget {
    Personal {},
    Pool {
        pool: String,
        #[serde(rename = "poolId")]
        pool_id: String,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RecordedRunner {
    id: String,
    organization_id: String,
    pool_id: String,
}

impl RecordedRunner {
    fn valid(&self) -> bool {
        valid_typed_id(&self.id, "rnr_")
            && valid_typed_id(&self.organization_id, "org_")
            && valid_typed_id(&self.pool_id, "rpl_")
    }
}

impl RegistrationRecord {
    fn valid(&self) -> bool {
        self.schema_version == 1
            && Url::parse(&self.deployment).is_ok()
            && !self.organization.is_empty()
            && match &self.target {
                RecordedTarget::Personal {} => true,
                RecordedTarget::Pool { pool, pool_id } => {
                    !pool.is_empty() && valid_typed_id(pool_id, "rpl_")
                }
            }
            && valid_typed_id(&self.principal_id, "prn_")
            && valid_idempotency_key(&self.idempotency_key)
            && parse_rfc3339(&self.staged_at).is_some()
            && self.runner.as_ref().is_none_or(RecordedRunner::valid)
            && self.enrolled_at.as_deref().is_none_or(|enrolled_at| {
                self.runner.is_some() && parse_rfc3339(enrolled_at).is_some()
            })
    }

    /// The conflict `kind` this record at `path` stands in.
    pub(super) fn conflict(
        &self,
        kind: RegistrationConflictKind,
        path: &Path,
    ) -> RegistrationConflict {
        RegistrationConflict {
            kind,
            path: path.to_owned(),
            runner_id: self.runner.as_ref().map(|runner| runner.id.clone()),
            recorded: Some(self.options()),
        }
    }

    fn options(&self) -> RecordedOptions {
        RecordedOptions {
            organization: self.organization.clone(),
            target: match &self.target {
                RecordedTarget::Personal {} => RegistrationTarget::Personal,
                RecordedTarget::Pool { pool, .. } => RegistrationTarget::Pool(pool.clone()),
            },
        }
    }

    /// Whether a start's options name this record's registration: the same
    /// mode, the organization as recorded or the registered runner's
    /// organization ID, and the pool as recorded or its ID.
    fn matches(&self, organization: &str, target: &RegistrationTarget) -> bool {
        let organization_matches = self.organization == organization
            || self
                .runner
                .as_ref()
                .is_some_and(|runner| runner.organization_id == organization);
        organization_matches
            && match (&self.target, target) {
                (RecordedTarget::Personal {}, RegistrationTarget::Personal) => true,
                (RecordedTarget::Pool { pool, pool_id }, RegistrationTarget::Pool(given)) => {
                    given == pool || given == pool_id
                }
                _ => false,
            }
    }
}

/// Reads the host's registration record, `None` when it is absent. A record
/// that is not a private regular file, does not parse or does not validate
/// is a failure naming it.
pub(super) fn read_registration_record(
    path: &Path,
) -> Result<Option<RegistrationRecord>, EnrollmentError> {
    let invalid = |requirement| EnrollmentError::InvalidRegistrationRecord {
        record: path.to_owned(),
        requirement,
    };
    let Some(bytes) =
        read_private_document(path, STATE_LIMIT).map_err(|failure| invalid(failure.requirement))?
    else {
        return Ok(None);
    };
    let record = serde_json::from_slice::<RegistrationRecord>(&bytes)
        .ok()
        .filter(RegistrationRecord::valid)
        .ok_or_else(|| invalid(RECORD_INVALID))?;
    Ok(Some(record))
}
