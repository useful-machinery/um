use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use um_support::{Availability, CredentialStatus, NextAction};

pub(crate) const REQUEST_LIMIT: usize = 4_096;
pub(crate) const RESPONSE_LIMIT: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Status,
    ReloadCredential,
    SyncAgentLogins,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlError {
    InvalidRequest,
    UnsupportedVersion,
    RunnerBusy,
    NoPendingCredential,
    PendingRegistrationMismatch,
    PendingAuthenticationFailed,
    PendingProtocolFailed,
    PendingConnectionFailed,
    StateUpdateFailed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    Running,
    Stopping,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Connecting,
    Connected,
    BackingOff,
    AuthenticationFailed,
    ProtocolFailed,
    Stopping,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionFailure {
    Network,
    RateLimited,
    CloudUnavailable,
    Authentication,
    Protocol,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AssignmentCounts {
    pub preparing: u64,
    pub accepted: u64,
    pub running: u64,
    pub finishing: u64,
    pub reporting: u64,
}

impl AssignmentCounts {
    pub fn total(self) -> Option<u64> {
        self.preparing
            .checked_add(self.accepted)?
            .checked_add(self.running)?
            .checked_add(self.finishing)?
            .checked_add(self.reporting)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StatusSnapshot {
    pub process_state: ProcessState,
    pub boot_id: String,
    pub uptime_milliseconds: u64,
    pub connection_state: ConnectionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_connected_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_credential_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_credential_id: Option<String>,
    pub assignment_counts: AssignmentCounts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_connection_failure: Option<ConnectionFailure>,
    /// Each provider's readiness, once the runner has set up agent logins
    /// (REQ-RUNNERENROLL-42). Absent until the first pass records it, seconds
    /// after the runner starts; from then on a `um runner status` older than
    /// agent logins refuses the answer, the replaced-binary case chapter 11
    /// documents.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_logins: Option<AgentLoginReadiness>,
}

/// A provider in the runner's agent-login readiness.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLoginProvider {
    ClaudeCode,
    Codex,
    Github,
    Gitlab,
    Linear,
}

impl AgentLoginProvider {
    /// Every provider this build knows, the brokered ones in the order the
    /// readiness lists them.
    pub const ALL: [Self; 5] = [
        Self::ClaudeCode,
        Self::Codex,
        Self::Github,
        Self::Gitlab,
        Self::Linear,
    ];

    /// The provider `name` names on the wire; `None` for one this build
    /// does not know.
    pub fn named(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|provider| provider.as_str() == name)
    }

    /// An OAuth provider, whose token Cloud issues for each assignment
    /// rather than once to the runner: its entry records one assignment's
    /// outcome (REQ-GRANT-88).
    pub const fn is_brokered(self) -> bool {
        matches!(self, Self::Github | Self::Gitlab | Self::Linear)
    }

    /// An OAuth provider's name as a grant's provider, which the grant-token
    /// route and `um grant` take: GitLab is the instance `gitlab.com`.
    /// `None` for an installable provider.
    pub const fn grant_provider(self) -> Option<&'static str> {
        match self {
            Self::Github => Some("github"),
            Self::Gitlab => Some("gitlab.com"),
            Self::Linear => Some("linear"),
            Self::Codex | Self::ClaudeCode => None,
        }
    }

    /// The provider's name on the wire.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClaudeCode => "claude_code",
            Self::Codex => "codex",
            Self::Github => "github",
            Self::Gitlab => "gitlab",
            Self::Linear => "linear",
        }
    }
}

/// A provider's readiness state (REQ-GRANT-38).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLoginState {
    Ready,
    NotAuthorized,
    Revoked,
    ForeignLogin,
    UnsupportedTarget,
    StaleGeneration,
    CloudUnavailable,
    InstallFailed,
    NotDelivered,
    /// Cloud refused an OAuth provider's token because its grant needs the
    /// member's consent again (REQ-GRANT-88); never an agent login's state.
    ReconnectRequired,
}

impl AgentLoginState {
    /// Whether an agent login's provider is, or may be, authorized for the
    /// runner without its login being ready (REQ-GRANT-41): every state but `ready`,
    /// the two Cloud answers that no login is authorized, and the brokered
    /// providers' `not_delivered`. `cloud_unavailable` counts: a runner
    /// records it only for a provider it last knew as authorized or
    /// installed (REQ-GRANT-38).
    pub const fn fails_readiness(self) -> bool {
        !matches!(
            self,
            Self::Ready | Self::NotAuthorized | Self::Revoked | Self::NotDelivered
        )
    }

    /// Whether an entry written before selection was recorded reads as
    /// selected: exactly when its state is one only an authorized login has
    /// (REQ-GRANT-38).
    pub const fn selected_by_state(self) -> bool {
        matches!(self, Self::Ready) || self.fails_readiness()
    }

    /// Whether an OAuth provider's entry may hold the state (REQ-GRANT-88):
    /// every state but the two only an installed agent login has.
    pub const fn held_by_grant(self) -> bool {
        !matches!(self, Self::ForeignLogin | Self::StaleGeneration)
    }

    /// Whether an agent login's entry may hold the state (REQ-GRANT-38):
    /// every state but the two only an OAuth provider has.
    pub const fn held_by_login(self) -> bool {
        !matches!(self, Self::NotDelivered | Self::ReconnectRequired)
    }

    /// The state's name on the wire.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NotAuthorized => "not_authorized",
            Self::Revoked => "revoked",
            Self::ForeignLogin => "foreign_login",
            Self::UnsupportedTarget => "unsupported_target",
            Self::StaleGeneration => "stale_generation",
            Self::CloudUnavailable => "cloud_unavailable",
            Self::InstallFailed => "install_failed",
            Self::NotDelivered => "not_delivered",
            Self::ReconnectRequired => "reconnect_required",
        }
    }
}

/// How the provider answered the last use of an installed login
/// (REQ-GRANT-52).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLoginUse {
    /// An agent step that received the login's variable finished
    /// successfully.
    Accepted,
    /// An agent step that received the login's variable failed with the
    /// harness error `unauthorized`.
    Refused,
}

/// One provider's readiness as the control socket returns it: the readiness
/// document's entry without the path of the install (REQ-RUNNERENROLL-63).
/// An OAuth provider's entry carries, while `ready`, the grant, generation
/// and expiry of the token issued, and the run of its assignment and when
/// that ended where the document records them (REQ-GRANT-88), never the
/// token.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProviderReadiness {
    pub provider: AgentLoginProvider,
    pub state: AgentLoginState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_login_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checked_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub installed_at: Option<String>,
    /// The grant an OAuth provider's `ready` token was issued from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_id: Option<String>,
    /// When an OAuth provider's `ready` token expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    /// Whether the provider is selected (REQ-GRANT-51); absent where
    /// the document records none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected: Option<bool>,
    /// How the provider answered the last use of the login installed
    /// (REQ-GRANT-52); only ever beside an install.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_use: Option<AgentLoginUse>,
    /// When that last use was (REQ-GRANT-41); only ever beside a last use,
    /// and absent where a serving process that predates it answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_use_at: Option<String>,
    /// The run of the assignment an OAuth provider's entry was recorded for
    /// (REQ-GRANT-88); only beside a check time, and absent where a serving
    /// process that predates it answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// When that assignment ended on the runner (REQ-GRANT-88); only beside
    /// a check time, absent while it runs and where a serving process that
    /// predates it answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
}

impl ProviderReadiness {
    /// Whether the provider is selected. An agent login's entry that records
    /// no selection is selected by its state (REQ-GRANT-38); an OAuth
    /// provider's is not selected.
    pub const fn is_selected(&self) -> bool {
        match self.selected {
            Some(selected) => selected,
            None => !self.provider.is_brokered() && self.state.selected_by_state(),
        }
    }

    /// The provider's availability and next action on the runner
    /// (REQ-GRANT-53). An OAuth provider's `ready` is `available` while its
    /// assignment runs, since no use of a grant token is recorded, and
    /// `unverified` once that has ended, which says nothing of the next one
    /// (REQ-GRANT-88); any other state of its entry is projected as an agent
    /// login's is.
    pub const fn credential_status(&self) -> CredentialStatus {
        let (availability, next_action) = match self.state {
            AgentLoginState::Ready if self.provider.is_brokered() && self.ended_at.is_some() => {
                (Availability::Unverified, NextAction::None)
            }
            AgentLoginState::Ready if self.provider.is_brokered() => {
                (Availability::Available, NextAction::None)
            }
            AgentLoginState::Ready => match self.last_use {
                Some(AgentLoginUse::Accepted) => (Availability::Available, NextAction::None),
                None => (Availability::Unverified, NextAction::None),
                Some(AgentLoginUse::Refused) => {
                    (Availability::ReconnectRequired, NextAction::Reconnect)
                }
            },
            AgentLoginState::InstallFailed
            | AgentLoginState::StaleGeneration
            | AgentLoginState::CloudUnavailable => {
                (Availability::DeliveryFailed, NextAction::SyncRunner)
            }
            AgentLoginState::UnsupportedTarget => {
                (Availability::DeliveryFailed, NextAction::CheckTarget)
            }
            AgentLoginState::NotAuthorized if self.is_selected() => (
                Availability::InsufficientPermission,
                NextAction::RestoreMembership,
            ),
            AgentLoginState::NotAuthorized | AgentLoginState::Revoked => {
                (Availability::NotConnected, NextAction::None)
            }
            AgentLoginState::ForeignLogin => {
                (Availability::NotDelivered, NextAction::ResolveServiceLogin)
            }
            AgentLoginState::NotDelivered => (Availability::NotDelivered, NextAction::None),
            AgentLoginState::ReconnectRequired => {
                (Availability::ReconnectRequired, NextAction::Reconnect)
            }
        };
        CredentialStatus {
            availability,
            next_action,
        }
    }

    /// The failure code of a provider Cloud authorized for the runner whose
    /// login is not ready to use (REQ-GRANT-41, REQ-RUNNERENROLL-86):
    /// its state when that fails readiness, or when it is `not_authorized`
    /// on a selected provider, whose login every admission withholds
    /// (REQ-GRANT-51); `reconnect_required` for a `ready` login refused
    /// at its last use; and `None` otherwise, an OAuth provider's entry
    /// included, which records one assignment's outcome and not the
    /// runner's (REQ-GRANT-88).
    pub const fn failure_code(&self) -> Option<&'static str> {
        if self.provider.is_brokered() {
            None
        } else if self.state.fails_readiness()
            || (matches!(self.state, AgentLoginState::NotAuthorized) && self.is_selected())
        {
            Some(self.state.as_str())
        } else if matches!(self.state, AgentLoginState::Ready)
            && matches!(self.last_use, Some(AgentLoginUse::Refused))
        {
            Some("reconnect_required")
        } else {
            None
        }
    }

    /// Whether the provider fails readiness: it has a failure code.
    pub const fn fails_readiness(&self) -> bool {
        self.failure_code().is_some()
    }
}

/// The readiness the runner last recorded for each provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AgentLoginReadiness {
    pub updated_at: String,
    pub providers: Vec<ProviderReadiness>,
}

/// Why a check of an installed login gave no answer (REQ-GRANT-90).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLoginCheckCause {
    /// The coding agent's program was not found, or not in the supported
    /// range, when the runner started.
    ProgramUnavailable,
    /// The program or the login could not be read or started.
    StartFailed,
    /// The program did not end the turn within the check's bound.
    TimedOut,
    /// The turn ended with a harness error other than `unauthorized`.
    HarnessError,
}

impl AgentLoginCheckCause {
    /// The cause's name on the wire and in telemetry.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProgramUnavailable => "program_unavailable",
            Self::StartFailed => "start_failed",
            Self::TimedOut => "timed_out",
            Self::HarnessError => "harness_error",
        }
    }
}

/// An installed login whose check in the setup gave no answer, with why
/// (REQ-GRANT-41, REQ-GRANT-90).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UnansweredCheck {
    pub provider: AgentLoginProvider,
    pub cause: AgentLoginCheckCause,
}

/// The `synced` answer: the readiness the setup recorded, and each login
/// whose check in that setup gave no answer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncedAgentLogins {
    pub readiness: AgentLoginReadiness,
    pub unanswered_checks: Vec<UnansweredCheck>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Response {
    Status(StatusSnapshot),
    Reloaded { credential_id: String },
    AgentLoginsSynced(SyncedAgentLogins),
    Error(ControlError),
}

#[derive(Serialize)]
#[serde(untagged)]
enum EncodedResponse<'a> {
    Status {
        #[serde(rename = "schemaVersion")]
        schema_version: u8,
        outcome: &'static str,
        status: &'a StatusSnapshot,
    },
    Reloaded {
        #[serde(rename = "schemaVersion")]
        schema_version: u8,
        outcome: &'static str,
        #[serde(rename = "credentialId")]
        credential_id: &'a str,
    },
    AgentLoginsSynced {
        #[serde(rename = "schemaVersion")]
        schema_version: u8,
        outcome: &'static str,
        #[serde(rename = "agentLogins")]
        agent_logins: &'a AgentLoginReadiness,
        #[serde(rename = "unansweredChecks", skip_serializing_if = "<[_]>::is_empty")]
        unanswered_checks: &'a [UnansweredCheck],
    },
    Error {
        #[serde(rename = "schemaVersion")]
        schema_version: u8,
        outcome: &'static str,
        error: ControlError,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DecodedResponse {
    schema_version: u8,
    outcome: String,
    #[serde(default)]
    status: Option<StatusSnapshot>,
    #[serde(default)]
    credential_id: Option<String>,
    #[serde(default)]
    agent_logins: Option<AgentLoginReadiness>,
    #[serde(default)]
    unanswered_checks: Option<Vec<UnansweredCheck>>,
    #[serde(default)]
    error: Option<ControlError>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProtocolFailure {
    Invalid,
    Oversized,
}

impl fmt::Display for ProtocolFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Runner Serve control protocol is invalid")
    }
}

impl std::error::Error for ProtocolFailure {}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Operation, ControlError> {
    if bytes.len() > REQUEST_LIMIT
        || bytes.last() != Some(&b'\n')
        || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(ControlError::InvalidRequest);
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes[..bytes.len() - 1])
        .map_err(|_| ControlError::InvalidRequest)?;
    let object = value.as_object().ok_or(ControlError::InvalidRequest)?;
    let keys = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if keys != BTreeSet::from(["operation", "schemaVersion"]) {
        return Err(ControlError::InvalidRequest);
    }
    let version = object
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64)
        .ok_or(ControlError::InvalidRequest)?;
    if version != 1 {
        return Err(ControlError::UnsupportedVersion);
    }
    match object.get("operation").and_then(serde_json::Value::as_str) {
        Some("status") => Ok(Operation::Status),
        Some("reload_credential") => Ok(Operation::ReloadCredential),
        Some("sync_agent_logins") => Ok(Operation::SyncAgentLogins),
        _ => Err(ControlError::InvalidRequest),
    }
}

pub(crate) fn encode_request(operation: Operation) -> Vec<u8> {
    let operation = match operation {
        Operation::Status => "status",
        Operation::ReloadCredential => "reload_credential",
        Operation::SyncAgentLogins => "sync_agent_logins",
    };
    let mut request = format!(r#"{{"schemaVersion":1,"operation":"{operation}"}}"#).into_bytes();
    request.push(b'\n');
    request
}

pub(crate) fn encode_response(response: &Response) -> Result<Vec<u8>, ProtocolFailure> {
    let value = match response {
        Response::Status(status) => EncodedResponse::Status {
            schema_version: 1,
            outcome: "ok",
            status,
        },
        Response::Reloaded { credential_id } => EncodedResponse::Reloaded {
            schema_version: 1,
            outcome: "reloaded",
            credential_id,
        },
        Response::AgentLoginsSynced(synced) => EncodedResponse::AgentLoginsSynced {
            schema_version: 1,
            outcome: "synced",
            agent_logins: &synced.readiness,
            unanswered_checks: &synced.unanswered_checks,
        },
        Response::Error(error) => EncodedResponse::Error {
            schema_version: 1,
            outcome: "error",
            error: *error,
        },
    };
    let mut bytes = serde_json::to_vec(&value).map_err(|_| ProtocolFailure::Invalid)?;
    bytes.push(b'\n');
    if bytes.len() > RESPONSE_LIMIT {
        return Err(ProtocolFailure::Oversized);
    }
    Ok(bytes)
}

pub(crate) fn decode_response(bytes: &[u8]) -> Result<Response, ProtocolFailure> {
    if bytes.len() > RESPONSE_LIMIT
        || bytes.last() != Some(&b'\n')
        || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(ProtocolFailure::Invalid);
    }
    let response: DecodedResponse =
        serde_json::from_slice(&bytes[..bytes.len() - 1]).map_err(|_| ProtocolFailure::Invalid)?;
    if response.schema_version != 1 {
        return Err(ProtocolFailure::Invalid);
    }
    match (
        response.outcome.as_str(),
        response.status,
        response.credential_id,
        response.agent_logins,
        response.unanswered_checks,
        response.error,
    ) {
        ("ok", Some(status), None, None, None, None) if valid_status(&status) => {
            Ok(Response::Status(status))
        }
        ("reloaded", None, Some(credential_id), None, None, None)
            if super::validation::valid_typed_id(&credential_id, "rrc_") =>
        {
            Ok(Response::Reloaded { credential_id })
        }
        ("synced", None, None, Some(readiness), unanswered_checks, None)
            if valid_readiness(&readiness)
                && unanswered_checks
                    .as_deref()
                    .is_none_or(valid_unanswered_checks) =>
        {
            Ok(Response::AgentLoginsSynced(SyncedAgentLogins {
                readiness,
                unanswered_checks: unanswered_checks.unwrap_or_default(),
            }))
        }
        ("error", None, None, None, None, Some(error)) => Ok(Response::Error(error)),
        _ => Err(ProtocolFailure::Invalid),
    }
}

/// A `synced` answer's unanswered checks, when it carries the member: at
/// least one, each of a different installable provider.
fn valid_unanswered_checks(checks: &[UnansweredCheck]) -> bool {
    let mut providers = BTreeSet::new();
    !checks.is_empty()
        && checks
            .iter()
            .all(|check| !check.provider.is_brokered() && providers.insert(check.provider))
}

fn valid_status(status: &StatusSnapshot) -> bool {
    super::validation::valid_typed_id(&status.boot_id, "rbt_")
        && status
            .current_credential_id
            .as_deref()
            .is_none_or(|value| super::validation::valid_typed_id(value, "rrc_"))
        && status
            .pending_credential_id
            .as_deref()
            .is_none_or(|value| super::validation::valid_typed_id(value, "rrc_"))
        && status
            .assignment_counts
            .total()
            .is_some_and(|total| total <= 1)
        && status
            .last_connected_at
            .as_deref()
            .is_none_or(valid_timestamp)
        && status.agent_logins.as_ref().is_none_or(valid_readiness)
}

/// The most providers a readiness may list, as in `runner-agent-logins-v1`.
const MAXIMUM_PROVIDERS: usize = 16;

fn valid_readiness(readiness: &AgentLoginReadiness) -> bool {
    let mut providers = BTreeSet::new();
    valid_timestamp(&readiness.updated_at)
        && readiness.providers.len() <= MAXIMUM_PROVIDERS
        && readiness
            .providers
            .iter()
            .all(|entry| providers.insert(entry.provider) && valid_provider_readiness(entry))
}

fn valid_provider_readiness(entry: &ProviderReadiness) -> bool {
    if entry.provider.is_brokered() {
        return valid_grant_readiness(entry);
    }
    entry.state.held_by_login()
        && entry.grant_id.is_none()
        && entry.expires_at.is_none()
        && entry.run_id.is_none()
        && entry.ended_at.is_none()
        && entry
            .agent_login_id
            .as_deref()
            .is_none_or(|value| super::validation::valid_typed_id(value, "agl_"))
        && entry.generation.is_none_or(|generation| generation >= 1)
        && entry.generation.is_some() == entry.installed_at.is_some()
        && (entry.generation.is_none() || entry.agent_login_id.is_some())
        && (entry.last_use.is_none() || entry.generation.is_some())
        && (entry.last_use_at.is_none() || entry.last_use.is_some())
        && entry.last_use_at.as_deref().is_none_or(valid_timestamp)
        && entry.checked_at.as_deref().is_none_or(valid_timestamp)
        && entry.installed_at.as_deref().is_none_or(valid_timestamp)
}

/// An OAuth provider's entry (REQ-GRANT-88): a state such an entry holds,
/// with the grant, generation and expiry exactly when `ready`, a run and an
/// end only beside a check time, and none of an agent login's members.
fn valid_grant_readiness(entry: &ProviderReadiness) -> bool {
    let issued = match (&entry.grant_id, entry.generation, &entry.expires_at) {
        (None, None, None) => false,
        (Some(grant_id), Some(generation), Some(expires_at)) => {
            if !super::validation::valid_typed_id(grant_id, "pgr_")
                || generation == 0
                || !valid_timestamp(expires_at)
            {
                return false;
            }
            true
        }
        _ => return false,
    };
    entry.state.held_by_grant()
        && issued == (entry.state == AgentLoginState::Ready)
        && entry.agent_login_id.is_none()
        && entry.installed_at.is_none()
        && entry.last_use.is_none()
        && entry.last_use_at.is_none()
        && entry.checked_at.as_deref().is_none_or(valid_timestamp)
        && entry
            .run_id
            .as_deref()
            .is_none_or(|value| super::validation::valid_typed_id(value, "run_"))
        && entry.ended_at.as_deref().is_none_or(valid_timestamp)
        && (entry.checked_at.is_some() || (entry.run_id.is_none() && entry.ended_at.is_none()))
}

fn valid_timestamp(value: &str) -> bool {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_is_closed_and_lf_delimited() {
        assert_eq!(
            decode_request(b"{\"schemaVersion\":1,\"operation\":\"status\"}\n"),
            Ok(Operation::Status)
        );
        assert_eq!(
            decode_request(b"{\"schemaVersion\":2,\"operation\":\"status\"}\n"),
            Err(ControlError::UnsupportedVersion)
        );
        for request in [
            b"{\"schemaVersion\":1,\"operation\":\"other\"}\n".as_slice(),
            b"{\"schemaVersion\":1,\"operation\":\"status\",\"path\":\"/tmp/x\"}\n",
            b"{\"schemaVersion\":1,\"operation\":\"status\"}",
            b"{}\n",
        ] {
            assert_eq!(decode_request(request), Err(ControlError::InvalidRequest));
        }
    }

    #[test]
    fn request_and_response_bounds_include_the_lf_delimiter() {
        let mut request = encode_request(Operation::Status);
        request.splice(
            request.len() - 1..request.len() - 1,
            std::iter::repeat_n(b' ', REQUEST_LIMIT - request.len()),
        );
        assert_eq!(request.len(), REQUEST_LIMIT);
        assert_eq!(decode_request(&request), Ok(Operation::Status));
        request.insert(request.len() - 1, b' ');
        assert_eq!(decode_request(&request), Err(ControlError::InvalidRequest));

        let response = Response::Status(StatusSnapshot {
            process_state: ProcessState::Running,
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            uptime_milliseconds: 0,
            connection_state: ConnectionState::Connecting,
            last_connected_at: None,
            current_credential_id: None,
            pending_credential_id: None,
            assignment_counts: AssignmentCounts::default(),
            last_connection_failure: None,
            agent_logins: None,
        });
        let mut encoded = encode_response(&response).unwrap();
        encoded.splice(
            encoded.len() - 1..encoded.len() - 1,
            std::iter::repeat_n(b' ', RESPONSE_LIMIT - encoded.len()),
        );
        assert_eq!(encoded.len(), RESPONSE_LIMIT);
        assert_eq!(decode_response(&encoded), Ok(response));
        encoded.insert(encoded.len() - 1, b' ');
        assert_eq!(decode_response(&encoded), Err(ProtocolFailure::Invalid));
    }

    // REQ-RUNNERENROLL-43
    #[test]
    fn canonical_schema_accepts_only_valid_control_fixtures() {
        let cli_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let validator = control_validator();
        let fixtures = cli_root.join("tests/fixtures/runner-control/v1");
        for (kind, expected) in [("valid", true), ("invalid", false)] {
            for entry in std::fs::read_dir(fixtures.join(kind)).unwrap() {
                let path = entry.unwrap().path();
                let value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                assert_eq!(validator.is_valid(&value), expected, "{}", path.display());
            }
        }
    }

    fn readiness() -> AgentLoginReadiness {
        let brokered = |provider| ProviderReadiness {
            provider,
            state: AgentLoginState::NotDelivered,
            agent_login_id: None,
            generation: None,
            checked_at: None,
            installed_at: None,
            grant_id: None,
            expires_at: None,
            selected: None,
            last_use: None,
            last_use_at: None,
            run_id: None,
            ended_at: None,
        };
        AgentLoginReadiness {
            updated_at: "2026-11-12T01:00:00Z".to_owned(),
            providers: vec![
                ProviderReadiness {
                    provider: AgentLoginProvider::Codex,
                    state: AgentLoginState::Ready,
                    agent_login_id: Some("agl_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
                    generation: Some(3),
                    checked_at: Some("2026-11-12T01:00:00Z".to_owned()),
                    installed_at: Some("2026-11-12T00:55:00Z".to_owned()),
                    grant_id: None,
                    expires_at: None,
                    selected: None,
                    last_use: None,
                    last_use_at: None,
                    run_id: None,
                    ended_at: None,
                },
                brokered(AgentLoginProvider::Github),
                brokered(AgentLoginProvider::Gitlab),
                brokered(AgentLoginProvider::Linear),
            ],
        }
    }

    /// The `synced` answer carrying `readiness` and no unanswered check.
    fn synced(readiness: AgentLoginReadiness) -> Response {
        Response::AgentLoginsSynced(SyncedAgentLogins {
            readiness,
            unanswered_checks: Vec::new(),
        })
    }

    fn control_validator() -> jsonschema::Validator {
        let cli_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let schema: serde_json::Value = serde_json::from_slice(
            &std::fs::read(cli_root.join("schemas/runner-control-v1.schema.json")).unwrap(),
        )
        .unwrap();
        jsonschema::validator_for(&schema).unwrap()
    }

    // REQ-RUNNERENROLL-63, REQ-RUNNERENROLL-82
    #[test]
    fn sync_request_decodes_and_its_answer_is_closed_and_pathless() {
        let request = encode_request(Operation::SyncAgentLogins);
        assert_eq!(
            request,
            b"{\"schemaVersion\":1,\"operation\":\"sync_agent_logins\"}\n"
        );
        assert_eq!(decode_request(&request), Ok(Operation::SyncAgentLogins));
        let validator = control_validator();
        let request: serde_json::Value = serde_json::from_slice(&request).unwrap();
        assert!(validator.is_valid(&request));

        let response = synced(readiness());
        let encoded = encode_response(&response).unwrap();
        assert_eq!(decode_response(&encoded), Ok(response));
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(value["outcome"], "synced");
        assert!(validator.is_valid(&value));

        let busy = encode_response(&Response::Error(ControlError::RunnerBusy)).unwrap();
        assert!(validator.is_valid(&serde_json::from_slice(&busy).unwrap()));

        // The control socket returns no path: an entry that names one is
        // neither valid nor decoded.
        value["agentLogins"]["providers"][0]["installedPath"] =
            serde_json::Value::from("/var/lib/um-runner/.runner-agent-login/codex/auth.json");
        assert!(!validator.is_valid(&value));
        let mut with_path = serde_json::to_vec(&value).unwrap();
        with_path.push(b'\n');
        assert_eq!(decode_response(&with_path), Err(ProtocolFailure::Invalid));
    }

    // REQ-GRANT-41, REQ-GRANT-90
    #[test]
    fn a_synced_answer_names_each_unanswered_check_by_provider_and_closed_cause() {
        let response = Response::AgentLoginsSynced(SyncedAgentLogins {
            readiness: readiness(),
            unanswered_checks: vec![UnansweredCheck {
                provider: AgentLoginProvider::Codex,
                cause: AgentLoginCheckCause::TimedOut,
            }],
        });
        let encoded = encode_response(&response).unwrap();
        assert_eq!(decode_response(&encoded), Ok(response));
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            value["unansweredChecks"],
            serde_json::json!([{"provider": "codex", "cause": "timed_out"}])
        );

        // Absent when every check answered, and only then.
        let answered = encode_response(&synced(readiness())).unwrap();
        let answered: serde_json::Value = serde_json::from_slice(&answered).unwrap();
        assert!(answered.get("unansweredChecks").is_none(), "{answered}");
        for checks in [
            serde_json::json!([]),
            serde_json::json!([{"provider": "codex", "cause": "overloaded"}]),
            serde_json::json!([{"provider": "github", "cause": "timed_out"}]),
            serde_json::json!([
                {"provider": "codex", "cause": "timed_out"},
                {"provider": "codex", "cause": "start_failed"}
            ]),
            serde_json::json!([{"provider": "codex", "cause": "timed_out", "detail": "x"}]),
        ] {
            let mut invalid = answered.clone();
            invalid["unansweredChecks"] = checks;
            let mut bytes = serde_json::to_vec(&invalid).unwrap();
            bytes.push(b'\n');
            assert_eq!(
                decode_response(&bytes),
                Err(ProtocolFailure::Invalid),
                "{invalid}"
            );
        }
    }

    /// An OAuth provider's entry in `state`, holding the grant issued when
    /// it is `ready` (REQ-GRANT-88).
    fn grant_entry(state: AgentLoginState, selected: Option<bool>) -> ProviderReadiness {
        let ready = state == AgentLoginState::Ready;
        ProviderReadiness {
            provider: AgentLoginProvider::Github,
            state,
            agent_login_id: None,
            generation: ready.then_some(4),
            checked_at: Some("2026-11-12T01:00:00Z".to_owned()),
            installed_at: None,
            grant_id: ready.then(|| "pgr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
            expires_at: ready.then(|| "2026-11-12T09:00:00Z".to_owned()),
            selected,
            last_use: None,
            last_use_at: None,
            run_id: None,
            ended_at: None,
        }
    }

    /// Every state an OAuth provider's entry may hold (REQ-GRANT-88).
    const GRANT_STATES: [AgentLoginState; 8] = [
        AgentLoginState::NotDelivered,
        AgentLoginState::Ready,
        AgentLoginState::NotAuthorized,
        AgentLoginState::Revoked,
        AgentLoginState::ReconnectRequired,
        AgentLoginState::UnsupportedTarget,
        AgentLoginState::CloudUnavailable,
        AgentLoginState::InstallFailed,
    ];

    // REQ-GRANT-88
    #[test]
    fn readiness_carries_each_oauth_entry_and_only_as_the_schema_admits_it() {
        let validator = control_validator();
        for state in GRANT_STATES {
            let mut readiness = readiness();
            readiness.providers[1] = grant_entry(state, Some(true));
            let response = synced(readiness);
            let encoded = encode_response(&response).unwrap();
            assert_eq!(decode_response(&encoded), Ok(response), "{state:?}");
            let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
            assert!(validator.is_valid(&value), "{value}");
        }
    }

    const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";

    // REQ-GRANT-88
    #[test]
    fn an_oauth_entry_carries_its_run_and_end_and_only_well_formed() {
        let validator = control_validator();
        let ended = ProviderReadiness {
            run_id: Some(RUN_ID.to_owned()),
            ended_at: Some("2026-11-12T01:05:00Z".to_owned()),
            ..grant_entry(AgentLoginState::Ready, Some(true))
        };
        let mut carrying = readiness();
        carrying.providers[1] = ended.clone();
        let response = synced(carrying);
        let encoded = encode_response(&response).unwrap();
        assert_eq!(decode_response(&encoded), Ok(response));
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert!(validator.is_valid(&value), "{value}");
        assert_eq!(value["agentLogins"]["providers"][1]["runId"], RUN_ID);
        assert_eq!(
            value["agentLogins"]["providers"][1]["endedAt"],
            "2026-11-12T01:05:00Z"
        );

        let codex = readiness().providers[0].clone();
        for (index, invalid) in [
            (
                1,
                ProviderReadiness {
                    run_id: Some("asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
                    ..ended.clone()
                },
            ),
            (
                1,
                ProviderReadiness {
                    ended_at: Some("after the run".to_owned()),
                    ..ended.clone()
                },
            ),
            (
                1,
                ProviderReadiness {
                    checked_at: None,
                    ended_at: None,
                    ..ended.clone()
                },
            ),
            (
                1,
                ProviderReadiness {
                    checked_at: None,
                    run_id: None,
                    ..ended.clone()
                },
            ),
            (
                0,
                ProviderReadiness {
                    run_id: Some(RUN_ID.to_owned()),
                    ..codex.clone()
                },
            ),
            (
                0,
                ProviderReadiness {
                    ended_at: Some("2026-11-12T01:05:00Z".to_owned()),
                    ..codex.clone()
                },
            ),
        ] {
            let mut readiness = readiness();
            readiness.providers[index] = invalid.clone();
            let encoded = encode_response(&synced(readiness)).unwrap();
            assert_eq!(
                decode_response(&encoded),
                Err(ProtocolFailure::Invalid),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn readiness_is_refused_unless_each_entry_is_one_its_provider_holds() {
        let mut brokered_ready_without_grant = readiness();
        brokered_ready_without_grant.providers[1].state = AgentLoginState::Ready;
        let mut brokered_grant_not_ready = readiness();
        brokered_grant_not_ready.providers[1] = ProviderReadiness {
            state: AgentLoginState::Revoked,
            ..grant_entry(AgentLoginState::Ready, Some(false))
        };
        let mut brokered_malformed_grant = readiness();
        brokered_malformed_grant.providers[1] = ProviderReadiness {
            grant_id: Some("agl_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
            ..grant_entry(AgentLoginState::Ready, Some(true))
        };
        let mut brokered_agent_login_state = readiness();
        brokered_agent_login_state.providers[1] =
            grant_entry(AgentLoginState::ForeignLogin, Some(true));
        let mut brokered_install = readiness();
        brokered_install.providers[1] = ProviderReadiness {
            installed_at: Some("2026-11-12T00:55:00Z".to_owned()),
            ..grant_entry(AgentLoginState::Ready, Some(true))
        };
        let mut brokered_last_use_at = readiness();
        brokered_last_use_at.providers[1] = ProviderReadiness {
            last_use_at: Some("2026-11-12T00:58:00Z".to_owned()),
            ..grant_entry(AgentLoginState::Ready, Some(true))
        };
        let mut codex_not_delivered = readiness();
        codex_not_delivered.providers[0] = ProviderReadiness {
            state: AgentLoginState::NotDelivered,
            agent_login_id: None,
            generation: None,
            checked_at: None,
            installed_at: None,
            ..codex_not_delivered.providers[0].clone()
        };
        let mut codex_reconnect_required = readiness();
        codex_reconnect_required.providers[0].state = AgentLoginState::ReconnectRequired;
        let mut codex_grant = readiness();
        codex_grant.providers[0].grant_id = Some("pgr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned());
        let mut duplicate = readiness();
        duplicate.providers.push(duplicate.providers[1].clone());
        let mut generation_without_install = readiness();
        generation_without_install.providers[0].installed_at = None;
        let mut last_use_without_install = readiness();
        last_use_without_install.providers[0] = ProviderReadiness {
            state: AgentLoginState::NotAuthorized,
            generation: None,
            installed_at: None,
            last_use: Some(AgentLoginUse::Refused),
            ..last_use_without_install.providers[0].clone()
        };
        // A time is stated only with the last use it dates (REQ-GRANT-41).
        let mut last_use_at_without_last_use = readiness();
        last_use_at_without_last_use.providers[0].last_use_at =
            Some("2026-11-12T00:58:00Z".to_owned());
        for invalid in [
            brokered_ready_without_grant,
            brokered_grant_not_ready,
            brokered_malformed_grant,
            brokered_agent_login_state,
            brokered_install,
            brokered_last_use_at,
            codex_not_delivered,
            codex_reconnect_required,
            codex_grant,
            duplicate,
            generation_without_install,
            last_use_without_install,
            last_use_at_without_last_use,
        ] {
            let encoded = encode_response(&synced(invalid)).unwrap();
            assert_eq!(decode_response(&encoded), Err(ProtocolFailure::Invalid));
        }
    }

    // REQ-GRANT-53, REQ-GRANT-88
    #[test]
    fn an_oauth_entry_projects_like_an_agent_login_and_never_fails_readiness() {
        use AgentLoginState as S;
        use um_support::{Availability as A, NextAction as N};
        for (state, availability, next_action) in [
            (S::Ready, A::Available, N::None),
            (S::ReconnectRequired, A::ReconnectRequired, N::Reconnect),
            (S::NotAuthorized, A::NotConnected, N::None),
            (S::Revoked, A::NotConnected, N::None),
            (S::UnsupportedTarget, A::DeliveryFailed, N::CheckTarget),
            (S::CloudUnavailable, A::DeliveryFailed, N::SyncRunner),
            (S::InstallFailed, A::DeliveryFailed, N::SyncRunner),
            (S::NotDelivered, A::NotDelivered, N::None),
        ] {
            for selected in [Some(true), Some(false), None] {
                let entry = grant_entry(state, selected);
                // No answer leaves a provider `not_authorized` and selected.
                if state == S::NotAuthorized && selected == Some(true) {
                    assert_eq!(entry.failure_code(), None);
                    continue;
                }
                assert_eq!(
                    entry.credential_status(),
                    um_support::CredentialStatus {
                        availability,
                        next_action,
                    },
                    "{state:?} {selected:?}"
                );
                // One assignment's outcome, never the runner's
                // (REQ-GRANT-88).
                assert_eq!(entry.failure_code(), None, "{state:?} {selected:?}");
                assert!(!entry.fails_readiness(), "{state:?} {selected:?}");
            }
        }
        // `ready` is `available` whatever the last use, which no grant
        // token records.
        let mut refused = grant_entry(S::Ready, Some(true));
        refused.last_use = Some(AgentLoginUse::Refused);
        assert_eq!(
            refused.credential_status().availability,
            um_support::Availability::Available
        );
    }

    // REQ-GRANT-53, REQ-GRANT-88
    #[test]
    fn an_ended_oauth_entry_that_was_ready_is_unverified_and_no_other_state_changes() {
        use um_support::{Availability as A, CredentialStatus, NextAction as N};
        for state in GRANT_STATES {
            for selected in [Some(true), Some(false), None] {
                let current = grant_entry(state, selected);
                let mut ended = current.clone();
                ended.run_id = Some("run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned());
                ended.ended_at = Some("2026-11-12T02:00:00Z".to_owned());
                if state == AgentLoginState::Ready {
                    assert_eq!(
                        current.credential_status(),
                        CredentialStatus {
                            availability: A::Available,
                            next_action: N::None,
                        },
                        "{selected:?}"
                    );
                    // An ended assignment's token says nothing of the next one.
                    assert_eq!(
                        ended.credential_status(),
                        CredentialStatus {
                            availability: A::Unverified,
                            next_action: N::None,
                        },
                        "{selected:?}"
                    );
                } else {
                    assert_eq!(
                        ended.credential_status(),
                        current.credential_status(),
                        "{state:?} {selected:?}"
                    );
                }
                assert_eq!(current.failure_code(), None, "{state:?} {selected:?}");
                assert_eq!(ended.failure_code(), None, "{state:?} {selected:?}");
            }
        }
    }

    // REQ-GRANT-41
    #[test]
    fn only_an_authorized_provider_that_is_not_ready_fails_readiness() {
        for (state, fails) in [
            (AgentLoginState::Ready, false),
            (AgentLoginState::NotAuthorized, false),
            (AgentLoginState::Revoked, false),
            (AgentLoginState::NotDelivered, false),
            (AgentLoginState::ForeignLogin, true),
            (AgentLoginState::UnsupportedTarget, true),
            (AgentLoginState::StaleGeneration, true),
            (AgentLoginState::CloudUnavailable, true),
            (AgentLoginState::InstallFailed, true),
            (AgentLoginState::ReconnectRequired, true),
        ] {
            assert_eq!(state.fails_readiness(), fails, "{state:?}");
        }
    }

    // REQ-GRANT-53
    #[test]
    fn each_readiness_maps_to_one_availability_and_next_action() {
        use AgentLoginState as S;
        use um_support::{Availability as A, NextAction as N};
        let accepted = Some(AgentLoginUse::Accepted);
        let refused = Some(AgentLoginUse::Refused);
        for (state, selected, last_use, availability, next_action) in [
            (S::Ready, Some(true), accepted, A::Available, N::None),
            (S::Ready, Some(true), None, A::Unverified, N::None),
            (
                S::Ready,
                Some(true),
                refused,
                A::ReconnectRequired,
                N::Reconnect,
            ),
            (
                S::InstallFailed,
                Some(true),
                None,
                A::DeliveryFailed,
                N::SyncRunner,
            ),
            (
                S::StaleGeneration,
                Some(true),
                None,
                A::DeliveryFailed,
                N::SyncRunner,
            ),
            (
                S::CloudUnavailable,
                Some(true),
                None,
                A::DeliveryFailed,
                N::SyncRunner,
            ),
            (
                S::UnsupportedTarget,
                Some(true),
                None,
                A::DeliveryFailed,
                N::CheckTarget,
            ),
            (
                S::NotAuthorized,
                Some(true),
                None,
                A::InsufficientPermission,
                N::RestoreMembership,
            ),
            (
                S::NotAuthorized,
                Some(false),
                None,
                A::NotConnected,
                N::None,
            ),
            // An entry written before selection was recorded is selected
            // by its state (REQ-GRANT-38): `not_authorized` is not.
            (S::NotAuthorized, None, None, A::NotConnected, N::None),
            (S::Revoked, Some(false), None, A::NotConnected, N::None),
            (
                S::ForeignLogin,
                Some(true),
                None,
                A::NotDelivered,
                N::ResolveServiceLogin,
            ),
            (S::NotDelivered, None, None, A::NotDelivered, N::None),
        ] {
            let entry = ProviderReadiness {
                provider: AgentLoginProvider::Codex,
                state,
                agent_login_id: None,
                generation: None,
                checked_at: None,
                installed_at: None,
                grant_id: None,
                expires_at: None,
                selected,
                last_use,
                last_use_at: None,
                run_id: None,
                ended_at: None,
            };
            assert_eq!(
                entry.credential_status(),
                um_support::CredentialStatus {
                    availability,
                    next_action,
                },
                "{state:?} {selected:?} {last_use:?}"
            );
            // The mapping keeps every next action an availability fixes.
            assert!(
                availability
                    .fixed_next_action()
                    .is_none_or(|fixed| fixed == next_action),
                "{availability:?}"
            );
        }
    }

    // REQ-GRANT-41, REQ-RUNNERENROLL-86
    #[test]
    fn a_ready_login_refused_at_its_last_use_fails_readiness() {
        let mut entry = readiness().providers[0].clone();
        for (last_use, code) in [
            (None, None),
            (Some(AgentLoginUse::Accepted), None),
            (Some(AgentLoginUse::Refused), Some("reconnect_required")),
        ] {
            entry.last_use = last_use;
            assert_eq!(entry.failure_code(), code, "{last_use:?}");
            assert_eq!(entry.fails_readiness(), code.is_some(), "{last_use:?}");
        }
        entry.state = AgentLoginState::InstallFailed;
        assert_eq!(entry.failure_code(), Some("install_failed"));
    }

    // REQ-GRANT-41, REQ-RUNNERENROLL-86, REQ-GRANT-51
    #[test]
    fn a_selected_provider_cloud_refuses_fails_readiness() {
        // Cloud names an authorization in force and refuses its owner: the
        // login is withheld from every assignment, so the provider is not
        // ready. One nobody authorized, recorded as not selected or before
        // selection was recorded, is not a failure.
        let mut entry = readiness().providers[0].clone();
        entry.state = AgentLoginState::NotAuthorized;
        entry.generation = None;
        entry.installed_at = None;
        entry.last_use = None;
        for (selected, code) in [
            (Some(true), Some("not_authorized")),
            (Some(false), None),
            (None, None),
        ] {
            entry.selected = selected;
            assert_eq!(entry.failure_code(), code, "{selected:?}");
            assert_eq!(entry.fails_readiness(), code.is_some(), "{selected:?}");
        }
    }

    // REQ-RUNNERENROLL-42
    #[test]
    fn status_reads_without_readiness_and_round_trips_with_it() {
        let without = b"{\"schemaVersion\":1,\"outcome\":\"ok\",\"status\":{\"processState\":\"running\",\"bootId\":\"rbt_01k0z6r1w8f4jy2m7q9v3x5abc\",\"uptimeMilliseconds\":10,\"connectionState\":\"connected\",\"assignmentCounts\":{\"preparing\":0,\"accepted\":0,\"running\":0,\"finishing\":0,\"reporting\":0}}}\n";
        let Ok(Response::Status(status)) = decode_response(without) else {
            panic!("a status without readiness did not decode");
        };
        assert_eq!(status.agent_logins, None);
        assert_eq!(
            encode_response(&Response::Status(status.clone())).unwrap(),
            without
        );

        let response = Response::Status(StatusSnapshot {
            agent_logins: Some(readiness()),
            ..status
        });
        let encoded = encode_response(&response).unwrap();
        assert_eq!(decode_response(&encoded), Ok(response));
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert!(control_validator().is_valid(&value));
        assert_eq!(
            value["status"]["agentLogins"]["providers"][0]["generation"],
            3
        );

        // Selection and last use, with its time, travel beside the state
        // where the document records them (REQ-GRANT-41, REQ-GRANT-53).
        let mut with_last_use = readiness();
        with_last_use.providers[0].selected = Some(true);
        with_last_use.providers[0].last_use = Some(AgentLoginUse::Refused);
        with_last_use.providers[0].last_use_at = Some("2026-11-12T00:58:00Z".to_owned());
        let response = synced(with_last_use);
        let encoded = encode_response(&response).unwrap();
        assert_eq!(decode_response(&encoded), Ok(response));
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert!(control_validator().is_valid(&value), "{value}");
        assert_eq!(value["agentLogins"]["providers"][0]["selected"], true);
        assert_eq!(value["agentLogins"]["providers"][0]["lastUse"], "refused");
        assert_eq!(
            value["agentLogins"]["providers"][0]["lastUseAt"],
            "2026-11-12T00:58:00Z"
        );
    }

    // REQ-RUNNERENROLL-82
    #[test]
    fn error_responses_carry_only_the_closed_control_error_set() {
        let closed = [
            (ControlError::InvalidRequest, "invalid_request"),
            (ControlError::UnsupportedVersion, "unsupported_version"),
            (ControlError::RunnerBusy, "runner_busy"),
            (ControlError::NoPendingCredential, "no_pending_credential"),
            (
                ControlError::PendingRegistrationMismatch,
                "pending_registration_mismatch",
            ),
            (
                ControlError::PendingAuthenticationFailed,
                "pending_authentication_failed",
            ),
            (
                ControlError::PendingProtocolFailed,
                "pending_protocol_failed",
            ),
            (
                ControlError::PendingConnectionFailed,
                "pending_connection_failed",
            ),
            (ControlError::StateUpdateFailed, "state_update_failed"),
        ];
        let cli_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let schema: serde_json::Value = serde_json::from_slice(
            &std::fs::read(cli_root.join("schemas/runner-control-v1.schema.json")).unwrap(),
        )
        .unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let schema_codes = schema["$defs"]["errorResponse"]["properties"]["error"]["enum"]
            .as_array()
            .unwrap()
            .iter()
            .map(|code| code.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            schema_codes,
            closed.iter().map(|(_, code)| *code).collect::<Vec<_>>()
        );
        for (error, code) in closed {
            let encoded = encode_response(&Response::Error(error)).unwrap();
            let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(value["outcome"], "error");
            assert_eq!(value["error"], code);
            assert!(validator.is_valid(&value), "{code}");
            assert_eq!(decode_response(&encoded), Ok(Response::Error(error)));
        }
        assert_eq!(
            decode_response(
                b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"credential_missing\"}\n"
            ),
            Err(ProtocolFailure::Invalid)
        );
    }

    #[test]
    fn status_round_trips_with_exact_idle_counts() {
        let response = Response::Status(StatusSnapshot {
            process_state: ProcessState::Running,
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            uptime_milliseconds: 10,
            connection_state: ConnectionState::Connecting,
            last_connected_at: None,
            current_credential_id: Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
            pending_credential_id: None,
            assignment_counts: AssignmentCounts::default(),
            last_connection_failure: None,
            agent_logins: None,
        });
        let encoded = encode_response(&response).unwrap();
        assert_eq!(decode_response(&encoded).unwrap(), response);
        assert_eq!(encoded.iter().filter(|byte| **byte == b'\n').count(), 1);
        let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            value["status"]["assignmentCounts"]
                .as_object()
                .unwrap()
                .len(),
            5
        );
    }
}
