use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};
use useful_support::{Availability, CredentialStatus, NextAction, is_identifier};

pub(crate) const REQUEST_LIMIT: usize = 4_096;
pub(crate) const RESPONSE_LIMIT: usize = 65_536;
/// The bound of the two open answers, `overview` and `assignment`, including the LF
/// (REQ-RUNNERENROLL-136); every other answer keeps `RESPONSE_LIMIT`.
pub(crate) const OVERVIEW_RESPONSE_LIMIT: usize = 1_048_576;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Status,
    ReloadCredential,
    SyncAgentLogins,
}

/// A request the serving process reads: one of the operations `request` sends, or the
/// `overview` or the `assignment` a client asks for by name (REQ-RUNNERENROLL-135).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ControlRequest {
    Operation(Operation),
    Overview,
    Assignment(AssignmentQuery),
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
    /// The held tree runs whose loops last reported a wait, which are not counted
    /// `running` (REQ-RUNNERENROLL-80). Omitted when zero, so a `useful runner status`
    /// that predates it reads a serving process that holds no waiting run, and absent
    /// from the answer of a serving process that predates it.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub waiting: u64,
    pub finishing: u64,
    pub reporting: u64,
}

impl AssignmentCounts {
    pub fn total(self) -> Option<u64> {
        self.preparing
            .checked_add(self.accepted)?
            .checked_add(self.running)?
            .checked_add(self.waiting)?
            .checked_add(self.finishing)?
            .checked_add(self.reporting)
    }
}

fn is_zero(count: &u64) -> bool {
    *count == 0
}

fn is_false(flag: &bool) -> bool {
    !*flag
}

/// Whether a held run is working or waiting, the two words Cloud's reads of a runner's
/// runs use (REQ-RUNNERENROLL-42).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HeldRunState {
    Working,
    Waiting,
}

/// A run the runner holds that has not ended (REQ-RUNNERENROLL-42).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HeldRun {
    pub run_id: String,
    pub state: HeldRunState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StatusSnapshot {
    pub process_state: ProcessState,
    pub boot_id: String,
    pub uptime_milliseconds: u64,
    /// The absolute path of the log file `runner.log` the process appends its records
    /// to, present exactly when the process opened its log (REQ-RUNNERENROLL-42,
    /// REQ-TELEMETRY-52).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_path: Option<String>,
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
    /// after the runner starts; from then on a `useful runner status` older than
    /// agent logins refuses the answer, the replaced-binary case chapter 11
    /// documents.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_logins: Option<AgentLoginReadiness>,
    /// Each run the runner holds that has not ended, the working ones first and then in
    /// the order the runner took them (REQ-RUNNERENROLL-42). Omitted when empty, so a
    /// `useful runner status` that predates it reads a serving process that holds no run,
    /// and absent from the answer of a serving process that predates it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub held_runs: Vec<HeldRun>,
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
    /// route and `useful integration` take: GitLab is the instance `gitlab.com`.
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
    /// runner without its login being ready: every state but `ready`, the
    /// two Cloud answers that no login is authorized, and the brokered
    /// providers' `not_delivered`. `cloud_unavailable` counts: a runner
    /// records it only for a provider it last knew as authorized or
    /// installed (REQ-GRANT-38). `foreign_login` counts too, though it fails
    /// nothing (`fails_readiness`).
    pub const fn authorized_not_ready(self) -> bool {
        !matches!(
            self,
            Self::Ready | Self::NotAuthorized | Self::Revoked | Self::NotDelivered
        )
    }

    /// Whether an agent login's provider in the state fails readiness
    /// (REQ-GRANT-41, REQ-RUNNERENROLL-86): it is authorized and not ready,
    /// and not in `foreign_login`, where the host's own login is in use
    /// (REQ-GRANT-51).
    pub const fn fails_readiness(self) -> bool {
        self.authorized_not_ready() && !matches!(self, Self::ForeignLogin)
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

/// Where a provider's credential comes from on the runner (REQ-GRANT-92).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialSource {
    /// The runner's host provides it: the service environment carries an
    /// OAuth provider's names, or the service has a coding agent's login of
    /// its own.
    Host,
    /// A member's grant provides it: the token an assignment was issued and
    /// holds, or the agent login the runner keeps and offers.
    Grant,
}

impl CredentialSource {
    /// The source's name on the wire.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Grant => "grant",
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
    /// Whether an earlier release recorded the provider as selected
    /// (REQ-GRANT-38): accepted from a serving process of that release, and
    /// never written.
    #[serde(default, rename = "selected", skip_serializing)]
    pub(crate) earlier_selection: EarlierSelection,
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
    /// Where the provider's credential comes from (REQ-GRANT-92), as the
    /// readiness document records it: a coding agent's as of the last setup
    /// pass, an OAuth provider's for the run of `run_id`, beside which alone
    /// it is recorded; absent for a provider neither the host nor a grant
    /// provides, and where a serving process that predates it answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<CredentialSource>,
}

/// The `selected` member an earlier release wrote on a provider's entry
/// (REQ-GRANT-38): read as a boolean so what that release wrote still
/// parses, and holding nothing of it, since no provider is selected
/// (REQ-GRANT-51). Nothing writes it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct EarlierSelection;

impl<'de> Deserialize<'de> for EarlierSelection {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        bool::deserialize(deserializer).map(|_| Self)
    }
}

impl ProviderReadiness {
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
    /// its state when that fails readiness; `reconnect_required` for a
    /// `ready` login refused at its last use; and `None` otherwise:
    /// `foreign_login`, where the host's own login is in use
    /// (REQ-GRANT-51), `not_authorized` and an OAuth provider's entry
    /// included, which records one assignment's outcome and not the
    /// runner's (REQ-GRANT-88).
    pub const fn failure_code(&self) -> Option<&'static str> {
        if self.provider.is_brokered() {
            None
        } else if self.state.fails_readiness() {
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

pub(crate) fn decode_request(bytes: &[u8]) -> Result<ControlRequest, ControlError> {
    let request = request_object(bytes)?;
    match request.get("operation").and_then(serde_json::Value::as_str) {
        Some("status") => Ok(ControlRequest::Operation(Operation::Status)),
        Some("reload_credential") => Ok(ControlRequest::Operation(Operation::ReloadCredential)),
        Some("sync_agent_logins") => Ok(ControlRequest::Operation(Operation::SyncAgentLogins)),
        Some("overview") => Ok(ControlRequest::Overview),
        // An `assignment` request with a member it does not admit is `invalid_request`
        // (REQ-RUNNERENROLL-144).
        Some("assignment") => assignment_query(&request)
            .map(ControlRequest::Assignment)
            .ok_or(ControlError::InvalidRequest),
        _ => Err(ControlError::InvalidRequest),
    }
}

/// One request line's object, once it holds exactly the members its operation admits and
/// names this version: `schemaVersion` and `operation`, and for `assignment` also
/// `assignmentId` and, optionally, `run` and `step`.
fn request_object(
    bytes: &[u8],
) -> Result<serde_json::Map<String, serde_json::Value>, ControlError> {
    if bytes.len() > REQUEST_LIMIT
        || bytes.last() != Some(&b'\n')
        || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(ControlError::InvalidRequest);
    }
    let serde_json::Value::Object(object) = serde_json::from_slice(&bytes[..bytes.len() - 1])
        .map_err(|_| ControlError::InvalidRequest)?
    else {
        return Err(ControlError::InvalidRequest);
    };
    let (required, optional): (&[&str], &[&str]) =
        match object.get("operation").and_then(serde_json::Value::as_str) {
            Some("assignment") => (
                &["schemaVersion", "operation", "assignmentId"],
                &["run", "step"],
            ),
            _ => (&["schemaVersion", "operation"], &[]),
        };
    if !required.iter().all(|key| object.contains_key(*key))
        || object
            .keys()
            .any(|key| !required.contains(&key.as_str()) && !optional.contains(&key.as_str()))
    {
        return Err(ControlError::InvalidRequest);
    }
    let version = object
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64)
        .ok_or(ControlError::InvalidRequest)?;
    if version != 1 {
        return Err(ControlError::UnsupportedVersion);
    }
    Ok(object)
}

/// The query of an `assignment` request whose members are the ones it admits.
fn assignment_query(
    request: &serde_json::Map<String, serde_json::Value>,
) -> Option<AssignmentQuery> {
    let assignment_id = request
        .get("assignmentId")?
        .as_str()
        .filter(|id| super::validation::valid_typed_id(id, "asn_"))?
        .to_owned();
    let run = match request.get("run") {
        None => Vec::new(),
        Some(serde_json::Value::Array(elements)) if elements.len() <= MAXIMUM_RUN_PATH => elements
            .iter()
            .map(requested_run_element)
            .collect::<Option<Vec<_>>>()?,
        Some(_) => return None,
    };
    let step = match request.get("step") {
        None => None,
        Some(step) => Some(step.as_str().filter(|step| is_identifier(step))?.to_owned()),
    };
    Some(AssignmentQuery {
        assignment_id,
        run,
        step,
    })
}

/// One level of a requested child run's path: exactly `step`, an optional `item` and
/// `ordinal`, each number within the bound the engine puts on it.
fn requested_run_element(element: &serde_json::Value) -> Option<OverviewRunElement> {
    let element = element.as_object()?;
    if element
        .keys()
        .any(|key| !matches!(key.as_str(), "step" | "item" | "ordinal"))
    {
        return None;
    }
    let number = |value: &serde_json::Value| {
        value
            .as_u64()
            .filter(|number| *number <= MAXIMUM_RUN_NUMBER)
    };
    let item = match element.get("item") {
        None => None,
        Some(item) => Some(number(item)?),
    };
    Some(OverviewRunElement {
        step: element
            .get("step")?
            .as_str()
            .filter(|step| is_identifier(step))?
            .to_owned(),
        item,
        ordinal: number(element.get("ordinal")?)?,
    })
}

pub(crate) fn encode_request(operation: Operation) -> Vec<u8> {
    encode_operation(match operation {
        Operation::Status => "status",
        Operation::ReloadCredential => "reload_credential",
        Operation::SyncAgentLogins => "sync_agent_logins",
    })
}

/// The `overview` request (REQ-RUNNERENROLL-135), which a client asks for by name and
/// `Operation` does not carry.
pub(crate) fn encode_overview_request() -> Vec<u8> {
    encode_operation("overview")
}

fn encode_operation(operation: &str) -> Vec<u8> {
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

/// The JSON of one answer line: at most `limit` bytes, ending in its only LF.
fn answer_json(bytes: &[u8], limit: usize) -> Result<&[u8], ProtocolFailure> {
    match bytes.split_last() {
        Some((b'\n', json)) if bytes.len() <= limit && !json.contains(&b'\n') => Ok(json),
        _ => Err(ProtocolFailure::Invalid),
    }
}

pub(crate) fn decode_response(bytes: &[u8]) -> Result<Response, ProtocolFailure> {
    let response: DecodedResponse = serde_json::from_slice(answer_json(bytes, RESPONSE_LIMIT)?)
        .map_err(|_| ProtocolFailure::Invalid)?;
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
        && status.assignment_counts.total().is_some()
        && valid_held_runs(&status.held_runs)
        && status
            .last_connected_at
            .as_deref()
            .is_none_or(valid_timestamp)
        && status.agent_logins.as_ref().is_none_or(valid_readiness)
        && status.log_path.as_deref().is_none_or(valid_log_path)
}

/// The longest log path a status may carry, in characters.
const MAXIMUM_LOG_PATH: usize = 4096;

/// A status's log path: an absolute path of at most `MAXIMUM_LOG_PATH` characters.
fn valid_log_path(path: &str) -> bool {
    path.starts_with('/') && path.chars().count() <= MAXIMUM_LOG_PATH
}

/// The most runs a status may list, the most assignments a runner holds.
const MAXIMUM_HELD_RUNS: usize = 128;

/// A status's held runs: at most `MAXIMUM_HELD_RUNS`, each a run's ID listed once.
fn valid_held_runs(held_runs: &[HeldRun]) -> bool {
    let mut run_ids = BTreeSet::new();
    held_runs.len() <= MAXIMUM_HELD_RUNS
        && held_runs.iter().all(|held| {
            super::validation::valid_typed_id(&held.run_id, "run_")
                && run_ids.insert(held.run_id.as_str())
        })
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
        && (entry.source.is_none() || entry.run_id.is_some())
}

fn valid_timestamp(value: &str) -> bool {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).is_ok()
}

/// A vocabulary word of the open `overview` answer (REQ-RUNNERENROLL-135): the word this
/// release knows, or the string as received. Either is written as the plain word.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenWord<T> {
    Known(T),
    Unknown(String),
}

impl<T> From<T> for OpenWord<T> {
    fn from(known: T) -> Self {
        Self::Known(known)
    }
}

impl<T: Serialize> Serialize for OpenWord<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Known(known) => known.serialize(serializer),
            Self::Unknown(word) => serializer.serialize_str(word),
        }
    }
}

impl<'de, T: serde::de::DeserializeOwned> Deserialize<'de> for OpenWord<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::IntoDeserializer;
        let word = String::deserialize(deserializer)?;
        let known: Result<T, serde::de::value::Error> =
            T::deserialize(word.as_str().into_deserializer());
        Ok(known.map_or(Self::Unknown(word), Self::Known))
    }
}

/// An overview entry's phase, as the assignment counts name it (REQ-RUNNERENROLL-80).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentPhase {
    Preparing,
    Accepted,
    Running,
    Waiting,
    Finishing,
    Reporting,
}

/// The health of the time Cloud granted a running assignment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseHealth {
    Ok,
    /// The renewal is due and not yet answered.
    Renewing,
    /// No renewal came and the runner is cancelling the run.
    Expiring,
    Revoked,
}

/// The state of a step running now (REQ-LOCALRUN-177).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveStepState {
    Starting,
    Running,
    CapturingOutputs,
    /// The step's recovery handler runs; only a single-workflow run reports it.
    Recovering,
    Cancelling,
}

/// What the serving process is doing, from what it holds in memory
/// (REQ-RUNNERENROLL-133). Open at every level: a member this release does not know is
/// dropped (REQ-RUNNERENROLL-135).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Overview {
    pub runner: OverviewRunner,
    pub connection: OverviewConnection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_credential_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_credential_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_logins: Option<OverviewAgentLogins>,
    pub assignment_counts: OverviewCounts,
    /// The runner's reports Cloud has not yet acknowledged.
    pub unacknowledged_reports: u64,
    pub assignments: Vec<OverviewAssignment>,
    /// The entries left out past the bound (REQ-RUNNERENROLL-136).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub assignments_omitted: u64,
    /// The assignments that ended in this process and that the runner holds no longer,
    /// newest first (REQ-RUNNERENROLL-137).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_assignments: Vec<OverviewRecentAssignment>,
    /// The recent assignments left out past the bound (REQ-RUNNERENROLL-136).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub recent_assignments_omitted: u64,
    /// The runner's own events, newest first (REQ-RUNNERENROLL-138).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<OverviewEvent>,
    /// The events left out past the bound (REQ-RUNNERENROLL-136).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub events_omitted: u64,
}

/// How an assignment's run ended, the word its `runner.run` record ends with as
/// `um.run.result` (REQ-RUNNERENROLL-137).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunResult {
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
    Aborted,
    Rejected,
    Fenced,
}

/// An assignment that ended in this process and that the runner holds no longer
/// (REQ-RUNNERENROLL-137).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewRecentAssignment {
    pub assignment_id: String,
    pub run_id: String,
    pub workflow_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub result: OpenWord<RunResult>,
    /// The root's chosen outcome, when a tree run exited with one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chosen_outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interruption_cause: Option<String>,
    /// When the run ended, or when the runner gave the offer up if it never ran.
    pub ended_at: String,
    /// How long the runner had held the assignment then.
    pub held_milliseconds: u64,
}

/// The kind of one of the runner's own events (REQ-RUNNERENROLL-138).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerEventKind {
    Connected,
    ConnectionFailed,
    CredentialReloaded,
    CredentialReloadFailed,
    AgentLoginsSetUp,
    LeaseLost,
    RunContinued,
    WorkspacesFound,
    ObservationRefused,
    TelemetryDropped,
    Stopping,
}

/// What asked for an agent-login setup (REQ-RUNNERENROLL-138).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLoginSetupCause {
    Schedule,
    Sync,
    Admission,
}

/// How an agent-login setup ended (REQ-RUNNERENROLL-138).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLoginSetupResult {
    Finished,
    RunnerBusy,
    Failed,
}

/// One of the runner's own events (REQ-RUNNERENROLL-138): one flat shape whose facts are
/// optional members, so a reader keeps a kind it does not know with the members it knows.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewEvent {
    pub at: String,
    pub kind: OpenWord<RunnerEventKind>,
    /// How many times in a row the event happened; absent reads as once.
    #[serde(default = "once")]
    pub count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<OpenWord<ConnectionFailure>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_in_milliseconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<OpenWord<ControlError>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_cause: Option<OpenWord<AgentLoginSetupCause>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_result: Option<OpenWord<AgentLoginSetupResult>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspaces: Option<u64>,
}

const fn once() -> u64 {
    1
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewRunner {
    pub runner_id: String,
    pub boot_id: String,
    pub version: String,
    pub uptime_milliseconds: u64,
    pub process_state: OpenWord<ProcessState>,
    /// The time left of the stop's bound, while the process is stopping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_time_left_milliseconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewConnection {
    pub state: OpenWord<ConnectionState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_connected_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<OpenWord<ConnectionFailure>>,
    /// The failed attempts since the last connection, while backing off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts_since_connected: Option<u64>,
    /// The time until the next attempt, while backing off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_in_milliseconds: Option<u64>,
}

/// Each provider's readiness with the members `status` gives (REQ-RUNNERENROLL-42).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewAgentLogins {
    pub updated_at: String,
    pub providers: Vec<OverviewProviderReadiness>,
}

impl From<AgentLoginReadiness> for OverviewAgentLogins {
    fn from(readiness: AgentLoginReadiness) -> Self {
        Self {
            updated_at: readiness.updated_at,
            providers: readiness
                .providers
                .into_iter()
                .map(OverviewProviderReadiness::Known)
                .collect(),
        }
    }
}

/// A provider's entry in an overview: the entry this release reads as its own, or else
/// the provider and state as received (REQ-RUNNERENROLL-135).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum OverviewProviderReadiness {
    Known(ProviderReadiness),
    Unknown { provider: String, state: String },
}

impl<'de> Deserialize<'de> for OverviewProviderReadiness {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        if let Ok(entry) = ProviderReadiness::deserialize(&value)
            && valid_provider_readiness(&entry)
        {
            return Ok(Self::Known(entry));
        }
        let word = |member: &str| value.get(member).and_then(serde_json::Value::as_str);
        match (word("provider"), word("state")) {
            (Some(provider), Some(state)) => Ok(Self::Unknown {
                provider: provider.to_owned(),
                state: state.to_owned(),
            }),
            _ => Err(serde::de::Error::custom(
                "an agent-login entry without its provider and state",
            )),
        }
    }
}

/// The six assignment counts (REQ-RUNNERENROLL-80); a count absent reads as zero.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewCounts {
    #[serde(default)]
    pub preparing: u64,
    #[serde(default)]
    pub accepted: u64,
    #[serde(default)]
    pub running: u64,
    #[serde(default)]
    pub waiting: u64,
    #[serde(default)]
    pub finishing: u64,
    #[serde(default)]
    pub reporting: u64,
}

impl OverviewCounts {
    pub fn total(self) -> Option<u64> {
        self.preparing
            .checked_add(self.accepted)?
            .checked_add(self.running)?
            .checked_add(self.waiting)?
            .checked_add(self.finishing)?
            .checked_add(self.reporting)
    }
}

/// One assignment the runner holds (REQ-RUNNERENROLL-134).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewAssignment {
    pub assignment_id: String,
    pub run_id: String,
    pub attempt_number: u64,
    pub workflow_path: String,
    pub phase: OpenWord<AssignmentPhase>,
    /// `working` or `waiting` as `status` lists the held run; absent once it has ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<OpenWord<HeldRunState>>,
    /// The time since the runner took the offer.
    pub held_milliseconds: u64,
    pub continues_earlier_attempt: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<OverviewLease>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<Vec<StepPathPart>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<OverviewWait>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub running_steps: Vec<OverviewRunningStep>,
    /// The running steps left out past the 16 listed.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub running_steps_omitted: u64,
}

/// The time Cloud granted a running assignment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewLease {
    pub health: OpenWord<LeaseHealth>,
    /// The time left before the runner would begin cancelling the run.
    pub time_left_milliseconds: u64,
}

/// One part of a position or a wait's step: a step ID or an element number.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum StepPathPart {
    Name(String),
    Number(u64),
}

/// The wait a tree run's resume loop last read; `kind` is the engine's word.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewWait {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub step: Vec<StepPathPart>,
}

/// A step running now, named by its run's path, its run's attempt and its ID.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewRunningStep {
    pub run: Vec<OverviewRunElement>,
    pub run_attempt: u64,
    pub step: String,
    pub state: OpenWord<LiveStepState>,
}

/// One level of a child run's path: the `workflow` step, its fan-out element and the
/// child's ordinal (REQ-LOCALRUN-177).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewRunElement {
    pub step: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item: Option<u64>,
    pub ordinal: u64,
}

/// What an `overview` request is answered: the overview, or the closed error the
/// serving process refused it with, `invalid_request` from an earlier release
/// (REQ-RUNNERENROLL-135).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OverviewAnswer {
    Overview(Box<Overview>),
    Refused(ControlError),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecodedOverview {
    schema_version: u8,
    outcome: String,
    #[serde(default)]
    overview: Option<Overview>,
    #[serde(default)]
    error: Option<ControlError>,
}

/// Writes an `overview` answer within `OVERVIEW_RESPONSE_LIMIT`.
pub(crate) fn encode_overview_response(overview: &Overview) -> Result<Vec<u8>, ProtocolFailure> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct EncodedOverview<'a> {
        schema_version: u8,
        outcome: &'static str,
        overview: &'a Overview,
    }

    open_answer_bytes(&EncodedOverview {
        schema_version: 1,
        outcome: "overview",
        overview,
    })
}

/// One open answer's line within `OVERVIEW_RESPONSE_LIMIT`, the LF included.
fn open_answer_bytes(answer: &impl Serialize) -> Result<Vec<u8>, ProtocolFailure> {
    let mut bytes = serde_json::to_vec(answer).map_err(|_| ProtocolFailure::Invalid)?;
    bytes.push(b'\n');
    if bytes.len() > OVERVIEW_RESPONSE_LIMIT {
        return Err(ProtocolFailure::Oversized);
    }
    Ok(bytes)
}

/// Reads the answer to an `overview` request by its `outcome`
/// (REQ-RUNNERENROLL-135, REQ-RUNNERENROLL-136).
pub(crate) fn decode_overview_response(bytes: &[u8]) -> Result<OverviewAnswer, ProtocolFailure> {
    let response: DecodedOverview =
        serde_json::from_slice(answer_json(bytes, OVERVIEW_RESPONSE_LIMIT)?)
            .map_err(|_| ProtocolFailure::Invalid)?;
    if response.schema_version != 1 {
        return Err(ProtocolFailure::Invalid);
    }
    match (response.outcome.as_str(), response.overview, response.error) {
        ("overview", Some(overview), _) if valid_overview(&overview) => {
            Ok(OverviewAnswer::Overview(Box::new(overview)))
        }
        ("error", _, Some(error)) => Ok(OverviewAnswer::Refused(error)),
        _ => Err(ProtocolFailure::Invalid),
    }
}

/// The most entries an overview lists (REQ-RUNNERENROLL-136): the 128 assignments a
/// runner holds and the ended ones still reporting beside them.
pub(crate) const MAXIMUM_OVERVIEW_ASSIGNMENTS: usize = 256;
/// The most running steps an overview entry lists, which the writer cuts to and the reader
/// refuses past.
pub(crate) const MAXIMUM_RUNNING_STEPS: usize = 16;
/// The most entries an overview's recent assignments and its events each list
/// (REQ-RUNNERENROLL-137, REQ-RUNNERENROLL-138): more than the writer keeps, so a later
/// change of what it keeps does not break this release's reader.
const MAXIMUM_OVERVIEW_HISTORY: usize = 256;

fn valid_overview(overview: &Overview) -> bool {
    let typed = |value: &str, prefix| super::validation::valid_typed_id(value, prefix);
    let typed_if =
        |value: &Option<String>, prefix| value.as_deref().is_none_or(|value| typed(value, prefix));
    let mut assignment_ids = BTreeSet::new();
    typed(&overview.runner.runner_id, "rnr_")
        && typed(&overview.runner.boot_id, "rbt_")
        && overview
            .connection
            .last_connected_at
            .as_deref()
            .is_none_or(valid_timestamp)
        && typed_if(&overview.current_credential_id, "rrc_")
        && typed_if(&overview.pending_credential_id, "rrc_")
        && overview.assignment_counts.total().is_some()
        && overview.assignments.len() <= MAXIMUM_OVERVIEW_ASSIGNMENTS
        && overview.assignments.iter().all(|entry| {
            valid_overview_entry(entry) && assignment_ids.insert(entry.assignment_id.as_str())
        })
        && overview.recent_assignments.len() <= MAXIMUM_OVERVIEW_HISTORY
        && overview.recent_assignments.iter().all(|entry| {
            typed(&entry.assignment_id, "asn_")
                && typed(&entry.run_id, "run_")
                && valid_timestamp(&entry.ended_at)
        })
        && overview.events.len() <= MAXIMUM_OVERVIEW_HISTORY
        && overview.events.iter().all(|event| {
            valid_timestamp(&event.at)
                && event.count >= 1
                && typed_if(&event.run_id, "run_")
                && typed_if(&event.assignment_id, "asn_")
                && typed_if(&event.credential_id, "rrc_")
        })
}

/// An entry an overview or an assignment answer may hold (REQ-RUNNERENROLL-134).
fn valid_overview_entry(entry: &OverviewAssignment) -> bool {
    super::validation::valid_typed_id(&entry.assignment_id, "asn_")
        && super::validation::valid_typed_id(&entry.run_id, "run_")
        && entry.running_steps.len() <= MAXIMUM_RUNNING_STEPS
}

/// The deepest child run a request or an answer names: a run path holds at most 16
/// levels, as the overview's running steps do.
pub(crate) const MAXIMUM_RUN_PATH: usize = 16;
/// The engine's bound on a fan-out element and a child ordinal.
const MAXIMUM_RUN_NUMBER: u64 = 2_147_483_647;
/// The most steps an assignment answer lists, the rest counted in `stepsOmitted`.
pub(crate) const WRITTEN_ASSIGNMENT_STEPS: usize = 1_024;
/// The most steps the reader admits: more than the writer lists, so a later change of
/// what it lists does not break this release's reader.
pub(crate) const MAXIMUM_ASSIGNMENT_STEPS: usize = 4_096;
/// The most records of a `step_records` log the reader admits: more than a step's ring
/// holds.
pub(crate) const MAXIMUM_LOG_RECORDS: usize = 16_384;
/// The most child runs an assignment answer lists of one step, the earlier ones counted
/// in `childRunsOmitted`.
pub(crate) const WRITTEN_CHILD_RUNS: usize = 64;
/// The most child runs of one step the reader admits.
pub(crate) const MAXIMUM_CHILD_RUNS: usize = 256;
/// The most bytes of a retained output or of the run log an assignment answer gives
/// (REQ-RUNNERENROLL-145).
pub(crate) const LOG_WINDOW_BYTES: u64 = 262_144;

/// What an `assignment` request asks for (REQ-RUNNERENROLL-144): one held assignment, the
/// child run `run` names, the root when it is empty, and the step whose output is wanted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentQuery {
    pub assignment_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub run: Vec<OverviewRunElement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

/// One assignment the runner holds, its run as the run directory says and the output of
/// a step or the end of the run log (REQ-RUNNERENROLL-144, REQ-RUNNERENROLL-145). Open at
/// every level: a member this release does not know is dropped (REQ-RUNNERENROLL-135).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentDetail {
    /// The entry `overview` lists for the assignment.
    pub entry: OverviewAssignment,
    /// The run read, present while the assignment runs or waits and its run directory
    /// read whole.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<AssignmentRun>,
    /// Absent while the assignment has no run log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log: Option<AssignmentLog>,
}

/// The run an assignment answer reads: the root, or the child run the request named.
/// Every vocabulary is the engine's word as it wrote it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentRun {
    /// The child run asked for; empty for the root.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path: Vec<OverviewRunElement>,
    pub workflow_path: String,
    /// The run record's state: `running`, `waiting` or `exited`.
    pub state: String,
    pub attempts: u64,
    /// The run record's outcome, once it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    /// The newest attempt's number and state.
    pub attempt_number: u64,
    pub attempt_state: String,
    /// The root wait record's kind, start and step while the run waits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<OverviewWait>,
    /// The newest attempt's steps then finalizers, in the definition's presentation order.
    pub steps: Vec<AssignmentStep>,
    /// The steps left out past the bound.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub steps_omitted: u64,
}

/// One step or finalizer of the newest attempt. Exactly one of `command`, `agent` and
/// `workflow` says what it runs, as the workflow declares it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentStep {
    pub id: String,
    /// `step` or `finalizer`.
    pub role: String,
    /// The workflow format's word: `cmd`, `agent` or `workflow`.
    pub kind: String,
    /// The step's direct dependencies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<String>,
    pub state: String,
    pub failure_policy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// The time since the start of a step in flight, or to the last finish among a settled
    /// step's invocations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_milliseconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<AssignmentCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<AssignmentAgent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<AssignmentWorkflow>,
    /// Each declared output and whether the attempt committed it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<AssignmentOutput>,
    /// A `workflow` step's child runs in the engine's order, the last of them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub child_runs: Vec<AssignmentChildRun>,
    /// The earlier child runs left out.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub child_runs_omitted: u64,
}

/// What a `cmd` step runs; `cwd` is the declared one, never resolved against the host.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentCommand {
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

/// What an `agent` step runs: its profile and that profile's harness, model and effort.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentAgent {
    pub profile: String,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
}

/// What a `workflow` step runs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentWorkflow {
    pub file: String,
}

/// A declared output of a step and whether the attempt committed it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentOutput {
    pub name: String,
    pub kind: String,
    pub committed: bool,
}

/// A child run of a `workflow` step: its ordinal, a fan-out's element, the child record's
/// state and its outcome once exited.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentChildRun {
    pub ordinal: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item: Option<u64>,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
}

/// Where an assignment answer's log comes from (REQ-RUNNERENROLL-145).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentLogSource {
    /// The retained output of a settled step's newest invocation that retained any.
    Step,
    /// The end of the assignment's `tree-run.log`.
    RunLog,
    /// A single-workflow assignment's named step's newest records.
    StepRecords,
}

/// The output of a named settled step, the end of the run log, or a single-workflow
/// assignment's named step's newest records (REQ-RUNNERENROLL-145). A `step` log carries
/// `step`, and the invocation's ID and streams when it retained output; a `run_log` log
/// carries `text` and `omittedBytes`; a `step_records` log carries `step`, `records` and
/// the older records' count and bytes when it names a step of the run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentLog {
    pub source: OpenWord<AssignmentLogSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<AssignmentLogStream>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<AssignmentLogStream>,
    /// The step's earlier invocations that retained output.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub earlier_invocations_omitted: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// The run log's bytes before `text`, or the older records' bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub omitted_bytes: Option<u64>,
    /// The step's newest whole records in the order observed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<AssignmentLogRecord>,
    /// The step's older records left out.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub omitted_records: u64,
}

/// One record of a step's output: `stdout` or `stderr` for a command, the engine's word
/// for an agent's, the record made visible, and whether it is a piece the output was
/// split into. Open: a member this release does not know is dropped.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentLogRecord {
    pub source: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub continuation: bool,
}

/// One retained stream of a step's invocation: the record's retained and discarded counts,
/// the end of the retained file as text and the bytes of the file before it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentLogStream {
    pub retained_bytes: u64,
    pub discarded_bytes: u64,
    pub omitted_bytes: u64,
    pub text: String,
}

/// What an `assignment` request is answered: the assignment, `assignment_not_held` for an
/// assignment the runner neither holds nor still reports, or the closed error the serving
/// process refused it with, `invalid_request` from an earlier release
/// (REQ-RUNNERENROLL-135, REQ-RUNNERENROLL-144).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AssignmentAnswer {
    Assignment(Box<AssignmentDetail>),
    NotHeld,
    Refused(ControlError),
}

/// The error word of the answer to an assignment the runner does not hold.
const ASSIGNMENT_NOT_HELD: &str = "assignment_not_held";

/// Writes the `assignment` request for `query`, refused past `REQUEST_LIMIT`.
pub(crate) fn encode_assignment_request(
    query: &AssignmentQuery,
) -> Result<Vec<u8>, ProtocolFailure> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct EncodedAssignmentRequest<'a> {
        schema_version: u8,
        operation: &'static str,
        #[serde(flatten)]
        query: &'a AssignmentQuery,
    }

    let mut bytes = serde_json::to_vec(&EncodedAssignmentRequest {
        schema_version: 1,
        operation: "assignment",
        query,
    })
    .map_err(|_| ProtocolFailure::Invalid)?;
    bytes.push(b'\n');
    if bytes.len() > REQUEST_LIMIT {
        return Err(ProtocolFailure::Oversized);
    }
    Ok(bytes)
}

/// Writes an `assignment` answer within `OVERVIEW_RESPONSE_LIMIT`.
pub(crate) fn encode_assignment_response(
    detail: &AssignmentDetail,
) -> Result<Vec<u8>, ProtocolFailure> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct EncodedAssignment<'a> {
        schema_version: u8,
        outcome: &'static str,
        assignment: &'a AssignmentDetail,
    }

    open_answer_bytes(&EncodedAssignment {
        schema_version: 1,
        outcome: "assignment",
        assignment: detail,
    })
}

/// The answer to an `assignment` request for an assignment the runner neither holds nor
/// still reports.
pub(crate) fn encode_assignment_not_held() -> Vec<u8> {
    let mut bytes =
        format!(r#"{{"schemaVersion":1,"outcome":"error","error":"{ASSIGNMENT_NOT_HELD}"}}"#)
            .into_bytes();
    bytes.push(b'\n');
    bytes
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecodedAssignment {
    schema_version: u8,
    outcome: String,
    #[serde(default)]
    assignment: Option<AssignmentDetail>,
    #[serde(default)]
    error: Option<String>,
}

/// Reads the answer to an `assignment` request by its `outcome`
/// (REQ-RUNNERENROLL-135, REQ-RUNNERENROLL-136, REQ-RUNNERENROLL-144).
pub(crate) fn decode_assignment_response(
    bytes: &[u8],
) -> Result<AssignmentAnswer, ProtocolFailure> {
    let response: DecodedAssignment =
        serde_json::from_slice(answer_json(bytes, OVERVIEW_RESPONSE_LIMIT)?)
            .map_err(|_| ProtocolFailure::Invalid)?;
    if response.schema_version != 1 {
        return Err(ProtocolFailure::Invalid);
    }
    match (
        response.outcome.as_str(),
        response.assignment,
        response.error,
    ) {
        ("assignment", Some(detail), _) if valid_assignment(&detail) => {
            Ok(AssignmentAnswer::Assignment(Box::new(detail)))
        }
        ("error", _, Some(word)) if word == ASSIGNMENT_NOT_HELD => Ok(AssignmentAnswer::NotHeld),
        ("error", _, Some(word)) => {
            use serde::de::IntoDeserializer;
            let error: Result<ControlError, serde::de::value::Error> =
                ControlError::deserialize(word.as_str().into_deserializer());
            error
                .map(AssignmentAnswer::Refused)
                .map_err(|_| ProtocolFailure::Invalid)
        }
        _ => Err(ProtocolFailure::Invalid),
    }
}

fn valid_assignment(detail: &AssignmentDetail) -> bool {
    valid_overview_entry(&detail.entry)
        && detail.run.as_ref().is_none_or(|run| {
            run.path.len() <= MAXIMUM_RUN_PATH
                && run.steps.len() <= MAXIMUM_ASSIGNMENT_STEPS
                && run
                    .steps
                    .iter()
                    .all(|step| step.child_runs.len() <= MAXIMUM_CHILD_RUNS)
        })
        && detail
            .log
            .as_ref()
            .is_none_or(|log| log.records.len() <= MAXIMUM_LOG_RECORDS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_is_closed_and_lf_delimited() {
        assert_eq!(
            decode_request(b"{\"schemaVersion\":1,\"operation\":\"status\"}\n"),
            Ok(ControlRequest::Operation(Operation::Status))
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
        assert_eq!(
            decode_request(&request),
            Ok(ControlRequest::Operation(Operation::Status))
        );
        request.insert(request.len() - 1, b' ');
        assert_eq!(decode_request(&request), Err(ControlError::InvalidRequest));

        let response = Response::Status(StatusSnapshot {
            process_state: ProcessState::Running,
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            uptime_milliseconds: 0,
            log_path: None,
            connection_state: ConnectionState::Connecting,
            last_connected_at: None,
            current_credential_id: None,
            pending_credential_id: None,
            assignment_counts: AssignmentCounts::default(),
            last_connection_failure: None,
            agent_logins: None,
            held_runs: Vec::new(),
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

    // REQ-RUNNERENROLL-42, REQ-RUNNERENROLL-80: a snapshot of several held runs, one
    // waiting, decodes with each nonzero count and each run's state; a run in a state
    // other than working or waiting is refused.
    #[test]
    fn status_with_held_runs_decodes_and_an_unknown_held_run_state_is_refused() {
        let cli_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let fixtures = cli_root.join("tests/fixtures/runner-control/v1");
        let validator = control_validator();
        let read = |path: &str| std::fs::read(fixtures.join(path)).unwrap();

        let valid = read("valid/status-held-runs-response.json");
        assert!(validator.is_valid(&serde_json::from_slice(&valid).unwrap()));
        let Ok(Response::Status(status)) = decode_response(&valid) else {
            panic!("the held-runs snapshot did not decode");
        };
        assert_eq!(
            status.assignment_counts,
            AssignmentCounts {
                preparing: 1,
                waiting: 1,
                ..AssignmentCounts::default()
            }
        );
        assert_eq!(
            status.held_runs,
            [
                HeldRun {
                    run_id: "run_01k0z6r1w8f4jy2m7q9v3x5abp".to_owned(),
                    state: HeldRunState::Working,
                },
                HeldRun {
                    run_id: "run_01k0z6r1w8f4jy2m7q9v3x5abq".to_owned(),
                    state: HeldRunState::Waiting,
                },
            ]
        );
        assert_eq!(
            encode_response(&Response::Status(status.clone())).unwrap(),
            valid
        );

        let invalid = read("invalid/status-held-run-unknown-state.json");
        assert!(!validator.is_valid(&serde_json::from_slice(&invalid).unwrap()));
        assert_eq!(decode_response(&invalid), Err(ProtocolFailure::Invalid));

        // The same run listed twice, or a listed ID that is not a run's, is refused.
        let mut twice = status.clone();
        twice.held_runs[1].run_id = twice.held_runs[0].run_id.clone();
        let mut foreign = status;
        foreign.held_runs[0].run_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abp".to_owned();
        for refused in [twice, foreign] {
            let encoded = encode_response(&Response::Status(refused)).unwrap();
            assert_eq!(decode_response(&encoded), Err(ProtocolFailure::Invalid));
        }
    }

    // REQ-RUNNERENROLL-42: the path of the log the process writes travels as an absolute
    // path; a status whose log path is relative or empty is refused.
    #[test]
    fn status_with_a_log_path_decodes_and_a_relative_or_empty_one_is_refused() {
        let cli_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let fixtures = cli_root.join("tests/fixtures/runner-control/v1");
        let read = |path: &str| std::fs::read(fixtures.join(path)).unwrap();

        let valid = read("valid/status-log-path-response.json");
        let Ok(Response::Status(status)) = decode_response(&valid) else {
            panic!("the log-path snapshot did not decode");
        };
        assert_eq!(
            status.log_path.as_deref(),
            Some("/var/lib/runner/logs/runner.log")
        );
        assert_eq!(
            encode_response(&Response::Status(status.clone())).unwrap(),
            valid
        );

        let relative = read("invalid/status-log-path-relative.json");
        assert_eq!(decode_response(&relative), Err(ProtocolFailure::Invalid));
        let empty = StatusSnapshot {
            log_path: Some(String::new()),
            ..status
        };
        let encoded = encode_response(&Response::Status(empty)).unwrap();
        assert_eq!(decode_response(&encoded), Err(ProtocolFailure::Invalid));
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
            earlier_selection: EarlierSelection,
            last_use: None,
            last_use_at: None,
            run_id: None,
            ended_at: None,
            source: None,
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
                    earlier_selection: EarlierSelection,
                    last_use: None,
                    last_use_at: None,
                    run_id: None,
                    ended_at: None,
                    source: None,
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
        assert_eq!(
            decode_request(&request),
            Ok(ControlRequest::Operation(Operation::SyncAgentLogins))
        );
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
    fn grant_entry(state: AgentLoginState) -> ProviderReadiness {
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
            earlier_selection: EarlierSelection,
            last_use: None,
            last_use_at: None,
            run_id: None,
            ended_at: None,
            source: None,
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
            readiness.providers[1] = grant_entry(state);
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
            ..grant_entry(AgentLoginState::Ready)
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
            // A source is an assignment's, so one with no run is refused
            // (REQ-GRANT-92).
            (
                1,
                ProviderReadiness {
                    run_id: None,
                    ended_at: None,
                    source: Some(CredentialSource::Host),
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
            ..grant_entry(AgentLoginState::Ready)
        };
        let mut brokered_malformed_grant = readiness();
        brokered_malformed_grant.providers[1] = ProviderReadiness {
            grant_id: Some("agl_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
            ..grant_entry(AgentLoginState::Ready)
        };
        let mut brokered_agent_login_state = readiness();
        brokered_agent_login_state.providers[1] = grant_entry(AgentLoginState::ForeignLogin);
        let mut brokered_install = readiness();
        brokered_install.providers[1] = ProviderReadiness {
            installed_at: Some("2026-11-12T00:55:00Z".to_owned()),
            ..grant_entry(AgentLoginState::Ready)
        };
        let mut brokered_last_use_at = readiness();
        brokered_last_use_at.providers[1] = ProviderReadiness {
            last_use_at: Some("2026-11-12T00:58:00Z".to_owned()),
            ..grant_entry(AgentLoginState::Ready)
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
        use useful_support::{Availability as A, NextAction as N};
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
            let entry = grant_entry(state);
            assert_eq!(
                entry.credential_status(),
                useful_support::CredentialStatus {
                    availability,
                    next_action,
                },
                "{state:?}"
            );
            // One assignment's outcome, never the runner's
            // (REQ-GRANT-88).
            assert_eq!(entry.failure_code(), None, "{state:?}");
            assert!(!entry.fails_readiness(), "{state:?}");
        }
        // `ready` is `available` whatever the last use, which no grant
        // token records.
        let mut refused = grant_entry(S::Ready);
        refused.last_use = Some(AgentLoginUse::Refused);
        assert_eq!(
            refused.credential_status().availability,
            useful_support::Availability::Available
        );
    }

    // REQ-GRANT-53, REQ-GRANT-88
    #[test]
    fn an_ended_oauth_entry_that_was_ready_is_unverified_and_no_other_state_changes() {
        use useful_support::{Availability as A, CredentialStatus, NextAction as N};
        for state in GRANT_STATES {
            let current = grant_entry(state);
            let mut ended = current.clone();
            ended.run_id = Some("run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned());
            ended.ended_at = Some("2026-11-12T02:00:00Z".to_owned());
            if state == AgentLoginState::Ready {
                assert_eq!(
                    current.credential_status(),
                    CredentialStatus {
                        availability: A::Available,
                        next_action: N::None,
                    }
                );
                // An ended assignment's token says nothing of the next one.
                assert_eq!(
                    ended.credential_status(),
                    CredentialStatus {
                        availability: A::Unverified,
                        next_action: N::None,
                    }
                );
            } else {
                assert_eq!(
                    ended.credential_status(),
                    current.credential_status(),
                    "{state:?}"
                );
            }
            assert_eq!(current.failure_code(), None, "{state:?}");
            assert_eq!(ended.failure_code(), None, "{state:?}");
        }
    }

    // REQ-GRANT-41, REQ-GRANT-38
    #[test]
    fn only_an_authorized_provider_that_is_not_ready_fails_readiness() {
        // `foreign_login` is authorized, or may be, and not ready, but the
        // host's own login is in use: it fails nothing (REQ-GRANT-51).
        for (state, authorized_not_ready, fails) in [
            (AgentLoginState::Ready, false, false),
            (AgentLoginState::NotAuthorized, false, false),
            (AgentLoginState::Revoked, false, false),
            (AgentLoginState::NotDelivered, false, false),
            (AgentLoginState::ForeignLogin, true, false),
            (AgentLoginState::UnsupportedTarget, true, true),
            (AgentLoginState::StaleGeneration, true, true),
            (AgentLoginState::CloudUnavailable, true, true),
            (AgentLoginState::InstallFailed, true, true),
            (AgentLoginState::ReconnectRequired, true, true),
        ] {
            assert_eq!(
                state.authorized_not_ready(),
                authorized_not_ready,
                "{state:?}"
            );
            assert_eq!(state.fails_readiness(), fails, "{state:?}");
        }
    }

    // REQ-GRANT-53
    #[test]
    fn each_readiness_maps_to_one_availability_and_next_action() {
        use AgentLoginState as S;
        use useful_support::{Availability as A, NextAction as N};
        let accepted = Some(AgentLoginUse::Accepted);
        let refused = Some(AgentLoginUse::Refused);
        for (state, last_use, availability, next_action) in [
            (S::Ready, accepted, A::Available, N::None),
            (S::Ready, None, A::Unverified, N::None),
            (S::Ready, refused, A::ReconnectRequired, N::Reconnect),
            (S::InstallFailed, None, A::DeliveryFailed, N::SyncRunner),
            (S::StaleGeneration, None, A::DeliveryFailed, N::SyncRunner),
            (S::CloudUnavailable, None, A::DeliveryFailed, N::SyncRunner),
            (
                S::UnsupportedTarget,
                None,
                A::DeliveryFailed,
                N::CheckTarget,
            ),
            (S::NotAuthorized, None, A::NotConnected, N::None),
            (S::Revoked, None, A::NotConnected, N::None),
            (
                S::ForeignLogin,
                None,
                A::NotDelivered,
                N::ResolveServiceLogin,
            ),
            (S::NotDelivered, None, A::NotDelivered, N::None),
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
                earlier_selection: EarlierSelection,
                last_use,
                last_use_at: None,
                run_id: None,
                ended_at: None,
                source: None,
            };
            assert_eq!(
                entry.credential_status(),
                useful_support::CredentialStatus {
                    availability,
                    next_action,
                },
                "{state:?} {last_use:?}"
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

    // REQ-GRANT-41, REQ-GRANT-53, REQ-GRANT-38, REQ-RUNNERENROLL-86
    #[test]
    fn a_provider_cloud_refuses_or_the_host_logs_in_for_never_fails_readiness() {
        use useful_support::{Availability as A, CredentialStatus, NextAction as N};
        // An entry as a serving process answers it: one of an earlier
        // release may still say whether the provider was selected, which
        // changes nothing.
        let entry = |state: &str, selected: Option<bool>| {
            let mut entry = serde_json::json!({
                "provider": "claude_code",
                "state": state,
                "agentLoginId": "agl_01k0z6r1w8f4jy2m7q9v3x5abc",
                "checkedAt": "2026-11-12T01:00:00Z",
            });
            if let Some(selected) = selected {
                entry["selected"] = selected.into();
            }
            serde_json::from_value::<ProviderReadiness>(entry).unwrap()
        };
        for selected in [Some(true), Some(false), None] {
            // Cloud refuses the provider: no login is authorized for it.
            let refused = entry("not_authorized", selected);
            assert_eq!(
                refused.credential_status(),
                CredentialStatus {
                    availability: A::NotConnected,
                    next_action: N::None,
                },
                "{selected:?}"
            );
            assert_eq!(refused.failure_code(), None, "{selected:?}");
            assert!(!refused.fails_readiness(), "{selected:?}");

            // The host's own login is in use (REQ-GRANT-51).
            let host = entry("foreign_login", selected);
            assert_eq!(
                host.credential_status(),
                CredentialStatus {
                    availability: A::NotDelivered,
                    next_action: N::ResolveServiceLogin,
                },
                "{selected:?}"
            );
            assert_eq!(host.failure_code(), None, "{selected:?}");
            assert!(!host.fails_readiness(), "{selected:?}");
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

        // The last use, with its time, travels beside the state where the
        // document records it (REQ-GRANT-41, REQ-GRANT-53).
        let mut with_last_use = readiness();
        with_last_use.providers[0].last_use = Some(AgentLoginUse::Refused);
        with_last_use.providers[0].last_use_at = Some("2026-11-12T00:58:00Z".to_owned());
        let response = synced(with_last_use);
        let encoded = encode_response(&response).unwrap();
        assert_eq!(decode_response(&encoded), Ok(response.clone()));
        let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert!(control_validator().is_valid(&value), "{value}");
        assert_eq!(value["agentLogins"]["providers"][0]["lastUse"], "refused");
        assert_eq!(
            value["agentLogins"]["providers"][0]["lastUseAt"],
            "2026-11-12T00:58:00Z"
        );

        // A serving process of an earlier release still says whether the
        // provider was selected: its answer decodes, and the member is
        // written on none (REQ-GRANT-38).
        assert!(
            value["agentLogins"]["providers"][0]
                .get("selected")
                .is_none()
        );
        value["agentLogins"]["providers"][0]["selected"] = true.into();
        assert!(control_validator().is_valid(&value), "{value}");
        let mut earlier = serde_json::to_vec(&value).unwrap();
        earlier.push(b'\n');
        assert_eq!(decode_response(&earlier), Ok(response.clone()));
        assert_eq!(encode_response(&response).unwrap(), encoded);
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
        // `assignment_not_held` answers only an `assignment` request, whose reader alone
        // knows it (REQ-RUNNERENROLL-144).
        assert_eq!(
            schema_codes,
            closed
                .iter()
                .map(|(_, code)| *code)
                .chain(["assignment_not_held"])
                .collect::<Vec<_>>()
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
            log_path: None,
            connection_state: ConnectionState::Connecting,
            last_connected_at: None,
            current_credential_id: Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
            pending_credential_id: None,
            assignment_counts: AssignmentCounts::default(),
            last_connection_failure: None,
            agent_logins: None,
            held_runs: Vec::new(),
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

    fn overview_fixture(path: &str) -> Vec<u8> {
        let cli_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        std::fs::read(cli_root.join("tests/fixtures/runner-control/v1").join(path)).unwrap()
    }

    /// The overview `valid/overview-response.json` states.
    fn stated_overview() -> Overview {
        let workflow_path = ".useful/engineering/workflow.yaml".to_owned();
        Overview {
            runner: OverviewRunner {
                runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                version: "0.83.0".to_owned(),
                uptime_milliseconds: 11_520_000,
                process_state: OpenWord::Known(ProcessState::Stopping),
                stop_time_left_milliseconds: Some(298_000),
            },
            connection: OverviewConnection {
                state: OpenWord::Known(ConnectionState::BackingOff),
                last_connected_at: Some("2026-11-19T14:02:00Z".to_owned()),
                last_failure: Some(OpenWord::Known(ConnectionFailure::Network)),
                attempts_since_connected: Some(3),
                next_attempt_in_milliseconds: Some(4_200),
            },
            current_credential_id: Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
            pending_credential_id: Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned()),
            agent_logins: Some(OverviewAgentLogins {
                updated_at: "2026-11-19T14:00:00Z".to_owned(),
                providers: vec![
                    OverviewProviderReadiness::Known(ProviderReadiness {
                        provider: AgentLoginProvider::Codex,
                        state: AgentLoginState::Ready,
                        agent_login_id: Some("agl_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned()),
                        generation: Some(3),
                        checked_at: Some("2026-11-19T14:00:00Z".to_owned()),
                        installed_at: Some("2026-11-19T13:55:00Z".to_owned()),
                        grant_id: None,
                        expires_at: None,
                        earlier_selection: EarlierSelection,
                        last_use: None,
                        last_use_at: None,
                        run_id: None,
                        ended_at: None,
                        source: None,
                    }),
                    OverviewProviderReadiness::Known(ProviderReadiness {
                        checked_at: None,
                        ..grant_entry(AgentLoginState::NotDelivered)
                    }),
                ],
            }),
            assignment_counts: OverviewCounts {
                running: 1,
                waiting: 1,
                ..OverviewCounts::default()
            },
            unacknowledged_reports: 2,
            assignments: vec![
                OverviewAssignment {
                    assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    run_id: "run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    attempt_number: 1,
                    workflow_path: workflow_path.clone(),
                    phase: OpenWord::Known(AssignmentPhase::Running),
                    activity: Some(OpenWord::Known(HeldRunState::Working)),
                    held_milliseconds: 1_080_000,
                    continues_earlier_attempt: false,
                    lease: Some(OverviewLease {
                        health: OpenWord::Known(LeaseHealth::Ok),
                        time_left_milliseconds: 42_000,
                    }),
                    subject: None,
                    run_url: Some(
                        "https://cloud.example.test/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    ),
                    position: Some(vec![StepPathPart::Name("build".to_owned())]),
                    wait: None,
                    running_steps: vec![OverviewRunningStep {
                        run: vec![OverviewRunElement {
                            step: "build".to_owned(),
                            item: Some(0),
                            ordinal: 1,
                        }],
                        run_attempt: 1,
                        step: "implement".to_owned(),
                        state: OpenWord::Known(LiveStepState::Running),
                    }],
                    running_steps_omitted: 2,
                },
                OverviewAssignment {
                    assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
                    run_id: "run_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
                    attempt_number: 2,
                    workflow_path,
                    phase: OpenWord::Known(AssignmentPhase::Waiting),
                    activity: Some(OpenWord::Known(HeldRunState::Waiting)),
                    held_milliseconds: 3_600_000,
                    continues_earlier_attempt: true,
                    lease: Some(OverviewLease {
                        health: OpenWord::Known(LeaseHealth::Renewing),
                        time_left_milliseconds: 9_000,
                    }),
                    subject: Some("AUTO-751".to_owned()),
                    run_url: None,
                    position: Some(vec![
                        StepPathPart::Name("review".to_owned()),
                        StepPathPart::Number(0),
                    ]),
                    wait: Some(OverviewWait {
                        kind: "external".to_owned(),
                        since: Some("2026-11-19T14:10:00Z".to_owned()),
                        step: vec![
                            StepPathPart::Name("review".to_owned()),
                            StepPathPart::Number(0),
                            StepPathPart::Name("queue".to_owned()),
                        ],
                    }),
                    running_steps: Vec::new(),
                    running_steps_omitted: 0,
                },
            ],
            assignments_omitted: 1,
            recent_assignments: stated_recent_assignments(),
            recent_assignments_omitted: 1,
            events: stated_events(),
            events_omitted: 2,
        }
    }

    /// The recent assignments `valid/overview-response.json` states, newest first.
    fn stated_recent_assignments() -> Vec<OverviewRecentAssignment> {
        let recent =
            |suffix: &str, result, ended_at: &str, held_milliseconds| OverviewRecentAssignment {
                assignment_id: format!("asn_01k0z6r1w8f4jy2m7q9v3x5{suffix}"),
                run_id: format!("run_01k0z6r1w8f4jy2m7q9v3x5{suffix}"),
                workflow_path: ".useful/engineering/workflow.yaml".to_owned(),
                subject: None,
                result: OpenWord::Known(result),
                chosen_outcome: None,
                failure_code: None,
                interruption_cause: None,
                ended_at: ended_at.to_owned(),
                held_milliseconds,
            };
        vec![
            OverviewRecentAssignment {
                subject: Some("AUTO-740".to_owned()),
                chosen_outcome: Some("merged".to_owned()),
                ..recent("abe", RunResult::Succeeded, "2026-11-19T13:50:00Z", 720_000)
            },
            OverviewRecentAssignment {
                failure_code: Some("step_failed".to_owned()),
                ..recent("abf", RunResult::Failed, "2026-11-19T13:40:00Z", 41_000)
            },
            OverviewRecentAssignment {
                interruption_cause: Some("execution_lease_expired".to_owned()),
                ..recent(
                    "abg",
                    RunResult::Interrupted,
                    "2026-11-19T13:30:00Z",
                    96_000,
                )
            },
        ]
    }

    /// The events `valid/overview-response.json` states, one of each kind, newest first.
    fn stated_events() -> Vec<OverviewEvent> {
        let event = |minute: u8, kind, count| OverviewEvent {
            at: format!("2026-11-19T14:{minute:02}:00Z"),
            kind: OpenWord::Known(kind),
            count,
            failure: None,
            next_attempt_in_milliseconds: None,
            credential_id: None,
            error: None,
            setup_cause: None,
            setup_result: None,
            run_id: None,
            assignment_id: None,
            workspaces: None,
        };
        vec![
            event(12, RunnerEventKind::Stopping, 1),
            event(11, RunnerEventKind::TelemetryDropped, 512),
            event(10, RunnerEventKind::ObservationRefused, 40),
            OverviewEvent {
                workspaces: Some(2),
                ..event(9, RunnerEventKind::WorkspacesFound, 1)
            },
            OverviewEvent {
                run_id: Some("run_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned()),
                assignment_id: Some("asn_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned()),
                ..event(8, RunnerEventKind::RunContinued, 1)
            },
            OverviewEvent {
                run_id: Some("run_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned()),
                assignment_id: Some("asn_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned()),
                ..event(7, RunnerEventKind::LeaseLost, 1)
            },
            OverviewEvent {
                setup_cause: Some(OpenWord::Known(AgentLoginSetupCause::Schedule)),
                setup_result: Some(OpenWord::Known(AgentLoginSetupResult::Finished)),
                ..event(6, RunnerEventKind::AgentLoginsSetUp, 12)
            },
            OverviewEvent {
                error: Some(OpenWord::Known(ControlError::NoPendingCredential)),
                ..event(5, RunnerEventKind::CredentialReloadFailed, 1)
            },
            OverviewEvent {
                credential_id: Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned()),
                ..event(4, RunnerEventKind::CredentialReloaded, 1)
            },
            OverviewEvent {
                failure: Some(OpenWord::Known(ConnectionFailure::Network)),
                next_attempt_in_milliseconds: Some(4_200),
                ..event(3, RunnerEventKind::ConnectionFailed, 3)
            },
            event(2, RunnerEventKind::Connected, 1),
        ]
    }

    fn decoded_overview(bytes: &[u8]) -> Overview {
        match decode_overview_response(bytes) {
            Ok(OverviewAnswer::Overview(overview)) => *overview,
            other => panic!("the overview did not decode: {other:?}"),
        }
    }

    fn encoded_value(value: &serde_json::Value) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(value).unwrap();
        bytes.push(b'\n');
        bytes
    }

    // REQ-RUNNERENROLL-135, REQ-RUNNERENROLL-137, REQ-RUNNERENROLL-138: the overview
    // answer, with its recent assignments and its events, decodes to what it states and
    // encodes back to its bytes; the idle answer carries only the required members, and
    // neither list nor its count.
    #[test]
    fn an_overview_decodes_to_what_it_states_and_encodes_to_its_bytes() {
        let bytes = overview_fixture("valid/overview-response.json");
        let overview = decoded_overview(&bytes);
        assert_eq!(overview, stated_overview());
        assert_eq!(encode_overview_response(&overview).unwrap(), bytes);

        let idle = overview_fixture("valid/overview-idle-response.json");
        let overview = decoded_overview(&idle);
        assert_eq!(overview.assignments, []);
        assert_eq!(overview.assignment_counts, OverviewCounts::default());
        assert_eq!(overview.agent_logins, None);
        assert_eq!(overview.recent_assignments, []);
        assert_eq!(overview.recent_assignments_omitted, 0);
        assert_eq!(overview.events, []);
        assert_eq!(overview.events_omitted, 0);
        assert_eq!(encode_overview_response(&overview).unwrap(), idle);

        assert_eq!(
            encode_overview_request(),
            overview_fixture("valid/overview-request.json")
        );
        assert_eq!(
            decode_request(&overview_fixture("valid/overview-request.json")),
            Ok(ControlRequest::Overview)
        );
        assert_eq!(
            decode_request(&overview_fixture(
                "invalid/overview-request-extra-field.json"
            )),
            Err(ControlError::InvalidRequest)
        );
    }

    // REQ-RUNNERENROLL-135, REQ-RUNNERENROLL-137, REQ-RUNNERENROLL-138: a member this
    // release does not know is dropped at every level, a vocabulary word it does not know
    // is kept as received, an agent-login entry it cannot read as its own is kept as its
    // provider and state, and an event without its count happened once.
    #[test]
    fn an_overview_keeps_unknown_words_and_drops_unknown_members() {
        let mut value: serde_json::Value =
            serde_json::from_slice(&overview_fixture("valid/overview-response.json")).unwrap();
        let unknown = serde_json::json!({"nested": [1, "two"]});
        for member in [
            "",
            "/overview",
            "/overview/runner",
            "/overview/connection",
            "/overview/agentLogins",
            "/overview/assignmentCounts",
            "/overview/assignments/0",
            "/overview/assignments/0/lease",
            "/overview/assignments/0/runningSteps/0",
            "/overview/assignments/0/runningSteps/0/run/0",
            "/overview/assignments/1/wait",
            "/overview/recentAssignments/0",
            "/overview/events/0",
        ] {
            value.pointer_mut(member).unwrap()["laterMember"] = unknown.clone();
        }
        let overview = &mut value["overview"];
        overview["recentAssignments"][0]["result"] = "superseded".into();
        overview["events"][0]["kind"] = "rebooted".into();
        let set_up = overview["events"][6].as_object_mut().unwrap();
        assert_eq!(set_up.remove("count"), Some(12.into()));
        overview["runner"]["processState"] = "draining".into();
        overview["connection"]["state"] = "probing".into();
        overview["connection"]["lastFailure"] = "tls".into();
        overview["assignments"][0]["phase"] = "paused".into();
        overview["assignments"][0]["activity"] = "thinking".into();
        overview["assignments"][0]["lease"]["health"] = "suspended".into();
        overview["assignments"][0]["runningSteps"][0]["state"] = "retrying".into();
        let providers = overview["agentLogins"]["providers"].as_array_mut().unwrap();
        providers.push(serde_json::json!({"provider": "gemini", "state": "ready"}));
        providers.push(serde_json::json!({"provider": "codex", "state": "warming"}));
        providers.push(
            serde_json::json!({"provider": "github", "state": "not_delivered", "laterMember": 1}),
        );

        let mut expected = stated_overview();
        fn word<T>(word: &str) -> OpenWord<T> {
            OpenWord::Unknown(word.to_owned())
        }
        expected.runner.process_state = word("draining");
        expected.connection.state = word("probing");
        expected.connection.last_failure = Some(word("tls"));
        let entry = &mut expected.assignments[0];
        entry.phase = word("paused");
        entry.activity = Some(word("thinking"));
        if let Some(lease) = entry.lease.as_mut() {
            lease.health = word("suspended");
        }
        entry.running_steps[0].state = word("retrying");
        expected.recent_assignments[0].result = word("superseded");
        expected.events[0].kind = word("rebooted");
        expected.events[6].count = 1;
        if let Some(logins) = expected.agent_logins.as_mut() {
            for (provider, state) in [
                ("gemini", "ready"),
                ("codex", "warming"),
                ("github", "not_delivered"),
            ] {
                logins.providers.push(OverviewProviderReadiness::Unknown {
                    provider: provider.to_owned(),
                    state: state.to_owned(),
                });
            }
        }

        let decoded = decoded_overview(&encoded_value(&value));
        assert_eq!(decoded, expected);
        let encoded: serde_json::Value =
            serde_json::from_slice(&encode_overview_response(&decoded).unwrap()).unwrap();
        assert_eq!(encoded["overview"]["runner"]["processState"], "draining");
        assert_eq!(
            encoded["overview"]["recentAssignments"][0]["result"],
            "superseded"
        );
        assert_eq!(encoded["overview"]["events"][0]["kind"], "rebooted");
        assert_eq!(encoded["overview"]["events"][6]["count"], 1);
        assert_eq!(
            encoded["overview"]["agentLogins"]["providers"][2],
            serde_json::json!({"provider": "gemini", "state": "ready"})
        );
        assert!(control_validator().is_valid(&value));
    }

    // REQ-RUNNERENROLL-135, REQ-RUNNERENROLL-137, REQ-RUNNERENROLL-138: an overview the
    // reader cannot read is a protocol failure, never a partial view.
    #[test]
    fn an_overview_the_reader_cannot_read_is_invalid() {
        let value: serde_json::Value =
            serde_json::from_slice(&overview_fixture("valid/overview-response.json")).unwrap();
        let mut refused = Vec::new();

        let mut foreign_run = value.clone();
        foreign_run["overview"]["assignments"][0]["runId"] =
            "asn_01k0z6r1w8f4jy2m7q9v3x5abc".into();
        refused.push(foreign_run);

        let mut repeated = value.clone();
        repeated["overview"]["assignments"][1]["assignmentId"] =
            repeated["overview"]["assignments"][0]["assignmentId"].clone();
        refused.push(repeated);

        let mut seventeen = value.clone();
        let step = seventeen["overview"]["assignments"][0]["runningSteps"][0].clone();
        seventeen["overview"]["assignments"][0]["runningSteps"] =
            serde_json::Value::Array(vec![step; 17]);
        refused.push(seventeen);

        let mut ok = value.clone();
        ok["outcome"] = "ok".into();
        refused.push(ok);

        let mut nameless = value.clone();
        nameless["overview"]["agentLogins"]["providers"][0] = serde_json::json!({"ready": true});
        refused.push(nameless);

        let mut recent_foreign_run = value.clone();
        recent_foreign_run["overview"]["recentAssignments"][0]["runId"] =
            "asn_01k0z6r1w8f4jy2m7q9v3x5abe".into();
        refused.push(recent_foreign_run);

        let mut recent_untimed = value.clone();
        recent_untimed["overview"]["recentAssignments"][0]["endedAt"] = "2026-11-19 13:50".into();
        refused.push(recent_untimed);

        let mut event_untimed = value.clone();
        event_untimed["overview"]["events"][0]["at"] = "yesterday".into();
        refused.push(event_untimed);

        let mut never_happened = value.clone();
        never_happened["overview"]["events"][0]["count"] = 0.into();
        refused.push(never_happened);

        let mut foreign_credential = value.clone();
        assert_eq!(
            foreign_credential["overview"]["events"][8]["kind"],
            "credential_reloaded"
        );
        foreign_credential["overview"]["events"][8]["credentialId"] =
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".into();
        refused.push(foreign_credential);

        let mut too_many_events = value.clone();
        let event = too_many_events["overview"]["events"][0].clone();
        too_many_events["overview"]["events"] = serde_json::Value::Array(vec![event; 257]);
        refused.push(too_many_events);

        let mut later_version = value;
        later_version["schemaVersion"] = 2.into();
        refused.push(later_version);

        for refused in refused {
            assert_eq!(
                decode_overview_response(&encoded_value(&refused)),
                Err(ProtocolFailure::Invalid),
                "{refused}"
            );
        }
        for invalid in [
            "invalid/overview-without-runner.json",
            "invalid/overview-entry-without-assignment-id.json",
            "invalid/overview-recent-assignment-without-result.json",
            "invalid/overview-event-without-kind.json",
        ] {
            assert_eq!(
                decode_overview_response(&overview_fixture(invalid)),
                Err(ProtocolFailure::Invalid),
                "{invalid}"
            );
        }

        assert_eq!(
            decode_overview_response(&overview_fixture("valid/error-response.json")),
            Ok(OverviewAnswer::Refused(
                ControlError::PendingAuthenticationFailed
            ))
        );
    }

    // REQ-RUNNERENROLL-136: the overview answer's bound is 1048576 bytes including its
    // LF, while every other answer keeps 65536.
    #[test]
    fn the_overview_bound_is_its_own_and_the_status_bound_is_unchanged() {
        let mut encoded = overview_fixture("valid/overview-response.json");
        encoded.splice(
            encoded.len() - 1..encoded.len() - 1,
            std::iter::repeat_n(b' ', OVERVIEW_RESPONSE_LIMIT - encoded.len()),
        );
        assert_eq!(encoded.len(), 1_048_576);
        assert_eq!(decoded_overview(&encoded), stated_overview());
        encoded.insert(encoded.len() - 1, b' ');
        assert_eq!(
            decode_overview_response(&encoded),
            Err(ProtocolFailure::Invalid)
        );

        let mut status = overview_fixture("valid/status-idle-response.json");
        status.splice(
            status.len() - 1..status.len() - 1,
            std::iter::repeat_n(b' ', 65_537 - status.len()),
        );
        assert_eq!(status.len(), 65_537);
        assert_eq!(decode_response(&status), Err(ProtocolFailure::Invalid));
    }

    fn timestamp_free_entry(id: &str, phase: AssignmentPhase) -> OverviewAssignment {
        OverviewAssignment {
            assignment_id: format!("asn_01k0z6r1w8f4jy2m7q9v3x5{id}"),
            run_id: format!("run_01k0z6r1w8f4jy2m7q9v3x5{id}"),
            attempt_number: 1,
            workflow_path: ".useful/engineering/workflow.yaml".to_owned(),
            phase: phase.into(),
            activity: None,
            held_milliseconds: 2_000,
            continues_earlier_attempt: false,
            lease: None,
            subject: None,
            run_url: None,
            position: None,
            wait: None,
            running_steps: Vec::new(),
            running_steps_omitted: 0,
        }
    }

    fn element(step: &str, item: Option<u64>, ordinal: u64) -> OverviewRunElement {
        OverviewRunElement {
            step: step.to_owned(),
            item,
            ordinal,
        }
    }

    fn strings(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| (*word).to_owned()).collect()
    }

    /// A required step of the run with no times, no declaration and no outputs.
    fn required_step(id: &str, kind: &str, state: &str) -> AssignmentStep {
        AssignmentStep {
            id: id.to_owned(),
            role: "step".to_owned(),
            kind: kind.to_owned(),
            needs: Vec::new(),
            state: state.to_owned(),
            failure_policy: "required".to_owned(),
            started_at: None,
            duration_milliseconds: None,
            command: None,
            agent: None,
            workflow: None,
            outputs: Vec::new(),
            child_runs: Vec::new(),
            child_runs_omitted: 0,
        }
    }

    /// The assignment `valid/assignment-response.json` states.
    fn stated_assignment() -> AssignmentDetail {
        let path = vec![element("build", Some(1), 2)];
        let mut entry = timestamp_free_entry("abc", AssignmentPhase::Running);
        entry.attempt_number = 2;
        entry.activity = Some(HeldRunState::Working.into());
        entry.held_milliseconds = 1_080_000;
        entry.continues_earlier_attempt = true;
        entry.lease = Some(OverviewLease {
            health: LeaseHealth::Ok.into(),
            time_left_milliseconds: 42_000,
        });
        entry.position = Some(vec![StepPathPart::Name("build".to_owned())]);
        entry.running_steps = vec![OverviewRunningStep {
            run: path.clone(),
            run_attempt: 2,
            step: "implement".to_owned(),
            state: LiveStepState::Running.into(),
        }];
        let output = |name: &str, kind: &str, committed| AssignmentOutput {
            name: name.to_owned(),
            kind: kind.to_owned(),
            committed,
        };
        let stream =
            |retained_bytes, discarded_bytes, omitted_bytes, text: &str| AssignmentLogStream {
                retained_bytes,
                discarded_bytes,
                omitted_bytes,
                text: text.to_owned(),
            };
        AssignmentDetail {
            entry,
            run: Some(AssignmentRun {
                path,
                workflow_path: "build.yaml".to_owned(),
                state: "running".to_owned(),
                attempts: 2,
                outcome: None,
                attempt_number: 2,
                attempt_state: "running".to_owned(),
                wait: Some(OverviewWait {
                    kind: "external".to_owned(),
                    since: Some("2026-11-19T14:10:00Z".to_owned()),
                    step: vec![StepPathPart::Name("queue".to_owned())],
                }),
                steps: vec![
                    AssignmentStep {
                        started_at: Some("2026-11-19T14:02:00Z".to_owned()),
                        duration_milliseconds: Some(4_100),
                        command: Some(AssignmentCommand {
                            argv: strings(&["scripts/plan", "--all"]),
                            cwd: Some("tools".to_owned()),
                        }),
                        outputs: vec![output("plan", "text", true), output("notes", "file", false)],
                        ..required_step("plan", "cmd", "succeeded")
                    },
                    AssignmentStep {
                        needs: strings(&["plan"]),
                        started_at: Some("2026-11-19T14:02:05Z".to_owned()),
                        duration_milliseconds: Some(61_000),
                        workflow: Some(AssignmentWorkflow {
                            file: "implement.yaml".to_owned(),
                        }),
                        child_runs: vec![
                            AssignmentChildRun {
                                ordinal: 1,
                                item: Some(0),
                                state: "exited".to_owned(),
                                outcome: Some("approved".to_owned()),
                            },
                            AssignmentChildRun {
                                ordinal: 1,
                                item: Some(1),
                                state: "running".to_owned(),
                                outcome: None,
                            },
                        ],
                        child_runs_omitted: 2,
                        ..required_step("implement", "workflow", "running")
                    },
                    AssignmentStep {
                        role: "finalizer".to_owned(),
                        failure_policy: "advisory".to_owned(),
                        agent: Some(AssignmentAgent {
                            profile: "reviewer".to_owned(),
                            harness: "claude_code".to_owned(),
                            model: Some("claude-opus-4".to_owned()),
                            effort: Some("high".to_owned()),
                        }),
                        ..required_step("review", "agent", "pending")
                    },
                ],
                steps_omitted: 0,
            }),
            log: Some(AssignmentLog {
                source: AssignmentLogSource::Step.into(),
                step: Some("plan".to_owned()),
                invocation_id: Some(3),
                stdout: Some(stream(9_000, 12, 8_989, "plan ready\n")),
                stderr: Some(stream(0, 0, 0, "")),
                earlier_invocations_omitted: 1,
                text: None,
                omitted_bytes: None,
                records: Vec::new(),
                omitted_records: 0,
            }),
        }
    }

    /// The query `valid/assignment-request.json` states.
    fn stated_query() -> AssignmentQuery {
        AssignmentQuery {
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            run: vec![element("build", Some(1), 2), element("fix", None, 1)],
            step: Some("implement".to_owned()),
        }
    }

    /// The query an `assignment` request line states, as the serving process reads it.
    fn decoded_query(bytes: &[u8]) -> Result<AssignmentQuery, ControlError> {
        match decode_request(bytes)? {
            ControlRequest::Assignment(query) => Ok(query),
            other => panic!("an assignment request read as {other:?}"),
        }
    }

    fn decoded_assignment(bytes: &[u8]) -> AssignmentDetail {
        match decode_assignment_response(bytes) {
            Ok(AssignmentAnswer::Assignment(detail)) => *detail,
            other => panic!("the assignment did not decode: {other:?}"),
        }
    }

    // REQ-RUNNERENROLL-144: the assignment answer decodes to what it states and encodes
    // back to its bytes; an entry alone is an answer with no run and no log; an assignment
    // the runner does not hold, and a request an earlier release refuses, are answers.
    #[test]
    fn an_assignment_decodes_to_what_it_states_and_encodes_to_its_bytes() {
        let bytes = overview_fixture("valid/assignment-response.json");
        let detail = decoded_assignment(&bytes);
        assert_eq!(detail, stated_assignment());
        assert_eq!(encode_assignment_response(&detail).unwrap(), bytes);

        let entry_only = overview_fixture("valid/assignment-entry-response.json");
        let detail = decoded_assignment(&entry_only);
        assert_eq!(
            detail,
            AssignmentDetail {
                entry: timestamp_free_entry("abd", AssignmentPhase::Preparing),
                run: None,
                log: None,
            }
        );
        assert_eq!(encode_assignment_response(&detail).unwrap(), entry_only);

        let not_held = overview_fixture("valid/assignment-not-held-response.json");
        assert_eq!(
            decode_assignment_response(&not_held),
            Ok(AssignmentAnswer::NotHeld)
        );
        assert_eq!(encode_assignment_not_held(), not_held);
        assert_eq!(
            decode_assignment_response(
                b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"invalid_request\"}\n"
            ),
            Ok(AssignmentAnswer::Refused(ControlError::InvalidRequest))
        );
    }

    // REQ-RUNNERENROLL-144: an `assignment` request reads into the query it states, and
    // admits exactly its ID, a bounded `run` path of identifiers and numbers and a step
    // identifier; no other operation admits those members.
    #[test]
    fn an_assignment_request_reads_into_its_query_and_admits_nothing_else() {
        let request = overview_fixture("valid/assignment-request.json");
        assert_eq!(decoded_query(&request), Ok(stated_query()));
        assert_eq!(encode_assignment_request(&stated_query()).unwrap(), request);
        let root = overview_fixture("valid/assignment-root-request.json");
        let root_query = AssignmentQuery {
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            run: Vec::new(),
            step: None,
        };
        assert_eq!(decoded_query(&root), Ok(root_query.clone()));
        assert_eq!(encode_assignment_request(&root_query).unwrap(), root);

        let value: serde_json::Value = serde_json::from_slice(&request).unwrap();
        let mut refused = Vec::new();
        let mut without_id = value.clone();
        without_id.as_object_mut().unwrap().remove("assignmentId");
        refused.push(without_id);
        let mut run_id = value.clone();
        run_id["assignmentId"] = "run_01k0z6r1w8f4jy2m7q9v3x5abc".into();
        refused.push(run_id);
        let mut seventeen = value.clone();
        seventeen["run"] = serde_json::Value::Array(vec![value["run"][1].clone(); 17]);
        refused.push(seventeen);
        let mut extra_member = value.clone();
        extra_member["run"][0]["attempt"] = 1.into();
        refused.push(extra_member);
        let mut past_the_bound = value.clone();
        past_the_bound["run"][0]["ordinal"] = 2_147_483_648_u64.into();
        refused.push(past_the_bound);
        let mut not_an_identifier = value.clone();
        not_an_identifier["step"] = "../implement".into();
        refused.push(not_an_identifier);
        let mut unknown_key = value.clone();
        unknown_key["path"] = "/tmp/x".into();
        refused.push(unknown_key);
        for refused in refused {
            assert_eq!(
                decoded_query(&encoded_value(&refused)),
                Err(ControlError::InvalidRequest),
                "{refused}"
            );
        }
        for invalid in [
            "invalid/assignment-request-without-assignment-id.json",
            "invalid/assignment-request-extra-field.json",
        ] {
            assert_eq!(
                decoded_query(&overview_fixture(invalid)),
                Err(ControlError::InvalidRequest),
                "{invalid}"
            );
        }

        let mut sixteen = value;
        sixteen["run"] = serde_json::Value::Array(vec![sixteen["run"][1].clone(); 16]);
        assert_eq!(
            decoded_query(&encoded_value(&sixteen)).map(|query| query.run.len()),
            Ok(16)
        );

        for operation in ["status", "overview"] {
            let request = format!(
                r#"{{"schemaVersion":1,"operation":"{operation}","assignmentId":"asn_01k0z6r1w8f4jy2m7q9v3x5abc"}}"#
            );
            assert_eq!(
                decode_request(&encoded_value(&serde_json::from_str(&request).unwrap())),
                Err(ControlError::InvalidRequest),
                "{operation}"
            );
        }
    }

    // REQ-RUNNERENROLL-135: a member this release does not know is dropped at every level
    // of the assignment answer, and a step state, a kind and a log source it does not know
    // are kept as received.
    #[test]
    fn an_assignment_keeps_unknown_words_and_drops_unknown_members() {
        let mut value: serde_json::Value =
            serde_json::from_slice(&overview_fixture("valid/assignment-response.json")).unwrap();
        let unknown = serde_json::json!({"nested": [1, "two"]});
        for member in [
            "",
            "/assignment",
            "/assignment/entry",
            "/assignment/run",
            "/assignment/run/path/0",
            "/assignment/run/wait",
            "/assignment/run/steps/0",
            "/assignment/run/steps/0/command",
            "/assignment/run/steps/0/outputs/0",
            "/assignment/run/steps/1/workflow",
            "/assignment/run/steps/1/childRuns/0",
            "/assignment/run/steps/2/agent",
            "/assignment/log",
            "/assignment/log/stdout",
        ] {
            value.pointer_mut(member).unwrap()["laterMember"] = unknown.clone();
        }
        value["assignment"]["run"]["steps"][0]["state"] = "skipped_later".into();
        value["assignment"]["run"]["steps"][2]["kind"] = "subagent".into();
        value["assignment"]["log"]["source"] = "artifact".into();

        let mut expected = stated_assignment();
        if let Some(run) = expected.run.as_mut() {
            run.steps[0].state = "skipped_later".to_owned();
            run.steps[2].kind = "subagent".to_owned();
        }
        if let Some(log) = expected.log.as_mut() {
            log.source = OpenWord::Unknown("artifact".to_owned());
        }
        let decoded = decoded_assignment(&encoded_value(&value));
        assert_eq!(decoded, expected);
        let encoded: serde_json::Value =
            serde_json::from_slice(&encode_assignment_response(&decoded).unwrap()).unwrap();
        assert_eq!(encoded["assignment"]["log"]["source"], "artifact");
        assert_eq!(encoded["assignment"]["run"]["steps"][2]["kind"], "subagent");
        assert!(control_validator().is_valid(&value));
    }

    // REQ-RUNNERENROLL-145: with no step output to give, the answer's log is the end of
    // the run log with the bytes before it counted.
    #[test]
    fn a_run_log_answer_carries_its_text_and_what_was_left_out() {
        let bytes = overview_fixture("valid/assignment-run-log-response.json");
        let detail = decoded_assignment(&bytes);
        assert_eq!(detail.run, None);
        assert_eq!(
            detail.log,
            Some(AssignmentLog {
                source: AssignmentLogSource::RunLog.into(),
                step: None,
                invocation_id: None,
                stdout: None,
                stderr: None,
                earlier_invocations_omitted: 0,
                text: Some("resumed the run\nwaiting for the queue\n".to_owned()),
                omitted_bytes: Some(4_096),
                records: Vec::new(),
                omitted_records: 0,
            })
        );
        assert_eq!(encode_assignment_response(&detail).unwrap(), bytes);
    }

    /// The assignment `valid/assignment-step-records-response.json` states.
    fn stated_step_records_assignment() -> AssignmentDetail {
        let mut entry = timestamp_free_entry("abe", AssignmentPhase::Running);
        entry.workflow_path = "ci.yaml".to_owned();
        entry.activity = Some(HeldRunState::Working.into());
        entry.held_milliseconds = 61_000;
        entry.lease = Some(OverviewLease {
            health: LeaseHealth::Ok.into(),
            time_left_milliseconds: 42_000,
        });
        entry.running_steps = vec![OverviewRunningStep {
            run: Vec::new(),
            run_attempt: 1,
            step: "build".to_owned(),
            state: LiveStepState::Running.into(),
        }];
        let record = |source: &str, text: &str, continuation| AssignmentLogRecord {
            source: source.to_owned(),
            text: text.to_owned(),
            continuation,
        };
        AssignmentDetail {
            entry,
            run: Some(AssignmentRun {
                path: Vec::new(),
                workflow_path: "ci.yaml".to_owned(),
                state: "running".to_owned(),
                attempts: 1,
                outcome: None,
                attempt_number: 1,
                attempt_state: "running".to_owned(),
                wait: None,
                steps: vec![
                    AssignmentStep {
                        started_at: Some("2026-11-20T14:02:00Z".to_owned()),
                        duration_milliseconds: Some(61_000),
                        command: Some(AssignmentCommand {
                            argv: strings(&["make", "build"]),
                            cwd: None,
                        }),
                        ..required_step("build", "cmd", "running")
                    },
                    AssignmentStep {
                        needs: strings(&["build"]),
                        agent: Some(AssignmentAgent {
                            profile: "reviewer".to_owned(),
                            harness: "claude_code".to_owned(),
                            model: Some("claude-opus-4".to_owned()),
                            effort: Some("high".to_owned()),
                        }),
                        ..required_step("review", "agent", "pending")
                    },
                ],
                steps_omitted: 0,
            }),
            log: Some(AssignmentLog {
                source: AssignmentLogSource::StepRecords.into(),
                step: Some("build".to_owned()),
                invocation_id: None,
                stdout: None,
                stderr: None,
                earlier_invocations_omitted: 0,
                text: None,
                omitted_bytes: Some(3_400),
                records: vec![
                    record("stdout", "compiling core", false),
                    record("stderr", "warning: unused import", false),
                    record("stdout", " in core/src/lib.rs", true),
                ],
                omitted_records: 12,
            }),
        }
    }

    // REQ-RUNNERENROLL-145: a single-workflow assignment's log is the named step's records
    // with the older ones counted; with no step named it is its source alone.
    #[test]
    fn a_step_records_answer_carries_the_records_and_what_was_left_out() {
        let bytes = overview_fixture("valid/assignment-step-records-response.json");
        let detail = decoded_assignment(&bytes);
        assert_eq!(detail, stated_step_records_assignment());
        assert_eq!(encode_assignment_response(&detail).unwrap(), bytes);

        let none = overview_fixture("valid/assignment-step-records-none-response.json");
        let detail = decoded_assignment(&none);
        let mut expected = stated_step_records_assignment();
        expected.log = Some(AssignmentLog {
            source: AssignmentLogSource::StepRecords.into(),
            step: None,
            invocation_id: None,
            stdout: None,
            stderr: None,
            earlier_invocations_omitted: 0,
            text: None,
            omitted_bytes: None,
            records: Vec::new(),
            omitted_records: 0,
        });
        assert_eq!(detail, expected);
        assert_eq!(encode_assignment_response(&detail).unwrap(), none);
    }

    // REQ-RUNNERENROLL-135: a record source and a running step's state this release does
    // not know are kept as received, a member it does not know inside a record is dropped,
    // and `recovering` is a known running step's state.
    #[test]
    fn a_step_records_answer_keeps_unknown_words() {
        let mut value: serde_json::Value = serde_json::from_slice(&overview_fixture(
            "valid/assignment-step-records-response.json",
        ))
        .unwrap();
        value["assignment"]["log"]["records"][1]["source"] = "screenshot".into();
        value["assignment"]["log"]["records"][1]["laterMember"] = 7.into();
        let mut expected = stated_step_records_assignment();
        if let Some(log) = expected.log.as_mut() {
            log.records[1].source = "screenshot".to_owned();
        }
        assert_eq!(decoded_assignment(&encoded_value(&value)), expected);

        for (word, state) in [
            ("recovering", OpenWord::Known(LiveStepState::Recovering)),
            ("paused_later", OpenWord::Unknown("paused_later".to_owned())),
        ] {
            value["assignment"]["entry"]["runningSteps"][0]["state"] = word.into();
            let detail = decoded_assignment(&encoded_value(&value));
            assert_eq!(detail.entry.running_steps[0].state, state, "{word}");
        }
    }

    // REQ-RUNNERENROLL-136: a log of 16384 records is read and one more is invalid.
    #[test]
    fn a_step_records_answer_past_the_record_bound_is_invalid() {
        let mut value: serde_json::Value = serde_json::from_slice(&overview_fixture(
            "valid/assignment-step-records-response.json",
        ))
        .unwrap();
        let record = serde_json::json!({"source": "stdout", "text": ""});
        value["assignment"]["log"]["records"] =
            serde_json::Value::Array(vec![record.clone(); 16_384]);
        assert_eq!(
            decoded_assignment(&encoded_value(&value))
                .log
                .map(|log| log.records.len()),
            Some(16_384)
        );
        value["assignment"]["log"]["records"] = serde_json::Value::Array(vec![record; 16_385]);
        assert_eq!(
            decode_assignment_response(&encoded_value(&value)),
            Err(ProtocolFailure::Invalid)
        );
        assert_eq!(
            decode_assignment_response(&overview_fixture(
                "invalid/assignment-log-record-without-text.json"
            )),
            Err(ProtocolFailure::Invalid)
        );
    }

    // REQ-RUNNERENROLL-136: the assignment answer shares the overview's bound, and an
    // answer the reader cannot read, or that passes the bounds it puts on a run, is a
    // protocol failure, never a partial view.
    #[test]
    fn an_assignment_the_reader_cannot_read_is_invalid() {
        let mut encoded = overview_fixture("valid/assignment-response.json");
        encoded.splice(
            encoded.len() - 1..encoded.len() - 1,
            std::iter::repeat_n(b' ', 1_048_576 - encoded.len()),
        );
        assert_eq!(decoded_assignment(&encoded), stated_assignment());
        encoded.insert(encoded.len() - 1, b' ');
        assert_eq!(
            decode_assignment_response(&encoded),
            Err(ProtocolFailure::Invalid)
        );

        let value: serde_json::Value =
            serde_json::from_slice(&overview_fixture("valid/assignment-response.json")).unwrap();
        let mut refused = Vec::new();
        let mut wrong_type = value.clone();
        wrong_type["assignment"]["entry"]["runId"] = 7.into();
        refused.push(wrong_type);
        let mut foreign_run = value.clone();
        foreign_run["assignment"]["entry"]["runId"] = "asn_01k0z6r1w8f4jy2m7q9v3x5abc".into();
        refused.push(foreign_run);
        let mut deep = value.clone();
        let level = deep["assignment"]["run"]["path"][0].clone();
        deep["assignment"]["run"]["path"] = serde_json::Value::Array(vec![level; 17]);
        refused.push(deep);
        // A step of the required members alone, so that thousands stay within the bound.
        let pending = serde_json::json!({
            "id": "a", "role": "step", "kind": "cmd", "state": "pending", "failurePolicy": "required"
        });
        let mut too_many_steps = value.clone();
        too_many_steps["assignment"]["run"]["steps"] =
            serde_json::Value::Array(vec![pending.clone(); 4_097]);
        refused.push(too_many_steps);
        let mut too_many_children = value.clone();
        let child = too_many_children["assignment"]["run"]["steps"][1]["childRuns"][1].clone();
        too_many_children["assignment"]["run"]["steps"][1]["childRuns"] =
            serde_json::Value::Array(vec![child; 257]);
        refused.push(too_many_children);
        let mut overview = value.clone();
        overview["outcome"] = "overview".into();
        refused.push(overview);
        let mut unknown_error = value.clone();
        unknown_error["outcome"] = "error".into();
        unknown_error["error"] = "credential_missing".into();
        refused.push(unknown_error);
        let mut later_version = value.clone();
        later_version["schemaVersion"] = 2.into();
        refused.push(later_version);
        for refused in refused {
            assert_eq!(
                decode_assignment_response(&encoded_value(&refused)),
                Err(ProtocolFailure::Invalid),
                "{}",
                refused["outcome"]
            );
        }
        assert_eq!(
            decode_assignment_response(&overview_fixture("invalid/assignment-without-entry.json")),
            Err(ProtocolFailure::Invalid)
        );

        let mut at_the_bounds = value;
        let run = &mut at_the_bounds["assignment"]["run"];
        let level = run["path"][0].clone();
        run["path"] = serde_json::Value::Array(vec![level; 16]);
        let child = run["steps"][1]["childRuns"][1].clone();
        run["steps"][1]["childRuns"] = serde_json::Value::Array(vec![child; 256]);
        let steps = run["steps"].as_array_mut().unwrap();
        steps.resize(4_096, pending);
        let detail = decoded_assignment(&encoded_value(&at_the_bounds));
        assert_eq!(
            detail.run.map(|run| (
                run.path.len(),
                run.steps.len(),
                run.steps[1].child_runs.len()
            )),
            Some((16, 4_096, 256))
        );
    }
}
