//! The runner's requests to Cloud for what it keeps in its agent-login
//! directory: its agent-login status and one delivery, which name neither a
//! boot nor an assignment, since an agent login is a standing install bound
//! to the runner alone, and the OAuth providers an assignment can be issued
//! a grant token of (REQ-GRANT-97) and the token of one of them, which name
//! both (REQ-GRANT-84); and its report of where an assignment's credentials
//! come from, which names both too (REQ-GRANT-91). Each is a POST on the
//! HTTP base of the runner's endpoint, authenticated by the enrolled runner
//! credential.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use base64::Engine as _;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize, Serializer};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use url::Url;
use zeroize::Zeroizing;

use super::readiness::{LastUse, UseOutcome};
use super::{FailureCause, OperatingSystem};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState, CredentialSource};
use crate::credential::Credential;
use crate::runner_http::{self, SensitiveString};

const STATUS_PATH: &str = "/v1/runner/agent-logins/status";
const DELIVERY_PATH: &str = "/v1/runner/agent-logins/delivery";
const GRANT_TOKEN_PATH: &str = "/v1/runner/grants/token";
const GRANT_PROVIDERS_PATH: &str = "/v1/runner/grants/providers";
const TOKEN_SERVICES_PATH: &str = "/v1/runner/integrations/providers";
const CREDENTIAL_SOURCES_PATH: &str = "/v1/runner/credential-sources";
/// Cloud accepts no larger material for any provider.
const MAXIMUM_MATERIAL_BYTES: usize = 32 * 1024;
/// Cloud lists no more providers for an assignment (REQ-GRANT-97).
const MAXIMUM_LISTED_PROVIDERS: usize = 32;
/// The longest provider id, `^[a-z][a-z0-9.-]{0,62}$`.
const MAXIMUM_PROVIDER_ID_BYTES: usize = 63;

/// Cloud's side of agent-login setup. Each call blocks until Cloud answers
/// or the request fails, so a caller on an async runtime runs it on a
/// blocking thread.
pub(crate) trait AgentLoginCloud: Send + Sync {
    /// Which agent logins Cloud has authorized for this runner among the
    /// providers the request names (REQ-GRANT-25). An entry for a
    /// provider this build cannot install is dropped.
    fn status(&self, request: &StatusRequest) -> Result<Vec<ProviderLogin>, CloudFailure>;

    /// One generation of one agent login (REQ-GRANT-26).
    fn delivery(&self, request: &DeliveryRequest) -> Result<Delivery, DeliveryFailure>;

    /// The access token of the grant Cloud resolves for the assignment the
    /// request names (REQ-GRANT-84). A Cloud without the route answers
    /// `NoRoute`.
    fn grant_token(&self, request: &GrantTokenRequest) -> Result<IssuedToken, GrantTokenFailure>;

    /// The OAuth providers Cloud can issue the assignment the request names
    /// a grant token of, in its order, each with the two names its token is
    /// offered under (REQ-GRANT-97). A Cloud without the route answers
    /// `NoRoute`.
    fn grant_providers(
        &self,
        request: &GrantProvidersRequest,
    ) -> Result<Vec<ListedProvider>, GrantProvidersFailure>;

    /// The token services the assignment the request names declares, in
    /// Cloud's order, each with the two names its token is offered under
    /// (REQ-INTEGRATION-9, REQ-INTEGRATION-12): the request and answers of
    /// `grant_providers` under another path, where no id is one of the
    /// runner's three. A Cloud without the route answers `NoRoute`.
    fn token_services(
        &self,
        request: &GrantProvidersRequest,
    ) -> Result<Vec<ListedProvider>, GrantProvidersFailure>;

    /// Tells Cloud where the credentials of the assignment the request names
    /// come from (REQ-GRANT-91, REQ-GRANT-93), a token service's included
    /// (REQ-INTEGRATION-12), in one bounded request; Cloud
    /// answers nothing more than that it recorded the report. A Cloud
    /// without the route answers `NoRoute`.
    fn credential_sources(
        &self,
        request: &CredentialSourcesRequest,
    ) -> Result<(), CredentialSourcesFailure>;

    /// Makes later requests use the endpoint and credential the runner now
    /// serves with, after a live credential promotion.
    fn follow_credential(&self, endpoint: &Url, runner_credential: &Credential);
}

/// What the runner tells Cloud when it asks for its status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StatusRequest {
    pub(crate) os: OperatingSystem,
    /// Each provider the runner can install, at most once.
    pub(crate) providers: Vec<ProviderReport>,
}

/// One provider as the runner reports it: what it installed, its last
/// readiness and how the provider answered the installed login's last use
/// and when, where there are any.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderReport {
    pub(crate) provider: AgentLoginProvider,
    pub(crate) installed: Option<InstalledLogin>,
    pub(crate) state: Option<AgentLoginState>,
    pub(crate) last_use: Option<LastUse>,
}

impl ProviderReport {
    /// The report of `provider`, stating `last_use` only with `state`
    /// `ready` and an install, the only report Cloud takes it with
    /// (REQ-GRANT-25, REQ-GRANT-52).
    pub(crate) fn new(
        provider: AgentLoginProvider,
        installed: Option<InstalledLogin>,
        state: Option<AgentLoginState>,
        last_use: Option<LastUse>,
    ) -> Self {
        let stated = installed.is_some() && state == Some(AgentLoginState::Ready);
        Self {
            provider,
            installed,
            state,
            last_use: last_use.filter(|_| stated),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InstalledLogin {
    pub(crate) agent_login_id: String,
    pub(crate) generation: u64,
}

/// Cloud's answer about one provider the runner named.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderLogin {
    pub(crate) provider: AgentLoginProvider,
    pub(crate) agent_login_id: String,
    pub(crate) status: LoginStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LoginStatus {
    Authorized {
        generation: u64,
        record_schema_version: u32,
    },
    Revoked,
    Unauthorized,
    UnsupportedTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeliveryRequest {
    pub(crate) os: OperatingSystem,
    pub(crate) provider: AgentLoginProvider,
    pub(crate) agent_login_id: String,
    pub(crate) generation: u64,
}

/// One delivered generation of an agent login.
#[derive(Debug)]
pub(crate) struct Delivery {
    pub(crate) provider: AgentLoginProvider,
    pub(crate) agent_login_id: String,
    pub(crate) generation: u64,
    pub(crate) record_schema_version: u32,
    pub(crate) material: LoginMaterial,
}

/// Decoded agent-login material: cleared when dropped and never shown by
/// `Debug`.
pub(crate) struct LoginMaterial(Zeroizing<Vec<u8>>);

impl LoginMaterial {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        Self(Zeroizing::new(bytes.to_vec()))
    }
}

impl std::fmt::Debug for LoginMaterial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LoginMaterial([redacted])")
    }
}

/// The reason Cloud names when it refuses a delivery.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeliveryRefusal {
    Unauthorized,
    Revoked,
    StaleGeneration,
    UnsupportedTarget,
}

/// A request Cloud did not answer usefully; the runner records any of them
/// as `cloud_unavailable` for a provider it last knew as authorized or
/// installed, and keeps what it knew of any other.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CloudFailure {
    /// No answer: the connection failed or timed out.
    Transport,
    /// A status other than the request's answers, as from a Cloud without
    /// the routes or one that is failing.
    Status(u16),
    /// An answer the runner must not trust: not private, not JSON, too
    /// large, or not the closed shape of schema version 1.
    InvalidResponse,
}

impl CloudFailure {
    pub(crate) const fn cause(self) -> FailureCause {
        match self {
            Self::Transport => FailureCause::Transport,
            Self::Status(status) => FailureCause::HttpStatus(status),
            Self::InvalidResponse => FailureCause::InvalidResponse,
        }
    }
}

/// A provider whose token the runner asks Cloud for: one of the runner's
/// three OAuth providers, GitHub, GitLab.com and Linear, an OAuth provider a
/// deployment added by configuration, which Cloud lists by its id
/// (REQ-GRANT-97), or a token service the assignment declares, which Cloud
/// lists by its id on a list of its own (REQ-INTEGRATION-12).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GrantProvider(GrantProviderKind);

#[derive(Clone, Debug, Eq, PartialEq)]
enum GrantProviderKind {
    /// GitHub, GitLab.com or Linear.
    Runner(RunnerGrantProvider),
    /// A valid provider id of the OAuth list that is not one of the
    /// runner's three.
    Configured(String),
    /// A valid provider id of the token-service list, never one of the
    /// runner's three; its token is issued from an integration token.
    TokenService(String),
}

/// One of the runner's three OAuth providers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunnerGrantProvider {
    Github,
    Gitlab,
    Linear,
}

impl RunnerGrantProvider {
    const ALL: [Self; 3] = [Self::Github, Self::Gitlab, Self::Linear];

    const fn agent_login_provider(self) -> AgentLoginProvider {
        match self {
            Self::Github => AgentLoginProvider::Github,
            Self::Gitlab => AgentLoginProvider::Gitlab,
            Self::Linear => AgentLoginProvider::Linear,
        }
    }

    /// Cloud's id of the provider, as `AgentLoginProvider::grant_provider`
    /// names it.
    const fn cloud_id(self) -> &'static str {
        match self {
            Self::Github => "github",
            Self::Gitlab => "gitlab.com",
            Self::Linear => "linear",
        }
    }
}

impl GrantProvider {
    /// The provider Cloud lists as `id`: one of the runner's three where
    /// `id` is the grant provider of one (`github`, `gitlab.com` or
    /// `linear`), and a configured provider for any other valid provider id,
    /// so `gitlab` is a configured provider and not the runner's GitLab.
    /// `None` for an id outside `^[a-z][a-z0-9.-]{0,62}$`.
    pub(crate) fn listed(id: &str) -> Option<Self> {
        if !valid_provider_id(id) {
            return None;
        }
        Some(Self(
            RunnerGrantProvider::ALL
                .into_iter()
                .find(|provider| provider.cloud_id() == id)
                .map_or_else(
                    || GrantProviderKind::Configured(id.to_owned()),
                    GrantProviderKind::Runner,
                ),
        ))
    }

    /// The token service Cloud lists as `id` (REQ-INTEGRATION-12); `None`
    /// for an id outside `^[a-z][a-z0-9.-]{0,62}$` and for `github`,
    /// `gitlab.com` and `linear`, which are never token services.
    pub(crate) fn token_service(id: &str) -> Option<Self> {
        (valid_provider_id(id) && !runner_provider_id(id))
            .then(|| Self(GrantProviderKind::TokenService(id.to_owned())))
    }

    /// Cloud's id of the provider, which a token request names and its
    /// answer repeats: `github`, `gitlab.com` or `linear` for the runner's
    /// three, and the listed id for any other provider.
    pub(crate) fn cloud_id(&self) -> &str {
        match &self.0 {
            GrantProviderKind::Runner(provider) => provider.cloud_id(),
            GrantProviderKind::Configured(id) | GrantProviderKind::TokenService(id) => id,
        }
    }

    /// The runner's own provider for its three; `None` for a configured
    /// provider and a token service.
    pub(crate) const fn agent_login_provider(&self) -> Option<AgentLoginProvider> {
        match &self.0 {
            GrantProviderKind::Runner(provider) => Some(provider.agent_login_provider()),
            GrantProviderKind::Configured(_) | GrantProviderKind::TokenService(_) => None,
        }
    }

    /// The prefix of the ID Cloud names the token's source by: an
    /// integration token's (`itk_`) for a token service, a grant's (`pgr_`)
    /// for any other provider.
    const fn issued_id_prefix(&self) -> &'static str {
        match &self.0 {
            GrantProviderKind::TokenService(_) => "itk_",
            GrantProviderKind::Runner(_) | GrantProviderKind::Configured(_) => "pgr_",
        }
    }
}

/// Whether `id` is Cloud's id of one of the runner's three.
fn runner_provider_id(id: &str) -> bool {
    RunnerGrantProvider::ALL
        .into_iter()
        .any(|provider| provider.cloud_id() == id)
}

impl TryFrom<AgentLoginProvider> for GrantProvider {
    /// An installable provider, which has no grant token.
    type Error = AgentLoginProvider;

    /// GitHub, GitLab or Linear; an installable provider is refused as
    /// itself.
    fn try_from(provider: AgentLoginProvider) -> Result<Self, Self::Error> {
        let runner = match provider {
            AgentLoginProvider::Github => RunnerGrantProvider::Github,
            AgentLoginProvider::Gitlab => RunnerGrantProvider::Gitlab,
            AgentLoginProvider::Linear => RunnerGrantProvider::Linear,
            AgentLoginProvider::Codex | AgentLoginProvider::ClaudeCode => return Err(provider),
        };
        Ok(Self(GrantProviderKind::Runner(runner)))
    }
}

/// Whether `id` is a provider id, `^[a-z][a-z0-9.-]{0,62}$`.
fn valid_provider_id(id: &str) -> bool {
    let mut bytes = id.bytes();
    id.len() <= MAXIMUM_PROVIDER_ID_BYTES
        && bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'.' || byte == b'-'
        })
}

/// A request for the access token of the grant of an assignment the runner
/// holds, for one OAuth provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GrantTokenRequest {
    pub(crate) boot_id: String,
    pub(crate) assignment_id: String,
    pub(crate) provider: GrantProvider,
}

/// A request for the OAuth providers of an assignment the runner holds
/// (REQ-GRANT-97).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GrantProvidersRequest {
    pub(crate) boot_id: String,
    pub(crate) assignment_id: String,
}

/// One provider Cloud lists for an assignment: its id, which is a provider
/// id, and the names its token and the path of its file are to be offered
/// under, which the runner has not yet checked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListedProvider {
    pub(crate) provider: String,
    pub(crate) token: String,
    pub(crate) token_file: String,
}

/// A provider list Cloud did not answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GrantProvidersFailure {
    /// Cloud has no such route (404): it lists nothing, and is asked for
    /// the runner's three as before the list.
    NoRoute,
    /// Cloud does not count the assignment as one this boot holds (409).
    AssignmentNotCurrent,
    Cloud(CloudFailure),
}

impl From<CloudFailure> for GrantProvidersFailure {
    fn from(failure: CloudFailure) -> Self {
        Self::Cloud(failure)
    }
}

/// The access token Cloud issued from a grant (REQ-GRANT-84, REQ-GRANT-85),
/// or from an integration token for a token service (REQ-INTEGRATION-10).
#[derive(Debug)]
pub(crate) struct IssuedToken {
    /// The grant's `pgr_` ID, or the integration token's `itk_` ID for a
    /// token service.
    pub(crate) grant_id: String,
    /// The grant's generation the token belongs to, or the integration
    /// token's version, at least 1.
    pub(crate) generation: u64,
    pub(crate) access_token: GrantAccessToken,
    pub(crate) expires_at: OffsetDateTime,
    /// How long the runner waits before asking again (REQ-GRANT-85,
    /// REQ-GRANT-89).
    pub(crate) refresh_after: std::time::Duration,
}

/// A grant access token: one line of 1 to 4096 printable characters
/// without spaces, cleared when dropped and never shown by `Debug`.
pub(crate) struct GrantAccessToken(Zeroizing<Vec<u8>>);

impl GrantAccessToken {
    /// The token `bytes` hold; `None` when they are not one of its form.
    pub(crate) fn new(bytes: &[u8]) -> Option<Self> {
        super::valid_token(bytes).then(|| Self(Zeroizing::new(bytes.to_vec())))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for GrantAccessToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GrantAccessToken([redacted])")
    }
}

/// The reason Cloud names when it issues no token (REQ-GRANT-84).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum GrantTokenRefusal {
    Unauthorized,
    Revoked,
    ReconnectRequired,
    ProviderUnavailable,
    AssignmentNotCurrent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GrantTokenFailure {
    Refused(GrantTokenRefusal),
    /// Cloud has no such route, so it authorizes nothing.
    NoRoute,
    Cloud(CloudFailure),
}

impl From<CloudFailure> for GrantTokenFailure {
    fn from(failure: CloudFailure) -> Self {
        Self::Cloud(failure)
    }
}

/// The runner's report of where each provider's credential comes from for
/// an assignment of this boot (REQ-GRANT-91).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CredentialSourcesRequest {
    pub(crate) boot_id: String,
    pub(crate) assignment_id: String,
    /// Each provider that has a source, at most once.
    pub(crate) sources: Vec<ReportedSource>,
    /// Each token service Cloud listed for the assignment that has a
    /// source, in the list's order, at most once (REQ-INTEGRATION-12):
    /// reported in the same `sources` array, after the providers'.
    pub(crate) token_services: Vec<ReportedTokenService>,
}

/// One token service's source as the runner reports it
/// (REQ-INTEGRATION-12), by the id Cloud's token-service list named it by.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReportedTokenService {
    pub(crate) provider: String,
    pub(crate) source: TokenServiceSource,
}

/// Where a token service's token comes from on the runner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TokenServiceSource {
    /// The service environment carries either of its names.
    Host,
    /// The token the runner holds, issued from the integration token of
    /// this `itk_` ID.
    Token { integration_token_id: String },
}

/// One provider's source as the runner reports it: an OAuth grant names
/// the grant its token was issued from, and a coding agent's grant the
/// agent login the runner's record names, where it names one; a host
/// source names neither.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReportedSource {
    pub(crate) provider: AgentLoginProvider,
    pub(crate) source: CredentialSource,
    /// The `pgr_` ID of an OAuth provider's grant.
    pub(crate) grant_id: Option<String>,
    /// The `agl_` ID of a coding agent's agent login.
    pub(crate) agent_login_id: Option<String>,
}

/// A credential-source report Cloud did not record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CredentialSourcesFailure {
    /// Cloud has no such route, so it records no source.
    NoRoute,
    /// Cloud refused the report (409), or answered it with another status,
    /// or did not answer.
    Cloud(CloudFailure),
}

impl From<CloudFailure> for CredentialSourcesFailure {
    fn from(failure: CloudFailure) -> Self {
        Self::Cloud(failure)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliveryFailure {
    Refused(DeliveryRefusal),
    Cloud(CloudFailure),
}

impl From<CloudFailure> for DeliveryFailure {
    fn from(failure: CloudFailure) -> Self {
        Self::Cloud(failure)
    }
}

/// The requests over HTTP with the runner's current credential.
#[derive(Debug)]
pub(crate) struct HttpAgentLoginCloud {
    target: Mutex<Target>,
    timeout: Duration,
}

/// Where the requests go and the credential they carry.
#[derive(Clone, Debug)]
struct Target {
    status_endpoint: Url,
    delivery_endpoint: Url,
    grant_token_endpoint: Url,
    grant_providers_endpoint: Url,
    token_services_endpoint: Url,
    credential_sources_endpoint: Url,
    runner_credential: Credential,
}

impl Target {
    /// `None` for an endpoint that is not `ws(s)`.
    fn new(endpoint: &Url, runner_credential: &Credential) -> Option<Self> {
        Some(Self {
            status_endpoint: runner_http::private_runner_http_endpoint(endpoint, STATUS_PATH)
                .ok()?,
            delivery_endpoint: runner_http::private_runner_http_endpoint(endpoint, DELIVERY_PATH)
                .ok()?,
            grant_token_endpoint: runner_http::private_runner_http_endpoint(
                endpoint,
                GRANT_TOKEN_PATH,
            )
            .ok()?,
            grant_providers_endpoint: runner_http::private_runner_http_endpoint(
                endpoint,
                GRANT_PROVIDERS_PATH,
            )
            .ok()?,
            token_services_endpoint: runner_http::private_runner_http_endpoint(
                endpoint,
                TOKEN_SERVICES_PATH,
            )
            .ok()?,
            credential_sources_endpoint: runner_http::private_runner_http_endpoint(
                endpoint,
                CREDENTIAL_SOURCES_PATH,
            )
            .ok()?,
            runner_credential: runner_credential.clone(),
        })
    }
}

impl HttpAgentLoginCloud {
    /// The client for the runner's `ws(s)` endpoint; `None` for any other
    /// scheme.
    pub(crate) fn new(endpoint: &Url, runner_credential: &Credential) -> Option<Self> {
        Some(Self {
            target: Mutex::new(Target::new(endpoint, runner_credential)?),
            timeout: runner_http::REQUEST_TIMEOUT,
        })
    }

    fn target(&self) -> Target {
        self.target
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A provider list of the shape of `grant_providers` from `endpoint`,
    /// each id a provider id, at most once, and one `accepted` takes.
    fn list(
        &self,
        endpoint: impl Fn(&Target) -> &Url,
        request: &GrantProvidersRequest,
        accepted: impl Fn(&str) -> bool,
    ) -> Result<Vec<ListedProvider>, GrantProvidersFailure> {
        let body = GrantProvidersRequestBody {
            schema_version: 1,
            boot_id: &request.boot_id,
            assignment_id: &request.assignment_id,
        };
        let response = self.post(endpoint, &body, &[StatusCode::OK, StatusCode::CONFLICT])?;
        match response.status {
            StatusCode::OK => {}
            StatusCode::CONFLICT => {
                let refusal: GrantProvidersRefusalAnswer =
                    serde_json::from_slice(&response.body)
                        .map_err(|_| CloudFailure::InvalidResponse)?;
                if refusal.schema_version != 1 || refusal.reason != "assignment_not_current" {
                    return Err(CloudFailure::InvalidResponse.into());
                }
                return Err(GrantProvidersFailure::AssignmentNotCurrent);
            }
            StatusCode::NOT_FOUND => return Err(GrantProvidersFailure::NoRoute),
            status => return Err(CloudFailure::Status(status.as_u16()).into()),
        }
        let answer: GrantProvidersAnswer =
            serde_json::from_slice(&response.body).map_err(|_| CloudFailure::InvalidResponse)?;
        if answer.schema_version != 1 || answer.providers.len() > MAXIMUM_LISTED_PROVIDERS {
            return Err(CloudFailure::InvalidResponse.into());
        }
        let mut listed: Vec<ListedProvider> = Vec::with_capacity(answer.providers.len());
        for entry in answer.providers {
            // The names are checked where the token is offered, one entry
            // at a time.
            if !valid_provider_id(&entry.provider)
                || !accepted(&entry.provider)
                || listed.iter().any(|known| known.provider == entry.provider)
            {
                return Err(CloudFailure::InvalidResponse.into());
            }
            listed.push(ListedProvider {
                provider: entry.provider,
                token: entry.environment.token,
                token_file: entry.environment.token_file,
            });
        }
        Ok(listed)
    }

    fn post(
        &self,
        endpoint: impl Fn(&Target) -> &Url,
        body: &impl Serialize,
        json_statuses: &[StatusCode],
    ) -> Result<runner_http::Response, CloudFailure> {
        // A body that cannot be encoded is never sent.
        let body = serde_json::to_vec(body).map_err(|_| CloudFailure::Transport)?;
        let target = self.target();
        runner_http::post_json(
            endpoint(&target),
            &target.runner_credential,
            body,
            json_statuses,
            self.timeout,
            None,
        )
        .map_err(|failure| match failure {
            runner_http::Failure::Unavailable | runner_http::Failure::Cancelled => {
                CloudFailure::Transport
            }
            runner_http::Failure::InvalidResponse => CloudFailure::InvalidResponse,
        })
    }
}

impl AgentLoginCloud for HttpAgentLoginCloud {
    fn follow_credential(&self, endpoint: &Url, runner_credential: &Credential) {
        if let Some(target) = Target::new(endpoint, runner_credential) {
            *self.target.lock().unwrap_or_else(PoisonError::into_inner) = target;
        }
    }

    fn status(&self, request: &StatusRequest) -> Result<Vec<ProviderLogin>, CloudFailure> {
        let body = StatusRequestBody {
            schema_version: 1,
            os: request.os,
            providers: request
                .providers
                .iter()
                .map(|report| ProviderReportBody {
                    provider: report.provider,
                    record_schema_versions: report.provider.record_schema_versions(),
                    installed: report.installed.as_ref(),
                    state: report.state,
                    last_use: report.last_use.map(|last_use| last_use.outcome),
                    last_use_at: report.last_use.map(|last_use| last_use.at),
                })
                .collect(),
        };
        let response = self.post(|target| &target.status_endpoint, &body, &[StatusCode::OK])?;
        if response.status != StatusCode::OK {
            return Err(CloudFailure::Status(response.status.as_u16()));
        }
        let answer: StatusAnswer =
            serde_json::from_slice(&response.body).map_err(|_| CloudFailure::InvalidResponse)?;
        if answer.schema_version != 1 {
            return Err(CloudFailure::InvalidResponse);
        }
        let mut logins: Vec<ProviderLogin> = Vec::with_capacity(answer.logins.len());
        for entry in answer.logins {
            let Some(provider) = AgentLoginProvider::installable(&entry.provider) else {
                continue;
            };
            let status = match (
                entry.state.as_str(),
                entry.generation,
                entry.record_schema_version,
            ) {
                ("authorized", Some(generation), Some(record_schema_version))
                    if generation >= 1 && record_schema_version >= 1 =>
                {
                    LoginStatus::Authorized {
                        generation,
                        record_schema_version,
                    }
                }
                ("revoked", None, None) => LoginStatus::Revoked,
                ("unauthorized", None, None) => LoginStatus::Unauthorized,
                ("unsupported_target", None, None) => LoginStatus::UnsupportedTarget,
                _ => return Err(CloudFailure::InvalidResponse),
            };
            if !useful_support::valid_typed_id(&entry.agent_login_id, "agl_")
                || logins.iter().any(|login| login.provider == provider)
            {
                return Err(CloudFailure::InvalidResponse);
            }
            logins.push(ProviderLogin {
                provider,
                agent_login_id: entry.agent_login_id,
                status,
            });
        }
        Ok(logins)
    }

    fn delivery(&self, request: &DeliveryRequest) -> Result<Delivery, DeliveryFailure> {
        let body = DeliveryRequestBody {
            schema_version: 1,
            os: request.os,
            provider: request.provider,
            agent_login_id: &request.agent_login_id,
            generation: request.generation,
        };
        let response = self.post(
            |target| &target.delivery_endpoint,
            &body,
            &[StatusCode::OK, StatusCode::CONFLICT],
        )?;
        match response.status {
            StatusCode::OK => {}
            StatusCode::CONFLICT => {
                let refusal: RefusalAnswer = serde_json::from_slice(&response.body)
                    .map_err(|_| CloudFailure::InvalidResponse)?;
                if refusal.schema_version != 1 {
                    return Err(CloudFailure::InvalidResponse.into());
                }
                return Err(DeliveryFailure::Refused(refusal.reason));
            }
            status => {
                return Err(CloudFailure::Status(status.as_u16()).into());
            }
        }
        let answer: DeliveryAnswer =
            serde_json::from_slice(&response.body).map_err(|_| CloudFailure::InvalidResponse)?;
        if answer.schema_version != 1
            || answer.provider != request.provider.as_str()
            || answer.agent_login_id != request.agent_login_id
            || answer.generation != request.generation
            || answer.record_schema_version < 1
        {
            return Err(CloudFailure::InvalidResponse.into());
        }
        let material = base64::engine::general_purpose::STANDARD
            .decode(answer.material.0.as_bytes())
            .map(Zeroizing::new)
            .map_err(|_| CloudFailure::InvalidResponse)?;
        if material.is_empty() || material.len() > MAXIMUM_MATERIAL_BYTES {
            return Err(CloudFailure::InvalidResponse.into());
        }
        Ok(Delivery {
            provider: request.provider,
            agent_login_id: answer.agent_login_id,
            generation: answer.generation,
            record_schema_version: answer.record_schema_version,
            material: LoginMaterial(material),
        })
    }

    fn grant_token(&self, request: &GrantTokenRequest) -> Result<IssuedToken, GrantTokenFailure> {
        let provider = request.provider.cloud_id();
        let body = GrantTokenRequestBody {
            schema_version: 1,
            boot_id: &request.boot_id,
            assignment_id: &request.assignment_id,
            provider,
        };
        let response = self.post(
            |target| &target.grant_token_endpoint,
            &body,
            &[StatusCode::OK, StatusCode::CONFLICT],
        )?;
        match response.status {
            StatusCode::OK => {}
            StatusCode::CONFLICT => {
                let refusal: GrantTokenRefusalAnswer = serde_json::from_slice(&response.body)
                    .map_err(|_| CloudFailure::InvalidResponse)?;
                if refusal.schema_version != 1 {
                    return Err(CloudFailure::InvalidResponse.into());
                }
                return Err(GrantTokenFailure::Refused(refusal.reason));
            }
            StatusCode::NOT_FOUND => return Err(GrantTokenFailure::NoRoute),
            status => return Err(CloudFailure::Status(status.as_u16()).into()),
        }
        let answer: IssuedTokenAnswer =
            serde_json::from_slice(&response.body).map_err(|_| CloudFailure::InvalidResponse)?;
        let expires_at = OffsetDateTime::parse(&answer.expires_at, &Rfc3339)
            .map_err(|_| CloudFailure::InvalidResponse)?;
        if answer.schema_version != 1
            || answer.provider != provider
            || answer.generation < 1
            || !useful_support::valid_typed_id(
                &answer.grant_id,
                request.provider.issued_id_prefix(),
            )
        {
            return Err(CloudFailure::InvalidResponse.into());
        }
        let access_token = GrantAccessToken::new(answer.access_token.0.as_bytes())
            .ok_or(CloudFailure::InvalidResponse)?;
        Ok(IssuedToken {
            grant_id: answer.grant_id,
            generation: answer.generation,
            access_token,
            expires_at,
            refresh_after: std::time::Duration::from_secs(answer.refresh_after_seconds),
        })
    }

    fn grant_providers(
        &self,
        request: &GrantProvidersRequest,
    ) -> Result<Vec<ListedProvider>, GrantProvidersFailure> {
        self.list(|target| &target.grant_providers_endpoint, request, |_| true)
    }

    fn token_services(
        &self,
        request: &GrantProvidersRequest,
    ) -> Result<Vec<ListedProvider>, GrantProvidersFailure> {
        self.list(
            |target| &target.token_services_endpoint,
            request,
            |id| !runner_provider_id(id),
        )
    }

    fn credential_sources(
        &self,
        request: &CredentialSourcesRequest,
    ) -> Result<(), CredentialSourcesFailure> {
        let body = CredentialSourcesRequestBody {
            schema_version: 1,
            boot_id: &request.boot_id,
            assignment_id: &request.assignment_id,
            sources: request
                .sources
                .iter()
                .map(|entry| CredentialSourceBody {
                    // Cloud names an OAuth provider as its provider table
                    // does, `gitlab.com` for GitLab.
                    provider: entry
                        .provider
                        .grant_provider()
                        .unwrap_or(entry.provider.as_str()),
                    source: entry.source.as_str(),
                    grant_id: entry.grant_id.as_deref(),
                    agent_login_id: entry.agent_login_id.as_deref(),
                    integration_token_id: None,
                })
                // A token service's entry follows the providers', by the id
                // Cloud listed it by, and names no grant or agent login.
                .chain(request.token_services.iter().map(|entry| {
                    let (source, integration_token_id) = match &entry.source {
                        TokenServiceSource::Host => ("host", None),
                        TokenServiceSource::Token {
                            integration_token_id,
                        } => ("token", Some(integration_token_id.as_str())),
                    };
                    CredentialSourceBody {
                        provider: &entry.provider,
                        source,
                        grant_id: None,
                        agent_login_id: None,
                        integration_token_id,
                    }
                }))
                .collect(),
        };
        // Cloud's answer carries nothing the runner reads: a refusal's
        // reason is recorded as its status alone.
        let response = self.post(|target| &target.credential_sources_endpoint, &body, &[])?;
        match response.status {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => Err(CredentialSourcesFailure::NoRoute),
            status => Err(CloudFailure::Status(status.as_u16()).into()),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusRequestBody<'a> {
    schema_version: u32,
    os: OperatingSystem,
    providers: Vec<ProviderReportBody<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderReportBody<'a> {
    provider: AgentLoginProvider,
    record_schema_versions: &'static [u32],
    #[serde(skip_serializing_if = "Option::is_none")]
    installed: Option<&'a InstalledLogin>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<AgentLoginState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_use: Option<UseOutcome>,
    /// Stated exactly with `last_use`, in RFC 3339 as the readiness
    /// document records it.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_rfc3339"
    )]
    last_use_at: Option<OffsetDateTime>,
}

/// An RFC 3339 time; one that has no RFC 3339 form fails the body, which is
/// then never sent.
fn serialize_rfc3339<S: Serializer>(
    value: &Option<OffsetDateTime>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.serialize_str(
            &value
                .format(&Rfc3339)
                .map_err(<S::Error as serde::ser::Error>::custom)?,
        ),
        None => serializer.serialize_none(),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StatusAnswer {
    schema_version: u64,
    logins: Vec<StatusEntry>,
}

/// One entry of the status answer. The provider and state are read as text
/// so that an entry for a provider this build does not know is dropped
/// rather than refused.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StatusEntry {
    provider: String,
    state: String,
    agent_login_id: String,
    generation: Option<u64>,
    record_schema_version: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryRequestBody<'a> {
    schema_version: u32,
    os: OperatingSystem,
    provider: AgentLoginProvider,
    agent_login_id: &'a str,
    generation: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeliveryAnswer {
    schema_version: u64,
    provider: String,
    agent_login_id: String,
    generation: u64,
    record_schema_version: u32,
    material: SensitiveString,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RefusalAnswer {
    schema_version: u64,
    reason: DeliveryRefusal,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GrantTokenRequestBody<'a> {
    schema_version: u32,
    boot_id: &'a str,
    assignment_id: &'a str,
    provider: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GrantProvidersRequestBody<'a> {
    schema_version: u32,
    boot_id: &'a str,
    assignment_id: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantProvidersAnswer {
    schema_version: u64,
    providers: Vec<GrantProvidersEntry>,
}

/// One listed provider. Its id is read as text so the client can refuse an
/// id that is not a provider id.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantProvidersEntry {
    provider: String,
    environment: GrantProvidersEnvironment,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantProvidersEnvironment {
    token: String,
    token_file: String,
}

/// Cloud's one refusal of a provider list; its reason is read as text and
/// compared, since `assignment_not_current` is the only one.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantProvidersRefusalAnswer {
    schema_version: u64,
    reason: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialSourcesRequestBody<'a> {
    schema_version: u32,
    boot_id: &'a str,
    assignment_id: &'a str,
    sources: Vec<CredentialSourceBody<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialSourceBody<'a> {
    provider: &'a str,
    source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    grant_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_login_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    integration_token_id: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IssuedTokenAnswer {
    schema_version: u64,
    provider: String,
    grant_id: String,
    generation: u64,
    access_token: SensitiveString,
    expires_at: String,
    /// The wait before asking again (REQ-GRANT-85), a whole number of
    /// seconds.
    refresh_after_seconds: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantTokenRefusalAnswer {
    schema_version: u64,
    reason: GrantTokenRefusal,
}

#[cfg(test)]
mod tests;
