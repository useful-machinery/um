//! The runner's two agent-login requests to Cloud: its status and one
//! delivery. Both are POSTs on the HTTP base of the runner's endpoint,
//! authenticated by the enrolled runner credential, and name neither a boot
//! nor an assignment: an agent login is a standing install bound to the
//! runner alone.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use base64::Engine as _;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use url::Url;
use zeroize::Zeroizing;

use super::readiness::UseOutcome;
use super::{FailureCause, OperatingSystem};
use crate::control_protocol::{AgentLoginProvider, AgentLoginState};
use crate::credential::Credential;
use crate::runner_http::{self, SensitiveString};

const STATUS_PATH: &str = "/v1/runner/agent-logins/status";
const DELIVERY_PATH: &str = "/v1/runner/agent-logins/delivery";
/// Cloud accepts no larger material for any provider.
const MAXIMUM_MATERIAL_BYTES: usize = 32 * 1024;

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
/// readiness and how the provider answered the installed login's last use,
/// where there are any.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderReport {
    pub(crate) provider: AgentLoginProvider,
    pub(crate) installed: Option<InstalledLogin>,
    pub(crate) state: Option<AgentLoginState>,
    pub(crate) last_use: Option<UseOutcome>,
}

impl ProviderReport {
    /// The report of `provider`, stating `last_use` only with `state`
    /// `ready` and an install, the only report Cloud takes it with
    /// (REQ-GRANT-25, REQ-GRANT-52).
    pub(crate) fn new(
        provider: AgentLoginProvider,
        installed: Option<InstalledLogin>,
        state: Option<AgentLoginState>,
        last_use: Option<UseOutcome>,
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

/// The two requests over HTTP with the runner's current credential.
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
                    last_use: report.last_use,
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
            if !um_support::valid_typed_id(&entry.agent_login_id, "agl_")
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

#[cfg(test)]
mod tests;
