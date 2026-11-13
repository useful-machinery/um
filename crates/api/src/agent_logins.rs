//! Agent-login operations for the signed-in member over the generated principals client.
//!
//! Responses decode leniently into the generated models, which name no material, so nothing
//! a response wrongly adds can reach a caller. Material travels only in a request body built
//! here from zeroizing storage, and a generated error is never formatted: its body is
//! untrusted text.
use std::fmt;

use base64::Engine as _;
use reqwest::StatusCode;
use zeroize::{Zeroize as _, Zeroizing};

use super::generated::{apis, models};
use super::http_client::zeroize_generated_bearer_access_token;
use super::resource::{
    ProblemFailure, ProblemRow, ResourceFailure, classify_problem_error, confirmed_delete,
    expect_problem_status, mutation_request, receive_raw, resource_configuration,
    validate_json_media_type, validate_mutation_key,
};
use super::{HttpTransportPolicy, UnreachableCategory, problem};

pub type AgentLogin = models::AgentLogin;
pub type AgentLoginList = models::AgentLoginList;
pub type AgentLoginAuthorization = models::AgentLoginAuthorization;
pub type AgentLoginState = models::agent_login::State;
pub type AgentLoginReportState = models::agent_login_report::State;
pub type AgentLoginProvider = models::agent_login::Provider;
pub type AgentLoginImportProvider = models::import_agent_login_request::Provider;
// Reached through `AgentLogin`'s fields; a caller reads their states by `as_str`.
type AgentLoginEndedAuthorization = models::AgentLoginEndedAuthorization;
type AgentLoginRemoval = models::AgentLoginRemoval;
type AgentLoginRemovalState = models::agent_login_removal::State;
type AgentLoginReport = models::AgentLoginReport;
type AgentLoginReportLastUse = models::agent_login_report::LastUse;
type AgentLoginStop = models::AgentLoginStop;

impl AgentLoginRemovalState {
    /// The removal state's wire name (REQ-AGENTLOGIN-37).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AgentLoginRemovalStateNotDelivered => "not_delivered",
            Self::AgentLoginRemovalStatePending => "pending",
            Self::AgentLoginRemovalStateConfirmed => "confirmed",
        }
    }
}

impl AgentLoginReportLastUse {
    /// The last use's wire name (REQ-AGENTLOGIN-17).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AgentLoginReportLastUseAccepted => "accepted",
            Self::AgentLoginReportLastUseRefused => "refused",
        }
    }
}

/// The schema version of both providers' material form (REQ-AGENTLOGIN-2).
pub const AGENT_LOGIN_RECORD_SCHEMA_VERSION: i32 = 1;

const AGENT_LOGIN_NOT_FOUND: &str =
    "https://api.usefulmachinery.com/problems/agent-login-not-found";
const MATERIAL_INVALID: &str =
    "https://api.usefulmachinery.com/problems/agent-login-material-invalid";
const RUNNER_NOT_FOUND: &str =
    "https://api.usefulmachinery.com/problems/agent-login-runner-not-found";
const RUNNER_CONFLICT: &str =
    "https://api.usefulmachinery.com/problems/agent-login-runner-conflict";
const ALREADY_AUTHORIZED: &str =
    "https://api.usefulmachinery.com/problems/agent-login-already-authorized";
const AUTHORIZATION_NOT_FOUND: &str =
    "https://api.usefulmachinery.com/problems/agent-login-authorization-not-found";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgentLoginFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    IdempotencyConflict,
    ProviderUnavailable,
    AgentLoginNotFound,
    OrganizationNotFound,
    MaterialInvalid,
    LimitReached,
    RunnerNotFound,
    RunnerHoldsLogin,
    LoginAlreadyAuthorized,
    AuthorizationNotFound,
    RateLimited { retry_after: Option<u64> },
    Unreachable { category: UnreachableCategory },
    InvalidResponse { credential_rejected: bool },
}

impl AgentLoginFailure {
    pub const fn credential_rejected(&self) -> bool {
        matches!(
            self,
            Self::Unauthenticated
                | Self::InvalidResponse {
                    credential_rejected: true
                }
        )
    }
}

impl From<ResourceFailure> for AgentLoginFailure {
    fn from(failure: ResourceFailure) -> Self {
        failure.into_problem_failure()
    }
}

impl ProblemFailure for AgentLoginFailure {
    fn rate_limited(retry_after: Option<u64>) -> Self {
        Self::RateLimited { retry_after }
    }

    fn unreachable(category: UnreachableCategory) -> Self {
        Self::Unreachable { category }
    }

    fn invalid_response(credential_rejected: bool) -> Self {
        Self::InvalidResponse {
            credential_rejected,
        }
    }
}

const fn invalid() -> AgentLoginFailure {
    AgentLoginFailure::InvalidResponse {
        credential_rejected: false,
    }
}

/// The operation a response answers. Only an import and an organization lookup name an
/// organization, so only their generic `not-found` means that organization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    NamesOrganization,
    Other,
}

/// The problems every agent-login operation answers, by status and type. Anything else is
/// outside the contract.
const PROBLEMS: [ProblemRow<AgentLoginFailure>; 13] = [
    (
        StatusCode::BAD_REQUEST,
        problem::BAD_REQUEST,
        AgentLoginFailure::InvalidInput,
    ),
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        problem::REQUEST_BODY_TOO_LARGE,
        AgentLoginFailure::InvalidInput,
    ),
    (
        StatusCode::UNAUTHORIZED,
        problem::UNAUTHORIZED,
        AgentLoginFailure::Unauthenticated,
    ),
    (
        StatusCode::FORBIDDEN,
        problem::FORBIDDEN,
        AgentLoginFailure::Forbidden,
    ),
    (
        StatusCode::CONFLICT,
        problem::IDEMPOTENCY_CONFLICT,
        AgentLoginFailure::IdempotencyConflict,
    ),
    (
        StatusCode::SERVICE_UNAVAILABLE,
        problem::PROVIDER_UNAVAILABLE,
        AgentLoginFailure::ProviderUnavailable,
    ),
    (
        StatusCode::NOT_FOUND,
        AGENT_LOGIN_NOT_FOUND,
        AgentLoginFailure::AgentLoginNotFound,
    ),
    (
        StatusCode::BAD_REQUEST,
        MATERIAL_INVALID,
        AgentLoginFailure::MaterialInvalid,
    ),
    (
        StatusCode::CONFLICT,
        problem::QUANTITY_LIMIT_REACHED,
        AgentLoginFailure::LimitReached,
    ),
    (
        StatusCode::NOT_FOUND,
        RUNNER_NOT_FOUND,
        AgentLoginFailure::RunnerNotFound,
    ),
    (
        StatusCode::CONFLICT,
        RUNNER_CONFLICT,
        AgentLoginFailure::RunnerHoldsLogin,
    ),
    (
        StatusCode::CONFLICT,
        ALREADY_AUTHORIZED,
        AgentLoginFailure::LoginAlreadyAuthorized,
    ),
    (
        StatusCode::NOT_FOUND,
        AUTHORIZATION_NOT_FOUND,
        AgentLoginFailure::AuthorizationNotFound,
    ),
];

/// The problems only an operation that names an organization answers.
const ORGANIZATION_PROBLEMS: [ProblemRow<AgentLoginFailure>; 1] = [(
    StatusCode::NOT_FOUND,
    problem::NOT_FOUND,
    AgentLoginFailure::OrganizationNotFound,
)];

impl Operation {
    fn problems(self) -> impl Iterator<Item = &'static ProblemRow<AgentLoginFailure>> {
        let scoped: &'static [ProblemRow<AgentLoginFailure>] = match self {
            Self::NamesOrganization => &ORGANIZATION_PROBLEMS,
            Self::Other => &[],
        };
        PROBLEMS.iter().chain(scoped)
    }
}

fn classify_error<E>(operation: Operation, error: apis::Error<E>) -> AgentLoginFailure {
    classify_problem_error(error, operation.problems())
}

/// The login a member imports: the provider's own material, sent byte for byte as base64.
pub struct AgentLoginImport<'a> {
    pub organization: &'a str,
    pub provider: AgentLoginImportProvider,
    pub label: Option<&'a str>,
    pub material: &'a [u8],
}

pub struct AgentLoginApi {
    configuration: apis::configuration::Configuration,
}

impl AgentLoginApi {
    pub fn new(
        api_url: &str,
        token: &str,
        policy: HttpTransportPolicy,
    ) -> Result<Self, AgentLoginApiError> {
        let configuration = resource_configuration(
            api_url,
            token,
            policy,
            AgentLoginApiError::InvalidEndpoint,
            AgentLoginApiError::BuildClient,
        )?;
        Ok(Self { configuration })
    }

    fn endpoint(&self, segments: &[&str]) -> String {
        let mut endpoint = format!(
            "{}/v1/me/agent-logins",
            self.configuration.base_path.trim_end_matches('/')
        );
        for segment in segments {
            endpoint.push('/');
            endpoint.push_str(&apis::urlencode(segment));
        }
        endpoint
    }

    pub fn list(
        &self,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<AgentLoginList, AgentLoginFailure> {
        let page = apis::principals_api::list_agent_logins(&self.configuration, limit, cursor)
            .map_err(|error| classify_error(Operation::Other, error))?;
        if !page.items.iter().all(valid_agent_login) {
            return Err(invalid());
        }
        Ok(page)
    }

    pub fn get(&self, agent_login_id: &str) -> Result<AgentLogin, AgentLoginFailure> {
        let login = apis::principals_api::get_agent_login(&self.configuration, agent_login_id)
            .map_err(|error| classify_error(Operation::Other, error))?;
        if login.id != agent_login_id || !valid_agent_login(&login) {
            return Err(invalid());
        }
        Ok(login)
    }

    /// The ID of the organization an exact slug names, among the member's organizations.
    pub fn organization_id(&self, slug: &str) -> Result<String, AgentLoginFailure> {
        let organization = apis::organizations_api::get_organization(&self.configuration, slug)
            .map_err(|error| classify_error(Operation::NamesOrganization, error))?;
        if organization.slug != slug || !um_support::valid_typed_id(&organization.id, "org_") {
            return Err(invalid());
        }
        Ok(organization.id)
    }

    /// Stores a new agent login at generation 1 (REQ-AGENTLOGIN-1).
    pub fn import(
        &self,
        login: &AgentLoginImport<'_>,
        key: &str,
    ) -> Result<AgentLogin, AgentLoginFailure> {
        let material = encode_material(login.material);
        let mut model = models::ImportAgentLoginRequest::new(
            login.organization.to_owned(),
            login.provider,
            AGENT_LOGIN_RECORD_SCHEMA_VERSION,
            material.to_string(),
        );
        model.label = login.label.map(str::to_owned);
        let body = serialize_body(&model);
        model.material.zeroize();
        let created: AgentLogin = self.send_json(
            Operation::NamesOrganization,
            reqwest::Method::POST,
            &self.endpoint(&[]),
            key,
            body?,
            StatusCode::CREATED,
        )?;
        if um_support::wire_name(created.provider) != um_support::wire_name(login.provider)
            || !valid_agent_login(&created)
        {
            return Err(invalid());
        }
        Ok(created)
    }

    /// Stores new material as the agent login's next generation (REQ-AGENTLOGIN-6).
    pub fn replace(
        &self,
        agent_login_id: &str,
        material: &[u8],
        key: &str,
    ) -> Result<AgentLogin, AgentLoginFailure> {
        let material = encode_material(material);
        let mut model = models::ReplaceAgentLoginMaterialRequest::new(
            AGENT_LOGIN_RECORD_SCHEMA_VERSION,
            material.to_string(),
        );
        let body = serialize_body(&model);
        model.material.zeroize();
        let replaced: AgentLogin = self.send_json(
            Operation::Other,
            reqwest::Method::POST,
            &self.endpoint(&[agent_login_id, "generations"]),
            key,
            body?,
            StatusCode::OK,
        )?;
        if replaced.id != agent_login_id || !valid_agent_login(&replaced) {
            return Err(invalid());
        }
        Ok(replaced)
    }

    /// Revokes the agent login (REQ-AGENTLOGIN-7).
    pub fn revoke(&self, agent_login_id: &str, key: &str) -> Result<(), AgentLoginFailure> {
        self.send_empty(&self.endpoint(&[agent_login_id]), key)
    }

    /// Authorizes one runner to receive the agent login (REQ-AGENTLOGIN-8).
    pub fn authorize(
        &self,
        agent_login_id: &str,
        runner_id: &str,
        key: &str,
    ) -> Result<AgentLoginAuthorization, AgentLoginFailure> {
        let body = serialize_body(&models::AuthorizeAgentLoginRunnerRequest::new(
            runner_id.to_owned(),
        ))?;
        let authorization: AgentLoginAuthorization = self.send_json(
            Operation::Other,
            reqwest::Method::POST,
            &self.endpoint(&[agent_login_id, "authorizations"]),
            key,
            body,
            StatusCode::CREATED,
        )?;
        if authorization.runner_id != runner_id || !valid_authorization(&authorization) {
            return Err(invalid());
        }
        Ok(authorization)
    }

    /// Ends one authorization of the agent login (REQ-AGENTLOGIN-9).
    pub fn deauthorize(
        &self,
        agent_login_id: &str,
        authorization_id: &str,
        key: &str,
    ) -> Result<(), AgentLoginFailure> {
        self.send_empty(
            &self.endpoint(&[agent_login_id, "authorizations", authorization_id]),
            key,
        )
    }

    // The contract's answer to each mutation echoes this request's key; the generated client
    // drops response headers, so these requests go out under its configured client instead.
    fn send_json<T: serde::de::DeserializeOwned>(
        &self,
        operation: Operation,
        method: reqwest::Method,
        endpoint: &str,
        key: &str,
        body: Zeroizing<Vec<u8>>,
        expected: StatusCode,
    ) -> Result<T, AgentLoginFailure> {
        let response = receive_raw(
            mutation_request(&self.configuration, method, endpoint, key)
                .header(reqwest::header::CONTENT_TYPE, problem::JSON_MEDIA_TYPE)
                .body(body.to_vec()),
        )?;
        let response = expect_problem_status(response, expected, operation.problems())?;
        validate_mutation_key(&response.headers, key)?;
        validate_json_media_type(&response.headers)?;
        serde_json::from_slice(&response.body).map_err(|_| invalid())
    }

    fn send_empty(&self, endpoint: &str, key: &str) -> Result<(), AgentLoginFailure> {
        confirmed_delete(
            &self.configuration,
            endpoint,
            key,
            Operation::Other.problems(),
        )
    }
}

impl Drop for AgentLoginApi {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
    }
}

fn encode_material(material: &[u8]) -> Zeroizing<String> {
    let mut encoded = Zeroizing::new(String::new());
    base64::engine::general_purpose::STANDARD.encode_string(material, &mut encoded);
    encoded
}

fn serialize_body(value: &impl serde::Serialize) -> Result<Zeroizing<Vec<u8>>, AgentLoginFailure> {
    // Serializing a generated model of strings and integers cannot fail; a failure is still
    // reported rather than sent.
    serde_json::to_vec(value)
        .map(Zeroizing::new)
        .map_err(|_| invalid())
}

fn valid_agent_login(value: &AgentLogin) -> bool {
    um_support::valid_typed_id(&value.id, "agl_")
        && um_support::valid_typed_id(&value.organization_id, "org_")
        && value.generation >= 1
        && value.record_schema_version >= 1
        && value.key_version >= 1
        && !value.created_at.is_empty()
        && !value.imported_at.is_empty()
        && value
            .revoked_at
            .as_deref()
            .is_none_or(|time| !time.is_empty())
        && value.authorizations.iter().all(valid_authorization)
        && value
            .ended_authorizations
            .iter()
            .flatten()
            .all(valid_ended_authorization)
}

fn valid_authorization(value: &AgentLoginAuthorization) -> bool {
    valid_authorization_record(
        &value.id,
        &value.runner_id,
        &value.created_at,
        value.delivery.as_deref(),
        value.report.as_deref(),
    )
}

fn valid_ended_authorization(value: &AgentLoginEndedAuthorization) -> bool {
    valid_authorization_record(
        &value.id,
        &value.runner_id,
        &value.created_at,
        value.delivery.as_deref(),
        value.report.as_deref(),
    ) && !value.ended_at.is_empty()
        && valid_removal(&value.removal)
        && value.stop.as_deref().is_none_or(valid_stop)
}

// A stop names the run asked to stop and the cancellation request made of it
// (REQ-AGENTLOGIN-38).
fn valid_stop(value: &AgentLoginStop) -> bool {
    um_support::valid_typed_id(&value.run_id, "run_")
        && um_support::valid_typed_id(&value.cancellation_request_id, "cmd_")
        && !value.requested_at.is_empty()
}

fn valid_authorization_record(
    id: &str,
    runner_id: &str,
    created_at: &str,
    delivery: Option<&models::AgentLoginDelivery>,
    report: Option<&AgentLoginReport>,
) -> bool {
    um_support::valid_typed_id(id, "aga_")
        && um_support::valid_typed_id(runner_id, "rnr_")
        && !created_at.is_empty()
        && delivery
            .is_none_or(|delivery| delivery.generation >= 1 && !delivery.delivered_at.is_empty())
        && report.is_none_or(|report| {
            report.generation.is_none_or(|generation| generation >= 1)
                && !report.reported_at.is_empty()
        })
}

// A removal carries its confirmation time exactly when it is confirmed.
fn valid_removal(value: &AgentLoginRemoval) -> bool {
    match (value.state, value.confirmed_at.as_deref()) {
        (AgentLoginRemovalState::AgentLoginRemovalStateConfirmed, Some(time)) => !time.is_empty(),
        (AgentLoginRemovalState::AgentLoginRemovalStateConfirmed, None) | (_, Some(_)) => false,
        (_, None) => true,
    }
}

#[derive(Debug)]
pub enum AgentLoginApiError {
    InvalidEndpoint,
    BuildClient(reqwest::Error),
}

impl fmt::Display for AgentLoginApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                f,
                "invalid agent login API endpoint; check the deployment URL and HTTP policy"
            ),
            Self::BuildClient(error) => write!(f, "prepare agent login networking: {error}"),
        }
    }
}

impl std::error::Error for AgentLoginApiError {}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "agent login client unit tests use Rust test assertions"
)]
mod tests {
    use super::*;
    use crate::resource::{classify_problem, problem_fixture};

    // REQ-AGENTLOGIN-21
    #[test]
    fn each_refusal_maps_to_one_outcome_and_a_generic_not_found_only_for_an_organization() {
        use AgentLoginFailure as F;
        for (status, slug, operation, want) in [
            (
                503,
                "provider-unavailable",
                Operation::Other,
                F::ProviderUnavailable,
            ),
            (
                404,
                "agent-login-not-found",
                Operation::Other,
                F::AgentLoginNotFound,
            ),
            (
                404,
                "not-found",
                Operation::NamesOrganization,
                F::OrganizationNotFound,
            ),
            (
                400,
                "agent-login-material-invalid",
                Operation::NamesOrganization,
                F::MaterialInvalid,
            ),
            (
                409,
                "quantity-limit-reached",
                Operation::NamesOrganization,
                F::LimitReached,
            ),
            (
                404,
                "agent-login-runner-not-found",
                Operation::Other,
                F::RunnerNotFound,
            ),
            (
                409,
                "agent-login-runner-conflict",
                Operation::Other,
                F::RunnerHoldsLogin,
            ),
            (
                409,
                "agent-login-already-authorized",
                Operation::Other,
                F::LoginAlreadyAuthorized,
            ),
            (
                404,
                "agent-login-authorization-not-found",
                Operation::Other,
                F::AuthorizationNotFound,
            ),
            (
                409,
                "idempotency-conflict",
                Operation::Other,
                F::IdempotencyConflict,
            ),
            (404, "not-found", Operation::Other, invalid()),
            (409, "agent-login-not-found", Operation::Other, invalid()),
        ] {
            let status = StatusCode::from_u16(status).unwrap();
            assert_eq!(
                classify_problem(
                    status,
                    &problem_fixture(status.as_u16(), slug),
                    None,
                    operation.problems()
                ),
                want,
                "{status} {slug}"
            );
        }
        assert_eq!(
            classify_problem(
                StatusCode::TOO_MANY_REQUESTS,
                &problem_fixture(429, "rate-limit-exceeded"),
                Some(3),
                Operation::Other.problems()
            ),
            F::RateLimited {
                retry_after: Some(3)
            }
        );
    }

    fn login_with_ended(ended: serde_json::Value) -> AgentLogin {
        serde_json::from_value(serde_json::json!({
            "id": "agl_01k0z6r1w8f4jy2m7q9v3x5abc",
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "provider": "codex",
            "state": "revoked",
            "generation": 1,
            "recordSchemaVersion": 1,
            "keyVersion": 1,
            "createdAt": "2026-11-01T09:00:00Z",
            "importedAt": "2026-11-01T09:00:00Z",
            "revokedAt": "2026-11-02T09:00:00Z",
            "authorizations": [],
            "endedAuthorizations": [ended]
        }))
        .unwrap()
    }

    fn ended(runner_id: &str, ended_at: &str, removal: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": runner_id,
            "createdAt": "2026-11-01T09:05:00Z",
            "endedAt": ended_at,
            "delivery": {"generation": 1, "deliveredAt": "2026-11-01T09:06:00Z"},
            "report": {"state": "ready", "generation": 1, "lastUse": "refused", "reportedAt": "2026-11-01T09:07:00Z"},
            "removal": removal
        })
    }

    // REQ-AGENTLOGIN-3
    // REQ-AGENTLOGIN-37
    #[test]
    fn an_ended_authorization_is_checked_and_a_removal_time_comes_only_with_confirmed() {
        const RUNNER: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
        const ENDED_AT: &str = "2026-11-02T09:00:00Z";
        let confirmed =
            serde_json::json!({"state": "confirmed", "confirmedAt": "2026-11-02T09:01:00Z"});
        for (ended, valid) in [
            (
                ended(RUNNER, ENDED_AT, serde_json::json!({"state": "pending"})),
                true,
            ),
            (
                ended(
                    RUNNER,
                    ENDED_AT,
                    serde_json::json!({"state": "not_delivered"}),
                ),
                true,
            ),
            (ended(RUNNER, ENDED_AT, confirmed.clone()), true),
            (
                ended(RUNNER, ENDED_AT, serde_json::json!({"state": "confirmed"})),
                false,
            ),
            (
                ended(
                    RUNNER,
                    ENDED_AT,
                    serde_json::json!({"state": "pending", "confirmedAt": "2026-11-02T09:01:00Z"}),
                ),
                false,
            ),
            (ended("builder", ENDED_AT, confirmed.clone()), false),
            (ended(RUNNER, "", confirmed), false),
        ] {
            assert_eq!(
                valid_agent_login(&login_with_ended(ended.clone())),
                valid,
                "{ended}"
            );
        }
    }

    // REQ-AGENTLOGIN-3
    // REQ-AGENTLOGIN-38
    #[test]
    fn an_ended_authorizations_stop_names_a_run_and_a_cancellation_request() {
        const RUN: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
        const REQUEST: &str = "cmd_01k0z6r1w8f4jy2m7q9v3x5abc";
        const REQUESTED_AT: &str = "2026-11-02T09:00:01Z";
        for (run_id, request_id, requested_at, valid) in [
            (RUN, REQUEST, REQUESTED_AT, true),
            (REQUEST, REQUEST, REQUESTED_AT, false),
            (RUN, RUN, REQUESTED_AT, false),
            (RUN, REQUEST, "", false),
        ] {
            let mut ended = ended(
                "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
                "2026-11-02T09:00:00Z",
                serde_json::json!({"state": "pending"}),
            );
            ended["stop"] = serde_json::json!({
                "runId": run_id,
                "cancellationRequestId": request_id,
                "requestedAt": requested_at
            });
            assert_eq!(
                valid_agent_login(&login_with_ended(ended.clone())),
                valid,
                "{ended}"
            );
        }
    }
}
