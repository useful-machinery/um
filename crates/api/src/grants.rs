//! Provider grant operations for the signed-in member over the generated principals client.
//!
//! Responses decode leniently: an added server member never breaks the CLI, and nothing
//! the models do not name (grant material above all) can reach a caller.
use std::fmt;

use reqwest::{StatusCode, Url};

use super::generated::{apis, models};
use super::http_client::zeroize_generated_bearer_access_token;
use super::resource::{
    ResourceFailure, mutation_request, receive_raw, resource_configuration,
    validate_json_media_type, validate_mutation_key,
};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error, problem};

pub type Grant = models::ProviderGrant;
pub type GrantList = models::ProviderGrantList;
pub type GrantUse = models::ProviderGrantUse;
pub type GrantUseList = models::ProviderGrantUseList;
pub type GrantSession = models::ProviderGrantSession;
pub type GrantProvider = models::create_grant_session_request::Provider;
pub type GrantState = models::provider_grant::State;
pub type GrantSessionStatus = models::provider_grant_session::Status;
pub type GrantSessionReason = models::provider_grant_session::ReasonCode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrantFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    IdempotencyConflict,
    ProviderUnavailable,
    RateLimited { retry_after: Option<u64> },
    Unreachable { category: UnreachableCategory },
    InvalidResponse { credential_rejected: bool },
}

impl GrantFailure {
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

impl From<ResourceFailure> for GrantFailure {
    // `receive_raw` fails only as unreachable or as an invalid response.
    fn from(failure: ResourceFailure) -> Self {
        match failure {
            ResourceFailure::Unreachable { category } => Self::Unreachable { category },
            ResourceFailure::InvalidResponse {
                credential_rejected,
            } => Self::InvalidResponse {
                credential_rejected,
            },
            _ => invalid(),
        }
    }
}

const fn invalid() -> GrantFailure {
    GrantFailure::InvalidResponse {
        credential_rejected: false,
    }
}

/// The problems a grant operation answers, by status and type. Anything else is outside
/// the contract.
const PROBLEMS: [(StatusCode, &str, GrantFailure); 9] = [
    (
        StatusCode::BAD_REQUEST,
        problem::BAD_REQUEST,
        GrantFailure::InvalidInput,
    ),
    (
        StatusCode::UNAUTHORIZED,
        problem::UNAUTHORIZED,
        GrantFailure::Unauthenticated,
    ),
    (
        StatusCode::FORBIDDEN,
        problem::FORBIDDEN,
        GrantFailure::Forbidden,
    ),
    (
        StatusCode::NOT_FOUND,
        problem::NOT_FOUND,
        GrantFailure::NotFound,
    ),
    (
        StatusCode::NOT_FOUND,
        problem::IDENTITY_NOT_FOUND,
        GrantFailure::NotFound,
    ),
    (
        StatusCode::NOT_FOUND,
        problem::GRANT_NOT_FOUND,
        GrantFailure::NotFound,
    ),
    (
        StatusCode::NOT_FOUND,
        problem::GRANT_SESSION_NOT_FOUND,
        GrantFailure::NotFound,
    ),
    (
        StatusCode::CONFLICT,
        problem::IDEMPOTENCY_CONFLICT,
        GrantFailure::IdempotencyConflict,
    ),
    (
        StatusCode::SERVICE_UNAVAILABLE,
        problem::PROVIDER_UNAVAILABLE,
        GrantFailure::ProviderUnavailable,
    ),
];

fn classify(status: StatusCode, body: &[u8], retry_after: Option<u64>) -> GrantFailure {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return GrantFailure::RateLimited { retry_after };
    }
    let kind = problem::decode(body, status)
        .ok()
        .map(|problem| problem.r#type);
    if let Some((_, _, failure)) = PROBLEMS
        .iter()
        .find(|(expected, slug, _)| *expected == status && kind.as_deref() == Some(*slug))
    {
        return failure.clone();
    }
    if status.is_server_error() {
        return GrantFailure::Unreachable {
            category: UnreachableCategory::Server,
        };
    }
    GrantFailure::InvalidResponse {
        credential_rejected: status == StatusCode::UNAUTHORIZED && kind.is_none(),
    }
}

fn classify_error<E>(error: apis::Error<E>) -> GrantFailure {
    match error {
        apis::Error::Reqwest(error) => GrantFailure::Unreachable {
            category: classify_reqwest_error(&error),
        },
        apis::Error::ResponseError(response) => {
            classify(response.status, response.content.as_bytes(), None)
        }
        apis::Error::Serde(_) | apis::Error::Io(_) => invalid(),
    }
}

pub struct GrantApi {
    configuration: apis::configuration::Configuration,
}

impl GrantApi {
    pub fn new(
        api_url: &str,
        token: &str,
        policy: HttpTransportPolicy,
    ) -> Result<Self, GrantApiError> {
        let configuration = resource_configuration(
            api_url,
            token,
            policy,
            GrantApiError::InvalidEndpoint,
            GrantApiError::BuildClient,
        )?;
        Ok(Self { configuration })
    }

    /// Starts a consent that creates the identity link and the grant together.
    pub fn start(&self, provider: GrantProvider, key: &str) -> Result<GrantSession, GrantFailure> {
        let session = apis::principals_api::create_grant_session(
            &self.configuration,
            key,
            models::CreateGrantSessionRequest::new(provider),
        )
        .map_err(classify_error)?;
        let session = validate_session(session, None)?;
        if um_support::wire_name(session.provider) != um_support::wire_name(provider) {
            return Err(invalid());
        }
        Ok(session)
    }

    /// Starts a consent at the provider of one of the member's own identity links.
    pub fn start_for_identity(
        &self,
        identity_id: &str,
        key: &str,
    ) -> Result<GrantSession, GrantFailure> {
        let session = apis::principals_api::create_identity_grant_session(
            &self.configuration,
            identity_id,
            key,
        )
        .map_err(classify_error)?;
        let session = validate_session(session, None)?;
        if session.identity_id.as_deref() != Some(identity_id) {
            return Err(invalid());
        }
        Ok(session)
    }

    // The generated GET discards response headers on errors. Observe the same generated
    // model through its configured client so Retry-After survives rate-limited polling.
    pub fn session(&self, id: &str) -> Result<GrantSession, GrantFailure> {
        let endpoint = format!(
            "{}/v1/me/grant-sessions/{}",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(id)
        );
        let response = receive_raw(super::generated_api_request(
            &self.configuration,
            reqwest::Method::GET,
            &endpoint,
        ))?;
        if response.status != StatusCode::OK {
            return Err(classify(
                response.status,
                &response.body,
                response.retry_after,
            ));
        }
        validate_json_media_type(&response.headers)?;
        let session = serde_json::from_slice(&response.body).map_err(|_| invalid())?;
        validate_session(session, Some(id))
    }

    pub fn list(
        &self,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<GrantList, GrantFailure> {
        let page = apis::principals_api::list_grants(&self.configuration, limit, cursor)
            .map_err(classify_error)?;
        if !page.items.iter().all(valid_grant) {
            return Err(invalid());
        }
        Ok(page)
    }

    pub fn uses(
        &self,
        grant_id: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<GrantUseList, GrantFailure> {
        let page =
            apis::principals_api::list_grant_uses(&self.configuration, grant_id, limit, cursor)
                .map_err(classify_error)?;
        if !page.items.iter().all(|item| valid_use(item, grant_id)) {
            return Err(invalid());
        }
        Ok(page)
    }

    // The generated DELETE accepts any 2xx and drops its headers; the contract's answer is
    // 204 echoing this request's key.
    pub fn revoke(&self, grant_id: &str, key: &str) -> Result<(), GrantFailure> {
        let endpoint = format!(
            "{}/v1/me/grants/{}",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(grant_id)
        );
        let response = receive_raw(mutation_request(
            &self.configuration,
            reqwest::Method::DELETE,
            &endpoint,
            key,
        ))?;
        if response.status != StatusCode::NO_CONTENT {
            return Err(classify(
                response.status,
                &response.body,
                response.retry_after,
            ));
        }
        validate_mutation_key(&response.headers, key)?;
        Ok(())
    }
}

impl Drop for GrantApi {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
    }
}

fn valid_grant(value: &Grant) -> bool {
    um_support::valid_typed_id(&value.id, "pgr_")
        && um_support::valid_typed_id(&value.identity_id, "idn_")
        && !value.created_at.is_empty()
        && [
            &value.last_used_at,
            &value.access_expires_at,
            &value.refresh_expires_at,
        ]
        .iter()
        .all(|time| time.as_deref().is_none_or(|time| !time.is_empty()))
}

fn valid_use(value: &GrantUse, grant_id: &str) -> bool {
    um_support::valid_typed_id(&value.id, "pgu_")
        && value.grant_id == grant_id
        && !value.run_id.is_empty()
        && !value.assignment_id.is_empty()
        && !value.organization_id.is_empty()
        && !value.issued_at.is_empty()
}

fn validate_session(value: GrantSession, id: Option<&str>) -> Result<GrantSession, GrantFailure> {
    let pending = value.status == GrantSessionStatus::ProviderGrantSessionStatusPending;
    let completed = value.status == GrantSessionStatus::ProviderGrantSessionStatusCompleted;
    if !um_support::valid_typed_id(&value.id, "pgs_")
        || id.is_some_and(|id| value.id != id)
        || value.created_at.is_empty()
        || value.expires_at.is_empty()
        || (!pending && value.authorization_url.is_some())
        || (!completed && value.result_grant.is_some())
        || value
            .authorization_url
            .as_deref()
            .is_some_and(|url| !valid_authorization_url(url))
        || value
            .result_grant
            .as_deref()
            .is_some_and(|grant| !valid_grant(grant))
    {
        return Err(invalid());
    }
    Ok(value)
}

fn valid_authorization_url(url: &str) -> bool {
    Url::parse(url).is_ok_and(|parsed| {
        !url.chars().any(char::is_control)
            && parsed.scheme() == "https"
            && parsed.host_str().is_some()
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.fragment().is_none()
    })
}

#[derive(Debug)]
pub enum GrantApiError {
    InvalidEndpoint,
    BuildClient(reqwest::Error),
}

impl fmt::Display for GrantApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                f,
                "invalid provider grant API endpoint; check the deployment URL and HTTP policy"
            ),
            Self::BuildClient(error) => write!(f, "prepare provider grant networking: {error}"),
        }
    }
}

impl std::error::Error for GrantApiError {}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "grant client unit tests use Rust test assertions"
)]
mod tests {
    use super::*;
    use um_test_support::ScriptedHttpServer;

    fn observe(response: &str) -> Result<GrantSession, GrantFailure> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = ScriptedHttpServer::respond(response.as_bytes().to_vec());
        let api = GrantApi::new(
            &server.api_url,
            "fixture-token",
            HttpTransportPolicy::AllowInsecureHttp,
        )
        .unwrap();
        let result = api.session("pgs_01k0z6r1w8f4jy2m7q9v3x5abc");
        let request = server.finish_one();
        assert!(
            request.starts_with("GET /api/v1/me/grant-sessions/pgs_01k0z6r1w8f4jy2m7q9v3x5abc ")
        );
        result
    }

    fn session_response(status: &str, authorization_url: &str) -> String {
        let body = serde_json::json!({
            "id": "pgs_01k0z6r1w8f4jy2m7q9v3x5abc", "provider": "github", "status": status,
            "authorizationUrl": authorization_url,
            "createdAt": "2026-09-05T12:00:00Z", "expiresAt": "2026-09-05T12:15:00Z"
        })
        .to_string();
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    // REQ-GRANT-1
    #[test]
    fn session_polling_keeps_retry_after_and_admits_only_a_pending_https_consent_url() {
        let body = r#"{"type":"https://api.usefulmachinery.com/problems/rate-limit-exceeded","title":"Slow down","status":429}"#;
        let limited = format!(
            "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/problem+json\r\nRetry-After: 7\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        assert_eq!(
            observe(&limited),
            Err(GrantFailure::RateLimited {
                retry_after: Some(7)
            })
        );
        let consent = "https://github.com/login/oauth/authorize?state=fixture";
        assert!(observe(&session_response("pending", consent)).is_ok());
        for (status, url) in [
            ("pending", "http://github.com/login/oauth/authorize"),
            (
                "pending",
                "https://user:pass@github.com/login/oauth/authorize",
            ),
            ("failed", consent),
        ] {
            assert_eq!(
                observe(&session_response(status, url)),
                Err(invalid()),
                "{status} {url}"
            );
        }
    }
}
