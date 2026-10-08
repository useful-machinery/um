use std::fmt;
use std::io;
use std::time::Duration;

use reqwest::header::{AUTHORIZATION, HeaderValue};
use reqwest::{Response, StatusCode, Url};

use super::bearer_authorization;
use super::http_client::{DnsResolutionError, HttpClient, HttpEndpointError};
use super::http_util::{self, BufferedResponseError};
use super::principal_profile::{self, PrincipalProfile};
use super::problem;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const PRINCIPAL_NOT_PROVISIONED: &str =
    "https://api.usefulmachinery.com/problems/principal-not-provisioned";
const JSON_MEDIA_TYPE: &str = "application/json";
const ACCEPTED_MEDIA_TYPES: &str = "application/json, application/problem+json";

#[derive(Debug, Eq, PartialEq)]
pub struct AuthenticatedPrincipal {
    pub principal: PrincipalProfile,
    pub actions: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum CurrentPrincipalOutcome {
    Authenticated(AuthenticatedPrincipal),
    SignupRequired {
        actions: Option<Vec<serde_json::Value>>,
    },
    Unauthenticated,
    Unreachable(UnreachableCategory),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnreachableCategory {
    Dns,
    Timeout,
    Connection,
    Tls,
    RateLimited,
    Server,
}

impl UnreachableCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dns => "dns",
            Self::Timeout => "timeout",
            Self::Connection => "connection",
            Self::Tls => "tls",
            Self::RateLimited => "rate_limited",
            Self::Server => "server",
        }
    }

    pub const fn retryable_observation(self) -> bool {
        matches!(self, Self::Connection | Self::Timeout | Self::Server)
    }
}

#[derive(Debug)]
pub struct CurrentPrincipalError {
    kind: CurrentPrincipalErrorKind,
    credential_rejected: bool,
}

impl CurrentPrincipalError {
    pub fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }

    pub fn is_local(&self) -> bool {
        !matches!(&self.kind, CurrentPrincipalErrorKind::Protocol { .. })
    }

    fn local(kind: CurrentPrincipalErrorKind) -> Self {
        Self {
            kind,
            credential_rejected: false,
        }
    }

    fn protocol(reason: &'static str, credential_rejected: bool) -> Self {
        Self {
            kind: CurrentPrincipalErrorKind::Protocol { reason },
            credential_rejected,
        }
    }
}

#[derive(Debug)]
enum CurrentPrincipalErrorKind {
    Endpoint(HttpEndpointError),
    InvalidAuthorizationHeader,
    Protocol { reason: &'static str },
}

impl fmt::Display for CurrentPrincipalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            CurrentPrincipalErrorKind::Endpoint(HttpEndpointError::Invalid) => write!(
                formatter,
                "the deployment API URL cannot form a current-principal endpoint"
            ),
            CurrentPrincipalErrorKind::Endpoint(HttpEndpointError::InsecureHttp) => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            CurrentPrincipalErrorKind::InvalidAuthorizationHeader => {
                write!(
                    formatter,
                    "the stored access token cannot be represented as a bearer credential"
                )
            }
            CurrentPrincipalErrorKind::Protocol { reason } => {
                write!(
                    formatter,
                    "current-principal response violates the public API contract: {reason}"
                )
            }
        }
    }
}

pub fn get_current_principal(
    client: &HttpClient,
    api_url: &str,
    access_token: Option<&str>,
) -> Result<CurrentPrincipalOutcome, CurrentPrincipalError> {
    get_current_principal_with_timeout(client, api_url, access_token, REQUEST_TIMEOUT)
}

fn get_current_principal_with_timeout(
    client: &HttpClient,
    api_url: &str,
    access_token: Option<&str>,
    timeout: Duration,
) -> Result<CurrentPrincipalOutcome, CurrentPrincipalError> {
    let endpoint = client.endpoint(api_url, &["v1", "me"]).map_err(|error| {
        CurrentPrincipalError::local(CurrentPrincipalErrorKind::Endpoint(error))
    })?;
    let authorization = access_token
        .map(bearer_authorization)
        .transpose()
        .map_err(|_| {
            CurrentPrincipalError::local(CurrentPrincipalErrorKind::InvalidAuthorizationHeader)
        })?;
    let result = client.run(
        timeout,
        execute_current_principal_request(client, endpoint, authorization, timeout),
    );

    match result {
        Ok(result) => result,
        Err(_) => Ok(CurrentPrincipalOutcome::Unreachable(
            UnreachableCategory::Timeout,
        )),
    }
}

async fn execute_current_principal_request(
    client: &HttpClient,
    endpoint: Url,
    authorization: Option<HeaderValue>,
    timeout: Duration,
) -> Result<CurrentPrincipalOutcome, CurrentPrincipalError> {
    let mut request = client
        .inner()
        .get(endpoint)
        .timeout(timeout)
        .header(reqwest::header::ACCEPT, ACCEPTED_MEDIA_TYPES);
    if let Some(authorization) = authorization {
        request = request.header(AUTHORIZATION, authorization);
    }

    let response = match request.send().await {
        Ok(response) => response,
        Err(error) if error.is_builder() => {
            return Err(CurrentPrincipalError::protocol(
                "the current-principal request could not be constructed",
                false,
            ));
        }
        Err(error) => {
            return Ok(CurrentPrincipalOutcome::Unreachable(
                classify_reqwest_error(&error),
            ));
        }
    };

    decode_response(response).await
}

async fn decode_response(
    response: Response,
) -> Result<CurrentPrincipalOutcome, CurrentPrincipalError> {
    let (status, content_type, body) = match http_util::decode_response_parts(response).await {
        Ok(parts) => parts,
        Err(BufferedResponseError::InvalidContentType { status }) => {
            return Err(CurrentPrincipalError::protocol(
                "the Content-Type header is not valid text",
                status == StatusCode::UNAUTHORIZED,
            ));
        }
        Err(BufferedResponseError::TooLarge { status }) => {
            return Err(CurrentPrincipalError::protocol(
                "the response body exceeds 1 MiB",
                status == StatusCode::UNAUTHORIZED,
            ));
        }
        Err(BufferedResponseError::Transport { source, .. }) => {
            return Ok(CurrentPrincipalOutcome::Unreachable(
                classify_reqwest_error(&source),
            ));
        }
    };

    match status {
        StatusCode::OK => {
            require_media_type(content_type.as_deref(), JSON_MEDIA_TYPE, false)?;
            decode_authenticated(&body)
        }
        StatusCode::UNAUTHORIZED => {
            let problem = problem::decode_parts(&body, status, content_type.as_deref())
                .map_err(|reason| CurrentPrincipalError::protocol(reason, true))?;
            let _ = problem;
            Ok(CurrentPrincipalOutcome::Unauthenticated)
        }
        StatusCode::FORBIDDEN => {
            let problem = problem::decode_parts(&body, status, content_type.as_deref())
                .map_err(|reason| CurrentPrincipalError::protocol(reason, false))?;
            if problem.r#type != PRINCIPAL_NOT_PROVISIONED {
                return Err(CurrentPrincipalError::protocol(
                    "a 403 response is not the principal-not-provisioned problem",
                    false,
                ));
            }
            Ok(CurrentPrincipalOutcome::SignupRequired {
                actions: problem.actions,
            })
        }
        StatusCode::TOO_MANY_REQUESTS => Ok(CurrentPrincipalOutcome::Unreachable(
            UnreachableCategory::RateLimited,
        )),
        status if status.is_server_error() => Ok(CurrentPrincipalOutcome::Unreachable(
            UnreachableCategory::Server,
        )),
        status if status.is_redirection() => Err(CurrentPrincipalError::protocol(
            "redirect responses are not permitted",
            false,
        )),
        _ => Err(CurrentPrincipalError::protocol(
            "the HTTP status is not valid for this operation",
            false,
        )),
    }
}

fn require_media_type(
    actual: Option<&str>,
    expected: &'static str,
    credential_rejected: bool,
) -> Result<(), CurrentPrincipalError> {
    http_util::require_media_type(actual, expected)
        .map_err(|reason| CurrentPrincipalError::protocol(reason, credential_rejected))
}

fn decode_authenticated(body: &[u8]) -> Result<CurrentPrincipalOutcome, CurrentPrincipalError> {
    let response: super::generated::models::CurrentPrincipalResponse = serde_json::from_slice(body)
        .map_err(|_| {
            CurrentPrincipalError::protocol(
                "the current-principal response fields are invalid",
                false,
            )
        })?;
    let principal = principal_profile::from_api(*response.principal)
        .map_err(|reason| CurrentPrincipalError::protocol(reason, false))?;
    Ok(CurrentPrincipalOutcome::Authenticated(
        AuthenticatedPrincipal {
            principal,
            actions: response.actions,
        },
    ))
}

pub fn classify_reqwest_error(error: &reqwest::Error) -> UnreachableCategory {
    if error.is_timeout() {
        UnreachableCategory::Timeout
    } else {
        classify_error_chain(error)
    }
}

pub(super) fn classify_error_chain(
    error: &(dyn std::error::Error + 'static),
) -> UnreachableCategory {
    let mut source = Some(error);
    let mut timed_out = false;
    let mut dns_failed = false;
    let mut tls_failed = false;

    while let Some(current) = source {
        if let Some(io_error) = current.downcast_ref::<io::Error>() {
            let io_kinds = classify_io_error(io_error);
            timed_out |= io_kinds.timed_out;
            dns_failed |= io_kinds.dns_failed;
            tls_failed |= io_kinds.tls_failed;
        }
        dns_failed |= current.downcast_ref::<DnsResolutionError>().is_some();
        tls_failed |= current.downcast_ref::<rustls::Error>().is_some();
        source = current.source();
    }

    if timed_out {
        UnreachableCategory::Timeout
    } else if dns_failed {
        UnreachableCategory::Dns
    } else if tls_failed {
        UnreachableCategory::Tls
    } else {
        UnreachableCategory::Connection
    }
}

#[derive(Default)]
struct IoErrorKinds {
    timed_out: bool,
    dns_failed: bool,
    tls_failed: bool,
}

fn classify_io_error(mut error: &io::Error) -> IoErrorKinds {
    let mut kinds = IoErrorKinds::default();
    loop {
        kinds.timed_out |= error.kind() == io::ErrorKind::TimedOut;
        kinds.tls_failed |= error.kind() == io::ErrorKind::InvalidData;
        let Some(inner) = error.get_ref() else {
            break;
        };
        kinds.dns_failed |= inner.downcast_ref::<DnsResolutionError>().is_some();
        kinds.tls_failed |= inner.downcast_ref::<rustls::Error>().is_some();
        let Some(nested) = inner.downcast_ref::<io::Error>() else {
            break;
        };
        error = nested;
    }
    kinds
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "principal API unit tests use Rust test assertions and fixture extraction"
)]
mod tests;
