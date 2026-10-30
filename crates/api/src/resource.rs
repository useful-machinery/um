//! Closed-response plumbing shared by the hand-written project resource clients.
//!
//! The generated client drops error headers and tolerates unknown members. These
//! helpers keep its models while reading `Retry-After`, bounding every body,
//! confirming idempotency keys and refusing any response outside the contract.
use std::time::Duration;

use reqwest::{StatusCode, Url};

use super::generated::apis;
use super::http_client::generated_configuration;
use super::http_util::{self, BoundedBodyError};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error, problem};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResourceFailure {
    Unauthenticated,
    Forbidden,
    /// `problem` names the typed problem (its type's last path segment), `None` for
    /// the generic `bad-request`.
    InvalidInput {
        problem: Option<String>,
    },
    NotFound,
    Conflict,
    Unreachable {
        category: UnreachableCategory,
    },
    RateLimited {
        retry_after: Option<u64>,
    },
    InvalidResponse {
        credential_rejected: bool,
    },
}

impl ResourceFailure {
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

#[derive(Clone, Copy)]
pub enum LifecycleAction {
    Enable,
    Disable,
    Delete,
}

/// The problem types a resource maps to `InvalidInput` (400 and 422) and `Conflict` (409).
pub(crate) struct ProblemVocabulary {
    pub bad_request: &'static [&'static str],
    pub unprocessable: &'static [&'static str],
    pub conflict: &'static [&'static str],
}

/// Applies the URL policy shared by the resource clients and builds their generated
/// configuration with the 20-second request timeout.
pub(crate) fn resource_configuration<E>(
    api_url: &str,
    token: &str,
    policy: HttpTransportPolicy,
    invalid_endpoint: E,
    build_client: impl FnOnce(reqwest::Error) -> E,
) -> Result<apis::configuration::Configuration, E> {
    let Ok(parsed) = Url::parse(api_url) else {
        return Err(invalid_endpoint);
    };
    if !policy.permits(&parsed)
        || parsed.cannot_be_a_base()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(invalid_endpoint);
    }
    generated_configuration(api_url, token, policy, Duration::from_secs(20)).map_err(build_client)
}

pub(crate) fn mutation_request(
    configuration: &apis::configuration::Configuration,
    method: reqwest::Method,
    endpoint: &str,
    key: &str,
) -> reqwest::blocking::RequestBuilder {
    super::generated_api_request(configuration, method, endpoint).header("Idempotency-Key", key)
}

pub(crate) fn receive_closed_json_response<T: serde::de::DeserializeOwned + serde::Serialize>(
    request: reqwest::blocking::RequestBuilder,
    expected_status: StatusCode,
    vocabulary: &ProblemVocabulary,
) -> Result<T, ResourceFailure> {
    let (body, headers) = receive_response(request, expected_status, vocabulary)?;
    validate_json_media_type(&headers)?;
    decode_closed_json(&body)
}

pub(crate) fn receive_confirmed_json_response<T: serde::de::DeserializeOwned + serde::Serialize>(
    request: reqwest::blocking::RequestBuilder,
    expected_status: StatusCode,
    key: &str,
    vocabulary: &ProblemVocabulary,
) -> Result<T, ResourceFailure> {
    let (body, headers) = receive_response(request, expected_status, vocabulary)?;
    validate_mutation_key(&headers, key)?;
    validate_json_media_type(&headers)?;
    decode_closed_json(&body)
}

pub(crate) fn validate_mutation_key(
    headers: &reqwest::header::HeaderMap,
    key: &str,
) -> Result<(), ResourceFailure> {
    let keys = headers.get_all("Idempotency-Key");
    if keys.iter().count() != 1
        || keys
            .iter()
            .next()
            .is_none_or(|value| value.as_bytes() != key.as_bytes())
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn decode_closed_json<T: serde::de::DeserializeOwned + serde::Serialize>(
    body: &[u8],
) -> Result<T, ResourceFailure> {
    let raw: serde_json::Value = serde_json::from_slice(body).map_err(|_| invalid())?;
    let decoded: T = serde_json::from_value(raw.clone()).map_err(|_| invalid())?;
    if serde_json::to_value(&decoded).map_err(|_| invalid())? != raw {
        return Err(invalid());
    }
    Ok(decoded)
}

pub(crate) fn receive_json_response(
    request: reqwest::blocking::RequestBuilder,
    expected_status: StatusCode,
    vocabulary: &ProblemVocabulary,
) -> Result<Vec<u8>, ResourceFailure> {
    let (body, headers) = receive_response(request, expected_status, vocabulary)?;
    validate_json_media_type(&headers)?;
    Ok(body)
}

pub(crate) fn validate_json_media_type(
    headers: &reqwest::header::HeaderMap,
) -> Result<(), ResourceFailure> {
    http_util::require_header_media_type(
        headers.get(reqwest::header::CONTENT_TYPE),
        problem::JSON_MEDIA_TYPE,
    )
    .map_err(|_| invalid())
}

pub(crate) fn receive_response(
    request: reqwest::blocking::RequestBuilder,
    expected_status: StatusCode,
    vocabulary: &ProblemVocabulary,
) -> Result<(Vec<u8>, reqwest::header::HeaderMap), ResourceFailure> {
    let response = request
        .send()
        .map_err(|error| ResourceFailure::Unreachable {
            category: classify_reqwest_error(&error),
        })?;
    let status = response.status();
    let headers = response.headers().clone();
    let retry_after = retry_after(&headers);
    let body = http_util::read_bounded_blocking_body(response).map_err(|error| match error {
        BoundedBodyError::TooLarge => ResourceFailure::InvalidResponse {
            credential_rejected: status == StatusCode::UNAUTHORIZED,
        },
        BoundedBodyError::Transport(error) => ResourceFailure::Unreachable {
            category: classify_reqwest_error(&error),
        },
    })?;
    if status != expected_status {
        return Err(classify_response(status, &body, retry_after, vocabulary));
    }
    Ok((body, headers))
}

/// The delay-seconds form of `Retry-After`; an HTTP date or malformed value is absent.
pub(crate) fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get("Retry-After")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

pub(crate) const fn invalid() -> ResourceFailure {
    ResourceFailure::InvalidResponse {
        credential_rejected: false,
    }
}

pub(crate) fn classify_error<E>(
    error: apis::Error<E>,
    vocabulary: &ProblemVocabulary,
) -> ResourceFailure {
    match error {
        apis::Error::Reqwest(error) => ResourceFailure::Unreachable {
            category: classify_reqwest_error(&error),
        },
        apis::Error::ResponseError(response) => classify_response(
            response.status,
            response.content.as_bytes(),
            None,
            vocabulary,
        ),
        apis::Error::Serde(_) | apis::Error::Io(_) => invalid(),
    }
}

pub(crate) fn classify_response(
    status: StatusCode,
    body: &[u8],
    retry_after: Option<u64>,
    vocabulary: &ProblemVocabulary,
) -> ResourceFailure {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return ResourceFailure::RateLimited { retry_after };
    }
    if status.is_server_error() {
        return ResourceFailure::Unreachable {
            category: UnreachableCategory::Server,
        };
    }
    let Ok(decoded) = problem::decode(body, status) else {
        return ResourceFailure::InvalidResponse {
            credential_rejected: status == StatusCode::UNAUTHORIZED,
        };
    };
    match (status, decoded.r#type.as_str()) {
        (StatusCode::BAD_REQUEST, kind) if vocabulary.bad_request.contains(&kind) => {
            ResourceFailure::InvalidInput {
                problem: (kind != problem::BAD_REQUEST)
                    .then(|| kind.rsplit('/').next().unwrap_or(kind).to_owned()),
            }
        }
        (StatusCode::UNPROCESSABLE_ENTITY, kind) if vocabulary.unprocessable.contains(&kind) => {
            ResourceFailure::InvalidInput {
                problem: Some(kind.rsplit('/').next().unwrap_or(kind).to_owned()),
            }
        }
        (StatusCode::UNAUTHORIZED, problem::UNAUTHORIZED) => ResourceFailure::Unauthenticated,
        (StatusCode::FORBIDDEN, problem::FORBIDDEN) => ResourceFailure::Forbidden,
        (StatusCode::NOT_FOUND, problem::NOT_FOUND) => ResourceFailure::NotFound,
        (StatusCode::CONFLICT, kind) if vocabulary.conflict.contains(&kind) => {
            ResourceFailure::Conflict
        }
        _ => invalid(),
    }
}
