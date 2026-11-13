//! Closed-response plumbing shared by the hand-written resource clients.
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

/// A response whose status the caller classifies itself, read within the body bound.
pub(crate) struct RawResponse {
    pub status: StatusCode,
    pub body: Vec<u8>,
    pub headers: reqwest::header::HeaderMap,
    pub retry_after: Option<u64>,
}

/// Sends the request and reads its bounded body; only transport and an oversized body fail.
pub(crate) fn receive_raw(
    request: reqwest::blocking::RequestBuilder,
) -> Result<RawResponse, ResourceFailure> {
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
    Ok(RawResponse {
        status,
        body,
        headers,
        retry_after,
    })
}

pub(crate) fn receive_response(
    request: reqwest::blocking::RequestBuilder,
    expected_status: StatusCode,
    vocabulary: &ProblemVocabulary,
) -> Result<(Vec<u8>, reqwest::header::HeaderMap), ResourceFailure> {
    let response = receive_raw(request)?;
    if response.status != expected_status {
        return Err(classify_response(
            response.status,
            &response.body,
            response.retry_after,
            vocabulary,
        ));
    }
    Ok((response.body, response.headers))
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

/// A client's failure type that a problem table classifies: its own outcomes for the problems
/// its operations answer, and these for everything outside the table.
pub(crate) trait ProblemFailure: Clone {
    fn rate_limited(retry_after: Option<u64>) -> Self;
    fn unreachable(category: UnreachableCategory) -> Self;
    fn invalid_response(credential_rejected: bool) -> Self;
}

/// One problem an operation answers: its status, its type and the failure it means.
pub(crate) type ProblemRow<F> = (StatusCode, &'static str, F);

impl ResourceFailure {
    /// The failure of `receive_raw` or a response check, which fail only as unreachable or as
    /// an invalid response, in a problem-classified client's terms.
    pub(crate) fn into_problem_failure<F: ProblemFailure>(self) -> F {
        match self {
            Self::Unreachable { category } => F::unreachable(category),
            Self::InvalidResponse {
                credential_rejected,
            } => F::invalid_response(credential_rejected),
            _ => F::invalid_response(false),
        }
    }
}

/// Classifies a refusal by the row matching its status and problem type exactly. A rate limit
/// keeps its `Retry-After`; any other unmatched server error is unreachable, and anything else
/// is outside the contract.
pub(crate) fn classify_problem<'a, F: ProblemFailure + 'a>(
    status: StatusCode,
    body: &[u8],
    retry_after: Option<u64>,
    problems: impl IntoIterator<Item = &'a ProblemRow<F>>,
) -> F {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return F::rate_limited(retry_after);
    }
    let kind = problem::decode(body, status)
        .ok()
        .map(|problem| problem.r#type);
    if let Some((_, _, failure)) = problems
        .into_iter()
        .find(|(expected, slug, _)| *expected == status && kind.as_deref() == Some(*slug))
    {
        return failure.clone();
    }
    if status.is_server_error() {
        return F::unreachable(UnreachableCategory::Server);
    }
    F::invalid_response(status == StatusCode::UNAUTHORIZED && kind.is_none())
}

/// Classifies a generated client's error against a problem table.
pub(crate) fn classify_problem_error<'a, E, F: ProblemFailure + 'a>(
    error: apis::Error<E>,
    problems: impl IntoIterator<Item = &'a ProblemRow<F>>,
) -> F {
    match error {
        apis::Error::Reqwest(error) => F::unreachable(classify_reqwest_error(&error)),
        apis::Error::ResponseError(response) => {
            classify_problem(response.status, response.content.as_bytes(), None, problems)
        }
        apis::Error::Serde(_) | apis::Error::Io(_) => F::invalid_response(false),
    }
}

/// The response when its status is the contract's answer, otherwise the failure its problem
/// names in `problems`.
pub(crate) fn expect_problem_status<'a, F: ProblemFailure + 'a>(
    response: RawResponse,
    expected: StatusCode,
    problems: impl IntoIterator<Item = &'a ProblemRow<F>>,
) -> Result<RawResponse, F> {
    if response.status == expected {
        Ok(response)
    } else {
        Err(classify_problem(
            response.status,
            &response.body,
            response.retry_after,
            problems,
        ))
    }
}

/// Sends a DELETE whose contract answer is 204 echoing this request's key. The generated
/// DELETE accepts any 2xx and drops its headers, so it goes out under the configured client.
pub(crate) fn confirmed_delete<'a, F: ProblemFailure + From<ResourceFailure> + 'a>(
    configuration: &apis::configuration::Configuration,
    endpoint: &str,
    key: &str,
    problems: impl IntoIterator<Item = &'a ProblemRow<F>>,
) -> Result<(), F> {
    let response = receive_raw(mutation_request(
        configuration,
        reqwest::Method::DELETE,
        endpoint,
        key,
    ))?;
    let response = expect_problem_status(response, StatusCode::NO_CONTENT, problems)?;
    validate_mutation_key(&response.headers, key)?;
    Ok(())
}

/// A problem-details body of the given status whose `type` is the API problem `kind`, for the
/// vocabulary tests of each resource client.
#[cfg(test)]
#[allow(clippy::expect_used, reason = "test fixture construction")]
pub(crate) fn problem_fixture(status: u16, kind: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "type": format!("https://api.usefulmachinery.com/problems/{kind}"),
        "title": "Refused",
        "status": status,
    }))
    .expect("problem fixture")
}
