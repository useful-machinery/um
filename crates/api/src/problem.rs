use reqwest::StatusCode;
use reqwest::header::HeaderValue;

use super::generated::models;

pub(super) const JSON_MEDIA_TYPE: &str = "application/json";
pub(super) const PROBLEM_MEDIA_TYPE: &str = "application/problem+json";
pub(super) const ACCEPTED_MEDIA_TYPES: &str = "application/json, application/problem+json";
pub(super) const BAD_REQUEST: &str = "https://api.usefulmachinery.com/problems/bad-request";
pub(super) const UNAUTHORIZED: &str = "https://api.usefulmachinery.com/problems/unauthorized";
pub(super) const FORBIDDEN: &str = "https://api.usefulmachinery.com/problems/forbidden";
pub(super) const NOT_FOUND: &str = "https://api.usefulmachinery.com/problems/not-found";
pub(super) const IDEMPOTENCY_CONFLICT: &str =
    "https://api.usefulmachinery.com/problems/idempotency-conflict";
pub(super) const RATE_LIMIT_EXCEEDED: &str =
    "https://api.usefulmachinery.com/problems/rate-limit-exceeded";
pub(super) const REQUEST_BODY_TOO_LARGE: &str =
    "https://api.usefulmachinery.com/problems/request-body-too-large";
pub(super) const QUANTITY_LIMIT_REACHED: &str =
    "https://api.usefulmachinery.com/problems/quantity-limit-reached";
pub(super) const PROVIDER_UNAVAILABLE: &str =
    "https://api.usefulmachinery.com/problems/provider-unavailable";
pub(super) const IDENTITY_NOT_FOUND: &str =
    "https://api.usefulmachinery.com/problems/identity-not-found";
pub(super) const GRANT_NOT_FOUND: &str = "https://api.usefulmachinery.com/problems/grant-not-found";
pub(super) const GRANT_SESSION_NOT_FOUND: &str =
    "https://api.usefulmachinery.com/problems/grant-session-not-found";
pub(super) const GRANT_SESSION_CONFLICT: &str =
    "https://api.usefulmachinery.com/problems/grant-session-conflict";
pub(super) const SERVICE_UNAVAILABLE: &str =
    "https://api.usefulmachinery.com/problems/service-unavailable";
pub(super) const PROJECT_ARCHIVED: &str =
    "https://api.usefulmachinery.com/problems/project-archived";
/// The `reason` a conflict carries when its project is archived, under a type a published CLI
/// already reads as a conflict (`webhook-conflict`, `google-docs-action-conflict`).
pub(super) const PROJECT_ARCHIVED_REASON: &str = "project-archived";
/// The `reason` a `google-docs-action-conflict` carries when its project is bound to a GitLab
/// repository (REQ-DOCSACTION-128).
pub(super) const GOOGLE_DOCS_PROJECT_UNSUPPORTED_REASON: &str = "google-docs-project-unsupported";

pub(super) fn decode_type(
    response: &super::http_util::BufferedResponse,
) -> Result<String, &'static str> {
    decode_type_parts(
        &response.body,
        response.status,
        response.content_type.as_deref(),
    )
}

pub(super) fn require_type(
    response: &super::http_util::BufferedResponse,
    expected_type: &str,
) -> Result<(), &'static str> {
    require_type_parts(
        &response.body,
        response.status,
        response.content_type.as_deref(),
        expected_type,
    )
}

pub(super) fn decode(
    body: &[u8],
    expected_status: StatusCode,
) -> Result<models::Problem, &'static str> {
    let problem: models::Problem =
        serde_json::from_slice(body).map_err(|_| "the problem response body is invalid")?;
    if problem.status != i32::from(expected_status.as_u16()) {
        return Err("the problem status does not match the HTTP status");
    }
    Ok(problem)
}

pub(super) fn decode_parts(
    body: &[u8],
    status: StatusCode,
    content_type: Option<&str>,
) -> Result<models::Problem, &'static str> {
    super::http_util::require_media_type(content_type, PROBLEM_MEDIA_TYPE)?;
    decode(body, status)
}

pub(super) fn decode_header_parts(
    body: &[u8],
    status: StatusCode,
    content_type: Option<&HeaderValue>,
) -> Result<models::Problem, &'static str> {
    super::http_util::require_header_media_type(content_type, PROBLEM_MEDIA_TYPE)?;
    decode(body, status)
}

pub(super) fn decode_type_parts(
    body: &[u8],
    status: StatusCode,
    content_type: Option<&str>,
) -> Result<String, &'static str> {
    decode_parts(body, status, content_type).map(|problem| problem.r#type)
}

pub(super) fn require_type_parts(
    body: &[u8],
    status: StatusCode,
    content_type: Option<&str>,
    expected_type: &str,
) -> Result<(), &'static str> {
    if decode_type_parts(body, status, content_type)? == expected_type {
        Ok(())
    } else {
        Err("the problem type is not valid for its HTTP status")
    }
}

/// Whether `values` is a non-empty ordered selection from `allowed`: each value appears in
/// `allowed` after the one before it, so a value out of order or repeated fails.
pub(super) fn ordered_selection<T: PartialEq>(values: &[T], allowed: &[T]) -> bool {
    if values.is_empty() {
        return false;
    }
    let mut next = 0;
    for value in values {
        let Some(offset) = allowed[next..].iter().position(|allowed| allowed == value) else {
            return false;
        };
        next += offset + 1;
    }
    true
}
