//! Google Docs action transport using generated wire models and the configured API client.
//! The generated response union accepts tombstones as defaulted active models, so reads
//! discriminate the closed shape before decoding.
use std::time::Duration;

use reqwest::{Method, StatusCode, Url, header::CONTENT_TYPE};
use serde::{Serialize, de::DeserializeOwned};

use super::{
    HttpTransportPolicy, UnreachableCategory, classify_reqwest_error,
    generated::{apis, models},
    http_client::generated_configuration,
    http_util::{self, BoundedBodyError},
    problem,
};

use models::{
    CreateGoogleDocsActionRequest, GoogleDocsAction, GoogleDocsActionList,
    PatchGoogleDocsActionRequest,
};

#[derive(Serialize)]
#[serde(untagged)]
pub enum ActionRead {
    Active(Box<GoogleDocsAction>),
    Deleted(models::GoogleDocsActionTombstone),
}

// A replay rejection cannot settle an earlier dispatch with an unread reply.
#[derive(Debug)]
pub enum ActionFailure {
    Uncertain(Option<Box<ActionFailure>>),
    Unauthenticated,
    Forbidden,
    NotFound,
    InvalidInput,
    Conflict,
    RateLimited(Option<u64>),
    Unreachable(UnreachableCategory),
    Protocol { credential_rejected: bool },
}
impl ActionFailure {
    pub fn credential_rejected(&self) -> bool {
        match self {
            Self::Uncertain(replay) => replay
                .as_ref()
                .is_some_and(|failure| failure.credential_rejected()),
            Self::Unauthenticated
            | Self::Protocol {
                credential_rejected: true,
            } => true,
            _ => false,
        }
    }
}
fn after_ambiguous_dispatch(failure: ActionFailure, ambiguous: bool) -> ActionFailure {
    if ambiguous {
        ActionFailure::Uncertain(Some(Box::new(failure)))
    } else {
        failure
    }
}

// The server permits 100 full 1 MiB normalized configurations per page. Its JSON
// encoder escapes HTML (up to six wire bytes per original byte); allow identity,
// timestamps, pagination and object framing on top of that expansion. The bounded
// reader grows with the received body rather than reserving this ceiling upfront.
const ACTION_WIRE_BYTES: usize = 6 * 1024 * 1024 + 16 * 1024;
const PAGE_FRAMING_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
enum ResponseShape {
    Action(StatusCode),
    Page(usize),
    Empty,
}
impl ResponseShape {
    fn expected(self) -> (StatusCode, usize) {
        match self {
            Self::Action(status) => (status, ACTION_WIRE_BYTES),
            Self::Page(size) => (
                StatusCode::OK,
                size * ACTION_WIRE_BYTES + PAGE_FRAMING_BYTES,
            ),
            Self::Empty => (StatusCode::NO_CONTENT, http_util::MAX_RESPONSE_BODY_BYTES),
        }
    }
}

fn protocol(credential_rejected: bool) -> ActionFailure {
    ActionFailure::Protocol {
        credential_rejected,
    }
}

pub struct ActionApi {
    configuration: apis::configuration::Configuration,
}
impl ActionApi {
    pub fn new(
        api_url: &str,
        token: &str,
        policy: HttpTransportPolicy,
    ) -> Result<Self, super::projects::ProjectApiError> {
        use super::projects::ProjectApiError as Error;
        let url = Url::parse(api_url).map_err(|_| Error::InvalidEndpoint)?;
        if !policy.permits(&url)
            || url.cannot_be_a_base()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::InvalidEndpoint);
        }
        Ok(Self {
            configuration: generated_configuration(api_url, token, policy, Duration::from_secs(20))
                .map_err(Error::BuildClient)?,
        })
    }

    fn path(&self, org: &str, project: &str, id: Option<&str>, suffix: Option<&str>) -> String {
        let mut path = format!(
            "{}/v1/organizations/{}/projects/{}/google-docs-actions",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(org),
            apis::urlencode(project)
        );
        if let Some(id) = id {
            path.push('/');
            path.push_str(&apis::urlencode(id));
        }
        if let Some(suffix) = suffix {
            path.push('/');
            path.push_str(suffix);
        }
        path
    }

    fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        key: Option<&str>,
        body: Option<&[u8]>,
        shape: ResponseShape,
    ) -> Result<(Vec<u8>, bool), ActionFailure> {
        let (expected, response_limit) = shape.expected();
        let attempts = if key.is_some() { 2 } else { 1 };
        let mut ambiguous = false;
        for attempt in 0..attempts {
            let mut request =
                super::generated_api_request(&self.configuration, method.clone(), path);
            if !query.is_empty() {
                request = request.query(query);
            }
            if let Some(key) = key {
                request = request.header("Idempotency-Key", key);
            }
            if let Some(body) = body {
                request = request
                    .header(CONTENT_TYPE, problem::JSON_MEDIA_TYPE)
                    .body(body.to_vec());
            }
            let response = match request.send() {
                Ok(response) => response,
                Err(error) => {
                    let category = classify_reqwest_error(&error);
                    if key.is_some()
                        && http_util::can_retry_ambiguous_mutation(attempt, attempts, category)
                    {
                        ambiguous = true;
                        continue;
                    }
                    return Err(after_ambiguous_dispatch(
                        ActionFailure::Unreachable(category),
                        ambiguous,
                    ));
                }
            };
            let status = response.status();
            let headers = response.headers().clone();
            let limit = if status == expected {
                response_limit
            } else {
                http_util::MAX_RESPONSE_BODY_BYTES
            };
            let bytes = match http_util::buffer_blocking_response_with_limit(response, limit) {
                Ok(response) => response.body,
                Err(BoundedBodyError::Transport(error)) => {
                    let category = classify_reqwest_error(&error);
                    if status == expected
                        && key.is_some()
                        && http_util::can_retry_ambiguous_mutation(attempt, attempts, category)
                    {
                        ambiguous = true;
                        continue;
                    }
                    return Err(after_ambiguous_dispatch(
                        ActionFailure::Unreachable(category),
                        ambiguous,
                    ));
                }
                Err(BoundedBodyError::TooLarge) => {
                    return Err(after_ambiguous_dispatch(
                        protocol(status == StatusCode::UNAUTHORIZED),
                        ambiguous,
                    ));
                }
            };
            if status != expected {
                if status.is_server_error() {
                    return Err(after_ambiguous_dispatch(
                        ActionFailure::Unreachable(UnreachableCategory::Server),
                        ambiguous,
                    ));
                }
                if status == StatusCode::TOO_MANY_REQUESTS {
                    let retry_after = headers
                        .get("Retry-After")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok())
                        .filter(|value| *value > 0);
                    return Err(after_ambiguous_dispatch(
                        ActionFailure::RateLimited(retry_after),
                        ambiguous,
                    ));
                }
                if http_util::require_header_media_type(
                    headers.get(CONTENT_TYPE),
                    problem::PROBLEM_MEDIA_TYPE,
                )
                .is_err()
                {
                    return Err(after_ambiguous_dispatch(
                        protocol(status == StatusCode::UNAUTHORIZED),
                        ambiguous,
                    ));
                }
                let decoded = problem::decode(&bytes, status).map_err(|_| {
                    after_ambiguous_dispatch(
                        protocol(status == StatusCode::UNAUTHORIZED),
                        ambiguous,
                    )
                })?;
                return Err(after_ambiguous_dispatch(
                    match (status, decoded.r#type.as_str()) {
                        (StatusCode::UNAUTHORIZED, problem::UNAUTHORIZED) => {
                            ActionFailure::Unauthenticated
                        }
                        (StatusCode::FORBIDDEN, problem::FORBIDDEN) => ActionFailure::Forbidden,
                        (StatusCode::NOT_FOUND, problem::NOT_FOUND) => ActionFailure::NotFound,
                        (StatusCode::BAD_REQUEST, problem::BAD_REQUEST) => {
                            ActionFailure::InvalidInput
                        }
                        (
                            StatusCode::CONFLICT,
                            "https://api.usefulmachinery.com/problems/source-connection-conflict",
                        ) => ActionFailure::Conflict,
                        _ => protocol(status == StatusCode::UNAUTHORIZED),
                    },
                    ambiguous,
                ));
            }
            if let Some(key) = key {
                let mut values = headers.get_all("Idempotency-Key").iter();
                if values.next().map(reqwest::header::HeaderValue::as_bytes) != Some(key.as_bytes())
                    || values.next().is_some()
                {
                    return Err(after_ambiguous_dispatch(protocol(false), ambiguous));
                }
            }
            if expected == StatusCode::NO_CONTENT {
                if !bytes.is_empty() {
                    return Err(after_ambiguous_dispatch(protocol(false), ambiguous));
                }
            } else if http_util::require_header_media_type(
                headers.get(CONTENT_TYPE),
                problem::JSON_MEDIA_TYPE,
            )
            .is_err()
            {
                return Err(after_ambiguous_dispatch(protocol(false), ambiguous));
            }
            return Ok((bytes, ambiguous));
        }
        Err(ActionFailure::Unreachable(UnreachableCategory::Connection))
    }

    fn json<T: DeserializeOwned + Serialize>(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        key: Option<&str>,
        body: Option<&[u8]>,
        shape: ResponseShape,
    ) -> Result<(T, bool), ActionFailure> {
        let (bytes, ambiguous) = self.request(method, path, query, key, body, shape)?;
        let invalid = || after_ambiguous_dispatch(protocol(false), ambiguous);
        let raw: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        let value: T = serde_json::from_value(raw.clone()).map_err(|_| invalid())?;
        if serde_json::to_value(&value).map_err(|_| invalid())? != raw {
            return Err(invalid());
        }
        Ok((value, ambiguous))
    }
    pub fn create(
        &self,
        org: &str,
        project: &str,
        key: &str,
        body: &CreateGoogleDocsActionRequest,
    ) -> Result<GoogleDocsAction, ActionFailure> {
        let bytes = serde_json::to_vec(body).map_err(|_| protocol(false))?;
        let value = self.json(
            Method::POST,
            &self.path(org, project, None, None),
            &[],
            Some(key),
            Some(&bytes),
            ResponseShape::Action(StatusCode::CREATED),
        )?;
        validate(value.0, project, None)
            .map_err(|failure| after_ambiguous_dispatch(failure, value.1))
    }
    pub fn update(
        &self,
        org: &str,
        project: &str,
        id: &str,
        key: &str,
        body: &PatchGoogleDocsActionRequest,
    ) -> Result<GoogleDocsAction, ActionFailure> {
        let bytes = serde_json::to_vec(body).map_err(|_| protocol(false))?;
        let value = self.json(
            Method::PATCH,
            &self.path(org, project, Some(id), None),
            &[],
            Some(key),
            Some(&bytes),
            ResponseShape::Action(StatusCode::OK),
        )?;
        validate(value.0, project, Some(id))
            .map_err(|failure| after_ambiguous_dispatch(failure, value.1))
    }
    pub fn list(
        &self,
        org: &str,
        project: &str,
        limit: Option<u16>,
        cursor: Option<&str>,
    ) -> Result<GoogleDocsActionList, ActionFailure> {
        let mut query = Vec::new();
        if let Some(limit) = limit {
            query.push(("limit".to_owned(), limit.to_string()));
        }
        if let Some(cursor) = cursor {
            query.push(("cursor".to_owned(), cursor.to_owned()));
        }
        let (page, _): (GoogleDocsActionList, bool) = self.json(
            Method::GET,
            &self.path(org, project, None, None),
            &query,
            None,
            None,
            ResponseShape::Page(usize::from(limit.unwrap_or(50).clamp(1, 100))),
        )?;
        if page.next_cursor.as_deref() == Some("") {
            return Err(protocol(false));
        }
        for item in &page.items {
            validate(item.clone(), project, None)?;
        }
        Ok(page)
    }
    pub fn get(&self, org: &str, project: &str, id: &str) -> Result<ActionRead, ActionFailure> {
        let (bytes, _) = self.request(
            Method::GET,
            &self.path(org, project, Some(id), None),
            &[],
            None,
            None,
            ResponseShape::Action(StatusCode::OK),
        )?;
        let raw: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| protocol(false))?;
        if raw.get("deleted").is_some() {
            if raw.as_object().is_none_or(|fields| fields.len() != 4)
                || raw.get("deleted") != Some(&serde_json::Value::Bool(true))
            {
                return Err(protocol(false));
            }
            let value: models::GoogleDocsActionTombstone =
                serde_json::from_value(raw).map_err(|_| protocol(false))?;
            if value.id != id || value.project_id != project || value.deleted_at.is_empty() {
                return Err(protocol(false));
            }
            Ok(ActionRead::Deleted(value))
        } else {
            let value: GoogleDocsAction =
                serde_json::from_value(raw.clone()).map_err(|_| protocol(false))?;
            if serde_json::to_value(&value).map_err(|_| protocol(false))? != raw {
                return Err(protocol(false));
            }
            Ok(ActionRead::Active(Box::new(validate(
                value,
                project,
                Some(id),
            )?)))
        }
    }
    pub fn lifecycle(
        &self,
        org: &str,
        project: &str,
        id: &str,
        key: &str,
        kind: Lifecycle,
    ) -> Result<Option<GoogleDocsAction>, ActionFailure> {
        let (method, suffix, expected) = match kind {
            Lifecycle::Enable => (Method::POST, Some("enable"), StatusCode::OK),
            Lifecycle::Disable => (Method::POST, Some("disable"), StatusCode::OK),
            Lifecycle::Delete => (Method::DELETE, None, StatusCode::NO_CONTENT),
        };
        let path = self.path(org, project, Some(id), suffix);
        if expected == StatusCode::NO_CONTENT {
            self.request(method, &path, &[], Some(key), None, ResponseShape::Empty)?;
            Ok(None)
        } else {
            let value = self.json(
                method,
                &path,
                &[],
                Some(key),
                None,
                ResponseShape::Action(expected),
            )?;
            validate(value.0, project, Some(id))
                .map_err(|failure| after_ambiguous_dispatch(failure, value.1))
                .map(Some)
        }
    }
}
#[derive(Clone, Copy)]
pub enum Lifecycle {
    Enable,
    Disable,
    Delete,
}
fn validate(
    value: GoogleDocsAction,
    project: &str,
    id: Option<&str>,
) -> Result<GoogleDocsAction, ActionFailure> {
    if !um_support::valid_typed_id(&value.id, "gda_")
        || value.project_id != project
        || id.is_some_and(|id| value.id != id)
        || value.version < 1
        || value.created_at.is_empty()
        || value.updated_at.is_empty()
    {
        return Err(protocol(false));
    }
    Ok(value)
}
impl Drop for ActionApi {
    fn drop(&mut self) {
        super::clear_generated_access_token(&mut self.configuration);
    }
}
