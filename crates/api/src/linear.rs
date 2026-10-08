//! Nonsecret Linear connection projections over the generated source-connection client.
use std::fmt;
use std::time::Duration;

use reqwest::{StatusCode, Url};

use super::generated::{apis, models};
use super::http_client::{generated_configuration, zeroize_generated_bearer_access_token};
use super::http_util::{self, BoundedBodyError};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error, problem};

pub type LinearSession = models::LinearAuthorizationSession;
pub type LinearConnection = models::LinearConnection;
pub type LinearConnectionList = models::LinearConnectionList;
pub type LinearEvaluation = models::LinearEvaluation;
pub type LinearTrigger = models::LinearTrigger;
pub type LinearTriggerList = models::LinearTriggerList;

#[derive(Clone, serde::Serialize)]
#[serde(untagged)]
pub enum LinearTriggerRead {
    Active(LinearTrigger),
    Deleted(models::LinearTriggerTombstone),
}
pub type LinearEvaluationList = models::LinearEvaluationList;
pub type CreateLinearTriggerRequest = models::CreateLinearTriggerRequest;
pub type UpdateLinearTriggerRequest = models::UpdateLinearTriggerRequest;

pub struct EvaluationFilters<'a> {
    pub limit: Option<i32>,
    pub cursor: Option<&'a str>,
    pub state: Option<&'a str>,
    pub run_id: Option<&'a str>,
}
pub type LinearEvaluationState = models::linear_evaluation::State;
pub type LinearSessionStatus = models::linear_authorization_session::Status;

pub struct LinearApi {
    configuration: apis::configuration::Configuration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinearFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    Conflict,
    Unreachable { category: UnreachableCategory },
    RateLimited { retry_after: Option<u64> },
    InvalidResponse { credential_rejected: bool },
}

impl LinearFailure {
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

impl LinearApi {
    pub fn new(
        api_url: &str,
        token: &str,
        policy: HttpTransportPolicy,
    ) -> Result<Self, LinearApiError> {
        let parsed = Url::parse(api_url).map_err(|_| LinearApiError::InvalidEndpoint)?;
        if !policy.permits(&parsed)
            || parsed.cannot_be_a_base()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(LinearApiError::InvalidEndpoint);
        }
        let configuration =
            generated_configuration(api_url, token, policy, Duration::from_secs(20))
                .map_err(LinearApiError::BuildClient)?;
        Ok(Self { configuration })
    }

    pub fn start(
        &self,
        organization: &str,
        connection_id: Option<&str>,
        key: &str,
    ) -> Result<LinearSession, LinearFailure> {
        let mut request =
            models::CreateLinearAuthorizationSessionRequest::new(match connection_id {
                Some(_) => {
                    models::create_linear_authorization_session_request::Operation::Reauthorize
                }
                None => models::create_linear_authorization_session_request::Operation::Connect,
            });
        request.connection_id = connection_id.map(str::to_owned);
        let session = apis::source_connections_api::create_linear_authorization_session(
            &self.configuration,
            organization,
            key,
            request,
        )
        .map_err(classify_error)?;
        let session = validate_session(session, organization, None)?;
        let matches_operation = match connection_id {
            Some(id) => {
                session.operation == models::linear_authorization_session::Operation::Reauthorize
                    && session.connection_id.as_deref() == Some(id)
            }
            None => {
                session.operation == models::linear_authorization_session::Operation::Connect
                    && session.connection_id.is_none()
            }
        };
        if !matches_operation {
            return Err(invalid());
        }
        Ok(session)
    }

    // The generated GET discards response headers on errors. Observe the same generated
    // model using its configured client so Retry-After survives rate-limited polling.
    pub fn session(&self, organization: &str, id: &str) -> Result<LinearSession, LinearFailure> {
        let endpoint = format!(
            "{}/v1/organizations/{}/connections/linear/authorization-sessions/{}",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization),
            apis::urlencode(id)
        );
        let mut request = self.configuration.client.get(endpoint);
        if let Some(token) = &self.configuration.bearer_access_token {
            request = request.bearer_auth(token);
        }
        let body = receive_json_response(request, StatusCode::OK)?;
        let session = serde_json::from_slice(&body).map_err(|_| invalid())?;
        validate_session(session, organization, Some(id))
    }

    pub fn connection(
        &self,
        organization: &str,
        id: &str,
    ) -> Result<LinearConnection, LinearFailure> {
        let connection = apis::source_connections_api::get_linear_connection(
            &self.configuration,
            organization,
            id,
        )
        .map_err(classify_error)?;
        validate_connection(connection, organization, Some(id))
    }

    pub fn list(
        &self,
        organization: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<LinearConnectionList, LinearFailure> {
        let page = apis::source_connections_api::list_linear_connections(
            &self.configuration,
            organization,
            limit,
            cursor,
        )
        .map_err(classify_error)?;
        for connection in &page.items {
            validate_connection(connection.clone(), organization, None)?;
        }
        Ok(page)
    }

    pub fn create_trigger(
        &self,
        org: &str,
        project: &str,
        key: &str,
        request: CreateLinearTriggerRequest,
    ) -> Result<LinearTrigger, LinearFailure> {
        let endpoint = self.trigger_endpoint(org, project, None);
        let request = self
            .mutation_request(reqwest::Method::POST, &endpoint, key)
            .json(&request);
        receive_confirmed_json_response(request, StatusCode::CREATED, key)
            .and_then(|value| validate_trigger(value, project, None))
    }

    pub fn update_trigger(
        &self,
        org: &str,
        project: &str,
        id: &str,
        key: &str,
        request: UpdateLinearTriggerRequest,
    ) -> Result<LinearTrigger, LinearFailure> {
        let endpoint = self.trigger_endpoint(org, project, Some(id));
        let request = self
            .mutation_request(reqwest::Method::PATCH, &endpoint, key)
            .json(&request);
        receive_confirmed_json_response(request, StatusCode::OK, key)
            .and_then(|value| validate_trigger(value, project, Some(id)))
    }

    pub fn trigger(
        &self,
        org: &str,
        project: &str,
        id: &str,
    ) -> Result<LinearTriggerRead, LinearFailure> {
        // The generated untagged union's active model has defaulted required fields;
        // it would consume a tombstone before reaching the tombstone variant.
        let endpoint = self.trigger_endpoint(org, project, Some(id));
        let body = receive_json_response(
            super::generated_api_request(&self.configuration, reqwest::Method::GET, &endpoint),
            StatusCode::OK,
        )?;
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| invalid())?;
        if value.get("deleted").is_some() {
            if value.as_object().is_none_or(|m| m.len() != 4)
                || value.get("deleted") != Some(&serde_json::Value::Bool(true))
            {
                return Err(invalid());
            }
            let tombstone: models::LinearTriggerTombstone =
                serde_json::from_value(value).map_err(|_| invalid())?;
            if tombstone.id != id
                || tombstone.project_id != project
                || tombstone.deleted_at.is_empty()
            {
                return Err(invalid());
            }
            Ok(LinearTriggerRead::Deleted(tombstone))
        } else {
            let active: LinearTrigger =
                serde_json::from_value(value.clone()).map_err(|_| invalid())?;
            // Generated models can drop unknown properties (including new mapping
            // vocabulary) rather than fail. Never present a partial configuration.
            if serde_json::to_value(&active).map_err(|_| invalid())? != value {
                return Err(invalid());
            }
            validate_trigger(active, project, Some(id)).map(LinearTriggerRead::Active)
        }
    }

    pub fn triggers(
        &self,
        org: &str,
        project: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<LinearTriggerList, LinearFailure> {
        let endpoint = format!(
            "{}/v1/organizations/{}/projects/{}/triggers",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(org),
            apis::urlencode(project),
        );
        let mut request =
            super::generated_api_request(&self.configuration, reqwest::Method::GET, &endpoint);
        if let Some(limit) = limit {
            request = request.query(&[("limit", limit)]);
        }
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let page: LinearTriggerList = receive_closed_json_response(request, StatusCode::OK)?;
        for item in &page.items {
            validate_trigger(item.clone(), project, None)?;
        }
        Ok(page)
    }

    pub fn trigger_action(
        &self,
        org: &str,
        project: &str,
        id: &str,
        key: &str,
        action: TriggerAction,
    ) -> Result<Option<LinearTrigger>, LinearFailure> {
        let endpoint = self.trigger_endpoint(org, project, Some(id));
        let value = match action {
            TriggerAction::Enable | TriggerAction::Disable => {
                let suffix = if matches!(action, TriggerAction::Enable) {
                    "/enable"
                } else {
                    "/disable"
                };
                let request =
                    self.mutation_request(reqwest::Method::POST, &(endpoint + suffix), key);
                Some(receive_confirmed_json_response(
                    request,
                    StatusCode::OK,
                    key,
                )?)
            }
            TriggerAction::Delete => {
                let request = self.mutation_request(reqwest::Method::DELETE, &endpoint, key);
                let (body, headers) = receive_response(request, StatusCode::NO_CONTENT)?;
                validate_mutation_key(&headers, key)?;
                if !body.is_empty() {
                    return Err(invalid());
                }
                None
            }
        };
        value
            .map(|trigger| validate_trigger(trigger, project, Some(id)))
            .transpose()
    }

    fn trigger_endpoint(&self, org: &str, project: &str, id: Option<&str>) -> String {
        let mut endpoint = format!(
            "{}/v1/organizations/{}/projects/{}/triggers",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(org),
            apis::urlencode(project),
        );
        if let Some(id) = id {
            endpoint.push('/');
            endpoint.push_str(&apis::urlencode(id));
        }
        endpoint
    }

    fn mutation_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        key: &str,
    ) -> reqwest::blocking::RequestBuilder {
        super::generated_api_request(&self.configuration, method, endpoint)
            .header("Idempotency-Key", key)
    }

    pub fn evaluations(
        &self,
        org: &str,
        project: &str,
        trigger: &str,
        filters: EvaluationFilters<'_>,
    ) -> Result<LinearEvaluationList, LinearFailure> {
        let endpoint = format!(
            "{}/v1/organizations/{}/projects/{}/triggers/{}/evaluations",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(org),
            apis::urlencode(project),
            apis::urlencode(trigger),
        );
        let mut request =
            super::generated_api_request(&self.configuration, reqwest::Method::GET, &endpoint);
        if let Some(limit) = filters.limit {
            request = request.query(&[("limit", limit)]);
        }
        for (name, value) in [
            ("cursor", filters.cursor),
            ("state", filters.state),
            ("runId", filters.run_id),
        ] {
            if let Some(value) = value {
                request = request.query(&[(name, value)]);
            }
        }
        let page: LinearEvaluationList = receive_closed_json_response(request, StatusCode::OK)?;
        for item in &page.items {
            validate_evaluation(item.clone(), trigger, &item.id, None)?;
        }
        Ok(page)
    }

    pub fn evaluation(
        &self,
        organization: &str,
        project: &str,
        trigger: &str,
        id: &str,
    ) -> Result<LinearEvaluation, LinearFailure> {
        self.request_evaluation(
            reqwest::Method::GET,
            organization,
            project,
            trigger,
            id,
            None,
        )
        .and_then(|evaluation| validate_evaluation(evaluation, trigger, id, None))
    }

    pub fn retry_evaluation(
        &self,
        organization: &str,
        project: &str,
        trigger: &str,
        id: &str,
        key: &str,
    ) -> Result<LinearEvaluation, LinearFailure> {
        self.request_evaluation(
            reqwest::Method::POST,
            organization,
            project,
            trigger,
            id,
            Some(key),
        )
        .and_then(|evaluation| validate_evaluation(evaluation, trigger, id, Some(2)))
    }

    // The generated client drops error headers. Keep its model while reading
    // Retry-After and bounding both success and problem bodies here.
    fn request_evaluation(
        &self,
        method: reqwest::Method,
        organization: &str,
        project: &str,
        trigger: &str,
        id: &str,
        key: Option<&str>,
    ) -> Result<LinearEvaluation, LinearFailure> {
        let suffix = if key.is_some() { "/retry" } else { "" };
        let endpoint = format!(
            "{}/v1/organizations/{}/projects/{}/triggers/{}/evaluations/{}{}",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization),
            apis::urlencode(project),
            apis::urlencode(trigger),
            apis::urlencode(id),
            suffix
        );
        let expected_status = if key.is_some() {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        };
        let mut request = super::generated_api_request(&self.configuration, method, &endpoint);
        if let Some(key) = key {
            request = request.header("Idempotency-Key", key);
        }
        if let Some(key) = key {
            receive_confirmed_json_response(request, expected_status, key)
        } else {
            receive_closed_json_response(request, expected_status)
        }
    }

    pub fn disconnect(
        &self,
        organization: &str,
        id: &str,
        key: &str,
    ) -> Result<LinearConnection, LinearFailure> {
        let connection = apis::source_connections_api::disconnect_linear_connection(
            &self.configuration,
            organization,
            id,
            key,
        )
        .map_err(classify_error)?;
        validate_connection(connection, organization, Some(id))
    }

    pub fn delete(&self, organization: &str, id: &str, key: &str) -> Result<(), LinearFailure> {
        apis::source_connections_api::delete_linear_connection(
            &self.configuration,
            organization,
            id,
            key,
        )
        .map_err(classify_error)
    }
}

#[derive(Clone, Copy)]
pub enum TriggerAction {
    Enable,
    Disable,
    Delete,
}

fn validate_trigger(
    value: LinearTrigger,
    project: &str,
    id: Option<&str>,
) -> Result<LinearTrigger, LinearFailure> {
    if value.project_id != project
        || id.is_some_and(|id| value.id != id)
        || value.id.is_empty()
        || value.version < 1
        || value.grant_id.is_empty()
        || value.source.connection_id.is_empty()
        || value.target.workflow_path.is_empty()
        || value.target.execution_principal_id.is_empty()
        || !(1..=20).contains(&value.conditions.len())
        || value.created_at.is_empty()
        || value.updated_at.is_empty()
    {
        return Err(invalid());
    }
    Ok(value)
}

impl Drop for LinearApi {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
    }
}

fn receive_closed_json_response<T: serde::de::DeserializeOwned + serde::Serialize>(
    request: reqwest::blocking::RequestBuilder,
    expected_status: StatusCode,
) -> Result<T, LinearFailure> {
    let (body, headers) = receive_response(request, expected_status)?;
    validate_json_media_type(&headers)?;
    decode_closed_json(&body)
}

fn receive_confirmed_json_response<T: serde::de::DeserializeOwned + serde::Serialize>(
    request: reqwest::blocking::RequestBuilder,
    expected_status: StatusCode,
    key: &str,
) -> Result<T, LinearFailure> {
    let (body, headers) = receive_response(request, expected_status)?;
    validate_mutation_key(&headers, key)?;
    validate_json_media_type(&headers)?;
    decode_closed_json(&body)
}

fn validate_mutation_key(
    headers: &reqwest::header::HeaderMap,
    key: &str,
) -> Result<(), LinearFailure> {
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

fn decode_closed_json<T: serde::de::DeserializeOwned + serde::Serialize>(
    body: &[u8],
) -> Result<T, LinearFailure> {
    let raw: serde_json::Value = serde_json::from_slice(body).map_err(|_| invalid())?;
    let decoded: T = serde_json::from_value(raw.clone()).map_err(|_| invalid())?;
    if serde_json::to_value(&decoded).map_err(|_| invalid())? != raw {
        return Err(invalid());
    }
    Ok(decoded)
}

fn receive_json_response(
    request: reqwest::blocking::RequestBuilder,
    expected_status: StatusCode,
) -> Result<Vec<u8>, LinearFailure> {
    let (body, headers) = receive_response(request, expected_status)?;
    validate_json_media_type(&headers)?;
    Ok(body)
}

fn validate_json_media_type(headers: &reqwest::header::HeaderMap) -> Result<(), LinearFailure> {
    http_util::require_header_media_type(
        headers.get(reqwest::header::CONTENT_TYPE),
        problem::JSON_MEDIA_TYPE,
    )
    .map_err(|_| invalid())
}

fn receive_response(
    request: reqwest::blocking::RequestBuilder,
    expected_status: StatusCode,
) -> Result<(Vec<u8>, reqwest::header::HeaderMap), LinearFailure> {
    let response = request.send().map_err(|error| LinearFailure::Unreachable {
        category: classify_reqwest_error(&error),
    })?;
    let status = response.status();
    let headers = response.headers().clone();
    let retry_after = response
        .headers()
        .get("Retry-After")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let body = http_util::read_bounded_blocking_body(response).map_err(|error| match error {
        BoundedBodyError::TooLarge => LinearFailure::InvalidResponse {
            credential_rejected: status == StatusCode::UNAUTHORIZED,
        },
        BoundedBodyError::Transport(error) => LinearFailure::Unreachable {
            category: classify_reqwest_error(&error),
        },
    })?;
    if status != expected_status {
        return Err(classify_response(status, &body, retry_after));
    }
    Ok((body, headers))
}

fn validate_evaluation(
    evaluation: LinearEvaluation,
    trigger: &str,
    id: &str,
    minimum_cycle: Option<i32>,
) -> Result<LinearEvaluation, LinearFailure> {
    if evaluation.id != id
        || evaluation.trigger_id != trigger
        || evaluation.cycle_number < minimum_cycle.unwrap_or(1)
        || evaluation.accepted_at.is_empty()
        || evaluation.grant_id.is_empty()
        || (evaluation.state == LinearEvaluationState::RunCreated) != evaluation.run_id.is_some()
    {
        return Err(invalid());
    }
    Ok(evaluation)
}

fn invalid() -> LinearFailure {
    LinearFailure::InvalidResponse {
        credential_rejected: false,
    }
}

fn validate_connection(
    value: LinearConnection,
    _organization: &str,
    id: Option<&str>,
) -> Result<LinearConnection, LinearFailure> {
    if !um_support::valid_typed_id(&value.id, "lcn_")
        || value.organization_id.is_empty()
        || id.is_some_and(|id| value.id != id)
        || value.lifecycle_generation < 1
        || value.created_at.is_empty()
        || value.updated_at.is_empty()
    {
        return Err(invalid());
    }
    // Organization refs may be slugs; the connection carries the canonical organization ID.
    Ok(value)
}

fn validate_session(
    value: LinearSession,
    organization: &str,
    id: Option<&str>,
) -> Result<LinearSession, LinearFailure> {
    if !um_support::valid_typed_id(&value.id, "las_")
        || value.organization_id.is_empty()
        || id.is_some_and(|id| value.id != id)
        || value.created_at.is_empty()
        || value.expires_at.is_empty()
    {
        return Err(invalid());
    }
    if value.status != models::linear_authorization_session::Status::LinearSessionPending
        && value.authorization_url.is_some()
    {
        return Err(invalid());
    }
    if let Some(url) = &value.authorization_url {
        let parsed = Url::parse(url).map_err(|_| invalid())?;
        if url.chars().any(char::is_control)
            || parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid());
        }
    }
    if let Some(connection) = &value.result_connection {
        validate_connection((**connection).clone(), organization, None)?;
    }
    Ok(value)
}

fn classify_error<E>(error: apis::Error<E>) -> LinearFailure {
    match error {
        apis::Error::Reqwest(error) => LinearFailure::Unreachable {
            category: classify_reqwest_error(&error),
        },
        apis::Error::ResponseError(response) => {
            classify_response(response.status, response.content.as_bytes(), None)
        }
        apis::Error::Serde(_) | apis::Error::Io(_) => invalid(),
    }
}

fn classify_response(status: StatusCode, body: &[u8], retry_after: Option<u64>) -> LinearFailure {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return LinearFailure::RateLimited { retry_after };
    }
    if status.is_server_error() {
        return LinearFailure::Unreachable {
            category: UnreachableCategory::Server,
        };
    }
    let Ok(decoded) = problem::decode(body, status) else {
        return LinearFailure::InvalidResponse {
            credential_rejected: status == StatusCode::UNAUTHORIZED,
        };
    };
    match (status, decoded.r#type.as_str()) {
        (StatusCode::BAD_REQUEST, problem::BAD_REQUEST) => LinearFailure::InvalidInput,
        (StatusCode::UNAUTHORIZED, problem::UNAUTHORIZED) => LinearFailure::Unauthenticated,
        (StatusCode::FORBIDDEN, problem::FORBIDDEN) => LinearFailure::Forbidden,
        (StatusCode::NOT_FOUND, problem::NOT_FOUND) => LinearFailure::NotFound,
        (
            StatusCode::CONFLICT,
            "https://api.usefulmachinery.com/problems/source-connection-conflict",
        ) => LinearFailure::Conflict,
        _ => invalid(),
    }
}

#[derive(Debug)]
pub enum LinearApiError {
    InvalidEndpoint,
    BuildClient(reqwest::Error),
}
impl fmt::Display for LinearApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                f,
                "invalid Linear connection API endpoint; check the deployment URL and HTTP policy"
            ),
            Self::BuildClient(error) => write!(f, "prepare Linear connection networking: {error}"),
        }
    }
}
impl std::error::Error for LinearApiError {}
