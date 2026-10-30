//! Nonsecret Linear connection projections over the generated source-connection client.
use std::fmt;

use reqwest::{StatusCode, Url};

use super::generated::{apis, models};
use super::http_client::zeroize_generated_bearer_access_token;
use super::resource::{
    LifecycleAction, ProblemVocabulary, ResourceFailure, classify_error, decode_closed_json,
    invalid, mutation_request, receive_closed_json_response, receive_confirmed_json_response,
    receive_json_response, receive_response, resource_configuration, validate_mutation_key,
};
use super::{HttpTransportPolicy, problem};

pub type LinearSession = models::LinearAuthorizationSession;
pub type LinearConnection = models::LinearConnection;
pub type LinearConnectionList = models::LinearConnectionList;
pub type LinearTeamList = models::LinearTeamList;
pub type LinearTeamStateList = models::LinearTeamStateList;
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

pub type LinearFailure = ResourceFailure;
pub type TriggerAction = LifecycleAction;

const LINEAR: ProblemVocabulary = ProblemVocabulary {
    bad_request: &[problem::BAD_REQUEST],
    unprocessable: &["https://api.usefulmachinery.com/problems/workflow-source-invalid"],
    conflict: &[
        "https://api.usefulmachinery.com/problems/source-connection-conflict",
        problem::QUANTITY_LIMIT_REACHED,
        "https://api.usefulmachinery.com/problems/workflow-source-unavailable",
    ],
};

impl LinearApi {
    pub fn new(
        api_url: &str,
        token: &str,
        policy: HttpTransportPolicy,
    ) -> Result<Self, LinearApiError> {
        let configuration = resource_configuration(
            api_url,
            token,
            policy,
            LinearApiError::InvalidEndpoint,
            LinearApiError::BuildClient,
        )?;
        Ok(Self { configuration })
    }

    pub fn start(
        &self,
        organization: &str,
        connection_id: Option<&str>,
        key: &str,
        enable_state_updates: bool,
    ) -> Result<LinearSession, LinearFailure> {
        let mut request =
            models::CreateLinearAuthorizationSessionRequest::new(match connection_id {
                Some(_) => {
                    models::create_linear_authorization_session_request::Operation::Reauthorize
                }
                None => models::create_linear_authorization_session_request::Operation::Connect,
            });
        request.connection_id = connection_id.map(str::to_owned);
        if enable_state_updates {
            request.enable_state_updates = Some(true);
        }
        let session = apis::source_connections_api::create_linear_authorization_session(
            &self.configuration,
            organization,
            key,
            request,
        )
        .map_err(|error| classify_error(error, &LINEAR))?;
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
        let body = receive_json_response(request, StatusCode::OK, &LINEAR)?;
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
        .map_err(|error| classify_error(error, &LINEAR))?;
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
        .map_err(|error| classify_error(error, &LINEAR))?;
        for connection in &page.items {
            validate_connection(connection.clone(), organization, None)?;
        }
        Ok(page)
    }

    pub fn teams(
        &self,
        organization: &str,
        connection: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<LinearTeamList, LinearFailure> {
        let endpoint = self.discovery_endpoint(organization, connection, None, limit, cursor)?;
        let page: LinearTeamList = receive_closed_json_response(
            super::generated_api_request(&self.configuration, reqwest::Method::GET, &endpoint),
            StatusCode::OK,
            &LINEAR,
        )?;
        if page
            .items
            .iter()
            .any(|team| team.id.is_empty() || team.name.is_empty())
        {
            return Err(invalid());
        }
        Ok(page)
    }

    pub fn states(
        &self,
        organization: &str,
        connection: &str,
        team: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<LinearTeamStateList, LinearFailure> {
        let endpoint =
            self.discovery_endpoint(organization, connection, Some(team), limit, cursor)?;
        let page: LinearTeamStateList = receive_closed_json_response(
            super::generated_api_request(&self.configuration, reqwest::Method::GET, &endpoint),
            StatusCode::OK,
            &LINEAR,
        )?;
        if page
            .items
            .iter()
            .any(|state| state.id.is_empty() || state.name.is_empty())
        {
            return Err(invalid());
        }
        for suggestion in [
            &page.preset_suggestions.started,
            &page.preset_suggestions.succeeded,
            &page.preset_suggestions.failed,
        ] {
            use crate::generated::models::linear_preset_suggestion::Status;
            if (suggestion.status == Status::Suggested) != suggestion.state_id.is_some() {
                return Err(invalid());
            }
        }
        Ok(page)
    }

    fn discovery_endpoint(
        &self,
        organization: &str,
        connection: &str,
        team: Option<&str>,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<String, LinearFailure> {
        let mut endpoint = format!(
            "{}/v1/organizations/{}/connections/linear/{}/teams",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization),
            apis::urlencode(connection),
        );
        if let Some(team) = team {
            endpoint.push('/');
            endpoint.push_str(&apis::urlencode(team));
            endpoint.push_str("/states");
        }
        let mut url = Url::parse(&endpoint).map_err(|_| invalid())?;
        if limit.is_some() || cursor.is_some() {
            let mut query = url.query_pairs_mut();
            if let Some(limit) = limit {
                query.append_pair("limit", &limit.to_string());
            }
            if let Some(cursor) = cursor {
                query.append_pair("cursor", cursor);
            }
        }
        Ok(url.to_string())
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
        receive_confirmed_json_response(request, StatusCode::CREATED, key, &LINEAR)
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
        receive_confirmed_json_response(request, StatusCode::OK, key, &LINEAR)
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
            &LINEAR,
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
            // Generated models can drop unknown properties (including new mapping
            // vocabulary) rather than fail; the closed decode never presents a partial
            // configuration.
            let active: LinearTrigger = decode_closed_json(&body)?;
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
        let page: LinearTriggerList =
            receive_closed_json_response(request, StatusCode::OK, &LINEAR)?;
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
                    &LINEAR,
                )?)
            }
            TriggerAction::Delete => {
                let request = self.mutation_request(reqwest::Method::DELETE, &endpoint, key);
                let (body, headers) = receive_response(request, StatusCode::NO_CONTENT, &LINEAR)?;
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
        mutation_request(&self.configuration, method, endpoint, key)
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
        let page: LinearEvaluationList =
            receive_closed_json_response(request, StatusCode::OK, &LINEAR)?;
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
            receive_confirmed_json_response(request, expected_status, key, &LINEAR)
        } else {
            receive_closed_json_response(request, expected_status, &LINEAR)
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
        .map_err(|error| classify_error(error, &LINEAR))?;
        validate_connection(connection, organization, Some(id))
    }

    pub fn delete(&self, organization: &str, id: &str, key: &str) -> Result<(), LinearFailure> {
        apis::source_connections_api::delete_linear_connection(
            &self.configuration,
            organization,
            id,
            key,
        )
        .map_err(|error| classify_error(error, &LINEAR))
    }
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
