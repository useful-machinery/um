//! Nonsecret Linear connection projections over the generated source-connection client.
use std::fmt;

use reqwest::{StatusCode, Url};

use super::generated::{apis, models};
use super::http_client::zeroize_generated_bearer_access_token;
use super::resource::{
    ProblemVocabulary, ResourceFailure, classify_error, invalid, receive_closed_json_response,
    receive_json_response, resource_configuration,
};
use super::{HttpTransportPolicy, problem};

pub type LinearSession = models::LinearAuthorizationSession;
pub type LinearConnection = models::LinearConnection;
pub type LinearConnectionList = models::LinearConnectionList;
pub type LinearTeamList = models::LinearTeamList;
pub type LinearTeamStateList = models::LinearTeamStateList;
pub type LinearSessionStatus = models::linear_authorization_session::Status;

pub struct LinearApi {
    configuration: apis::configuration::Configuration,
}

pub type LinearFailure = ResourceFailure;

const LINEAR: ProblemVocabulary = ProblemVocabulary {
    bad_request: &[problem::BAD_REQUEST],
    unprocessable: &["https://api.usefulmachinery.com/problems/workflow-source-invalid"],
    conflict: &[
        "https://api.usefulmachinery.com/problems/source-connection-conflict",
        problem::IDEMPOTENCY_CONFLICT,
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

impl Drop for LinearApi {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
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

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::expect_used,
    reason = "Linear client unit tests use Rust test assertions and fixture extraction"
)]
mod tests {
    use reqwest::StatusCode;

    use super::super::resource::{ResourceFailure, classify_response, problem_fixture};
    use super::LINEAR;

    fn classify(status: StatusCode, body: &[u8]) -> ResourceFailure {
        classify_response(status, body, None, &LINEAR)
    }

    // REQ-API-7
    #[test]
    fn linear_conflicts_classify_with_the_linear_vocabulary() {
        for kind in [
            "source-connection-conflict",
            "idempotency-conflict",
            "quantity-limit-reached",
            "workflow-source-unavailable",
        ] {
            assert_eq!(
                classify(StatusCode::CONFLICT, &problem_fixture(409, kind)),
                ResourceFailure::Conflict,
                "{kind}"
            );
        }
        assert!(matches!(
            classify(
                StatusCode::CONFLICT,
                &problem_fixture(409, "candidate-conflict")
            ),
            ResourceFailure::InvalidResponse { .. }
        ));
    }
}
