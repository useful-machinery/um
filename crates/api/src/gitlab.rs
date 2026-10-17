//! GitLab connection operations backed by the generated source-connections client.
use std::io::{self, Read};
use std::time::Duration;

use reqwest::{StatusCode, Url};
use serde::Serialize;
use serde_json::Value;
use zeroize::Zeroizing;

use super::generated::{
    apis::{self, source_connections_api as source},
    models,
};
use super::http_client::{generated_configuration, zeroize_generated_bearer_access_token};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error, problem};

const TIMEOUT: Duration = Duration::from_secs(20);

pub struct GitLabApi {
    configuration: apis::configuration::Configuration,
}

// reqwest's buffered JSON body is not erased on drop. Stream a sized, zeroizing
// serialized body instead; the generated request function retains route and wire ownership.
struct SecretReader {
    bytes: Zeroizing<Vec<u8>>,
    offset: usize,
}
impl Read for SecretReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let remaining = &self.bytes[self.offset..];
        let count = remaining.len().min(output.len());
        output[..count].copy_from_slice(&remaining[..count]);
        self.offset += count;
        Ok(count)
    }
}
pub(crate) fn secret_body(
    value: &impl Serialize,
) -> Result<reqwest::blocking::Body, serde_json::Error> {
    let bytes = Zeroizing::new(serde_json::to_vec(value)?);
    let length = bytes.len() as u64;
    Ok(reqwest::blocking::Body::sized(
        SecretReader { bytes, offset: 0 },
        length,
    ))
}

pub struct GitLabRepositoryQuery<'a> {
    pub limit: Option<i32>,
    pub cursor: Option<&'a str>,
    pub search: Option<&'a str>,
    pub repository_id: Option<&'a str>,
    pub selected: bool,
}

pub struct GitLabSelection<'a> {
    pub version: i64,
    pub repository_ids: &'a [String],
    pub link: bool,
    pub delegator: Option<&'a str>,
}

pub struct GitLabSubmission<'a> {
    pub version: i64,
    pub kind: models::submit_git_lab_candidate_request::Kind,
    pub account_use: Option<models::submit_git_lab_candidate_request::AccountUse>,
    pub token: &'a Zeroizing<String>,
    pub delegator: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitLabFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    Conflict,
    CandidateUnavailable,
    IdentityMismatch,
    Unreachable(UnreachableCategory),
    Protocol,
}

impl GitLabFailure {
    pub const fn credential_rejected(&self) -> bool {
        matches!(self, Self::Unauthenticated)
    }
}

fn classify<E>(error: apis::Error<E>) -> GitLabFailure {
    match error {
        apis::Error::Reqwest(error) => GitLabFailure::Unreachable(classify_reqwest_error(&error)),
        apis::Error::ResponseError(response) => {
            let status = response.status;
            if status.is_server_error() {
                return GitLabFailure::Unreachable(UnreachableCategory::Server);
            }
            let Ok(p) = problem::decode(response.content.as_bytes(), status) else {
                return GitLabFailure::Protocol;
            };
            match (status, p.r#type.as_str()) {
                (StatusCode::BAD_REQUEST, problem::BAD_REQUEST) => GitLabFailure::InvalidInput,
                (StatusCode::UNAUTHORIZED, problem::UNAUTHORIZED) => GitLabFailure::Unauthenticated,
                (StatusCode::FORBIDDEN, problem::FORBIDDEN) => GitLabFailure::Forbidden,
                (StatusCode::NOT_FOUND, problem::NOT_FOUND) => GitLabFailure::NotFound,
                (
                    StatusCode::CONFLICT,
                    "https://api.usefulmachinery.com/problems/candidate-conflict"
                    | "https://api.usefulmachinery.com/problems/connection-version-conflict"
                    | "https://api.usefulmachinery.com/problems/connection-limit-reached"
                    | "https://api.usefulmachinery.com/problems/connection-conflict"
                    | "https://api.usefulmachinery.com/problems/cursor-stale"
                    | "https://api.usefulmachinery.com/problems/repository-unavailable"
                    | "https://api.usefulmachinery.com/problems/repository-limit-reached"
                    | "https://api.usefulmachinery.com/problems/idempotency-conflict",
                ) => GitLabFailure::Conflict,
                _ => GitLabFailure::Protocol,
            }
        }
        apis::Error::Serde(_) | apis::Error::Io(_) => GitLabFailure::Protocol,
    }
}

fn document(value: impl Serialize) -> Result<Value, GitLabFailure> {
    serde_json::to_value(value).map_err(|_| GitLabFailure::Protocol)
}

fn subject(value: impl Serialize, expected: &[(&str, &str)]) -> Result<Value, GitLabFailure> {
    let value = document(value)?;
    if expected.iter().all(|(field, id)| value[*field] == *id) {
        Ok(value)
    } else {
        Err(GitLabFailure::Protocol)
    }
}

impl GitLabApi {
    pub fn new(url: &str, token: &str, policy: HttpTransportPolicy) -> Result<Self, &'static str> {
        let parsed = Url::parse(url).map_err(|_| "invalid API endpoint")?;
        if !policy.permits(&parsed)
            || parsed.cannot_be_a_base()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err("invalid API endpoint");
        }
        let configuration = generated_configuration(url, token, policy, TIMEOUT)
            .map_err(|_| "cannot prepare GitLab API networking")?;
        Ok(Self { configuration })
    }
    pub fn create(
        &self,
        org: &str,
        key: &str,
        instance: &str,
        delegator: Option<&str>,
    ) -> Result<Value, GitLabFailure> {
        let mut body = models::CreateGitLabConnectionRequest::new(instance.to_owned());
        body.delegator_principal_id = delegator.map(str::to_owned);
        document(
            source::create_git_lab_connection(&self.configuration, org, key, body)
                .map_err(classify)?,
        )
    }
    pub fn list(
        &self,
        org: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<Value, GitLabFailure> {
        document(
            source::list_git_lab_connections(&self.configuration, org, limit, cursor)
                .map_err(classify)?,
        )
    }
    pub fn show(&self, org: &str, id: &str) -> Result<Value, GitLabFailure> {
        subject(
            source::get_git_lab_connection(&self.configuration, org, id).map_err(classify)?,
            &[("id", id)],
        )
    }
    pub fn repositories(
        &self,
        org: &str,
        id: &str,
        query: GitLabRepositoryQuery<'_>,
    ) -> Result<Value, GitLabFailure> {
        document(
            source::list_git_lab_repositories(
                &self.configuration,
                org,
                id,
                query.limit,
                query.cursor,
                query.search,
                query.repository_id,
                query.selected.then_some(true),
            )
            .map_err(classify)?,
        )
    }
    pub fn select_repository(
        &self,
        org: &str,
        id: &str,
        key: &str,
        selection: GitLabSelection<'_>,
    ) -> Result<Value, GitLabFailure> {
        let mut body = models::PatchGitLabRepositorySelectionRequest::new(selection.version);
        if selection.link {
            body.add_provider_repository_ids = Some(selection.repository_ids.to_vec());
        } else {
            body.remove_provider_repository_ids = Some(selection.repository_ids.to_vec());
        }
        body.delegator_principal_id = selection.delegator.map(str::to_owned);
        subject(
            source::patch_git_lab_repository_selection(&self.configuration, org, id, key, body)
                .map_err(classify)?,
            &[("id", id)],
        )
    }
    pub fn remove(
        &self,
        org: &str,
        id: &str,
        key: &str,
        version: i64,
        delegator: Option<&str>,
    ) -> Result<Value, GitLabFailure> {
        let mut body = models::DisconnectGitLabConnectionRequest::new(version);
        body.delegator_principal_id = delegator.map(str::to_owned);
        subject(
            source::disconnect_git_lab_connection(&self.configuration, org, id, key, body)
                .map_err(classify)?,
            &[("id", id)],
        )
    }
    pub fn submit(
        &self,
        org: &str,
        id: &str,
        key: &str,
        input: GitLabSubmission<'_>,
    ) -> Result<Value, GitLabFailure> {
        let mut body = models::SubmitGitLabCandidateRequest::new(
            input.version,
            input.kind,
            input.token.to_string(),
        );
        body.account_use = input.account_use;
        body.delegator_principal_id = input.delegator.map(str::to_owned);
        // The generated client consumes the request. Keep the input in zeroizing storage and
        // never format a generated error, whose response may contain untrusted provider text.
        let result = source::submit_git_lab_candidate(&self.configuration, org, id, key, body);
        subject(result.map_err(classify)?, &[("connectionId", id)])
    }
    pub fn candidate(&self, org: &str, id: &str, candidate: &str) -> Result<Value, GitLabFailure> {
        subject(
            source::get_git_lab_candidate(&self.configuration, org, id, candidate)
                .map_err(classify)?,
            &[("id", candidate), ("connectionId", id)],
        )
    }
    pub fn activate(
        &self,
        org: &str,
        id: &str,
        candidate: &str,
        key: &str,
        user: &str,
    ) -> Result<Value, GitLabFailure> {
        subject(
            source::activate_git_lab_candidate(
                &self.configuration,
                org,
                id,
                candidate,
                key,
                models::ActivateGitLabCandidateRequest::new(user.to_owned(), true),
            )
            .map_err(classify)?,
            &[("id", id)],
        )
    }
    pub fn cancel(
        &self,
        org: &str,
        id: &str,
        candidate: &str,
        key: &str,
    ) -> Result<Value, GitLabFailure> {
        source::cancel_git_lab_candidate(&self.configuration, org, id, candidate, key)
            .map_err(classify)?;
        // 204 acknowledges the request even for an already-terminal candidate. In
        // particular, cancellation cannot undo an activated credential.
        Ok(serde_json::json!({"id":candidate,"connectionId":id}))
    }
}
impl Drop for GitLabApi {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
    }
}
