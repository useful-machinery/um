//! Cloud workflow definitions and the trees they publish (REQ-CLOUDDEF-1 to REQ-CLOUDDEF-11),
//! for `um workflow definition publish --cloud` (REQ-CLOUDDEF-14) and the `um workflow
//! definition cloud` group (REQ-CLOUDDEF-15).
//!
//! As for the other resource clients, requests go through the generated configuration and
//! models with a hand-written client: the generated one drops error headers and tolerates
//! unknown members, so every response is decoded closed, a keyed mutation must echo its
//! `Idempotency-Key`, and a refusal keeps its problem type's slug and the `workflow` safe
//! facts for the caller to report.
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use base64::Engine as _;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, IF_NONE_MATCH};
use reqwest::{Method, StatusCode, Url};

use super::generated::{apis, models};
use super::http_client::{SignedStorageRequestError, zeroize_generated_bearer_access_token};
use super::resource::{
    RawResponse, ResourceFailure, decode_closed_json, mutation_request, receive_raw,
    resource_configuration, validate_json_media_type, validate_mutation_key,
};
use super::{HttpClient, UnreachableCategory, problem};

pub type WorkflowDefinition = models::WorkflowDefinition;
pub type WorkflowDefinitionEvaluation = models::WorkflowDefinitionEvaluation;
pub type WorkflowDefinitionPublication = models::WorkflowDefinitionPublication;
pub type WorkflowDefinitionRevision = models::WorkflowDefinitionRevision;
pub type WorkflowProblemDetail = models::WorkflowProblemDetail;
pub type WorkflowTree = models::WorkflowTree;
/// Binding values keyed by binding key: a string, or for `members` an array of strings.
pub type WorkflowBindingValues = HashMap<String, serde_json::Value>;

const STORAGE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const PROBLEM_TYPE_PREFIX: &str = "https://api.usefulmachinery.com/problems/";
const DEFINITION_ID_PREFIX: &str = "def_";
const EVALUATION_ID_PREFIX: &str = "wev_";

/// One file of a tree to upload: its tree-relative path, bytes and owner-execute bit.
#[derive(Clone, Copy)]
pub struct WorkflowTreeFile<'a> {
    pub path: &'a str,
    pub bytes: &'a [u8],
    pub executable: bool,
}

impl fmt::Debug for WorkflowTreeFile<'_> {
    /// The length, never the contents.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowTreeFile")
            .field("path", &self.path)
            .field("length", &self.bytes.len())
            .field("executable", &self.executable)
            .finish()
    }
}

/// What a create or publish records: the sealed tree, `sha256:<hex>`, the bindings, and the
/// tree's files the revision runs executable, in bytewise order; omitted bindings take the
/// root's defaults on create and the current snapshot on publish, and omitted executable
/// paths keep the bits the tree was sealed with.
#[derive(Clone, Copy, Debug)]
pub struct WorkflowPublication<'a> {
    pub tree_digest: &'a str,
    pub bindings: Option<&'a WorkflowBindingValues>,
    pub executable_paths: Option<&'a [String]>,
}

/// The exact filters of a definition's evaluation list (REQ-TRIGGER-132): an evaluation
/// state and the run an evaluation created.
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkflowEvaluationFilter<'a> {
    pub state: Option<&'a str>,
    pub run_id: Option<&'a str>,
}

/// A keyed change to a definition's settings (REQ-CLOUDDEF-9 to REQ-CLOUDDEF-11).
#[derive(Clone, Copy)]
enum Change<'a> {
    Enable,
    Disable,
    Archive,
    Unarchive,
    /// Sets the execution principal to this principal id.
    ExecutionPrincipal(&'a str),
}

/// A problem response the Gateway answered: its HTTP status, the slug its `type` ends in,
/// and the `workflow` safe facts it carried.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowRefusal {
    pub status: u16,
    pub problem: String,
    pub workflow: Option<WorkflowProblemDetail>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowDefinitionFailure {
    Unauthenticated,
    Refused(Box<WorkflowRefusal>),
    /// Signed storage refused a file upload with this status.
    UploadRejected {
        status: u16,
    },
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

impl WorkflowDefinitionFailure {
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

const fn invalid() -> WorkflowDefinitionFailure {
    WorkflowDefinitionFailure::InvalidResponse {
        credential_rejected: false,
    }
}

pub struct WorkflowDefinitionApi<'a> {
    configuration: apis::configuration::Configuration,
    storage: &'a HttpClient,
}

impl<'a> WorkflowDefinitionApi<'a> {
    /// A client of the deployment at `api_url` under `storage`'s transport policy, which also
    /// carries the signed storage uploads.
    pub fn new(
        api_url: &str,
        token: &str,
        storage: &'a HttpClient,
    ) -> Result<Self, WorkflowDefinitionApiError> {
        let configuration = resource_configuration(
            api_url,
            token,
            storage.transport_policy(),
            WorkflowDefinitionApiError::InvalidEndpoint,
            WorkflowDefinitionApiError::BuildClient,
        )?;
        Ok(Self {
            configuration,
            storage,
        })
    }

    /// Uploads and seals the tree `digest` (`sha256:<hex>`) of `files` (REQ-CLOUDDEF-3):
    /// posts the manifest, puts each file the organization does not retain to its signed
    /// capability, and seals the tree under `key` unless the Gateway answered it sealed. The
    /// sealed tree, with whether the Gateway already held it sealed, so that nothing was
    /// uploaded or sealed (REQ-CLOUDDEF-22).
    pub fn upload_tree(
        &self,
        organization: &str,
        digest: &str,
        files: &[WorkflowTreeFile<'_>],
        key: &str,
    ) -> Result<(WorkflowTree, bool), WorkflowDefinitionFailure> {
        let manifest = models::WorkflowTreeManifest::new(
            digest.to_owned(),
            files
                .iter()
                .map(|file| {
                    let size = i64::try_from(file.bytes.len()).map_err(|_| invalid())?;
                    Ok(models::WorkflowTreeManifestFile::new(
                        file.path.to_owned(),
                        size,
                        lowercase_hex(&sha256(file.bytes)),
                        file.executable,
                    ))
                })
                .collect::<Result<Vec<_>, WorkflowDefinitionFailure>>()?,
        );
        let endpoint = self.endpoint(organization, &["workflow-trees"]);
        let request = super::generated_api_request(&self.configuration, Method::POST, &endpoint)
            .json(&manifest);
        let issued: models::WorkflowTreeUploadCapabilities =
            receive(request, StatusCode::OK, None)?;
        if issued.tree.digest != digest {
            return Err(invalid());
        }
        for capability in &issued.capabilities {
            let file = files
                .iter()
                .zip(&manifest.files)
                .find(|(_, entry)| entry.sha256 == capability.sha256)
                .map(|(file, _)| file)
                .ok_or_else(invalid)?;
            self.put_object(capability, file.bytes)?;
        }
        if issued.tree.state == models::workflow_tree::State::Sealed {
            return Ok((*issued.tree, true));
        }
        let endpoint = self.endpoint(organization, &["workflow-trees", digest, "seal"]);
        let request = mutation_request(&self.configuration, Method::POST, &endpoint, key);
        let sealed: WorkflowTree = receive(request, StatusCode::OK, Some(key))?;
        if sealed.digest != digest || sealed.state != models::workflow_tree::State::Sealed {
            return Err(invalid());
        }
        Ok((sealed, false))
    }

    /// The definition `id` of `project` (REQ-CLOUDDEF-8).
    pub fn definition(
        &self,
        organization: &str,
        project: &str,
        id: &str,
    ) -> Result<WorkflowDefinition, WorkflowDefinitionFailure> {
        let endpoint = self.definitions_endpoint(organization, project, &[id]);
        let request = super::generated_api_request(&self.configuration, Method::GET, &endpoint);
        let definition: WorkflowDefinition = receive(request, StatusCode::OK, None)?;
        validate_definition(&definition, project, Some(id))?;
        Ok(definition)
    }

    /// One page of `project`'s definitions in id order, with the next page's cursor
    /// (REQ-CLOUDDEF-1); `limit` and `cursor` select the page.
    pub fn definitions(
        &self,
        organization: &str,
        project: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<(Vec<WorkflowDefinition>, Option<String>), WorkflowDefinitionFailure> {
        let endpoint = self.definitions_endpoint(organization, project, &[]);
        let list: models::WorkflowDefinitionList = receive(
            self.page_request(&endpoint, limit, cursor),
            StatusCode::OK,
            None,
        )?;
        for definition in &list.items {
            validate_definition(definition, project, None)?;
        }
        Ok((list.items, list.next_cursor))
    }

    /// One page of definition `id`'s revisions, newest first, with the next page's cursor
    /// (REQ-CLOUDDEF-8).
    pub fn revisions(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<(Vec<WorkflowDefinitionRevision>, Option<String>), WorkflowDefinitionFailure> {
        let endpoint = self.definitions_endpoint(organization, project, &[id, "revisions"]);
        let list: models::WorkflowDefinitionRevisionList = receive(
            self.page_request(&endpoint, limit, cursor),
            StatusCode::OK,
            None,
        )?;
        for revision in &list.items {
            validate_revision(revision, id, None)?;
        }
        Ok((list.items, list.next_cursor))
    }

    /// Revision `number` of definition `id` (REQ-CLOUDDEF-8).
    pub fn revision(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        number: i32,
    ) -> Result<WorkflowDefinitionRevision, WorkflowDefinitionFailure> {
        let number_segment = number.to_string();
        let endpoint =
            self.definitions_endpoint(organization, project, &[id, "revisions", &number_segment]);
        let request = super::generated_api_request(&self.configuration, Method::GET, &endpoint);
        let revision: WorkflowDefinitionRevision = receive(request, StatusCode::OK, None)?;
        validate_revision(&revision, id, Some(number))?;
        Ok(revision)
    }

    /// One page of definition `id`'s retained start evaluations, newest first, with the next
    /// page's cursor (REQ-TRIGGER-132); `filter`, `limit` and `cursor` select the page.
    pub fn evaluations(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        filter: WorkflowEvaluationFilter<'_>,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<(Vec<WorkflowDefinitionEvaluation>, Option<String>), WorkflowDefinitionFailure>
    {
        let endpoint = self.definitions_endpoint(organization, project, &[id, "evaluations"]);
        let mut request = self.page_request(&endpoint, limit, cursor);
        if let Some(state) = filter.state {
            request = request.query(&[("state", state)]);
        }
        if let Some(run_id) = filter.run_id {
            request = request.query(&[("runId", run_id)]);
        }
        let list: models::WorkflowDefinitionEvaluationList =
            receive(request, StatusCode::OK, None)?;
        for evaluation in &list.items {
            validate_evaluation(evaluation, id, None)?;
        }
        Ok((list.items, list.next_cursor))
    }

    /// The start evaluation `evaluation` of definition `id` (REQ-TRIGGER-132).
    pub fn evaluation(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        evaluation: &str,
    ) -> Result<WorkflowDefinitionEvaluation, WorkflowDefinitionFailure> {
        let endpoint =
            self.definitions_endpoint(organization, project, &[id, "evaluations", evaluation]);
        let request = super::generated_api_request(&self.configuration, Method::GET, &endpoint);
        let read: WorkflowDefinitionEvaluation = receive(request, StatusCode::OK, None)?;
        validate_evaluation(&read, id, Some(evaluation))?;
        Ok(read)
    }

    /// Turns on automatic starts of definition `id` under `key` (REQ-CLOUDDEF-9).
    pub fn enable(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        key: &str,
    ) -> Result<WorkflowDefinition, WorkflowDefinitionFailure> {
        self.change(organization, project, id, key, Change::Enable)
    }

    /// Turns off automatic starts of definition `id` under `key`.
    pub fn disable(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        key: &str,
    ) -> Result<WorkflowDefinition, WorkflowDefinitionFailure> {
        self.change(organization, project, id, key, Change::Disable)
    }

    /// Archives definition `id` under `key` (REQ-CLOUDDEF-10).
    pub fn archive(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        key: &str,
    ) -> Result<WorkflowDefinition, WorkflowDefinitionFailure> {
        self.change(organization, project, id, key, Change::Archive)
    }

    /// Restores archived definition `id` under `key` (REQ-CLOUDDEF-10).
    pub fn unarchive(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        key: &str,
    ) -> Result<WorkflowDefinition, WorkflowDefinitionFailure> {
        self.change(organization, project, id, key, Change::Unarchive)
    }

    /// Sets the execution principal of definition `id` to `principal` under `key`
    /// (REQ-CLOUDDEF-11).
    pub fn set_execution_principal(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        key: &str,
        principal: &str,
    ) -> Result<WorkflowDefinition, WorkflowDefinitionFailure> {
        self.change(
            organization,
            project,
            id,
            key,
            Change::ExecutionPrincipal(principal),
        )
    }

    /// Applies `change` to definition `id` under `key`: the definition as the change left
    /// it, which must show the change.
    fn change(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        key: &str,
        change: Change<'_>,
    ) -> Result<WorkflowDefinition, WorkflowDefinitionFailure> {
        let (method, action) = match change {
            Change::Enable => (Method::POST, "enable"),
            Change::Disable => (Method::POST, "disable"),
            Change::Archive => (Method::POST, "archive"),
            Change::Unarchive => (Method::POST, "unarchive"),
            Change::ExecutionPrincipal(_) => (Method::PUT, "execution-principal"),
        };
        let endpoint = self.definitions_endpoint(organization, project, &[id, action]);
        let mut request = mutation_request(&self.configuration, method, &endpoint, key);
        if let Change::ExecutionPrincipal(principal) = change {
            request = request.json(
                &models::SetWorkflowDefinitionExecutionPrincipalRequest::new(principal.to_owned()),
            );
        }
        let definition: WorkflowDefinition = receive(request, StatusCode::OK, Some(key))?;
        validate_definition(&definition, project, Some(id))?;
        let archived = definition.state == models::workflow_definition::State::Archived;
        let shown = match change {
            Change::Enable => definition.automatic_starts && !archived,
            Change::Disable => !definition.automatic_starts,
            Change::Archive => archived && !definition.automatic_starts,
            Change::Unarchive => !archived,
            Change::ExecutionPrincipal(principal) => {
                definition.execution_principal_id.as_deref() == Some(principal)
            }
        };
        if !shown {
            return Err(invalid());
        }
        Ok(definition)
    }

    /// Creates a definition of `project` with revision 1 of the sealed tree (REQ-CLOUDDEF-4)
    /// or, when `reuse_identical`, answers the project's definition whose current revision
    /// is identical to the publication (REQ-CLOUDDEF-21): the publication, with whether it is
    /// such a reused definition. A create that did not ask for reuse never sends the member,
    /// and a reuse answered to it is an invalid response.
    pub fn create_definition(
        &self,
        organization: &str,
        project: &str,
        key: &str,
        publication: WorkflowPublication<'_>,
        reuse_identical: bool,
    ) -> Result<(WorkflowDefinitionPublication, bool), WorkflowDefinitionFailure> {
        let mut body =
            models::CreateWorkflowDefinitionRequest::new(publication.tree_digest.to_owned());
        body.bindings = publication.bindings.cloned();
        body.executable_paths = publication.executable_paths.map(<[String]>::to_vec);
        body.reuse_identical = reuse_identical.then_some(true);
        let endpoint = self.definitions_endpoint(organization, project, &[]);
        let request =
            mutation_request(&self.configuration, Method::POST, &endpoint, key).json(&body);
        let accepted: &[StatusCode] = if reuse_identical {
            &[StatusCode::CREATED, StatusCode::OK]
        } else {
            &[StatusCode::CREATED]
        };
        let (status, published): (_, WorkflowDefinitionPublication) =
            receive_one_of(request, accepted, Some(key))?;
        let reused = status == StatusCode::OK;
        // A reuse answers the definition's current revision, whatever its number.
        let revision = if reused {
            published.definition.current_revision
        } else {
            1
        };
        validate_publication(&published, project, None, revision, publication.tree_digest)?;
        Ok((published, reused))
    }

    /// Publishes the next revision of definition `id` over `expected_current_revision`
    /// (REQ-CLOUDDEF-5); a later current is refused 409 `definition-revision-conflict`.
    pub fn publish_revision(
        &self,
        organization: &str,
        project: &str,
        id: &str,
        key: &str,
        expected_current_revision: i32,
        publication: WorkflowPublication<'_>,
    ) -> Result<WorkflowDefinitionPublication, WorkflowDefinitionFailure> {
        let mut body =
            models::PublishWorkflowDefinitionRevisionRequest::new(expected_current_revision);
        body.tree_digest = Some(publication.tree_digest.to_owned());
        body.bindings = publication.bindings.cloned();
        body.executable_paths = publication.executable_paths.map(<[String]>::to_vec);
        let next = expected_current_revision
            .checked_add(1)
            .ok_or_else(invalid)?;
        let endpoint = self.definitions_endpoint(organization, project, &[id, "revisions"]);
        let request =
            mutation_request(&self.configuration, Method::POST, &endpoint, key).json(&body);
        let published = receive(request, StatusCode::CREATED, Some(key))?;
        validate_publication(&published, project, Some(id), next, publication.tree_digest)?;
        Ok(published)
    }

    fn put_object(
        &self,
        capability: &models::WorkflowTreeUploadCapability,
        bytes: &[u8],
    ) -> Result<(), WorkflowDefinitionFailure> {
        let required = &capability.required_headers;
        if required.content_length != bytes.len().to_string()
            || required.if_none_match
                != models::run_input_upload_required_headers::IfNoneMatch::Star
            || required.x_amz_checksum_sha256
                != base64::engine::general_purpose::STANDARD.encode(sha256(bytes))
        {
            return Err(invalid());
        }
        let url = Url::parse(&capability.url).map_err(|_| invalid())?;
        if !self.storage.transport_policy().permits(&url)
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid());
        }
        let header = |value: &str| HeaderValue::from_str(value).map_err(|_| invalid());
        let mut headers = HeaderMap::with_capacity(4);
        headers.insert(CONTENT_LENGTH, header(&required.content_length)?);
        headers.insert(CONTENT_TYPE, header(&required.content_type)?);
        headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        headers.insert(
            "x-amz-checksum-sha256",
            header(&required.x_amz_checksum_sha256)?,
        );
        let status =
            self.storage
                .signed_storage_put(&url, headers, bytes, STORAGE_REQUEST_TIMEOUT)
                .map_err(|error| match error {
                    SignedStorageRequestError::Build
                    | SignedStorageRequestError::InvalidRequest => invalid(),
                    SignedStorageRequestError::Unreachable(category) => {
                        WorkflowDefinitionFailure::Unreachable { category }
                    }
                })?;
        // An object already present fails `If-None-Match: *`; the seal confirms its bytes.
        if matches!(
            status,
            StatusCode::OK
                | StatusCode::CREATED
                | StatusCode::NO_CONTENT
                | StatusCode::PRECONDITION_FAILED
        ) {
            Ok(())
        } else if status.is_server_error() {
            Err(WorkflowDefinitionFailure::Unreachable {
                category: UnreachableCategory::Server,
            })
        } else {
            Err(WorkflowDefinitionFailure::UploadRejected {
                status: status.as_u16(),
            })
        }
    }

    fn page_request(
        &self,
        endpoint: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> reqwest::blocking::RequestBuilder {
        let mut request = super::generated_api_request(&self.configuration, Method::GET, endpoint);
        if let Some(limit) = limit {
            request = request.query(&[("limit", limit)]);
        }
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        request
    }

    /// `{base}/v1/organizations/{organization}[/{segment}…]`.
    fn endpoint(&self, organization: &str, segments: &[&str]) -> String {
        let mut endpoint = format!(
            "{}/v1/organizations/{}",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization),
        );
        for segment in segments {
            endpoint.push('/');
            endpoint.push_str(&apis::urlencode(segment));
        }
        endpoint
    }

    fn definitions_endpoint(&self, organization: &str, project: &str, segments: &[&str]) -> String {
        let mut path = vec!["projects", project, "workflow-definitions"];
        path.extend_from_slice(segments);
        self.endpoint(organization, &path)
    }
}

impl Drop for WorkflowDefinitionApi<'_> {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
    }
}

/// The closed JSON body of an `expected` response, its key confirmed when `key` is given;
/// any other status is classified.
fn receive<T: serde::de::DeserializeOwned + serde::Serialize>(
    request: reqwest::blocking::RequestBuilder,
    expected: StatusCode,
    key: Option<&str>,
) -> Result<T, WorkflowDefinitionFailure> {
    receive_one_of(request, &[expected], key).map(|(_, body)| body)
}

/// The status and closed JSON body of a response whose status is one of `accepted`, its key
/// confirmed when `key` is given; any other status is classified.
fn receive_one_of<T: serde::de::DeserializeOwned + serde::Serialize>(
    request: reqwest::blocking::RequestBuilder,
    accepted: &[StatusCode],
    key: Option<&str>,
) -> Result<(StatusCode, T), WorkflowDefinitionFailure> {
    let response = receive_raw(request).map_err(|failure| match failure {
        ResourceFailure::Unreachable { category } => {
            WorkflowDefinitionFailure::Unreachable { category }
        }
        ResourceFailure::InvalidResponse {
            credential_rejected,
        } => WorkflowDefinitionFailure::InvalidResponse {
            credential_rejected,
        },
        _ => invalid(),
    })?;
    if !accepted.contains(&response.status) {
        return Err(classify(&response));
    }
    if let Some(key) = key {
        validate_mutation_key(&response.headers, key).map_err(|_| invalid())?;
    }
    validate_json_media_type(&response.headers).map_err(|_| invalid())?;
    let body = decode_closed_json(&response.body).map_err(|_| invalid())?;
    Ok((response.status, body))
}

fn classify(response: &RawResponse) -> WorkflowDefinitionFailure {
    let status = response.status;
    if status == StatusCode::TOO_MANY_REQUESTS {
        return WorkflowDefinitionFailure::RateLimited {
            retry_after: response.retry_after,
        };
    }
    let decoded = problem::decode_header_parts(
        &response.body,
        status,
        response.headers.get(reqwest::header::CONTENT_TYPE),
    );
    let Ok(decoded) = decoded else {
        return if status.is_server_error() {
            WorkflowDefinitionFailure::Unreachable {
                category: UnreachableCategory::Server,
            }
        } else {
            WorkflowDefinitionFailure::InvalidResponse {
                credential_rejected: status == StatusCode::UNAUTHORIZED,
            }
        };
    };
    if status == StatusCode::UNAUTHORIZED {
        return if decoded.r#type == problem::UNAUTHORIZED {
            WorkflowDefinitionFailure::Unauthenticated
        } else {
            WorkflowDefinitionFailure::InvalidResponse {
                credential_rejected: true,
            }
        };
    }
    match decoded.r#type.strip_prefix(PROBLEM_TYPE_PREFIX) {
        Some(slug)
            if (status.is_client_error() || status.is_server_error())
                && !slug.is_empty()
                && slug.bytes().all(|byte| {
                    byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                }) =>
        {
            WorkflowDefinitionFailure::Refused(Box::new(WorkflowRefusal {
                status: status.as_u16(),
                problem: slug.to_owned(),
                workflow: decoded.workflow.map(|detail| *detail),
            }))
        }
        _ => invalid(),
    }
}

fn validate_definition(
    value: &WorkflowDefinition,
    project: &str,
    id: Option<&str>,
) -> Result<(), WorkflowDefinitionFailure> {
    if !um_support::valid_typed_id(&value.id, DEFINITION_ID_PREFIX)
        || id.is_some_and(|id| value.id != id)
        || value.project_id != project
        || value.current_revision < 1
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_revision(
    value: &WorkflowDefinitionRevision,
    id: &str,
    number: Option<i32>,
) -> Result<(), WorkflowDefinitionFailure> {
    if value.definition_id != id
        || value.number < 1
        || number.is_some_and(|number| value.number != number)
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_evaluation(
    value: &WorkflowDefinitionEvaluation,
    definition: &str,
    id: Option<&str>,
) -> Result<(), WorkflowDefinitionFailure> {
    if !um_support::valid_typed_id(&value.id, EVALUATION_ID_PREFIX)
        || id.is_some_and(|id| value.id != id)
        || value.definition_id != definition
        || value.revision < 1
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_publication(
    value: &WorkflowDefinitionPublication,
    project: &str,
    id: Option<&str>,
    revision: i32,
    tree_digest: &str,
) -> Result<(), WorkflowDefinitionFailure> {
    validate_definition(&value.definition, project, id)?;
    if value.revision.definition_id != value.definition.id
        || value.revision.number != revision
        || value.definition.current_revision != revision
        || value.revision.tree_digest != tree_digest
    {
        return Err(invalid());
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    super::run_inputs::digest_bytes(bytes)
}

fn lowercase_hex(bytes: &[u8; 32]) -> String {
    um_support::lowercase_hex(bytes)
}

#[derive(Debug)]
pub enum WorkflowDefinitionApiError {
    InvalidEndpoint,
    BuildClient(reqwest::Error),
}

impl fmt::Display for WorkflowDefinitionApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                f,
                "invalid workflow definition API endpoint; check the deployment URL and HTTP policy"
            ),
            Self::BuildClient(error) => {
                write!(f, "prepare workflow definition networking: {error}")
            }
        }
    }
}

impl std::error::Error for WorkflowDefinitionApiError {}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::expect_used,
    reason = "workflow definition client unit tests use Rust test assertions"
)]
mod tests {
    use reqwest::StatusCode;
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};

    use um_test_support::{REQUEST_IDEMPOTENCY_KEY_ECHO, ScriptedHttpServer};

    use super::super::resource::{RawResponse, problem_fixture};
    use super::{
        WorkflowDefinitionApi, WorkflowDefinitionFailure, WorkflowEvaluationFilter,
        WorkflowPublication, WorkflowRefusal, classify,
    };
    use crate::{HttpClient, HttpTransportPolicy};

    fn problem(status: u16, kind: &str) -> RawResponse {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        RawResponse {
            status: StatusCode::from_u16(status).expect("status"),
            body: problem_fixture(status, kind),
            headers,
            retry_after: None,
        }
    }

    // REQ-CLOUDDEF-14: a refusal keeps its problem slug for the command to report.
    #[test]
    fn a_problem_response_is_a_refusal_naming_its_slug_unless_it_is_unauthorized() {
        assert_eq!(
            classify(&problem(409, "definition-revision-conflict")),
            WorkflowDefinitionFailure::Refused(Box::new(WorkflowRefusal {
                status: 409,
                problem: "definition-revision-conflict".to_owned(),
                workflow: None,
            }))
        );
        assert_eq!(
            classify(&problem(503, "workflow-validator-unavailable")),
            WorkflowDefinitionFailure::Refused(Box::new(WorkflowRefusal {
                status: 503,
                problem: "workflow-validator-unavailable".to_owned(),
                workflow: None,
            }))
        );
        assert_eq!(
            classify(&problem(401, "unauthorized")),
            WorkflowDefinitionFailure::Unauthenticated
        );
        assert!(matches!(
            classify(&problem(409, "Not A Slug")),
            WorkflowDefinitionFailure::InvalidResponse { .. }
        ));
    }

    // REQ-CLOUDDEF-22: a 200 answering a create is the project's identical definition, reused,
    // only when the create asked for reuse; to any other create it is an invalid response.
    #[test]
    fn a_reused_definition_is_accepted_only_when_the_create_asked_for_reuse() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let project = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
        let definition = "def_01k0z6r1w8f4jy2m7q9v3x5abc";
        let digest = format!("sha256:{}", "a".repeat(64));
        let at = "2026-11-10T12:00:00Z";
        // The definition stands at revision 3, which a reuse answers as it is.
        let body = serde_json::to_vec(&serde_json::json!({
            "definition": {
                "id": definition, "projectId": project, "name": "engineering",
                "state": "active", "automaticStarts": false, "executionPrincipalId": null,
                "currentRevision": 3, "createdAt": at, "updatedAt": at,
            },
            "revision": {
                "definitionId": definition, "number": 3, "treeDigest": digest,
                "bindings": {}, "resolution": {}, "publishedAt": at,
                "publishedBy": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
            },
        }))
        .expect("publication");
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nIdempotency-Key: {REQUEST_IDEMPOTENCY_KEY_ECHO}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(&body);
        let server = ScriptedHttpServer::respond_in_sequence(vec![response.clone(), response]);
        let storage = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).expect("client");
        let api =
            WorkflowDefinitionApi::new(&server.api_url, "fixture-token", &storage).expect("api");
        let publication = WorkflowPublication {
            tree_digest: &digest,
            bindings: None,
            executable_paths: None,
        };

        assert_eq!(
            api.create_definition("acme", project, "create-key", publication, false)
                .map(|(_, reused)| reused),
            Err(WorkflowDefinitionFailure::InvalidResponse {
                credential_rejected: false
            })
        );
        let (published, reused) = api
            .create_definition("acme", project, "reuse-key", publication, true)
            .expect("a reuse the create asked for");
        assert!(reused);
        assert_eq!(published.definition.id, definition);
        assert_eq!(published.revision.number, 3);
        let requests = server.finish();
        assert!(!requests[0].contains("reuseIdentical"), "{}", requests[0]);
        assert!(
            requests[1].contains(r#""reuseIdentical":true"#),
            "{}",
            requests[1]
        );
    }

    // REQ-COMPAT-20: a page longer than the 100-item `limit` maximum this release requests
    // is still an answer it reads.
    #[test]
    fn an_evaluation_page_of_any_length_decodes() {
        let length = 101;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let definition = "def_01k0z6r1w8f4jy2m7q9v3x5abc";
        let evaluation = serde_json::json!({
            "id": "wev_01k0z6r1w8f4jy2m7q9v3x5abc", "definitionId": definition, "revision": 2,
            "triggerName": "start-on-todo", "connectionId": "lnc_01k0z6r1w8f4jy2m7q9v3x5abc",
            "executionPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5xyz",
            "eventName": "issue.state_entered",
            "event": {
                "issue": {"id": "issue-7", "identifier": "AUTO-7"},
                "issueUpdatedAt": "2026-11-10T12:00:00Z", "team": "team-1",
                "state": "state-todo", "actor": null, "at": "2026-11-10T12:00:00Z",
            },
            "state": "failed", "reason": "owner_unresolved", "observations": null,
            "runId": null, "blockingRunId": null, "cycleNumber": 1, "attemptNumber": 2,
            "cycleStartedAt": "2026-11-10T12:00:00Z", "cycleDeadline": "2026-11-11T12:00:00Z",
            "nextAttemptAt": null, "acceptedAt": "2026-11-10T12:00:00Z",
            "expiresAt": "2026-12-10T12:00:00Z", "settledAt": "2026-11-10T12:00:00Z",
        });
        let body = serde_json::to_vec(&serde_json::json!({
            "items": vec![evaluation; length],
            "nextCursor": "older-evaluations",
        }))
        .expect("page");
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(&body);
        let server = um_test_support::ScriptedHttpServer::respond(response);
        let storage = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).expect("client");
        let api =
            WorkflowDefinitionApi::new(&server.api_url, "fixture-token", &storage).expect("api");
        let (items, cursor) = api
            .evaluations(
                "acme",
                "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                definition,
                WorkflowEvaluationFilter {
                    state: None,
                    run_id: None,
                },
                None,
                None,
            )
            .expect("page of any length");
        assert_eq!(items.len(), length);
        assert_eq!(cursor.as_deref(), Some("older-evaluations"));
        server.finish_one();
    }
}
