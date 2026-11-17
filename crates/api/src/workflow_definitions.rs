//! Cloud workflow definitions and the trees they publish (REQ-CLOUDDEF-1 to REQ-CLOUDDEF-11),
//! for `useful workflow definition publish --cloud` (REQ-CLOUDDEF-14) and the `useful workflow
//! definition cloud` group (REQ-CLOUDDEF-15).
//!
//! As for the other resource clients, requests go through the generated configuration and
//! models with a hand-written client: the generated one drops error headers, so here a keyed
//! mutation must echo its `Idempotency-Key` and a refusal keeps its problem type's slug and
//! the `workflow` safe facts for the caller to report. A definition, revision, evaluation or
//! publication answer is decoded into its generated model, which ignores a member this
//! release does not know, and then checked against the request (REQ-CLOUDDEF-23); the tree
//! upload and seal answers are decoded closed. An answer, or a value the request must carry,
//! that fails a check is an invalid response naming the exchange and the check that failed and
//! retaining the error received, which is never formatted (INV-CLI-18, REQ-CLOUDDEF-23).
use std::collections::HashMap;
use std::fmt;
use std::num::TryFromIntError;
use std::time::Duration;

use base64::Engine as _;
use reqwest::header::{
    CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, IF_NONE_MATCH, InvalidHeaderValue,
};
use reqwest::{Method, StatusCode, Url};

use super::generated::{apis, models};
use super::http_client::{SignedStorageRequestError, zeroize_generated_bearer_access_token};
use super::resource::{
    ClosedJsonError, RawResponse, ResourceFailure, mutation_request, receive_raw,
    resource_configuration, try_decode_closed_json, validate_json_media_type,
    validate_mutation_key,
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

#[derive(Debug, PartialEq)]
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
    /// An answer, or a value the request must carry, that fails a check (REQ-CLOUDDEF-23).
    InvalidResponse(Box<WorkflowInvalidResponse>),
}

impl WorkflowDefinitionFailure {
    pub fn credential_rejected(&self) -> bool {
        match self {
            Self::Unauthenticated => true,
            Self::InvalidResponse(invalid) => invalid.credential_rejected,
            Self::Refused(_)
            | Self::UploadRejected { .. }
            | Self::Unreachable { .. }
            | Self::RateLimited { .. } => false,
        }
    }
}

impl From<WorkflowInvalidResponse> for WorkflowDefinitionFailure {
    fn from(invalid: WorkflowInvalidResponse) -> Self {
        Self::InvalidResponse(Box::new(invalid))
    }
}

/// The exchange with Cloud or signed storage whose boundary an invalid response failed: the
/// public method's request, or for a tree upload the part of it (REQ-CLOUDDEF-28).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowDefinitionStage {
    /// The manifest and the upload answer, up to and including the capability lookup.
    TreeUpload,
    /// One file's signed upload capability and its upload.
    FileUpload,
    TreeSeal,
    DefinitionCreate,
    RevisionPublish,
    DefinitionRead,
    DefinitionList,
    RevisionRead,
    RevisionList,
    EvaluationRead,
    EvaluationList,
    /// Enable, disable, archive, unarchive and the execution principal.
    DefinitionChange,
}

impl WorkflowDefinitionStage {
    /// The stage's published slug.
    pub const fn slug(self) -> &'static str {
        match self {
            Self::TreeUpload => "tree_upload",
            Self::FileUpload => "file_upload",
            Self::TreeSeal => "tree_seal",
            Self::DefinitionCreate => "definition_create",
            Self::RevisionPublish => "revision_publish",
            Self::DefinitionRead => "definition_read",
            Self::DefinitionList => "definition_list",
            Self::RevisionRead => "revision_read",
            Self::RevisionList => "revision_list",
            Self::EvaluationRead => "evaluation_read",
            Self::EvaluationList => "evaluation_list",
            Self::DefinitionChange => "definition_change",
        }
    }
}

/// What failed at an invalid response's stage (REQ-CLOUDDEF-28).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowDefinitionCheck {
    /// A value the request must carry cannot be sent: a file's size or the next revision.
    RequestValue,
    /// The answer is larger than the CLI reads.
    ResponseSize,
    /// An answer under a status the request does not accept is not a problem response.
    ProblemDecode,
    /// A problem response names no refusal, or under 401 is other than `unauthorized`.
    ProblemType,
    /// A keyed answer does not carry the request's `Idempotency-Key`.
    IdempotencyKey,
    /// The answer is not `application/json`.
    MediaType,
    /// The body is not JSON.
    BodySyntax,
    /// A known member is missing, of another type or outside its vocabulary.
    BodyShape,
    /// A tree upload or seal answer carries what this release does not know.
    BodyClosed,
    /// The answer names something other than what was asked.
    Identity,
    /// An upload capability's required headers are not the file's.
    CapabilityHeaders,
    /// An upload capability's URL does not parse.
    CapabilityUrlSyntax,
    /// The transport policy refuses an upload capability's URL, or it has no host, a user or
    /// password, or a fragment.
    CapabilityUrlPolicy,
    /// A required header value cannot be sent.
    CapabilityHeaderValue,
    /// The signed storage client could not be formed.
    StorageClient,
    /// The signed storage request could not be formed.
    StorageRequest,
}

impl WorkflowDefinitionCheck {
    /// The check's published slug.
    pub const fn slug(self) -> &'static str {
        match self {
            Self::RequestValue => "request_value",
            Self::ResponseSize => "response_size",
            Self::ProblemDecode => "problem_decode",
            Self::ProblemType => "problem_type",
            Self::IdempotencyKey => "idempotency_key",
            Self::MediaType => "media_type",
            Self::BodySyntax => "body_syntax",
            Self::BodyShape => "body_shape",
            Self::BodyClosed => "body_closed",
            Self::Identity => "identity",
            Self::CapabilityHeaders => "capability_headers",
            Self::CapabilityUrlSyntax => "capability_url_syntax",
            Self::CapabilityUrlPolicy => "capability_url_policy",
            Self::CapabilityHeaderValue => "capability_header_value",
            Self::StorageClient => "storage_client",
            Self::StorageRequest => "storage_request",
        }
    }
}

use WorkflowDefinitionCheck as Check;
use WorkflowDefinitionStage as Stage;

/// An invalid response: where it was detected, the status a check of the status judged, and
/// the error received, its [`std::error::Error::source`]. `Debug` and `Display` print the
/// stage, the check and the status alone, since the error's text can quote the response.
pub struct WorkflowInvalidResponse {
    stage: Stage,
    check: Check,
    status: Option<u16>,
    credential_rejected: bool,
    cause: Option<Cause>,
}

/// The error an invalid response received.
enum Cause {
    Size(TryFromIntError),
    Json(serde_json::Error),
    Url(url::ParseError),
    HeaderValue(InvalidHeaderValue),
    StorageClient(rustls::Error),
    StorageRequest(hyper::http::Error),
}

impl Cause {
    fn error(&self) -> &(dyn std::error::Error + 'static) {
        match self {
            Self::Size(error) => error,
            Self::Json(error) => error,
            Self::Url(error) => error,
            Self::HeaderValue(error) => error,
            Self::StorageClient(error) => error,
            Self::StorageRequest(error) => error,
        }
    }
}

impl WorkflowInvalidResponse {
    fn caused_by(self, cause: Cause) -> Self {
        Self {
            cause: Some(cause),
            ..self
        }
    }

    pub const fn stage(&self) -> Stage {
        self.stage
    }

    pub const fn check(&self) -> Check {
        self.check
    }

    /// The HTTP status answered, for `problem_decode` and `problem_type`.
    pub const fn status(&self) -> Option<u16> {
        self.status
    }

    /// Whether the answer was a 401, which rejects the session.
    pub const fn credential_rejected(&self) -> bool {
        self.credential_rejected
    }
}

/// An invalid response at `stage` failing `check`, with nothing received.
const fn check_failed(stage: Stage, check: Check) -> WorkflowInvalidResponse {
    WorkflowInvalidResponse {
        stage,
        check,
        status: None,
        credential_rejected: false,
        cause: None,
    }
}

/// A body at `stage` that `serde_json` refused: `body_shape` when it is JSON that does not fit
/// the model, `body_syntax` otherwise.
fn undecodable(stage: Stage, error: serde_json::Error) -> WorkflowInvalidResponse {
    let check = match error.classify() {
        serde_json::error::Category::Data => Check::BodyShape,
        serde_json::error::Category::Io
        | serde_json::error::Category::Syntax
        | serde_json::error::Category::Eof => Check::BodySyntax,
    };
    check_failed(stage, check).caused_by(Cause::Json(error))
}

impl PartialEq for WorkflowInvalidResponse {
    /// The safe facts alike; the received error is not compared.
    fn eq(&self, other: &Self) -> bool {
        (
            self.stage,
            self.check,
            self.status,
            self.credential_rejected,
        ) == (
            other.stage,
            other.check,
            other.status,
            other.credential_rejected,
        )
    }
}

impl fmt::Debug for WorkflowInvalidResponse {
    /// The stage, the check and the status, never the received error.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowInvalidResponse")
            .field("stage", &self.stage.slug())
            .field("check", &self.check.slug())
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for WorkflowInvalidResponse {
    /// The stage, the check and the status, never the received error.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid response at {}: {}",
            self.stage.slug(),
            self.check.slug()
        )?;
        if let Some(status) = self.status {
            write!(formatter, " (HTTP {status})")?;
        }
        Ok(())
    }
}

impl std::error::Error for WorkflowInvalidResponse {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().map(Cause::error)
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
                    let size = i64::try_from(file.bytes.len()).map_err(|error| {
                        check_failed(Stage::TreeUpload, Check::RequestValue)
                            .caused_by(Cause::Size(error))
                    })?;
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
            receive_closed(request, StatusCode::OK, None, Stage::TreeUpload)?;
        if issued.tree.digest != digest {
            return Err(check_failed(Stage::TreeUpload, Check::Identity).into());
        }
        for capability in &issued.capabilities {
            let file = files
                .iter()
                .zip(&manifest.files)
                .find(|(_, entry)| entry.sha256 == capability.sha256)
                .map(|(file, _)| file)
                .ok_or(check_failed(Stage::TreeUpload, Check::Identity))?;
            self.put_object(capability, file.bytes)?;
        }
        if issued.tree.state == models::workflow_tree::State::Sealed {
            return Ok((*issued.tree, true));
        }
        let endpoint = self.endpoint(organization, &["workflow-trees", digest, "seal"]);
        let request = mutation_request(&self.configuration, Method::POST, &endpoint, key);
        let sealed: WorkflowTree =
            receive_closed(request, StatusCode::OK, Some(key), Stage::TreeSeal)?;
        if sealed.digest != digest || sealed.state != models::workflow_tree::State::Sealed {
            return Err(check_failed(Stage::TreeSeal, Check::Identity).into());
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
        let stage = Stage::DefinitionRead;
        let definition: WorkflowDefinition = receive(request, StatusCode::OK, None, stage)?;
        validate_definition(&definition, project, Some(id), stage)?;
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
        let stage = Stage::DefinitionList;
        let list: models::WorkflowDefinitionList = receive(
            self.page_request(&endpoint, limit, cursor),
            StatusCode::OK,
            None,
            stage,
        )?;
        for definition in &list.items {
            validate_definition(definition, project, None, stage)?;
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
        let stage = Stage::RevisionList;
        let list: models::WorkflowDefinitionRevisionList = receive(
            self.page_request(&endpoint, limit, cursor),
            StatusCode::OK,
            None,
            stage,
        )?;
        for revision in &list.items {
            validate_revision(revision, id, None, stage)?;
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
        let stage = Stage::RevisionRead;
        let revision: WorkflowDefinitionRevision = receive(request, StatusCode::OK, None, stage)?;
        validate_revision(&revision, id, Some(number), stage)?;
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
        let stage = Stage::EvaluationList;
        let list: models::WorkflowDefinitionEvaluationList =
            receive(request, StatusCode::OK, None, stage)?;
        for evaluation in &list.items {
            validate_evaluation(evaluation, id, None, stage)?;
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
        let stage = Stage::EvaluationRead;
        let read: WorkflowDefinitionEvaluation = receive(request, StatusCode::OK, None, stage)?;
        validate_evaluation(&read, id, Some(evaluation), stage)?;
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
        let stage = Stage::DefinitionChange;
        let definition: WorkflowDefinition = receive(request, StatusCode::OK, Some(key), stage)?;
        validate_definition(&definition, project, Some(id), stage)?;
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
            return Err(check_failed(stage, Check::Identity).into());
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
        let stage = Stage::DefinitionCreate;
        let (status, published): (_, WorkflowDefinitionPublication) =
            receive_one_of(request, accepted, Some(key), stage)?;
        let reused = status == StatusCode::OK;
        // A reuse answers the definition's current revision, whatever its number.
        let revision = if reused {
            published.definition.current_revision
        } else {
            1
        };
        validate_publication(
            &published,
            project,
            None,
            revision,
            publication.tree_digest,
            stage,
        )?;
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
        let stage = Stage::RevisionPublish;
        let next = expected_current_revision
            .checked_add(1)
            .ok_or(check_failed(stage, Check::RequestValue))?;
        let endpoint = self.definitions_endpoint(organization, project, &[id, "revisions"]);
        let request =
            mutation_request(&self.configuration, Method::POST, &endpoint, key).json(&body);
        let published = receive(request, StatusCode::CREATED, Some(key), stage)?;
        validate_publication(
            &published,
            project,
            Some(id),
            next,
            publication.tree_digest,
            stage,
        )?;
        Ok(published)
    }

    /// Puts `bytes` to the signed upload `capability`, all of it the `file_upload` stage.
    fn put_object(
        &self,
        capability: &models::WorkflowTreeUploadCapability,
        bytes: &[u8],
    ) -> Result<(), WorkflowDefinitionFailure> {
        let stage = Stage::FileUpload;
        let required = &capability.required_headers;
        if required.content_length != bytes.len().to_string()
            || required.if_none_match
                != models::run_input_upload_required_headers::IfNoneMatch::Star
            || required.x_amz_checksum_sha256
                != base64::engine::general_purpose::STANDARD.encode(sha256(bytes))
        {
            return Err(check_failed(stage, Check::CapabilityHeaders).into());
        }
        let url = Url::parse(&capability.url).map_err(|error| {
            check_failed(stage, Check::CapabilityUrlSyntax).caused_by(Cause::Url(error))
        })?;
        if !self.storage.transport_policy().permits(&url)
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(check_failed(stage, Check::CapabilityUrlPolicy).into());
        }
        let header = |value: &str| {
            HeaderValue::from_str(value).map_err(|error| {
                check_failed(stage, Check::CapabilityHeaderValue)
                    .caused_by(Cause::HeaderValue(error))
            })
        };
        let mut headers = HeaderMap::with_capacity(4);
        headers.insert(CONTENT_LENGTH, header(&required.content_length)?);
        headers.insert(CONTENT_TYPE, header(&required.content_type)?);
        headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        headers.insert(
            "x-amz-checksum-sha256",
            header(&required.x_amz_checksum_sha256)?,
        );
        let status = self
            .storage
            .signed_storage_put(&url, headers, bytes, STORAGE_REQUEST_TIMEOUT)
            .map_err(|error| match error {
                SignedStorageRequestError::Build(error) => {
                    check_failed(stage, Check::StorageClient)
                        .caused_by(Cause::StorageClient(error))
                        .into()
                }
                SignedStorageRequestError::InvalidRequest(error) => {
                    check_failed(stage, Check::StorageRequest)
                        .caused_by(Cause::StorageRequest(error))
                        .into()
                }
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

/// The JSON body of an `expected` response, its key confirmed when `key` is given; any other
/// status is classified. The body is read open, as [`receive_one_of`] reads it.
fn receive<T: serde::de::DeserializeOwned>(
    request: reqwest::blocking::RequestBuilder,
    expected: StatusCode,
    key: Option<&str>,
    stage: Stage,
) -> Result<T, WorkflowDefinitionFailure> {
    receive_one_of(request, &[expected], key, stage).map(|(_, body)| body)
}

/// The status and JSON body of a response whose status is one of `accepted`, its key
/// confirmed when `key` is given; any other status is classified. The body is decoded
/// straight into the generated model, which refuses a required member that is missing, a
/// mistyped one or a value outside its vocabulary and ignores a member this release does not
/// know (REQ-CLOUDDEF-23).
fn receive_one_of<T: serde::de::DeserializeOwned>(
    request: reqwest::blocking::RequestBuilder,
    accepted: &[StatusCode],
    key: Option<&str>,
    stage: Stage,
) -> Result<(StatusCode, T), WorkflowDefinitionFailure> {
    let response = receive_accepted(request, accepted, key, stage)?;
    let body = serde_json::from_slice(&response.body).map_err(|error| undecodable(stage, error))?;
    Ok((response.status, body))
}

/// The closed JSON body of an `expected` response, its key confirmed when `key` is given; any
/// other status is classified. Only the tree upload and seal answers are read closed: the
/// upload answer carries the signed capabilities whose required headers are sent exactly.
fn receive_closed<T: serde::de::DeserializeOwned + serde::Serialize>(
    request: reqwest::blocking::RequestBuilder,
    expected: StatusCode,
    key: Option<&str>,
    stage: Stage,
) -> Result<T, WorkflowDefinitionFailure> {
    let response = receive_accepted(request, &[expected], key, stage)?;
    try_decode_closed_json(&response.body).map_err(|error| {
        match error {
            ClosedJsonError::Syntax(error) => {
                check_failed(stage, Check::BodySyntax).caused_by(Cause::Json(error))
            }
            ClosedJsonError::Shape(error) => {
                check_failed(stage, Check::BodyShape).caused_by(Cause::Json(error))
            }
            ClosedJsonError::Encode(error) => {
                check_failed(stage, Check::BodyClosed).caused_by(Cause::Json(error))
            }
            ClosedJsonError::Differs => check_failed(stage, Check::BodyClosed),
        }
        .into()
    })
}

/// A JSON response whose status is one of `accepted`, its key confirmed when `key` is given,
/// with its body still to decode; any other status is classified.
fn receive_accepted(
    request: reqwest::blocking::RequestBuilder,
    accepted: &[StatusCode],
    key: Option<&str>,
    stage: Stage,
) -> Result<RawResponse, WorkflowDefinitionFailure> {
    // `receive_raw` fails only unreachable or, for a body over the bound, as an invalid
    // response that keeps a 401's rejected session.
    let response = receive_raw(request).map_err(|failure| match failure {
        ResourceFailure::Unreachable { category } => {
            WorkflowDefinitionFailure::Unreachable { category }
        }
        failure => WorkflowInvalidResponse {
            credential_rejected: failure.credential_rejected(),
            ..check_failed(stage, Check::ResponseSize)
        }
        .into(),
    })?;
    if !accepted.contains(&response.status) {
        return Err(classify(&response, stage));
    }
    if let Some(key) = key {
        validate_mutation_key(&response.headers, key)
            .map_err(|_| check_failed(stage, Check::IdempotencyKey))?;
    }
    validate_json_media_type(&response.headers)
        .map_err(|_| check_failed(stage, Check::MediaType))?;
    Ok(response)
}

fn classify(response: &RawResponse, stage: Stage) -> WorkflowDefinitionFailure {
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
    // A check of the status keeps it, and a 401 keeps rejecting the session.
    let judged = |check| -> WorkflowDefinitionFailure {
        WorkflowInvalidResponse {
            status: Some(status.as_u16()),
            credential_rejected: status == StatusCode::UNAUTHORIZED,
            ..check_failed(stage, check)
        }
        .into()
    };
    let Ok(decoded) = decoded else {
        return if status.is_server_error() {
            WorkflowDefinitionFailure::Unreachable {
                category: UnreachableCategory::Server,
            }
        } else {
            judged(Check::ProblemDecode)
        };
    };
    if status == StatusCode::UNAUTHORIZED {
        return if decoded.r#type == problem::UNAUTHORIZED {
            WorkflowDefinitionFailure::Unauthenticated
        } else {
            judged(Check::ProblemType)
        };
    }
    // A transient problem under its declared status is Cloud being unable to serve the
    // request, as a 5xx that is no problem response is (REQ-CLOUDDEF-25).
    if matches!(
        (status, decoded.r#type.as_str()),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            problem::SERVICE_UNAVAILABLE
        ) | (StatusCode::SERVICE_UNAVAILABLE, problem::RETRYABLE_CONFLICT)
            | (
                StatusCode::INTERNAL_SERVER_ERROR,
                problem::INTERNAL_SERVER_ERROR
            )
    ) {
        return WorkflowDefinitionFailure::Unreachable {
            category: UnreachableCategory::Server,
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
        _ => judged(Check::ProblemType),
    }
}

fn validate_definition(
    value: &WorkflowDefinition,
    project: &str,
    id: Option<&str>,
    stage: Stage,
) -> Result<(), WorkflowInvalidResponse> {
    if !useful_support::valid_typed_id(&value.id, DEFINITION_ID_PREFIX)
        || id.is_some_and(|id| value.id != id)
        || value.project_id != project
        || value.current_revision < 1
    {
        return Err(check_failed(stage, Check::Identity));
    }
    Ok(())
}

fn validate_revision(
    value: &WorkflowDefinitionRevision,
    id: &str,
    number: Option<i32>,
    stage: Stage,
) -> Result<(), WorkflowInvalidResponse> {
    if value.definition_id != id
        || value.number < 1
        || number.is_some_and(|number| value.number != number)
    {
        return Err(check_failed(stage, Check::Identity));
    }
    Ok(())
}

fn validate_evaluation(
    value: &WorkflowDefinitionEvaluation,
    definition: &str,
    id: Option<&str>,
    stage: Stage,
) -> Result<(), WorkflowInvalidResponse> {
    if !useful_support::valid_typed_id(&value.id, EVALUATION_ID_PREFIX)
        || id.is_some_and(|id| value.id != id)
        || value.definition_id != definition
        || value.revision < 1
    {
        return Err(check_failed(stage, Check::Identity));
    }
    Ok(())
}

fn validate_publication(
    value: &WorkflowDefinitionPublication,
    project: &str,
    id: Option<&str>,
    revision: i32,
    tree_digest: &str,
    stage: Stage,
) -> Result<(), WorkflowInvalidResponse> {
    validate_definition(&value.definition, project, id, stage)?;
    if value.revision.definition_id != value.definition.id
        || value.revision.number != revision
        || value.definition.current_revision != revision
        || value.revision.tree_digest != tree_digest
    {
        return Err(check_failed(stage, Check::Identity));
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    super::run_inputs::digest_bytes(bytes)
}

fn lowercase_hex(bytes: &[u8; 32]) -> String {
    useful_support::lowercase_hex(bytes)
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

    use useful_test_support::{REQUEST_IDEMPOTENCY_KEY_ECHO, ScriptedHttpServer};

    use std::error::Error as _;
    use std::fmt;

    use base64::Engine as _;

    use super::super::resource::{RawResponse, problem_fixture};
    use super::{
        WorkflowDefinitionApi, WorkflowDefinitionCheck as Check, WorkflowDefinitionFailure,
        WorkflowDefinitionStage as Stage, WorkflowEvaluationFilter, WorkflowInvalidResponse,
        WorkflowPublication, WorkflowRefusal, WorkflowTreeFile, classify, lowercase_hex, sha256,
    };
    use crate::{HttpClient, HttpTransportPolicy, UnreachableCategory};

    /// A value an answer carries that no output of a failure may print.
    const SENTINEL: &str = "sentinel-a1b2c3";

    fn raw(status: u16, content_type: &'static str, body: Vec<u8>) -> RawResponse {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        RawResponse {
            status: StatusCode::from_u16(status).expect("status"),
            body,
            headers,
            retry_after: None,
        }
    }

    fn problem(status: u16, kind: &str) -> RawResponse {
        raw(
            status,
            "application/problem+json",
            problem_fixture(status, kind),
        )
    }

    /// The kind of error an invalid response retains as its source.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Source {
        None,
        Json,
        Url,
        HeaderValue,
        Other,
    }

    /// The invalid response `failed` is, checked to name `stage` and `check`, to retain a
    /// `source` of that kind, and to print nothing of [`SENTINEL`] through `Debug` or
    /// `Display`.
    fn invalid_response<T: fmt::Debug>(
        failed: Result<T, WorkflowDefinitionFailure>,
        stage: Stage,
        check: Check,
        source: Source,
    ) -> Box<WorkflowInvalidResponse> {
        let failure = failed.expect_err("an invalid response");
        let debug = format!("{failure:?}");
        let invalid = match failure {
            WorkflowDefinitionFailure::InvalidResponse(invalid) => Some(invalid),
            _ => None,
        };
        assert!(invalid.is_some(), "not an invalid response: {debug}");
        let invalid = invalid.expect("an invalid response");
        assert_eq!(
            (invalid.stage().slug(), invalid.check().slug()),
            (stage.slug(), check.slug()),
            "{debug}"
        );
        let retained = match invalid.source() {
            None => Source::None,
            Some(error) if error.is::<serde_json::Error>() => Source::Json,
            Some(error) if error.is::<url::ParseError>() => Source::Url,
            Some(error) if error.is::<reqwest::header::InvalidHeaderValue>() => Source::HeaderValue,
            Some(_) => Source::Other,
        };
        assert_eq!(retained, source, "{debug}");
        let display = invalid.to_string();
        assert!(!debug.contains(SENTINEL), "{debug}");
        assert!(!display.contains(SENTINEL), "{display}");
        invalid
    }

    // REQ-CLOUDDEF-14: a refusal keeps its problem slug for the command to report.
    // INV-CLI-18: an answer under a status the request does not accept that is no problem
    // response, or whose type is no refusal, names the check and keeps the status, and a 401
    // keeps rejecting the session.
    #[test]
    fn a_problem_response_is_a_refusal_naming_its_slug_unless_it_is_unauthorized() {
        let stage = Stage::DefinitionCreate;
        assert_eq!(
            classify(&problem(409, "definition-revision-conflict"), stage),
            WorkflowDefinitionFailure::Refused(Box::new(WorkflowRefusal {
                status: 409,
                problem: "definition-revision-conflict".to_owned(),
                workflow: None,
            }))
        );
        assert_eq!(
            classify(&problem(503, "workflow-validator-unavailable"), stage),
            WorkflowDefinitionFailure::Refused(Box::new(WorkflowRefusal {
                status: 503,
                problem: "workflow-validator-unavailable".to_owned(),
                workflow: None,
            }))
        );
        assert_eq!(
            classify(&problem(401, "unauthorized"), stage),
            WorkflowDefinitionFailure::Unauthenticated
        );
        let html = || b"<html>not found</html>".to_vec();
        for (response, check, status, credential_rejected) in [
            (problem(409, "Not A Slug"), Check::ProblemType, 409, false),
            (
                raw(404, "text/html", html()),
                Check::ProblemDecode,
                404,
                false,
            ),
            (
                raw(401, "text/html", html()),
                Check::ProblemDecode,
                401,
                true,
            ),
            (problem(401, "forbidden"), Check::ProblemType, 401, true),
        ] {
            let failure = classify(&response, stage);
            assert_eq!(failure.credential_rejected(), credential_rejected);
            let invalid = invalid_response(Err::<(), _>(failure), stage, check, Source::None);
            assert_eq!(invalid.status(), Some(status));
            assert_eq!(invalid.credential_rejected(), credential_rejected);
        }
    }

    // REQ-CLOUDDEF-25: the three transient problems, each under its declared status, are
    // Cloud being unreachable; a 503 that names its own cause and a transient type under
    // another status stay refusals with their slug, and a rate limit stays a rate limit.
    #[test]
    fn a_transient_problem_is_unreachable_and_a_named_cause_stays_a_refusal() {
        for (status, kind) in [
            (503, "service-unavailable"),
            (503, "retryable-conflict"),
            (500, "internal-server-error"),
        ] {
            assert_eq!(
                classify(&problem(status, kind), Stage::RevisionPublish),
                WorkflowDefinitionFailure::Unreachable {
                    category: UnreachableCategory::Server,
                },
                "{status} {kind}"
            );
        }
        for (status, kind) in [
            (503, "workflow-validator-unavailable"),
            (503, "binding-resolution-unavailable"),
            (503, "workflow-tree-storage-unavailable"),
            (500, "service-unavailable"),
            (409, "retryable-conflict"),
            (503, "internal-server-error"),
        ] {
            assert_eq!(
                classify(&problem(status, kind), Stage::RevisionPublish),
                WorkflowDefinitionFailure::Refused(Box::new(WorkflowRefusal {
                    status,
                    problem: kind.to_owned(),
                    workflow: None,
                })),
                "{status} {kind}"
            );
        }
        assert_eq!(
            classify(&problem(429, "rate-limit-exceeded"), Stage::RevisionPublish),
            WorkflowDefinitionFailure::RateLimited { retry_after: None }
        );
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
        let body = serde_json::json!({
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
        });
        let response = response("200 OK", "application/json", true, &body);
        let server = ScriptedHttpServer::respond_in_sequence(vec![response.clone(), response]);
        let storage = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).expect("client");
        let api =
            WorkflowDefinitionApi::new(&server.api_url, "fixture-token", &storage).expect("api");
        let publication = WorkflowPublication {
            tree_digest: &digest,
            bindings: None,
            executable_paths: None,
        };

        // INV-CLI-18: the 200 is a status the create did not accept, and no problem response.
        let invalid = invalid_response(
            api.create_definition("acme", project, "create-key", publication, false)
                .map(|(_, reused)| reused),
            Stage::DefinitionCreate,
            Check::ProblemDecode,
            Source::None,
        );
        assert_eq!(invalid.status(), Some(200));
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
        let body = serde_json::json!({
            "items": vec![evaluation; length],
            "nextCursor": "older-evaluations",
        });
        let server =
            ScriptedHttpServer::respond(response("200 OK", "application/json", false, &body));
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

    const ORGANIZATION: &str = "acme";
    const PROJECT: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
    const DEFINITION: &str = "def_01k0z6r1w8f4jy2m7q9v3x5abc";
    const EVALUATION: &str = "wev_01k0z6r1w8f4jy2m7q9v3x5abc";
    const AT: &str = "2026-11-10T12:00:00Z";
    /// The member a later Cloud release adds, which this release does not know.
    const ADDED: &str = "addedLater";

    fn tree_digest() -> String {
        format!("sha256:{}", "a".repeat(64))
    }

    fn added() -> serde_json::Value {
        serde_json::json!({"kind": "added", "count": 1})
    }

    fn definition_answer(current_revision: i32) -> serde_json::Value {
        serde_json::json!({
            "id": DEFINITION, "projectId": PROJECT, "name": "engineering", "state": "active",
            "automaticStarts": true, "executionPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5xyz",
            "currentRevision": current_revision, "createdAt": AT, "updatedAt": AT,
            ADDED: added(),
        })
    }

    fn revision_answer(number: i32) -> serde_json::Value {
        serde_json::json!({
            "definitionId": DEFINITION, "number": number, "treeDigest": tree_digest(),
            "bindings": {}, "resolution": {}, "publishedAt": AT,
            "publishedBy": "prn_01k0z6r1w8f4jy2m7q9v3x5abc", ADDED: added(),
        })
    }

    fn evaluation_answer() -> serde_json::Value {
        serde_json::json!({
            "id": EVALUATION, "definitionId": DEFINITION, "revision": 2,
            "triggerName": "start-on-todo", "connectionId": "lnc_01k0z6r1w8f4jy2m7q9v3x5abc",
            "executionPrincipalId": "prn_01k0z6r1w8f4jy2m7q9v3x5xyz",
            "eventName": "issue.state_entered",
            "event": {
                "issue": {"id": "issue-7", "identifier": "AUTO-7"},
                "issueUpdatedAt": AT, "team": "team-1", "state": "state-todo", "actor": null,
                "at": AT, ADDED: added(),
            },
            "state": "run_created", "reason": null, "observations": null,
            "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abc", "blockingRunId": null, "cycleNumber": 1,
            "attemptNumber": 1, "cycleStartedAt": AT, "cycleDeadline": "2026-11-11T12:00:00Z",
            "nextAttemptAt": null, "acceptedAt": AT, "expiresAt": "2026-12-10T12:00:00Z",
            "settledAt": AT, ADDED: added(),
        })
    }

    fn publication_answer(revision: i32) -> serde_json::Value {
        serde_json::json!({
            "definition": definition_answer(revision),
            "revision": revision_answer(revision),
            ADDED: added(),
        })
    }

    fn page_answer(item: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "items": [item, item], "nextCursor": "next-page", ADDED: added(),
        })
    }

    /// `answer` without the member this release does not know, at any depth.
    fn known(mut answer: serde_json::Value) -> serde_json::Value {
        fn strip(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Object(members) => {
                    members.remove(ADDED);
                    members.values_mut().for_each(strip);
                }
                serde_json::Value::Array(items) => items.iter_mut().for_each(strip),
                _ => {}
            }
        }
        strip(&mut answer);
        answer
    }

    fn response(
        status: &str,
        content_type: &str,
        echo_key: bool,
        body: &serde_json::Value,
    ) -> Vec<u8> {
        response_bytes(
            status,
            content_type,
            echo_key,
            &serde_json::to_vec(body).expect("answer"),
        )
    }

    fn response_bytes(status: &str, content_type: &str, echo_key: bool, body: &[u8]) -> Vec<u8> {
        let key = if echo_key {
            format!("Idempotency-Key: {REQUEST_IDEMPOTENCY_KEY_ECHO}\r\n")
        } else {
            String::new()
        };
        let mut response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n{key}Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        response
    }

    /// A page the client returned, encoded as the contract's JSON.
    fn page<T: serde::Serialize>((items, next): (Vec<T>, Option<String>)) -> serde_json::Value {
        serde_json::json!({"items": items, "nextCursor": next})
    }

    /// One request of the definition client whose answer may carry members it does not know.
    #[derive(Clone, Copy, Debug)]
    enum Read {
        Definition,
        Definitions,
        Revision,
        Revisions,
        Evaluation,
        Evaluations,
        Create,
        Publish,
        Enable,
    }

    impl Read {
        const ALL: [Self; 9] = [
            Self::Definition,
            Self::Definitions,
            Self::Revision,
            Self::Revisions,
            Self::Evaluation,
            Self::Evaluations,
            Self::Create,
            Self::Publish,
            Self::Enable,
        ];

        /// The valid answer, carrying a member this release does not know at its top level
        /// and in every object it nests.
        fn answer(self) -> serde_json::Value {
            match self {
                Self::Definition | Self::Enable => definition_answer(3),
                Self::Definitions => page_answer(&definition_answer(3)),
                Self::Revision => revision_answer(3),
                Self::Revisions => page_answer(&revision_answer(3)),
                Self::Evaluation => evaluation_answer(),
                Self::Evaluations => page_answer(&evaluation_answer()),
                // A create is revision 1; a publish over revision 2 is revision 3.
                Self::Create => publication_answer(1),
                Self::Publish => publication_answer(3),
            }
        }

        fn status(self) -> &'static str {
            match self {
                Self::Create | Self::Publish => "201 Created",
                _ => "200 OK",
            }
        }

        const fn keyed(self) -> bool {
            matches!(self, Self::Create | Self::Publish | Self::Enable)
        }

        /// The stage an invalid answer to this request names.
        const fn stage(self) -> Stage {
            match self {
                Self::Definition => Stage::DefinitionRead,
                Self::Definitions => Stage::DefinitionList,
                Self::Revision => Stage::RevisionRead,
                Self::Revisions => Stage::RevisionList,
                Self::Evaluation => Stage::EvaluationRead,
                Self::Evaluations => Stage::EvaluationList,
                Self::Create => Stage::DefinitionCreate,
                Self::Publish => Stage::RevisionPublish,
                Self::Enable => Stage::DefinitionChange,
            }
        }

        /// Sends the request to a server answering `response`: what the client returned,
        /// encoded as the contract's JSON.
        fn send(self, response: Vec<u8>) -> Result<serde_json::Value, WorkflowDefinitionFailure> {
            let server = ScriptedHttpServer::respond(response);
            let storage = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).expect("client");
            let api = WorkflowDefinitionApi::new(&server.api_url, "fixture-token", &storage)
                .expect("api");
            let digest = tree_digest();
            let publication = WorkflowPublication {
                tree_digest: &digest,
                bindings: None,
                executable_paths: None,
            };
            let read = match self {
                Self::Definition => api
                    .definition(ORGANIZATION, PROJECT, DEFINITION)
                    .map(|value| serde_json::json!(value)),
                Self::Definitions => api.definitions(ORGANIZATION, PROJECT, None, None).map(page),
                Self::Revision => api
                    .revision(ORGANIZATION, PROJECT, DEFINITION, 3)
                    .map(|value| serde_json::json!(value)),
                Self::Revisions => api
                    .revisions(ORGANIZATION, PROJECT, DEFINITION, None, None)
                    .map(page),
                Self::Evaluation => api
                    .evaluation(ORGANIZATION, PROJECT, DEFINITION, EVALUATION)
                    .map(|value| serde_json::json!(value)),
                Self::Evaluations => api
                    .evaluations(
                        ORGANIZATION,
                        PROJECT,
                        DEFINITION,
                        WorkflowEvaluationFilter::default(),
                        None,
                        None,
                    )
                    .map(page),
                Self::Create => api
                    .create_definition(ORGANIZATION, PROJECT, "create-key", publication, false)
                    .map(|(value, _)| serde_json::json!(value)),
                Self::Publish => api
                    .publish_revision(
                        ORGANIZATION,
                        PROJECT,
                        DEFINITION,
                        "publish-key",
                        2,
                        publication,
                    )
                    .map(|value| serde_json::json!(value)),
                Self::Enable => api
                    .enable(ORGANIZATION, PROJECT, DEFINITION, "enable-key")
                    .map(|value| serde_json::json!(value)),
            };
            server.finish_one();
            read
        }
    }

    // REQ-CLOUDDEF-23, REQ-COMPAT-20: a member a later Cloud release adds to a definition,
    // revision, evaluation or publication answer, at any depth, is ignored and the members
    // this release knows are read as sent.
    #[test]
    fn an_answer_carrying_members_this_release_does_not_know_is_read() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        for read in Read::ALL {
            let answer = read.answer();
            assert_eq!(
                read.send(response(
                    read.status(),
                    "application/json",
                    read.keyed(),
                    &answer
                )),
                Ok(known(answer)),
                "{read:?}"
            );
        }
    }

    /// One departure from a valid additive answer.
    #[derive(Debug)]
    enum Fault {
        /// The answer's top-level member is absent.
        Remove(&'static str),
        /// The member at this JSON pointer has this value.
        Set(&'static str, serde_json::Value),
        /// The answer is served under this media type.
        MediaType(&'static str),
        /// The keyed answer does not echo the request's `Idempotency-Key`.
        NoKeyEcho,
        /// The answer's body is these bytes, which are not JSON.
        Body(&'static [u8]),
    }

    // REQ-CLOUDDEF-23: what this release knows of an answer is still checked: its shape, its
    // identity against the request, its media type and a keyed change's key echo.
    // INV-CLI-18: each failed check names the read's stage and the check, a body that is not
    // JSON or does not fit the model retains its decoding error, and none prints the answer.
    #[test]
    fn an_additive_answer_failing_a_known_check_is_an_invalid_response() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let other_digest = format!("sha256:{}", "b".repeat(64));
        let cases = [
            (
                Read::Definition,
                Fault::Remove("currentRevision"),
                Check::BodyShape,
            ),
            (
                Read::Definition,
                Fault::Remove("executionPrincipalId"),
                Check::BodyShape,
            ),
            (
                Read::Definition,
                Fault::Set("/currentRevision", serde_json::json!(SENTINEL)),
                Check::BodyShape,
            ),
            (
                Read::Definition,
                Fault::Set("/state", serde_json::json!("deleted")),
                Check::BodyShape,
            ),
            (
                Read::Revisions,
                Fault::Set("/items/0/number", serde_json::json!(SENTINEL)),
                Check::BodyShape,
            ),
            (
                Read::Create,
                Fault::Body(b"<html>sentinel-a1b2c3</html>"),
                Check::BodySyntax,
            ),
            (
                Read::Evaluation,
                Fault::Body(br#"{"id": "wev_"#),
                Check::BodySyntax,
            ),
            (
                Read::Definitions,
                Fault::Set("/items/1/id", serde_json::json!(PROJECT)),
                Check::Identity,
            ),
            (
                Read::Definition,
                Fault::Set("/id", serde_json::json!("def_01k0z6r1w8f4jy2m7q9v3x5xyz")),
                Check::Identity,
            ),
            (
                Read::Definition,
                Fault::Set(
                    "/projectId",
                    serde_json::json!("prj_01k0z6r1w8f4jy2m7q9v3x5xyz"),
                ),
                Check::Identity,
            ),
            (
                Read::Revision,
                Fault::Set(
                    "/definitionId",
                    serde_json::json!("def_01k0z6r1w8f4jy2m7q9v3x5xyz"),
                ),
                Check::Identity,
            ),
            (
                Read::Revision,
                Fault::Set("/number", serde_json::json!(4)),
                Check::Identity,
            ),
            (
                Read::Evaluations,
                Fault::Set(
                    "/items/0/id",
                    serde_json::json!("run_01k0z6r1w8f4jy2m7q9v3x5abc"),
                ),
                Check::Identity,
            ),
            (
                Read::Publish,
                Fault::Set("/revision/treeDigest", serde_json::json!(other_digest)),
                Check::Identity,
            ),
            (
                Read::Enable,
                Fault::Set("/automaticStarts", serde_json::json!(false)),
                Check::Identity,
            ),
            (Read::Enable, Fault::NoKeyEcho, Check::IdempotencyKey),
            (
                Read::Definition,
                Fault::MediaType("text/plain"),
                Check::MediaType,
            ),
        ];
        for (read, fault, check) in cases {
            let mut answer = read.answer();
            let mut content_type = "application/json";
            let mut echo_key = read.keyed();
            let mut body = None;
            match &fault {
                Fault::Remove(member) => {
                    answer
                        .as_object_mut()
                        .and_then(|members| members.remove(*member))
                        .expect("member");
                }
                Fault::Set(pointer, value) => {
                    *answer.pointer_mut(pointer).expect("member") = value.clone();
                }
                Fault::MediaType(media_type) => content_type = media_type,
                Fault::NoKeyEcho => echo_key = false,
                Fault::Body(bytes) => body = Some(*bytes),
            }
            let response = match body {
                Some(body) => response_bytes(read.status(), content_type, echo_key, body),
                None => response(read.status(), content_type, echo_key, &answer),
            };
            let source = if matches!(check, Check::BodySyntax | Check::BodyShape) {
                Source::Json
            } else {
                Source::None
            };
            let invalid = invalid_response(read.send(response), read.stage(), check, source);
            assert_eq!(
                (invalid.status(), invalid.credential_rejected()),
                (None, false),
                "{read:?} {fault:?}"
            );
        }
    }

    fn tree_answer(digest: &str, state: &str, sealed_at: Option<&str>) -> serde_json::Value {
        serde_json::json!({
            "digest": digest, "state": state, "sizeBytes": 0, "fileCount": 0,
            "name": null, "createdAt": AT, "sealedAt": sealed_at,
        })
    }

    fn upload_answer(
        tree: serde_json::Value,
        capabilities: Vec<serde_json::Value>,
    ) -> serde_json::Value {
        serde_json::json!({
            "tree": tree, "capabilityExpiresAt": null, "capabilities": capabilities,
        })
    }

    /// Uploads the tree of `files` against a server answering `responses` in turn, one for
    /// each request the upload sends: the sealed tree's digest and whether it was sealed
    /// already.
    fn upload(
        files: &[WorkflowTreeFile<'_>],
        responses: Vec<Vec<u8>>,
    ) -> Result<(String, bool), WorkflowDefinitionFailure> {
        let server = ScriptedHttpServer::respond_in_sequence(responses);
        let storage = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp).expect("client");
        let api =
            WorkflowDefinitionApi::new(&server.api_url, "fixture-token", &storage).expect("api");
        let uploaded = api
            .upload_tree(ORGANIZATION, &tree_digest(), files, "seal-key")
            .map(|(tree, already_sealed)| (tree.digest, already_sealed));
        server.finish();
        uploaded
    }

    // REQ-CLOUDDEF-23: the tree upload and seal answers stay closed, since the CLI sends the
    // upload answer's signed capabilities' required headers exactly.
    // INV-CLI-18: a refused upload or seal answer names its stage.
    #[test]
    fn a_tree_upload_or_seal_answer_carrying_a_member_this_release_does_not_know_is_refused() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let digest = tree_digest();
        let additive = |mut answer: serde_json::Value| {
            answer
                .as_object_mut()
                .expect("object")
                .insert(ADDED.to_owned(), added());
            answer
        };
        // The upload answer, then the seal answer when the upload answer leaves a seal to send.
        let upload = |answers: &[&serde_json::Value]| {
            let responses = answers
                .iter()
                .enumerate()
                .map(|(index, answer)| response("200 OK", "application/json", index == 1, answer))
                .collect();
            upload(&[], responses)
        };

        let already_sealed = upload_answer(tree_answer(&digest, "sealed", Some(AT)), vec![]);
        assert_eq!(upload(&[&already_sealed]), Ok((digest.clone(), true)));
        invalid_response(
            upload(&[&additive(already_sealed)]),
            Stage::TreeUpload,
            Check::BodyClosed,
            Source::None,
        );

        let uploading = upload_answer(tree_answer(&digest, "uploading", None), vec![]);
        let sealed = tree_answer(&digest, "sealed", Some(AT));
        assert_eq!(upload(&[&uploading, &sealed]), Ok((digest.clone(), false)));
        invalid_response(
            upload(&[&uploading, &additive(sealed)]),
            Stage::TreeSeal,
            Check::BodyClosed,
            Source::None,
        );
    }

    // INV-CLI-18, REQ-CLOUDDEF-23: a tree upload whose answer, signed upload capability or
    // seal answer fails a check names the stage and the check, retains the error a capability
    // value was refused with, and prints nothing of the capability's URL or of the answer.
    #[test]
    fn a_signed_tree_upload_failing_a_check_names_its_stage_and_check() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let digest = tree_digest();
        let bytes = b"name: engineering\n";
        let file = WorkflowTreeFile {
            path: "workflow.yaml",
            bytes,
            executable: false,
        };
        let sha = lowercase_hex(&sha256(bytes));
        let length = bytes.len().to_string();
        let signed = format!("http://127.0.0.1:9/storage/{sha}?X-Amz-Signature={SENTINEL}");
        let capability = |sha: &str, url: &str, length: &str, content_type: &str| {
            serde_json::json!({
                "sha256": sha, "url": url,
                "requiredHeaders": {
                    "contentLength": length, "contentType": content_type, "ifNoneMatch": "*",
                    "xAmzChecksumSha256":
                        base64::engine::general_purpose::STANDARD.encode(sha256(bytes)),
                },
            })
        };
        let uploading = |capabilities| {
            response(
                "200 OK",
                "application/json",
                false,
                &upload_answer(tree_answer(&digest, "uploading", None), capabilities),
            )
        };
        let issued = |url: &str, length: &str, content_type: &str| {
            uploading(vec![capability(&sha, url, length, content_type)])
        };
        let seal = |echo_key, state, sealed_at| {
            response(
                "200 OK",
                "application/json",
                echo_key,
                &tree_answer(&digest, state, sealed_at),
            )
        };
        let mut mistyped = upload_answer(tree_answer(&digest, "uploading", None), vec![]);
        *mistyped.pointer_mut("/tree/fileCount").expect("member") = serde_json::json!(SENTINEL);
        let octets = "application/octet-stream";
        let cases = [
            (
                vec![response(
                    "200 OK",
                    "application/json",
                    false,
                    &upload_answer(
                        tree_answer(&format!("sha256:{}", "b".repeat(64)), "uploading", None),
                        vec![],
                    ),
                )],
                Stage::TreeUpload,
                Check::Identity,
                Source::None,
            ),
            (
                vec![uploading(vec![capability(
                    &"b".repeat(64),
                    &signed,
                    &length,
                    octets,
                )])],
                Stage::TreeUpload,
                Check::Identity,
                Source::None,
            ),
            (
                vec![response("200 OK", "application/json", false, &mistyped)],
                Stage::TreeUpload,
                Check::BodyShape,
                Source::Json,
            ),
            (
                vec![issued(&format!("http//{SENTINEL}"), &length, octets)],
                Stage::FileUpload,
                Check::CapabilityUrlSyntax,
                Source::Url,
            ),
            (
                vec![issued(
                    &format!(
                        "http://fixture:{SENTINEL}@127.0.0.1:9/storage/{sha}?X-Amz-Signature={SENTINEL}"
                    ),
                    &length,
                    octets,
                )],
                Stage::FileUpload,
                Check::CapabilityUrlPolicy,
                Source::None,
            ),
            (
                vec![issued(&signed, &(bytes.len() + 1).to_string(), octets)],
                Stage::FileUpload,
                Check::CapabilityHeaders,
                Source::None,
            ),
            (
                vec![issued(&signed, &length, &format!("text/plain\n{SENTINEL}"))],
                Stage::FileUpload,
                Check::CapabilityHeaderValue,
                Source::HeaderValue,
            ),
            (
                vec![uploading(vec![]), seal(false, "sealed", Some(AT))],
                Stage::TreeSeal,
                Check::IdempotencyKey,
                Source::None,
            ),
            (
                vec![uploading(vec![]), seal(true, "uploading", None)],
                Stage::TreeSeal,
                Check::Identity,
                Source::None,
            ),
        ];
        for (responses, stage, check, source) in cases {
            let invalid = invalid_response(upload(&[file], responses), stage, check, source);
            assert_eq!(
                (invalid.status(), invalid.credential_rejected()),
                (None, false)
            );
        }
    }
}
