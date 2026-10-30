use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use reqwest::header::HeaderValue;
use reqwest::{Method, StatusCode, Url};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::generated::models::run_linear_lifecycle::{
    OwnershipState as LifecycleState, ReasonCode as LifecycleReason,
};
use super::generated::models::run_linear_lifecycle_ownership_event::EventType as LifecycleEvent;
use super::generated::{apis, models};
use super::http_client::{HttpClient, generated_configuration};
use super::http_util::{self, BoundedBodyError, BufferedBlockingResponse};
use super::problem::{self, BAD_REQUEST, FORBIDDEN, JSON_MEDIA_TYPE, NOT_FOUND, UNAUTHORIZED};
use super::{HttpTransportPolicy, UnreachableCategory, classify_reqwest_error};
use um_support::wire_name;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const CREATE_ATTEMPTS: usize = 2;
const PRIVATE_CACHE_CONTROL: &str = "private, no-store";
const RUN_CREATION_REJECTED: &str =
    "https://api.usefulmachinery.com/problems/run-creation-rejected";
// 100 rows can each carry 16 KiB of context. Go's JSON encoder may expand
// HTML-sensitive bytes sixfold; workflow paths and display/placement names add
// less than 3 MiB at their admitted bounds. Keep the larger budget list-only.
const MAX_RUN_LIST_BODY_BYTES: usize = 16 * 1024 * 1024;

pub type Run = models::Run;
pub type RunList = models::RunList;
pub type RunObservation = models::RunObservation;
pub type RunLinearLifecycle = models::RunLinearLifecycle;
pub type RunLinearLifecycleOwnershipEvent = models::RunLinearLifecycleOwnershipEvent;
pub type RunLinearLifecycleUpdate = models::RunLinearLifecycleUpdate;
pub(crate) type RunLinearLifecycleUpdateList = models::RunLinearLifecycleUpdateList;
pub(crate) type RunLinearLifecycleRetryReceipt = models::RunLinearLifecycleRetryReceipt;
pub type RunState = models::run::State;
pub type RunCreationAcceptance = models::RunCreationAcceptance;
pub type RunCreationPending = models::RunCreationPending;
pub type RunArtifactDelivery = models::RunArtifactDelivery;
pub type ContinuationAdmissionViolation = models::ContinuationAdmissionViolation;
pub type RunContinuationEnvelope = models::RunContinuationEnvelope;
pub type RunContinuationReceipt = models::RunContinuationReceipt;
pub type RunContinuationRequest = models::RunContinuationRequest;
pub type RunContinuationDefinition = models::RunContinuationRequestDefinition;
pub type RunContinuationReplacement = models::RunContinuationRequestDefinitionOneOf;
pub type RunContinuationReplacementSource = models::RunContinuationRequestDefinitionOneOfReplaced;
pub type RunContinuationWorkspaceModified = models::RunContinuationWorkspaceModified;
pub type RunRetryReceipt = models::RunRetryReceipt;
pub type RunRetryState = models::run_retry_receipt::State;
pub type RunRetryRejection = models::run_retry_receipt::Rejection;
pub type RunRetryFailureCode = models::run_retry_failure::Code;
pub type RunCancellation = models::RunCancellation;
pub type RunInterruption = models::RunInterruption;
pub type RunInterruptionPhase = models::run_interruption::Phase;
pub type RunContinuationPreparation = models::run_continuation_workspace::Preparation;
pub type RunCancellationEnvelope = models::RunCancellationEnvelope;
pub type RunCancellationReceipt = models::RunCancellationReceipt;
pub type RunCancellationMode = models::run_cancellation_request::Mode;
pub type RunCancellationReceiptState = models::run_cancellation_receipt::State;
pub type RunCancellationReceiptMode = models::run_cancellation_receipt::Mode;
pub type RunCancellationEffectiveMode = models::run_cancellation::Mode;
pub type RunCancellationResolutionKind = models::run_cancellation_resolution::Kind;
pub type RunPublicationHandoffState = models::run_publication_handoff::State;

#[derive(Clone, Debug, PartialEq)]
pub enum RunRead {
    Materialized(Box<Run>),
    Pending(RunCreationPending),
}

pub struct CreateRunInput<'a> {
    pub project_id: &'a str,
    pub workflow_path: &'a str,
    pub source_branch: Option<&'a str>,
    pub workspace_branch: Option<&'a str>,
    pub workspace_commit: Option<&'a str>,
    pub display_name: Option<&'a str>,
    pub input_set_id: Option<&'a str>,
    pub publish_export: Option<&'a str>,
    pub publish_target: Option<super::publications::ExistingPublicationTarget<'a>>,
    pub integration_context: Option<&'a BTreeMap<String, String>>,
}

pub struct RunListFilter<'a> {
    pub limit: Option<u16>,
    pub cursor: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub state_group: Option<&'a str>,
    pub created_after: Option<&'a str>,
    pub context: &'a [(String, String)],
}

pub struct RunApi<'a> {
    pub(super) configuration: apis::configuration::Configuration,
    pub(super) storage_transport: &'a HttpClient,
    pub(super) transport_policy: HttpTransportPolicy,
}

impl<'a> RunApi<'a> {
    pub fn new(
        api_url: &str,
        access_token: &str,
        transport_policy: HttpTransportPolicy,
        storage_transport: &'a HttpClient,
    ) -> Result<Self, RunApiError> {
        let parsed = Url::parse(api_url).map_err(|_| RunApiError::InvalidEndpoint)?;
        if !transport_policy.permits(&parsed) {
            return Err(if parsed.scheme() == "http" {
                RunApiError::InsecureHttp
            } else {
                RunApiError::InvalidEndpoint
            });
        }
        if parsed.cannot_be_a_base() || parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(RunApiError::InvalidEndpoint);
        }
        let configuration =
            generated_configuration(api_url, access_token, transport_policy, REQUEST_TIMEOUT)
                .map_err(RunApiError::BuildClient)?;
        Ok(Self {
            configuration,
            storage_transport,
            transport_policy,
        })
    }

    pub fn create(
        &self,
        organization: &str,
        idempotency_key: &str,
        input: CreateRunInput<'_>,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunCreationAcceptance, RunFailure> {
        if input.workspace_branch.is_some() && input.workspace_commit.is_some()
            || input
                .workspace_branch
                .is_some_and(|branch| branch.is_empty())
            || input
                .workspace_commit
                .is_some_and(|commit| !lowercase_hex(commit, 40))
        {
            return Err(RunFailure::InvalidInput);
        }
        let mut request = models::CreateRunRequest::new(
            input.project_id.to_owned(),
            input.workflow_path.to_owned(),
        );
        request.source_branch = input.source_branch.map(str::to_owned);
        request.workspace_source = match (input.workspace_branch, input.workspace_commit) {
            (Some(branch), None) => Some(Box::new(
                models::WorkspaceSourceSelection::WorkspaceSourceSelectionOneOf(Box::new(
                    models::WorkspaceSourceSelectionOneOf::new(
                        models::workspace_source_selection_one_of::Kind::Branch,
                        branch.to_owned(),
                    ),
                )),
            )),
            (None, Some(commit)) => Some(Box::new(
                models::WorkspaceSourceSelection::WorkspaceSourceSelectionOneOf1(Box::new(
                    models::WorkspaceSourceSelectionOneOf1::new(
                        models::workspace_source_selection_one_of_1::Kind::Commit,
                        commit.to_owned(),
                    ),
                )),
            )),
            (None, None) | (Some(_), Some(_)) => None,
        };
        request.display_name = input.display_name.map(str::to_owned);
        request.input_set_id = input.input_set_id.map(|id| Some(id.to_owned()));
        request.publication = input.publish_export.map(|name| {
            let mut publication = models::RunPublicationRequest::new(name.to_owned());
            publication.target = input
                .publish_target
                .map(|target| Box::new(super::publications::target_selection(target)));
            Box::new(publication)
        });
        request.integration_context = input.integration_context.map(|context| {
            Some(
                context
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            )
        });
        let endpoint = self.collection_endpoint(organization);
        let response = self.send_api_request(
            StatusCode::ACCEPTED,
            Some(idempotency_key),
            begin_dispatch,
            || {
                self.request(Method::POST, &endpoint)
                    .header("Idempotency-Key", idempotency_key)
                    .json(&request)
            },
        )?;
        decode_create_response(response, organization, idempotency_key)
    }

    pub(super) fn send_api_request(
        &self,
        success_status: StatusCode,
        idempotency_key: Option<&str>,
        begin_dispatch: impl Fn() -> bool,
        build: impl FnMut() -> reqwest::blocking::RequestBuilder,
    ) -> Result<ReceivedResponse, RunFailure> {
        self.send_api_request_with_statuses(
            &[success_status],
            idempotency_key,
            begin_dispatch,
            build,
        )
        .map(|(response, _)| response)
        .map_err(|(failure, _)| failure)
    }

    fn send_api_request_with_statuses(
        &self,
        success_statuses: &[StatusCode],
        idempotency_key: Option<&str>,
        begin_dispatch: impl Fn() -> bool,
        mut build: impl FnMut() -> reqwest::blocking::RequestBuilder,
    ) -> Result<(ReceivedResponse, bool), (RunFailure, bool)> {
        let mut ambiguous_attempt = false;
        let attempts = if idempotency_key.is_some() {
            CREATE_ATTEMPTS
        } else {
            1
        };
        let mut last_transport_failure = UnreachableCategory::Connection;
        for attempt in 0..attempts {
            if !begin_dispatch() {
                return Err((RunFailure::Interrupted, ambiguous_attempt));
            }
            let response = match build().send() {
                Ok(response) => response,
                Err(error) => {
                    let category = classify_reqwest_error(&error);
                    last_transport_failure = category;
                    if idempotency_key.is_some()
                        && http_util::can_retry_ambiguous_mutation(
                            attempt,
                            CREATE_ATTEMPTS,
                            category,
                        )
                    {
                        ambiguous_attempt = true;
                        um_support::sleep(um_support::short_retry_delay());
                        continue;
                    }
                    return Err((RunFailure::Unreachable(category), ambiguous_attempt));
                }
            };
            let status = response.status();
            if success_statuses.contains(&status)
                && let Some(idempotency_key) = idempotency_key
            {
                require_exact_header(
                    response.headers().get_all("Idempotency-Key").iter(),
                    idempotency_key,
                )
                .map_err(|failure| (failure, ambiguous_attempt))?;
            }
            match http_util::buffer_blocking_response(response) {
                Ok(response) => return Ok((response, ambiguous_attempt)),
                Err(BoundedBodyError::TooLarge) => {
                    return Err((
                        RunFailure::protocol(status == StatusCode::UNAUTHORIZED),
                        ambiguous_attempt,
                    ));
                }
                Err(BoundedBodyError::Transport(error))
                    if idempotency_key.is_some() && success_statuses.contains(&status) =>
                {
                    let category = classify_reqwest_error(&error);
                    last_transport_failure = category;
                    if http_util::can_retry_ambiguous_mutation(attempt, CREATE_ATTEMPTS, category) {
                        ambiguous_attempt = true;
                        um_support::sleep(um_support::short_retry_delay());
                        continue;
                    }
                    return Err((RunFailure::Unreachable(category), ambiguous_attempt));
                }
                Err(BoundedBodyError::Transport(_)) if status == StatusCode::UNAUTHORIZED => {
                    return Err((RunFailure::protocol(true), ambiguous_attempt));
                }
                Err(BoundedBodyError::Transport(error)) => {
                    return Err((
                        RunFailure::Unreachable(if status.is_server_error() {
                            UnreachableCategory::Server
                        } else {
                            classify_reqwest_error(&error)
                        }),
                        ambiguous_attempt,
                    ));
                }
            }
        }
        Err((
            RunFailure::Unreachable(last_transport_failure),
            ambiguous_attempt,
        ))
    }

    pub fn request_continuation(
        &self,
        organization: &str,
        run_id: &str,
        key: &str,
        request: &RunContinuationRequest,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunContinuationEnvelope, RunFailure> {
        let endpoint = format!(
            "{}/{}/continuation-requests",
            self.collection_endpoint(organization),
            apis::urlencode(run_id)
        );
        let (response, ambiguous) = self
            .send_api_request_with_statuses(
                &[StatusCode::CREATED],
                Some(key),
                begin_dispatch,
                || {
                    self.request(Method::POST, &endpoint)
                        .header("Idempotency-Key", key)
                        .json(request)
                },
            )
            .map_err(|(failure, ambiguous)| {
                if ambiguous {
                    RunFailure::ContinuationAcceptanceUnknown
                } else {
                    failure
                }
            })?;
        // No unsuccessful replay, including 404, establishes whether the first
        // transport-ambiguous request was admitted.
        if ambiguous && response.status != StatusCode::CREATED {
            return Err(RunFailure::ContinuationAcceptanceUnknown);
        }
        if response.status != StatusCode::CREATED {
            return Err(classify_continuation_failure(&response));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.idempotency_keys.iter(), key)?;
        require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
        if !valid_recovery_json_shape(&response.body) {
            return Err(RunFailure::protocol(false));
        }
        let envelope: RunContinuationEnvelope =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        validate_continuation_envelope(&envelope, run_id, None, Some(request))?;
        let location = format!(
            "/v1/organizations/{}/runs/{}/continuation-requests/{}",
            apis::urlencode(&envelope.request.organization_id),
            apis::urlencode(run_id),
            apis::urlencode(&envelope.request.request_id)
        );
        require_exact_header(response.locations.iter(), &location)?;
        Ok(envelope)
    }

    pub fn get_continuation(
        &self,
        organization: &str,
        run_id: &str,
        request_id: &str,
        timeout: Option<Duration>,
    ) -> Result<RunContinuationEnvelope, RunFailure> {
        let endpoint = format!(
            "{}/{}/continuation-requests/{}",
            self.collection_endpoint(organization),
            apis::urlencode(run_id),
            apis::urlencode(request_id)
        );
        let response = self.read_response(&endpoint, timeout)?;
        if response.status != StatusCode::OK {
            return Err(classify_failure(&response, RunOperation::Get));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
        if !valid_recovery_json_shape(&response.body) {
            return Err(RunFailure::protocol(false));
        }
        let envelope: RunContinuationEnvelope =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        validate_continuation_envelope(&envelope, run_id, Some(request_id), None)?;
        Ok(envelope)
    }

    pub fn request_retry(
        &self,
        organization: &str,
        run_id: &str,
        key: &str,
        expected_version: Option<i64>,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunRetryReceipt, RunFailure> {
        let endpoint = format!(
            "{}/{}/retry",
            self.collection_endpoint(organization),
            apis::urlencode(run_id)
        );
        let mut request = models::RunRetryRequest::new();
        request.expected_version = expected_version;
        let (response, ambiguous_attempt) = self
            .send_api_request_with_statuses(
                &[StatusCode::ACCEPTED],
                Some(key),
                begin_dispatch,
                || {
                    self.request(Method::POST, &endpoint)
                        .header("Idempotency-Key", key)
                        .json(&request)
                },
            )
            .map_err(|(failure, ambiguous)| {
                if ambiguous && failure.credential_rejected() {
                    RunFailure::RetryAmbiguousAuthentication
                } else {
                    failure
                }
            })?;
        if response.status == StatusCode::UNAUTHORIZED && ambiguous_attempt {
            return Err(RunFailure::RetryAmbiguousAuthentication);
        }
        if response.status == StatusCode::TOO_MANY_REQUESTS {
            return Err(if ambiguous_attempt {
                RunFailure::RetryAmbiguousRateLimited
            } else {
                RunFailure::Unreachable(UnreachableCategory::RateLimited)
            });
        }
        if response.status != StatusCode::ACCEPTED {
            return Err(classify_retry_failure(&response));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.idempotency_keys.iter(), key)?;
        require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
        let receipt: RunRetryReceipt =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        validate_retry_receipt(&receipt, run_id)?;
        let location = format!(
            "/v1/organizations/{}/runs/{}/retry-requests/{}",
            apis::urlencode(&receipt.organization_id),
            apis::urlencode(run_id),
            apis::urlencode(&receipt.id)
        );
        require_exact_header(response.locations.iter(), &location)?;
        Ok(receipt)
    }

    pub fn get_retry(
        &self,
        organization: &str,
        run_id: &str,
        request_id: &str,
        timeout: Option<Duration>,
    ) -> Result<RunRetryReceipt, RunFailure> {
        let endpoint = format!(
            "{}/{}/retry-requests/{}",
            self.collection_endpoint(organization),
            apis::urlencode(run_id),
            apis::urlencode(request_id)
        );
        let response = self.read_response(&endpoint, timeout)?;
        if response.status != StatusCode::OK {
            return Err(classify_retry_failure(&response));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
        let receipt: RunRetryReceipt =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        validate_retry_receipt(&receipt, run_id)?;
        if receipt.id != request_id {
            return Err(RunFailure::protocol(false));
        }
        Ok(receipt)
    }

    pub fn get_linear_lifecycle(
        &self,
        organization: &str,
        run_id: &str,
    ) -> Result<RunLinearLifecycle, RunFailure> {
        let endpoint = format!(
            "{}/{}/linear-lifecycle",
            self.collection_endpoint(organization),
            apis::urlencode(run_id)
        );
        let response = self.read_response(&endpoint, None)?;
        if response.status != StatusCode::OK {
            return Err(classify_failure(&response, RunOperation::Get));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
        let view: RunLinearLifecycle =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        if validate_linear_lifecycle(&view) {
            Ok(view)
        } else {
            Err(RunFailure::protocol(false))
        }
    }

    pub fn list_linear_lifecycle_updates(
        &self,
        organization: &str,
        run_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<RunLinearLifecycleUpdateList, RunFailure> {
        let mut endpoint = Url::parse(&format!(
            "{}/{}/linear-lifecycle/updates",
            self.collection_endpoint(organization),
            apis::urlencode(run_id)
        ))
        .map_err(|_| RunFailure::protocol(false))?;
        // Opening the query writes a `?` even when no pair follows.
        if limit.is_some() || cursor.is_some() {
            let mut query = endpoint.query_pairs_mut();
            if let Some(limit) = limit {
                query.append_pair("limit", &limit.to_string());
            }
            if let Some(cursor) = cursor {
                query.append_pair("cursor", cursor);
            }
        }
        decode_linear_lifecycle_update_list(self.read_response(endpoint.as_str(), None)?)
    }

    /// Sends one keyed owner retry; transport recovery replays the same key, and
    /// no unsuccessful replay establishes whether an ambiguous first request
    /// opened the cycle.
    pub fn retry_linear_lifecycle_update(
        &self,
        organization: &str,
        run_id: &str,
        update_id: &str,
        key: &str,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunLinearLifecycleRetryReceipt, RunFailure> {
        let endpoint = format!(
            "{}/{}/linear-lifecycle/updates/{}/retry",
            self.collection_endpoint(organization),
            apis::urlencode(run_id),
            apis::urlencode(update_id)
        );
        let (response, ambiguous) = self
            .send_api_request_with_statuses(
                &[StatusCode::ACCEPTED],
                Some(key),
                begin_dispatch,
                || {
                    self.request(Method::POST, &endpoint)
                        .header("Idempotency-Key", key)
                },
            )
            .map_err(|(failure, ambiguous)| {
                if ambiguous {
                    RunFailure::LinearLifecycleRetryAcceptanceUnknown
                } else {
                    failure
                }
            })?;
        if response.status != StatusCode::ACCEPTED {
            return Err(if ambiguous {
                RunFailure::LinearLifecycleRetryAcceptanceUnknown
            } else {
                classify_failure(&response, RunOperation::LinearLifecycleRetry)
            });
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.idempotency_keys.iter(), key)?;
        require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
        let receipt: RunLinearLifecycleRetryReceipt =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        validate_linear_lifecycle_retry_receipt(&receipt, run_id, update_id)?;
        Ok(receipt)
    }

    pub fn cancel(
        &self,
        organization: &str,
        run_id: &str,
        key: &str,
        mode: RunCancellationMode,
        begin_dispatch: impl Fn() -> bool,
    ) -> Result<RunCancellationEnvelope, RunFailure> {
        let endpoint = format!(
            "{}/{}/cancellation-requests",
            self.collection_endpoint(organization),
            apis::urlencode(run_id)
        );
        let (response, _) = self
            .send_api_request_with_statuses(
                &[StatusCode::OK, StatusCode::ACCEPTED],
                Some(key),
                begin_dispatch,
                || {
                    self.request(Method::POST, &endpoint)
                        .header("Idempotency-Key", key)
                        .json(&models::RunCancellationRequest::new(mode))
                },
            )
            .map_err(|(failure, _)| failure)?;
        let status = response.status;
        if !matches!(status, StatusCode::OK | StatusCode::ACCEPTED) {
            return Err(classify_failure(&response, RunOperation::Create));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.idempotency_keys.iter(), key)?;
        require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
        if !valid_recovery_json_shape(&response.body) {
            return Err(RunFailure::protocol(false));
        }
        let envelope: RunCancellationEnvelope =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        let expected_location = format!(
            "/v1/organizations/{}/runs/{}/cancellation-requests/{}",
            apis::urlencode(&envelope.request.organization_id),
            apis::urlencode(run_id),
            apis::urlencode(&envelope.request.id)
        );
        let envelope = validate_cancellation_envelope(envelope, run_id, Some(mode), Some(status))?;
        require_exact_header(response.locations.iter(), &expected_location)?;
        Ok(envelope)
    }

    pub fn get_cancellation(
        &self,
        organization: &str,
        run_id: &str,
        request_id: &str,
        timeout: Option<Duration>,
    ) -> Result<RunCancellationEnvelope, RunFailure> {
        let endpoint = format!(
            "{}/{}/cancellation-requests/{}",
            self.collection_endpoint(organization),
            apis::urlencode(run_id),
            apis::urlencode(request_id)
        );
        let response = self.read_response(&endpoint, timeout)?;
        if response.status != StatusCode::OK {
            return Err(classify_failure(&response, RunOperation::Get));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
        if !valid_recovery_json_shape(&response.body) {
            return Err(RunFailure::protocol(false));
        }
        let envelope =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        let envelope = validate_cancellation_envelope(envelope, run_id, None, None)?;
        if envelope.request.id != request_id {
            return Err(RunFailure::protocol(false));
        }
        Ok(envelope)
    }

    pub fn list(
        &self,
        organization: &str,
        filter: RunListFilter<'_>,
    ) -> Result<RunList, RunFailure> {
        let mut endpoint = Url::parse(&self.collection_endpoint(organization))
            .map_err(|_| RunFailure::protocol(false))?;
        {
            let mut query = endpoint.query_pairs_mut();
            if let Some(limit) = filter.limit {
                query.append_pair("limit", &limit.to_string());
            }
            if let Some(cursor) = filter.cursor {
                query.append_pair("cursor", cursor);
            }
            if let Some(project_id) = filter.project_id {
                query.append_pair("projectId", project_id);
            }
            if let Some(state_group) = filter.state_group {
                query.append_pair("stateGroup", state_group);
            }
            if let Some(created_after) = filter.created_after {
                query.append_pair("createdAfter", created_after);
            }
            for (key, value) in filter.context {
                query.append_pair("integrationContext", &format!("{key}={value}"));
            }
        }
        let response =
            self.read_response_with_limit(endpoint.as_str(), None, MAX_RUN_LIST_BODY_BYTES)?;
        if response.status != StatusCode::OK {
            return Err(classify_failure(&response, RunOperation::Get));
        }
        require_media_type(&response, JSON_MEDIA_TYPE, false)?;
        require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
        let list: RunList =
            serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
        if list.items.len() > 100
            || list.items.iter().any(|item| {
                !um_support::valid_typed_id(&item.id, "run_")
                    || !um_support::valid_typed_id(&item.project_id, "prj_")
            })
        {
            return Err(RunFailure::protocol(false));
        }
        Ok(list)
    }

    pub fn get(&self, organization: &str, run_id: &str) -> Result<RunRead, RunFailure> {
        self.get_with_timeout(organization, run_id, None)
    }

    pub fn get_with_timeout(
        &self,
        organization: &str,
        run_id: &str,
        timeout: Option<Duration>,
    ) -> Result<RunRead, RunFailure> {
        let endpoint = format!(
            "{}/{}",
            self.collection_endpoint(organization),
            apis::urlencode(run_id)
        );
        decode_get_response(self.read_response(&endpoint, timeout)?, run_id)
    }

    fn read_response(
        &self,
        endpoint: &str,
        timeout: Option<Duration>,
    ) -> Result<ReceivedResponse, RunFailure> {
        self.read_response_with_limit(endpoint, timeout, http_util::MAX_RESPONSE_BODY_BYTES)
    }

    fn read_response_with_limit(
        &self,
        endpoint: &str,
        timeout: Option<Duration>,
        limit: usize,
    ) -> Result<ReceivedResponse, RunFailure> {
        let mut request = self.request(Method::GET, endpoint);
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        let response = request
            .send()
            .map_err(|error| RunFailure::Unreachable(classify_reqwest_error(&error)))?;
        let status = response.status();
        let limit = if status == StatusCode::OK {
            limit
        } else {
            http_util::MAX_RESPONSE_BODY_BYTES
        };
        http_util::buffer_blocking_response_with_limit(response, limit).map_err(|error| match error
        {
            BoundedBodyError::TooLarge => RunFailure::protocol(status == StatusCode::UNAUTHORIZED),
            BoundedBodyError::Transport(_) if status == StatusCode::UNAUTHORIZED => {
                RunFailure::protocol(true)
            }
            BoundedBodyError::Transport(error) => {
                RunFailure::Unreachable(if status.is_server_error() {
                    UnreachableCategory::Server
                } else {
                    classify_reqwest_error(&error)
                })
            }
        })
    }

    fn collection_endpoint(&self, organization: &str) -> String {
        format!(
            "{}/v1/organizations/{}/runs",
            self.configuration.base_path.trim_end_matches('/'),
            apis::urlencode(organization)
        )
    }

    pub(super) fn request(
        &self,
        method: Method,
        endpoint: &str,
    ) -> reqwest::blocking::RequestBuilder {
        super::generated_api_request(&self.configuration, method, endpoint)
    }
}

impl Drop for RunApi<'_> {
    fn drop(&mut self) {
        super::clear_generated_access_token(&mut self.configuration);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum RunFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    Conflict,
    /// Reviewed input-admission problem fields only; never carry server prose.
    InputAdmissionRejected {
        status: u16,
        problem_type: &'static str,
        diagnostic: Option<(&'static str, String)>,
    },
    /// Reviewed source-admission problem fields only; never carry server prose.
    /// `request_id` is the response's `X-Request-ID` when it is a `req_` typed id.
    AdmissionRejected {
        status: u16,
        problem_type: &'static str,
        request_id: Option<String>,
    },
    IdempotencyConflict,
    CreationRejected,
    RetryConflict(RetryConflict),
    RetryAfter(Duration),
    RetryAmbiguousRateLimited,
    RetryAmbiguousAuthentication,
    ContinuationAcceptanceUnknown,
    LinearLifecycleRetryAcceptanceUnknown,
    ContinuationAdmissionInvalid(Vec<models::ContinuationAdmissionViolation>),
    Gone,
    Unreachable(UnreachableCategory),
    InputUploadRejected,
    InputDownloadRejected,
    Interrupted,
    Protocol {
        credential_rejected: bool,
    },
}

impl RunFailure {
    pub fn credential_rejected(&self) -> bool {
        matches!(
            self,
            Self::Unauthenticated
                | Self::Protocol {
                    credential_rejected: true
                }
        )
    }

    pub fn retryable_observation(&self) -> bool {
        matches!(self, Self::Unreachable(category) if category.retryable_observation())
    }

    pub(super) fn protocol(credential_rejected: bool) -> Self {
        Self::Protocol {
            credential_rejected,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryConflict {
    TriggerSlot,
    Pending,
    Idempotency,
}

pub(super) type ReceivedResponse = BufferedBlockingResponse;

fn classify_retry_failure(response: &ReceivedResponse) -> RunFailure {
    if response.status == StatusCode::CONFLICT {
        return match problem::decode_header_parts(
            &response.body,
            response.status,
            response.content_type.as_ref(),
        ) {
            Ok(problem) => match problem.r#type.as_str() {
                "https://api.usefulmachinery.com/problems/trigger-active-run" => {
                    RunFailure::RetryConflict(RetryConflict::TriggerSlot)
                }
                "https://api.usefulmachinery.com/problems/run-retry-pending" => {
                    RunFailure::RetryConflict(RetryConflict::Pending)
                }
                "https://api.usefulmachinery.com/problems/idempotency-conflict" => {
                    RunFailure::RetryConflict(RetryConflict::Idempotency)
                }
                _ => RunFailure::Conflict,
            },
            Err(_) => RunFailure::protocol(false),
        };
    }
    if matches!(
        response.status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    ) && let Some(delay) = response
        .retry_afters
        .first()
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
    {
        return RunFailure::RetryAfter(Duration::from_secs(delay));
    }
    classify_failure(response, RunOperation::Get)
}

fn classify_continuation_failure(response: &ReceivedResponse) -> RunFailure {
    if response.status == StatusCode::CONFLICT {
        return match problem::decode_header_parts(
            &response.body,
            response.status,
            response.content_type.as_ref(),
        ) {
            Ok(problem)
                if problem.r#type
                    == "https://api.usefulmachinery.com/problems/continuation-admission-invalid" =>
            {
                problem
                    .continuation_violations
                    .filter(|violations| {
                        !violations.is_empty()
                            && violations.iter().all(|violation| {
                                violation.recovery_reason.is_none_or(|reason| {
                                    let code = match violation.code {
                                        models::continuation_admission_violation::Code::RetainedWorkspaceUnavailable => "retained_workspace_unavailable",
                                        models::continuation_admission_violation::Code::OwnershipUnproven => "ownership_unproven",
                                        _ => return false,
                                    };
                                    valid_recovery_reason_for_code(code, reason)
                                })
                            })
                    })
                    .map_or(
                        RunFailure::protocol(false),
                        RunFailure::ContinuationAdmissionInvalid,
                    )
            }
            Ok(problem)
                if problem.r#type
                    == "https://api.usefulmachinery.com/problems/idempotency-conflict" =>
            {
                RunFailure::IdempotencyConflict
            }
            Ok(_) => RunFailure::Conflict,
            Err(_) => RunFailure::protocol(false),
        };
    }
    // A continuation names no input set, so a gone input is never an input
    // admission rejection here.
    if response.status == StatusCode::GONE {
        return validated_problem_failure(response, None, RunFailure::Gone, false);
    }
    classify_failure(response, RunOperation::Create)
}

fn validate_continuation_envelope(
    envelope: &RunContinuationEnvelope,
    run_id: &str,
    request_id: Option<&str>,
    submitted: Option<&RunContinuationRequest>,
) -> Result<(), RunFailure> {
    let receipt = &envelope.request;
    if !um_support::valid_typed_id(&receipt.request_id, "cmd_")
        || request_id.is_some_and(|id| id != receipt.request_id)
        || !um_support::valid_typed_id(&receipt.organization_id, "org_")
        || receipt.run_id != run_id
        || !um_support::valid_typed_id(&receipt.attempt_id, "atm_")
        || receipt.attempt_number < 2
        || !valid_timestamp(&receipt.accepted_at)
        || envelope.run.id != run_id
        || envelope.run.organization_id != receipt.organization_id
        || envelope.run.current_attempt_number < receipt.attempt_number
        || (request_id.is_none() && envelope.run.current_attempt_number != receipt.attempt_number)
        || submitted.is_some_and(|submitted| {
            envelope
                .run
                .continuation
                .as_ref()
                .is_none_or(|continuation| continuation.request.as_ref() != submitted)
        })
        || (envelope.run.current_attempt_number == receipt.attempt_number
            && (envelope.run.current_attempt_id != receipt.attempt_id
                || envelope
                    .run
                    .continuation
                    .as_ref()
                    .is_none_or(|continuation| {
                        continuation.reexecuted_steps != receipt.reexecuted_steps
                            || continuation.inherited_steps != receipt.inherited_steps
                    })))
    {
        return Err(RunFailure::protocol(false));
    }
    validate_run((*envelope.run).clone(), run_id).map(|_| ())
}

fn validate_retry_receipt(receipt: &RunRetryReceipt, run_id: &str) -> Result<(), RunFailure> {
    let valid = um_support::valid_typed_id(&receipt.id, "cmd_")
        && um_support::valid_typed_id(&receipt.organization_id, "org_")
        && receipt.run_id == run_id
        && um_support::valid_typed_id(run_id, "run_")
        && receipt.observed_version >= 0
        && valid_timestamp(&receipt.accepted_at)
        && receipt.resolved_at.as_deref().is_none_or(valid_timestamp)
        && receipt
            .attempt_id
            .as_deref()
            .is_none_or(|id| um_support::valid_typed_id(id, "atm_"))
        && receipt.attempt_number.is_none_or(|number| number > 0)
        && receipt.run_version.is_none_or(|version| version > 0)
        && match receipt.state {
            RunRetryState::Pending => {
                receipt.resolved_at.is_none()
                    && receipt.rejection.is_none()
                    && receipt.failure.is_none()
                    && receipt.attempt_id.is_none()
                    && receipt.attempt_number.is_none()
                    && receipt.run_version.is_none()
            }
            RunRetryState::Applied => {
                receipt.resolved_at.is_some()
                    && receipt.rejection.is_none()
                    && receipt.failure.is_none()
                    && receipt.fault.is_none()
                    && receipt.attempt_id.is_some()
                    && receipt.attempt_number.is_some()
                    && receipt.run_version.is_some()
            }
            RunRetryState::Rejected => {
                receipt.resolved_at.is_some()
                    && receipt.rejection.is_some()
                    && receipt.failure.is_none()
                    && receipt.fault.is_none()
                    && receipt.attempt_id.is_none()
                    && receipt.attempt_number.is_none()
                    && receipt.run_version.is_none()
            }
            RunRetryState::Failed => {
                receipt.resolved_at.is_some()
                    && receipt.failure.as_deref().is_some_and(valid_retry_failure)
                    && receipt.rejection.is_none()
                    && receipt.fault.is_none()
                    && receipt.attempt_id.is_none()
                    && receipt.attempt_number.is_none()
                    && receipt.run_version.is_none()
            }
        };
    if valid {
        Ok(())
    } else {
        Err(RunFailure::protocol(false))
    }
}

fn valid_retry_failure(failure: &models::RunRetryFailure) -> bool {
    failure
        .diagnostic
        .error_type
        .as_deref()
        .is_some_and(um_support::is_diagnostic_slug)
}

fn decode_create_response(
    response: ReceivedResponse,
    organization: &str,
    expected_idempotency_key: &str,
) -> Result<RunCreationAcceptance, RunFailure> {
    if response.status != StatusCode::ACCEPTED {
        return Err(classify_failure(&response, RunOperation::Create));
    }
    require_media_type(&response, JSON_MEDIA_TYPE, false)?;
    require_exact_header(response.idempotency_keys.iter(), expected_idempotency_key)?;
    let acceptance: RunCreationAcceptance =
        serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
    validate_acceptance(acceptance, organization, &response.locations)
}

fn decode_get_response(
    response: ReceivedResponse,
    requested_run_id: &str,
) -> Result<RunRead, RunFailure> {
    match response.status {
        StatusCode::OK => {
            require_media_type(&response, JSON_MEDIA_TYPE, false)?;
            require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
            if !valid_recovery_json_shape(&response.body) {
                return Err(RunFailure::protocol(false));
            }
            let run =
                serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
            validate_run(run, requested_run_id)
                .map(Box::new)
                .map(RunRead::Materialized)
        }
        StatusCode::ACCEPTED => {
            require_media_type(&response, JSON_MEDIA_TYPE, false)?;
            require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
            let pending: RunCreationPending =
                serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
            if pending.run_id == requested_run_id
                && um_support::valid_typed_id(&pending.run_id, "run_")
            {
                Ok(RunRead::Pending(pending))
            } else {
                Err(RunFailure::protocol(false))
            }
        }
        StatusCode::CONFLICT => {
            require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
            Err(validated_problem_failure(
                &response,
                Some(RUN_CREATION_REJECTED),
                RunFailure::CreationRejected,
                false,
            ))
        }
        _ => Err(classify_failure(&response, RunOperation::Get)),
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum RunOperation {
    Create,
    Get,
    Input,
    LinearLifecycleRetry,
}

pub(super) fn classify_failure(response: &ReceivedResponse, operation: RunOperation) -> RunFailure {
    match response.status {
        StatusCode::BAD_REQUEST => validated_problem_failure(
            response,
            (operation != RunOperation::Input).then_some(BAD_REQUEST),
            RunFailure::InvalidInput,
            false,
        ),
        StatusCode::UNAUTHORIZED => validated_problem_failure(
            response,
            Some(UNAUTHORIZED),
            RunFailure::Unauthenticated,
            true,
        ),
        StatusCode::FORBIDDEN => {
            validated_problem_failure(response, Some(FORBIDDEN), RunFailure::Forbidden, false)
        }
        StatusCode::NOT_FOUND => {
            validated_problem_failure(response, Some(NOT_FOUND), RunFailure::NotFound, false)
        }
        // Every retry rejection is one conflict, whatever problem type it carries.
        StatusCode::CONFLICT if operation == RunOperation::LinearLifecycleRetry => {
            validated_problem_failure(response, None, RunFailure::Conflict, false)
        }
        StatusCode::CONFLICT if matches!(operation, RunOperation::Create | RunOperation::Input) => {
            match problem::decode_header_parts(
                &response.body,
                response.status,
                response.content_type.as_ref(),
            ) {
                Ok(problem)
                    if problem.r#type
                        == "https://api.usefulmachinery.com/problems/idempotency-conflict" =>
                {
                    RunFailure::IdempotencyConflict
                }
                Ok(problem) if operation == RunOperation::Create => {
                    input_admission_rejection(response, problem.clone())
                        .or_else(|| source_admission_rejection(response, problem))
                        .unwrap_or(RunFailure::Conflict)
                }
                Ok(_) => RunFailure::Conflict,
                Err(_) => RunFailure::protocol(false),
            }
        }
        StatusCode::UNPROCESSABLE_ENTITY if operation == RunOperation::Create => {
            match problem::decode_header_parts(
                &response.body,
                response.status,
                response.content_type.as_ref(),
            ) {
                Ok(problem) => {
                    source_admission_rejection(response, problem).unwrap_or(RunFailure::Conflict)
                }
                Err(_) => RunFailure::protocol(false),
            }
        }
        StatusCode::GONE if matches!(operation, RunOperation::Create | RunOperation::Input) => {
            if operation == RunOperation::Create
                && let Ok(problem) = problem::decode_header_parts(
                    &response.body,
                    response.status,
                    response.content_type.as_ref(),
                )
                && let Some(rejection) = input_admission_rejection(response, problem)
            {
                return rejection;
            }
            validated_problem_failure(response, None, RunFailure::Gone, false)
        }
        StatusCode::PAYLOAD_TOO_LARGE | StatusCode::UNSUPPORTED_MEDIA_TYPE
            if matches!(operation, RunOperation::Create | RunOperation::Input) =>
        {
            validated_problem_failure(response, None, RunFailure::InvalidInput, false)
        }
        StatusCode::TOO_MANY_REQUESTS => RunFailure::Unreachable(UnreachableCategory::RateLimited),
        status if status.is_server_error() => RunFailure::Unreachable(UnreachableCategory::Server),
        _ => RunFailure::protocol(false),
    }
}

// Only repository-owned terminal input-set problems qualify. Server detail and
// title are never used; an absent or unsafe diagnostic does not erase the status.
fn input_admission_rejection(
    response: &ReceivedResponse,
    problem: models::Problem,
) -> Option<RunFailure> {
    let problem_type = match (response.status, problem.r#type.as_str()) {
        (
            StatusCode::CONFLICT,
            "https://api.usefulmachinery.com/problems/run-input-bindings-invalid",
        ) => "run-input-bindings-invalid",
        (
            StatusCode::CONFLICT,
            "https://api.usefulmachinery.com/problems/run-input-schema-mismatch",
        ) => "run-input-schema-mismatch",
        (
            StatusCode::CONFLICT,
            "https://api.usefulmachinery.com/problems/run-input-set-not-sealed",
        ) => "run-input-set-not-sealed",
        (
            StatusCode::CONFLICT,
            "https://api.usefulmachinery.com/problems/run-input-set-consumed",
        ) => "run-input-set-consumed",
        (StatusCode::GONE, "https://api.usefulmachinery.com/problems/run-input-set-expired") => {
            "run-input-set-expired"
        }
        (StatusCode::GONE, "https://api.usefulmachinery.com/problems/run-input-content-gone") => {
            "run-input-content-gone"
        }
        _ => return None,
    };
    let diagnostic = problem.diagnostic.and_then(|diagnostic| {
        if diagnostic.input.is_empty()
            || diagnostic.input.len() > 128
            || !diagnostic
                .input
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            return None;
        }
        let code = match diagnostic.code {
            models::run_input_diagnostic::Code::MissingRequiredInput => "missing_required_input",
            models::run_input_diagnostic::Code::UnexpectedInput => "unexpected_input",
            models::run_input_diagnostic::Code::InputKindMismatch => "input_kind_mismatch",
            models::run_input_diagnostic::Code::InputSchemaMismatch => "input_schema_mismatch",
            models::run_input_diagnostic::Code::InputMediaTypeMismatch => {
                "input_media_type_mismatch"
            }
        };
        Some((code, diagnostic.input))
    });
    Some(RunFailure::InputAdmissionRejected {
        status: response.status.as_u16(),
        problem_type,
        diagnostic,
    })
}

// A definite source-admission rejection names only its reviewed type and the
// response's typed request id. Server detail and title are never used.
fn source_admission_rejection(
    response: &ReceivedResponse,
    problem: models::Problem,
) -> Option<RunFailure> {
    let problem_type = match (response.status, problem.r#type.as_str()) {
        (
            StatusCode::CONFLICT,
            "https://api.usefulmachinery.com/problems/workspace-selection-unsupported",
        ) => "workspace-selection-unsupported",
        (
            StatusCode::CONFLICT,
            "https://api.usefulmachinery.com/problems/workflow-source-unavailable",
        ) => "workflow-source-unavailable",
        (
            StatusCode::CONFLICT,
            "https://api.usefulmachinery.com/problems/workspace-source-unavailable",
        ) => "workspace-source-unavailable",
        (
            StatusCode::CONFLICT,
            "https://api.usefulmachinery.com/problems/workspace-membership-unproven",
        ) => "workspace-membership-unproven",
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            "https://api.usefulmachinery.com/problems/workflow-source-invalid",
        ) => "workflow-source-invalid",
        _ => return None,
    };
    let request_id = response
        .request_ids
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find(|value| um_support::valid_typed_id(value, "req_"))
        .map(str::to_owned);
    Some(RunFailure::AdmissionRejected {
        status: response.status.as_u16(),
        problem_type,
        request_id,
    })
}

fn validated_problem_failure(
    response: &ReceivedResponse,
    expected_type: Option<&str>,
    failure: RunFailure,
    credential_rejected: bool,
) -> RunFailure {
    match require_problem_type(response, expected_type, credential_rejected) {
        Ok(()) => failure,
        Err(error) => error,
    }
}

fn validate_acceptance(
    acceptance: RunCreationAcceptance,
    organization: &str,
    locations: &[HeaderValue],
) -> Result<RunCreationAcceptance, RunFailure> {
    if !um_support::valid_typed_id(&acceptance.run_id, "run_") {
        return Err(RunFailure::protocol(false));
    }
    let expected_location = format!(
        "/v1/organizations/{}/runs/{}",
        apis::urlencode(organization),
        apis::urlencode(&acceptance.run_id)
    );
    require_exact_header(locations.iter(), &expected_location)?;
    Ok(acceptance)
}

fn validate_cancellation_envelope(
    envelope: RunCancellationEnvelope,
    run_id: &str,
    mode: Option<RunCancellationMode>,
    status: Option<StatusCode>,
) -> Result<RunCancellationEnvelope, RunFailure> {
    use models::run_cancellation_receipt::State;
    use models::run_cancellation_resolution::Kind;
    let receipt = &envelope.request;
    let resolution = receipt.resolution.as_deref();
    let valid = um_support::valid_typed_id(&receipt.id, "cmd_")
        && um_support::valid_typed_id(&receipt.organization_id, "org_")
        && receipt.run_id == run_id
        && receipt
            .attempt_id
            .as_deref()
            .is_none_or(|id| um_support::valid_typed_id(id, "atm_"))
        && valid_timestamp(&receipt.accepted_at)
        && mode.is_none_or(|expected| {
            serde_json::to_value(expected).ok() == serde_json::to_value(receipt.mode).ok()
        })
        && (receipt.state == State::Pending) == resolution.is_none()
        && resolution.is_none_or(|resolution| {
            valid_timestamp(&resolution.resolved_at)
                && resolution
                    .effective_request_id
                    .as_deref()
                    .is_none_or(|id| um_support::valid_typed_id(id, "cmd_"))
                && resolution.run_version.is_none_or(|version| version > 0)
        })
        && status.is_none_or(|status| match status {
            StatusCode::OK => {
                resolution.is_some_and(|r| r.kind == Kind::AlreadyTerminal)
                    && envelope
                        .run
                        .as_deref()
                        .is_some_and(|run| terminal_run(run.state))
            }
            // An admitted retry can retain the preceding terminal attempt's Run
            // projection until the new attempt is projected.
            StatusCode::ACCEPTED => receipt.state == State::Pending,
            _ => false,
        })
        && envelope.run.as_deref().is_none_or(|run| {
            run.organization_id == receipt.organization_id
                && validate_run(run.clone(), run_id).is_ok()
        })
        && resolution.is_none_or(|resolution| {
            if resolution.kind == Kind::CreationRejected {
                return envelope.run.is_none();
            }
            envelope.run.as_deref().is_some_and(|run| {
                resolution
                    .run_version
                    .is_none_or(|version| run.version >= version)
            })
        });
    if valid {
        Ok(envelope)
    } else {
        Err(RunFailure::protocol(false))
    }
}

fn terminal_run(state: RunState) -> bool {
    matches!(
        state,
        RunState::Succeeded
            | RunState::Failed
            | RunState::Cancelled
            | RunState::Interrupted
            | RunState::Rejected
    )
}

fn validate_run(run: Run, requested_run_id: &str) -> Result<Run, RunFailure> {
    let workflow_source = &run.workflow_definition_source;
    let workspace_source = &run.primary_workspace_source;
    let inputs = &run.inputs;
    let valid = run.id == requested_run_id
        && um_support::valid_typed_id(&run.id, "run_")
        && um_support::valid_typed_id(&run.organization_id, "org_")
        && um_support::valid_typed_id(&run.project_id, "prj_")
        && um_support::valid_typed_id(&run.execution_spec_id, "xsp_")
        && um_support::valid_typed_id(&run.current_attempt_id, "atm_")
        && run.version >= 1
        && run.current_attempt_number >= 1
        && valid_bounded_string(&run.source_branch, 1, 1024)
        && run
            .display_name
            .as_deref()
            .is_none_or(|name| valid_bounded_string(name, 1, 200))
        && um_support::valid_typed_id(&workflow_source.repository_connection_id, "rpc_")
        && lowercase_hex(&workflow_source.commit_oid, 40)
        && valid_canonical_workflow_path(&workflow_source.workflow_path)
        && lowercase_hex(&workflow_source.workflow_source_closure_digest.value, 64)
        && um_support::valid_typed_id(&workspace_source.repository_connection_id, "rpc_")
        && lowercase_hex(&workspace_source.commit_oid, 40)
        && workflow_source.repository_connection_id == workspace_source.repository_connection_id
        && (match &run.workspace_membership {
            Some(membership) => {
                membership.repository_connection_id == workspace_source.repository_connection_id
                    && membership.workspace_commit_oid == workspace_source.commit_oid
                    && membership.change_request.as_deref().is_none_or(|change| {
                        change.head_repository_id == membership.provider_repository_id
                    })
                    && membership
                        .provider_repository_id
                        .parse::<u64>()
                        .is_ok_and(|id| id > 0)
                    && membership.witness.r#ref.starts_with("refs/heads/")
                    && membership.witness.r#ref.len() > "refs/heads/".len()
                    && lowercase_hex(&membership.witness.tip_oid, 40)
                    && match membership.witness.relationship {
                        models::workspace_membership_witness::Relationship::Equal => {
                            membership.witness.tip_oid == workspace_source.commit_oid
                        }
                        models::workspace_membership_witness::Relationship::Ancestor => {
                            membership.witness.tip_oid != workspace_source.commit_oid
                        }
                    }
                    && match run.workspace_source.as_deref() {
                        None => run
                            .workspace_branch
                            .as_deref()
                            .is_some_and(|branch| valid_bounded_string(branch, 1, 1024)),
                        Some(models::WorkspaceSourceSelection::WorkspaceSourceSelectionOneOf(
                            selection,
                        )) => {
                            run.workspace_branch.as_deref() == Some(selection.branch.as_str())
                                && valid_bounded_string(&selection.branch, 1, 1024)
                        }
                        Some(models::WorkspaceSourceSelection::WorkspaceSourceSelectionOneOf1(
                            selection,
                        )) => {
                            run.workspace_branch.is_none()
                                && selection.commit_oid == workspace_source.commit_oid
                        }
                    }
            }
            None => {
                workflow_source.commit_oid == workspace_source.commit_oid
                    && run.workspace_source.is_none()
                    && run.workspace_branch.is_none()
            }
        })
        && valid_source_display_snapshot(
            run.source_display_snapshot.as_deref(),
            workspace_source.provider_kind,
        )
        && inputs
            .input_set_id
            .as_deref()
            .is_none_or(|id| um_support::valid_typed_id(id, "ris_"))
        && (0..=256).contains(&inputs.attachment_count)
        && (0..=268_435_456).contains(&inputs.aggregate_bytes)
        && valid_integration_context(
            run.integration_context.len(),
            run.integration_context
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        && valid_cancellation(run.cancellation.as_deref())
        && (run.state != models::run::State::Cancelling || run.cancellation.is_some())
        && valid_interruption(run.state, run.interruption.as_deref())
        && run
            .failure
            .as_deref()
            .and_then(|failure| failure.recovery_reason)
            .is_none_or(|reason| {
                run.failure.as_deref().is_some_and(|failure| {
                    failure
                        .detail
                        .get("code")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|code| valid_recovery_reason_for_code(code, reason))
                })
            })
        && run
            .rejection
            .as_deref()
            .and_then(|rejection| rejection.recovery_reason)
            .is_none_or(|reason| {
                run.rejection.as_deref().is_some_and(|rejection| {
                    valid_recovery_reason_for_code(&rejection.reason, reason)
                })
            })
        && run
            .observation
            .as_deref()
            .and_then(|observation| observation.last_decline.as_deref())
            .is_none_or(|decline| {
                decline.recovery_reason.is_none_or(|reason| {
                    if decline.code != models::run_placement_decline::Code::RunnerUnable {
                        return false;
                    }
                    match decline.reason {
                        Some(models::run_placement_decline::Reason::OwnershipUnproven) => {
                            valid_recovery_reason_for_code("ownership_unproven", reason)
                        }
                        Some(
                            models::run_placement_decline::Reason::RetainedWorkspaceUnavailable,
                        ) => {
                            valid_recovery_reason_for_code("retained_workspace_unavailable", reason)
                        }
                        _ => false,
                    }
                })
            })
        && valid_artifact_delivery(run.artifact_delivery.as_deref())
        && valid_portable_result(&run)
        && valid_staged_continuation(&run)
        && valid_workspace_recovery(run.workspace_recovery.as_deref())
        && run
            .linear_lifecycle
            .as_deref()
            .is_none_or(validate_linear_lifecycle)
        && run.publication.as_deref().is_none_or(|handoff| {
            handoff.target.as_deref().is_none_or(|target| {
                target.number > 0
                    && target.number <= 9_007_199_254_740_991
                    && target
                        .provider_change_request_id
                        .parse::<i64>()
                        .is_ok_and(|id| {
                            id > 0 && id.to_string() == target.provider_change_request_id
                        })
                    && !target.source_ref.is_empty()
                    && target.source_ref.len() <= 1024
                    && lowercase_hex(&target.expected_head_oid, 40)
            })
        })
        && valid_run_actor(&run.actor)
        && valid_timestamp(&run.created_at)
        && valid_timestamp(&run.updated_at);
    if valid {
        Ok(run)
    } else {
        Err(RunFailure::protocol(false))
    }
}

// REQ-RUN-82: an app run names no requester; a member's run names the
// requester's principal.
fn valid_run_actor(actor: &models::RunActor) -> bool {
    match actor.kind {
        models::run_actor::Kind::App => actor.requester_principal_id.is_none(),
        models::run_actor::Kind::Member => actor
            .requester_principal_id
            .as_deref()
            .is_some_and(|id| um_support::valid_typed_id(id, "prn_")),
    }
}

fn valid_source_display_snapshot(
    snapshot: Option<&models::SourceDisplaySnapshot>,
    provider: models::primary_workspace_source::ProviderKind,
) -> bool {
    let Some(snapshot) = snapshot else {
        // GitHub execution-1 records predate immutable display snapshots.
        return provider == models::primary_workspace_source::ProviderKind::Github;
    };
    let repository = &snapshot.repository;
    let name = snapshot.project_name.as_bytes();
    let project_name_valid = (1..=63).contains(&name.len())
        && name.first().is_some_and(u8::is_ascii_alphanumeric)
        && name.last().is_some_and(u8::is_ascii_alphanumeric)
        && name
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
    if !valid_bounded_string(&snapshot.organization_display_name, 1, 200)
        || snapshot.organization_display_name.trim() != snapshot.organization_display_name
        || snapshot
            .organization_display_name
            .chars()
            .any(char::is_control)
        || !project_name_valid
    {
        return false;
    }
    match (provider, repository.provider_kind) {
        (
            models::primary_workspace_source::ProviderKind::Github,
            models::source_display_repository::ProviderKind::Github,
        ) => {
            repository.instance_url.is_none()
                && (3..=255).contains(&repository.full_name.len())
                && repository
                    .full_name
                    .split_once('/')
                    .is_some_and(|(owner, name)| {
                        !owner.is_empty()
                            && !name.is_empty()
                            && [owner, name].iter().all(|segment| {
                                segment.bytes().all(|byte| {
                                    byte.is_ascii_alphanumeric()
                                        || matches!(byte, b'_' | b'.' | b'-')
                                })
                            })
                    })
        }
        (
            models::primary_workspace_source::ProviderKind::Gitlab,
            models::source_display_repository::ProviderKind::Gitlab,
        ) => {
            repository.instance_url
                == Some(Some(
                    models::source_display_repository::InstanceUrl::HttpsColonSlashSlashGitlabCom,
                ))
                && super::projects::valid_gitlab_path(&repository.full_name)
        }
        _ => false,
    }
}

fn valid_cancellation(cancellation: Option<&models::RunCancellation>) -> bool {
    let Some(cancellation) = cancellation else {
        return true;
    };
    match cancellation.mode {
        models::run_cancellation::Mode::Graceful => {
            cancellation
                .graceful_request_id
                .as_deref()
                .is_some_and(|id| um_support::valid_typed_id(id, "cmd_"))
                && cancellation.force_request_id.is_none()
        }
        models::run_cancellation::Mode::Force => {
            cancellation
                .force_request_id
                .as_deref()
                .is_some_and(|id| um_support::valid_typed_id(id, "cmd_"))
                && cancellation
                    .graceful_request_id
                    .as_deref()
                    .is_none_or(|id| um_support::valid_typed_id(id, "cmd_"))
        }
    }
}

fn valid_interruption(
    state: models::run::State,
    interruption: Option<&models::RunInterruption>,
) -> bool {
    if state != models::run::State::Interrupted {
        return interruption.is_none();
    }
    let Some(interruption) = interruption else {
        return false;
    };
    if interruption.recovery_reason.is_some_and(|reason| {
        let code = match interruption.cause {
            models::run_interruption::Cause::RetainedWorkspaceUnavailable => {
                "retained_workspace_unavailable"
            }
            models::run_interruption::Cause::OwnershipUnproven => "ownership_unproven",
            _ => return true,
        };
        !valid_recovery_reason_for_code(code, reason)
    }) {
        return false;
    }
    match interruption.cause {
        models::run_interruption::Cause::ExecutorFault => {
            interruption.stop_confirmed && interruption.executor_fault.is_some()
        }
        models::run_interruption::Cause::ExecutorShutdown => {
            interruption.stop_confirmed && interruption.executor_fault.is_none()
        }
        models::run_interruption::Cause::ExecutionLeaseExpired => {
            interruption.executor_fault.is_none()
        }
        models::run_interruption::Cause::RetainedWorkspaceUnavailable
        | models::run_interruption::Cause::OwnershipUnproven => {
            !interruption.stop_confirmed && interruption.executor_fault.is_none()
        }
    }
}

fn valid_portable_result(run: &Run) -> bool {
    let delivered = matches!(
        run.artifact_delivery.as_deref(),
        Some(models::RunArtifactDelivery::RunArtifactDeliverySucceeded(_))
    );
    matches!(run.portable_result, models::run::PortableResult::Available) == delivered
}

fn valid_staged_continuation(run: &Run) -> bool {
    let Some(continuation) = &run.continuation else {
        return true;
    };
    let workspace = &continuation.workspace;
    if let Some(recovery) = workspace.recovery.as_deref() {
        if continuation.schema_version != Some(models::run_continuation::SchemaVersion::Variant2)
            || !um_support::valid_typed_id(&recovery.workspace_id, "rws_")
            || !um_support::valid_typed_id(&recovery.work_root_id, "rwr_")
            || !um_support::valid_typed_id(&recovery.runner_id, "rnr_")
            || !um_support::valid_typed_id(&recovery.origin_runner_boot_id, "rbt_")
        {
            return false;
        }
        let grant = recovery.target_runner_boot_id.is_some();
        if recovery.claiming_assignment_id.is_some() != grant
            || recovery.claim_generation.is_some() != grant
            || recovery
                .target_runner_boot_id
                .as_deref()
                .is_some_and(|id| !um_support::valid_typed_id(id, "rbt_"))
            || recovery
                .claiming_assignment_id
                .as_deref()
                .is_some_and(|id| !um_support::valid_typed_id(id, "asn_"))
            || recovery
                .claim_generation
                .is_some_and(|generation| !(1..=9_007_199_254_740_991).contains(&generation))
            || recovery
                .failure
                .as_deref()
                .is_some_and(|failure| !valid_recovery_pair(failure.code, failure.recovery_reason))
        {
            return false;
        }
        if matches!(
            workspace.preparation,
            models::run_continuation_workspace::Preparation::Ready
        ) && !grant
        {
            return false;
        }
    } else if continuation.schema_version.is_some() {
        return false;
    }
    if !workspace
        .prior_settlement_snapshot
        .as_deref()
        .is_none_or(|snapshot| valid_continuation_snapshot(snapshot, true))
    {
        return false;
    }
    let modified_unknown = matches!(
        workspace.modified.as_ref(),
        models::RunContinuationWorkspaceModified::String(value) if value == "unknown"
    );
    match workspace.preparation {
        models::run_continuation_workspace::Preparation::Pending => {
            !terminal_run(run.state)
                && workspace.start_snapshot.is_none()
                && workspace.quiescence.is_none()
                && modified_unknown
        }
        models::run_continuation_workspace::Preparation::Unavailable => {
            matches!(
                run.state,
                RunState::Failed | RunState::Cancelled | RunState::Interrupted
            ) && run.artifact_delivery.is_none()
                && workspace.start_snapshot.is_none()
                && workspace.quiescence.is_none()
                && modified_unknown
        }
        models::run_continuation_workspace::Preparation::Ready => {
            let Some(start) = workspace.start_snapshot.as_deref() else {
                return false;
            };
            if !valid_continuation_snapshot(start, false)
                || !workspace.quiescence.as_ref().is_some_and(|proof| {
                    proof.groups_recorded >= 0
                        && proof.groups_terminated >= 0
                        && proof.groups_absent >= 0
                        && proof.groups_terminated.checked_add(proof.groups_absent)
                            == Some(proof.groups_recorded)
                        && valid_timestamp(&proof.proven_at)
                })
            {
                return false;
            }
            let prior = workspace.prior_settlement_snapshot.as_deref();
            let comparable = workspace.execution_root == workspace.prior_execution_root
                && start.value.is_some()
                && prior.is_some_and(|prior| {
                    prior.value.is_some()
                        && prior.algorithm == start.algorithm
                        && prior.settled_by
                            == Some(models::run_continuation_snapshot::SettledBy::Engine)
                });
            if comparable {
                let changed = prior
                    .and_then(|prior| prior.value.as_ref())
                    .zip(start.value.as_ref())
                    .is_some_and(|(prior, start)| prior != start);
                matches!(
                    workspace.modified.as_ref(),
                    models::RunContinuationWorkspaceModified::Boolean(value) if *value == changed
                )
            } else {
                modified_unknown
            }
        }
    }
}

// OpenAPI Generator represents required-null fields as Option and otherwise
// loses the difference between explicit null and missing keys. Keep the
// public read boundary closed before deserializing into generated models.
fn valid_recovery_json_shape(bytes: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return false;
    };
    let run = value.get("run").unwrap_or(&value);
    let Some(run) = run.as_object() else {
        return true; // Nullable envelope run; the typed envelope checks it.
    };
    if let Some(target) = run
        .get("publication")
        .and_then(|handoff| handoff.get("target"))
    {
        let Some(object) = target.as_object() else {
            return false;
        };
        let fields = [
            "kind",
            "number",
            "providerChangeRequestId",
            "sourceRef",
            "expectedHeadOid",
        ];
        if object.len() != fields.len() || !fields.iter().all(|field| object.contains_key(*field)) {
            return false;
        }
    }
    if let Some(status) = run.get("workspaceRecovery") {
        let Some(status) = status.as_object() else {
            return false;
        };
        if status.len() != 3
            || !["schemaVersion", "status", "reason"]
                .iter()
                .all(|key| status.contains_key(*key))
        {
            return false;
        }
    }
    let Some(continuation) = run
        .get("continuation")
        .and_then(serde_json::Value::as_object)
    else {
        return true;
    };
    let schema = continuation.get("schemaVersion");
    let recovery = continuation
        .get("workspace")
        .and_then(|workspace| workspace.get("recovery"));
    match (schema, recovery) {
        (None, None) => true,
        (Some(version), Some(recovery)) if version == 2 => {
            let Some(recovery) = recovery.as_object() else {
                return false;
            };
            let required = [
                "schemaVersion",
                "workspaceId",
                "workRootId",
                "runnerId",
                "originRunnerBootId",
                "targetRunnerBootId",
                "claimingAssignmentId",
                "claimGeneration",
                "failure",
            ];
            if recovery.len() != required.len()
                || !required.iter().all(|key| recovery.contains_key(*key))
            {
                return false;
            }
            match recovery.get("failure") {
                Some(serde_json::Value::Null) => true,
                Some(serde_json::Value::Object(failure)) => {
                    failure.len() == 2
                        && failure.contains_key("code")
                        && failure.contains_key("recoveryReason")
                }
                _ => false,
            }
        }
        _ => false,
    }
}

fn valid_workspace_recovery(status: Option<&models::WorkspaceRecoveryStatus>) -> bool {
    let Some(status) = status else {
        return true;
    };
    match status.status {
        models::workspace_recovery_status::Status::Pending
        | models::workspace_recovery_status::Status::Established => status.reason.is_none(),
        models::workspace_recovery_status::Status::NotEstablished => status.reason.is_some(),
    }
}

fn valid_recovery_pair(
    code: models::continuation_recovery_failure::Code,
    reason: models::RecoveryReason,
) -> bool {
    let code = match code {
        models::continuation_recovery_failure::Code::RetainedWorkspaceUnavailable => {
            "retained_workspace_unavailable"
        }
        models::continuation_recovery_failure::Code::OwnershipUnproven => "ownership_unproven",
    };
    valid_recovery_reason_for_code(code, reason)
}

// The "Binding ownership states and reason codes" table: each code names the one
// ownership state it may accompany; awaiting_event and current carry none.
const LINEAR_LIFECYCLE_REASON_STATES: [(LifecycleReason, LifecycleState); 18] = [
    (LifecycleReason::TeamUnmapped, LifecycleState::Skipped),
    (LifecycleReason::SupersededRun, LifecycleState::Superseded),
    (
        LifecycleReason::IssueStateChanged,
        LifecycleState::Relinquished,
    ),
    (LifecycleReason::TeamChanged, LifecycleState::Stopped),
    (LifecycleReason::LifecycleDisabled, LifecycleState::Stopped),
    (LifecycleReason::TriggerDisabled, LifecycleState::Stopped),
    (LifecycleReason::TriggerDeleted, LifecycleState::Stopped),
    (LifecycleReason::GrantReplaced, LifecycleState::Stopped),
    (
        LifecycleReason::ConnectionDisconnected,
        LifecycleState::Stopped,
    ),
    (LifecycleReason::ConnectionDeleted, LifecycleState::Stopped),
    (LifecycleReason::ConnectionRevoked, LifecycleState::Stopped),
    (
        LifecycleReason::ConnectionRecoveryRequired,
        LifecycleState::Stopped,
    ),
    (
        LifecycleReason::WritePermissionLost,
        LifecycleState::Stopped,
    ),
    (LifecycleReason::TeamAccessLost, LifecycleState::Stopped),
    (
        LifecycleReason::PrincipalUnavailable,
        LifecycleState::Stopped,
    ),
    (LifecycleReason::MembershipLost, LifecycleState::Stopped),
    (LifecycleReason::PermissionLost, LifecycleState::Stopped),
    (
        LifecycleReason::OrganizationUnavailable,
        LifecycleState::Stopped,
    ),
];

fn valid_linear_lifecycle_reason(state: LifecycleState, code: Option<LifecycleReason>) -> bool {
    match code {
        None => matches!(
            state,
            LifecycleState::AwaitingEvent | LifecycleState::Current
        ),
        Some(code) => LINEAR_LIFECYCLE_REASON_STATES.contains(&(code, state)),
    }
}

// The "Update states and reason codes" table, by wire name: each code names the
// update states it may accompany.
const LINEAR_LIFECYCLE_UPDATE_REASON_STATES: [(&str, &[&str]); 27] = [
    ("superseded_run", &["skipped"]),
    ("superseded_attempt", &["skipped"]),
    ("superseded_event", &["skipped"]),
    ("cancellation_preserves_state", &["preserved"]),
    ("issue_state_changed", &["stopped"]),
    ("team_changed", &["stopped"]),
    ("lifecycle_disabled", &["stopped"]),
    ("trigger_disabled", &["stopped"]),
    ("trigger_deleted", &["stopped"]),
    ("grant_replaced", &["stopped"]),
    ("connection_disconnected", &["stopped"]),
    ("connection_deleted", &["stopped"]),
    ("connection_revoked", &["stopped"]),
    ("connection_recovery_required", &["stopped"]),
    ("write_permission_lost", &["stopped"]),
    ("team_access_lost", &["stopped"]),
    ("principal_unavailable", &["stopped"]),
    ("membership_lost", &["stopped"]),
    ("permission_lost", &["stopped"]),
    ("organization_unavailable", &["stopped"]),
    ("provider_unavailable", &["retry_waiting"]),
    ("rate_limited", &["retry_waiting"]),
    ("issue_unavailable", &["failed"]),
    ("state_unavailable", &["failed"]),
    ("outcome_unresolved", &["pending", "retry_waiting"]),
    ("internal_sync_failure", &["retry_waiting", "failed"]),
    ("retry_exhausted", &["failed"]),
];

fn valid_linear_lifecycle_update_reason(state: &str, code: Option<&str>) -> bool {
    match code {
        None => matches!(state, "pending" | "applied" | "already_satisfied"),
        Some(code) => LINEAR_LIFECYCLE_UPDATE_REASON_STATES
            .iter()
            .any(|(known, states)| *known == code && states.contains(&state)),
    }
}

// Mirrors cloudrun.LinearLifecycleUpdate.Validate: the schema closes the enums,
// but not the reason and state pair, the preserved-only absent desired state,
// the step-exact node, the blocking update's reason, or a diagnostic with no
// facts.
fn validate_linear_lifecycle_update(update: &RunLinearLifecycleUpdate) -> bool {
    let (Some(state), Some(event_type)) = (wire_name(update.state), wire_name(update.event_type))
    else {
        return false;
    };
    let code = update.reason_code.and_then(wire_name);
    if update.reason_code.is_some() && code.is_none() {
        return false;
    }
    let nonempty = |value: &Option<String>| value.as_deref().is_none_or(|value| !value.is_empty());
    um_support::valid_typed_id(&update.id, "llu_")
        && um_support::valid_typed_id(&update.attempt_id, "atm_")
        && update.attempt_number >= 1
        && !update.event_id.is_empty()
        && update.event_sequence >= 1
        && valid_timestamp(&update.event_occurred_at)
        && valid_timestamp(&update.created_at)
        && valid_timestamp(&update.updated_at)
        && update.cycle_number >= 1
        && valid_timestamp(&update.cycle_deadline)
        && update.request_count >= 0
        && update
            .next_attempt_at
            .as_deref()
            .is_none_or(valid_timestamp)
        && valid_linear_lifecycle_update_reason(&state, code.as_deref())
        && match update.node.as_deref() {
            Some(node) => event_type.starts_with("step.") && !node.id.is_empty(),
            None => !event_type.starts_with("step."),
        }
        && update.desired_state_id.is_none() == (state == "preserved")
        && nonempty(&update.desired_state_id)
        && nonempty(&update.observed_state_id)
        && nonempty(&update.confirmed_state_id)
        && update.blocking_update.as_deref().is_none_or(|blocking| {
            code.as_deref() == Some("outcome_unresolved")
                && um_support::valid_typed_id(&blocking.run_id, "run_")
                && um_support::valid_typed_id(&blocking.update_id, "llu_")
        })
        && update.diagnostic.as_deref().is_none_or(|facts| {
            let nonempty_text =
                |value: &Option<String>| value.as_deref().is_none_or(|text| !text.is_empty());
            (facts.cause.is_some()
                || facts.provider_status.is_some()
                || facts.transport_error.is_some()
                || facts.retry_after.is_some())
                && nonempty_text(&facts.cause)
                && nonempty_text(&facts.transport_error)
                && facts.retry_after.as_deref().is_none_or(valid_timestamp)
        })
}

// Accepts one page of a run's update history: at most 100 updates, each valid
// as the view's latest update, and a nonempty bounded cursor when present.
fn decode_linear_lifecycle_update_list(
    response: ReceivedResponse,
) -> Result<RunLinearLifecycleUpdateList, RunFailure> {
    if response.status != StatusCode::OK {
        return Err(classify_failure(&response, RunOperation::Get));
    }
    require_media_type(&response, JSON_MEDIA_TYPE, false)?;
    require_exact_header(response.cache_controls.iter(), PRIVATE_CACHE_CONTROL)?;
    let list: RunLinearLifecycleUpdateList =
        serde_json::from_slice(&response.body).map_err(|_| RunFailure::protocol(false))?;
    let valid_cursor = list
        .next_cursor
        .as_deref()
        .is_none_or(|cursor| !cursor.is_empty() && cursor.len() <= 2048);
    if list.items.len() <= 100
        && valid_cursor
        && list.items.iter().all(validate_linear_lifecycle_update)
    {
        Ok(list)
    } else {
        Err(RunFailure::protocol(false))
    }
}

// Mirrors cloudrun.LinearLifecycleRetryReceipt.Validate and binds the receipt to
// the route it answers.
fn validate_linear_lifecycle_retry_receipt(
    receipt: &RunLinearLifecycleRetryReceipt,
    run_id: &str,
    update_id: &str,
) -> Result<(), RunFailure> {
    if receipt.run_id == run_id
        && receipt.update_id == update_id
        && receipt.cycle_number >= 2
        && valid_timestamp(&receipt.accepted_at)
        && valid_timestamp(&receipt.cycle_deadline)
    {
        Ok(())
    } else {
        Err(RunFailure::protocol(false))
    }
}

// Mirrors cloudrun.LinearLifecycle.Validate: the schema closes the enums, but not
// the reason and state pair, the attempt pair, the step-only event node, or the
// latest update's own rules.
fn validate_linear_lifecycle(view: &RunLinearLifecycle) -> bool {
    let step_event = |event_type: LifecycleEvent| {
        matches!(
            event_type,
            LifecycleEvent::StepStarted
                | LifecycleEvent::StepSucceeded
                | LifecycleEvent::StepFailed
                | LifecycleEvent::StepSkipped
                | LifecycleEvent::StepCancelled
                | LifecycleEvent::StepBlocked
                | LifecycleEvent::StepNotRun
        )
    };
    um_support::valid_typed_id(&view.trigger_id, "trg_")
        && view.configuration_version >= 1
        && um_support::valid_typed_id(&view.connection_id, "lcn_")
        && !view.workspace_id.is_empty()
        && !view.issue_id.is_empty()
        && valid_linear_lifecycle_reason(view.ownership_state, view.reason_code)
        && match (view.attempt_id.as_deref(), view.attempt_number) {
            (None, None) => true,
            (Some(attempt_id), Some(number)) => {
                um_support::valid_typed_id(attempt_id, "atm_") && number >= 1
            }
            _ => false,
        }
        && view.controlling_run_id.as_deref().is_none_or(|run_id| {
            view.ownership_state == LifecycleState::Superseded
                && um_support::valid_typed_id(run_id, "run_")
        })
        && view
            .latest_update
            .as_deref()
            .is_none_or(validate_linear_lifecycle_update)
        && view.ownership_event.as_deref().is_none_or(|event| {
            !event.event_id.is_empty()
                && event.sequence >= 1
                && valid_timestamp(&event.occurred_at)
                && um_support::valid_typed_id(&event.attempt_id, "atm_")
                && event.attempt_number >= 1
                && event
                    .node
                    .as_deref()
                    .is_none_or(|node| step_event(event.event_type) && !node.id.is_empty())
        })
}

fn valid_recovery_reason_for_code(code: &str, reason: models::RecoveryReason) -> bool {
    use models::RecoveryReason as Reason;
    let unavailable = matches!(
        reason,
        Reason::WorkspaceNotFound
            | Reason::RecoveryRecordUnsupported
            | Reason::RecoveryNotEstablished
    );
    match code {
        "retained_workspace_unavailable" => unavailable,
        "ownership_unproven" => !unavailable,
        _ => false,
    }
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "recovery reader tests use assertions and fixture extraction"
)]
mod recovery_read_tests {
    use super::*;

    #[test]
    #[ignore = "requires the admitted Run API response supplied by the cross-component harness"]
    fn admitted_gitlab_run_response_decodes_in_cli() {
        let file = std::env::var("UM_GITLAB_ADMITTED_API_FILE").unwrap();
        let document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
        let run_id = document["id"].as_str().unwrap();
        let expected = document["sourceDisplaySnapshot"].clone();
        let encoded = serde_json::to_vec(&document).unwrap();
        assert!(valid_recovery_json_shape(&encoded));
        let typed: Run = serde_json::from_slice(&encoded).unwrap();
        assert!(validate_run(typed, run_id).is_ok());
        let response = decode_get_response(
            BufferedBlockingResponse {
                status: StatusCode::OK,
                content_type: Some(HeaderValue::from_static("application/json")),
                idempotency_keys: vec![],
                locations: vec![],
                cache_controls: vec![HeaderValue::from_static(PRIVATE_CACHE_CONTROL)],
                retry_afters: vec![],
                request_ids: vec![],
                body: serde_json::to_vec(&document).unwrap(),
            },
            run_id,
        )
        .unwrap();
        assert!(matches!(response, RunRead::Materialized(_)));
        if let RunRead::Materialized(run) = response {
            assert_eq!(
                serde_json::to_value(run.as_ref()).unwrap()["sourceDisplaySnapshot"],
                expected,
            );
        }
    }

    // REQ-WORKSPACE-11, REQ-WORKSPACE-31
    #[test]
    fn run_reader_preserves_closed_gitlab_snapshot_and_github_history() {
        let mut run: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/run-continuation-pending.json"
        ))
        .unwrap();
        run = run["run"].take();
        let run_id = run["id"].as_str().unwrap().to_owned();
        let read = |value: &serde_json::Value| {
            decode_get_response(
                BufferedBlockingResponse {
                    status: StatusCode::OK,
                    content_type: Some(HeaderValue::from_static("application/json")),
                    idempotency_keys: vec![],
                    locations: vec![],
                    cache_controls: vec![HeaderValue::from_static(PRIVATE_CACHE_CONTROL)],
                    retry_afters: vec![],
                    request_ids: vec![],
                    body: serde_json::to_vec(value).unwrap(),
                },
                &run_id,
            )
        };
        assert!(matches!(read(&run), Ok(RunRead::Materialized(_))));
        run["sourceDisplaySnapshot"]["repository"]["instanceUrl"] = serde_json::Value::Null;
        assert!(read(&run).is_err()); // GitHub forbids even an explicit null instance.
        run["sourceDisplaySnapshot"] = serde_json::Value::Null;
        assert!(read(&run).is_ok()); // Execution-1 GitHub had no snapshot.
        run["primaryWorkspaceSource"]["providerKind"] = serde_json::json!("gitlab");
        assert!(read(&run).is_err());
        for length in [256, 8192] {
            let name = format!("group/{}/project", "p".repeat(length - 14));
            run["sourceDisplaySnapshot"] = serde_json::json!({
                "organizationDisplayName": "Example Organization", "projectName": "example-project",
                "repository": {"providerKind": "gitlab", "instanceUrl": "https://gitlab.com",
                               "fullName": name}
            });
            let snapshot = read(&run).ok().and_then(|value| match value {
                RunRead::Materialized(valid) => serde_json::to_value(valid.as_ref())
                    .ok()
                    .map(|value| value["sourceDisplaySnapshot"].clone()),
                RunRead::Pending(_) => None,
            });
            assert_eq!(snapshot, Some(run["sourceDisplaySnapshot"].clone()));
        }
        // Malformed and mismatched variants below use the maximum valid name.
        let original = run.clone();
        let invalid = |change: &dyn Fn(&mut serde_json::Value)| {
            let mut candidate = original.clone();
            change(&mut candidate);
            assert!(read(&candidate).is_err());
        };
        invalid(&|value| value["sourceDisplaySnapshot"] = serde_json::Value::Null);
        invalid(&|value| {
            value
                .as_object_mut()
                .unwrap()
                .remove("sourceDisplaySnapshot");
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]["providerKind"] =
                serde_json::json!("github")
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]
                .as_object_mut()
                .unwrap()
                .remove("providerKind");
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]
                .as_object_mut()
                .unwrap()
                .remove("fullName");
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]["instanceUrl"] = serde_json::Value::Null
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]["instanceUrl"] =
                serde_json::json!("https://other.example")
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]["extra"] = serde_json::json!(true)
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]["fullName"] =
                serde_json::json!(format!("group/{}", "p".repeat(8187)))
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]["fullName"] =
                serde_json::json!("group/../project")
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]["fullName"] =
                serde_json::json!("group/%2F/project")
        });
        invalid(&|value| {
            value["sourceDisplaySnapshot"]["repository"]["providerKind"] =
                serde_json::json!("unknown")
        });
    }

    #[test]
    fn classified_interruption_requires_a_matching_closed_reason() {
        let mut value = serde_json::json!({
            "phase":"accepted", "cause":"ownership_unproven", "executorFault":null,
            "stopConfirmed":false, "recoveryReason":"host_identity_mismatch"
        });
        let valid: models::RunInterruption = serde_json::from_value(value.clone()).unwrap();
        assert!(valid_interruption(
            models::run::State::Interrupted,
            Some(&valid)
        ));
        value["recoveryReason"] = serde_json::json!("workspace_not_found");
        let mismatched: models::RunInterruption = serde_json::from_value(value.clone()).unwrap();
        assert!(!valid_interruption(
            models::run::State::Interrupted,
            Some(&mismatched)
        ));
        value["recoveryReason"] = serde_json::json!("unclassified");
        assert!(serde_json::from_value::<models::RunInterruption>(value).is_err());
    }

    #[test]
    fn classified_ownership_admission_problem_decodes_without_losing_reason() {
        let response = |code: &str, reason: &str| BufferedBlockingResponse {
            status: StatusCode::CONFLICT,
            content_type: Some(HeaderValue::from_static("application/problem+json")),
            idempotency_keys: vec![],
            locations: vec![],
            cache_controls: vec![],
            retry_afters: vec![],
            request_ids: vec![],
            body: serde_json::to_vec(&serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/continuation-admission-invalid",
                "title": "Admission invalid", "status": 409,
                "continuationViolations": [{"code": code, "recoveryReason": reason}]
            }))
            .unwrap(),
        };
        assert!(matches!(
            classify_continuation_failure(&response("ownership_unproven", "host_identity_mismatch")),
            RunFailure::ContinuationAdmissionInvalid(violations)
                if violations.len() == 1
                    && violations[0].code
                        == models::continuation_admission_violation::Code::OwnershipUnproven
                    && violations[0].recovery_reason
                        == Some(models::RecoveryReason::HostIdentityMismatch)
        ));
        assert!(matches!(
            classify_continuation_failure(&response("ownership_unproven", "workspace_not_found")),
            RunFailure::Protocol { .. }
        ));
    }

    // REQ-RUNINPUT-59, REQ-CONTINUE-72
    #[test]
    fn gone_input_content_is_a_create_admission_rejection_and_a_gone_continuation() {
        let response = BufferedBlockingResponse {
            status: StatusCode::GONE,
            content_type: Some(HeaderValue::from_static("application/problem+json")),
            idempotency_keys: vec![],
            locations: vec![],
            cache_controls: vec![],
            retry_afters: vec![],
            request_ids: vec![],
            body: serde_json::to_vec(&serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/run-input-content-gone",
                "title": "Input content gone", "status": 410
            }))
            .unwrap(),
        };
        assert!(matches!(
            classify_continuation_failure(&response),
            RunFailure::Gone
        ));
        assert!(matches!(
            classify_failure(&response, RunOperation::Create),
            RunFailure::InputAdmissionRejected {
                status: 410,
                problem_type: "run-input-content-gone",
                diagnostic: None,
            }
        ));
    }

    #[test]
    fn placement_decline_preserves_legacy_omission_and_validates_recovery_pair() {
        let mut envelope: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/run-continuation-pending.json"
        ))
        .unwrap();
        let id = envelope["run"]["id"].as_str().unwrap().to_owned();
        envelope["run"]["observation"] = serde_json::json!({
            "observedAt": "2026-08-23T12:02:00Z", "placement": null,
            "assignment": null, "lastTransition": null,
            "lastDecline": {
                "code": "runner_unable", "reason": "ownership_unproven",
                "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
                "declinedAt": "2026-08-23T12:02:00Z"
            }
        });
        let read = |document: &serde_json::Value| {
            let run: models::Run = serde_json::from_value(document["run"].clone()).unwrap();
            validate_run(run, &id).is_ok()
        };
        assert!(read(&envelope));
        envelope["run"]["observation"]["lastDecline"]["recoveryReason"] =
            serde_json::json!("host_identity_mismatch");
        assert!(read(&envelope));
        envelope["run"]["observation"]["lastDecline"]["recoveryReason"] =
            serde_json::json!("workspace_not_found");
        assert!(!read(&envelope));
        envelope["run"]["observation"]["lastDecline"]["recoveryReason"] =
            serde_json::json!("unclassified");
        assert!(serde_json::from_value::<models::Run>(envelope["run"].clone()).is_err());
    }

    #[test]
    fn historical_run_read_accepts_null_workspace_provenance() {
        let document: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/run-continuation-pending.json"
        ))
        .unwrap();
        let run = &document["run"];
        let run_id = run["id"].as_str().unwrap();
        assert!(run["workspaceBranch"].is_null());
        assert!(run["workspaceMembership"].is_null());
        let response = |run: &serde_json::Value| BufferedBlockingResponse {
            status: StatusCode::OK,
            content_type: Some(HeaderValue::from_static("application/json")),
            idempotency_keys: vec![],
            locations: vec![],
            cache_controls: vec![HeaderValue::from_static(PRIVATE_CACHE_CONTROL)],
            retry_afters: vec![],
            request_ids: vec![],
            body: serde_json::to_vec(run).unwrap(),
        };
        assert!(matches!(
            decode_get_response(response(run), run_id),
            Ok(RunRead::Materialized(_))
        ));
        let mut incompatible = run.clone();
        incompatible["workspaceBranch"] = serde_json::json!("main");
        assert!(matches!(
            decode_get_response(response(&incompatible), run_id),
            Err(RunFailure::Protocol { .. })
        ));
    }

    #[test]
    fn run_reader_binds_independent_workspace_selection_to_retained_membership() {
        let mut document: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/run-continuation-pending.json"
        ))
        .unwrap();
        let run = &mut document["run"];
        let run_id = run["id"].as_str().unwrap().to_owned();
        let workspace_oid = run["primaryWorkspaceSource"]["commitOid"].clone();
        run["workspaceBranch"] = serde_json::json!("main");
        run["workspaceMembership"] = serde_json::json!({
            "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
            "providerKind": "github", "providerRepositoryId": "1234",
            "objectFormat": "sha1", "workspaceCommitOid": workspace_oid,
            "witness": {"ref":"refs/heads/main", "tipOid": workspace_oid,
                "relationship":"equal", "observedAt":"2026-08-23T12:02:00Z"}
        });
        let valid = |value: &serde_json::Value| {
            let run: models::Run = serde_json::from_value(value.clone()).unwrap();
            validate_run(run, &run_id).is_ok()
        };
        assert!(valid(run)); // Schema 3's default workspace branch has no explicit selector.
        run["workspaceSource"] = serde_json::json!({"kind":"branch", "branch":"other"});
        assert!(!valid(run));
        run["workspaceSource"] = serde_json::json!({"kind":"branch", "branch":"main"});
        assert!(valid(run));
        run["workspaceSource"] = serde_json::json!({"kind":"commit", "commitOid": workspace_oid});
        run["workspaceBranch"] = serde_json::Value::Null;
        assert!(valid(run));
        run["workspaceMembership"]["witness"]["tipOid"] =
            serde_json::json!("abcdef0123456789abcdef0123456789abcdef01");
        assert!(!valid(run));
        run["workspaceMembership"] = serde_json::Value::Null;
        assert!(!valid(run));
    }

    #[test]
    fn continuation_reads_absence_pending_and_rejected_recovery_pairs() {
        let mut document: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/run-continuation-pending.json"
        ))
        .unwrap();
        assert!(valid_recovery_json_shape(
            &serde_json::to_vec(&document).unwrap()
        ));
        let legacy: models::RunContinuationEnvelope =
            serde_json::from_value(document.clone()).unwrap();
        assert!(legacy.run.workspace_recovery.is_none());
        assert!(
            legacy
                .run
                .continuation
                .as_ref()
                .unwrap()
                .workspace
                .recovery
                .is_none()
        );
        assert!(valid_staged_continuation(&legacy.run));

        document["run"]["workspaceRecovery"] = serde_json::json!({
            "schemaVersion": 1, "status": "established", "reason": null
        });
        document["run"]["continuation"]["schemaVersion"] = serde_json::json!(2);
        document["run"]["continuation"]["workspace"]["recovery"] = serde_json::json!({
            "schemaVersion": 1, "workspaceId": "rws_01k0z6r1w8f4jy2m7q9v3x5abc",
            "workRootId": "rwr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "originRunnerBootId": "rbt_01k0z6r1w8f4jy2m7q9v3x5abc",
            "targetRunnerBootId": null, "claimingAssignmentId": null,
            "claimGeneration": null, "failure": null
        });
        assert!(valid_recovery_json_shape(
            &serde_json::to_vec(&document).unwrap()
        ));
        let parsed: models::RunContinuationEnvelope =
            serde_json::from_value(document.clone()).unwrap();
        assert!(valid_workspace_recovery(
            parsed.run.workspace_recovery.as_deref()
        ));
        assert!(valid_staged_continuation(&parsed.run));

        let mut missing_reason = document.clone();
        missing_reason["run"]["workspaceRecovery"]
            .as_object_mut()
            .unwrap()
            .remove("reason");
        assert!(!valid_recovery_json_shape(
            &serde_json::to_vec(&missing_reason).unwrap()
        ));
        let mut unknown_member = document.clone();
        unknown_member["run"]["continuation"]["workspace"]["recovery"]["unexpected"] =
            serde_json::json!(true);
        assert!(!valid_recovery_json_shape(
            &serde_json::to_vec(&unknown_member).unwrap()
        ));

        document["run"]["continuation"]["workspace"]["recovery"]["failure"] = serde_json::json!({
            "code": "ownership_unproven", "recoveryReason": "host_identity_mismatch"
        });
        let failure: models::RunContinuationEnvelope =
            serde_json::from_value(document.clone()).unwrap();
        assert!(valid_staged_continuation(&failure.run));
        document["run"]["continuation"]["workspace"]["recovery"]["failure"]["code"] =
            serde_json::json!("retained_workspace_unavailable");
        let invalid: models::RunContinuationEnvelope =
            serde_json::from_value(document.clone()).unwrap();
        assert!(!valid_staged_continuation(&invalid.run));
        document["run"]["continuation"]["workspace"]["recovery"]
            .as_object_mut()
            .unwrap()
            .remove("targetRunnerBootId");
        assert!(serde_json::from_value::<models::RunContinuationEnvelope>(document).is_err());
    }

    fn create_problem_response(
        status: StatusCode,
        slug: &str,
        request_ids: &[&'static str],
    ) -> BufferedBlockingResponse {
        BufferedBlockingResponse {
            status,
            content_type: Some(HeaderValue::from_static("application/problem+json")),
            idempotency_keys: vec![],
            locations: vec![],
            cache_controls: vec![],
            retry_afters: vec![],
            request_ids: request_ids
                .iter()
                .map(|value| HeaderValue::from_static(value))
                .collect(),
            body: serde_json::to_vec(&serde_json::json!({
                "type": format!("https://api.usefulmachinery.com/problems/{slug}"),
                "title": "Server title", "status": status.as_u16(),
                "detail": "Server detail"
            }))
            .unwrap(),
        }
    }

    // REQ-RUN-80
    #[test]
    fn source_admission_problem_on_create_is_a_definite_rejection_with_request_id() {
        let request_id = "req_01m622k6qsf09npetkmdkdj51j";
        assert_eq!(
            classify_failure(
                &create_problem_response(
                    StatusCode::CONFLICT,
                    "workspace-selection-unsupported",
                    &[request_id],
                ),
                RunOperation::Create,
            ),
            RunFailure::AdmissionRejected {
                status: 409,
                problem_type: "workspace-selection-unsupported",
                request_id: Some(request_id.to_owned()),
            }
        );
    }

    // REQ-RUN-80
    #[test]
    fn source_admission_rejection_omits_an_absent_or_untyped_request_id() {
        for request_ids in [&["not-an-id"][..], &[]] {
            assert_eq!(
                classify_failure(
                    &create_problem_response(
                        StatusCode::CONFLICT,
                        "workspace-selection-unsupported",
                        request_ids,
                    ),
                    RunOperation::Create,
                ),
                RunFailure::AdmissionRejected {
                    status: 409,
                    problem_type: "workspace-selection-unsupported",
                    request_id: None,
                }
            );
        }
    }

    // REQ-RUN-80
    #[test]
    fn invalid_workflow_source_on_create_is_a_definite_rejection() {
        assert_eq!(
            classify_failure(
                &create_problem_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "workflow-source-invalid",
                    &[],
                ),
                RunOperation::Create,
            ),
            RunFailure::AdmissionRejected {
                status: 422,
                problem_type: "workflow-source-invalid",
                request_id: None,
            }
        );
    }

    // REQ-RUN-80
    #[test]
    fn unlisted_create_conflict_stays_a_conflict() {
        assert_eq!(
            classify_failure(
                &create_problem_response(StatusCode::CONFLICT, "project-not-ready", &[]),
                RunOperation::Create,
            ),
            RunFailure::Conflict
        );
    }

    // REQ-RUN-57
    #[test]
    fn source_resolution_outage_on_create_stays_unconfirmed() {
        assert_eq!(
            classify_failure(
                &create_problem_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "source-resolution-unavailable",
                    &["req_01m622k6qsf09npetkmdkdj51j"],
                ),
                RunOperation::Create,
            ),
            RunFailure::Unreachable(UnreachableCategory::Server)
        );
    }
}

fn valid_continuation_snapshot(
    snapshot: &models::RunContinuationSnapshot,
    settlement: bool,
) -> bool {
    (snapshot.settled_by.is_some() == settlement)
        && (snapshot.unavailable.is_some()
            && snapshot.value.is_none()
            && snapshot.taken_at.is_none()
            || snapshot.unavailable.is_none()
                && snapshot
                    .value
                    .as_deref()
                    .is_some_and(|value| lowercase_hex(value, 64))
                && snapshot.taken_at.as_deref().is_some_and(valid_timestamp))
}

fn valid_artifact_delivery(delivery: Option<&models::RunArtifactDelivery>) -> bool {
    match delivery {
        None => true,
        Some(models::RunArtifactDelivery::RunArtifactDeliverySucceeded(succeeded)) => {
            um_support::valid_typed_id(&succeeded.artifact_set_id, "ats_")
        }
        Some(models::RunArtifactDelivery::RunArtifactDeliveryRegistrationFailed(_))
        | Some(models::RunArtifactDelivery::RunArtifactDeliveryUploadFailed(_))
        | Some(models::RunArtifactDelivery::RunArtifactDeliveryPreparationFailed(_)) => true,
    }
}

pub(super) fn require_exact_header<'a>(
    mut values: impl Iterator<Item = &'a HeaderValue>,
    expected: &str,
) -> Result<(), RunFailure> {
    if values.next().and_then(|value| value.to_str().ok()) == Some(expected)
        && values.next().is_none()
    {
        Ok(())
    } else {
        Err(RunFailure::protocol(false))
    }
}

fn require_problem_type(
    response: &ReceivedResponse,
    expected_type: Option<&str>,
    credential_rejected: bool,
) -> Result<(), RunFailure> {
    let decoded = problem::decode_header_parts(
        &response.body,
        response.status,
        response.content_type.as_ref(),
    )
    .map_err(|_| RunFailure::protocol(credential_rejected))?;
    if expected_type.is_none_or(|expected| decoded.r#type == expected) {
        Ok(())
    } else {
        Err(RunFailure::protocol(credential_rejected))
    }
}

pub(super) fn require_media_type(
    response: &ReceivedResponse,
    expected: &str,
    credential_rejected: bool,
) -> Result<(), RunFailure> {
    http_util::require_header_media_type(response.content_type.as_ref(), expected)
        .map_err(|_| RunFailure::protocol(credential_rejected))
}

pub fn valid_integration_context<'a>(
    entry_count: usize,
    entries: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> bool {
    if entry_count > 32 {
        return false;
    }
    let mut encoded_size = 2;
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if key.is_empty()
            || key.len() > 64
            || value.len() > 1024
            || key.contains('\0')
            || value.contains('\0')
        {
            return false;
        }
        encoded_size += usize::from(index > 0)
            + compact_json_string_size(key)
            + 1
            + compact_json_string_size(value);
        if encoded_size > 16 * 1024 {
            return false;
        }
    }
    true
}

// This is the shared API/CLI size contract: compact UTF-8 JSON escapes only
// syntax and controls, so HTML-sensitive and Unicode separator characters keep
// their literal UTF-8 size.
fn compact_json_string_size(value: &str) -> usize {
    2 + value
        .chars()
        .map(|character| match character {
            '"' | '\\' | '\u{0008}' | '\u{0009}' | '\u{000a}' | '\u{000c}' | '\u{000d}' => 2,
            '\u{0000}'..='\u{001f}' => 6,
            _ => character.len_utf8(),
        })
        .sum::<usize>()
}

fn valid_bounded_string(value: &str, minimum: usize, maximum: usize) -> bool {
    let length = value.chars().count();
    (minimum..=maximum).contains(&length)
}

fn valid_canonical_workflow_path(value: &str) -> bool {
    valid_bounded_string(value, 1, 4096)
        && !value.starts_with('/')
        && value.split('/').all(|component| {
            !component.is_empty()
                && component != "."
                && component != ".."
                && !component.contains('\0')
        })
}

fn lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_timestamp(value: &str) -> bool {
    OffsetDateTime::parse(value, &Rfc3339).is_ok()
}

#[derive(Debug)]
pub enum RunApiError {
    InvalidEndpoint,
    InsecureHttp,
    BuildClient(reqwest::Error),
}

impl fmt::Display for RunApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                formatter,
                "the deployment API URL cannot form a Cloud run endpoint"
            ),
            Self::InsecureHttp => write!(
                formatter,
                "the deployment API URL uses insecure HTTP; rerun with --allow-insecure-http to permit it"
            ),
            Self::BuildClient(error) => write!(formatter, "prepare Cloud run networking: {error}"),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "lifecycle view tests use assertions and fixture extraction"
)]
mod linear_lifecycle_tests {
    use super::*;

    const STATES: [&str; 6] = [
        "awaiting_event",
        "current",
        "skipped",
        "superseded",
        "relinquished",
        "stopped",
    ];
    // The "Binding ownership states and reason codes" table, one row per code.
    const TABLE: [(&str, &str); 18] = [
        ("team_unmapped", "skipped"),
        ("superseded_run", "superseded"),
        ("issue_state_changed", "relinquished"),
        ("team_changed", "stopped"),
        ("lifecycle_disabled", "stopped"),
        ("trigger_disabled", "stopped"),
        ("trigger_deleted", "stopped"),
        ("grant_replaced", "stopped"),
        ("connection_disconnected", "stopped"),
        ("connection_deleted", "stopped"),
        ("connection_revoked", "stopped"),
        ("connection_recovery_required", "stopped"),
        ("write_permission_lost", "stopped"),
        ("team_access_lost", "stopped"),
        ("principal_unavailable", "stopped"),
        ("membership_lost", "stopped"),
        ("permission_lost", "stopped"),
        ("organization_unavailable", "stopped"),
    ];

    fn view(state: &str, code: Option<&str>) -> serde_json::Value {
        let mut view = serde_json::json!({
            "triggerId": "trg_01k0z6r1w8f4jy2m7q9v3x5abc",
            "configurationVersion": 1,
            "connectionId": "lcn_01k0z6r1w8f4jy2m7q9v3x5abc",
            "workspaceId": "a3b2c1d0-1111-4222-8333-944455556666",
            "issueId": "b4c3d2e1-2222-4333-8444-955566667777",
            "ownershipState": state,
        });
        if let Some(code) = code {
            view["reasonCode"] = serde_json::json!(code);
        }
        view
    }

    fn accepted(value: &serde_json::Value) -> bool {
        serde_json::from_value::<RunLinearLifecycle>(value.clone())
            .is_ok_and(|view| validate_linear_lifecycle(&view))
    }

    // REQ-TRIGGER-107
    #[test]
    fn linear_lifecycle_accepts_exactly_the_reason_and_state_table() {
        for state in STATES {
            assert_eq!(
                accepted(&view(state, None)),
                matches!(state, "awaiting_event" | "current"),
                "{state} without a code"
            );
        }
        for (code, required) in TABLE {
            for state in STATES {
                assert_eq!(
                    accepted(&view(state, Some(code))),
                    state == required,
                    "{code} with {state}"
                );
            }
        }
        assert!(!accepted(&view("stopped", Some("unclassified"))));
        assert!(!accepted(&view("released", None)));
    }

    // REQ-TRIGGER-107
    #[test]
    fn linear_lifecycle_refuses_half_attempts_stray_nodes_and_bad_identities() {
        let event = serde_json::json!({
            "eventId": "evt-1", "eventType": "step.started", "sequence": 4,
            "occurredAt": "2026-10-26T12:00:00Z",
            "attemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abc", "attemptNumber": 1,
            "node": {"scope": [], "role": "step", "id": "build"}
        });
        let mut complete = view("current", None);
        complete["attemptId"] = serde_json::json!("atm_01k0z6r1w8f4jy2m7q9v3x5abc");
        complete["attemptNumber"] = serde_json::json!(1);
        complete["ownershipEvent"] = event;
        assert!(accepted(&complete));
        for member in ["attemptId", "attemptNumber"] {
            let mut half = complete.clone();
            half.as_object_mut().unwrap().remove(member);
            assert!(!accepted(&half), "attempt pair without {member}");
        }
        for event_type in ["run.started", "publication.published"] {
            let mut stray = complete.clone();
            stray["ownershipEvent"]["eventType"] = serde_json::json!(event_type);
            assert!(!accepted(&stray), "node on {event_type}");
            stray["ownershipEvent"]
                .as_object_mut()
                .unwrap()
                .remove("node");
            assert!(accepted(&stray), "{event_type} without a node");
        }
        let mut incomplete = complete.clone();
        incomplete["ownershipEvent"]
            .as_object_mut()
            .unwrap()
            .remove("sequence");
        assert!(!accepted(&incomplete));
        let mut controlling = complete.clone();
        controlling["controllingRunId"] = serde_json::json!("run_01k0z6r1w8f4jy2m7q9v3x5abc");
        assert!(!accepted(&controlling)); // Only a superseded binding names a controller.
        for (member, value) in [
            ("triggerId", serde_json::json!("trigger-1")),
            (
                "connectionId",
                serde_json::json!("rpc_01k0z6r1w8f4jy2m7q9v3x5abc"),
            ),
            ("configurationVersion", serde_json::json!(0)),
            ("workspaceId", serde_json::json!("")),
        ] {
            let mut invalid = view("awaiting_event", None);
            invalid[member] = value;
            assert!(!accepted(&invalid), "{member}");
        }
    }

    const UPDATE_STATES: [&str; 8] = [
        "pending",
        "retry_waiting",
        "applied",
        "already_satisfied",
        "preserved",
        "skipped",
        "failed",
        "stopped",
    ];
    // The "Update states and reason codes" table, one row per code.
    const UPDATE_TABLE: [(&str, &[&str]); 27] = [
        ("superseded_run", &["skipped"]),
        ("superseded_attempt", &["skipped"]),
        ("superseded_event", &["skipped"]),
        ("cancellation_preserves_state", &["preserved"]),
        ("issue_state_changed", &["stopped"]),
        ("team_changed", &["stopped"]),
        ("lifecycle_disabled", &["stopped"]),
        ("trigger_disabled", &["stopped"]),
        ("trigger_deleted", &["stopped"]),
        ("grant_replaced", &["stopped"]),
        ("connection_disconnected", &["stopped"]),
        ("connection_deleted", &["stopped"]),
        ("connection_revoked", &["stopped"]),
        ("connection_recovery_required", &["stopped"]),
        ("write_permission_lost", &["stopped"]),
        ("team_access_lost", &["stopped"]),
        ("principal_unavailable", &["stopped"]),
        ("membership_lost", &["stopped"]),
        ("permission_lost", &["stopped"]),
        ("organization_unavailable", &["stopped"]),
        ("provider_unavailable", &["retry_waiting"]),
        ("rate_limited", &["retry_waiting"]),
        ("issue_unavailable", &["failed"]),
        ("state_unavailable", &["failed"]),
        ("outcome_unresolved", &["pending", "retry_waiting"]),
        ("internal_sync_failure", &["retry_waiting", "failed"]),
        ("retry_exhausted", &["failed"]),
    ];

    // A view whose latest update is in state with code; the desired state
    // follows the preserved rule so only the pair varies.
    fn with_update(state: &str, code: Option<&str>) -> serde_json::Value {
        let mut update = serde_json::json!({
            "id": "llu_01k0z6r1w8f4jy2m7q9v3x5abc",
            "attemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abc", "attemptNumber": 1,
            "eventId": "evt-2", "eventType": "run.started", "eventSequence": 2,
            "eventOccurredAt": "2026-10-26T12:01:00Z",
            "createdAt": "2026-10-26T12:01:01Z", "updatedAt": "2026-10-26T12:01:01Z",
            "state": state, "cycleNumber": 1, "cycleDeadline": "2026-10-27T12:01:00Z",
            "requestCount": 0, "retryable": false,
        });
        if state != "preserved" {
            update["desiredStateId"] = serde_json::json!("state-progress");
        }
        if let Some(code) = code {
            update["reasonCode"] = serde_json::json!(code);
        }
        let mut view = view("awaiting_event", None);
        view["latestUpdate"] = update;
        view
    }

    // REQ-TRIGGER-111
    #[test]
    fn linear_lifecycle_update_accepts_exactly_the_reason_and_state_table() {
        for state in UPDATE_STATES {
            assert_eq!(
                accepted(&with_update(state, None)),
                matches!(state, "pending" | "applied" | "already_satisfied"),
                "{state} without a code"
            );
        }
        for (code, allowed) in UPDATE_TABLE {
            for state in UPDATE_STATES {
                assert_eq!(
                    accepted(&with_update(state, Some(code))),
                    allowed.contains(&state),
                    "{code} with {state}"
                );
            }
        }
        assert!(!accepted(&with_update("failed", Some("unclassified"))));
        assert!(!accepted(&with_update("queued", None)));
    }

    // REQ-TRIGGER-111
    #[test]
    fn linear_lifecycle_update_refuses_stray_members() {
        let mut complete = with_update("retry_waiting", Some("outcome_unresolved"));
        let update = &mut complete["latestUpdate"];
        update["eventType"] = serde_json::json!("step.failed");
        update["node"] = serde_json::json!({"scope": [], "role": "step", "id": "build"});
        update["observedStateId"] = serde_json::json!("state-todo");
        update["confirmedStateId"] = serde_json::json!("state-todo");
        update["nextAttemptAt"] = serde_json::json!("2026-10-26T12:05:00Z");
        update["requestCount"] = serde_json::json!(2);
        update["retryable"] = serde_json::json!(true);
        update["blockingUpdate"] = serde_json::json!({
            "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abc",
            "updateId": "llu_01k0z6r1w8f4jy2m7q9v3x5abd"
        });
        assert!(accepted(&complete));
        let refused = |member: &str, value: Option<serde_json::Value>| {
            let mut changed = complete.clone();
            let update = changed["latestUpdate"].as_object_mut().unwrap();
            match value {
                Some(value) => update.insert(member.to_owned(), value),
                None => update.remove(member),
            };
            !accepted(&changed)
        };
        assert!(refused("node", None), "step event without its node");
        assert!(refused("eventType", Some(serde_json::json!("run.failed"))));
        assert!(refused("id", Some(serde_json::json!("update-1"))));
        assert!(refused("cycleNumber", Some(serde_json::json!(0))));
        assert!(refused("requestCount", Some(serde_json::json!(-1))));
        assert!(refused("desiredStateId", None));
        assert!(refused("desiredStateId", Some(serde_json::json!(""))));
        assert!(refused(
            "cycleDeadline",
            Some(serde_json::json!("tomorrow"))
        ));
        assert!(refused(
            "reasonCode",
            Some(serde_json::json!("rate_limited"))
        ));
        assert!(refused(
            "blockingUpdate",
            Some(
                serde_json::json!({"runId": "run-1", "updateId": "llu_01k0z6r1w8f4jy2m7q9v3x5abd"})
            )
        ));
        let mut preserved = with_update("preserved", Some("cancellation_preserves_state"));
        assert!(accepted(&preserved));
        preserved["latestUpdate"]["desiredStateId"] = serde_json::json!("state-done");
        assert!(
            !accepted(&preserved),
            "preserved update with a desired state"
        );
    }

    // REQ-TRIGGER-111
    #[test]
    fn linear_lifecycle_update_refuses_an_empty_diagnostic() {
        let mut exhausted = with_update("failed", Some("retry_exhausted"));
        exhausted["latestUpdate"]["retryable"] = serde_json::json!(true);
        exhausted["latestUpdate"]["diagnostic"] = serde_json::json!({
            "cause": "provider_unavailable", "providerStatus": 503,
            "transportError": "timed_out", "retryAfter": "2026-10-26T12:10:00Z"
        });
        assert!(accepted(&exhausted));
        let with_diagnostic = |diagnostic: serde_json::Value| {
            let mut changed = exhausted.clone();
            changed["latestUpdate"]["diagnostic"] = diagnostic;
            accepted(&changed)
        };
        assert!(with_diagnostic(
            serde_json::json!({"cause": "provider_unavailable"})
        ));
        assert!(!with_diagnostic(serde_json::json!({})), "empty diagnostic");
        assert!(!with_diagnostic(serde_json::json!({"cause": ""})));
        assert!(!with_diagnostic(serde_json::json!({"retryAfter": "later"})));
        assert!(!with_diagnostic(
            serde_json::json!({"providerStatus": "503"})
        ));
    }

    fn history_response(
        page: &serde_json::Value,
        cache_control: Option<&'static str>,
    ) -> ReceivedResponse {
        BufferedBlockingResponse {
            status: StatusCode::OK,
            content_type: Some(HeaderValue::from_static("application/json")),
            idempotency_keys: vec![],
            locations: vec![],
            cache_controls: cache_control
                .into_iter()
                .map(HeaderValue::from_static)
                .collect(),
            retry_afters: vec![],
            request_ids: vec![],
            body: serde_json::to_vec(page).unwrap(),
        }
    }

    // REQ-TRIGGER-118, REQ-TRIGGER-111
    #[test]
    fn linear_lifecycle_history_accepts_only_valid_pages() {
        let update =
            |state: &str, code: Option<&str>| with_update(state, code)["latestUpdate"].clone();
        let mut exhausted = update("failed", Some("retry_exhausted"));
        exhausted["retryable"] = serde_json::json!(true);
        exhausted["diagnostic"] = serde_json::json!({"cause": "provider_unavailable"});
        let page = serde_json::json!({
            "items": [update("pending", None), exhausted],
            "nextCursor": "opaque-cursor"
        });
        let read = |page: &serde_json::Value| {
            decode_linear_lifecycle_update_list(history_response(page, Some(PRIVATE_CACHE_CONTROL)))
        };
        let list = read(&page).unwrap();
        assert_eq!(list.items.len(), 2);
        assert_eq!(list.next_cursor.as_deref(), Some("opaque-cursor"));
        assert_eq!(
            list.items[1]
                .diagnostic
                .as_deref()
                .and_then(|facts| facts.cause.as_deref()),
            Some("provider_unavailable")
        );
        assert!(
            read(&serde_json::json!({"items": []})).is_ok_and(|list| list.next_cursor.is_none())
        );
        assert!(
            decode_linear_lifecycle_update_list(history_response(&page, None)).is_err(),
            "missing Cache-Control"
        );
        let refused = |change: &dyn Fn(&mut serde_json::Value)| {
            let mut changed = page.clone();
            change(&mut changed);
            read(&changed).is_err()
        };
        assert!(refused(
            &|page| page["items"][0]["state"] = serde_json::json!("skipped")
        ));
        assert!(refused(
            &|page| page["items"][1]["diagnostic"] = serde_json::json!({})
        ));
        assert!(refused(&|page| page["nextCursor"] = serde_json::json!("")));
        assert!(refused(&|page| {
            page["items"] = serde_json::Value::Array(vec![update("pending", None); 101]);
        }));
    }

    // REQ-TRIGGER-107
    #[test]
    fn linear_lifecycle_invalid_view_makes_the_run_read_invalid() {
        let mut document: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/run-continuation-pending.json"
        ))
        .unwrap();
        let run = &mut document["run"];
        let run_id = run["id"].as_str().unwrap().to_owned();
        let valid = |value: &serde_json::Value| {
            let run: models::Run = serde_json::from_value(value.clone()).unwrap();
            validate_run(run, &run_id).is_ok()
        };
        run["linearLifecycle"] = view("skipped", Some("team_unmapped"));
        assert!(valid(run));
        run["linearLifecycle"] = view("awaiting_event", Some("team_unmapped"));
        assert!(!valid(run));
    }
}
