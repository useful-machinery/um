//! Policy-aware webhook transport. Generated models describe the wire, not its response policy.
use std::time::Duration;

use reqwest::{Method, StatusCode, Url, header::CONTENT_TYPE};
use serde::de::DeserializeOwned;

use super::{
    HttpTransportPolicy, UnreachableCategory, classify_reqwest_error,
    generated::{apis, models},
    http_client::generated_configuration,
    http_util, problem,
};

pub use models::{WebhookDelivery, WebhookDeliveryList};

/// Generated wire decoding with secret-safe diagnostic formatting. Explicit
/// serialization remains available for the show-once create/rotate response.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct WebhookSubscription(models::WebhookSubscription);

impl std::ops::Deref for WebhookSubscription {
    type Target = models::WebhookSubscription;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for WebhookSubscription {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::fmt::Debug for WebhookSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebhookSubscription")
            .field("id", &self.id)
            .field("project_id", &self.project_id)
            .field("version", &self.version)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct WebhookSubscriptionList {
    pub items: Vec<WebhookSubscription>,
    pub next_cursor: Option<String>,
}

impl<'de> serde::Deserialize<'de> for WebhookSubscriptionList {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = models::WebhookSubscriptionList::deserialize(deserializer)?;
        Ok(Self {
            items: wire.items.into_iter().map(WebhookSubscription).collect(),
            next_cursor: wire.next_cursor,
        })
    }
}

pub struct WebhookApi {
    configuration: apis::configuration::Configuration,
}

#[derive(Clone, Copy, Debug)]
pub enum WebhookFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    NotFound,
    Conflict,
    RateLimited(u64),
    Unreachable(UnreachableCategory),
    Protocol { credential_rejected: bool },
}

// Webhook and run failures are separate closed policy domains despite sharing
// the credential rejection predicate used by human-session refresh.
impl WebhookFailure {
    pub fn credential_rejected(&self) -> bool {
        matches!(
            self,
            Self::Unauthenticated
                | Self::Protocol {
                    credential_rejected: true
                }
        )
    }
}

#[derive(Clone, Copy)]
pub enum Mutation {
    Create,
    Update,
    Enable,
    Disable,
    Delete,
    Rotate,
    Revoke,
    Test,
    Replay,
}
impl Mutation {
    fn status(self) -> StatusCode {
        match self {
            Self::Create => StatusCode::CREATED,
            Self::Delete => StatusCode::NO_CONTENT,
            Self::Test | Self::Replay => StatusCode::ACCEPTED,
            _ => StatusCode::OK,
        }
    }
    fn method(self) -> Method {
        match self {
            Self::Create
            | Self::Enable
            | Self::Disable
            | Self::Rotate
            | Self::Revoke
            | Self::Test
            | Self::Replay => Method::POST,
            Self::Update => Method::PATCH,
            Self::Delete => Method::DELETE,
        }
    }
}

impl WebhookApi {
    pub fn new(
        api_url: &str,
        token: &str,
        policy: HttpTransportPolicy,
    ) -> Result<Self, super::projects::ProjectApiError> {
        use super::projects::ProjectApiError as Error;
        let url = Url::parse(api_url).map_err(|_| Error::InvalidEndpoint)?;
        if !policy.permits(&url) {
            return Err(if url.scheme() == "http" {
                Error::InsecureHttp
            } else {
                Error::InvalidEndpoint
            });
        }
        if url.cannot_be_a_base() || url.query().is_some() || url.fragment().is_some() {
            return Err(Error::InvalidEndpoint);
        }
        Ok(Self {
            configuration: generated_configuration(api_url, token, policy, Duration::from_secs(20))
                .map_err(Error::BuildClient)?,
        })
    }

    pub fn subscriptions(
        &self,
        org: &str,
        project: &str,
        limit: Option<u16>,
        cursor: Option<&str>,
    ) -> Result<WebhookSubscriptionList, WebhookFailure> {
        let page: WebhookSubscriptionList = self.read(
            &["v1", "organizations", org, "projects", project, "webhooks"],
            limit,
            cursor,
            &[],
        )?;
        if page.next_cursor.as_deref() == Some("")
            || page
                .items
                .iter()
                .any(|item| !valid_subscription(item, project, false))
        {
            return Err(protocol(false));
        }
        Ok(page)
    }
    pub fn subscription(
        &self,
        org: &str,
        project: &str,
        webhook: &str,
    ) -> Result<WebhookSubscription, WebhookFailure> {
        let item: WebhookSubscription = self.read(&base(org, project, webhook), None, None, &[])?;
        if !valid_subscription(&item, project, false) || item.id != webhook {
            return Err(protocol(false));
        }
        Ok(item)
    }
    pub fn deliveries(
        &self,
        org: &str,
        project: &str,
        webhook: &str,
        limit: Option<u16>,
        cursor: Option<&str>,
        filters: (Option<&str>, Option<&str>),
    ) -> Result<WebhookDeliveryList, WebhookFailure> {
        let (run_id, state) = filters;
        let path = child(org, project, webhook, "deliveries");
        let mut filters = Vec::new();
        if let Some(run_id) = run_id {
            filters.push(("runId", run_id));
        }
        if let Some(state) = state {
            filters.push(("state", state));
        }
        let page: WebhookDeliveryList = self.read(&path, limit, cursor, &filters)?;
        if page.next_cursor.as_deref() == Some("")
            || page.items.iter().any(|item| {
                !valid_delivery(item)
                    || run_id.is_some_and(|id| item.run_id.as_deref() != Some(id))
                    || state.is_some_and(|state| {
                        serde_json::to_value(item.state)
                            .ok()
                            .as_ref()
                            .and_then(serde_json::Value::as_str)
                            != Some(state)
                    })
            })
        {
            return Err(protocol(false));
        }
        Ok(page)
    }
    pub fn delivery(
        &self,
        org: &str,
        project: &str,
        webhook: &str,
        delivery: &str,
    ) -> Result<WebhookDelivery, WebhookFailure> {
        let mut path = child(org, project, webhook, "deliveries").to_vec();
        path.push(delivery);
        let item: WebhookDelivery = self.read(&path, None, None, &[])?;
        if item.id != delivery || !valid_delivery(&item) {
            return Err(protocol(false));
        }
        Ok(item)
    }
    pub fn queue_delivery(
        &self,
        kind: Mutation,
        org: &str,
        project: &str,
        webhook: &str,
        delivery: Option<&str>,
        key: &str,
    ) -> Result<WebhookDelivery, WebhookFailure> {
        let result = self.mutate::<WebhookDelivery>(
            kind,
            org,
            project,
            (Some(webhook), delivery),
            key,
            None::<&serde_json::Value>,
        )?;
        let value = result.ok_or_else(|| protocol(false))?;
        if !valid_delivery(&value)
            || match kind {
                Mutation::Test => value.event_type != "webhook.test" || delivery.is_some(),
                Mutation::Replay => delivery.is_none_or(|id| value.id != id),
                _ => true,
            }
        {
            return Err(protocol(false));
        }
        Ok(value)
    }
    fn read<T: DeserializeOwned>(
        &self,
        path: &[&str],
        limit: Option<u16>,
        cursor: Option<&str>,
        filters: &[(&str, &str)],
    ) -> Result<T, WebhookFailure> {
        let mut query: Vec<(&str, String)> = filters
            .iter()
            .map(|(key, value)| (*key, (*value).to_owned()))
            .collect();
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor.to_owned()));
        }
        self.request(Method::GET, path, &query, None, None, None)
    }
    pub fn mutate<T: DeserializeOwned>(
        &self,
        kind: Mutation,
        org: &str,
        project: &str,
        target: (Option<&str>, Option<&str>),
        key: &str,
        body: Option<&impl serde::Serialize>,
    ) -> Result<Option<T>, WebhookFailure> {
        let (webhook, delivery) = target;
        let mut path = vec!["v1", "organizations", org, "projects", project, "webhooks"];
        if let Some(webhook) = webhook {
            path.push(webhook);
        }
        if let Some(delivery) = delivery {
            path.extend(["deliveries", delivery]);
        }
        match kind {
            Mutation::Enable => path.push("enable"),
            Mutation::Disable => path.push("disable"),
            Mutation::Rotate => path.push("rotate-secret"),
            Mutation::Revoke => path.push("revoke-previous-secret"),
            Mutation::Test => path.push("test"),
            Mutation::Replay => path.push("replay"),
            _ => (),
        }
        let bytes = body
            .map(|body| serde_json::to_vec(body).map_err(|_| protocol(false)))
            .transpose()?;
        self.request(
            kind.method(),
            &path,
            &[],
            Some(key),
            bytes.as_deref(),
            Some(kind),
        )
    }
    fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &[&str],
        query: &[(&str, String)],
        key: Option<&str>,
        body: Option<&[u8]>,
        kind: Option<Mutation>,
    ) -> Result<T, WebhookFailure> {
        let expected = kind.map_or(StatusCode::OK, Mutation::status);
        let endpoint = format!(
            "{}/{}",
            self.configuration.base_path.trim_end_matches('/'),
            path.iter()
                .map(apis::urlencode)
                .collect::<Vec<_>>()
                .join("/")
        );
        let mut last = UnreachableCategory::Connection;
        for attempt in 0..if key.is_some() { 2 } else { 1 } {
            let mut request =
                super::generated_api_request(&self.configuration, method.clone(), &endpoint);
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
                    last = classify_reqwest_error(&error);
                    if attempt == 0
                        && key.is_some()
                        && matches!(
                            last,
                            UnreachableCategory::Connection | UnreachableCategory::Timeout
                        )
                    {
                        continue;
                    }
                    return Err(WebhookFailure::Unreachable(last));
                }
            };
            let status = response.status();
            let headers = response.headers().clone();
            let bytes = match http_util::read_bounded_blocking_body(response) {
                Ok(bytes) => bytes,
                Err(http_util::BoundedBodyError::Transport(error))
                    if status == expected && key.is_some() && attempt == 0 =>
                {
                    last = classify_reqwest_error(&error);
                    continue;
                }
                Err(http_util::BoundedBodyError::Transport(error)) => {
                    return Err(WebhookFailure::Unreachable(classify_reqwest_error(&error)));
                }
                Err(http_util::BoundedBodyError::TooLarge) => {
                    return Err(protocol(status == StatusCode::UNAUTHORIZED));
                }
            };
            if status != expected {
                if status.is_server_error() {
                    return Err(WebhookFailure::Unreachable(UnreachableCategory::Server));
                }
                if headers
                    .get(CONTENT_TYPE)
                    .and_then(|v| http_util::media_type(v).ok())
                    .as_deref()
                    != Some(problem::PROBLEM_MEDIA_TYPE)
                {
                    return Err(protocol(status == StatusCode::UNAUTHORIZED));
                }
                let decoded = problem::decode(&bytes, status)
                    .map_err(|_| protocol(status == StatusCode::UNAUTHORIZED))?;
                let failure = match (status, decoded.r#type.as_str()) {
                    (
                        StatusCode::UNAUTHORIZED,
                        "https://api.usefulmachinery.com/problems/unauthorized",
                    ) => WebhookFailure::Unauthenticated,
                    (
                        StatusCode::FORBIDDEN,
                        "https://api.usefulmachinery.com/problems/forbidden",
                    ) => WebhookFailure::Forbidden,
                    (
                        StatusCode::NOT_FOUND,
                        "https://api.usefulmachinery.com/problems/not-found",
                    ) => WebhookFailure::NotFound,
                    (
                        StatusCode::BAD_REQUEST,
                        "https://api.usefulmachinery.com/problems/bad-request",
                    )
                    | (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "https://api.usefulmachinery.com/problems/payload-too-large",
                    )
                    | (
                        StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        "https://api.usefulmachinery.com/problems/unsupported-media-type",
                    ) => WebhookFailure::InvalidInput,
                    (
                        StatusCode::CONFLICT,
                        "https://api.usefulmachinery.com/problems/webhook-conflict",
                    ) => WebhookFailure::Conflict,
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        "https://api.usefulmachinery.com/problems/rate-limited",
                    ) => {
                        let delay = headers
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok())
                            .filter(|n| {
                                *n > 0
                                    && n.to_string()
                                        == headers
                                            .get("Retry-After")
                                            .and_then(|v| v.to_str().ok())
                                            .unwrap_or("")
                            });
                        delay.map_or_else(|| protocol(false), WebhookFailure::RateLimited)
                    }
                    _ => protocol(status == StatusCode::UNAUTHORIZED),
                };
                return Err(failure);
            }
            if expected != StatusCode::NO_CONTENT
                && headers
                    .get(CONTENT_TYPE)
                    .and_then(|v| http_util::media_type(v).ok())
                    .as_deref()
                    != Some(problem::JSON_MEDIA_TYPE)
            {
                return Err(protocol(false));
            }
            if let Some(key) = key {
                let mut values = headers.get_all("Idempotency-Key").iter();
                if values.next().and_then(|v| v.to_str().ok()) != Some(key)
                    || values.next().is_some()
                {
                    return Err(protocol(false));
                }
            }
            if expected == StatusCode::NO_CONTENT {
                if !bytes.is_empty() {
                    return Err(protocol(false));
                }
                return serde_json::from_str("null").map_err(|_| protocol(false));
            }
            return serde_json::from_slice(&bytes).map_err(|_| protocol(false));
        }
        Err(WebhookFailure::Unreachable(last))
    }
}
fn valid_subscription(value: &WebhookSubscription, project: &str, allow_secret: bool) -> bool {
    um_support::valid_typed_id(&value.id, "whs_")
        && value.project_id == project
        && value.version > 0
        && (allow_secret || value.secret.is_none())
}

fn valid_delivery(value: &WebhookDelivery) -> bool {
    let lifecycle = matches!(
        value.event_type.as_str(),
        "run.queued"
            | "run.started"
            | "run.succeeded"
            | "run.failed"
            | "run.cancelled"
            | "run.interrupted"
            | "run.rejected"
            | "step.started"
            | "step.succeeded"
            | "step.failed"
            | "step.skipped"
            | "step.cancelled"
            | "step.blocked"
            | "step.not_run"
    );
    let step = value.event_type.starts_with("step.");
    um_support::valid_typed_id(&value.id, "whd_")
        && um_support::valid_typed_id(&value.event_id, "evt_")
        && value.subscription_version > 0
        && !value.created_at.is_empty()
        && if lifecycle {
            value
                .run_id
                .as_deref()
                .is_some_and(|id| um_support::valid_typed_id(id, "run_"))
                && value
                    .attempt_id
                    .as_deref()
                    .is_some_and(|id| um_support::valid_typed_id(id, "atm_"))
                && value
                    .workflow_path
                    .as_deref()
                    .is_some_and(|path| !path.is_empty())
                && value.sequence.is_some_and(|number| number > 0)
                && (step == value.node.is_some())
                && value.node.as_ref().is_none_or(|node| {
                    node.scope.len() <= 64
                        && node
                            .scope
                            .iter()
                            .all(|part| !part.is_empty() && part.len() <= 64)
                        && !node.id.is_empty()
                        && node.id.len() <= 64
                })
        } else {
            value.event_type == "webhook.test"
                && value.run_id.is_none()
                && value.attempt_id.is_none()
                && value.workflow_path.is_none()
                && value.sequence.is_none()
                && value.node.is_none()
        }
        && value.cycles.as_ref().is_none_or(|cycles| {
            cycles.iter().all(|cycle| {
                cycle.number > 0
                    && cycle.attempts.iter().all(|attempt| {
                        um_support::valid_typed_id(&attempt.id, "wha_")
                            && attempt.current_key_version > 0
                    })
            })
        })
}

fn protocol(credential_rejected: bool) -> WebhookFailure {
    WebhookFailure::Protocol {
        credential_rejected,
    }
}
fn base<'a>(org: &'a str, project: &'a str, webhook: &'a str) -> [&'a str; 7] {
    [
        "v1",
        "organizations",
        org,
        "projects",
        project,
        "webhooks",
        webhook,
    ]
}
fn child<'a>(org: &'a str, project: &'a str, webhook: &'a str, child: &'a str) -> [&'a str; 8] {
    [
        "v1",
        "organizations",
        org,
        "projects",
        project,
        "webhooks",
        webhook,
        child,
    ]
}
impl Drop for WebhookApi {
    fn drop(&mut self) {
        super::clear_generated_access_token(&mut self.configuration);
    }
}
