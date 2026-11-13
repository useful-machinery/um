//! The member's grants of both kinds under `/v1/me/grants` (REQ-GRANT-58 to REQ-GRANT-62),
//! over the generated principals client.
//!
//! Responses decode leniently into the generated models, which name no material, so nothing
//! a response wrongly adds can reach a caller, and a generated error is never formatted: its
//! body is untrusted text. Material travels only in a request body built here from zeroizing
//! storage. The earlier `GrantApi` and `AgentLoginApi` stay for the earlier commands.
use std::fmt;

use reqwest::StatusCode;
use zeroize::Zeroize as _;

use super::agent_logins::{valid_authorization_record, valid_ended_authorization};
use super::generated::{apis, models};
use super::http_client::zeroize_generated_bearer_access_token;
use super::resource::{
    ProblemFailure, ProblemRow, RECORD_SCHEMA_VERSION, ResourceFailure, classify_problem_error,
    confirmed_delete, encode_material, expect_problem_status, mutation_request, organization,
    organization_id, receive_raw, resource_configuration, serialize_body, validate_json_media_type,
    validate_mutation_key,
};
use super::{HttpTransportPolicy, UnreachableCategory, problem};

pub type MemberGrant = models::Grant;
pub type MemberGrantList = models::GrantList;
pub type MemberGrantAuthorization = models::GrantAuthorization;
pub type MemberGrantKind = models::grant::Kind;
pub type MemberGrantProvider = models::grant::Provider;
pub type MemberGrantState = models::grant::State;
// A native grant's exchange with a runner and its ended authorizations are the agent-login
// models the grant carries unchanged (REQ-GRANT-13).
pub type GrantReport = models::AgentLoginReport;
pub type GrantEndedAuthorization = models::AgentLoginEndedAuthorization;

impl MemberGrantKind {
    /// The kind's wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GrantKindOAuth => "oauth",
            Self::GrantKindNative => "native",
        }
    }
}

impl MemberGrantProvider {
    /// The provider's wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GrantProviderGithub => "github",
            Self::GrantProviderGitlabCom => "gitlab.com",
            Self::GrantProviderLinear => "linear",
            Self::GrantProviderClaudeCode => "claude_code",
            Self::GrantProviderCodex => "codex",
        }
    }

    /// The kind every grant of this provider is.
    pub const fn kind(self) -> MemberGrantKind {
        match self {
            Self::GrantProviderGithub
            | Self::GrantProviderGitlabCom
            | Self::GrantProviderLinear => MemberGrantKind::GrantKindOAuth,
            Self::GrantProviderClaudeCode | Self::GrantProviderCodex => {
                MemberGrantKind::GrantKindNative
            }
        }
    }
}

impl MemberGrantState {
    /// The state's wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GrantStateActive => "active",
            Self::GrantStateReconnectRequired => "reconnect_required",
            Self::GrantStateRevoked => "revoked",
        }
    }
}

const MATERIAL_INVALID: &str = "https://api.usefulmachinery.com/problems/grant-material-invalid";
const RUNNER_NOT_FOUND: &str = "https://api.usefulmachinery.com/problems/grant-runner-not-found";
const RUNNER_CONFLICT: &str = "https://api.usefulmachinery.com/problems/grant-runner-conflict";
const ALREADY_AUTHORIZED: &str =
    "https://api.usefulmachinery.com/problems/grant-already-authorized";
const AUTHORIZATION_NOT_FOUND: &str =
    "https://api.usefulmachinery.com/problems/grant-authorization-not-found";
const KIND_MISMATCH: &str = "https://api.usefulmachinery.com/problems/grant-kind-mismatch";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemberGrantFailure {
    Unauthenticated,
    Forbidden,
    InvalidInput,
    IdempotencyConflict,
    ProviderUnavailable,
    GrantNotFound,
    OrganizationNotFound,
    MaterialInvalid,
    LimitReached,
    RunnerNotFound,
    RunnerHoldsLogin,
    AlreadyAuthorized,
    AuthorizationNotFound,
    RateLimited { retry_after: Option<u64> },
    Unreachable { category: UnreachableCategory },
    InvalidResponse { credential_rejected: bool },
}

impl MemberGrantFailure {
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

impl From<ResourceFailure> for MemberGrantFailure {
    fn from(failure: ResourceFailure) -> Self {
        failure.into_problem_failure()
    }
}

impl ProblemFailure for MemberGrantFailure {
    fn rate_limited(retry_after: Option<u64>) -> Self {
        Self::RateLimited { retry_after }
    }

    fn unreachable(category: UnreachableCategory) -> Self {
        Self::Unreachable { category }
    }

    fn invalid_response(credential_rejected: bool) -> Self {
        Self::InvalidResponse {
            credential_rejected,
        }
    }
}

const fn invalid() -> MemberGrantFailure {
    MemberGrantFailure::InvalidResponse {
        credential_rejected: false,
    }
}

/// The problems every operation on the grants resource answers, by status and type. A generic
/// `not-found` is outside the contract here; only the organization lookup answers it.
const PROBLEMS: [ProblemRow<MemberGrantFailure>; 14] = [
    (
        StatusCode::BAD_REQUEST,
        problem::BAD_REQUEST,
        MemberGrantFailure::InvalidInput,
    ),
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        problem::REQUEST_BODY_TOO_LARGE,
        MemberGrantFailure::InvalidInput,
    ),
    (
        StatusCode::UNAUTHORIZED,
        problem::UNAUTHORIZED,
        MemberGrantFailure::Unauthenticated,
    ),
    (
        StatusCode::FORBIDDEN,
        problem::FORBIDDEN,
        MemberGrantFailure::Forbidden,
    ),
    (
        StatusCode::CONFLICT,
        problem::IDEMPOTENCY_CONFLICT,
        MemberGrantFailure::IdempotencyConflict,
    ),
    (
        StatusCode::SERVICE_UNAVAILABLE,
        problem::PROVIDER_UNAVAILABLE,
        MemberGrantFailure::ProviderUnavailable,
    ),
    (
        StatusCode::NOT_FOUND,
        problem::GRANT_NOT_FOUND,
        MemberGrantFailure::GrantNotFound,
    ),
    (
        StatusCode::BAD_REQUEST,
        MATERIAL_INVALID,
        MemberGrantFailure::MaterialInvalid,
    ),
    (
        StatusCode::CONFLICT,
        problem::QUANTITY_LIMIT_REACHED,
        MemberGrantFailure::LimitReached,
    ),
    (
        StatusCode::NOT_FOUND,
        RUNNER_NOT_FOUND,
        MemberGrantFailure::RunnerNotFound,
    ),
    (
        StatusCode::CONFLICT,
        RUNNER_CONFLICT,
        MemberGrantFailure::RunnerHoldsLogin,
    ),
    (
        StatusCode::CONFLICT,
        ALREADY_AUTHORIZED,
        MemberGrantFailure::AlreadyAuthorized,
    ),
    (
        StatusCode::NOT_FOUND,
        AUTHORIZATION_NOT_FOUND,
        MemberGrantFailure::AuthorizationNotFound,
    ),
    // A native grant's operation, its next generation, on an OAuth grant: the request names a
    // grant of the wrong kind.
    (
        StatusCode::CONFLICT,
        KIND_MISMATCH,
        MemberGrantFailure::InvalidInput,
    ),
];

/// The problem only the operations that name an organization, its lookup and an import,
/// answer.
const ORGANIZATION_PROBLEMS: [ProblemRow<MemberGrantFailure>; 1] = [(
    StatusCode::NOT_FOUND,
    problem::NOT_FOUND,
    MemberGrantFailure::OrganizationNotFound,
)];

fn classify_error<E>(error: apis::Error<E>) -> MemberGrantFailure {
    classify_problem_error(error, &PROBLEMS)
}

/// The largest page the list serves.
pub const MEMBER_GRANT_PAGE: i32 = 200;

/// The member's active organizations on one page, each as its ID and slug, and the cursor of
/// the next page.
type OrganizationPage = (Vec<(String, String)>, Option<String>);

pub struct MemberGrantApi {
    configuration: apis::configuration::Configuration,
}

impl MemberGrantApi {
    pub fn new(
        api_url: &str,
        token: &str,
        policy: HttpTransportPolicy,
    ) -> Result<Self, MemberGrantApiError> {
        let configuration = resource_configuration(
            api_url,
            token,
            policy,
            MemberGrantApiError::InvalidEndpoint,
            MemberGrantApiError::BuildClient,
        )?;
        Ok(Self { configuration })
    }

    fn endpoint(&self, segments: &[&str]) -> String {
        let mut endpoint = format!(
            "{}/v1/me/grants",
            self.configuration.base_path.trim_end_matches('/')
        );
        for segment in segments {
            endpoint.push('/');
            endpoint.push_str(&apis::urlencode(segment));
        }
        endpoint
    }

    /// One page of the member's grants of `kind`, or of both kinds, OAuth grants first, when
    /// it is `None` (REQ-GRANT-58).
    pub fn list(
        &self,
        kind: Option<MemberGrantKind>,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<MemberGrantList, MemberGrantFailure> {
        let filter = kind.map_or("all", MemberGrantKind::as_str);
        let page =
            apis::principals_api::list_grants(&self.configuration, Some(filter), limit, cursor)
                .map_err(classify_error)?;
        if !page
            .items
            .iter()
            .all(|grant| valid_grant(grant) && kind.is_none_or(|kind| grant.kind == kind))
        {
            return Err(invalid());
        }
        Ok(page)
    }

    /// The providers this deployment serves grants for (REQ-GRANT-78), or `None` from a
    /// deployment that predates the read and answers its route 404, which says nothing of what
    /// it serves. The answer is read as text rather than into the generated enumeration, so a
    /// provider a later deployment serves and this client does not know is passed over
    /// (REQ-GRANT-75).
    pub fn served_providers(&self) -> Result<Option<Vec<MemberGrantProvider>>, MemberGrantFailure> {
        let endpoint = format!(
            "{}/v1/me/grant-providers",
            self.configuration.base_path.trim_end_matches('/')
        );
        let response = receive_raw(super::generated_api_request(
            &self.configuration,
            reqwest::Method::GET,
            &endpoint,
        ))?;
        if response.status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = expect_problem_status(response, StatusCode::OK, &PROBLEMS)?;
        validate_json_media_type(&response.headers)?;
        served(&response.body).map(Some)
    }

    /// One of the member's grants of either kind, by its ID (REQ-GRANT-58).
    pub fn get(&self, grant_id: &str) -> Result<MemberGrant, MemberGrantFailure> {
        let grant = apis::principals_api::get_grant(&self.configuration, grant_id)
            .map_err(classify_error)?;
        if grant.id != grant_id || !valid_grant(&grant) {
            return Err(invalid());
        }
        Ok(grant)
    }

    /// The slug of one of the member's organizations, by its ID.
    pub fn organization_slug(&self, organization_id: &str) -> Result<String, MemberGrantFailure> {
        let organization = organization(
            &self.configuration,
            organization_id,
            PROBLEMS.iter().chain(&ORGANIZATION_PROBLEMS),
        )?;
        if organization.id != organization_id || organization.slug.is_empty() {
            return Err(invalid());
        }
        Ok(organization.slug)
    }

    /// The ID of one of the member's organizations, by its exact slug.
    pub fn organization_id(&self, slug: &str) -> Result<String, MemberGrantFailure> {
        organization_id(
            &self.configuration,
            slug,
            PROBLEMS.iter().chain(&ORGANIZATION_PROBLEMS),
        )
    }

    /// One page of the organizations the member is an active member of, each as its ID and
    /// slug, and the cursor of the next page (`GET /v1/me/memberships`). Cloud names an
    /// organization's slug only while the member, the membership and the organization are all
    /// active; the other memberships are left out.
    pub fn active_organizations(
        &self,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<OrganizationPage, MemberGrantFailure> {
        use models::current_principal_membership_entry::{OrganizationState, State};
        let page = apis::principals_api::list_current_principal_memberships(
            &self.configuration,
            limit,
            cursor,
        )
        .map_err(classify_error)?;
        let mut organizations = Vec::new();
        for membership in page.items {
            if !um_support::valid_typed_id(&membership.organization_id, "org_") {
                return Err(invalid());
            }
            if membership.state == State::Active
                && membership.organization_state == OrganizationState::Active
            {
                let slug = membership
                    .organization_slug
                    .filter(|slug| !slug.is_empty())
                    .ok_or_else(invalid)?;
                organizations.push((membership.organization_id, slug));
            }
        }
        Ok((organizations, page.next_cursor))
    }

    /// Authorizes one runner for the grant (REQ-GRANT-59).
    pub fn authorize(
        &self,
        grant_id: &str,
        runner_id: &str,
        key: &str,
    ) -> Result<MemberGrantAuthorization, MemberGrantFailure> {
        let body = serialize_body(&models::AuthorizeGrantRunnerRequest::new(
            runner_id.to_owned(),
        ))?;
        let authorization: MemberGrantAuthorization = self.post(
            &self.endpoint(&[grant_id, "authorizations"]),
            key,
            &body,
            StatusCode::CREATED,
            &PROBLEMS,
        )?;
        let prefix = authorization_prefix(grant_id).ok_or_else(invalid)?;
        if authorization.runner_id != runner_id || !valid_authorization(&authorization, prefix) {
            return Err(invalid());
        }
        Ok(authorization)
    }

    /// Ends one authorization of the grant (REQ-GRANT-60).
    pub fn deauthorize(
        &self,
        grant_id: &str,
        authorization_id: &str,
        key: &str,
    ) -> Result<(), MemberGrantFailure> {
        confirmed_delete(
            &self.configuration,
            &self.endpoint(&[grant_id, "authorizations", authorization_id]),
            key,
            &PROBLEMS,
        )
    }

    /// Revokes the grant (REQ-GRANT-62).
    pub fn revoke(&self, grant_id: &str, key: &str) -> Result<(), MemberGrantFailure> {
        confirmed_delete(
            &self.configuration,
            &self.endpoint(&[grant_id]),
            key,
            &PROBLEMS,
        )
    }

    /// Imports a coding agent's login as a native grant of `provider` at generation 1 for the
    /// organization `organization` names by ID or slug (REQ-GRANT-61). An OAuth provider's
    /// grant is made by its consent, so one is refused here before anything is sent.
    pub fn import(
        &self,
        organization: &str,
        provider: MemberGrantProvider,
        material: &[u8],
        key: &str,
    ) -> Result<MemberGrant, MemberGrantFailure> {
        use models::import_grant_request::Provider;
        let wire = match provider {
            MemberGrantProvider::GrantProviderClaudeCode => {
                Provider::ImportGrantRequestProviderClaudeCode
            }
            MemberGrantProvider::GrantProviderCodex => Provider::ImportGrantRequestProviderCodex,
            _ => return Err(MemberGrantFailure::InvalidInput),
        };
        let material = encode_material(material);
        let mut model = models::ImportGrantRequest::new(
            organization.to_owned(),
            wire,
            RECORD_SCHEMA_VERSION,
            material.to_string(),
        );
        let body = serialize_body(&model);
        model.material.zeroize();
        let endpoint = format!(
            "{}/v1/me/grant-imports",
            self.configuration.base_path.trim_end_matches('/')
        );
        let grant: MemberGrant = self.post(
            &endpoint,
            key,
            &body?,
            StatusCode::CREATED,
            PROBLEMS.iter().chain(&ORGANIZATION_PROBLEMS),
        )?;
        // A slug is not checked: the grant names its organization by ID.
        let named = !um_support::valid_typed_id(organization, "org_")
            || grant.organization_id.as_deref() == Some(organization);
        if grant.provider != provider || !named || !valid_grant(&grant) {
            return Err(invalid());
        }
        Ok(grant)
    }

    /// Stores new material as the native grant's next generation (REQ-GRANT-61, REQ-GRANT-16):
    /// the grant keeps its ID, organization, provider and authorizations.
    pub fn replace(
        &self,
        grant_id: &str,
        material: &[u8],
        key: &str,
    ) -> Result<MemberGrant, MemberGrantFailure> {
        let material = encode_material(material);
        let mut model =
            models::ReplaceGrantMaterialRequest::new(RECORD_SCHEMA_VERSION, material.to_string());
        let body = serialize_body(&model);
        model.material.zeroize();
        let grant: MemberGrant = self.post(
            &self.endpoint(&[grant_id, "generations"]),
            key,
            &body?,
            StatusCode::OK,
            &PROBLEMS,
        )?;
        if grant.id != grant_id || !valid_grant(&grant) {
            return Err(invalid());
        }
        Ok(grant)
    }

    // The contract's answer to each mutation echoes this request's key; the generated client
    // drops response headers, so these requests go out under its configured client instead.
    // The copy of the body the client sends is its own and is not zeroized.
    fn post<'a, T: serde::de::DeserializeOwned>(
        &self,
        endpoint: &str,
        key: &str,
        body: &[u8],
        expected: StatusCode,
        problems: impl IntoIterator<Item = &'a ProblemRow<MemberGrantFailure>>,
    ) -> Result<T, MemberGrantFailure> {
        let response = receive_raw(
            mutation_request(&self.configuration, reqwest::Method::POST, endpoint, key)
                .header(reqwest::header::CONTENT_TYPE, problem::JSON_MEDIA_TYPE)
                .body(body.to_vec()),
        )?;
        let response = expect_problem_status(response, expected, problems)?;
        validate_mutation_key(&response.headers, key)?;
        validate_json_media_type(&response.headers)?;
        serde_json::from_slice(&response.body).map_err(|_| invalid())
    }
}

impl Drop for MemberGrantApi {
    fn drop(&mut self) {
        zeroize_generated_bearer_access_token(&mut self.configuration);
    }
}

/// The prefix of an authorization of the grant `grant_id` names: `pga_` under an OAuth grant,
/// `aga_` under a native one.
fn authorization_prefix(grant_id: &str) -> Option<&'static str> {
    if um_support::valid_typed_id(grant_id, "pgr_") {
        Some("pga_")
    } else if um_support::valid_typed_id(grant_id, "agl_") {
        Some("aga_")
    } else {
        None
    }
}

/// One entry of the served-providers answer, as the text it carries.
#[derive(serde::Deserialize)]
struct ServedProvider {
    provider: String,
    kind: String,
}

#[derive(serde::Deserialize)]
struct ServedProviders {
    items: Vec<ServedProvider>,
}

/// The providers a served-providers answer names that this client knows, in its order. Each
/// must be named once and with its own kind; a name this client does not know is ignored,
/// whatever its kind.
fn served(body: &[u8]) -> Result<Vec<MemberGrantProvider>, MemberGrantFailure> {
    let list: ServedProviders = serde_json::from_slice(body).map_err(|_| invalid())?;
    let mut served = Vec::with_capacity(list.items.len());
    for item in list.items {
        let Ok(provider) =
            serde_json::from_value::<MemberGrantProvider>(serde_json::Value::String(item.provider))
        else {
            continue;
        };
        if served.contains(&provider) || item.kind != provider.kind().as_str() {
            return Err(invalid());
        }
        served.push(provider);
    }
    Ok(served)
}

/// A grant whose ID, provider and kind agree, with the members its kind requires.
fn valid_grant(value: &MemberGrant) -> bool {
    let kind_members = match value.kind {
        MemberGrantKind::GrantKindOAuth => value
            .identity_id
            .as_deref()
            .is_some_and(|id| um_support::valid_typed_id(id, "idn_")),
        MemberGrantKind::GrantKindNative => {
            value
                .organization_id
                .as_deref()
                .is_some_and(|id| um_support::valid_typed_id(id, "org_"))
                && value.state != MemberGrantState::GrantStateReconnectRequired
                && value
                    .ended_authorizations
                    .iter()
                    .flatten()
                    .all(valid_ended_authorization)
        }
    };
    let authorization_kind = match value.kind {
        MemberGrantKind::GrantKindOAuth => "pga_",
        MemberGrantKind::GrantKindNative => "aga_",
    };
    authorization_prefix(&value.id) == Some(authorization_kind)
        && value.provider.kind() == value.kind
        && value.generation >= 1
        && !value.created_at.is_empty()
        && kind_members
        && value
            .authorizations
            .iter()
            .all(|authorization| valid_authorization(authorization, authorization_kind))
        && [
            &value.last_used_at,
            &value.access_expires_at,
            &value.refresh_expires_at,
            &value.imported_at,
            &value.revoked_at,
        ]
        .iter()
        .all(|time| time.as_deref().is_none_or(|time| !time.is_empty()))
}

/// An authorization checked as an agent login's is (REQ-GRANT-13), naming its organization.
fn valid_authorization(value: &MemberGrantAuthorization, prefix: &str) -> bool {
    valid_authorization_record(
        prefix,
        &value.id,
        &value.runner_id,
        &value.created_at,
        value.delivery.as_deref(),
        value.report.as_deref(),
    ) && um_support::valid_typed_id(&value.organization_id, "org_")
}

#[derive(Debug)]
pub enum MemberGrantApiError {
    InvalidEndpoint,
    BuildClient(reqwest::Error),
}

impl fmt::Display for MemberGrantApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEndpoint => write!(
                f,
                "invalid grant API endpoint; check the deployment URL and HTTP policy"
            ),
            Self::BuildClient(error) => write!(f, "prepare grant networking: {error}"),
        }
    }
}

impl std::error::Error for MemberGrantApiError {}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "grant client unit tests use Rust test assertions"
)]
mod tests {
    use super::*;
    use crate::resource::{classify_problem, problem_fixture};

    // REQ-GRANT-65
    #[test]
    fn each_grant_refusal_maps_to_one_outcome_and_a_generic_not_found_only_for_an_organization() {
        use MemberGrantFailure as F;
        for (status, slug, problems, want) in [
            (
                503,
                "provider-unavailable",
                &PROBLEMS[..],
                F::ProviderUnavailable,
            ),
            (404, "grant-not-found", &PROBLEMS[..], F::GrantNotFound),
            (
                400,
                "grant-material-invalid",
                &PROBLEMS[..],
                F::MaterialInvalid,
            ),
            (
                409,
                "quantity-limit-reached",
                &PROBLEMS[..],
                F::LimitReached,
            ),
            (
                404,
                "grant-runner-not-found",
                &PROBLEMS[..],
                F::RunnerNotFound,
            ),
            (
                409,
                "grant-runner-conflict",
                &PROBLEMS[..],
                F::RunnerHoldsLogin,
            ),
            (
                409,
                "grant-already-authorized",
                &PROBLEMS[..],
                F::AlreadyAuthorized,
            ),
            (
                404,
                "grant-authorization-not-found",
                &PROBLEMS[..],
                F::AuthorizationNotFound,
            ),
            (
                409,
                "idempotency-conflict",
                &PROBLEMS[..],
                F::IdempotencyConflict,
            ),
            (409, "grant-kind-mismatch", &PROBLEMS[..], F::InvalidInput),
            (404, "not-found", &PROBLEMS[..], invalid()),
            (
                404,
                "not-found",
                &ORGANIZATION_PROBLEMS[..],
                F::OrganizationNotFound,
            ),
            (409, "grant-not-found", &PROBLEMS[..], invalid()),
        ] {
            let status = StatusCode::from_u16(status).unwrap();
            assert_eq!(
                classify_problem(
                    status,
                    &problem_fixture(status.as_u16(), slug),
                    None,
                    problems
                ),
                want,
                "{status} {slug}"
            );
        }
    }

    fn grant(members: serde_json::Value) -> MemberGrant {
        let mut grant = serde_json::json!({
            "id": "pgr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "kind": "oauth",
            "provider": "github",
            "state": "active",
            "generation": 1,
            "createdAt": "2026-11-01T09:00:00Z",
            "authorizations": [],
            "identityId": "idn_01k0z6r1w8f4jy2m7q9v3x5abc"
        });
        for (name, value) in members.as_object().unwrap() {
            grant[name] = value.clone();
        }
        serde_json::from_value(grant).unwrap()
    }

    fn native(members: serde_json::Value) -> MemberGrant {
        let mut base = serde_json::json!({
            "id": "agl_01k0z6r1w8f4jy2m7q9v3x5abc",
            "kind": "native",
            "provider": "codex",
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "identityId": null
        });
        for (name, value) in members.as_object().unwrap() {
            base[name] = value.clone();
        }
        grant(base)
    }

    fn ended(removal: serde_json::Value, run_id: &str) -> serde_json::Value {
        serde_json::json!({"endedAuthorizations": [{
            "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "createdAt": "2026-11-01T09:05:00Z",
            "endedAt": "2026-11-02T09:00:00Z",
            "removal": removal,
            "stop": {
                "runId": run_id,
                "cancellationRequestId": "cmd_01k0z6r1w8f4jy2m7q9v3x5abc",
                "requestedAt": "2026-11-02T09:00:01Z"
            }
        }]})
    }

    // REQ-GRANT-58
    #[test]
    fn a_grants_id_provider_and_kind_agree_and_carry_what_its_kind_requires() {
        const RUN: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
        let pending = serde_json::json!({"state": "pending"});
        let confirmed =
            serde_json::json!({"state": "confirmed", "confirmedAt": "2026-11-02T09:01:00Z"});
        for (grant, valid) in [
            (grant(serde_json::json!({})), true),
            (grant(serde_json::json!({"provider": "codex"})), false),
            (grant(serde_json::json!({"kind": "native"})), false),
            (grant(serde_json::json!({"identityId": null})), false),
            (grant(serde_json::json!({"generation": 0})), false),
            (
                grant(serde_json::json!({"authorizations": [{
                    "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "createdAt": "2026-11-01T09:05:00Z"
                }]})),
                false,
            ),
            (native(serde_json::json!({})), true),
            (native(serde_json::json!({"organizationId": null})), false),
            (
                native(serde_json::json!({"state": "reconnect_required"})),
                false,
            ),
            (native(ended(pending.clone(), RUN)), true),
            (native(ended(confirmed, RUN)), true),
            (
                native(ended(serde_json::json!({"state": "confirmed"}), RUN)),
                false,
            ),
            (
                native(ended(pending, "cmd_01k0z6r1w8f4jy2m7q9v3x5abc")),
                false,
            ),
        ] {
            assert_eq!(valid_grant(&grant), valid, "{grant:?}");
        }
    }

    // REQ-GRANT-64
    // REQ-GRANT-13
    #[test]
    fn a_native_grants_authorizations_are_checked_as_an_agent_logins_are() {
        let authorization = serde_json::json!({
            "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "createdAt": "2026-11-01T09:05:00Z",
            "delivery": {"generation": 1, "deliveredAt": "2026-11-01T09:06:00Z"},
            "report": {"state": "ready", "generation": 1, "lastUse": "accepted", "reportedAt": "2026-11-01T09:07:00Z"}
        });
        let mut ended = ended(
            serde_json::json!({"state": "pending"}),
            "run_01k0z6r1w8f4jy2m7q9v3x5abc",
        )["endedAuthorizations"][0]
            .clone();
        ended["delivery"] = authorization["delivery"].clone();
        ended["report"] = authorization["report"].clone();
        let malformed: [(&str, serde_json::Value); 5] = [
            ("/createdAt", serde_json::json!("")),
            ("/delivery/generation", serde_json::json!(0)),
            ("/delivery/deliveredAt", serde_json::json!("")),
            ("/report/generation", serde_json::json!(0)),
            ("/report/reportedAt", serde_json::json!("")),
        ];
        assert!(valid_grant(&native(
            serde_json::json!({"authorizations": [authorization.clone()]})
        )));
        assert!(valid_grant(&native(
            serde_json::json!({"endedAuthorizations": [ended.clone()]})
        )));
        for (pointer, value) in &malformed {
            let mut current = authorization.clone();
            *current.pointer_mut(pointer).unwrap() = value.clone();
            assert!(
                !valid_grant(&native(serde_json::json!({"authorizations": [current]}))),
                "authorization {pointer}"
            );
        }
        for (pointer, value) in malformed
            .iter()
            .chain([&("/stop/requestedAt", serde_json::json!(""))])
        {
            let mut former = ended.clone();
            *former.pointer_mut(pointer).unwrap() = value.clone();
            assert!(
                !valid_grant(&native(
                    serde_json::json!({"endedAuthorizations": [former]})
                )),
                "ended authorization {pointer}"
            );
        }
    }

    // REQ-GRANT-75
    #[test]
    fn a_served_provider_this_client_does_not_know_is_ignored() {
        use MemberGrantProvider as P;
        // A later deployment may serve a provider, of a kind, this client was built before.
        let body = serde_json::json!({"items": [
            {"provider": "github", "kind": "oauth"},
            {"provider": "bitbucket", "kind": "oauth"},
            {"provider": "cursor", "kind": "hosted"},
            {"provider": "codex", "kind": "native"},
        ]});
        assert_eq!(
            served(body.to_string().as_bytes()),
            Ok(vec![P::GrantProviderGithub, P::GrantProviderCodex])
        );
        assert_eq!(served(br#"{"items": []}"#), Ok(Vec::new()));

        // A provider it does know is named once and with its own kind.
        for body in [
            serde_json::json!({"items": [
                {"provider": "codex", "kind": "native"},
                {"provider": "codex", "kind": "native"},
            ]}),
            serde_json::json!({"items": [{"provider": "codex", "kind": "oauth"}]}),
            serde_json::json!({"items": [{"provider": "codex"}]}),
            serde_json::json!({}),
        ] {
            assert_eq!(
                served(body.to_string().as_bytes()),
                Err(invalid()),
                "{body}"
            );
        }
    }
}
