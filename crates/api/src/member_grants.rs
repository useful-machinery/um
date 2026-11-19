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
    ProblemFailure, ProblemRow, RECORD_SCHEMA_VERSION, ResourceFailure, classify_problem,
    classify_problem_error, confirmed_delete, encode_material, expect_problem_status,
    mutation_request, organization, organization_id, receive_raw, resource_configuration,
    serialize_body, validate_json_media_type, validate_mutation_key,
};
use super::{HttpTransportPolicy, UnreachableCategory, problem};

pub type MemberGrant = models::Grant;
pub type MemberGrantList = models::GrantList;
pub type MemberGrantAuthorization = models::GrantAuthorization;
pub type MemberGrantKind = models::grant::Kind;
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

/// A provider this client has a built-in name for: the three OAuth providers it signs in to
/// without asking the deployment, and the coding agents, the only native providers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemberGrantProvider {
    GrantProviderGithub,
    GrantProviderGitlabCom,
    GrantProviderLinear,
    GrantProviderClaudeCode,
    GrantProviderCodex,
}

impl MemberGrantProvider {
    const ALL: [Self; 5] = [
        Self::GrantProviderGithub,
        Self::GrantProviderGitlabCom,
        Self::GrantProviderLinear,
        Self::GrantProviderClaudeCode,
        Self::GrantProviderCodex,
    ];

    /// The provider a wire name names, or `None` for a name this client does not know.
    pub fn from_wire(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|provider| provider.as_str() == name)
    }

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
    // The refusals only the integration-token routes answer (REQ-INTEGRATION-2 to
    // REQ-INTEGRATION-6), which a grant's name would misreport.
    IntegrationTokenInvalid,
    IntegrationTokenExists,
    IntegrationTokenNotFound,
    IntegrationTokenRunnerNotFound,
    IntegrationTokenAlreadyAuthorized,
    IntegrationTokenAuthorizationNotFound,
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

pub(crate) const fn invalid() -> MemberGrantFailure {
    MemberGrantFailure::InvalidResponse {
        credential_rejected: false,
    }
}

/// The problems every operation on the grants and the integration tokens answers, by status and
/// type. A generic `not-found` is outside the contract here; only an operation that names an
/// organization answers it.
pub(crate) static SHARED_PROBLEMS: [ProblemRow<MemberGrantFailure>; 6] = [
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
];

/// The problems only the operations on the grants resource answer.
static GRANT_PROBLEMS: [ProblemRow<MemberGrantFailure>; 8] = [
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

/// The problems every operation on the grants resource answers.
fn problems() -> impl Iterator<Item = &'static ProblemRow<MemberGrantFailure>> {
    SHARED_PROBLEMS.iter().chain(&GRANT_PROBLEMS)
}

/// The problem only the operations that name an organization, its lookup, an import and the
/// organization's integration tokens, answer.
pub(crate) static ORGANIZATION_PROBLEMS: [ProblemRow<MemberGrantFailure>; 1] = [(
    StatusCode::NOT_FOUND,
    problem::NOT_FOUND,
    MemberGrantFailure::OrganizationNotFound,
)];

fn classify_error<E>(error: apis::Error<E>) -> MemberGrantFailure {
    classify_problem_error(error, problems())
}

/// The largest page the list serves.
pub const MEMBER_GRANT_PAGE: i32 = 200;

/// The provider vocabulary this release decodes (REQ-GRANT-95, REQ-GRANT-96): an answer may name
/// a grant of any provider of the deployment's table, which this client shows by its id. It is
/// sent on every operation that takes it, the grant list, a grant, its uses, a consent begun by
/// provider or from an identity, a consent session and the identity list; a deployment that
/// predates the parameter ignores it and names only the built-in providers.
pub(crate) const PROVIDER_VOCABULARY: i32 = 2;

/// Whether `name` has the shape of a provider id: a lowercase letter followed by at most 62
/// lowercase letters, digits, dots and hyphens, the shape the deployment's table admits
/// (REQ-PROVIDER-1). Every OAuth provider is named so, and `codex`, but not `claude_code`.
pub fn valid_provider_id(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && name.len() <= 63
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'.' || byte == b'-'
        })
}

/// Whether an answer may name `name` as an OAuth grant's provider (REQ-GRANT-96): any provider
/// id but a coding agent's, since the coding agents are the native providers and no others.
pub(crate) fn oauth_provider(name: &str) -> bool {
    valid_provider_id(name)
        && MemberGrantProvider::from_wire(name)
            .is_none_or(|provider| provider.kind() == MemberGrantKind::GrantKindOAuth)
}

/// The member's active organizations on one page, each as its ID and slug, and the cursor of
/// the next page.
type OrganizationPage = (Vec<(String, String)>, Option<String>);

/// One of the member's active memberships: its organization's ID and slug, and whether the
/// membership's role is `owner`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveMembership {
    pub organization_id: String,
    pub slug: String,
    pub owner: bool,
}

pub struct MemberGrantApi {
    pub(crate) configuration: apis::configuration::Configuration,
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

    /// The URL of the API path `segments` name below `/v1`, each segment encoded.
    pub(crate) fn route<'a>(&self, segments: impl IntoIterator<Item = &'a str>) -> String {
        let mut endpoint = format!("{}/v1", self.configuration.base_path.trim_end_matches('/'));
        for segment in segments {
            endpoint.push('/');
            endpoint.push_str(&apis::urlencode(segment));
        }
        endpoint
    }

    fn endpoint(&self, segments: &[&str]) -> String {
        self.route(["me", "grants"].into_iter().chain(segments.iter().copied()))
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
        let page = apis::principals_api::list_grants(
            &self.configuration,
            Some(filter),
            limit,
            cursor,
            Some(PROVIDER_VOCABULARY),
        )
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

    /// The providers this deployment serves grants for that this release can use, in its order
    /// (REQ-GRANT-78), or `None` from a deployment that predates the read and answers its route
    /// 404, which says nothing of what it serves. The answer is read as text rather than into the
    /// generated models, so a provider a later deployment serves that this release cannot use is
    /// passed over (REQ-GRANT-75).
    pub fn served_providers(&self) -> Result<Option<Vec<ServedGrantProvider>>, MemberGrantFailure> {
        self.served_answer()?.map(|body| served(&body)).transpose()
    }

    /// The body of the served-providers answer, or `None` where the deployment predates the read.
    pub(crate) fn served_answer(&self) -> Result<Option<Vec<u8>>, MemberGrantFailure> {
        let endpoint = self.route(["me", "grant-providers"]);
        let response = receive_raw(super::generated_api_request(
            &self.configuration,
            reqwest::Method::GET,
            &endpoint,
        ))?;
        if response.status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = expect_problem_status(response, StatusCode::OK, problems())?;
        validate_json_media_type(&response.headers)?;
        Ok(Some(response.body))
    }

    /// One of the member's grants of either kind, by its ID (REQ-GRANT-58).
    pub fn get(&self, grant_id: &str) -> Result<MemberGrant, MemberGrantFailure> {
        let grant = apis::principals_api::get_grant(
            &self.configuration,
            grant_id,
            Some(PROVIDER_VOCABULARY),
        )
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
            problems().chain(&ORGANIZATION_PROBLEMS),
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
            problems().chain(&ORGANIZATION_PROBLEMS),
        )
    }

    /// One page of the organizations the member is an active member of, each as its ID and
    /// slug, and the cursor of the next page (`GET /v1/me/memberships`), as
    /// [`MemberGrantApi::active_memberships`] reads them.
    pub fn active_organizations(
        &self,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<OrganizationPage, MemberGrantFailure> {
        let (memberships, next_cursor) = self.active_memberships(limit, cursor)?;
        let organizations = memberships
            .into_iter()
            .map(|membership| (membership.organization_id, membership.slug))
            .collect();
        Ok((organizations, next_cursor))
    }

    /// One page of the member's active memberships, each with its organization's ID and slug
    /// and its role, and the cursor of the next page (`GET /v1/me/memberships`). Cloud names an
    /// organization's slug only while the member, the membership and the organization are all
    /// active; the other memberships are left out.
    pub fn active_memberships(
        &self,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<(Vec<ActiveMembership>, Option<String>), MemberGrantFailure> {
        use models::current_principal_membership_entry::{OrganizationState, Role, State};
        let page = apis::principals_api::list_current_principal_memberships(
            &self.configuration,
            limit,
            cursor,
        )
        .map_err(classify_error)?;
        let mut memberships = Vec::new();
        for membership in page.items {
            if !useful_support::valid_typed_id(&membership.organization_id, "org_") {
                return Err(invalid());
            }
            if membership.state == State::Active
                && membership.organization_state == OrganizationState::Active
            {
                let slug = membership
                    .organization_slug
                    .filter(|slug| !slug.is_empty())
                    .ok_or_else(invalid)?;
                memberships.push(ActiveMembership {
                    organization_id: membership.organization_id,
                    slug,
                    owner: membership.role == Role::Owner,
                });
            }
        }
        Ok((memberships, page.next_cursor))
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
            &[StatusCode::CREATED],
            problems(),
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
            problems(),
        )
    }

    /// Revokes the grant (REQ-GRANT-62).
    pub fn revoke(&self, grant_id: &str, key: &str) -> Result<(), MemberGrantFailure> {
        confirmed_delete(
            &self.configuration,
            &self.endpoint(&[grant_id]),
            key,
            problems(),
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
            &[StatusCode::CREATED],
            problems().chain(&ORGANIZATION_PROBLEMS),
        )?;
        // A slug is not checked: the grant names its organization by ID.
        let named = !useful_support::valid_typed_id(organization, "org_")
            || grant.organization_id.as_deref() == Some(organization);
        if grant.provider != provider.as_str() || !named || !valid_grant(&grant) {
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
            &[StatusCode::OK],
            problems(),
        )?;
        if grant.id != grant_id || !valid_grant(&grant) {
            return Err(invalid());
        }
        Ok(grant)
    }

    // The contract's answer to each mutation echoes this request's key; the generated client
    // drops response headers, so these requests go out under its configured client instead.
    // The copy of the body the client sends is its own and is not zeroized. `expected` holds
    // each status the contract answers the mutation with.
    pub(crate) fn post<'a, T: serde::de::DeserializeOwned>(
        &self,
        endpoint: &str,
        key: &str,
        body: &[u8],
        expected: &[StatusCode],
        problems: impl IntoIterator<Item = &'a ProblemRow<MemberGrantFailure>>,
    ) -> Result<T, MemberGrantFailure> {
        let response = receive_raw(
            mutation_request(&self.configuration, reqwest::Method::POST, endpoint, key)
                .header(reqwest::header::CONTENT_TYPE, problem::JSON_MEDIA_TYPE)
                .body(body.to_vec()),
        )?;
        if !expected.contains(&response.status) {
            return Err(classify_problem(
                response.status,
                &response.body,
                response.retry_after,
                problems,
            ));
        }
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
    if useful_support::valid_typed_id(grant_id, "pgr_") {
        Some("pga_")
    } else if useful_support::valid_typed_id(grant_id, "agl_") {
        Some("aga_")
    } else {
        None
    }
}

/// A provider a deployment serves grants for that this release can use (REQ-GRANT-78): an OAuth
/// provider named by a provider id, or a coding agent, with the name the deployment gives it for
/// people when that name is fit to print.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServedGrantProvider {
    /// The provider's id, as a grant and a consent name it.
    pub name: String,
    pub kind: MemberGrantKind,
    /// The deployment's name for the provider, kept only when it meets the rule the
    /// deployment's table holds it to.
    pub display_name: Option<String>,
}

/// One entry of the served-providers answer, as the text it carries. The display name is read as
/// any value, since one that is not a printable string is as if absent.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ServedEntry {
    pub(crate) provider: String,
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) display_name: serde_json::Value,
}

#[derive(serde::Deserialize)]
pub(crate) struct ServedEntries {
    pub(crate) items: Vec<ServedEntry>,
}

/// The longest display name the deployment's table admits, in bytes.
const DISPLAY_NAME_BYTES: usize = 64;

/// The display name of a served entry when it meets the table's rule: 1 to 64 bytes, with no
/// control character and no surrounding whitespace. It is another party's text bound for a
/// terminal, so anything else is dropped rather than refused.
pub(crate) fn display_name(value: serde_json::Value) -> Option<String> {
    let serde_json::Value::String(name) = value else {
        return None;
    };
    (!name.is_empty()
        && name.len() <= DISPLAY_NAME_BYTES
        && name.trim() == name
        && !name.chars().any(char::is_control))
    .then_some(name)
}

/// The providers a served-providers answer names that this release can use, in its order: each
/// `oauth` entry named by a provider id, and `claude_code` and `codex` as `native`. An entry of
/// another kind, another `native` name or an `oauth` name outside the id shape is passed over.
/// A provider kept is named once, and a built-in one only with its own kind.
pub(crate) fn served(body: &[u8]) -> Result<Vec<ServedGrantProvider>, MemberGrantFailure> {
    let list: ServedEntries = serde_json::from_slice(body).map_err(|_| invalid())?;
    let mut served: Vec<ServedGrantProvider> = Vec::with_capacity(list.items.len());
    for item in list.items {
        let kind = match item.kind.as_str() {
            "oauth" => MemberGrantKind::GrantKindOAuth,
            "native" => MemberGrantKind::GrantKindNative,
            _ => continue,
        };
        let built_in = MemberGrantProvider::from_wire(&item.provider);
        if built_in.is_some_and(|provider| provider.kind() != kind) {
            return Err(invalid());
        }
        let usable = match kind {
            MemberGrantKind::GrantKindOAuth => oauth_provider(&item.provider),
            MemberGrantKind::GrantKindNative => built_in.is_some(),
        };
        if !usable {
            continue;
        }
        if served.iter().any(|kept| kept.name == item.provider) {
            return Err(invalid());
        }
        served.push(ServedGrantProvider {
            name: item.provider,
            kind,
            display_name: display_name(item.display_name),
        });
    }
    Ok(served)
}

/// A grant whose ID, provider and kind agree, with the members its kind requires: a native
/// grant's provider is a coding agent, and an OAuth grant's any other provider id.
fn valid_grant(value: &MemberGrant) -> bool {
    let kind_members = match value.kind {
        MemberGrantKind::GrantKindOAuth => value
            .identity_id
            .as_deref()
            .is_some_and(|id| useful_support::valid_typed_id(id, "idn_")),
        MemberGrantKind::GrantKindNative => {
            value
                .organization_id
                .as_deref()
                .is_some_and(|id| useful_support::valid_typed_id(id, "org_"))
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
    let provider = match value.kind {
        MemberGrantKind::GrantKindOAuth => oauth_provider(&value.provider),
        MemberGrantKind::GrantKindNative => MemberGrantProvider::from_wire(&value.provider)
            .is_some_and(|provider| provider.kind() == MemberGrantKind::GrantKindNative),
    };
    authorization_prefix(&value.id) == Some(authorization_kind)
        && provider
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
    ) && useful_support::valid_typed_id(&value.organization_id, "org_")
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
        let grant: Vec<_> = problems().cloned().collect();
        for (status, slug, problems, want) in [
            (
                503,
                "provider-unavailable",
                &grant[..],
                F::ProviderUnavailable,
            ),
            (404, "grant-not-found", &grant[..], F::GrantNotFound),
            (
                400,
                "grant-material-invalid",
                &grant[..],
                F::MaterialInvalid,
            ),
            (409, "quantity-limit-reached", &grant[..], F::LimitReached),
            (404, "grant-runner-not-found", &grant[..], F::RunnerNotFound),
            (
                409,
                "grant-runner-conflict",
                &grant[..],
                F::RunnerHoldsLogin,
            ),
            (
                409,
                "grant-already-authorized",
                &grant[..],
                F::AlreadyAuthorized,
            ),
            (
                404,
                "grant-authorization-not-found",
                &grant[..],
                F::AuthorizationNotFound,
            ),
            (
                409,
                "idempotency-conflict",
                &grant[..],
                F::IdempotencyConflict,
            ),
            (409, "grant-kind-mismatch", &grant[..], F::InvalidInput),
            (404, "not-found", &grant[..], invalid()),
            (
                404,
                "not-found",
                &ORGANIZATION_PROBLEMS[..],
                F::OrganizationNotFound,
            ),
            (409, "grant-not-found", &grant[..], invalid()),
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

    /// What `call` answers when the deployment answers `body`, and the request it sent.
    fn read(
        body: &serde_json::Value,
        call: impl FnOnce(&MemberGrantApi) -> Result<(), MemberGrantFailure>,
    ) -> (Result<(), MemberGrantFailure>, String) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let body = body.to_string();
        let server = useful_test_support::ScriptedHttpServer::respond(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .into_bytes(),
        );
        let api = MemberGrantApi::new(
            &server.api_url,
            "fixture-token",
            HttpTransportPolicy::AllowInsecureHttp,
        )
        .unwrap();
        let result = call(&api);
        (result, server.finish_one())
    }

    // REQ-GRANT-58
    // REQ-GRANT-96
    // REQ-COMPAT-20
    #[test]
    fn a_grant_of_any_well_formed_oauth_provider_is_read_and_one_of_another_kind_is_invalid() {
        const ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abc";
        const NATIVE_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
        let longest = format!("a{}", "b".repeat(62));
        let too_long = format!("a{}", "b".repeat(63));
        for (id, kind, provider, want) in [
            (ID, "oauth", "github", Ok(())),
            (ID, "oauth", "example", Ok(())),
            (ID, "oauth", "acme-2.example", Ok(())),
            (ID, "oauth", longest.as_str(), Ok(())),
            (ID, "oauth", too_long.as_str(), Err(invalid())),
            (ID, "oauth", "codex", Err(invalid())),
            (ID, "oauth", "claude_code", Err(invalid())),
            (ID, "oauth", "Example", Err(invalid())),
            (ID, "oauth", "exa_mple", Err(invalid())),
            (ID, "oauth", "1password", Err(invalid())),
            (ID, "oauth", "", Err(invalid())),
            (NATIVE_ID, "native", "codex", Ok(())),
            (NATIVE_ID, "native", "example", Err(invalid())),
            (NATIVE_ID, "native", "github", Err(invalid())),
        ] {
            let mut grant = serde_json::json!({
                "id": id,
                "kind": kind,
                "provider": provider,
                "state": "active",
                "generation": 1,
                "createdAt": "2026-11-01T09:00:00Z",
                "authorizations": [],
                "identityId": "idn_01k0z6r1w8f4jy2m7q9v3x5abc"
            });
            if kind == "native" {
                grant["identityId"] = serde_json::Value::Null;
                grant["organizationId"] = "org_01k0z6r1w8f4jy2m7q9v3x5abc".into();
            }
            let page = serde_json::json!({"items": [grant.clone()], "nextCursor": null});
            let (listed, request) = read(&page, |api| {
                api.list(None, Some(MEMBER_GRANT_PAGE), Some("grants-2"))
                    .map(|_| ())
            });
            assert_eq!(listed, want, "list {kind} {provider}");
            assert!(
                request.starts_with(
                    "GET /api/v1/me/grants?kind=all&limit=200&cursor=grants-2&vocabulary=2 "
                ),
                "{request}"
            );
            let (got, request) = read(&grant, |api| api.get(id).map(|_| ()));
            assert_eq!(got, want, "get {kind} {provider}");
            assert!(
                request.starts_with(&format!("GET /api/v1/me/grants/{id}?vocabulary=2 ")),
                "{request}"
            );
        }
    }

    // REQ-GRANT-64
    // REQ-GRANT-13
    // REQ-GRANT-27
    #[test]
    fn a_native_grants_authorizations_are_checked_as_an_agent_logins_are() {
        let authorization = serde_json::json!({
            "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "createdAt": "2026-11-01T09:05:00Z",
            "delivery": {"generation": 1, "deliveredAt": "2026-11-01T09:06:00Z"},
            "report": {"state": "ready", "generation": 1, "lastUse": "accepted", "lastUseAt": "2026-11-01T09:06:30Z", "reportedAt": "2026-11-01T09:07:00Z"}
        });
        let mut ended = ended(
            serde_json::json!({"state": "pending"}),
            "run_01k0z6r1w8f4jy2m7q9v3x5abc",
        )["endedAuthorizations"][0]
            .clone();
        ended["delivery"] = authorization["delivery"].clone();
        ended["report"] = authorization["report"].clone();
        let malformed: [(&str, serde_json::Value); 7] = [
            ("/createdAt", serde_json::json!("")),
            ("/delivery/generation", serde_json::json!(0)),
            ("/delivery/deliveredAt", serde_json::json!("")),
            ("/report/generation", serde_json::json!(0)),
            ("/report/reportedAt", serde_json::json!("")),
            ("/report/lastUseAt", serde_json::json!("")),
            ("/report/lastUse", serde_json::Value::Null),
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

    fn served_oauth(name: &str, display_name: Option<&str>) -> ServedGrantProvider {
        ServedGrantProvider {
            name: name.to_owned(),
            kind: MemberGrantKind::GrantKindOAuth,
            display_name: display_name.map(str::to_owned),
        }
    }

    // REQ-GRANT-78
    // REQ-GRANT-75
    #[test]
    fn a_served_provider_this_release_cannot_use_is_passed_over() {
        // A later deployment may serve a provider of a kind, or named in a shape, this release was
        // built before; it keeps every OAuth provider of the id shape and the two coding agents.
        let body = serde_json::json!({"items": [
            {"provider": "github", "kind": "oauth", "displayName": "GitHub"},
            {"provider": "example", "kind": "oauth", "displayName": "Example Cloud"},
            {"provider": "acme-2.example", "kind": "oauth"},
            {"provider": "Example", "kind": "oauth"},
            {"provider": "exa_mple", "kind": "oauth"},
            {"provider": "cursor", "kind": "hosted"},
            {"provider": "cursor", "kind": "hosted"},
            {"provider": "aider", "kind": "native"},
            {"provider": "codex", "kind": "native", "displayName": "Codex"},
        ]});
        assert_eq!(
            served(body.to_string().as_bytes()),
            Ok(vec![
                served_oauth("github", Some("GitHub")),
                served_oauth("example", Some("Example Cloud")),
                served_oauth("acme-2.example", None),
                ServedGrantProvider {
                    name: "codex".to_owned(),
                    kind: MemberGrantKind::GrantKindNative,
                    display_name: Some("Codex".to_owned()),
                },
            ])
        );
        assert_eq!(served(br#"{"items": []}"#), Ok(Vec::new()));

        // A display name is kept only as the deployment's table holds one, since it is printed;
        // any other is as if absent.
        let longest = "\u{e9}".repeat(32);
        let too_long = "\u{e9}".repeat(33);
        for (display_name, kept) in [
            (serde_json::json!("Example Cloud"), true),
            (serde_json::json!(longest), true),
            (serde_json::json!(too_long), false),
            (serde_json::json!(""), false),
            (serde_json::json!(" Example"), false),
            (serde_json::json!("Example\t"), false),
            (serde_json::json!("Exa\u{1b}[31mmple"), false),
            (serde_json::json!(42), false),
            (serde_json::Value::Null, false),
        ] {
            let body = serde_json::json!({"items": [
                {"provider": "example", "kind": "oauth", "displayName": display_name},
            ]});
            let want = served_oauth("example", display_name.as_str().filter(|_| kept));
            assert_eq!(
                served(body.to_string().as_bytes()),
                Ok(vec![want]),
                "{display_name}"
            );
        }

        // A provider it keeps is named once, and a built-in one with its own kind.
        for body in [
            serde_json::json!({"items": [
                {"provider": "example", "kind": "oauth"},
                {"provider": "example", "kind": "oauth", "displayName": "Example Cloud"},
            ]}),
            serde_json::json!({"items": [
                {"provider": "codex", "kind": "native"},
                {"provider": "codex", "kind": "native"},
            ]}),
            serde_json::json!({"items": [{"provider": "codex", "kind": "oauth"}]}),
            serde_json::json!({"items": [{"provider": "github", "kind": "native"}]}),
            serde_json::json!({"items": [{"provider": "codex"}]}),
            serde_json::json!({"items": [{"kind": "oauth"}]}),
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
