//! The integration tokens of the member and of an organization (REQ-INTEGRATION-2 to
//! REQ-INTEGRATION-6), and the token services the deployment serves (REQ-GRANT-78), as
//! operations of the member grant client, whose session and failure the grant commands hold.
//!
//! A record decodes leniently into the generated models, which name no token, and a generated
//! error is never formatted. The token travels only in a request body built here from the
//! caller's bytes, in storage zeroized once the body is serialized.
use reqwest::StatusCode;
use zeroize::Zeroize as _;

use super::generated::{apis, models};
use super::member_grants::{
    MemberGrantApi, MemberGrantFailure, MemberGrantProvider, ORGANIZATION_PROBLEMS,
    SHARED_PROBLEMS, ServedEntries, ServedGrantProvider, display_name, invalid, served,
    valid_provider_id,
};
use super::resource::{ProblemRow, classify_problem_error, confirmed_delete, serialize_body};

pub type IntegrationToken = models::IntegrationToken;
pub type IntegrationTokenList = models::IntegrationTokenList;
pub type IntegrationTokenAuthorization = models::IntegrationTokenAuthorization;
pub type IntegrationTokenSide = models::integration_token::Side;

/// Whose integration tokens an operation acts on: the member's own, or the organization's that
/// an ID or a slug names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationTokenOwner<'a> {
    Member,
    Organization(&'a str),
}

const TOKEN_INVALID: &str = "https://api.usefulmachinery.com/problems/integration-token-invalid";
const TOKEN_EXISTS: &str = "https://api.usefulmachinery.com/problems/integration-token-exists";
const TOKEN_NOT_FOUND: &str =
    "https://api.usefulmachinery.com/problems/integration-token-not-found";
const TOKEN_RUNNER_NOT_FOUND: &str =
    "https://api.usefulmachinery.com/problems/integration-token-runner-not-found";
const TOKEN_ALREADY_AUTHORIZED: &str =
    "https://api.usefulmachinery.com/problems/integration-token-already-authorized";
const TOKEN_AUTHORIZATION_NOT_FOUND: &str =
    "https://api.usefulmachinery.com/problems/integration-token-authorization-not-found";

/// The problems only the integration-token routes answer.
static TOKEN_PROBLEMS: [ProblemRow<MemberGrantFailure>; 6] = [
    (
        StatusCode::BAD_REQUEST,
        TOKEN_INVALID,
        MemberGrantFailure::IntegrationTokenInvalid,
    ),
    (
        StatusCode::CONFLICT,
        TOKEN_EXISTS,
        MemberGrantFailure::IntegrationTokenExists,
    ),
    (
        StatusCode::NOT_FOUND,
        TOKEN_NOT_FOUND,
        MemberGrantFailure::IntegrationTokenNotFound,
    ),
    (
        StatusCode::NOT_FOUND,
        TOKEN_RUNNER_NOT_FOUND,
        MemberGrantFailure::IntegrationTokenRunnerNotFound,
    ),
    (
        StatusCode::CONFLICT,
        TOKEN_ALREADY_AUTHORIZED,
        MemberGrantFailure::IntegrationTokenAlreadyAuthorized,
    ),
    (
        StatusCode::NOT_FOUND,
        TOKEN_AUTHORIZATION_NOT_FOUND,
        MemberGrantFailure::IntegrationTokenAuthorizationNotFound,
    ),
];

impl IntegrationTokenOwner<'_> {
    /// The side a record of this owner's token states.
    const fn side(self) -> IntegrationTokenSide {
        match self {
            Self::Member => IntegrationTokenSide::IntegrationTokenSideMember,
            Self::Organization(_) => IntegrationTokenSide::IntegrationTokenSideOrg,
        }
    }

    /// The problems an operation on this owner's tokens answers: a generic `not-found` only on
    /// an organization's route, where it names an organization the member cannot read.
    fn problems(self) -> impl Iterator<Item = &'static ProblemRow<MemberGrantFailure>> {
        let organization: &'static [ProblemRow<MemberGrantFailure>] = match self {
            Self::Member => &[],
            Self::Organization(_) => &ORGANIZATION_PROBLEMS,
        };
        SHARED_PROBLEMS
            .iter()
            .chain(&TOKEN_PROBLEMS)
            .chain(organization)
    }

    /// Whether `organization_id`, as a record names it, is this owner's: none for the member,
    /// and for an organization one, which is the organization asked for where it was named by
    /// ID. A slug is not checked: a record names its organization by ID.
    fn names(self, organization_id: Option<&str>) -> bool {
        match (self, organization_id) {
            (Self::Member, None) => true,
            (Self::Organization(reference), Some(id)) => {
                useful_support::valid_typed_id(id, "org_")
                    && (!useful_support::valid_typed_id(reference, "org_") || id == reference)
            }
            _ => false,
        }
    }
}

/// A token service a deployment serves (REQ-GRANT-78): a provider of kind `token` named by a
/// provider id that is no built-in provider's, with the name the deployment gives it for people
/// when that name is fit to print.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServedTokenService {
    /// The service's id, as a token and a workflow name it.
    pub name: String,
    /// The deployment's name for the service, kept only when it meets the rule the deployment's
    /// table holds it to.
    pub display_name: Option<String>,
}

/// One served-providers answer read both ways: the providers a grant is made for, exactly as
/// `served_providers` reads them, and the token services.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServedIntegrations {
    pub providers: Vec<ServedGrantProvider>,
    pub token_services: Vec<ServedTokenService>,
}

/// The token services a served-providers answer names, in its order. An entry of kind `token`
/// whose name is outside the provider-id shape, is a built-in provider's or is named again is
/// passed over, so this reading never refuses an answer `served` accepts.
pub(crate) fn served_token_services(
    body: &[u8],
) -> Result<Vec<ServedTokenService>, MemberGrantFailure> {
    let list: ServedEntries = serde_json::from_slice(body).map_err(|_| invalid())?;
    let mut services: Vec<ServedTokenService> = Vec::new();
    for item in list.items {
        if item.kind != "token"
            || !token_service_id(&item.provider)
            || services.iter().any(|kept| kept.name == item.provider)
        {
            continue;
        }
        services.push(ServedTokenService {
            name: item.provider,
            display_name: display_name(item.display_name),
        });
    }
    Ok(services)
}

/// Whether `name` can name a token service: a provider id that no built-in provider has.
fn token_service_id(name: &str) -> bool {
    valid_provider_id(name) && MemberGrantProvider::from_wire(name).is_none()
}

impl MemberGrantApi {
    /// The providers and the token services this deployment serves, from one read of
    /// `GET /v1/me/grant-providers` (REQ-GRANT-78), or `None` from a deployment that predates
    /// the read. A token service an answer also names as a provider is not one.
    pub fn served_integrations(&self) -> Result<Option<ServedIntegrations>, MemberGrantFailure> {
        let Some(body) = self.served_answer()? else {
            return Ok(None);
        };
        let providers = served(&body)?;
        let token_services = served_token_services(&body)?
            .into_iter()
            .filter(|service| !providers.iter().any(|kept| kept.name == service.name))
            .collect();
        Ok(Some(ServedIntegrations {
            providers,
            token_services,
        }))
    }

    /// The URL of the owner's tokens, or of what `segments` name below them.
    fn integration_token_route(
        &self,
        owner: IntegrationTokenOwner<'_>,
        segments: &[&str],
    ) -> String {
        let mut path = match owner {
            IntegrationTokenOwner::Member => vec!["me"],
            IntegrationTokenOwner::Organization(reference) => vec!["organizations", reference],
        };
        path.push("integration-tokens");
        path.extend_from_slice(segments);
        self.route(path)
    }

    /// One page of the owner's unrevoked tokens, oldest first (REQ-INTEGRATION-4).
    pub fn integration_tokens(
        &self,
        owner: IntegrationTokenOwner<'_>,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<IntegrationTokenList, MemberGrantFailure> {
        let page = match owner {
            IntegrationTokenOwner::Member => {
                apis::principals_api::list_integration_tokens(&self.configuration, limit, cursor)
                    .map_err(|error| classify_problem_error(error, owner.problems()))
            }
            IntegrationTokenOwner::Organization(reference) => {
                apis::organizations_api::list_organization_integration_tokens(
                    &self.configuration,
                    reference,
                    limit,
                    cursor,
                )
                .map_err(|error| classify_problem_error(error, owner.problems()))
            }
        }?;
        if !page.items.iter().all(|token| valid_token(token, owner)) {
            return Err(invalid());
        }
        Ok(page)
    }

    /// Stores the owner's token of the token service `provider` (REQ-INTEGRATION-2,
    /// REQ-INTEGRATION-3), as the next version of the one the owner holds when `replace` is
    /// set. `token` is read from the caller's zeroizing storage and is sent, never kept.
    pub fn store_integration_token(
        &self,
        owner: IntegrationTokenOwner<'_>,
        provider: &str,
        token: &[u8],
        replace: bool,
        key: &str,
    ) -> Result<IntegrationToken, MemberGrantFailure> {
        let token = std::str::from_utf8(token).map_err(|_| MemberGrantFailure::InvalidInput)?;
        let mut model =
            models::StoreIntegrationTokenRequest::new(provider.to_owned(), token.to_owned());
        model.replace = replace.then_some(true);
        let body = serialize_body(&model);
        model.token.zeroize();
        let record: IntegrationToken = self.post(
            &self.integration_token_route(owner, &[]),
            key,
            &body?,
            &[StatusCode::CREATED, StatusCode::OK],
            owner.problems(),
        )?;
        if record.provider != provider || !valid_token(&record, owner) {
            return Err(invalid());
        }
        Ok(record)
    }

    /// Authorizes one runner for one of the owner's tokens (REQ-INTEGRATION-5).
    pub fn authorize_integration_token(
        &self,
        owner: IntegrationTokenOwner<'_>,
        token_id: &str,
        runner_id: &str,
        key: &str,
    ) -> Result<IntegrationTokenAuthorization, MemberGrantFailure> {
        let body = serialize_body(&models::AuthorizeIntegrationTokenRunnerRequest::new(
            runner_id.to_owned(),
        ))?;
        let authorization: IntegrationTokenAuthorization = self.post(
            &self.integration_token_route(owner, &[token_id, "authorizations"]),
            key,
            &body,
            &[StatusCode::CREATED],
            owner.problems(),
        )?;
        let organization = match owner {
            IntegrationTokenOwner::Member => true,
            IntegrationTokenOwner::Organization(_) => {
                owner.names(Some(&authorization.organization_id))
            }
        };
        if authorization.runner_id != runner_id
            || !organization
            || !valid_authorization(&authorization)
        {
            return Err(invalid());
        }
        Ok(authorization)
    }

    /// Ends one authorization of one of the owner's tokens (REQ-INTEGRATION-5).
    pub fn deauthorize_integration_token(
        &self,
        owner: IntegrationTokenOwner<'_>,
        token_id: &str,
        authorization_id: &str,
        key: &str,
    ) -> Result<(), MemberGrantFailure> {
        confirmed_delete(
            &self.configuration,
            &self.integration_token_route(owner, &[token_id, "authorizations", authorization_id]),
            key,
            owner.problems(),
        )
    }

    /// Revokes one of the owner's tokens (REQ-INTEGRATION-6).
    pub fn revoke_integration_token(
        &self,
        owner: IntegrationTokenOwner<'_>,
        token_id: &str,
        key: &str,
    ) -> Result<(), MemberGrantFailure> {
        confirmed_delete(
            &self.configuration,
            &self.integration_token_route(owner, &[token_id]),
            key,
            owner.problems(),
        )
    }

    /// One page of the directory of the organization an ID or a slug names, which any of its
    /// members may read (`GET /v1/organizations/{organizationRef}/memberships`): each
    /// membership's principal by ID with the display name the directory gives it, and the cursor
    /// of the next page. Who connected an organization's token is named from it
    /// (REQ-INTEGRATION-19).
    pub fn organization_directory(
        &self,
        organization: &str,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<DirectoryPage, MemberGrantFailure> {
        let page = apis::organizations_api::list_organization_memberships(
            &self.configuration,
            organization,
            limit,
            cursor,
        )
        .map_err(|error| {
            classify_problem_error(
                error,
                IntegrationTokenOwner::Organization(organization).problems(),
            )
        })?;
        let mut names = Vec::with_capacity(page.items.len());
        for entry in page.items {
            if !useful_support::valid_typed_id(&entry.principal_id, "prn_") {
                return Err(invalid());
            }
            let name = entry.display_name.filter(|name| !name.is_empty());
            names.push((entry.principal_id, name));
        }
        Ok((names, page.next_cursor))
    }
}

/// A page of an organization's directory: each principal's ID with its display name where the
/// directory gives one, and the next page's cursor.
type DirectoryPage = (Vec<(String, Option<String>)>, Option<String>);

/// A token record of `owner`: its ID, a token service's name, the owner's side and organization,
/// a version from 1, who stored it and when, and its authorizations.
fn valid_token(token: &IntegrationToken, owner: IntegrationTokenOwner<'_>) -> bool {
    useful_support::valid_typed_id(&token.id, "itk_")
        && token_service_id(&token.provider)
        && token.side == owner.side()
        && owner.names(token.organization_id.as_deref())
        && token.version >= 1
        && useful_support::valid_typed_id(&token.connected_by, "prn_")
        && !token.connected_at.is_empty()
        && !token.created_at.is_empty()
        && token.authorizations.iter().all(valid_authorization)
}

fn valid_authorization(authorization: &IntegrationTokenAuthorization) -> bool {
    useful_support::valid_typed_id(&authorization.id, "ita_")
        && useful_support::valid_typed_id(&authorization.runner_id, "rnr_")
        && useful_support::valid_typed_id(&authorization.organization_id, "org_")
        && !authorization.created_at.is_empty()
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "integration token client unit tests use Rust test assertions"
)]
mod tests {
    use super::*;
    use crate::member_grants::served;
    use crate::resource::{classify_problem, problem_fixture};
    use crate::{HttpTransportPolicy, MemberGrantKind, ServedGrantProvider};

    const KEY: &str = "integration-token-unit-test-idempotency-key";
    const TOKEN: &str = "integration-token-unit-test-token-sentinel";
    const ORG_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
    const OTHER_ORG_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5xyz";
    const TOKEN_ID: &str = "itk_01k0z6r1w8f4jy2m7q9v3x5abc";
    const AUTHORIZATION_ID: &str = "ita_01k0z6r1w8f4jy2m7q9v3x5abc";
    const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";

    fn response(status: &str, content_type: Option<&str>, key: bool, body: &[u8]) -> Vec<u8> {
        let mut response = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
        if let Some(content_type) = content_type {
            response.push_str(&format!("Content-Type: {content_type}\r\n"));
        }
        if key {
            response.push_str(&format!("Idempotency-Key: {KEY}\r\n"));
        }
        response.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        let mut response = response.into_bytes();
        response.extend_from_slice(body);
        response
    }

    fn json(status: &str, key: bool, body: &serde_json::Value) -> Vec<u8> {
        response(
            status,
            Some("application/json"),
            key,
            body.to_string().as_bytes(),
        )
    }

    /// What `call` answers when the deployment answers `response`, and the request it sent.
    fn exchange<T>(response: Vec<u8>, call: impl FnOnce(&MemberGrantApi) -> T) -> (T, String) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = useful_test_support::ScriptedHttpServer::respond(response);
        let api = MemberGrantApi::new(
            &server.api_url,
            "fixture-bearer",
            HttpTransportPolicy::AllowInsecureHttp,
        )
        .unwrap();
        let result = call(&api);
        (result, server.finish_one())
    }

    fn body(request: &str) -> serde_json::Value {
        serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
        request.lines().find_map(|line| {
            let (actual, value) = line.split_once(':')?;
            actual.eq_ignore_ascii_case(name).then_some(value.trim())
        })
    }

    fn record(owner: IntegrationTokenOwner<'_>, members: &serde_json::Value) -> serde_json::Value {
        let mut record = serde_json::json!({
            "id": TOKEN_ID,
            "provider": "mergify",
            "side": "member",
            "version": 1,
            "connectedBy": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
            "connectedAt": "2026-11-18T09:00:00Z",
            "createdAt": "2026-11-18T09:00:00Z",
            "authorizations": [{
                "id": AUTHORIZATION_ID,
                "runnerId": RUNNER_ID,
                "runnerName": "build-host-1",
                "organizationId": ORG_ID,
                "createdAt": "2026-11-18T09:05:00Z"
            }]
        });
        if let IntegrationTokenOwner::Organization(_) = owner {
            record["side"] = "org".into();
            record["organizationId"] = ORG_ID.into();
        }
        for (name, value) in members.as_object().unwrap() {
            record[name] = value.clone();
        }
        record
    }

    /// The path, below the fixture's `/api`, of the owner's tokens.
    fn route(owner: IntegrationTokenOwner<'_>) -> String {
        match owner {
            IntegrationTokenOwner::Member => "/api/v1/me/integration-tokens".to_owned(),
            IntegrationTokenOwner::Organization(reference) => {
                format!("/api/v1/organizations/{reference}/integration-tokens")
            }
        }
    }

    const OWNERS: [IntegrationTokenOwner<'static>; 3] = [
        IntegrationTokenOwner::Member,
        IntegrationTokenOwner::Organization("acme"),
        IntegrationTokenOwner::Organization(ORG_ID),
    ];

    fn token_service(name: &str, display_name: Option<&str>) -> ServedTokenService {
        ServedTokenService {
            name: name.to_owned(),
            display_name: display_name.map(str::to_owned),
        }
    }

    // REQ-GRANT-78
    #[test]
    fn the_served_token_services_are_read_from_the_answer_that_names_the_providers() {
        let answer = serde_json::json!({"items": [
            {"provider": "github", "kind": "oauth", "displayName": "GitHub"},
            {"provider": "codex", "kind": "native", "displayName": "Codex"},
            {"provider": "mergify", "kind": "token", "displayName": "Mergify"},
            {"provider": "Bad_Name", "kind": "token"},
            {"provider": "linear", "kind": "token"},
            {"provider": "claude_code", "kind": "token"},
            {"provider": "buildkite", "kind": "token", "displayName": "Build\u{1b}[31mkite"},
        ]});
        let bytes = answer.to_string().into_bytes();
        let providers = vec![
            ServedGrantProvider {
                name: "github".to_owned(),
                kind: MemberGrantKind::GrantKindOAuth,
                display_name: Some("GitHub".to_owned()),
            },
            ServedGrantProvider {
                name: "codex".to_owned(),
                kind: MemberGrantKind::GrantKindNative,
                display_name: Some("Codex".to_owned()),
            },
        ];
        let token_services = vec![
            token_service("mergify", Some("Mergify")),
            token_service("buildkite", None),
        ];
        assert_eq!(served(&bytes), Ok(providers.clone()));
        assert_eq!(served_token_services(&bytes), Ok(token_services.clone()));

        // One request answers both readings.
        let (got, request) = exchange(json("200 OK", false, &answer), |api| {
            api.served_integrations()
        });
        assert_eq!(
            got,
            Ok(Some(ServedIntegrations {
                providers,
                token_services
            }))
        );
        assert!(
            request.starts_with("GET /api/v1/me/grant-providers HTTP/1.1\r\n"),
            "{request}"
        );

        // A deployment that predates the read says nothing of what it serves.
        let not_found = response(
            "404 Not Found",
            Some("application/problem+json"),
            false,
            &problem_fixture(404, "not-found"),
        );
        let (got, _) = exchange(not_found, |api| api.served_integrations());
        assert_eq!(got, Ok(None));
    }

    // REQ-INTEGRATION-2
    // REQ-INTEGRATION-3
    #[test]
    fn a_store_sends_the_owners_route_key_and_body_and_answers_the_record() {
        for owner in OWNERS {
            for (replace, status) in [(false, "201 Created"), (true, "200 OK")] {
                let answer = record(owner, &serde_json::json!({}));
                let (got, request) = exchange(json(status, true, &answer), |api| {
                    api.store_integration_token(owner, "mergify", TOKEN.as_bytes(), replace, KEY)
                });
                assert_eq!(
                    got,
                    Ok(serde_json::from_value(answer).unwrap()),
                    "{owner:?} {status}"
                );
                assert!(
                    request.starts_with(&format!("POST {} HTTP/1.1\r\n", route(owner))),
                    "{request}"
                );
                assert_eq!(header(&request, "idempotency-key"), Some(KEY));
                let mut want = serde_json::json!({"provider": "mergify", "token": TOKEN});
                if replace {
                    want["replace"] = true.into();
                }
                assert_eq!(body(&request), want, "{owner:?} {status}");
            }
        }
    }

    // REQ-INTEGRATION-2
    // REQ-INTEGRATION-3
    #[test]
    fn a_stored_record_that_is_not_what_was_asked_is_an_invalid_response() {
        let invalid = Err(MemberGrantFailure::InvalidResponse {
            credential_rejected: false,
        });
        let member = IntegrationTokenOwner::Member;
        let by_slug = IntegrationTokenOwner::Organization("acme");
        let by_id = IntegrationTokenOwner::Organization(ORG_ID);
        for (owner, members, valid) in [
            (member, serde_json::json!({"provider": "buildkite"}), false),
            (
                member,
                serde_json::json!({"side": "org", "organizationId": ORG_ID}),
                false,
            ),
            (member, serde_json::json!({"organizationId": ORG_ID}), false),
            (
                member,
                serde_json::json!({"id": "pgr_01k0z6r1w8f4jy2m7q9v3x5abc"}),
                false,
            ),
            (member, serde_json::json!({"version": 0}), false),
            (by_slug, serde_json::json!({"side": "member"}), false),
            (by_slug, serde_json::json!({"organizationId": null}), false),
            (
                by_slug,
                serde_json::json!({"organizationId": OTHER_ORG_ID}),
                true,
            ),
            (
                by_id,
                serde_json::json!({"organizationId": OTHER_ORG_ID}),
                false,
            ),
            (by_id, serde_json::json!({"version": 2}), true),
        ] {
            let answer = record(owner, &members);
            let (got, _) = exchange(json("201 Created", true, &answer), |api| {
                api.store_integration_token(owner, "mergify", TOKEN.as_bytes(), false, KEY)
            });
            if valid {
                assert_eq!(
                    got,
                    Ok(serde_json::from_value(answer).unwrap()),
                    "{members}"
                );
            } else {
                assert_eq!(got, invalid, "{owner:?} {members}");
            }
        }
    }

    // REQ-INTEGRATION-4
    #[test]
    fn each_list_sends_its_limit_and_cursor_and_yields_the_page_and_its_next_cursor() {
        for owner in OWNERS {
            let page = serde_json::json!({
                "items": [record(owner, &serde_json::json!({}))],
                "nextCursor": "tokens-3"
            });
            let (got, request) = exchange(json("200 OK", false, &page), |api| {
                api.integration_tokens(owner, Some(200), Some("tokens-2"))
            });
            assert_eq!(got, Ok(serde_json::from_value(page).unwrap()), "{owner:?}");
            assert!(
                request.starts_with(&format!(
                    "GET {}?limit=200&cursor=tokens-2 HTTP/1.1\r\n",
                    route(owner)
                )),
                "{request}"
            );
        }
    }

    // REQ-INTEGRATION-5
    // REQ-INTEGRATION-6
    #[test]
    fn authorize_deauthorize_and_revoke_send_the_contracts_routes_each_under_its_key() {
        let no_content = || response("204 No Content", None, true, &[]);
        for owner in OWNERS {
            let tokens = route(owner);
            let authorization = record(owner, &serde_json::json!({}))["authorizations"][0].clone();
            let (got, request) = exchange(json("201 Created", true, &authorization), |api| {
                api.authorize_integration_token(owner, TOKEN_ID, RUNNER_ID, KEY)
            });
            assert_eq!(got, Ok(serde_json::from_value(authorization).unwrap()));
            assert!(
                request.starts_with(&format!(
                    "POST {tokens}/{TOKEN_ID}/authorizations HTTP/1.1\r\n"
                )),
                "{request}"
            );
            assert_eq!(header(&request, "idempotency-key"), Some(KEY));
            assert_eq!(body(&request), serde_json::json!({"runnerId": RUNNER_ID}));

            let (got, request) = exchange(no_content(), |api| {
                api.deauthorize_integration_token(owner, TOKEN_ID, AUTHORIZATION_ID, KEY)
            });
            assert_eq!(got, Ok(()));
            assert!(
                request.starts_with(&format!(
                    "DELETE {tokens}/{TOKEN_ID}/authorizations/{AUTHORIZATION_ID} HTTP/1.1\r\n"
                )),
                "{request}"
            );
            assert_eq!(header(&request, "idempotency-key"), Some(KEY));

            let (got, request) = exchange(no_content(), |api| {
                api.revoke_integration_token(owner, TOKEN_ID, KEY)
            });
            assert_eq!(got, Ok(()));
            assert!(
                request.starts_with(&format!("DELETE {tokens}/{TOKEN_ID} HTTP/1.1\r\n")),
                "{request}"
            );
            assert_eq!(header(&request, "idempotency-key"), Some(KEY));
        }

        // An authorization for another runner than the one asked for is not the answer.
        let other = record(IntegrationTokenOwner::Member, &serde_json::json!({}))["authorizations"]
            [0]
        .clone();
        let (got, _) = exchange(json("201 Created", true, &other), |api| {
            api.authorize_integration_token(
                IntegrationTokenOwner::Member,
                TOKEN_ID,
                "rnr_01k0z6r1w8f4jy2m7q9v3x5xyz",
                KEY,
            )
        });
        assert_eq!(
            got,
            Err(MemberGrantFailure::InvalidResponse {
                credential_rejected: false
            })
        );
    }

    // REQ-INTEGRATION-2
    // REQ-INTEGRATION-3
    // REQ-INTEGRATION-5
    // REQ-INTEGRATION-6
    #[test]
    fn each_contracted_problem_maps_to_one_failure_and_a_generic_not_found_only_for_an_organization()
     {
        use MemberGrantFailure as F;
        let member = IntegrationTokenOwner::Member;
        let organization = IntegrationTokenOwner::Organization("acme");
        let invalid = F::InvalidResponse {
            credential_rejected: false,
        };
        for owner in [member, organization] {
            for (status, slug, want) in [
                (400, "integration-token-invalid", F::IntegrationTokenInvalid),
                (409, "integration-token-exists", F::IntegrationTokenExists),
                (
                    404,
                    "integration-token-not-found",
                    F::IntegrationTokenNotFound,
                ),
                (
                    404,
                    "integration-token-runner-not-found",
                    F::IntegrationTokenRunnerNotFound,
                ),
                (
                    409,
                    "integration-token-already-authorized",
                    F::IntegrationTokenAlreadyAuthorized,
                ),
                (
                    404,
                    "integration-token-authorization-not-found",
                    F::IntegrationTokenAuthorizationNotFound,
                ),
                (503, "provider-unavailable", F::ProviderUnavailable),
                (403, "forbidden", F::Forbidden),
                (409, "idempotency-conflict", F::IdempotencyConflict),
                // A grant's refusal is outside the token routes' contract.
                (404, "grant-not-found", invalid.clone()),
                (409, "grant-already-authorized", invalid.clone()),
                (404, "integration-token-exists", invalid.clone()),
            ] {
                let status = StatusCode::from_u16(status).unwrap();
                assert_eq!(
                    classify_problem(
                        status,
                        &problem_fixture(status.as_u16(), slug),
                        None,
                        owner.problems()
                    ),
                    want,
                    "{owner:?} {status} {slug}"
                );
            }
            let not_found = StatusCode::NOT_FOUND;
            assert_eq!(
                classify_problem(
                    not_found,
                    &problem_fixture(404, "not-found"),
                    None,
                    owner.problems()
                ),
                if owner == member {
                    invalid.clone()
                } else {
                    F::OrganizationNotFound
                },
                "{owner:?}"
            );
        }
    }
}
