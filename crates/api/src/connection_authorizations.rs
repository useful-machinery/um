//! An organization's connection authorizations (REQ-INTEGRATION-26 to REQ-INTEGRATION-32): the
//! permission an owner gives one runner to use one of the organization's connections in one
//! project, listed, made and ended as operations of the member grant client, whose session and
//! failure the grant commands hold.
//!
//! A record decodes leniently into the generated models where a later Cloud may say more, and a
//! generated error or a record is never formatted. An authorization holds no credential.
use reqwest::StatusCode;

use super::generated::{apis, models};
use super::member_grants::{
    MemberGrantApi, MemberGrantFailure, ORGANIZATION_PROBLEMS, SHARED_PROBLEMS, invalid,
    valid_provider_id,
};
use super::resource::{
    ProblemRow, classify_problem_error, confirmed_delete, organization, serialize_body,
};

pub type ConnectionAuthorization = models::ConnectionAuthorization;
pub type ConnectionAuthorizationList = models::ConnectionAuthorizationList;
pub type ConnectionAuthorizationScope = models::ConnectionAuthorizationScope;
pub type ConnectionAuthorizationState = models::connection_authorization_scope::State;
pub type GitHubInstallationScope = models::GitHubInstallationScope;
pub type GitHubRepositorySelection = models::git_hub_installation_scope::RepositorySelection;
pub type GitHubInstallationPermission = models::GitHubInstallationPermission;

/// A refusal only the connection authorization routes answer, each its own problem type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionAuthorizationRefusal {
    /// The project is unknown or another organization's.
    ProjectNotFound,
    /// The runner is unknown, deleted or another organization's.
    RunnerNotFound,
    /// The organization has no such connection at the named provider.
    ConnectionNotFound,
    /// The authorization to end is unknown or another organization's.
    AuthorizationNotFound,
    /// Cloud cannot issue this provider's organization connection to a runner.
    ProviderIneligible,
    /// The organization's connection is not `active`.
    ConnectionInactive,
    /// An authorization is in force for the project, the runner and the provider already.
    AlreadyAuthorized,
}

/// The problem row of a refusal only these routes answer.
const fn row(
    status: StatusCode,
    kind: &'static str,
    refusal: ConnectionAuthorizationRefusal,
) -> ProblemRow<MemberGrantFailure> {
    (
        status,
        kind,
        MemberGrantFailure::ConnectionAuthorizationRefused(refusal),
    )
}

/// The problems only the connection authorization routes answer.
static AUTHORIZATION_PROBLEMS: [ProblemRow<MemberGrantFailure>; 7] = {
    use ConnectionAuthorizationRefusal as R;
    use StatusCode as S;
    [
        row(
            S::NOT_FOUND,
            "https://api.usefulmachinery.com/problems/connection-authorization-project-not-found",
            R::ProjectNotFound,
        ),
        row(
            S::NOT_FOUND,
            "https://api.usefulmachinery.com/problems/connection-authorization-runner-not-found",
            R::RunnerNotFound,
        ),
        row(
            S::NOT_FOUND,
            "https://api.usefulmachinery.com/problems/connection-authorization-connection-not-found",
            R::ConnectionNotFound,
        ),
        row(
            S::NOT_FOUND,
            "https://api.usefulmachinery.com/problems/connection-authorization-not-found",
            R::AuthorizationNotFound,
        ),
        row(
            S::CONFLICT,
            "https://api.usefulmachinery.com/problems/connection-authorization-provider-ineligible",
            R::ProviderIneligible,
        ),
        row(
            S::CONFLICT,
            "https://api.usefulmachinery.com/problems/connection-authorization-connection-inactive",
            R::ConnectionInactive,
        ),
        row(
            S::CONFLICT,
            "https://api.usefulmachinery.com/problems/connection-authorization-already-authorized",
            R::AlreadyAuthorized,
        ),
    ]
};

/// The problems an operation on an organization's connection authorizations answers. An
/// unrouted path and an unknown organization are both the generic `not-found`.
fn problems() -> impl Iterator<Item = &'static ProblemRow<MemberGrantFailure>> {
    SHARED_PROBLEMS
        .iter()
        .chain(&AUTHORIZATION_PROBLEMS)
        .chain(&ORGANIZATION_PROBLEMS)
}

/// Which of an organization's connection authorizations a list keeps: each member given keeps
/// those that name exactly that value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectionAuthorizationFilter<'a> {
    pub project_id: Option<&'a str>,
    pub runner_id: Option<&'a str>,
    pub provider: Option<&'a str>,
    pub connection_id: Option<&'a str>,
}

impl ConnectionAuthorizationFilter<'_> {
    fn keeps(&self, authorization: &ConnectionAuthorization) -> bool {
        let kept = |wanted: Option<&str>, value: &str| wanted.is_none_or(|wanted| wanted == value);
        kept(self.project_id, &authorization.project_id)
            && kept(self.runner_id, &authorization.runner_id)
            && kept(self.provider, &authorization.provider)
            && kept(self.connection_id, &authorization.connection_id)
    }
}

/// What a connection authorization is made for: one runner, one connection of a provider, in
/// one project.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionAuthorizationRequest<'a> {
    pub project_id: &'a str,
    pub runner_id: &'a str,
    pub provider: &'a str,
    pub connection_id: &'a str,
}

impl MemberGrantApi {
    /// The URL of the organization's connection authorizations, or of the one `authorization`
    /// names.
    fn connection_authorization_route(
        &self,
        organization: &str,
        authorization: Option<&str>,
    ) -> String {
        self.route(
            ["organizations", organization, "connection-authorizations"]
                .into_iter()
                .chain(authorization),
        )
    }

    /// One page of the connection authorizations in force of the organization an ID or a slug
    /// names, oldest first, that `filter` keeps (REQ-INTEGRATION-28).
    pub fn connection_authorizations(
        &self,
        organization: &str,
        filter: ConnectionAuthorizationFilter<'_>,
        limit: Option<i32>,
        cursor: Option<&str>,
    ) -> Result<ConnectionAuthorizationList, MemberGrantFailure> {
        let page = apis::organizations_api::list_connection_authorizations(
            &self.configuration,
            organization,
            limit,
            cursor,
            filter.project_id,
            filter.runner_id,
            filter.provider,
            filter.connection_id,
        )
        .map_err(|error| classify_problem_error(error, problems()))?;
        if !page.items.iter().all(|authorization| {
            filter.keeps(authorization) && valid_authorization(authorization, organization)
        }) {
            return Err(invalid());
        }
        Ok(page)
    }

    /// Authorizes one runner for one of the organization's connections in one project
    /// (REQ-INTEGRATION-27), answering the authorization with its scope as the provider grants
    /// it now (REQ-INTEGRATION-32).
    pub fn authorize_connection(
        &self,
        organization: &str,
        request: &ConnectionAuthorizationRequest<'_>,
        key: &str,
    ) -> Result<ConnectionAuthorization, MemberGrantFailure> {
        let body = serialize_body(&models::CreateConnectionAuthorizationRequest::new(
            request.project_id.to_owned(),
            request.runner_id.to_owned(),
            request.provider.to_owned(),
            request.connection_id.to_owned(),
        ))?;
        let authorization: ConnectionAuthorization = self.post(
            &self.connection_authorization_route(organization, None),
            key,
            &body,
            &[StatusCode::CREATED],
            problems(),
        )?;
        let asked = ConnectionAuthorizationFilter {
            project_id: Some(request.project_id),
            runner_id: Some(request.runner_id),
            provider: Some(request.provider),
            connection_id: Some(request.connection_id),
        };
        if !asked.keeps(&authorization) || !valid_authorization(&authorization, organization) {
            return Err(invalid());
        }
        Ok(authorization)
    }

    /// Ends one of the organization's connection authorizations, or answers as it does for one
    /// already ended (REQ-INTEGRATION-29).
    pub fn end_connection_authorization(
        &self,
        organization: &str,
        authorization_id: &str,
        key: &str,
    ) -> Result<(), MemberGrantFailure> {
        confirmed_delete(
            &self.configuration,
            &self.connection_authorization_route(organization, Some(authorization_id)),
            key,
            problems(),
        )
    }

    /// Whether the organization an ID or a slug names answers its read, which tells a deployment
    /// that does not route the connection authorizations, whose routes answer the generic
    /// `not-found`, from an organization the member cannot read.
    pub fn organization_answers(&self, reference: &str) -> Result<bool, MemberGrantFailure> {
        match organization(
            &self.configuration,
            reference,
            SHARED_PROBLEMS.iter().chain(&ORGANIZATION_PROBLEMS),
        ) {
            Ok(_) => Ok(true),
            Err(MemberGrantFailure::OrganizationNotFound) => Ok(false),
            Err(failure) => Err(failure),
        }
    }
}

/// An authorization of the organization `reference` names: typed IDs for itself, its
/// organization, project, runner, connection and maker, a provider id, when it was made, and a
/// scope whose GitHub permissions each have a name and an access. Where `reference` is an ID the
/// record's organization is that one; a slug is not checked, since a record names its
/// organization by ID. The scope of a provider this release does not decode is kept as it is.
fn valid_authorization(authorization: &ConnectionAuthorization, reference: &str) -> bool {
    let organization = &authorization.organization_id;
    useful_support::valid_typed_id(&authorization.id, "cna_")
        && useful_support::valid_typed_id(organization, "org_")
        && (!useful_support::valid_typed_id(reference, "org_") || organization == reference)
        && valid_provider_id(&authorization.provider)
        && useful_support::valid_typed_id(&authorization.project_id, "prj_")
        && useful_support::valid_typed_id(&authorization.runner_id, "rnr_")
        && ["ghi_", "lcn_", "glc_"]
            .iter()
            .any(|prefix| useful_support::valid_typed_id(&authorization.connection_id, prefix))
        && useful_support::valid_typed_id(&authorization.authorized_by, "prn_")
        && !authorization.created_at.is_empty()
        && authorization.scope.github.as_deref().is_none_or(|github| {
            github
                .permissions
                .iter()
                .all(|permission| !permission.name.is_empty() && !permission.access.is_empty())
        })
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "connection authorization client unit tests use Rust test assertions"
)]
mod tests {
    use super::*;
    use crate::HttpTransportPolicy;
    use crate::resource::{classify_problem, problem_fixture};

    const KEY: &str = "connection-authorization-unit-test-idempotency-key";
    const ORG_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
    const AUTHORIZATION_ID: &str = "cna_01k0z6r1w8f4jy2m7q9v3x5abc";
    const PROJECT_ID: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
    const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
    const CONNECTION_ID: &str = "ghi_01k0z6r1w8f4jy2m7q9v3x5abc";
    const ROUTE: &str = "/api/v1/organizations/acme/connection-authorizations";

    /// A response of `status` echoing the key where `key` is set, with `body` as JSON or none.
    fn answer(status: &str, key: bool, body: Option<&serde_json::Value>) -> Vec<u8> {
        let mut head = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
        if key {
            head.push_str(&format!("Idempotency-Key: {KEY}\r\n"));
        }
        let body = body.map(ToString::to_string).unwrap_or_default();
        if !body.is_empty() {
            head.push_str("Content-Type: application/json\r\n");
        }
        format!("{head}Content-Length: {}\r\n\r\n{body}", body.len()).into_bytes()
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

    fn sent_body(request: &str) -> serde_json::Value {
        serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
    }

    fn sent_key(request: &str) -> Option<&str> {
        request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("idempotency-key")
                .then_some(value.trim())
        })
    }

    /// An authorization record with `members` set over the fixture's.
    fn record(members: &serde_json::Value) -> serde_json::Value {
        let mut record = serde_json::json!({
            "id": AUTHORIZATION_ID,
            "organizationId": ORG_ID,
            "provider": "github",
            "projectId": PROJECT_ID,
            "projectName": "website",
            "runnerId": RUNNER_ID,
            "runnerName": "build-host-1",
            "connectionId": CONNECTION_ID,
            "connectionAccount": "acme-labs",
            "authorizedBy": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
            "createdAt": "2026-11-18T09:00:00Z",
            "scope": {
                "state": "current",
                "github": {
                    "repositorySelection": "selected",
                    "permissions": [
                        {"name": "contents", "access": "write"},
                        {"name": "metadata", "access": "read"}
                    ]
                }
            }
        });
        for (name, value) in members.as_object().unwrap() {
            record[name] = value.clone();
        }
        record
    }

    fn page(items: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({"items": items, "nextCursor": "authorizations-3"})
    }

    const fn request() -> ConnectionAuthorizationRequest<'static> {
        ConnectionAuthorizationRequest {
            project_id: PROJECT_ID,
            runner_id: RUNNER_ID,
            provider: "github",
            connection_id: CONNECTION_ID,
        }
    }

    const INVALID: MemberGrantFailure = MemberGrantFailure::InvalidResponse {
        credential_rejected: false,
    };

    // REQ-INTEGRATION-28
    #[test]
    fn the_list_sends_each_filter_it_was_given_and_answers_the_page_and_its_next_cursor() {
        let full = ConnectionAuthorizationFilter {
            project_id: Some(PROJECT_ID),
            runner_id: Some(RUNNER_ID),
            provider: Some("github"),
            connection_id: Some(CONNECTION_ID),
        };
        for (filter, query) in [
            (
                ConnectionAuthorizationFilter::default(),
                "limit=200&cursor=authorizations-2".to_owned(),
            ),
            (
                ConnectionAuthorizationFilter {
                    project_id: Some(PROJECT_ID),
                    ..ConnectionAuthorizationFilter::default()
                },
                format!("limit=200&cursor=authorizations-2&projectId={PROJECT_ID}"),
            ),
            (
                full,
                format!(
                    "limit=200&cursor=authorizations-2&projectId={PROJECT_ID}&runnerId={RUNNER_ID}\
                     &provider=github&connectionId={CONNECTION_ID}"
                ),
            ),
        ] {
            let answer_page = page(vec![record(&serde_json::json!({}))]);
            let (got, sent) = exchange(answer("200 OK", false, Some(&answer_page)), |api| {
                api.connection_authorizations("acme", filter, Some(200), Some("authorizations-2"))
            });
            assert_eq!(
                got,
                Ok(serde_json::from_value(answer_page).unwrap()),
                "{query}"
            );
            assert!(
                sent.starts_with(&format!("GET {ROUTE}?{query} HTTP/1.1\r\n")),
                "{sent}"
            );
        }

        // An item the filter does not keep is not this list's answer.
        let other = page(vec![record(
            &serde_json::json!({"projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5xyz"}),
        )]);
        let (got, _) = exchange(answer("200 OK", false, Some(&other)), |api| {
            api.connection_authorizations("acme", full, None, None)
        });
        assert_eq!(got, Err(INVALID));
    }

    // REQ-INTEGRATION-28
    // REQ-INTEGRATION-32
    #[test]
    fn a_listed_record_decodes_each_scope_and_is_refused_where_a_member_is_not_of_its_form() {
        for (members, valid) in [
            (serde_json::json!({}), true),
            (serde_json::json!({"scope": {"state": "unavailable"}}), true),
            // A later provider's scope is a member this release does not decode.
            (
                serde_json::json!({"scope": {"state": "current", "gitlab": {"level": 30}}}),
                true,
            ),
            (
                serde_json::json!({"connectionId": "lcn_01k0z6r1w8f4jy2m7q9v3x5abc"}),
                true,
            ),
            (serde_json::json!({"provider": "gitlab.com"}), true),
            (
                serde_json::json!({"id": "ita_01k0z6r1w8f4jy2m7q9v3x5abc"}),
                false,
            ),
            (
                serde_json::json!({"projectId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc"}),
                false,
            ),
            (
                serde_json::json!({"runnerId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc"}),
                false,
            ),
            (
                serde_json::json!({"authorizedBy": "org_01k0z6r1w8f4jy2m7q9v3x5abc"}),
                false,
            ),
            (serde_json::json!({"createdAt": ""}), false),
            (
                serde_json::json!({"connectionId": "itk_01k0z6r1w8f4jy2m7q9v3x5abc"}),
                false,
            ),
            (serde_json::json!({"provider": "Bad_Name"}), false),
            (serde_json::json!({"organizationId": "acme"}), false),
            (serde_json::json!({"scope": {"state": "partial"}}), false),
            (
                serde_json::json!({"scope": {"state": "current", "github": {
                    "repositorySelection": "all",
                    "permissions": [{"name": "contents", "access": ""}]
                }}}),
                false,
            ),
        ] {
            let answer_page = page(vec![record(&members)]);
            let (got, _) = exchange(answer("200 OK", false, Some(&answer_page)), |api| {
                api.connection_authorizations(
                    "acme",
                    ConnectionAuthorizationFilter::default(),
                    None,
                    None,
                )
            });
            if valid {
                let decoded: ConnectionAuthorizationList =
                    serde_json::from_value(answer_page).unwrap();
                assert_eq!(got, Ok(decoded), "{members}");
            } else {
                assert_eq!(got, Err(INVALID), "{members}");
            }
        }

        // The scopes decode to their states, a GitHub grant with its permissions.
        let (got, _) = exchange(
            answer(
                "200 OK",
                false,
                Some(&page(vec![
                    record(&serde_json::json!({})),
                    record(&serde_json::json!({"scope": {"state": "unavailable"}})),
                ])),
            ),
            |api| {
                api.connection_authorizations(
                    "acme",
                    ConnectionAuthorizationFilter::default(),
                    None,
                    None,
                )
            },
        );
        let got = got.unwrap();
        assert_eq!(got.next_cursor.as_deref(), Some("authorizations-3"));
        let github = got.items[0].scope.github.as_deref().unwrap();
        assert_eq!(
            got.items[0].scope.state,
            ConnectionAuthorizationState::Current
        );
        assert_eq!(
            github.repository_selection,
            GitHubRepositorySelection::GitHubInstallationScopeRepositorySelectionSelected
        );
        assert_eq!(
            github
                .permissions
                .iter()
                .map(|permission| (permission.name.as_str(), permission.access.as_str()))
                .collect::<Vec<_>>(),
            [("contents", "write"), ("metadata", "read")]
        );
        assert_eq!(
            got.items[1].scope.state,
            ConnectionAuthorizationState::Unavailable
        );
        assert_eq!(got.items[1].scope.github, None);
    }

    // REQ-INTEGRATION-27
    #[test]
    fn an_authorization_sends_its_key_and_exactly_its_four_members_and_answers_the_record() {
        let made = record(&serde_json::json!({}));
        let (got, sent) = exchange(answer("201 Created", true, Some(&made)), |api| {
            api.authorize_connection("acme", &request(), KEY)
        });
        assert_eq!(got, Ok(serde_json::from_value(made).unwrap()));
        assert!(
            sent.starts_with(&format!("POST {ROUTE} HTTP/1.1\r\n")),
            "{sent}"
        );
        assert_eq!(sent_key(&sent), Some(KEY));
        assert_eq!(
            sent_body(&sent),
            serde_json::json!({
                "projectId": PROJECT_ID,
                "runnerId": RUNNER_ID,
                "provider": "github",
                "connectionId": CONNECTION_ID
            })
        );

        // An answer that does not echo the key, or names another project, runner, provider or
        // connection than asked, is not this request's answer.
        let (got, _) = exchange(
            answer("201 Created", false, Some(&record(&serde_json::json!({})))),
            |api| api.authorize_connection("acme", &request(), KEY),
        );
        assert_eq!(got, Err(INVALID));
        for members in [
            serde_json::json!({"projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5xyz"}),
            serde_json::json!({"runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5xyz"}),
            serde_json::json!({"provider": "gitlab.com"}),
            serde_json::json!({"connectionId": "ghi_01k0z6r1w8f4jy2m7q9v3x5xyz"}),
        ] {
            let (got, _) = exchange(
                answer("201 Created", true, Some(&record(&members))),
                |api| api.authorize_connection("acme", &request(), KEY),
            );
            assert_eq!(got, Err(INVALID), "{members}");
        }
    }

    // REQ-INTEGRATION-29
    #[test]
    fn an_end_sends_the_authorizations_route_under_its_key_and_answers_on_no_content() {
        let (got, sent) = exchange(answer("204 No Content", true, None), |api| {
            api.end_connection_authorization("acme", AUTHORIZATION_ID, KEY)
        });
        assert_eq!(got, Ok(()));
        assert!(
            sent.starts_with(&format!("DELETE {ROUTE}/{AUTHORIZATION_ID} HTTP/1.1\r\n")),
            "{sent}"
        );
        assert_eq!(sent_key(&sent), Some(KEY));
    }

    #[test]
    fn the_organization_read_tells_an_unserved_route_from_an_unknown_organization() {
        let organization = serde_json::json!({
            "id": ORG_ID,
            "slug": "acme",
            "displayName": "Acme Labs",
            "state": "active",
            "createdAt": "2026-11-18T09:00:00Z",
            "updatedAt": "2026-11-18T09:00:00Z"
        });
        let (got, sent) = exchange(answer("200 OK", false, Some(&organization)), |api| {
            api.organization_answers("acme")
        });
        assert_eq!(got, Ok(true));
        assert!(
            sent.starts_with("GET /api/v1/organizations/acme HTTP/1.1\r\n"),
            "{sent}"
        );

        let mut not_found = b"HTTP/1.1 404 Not Found\r\nConnection: close\r\n\
            Content-Type: application/problem+json\r\n"
            .to_vec();
        let problem = problem_fixture(404, "not-found");
        not_found
            .extend_from_slice(format!("Content-Length: {}\r\n\r\n", problem.len()).as_bytes());
        not_found.extend_from_slice(&problem);
        let (got, _) = exchange(not_found, |api| api.organization_answers("acme"));
        assert_eq!(got, Ok(false));
    }

    // REQ-INTEGRATION-27
    // REQ-INTEGRATION-28
    // REQ-INTEGRATION-29
    #[test]
    fn each_contracted_problem_maps_to_its_own_failure_at_its_status() {
        use ConnectionAuthorizationRefusal as R;
        use MemberGrantFailure as F;
        let refused = F::ConnectionAuthorizationRefused;
        for (status, slug, want) in [
            (
                404,
                "connection-authorization-project-not-found",
                refused(R::ProjectNotFound),
            ),
            (
                404,
                "connection-authorization-runner-not-found",
                refused(R::RunnerNotFound),
            ),
            (
                404,
                "connection-authorization-connection-not-found",
                refused(R::ConnectionNotFound),
            ),
            (
                404,
                "connection-authorization-not-found",
                refused(R::AuthorizationNotFound),
            ),
            (
                409,
                "connection-authorization-provider-ineligible",
                refused(R::ProviderIneligible),
            ),
            (
                409,
                "connection-authorization-connection-inactive",
                refused(R::ConnectionInactive),
            ),
            (
                409,
                "connection-authorization-already-authorized",
                refused(R::AlreadyAuthorized),
            ),
            (403, "forbidden", F::Forbidden),
            (404, "not-found", F::OrganizationNotFound),
            (409, "idempotency-conflict", F::IdempotencyConflict),
            // A problem at another status than its contract's, or another route's, is outside
            // these routes' contract.
            (409, "connection-authorization-not-found", INVALID),
            (404, "connection-authorization-already-authorized", INVALID),
            (404, "integration-token-not-found", INVALID),
            (409, "grant-already-authorized", INVALID),
        ] {
            let status = StatusCode::from_u16(status).unwrap();
            assert_eq!(
                classify_problem(
                    status,
                    &problem_fixture(status.as_u16(), slug),
                    None,
                    problems()
                ),
                want,
                "{status} {slug}"
            );
        }
    }
}
