use super::*;

use super::agent_logins::{
    AFTER_TOKEN, AMBIENT_MARKER, AmbientHome, CLAUDE_FLOW, CLAUDE_SIGN_IN_URL, CLAUDE_TOKEN,
    CLAUDE_TOKEN_SENTINEL, CODEX_MATERIAL, DEVICE_CODE_PROMPT, FakeProgram, PASTE_CODE,
    PASTE_PROMPT, SENTINEL as CODEX_SENTINEL, SetupToken, SignIn, encoded, private_file,
};
use super::runner_doctor_workflow::{
    LINEAR_LIST, connection_page, held as held_grant, requirement,
};

const TOKEN: &str = "unique-grant-list-session-token";
pub(super) const OAUTH_GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const REVOKED_OAUTH_GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abd";
pub(super) const NATIVE_GRANT_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
const OTHER_NATIVE_GRANT_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abd";
pub(super) const IDENTITY_ID: &str = "idn_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const ENDED_RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abd";
const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const EMAIL: &str = "ada@example.test";
// Material no fixture is meant to carry: every grant below carries it in a member no model
// names, so it stands for anything a response might wrongly add.
pub(super) const SENTINEL: &str = "private-grant-list-material-sentinel";

fn prepared(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, std::path::PathBuf) {
    let server = ScriptedServer::respond(responses);
    let directory = private_credential_directory();
    let path = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    (server, directory, path)
}

fn run_grant(server: &ScriptedServer, credential: &Path, args: &[&str]) -> Output {
    let mut arguments = vec!["grant"];
    arguments.extend_from_slice(args);
    arguments.push("--allow-insecure-http");
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential.to_str().unwrap(),
    );
    let output = run_with_env(&arguments, &environment);
    assert_no_secret_output(&output, &[SENTINEL, TOKEN]);
    output
}

fn with_material(mut grant: serde_json::Value) -> serde_json::Value {
    grant["material"] = SENTINEL.into();
    grant
}

pub(super) fn page(items: &[serde_json::Value], next_cursor: Option<&str>) -> Vec<u8> {
    let items: Vec<_> = items.iter().cloned().map(with_material).collect();
    let mut body = serde_json::json!({"items": items});
    if let Some(cursor) = next_cursor {
        body["nextCursor"] = cursor.into();
    }
    json_http_response("200 OK", body)
}

fn one(grant: &serde_json::Value) -> Vec<u8> {
    json_http_response("200 OK", with_material(grant.clone()))
}

// A GitHub grant authorized for one runner.
pub(super) fn oauth_grant(id: &str, provider: &str, state: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "kind": "oauth",
        "provider": provider,
        "state": state,
        "generation": 1,
        "createdAt": "2026-11-01T08:00:00Z",
        "authorizations": [{
            "id": "pga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": RUNNER_ID,
            "organizationId": ORGANIZATION_ID,
            "createdAt": "2026-11-01T08:05:00Z"
        }],
        "identityId": IDENTITY_ID,
        "scopes": ["repo", "read:org"],
        "lastUsedAt": "2026-11-04T08:00:00Z",
        "accessExpiresAt": "2026-11-04T09:00:00Z",
        "refreshExpiresAt": "2027-05-01T08:00:00Z"
    })
}

// The GitHub grant with no runner authorized.
fn oauth_grant_unauthorized(id: &str) -> serde_json::Value {
    let mut grant = oauth_grant(id, "github", "active");
    grant["authorizations"] = serde_json::json!([]);
    grant
}

// A Codex grant with no runner authorized.
fn native_grant(id: &str, state: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "kind": "native",
        "provider": "codex",
        "state": state,
        "generation": 2,
        "createdAt": "2026-11-01T09:00:00Z",
        "authorizations": [],
        "organizationId": ORGANIZATION_ID,
        "label": "build laptop",
        "recordSchemaVersion": 1,
        "keyVersion": 1,
        "importedAt": "2026-11-02T10:00:00Z",
        "endedAuthorizations": []
    })
}

// The Codex grant authorized for one runner, which reported it ready and accepted at its last
// use, after another runner's authorization ended while it was serving a run (REQ-GRANT-13,
// REQ-GRANT-47, REQ-GRANT-48).
pub(super) fn native_grant_in_use() -> serde_json::Value {
    let mut grant = native_grant(NATIVE_GRANT_ID, "active");
    grant["authorizations"] = serde_json::json!([{
        "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
        "runnerId": RUNNER_ID,
        "organizationId": ORGANIZATION_ID,
        "createdAt": "2026-11-02T10:05:00Z",
        "delivery": {"generation": 2, "deliveredAt": "2026-11-02T10:06:00Z"},
        "report": {
            "state": "ready",
            "generation": 2,
            "lastUse": "accepted",
            "reportedAt": "2026-11-02T10:07:00Z"
        }
    }]);
    grant["endedAuthorizations"] = serde_json::json!([{
        "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abd",
        "runnerId": ENDED_RUNNER_ID,
        "createdAt": "2026-11-01T09:10:00Z",
        "delivery": {"generation": 1, "deliveredAt": "2026-11-01T09:11:00Z"},
        "endedAt": "2026-11-03T09:00:00Z",
        "removal": {"state": "pending"},
        "stop": {
            "runId": RUN_ID,
            "cancellationRequestId": "cmd_01k0z6r1w8f4jy2m7q9v3x5abc",
            "requestedAt": "2026-11-03T09:00:01Z"
        }
    }]);
    grant
}

fn identities(items: serde_json::Value) -> Vec<u8> {
    json_http_response("200 OK", serde_json::json!({"items": items}))
}

fn ada() -> serde_json::Value {
    serde_json::json!([{
        "id": IDENTITY_ID,
        "kind": "oidc",
        "issuer": "https://github.com",
        "subject": "1024",
        "assertedEmail": EMAIL,
        "emailVerified": true,
        "createdAt": "2026-11-01T08:00:00Z",
        "current": false
    }])
}

fn organization() -> Vec<u8> {
    json_http_response(
        "200 OK",
        serde_json::json!({
            "id": ORGANIZATION_ID,
            "state": "active",
            "displayName": "Acme",
            "slug": "acme",
            "createdAt": "2026-11-01T07:00:00Z",
            "updatedAt": "2026-11-01T07:00:00Z"
        }),
    )
}

fn problem(status: &str, code: u16, slug: &str) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": format!("https://api.usefulmachinery.com/problems/{slug}"),
            "title": "Grant result",
            "status": code
        }),
    )
}

fn json_output(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn request_lines(requests: &[String]) -> Vec<&str> {
    requests
        .iter()
        .map(|request| request.lines().next().unwrap())
        .collect()
}

/// Every request carried the member's session, and none went to the earlier agent-login
/// resource.
fn assert_member_requests(requests: &[String]) {
    for request in requests {
        assert_eq!(
            header_value(request, "authorization"),
            format!("Bearer {TOKEN}")
        );
        assert!(!request.contains("/agent-logins"), "{request}");
    }
}

fn not_connected(provider: &str) -> serde_json::Value {
    serde_json::json!({
        "provider": provider,
        "availability": "not_connected",
        "nextAction": "connect"
    })
}

/// Standard error after its first line, which must be the notice an earlier command prints
/// naming `replacement`, the command that replaces it (REQ-GRANT-77). Its wording is not a
/// contract.
pub(super) fn after_notice(output: &Output, replacement: &str) -> String {
    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    let (notice, rest) = stderr.split_once('\n').unwrap_or((&stderr, ""));
    assert!(
        notice.contains(replacement),
        "{replacement} not in\n{stderr}"
    );
    rest.to_owned()
}

/// Every command a printed next action names is a `um grant` or `um runner grant` command, and
/// none of the earlier commands is named anywhere in `text` (REQ-GRANT-77).
pub(super) fn assert_next_actions_name_the_grant_commands(text: &str) {
    let mut commands = 0;
    for line in text.lines().filter(|line| line.contains("next action:")) {
        for command in line.split('`').skip(1).step_by(2) {
            commands += 1;
            assert!(
                command.starts_with("um grant ") || command.starts_with("um runner grant "),
                "{line}"
            );
        }
    }
    assert!(commands > 0, "no next action names a command in\n{text}");
    for earlier in [
        "um auth agent-login",
        "um auth grant",
        "um runner agent-login",
    ] {
        assert!(!text.contains(earlier), "{earlier} in\n{text}");
    }
}

// The two pages of the member's grants: an active GitHub grant, then an active Codex grant and
// a revoked Linear grant.
fn two_pages() -> [Vec<u8>; 2] {
    [
        page(
            &[oauth_grant(OAUTH_GRANT_ID, "github", "active")],
            Some("grants-2"),
        ),
        page(
            &[
                native_grant(NATIVE_GRANT_ID, "active"),
                oauth_grant(REVOKED_OAUTH_GRANT_ID, "linear", "revoked"),
            ],
            None,
        ),
    ]
}

pub(super) const LIST_FIRST_PAGE: &str = "GET /api/v1/me/grants?kind=all&limit=200 HTTP/1.1";
const LIST_SECOND_PAGE: &str = "GET /api/v1/me/grants?kind=all&limit=200&cursor=grants-2 HTTP/1.1";
const READ_IDENTITIES: &str = "GET /api/v1/me/identities?limit=200 HTTP/1.1";
const READ_ORGANIZATION: &str = "GET /api/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";

// REQ-GRANT-64
// REQ-GRANT-77
#[test]
fn list_prints_every_grant_with_its_account_runners_and_status_and_names_the_rest() {
    let mut responses = Vec::new();
    for _ in 0..2 {
        responses.extend(two_pages());
        responses.push(organization());
        responses.push(identities(ada()));
    }
    let (server, _directory, credential) = prepared(responses);
    let json = run_grant(&server, &credential, &["list", "--json"]);
    let human = run_grant(&server, &credential, &["list"]);

    assert_eq!(
        json.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    assert_eq!(
        json_output(&json),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "listed",
            "grants": [
                {
                    "id": OAUTH_GRANT_ID,
                    "provider": "github",
                    "kind": "oauth",
                    "account": {"kind": "identity", "id": IDENTITY_ID, "name": EMAIL},
                    "state": "active",
                    "generation": 1,
                    "createdAt": "2026-11-01T08:00:00Z",
                    "authorizations": [{
                        "id": "pga_01k0z6r1w8f4jy2m7q9v3x5abc",
                        "runnerId": RUNNER_ID,
                        "organizationId": ORGANIZATION_ID,
                        "createdAt": "2026-11-01T08:05:00Z",
                        "availability": "available",
                        "nextAction": "none"
                    }]
                },
                {
                    "id": NATIVE_GRANT_ID,
                    "provider": "codex",
                    "kind": "native",
                    "account": {"kind": "organization", "id": ORGANIZATION_ID, "name": "acme"},
                    "label": "build laptop",
                    "state": "active",
                    "generation": 2,
                    "createdAt": "2026-11-01T09:00:00Z",
                    "authorizations": [],
                    "availability": "not_connected",
                    "nextAction": "authorize_runner"
                }
            ],
            "notConnected": [
                not_connected("gitlab.com"),
                not_connected("linear"),
                not_connected("claude_code")
            ]
        })
    );
    assert!(json.stderr.is_empty());

    assert_eq!(human.status.code(), Some(0));
    let text = stdout(&human);
    let (grants, rest) = text.split_once("── not connected ──").unwrap();
    let (oauth, native) = grants.split_once(NATIVE_GRANT_ID).unwrap();
    for name in [
        OAUTH_GRANT_ID,
        "provider: github",
        "kind: oauth",
        EMAIL,
        IDENTITY_ID,
        "state: active",
        "generation: 1",
        RUNNER_ID,
        "availability: available",
    ] {
        assert!(oauth.contains(name), "missing {name:?} in\n{text}");
    }
    for name in [
        "provider: codex",
        "kind: native",
        "acme",
        ORGANIZATION_ID,
        "generation: 2",
        "authorizations: none",
        "availability: not_connected",
        "next action: authorize_runner",
    ] {
        assert!(native.contains(name), "missing {name:?} in\n{text}");
    }
    for name in ["gitlab.com", "linear", "claude_code"] {
        assert!(rest.contains(name), "missing {name:?} in\n{text}");
    }
    assert!(!text.contains(REVOKED_OAUTH_GRANT_ID), "{text}");
    // REQ-GRANT-77
    assert_next_actions_name_the_grant_commands(&text);

    let requests = server.finish();
    assert_member_requests(&requests);
    let expected = [
        LIST_FIRST_PAGE,
        LIST_SECOND_PAGE,
        READ_ORGANIZATION,
        READ_IDENTITIES,
    ];
    assert_eq!(request_lines(&requests[..4]), expected);
    assert_eq!(request_lines(&requests[4..]), expected);
}

// REQ-GRANT-64
#[test]
fn an_account_the_member_cannot_name_is_printed_by_its_id() {
    let (server, _directory, credential) = prepared(vec![
        page(
            &[
                oauth_grant(OAUTH_GRANT_ID, "github", "active"),
                native_grant(NATIVE_GRANT_ID, "active"),
            ],
            None,
        ),
        problem("404 Not Found", 404, "not-found"),
        identities(serde_json::json!([])),
    ]);
    let output = run_grant(&server, &credential, &["list", "--json"]);

    assert_eq!(output.status.code(), Some(0));
    let document = json_output(&output);
    assert_eq!(
        document["grants"][0]["account"],
        serde_json::json!({"kind": "identity", "id": IDENTITY_ID})
    );
    assert_eq!(
        document["grants"][1]["account"],
        serde_json::json!({"kind": "organization", "id": ORGANIZATION_ID})
    );
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [LIST_FIRST_PAGE, READ_ORGANIZATION, READ_IDENTITIES]
    );
}

// REQ-GRANT-64
// REQ-GRANT-53
#[test]
fn list_names_the_command_that_authorizes_a_runner_for_an_oauth_grant_with_none() {
    let responses = vec![
        page(&[oauth_grant_unauthorized(OAUTH_GRANT_ID)], None),
        identities(ada()),
        page(&[oauth_grant_unauthorized(OAUTH_GRANT_ID)], None),
        identities(ada()),
    ];
    let (server, _directory, credential) = prepared(responses);
    let json = run_grant(&server, &credential, &["list", "--json"]);
    let human = run_grant(&server, &credential, &["list"]);

    assert_eq!(
        json.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    let grant = &json_output(&json)["grants"][0];
    assert_eq!(grant["id"], OAUTH_GRANT_ID);
    assert_eq!(grant["authorizations"], serde_json::json!([]));
    assert_eq!(grant["availability"], "not_connected");
    assert_eq!(grant["nextAction"], "authorize_runner");

    assert_eq!(human.status.code(), Some(0));
    let text = stdout(&human);
    let (grants, _) = text.split_once("── not connected ──").unwrap();
    let line = grants
        .lines()
        .find(|line| line.contains("next action:"))
        .unwrap_or_else(|| panic!("no next action in\n{text}"));
    let commands: Vec<_> = line.split('`').skip(1).step_by(2).collect();
    assert!(
        line.trim_start()
            .starts_with("next action: authorize_runner"),
        "{line}"
    );
    assert_eq!(
        commands,
        [format!("um grant authorize {OAUTH_GRANT_ID} RUNNER --yes")],
        "{line}"
    );
    server.finish();
}

// REQ-GRANT-64
#[test]
fn list_grant_shows_a_native_grants_exchange_and_ended_authorizations() {
    let grant = native_grant_in_use();
    let (server, _directory, credential) = prepared(vec![
        one(&grant),
        organization(),
        one(&grant),
        organization(),
    ]);
    let json = run_grant(&server, &credential, &["list", NATIVE_GRANT_ID, "--json"]);
    let human = run_grant(&server, &credential, &["list", NATIVE_GRANT_ID]);

    assert_eq!(
        json.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    let mut authorization = grant["authorizations"][0].clone();
    authorization["availability"] = "available".into();
    authorization["nextAction"] = "none".into();
    assert_eq!(
        json_output(&json),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "shown",
            "grant": {
                "id": NATIVE_GRANT_ID,
                "provider": "codex",
                "kind": "native",
                "account": {"kind": "organization", "id": ORGANIZATION_ID, "name": "acme"},
                "label": "build laptop",
                "state": "active",
                "generation": 2,
                "createdAt": "2026-11-01T09:00:00Z",
                "authorizations": [authorization],
                "recordSchemaVersion": 1,
                "keyVersion": 1,
                "importedAt": "2026-11-02T10:00:00Z",
                "endedAuthorizations": grant["endedAuthorizations"]
            }
        })
    );

    assert_eq!(human.status.code(), Some(0));
    let text = stdout(&human);
    let (in_force, ended) = text.split_once("ended authorizations: 1").unwrap();
    for name in [
        "delivered: generation 2 at 2026-11-02T10:06:00Z",
        "reported: ready, generation 2 at 2026-11-02T10:07:00Z",
        "last use: accepted",
        "availability: available",
    ] {
        assert!(in_force.contains(name), "missing {name:?} in\n{text}");
    }
    for name in [
        ENDED_RUNNER_ID,
        "ended: 2026-11-03T09:00:00Z",
        "removal: pending",
        RUN_ID,
    ] {
        assert!(ended.contains(name), "missing {name:?} in\n{text}");
    }

    let requests = server.finish();
    assert_member_requests(&requests);
    let read = "GET /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
    assert_eq!(
        request_lines(&requests),
        [read, READ_ORGANIZATION, read, READ_ORGANIZATION]
    );
}

// REQ-GRANT-64
#[test]
fn list_grant_shows_an_oauth_grants_scopes_last_use_and_expiries() {
    let grant = oauth_grant(OAUTH_GRANT_ID, "github", "active");
    let (server, _directory, credential) = prepared(vec![
        one(&grant),
        identities(ada()),
        one(&grant),
        identities(ada()),
    ]);
    let json = run_grant(&server, &credential, &["list", OAUTH_GRANT_ID, "--json"]);
    let human = run_grant(&server, &credential, &["list", OAUTH_GRANT_ID]);

    assert_eq!(json.status.code(), Some(0));
    let shown = &json_output(&json)["grant"];
    for member in [
        "scopes",
        "lastUsedAt",
        "accessExpiresAt",
        "refreshExpiresAt",
    ] {
        assert_eq!(shown[member], grant[member], "{member}");
    }
    assert_eq!(shown["authorizations"][0]["availability"], "available");
    assert!(shown["authorizations"][0].get("delivery").is_none());
    assert!(shown.get("endedAuthorizations").is_none());

    assert_eq!(human.status.code(), Some(0));
    let text = stdout(&human);
    for name in [
        "scopes: repo, read:org",
        "last used: 2026-11-04T08:00:00Z",
        "access expires: 2026-11-04T09:00:00Z",
        "refresh expires: 2027-05-01T08:00:00Z",
    ] {
        assert!(text.contains(name), "missing {name:?} in\n{text}");
    }
    assert!(!text.contains("delivered:"), "{text}");

    let requests = server.finish();
    assert_member_requests(&requests);
    let read = "GET /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
    assert_eq!(
        request_lines(&requests),
        [read, READ_IDENTITIES, read, READ_IDENTITIES]
    );
}

// REQ-GRANT-64: a failed identity read still prints the grant, its account by identity ID,
// and says on standard error that the names could not be read.
#[test]
fn list_grant_whose_identity_read_fails_prints_the_grant_and_a_diagnostic() {
    let grant = oauth_grant(OAUTH_GRANT_ID, "github", "active");
    let unavailable = || problem("503 Service Unavailable", 503, "service-unavailable");
    let (server, _directory, credential) =
        prepared(vec![one(&grant), unavailable(), one(&grant), unavailable()]);
    let human = run_grant(&server, &credential, &["list", OAUTH_GRANT_ID]);
    let json = run_grant(&server, &credential, &["list", OAUTH_GRANT_ID, "--json"]);

    assert_eq!(human.status.code(), Some(0), "{}", stderr(&human));
    let text = stdout(&human);
    assert!(text.contains(OAUTH_GRANT_ID), "{text}");
    assert!(text.contains(IDENTITY_ID), "{text}");
    assert!(
        stderr(&human).starts_with("warning: "),
        "{}",
        stderr(&human)
    );

    assert_eq!(json.status.code(), Some(0), "{}", stderr(&json));
    let document = json_output(&json);
    assert_eq!(document["outcome"], "shown");
    assert_eq!(document["grant"]["id"], OAUTH_GRANT_ID);
    assert!(stderr(&json).starts_with("warning: "), "{}", stderr(&json));

    let requests = server.finish();
    assert_member_requests(&requests);
    let read = "GET /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
    assert_eq!(
        request_lines(&requests),
        [read, READ_IDENTITIES, read, READ_IDENTITIES]
    );
}

// REQ-GRANT-65
#[test]
fn a_provider_name_resolves_to_the_members_one_unrevoked_grant_of_it() {
    let (server, _directory, credential) = prepared(vec![
        page(
            &[
                oauth_grant(OAUTH_GRANT_ID, "github", "active"),
                native_grant(OTHER_NATIVE_GRANT_ID, "revoked"),
                native_grant(NATIVE_GRANT_ID, "active"),
            ],
            None,
        ),
        organization(),
    ]);
    let output = run_grant(&server, &credential, &["list", "codex", "--json"]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let document = json_output(&output);
    assert_eq!(document["outcome"], "shown");
    assert_eq!(document["grant"]["id"], NATIVE_GRANT_ID);
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [LIST_FIRST_PAGE, READ_ORGANIZATION]
    );
}

// REQ-GRANT-65
#[test]
fn a_provider_name_with_no_grant_is_not_found_and_one_with_several_is_refused_naming_them() {
    let (server, _directory, credential) = prepared(vec![
        page(&[native_grant(NATIVE_GRANT_ID, "active")], None),
        page(
            &[
                native_grant(NATIVE_GRANT_ID, "active"),
                native_grant(OTHER_NATIVE_GRANT_ID, "active"),
            ],
            None,
        ),
        problem("404 Not Found", 404, "grant-not-found"),
    ]);
    let none = run_grant(&server, &credential, &["list", "claude_code", "--json"]);
    let several = run_grant(&server, &credential, &["list", "codex"]);
    let unknown = run_grant(&server, &credential, &["list", NATIVE_GRANT_ID, "--json"]);

    assert_eq!(none.status.code(), Some(1));
    assert_eq!(json_output(&none)["outcome"], "grant_not_found");
    // An unknown ID is refused by Cloud with the same outcome.
    assert_eq!(unknown.status.code(), Some(1));
    assert_eq!(json_output(&unknown), json_output(&none));

    assert_eq!(several.status.code(), Some(2));
    assert!(several.stdout.is_empty());
    let stderr = String::from_utf8(several.stderr).unwrap();
    for id in [NATIVE_GRANT_ID, OTHER_NATIVE_GRANT_ID] {
        assert!(stderr.contains(id), "missing {id} in\n{stderr}");
    }

    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(requests.len(), 3);
}

// REQ-GRANT-65
#[test]
fn a_grant_that_is_no_grant_id_nor_provider_is_refused_before_anything_is_sent() {
    let (server, _directory, credential) = prepared(vec![]);
    for grant in [
        "grt_01k0z6r1w8f4jy2m7q9v3x5abc",
        "pgr_",
        "agl_not-an-id",
        "bitbucket",
        "GitHub",
    ] {
        let output = run_grant(&server, &credential, &["list", grant, "--json"]);
        assert_eq!(output.status.code(), Some(2), "{grant}");
        assert!(output.stdout.is_empty(), "{grant}");
    }
    assert!(server.finish().is_empty());
}

// REQ-GRANT-64
#[test]
fn a_service_api_key_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let key = directory.path().join("service.key");
    fs::write(&key, "umsk_unused\n").unwrap();
    fs::set_permissions(&key, Permissions::from_mode(0o600)).unwrap();
    let (server, _credentials, credential) = prepared(vec![]);
    let output = run_grant(
        &server,
        &credential,
        &[
            "list",
            "--service-api-key-file",
            key.to_str().unwrap(),
            "--json",
        ],
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(server.finish().is_empty());
}

const LINEAR_GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abe";
const OTHER_ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abd";
const NEW_RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abe";
const CANCELLATION_REQUEST_ID: &str = "cmd_01k0z6r1w8f4jy2m7q9v3x5abc";

fn request_body(request: &str) -> serde_json::Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}

// The authorization Cloud makes of `grant_id` for `runner_id`, under the prefix its kind names.
fn authorized(grant_id: &str, runner_id: &str) -> Vec<u8> {
    let prefix = if grant_id.starts_with("pgr_") {
        "pga_"
    } else {
        "aga_"
    };
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": format!("{prefix}01k0z6r1w8f4jy2m7q9v3x5abf"),
            "runnerId": runner_id,
            "organizationId": ORGANIZATION_ID,
            "createdAt": "2026-11-06T09:00:00Z"
        }))
        .unwrap(),
    )
}

fn deleted() -> Vec<u8> {
    http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        b"",
    )
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).unwrap()
}

// What a leaf states on standard error before it acts: the line naming the grant it acts on,
// then each statement indented under it. The wording is not a contract, so a statement is
// identified by where it is printed and compared only with the refusal that states it too.
fn statement(output: &Output, grant_id: &str) -> Vec<String> {
    let text = stderr(output);
    let mut lines = text.lines();
    let heading = lines.next().unwrap_or_default();
    assert!(heading.contains(grant_id), "{grant_id} not in\n{text}");
    lines
        .map_while(|line| line.strip_prefix("  "))
        .map(str::to_owned)
        .collect()
}

// Runs `args` without `--yes`, which is refused with exit 2 before anything is sent or printed
// on standard output, naming the option it lacks and the leaf's command; its standard error.
fn refusal_without_yes(server: &ScriptedServer, credential: &Path, args: &[&str]) -> String {
    let output = run_grant(server, credential, args);
    assert_eq!(output.status.code(), Some(2), "{args:?}");
    assert!(output.stdout.is_empty(), "{args:?}");
    let text = stderr(&output);
    assert!(text.contains("--yes"), "{args:?}: {text}");
    assert!(
        text.contains(&format!("um grant {}", args[0])),
        "{args:?}: {text}"
    );
    text
}

// A statement made before acting is one the refusal without `--yes` states for both kinds,
// each on a line of its own (REQ-GRANT-66, REQ-GRANT-67, REQ-GRANT-68).
fn assert_stated_by_the_refusal(statement: &[String], refusal: &str) {
    assert!(!statement.is_empty(), "no statement before acting");
    for line in statement {
        assert_eq!(
            refusal
                .lines()
                .filter(|refused| refused.ends_with(line.as_str()))
                .count(),
            1,
            "{line:?} not in\n{refusal}"
        );
    }
}

// REQ-GRANT-66
#[test]
fn authorize_states_who_can_use_the_grant_then_authorizes_the_runner_with_one_key() {
    let native = native_grant(NATIVE_GRANT_ID, "active");
    let oauth = oauth_grant(OAUTH_GRANT_ID, "github", "active");
    let (server, _directory, credential) = prepared(vec![
        one(&native),
        // The first answer is lost; the command replays the same key once.
        Vec::new(),
        authorized(NATIVE_GRANT_ID, RUNNER_ID),
        one(&oauth),
        authorized(OAUTH_GRANT_ID, NEW_RUNNER_ID),
    ]);
    let native_run = run_grant(
        &server,
        &credential,
        &["authorize", NATIVE_GRANT_ID, RUNNER_ID, "--yes", "--json"],
    );
    let oauth_run = run_grant(
        &server,
        &credential,
        &["authorize", OAUTH_GRANT_ID, NEW_RUNNER_ID, "--yes"],
    );

    // Before acting each states who can use the grant, as the refusal without `--yes` does:
    // for a native grant two exposures, the runs that runner serves and the steps on it that
    // can read the login, and for an OAuth grant one, the member's own runs.
    let refusal = refusal_without_yes(
        &server,
        &credential,
        &["authorize", NATIVE_GRANT_ID, RUNNER_ID],
    );
    let native_statement = statement(&native_run, NATIVE_GRANT_ID);
    let oauth_statement = statement(&oauth_run, OAUTH_GRANT_ID);
    assert_eq!(native_statement.len(), 2, "{}", stderr(&native_run));
    assert_eq!(oauth_statement.len(), 1, "{}", stderr(&oauth_run));
    assert!(
        oauth_statement
            .iter()
            .all(|line| !native_statement.contains(line)),
        "{oauth_statement:?} {native_statement:?}"
    );
    assert_stated_by_the_refusal(&native_statement, &refusal);
    assert_stated_by_the_refusal(&oauth_statement, &refusal);

    assert_eq!(native_run.status.code(), Some(0), "{}", stderr(&native_run));
    assert_eq!(
        json_output(&native_run),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "authorized",
            "grantId": NATIVE_GRANT_ID,
            "authorization": {
                "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abf",
                "runnerId": RUNNER_ID,
                "organizationId": ORGANIZATION_ID,
                "createdAt": "2026-11-06T09:00:00Z"
            }
        })
    );

    assert_eq!(oauth_run.status.code(), Some(0), "{}", stderr(&oauth_run));
    let text = stdout(&oauth_run);
    for name in [
        OAUTH_GRANT_ID,
        "pga_01k0z6r1w8f4jy2m7q9v3x5abf",
        NEW_RUNNER_ID,
    ] {
        assert!(text.contains(name), "missing {name:?} in\n{text}");
    }

    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            "GET /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1",
            "POST /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations HTTP/1.1",
            "POST /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations HTTP/1.1",
            "GET /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1",
            "POST /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations HTTP/1.1",
        ]
    );
    assert_eq!(
        request_body(&requests[1]),
        serde_json::json!({"runnerId": RUNNER_ID})
    );
    assert_eq!(
        request_body(&requests[4]),
        serde_json::json!({"runnerId": NEW_RUNNER_ID})
    );
    let key = header_value(&requests[1], "idempotency-key");
    assert!(!key.is_empty());
    assert_eq!(header_value(&requests[2], "idempotency-key"), key);
    assert_ne!(header_value(&requests[4], "idempotency-key"), key);
}

// REQ-GRANT-65
// REQ-GRANT-66
// REQ-GRANT-67
// REQ-GRANT-68
#[test]
fn a_missing_confirmation_or_a_malformed_target_exits_2_before_anything_is_sent() {
    let (server, _directory, credential) = prepared(vec![]);
    let refused = |args: &[&str]| {
        let output = run_grant(&server, &credential, args);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
    };

    // Without --yes each leaf names the option and itself; what it states for each kind is
    // the statement it makes before acting (each acting test compares the two).
    refusal_without_yes(
        &server,
        &credential,
        &["authorize", NATIVE_GRANT_ID, RUNNER_ID],
    );
    refusal_without_yes(&server, &credential, &["authorize", "--all", RUNNER_ID]);
    refusal_without_yes(
        &server,
        &credential,
        &["deauthorize", NATIVE_GRANT_ID, RUNNER_ID],
    );
    refusal_without_yes(&server, &credential, &["revoke", "codex"]);

    // GRANT and --all exclude each other, --all takes one runner, and a runner is named by its
    // ID, also where the command would otherwise offer the runners to choose from.
    refused(&["authorize", NATIVE_GRANT_ID, "--all", RUNNER_ID, "--yes"]);
    refused(&["authorize", NATIVE_GRANT_ID, "--all", "--yes"]);
    refused(&["authorize", "--all", RUNNER_ID, NEW_RUNNER_ID, "--yes"]);
    refused(&["authorize", NATIVE_GRANT_ID, "builder", "--yes"]);
    refused(&["authorize", "codex", "builder"]);
    refused(&["authorize", "--all", "builder"]);
    refused(&["deauthorize", "codex", "builder"]);
    refused(&["authorize", "--yes"]);
    refused(&["authorize", RUNNER_ID, "--yes"]);
    refused(&["revoke", "bitbucket", "--yes"]);

    assert!(server.finish().is_empty());
}

// Four unrevoked grants of the member and a revoked one: a GitHub grant already authorized
// for the runner, a Linear grant, a Codex grant of the runner's organization and a Claude Code
// grant of another organization.
fn every_kind() -> Vec<u8> {
    let mut other_organization = native_grant(OTHER_NATIVE_GRANT_ID, "active");
    other_organization["provider"] = "claude_code".into();
    other_organization["organizationId"] = OTHER_ORGANIZATION_ID.into();
    let mut linear = oauth_grant(LINEAR_GRANT_ID, "linear", "active");
    linear["authorizations"] = serde_json::json!([]);
    page(
        &[
            oauth_grant(OAUTH_GRANT_ID, "github", "active"),
            linear,
            oauth_grant(REVOKED_OAUTH_GRANT_ID, "gitlab.com", "revoked"),
            native_grant(NATIVE_GRANT_ID, "active"),
            other_organization,
        ],
        None,
    )
}

const AUTHORIZE_LINEAR: &str =
    "POST /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abe/authorizations HTTP/1.1";
const AUTHORIZE_CODEX: &str =
    "POST /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations HTTP/1.1";
const AUTHORIZE_CLAUDE_CODE: &str =
    "POST /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abd/authorizations HTTP/1.1";

fn grant_outcomes(document: &serde_json::Value) -> Vec<(String, String)> {
    document["grants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|grant| {
            (
                grant["grantId"].as_str().unwrap().to_owned(),
                grant["outcome"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

// REQ-GRANT-66
#[test]
fn authorize_all_authorizes_every_grant_that_can_serve_the_runner() {
    let mut responses = Vec::new();
    for _ in 0..2 {
        responses.extend([
            every_kind(),
            authorized(LINEAR_GRANT_ID, RUNNER_ID),
            authorized(NATIVE_GRANT_ID, RUNNER_ID),
            problem("404 Not Found", 404, "grant-runner-not-found"),
        ]);
    }
    let (server, _directory, credential) = prepared(responses);
    let json = run_grant(
        &server,
        &credential,
        &["authorize", "--all", RUNNER_ID, "--yes", "--json"],
    );
    let human = run_grant(
        &server,
        &credential,
        &["authorize", "--all", RUNNER_ID, "--yes"],
    );

    assert_eq!(json.status.code(), Some(0), "{}", stderr(&json));
    let document = json_output(&json);
    assert_eq!(document["outcome"], "authorized");
    assert_eq!(document["runnerId"], RUNNER_ID);
    assert_eq!(
        grant_outcomes(&document),
        [
            (OAUTH_GRANT_ID.to_owned(), "already_authorized".to_owned()),
            (LINEAR_GRANT_ID.to_owned(), "authorized".to_owned()),
            (NATIVE_GRANT_ID.to_owned(), "authorized".to_owned()),
            (
                OTHER_NATIVE_GRANT_ID.to_owned(),
                "other_organization".to_owned()
            ),
        ]
    );
    assert_eq!(
        document["grants"][2]["authorization"]["id"],
        "aga_01k0z6r1w8f4jy2m7q9v3x5abf"
    );

    assert_eq!(human.status.code(), Some(0), "{}", stderr(&human));
    let text = stdout(&human);
    for (id, outcome) in [
        (OAUTH_GRANT_ID, "already_authorized"),
        (LINEAR_GRANT_ID, "authorized"),
        (NATIVE_GRANT_ID, "authorized"),
        (OTHER_NATIVE_GRANT_ID, "other_organization"),
    ] {
        assert!(
            text.lines()
                .any(|line| line.contains(id) && line.contains(outcome)),
            "missing {id} {outcome} in\n{text}"
        );
    }

    let requests = server.finish();
    assert_member_requests(&requests);
    let expected = [
        LIST_FIRST_PAGE,
        AUTHORIZE_LINEAR,
        AUTHORIZE_CODEX,
        AUTHORIZE_CLAUDE_CODE,
    ];
    assert_eq!(request_lines(&requests[..4]), expected);
    assert_eq!(request_lines(&requests[4..]), expected);
    // Each grant's request carries a key of its own.
    let keys: std::collections::BTreeSet<_> = requests[1..4]
        .iter()
        .map(|request| header_value(request, "idempotency-key"))
        .collect();
    assert_eq!(keys.len(), 3);
}

// REQ-GRANT-65
// REQ-GRANT-66
#[test]
fn authorize_all_exits_1_when_a_grant_is_refused_for_another_reason() {
    let mut responses = Vec::new();
    for _ in 0..2 {
        responses.extend([
            every_kind(),
            authorized(LINEAR_GRANT_ID, RUNNER_ID),
            problem("409 Conflict", 409, "grant-runner-conflict"),
            problem("404 Not Found", 404, "grant-runner-not-found"),
        ]);
    }
    let (server, _directory, credential) = prepared(responses);
    let json = run_grant(
        &server,
        &credential,
        &["authorize", "--all", RUNNER_ID, "--yes", "--json"],
    );
    let human = run_grant(
        &server,
        &credential,
        &["authorize", "--all", RUNNER_ID, "--yes"],
    );

    assert_eq!(json.status.code(), Some(1), "{}", stderr(&json));
    let document = json_output(&json);
    assert_eq!(
        grant_outcomes(&document),
        [
            (OAUTH_GRANT_ID.to_owned(), "already_authorized".to_owned()),
            (LINEAR_GRANT_ID.to_owned(), "authorized".to_owned()),
            (NATIVE_GRANT_ID.to_owned(), "runner_holds_login".to_owned()),
            (
                OTHER_NATIVE_GRANT_ID.to_owned(),
                "other_organization".to_owned()
            ),
        ]
    );
    assert_ne!(document["outcome"], "authorized");

    assert_eq!(human.status.code(), Some(1));
    let text = format!("{}{}", stdout(&human), stderr(&human));
    assert!(text.contains("runner_holds_login"), "{text}");
    assert!(text.contains("um grant deauthorize"), "{text}");
    assert_eq!(server.finish().len(), 8);
}

// REQ-GRANT-65
// REQ-GRANT-66
#[test]
fn authorize_all_tells_a_codex_grant_authorized_elsewhere_from_one_authorized_here() {
    // The GitHub grant is authorized for the runner already; the Codex grant is refused because
    // it is authorized for another runner.
    let grants = || {
        page(
            &[
                oauth_grant(OAUTH_GRANT_ID, "github", "active"),
                native_grant(NATIVE_GRANT_ID, "active"),
            ],
            None,
        )
    };
    let elsewhere = || problem("409 Conflict", 409, "grant-already-authorized");
    let (server, _directory, credential) =
        prepared(vec![grants(), elsewhere(), grants(), elsewhere()]);
    let json = run_grant(
        &server,
        &credential,
        &["authorize", "--all", RUNNER_ID, "--yes", "--json"],
    );
    let human = run_grant(
        &server,
        &credential,
        &["authorize", "--all", RUNNER_ID, "--yes"],
    );

    assert_eq!(json.status.code(), Some(1), "{}", stderr(&json));
    let document = json_output(&json);
    assert_eq!(document["outcome"], "refused");
    assert_eq!(
        grant_outcomes(&document),
        [
            (OAUTH_GRANT_ID.to_owned(), "already_authorized".to_owned()),
            (
                NATIVE_GRANT_ID.to_owned(),
                "authorized_elsewhere".to_owned()
            ),
        ]
    );

    assert_eq!(human.status.code(), Some(1), "{}", stderr(&human));
    let text = stdout(&human);
    let line = |id: &str| {
        text.lines()
            .find(|line| line.contains(id))
            .unwrap_or_else(|| panic!("missing {id} in\n{text}"))
    };
    assert!(
        line(OAUTH_GRANT_ID).contains("already_authorized"),
        "{text}"
    );
    assert!(
        line(NATIVE_GRANT_ID).contains("authorized_elsewhere"),
        "{text}"
    );
    assert!(
        !line(NATIVE_GRANT_ID).contains("already_authorized"),
        "{text}"
    );
    // The refusal's remedy names the command that frees the grant.
    assert!(
        stderr(&human).contains("um grant deauthorize"),
        "{}",
        stderr(&human)
    );

    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            LIST_FIRST_PAGE,
            AUTHORIZE_CODEX,
            LIST_FIRST_PAGE,
            AUTHORIZE_CODEX
        ]
    );
}

// REQ-GRANT-66
#[test]
fn authorize_all_reports_runner_not_found_when_no_grant_finds_the_runner() {
    let mut linear = oauth_grant(LINEAR_GRANT_ID, "linear", "active");
    linear["authorizations"] = serde_json::json!([]);
    let not_found = || problem("404 Not Found", 404, "grant-runner-not-found");
    let (server, _directory, credential) = prepared(vec![
        page(&[linear, native_grant(NATIVE_GRANT_ID, "active")], None),
        not_found(),
        not_found(),
    ]);
    let output = run_grant(
        &server,
        &credential,
        &["authorize", "--all", NEW_RUNNER_ID, "--yes", "--json"],
    );

    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert_eq!(json_output(&output)["outcome"], "runner_not_found");
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [LIST_FIRST_PAGE, AUTHORIZE_LINEAR, AUTHORIZE_CODEX]
    );
}

// Cloud's answer to a request over the rate limit: a failure that is no refusal of one grant or
// one runner.
fn rate_limited() -> Vec<u8> {
    http_response_with_headers(
        "429 Too Many Requests",
        Some("application/problem+json"),
        &[("Retry-After", "7")],
        &serde_json::to_vec(&serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/rate-limit-exceeded",
            "title": "Slow down",
            "status": 429
        }))
        .unwrap(),
    )
}

// REQ-GRANT-66
#[test]
fn authorize_all_reports_the_authorizations_made_before_a_failure_that_ends_it() {
    let mut linear = oauth_grant(LINEAR_GRANT_ID, "linear", "active");
    linear["authorizations"] = serde_json::json!([]);
    let mut claude_code = native_grant(OTHER_NATIVE_GRANT_ID, "active");
    claude_code["provider"] = "claude_code".into();
    // Linear and Codex are authorized, Claude Code is rate limited, and the GitHub grant
    // after it is never tried.
    let grants = || {
        page(
            &[
                linear.clone(),
                native_grant(NATIVE_GRANT_ID, "active"),
                claude_code.clone(),
                oauth_grant_unauthorized(OAUTH_GRANT_ID),
            ],
            None,
        )
    };
    let mut responses = Vec::new();
    for _ in 0..2 {
        responses.extend([
            grants(),
            authorized(LINEAR_GRANT_ID, RUNNER_ID),
            authorized(NATIVE_GRANT_ID, RUNNER_ID),
            rate_limited(),
        ]);
    }
    let (server, _directory, credential) = prepared(responses);
    let json = run_grant(
        &server,
        &credential,
        &["authorize", "--all", RUNNER_ID, "--yes", "--json"],
    );
    let human = run_grant(
        &server,
        &credential,
        &["authorize", "--all", RUNNER_ID, "--yes"],
    );

    assert_eq!(json.status.code(), Some(1), "{}", stderr(&json));
    let documents: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(&json.stdout)
        .into_iter()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(documents.len(), 2, "{}", stdout(&json));
    assert_eq!(documents[0]["outcome"], "refused");
    assert_eq!(documents[0]["runnerId"], RUNNER_ID);
    assert_eq!(
        grant_outcomes(&documents[0]),
        [
            (LINEAR_GRANT_ID.to_owned(), "authorized".to_owned()),
            (NATIVE_GRANT_ID.to_owned(), "authorized".to_owned()),
        ]
    );
    assert_eq!(
        documents[0]["grants"][1]["authorization"]["id"],
        "aga_01k0z6r1w8f4jy2m7q9v3x5abf"
    );
    assert_eq!(documents[1]["outcome"], "rate_limited");
    assert_eq!(documents[1]["retryAfter"], 7);

    assert_eq!(human.status.code(), Some(1), "{}", stderr(&human));
    let text = stdout(&human);
    for id in [LINEAR_GRANT_ID, NATIVE_GRANT_ID] {
        assert!(
            text.lines()
                .any(|line| line.contains(id) && line.contains("authorized")),
            "missing {id} in\n{text}"
        );
    }
    assert!(!text.contains(OAUTH_GRANT_ID), "{text}");
    // The failure names the grant it ended at.
    assert!(
        stderr(&human).contains(OTHER_NATIVE_GRANT_ID),
        "{}",
        stderr(&human)
    );

    let requests = server.finish();
    assert_member_requests(&requests);
    let expected = [
        LIST_FIRST_PAGE,
        AUTHORIZE_LINEAR,
        AUTHORIZE_CODEX,
        AUTHORIZE_CLAUDE_CODE,
    ];
    assert_eq!(request_lines(&requests[..4]), expected);
    assert_eq!(request_lines(&requests[4..]), expected);
}

// A Codex grant whose authorization for the runner ended while it was serving a run, as the
// grant is read after the change.
fn native_grant_deauthorized() -> serde_json::Value {
    let mut grant = native_grant_in_use();
    let mut ended = grant["authorizations"][0].clone();
    ended["endedAt"] = "2026-11-06T09:00:00Z".into();
    ended["removal"] = serde_json::json!({"state": "pending"});
    ended["stop"] = serde_json::json!({
        "runId": RUN_ID,
        "cancellationRequestId": CANCELLATION_REQUEST_ID,
        "requestedAt": "2026-11-06T09:00:01Z"
    });
    ended.as_object_mut().unwrap().remove("organizationId");
    grant["authorizations"] = serde_json::json!([]);
    grant["endedAuthorizations"] = serde_json::json!([ended]);
    grant
}

const DEAUTHORIZE_NATIVE: &str = "DELETE /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations/aga_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";

// REQ-GRANT-67
#[test]
fn deauthorize_ends_a_native_grants_authorization_and_reports_its_removal_and_stop() {
    let read = "GET /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
    let (server, _directory, credential) = prepared(vec![
        one(&native_grant_in_use()),
        Vec::new(),
        deleted(),
        one(&native_grant_deauthorized()),
        one(&native_grant_in_use()),
        deleted(),
        one(&native_grant_deauthorized()),
    ]);
    let json = run_grant(
        &server,
        &credential,
        &["deauthorize", NATIVE_GRANT_ID, RUNNER_ID, "--yes", "--json"],
    );
    let human = run_grant(
        &server,
        &credential,
        &["deauthorize", NATIVE_GRANT_ID, RUNNER_ID, "--yes"],
    );

    assert_eq!(json.status.code(), Some(0), "{}", stderr(&json));
    // Before acting it states that the run the runner serves will be asked to stop, as the
    // refusal without `--yes` does; the stop it reports is the outcome.
    assert_stated_by_the_refusal(
        &statement(&json, NATIVE_GRANT_ID),
        &refusal_without_yes(
            &server,
            &credential,
            &["deauthorize", NATIVE_GRANT_ID, RUNNER_ID],
        ),
    );
    assert_eq!(
        json_output(&json),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "deauthorized",
            "grantId": NATIVE_GRANT_ID,
            "authorizationId": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": RUNNER_ID,
            "removal": {"state": "pending"},
            "stop": {
                "runId": RUN_ID,
                "cancellationRequestId": CANCELLATION_REQUEST_ID,
                "requestedAt": "2026-11-06T09:00:01Z"
            }
        })
    );
    assert_eq!(human.status.code(), Some(0));
    let text = stdout(&human);
    for name in ["removal: pending", RUN_ID, CANCELLATION_REQUEST_ID] {
        assert!(text.contains(name), "missing {name:?} in\n{text}");
    }

    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            read,
            DEAUTHORIZE_NATIVE,
            DEAUTHORIZE_NATIVE,
            read,
            read,
            DEAUTHORIZE_NATIVE,
            read
        ]
    );
    let key = header_value(&requests[1], "idempotency-key");
    assert!(!key.is_empty());
    assert_eq!(header_value(&requests[2], "idempotency-key"), key);
}

// REQ-GRANT-67
#[test]
fn deauthorize_ends_an_oauth_grants_authorization_without_a_stop() {
    let (server, _directory, credential) = prepared(vec![
        page(&[oauth_grant(OAUTH_GRANT_ID, "github", "active")], None),
        deleted(),
    ]);
    let output = run_grant(
        &server,
        &credential,
        &["deauthorize", "github", RUNNER_ID, "--yes", "--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_stated_by_the_refusal(
        &statement(&output, OAUTH_GRANT_ID),
        &refusal_without_yes(&server, &credential, &["deauthorize", "github", RUNNER_ID]),
    );
    // No removal and no stop: the document is the authorization ended and nothing else.
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "deauthorized",
            "grantId": OAUTH_GRANT_ID,
            "authorizationId": "pga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": RUNNER_ID
        })
    );
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            LIST_FIRST_PAGE,
            "DELETE /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations/pga_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1"
        ]
    );
}

// The Codex grant revoked: both runners' authorizations ended, each with its removal state
// and the stop requested of its run.
fn native_grant_revoked() -> serde_json::Value {
    let mut grant = native_grant_deauthorized();
    grant["state"] = "revoked".into();
    grant["revokedAt"] = "2026-11-06T09:00:00Z".into();
    let earlier = native_grant_in_use()["endedAuthorizations"][0].clone();
    grant["endedAuthorizations"]
        .as_array_mut()
        .unwrap()
        .push(earlier);
    grant
}

// REQ-GRANT-68
#[test]
fn revoke_erases_a_native_grant_and_reports_each_runners_removal_and_stop() {
    let delete = "DELETE /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
    let read = "GET /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
    let after = native_grant_revoked();
    let (server, _directory, credential) = prepared(vec![
        page(&[native_grant_in_use()], None),
        deleted(),
        one(&after),
        one(&native_grant_in_use()),
        Vec::new(),
        deleted(),
        one(&after),
    ]);
    let human = run_grant(&server, &credential, &["revoke", "codex", "--yes"]);
    let json = run_grant(
        &server,
        &credential,
        &["revoke", NATIVE_GRANT_ID, "--yes", "--json"],
    );

    assert_eq!(human.status.code(), Some(0), "{}", stderr(&human));
    assert_stated_by_the_refusal(
        &statement(&human, NATIVE_GRANT_ID),
        &refusal_without_yes(&server, &credential, &["revoke", "codex"]),
    );
    let text = stdout(&human);
    for name in [
        RUNNER_ID,
        ENDED_RUNNER_ID,
        "removal: pending",
        RUN_ID,
        CANCELLATION_REQUEST_ID,
    ] {
        assert!(text.contains(name), "missing {name:?} in\n{text}");
    }
    // Each runner with its removal state and stop, then what those say, each a paragraph of
    // its own: what a removal confirms, how to follow a stop, and that the session at the
    // provider is not ended. Their wording is not a contract; the command one names is.
    let (_, runners) = text
        .split_once("runners: 2\n")
        .unwrap_or_else(|| panic!("no runners in\n{text}"));
    let mut paragraphs = runners.split("\n\n");
    let block = paragraphs.next().unwrap();
    for runner in [RUNNER_ID, ENDED_RUNNER_ID] {
        assert!(block.contains(&format!("runner: {runner}")), "{text}");
    }
    assert_eq!(block.matches("removal: pending").count(), 2, "{text}");
    assert_eq!(block.matches(CANCELLATION_REQUEST_ID).count(), 2, "{text}");
    let notes: Vec<_> = paragraphs.collect();
    assert_eq!(notes.len(), 3, "{text}");
    assert!(
        notes
            .iter()
            .flat_map(|note| note.lines())
            .any(|line| line.trim() == "um run cancel ORGANIZATION RUN --wait"),
        "{text}"
    );

    assert_eq!(json.status.code(), Some(0), "{}", stderr(&json));
    let document = json_output(&json);
    assert_eq!(document["outcome"], "revoked");
    assert_eq!(document["grantId"], NATIVE_GRANT_ID);
    assert_eq!(document["kind"], "native");
    assert_eq!(
        document["endedAuthorizations"],
        after["endedAuthorizations"]
    );

    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [LIST_FIRST_PAGE, delete, read, read, delete, delete, read]
    );
    assert_ne!(
        header_value(&requests[1], "idempotency-key"),
        header_value(&requests[4], "idempotency-key")
    );
    assert_eq!(
        header_value(&requests[4], "idempotency-key"),
        header_value(&requests[5], "idempotency-key")
    );
}

// REQ-GRANT-68
#[test]
fn revoke_revokes_an_oauth_grant_without_reading_it_after() {
    let (server, _directory, credential) = prepared(vec![
        one(&oauth_grant(OAUTH_GRANT_ID, "github", "active")),
        deleted(),
    ]);
    let output = run_grant(
        &server,
        &credential,
        &["revoke", OAUTH_GRANT_ID, "--yes", "--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_stated_by_the_refusal(
        &statement(&output, OAUTH_GRANT_ID),
        &refusal_without_yes(&server, &credential, &["revoke", OAUTH_GRANT_ID]),
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "revoked",
            "grantId": OAUTH_GRANT_ID,
            "provider": "github",
            "kind": "oauth"
        })
    );
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            "GET /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1",
            "DELETE /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1"
        ]
    );
    assert!(!header_value(&requests[1], "idempotency-key").is_empty());
    assert!(requests[1].ends_with("\r\n\r\n"), "{}", requests[1]);
}

// REQ-GRANT-65
// REQ-GRANT-66
// REQ-GRANT-67
// REQ-GRANT-68
#[test]
fn each_refusal_these_leaves_meet_exits_1_with_its_remedy() {
    let native = || one(&native_grant(NATIVE_GRANT_ID, "active"));
    let authorize: &[&str] = &["authorize", NATIVE_GRANT_ID, RUNNER_ID, "--yes"];
    // The arguments, Cloud's answers in order, the outcome, and the command its remedy names,
    // if it names one.
    type Case<'a> = (&'a [&'a str], Vec<Vec<u8>>, &'a str, Option<&'a str>);
    let cases: [Case; 5] = [
        (
            &["revoke", NATIVE_GRANT_ID, "--yes"],
            vec![problem("404 Not Found", 404, "grant-not-found")],
            "grant_not_found",
            Some("um grant list"),
        ),
        (
            authorize,
            vec![
                native(),
                problem("503 Service Unavailable", 503, "provider-unavailable"),
            ],
            "provider_unavailable",
            // Its remedy names no command: only the deployment changes what it serves.
            None,
        ),
        (
            authorize,
            vec![
                native(),
                problem("404 Not Found", 404, "grant-runner-not-found"),
            ],
            "runner_not_found",
            Some("um runner list"),
        ),
        (
            authorize,
            vec![
                native(),
                problem("409 Conflict", 409, "grant-runner-conflict"),
            ],
            "runner_holds_login",
            Some("um grant deauthorize"),
        ),
        // The runner holds no authorization in force, so nothing is deleted.
        (
            &["deauthorize", NATIVE_GRANT_ID, RUNNER_ID, "--yes"],
            vec![native()],
            "authorization_not_found",
            Some("um grant list"),
        ),
    ];
    for (args, responses, outcome, remedy) in cases {
        let sent = responses.len();
        let (server, _directory, credential) = prepared([responses.clone(), responses].concat());
        let json = run_grant(&server, &credential, &[args, &["--json"]].concat());
        let human = run_grant(&server, &credential, args);

        assert_eq!(json.status.code(), Some(1), "{outcome}");
        assert_eq!(json_output(&json)["outcome"], outcome);
        assert_eq!(human.status.code(), Some(1), "{outcome}");
        let text = stderr(&human);
        let commands: Vec<_> = text
            .lines()
            .map(str::trim_start)
            .filter(|line| line.starts_with("um "))
            .collect();
        match remedy {
            Some(command) => assert!(
                commands.iter().any(|line| line.starts_with(command)),
                "{outcome}: {text}"
            ),
            None => assert!(commands.is_empty(), "{outcome}: {text}"),
        }
        let requests = server.finish();
        assert_member_requests(&requests);
        assert_eq!(requests.len(), 2 * sent, "{outcome}");
        assert!(
            requests
                .iter()
                .all(|request| !request.starts_with("DELETE")),
            "{outcome}"
        );
    }
}

// REQ-GRANT-66
#[test]
fn a_codex_grant_authorized_for_another_runner_names_the_deauthorize_that_moves_it() {
    // Cloud answers a Codex grant authorized for another runner `grant-already-authorized`.
    let answers = || {
        vec![
            one(&native_grant(NATIVE_GRANT_ID, "active")),
            problem("409 Conflict", 409, "grant-already-authorized"),
        ]
    };
    let (server, _directory, credential) = prepared([answers(), answers()].concat());
    let args = ["authorize", NATIVE_GRANT_ID, RUNNER_ID, "--yes"];
    let json = run_grant(&server, &credential, &[&args[..], &["--json"]].concat());
    let human = run_grant(&server, &credential, &args);

    assert_eq!(json.status.code(), Some(1));
    assert_eq!(json_output(&json)["outcome"], "already_authorized");
    assert_eq!(human.status.code(), Some(1));
    let text = stderr(&human);
    assert!(
        text.contains("um grant deauthorize GRANT RUNNER --yes"),
        "{text}"
    );
    // Another sign-in cannot serve the other runner: the held grant is refused or replaced
    // in place (REQ-GRANT-73).
    assert!(!text.contains("um grant create codex"), "{text}");
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(requests.len(), 4);
    assert!(
        requests
            .iter()
            .all(|request| !request.starts_with("DELETE"))
    );
}

// `um grant authorize` and `deauthorize` given no `RUNNER`, run here with a null standard
// input, where they cannot prompt (REQ-GRANT-81).
const GLOBEX_RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abf";
const LIST_ACME_RUNNERS: &str = "GET /api/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/runner-registrations?limit=200 HTTP/1.1";
const LIST_GLOBEX_RUNNERS: &str = "GET /api/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abe/runner-registrations?limit=200 HTTP/1.1";

// A runner of `organization_id` in the pool `builders`, as the runner registrations list it.
fn registration(
    (id, organization_id, name): (&str, &str, &str),
    connectivity: &str,
    mode: &str,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "organizationId": organization_id,
        "runnerPool": {"id": "rpl_01k0z6r1w8f4jy2m7q9v3x5abc", "name": "builders"},
        "name": name,
        "administration": {
            "mode": mode,
            "createdAt": "2026-11-01T08:00:00Z",
            "updatedAt": "2026-11-01T08:00:00Z"
        },
        "enrollment": {"state": "credentialed", "validCredentialCount": 1},
        "connectivity": {"state": connectivity},
        "activity": {"state": "idle", "currentAssignmentCount": 0}
    })
}

fn registrations(items: &[serde_json::Value]) -> Vec<u8> {
    json_http_response("200 OK", serde_json::json!({"items": items}))
}

const ALPHA: (&str, &str, &str) = (ENDED_RUNNER_ID, ORGANIZATION_ID, "alpha");
const BRAVO: (&str, &str, &str) = (NEW_RUNNER_ID, ORGANIZATION_ID, "bravo");
const CHARLIE: (&str, &str, &str) = (RUNNER_ID, ORGANIZATION_ID, "charlie");
const DELTA: (&str, &str, &str) = (GLOBEX_RUNNER_ID, GLOBEX_ORGANIZATION_ID, "delta");

// Acme's three runners: an offline one, an online draining one, and, listed last, the one
// online and enabled.
fn acme_runners() -> Vec<u8> {
    registrations(&[
        registration(ALPHA, "offline", "enabled"),
        registration(BRAVO, "online", "draining"),
        registration(CHARLIE, "online", "enabled"),
    ])
}

// The runners `--json` names, by ID and name, in the order it names them.
fn offered(document: &serde_json::Value) -> Vec<(String, serde_json::Value)> {
    document["runners"]
        .as_array()
        .unwrap()
        .iter()
        .map(|runner| {
            (
                runner["id"].as_str().unwrap().to_owned(),
                runner["name"].clone(),
            )
        })
        .collect()
}

// Where in `output`'s standard error each runner is named, by the line that holds both its ID
// and its name; the runner each line names, in the order the lines come.
fn named_runners(output: &Output, runners: &[(&str, &str, &str)]) -> Vec<String> {
    let text = stderr(output);
    let mut named: Vec<_> = runners
        .iter()
        .map(|(id, _, name)| {
            let line = text
                .lines()
                .position(|line| line.contains(id) && line.contains(name))
                .unwrap_or_else(|| panic!("{name} {id} not on one line of\n{text}"));
            (line, (*name).to_owned())
        })
        .collect();
    named.sort();
    named.into_iter().map(|(_, name)| name).collect()
}

// REQ-GRANT-81
#[test]
fn authorize_without_a_runner_where_it_cannot_prompt_names_each_runner_it_offers() {
    let grants = || page(&[native_grant(NATIVE_GRANT_ID, "active")], None);
    let (server, _directory, credential) =
        prepared(vec![grants(), acme_runners(), grants(), acme_runners()]);
    let human = run_grant(&server, &credential, &["authorize", "codex"]);
    let json = run_grant(&server, &credential, &["authorize", "codex", "--json"]);

    // It exits as a missing argument does, naming each runner by its name and ID, the one
    // online and enabled first, though the server listed it last, and the rest by name.
    assert_eq!(human.status.code(), Some(2), "{}", stderr(&human));
    assert!(human.stdout.is_empty());
    assert_eq!(
        named_runners(&human, &[ALPHA, BRAVO, CHARLIE]),
        ["charlie", "alpha", "bravo"]
    );
    // It ends with the command line that names one of them.
    let text = stderr(&human);
    let last = text.lines().last().unwrap_or_default();
    assert!(
        last.contains("um grant authorize") && last.contains(RUNNER_ID) && last.contains("--yes"),
        "{text}"
    );

    assert_eq!(json.status.code(), Some(2), "{}", stderr(&json));
    let document = json_output(&json);
    assert_eq!(document["outcome"], "runner_required");
    assert_eq!(document["grantId"], NATIVE_GRANT_ID);
    assert_eq!(
        offered(&document),
        [
            (RUNNER_ID.to_owned(), "charlie".into()),
            (ENDED_RUNNER_ID.to_owned(), "alpha".into()),
            (NEW_RUNNER_ID.to_owned(), "bravo".into()),
        ]
    );
    assert_eq!(
        document["runners"][2],
        serde_json::json!({
            "id": NEW_RUNNER_ID,
            "name": "bravo",
            "pool": "builders",
            "connectivity": "online",
            "mode": "draining",
            "organizationId": ORGANIZATION_ID
        })
    );

    // It read the grant and the runners of its organization, and sent nothing.
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            LIST_FIRST_PAGE,
            LIST_ACME_RUNNERS,
            LIST_FIRST_PAGE,
            LIST_ACME_RUNNERS
        ]
    );
}

// REQ-GRANT-81
#[test]
fn deauthorize_without_a_runner_names_only_the_runners_the_grant_is_authorized_for() {
    let (server, _directory, credential) = prepared(vec![
        one(&native_grant_in_use()),
        acme_runners(),
        // An OAuth grant authorized for a runner whose registration is gone.
        one(&oauth_grant(OAUTH_GRANT_ID, "github", "active")),
        registrations(&[]),
    ]);
    let human = run_grant(&server, &credential, &["deauthorize", NATIVE_GRANT_ID]);
    let unregistered = run_grant(
        &server,
        &credential,
        &["deauthorize", OAUTH_GRANT_ID, "--json"],
    );

    assert_eq!(human.status.code(), Some(2), "{}", stderr(&human));
    assert!(human.stdout.is_empty());
    assert_eq!(named_runners(&human, &[CHARLIE]), ["charlie"]);
    let text = stderr(&human);
    for other in [ENDED_RUNNER_ID, NEW_RUNNER_ID] {
        assert!(!text.contains(other), "{other} in\n{text}");
    }

    // A runner no registration is found for is named by its ID alone.
    assert_eq!(unregistered.status.code(), Some(2));
    let document = json_output(&unregistered);
    assert_eq!(document["outcome"], "runner_required");
    assert_eq!(document["grantId"], OAUTH_GRANT_ID);
    assert_eq!(
        offered(&document),
        [(RUNNER_ID.to_owned(), serde_json::Value::Null)]
    );
    assert_eq!(document["runners"][0]["organizationId"], ORGANIZATION_ID);

    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            "GET /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1",
            LIST_ACME_RUNNERS,
            "GET /api/v1/me/grants/pgr_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1",
            LIST_ACME_RUNNERS,
        ]
    );
}

// REQ-GRANT-81
#[test]
fn authorize_all_without_a_runner_names_the_runners_of_every_active_organization() {
    let organizations = || {
        memberships(&[
            (ORGANIZATION_ID, "acme"),
            (GLOBEX_ORGANIZATION_ID, "globex"),
        ])
    };
    let globex = || registrations(&[registration(DELTA, "online", "enabled")]);
    let (server, _directory, credential) = prepared(vec![
        organizations(),
        acme_runners(),
        globex(),
        organizations(),
        acme_runners(),
        globex(),
    ]);
    let human = run_grant(&server, &credential, &["authorize", "--all"]);
    let json = run_grant(&server, &credential, &["authorize", "--all", "--json"]);

    assert_eq!(human.status.code(), Some(2), "{}", stderr(&human));
    assert!(human.stdout.is_empty());
    assert_eq!(
        named_runners(&human, &[ALPHA, BRAVO, CHARLIE, DELTA]),
        ["charlie", "delta", "alpha", "bravo"]
    );

    assert_eq!(json.status.code(), Some(2));
    let document = json_output(&json);
    assert_eq!(document["outcome"], "runner_required");
    // No one grant is named.
    assert!(document.get("grantId").is_none(), "{document}");
    assert_eq!(
        document["runners"][1]["organizationId"],
        GLOBEX_ORGANIZATION_ID
    );
    assert_eq!(offered(&document).len(), 4);

    // The ended membership's organization is not read.
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            LIST_MEMBERSHIPS,
            LIST_ACME_RUNNERS,
            LIST_GLOBEX_RUNNERS,
            LIST_MEMBERSHIPS,
            LIST_ACME_RUNNERS,
            LIST_GLOBEX_RUNNERS,
        ]
    );
}

// REQ-GRANT-81
#[test]
fn a_command_with_no_runner_to_offer_exits_1_with_why() {
    let mut oauth = oauth_grant(OAUTH_GRANT_ID, "github", "active");
    oauth["authorizations"][0]["runnerId"] = ALPHA.0.into();
    // The arguments, Cloud's answers in order, and the outcome.
    type Case<'a> = (&'a [&'a str], Vec<Vec<u8>>, &'a str);
    let cases: [Case; 5] = [
        // The grant's organization holds no runner.
        (
            &["authorize", NATIVE_GRANT_ID],
            vec![
                one(&native_grant(NATIVE_GRANT_ID, "active")),
                registrations(&[]),
            ],
            "runner_not_found",
        ),
        (
            &["authorize", "--all"],
            vec![only_acme(), registrations(&[])],
            "runner_not_found",
        ),
        // The OAuth grant is authorized for every runner of the member's one organization.
        (
            &["authorize", OAUTH_GRANT_ID],
            vec![
                one(&oauth),
                only_acme(),
                registrations(&[registration(ALPHA, "online", "enabled")]),
            ],
            "already_authorized",
        ),
        // A Codex grant serves one runner, and this one is authorized for one already, so no
        // runner is read.
        (
            &["authorize", NATIVE_GRANT_ID],
            vec![one(&native_grant_in_use())],
            "authorized_elsewhere",
        ),
        (
            &["deauthorize", NATIVE_GRANT_ID],
            vec![one(&native_grant(NATIVE_GRANT_ID, "active"))],
            "authorization_not_found",
        ),
    ];
    for (args, responses, outcome) in cases {
        let sent = responses.len();
        let (server, _directory, credential) = prepared([responses.clone(), responses].concat());
        let json = run_grant(&server, &credential, &[args, &["--json"]].concat());
        let human = run_grant(&server, &credential, args);

        assert_eq!(json.status.code(), Some(1), "{outcome}: {}", stderr(&json));
        assert_eq!(json_output(&json)["outcome"], outcome);
        assert_eq!(human.status.code(), Some(1), "{outcome}");
        assert!(human.stdout.is_empty(), "{outcome}");
        let requests = server.finish();
        assert_member_requests(&requests);
        assert_eq!(requests.len(), 2 * sent, "{outcome}");
        assert!(
            requests.iter().all(|request| request.starts_with("GET ")),
            "{outcome}"
        );
    }
}

// `um grant authorize` and `deauthorize` given no `RUNNER` on a terminal, where they show the
// runners as a list to choose from (REQ-GRANT-82).

/// `um grant` with `args`, to run on a terminal: as the leader of a new session whose
/// controlling terminal is its standard input, the terminal the list measures to draw on. The
/// CLI test environment includes Python.
fn grant_on_terminal(server: &ScriptedServer, credential: &Path, args: &[&str]) -> Command {
    let mut command = Command::new("python3");
    command
        .args([
            "-c",
            "import fcntl, os, sys, termios; os.setsid(); \
             fcntl.ioctl(0, termios.TIOCSCTTY, 0); os.execv(sys.argv[1], sys.argv[1:])",
            env!("CARGO_BIN_EXE_um"),
            "grant",
        ])
        .args(args)
        .arg("--allow-insecure-http");
    for variable in DEPLOYMENT_VARIABLES
        .into_iter()
        .chain(RUNNER_TELEMETRY_VARIABLES)
    {
        command.env_remove(variable);
    }
    command.envs(deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential.to_str().unwrap(),
    ));
    command
}

const AUTHORIZE_NATIVE: &str =
    "POST /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations HTTP/1.1";
// Ctrl-C, as the list reads it from a terminal in raw mode.
const INTERRUPT: &str = "\x03";

// REQ-GRANT-82
// INV-CLI-16
#[test]
fn authorize_on_a_terminal_without_a_runner_chooses_it_from_the_list_then_asks() {
    // The keys typed at the list and at the question, whether `--yes` is given, and whether
    // the runner is authorized; the exit status follows.
    let cases: [(&str, Option<&str>, bool, bool); 5] = [
        ("\r", Some("y\n"), false, true),
        ("\r", Some("n\n"), false, false),
        ("\r", None, true, true),
        // Esc leaves the list without a choice.
        ("\x1b", None, false, false),
        // Ctrl-C at the list interrupts the command.
        (INTERRUPT, None, false, false),
    ];
    for (at_list, at_question, yes, authorized_runner) in cases {
        let case = format!("{at_list:?} {at_question:?} --yes {yes}");
        let mut responses = vec![
            page(&[native_grant(NATIVE_GRANT_ID, "active")], None),
            // An offline runner, an online disabled one, and the one online and enabled.
            registrations(&[
                registration(ALPHA, "offline", "enabled"),
                registration(BRAVO, "online", "disabled"),
                registration(CHARLIE, "online", "enabled"),
            ]),
        ];
        if authorized_runner {
            responses.push(authorized(NATIVE_GRANT_ID, RUNNER_ID));
        }
        let (server, _directory, credential) = prepared(responses);
        let mut args = vec!["authorize", "codex"];
        if yes {
            args.push("--yes");
        }
        // The list is shown once its last runner is.
        let mut steps = vec![(NEW_RUNNER_ID, at_list)];
        steps.extend(at_question.map(|answer| ("[y/N]", answer)));
        let (shown, output) = on_terminal(grant_on_terminal(&server, &credential, &args), &steps);

        let requests = server.finish();
        assert_member_requests(&requests);
        let mut expected = vec![LIST_FIRST_PAGE, LIST_ACME_RUNNERS];
        if authorized_runner {
            expected.push(AUTHORIZE_NATIVE);
        }
        assert_eq!(request_lines(&requests), expected, "{case}\n{shown}");
        let text = stdout(&output);
        // The list is drawn on the terminal, standard error, and standard output holds only the
        // result: the authorization of the runner chosen, the first, online and enabled.
        for listed in [ENDED_RUNNER_ID, NEW_RUNNER_ID, "alpha", "bravo"] {
            assert!(!text.contains(listed), "{case}: {listed} in\n{text}");
        }
        if authorized_runner {
            assert_eq!(output.status.code(), Some(0), "{case}\n{shown}");
            assert_eq!(
                request_body(&requests[2]),
                serde_json::json!({"runnerId": RUNNER_ID})
            );
            assert!(
                text.contains("aga_01k0z6r1w8f4jy2m7q9v3x5abf"),
                "{case}: {text}"
            );
        } else {
            let status = if at_list == INTERRUPT { 130 } else { 2 };
            assert_eq!(output.status.code(), Some(status), "{case}\n{shown}");
            assert!(text.is_empty(), "{case}: {text}");
        }
        // With `--yes`, or with no runner chosen, no question is asked.
        assert_eq!(
            shown.contains("[y/N]"),
            at_question.is_some(),
            "{case}\n{shown}"
        );
        if at_question.is_some() {
            // The list comes first, then the statement, naming the grant and the runner chosen
            // by its name and ID, then the question.
            let before = &shown[..shown.find("[y/N]").unwrap()];
            let heading = before.rfind(NATIVE_GRANT_ID).unwrap();
            let start = before[..heading].rfind('\n').map_or(0, |at| at + 1);
            let end = before[heading..]
                .find('\n')
                .map_or(before.len(), |at| heading + at);
            let line = &before[start..end];
            assert!(
                before.find(ENDED_RUNNER_ID).unwrap() < start,
                "{case}\n{shown}"
            );
            assert!(
                line.contains("charlie") && line.contains(RUNNER_ID),
                "{case}: {line:?}"
            );
        }
    }
}

// REQ-GRANT-81
// INV-CLI-16
#[test]
fn json_on_a_terminal_shows_no_list_and_names_the_runners_offered() {
    let (server, _directory, credential) = prepared(vec![
        page(&[native_grant(NATIVE_GRANT_ID, "active")], None),
        acme_runners(),
    ]);
    let command = grant_on_terminal(&server, &credential, &["authorize", "codex", "--json"]);
    // Nothing is typed: with `--json` the command never prompts, on a terminal too.
    let (shown, output) = on_terminal(command, &[]);

    assert_eq!(output.status.code(), Some(2), "{shown}");
    let document = json_output(&output);
    assert_eq!(document["outcome"], "runner_required");
    assert_eq!(offered(&document).len(), 3);
    assert!(!shown.contains(RUNNER_ID), "{shown}");
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_FIRST_PAGE, LIST_ACME_RUNNERS]
    );
}

// REQ-GRANT-82
// INV-CLI-16
#[test]
fn deauthorize_on_a_terminal_without_a_runner_offers_only_the_authorized_runner() {
    let (server, _directory, credential) = prepared(vec![
        one(&native_grant_in_use()),
        acme_runners(),
        deleted(),
        one(&native_grant_deauthorized()),
    ]);
    let command = grant_on_terminal(&server, &credential, &["deauthorize", NATIVE_GRANT_ID]);
    let (shown, output) = on_terminal(command, &[(RUNNER_ID, "\r"), ("[y/N]", "y\n")]);

    assert_eq!(output.status.code(), Some(0), "{shown}");
    for other in [ENDED_RUNNER_ID, NEW_RUNNER_ID] {
        assert!(!shown.contains(other), "{other} offered in\n{shown}");
    }
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            "GET /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1",
            LIST_ACME_RUNNERS,
            DEAUTHORIZE_NATIVE,
            "GET /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1",
        ]
    );
}

// `um grant authorize` and `deauthorize` given no `RUNNER` on a terminal, offered more than one
// runner, also offer all of them as one choice (REQ-GRANT-83).

// How the list's last entry, the one that stands for all the runners, begins.
const ALL_RUNNERS: &str = "All ";
// The keys that move down the list `count` entries.
fn down(count: usize) -> String {
    "\x1b[B".repeat(count)
}

// A Claude Code grant, which can serve several runners, authorized for none.
fn claude_code_grant() -> serde_json::Value {
    let mut grant = native_grant(NATIVE_GRANT_ID, "active");
    grant["provider"] = "claude_code".into();
    grant
}

// Acme's three runners, all online and enabled, so offered by name: alpha, bravo, charlie.
fn three_ready_runners() -> Vec<u8> {
    registrations(&[
        registration(CHARLIE, "online", "enabled"),
        registration(ALPHA, "online", "enabled"),
        registration(BRAVO, "online", "enabled"),
    ])
}

// REQ-GRANT-83
#[test]
fn authorize_with_all_runners_chosen_authorizes_each_under_its_own_key() {
    let runners = [ENDED_RUNNER_ID, NEW_RUNNER_ID, RUNNER_ID];
    // Whether Cloud refuses the second runner, which holds another Claude Code login.
    for refused in [false, true] {
        let second = if refused {
            problem("409 Conflict", 409, "grant-runner-conflict")
        } else {
            authorized(NATIVE_GRANT_ID, NEW_RUNNER_ID)
        };
        let (server, _directory, credential) = prepared(vec![
            page(&[claude_code_grant()], None),
            three_ready_runners(),
            authorized(NATIVE_GRANT_ID, ENDED_RUNNER_ID),
            second,
            authorized(NATIVE_GRANT_ID, RUNNER_ID),
        ]);
        let command =
            grant_on_terminal(&server, &credential, &["authorize", "claude_code", "--yes"]);
        // The last runner offered is charlie; the entry for all follows it.
        let (shown, output) = on_terminal(command, &[(RUNNER_ID, &format!("{}\r", down(3)))]);

        assert!(shown.contains(ALL_RUNNERS), "{shown}");
        let requests = server.finish();
        assert_member_requests(&requests);
        assert_eq!(
            request_lines(&requests),
            [
                LIST_FIRST_PAGE,
                LIST_ACME_RUNNERS,
                AUTHORIZE_NATIVE,
                AUTHORIZE_NATIVE,
                AUTHORIZE_NATIVE
            ],
            "{shown}"
        );
        let posts = &requests[2..];
        for (request, runner_id) in posts.iter().zip(runners) {
            assert_eq!(
                request_body(request),
                serde_json::json!({"runnerId": runner_id})
            );
        }
        let keys: std::collections::BTreeSet<_> = posts
            .iter()
            .map(|request| header_value(request, "idempotency-key"))
            .collect();
        assert_eq!(keys.len(), 3, "one key per runner");
        assert!(!keys.contains(""));
        // One outcome per runner: the authorization of each runner authorized on standard
        // output, the refusal on the terminal naming the runner refused, with its remedy.
        let text = stdout(&output);
        for runner_id in runners {
            assert_eq!(
                text.contains(runner_id),
                !refused || runner_id != NEW_RUNNER_ID,
                "{runner_id} in\n{text}"
            );
        }
        if refused {
            assert_eq!(output.status.code(), Some(1), "{shown}");
            let after = &shown[shown.rfind(NEW_RUNNER_ID).unwrap()..];
            assert!(
                after.contains("um grant deauthorize GRANT RUNNER --yes"),
                "{shown}"
            );
        } else {
            assert_eq!(output.status.code(), Some(0), "{shown}");
        }
    }
}

// REQ-GRANT-83
#[test]
fn a_failure_that_is_not_one_runners_refusal_ends_the_command_at_that_runner() {
    // Alpha is authorized, bravo's request is rate limited, and charlie after it is never
    // tried.
    let (server, _directory, credential) = prepared(vec![
        page(&[claude_code_grant()], None),
        three_ready_runners(),
        authorized(NATIVE_GRANT_ID, ENDED_RUNNER_ID),
        rate_limited(),
    ]);
    let command = grant_on_terminal(&server, &credential, &["authorize", "claude_code", "--yes"]);
    let (shown, output) = on_terminal(command, &[(RUNNER_ID, &format!("{}\r", down(3)))]);

    assert_eq!(output.status.code(), Some(1), "{shown}");
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            LIST_FIRST_PAGE,
            LIST_ACME_RUNNERS,
            AUTHORIZE_NATIVE,
            AUTHORIZE_NATIVE
        ],
        "{shown}"
    );
    // The outcome before the failure is printed, then the failure, naming the runner it ended
    // at: after the statement, which names the grant, the terminal names bravo alone.
    let text = stdout(&output);
    assert!(text.contains(ENDED_RUNNER_ID), "{text}");
    assert!(!text.contains(NEW_RUNNER_ID), "{text}");
    let after = &shown[shown.rfind(NATIVE_GRANT_ID).unwrap()..];
    assert!(after.contains(NEW_RUNNER_ID), "{shown}");
    assert!(!after.contains(RUNNER_ID), "{shown}");
}

// REQ-GRANT-83
#[test]
fn deauthorize_with_all_runners_chosen_ends_each_authorization() {
    let mut grant = claude_code_grant();
    grant["authorizations"] = serde_json::json!([
        {
            "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": RUNNER_ID,
            "organizationId": ORGANIZATION_ID,
            "createdAt": "2026-11-02T10:05:00Z"
        },
        {
            "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abe",
            "runnerId": NEW_RUNNER_ID,
            "organizationId": ORGANIZATION_ID,
            "createdAt": "2026-11-02T10:06:00Z"
        }
    ]);
    let (server, _directory, credential) = prepared(vec![
        one(&grant),
        acme_runners(),
        deleted(),
        one(&grant),
        deleted(),
        one(&grant),
    ]);
    let command = grant_on_terminal(
        &server,
        &credential,
        &["deauthorize", NATIVE_GRANT_ID, "--yes"],
    );
    // Offered charlie, online and enabled, then bravo, then the entry for all.
    let (shown, output) = on_terminal(command, &[(NEW_RUNNER_ID, &format!("{}\r", down(2)))]);

    assert_eq!(output.status.code(), Some(0), "{shown}");
    let read = "GET /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1";
    let requests = server.finish();
    assert_member_requests(&requests);
    assert_eq!(
        request_lines(&requests),
        [
            read,
            LIST_ACME_RUNNERS,
            DEAUTHORIZE_NATIVE,
            read,
            "DELETE /api/v1/me/grants/agl_01k0z6r1w8f4jy2m7q9v3x5abc/authorizations/aga_01k0z6r1w8f4jy2m7q9v3x5abe HTTP/1.1",
            read,
        ],
        "{shown}"
    );
    assert_ne!(
        header_value(&requests[2], "idempotency-key"),
        header_value(&requests[4], "idempotency-key")
    );
    let text = stdout(&output);
    for runner_id in [RUNNER_ID, NEW_RUNNER_ID] {
        assert!(text.contains(runner_id), "{runner_id} not in\n{text}");
    }
}

// REQ-GRANT-83
#[test]
fn a_codex_grants_list_and_a_list_of_one_runner_offer_no_choice_of_all() {
    let one_runner = || registrations(&[registration(CHARLIE, "online", "enabled")]);
    let cases = [
        (
            native_grant(NATIVE_GRANT_ID, "active"),
            three_ready_runners(),
        ),
        (claude_code_grant(), one_runner()),
    ];
    for (grant, runners) in cases {
        let provider = grant["provider"].as_str().unwrap().to_owned();
        let (server, _directory, credential) = prepared(vec![page(&[grant], None), runners]);
        let command = grant_on_terminal(&server, &credential, &["authorize", &provider]);
        // Esc leaves the list once its last runner, charlie, is shown.
        let (shown, output) = on_terminal(command, &[(RUNNER_ID, "\x1b")]);

        assert_eq!(output.status.code(), Some(2), "{provider}: {shown}");
        assert!(!shown.contains(ALL_RUNNERS), "{provider}: {shown}");
        let requests = server.finish();
        assert_eq!(
            request_lines(&requests),
            [LIST_FIRST_PAGE, LIST_ACME_RUNNERS],
            "{provider}"
        );
    }
}

// `um grant create`. Each case sets in its own environment whether this machine has a
// browser, so no case can open a real one: the only opener on `PATH` is a fake that records
// what it is given.

const SESSION_ID: &str = "pgs_01k0z6r1w8f4jy2m7q9v3x5abc";
const CONSENT_URL: &str = "https://linear.app/oauth/authorize?state=public-fixture-state";
const OPENER: &str = if cfg!(target_os = "macos") {
    "open"
} else {
    "xdg-open"
};
// Every variable the browser rule reads besides `PATH`, removed from each child so that only
// the case decides.
const BROWSER_VARIABLES: [&str; 5] = [
    "SSH_CONNECTION",
    "SSH_CLIENT",
    "SSH_TTY",
    "DISPLAY",
    "WAYLAND_DISPLAY",
];
const LIST_OAUTH: &str = "GET /api/v1/me/grants?kind=oauth&limit=200 HTTP/1.1";
const START_CONSENT: &str = "POST /api/v1/me/grants HTTP/1.1";

/// Where `um grant create` runs.
#[derive(Clone, Copy, Debug)]
pub(super) enum Place {
    /// No SSH variable, and on Linux a display.
    Desktop,
    /// A remote shell, which forwards a display on Linux.
    Remote,
    /// No SSH variable and no display.
    Headless,
}

/// A directory holding only the fake opener, which records how many arguments it was given
/// and the first, then exits with its status.
pub(super) struct Opener {
    directory: tempfile::TempDir,
}

impl Opener {
    pub(super) fn new(status: u8) -> Self {
        Self::running(&format!("exit {status}"))
    }

    /// An opener that, like `xdg-open` running a browser in the foreground, does not return
    /// until the browser quits: it records its process id in `pid` and sleeps.
    fn lingering() -> Self {
        use super::pi_installation::quote;
        Self::running(&format!(
            "PATH={}\nexport PATH\necho \"$$\" > pid\nexec sleep 600",
            quote(&std::env::var("PATH").unwrap_or_default())
        ))
    }

    // An opener that records its arguments, then runs `rest` in its directory.
    fn running(rest: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let script = directory.path().join(OPENER);
        fs::write(
            &script,
            format!(
                "#!/bin/sh\ncd '{}'\nprintf '%s %s\\n' \"$#\" \"$1\" >> opened\n{rest}\n",
                directory.path().display()
            ),
        )
        .unwrap();
        fs::set_permissions(&script, Permissions::from_mode(0o755)).unwrap();
        Self { directory }
    }

    /// Ends a lingering opener once it has recorded its process id.
    fn end(&self) {
        let record = self.directory.path().join("pid");
        let pid = poll_until(
            "the opener recording its process id",
            || fs::read_to_string(&record).unwrap_or_default(),
            |pid| pid.ends_with('\n'),
        );
        let _ = rustix::process::kill_process(
            rustix::process::Pid::from_raw(pid.trim().parse().unwrap()).unwrap(),
            rustix::process::Signal::KILL,
        );
    }

    /// What the openers recorded, sorted, once at least `count` have: `um` starts an opener
    /// and goes on without it, so an opener may record after `um` has returned.
    pub(super) fn opened(&self, count: usize) -> Vec<String> {
        let mut opened = poll_until(
            "the opener recording its arguments",
            || {
                fs::read_to_string(self.directory.path().join("opened"))
                    .map(|record| record.lines().map(str::to_owned).collect::<Vec<_>>())
                    .unwrap_or_default()
            },
            |opened| opened.len() >= count,
        );
        opened.sort();
        opened
    }
}

/// The leading words of `um grant create`.
const GRANT_CREATE: &[&str] = &["grant", "create"];
/// The leading words of `um runner doctor`.
const RUNNER_DOCTOR: &[&str] = &["runner", "doctor"];

fn create_command(
    server: &ScriptedServer,
    credential: &Path,
    opener: &Opener,
    place: Place,
    args: &[&str],
) -> Command {
    opener_command(
        server,
        credential,
        opener,
        place,
        &[GRANT_CREATE, args].concat(),
    )
}

/// `um` with `arguments`, where `place` decides whether this machine has a browser and the
/// only opener on `PATH` is `opener`.
pub(super) fn opener_command(
    server: &ScriptedServer,
    credential: &Path,
    opener: &Opener,
    place: Place,
    arguments: &[&str],
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command.args(arguments).arg("--allow-insecure-http");
    for variable in DEPLOYMENT_VARIABLES
        .into_iter()
        .chain(RUNNER_TELEMETRY_VARIABLES)
        .chain(BROWSER_VARIABLES)
    {
        command.env_remove(variable);
    }
    command
        .envs(deployment_environment_with_issuer(
            &server.api_url,
            &server.issuer,
            credential.to_str().unwrap(),
        ))
        .env("PATH", opener.directory.path());
    let display = cfg!(target_os = "linux").then_some(("DISPLAY", ":0"));
    match place {
        Place::Desktop => command.envs(display),
        Place::Remote => command
            .envs(display)
            .env("SSH_CONNECTION", "198.51.100.7 52000 192.0.2.10 22"),
        Place::Headless => &mut command,
    };
    command
}

/// Runs `um grant create` with standard input that is not a terminal.
fn run_create(
    server: &ScriptedServer,
    credential: &Path,
    opener: &Opener,
    place: Place,
    args: &[&str],
) -> Output {
    let output = create_command(server, credential, opener, place, args)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_no_secret_output(&output, &[SENTINEL, TOKEN]);
    output
}

// The Linear consent as the API presents it, carrying material the CLI must drop.
fn session(status: &str) -> serde_json::Value {
    let mut session = serde_json::json!({
        "id": SESSION_ID,
        "provider": "linear",
        "status": status,
        "createdAt": "2026-11-13T08:00:00Z",
        "expiresAt": "2026-11-13T08:15:00Z",
        "accessToken": SENTINEL
    });
    match status {
        "pending" => session["authorizationUrl"] = CONSENT_URL.into(),
        "completed" => {
            session["completedAt"] = "2026-11-13T08:01:00Z".into();
            session["resultGrant"] = serde_json::json!({
                "id": OAUTH_GRANT_ID,
                "identityId": IDENTITY_ID,
                "provider": "linear",
                "scopes": ["read"],
                "state": "active",
                "createdAt": "2026-11-13T08:01:00Z",
                "accessToken": SENTINEL
            });
        }
        _ => {}
    }
    session
}

fn started() -> Vec<u8> {
    json_http_response("201 Created", session("pending"))
}

fn polled(session: serde_json::Value) -> Vec<u8> {
    json_http_response("200 OK", session)
}

fn nothing_held() -> Vec<u8> {
    page(&[], None)
}

fn events(output: &Output) -> Vec<serde_json::Value> {
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

fn poll_line() -> String {
    format!("GET /api/v1/me/grant-sessions/{SESSION_ID} HTTP/1.1")
}

// REQ-GRANT-69
#[test]
fn create_starts_the_consent_opens_its_url_and_waits_for_the_grant() {
    // An opener that fails changes nothing: the URL is printed whatever it does.
    for (status, json) in [(0, false), (0, true), (1, false)] {
        let (server, _directory, credential) = prepared(vec![
            nothing_held(),
            started(),
            polled(session("pending")),
            polled(session("completed")),
        ]);
        let opener = Opener::new(status);
        let mut args = vec!["linear"];
        if json {
            args.push("--json");
        }
        let output = run_create(&server, &credential, &opener, Place::Desktop, &args);

        assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
        assert_eq!(opener.opened(1), [format!("1 {CONSENT_URL}")]);
        if json {
            let events = events(&output);
            assert_eq!(events.len(), 2);
            assert_eq!(events[0]["event"], "consent_required");
            assert_eq!(events[0]["sessionId"], SESSION_ID);
            assert_eq!(events[0]["provider"], "linear");
            assert_eq!(events[0]["authorizationUrl"], CONSENT_URL);
            assert_eq!(events[1]["event"], "consent_result");
            assert_eq!(events[1]["outcome"], "completed");
            assert_eq!(events[1]["grant"]["id"], OAUTH_GRANT_ID);
            assert_eq!(events[1]["grant"]["provider"], "linear");
        } else {
            let text = stdout(&output);
            assert!(text.contains(CONSENT_URL), "{text}");
            assert!(text.contains(&format!("grant: {OAUTH_GRANT_ID}")), "{text}");
        }
        let requests = server.finish();
        assert_member_requests(&requests);
        let poll = poll_line();
        assert_eq!(
            request_lines(&requests),
            [LIST_OAUTH, START_CONSENT, poll.as_str(), poll.as_str()]
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                requests[1].split_once("\r\n\r\n").unwrap().1
            )
            .unwrap(),
            serde_json::json!({"provider": "linear"})
        );
        assert!(!header_value(&requests[1], "idempotency-key").is_empty());
    }
}

// REQ-GRANT-69
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the bound on um's return is far below the opener's own life, so real time only separates returning from waiting for the opener"
)]
fn an_opener_that_waits_for_the_browser_to_quit_does_not_keep_the_consent_waiting() {
    let mut responses = vec![nothing_held()];
    responses.extend(approved("github"));
    let (server, _directory, credential) = prepared(responses);
    let opener = Opener::lingering();
    let child = create_command(
        &server,
        &credential,
        &opener,
        Place::Desktop,
        &["github", "--json"],
    )
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();

    let (exited, exit) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let output = child.wait_with_output();
        exited.send(()).ok();
        output
    });
    let returned = exit.recv_timeout(Duration::from_secs(20)).is_ok();
    opener.end();
    assert!(returned, "um waited for the opener to return");
    let output = waiter.join().unwrap().unwrap();

    assert_no_secret_output(&output, &[SENTINEL, TOKEN]);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let events = events(&output);
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["event"], "consent_result");
    assert_eq!(events[1]["outcome"], "completed");
    assert_eq!(events[1]["grant"]["provider"], "github");
    assert_eq!(opener.opened(1), [format!("1 {}", consent_url("github"))]);
    let poll = poll_line();
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_OAUTH, START_CONSENT, poll.as_str()]
    );
}

// REQ-GRANT-69
#[test]
fn create_prints_the_url_without_opening_it_where_this_machine_has_no_browser() {
    // Only Linux needs a display for a browser.
    for (place, opened) in [
        (Place::Remote, false),
        (Place::Headless, !cfg!(target_os = "linux")),
    ] {
        let (server, _directory, credential) = prepared(vec![nothing_held(), started()]);
        let opener = Opener::new(0);
        let output = run_create(
            &server,
            &credential,
            &opener,
            place,
            &["linear", "--no-wait"],
        );

        assert_eq!(output.status.code(), Some(0), "{place:?}");
        assert!(stdout(&output).contains(CONSENT_URL), "{place:?}");
        assert_eq!(
            opener.opened(usize::from(opened)).len(),
            usize::from(opened),
            "{place:?}"
        );
        assert_eq!(request_lines(&server.finish()), [LIST_OAUTH, START_CONSENT]);
    }
}

// REQ-GRANT-69
#[test]
fn create_without_waiting_returns_after_the_start_event() {
    let (server, _directory, credential) = prepared(vec![nothing_held(), started()]);
    let opener = Opener::new(0);
    let output = run_create(
        &server,
        &credential,
        &opener,
        Place::Desktop,
        &["linear", "--no-wait", "--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let events = events(&output);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event"], "consent_required");
    assert_eq!(events[0]["outcome"], "pending");
    assert_eq!(events[0]["authorizationUrl"], CONSENT_URL);
    assert_eq!(opener.opened(1), [format!("1 {CONSENT_URL}")]);
    assert_eq!(request_lines(&server.finish()), [LIST_OAUTH, START_CONSENT]);
}

// REQ-GRANT-69
#[test]
fn create_reports_a_consent_that_did_not_complete_with_its_reason() {
    for (status, reason) in [("failed", "consent_denied"), ("expired", "session_expired")] {
        let mut settled = session(status);
        settled["reasonCode"] = reason.into();
        let responses = vec![nothing_held(), started(), polled(settled)];
        let (server, _directory, credential) = prepared([responses.clone(), responses].concat());
        let opener = Opener::new(0);
        let json = run_create(
            &server,
            &credential,
            &opener,
            Place::Remote,
            &["linear", "--json"],
        );
        let human = run_create(&server, &credential, &opener, Place::Remote, &["linear"]);

        assert_eq!(json.status.code(), Some(1), "{status}");
        let events = events(&json);
        assert_eq!(events.len(), 2, "{status}");
        assert_eq!(events[1]["event"], "consent_result");
        assert_eq!(events[1]["outcome"], status);
        assert_eq!(events[1]["reason"], reason);
        assert!(events[1]["grant"].is_null());
        assert_eq!(human.status.code(), Some(1), "{status}");
        assert!(
            stdout(&human).contains(&format!("reason: {reason}")),
            "{status}"
        );
        assert_eq!(server.finish().len(), 6, "{status}");
    }
}

// REQ-GRANT-69
#[test]
fn create_reports_a_refused_consent_without_polling() {
    for (slug, status, code, outcome, exit) in [
        (
            "provider-unavailable",
            "503 Service Unavailable",
            503,
            "provider_unavailable",
            4,
        ),
        (
            "grant-session-conflict",
            "409 Conflict",
            409,
            "consent_in_progress",
            1,
        ),
    ] {
        let (server, _directory, credential) =
            prepared(vec![nothing_held(), problem(status, code, slug)]);
        let opener = Opener::new(0);
        let output = run_create(
            &server,
            &credential,
            &opener,
            Place::Desktop,
            &["linear", "--json"],
        );

        assert_eq!(output.status.code(), Some(exit), "{outcome}");
        let events = events(&output);
        assert_eq!(events.len(), 1, "{outcome}");
        assert_eq!(events[0]["outcome"], outcome);
        assert!(opener.opened(0).is_empty(), "{outcome}");
        assert_eq!(
            request_lines(&server.finish()),
            [LIST_OAUTH, START_CONSENT],
            "{outcome}"
        );
    }
}

// REQ-GRANT-73
#[test]
fn create_without_a_terminal_replaces_a_held_grant_only_with_replace() {
    for state in ["active", "reconnect_required"] {
        let held = page(
            &[
                oauth_grant(REVOKED_OAUTH_GRANT_ID, "github", "active"),
                oauth_grant(OAUTH_GRANT_ID, "linear", state),
            ],
            None,
        );
        let (server, _directory, credential) = prepared(vec![held.clone(), held, started()]);
        let opener = Opener::new(0);
        let refused = run_create(&server, &credential, &opener, Place::Remote, &["linear"]);
        let replaced = run_create(
            &server,
            &credential,
            &opener,
            Place::Remote,
            &["linear", "--replace", "--no-wait", "--json"],
        );

        assert_eq!(refused.status.code(), Some(2), "{state}");
        let text = stderr(&refused);
        assert!(text.contains("--replace"), "{state}: {text}");
        assert!(text.contains(OAUTH_GRANT_ID), "{state}: {text}");
        assert!(refused.stdout.is_empty(), "{state}");
        assert_eq!(replaced.status.code(), Some(0), "{state}");
        assert_eq!(events(&replaced)[0]["sessionId"], SESSION_ID);
        assert_eq!(
            request_lines(&server.finish()),
            [LIST_OAUTH, LIST_OAUTH, START_CONSENT],
            "{state}"
        );
    }
}

// REQ-GRANT-73
#[test]
fn create_does_not_hold_a_revoked_grant_or_one_of_another_provider_against_a_consent() {
    let (server, _directory, credential) = prepared(vec![
        page(
            &[
                oauth_grant(OAUTH_GRANT_ID, "github", "active"),
                oauth_grant(REVOKED_OAUTH_GRANT_ID, "linear", "revoked"),
            ],
            None,
        ),
        started(),
    ]);
    let opener = Opener::new(0);
    let output = run_create(
        &server,
        &credential,
        &opener,
        Place::Remote,
        &["linear", "--no-wait"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(request_lines(&server.finish()), [LIST_OAUTH, START_CONSENT]);
}

/// Runs `command` with a pseudo-terminal as its standard input and standard error, waits for
/// its `[y/N]` question, answers it with `answer`, and returns what the terminal showed up to
/// the question and the command's output.
fn answered(command: Command, answer: &str) -> (String, Output) {
    const QUESTION: &str = "[y/N]";
    let (shown, output) = on_terminal(command, &[(QUESTION, answer)]);
    let asked = shown
        .find(QUESTION)
        .map_or(shown.len(), |at| at + QUESTION.len());
    (shown[..asked].to_owned(), output)
}

/// Where `needle` first appears in `haystack`.
fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Runs `command` with a pseudo-terminal of a real size as its standard input and standard
/// error and its standard output piped. At each step it waits until the terminal shows the
/// step's text, after what the step before waited for, then types the step's keys. Returns all
/// the terminal showed until the command ended, and the command's output.
fn on_terminal(mut command: Command, steps: &[(&str, &str)]) -> (String, Output) {
    let size = rustix::termios::Winsize {
        ws_row: 40,
        ws_col: 160,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty = open_test_pty(Some(&size)).unwrap();
    command
        .stdin(Stdio::from(pty.slave.try_clone().unwrap()))
        .stderr(Stdio::from(pty.slave))
        .stdout(Stdio::piped());
    let child = command.spawn().unwrap();
    // The command holds this process's copies of the terminal's peer; once they close,
    // reading the terminal ends when the child exits.
    drop(command);
    let (sender, chunks) = mpsc::channel();
    let mut reader = fs::File::from(rustix::io::dup(&pty.master).unwrap());
    let reading = thread::spawn(move || {
        let mut buffer = [0; 512];
        while let Ok(count) = reader.read(&mut buffer) {
            if count == 0 || sender.send(buffer[..count].to_vec()).is_err() {
                break;
            }
        }
    });
    let mut shown = Vec::new();
    let mut seen = 0;
    let mut writer = fs::File::from(pty.master);
    for (text, keys) in steps {
        let at = loop {
            if let Some(at) = find_bytes(&shown[seen..], text.as_bytes()) {
                break seen + at;
            }
            let chunk = chunks.recv().unwrap_or_else(|_| {
                panic!(
                    "the terminal never showed {text:?}:\n{}",
                    String::from_utf8_lossy(&shown)
                )
            });
            shown.extend(chunk);
        };
        seen = at + text.len();
        writer.write_all(keys.as_bytes()).unwrap();
    }
    let output = child.wait_with_output().unwrap();
    drop(writer);
    shown.extend(chunks.iter().flatten());
    reading.join().unwrap();
    (String::from_utf8_lossy(&shown).into_owned(), output)
}

// REQ-GRANT-73
#[test]
fn create_on_a_terminal_asks_before_it_replaces_a_held_grant() {
    for (answer, replaced) in [("y\n", true), ("n\n", false)] {
        let mut responses = vec![page(
            &[oauth_grant(OAUTH_GRANT_ID, "linear", "active")],
            None,
        )];
        if replaced {
            responses.push(started());
        }
        let (server, _directory, credential) = prepared(responses);
        let opener = Opener::new(0);
        let command = create_command(
            &server,
            &credential,
            &opener,
            Place::Remote,
            &["linear", "--no-wait"],
        );
        let (transcript, output) = answered(command, answer);

        assert!(transcript.contains(OAUTH_GRANT_ID), "{transcript}");
        assert_eq!(
            output.status.code(),
            Some(if replaced { 0 } else { 2 }),
            "{answer:?}"
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).contains(CONSENT_URL),
            replaced
        );
        let requests = server.finish();
        let expected: &[&str] = if replaced {
            &[LIST_OAUTH, START_CONSENT]
        } else {
            &[LIST_OAUTH]
        };
        assert_eq!(request_lines(&requests), expected, "{answer:?}");
    }
}

// `um grant create codex`: Codex's own sign-in in a private home, or an auth.json the member
// names, imported for one organization. The fake `codex` and the member's home with its
// ambient Codex login are the `um auth agent-login` tests'.

const GLOBEX_ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abe";
const THIRD_NATIVE_GRANT_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abe";
const LIST_MEMBERSHIPS: &str = "GET /api/v1/me/memberships?limit=200 HTTP/1.1";
const LIST_NATIVE: &str = "GET /api/v1/me/grants?kind=native&limit=200 HTTP/1.1";
const IMPORT_GRANT: &str = "POST /api/v1/me/grant-imports HTTP/1.1";
const READ_ACME: &str = "GET /api/v1/organizations/acme HTTP/1.1";

fn replace_line(grant_id: &str) -> String {
    format!("POST /api/v1/me/grants/{grant_id}/generations HTTP/1.1")
}

// The member's memberships: an active one in each organization named, and an ended one, which
// names no slug and makes no organization the member's.
fn memberships(organizations: &[(&str, &str)]) -> Vec<u8> {
    let mut items: Vec<_> = organizations
        .iter()
        .enumerate()
        .map(|(index, (id, slug))| {
            serde_json::json!({
                "id": format!("mbr_01k0z6r1w8f4jy2m7q9v3x5ab{index}"),
                "organizationId": id,
                "organizationState": "active",
                "organizationDisplayName": slug,
                "organizationSlug": slug,
                "role": "member",
                "state": "active",
                "createdAt": "2026-11-01T08:00:00Z",
                "updatedAt": "2026-11-01T08:00:00Z"
            })
        })
        .collect();
    items.push(serde_json::json!({
        "id": "mbr_01k0z6r1w8f4jy2m7q9v3x5abz",
        "organizationId": OTHER_ORGANIZATION_ID,
        "organizationState": "active",
        "role": "member",
        "state": "ended",
        "createdAt": "2026-10-01T08:00:00Z",
        "updatedAt": "2026-10-02T08:00:00Z",
        "terminalAt": "2026-10-02T08:00:00Z"
    }));
    json_http_response("200 OK", serde_json::json!({"items": items}))
}

fn only_acme() -> Vec<u8> {
    memberships(&[(ORGANIZATION_ID, "acme")])
}

fn acme() -> Vec<u8> {
    json_http_response(
        "200 OK",
        serde_json::json!({
            "id": ORGANIZATION_ID,
            "state": "active",
            "displayName": "Acme",
            "slug": "acme",
            "createdAt": "2026-11-01T09:00:00Z",
            "updatedAt": "2026-11-01T09:00:00Z"
        }),
    )
}

fn codex_grant(id: &str, organization_id: &str, state: &str) -> serde_json::Value {
    let mut grant = native_grant(id, state);
    grant["organizationId"] = organization_id.into();
    grant
}

// A coding agent's provider and the material the import of its login sends.
type Login = (&'static str, &'static str);
const CODEX: Login = ("codex", CODEX_MATERIAL);
const CLAUDE_CODE: Login = ("claude_code", CLAUDE_TOKEN);

// The grant of the provider an import or a replacement answers with, echoing the request's key.
fn stored(login: Login, status: &str, generation: i64) -> Vec<u8> {
    stored_in(ORGANIZATION_ID, login, status, generation)
}

fn stored_in(
    organization_id: &str,
    (provider, _): Login,
    status: &str,
    generation: i64,
) -> Vec<u8> {
    let mut grant = codex_grant(NATIVE_GRANT_ID, organization_id, "active");
    grant["provider"] = provider.into();
    grant["generation"] = generation.into();
    http_response_with_headers(
        status,
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&with_material(grant)).unwrap(),
    )
}

/// Runs `um grant create` from the member's `home` with the fake `program`, when there is one,
/// and the opener on `PATH`, and standard input `input`. Whatever the outcome, the
/// member's home is as it was, the temporary sign-in directory is gone and no login reached
/// the output (REQ-GRANT-74, INV-CLI-13, INV-CLI-14).
fn run_native(
    server: &ScriptedServer,
    credential: &Path,
    home: &AmbientHome,
    program: Option<&FakeProgram>,
    place: Place,
    args: &[&str],
    input: Input<'_>,
) -> Output {
    let fake = Fake {
        program,
        environment: &[],
    };
    run_fake(server, credential, home, fake, place, args, input)
}

/// The fake sign-in program on `PATH`, when there is one, and the variables that choose how it
/// behaves.
#[derive(Clone, Copy)]
struct Fake<'a> {
    program: Option<&'a FakeProgram>,
    environment: &'a [(&'a str, &'a str)],
}

/// [`run_native`] with the variables of `fake` set besides.
fn run_fake(
    server: &ScriptedServer,
    credential: &Path,
    home: &AmbientHome,
    Fake {
        program,
        environment,
    }: Fake<'_>,
    place: Place,
    args: &[&str],
    input: Input<'_>,
) -> Output {
    let opener = Opener::new(0);
    let programs: Vec<_> = program.into_iter().collect();
    let fakes = Fakes {
        opener: &opener,
        programs: &programs,
        environment,
        checkout: None,
    };
    let arguments = [GRANT_CREATE, args].concat();
    let output = run_programs(server, credential, home, fakes, place, &arguments, input);
    assert!(opener.opened(0).is_empty());
    output
}

/// The opener and the fake sign-in programs on `PATH`, and the variables that choose how they
/// behave, and the checkout the command runs in, if any.
#[derive(Clone, Copy)]
struct Fakes<'a> {
    opener: &'a Opener,
    programs: &'a [&'a FakeProgram],
    environment: &'a [(&'a str, &'a str)],
    checkout: Option<Checkout<'a>>,
}

/// The directory `um` runs in and the directory of the fake `git` it finds there.
#[derive(Clone, Copy)]
struct Checkout<'a> {
    directory: &'a Path,
    git: &'a Path,
}

/// What `um` reads on its standard input.
#[derive(Clone, Copy)]
enum Input<'a> {
    /// Nothing: standard input is null, so it is no terminal.
    Null,
    /// These bytes on a pipe, as `--token-file -` and `--from-file -` read them.
    Piped(&'a [u8]),
    /// A terminal these bytes are typed on, which Claude Code's sign-in needs (REQ-GRANT-71).
    Typed(&'a [u8]),
}

/// Runs `command` with a new terminal as its standard input and its outputs piped, types
/// `typed` on the terminal and waits for it; the terminal stays open until the command ends.
fn typed_output(command: &mut Command, typed: &[u8]) -> Output {
    let pty = open_test_pty(None).unwrap();
    let child = command
        .stdin(Stdio::from(pty.slave))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut terminal = fs::File::from(pty.master);
    terminal.write_all(typed).unwrap();
    let output = child.wait_with_output().unwrap();
    drop(terminal);
    output
}

/// Runs `um` with `arguments`, its command's leading words first, from the member's `home` with
/// `fakes` and standard input `input`, and checks what [`run_native`] promises of its outcome.
fn run_programs(
    server: &ScriptedServer,
    credential: &Path,
    home: &AmbientHome,
    Fakes {
        opener,
        programs,
        environment,
        checkout,
    }: Fakes<'_>,
    place: Place,
    arguments: &[&str],
    input: Input<'_>,
) -> Output {
    let path = std::env::join_paths(
        std::iter::once(opener.directory.path())
            .chain(programs.iter().map(|program| program.bin.as_path()))
            .chain(checkout.map(|checkout| checkout.git)),
    )
    .unwrap();
    let mut command = opener_command(server, credential, opener, place, arguments);
    command
        .env("PATH", path)
        .envs(home.variables())
        .envs(environment.iter().copied());
    if let Some(checkout) = checkout {
        command.current_dir(checkout.directory);
    }
    let output = match input {
        Input::Null => command.stdin(Stdio::null()).output().unwrap(),
        Input::Typed(typed) => typed_output(&mut command, typed),
        Input::Piped(input) => {
            let mut child = command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(input).unwrap();
            child.wait_with_output().unwrap()
        }
    };
    assert_no_secret_output(
        &output,
        &[
            SENTINEL,
            TOKEN,
            CODEX_SENTINEL,
            AMBIENT_MARKER,
            &encoded(CODEX_MATERIAL.as_bytes()),
            CLAUDE_TOKEN_SENTINEL,
            &encoded(CLAUDE_TOKEN.as_bytes()),
        ],
    );
    home.assert_untouched_and_clean();
    output
}

fn assert_imported(request: &str, organization: &str, (provider, material): Login) {
    assert!(
        request.starts_with(&format!("{IMPORT_GRANT}\r\n")),
        "{request}"
    );
    assert!(!header_value(request, "idempotency-key").is_empty());
    assert_eq!(
        request_body(request),
        serde_json::json!({
            "organization": organization,
            "provider": provider,
            "recordSchemaVersion": 1,
            "material": encoded(material.as_bytes())
        })
    );
}

// REQ-GRANT-70
// REQ-GRANT-72
// REQ-GRANT-74
// INV-CLI-13
#[test]
fn create_codex_signs_in_in_a_private_codex_home_and_imports_the_login_it_wrote() {
    for args in [&["codex"][..], &["codex", "--login", "--json"]] {
        let (server, _directory, credential) = prepared(vec![
            serving_every(),
            only_acme(),
            nothing_held(),
            stored(CODEX, "201 Created", 1),
        ]);
        let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
        let home = AmbientHome::new();
        let output = run_native(
            &server,
            &credential,
            &home,
            Some(&codex),
            Place::Desktop,
            args,
            Input::Null,
        );

        assert_eq!(
            output.status.code(),
            Some(0),
            "{args:?}: {}",
            stderr(&output)
        );
        if args.contains(&"--json") {
            let events = events(&output);
            assert_eq!(events.len(), 1, "{events:?}");
            assert_eq!(events[0]["event"], "import_result");
            assert_eq!(events[0]["outcome"], "imported");
            assert_eq!(events[0]["grant"]["id"], NATIVE_GRANT_ID);
            assert_eq!(events[0]["grant"]["generation"], 1);
        } else {
            let text = stdout(&output);
            for fact in [NATIVE_GRANT_ID, "provider: codex", "generation: 1", "acme"] {
                assert!(text.contains(fact), "{fact}: {text}");
            }
        }
        // The sign-in's own output reaches the member on standard error.
        assert!(stderr(&output).contains("codex-sign-in-prompt"));
        assert_eq!(codex.recorded("arguments").unwrap(), "login\n", "{args:?}");
        let codex_home = std::path::PathBuf::from(codex.recorded("home").unwrap());
        assert_ne!(codex_home, home.ambient_codex());
        assert!(codex_home.starts_with(home.temporary.path()));
        assert!(!codex_home.exists());
        assert!(
            codex.recorded("mode").unwrap().starts_with("drwx------"),
            "{:?}",
            codex.recorded("mode")
        );
        let requests = server.finish();
        assert_eq!(
            request_lines(&requests),
            [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE, IMPORT_GRANT],
            "{args:?}"
        );
        assert_imported(&requests[3], ORGANIZATION_ID, CODEX);
    }
}

// REQ-GRANT-70
#[test]
fn create_codex_signs_in_by_device_code_with_device_or_where_this_machine_has_no_browser() {
    for (place, args) in [
        (Place::Remote, &["codex"][..]),
        (Place::Desktop, &["codex", "--device"]),
    ] {
        let (server, _directory, credential) = prepared(vec![
            serving_every(),
            only_acme(),
            nothing_held(),
            stored(CODEX, "201 Created", 1),
        ]);
        let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
        let home = AmbientHome::new();
        let output = run_native(
            &server,
            &credential,
            &home,
            Some(&codex),
            place,
            args,
            Input::Null,
        );

        assert_eq!(
            output.status.code(),
            Some(0),
            "{place:?}: {}",
            stderr(&output)
        );
        assert_eq!(
            codex.recorded("arguments").unwrap(),
            "login --device-auth\n",
            "{place:?}"
        );
        assert!(stderr(&output).contains(DEVICE_CODE_PROMPT), "{place:?}");
        let requests = server.finish();
        assert_eq!(
            request_lines(&requests),
            [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE, IMPORT_GRANT]
        );
        assert_imported(&requests[3], ORGANIZATION_ID, CODEX);
    }
}

// REQ-GRANT-70
#[test]
fn create_codex_imports_a_named_file_or_standard_input_without_running_codex() {
    let directory = tempfile::tempdir().unwrap();
    let file = private_file(directory.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    for (path, input) in [
        (file.to_str().unwrap(), Input::Null),
        ("-", Input::Piped(CODEX_MATERIAL.as_bytes())),
    ] {
        let (server, _directory, credential) = prepared(vec![
            only_acme(),
            nothing_held(),
            stored(CODEX, "201 Created", 1),
        ]);
        let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
        let home = AmbientHome::new();
        let output = run_native(
            &server,
            &credential,
            &home,
            Some(&codex),
            Place::Desktop,
            &["codex", "--from-file", path],
            input,
        );

        assert_eq!(output.status.code(), Some(0), "{path}: {}", stderr(&output));
        assert!(stdout(&output).contains(NATIVE_GRANT_ID));
        assert_eq!(codex.recorded("arguments"), None, "{path}");
        let requests = server.finish();
        assert_eq!(
            request_lines(&requests),
            [LIST_MEMBERSHIPS, LIST_NATIVE, IMPORT_GRANT]
        );
        assert_imported(&requests[2], ORGANIZATION_ID, CODEX);
    }
}

// REQ-GRANT-70
// REQ-GRANT-71
// REQ-GRANT-72
#[test]
fn create_refuses_an_unsafe_file_or_an_option_the_provider_does_not_take_before_any_request() {
    let directory = tempfile::tempdir().unwrap();
    let path = |name: &str, contents: &[u8], mode: u32| {
        let file = private_file(directory.path(), name, contents);
        fs::set_permissions(&file, Permissions::from_mode(mode)).unwrap();
        file.to_str().unwrap().to_owned()
    };
    let readable = path("readable.json", CODEX_MATERIAL.as_bytes(), 0o644);
    let oversized = path("oversized.json", &vec![b' '; 32 * 1024 + 1], 0o600);
    let array = path(
        "array.json",
        format!("[\"{CODEX_SENTINEL}\"]").as_bytes(),
        0o600,
    );
    let login = path("auth.json", CODEX_MATERIAL.as_bytes(), 0o600);
    let cases: [&[&str]; 13] = [
        &["codex", "--from-file", &readable],
        &["codex", "--from-file", &oversized],
        &["codex", "--from-file", &array],
        &["codex", "--token-file", &login],
        &["codex", "--login", "--device"],
        &["codex", "--from-file", &login, "--login"],
        &["github", "--organization", "acme"],
        &["linear", "--from-file", &login],
        &["gitlab.com", "--device"],
        &["github", "--token-file", &login],
        &["claude_code", "--from-file", &login],
        &["claude_code", "--login"],
        &["claude_code", "--device"],
    ];
    for args in cases {
        let (server, _directory, credential) = prepared(vec![]);
        let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
        let home = AmbientHome::new();
        let output = run_native(
            &server,
            &credential,
            &home,
            Some(&codex),
            Place::Desktop,
            args,
            Input::Null,
        );

        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            stderr(&output)
        );
        assert!(output.stdout.is_empty(), "{args:?}");
        assert_eq!(codex.recorded("arguments"), None, "{args:?}");
        assert!(server.finish().is_empty(), "{args:?}");
    }
}

// REQ-GRANT-70
// REQ-GRANT-74
#[test]
fn a_codex_sign_in_that_fails_stores_nothing_and_reports_provider_login_failed() {
    let cases = [
        ("exits nonzero", Some(SignIn::Exits(3))),
        ("writes no login", Some(SignIn::WritesNothing)),
        ("is interrupted", Some(SignIn::Interrupts)),
        ("is not on PATH", None),
    ];
    for (case, sign_in) in cases {
        let codex = sign_in.map(FakeProgram::codex);
        for json in [false, true] {
            let (server, _directory, credential) =
                prepared(vec![serving_every(), only_acme(), nothing_held()]);
            let home = AmbientHome::new();
            let args: &[&str] = if json {
                &["codex", "--json"]
            } else {
                &["codex"]
            };
            let output = run_native(
                &server,
                &credential,
                &home,
                codex.as_ref(),
                Place::Desktop,
                args,
                Input::Null,
            );

            assert_eq!(output.status.code(), Some(1), "{case}: {}", stderr(&output));
            if json {
                let events = events(&output);
                assert_eq!(events.len(), 1, "{case}: {events:?}");
                assert_eq!(events[0]["outcome"], "provider_login_failed", "{case}");
            } else {
                assert!(output.stdout.is_empty(), "{case}");
                assert!(stderr(&output).contains("--from-file"), "{case}");
            }
            assert_eq!(
                request_lines(&server.finish()),
                [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE],
                "{case}"
            );
        }
    }
}

// REQ-GRANT-72
#[test]
fn create_codex_imports_for_the_named_organization_and_needs_one_when_the_member_has_several() {
    let (server, _directory, credential) = prepared(vec![
        serving_every(),
        memberships(&[
            (ORGANIZATION_ID, "acme"),
            (GLOBEX_ORGANIZATION_ID, "globex"),
        ]),
    ]);
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let home = AmbientHome::new();
    let refused = run_native(
        &server,
        &credential,
        &home,
        Some(&codex),
        Place::Desktop,
        &["codex"],
        Input::Null,
    );

    assert_eq!(refused.status.code(), Some(2), "{}", stderr(&refused));
    let text = stderr(&refused);
    for fact in ["--organization", "acme", "globex"] {
        assert!(text.contains(fact), "{fact}: {text}");
    }
    assert_eq!(codex.recorded("arguments"), None);
    assert_eq!(
        request_lines(&server.finish()),
        [READ_SERVED, LIST_MEMBERSHIPS]
    );

    let (server, _directory, credential) = prepared(vec![
        serving_every(),
        acme(),
        nothing_held(),
        stored(CODEX, "201 Created", 1),
    ]);
    let named = run_native(
        &server,
        &credential,
        &home,
        Some(&codex),
        Place::Desktop,
        &["codex", "--organization", "acme"],
        Input::Null,
    );

    assert_eq!(named.status.code(), Some(0), "{}", stderr(&named));
    assert_eq!(codex.recorded("arguments").unwrap(), "login\n");
    let requests = server.finish();
    assert_eq!(
        request_lines(&requests),
        [READ_SERVED, READ_ACME, LIST_NATIVE, IMPORT_GRANT]
    );
    assert_imported(&requests[3], ORGANIZATION_ID, CODEX);
}

// REQ-GRANT-73
#[test]
fn create_codex_stores_the_grant_held_in_that_organization_as_its_next_generation() {
    let held = || {
        page(
            &[
                codex_grant(NATIVE_GRANT_ID, ORGANIZATION_ID, "active"),
                codex_grant(OTHER_NATIVE_GRANT_ID, GLOBEX_ORGANIZATION_ID, "active"),
                codex_grant(THIRD_NATIVE_GRANT_ID, ORGANIZATION_ID, "revoked"),
            ],
            None,
        )
    };
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let home = AmbientHome::new();

    let (server, _directory, credential) = prepared(vec![serving_every(), only_acme(), held()]);
    let refused = run_native(
        &server,
        &credential,
        &home,
        Some(&codex),
        Place::Desktop,
        &["codex"],
        Input::Null,
    );
    assert_eq!(refused.status.code(), Some(2), "{}", stderr(&refused));
    let text = stderr(&refused);
    assert!(text.contains("--replace"), "{text}");
    assert!(text.contains(NATIVE_GRANT_ID), "{text}");
    assert!(!text.contains(OTHER_NATIVE_GRANT_ID), "{text}");
    assert_eq!(codex.recorded("arguments"), None);
    assert_eq!(
        request_lines(&server.finish()),
        [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE]
    );

    // A lost answer is replayed once under the same key.
    let (server, _directory, credential) = prepared(vec![
        serving_every(),
        only_acme(),
        held(),
        Vec::new(),
        stored(CODEX, "200 OK", 3),
    ]);
    let replaced = run_native(
        &server,
        &credential,
        &home,
        Some(&codex),
        Place::Desktop,
        &["codex", "--replace", "--json"],
        Input::Null,
    );
    assert_eq!(replaced.status.code(), Some(0), "{}", stderr(&replaced));
    let events = events(&replaced);
    assert_eq!(events[0]["outcome"], "replaced");
    assert_eq!(events[0]["grant"]["id"], NATIVE_GRANT_ID);
    assert_eq!(events[0]["grant"]["generation"], 3);
    assert_eq!(codex.recorded("arguments").unwrap(), "login\n");
    let requests = server.finish();
    let replace = replace_line(NATIVE_GRANT_ID);
    assert_eq!(
        request_lines(&requests),
        [
            READ_SERVED,
            LIST_MEMBERSHIPS,
            LIST_NATIVE,
            &replace,
            &replace
        ]
    );
    for request in &requests[3..] {
        assert_eq!(
            request_body(request),
            serde_json::json!({
                "recordSchemaVersion": 1,
                "material": encoded(CODEX_MATERIAL.as_bytes())
            })
        );
    }
    assert_eq!(
        header_value(&requests[3], "idempotency-key"),
        header_value(&requests[4], "idempotency-key")
    );
}

// REQ-GRANT-73
#[test]
fn create_codex_names_the_several_grants_held_in_that_organization_and_starts_no_sign_in() {
    let (server, _directory, credential) = prepared(vec![
        serving_every(),
        only_acme(),
        page(
            &[
                codex_grant(NATIVE_GRANT_ID, ORGANIZATION_ID, "active"),
                codex_grant(OTHER_NATIVE_GRANT_ID, ORGANIZATION_ID, "active"),
            ],
            None,
        ),
    ]);
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let home = AmbientHome::new();
    let output = run_native(
        &server,
        &credential,
        &home,
        Some(&codex),
        Place::Desktop,
        &["codex", "--replace"],
        Input::Null,
    );

    assert_eq!(output.status.code(), Some(2), "{}", stderr(&output));
    let text = stderr(&output);
    for id in [NATIVE_GRANT_ID, OTHER_NATIVE_GRANT_ID] {
        assert!(text.contains(id), "{id}: {text}");
    }
    assert_eq!(codex.recorded("arguments"), None);
    assert_eq!(
        request_lines(&server.finish()),
        [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE]
    );
}

// REQ-GRANT-73
// INV-CLI-13
#[test]
fn create_codex_starts_no_sign_in_without_a_session() {
    let server = ScriptedServer::respond(vec![]);
    let directory = private_credential_directory();
    let credential = directory.path().join("credentials.json");
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let home = AmbientHome::new();
    let output = run_native(
        &server,
        &credential,
        &home,
        Some(&codex),
        Place::Desktop,
        &["codex", "--json"],
        Input::Null,
    );

    assert_eq!(output.status.code(), Some(3), "{}", stderr(&output));
    assert_eq!(events(&output)[0]["outcome"], "unauthenticated");
    assert_eq!(codex.recorded("arguments"), None);
    assert!(server.finish().is_empty());
}

// `um grant create claude_code`: `claude setup-token` on a terminal `um` owns, its prompts
// passed through and the token it prints taken out of what the member sees, or a token the
// member names, imported for one organization.

fn claude_grant(id: &str, state: &str) -> serde_json::Value {
    let mut grant = codex_grant(id, ORGANIZATION_ID, state);
    grant["provider"] = "claude_code".into();
    grant
}

// REQ-GRANT-71
// REQ-GRANT-74
// INV-CLI-14
#[test]
fn create_claude_code_runs_setup_token_on_a_terminal_and_imports_the_token_without_showing_it() {
    let paste = format!("{PASTE_CODE}\n");
    for (flow, place, input) in [
        ("browser", Place::Desktop, Input::Typed(b"")),
        ("paste", Place::Remote, Input::Typed(paste.as_bytes())),
    ] {
        let (server, _directory, credential) = prepared(vec![
            serving_every(),
            only_acme(),
            nothing_held(),
            stored(CLAUDE_CODE, "201 Created", 1),
        ]);
        let claude = FakeProgram::claude(SetupToken::Prints);
        let home = AmbientHome::new();
        let fake = Fake {
            program: Some(&claude),
            environment: &[(CLAUDE_FLOW, flow)],
        };
        let output = run_fake(
            &server,
            &credential,
            &home,
            fake,
            place,
            &["claude_code"],
            input,
        );

        assert_eq!(output.status.code(), Some(0), "{flow}: {}", stderr(&output));
        let text = stdout(&output);
        for fact in [
            NATIVE_GRANT_ID,
            "provider: claude_code",
            "generation: 1",
            "acme",
        ] {
            assert!(text.contains(fact), "{flow}: {fact}: {text}");
        }
        // The sign-in's prompts and what follows the token reach the member on standard
        // error, the token does not (run_native checks every output for it).
        let shown = stderr(&output);
        assert!(shown.contains(CLAUDE_SIGN_IN_URL), "{flow}: {shown}");
        assert!(shown.contains(AFTER_TOKEN), "{flow}: {shown}");
        assert_eq!(
            shown.contains(PASTE_PROMPT),
            flow == "paste",
            "{flow}: {shown}"
        );
        assert!(!shown.contains("sk-ant-"), "{flow}: {shown}");
        assert_eq!(claude.recorded("arguments").unwrap(), "setup-token\n");
        assert_eq!(claude.recorded("terminal").unwrap(), "yes\n", "{flow}");
        let config = std::path::PathBuf::from(claude.recorded("config").unwrap());
        assert_ne!(config, home.ambient_claude());
        assert!(config.starts_with(home.temporary.path()), "{config:?}");
        assert!(!config.exists());
        assert!(
            claude.recorded("mode").unwrap().starts_with("drwx------"),
            "{:?}",
            claude.recorded("mode")
        );
        let requests = server.finish();
        assert_eq!(
            request_lines(&requests),
            [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE, IMPORT_GRANT],
            "{flow}"
        );
        assert_imported(&requests[3], ORGANIZATION_ID, CLAUDE_CODE);
    }
}

// REQ-GRANT-71
// REQ-GRANT-74
#[test]
fn a_claude_setup_token_that_fails_stores_nothing_and_reports_provider_login_failed() {
    let cases = [
        ("exits nonzero", Some(SetupToken::Exits(3))),
        ("prints no token", Some(SetupToken::PrintsNothing)),
        ("is interrupted", Some(SetupToken::Interrupts)),
        ("is not on PATH", None),
    ];
    for (case, setup_token) in cases {
        let claude = setup_token.map(FakeProgram::claude);
        for json in [false, true] {
            let (server, _directory, credential) =
                prepared(vec![serving_every(), only_acme(), nothing_held()]);
            let home = AmbientHome::new();
            let args: &[&str] = if json {
                &["claude_code", "--json"]
            } else {
                &["claude_code"]
            };
            let output = run_native(
                &server,
                &credential,
                &home,
                claude.as_ref(),
                Place::Desktop,
                args,
                Input::Typed(b""),
            );

            assert_eq!(output.status.code(), Some(1), "{case}: {}", stderr(&output));
            if json {
                let events = events(&output);
                assert_eq!(events.len(), 1, "{case}: {events:?}");
                assert_eq!(events[0]["outcome"], "provider_login_failed", "{case}");
            } else {
                assert!(output.stdout.is_empty(), "{case}");
                assert!(stderr(&output).contains("--token-file"), "{case}");
            }
            assert_eq!(
                request_lines(&server.finish()),
                [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE],
                "{case}"
            );
        }
    }
}

// REQ-GRANT-71
#[test]
fn create_claude_code_without_a_terminal_starts_no_sign_in_and_names_the_token_file() {
    let claude = FakeProgram::claude(SetupToken::Prints);
    for json in [false, true] {
        let (server, _directory, credential) = prepared(vec![]);
        let home = AmbientHome::new();
        let args: &[&str] = if json {
            &["claude_code", "--json"]
        } else {
            &["claude_code"]
        };
        let output = run_native(
            &server,
            &credential,
            &home,
            Some(&claude),
            Place::Desktop,
            args,
            Input::Null,
        );

        assert_eq!(output.status.code(), Some(1), "{json}: {}", stderr(&output));
        if json {
            let events = events(&output);
            assert_eq!(events.len(), 1, "{events:?}");
            assert_eq!(events[0]["outcome"], "provider_login_failed");
        } else {
            assert!(output.stdout.is_empty());
            assert!(
                stderr(&output).contains("--token-file"),
                "{}",
                stderr(&output)
            );
        }
        assert!(server.finish().is_empty(), "{json}");
    }
    assert_eq!(claude.recorded("arguments"), None);
}

/// `um grant create claude_code` with the fake `claude` on `PATH` in the browser flow, standard
/// input `input`, a terminal's secondary side, and its outputs piped, started but not waited
/// for.
fn spawn_claude_sign_in(
    server: &ScriptedServer,
    credential: &Path,
    home: &AmbientHome,
    claude: &Path,
    input: Stdio,
) -> std::process::Child {
    let opener = Opener::new(0);
    let path = std::env::join_paths([opener.directory.path(), claude]).unwrap();
    create_command(
        server,
        credential,
        &opener,
        Place::Desktop,
        &["claude_code"],
    )
    .env("PATH", path)
    .envs(home.variables())
    .env(CLAUDE_FLOW, "browser")
    .stdin(input)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap()
}

// REQ-GRANT-71
#[test]
fn input_written_after_the_claude_sign_in_ends_is_left_for_the_next_reader() {
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![
        serving_every(),
        only_acme(),
        nothing_held(),
        stored(CLAUDE_CODE, "201 Created", 1),
    ]);
    let directory = private_credential_directory();
    let credential = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let claude = FakeProgram::claude(SetupToken::Prints);
    let home = AmbientHome::new();
    let pty = open_test_pty(None).unwrap();
    // The test keeps its own copy of the terminal's secondary side, so what `um` leaves unread
    // stays readable.
    let kept = fs::File::from(pty.slave.try_clone().unwrap());
    let child = spawn_claude_sign_in(
        &server,
        &credential,
        &home,
        &claude.bin,
        Stdio::from(pty.slave),
    );

    // The import is requested once the sign-in has ended; its answer waits for the input.
    let requests: Vec<_> = (0..4).map(|_| server.next_request()).collect();
    let typed = b"typed-after-the-sign-in\n";
    let mut terminal = fs::File::from(pty.master);
    terminal.write_all(typed).unwrap();
    server.release_paused_response();
    let output = child.wait_with_output().unwrap();

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    // The sign-in restored the terminal's line mode, so the typed line waits there whole.
    let mut ready = [rustix::event::PollFd::new(
        &kept,
        rustix::event::PollFlags::IN,
    )];
    let wait = rustix::event::Timespec {
        tv_sec: 20,
        tv_nsec: 0,
    };
    assert_eq!(rustix::event::poll(&mut ready, Some(&wait)).unwrap(), 1);
    let mut left = vec![0; typed.len() + 1];
    let count = (&kept).read(&mut left).unwrap();
    assert_eq!(&left[..count], typed);
    drop(terminal);
    assert_eq!(
        request_lines(&requests),
        [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE, IMPORT_GRANT]
    );
    assert_imported(&requests[3], ORGANIZATION_ID, CLAUDE_CODE);
    assert!(server.finish().is_empty());
    assert_no_secret_output(&output, &[TOKEN, CLAUDE_TOKEN_SENTINEL]);
    home.assert_untouched_and_clean();
}

// REQ-GRANT-71
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the bound on um's return is far below the descendant's own life, so real time only separates returning from waiting for the descendant"
)]
fn a_descendant_of_the_claude_sign_in_holding_its_terminal_does_not_keep_um_running() {
    use super::pi_installation::quote;
    let (server, _directory, credential) = prepared(vec![
        serving_every(),
        only_acme(),
        nothing_held(),
        stored(CLAUDE_CODE, "201 Created", 1),
    ]);
    // A `claude` that leaves a descendant holding its terminal, deaf to the hangup its end
    // sends, then prints the token and exits.
    let fake = tempfile::tempdir().unwrap();
    let bin = fake.path().join("bin");
    let descendant = fake.path().join("descendant");
    fs::create_dir(&bin).unwrap();
    fs::write(
        bin.join("claude"),
        format!(
            "#!/bin/sh\nPATH={tools}\nexport PATH\n\
             (trap '' HUP; exec sleep 120) &\n\
             echo \"$!\" > {descendant}\n\
             printf '%s\\n' {token}\n",
            tools = quote(&std::env::var("PATH").unwrap_or_default()),
            descendant = quote(descendant.to_str().unwrap()),
            token = quote(CLAUDE_TOKEN),
        ),
    )
    .unwrap();
    fs::set_permissions(bin.join("claude"), Permissions::from_mode(0o755)).unwrap();
    let home = AmbientHome::new();
    let pty = open_test_pty(None).unwrap();
    let child = spawn_claude_sign_in(&server, &credential, &home, &bin, Stdio::from(pty.slave));

    let (exited, exit) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let output = child.wait_with_output();
        exited.send(()).ok();
        output
    });
    let returned = exit.recv_timeout(Duration::from_secs(20)).is_ok();
    let holder: i32 = fs::read_to_string(&descendant)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let _ = rustix::process::kill_process(
        rustix::process::Pid::from_raw(holder).unwrap(),
        rustix::process::Signal::KILL,
    );
    assert!(
        returned,
        "um waited for the descendant holding the terminal"
    );
    let output = waiter.join().unwrap().unwrap();

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert!(stdout(&output).contains(NATIVE_GRANT_ID));
    let requests = server.finish();
    assert_eq!(
        request_lines(&requests),
        [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE, IMPORT_GRANT]
    );
    assert_imported(&requests[3], ORGANIZATION_ID, CLAUDE_CODE);
    assert_no_secret_output(&output, &[TOKEN, CLAUDE_TOKEN_SENTINEL]);
    home.assert_untouched_and_clean();
}

// REQ-GRANT-71
// INV-CLI-14
#[test]
fn the_claude_sign_in_leads_its_own_session_with_the_terminal_um_owns_as_its_controlling_terminal()
{
    use super::pi_installation::quote;
    const ON_ITS_TERMINAL: &str = "claude-wrote-to-its-controlling-terminal";
    let (server, _directory, credential) = prepared(vec![
        serving_every(),
        only_acme(),
        nothing_held(),
        stored(CLAUDE_CODE, "201 Created", 1),
    ]);
    // A `claude` that goes on only when it leads its own session and has a controlling
    // terminal, which it writes to; what it writes there reaches the member only when that
    // terminal is the one `um` relays.
    let fake = tempfile::tempdir().unwrap();
    let bin = fake.path().join("bin");
    fs::create_dir(&bin).unwrap();
    fs::write(
        bin.join("claude"),
        format!(
            "#!/bin/sh\nPATH={tools}\nexport PATH\n\
             python3 -c 'import os, sys; leader = int(sys.argv[1]); \
             sys.exit(os.getsid(leader) != leader)' \"$$\" || exit 7\n\
             {{ : < /dev/tty; }} 2>/dev/null || exit 8\n\
             echo {ON_ITS_TERMINAL} > /dev/tty\n\
             printf '%s\\n' {token}\n",
            tools = quote(&std::env::var("PATH").unwrap_or_default()),
            token = quote(CLAUDE_TOKEN),
        ),
    )
    .unwrap();
    fs::set_permissions(bin.join("claude"), Permissions::from_mode(0o755)).unwrap();
    let home = AmbientHome::new();
    let pty = open_test_pty(None).unwrap();
    let output = spawn_claude_sign_in(&server, &credential, &home, &bin, Stdio::from(pty.slave))
        .wait_with_output()
        .unwrap();

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert!(stderr(&output).contains(ON_ITS_TERMINAL));
    let requests = server.finish();
    assert_eq!(
        request_lines(&requests),
        [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE, IMPORT_GRANT]
    );
    assert_imported(&requests[3], ORGANIZATION_ID, CLAUDE_CODE);
    assert_no_secret_output(&output, &[TOKEN, CLAUDE_TOKEN_SENTINEL]);
    home.assert_untouched_and_clean();
}

// REQ-GRANT-71
#[test]
fn create_claude_code_imports_a_named_token_file_or_standard_input_without_running_claude() {
    let directory = tempfile::tempdir().unwrap();
    let line = format!("{CLAUDE_TOKEN}\n");
    let file = private_file(directory.path(), "token", line.as_bytes());
    // Neither source needs a terminal (REQ-GRANT-71).
    for (path, input) in [
        (file.to_str().unwrap(), Input::Null),
        ("-", Input::Piped(line.as_bytes())),
    ] {
        let (server, _directory, credential) = prepared(vec![
            only_acme(),
            nothing_held(),
            stored(CLAUDE_CODE, "201 Created", 1),
        ]);
        let claude = FakeProgram::claude(SetupToken::Prints);
        let home = AmbientHome::new();
        let output = run_native(
            &server,
            &credential,
            &home,
            Some(&claude),
            Place::Desktop,
            &["claude_code", "--token-file", path],
            input,
        );

        assert_eq!(output.status.code(), Some(0), "{path}: {}", stderr(&output));
        assert!(stdout(&output).contains(NATIVE_GRANT_ID));
        assert_eq!(claude.recorded("arguments"), None, "{path}");
        let requests = server.finish();
        assert_eq!(
            request_lines(&requests),
            [LIST_MEMBERSHIPS, LIST_NATIVE, IMPORT_GRANT]
        );
        assert_imported(&requests[2], ORGANIZATION_ID, CLAUDE_CODE);
    }
}

// REQ-GRANT-73
#[test]
fn create_claude_code_stores_the_token_as_the_held_grants_next_generation_with_replace() {
    let (server, _directory, credential) = prepared(vec![
        serving_every(),
        only_acme(),
        page(&[claude_grant(NATIVE_GRANT_ID, "active")], None),
        stored(CLAUDE_CODE, "200 OK", 3),
    ]);
    let claude = FakeProgram::claude(SetupToken::Prints);
    let home = AmbientHome::new();
    let output = run_native(
        &server,
        &credential,
        &home,
        Some(&claude),
        Place::Desktop,
        &["claude_code", "--replace", "--json"],
        Input::Typed(b""),
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let events = events(&output);
    assert_eq!(events[0]["outcome"], "replaced");
    assert_eq!(events[0]["grant"]["id"], NATIVE_GRANT_ID);
    assert_eq!(events[0]["grant"]["generation"], 3);
    let requests = server.finish();
    let replace = replace_line(NATIVE_GRANT_ID);
    assert_eq!(
        request_lines(&requests),
        [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE, &replace]
    );
    assert_eq!(
        request_body(&requests[3]),
        serde_json::json!({
            "recordSchemaVersion": 1,
            "material": encoded(CLAUDE_TOKEN.as_bytes())
        })
    );
}

// `um grant create` with no provider: every provider the member lacks, one after another.

const LIST_EVERY_KIND: &str = LIST_FIRST_PAGE;

// The URL a consent of `provider` asks the member to approve.
fn consent_url(provider: &str) -> String {
    format!("https://{provider}.example.test/oauth/authorize?state=public-fixture-state")
}

// The consent of `provider` as the API presents it, carrying material the CLI must drop.
fn consent_of(provider: &str, status: &str) -> serde_json::Value {
    let mut consent = session(status);
    consent["provider"] = provider.into();
    match status {
        "pending" => consent["authorizationUrl"] = consent_url(provider).into(),
        "completed" => consent["resultGrant"]["provider"] = provider.into(),
        _ => {}
    }
    consent
}

// The answers to a consent of `provider` that the member approves.
fn approved(provider: &str) -> [Vec<u8>; 2] {
    [
        json_http_response("201 Created", consent_of(provider, "pending")),
        polled(consent_of(provider, "completed")),
    ]
}

fn claude_grant_of(id: &str) -> serde_json::Value {
    claude_grant(id, "active")
}

// The member's grants: an active one of each OAuth provider in `oauth`, and an active Codex and
// Claude Code grant in Acme when `natives` is set.
fn holding(oauth: &[&str], natives: bool) -> Vec<u8> {
    let mut grants: Vec<_> = oauth
        .iter()
        .map(|provider| oauth_grant(OAUTH_GRANT_ID, provider, "active"))
        .collect();
    if natives {
        grants.push(native_grant(NATIVE_GRANT_ID, "active"));
        grants.push(claude_grant_of(OTHER_NATIVE_GRANT_ID));
    }
    page(&grants, None)
}

/// Runs `um grant create` with the fake opener, `codex` and `claude` on `PATH` and a terminal as
/// its standard input.
fn run_every(
    server: &ScriptedServer,
    credential: &Path,
    opener: &Opener,
    programs: &[&FakeProgram],
    args: &[&str],
) -> Output {
    run_in(server, credential, opener, programs, None, args)
}

/// [`run_every`] in `checkout`, when there is one, with its fake `git` on `PATH` besides.
fn run_in(
    server: &ScriptedServer,
    credential: &Path,
    opener: &Opener,
    programs: &[&FakeProgram],
    checkout: Option<Checkout<'_>>,
    args: &[&str],
) -> Output {
    run_command_in(
        server,
        credential,
        opener,
        programs,
        checkout,
        GRANT_CREATE,
        args,
    )
}

/// [`run_in`] for the command whose leading words are `command`, such as [`RUNNER_DOCTOR`].
fn run_command_in(
    server: &ScriptedServer,
    credential: &Path,
    opener: &Opener,
    programs: &[&FakeProgram],
    checkout: Option<Checkout<'_>>,
    command: &[&str],
    args: &[&str],
) -> Output {
    let home = AmbientHome::new();
    let fakes = Fakes {
        opener,
        programs,
        environment: &[(CLAUDE_FLOW, "browser")],
        checkout,
    };
    let arguments = [command, args].concat();
    run_programs(
        server,
        credential,
        &home,
        fakes,
        Place::Desktop,
        &arguments,
        Input::Typed(b""),
    )
}

fn start_body(request: &str) -> serde_json::Value {
    request_body(request)["provider"].clone()
}

// The provider and outcome of each entry of the summary that ends the JSON output.
fn summary(events: &[serde_json::Value]) -> (String, Vec<(String, String)>) {
    let last = events.last().unwrap();
    assert_eq!(last["event"], "summary", "{events:?}");
    let providers = last["providers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["provider"].as_str().unwrap().to_owned(),
                entry["outcome"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    (last["outcome"].as_str().unwrap().to_owned(), providers)
}

fn outcomes(entries: &[(&str, &str)]) -> Vec<(String, String)> {
    entries
        .iter()
        .map(|(provider, outcome)| ((*provider).to_owned(), (*outcome).to_owned()))
        .collect()
}

// REQ-GRANT-75
#[test]
fn create_without_a_provider_signs_in_to_each_provider_the_member_lacks_in_order() {
    for args in [&[][..], &["--all"]] {
        let (server, _directory, credential) = prepared(
            [
                vec![holding(&["github"], false), serving_every(), only_acme()],
                approved("gitlab.com").to_vec(),
                approved("linear").to_vec(),
                vec![
                    stored(CLAUDE_CODE, "201 Created", 1),
                    stored(CODEX, "201 Created", 1),
                ],
            ]
            .concat(),
        );
        let opener = Opener::new(0);
        let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
        let claude = FakeProgram::claude(SetupToken::Prints);
        let output = run_every(&server, &credential, &opener, &[&codex, &claude], args);

        assert_eq!(
            output.status.code(),
            Some(0),
            "{args:?}: {}",
            stderr(&output)
        );
        assert_eq!(
            opener.opened(2),
            [
                format!("1 {}", consent_url("gitlab.com")),
                format!("1 {}", consent_url("linear"))
            ],
            "{args:?}"
        );
        assert_eq!(claude.recorded("arguments").unwrap(), "setup-token\n");
        assert_eq!(codex.recorded("arguments").unwrap(), "login\n");
        let text = stdout(&output);
        for fact in [
            "gitlab.com: created",
            "linear: created",
            "claude_code: created",
            "codex: created",
        ] {
            assert!(text.contains(fact), "{args:?}: {fact}: {text}");
        }
        assert!(!text.contains("github:"), "{args:?}: {text}");
        let requests = server.finish();
        assert_member_requests(&requests);
        let poll = poll_line();
        assert_eq!(
            request_lines(&requests),
            [
                LIST_EVERY_KIND,
                READ_SERVED,
                LIST_MEMBERSHIPS,
                START_CONSENT,
                poll.as_str(),
                START_CONSENT,
                poll.as_str(),
                IMPORT_GRANT,
                IMPORT_GRANT
            ],
            "{args:?}"
        );
        assert_eq!(start_body(&requests[3]), "gitlab.com");
        assert_eq!(start_body(&requests[5]), "linear");
        assert_imported(&requests[7], ORGANIZATION_ID, CLAUDE_CODE);
        assert_imported(&requests[8], ORGANIZATION_ID, CODEX);
    }
}

// REQ-GRANT-75
#[test]
fn a_sign_in_that_fails_is_reported_and_the_ones_after_it_still_run() {
    let mut denied = consent_of("linear", "failed");
    denied["reasonCode"] = "consent_denied".into();
    let (server, _directory, credential) = prepared(
        [
            vec![holding(&["github"], false), serving_every(), only_acme()],
            approved("gitlab.com").to_vec(),
            vec![
                json_http_response("201 Created", consent_of("linear", "pending")),
                polled(denied),
                stored(CLAUDE_CODE, "201 Created", 1),
                stored(CODEX, "201 Created", 1),
            ],
        ]
        .concat(),
    );
    let opener = Opener::new(0);
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let claude = FakeProgram::claude(SetupToken::Prints);
    let output = run_every(
        &server,
        &credential,
        &opener,
        &[&codex, &claude],
        &["--json"],
    );

    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    // Each provider's own events, newline-delimited, then the summary.
    let events = events(&output);
    let names: Vec<_> = events
        .iter()
        .map(|event| event["event"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "consent_required",
            "consent_result",
            "consent_required",
            "consent_result",
            "import_result",
            "import_result",
            "summary"
        ]
    );
    assert_eq!(events[1]["grant"]["provider"], "gitlab.com");
    assert_eq!(events[3]["outcome"], "failed");
    assert_eq!(events[3]["reason"], "consent_denied");
    assert_eq!(events[4]["grant"]["provider"], "claude_code");
    assert_eq!(events[5]["grant"]["provider"], "codex");
    assert_eq!(
        summary(&events),
        (
            "failed".to_owned(),
            outcomes(&[
                ("gitlab.com", "created"),
                ("linear", "failed"),
                ("claude_code", "created"),
                ("codex", "created")
            ])
        )
    );
    let requests = server.finish();
    assert_eq!(request_lines(&requests)[7..], [IMPORT_GRANT, IMPORT_GRANT]);
}

// REQ-GRANT-71
// REQ-GRANT-75
#[test]
fn without_a_terminal_the_claude_sign_in_fails_and_the_codex_sign_in_after_it_still_runs() {
    let (server, _directory, credential) = prepared(vec![
        holding(&["github", "gitlab.com", "linear"], false),
        serving_every(),
        only_acme(),
        stored(CODEX, "201 Created", 1),
    ]);
    let opener = Opener::new(0);
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let claude = FakeProgram::claude(SetupToken::Prints);
    let home = AmbientHome::new();
    let fakes = Fakes {
        opener: &opener,
        programs: &[&codex, &claude],
        environment: &[],
        checkout: None,
    };
    let arguments = [GRANT_CREATE, &["--json"]].concat();
    let output = run_programs(
        &server,
        &credential,
        &home,
        fakes,
        Place::Desktop,
        &arguments,
        Input::Null,
    );

    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    let events = events(&output);
    assert_eq!(events[0]["outcome"], "provider_login_failed", "{events:?}");
    assert_eq!(
        summary(&events),
        (
            "failed".to_owned(),
            outcomes(&[("claude_code", "failed"), ("codex", "created")])
        )
    );
    assert_eq!(claude.recorded("arguments"), None);
    assert_eq!(codex.recorded("arguments").unwrap(), "login\n");
    let requests = server.finish();
    assert_eq!(
        request_lines(&requests),
        [LIST_EVERY_KIND, READ_SERVED, LIST_MEMBERSHIPS, IMPORT_GRANT]
    );
    assert_imported(&requests[3], ORGANIZATION_ID, CODEX);
}

/// A directory holding a fake `claude` that runs `then` once it has started, for a sign-in a
/// signal ends; its `sleep` comes from the test's `PATH`, as `FakeProgram`'s tools do.
fn signalled_claude(then: &str) -> tempfile::TempDir {
    use super::pi_installation::quote;
    let directory = tempfile::tempdir().unwrap();
    let claude = directory.path().join("claude");
    let tools = std::env::var("PATH").unwrap_or_default();
    fs::write(
        &claude,
        format!(
            "#!/bin/sh\nPATH={}\nexport PATH\necho claude-sign-in-started\n{then}\n",
            quote(&tools)
        ),
    )
    .unwrap();
    fs::set_permissions(&claude, Permissions::from_mode(0o755)).unwrap();
    directory
}

// REQ-GRANT-75
// REQ-GRANT-71
#[test]
fn a_signal_that_ends_the_claude_sign_in_stops_the_command_with_its_status() {
    // How the sign-in ends, and the status and outcome it stops `um grant create` with: the
    // member's interrupt reaches the sign-in's own terminal, so `claude` alone receives it,
    // while a signal sent to `um` is passed to `claude`.
    let cases = [
        ("kill -INT $$", 130, "interrupted"),
        ("kill -TERM $$", 143, "terminated"),
        ("kill -HUP $$", 143, "terminated"),
        ("kill -INT \"$PPID\"\nexec sleep 30", 130, "interrupted"),
        ("kill -TERM \"$PPID\"\nexec sleep 30", 143, "terminated"),
    ];
    for (then, status, outcome) in cases {
        let claude = signalled_claude(then);
        let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
        let run = |server: &ScriptedServer, credential: &Path, args: &[&str]| {
            let opener = Opener::new(0);
            let home = AmbientHome::new();
            let path =
                std::env::join_paths([opener.directory.path(), claude.path(), &codex.bin]).unwrap();
            let output = typed_output(
                create_command(server, credential, &opener, Place::Desktop, args)
                    .env("PATH", path)
                    .envs(home.variables()),
                b"",
            );
            home.assert_untouched_and_clean();
            output
        };

        let (server, _directory, credential) = prepared(vec![
            holding(&["github", "gitlab.com", "linear"], false),
            serving_every(),
            only_acme(),
        ]);
        let output = run(&server, &credential, &["--json"]);
        assert_eq!(
            output.status.code(),
            Some(status),
            "{then}: {}",
            stderr(&output)
        );
        assert_eq!(
            summary(&events(&output)),
            (outcome.to_owned(), outcomes(&[("claude_code", outcome)])),
            "{then}"
        );
        assert_eq!(codex.recorded("arguments"), None, "{then}");
        assert_eq!(
            request_lines(&server.finish()),
            [LIST_EVERY_KIND, READ_SERVED, LIST_MEMBERSHIPS],
            "{then}"
        );

        // A single provider's sign-in that a signal ends is a failed sign-in.
        let (server, _directory, credential) =
            prepared(vec![serving_every(), only_acme(), nothing_held()]);
        let output = run(&server, &credential, &["claude_code", "--json"]);
        assert_eq!(output.status.code(), Some(1), "{then}: {}", stderr(&output));
        let events = events(&output);
        assert_eq!(events.len(), 1, "{then}: {events:?}");
        assert_eq!(events[0]["outcome"], "provider_login_failed", "{then}");
        assert_eq!(
            request_lines(&server.finish()),
            [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE],
            "{then}"
        );
    }
}

// REQ-GRANT-75
#[test]
fn a_provider_the_deployment_does_not_serve_is_reported_and_passed_over_without_a_sign_in() {
    let (server, _directory, credential) = prepared(
        [
            vec![
                holding(&["github"], true),
                problem("503 Service Unavailable", 503, "provider-unavailable"),
            ],
            approved("linear").to_vec(),
        ]
        .concat(),
    );
    let opener = Opener::new(0);
    let output = run_every(&server, &credential, &opener, &[], &["--json"]);

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let events = events(&output);
    assert_eq!(events[0]["event"], "consent_result");
    assert_eq!(events[0]["outcome"], "provider_unavailable");
    assert_eq!(
        summary(&events),
        (
            "completed".to_owned(),
            outcomes(&[
                ("gitlab.com", "provider_unavailable"),
                ("linear", "created")
            ])
        )
    );
    assert_eq!(opener.opened(1), [format!("1 {}", consent_url("linear"))]);
    let requests = server.finish();
    let poll = poll_line();
    assert_eq!(
        request_lines(&requests),
        [LIST_EVERY_KIND, START_CONSENT, START_CONSENT, poll.as_str()]
    );
    assert_eq!(start_body(&requests[1]), "gitlab.com");
    assert_eq!(start_body(&requests[2]), "linear");
}

const READ_SERVED: &str = "GET /api/v1/me/grant-providers HTTP/1.1";

/// The deployment's answer naming the providers it serves, each with its kind (REQ-GRANT-78).
fn serving(providers: &[&str]) -> Vec<u8> {
    let items: Vec<_> = providers
        .iter()
        .map(|provider| {
            let kind = if matches!(*provider, "claude_code" | "codex") {
                "native"
            } else {
                "oauth"
            };
            serde_json::json!({"provider": provider, "kind": kind})
        })
        .collect();
    json_http_response("200 OK", serde_json::json!({"items": items}))
}

/// The answer of a deployment that serves every provider.
fn serving_every() -> Vec<u8> {
    serving(&["github", "gitlab.com", "linear", "claude_code", "codex"])
}

/// The answer of a deployment that predates the read of the providers it serves.
fn predating_served() -> Vec<u8> {
    problem("404 Not Found", 404, "not-found")
}

/// The outcome of each `import_result` event, in order.
fn import_outcomes(events: &[serde_json::Value]) -> Vec<&str> {
    events
        .iter()
        .filter(|event| event["event"] == "import_result")
        .map(|event| event["outcome"].as_str().unwrap())
        .collect()
}

// REQ-GRANT-75
// REQ-GRANT-78
#[test]
fn a_coding_agent_the_deployment_does_not_serve_is_passed_over_before_its_sign_in() {
    // Named alone, it is refused before its organization, its sign-in or an import.
    let (server, _directory, credential) = prepared(vec![serving(&["github", "codex"])]);
    let claude = FakeProgram::claude(SetupToken::Prints);
    let home = AmbientHome::new();
    let output = run_native(
        &server,
        &credential,
        &home,
        Some(&claude),
        Place::Desktop,
        &["claude_code", "--json"],
        Input::Typed(b""),
    );

    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert_eq!(import_outcomes(&events(&output)), ["provider_unavailable"]);
    assert_eq!(claude.recorded("arguments"), None);
    assert_eq!(request_lines(&server.finish()), [READ_SERVED]);

    // Without a provider, it is passed over and the coding agent the deployment serves is
    // signed in to.
    let (server, _directory, credential) = prepared(vec![
        holding(&["github", "gitlab.com", "linear"], false),
        serving(&["github", "gitlab.com", "linear", "codex"]),
        only_acme(),
        stored(CODEX, "201 Created", 1),
    ]);
    let opener = Opener::new(0);
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let claude = FakeProgram::claude(SetupToken::Prints);
    let output = run_every(
        &server,
        &credential,
        &opener,
        &[&codex, &claude],
        &["--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let events = events(&output);
    assert_eq!(
        import_outcomes(&events),
        ["provider_unavailable", "imported"]
    );
    assert_eq!(
        summary(&events).1,
        outcomes(&[
            ("claude_code", "provider_unavailable"),
            ("codex", "created")
        ])
    );
    assert_eq!(claude.recorded("arguments"), None);
    assert_eq!(codex.recorded("arguments").unwrap(), "login\n");
    let requests = server.finish();
    assert_eq!(
        request_lines(&requests),
        [LIST_EVERY_KIND, READ_SERVED, LIST_MEMBERSHIPS, IMPORT_GRANT]
    );
    assert_imported(&requests[3], ORGANIZATION_ID, CODEX);
}

// REQ-GRANT-75
#[test]
fn a_deployment_that_predates_the_served_providers_read_is_signed_in_to_as_before() {
    let (server, _directory, credential) = prepared(vec![
        predating_served(),
        only_acme(),
        nothing_held(),
        stored(CLAUDE_CODE, "201 Created", 1),
    ]);
    let claude = FakeProgram::claude(SetupToken::Prints);
    let home = AmbientHome::new();
    let fake = Fake {
        program: Some(&claude),
        environment: &[(CLAUDE_FLOW, "browser")],
    };
    let output = run_fake(
        &server,
        &credential,
        &home,
        fake,
        Place::Desktop,
        &["claude_code"],
        Input::Typed(b""),
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(claude.recorded("arguments").unwrap(), "setup-token\n");
    let requests = server.finish();
    assert_eq!(
        request_lines(&requests),
        [READ_SERVED, LIST_MEMBERSHIPS, LIST_NATIVE, IMPORT_GRANT]
    );
    assert_imported(&requests[3], ORGANIZATION_ID, CLAUDE_CODE);

    let (server, _directory, credential) = prepared(vec![
        holding(&["github", "gitlab.com", "linear"], false),
        predating_served(),
        only_acme(),
        stored(CLAUDE_CODE, "201 Created", 1),
        stored(CODEX, "201 Created", 1),
    ]);
    let opener = Opener::new(0);
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let claude = FakeProgram::claude(SetupToken::Prints);
    let output = run_every(
        &server,
        &credential,
        &opener,
        &[&codex, &claude],
        &["--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(
        summary(&events(&output)).1,
        outcomes(&[("claude_code", "created"), ("codex", "created")])
    );
    assert_eq!(
        request_lines(&server.finish()),
        [
            LIST_EVERY_KIND,
            READ_SERVED,
            LIST_MEMBERSHIPS,
            IMPORT_GRANT,
            IMPORT_GRANT
        ]
    );
}

/// The report that ends the human output of `um grant create` without a provider, whose first
/// outcome line names `first`: its verdict line, then the lines after it.
fn summary_report<'a>(text: &'a str, first: &str) -> Vec<&'a str> {
    let lines: Vec<_> = text.lines().collect();
    let entry = lines
        .iter()
        .rposition(|line| line.starts_with(&format!("{first}: ")))
        .unwrap_or_else(|| panic!("no outcome of {first}: {text}"));
    lines[entry - 1..].to_vec()
}

/// The lines of a summary report after its data fields: its remedies.
fn remedies<'a>(report: &[&'a str]) -> Vec<&'a str> {
    let fields = report
        .iter()
        .position(|line| line.starts_with("deployment: "))
        .unwrap();
    report[fields + 1..].to_vec()
}

// REQ-GRANT-75
#[test]
fn a_summary_with_a_provider_passed_over_is_no_success_verdict_and_names_it() {
    let (server, _directory, credential) = prepared(
        [
            vec![
                holding(&["github"], true),
                problem("503 Service Unavailable", 503, "provider-unavailable"),
            ],
            approved("linear").to_vec(),
        ]
        .concat(),
    );
    let opener = Opener::new(0);
    let output = run_every(&server, &credential, &opener, &[], &[]);

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let text = stdout(&output);
    let report = summary_report(&text, "gitlab.com");
    assert!(!report[0].starts_with('✓'), "{text}");
    assert!(
        report.contains(&"gitlab.com: provider_unavailable") && report.contains(&"linear: created"),
        "{text}"
    );
    assert!(
        remedies(&report)
            .iter()
            .any(|line| line.contains("gitlab.com")),
        "{text}"
    );
    let poll = poll_line();
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_EVERY_KIND, START_CONSENT, START_CONSENT, poll.as_str()]
    );
}

// REQ-GRANT-75
#[test]
fn a_summary_of_consents_only_started_is_no_success_verdict_and_names_where_to_see_them() {
    let (server, _directory, credential) = prepared(vec![
        holding(&["github", "gitlab.com"], true),
        json_http_response("201 Created", consent_of("linear", "pending")),
    ]);
    let opener = Opener::new(0);
    let output = run_every(&server, &credential, &opener, &[], &["--no-wait"]);

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let text = stdout(&output);
    let report = summary_report(&text, "linear");
    assert!(!report[0].starts_with('✓'), "{text}");
    assert!(report.contains(&"linear: started"), "{text}");
    assert!(
        remedies(&report)
            .iter()
            .any(|line| line.trim() == "um grant list"),
        "{text}"
    );
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_EVERY_KIND, START_CONSENT]
    );
}

// REQ-GRANT-75
#[test]
fn a_summary_with_codex_missing_names_installing_it_before_running_the_command_again() {
    let (server, _directory, credential) = prepared(vec![
        holding(&["github", "gitlab.com", "linear"], false),
        serving_every(),
        only_acme(),
        stored(CLAUDE_CODE, "201 Created", 1),
    ]);
    let opener = Opener::new(0);
    // `claude` alone is on `PATH`.
    let claude = FakeProgram::claude(SetupToken::Prints);
    let output = run_every(&server, &credential, &opener, &[&claude], &[]);

    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    let text = stdout(&output);
    let report = summary_report(&text, "claude_code");
    assert!(report[0].starts_with('✗'), "{text}");
    assert!(report.contains(&"codex: failed"), "{text}");
    let remedies = remedies(&report);
    assert!(
        remedies.iter().any(|line| {
            !line.trim_start().starts_with("um ") && line.contains("codex") && line.contains("PATH")
        }),
        "{text}"
    );
    for command in ["um grant create codex --from-file PATH", "um grant create"] {
        assert!(
            remedies.iter().any(|line| line.trim() == command),
            "{command}: {text}"
        );
    }
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_EVERY_KIND, READ_SERVED, LIST_MEMBERSHIPS, IMPORT_GRANT]
    );
}

// REQ-GRANT-75
#[test]
fn a_member_who_holds_every_provider_is_told_so_after_only_the_list() {
    for json in [false, true] {
        let (server, _directory, credential) =
            prepared(vec![holding(&["github", "gitlab.com", "linear"], true)]);
        let opener = Opener::new(0);
        let args: &[&str] = if json { &["--json"] } else { &[] };
        let output = run_every(&server, &credential, &opener, &[], args);

        assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
        if json {
            let events = events(&output);
            assert_eq!(events.len(), 1);
            assert_eq!(summary(&events), ("nothing_to_create".to_owned(), vec![]));
        } else {
            assert!(!output.stdout.is_empty());
        }
        assert!(opener.opened(0).is_empty());
        assert_eq!(request_lines(&server.finish()), [LIST_EVERY_KIND]);
    }
}

// REQ-GRANT-72
// REQ-GRANT-75
#[test]
fn an_unresolved_organization_passes_over_the_coding_agents_and_the_consents_still_run() {
    let cases = [
        (
            "several",
            memberships(&[
                (ORGANIZATION_ID, "acme"),
                (GLOBEX_ORGANIZATION_ID, "globex"),
            ]),
        ),
        ("none", memberships(&[])),
    ];
    for (case, organizations) in cases {
        for json in [true, false] {
            let (server, _directory, credential) = prepared(
                [
                    vec![holding(&[], false), serving_every(), organizations.clone()],
                    approved("github").to_vec(),
                    approved("gitlab.com").to_vec(),
                    approved("linear").to_vec(),
                ]
                .concat(),
            );
            let opener = Opener::new(0);
            let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
            let claude = FakeProgram::claude(SetupToken::Prints);
            let args: &[&str] = if json { &["--json"] } else { &[] };
            let output = run_every(&server, &credential, &opener, &[&codex, &claude], args);

            // The grants made count as they do beside a provider the deployment does not serve.
            assert_eq!(output.status.code(), Some(0), "{case}: {}", stderr(&output));
            assert_eq!(opener.opened(3).len(), 3, "{case}");
            assert_eq!(codex.recorded("arguments"), None, "{case}");
            assert_eq!(claude.recorded("arguments"), None, "{case}");
            if json {
                let events = events(&output);
                let passed_over: Vec<_> = events
                    .iter()
                    .filter(|event| event["event"] == "import_result")
                    .map(|event| event["outcome"].as_str().unwrap())
                    .collect();
                assert_eq!(
                    passed_over,
                    ["organization_required", "organization_required"],
                    "{case}"
                );
                assert_eq!(
                    summary(&events),
                    (
                        "completed".to_owned(),
                        outcomes(&[
                            ("github", "created"),
                            ("gitlab.com", "created"),
                            ("linear", "created"),
                            ("claude_code", "organization_required"),
                            ("codex", "organization_required")
                        ])
                    ),
                    "{case}"
                );
            } else {
                let text = stderr(&output);
                assert!(
                    text.contains("um grant create --organization ORGANIZATION"),
                    "{case}: {text}"
                );
            }
            let poll = poll_line();
            assert_eq!(
                request_lines(&server.finish()),
                [
                    LIST_EVERY_KIND,
                    READ_SERVED,
                    LIST_MEMBERSHIPS,
                    START_CONSENT,
                    poll.as_str(),
                    START_CONSENT,
                    poll.as_str(),
                    START_CONSENT,
                    poll.as_str()
                ],
                "{case}"
            );
        }

        // With nothing else to make, passing over the coding agents makes no grant.
        let (server, _directory, credential) = prepared(vec![
            holding(&["github", "gitlab.com", "linear"], false),
            serving_every(),
            organizations,
        ]);
        let opener = Opener::new(0);
        let output = run_every(&server, &credential, &opener, &[], &["--json"]);

        assert_eq!(output.status.code(), Some(1), "{case}: {}", stderr(&output));
        assert_eq!(
            summary(&events(&output)),
            (
                "failed".to_owned(),
                outcomes(&[
                    ("claude_code", "organization_required"),
                    ("codex", "organization_required")
                ])
            ),
            "{case}"
        );
        assert_eq!(
            request_lines(&server.finish()),
            [LIST_EVERY_KIND, READ_SERVED, LIST_MEMBERSHIPS],
            "{case}"
        );
    }
}

// REQ-GRANT-72
// REQ-GRANT-75
#[test]
fn a_coding_agent_held_only_in_another_organization_is_selected_for_the_one_named() {
    let mut claude_in_globex = claude_grant_of(OTHER_NATIVE_GRANT_ID);
    claude_in_globex["organizationId"] = GLOBEX_ORGANIZATION_ID.into();
    let held = page(
        &[
            oauth_grant(OAUTH_GRANT_ID, "github", "active"),
            oauth_grant(OAUTH_GRANT_ID, "gitlab.com", "active"),
            oauth_grant(OAUTH_GRANT_ID, "linear", "active"),
            codex_grant(NATIVE_GRANT_ID, ORGANIZATION_ID, "active"),
            claude_in_globex,
        ],
        None,
    );
    let (server, _directory, credential) = prepared(vec![
        held,
        serving_every(),
        stored_in(GLOBEX_ORGANIZATION_ID, CODEX, "201 Created", 1),
    ]);
    let opener = Opener::new(0);
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let claude = FakeProgram::claude(SetupToken::Prints);
    let output = run_every(
        &server,
        &credential,
        &opener,
        &[&codex, &claude],
        &["--organization", GLOBEX_ORGANIZATION_ID, "--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(
        summary(&events(&output)),
        ("completed".to_owned(), outcomes(&[("codex", "created")]))
    );
    assert_eq!(codex.recorded("arguments").unwrap(), "login\n");
    assert_eq!(claude.recorded("arguments"), None);
    let requests = server.finish();
    assert_eq!(
        request_lines(&requests),
        [LIST_EVERY_KIND, READ_SERVED, IMPORT_GRANT]
    );
    assert_imported(&requests[2], GLOBEX_ORGANIZATION_ID, CODEX);
}

// REQ-GRANT-75
// REQ-GRANT-79
#[test]
fn an_option_that_needs_a_provider_or_all_with_one_exits_2_before_any_request() {
    let directory = tempfile::tempdir().unwrap();
    let login = private_file(directory.path(), "auth.json", CODEX_MATERIAL.as_bytes());
    let login = login.to_str().unwrap();
    let cases: [&[&str]; 6] = [
        &["--from-file", login],
        &["--token-file", login],
        &["--replace"],
        &["--all", "--replace"],
        &["--all", "linear"],
        &["codex", "--all"],
    ];
    for args in cases {
        let (server, _directory, credential) = prepared(vec![]);
        let opener = Opener::new(0);
        let output = run_every(&server, &credential, &opener, &[], args);

        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            stderr(&output)
        );
        assert!(output.stdout.is_empty(), "{args:?}");
        assert!(server.finish().is_empty(), "{args:?}");
    }
}

// REQ-GRANT-71
#[test]
fn a_coding_agent_refuses_the_options_that_wait_for_a_consent_which_still_apply_without_one() {
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let claude = FakeProgram::claude(SetupToken::Prints);
    let programs: [&FakeProgram; 2] = [&codex, &claude];
    let cases: [&[&str]; 5] = [
        &["codex", "--no-wait"],
        &["codex", "--timeout", "30s"],
        &["claude_code", "--no-wait"],
        &["claude_code", "--timeout", "30s"],
        &["claude_code", "--login"],
    ];
    for args in cases {
        let (server, _directory, credential) = prepared(vec![]);
        let opener = Opener::new(0);
        let output = run_every(&server, &credential, &opener, &programs, args);

        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            stderr(&output)
        );
        assert!(output.stdout.is_empty(), "{args:?}");
        assert_eq!(codex.recorded("arguments"), None, "{args:?}");
        assert_eq!(claude.recorded("arguments"), None, "{args:?}");
        assert!(server.finish().is_empty(), "{args:?}");
    }

    // `--login` is Codex's: the remedy names Claude Code's own sign-in, the default.
    let (server, _directory, credential) = prepared(vec![]);
    let opener = Opener::new(0);
    let output = run_every(
        &server,
        &credential,
        &opener,
        &programs,
        &["claude_code", "--login"],
    );
    let shown = stderr(&output);
    assert!(
        shown
            .lines()
            .any(|line| line.trim() == "um grant create claude_code"),
        "{shown}"
    );

    // Without a provider, both still apply to the consents.
    let (server, _directory, credential) = prepared(vec![
        holding(&["github", "gitlab.com"], true),
        json_http_response("201 Created", consent_of("linear", "pending")),
    ]);
    let output = run_every(
        &server,
        &credential,
        &opener,
        &programs,
        &["--no-wait", "--json"],
    );
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(
        summary(&events(&output)).1,
        outcomes(&[("linear", "started")])
    );
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_EVERY_KIND, START_CONSENT]
    );

    let (server, _directory, credential) = prepared(
        [
            vec![holding(&["github", "gitlab.com"], true)],
            approved("linear").to_vec(),
        ]
        .concat(),
    );
    let output = run_every(
        &server,
        &credential,
        &opener,
        &programs,
        &["--timeout", "30s", "--json"],
    );
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(
        summary(&events(&output)).1,
        outcomes(&[("linear", "created")])
    );
    assert_eq!(codex.recorded("arguments"), None);
    assert_eq!(claude.recorded("arguments"), None);
}

// A root workflow that runs `child.yaml` and forwards a Linear credential and a credential of no
// known kind.
const ROOT_WORKFLOW: &str = r#"schemaVersion: 1
environmentPassthrough: [LINEAR_API_KEY, BUILDKITE_API_TOKEN]
steps:
  stage:
    kind: workflow
    workflow: child.yaml
    on:
      succeeded: exit.succeeded
      failed: exit.failed
"#;

// The child workflow: a Claude Code agent step and a command step.
const CHILD_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  writing:
    harness:
      kind: claude_code
      config:
        model: claude-opus-4-1
        effort: xhigh
steps:
  draft:
    kind: agent
    agent:
      profile: writing
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
  check:
    kind: cmd
    dependsOn: [draft]
    command:
      argv: ["./check"]
"#;

// A workflow that requires `glab`, which calls for the kind `gitlab`.
const GITLAB_WORKFLOW: &str = r#"schemaVersion: 1
requires:
  tools: [glab]
steps:
  build:
    kind: cmd
    command:
      argv: ["./build"]
"#;

/// A directory of workflow files, which is a Git repository, as the fake `git` finds one, when
/// it holds a `.git` marker.
struct Tree {
    _temporary: tempfile::TempDir,
    root: std::path::PathBuf,
}

impl Tree {
    fn new(files: &[(&str, &str)], repository: bool) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("source");
        fs::create_dir_all(root.join("prompts")).unwrap();
        for &(path, contents) in files {
            fs::write(root.join(path), contents).unwrap();
        }
        if repository {
            fs::create_dir(root.join(".git")).unwrap();
        }
        Self {
            _temporary: temporary,
            root,
        }
    }

    /// `root.yaml` running `child.yaml`: Linear, Claude Code, and as unknown a forwarded variable
    /// of no known kind and a command step.
    fn linear_and_claude_code(repository: bool) -> Self {
        Self::new(
            &[
                ("root.yaml", ROOT_WORKFLOW),
                ("child.yaml", CHILD_WORKFLOW),
                ("prompts/system.md", "System prompt."),
                ("prompts/message.md", "Message."),
            ],
            repository,
        )
    }

    fn gitlab() -> Self {
        Self::new(&[("gitlab.yaml", GITLAB_WORKFLOW)], true)
    }

    fn path(&self, file: &str) -> String {
        self.root.join(file).to_str().unwrap().to_owned()
    }
}

/// A fake `git` that records each invocation's arguments beside itself, answers
/// `rev-parse --show-toplevel` with the nearest directory at or above the physical working
/// directory that holds a `.git` marker, and fails anything else, so no `origin` remote is found.
pub(super) struct Git(pub(super) tempfile::TempDir);

impl Git {
    pub(super) fn new() -> Self {
        Self(fake_git(
            "printf '%s\\n' \"$*\" >> \"${0%/*}/invoked\"\n\
             if [ \"$1 $2\" = 'rev-parse --show-toplevel' ]; then\n\
             directory=$(pwd -P)\n\
             while [ -n \"$directory\" ]; do\n\
             if [ -e \"$directory/.git\" ]; then echo \"$directory\"; exit 0; fi\n\
             directory=${directory%/*}\n\
             done\n\
             echo 'fatal: not a git repository' >&2\n\
             exit 128\n\
             fi\n\
             exit 1",
        ))
    }

    fn in_tree<'a>(&'a self, tree: &'a Tree) -> Option<Checkout<'a>> {
        Some(Checkout {
            directory: &tree.root,
            git: self.0.path(),
        })
    }

    pub(super) fn invocations(&self) -> Vec<String> {
        fs::read_to_string(self.0.path().join("invoked"))
            .map(|record| record.lines().map(str::to_owned).collect())
            .unwrap_or_default()
    }
}

// The entry of the tree's plan for each provider in `entries`: its name, and its origins as the
// doctor's JSON carries them.
fn planned(event: &serde_json::Value, entries: &str) -> Vec<(String, serde_json::Value)> {
    event[entries]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["provider"].as_str().unwrap().to_owned(),
                entry["origins"].clone(),
            )
        })
        .collect()
}

fn providers_of(event: &serde_json::Value, entries: &str) -> Vec<String> {
    planned(event, entries)
        .into_iter()
        .map(|(provider, _)| provider)
        .collect()
}

const CLAUDE_CODE_ORIGINS: &str = r#"[{"declaration": "agent_profile", "workflow": "child.yaml", "node": {"kind": "step", "id": "draft"}, "name": "writing"}]"#;
const LINEAR_ORIGINS: &str =
    r#"[{"declaration": "forwarded_variable", "workflow": "root.yaml", "name": "LINEAR_API_KEY"}]"#;

fn origins(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap()
}

// REQ-GRANT-76
#[test]
fn create_workflow_signs_in_to_only_the_providers_its_tree_calls_for() {
    let tree = Tree::linear_and_claude_code(true);
    let git = Git::new();
    let (server, _directory, credential) = prepared(
        [
            vec![nothing_held(), serving_every(), only_acme()],
            approved("linear").to_vec(),
            vec![stored(CLAUDE_CODE, "201 Created", 1)],
        ]
        .concat(),
    );
    let opener = Opener::new(0);
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    let claude = FakeProgram::claude(SetupToken::Prints);
    let output = run_in(
        &server,
        &credential,
        &opener,
        &[&codex, &claude],
        git.in_tree(&tree),
        &["--workflow", "root.yaml"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(opener.opened(1), [format!("1 {}", consent_url("linear"))]);
    assert_eq!(claude.recorded("arguments").unwrap(), "setup-token\n");
    assert_eq!(codex.recorded("arguments"), None);
    let text = stdout(&output);
    for fact in [
        "root.yaml",
        "child.yaml",
        "LINEAR_API_KEY",
        "BUILDKITE_API_TOKEN",
        "linear: created",
        "claude_code: created",
    ] {
        assert!(text.contains(fact), "{fact}: {text}");
    }
    for absent in ["github:", "gitlab.com:", "codex:"] {
        assert!(!text.contains(absent), "{absent}: {text}");
    }
    // Git is asked for the repository's root and its `origin` remote, and nothing else.
    assert_eq!(
        git.invocations(),
        ["rev-parse --show-toplevel", "remote get-url origin"]
    );
    let requests = server.finish();
    assert_member_requests(&requests);
    let poll = poll_line();
    assert_eq!(
        request_lines(&requests),
        [
            LIST_EVERY_KIND,
            READ_SERVED,
            LIST_MEMBERSHIPS,
            START_CONSENT,
            poll.as_str(),
            IMPORT_GRANT
        ]
    );
    assert_eq!(start_body(&requests[3]), "linear");
    assert_imported(&requests[5], ORGANIZATION_ID, CLAUDE_CODE);
}

// REQ-GRANT-76
#[test]
fn a_held_provider_is_passed_over_and_json_carries_the_plan_before_the_sign_ins() {
    let tree = Tree::linear_and_claude_code(true);
    let git = Git::new();
    let (server, _directory, credential) = prepared(vec![
        holding(&["linear"], false),
        serving_every(),
        only_acme(),
        stored(CLAUDE_CODE, "201 Created", 1),
    ]);
    let opener = Opener::new(0);
    let claude = FakeProgram::claude(SetupToken::Prints);
    let output = run_in(
        &server,
        &credential,
        &opener,
        &[&claude],
        git.in_tree(&tree),
        &["--workflow", "root.yaml", "--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let events = events(&output);
    let plan = &events[0];
    assert_eq!(plan["event"], "workflow_requirements", "{events:?}");
    assert_eq!(
        plan["workflows"],
        serde_json::json!(["child.yaml", "root.yaml"])
    );
    assert_eq!(
        planned(plan, "selected"),
        [("claude_code".to_owned(), origins(CLAUDE_CODE_ORIGINS))]
    );
    assert_eq!(
        planned(plan, "held"),
        [("linear".to_owned(), origins(LINEAR_ORIGINS))]
    );
    // What the declarations cannot show is listed, never guessed at.
    assert_eq!(
        plan["unknown"],
        serde_json::json!([
            {"declaration": "command", "workflow": "child.yaml", "node": {"kind": "step", "id": "check"}},
            {"declaration": "agent", "workflow": "child.yaml", "node": {"kind": "step", "id": "draft"}},
            {"declaration": "forwarded_variable", "workflow": "root.yaml", "name": "BUILDKITE_API_TOKEN"},
            {"declaration": "repository", "name": "origin"}
        ])
    );
    assert_eq!(events[1]["event"], "import_result");
    assert_eq!(events[1]["grant"]["provider"], "claude_code");
    assert_eq!(
        summary(&events),
        (
            "completed".to_owned(),
            outcomes(&[("claude_code", "created")])
        )
    );
    assert!(opener.opened(0).is_empty());
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_EVERY_KIND, READ_SERVED, LIST_MEMBERSHIPS, IMPORT_GRANT]
    );
}

// The Claude Code grant `id` in Acme, authorized for a runner that reported it ready at its
// generation and refused at its last use, so its status there is `reconnect_required`
// (REQ-GRANT-53).
fn claude_grant_refused(id: &str) -> serde_json::Value {
    let mut grant = native_grant_in_use();
    grant["id"] = id.into();
    grant["provider"] = "claude_code".into();
    grant["authorizations"][0]["report"]["lastUse"] = "refused".into();
    grant
}

// The member's grants: a Linear grant whose consent lapsed and a Claude Code grant a runner
// reported refused.
fn needing_reconnect() -> Vec<u8> {
    page(
        &[
            oauth_grant(OAUTH_GRANT_ID, "linear", "reconnect_required"),
            claude_grant_refused(NATIVE_GRANT_ID),
        ],
        None,
    )
}

// REQ-GRANT-79
// REQ-GRANT-76
// REQ-GRANT-73
#[test]
fn create_workflow_replaces_a_grant_that_needs_reconnecting_off_a_terminal_only_with_replace() {
    let tree = Tree::linear_and_claude_code(true);
    let git = Git::new();
    let opener = Opener::new(0);

    // Without --replace nothing is settled: both are passed over, and no sign-in starts.
    let (server, _directory, credential) = prepared(vec![needing_reconnect(), serving_every()]);
    let claude = FakeProgram::claude(SetupToken::Prints);
    let kept = run_in(
        &server,
        &credential,
        &opener,
        &[&claude],
        git.in_tree(&tree),
        &["--workflow", "root.yaml", "--json"],
    );

    assert_eq!(kept.status.code(), Some(1), "{}", stderr(&kept));
    let kept_events = events(&kept);
    let plan = &kept_events[0];
    assert_eq!(plan["event"], "workflow_requirements", "{kept_events:?}");
    assert!(providers_of(plan, "selected").is_empty(), "{plan}");
    assert!(providers_of(plan, "reconnect").is_empty(), "{plan}");
    assert_eq!(providers_of(plan, "held"), ["linear", "claude_code"]);
    assert_eq!(kept_events.len(), 2, "{kept_events:?}");
    assert_eq!(
        summary(&kept_events),
        (
            "failed".to_owned(),
            outcomes(&[
                ("linear", "reconnect_required"),
                ("claude_code", "reconnect_required")
            ])
        )
    );
    assert_eq!(claude.recorded("arguments"), None);
    assert!(opener.opened(0).is_empty());
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_EVERY_KIND, READ_SERVED]
    );

    // The report names the command that replaces each grant passed over.
    let (server, _directory, credential) = prepared(vec![needing_reconnect(), serving_every()]);
    let told = run_in(
        &server,
        &credential,
        &opener,
        &[&claude],
        git.in_tree(&tree),
        &["--workflow", "root.yaml"],
    );

    assert_eq!(told.status.code(), Some(1), "{}", stderr(&told));
    let report = stdout(&told);
    for provider in ["linear", "claude_code"] {
        assert!(
            report.contains(&format!("um grant create {provider} --replace")),
            "{report}"
        );
    }
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_EVERY_KIND, READ_SERVED]
    );

    // With --replace the Linear consent runs and the Claude Code sign-in is stored as the held
    // grant's next generation, not as a new import.
    let (server, _directory, credential) = prepared(
        [
            vec![needing_reconnect(), serving_every()],
            approved("linear").to_vec(),
            vec![stored(CLAUDE_CODE, "200 OK", 3)],
        ]
        .concat(),
    );
    let claude = FakeProgram::claude(SetupToken::Prints);
    let replaced = run_in(
        &server,
        &credential,
        &opener,
        &[&claude],
        git.in_tree(&tree),
        &["--workflow", "root.yaml", "--replace", "--json"],
    );

    assert_eq!(replaced.status.code(), Some(0), "{}", stderr(&replaced));
    let events = events(&replaced);
    let plan = &events[0];
    assert!(providers_of(plan, "selected").is_empty(), "{plan}");
    assert!(providers_of(plan, "held").is_empty(), "{plan}");
    assert_eq!(
        planned(plan, "reconnect"),
        [
            ("linear".to_owned(), origins(LINEAR_ORIGINS)),
            ("claude_code".to_owned(), origins(CLAUDE_CODE_ORIGINS))
        ]
    );
    let import = events
        .iter()
        .find(|event| event["event"] == "import_result")
        .unwrap();
    assert_eq!(import["outcome"], "replaced");
    assert_eq!(import["grant"]["id"], NATIVE_GRANT_ID);
    assert_eq!(
        summary(&events),
        (
            "completed".to_owned(),
            outcomes(&[("linear", "replaced"), ("claude_code", "replaced")])
        )
    );
    assert_eq!(claude.recorded("arguments").unwrap(), "setup-token\n");
    assert_eq!(opener.opened(1), [format!("1 {}", consent_url("linear"))]);
    let requests = server.finish();
    let poll = poll_line();
    let replace = replace_line(NATIVE_GRANT_ID);
    assert_eq!(
        request_lines(&requests),
        [
            LIST_EVERY_KIND,
            READ_SERVED,
            START_CONSENT,
            poll.as_str(),
            replace.as_str()
        ]
    );
    assert_eq!(start_body(&requests[2]), "linear");
    assert_eq!(
        request_body(&requests[4]),
        serde_json::json!({
            "recordSchemaVersion": 1,
            "material": encoded(CLAUDE_TOKEN.as_bytes())
        })
    );
}

// REQ-GRANT-79
// REQ-GRANT-73
#[test]
fn create_workflow_on_a_terminal_asks_before_it_replaces_a_grant_that_needs_reconnecting() {
    let tree = Tree::linear_and_claude_code(true);
    let git = Git::new();
    for (answer, replaced) in [("y\n", true), ("\n", false)] {
        // The Claude Code grant is healthy, so it is held and never asked about.
        let mut responses = vec![page(
            &[
                oauth_grant(OAUTH_GRANT_ID, "linear", "reconnect_required"),
                claude_grant_of(NATIVE_GRANT_ID),
            ],
            None,
        )];
        if replaced {
            responses.extend(approved("linear"));
        }
        let (server, _directory, credential) = prepared(responses);
        let opener = Opener::new(0);
        let mut command = create_command(
            &server,
            &credential,
            &opener,
            Place::Remote,
            &["--workflow", "root.yaml", "--json"],
        );
        command
            .env(
                "PATH",
                std::env::join_paths([opener.directory.path(), git.0.path()]).unwrap(),
            )
            .current_dir(&tree.root);
        let (transcript, output) = answered(command, answer);

        assert!(transcript.contains(OAUTH_GRANT_ID), "{transcript}");
        // The question precedes the plan and any sign-in.
        assert!(!transcript.contains("── linear ──"), "{transcript}");
        assert_eq!(
            output.status.code(),
            Some(if replaced { 0 } else { 1 }),
            "{answer:?}"
        );
        let events = events(&output);
        let plan = &events[0];
        assert_eq!(plan["event"], "workflow_requirements", "{events:?}");
        let (reconnect, held, outcome, summary_outcome): (&[&str], &[&str], _, _) = if replaced {
            (&["linear"], &["claude_code"], "replaced", "completed")
        } else {
            (
                &[],
                &["linear", "claude_code"],
                "reconnect_required",
                "failed",
            )
        };
        assert_eq!(providers_of(plan, "reconnect"), reconnect, "{answer:?}");
        assert_eq!(providers_of(plan, "held"), held, "{answer:?}");
        assert_eq!(
            summary(&events),
            (summary_outcome.to_owned(), outcomes(&[("linear", outcome)])),
            "{answer:?}"
        );
        let poll = poll_line();
        let expected: &[&str] = if replaced {
            &[LIST_EVERY_KIND, START_CONSENT, poll.as_str()]
        } else {
            &[LIST_EVERY_KIND]
        };
        assert_eq!(request_lines(&server.finish()), expected, "{answer:?}");
    }
}

// REQ-GRANT-79
#[test]
fn create_workflow_never_replaces_a_provider_held_healthy_beside_one_that_needs_reconnecting() {
    let tree = Tree::linear_and_claude_code(true);
    let git = Git::new();
    // Linear is held through one working grant and one whose consent lapsed; Claude Code
    // through two grants in Acme that both need reconnecting, between which the command
    // cannot choose.
    let (server, _directory, credential) = prepared(vec![
        page(
            &[
                oauth_grant(OAUTH_GRANT_ID, "linear", "active"),
                oauth_grant(LINEAR_GRANT_ID, "linear", "reconnect_required"),
                claude_grant_refused(NATIVE_GRANT_ID),
                claude_grant_refused(OTHER_NATIVE_GRANT_ID),
            ],
            None,
        ),
        serving_every(),
    ]);
    let opener = Opener::new(0);
    let claude = FakeProgram::claude(SetupToken::Prints);
    let output = run_in(
        &server,
        &credential,
        &opener,
        &[&claude],
        git.in_tree(&tree),
        &["--workflow", "root.yaml", "--replace", "--json"],
    );

    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    let events = events(&output);
    let plan = &events[0];
    assert!(providers_of(plan, "reconnect").is_empty(), "{plan}");
    assert_eq!(providers_of(plan, "held"), ["linear", "claude_code"]);
    assert_eq!(
        summary(&events),
        (
            "failed".to_owned(),
            outcomes(&[("claude_code", "reconnect_required")])
        )
    );
    assert_eq!(claude.recorded("arguments"), None);
    assert!(opener.opened(0).is_empty());
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_EVERY_KIND, READ_SERVED]
    );
}

// REQ-GRANT-75
// REQ-GRANT-79
#[test]
fn create_all_selects_nothing_for_a_grant_that_needs_reconnecting() {
    let (server, _directory, credential) = prepared(vec![page(
        &[
            oauth_grant(OAUTH_GRANT_ID, "github", "active"),
            oauth_grant(OAUTH_GRANT_ID, "gitlab.com", "active"),
            oauth_grant(LINEAR_GRANT_ID, "linear", "reconnect_required"),
            native_grant(OTHER_NATIVE_GRANT_ID, "active"),
            claude_grant_refused(NATIVE_GRANT_ID),
        ],
        None,
    )]);
    let opener = Opener::new(0);
    let claude = FakeProgram::claude(SetupToken::Prints);
    let output = run_every(
        &server,
        &credential,
        &opener,
        &[&claude],
        &["--all", "--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    assert_eq!(
        summary(&events(&output)),
        ("nothing_to_create".to_owned(), Vec::new())
    );
    assert_eq!(claude.recorded("arguments"), None);
    assert_eq!(request_lines(&server.finish()), [LIST_EVERY_KIND]);
}

// `um runner doctor --workflow FILE --connect`: the flow of `um grant create --workflow` over the
// doctor's derivation, then the doctor's report of the member's grants, read again.

/// What one command run in a checkout of a tree left: its output, the requests its deployment
/// saw, and the fake `claude` it found.
struct Ran {
    output: Output,
    requests: Vec<String>,
    claude: FakeProgram,
}

/// Runs the command whose leading words are `command` with `args` in a checkout of `tree`,
/// against a deployment answering `script`, with `claude` and the opener on `PATH`.
fn ran_in(
    tree: &Tree,
    claude: FakeProgram,
    script: Vec<Vec<u8>>,
    command: &[&str],
    args: &[&str],
) -> Ran {
    let git = Git::new();
    let opener = Opener::new(0);
    let (server, _directory, credential) = prepared(script);
    let output = run_command_in(
        &server,
        &credential,
        &opener,
        &[&claude],
        git.in_tree(tree),
        command,
        args,
    );
    Ran {
        output,
        requests: server.finish(),
        claude,
    }
}

/// The events of `output`, each with its `deployment` set aside, since two scripted deployments
/// have different addresses.
fn events_elsewhere(output: &Output) -> Vec<serde_json::Value> {
    events(output)
        .into_iter()
        .map(|mut event| {
            event.as_object_mut().unwrap().remove("deployment");
            event
        })
        .collect()
}

/// The `report` event that ends the doctor's output, once each event before it is checked to
/// be the one `um grant create` printed in its place against the same script (INV-CLI-15).
fn report_after(create: &Output, doctor: &Output) -> serde_json::Value {
    let mut events = events(doctor);
    let report = events.pop().unwrap();
    assert_eq!(report["event"], "report", "{report}");
    let flow: Vec<_> = events_elsewhere(doctor)
        .into_iter()
        .take(events.len())
        .collect();
    assert_eq!(flow, events_elsewhere(create));
    report
}

/// The doctor's events but the last, which is its report.
fn flow_events(output: &Output) -> Vec<serde_json::Value> {
    let mut events = events(output);
    assert_eq!(events.pop().unwrap()["event"], "report", "{events:?}");
    events
}

/// The doctor's deployment saw the requests `create`'s saw, then one more grants list for the
/// report.
fn assert_flow_requests_then_a_list(doctor: &Ran, create: &Ran) {
    let lines = request_lines(&doctor.requests);
    let (last, flow) = lines.split_last().unwrap();
    assert_eq!(flow, request_lines(&create.requests));
    assert_eq!(*last, LIST_EVERY_KIND);
}

/// The member's grants once a Linear consent and a Claude Code sign-in made one of each, no
/// runner authorized for either.
fn made_unauthorized() -> Vec<u8> {
    let mut linear = oauth_grant(OAUTH_GRANT_ID, "linear", "active");
    linear["authorizations"] = serde_json::json!([]);
    page(&[linear, claude_grant_of(NATIVE_GRANT_ID)], None)
}

/// The script of a member who holds nothing the tree calls for: the grants list, the providers
/// served, the one membership, the Linear consent approved and the Claude Code import.
fn signing_in_to_both() -> Vec<Vec<u8>> {
    [
        vec![nothing_held(), serving_every(), only_acme()],
        approved("linear").to_vec(),
        vec![stored(CLAUDE_CODE, "201 Created", 1)],
    ]
    .concat()
}

/// A grant `grant` of a report's requirement carries with no runner authorized: none reported,
/// and its own status `not_connected` with `authorize_runner` (REQ-CREDREADY-23).
fn assert_authorizes_no_runner(grant: &serde_json::Value) {
    assert_eq!(grant["authorizations"], serde_json::json!([]), "{grant}");
    assert_eq!(grant["availability"], "not_connected", "{grant}");
    assert_eq!(grant["nextAction"], "authorize_runner", "{grant}");
}

const CONNECT: [&str; 4] = ["--workflow", "root.yaml", "--connect", "--json"];

// REQ-CREDREADY-24
// REQ-CREDREADY-25
// INV-CLI-15
#[test]
fn doctor_connect_runs_the_grant_flow_then_reports_the_grants_read_again() {
    let tree = Tree::linear_and_claude_code(true);
    let create = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Prints),
        signing_in_to_both(),
        GRANT_CREATE,
        &["--workflow", "root.yaml", "--json"],
    );
    let doctor = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Prints),
        [signing_in_to_both(), vec![made_unauthorized()]].concat(),
        RUNNER_DOCTOR,
        &CONNECT,
    );

    assert_eq!(
        create.output.status.code(),
        Some(0),
        "{}",
        stderr(&create.output)
    );
    assert_eq!(
        doctor.output.status.code(),
        Some(0),
        "{}",
        stderr(&doctor.output)
    );
    let report = report_after(&create.output, &doctor.output);
    let flow = flow_events(&doctor.output);
    assert_eq!(flow[0]["event"], "workflow_requirements", "{flow:?}");
    assert_eq!(
        summary(&flow),
        (
            "completed".to_owned(),
            outcomes(&[("linear", "created"), ("claude_code", "created")])
        )
    );
    assert_eq!(report["schemaVersion"], 1);
    assert_eq!(
        report["workflows"],
        serde_json::json!(["child.yaml", "root.yaml"])
    );
    for (kind, id) in [("linear", OAUTH_GRANT_ID), ("claude_code", NATIVE_GRANT_ID)] {
        let grant = held_grant(&report, kind);
        assert_eq!(grant["id"], id, "{kind}");
        assert_authorizes_no_runner(grant);
    }
    assert_eq!(
        doctor.claude.recorded("arguments").unwrap(),
        "setup-token\n"
    );
    assert_member_requests(&doctor.requests);
    assert_flow_requests_then_a_list(&doctor, &create);
}

// The workflow of one command step, which calls for no kind.
const NO_KIND_WORKFLOW: &str = r#"schemaVersion: 1
steps:
  build:
    kind: cmd
    command:
      argv: ["./build"]
"#;

// REQ-CREDREADY-25
// REQ-CREDREADY-9
#[test]
fn doctor_connect_for_a_member_who_holds_each_kind_starts_no_sign_in_and_still_reports() {
    let tree = Tree::linear_and_claude_code(true);
    let held = || holding(&["linear"], true);
    let doctor = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Prints),
        vec![held(), held()],
        RUNNER_DOCTOR,
        &CONNECT,
    );

    assert_eq!(
        doctor.output.status.code(),
        Some(0),
        "{}",
        stderr(&doctor.output)
    );
    let flow = flow_events(&doctor.output);
    assert_eq!(flow.len(), 2, "{flow:?}");
    assert_eq!(summary(&flow), ("nothing_to_create".to_owned(), Vec::new()));
    let report = events(&doctor.output).pop().unwrap();
    assert_eq!(held_grant(&report, "linear")["id"], OAUTH_GRANT_ID);
    assert_eq!(
        held_grant(&report, "claude_code")["id"],
        OTHER_NATIVE_GRANT_ID
    );
    assert_eq!(doctor.claude.recorded("arguments"), None);
    assert_eq!(
        request_lines(&doctor.requests),
        [LIST_EVERY_KIND, LIST_EVERY_KIND]
    );

    // Without --json the report follows the flow's on standard output.
    let human = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Prints),
        vec![held(), held()],
        RUNNER_DOCTOR,
        &CONNECT[..3],
    );
    assert_eq!(
        human.output.status.code(),
        Some(0),
        "{}",
        stderr(&human.output)
    );
    let text = stdout(&human.output);
    for fact in [OAUTH_GRANT_ID, OTHER_NATIVE_GRANT_ID, RUNNER_ID] {
        assert!(text.contains(fact), "{fact}: {text}");
    }
    assert_eq!(
        request_lines(&human.requests),
        [LIST_EVERY_KIND, LIST_EVERY_KIND]
    );

    // A tree that calls for no kind takes the flow's path for nothing to create, and neither the
    // flow nor the report asks Cloud anything.
    let tree = Tree::new(&[("build.yaml", NO_KIND_WORKFLOW)], true);
    let nothing = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Prints),
        Vec::new(),
        RUNNER_DOCTOR,
        &["--workflow", "build.yaml", "--connect", "--json"],
    );
    assert_eq!(
        nothing.output.status.code(),
        Some(0),
        "{}",
        stderr(&nothing.output)
    );
    let flow = flow_events(&nothing.output);
    assert_eq!(summary(&flow), ("nothing_to_create".to_owned(), Vec::new()));
    let report = events(&nothing.output).pop().unwrap();
    assert_eq!(report["requirements"], serde_json::json!([]));
    assert_eq!(report["deployment"], serde_json::Value::Null);
    assert!(nothing.requests.is_empty(), "{:?}", nothing.requests);
}

// REQ-CREDREADY-25
#[test]
fn doctor_connect_whose_sign_in_fails_exits_1_and_still_reports() {
    let tree = Tree::linear_and_claude_code(true);
    let mut linear = oauth_grant(OAUTH_GRANT_ID, "linear", "active");
    linear["authorizations"] = serde_json::json!([]);
    let doctor = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Exits(1)),
        [
            vec![nothing_held(), serving_every(), only_acme()],
            approved("linear").to_vec(),
            vec![page(&[linear], None)],
        ]
        .concat(),
        RUNNER_DOCTOR,
        &CONNECT,
    );

    assert_eq!(
        doctor.output.status.code(),
        Some(1),
        "{}",
        stderr(&doctor.output)
    );
    assert_eq!(
        summary(&flow_events(&doctor.output)),
        (
            "failed".to_owned(),
            outcomes(&[("linear", "created"), ("claude_code", "failed")])
        )
    );
    let report = events(&doctor.output).pop().unwrap();
    assert_authorizes_no_runner(held_grant(&report, "linear"));
    assert_eq!(
        requirement(&report, "claude_code")["grant"],
        serde_json::json!({"provider": "claude_code", "grants": [], "availability": "not_connected", "nextAction": "connect"})
    );
    let poll = poll_line();
    assert_eq!(
        request_lines(&doctor.requests),
        [
            LIST_EVERY_KIND,
            READ_SERVED,
            LIST_MEMBERSHIPS,
            START_CONSENT,
            poll.as_str(),
            LIST_EVERY_KIND
        ]
    );
}

// REQ-CREDREADY-25
#[test]
fn doctor_connect_without_a_session_ends_as_grant_create_does_with_no_report() {
    let tree = Tree::linear_and_claude_code(true);
    let git = Git::new();
    let opener = Opener::new(0);
    let signed_out = |command: &[&str], args: &[&str]| {
        let server = ScriptedServer::respond(Vec::new());
        let directory = private_credential_directory();
        let credential = directory.path().join("credentials.json");
        let claude = FakeProgram::claude(SetupToken::Prints);
        let output = run_command_in(
            &server,
            &credential,
            &opener,
            &[&claude],
            git.in_tree(&tree),
            command,
            args,
        );
        assert_eq!(claude.recorded("arguments"), None);
        assert!(server.finish().is_empty());
        output
    };

    let create = signed_out(GRANT_CREATE, &["--workflow", "root.yaml", "--json"]);
    let doctor = signed_out(RUNNER_DOCTOR, &CONNECT);

    assert_eq!(create.status.code(), Some(3), "{}", stderr(&create));
    assert_eq!(
        doctor.status.code(),
        create.status.code(),
        "{}",
        stderr(&doctor)
    );
    assert_eq!(events_elsewhere(&doctor), events_elsewhere(&create));
    assert!(
        events(&doctor)
            .iter()
            .all(|event| event["event"] != "report"),
        "{}",
        stdout(&doctor)
    );
}

// REQ-CREDREADY-25
// REQ-GRANT-75
#[test]
fn a_signal_that_ends_the_claude_sign_in_ends_doctor_connect_with_its_status_and_no_report() {
    let tree = Tree::linear_and_claude_code(true);
    let doctor = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Interrupts),
        vec![holding(&["linear"], false), serving_every(), only_acme()],
        RUNNER_DOCTOR,
        &CONNECT,
    );

    assert_eq!(
        doctor.output.status.code(),
        Some(143),
        "{}",
        stderr(&doctor.output)
    );
    let events = events(&doctor.output);
    assert_eq!(
        summary(&events),
        (
            "terminated".to_owned(),
            outcomes(&[("claude_code", "terminated")])
        )
    );
    assert_eq!(
        request_lines(&doctor.requests),
        [LIST_EVERY_KIND, READ_SERVED, LIST_MEMBERSHIPS]
    );
}

// REQ-CREDREADY-25
// REQ-CREDREADY-17
#[test]
fn doctor_connect_with_readiness_reads_the_connections_after_the_sign_ins() {
    let tree = Tree::linear_and_claude_code(true);
    let doctor = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Prints),
        [
            vec![nothing_held(), serving_every(), acme()],
            approved("linear").to_vec(),
            vec![
                stored(CLAUDE_CODE, "201 Created", 1),
                connection_page(&[], None),
                acme(),
                made_unauthorized(),
            ],
        ]
        .concat(),
        RUNNER_DOCTOR,
        &[&CONNECT[..], &["--readiness", "--organization-id", "acme"]].concat(),
    );

    // The sign-ins made both grants, and the missing Linear connection fails the report.
    assert_eq!(
        doctor.output.status.code(),
        Some(1),
        "{}",
        stderr(&doctor.output)
    );
    assert_eq!(
        summary(&flow_events(&doctor.output)),
        (
            "completed".to_owned(),
            outcomes(&[("linear", "created"), ("claude_code", "created")])
        )
    );
    let report = events(&doctor.output).pop().unwrap();
    assert_eq!(report["summary"]["missing"], 1, "{report}");
    assert_authorizes_no_runner(held_grant(&report, "linear"));
    let lines = request_lines(&doctor.requests);
    let poll = poll_line();
    assert_eq!(
        lines[..6],
        [
            LIST_EVERY_KIND,
            READ_SERVED,
            READ_ACME,
            START_CONSENT,
            poll.as_str(),
            IMPORT_GRANT
        ]
    );
    assert!(lines[6].starts_with(LINEAR_LIST), "{lines:?}");
    assert_eq!(lines[7..], [READ_ACME, LIST_EVERY_KIND]);
}

// REQ-CREDREADY-24
// REQ-GRANT-79
// INV-CLI-15
#[test]
fn doctor_connect_with_replace_replaces_as_grant_create_does() {
    let tree = Tree::linear_and_claude_code(true);
    let script = || {
        [
            vec![needing_reconnect(), serving_every()],
            approved("linear").to_vec(),
            vec![stored(CLAUDE_CODE, "200 OK", 3)],
        ]
        .concat()
    };
    let create = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Prints),
        script(),
        GRANT_CREATE,
        &["--workflow", "root.yaml", "--replace", "--json"],
    );
    let doctor = ran_in(
        &tree,
        FakeProgram::claude(SetupToken::Prints),
        [script(), vec![made_unauthorized()]].concat(),
        RUNNER_DOCTOR,
        &[&CONNECT[..], &["--replace"]].concat(),
    );

    assert_eq!(
        doctor.output.status.code(),
        Some(0),
        "{}",
        stderr(&doctor.output)
    );
    report_after(&create.output, &doctor.output);
    assert_eq!(
        summary(&flow_events(&doctor.output)),
        (
            "completed".to_owned(),
            outcomes(&[("linear", "replaced"), ("claude_code", "replaced")])
        )
    );
    assert_flow_requests_then_a_list(&doctor, &create);
}

// REQ-CREDREADY-23
#[test]
fn doctor_connect_with_a_runner_reports_each_grant_on_that_runner_alone() {
    let tree = Tree::linear_and_claude_code(true);
    let held = || holding(&["linear"], true);
    let on = |runner: &str| {
        let doctor = ran_in(
            &tree,
            FakeProgram::claude(SetupToken::Prints),
            vec![held(), held()],
            RUNNER_DOCTOR,
            &[&CONNECT[..], &["--runner-id", runner]].concat(),
        );
        assert_eq!(
            doctor.output.status.code(),
            Some(0),
            "{}",
            stderr(&doctor.output)
        );
        events(&doctor.output).pop().unwrap()
    };

    let named = on(RUNNER_ID);
    let other = on(NEW_RUNNER_ID);

    assert_eq!(
        held_grant(&named, "linear")["authorizations"],
        serde_json::json!([{"runnerId": RUNNER_ID, "availability": "available", "nextAction": "none"}])
    );
    assert_authorizes_no_runner(held_grant(&other, "linear"));
}

// REQ-CREDREADY-24
#[test]
fn doctor_connect_refuses_options_it_does_not_take_before_anything_is_asked() {
    let tree = Tree::linear_and_claude_code(true);
    // Each case is otherwise a command the doctor accepts, so only the rule it names refuses
    // it: the harness adds --allow-insecure-http, which alone needs a mode that contacts Cloud,
    // and a service API key alone needs --readiness.
    let cases: [&[&str]; 3] = [
        &["--connect", "--json"],
        &["--workflow", "root.yaml", "--grants", "--replace", "--json"],
        &[
            "--workflow",
            "root.yaml",
            "--connect",
            "--readiness",
            "--organization-id",
            ORGANIZATION_ID,
            "--service-api-key-file",
            "/dev/null/key",
            "--json",
        ],
    ];
    for args in cases {
        let doctor = ran_in(
            &tree,
            FakeProgram::claude(SetupToken::Prints),
            Vec::new(),
            RUNNER_DOCTOR,
            args,
        );

        assert_eq!(
            doctor.output.status.code(),
            Some(2),
            "{args:?}: {}",
            stderr(&doctor.output)
        );
        assert!(doctor.output.stdout.is_empty(), "{args:?}");
        assert_eq!(doctor.claude.recorded("arguments"), None, "{args:?}");
        assert!(doctor.requests.is_empty(), "{args:?}");
    }
}

// SC-GRANT-4's workflow: GitHub through the `gh` it requires, Linear through a forwarded
// variable, Claude Code through `child.yaml`'s agent step, and a forwarded variable of no
// known kind.
const CONNECTING_WORKFLOW: &str = r#"schemaVersion: 1
requires:
  tools: [gh]
environmentPassthrough: [LINEAR_API_KEY, BUILDKITE_API_TOKEN]
steps:
  stage:
    kind: workflow
    workflow: child.yaml
    on:
      succeeded: exit.succeeded
      failed: exit.failed
"#;

fn authorize_line(grant_id: &str) -> String {
    format!("POST /api/v1/me/grants/{grant_id}/authorizations HTTP/1.1")
}

// SC-GRANT-4, REQ-GRANT-76, REQ-GRANT-69, REQ-GRANT-71, REQ-GRANT-72, REQ-GRANT-74,
// REQ-GRANT-64, REQ-GRANT-66
#[test]
fn connecting_what_a_workflow_needs_leaves_grants_one_command_authorizes_a_runner_for() {
    let tree = Tree::new(
        &[
            ("workflow.yaml", CONNECTING_WORKFLOW),
            ("child.yaml", CHILD_WORKFLOW),
            ("prompts/system.md", "System prompt."),
            ("prompts/message.md", "Message."),
        ],
        true,
    );
    let git = Git::new();
    // Ada, a member of Acme alone, holds a GitHub grant; the Linear consent and the Claude Code
    // sign-in make the other two.
    let github = oauth_grant_unauthorized(OAUTH_GRANT_ID);
    let mut linear = oauth_grant(LINEAR_GRANT_ID, "linear", "active");
    linear["authorizations"] = serde_json::json!([]);
    let claude_code = claude_grant_of(NATIVE_GRANT_ID);
    let mut consented = consent_of("linear", "completed");
    consented["resultGrant"]["id"] = LINEAR_GRANT_ID.into();
    let held = [github.clone(), linear.clone(), claude_code.clone()];
    // Each grant as it reads once the runner is authorized for it.
    let after: Vec<_> = held
        .iter()
        .map(|grant| {
            let mut grant = grant.clone();
            let prefix = if grant["kind"] == "oauth" {
                "pga"
            } else {
                "aga"
            };
            grant["authorizations"] = serde_json::json!([{
                "id": format!("{prefix}_01k0z6r1w8f4jy2m7q9v3x5abf"),
                "runnerId": RUNNER_ID,
                "organizationId": ORGANIZATION_ID,
                "createdAt": "2026-11-06T09:00:00Z"
            }]);
            grant
        })
        .collect();
    let (server, _directory, credential) = prepared(vec![
        // um grant create --workflow workflow.yaml
        page(&[github], None),
        serving_every(),
        only_acme(),
        json_http_response("201 Created", consent_of("linear", "pending")),
        polled(consented),
        stored(CLAUDE_CODE, "201 Created", 1),
        // um grant list
        page(&held, None),
        organization(),
        identities(ada()),
        // um grant authorize --all RUNNER --yes
        page(&held, None),
        authorized(OAUTH_GRANT_ID, RUNNER_ID),
        authorized(LINEAR_GRANT_ID, RUNNER_ID),
        authorized(NATIVE_GRANT_ID, RUNNER_ID),
        // um grant list
        page(&after, None),
        organization(),
        identities(ada()),
    ]);
    let opener = Opener::new(0);
    let claude = FakeProgram::claude(SetupToken::Prints);
    // run_in checks that no output carries the token or Ada's own Claude Code configuration
    // changed (REQ-GRANT-74).
    let created = run_in(
        &server,
        &credential,
        &opener,
        &[&claude],
        git.in_tree(&tree),
        &["--workflow", "workflow.yaml", "--json"],
    );
    let listed = run_grant(&server, &credential, &["list", "--json"]);
    let authorized_all = run_grant(
        &server,
        &credential,
        &["authorize", "--all", RUNNER_ID, "--yes", "--json"],
    );
    let listed_after = run_grant(&server, &credential, &["list", "--json"]);

    // GitHub is passed over as held, Linear and Claude Code are signed in to, and the variable
    // of no known kind is listed as unknown (REQ-GRANT-76).
    assert_eq!(created.status.code(), Some(0), "{}", stderr(&created));
    let events = events(&created);
    let plan = &events[0];
    assert_eq!(plan["event"], "workflow_requirements", "{events:?}");
    assert_eq!(providers_of(plan, "held"), ["github"]);
    assert_eq!(providers_of(plan, "selected"), ["linear", "claude_code"]);
    assert!(
        plan["unknown"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!({
                "declaration": "forwarded_variable",
                "workflow": "workflow.yaml",
                "name": "BUILDKITE_API_TOKEN"
            })),
        "{plan}"
    );
    assert_eq!(
        summary(&events),
        (
            "completed".to_owned(),
            outcomes(&[("linear", "created"), ("claude_code", "created")])
        )
    );
    // The Linear consent URL opens (REQ-GRANT-69), and `claude setup-token` runs on a terminal
    // and shows its prompts (REQ-GRANT-71).
    assert_eq!(opener.opened(1), [format!("1 {}", consent_url("linear"))]);
    assert_eq!(claude.recorded("arguments").unwrap(), "setup-token\n");
    assert_eq!(claude.recorded("terminal").unwrap(), "yes\n");
    assert!(
        stderr(&created).contains(CLAUDE_SIGN_IN_URL),
        "{}",
        stderr(&created)
    );

    // Three grants, none with a runner authorized, each naming the authorization as its next
    // action (REQ-GRANT-64).
    let grant_ids = [OAUTH_GRANT_ID, LINEAR_GRANT_ID, NATIVE_GRANT_ID];
    assert_eq!(listed.status.code(), Some(0), "{}", stderr(&listed));
    let document = json_output(&listed);
    let grants = document["grants"].as_array().unwrap();
    assert_eq!(
        grants.iter().map(|grant| &grant["id"]).collect::<Vec<_>>(),
        grant_ids
    );
    for grant in grants {
        assert_eq!(grant["authorizations"], serde_json::json!([]), "{grant}");
        assert_eq!(grant["availability"], "not_connected", "{grant}");
        assert_eq!(grant["nextAction"], "authorize_runner", "{grant}");
    }

    // One command authorizes the runner for each (REQ-GRANT-66), and the list shows it on each.
    assert_eq!(
        authorized_all.status.code(),
        Some(0),
        "{}",
        stderr(&authorized_all)
    );
    let document = json_output(&authorized_all);
    assert_eq!(document["outcome"], "authorized");
    assert_eq!(
        grant_outcomes(&document),
        grant_ids.map(|id| (id.to_owned(), "authorized".to_owned()))
    );
    assert_eq!(
        listed_after.status.code(),
        Some(0),
        "{}{}",
        stdout(&listed_after),
        stderr(&listed_after)
    );
    for grant in json_output(&listed_after)["grants"].as_array().unwrap() {
        let runners: Vec<_> = grant["authorizations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|authorization| &authorization["runnerId"])
            .collect();
        assert_eq!(runners, [RUNNER_ID], "{grant}");
    }

    let requests = server.finish();
    assert_member_requests(&requests);
    let poll = poll_line();
    let list = [LIST_FIRST_PAGE, READ_ORGANIZATION, READ_IDENTITIES];
    let authorizations = grant_ids.map(authorize_line);
    let expected: Vec<&str> = [
        LIST_EVERY_KIND,
        READ_SERVED,
        LIST_MEMBERSHIPS,
        START_CONSENT,
        poll.as_str(),
        IMPORT_GRANT,
    ]
    .into_iter()
    .chain(list)
    .chain([LIST_FIRST_PAGE])
    .chain(authorizations.iter().map(String::as_str))
    .chain(list)
    .collect();
    assert_eq!(request_lines(&requests), expected);
    assert_eq!(start_body(&requests[3]), "linear");
    // The Claude Code grant is made for Acme, which Ada did not name (REQ-GRANT-72).
    assert_imported(&requests[5], ORGANIZATION_ID, CLAUDE_CODE);
    for request in &requests[10..13] {
        assert_eq!(
            request_body(request),
            serde_json::json!({"runnerId": RUNNER_ID})
        );
    }
}

// The child of SC-CREDREADY-3's workflow: its one step is a Codex agent, which would start the
// fake `codex` if it ran.
const CODEX_CHILD_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  writing:
    harness:
      kind: codex
      config:
        model: gpt-5.4
        effort: xhigh
steps:
  draft:
    kind: agent
    agent:
      profile: writing
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
"#;

/// The Codex grant authorized for `RUNNER_ID`, which reported it ready at generation 2 and
/// refused at its last use, as it reads at `generation`.
fn codex_refused_at(generation: i64) -> serde_json::Value {
    let mut grant = native_grant_in_use();
    grant["generation"] = generation.into();
    grant["authorizations"][0]["report"]["lastUse"] = "refused".into();
    grant
}

/// The fields of each requirement of `report` that a grant never changes, and its summary.
fn requirement_states(report: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut states: Vec<_> = report["requirements"]
        .as_array()
        .unwrap()
        .iter()
        .map(|requirement| {
            serde_json::json!([
                requirement["kind"],
                requirement["state"],
                requirement["code"],
                requirement["cloud"],
                requirement["nextAction"]
            ])
        })
        .collect();
    states.push(report["summary"].clone());
    states
}

// SC-CREDREADY-3, REQ-CREDREADY-22, REQ-CREDREADY-23, REQ-CREDREADY-18, REQ-CREDREADY-24,
// REQ-GRANT-79, REQ-CREDREADY-25, REQ-CREDREADY-9
#[test]
fn connecting_what_a_tree_lacks_from_the_doctor_reports_the_grants_it_made_on_the_runner() {
    let elsewhere = tempfile::tempdir().unwrap();
    let tree = Tree::new(
        &[
            ("workflow.yaml", CONNECTING_WORKFLOW),
            ("child.yaml", CODEX_CHILD_WORKFLOW),
            ("prompts/system.md", "System prompt."),
            ("prompts/message.md", "Message."),
        ],
        true,
    );
    let git = Git::new();
    // Ada, a member of Acme alone, holds a GitHub grant and a Codex grant authorized for the
    // runner, where Codex was refused at its last use.
    let github = oauth_grant(OAUTH_GRANT_ID, "github", "active");
    let before = page(&[github.clone(), codex_refused_at(2)], None);
    // The Linear consent makes a grant with no runner authorized, and the Codex sign-in is
    // stored as the grant's generation 3, which the runner has not yet reported installing.
    let mut linear = oauth_grant(LINEAR_GRANT_ID, "linear", "active");
    linear["authorizations"] = serde_json::json!([]);
    let mut consented = consent_of("linear", "completed");
    consented["resultGrant"]["id"] = LINEAR_GRANT_ID.into();
    let after = page(&[github, linear, codex_refused_at(3)], None);
    let (server, _directory, credential) = prepared(vec![
        // um runner doctor --workflow workflow.yaml --grants --runner-id RUNNER
        before.clone(),
        // um runner doctor --workflow workflow.yaml --connect --replace --runner-id RUNNER
        before,
        serving_every(),
        json_http_response("201 Created", consent_of("linear", "pending")),
        polled(consented),
        stored(CODEX, "200 OK", 3),
        after,
    ]);
    let opener = Opener::new(0);
    let codex = FakeProgram::codex(SignIn::Writes(CODEX_MATERIAL));
    // run_command_in checks that no output carries the login and that Ada's own Codex home is
    // untouched (REQ-GRANT-74).
    let doctor = |args: &[&str]| {
        let output = run_command_in(
            &server,
            &credential,
            &opener,
            &[&codex],
            git.in_tree(&tree),
            RUNNER_DOCTOR,
            &[&["--workflow", "workflow.yaml"], args, &["--json"]].concat(),
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "{args:?}: {}",
            stderr(&output)
        );
        output
    };

    // The report without a grant read, which contacts nothing.
    let offline = Command::new(env!("CARGO_BIN_EXE_um"))
        .args(["runner", "doctor", "--workflow", "workflow.yaml", "--json"])
        .current_dir(&tree.root)
        .env_clear()
        .env("PATH", git.0.path())
        .env("HOME", elsewhere.path())
        .output()
        .unwrap();
    assert_eq!(offline.status.code(), Some(0), "{}", stderr(&offline));
    let offline = json_output(&offline);
    let grants = json_output(&doctor(&["--grants", "--runner-id", RUNNER_ID]));

    // GitHub is available on the runner, Linear lacks a grant and Codex needs reconnecting
    // there, and no requirement's state changes (REQ-CREDREADY-22, REQ-CREDREADY-23,
    // REQ-CREDREADY-18).
    assert_eq!(requirement_states(&grants), requirement_states(&offline));
    assert_eq!(
        held_grant(&grants, "github")["authorizations"],
        serde_json::json!([{"runnerId": RUNNER_ID, "availability": "available", "nextAction": "none"}])
    );
    assert_eq!(
        requirement(&grants, "linear")["grant"],
        serde_json::json!({"provider": "linear", "grants": [], "availability": "not_connected", "nextAction": "connect"})
    );
    let codex_grant = held_grant(&grants, "codex");
    assert_eq!(codex_grant["generation"], 2);
    assert_eq!(
        codex_grant["authorizations"],
        serde_json::json!([{"runnerId": RUNNER_ID, "availability": "reconnect_required", "nextAction": "reconnect"}])
    );

    let connected = doctor(&["--connect", "--replace", "--runner-id", RUNNER_ID]);

    // The plan names Linear to sign in, Codex to reconnect and GitHub as held; the Linear
    // consent is approved and the Codex sign-in replaces the held grant (REQ-CREDREADY-24,
    // REQ-GRANT-79).
    let flow = flow_events(&connected);
    let plan = &flow[0];
    assert_eq!(plan["event"], "workflow_requirements", "{flow:?}");
    assert_eq!(providers_of(plan, "selected"), ["linear"]);
    assert_eq!(providers_of(plan, "reconnect"), ["codex"]);
    assert_eq!(providers_of(plan, "held"), ["github"]);
    assert_eq!(opener.opened(1), [format!("1 {}", consent_url("linear"))]);
    // `codex` was last started for the sign-in, so the tree's agent step did not run once the
    // sign-ins ended (REQ-CREDREADY-9).
    assert_eq!(codex.recorded("arguments").unwrap(), "login\n");
    let import = flow
        .iter()
        .find(|event| event["event"] == "import_result")
        .unwrap();
    assert_eq!(import["outcome"], "replaced");
    assert_eq!(import["grant"]["id"], NATIVE_GRANT_ID);
    assert_eq!(import["grant"]["generation"], 3);
    assert_eq!(
        summary(&flow),
        (
            "completed".to_owned(),
            outcomes(&[("linear", "created"), ("codex", "replaced")])
        )
    );

    // The report that follows reads the grants again: Linear's has no runner authorized, and
    // Codex is pending on the runner at its next generation (REQ-CREDREADY-25).
    let report = events(&connected).pop().unwrap();
    assert_eq!(requirement_states(&report), requirement_states(&offline));
    let linear_grant = held_grant(&report, "linear");
    assert_eq!(linear_grant["id"], LINEAR_GRANT_ID);
    assert_authorizes_no_runner(linear_grant);
    let codex_grant = held_grant(&report, "codex");
    assert_eq!(codex_grant["id"], NATIVE_GRANT_ID);
    assert_eq!(codex_grant["generation"], 3);
    assert_eq!(
        codex_grant["authorizations"],
        serde_json::json!([{"runnerId": RUNNER_ID, "availability": "pending", "nextAction": "wait_for_runner"}])
    );

    let requests = server.finish();
    assert_member_requests(&requests);
    let poll = poll_line();
    let replace = replace_line(NATIVE_GRANT_ID);
    assert_eq!(
        request_lines(&requests),
        [
            LIST_EVERY_KIND,
            LIST_EVERY_KIND,
            READ_SERVED,
            START_CONSENT,
            poll.as_str(),
            replace.as_str(),
            LIST_EVERY_KIND
        ]
    );
    assert_eq!(start_body(&requests[3]), "linear");
    assert_eq!(
        request_body(&requests[5]),
        serde_json::json!({
            "recordSchemaVersion": 1,
            "material": encoded(CODEX_MATERIAL.as_bytes())
        })
    );
}

// REQ-GRANT-76
#[test]
fn a_tree_that_calls_for_gitlab_selects_gitlab_com() {
    let tree = Tree::gitlab();
    let git = Git::new();
    let (server, _directory, credential) =
        prepared([vec![nothing_held()], approved("gitlab.com").to_vec()].concat());
    let opener = Opener::new(0);
    let output = run_in(
        &server,
        &credential,
        &opener,
        &[],
        git.in_tree(&tree),
        &["--workflow", "gitlab.yaml", "--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let events = events(&output);
    assert_eq!(
        planned(&events[0], "selected"),
        [(
            "gitlab.com".to_owned(),
            serde_json::json!([{"declaration": "required_tool", "workflow": "gitlab.yaml", "name": "glab"}])
        )]
    );
    assert_eq!(
        summary(&events),
        (
            "completed".to_owned(),
            outcomes(&[("gitlab.com", "created")])
        )
    );
    let requests = server.finish();
    let poll = poll_line();
    assert_eq!(
        request_lines(&requests),
        [LIST_EVERY_KIND, START_CONSENT, poll.as_str()]
    );
    assert_eq!(start_body(&requests[1]), "gitlab.com");
}

// REQ-GRANT-76
#[test]
fn files_in_two_repositories_are_each_derived_under_their_own_root_and_merged() {
    let first = Tree::linear_and_claude_code(true);
    let second = Tree::gitlab();
    let git = Git::new();
    let (server, _directory, credential) = prepared(
        [
            vec![holding(&["linear"], true)],
            approved("gitlab.com").to_vec(),
        ]
        .concat(),
    );
    let opener = Opener::new(0);
    let other = second.path("gitlab.yaml");
    let output = run_in(
        &server,
        &credential,
        &opener,
        &[],
        git.in_tree(&first),
        &["--workflow", "root.yaml", "--workflow", &other, "--json"],
    );

    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
    let events = events(&output);
    let plan = &events[0];
    assert_eq!(
        plan["workflows"],
        serde_json::json!(["child.yaml", "root.yaml", "gitlab.yaml"])
    );
    assert_eq!(providers_of(plan, "selected"), ["gitlab.com"]);
    assert_eq!(providers_of(plan, "held"), ["linear", "claude_code"]);
    // The two repositories' unknown `origin` remotes are one entry.
    let repositories = plan["unknown"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["declaration"] == "repository")
        .count();
    assert_eq!(repositories, 1, "{plan}");
    assert_eq!(
        git.invocations(),
        [
            "rev-parse --show-toplevel",
            "rev-parse --show-toplevel",
            "remote get-url origin",
            "remote get-url origin"
        ]
    );
    let poll = poll_line();
    assert_eq!(
        request_lines(&server.finish()),
        [LIST_EVERY_KIND, START_CONSENT, poll.as_str()]
    );
}

// REQ-GRANT-76
#[test]
fn a_named_source_root_is_used_as_given_and_a_tree_calling_for_nothing_lacked_exits_0() {
    for json in [false, true] {
        // No `.git` marker: only `--source-root` bounds the files.
        let tree = Tree::linear_and_claude_code(false);
        let git = Git::new();
        let (server, _directory, credential) = prepared(vec![holding(&["linear"], true)]);
        let opener = Opener::new(0);
        let root = tree.root.to_str().unwrap();
        let mut args = vec!["--workflow", "root.yaml", "--source-root", root];
        if json {
            args.push("--json");
        }
        let output = run_in(
            &server,
            &credential,
            &opener,
            &[],
            git.in_tree(&tree),
            &args,
        );

        assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
        if json {
            let events = events(&output);
            assert_eq!(events.len(), 2, "{events:?}");
            assert_eq!(providers_of(&events[0], "selected"), Vec::<String>::new());
            assert_eq!(providers_of(&events[0], "held"), ["linear", "claude_code"]);
            assert_eq!(summary(&events), ("nothing_to_create".to_owned(), vec![]));
        } else {
            assert!(stdout(&output).contains("root.yaml"));
        }
        assert_eq!(git.invocations(), ["remote get-url origin"]);
        assert!(opener.opened(0).is_empty());
        assert_eq!(request_lines(&server.finish()), [LIST_EVERY_KIND]);
    }
}

// REQ-GRANT-76
#[test]
fn a_file_in_no_repository_or_that_does_not_resolve_or_with_a_provider_starts_nothing() {
    let outside = Tree::linear_and_claude_code(false);
    let inside = Tree::linear_and_claude_code(true);
    let cases: [(&Tree, &[&str], i32); 6] = [
        (&outside, &["--workflow", "root.yaml"], 2),
        (&inside, &["--workflow", "absent.yaml"], 1),
        (&inside, &["--workflow", "absent.yaml", "--json"], 1),
        (&inside, &["--workflow", "root.yaml", "linear"], 2),
        (&inside, &["--workflow", "root.yaml", "--all"], 2),
        (&inside, &["--source-root", "."], 2),
    ];
    for (tree, args, code) in cases {
        let git = Git::new();
        let (server, _directory, credential) = prepared(vec![]);
        let opener = Opener::new(0);
        let claude = FakeProgram::claude(SetupToken::Prints);
        let output = run_in(
            &server,
            &credential,
            &opener,
            &[&claude],
            git.in_tree(tree),
            args,
        );

        assert_eq!(
            output.status.code(),
            Some(code),
            "{args:?}: {}",
            stderr(&output)
        );
        if args[1] == "absent.yaml" {
            // The rejection is the one `um workflow validate` prints.
            let mut validate = vec!["workflow", "validate", "--source-root", "."];
            validate.extend(&args[2..]);
            validate.push("absent.yaml");
            let validated = run_with_env_from(&validate, &[], Some(&tree.root));
            assert_eq!(validated.status.code(), Some(1), "{validated:?}");
            assert_eq!(stdout(&output), stdout(&validated), "{args:?}");
        } else {
            assert!(output.stdout.is_empty(), "{args:?}");
        }
        if std::ptr::eq(tree, &outside) {
            assert!(stderr(&output).contains("--source-root"), "{args:?}");
        }
        assert!(opener.opened(0).is_empty(), "{args:?}");
        assert_eq!(claude.recorded("arguments"), None, "{args:?}");
        assert!(server.finish().is_empty(), "{args:?}");
    }
}

// REQ-GRANT-76
#[test]
fn a_file_whose_directory_is_missing_is_rejected_as_workflow_validate_rejects_it() {
    for json in [false, true] {
        let tree = Tree::linear_and_claude_code(true);
        let git = Git::new();
        let (server, _directory, credential) = prepared(vec![]);
        let opener = Opener::new(0);
        let mut args = vec!["--workflow", "missing-dir/root.yaml"];
        if json {
            args.push("--json");
        }
        let output = run_in(
            &server,
            &credential,
            &opener,
            &[],
            git.in_tree(&tree),
            &args,
        );

        let mut validate = vec!["workflow", "validate", "--source-root", "."];
        validate.extend(&args[2..]);
        validate.push("missing-dir/root.yaml");
        let validated = run_with_env_from(&validate, &[], Some(&tree.root));
        assert_eq!(
            output.status.code(),
            validated.status.code(),
            "{json}: {}",
            stderr(&output)
        );
        assert_eq!(stdout(&output), stdout(&validated), "{json}");
        assert!(opener.opened(0).is_empty(), "{json}");
        assert!(server.finish().is_empty(), "{json}");
    }
}

// REQ-GRANT-76
#[test]
fn git_missing_from_path_or_failing_is_reported_as_that_and_starts_nothing() {
    let tree = Tree::linear_and_claude_code(true);
    let file = tree.path("root.yaml");
    let directory = tree.root.to_str().unwrap();
    // No `git` at all on `PATH`, and a `git` that fails every command in a repository.
    let missing = tempfile::tempdir().unwrap();
    let failing = fake_git("exit 128");
    for (git, code) in [(missing.path(), 1), (failing.path(), 2)] {
        let (server, _directory, credential) = prepared(vec![]);
        let opener = Opener::new(0);
        let checkout = Checkout {
            directory: &tree.root,
            git,
        };
        let output = run_in(
            &server,
            &credential,
            &opener,
            &[],
            Some(checkout),
            &["--workflow", &file],
        );

        let text = stderr(&output);
        assert_eq!(output.status.code(), Some(code), "{text}");
        assert!(output.stdout.is_empty(), "{text}");
        if code == 1 {
            assert!(text.contains("git") && text.contains("PATH"), "{text}");
        } else {
            assert!(text.contains("git rev-parse --show-toplevel"), "{text}");
            assert!(text.contains(directory), "{text}");
        }
        assert!(text.contains("--source-root"), "{text}");
        assert!(opener.opened(0).is_empty());
        assert!(server.finish().is_empty());
    }
}
