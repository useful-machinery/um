use super::*;

const TOKEN: &str = "unique-credential-status-session-token";
const GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abc";
const REVOKED_GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abd";
const IDENTITY_ID: &str = "idn_01k0z6r1w8f4jy2m7q9v3x5abc";
const AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
const REVOKED_AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abd";
const OTHER_AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abe";
const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
const OTHER_ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abd";
const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
const SECOND_RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abd";
const ENDED_RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abe";
const RUN_ID: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const SENTINEL: &str = "private-credential-status-material-sentinel";

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

fn run_auth(server: &ScriptedServer, credential: &Path, args: &[&str]) -> Output {
    let mut arguments = vec!["auth"];
    arguments.extend_from_slice(args);
    arguments.push("--allow-insecure-http");
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential.to_str().unwrap(),
    );
    run_with_env(&arguments, &environment)
}

// A page of a list, each item carrying a material member that stands for any field a response
// might wrongly carry.
fn page(items: serde_json::Value, next_cursor: Option<&str>) -> Vec<u8> {
    let mut items = items;
    for item in items.as_array_mut().unwrap() {
        item["material"] = SENTINEL.into();
    }
    let mut body = serde_json::json!({"items": items});
    if let Some(cursor) = next_cursor {
        body["nextCursor"] = cursor.into();
    }
    json_http_response("200 OK", body)
}

fn grant(id: &str, provider: &str, state: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "identityId": IDENTITY_ID,
        "provider": provider,
        "scopes": ["repo"],
        "state": state,
        "createdAt": "2026-11-01T08:00:00Z"
    })
}

fn agent_login(
    id: &str,
    organization_id: &str,
    provider: &str,
    state: &str,
    authorizations: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "organizationId": organization_id,
        "provider": provider,
        "label": "build laptop",
        "state": state,
        "generation": 2,
        "recordSchemaVersion": 1,
        "keyVersion": 1,
        "createdAt": "2026-11-01T09:00:00Z",
        "importedAt": "2026-11-02T10:00:00Z",
        "authorizations": authorizations,
        "endedAuthorizations": []
    })
}

// A Codex login authorized for two runners: the first reported it `ready` at the current
// generation and accepted at its last use, the second has not yet asked.
fn codex_login() -> serde_json::Value {
    agent_login(
        AGENT_LOGIN_ID,
        ORGANIZATION_ID,
        "codex",
        "active",
        serde_json::json!([
            {
                "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
                "runnerId": RUNNER_ID,
                "createdAt": "2026-11-01T09:05:00Z",
                "delivery": {"generation": 2, "deliveredAt": "2026-11-02T10:01:00Z"},
                "report": {
                    "state": "ready",
                    "generation": 2,
                    "lastUse": "accepted",
                    "reportedAt": "2026-11-02T10:02:00Z"
                }
            },
            {
                "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abd",
                "runnerId": SECOND_RUNNER_ID,
                "createdAt": "2026-11-03T09:05:00Z"
            }
        ]),
    )
}

// A revoked Claude Code login whose runner was serving a run when it was revoked and has not
// confirmed removing its copy (REQ-AGENTLOGIN-37, REQ-AGENTLOGIN-38).
fn revoked_claude_code_login() -> serde_json::Value {
    let mut login = agent_login(
        REVOKED_AGENT_LOGIN_ID,
        ORGANIZATION_ID,
        "claude_code",
        "revoked",
        serde_json::json!([]),
    );
    login["revokedAt"] = "2026-11-05T09:00:00Z".into();
    login["endedAuthorizations"] = serde_json::json!([{
        "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abe",
        "runnerId": ENDED_RUNNER_ID,
        "createdAt": "2026-11-01T09:10:00Z",
        "delivery": {"generation": 2, "deliveredAt": "2026-11-01T09:11:00Z"},
        "endedAt": "2026-11-05T09:00:00Z",
        "removal": {"state": "pending"},
        "stop": {
            "runId": RUN_ID,
            "cancellationRequestId": "cmd_01k0z6r1w8f4jy2m7q9v3x5abc",
            "requestedAt": "2026-11-05T09:00:01Z"
        }
    }]);
    login
}

fn json_output(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

fn not_connected(provider: &str) -> serde_json::Value {
    serde_json::json!({
        "provider": provider,
        "kind": "none",
        "availability": "not_connected",
        "nextAction": "connect"
    })
}

fn codex_row(runner_id: &str, availability: &str, next_action: &str) -> serde_json::Value {
    serde_json::json!({
        "provider": "codex",
        "kind": "agent_login",
        "id": AGENT_LOGIN_ID,
        "label": "build laptop",
        "organizationId": ORGANIZATION_ID,
        "generation": 2,
        "runnerId": runner_id,
        "availability": availability,
        "nextAction": next_action
    })
}

// Both lists, each over two pages: the agent logins, then a `reconnect_required` GitHub grant
// and a revoked Linear one.
fn two_page_lists() -> Vec<Vec<u8>> {
    vec![
        page(serde_json::json!([codex_login()]), Some("agent-logins-2")),
        page(serde_json::json!([revoked_claude_code_login()]), None),
        page(
            serde_json::json!([grant(GRANT_ID, "github", "reconnect_required")]),
            Some("grants-2"),
        ),
        page(
            serde_json::json!([grant(REVOKED_GRANT_ID, "linear", "revoked")]),
            None,
        ),
    ]
}

fn assert_reads_both_lists_to_their_last_page(requests: &[String]) {
    let lines: Vec<_> = requests
        .iter()
        .map(|request| request.lines().next().unwrap())
        .collect();
    assert_eq!(
        lines,
        [
            "GET /api/v1/me/agent-logins?limit=200 HTTP/1.1",
            "GET /api/v1/me/agent-logins?limit=200&cursor=agent-logins-2 HTTP/1.1",
            "GET /api/v1/me/grants?limit=200 HTTP/1.1",
            "GET /api/v1/me/grants?limit=200&cursor=grants-2 HTTP/1.1",
        ]
    );
    for request in requests {
        assert_eq!(
            header_value(request, "authorization"),
            format!("Bearer {TOKEN}")
        );
    }
}

// REQ-AGENTLOGIN-45
#[test]
fn status_reports_every_grant_and_authorization_and_each_provider_not_connected() {
    let mut responses = two_page_lists();
    responses.extend(two_page_lists());
    let (server, _directory, credential) = prepared(responses);
    let output = run_auth(&server, &credential, &["credential", "status", "--json"]);
    let human = run_auth(&server, &credential, &["credential", "status"]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut ended = revoked_claude_code_login()["endedAuthorizations"][0].clone();
    ended["agentLoginId"] = REVOKED_AGENT_LOGIN_ID.into();
    ended["provider"] = "claude_code".into();
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "reported",
            "credentials": [
                {
                    "provider": "github",
                    "kind": "grant",
                    "id": GRANT_ID,
                    "identityId": IDENTITY_ID,
                    "availability": "reconnect_required",
                    "nextAction": "reconnect"
                },
                not_connected("gitlab.com"),
                not_connected("linear"),
                not_connected("claude_code"),
                codex_row(RUNNER_ID, "available", "none"),
                codex_row(SECOND_RUNNER_ID, "pending", "wait_for_runner"),
            ],
            "endedAuthorizations": [ended]
        })
    );
    assert!(output.stderr.is_empty());
    assert_no_secret_output(&output, &[SENTINEL, TOKEN]);

    assert_eq!(human.status.code(), Some(0));
    assert_no_secret_output(&human, &[SENTINEL, TOKEN]);
    let text = String::from_utf8(human.stdout).unwrap();
    let (rows, ended) = text.split_once("ended authorizations: 1").unwrap();
    for name in [
        GRANT_ID,
        AGENT_LOGIN_ID,
        RUNNER_ID,
        SECOND_RUNNER_ID,
        "reconnect_required",
        "wait_for_runner",
        "not_connected",
        "um auth grant create github",
    ] {
        assert!(rows.contains(name), "missing {name:?} in\n{text}");
    }
    assert!(!rows.contains(REVOKED_GRANT_ID), "{text}");
    assert!(!rows.contains(REVOKED_AGENT_LOGIN_ID), "{text}");
    for name in [REVOKED_AGENT_LOGIN_ID, ENDED_RUNNER_ID, RUN_ID, "pending"] {
        assert!(ended.contains(name), "missing {name:?} in\n{text}");
    }

    let requests = server.finish();
    let (json_requests, human_requests) = requests.split_at(4);
    assert_reads_both_lists_to_their_last_page(json_requests);
    assert_reads_both_lists_to_their_last_page(human_requests);
}

// REQ-AGENTLOGIN-45
#[test]
fn organization_keeps_only_that_organizations_agent_logins() {
    let other = agent_login(
        OTHER_AGENT_LOGIN_ID,
        OTHER_ORGANIZATION_ID,
        "codex",
        "active",
        serde_json::json!([]),
    );
    let logins = || page(serde_json::json!([codex_login(), other.clone()]), None);
    let no_grants = || page(serde_json::json!([]), None);
    let (server, _directory, credential) = prepared(vec![
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
        ),
        logins(),
        no_grants(),
        logins(),
        no_grants(),
    ]);
    let by_slug = run_auth(
        &server,
        &credential,
        &["credential", "status", "--organization", "acme", "--json"],
    );
    let by_id = run_auth(
        &server,
        &credential,
        &[
            "credential",
            "status",
            "--organization",
            OTHER_ORGANIZATION_ID,
            "--json",
        ],
    );

    assert_eq!(
        by_slug.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&by_slug.stderr)
    );
    let codex: Vec<_> = json_output(&by_slug)["credentials"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["provider"] == "codex")
        .cloned()
        .collect();
    assert_eq!(
        codex,
        [
            codex_row(RUNNER_ID, "available", "none"),
            codex_row(SECOND_RUNNER_ID, "pending", "wait_for_runner"),
        ]
    );
    assert!(!String::from_utf8_lossy(&by_slug.stdout).contains(OTHER_AGENT_LOGIN_ID));

    // The other organization's agent login has no authorization in force: it is one row whose
    // next action is to authorize a runner (REQ-AGENTLOGIN-43).
    assert_eq!(by_id.status.code(), Some(0));
    let document = json_output(&by_id);
    let codex: Vec<_> = document["credentials"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["provider"] == "codex")
        .collect();
    assert_eq!(codex.len(), 1, "{document}");
    assert_eq!(codex[0]["id"], OTHER_AGENT_LOGIN_ID);
    assert_eq!(codex[0]["availability"], "not_connected");
    assert_eq!(codex[0]["nextAction"], "authorize_runner");
    assert!(codex[0].get("runnerId").is_none(), "{document}");
    assert!(!document.to_string().contains(AGENT_LOGIN_ID));

    let requests = server.finish();
    assert!(requests[0].starts_with("GET /api/v1/organizations/acme HTTP/1.1\r\n"));
    // An organization ID is used as it is, with no lookup.
    assert!(requests[3].starts_with("GET /api/v1/me/agent-logins?limit=200 HTTP/1.1\r\n"));
    assert_eq!(requests.len(), 5);
}

fn problem(status: &str, code: u16, slug: &str) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": format!("https://api.usefulmachinery.com/problems/{slug}"),
            "title": "Credential result",
            "status": code
        }),
    )
}

// REQ-AGENTLOGIN-45
#[test]
fn a_failed_read_is_reported_as_its_own_family_reports_it() {
    let unavailable = || problem("503 Service Unavailable", 503, "provider-unavailable");
    // The agent-login list fails: nothing else is read.
    let (server, _directory, credential) = prepared(vec![unavailable(), unavailable()]);
    let status = run_auth(&server, &credential, &["credential", "status", "--json"]);
    let family = run_auth(&server, &credential, &["agent-login", "list", "--json"]);
    assert_eq!(server.finish().len(), 2);
    assert_eq!(status.status.code(), family.status.code());
    assert_eq!(json_output(&status), json_output(&family));

    // The grant list fails after the agent logins were read.
    let (server, _directory, credential) = prepared(vec![
        page(serde_json::json!([]), None),
        unavailable(),
        unavailable(),
    ]);
    let status = run_auth(&server, &credential, &["credential", "status", "--json"]);
    let family = run_auth(&server, &credential, &["grant", "list", "--json"]);
    assert_eq!(server.finish().len(), 3);
    assert_ne!(status.status.code(), Some(0));
    assert_eq!(status.status.code(), family.status.code());
    assert_eq!(json_output(&status), json_output(&family));
}

// REQ-AGENTLOGIN-45
#[test]
fn a_service_api_key_is_refused_as_the_agent_login_leaves_refuse_it() {
    let directory = tempfile::tempdir().unwrap();
    let key = directory.path().join("service.key");
    fs::write(&key, "umsk_unused\n").unwrap();
    fs::set_permissions(&key, Permissions::from_mode(0o600)).unwrap();
    let key = key.to_str().unwrap();
    let (server, _credentials, credential) = prepared(vec![]);
    let refused = |args: &[&str]| {
        let mut args = args.to_vec();
        args.extend(["--service-api-key-file", key, "--json"]);
        let output = run_auth(&server, &credential, &args);
        assert!(output.stdout.is_empty(), "{args:?}");
        output.status.code()
    };
    let status = refused(&["credential", "status"]);
    let family = refused(&["agent-login", "list"]);
    assert_eq!(status, Some(2));
    assert_eq!(status, family);
    assert!(server.finish().is_empty());
}
