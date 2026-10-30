use super::*;

const TOKEN: &str = "unique-grant-session-token";
const GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abc";
const SECOND_GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abd";
const SENTINEL: &str = "private-grant-material-sentinel";

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

fn run_grant(server: &ScriptedServer, credential: &std::path::Path, args: &[&str]) -> Output {
    let mut arguments = vec!["auth", "grant"];
    arguments.extend_from_slice(args);
    arguments.push("--allow-insecure-http");
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential.to_str().unwrap(),
    );
    run_with_env(&arguments, &environment)
}

// The grants as the API presents them; the material member stands for any field a
// response might wrongly carry, which the CLI must never print.
fn grants() -> serde_json::Value {
    serde_json::json!([
        {
            "id": GRANT_ID,
            "identityId": "idn_01k0z6r1w8f4jy2m7q9v3x5abc",
            "provider": "github",
            "scopes": ["repo", "read:user"],
            "state": "active",
            "lastUsedAt": "2026-09-06T08:00:00Z",
            "accessExpiresAt": "2026-09-06T16:00:00Z",
            "refreshExpiresAt": "2027-03-06T08:00:00Z",
            "createdAt": "2026-09-05T12:00:00Z"
        },
        {
            "id": SECOND_GRANT_ID,
            "identityId": "idn_01k0z6r1w8f4jy2m7q9v3x5abd",
            "provider": "gitlab.com",
            "scopes": ["api"],
            "state": "reconnect_required",
            "createdAt": "2026-09-05T13:00:00Z"
        }
    ])
}

fn with_material(items: &serde_json::Value) -> serde_json::Value {
    let mut items = items.clone();
    for item in items.as_array_mut().unwrap() {
        item["accessToken"] = SENTINEL.into();
    }
    items
}

fn page(items: serde_json::Value, next_cursor: Option<&str>) -> Vec<u8> {
    let mut body = serde_json::json!({"items": items});
    if let Some(cursor) = next_cursor {
        body["nextCursor"] = cursor.into();
    }
    json_http_response("200 OK", body)
}

fn grant_problem(status: &str, code: u16, slug: &str) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": format!("https://api.usefulmachinery.com/problems/{slug}"),
            "title": "Provider grant result",
            "status": code
        }),
    )
}

fn json_output(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap()
}

// REQ-GRANT-3, REQ-GRANT-4
#[test]
fn list_returns_one_page_of_grants_without_material() {
    let (server, _directory, credential) = prepared(vec![page(
        with_material(&grants()),
        Some("opaque-next-page"),
    )]);
    let output = run_grant(
        &server,
        &credential,
        &["list", "--limit", "2", "--cursor", "page-before", "--json"],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "listed",
            "items": grants(),
            "nextCursor": "opaque-next-page"
        })
    );
    assert!(output.stderr.is_empty());
    assert_no_secret_output(&output, &[SENTINEL]);
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with("GET /api/v1/me/grants?limit=2&cursor=page-before HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
}

// REQ-GRANT-3, REQ-GRANT-4
#[test]
fn list_renders_each_grant_state_and_provider_for_a_person() {
    let (server, _directory, credential) = prepared(vec![page(with_material(&grants()), None)]);
    let output = run_grant(&server, &credential, &["list"]);

    assert!(output.status.success());
    assert_no_secret_output(&output, &[SENTINEL]);
    let text = String::from_utf8(output.stdout).unwrap();
    let fields = |key: &str| {
        text.lines()
            .filter_map(|line| line.strip_prefix(key))
            .collect::<Vec<_>>()
    };
    assert_eq!(fields("state: "), ["active", "reconnect_required"]);
    assert_eq!(fields("provider: "), ["github", "gitlab.com"]);
    assert_eq!(fields("grant: "), [GRANT_ID, SECOND_GRANT_ID]);
    server.finish();
}

// REQ-GRANT-3
#[test]
fn list_reports_an_unserved_provider_and_a_rejected_session() {
    let cases = [
        (
            vec![grant_problem(
                "503 Service Unavailable",
                503,
                "provider-unavailable",
            )],
            "provider_unavailable",
            4,
        ),
        (
            vec![
                grant_problem("401 Unauthorized", 401, "unauthorized"),
                json_http_response(
                    "400 Bad Request",
                    serde_json::json!({"error": "invalid_grant"}),
                ),
            ],
            "unauthenticated",
            3,
        ),
    ];
    for (responses, outcome, code) in cases {
        let (server, _directory, credential) = prepared(responses);
        let output = run_grant(&server, &credential, &["list", "--json"]);
        assert_eq!(output.status.code(), Some(code), "{outcome}");
        assert_eq!(json_output(&output)["outcome"], outcome);
        server.finish();
    }
}

// REQ-GRANT-8
#[test]
fn uses_lists_the_tokens_issued_from_one_grant() {
    let item = serde_json::json!({
        "id": "pgu_01k0z6r1w8f4jy2m7q9v3x5abc",
        "grantId": GRANT_ID,
        "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abc",
        "assignmentId": "asg_01k0z6r1w8f4jy2m7q9v3x5abc",
        "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
        "provider": "github",
        "issuedAt": "2026-09-06T08:00:00Z"
    });
    let (server, _directory, credential) = prepared(vec![page(
        with_material(&serde_json::json!([item.clone()])),
        Some("opaque-older-uses"),
    )]);
    let output = run_grant(
        &server,
        &credential,
        &["uses", GRANT_ID, "--limit", "1", "--json"],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "listed",
            "items": [item],
            "nextCursor": "opaque-older-uses"
        })
    );
    assert_no_secret_output(&output, &[SENTINEL]);
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "GET /api/v1/me/grants/{GRANT_ID}/uses?limit=1 HTTP/1.1\r\n"
    )));
}

// REQ-GRANT-8
#[test]
fn uses_of_an_unknown_grant_is_not_found() {
    let (server, _directory, credential) =
        prepared(vec![grant_problem("404 Not Found", 404, "grant-not-found")]);
    let output = run_grant(&server, &credential, &["uses", GRANT_ID, "--json"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(json_output(&output)["outcome"], "not_found");
    server.finish();
}

fn revoked() -> Vec<u8> {
    http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        b"",
    )
}

// REQ-GRANT-6
#[test]
fn revoke_deletes_the_grant_with_one_key_and_no_body() {
    let (server, _directory, credential) = prepared(vec![revoked()]);
    let output = run_grant(
        &server,
        &credential,
        &["revoke", GRANT_ID, "--yes", "--json"],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        json_output(&output),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "revoked",
            "grantId": GRANT_ID
        })
    );
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!("DELETE /api/v1/me/grants/{GRANT_ID} HTTP/1.1\r\n")));
    assert!(!header_value(&request, "idempotency-key").is_empty());
    assert_eq!(request.split_once("\r\n\r\n").unwrap().1, "");
}

// REQ-GRANT-6
#[test]
fn revoke_replays_its_key_once_after_a_server_failure() {
    let (server, _directory, credential) = prepared(vec![
        grant_problem("502 Bad Gateway", 502, "internal-server-error"),
        revoked(),
    ]);
    let output = run_grant(
        &server,
        &credential,
        &["revoke", GRANT_ID, "--yes", "--json"],
    );

    assert!(output.status.success());
    assert_eq!(json_output(&output)["outcome"], "revoked");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

// REQ-GRANT-6
#[test]
fn revoke_reports_an_unknown_grant_and_a_conflicting_key() {
    for (response, outcome) in [
        (
            grant_problem("404 Not Found", 404, "grant-not-found"),
            "not_found",
        ),
        (
            grant_problem("409 Conflict", 409, "idempotency-conflict"),
            "idempotency_conflict",
        ),
    ] {
        let (server, _directory, credential) = prepared(vec![response]);
        let output = run_grant(
            &server,
            &credential,
            &["revoke", GRANT_ID, "--yes", "--json"],
        );
        assert_eq!(output.status.code(), Some(1), "{outcome}");
        assert_eq!(json_output(&output)["outcome"], outcome);
        server.finish();
    }
}

const SESSION_ID: &str = "pgs_01k0z6r1w8f4jy2m7q9v3x5abc";
const IDENTITY_ID: &str = "idn_01k0z6r1w8f4jy2m7q9v3x5abc";
const AUTHORIZATION_URL: &str =
    "https://github.com/login/oauth/authorize?state=public-fixture-state";

// A consent session as the API presents it, carrying a material member the CLI must drop.
fn consent(status: &str) -> serde_json::Value {
    let mut session = serde_json::json!({
        "id": SESSION_ID,
        "provider": "github",
        "status": status,
        "createdAt": "2026-09-05T12:00:00Z",
        "expiresAt": "2026-09-05T12:15:00Z",
        "accessToken": SENTINEL
    });
    match status {
        "pending" => session["authorizationUrl"] = AUTHORIZATION_URL.into(),
        "completed" => {
            session["completedAt"] = "2026-09-05T12:01:00Z".into();
            session["resultGrant"] = with_material(&grants())[0].clone();
        }
        _ => {}
    }
    session
}

fn started() -> Vec<u8> {
    json_http_response("201 Created", consent("pending"))
}

fn observed(session: serde_json::Value) -> Vec<u8> {
    json_http_response("200 OK", session)
}

fn json_lines(output: &Output) -> Vec<serde_json::Value> {
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

fn request_body(request: &str) -> &str {
    request.split_once("\r\n\r\n").unwrap().1
}

// SC-GRANT-1, REQ-GRANT-1, REQ-GRANT-4
#[test]
fn create_starts_a_consent_prints_its_url_and_polls_until_the_grant_exists() {
    let (server, _directory, credential) = prepared(vec![
        started(),
        observed(consent("pending")),
        observed(consent("completed")),
    ]);
    let output = run_grant(&server, &credential, &["create", "github", "--json"]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = json_lines(&output);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["event"], "consent_required");
    assert_eq!(lines[0]["outcome"], "pending");
    assert_eq!(lines[0]["sessionId"], SESSION_ID);
    assert_eq!(lines[0]["provider"], "github");
    assert_eq!(lines[0]["authorizationUrl"], AUTHORIZATION_URL);
    assert_eq!(lines[1]["event"], "consent_result");
    assert_eq!(lines[1]["outcome"], "completed");
    assert_eq!(lines[1]["sessionId"], SESSION_ID);
    assert_eq!(lines[1]["grant"]["id"], GRANT_ID);
    assert_eq!(lines[1]["grant"]["identityId"], IDENTITY_ID);
    assert_eq!(lines[1]["grant"]["provider"], "github");
    assert_eq!(lines[1]["grant"]["state"], "active");
    assert_no_secret_output(&output, &[SENTINEL]);
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("POST /api/v1/me/grants HTTP/1.1\r\n"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&requests[0])).unwrap(),
        serde_json::json!({"provider": "github"})
    );
    assert!(header_value(&requests[0], "content-type").starts_with("application/json"));
    assert!(!header_value(&requests[0], "idempotency-key").is_empty());
    for request in &requests[1..] {
        assert!(request.starts_with(&format!(
            "GET /api/v1/me/grant-sessions/{SESSION_ID} HTTP/1.1\r\n"
        )));
    }
}

// SC-GRANT-1, REQ-GRANT-1
#[test]
fn create_reports_a_consent_that_did_not_complete_with_its_reason() {
    let mut failed = consent("failed");
    failed["reasonCode"] = "subject_mismatch".into();
    let (server, _directory, credential) = prepared(vec![started(), observed(failed)]);
    let output = run_grant(&server, &credential, &["create", "github", "--json"]);

    assert_eq!(output.status.code(), Some(1));
    let lines = json_lines(&output);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[1]["event"], "consent_result");
    assert_eq!(lines[1]["outcome"], "failed");
    assert_eq!(lines[1]["reason"], "subject_mismatch");
    assert!(lines[1]["grant"].is_null());
    assert_no_secret_output(&output, &[SENTINEL]);
    assert_eq!(server.finish().len(), 2);
}

// REQ-GRANT-1
#[test]
fn create_for_a_linked_identity_posts_no_body_and_returns_without_waiting() {
    let mut pending = consent("pending");
    pending["identityId"] = IDENTITY_ID.into();
    let (server, _directory, credential) =
        prepared(vec![json_http_response("201 Created", pending)]);
    let output = run_grant(
        &server,
        &credential,
        &[
            "create",
            "github",
            "--identity-id",
            IDENTITY_ID,
            "--no-wait",
            "--json",
        ],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = json_lines(&output);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["event"], "consent_required");
    assert_eq!(lines[0]["authorizationUrl"], AUTHORIZATION_URL);
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with(&format!(
        "POST /api/v1/me/identities/{IDENTITY_ID}/grants HTTP/1.1\r\n"
    )));
    assert!(!header_value(&requests[0], "idempotency-key").is_empty());
    assert_eq!(request_body(&requests[0]), "");
}

// REQ-GRANT-1, REQ-GRANT-4
#[test]
fn create_without_waiting_prints_only_the_pending_consent() {
    let (server, _directory, credential) = prepared(vec![started()]);
    let output = run_grant(
        &server,
        &credential,
        &["create", "github", "--no-wait", "--json"],
    );

    assert!(output.status.success());
    let lines = json_lines(&output);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["outcome"], "pending");
    assert_eq!(lines[0]["sessionId"], SESSION_ID);
    assert_eq!(lines[0]["expiresAt"], "2026-09-05T12:15:00Z");
    assert_no_secret_output(&output, &[SENTINEL]);
    assert_eq!(server.finish().len(), 1);
}

// REQ-GRANT-1
#[test]
fn create_reports_refused_starts_without_a_further_request() {
    let cases = [
        (
            &["create", "gitlab.com", "--json"][..],
            grant_problem("503 Service Unavailable", 503, "provider-unavailable"),
            "provider_unavailable",
            4,
        ),
        (
            &["create", "github", "--identity-id", IDENTITY_ID, "--json"][..],
            grant_problem("404 Not Found", 404, "identity-not-found"),
            "not_found",
            1,
        ),
        (
            &["create", "github", "--json"][..],
            grant_problem("409 Conflict", 409, "idempotency-conflict"),
            "idempotency_conflict",
            1,
        ),
    ];
    for (arguments, response, outcome, code) in cases {
        let (server, _directory, credential) = prepared(vec![response]);
        let output = run_grant(&server, &credential, arguments);
        assert_eq!(output.status.code(), Some(code), "{outcome}");
        let lines = json_lines(&output);
        assert_eq!(lines.len(), 1, "{outcome}");
        assert_eq!(lines[0]["outcome"], outcome);
        let requests = server.finish();
        assert_eq!(requests.len(), 1, "{outcome}");
        if outcome == "provider_unavailable" {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(request_body(&requests[0])).unwrap(),
                serde_json::json!({"provider": "gitlab.com"})
            );
        }
    }
}

// REQ-GRANT-1
#[test]
fn create_replays_its_key_once_after_a_lost_start_response() {
    let (server, _directory, credential) = prepared(vec![Vec::new(), started()]);
    let output = run_grant(
        &server,
        &credential,
        &["create", "github", "--no-wait", "--json"],
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(json_lines(&output)[0]["sessionId"], SESSION_ID);
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

// REQ-GRANT-1
#[test]
fn create_waits_out_a_rate_limited_poll_and_then_completes() {
    let limited = http_response_with_headers(
        "429 Too Many Requests",
        Some("application/problem+json"),
        &[("Retry-After", "1")],
        &serde_json::to_vec(&serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/rate-limit-exceeded",
            "title": "Slow down",
            "status": 429
        }))
        .unwrap(),
    );
    let (server, _directory, credential) =
        prepared(vec![started(), limited, observed(consent("completed"))]);
    let output = run_grant(&server, &credential, &["create", "github", "--json"]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = json_lines(&output);
    assert_eq!(lines.last().unwrap()["outcome"], "completed");
    assert_eq!(server.finish().len(), 3);
}
