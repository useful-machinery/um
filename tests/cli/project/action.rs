use super::*;
const ACTION: &str = "gda_01k0z6r1w8f4jy2m7q9v3x5abc";
fn action() -> serde_json::Value {
    serde_json::json!({"id":ACTION,"projectId":PROJECT_ID,"version":3,"createdAt":"2026-09-01T00:00:00Z","updatedAt":"2026-09-01T00:00:00Z","displayName":"Review","workflowPath":".um/workflows/review.yaml","enabled":true,"description":"","sortOrder":0,"inputBindings":{},"inputPresentation":{},"publication":null})
}
fn response(status: &str, body: serde_json::Value, mutation: bool) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/json"),
        if mutation {
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)]
        } else {
            &[]
        },
        &serde_json::to_vec(&body).unwrap(),
    )
}
fn run(leaf: &[&str], server: &ScriptedServer, credential: &str) -> Output {
    let mut args = vec!["project", "action"];
    args.extend_from_slice(leaf);
    args.extend(["--json", "--allow-insecure-http"]);
    run_project(&args, server, credential)
}
fn file(contents: &str) -> (tempfile::TempDir, String) {
    let dir = private_credential_directory();
    let path = dir.path().join("config.json");
    fs::write(&path, contents).unwrap();
    (dir, path.to_str().unwrap().to_owned())
}
fn prepared(responses: Vec<Vec<u8>>) -> (ScriptedServer, tempfile::TempDir, String) {
    prepared_project(responses)
}
fn collection() -> String {
    format!("/api/v1/organizations/{ORGANIZATION}/projects/{PROJECT_ID}/google-docs-actions")
}
fn target(suffix: &str) -> String {
    format!("{}/{ACTION}{suffix}", collection())
}
// REQ-DOCSACTION-65
#[test]
fn documented_example_is_accepted_by_the_create_route() {
    let (_dir, path) = file(include_str!("../../../examples/action-config.json"));
    let (server, _credentials, credential) =
        prepared(vec![response("201 Created", action(), true)]);
    assert_json_success(
        &run(
            &["create", ORGANIZATION, PROJECT_ID, "--config-file", &path],
            &server,
            &credential,
        ),
        "created",
    );
    let request = server.finish().remove(0);
    assert_eq!(
        request_body(&request)["inputBindings"]["documentId"],
        serde_json::json!({"type":"document","field":"id"})
    );
}

#[test]
fn source_json_may_exceed_normalized_configuration_limit() {
    let source = format!(
        "{{\"displayName\":\"Review\",\"workflowPath\":\".um/workflows/review.yaml\",\"enabled\":true{}}}",
        " ".repeat(1024 * 1024)
    );
    let (_dir, path) = file(&source);
    let (server, _credentials, credential) =
        prepared(vec![response("201 Created", action(), true)]);
    assert_json_success(
        &run(
            &["create", ORGANIZATION, PROJECT_ID, "--config-file", &path],
            &server,
            &credential,
        ),
        "created",
    );
    assert_eq!(request_body(&server.finish().remove(0))["enabled"], true);
}

// REQ-DOCSACTION-65
#[test]
fn action_routes_and_closed_update() {
    let (_dir, path) = file(
        r#"{"displayName":"Review","workflowPath":".um/workflows/review.yaml","enabled":false,"inputBindings":{"fixed":{"type":"literal","kind":"json","value":null}}}"#,
    );
    let (server, _credentials, credential) =
        prepared(vec![response("201 Created", action(), true)]);
    let result = run(
        &["create", ORGANIZATION, PROJECT_ID, "--config-file", &path],
        &server,
        &credential,
    );
    assert_eq!(
        assert_json_success(&result, "created")["action"]["id"],
        ACTION
    );
    let req = server.finish().remove(0);
    assert_eq!(
        req.lines().next(),
        Some(format!("POST {} HTTP/1.1", collection()).as_str())
    );
    assert_eq!(
        request_body(&req)["inputBindings"]["fixed"]["value"],
        serde_json::Value::Null
    );
    assert_eq!(request_body(&req)["enabled"], false);
    assert!(!header_value(&req, "idempotency-key").is_empty());

    fs::write(&path, r#"{"publication":null,"inputBindings":{}}"#).unwrap();
    let (server, _credentials, credential) = prepared(vec![response("200 OK", action(), true)]);
    assert_json_success(
        &run(
            &[
                "update",
                ORGANIZATION,
                PROJECT_ID,
                ACTION,
                "--expected-version",
                "3",
                "--config-file",
                &path,
            ],
            &server,
            &credential,
        ),
        "updated",
    );
    let req = server.finish().remove(0);
    assert_eq!(
        req.lines().next(),
        Some(format!("PATCH {} HTTP/1.1", target("")).as_str())
    );
    assert_eq!(
        request_body(&req),
        serde_json::json!({"expectedVersion":3,"publication":null,"inputBindings":{}})
    );
    fs::write(&path, r#"{"description":"new"}"#).unwrap();
    let (server, _credentials, credential) = prepared(vec![response("200 OK", action(), true)]);
    assert_json_success(
        &run(
            &[
                "update",
                ORGANIZATION,
                PROJECT_ID,
                ACTION,
                "--expected-version",
                "3",
                "--config-file",
                &path,
            ],
            &server,
            &credential,
        ),
        "updated",
    );
    assert_eq!(
        request_body(&server.finish().remove(0)),
        serde_json::json!({"expectedVersion":3,"description":"new"})
    );
}
// REQ-DOCSACTION-65, REQ-DOCSACTION-67, REQ-DOCSACTION-68
#[test]
fn reads_lifecycle_and_delete_observe_status_and_confirmation() {
    let (server, _credentials, credential) = prepared(vec![response(
        "200 OK",
        serde_json::json!({"items":[action()],"nextCursor":"next"}),
        false,
    )]);
    let page = assert_json_success(
        &run(
            &[
                "list",
                ORGANIZATION,
                PROJECT_ID,
                "--limit",
                "1",
                "--cursor",
                "from",
            ],
            &server,
            &credential,
        ),
        "listed",
    );
    assert_eq!(page["items"][0]["id"], ACTION);
    assert_eq!(page["nextCursor"], "next");
    let req = server.finish().remove(0);
    assert_eq!(
        req.lines().next(),
        Some(format!("GET {}?limit=1&cursor=from HTTP/1.1", collection()).as_str())
    );
    assert!(!req.contains("idempotency-key:"));
    for (name, suffix, outcome) in [
        ("enable", "/enable", "enabled"),
        ("disable", "/disable", "disabled"),
    ] {
        let (server, _credentials, credential) = prepared(vec![response("200 OK", action(), true)]);
        let value = assert_json_success(
            &run(
                &[name, ORGANIZATION, PROJECT_ID, ACTION],
                &server,
                &credential,
            ),
            outcome,
        );
        assert_eq!(value["action"]["id"], ACTION);
        let req = server.finish().remove(0);
        assert_eq!(
            req.lines().next(),
            Some(format!("POST {} HTTP/1.1", target(suffix)).as_str())
        );
        assert_eq!(req.split_once("\r\n\r\n").unwrap().1, "");
    }
    let (server, _credentials, credential) = prepared(vec![response("200 OK", action(), false)]);
    assert_eq!(
        assert_json_success(
            &run(
                &["show", ORGANIZATION, PROJECT_ID, ACTION],
                &server,
                &credential
            ),
            "found"
        )["action"]["id"],
        ACTION
    );
    assert_eq!(
        server.finish()[0].lines().next(),
        Some(format!("GET {} HTTP/1.1", target("")).as_str())
    );
    let tombstone = serde_json::json!({"id":ACTION,"projectId":PROJECT_ID,"deleted":true,"deletedAt":"2026-09-02T00:00:00Z"});
    let (server, _credentials, credential) =
        prepared(vec![response("200 OK", tombstone.clone(), false)]);
    assert_eq!(
        assert_json_success(
            &run(
                &["show", ORGANIZATION, PROJECT_ID, ACTION],
                &server,
                &credential
            ),
            "deleted"
        )["tombstone"],
        tombstone
    );
    assert_eq!(
        server.finish()[0].lines().next(),
        Some(format!("GET {} HTTP/1.1", target("")).as_str())
    );
    let (server, _credentials, credential) = prepared(vec![http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        b"",
    )]);
    assert_json_success(
        &run(
            &["delete", ORGANIZATION, PROJECT_ID, ACTION, "--yes"],
            &server,
            &credential,
        ),
        "deleted",
    );
    let req = server.finish().remove(0);
    assert_eq!(
        req.lines().next(),
        Some(format!("DELETE {} HTTP/1.1", target("")).as_str())
    );
    assert_eq!(req.split_once("\r\n\r\n").unwrap().1, "");
    let (server, _credentials, credential) = prepared(vec![]);
    assert!(
        !run(
            &["delete", ORGANIZATION, PROJECT_ID, ACTION],
            &server,
            &credential
        )
        .status
        .success()
    );
    assert!(server.finish().is_empty());
}
// REQ-DOCSACTION-70
#[test]
fn human_output_uses_safe_metadata_and_errors_use_stderr() {
    let (server, _credentials, credential) = prepared(vec![response("200 OK", action(), false)]);
    let output = run_project(
        &[
            "project",
            "action",
            "show",
            ORGANIZATION,
            PROJECT_ID,
            ACTION,
            "--allow-insecure-http",
        ],
        &server,
        &credential,
    );
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(String::from_utf8_lossy(&output.stdout).contains(ACTION));
    assert_eq!(server.finish().len(), 1);

    let response = problem_http_response(
        "403 Forbidden",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/forbidden", "title":"private-literal", "status":403}),
    );
    let (server, _credentials, credential) = prepared(vec![response]);
    let output = run_project(
        &[
            "project",
            "action",
            "disable",
            ORGANIZATION,
            PROJECT_ID,
            ACTION,
            "--allow-insecure-http",
        ],
        &server,
        &credential,
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private-literal"));
    assert_eq!(server.finish().len(), 1);
}

// REQ-DOCSACTION-66
#[test]
fn invalid_input_and_denial_do_not_leak_literals() {
    let (_dir, path) = file(r#"{"publication":{"exportName":"secret","extra":"private"}}"#);
    let (server, _credentials, credential) = prepared(vec![]);
    let result = run(
        &[
            "update",
            ORGANIZATION,
            PROJECT_ID,
            ACTION,
            "--expected-version",
            "1",
            "--config-file",
            &path,
        ],
        &server,
        &credential,
    );
    assert!(!result.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.stdout).unwrap()["outcome"],
        "invalid_input"
    );
    assert!(
        !format!(
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        )
        .contains("private")
    );
    assert!(server.finish().is_empty());
    let (server, _credentials, credential) = prepared(vec![]);
    assert!(
        !run(
            &[
                "update",
                ORGANIZATION,
                PROJECT_ID,
                ACTION,
                "--expected-version",
                "0",
                "--config-file",
                &path
            ],
            &server,
            &credential
        )
        .status
        .success()
    );
    assert!(server.finish().is_empty());
    let response = problem_http_response(
        "403 Forbidden",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/forbidden","title":"private","status":403}),
    );
    let (server, _credentials, credential) = prepared(vec![response]);
    let result = run(
        &["disable", ORGANIZATION, PROJECT_ID, ACTION],
        &server,
        &credential,
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.stdout).unwrap()["outcome"],
        "forbidden"
    );
    assert!(!String::from_utf8_lossy(&result.stdout).contains("private"));
    assert_eq!(server.finish().len(), 1);
    let response = http_response_with_headers("429 Too Many Requests", Some("application/problem+json"), &[("Retry-After", "17")], &serde_json::to_vec(&serde_json::json!({"type":"https://api.usefulmachinery.com/problems/rate-limited","title":"private","status":429})).unwrap());
    let (server, _credentials, credential) = prepared(vec![response]);
    let result = run(&["list", ORGANIZATION, PROJECT_ID], &server, &credential);
    let value: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(value["outcome"], "rate_limited");
    assert_eq!(value["retryAfter"], 17);
    assert_eq!(server.finish().len(), 1);
}
// REQ-DOCSACTION-66
#[test]
fn standard_input_conflict_is_rejected_before_authentication() {
    let config =
        br#"{"displayName":"Review","workflowPath":".um/workflows/review.yaml","enabled":true}"#;
    let (server, _credentials, credential) =
        prepared(vec![response("201 Created", action(), true)]);
    let environment = deployment_environment(&server.api_url, &credential);
    let output = run_with_stdin(
        &[
            "project",
            "action",
            "create",
            ORGANIZATION,
            PROJECT_ID,
            "--config-file",
            "-",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
        config,
    );
    assert_json_success(&output, "created");
    assert_eq!(server.finish().len(), 1);
    let (server, _credentials, credential) = prepared(vec![]);
    let environment = deployment_environment(&server.api_url, &credential);
    let output = run_with_stdin(
        &[
            "project",
            "action",
            "create",
            ORGANIZATION,
            PROJECT_ID,
            "--config-file",
            "-",
            "--service-api-key-file",
            "-",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
        config,
    );
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_input"
    );
    assert!(server.finish().is_empty());
}

// REQ-DOCSACTION-69
#[test]
fn lost_reply_retries_identical_body_and_key() {
    let (_dir, path) = file(
        r#"{"displayName":"Review","workflowPath":".um/workflows/review.yaml","enabled":false}"#,
    );
    let (server, _credentials, credential) =
        prepared(vec![Vec::new(), response("201 Created", action(), true)]);
    assert_json_success(
        &run(
            &["create", ORGANIZATION, PROJECT_ID, "--config-file", &path],
            &server,
            &credential,
        ),
        "created",
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
}

// REQ-DOCSACTION-69, REQ-DOCSACTION-70
#[test]
fn unresolved_reply_reports_key_without_configuration() {
    let (_dir, path) = file(
        r#"{"displayName":"Review","workflowPath":".um/workflows/review.yaml","enabled":true,"inputBindings":{"x":{"type":"literal","kind":"text","value":"private-literal"}}}"#,
    );
    let (server, _credentials, credential) = prepared(vec![Vec::new(), Vec::new()]);
    let output = run(
        &["create", ORGANIZATION, PROJECT_ID, "--config-file", &path],
        &server,
        &credential,
    );
    assert!(!output.status.success());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        result["idempotencyKey"],
        header_value(&requests[0], "idempotency-key")
    );
    assert_eq!(result["nextAction"], "inspect_resource");
    assert!(
        !format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .contains("private-literal")
    );
}

// REQ-DOCSACTION-69
#[test]
fn invalid_successful_replay_does_not_settle_a_lost_reply() {
    let (_dir, path) = file(
        r#"{"displayName":"Review","workflowPath":".um/workflows/review.yaml","enabled":true}"#,
    );
    let mut wrong = action();
    wrong["projectId"] = "other-project".into();
    let (server, _credentials, credential) =
        prepared(vec![Vec::new(), response("201 Created", wrong, true)]);
    let output = run(
        &["create", ORGANIZATION, PROJECT_ID, "--config-file", &path],
        &server,
        &credential,
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "result_uncertain");
    assert_eq!(result["replayOutcome"], "invalid_response");
    assert_eq!(
        result["idempotencyKey"],
        header_value(&requests[0], "idempotency-key")
    );
}

#[test]
fn rejected_replay_does_not_settle_a_lost_create_reply() {
    let (_dir, path) = file(
        r#"{"displayName":"Review","workflowPath":".um/workflows/review.yaml","enabled":true}"#,
    );
    let denied = problem_http_response(
        "403 Forbidden",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/forbidden","title":"private","status":403}),
    );
    let (server, _credentials, credential) = prepared(vec![Vec::new(), denied]);
    let output = run(
        &["create", ORGANIZATION, PROJECT_ID, "--config-file", &path],
        &server,
        &credential,
    );
    assert_eq!(output.status.code(), Some(4));
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
    let key = header_value(&requests[0], "idempotency-key");
    assert_eq!(key, header_value(&requests[1], "idempotency-key"));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "result_uncertain");
    assert_eq!(result["idempotencyKey"], key);
    assert_eq!(result["replayOutcome"], "forbidden");
    assert_eq!(result["nextAction"], "inspect_resource");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("private"));
}

#[test]
fn action_conflict_is_not_a_protocol_failure() {
    // The current Gateway uses the shared source-connection-conflict problem for
    // both a stale expectedVersion and a reused request identity.
    let (_dir, path) = file(r#"{"description":"changed"}"#);
    let conflict = problem_http_response(
        "409 Conflict",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/source-connection-conflict","title":"private","status":409}),
    );
    let (server, _credentials, credential) = prepared(vec![conflict]);
    let output = run(
        &[
            "update",
            ORGANIZATION,
            PROJECT_ID,
            ACTION,
            "--config-file",
            &path,
            "--expected-version",
            "1",
        ],
        &server,
        &credential,
    );
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "conflict");
    assert!(result.get("idempotencyKey").is_none());
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn rejected_replay_after_credential_refresh_stays_uncertain() {
    let (_dir, path) = file(
        r#"{"displayName":"Review","workflowPath":".um/workflows/review.yaml","enabled":true}"#,
    );
    let unauthorized = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/unauthorized","title":"private","status":401}),
    );
    let refreshed = json_http_response(
        "200 OK",
        serde_json::json!({"access_token":"new-action-token", "refresh_token":"new-action-refresh", "token_type":"Bearer", "expires_in":3600}),
    );
    let forbidden = problem_http_response(
        "403 Forbidden",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/forbidden","title":"private","status":403}),
    );
    let server = ScriptedServer::respond(vec![Vec::new(), unauthorized, refreshed, forbidden]);
    let directory = private_credential_directory();
    let credentials = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credentials,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credentials.to_str().unwrap(),
    );
    let output = run_with_env(
        &[
            "project",
            "action",
            "create",
            ORGANIZATION,
            PROJECT_ID,
            "--config-file",
            &path,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(output.status.code(), Some(4));
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[2].starts_with("POST /auth/oauth/token HTTP/1.1"));
    let key = header_value(&requests[0], "idempotency-key");
    for request in [&requests[1], &requests[3]] {
        assert_eq!(header_value(request, "idempotency-key"), key);
        assert_eq!(request_body(request), request_body(&requests[0]));
    }
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "result_uncertain");
    assert_eq!(result["replayOutcome"], "forbidden");
    assert_eq!(result["idempotencyKey"], key);
}

#[test]
fn list_accepts_multiple_large_valid_action_configurations() {
    let items: Vec<_> = (0..3).map(|index| {
        let mut item = action();
        item["id"] = format!("gda_01k0z6r1w8f4jy2m7q9v3x5ab{index}").into();
        item["inputBindings"] = serde_json::json!({"large":{"type":"literal","kind":"text","value":"<".repeat(400_000)}});
        item
    }).collect();
    let page = serde_json::json!({"items":items, "nextCursor":null});
    let body = serde_json::to_vec(&page).unwrap();
    assert!(body.len() > 1024 * 1024);
    let (server, _credentials, credential) = prepared(vec![http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        &body,
    )]);
    let output = run(
        &["list", ORGANIZATION, PROJECT_ID, "--limit", "3"],
        &server,
        &credential,
    );
    let result = assert_json_success(&output, "listed");
    assert_eq!(result["items"].as_array().unwrap().len(), 3);
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn single_action_allows_wire_escaping_above_one_mebibyte() {
    let mut item = action();
    item["inputBindings"] =
        serde_json::json!({"large":{"type":"literal","kind":"text","value":"<".repeat(260_000)}});
    // Gateway's JSON encoder escapes HTML even though the normalized stored
    // configuration fits in 1 MiB.
    let wire = serde_json::to_string(&item)
        .unwrap()
        .replace('<', "\\u003c");
    assert!(wire.len() > 1024 * 1024);
    let (server, _credentials, credential) = prepared(vec![http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[],
        wire.as_bytes(),
    )]);
    let result = assert_json_success(
        &run(
            &["show", ORGANIZATION, PROJECT_ID, ACTION],
            &server,
            &credential,
        ),
        "found",
    );
    assert_eq!(
        result["action"]["inputBindings"]["large"]["value"]
            .as_str()
            .unwrap()
            .len(),
        260_000
    );
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn refresh_preserves_mutation_identity() {
    let (_dir, path) = file(
        r#"{"displayName":"Review","workflowPath":".um/workflows/review.yaml","enabled":true}"#,
    );
    let denied = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/unauthorized", "title":"Rejected", "status":401}),
    );
    let refreshed = json_http_response(
        "200 OK",
        serde_json::json!({"access_token":"new-action-token", "refresh_token":"new-action-refresh", "token_type":"Bearer", "expires_in":3600}),
    );
    let server = ScriptedServer::respond(vec![
        denied,
        refreshed,
        response("201 Created", action(), true),
    ]);
    let directory = private_credential_directory();
    let credentials = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credentials,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credentials.to_str().unwrap(),
    );
    let output = run_with_env(
        &[
            "project",
            "action",
            "create",
            ORGANIZATION,
            PROJECT_ID,
            "--config-file",
            &path,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_json_success(&output, "created");
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1"));
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[2], "idempotency-key")
    );
    assert_eq!(request_body(&requests[0]), request_body(&requests[2]));
}
