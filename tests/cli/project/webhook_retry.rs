use super::*;

#[test]
fn lost_mutation_response_reuses_identity_and_bytes_without_reporting_success() {
    let input = private_credential_directory();
    let file = input.path().join("selection.json");
    fs::write(
        &file,
        r#"{"url":"https://receiver.example.com/hook","eventTypes":["run.failed"]}"#,
    )
    .unwrap();
    let server = ScriptedServer::respond(vec![Vec::new(), Vec::new()]);
    let (server, _credentials_directory, credentials) = prepared_server(server);
    let output = run(
        &[
            "create",
            ORGANIZATION,
            PROJECT_ID,
            "--config-file",
            file.to_str().unwrap(),
        ],
        &server,
        &credentials,
    );
    assert!(!output.status.success());
    let output_text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output_text.contains("receiver.example.com"));
    assert!(!output_text.contains("secret"));
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        document["idempotencyKey"],
        header_value(&requests[0], "idempotency-key")
    );
    assert_eq!(document["projectId"], PROJECT_ID);
    assert_eq!(document["nextAction"], "inspect_resource");
    assert!(document.get("webhookId").is_none());
}

#[test]
fn human_uncertainty_and_metadata_only_use_safe_known_identity() {
    let directory = private_credential_directory();
    let file = directory.path().join("selection.json");
    fs::write(
        &file,
        r#"{"url":"https://receiver.example.com/hook","eventTypes":["run.failed"]}"#,
    )
    .unwrap();
    let leaf = [
        "project",
        "webhook",
        "create",
        ORGANIZATION,
        PROJECT_ID,
        "--config-file",
        file.to_str().unwrap(),
        "--allow-insecure-http",
    ];
    let server = ScriptedServer::respond(vec![response("201 Created", subscription(false), true)]);
    let (server, _directory, credential) = prepared_server(server);
    let output = run_project(&leaf, &server, &credential);
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("No secret recovered"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("Signing secret (shown once)"));
    assert_eq!(server.finish().len(), 1);

    let server = ScriptedServer::respond(vec![Vec::new(), Vec::new()]);
    let (server, _directory, credential) = prepared_server(server);
    let output = run_project(&leaf, &server, &credential);
    assert!(!output.status.success());
    let requests = server.finish();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(PROJECT_ID));
    assert!(stderr.contains(header_value(&requests[0], "idempotency-key")));
    assert!(stderr.contains("Inspect the subscription or delivery"));
    assert!(!stderr.contains("receiver.example.com"));
}

#[test]
fn webhook_generic_conflict_and_rate_limit_preserve_structured_outcomes() {
    for (status, problem_type, outcome, retry) in [
        ("409 Conflict", "webhook-conflict", "conflict", false),
        (
            "429 Too Many Requests",
            "rate-limited",
            "rate_limited",
            true,
        ),
    ] {
        let body = serde_json::json!({"type": format!("https://api.usefulmachinery.com/problems/{problem_type}"), "title": "No secret here", "status": if retry {429} else {409}});
        let response = http_response_with_headers(
            status,
            Some("application/problem+json"),
            if retry { &[("Retry-After", "17")] } else { &[] },
            &serde_json::to_vec(&body).unwrap(),
        );
        let server = ScriptedServer::respond(vec![response]);
        let (server, _credential_directory, credentials) = prepared_server(server);
        let output = run(
            &["test", ORGANIZATION, PROJECT_ID, WEBHOOK],
            &server,
            &credentials,
        );
        assert!(!output.status.success());
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["outcome"], outcome);
        if retry {
            assert_eq!(value["retryAfter"], 17);
        }
        assert!(!String::from_utf8_lossy(&output.stdout).contains("No secret here"));
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn credential_refresh_keeps_one_mutation_key_and_body() {
    let input = private_credential_directory();
    let file = input.path().join("selection.json");
    fs::write(&file, r#"{"url":"https://receiver.example.com/hook","eventTypes":["run.failed"],"contextKeys":[]}"#).unwrap();
    let rejected = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
            "type":"https://api.usefulmachinery.com/problems/unauthorized", "title":"Rejected", "status":401
        }),
    );
    let refreshed = json_http_response(
        "200 OK",
        serde_json::json!({
            "access_token":"new-webhook-token", "refresh_token":"new-webhook-refresh", "token_type":"Bearer", "expires_in":3600
        }),
    );
    let server = ScriptedServer::respond(vec![
        rejected,
        refreshed,
        response("201 Created", subscription(false), true),
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
        &args(&[
            "create",
            ORGANIZATION,
            PROJECT_ID,
            "--config-file",
            file.to_str().unwrap(),
        ]),
        &environment,
    );
    assert_json_success(&output, "created");
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].starts_with("POST /auth/oauth/token HTTP/1.1"));
    assert_eq!(
        header_value(&requests[0], "authorization"),
        format!("Bearer {TOKEN}")
    );
    assert_eq!(
        header_value(&requests[2], "authorization"),
        "Bearer new-webhook-token"
    );
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[2], "idempotency-key")
    );
    assert_eq!(
        requests[0].split_once("\r\n\r\n").unwrap().1,
        requests[2].split_once("\r\n\r\n").unwrap().1
    );
}

#[test]
fn lost_success_body_returns_metadata_without_revealing_original_secret() {
    let input = private_credential_directory();
    let file = input.path().join("selection.json");
    fs::write(
        &file,
        r#"{"url":"https://receiver.example.com/hook","eventTypes":["run.failed"]}"#,
    )
    .unwrap();
    let partial = b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 512\r\nConnection: close\r\n\r\n{\"secret\":\"DO_NOT_DISCLOSE\"".to_vec();
    let server = ScriptedServer::respond(vec![
        partial,
        response("201 Created", subscription(false), true),
    ]);
    let (server, _directory, credential) = prepared_server(server);
    let output = run(
        &[
            "create",
            ORGANIZATION,
            PROJECT_ID,
            "--config-file",
            file.to_str().unwrap(),
        ],
        &server,
        &credential,
    );
    let value = assert_json_success(&output, "created");
    assert!(value["webhook"].get("secret").is_none());
    assert_eq!(value["secretDisclosure"], "metadata_only");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("DO_NOT_DISCLOSE"));
}

#[test]
fn metadata_only_mutation_and_authorization_failures_do_not_reveal_secrets() {
    let input = private_credential_directory();
    let file = input.path().join("selection.json");
    fs::write(
        &file,
        r#"{"url":"https://receiver.example.com/hook","eventTypes":["run.failed"]}"#,
    )
    .unwrap();
    let commands = [
        (
            vec![
                "create",
                ORGANIZATION,
                PROJECT_ID,
                "--config-file",
                file.to_str().unwrap(),
            ],
            "201 Created",
            subscription(false),
            "created",
        ),
        (
            vec!["rotate-secret", ORGANIZATION, PROJECT_ID, WEBHOOK],
            "200 OK",
            subscription(false),
            "rotated",
        ),
    ];
    for (args, status, body, outcome) in commands {
        let server = ScriptedServer::respond(vec![response(status, body, true)]);
        let (server, _credential_directory, credentials) = prepared_server(server);
        let output = run(&args, &server, &credentials);
        let value = assert_json_success(&output, outcome);
        assert!(value["webhook"].get("secret").is_none());
        assert_eq!(value["secretDisclosure"], "metadata_only");
        assert_eq!(server.finish().len(), 1);
    }
    for (status, problem_type, outcome, code) in [
        ("403 Forbidden", "forbidden", "forbidden", 403),
        ("404 Not Found", "not-found", "not_found", 404),
    ] {
        let response = problem_http_response(
            status,
            serde_json::json!({"type":format!("https://api.usefulmachinery.com/problems/{problem_type}"),"title":"Do not print this","status":code}),
        );
        let server = ScriptedServer::respond(vec![response]);
        let (server, _credential_directory, credentials) = prepared_server(server);
        let output = run(
            &["show", ORGANIZATION, PROJECT_ID, WEBHOOK],
            &server,
            &credentials,
        );
        assert!(!output.status.success());
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["outcome"], outcome);
        assert_eq!(server.finish().len(), 1);
    }
}
