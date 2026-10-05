use super::*;

#[path = "webhook_retry.rs"]
mod webhook_retry;

const WEBHOOK: &str = "whs_01k0z6r1w8f4jy2m7q9v3x5abc";
const DELIVERY: &str = "whd_01k0z6r1w8f4jy2m7q9v3x5abc";
const EVENT: &str = "evt_01k0z6r1w8f4jy2m7q9v3x5abc";

fn subscription(secret: bool) -> serde_json::Value {
    let mut body = serde_json::json!({"id":WEBHOOK,"projectId":PROJECT_ID,"url":"https://receiver.example.com/hook","state":"enabled","version":7,"eventTypes":["run.failed"],"contextKeys":[],"createdAt":"2026-09-01T00:00:00Z","updatedAt":"2026-09-01T00:00:00Z"});
    if secret {
        body["secret"] = serde_json::json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    }
    body
}
fn delivery() -> serde_json::Value {
    serde_json::json!({"id":DELIVERY,"eventId":EVENT,"eventType":"run.failed","runId":RUN_ID,"attemptId":"atm_01k0z6r1w8f4jy2m7q9v3x5abc","workflowPath":"workflows/build.yaml","sequence":7,"subscriptionVersion":7,"createdAt":"2026-09-01T00:00:00Z","state":"failed","cycles":[{"number":1,"origin":"automatic","state":"failed","dueAt":"2026-09-01T00:00:00Z","createdAt":"2026-09-01T00:00:00Z","updatedAt":"2026-09-01T00:00:00Z","attempts":[]}]})
}
fn test_delivery() -> serde_json::Value {
    serde_json::json!({"id":DELIVERY,"eventId":EVENT,"eventType":"webhook.test","subscriptionVersion":7,"createdAt":"2026-09-01T00:00:00Z","state":"queued"})
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
fn args<'a>(leaf: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["project", "webhook"];
    args.extend_from_slice(leaf);
    args.extend(["--json", "--allow-insecure-http"]);
    args
}
fn run(leaf: &[&str], server: &ScriptedServer, credentials: &str) -> Output {
    run_project(&args(leaf), server, credentials)
}

#[test]
fn webhook_preserves_gateway_valid_raw_url_bytes() {
    for url in [
        "HTTPS://receiver.example.com/h",
        "https://receiver.example.com/h ",
    ] {
        let directory = private_credential_directory();
        let path = directory.path().join("selection.json");
        fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({"url":url,"eventTypes":["run.failed"]}))
                .unwrap(),
        )
        .unwrap();
        let mut metadata = subscription(true);
        metadata["url"] = serde_json::Value::String(url.to_owned());
        let server = ScriptedServer::respond(vec![response("201 Created", metadata, true)]);
        let (server, _directory, credential) = prepared_server(server);
        let output = run(
            &[
                "create",
                ORGANIZATION,
                PROJECT_ID,
                "--config-file",
                path.to_str().unwrap(),
            ],
            &server,
            &credential,
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        let (_, body) = requests[0].split_once("\r\n\r\n").unwrap();
        let body: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["url"], url);
    }
}

#[test]
fn webhook_owner_leaves_use_exact_paths_statuses_and_nonsecret_inspection() {
    let paths: &[(&[&str], &str, &str, bool)] = &[
        (
            &["show", ORGANIZATION, PROJECT_ID, WEBHOOK],
            "GET",
            "/webhooks/",
            false,
        ),
        (
            &["enable", ORGANIZATION, PROJECT_ID, WEBHOOK],
            "POST",
            "/enable",
            true,
        ),
        (
            &["disable", ORGANIZATION, PROJECT_ID, WEBHOOK],
            "POST",
            "/disable",
            true,
        ),
        (
            &["rotate-secret", ORGANIZATION, PROJECT_ID, WEBHOOK],
            "POST",
            "/rotate-secret",
            true,
        ),
        (
            &["revoke-previous-secret", ORGANIZATION, PROJECT_ID, WEBHOOK],
            "POST",
            "/revoke-previous-secret",
            true,
        ),
    ];
    for (leaf, method, suffix, mutation) in paths {
        let server = ScriptedServer::respond(vec![response(
            "200 OK",
            subscription(*suffix == "/rotate-secret"),
            *mutation,
        )]);
        let (server, _credential_directory, credential) = prepared_server(server);
        let output = run(leaf, &server, &credential);
        assert!(
            output.status.success(),
            "stdout: {} stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(json["schemaVersion"], 1);
        assert_eq!(json["webhook"]["id"], WEBHOOK);
        assert_eq!(
            json["webhook"]["secret"].is_string(),
            *suffix == "/rotate-secret"
        );
        let request = server.finish().remove(0);
        assert!(request.starts_with(method));
        assert!(request.lines().next().unwrap().contains(suffix));
        assert_eq!(request.contains("idempotency-key:"), *mutation);
    }
    let server = ScriptedServer::respond(vec![http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        b"",
    )]);
    let (server, _credential_directory, credentials) = prepared_server(server);
    assert_json_success(
        &run(
            &["delete", ORGANIZATION, PROJECT_ID, WEBHOOK, "--yes"],
            &server,
            &credentials,
        ),
        "deleted",
    );
    assert_eq!(server.finish().len(), 1);
    for (leaf, suffix) in [
        (&["test", ORGANIZATION, PROJECT_ID, WEBHOOK][..], "/test"),
        (
            &[
                "delivery",
                "replay",
                ORGANIZATION,
                PROJECT_ID,
                WEBHOOK,
                DELIVERY,
            ][..],
            "/replay",
        ),
    ] {
        let server = ScriptedServer::respond(vec![response(
            "202 Accepted",
            if suffix == "/test" {
                test_delivery()
            } else {
                delivery()
            },
            true,
        )]);
        let (server, _credential_directory, credentials) = prepared_server(server);
        let output = run(leaf, &server, &credentials);
        assert!(
            output.status.success(),
            "stdout: {} stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !String::from_utf8_lossy(&output.stdout)
                .contains("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        if suffix == "/replay" {
            assert_eq!(value["outcome"], "replayed");
            assert_eq!(value["metadataOnly"], true);
        }
        assert!(
            server
                .finish()
                .remove(0)
                .lines()
                .next()
                .unwrap()
                .contains(suffix)
        );
    }
}
fn prepared_server(server: ScriptedServer) -> (ScriptedServer, tempfile::TempDir, String) {
    let dir = private_credential_directory();
    let path = dir.path().join("credentials.json");
    write_credential_fixture(&path, &server.api_url, TOKEN, "2999-01-01T00:00:00Z");
    (server, dir, path.to_str().unwrap().to_owned())
}
#[test]
fn webhook_create_update_and_filtered_pages_preserve_selection() {
    let input = private_credential_directory();
    let file = input.path().join("selection.json");
    fs::write(&file, r#"{"url":"https://receiver.example.com/hook","eventTypes":["run.failed","step.started"],"workflowPaths":["workflows/build.yaml"],"steps":[{"scope":["outer"],"role":"step","id":"inner"}],"contextKeys":[]}"#).unwrap();
    let path = file.to_str().unwrap();
    let server = ScriptedServer::respond(vec![response("201 Created", subscription(true), true)]);
    let (server, _credential_directory, credentials) = prepared_server(server);
    let output = run(
        &["create", ORGANIZATION, PROJECT_ID, "--config-file", path],
        &server,
        &credentials,
    );
    assert!(
        output.status.success(),
        "stdout: {} stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result = assert_json_success(&output, "created");
    assert!(result["webhook"]["secret"].is_string());
    assert_eq!(result["secretDisclosure"], "shown_once");
    let request = server.finish().remove(0);
    assert_eq!(
        request_body(&request)["steps"][0]["scope"],
        serde_json::json!(["outer"])
    );
    assert_eq!(request_body(&request)["contextKeys"], serde_json::json!([]));

    fs::write(&file, r#"{"contextKeys":[]}"#).unwrap();
    let server = ScriptedServer::respond(vec![response("200 OK", subscription(false), true)]);
    let (server, _credential_directory, credentials) = prepared_server(server);
    assert_json_success(
        &run(
            &[
                "update",
                ORGANIZATION,
                PROJECT_ID,
                WEBHOOK,
                "--config-file",
                path,
                "--expected-version",
                "7",
            ],
            &server,
            &credentials,
        ),
        "updated",
    );
    let request = server.finish().remove(0);
    assert!(request.starts_with("PATCH "));
    assert_eq!(
        request_body(&request),
        serde_json::json!({"contextKeys":[],"expectedVersion":7})
    );

    for (leaf, body) in [
        (
            vec![
                "list",
                ORGANIZATION,
                PROJECT_ID,
                "--limit",
                "2",
                "--cursor",
                "opaque-9",
            ],
            serde_json::json!({"items":[subscription(false)], "nextCursor":"opaque-10"}),
        ),
        (
            vec![
                "delivery",
                "list",
                ORGANIZATION,
                PROJECT_ID,
                WEBHOOK,
                "--limit",
                "2",
                "--cursor",
                "opaque-9",
                "--run-id",
                RUN_ID,
                "--state",
                "failed",
            ],
            serde_json::json!({"items":[delivery()], "nextCursor":"opaque-10"}),
        ),
        (
            vec![
                "delivery",
                "show",
                ORGANIZATION,
                PROJECT_ID,
                WEBHOOK,
                DELIVERY,
            ],
            delivery(),
        ),
    ] {
        let server = ScriptedServer::respond(vec![response("200 OK", body, false)]);
        let (server, _credential_directory, credentials) = prepared_server(server);
        let output = run(&leaf, &server, &credentials);
        assert!(
            output.status.success(),
            "stdout: {} stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let request = server.finish().remove(0);
        if leaf.contains(&"opaque-9") {
            assert!(request.contains("cursor=opaque-9"));
            assert!(request.contains("limit=2"));
        }
        if leaf.contains(&RUN_ID) {
            assert!(request.contains(&format!("runId={RUN_ID}")));
            assert!(request.contains("state=failed"));
        }
    }
}
#[test]
fn webhook_rejects_mismatched_and_malformed_delivery_responses() {
    let mut cases = Vec::new();
    let mut wrong = delivery();
    wrong["id"] = serde_json::json!("whd_01k0z6r1w8f4jy2m7q9v3x5ab2");
    cases.push((
        vec![
            "delivery",
            "replay",
            ORGANIZATION,
            PROJECT_ID,
            WEBHOOK,
            DELIVERY,
        ],
        wrong,
        "202 Accepted",
    ));
    let mut wrong = test_delivery();
    wrong["eventType"] = serde_json::json!("run.failed");
    cases.push((
        vec!["test", ORGANIZATION, PROJECT_ID, WEBHOOK],
        wrong,
        "202 Accepted",
    ));
    for (field, value) in [
        ("sequence", serde_json::json!(0)),
        ("runId", serde_json::json!("run_wrong")),
        ("eventType", serde_json::json!("step.failed")),
        ("attemptId", serde_json::json!("atm_wrong")),
    ] {
        let mut wrong = delivery();
        wrong[field] = value;
        cases.push((
            vec![
                "delivery",
                "show",
                ORGANIZATION,
                PROJECT_ID,
                WEBHOOK,
                DELIVERY,
            ],
            wrong,
            "200 OK",
        ));
    }
    for (leaf, body, status) in cases {
        let server =
            ScriptedServer::respond(vec![response(status, body, status == "202 Accepted")]);
        let (server, _directory, credential) = prepared_server(server);
        let output = run(&leaf, &server, &credential);
        assert!(!output.status.success(), "accepted {}", leaf.join(" "));
        let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(document["outcome"], "invalid_response");
        if status == "202 Accepted" {
            assert_eq!(document["webhookId"], WEBHOOK);
            assert_eq!(document["nextAction"], "inspect_resource");
            assert!(document["idempotencyKey"].is_string());
        }
        assert_eq!(server.finish().len(), 1);
    }
    let mut wrong = delivery();
    wrong["sequence"] = serde_json::json!(0);
    let server = ScriptedServer::respond(vec![response(
        "200 OK",
        serde_json::json!({"items":[wrong]}),
        false,
    )]);
    let (server, _directory, credential) = prepared_server(server);
    let output = run(
        &["delivery", "list", ORGANIZATION, PROJECT_ID, WEBHOOK],
        &server,
        &credential,
    );
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_response"
    );
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn webhook_config_standard_input_and_service_actor_are_independent() {
    let source = br#"{"url":"https://receiver.example.com/hook","eventTypes":["run.failed"],"contextKeys":[]}"#;
    let server = ScriptedServer::respond(vec![response("201 Created", subscription(false), true)]);
    let (server, _directory, credential) = prepared_server(server);
    let environment = deployment_environment(&server.api_url, &credential);
    let output = run_with_stdin(
        &args(&["create", ORGANIZATION, PROJECT_ID, "--config-file", "-"]),
        &environment,
        source,
    );
    assert_json_success(&output, "created");
    let request = server.finish().remove(0);
    assert_eq!(request_body(&request)["contextKeys"], serde_json::json!([]));

    let server = ScriptedServer::respond(vec![]);
    let (_server, _directory, credential) = prepared_server(server);
    let environment = deployment_environment(&_server.api_url, &credential);
    let output = run_with_stdin(
        &args(&[
            "create",
            ORGANIZATION,
            PROJECT_ID,
            "--config-file",
            "-",
            "--service-api-key-file",
            "-",
        ]),
        &environment,
        source,
    );
    assert!(!output.status.success());
    assert!(_server.finish().is_empty());
}

#[test]
fn webhook_invalid_config_and_stdin_contention_make_no_requests() {
    let server = ScriptedServer::respond(vec![]);
    let (server, _directory, credential) = prepared_server(server);
    let help = run_with_env(
        &["project", "webhook", "delivery"],
        &deployment_environment(&server.api_url, &credential),
    );
    assert!(help.status.success());
    assert!(
        String::from_utf8_lossy(&help.stdout)
            .contains("Usage: um project webhook delivery [COMMAND]")
    );
    assert!(server.finish().is_empty());
    let server = ScriptedServer::respond(vec![]);
    let (server, _directory, credential) = prepared_server(server);
    let output = run(
        &["delete", ORGANIZATION, PROJECT_ID, WEBHOOK],
        &server,
        &credential,
    );
    assert!(!output.status.success());
    assert!(server.finish().is_empty());
    let input = private_credential_directory();
    let file = input.path().join("selection.json");
    for invalid in [
        r#"{"url":"https://receiver.example/h","url":"https://receiver.example/h","eventTypes":["run.failed"]}"#,
        r#"{"url":"https://receiver.example/h","eventTypes":["run.failed","run.failed"]}"#,
        r#"{"url":"https://receiver.example/h","eventTypes":[],"steps":[]}"#,
        r#"{"url":"https://receiver.example/h","eventTypes":["run.failed"],"steps":[{"scope":[],"role":"step","id":"a","extra":1}]}"#,
        r#"{"url":"https://receiver.example/h","eventTypes":["run.failed"],"workflowPaths":["../x"]}"#,
        r#"{"url":"https://receiver.example/h","eventTypes":["run.failed"],"extra":true}"#,
        r#"{"url":"https://@receiver.example/h","eventTypes":["run.failed"]}"#,
        r#"{"url":"https://[::1]/h","eventTypes":["run.failed"]}"#,
        r#"{"url":"https://[2001:4860:4860::8888]/h","eventTypes":["run.failed"]}"#,
        "{\"url\":\" https://receiver.example/h\",\"eventTypes\":[\"run.failed\"]}",
        "{\"url\":\"https://receiver.example/h\\u0001\",\"eventTypes\":[\"run.failed\"]}",
    ] {
        fs::write(&file, invalid).unwrap();
        let server = ScriptedServer::respond(vec![]);
        let (server, _credential_directory, credentials) = prepared_server(server);
        assert!(
            !run(
                &[
                    "create",
                    ORGANIZATION,
                    PROJECT_ID,
                    "--config-file",
                    file.to_str().unwrap()
                ],
                &server,
                &credentials
            )
            .status
            .success()
        );
        assert!(server.finish().is_empty());
    }
    fs::write(&file, vec![b' '; 65_537]).unwrap();
    let server = ScriptedServer::respond(vec![]);
    let (server, _credential_directory, credentials) = prepared_server(server);
    assert!(
        !run(
            &[
                "create",
                ORGANIZATION,
                PROJECT_ID,
                "--config-file",
                file.to_str().unwrap()
            ],
            &server,
            &credentials
        )
        .status
        .success()
    );
    assert!(server.finish().is_empty());
    for source in [input.path(), input.path().join("missing.json").as_path()] {
        let server = ScriptedServer::respond(vec![]);
        let (server, _directory, credentials) = prepared_server(server);
        let output = run(
            &[
                "create",
                ORGANIZATION,
                PROJECT_ID,
                "--config-file",
                source.to_str().unwrap(),
            ],
            &server,
            &credentials,
        );
        assert!(!output.status.success());
        assert!(server.finish().is_empty());
    }
}
