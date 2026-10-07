use super::*;

const TRIGGER: &str = "trg_01k0z6r1w8f4jy2m7q9v3x5abc";
const EVALUATION: &str = "tev_01k0z6r1w8f4jy2m7q9v3x5abc";
const GRANT: &str = "tgr_01k0z6r1w8f4jy2m7q9v3x5abc";
const LINEAR_CONNECTION: &str = "lcn_01k0z6r1w8f4jy2m7q9v3x5abc";

fn evaluation(state: &str, cycle: i32) -> serde_json::Value {
    let mut body = serde_json::json!({
        "id": EVALUATION, "triggerId": TRIGGER, "configurationVersion": 1,
        "connectionId": LINEAR_CONNECTION, "grantId": GRANT,
        "acceptedAt": "2026-09-01T00:00:00Z", "expiresAt": "2026-10-01T00:00:00Z",
        "state": state, "cycleNumber": cycle, "attemptNumber": 0,
        "cycleStartedAt": "2026-09-02T00:00:00Z", "cycleDeadline": "2026-09-03T00:00:00Z",
        "event": {"valid": true}
    });
    if state == "run_created" {
        body["runId"] = serde_json::json!(RUN_ID);
    }
    if state == "failed" {
        body["reasonCode"] = serde_json::json!("mapping_invalid");
    }
    body
}

fn response(status: &str, body: serde_json::Value) -> Vec<u8> {
    let headers = if status == "202 Accepted" {
        vec![(
            "Idempotency-Key",
            api_test_support::REQUEST_IDEMPOTENCY_KEY_ECHO,
        )]
    } else {
        vec![]
    };
    http_response_with_headers(
        status,
        Some("application/json"),
        &headers,
        &serde_json::to_vec(&body).unwrap(),
    )
}

fn run_human_retry(server: &ScriptedServer, key: &str) -> Output {
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
    run_with_env(&args(key), &environment)
}

fn args(key: &str) -> Vec<&str> {
    vec![
        "project",
        "trigger",
        "evaluation",
        "retry",
        ORGANIZATION,
        PROJECT_ID,
        TRIGGER,
        EVALUATION,
        "--idempotency-key",
        key,
        "--json",
        "--allow-insecure-http",
    ]
}

#[test]
fn retry_requires_the_confirmed_request_identity_before_polling() {
    let unconfirmed = http_response(
        "202 Accepted",
        Some("application/json"),
        &serde_json::to_vec(&evaluation("pending", 2)).unwrap(),
    );
    let (server, _directory, credential) = prepared_project(vec![unconfirmed.clone(), unconfirmed]);
    let output = run_project(&args("unconfirmed-cycle"), &server, &credential);
    assert_eq!(output.status.code(), Some(4));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "acceptance_unknown");
    assert_eq!(result["idempotencyKey"], "unconfirmed-cycle");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

#[test]
fn retry_replay_rejection_keeps_unknown_acceptance() {
    let (server, _directory, credential) = prepared_project(vec![
        Vec::new(),
        problem_http_response(
            "403 Forbidden",
            serde_json::json!({"type":"https://api.usefulmachinery.com/problems/forbidden","title":"Forbidden","status":403}),
        ),
    ]);
    let output = run_project(&args("recover-role-loss"), &server, &credential);
    assert_eq!(output.status.code(), Some(4));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "acceptance_unknown");
    assert_eq!(result["code"], "forbidden");
    assert_eq!(result["idempotencyKey"], "recover-role-loss");
    assert_eq!(server.finish().len(), 2);
}

#[test]
fn retry_first_post_refresh_failure_preserves_dispatched_key() {
    let server = ScriptedServer::respond(vec![
        problem_http_response(
            "401 Unauthorized",
            serde_json::json!({"type":"https://api.usefulmachinery.com/problems/unauthorized","title":"Unauthorized","status":401}),
        ),
        http_response("503 Service Unavailable", Some("application/json"), b"{}"),
    ]);
    let output = run_human_retry(&server, "first-post-refresh-fails");
    assert_eq!(output.status.code(), Some(4), "{output:?}");
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "acceptance_unknown");
    assert_eq!(result["idempotencyKey"], "first-post-refresh-fails");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with("POST /api/"));
    assert!(requests[1].starts_with("POST /auth/oauth/token "));
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        "first-post-refresh-fails"
    );
}

#[test]
fn retry_lost_response_and_failed_replay_refresh_preserves_original_key() {
    let server = ScriptedServer::respond(vec![
        Vec::new(),
        problem_http_response(
            "401 Unauthorized",
            serde_json::json!({"type":"https://api.usefulmachinery.com/problems/unauthorized","title":"Unauthorized","status":401}),
        ),
        http_response("503 Service Unavailable", Some("application/json"), b"{}"),
    ]);
    let output = run_human_retry(&server, "lost-then-refresh-fails");
    assert_eq!(output.status.code(), Some(4), "{output:?}");
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "acceptance_unknown");
    assert_eq!(result["idempotencyKey"], "lost-then-refresh-fails");
    assert!(result["acceptedCycle"].is_null());
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("POST /api/"));
    assert!(requests[1].starts_with("POST /api/"));
    assert!(requests[2].starts_with("POST /auth/oauth/token "));
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        "lost-then-refresh-fails"
    );
    assert_eq!(
        header_value(&requests[1], "idempotency-key"),
        "lost-then-refresh-fails"
    );
}

#[test]
fn retry_replays_lost_submission_then_inspects_terminal_state() {
    let key = "same-evaluation-cycle";
    let (server, _directory, credential) = prepared_project(vec![
        Vec::new(),
        response("202 Accepted", evaluation("pending", 2)),
        response("200 OK", evaluation("run_created", 2)),
    ]);
    let output = run_project(&args(key), &server, &credential);
    let result = assert_json_success(&output, "run_created");
    assert_eq!(result["acceptedCycle"], 2);
    assert_eq!(result["evaluation"]["runId"], RUN_ID);
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("POST "));
    assert!(requests[1].starts_with("POST "));
    assert!(requests[2].starts_with("GET "));
    for request in &requests[..2] {
        assert_eq!(header_value(request, "idempotency-key"), key);
        assert!(request.contains("/evaluations/tev_01k0z6r1w8f4jy2m7q9v3x5abc/retry"));
    }
}

#[test]
fn retry_reports_unknown_acceptance_with_replay_key_after_two_lost_responses() {
    let (server, _directory, credential) = prepared_project(vec![Vec::new(), Vec::new()]);
    let output = run_project(&args("recover-this-key"), &server, &credential);
    assert_eq!(output.status.code(), Some(4));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "acceptance_unknown");
    assert_eq!(result["idempotencyKey"], "recover-this-key");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.starts_with("POST ")));
}

#[test]
fn retry_refreshes_each_observation_without_repeating_submission() {
    let rejected = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
            "type":"https://api.usefulmachinery.com/problems/unauthorized", "title":"Unauthorized", "status":401
        }),
    );
    let refresh = |access: &str, renewal: &str| {
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": access, "refresh_token": renewal, "token_type": "Bearer", "expires_in": 3600
            }),
        )
    };
    let server = ScriptedServer::respond(vec![
        response("202 Accepted", evaluation("pending", 2)),
        rejected.clone(),
        refresh("first-observation-token", "first-observation-renewal"),
        response("200 OK", evaluation("pending", 2)),
        rejected,
        refresh("second-observation-token", "second-observation-renewal"),
        response("200 OK", evaluation("run_created", 2)),
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
    let output = run_with_env(&args("refresh-observation"), &environment);
    let result = assert_json_success(&output, "run_created");
    assert_eq!(result["acceptedCycle"], 2);
    let requests = server.finish();
    assert_eq!(requests.len(), 7);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("POST /api/"))
            .count(),
        1
    );
    assert!(requests[2].starts_with("POST /auth/oauth/token "));
    assert!(requests[5].starts_with("POST /auth/oauth/token "));
    assert_eq!(
        header_value(&requests[3], "authorization"),
        "Bearer first-observation-token"
    );
    assert_eq!(
        header_value(&requests[6], "authorization"),
        "Bearer second-observation-token"
    );
    assert_eq!(
        request_form(&requests[5])["refresh_token"],
        "first-observation-renewal"
    );
}

#[test]
fn retry_observation_transport_loss_keeps_accepted_cycle_not_unknown_submission() {
    let (server, _directory, credential) = prepared_project(vec![
        response("202 Accepted", evaluation("pending", 2)),
        Vec::new(),
    ]);
    let output = run_project(&args("observed-key"), &server, &credential);
    assert_eq!(output.status.code(), Some(4));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "observation_stopped");
    assert_eq!(result["acceptedCycle"], 2);
    assert_eq!(result["code"], "unavailable");
    assert_eq!(result["idempotencyKey"], "observed-key");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].starts_with("GET "));
}

#[test]
fn retry_observation_refresh_failure_keeps_confirmed_cycle() {
    let server = ScriptedServer::respond(vec![
        response("202 Accepted", evaluation("pending", 2)),
        problem_http_response(
            "401 Unauthorized",
            serde_json::json!({"type":"https://api.usefulmachinery.com/problems/unauthorized","title":"Unauthorized","status":401}),
        ),
        http_response("503 Service Unavailable", Some("application/json"), b"{}"),
    ]);
    let output = run_human_retry(&server, "accepted-before-refresh-fails");
    assert_eq!(output.status.code(), Some(4), "{output:?}");
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "observation_stopped");
    assert_eq!(result["acceptedCycle"], 2);
    assert_eq!(result["idempotencyKey"], "accepted-before-refresh-fails");
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("POST /api/"));
    assert!(requests[1].starts_with("GET /api/"));
    assert!(requests[2].starts_with("POST /auth/oauth/token "));
}

#[test]
fn retry_key_recovers_across_process_restart() {
    let (server, _directory, credential) = prepared_project(vec![
        Vec::new(),
        Vec::new(),
        response("202 Accepted", evaluation("pending", 2)),
        response("200 OK", evaluation("failed", 2)),
    ]);
    let first = run_project(&args("restart-key"), &server, &credential);
    assert_eq!(first.status.code(), Some(4));
    let second = run_project(&args("restart-key"), &server, &credential);
    assert_eq!(second.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(result["outcome"], "failed");
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(
        requests[..3]
            .iter()
            .all(|request| header_value(request, "idempotency-key") == "restart-key")
    );
}

#[test]
fn retry_signal_after_acceptance_reports_identity_without_cancelling_cycle() {
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![
        response("202 Accepted", evaluation("pending", 2)),
        response("200 OK", evaluation("pending", 2)),
    ]);
    let directory = private_credential_directory();
    let path = directory.path().join("credentials.json");
    write_credential_fixture(&path, &server.api_url, TOKEN, "2999-01-01T00:00:00Z");
    let environment = deployment_environment(&server.api_url, path.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args(args("signal-key"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();
    assert!(server.next_request().starts_with("POST "));
    assert!(server.next_request().starts_with("GET "));
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();
    assert_eq!(output.status.code(), Some(130));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "observation_stopped");
    assert_eq!(result["acceptedCycle"], 2);
    assert_eq!(result["idempotencyKey"], "signal-key");
    assert!(server.finish().is_empty());
}

#[test]
fn retry_timeout_after_acceptance_keeps_cycle_identity() {
    let mut server = ScriptedServer::respond_with_paused_last_response(vec![
        response("202 Accepted", evaluation("pending", 2)),
        response("200 OK", evaluation("pending", 2)),
    ]);
    let directory = private_credential_directory();
    let path = directory.path().join("credentials.json");
    write_credential_fixture(&path, &server.api_url, TOKEN, "2999-01-01T00:00:00Z");
    let environment = deployment_environment(&server.api_url, path.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    let mut arguments = args("timeout-key");
    arguments.extend(["--timeout", "2s"]);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command.spawn().unwrap();
    assert!(server.next_request().starts_with("POST "));
    assert!(server.next_request().starts_with("GET "));
    let output = child.wait_with_output().unwrap();
    server.release_paused_response();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "timed_out");
    assert_eq!(result["acceptedCycle"], 2);
    assert_eq!(result["idempotencyKey"], "timeout-key");
    assert!(server.finish().is_empty());
}

#[test]
fn retry_post_rate_limit_is_definitive_and_does_not_submit_again() {
    let (server, _directory, credential) = prepared_project(vec![http_response_with_headers(
        "429 Too Many Requests", Some("application/problem+json"),
        &[("Retry-After", "2")],
        br#"{"type":"https://api.usefulmachinery.com/problems/rate-limited","title":"Rate limited","status":429}"#,
    )]);
    let output = run_project(&args("limited-key"), &server, &credential);
    assert_eq!(output.status.code(), Some(4));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "error");
    assert_eq!(result["code"], "rate_limited");
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn retry_get_rate_limit_preserves_retry_after_then_observes_terminal_state() {
    let (server, _directory, credential) = prepared_project(vec![
        response("202 Accepted", evaluation("pending", 2)),
        http_response_with_headers("429 Too Many Requests", Some("application/problem+json"),
            &[("Retry-After", "1")], br#"{"type":"https://api.usefulmachinery.com/problems/rate-limited","title":"Rate limited","status":429}"#),
        response("200 OK", evaluation("run_created", 2)),
    ]);
    let output = run_project(&args("limited-get"), &server, &credential);
    assert_eq!(output.status.code(), Some(0));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "run_created");
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[1..]
            .iter()
            .all(|request| request.starts_with("GET "))
    );
}

#[test]
fn retry_role_loss_after_acceptance_never_claims_run_or_resubmits() {
    let (server, _directory, credential) = prepared_project(vec![
        response("202 Accepted", evaluation("pending", 2)),
        problem_http_response(
            "403 Forbidden",
            serde_json::json!({"type":"https://api.usefulmachinery.com/problems/forbidden","title":"Forbidden","status":403}),
        ),
    ]);
    let output = run_project(&args("accepted-before-role-loss"), &server, &credential);
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "observation_stopped");
    assert_eq!(result["acceptedCycle"], 2);
    assert!(result["evaluation"].is_null());
    assert_eq!(result["code"], "forbidden");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with("POST ") && requests[1].starts_with("GET "));
}

#[test]
fn retry_same_state_conflict_does_not_create_an_observation_cycle() {
    let (server, _directory, credential) = prepared_project(vec![problem_http_response(
        "409 Conflict",
        serde_json::json!({"type":"https://api.usefulmachinery.com/problems/source-connection-conflict","title":"Conflict","status":409}),
    )]);
    let output = run_project(&args("racing-owner"), &server, &credential);
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["acceptedCycle"], serde_json::Value::Null);
    assert_eq!(result["code"], "ineligible");
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn retry_never_treats_failed_or_newer_cycle_as_a_run() {
    for (state, cycle, expected) in [
        ("failed", 2, "failed"),
        ("run_created", 3, "observation_stopped"),
    ] {
        let (server, _directory, credential) = prepared_project(vec![
            response("202 Accepted", evaluation("pending", 2)),
            response("200 OK", evaluation(state, cycle)),
        ]);
        let output = run_project(&args("recover-cycle"), &server, &credential);
        assert_eq!(output.status.code(), Some(1));
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], expected);
        assert_eq!(result["acceptedCycle"], 2);
        assert_eq!(server.finish().len(), 2);
    }
}
