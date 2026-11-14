use super::*;

use std::process::Stdio;

const TOKEN: &str = "unique-runner-command-token-sentinel";
const SERVICE_API_KEY: &str =
    "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
pub(super) const ORGANIZATION: &str = "acme-research";
pub(super) const ORGANIZATION_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const POOL_ID: &str = "rpl_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const ACTIVATION_ID: &str = "rna_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const ACTIVATION_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
// rrc_ values are public IDs, not credential secrets.
const CREDENTIAL_ID: &str = "rrc_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const MEMBER_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abc";

pub(super) fn prepared_runner(
    responses: Vec<Vec<u8>>,
) -> (ScriptedServer, tempfile::TempDir, String) {
    let server = ScriptedServer::respond(responses);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let credential_path = credential_path.to_str().unwrap().to_owned();
    (server, credential_directory, credential_path)
}

pub(super) fn request_body(request: &str) -> &str {
    request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("request should contain a body separator")
}

fn pool_body() -> serde_json::Value {
    serde_json::json!({
        "id": POOL_ID,
        "organizationId": ORGANIZATION_ID,
        "name": "builders",
        "createdAt": "2026-08-09T12:00:00Z",
        "updatedAt": "2026-08-09T12:00:00Z"
    })
}

pub(super) fn activation_issuance_body() -> serde_json::Value {
    serde_json::json!({
        "activation": {
            "id": ACTIVATION_ID,
            "runnerId": RUNNER_ID,
            "state": "issued",
            "issuedAt": "2026-08-09T12:00:00Z",
            "expiresAt": "2026-08-09T13:00:00Z"
        },
        "artifact": {
            "schemaVersion": 1,
            "activationUrl": format!("https://api.usefulmachinery.com/v1/runner-enrollments/{ACTIVATION_ID}/activate"),
            "activationToken": format!("{ACTIVATION_ID}.{ACTIVATION_SECRET}"),
            "runnerId": RUNNER_ID,
            "expiresAt": "2026-08-09T13:00:00Z"
        }
    })
}

fn credential_body(state: &str) -> serde_json::Value {
    let mut credential = serde_json::json!({
        "id": CREDENTIAL_ID,
        "storedState": state,
        "effectiveState": state,
        "createdAt": "2026-08-09T12:02:00Z",
        "lastAuthenticatedAt": "2026-08-09T12:04:00Z"
    });
    match state {
        "retiring" => {
            credential["retireAt"] = serde_json::json!("2026-08-09T13:05:00Z");
        }
        "revoked" => {
            credential["revokedAt"] = serde_json::json!("2026-08-09T12:05:00Z");
        }
        _ => {}
    }
    credential
}

pub(super) fn pool_list_body() -> serde_json::Value {
    serde_json::json!({"items": [pool_body()]})
}

fn registration_list_body() -> serde_json::Value {
    serde_json::json!({"items": [registration_body()]})
}

fn deletion_success_response() -> Vec<u8> {
    http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &[],
    )
}

fn registration_body() -> serde_json::Value {
    serde_json::json!({
        "id": RUNNER_ID,
        "organizationId": ORGANIZATION_ID,
        "runnerPool": {"id": POOL_ID, "name": "builders"},
        "name": "builder-one",
        "administration": {
            "mode": "draining",
            "createdAt": "2026-08-09T12:00:00Z",
            "updatedAt": "2026-08-09T12:01:00Z"
        },
        "enrollment": {
            "state": "credentialed",
            "firstEnrolledAt": "2026-08-09T12:02:00Z",
            "validCredentialCount": 1
        },
        "connectivity": {
            "state": "online",
            "connectedAt": "2026-08-09T12:03:00Z",
            "lastSeenAt": "2026-08-09T12:04:00Z"
        },
        "activity": {"state": "assigned", "currentAssignmentCount": 1},
        "advertisedMetadata": {
            "runnerVersion": "1.2.3",
            "protocolVersion": 1
        }
    })
}

fn rate_limit_response() -> Vec<u8> {
    problem_http_response(
        "429 Too Many Requests",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/rate-limit-exceeded",
            "title": "Rate limit exceeded",
            "status": 429
        }),
    )
}

// REQ-RUNNERPOOL-60
#[test]
fn runner_deletion_requires_literal_confirmation_before_local_or_network_access() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let missing_credentials = tempfile::tempdir().unwrap().path().join("missing.json");
    let credential_path = missing_credentials.to_str().unwrap();
    let environment = deployment_environment(&api_url, credential_path);

    for args in [
        vec![
            "runner",
            "delete",
            ORGANIZATION,
            RUNNER_ID,
            "--allow-insecure-http",
        ],
        vec![
            "runner",
            "pool",
            "delete",
            ORGANIZATION,
            POOL_ID,
            "--yes=false",
            "--allow-insecure-http",
        ],
    ] {
        let output = run_with_env(&args, &environment);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(!stderr.contains('?'));
        assert!(!stderr.to_ascii_lowercase().contains("prompt"));
    }
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERPOOL-72
#[test]
fn runner_show_without_a_credential_reports_sign_in_on_stderr() {
    let output = run(&["runner", "show", ORGANIZATION, RUNNER_ID]);

    assert_eq!(output.status.code(), Some(3));
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        concat!(
            "error: runner administration requires sign-in\n\n",
            "Sign in first:\n",
            "  um auth login\n"
        )
    );
}

// REQ-RUNNERPOOL-73
#[test]
fn runner_protocol_failure_uses_the_shared_general_failure_status() {
    let (server, _directory, credential_path) = prepared_runner(vec![json_http_response(
        "200 OK",
        serde_json::json!({"invalidRunnerResponse": true}),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "show",
            ORGANIZATION,
            RUNNER_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "invalid_response"
    );
    assert_eq!(server.finish().len(), 1);
}

// REQ-RUNNERPOOL-46
#[test]
fn pool_create_sends_the_name_and_reports_the_created_pool() {
    let response = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/org_01k0z6r1w8f4jy2m7q9v3x5abc/runner-pools/rpl_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&pool_body()).unwrap(),
    );
    let (server, _directory, credential_path) = prepared_runner(vec![response]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "pool",
            "create",
            ORGANIZATION,
            "--name",
            "builders",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            concat!(
                "✓ Runner pool created.\n\n",
                "  Pool:         {}\n",
                "  Name:         builders\n",
                "  Organization: {}\n",
                "  Deployment:   {}\n"
            ),
            POOL_ID, ORGANIZATION_ID, server.api_url
        )
    );
    assert!(output.stderr.is_empty());

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/runner-pools HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
    assert_eq!(header_value(&request, "content-type"), "application/json");
    let key = header_value(&request, "idempotency-key");
    assert_eq!(key.len(), 64);
    assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({"name": "builders"})
    );
}

// REQ-RUNNERPOOL-47, REQ-RUNNERPOOL-68
#[test]
fn deletion_by_name_resolves_once_and_emits_exact_success_json() {
    for (args, resolution, expected_path, expected_output) in [
        (
            vec![
                "runner",
                "delete",
                ORGANIZATION,
                "builder-one",
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
            json_http_response("200 OK", registration_list_body()),
            format!(
                "DELETE /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID} HTTP/1.1\r\n"
            ),
            ("runnerId", RUNNER_ID),
        ),
        (
            vec![
                "runner",
                "pool",
                "delete",
                ORGANIZATION,
                "builders",
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
            json_http_response("200 OK", pool_list_body()),
            format!(
                "DELETE /api/v1/organizations/{ORGANIZATION}/runner-pools/{POOL_ID} HTTP/1.1\r\n"
            ),
            ("runnerPoolId", POOL_ID),
        ),
    ] {
        let (server, _directory, credential_path) =
            prepared_runner(vec![resolution, deletion_success_response()]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(&args, &environment);

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            format!(
                concat!(
                    "{{\n",
                    "  \"schemaVersion\": 1,\n",
                    "  \"deployment\": \"{}\",\n",
                    "  \"outcome\": \"deleted\",\n",
                    "  \"{}\": \"{}\",\n",
                    "  \"deleted\": true\n",
                    "}}\n"
                ),
                server.api_url, expected_output.0, expected_output.1
            )
        );
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET "));
        assert!(requests[1].starts_with(&expected_path));
        assert_eq!(request_body(&requests[1]), "");
        let key = header_value(&requests[1], "idempotency-key");
        assert_eq!(key.len(), 64);
        assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}

// REQ-RUNNERPOOL-71
#[test]
fn runner_deletion_reads_a_service_key_from_stdin_once() {
    let server = ScriptedServer::respond(vec![
        json_http_response("200 OK", registration_list_body()),
        deletion_success_response(),
    ]);
    let directory = private_credential_directory();
    let missing_human_store = directory.path().join("missing-human.json");
    let environment =
        deployment_environment(&server.api_url, missing_human_store.to_str().unwrap());

    let output = run_with_stdin(
        &[
            "runner",
            "delete",
            ORGANIZATION,
            "builder-one",
            "--yes",
            "--service-api-key-file",
            "-",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
        format!("{SERVICE_API_KEY}\n").as_bytes(),
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["outcome"],
        "deleted"
    );
    assert!(output.stderr.is_empty());
    assert!(!missing_human_store.exists());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(
            header_value(&request, "authorization"),
            format!("Bearer {SERVICE_API_KEY}")
        );
    }
}

#[test]
fn deletion_refresh_reuses_the_resolved_id_and_invocation_key() {
    let rejected = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/unauthorized",
            "title": "Unauthorized",
            "status": 401
        }),
    );
    let server = ScriptedServer::respond(vec![
        json_http_response("200 OK", registration_list_body()),
        rejected,
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-refreshed-delete-access-token",
                "refresh_token": "unique-refreshed-delete-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        deletion_success_response(),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "runner",
            "delete",
            ORGANIZATION,
            "builder-one",
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("GET "))
            .count(),
        1
    );
    let deletes = [&requests[1], &requests[3]];
    for request in deletes {
        assert!(request.starts_with(&format!(
            "DELETE /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID} HTTP/1.1\r\n"
        )));
    }
    assert_eq!(
        header_value(deletes[0], "idempotency-key"),
        header_value(deletes[1], "idempotency-key")
    );
    assert!(deletes[1].contains("authorization: Bearer unique-refreshed-delete-access-token\r\n"));
}

// REQ-RUNNERPOOL-69, REQ-RUNNERPOOL-78
#[test]
fn deletion_blockers_remain_ordered_and_bound_to_the_resolved_id() {
    let blocked = problem_http_response(
        "409 Conflict",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/runner-registration-delete-unavailable",
            "title": "Runner registration deletion unavailable",
            "status": 409,
            "blockers": ["capacity_reserved", "nonterminal_assignment"]
        }),
    );
    let (server, _directory, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", registration_body()),
        blocked,
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "delete",
            ORGANIZATION,
            RUNNER_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "delete_unavailable");
    assert_eq!(result["runnerId"], RUNNER_ID);
    assert_eq!(
        result["blockers"],
        serde_json::json!(["capacity_reserved", "nonterminal_assignment"])
    );
    assert!(result.get("deleted").is_none());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
}

// REQ-RUNNERPOOL-70
#[test]
fn post_dispatch_transport_server_and_protocol_failures_report_unknown_commitment() {
    let failures = [
        Vec::new(),
        http_response("204 No Content", None, &[]),
        problem_http_response(
            "500 Internal Server Error",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/internal-server-error",
                "title": "Internal server error",
                "status": 500
            }),
        ),
    ];
    for failure in failures {
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", registration_body()),
            failure,
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let output = run_with_env(
            &[
                "runner",
                "delete",
                ORGANIZATION,
                RUNNER_ID,
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(4));
        assert!(output.stderr.is_empty());
        let diagnostic: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(
            diagnostic,
            serde_json::json!({
                "schemaVersion": 1,
                "deployment": server.api_url,
                "outcome": "unknown",
                "runnerId": RUNNER_ID,
                "commitment": "unknown"
            })
        );
        assert!(diagnostic.get("deleted").is_none());
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET "));
        assert!(requests[1].starts_with("DELETE "));
    }
}

// REQ-RUNNERPOOL-70
#[test]
fn later_owner_directed_deletion_uses_a_new_key_after_unknown_commitment() {
    let (first, _directory, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", registration_body()),
        Vec::new(),
    ]);
    let environment = deployment_environment(&first.api_url, &credential_path);
    let first_output = run_with_env(
        &[
            "runner",
            "delete",
            ORGANIZATION,
            RUNNER_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(first_output.status.code(), Some(4));
    let first_requests = first.finish();
    let first_key = header_value(&first_requests[1], "idempotency-key").to_owned();

    let (later, _directory, later_credential_path) = prepared_runner(vec![
        json_http_response("200 OK", registration_body()),
        deletion_success_response(),
    ]);
    let later_environment = deployment_environment(&later.api_url, &later_credential_path);
    let later_output = run_with_env(
        &[
            "runner",
            "delete",
            ORGANIZATION,
            RUNNER_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &later_environment,
    );
    assert!(later_output.status.success());
    let later_requests = later.finish();
    let later_key = header_value(&later_requests[1], "idempotency-key");
    assert_ne!(first_key, later_key);
}

// REQ-RUNNERPOOL-70
#[cfg(target_os = "linux")]
#[test]
fn post_dispatch_interrupt_and_termination_report_unknown_without_retry() {
    for (signal, expected_exit) in [
        (rustix::process::Signal::INT, 130),
        (rustix::process::Signal::TERM, 143),
    ] {
        let mut server = ScriptedServer::respond_with_paused_last_response(vec![
            json_http_response("200 OK", registration_body()),
            deletion_success_response(),
        ]);
        let credential_directory = private_credential_directory();
        let credential_path = credential_directory.path().join("credentials.json");
        write_credential_fixture(
            &credential_path,
            &server.api_url,
            TOKEN,
            "2999-01-01T00:00:00Z",
        );
        let environment =
            deployment_environment(&server.api_url, credential_path.to_str().unwrap());
        let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
        command
            .args([
                "runner",
                "delete",
                ORGANIZATION,
                RUNNER_ID,
                "--yes",
                "--json",
                "--allow-insecure-http",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, value) in &environment {
            command.env(name, value);
        }
        let child = command.spawn().unwrap();

        let resolution = server.next_request();
        let deletion = server.next_request();
        assert!(resolution.starts_with("GET "));
        assert!(deletion.starts_with("DELETE "));
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
            signal,
        )
        .unwrap();
        let output = child.wait_with_output().unwrap();
        server.release_paused_response();
        assert_eq!(output.status.code(), Some(expected_exit));
        assert!(output.stderr.is_empty());
        let diagnostic: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(diagnostic["runnerId"], RUNNER_ID);
        assert_eq!(diagnostic["commitment"], "unknown");
        assert!(diagnostic.get("deleted").is_none());
        assert!(server.finish().is_empty());
    }
}

#[test]
fn runner_create_refreshes_a_token_rejected_during_activation() {
    let registration = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runner-registrations/rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&registration_body()).unwrap(),
    );
    let issuance = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runner-registrations/rnr_01k0z6r1w8f4jy2m7q9v3x5abc/activations/rna_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&activation_issuance_body()).unwrap(),
    );
    let rejected = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/unauthorized",
            "title": "Unauthorized",
            "status": 401
        }),
    );
    let server = ScriptedServer::respond(vec![
        json_http_response("200 OK", pool_body()),
        registration.clone(),
        rejected,
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-refreshed-runner-create-access-token",
                "refresh_token": "unique-refreshed-runner-create-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        json_http_response("200 OK", pool_body()),
        registration,
        issuance,
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );
    let activation_directory = private_credential_directory();
    let activation_path = activation_directory.path().join("activation.json");

    let output = run_with_env(
        &[
            "runner",
            "create",
            ORGANIZATION,
            "--pool-id",
            POOL_ID,
            "--activation-file",
            activation_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(
        output.status.success(),
        "runner create should refresh and replay after activation rejects its token: {output:?}"
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 7);
    assert!(requests[3].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert!(
        requests[6]
            .contains("authorization: Bearer unique-refreshed-runner-create-access-token\r\n")
    );
    assert_eq!(
        header_value(&requests[1], "idempotency-key"),
        header_value(&requests[5], "idempotency-key")
    );
    assert_eq!(
        header_value(&requests[2], "idempotency-key"),
        header_value(&requests[6], "idempotency-key")
    );
    assert!(activation_path.exists());
}

// REQ-RUNNERENROLL-8
#[test]
fn runner_create_preserves_registration_when_refresh_replay_fails_early() {
    let registration = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runner-registrations/rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&registration_body()).unwrap(),
    );
    let rejected = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/unauthorized",
            "title": "Unauthorized",
            "status": 401
        }),
    );
    let server = ScriptedServer::respond(vec![
        json_http_response("200 OK", pool_body()),
        registration,
        rejected,
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-refreshed-runner-create-access-token",
                "refresh_token": "unique-refreshed-runner-create-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        rate_limit_response(),
    ]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );
    let activation_directory = private_credential_directory();
    let activation_path = activation_directory.path().join("activation.json");

    let output = run_with_env(
        &[
            "runner",
            "create",
            ORGANIZATION,
            "--pool-id",
            POOL_ID,
            "--activation-file",
            activation_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(4), "{output:?}");
    assert!(output.stderr.is_empty());
    let failure: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(failure["outcome"], "rate_limited");
    assert_eq!(
        failure["runnerId"], RUNNER_ID,
        "the committed registration must survive a failed refresh replay"
    );
    assert!(!activation_path.exists());
    assert_eq!(server.finish().len(), 5);
}

// REQ-RUNNERENROLL-1, REQ-RUNNERENROLL-2, REQ-RUNNERENROLL-5
#[test]
fn runner_creation_commands_stdout_contains_only_the_transferable_artifact() {
    for (arguments, creates_runner) in [
        (
            vec![
                "runner",
                "create",
                ORGANIZATION,
                "--pool-id",
                "builders",
                "--activation-file",
                "-",
                "--allow-insecure-http",
            ],
            true,
        ),
        (
            vec![
                "runner",
                "activation",
                "issue",
                ORGANIZATION,
                RUNNER_ID,
                "--activation-file",
                "-",
                "--allow-insecure-http",
            ],
            false,
        ),
    ] {
        let registration = http_response_with_headers(
            "201 Created",
            Some("application/json"),
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/acme-research/runner-registrations/rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
            ],
            &serde_json::to_vec(&registration_body()).unwrap(),
        );
        let issuance = http_response_with_headers(
            "201 Created",
            Some("application/json"),
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    "/v1/organizations/acme-research/runner-registrations/rnr_01k0z6r1w8f4jy2m7q9v3x5abc/activations/rna_01k0z6r1w8f4jy2m7q9v3x5abc",
                ),
            ],
            &serde_json::to_vec(&activation_issuance_body()).unwrap(),
        );
        let mut responses = vec![json_http_response(
            "200 OK",
            if creates_runner {
                pool_list_body()
            } else {
                registration_body()
            },
        )];
        if creates_runner {
            responses.push(registration);
        }
        responses.push(issuance);
        let (server, _directory, credential_path) = prepared_runner(responses);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(&arguments, &environment);

        assert!(output.status.success());
        let artifact: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(artifact, activation_issuance_body()["artifact"]);
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(!stderr.contains(ACTIVATION_SECRET));

        let requests = server.finish();
        let mut expected_paths = Vec::new();
        if creates_runner {
            assert!(requests[0].starts_with(&format!(
                "GET /api/v1/organizations/{ORGANIZATION}/runner-pools?limit=200 HTTP/1.1\r\n"
            )));
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(request_body(&requests[1])).unwrap()["runnerPoolId"],
                POOL_ID
            );
            expected_paths.push(format!(
                "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations HTTP/1.1\r\n"
            ));
        }
        expected_paths.push(format!(
            "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/activations HTTP/1.1\r\n"
        ));
        assert_eq!(requests.len(), expected_paths.len() + 1);
        for (request, expected_path) in requests.iter().skip(1).zip(expected_paths) {
            assert!(request.starts_with(&expected_path));
        }
    }
}

// REQ-RUNNERENROLL-7
#[test]
fn runner_creation_json_includes_deployment_without_exposing_the_artifact() {
    for creates_runner in [true, false] {
        let registration = http_response_with_headers(
            "201 Created",
            Some("application/json"),
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    &format!("/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}"),
                ),
            ],
            &serde_json::to_vec(&registration_body()).unwrap(),
        );
        let issuance = http_response_with_headers(
            "201 Created",
            Some("application/json"),
            &[
                ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
                (
                    "Location",
                    &format!(
                        "/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/activations/{ACTIVATION_ID}"
                    ),
                ),
            ],
            &serde_json::to_vec(&activation_issuance_body()).unwrap(),
        );
        let mut responses = vec![json_http_response(
            "200 OK",
            if creates_runner {
                pool_list_body()
            } else {
                registration_body()
            },
        )];
        if creates_runner {
            responses.push(registration);
        }
        responses.push(issuance);
        let (server, _credentials, credential_path) = prepared_runner(responses);
        let directory = tempfile::tempdir().unwrap();
        let artifact_path = directory.path().join("activation.json");
        let artifact_path = artifact_path.to_str().unwrap();
        let mut args = if creates_runner {
            vec!["runner", "create", ORGANIZATION, "--pool-id", "builders"]
        } else {
            vec!["runner", "activation", "issue", ORGANIZATION, RUNNER_ID]
        };
        args.extend([
            "--activation-file",
            artifact_path,
            "--json",
            "--allow-insecure-http",
        ]);
        let output = run_with_env(
            &args,
            &deployment_environment(&server.api_url, &credential_path),
        );
        assert!(output.status.success(), "{output:?}");
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["deployment"], server.api_url);
        assert_eq!(
            report["activation"],
            activation_issuance_body()["activation"]
        );
        assert!(report.get("activationToken").is_none());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fs::read(artifact_path).unwrap()).unwrap(),
            activation_issuance_body()["artifact"]
        );
        server.finish();
    }
}

// REQ-RUNNERENROLL-8
#[test]
fn runner_create_reports_activation_failure_with_the_created_registration() {
    let registration = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/organizations/acme-research/runner-registrations/rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            ),
        ],
        &serde_json::to_vec(&registration_body()).unwrap(),
    );
    let (server, _directory, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", pool_body()),
        registration,
        rate_limit_response(),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let activation_directory = private_credential_directory();
    let activation_path = activation_directory
        .path()
        .join("created-runner-activation.json");

    let created = run_with_env(
        &[
            "runner",
            "create",
            ORGANIZATION,
            "--pool-id",
            POOL_ID,
            "--activation-file",
            activation_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(created.status.code(), Some(4));
    assert!(created.stderr.is_empty());
    let created_failure: serde_json::Value = serde_json::from_slice(&created.stdout).unwrap();
    assert_eq!(created_failure["outcome"], "rate_limited");
    assert_eq!(created_failure["runnerId"], RUNNER_ID);
    assert_eq!(created_failure["deployment"], server.api_url);
    assert!(!activation_path.exists());
    assert_eq!(server.finish().len(), 3);

    let (server, _directory, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", registration_body()),
        rate_limit_response(),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let standalone_path = activation_directory
        .path()
        .join("standalone-activation.json");
    let standalone = run_with_env(
        &[
            "runner",
            "activation",
            "issue",
            ORGANIZATION,
            RUNNER_ID,
            "--activation-file",
            standalone_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(standalone.status.code(), created.status.code());
    assert!(standalone.stderr.is_empty());
    let standalone_failure: serde_json::Value = serde_json::from_slice(&standalone.stdout).unwrap();
    assert_eq!(standalone_failure["outcome"], created_failure["outcome"]);
    assert!(standalone_failure.get("runnerId").is_none());
    assert!(!standalone_path.exists());
    assert_eq!(server.finish().len(), 2);
}

/// The API's answer to the read of the signed-in principal, `MEMBER_ID`.
pub(super) fn current_principal_response() -> Vec<u8> {
    json_http_response(
        "200 OK",
        serde_json::json!({
            "principal": {"id": MEMBER_ID, "type": "human", "state": "active"}
        }),
    )
}

pub(super) fn registration_created_response() -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                &format!("/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}"),
            ),
        ],
        &serde_json::to_vec(&registration_body()).unwrap(),
    )
}

fn activation_issued_response() -> Vec<u8> {
    activation_issued_response_with(&activation_issuance_body())
}

pub(super) fn activation_issued_response_with(issuance: &serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                &format!(
                    "/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/activations/{ACTIVATION_ID}"
                ),
            ),
        ],
        &serde_json::to_vec(issuance).unwrap(),
    )
}

// REQ-RUNNERENROLL-1, REQ-RUNNERPOOL-86
#[test]
fn runner_create_by_member_sends_the_member_and_no_pool() {
    let (server, _credentials, credential_path) = prepared_runner(vec![
        registration_created_response(),
        activation_issued_response(),
    ]);
    let directory = tempfile::tempdir().unwrap();
    let artifact_path = directory.path().join("activation.json");

    let output = run_with_env(
        &[
            "runner",
            "create",
            ORGANIZATION,
            "--member",
            MEMBER_ID,
            "--activation-file",
            artifact_path.to_str().unwrap(),
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );

    assert!(output.status.success(), "{output:?}");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations HTTP/1.1\r\n"
    )));
    let body: serde_json::Value = serde_json::from_str(request_body(&requests[0])).unwrap();
    assert_eq!(body["memberId"], MEMBER_ID);
    assert!(body.get("runnerPoolId").is_none(), "{body}");
}

// REQ-RUNNERENROLL-1, REQ-RUNNERPOOL-87
#[test]
fn runner_create_with_neither_pool_nor_member_sends_neither_and_issues_the_activation() {
    let (server, _credentials, credential_path) = prepared_runner(vec![
        registration_created_response(),
        activation_issued_response(),
    ]);
    let directory = tempfile::tempdir().unwrap();
    let artifact_path = directory.path().join("activation.json");

    let output = run_with_env(
        &[
            "runner",
            "create",
            ORGANIZATION,
            "--activation-file",
            artifact_path.to_str().unwrap(),
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );

    assert!(output.status.success(), "{output:?}");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations HTTP/1.1\r\n"
    )));
    let body: serde_json::Value = serde_json::from_str(request_body(&requests[0])).unwrap();
    assert!(body.get("runnerPoolId").is_none(), "{body}");
    assert!(body.get("memberId").is_none(), "{body}");
    assert!(requests[1].starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/activations HTTP/1.1\r\n"
    )));
    let artifact: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&artifact_path).unwrap()).unwrap();
    assert_eq!(artifact["runnerId"], RUNNER_ID);
}

// REQ-RUNNERENROLL-1, REQ-RUNNERPOOL-87
#[test]
fn runner_create_without_a_personal_pool_exits_with_pool_personal_unavailable() {
    for json in [true, false] {
        let refused = problem_http_response(
            "409 Conflict",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/runner-pool-personal-unavailable",
                "title": "Personal runner pool unavailable",
                "status": 409
            }),
        );
        let (server, _credentials, credential_path) = prepared_runner(vec![refused]);
        let directory = tempfile::tempdir().unwrap();
        let artifact_path = directory.path().join("activation.json");
        let mut args = vec![
            "runner",
            "create",
            ORGANIZATION,
            "--activation-file",
            artifact_path.to_str().unwrap(),
        ];
        if json {
            args.push("--json");
        }
        args.push("--allow-insecure-http");

        let output = run_with_env(
            &args,
            &deployment_environment(&server.api_url, &credential_path),
        );

        assert_eq!(output.status.code(), Some(1), "{output:?}");
        if json {
            assert!(output.stderr.is_empty(), "{output:?}");
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "pool_personal_unavailable");
            assert!(result.get("runnerId").is_none(), "{result}");
        } else {
            assert!(output.stdout.is_empty());
            assert!(String::from_utf8_lossy(&output.stderr).contains("--pool-id"));
        }
        assert!(!artifact_path.exists());
        assert_eq!(server.finish().len(), 1);
    }
}

// REQ-RUNNERENROLL-12, REQ-RUNNERPOOL-87
#[test]
fn runner_disable_refused_by_cloud_exits_with_forbidden() {
    let refused = problem_http_response(
        "403 Forbidden",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/forbidden",
            "title": "Forbidden",
            "status": 403
        }),
    );
    let (server, _directory, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", registration_body()),
        refused,
    ]);

    let output = run_with_env(
        &[
            "runner",
            "disable",
            ORGANIZATION,
            RUNNER_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "forbidden");
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].starts_with(&format!(
        "PUT /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/mode HTTP/1.1\r\n"
    )));
}

// REQ-RUNNERENROLL-1
#[test]
fn runner_create_refuses_a_member_with_a_pool_or_a_malformed_member_before_network_access() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let missing_credentials = tempfile::tempdir().unwrap().path().join("missing.json");
    let environment = deployment_environment(&api_url, missing_credentials.to_str().unwrap());

    for target in [
        vec!["--member", MEMBER_ID, "--pool-id", POOL_ID],
        vec!["--member", "prn_malformed"],
        vec!["--member", POOL_ID],
    ] {
        let mut args = vec!["runner", "create", ORGANIZATION];
        args.extend(target);
        args.extend(["--activation-file", "-", "--allow-insecure-http"]);
        let output = run_with_env(&args, &environment);
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty());
    }
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-10, REQ-RUNNERENROLL-11
#[test]
fn activation_list_and_revoke_report_the_deployment() {
    let activation = activation_issuance_body()["activation"].clone();
    for revoke in [false, true] {
        let body = if revoke {
            let mut revoked = activation.clone();
            revoked["state"] = serde_json::json!("revoked");
            revoked
        } else {
            serde_json::json!({"items": [activation]})
        };
        let response = if revoke {
            http_response_with_headers(
                "200 OK",
                Some("application/json"),
                &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
                &serde_json::to_vec(&body).unwrap(),
            )
        } else {
            json_http_response("200 OK", body.clone())
        };
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", registration_body()),
            response,
        ]);
        let mut args = vec![
            "runner",
            "activation",
            if revoke { "revoke" } else { "list" },
            ORGANIZATION,
            RUNNER_ID,
        ];
        if revoke {
            args.extend([ACTIVATION_ID, "--yes"]);
        }
        args.extend(["--json", "--allow-insecure-http"]);
        let output = run_with_env(
            &args,
            &deployment_environment(&server.api_url, &credential_path),
        );
        assert!(output.status.success(), "{output:?}");
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["deployment"], server.api_url);
        if revoke {
            assert_eq!(report["activation"], body);
        } else {
            assert_eq!(report["items"], body["items"]);
        }
        server.finish();
    }
}

// REQ-RUNNERENROLL-52
#[test]
fn credential_list_returns_only_lifecycle_metadata() {
    let credentials = serde_json::json!({
        "items": [credential_body("active")],
        "nextCursor": "next-credentials"
    });
    let (server, _directory, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", registration_body()),
        json_http_response("200 OK", credentials),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "credential",
            "list",
            ORGANIZATION,
            RUNNER_ID,
            "--limit",
            "1",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "listed");
    assert_eq!(result["deployment"], server.api_url);
    assert_eq!(result["items"][0], credential_body("active"));
    let encoded = String::from_utf8(output.stdout).unwrap();
    assert!(!encoded.contains("verifier"));
    assert!(!encoded.contains("secret"));

    let requests = server.finish();
    assert!(requests[1].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/credentials?limit=1 HTTP/1.1\r\n"
    )));
}

// REQ-RUNNERENROLL-53, REQ-RUNNERENROLL-54
#[test]
fn credential_mutations_send_empty_bodies_and_idempotency_keys() {
    for (command, subresource, state) in [
        ("retire", "retirement", "retiring"),
        ("revoke", "revocation", "revoked"),
    ] {
        let response = http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
            &serde_json::to_vec(&credential_body(state)).unwrap(),
        );
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", registration_body()),
            response,
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &[
                "runner",
                "credential",
                command,
                ORGANIZATION,
                RUNNER_ID,
                CREDENTIAL_ID,
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["deployment"], server.api_url);
        assert_eq!(result["credential"], credential_body(state));

        let requests = server.finish();
        assert!(requests[1].starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/credentials/{CREDENTIAL_ID}/{subresource} HTTP/1.1\r\n"
        )));
        assert_eq!(request_body(&requests[1]), "{}");
        let key = header_value(&requests[1], "idempotency-key");
        assert_eq!(key.len(), 64);
        assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}

// REQ-RUNNERENROLL-55, REQ-RUNNERENROLL-83
#[test]
fn refused_credential_transition_reports_its_outcome_and_advises_listing_credentials() {
    for json in [true, false] {
        let refused = problem_http_response(
            "409 Conflict",
            serde_json::json!({
                "type": "https://api.usefulmachinery.com/problems/runner-credential-transition-unavailable",
                "title": "Runner credential transition unavailable",
                "status": 409
            }),
        );
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", registration_body()),
            refused,
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec![
            "runner",
            "credential",
            "retire",
            ORGANIZATION,
            RUNNER_ID,
            CREDENTIAL_ID,
            "--yes",
            "--allow-insecure-http",
        ];
        if json {
            args.push("--json");
        }

        let output = run_with_env(&args, &environment);

        assert_eq!(output.status.code(), Some(1));
        if json {
            assert!(output.stderr.is_empty());
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "credential_transition_unavailable");
        } else {
            assert!(output.stdout.is_empty());
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .to_ascii_lowercase()
                    .contains("list credentials")
            );
        }
        assert_eq!(server.finish().len(), 2);
    }
}

// REQ-RUNNERENROLL-13, REQ-RUNNERENROLL-17, REQ-RUNNERENROLL-19
#[test]
fn enrollment_accepts_an_artifact_from_explicit_stdin() {
    let enrollment = serde_json::json!({
        "schemaVersion": 1,
        "runnerId": RUNNER_ID,
        "runnerName": "builder-one",
        "organization": {
            "id": ORGANIZATION_ID,
            "displayName": "Acme Research"
        },
        "runnerPool": {
            "id": POOL_ID,
            "name": "builders"
        },
        "credentialId": "rrc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "connectionUrl": "ws://127.0.0.1:8765/v1/runner/connect"
    });
    let server = ScriptedServer::respond(vec![json_http_response("201 Created", enrollment)]);
    let directory = tempfile::tempdir().unwrap();
    let state_path = directory.path().join("state/runner-state.json");
    let config_path = directory.path().join("runner.json");
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "deploymentMode": "development",
            "runnerStatePath": state_path,
            "controlSocketPath": directory.path().join("run/runner.sock"),
            "workRoot": directory.path().join("work")
        }))
        .unwrap(),
    )
    .unwrap();
    let activation_url = format!(
        "{}/v1/runner-enrollments/{ACTIVATION_ID}/activate",
        server.api_url.trim_end_matches("/api")
    );
    let artifact = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 1,
        "activationUrl": activation_url,
        "activationToken": format!("{ACTIVATION_ID}.{ACTIVATION_SECRET}"),
        "runnerId": RUNNER_ID,
        "expiresAt": "2999-01-01T00:00:00Z"
    }))
    .unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "runner",
            "enroll",
            "--activation-file",
            "-",
            "--config",
            config_path.to_str().unwrap(),
            "--json",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES
        .into_iter()
        .chain(RUNNER_TELEMETRY_VARIABLES)
    {
        command.env_remove(variable);
    }
    let mut child = command.spawn().unwrap();
    child.stdin.take().unwrap().write_all(&artifact).unwrap();
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "enrolled");
    assert_eq!(
        result["deployment"],
        server.api_url.trim_end_matches("/api")
    );
    let state: serde_json::Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    let secret = state["currentCredential"]["secret"].as_str().unwrap();
    assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "POST /v1/runner-enrollments/{ACTIVATION_ID}/activate HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {ACTIVATION_ID}.{ACTIVATION_SECRET}")
    );
    assert!(!request_body(&request).contains(secret));
}

// REQ-RUNNERENROLL-23
#[test]
fn enrollment_gone_reports_that_the_commit_did_not_complete() {
    let server = ScriptedServer::respond(vec![http_response("410 Gone", None, &[])]);
    let directory = private_credential_directory();
    let state_path = directory.path().join("state/runner-state.json");
    let config_path = directory.path().join("runner.json");
    fs::write(
        &config_path,
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "deploymentMode": "development",
            "runnerStatePath": state_path,
            "controlSocketPath": directory.path().join("run/runner.sock"),
            "workRoot": directory.path().join("work")
        }))
        .unwrap(),
    )
    .unwrap();
    let activation_path = directory.path().join("activation.json");
    let activation_url = format!(
        "{}/v1/runner-enrollments/{ACTIVATION_ID}/activate",
        server.api_url.trim_end_matches("/api")
    );
    fs::write(
        &activation_path,
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "activationUrl": activation_url,
            "activationToken": format!("{ACTIVATION_ID}.{ACTIVATION_SECRET}"),
            "runnerId": RUNNER_ID,
            "expiresAt": "2999-01-01T00:00:00Z"
        }))
        .unwrap(),
    )
    .unwrap();
    fs::set_permissions(&activation_path, Permissions::from_mode(0o600)).unwrap();

    let output = run(&[
        "runner",
        "enroll",
        "--activation-file",
        activation_path.to_str().unwrap(),
        "--config",
        config_path.to_str().unwrap(),
        "--json",
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "outcome": "gone",
            "deployment": server.api_url.trim_end_matches("/api"),
            "activationId": ACTIVATION_ID
        })
    );
    assert_eq!(server.finish().len(), 1);
}

// REQ-RUNNERPOOL-53
#[test]
fn runner_mode_commands_send_closed_targets_with_idempotency() {
    for (command, mode) in [
        ("enable", "enabled"),
        ("drain", "draining"),
        ("disable", "disabled"),
    ] {
        let response = http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
            &serde_json::to_vec(&registration_body()).unwrap(),
        );
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", registration_body()),
            response,
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &[
                "runner",
                command,
                ORGANIZATION,
                RUNNER_ID,
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], mode);
        let requests = server.finish();
        assert!(requests[1].starts_with(&format!(
            "PUT /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/mode HTTP/1.1\r\n"
        )));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(request_body(&requests[1])).unwrap(),
            serde_json::json!({"mode": mode})
        );
        assert_eq!(
            header_value(&requests[1], "content-type"),
            "application/json"
        );
        assert_eq!(header_value(&requests[1], "idempotency-key").len(), 64);
    }
}

// REQ-RUNNERPOOL-47, REQ-RUNNERPOOL-57
#[test]
fn runner_move_resolves_the_destination_and_sends_only_its_id() {
    for pool in [POOL_ID, "builders"] {
        let response = http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
            &serde_json::to_vec(&registration_body()).unwrap(),
        );
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", registration_body()),
            json_http_response(
                "200 OK",
                if pool == POOL_ID {
                    pool_body()
                } else {
                    pool_list_body()
                },
            ),
            response,
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);

        let output = run_with_env(
            &[
                "runner",
                "move",
                ORGANIZATION,
                RUNNER_ID,
                "--pool-id",
                pool,
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["outcome"], "moved");
        let requests = server.finish();
        if pool != POOL_ID {
            assert!(requests[1].starts_with(&format!(
                "GET /api/v1/organizations/{ORGANIZATION}/runner-pools?limit=200 HTTP/1.1\r\n"
            )));
        }
        assert!(requests[2].starts_with(&format!(
        "PUT /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/pool HTTP/1.1\r\n"
    )));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(request_body(&requests[2])).unwrap(),
            serde_json::json!({"runnerPoolId": POOL_ID})
        );
    }
}

fn personal_pool_body() -> serde_json::Value {
    let mut pool = pool_body();
    pool["member"] = serde_json::json!({"id": MEMBER_ID, "displayName": "Ada Lovelace"});
    pool
}

// REQ-RUNNERPOOL-84
#[test]
fn pool_member_commands_are_unknown_and_send_nothing() {
    for leaf in [
        vec!["set", ORGANIZATION, POOL_ID, MEMBER_ID],
        vec!["remove", ORGANIZATION, POOL_ID, "--yes"],
    ] {
        let (server, _directory, credential_path) = prepared_runner(vec![]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec!["runner", "pool", "member"];
        args.extend(leaf);
        args.push("--allow-insecure-http");

        let output = run_with_env(&args, &environment);

        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty());
        assert!(server.finish().is_empty());
    }
}

/// The shared 403 `forbidden` problem whose `reason` names the personal-pool refusal.
fn personal_pool_refusal_response() -> Vec<u8> {
    problem_http_response(
        "403 Forbidden",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/forbidden",
            "title": "Forbidden",
            "status": 403,
            "detail": "A member's personal runner pool cannot be renamed, or deleted while its member belongs to the organization.",
            "reason": "runner-pool-personal"
        }),
    )
}

// REQ-RUNNERPOOL-78, REQ-RUNNERPOOL-84
#[test]
fn personal_pool_rename_refusal_exits_with_pool_personal() {
    for json in [true, false] {
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", personal_pool_body()),
            personal_pool_refusal_response(),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec![
            "runner",
            "pool",
            "rename",
            ORGANIZATION,
            POOL_ID,
            "--name",
            "renamed",
        ];
        if json {
            args.push("--json");
        }
        args.push("--allow-insecure-http");

        let output = run_with_env(&args, &environment);

        assert_eq!(output.status.code(), Some(1));
        if json {
            assert!(output.stderr.is_empty());
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "pool_personal");
        } else {
            assert!(output.stdout.is_empty());
            assert!(String::from_utf8_lossy(&output.stderr).starts_with("error: "));
        }
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with(&format!(
            "PATCH /api/v1/organizations/{ORGANIZATION}/runner-pools/{POOL_ID} HTTP/1.1\r\n"
        )));
    }
}

// REQ-RUNNERPOOL-78, REQ-RUNNERPOOL-84
#[test]
fn personal_pool_deletion_refusal_exits_with_pool_personal() {
    for json in [true, false] {
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", personal_pool_body()),
            personal_pool_refusal_response(),
        ]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut args = vec!["runner", "pool", "delete", ORGANIZATION, POOL_ID, "--yes"];
        if json {
            args.push("--json");
        }
        args.push("--allow-insecure-http");

        let output = run_with_env(&args, &environment);

        assert_eq!(output.status.code(), Some(1));
        if json {
            assert!(output.stderr.is_empty());
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "pool_personal");
            assert_eq!(result["runnerPoolId"], POOL_ID);
            assert!(result.get("commitment").is_none(), "{result}");
        } else {
            assert!(output.stdout.is_empty());
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.starts_with("error: "), "{stderr}");
            assert!(stderr.contains(POOL_ID), "{stderr}");
        }
        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET "));
        assert!(requests[1].starts_with(&format!(
            "DELETE /api/v1/organizations/{ORGANIZATION}/runner-pools/{POOL_ID} HTTP/1.1\r\n"
        )));
    }
}

// REQ-RUNNERPOOL-46, REQ-RUNNERPOOL-84
#[test]
fn pool_show_prints_the_personal_pool_member() {
    let (server, _directory, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", personal_pool_body()),
        pool_status_unavailable_response(),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "pool",
            "show",
            ORGANIZATION,
            POOL_ID,
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            concat!(
                "✓ Runner pool found.\n\n",
                "  Pool:         {}\n",
                "  Name:         builders\n",
                "  Organization: {}\n",
                "  Member:       {} (Ada Lovelace)\n",
                "  Deployment:   {}\n"
            ),
            POOL_ID, ORGANIZATION_ID, MEMBER_ID, server.api_url
        )
    );
    assert_eq!(server.finish().len(), 2);
}

fn pool_status_body() -> serde_json::Value {
    serde_json::json!({
        "runnerPoolId": POOL_ID,
        "queuedRuns": 5,
        "runners": {"total": 7, "online": 4, "offline": 3, "busy": 2, "idle": 1}
    })
}

fn pool_status_unavailable_response() -> Vec<u8> {
    problem_http_response(
        "404 Not Found",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/not-found",
            "title": "Not found",
            "status": 404
        }),
    )
}

fn pool_show(server: &ScriptedServer, credential_path: &str, json: bool) -> std::process::Output {
    let environment = deployment_environment(&server.api_url, credential_path);
    let mut args = vec!["runner", "pool", "show", ORGANIZATION, "builders"];
    if json {
        args.push("--json");
    }
    args.push("--allow-insecure-http");
    run_with_env(&args, &environment)
}

fn assert_pool_then_status_requests(requests: &[String]) {
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(requests[0].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-pools?"
    )));
    assert!(requests[1].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-pools/{POOL_ID}/status HTTP/1.1\r\n"
    )));
}

// REQ-RUNNERPOOL-89, REQ-RUNNERPOOL-47
#[test]
fn pool_show_reads_the_pool_then_its_status_and_reports_its_load() {
    for json in [false, true] {
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", pool_list_body()),
            json_http_response("200 OK", pool_status_body()),
        ]);

        let output = pool_show(&server, &credential_path, json);

        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
        if json {
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "found");
            assert_eq!(result["pool"]["id"], POOL_ID);
            assert_eq!(result["status"], pool_status_body());
        } else {
            let stdout = String::from_utf8(output.stdout).unwrap();
            let lines = stdout.lines().collect::<Vec<_>>();
            let position = |label: &str| {
                lines
                    .iter()
                    .position(|line| line.trim_start().starts_with(label))
                    .unwrap_or_else(|| panic!("{label} should be printed: {stdout}"))
            };
            let identity = position("Organization:");
            let waiting = position("Waiting runs:");
            let runners = position("Runners:");
            assert!(identity < waiting && waiting < runners, "{stdout}");
            assert!(lines[waiting].trim_end().ends_with(" 5"), "{stdout}");
            for count in ["4 online", "2 busy", "1 idle", "3 offline"] {
                assert!(lines[runners].contains(count), "{count}: {stdout}");
            }
        }
        assert_pool_then_status_requests(&server.finish());
    }
}

// REQ-RUNNERPOOL-89
#[test]
fn pool_show_against_a_cloud_without_pool_status_prints_the_pool_alone() {
    for json in [false, true] {
        let (server, _directory, credential_path) = prepared_runner(vec![
            json_http_response("200 OK", pool_list_body()),
            pool_status_unavailable_response(),
        ]);

        let output = pool_show(&server, &credential_path, json);

        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
        if json {
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["outcome"], "found");
            assert_eq!(result["pool"]["id"], POOL_ID);
            assert!(result.get("status").is_none(), "{result}");
        } else {
            let stdout = String::from_utf8(output.stdout).unwrap();
            assert!(stdout.contains(POOL_ID), "{stdout}");
            assert!(!stdout.contains("Waiting runs"), "{stdout}");
            assert!(!stdout.contains("Runners:"), "{stdout}");
        }
        assert_pool_then_status_requests(&server.finish());
    }
}

// REQ-RUNNERPOOL-50, REQ-RUNNERPOOL-52
#[test]
fn runner_commands_report_reserved_run_and_idle_capacity() {
    for assigned in [true, false] {
        let mut runner = registration_body();
        if assigned {
            runner["activity"]["currentAssignment"] = serde_json::json!({
                "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abc",
                "runDisplayName": "Production release",
                "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                "assignedAt": "2026-08-09T12:05:00Z"
            });
        } else {
            runner["activity"] = serde_json::json!({
                "state": "idle", "currentAssignmentCount": 0
            });
        }
        let responses = vec![
            json_http_response("200 OK", runner.clone()),
            json_http_response("200 OK", runner.clone()),
            json_http_response("200 OK", serde_json::json!({"items": [runner.clone()]})),
            json_http_response("200 OK", serde_json::json!({"items": [runner.clone()]})),
        ];
        let (server, _directory, credential_path) = prepared_runner(responses);
        let environment = deployment_environment(&server.api_url, &credential_path);
        for (command, arguments) in [("show", vec![RUNNER_ID]), ("list", vec![])] {
            for json in [false, true] {
                let mut args = vec!["runner", command, ORGANIZATION];
                args.extend(arguments.iter().copied());
                if json {
                    args.push("--json");
                }
                args.push("--allow-insecure-http");
                let output = run_with_env(&args, &environment);
                assert!(
                    output.status.success(),
                    "{command} failed: {:?}",
                    output.stderr
                );
                assert!(output.stderr.is_empty());
                if json {
                    let document: serde_json::Value =
                        serde_json::from_slice(&output.stdout).unwrap();
                    let activity = if command == "show" {
                        &document["runner"]["activity"]
                    } else {
                        &document["items"][0]["activity"]
                    };
                    assert_eq!(
                        activity["currentAssignmentCount"],
                        if assigned { 1 } else { 0 }
                    );
                    if assigned {
                        assert_eq!(
                            activity["currentAssignment"],
                            runner["activity"]["currentAssignment"]
                        );
                    } else {
                        assert!(activity.get("currentAssignment").is_none());
                    }
                } else {
                    let text = String::from_utf8(output.stdout).unwrap();
                    assert!(text.contains(if assigned { "assigned" } else { "idle" }));
                    for field in [
                        "run_01k0z6r1w8f4jy2m7q9v3x5abc",
                        "Production release",
                        "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                        "2026-08-09T12:05:00Z",
                    ] {
                        assert_eq!(text.contains(field), assigned, "{command} {field}: {text}");
                    }
                }
            }
        }
        assert_eq!(server.finish().len(), 4);
    }
}

// REQ-RUNNERPOOL-51
#[test]
fn runner_show_reports_independent_cloud_and_informational_projections() {
    let (server, _directory, credential_path) =
        prepared_runner(vec![json_http_response("200 OK", registration_body())]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "show",
            ORGANIZATION,
            RUNNER_ID,
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    for expected in [
        "draining",
        "credentialed",
        "online",
        "assigned",
        "1.2.3",
        &server.api_url,
    ] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in {stdout:?}"
        );
    }
    assert!(output.stderr.is_empty());

    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID} HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {TOKEN}")
    );
}

const JOIN_KEY_ID: &str = "rjk_01k0z6r1w8f4jy2m7q9v3x5abc";
const JOIN_KEY_SECRET: &str = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
const SHARED_POOL_ID: &str = "rpl_01k0z6r1w8f4jy2m7q9v3x5abd";
const OTHER_MEMBER_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abd";

fn join_key_body(pool_id: &str, state: &str) -> serde_json::Value {
    let mut key = serde_json::json!({
        "id": JOIN_KEY_ID,
        "runnerPoolId": pool_id,
        "issuedByPrincipalId": MEMBER_ID,
        "state": state,
        "issuedAt": "2026-11-09T12:00:00Z",
        "expiresAt": "2026-11-16T12:00:00Z",
        "offlineRemovalSeconds": 3600
    });
    if state == "revoked" {
        key["revokedAt"] = serde_json::json!("2026-11-10T12:00:00Z");
    }
    key
}

fn join_key_issued_response(pool_id: &str) -> Vec<u8> {
    let body = serde_json::json!({
        "joinKey": join_key_body(pool_id, "issued"),
        "artifact": join_key_artifact()
    });
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                &format!("/v1/organizations/{ORGANIZATION}/runner-join-keys/{JOIN_KEY_ID}"),
            ),
        ],
        &serde_json::to_vec(&body).unwrap(),
    )
}

fn join_key_artifact() -> serde_json::Value {
    serde_json::json!({
        "schemaVersion": 1,
        "joinUrl": format!("https://api.usefulmachinery.com/v1/runner-join-keys/{JOIN_KEY_ID}/join"),
        "joinKeyId": JOIN_KEY_ID,
        "joinToken": format!("{JOIN_KEY_ID}.{JOIN_KEY_SECRET}"),
        "expiresAt": "2026-11-16T12:00:00Z"
    })
}

fn shared_pool_body() -> serde_json::Value {
    let mut pool = pool_body();
    pool["id"] = serde_json::json!(SHARED_POOL_ID);
    pool["name"] = serde_json::json!("ci");
    pool
}

fn assert_join_key_issuance_request(request: &str, pool_id: &str, expires: u64, removal: u64) {
    assert!(
        request.starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/runner-pools/{pool_id}/join-keys HTTP/1.1\r\n"
        )),
        "{request}"
    );
    assert_eq!(header_value(request, "idempotency-key").len(), 64);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(request)).unwrap(),
        serde_json::json!({"expiresInSeconds": expires, "offlineRemovalSeconds": removal})
    );
}

// REQ-RUNNERENROLL-93
#[test]
fn join_key_issue_resolves_the_pool_sends_seconds_and_writes_a_private_artifact() {
    let (server, _credentials, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", serde_json::json!({"items": [shared_pool_body()]})),
        join_key_issued_response(SHARED_POOL_ID),
    ]);
    let directory = tempfile::tempdir().unwrap();
    let artifact_path = directory.path().join("join-key.json");
    let artifact_path = artifact_path.to_str().unwrap();

    let output = run_with_env(
        &[
            "runner",
            "join-key",
            "issue",
            ORGANIZATION,
            "--pool-id",
            "ci",
            "--expires-in",
            "7d",
            "--remove-after",
            "1h",
            "--join-key-file",
            artifact_path,
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(JOIN_KEY_ID), "{stdout}");
    assert!(!stdout.contains(JOIN_KEY_SECRET));
    assert!(
        !String::from_utf8(output.stderr)
            .unwrap()
            .contains(JOIN_KEY_SECRET)
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-pools?limit=200 HTTP/1.1\r\n"
    )));
    assert_join_key_issuance_request(&requests[1], SHARED_POOL_ID, 604_800, 3_600);

    let metadata = fs::metadata(artifact_path).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    let written = fs::read_to_string(artifact_path).unwrap();
    assert_eq!(written.lines().count(), 1, "{written}");
    assert!(written.ends_with('\n'));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&written).unwrap(),
        join_key_artifact()
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let repeated = run_with_env(
        &[
            "runner",
            "join-key",
            "issue",
            ORGANIZATION,
            "--pool-id",
            "ci",
            "--expires-in",
            "7d",
            "--remove-after",
            "1h",
            "--join-key-file",
            artifact_path,
            "--allow-insecure-http",
        ],
        &deployment_environment(&api_url, &credential_path),
    );
    assert!(!repeated.status.success());
    assert_eq!(fs::read_to_string(artifact_path).unwrap(), written);
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-93
#[test]
fn join_key_issue_without_a_pool_uses_the_callers_personal_pool_and_writes_only_the_artifact() {
    let mut other_personal = personal_pool_body();
    other_personal["id"] = serde_json::json!("rpl_01k0z6r1w8f4jy2m7q9v3x5abe");
    other_personal["member"]["id"] = serde_json::json!(OTHER_MEMBER_ID);
    let (server, _credentials, credential_path) = prepared_runner(vec![
        current_principal_response(),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "items": [shared_pool_body(), other_personal, personal_pool_body()]
            }),
        ),
        join_key_issued_response(POOL_ID),
    ]);

    let output = run_with_env(
        &[
            "runner",
            "join-key",
            "issue",
            ORGANIZATION,
            "--expires-in",
            "30m",
            "--remove-after",
            "5m",
            "--join-key-file",
            "-",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.lines().count(), 1, "{stdout}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stdout).unwrap(),
        join_key_artifact()
    );
    assert!(
        !String::from_utf8(output.stderr)
            .unwrap()
            .contains(JOIN_KEY_SECRET)
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("GET /api/v1/me HTTP/1.1\r\n"));
    assert!(requests[1].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-pools?limit=200 HTTP/1.1\r\n"
    )));
    assert_join_key_issuance_request(&requests[2], POOL_ID, 1_800, 300);
}

// REQ-RUNNERENROLL-93
#[test]
fn join_key_issue_without_a_file_writes_only_the_artifact_to_stdout() {
    let (server, _credentials, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", shared_pool_body()),
        join_key_issued_response(SHARED_POOL_ID),
    ]);
    let working_directory = tempfile::tempdir().unwrap();
    let args = [
        "runner",
        "join-key",
        "issue",
        ORGANIZATION,
        "--pool-id",
        SHARED_POOL_ID,
        "--expires-in",
        "1h",
        "--remove-after",
        "7d",
        "--allow-insecure-http",
    ];

    let output = run_with_env_in(
        &args,
        &deployment_environment(&server.api_url, &credential_path),
        working_directory.path(),
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.lines().count(), 1, "{stdout}");
    assert!(stdout.ends_with('\n'));
    let artifact: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(artifact, join_key_artifact());
    assert_eq!(
        artifact["joinToken"],
        format!("{JOIN_KEY_ID}.{JOIN_KEY_SECRET}")
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(JOIN_KEY_ID), "{stderr}");
    assert!(!stderr.contains(JOIN_KEY_SECRET), "{stderr}");
    assert_eq!(
        fs::read_dir(working_directory.path()).unwrap().count(),
        0,
        "no file is created when none is named"
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_join_key_issuance_request(&requests[1], SHARED_POOL_ID, 3_600, 604_800);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let mut json_args = args.to_vec();
    json_args.push("--json");
    let refused = run_with_env_in(
        &json_args,
        &deployment_environment(&api_url, &credential_path),
        working_directory.path(),
    );
    assert!(!refused.status.success(), "{refused:?}");
    assert!(refused.stdout.is_empty(), "{refused:?}");
    assert_eq!(fs::read_dir(working_directory.path()).unwrap().count(), 0);
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-93
#[test]
fn join_key_issue_without_a_personal_pool_sends_no_issuance() {
    let (server, _credentials, credential_path) = prepared_runner(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "principal": {"id": MEMBER_ID, "type": "service", "state": "active"}
            }),
        ),
        json_http_response("200 OK", serde_json::json!({"items": [shared_pool_body()]})),
    ]);
    let directory = tempfile::tempdir().unwrap();
    let artifact_path = directory.path().join("join-key.json");

    let output = run_with_env(
        &[
            "runner",
            "join-key",
            "issue",
            ORGANIZATION,
            "--expires-in",
            "1d",
            "--remove-after",
            "1h",
            "--join-key-file",
            artifact_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "pool_personal_unavailable");
    assert!(!artifact_path.exists());
    assert_eq!(server.finish().len(), 2);
}

// REQ-RUNNERENROLL-93
#[test]
fn join_key_issue_json_reports_the_key_without_its_token() {
    let (server, _credentials, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", shared_pool_body()),
        join_key_issued_response(SHARED_POOL_ID),
    ]);
    let directory = tempfile::tempdir().unwrap();
    let artifact_path = directory.path().join("join-key.json");
    let artifact_path = artifact_path.to_str().unwrap();

    let output = run_with_env(
        &[
            "runner",
            "join-key",
            "issue",
            ORGANIZATION,
            "--pool-id",
            SHARED_POOL_ID,
            "--expires-in",
            "365d",
            "--remove-after",
            "30d",
            "--join-key-file",
            artifact_path,
            "--json",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains(JOIN_KEY_SECRET), "{stdout}");
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["outcome"], "issued");
    assert_eq!(report["deployment"], server.api_url);
    assert_eq!(report["joinKey"], join_key_body(SHARED_POOL_ID, "issued"));
    assert_eq!(report["joinKeyFile"], artifact_path);
    let requests = server.finish();
    assert_join_key_issuance_request(&requests[1], SHARED_POOL_ID, 31_536_000, 2_592_000);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    for (expires, removal, destination, json) in [
        ("7d", "1h", "-", true),
        ("366d", "1h", "-", false),
        ("59s", "1h", "-", false),
        ("7d", "4m", "-", false),
        ("7d", "31d", "-", false),
        ("7w", "1h", "-", false),
        ("-1d", "1h", "-", false),
    ] {
        let mut args = vec![
            "runner",
            "join-key",
            "issue",
            ORGANIZATION,
            "--pool-id",
            SHARED_POOL_ID,
            "--expires-in",
            expires,
            "--remove-after",
            removal,
            "--join-key-file",
            destination,
        ];
        if json {
            args.push("--json");
        }
        args.push("--allow-insecure-http");
        let refused = run_with_env(&args, &deployment_environment(&api_url, &credential_path));
        assert!(!refused.status.success(), "{args:?}");
        assert!(refused.stdout.is_empty(), "{args:?}");
    }
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-94
#[test]
fn join_key_list_prints_each_keys_pool_state_expiry_and_removal_time() {
    let page = serde_json::json!({
        "items": [join_key_body(POOL_ID, "issued"), join_key_body(SHARED_POOL_ID, "revoked")],
        "nextCursor": "next-join-keys"
    });
    let (server, _credentials, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", page.clone()),
        json_http_response("200 OK", shared_pool_body()),
        json_http_response("200 OK", page.clone()),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "runner",
            "join-key",
            "list",
            ORGANIZATION,
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let rows: Vec<_> = stdout
        .lines()
        .filter(|line| line.contains(JOIN_KEY_ID))
        .collect();
    assert_eq!(rows.len(), 2, "{stdout}");
    for (row, pool, state) in [
        (rows[0], POOL_ID, "issued"),
        (rows[1], SHARED_POOL_ID, "revoked"),
    ] {
        for field in [pool, MEMBER_ID, state, "2026-11-16T12:00:00Z", "1h"] {
            assert!(row.contains(field), "missing {field:?} in {row:?}");
        }
    }
    assert!(stdout.contains("next-join-keys"));

    let filtered = run_with_env(
        &[
            "runner",
            "join-key",
            "list",
            ORGANIZATION,
            "--pool-id",
            SHARED_POOL_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(filtered.status.success(), "{filtered:?}");
    let report: serde_json::Value = serde_json::from_slice(&filtered.stdout).unwrap();
    assert_eq!(report["outcome"], "listed");
    assert_eq!(report["items"], page["items"]);
    assert_eq!(report["nextCursor"], "next-join-keys");

    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-join-keys HTTP/1.1\r\n"
    )));
    assert!(requests[2].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-join-keys?runnerPoolId={SHARED_POOL_ID} HTTP/1.1\r\n"
    )));
}

// REQ-RUNNERENROLL-94
#[test]
fn join_key_revoke_requires_confirmation_and_sends_the_revocation() {
    let (server, _credentials, credential_path) = prepared_runner(vec![]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let unconfirmed = run_with_env(
        &[
            "runner",
            "join-key",
            "revoke",
            ORGANIZATION,
            JOIN_KEY_ID,
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert_eq!(unconfirmed.status.code(), Some(2));
    assert!(unconfirmed.stdout.is_empty());
    assert!(server.finish().is_empty());

    let revoked = join_key_body(POOL_ID, "revoked");
    let (server, _credentials, credential_path) =
        prepared_runner(vec![http_response_with_headers(
            "200 OK",
            Some("application/json"),
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
            &serde_json::to_vec(&revoked).unwrap(),
        )]);
    let output = run_with_env(
        &[
            "runner",
            "join-key",
            "revoke",
            ORGANIZATION,
            JOIN_KEY_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &deployment_environment(&server.api_url, &credential_path),
    );
    assert!(output.status.success(), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "revoked");
    assert_eq!(report["joinKey"], revoked);
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "POST /api/v1/organizations/{ORGANIZATION}/runner-join-keys/{JOIN_KEY_ID}/revocation HTTP/1.1\r\n"
    )));
    assert_eq!(header_value(&request, "idempotency-key").len(), 64);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({})
    );
}

// REQ-RUNNERPOOL-88
#[test]
fn runner_show_prints_the_join_key_of_a_joined_runner() {
    let mut joined = registration_body();
    joined["joinKey"] = serde_json::json!({"id": JOIN_KEY_ID});
    let (server, _credentials, credential_path) = prepared_runner(vec![
        json_http_response("200 OK", joined),
        json_http_response("200 OK", registration_body()),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let args = [
        "runner",
        "show",
        ORGANIZATION,
        RUNNER_ID,
        "--allow-insecure-http",
    ];

    let shown = run_with_env(&args, &environment);
    let by_hand = run_with_env(&args, &environment);

    assert!(shown.status.success(), "{shown:?}");
    assert!(
        String::from_utf8(shown.stdout)
            .unwrap()
            .lines()
            .any(|line| line.contains("Join key") && line.contains(JOIN_KEY_ID))
    );
    assert!(by_hand.status.success(), "{by_hand:?}");
    assert!(
        !String::from_utf8(by_hand.stdout)
            .unwrap()
            .contains("Join key")
    );
    assert_eq!(server.finish().len(), 2);
}
