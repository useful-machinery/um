use super::*;

const CREDENTIAL_ID: &str = "crd_01k0z6r1w8f4jy2m7q9v3x5abc";
const API_KEY: &str = "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const CREATED_AT: &str = "2026-01-02T03:04:05Z";

fn service_key_file(directory: &tempfile::TempDir) -> String {
    let path = directory.path().join("service.key");
    fs::write(&path, format!("{API_KEY}\n")).expect("service API-key fixture should be written");
    fs::set_permissions(&path, Permissions::from_mode(0o600))
        .expect("service API-key fixture should be private");
    path.to_string_lossy().into_owned()
}

fn service_environment<'a>(api_url: &'a str, credentials: &'a str) -> [(&'static str, &'a str); 6] {
    deployment_environment(api_url, credentials)
}

fn created_service_response() -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "principal": {
                "id": "prn_service",
                "type": "service",
                "state": "active",
                "displayName": "Build agent"
            },
            "initialCredential": {
                "id": CREDENTIAL_ID,
                "apiKey": API_KEY,
                "createdAt": CREATED_AT
            }
        }))
        .unwrap(),
    )
}

fn issued_credential_response() -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": CREDENTIAL_ID,
            "apiKey": API_KEY,
            "createdAt": CREATED_AT
        }))
        .unwrap(),
    )
}

fn human_credentials_for(server: &ScriptedServer, directory: &tempfile::TempDir) -> String {
    let path = directory.path().join("credentials.json");
    write_credential_fixture(
        &path,
        &server.api_url,
        "human-access-token",
        "2999-01-01T00:00:00Z",
    );
    path.to_string_lossy().into_owned()
}

/// Runs `um` with a zero file-size limit so writing the one-time key to a file fails after the
/// server has committed it, while JSON results on the standard-output pipe still succeed.
fn run_with_failing_key_file(args: &[&str], environment: &[(&str, &str)]) -> Output {
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("ulimit -f 0 && trap '' XFSZ && exec \"$0\" \"$@\"")
        .arg(env!("CARGO_BIN_EXE_um"))
        .args(args)
        .stdin(Stdio::null())
        .env_remove(CREDENTIALS_FILE_VARIABLE);
    for variable in DEPLOYMENT_VARIABLES {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    command.output().expect("um should run")
}

// REQ-SERVICEPRINCIPAL-3
#[test]
fn rejected_display_name_reports_invalid_display_name_and_removes_the_reserved_file() {
    let server = ScriptedServer::respond(vec![problem_http_response(
        "400 Bad Request",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/bad-request",
            "title": "Bad request",
            "status": 400,
            "detail": "display-name-detail-sentinel"
        }),
    )]);
    let directory = private_credential_directory();
    let credentials = human_credentials_for(&server, &directory);
    let destination = directory.path().join("never.key");

    let output = run_with_env(
        &[
            "service-principal",
            "create",
            "--display-name",
            "Build agent",
            "--api-key-file",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &credentials),
    );

    assert_eq!(output.status.code(), Some(1));
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "invalid_display_name");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("display-name-detail-sentinel"));
    assert!(!destination.exists());
    assert_eq!(server.finish().len(), 1);
}

// REQ-SERVICEPRINCIPAL-10
#[test]
fn json_with_standard_output_key_destination_is_refused_before_any_request() {
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    for args in [
        &[
            "service-principal",
            "create",
            "--display-name",
            "Build agent",
        ][..],
        &[
            "service-principal",
            "credential",
            "issue",
            "--service-api-key-file",
            &service_key,
        ][..],
    ] {
        let server = ScriptedServer::respond(Vec::new());
        let credential_directory = private_credential_directory();
        let credentials = human_credentials_for(&server, &credential_directory);
        let mut arguments = args.to_vec();
        arguments.extend(["--api-key-file", "-", "--json", "--allow-insecure-http"]);

        let output = run_with_env(
            &arguments,
            &service_environment(&server.api_url, &credentials),
        );

        assert_eq!(output.status.code(), Some(1), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        assert!(server.finish().is_empty(), "{args:?}");
    }
}

// REQ-SERVICEPRINCIPAL-13
#[test]
fn failed_key_delivery_keeps_the_credential_and_names_the_recovery() {
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    for (args, response, recovery) in [
        (
            &[
                "service-principal",
                "create",
                "--display-name",
                "Build agent",
            ][..],
            created_service_response(),
            "operator_recovery_required",
        ),
        (
            &[
                "service-principal",
                "credential",
                "issue",
                "--service-api-key-file",
                &service_key,
            ][..],
            issued_credential_response(),
            "issue_replacement",
        ),
    ] {
        let server = ScriptedServer::respond(vec![response]);
        let credential_directory = private_credential_directory();
        let credentials = human_credentials_for(&server, &credential_directory);
        let destination = directory.path().join(format!("{recovery}.key"));
        let mut arguments = args.to_vec();
        arguments.extend([
            "--api-key-file",
            destination.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ]);

        let output = run_with_failing_key_file(
            &arguments,
            &service_environment(&server.api_url, &credentials),
        );

        assert_eq!(output.status.code(), Some(1), "{args:?}");
        let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(document["outcome"], "api_key_delivery_failed");
        assert_eq!(document["credential"]["id"], CREDENTIAL_ID);
        assert_eq!(document["recovery"], recovery);
        assert!(!String::from_utf8_lossy(&output.stdout).contains(API_KEY));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(API_KEY));
        assert_eq!(
            server.finish().len(),
            1,
            "the CLI must not send a revocation after failed delivery"
        );
    }
}

// REQ-SERVICEPRINCIPAL-14
#[test]
fn credential_management_requires_an_explicit_service_key() {
    let output = run(&["service-principal", "credential", "list"]);

    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--service-api-key-file <PATH|->"));
}

// REQ-SERVICEPRINCIPAL-1, REQ-SERVICEPRINCIPAL-5, REQ-SERVICEPRINCIPAL-8, REQ-SERVICEPRINCIPAL-29
#[test]
fn service_principal_creation_writes_the_key_only_to_the_protected_destination() {
    let server = ScriptedServer::respond(vec![created_service_response()]);
    let human_credentials = private_credential_directory();
    let credentials_path = human_credentials.path().join("credentials.json");
    write_credential_fixture(
        &credentials_path,
        &server.api_url,
        "human-access-token",
        "2999-01-01T00:00:00Z",
    );
    let credentials = credentials_path.to_string_lossy().into_owned();
    let key_directory = tempfile::tempdir().unwrap();
    let destination = key_directory.path().join("issued.key");
    let destination_text = destination.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "create",
            "--display-name",
            "Build agent",
            "--api-key-file",
            &destination_text,
            "--json",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &credentials),
    );

    assert!(output.status.success());
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "created");
    assert_eq!(document["principal"]["type"], "service");
    assert_eq!(document["initialCredential"]["id"], CREDENTIAL_ID);
    assert_eq!(document["apiKeyFile"], destination_text);
    assert!(!String::from_utf8_lossy(&output.stdout).contains(API_KEY));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(API_KEY));
    assert_eq!(
        fs::read_to_string(&destination).unwrap(),
        format!("{API_KEY}\n")
    );
    assert_eq!(
        fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let request = server.finish().pop().unwrap();
    assert_eq!(
        header_value(&request, "authorization"),
        "Bearer human-access-token"
    );
}

// REQ-SERVICEPRINCIPAL-9, REQ-SERVICEPRINCIPAL-29
#[test]
fn issuance_to_stdout_emits_only_the_api_key_on_stdout() {
    let server = ScriptedServer::respond(vec![issued_credential_response()]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let credentials = directory.path().join("unused-human.json");
    let credentials = credentials.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "issue",
            "--service-api-key-file",
            &service_key,
            "--api-key-file",
            "-",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &credentials),
    );

    assert!(output.status.success());
    assert_eq!(output.stdout, format!("{API_KEY}\n").as_bytes());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(API_KEY));
    server.finish();
}

// REQ-SERVICEPRINCIPAL-11, REQ-SERVICEPRINCIPAL-17
#[test]
fn issuance_delivers_the_committed_key_when_interrupted_after_dispatch() {
    let mut server =
        ScriptedServer::respond_with_paused_last_response(vec![issued_credential_response()]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let destination = directory.path().join("replacement.key");
    let destination_text = destination.to_string_lossy().into_owned();
    let missing_human_credentials = directory.path().join("human.json");
    let environment =
        service_environment(&server.api_url, missing_human_credentials.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "service-principal",
            "credential",
            "issue",
            "--service-api-key-file",
            &service_key,
            "--api-key-file",
            &destination_text,
            "--json",
            "--allow-insecure-http",
        ])
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

    let request = server.next_request();
    assert!(request.starts_with("POST /api/v1/me/credentials HTTP/1.1\r\n"));
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    server.release_paused_response();
    let output = child.wait_with_output().unwrap();

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "issued");
    assert_eq!(document["credential"]["id"], CREDENTIAL_ID);
    assert_eq!(document["apiKeyFile"], destination_text);
    assert!(!String::from_utf8_lossy(&output.stdout).contains(API_KEY));
    assert_eq!(
        fs::read_to_string(destination).unwrap(),
        format!("{API_KEY}\n")
    );
    assert!(server.finish().is_empty());
}

// REQ-SERVICEPRINCIPAL-7
#[test]
fn issuance_replay_reports_unrecoverable_secret_and_removes_the_reserved_file() {
    let response = http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &serde_json::to_vec(&serde_json::json!({
            "id": CREDENTIAL_ID,
            "createdAt": CREATED_AT
        }))
        .unwrap(),
    );
    let server = ScriptedServer::respond(vec![response]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let missing_human_credentials = directory.path().join("human.json");
    let missing_human_credentials = missing_human_credentials.to_string_lossy().into_owned();
    let destination = directory.path().join("replacement.key");
    let destination_text = destination.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "issue",
            "--service-api-key-file",
            &service_key,
            "--api-key-file",
            &destination_text,
            "--json",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &missing_human_credentials),
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "api_key_unavailable",
            "credential": {
                "id": CREDENTIAL_ID,
                "createdAt": CREATED_AT
            }
        })
    );
    assert!(output.stderr.is_empty());
    assert!(!destination.exists());
    let request = server.finish().pop().unwrap();
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {API_KEY}")
    );
}

// REQ-AUTH-49, REQ-AUTH-54
// REQ-SERVICEPRINCIPAL-27
#[test]
fn rejected_service_key_uses_the_authentication_exit_and_structured_outcome() {
    let response = problem_http_response(
        "401 Unauthorized",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/unauthorized",
            "title": "Unauthorized",
            "status": 401
        }),
    );
    let server = ScriptedServer::respond(vec![response]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let credentials = directory.path().join("unused-human.json");
    let credentials = credentials.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "list",
            "--service-api-key-file",
            &service_key,
            "--json",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &credentials),
    );

    assert_eq!(output.status.code(), Some(3));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "unauthenticated");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(API_KEY));
    assert!(output.stderr.is_empty());
    server.finish();
}

// REQ-AUTH-48
// REQ-SERVICEPRINCIPAL-25, REQ-SERVICEPRINCIPAL-29
#[test]
fn service_credential_list_is_non_secret_and_rejects_public_key_files_locally() {
    let response = json_http_response(
        "200 OK",
        serde_json::json!({
            "items": [{
                "id": CREDENTIAL_ID,
                "createdAt": CREATED_AT,
                "current": true
            }]
        }),
    );
    let server = ScriptedServer::respond(vec![response]);
    let directory = private_credential_directory();
    let service_key = service_key_file(&directory);
    let credentials = directory.path().join("human.json");
    let credentials = credentials.to_string_lossy().into_owned();

    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "list",
            "--service-api-key-file",
            &service_key,
            "--json",
            "--allow-insecure-http",
        ],
        &service_environment(&server.api_url, &credentials),
    );
    assert!(output.status.success());
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["items"][0]["current"], true);
    assert!(document["items"][0].get("apiKey").is_none());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(API_KEY));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(API_KEY));
    server.finish();

    fs::set_permissions(&service_key, Permissions::from_mode(0o644)).unwrap();
    let output = run_with_env(
        &[
            "service-principal",
            "credential",
            "list",
            "--service-api-key-file",
            &service_key,
            "--allow-insecure-http",
        ],
        &service_environment("http://127.0.0.1:1", &credentials),
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("file mode must be 0600"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(API_KEY));
}
