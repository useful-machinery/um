use super::*;

const CURRENT_TOKEN: &str = "unique-current-identity-session-token";
const SERVICE_API_KEY: &str =
    "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const CURRENT_IDENTITY_ID: &str = "idn_01k0z6r1w8f4jy2m7q9v3x5abc";
const LINKED_IDENTITY_ID: &str = "idn_01k0z6r1w8f4jy2m7q9v3x5abd";
const AUTH0_ISSUER: &str = "https://auth.usefulmachinery.com/";

// Synthetic stable ID with the subject shape observed twice in the isolated
// UM-2562 Auth0 connection; production qualification is a later binding gate.
fn observed_gitlab_identity() -> serde_json::Value {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/identity/gitlab-com-namespace.json"
    ))
    .unwrap();
    let mut value = identity(
        LINKED_IDENTITY_ID,
        fixture["issuer"].as_str().unwrap(),
        fixture["subject"].as_str().unwrap(),
        false,
    );
    value.as_object_mut().unwrap().remove("assertedEmail");
    value.as_object_mut().unwrap().remove("emailVerified");
    value
}

fn with_provider(mut identity: serde_json::Value, provider: &str) -> serde_json::Value {
    identity["provider"] = provider.into();
    identity
}

fn identity(id: &str, issuer: &str, subject: &str, current: bool) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "kind": "oidc",
        "issuer": issuer,
        "subject": subject,
        "assertedEmail": format!("{subject}@example.test"),
        "emailVerified": true,
        "createdAt": "2026-09-05T12:00:00Z",
        "current": current
    })
}

fn workload_identity(id: &str, issuer: &str, subject: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "kind": "workload_oidc",
        "issuer": issuer,
        "subject": subject,
        "createdAt": "2026-09-05T12:00:00Z",
        "current": false
    })
}

fn identity_page(items: serde_json::Value, next_cursor: Option<&str>) -> Vec<u8> {
    let mut body = serde_json::json!({"items": items});
    if let Some(cursor) = next_cursor {
        body["nextCursor"] = serde_json::Value::String(cursor.to_owned());
    }
    json_http_response("200 OK", body)
}

fn identity_problem(status: &str, code: u16, problem_type: &str) -> Vec<u8> {
    problem_http_response(
        status,
        serde_json::json!({
            "type": problem_type,
            "title": "Identity operation result",
            "status": code
        }),
    )
}

fn link_success(identity: serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        "201 Created",
        Some("application/json"),
        &[
            ("Idempotency-Key", ECHO_IDEMPOTENCY_KEY),
            (
                "Location",
                "/v1/me/identities/idn_01k0z6r1w8f4jy2m7q9v3x5abd",
            ),
        ],
        &serde_json::to_vec(&identity).unwrap(),
    )
}

fn identity_link_flow_responses(mut terminal: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    let mut responses = vec![
        identity_page(
            serde_json::json!([identity(
                CURRENT_IDENTITY_ID,
                "https://issuer.example/",
                "ada-current",
                true
            )]),
            None,
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-private-link-device-code",
                "user_code": "LINK-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "verification_uri_complete": "https://auth.fixture.example/activate?user_code=LINK-CODE",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-proposed-identity-access-token",
                "token_type": "Bearer",
                "expires_in": 300
            }),
        ),
    ];
    responses.append(&mut terminal);
    responses
}

fn prepared_identity_command(
    responses: Vec<Vec<u8>>,
) -> (ScriptedServer, tempfile::TempDir, std::path::PathBuf) {
    let server = ScriptedServer::respond(responses);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        CURRENT_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    (server, credential_directory, credential_path)
}

#[test]
fn list_returns_one_exact_page_with_current_and_provenance_fields() {
    let server = ScriptedServer::respond(vec![identity_page(
        serde_json::json!([
            identity(
                CURRENT_IDENTITY_ID,
                "https://issuer.example/",
                "ada-current",
                true
            ),
            identity(
                LINKED_IDENTITY_ID,
                "https://work.example/",
                "ada-work",
                false
            )
        ]),
        Some("opaque-next-page"),
    )]);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        CURRENT_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "auth",
            "identity",
            "list",
            "--limit",
            "2",
            "--cursor",
            "page-before",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "listed",
            "items": [
                with_provider(identity(CURRENT_IDENTITY_ID, "https://issuer.example/", "ada-current", true), "unknown"),
                with_provider(identity(LINKED_IDENTITY_ID, "https://work.example/", "ada-work", false), "unknown")
            ],
            "nextCursor": "opaque-next-page"
        })
    );
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert!(
        request.starts_with("GET /api/v1/me/identities?limit=2&cursor=page-before HTTP/1.1\r\n")
    );
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {CURRENT_TOKEN}")
    );
}

#[test]
fn list_presents_only_exact_auth0_human_namespaces_without_changing_api_fields() {
    let mut gitlab = observed_gitlab_identity();
    gitlab["id"] = "idn_01k0z6r1w8f4jy2m7q9v3x5abe".into();
    let items = vec![
        identity(CURRENT_IDENTITY_ID, AUTH0_ISSUER, "github|321", true),
        identity(LINKED_IDENTITY_ID, AUTH0_ISSUER, "google-oauth2|987", false),
        gitlab,
        identity(
            "idn_01k0z6r1w8f4jy2m7q9v3x5abf",
            AUTH0_ISSUER,
            "oauth2|other|123",
            false,
        ),
        identity(
            "idn_01k0z6r1w8f4jy2m7q9v3x5abg",
            "https://other.example/",
            "github|321",
            false,
        ),
        identity(
            "idn_01k0z6r1w8f4jy2m7q9v3x5abh",
            AUTH0_ISSUER,
            "github|",
            false,
        ),
        identity(
            "idn_01k0z6r1w8f4jy2m7q9v3x5abj",
            AUTH0_ISSUER,
            "oauth2|um-gitlab-com-signin|um-gitlab-com:abc",
            false,
        ),
    ];
    let expected = items
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, item)| match index {
            0 => with_provider(item, "github"),
            1 => with_provider(item, "google"),
            2 => with_provider(item, "gitlab.com"),
            _ => with_provider(item, "unknown"),
        })
        .collect::<Vec<_>>();

    for json in [true, false] {
        let server = ScriptedServer::respond(vec![identity_page(serde_json::json!(items), None)]);
        let directory = private_credential_directory();
        let path = directory.path().join("credentials.json");
        write_credential_fixture_for_deployment(
            &path,
            &server.api_url,
            &server.issuer,
            CURRENT_TOKEN,
            "2999-01-01T00:00:00Z",
        );
        let environment = deployment_environment_with_issuer(
            &server.api_url,
            &server.issuer,
            path.to_str().unwrap(),
        );
        let mut args = vec!["auth", "identity", "list", "--allow-insecure-http"];
        if json {
            args.push("--json");
        }
        let output = run_with_env(&args, &environment);
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        if json {
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(
                result,
                serde_json::json!({
                    "schemaVersion": 1, "deployment": server.api_url,
                    "outcome": "listed", "items": expected,
                })
            );
        } else {
            let text = String::from_utf8(output.stdout).unwrap();
            for (item, label) in items.iter().zip([
                "GitHub",
                "Google",
                "GitLab.com",
                "Other",
                "Other",
                "Other",
                "Other",
            ]) {
                let block = format!(
                    "identity: {}\nsign-in method: {label}\ncurrent: {}\nissuer: {}\nsubject: {}",
                    item["id"].as_str().unwrap(),
                    if item["current"] == true { "yes" } else { "no" },
                    item["issuer"].as_str().unwrap(),
                    item["subject"].as_str().unwrap(),
                );
                assert!(text.contains(&block), "missing identity block: {block}");
            }
            assert!(text.contains("sign-in method: GitLab.com\ncurrent: no\nissuer: https://auth.usefulmachinery.com/\nsubject: oauth2|um-gitlab-com-signin|um-gitlab-com:123456\nlinked:"));
            assert!(text.contains("asserted email: github|321@example.test"));
            assert!(text.contains("email verified: yes"));
            assert_eq!(text.matches("sign-in method:").count(), items.len());
            assert!(text.contains("identity: idn_"));
        }
        server.finish();
    }
}

#[test]
fn service_list_does_not_label_a_workload_subject() {
    let workload = workload_identity(LINKED_IDENTITY_ID, AUTH0_ISSUER, "github|321");
    for json in [true, false] {
        let server = ScriptedServer::respond(vec![identity_page(
            serde_json::json!([workload.clone()]),
            None,
        )]);
        let directory = private_credential_directory();
        let key = directory.path().join("service.key");
        fs::write(&key, format!("{SERVICE_API_KEY}\n")).unwrap();
        fs::set_permissions(&key, Permissions::from_mode(0o600)).unwrap();
        let missing_store = directory.path().join("no-human-session.json");
        let environment = deployment_environment(&server.api_url, missing_store.to_str().unwrap());
        let mut args = vec![
            "auth",
            "identity",
            "list",
            "--service-api-key-file",
            key.to_str().unwrap(),
            "--allow-insecure-http",
        ];
        if json {
            args.push("--json");
        }
        let output = run_with_env(&args, &environment);
        assert!(output.status.success());
        if json {
            let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["items"], serde_json::json!([workload]));
        } else {
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(
                text.contains("issuer: https://auth.usefulmachinery.com/\nsubject: github|321")
            );
            assert!(!text.contains("sign-in method:"));
        }
        assert!(!missing_store.exists());
        assert_eq!(server.finish().len(), 1);
    }
}

#[test]
fn unknown_identity_remains_removable_by_opaque_id() {
    let unknown = identity(
        LINKED_IDENTITY_ID,
        "https://other.example/",
        "github|321",
        false,
    );
    let server = ScriptedServer::respond(vec![
        identity_page(
            serde_json::json!([
                identity(CURRENT_IDENTITY_ID, AUTH0_ISSUER, "github|321", true),
                unknown.clone(),
            ]),
            None,
        ),
        http_response_with_headers(
            "204 No Content",
            None,
            &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
            &[],
        ),
    ]);
    let directory = private_credential_directory();
    let path = directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &path,
        &server.api_url,
        &server.issuer,
        CURRENT_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, path.to_str().unwrap());
    let listed = run_with_env(
        &[
            "auth",
            "identity",
            "list",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(listed.status.success());
    let value: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(value["items"][1], with_provider(unknown, "unknown"));
    let removed = run_with_env(
        &[
            "auth",
            "identity",
            "remove",
            LINKED_IDENTITY_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(removed.status.success());
    let value: serde_json::Value = serde_json::from_slice(&removed.stdout).unwrap();
    assert_eq!(value["identityId"], LINKED_IDENTITY_ID);
    assert_eq!(value["outcome"], "removed");
    let requests = server.finish();
    assert!(requests[1].starts_with(&format!(
        "DELETE /api/v1/me/identities/{LINKED_IDENTITY_ID} HTTP/1.1"
    )));
}

#[test]
fn successful_link_presents_human_provider_but_not_workload_provider() {
    for (linked, provider, label) in [
        (
            identity(LINKED_IDENTITY_ID, AUTH0_ISSUER, "github|321", false),
            "github",
            "GitHub",
        ),
        (
            identity(LINKED_IDENTITY_ID, AUTH0_ISSUER, "google-oauth2|987", false),
            "google",
            "Google",
        ),
        (observed_gitlab_identity(), "gitlab.com", "GitLab.com"),
        (
            identity(
                LINKED_IDENTITY_ID,
                "https://other.example/",
                "github|321",
                false,
            ),
            "unknown",
            "Other",
        ),
    ] {
        for json in [true, false] {
            let (server, _directory, path) = prepared_identity_command(
                identity_link_flow_responses(vec![link_success(linked.clone())]),
            );
            let environment = deployment_environment_with_issuer(
                &server.api_url,
                &server.issuer,
                path.to_str().unwrap(),
            );
            let mut args = vec!["auth", "identity", "link", "--allow-insecure-http"];
            if json {
                args.push("--json");
            }
            let output = run_with_env(&args, &environment);
            assert!(output.status.success());
            if json {
                assert!(output.stderr.is_empty());
                let events = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(events.len(), 2);
                assert_eq!(
                    events[1],
                    serde_json::json!({
                        "schemaVersion": 1, "event": "result", "deployment": server.api_url,
                        "outcome": "linked", "identity": with_provider(linked.clone(), provider),
                        "localSessionIdentity": "unchanged",
                    })
                );
            } else {
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains("Waiting for authorization")
                );
                let text = String::from_utf8(output.stdout).unwrap();
                assert!(text.contains(&format!(
                    "identity: {}\nsign-in method: {label}\ncurrent: no\nissuer: {}\nsubject: {}",
                    linked["id"].as_str().unwrap(),
                    linked["issuer"].as_str().unwrap(),
                    linked["subject"].as_str().unwrap(),
                )));
                assert!(text.contains("local session identity: unchanged"));
                assert_eq!(
                    text.contains("asserted email:"),
                    linked.get("assertedEmail").is_some()
                );
            }
            assert_eq!(server.finish().len(), 4);
        }
    }
}

#[test]
fn service_link_reports_the_committed_result_when_interrupted_after_dispatch() {
    let linked = workload_identity(
        LINKED_IDENTITY_ID,
        "https://workload.example/",
        "deployment-worker",
    );
    let mut server =
        ScriptedServer::respond_with_paused_last_response(vec![link_success(linked.clone())]);
    let directory = private_credential_directory();
    let service_key_path = directory.path().join("service.key");
    fs::write(&service_key_path, format!("{SERVICE_API_KEY}\n")).unwrap();
    fs::set_permissions(&service_key_path, Permissions::from_mode(0o600)).unwrap();
    let workload_token_path = directory.path().join("workload.token");
    fs::write(&workload_token_path, b"private-workload-token\n").unwrap();
    fs::set_permissions(&workload_token_path, Permissions::from_mode(0o600)).unwrap();
    let missing_human_store = directory.path().join("missing-human.json");
    let environment =
        deployment_environment(&server.api_url, missing_human_store.to_str().unwrap());
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "auth",
            "identity",
            "link",
            "--service-api-key-file",
            service_key_path.to_str().unwrap(),
            "--workload-token-file",
            workload_token_path.to_str().unwrap(),
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
    assert!(request.starts_with("POST /api/v1/me/identities HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {SERVICE_API_KEY}")
    );
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap(),
        rustix::process::Signal::INT,
    )
    .unwrap();
    server.release_paused_response();
    let output = child.wait_with_output().unwrap();

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "event": "result",
            "deployment": server.api_url,
            "outcome": "linked",
            "identity": linked,
            "localSessionIdentity": "unchanged"
        })
    );
    assert!(!missing_human_store.exists());
    assert!(server.finish().is_empty());
}

#[test]
fn link_uses_fresh_separate_browser_proof_and_keeps_the_local_session() {
    let linked = identity(
        LINKED_IDENTITY_ID,
        "https://work.example/",
        "ada-work",
        false,
    );
    let (server, _directory, credential_path) =
        prepared_identity_command(identity_link_flow_responses(vec![
            Vec::new(),
            link_success(linked.clone()),
        ]));
    let before = fs::read(&credential_path).unwrap();
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "auth",
            "identity",
            "link",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let events = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event"], "activation_required");
    assert_eq!(events[0]["operation"], "identity_link");
    assert_eq!(
        events[1],
        serde_json::json!({
            "schemaVersion": 1,
            "event": "result",
            "deployment": server.api_url,
            "outcome": "linked",
            "identity": with_provider(linked, "unknown"),
            "localSessionIdentity": "unchanged"
        })
    );
    assert!(output.stderr.is_empty());
    assert_eq!(fs::read(&credential_path).unwrap(), before);
    let requests = server.finish();
    assert_eq!(requests.len(), 5);
    assert!(requests[0].starts_with("GET /api/v1/me/identities?limit=1 HTTP/1.1\r\n"));
    assert!(requests[1].starts_with("POST /auth/oauth/device/code HTTP/1.1\r\n"));
    assert_eq!(
        request_form(&requests[1]).get("scope").map(String::as_str),
        Some("openid profile email")
    );
    assert!(requests[2].starts_with("POST /auth/oauth/token HTTP/1.1\r\n"));
    assert_eq!(requests[3], requests[4]);
    assert!(requests[3].starts_with("POST /api/v1/me/identities HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&requests[3], "authorization"),
        format!("Bearer {CURRENT_TOKEN}")
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(requests[3].split_once("\r\n\r\n").unwrap().1)
            .unwrap(),
        serde_json::json!({
            "proposedIdentityAccessToken": "unique-proposed-identity-access-token"
        })
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for secret in [
        CURRENT_TOKEN,
        "unique-private-link-device-code",
        "unique-proposed-identity-access-token",
    ] {
        assert!(!combined.contains(secret));
    }
}

#[test]
fn link_keeps_the_preflight_acting_session_across_browser_proof() {
    use std::io::{BufRead as _, BufReader};
    use std::process::Stdio;

    let linked = identity(
        LINKED_IDENTITY_ID,
        "https://work.example/",
        "ada-work",
        false,
    );
    let mut server = ScriptedServer::start(
        identity_link_flow_responses(vec![link_success(linked)]),
        Some(2),
    );
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        CURRENT_TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );
    let empty_path = tempfile::tempdir().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args([
            "auth",
            "identity",
            "link",
            "--json",
            "--allow-insecure-http",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(CREDENTIALS_FILE_VARIABLE)
        .env("PATH", empty_path.path());
    for variable in DEPLOYMENT_VARIABLES
        .into_iter()
        .chain(RUNNER_TELEMETRY_VARIABLES)
    {
        command.env_remove(variable);
    }
    for (name, value) in environment {
        command.env(name, value);
    }
    let mut child = command.spawn().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut activation_line = String::new();
    stdout.read_line(&mut activation_line).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(activation_line.trim()).unwrap()["event"],
        "activation_required"
    );

    assert!(
        server
            .next_request()
            .starts_with("GET /api/v1/me/identities?limit=1 HTTP/1.1\r\n")
    );
    assert!(
        server
            .next_request()
            .starts_with("POST /auth/oauth/device/code HTTP/1.1\r\n")
    );
    assert!(
        server
            .next_request()
            .starts_with("POST /auth/oauth/token HTTP/1.1\r\n")
    );

    fs::remove_file(&credential_path).unwrap();
    write_credential_fixture_for_deployment(
        &credential_path,
        &server.api_url,
        &server.issuer,
        "replacement-acting-session-token",
        "2999-01-01T00:00:00Z",
    );
    server.release_paused_response();

    let status = child.wait().unwrap();
    assert!(status.success());
    let link_request = server.next_request();
    assert!(link_request.starts_with("POST /api/v1/me/identities HTTP/1.1\r\n"));
    assert_eq!(
        header_value(&link_request, "authorization"),
        format!("Bearer {CURRENT_TOKEN}"),
        "the browser proof must not be attached to an account that replaced the acting session while the command was waiting"
    );
}

#[test]
fn link_reports_identity_unavailable_without_exposing_or_replacing_credentials() {
    let conflict = identity_problem(
        "409 Conflict",
        409,
        "https://api.usefulmachinery.com/problems/identity-unavailable",
    );
    let (server, _directory, credential_path) =
        prepared_identity_command(identity_link_flow_responses(vec![conflict]));
    let before = fs::read(&credential_path).unwrap();
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "auth",
            "identity",
            "link",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let events = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["outcome"], "identity_unavailable");
    assert_eq!(events[1]["localSessionIdentity"], "unchanged");
    assert_eq!(fs::read(&credential_path).unwrap(), before);
    assert!(output.stderr.is_empty());
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    let combined = String::from_utf8_lossy(&output.stdout);
    assert!(!combined.contains(CURRENT_TOKEN));
    assert!(!combined.contains("unique-proposed-identity-access-token"));
}

#[test]
fn service_link_reports_workload_policy_and_quantity_failures() {
    let directory = private_credential_directory();
    let service_key_path = directory.path().join("service.key");
    fs::write(&service_key_path, format!("{SERVICE_API_KEY}\n")).unwrap();
    fs::set_permissions(&service_key_path, Permissions::from_mode(0o600)).unwrap();
    let workload_token_path = directory.path().join("workload.token");
    fs::write(&workload_token_path, b"private-workload-token\n").unwrap();
    fs::set_permissions(&workload_token_path, Permissions::from_mode(0o600)).unwrap();
    let missing_human_store = directory.path().join("missing-human.json");

    let cases = [
        (
            "403 Forbidden",
            403,
            "https://api.usefulmachinery.com/problems/workload-identity-linking-not-permitted",
            "workload_identity_linking_not_permitted",
        ),
        (
            "409 Conflict",
            409,
            "https://api.usefulmachinery.com/problems/quantity-limit-reached",
            "quantity_limit_reached",
        ),
    ];
    for (status, code, problem_type, expected_outcome) in cases {
        let server = ScriptedServer::respond(vec![identity_problem(status, code, problem_type)]);
        let environment =
            deployment_environment(&server.api_url, missing_human_store.to_str().unwrap());

        let output = run_with_env(
            &[
                "auth",
                "identity",
                "link",
                "--service-api-key-file",
                service_key_path.to_str().unwrap(),
                "--workload-token-file",
                workload_token_path.to_str().unwrap(),
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
            serde_json::json!({
                "schemaVersion": 1,
                "event": "result",
                "deployment": server.api_url,
                "outcome": expected_outcome,
                "localSessionIdentity": "unchanged"
            })
        );
        assert!(output.stderr.is_empty());
        assert!(!missing_human_store.exists());
        let request = server.finish().pop().unwrap();
        assert!(request.starts_with("POST /api/v1/me/identities HTTP/1.1\r\n"));
        assert_eq!(
            header_value(&request, "authorization"),
            format!("Bearer {SERVICE_API_KEY}")
        );
    }
}

#[test]
fn link_does_not_report_an_unchanged_session_after_rejected_credential_cleanup() {
    let rejected = identity_problem(
        "401 Unauthorized",
        401,
        "https://api.usefulmachinery.com/problems/unauthorized",
    );
    let (server, _directory, credential_path) =
        prepared_identity_command(identity_link_flow_responses(vec![
            rejected,
            json_http_response(
                "400 Bad Request",
                serde_json::json!({"error": "invalid_grant"}),
            ),
        ]));
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let output = run_with_env(
        &[
            "auth",
            "identity",
            "link",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(3));
    let events = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["outcome"], "unauthenticated");
    let stored: serde_json::Value =
        serde_json::from_slice(&fs::read(&credential_path).unwrap()).unwrap();
    assert!(stored["credentials"].as_array().unwrap().is_empty());
    assert_eq!(events[1]["localSessionIdentity"], "removed");
    assert!(output.stderr.is_empty());
    assert_eq!(server.finish().len(), 5);
}

#[test]
fn stored_unclaimed_login_requires_forced_original_sign_in_before_separate_link() {
    let original_principal = serde_json::json!({
        "principal": {
            "id": "prn_01k0z6r1w8f4jy2m7q9v3x5abc", "type": "human", "state": "active"
        }
    });
    let principal_response = json_http_response("200 OK", original_principal);
    let mut responses = vec![
        identity_problem(
            "403 Forbidden",
            403,
            "https://api.usefulmachinery.com/problems/principal-not-provisioned",
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "private-original-method-code",
                "user_code": "ORIGINAL-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600, "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "original-principal-token", "refresh_token": "original-refresh",
                "token_type": "Bearer", "expires_in": 3600
            }),
        ),
        principal_response.clone(),
        principal_response,
    ];
    let linked = observed_gitlab_identity();
    responses.extend(identity_link_flow_responses(vec![link_success(
        linked.clone(),
    )]));
    let (server, _directory, path) = prepared_identity_command(responses);
    let environment =
        deployment_environment_with_issuer(&server.api_url, &server.issuer, path.to_str().unwrap());

    let ordinary = run_with_env(
        &["auth", "login", "--json", "--allow-insecure-http"],
        &environment,
    );
    assert!(ordinary.status.success());
    let events = String::from_utf8(ordinary.stdout).unwrap();
    let status: serde_json::Value = serde_json::from_str(events.trim()).unwrap();
    assert_eq!(status["status"]["state"], "signup_required");
    assert!(!events.contains("activation_required"));

    let forced = run_with_env(
        &[
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(forced.status.success());
    let events = String::from_utf8(forced.stdout).unwrap();
    assert!(events.contains("activation_required"));
    let completion: serde_json::Value =
        serde_json::from_str(events.lines().last().unwrap()).unwrap();
    assert_eq!(completion["status"]["state"], "authenticated");

    let confirmation = run_with_env(
        &["auth", "status", "--json", "--allow-insecure-http"],
        &environment,
    );
    assert!(confirmation.status.success());
    let confirmed: serde_json::Value = serde_json::from_slice(&confirmation.stdout).unwrap();
    assert_eq!(confirmed["state"], "authenticated");
    assert_eq!(
        confirmed["principal"]["id"],
        "prn_01k0z6r1w8f4jy2m7q9v3x5abc"
    );
    let original_session = fs::read(&path).unwrap();

    let link = run_with_env(
        &[
            "auth",
            "identity",
            "link",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(link.status.success());
    let result: serde_json::Value = serde_json::from_str(
        String::from_utf8_lossy(&link.stdout)
            .lines()
            .last()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["outcome"], "linked");
    assert_eq!(result["identity"], with_provider(linked, "gitlab.com"));
    assert_eq!(result["localSessionIdentity"], "unchanged");
    assert_eq!(fs::read(&path).unwrap(), original_session);
    let requests = server.finish();
    assert_eq!(requests.len(), 9);
    assert!(requests[0].starts_with("GET /api/v1/me HTTP/1.1"));
    assert!(requests[1].starts_with("POST /auth/oauth/device/code HTTP/1.1"));
    assert!(requests[4].starts_with("GET /api/v1/me HTTP/1.1"));
    assert!(requests[5].starts_with("GET /api/v1/me/identities?limit=1 HTTP/1.1"));
    assert_eq!(
        header_value(&requests[8], "authorization"),
        "Bearer original-principal-token"
    );
}

#[test]
fn removing_the_former_session_identity_keeps_the_forced_login_session() {
    let removal = http_response_with_headers(
        "204 No Content",
        None,
        &[("Idempotency-Key", ECHO_IDEMPOTENCY_KEY)],
        &[],
    );
    let (server, _directory, credential_path) = prepared_identity_command(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "device_code": "unique-replacement-device-code",
                "user_code": "REPLACE-CODE",
                "verification_uri": "https://auth.fixture.example/activate",
                "expires_in": 600,
                "interval": 1
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "access_token": "unique-replacement-session-token",
                "refresh_token": "unique-replacement-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "principal": {
                    "id": "prn_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "type": "human",
                    "state": "active"
                }
            }),
        ),
        removal,
    ]);
    let environment = deployment_environment_with_issuer(
        &server.api_url,
        &server.issuer,
        credential_path.to_str().unwrap(),
    );

    let login = run_with_env(
        &[
            "auth",
            "login",
            "--force",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(login.status.success());
    let replacement_session = fs::read(&credential_path).unwrap();
    assert!(
        replacement_session
            .windows("unique-replacement-session-token".len())
            .any(|window| window == b"unique-replacement-session-token")
    );

    let removal = run_with_env(
        &[
            "auth",
            "identity",
            "remove",
            CURRENT_IDENTITY_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(removal.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&removal.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "removed",
            "identityId": CURRENT_IDENTITY_ID,
            "localSessionIdentity": "unchanged"
        })
    );
    assert!(removal.stderr.is_empty());
    assert_eq!(fs::read(&credential_path).unwrap(), replacement_session);
    let requests = server.finish();
    assert_eq!(requests.len(), 4);
    assert!(requests[3].starts_with(&format!(
        "DELETE /api/v1/me/identities/{CURRENT_IDENTITY_ID} HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&requests[3], "authorization"),
        "Bearer unique-replacement-session-token"
    );
    assert_eq!(requests[3].split_once("\r\n\r\n").unwrap().1, "");
}

#[test]
fn remove_reports_freshness_and_retention_outcomes() {
    let cases = [
        (
            identity_problem(
                "403 Forbidden",
                403,
                "https://api.usefulmachinery.com/problems/reauthentication-required",
            ),
            "reauthentication_required",
        ),
        (
            identity_problem(
                "409 Conflict",
                409,
                "https://api.usefulmachinery.com/problems/identity-removal-unavailable",
            ),
            "removal_unavailable",
        ),
    ];

    for (response, expected_outcome) in cases {
        let (server, _directory, credential_path) = prepared_identity_command(vec![response]);
        let environment = deployment_environment_with_issuer(
            &server.api_url,
            &server.issuer,
            credential_path.to_str().unwrap(),
        );

        let output = run_with_env(
            &[
                "auth",
                "identity",
                "remove",
                LINKED_IDENTITY_ID,
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
            &environment,
        );

        assert_eq!(output.status.code(), Some(1));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["outcome"], expected_outcome);
        assert!(output.stderr.is_empty());
        server.finish();
    }
}

#[test]
fn service_remove_reports_disabled_workload_identity_linking() {
    let server = ScriptedServer::respond(vec![identity_problem(
        "403 Forbidden",
        403,
        "https://api.usefulmachinery.com/problems/workload-identity-linking-not-permitted",
    )]);
    let directory = private_credential_directory();
    let service_key_path = directory.path().join("service.key");
    fs::write(&service_key_path, format!("{SERVICE_API_KEY}\n")).unwrap();
    fs::set_permissions(&service_key_path, Permissions::from_mode(0o600)).unwrap();
    let missing_human_store = directory.path().join("missing-human.json");
    let environment =
        deployment_environment(&server.api_url, missing_human_store.to_str().unwrap());

    let output = run_with_env(
        &[
            "auth",
            "identity",
            "remove",
            LINKED_IDENTITY_ID,
            "--yes",
            "--service-api-key-file",
            service_key_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "deployment": server.api_url,
            "outcome": "workload_identity_linking_not_permitted"
        })
    );
    assert!(output.stderr.is_empty());
    assert!(!missing_human_store.exists());
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "DELETE /api/v1/me/identities/{LINKED_IDENTITY_ID} HTTP/1.1\r\n"
    )));
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {SERVICE_API_KEY}")
    );
}

#[test]
fn service_link_human_does_not_label_a_workload_subject() {
    let linked = workload_identity(LINKED_IDENTITY_ID, AUTH0_ISSUER, "github|321");
    let server = ScriptedServer::respond(vec![link_success(linked)]);
    let directory = private_credential_directory();
    let key = directory.path().join("service.key");
    let proof = directory.path().join("workload.token");
    fs::write(&key, format!("{SERVICE_API_KEY}\n")).unwrap();
    fs::write(&proof, "workload-proof\n").unwrap();
    fs::set_permissions(&key, Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(&proof, Permissions::from_mode(0o600)).unwrap();
    let missing_store = directory.path().join("no-human-session.json");
    let environment = deployment_environment(&server.api_url, missing_store.to_str().unwrap());
    let output = run_with_env(
        &[
            "auth",
            "identity",
            "link",
            "--service-api-key-file",
            key.to_str().unwrap(),
            "--workload-token-file",
            proof.to_str().unwrap(),
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("identity: idn_"));
    assert!(text.contains("issuer: https://auth.usefulmachinery.com/\nsubject: github|321"));
    assert!(!text.contains("sign-in method:"));
    assert!(!missing_store.exists());
    assert_eq!(server.finish().len(), 1);
}

#[test]
fn service_identity_link_uses_explicit_private_key_and_workload_token_files() {
    const SERVICE_KEY: &str =
        "crd_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const WORKLOAD_TOKEN: &str = "fresh-workload-token-sentinel";
    let linked = workload_identity(
        LINKED_IDENTITY_ID,
        "https://workload.example/",
        "build-agent",
    );
    let server = ScriptedServer::respond(vec![link_success(linked)]);
    let directory = private_credential_directory();
    let service_key_path = directory.path().join("service.key");
    let workload_token_path = directory.path().join("workload.token");
    for (path, value) in [
        (&service_key_path, SERVICE_KEY),
        (&workload_token_path, WORKLOAD_TOKEN),
    ] {
        fs::write(path, format!("{value}\n")).unwrap();
        fs::set_permissions(path, Permissions::from_mode(0o600)).unwrap();
    }
    let human_credentials = directory.path().join("unused-human.json");
    let environment = deployment_environment(&server.api_url, human_credentials.to_str().unwrap());

    let output = run_with_env(
        &[
            "auth",
            "identity",
            "link",
            "--service-api-key-file",
            service_key_path.to_str().unwrap(),
            "--workload-token-file",
            workload_token_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["outcome"], "linked");
    assert_eq!(result["identity"]["id"], LINKED_IDENTITY_ID);
    assert_eq!(result["identity"]["kind"], "workload_oidc");
    assert!(result["identity"].get("assertedEmail").is_none());
    assert!(result["identity"].get("emailVerified").is_none());
    assert!(result["identity"].get("provider").is_none());
    for secret in [SERVICE_KEY, WORKLOAD_TOKEN] {
        assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    }
    let request = server.finish().pop().unwrap();
    assert_eq!(
        header_value(&request, "authorization"),
        format!("Bearer {SERVICE_KEY}")
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request.split_once("\r\n\r\n").unwrap().1)
            .unwrap(),
        serde_json::json!({"proposedIdentityAccessToken": WORKLOAD_TOKEN})
    );
}
