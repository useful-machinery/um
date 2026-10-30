use std::fs::{OpenOptions, Permissions};
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use super::*;

const TOKEN: &str = "unique-invitation-command-token-sentinel";
const ORG_ID: &str = "org_01k0z6r1w8f4jy2m7q9v3x5abc";
const PRINCIPAL_ID: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abc";
const INVITATION_ID: &str = "inv_01k0z6r1w8f4jy2m7q9v3x5abc";
const MEMBERSHIP_ID: &str = "mem_01k0z6r1w8f4jy2m7q9v3x5abc";
const CAPABILITY: &str =
    "inv_01k0z6r1w8f4jy2m7q9v3x5abc.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn invitation(state: &str, terminal_at: Option<&str>) -> serde_json::Value {
    let mut value = serde_json::json!({
        "id": INVITATION_ID,
        "organizationId": ORG_ID,
        "issuerPrincipalId": PRINCIPAL_ID,
        "targetKind": "principal",
        "state": state,
        "issuedAt": "2026-01-02T03:04:05Z",
        "expiresAt": "2026-01-09T03:04:05Z"
    });
    if state == "outstanding" {
        value["targetPrincipalId"] = PRINCIPAL_ID.into();
    }
    if let Some(terminal_at) = terminal_at {
        value["terminalAt"] = terminal_at.into();
    }
    value
}

fn prepared(
    responses: Vec<Vec<u8>>,
) -> (
    ScriptedServer,
    tempfile::TempDir,
    std::path::PathBuf,
    String,
) {
    let server = ScriptedServer::respond(responses);
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &server.api_url,
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let credential_path_string = credential_path.to_str().unwrap().to_owned();
    (
        server,
        credential_directory,
        credential_path,
        credential_path_string,
    )
}

fn write_capability(path: &Path, contents: &str, mode: u32) {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)
        .expect("create capability file");
    // Creation modes are masked by the process umask; fixtures need the exact mode.
    file.set_permissions(Permissions::from_mode(mode))
        .expect("set capability file permissions");
    writeln!(file, "{contents}").expect("write capability file");
}

fn json_response_with_headers(
    status: &str,
    value: &serde_json::Value,
    headers: &[(&str, &str)],
) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/json"),
        headers,
        &serde_json::to_vec(value).unwrap(),
    )
}

fn problem_response_with_headers(
    status: &str,
    problem_type: &str,
    status_code: u16,
    headers: &[(&str, &str)],
) -> Vec<u8> {
    http_response_with_headers(
        status,
        Some("application/problem+json"),
        headers,
        &serde_json::to_vec(&serde_json::json!({
            "type": problem_type,
            "title": "Invitation request failed",
            "status": status_code
        }))
        .unwrap(),
    )
}

fn request_body(request: &str) -> &str {
    request.split_once("\r\n\r\n").unwrap().1
}

fn assert_authenticated(request: &str) {
    assert_eq!(
        header_value(request, "authorization"),
        format!("Bearer {TOKEN}")
    );
}

// REQ-INVITE-57
#[test]
fn organization_invitation_issue_sends_target_and_idempotency_key() {
    let location = format!("/v1/invitations/{INVITATION_ID}");
    let (server, _directory, _path, credential_path) = prepared(vec![json_response_with_headers(
        "201 Created",
        &invitation("outstanding", None),
        &[
            ("idempotency-key", ECHO_IDEMPOTENCY_KEY),
            ("location", &location),
        ],
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "invitation",
            "issue",
            ORG_ID,
            "--principal-id",
            PRINCIPAL_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"outcome\": \"issued\""));
    assert!(stdout.contains(INVITATION_ID));
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "POST /api/v1/organizations/{ORG_ID}/invitations HTTP/1.1\r\n"
    )));
    assert_authenticated(&request);
    assert!(!header_value(&request, "idempotency-key").is_empty());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({ "kind": "principal", "principalId": PRINCIPAL_ID })
    );
}

// REQ-INVITE-62, REQ-INVITE-63, REQ-INVITE-64, REQ-INVITE-81, REQ-INVITE-87
#[test]
fn organization_invitation_list_preserves_terminal_history_and_cursor() {
    let mut accepted = invitation("accepted", Some("2026-01-03T03:04:05Z"));
    accepted["targetKind"] = "email".into();
    accepted["targetEmail"] = "delivered@example.test".into();
    accepted["acceptingPrincipalId"] = "prn_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    accepted["acceptedMembershipId"] = "mem_01k0z6r1w8f4jy2m7q9v3x5abc".into();
    accepted["issuerIdentity"] = serde_json::json!({"principalId": PRINCIPAL_ID, "nameState": "snapshot", "displayName": "At issue", "linkedEmails": [{"email": "issuer@example.test", "verified": false}]});
    let accepting_identity = serde_json::json!({"principalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abd", "nameState": "live", "displayName": "Now", "linkedEmails": [{"email": "accepter@example.test", "verified": true}]});
    accepted["acceptingIdentity"] = accepting_identity.clone();
    let mut revoked = invitation("revoked", Some("2026-01-04T03:04:05Z"));
    revoked["id"] = "inv_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    revoked["targetKind"] = "email".into();
    revoked["targetEmail"] = "not-joined@example.test".into();
    let mut expired = invitation("expired", Some("2026-01-09T03:04:05Z"));
    expired["id"] = "inv_01k0z6r1w8f4jy2m7q9v3x5abe".into();
    expired["targetKind"] = "email".into(); // Older erased offer remains unavailable.
    let mut principal_target = invitation("revoked", Some("2026-01-05T03:04:05Z"));
    principal_target["id"] = "inv_01k0z6r1w8f4jy2m7q9v3x5abf".into();
    let target_identity = serde_json::json!({"principalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abe", "nameState": "unavailable"});
    principal_target["targetIdentity"] = target_identity.clone();
    let (server, _directory, _path, credential_path) = prepared(vec![json_http_response(
        "200 OK",
        serde_json::json!({
            "items": [accepted, revoked, expired, principal_target],
            "nextCursor": "page-two"
        }),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "organization",
            "invitation",
            "list",
            ORG_ID,
            "--limit",
            "4",
            "--cursor",
            "page-one",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"state\": \"accepted\""));
    assert!(stdout.contains("\"state\": \"revoked\""));
    assert!(stdout.contains("\"state\": \"expired\""));
    let document: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(document["schemaVersion"], 2);
    assert_eq!(
        document["items"][0]["targetEmail"],
        "delivered@example.test"
    );
    assert_eq!(
        document["items"][0]["acceptingPrincipalId"],
        "prn_01k0z6r1w8f4jy2m7q9v3x5abd"
    );
    assert_eq!(
        document["items"][0]["acceptedMembershipId"],
        "mem_01k0z6r1w8f4jy2m7q9v3x5abc"
    );
    assert!(document["items"][1].get("acceptedMembershipId").is_none());
    assert_eq!(
        document["items"][0]["issuerIdentity"]["nameState"],
        "snapshot"
    );
    assert_eq!(
        document["items"][0]["issuerIdentity"]["linkedEmails"][0]["verified"],
        false
    );
    assert_eq!(
        document["items"][0]["acceptingIdentity"],
        accepting_identity
    );
    assert_eq!(document["items"][3]["targetIdentity"], target_identity);
    assert_eq!(
        document["items"][1]["targetEmail"],
        "not-joined@example.test"
    );
    assert_eq!(document["items"][2]["targetKind"], "email");
    assert!(document["items"][2].get("targetEmail").is_none());
    assert_eq!(document["nextCursor"], "page-two");
    assert!(output.stderr.is_empty());
    let request = server.finish().pop().unwrap();
    assert!(request.starts_with(&format!(
        "GET /api/v1/organizations/{ORG_ID}/invitations?limit=4&cursor=page-one HTTP/1.1\r\n"
    )));
    assert_authenticated(&request);
}

// REQ-INVITE-63
#[test]
fn organization_invitation_history_human_labels_recorded_and_unavailable_links() {
    let mut recorded = invitation("accepted", Some("2026-01-03T03:04:05Z"));
    recorded["targetKind"] = "email".into();
    recorded["targetEmail"] = "delivered@example.test".into();
    recorded["acceptingPrincipalId"] = "prn_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    recorded["acceptedMembershipId"] = "mem_01k0z6r1w8f4jy2m7q9v3x5abc".into();
    let mut older = invitation("accepted", Some("2026-01-03T03:04:05Z"));
    older["id"] = "inv_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    let (server, _directory, _path, credential_path) = prepared(vec![json_http_response(
        "200 OK",
        serde_json::json!({ "items": [recorded, older] }),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "organization",
            "invitation",
            "list",
            ORG_ID,
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("delivered@example.test"));
    assert!(stdout.contains("accepted by: prn_01k0z6r1w8f4jy2m7q9v3x5abd"));
    assert!(stdout.contains("accepted membership: mem_01k0z6r1w8f4jy2m7q9v3x5abc"));
    assert!(stdout.contains("acceptance link: unavailable"));
    server.finish();
}

// REQ-INVITE-89
#[test]
fn organization_invitation_history_human_reads_each_party_by_its_name_state() {
    let mut nameless = invitation("accepted", Some("2026-01-03T03:04:05Z"));
    nameless["targetKind"] = "email".into();
    nameless["targetEmail"] = "delivered@example.test".into();
    nameless["issuerIdentity"] = serde_json::json!({"principalId": PRINCIPAL_ID, "nameState": "live", "linkedEmails": [{"email": "issuer@example.test", "verified": true}]});
    nameless["acceptingIdentity"] = serde_json::json!({"principalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abd", "nameState": "snapshot", "displayName": "At acceptance"});
    let mut principal_target = invitation("revoked", Some("2026-01-04T03:04:05Z"));
    principal_target["id"] = "inv_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    principal_target["targetIdentity"] = serde_json::json!({"principalId": "prn_01k0z6r1w8f4jy2m7q9v3x5abe", "nameState": "unavailable"});
    let mut unrecorded = invitation("expired", Some("2026-01-09T03:04:05Z"));
    unrecorded["id"] = "inv_01k0z6r1w8f4jy2m7q9v3x5abe".into();
    unrecorded["targetKind"] = "email".into();
    let (server, _directory, _path, credential_path) = prepared(vec![json_http_response(
        "200 OK",
        serde_json::json!({ "items": [nameless, principal_target, unrecorded] }),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);
    let output = run_with_env(
        &[
            "organization",
            "invitation",
            "list",
            ORG_ID,
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    // Each invitation's lines, so that a line from another invitation cannot satisfy them.
    let block = |id: &str| -> Vec<&str> {
        stdout
            .split("\n\n")
            .find(|block| block.starts_with(&format!("invitation: {id} ")))
            .unwrap_or_else(|| panic!("no block for {id} in {stdout}"))
            .lines()
            .collect()
    };
    let nameless = block(INVITATION_ID);
    for line in [
        "issuer identity: no name".to_owned(),
        format!("issuer identity principal: {PRINCIPAL_ID}"),
        "issuer identity login email: issuer@example.test".to_owned(),
        "accepting identity: At acceptance".to_owned(),
    ] {
        assert!(nameless.contains(&line.as_str()), "{line} in {nameless:?}");
    }
    let principal_target = block("inv_01k0z6r1w8f4jy2m7q9v3x5abd");
    for line in [
        "recipient identity: unavailable",
        "recipient identity principal: prn_01k0z6r1w8f4jy2m7q9v3x5abe",
    ] {
        assert!(
            principal_target.contains(&line),
            "{line} in {principal_target:?}"
        );
    }
    let unrecorded = block("inv_01k0z6r1w8f4jy2m7q9v3x5abe");
    assert!(unrecorded.contains(&"issuer identity: unavailable"));
    server.finish();
}

// REQ-INVITE-67, REQ-INVITE-68
#[test]
fn invitation_inbox_and_preview_are_available_to_the_current_principal() {
    let (server, _directory, _path, credential_path) = prepared(vec![
        json_http_response(
            "200 OK",
            serde_json::json!({
                "items": [{
                    "id": INVITATION_ID,
                    "organizationId": ORG_ID,
                    "organizationDisplayName": "Example Organization",
                    "organizationSlug": "example",
                    "issuerPrincipalId": PRINCIPAL_ID,
                    "expiresAt": "2026-01-09T03:04:05Z"
                }]
            }),
        ),
        json_http_response(
            "200 OK",
            serde_json::json!({
                "id": INVITATION_ID,
                "organizationId": ORG_ID,
                "organizationDisplayName": "Example Organization",
                "organizationSlug": "example",
                "targetKind": "email",
                "expiresAt": "2026-01-09T03:04:05Z"
            }),
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let inbox = run_with_env(
        &["invitation", "list", "--allow-insecure-http"],
        &environment,
    );
    assert!(inbox.status.success(), "{inbox:?}");
    let stdout = String::from_utf8(inbox.stdout).unwrap();
    assert!(stdout.contains("Invitation inbox listed"));
    assert!(stdout.contains("Example Organization"));
    assert!(inbox.stderr.is_empty());

    let preview = run_with_env(
        &[
            "invitation",
            "preview",
            INVITATION_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(preview.status.success(), "{preview:?}");
    let stdout = String::from_utf8(preview.stdout).unwrap();
    assert!(stdout.contains("\"outcome\": \"previewed\""));
    assert!(stdout.contains("\"targetKind\": \"email\""));
    assert!(preview.stderr.is_empty());

    let requests = server.finish();
    assert!(requests[0].starts_with("GET /api/v1/me/invitations HTTP/1.1\r\n"));
    assert!(requests[1].starts_with(&format!(
        "POST /api/v1/invitations/{INVITATION_ID}/preview HTTP/1.1\r\n"
    )));
    assert_eq!(request_body(&requests[1]), "{}");
    requests
        .iter()
        .for_each(|request| assert_authenticated(request));
}

// REQ-INVITE-76, REQ-INVITE-78, REQ-INVITE-79
#[test]
fn invitation_accept_reads_a_protected_capability_file_without_printing_it() {
    let (server, capability_directory, _path, credential_path) =
        prepared(vec![json_response_with_headers(
            "200 OK",
            &serde_json::json!({
                "id": MEMBERSHIP_ID,
                "organizationId": ORG_ID,
                "principalId": PRINCIPAL_ID,
                "role": "member",
                "state": "active",
                "createdAt": "2026-01-02T03:04:05Z",
                "updatedAt": "2026-01-02T03:04:05Z"
            }),
            &[("idempotency-key", ECHO_IDEMPOTENCY_KEY)],
        )]);
    let capability_path = capability_directory.path().join("invitation.capability");
    write_capability(&capability_path, CAPABILITY, 0o600);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "invitation",
            "accept",
            INVITATION_ID,
            "--capability-file",
            capability_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("\"outcome\": \"accepted\""));
    assert!(!stdout.contains(CAPABILITY));
    assert!(!stderr.contains(CAPABILITY));
    let request = server.finish().pop().unwrap();
    assert_authenticated(&request);
    assert!(!header_value(&request, "idempotency-key").is_empty());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&request)).unwrap(),
        serde_json::json!({ "capability": CAPABILITY })
    );
}

// REQ-INVITE-73
#[test]
fn invitation_accept_name_gate_reports_a_recoverable_schema_one_failure() {
    for json in [false, true] {
        let (server, _directory, _path, credential_path) =
            prepared(vec![problem_response_with_headers(
                "409 Conflict",
                "https://api.usefulmachinery.com/problems/human-display-name-required",
                409,
                &[],
            )]);
        let environment = deployment_environment(&server.api_url, &credential_path);
        let mut arguments = vec![
            "invitation",
            "accept",
            INVITATION_ID,
            "--allow-insecure-http",
        ];
        if json {
            arguments.push("--json");
        }
        let output = run_with_env(&arguments, &environment);
        assert_eq!(output.status.code(), Some(1));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("Invitation request failed"));
        if json {
            let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(document["schemaVersion"], 1);
            assert_eq!(document["outcome"], "human_display_name_required");
            assert!(output.stderr.is_empty());
        } else {
            let message = String::from_utf8(output.stderr).unwrap();
            assert!(message.contains("um account update --display-name"));
        }
        let request = server.finish().pop().unwrap();
        assert_authenticated(&request);
        assert!(request.starts_with(&format!(
            "POST /api/v1/invitations/{INVITATION_ID}/accept HTTP/1.1\r\n"
        )));
    }
}

// REQ-INVITE-66, REQ-INVITE-75
#[test]
fn invitation_decline_and_owner_revoke_have_deterministic_success_output() {
    let (server, _directory, _path, credential_path) = prepared(vec![
        http_response_with_headers(
            "204 No Content",
            None,
            &[("idempotency-key", ECHO_IDEMPOTENCY_KEY)],
            &[],
        ),
        http_response_with_headers(
            "204 No Content",
            None,
            &[("idempotency-key", ECHO_IDEMPOTENCY_KEY)],
            &[],
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let decline = run_with_env(
        &[
            "invitation",
            "decline",
            INVITATION_ID,
            "--yes",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(decline.status.success(), "{decline:?}");
    assert!(
        String::from_utf8(decline.stdout)
            .unwrap()
            .contains("Invitation declined")
    );
    assert!(decline.stderr.is_empty());

    let revoke = run_with_env(
        &[
            "organization",
            "invitation",
            "revoke",
            ORG_ID,
            INVITATION_ID,
            "--yes",
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );
    assert!(revoke.status.success(), "{revoke:?}");
    assert!(
        String::from_utf8(revoke.stdout)
            .unwrap()
            .contains("\"outcome\": \"revoked\"")
    );
    assert!(revoke.stderr.is_empty());

    let requests = server.finish();
    assert!(requests[0].starts_with(&format!(
        "POST /api/v1/invitations/{INVITATION_ID}/decline HTTP/1.1\r\n"
    )));
    assert!(requests[1].starts_with(&format!(
        "DELETE /api/v1/organizations/{ORG_ID}/invitations/{INVITATION_ID} HTTP/1.1\r\n"
    )));
    requests.iter().for_each(|request| {
        assert_authenticated(request);
        assert!(!header_value(request, "idempotency-key").is_empty());
    });
}

// REQ-INVITE-76, REQ-INVITE-77
#[test]
fn capability_file_rejects_group_access_without_printing_the_secret() {
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        "http://127.0.0.1:9/api",
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let capability_path = credential_directory.path().join("unsafe.capability");
    write_capability(&capability_path, CAPABILITY, 0o640);
    let environment =
        deployment_environment("http://127.0.0.1:9/api", credential_path.to_str().unwrap());

    let output = run_with_env(
        &[
            "invitation",
            "preview",
            INVITATION_ID,
            "--capability-file",
            capability_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("\"outcome\": \"capability_source_unavailable\""));
    assert!(!stdout.contains(CAPABILITY));
    assert!(!stderr.contains(CAPABILITY));
}

// REQ-INVITE-77
#[test]
fn invitation_capability_must_match_the_path_invitation() {
    let credential_directory = private_credential_directory();
    let credential_path = credential_directory.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        "http://127.0.0.1:9/api",
        TOKEN,
        "2999-01-01T00:00:00Z",
    );
    let capability_path = credential_directory.path().join("invitation.capability");
    write_capability(
        &capability_path,
        "inv_01k0z6r1w8f4jy2m7q9v3x5abd.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        0o600,
    );
    let environment =
        deployment_environment("http://127.0.0.1:9/api", credential_path.to_str().unwrap());

    let output = run_with_env(
        &[
            "invitation",
            "accept",
            INVITATION_ID,
            "--capability-file",
            capability_path.to_str().unwrap(),
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stdout.contains("\"outcome\": \"invalid_capability\""));
    assert!(!stdout.contains("AAAAAAAA"));
    assert!(!stderr.contains("AAAAAAAA"));
}

// REQ-INVITE-69
#[test]
fn unavailable_invitation_returns_stable_json_and_exit_code() {
    let (server, _directory, _path, credential_path) = prepared(vec![problem_http_response(
        "409 Conflict",
        serde_json::json!({
            "type": "https://api.usefulmachinery.com/problems/invitation-unavailable",
            "title": "Invitation unavailable",
            "status": 409
        }),
    )]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let output = run_with_env(
        &[
            "invitation",
            "preview",
            INVITATION_ID,
            "--json",
            "--allow-insecure-http",
        ],
        &environment,
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("\"outcome\": \"invitation_unavailable\"")
    );
    assert!(output.stderr.is_empty());
    server.finish();
}

// REQ-INVITE-59, REQ-INVITE-74
#[test]
fn invitation_api_failures_use_stable_outcomes_and_exit_codes() {
    let (server, _directory, _path, credential_path) = prepared(vec![
        problem_response_with_headers(
            "429 Too Many Requests",
            "https://api.usefulmachinery.com/problems/rate-limit-exceeded",
            429,
            &[("Retry-After", "17")],
        ),
        problem_response_with_headers(
            "409 Conflict",
            "https://api.usefulmachinery.com/problems/membership-limit-reached",
            409,
            &[],
        ),
        problem_response_with_headers(
            "409 Conflict",
            "https://api.usefulmachinery.com/problems/idempotency-conflict",
            409,
            &[],
        ),
        problem_response_with_headers(
            "409 Conflict",
            "https://api.usefulmachinery.com/problems/quantity-limit-reached",
            409,
            &[],
        ),
        problem_response_with_headers(
            "409 Conflict",
            "https://api.usefulmachinery.com/problems/quantity-limit-reached",
            409,
            &[],
        ),
    ]);
    let environment = deployment_environment(&server.api_url, &credential_path);

    let cases = [
        (
            vec![
                "organization",
                "invitation",
                "issue",
                ORG_ID,
                "--email",
                "teammate@example.com",
                "--json",
                "--allow-insecure-http",
            ],
            4,
            "rate_limited",
            Some(17),
        ),
        (
            vec![
                "invitation",
                "accept",
                INVITATION_ID,
                "--json",
                "--allow-insecure-http",
            ],
            1,
            "membership_limit_reached",
            None,
        ),
        (
            vec![
                "invitation",
                "decline",
                INVITATION_ID,
                "--yes",
                "--json",
                "--allow-insecure-http",
            ],
            1,
            "idempotency_conflict",
            None,
        ),
        (
            vec![
                "organization",
                "invitation",
                "issue",
                ORG_ID,
                "--principal-id",
                PRINCIPAL_ID,
                "--json",
                "--allow-insecure-http",
            ],
            1,
            "quantity_limit_reached",
            None,
        ),
        (
            vec![
                "invitation",
                "accept",
                INVITATION_ID,
                "--json",
                "--allow-insecure-http",
            ],
            1,
            "quantity_limit_reached",
            None,
        ),
    ];

    for (arguments, exit_code, outcome, retry_after) in cases {
        let output = run_with_env(&arguments, &environment);
        assert_eq!(output.status.code(), Some(exit_code), "{output:?}");
        assert!(output.stderr.is_empty());
        let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(document["schemaVersion"], 1);
        assert_eq!(document["outcome"], outcome);
        if let Some(retry_after) = retry_after {
            assert_eq!(document["retryAfter"], retry_after);
        }
    }

    assert_eq!(server.finish().len(), 5);
}
