use std::collections::BTreeSet;

use um_test_support::ScriptedHttpServer;

use super::*;

const ACTIVATION_ID: &str = "rna_01k0z6r1w8f4jy2m7q9v3x5abc";
const OTHER_ACTIVATION_ID: &str = "rna_01k0z6r1w8f4jy2m7q9v3x5abd";
const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
const CREDENTIAL_ID: &str = "rrc_01k0z6r1w8f4jy2m7q9v3x5abc";
const REPLACEMENT_CREDENTIAL_ID: &str = "rrc_01k0z6r1w8f4jy2m7q9v3x5abd";
const ACTIVATION_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const RUNNER_STATE_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/runner-state-v1.schema.json"
));
pub(super) const OPERATOR_CONFIG_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/runner-operator-config-v1.schema.json"
));
const ENROLLMENT_JOURNAL_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/runner-enrollment-journal-v1.schema.json"
));
const TERMINAL_RECEIPT_SCHEMA: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/runner-enrollment-terminal-receipt-v1.schema.json"
));

pub(super) fn assert_json_matches_schema(format: &str, schema_source: &str, bytes: &[u8]) {
    let schema: serde_json::Value =
        serde_json::from_str(schema_source).expect("decode mirrored runner host schema");
    let document: serde_json::Value =
        serde_json::from_slice(bytes).expect("decode produced runner host document");
    let validator = jsonschema::draft202012::new(&schema).expect("compile runner host schema");
    assert!(
        validator.is_valid(&document),
        "{format} producer output does not match its checked-in schema"
    );
}

// REQ-RUNNERENROLL-15, REQ-RUNNERENROLL-16
#[test]
fn url_policy_requires_secure_or_explicit_loopback_development_transport() {
    for (mode, value, kind, accepted) in [
        (
            DeploymentMode::Production,
            "https://api.usefulmachinery.com/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            true,
        ),
        (
            DeploymentMode::Production,
            "http://localhost:8080/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            false,
        ),
        (
            DeploymentMode::Development,
            "http://127.0.0.1:8080/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            true,
        ),
        (
            DeploymentMode::Development,
            "http://127.1:8080/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            false,
        ),
        (
            DeploymentMode::Development,
            "http://host.docker.internal:8080/v1/runner-enrollments/rna_01k0z6r1w8f4jy2m7q9v3x5abc/activate",
            CloudURLKind::Activation,
            false,
        ),
        (
            DeploymentMode::Development,
            "ws://[::1]:8080/v1/runner/connect",
            CloudURLKind::Connection,
            true,
        ),
        (
            DeploymentMode::Development,
            "ws://192.168.1.3:8080/v1/runner/connect",
            CloudURLKind::Connection,
            false,
        ),
        (
            DeploymentMode::Production,
            "wss://user@example.test/v1/runner/connect",
            CloudURLKind::Connection,
            false,
        ),
        (
            DeploymentMode::Production,
            "wss://@example.test/v1/runner/connect",
            CloudURLKind::Connection,
            false,
        ),
        (
            DeploymentMode::Production,
            "wss://example.test/v1/runner/connect#fragment",
            CloudURLKind::Connection,
            false,
        ),
        (
            DeploymentMode::Production,
            "wss://example.test/v1/runner/connect?endpoint=other",
            CloudURLKind::Connection,
            false,
        ),
    ] {
        assert_eq!(
            validate_cloud_url(value, mode, kind).is_ok(),
            accepted,
            "{value}"
        );
    }
}

// REQ-RUNNERENROLL-14
#[test]
fn operator_config_written_bytes_match_schema_in_both_deployment_modes() {
    let temporary = tempfile::tempdir().expect("create config fixture");
    let config_path = temporary.path().join("runner.json");
    for mode in ["production", "development"] {
        let document = serde_json::json!({
            "schemaVersion": 1,
            "deploymentMode": mode,
            "runnerStatePath": "/var/lib/scherzo-cloud/runner-state.json",
            "controlSocketPath": "/run/scherzo-cloud/runner.sock",
            "workRoot": "/var/lib/scherzo-cloud/work"
        });
        fs::write(
            &config_path,
            serde_json::to_vec(&document).expect("encode config"),
        )
        .expect("write config");
        let bytes = fs::read(&config_path).expect("read written operator config");
        assert_json_matches_schema("runner operator config", OPERATOR_CONFIG_SCHEMA, &bytes);
        assert!(load_operator_config(&config_path).is_ok(), "{mode}");
    }
}

#[test]
fn activation_artifact_accepts_a_plain_relative_destination() {
    assert_eq!(
        artifact_parent(Path::new("activation.json")),
        Path::new(".")
    );
}

// REQ-RUNNERENROLL-18
// INV-RUNNER-9
#[test]
fn atomic_state_write_uses_private_permissions_and_rejects_links() {
    let directory = tempfile::TempDir::new().expect("create directory");
    fs::set_permissions(directory.path(), Permissions::from_mode(0o700))
        .expect("protect directory");
    let path = directory.path().join("state.json");
    atomic_write_json(&path, &serde_json::json!({"schemaVersion": 1})).expect("write state");
    assert_eq!(
        fs::metadata(&path).expect("state metadata").mode() & 0o777,
        0o600
    );

    let link = directory.path().join("link.json");
    std::os::unix::fs::symlink(&path, &link).expect("create symbolic link");
    assert!(atomic_write_json(&link, &serde_json::json!({"schemaVersion": 1})).is_err());

    let hard_link = directory.path().join("hard-link.json");
    fs::hard_link(&path, &hard_link).expect("create hard link");
    let original = fs::read(&path).expect("read original state");
    assert!(atomic_write_json(&path, &serde_json::json!({"schemaVersion": 2})).is_err());
    assert_eq!(fs::read(&path).expect("reread original state"), original);
}

// INV-TRUST-1
// REQ-RUNNERENROLL-18
// INV-RUNNER-9
#[test]
fn successful_enrollment_commits_schema_valid_state_without_transmitting_secret() {
    let server = ScriptedHttpServer::respond(enrollment_response());
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());

    let outcome = enroll(
        Some(&fixture.activation_path),
        &fixture.config_path,
        false,
        false,
    )
    .expect("enroll runner");
    let EnrollmentOutcome::Enrolled {
        response,
        replacement,
        deployment,
    } = outcome
    else {
        panic!("enrollment unexpectedly returned gone");
    };
    assert!(!replacement);
    assert_eq!(deployment, server.api_url.trim_end_matches("/api/"));
    assert_eq!(response.credential_id(), CREDENTIAL_ID);
    assert!(!fixture.journal_path().exists());

    let state_metadata = fs::metadata(&fixture.state_path).expect("state metadata");
    assert_eq!(state_metadata.mode() & 0o777, 0o600);
    assert_eq!(state_metadata.nlink(), 1);
    assert_eq!(
        fs::metadata(fixture.state_path.parent().expect("state parent"))
            .expect("state directory metadata")
            .mode()
            & 0o777,
        0o700,
    );
    let state_bytes = fs::read(&fixture.state_path).expect("read protected state");
    assert_json_matches_schema("runner state", RUNNER_STATE_SCHEMA, &state_bytes);
    let state: RunnerState = serde_json::from_slice(&state_bytes).expect("decode protected state");
    assert_eq!(state.runner_id, RUNNER_ID);
    assert_eq!(state.current_credential.id, CREDENTIAL_ID);
    assert!(valid_secret(&state.current_credential.secret));

    let service = load_runner_service_configuration(&fixture.config_path)
        .expect("load Runner Serve configuration from enrolled state");
    assert_eq!(service.runner_id, RUNNER_ID);
    assert_eq!(service.connection_url, state.connection_url);
    assert_eq!(service.credential_id, CREDENTIAL_ID);
    assert_eq!(service.credential_secret, state.current_credential.secret);

    let request = server.finish_one();
    let body = request_body(&request);
    let decoded: serde_json::Value = serde_json::from_str(body).expect("decode request body");
    let keys = decoded
        .as_object()
        .expect("request object")
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        keys,
        BTreeSet::from([
            "credentialSecretVerifier".to_owned(),
            "schemaVersion".to_owned(),
        ])
    );
    let complete_credential = format!("{CREDENTIAL_ID}.{}", state.current_credential.secret);
    assert!(!request.contains(&state.current_credential.secret));
    assert!(!request.contains(&complete_credential));
}

// REQ-RUNNERENROLL-48
#[test]
fn replacement_enrollment_preserves_current_and_reuses_the_staged_pending_credential() {
    let server = ScriptedHttpServer::respond(enrollment_response_with_credential(
        REPLACEMENT_CREDENTIAL_ID,
    ));
    let fixture = EnrollmentFixture::new(&server, OTHER_ACTIVATION_ID, future_time());
    fixture.write_current_state();

    let outcome = enroll(
        Some(&fixture.activation_path),
        &fixture.config_path,
        true,
        false,
    )
    .expect("enroll replacement credential");
    assert!(matches!(
        outcome,
        EnrollmentOutcome::Enrolled {
            replacement: true,
            ..
        }
    ));
    server.finish_one();

    let state: RunnerState =
        serde_json::from_slice(&fs::read(&fixture.state_path).expect("read replacement state"))
            .expect("decode replacement state");
    assert_eq!(state.runner_id, RUNNER_ID);
    assert_eq!(state.current_credential.id, CREDENTIAL_ID);
    assert_eq!(
        state
            .pending_credential
            .as_ref()
            .map(|value| value.id.as_str()),
        Some(REPLACEMENT_CREDENTIAL_ID)
    );
    assert!(!fixture.journal_path().exists());
    assert_eq!(
        replacement_disposition(&fixture.config_path, RUNNER_ID, REPLACEMENT_CREDENTIAL_ID)
            .expect("read replacement disposition"),
        ReplacementDisposition::Pending
    );

    let staged_state = fs::read(&fixture.state_path).expect("read staged state");
    assert_json_matches_schema("runner state", RUNNER_STATE_SCHEMA, &staged_state);
    let retry = enroll(
        Some(&fixture.activation_path),
        &fixture.config_path,
        true,
        false,
    )
    .expect("reuse staged pending credential without another Cloud request");
    assert!(matches!(
        retry,
        EnrollmentOutcome::ReplacementCredential {
            runner_id,
            credential_id,
            ..
        } if runner_id == RUNNER_ID && credential_id == REPLACEMENT_CREDENTIAL_ID
    ));
    assert_eq!(
        fs::read(&fixture.state_path).expect("read retried state"),
        staged_state
    );

    let service =
        load_runner_service_configuration(&fixture.config_path).expect("load pending state");
    service
        .state_access
        .promote(RUNNER_ID, CREDENTIAL_ID, REPLACEMENT_CREDENTIAL_ID)
        .expect("promote replacement state");
    assert_eq!(
        replacement_disposition(&fixture.config_path, RUNNER_ID, REPLACEMENT_CREDENTIAL_ID)
            .expect("read promoted disposition"),
        ReplacementDisposition::Current
    );
    let promoted_state = fs::read(&fixture.state_path).expect("read promoted state");
    assert_json_matches_schema("runner state", RUNNER_STATE_SCHEMA, &promoted_state);
    let retry = enroll(
        Some(&fixture.activation_path),
        &fixture.config_path,
        true,
        false,
    )
    .expect("recover a lost promotion response without another Cloud request");
    assert!(matches!(
        retry,
        EnrollmentOutcome::ReplacementCredential {
            runner_id,
            credential_id,
            ..
        } if runner_id == RUNNER_ID && credential_id == REPLACEMENT_CREDENTIAL_ID
    ));
    assert_eq!(
        fs::read(&fixture.state_path).expect("read recovered promoted state"),
        promoted_state
    );
}

// REQ-RUNNERENROLL-16
#[test]
fn enrollment_rejects_redirects_without_losing_the_journal() {
    let server = ScriptedHttpServer::respond(redirect_response(
        "http://127.0.0.1:9/v1/runner-enrollments/redirected/activate",
    ));
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());

    assert!(matches!(
        enroll(
            Some(&fixture.activation_path),
            &fixture.config_path,
            false,
            false,
        ),
        Err(EnrollmentError::InvalidResponse)
    ));
    let journal = fs::read(fixture.journal_path()).expect("read redirect journal");
    assert!(String::from_utf8_lossy(&journal).contains(ACTIVATION_ID));
    server.finish_one();
}

// REQ-RUNNERENROLL-21, REQ-RUNNERENROLL-23
#[test]
fn enrollment_journal_and_terminal_receipt_match_schemas_across_retries() {
    let server = ScriptedHttpServer::respond_in_sequence(vec![
        empty_response("500 Internal Server Error"),
        empty_response("401 Unauthorized"),
        empty_response("409 Conflict"),
        empty_response("410 Gone"),
    ]);
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());

    assert!(matches!(
        enroll(
            Some(&fixture.activation_path),
            &fixture.config_path,
            false,
            false,
        ),
        Err(EnrollmentError::NetworkAmbiguous(
            AmbiguousCause::CloudUnavailable(500)
        ))
    ));
    let journal_path = fixture.journal_path();
    let original = fs::read(&journal_path).expect("read staged journal");
    assert_json_matches_schema(
        "runner enrollment journal",
        ENROLLMENT_JOURNAL_SCHEMA,
        &original,
    );
    let journal: EnrollmentJournal = serde_json::from_slice(&original).expect("decode journal");
    assert!(valid_secret(&journal.credential_secret));

    assert!(matches!(
        enroll(None, &fixture.config_path, false, true),
        Err(EnrollmentError::Unauthorized)
    ));
    assert_eq!(
        fs::read(&journal_path).expect("read unauthorized journal"),
        original
    );
    assert!(matches!(
        enroll(None, &fixture.config_path, false, true),
        Err(EnrollmentError::Conflict)
    ));
    assert_eq!(
        fs::read(&journal_path).expect("read conflicting journal"),
        original
    );

    let outcome = enroll(None, &fixture.config_path, false, true).expect("resolve gone journal");
    assert!(matches!(
        outcome,
        EnrollmentOutcome::Gone { activation_id, .. } if activation_id == ACTIVATION_ID
    ));
    let receipt_bytes = fs::read(&journal_path).expect("read terminal receipt");
    assert_json_matches_schema(
        "runner enrollment terminal receipt",
        TERMINAL_RECEIPT_SCHEMA,
        &receipt_bytes,
    );
    let receipt: TerminalReceipt = serde_json::from_slice(&receipt_bytes).expect("decode receipt");
    assert!(valid_terminal_receipt(&receipt));
    for secret_member in [
        "activationToken",
        "credentialSecret",
        "credentialSecretVerifier",
        "idempotencyKey",
        ACTIVATION_SECRET,
        &journal.credential_secret,
    ] {
        assert!(!String::from_utf8_lossy(&receipt_bytes).contains(secret_member));
    }

    let requests = server.finish();
    let first_body = request_body(&requests[0]);
    let first_authorization = request_header(&requests[0], "authorization");
    let first_key = request_header(&requests[0], "idempotency-key");
    for request in &requests[1..] {
        assert_eq!(request_body(request), first_body);
        assert_eq!(
            request_header(request, "authorization"),
            first_authorization
        );
        assert_eq!(request_header(request, "idempotency-key"), first_key);
    }

    let replacement_server =
        ScriptedHttpServer::respond(empty_response("500 Internal Server Error"));
    let replacement = fixture.artifact(&replacement_server, OTHER_ACTIVATION_ID, future_time());
    let replacement_path = fixture.root.path().join("replacement-activation.json");
    write_artifact(&replacement_path, &replacement);
    assert!(matches!(
        enroll(Some(&replacement_path), &fixture.config_path, false, false,),
        Err(EnrollmentError::NetworkAmbiguous(
            AmbiguousCause::CloudUnavailable(500)
        ))
    ));
    let replacement_journal = fs::read(&journal_path).expect("read replacement journal");
    assert!(String::from_utf8_lossy(&replacement_journal).contains(OTHER_ACTIVATION_ID));
    replacement_server.finish_one();
}

// REQ-RUNNERENROLL-21
#[test]
fn an_ambiguous_enrollment_names_its_cause() {
    for (response, expected, code) in [
        (Vec::new(), AmbiguousCause::Network, "network"),
        (
            empty_response("429 Too Many Requests"),
            AmbiguousCause::RateLimited,
            "rate_limited",
        ),
        (
            empty_response("503 Service Unavailable"),
            AmbiguousCause::CloudUnavailable(503),
            "cloud_unavailable",
        ),
    ] {
        let server = ScriptedHttpServer::respond(response);
        let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());

        let result = enroll(
            Some(&fixture.activation_path),
            &fixture.config_path,
            false,
            false,
        );

        let Err(error) = result else {
            panic!("expected an ambiguous outcome for {code}, got {result:?}");
        };
        let EnrollmentError::NetworkAmbiguous(cause) = error else {
            panic!("expected an ambiguous outcome for {code}, got {error:?}");
        };
        assert_eq!(cause, expected);
        assert_eq!(cause.code(), code);
        // The failure `um runner enroll` prints is this error's message.
        assert!(error.to_string().contains(code), "{error}");
        assert!(fixture.journal_path().is_file(), "journal kept for {code}");
        server.finish_one();
    }
}

// REQ-RUNNERENROLL-22
#[test]
fn resume_finishes_after_state_commits_before_journal_removal() {
    let server = ScriptedHttpServer::respond(enrollment_response());
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());
    let state_directory = fixture.state_path.parent().expect("state directory");
    ensure_private_directory(state_directory).expect("create state directory");
    let journal = stage_journal(
        &fixture.journal_path(),
        EnrollmentTarget::Activation(fixture.artifact.clone()),
        false,
    )
    .expect("stage journal");
    let response: EnrollmentResponse =
        serde_json::from_value(enrollment_response_document()).expect("decode enrollment response");
    persist_enrollment_state(
        &fixture.state_path,
        &journal,
        &response,
        DeploymentMode::Development,
    )
    .expect("commit state before simulated crash");
    let committed_state = fs::read(&fixture.state_path).expect("read committed state");
    assert!(fixture.journal_path().exists());

    let outcome = enroll(None, &fixture.config_path, false, true).expect("resume enrollment");
    assert!(matches!(outcome, EnrollmentOutcome::Enrolled { .. }));
    assert_eq!(
        fs::read(&fixture.state_path).expect("read recovered state"),
        committed_state
    );
    assert!(!fixture.journal_path().exists());
    server.finish_one();
}

#[test]
fn replacement_resume_recognizes_a_startup_promotion_before_journal_cleanup() {
    let server = ScriptedHttpServer::respond(enrollment_response_with_credential(
        REPLACEMENT_CREDENTIAL_ID,
    ));
    let fixture = EnrollmentFixture::new(&server, OTHER_ACTIVATION_ID, future_time());
    let state_directory = fixture.state_path.parent().expect("state directory");
    ensure_private_directory(state_directory).expect("create state directory");
    atomic_write_json(
        &fixture.state_path,
        &serde_json::json!({
            "schemaVersion": 1,
            "runnerId": RUNNER_ID,
            "connectionUrl": "ws://127.0.0.1:8765/v1/runner/connect",
            "currentCredential": {
                "id": CREDENTIAL_ID,
                "secret": ACTIVATION_SECRET,
                "activationId": ACTIVATION_ID,
                "enrolledAt": "2026-08-06T12:00:00Z"
            },
            "updatedAt": "2026-08-06T12:00:00Z"
        }),
    )
    .expect("write current runner state");
    let journal = stage_journal(
        &fixture.journal_path(),
        EnrollmentTarget::Activation(fixture.artifact.clone()),
        true,
    )
    .expect("stage replacement journal");
    let response: EnrollmentResponse = serde_json::from_value(
        enrollment_response_document_with_credential(REPLACEMENT_CREDENTIAL_ID),
    )
    .expect("decode replacement response");
    persist_enrollment_state(
        &fixture.state_path,
        &journal,
        &response,
        DeploymentMode::Development,
    )
    .expect("stage pending replacement");
    let service =
        load_runner_service_configuration(&fixture.config_path).expect("load pending state");
    service
        .state_access
        .promote(RUNNER_ID, CREDENTIAL_ID, REPLACEMENT_CREDENTIAL_ID)
        .expect("simulate startup promotion");
    let promoted_state = fs::read(&fixture.state_path).expect("read promoted state");
    assert!(fixture.journal_path().exists());

    let outcome = enroll(None, &fixture.config_path, false, true)
        .expect("resume replacement after startup promotion");
    assert!(matches!(
        outcome,
        EnrollmentOutcome::Enrolled {
            replacement: true,
            ..
        }
    ));
    assert_eq!(
        fs::read(&fixture.state_path).expect("read resumed promoted state"),
        promoted_state
    );
    assert!(!fixture.journal_path().exists());
    server.finish_one();
}

#[test]
fn journal_rejects_a_verifier_that_does_not_match_its_secret() {
    let journal = EnrollmentJournal {
        schema_version: 1,
        target: EnrollmentTarget::Activation(ActivationArtifact {
            schema_version: 1,
            activation_url: format!(
                "https://api.usefulmachinery.com/v1/runner-enrollments/{ACTIVATION_ID}/activate"
            ),
            activation_token: format!("{ACTIVATION_ID}.{ACTIVATION_SECRET}"),
            runner_id: RUNNER_ID.to_owned(),
            expires_at: future_time(),
        }),
        credential_secret: ACTIVATION_SECRET.to_owned(),
        credential_secret_verifier: ACTIVATION_SECRET.to_owned(),
        idempotency_key: "enrollment-idempotency".to_owned(),
        replace_credential: false,
        staged_at: future_time(),
    };
    assert!(matches!(
        validate_journal(&journal, DeploymentMode::Production),
        Err(EnrollmentError::InvalidJournal)
    ));
}

// REQ-RUNNERENROLL-17, REQ-RUNNERENROLL-22
#[test]
fn resume_sends_an_expired_journal_instead_of_applying_local_expiry() {
    let server = ScriptedHttpServer::respond(enrollment_response());
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, expired_time());
    assert!(matches!(
        read_activation_artifact(&fixture.activation_path, DeploymentMode::Development),
        Err(EnrollmentError::ExpiredArtifact)
    ));

    let state_directory = fixture.state_path.parent().expect("state directory");
    ensure_private_directory(state_directory).expect("create state directory");
    stage_journal(
        &fixture.journal_path(),
        EnrollmentTarget::Activation(fixture.artifact.clone()),
        false,
    )
    .expect("stage expired journal");
    let outcome = enroll(None, &fixture.config_path, false, true).expect("resume expired journal");
    assert!(matches!(outcome, EnrollmentOutcome::Enrolled { .. }));
    assert!(!fixture.journal_path().exists());
    server.finish_one();
}

// REQ-RUNNERENROLL-6
#[test]
fn activation_file_is_created_private_and_never_replaces_an_existing_path() {
    let directory = tempfile::TempDir::new().expect("create directory");
    fs::set_permissions(directory.path(), Permissions::from_mode(0o700))
        .expect("protect directory");
    let artifact = ActivationArtifact {
        schema_version: 1,
        activation_url: format!(
            "http://127.0.0.1:9/v1/runner-enrollments/{ACTIVATION_ID}/activate"
        ),
        activation_token: format!("{ACTIVATION_ID}.{ACTIVATION_SECRET}"),
        runner_id: RUNNER_ID.to_owned(),
        expires_at: future_time(),
    };
    let path = directory.path().join("activation.json");
    let destination = path.to_str().expect("activation path should be UTF-8");

    write_activation_file(destination, &artifact).expect("write activation file");
    let metadata = fs::metadata(&path).expect("activation file metadata");
    assert_eq!(metadata.mode() & 0o777, 0o600);
    assert_eq!(metadata.uid(), rustix::process::geteuid().as_raw());
    let written = fs::read(&path).expect("read activation file");
    assert!(matches!(
        write_activation_file(destination, &artifact),
        Err(EnrollmentError::ArtifactWrite)
    ));
    assert_eq!(fs::read(&path).expect("reread activation file"), written);

    let customer_file = directory.path().join("customer.txt");
    fs::write(&customer_file, b"customer content").expect("write customer file");
    let link = directory.path().join("link.json");
    std::os::unix::fs::symlink(&customer_file, &link).expect("create symbolic link");
    assert!(matches!(
        write_activation_file(link.to_str().expect("link path should be UTF-8"), &artifact),
        Err(EnrollmentError::ArtifactWrite)
    ));
    assert_eq!(
        fs::read(&customer_file).expect("read customer file"),
        b"customer content"
    );
}

// REQ-RUNNERENROLL-20
#[test]
fn enrollment_refuses_an_enrolled_host_and_a_replacement_for_another_runner() {
    let server = ScriptedHttpServer::respond_in_sequence(Vec::new());
    let fixture = EnrollmentFixture::new(&server, OTHER_ACTIVATION_ID, future_time());
    fixture.write_current_state();
    let state = fs::read(&fixture.state_path).expect("read current state");

    assert!(matches!(
        enroll(
            Some(&fixture.activation_path),
            &fixture.config_path,
            false,
            false,
        ),
        Err(EnrollmentError::StateAlreadyEnrolled)
    ));

    let mut other_runner = fixture.artifact(&server, OTHER_ACTIVATION_ID, future_time());
    other_runner.runner_id = "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned();
    let other_runner_path = fixture.root.path().join("other-runner-activation.json");
    write_artifact(&other_runner_path, &other_runner);
    assert!(matches!(
        enroll(Some(&other_runner_path), &fixture.config_path, true, false),
        Err(EnrollmentError::RunnerMismatch)
    ));

    assert_eq!(fs::read(&fixture.state_path).expect("reread state"), state);
    assert!(!fixture.journal_path().exists());
    assert!(server.finish().is_empty());
}

// REQ-RUNNERENROLL-24
#[test]
fn an_unresolved_journal_refuses_a_different_activation_until_resumed() {
    let server = ScriptedHttpServer::respond_in_sequence(vec![
        empty_response("500 Internal Server Error"),
        empty_response("410 Gone"),
        enrollment_response(),
    ]);
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());
    assert!(matches!(
        enroll(
            Some(&fixture.activation_path),
            &fixture.config_path,
            false,
            false,
        ),
        Err(EnrollmentError::NetworkAmbiguous(
            AmbiguousCause::CloudUnavailable(500)
        ))
    ));
    let journal = fs::read(fixture.journal_path()).expect("read unresolved journal");

    let other = fixture.artifact(&server, OTHER_ACTIVATION_ID, future_time());
    let other_path = fixture.root.path().join("other-activation.json");
    write_artifact(&other_path, &other);
    assert!(matches!(
        enroll(Some(&other_path), &fixture.config_path, false, false),
        Err(EnrollmentError::UnresolvedJournal)
    ));
    assert_eq!(
        fs::read(fixture.journal_path()).expect("reread unresolved journal"),
        journal
    );

    assert!(matches!(
        enroll(None, &fixture.config_path, false, true).expect("resume to a definite outcome"),
        EnrollmentOutcome::Gone { .. }
    ));
    assert!(matches!(
        enroll(Some(&other_path), &fixture.config_path, false, false)
            .expect("enroll the different activation after resolution"),
        EnrollmentOutcome::Enrolled { .. }
    ));
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].contains(OTHER_ACTIVATION_ID));
}

struct EnrollmentFixture {
    root: tempfile::TempDir,
    artifact: ActivationArtifact,
    activation_path: PathBuf,
    config_path: PathBuf,
    state_path: PathBuf,
}

impl EnrollmentFixture {
    fn new(server: &ScriptedHttpServer, activation_id: &str, expires_at: String) -> Self {
        let root = tempfile::TempDir::new().expect("create enrollment fixture");
        fs::set_permissions(root.path(), Permissions::from_mode(0o700))
            .expect("protect enrollment fixture");
        let state_path = root.path().join("state/runner-state.json");
        let config_path = root.path().join("runner-config.json");
        let artifact = activation_artifact(server, activation_id, expires_at);
        let activation_path = root.path().join("activation.json");
        write_artifact(&activation_path, &artifact);
        let config = serde_json::json!({
            "schemaVersion": 1,
            "deploymentMode": "development",
            "runnerStatePath": state_path,
            "controlSocketPath": root.path().join("run/runner.sock"),
            "workRoot": root.path().join("work")
        });
        fs::write(
            &config_path,
            serde_json::to_vec_pretty(&config).expect("encode runner config"),
        )
        .expect("write runner config");
        Self {
            root,
            artifact,
            activation_path,
            config_path,
            state_path,
        }
    }

    fn artifact(
        &self,
        server: &ScriptedHttpServer,
        activation_id: &str,
        expires_at: String,
    ) -> ActivationArtifact {
        activation_artifact(server, activation_id, expires_at)
    }

    fn write_current_state(&self) {
        let state_directory = self.state_path.parent().expect("state directory");
        ensure_private_directory(state_directory).expect("create state directory");
        atomic_write_json(
            &self.state_path,
            &serde_json::json!({
                "schemaVersion": 1,
                "runnerId": RUNNER_ID,
                "connectionUrl": "ws://127.0.0.1:8765/v1/runner/connect",
                "currentCredential": {
                    "id": CREDENTIAL_ID,
                    "secret": ACTIVATION_SECRET,
                    "activationId": ACTIVATION_ID,
                    "enrolledAt": "2026-08-06T12:00:00Z"
                },
                "updatedAt": "2026-08-06T12:00:00Z"
            }),
        )
        .expect("write current runner state");
    }

    fn journal_path(&self) -> PathBuf {
        self.state_path
            .parent()
            .expect("state directory")
            .join(JOURNAL_FILE)
    }
}

fn activation_artifact(
    server: &ScriptedHttpServer,
    activation_id: &str,
    expires_at: String,
) -> ActivationArtifact {
    let base = server.api_url.trim_end_matches("/api/");
    ActivationArtifact {
        schema_version: 1,
        activation_url: format!("{base}/v1/runner-enrollments/{activation_id}/activate"),
        activation_token: format!("{activation_id}.{ACTIVATION_SECRET}"),
        runner_id: RUNNER_ID.to_owned(),
        expires_at,
    }
}

fn write_artifact(path: &Path, artifact: &ActivationArtifact) {
    let mut file = create_new_private_file(path).expect("create activation artifact");
    artifact
        .write_json(&mut file)
        .expect("write activation artifact");
    file.sync_all().expect("sync activation artifact");
}

fn enrollment_response() -> Vec<u8> {
    enrollment_response_with_credential(CREDENTIAL_ID)
}

fn enrollment_response_with_credential(credential_id: &str) -> Vec<u8> {
    json_response(&enrollment_response_document_with_credential(credential_id))
}

fn enrollment_response_document() -> serde_json::Value {
    enrollment_response_document_with_credential(CREDENTIAL_ID)
}

fn enrollment_response_document_with_credential(credential_id: &str) -> serde_json::Value {
    serde_json::json!({
        "schemaVersion": 1,
        "runnerId": RUNNER_ID,
        "runnerName": "runner-test",
        "organization": {
            "id": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "displayName": "Test organization"
        },
        "runnerPool": {
            "id": "rpl_01k0z6r1w8f4jy2m7q9v3x5abc",
            "name": "builders"
        },
        "credentialId": credential_id,
        "connectionUrl": "ws://127.0.0.1:8765/v1/runner/connect"
    })
}

fn json_response(value: &serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(value).expect("encode response");
    format!(
        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes()
    .into_iter()
    .chain(body)
    .collect()
}

fn empty_response(status: &str) -> Vec<u8> {
    format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").into_bytes()
}

fn redirect_response(location: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .into_bytes()
}

fn request_body(request: &str) -> &str {
    request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("request should contain a body separator")
}

fn request_header<'a>(request: &'a str, expected_name: &str) -> &'a str {
    request
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case(expected_name)
                .then_some(value.trim())
        })
        .expect("request should contain the expected header")
}

fn future_time() -> String {
    (um_support::utc_now() + time::Duration::days(1))
        .format(&Rfc3339)
        .expect("format future time")
}

fn expired_time() -> String {
    (um_support::utc_now() - time::Duration::days(3))
        .format(&Rfc3339)
        .expect("format expired time")
}

const JOIN_KEY_ID: &str = "rjk_01k0z6r1w8f4jy2m7q9v3x5abc";
const OTHER_JOIN_KEY_ID: &str = "rjk_01k0z6r1w8f4jy2m7q9v3x5abd";
const JOIN_SECRET: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";

fn join_key(server: &ScriptedHttpServer, join_key_id: &str, expires_at: &str) -> JoinKeyArtifact {
    let base = server.api_url.trim_end_matches("/api/");
    JoinKeyArtifact::parse(
        serde_json::json!({
            "schemaVersion": 1,
            "joinUrl": format!("{base}/v1/runner-join-keys/{join_key_id}/join"),
            "joinKeyId": join_key_id,
            "joinToken": format!("{join_key_id}.{JOIN_SECRET}"),
            "expiresAt": expires_at
        })
        .to_string()
        .as_bytes(),
    )
    .expect("parse join key")
}

fn assert_schema_rejects(format: &str, schema_source: &str, document: &serde_json::Value) {
    let schema: serde_json::Value =
        serde_json::from_str(schema_source).expect("decode mirrored runner host schema");
    let validator = jsonschema::draft202012::new(&schema).expect("compile runner host schema");
    assert!(
        !validator.is_valid(document),
        "{format} schema accepted {document}"
    );
}

// REQ-RUNNERENROLL-97
#[test]
fn an_interrupted_join_keeps_a_token_free_journal_and_resumes_the_same_request() {
    let server = ScriptedHttpServer::respond_in_sequence(vec![
        empty_response("400 Bad Request"),
        enrollment_response(),
    ]);
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());

    assert!(matches!(
        join_unless_enrolled(
            &fixture.config_path,
            join_key(&server, JOIN_KEY_ID, &future_time()),
            |_| {},
        ),
        Err(EnrollmentError::InvalidResponse)
    ));
    let journal_bytes = fs::read(fixture.journal_path()).expect("read staged join journal");
    assert_json_matches_schema(
        "runner enrollment journal",
        ENROLLMENT_JOURNAL_SCHEMA,
        &journal_bytes,
    );
    let journal_document: serde_json::Value =
        serde_json::from_slice(&journal_bytes).expect("decode join journal");
    assert_eq!(journal_document["joinKey"]["joinKeyId"], JOIN_KEY_ID);
    assert!(journal_document.get("activationArtifact").is_none());
    assert!(!String::from_utf8_lossy(&journal_bytes).contains(JOIN_SECRET));
    assert!(!fixture.state_path.exists());

    let outcome = join_unless_enrolled(
        &fixture.config_path,
        join_key(&server, JOIN_KEY_ID, &expired_time()),
        |_| {},
    )
    .expect("resume the staged join even after the key's expiry");
    assert!(matches!(outcome, JoinOutcome::Joined(_)));
    assert!(!fixture.journal_path().exists());
    let state_bytes = fs::read(&fixture.state_path).expect("read joined state");
    assert_json_matches_schema("runner state", RUNNER_STATE_SCHEMA, &state_bytes);
    assert!(!String::from_utf8_lossy(&state_bytes).contains(JOIN_SECRET));
    let state: RunnerState = serde_json::from_slice(&state_bytes).expect("decode joined state");
    assert_eq!(
        state.current_credential.join_key_id.as_deref(),
        Some(JOIN_KEY_ID)
    );
    assert_eq!(state.current_credential.activation_id, None);
    let service = load_runner_service_configuration(&fixture.config_path)
        .expect("serve with the joined state");
    assert_eq!(service.credential_id, CREDENTIAL_ID);

    let requests = server.finish();
    assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
    assert_eq!(
        request_header(&requests[0], "idempotency-key"),
        request_header(&requests[1], "idempotency-key")
    );
    for request in &requests {
        assert_eq!(
            request_header(request, "authorization"),
            format!("Bearer {JOIN_KEY_ID}.{JOIN_SECRET}")
        );
    }
}

// REQ-RUNNERENROLL-97, REQ-RUNNERENROLL-24
#[test]
fn a_staged_join_names_its_journal_and_key_to_a_start_with_another_key_or_an_enrollment() {
    let server = ScriptedHttpServer::respond_in_sequence(vec![empty_response("400 Bad Request")]);
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());
    assert!(matches!(
        join_unless_enrolled(
            &fixture.config_path,
            join_key(&server, JOIN_KEY_ID, &future_time()),
            |_| {},
        ),
        Err(EnrollmentError::InvalidResponse)
    ));
    let journal = fs::read(fixture.journal_path()).expect("read staged join journal");
    let staged = |error: EnrollmentError| match error {
        EnrollmentError::JoinJournalForAnotherKey {
            journal,
            join_key_id,
        }
        | EnrollmentError::UnresolvedJoin {
            journal,
            join_key_id,
        } => (journal, join_key_id),
        other => panic!("expected the staged join's facts, got {other:?}"),
    };
    let expected = (fixture.journal_path(), JOIN_KEY_ID.to_owned());

    // The journal was written before the request was sent, so Cloud may have
    // registered a runner from it: another key cannot supersede it.
    let error = join_unless_enrolled(
        &fixture.config_path,
        join_key(&server, OTHER_JOIN_KEY_ID, &future_time()),
        |_| {},
    )
    .expect_err("another key cannot supersede a staged join");
    assert!(matches!(
        error,
        EnrollmentError::JoinJournalForAnotherKey { .. }
    ));
    assert_eq!(staged(error), expected);

    // Enrollment, resumed or with an activation, cannot resolve it either.
    let resumed = enroll(None, &fixture.config_path, false, true)
        .expect_err("enrollment's resume cannot send a join");
    assert!(matches!(resumed, EnrollmentError::UnresolvedJoin { .. }));
    assert_eq!(staged(resumed), expected);
    let activated = enroll(
        Some(&fixture.activation_path),
        &fixture.config_path,
        false,
        false,
    )
    .expect_err("an activation cannot supersede a staged join");
    assert!(matches!(activated, EnrollmentError::UnresolvedJoin { .. }));
    assert_eq!(staged(activated), expected);

    assert_eq!(
        fs::read(fixture.journal_path()).expect("reread staged join journal"),
        journal
    );
    assert!(!fixture.state_path.exists());
    assert_eq!(server.finish().len(), 1);
}

// REQ-RUNNERENROLL-97
#[test]
fn a_resent_join_answered_unauthorized_keeps_and_names_its_journal() {
    let server = ScriptedHttpServer::respond_in_sequence(vec![
        empty_response("400 Bad Request"),
        empty_response("401 Unauthorized"),
    ]);
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());
    assert!(matches!(
        join_unless_enrolled(
            &fixture.config_path,
            join_key(&server, JOIN_KEY_ID, &future_time()),
            |_| {},
        ),
        Err(EnrollmentError::InvalidResponse)
    ));
    let journal = fs::read(fixture.journal_path()).expect("read staged join journal");

    let error = join_unless_enrolled(
        &fixture.config_path,
        join_key(&server, JOIN_KEY_ID, &future_time()),
        |_| {},
    )
    .expect_err("Cloud rejected the resent join's token");

    // The first send may have registered a runner, so the refusal keeps the
    // journal for the join to be completed or cleaned up deliberately.
    assert!(
        fixture.journal_path().exists(),
        "the refusal removed the journal"
    );
    assert_eq!(
        fs::read(fixture.journal_path()).expect("reread staged join journal"),
        journal
    );
    let EnrollmentError::StagedJoinUnauthorized {
        journal,
        join_key_id,
    } = error
    else {
        panic!("expected the staged join's refusal, got {error:?}");
    };
    assert_eq!(
        (journal, join_key_id),
        (fixture.journal_path(), JOIN_KEY_ID.to_owned())
    );
    assert!(!fixture.state_path.exists());
    assert_eq!(server.finish().len(), 2);
}

// REQ-RUNNERENROLL-97, REQ-RUNNERENROLL-113
#[test]
fn a_join_refused_unauthorized_after_an_ambiguous_answer_in_the_same_start_keeps_its_journal() {
    let server = ScriptedHttpServer::respond_in_sequence(vec![
        empty_response("503 Service Unavailable"),
        empty_response("401 Unauthorized"),
    ]);
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());
    let mut retries = Vec::new();

    let error = join_unless_enrolled(
        &fixture.config_path,
        join_key(&server, JOIN_KEY_ID, &future_time()),
        |retry| retries.push(retry),
    )
    .expect_err("Cloud rejected the join's token after an ambiguous answer");

    // The first send may have registered a runner, so the refusal of the
    // second keeps the journal that alone holds its credential secret.
    let EnrollmentError::StagedJoinUnauthorized {
        journal,
        join_key_id,
    } = error
    else {
        panic!("expected the staged join's refusal, got {error:?}");
    };
    assert_eq!(
        (journal, join_key_id),
        (fixture.journal_path(), JOIN_KEY_ID.to_owned())
    );
    let journal_bytes = fs::read(fixture.journal_path()).expect("read kept join journal");
    assert_json_matches_schema(
        "runner enrollment journal",
        ENROLLMENT_JOURNAL_SCHEMA,
        &journal_bytes,
    );
    assert!(!String::from_utf8_lossy(&journal_bytes).contains(JOIN_SECRET));
    assert!(!fixture.state_path.exists());

    let [retry] = retries[..] else {
        panic!("expected exactly one retry, got {retries:?}");
    };
    assert_eq!(retry.cause, AmbiguousCause::CloudUnavailable(503));
    assert!(retry.delay <= Duration::from_secs(1), "{retry:?}");

    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        request_header(&requests[0], "idempotency-key"),
        request_header(&requests[1], "idempotency-key")
    );
    assert_eq!(request_body(&requests[0]), request_body(&requests[1]));
}

// REQ-RUNNERENROLL-97
#[test]
fn a_join_refused_gone_clears_its_journal_and_says_whether_it_was_resent() {
    let server = ScriptedHttpServer::respond_in_sequence(vec![
        empty_response("410 Gone"),
        empty_response("400 Bad Request"),
        empty_response("410 Gone"),
    ]);
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());
    let start = || {
        join_unless_enrolled(
            &fixture.config_path,
            join_key(&server, JOIN_KEY_ID, &future_time()),
            |_| {},
        )
    };

    // A first send refused: no earlier send can have registered a runner.
    assert!(matches!(
        start(),
        Err(EnrollmentError::JoinRefused {
            refusal: JoinRefusal::Gone,
            resent: false,
        })
    ));
    assert!(!fixture.journal_path().exists());
    assert!(!fixture.state_path.exists());

    // A staged join resent by the next start and refused: the earlier send may
    // have registered a runner that Cloud has since deleted.
    assert!(matches!(start(), Err(EnrollmentError::InvalidResponse)));
    assert!(fixture.journal_path().exists());
    assert!(matches!(
        start(),
        Err(EnrollmentError::JoinRefused {
            refusal: JoinRefusal::Gone,
            resent: true,
        })
    ));
    assert!(!fixture.journal_path().exists());
    assert!(!fixture.state_path.exists());
    assert_eq!(server.finish().len(), 3);
}

// REQ-RUNNERENROLL-97
#[test]
fn a_staged_join_refused_unauthorized_names_its_journal_without_the_resend_remedy() {
    let journal = PathBuf::from("/var/lib/um/runner-state.join.json");
    let resend = ResendRemedy {
        journal: &journal,
        join_key_id: JOIN_KEY_ID,
    }
    .to_string();
    let another_key = EnrollmentError::JoinJournalForAnotherKey {
        journal: journal.clone(),
        join_key_id: JOIN_KEY_ID.to_owned(),
    }
    .to_string();
    let unauthorized = EnrollmentError::StagedJoinUnauthorized {
        journal: journal.clone(),
        join_key_id: JOIN_KEY_ID.to_owned(),
    }
    .to_string();

    assert!(another_key.contains(&resend));
    assert!(unauthorized.contains(&journal.display().to_string()));
    assert!(unauthorized.contains(JOIN_KEY_ID));
    // Cloud just rejected the token this start gave, so resending it with the
    // same key cannot be the remedy.
    assert!(!unauthorized.contains(&resend));
}

// REQ-RUNNERENROLL-97
#[test]
fn an_enrolled_host_removes_a_join_journal_its_state_already_holds() {
    let server = ScriptedHttpServer::respond_in_sequence(Vec::new());
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());
    let key = join_key(&server, JOIN_KEY_ID, &future_time());
    let state_directory = fixture.state_path.parent().expect("state directory");
    ensure_private_directory(state_directory).expect("create state directory");
    let journal =
        stage_join(&fixture.journal_path(), &key, DeploymentMode::Development).expect("stage join");
    let response: EnrollmentResponse =
        serde_json::from_value(enrollment_response_document()).expect("decode response");
    persist_enrollment_state(
        &fixture.state_path,
        &journal,
        &response,
        DeploymentMode::Development,
    )
    .expect("persist joined state");

    assert!(matches!(
        join_unless_enrolled(&fixture.config_path, key, |_| {}),
        Ok(JoinOutcome::AlreadyEnrolled)
    ));
    assert!(!fixture.journal_path().exists());
    assert!(server.finish().is_empty());
}

// REQ-RUNNERENROLL-97
#[test]
fn host_files_name_exactly_one_activation_or_join_key() {
    let credential = |origin: serde_json::Value| {
        let mut credential = serde_json::json!({
            "id": CREDENTIAL_ID,
            "secret": ACTIVATION_SECRET,
            "enrolledAt": "2026-08-06T12:00:00Z"
        });
        credential
            .as_object_mut()
            .expect("credential object")
            .extend(origin.as_object().expect("origin object").clone());
        serde_json::json!({
            "schemaVersion": 1,
            "runnerId": RUNNER_ID,
            "connectionUrl": "ws://127.0.0.1:8765/v1/runner/connect",
            "currentCredential": credential,
            "updatedAt": "2026-08-06T12:00:00Z"
        })
    };
    let root = tempfile::TempDir::new().expect("create state fixture");
    fs::set_permissions(root.path(), Permissions::from_mode(0o700)).expect("protect fixture");
    let state_path = root.path().join("runner-state.json");
    for (origin, valid) in [
        (serde_json::json!({"activationId": ACTIVATION_ID}), true),
        (serde_json::json!({"joinKeyId": JOIN_KEY_ID}), true),
        (
            serde_json::json!({"activationId": ACTIVATION_ID, "joinKeyId": JOIN_KEY_ID}),
            false,
        ),
        (serde_json::json!({}), false),
    ] {
        let state = credential(origin);
        let bytes = serde_json::to_vec(&state).expect("encode state");
        if valid {
            assert_json_matches_schema("runner state", RUNNER_STATE_SCHEMA, &bytes);
        } else {
            assert_schema_rejects("runner state", RUNNER_STATE_SCHEMA, &state);
        }
        let _ = fs::remove_file(&state_path);
        atomic_write_json(&state_path, &state).expect("write state");
        assert_eq!(
            read_runner_state_if_present(&state_path, DeploymentMode::Development).is_ok(),
            valid,
            "{state}"
        );
    }

    let journal = |target: serde_json::Value| {
        let mut journal = serde_json::json!({
            "schemaVersion": 1,
            "credentialSecret": ACTIVATION_SECRET,
            "credentialSecretVerifier": ACTIVATION_SECRET,
            "idempotencyKey": "join-idempotency",
            "replaceCredential": false,
            "stagedAt": "2026-08-06T12:00:00Z"
        });
        journal
            .as_object_mut()
            .expect("journal object")
            .extend(target.as_object().expect("target object").clone());
        journal
    };
    let join_target = serde_json::json!({"joinKey": {
        "joinUrl": format!("https://api.usefulmachinery.com/v1/runner-join-keys/{JOIN_KEY_ID}/join"),
        "joinKeyId": JOIN_KEY_ID,
        "expiresAt": "2026-08-07T12:00:00Z"
    }});
    let activation_target = serde_json::json!({"activationArtifact": {
        "schemaVersion": 1,
        "activationUrl": format!("https://api.usefulmachinery.com/v1/runner-enrollments/{ACTIVATION_ID}/activate"),
        "activationToken": format!("{ACTIVATION_ID}.{ACTIVATION_SECRET}"),
        "runnerId": RUNNER_ID,
        "expiresAt": "2026-08-07T12:00:00Z"
    }});
    let mut both = activation_target.clone();
    both.as_object_mut()
        .expect("target object")
        .extend(join_target.as_object().expect("join object").clone());
    for (target, valid) in [
        (activation_target, true),
        (join_target, true),
        (both, false),
        (serde_json::json!({}), false),
    ] {
        let document = journal(target);
        if valid {
            assert_json_matches_schema(
                "runner enrollment journal",
                ENROLLMENT_JOURNAL_SCHEMA,
                &serde_json::to_vec(&document).expect("encode journal"),
            );
        } else {
            assert_schema_rejects(
                "runner enrollment journal",
                ENROLLMENT_JOURNAL_SCHEMA,
                &document,
            );
        }
        assert_eq!(
            serde_json::from_value::<EnrollmentJournal>(document.clone()).is_ok(),
            valid,
            "{document}"
        );
    }
}

// REQ-RUNNERENROLL-106
#[test]
fn a_service_load_that_cannot_take_the_state_lock_names_the_lock() {
    let server = ScriptedHttpServer::respond_in_sequence(Vec::new());
    let fixture = EnrollmentFixture::new(&server, ACTIVATION_ID, future_time());
    fixture.write_current_state();
    let state_directory = fixture.state_path.parent().expect("state directory");
    let lock = acquire_state_lock(&state_directory.join(LOCK_FILE)).expect("hold the state lock");

    assert_eq!(
        crate::Config::load(&fixture.config_path).map(|_| ()),
        Err(crate::ConfigError::StateLockUnavailable),
    );

    drop(lock);
    assert_eq!(
        crate::Config::load(&fixture.config_path).map(|_| ()),
        Err(crate::ConfigError::WorkRootUnavailable),
    );
    assert!(server.finish().is_empty());
}

// REQ-RUNNERENROLL-106
#[test]
fn an_unavailable_state_lock_renders_as_the_enrollment_lock_failure() {
    assert_eq!(
        crate::ConfigError::StateLockUnavailable.to_string(),
        EnrollmentError::StateLock.to_string(),
    );
    assert_ne!(
        crate::ConfigError::StateLockUnavailable.to_string(),
        crate::ConfigError::InvalidOperatorConfiguration.to_string(),
    );
}
