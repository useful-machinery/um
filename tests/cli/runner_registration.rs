//! `um runner serve --personal` and `--pool-id` on a host with no enrolled
//! state: the real `um` against a scripted API, a scripted enrollment
//! endpoint and a connection witness for the runner endpoint.

use super::*;

use std::path::PathBuf;

use super::runner_administration::{
    ACTIVATION_ID, ACTIVATION_SECRET, MEMBER_ID, ORGANIZATION, ORGANIZATION_ID, POOL_ID, RUNNER_ID,
    activation_issuance_body, activation_issued_response_with, current_principal_response,
    pool_list_body, prepared_runner, registration_created_response, request_body,
};
use super::runner_enrollment::{
    ACTIVATION_ID as STATE_ACTIVATION_ID, CURRENT_CREDENTIAL_ID, CURRENT_SECRET, ConnectionWitness,
    JOIN_KEY_VARIABLE, JoinHost, future_expiry, join_base, join_key_artifact, join_response_body,
    verifier_of, write_private_json,
};

const REGISTRATION_FILE: &str = ".runner-registration.json";

/// An issuance whose artifact the host can enroll with: it points at
/// `enrollment`'s activation endpoint and has not expired.
fn activation_issued_for(enrollment: &api_test_support::ScriptedHttpServer) -> Vec<u8> {
    let mut issuance = activation_issuance_body();
    issuance["activation"]["expiresAt"] = future_expiry().into();
    issuance["artifact"]["expiresAt"] = future_expiry().into();
    issuance["artifact"]["activationUrl"] = format!(
        "{}/v1/runner-enrollments/{ACTIVATION_ID}/activate",
        join_base(enrollment)
    )
    .into();
    activation_issued_response_with(&issuance)
}

/// Cloud's 201 to the registered runner's enrollment.
fn enrolled_response(connection_url: &str) -> Vec<u8> {
    let mut enrolled = join_response_body(0, connection_url);
    enrolled["runnerId"] = RUNNER_ID.into();
    json_http_response("201 Created", enrolled)
}

/// A signed-in Cloud: the API answers `before` and then the activation
/// issuance, the enrollment endpoint answers the registered runner's
/// enrollment, and the witness takes the runner's connection.
struct SignedInCloud {
    witness: ConnectionWitness,
    enrollment: api_test_support::ScriptedHttpServer,
    api: ScriptedServer,
    _credentials: tempfile::TempDir,
    credential_path: String,
}

impl SignedInCloud {
    fn new(before: Vec<Vec<u8>>) -> Self {
        let witness = ConnectionWitness::new();
        let enrollment =
            api_test_support::ScriptedHttpServer::respond(enrolled_response(&witness.url));
        let mut responses = before;
        responses.push(activation_issued_for(&enrollment));
        let (api, credentials, credential_path) = prepared_runner(responses);
        Self {
            witness,
            enrollment,
            api,
            _credentials: credentials,
            credential_path,
        }
    }

    fn environment(&self) -> [(&'static str, &str); 6] {
        deployment_environment(&self.api.api_url, &self.credential_path)
    }

    /// Starts `host` with `arguments` and waits for its runner to connect;
    /// answers the connection request and the start's standard error.
    fn serve_until_connected(&self, host: &JoinHost, arguments: &[&str]) -> (String, String) {
        let mut serve = host.serve(arguments, &self.environment());
        let connection = self.witness.next_connection(&mut serve);
        let stderr = serve.stop();
        let connection = connection
            .unwrap_or_else(|| panic!("Runner Serve stopped before connecting: {stderr}"));
        (connection, stderr)
    }

    /// The API requests in the order they arrived, and the enrollment.
    fn finish(self) -> (Vec<String>, String) {
        (self.api.finish(), self.enrollment.finish_one())
    }
}

/// A host with an operator configuration and no state; its join key file
/// names a Cloud that is never contacted.
fn registration_host() -> JoinHost {
    JoinHost::new(&join_key_artifact("http://127.0.0.1:1", &future_expiry()))
}

fn record_path(host: &JoinHost) -> PathBuf {
    host.state.parent().unwrap().join(REGISTRATION_FILE)
}

fn record_document(host: &JoinHost) -> serde_json::Value {
    serde_json::from_slice(&fs::read(record_path(host)).unwrap()).unwrap()
}

/// A Cloud address that records whether anything connected to it.
struct SilentCloud {
    listener: TcpListener,
    api_url: String,
}

impl SilentCloud {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        Self { listener, api_url }
    }

    fn assert_untouched(&self) {
        self.listener.set_nonblocking(true).unwrap();
        assert_eq!(
            self.listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
}

fn files_under(directory: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(files_under(&path));
        } else {
            files.push(path);
        }
    }
    files
}

fn assert_signed_in_registration_request(request: &str) -> serde_json::Value {
    assert!(
        request.starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations HTTP/1.1\r\n"
        )),
        "{request}"
    );
    assert!(!header_value(request, "idempotency-key").is_empty());
    let body: serde_json::Value = serde_json::from_str(request_body(request)).unwrap();
    assert!(body.get("memberId").is_none(), "{body}");
    assert!(body.get("name").is_none(), "{body}");
    body
}

fn assert_activation_and_enrollment(activation: &str, enrollment: &str) {
    assert!(
        activation.starts_with(&format!(
            "POST /api/v1/organizations/{ORGANIZATION}/runner-registrations/{RUNNER_ID}/activations HTTP/1.1\r\n"
        )),
        "{activation}"
    );
    assert!(
        enrollment.starts_with(&format!(
            "POST /v1/runner-enrollments/{ACTIVATION_ID}/activate HTTP/1.1\r\n"
        )),
        "{enrollment}"
    );
    assert_eq!(
        header_value(enrollment, "authorization"),
        format!("Bearer {ACTIVATION_ID}.{ACTIVATION_SECRET}")
    );
}

/// The host serves as the registered runner with its own credential, and
/// nothing on it or on standard error holds the activation token.
fn assert_enrolled_and_connected(host: &JoinHost, connection: &str, stderr: &str) {
    let state = host.state_document();
    assert_eq!(state["runnerId"], RUNNER_ID);
    assert_eq!(state["currentCredential"]["activationId"], ACTIVATION_ID);
    let credential_id = state["currentCredential"]["id"].as_str().unwrap();
    let secret = state["currentCredential"]["secret"].as_str().unwrap();
    assert_eq!(
        header_value(connection, "authorization"),
        format!("Bearer {credential_id}.{secret}")
    );
    assert!(stderr.contains(RUNNER_ID), "{stderr}");
    assert!(!stderr.contains(ACTIVATION_SECRET), "{stderr}");
    assert!(!stderr.contains(secret), "{stderr}");
    assert!(!host.journal.exists());
    for file in files_under(host.root.path()) {
        let bytes = fs::read(&file).unwrap();
        assert!(
            !bytes
                .windows(ACTIVATION_SECRET.len())
                .any(|part| part == ACTIVATION_SECRET.as_bytes()),
            "{} holds the activation token",
            file.display()
        );
    }
    let record = record_document(host);
    assert_eq!(record["runner"]["id"], RUNNER_ID);
    assert!(record.get("enrolledAt").is_some(), "{record}");
}

// REQ-RUNNERENROLL-116, REQ-RUNNERENROLL-115
#[test]
fn personal_registers_enrolls_and_serves_a_host_with_no_state() {
    let cloud = SignedInCloud::new(vec![
        current_principal_response(),
        registration_created_response(),
    ]);
    let host = registration_host();

    let (connection, stderr) = cloud.serve_until_connected(
        &host,
        &[
            "--personal",
            "--organization",
            ORGANIZATION,
            "--allow-insecure-http",
        ],
    );

    assert_enrolled_and_connected(&host, &connection, &stderr);
    let (requests, enrollment) = cloud.finish();
    assert_eq!(requests.len(), 3, "{requests:?}");
    assert!(
        requests[0].starts_with("GET /api/v1/me HTTP/1.1\r\n"),
        "{}",
        requests[0]
    );
    let body = assert_signed_in_registration_request(&requests[1]);
    assert!(body.get("runnerPoolId").is_none(), "{body}");
    assert_activation_and_enrollment(&requests[2], &enrollment);
}

// REQ-RUNNERENROLL-115
#[test]
fn pool_id_resolves_the_named_pool_once_and_registers_in_it() {
    let cloud = SignedInCloud::new(vec![
        current_principal_response(),
        json_http_response("200 OK", pool_list_body()),
        registration_created_response(),
    ]);
    let host = registration_host();

    let (connection, stderr) = cloud.serve_until_connected(
        &host,
        &[
            "--pool-id",
            "builders",
            "--organization",
            ORGANIZATION,
            "--allow-insecure-http",
        ],
    );

    assert_enrolled_and_connected(&host, &connection, &stderr);
    let (requests, enrollment) = cloud.finish();
    assert_eq!(requests.len(), 4, "{requests:?}");
    assert!(requests[0].starts_with("GET /api/v1/me HTTP/1.1\r\n"));
    assert!(requests[1].starts_with(&format!(
        "GET /api/v1/organizations/{ORGANIZATION}/runner-pools?limit=200 HTTP/1.1\r\n"
    )));
    let body = assert_signed_in_registration_request(&requests[2]);
    assert_eq!(body["runnerPoolId"], POOL_ID);
    assert_activation_and_enrollment(&requests[3], &enrollment);
}

// REQ-RUNNERENROLL-116
#[test]
fn a_start_with_no_sign_in_exits_3_having_registered_and_written_nothing() {
    let cloud = SilentCloud::new();
    let credentials = private_credential_directory();
    let credential_path = credentials
        .path()
        .join("credentials.json")
        .to_string_lossy()
        .into_owned();
    let host = registration_host();

    let (exit, stderr) = host
        .serve(
            &[
                "--personal",
                "--organization",
                ORGANIZATION,
                "--allow-insecure-http",
            ],
            &deployment_environment(&cloud.api_url, &credential_path),
        )
        .wait_for_exit();

    assert_eq!(exit.code(), Some(3), "{stderr}");
    assert!(
        stderr.lines().any(|line| line.trim() == "um auth login"),
        "{stderr}"
    );
    assert!(!record_path(&host).exists());
    assert!(!host.state.exists());
    cloud.assert_untouched();
}

// REQ-RUNNERENROLL-116: without --allow-insecure-http the registration's
// requests refuse a plain-HTTP API address, so the sign-in is sent nowhere.
#[test]
fn a_plain_http_api_address_is_refused_without_allow_insecure_http() {
    let cloud = SignedInSilentCloud::new();
    let host = registration_host();

    let (exit, stderr) = host
        .serve(
            &["--personal", "--organization", ORGANIZATION],
            &cloud.environment(),
        )
        .wait_for_exit();

    assert!(!exit.success(), "{stderr}");
    assert!(!record_path(&host).exists());
    assert!(!host.state.exists());
    cloud.cloud.assert_untouched();
}

// REQ-RUNNERENROLL-115
#[test]
fn conflicting_or_incomplete_registration_modes_are_usage_errors_that_send_nothing() {
    let cloud = SilentCloud::new();
    let credentials = private_credential_directory();
    let credential_path = credentials.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &cloud.api_url,
        "unique-registration-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path = credential_path.to_string_lossy().into_owned();
    let deployment = deployment_environment(&cloud.api_url, &credential_path);
    let host = registration_host();
    let key_file = host.key_file.to_string_lossy().into_owned();
    let artifact = fs::read_to_string(&host.key_file).unwrap();
    let mut with_variable = deployment.to_vec();
    with_variable.push((JOIN_KEY_VARIABLE, &artifact));

    let personal = ["--personal", "--organization", ORGANIZATION];
    let pool = ["--pool-id", "builders", "--organization", ORGANIZATION];
    // Each case: the arguments after --config and the environment.
    type Case<'a> = (Vec<&'a str>, &'a [(&'a str, &'a str)]);
    let mut cases: Vec<Case<'_>> = vec![
        (
            vec![
                "--personal",
                "--pool-id",
                "builders",
                "--organization",
                ORGANIZATION,
            ],
            &deployment,
        ),
        (vec!["--personal"], &deployment),
        (vec!["--pool-id", "builders"], &deployment),
        (vec!["--organization", ORGANIZATION], &deployment),
        (vec!["--allow-insecure-http"], &deployment),
    ];
    for mode in [&personal[..], &pool[..]] {
        cases.push((
            [mode, &["--join-key-file", key_file.as_str()][..]].concat(),
            &deployment,
        ));
        cases.push(([mode, &["--join-key"][..]].concat(), &deployment));
        cases.push((mode.to_vec(), with_variable.as_slice()));
    }

    for (arguments, environment) in cases {
        let (exit, stderr) = host.serve(&arguments, environment).wait_for_exit();

        assert_eq!(exit.code(), Some(2), "{arguments:?}: {stderr}");
    }
    assert!(!host.state.parent().unwrap().exists());
    cloud.assert_untouched();
}

// REQ-RUNNERENROLL-116
#[test]
fn a_host_without_a_work_root_fails_with_its_configuration_before_registering() {
    let cloud = SilentCloud::new();
    let credentials = private_credential_directory();
    let credential_path = credentials.path().join("credentials.json");
    write_credential_fixture(
        &credential_path,
        &cloud.api_url,
        "unique-registration-token",
        "2999-01-01T00:00:00Z",
    );
    let credential_path = credential_path.to_string_lossy().into_owned();
    let host = registration_host();
    fs::remove_dir(host.root.path().join("work")).unwrap();

    let (exit, stderr) = host
        .serve(
            &[
                "--personal",
                "--organization",
                ORGANIZATION,
                "--allow-insecure-http",
            ],
            &deployment_environment(&cloud.api_url, &credential_path),
        )
        .wait_for_exit();

    assert_eq!(exit.code(), Some(1), "{stderr}");
    assert!(stderr.contains(&host.config), "{stderr}");
    assert!(!record_path(&host).exists());
    assert!(!host.state.exists());
    cloud.assert_untouched();
}

// REQ-RUNNERENROLL-117
#[test]
fn an_ambiguous_registration_is_resent_with_the_same_key_and_completes() {
    let cloud = SignedInCloud::new(vec![
        current_principal_response(),
        http_response("503 Service Unavailable", None, b""),
        current_principal_response(),
        registration_created_response(),
    ]);
    let host = registration_host();
    let arguments = [
        "--personal",
        "--organization",
        ORGANIZATION,
        "--allow-insecure-http",
    ];

    let (exit, stderr) = host.serve(&arguments, &cloud.environment()).wait_for_exit();

    assert!(!exit.success(), "{stderr}");
    assert!(
        stderr.lines().any(|line| line == "code: unreachable"),
        "{stderr}"
    );
    assert!(
        stderr.contains(&record_path(&host).display().to_string()),
        "{stderr}"
    );
    let record = record_document(&host);
    assert!(record.get("runner").is_none(), "{record}");
    assert!(!host.state.exists());

    let (connection, stderr) = cloud.serve_until_connected(&host, &arguments);

    assert_enrolled_and_connected(&host, &connection, &stderr);
    let (requests, enrollment) = cloud.finish();
    assert_eq!(requests.len(), 5, "{requests:?}");
    let first = assert_signed_in_registration_request(&requests[1]);
    let resent = assert_signed_in_registration_request(&requests[3]);
    assert_eq!(resent, first);
    assert_eq!(
        header_value(&requests[3], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert_eq!(
        header_value(&requests[1], "idempotency-key"),
        record["idempotencyKey"]
    );
    assert_activation_and_enrollment(&requests[4], &enrollment);
}

// REQ-RUNNERENROLL-116
#[test]
fn a_refused_registration_exits_with_forbidden_and_leaves_nothing() {
    let (server, _credentials, credential_path) = prepared_runner(vec![
        current_principal_response(),
        forbidden_problem_response(),
    ]);
    let host = registration_host();

    let (exit, stderr) = host
        .serve(
            &[
                "--personal",
                "--organization",
                ORGANIZATION,
                "--allow-insecure-http",
            ],
            &deployment_environment(&server.api_url, &credential_path),
        )
        .wait_for_exit();

    assert_eq!(exit.code(), Some(1), "{stderr}");
    assert!(
        stderr.lines().any(|line| line == "code: forbidden"),
        "{stderr}"
    );
    assert!(!record_path(&host).exists());
    assert!(!host.state.exists());
    let requests = server.finish();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_signed_in_registration_request(&requests[1]);
}

/// A registration record for `RUNNER_ID` in the personal pool of
/// `ORGANIZATION`, complete unless `enrolled` is false.
fn personal_record(enrolled: bool) -> serde_json::Value {
    let mut record = serde_json::json!({
        "schemaVersion": 1,
        "deployment": "https://api.usefulmachinery.com/",
        "organization": ORGANIZATION,
        "target": {"kind": "personal"},
        "principalId": MEMBER_ID,
        "idempotencyKey": "registration-idempotency",
        "stagedAt": "2026-11-13T12:00:00Z",
        "runner": {"id": RUNNER_ID, "organizationId": ORGANIZATION_ID, "poolId": POOL_ID}
    });
    if enrolled {
        record["enrolledAt"] = "2026-11-13T12:00:05Z".into();
    }
    record
}

fn create_state_directory(host: &JoinHost) {
    let directory = host.state.parent().unwrap();
    fs::create_dir(directory).unwrap();
    fs::set_permissions(directory, Permissions::from_mode(0o700)).unwrap();
}

/// Writes `record` on `host`, whose state directory exists.
fn write_record(host: &JoinHost, record: &serde_json::Value) {
    write_private_json(&record_path(host), record);
}

/// A host enrolled as `RUNNER_ID`, connecting to `connection_url`, whose
/// registration record says `--personal --organization ORGANIZATION` made it.
fn registered_host(connection_url: &str) -> JoinHost {
    let host = registration_host();
    host.write_enrolled_state(connection_url);
    write_record(&host, &personal_record(true));
    host
}

/// A deployment whose API records whether anything reached it, with a
/// sign-in, for starts that must not contact it.
struct SignedInSilentCloud {
    cloud: SilentCloud,
    _credentials: tempfile::TempDir,
    credential_path: String,
}

impl SignedInSilentCloud {
    fn new() -> Self {
        let cloud = SilentCloud::new();
        let credentials = private_credential_directory();
        let credential_path = credentials.path().join("credentials.json");
        write_credential_fixture(
            &credential_path,
            &cloud.api_url,
            "unique-registration-token",
            "2999-01-01T00:00:00Z",
        );
        Self {
            cloud,
            _credentials: credentials,
            credential_path: credential_path.to_string_lossy().into_owned(),
        }
    }

    fn environment(&self) -> [(&'static str, &str); 6] {
        deployment_environment(&self.cloud.api_url, &self.credential_path)
    }
}

/// The host documents a start must leave as they were: the state, the
/// registration record and the enrollment journal, each when present. The
/// service's own files beside them are not among them.
fn host_files(host: &JoinHost) -> Vec<(PathBuf, Option<Vec<u8>>)> {
    [host.state.clone(), record_path(host), host.journal.clone()]
        .into_iter()
        .map(|path| {
            let bytes = fs::read(&path).ok();
            (path, bytes)
        })
        .collect()
}

fn assert_code(stderr: &str, code: &str) {
    assert!(
        stderr.lines().any(|line| line == format!("code: {code}")),
        "{stderr}"
    );
}

const PERSONAL: [&str; 4] = [
    "--personal",
    "--organization",
    ORGANIZATION,
    "--allow-insecure-http",
];

// REQ-RUNNERENROLL-118, INV-RUNNER-27
#[test]
fn a_registered_host_starts_again_with_its_options_and_no_sign_in_as_the_same_runner() {
    let cloud = SignedInCloud::new(vec![
        current_principal_response(),
        registration_created_response(),
    ]);
    let host = registration_host();
    let (first, _) = cloud.serve_until_connected(&host, &PERSONAL);
    let files = host_files(&host);

    // No credentials file and no deployment variables: the start reads no
    // human sign-in or deployment.
    let mut serve = host.serve(&PERSONAL, &[]);
    let connection = cloud.witness.next_connection(&mut serve);
    let stderr = serve.stop();
    let connection =
        connection.unwrap_or_else(|| panic!("Runner Serve stopped before connecting: {stderr}"));

    assert_eq!(
        header_value(&connection, "authorization"),
        header_value(&first, "authorization")
    );
    assert_eq!(host_files(&host), files);
    let (requests, _) = cloud.finish();
    assert_eq!(requests.len(), 3, "{requests:?}");
}

// REQ-RUNNERENROLL-118
#[test]
fn a_registered_host_started_with_another_target_fails_and_sends_nothing() {
    let witness = ConnectionWitness::new();
    let host = registered_host(&witness.url);
    let files = host_files(&host);
    let cloud = SignedInSilentCloud::new();

    for arguments in [
        &[
            "--pool-id",
            "other",
            "--organization",
            ORGANIZATION,
            "--allow-insecure-http",
        ][..],
        &[
            "--personal",
            "--organization",
            "other-organization",
            "--allow-insecure-http",
        ][..],
    ] {
        let (exit, stderr) = host.serve(arguments, &cloud.environment()).wait_for_exit();

        assert_eq!(exit.code(), Some(1), "{arguments:?}: {stderr}");
        assert_code(&stderr, "registration_target_conflict");
        assert_eq!(host_files(&host), files, "{arguments:?}");
    }
    cloud.cloud.assert_untouched();
}

// REQ-RUNNERENROLL-118
#[test]
fn a_host_enrolled_with_an_activation_refuses_a_signed_in_mode() {
    let witness = ConnectionWitness::new();
    let host = registration_host();
    host.write_enrolled_state(&witness.url);
    let files = host_files(&host);
    let cloud = SignedInSilentCloud::new();

    let (exit, stderr) = host.serve(&PERSONAL, &cloud.environment()).wait_for_exit();

    assert_eq!(exit.code(), Some(1), "{stderr}");
    assert_code(&stderr, "registration_target_conflict");
    assert_eq!(host_files(&host), files);
    cloud.cloud.assert_untouched();
}

// REQ-RUNNERENROLL-118
#[test]
fn a_registered_host_whose_credential_is_rejected_registers_nothing() {
    let witness = ConnectionWitness::rejecting();
    let host = registered_host(&witness.url);
    let files = host_files(&host);
    let cloud = SignedInSilentCloud::new();

    let mut serve = host.serve(&PERSONAL, &cloud.environment());
    let connection = witness.next_connection(&mut serve);
    let stderr = serve.stop();

    assert!(connection.is_some(), "{stderr}");
    assert_eq!(host_files(&host), files);
    cloud.cloud.assert_untouched();
}

// REQ-RUNNERENROLL-118
#[test]
fn a_signed_in_start_beside_a_staged_join_and_a_join_beside_a_record_are_unresolved() {
    let join = SilentCloud::new();
    let join_base = join.api_url.trim_end_matches("/api").to_owned();
    let cloud = SignedInSilentCloud::new();

    let staged_join = JoinHost::new(&join_key_artifact(&join_base, &future_expiry()));
    create_state_directory(&staged_join);
    staged_join.write_join_journal(&join_base, CURRENT_SECRET);
    let recorded = JoinHost::new(&join_key_artifact(&join_base, &future_expiry()));
    let mut pending = personal_record(false);
    pending.as_object_mut().unwrap().remove("runner");
    create_state_directory(&recorded);
    write_record(&recorded, &pending);
    let key_file = recorded.key_file.to_string_lossy().into_owned();

    for (host, arguments, environment) in [
        (&staged_join, &PERSONAL[..], &cloud.environment()[..]),
        (
            &recorded,
            &["--join-key-file", key_file.as_str()][..],
            &[][..],
        ),
    ] {
        let files = host_files(host);

        let (exit, stderr) = host.serve(arguments, environment).wait_for_exit();

        assert_eq!(exit.code(), Some(1), "{arguments:?}: {stderr}");
        assert_code(&stderr, "registration_unresolved");
        assert_eq!(host_files(host), files, "{arguments:?}");
        assert!(!host.state.exists());
    }
    cloud.cloud.assert_untouched();
    join.assert_untouched();
}

// REQ-RUNNERENROLL-118
#[test]
fn a_complete_record_without_state_fails_and_sends_nothing() {
    let host = registration_host();
    create_state_directory(&host);
    write_record(&host, &personal_record(true));
    let files = host_files(&host);
    let cloud = SignedInSilentCloud::new();

    let (exit, stderr) = host.serve(&PERSONAL, &cloud.environment()).wait_for_exit();

    assert_eq!(exit.code(), Some(1), "{stderr}");
    assert_code(&stderr, "registration_state_missing");
    assert_eq!(host_files(&host), files);
    assert!(!host.state.exists());
    cloud.cloud.assert_untouched();
}

// REQ-RUNNERENROLL-118
#[test]
fn an_enrolled_host_completes_its_record_and_removes_the_consumed_activation() {
    let witness = ConnectionWitness::new();
    let host = registration_host();
    host.write_enrolled_state(&witness.url);
    write_record(&host, &personal_record(false));
    // The activation journal whose credential the state already holds, as an
    // interrupted registration leaves it.
    write_private_json(
        &host.journal,
        &serde_json::json!({
            "schemaVersion": 1,
            "activationArtifact": {
                "schemaVersion": 1,
                "activationUrl": format!(
                    "http://127.0.0.1:1/v1/runner-enrollments/{STATE_ACTIVATION_ID}/activate"
                ),
                "activationToken": format!("{STATE_ACTIVATION_ID}.{ACTIVATION_SECRET}"),
                "runnerId": RUNNER_ID,
                "expiresAt": future_expiry()
            },
            "credentialSecret": CURRENT_SECRET,
            "credentialSecretVerifier": verifier_of(CURRENT_SECRET),
            "idempotencyKey": "0".repeat(64),
            "replaceCredential": false,
            "stagedAt": "2026-11-13T12:00:02Z"
        }),
    );
    let state = fs::read(&host.state).unwrap();

    let mut serve = host.serve(&PERSONAL, &[]);
    let connection = witness.next_connection(&mut serve);
    let stderr = serve.stop();
    let connection =
        connection.unwrap_or_else(|| panic!("Runner Serve stopped before connecting: {stderr}"));

    assert_eq!(
        header_value(&connection, "authorization"),
        format!("Bearer {CURRENT_CREDENTIAL_ID}.{CURRENT_SECRET}")
    );
    assert_eq!(fs::read(&host.state).unwrap(), state);
    assert!(!host.journal.exists());
    let mut expected = personal_record(false);
    let record = record_document(&host);
    assert!(record.get("enrolledAt").is_some(), "{record}");
    expected["enrolledAt"] = record["enrolledAt"].clone();
    assert_eq!(record, expected);
}
