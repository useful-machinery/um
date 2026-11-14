use super::*;

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::process::{Child, ExitStatus};

use base64::Engine as _;

const RUNNER_ID: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
pub(super) const CURRENT_CREDENTIAL_ID: &str = "rrc_01k0z6r1w8f4jy2m7q9v3x5abc";
const PENDING_CREDENTIAL_ID: &str = "rrc_01k0z6r1w8f4jy2m7q9v3x5abd";
pub(super) const ACTIVATION_ID: &str = "rna_01k0z6r1w8f4jy2m7q9v3x5abd";
pub(super) const CURRENT_SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const PENDING_SECRET: &str = "AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI";

struct PendingEnrollmentFixture {
    _root: tempfile::TempDir,
    _runtime: tempfile::TempDir,
    config: String,
    activation: String,
    state: std::path::PathBuf,
    socket: std::path::PathBuf,
}

impl PendingEnrollmentFixture {
    fn new() -> Self {
        let root = private_credential_directory();
        let runtime = tempfile::tempdir_in("/tmp").unwrap();
        fs::set_permissions(runtime.path(), Permissions::from_mode(0o700)).unwrap();
        let state_directory = root.path().join("state");
        fs::create_dir(&state_directory).unwrap();
        fs::set_permissions(&state_directory, Permissions::from_mode(0o700)).unwrap();
        let state = state_directory.join("runner-state.json");
        fs::write(
            &state,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schemaVersion": 1,
                "runnerId": RUNNER_ID,
                "connectionUrl": "ws://127.0.0.1:1/v1/runner/connect",
                "currentCredential": {
                    "id": CURRENT_CREDENTIAL_ID,
                    "secret": CURRENT_SECRET,
                    "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abc",
                    "enrolledAt": "2026-08-06T12:00:00Z"
                },
                "pendingCredential": {
                    "id": PENDING_CREDENTIAL_ID,
                    "secret": PENDING_SECRET,
                    "activationId": ACTIVATION_ID,
                    "enrolledAt": "2026-08-06T13:00:00Z"
                },
                "updatedAt": "2026-08-06T13:00:00Z"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::set_permissions(&state, Permissions::from_mode(0o600)).unwrap();

        let socket = runtime.path().join("runner.sock");
        let config = root.path().join("runner.json");
        fs::write(
            &config,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schemaVersion": 1,
                "deploymentMode": "development",
                "runnerStatePath": state,
                "controlSocketPath": socket,
                "workRoot": root.path().join("work")
            }))
            .unwrap(),
        )
        .unwrap();

        let activation = root.path().join("replacement-activation.json");
        write_private_json(
            &activation,
            &activation_artifact("http://127.0.0.1:1", ACTIVATION_ID),
        );

        Self {
            _root: root,
            _runtime: runtime,
            config: config.to_string_lossy().into_owned(),
            activation: activation.to_string_lossy().into_owned(),
            state,
            socket,
        }
    }

    fn command(&self) -> [&str; 8] {
        [
            "runner",
            "enroll",
            "--replace-credential",
            "--activation-file",
            &self.activation,
            "--config",
            &self.config,
            "--json",
        ]
    }
}

/// An activation artifact for `activation_id`, addressed to the Cloud at
/// `base`, that enrolls `RUNNER_ID`.
pub(super) fn activation_artifact(base: &str, activation_id: &str) -> serde_json::Value {
    serde_json::json!({
        "schemaVersion": 1,
        "activationUrl": format!("{base}/v1/runner-enrollments/{activation_id}/activate"),
        "activationToken": format!("{activation_id}.{CURRENT_SECRET}"),
        "runnerId": RUNNER_ID,
        "expiresAt": future_expiry()
    })
}

/// Cloud's answer to an activation of `RUNNER_ID`: the credential
/// `PENDING_CREDENTIAL_ID`, enrolled.
pub(super) fn enrollment_body() -> serde_json::Value {
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
        "credentialId": PENDING_CREDENTIAL_ID,
        "connectionUrl": "ws://127.0.0.1:1/v1/runner/connect"
    })
}

fn accept_control_request(listener: &UnixListener) -> (UnixStream, Vec<u8>) {
    let (mut stream, _) = listener.accept().unwrap();
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while stream.read_exact(&mut byte).is_ok() {
        request.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }
    (stream, request)
}

// REQ-RUNNERENROLL-48, REQ-RUNNERENROLL-49, REQ-RUNNERENROLL-81
#[test]
fn enrolls_then_promotes_a_replacement_without_exposing_credential_material() {
    let fixture = PendingEnrollmentFixture::new();
    let mut state: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.state).unwrap()).unwrap();
    state.as_object_mut().unwrap().remove("pendingCredential");
    fs::write(&fixture.state, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
    fs::set_permissions(&fixture.state, Permissions::from_mode(0o600)).unwrap();

    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let cloud_address = cloud.local_addr().unwrap();
    write_private_json(
        Path::new(&fixture.activation),
        &activation_artifact(&format!("http://{cloud_address}"), ACTIVATION_ID),
    );
    let cloud_server = thread::spawn(move || {
        let (mut stream, _) = cloud.accept().unwrap();
        let request = String::from_utf8(read_request(&mut stream)).unwrap();
        let body = serde_json::to_vec(&enrollment_body()).unwrap();
        write!(
            stream,
            "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
        request
    });

    let control = UnixListener::bind(&fixture.socket).unwrap();
    let state_path = fixture.state.clone();
    let control_server = thread::spawn(move || {
        let (mut stream, request) = accept_control_request(&control);
        let mut state: serde_json::Value =
            serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
        let pending = state
            .as_object_mut()
            .unwrap()
            .remove("pendingCredential")
            .expect("replacement enrollment should stage pending state");
        state["currentCredential"] = pending;
        state["lastPromotion"] = serde_json::json!({
            "credentialId": PENDING_CREDENTIAL_ID,
            "activationId": ACTIVATION_ID,
            "promotedAt": "2026-08-06T13:01:00Z"
        });
        state["updatedAt"] = serde_json::Value::String("2026-08-06T13:01:00Z".to_owned());
        fs::write(&state_path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
        fs::set_permissions(&state_path, Permissions::from_mode(0o600)).unwrap();
        stream
            .write_all(
                format!(
                    "{{\"schemaVersion\":1,\"outcome\":\"reloaded\",\"credentialId\":\"{PENDING_CREDENTIAL_ID}\"}}\n"
                )
                .as_bytes(),
            )
            .unwrap();
        request
    });

    let output = run(&fixture.command());
    assert!(output.status.success(), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "rotation_completed");
    assert_eq!(report["cloudEnrollment"]["outcome"], "enrolled");
    assert_eq!(report["livePromotion"]["outcome"], "promoted");
    assert!(output.stderr.is_empty());

    let cloud_request = cloud_server.join().unwrap();
    assert!(
        cloud_request
            .to_ascii_lowercase()
            .contains("idempotency-key:")
    );
    let control_request = String::from_utf8(control_server.join().unwrap()).unwrap();
    assert_eq!(
        control_request,
        "{\"schemaVersion\":1,\"operation\":\"reload_credential\"}\n"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.state).unwrap()).unwrap();
    assert_eq!(state["currentCredential"]["id"], PENDING_CREDENTIAL_ID);
    assert!(state.get("pendingCredential").is_none());
    let enrolled_secret = state["currentCredential"]["secret"].as_str().unwrap();
    for output in [&output.stdout, &output.stderr, control_request.as_bytes()] {
        for secret in [CURRENT_SECRET, PENDING_SECRET, enrolled_secret] {
            assert!(!bytes_contain(output, secret));
        }
    }
}

// REQ-RUNNERENROLL-51
#[test]
fn retries_a_staged_replacement_through_the_secret_free_control_request() {
    let fixture = PendingEnrollmentFixture::new();
    let listener = UnixListener::bind(&fixture.socket).unwrap();
    let server = thread::spawn(move || {
        let (mut stream, request) = accept_control_request(&listener);
        stream
            .write_all(
                format!(
                    "{{\"schemaVersion\":1,\"outcome\":\"reloaded\",\"credentialId\":\"{PENDING_CREDENTIAL_ID}\"}}\n"
                )
                .as_bytes(),
            )
            .unwrap();
        request
    });

    let output = run(&fixture.command());
    assert!(output.status.success(), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "rotation_completed");
    assert_eq!(report["cloudEnrollment"]["outcome"], "already_enrolled");
    assert_eq!(report["livePromotion"]["outcome"], "promoted");
    assert!(output.stderr.is_empty());

    let request = String::from_utf8(server.join().unwrap()).unwrap();
    assert_eq!(
        request,
        "{\"schemaVersion\":1,\"operation\":\"reload_credential\"}\n"
    );
    for output in [&output.stdout, &output.stderr, request.as_bytes()] {
        assert!(!bytes_contain(output, CURRENT_SECRET));
        assert!(!bytes_contain(output, PENDING_SECRET));
    }
}

// REQ-RUNNERENROLL-50
#[test]
fn explicit_state_update_failure_is_not_reinterpreted_as_success() {
    let fixture = PendingEnrollmentFixture::new();
    let listener = UnixListener::bind(&fixture.socket).unwrap();
    let state_path = fixture.state.clone();
    let server = thread::spawn(move || {
        let (mut stream, request) = accept_control_request(&listener);
        let mut state: serde_json::Value =
            serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
        let pending = state
            .as_object_mut()
            .unwrap()
            .remove("pendingCredential")
            .expect("fixture should begin with a pending credential");
        state["currentCredential"] = pending;
        state["lastPromotion"] = serde_json::json!({
            "credentialId": PENDING_CREDENTIAL_ID,
            "activationId": ACTIVATION_ID,
            "promotedAt": "2026-08-06T13:01:00Z"
        });
        state["updatedAt"] = serde_json::Value::String("2026-08-06T13:01:00Z".to_owned());
        fs::write(&state_path, serde_json::to_vec_pretty(&state).unwrap()).unwrap();
        stream
            .write_all(
                b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"state_update_failed\"}\n",
            )
            .unwrap();
        request
    });

    let output = run(&fixture.command());
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "rotation_incomplete");
    assert_eq!(report["cloudEnrollment"]["outcome"], "already_enrolled");
    assert_eq!(report["livePromotion"]["outcome"], "pending");
    assert_eq!(report["livePromotion"]["error"], "state_update_failed");
    assert!(output.stderr.is_empty());
    let request = String::from_utf8(server.join().unwrap()).unwrap();
    assert_eq!(
        request,
        "{\"schemaVersion\":1,\"operation\":\"reload_credential\"}\n"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.state).unwrap()).unwrap();
    assert_eq!(state["currentCredential"]["id"], PENDING_CREDENTIAL_ID);
    assert!(state.get("pendingCredential").is_none());
}

// REQ-RUNNERENROLL-50
#[test]
fn reports_cloud_enrollment_separately_when_runner_serve_is_unreachable() {
    let fixture = PendingEnrollmentFixture::new();
    let original = fs::read(&fixture.state).unwrap();
    let output = run(&fixture.command());
    assert_eq!(output.status.code(), Some(4), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["outcome"], "rotation_incomplete");
    assert_eq!(report["cloudEnrollment"]["outcome"], "already_enrolled");
    assert_eq!(report["livePromotion"]["outcome"], "pending");
    assert_eq!(report["livePromotion"]["error"], "runner_serve_unreachable");
    assert!(output.stderr.is_empty());
    assert_eq!(fs::read(&fixture.state).unwrap(), original);
    for output in [&output.stdout, &output.stderr] {
        assert!(!bytes_contain(output, CURRENT_SECRET));
        assert!(!bytes_contain(output, PENDING_SECRET));
    }
}

const JOIN_KEY_ID: &str = "rjk_01k0z6r1w8f4jy2m7q9v3x5abc";
const JOIN_SECRET: &str = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc";
pub(super) const JOIN_KEY_VARIABLE: &str = "UM_RUNNER_JOIN_KEY";
const JOINED_CREDENTIAL_IDS: [&str; 3] = [
    "rrc_01k0z6r1w8f4jy2m7q9v3x5ab1",
    "rrc_01k0z6r1w8f4jy2m7q9v3x5ab2",
    "rrc_01k0z6r1w8f4jy2m7q9v3x5ab3",
];
const JOINED_RUNNER_IDS: [&str; 3] = [
    "rnr_01k0z6r1w8f4jy2m7q9v3x5ab1",
    "rnr_01k0z6r1w8f4jy2m7q9v3x5ab2",
    "rnr_01k0z6r1w8f4jy2m7q9v3x5ab3",
];

fn join_token() -> String {
    format!("{JOIN_KEY_ID}.{JOIN_SECRET}")
}

pub(super) fn join_base(server: &api_test_support::ScriptedHttpServer) -> String {
    server.api_url.trim_end_matches("/api/").to_owned()
}

pub(super) fn join_key_artifact(base: &str, expires_at: &str) -> String {
    serde_json::json!({
        "schemaVersion": 1,
        "joinUrl": format!("{base}/v1/runner-join-keys/{JOIN_KEY_ID}/join"),
        "joinKeyId": JOIN_KEY_ID,
        "joinToken": join_token(),
        "expiresAt": expires_at
    })
    .to_string()
}

pub(super) fn join_response(index: usize, connection_url: &str) -> Vec<u8> {
    json_http_response("201 Created", join_response_body(index, connection_url))
}

/// Cloud's answer to the join of host `index`, which names the runner it
/// registered and the credential it recorded.
pub(super) fn join_response_body(index: usize, connection_url: &str) -> serde_json::Value {
    serde_json::json!({
            "schemaVersion": 1,
            "runnerId": JOINED_RUNNER_IDS[index],
            "runnerName": format!("joined-{index}"),
            "organization": {
                "id": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
                "displayName": "Test organization"
            },
            "runnerPool": {
                "id": "rpl_01k0z6r1w8f4jy2m7q9v3x5abc",
                "name": "ci"
            },
        "credentialId": JOINED_CREDENTIAL_IDS[index],
        "connectionUrl": connection_url
    })
}

/// A host directory with an operator configuration, a join key file and no
/// enrolled state.
pub(super) struct JoinHost {
    pub(super) root: tempfile::TempDir,
    _runtime: tempfile::TempDir,
    empty_path: tempfile::TempDir,
    pub(super) config: String,
    pub(super) state: PathBuf,
    pub(super) journal: PathBuf,
    pub(super) key_file: PathBuf,
}

impl JoinHost {
    pub(super) fn new(artifact: &str) -> Self {
        let root = private_credential_directory();
        let runtime = tempfile::tempdir_in("/tmp").unwrap();
        fs::set_permissions(runtime.path(), Permissions::from_mode(0o700)).unwrap();
        let work = root.path().join("work");
        fs::create_dir(&work).unwrap();
        fs::set_permissions(&work, Permissions::from_mode(0o700)).unwrap();
        let state_directory = root.path().join("state");
        let state = state_directory.join("runner-state.json");
        let config = root.path().join("runner.json");
        fs::write(
            &config,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schemaVersion": 1,
                "deploymentMode": "development",
                "runnerStatePath": state,
                "controlSocketPath": runtime.path().join("runner.sock"),
                "workRoot": work
            }))
            .unwrap(),
        )
        .unwrap();
        let key_file = root.path().join("join-key.json");
        fs::write(&key_file, artifact).unwrap();
        fs::set_permissions(&key_file, Permissions::from_mode(0o600)).unwrap();
        Self {
            root,
            _runtime: runtime,
            empty_path: tempfile::tempdir().unwrap(),
            config: config.to_string_lossy().into_owned(),
            state,
            journal: state_directory.join(".runner-enrollment.json"),
            key_file,
        }
    }

    pub(super) fn write_enrolled_state(&self, connection_url: &str) {
        self.write_state(connection_url, ("activationId", ACTIVATION_ID));
    }

    /// Enrolled state whose credential the key `JOIN_KEY_ID` joined.
    fn write_joined_state(&self, connection_url: &str) {
        self.write_state(connection_url, ("joinKeyId", JOIN_KEY_ID));
    }

    fn write_state(&self, connection_url: &str, (origin, origin_id): (&str, &str)) {
        let directory = self.state.parent().unwrap();
        fs::create_dir(directory).unwrap();
        fs::set_permissions(directory, Permissions::from_mode(0o700)).unwrap();
        let mut credential = serde_json::json!({
            "id": CURRENT_CREDENTIAL_ID,
            "secret": CURRENT_SECRET,
            "enrolledAt": "2026-08-06T12:00:00Z"
        });
        credential[origin] = origin_id.into();
        write_private_json(
            &self.state,
            &serde_json::json!({
                "schemaVersion": 1,
                "runnerId": RUNNER_ID,
                "connectionUrl": connection_url,
                "currentCredential": credential,
                "updatedAt": "2026-08-06T12:00:00Z"
            }),
        );
    }

    /// A join journal staged for `JOIN_KEY_ID` with the credential `secret`.
    pub(super) fn write_join_journal(&self, base: &str, secret: &str) {
        write_private_json(
            &self.journal,
            &serde_json::json!({
                "schemaVersion": 1,
                "joinKey": {
                    "joinUrl": format!("{base}/v1/runner-join-keys/{JOIN_KEY_ID}/join"),
                    "joinKeyId": JOIN_KEY_ID,
                    "expiresAt": future_expiry()
                },
                "credentialSecret": secret,
                "credentialSecretVerifier": verifier_of(secret),
                "idempotencyKey": "0".repeat(64),
                "replaceCredential": false,
                "stagedAt": "2026-08-06T12:00:00Z"
            }),
        );
    }

    fn serve_with_key_file(&self) -> ServeProcess {
        let key_file = self.key_file.to_string_lossy().into_owned();
        self.serve(&["--join-key-file", &key_file], &[])
    }

    pub(super) fn serve(&self, arguments: &[&str], environment: &[(&str, &str)]) -> ServeProcess {
        self.serve_with_input(arguments, environment, StandardInput::Null)
    }

    fn serve_with_input(
        &self,
        arguments: &[&str],
        environment: &[(&str, &str)],
        standard_input: StandardInput<'_>,
    ) -> ServeProcess {
        let mut command = serve_command(&["--config", &self.config]);
        command.args(arguments).env("PATH", self.empty_path.path());
        for (name, value) in environment {
            command.env(name, value);
        }
        ServeProcess::start(command, standard_input)
    }

    pub(super) fn state_document(&self) -> serde_json::Value {
        serde_json::from_slice(&fs::read(&self.state).unwrap()).unwrap()
    }
}

/// What `um runner serve` reads as its standard input.
pub(super) enum StandardInput<'a> {
    Null,
    /// A pipe the text is written to and then closed.
    Piped(&'a str),
    /// An open descriptor, such as a pseudoterminal's peer end or a pipe's
    /// read end, whose writer the test holds.
    Descriptor(&'a std::os::fd::OwnedFd),
}

pub(super) fn write_private_json(path: &Path, document: &serde_json::Value) {
    fs::write(path, serde_json::to_vec_pretty(document).unwrap()).unwrap();
    fs::set_permissions(path, Permissions::from_mode(0o600)).unwrap();
}

/// `um runner serve ARGUMENTS` with telemetry off and no join key inherited
/// from the suite's environment.
pub(super) fn serve_command(arguments: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_um"));
    command
        .args(["runner", "serve"])
        .args(arguments)
        .env_remove(JOIN_KEY_VARIABLE);
    for variable in RUNNER_TELEMETRY_VARIABLES {
        command.env_remove(variable);
    }
    command.env("OTEL_SDK_DISABLED", "true");
    command
}

/// A running `um runner serve`, killed when dropped.
pub(super) struct ServeProcess {
    child: Child,
    stderr: Option<thread::JoinHandle<String>>,
}

impl ServeProcess {
    /// Starts `command` reading `standard_input`, with its standard error
    /// collected.
    pub(super) fn start(mut command: Command, standard_input: StandardInput<'_>) -> Self {
        command
            .stdin(match standard_input {
                StandardInput::Null => Stdio::null(),
                StandardInput::Piped(_) => Stdio::piped(),
                StandardInput::Descriptor(input) => Stdio::from(input.try_clone().unwrap()),
            })
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = command.spawn().unwrap();
        if let StandardInput::Piped(standard_input) = standard_input {
            let mut stdin = child.stdin.take().unwrap();
            stdin.write_all(standard_input.as_bytes()).unwrap();
        }
        let mut stderr = child.stderr.take().unwrap();
        let stderr = thread::spawn(move || {
            let mut output = String::new();
            let _ = stderr.read_to_string(&mut output);
            output
        });
        Self {
            child,
            stderr: Some(stderr),
        }
    }

    pub(super) fn exited(&mut self) -> Option<ExitStatus> {
        self.child.try_wait().unwrap()
    }

    pub(super) fn wait_for_exit(mut self) -> (ExitStatus, String) {
        let status = poll_until("Runner Serve exit", || self.exited(), Option::is_some).unwrap();
        (status, self.stderr.take().unwrap().join().unwrap())
    }

    pub(super) fn stop(mut self) -> String {
        if self.exited().is_none() {
            self.child.kill().unwrap();
        }
        self.child.wait().unwrap();
        self.stderr.take().unwrap().join().unwrap()
    }
}

impl Drop for ServeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The fake Cloud's runner connection endpoint: it records each upgrade
/// request and closes the connection, so the runner keeps reconnecting.
pub(super) struct ConnectionWitness {
    pub(super) url: String,
    requests: mpsc::Receiver<String>,
}

impl ConnectionWitness {
    pub(super) fn new() -> Self {
        Self::answering(None)
    }

    /// A connection endpoint that refuses the runner's credential with 401
    /// and reports each request once the runner has closed the connection,
    /// that is, once it has read the refusal.
    pub(super) fn rejecting() -> Self {
        Self::answering(Some(
            b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n",
        ))
    }

    fn answering(response: Option<&'static [u8]>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/v1/runner/connect", listener.local_addr().unwrap());
        let (sender, requests) = mpsc::channel();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else {
                    break;
                };
                let request = String::from_utf8_lossy(&read_request(&mut stream)).into_owned();
                if let Some(response) = response {
                    let _ = stream.write_all(response);
                    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(10)));
                    let _ = io::copy(&mut stream, &mut io::sink());
                }
                if sender.send(request).is_err() {
                    break;
                }
            }
        });
        Self { url, requests }
    }

    pub(super) fn next_connection(&self, serve: &mut ServeProcess) -> Option<String> {
        let mut received = None;
        poll_until(
            "runner connection",
            || {
                if received.is_none() {
                    received = self.requests.try_recv().ok();
                }
                (received.is_some(), serve.exited())
            },
            |(connected, exited)| *connected || exited.is_some(),
        );
        received
    }
}

pub(super) fn connection_bearer(serve: ServeProcess, witness: &ConnectionWitness) -> String {
    let mut serve = serve;
    let Some(request) = witness.next_connection(&mut serve) else {
        let stderr = serve.stop();
        panic!("Runner Serve stopped before connecting: {stderr}");
    };
    let stderr = serve.stop();
    assert!(!stderr.contains(JOIN_SECRET), "{stderr}");
    header_value(&request, "authorization").to_owned()
}

pub(super) fn verifier_of(secret: &str) -> String {
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(secret)
        .unwrap();
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(ring::digest::digest(&ring::digest::SHA256, &decoded).as_ref())
}

fn join_body(request: &str) -> serde_json::Value {
    serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap()
}

fn assert_joined_state(host: &JoinHost, index: usize, connection_url: &str) -> String {
    let metadata = fs::metadata(&host.state).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert!(!host.journal.exists());
    let bytes = fs::read(&host.state).unwrap();
    assert!(!bytes_contain(&bytes, JOIN_SECRET));
    let state = host.state_document();
    assert_eq!(state["runnerId"], JOINED_RUNNER_IDS[index]);
    assert_eq!(state["connectionUrl"], connection_url);
    assert_eq!(
        state["currentCredential"]["id"],
        JOINED_CREDENTIAL_IDS[index]
    );
    assert_eq!(state["currentCredential"]["joinKeyId"], JOIN_KEY_ID);
    assert!(state["currentCredential"].get("activationId").is_none());
    state["currentCredential"]["secret"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn assert_join_request(request: &str, secret: &str) {
    assert!(
        request.starts_with(&format!(
            "POST /v1/runner-join-keys/{JOIN_KEY_ID}/join HTTP/1.1\r\n"
        )),
        "{request}"
    );
    assert_eq!(
        header_value(request, "authorization"),
        format!("Bearer {}", join_token())
    );
    assert!(!header_value(request, "idempotency-key").is_empty());
    assert_eq!(
        join_body(request),
        serde_json::json!({"schemaVersion": 1, "credentialSecretVerifier": verifier_of(secret)})
    );
    assert!(!request.contains(secret));
}

pub(super) fn future_expiry() -> String {
    "2099-08-06T20:00:00Z".to_owned()
}

// REQ-RUNNERENROLL-97, REQ-RUNNERENROLL-30
#[test]
fn serve_joins_with_a_key_file_on_a_host_without_state_and_connects_with_its_own_credential() {
    let witness = ConnectionWitness::new();
    let server = api_test_support::ScriptedHttpServer::respond(join_response(0, &witness.url));
    let host = JoinHost::new(&join_key_artifact(&join_base(&server), &future_expiry()));

    let bearer = connection_bearer(host.serve_with_key_file(), &witness);

    let secret = assert_joined_state(&host, 0, &witness.url);
    assert_eq!(
        bearer,
        format!("Bearer {}.{secret}", JOINED_CREDENTIAL_IDS[0])
    );
    assert_join_request(&server.finish_one(), &secret);
}

// REQ-RUNNERENROLL-97
#[test]
fn serve_joins_with_the_key_in_the_environment() {
    let witness = ConnectionWitness::new();
    let server = api_test_support::ScriptedHttpServer::respond(join_response(0, &witness.url));
    let artifact = join_key_artifact(&join_base(&server), &future_expiry());
    let host = JoinHost::new(&artifact);

    let bearer = connection_bearer(
        host.serve(&[], &[(JOIN_KEY_VARIABLE, artifact.as_str())]),
        &witness,
    );

    let secret = assert_joined_state(&host, 0, &witness.url);
    assert_eq!(
        bearer,
        format!("Bearer {}.{secret}", JOINED_CREDENTIAL_IDS[0])
    );
    assert_join_request(&server.finish_one(), &secret);
}

// REQ-RUNNERENROLL-97
#[test]
fn serve_joins_with_the_key_on_piped_standard_input() {
    let witness = ConnectionWitness::new();
    let server = api_test_support::ScriptedHttpServer::respond(join_response(0, &witness.url));
    let artifact = join_key_artifact(&join_base(&server), &future_expiry());
    let host = JoinHost::new(&artifact);

    let bearer = connection_bearer(
        host.serve_with_input(
            &["--join-key-file", "-"],
            &[],
            StandardInput::Piped(&artifact),
        ),
        &witness,
    );

    let secret = assert_joined_state(&host, 0, &witness.url);
    assert_eq!(
        bearer,
        format!("Bearer {}.{secret}", JOINED_CREDENTIAL_IDS[0])
    );
    assert_join_request(&server.finish_one(), &secret);
}

// REQ-RUNNERENROLL-97
#[test]
fn one_key_joins_three_configured_hosts_with_three_verifiers() {
    // Each host connects to its own witness: a stopped host's reconnection
    // can then never be taken for the next host's connection.
    let witnesses = [
        ConnectionWitness::new(),
        ConnectionWitness::new(),
        ConnectionWitness::new(),
    ];
    let server = api_test_support::ScriptedHttpServer::respond_in_sequence(
        witnesses
            .iter()
            .enumerate()
            .map(|(index, witness)| join_response(index, &witness.url))
            .collect(),
    );
    let artifact = join_key_artifact(&join_base(&server), &future_expiry());
    let hosts = [
        JoinHost::new(&artifact),
        JoinHost::new(&artifact),
        JoinHost::new(&artifact),
    ];

    let mut secrets = Vec::new();
    for (index, (host, witness)) in hosts.iter().zip(&witnesses).enumerate() {
        let bearer = connection_bearer(host.serve_with_key_file(), witness);
        let secret = assert_joined_state(host, index, &witness.url);
        assert_eq!(
            bearer,
            format!("Bearer {}.{secret}", JOINED_CREDENTIAL_IDS[index])
        );
        secrets.push(secret);
    }

    let requests = server.finish();
    let mut verifiers = std::collections::BTreeSet::new();
    let mut idempotency_keys = std::collections::BTreeSet::new();
    for (request, secret) in requests.iter().zip(&secrets) {
        assert_join_request(request, secret);
        verifiers.insert(
            join_body(request)["credentialSecretVerifier"]
                .clone()
                .to_string(),
        );
        idempotency_keys.insert(header_value(request, "idempotency-key").to_owned());
    }
    assert_eq!(verifiers.len(), 3);
    assert_eq!(idempotency_keys.len(), 3);
}

// REQ-RUNNERENROLL-97, REQ-RUNNERENROLL-30
#[test]
fn an_enrolled_host_serves_with_its_state_and_sends_no_join() {
    let witness = ConnectionWitness::new();
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let host = JoinHost::new(&join_key_artifact(&base, &future_expiry()));
    host.write_enrolled_state(&witness.url);
    let original = fs::read(&host.state).unwrap();

    let bearer = connection_bearer(host.serve_with_key_file(), &witness);

    assert_eq!(
        bearer,
        format!("Bearer {CURRENT_CREDENTIAL_ID}.{CURRENT_SECRET}")
    );
    assert_eq!(fs::read(&host.state).unwrap(), original);
    assert!(!host.journal.exists());
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

/// A test-only variable given to Runner Serve beside its join key: seeing it
/// proves the observation read the serving process's environment.
const ENVIRONMENT_WITNESS: &str = "UM_TEST_ENVIRONMENT_WITNESS";

/// The raw environment block of the running process `pid`: on Linux its
/// `/proc` entry; elsewhere the output of `ps`, which prints the arguments and
/// then the environment of a process of the same user.
fn process_environment(pid: u32) -> Vec<u8> {
    if cfg!(target_os = "linux") {
        return fs::read(format!("/proc/{pid}/environ")).unwrap();
    }
    let output = Command::new("/bin/ps")
        .args(["eww", "-o", "command=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "ps: {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn bytes_contain(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|part| part == needle.as_bytes())
}

/// Starts Runner Serve with the join key `artifact` in its environment and,
/// once it connects to `witness`, observes the serving process's environment.
/// A failure names `case` and never prints the environment, which holds the
/// test's own ambient variables.
fn assert_serving_environment_holds_no_join_key(
    case: &str,
    host: &JoinHost,
    artifact: &str,
    witness: &ConnectionWitness,
) {
    let mut serve = host.serve(
        &[],
        &[
            (JOIN_KEY_VARIABLE, artifact),
            (ENVIRONMENT_WITNESS, "present"),
        ],
    );
    // `exec` keeps the process id and only the final image connects, so
    // after the connection this id is the serving process.
    let connected = witness.next_connection(&mut serve).is_some();
    let environment = connected.then(|| process_environment(serve.child.id()));
    let stderr = serve.stop();
    let Some(environment) = environment else {
        panic!("{case}: Runner Serve stopped before connecting: {stderr}");
    };
    assert!(
        bytes_contain(&environment, &format!("{ENVIRONMENT_WITNESS}=present")),
        "{case}: the observation does not hold {ENVIRONMENT_WITNESS}"
    );
    assert!(
        !bytes_contain(&environment, JOIN_KEY_VARIABLE),
        "{case}: the serving process holds {JOIN_KEY_VARIABLE}"
    );
    assert!(
        !bytes_contain(&environment, JOIN_SECRET),
        "{case}: the serving process holds the join secret"
    );
}

// REQ-RUNNERENROLL-97
#[test]
fn the_serving_process_holds_no_join_key_in_its_environment() {
    // A host that joins with the key.
    let witness = ConnectionWitness::new();
    let server = api_test_support::ScriptedHttpServer::respond(join_response(0, &witness.url));
    let artifact = join_key_artifact(&join_base(&server), &future_expiry());
    let host = JoinHost::new(&artifact);
    assert_serving_environment_holds_no_join_key("a host that joins", &host, &artifact, &witness);
    server.finish_one();

    // A host already enrolled, which never contacts Cloud.
    let witness = ConnectionWitness::new();
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let artifact = join_key_artifact(&base, &future_expiry());
    let host = JoinHost::new(&artifact);
    host.write_enrolled_state(&witness.url);
    assert_serving_environment_holds_no_join_key("an enrolled host", &host, &artifact, &witness);
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-97
#[test]
fn a_refused_join_key_stops_the_start_and_writes_no_state() {
    for (status, code) in [
        ("410 Gone", "join_key_gone"),
        ("401 Unauthorized", "join_key_unauthorized"),
    ] {
        let server =
            api_test_support::ScriptedHttpServer::respond(http_response(status, None, b""));
        let host = JoinHost::new(&join_key_artifact(&join_base(&server), &future_expiry()));

        let (exit, stderr) = host.serve_with_key_file().wait_for_exit();

        assert_eq!(exit.code(), Some(1), "{status}: {stderr}");
        assert!(
            stderr
                .lines()
                .any(|line| line.trim() == format!("code: {code}")),
            "{status}: {stderr}"
        );
        assert!(!stderr.contains(JOIN_SECRET));
        assert!(!host.state.exists());
        assert!(!host.journal.exists());
        server.finish_one();
    }
}

// REQ-RUNNERENROLL-97
#[test]
fn an_ambiguous_join_is_retried_as_the_same_request_and_makes_one_runner() {
    let witness = ConnectionWitness::new();
    let server = api_test_support::ScriptedHttpServer::respond_in_sequence(vec![
        Vec::new(),
        join_response(0, &witness.url),
    ]);
    let host = JoinHost::new(&join_key_artifact(&join_base(&server), &future_expiry()));

    let bearer = connection_bearer(host.serve_with_key_file(), &witness);

    let secret = assert_joined_state(&host, 0, &witness.url);
    assert_eq!(
        bearer,
        format!("Bearer {}.{secret}", JOINED_CREDENTIAL_IDS[0])
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_join_request(request, &secret);
    }
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
    assert_eq!(join_body(&requests[0]), join_body(&requests[1]));
}

// REQ-RUNNERENROLL-97, REQ-RUNNERENROLL-113
#[test]
fn a_join_retry_is_reported_and_a_later_unauthorized_answer_keeps_the_journal() {
    let server = api_test_support::ScriptedHttpServer::respond_in_sequence(vec![
        http_response("503 Service Unavailable", None, b""),
        http_response("401 Unauthorized", None, b""),
    ]);
    let host = JoinHost::new(&join_key_artifact(&join_base(&server), &future_expiry()));

    let (exit, stderr) = host.serve_with_key_file().wait_for_exit();

    assert_eq!(exit.code(), Some(1), "{stderr}");
    let lines: Vec<&str> = stderr.lines().map(str::trim).collect();
    let cause = lines
        .iter()
        .position(|line| *line == "cause: cloud_unavailable")
        .unwrap_or_else(|| panic!("no retry cause line: {stderr}"));
    let refusal = lines
        .iter()
        .position(|line| *line == "code: join_key_unauthorized")
        .unwrap_or_else(|| panic!("no refusal code line: {stderr}"));
    assert!(cause < refusal, "{stderr}");
    assert!(
        lines[..cause].iter().any(|line| line.contains(JOIN_KEY_ID)),
        "{stderr}"
    );
    assert!(!stderr.contains(JOIN_SECRET), "{stderr}");
    assert!(host.journal.exists());
    assert!(!host.state.exists());
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        header_value(&requests[0], "idempotency-key"),
        header_value(&requests[1], "idempotency-key")
    );
}

// REQ-RUNNERENROLL-97, REQ-RUNNERENROLL-17
#[test]
fn an_unsafe_or_expired_join_key_is_refused_before_contacting_cloud() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let artifact = join_key_artifact(&base, &future_expiry());

    let linked = JoinHost::new(&artifact);
    let link = linked.root.path().join("join-key-link.json");
    std::os::unix::fs::symlink(&linked.key_file, &link).unwrap();
    let link = link.to_string_lossy().into_owned();
    let shared = JoinHost::new(&artifact);
    fs::set_permissions(&shared.key_file, Permissions::from_mode(0o644)).unwrap();
    let oversized = JoinHost::new(&format!("{artifact}{}", " ".repeat(4097)));
    let expired = JoinHost::new(&join_key_artifact(&base, "2020-01-01T00:00:00Z"));

    for (host, process, expected) in [
        (&linked, linked.serve(&["--join-key-file", &link], &[]), 1),
        (&shared, shared.serve_with_key_file(), 1),
        (&oversized, oversized.serve_with_key_file(), 1),
        (&expired, expired.serve_with_key_file(), 1),
        (
            &linked,
            linked.serve(
                &["--join-key-file", &link],
                &[(JOIN_KEY_VARIABLE, artifact.as_str())],
            ),
            2,
        ),
    ] {
        let (exit, stderr) = process.wait_for_exit();
        assert_eq!(exit.code(), Some(expected), "{stderr}");
        assert!(!stderr.contains(JOIN_SECRET), "{stderr}");
        assert!(!host.state.exists());
        assert!(!host.journal.exists());
    }
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-97: a host that holds enrolled state serves with it and
// never reads its join key, so a key that is missing, unsafe, empty or
// malformed does not stop the start.
#[test]
fn an_enrolled_host_serves_without_reading_its_join_key() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let artifact = join_key_artifact(&base, &future_expiry());

    let missing = JoinHost::new(&artifact);
    fs::remove_file(&missing.key_file).unwrap();
    let shared = JoinHost::new(&artifact);
    fs::set_permissions(&shared.key_file, Permissions::from_mode(0o644)).unwrap();
    let empty = JoinHost::new(&artifact);
    let malformed = JoinHost::new(&artifact);

    // Each case: the host, the environment it starts with, and whether it names its key file.
    type Case<'a> = (&'a JoinHost, &'a [(&'a str, &'a str)], bool);
    let cases: [Case<'_>; 4] = [
        (&missing, &[], true),
        (&shared, &[], true),
        (&empty, &[(JOIN_KEY_VARIABLE, "")], false),
        (&malformed, &[(JOIN_KEY_VARIABLE, "not a join key")], false),
    ];
    for (host, environment, with_key_file) in cases {
        let witness = ConnectionWitness::new();
        host.write_enrolled_state(&witness.url);
        let original = fs::read(&host.state).unwrap();
        let key_file = host.key_file.to_string_lossy().into_owned();
        let arguments: &[&str] = if with_key_file {
            &["--join-key-file", &key_file]
        } else {
            &[]
        };

        let bearer = connection_bearer(host.serve(arguments, environment), &witness);

        assert_eq!(
            bearer,
            format!("Bearer {CURRENT_CREDENTIAL_ID}.{CURRENT_SECRET}")
        );
        assert_eq!(fs::read(&host.state).unwrap(), original);
        assert!(!host.journal.exists());
    }
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-104, REQ-RUNNERENROLL-30: a host whose state file is present
// is enrolled even when its configuration or state does not load, so the start
// fails with that cause and never reads its join key, whatever the key's condition.
#[test]
fn an_enrolled_host_that_does_not_load_fails_with_its_own_cause() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let connection_url = format!("ws://{}/v1/runner/connect", cloud.local_addr().unwrap());
    let artifact = join_key_artifact(&base, &future_expiry());

    let remove_work_root = |host: &JoinHost| fs::remove_dir(host.root.path().join("work")).unwrap();
    let remove_key_file = |host: &JoinHost| fs::remove_file(&host.key_file).unwrap();
    let missing_key = JoinHost::new(&artifact);
    remove_work_root(&missing_key);
    remove_key_file(&missing_key);
    let shared_key = JoinHost::new(&artifact);
    remove_work_root(&shared_key);
    fs::set_permissions(&shared_key.key_file, Permissions::from_mode(0o644)).unwrap();
    let malformed_variable = JoinHost::new(&artifact);
    remove_work_root(&malformed_variable);
    let empty_state = JoinHost::new(&artifact);
    remove_key_file(&empty_state);

    // Each case: the host, the environment it starts with, whether it names
    // its key file, and whether its state is overwritten with `{}`.
    type Case<'a> = (&'a JoinHost, &'a [(&'a str, &'a str)], bool, bool);
    let cases: [Case<'_>; 4] = [
        (&missing_key, &[], true, false),
        (&shared_key, &[], true, false),
        (
            &malformed_variable,
            &[(JOIN_KEY_VARIABLE, "not a join key")],
            false,
            false,
        ),
        (&empty_state, &[], true, true),
    ];
    for (host, environment, with_key_file, empty) in cases {
        host.write_enrolled_state(&connection_url);
        if empty {
            write_private_json(&host.state, &serde_json::json!({}));
        }
        let original = fs::read(&host.state).unwrap();
        let key_file = host.key_file.to_string_lossy().into_owned();
        let arguments: &[&str] = if with_key_file {
            &["--join-key-file", &key_file]
        } else {
            &[]
        };

        let (exit, stderr) = host.serve(arguments, environment).wait_for_exit();

        assert_eq!(exit.code(), Some(1), "{stderr}");
        assert!(stderr.contains(&host.config), "{stderr}");
        assert!(!stderr.contains(&key_file), "{stderr}");
        assert!(!stderr.contains(JOIN_KEY_VARIABLE), "{stderr}");
        assert_eq!(fs::read(&host.state).unwrap(), original);
        assert!(!host.journal.exists());
    }
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-105, REQ-RUNNERENROLL-30: a host with no state whose work
// root does not exist fails with that cause before it reads its join key or
// contacts Cloud, so it registers no runner, stages no join and writes no
// state, and a join an earlier start staged stays as it was.
#[test]
fn a_host_without_a_work_root_fails_before_it_joins() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let artifact = join_key_artifact(&base, &future_expiry());

    let remove_work_root = |host: &JoinHost| fs::remove_dir(host.root.path().join("work")).unwrap();
    let key_file = JoinHost::new(&artifact);
    remove_work_root(&key_file);
    let variable = JoinHost::new(&artifact);
    remove_work_root(&variable);
    let staged = JoinHost::new(&artifact);
    remove_work_root(&staged);
    let state_directory = staged.state.parent().unwrap();
    fs::create_dir(state_directory).unwrap();
    fs::set_permissions(state_directory, Permissions::from_mode(0o700)).unwrap();
    staged.write_join_journal(&base, JOIN_SECRET);
    let staged_journal = fs::read(&staged.journal).unwrap();
    let missing_key = JoinHost::new(&artifact);
    remove_work_root(&missing_key);
    fs::remove_file(&missing_key.key_file).unwrap();

    // Each case: the host, the environment it starts with, whether it names
    // its key file, and the journal it must leave.
    type Case<'a> = (
        &'a JoinHost,
        &'a [(&'a str, &'a str)],
        bool,
        Option<&'a [u8]>,
    );
    let cases: [Case<'_>; 4] = [
        (&key_file, &[], true, None),
        (
            &variable,
            &[(JOIN_KEY_VARIABLE, artifact.as_str())],
            false,
            None,
        ),
        (&staged, &[], true, Some(&staged_journal)),
        (&missing_key, &[], true, None),
    ];
    for (host, environment, with_key_file, journal) in cases {
        let key_file = host.key_file.to_string_lossy().into_owned();
        let arguments: &[&str] = if with_key_file {
            &["--join-key-file", &key_file]
        } else {
            &[]
        };

        let (exit, stderr) = host.serve(arguments, environment).wait_for_exit();

        assert_eq!(exit.code(), Some(1), "{stderr}");
        assert!(stderr.contains(&host.config), "{stderr}");
        assert!(!stderr.contains(JOIN_SECRET), "{stderr}");
        assert!(!stderr.contains(&key_file), "{stderr}");
        assert!(!host.state.exists());
        match journal {
            Some(journal) => assert_eq!(fs::read(&host.journal).unwrap(), journal),
            None => assert!(!host.journal.exists()),
        }
    }
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-105, REQ-RUNNERENROLL-30: a host with no state whose
// operator configuration is missing or malformed fails with that cause before
// it reads its join key, so a key file that is gone is never the failure.
#[test]
fn a_host_whose_configuration_does_not_read_fails_before_it_reads_its_join_key() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let artifact = join_key_artifact(&base, &future_expiry());
    let missing = JoinHost::new(&artifact);
    fs::remove_file(&missing.config).unwrap();
    let malformed = JoinHost::new(&artifact);
    fs::write(&malformed.config, b"{").unwrap();

    for host in [&missing, &malformed] {
        fs::remove_file(&host.key_file).unwrap();
        let key_file = host.key_file.to_string_lossy().into_owned();

        let (exit, stderr) = host.serve_with_key_file().wait_for_exit();

        assert_eq!(exit.code(), Some(1), "{stderr}");
        assert!(stderr.contains(&host.config), "{stderr}");
        assert!(!stderr.contains(&key_file), "{stderr}");
        assert!(!host.state.exists());
        assert!(!host.journal.exists());
    }
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-97: the start of an enrolled host, with or without a join
// key, removes the journal its own completed join left, a second copy of the
// state's credential secret, and leaves a journal for another credential.
#[test]
fn an_enrolled_host_removes_only_its_completed_joins_journal() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let artifact = join_key_artifact(&base, &future_expiry());

    let with_key = JoinHost::new(&artifact);
    let without_key = JoinHost::new(&artifact);
    let another_credential = JoinHost::new(&artifact);
    let cases: [(&JoinHost, bool, &str, bool); 3] = [
        (&with_key, true, CURRENT_SECRET, false),
        (&without_key, false, CURRENT_SECRET, false),
        (&another_credential, false, PENDING_SECRET, true),
    ];
    for (host, with_key_file, journal_secret, kept) in cases {
        let witness = ConnectionWitness::new();
        host.write_joined_state(&witness.url);
        host.write_join_journal(&base, journal_secret);
        let journal = fs::read(&host.journal).unwrap();
        let key_file = host.key_file.to_string_lossy().into_owned();
        let arguments: &[&str] = if with_key_file {
            &["--join-key-file", &key_file]
        } else {
            &[]
        };

        let bearer = connection_bearer(host.serve(arguments, &[]), &witness);

        assert_eq!(
            bearer,
            format!("Bearer {CURRENT_CREDENTIAL_ID}.{CURRENT_SECRET}")
        );
        if kept {
            assert_eq!(fs::read(&host.journal).unwrap(), journal);
        } else {
            assert!(!host.journal.exists());
        }
    }
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

/// Whether the terminal whose peer end is `peer` echoes its input.
fn echoes(peer: &std::os::fd::OwnedFd) -> bool {
    rustix::termios::tcgetattr(peer)
        .unwrap()
        .local_modes
        .contains(rustix::termios::LocalModes::ECHO)
}

/// What the terminal's controlling end has to be read once nothing writes to it.
fn terminal_output(controlling: &std::os::fd::OwnedFd) -> Vec<u8> {
    let flags = rustix::fs::fcntl_getfl(controlling).unwrap();
    rustix::fs::fcntl_setfl(controlling, flags | rustix::fs::OFlags::NONBLOCK).unwrap();
    let mut output = Vec::new();
    let mut buffer = [0; 512];
    while let Ok(count @ 1..) = rustix::io::read(controlling, &mut buffer) {
        output.extend_from_slice(&buffer[..count]);
    }
    output
}

fn contains_join_secret(bytes: &[u8]) -> bool {
    bytes
        .windows(JOIN_SECRET.len())
        .any(|part| part == JOIN_SECRET.as_bytes())
}

// REQ-RUNNERENROLL-114, REQ-RUNNERENROLL-97
#[test]
fn serve_joins_with_the_key_given_at_a_prompt_that_does_not_echo_it() {
    let witness = ConnectionWitness::new();
    let server = api_test_support::ScriptedHttpServer::respond(join_response(0, &witness.url));
    let artifact = join_key_artifact(&join_base(&server), &future_expiry());
    let host = JoinHost::new(&artifact);
    let terminal = open_test_pty(None).unwrap();

    let mut serve = host.serve_with_input(
        &["--join-key"],
        &[],
        StandardInput::Descriptor(&terminal.slave),
    );
    poll_until(
        "join key prompt",
        || (echoes(&terminal.slave), serve.exited()),
        |(echo, exited)| !echo || exited.is_some(),
    );
    fs::File::from(terminal.master.try_clone().unwrap())
        .write_all(format!("{artifact}\n").as_bytes())
        .unwrap();
    let bearer = connection_bearer(serve, &witness);

    assert!(echoes(&terminal.slave));
    let shown = terminal_output(&terminal.master);
    assert!(
        !contains_join_secret(&shown),
        "{}",
        String::from_utf8_lossy(&shown)
    );
    let secret = assert_joined_state(&host, 0, &witness.url);
    assert_eq!(
        bearer,
        format!("Bearer {}.{secret}", JOINED_CREDENTIAL_IDS[0])
    );
    assert_join_request(&server.finish_one(), &secret);
}

// REQ-RUNNERENROLL-114: a start that cannot prompt fails at once, its standard
// input left open, and stages, sends and writes nothing.
#[test]
fn the_join_key_prompt_without_a_terminal_is_a_usage_error_that_sends_nothing() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let host = JoinHost::new(&join_key_artifact(&base, &future_expiry()));
    let (reader, _writer) = io::pipe().unwrap();
    let reader = std::os::fd::OwnedFd::from(reader);

    let (exit, stderr) = host
        .serve_with_input(&["--join-key"], &[], StandardInput::Descriptor(&reader))
        .wait_for_exit();

    assert_eq!(exit.code(), Some(2), "{stderr}");
    assert!(stderr.contains("--join-key-file"), "{stderr}");
    assert!(stderr.contains(JOIN_KEY_VARIABLE), "{stderr}");
    assert!(!host.state.exists());
    assert!(!host.journal.exists());
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-104
#[test]
fn an_enrolled_host_started_with_the_join_key_prompt_serves_without_a_terminal() {
    let witness = ConnectionWitness::new();
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let host = JoinHost::new(&join_key_artifact(&base, &future_expiry()));
    host.write_enrolled_state(&witness.url);
    let original = fs::read(&host.state).unwrap();

    let bearer = connection_bearer(host.serve(&["--join-key"], &[]), &witness);

    assert_eq!(
        bearer,
        format!("Bearer {CURRENT_CREDENTIAL_ID}.{CURRENT_SECRET}")
    );
    assert_eq!(fs::read(&host.state).unwrap(), original);
    assert!(!host.journal.exists());
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-97: the prompt with a key file or the variable is refused
// from the arguments and the environment alone, before the operator
// configuration, the key file or the terminal is read.
#[test]
fn the_join_key_prompt_with_another_join_key_input_is_a_usage_error() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let artifact = join_key_artifact(&base, &future_expiry());
    let host = JoinHost::new(&artifact);
    fs::remove_file(&host.config).unwrap();
    fs::remove_file(&host.key_file).unwrap();
    let key_file = host.key_file.to_string_lossy().into_owned();
    let terminal = open_test_pty(None).unwrap();

    type Case<'a> = (&'a [&'a str], &'a [(&'a str, &'a str)]);
    let cases: [Case<'_>; 2] = [
        (&["--join-key", "--join-key-file", &key_file], &[]),
        (&["--join-key"], &[(JOIN_KEY_VARIABLE, artifact.as_str())]),
    ];
    for (arguments, environment) in cases {
        let (exit, stderr) = host
            .serve_with_input(
                arguments,
                environment,
                StandardInput::Descriptor(&terminal.slave),
            )
            .wait_for_exit();

        assert_eq!(exit.code(), Some(2), "{arguments:?}: {stderr}");
        assert!(stderr.contains(JOIN_KEY_VARIABLE), "{stderr}");
        assert!(!stderr.contains(JOIN_SECRET), "{stderr}");
        assert!(echoes(&terminal.slave));
        assert!(!host.state.exists());
        assert!(!host.journal.exists());
    }
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-105: a host with no state that cannot serve fails with its
// configuration's cause before it prompts, so it never waits for a key.
#[test]
fn a_host_without_a_work_root_fails_before_it_prompts_for_its_join_key() {
    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let host = JoinHost::new(&join_key_artifact(&base, &future_expiry()));
    fs::remove_dir(host.root.path().join("work")).unwrap();
    let terminal = open_test_pty(None).unwrap();

    let (exit, stderr) = host
        .serve_with_input(
            &["--join-key"],
            &[],
            StandardInput::Descriptor(&terminal.slave),
        )
        .wait_for_exit();

    assert_eq!(exit.code(), Some(1), "{stderr}");
    assert!(stderr.contains(&host.config), "{stderr}");
    assert!(echoes(&terminal.slave));
    assert!(!host.state.exists());
    assert!(!host.journal.exists());
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

// REQ-RUNNERENROLL-114: a signal that ends the start during the prompt leaves
// the terminal echoing, and the start ends as the signal ends a process.
#[test]
fn a_signal_during_the_join_key_prompt_restores_the_terminal() {
    use std::os::unix::process::ExitStatusExt;

    let cloud = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", cloud.local_addr().unwrap());
    let host = JoinHost::new(&join_key_artifact(&base, &future_expiry()));
    for signal in [
        nix::sys::signal::Signal::SIGINT,
        nix::sys::signal::Signal::SIGTERM,
    ] {
        let terminal = open_test_pty(None).unwrap();
        let mut serve = host.serve_with_input(
            &["--join-key"],
            &[],
            StandardInput::Descriptor(&terminal.slave),
        );
        poll_until(
            "join key prompt",
            || (echoes(&terminal.slave), serve.exited()),
            |(echo, exited)| !echo || exited.is_some(),
        );
        let id = i32::try_from(serve.child.id()).unwrap();
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(id), signal).unwrap();

        let (exit, stderr) = serve.wait_for_exit();

        assert_eq!(exit.signal(), Some(signal as i32), "{signal}: {stderr}");
        assert!(echoes(&terminal.slave), "{signal}");
        assert!(!host.state.exists());
        assert!(!host.journal.exists());
    }
    cloud.set_nonblocking(true).unwrap();
    assert_eq!(
        cloud.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}
