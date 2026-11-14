use super::*;

use std::io::{BufRead as _, BufReader};
use std::os::unix::net::UnixListener;
use std::sync::{Arc, Condvar, Mutex};

use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::{HeaderValue, header};

pub(super) fn write_status_config(
    directory: &tempfile::TempDir,
    socket_path: &std::path::Path,
) -> String {
    let config = directory.path().join("runner.json");
    write_status_config_at(&config, socket_path);
    config.to_string_lossy().into_owned()
}

/// Writes at `config` an operator configuration naming `socket_path` as its
/// control socket and a state and work root a status must not read.
pub(super) fn write_status_config_at(config: &std::path::Path, socket_path: &std::path::Path) {
    fs::write(
        config,
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "deploymentMode": "production",
            "runnerStatePath": config.with_file_name("state-that-status-must-not-read.json"),
            "controlSocketPath": socket_path,
            "workRoot": "/path-status-must-not-read"
        }))
        .unwrap(),
    )
    .unwrap();
}

/// The control socket request `um runner status` sends.
pub(super) const STATUS_REQUEST: &[u8] = b"{\"schemaVersion\":1,\"operation\":\"status\"}\n";

/// A live runner's answer to a status request: connected, one assignment
/// running, boot `rbt_01k0z6r1w8f4jy2m7q9v3x5abc`.
pub(super) const CONNECTED_STATUS: &[u8] = b"{\"schemaVersion\":1,\"outcome\":\"ok\",\"status\":{\"processState\":\"running\",\"bootId\":\"rbt_01k0z6r1w8f4jy2m7q9v3x5abc\",\"uptimeMilliseconds\":273600000,\"connectionState\":\"connected\",\"lastConnectedAt\":\"2026-08-06T19:00:00Z\",\"currentCredentialId\":\"rrc_01k0z6r1w8f4jy2m7q9v3x5abc\",\"assignmentCounts\":{\"preparing\":0,\"accepted\":0,\"running\":1,\"finishing\":0,\"reporting\":0}}}\n";

/// The protected state of a host enrolled to connect to `endpoint`.
pub(super) fn enrolled_state_document(endpoint: &str) -> serde_json::Value {
    let credential_secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0_u8; 32]);
    serde_json::json!({
        "schemaVersion": 1,
        "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
        "connectionUrl": endpoint,
        "currentCredential": {
            "id": "rrc_01k0z6r1w8f4jy2m7q9v3x5abc",
            "secret": credential_secret,
            "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abc",
            "enrolledAt": "2026-08-06T12:00:00Z"
        },
        "updatedAt": "2026-08-06T12:00:00Z"
    })
}

pub(super) struct ServeStatusFixture {
    root: tempfile::TempDir,
    _runtime: tempfile::TempDir,
    pub(super) config: String,
}

impl ServeStatusFixture {
    pub(super) fn new(endpoint: &str) -> Self {
        Self::new_inner(endpoint, false)
    }

    fn with_pending(endpoint: &str) -> Self {
        Self::new_inner(endpoint, true)
    }

    fn new_inner(endpoint: &str, pending: bool) -> Self {
        let root = private_credential_directory();
        let runtime = tempfile::tempdir_in("/tmp").unwrap();
        fs::set_permissions(runtime.path(), Permissions::from_mode(0o700)).unwrap();
        let state_directory = root.path().join("state");
        let source = root.path().join("source");
        let work = root.path().join("work");
        fs::create_dir(&state_directory).unwrap();
        fs::set_permissions(&state_directory, Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(&source).unwrap();
        fs::create_dir(&work).unwrap();
        fs::set_permissions(&work, Permissions::from_mode(0o700)).unwrap();
        fs::write(
            source.join("workflow.yaml"),
            "schemaVersion: 1\nsteps: {}\n",
        )
        .unwrap();
        let state_path = state_directory.join("runner.json");
        let mut state = enrolled_state_document(endpoint);
        if pending {
            let pending_secret =
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([2_u8; 32]);
            state["pendingCredential"] = serde_json::json!({
                "id": "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
                "secret": pending_secret,
                "activationId": "rna_01k0z6r1w8f4jy2m7q9v3x5abd",
                "enrolledAt": "2026-08-06T13:00:00Z"
            });
        }
        fs::write(&state_path, serde_json::to_vec(&state).unwrap()).unwrap();
        fs::set_permissions(&state_path, Permissions::from_mode(0o600)).unwrap();
        let config_path = root.path().join("config.json");
        fs::write(
            &config_path,
            serde_json::to_vec(&serde_json::json!({
                "schemaVersion": 1,
                "deploymentMode": "development",
                "runnerStatePath": state_path,
                "controlSocketPath": runtime.path().join("runner.sock"),
                "workRoot": work
            }))
            .unwrap(),
        )
        .unwrap();
        Self {
            root,
            _runtime: runtime,
            config: config_path.to_string_lossy().into_owned(),
        }
    }

    /// The directory of `runnerStatePath`, where the runner keeps its
    /// readiness document.
    pub(super) fn state_directory(&self) -> std::path::PathBuf {
        self.root.path().join("state")
    }
}

pub(super) fn cloud_welcome() -> Message {
    Message::Text(
        serde_json::json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abh",
            "sentAt": "2026-07-23T00:00:00Z",
            "type": "welcome",
            "payloadVersion": 1,
            "payload": {
                "sessionId": "rsn_01k0z6r1w8f4jy2m7q9v3x5abj",
                "pingIntervalSeconds": 10,
                "pongTimeoutSeconds": 30,
                "leasePolicy": {
                    "schemaVersion": 2,
                    "forceStopAndReapBudgetMilliseconds": 5000,
                    "terminalReportDeliveryBudgetMilliseconds": 5000,
                    "renewalDeliveryBudgetMilliseconds": 5000,
                    "leaseDurationMilliseconds": 371000,
                    "fencingMarginMilliseconds": 11000
                }
            }
        })
        .to_string()
        .into(),
    )
}

pub(super) fn cloud_observation_ack(message_id: &str, sequence: u64) -> Message {
    Message::Text(
        serde_json::json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abk",
            "sentAt": "2026-07-23T00:00:01Z",
            "type": "observation_ack",
            "payloadVersion": 1,
            "payload": {
                "acknowledgedMessageId": message_id,
                "acknowledgedSequence": sequence
            }
        })
        .to_string()
        .into(),
    )
}

fn cloud_assignment_offer_with_mismatched_sources() -> Message {
    Message::Text(
        serde_json::json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abm",
            "sentAt": "2026-07-23T00:00:02Z",
            "type": "assignment_offer",
            "payloadVersion": 1,
            "payload": {
                "effectId": "eff_01k0z6r1w8f4jy2m7q9v3x5abg",
                "assignmentId": "asn_01k0z6r1w8f4jy2m7q9v3x5abn",
                "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abp",
                "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                "attemptNumber": 1,
                "executionSpec": {
                    "executionSpecId": "xsp_01k0z6r1w8f4jy2m7q9v3x5abq",
                    "schemaVersion": 1,
                    "executionLimits": {
                        "maximumParallelSteps": 1,
                        "cancellationGraceSeconds": 1
                    },
                    "sourceBranch": "main",
                    "workflowDefinitionSource": {
                        "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
                        "objectFormat": "sha1",
                        "commitOid": "0123456789abcdef0123456789abcdef01234567",
                        "workflowPath": "workflow.yaml",
                        "workflowSourceClosureDigest": {
                            "algorithm": "sha256",
                            "value": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        }
                    },
                    "primaryWorkspaceSource": {
                        "kind": "connected_repository",
                        "providerKind": "github",
                        "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
                        "objectFormat": "sha1",
                        "commitOid": "1123456789abcdef0123456789abcdef01234567",
                        "materializationContract": "git_full_clone_v1"
                    },
                    "capacity": {
                        "executionContract": "workflow_v1_cloud_inputs_artifacts@1",
                        "sourceClosureDigest": {
                            "algorithm": "sha256",
                            "value": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        },
                        "generalMaximumTransitions": 8,
                        "selectedMaximumTransitions": 7,
                        "maximumInvocations": 1,
                        "maximumRetainedBytesPerInvocation": 4194304,
                        "diagnosticRetentionBytes": 8388608,
                        "nativeSessionRetentionBytes": 4194304,
                        "aggregateRetentionBytes": 12582912,
                        "conditionTransitionCount": 0,
                        "aggregateConditionTransitionBytes": 0,
                        "terminalResultStructureBytes": 67108864,
                        "portableResultBytes": 202027692,
                        "encodedOutboxBytes": 85458944
                    },
                    "runInputs": null
                },
                "attemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abc"
            }
        })
        .to_string()
        .into(),
    )
}

const AGENT_LOGIN_REQUEST: &[u8] = b"POST /v1/runner/agent-logins/";

/// The next connection of the serving runner other than its agent-login
/// setup, which shares the endpoint's host and port: this fake gateway has
/// no agent-login routes and answers those requests 404, as a Cloud without
/// them would.
fn accept_runner_request(listener: &TcpListener) -> std::net::TcpStream {
    loop {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        if !request.starts_with(AGENT_LOGIN_REQUEST) {
            return stream;
        }
        let _ = stream.write_all(&http_response("404 Not Found", None, b""));
    }
}

/// `accept_runner_request` on an async listener, leaving the request unread.
async fn accept_runner_connection(listener: &tokio::net::TcpListener) -> tokio::net::TcpStream {
    accept_runner_connection_answering(listener, &|_request: &[u8]| {
        http_response("404 Not Found", None, b"")
    })
    .await
}

/// `accept_runner_connection` for a fake gateway that has agent-login
/// routes: `agent_logins` is given each agent-login request it read, head
/// and body, and returns the whole HTTP answer.
pub(super) async fn accept_runner_connection_answering(
    listener: &tokio::net::TcpListener,
    agent_logins: &dyn Fn(&[u8]) -> Vec<u8>,
) -> tokio::net::TcpStream {
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let mut start = [0_u8; AGENT_LOGIN_REQUEST.len()];
        let mut peeked = 0;
        while peeked < start.len() {
            peeked = stream.peek(&mut start).await.unwrap();
            if peeked == 0 || !AGENT_LOGIN_REQUEST.starts_with(&start[..peeked]) {
                break;
            }
        }
        if start != AGENT_LOGIN_REQUEST {
            return stream;
        }
        // The runner sends each short request whole, so reading it blocks
        // this fake only briefly.
        let mut stream = stream.into_std().unwrap();
        stream.set_nonblocking(false).unwrap();
        let request = read_request(&mut stream);
        let _ = stream.write_all(&agent_logins(&request));
    }
}

#[allow(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback requires its large error type"
)]
pub(super) async fn accept_runner_socket(
    stream: tokio::net::TcpStream,
) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    tokio_tungstenite::accept_hdr_async(
        stream,
        |_request: &tokio_tungstenite::tungstenite::handshake::server::Request,
         mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            response.headers_mut().insert(
                header::SEC_WEBSOCKET_PROTOCOL,
                HeaderValue::from_static("um.runner.v1"),
            );
            Ok(response)
        },
    )
    .await
    .unwrap()
}

// REQ-RUNNERENROLL-41, REQ-RUNNERENROLL-42, REQ-RUNNERENROLL-80: a status
// without readiness, as from a runner that has set up nothing, prints no
// agent-login line.
#[test]
fn runner_status_queries_only_the_configured_local_socket() {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let config = write_status_config(&directory, &socket_path);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 64];
        let read = stream.read(&mut request).unwrap();
        assert_eq!(&request[..read], STATUS_REQUEST);
        stream.write_all(CONNECTED_STATUS).unwrap();
    });

    let output = run_with_env(
        &["runner", "status", "--config", &config],
        &[(
            "UM_API_URL",
            "https://cloud-status-must-not-be-read.example",
        )],
    );

    assert!(output.status.success());
    let expected = [
        "process:        running\n",
        "boot:           rbt_01k0z6r1w8f4jy2m7q9v3x5abc\n",
        "uptime:         3d 4h\n",
        "connection:     connected\n",
        "last connected: 2026-08-06T19:00:00Z\n",
        "credential:     ",
        "rrc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "\nassignments:    1 total (running: 1)\n",
    ]
    .concat();
    assert_eq!(output.stdout, expected.as_bytes());
    assert!(output.stderr.is_empty());
    server.join().unwrap();
}

// REQ-RUNNERENROLL-35
// REQ-TELEMETRY-1
#[test]
fn terminal_authentication_remains_locally_inspectable() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}/v1/runner/connect", listener.local_addr().unwrap());
    let fixture = ServeStatusFixture::new(&endpoint);
    let (request_seen, request_received) = mpsc::sync_channel(1);
    let (response_release, response_released) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let mut stream = accept_runner_request(&listener);
        request_seen.send(()).unwrap();
        response_released.recv().unwrap();
        stream
            .write_all(b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n")
            .unwrap();
    });
    let mut serve = Command::new(env!("CARGO_BIN_EXE_um"))
        .args(["runner", "serve", "--config", &fixture.config])
        .env("OTEL_SDK_DISABLED", "true")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let serve_stderr = serve.stderr.take().unwrap();
    #[derive(Clone, Copy)]
    enum TerminalObservation {
        Reported,
        StderrClosed,
    }
    let terminal_observation = Arc::new((Mutex::new(None), Condvar::new()));
    let reader_observation = Arc::clone(&terminal_observation);
    let stderr_reader = thread::spawn(move || {
        let mut output = String::new();
        for line in BufReader::new(serve_stderr).lines() {
            let Ok(line) = line else {
                break;
            };
            output.push_str(&line);
            output.push('\n');
            let is_terminal_authentication = serde_json::from_str::<serde_json::Value>(&line)
                .is_ok_and(|record| {
                    record["event.name"].as_str() == Some("runner.gateway_connection")
                        && record["um.connection.failure_kind"].as_str()
                            == Some("terminal_authentication")
                        && record["um.outcome"].as_str() == Some("failure")
                });
            if is_terminal_authentication {
                let (observation, changed) = &*reader_observation;
                let mut observation = observation.lock().unwrap();
                if observation.is_none() {
                    *observation = Some(TerminalObservation::Reported);
                    changed.notify_one();
                }
            }
        }
        let (observation, changed) = &*reader_observation;
        let mut observation = observation.lock().unwrap();
        if observation.is_none() {
            *observation = Some(TerminalObservation::StderrClosed);
            changed.notify_one();
        }
        output
    });
    request_received.recv().unwrap();
    response_release.send(()).unwrap();
    server.join().unwrap();

    // The completed gateway event is emitted after LiveStatus enters its terminal
    // authentication state, so it is an explicit synchronization signal for one query.
    const TERMINAL_EVENT_TIMEOUT: Duration = Duration::from_secs(30);
    let (observation, changed) = &*terminal_observation;
    let (observation, _) = changed
        .wait_timeout_while(
            observation.lock().unwrap(),
            TERMINAL_EVENT_TIMEOUT,
            |observation| observation.is_none(),
        )
        .unwrap();
    let wait_failure = match *observation {
        Some(TerminalObservation::Reported) => None,
        Some(TerminalObservation::StderrClosed) => {
            Some("Runner Serve closed stderr before reporting terminal authentication".to_owned())
        }
        None => Some(format!(
            "Runner Serve did not report terminal authentication within {TERMINAL_EVENT_TIMEOUT:?}"
        )),
    };
    drop(observation);
    if let Some(wait_failure) = wait_failure {
        let _ = serve.kill();
        let serve_status = serve.wait().unwrap();
        let serve_stderr = stderr_reader.join().unwrap();
        panic!("{wait_failure}: {serve_status:?}\n{serve_stderr}");
    }
    let status = run(&["runner", "status", "--config", &fixture.config]);
    if serve.try_wait().unwrap().is_none() {
        serve.kill().unwrap();
    }
    serve.wait().unwrap();
    let serve_stderr = stderr_reader.join().unwrap();

    assert!(
        status.status.success(),
        "Runner Serve became unreachable after terminal authentication: {serve_stderr}"
    );
    assert!(
        String::from_utf8_lossy(&status.stdout).contains("authentication_failed"),
        "unexpected status output: {}",
        String::from_utf8_lossy(&status.stdout)
    );
}

#[test]
fn pending_handshake_preserves_contiguous_current_boot_sequences() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("ws://{}/v1/runner/connect", listener.local_addr().unwrap());
    let fixture = ServeStatusFixture::with_pending(&endpoint);
    let (sequences_sent, sequences_received) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let mut current =
                    accept_runner_socket(accept_runner_connection(&listener).await).await;
                let Some(Ok(Message::Text(current_hello))) = current.next().await else {
                    panic!("current connection omitted hello");
                };
                let current_hello: serde_json::Value =
                    serde_json::from_str(&current_hello).unwrap();
                current.send(cloud_welcome()).await.unwrap();
                current
                    .send(cloud_observation_ack(
                        current_hello["messageId"].as_str().unwrap(),
                        current_hello["sequence"].as_u64().unwrap(),
                    ))
                    .await
                    .unwrap();

                // Hold the pending HTTP upgrade so any current-session frames
                // must remain contiguous before the candidate hello is sent.
                let pending_stream = accept_runner_connection(&listener).await;
                // Keep this sequencing proof independent of source-provider I/O. A
                // mismatched identity pair produces a synchronous structured rejection
                // while the candidate handshake remains deliberately blocked.
                current
                    .send(cloud_assignment_offer_with_mismatched_sources())
                    .await
                    .unwrap();
                let mut latest_current_sequence = 0;
                while latest_current_sequence < 3 {
                    let Some(Ok(Message::Text(frame))) = current.next().await else {
                        panic!("current connection omitted assignment observations");
                    };
                    let frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
                    latest_current_sequence =
                        latest_current_sequence.max(frame["sequence"].as_u64().unwrap());
                    if frame["type"] == "assignment_rejected" {
                        assert_eq!(
                            frame["payload"]["decline"],
                            serde_json::json!({
                                "type": "execution_spec_invalid",
                                "reason": "invalid_source_projection"
                            })
                        );
                        break;
                    }
                    assert_ne!(frame["type"], "assignment_accepted");
                }

                let mut pending = accept_runner_socket(pending_stream).await;
                let Some(Ok(Message::Text(pending_hello))) = pending.next().await else {
                    panic!("pending connection omitted hello");
                };
                let pending_hello: serde_json::Value =
                    serde_json::from_str(&pending_hello).unwrap();
                sequences_sent
                    .send((
                        latest_current_sequence,
                        pending_hello["sequence"].as_u64().unwrap(),
                    ))
                    .unwrap();
            });
    });
    let mut serve = Command::new(env!("CARGO_BIN_EXE_um"))
        .args(["runner", "serve", "--config", &fixture.config])
        .env("OTEL_SDK_DISABLED", "true")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let (latest_current_sequence, pending_hello_sequence) = sequences_received.recv().unwrap();
    serve.kill().unwrap();
    serve.wait().unwrap();
    server.join().unwrap();

    assert!(
        pending_hello_sequence > latest_current_sequence,
        "pending hello sequence {pending_hello_sequence} regressed behind current sequence {latest_current_sequence}"
    );
}

fn run_status_with_response(response: &'static [u8]) -> Output {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 64];
        let _ = stream.read(&mut request).unwrap();
        stream.write_all(response).unwrap();
    });
    let config = write_status_config(&directory, &socket_path);

    let output = run(&["runner", "status", "--config", &config]);

    server.join().unwrap();
    output
}

// REQ-RUNNERENROLL-42
#[test]
fn runner_status_prints_each_provider_readiness_once_the_runner_has_set_up() {
    let output = run_status_with_response(
        b"{\"schemaVersion\":1,\"outcome\":\"ok\",\"status\":{\"processState\":\"running\",\"bootId\":\"rbt_01k0z6r1w8f4jy2m7q9v3x5abc\",\"uptimeMilliseconds\":10000,\"connectionState\":\"connected\",\"assignmentCounts\":{\"preparing\":0,\"accepted\":0,\"running\":0,\"finishing\":0,\"reporting\":0},\"agentLogins\":{\"updatedAt\":\"2026-11-12T01:00:00Z\",\"providers\":[{\"provider\":\"codex\",\"state\":\"ready\",\"agentLoginId\":\"agl_01k0z6r1w8f4jy2m7q9v3x5abc\",\"generation\":3,\"checkedAt\":\"2026-11-12T01:00:00Z\",\"installedAt\":\"2026-11-12T00:55:00Z\"},{\"provider\":\"claude_code\",\"state\":\"foreign_login\",\"checkedAt\":\"2026-11-12T01:00:00Z\"},{\"provider\":\"github\",\"state\":\"not_delivered\"},{\"provider\":\"gitlab\",\"state\":\"not_delivered\"},{\"provider\":\"linear\",\"state\":\"not_delivered\"}]}}}\n",
    );

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let expected = [
        "process:        running\n",
        "boot:           rbt_01k0z6r1w8f4jy2m7q9v3x5abc\n",
        "uptime:         10s\n",
        "connection:     connected\n",
        "assignments:    0 total\n",
    ]
    .concat();
    assert!(stdout.starts_with(&expected), "{stdout}");
    // A readiness without `selected` or `lastUse`, as an earlier runner
    // reports it, gives the availability its state alone gives
    // (REQ-GRANT-53).
    super::runner_agent_login::assert_provider_lines(
        &stdout,
        &[
            ("codex", "ready", "unverified"),
            ("claude_code", "foreign_login", "not_delivered"),
            ("github", "not_delivered", "not_delivered"),
            ("gitlab", "not_delivered", "not_delivered"),
            ("linear", "not_delivered", "not_delivered"),
        ],
    );
    assert!(output.stderr.is_empty());
}

// REQ-RUNNERENROLL-42, REQ-GRANT-53, REQ-GRANT-77
#[test]
fn runner_status_prints_the_availability_selection_and_last_use_give() {
    let output = run_status_with_response(
        b"{\"schemaVersion\":1,\"outcome\":\"ok\",\"status\":{\"processState\":\"running\",\"bootId\":\"rbt_01k0z6r1w8f4jy2m7q9v3x5abc\",\"uptimeMilliseconds\":10000,\"connectionState\":\"connected\",\"assignmentCounts\":{\"preparing\":0,\"accepted\":0,\"running\":0,\"finishing\":0,\"reporting\":0},\"agentLogins\":{\"updatedAt\":\"2026-11-12T01:00:00Z\",\"providers\":[{\"provider\":\"codex\",\"state\":\"ready\",\"agentLoginId\":\"agl_01k0z6r1w8f4jy2m7q9v3x5abc\",\"generation\":3,\"checkedAt\":\"2026-11-12T01:00:00Z\",\"installedAt\":\"2026-11-12T00:55:00Z\",\"selected\":true,\"lastUse\":\"refused\"},{\"provider\":\"claude_code\",\"state\":\"not_authorized\",\"agentLoginId\":\"agl_01k0z6r1w8f4jy2m7q9v3x5abd\",\"checkedAt\":\"2026-11-12T01:00:00Z\",\"selected\":true},{\"provider\":\"github\",\"state\":\"not_delivered\"},{\"provider\":\"gitlab\",\"state\":\"not_delivered\"},{\"provider\":\"linear\",\"state\":\"not_delivered\"}]}}}\n",
    );

    // Status reports; it does not fail for a provider's availability.
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    super::runner_agent_login::assert_provider_lines(
        &stdout,
        &[
            ("codex", "ready", "reconnect_required"),
            ("claude_code", "not_authorized", "insufficient_permission"),
            ("github", "not_delivered", "not_delivered"),
            ("gitlab", "not_delivered", "not_delivered"),
            ("linear", "not_delivered", "not_delivered"),
        ],
    );
    // Each next action is the owner's, so its line names the agent login.
    for (provider, next_action, agent_login_id) in [
        ("codex", "reconnect", "agl_01k0z6r1w8f4jy2m7q9v3x5abc"),
        (
            "claude_code",
            "restore_membership",
            "agl_01k0z6r1w8f4jy2m7q9v3x5abd",
        ),
    ] {
        let line = stdout
            .lines()
            .find(|line| line.starts_with(&format!("{provider} login:")))
            .unwrap();
        assert!(line.contains(next_action), "{line}");
        assert!(line.contains(agent_login_id), "{line}");
    }
    // Its owner reconnects it with `um grant create` (REQ-GRANT-77).
    let codex = stdout.lines().find(|line| line.starts_with("codex login:"));
    assert!(
        codex.is_some_and(|line| line.contains("`um grant create codex --replace`")),
        "{stdout}"
    );
    assert!(output.stderr.is_empty());
}

// REQ-RUNNERENROLL-44
#[test]
fn runner_status_maps_control_protocol_errors_through_the_protocol_outcome() {
    let output = run_status_with_response(
        b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"unsupported_version\"}\n",
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}

// REQ-RUNNERENROLL-44
#[test]
fn runner_status_maps_malformed_control_responses_through_the_protocol_outcome() {
    let output = run_status_with_response(b"not-json\n");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}

// REQ-RUNNERENROLL-44
#[test]
fn runner_status_reports_absent_and_refused_sockets_as_not_reachable() {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    let config = write_status_config(&directory, &socket_path);
    let output = run(&["runner", "status", "--config", &config]);
    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());

    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    drop(UnixListener::bind(&socket_path).unwrap());
    let config = write_status_config(&directory, &socket_path);
    let output = run(&["runner", "status", "--config", &config]);
    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}
