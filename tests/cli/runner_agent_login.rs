use super::*;

use std::io::{BufRead as _, BufReader};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use base64::Engine as _;
use futures_util::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite::Message;

use super::grant::after_notice;
use super::runner_status::{
    ServeStatusFixture, accept_runner_connection_answering, accept_runner_socket,
    cloud_observation_ack, cloud_welcome, write_status_config,
};

pub(super) const SYNC_REQUEST: &[u8] =
    b"{\"schemaVersion\":1,\"operation\":\"sync_agent_logins\"}\n";

/// The control socket's answer to a sync whose `codex` entry is `state`.
pub(super) fn synced_response(codex: &str) -> Vec<u8> {
    let mut response = serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 1,
        "outcome": "synced",
        "agentLogins": {
            "updatedAt": "2026-11-12T01:00:00Z",
            "providers": [
                serde_json::from_str::<serde_json::Value>(codex).unwrap(),
                {"provider": "github", "state": "not_delivered"},
                {"provider": "gitlab", "state": "not_delivered"},
                {"provider": "linear", "state": "not_delivered"}
            ]
        }
    }))
    .unwrap();
    response.push(b'\n');
    response
}

pub(super) const READY_CODEX: &str = r#"{"provider":"codex","state":"ready","agentLoginId":"agl_01k0z6r1w8f4jy2m7q9v3x5abc","generation":3,"checkedAt":"2026-11-12T01:00:00Z","installedAt":"2026-11-12T00:55:00Z"}"#;

/// The sync command (REQ-GRANT-41).
pub(super) const SYNC: [&str; 3] = ["runner", "grant", "sync"];
/// Its earlier spelling, which sends the same request and prints the same
/// output with the same exit status, after a line on standard error naming
/// `um runner grant sync` (REQ-GRANT-77).
pub(super) const EARLIER_SYNC: [&str; 3] = ["runner", "agent-login", "sync"];

const BUSY_RESPONSE: &[u8] =
    b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"runner_busy\"}\n";

/// Runs `command --config PATH` and `extra` against a fake control socket
/// that answers `response` to one request, and returns the command's output
/// with the request the socket received.
fn sync_against(command: [&str; 3], response: Vec<u8>, extra: &[&str]) -> (Output, Vec<u8>) {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    let socket = ScriptedControlSocket::answering(&socket_path, response);
    let config = write_status_config(&directory, &socket_path);
    let mut args = command.to_vec();
    args.extend_from_slice(&["--config", &config]);
    args.extend_from_slice(extra);
    let output = run_with_env(
        &args,
        &[("UM_API_URL", "https://cloud-sync-must-not-be-read.example")],
    );
    (output, socket.request())
}

/// A fake control socket that answers one request with a scripted response.
pub(super) struct ScriptedControlSocket {
    path: PathBuf,
    server: thread::JoinHandle<Vec<u8>>,
}

impl ScriptedControlSocket {
    /// Binds `path` and answers `response` to the first request line.
    pub(super) fn answering(path: &Path, response: Vec<u8>) -> Self {
        let listener = UnixListener::bind(path).unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = Vec::new();
            reader.read_until(b'\n', &mut request).unwrap();
            // A command that never connected leaves only the closed stream
            // below, which cannot take the answer.
            let _ = reader.get_mut().write_all(&response);
            request
        });
        Self {
            path: path.to_owned(),
            server,
        }
    }

    /// The request the socket received, empty when the command never
    /// connected.
    pub(super) fn request(self) -> Vec<u8> {
        // Release the server if the command exited without connecting, so the
        // test fails on the empty request instead of waiting forever.
        drop(UnixStream::connect(&self.path));
        self.server.join().unwrap()
    }
}

/// Runs `command --config PATH` with no runner listening on the socket the
/// configuration names.
fn sync_with_no_live_runner(command: [&str; 3], config: &str) -> Output {
    let mut args = command.to_vec();
    args.extend_from_slice(&["--config", config]);
    run(&args)
}

// REQ-GRANT-41
#[test]
fn sync_asks_the_live_runner_and_prints_each_provider_readiness() {
    let (output, request) = sync_against(SYNC, synced_response(READY_CODEX), &[]);

    assert_eq!(request, SYNC_REQUEST);
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_provider_lines(
        &stdout,
        &[
            ("codex", "ready", "unverified"),
            ("github", "not_delivered", "not_delivered"),
            ("gitlab", "not_delivered", "not_delivered"),
            ("linear", "not_delivered", "not_delivered"),
        ],
    );
    assert!(output.stderr.is_empty());
}

// REQ-GRANT-41
// REQ-GRANT-77
#[test]
fn the_earlier_sync_spelling_prints_the_same_output_and_exit_status() {
    let refused = READY_CODEX.replace(
        r#""installedAt""#,
        r#""selected":true,"lastUse":"refused","installedAt""#,
    );
    for (response, code) in [
        (synced_response(READY_CODEX), 0),
        (synced_response(&refused), 1),
        (BUSY_RESPONSE.to_vec(), 4),
    ] {
        for extra in [&[][..], &["--json"][..]] {
            let (output, request) = sync_against(SYNC, response.clone(), extra);
            let (earlier, earlier_request) = sync_against(EARLIER_SYNC, response.clone(), extra);
            assert_eq!(request, SYNC_REQUEST);
            assert_eq!(earlier_request, SYNC_REQUEST);
            assert_eq!(output.status.code(), Some(code), "{extra:?}");
            assert_eq!(earlier.status.code(), Some(code), "{extra:?}");
            assert_eq!(earlier.stdout, output.stdout, "{extra:?}");
            assert_eq!(
                after_notice(&earlier, "um runner grant sync").as_bytes(),
                output.stderr,
                "{extra:?}"
            );
        }
    }

    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let config = write_status_config(&directory, &runtime.path().join("runner.sock"));
    let output = sync_with_no_live_runner(SYNC, &config);
    let earlier = sync_with_no_live_runner(EARLIER_SYNC, &config);
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(earlier.status.code(), Some(4));
    assert_eq!(earlier.stdout, output.stdout);
    assert_eq!(
        after_notice(&earlier, "um runner grant sync").as_bytes(),
        output.stderr
    );
}

/// Asserts that `stdout` has exactly one line per provider, in order, each
/// naming the provider's state and availability (REQ-GRANT-53).
pub(super) fn assert_provider_lines(stdout: &str, providers: &[(&str, &str, &str)]) {
    let lines = stdout
        .lines()
        .filter(|line| line.contains(" login:"))
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), providers.len(), "{stdout}");
    for (line, (provider, state, availability)) in lines.iter().zip(providers) {
        let words = line
            .split([' ', ',', ':'])
            .filter(|word| !word.is_empty())
            .collect::<Vec<_>>();
        assert_eq!(words.first(), Some(provider), "{line}");
        assert!(words.contains(state), "{line}");
        assert!(words.contains(availability), "{line}");
    }
}

// REQ-GRANT-41
#[test]
fn sync_json_prints_the_readiness_as_a_document() {
    let (output, request) = sync_against(SYNC, synced_response(READY_CODEX), &["--json"]);

    assert_eq!(request, SYNC_REQUEST);
    assert_eq!(output.status.code(), Some(0));
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["schemaVersion"], 1);
    assert_eq!(document["outcome"], "synced");
    // The control socket's readiness, each provider with its availability
    // and next action (REQ-GRANT-53).
    let mut expected = serde_json::from_slice::<serde_json::Value>(&synced_response(READY_CODEX))
        .unwrap()["agentLogins"]
        .clone();
    for (entry, (availability, next_action)) in expected["providers"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .zip([
            ("unverified", "none"),
            ("not_delivered", "none"),
            ("not_delivered", "none"),
            ("not_delivered", "none"),
        ])
    {
        entry["availability"] = availability.into();
        entry["nextAction"] = next_action.into();
    }
    assert_eq!(document["agentLogins"], expected);
}

// REQ-GRANT-41, REQ-GRANT-53
#[test]
fn sync_reports_each_providers_availability_and_fails_a_login_refused_at_its_last_use() {
    let used = |last_use: &str| {
        READY_CODEX.replace(
            r#""installedAt""#,
            &format!(r#""selected":true,"lastUse":"{last_use}","installedAt""#),
        )
    };
    for (codex, code, availability, next_action) in [
        (used("refused"), 1, "reconnect_required", "reconnect"),
        (used("accepted"), 0, "available", "none"),
        (READY_CODEX.to_owned(), 0, "unverified", "none"),
        (
            r#"{"provider":"codex","state":"not_authorized","agentLoginId":"agl_01k0z6r1w8f4jy2m7q9v3x5abc","checkedAt":"2026-11-12T01:00:00Z","selected":true}"#.to_owned(),
            1,
            "insufficient_permission",
            "restore_membership",
        ),
    ] {
        let (output, _) = sync_against(SYNC, synced_response(&codex), &["--json"]);
        assert_eq!(output.status.code(), Some(code), "{codex}");
        let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let providers = document["agentLogins"]["providers"].as_array().unwrap();
        assert_eq!(providers[0]["availability"], availability, "{codex}");
        assert_eq!(providers[0]["nextAction"], next_action, "{codex}");
        assert!(
            providers[1..]
                .iter()
                .all(|entry| entry["availability"] == "not_delivered"
                    && entry["nextAction"] == "none"),
            "{document}"
        );

        let (output, _) = sync_against(SYNC, synced_response(&codex), &[]);
        assert_eq!(output.status.code(), Some(code), "{codex}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        let codex_line = stdout.lines().next().unwrap();
        assert!(codex_line.contains(availability), "{stdout}");
        // A next action is printed only when there is one to take.
        assert_eq!(
            codex_line.contains(next_action),
            next_action != "none",
            "{stdout}"
        );
    }
}

// REQ-GRANT-41
#[test]
fn sync_exits_1_only_when_an_authorized_provider_is_not_ready() {
    for (codex, code) in [
        (
            r#"{"provider":"codex","state":"not_authorized","checkedAt":"2026-11-12T01:00:00Z"}"#,
            0,
        ),
        (
            r#"{"provider":"codex","state":"revoked","agentLoginId":"agl_01k0z6r1w8f4jy2m7q9v3x5abc","checkedAt":"2026-11-12T01:00:00Z"}"#,
            0,
        ),
        (
            r#"{"provider":"codex","state":"foreign_login","agentLoginId":"agl_01k0z6r1w8f4jy2m7q9v3x5abc","checkedAt":"2026-11-12T01:00:00Z"}"#,
            1,
        ),
        (
            r#"{"provider":"codex","state":"cloud_unavailable","checkedAt":"2026-11-12T01:00:00Z"}"#,
            1,
        ),
    ] {
        for extra in [&[][..], &["--json"][..]] {
            let (output, _) = sync_against(SYNC, synced_response(codex), extra);
            assert_eq!(output.status.code(), Some(code), "{codex} {extra:?}");
            assert!(!output.stdout.is_empty());
        }
    }
}

// REQ-GRANT-41
#[test]
fn sync_exits_4_when_no_runner_is_live_or_it_holds_an_assignment() {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let config = write_status_config(&directory, &runtime.path().join("runner.sock"));
    let absent = sync_with_no_live_runner(SYNC, &config);
    assert_eq!(absent.status.code(), Some(4));
    assert!(absent.stdout.is_empty());
    assert!(!absent.stderr.is_empty());

    let (busy, request) = sync_against(SYNC, BUSY_RESPONSE.to_vec(), &["--json"]);
    assert_eq!(request, SYNC_REQUEST);
    assert_eq!(busy.status.code(), Some(4));
    assert!(busy.stdout.is_empty());
    assert!(String::from_utf8_lossy(&busy.stderr).contains("runner_busy"));
}

// REQ-GRANT-41
#[test]
fn sync_against_an_older_runner_says_it_must_be_restarted_on_this_release() {
    let (output, _) = sync_against(
        SYNC,
        b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"invalid_request\"}\n".to_vec(),
        &[],
    );

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("restart"), "{stderr}");
    assert!(stderr.contains("um runner serve"), "{stderr}");
}

const AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
/// The Codex login Cloud delivers; its token is the sentinel no output may
/// carry.
const CODEX_MATERIAL: &str =
    r#"{"auth_mode":"chatgpt","tokens":{"access_token":"agent-login-sentinel-5d1f7b"}}"#;
const SENTINEL: &str = "agent-login-sentinel-5d1f7b";
const CLAUDE_CODE_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abd";
/// The Claude Code token Cloud delivers, which no output may carry either.
const CLAUDE_CODE_TOKEN: &str = "sk-ant-oat01-claude-code-sentinel-8e2a4c";

fn private_json_answer(body: &serde_json::Value) -> Vec<u8> {
    http_response_with_headers(
        "200 OK",
        Some("application/json"),
        &[("Cache-Control", "private, no-store")],
        body.to_string().as_bytes(),
    )
}

/// Cloud's agent-login routes for a runner authorized for one Codex login
/// and one Claude Code login, each at generation 1.
fn cloud_with_agent_logins(request: &[u8]) -> Vec<u8> {
    let authorized = |provider: &str, agent_login_id: &str| {
        serde_json::json!({
            "provider": provider,
            "state": "authorized",
            "agentLoginId": agent_login_id,
            "generation": 1,
            "recordSchemaVersion": 1
        })
    };
    let delivered = |provider: &str, agent_login_id: &str, material: &str| {
        private_json_answer(&serde_json::json!({
            "schemaVersion": 1,
            "provider": provider,
            "agentLoginId": agent_login_id,
            "generation": 1,
            "recordSchemaVersion": 1,
            "material": base64::engine::general_purpose::STANDARD.encode(material)
        }))
    };
    let for_claude_code = String::from_utf8_lossy(request).contains(r#""provider":"claude_code""#);
    if request.starts_with(b"POST /v1/runner/agent-logins/status ") {
        private_json_answer(&serde_json::json!({
            "schemaVersion": 1,
            "logins": [
                authorized("codex", AGENT_LOGIN_ID),
                authorized("claude_code", CLAUDE_CODE_LOGIN_ID)
            ]
        }))
    } else if request.starts_with(b"POST /v1/runner/agent-logins/delivery ") && for_claude_code {
        delivered("claude_code", CLAUDE_CODE_LOGIN_ID, CLAUDE_CODE_TOKEN)
    } else if request.starts_with(b"POST /v1/runner/agent-logins/delivery ") {
        delivered("codex", AGENT_LOGIN_ID, CODEX_MATERIAL)
    } else {
        cloud_without_agent_logins(request)
    }
}

/// A Cloud without the agent-login routes, which answers every request 404.
fn cloud_without_agent_logins(_request: &[u8]) -> Vec<u8> {
    http_response("404 Not Found", None, b"")
}

/// Serves the runner's connection with a welcome and its agent-login
/// requests from `agent_logins`, for as long as the test runs.
fn serve_gateway_with_agent_logins(listener: TcpListener, agent_logins: fn(&[u8]) -> Vec<u8>) {
    thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                loop {
                    let stream = accept_runner_connection_answering(&listener, &agent_logins).await;
                    tokio::spawn(async move {
                        let mut socket = accept_runner_socket(stream).await;
                        let Some(Ok(Message::Text(hello))) = socket.next().await else {
                            return;
                        };
                        let hello: serde_json::Value = serde_json::from_str(&hello).unwrap();
                        let _ = socket.send(cloud_welcome()).await;
                        let _ = socket
                            .send(cloud_observation_ack(
                                hello["messageId"].as_str().unwrap(),
                                hello["sequence"].as_u64().unwrap(),
                            ))
                            .await;
                        while let Some(Ok(_)) = socket.next().await {}
                    });
                }
            });
    });
}

/// A live `um runner serve` against a Cloud whose agent-login routes answer
/// with `agent_logins`, asked once setup has run: `sync --json`, then
/// `status` and the `credentials.agent-logins` doctor check.
struct ServedRunner {
    fixture: ServeStatusFixture,
    home: tempfile::TempDir,
    sync: Output,
    status: Output,
    doctor: Output,
    serve_stderr: String,
}

impl ServedRunner {
    fn against(agent_logins: fn(&[u8]) -> Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("ws://{}/v1/runner/connect", listener.local_addr().unwrap());
        let fixture = ServeStatusFixture::new(&endpoint);
        let home = tempfile::tempdir().unwrap();
        serve_gateway_with_agent_logins(listener, agent_logins);
        let mut serve = Command::new(env!("CARGO_BIN_EXE_um"))
            .args(["runner", "serve", "--config", &fixture.config])
            .env("OTEL_SDK_DISABLED", "true")
            .env("HOME", home.path())
            .env_remove("CODEX_HOME")
            .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
            .env_remove("CLAUDE_CONFIG_DIR")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let serve_stderr = serve.stderr.take().unwrap();
        let stderr_reader = thread::spawn(move || {
            let mut output = String::new();
            let _ = BufReader::new(serve_stderr).read_to_string(&mut output);
            output
        });

        let sync = poll_until(
            "agent-login sync against the live runner",
            || {
                run(&[
                    "runner",
                    "agent-login",
                    "sync",
                    "--json",
                    "--config",
                    &fixture.config,
                ])
            },
            |output| output.status.code() != Some(4),
        );
        let status = run(&["runner", "status", "--config", &fixture.config]);
        let doctor = run(&[
            "runner",
            "doctor",
            "--check",
            "credentials.agent-logins",
            "--json",
            "--config",
            &fixture.config,
        ]);
        serve.kill().unwrap();
        serve.wait().unwrap();
        let serve_stderr = stderr_reader.join().unwrap();
        Self {
            fixture,
            home,
            sync,
            status,
            doctor,
            serve_stderr,
        }
    }
}

// REQ-RUNNERENROLL-86, REQ-GRANT-41, REQ-GRANT-38
#[test]
fn a_runner_with_nothing_authorized_passes_against_a_cloud_without_the_routes() {
    let ServedRunner {
        sync,
        doctor,
        serve_stderr,
        ..
    } = ServedRunner::against(cloud_without_agent_logins);

    assert_eq!(
        sync.status.code(),
        Some(0),
        "{}\n{serve_stderr}",
        String::from_utf8_lossy(&sync.stderr)
    );
    let synced: serde_json::Value = serde_json::from_slice(&sync.stdout).unwrap();
    let providers = synced["agentLogins"]["providers"].as_array().unwrap();
    assert!(
        providers
            .iter()
            .all(|provider| provider["state"] != "cloud_unavailable"),
        "{synced}"
    );
    assert!(
        doctor.status.success(),
        "{}",
        String::from_utf8_lossy(&doctor.stdout)
    );
}

// REQ-GRANT-36, REQ-GRANT-37, REQ-GRANT-42, REQ-GRANT-44
#[test]
fn installed_agent_logins_reach_their_files_and_no_output() {
    let ServedRunner {
        fixture,
        home,
        sync,
        status,
        doctor,
        serve_stderr,
    } = ServedRunner::against(cloud_with_agent_logins);

    assert_eq!(
        sync.status.code(),
        Some(0),
        "{}\n{serve_stderr}",
        String::from_utf8_lossy(&sync.stderr)
    );
    let synced: serde_json::Value = serde_json::from_slice(&sync.stdout).unwrap();
    assert_eq!(synced["agentLogins"]["providers"][0]["state"], "ready");
    assert_eq!(synced["agentLogins"]["providers"][0]["generation"], 1);
    assert_eq!(
        synced["agentLogins"]["providers"][1]["provider"],
        "claude_code"
    );
    assert_eq!(synced["agentLogins"]["providers"][1]["state"], "ready");
    assert_eq!(synced["agentLogins"]["providers"][1]["generation"], 1);
    // Both logins are in the runner's own directory beside its state, and
    // nothing is written under the service's home (REQ-GRANT-36,
    // REQ-GRANT-37).
    let mode =
        |path: &std::path::Path| fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777;
    let directory = fixture.state_directory().join(".runner-agent-login");
    assert_eq!(mode(&directory), 0o700);
    let token = directory.join("claude-code.token");
    assert_eq!(fs::read_to_string(&token).unwrap(), CLAUDE_CODE_TOKEN);
    assert_eq!(mode(&token), 0o600);
    let codex_home = directory.join("codex");
    assert_eq!(mode(&codex_home), 0o700);
    assert_eq!(
        fs::read_to_string(codex_home.join("auth.json")).unwrap(),
        CODEX_MATERIAL
    );
    assert_eq!(mode(&codex_home.join("auth.json")), 0o600);
    assert_eq!(
        fs::read_to_string(codex_home.join("config.toml")).unwrap(),
        "cli_auth_credentials_store = \"file\"\n"
    );
    assert_eq!(fs::read_dir(home.path()).unwrap().count(), 0);
    assert!(status.status.success());
    let status_text = String::from_utf8_lossy(&status.stdout);
    assert!(status_text.contains("codex login:       ready"));
    assert!(status_text.contains("claude_code login: ready (generation 1)"));
    assert!(
        doctor.status.success(),
        "{}",
        String::from_utf8_lossy(&doctor.stdout)
    );
    assert!(serve_stderr.contains("runner.agent_login_setup"));

    let encoded = base64::engine::general_purpose::STANDARD.encode(CODEX_MATERIAL);
    let encoded_token = base64::engine::general_purpose::STANDARD.encode(CLAUDE_CODE_TOKEN);
    let readiness =
        fs::read_to_string(fixture.state_directory().join(".runner-agent-logins.json")).unwrap();
    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    for (name, output) in [
        ("runner standard error", serve_stderr),
        ("readiness document", readiness),
        ("runner status", text(&status.stdout)),
        ("runner status error", text(&status.stderr)),
        ("agent-login sync", text(&sync.stdout)),
        ("agent-login sync error", text(&sync.stderr)),
        ("runner doctor", text(&doctor.stdout)),
        ("runner doctor error", text(&doctor.stderr)),
    ] {
        assert!(!output.contains(SENTINEL), "{name} carries the agent login");
        assert!(!output.contains(&encoded), "{name} carries the agent login");
        assert!(
            !output.contains(CLAUDE_CODE_TOKEN),
            "{name} carries the Claude Code token"
        );
        assert!(
            !output.contains(&encoded_token),
            "{name} carries the Claude Code token"
        );
    }
}

/// The GitHub token Cloud issues for the assignment, which no output may
/// carry.
const GRANT_TOKEN: &str = "gho_grant-token-sentinel-3c9e1a";
const GRANT_ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abn";

/// The pinned assignment offer and its prepare, whose preparation expires
/// long after the test.
fn offer_and_prepare() -> (Message, Message) {
    let fixture = |name: &str| {
        let path = format!(
            "{}/tests/fixtures/runner-protocol/v1/valid/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        serde_json::from_str::<serde_json::Value>(&fs::read_to_string(path).unwrap()).unwrap()
    };
    let offer = fixture("cloud-assignment-offer.json");
    let mut prepare = fixture("cloud-assignment-prepare.json");
    prepare["payload"]["preparationExpiresAt"] = "2099-01-01T00:00:00Z".into();
    assert_eq!(offer["payload"]["assignmentId"], GRANT_ASSIGNMENT);
    assert_eq!(prepare["payload"]["assignmentId"], GRANT_ASSIGNMENT);
    (
        Message::Text(offer.to_string().into()),
        Message::Text(prepare.to_string().into()),
    )
}

/// Cloud's grant-token route, which issues the run's GitHub token and has
/// no grant of the other providers for it. Any other route is answered 404.
fn cloud_issuing_a_github_token(request: &[u8]) -> Vec<u8> {
    if !request.starts_with(b"POST /v1/runner/grants/token ") {
        return cloud_without_agent_logins(request);
    }
    let text = String::from_utf8_lossy(request);
    assert!(
        text.contains(&format!(r#""assignmentId":"{GRANT_ASSIGNMENT}""#)),
        "{text}"
    );
    assert!(text.contains(r#""bootId":"rbt_"#), "{text}");
    if text.contains(r#""provider":"github""#) {
        private_json_answer(&serde_json::json!({
            "schemaVersion": 1,
            "provider": "github",
            "grantId": "pgr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "generation": 2,
            "accessToken": GRANT_TOKEN,
            "expiresAt": "2099-01-01T00:00:00.5Z",
            "refreshAfterSeconds": 600
        }))
    } else {
        http_response_with_headers(
            "409 Conflict",
            Some("application/json"),
            &[("Cache-Control", "private, no-store")],
            br#"{"schemaVersion":1,"reason":"unauthorized"}"#,
        )
    }
}

/// Serves the runner's connection: a welcome, an acknowledgement of each
/// frame, the pinned offer and, once the runner is preparing, its prepare;
/// sends the payload of the rejection that follows to `rejected`.
async fn serve_one_assignment(
    stream: tokio::net::TcpStream,
    rejected: mpsc::SyncSender<serde_json::Value>,
) {
    let mut socket = accept_runner_socket(stream).await;
    let (offer, prepare) = offer_and_prepare();
    let mut prepare = Some(prepare);
    let mut offered = false;
    while let Some(Ok(Message::Text(frame))) = socket.next().await {
        let frame: serde_json::Value = serde_json::from_str(&frame).unwrap();
        if frame["type"] == "hello" {
            socket.send(cloud_welcome()).await.unwrap();
        }
        if let (Some(message_id), Some(sequence)) =
            (frame["messageId"].as_str(), frame["sequence"].as_u64())
        {
            socket
                .send(cloud_observation_ack(message_id, sequence))
                .await
                .unwrap();
        }
        if !offered {
            offered = true;
            socket.send(offer.clone()).await.unwrap();
        }
        if frame["type"] == "assignment_preparing"
            && let Some(prepare) = prepare.take()
        {
            socket.send(prepare).await.unwrap();
        }
        if frame["type"] == "assignment_rejected" {
            let _ = rejected.try_send(frame["payload"].clone());
        }
    }
}

// REQ-GRANT-86, REQ-GRANT-88, INV-RUNNER-28
#[test]
fn an_admitted_assignment_keeps_its_grant_token_in_its_file_and_prints_it_nowhere() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("ws://{}/v1/runner/connect", listener.local_addr().unwrap());
    let fixture = ServeStatusFixture::new(&endpoint);
    let home = tempfile::tempdir().unwrap();
    let token_file = fixture
        .state_directory()
        .join(".runner-agent-login/github.token");
    // What the token file held, and its mode, when the admission went on to
    // ask for its source credential, the step after the grant tokens.
    let (held_sender, held) = mpsc::sync_channel(1);
    let (rejected_sender, rejected) = mpsc::sync_channel(1);
    let watched = token_file.clone();
    let routes = move |request: &[u8]| {
        if request.starts_with(b"POST /v1/runner/source-credentials ") {
            let _ = held_sender.try_send(
                fs::read_to_string(&watched).ok().zip(
                    fs::symlink_metadata(&watched)
                        .ok()
                        .map(|metadata| metadata.permissions().mode() & 0o777),
                ),
            );
        }
        cloud_issuing_a_github_token(request)
    };
    thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let stream = accept_runner_connection_answering(&listener, &routes).await;
                tokio::spawn(serve_one_assignment(stream, rejected_sender));
                // The runner's HTTP requests go on arriving on this listener.
                loop {
                    drop(accept_runner_connection_answering(&listener, &routes).await);
                }
            });
    });
    let mut serve = Command::new(env!("CARGO_BIN_EXE_um"))
        .args(["runner", "serve", "--config", &fixture.config])
        .env("OTEL_SDK_DISABLED", "true")
        .env("HOME", home.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let serve_stderr = serve.stderr.take().unwrap();
    let stderr_reader = thread::spawn(move || {
        let mut output = String::new();
        let _ = BufReader::new(serve_stderr).read_to_string(&mut output);
        output
    });

    let held_at_source = poll_until(
        "the admission's request for its source credential",
        || held.try_recv().ok(),
        Option::is_some,
    );
    // The source route has no credential, so the admission is rejected and
    // the assignment ends.
    let rejection = poll_until(
        "the admission's rejection",
        || rejected.try_recv().ok(),
        Option::is_some,
    );
    let removed = poll_until(
        "the token file's removal once the assignment ended",
        || token_file.exists(),
        |exists| !exists,
    );
    let status = run(&["runner", "status", "--config", &fixture.config]);
    serve.kill().unwrap();
    serve.wait().unwrap();
    let serve_stderr = stderr_reader.join().unwrap();

    assert_eq!(
        held_at_source,
        Some(Some((GRANT_TOKEN.to_owned(), 0o600))),
        "{serve_stderr}"
    );
    assert!(rejection.is_some(), "{serve_stderr}");
    assert!(!removed);
    let readiness: serde_json::Value = serde_json::from_slice(
        &fs::read(fixture.state_directory().join(".runner-agent-logins.json")).unwrap(),
    )
    .unwrap();
    let entry = |provider: &str| {
        readiness["providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["provider"] == provider)
            .cloned()
            .unwrap()
    };
    assert_eq!(entry("github")["state"], "ready", "{readiness}");
    assert_eq!(entry("github")["generation"], 2, "{readiness}");
    assert_eq!(entry("github")["selected"], true, "{readiness}");
    for provider in ["gitlab", "linear"] {
        assert_eq!(entry(provider)["state"], "not_authorized", "{readiness}");
    }
    assert!(status.status.success());
    assert!(
        serve_stderr.contains("runner.grant_token"),
        "{serve_stderr}"
    );
    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    for (name, output) in [
        ("runner standard error", serve_stderr),
        ("readiness document", readiness.to_string()),
        ("runner status", text(&status.stdout)),
        ("runner status error", text(&status.stderr)),
    ] {
        assert!(
            !output.contains(GRANT_TOKEN),
            "{name} carries the grant token"
        );
        assert!(
            !output.contains("github.token"),
            "{name} names the token file"
        );
    }
}
