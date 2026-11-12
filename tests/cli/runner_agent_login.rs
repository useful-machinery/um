use super::*;

use std::io::{BufRead as _, BufReader};
use std::os::unix::net::UnixListener;

use base64::Engine as _;
use futures_util::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite::Message;

use super::runner_status::{
    ServeStatusFixture, accept_runner_connection_answering, accept_runner_socket,
    cloud_observation_ack, cloud_welcome, write_status_config,
};

const SYNC_REQUEST: &[u8] = b"{\"schemaVersion\":1,\"operation\":\"sync_agent_logins\"}\n";

/// The control socket's answer to a sync whose `codex` entry is `state`.
fn synced_response(codex: &str) -> Vec<u8> {
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

const READY_CODEX: &str = r#"{"provider":"codex","state":"ready","agentLoginId":"agl_01k0z6r1w8f4jy2m7q9v3x5abc","generation":3,"checkedAt":"2026-11-12T01:00:00Z","installedAt":"2026-11-12T00:55:00Z"}"#;

/// Runs `um runner agent-login sync --config PATH` and `extra` against a fake
/// control socket that answers `response` to one request, and returns the
/// command's output with the request the socket received.
fn sync_against(response: Vec<u8>, extra: &[&str]) -> (Output, Vec<u8>) {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let socket_path = runtime.path().join("runner.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream);
        let mut request = Vec::new();
        reader.read_until(b'\n', &mut request).unwrap();
        reader.get_mut().write_all(&response).unwrap();
        request
    });
    let config = write_status_config(&directory, &socket_path);
    let mut args = vec!["runner", "agent-login", "sync", "--config", &config];
    args.extend_from_slice(extra);
    let output = run_with_env(
        &args,
        &[("UM_API_URL", "https://cloud-sync-must-not-be-read.example")],
    );
    (output, server.join().unwrap())
}

// REQ-AGENTLOGIN-31
#[test]
fn sync_asks_the_live_runner_and_prints_each_provider_readiness() {
    let (output, request) = sync_against(synced_response(READY_CODEX), &[]);

    assert_eq!(request, SYNC_REQUEST);
    assert_eq!(output.status.code(), Some(0));
    let expected = [
        "codex login:       ready (generation 3)\n",
        "github login:      not_delivered\n",
        "gitlab login:      not_delivered\n",
        "linear login:      not_delivered\n",
    ]
    .concat();
    assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
    assert!(output.stderr.is_empty());
}

// REQ-AGENTLOGIN-31
#[test]
fn sync_json_prints_the_readiness_as_a_document() {
    let (output, request) = sync_against(synced_response(READY_CODEX), &["--json"]);

    assert_eq!(request, SYNC_REQUEST);
    assert_eq!(output.status.code(), Some(0));
    let document: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["schemaVersion"], 1);
    assert_eq!(document["outcome"], "synced");
    assert_eq!(
        document["agentLogins"],
        serde_json::from_slice::<serde_json::Value>(&synced_response(READY_CODEX)).unwrap()["agentLogins"]
    );
}

// REQ-AGENTLOGIN-31
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
            let (output, _) = sync_against(synced_response(codex), extra);
            assert_eq!(output.status.code(), Some(code), "{codex} {extra:?}");
            assert!(!output.stdout.is_empty());
        }
    }
}

// REQ-AGENTLOGIN-31
#[test]
fn sync_exits_4_when_no_runner_is_live_or_it_holds_an_assignment() {
    let directory = private_credential_directory();
    let runtime = tempfile::tempdir_in("/tmp").unwrap();
    let config = write_status_config(&directory, &runtime.path().join("runner.sock"));
    let absent = run(&["runner", "agent-login", "sync", "--config", &config]);
    assert_eq!(absent.status.code(), Some(4));
    assert!(absent.stdout.is_empty());
    assert!(!absent.stderr.is_empty());

    let (busy, request) = sync_against(
        b"{\"schemaVersion\":1,\"outcome\":\"error\",\"error\":\"runner_busy\"}\n".to_vec(),
        &["--json"],
    );
    assert_eq!(request, SYNC_REQUEST);
    assert_eq!(busy.status.code(), Some(4));
    assert!(busy.stdout.is_empty());
    assert!(String::from_utf8_lossy(&busy.stderr).contains("runner_busy"));
}

// REQ-AGENTLOGIN-31
#[test]
fn sync_against_an_older_runner_says_it_must_be_restarted_on_this_release() {
    let (output, _) = sync_against(
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

// REQ-RUNNERENROLL-86, REQ-AGENTLOGIN-31, REQ-AGENTLOGIN-28
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

// REQ-AGENTLOGIN-26, REQ-AGENTLOGIN-27, REQ-AGENTLOGIN-32, REQ-AGENTLOGIN-34
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
    // nothing is written under the service's home (REQ-AGENTLOGIN-26,
    // REQ-AGENTLOGIN-27).
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
