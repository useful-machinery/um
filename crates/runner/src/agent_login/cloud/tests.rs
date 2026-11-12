use std::net::TcpListener;
use std::time::Duration;

use url::Url;

use super::*;
use crate::credential::test_credential;
use crate::runner_http::test_server::{self, RecordedRequest};

const AGENT_LOGIN: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
const MATERIAL: &str = r#"{"tokens":{"access_token":"sentinel-agent-login-material"}}"#;

fn client(endpoint: &Url) -> HttpAgentLoginCloud {
    HttpAgentLoginCloud::new(endpoint, &test_credential()).unwrap()
}

fn header<'a>(request: &'a RecordedRequest, name: &str) -> Option<&'a str> {
    request.head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then_some(value.trim())
    })
}

fn status_request() -> StatusRequest {
    StatusRequest {
        os: OperatingSystem::Linux,
        providers: vec![ProviderReport {
            provider: AgentLoginProvider::Codex,
            installed: Some(InstalledLogin {
                agent_login_id: AGENT_LOGIN.to_owned(),
                generation: 2,
            }),
            state: Some(AgentLoginState::Ready),
        }],
    }
}

fn delivery_request() -> DeliveryRequest {
    DeliveryRequest {
        os: OperatingSystem::Macos,
        provider: AgentLoginProvider::Codex,
        agent_login_id: AGENT_LOGIN.to_owned(),
        generation: 3,
    }
}

fn answer_status(
    status: &str,
    headers: &str,
    body: &str,
) -> Result<Vec<ProviderLogin>, CloudFailure> {
    let (endpoint, _request, server) = test_server::respond_once(status, headers, body);
    let result = client(&endpoint).status(&status_request());
    server.join().unwrap();
    result
}

fn answer_delivery(status: &str, headers: &str, body: &str) -> Result<Delivery, DeliveryFailure> {
    let (endpoint, _request, server) = test_server::respond_once(status, headers, body);
    let result = client(&endpoint).delivery(&delivery_request());
    server.join().unwrap();
    result
}

fn delivery_answer() -> String {
    let material = base64::engine::general_purpose::STANDARD.encode(MATERIAL);
    format!(
        r#"{{"schemaVersion":1,"provider":"codex","agentLoginId":"{AGENT_LOGIN}","generation":3,"recordSchemaVersion":1,"material":"{material}"}}"#
    )
}

// REQ-AGENTLOGIN-15
#[test]
fn status_posts_the_runner_report_with_its_bearer_and_reads_each_provider() {
    let answer = format!(
        r#"{{"schemaVersion":1,"logins":[{{"provider":"codex","state":"authorized","agentLoginId":"{AGENT_LOGIN}","generation":3,"recordSchemaVersion":1}}]}}"#
    );
    let (endpoint, request, server) =
        test_server::respond_once("200 OK", &test_server::broker_headers("", &answer), &answer);

    let logins = client(&endpoint).status(&status_request()).unwrap();

    let request = request.recv().unwrap();
    server.join().unwrap();
    assert!(
        request
            .head
            .starts_with("POST /v1/runner/agent-logins/status HTTP/1.1\r\n")
    );
    assert_eq!(
        header(&request, "authorization"),
        Some(format!("Bearer {}", test_credential().bearer_value()).as_str())
    );
    assert_eq!(header(&request, "content-type"), Some("application/json"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "os": "linux",
            "providers": [{
                "provider": "codex",
                "recordSchemaVersions": [1],
                "installed": {"agentLoginId": AGENT_LOGIN, "generation": 2},
                "state": "ready",
            }],
        })
    );
    assert_eq!(
        logins,
        [ProviderLogin {
            provider: AgentLoginProvider::Codex,
            agent_login_id: AGENT_LOGIN.to_owned(),
            status: LoginStatus::Authorized {
                generation: 3,
                record_schema_version: 1,
            },
        }]
    );
}

// REQ-AGENTLOGIN-15
#[test]
fn status_omits_what_the_runner_has_not_got_and_derives_the_https_base() {
    let (endpoint, request, server) = test_server::respond_once(
        "200 OK",
        &test_server::broker_headers("", "{}"),
        r#"{"schemaVersion":1,"logins":[]}"#,
    );
    let mut report = status_request();
    report.os = OperatingSystem::Macos;
    report.providers[0].installed = None;
    report.providers[0].state = None;

    assert_eq!(client(&endpoint).status(&report), Ok(Vec::new()));
    let request = request.recv().unwrap();
    server.join().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "os": "macos",
            "providers": [{"provider": "codex", "recordSchemaVersions": [1]}],
        })
    );

    let secure = client(&Url::parse("wss://cloud.example/v1/runner/connect?boot=1").unwrap());
    let secure = secure.target();
    assert_eq!(
        secure.status_endpoint.as_str(),
        "https://cloud.example/v1/runner/agent-logins/status"
    );
    assert_eq!(
        secure.delivery_endpoint.as_str(),
        "https://cloud.example/v1/runner/agent-logins/delivery"
    );
    assert!(
        HttpAgentLoginCloud::new(
            &Url::parse("https://cloud.example/").unwrap(),
            &test_credential()
        )
        .is_none()
    );
}

// REQ-AGENTLOGIN-15, REQ-AGENTLOGIN-34
#[test]
fn status_names_claude_code_with_the_record_schema_versions_this_build_installs() {
    let (endpoint, request, server) = test_server::respond_once(
        "200 OK",
        &test_server::broker_headers("", "{}"),
        r#"{"schemaVersion":1,"logins":[]}"#,
    );
    let mut report = status_request();
    report.providers.push(ProviderReport {
        provider: AgentLoginProvider::ClaudeCode,
        installed: None,
        state: Some(AgentLoginState::ForeignLogin),
    });

    assert_eq!(client(&endpoint).status(&report), Ok(Vec::new()));
    let request = request.recv().unwrap();
    server.join().unwrap();
    let body = serde_json::from_str::<serde_json::Value>(&request.body).unwrap();
    assert_eq!(
        body["providers"][1],
        serde_json::json!({
            "provider": "claude_code",
            "recordSchemaVersions": [1],
            "state": "foreign_login",
        })
    );
}

// REQ-AGENTLOGIN-15
#[test]
fn status_drops_a_provider_this_build_does_not_know_and_reads_each_refusal_state() {
    let answer = format!(
        r#"{{"schemaVersion":1,"logins":[{{"provider":"future_agent","state":"authorized","agentLoginId":"{AGENT_LOGIN}","generation":1,"recordSchemaVersion":1}},{{"provider":"claude_code","state":"authorized","agentLoginId":"{AGENT_LOGIN}","generation":2,"recordSchemaVersion":1}},{{"provider":"codex","state":"revoked","agentLoginId":"{AGENT_LOGIN}"}}]}}"#
    );
    assert_eq!(
        answer_status("200 OK", &test_server::broker_headers("", &answer), &answer),
        Ok(vec![
            ProviderLogin {
                provider: AgentLoginProvider::ClaudeCode,
                agent_login_id: AGENT_LOGIN.to_owned(),
                status: LoginStatus::Authorized {
                    generation: 2,
                    record_schema_version: 1,
                },
            },
            ProviderLogin {
                provider: AgentLoginProvider::Codex,
                agent_login_id: AGENT_LOGIN.to_owned(),
                status: LoginStatus::Revoked,
            },
        ])
    );
    for (state, status) in [
        ("unauthorized", LoginStatus::Unauthorized),
        ("unsupported_target", LoginStatus::UnsupportedTarget),
    ] {
        let answer = format!(
            r#"{{"schemaVersion":1,"logins":[{{"provider":"codex","state":"{state}","agentLoginId":"{AGENT_LOGIN}"}}]}}"#
        );
        let logins =
            answer_status("200 OK", &test_server::broker_headers("", &answer), &answer).unwrap();
        assert_eq!(logins[0].status, status);
    }
}

// REQ-AGENTLOGIN-15
#[test]
fn status_refuses_an_answer_that_is_not_private_or_not_the_closed_shape() {
    let entry = format!(
        r#"{{"provider":"codex","state":"authorized","agentLoginId":"{AGENT_LOGIN}","generation":3,"recordSchemaVersion":1}}"#
    );
    let valid = format!(r#"{{"schemaVersion":1,"logins":[{entry}]}}"#);
    let cases = [
        (
            "Content-Type: application/json\r\n".to_owned(),
            valid.clone(),
        ),
        (
            test_server::broker_headers("", "{}"),
            format!(r#"{{"schemaVersion":1,"logins":[{entry}],"next":null}}"#),
        ),
        (
            test_server::broker_headers("", "{}"),
            valid.replace(
                r#""recordSchemaVersion":1"#,
                r#""recordSchemaVersion":1,"extra":1"#,
            ),
        ),
        (
            test_server::broker_headers("", "{}"),
            format!(r#"{{"schemaVersion":2,"logins":[{entry}]}}"#),
        ),
        (
            test_server::broker_headers("", "{}"),
            valid.replace("authorized", "suspended"),
        ),
        (
            test_server::broker_headers("", "{}"),
            format!(r#"{{"schemaVersion":1,"logins":[{entry},{entry}]}}"#),
        ),
    ];
    for (headers, body) in cases {
        assert_eq!(
            answer_status("200 OK", &headers, &body),
            Err(CloudFailure::InvalidResponse),
            "{body}"
        );
    }
}

// REQ-AGENTLOGIN-16
#[test]
fn delivery_posts_the_generation_asked_for_and_yields_the_decoded_material() {
    let answer = delivery_answer();
    let (endpoint, request, server) =
        test_server::respond_once("200 OK", &test_server::broker_headers("", &answer), &answer);

    let delivery = client(&endpoint).delivery(&delivery_request()).unwrap();

    let request = request.recv().unwrap();
    server.join().unwrap();
    assert!(
        request
            .head
            .starts_with("POST /v1/runner/agent-logins/delivery HTTP/1.1\r\n")
    );
    assert_eq!(
        header(&request, "authorization"),
        Some(format!("Bearer {}", test_credential().bearer_value()).as_str())
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({
            "schemaVersion": 1,
            "os": "macos",
            "provider": "codex",
            "agentLoginId": AGENT_LOGIN,
            "generation": 3,
        })
    );
    assert_eq!(delivery.provider, AgentLoginProvider::Codex);
    assert_eq!(delivery.agent_login_id, AGENT_LOGIN);
    assert_eq!(delivery.generation, 3);
    assert_eq!(delivery.record_schema_version, 1);
    assert_eq!(delivery.material.as_bytes(), MATERIAL.as_bytes());
}

// REQ-AGENTLOGIN-16
#[test]
fn delivery_refusal_yields_exactly_the_reason_cloud_names() {
    for (reason, refusal) in [
        ("unauthorized", DeliveryRefusal::Unauthorized),
        ("revoked", DeliveryRefusal::Revoked),
        ("stale_generation", DeliveryRefusal::StaleGeneration),
        ("unsupported_target", DeliveryRefusal::UnsupportedTarget),
    ] {
        let body = format!(r#"{{"schemaVersion":1,"reason":"{reason}"}}"#);
        assert_eq!(
            answer_delivery(
                "409 Conflict",
                &test_server::broker_headers("", &body),
                &body
            )
            .unwrap_err(),
            DeliveryFailure::Refused(refusal)
        );
    }
    let unknown = r#"{"schemaVersion":1,"reason":"expired"}"#;
    assert_eq!(
        answer_delivery(
            "409 Conflict",
            &test_server::broker_headers("", unknown),
            unknown
        )
        .unwrap_err(),
        DeliveryFailure::Cloud(CloudFailure::InvalidResponse)
    );
}

// REQ-AGENTLOGIN-16
#[test]
fn delivery_refuses_an_answer_for_another_generation_or_with_unusable_material() {
    let valid = delivery_answer();
    for body in [
        valid.replace(r#""generation":3"#, r#""generation":2"#),
        valid.replace(r#""provider":"codex""#, r#""provider":"claude_code""#),
        valid.replace(r#""schemaVersion":1"#, r#""schemaVersion":2"#),
        valid.replace(
            r#""recordSchemaVersion":1"#,
            r#""recordSchemaVersion":1,"extra":1"#,
        ),
        format!(
            r#"{{"schemaVersion":1,"provider":"codex","agentLoginId":"{AGENT_LOGIN}","generation":3,"recordSchemaVersion":1,"material":"not base64!"}}"#
        ),
        format!(
            r#"{{"schemaVersion":1,"provider":"codex","agentLoginId":"{AGENT_LOGIN}","generation":3,"recordSchemaVersion":1,"material":""}}"#
        ),
    ] {
        assert_eq!(
            answer_delivery("200 OK", &test_server::broker_headers("", &body), &body).unwrap_err(),
            DeliveryFailure::Cloud(CloudFailure::InvalidResponse),
            "{body}"
        );
    }
    assert_eq!(
        answer_delivery("200 OK", "Content-Type: application/json\r\n", &valid).unwrap_err(),
        DeliveryFailure::Cloud(CloudFailure::InvalidResponse)
    );
}

// REQ-AGENTLOGIN-15
// REQ-AGENTLOGIN-16
// REQ-TELEMETRY-45
#[test]
fn a_cloud_without_the_routes_or_failing_is_unavailable_with_its_status() {
    for (status, code, headers) in [
        ("404 Not Found", 404, ""),
        ("500 Internal Server Error", 500, ""),
        ("502 Bad Gateway", 502, ""),
        (
            "503 Service Unavailable",
            503,
            "Cache-Control: private, no-store\r\n",
        ),
    ] {
        let unavailable = CloudFailure::Status(code);
        assert_eq!(
            answer_status(status, headers, ""),
            Err(unavailable),
            "{status}"
        );
        assert_eq!(
            answer_delivery(status, headers, "").unwrap_err(),
            DeliveryFailure::Cloud(unavailable),
            "{status}"
        );
    }

    let unavailable = CloudFailure::Transport;

    let closed = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let endpoint = Url::parse(&format!(
        "ws://{}/v1/runner/connect",
        closed.local_addr().unwrap()
    ))
    .unwrap();
    drop(closed);
    assert_eq!(
        client(&endpoint).status(&status_request()),
        Err(unavailable)
    );
    assert_eq!(
        client(&endpoint).delivery(&delivery_request()).unwrap_err(),
        DeliveryFailure::Cloud(unavailable)
    );
}

// REQ-AGENTLOGIN-15
// REQ-AGENTLOGIN-16
// REQ-TELEMETRY-45
#[test]
fn a_request_cloud_does_not_answer_in_time_fails_in_transport() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let endpoint = Url::parse(&format!(
        "ws://{}/v1/runner/connect",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let (release, released) = std::sync::mpsc::sync_channel::<()>(0);
    let server = std::thread::spawn(move || {
        let mut held = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            test_server::read_request(&mut stream);
            held.push(stream);
        }
        released.recv().unwrap();
    });
    let mut cloud = client(&endpoint);
    cloud.timeout = Duration::from_millis(200);
    let unavailable = CloudFailure::Transport;

    assert_eq!(cloud.status(&status_request()), Err(unavailable));
    assert_eq!(
        cloud.delivery(&delivery_request()).unwrap_err(),
        DeliveryFailure::Cloud(unavailable)
    );
    release.send(()).unwrap();
    server.join().unwrap();
}

// REQ-AGENTLOGIN-16
#[test]
fn material_never_appears_in_debug() {
    let answer = delivery_answer();
    let encoded = base64::engine::general_purpose::STANDARD.encode(MATERIAL);
    let delivery = answer_delivery("200 OK", &test_server::broker_headers("", &answer), &answer);
    let cloud = client(&Url::parse("ws://127.0.0.1:9/v1/runner/connect").unwrap());

    for rendered in [
        format!("{delivery:?}"),
        format!("{delivery:#?}"),
        format!("{:?}", delivery.as_ref().unwrap().material),
        format!("{cloud:?}"),
    ] {
        assert!(
            !rendered.contains("sentinel-agent-login-material"),
            "{rendered}"
        );
        assert!(!rendered.contains(&encoded), "{rendered}");
        assert!(
            !rendered.contains(test_credential().bearer_value()),
            "{rendered}"
        );
    }
}
