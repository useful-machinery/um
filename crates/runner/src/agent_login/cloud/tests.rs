use std::net::TcpListener;
use std::time::Duration;

use url::Url;

use super::*;
use crate::credential::test_credential;
use crate::runner_http::test_server::{self, RecordedRequest};

const AGENT_LOGIN: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
const GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abc";
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
            last_use: None,
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

// REQ-GRANT-25
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

// REQ-GRANT-25
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

// REQ-GRANT-25, REQ-GRANT-44
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
        last_use: None,
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

// REQ-GRANT-25, REQ-GRANT-52
#[test]
fn status_states_the_last_use_and_its_time_only_of_a_ready_install() {
    let installed = || {
        Some(InstalledLogin {
            agent_login_id: AGENT_LOGIN.to_owned(),
            generation: 2,
        })
    };
    let at = OffsetDateTime::from_unix_timestamp(1_794_450_000).unwrap();
    let used = |outcome| Some(LastUse { outcome, at });
    let at = Some("2026-11-12T02:20:00Z");
    for (installed, state, last_use, stated, stated_at) in [
        (
            installed(),
            Some(AgentLoginState::Ready),
            used(UseOutcome::Refused),
            Some("refused"),
            at,
        ),
        (
            installed(),
            Some(AgentLoginState::Ready),
            used(UseOutcome::Accepted),
            Some("accepted"),
            at,
        ),
        (installed(), Some(AgentLoginState::Ready), None, None, None),
        (
            installed(),
            Some(AgentLoginState::StaleGeneration),
            used(UseOutcome::Refused),
            None,
            None,
        ),
        (installed(), None, used(UseOutcome::Refused), None, None),
        (
            None,
            Some(AgentLoginState::Ready),
            used(UseOutcome::Refused),
            None,
            None,
        ),
    ] {
        let (endpoint, request, server) = test_server::respond_once(
            "200 OK",
            &test_server::broker_headers("", "{}"),
            r#"{"schemaVersion":1,"logins":[]}"#,
        );
        let report = StatusRequest {
            os: OperatingSystem::Linux,
            providers: vec![ProviderReport::new(
                AgentLoginProvider::Codex,
                installed.clone(),
                state,
                last_use,
            )],
        };

        assert_eq!(client(&endpoint).status(&report), Ok(Vec::new()));
        let request = request.recv().unwrap();
        server.join().unwrap();
        let body = serde_json::from_str::<serde_json::Value>(&request.body).unwrap();
        let entry = &body["providers"][0];
        assert_eq!(
            entry.get("lastUse").and_then(serde_json::Value::as_str),
            stated,
            "{entry}"
        );
        assert_eq!(
            entry.get("lastUseAt").and_then(serde_json::Value::as_str),
            stated_at,
            "{entry}"
        );
        assert_eq!(entry.get("state").is_some(), state.is_some(), "{entry}");
        assert_eq!(
            entry.get("installed").is_some(),
            installed.is_some(),
            "{entry}"
        );
    }
}

// REQ-GRANT-25
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

// REQ-GRANT-25
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

// REQ-GRANT-26
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

// REQ-GRANT-26
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

/// The pinned grant-token wire of `cli/tests/fixtures/runner-grant-token/v1/`.
fn grant_token_fixture(name: &str) -> String {
    let path = format!(
        "{}/../../tests/fixtures/runner-grant-token/v1/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(path).unwrap()
}

fn grant_token_request() -> GrantTokenRequest {
    let request: serde_json::Value =
        serde_json::from_str(&grant_token_fixture("request.json")).unwrap();
    GrantTokenRequest {
        boot_id: request["bootId"].as_str().unwrap().to_owned(),
        assignment_id: request["assignmentId"].as_str().unwrap().to_owned(),
        provider: AgentLoginProvider::Gitlab,
    }
}

fn answer_grant_token(
    status: &str,
    headers: &str,
    body: &str,
) -> Result<IssuedToken, GrantTokenFailure> {
    let (endpoint, _request, server) = test_server::respond_once(status, headers, body);
    let result = client(&endpoint).grant_token(&grant_token_request());
    server.join().unwrap();
    result
}

// REQ-GRANT-86, REQ-GRANT-89
#[test]
fn a_grant_token_request_posts_the_pinned_body_and_reads_the_issued_token() {
    let issued = grant_token_fixture("issued.json");
    let (endpoint, request, server) =
        test_server::respond_once("200 OK", &test_server::broker_headers("", &issued), &issued);

    let token = client(&endpoint)
        .grant_token(&grant_token_request())
        .unwrap();

    let request = request.recv().unwrap();
    server.join().unwrap();
    assert!(
        request
            .head
            .starts_with("POST /v1/runner/grants/token HTTP/1.1\r\n")
    );
    assert_eq!(
        header(&request, "authorization"),
        Some(format!("Bearer {}", test_credential().bearer_value()).as_str())
    );
    assert_eq!(header(&request, "content-type"), Some("application/json"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&request.body).unwrap(),
        serde_json::from_str::<serde_json::Value>(&grant_token_fixture("request.json")).unwrap()
    );
    assert_eq!(token.grant_id, GRANT_ID);
    assert_eq!(token.generation, 4);
    assert_eq!(
        token.access_token.as_bytes(),
        b"synthetic-grant-access-token"
    );
    // 2026-11-13T22:00:00Z.
    let expiry = OffsetDateTime::from_unix_timestamp(1_794_607_200).unwrap();
    assert_eq!(token.expires_at, expiry);
    assert_eq!(token.refresh_after, std::time::Duration::from_secs(5040));

    // Cloud writes the expiry with a fraction of a second when it has one.
    let fractional = issued.replace("2026-11-13T22:00:00Z", "2026-11-13T22:00:00.123456Z");
    let token = answer_grant_token(
        "200 OK",
        &test_server::broker_headers("", &fractional),
        &fractional,
    )
    .unwrap();
    assert_eq!(
        token.expires_at,
        expiry + time::Duration::microseconds(123_456)
    );
}

// REQ-GRANT-86
#[test]
fn a_grant_token_refusal_yields_exactly_the_reason_cloud_names() {
    let pinned = grant_token_fixture("refusal.json");
    assert_eq!(
        answer_grant_token(
            "409 Conflict",
            &test_server::broker_headers("", &pinned),
            &pinned
        )
        .unwrap_err(),
        GrantTokenFailure::Refused(GrantTokenRefusal::Revoked)
    );
    for (reason, refusal) in [
        ("unauthorized", GrantTokenRefusal::Unauthorized),
        ("revoked", GrantTokenRefusal::Revoked),
        ("reconnect_required", GrantTokenRefusal::ReconnectRequired),
        (
            "provider_unavailable",
            GrantTokenRefusal::ProviderUnavailable,
        ),
        (
            "assignment_not_current",
            GrantTokenRefusal::AssignmentNotCurrent,
        ),
    ] {
        let body = format!(r#"{{"schemaVersion":1,"reason":"{reason}"}}"#);
        assert_eq!(
            answer_grant_token(
                "409 Conflict",
                &test_server::broker_headers("", &body),
                &body
            )
            .unwrap_err(),
            GrantTokenFailure::Refused(refusal)
        );
    }
    let unknown = r#"{"schemaVersion":1,"reason":"stale_generation"}"#;
    assert_eq!(
        answer_grant_token(
            "409 Conflict",
            &test_server::broker_headers("", unknown),
            unknown
        )
        .unwrap_err(),
        GrantTokenFailure::Cloud(CloudFailure::InvalidResponse)
    );
}

// REQ-GRANT-86
#[test]
fn a_cloud_without_the_grant_token_route_authorizes_nothing_and_a_failing_one_is_unavailable() {
    assert_eq!(
        answer_grant_token("404 Not Found", "", "").unwrap_err(),
        GrantTokenFailure::NoRoute
    );
    for (status, code, headers) in [
        ("401 Unauthorized", 401, ""),
        ("429 Too Many Requests", 429, "Retry-After: 7\r\n"),
        ("503 Service Unavailable", 503, ""),
    ] {
        assert_eq!(
            answer_grant_token(status, headers, "").unwrap_err(),
            GrantTokenFailure::Cloud(CloudFailure::Status(code)),
            "{status}"
        );
    }

    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let endpoint = Url::parse(&format!(
        "ws://{}/v1/runner/connect",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let (release, released) = std::sync::mpsc::sync_channel::<()>(0);
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        test_server::read_request(&mut stream);
        released.recv().unwrap();
    });
    let mut cloud = client(&endpoint);
    cloud.timeout = Duration::from_millis(200);
    assert_eq!(
        cloud.grant_token(&grant_token_request()).unwrap_err(),
        GrantTokenFailure::Cloud(CloudFailure::Transport)
    );
    release.send(()).unwrap();
    server.join().unwrap();
}

// REQ-GRANT-86
#[test]
fn a_grant_token_answer_of_another_shape_or_an_unusable_token_is_invalid() {
    let valid = grant_token_fixture("issued.json");
    for body in [
        valid.replace(r#""provider": "gitlab.com""#, r#""provider": "gitlab""#),
        valid.replace(r#""schemaVersion": 1"#, r#""schemaVersion": 2"#),
        valid.replace(r#""generation": 4"#, r#""generation": 0"#),
        valid.replace(GRANT_ID, AGENT_LOGIN),
        valid.replace("synthetic-grant-access-token", "two words"),
        valid.replace("synthetic-grant-access-token", ""),
        valid.replace("synthetic-grant-access-token", &"t".repeat(4097)),
        valid.replace("2026-11-13T22:00:00Z", "tomorrow"),
        valid.replace(
            r#""refreshAfterSeconds": 5040"#,
            r#""refreshAfterSeconds": -1"#,
        ),
        valid.replace(
            r#""refreshAfterSeconds": 5040"#,
            r#""refreshAfterSeconds": 5040, "refreshToken": "x""#,
        ),
        "{".to_owned(),
    ] {
        assert_ne!(body, valid);
        assert_eq!(
            answer_grant_token("200 OK", &test_server::broker_headers("", &body), &body)
                .unwrap_err(),
            GrantTokenFailure::Cloud(CloudFailure::InvalidResponse),
            "{body}"
        );
    }
    // An answer that is not private is not read.
    assert_eq!(
        answer_grant_token("200 OK", "Content-Type: application/json\r\n", &valid).unwrap_err(),
        GrantTokenFailure::Cloud(CloudFailure::InvalidResponse)
    );
}

// REQ-GRANT-86
#[test]
fn a_grant_access_token_never_appears_in_debug() {
    let issued = grant_token_fixture("issued.json");
    let token = answer_grant_token("200 OK", &test_server::broker_headers("", &issued), &issued);
    // The answer is shown, all but its token.
    assert!(format!("{token:?}").contains(GRANT_ID));
    for rendered in [
        format!("{token:?}"),
        format!("{token:#?}"),
        format!("{:?}", token.as_ref().unwrap().access_token),
    ] {
        assert!(
            !rendered.contains("synthetic-grant-access-token"),
            "{rendered}"
        );
    }
}

// REQ-GRANT-26
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

// REQ-GRANT-25
// REQ-GRANT-26
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

// REQ-GRANT-25
// REQ-GRANT-26
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

// REQ-GRANT-26
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
