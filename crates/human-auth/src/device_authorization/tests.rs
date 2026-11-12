use std::io::Write as _;
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use super::*;
use anyhow::Context as _;
use um_api::MAX_RESPONSE_BODY_BYTES;
use um_test_support::{ScriptedHttpServer, read_request};

fn deployment(server: &ScriptedHttpServer) -> anyhow::Result<Deployment> {
    let issuer = format!(
        "{}tenant/",
        server
            .api_url
            .strip_suffix("api/")
            .context("fixture API URL should end with api/")?
    );
    Ok(Deployment::for_test(
        "http://api.fixture.example".to_owned(),
        issuer,
    )?)
}

fn response(status: &str, content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    let content_type = content_type
        .map(|value| format!("Content-Type: {value}\r\n"))
        .unwrap_or_default();
    let mut response = format!(
        "HTTP/1.1 {status}\r\nConnection: close\r\n{content_type}Content-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}

fn json_response(status: &str, value: serde_json::Value) -> anyhow::Result<Vec<u8>> {
    Ok(response(
        status,
        Some("application/json; charset=utf-8"),
        &serde_json::to_vec(&value).context("fixture value should exist")?,
    ))
}

fn keep_alive_json_response(status: &str, value: serde_json::Value) -> anyhow::Result<Vec<u8>> {
    let body = serde_json::to_vec(&value).context("fixture value should exist")?;
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(&body);
    Ok(response)
}

fn request_form(request: &str) -> std::collections::HashMap<String, String> {
    let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
    url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect()
}

// REQ-AUTH-23
#[test]
fn device_authorization_requests_the_exact_client_audience_and_scopes() -> anyhow::Result<()> {
    let server = ScriptedHttpServer::respond(json_response(
        "200 OK",
        serde_json::json!({
            "device_code": "unique-private-device-code",
            "user_code": "ABCD-EFGH",
            "verification_uri": "https://auth.fixture.example/activate",
            "verification_uri_complete": "https://auth.fixture.example/activate?user_code=ABCD-EFGH",
            "expires_in": 600,
            "interval": 2
        }),
    )?);
    let deployment = deployment(&server)?;
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp)
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    let authorization =
        authorize(&client, &deployment).map_err(|error| anyhow::anyhow!("{error}"))?;

    check_eq!(authorization.device_code(), "unique-private-device-code");
    check_eq!(authorization.user_code(), "ABCD-EFGH");
    check_eq!(authorization.expires_in(), Duration::from_secs(600));
    check_eq!(authorization.interval(), Duration::from_secs(2));
    let debug = format!("{authorization:?}");
    check!(!debug.contains("unique-private-device-code"));
    check!(!debug.contains("ABCD-EFGH"));
    check!(!debug.contains("auth.fixture.example"));

    let requests = server.finish();
    check!(requests[0].starts_with("POST /tenant/oauth/device/code HTTP/1.1\r\n"));
    let form = request_form(&requests[0]);
    check_eq!(
        form.get("client_id")
            .context("fixture value should exist")?,
        "fixture-public-client"
    );
    check_eq!(
        form.get("audience").context("fixture value should exist")?,
        "https://api.fixture.example"
    );
    check_eq!(
        form.get("scope").context("fixture value should exist")?,
        "openid profile email offline_access"
    );
    check!(!form.contains_key("refresh_token"));
    Ok(())
}

#[test]
fn device_authorization_defaults_the_poll_interval_and_allows_missing_complete_uri()
-> anyhow::Result<()> {
    let authorization = decode_device_authorization(
        &serde_json::to_vec(&serde_json::json!({
            "device_code": "private",
            "user_code": "VISIBLE",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 60
        }))
        .context("fixture value should exist")?,
        HttpTransportPolicy::HttpsOnly,
    )
    .map_err(|error| anyhow::anyhow!("{error}"))?;

    check_eq!(authorization.interval(), DEFAULT_POLL_INTERVAL);
    check_eq!(authorization.verification_uri_complete(), None);
    Ok(())
}

// REQ-AUTH-53
#[test]
fn insecure_activation_urls_require_the_invocation_opt_in() -> anyhow::Result<()> {
    let body = serde_json::to_vec(&serde_json::json!({
        "device_code": "private",
        "user_code": "VISIBLE",
        "verification_uri": "http://auth.fixture.example/activate",
        "expires_in": 60
    }))
    .context("fixture value should exist")?;

    check!(matches!(
        decode_device_authorization(&body, HttpTransportPolicy::HttpsOnly),
        Err(AuthorizationError::Protocol { .. })
    ));
    check!(decode_device_authorization(&body, HttpTransportPolicy::AllowInsecureHttp).is_ok());
    Ok(())
}

// REQ-AUTH-23, REQ-AUTH-26
#[test]
fn token_polling_handles_standard_device_grant_outcomes() -> anyhow::Result<()> {
    let responses = [
        ("authorization_pending", "400 Bad Request"),
        ("slow_down", "429 Too Many Requests"),
        ("access_denied", "403 Forbidden"),
        ("expired_token", "400 Bad Request"),
    ]
    .map(|(error, status)| json_response(status, serde_json::json!({ "error": error })))
    .into_iter()
    .collect::<anyhow::Result<Vec<_>>>()?;
    let server = ScriptedHttpServer::respond_in_sequence(responses);
    let deployment = deployment(&server)?;
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp)
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    check!(matches!(
        poll_token(&client, &deployment, "private-device-code")
            .map_err(|error| anyhow::anyhow!("{error}"))?,
        TokenPoll::Pending
    ));
    check!(matches!(
        poll_token(&client, &deployment, "private-device-code")
            .map_err(|error| anyhow::anyhow!("{error}"))?,
        TokenPoll::SlowDown
    ));
    check!(matches!(
        poll_token(&client, &deployment, "private-device-code")
            .map_err(|error| anyhow::anyhow!("{error}"))?,
        TokenPoll::Denied
    ));
    check!(matches!(
        poll_token(&client, &deployment, "private-device-code")
            .map_err(|error| anyhow::anyhow!("{error}"))?,
        TokenPoll::Expired
    ));

    for request in server.finish() {
        check!(request.starts_with("POST /tenant/oauth/token HTTP/1.1\r\n"));
        let form = request_form(&request);
        check_eq!(
            form.get("grant_type")
                .context("fixture value should exist")?,
            "urn:ietf:params:oauth:grant-type:device_code"
        );
        check_eq!(
            form.get("device_code")
                .context("fixture value should exist")?,
            "private-device-code"
        );
        check_eq!(
            form.get("client_id")
                .context("fixture value should exist")?,
            "fixture-public-client"
        );
    }
    Ok(())
}

#[test]
fn token_polling_reuses_one_http_connection() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").context("fixture value should exist")?;
    let address = listener
        .local_addr()
        .context("fixture value should exist")?;
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().context("fixture value should exist")?;
        let mut requests = Vec::new();
        for error in ["authorization_pending", "access_denied"] {
            requests.push(String::from_utf8(read_request(&mut stream))?);
            stream.write_all(&keep_alive_json_response(
                "400 Bad Request",
                serde_json::json!({ "error": error }),
            )?)?;
        }
        Ok::<_, anyhow::Error>(requests)
    });
    let deployment = Deployment::for_test(
        "http://api.fixture.example".to_owned(),
        format!("http://{address}/tenant/"),
    )?;
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp)
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    check!(matches!(
        poll_token(&client, &deployment, "private-device-code")
            .map_err(|error| anyhow::anyhow!("{error}"))?,
        TokenPoll::Pending
    ));
    check!(matches!(
        poll_token(&client, &deployment, "private-device-code")
            .map_err(|error| anyhow::anyhow!("{error}"))?,
        TokenPoll::Denied
    ));

    let requests = server
        .join()
        .map_err(|_| anyhow::anyhow!("fixture server panicked"))??;
    check_eq!(requests.len(), 2);
    check!(
        requests
            .iter()
            .all(|request| request.starts_with("POST /tenant/oauth/token HTTP/1.1\r\n"))
    );
    Ok(())
}

// REQ-AUTH-24, REQ-AUTH-54
#[test]
fn issued_access_and_refresh_tokens_are_validated_and_redacted() -> anyhow::Result<()> {
    let server = ScriptedHttpServer::respond(json_response(
        "200 OK",
        serde_json::json!({
            "access_token": "unique-issued-access-token",
            "token_type": "bearer",
            "expires_in": 300,
            "refresh_token": "unique-issued-refresh-token"
        }),
    )?);
    let deployment = deployment(&server)?;
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp)
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    let TokenPoll::Issued(token) = poll_token(&client, &deployment, "private-device-code")
        .map_err(|error| anyhow::anyhow!("{error}"))?
    else {
        anyhow::bail!("expected an issued token");
    };

    check_eq!(token.access_token(), "unique-issued-access-token");
    check_eq!(token.refresh_token(), "unique-issued-refresh-token");
    check_eq!(token.expires_in(), Duration::from_secs(300));
    let debug = format!("{token:?}");
    check!(!debug.contains("unique-issued-access-token"));
    check!(!debug.contains("unique-issued-refresh-token"));
    server.finish();
    Ok(())
}

// REQ-AUTH-24
#[test]
fn invalid_device_and_token_payloads_are_protocol_errors() -> anyhow::Result<()> {
    for body in [
        serde_json::json!({
            "device_code": "",
            "user_code": "VISIBLE",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 60
        }),
        serde_json::json!({
            "device_code": "private",
            "user_code": "",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 60
        }),
        serde_json::json!({
            "device_code": "private",
            "user_code": "VISIBLE",
            "verification_uri": "javascript:alert(1)",
            "expires_in": 60
        }),
        serde_json::json!({
            "device_code": "private",
            "user_code": "VISIBLE",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 0
        }),
        serde_json::json!({
            "device_code": "private",
            "user_code": "VISIBLE",
            "verification_uri": "https://auth.fixture.example/activate",
            "expires_in": 60,
            "interval": 0
        }),
    ] {
        check!(matches!(
            decode_device_authorization(
                &serde_json::to_vec(&body).context("fixture value should exist")?,
                HttpTransportPolicy::HttpsOnly,
            ),
            Err(AuthorizationError::Protocol { .. })
        ));
    }

    let boundary_token = "x".repeat(MAX_ACCESS_TOKEN_BYTES);
    check!(
        decode_issued_token(
            &serde_json::to_vec(&serde_json::json!({
                "access_token": boundary_token,
                "refresh_token": "refresh-token",
                "token_type": "Bearer",
                "expires_in": 60
            }))
            .context("fixture value should exist")?
        )
        .is_ok()
    );

    for body in [
        serde_json::json!({
            "access_token": "token",
            "token_type": "Bearer",
            "expires_in": 60
        }),
        serde_json::json!({
            "access_token": "",
            "refresh_token": "refresh-token",
            "token_type": "Bearer",
            "expires_in": 60
        }),
        serde_json::json!({
            "access_token": "token",
            "refresh_token": "refresh-token",
            "token_type": "MAC",
            "expires_in": 60
        }),
        serde_json::json!({
            "access_token": "token",
            "refresh_token": "refresh-token",
            "token_type": "Bearer",
            "expires_in": 0
        }),
        serde_json::json!({
            "access_token": "x".repeat(MAX_ACCESS_TOKEN_BYTES + 1),
            "refresh_token": "refresh-token",
            "token_type": "Bearer",
            "expires_in": 60
        }),
    ] {
        check!(matches!(
            decode_issued_token(&serde_json::to_vec(&body).context("fixture value should exist")?),
            Err(AuthorizationError::Protocol { .. })
        ));
    }
    Ok(())
}

// REQ-AUTH-33
#[test]
fn oauth_request_deadline_bounds_the_complete_exchange() -> anyhow::Result<()> {
    // Keep the peer pending so this isolated check observes only the production
    // request deadline; release it deterministically after classification.
    let mut server =
        ScriptedHttpServer::respond_when_released(json_response("200 OK", serde_json::json!({}))?);

    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let Err(error) = post_form_with_timeout(
        &client,
        Url::parse(&format!("{}oauth/token", server.api_url))
            .context("fixture value should exist")?,
        &[],
        Duration::from_millis(20),
    ) else {
        server.release_response();
        server.finish_one();
        anyhow::bail!("slow OAuth request should time out");
    };

    check!(matches!(
        error,
        AuthorizationError::Unreachable(UnreachableCategory::Timeout)
    ));
    server.release_response();
    server.finish_one();
    Ok(())
}

// REQ-AUTH-33, REQ-AUTH-56
#[test]
fn redirects_oversized_responses_and_temporary_failures_are_classified() -> anyhow::Result<()> {
    let oversized = format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        MAX_RESPONSE_BODY_BYTES + 1
    )
    .into_bytes();
    let redirect = b"HTTP/1.1 302 Found\r\nConnection: close\r\nLocation: http://127.0.0.1:1/escaped\r\nContent-Length: 0\r\n\r\n".to_vec();
    let server = ScriptedHttpServer::respond_in_sequence(vec![
        redirect,
        oversized,
        response("429 Too Many Requests", None, &[]),
        response("503 Service Unavailable", None, &[]),
    ]);
    let deployment = deployment(&server)?;
    let client = HttpClient::new(HttpTransportPolicy::AllowInsecureHttp)
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    check!(matches!(
        authorize(&client, &deployment),
        Err(AuthorizationError::Protocol { .. })
    ));
    check!(matches!(
        authorize(&client, &deployment),
        Err(AuthorizationError::Protocol { .. })
    ));
    check!(matches!(
        authorize(&client, &deployment),
        Err(AuthorizationError::Unreachable(
            UnreachableCategory::RateLimited
        ))
    ));
    check!(matches!(
        authorize(&client, &deployment),
        Err(AuthorizationError::Unreachable(UnreachableCategory::Server))
    ));
    server.finish();
    Ok(())
}
