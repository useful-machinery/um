//! Bounded requests against a third-party provider's API: a `GET` with a bearer token for the
//! CLI's workflow wait conditions (REQ-WFEXEC-65) and a JSON `POST` with a caller-composed
//! `Authorization` value for the run mirror's Linear writes (REQ-LOCALRUN-192). The caller
//! names the URL and the credential, and this package owns the HTTP client (INV-CLI-3).

use std::fmt;
use std::time::Duration;

use reqwest::Url;
use reqwest::header::HeaderValue;

use super::http_client::blocking_client_builder;
use super::http_util::{BoundedBodyError, read_bounded_blocking_body};
use super::{HttpTransportPolicy, bearer_authorization};

/// A provider's answer: its status and a body of at most `MAX_RESPONSE_BODY_BYTES`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// A `GET` that produced no answer; its text names the cause and never the credential.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRequestError(String);

impl fmt::Display for ProviderRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ProviderRequestError {}

/// `GET url` with `Accept: accept` and `Authorization: Bearer <bearer_token>`, no redirect
/// and no retry, within `timeout`; a `url` the transport policy does not permit, a transport
/// failure and a body over the limit are errors.
pub fn provider_get(
    url: &str,
    accept: &str,
    bearer_token: &str,
    transport_policy: HttpTransportPolicy,
    timeout: Duration,
) -> Result<ProviderResponse, ProviderRequestError> {
    let authorization = bearer_authorization(bearer_token)
        .map_err(|_| ProviderRequestError("the token is not a valid header value".to_owned()))?;
    send(
        url,
        authorization,
        transport_policy,
        timeout,
        |client, url| client.get(url).header(reqwest::header::ACCEPT, accept),
    )
}

/// `POST url` with the JSON `body`, `Content-Type` and `Accept` `application/json`, and
/// `Authorization: <authorization>` exactly as given (Linear's personal keys go bare, never as
/// `Bearer`), no redirect and no retry, within `timeout`; errors as `provider_get`'s.
pub fn provider_post_json(
    url: &str,
    authorization: &str,
    body: &[u8],
    transport_policy: HttpTransportPolicy,
    timeout: Duration,
) -> Result<ProviderResponse, ProviderRequestError> {
    let authorization = HeaderValue::from_str(authorization).map_err(|_| {
        ProviderRequestError("the credential is not a valid header value".to_owned())
    })?;
    let body = body.to_vec();
    send(
        url,
        authorization,
        transport_policy,
        timeout,
        |client, url| {
            client
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header(reqwest::header::ACCEPT, "application/json")
                .body(body)
        },
    )
}

/// One request `build` makes from the client and the checked `url`, with `authorization`
/// marked sensitive, and its answer read within the body bound.
fn send(
    url: &str,
    mut authorization: HeaderValue,
    transport_policy: HttpTransportPolicy,
    timeout: Duration,
    build: impl FnOnce(&reqwest::blocking::Client, Url) -> reqwest::blocking::RequestBuilder,
) -> Result<ProviderResponse, ProviderRequestError> {
    let error = |message: String| ProviderRequestError(message);
    let parsed = Url::parse(url).map_err(|_| error(format!("{url:?} is not a URL")))?;
    if !transport_policy.permits(&parsed) {
        return Err(error(format!("{url:?} is not an https URL")));
    }
    authorization.set_sensitive(true);
    um_support::install_provider();
    let client = blocking_client_builder(transport_policy, timeout)
        .build()
        .map_err(|cause| error(format!("the HTTP client could not start: {cause}")))?;
    let response = build(&client, parsed)
        .header(reqwest::header::AUTHORIZATION, authorization)
        .send()
        .map_err(|cause| error(cause.without_url().to_string()))?;
    let status = response.status().as_u16();
    let body = read_bounded_blocking_body(response).map_err(|cause| match cause {
        BoundedBodyError::TooLarge => {
            error("the answer exceeds the response size limit".to_owned())
        }
        BoundedBodyError::Transport(cause) => error(cause.without_url().to_string()),
    })?;
    Ok(ProviderResponse { status, body })
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "provider request unit tests use Rust test assertions and fixture extraction"
)]
mod tests {
    use um_test_support::ScriptedHttpServer;

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn answer(status: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    // REQ-LOCALRUN-192: the JSON `POST` carries the caller's `Authorization` value whole, the
    // JSON media types and the body, and answers the status and body.
    #[test]
    fn provider_post_json_sends_the_body_with_the_authorization_given_whole() {
        let server = ScriptedHttpServer::respond(answer("200 OK", r#"{"data":{}}"#));
        let url = format!("{}graphql", server.api_url);
        let response = provider_post_json(
            &url,
            "lin_api_key",
            br#"{"query":"{ viewer { id } }"}"#,
            HttpTransportPolicy::AllowInsecureHttp,
            TIMEOUT,
        )
        .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, br#"{"data":{}}"#);
        let request = server.finish_one().to_ascii_lowercase();
        assert!(request.starts_with("post /api/graphql "), "{request}");
        for header in [
            "authorization: lin_api_key\r\n",
            "content-type: application/json\r\n",
            "accept: application/json\r\n",
        ] {
            assert!(request.contains(header), "{header} in {request}");
        }
        assert!(
            request.ends_with(r#"{"query":"{ viewer { id } }"}"#),
            "{request}"
        );
    }

    // REQ-LOCALRUN-192: only the development override admits `http`; the refusal names the
    // URL and never the credential.
    #[test]
    fn provider_post_json_refuses_http_unless_admitted() {
        let error = provider_post_json(
            "http://127.0.0.1:9/graphql",
            "lin_api_key",
            b"{}",
            HttpTransportPolicy::HttpsOnly,
            TIMEOUT,
        )
        .unwrap_err();
        assert!(error.to_string().contains("not an https URL"), "{error}");
        assert!(!error.to_string().contains("lin_api_key"), "{error}");
    }
}
