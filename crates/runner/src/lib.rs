#![cfg_attr(
    test,
    allow(
        clippy::disallowed_macros,
        clippy::expect_used,
        clippy::panic,
        clippy::unwrap_used,
        reason = "unit tests use Cargo-provided fixture paths and panic shortcuts"
    )
)]
#![cfg_attr(not(test), deny(clippy::large_futures))]

mod agent_login;
mod control_client;
mod control_protocol;
mod credential;
mod doctor;
mod enrollment;
mod service;
mod telemetry;
mod validation;

use std::fmt;

pub use control_client::{RequestFailure, request};
pub use control_protocol::{
    AgentLoginProvider, AgentLoginReadiness, AgentLoginState, AgentLoginUse, AssignmentCounts,
    ConnectionFailure, ConnectionState, ControlError, Operation, ProcessState, ProtocolFailure,
    ProviderReadiness, Response, StatusSnapshot,
};
pub use doctor::{
    CheckDescriptor, CheckResult, CredentialKind, CredentialOrigin, CredentialRequirement,
    CredentialSummary, Declaration, DeclarationKind, Node, NodeKind, OperatorConfigSource,
    OriginDeclaration, Outcome, ReadinessCode, Registry, RegistryError, Report,
    RepositoryRootError, RepositorySource, SelectionError, Status, Summary, UnknownCredential,
    UnknownDeclaration, WorkflowCredentials, built_in_registry, read_workflow_credentials,
    repository_root,
};
pub use enrollment::{
    ActivationArtifact, ActivationArtifactParts, AmbiguousCause, ArtifactDestination,
    ArtifactWriteError, EnrollmentError, EnrollmentOutcome, EnrollmentResponse, JoinKeyArtifact,
    JoinOutcome, JoinRefusal, JoinRetry, PrivateArtifactError, PrivateArtifactFile,
    PublicationStage, RecordedOptions, RegisteredRunner, RegistrationAuthority,
    RegistrationConflict, RegistrationConflictKind, RegistrationError, RegistrationOutcome,
    RegistrationStage, RegistrationTarget, ReplacementDisposition, RunnerHome, RunnerHomeError,
    confirm_registration, enroll, join_unless_enrolled, load_control_socket_path,
    register_unless_enrolled, remove_completed_join_journal, replacement_disposition,
    write_activation_file,
};
pub use service::{Config, ConfigError};

#[derive(Debug)]
pub struct ServiceError(service::ServiceError);

impl ServiceError {
    pub const fn requires_operator_recovery(&self) -> bool {
        self.0.requires_operator_recovery()
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for ServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

pub fn run(config: Config, service_version: &str) -> Result<(), ServiceError> {
    service::run(config, service_version).map_err(ServiceError)
}

#[cfg(feature = "test-fixtures")]
pub fn run_nested_workflow_delivery_failure_fixture(
    fixture_arguments: &[String],
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(anyhow::Error::from)?;
    runtime.block_on(service::run_nested_workflow_delivery_failure_scenario(
        fixture_arguments,
    ))
}

pub fn workflow_git_helper_requested() -> bool {
    service::workflow_git_helper_requested()
}

pub fn run_workflow_git_helper() -> bool {
    service::run_workflow_git_helper()
}

fn is_loopback(endpoint: &url::Url) -> bool {
    match endpoint.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

/// The plumbing every runner request to Cloud's HTTP routes shares: the
/// source brokers of `service::source` and the agent-login requests of
/// `agent_login`. A request is a JSON POST on the HTTP base of the runner's
/// `ws(s)` endpoint, authenticated by the enrolled runner credential, bounded
/// in time and in answer size; an answer that carries a body must be private
/// and JSON.
mod runner_http {
    use std::time::Duration;

    use reqwest::StatusCode;
    use reqwest::header::{CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER};
    use serde::Deserialize;
    use url::Url;
    use zeroize::{Zeroize as _, Zeroizing};

    use crate::credential::Credential;
    use um_execution::CaptureCancellation;

    pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
    const RESPONSE_LIMIT: usize = 128 * 1024;
    const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(10);

    /// Maps the runner's `wss` endpoint to `https` and `ws` to `http`, and
    /// replaces its path.
    pub(crate) fn private_runner_http_endpoint(endpoint: &Url, path: &str) -> Result<Url, ()> {
        let mut endpoint = endpoint.clone();
        match endpoint.scheme() {
            "wss" => endpoint.set_scheme("https")?,
            "ws" => endpoint.set_scheme("http")?,
            _ => return Err(()),
        }
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        endpoint.set_path(path);
        Ok(endpoint)
    }

    /// A secret JSON string, cleared when dropped.
    #[derive(Deserialize)]
    #[serde(transparent)]
    pub(crate) struct SensitiveString(pub(crate) String);

    impl Drop for SensitiveString {
        fn drop(&mut self) {
            self.0.zeroize();
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum Failure {
        /// No answer: the connection failed or timed out.
        Unavailable,
        /// The caller's cancellation fired before the answer was read.
        Cancelled,
        /// An answer the runner must not trust.
        InvalidResponse,
    }

    pub(crate) struct Response {
        pub(crate) status: StatusCode,
        /// Whether the answer carried `Cache-Control: private, no-store`. An
        /// answer with a body always does; a caller decides for the others.
        pub(crate) private_no_store: bool,
        /// The body of a status the caller named as JSON, else empty.
        pub(crate) body: Zeroizing<Vec<u8>>,
        /// The positive `Retry-After` of a 503 or a 429.
        pub(crate) retry_after: Option<Duration>,
    }

    /// POSTs `body` to `endpoint` with the runner's bearer and reads the
    /// answer, whose body is read only for a status in `json_statuses`. It
    /// blocks on a private runtime, so it must not run on a runtime thread.
    pub(crate) fn post_json(
        endpoint: &Url,
        runner_credential: &Credential,
        body: Vec<u8>,
        json_statuses: &[StatusCode],
        timeout: Duration,
        cancellation: Option<&CaptureCancellation>,
    ) -> Result<Response, Failure> {
        um_support::install_provider();
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|_| Failure::Unavailable)?;
        let request = client
            .post(endpoint.clone())
            .bearer_auth(runner_credential.bearer_value())
            .header(CONTENT_TYPE, "application/json")
            .body(body);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Failure::Unavailable)?;
        runtime.block_on(async {
            let mut response = if let Some(cancellation) = cancellation {
                tokio::select! {
                    result = request.send() => result.map_err(|_| Failure::Unavailable)?,
                    () = wait_for_cancellation(cancellation) => return Err(Failure::Cancelled),
                }
            } else {
                request.send().await.map_err(|_| Failure::Unavailable)?
            };
            let status = response.status();
            let retry_after = match response.headers().get(RETRY_AFTER) {
                Some(value)
                    if matches!(
                        status,
                        StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS
                    ) =>
                {
                    let seconds = value
                        .to_str()
                        .ok()
                        .and_then(|value| value.parse::<u64>().ok())
                        .filter(|seconds| *seconds > 0)
                        .ok_or(Failure::InvalidResponse)?;
                    Some(Duration::from_secs(seconds))
                }
                _ => None,
            };
            let private_no_store = response
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok())
                == Some("private, no-store");
            let mut body = Zeroizing::new(Vec::new());
            if !json_statuses.contains(&status) {
                return Ok(Response {
                    status,
                    private_no_store,
                    body,
                    retry_after,
                });
            }
            if !private_no_store
                || response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    != Some("application/json")
            {
                return Err(Failure::InvalidResponse);
            }
            loop {
                let chunk = if let Some(cancellation) = cancellation {
                    tokio::select! {
                        result = response.chunk() => result.map_err(|_| Failure::Unavailable)?,
                        () = wait_for_cancellation(cancellation) => return Err(Failure::Cancelled),
                    }
                } else {
                    response.chunk().await.map_err(|_| Failure::Unavailable)?
                };
                let Some(chunk) = chunk else {
                    break;
                };
                if body.len().saturating_add(chunk.len()) > RESPONSE_LIMIT {
                    return Err(Failure::InvalidResponse);
                }
                body.extend_from_slice(&chunk);
            }
            Ok(Response {
                status,
                private_no_store,
                body,
                retry_after,
            })
        })
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the private credential-request runtime must yield while polling its fence"
    )]
    pub(crate) async fn wait_for_cancellation(cancellation: &CaptureCancellation) {
        while !cancellation.is_cancelled() {
            tokio::time::sleep(CANCELLATION_POLL_INTERVAL).await;
        }
    }

    /// A one-request HTTP server for tests of the runner's requests to Cloud.
    #[cfg(test)]
    pub(crate) mod test_server {
        use std::io::Write as _;
        use std::net::{TcpListener, TcpStream};

        use url::Url;

        /// One request as the server read it: the request line and headers,
        /// and the body.
        pub(crate) struct RecordedRequest {
            pub(crate) head: String,
            pub(crate) body: String,
        }

        /// Serves one request on a fresh loopback port, answering with
        /// `status`, exactly `headers` and `body`; returns the runner's `ws`
        /// endpoint on that port and the request it read.
        pub(crate) fn respond_once(
            status: &str,
            headers: &str,
            body: &str,
        ) -> (
            Url,
            std::sync::mpsc::Receiver<RecordedRequest>,
            std::thread::JoinHandle<()>,
        ) {
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let address = listener.local_addr().unwrap();
            let (request_sender, request_receiver) = std::sync::mpsc::sync_channel(1);
            let status = status.to_owned();
            let headers = headers.to_owned();
            let body = body.to_owned();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                drop(listener);
                request_sender.send(read_request(&mut stream)).unwrap();
                write_response(&mut stream, &status, &headers, &body);
            });
            (
                Url::parse(&format!("ws://{address}/v1/runner/connect")).unwrap(),
                request_receiver,
                server,
            )
        }

        /// The headers of a broker answer: private, and JSON when there is a
        /// body, after `additional`.
        pub(crate) fn broker_headers(additional: &str, body: &str) -> String {
            let content_type = if body.is_empty() {
                ""
            } else {
                "Content-Type: application/json\r\n"
            };
            format!("Cache-Control: private, no-store\r\n{additional}{content_type}")
        }

        /// Frames one request with the shared loopback reader
        /// (INV-TESTING-25) and splits it at its blank line.
        pub(crate) fn read_request(stream: &mut TcpStream) -> RecordedRequest {
            let request = um_test_support::read_request(stream);
            let (head, body) =
                um_test_support::split_request(&request).expect("request head should be complete");
            RecordedRequest {
                head: String::from_utf8(head.to_vec()).unwrap(),
                body: String::from_utf8(body.to_vec()).unwrap(),
            }
        }

        pub(crate) fn write_response(
            stream: &mut TcpStream,
            status: &str,
            headers: &str,
            body: &str,
        ) {
            write!(
                stream,
                "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            )
            .unwrap();
            stream.flush().unwrap();
        }
    }
}
