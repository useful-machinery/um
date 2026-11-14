use std::future::Future;
use std::ops::Deref;
use std::os::unix::fs::PermissionsExt as _;
use std::panic::Location;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use rustix::io::Errno;
use rustix::process::{Pid, test_kill_process};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderValue, header};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};
use um_test_support::watchdog::{FIXTURE_WAIT_BASE, WatchdogExpired, host_scaled_timeout};

use crate::agent_login::ServiceEnvironment;
use crate::agent_login::check::{LoginCheck, LoginCheckOutcome};
use crate::agent_login::cloud::{
    AgentLoginCloud, CloudFailure, Delivery, DeliveryFailure, DeliveryRefusal, DeliveryRequest,
    GrantAccessToken, GrantTokenFailure, GrantTokenRequest, IssuedToken, LoginMaterial,
    LoginStatus, ProviderLogin, StatusRequest,
};
use crate::agent_login::readiness::Installation;
use crate::agent_login::setup::{AssignmentGate, SetupDependencies};
use crate::control_protocol::{AgentLoginCheckCause, AgentLoginProvider};
use crate::credential::Credential;
use crate::service::config::{AssignmentConfig, Config, ConfigError, RepositoryUrlPolicy};
use crate::service::connection::{ConnectionError, FrameSource, run_established_shared};
pub(crate) use crate::service::lease_clock::fixture_lease_clock;
use crate::service::{
    ConnectionAttempt, ConnectionFuture, Connector, Shutdown, ShutdownFuture, Sleeper,
};
use crate::telemetry::Recorder;

pub(crate) type FixtureSocket = WebSocketStream<TcpStream>;

const INTERNAL_WORKER_EXECUTABLE_VARIABLE: &str = "UM_TEST_INTERNAL_WORKER_EXECUTABLE";

/// The built `internal-worker` example that guards every command step a runner test
/// executes, as `cli/scripts/check-suite` exports it. Without it the execution crate
/// takes the example beside this test binary, never the binary itself (INV-TESTING-20),
/// and `cargo test -p um-runner` does not build that example, so every step would fail
/// at launch. A runner fixture therefore refuses to exist while the variable is unset,
/// failing once with the build command before any step can launch.
pub(crate) fn require_internal_worker_executable() -> PathBuf {
    let Some(worker) = std::env::var_os(INTERNAL_WORKER_EXECUTABLE_VARIABLE) else {
        panic!(
            "{INTERNAL_WORKER_EXECUTABLE_VARIABLE} must name the built internal-worker example: \
             run through cli/scripts/check-suite, or build it with `cargo build --locked \
             --workspace --all-features --bins --example internal-worker` and export its path"
        );
    };
    let worker = PathBuf::from(worker);
    assert!(
        worker.is_file(),
        "test worker executable is unavailable: {}",
        worker.display()
    );
    worker
}

/// A workflow whose one `cmd` step leads a guarded process group and holds a descendant
/// in it, recording `<leader> <descendant>` to `record` once both exist.
pub(crate) fn holding_step_workflow(record: &Path) -> String {
    let argv = serde_json::to_string(&[
        "sh",
        "-c",
        "sleep 300 & printf '%s %s\\n' \"$$\" \"$!\" > \"$0\"; wait",
        record.to_str().expect("fixture path should be UTF-8"),
    ])
    .expect("encode guarded step argv");
    format!("schemaVersion: 1\nsteps:\n  hold:\n    kind: cmd\n    command:\n      argv: {argv}\n")
}

/// The leader and descendant a `holding_step_workflow` step recorded, once it has.
pub(crate) fn guarded_processes(record: &Path) -> Option<(Pid, Pid)> {
    let recorded = std::fs::read_to_string(record).ok()?;
    let (leader, descendant) = recorded.strip_suffix('\n')?.split_once(' ')?;
    Some((
        Pid::from_raw(leader.parse().ok()?)?,
        Pid::from_raw(descendant.parse().ok()?)?,
    ))
}

pub(crate) fn process_exists(process: Pid) -> bool {
    !matches!(test_kill_process(process), Err(Errno::SRCH))
}

pub(crate) struct ConfigFixture {
    config: Config,
    _work_root: tempfile::TempDir,
}

impl ConfigFixture {
    pub(crate) fn new(
        gateway_url: &str,
        credential: Credential,
        allow_insecure_http: bool,
    ) -> Result<Self, ConfigError> {
        require_internal_worker_executable();
        let manifest = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .map_err(|_| ConfigError::WorkRootUnavailable)?;
        // The test boundary preserves TMPDIR, which may be inside this checkout.
        // Runner fixtures must not follow it back into the source tree.
        let work_root =
            tempfile::tempdir_in("/tmp").map_err(|_| ConfigError::WorkRootUnavailable)?;
        std::fs::set_permissions(work_root.path(), std::fs::Permissions::from_mode(0o700))
            .map_err(|_| ConfigError::WorkRootUnavailable)?;
        let assignment = AssignmentConfig::new(work_root.path())?;
        if assignment.work_root().starts_with(manifest) {
            return Err(ConfigError::WorkRootUnavailable);
        }
        let config = Config::new(
            gateway_url,
            credential,
            allow_insecure_http,
            assignment,
            RepositoryUrlPolicy::with_file_repositories(true),
        )?;
        Ok(Self {
            config,
            _work_root: work_root,
        })
    }

    pub(crate) fn cloned_config(&self) -> Config {
        self.config.clone()
    }
}

impl Deref for ConfigFixture {
    type Target = Config;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

/// Bounds a runner test's awaited wait against hangs on `FIXTURE_WAIT_BASE`, scaled by the
/// host's load (INV-TESTING-26). The case is the call site (`file:line`), and an expiry is
/// returned as an error that every call site prints, through `unwrap` or `expect` or in its
/// own panic message. A plain function, not an `async fn`, so the body is heap-pinned
/// before this wrapper's state exists (INV-TESTING-18).
#[track_caller]
pub(crate) fn with_watchdog<F: Future>(
    future: F,
) -> impl Future<Output = Result<F::Output, WatchdogExpired>> {
    watchdog_at(Location::caller(), FIXTURE_WAIT_BASE, future)
}

fn watchdog_at<F: Future>(
    location: &Location<'_>,
    base: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, WatchdogExpired>> {
    host_scaled_timeout(
        format!("{}:{}", location.file(), location.line()),
        base,
        future,
    )
}

struct DeterministicFrameSource {
    next: AtomicU64,
}

impl FrameSource for DeterministicFrameSource {
    fn public_id(&self, prefix: &str) -> String {
        let value = u128::from(self.next.fetch_add(1, Ordering::Relaxed) + 1);
        format!(
            "{prefix}{}",
            ulid::Ulid::from(value).to_string().to_ascii_lowercase()
        )
    }

    fn utc_timestamp(&self) -> Result<String, ConnectionError> {
        Ok("2026-07-23T00:00:00Z".to_owned())
    }
}

pub(crate) fn deterministic_frame_source() -> Arc<dyn FrameSource> {
    Arc::new(DeterministicFrameSource {
        next: AtomicU64::new(0),
    })
}

struct ControlledShutdown {
    requests: mpsc::UnboundedReceiver<()>,
}

impl Shutdown for ControlledShutdown {
    fn wait(&mut self) -> ShutdownFuture<'_> {
        Box::pin(async {
            let _ = self.requests.recv().await;
        })
    }
}

#[derive(Clone)]
pub(crate) struct ControlledShutdownTrigger {
    requests: mpsc::UnboundedSender<()>,
}

impl ControlledShutdownTrigger {
    pub(crate) fn notify_one(&self) {
        let _ = self.requests.send(());
    }
}

pub(crate) fn controlled_shutdown() -> (Box<dyn Shutdown>, ControlledShutdownTrigger) {
    let (requests, receiver) = mpsc::unbounded_channel();
    (
        Box::new(ControlledShutdown { requests: receiver }),
        ControlledShutdownTrigger { requests },
    )
}

#[derive(Clone, Default)]
pub(crate) struct DeterminismTranscript {
    events: Arc<Mutex<Vec<String>>>,
}

impl DeterminismTranscript {
    pub(crate) fn record(&self, event: String) {
        self.events
            .lock()
            .expect("determinism transcript mutex poisoned")
            .push(event);
    }

    pub(crate) fn snapshot(&self) -> Vec<String> {
        self.events
            .lock()
            .expect("determinism transcript mutex poisoned")
            .clone()
    }
}

pub(crate) struct SleepRelease {
    notification: Arc<Notify>,
    duration: Duration,
    deadline: Instant,
    clock: Arc<Mutex<Instant>>,
    utc_clock: Arc<Mutex<time::OffsetDateTime>>,
    transcript: Option<DeterminismTranscript>,
}

impl SleepRelease {
    pub(crate) fn release(self) {
        if let Some(transcript) = &self.transcript {
            transcript.record(format!("sleep.released:{}ms", self.duration.as_millis()));
        }
        let mut now = self.clock.lock().expect("controlled clock mutex poisoned");
        let elapsed = self
            .deadline
            .checked_duration_since(*now)
            .unwrap_or_default();
        *now += elapsed;
        drop(now);
        let mut utc_now = self
            .utc_clock
            .lock()
            .expect("controlled UTC clock mutex poisoned");
        *utc_now += time::Duration::try_from(elapsed)
            .expect("controlled UTC duration should be representable");
        drop(utc_now);
        self.notification.notify_one();
    }
}

struct ControlledSleeper {
    requests: mpsc::UnboundedSender<(Duration, SleepRelease)>,
    clock: Arc<Mutex<Instant>>,
    utc_clock: Arc<Mutex<time::OffsetDateTime>>,
    transcript: Option<DeterminismTranscript>,
}

impl Sleeper for ControlledSleeper {
    fn now(&self) -> Instant {
        *self.clock.lock().expect("controlled clock mutex poisoned")
    }

    fn utc_now(&self) -> time::OffsetDateTime {
        *self
            .utc_clock
            .lock()
            .expect("controlled UTC clock mutex poisoned")
    }

    fn sleep(&self, duration: Duration) -> super::SleepFuture<'_> {
        let requests = self.requests.clone();
        Box::pin(async move {
            let notification = Arc::new(Notify::new());
            if let Some(transcript) = &self.transcript {
                transcript.record(format!("sleep.requested:{}ms", duration.as_millis()));
            }
            let deadline = self
                .now()
                .checked_add(duration)
                .expect("controlled sleep deadline overflowed");
            requests
                .send((
                    duration,
                    SleepRelease {
                        notification: Arc::clone(&notification),
                        duration,
                        deadline,
                        clock: Arc::clone(&self.clock),
                        utc_clock: Arc::clone(&self.utc_clock),
                        transcript: self.transcript.clone(),
                    },
                ))
                .expect("controlled sleep receiver should remain open");
            notification.notified().await;
        })
    }
}

struct FixtureSleeper;

impl Sleeper for FixtureSleeper {
    #[expect(
        clippy::disallowed_methods,
        reason = "the fixture sleeper preserves production monotonic timer behavior"
    )]
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn utc_now(&self) -> time::OffsetDateTime {
        fixture_utc_origin()
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the fixture sleeper preserves production Tokio timer behavior"
    )]
    fn sleep(&self, duration: Duration) -> super::SleepFuture<'_> {
        Box::pin(tokio::time::sleep(duration))
    }
}

pub(crate) fn fixture_sleeper() -> Arc<dyn Sleeper> {
    Arc::new(FixtureSleeper)
}

pub(crate) fn controlled_sleeper() -> (
    Arc<dyn Sleeper>,
    mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
) {
    controlled_sleeper_with_optional_transcript(None)
}

pub(crate) fn controlled_sleeper_with_transcript(
    transcript: DeterminismTranscript,
) -> (
    Arc<dyn Sleeper>,
    mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
) {
    controlled_sleeper_with_optional_transcript(Some(transcript))
}

fn controlled_sleeper_with_optional_transcript(
    transcript: Option<DeterminismTranscript>,
) -> (
    Arc<dyn Sleeper>,
    mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
) {
    let (requests, receiver) = mpsc::unbounded_channel();
    (
        Arc::new(ControlledSleeper {
            requests,
            clock: Arc::new(Mutex::new(monotonic_test_origin())),
            utc_clock: Arc::new(Mutex::new(fixture_utc_origin())),
            transcript,
        }),
        receiver,
    )
}

#[expect(
    clippy::disallowed_methods,
    reason = "controlled sleepers need an opaque monotonic origin"
)]
fn monotonic_test_origin() -> Instant {
    Instant::now()
}

fn fixture_utc_origin() -> time::OffsetDateTime {
    time::OffsetDateTime::parse(
        "2026-07-23T00:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .expect("fixed fixture UTC origin should parse")
}

struct ScriptedDuplexState {
    pending_pong: Mutex<Option<Message>>,
}

pub(crate) struct ScriptedInbound {
    sender: mpsc::UnboundedSender<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    transcript: DeterminismTranscript,
}

impl ScriptedInbound {
    pub(crate) fn send(&self, message: Message) {
        self.transcript
            .record(format!("inbound.queued:{}", describe_message(&message)));
        self.sender
            .send(Ok(message))
            .expect("scripted inbound reader should remain open");
    }
}

pub(crate) struct ScriptedReader {
    receiver: mpsc::UnboundedReceiver<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    state: Arc<ScriptedDuplexState>,
    transcript: DeterminismTranscript,
}

impl Stream for ScriptedReader {
    type Item = Result<Message, tokio_tungstenite::tungstenite::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = std::task::ready!(Pin::new(&mut self.receiver).poll_recv(context));
        if let Some(Ok(message)) = &item {
            self.transcript
                .record(format!("inbound.read:{}", describe_message(message)));
            if let Message::Ping(payload) = message {
                *self
                    .state
                    .pending_pong
                    .lock()
                    .expect("scripted duplex mutex poisoned") =
                    Some(Message::Pong(payload.clone()));
            }
        }
        Poll::Ready(item)
    }
}

pub(crate) struct ScriptedWriter {
    outbound: mpsc::UnboundedSender<Message>,
    state: Arc<ScriptedDuplexState>,
    transcript: DeterminismTranscript,
}

impl ScriptedWriter {
    fn record(&self, message: Message) {
        self.transcript
            .record(format!("outbound:{}", describe_message(&message)));
        self.outbound
            .send(message)
            .expect("scripted outbound receiver should remain open");
    }
}

impl Sink<Message> for ScriptedWriter {
    type Error = tokio_tungstenite::tungstenite::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        self.record(item);
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let pending_pong = self
            .state
            .pending_pong
            .lock()
            .expect("scripted duplex mutex poisoned")
            .take();
        if let Some(pong) = pending_pong {
            self.record(pong);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

pub(crate) fn scripted_duplex(
    transcript: DeterminismTranscript,
) -> (
    ScriptedInbound,
    ScriptedReader,
    ScriptedWriter,
    mpsc::UnboundedReceiver<Message>,
) {
    let (inbound, receiver) = mpsc::unbounded_channel();
    let (outbound, outbound_receiver) = mpsc::unbounded_channel();
    let state = Arc::new(ScriptedDuplexState {
        pending_pong: Mutex::new(None),
    });
    (
        ScriptedInbound {
            sender: inbound,
            transcript: transcript.clone(),
        },
        ScriptedReader {
            receiver,
            state: Arc::clone(&state),
            transcript: transcript.clone(),
        },
        ScriptedWriter {
            outbound,
            state,
            transcript,
        },
        outbound_receiver,
    )
}

pub(crate) struct ScriptedConnection {
    pub(crate) inbound: ScriptedInbound,
    pub(crate) outbound: mpsc::UnboundedReceiver<Message>,
}

pub(crate) struct ScriptedConnector {
    attempts: mpsc::UnboundedSender<ScriptedConnection>,
    next_attempt: AtomicU64,
    transcript: DeterminismTranscript,
}

impl Connector for ScriptedConnector {
    fn connect<'a>(&'a self, attempt: ConnectionAttempt<'a>) -> ConnectionFuture<'a> {
        Box::pin(async move {
            let attempt_number = self.next_attempt.fetch_add(1, Ordering::Relaxed) + 1;
            self.transcript
                .record(format!("connection.attempt:{attempt_number}"));
            let (inbound, reader, writer, outbound) = scripted_duplex(self.transcript.clone());
            self.attempts
                .send(ScriptedConnection { inbound, outbound })
                .expect("scripted connection receiver should remain open");
            let outcome = run_established_shared(
                attempt.dependencies,
                attempt.opening,
                attempt.next_sequence,
                reader,
                writer,
            )
            .await;
            self.transcript
                .record(describe_connection_outcome(&outcome));
            outcome
        })
    }
}

pub(crate) fn scripted_connector(
    transcript: DeterminismTranscript,
) -> (
    ScriptedConnector,
    mpsc::UnboundedReceiver<ScriptedConnection>,
) {
    let (attempts, receiver) = mpsc::unbounded_channel();
    (
        ScriptedConnector {
            attempts,
            next_attempt: AtomicU64::new(0),
            transcript,
        },
        receiver,
    )
}

fn describe_connection_outcome(
    outcome: &Result<super::connection::ConnectionProgress, ConnectionError>,
) -> String {
    match outcome {
        Ok(progress) => format!(
            "connection.outcome:closed:opening_acknowledged={}:handshake_completed={}",
            progress.opening_acknowledged, progress.handshake_completed
        ),
        Err(error) => format!(
            "connection.outcome:{}:{}:opening_acknowledged={}:handshake_completed={}",
            if error.is_terminal() {
                "terminal"
            } else {
                "retryable"
            },
            error.connection_cause().error_type(),
            error.progress.opening_acknowledged,
            error.progress.handshake_completed,
        ),
    }
}

fn describe_message(message: &Message) -> String {
    match message {
        Message::Text(text) => format!("text:{text}"),
        Message::Binary(payload) => format!("binary:{payload:?}"),
        Message::Ping(payload) => format!("ping:{payload:?}"),
        Message::Pong(payload) => format!("pong:{payload:?}"),
        Message::Close(Some(close)) => format!("close:{}:{}", close.code, close.reason),
        Message::Close(None) => "close:none".to_owned(),
        Message::Frame(frame) => format!("frame:{frame:?}"),
    }
}

pub(crate) async fn sleep_request(
    requests: &mut mpsc::UnboundedReceiver<(Duration, SleepRelease)>,
    expected: Duration,
) -> SleepRelease {
    loop {
        let (duration, release) = requests
            .recv()
            .await
            .expect("controlled sleeper closed before requesting the expected timer");
        if duration == expected {
            return release;
        }
    }
}

pub(crate) async fn fixture_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture listener");
    let endpoint = format!(
        "ws://{}/v1/runner/connect",
        listener.local_addr().expect("fixture address")
    );
    (listener, endpoint)
}

#[allow(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback requires its large error type"
)]
pub(crate) async fn accept_fixture_socket(listener: &TcpListener) -> FixtureSocket {
    accept_fixture_socket_with_headers(listener).await.0
}

#[allow(
    clippy::result_large_err,
    reason = "tungstenite's handshake callback requires its large error type"
)]
pub(crate) async fn accept_fixture_socket_with_headers(
    listener: &TcpListener,
) -> (FixtureSocket, HeaderMap) {
    let (stream, _) = listener.accept().await.expect("accept fixture connection");
    let (captured, capture) = std::sync::mpsc::sync_channel(1);
    let socket = accept_hdr_async(stream, |request: &Request, mut response: Response| {
        captured
            .send(request.headers().clone())
            .expect("capture fixture upgrade headers");
        response.headers_mut().insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("um.runner.v1"),
        );
        Ok(response)
    })
    .await
    .expect("accept WebSocket fixture");
    (
        socket,
        capture
            .try_recv()
            .expect("fixture upgrade headers should be captured"),
    )
}

pub(crate) fn welcome() -> Message {
    Message::Text(
        json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abc",
            "sentAt": "2026-07-23T00:00:00Z",
            "type": "welcome",
            "payloadVersion": 1,
            "payload": {
                "sessionId": "rsn_01k0z6r1w8f4jy2m7q9v3x5abc",
                "pingIntervalSeconds": 1,
                "pongTimeoutSeconds": 2,
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

pub(crate) fn observation_acknowledgement(message_id: &str, sequence: u64) -> Message {
    observation_acknowledgement_with_cloud_id(
        "cmsg_01k0z6r1w8f4jy2m7q9v3x5abd",
        "2026-07-23T00:00:01Z",
        message_id,
        sequence,
    )
}

pub(crate) fn effect_observation_acknowledgement(message_id: &str, sequence: u64) -> Message {
    observation_acknowledgement_with_cloud_id(
        "cmsg_01k0z6r1w8f4jy2m7q9v3x5abf",
        "2026-07-23T00:00:03Z",
        message_id,
        sequence,
    )
}

pub(crate) fn terminal_observation_acknowledgement(message_id: &str, sequence: u64) -> Message {
    observation_acknowledgement_with_cloud_id(
        "cmsg_01k0z6r1w8f4jy2m7q9v3x5abg",
        "2026-07-23T00:00:04Z",
        message_id,
        sequence,
    )
}

fn observation_acknowledgement_with_cloud_id(
    cloud_message_id: &str,
    sent_at: &str,
    acknowledged_message_id: &str,
    acknowledged_sequence: u64,
) -> Message {
    Message::Text(
        json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": cloud_message_id,
            "sentAt": sent_at,
            "type": "observation_ack",
            "payloadVersion": 1,
            "payload": {
                "acknowledgedMessageId": acknowledged_message_id,
                "acknowledgedSequence": acknowledged_sequence
            }
        })
        .to_string()
        .into(),
    )
}

pub(crate) fn assignment_offer() -> Message {
    Message::Text(
        json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abe",
            "sentAt": "2026-07-23T00:00:02Z",
            "type": "assignment_offer",
            "payloadVersion": 1,
            "payload": {
                "effectId": "eff_01k0z6r1w8f4jy2m7q9v3x5abg",
                "assignmentId": "asn_01k0z6r1w8f4jy2m7q9v3x5abh",
                "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abj",
                "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
                "attemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
                "attemptNumber": 1,
                "executionSpec": {
                    "executionSpecId": "xsp_01k0z6r1w8f4jy2m7q9v3x5abc",
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
                        "commitOid": "0123456789abcdef0123456789abcdef01234567",
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
                }
            }
        })
        .to_string()
        .into(),
    )
}

pub(crate) fn assignment_prepare() -> Message {
    Message::Text(
        json!({
            "protocolVersion": 1,
            "direction": "cloud_to_runner",
            "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abq",
            "sentAt": "2026-07-23T00:00:03Z",
            "type": "assignment_prepare",
            "payloadVersion": 1,
            "payload": {
                "effectId": "eff_01k0z6r1w8f4jy2m7q9v3x5abh",
                "assignmentId": "asn_01k0z6r1w8f4jy2m7q9v3x5abh",
                "runId": "run_01k0z6r1w8f4jy2m7q9v3x5abj",
                "attemptId": "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
                "executionSpecId": "xsp_01k0z6r1w8f4jy2m7q9v3x5abc",
                "preparationExpiresAt": "2026-07-23T00:15:03Z"
            }
        })
        .to_string()
        .into(),
    )
}

pub(crate) async fn effect_acknowledgement(socket: &mut FixtureSocket) -> Value {
    loop {
        let Some(Ok(Message::Text(message))) = socket.next().await else {
            panic!("fixture did not receive effect acknowledgement");
        };
        let frame: Value = serde_json::from_str(&message).expect("decode runner frame");
        if matches!(
            frame["type"].as_str(),
            Some("workspace_inventory" | "workspace_inventory_complete")
        ) {
            socket
                .send(effect_observation_acknowledgement(
                    frame["messageId"].as_str().unwrap(),
                    frame["sequence"].as_u64().unwrap(),
                ))
                .await
                .expect("acknowledge optional startup discovery");
            continue;
        }
        assert_eq!(frame["type"], "effect_acknowledged");
        return frame;
    }
}

pub(crate) async fn begin_assignment_preparation(socket: &mut FixtureSocket) -> Value {
    let Some(Ok(Message::Text(preparing))) = socket.next().await else {
        panic!("fixture did not receive assignment preparation acknowledgement");
    };
    let preparing: Value =
        serde_json::from_str(&preparing).expect("decode assignment preparation acknowledgement");
    assert_eq!(preparing["type"], "assignment_preparing");
    socket
        .send(effect_observation_acknowledgement(
            preparing["messageId"].as_str().unwrap(),
            preparing["sequence"].as_u64().unwrap(),
        ))
        .await
        .expect("acknowledge assignment preparation");
    socket
        .send(assignment_prepare())
        .await
        .expect("send assignment prepare effect");
    let acknowledgement = effect_acknowledgement(socket).await;
    assert_eq!(acknowledgement["type"], "effect_acknowledged");
    acknowledgement
}

pub(crate) async fn offer_assignment_after_handshake(
    socket: &mut FixtureSocket,
    opening_message_id: &str,
    opening_sequence: u64,
) -> Value {
    socket.send(welcome()).await.expect("send welcome");
    socket
        .send(observation_acknowledgement(
            opening_message_id,
            opening_sequence,
        ))
        .await
        .expect("send opening acknowledgement");
    socket
        .send(assignment_offer())
        .await
        .expect("send assignment offer");
    effect_acknowledgement(socket).await
}

pub(crate) async fn expect_opening_hello(socket: &mut FixtureSocket) {
    let Some(Ok(Message::Text(_))) = socket.next().await else {
        panic!("fixture did not receive opening hello");
    };
}

pub(crate) async fn accept_opened_fixture_socket(listener: &TcpListener) -> FixtureSocket {
    let mut socket = accept_fixture_socket(listener).await;
    expect_opening_hello(&mut socket).await;
    socket
}

pub(crate) async fn expect_close_frame(socket: &mut FixtureSocket) -> CloseFrame {
    loop {
        match socket.next().await {
            Some(Ok(Message::Close(Some(frame)))) => break frame,
            Some(Ok(Message::Close(None))) => panic!("fixture received a close without a body"),
            Some(Ok(_)) => continue,
            other => panic!("fixture did not receive close: {other:?}"),
        }
    }
}

/// The agent login the fake Cloud names.
pub(crate) const FAKE_AGENT_LOGIN_ID: &str = "agl_01k0z6r1w8f4jy2m7q9v3x5abc";
/// Text in every material the fake Cloud delivers, which no output but the
/// installed login may hold.
pub(crate) const FAKE_AGENT_LOGIN_SENTINEL: &str = "agent-login-material-sentinel";

/// The Codex `auth.json` the fake Cloud delivers for `generation`.
pub(crate) fn fake_agent_login_material(generation: u64) -> Vec<u8> {
    json!({"tokens": {"access_token": format!("{FAKE_AGENT_LOGIN_SENTINEL}-{generation}")}})
        .to_string()
        .into_bytes()
}

/// Cloud's answer naming the fake agent login for Codex with `status`.
pub(crate) fn fake_codex_answer(status: LoginStatus) -> Result<Vec<ProviderLogin>, CloudFailure> {
    Ok(vec![ProviderLogin {
        provider: AgentLoginProvider::Codex,
        agent_login_id: FAKE_AGENT_LOGIN_ID.to_owned(),
        status,
    }])
}

/// Cloud's answer authorizing the fake agent login for Codex at `generation`.
pub(crate) fn fake_codex_authorized(generation: u64) -> Result<Vec<ProviderLogin>, CloudFailure> {
    fake_codex_answer(LoginStatus::Authorized {
        generation,
        record_schema_version: 1,
    })
}

/// The grant the fake Cloud issues tokens from.
pub(crate) const FAKE_GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abc";
/// Text in every grant token the fake Cloud issues, which no output but the
/// token file may hold.
pub(crate) const FAKE_GRANT_TOKEN_SENTINEL: &str = "grant-token-sentinel";

/// The token the fake Cloud issues for `provider` at `generation`.
pub(crate) fn fake_grant_token(provider: AgentLoginProvider, generation: u64) -> String {
    format!(
        "{FAKE_GRANT_TOKEN_SENTINEL}-{}-{generation}",
        provider.as_str()
    )
}

/// What the fake Cloud answers a grant-token request for one provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FakeGrantToken {
    /// Issues `fake_grant_token` from `FAKE_GRANT_ID` at this generation.
    Issue(u64),
    Fail(GrantTokenFailure),
}

/// The wait the fake Cloud names with each token it issues until a test
/// sets another, as Cloud's own fixture does.
const FAKE_GRANT_REFRESH_AFTER: Duration = Duration::from_secs(5040);

/// The expiry of each token the fake Cloud issues until a test sets
/// another: 2026-11-13T22:00:00Z.
pub(crate) fn fake_grant_expiry() -> time::OffsetDateTime {
    time::OffsetDateTime::from_unix_timestamp(1_794_607_200).expect("the fake expiry is a time")
}

/// A hook the fake Cloud calls with each grant-token request as it arrives.
type GrantRequestHook = Box<dyn Fn(&GrantTokenRequest) + Send + Sync>;

/// Cloud's side of agent-login setup for service tests: answers each status
/// request with what the test last set, or the next one only with what it set
/// for it, and delivers the fake material for the generation asked for, or
/// fails or refuses each delivery as the test last set. It counts both
/// requests and keeps each status request.
pub(crate) struct FakeAgentLoginCloud {
    answer: Mutex<Result<Vec<ProviderLogin>, CloudFailure>>,
    later: Mutex<Option<Result<Vec<ProviderLogin>, CloudFailure>>>,
    delivery_failure: Mutex<Option<DeliveryFailure>>,
    held: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    arrived: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
    status_requests: AtomicU64,
    reports: Mutex<Vec<StatusRequest>>,
    deliveries: AtomicU64,
    grant_tokens: Mutex<Vec<(AgentLoginProvider, FakeGrantToken)>>,
    /// The wait and the expiry each issued token carries.
    grant_terms: Mutex<(Duration, time::OffsetDateTime)>,
    grant_requests: Mutex<Vec<GrantTokenRequest>>,
    grant_request_hook: Mutex<Option<GrantRequestHook>>,
}

impl FakeAgentLoginCloud {
    pub(crate) fn new(answer: Result<Vec<ProviderLogin>, CloudFailure>) -> Arc<Self> {
        Arc::new(Self {
            answer: Mutex::new(answer),
            later: Mutex::new(None),
            delivery_failure: Mutex::new(None),
            held: Mutex::new(None),
            arrived: Mutex::new(None),
            status_requests: AtomicU64::new(0),
            reports: Mutex::new(Vec::new()),
            deliveries: AtomicU64::new(0),
            grant_tokens: Mutex::new(Vec::new()),
            grant_terms: Mutex::new((FAKE_GRANT_REFRESH_AFTER, fake_grant_expiry())),
            grant_requests: Mutex::new(Vec::new()),
            grant_request_hook: Mutex::new(None),
        })
    }

    /// What later grant-token requests for `provider` answer; until a test
    /// sets one, each is answered as by a Cloud without the route.
    pub(crate) fn answer_grant_token(&self, provider: AgentLoginProvider, answer: FakeGrantToken) {
        let mut answers = self.grant_tokens.lock().expect("fake Cloud mutex poisoned");
        answers.retain(|(answered, _)| *answered != provider);
        answers.push((provider, answer));
    }

    /// The wait before asking again and the expiry each later issued token
    /// carries.
    pub(crate) fn issue_grant_tokens_with(
        &self,
        refresh_after: Duration,
        expires_at: time::OffsetDateTime,
    ) {
        *self.grant_terms.lock().expect("fake Cloud mutex poisoned") = (refresh_after, expires_at);
    }

    /// Calls `hook` with each later grant-token request before it is
    /// answered.
    pub(crate) fn on_grant_request(&self, hook: GrantRequestHook) {
        *self
            .grant_request_hook
            .lock()
            .expect("fake Cloud mutex poisoned") = Some(hook);
    }

    /// Each grant-token request answered so far, in the order they arrived.
    pub(crate) fn grant_requests(&self) -> Vec<GrantTokenRequest> {
        self.grant_requests
            .lock()
            .expect("fake Cloud mutex poisoned")
            .clone()
    }

    /// What later status requests answer.
    pub(crate) fn answer(&self, answer: Result<Vec<ProviderLogin>, CloudFailure>) {
        *self.answer.lock().expect("fake Cloud mutex poisoned") = answer;
    }

    /// What the next status request answers, and `later` what each after it
    /// answers.
    pub(crate) fn answer_next_then(
        &self,
        next: Result<Vec<ProviderLogin>, CloudFailure>,
        later: Result<Vec<ProviderLogin>, CloudFailure>,
    ) {
        self.answer(next);
        *self.later.lock().expect("fake Cloud mutex poisoned") = Some(later);
    }

    /// How later delivery requests fail; `None` delivers again.
    pub(crate) fn fail_deliveries(&self, failure: Option<CloudFailure>) {
        *self
            .delivery_failure
            .lock()
            .expect("fake Cloud mutex poisoned") = failure.map(DeliveryFailure::from);
    }

    /// Refuses later delivery requests with `refusal`.
    pub(crate) fn refuse_deliveries(&self, refusal: DeliveryRefusal) {
        *self
            .delivery_failure
            .lock()
            .expect("fake Cloud mutex poisoned") = Some(DeliveryFailure::Refused(refusal));
    }

    /// Makes the next status request wait until the returned sender sends or
    /// is dropped.
    pub(crate) fn hold_next_status(&self) -> std::sync::mpsc::SyncSender<()> {
        let (release, held) = std::sync::mpsc::sync_channel(1);
        *self.held.lock().expect("fake Cloud mutex poisoned") = Some(held);
        release
    }

    /// `hold_next_status`, and a receiver that hears when the held request
    /// has arrived.
    pub(crate) fn hold_next_status_arrival(
        &self,
    ) -> (
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    ) {
        let release = self.hold_next_status();
        let (arrived, arrival) = std::sync::mpsc::sync_channel(1);
        *self.arrived.lock().expect("fake Cloud mutex poisoned") = Some(arrived);
        (release, arrival)
    }

    pub(crate) fn status_requests(&self) -> u64 {
        self.status_requests.load(Ordering::SeqCst)
    }

    /// Each status request answered so far, in order.
    pub(crate) fn reports(&self) -> Vec<StatusRequest> {
        self.reports
            .lock()
            .expect("fake Cloud mutex poisoned")
            .clone()
    }

    pub(crate) fn deliveries(&self) -> u64 {
        self.deliveries.load(Ordering::SeqCst)
    }
}

impl AgentLoginCloud for FakeAgentLoginCloud {
    fn status(&self, request: &StatusRequest) -> Result<Vec<ProviderLogin>, CloudFailure> {
        let held = self.held.lock().expect("fake Cloud mutex poisoned").take();
        if let Some(held) = held {
            if let Some(arrived) = self
                .arrived
                .lock()
                .expect("fake Cloud mutex poisoned")
                .take()
            {
                let _ = arrived.send(());
            }
            let _ = held.recv();
        }
        self.status_requests.fetch_add(1, Ordering::SeqCst);
        self.reports
            .lock()
            .expect("fake Cloud mutex poisoned")
            .push(request.clone());
        let mut answer = self.answer.lock().expect("fake Cloud mutex poisoned");
        let answered = answer.clone();
        if let Some(later) = self.later.lock().expect("fake Cloud mutex poisoned").take() {
            *answer = later;
        }
        answered
    }

    fn follow_credential(&self, _endpoint: &url::Url, _runner_credential: &Credential) {}

    fn delivery(&self, request: &DeliveryRequest) -> Result<Delivery, DeliveryFailure> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        if let Some(failure) = *self
            .delivery_failure
            .lock()
            .expect("fake Cloud mutex poisoned")
        {
            return Err(failure);
        }
        Ok(Delivery {
            provider: request.provider,
            agent_login_id: request.agent_login_id.clone(),
            generation: request.generation,
            record_schema_version: 1,
            material: LoginMaterial::from_bytes(&fake_agent_login_material(request.generation)),
        })
    }

    fn grant_token(&self, request: &GrantTokenRequest) -> Result<IssuedToken, GrantTokenFailure> {
        if let Some(hook) = self
            .grant_request_hook
            .lock()
            .expect("fake Cloud mutex poisoned")
            .as_ref()
        {
            hook(request);
        }
        self.grant_requests
            .lock()
            .expect("fake Cloud mutex poisoned")
            .push(request.clone());
        let answer = self
            .grant_tokens
            .lock()
            .expect("fake Cloud mutex poisoned")
            .iter()
            .find(|(provider, _)| *provider == request.provider)
            .map_or(
                FakeGrantToken::Fail(GrantTokenFailure::NoRoute),
                |(_, answer)| *answer,
            );
        let (refresh_after, expires_at) =
            *self.grant_terms.lock().expect("fake Cloud mutex poisoned");
        match answer {
            FakeGrantToken::Issue(generation) => Ok(IssuedToken {
                grant_id: FAKE_GRANT_ID.to_owned(),
                generation,
                access_token: GrantAccessToken::new(
                    fake_grant_token(request.provider, generation).as_bytes(),
                )
                .expect("the fake grant token is one of the token's form"),
                expires_at,
                refresh_after,
            }),
            FakeGrantToken::Fail(failure) => Err(failure),
        }
    }
}

/// An assignment gate that is idle for a number of calls and busy after
/// them, as when an assignment starts while a pass runs.
pub(crate) struct CountedGate {
    pub(crate) idle_calls: AtomicUsize,
}

impl CountedGate {
    /// A gate that is always idle.
    pub(crate) fn idle() -> Self {
        Self {
            idle_calls: AtomicUsize::new(usize::MAX),
        }
    }

    /// A gate that is always busy.
    pub(crate) fn busy() -> Self {
        Self {
            idle_calls: AtomicUsize::new(0),
        }
    }
}

impl AssignmentGate for CountedGate {
    fn while_idle(&self, change: &mut dyn FnMut()) -> bool {
        let idle = self
            .idle_calls
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if idle {
            change();
        }
        idle
    }
}

/// A login check for service tests: answers each provider as the test last
/// set, and until then `program_unavailable`, as a runner without the
/// coding agents' programs does. It keeps each login it was asked to check,
/// as its provider and generation.
pub(crate) struct FakeLoginCheck {
    answers: Mutex<Vec<(AgentLoginProvider, LoginCheckOutcome)>>,
    checked: Mutex<Vec<(AgentLoginProvider, u64)>>,
}

impl FakeLoginCheck {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(Vec::new()),
            checked: Mutex::new(Vec::new()),
        })
    }

    /// What later checks of `provider`'s login answer.
    pub(crate) fn answer(&self, provider: AgentLoginProvider, outcome: LoginCheckOutcome) {
        let mut answers = self.answers.lock().expect("fake check mutex poisoned");
        answers.retain(|(answered, _)| *answered != provider);
        answers.push((provider, outcome));
    }

    /// Each login checked so far, in order.
    pub(crate) fn checked(&self) -> Vec<(AgentLoginProvider, u64)> {
        self.checked
            .lock()
            .expect("fake check mutex poisoned")
            .clone()
    }
}

impl LoginCheck for FakeLoginCheck {
    fn check(&self, provider: AgentLoginProvider, login: &Installation) -> LoginCheckOutcome {
        self.checked
            .lock()
            .expect("fake check mutex poisoned")
            .push((provider, login.generation));
        self.answers
            .lock()
            .expect("fake check mutex poisoned")
            .iter()
            .find(|(answered, _)| *answered == provider)
            .map_or(
                LoginCheckOutcome::NoAnswer(AgentLoginCheckCause::ProgramUnavailable),
                |(_, outcome)| *outcome,
            )
    }
}

/// Agent-login setup against `cloud` for a service whose `HOME` is `home`
/// and whose protected state is in `state_directory`, checking logins with
/// a fresh `FakeLoginCheck`.
pub(crate) fn fake_agent_login_setup(
    cloud: Arc<FakeAgentLoginCloud>,
    home: &Path,
    state_directory: &Path,
    recorder: Arc<Recorder>,
) -> SetupDependencies {
    let home = std::ffi::OsString::from(home);
    SetupDependencies::new(
        cloud,
        FakeLoginCheck::new(),
        ServiceEnvironment::from_lookup(|name| (name == "HOME").then(|| home.clone())),
        &state_directory.join("runner-state.json"),
        recorder,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // INV-TESTING-26
    #[tokio::test]
    async fn watchdog_names_its_call_site_and_returns_the_case_output() {
        let location = Location::caller();
        let expired = watchdog_at(
            location,
            Duration::from_millis(1),
            std::future::pending::<()>(),
        )
        .await
        .expect_err("a case that never completes expires");
        let expected = format!(
            "{}:{} watchdog expired after",
            location.file(),
            location.line()
        );
        let rendered = format!("{expired:?}");
        assert!(rendered.starts_with(&expected), "{rendered}");

        assert_eq!(
            with_watchdog(async { 7 })
                .await
                .expect("a ready wait does not expire"),
            7
        );
    }

    #[tokio::test]
    async fn overlapping_releases_advance_both_clocks_by_elapsed_time() {
        let (sleeper, mut requests) = controlled_sleeper();
        let monotonic_origin = sleeper.now();
        let utc_origin = sleeper.utc_now();
        let first = tokio::spawn({
            let sleeper = Arc::clone(&sleeper);
            async move { sleeper.sleep(Duration::from_secs(2)).await }
        });
        let second = tokio::spawn({
            let sleeper = Arc::clone(&sleeper);
            async move { sleeper.sleep(Duration::from_secs(3)).await }
        });
        let mut releases = vec![
            requests.recv().await.unwrap(),
            requests.recv().await.unwrap(),
        ];
        releases.sort_by_key(|(duration, _)| *duration);

        for (_, release) in releases {
            release.release();
        }
        first.await.unwrap();
        second.await.unwrap();

        assert_eq!(
            sleeper.now().duration_since(monotonic_origin),
            Duration::from_secs(3)
        );
        assert_eq!(sleeper.utc_now() - utc_origin, time::Duration::seconds(3));
    }
}
