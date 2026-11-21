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
use useful_execution::{
    FailurePolicy, ObservedExecutions, ObservedStepTransition, SettledExecution, StartedExecution,
    StepStateKind, TransitionEvent, TransitionObservation, TransitionSequence, WorkflowNodeRole,
};
use useful_test_support::watchdog::{FIXTURE_WAIT_BASE, WatchdogExpired, host_scaled_timeout};

use crate::agent_login::ServiceEnvironment;
use crate::agent_login::check::{LoginCheck, LoginCheckOutcome};
use crate::agent_login::cloud::{
    AgentLoginCloud, CloudFailure, CredentialSourcesFailure, CredentialSourcesRequest, Delivery,
    DeliveryFailure, DeliveryRefusal, DeliveryRequest, GrantAccessToken, GrantProvider,
    GrantProvidersFailure, GrantProvidersRequest, GrantTokenFailure, GrantTokenRequest,
    IssuedToken, ListedProvider, LoginMaterial, LoginStatus, ProviderLogin, StatusRequest,
};
use crate::agent_login::readiness::Installation;
use crate::agent_login::setup::{AssignmentGate, SetupDependencies};
use crate::control_protocol::{AgentLoginCheckCause, AgentLoginProvider};
use crate::credential::Credential;
use crate::service::config::{AssignmentConfig, Config, ConfigError, RepositoryUrlPolicy};
use crate::service::connection::{ConnectionError, FrameSource, run_established_shared};
use crate::service::execution::{AssignmentRunView, RunnerExecutionInstant};
pub(crate) use crate::service::lease_clock::fixture_lease_clock;
use crate::service::{
    ConnectionAttempt, ConnectionFuture, Connector, Shutdown, ShutdownFuture, Sleeper,
};
use crate::telemetry::Recorder;

pub(crate) type FixtureSocket = WebSocketStream<TcpStream>;

const INTERNAL_WORKER_EXECUTABLE_VARIABLE: &str = "USEFUL_TEST_INTERNAL_WORKER_EXECUTABLE";

/// The built `internal-worker` example that guards every command step a runner test
/// executes, as `cli/scripts/check-suite` exports it. Without it the execution crate
/// takes the example beside this test binary, never the binary itself (INV-TESTING-20),
/// and `cargo test -p useful-runner` does not build that example, so every step would fail
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

/// Runs fixture Git in `repository` and returns its trimmed standard output; a failed
/// command panics with Git's standard error.
pub(crate) fn run_fixture_git(repository: &Path, arguments: &[&str]) -> String {
    let output = useful_test_support::fixture_git_command("git")
        .current_dir(repository)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture git command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
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

/// `with_watchdog` over `base` rather than `FIXTURE_WAIT_BASE`, for a wait a test needs to
/// see elapse: a case that requires `future` to stay pending for `base`, scaled by the
/// host's load, requires the expiry.
#[track_caller]
pub(crate) fn with_bound<F: Future>(
    base: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, WatchdogExpired>> {
    watchdog_at(Location::caller(), base, future)
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

/// The thread a preparation fence waits for its deadline on.
const PREPARATION_DEADLINE_THREAD: &str = "runner-preparation-deadline";

/// Whether the test has let go of a `PreparationTeardownHold`.
#[derive(Default)]
struct TeardownGate {
    open: Mutex<bool>,
    opened: std::sync::Condvar,
}

/// What holds an admission's worker in its teardown until it is dropped,
/// which a failed assertion's unwinding does too, so a red test ends rather
/// than waiting for the worker.
pub(crate) struct PreparationTeardownHold {
    gate: Arc<TeardownGate>,
}

impl PreparationTeardownHold {
    /// Lets the held teardown go on.
    pub(crate) fn let_go(self) {}
}

impl Drop for PreparationTeardownHold {
    fn drop(&mut self) {
        *self
            .gate
            .open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.gate.opened.notify_all();
    }
}

/// A sleeper that answers as `fixture_sleeper()` does, except that a sleep
/// dropped on a preparation fence's thread waits, as it is dropped, until
/// the test lets go of the hold.
struct TeardownHoldingSleeper {
    fixture: Arc<dyn Sleeper>,
    gate: Arc<TeardownGate>,
}

impl Sleeper for TeardownHoldingSleeper {
    fn now(&self) -> Instant {
        self.fixture.now()
    }

    fn utc_now(&self) -> time::OffsetDateTime {
        self.fixture.utc_now()
    }

    fn sleep(&self, duration: Duration) -> super::SleepFuture<'_> {
        Box::pin(HeldSleep {
            sleep: self.fixture.sleep(duration),
            gate: Arc::clone(&self.gate),
        })
    }
}

/// A sleep of a `TeardownHoldingSleeper`.
struct HeldSleep<'a> {
    sleep: super::SleepFuture<'a>,
    gate: Arc<TeardownGate>,
}

impl Future for HeldSleep<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        self.sleep.as_mut().poll(context)
    }
}

impl Drop for HeldSleep<'_> {
    fn drop(&mut self) {
        // Only the fence's own thread waits: a sleep dropped on the test's
        // runtime would stop the runtime that lets the hold go.
        if std::thread::current().name() != Some(PREPARATION_DEADLINE_THREAD) {
            return;
        }
        let open = self
            .gate
            .open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(
            self.gate
                .opened
                .wait_while(open, |open| !*open)
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }
}

/// A manager sleeper that holds each admission's worker in its teardown,
/// past the outcome it published and before what it captured is dropped,
/// until the test lets go of the returned hold: the worker drops its
/// preparation fence's deadline sleep on the fence's thread, and joins that
/// thread before its captures drop. Otherwise it answers as
/// `fixture_sleeper()` does.
pub(crate) fn preparation_teardown_holding_sleeper() -> (Arc<dyn Sleeper>, PreparationTeardownHold)
{
    let gate = Arc::new(TeardownGate::default());
    (
        Arc::new(TeardownHoldingSleeper {
            fixture: fixture_sleeper(),
            gate: Arc::clone(&gate),
        }),
        PreparationTeardownHold { gate },
    )
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
    let Some(Ok(Message::Text(message))) = socket.next().await else {
        panic!("fixture did not receive effect acknowledgement");
    };
    let frame: Value = serde_json::from_str(&message).expect("decode runner frame");
    assert_eq!(frame["type"], "effect_acknowledged");
    frame
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
/// The integration token the fake Cloud issues a token service's tokens
/// from when a test asks it to.
pub(crate) const FAKE_INTEGRATION_TOKEN_ID: &str = "itk_01k0z6r1w8f4jy2m7q9v3x5abc";
/// Text in every grant token the fake Cloud issues, which no output but the
/// token file may hold.
pub(crate) const FAKE_GRANT_TOKEN_SENTINEL: &str = "grant-token-sentinel";

/// The token the fake Cloud issues for `provider` at `generation`: one of
/// the runner's three by the name `AgentLoginProvider::as_str` gives it, a
/// configured provider by its id.
pub(crate) fn fake_grant_token<P>(provider: P, generation: u64) -> String
where
    P: TryInto<GrantProvider>,
    P::Error: std::fmt::Debug,
{
    let provider = grant_provider(provider);
    let name = match provider.agent_login_provider() {
        Some(provider) => provider.as_str(),
        None => provider.cloud_id(),
    };
    format!("{FAKE_GRANT_TOKEN_SENTINEL}-{name}-{generation}")
}

/// `provider` as a grant provider: a `GrantProvider`, or one of the
/// runner's three as an `AgentLoginProvider`.
fn grant_provider<P>(provider: P) -> GrantProvider
where
    P: TryInto<GrantProvider>,
    P::Error: std::fmt::Debug,
{
    provider
        .try_into()
        .expect("a test names a provider that has a grant token")
}

/// A request the fake Cloud took on a grant route, as it arrived.
#[derive(Clone, Debug, Eq, PartialEq)]
enum FakeGrantRequest {
    /// A request for an assignment's providers.
    Providers(GrantProvidersRequest),
    /// A request for an assignment's token services.
    TokenServices(GrantProvidersRequest),
    /// A request for one provider's token.
    Token(GrantTokenRequest),
}

/// What the fake Cloud answers a grant-token request for one provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FakeGrantToken {
    /// Issues `fake_grant_token` from `FAKE_GRANT_ID` at this generation.
    Issue(u64),
    /// Issues `fake_grant_token` from `FAKE_INTEGRATION_TOKEN_ID` at this
    /// version, as Cloud issues a token service's token.
    IssueIntegrationToken(u64),
    /// Issues `fake_grant_token` from this grant or integration token ID at
    /// this generation, as Cloud issues the token of a grant or token other
    /// than the one a runner holds.
    IssueFrom(&'static str, u64),
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
/// requests and keeps each status request, and each credential-source
/// report, which it answers as the test last set.
pub(crate) struct FakeAgentLoginCloud {
    answer: Mutex<Result<Vec<ProviderLogin>, CloudFailure>>,
    later: Mutex<Option<Result<Vec<ProviderLogin>, CloudFailure>>>,
    delivery_failure: Mutex<Option<DeliveryFailure>>,
    held: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    arrived: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
    status_requests: AtomicU64,
    reports: Mutex<Vec<StatusRequest>>,
    deliveries: AtomicU64,
    grant_tokens: Mutex<Vec<(GrantProvider, FakeGrantToken)>>,
    /// The wait and the expiry each issued token carries.
    grant_terms: Mutex<(Duration, time::OffsetDateTime)>,
    grant_providers_answer: Mutex<Result<Vec<ListedProvider>, GrantProvidersFailure>>,
    token_services_answer: Mutex<Result<Vec<ListedProvider>, GrantProvidersFailure>>,
    /// Each provider-list, token-service-list and grant-token request, in
    /// the order they arrived.
    grant_requests: Mutex<Vec<FakeGrantRequest>>,
    grant_request_hook: Mutex<Option<GrantRequestHook>>,
    credential_sources_answer: Mutex<Result<(), CredentialSourcesFailure>>,
    credential_source_reports: Mutex<Vec<CredentialSourcesRequest>>,
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
            grant_providers_answer: Mutex::new(Err(GrantProvidersFailure::NoRoute)),
            token_services_answer: Mutex::new(Err(GrantProvidersFailure::NoRoute)),
            grant_requests: Mutex::new(Vec::new()),
            grant_request_hook: Mutex::new(None),
            credential_sources_answer: Mutex::new(Err(CredentialSourcesFailure::NoRoute)),
            credential_source_reports: Mutex::new(Vec::new()),
        })
    }

    /// What later credential-source reports answer; until a test sets one,
    /// each is answered as by a Cloud without the route.
    pub(crate) fn answer_credential_sources(&self, answer: Result<(), CredentialSourcesFailure>) {
        *self
            .credential_sources_answer
            .lock()
            .expect("fake Cloud mutex poisoned") = answer;
    }

    /// Each credential-source report answered so far, in the order they
    /// arrived.
    pub(crate) fn credential_source_reports(&self) -> Vec<CredentialSourcesRequest> {
        self.credential_source_reports
            .lock()
            .expect("fake Cloud mutex poisoned")
            .clone()
    }

    /// What later grant-token requests for `provider` answer; until a test
    /// sets one, each is answered as by a Cloud without the route.
    pub(crate) fn answer_grant_token<P>(&self, provider: P, answer: FakeGrantToken)
    where
        P: TryInto<GrantProvider>,
        P::Error: std::fmt::Debug,
    {
        let provider = grant_provider(provider);
        let mut answers = self.grant_tokens.lock().expect("fake Cloud mutex poisoned");
        answers.retain(|(answered, _)| *answered != provider);
        answers.push((provider, answer));
    }

    /// What later provider-list requests answer; until a test sets one,
    /// each is answered as by a Cloud without the route.
    pub(crate) fn answer_grant_providers(
        &self,
        answer: Result<Vec<ListedProvider>, GrantProvidersFailure>,
    ) {
        *self
            .grant_providers_answer
            .lock()
            .expect("fake Cloud mutex poisoned") = answer;
    }

    /// What later token-service-list requests answer; until a test sets
    /// one, each is answered as by a Cloud without the route.
    pub(crate) fn answer_token_services(
        &self,
        answer: Result<Vec<ListedProvider>, GrantProvidersFailure>,
    ) {
        *self
            .token_services_answer
            .lock()
            .expect("fake Cloud mutex poisoned") = answer;
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
            .iter()
            .filter_map(|request| match request {
                FakeGrantRequest::Token(request) => Some(request.clone()),
                FakeGrantRequest::Providers(_) | FakeGrantRequest::TokenServices(_) => None,
            })
            .collect()
    }

    /// Each provider-list request answered so far, in the order they
    /// arrived, each with the number of grant-token requests that arrived
    /// before it.
    pub(crate) fn grant_providers_requests(&self) -> Vec<(usize, GrantProvidersRequest)> {
        self.list_requests(|request| match request {
            FakeGrantRequest::Providers(request) => Some(request),
            FakeGrantRequest::TokenServices(_) | FakeGrantRequest::Token(_) => None,
        })
    }

    /// Each token-service-list request answered so far, in the order they
    /// arrived, each with the number of grant-token requests that arrived
    /// before it.
    pub(crate) fn token_services_requests(&self) -> Vec<(usize, GrantProvidersRequest)> {
        self.list_requests(|request| match request {
            FakeGrantRequest::TokenServices(request) => Some(request),
            FakeGrantRequest::Providers(_) | FakeGrantRequest::Token(_) => None,
        })
    }

    /// Each list request `of` takes, with the number of grant-token
    /// requests that arrived before it.
    fn list_requests(
        &self,
        of: impl Fn(&FakeGrantRequest) -> Option<&GrantProvidersRequest>,
    ) -> Vec<(usize, GrantProvidersRequest)> {
        let mut tokens = 0;
        self.grant_requests
            .lock()
            .expect("fake Cloud mutex poisoned")
            .iter()
            .filter_map(|request| {
                if matches!(request, FakeGrantRequest::Token(_)) {
                    tokens += 1;
                }
                of(request).map(|listed| (tokens, listed.clone()))
            })
            .collect()
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
            .push(FakeGrantRequest::Token(request.clone()));
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
        let (grant_id, generation) = match answer {
            FakeGrantToken::Issue(generation) => (FAKE_GRANT_ID, generation),
            FakeGrantToken::IssueIntegrationToken(version) => (FAKE_INTEGRATION_TOKEN_ID, version),
            FakeGrantToken::IssueFrom(grant_id, generation) => (grant_id, generation),
            FakeGrantToken::Fail(failure) => return Err(failure),
        };
        Ok(IssuedToken {
            grant_id: grant_id.to_owned(),
            generation,
            access_token: GrantAccessToken::new(
                fake_grant_token(request.provider.clone(), generation).as_bytes(),
            )
            .expect("the fake grant token is one of the token's form"),
            expires_at,
            refresh_after,
        })
    }

    fn grant_providers(
        &self,
        request: &GrantProvidersRequest,
    ) -> Result<Vec<ListedProvider>, GrantProvidersFailure> {
        self.grant_requests
            .lock()
            .expect("fake Cloud mutex poisoned")
            .push(FakeGrantRequest::Providers(request.clone()));
        self.grant_providers_answer
            .lock()
            .expect("fake Cloud mutex poisoned")
            .clone()
    }

    fn token_services(
        &self,
        request: &GrantProvidersRequest,
    ) -> Result<Vec<ListedProvider>, GrantProvidersFailure> {
        self.grant_requests
            .lock()
            .expect("fake Cloud mutex poisoned")
            .push(FakeGrantRequest::TokenServices(request.clone()));
        self.token_services_answer
            .lock()
            .expect("fake Cloud mutex poisoned")
            .clone()
    }

    fn credential_sources(
        &self,
        request: &CredentialSourcesRequest,
    ) -> Result<(), CredentialSourcesFailure> {
        self.credential_source_reports
            .lock()
            .expect("fake Cloud mutex poisoned")
            .push(request.clone());
        *self
            .credential_sources_answer
            .lock()
            .expect("fake Cloud mutex poisoned")
    }
}

/// An assignment gate that lets setup act for a number of calls, at either
/// level, and refuses after them, as when a held run starts working while a
/// pass runs.
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

impl CountedGate {
    /// Counts one call, answering whether it is one of the idle ones.
    fn count_call(&self) -> bool {
        self.idle_calls
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
    }
}

impl AssignmentGate for CountedGate {
    fn none_working(&self) -> bool {
        self.count_call()
    }

    fn while_still(&self, change: &mut dyn FnMut()) -> bool {
        let idle = self.count_call();
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
    fake_agent_login_setup_serving(cloud, home, &[], state_directory, recorder)
}

/// `fake_agent_login_setup`, for a service whose environment also carries
/// `variables`.
pub(crate) fn fake_agent_login_setup_serving(
    cloud: Arc<FakeAgentLoginCloud>,
    home: &Path,
    variables: &[(&str, &str)],
    state_directory: &Path,
    recorder: Arc<Recorder>,
) -> SetupDependencies {
    let home = std::ffi::OsString::from(home);
    SetupDependencies::new(
        cloud,
        FakeLoginCheck::new(),
        ServiceEnvironment::from_variables(
            variables
                .iter()
                .map(|(name, value)| {
                    (
                        std::ffi::OsString::from(name),
                        std::ffi::OsString::from(value),
                    )
                })
                .chain([(std::ffi::OsString::from("HOME"), home)]),
        ),
        &state_directory.join("runner-state.json"),
        recorder,
    )
}

/// The view of the workflow `source` holds, resolved from a file as a run's is, with every
/// step `pending` and at most four run at once.
pub(crate) fn fixture_run_view(source: &str) -> AssignmentRunView {
    let directory = tempfile::tempdir().expect("create workflow fixture directory");
    let path = directory.path().join("build.yaml");
    std::fs::write(&path, source).expect("write workflow fixture");
    let workflow = useful_execution::resolve_workflow_file(directory.path(), &path)
        .expect("resolve workflow fixture");
    AssignmentRunView::new(&workflow, 4)
}

/// One step transition at `sequence` whose wire projection is `observed` and whose physical
/// executions are `settled` and `started`.
pub(crate) fn step_execution_transition(
    sequence: u64,
    (step, role): (&str, WorkflowNodeRole),
    (from, to): (StepStateKind, StepStateKind),
    observed: Option<ObservedStepTransition>,
    settled: Option<SettledExecution>,
    started: Option<StartedExecution>,
) -> TransitionObservation<RunnerExecutionInstant> {
    TransitionObservation {
        event: TransitionEvent::Step {
            sequence: TransitionSequence(sequence),
            step: step.to_owned(),
            role,
            failure_policy: FailurePolicy::Required,
            from,
            to,
        },
        step: observed,
        executions: ObservedExecutions { settled, started },
    }
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

/// The tree-run fixture the tree-run job's tests and the connection's share: a manager whose
/// offers run a published tree with an injected `useful` against a fake capability server.
pub(in crate::service) mod tree_fixture {
    use std::fs;
    use std::io::{BufRead as _, BufReader, Read as _, Write as _};
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use ring::digest::{SHA256, digest};
    use serde_json::{Value, json};
    use tokio_tungstenite::tungstenite::Message;
    use url::Url;
    use useful_execution::EnvironmentSnapshot;
    use useful_runner_protocol::{
        CloudFrame, WorkflowTreeFileV1RunnerProjection, decode_cloud_frame,
    };

    use super::{
        CountedGate, FakeAgentLoginCloud, FakeGrantToken, fake_agent_login_setup_serving,
        run_fixture_git,
    };
    use crate::agent_login::cloud::{GrantTokenFailure, GrantTokenRefusal};
    use crate::agent_login::directory::LoginDirectory;
    use crate::agent_login::setup::AgentLoginSetup;
    use crate::control_protocol::AgentLoginProvider;
    use crate::credential::test_credential;
    use crate::service::assignment::test_support::{keep_agent_logins_in, tree_run_manager};
    use crate::service::assignment::{AssignmentManager, AssignmentOffer};
    use crate::service::config::{AssignmentConfig, Config, RepositoryUrlPolicy};
    use crate::service::control::{AdmissionAgentLogins, LiveStatus};
    use crate::service::source::SourceCredentialBroker;
    use crate::service::tree_run::{HttpTreeBroker, TreeRunDependencies};

    /// The digest the fixture's offer names for its tree.
    pub(in crate::service) const TREE_DIGEST: &str =
        "sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945";

    /// The injected `useful`: answers `definition publish --json` with revision 1 of
    /// `$TREE_TEST_DIGEST`, making its revision directory as the local publish does, or, while
    /// `$TREE_TEST_PUBLISH_REPORT` is set, prints its value and exits 1 as a refused publish
    /// does, after writing `publish.args` either way; records
    /// each command line under `$TREE_TEST_LOG`, and for `run create` copies the revision's
    /// `resolution.json` there when one exists, copies the directory `$TREE_TEST_MIRROR`, when
    /// set, as the run's `mirror/`, and leaves a `run.json` reading `$TREE_TEST_STATE` and
    /// `$TREE_TEST_OUTCOME`, answering with `$TREE_TEST_RESULT` as the document's `result`, the
    /// engine's `{"outcome":"succeeded"}` when it is unset: for state `waiting`
    /// beside a `wait.json` of kind `$TREE_TEST_WAIT_KIND` whose rule names
    /// `$TREE_TEST_WAIT_SOURCE`, its `step` and the record's `position` the JSON
    /// `$TREE_TEST_POSITION` (`["merge"]` when unset), its `then` `$TREE_TEST_THEN` (`merge`
    /// when unset) and its `payload` the JSON `$TREE_TEST_WAIT_PAYLOAD` (`{}` when unset), the
    /// record first reading `running` at that position while
    /// `$TREE_TEST_LOG/running` exists, and for state `cancellable` a running record until `run
    /// cancel` ends it `canceled`; for state `refused` it makes no run and answers with the
    /// runtime's rejection whose diagnostic code is `$TREE_TEST_REFUSAL`, by default
    /// `required_tool_unavailable`, that of a root whose `requires.tools` names a missing tool.
    /// Each `run resume` appends its command line to
    /// `resume.args`, leaves its environment in `resume.env` and answers with the next word of
    /// `$TREE_TEST_RESUMES`, or with the word in `$TREE_TEST_LOG/answer` while that file
    /// exists: `held` (the default) reports the wait unchanged, `gated` waits until
    /// `$TREE_TEST_LOG/gate` no longer exists and then answers with the word in
    /// `$TREE_TEST_LOG/answer` when that file exists, reporting the wait unchanged while it
    /// still reads `gated` or is absent, `unavailable`
    /// refuses with `wait_condition_unavailable`, `refuse` refuses with
    /// `continuation_nodes_unsatisfied`, `stall` starts nothing and prints nothing until `run
    /// cancel` runs, and `continue` starts the record's next attempt at
    /// that position, then lands the run once `$TREE_TEST_LOG/proceed` exists, its result
    /// `{"outcome":"succeeded"}`. `run cancel` ends a waiting
    /// record `canceled`. A stage `$TREE_TEST_LOG/stages/<name>/` is copied into the run
    /// directory: `create` before the first record is written, `exited` after `create` writes
    /// an `exited` record, `resume<n>` by the n-th `resume` (after `continue` writes attempt 2)
    /// and `landed` before the run lands.
    /// `run event` copies its event and claim files to the log and answers by
    /// `$TREE_TEST_EVENT`: `cancel` applies `cancel`, ending the run `canceled`, after copying
    /// the stage `event` into the run directory, and signalling
    /// a `cancellable` process as the live run's cancellation does, `withdraw` applies
    /// `withdraw` with the JSON `$TREE_TEST_ACTION_OUTCOME` as its `actionOutcome`, leaving the
    /// record as it is, `not_later`
    /// refuses `event_not_later` before any read, and `owned` refuses `issue_in_owned_state`
    /// after one. `run stop` and `run lift` record their command lines; `run answer`, `approve`
    /// and `hand-back` append theirs to `control.args` and answer `recorded`, or, with
    /// `$TREE_TEST_CONTROL` `refuse`, refuse `question_unknown`, with `hang` hang until killed,
    /// with `silent` exit 0 printing nothing, which the runner reads as no document, and with
    /// `record_hang` (`approve` and `hand-back`) record their control for the run's
    /// wait under `controls/` and then hang until killed; `run retry` appends its command
    /// line there too and then answers as `resume`. `event`, `stop`, `lift`, `answer`,
    /// `approve`, `hand-back`, `retry` and `resume` (but not a retry's) each append their name
    /// to `$TREE_TEST_LOG/order`. `run event` with `$TREE_TEST_EVENT` `hang` starts a `sleep`
    /// beside it in its process group, writes the sleep's pid to `$TREE_TEST_LOG/hang.pid` and
    /// waits for it. While `$TREE_TEST_LOG/noise` exists every command first writes that many
    /// bytes to stderr. While `$TREE_TEST_LOG/unlaunchable` exists `run create` removes the
    /// injected `useful` itself, so no later command can be launched. While the directory
    /// `$TREE_TEST_LOG/git-home` exists `run create` first names its execution root in `git-root`
    /// and writes that root's local `credential.*` configuration to `create.git-config` and, run
    /// there with that directory as `HOME`, the answer of `git credential fill` for the root's
    /// `origin` to `create.credential` and its exit status to `create.credential.status`; and
    /// `run resume`, while `git-root` names a root, does the same to `resume.git-config`,
    /// `resume.credential` and `resume.credential.status` before it answers.
    /// `run withdraw` appends its command line to `withdraw.args` and its name to `order`, leaves
    /// its environment in `withdraw.env` and answers with the next word of
    /// `$TREE_TEST_WITHDRAWS`: `withdrawn` (the default) ends a waiting record `withdrawn`, as
    /// the runtime does, with the outcome write `withdrawn_write` under the run's `mirror/`, and
    /// answers `withdrawn`; `recorded` answers `recorded` and creates
    /// `$TREE_TEST_LOG/withdraw.recorded`, which a `cancellable` process honors by ending its run
    /// `withdrawn` the same way; `refuse` refuses `mirror_credentials_unavailable`;
    /// `withdrawn_hang` ends a waiting record as `withdrawn` does and then hangs until killed,
    /// printing nothing; and `hang` hangs until killed, leaving the record as it is.
    /// While the directory `$TREE_TEST_LOG/runs` exists, each run keeps all of the above in a log
    /// of its own instead, `runs/<assignment>` under it, named by the assignment directory its
    /// `USEFUL_HOME` lies under (`per_run_log`), so the runs of one manager count, gate and answer
    /// apart.
    const USEFUL_FIXTURE: &str = r#"#!/bin/sh
log="$TREE_TEST_LOG"
if [ -d "$log/runs" ]; then
  log="$log/runs/$(basename "$(dirname "$(dirname "$USEFUL_HOME")")")"
  mkdir -p "$log"
fi
position='["merge"]'
[ -z "$TREE_TEST_POSITION" ] || position="$TREE_TEST_POSITION"
wait_payload='{}'
[ -z "$TREE_TEST_WAIT_PAYLOAD" ] || wait_payload="$TREE_TEST_WAIT_PAYLOAD"
if [ -f "$log/noise" ]; then head -c "$(cat "$log/noise")" /dev/zero >&2; fi
stage() {
  if [ -d "$log/stages/$1" ]; then cp -R "$log/stages/$1/." "$run/"; fi
}
observe_git() {
  git -C "$1" config --local --get-regexp '^credential\.' > "$log/$2.git-config"
  printf 'url=%s\n\n' "$(git -C "$1" remote get-url origin)" |
    HOME="$log/git-home" GIT_TERMINAL_PROMPT=0 git -C "$1" credential fill > "$log/$2.credential" 2> /dev/null
  echo "$?" > "$log/$2.credential.status"
}
recorded() {
  printf '{"schemaVersion":1,"command":"useful workflow run %s","outcome":"recorded","exitStatus":0,"runDirectory":"%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5loc","control":{"ordinal":1,"event":"control.%s","path":"controls/0001-%s.json","wait":null},"wait":null}\n' "$1" "$2" "$1" "$1"
}
withdrawn() {
  rm -f "$1/wait.json"
  mkdir -p "$1/mirror"
  printf '{"schemaVersion":1,"hook":"outcome","at":"2026-07-23T00:00:05Z","issue":"AUTO-143","action":{"kind":"state","state":"Todo","binding":"todoState"},"outcome":"written"}\n' > "$1/mirror/0001-outcome.json"
  # Replaced whole, as the runtime writes it.
  printf '{"state":"exited","outcome":"withdrawn","attempts":1,"position":[]}\n' > "$1/run.json.next"
  mv "$1/run.json.next" "$1/run.json"
}
retrying=
if [ "$1 $2 $3" = "workflow run retry" ]; then
  printf '%s\n' "$*" >> "$log/control.args"
  echo retry >> "$log/order"
  retrying=1
  set -- workflow run resume "$4" --json
fi
case "$1 $2 $3" in
"workflow definition publish")
  printf '%s\n' "$@" > "$log/publish.args"
  if [ "${TREE_TEST_PUBLISH_REPORT+set}" ]; then
    printf '%s' "$TREE_TEST_PUBLISH_REPORT"
    exit 1
  fi
  env > "$log/publish.env"
  if test -x "$4/bin/check.sh"; then echo executable; else echo plain; fi > "$log/check.mode"
  if test -x "$4/workflow.yaml"; then echo executable; else echo plain; fi > "$log/workflow.mode"
  cp "$6" "$log/bindings.json"
  mkdir -p -m 700 "$USEFUL_HOME/workflows/def_01k0z6r1w8f4jy2m7q9v3x5loc/revisions/1"
  printf '{"definition":{"id":"def_01k0z6r1w8f4jy2m7q9v3x5loc"},"revision":{"n":1,"tree":"%s"}}\n' "$TREE_TEST_DIGEST"
  ;;
"workflow run create")
  printf '%s\n' "$@" > "$log/create.args"
  env > "$log/create.env"
  if [ -e "$log/unlaunchable" ]; then rm -f "$0"; fi
  if [ -d "$log/git-home" ]; then
    printf '%s' "$6" > "$log/git-root"
    observe_git "$6" create
  fi
  if [ "$TREE_TEST_STATE" = refused ]; then
    printf '{"schemaVersion":1,"command":"useful workflow run create","outcome":"rejected","exitStatus":1,"phase":"admission","workflow":{"path":"workflow.yaml"},"diagnostics":[{"code":"%s","message":"Install the tool the root workflow'"'"'s `requires.tools` names, or put it on the launching `PATH`.","location":{"kind":"required_tool","tool":"useful-test-no-such-tool"}}]}\n' "${TREE_TEST_REFUSAL:-required_tool_unavailable}"
    exit 1
  fi
  resolution="$USEFUL_HOME/workflows/def_01k0z6r1w8f4jy2m7q9v3x5loc/revisions/1/resolution.json"
  if test -e "$resolution"; then cp "$resolution" "$log/resolution.json"; fi
  run="$USEFUL_HOME/workflows/def_01k0z6r1w8f4jy2m7q9v3x5loc/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc"
  mkdir -p "$run"
  if test -n "$TREE_TEST_MIRROR"; then cp -R "$TREE_TEST_MIRROR" "$run/mirror"; fi
  stage create
  case "$TREE_TEST_STATE" in
  cancellable)
    printf '{"state":"running","outcome":null,"attempts":1,"position":["build"]}\n' > "$run/run.json"
    n=0
    while [ ! -e "$log/cancelled" ] && [ ! -e "$log/withdraw.recorded" ] && [ "$n" -lt 400 ]; do sleep 0.05; n=$((n + 1)); done
    if [ -e "$log/withdraw.recorded" ]; then
      withdrawn "$run"
    else
      printf '{"state":"exited","outcome":"canceled","attempts":1,"position":[]}\n' > "$run/run.json"
    fi
    ;;
  waiting)
    if [ -e "$log/running" ]; then
      printf '{"state":"running","outcome":null,"attempts":1,"position":%s}\n' "$position" > "$run/run.json"
      n=0
      while [ -e "$log/running" ] && [ "$n" -lt 400 ]; do sleep 0.05; n=$((n + 1)); done
    fi
    printf '{"kind":"%s","since":"2026-07-23T00:00:05Z","step":%s,"then":"%s","rule":{"source":"%s","event":"pull_request.merged"},"payload":%s}\n' "$TREE_TEST_WAIT_KIND" "$position" "${TREE_TEST_THEN:-merge}" "$TREE_TEST_WAIT_SOURCE" "$wait_payload" > "$run/wait.json"
    printf '{"state":"waiting","outcome":null,"attempts":1,"position":%s}\n' "$position" > "$run/run.json"
    ;;
  exited)
    printf '{"state":"exited","outcome":%s,"attempts":1,"position":[]}\n' "$TREE_TEST_OUTCOME" > "$run/run.json"
    stage exited
    ;;
  *)
    printf '{"state":"%s","outcome":%s,"attempts":1,"position":["build"]}\n' "$TREE_TEST_STATE" "$TREE_TEST_OUTCOME" > "$run/run.json"
    ;;
  esac
  result="$TREE_TEST_RESULT"
  [ -n "$result" ] || result='{"outcome":"succeeded"}'
  printf '{"runDirectory":"%s","result":%s}\n' "$run" "$result"
  ;;
"workflow run event")
  run="$4"
  printf '%s\n' "$@" >> "$log/event.args"
  echo event >> "$log/order"
  cp "$6" "$log/event.json"
  cp "$8" "$log/claim.json"
  case "$TREE_TEST_EVENT" in
  cancel)
    : > "$log/cancelled"
    rm -f "$4/wait.json"
    stage event
    printf '{"state":"exited","outcome":"canceled","attempts":1,"position":[]}\n' > "$4/run.json"
    printf '{"schemaVersion":1,"command":"useful workflow run event","outcome":"applied","exitStatus":0,"runDirectory":"%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5loc","trigger":"departed","action":"cancel","actionOutcome":"canceled","control":{"ordinal":1,"event":"cancel","path":"controls/0001-cancel.json","wait":null},"observation":{"team":"team_8c1b","state":"state_canceled","updatedAt":"2026-07-23T00:00:07.512Z"}}\n' "$4"
    ;;
  withdraw)
    printf '{"schemaVersion":1,"command":"useful workflow run event","outcome":"applied","exitStatus":0,"runDirectory":"%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5loc","trigger":"departed","action":"withdraw","actionOutcome":%s,"control":{"ordinal":1,"event":"withdraw","path":"controls/0001-withdraw.json","wait":null},"observation":{"team":"team_8c1b","state":"state_canceled","updatedAt":"2026-07-23T00:00:07.512Z"}}\n' "$4" "$TREE_TEST_ACTION_OUTCOME"
    ;;
  hang)
    sleep 60 &
    printf '%s\n' "$!" > "$log/hang.pid"
    wait
    ;;
  owned)
    printf '{"schemaVersion":1,"command":"useful workflow run event","outcome":"refused","exitStatus":1,"runDirectory":"%s","diagnostics":[{"code":"issue_in_owned_state","message":"owned"}],"observation":{"team":"team_8c1b","state":"state_build","updatedAt":null}}\n' "$4"
    exit 1
    ;;
  *)
    printf '{"schemaVersion":1,"command":"useful workflow run event","outcome":"refused","exitStatus":1,"runDirectory":"%s","diagnostics":[{"code":"event_not_later","message":"not later"}],"observation":null}\n' "$4"
    exit 1
    ;;
  esac
  ;;
"workflow run stop")
  printf '%s\n' "$@" > "$log/stop.args"
  echo stop >> "$log/order"
  recorded stop "$4"
  ;;
"workflow run lift")
  printf '%s\n' "$@" > "$log/lift.args"
  echo lift >> "$log/order"
  recorded lift "$4"
  ;;
"workflow run answer"|"workflow run approve"|"workflow run hand-back")
  printf '%s\n' "$*" >> "$log/control.args"
  echo "$3" >> "$log/order"
  if [ "$TREE_TEST_CONTROL" = refuse ]; then
    printf '{"schemaVersion":1,"command":"useful workflow run %s","outcome":"refused","exitStatus":1,"runDirectory":"%s","diagnostics":[{"code":"question_unknown","message":"unknown"}]}\n' "$3" "$4"
    exit 1
  fi
  if [ "$TREE_TEST_CONTROL" = silent ]; then
    exit 0
  fi
  if [ "$TREE_TEST_CONTROL" = record_hang ]; then
    name=$(printf '%s' "$3" | tr - _)
    mkdir -p "$4/controls"
    printf '{"source":"um","event":"control.%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5abp","by":"cloud","at":"2026-07-23T00:00:06Z","wait":"%s@2026-07-23T00:00:05Z"}\n' "$name" "$TREE_TEST_WAIT_KIND" > "$4/controls/0001-$name.json"
  fi
  case "$TREE_TEST_CONTROL" in
  hang|record_hang) sleep 60 & wait ;;
  esac
  recorded "$3" "$4"
  ;;
"workflow run resume")
  run="$4"
  [ -n "$retrying" ] || echo resume >> "$log/order"
  printf '%s\n' "$run" >> "$log/resume.log"
  printf '%s\n' "$*" >> "$log/resume.args"
  env > "$log/resume.env"
  if [ -f "$log/git-root" ]; then observe_git "$(cat "$log/git-root")" resume; fi
  n=$(wc -l < "$log/resume.log" | tr -d ' ')
  answer=$(printf '%s\n' $TREE_TEST_RESUMES | sed -n "${n}p")
  if [ -f "$log/answer" ]; then answer=$(cat "$log/answer"); fi
  if [ "$answer" = gated ]; then
    i=0
    while [ -e "$log/gate" ] && [ "$i" -lt 400 ]; do sleep 0.05; i=$((i + 1)); done
    if [ -f "$log/answer" ]; then answer=$(cat "$log/answer"); fi
  fi
  case "$answer" in
  continue)
    rm -f "$run/wait.json"
    next=$(($(sed -n 's/.*"attempts":\([0-9]*\).*/\1/p' "$run/run.json") + 1))
    printf '{"state":"running","outcome":null,"attempts":%s,"position":%s}\n' "$next" "$position" > "$run/run.json"
    stage "resume$n"
    i=0
    while [ ! -e "$log/proceed" ] && [ "$i" -lt 400 ]; do sleep 0.05; i=$((i + 1)); done
    stage landed
    printf '{"state":"exited","outcome":"landed","attempts":%s,"position":[]}\n' "$next" > "$run/run.json"
    printf '{"outcome":"succeeded","runDirectory":"%s","result":{"outcome":"succeeded"}}\n' "$run"
    ;;
  refuse)
    printf '{"schemaVersion":1,"outcome":"rejected","exitStatus":1,"phase":"continuation","runDirectory":"%s","diagnostics":[{"code":"continuation_nodes_unsatisfied","message":"refused"}]}\n' "$run"
    exit 1
    ;;
  unavailable)
    stage "resume$n"
    printf '{"schemaVersion":1,"outcome":"rejected","exitStatus":1,"phase":"continuation","runDirectory":"%s","attemptNumber":1,"diagnostics":[{"code":"wait_condition_unavailable","message":"unavailable"}]}\n' "$run"
    exit 1
    ;;
  stall)
    i=0
    while [ ! -e "$log/cancelled" ] && [ "$i" -lt 400 ]; do sleep 0.05; i=$((i + 1)); done
    exit 1
    ;;
  *)
    stage "resume$n"
    printf '{"schemaVersion":1,"outcome":"waiting","exitStatus":0,"runDirectory":"%s","attemptNumber":1}\n' "$run"
    ;;
  esac
  ;;
"workflow run withdraw")
  printf '%s\n' "$*" >> "$log/withdraw.args"
  echo withdraw >> "$log/order"
  env > "$log/withdraw.env"
  n=$(wc -l < "$log/withdraw.args" | tr -d ' ')
  answer=$(printf '%s\n' $TREE_TEST_WITHDRAWS | sed -n "${n}p")
  case "$answer" in
  recorded)
    : > "$log/withdraw.recorded"
    printf '{"schemaVersion":1,"command":"useful workflow run withdraw","outcome":"recorded","exitStatus":0,"runDirectory":"%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5loc"}\n' "$4"
    ;;
  refuse)
    printf '{"schemaVersion":1,"command":"useful workflow run withdraw","outcome":"refused","exitStatus":1,"runDirectory":"%s","diagnostics":[{"code":"mirror_credentials_unavailable","message":"unavailable"}]}\n' "$4"
    exit 1
    ;;
  hang) sleep 60 & wait ;;
  *)
    if grep -q '"state":"waiting"' "$4/run.json"; then withdrawn "$4"; fi
    if [ "$answer" = withdrawn_hang ]; then sleep 60 & wait; fi
    printf '{"schemaVersion":1,"command":"useful workflow run withdraw","outcome":"withdrawn","exitStatus":0,"runDirectory":"%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5loc"}\n' "$4"
    ;;
  esac
  ;;
"workflow run cancel")
  printf '%s\n' "$@" > "$log/cancel.args"
  : > "$log/cancelled"
  if grep -q '"state":"waiting"' "$4/run.json"; then
    rm -f "$4/wait.json"
    # Replaced whole, as the runtime writes it, so a `cancel` killed midway leaves it readable.
    printf '{"state":"exited","outcome":"canceled","attempts":1,"position":[]}\n' > "$4/run.json.next"
    mv "$4/run.json.next" "$4/run.json"
  fi
  ;;
esac
"#;

    /// A file of a published tree.
    pub(in crate::service) struct TreeFile {
        pub(in crate::service) path: &'static str,
        pub(in crate::service) bytes: &'static [u8],
        pub(in crate::service) executable: bool,
    }

    /// A dataflow tree: its root's steps `build` and `review` are `cmd` steps, so the runtime
    /// holds before none of them.
    pub(in crate::service) const TREE_FILES: &[TreeFile] = &[
    TreeFile {
        path: "workflow.yaml",
        bytes: b"schemaVersion: 1\nname: build\nsteps:\n  build:\n    kind: cmd\n    command:\n      argv: [/bin/sh, -c, 'exit 0']\n  review:\n    kind: cmd\n    dependsOn: [build]\n    command:\n      argv: [/bin/sh, -c, 'exit 0']\n",
        executable: false,
    },
    TreeFile {
        path: "bin/check.sh",
        bytes: b"#!/bin/sh\nexit 0\n",
        executable: true,
    },
];

    fn sha256_hex(bytes: &[u8]) -> String {
        digest(&SHA256, bytes)
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// A Gateway stand-in serving the tree capability route and the signed file URLs of
    /// `tree`.
    fn capability_server(tree: &'static [TreeFile]) -> (Url, Arc<Mutex<Vec<Value>>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        std::thread::spawn(move || {
            for connection in listener.incoming() {
                let Ok(mut connection) = connection else {
                    return;
                };
                let mut reader = BufReader::new(connection.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                let mut length = 0;
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    if header == "\r\n" {
                        break;
                    }
                    if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let target = request_line
                    .split(' ')
                    .nth(1)
                    .unwrap_or_default()
                    .to_owned();
                let (content_type, answer) = if let Some(path) = target.strip_prefix("/files/") {
                    let file = tree.iter().find(|file| file.path == path).unwrap();
                    ("application/octet-stream", file.bytes.to_vec())
                } else {
                    let request: Value = serde_json::from_slice(&body).unwrap();
                    let files = request["files"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|path| {
                            let file = tree
                                .iter()
                                .find(|file| file.path == path.as_str().unwrap())
                                .unwrap();
                            json!({
                                "path": file.path,
                                "sha256": sha256_hex(file.bytes),
                                "size": file.bytes.len(),
                                "executable": file.executable,
                                "url": format!("http://{address}/files/{}", file.path),
                            })
                        })
                        .collect::<Vec<_>>();
                    let answer = json!({
                        "schemaVersion": 1,
                        "digest": request["digest"],
                        "capabilityExpiresAt": "2099-01-01T00:00:00Z",
                        "files": files,
                    });
                    recorded.lock().unwrap().push(request);
                    ("application/json", serde_json::to_vec(&answer).unwrap())
                };
                write!(
                connection,
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                answer.len()
            )
            .unwrap();
                connection.write_all(&answer).unwrap();
            }
        });
        (
            Url::parse(&format!("ws://{address}/v1/runner/connect")).unwrap(),
            requests,
        )
    }

    const TREE_OFFER: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-workflow-tree.json"
    ));

    /// The protocol fixture's offer of a published tree.
    pub(in crate::service) fn tree_offer() -> CloudFrame {
        decode_cloud_frame(TREE_OFFER).unwrap()
    }

    /// The protocol fixture's offer of a published tree, as Cloud sends `offer`: its IDs, its
    /// commits and its tree's digest, path, stop and files set in the frame.
    pub(in crate::service) fn offer_frame(offer: &AssignmentOffer) -> Message {
        let mut frame: Value = serde_json::from_slice(TREE_OFFER).unwrap();
        let payload = &mut frame["payload"];
        payload["effectId"] = json!(offer.effect_id);
        payload["assignmentId"] = json!(offer.assignment_id);
        payload["runId"] = json!(offer.run_id);
        payload["projectId"] = json!(offer.project_id);
        payload["attemptId"] = json!(offer.attempt_id);
        payload["attemptNumber"] = json!(offer.attempt_number);
        let spec = &offer.execution_spec;
        let wire = &mut payload["executionSpec"];
        wire["workflowDefinitionSource"]["commitOid"] =
            json!(spec.workflow_definition_source.commit_oid);
        wire["primaryWorkspaceSource"]["commitOid"] =
            json!(spec.primary_workspace_source.commit_oid);
        let tree = spec.workflow_tree.as_ref().unwrap();
        let wire = &mut wire["workflowTree"];
        wire["digest"] = json!(tree.digest);
        wire["workflowPath"] = json!(tree.workflow_path);
        wire["stopBefore"] = json!(tree.stop_before);
        wire["files"] = tree
            .files
            .iter()
            .map(|file| {
                json!({
                    "path": file.path,
                    "sha256": file.sha256,
                    "size": file.size,
                    "executable": file.executable,
                })
            })
            .collect();
        Message::Text(frame.to_string().into())
    }

    /// The built `useful` beside the test binary, which serves the workflow-Git helper's
    /// internal mode the test binary does not; the checks build it before the tests.
    pub(in crate::service) fn built_useful() -> PathBuf {
        std::env::current_exe()
            .unwrap()
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .join(format!("useful{}", std::env::consts::EXE_SUFFIX))
    }

    /// Writes the injected `useful` to `directory`.
    pub(in crate::service) fn injected_useful(directory: &Path) -> PathBuf {
        let useful = directory.join("useful");
        fs::write(&useful, USEFUL_FIXTURE).unwrap();
        fs::set_permissions(&useful, fs::Permissions::from_mode(0o755)).unwrap();
        useful
    }

    /// An offered tree's files and the `useful` that publishes and runs it, written or found
    /// for the fixture's temporary directory: by default the injected script.
    pub(in crate::service) struct OfferedTree {
        pub(in crate::service) files: &'static [TreeFile],
        pub(in crate::service) useful: fn(&Path) -> PathBuf,
    }

    impl From<&'static [TreeFile]> for OfferedTree {
        fn from(files: &'static [TreeFile]) -> Self {
            Self {
                files,
                useful: injected_useful,
            }
        }
    }

    /// A manager whose next offer, `offer`, is a tree run, beside what it runs on: the
    /// temporary directory, the repository, the injected `useful`'s log and the capability
    /// requests the fake Gateway answered.
    pub(in crate::service) struct Fixture {
        pub(in crate::service) _temporary: tempfile::TempDir,
        /// The repository every workspace checkout clones.
        pub(in crate::service) source: PathBuf,
        pub(in crate::service) manager: AssignmentManager,
        pub(in crate::service) offer: AssignmentOffer,
        pub(in crate::service) log: PathBuf,
        pub(in crate::service) capability_requests: Arc<Mutex<Vec<Value>>>,
    }

    /// A manager whose next offer is a tree run of `tree`'s files, each workspace checked out
    /// through the broker `source_broker` makes for the fixture repository, `tree`'s `useful`,
    /// by default the injected one, reporting `published_digest` and leaving a record in
    /// `state` with `outcome`, `variables` added to the daemon's environment, its telemetry
    /// recorded to `recorder` and its tree-run dependencies those `tune` makes of the
    /// fixture's.
    #[expect(
        clippy::too_many_arguments,
        reason = "each fixture variant names the parameters it varies through its wrapper"
    )]
    pub(in crate::service) fn fixture_sourced(
        published_digest: &str,
        state: &str,
        outcome: &str,
        variables: &[(&str, &str)],
        recorder: Option<Arc<crate::telemetry::Recorder>>,
        tune: impl FnOnce(TreeRunDependencies) -> TreeRunDependencies,
        source_broker: impl FnOnce(&Path) -> Arc<dyn SourceCredentialBroker>,
        tree: impl Into<OfferedTree>,
    ) -> Fixture {
        let OfferedTree { files, useful } = tree.into();
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let work = temporary.path().join("work");
        let log = temporary.path().join("log");
        for directory in [&source, &work, &log] {
            fs::create_dir(directory).unwrap();
        }
        fs::set_permissions(&work, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(source.join("README"), "workspace\n").unwrap();
        run_fixture_git(&source, &["init", "--quiet", "--object-format=sha1"]);
        run_fixture_git(&source, &["config", "user.name", "Tree Fixture"]);
        run_fixture_git(
            &source,
            &["config", "user.email", "fixture@usefulmachinery.invalid"],
        );
        run_fixture_git(&source, &["add", "."]);
        run_fixture_git(&source, &["commit", "--quiet", "-m", "fixture"]);
        let commit = run_fixture_git(&source, &["rev-parse", "HEAD"]);
        let useful = useful(temporary.path());

        let (endpoint, capability_requests) = capability_server(files);
        let config = Config::new(
            "wss://gateway.example.test/v1/runner/connect",
            test_credential(),
            false,
            AssignmentConfig::new(&work).unwrap(),
            RepositoryUrlPolicy::with_file_repositories(true),
        )
        .unwrap();
        let broker = HttpTreeBroker::new(
            &endpoint,
            &test_credential(),
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abe",
        )
        .unwrap();
        let environment = EnvironmentSnapshot::new(
            [
                ("PATH", std::env::var_os("PATH").unwrap_or_default()),
                ("TREE_TEST_LOG", log.clone().into_os_string()),
                ("TREE_TEST_DIGEST", published_digest.into()),
                ("TREE_TEST_STATE", state.into()),
                ("TREE_TEST_OUTCOME", outcome.into()),
                ("GITHUB_TOKEN", "daemon-github-token".into()),
            ]
            .into_iter()
            .chain(
                variables
                    .iter()
                    .map(|(name, value)| (*name, std::ffi::OsString::from(value))),
            ),
        );
        let manager = tree_run_manager(
            &config,
            source_broker(&source),
            tune(
                TreeRunDependencies::new(Some(Arc::new(broker)))
                    .with_executable(useful)
                    .with_helper_executable(built_useful()),
            ),
            environment,
            recorder,
        );

        let CloudFrame::AssignmentOffer {
            effect_id,
            assignment_id,
            run_id,
            project_id,
            attempt_id,
            attempt_number,
            execution_spec,
            ..
        } = tree_offer()
        else {
            panic!("expected an assignment offer");
        };
        let mut execution_spec = *execution_spec;
        execution_spec
            .workflow_definition_source
            .commit_oid
            .clone_from(&commit);
        execution_spec.primary_workspace_source.commit_oid = commit;
        let tree = execution_spec.workflow_tree.as_mut().unwrap();
        tree.digest = TREE_DIGEST.to_owned();
        tree.workflow_path = "workflow.yaml".to_owned();
        tree.stop_before = Some("review".to_owned());
        tree.files = files
            .iter()
            .map(|file| WorkflowTreeFileV1RunnerProjection {
                path: file.path.to_owned(),
                sha256: sha256_hex(file.bytes),
                size: file.bytes.len() as u64,
                executable: file.executable,
            })
            .collect();
        Fixture {
            _temporary: temporary,
            source,
            manager,
            offer: AssignmentOffer {
                effect_id,
                assignment_id,
                run_id,
                project_id,
                attempt_id,
                attempt_number,
                execution_spec,
                continuation: None,
            },
            log,
            capability_requests,
        }
    }

    /// What `issue_github_token` set up: the state directory, the setup and the telemetry
    /// capture, which the caller keeps, the assignment's GitHub token file and the Cloud that
    /// issued it.
    pub(in crate::service) struct IssuedGithubToken {
        state: tempfile::TempDir,
        _setup: Arc<AgentLoginSetup>,
        _capture: crate::telemetry::TestCapture,
        pub(in crate::service) file: PathBuf,
        pub(in crate::service) cloud: Arc<FakeAgentLoginCloud>,
    }

    impl IssuedGithubToken {
        /// The GitHub token file of the assignment `assignment_id`, under its own directory.
        pub(in crate::service) fn file_for(&self, assignment_id: &str) -> PathBuf {
            login_directory(self.state.path())
                .grant_token(assignment_id, AgentLoginProvider::Github)
                .unwrap()
                .0
        }
    }

    /// The agent-login directory of the runner whose state lies in `state`.
    fn login_directory(state: &Path) -> LoginDirectory {
        LoginDirectory::beside(&state.join("runner-state.json"))
    }

    /// Has the fixture's manager, whose service carries `service`, set agent logins up for its
    /// admissions over a Cloud that issues the assignment a GitHub token of generation 3 and
    /// refuses a GitLab one.
    pub(in crate::service) fn issue_github_token(
        fixture: &mut Fixture,
        service: &[(&str, &str)],
    ) -> IssuedGithubToken {
        let state = tempfile::tempdir().unwrap();
        let home = state.path().join("service-home");
        fs::create_dir(&home).unwrap();
        let directory = login_directory(state.path());
        let (file, _) = directory
            .grant_token(&fixture.offer.assignment_id, AgentLoginProvider::Github)
            .unwrap();
        keep_agent_logins_in(&mut fixture.manager, directory);
        let cloud = FakeAgentLoginCloud::new(Ok(Vec::new()));
        cloud.answer_grant_token(AgentLoginProvider::Github, FakeGrantToken::Issue(3));
        cloud.answer_grant_token(
            AgentLoginProvider::Gitlab,
            FakeGrantToken::Fail(GrantTokenFailure::Refused(
                GrantTokenRefusal::ReconnectRequired,
            )),
        );
        let (recorder, capture) = crate::telemetry::test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abe");
        let setup = Arc::new(AgentLoginSetup::new(
            fake_agent_login_setup_serving(
                Arc::clone(&cloud),
                &home,
                service,
                state.path(),
                recorder,
            ),
            Arc::new(CountedGate::idle()),
            || time::OffsetDateTime::from_unix_timestamp(1_794_450_000).unwrap(),
        ));
        let status = Arc::new(LiveStatus::new(
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            None,
        ));
        fixture
            .manager
            .set_up_agent_logins_with(AdmissionAgentLogins::new(&setup, status));
        IssuedGithubToken {
            state,
            _setup: setup,
            _capture: capture,
            file,
            cloud,
        }
    }

    /// `id`, an ID of the fixture's offer, marked as another run's by `mark`, a digit other
    /// than 5.
    pub(in crate::service) fn other_id(id: &str, mark: char) -> String {
        let marked = id.replacen("v3x5", &format!("v3x{mark}"), 1);
        assert_ne!(marked, id, "{id} carries no mark");
        marked
    }

    /// A second offer of `offer`'s tree on the same manager, another run under effect,
    /// assignment, run and attempt IDs of its own, marked by `mark`.
    pub(in crate::service) fn other_offer(offer: &AssignmentOffer, mark: char) -> AssignmentOffer {
        AssignmentOffer {
            effect_id: other_id(&offer.effect_id, mark),
            assignment_id: other_id(&offer.assignment_id, mark),
            run_id: other_id(&offer.run_id, mark),
            attempt_id: other_id(&offer.attempt_id, mark),
            ..offer.clone()
        }
    }

    /// Has the injected `useful` keep each run's log apart and returns the one for `offer`'s
    /// run, created, which its gates, answers and counters read in place of the fixture's log.
    pub(in crate::service) fn per_run_log(fixture: &Fixture, offer: &AssignmentOffer) -> PathBuf {
        let log = fixture.log.join("runs").join(&offer.assignment_id);
        fs::create_dir_all(&log).unwrap();
        log
    }
}
