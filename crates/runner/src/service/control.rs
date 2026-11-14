use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions, Permissions};
use std::future::Future;
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use fs4::{FileExt, TryLockError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, oneshot};

use um_execution::HarnessLoginObserver;

use super::assignment::AssignmentManager;
use crate::agent_login::OfferedLogin;
use crate::agent_login::grant_token::AssignmentGrantTokens;
use crate::agent_login::readiness::ReadinessDocument;
use crate::agent_login::setup::{
    AgentLoginSetup, SYNC_BOUND, SetupAnswer, SetupCaller, SetupOutcome,
};
use crate::control_protocol::{
    AgentLoginReadiness, ConnectionFailure, ConnectionState, ControlError, Operation, ProcessState,
    Response, StatusSnapshot, SyncedAgentLogins, decode_request, encode_response,
};

const RUNTIME_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_MODE: u32 = 0o600;
const RUNTIME_LOCK_FILE: &str = ".runner-serve.lock";
const MAXIMUM_CONNECTIONS: usize = 16;
#[expect(
    clippy::cast_possible_wrap,
    reason = "O_NOFOLLOW fits in the signed custom_flags value on supported Unix targets"
)]
const NOFOLLOW_FLAG: i32 = rustix::fs::OFlags::NOFOLLOW.bits() as i32;

pub(super) struct ReloadRequest {
    pub(super) response: oneshot::Sender<Result<String, ControlError>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ControlTimeouts {
    io: Duration,
    snapshot: Duration,
    reload: Duration,
    /// How long a sync waits for its agent-login pass: `SYNC_BOUND`.
    agent_login_sync: Duration,
}

impl ControlTimeouts {
    pub(super) const fn production() -> Self {
        Self {
            io: Duration::from_secs(2),
            snapshot: Duration::from_secs(1),
            reload: Duration::from_secs(30),
            agent_login_sync: SYNC_BOUND,
        }
    }
}

/// What the control socket serves from: the live status, the assignment
/// manager for its counts, the reload channel and the agent-login setup,
/// which is absent without protected state.
#[derive(Clone)]
pub(super) struct ControlContext {
    pub(super) status: Arc<LiveStatus>,
    pub(super) assignments: Arc<Mutex<AssignmentManager>>,
    pub(super) reloads: mpsc::Sender<ReloadRequest>,
    pub(super) agent_logins: Option<Arc<AgentLoginSetup>>,
    pub(super) timeouts: ControlTimeouts,
}

/// Runs one agent-login setup for `caller` and publishes the readiness it
/// recorded in the live status, also when an assignment interrupted it
/// after it had recorded an install or removal: answers that readiness with
/// each login whose check gave no answer, `runner_busy` while the runner
/// holds an assignment, and `state_update_failed` when the readiness could
/// not be recorded. The live status carries no check's answer.
pub(super) async fn set_up_agent_logins(
    setup: &Arc<AgentLoginSetup>,
    status: &LiveStatus,
    caller: SetupCaller,
) -> Result<SyncedAgentLogins, ControlError> {
    let SetupAnswer {
        outcome,
        unanswered,
    } = setup.set_up(caller).await;
    publish(outcome, status).map(|readiness| SyncedAgentLogins {
        readiness,
        unanswered_checks: unanswered,
    })
}

/// Publishes in the live status the readiness a pass that ended with
/// `outcome` recorded, and answers it, or the error `set_up_agent_logins`
/// answers.
fn publish(
    outcome: SetupOutcome,
    status: &LiveStatus,
) -> Result<AgentLoginReadiness, ControlError> {
    match outcome {
        SetupOutcome::Busy(recorded) => {
            if let Some(readiness) = recorded.as_ref().and_then(ReadinessDocument::reported) {
                status.agent_logins_recorded(readiness);
            }
            Err(ControlError::RunnerBusy)
        }
        SetupOutcome::Failed => Err(ControlError::StateUpdateFailed),
        SetupOutcome::Recorded(document) => {
            let readiness = document.reported().ok_or(ControlError::StateUpdateFailed)?;
            status.agent_logins_recorded(readiness.clone());
            Ok(readiness)
        }
    }
}

/// The agent-login setup an admission makes for itself before its
/// assignment is offered the runner's logins (REQ-GRANT-49), publishing
/// what it records in the live status as every pass does. The assignment
/// manager holds it, and the setup's own gate holds the manager, so it holds
/// the setup weakly: a setup that is gone makes no pass.
#[derive(Clone)]
pub(super) struct AdmissionAgentLogins {
    setup: Weak<AgentLoginSetup>,
    status: Arc<LiveStatus>,
}

impl AdmissionAgentLogins {
    pub(super) fn new(setup: &Arc<AgentLoginSetup>, status: Arc<LiveStatus>) -> Self {
        Self {
            setup: Arc::downgrade(setup),
            status,
        }
    }

    /// Makes the admission's pass, when the readiness names a login, then
    /// asks Cloud for the grant tokens of the assignment `assignment_id` of
    /// the run `run_id` and this boot (REQ-GRANT-86), and publishes what
    /// each recorded. It
    /// blocks, on the admission's preparation task, for the bounded requests
    /// of each; what it fails at is the setup's own record and never the
    /// admission's. It answers what the admission holds of its assignment's
    /// grant tokens, which removes their files when it is dropped; `None`
    /// when the setup is gone.
    pub(super) fn set_up(
        &self,
        run_id: &str,
        assignment_id: &str,
    ) -> Option<AssignmentGrantTokens> {
        let setup = self.setup.upgrade()?;
        let admitted = setup.set_up_admission(self.status.boot_id(), run_id, assignment_id);
        if let Some(outcome) = admitted.agent_logins {
            let _ = publish(outcome, &self.status);
        }
        if let Some(document) = admitted.grants {
            let _ = publish(SetupOutcome::Recorded(document), &self.status);
        }
        Some(admitted.tokens)
    }

    /// The observer of the logins `offered` added to the admission's
    /// environment, which notes each use for the next pass
    /// (REQ-GRANT-52); `None` when it was offered none or the setup
    /// is gone.
    pub(super) fn observer(
        &self,
        offered: Vec<OfferedLogin>,
    ) -> Option<Arc<dyn HarnessLoginObserver>> {
        self.setup.upgrade()?.observer(offered)
    }
}

struct LiveStatusState {
    process_state: ProcessState,
    connection_state: ConnectionState,
    last_connected_at: Option<String>,
    current_credential_id: Option<String>,
    pending_credential_id: Option<String>,
    last_connection_failure: Option<ConnectionFailure>,
    agent_logins: Option<AgentLoginReadiness>,
}

pub(super) struct LiveStatus {
    boot_id: String,
    started_at: Instant,
    state: Mutex<LiveStatusState>,
    connected: tokio::sync::Notify,
}

impl LiveStatus {
    pub(super) fn new(
        boot_id: String,
        current_credential_id: String,
        pending_credential_id: Option<String>,
    ) -> Self {
        Self {
            boot_id,
            started_at: um_support::monotonic_now(),
            state: Mutex::new(LiveStatusState {
                process_state: ProcessState::Running,
                connection_state: ConnectionState::Connecting,
                last_connected_at: None,
                current_credential_id: Some(current_credential_id),
                pending_credential_id,
                last_connection_failure: None,
                agent_logins: None,
            }),
            connected: tokio::sync::Notify::new(),
        }
    }

    /// The boot this process serves as.
    pub(super) fn boot_id(&self) -> &str {
        &self.boot_id
    }

    pub(super) fn connecting(&self) {
        self.lock().connection_state = ConnectionState::Connecting;
    }

    pub(super) fn connected(&self, connected_at: Option<String>) {
        let mut state = self.lock();
        state.connection_state = ConnectionState::Connected;
        if connected_at.is_some() {
            state.last_connected_at = connected_at;
        }
        state.last_connection_failure = None;
        drop(state);
        self.connected.notify_waiters();
    }

    pub(super) async fn wait_until_connected(&self) {
        loop {
            let notified = self.connected.notified();
            if self.is_connected() {
                return;
            }
            notified.await;
        }
    }

    pub(super) fn is_connected(&self) -> bool {
        self.lock().connection_state == ConnectionState::Connected
    }

    pub(super) fn backing_off(&self, failure: ConnectionFailure) {
        self.update_connection(ConnectionState::BackingOff, Some(failure));
    }

    pub(super) fn authentication_failed(&self) {
        self.update_connection(
            ConnectionState::AuthenticationFailed,
            Some(ConnectionFailure::Authentication),
        );
    }

    pub(super) fn protocol_failed(&self) {
        self.update_connection(
            ConnectionState::ProtocolFailed,
            Some(ConnectionFailure::Protocol),
        );
    }

    pub(super) fn stopping(&self) {
        let mut state = self.lock();
        state.process_state = ProcessState::Stopping;
        state.connection_state = ConnectionState::Stopping;
    }

    pub(super) fn promoted(&self, credential_id: String) {
        let mut state = self.lock();
        state.current_credential_id = Some(credential_id);
        state.pending_credential_id = None;
    }

    pub(super) fn pending(&self, credential_id: Option<String>) {
        self.lock().pending_credential_id = credential_id;
    }

    /// The readiness the latest agent-login setup pass recorded.
    pub(super) fn agent_logins_recorded(&self, readiness: AgentLoginReadiness) {
        self.lock().agent_logins = Some(readiness);
    }

    fn update_connection(
        &self,
        connection_state: ConnectionState,
        failure: Option<ConnectionFailure>,
    ) {
        let mut state = self.lock();
        state.connection_state = connection_state;
        state.last_connection_failure = failure;
    }

    fn snapshot(
        &self,
        assignment_counts: crate::control_protocol::AssignmentCounts,
    ) -> StatusSnapshot {
        let state = self.lock();
        let uptime_milliseconds =
            u64::try_from(um_support::elapsed(self.started_at).as_millis()).unwrap_or(u64::MAX);
        StatusSnapshot {
            process_state: state.process_state,
            boot_id: self.boot_id.clone(),
            uptime_milliseconds,
            connection_state: state.connection_state,
            last_connected_at: state.last_connected_at.clone(),
            current_credential_id: state.current_credential_id.clone(),
            pending_credential_id: state.pending_credential_id.clone(),
            assignment_counts,
            last_connection_failure: state.last_connection_failure,
            agent_logins: state.agent_logins.clone(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LiveStatusState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(super) struct ControlServer {
    task: tokio::task::JoinHandle<()>,
    _socket: SocketGuard,
}

impl ControlServer {
    pub(super) fn bind(
        socket_path: &Path,
        context: ControlContext,
    ) -> Result<Self, ControlServerError> {
        let (listener, socket) = SocketGuard::bind(socket_path)?;
        let task = tokio::spawn(serve(listener, context));
        Ok(Self {
            task,
            _socket: socket,
        })
    }
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ControlServerError {
    UnsafeRuntimeDirectory,
    RuntimeOwned,
    UnsafeSocket,
    BindSocket,
}

impl fmt::Display for ControlServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::UnsafeRuntimeDirectory => "runner control runtime directory is unsafe",
            Self::RuntimeOwned => "another Runner Serve process owns the runtime",
            Self::UnsafeSocket => "runner control socket path is unsafe",
            Self::BindSocket => "runner control socket cannot be bound",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ControlServerError {}

#[derive(Debug)]
struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}

impl SocketGuard {
    fn bind(path: &Path) -> Result<(UnixListener, Self), ControlServerError> {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or(ControlServerError::UnsafeRuntimeDirectory)?;
        validate_runtime_directory(parent)?;
        let lock_path = parent.join(RUNTIME_LOCK_FILE);
        let lock = open_runtime_lock(&lock_path)?;
        match FileExt::try_lock(&lock) {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(ControlServerError::RuntimeOwned),
            Err(TryLockError::Error(_)) => return Err(ControlServerError::UnsafeRuntimeDirectory),
        }
        inspect_existing_socket(path)?;
        let listener = std::os::unix::net::UnixListener::bind(path)
            .map_err(|_| ControlServerError::BindSocket)?;
        listener
            .set_nonblocking(true)
            .map_err(|_| ControlServerError::BindSocket)?;
        fs::set_permissions(path, Permissions::from_mode(PRIVATE_MODE)).map_err(|_| {
            let _ = fs::remove_file(path);
            ControlServerError::BindSocket
        })?;
        let metadata = fs::symlink_metadata(path).map_err(|_| ControlServerError::BindSocket)?;
        if !safe_socket(&metadata) {
            let _ = fs::remove_file(path);
            return Err(ControlServerError::UnsafeSocket);
        }
        let listener = UnixListener::from_std(listener).map_err(|_| {
            let _ = fs::remove_file(path);
            ControlServerError::BindSocket
        })?;
        Ok((
            listener,
            Self {
                path: path.to_owned(),
                device: metadata.dev(),
                inode: metadata.ino(),
                _lock: lock,
            },
        ))
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.dev() == self.device && metadata.ino() == self.inode && safe_socket(&metadata)
        }) {
            let _ = fs::remove_file(&self.path);
        }
        // A concurrent fork can briefly retain this close-on-exec descriptor.
        // Unlock explicitly so that child cannot delay the next bind until exec.
        let _ = FileExt::unlock(&self._lock);
    }
}

fn validate_runtime_directory(path: &Path) -> Result<(), ControlServerError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| ControlServerError::UnsafeRuntimeDirectory)?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o7777 != RUNTIME_DIRECTORY_MODE
    {
        return Err(ControlServerError::UnsafeRuntimeDirectory);
    }
    Ok(())
}

fn open_runtime_lock(path: &Path) -> Result<File, ControlServerError> {
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(PRIVATE_MODE)
        .custom_flags(NOFOLLOW_FLAG)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(NOFOLLOW_FLAG)
            .open(path)
            .map_err(|_| ControlServerError::UnsafeRuntimeDirectory)?,
        Err(_) => return Err(ControlServerError::UnsafeRuntimeDirectory),
    };
    let metadata = file
        .metadata()
        .map_err(|_| ControlServerError::UnsafeRuntimeDirectory)?;
    if !safe_private_file(&metadata) {
        return Err(ControlServerError::UnsafeRuntimeDirectory);
    }
    Ok(file)
}

fn safe_private_file(metadata: &Metadata) -> bool {
    metadata.file_type().is_file()
        && metadata.uid() == rustix::process::geteuid().as_raw()
        && metadata.mode() & 0o7777 == PRIVATE_MODE
        && metadata.nlink() == 1
}

fn safe_socket(metadata: &Metadata) -> bool {
    metadata.file_type().is_socket()
        && metadata.uid() == rustix::process::geteuid().as_raw()
        && metadata.mode() & 0o7777 == PRIVATE_MODE
        && metadata.nlink() == 1
}

fn inspect_existing_socket(path: &Path) -> Result<(), ControlServerError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(ControlServerError::UnsafeSocket),
    };
    if !safe_socket(&metadata) {
        return Err(ControlServerError::UnsafeSocket);
    }
    match StdUnixStream::connect(path) {
        Ok(_) => Err(ControlServerError::RuntimeOwned),
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
            let current =
                fs::symlink_metadata(path).map_err(|_| ControlServerError::UnsafeSocket)?;
            if !safe_socket(&current)
                || current.dev() != metadata.dev()
                || current.ino() != metadata.ino()
            {
                return Err(ControlServerError::UnsafeSocket);
            }
            fs::remove_file(path).map_err(|_| ControlServerError::UnsafeSocket)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(ControlServerError::UnsafeSocket),
    }
}

async fn serve(listener: UnixListener, context: ControlContext) {
    let capacity = Arc::new(Semaphore::new(MAXIMUM_CONNECTIONS));
    while let Ok((stream, _)) = listener.accept().await {
        let Ok(permit) = Arc::clone(&capacity).try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let context = context.clone();
        tokio::spawn(async move {
            let _permit = permit;
            handle_connection(stream, context).await;
        });
    }
}

async fn handle_connection(mut stream: UnixStream, context: ControlContext) {
    let ControlContext {
        status,
        assignments,
        reloads,
        agent_logins,
        timeouts,
    } = context;
    if stream.peer_cred().ok().map(|credential| credential.uid())
        != Some(rustix::process::geteuid().as_raw())
    {
        return;
    }
    let operation = match read_request(&mut stream, timeouts.io).await {
        Ok(bytes) => decode_request(&bytes),
        Err(()) => Err(ControlError::InvalidRequest),
    };
    let response = match operation {
        Ok(Operation::Status) => {
            let snapshot = control_timeout(
                timeouts.snapshot,
                tokio::task::spawn_blocking(move || {
                    let counts = assignments
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .status_counts();
                    status.snapshot(counts)
                }),
            )
            .await;
            match snapshot {
                Ok(Ok(snapshot)) => Response::Status(snapshot),
                Ok(Err(_)) | Err(_) => Response::Error(ControlError::InvalidRequest),
            }
        }
        Ok(Operation::ReloadCredential) => {
            let (response, receive) = oneshot::channel();
            if reloads.try_send(ReloadRequest { response }).is_err() {
                Response::Error(ControlError::PendingConnectionFailed)
            } else {
                match control_timeout(timeouts.reload, receive).await {
                    Ok(Ok(Ok(credential_id))) => Response::Reloaded { credential_id },
                    Ok(Ok(Err(error))) => Response::Error(error),
                    Ok(Err(_)) | Err(_) => Response::Error(ControlError::PendingConnectionFailed),
                }
            }
        }
        Ok(Operation::SyncAgentLogins) => match agent_logins {
            // Every enrolled runner has a setup; one without protected state
            // cannot record readiness.
            None => Response::Error(ControlError::StateUpdateFailed),
            Some(setup) => {
                // The pass publishes its readiness even if the answer is
                // abandoned at the bound.
                let sync = tokio::spawn(async move {
                    set_up_agent_logins(&setup, &status, SetupCaller::Sync).await
                });
                match control_timeout(timeouts.agent_login_sync, sync).await {
                    Ok(Ok(Ok(synced))) => Response::AgentLoginsSynced(synced),
                    Ok(Ok(Err(error))) => Response::Error(error),
                    Ok(Err(_)) | Err(_) => Response::Error(ControlError::StateUpdateFailed),
                }
            }
        },
        Err(error) => Response::Error(error),
    };
    let Ok(bytes) = encode_response(&response) else {
        return;
    };
    let _ = control_timeout(timeouts.io, stream.write_all(&bytes)).await;
}

async fn read_request(stream: &mut UnixStream, io_timeout: Duration) -> Result<Vec<u8>, ()> {
    let mut request = Vec::with_capacity(256);
    let mut chunk = [0_u8; 512];
    control_timeout(io_timeout, async {
        loop {
            let read = stream.read(&mut chunk).await.map_err(|_| ())?;
            if read == 0 {
                return Err(());
            }
            request.extend_from_slice(&chunk[..read]);
            if request.len() > crate::control_protocol::REQUEST_LIMIT {
                return Err(());
            }
            if let Some(newline) = request.iter().position(|byte| *byte == b'\n') {
                return (newline + 1 == request.len()).then_some(request).ok_or(());
            }
        }
    })
    .await
    .map_err(|_| ())?
}

#[expect(
    clippy::disallowed_methods,
    reason = "this is the production boundary for bounded local-control I/O and work"
)]
async fn control_timeout<F: Future>(
    duration: Duration,
    future: F,
) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::time::timeout(duration, future).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    use crate::agent_login::cloud::{LoginStatus, ProviderLogin};
    use crate::agent_login::readiness::READINESS_FILE;
    use crate::control_protocol::{
        AgentLoginCheckCause, AgentLoginProvider, AgentLoginState, AssignmentCounts, Response,
        UnansweredCheck, decode_response,
    };
    use crate::credential::test_credential;
    use crate::service::assignment::test_support::manager;
    use crate::service::test_support::{
        ConfigFixture, CountedGate, FAKE_AGENT_LOGIN_ID, FAKE_AGENT_LOGIN_SENTINEL,
        FakeAgentLoginCloud, fake_agent_login_setup, fake_codex_authorized, fixture_lease_clock,
    };
    use crate::telemetry::test_recorder;

    const TEST_TIMEOUTS: ControlTimeouts = ControlTimeouts {
        io: Duration::from_millis(50),
        snapshot: Duration::from_secs(1),
        reload: Duration::from_secs(30),
        agent_login_sync: Duration::from_secs(30),
    };

    fn context(agent_logins: Option<Arc<AgentLoginSetup>>) -> ControlContext {
        let config = ConfigFixture::new(
            "ws://127.0.0.1:1/v1/runner/connect",
            test_credential(),
            true,
        )
        .unwrap();
        let assignments = Arc::new(Mutex::new(manager(
            &config,
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            fixture_lease_clock(),
        )));
        let status = Arc::new(LiveStatus::new(
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            None,
        ));
        let (reloads, _requests) = mpsc::channel(1);
        ControlContext {
            status,
            assignments,
            reloads,
            agent_logins,
            timeouts: TEST_TIMEOUTS,
        }
    }

    async fn exchange_in(context: ControlContext, request: &[u8]) -> Vec<u8> {
        let (mut client, server) = UnixStream::pair().unwrap();
        let server = tokio::spawn(handle_connection(server, context));
        client.write_all(request).await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        server.await.unwrap();
        response
    }

    async fn exchange(request: &[u8]) -> Vec<u8> {
        exchange_in(context(None), request).await
    }

    /// Agent-login setup against `cloud` for a service whose home and state
    /// directory are under `root`.
    fn agent_login_setup(
        cloud: Arc<FakeAgentLoginCloud>,
        root: &Path,
        gate: Arc<CountedGate>,
    ) -> Arc<AgentLoginSetup> {
        let (home, state) = (root.join("home"), root.join("state"));
        fs::create_dir(&home).unwrap();
        fs::create_dir(&state).unwrap();
        fs::set_permissions(&state, Permissions::from_mode(0o700)).unwrap();
        let (recorder, _capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
        Arc::new(AgentLoginSetup::new(
            fake_agent_login_setup(cloud, &home, &state, recorder),
            gate,
            || time::OffsetDateTime::from_unix_timestamp(1_794_450_000).unwrap(),
        ))
    }

    const SYNC_REQUEST: &[u8] = b"{\"schemaVersion\":1,\"operation\":\"sync_agent_logins\"}\n";
    const STATUS_REQUEST: &[u8] = b"{\"schemaVersion\":1,\"operation\":\"status\"}\n";

    // REQ-GRANT-41, REQ-RUNNERENROLL-42, REQ-RUNNERENROLL-63
    #[tokio::test]
    async fn sync_runs_a_pass_and_answers_its_readiness_without_a_path() {
        let root = tempfile::tempdir().unwrap();
        let cloud = FakeAgentLoginCloud::new(fake_codex_authorized(2));
        let context = context(Some(agent_login_setup(
            Arc::clone(&cloud),
            root.path(),
            Arc::new(CountedGate::idle()),
        )));

        let before = exchange_in(context.clone(), STATUS_REQUEST).await;
        let Response::Status(status) = decode_response(&before).unwrap() else {
            panic!("status request did not return a snapshot");
        };
        assert_eq!(status.agent_logins, None);
        assert!(!String::from_utf8(before).unwrap().contains("agentLogins"));

        let response = exchange_in(context.clone(), SYNC_REQUEST).await;
        let Response::AgentLoginsSynced(synced) = decode_response(&response).unwrap() else {
            panic!("sync request did not return readiness");
        };
        // The setup checked the login it left `ready`, and the fake check,
        // like a runner without the program, gave no answer (REQ-GRANT-90).
        assert_eq!(
            synced.unanswered_checks,
            [UnansweredCheck {
                provider: AgentLoginProvider::Codex,
                cause: AgentLoginCheckCause::ProgramUnavailable,
            }]
        );
        let readiness = synced.readiness;
        assert_eq!(cloud.status_requests(), 2);
        assert_eq!(cloud.deliveries(), 1);
        let states = readiness
            .providers
            .iter()
            .map(|entry| (entry.provider, entry.state, entry.generation))
            .collect::<Vec<_>>();
        assert_eq!(
            states,
            [
                (AgentLoginProvider::Codex, AgentLoginState::Ready, Some(2)),
                (
                    AgentLoginProvider::ClaudeCode,
                    AgentLoginState::NotAuthorized,
                    None
                ),
                (
                    AgentLoginProvider::Github,
                    AgentLoginState::NotDelivered,
                    None
                ),
                (
                    AgentLoginProvider::Gitlab,
                    AgentLoginState::NotDelivered,
                    None
                ),
                (
                    AgentLoginProvider::Linear,
                    AgentLoginState::NotDelivered,
                    None
                ),
            ]
        );
        assert!(
            root.path()
                .join("state/.runner-agent-login/codex/auth.json")
                .is_file()
        );

        let after = exchange_in(context, STATUS_REQUEST).await;
        let Response::Status(status) = decode_response(&after).unwrap() else {
            panic!("status request did not return a snapshot");
        };
        assert_eq!(status.agent_logins.as_ref(), Some(&readiness));
        // The live status carries no check's answer.
        assert!(!String::from_utf8_lossy(&after).contains("unansweredChecks"));
        for encoded in [response, after] {
            let encoded = String::from_utf8(encoded).unwrap();
            for forbidden in [
                "installedPath",
                root.path().to_str().unwrap(),
                FAKE_AGENT_LOGIN_SENTINEL,
            ] {
                assert!(!encoded.contains(forbidden), "{forbidden}");
            }
        }
    }

    // REQ-GRANT-41, REQ-RUNNERENROLL-82
    #[tokio::test]
    async fn sync_while_an_assignment_is_held_answers_runner_busy_without_asking() {
        let root = tempfile::tempdir().unwrap();
        let cloud = FakeAgentLoginCloud::new(fake_codex_authorized(1));
        let context = context(Some(agent_login_setup(
            Arc::clone(&cloud),
            root.path(),
            Arc::new(CountedGate::busy()),
        )));

        assert_eq!(
            decode_response(&exchange_in(context.clone(), SYNC_REQUEST).await).unwrap(),
            Response::Error(ControlError::RunnerBusy)
        );
        assert_eq!(cloud.status_requests(), 0);
        assert!(!root.path().join("state").join(READINESS_FILE).exists());
        let Response::Status(status) =
            decode_response(&exchange_in(context, STATUS_REQUEST).await).unwrap()
        else {
            panic!("status request did not return a snapshot");
        };
        assert_eq!(status.agent_logins, None);
    }

    // REQ-GRANT-41, REQ-RUNNERENROLL-42
    #[tokio::test]
    async fn a_sync_an_assignment_interrupts_answers_runner_busy_and_status_shows_what_it_recorded()
    {
        let root = tempfile::tempdir().unwrap();
        let authorized = |provider| ProviderLogin {
            provider,
            agent_login_id: FAKE_AGENT_LOGIN_ID.to_owned(),
            status: LoginStatus::Authorized {
                generation: 1,
                record_schema_version: 1,
            },
        };
        let cloud =
            FakeAgentLoginCloud::new(Ok(AgentLoginProvider::INSTALLABLE.map(authorized).to_vec()));
        // The pass finds the runner idle, asks for the Codex login and
        // installs it; an assignment starts before it asks for the next.
        let gate = Arc::new(CountedGate::idle());
        gate.idle_calls
            .store(3, std::sync::atomic::Ordering::SeqCst);
        let context = context(Some(agent_login_setup(
            Arc::clone(&cloud),
            root.path(),
            gate,
        )));

        assert_eq!(
            decode_response(&exchange_in(context.clone(), SYNC_REQUEST).await).unwrap(),
            Response::Error(ControlError::RunnerBusy)
        );
        assert_eq!(cloud.deliveries(), 1);
        let Response::Status(status) =
            decode_response(&exchange_in(context, STATUS_REQUEST).await).unwrap()
        else {
            panic!("status request did not return a snapshot");
        };
        let readiness = status
            .agent_logins
            .expect("status shows what the interrupted pass recorded");
        let codex = readiness
            .providers
            .iter()
            .find(|entry| entry.provider == AgentLoginProvider::Codex)
            .expect("the Codex entry");
        assert_eq!(codex.state, AgentLoginState::Ready);
    }

    // REQ-GRANT-41, REQ-GRANT-49
    #[tokio::test]
    async fn sync_while_an_admitted_assignment_is_held_answers_runner_busy() {
        let root = tempfile::tempdir().unwrap();
        let cloud = FakeAgentLoginCloud::new(fake_codex_authorized(1));
        let gate = Arc::new(CountedGate::idle());
        let setup = agent_login_setup(Arc::clone(&cloud), root.path(), Arc::clone(&gate));
        let context = context(Some(Arc::clone(&setup)));
        assert!(matches!(
            decode_response(&exchange_in(context.clone(), SYNC_REQUEST).await).unwrap(),
            Response::AgentLoginsSynced(_)
        ));

        // An assignment is admitted: its own pass installs generation 2 and
        // publishes it, and the assignment is then held.
        gate.idle_calls
            .store(0, std::sync::atomic::Ordering::SeqCst);
        cloud.answer(fake_codex_authorized(2));
        let admission = AdmissionAgentLogins::new(&setup, Arc::clone(&context.status));
        let tokens = tokio::task::spawn_blocking(move || {
            admission.set_up(
                "run_01k0z6r1w8f4jy2m7q9v3x5abc",
                "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
            )
        })
        .await
        .unwrap();
        assert!(tokens.is_some());
        let requests = (cloud.status_requests(), cloud.deliveries());
        assert_eq!(requests.1, 2);

        assert_eq!(
            decode_response(&exchange_in(context.clone(), SYNC_REQUEST).await).unwrap(),
            Response::Error(ControlError::RunnerBusy)
        );
        assert_eq!((cloud.status_requests(), cloud.deliveries()), requests);
        let Response::Status(status) =
            decode_response(&exchange_in(context, STATUS_REQUEST).await).unwrap()
        else {
            panic!("status request did not return a snapshot");
        };
        let codex = status
            .agent_logins
            .expect("status shows what the admission's pass recorded")
            .providers
            .into_iter()
            .find(|entry| entry.provider == AgentLoginProvider::Codex)
            .expect("the Codex entry");
        assert_eq!(
            (codex.state, codex.generation),
            (AgentLoginState::Ready, Some(2))
        );
    }

    // REQ-RUNNERENROLL-33, REQ-RUNNERENROLL-34
    #[tokio::test]
    async fn lifecycle_rejects_hostile_paths_and_recovers_an_owned_stale_socket() {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        fs::set_permissions(root.path(), Permissions::from_mode(0o700)).unwrap();
        let socket = root.path().join("runner.sock");

        let (listener, first) = SocketGuard::bind(&socket).unwrap();
        assert_eq!(fs::metadata(&socket).unwrap().mode() & 0o7777, 0o600);
        assert_eq!(
            SocketGuard::bind(&socket).unwrap_err(),
            ControlServerError::RuntimeOwned
        );
        drop(listener);
        drop(first);
        assert!(!socket.exists());

        let stale = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, Permissions::from_mode(0o600)).unwrap();
        drop(stale);
        let (_listener, recovered) = SocketGuard::bind(&socket).unwrap();
        drop(recovered);

        fs::write(&socket, b"hostile").unwrap();
        assert_eq!(
            SocketGuard::bind(&socket).unwrap_err(),
            ControlServerError::UnsafeSocket
        );
        fs::remove_file(&socket).unwrap();
        let target = root.path().join("target");
        fs::write(&target, b"target").unwrap();
        symlink(&target, &socket).unwrap();
        assert_eq!(
            SocketGuard::bind(&socket).unwrap_err(),
            ControlServerError::UnsafeSocket
        );
    }

    // REQ-RUNNERENROLL-63
    #[tokio::test]
    async fn serves_a_bounded_secret_free_idle_snapshot() {
        let response = exchange(b"{\"schemaVersion\":1,\"operation\":\"status\"}\n").await;
        let Response::Status(status) = decode_response(&response).unwrap() else {
            panic!("status request did not return a snapshot");
        };
        assert_eq!(status.assignment_counts, AssignmentCounts::default());
        assert_eq!(
            status.current_credential_id.as_deref(),
            Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abc")
        );
        let encoded = String::from_utf8(response).unwrap();
        for forbidden in [
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "activationId",
            "organization",
            "connectionUrl",
            "workflow",
            "environment",
        ] {
            assert!(!encoded.contains(forbidden));
        }
    }

    #[test]
    fn production_io_timeout_is_two_seconds() {
        assert_eq!(ControlTimeouts::production().io, Duration::from_secs(2));
    }

    // REQ-RUNNERENROLL-43
    #[test]
    fn reconnecting_retains_the_last_closed_failure_category() {
        let status = LiveStatus::new(
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            None,
        );
        status.backing_off(ConnectionFailure::CloudUnavailable);
        status.connecting();
        let snapshot = status.snapshot(AssignmentCounts::default());
        assert_eq!(snapshot.connection_state, ConnectionState::Connecting);
        assert_eq!(
            snapshot.last_connection_failure,
            Some(ConnectionFailure::CloudUnavailable)
        );
    }

    // REQ-RUNNERENROLL-63
    #[tokio::test]
    async fn contains_malformed_and_oversized_requests() {
        for request in [
            b"{".to_vec(),
            b"not-json\n".to_vec(),
            b"{\"schemaVersion\":1,\"operation\":\"run_command\"}\n".to_vec(),
            {
                let mut oversized = vec![b'x'; crate::control_protocol::REQUEST_LIMIT];
                oversized.push(b'\n');
                oversized
            },
        ] {
            assert_eq!(
                decode_response(&exchange(&request).await).unwrap(),
                Response::Error(ControlError::InvalidRequest)
            );
        }
    }

    // REQ-RUNNERENROLL-33
    #[test]
    fn runtime_directory_must_be_private_and_owned() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), Permissions::from_mode(0o770)).unwrap();
        assert_eq!(
            SocketGuard::bind(&root.path().join("runner.sock")).unwrap_err(),
            ControlServerError::UnsafeRuntimeDirectory
        );
    }
}
