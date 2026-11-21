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

use useful_execution::{
    FileTail, HarnessLoginObserver, LiveRunViewRequest, RunView, RunViewPathElement,
    RunViewRequest, RunViewStep, RunViewStepOutput, RunViewStream, RunViewWork, read_run_file_tail,
    read_run_view,
};

use super::assignment::{AssignmentManager, AssignmentOverview, AssignmentRead};
use super::disk::DiskReadingHandle;
use super::execution::AssignmentRunView;
use super::history::{EventLog, LoggedEvent};
use super::tree_run::published_path;
use crate::agent_login::OfferedLogin;
use crate::agent_login::ReportedSources;
use crate::agent_login::cloud::CredentialSourcesRequest;
use crate::agent_login::grant_token::AssignmentGrantTokens;
use crate::agent_login::readiness::ReadinessDocument;
use crate::agent_login::setup::{
    AgentLoginSetup, AssignmentGate, SYNC_BOUND, SetupAnswer, SetupCaller, SetupOutcome,
};
use crate::control_protocol::{
    AgentLoginReadiness, AgentLoginSetupCause, AgentLoginSetupResult, AssignmentAgent,
    AssignmentChildRun, AssignmentCommand, AssignmentDetail, AssignmentLog, AssignmentLogRecord,
    AssignmentLogSource, AssignmentLogStream, AssignmentOutput, AssignmentQuery, AssignmentRun,
    AssignmentStep, AssignmentWorkflow, ConnectionFailure, ConnectionState, ControlError,
    ControlRequest, LOG_WINDOW_BYTES, OVERVIEW_RESPONSE_LIMIT, Operation, Overview,
    OverviewAssignment, OverviewConnection, OverviewDisk, OverviewRunElement, OverviewRunner,
    OverviewWait, ProcessState, ProtocolFailure, Response, StatusDisk, StatusSnapshot,
    SyncedAgentLogins, WRITTEN_ASSIGNMENT_STEPS, WRITTEN_CHILD_RUNS, decode_request,
    encode_assignment_not_held, encode_assignment_response, encode_overview_response,
    encode_response,
};

const RUNTIME_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_MODE: u32 = 0o600;
const RUNTIME_LOCK_FILE: &str = ".runner-serve.lock";
const MAXIMUM_CONNECTIONS: usize = 16;
/// How many `assignment` requests read a run directory at a time, each holding its place
/// until its read ends (REQ-RUNNERENROLL-136, INV-RUNNER-45).
pub(super) const RUN_READ_PLACES: usize = 4;
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
/// manager for its counts and entries, the places for run directory reads in
/// progress, `RUN_READ_PLACES` of them, the reload channel, the agent-login
/// setup, which is absent without protected state, and the runner ID and
/// `useful` version an overview names.
#[derive(Clone)]
pub(super) struct ControlContext {
    pub(super) status: Arc<LiveStatus>,
    pub(super) assignments: Arc<Mutex<AssignmentManager>>,
    pub(super) run_reads: Arc<Semaphore>,
    pub(super) reloads: mpsc::Sender<ReloadRequest>,
    pub(super) agent_logins: Option<Arc<AgentLoginSetup>>,
    pub(super) timeouts: ControlTimeouts,
    pub(super) runner_id: String,
    pub(super) version: String,
}

/// Runs one agent-login setup for `caller` and publishes the readiness it
/// recorded in the live status, also when a held run's work interrupted it
/// after it had recorded an install or removal: answers that readiness with
/// each login whose check gave no answer, `runner_busy` when a run the
/// runner holds is working, or has a command in flight where the setup had
/// a login to install or remove (REQ-GRANT-40), or once a rejoined service
/// has ended the setup (INV-RUNNER-41), and `state_update_failed`
/// when the readiness could not be recorded. The live status carries no
/// check's answer.
pub(super) async fn set_up_agent_logins(
    setup: &Arc<AgentLoginSetup>,
    status: &LiveStatus,
    caller: SetupCaller,
) -> Result<SyncedAgentLogins, ControlError> {
    let SetupAnswer {
        outcome,
        unanswered,
    } = setup.set_up(caller).await;
    let cause = match caller {
        SetupCaller::Sync => AgentLoginSetupCause::Sync,
        SetupCaller::Schedule => AgentLoginSetupCause::Schedule,
    };
    publish_pass(cause, outcome, status).map(|readiness| SyncedAgentLogins {
        readiness,
        unanswered_checks: unanswered,
    })
}

/// Lists among the runner's events that a pass `cause` asked for ended with
/// `outcome` (REQ-RUNNERENROLL-138), then publishes and answers what it
/// recorded as `publish` does.
fn publish_pass(
    cause: AgentLoginSetupCause,
    outcome: SetupOutcome,
    status: &LiveStatus,
) -> Result<AgentLoginReadiness, ControlError> {
    let result = match outcome {
        SetupOutcome::Recorded(_) => AgentLoginSetupResult::Finished,
        SetupOutcome::Busy(_) => AgentLoginSetupResult::RunnerBusy,
        SetupOutcome::Failed => AgentLoginSetupResult::Failed,
    };
    status
        .events()
        .record(LoggedEvent::AgentLoginsSetUp { cause, result });
    publish(outcome, status)
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

    /// Makes the admission's pass, when the readiness names a login, under
    /// `beside`, the gate over the other runs the runner held when the
    /// preparation began (INV-RUNNER-15), then asks Cloud for the grant
    /// tokens of the assignment `assignment_id` of the run `run_id` and this
    /// boot (REQ-GRANT-86), whatever the gate answers, and publishes what
    /// each recorded. It
    /// blocks, on the admission's preparation task, for the bounded requests
    /// of each; what it fails at or the gate stops is the setup's own record
    /// and never the admission's. It answers what the admission holds of its
    /// assignment's grant tokens, which removes their files when it is
    /// dropped; `None` when the setup is gone.
    pub(super) fn set_up(
        &self,
        beside: &dyn AssignmentGate,
        run_id: &str,
        assignment_id: &str,
    ) -> Option<AssignmentGrantTokens> {
        let setup = self.setup.upgrade()?;
        let admitted =
            setup.set_up_admission_beside(beside, self.status.boot_id(), run_id, assignment_id);
        if let Some(outcome) = admitted.agent_logins {
            let _ = publish_pass(AgentLoginSetupCause::Admission, outcome, &self.status);
        }
        if let Some(document) = admitted.grants {
            let _ = publish(SetupOutcome::Recorded(document), &self.status);
        }
        Some(admitted.tokens)
    }

    /// Reports to Cloud where the credentials of the assignment
    /// `assignment_id` of this boot come from, `reported` (REQ-GRANT-93,
    /// REQ-INTEGRATION-12), through the setup, on the admission's
    /// preparation task once it has settled them; the setup records a
    /// report Cloud does not take, and nothing here fails the admission.
    /// Nothing is reported when the setup is gone.
    pub(super) fn report_sources(&self, assignment_id: &str, reported: ReportedSources) {
        let ReportedSources {
            sources,
            token_services,
        } = reported;
        if let Some(setup) = self.setup.upgrade() {
            setup.report_credential_sources(&CredentialSourcesRequest {
                boot_id: self.status.boot_id().to_owned(),
                assignment_id: assignment_id.to_owned(),
                sources,
                token_services,
            });
        }
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
    /// The failed attempts since the last connection.
    attempts_since_connected: u64,
    /// When the next attempt begins, while the connection backs off.
    next_attempt_at: Option<Instant>,
    /// When the stop began; none while the process runs.
    stop_began_at: Option<Instant>,
}

impl LiveStatusState {
    fn update_connection(
        &mut self,
        connection_state: ConnectionState,
        failure: Option<ConnectionFailure>,
    ) {
        self.connection_state = connection_state;
        self.last_connection_failure = failure;
        self.next_attempt_at = None;
    }
}

pub(super) struct LiveStatus {
    boot_id: String,
    started_at: Instant,
    log_path: Option<String>,
    /// The disk monitor's published reading; none for a service that starts no monitor.
    disk: Option<DiskReadingHandle>,
    state: Mutex<LiveStatusState>,
    connected: tokio::sync::Notify,
    /// The runner's own events, recorded once the state's lock is released.
    events: Arc<EventLog>,
}

impl LiveStatus {
    pub(super) fn new(
        boot_id: String,
        current_credential_id: String,
        pending_credential_id: Option<String>,
    ) -> Self {
        Self {
            boot_id,
            started_at: useful_support::monotonic_now(),
            log_path: None,
            disk: None,
            state: Mutex::new(LiveStatusState {
                process_state: ProcessState::Running,
                connection_state: ConnectionState::Connecting,
                last_connected_at: None,
                current_credential_id: Some(current_credential_id),
                pending_credential_id,
                last_connection_failure: None,
                agent_logins: None,
                attempts_since_connected: 0,
                next_attempt_at: None,
                stop_began_at: None,
            }),
            connected: tokio::sync::Notify::new(),
            events: Arc::new(EventLog::new()),
        }
    }

    /// The status of a process that writes its log at `log_path`, the path the
    /// recorder answered when its log opened (REQ-RUNNERENROLL-42). A path that is not
    /// UTF-8 or not absolute cannot travel in the snapshot and is shown as no log.
    pub(super) fn with_log_path(self, log_path: Option<&Path>) -> Self {
        Self {
            log_path: log_path
                .filter(|path| path.is_absolute())
                .and_then(Path::to_str)
                .map(str::to_owned),
            ..self
        }
    }

    /// The status of a process whose disk monitor publishes in `disk`: the snapshot and
    /// the overview give the reading it holds, copied before the status's own lock is
    /// taken (REQ-RUNNERENROLL-42, REQ-RUNNERENROLL-133, INV-RUNNER-49).
    pub(super) fn with_disk(self, disk: DiskReadingHandle) -> Self {
        Self {
            disk: Some(disk),
            ..self
        }
    }

    /// The disk monitor's newest published reading, with its path as text and its time
    /// as RFC 3339; none before the first, without a monitor, or when its time cannot be
    /// written.
    fn disk_reading(&self) -> Option<StatusDisk> {
        let reading = self.disk.as_ref()?.published()?;
        Some(StatusDisk {
            path: reading.path.to_string_lossy().into_owned(),
            available_bytes: reading.available_bytes,
            total_bytes: reading.total_bytes,
            minimum_free_bytes: reading.minimum_free_bytes,
            low: reading.low,
            read_at: reading
                .read_at
                .format(&time::format_description::well_known::Rfc3339)
                .ok()?,
        })
    }

    /// The log of the runner's own events (REQ-RUNNERENROLL-138).
    pub(super) fn events(&self) -> &Arc<EventLog> {
        &self.events
    }

    /// The boot this process serves as.
    pub(super) fn boot_id(&self) -> &str {
        &self.boot_id
    }

    pub(super) fn connecting(&self) {
        let mut state = self.lock();
        state.connection_state = ConnectionState::Connecting;
        state.next_attempt_at = None;
    }

    pub(super) fn connected(&self, connected_at: Option<String>) {
        let mut state = self.lock();
        state.connection_state = ConnectionState::Connected;
        if connected_at.is_some() {
            state.last_connected_at = connected_at;
        }
        state.last_connection_failure = None;
        state.attempts_since_connected = 0;
        state.next_attempt_at = None;
        drop(state);
        self.events.record(LoggedEvent::Connected);
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

    /// Counts a failed attempt, the next one `delay` from now.
    pub(super) fn backing_off(&self, failure: ConnectionFailure, delay: Duration) {
        let mut state = self.lock();
        state.update_connection(ConnectionState::BackingOff, Some(failure));
        state.attempts_since_connected = state.attempts_since_connected.saturating_add(1);
        state.next_attempt_at = useful_support::monotonic_now().checked_add(delay);
        drop(state);
        self.events.record(LoggedEvent::ConnectionFailed {
            failure,
            next_attempt: Some(delay),
        });
    }

    pub(super) fn authentication_failed(&self) {
        self.failed_finally(
            ConnectionState::AuthenticationFailed,
            ConnectionFailure::Authentication,
        );
    }

    pub(super) fn protocol_failed(&self) {
        self.failed_finally(ConnectionState::ProtocolFailed, ConnectionFailure::Protocol);
    }

    /// The connection ended in `connection_state` for `failure`, with no next attempt.
    fn failed_finally(&self, connection_state: ConnectionState, failure: ConnectionFailure) {
        self.lock()
            .update_connection(connection_state, Some(failure));
        self.events.record(LoggedEvent::ConnectionFailed {
            failure,
            next_attempt: None,
        });
    }

    pub(super) fn stopping(&self) {
        let mut state = self.lock();
        state.process_state = ProcessState::Stopping;
        state.connection_state = ConnectionState::Stopping;
        let first = state.stop_began_at.is_none();
        state
            .stop_began_at
            .get_or_insert_with(useful_support::monotonic_now);
        drop(state);
        if first {
            self.events.record(LoggedEvent::Stopping);
        }
    }

    pub(super) fn promoted(&self, credential_id: String) {
        let event = LoggedEvent::CredentialReloaded {
            credential_id: credential_id.clone(),
        };
        let mut state = self.lock();
        state.current_credential_id = Some(credential_id);
        state.pending_credential_id = None;
        drop(state);
        self.events.record(event);
    }

    pub(super) fn pending(&self, credential_id: Option<String>) {
        self.lock().pending_credential_id = credential_id;
    }

    /// The readiness the latest agent-login setup pass recorded.
    pub(super) fn agent_logins_recorded(&self, readiness: AgentLoginReadiness) {
        self.lock().agent_logins = Some(readiness);
    }

    fn uptime_milliseconds(&self) -> u64 {
        milliseconds(useful_support::elapsed(self.started_at))
    }

    /// The live status, beside the assignment counts and held runs the manager answered
    /// in one read (REQ-RUNNERENROLL-42).
    fn snapshot(
        &self,
        assignment_counts: crate::control_protocol::AssignmentCounts,
        held_runs: Vec<crate::control_protocol::HeldRun>,
    ) -> StatusSnapshot {
        let disk = self.disk_reading();
        let state = self.lock();
        let uptime_milliseconds = self.uptime_milliseconds();
        StatusSnapshot {
            process_state: state.process_state,
            boot_id: self.boot_id.clone(),
            uptime_milliseconds,
            log_path: self.log_path.clone(),
            disk: disk.map(Box::new),
            connection_state: state.connection_state,
            last_connected_at: state.last_connected_at.clone(),
            current_credential_id: state.current_credential_id.clone(),
            pending_credential_id: state.pending_credential_id.clone(),
            assignment_counts,
            last_connection_failure: state.last_connection_failure,
            agent_logins: state.agent_logins.clone(),
            held_runs,
        }
    }

    /// The overview of the process serving as `runner_id` with `version`, beside the
    /// assignments the manager answered in one read (REQ-RUNNERENROLL-133).
    fn overview(
        &self,
        runner_id: String,
        version: String,
        assignments: AssignmentOverview,
    ) -> Overview {
        let events = self.events.newest_first();
        let disk = self.disk_reading();
        let state = self.lock();
        let now = useful_support::monotonic_now();
        let backing_off = state.connection_state == ConnectionState::BackingOff;
        Overview {
            runner: OverviewRunner {
                runner_id,
                boot_id: self.boot_id.clone(),
                version,
                uptime_milliseconds: self.uptime_milliseconds(),
                process_state: state.process_state.into(),
                stop_time_left_milliseconds: state.stop_began_at.map(|began| {
                    milliseconds(
                        super::SHUTDOWN_TIMEOUT
                            .saturating_sub(now.saturating_duration_since(began)),
                    )
                }),
            },
            disk: disk.map(overview_disk),
            connection: OverviewConnection {
                state: state.connection_state.into(),
                last_connected_at: state.last_connected_at.clone(),
                last_failure: state.last_connection_failure.map(Into::into),
                attempts_since_connected: backing_off.then_some(state.attempts_since_connected),
                next_attempt_in_milliseconds: state
                    .next_attempt_at
                    .filter(|_| backing_off)
                    .map(|next| milliseconds(next.saturating_duration_since(now))),
            },
            current_credential_id: state.current_credential_id.clone(),
            pending_credential_id: state.pending_credential_id.clone(),
            agent_logins: state.agent_logins.clone().map(Into::into),
            assignment_counts: assignments.counts,
            unacknowledged_reports: assignments.unacknowledged_reports,
            assignments: assignments.assignments,
            assignments_omitted: assignments.assignments_omitted,
            recent_assignments: assignments.recent_assignments,
            recent_assignments_omitted: 0,
            events,
            events_omitted: 0,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LiveStatusState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The overview's form of the snapshot's disk reading, the same members.
fn overview_disk(disk: StatusDisk) -> OverviewDisk {
    OverviewDisk {
        path: disk.path,
        available_bytes: disk.available_bytes,
        total_bytes: disk.total_bytes,
        minimum_free_bytes: disk.minimum_free_bytes,
        low: disk.low,
        read_at: disk.read_at,
    }
}

pub(super) fn milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The bytes of `overview` within `limit`: while they pass it, the last event is left out and
/// counted in `eventsOmitted`, the oldest first; once none is left the last recent
/// assignment, counted in `recentAssignmentsOmitted`; and once none is left the last entry,
/// counted in `assignmentsOmitted`, so what the runner holds goes last
/// (REQ-RUNNERENROLL-136). `Oversized` when the overview without any of them passes it.
fn fit_overview(mut overview: Overview, limit: usize) -> Result<Vec<u8>, ProtocolFailure> {
    loop {
        match encode_overview_response(&overview) {
            Ok(bytes) if bytes.len() <= limit => return Ok(bytes),
            Ok(_) | Err(ProtocolFailure::Oversized) => {}
            Err(failure) => return Err(failure),
        }
        if overview.events.pop().is_some() {
            overview.events_omitted = overview.events_omitted.saturating_add(1);
        } else if overview.recent_assignments.pop().is_some() {
            overview.recent_assignments_omitted =
                overview.recent_assignments_omitted.saturating_add(1);
        } else if overview.assignments.pop().is_some() {
            overview.assignments_omitted = overview.assignments_omitted.saturating_add(1);
        } else {
            return Err(ProtocolFailure::Oversized);
        }
    }
}

/// The `overview` answer (REQ-RUNNERENROLL-133), built on a blocking worker under the
/// snapshot bound from one read of the manager, whose guard is dropped before the live
/// status is read and the answer encoded (INV-RUNNER-45). `None` when the snapshot is not
/// built within the bound or cannot be written: the connection is closed unanswered, since
/// `invalid_request` would read as a serving process of an earlier release.
async fn overview_answer(
    status: Arc<LiveStatus>,
    assignments: Arc<Mutex<AssignmentManager>>,
    runner_id: String,
    version: String,
    bound: Duration,
) -> Option<Vec<u8>> {
    control_timeout(
        bound,
        tokio::task::spawn_blocking(move || {
            let held = assignments
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .overview_assignments();
            fit_overview(
                status.overview(runner_id, version, held),
                OVERVIEW_RESPONSE_LIMIT,
            )
        }),
    )
    .await
    .ok()?
    .ok()?
    .ok()
}

/// The `assignment` answer (REQ-RUNNERENROLL-144, REQ-RUNNERENROLL-145), built under the
/// snapshot bound on a blocking worker that holds one of `run_reads`' places until it ends,
/// also when the answer was given up, so reads abandoned at the bound cannot accumulate. The
/// worker takes the manager's lock once for the entry and the run's two paths and releases
/// it before it reads the run directory or the run log (INV-RUNNER-45). `None` when no place
/// is free or the answer is not built within the bound, or cannot be written: the
/// connection is closed unanswered, as the overview's is.
async fn assignment_answer(
    assignments: Arc<Mutex<AssignmentManager>>,
    run_reads: Arc<Semaphore>,
    query: AssignmentQuery,
    bound: Duration,
) -> Option<Vec<u8>> {
    control_timeout(bound, async move {
        let place = run_reads.acquire_owned().await.ok()?;
        tokio::task::spawn_blocking(move || {
            let _place = place;
            let read = assignments
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .read_assignment(&query.assignment_id);
            match read {
                None => Ok(encode_assignment_not_held()),
                Some(read) => {
                    fit_assignment(assignment_detail(read, query), OVERVIEW_RESPONSE_LIMIT)
                }
            }
        })
        .await
        .ok()?
        .ok()
    })
    .await
    .ok()?
}

/// The assignment the manager's `read` gives, with the run its published run directory holds
/// at the path `query` names and its log (REQ-RUNNERENROLL-144, REQ-RUNNERENROLL-145). A
/// run directory that cannot be read whole, or a path that names no child run, leaves the
/// run out; a run log that is absent or cannot be read leaves the log out. Neither fails the
/// answer. A single-workflow run is answered from its view instead.
fn assignment_detail(read: AssignmentRead, query: AssignmentQuery) -> AssignmentDetail {
    let AssignmentRead {
        entry,
        paths,
        view,
        now,
    } = read;
    let AssignmentQuery {
        run: path, step, ..
    } = query;
    if let Some(view) = view {
        return viewed_assignment(entry, &view, &path, step);
    }
    let view = paths.directory.as_deref().and_then(|directory| {
        read_run_view(
            directory,
            &RunViewRequest {
                path: path
                    .iter()
                    .map(|element| RunViewPathElement {
                        step: element.step.clone(),
                        item: element.item,
                        ordinal: element.ordinal,
                    })
                    .collect(),
                step: step.clone(),
                now,
                maximum_steps: WRITTEN_ASSIGNMENT_STEPS,
                maximum_child_runs: WRITTEN_CHILD_RUNS,
                maximum_output_bytes: LOG_WINDOW_BYTES,
            },
        )
        .ok()
    });
    let (run, output) = match view {
        Some(view) => {
            let (run, output) = assignment_run(view, path);
            (Some(run), output)
        }
        None => (None, RunViewStepOutput::NotGiven),
    };
    let log = step_log(step, output).or_else(|| paths.log.as_deref().and_then(run_log));
    AssignmentDetail { entry, run, log }
}

/// A single-workflow assignment's answer from its run's `view`: the run and the newest records
/// of the named `step` while `path` names the root, and no run for a child run, which such a
/// run has none of. The log is always a `step_records` one, `source` alone when the view gave
/// no step's records: such a run has no run log (REQ-RUNNERENROLL-144,
/// REQ-RUNNERENROLL-145).
fn viewed_assignment(
    entry: OverviewAssignment,
    view: &AssignmentRunView,
    path: &[OverviewRunElement],
    step: Option<String>,
) -> AssignmentDetail {
    let source_alone = AssignmentLog {
        source: AssignmentLogSource::StepRecords.into(),
        step: None,
        invocation_id: None,
        stdout: None,
        stderr: None,
        earlier_invocations_omitted: 0,
        text: None,
        omitted_bytes: None,
        records: Vec::new(),
        omitted_records: 0,
    };
    if !path.is_empty() {
        return AssignmentDetail {
            entry,
            run: None,
            log: Some(source_alone),
        };
    }
    let (run, output) = assignment_run(
        view.run(&LiveRunViewRequest {
            step: step.clone(),
            maximum_steps: WRITTEN_ASSIGNMENT_STEPS,
            maximum_output_bytes: LOG_WINDOW_BYTES,
        }),
        Vec::new(),
    );
    let log = match output {
        RunViewStepOutput::Observed(observed) => AssignmentLog {
            step,
            records: observed
                .records
                .into_iter()
                .map(|record| AssignmentLogRecord {
                    source: record.source,
                    text: record.text,
                    continuation: record.continuation,
                })
                .collect(),
            omitted_records: observed.omitted_records,
            omitted_bytes: (observed.omitted_bytes > 0).then_some(observed.omitted_bytes),
            ..source_alone
        },
        RunViewStepOutput::NotGiven
        | RunViewStepOutput::SettledWithNone
        | RunViewStepOutput::Retained(_) => source_alone,
    };
    AssignmentDetail {
        entry,
        run: Some(run),
        log: Some(log),
    }
}

fn count(number: usize) -> u64 {
    u64::try_from(number).unwrap_or(u64::MAX)
}

/// `view` as an assignment answer gives the run at `path`, and the output of the step the
/// request named, which the answer gives as its log.
fn assignment_run(
    view: RunView,
    path: Vec<OverviewRunElement>,
) -> (AssignmentRun, RunViewStepOutput) {
    let RunView {
        workflow_path,
        state,
        attempts,
        outcome,
        attempt_number,
        attempt_state,
        wait,
        steps,
        steps_omitted,
        output,
    } = view;
    let run = AssignmentRun {
        path,
        workflow_path,
        state,
        attempts,
        outcome,
        attempt_number,
        attempt_state,
        wait: wait.map(|wait| OverviewWait {
            kind: wait.kind,
            since: Some(wait.since),
            // The overview's rule for a wait's step, so the two answers give one wait alike.
            step: published_path(&wait.step),
        }),
        steps: steps.into_iter().map(assignment_step).collect(),
        steps_omitted: count(steps_omitted),
    };
    (run, output)
}

/// One step of a read run as an assignment answer gives it.
fn assignment_step(step: RunViewStep) -> AssignmentStep {
    let RunViewStep {
        id,
        role,
        state,
        failure_policy,
        needs,
        started_at,
        duration,
        work,
        outputs,
    } = step;
    let mut entry = AssignmentStep {
        id,
        role,
        kind: work.kind().to_owned(),
        needs,
        state,
        failure_policy,
        started_at,
        duration_milliseconds: duration.map(milliseconds),
        command: None,
        agent: None,
        workflow: None,
        outputs: outputs
            .into_iter()
            .map(|output| AssignmentOutput {
                name: output.name,
                kind: output.kind,
                committed: output.committed,
            })
            .collect(),
        child_runs: Vec::new(),
        child_runs_omitted: 0,
    };
    let declared = |word: String| (!word.is_empty()).then_some(word);
    match work {
        RunViewWork::Command { argv, cwd } => {
            entry.command = Some(AssignmentCommand { argv, cwd });
        }
        RunViewWork::Agent {
            profile,
            harness,
            model,
            effort,
        } => {
            entry.agent = Some(AssignmentAgent {
                profile,
                harness,
                model: declared(model),
                effort: declared(effort),
            });
        }
        RunViewWork::Workflow {
            file,
            child_runs,
            child_runs_omitted,
        } => {
            entry.workflow = Some(AssignmentWorkflow { file });
            entry.child_runs = child_runs
                .into_iter()
                .map(|child| AssignmentChildRun {
                    ordinal: child.ordinal,
                    item: child.item,
                    state: child.state,
                    outcome: child.outcome,
                })
                .collect();
            entry.child_runs_omitted = count(child_runs_omitted);
        }
    }
    entry
}

/// The log of the named `step` whose output the read gave: its retained streams, or `source`
/// and `step` alone for a settled step that retained none. `None` when the read gave none,
/// for the run log to stand in.
fn step_log(step: Option<String>, output: RunViewStepOutput) -> Option<AssignmentLog> {
    let named = |step| AssignmentLog {
        source: AssignmentLogSource::Step.into(),
        step: Some(step),
        invocation_id: None,
        stdout: None,
        stderr: None,
        earlier_invocations_omitted: 0,
        text: None,
        omitted_bytes: None,
        records: Vec::new(),
        omitted_records: 0,
    };
    match (step, output) {
        (Some(step), RunViewStepOutput::SettledWithNone) => Some(named(step)),
        (Some(step), RunViewStepOutput::Retained(retained)) => Some(AssignmentLog {
            invocation_id: Some(retained.invocation_id),
            stdout: Some(log_stream(retained.stdout)),
            stderr: Some(log_stream(retained.stderr)),
            earlier_invocations_omitted: count(retained.earlier_invocations_omitted),
            ..named(step)
        }),
        _ => None,
    }
}

fn log_stream(stream: RunViewStream) -> AssignmentLogStream {
    let RunViewStream {
        retained_bytes,
        discarded_bytes,
        window,
        omitted_bytes,
    } = stream;
    // A window that bytes were left out before can begin inside a character: its
    // continuation bytes, at most three, are left out with them.
    let inside = if omitted_bytes == 0 {
        0
    } else {
        window
            .iter()
            .take(3)
            .take_while(|byte| **byte & 0xC0 == 0x80)
            .count()
    };
    AssignmentLogStream {
        retained_bytes,
        discarded_bytes,
        omitted_bytes: omitted_bytes.saturating_add(count(inside)),
        text: String::from_utf8_lossy(&window[inside..]).into_owned(),
    }
}

/// The end of the run log at `log`, at most `LOG_WINDOW_BYTES`, begun after a line feed when
/// bytes were left out before it (REQ-RUNNERENROLL-145). `None` when it is absent or cannot be
/// read.
fn run_log(log: &Path) -> Option<AssignmentLog> {
    let FileTail {
        bytes,
        omitted_bytes,
    } = read_run_file_tail(log.parent()?, log.file_name()?.to_str()?, LOG_WINDOW_BYTES).ok()?;
    let start = if omitted_bytes == 0 {
        0
    } else {
        bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(bytes.len(), |line_feed| line_feed + 1)
    };
    Some(AssignmentLog {
        source: AssignmentLogSource::RunLog.into(),
        step: None,
        invocation_id: None,
        stdout: None,
        stderr: None,
        earlier_invocations_omitted: 0,
        text: Some(String::from_utf8_lossy(&bytes[start..]).into_owned()),
        omitted_bytes: Some(omitted_bytes.saturating_add(count(start))),
        records: Vec::new(),
        omitted_records: 0,
    })
}

/// The bytes of `detail` within `limit`: while they pass it, its log loses the older half of
/// its records, counted in `omittedRecords` and their bytes in `omittedBytes`, until none is
/// left, and the longer of its texts loses its first half at a character boundary, the bytes
/// counted in that text's `omittedBytes`, until every text is empty; then the last step is
/// left out, counted in `stepsOmitted`; and once none is left the run. The entry is always
/// kept (REQ-RUNNERENROLL-136). `Oversized` when the entry alone passes it.
fn fit_assignment(mut detail: AssignmentDetail, limit: usize) -> Result<Vec<u8>, ProtocolFailure> {
    loop {
        match encode_assignment_response(&detail) {
            Ok(bytes) if bytes.len() <= limit => return Ok(bytes),
            Ok(_) | Err(ProtocolFailure::Oversized) => {}
            Err(failure) => return Err(failure),
        }
        if detail.log.as_mut().is_some_and(shorten_log) {
            continue;
        }
        match detail.run.as_mut() {
            Some(run) if !run.steps.is_empty() => {
                run.steps.pop();
                run.steps_omitted = run.steps_omitted.saturating_add(1);
            }
            Some(_) => detail.run = None,
            None => return Err(ProtocolFailure::Oversized),
        }
    }
}

/// Drops the older half of `log`'s records, at least one, counting them in `omittedRecords`
/// and their text's bytes in `omittedBytes`; once it has none, the first half of the longer
/// of its texts, at a character boundary, counting the bytes in that text's `omittedBytes`.
/// `false` when it has no record and every text is empty.
fn shorten_log(log: &mut AssignmentLog) -> bool {
    let AssignmentLog {
        stdout,
        stderr,
        text,
        omitted_bytes,
        records,
        omitted_records,
        ..
    } = log;
    if !records.is_empty() {
        let dropped = (records.len() / 2).max(1);
        let bytes = records
            .drain(..dropped)
            .map(|record| record.text.len())
            .fold(0_usize, usize::saturating_add);
        *omitted_records = omitted_records.saturating_add(count(dropped));
        *omitted_bytes = Some(omitted_bytes.unwrap_or(0).saturating_add(count(bytes)));
        return true;
    }
    let mut texts = Vec::with_capacity(3);
    for stream in [stdout, stderr].into_iter().flatten() {
        texts.push((&mut stream.text, &mut stream.omitted_bytes));
    }
    if let Some(text) = text {
        texts.push((text, omitted_bytes.get_or_insert(0)));
    }
    let Some((text, omitted)) = texts
        .into_iter()
        .max_by_key(|(text, _)| text.len())
        .filter(|(text, _)| !text.is_empty())
    else {
        return false;
    };
    let mut cut = text.len() - text.len() / 2;
    while !text.is_char_boundary(cut) {
        cut += 1;
    }
    text.drain(..cut);
    *omitted = omitted.saturating_add(count(cut));
    true
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
        run_reads,
        reloads,
        agent_logins,
        timeouts,
        runner_id,
        version,
    } = context;
    if stream.peer_cred().ok().map(|credential| credential.uid())
        != Some(rustix::process::geteuid().as_raw())
    {
        return;
    }
    let request = match read_request(&mut stream, timeouts.io).await {
        Ok(bytes) => decode_request(&bytes),
        Err(()) => Err(ControlError::InvalidRequest),
    };
    let operation = match request {
        Ok(ControlRequest::Overview) => {
            if let Some(bytes) =
                overview_answer(status, assignments, runner_id, version, timeouts.snapshot).await
            {
                let _ = control_timeout(timeouts.io, stream.write_all(&bytes)).await;
            }
            return;
        }
        Ok(ControlRequest::Assignment(query)) => {
            if let Some(bytes) =
                assignment_answer(assignments, run_reads, query, timeouts.snapshot).await
            {
                let _ = control_timeout(timeouts.io, stream.write_all(&bytes)).await;
            }
            return;
        }
        Ok(ControlRequest::Operation(operation)) => Ok(operation),
        Err(error) => Err(error),
    };
    let response = match operation {
        Ok(Operation::Status) => {
            let snapshot = control_timeout(
                timeouts.snapshot,
                tokio::task::spawn_blocking(move || {
                    let (counts, held_runs) = assignments
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .status_assignments();
                    status.snapshot(counts, held_runs)
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
        AgentLoginCheckCause, AgentLoginProvider, AgentLoginState, AssignmentAnswer,
        AssignmentCounts, AssignmentPhase, HeldRunState, LiveStepState, OpenWord, OverviewAnswer,
        OverviewAssignment, OverviewCounts, OverviewRunningStep, Response, RunnerEventKind,
        UnansweredCheck, decode_assignment_response, decode_overview_response, decode_response,
        encode_assignment_request,
    };
    use crate::credential::test_credential;
    use crate::service::assignment::AssignmentOffer;
    use crate::service::assignment::test_support::{
        install_run_activity, install_run_view, install_running_renewal_fixture, manager,
    };
    use crate::service::test_support::tree_fixture::tree_offer;
    use crate::service::test_support::{
        ConfigFixture, CountedGate, FAKE_AGENT_LOGIN_ID, FAKE_AGENT_LOGIN_SENTINEL,
        FakeAgentLoginCloud, fake_agent_login_setup, fake_codex_authorized, fixture_lease_clock,
        fixture_run_view, step_execution_transition,
    };
    use crate::service::tree_run::RunActivity;
    use crate::telemetry::test_recorder;
    use useful_execution::{
        ExecutionObservation, ExecutionObserver, StepStateKind, WorkflowNodeRole,
    };
    use useful_runner_protocol::CloudFrame;

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
            run_reads: Arc::new(Semaphore::new(RUN_READ_PLACES)),
            reloads,
            agent_logins,
            timeouts: TEST_TIMEOUTS,
            runner_id: config.credential().runner_id().to_owned(),
            version: crate::telemetry::TEST_SERVICE_VERSION.to_owned(),
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

    /// The cause and result of each `agent_logins_set_up` the overview of `context` lists,
    /// the newest first, each once (REQ-RUNNERENROLL-138).
    async fn logged_setups(
        context: &ControlContext,
    ) -> Vec<(AgentLoginSetupCause, AgentLoginSetupResult)> {
        overview_in(context)
            .await
            .events
            .into_iter()
            .filter(|event| event.kind == OpenWord::Known(RunnerEventKind::AgentLoginsSetUp))
            .map(|event| {
                assert_eq!(event.count, 1);
                match (event.setup_cause, event.setup_result) {
                    (Some(OpenWord::Known(cause)), Some(OpenWord::Known(result))) => {
                        (cause, result)
                    }
                    facts => panic!("a setup event without its facts: {facts:?}"),
                }
            })
            .collect()
    }

    // REQ-GRANT-41, REQ-RUNNERENROLL-42, REQ-RUNNERENROLL-63, REQ-RUNNERENROLL-138: the
    // pass a sync made is logged as an `agent_logins_set_up` it caused, finished.
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

        assert_eq!(
            logged_setups(&context).await,
            [(AgentLoginSetupCause::Sync, AgentLoginSetupResult::Finished)]
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

    // REQ-GRANT-41, REQ-RUNNERENROLL-82, REQ-RUNNERENROLL-138: the pass a sync made while a
    // held run works is logged as one it caused that found the runner busy.
    #[tokio::test]
    async fn sync_while_a_held_run_is_working_answers_runner_busy_without_asking() {
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
        assert_eq!(
            logged_setups(&context).await,
            [(
                AgentLoginSetupCause::Sync,
                AgentLoginSetupResult::RunnerBusy
            )]
        );
        let Response::Status(status) =
            decode_response(&exchange_in(context, STATUS_REQUEST).await).unwrap()
        else {
            panic!("status request did not return a snapshot");
        };
        assert_eq!(status.agent_logins, None);
    }

    // REQ-GRANT-41, REQ-RUNNERENROLL-42
    #[tokio::test]
    async fn a_sync_a_working_run_interrupts_answers_runner_busy_and_status_shows_what_it_recorded()
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
        // The pass finds no held run working, asks for the Codex login and
        // installs it; a run is working before it asks for the next.
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
                &super::super::tree_run::HeldRuns::Nothing,
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
        status.backing_off(ConnectionFailure::CloudUnavailable, Duration::from_secs(1));
        status.connecting();
        let snapshot = status.snapshot(AssignmentCounts::default(), Vec::new());
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

    const OVERVIEW_REQUEST: &[u8] = b"{\"schemaVersion\":1,\"operation\":\"overview\"}\n";

    /// The overview `context` answers.
    async fn overview_in(context: &ControlContext) -> Overview {
        let response = exchange_in(context.clone(), OVERVIEW_REQUEST).await;
        let Ok(OverviewAnswer::Overview(overview)) = decode_overview_response(&response) else {
            panic!("overview request did not return an overview: {response:?}");
        };
        *overview
    }

    // REQ-RUNNERENROLL-133: an idle process answers who it is, its connection and credential,
    // and no assignment; while it backs off it gives the attempts since it was last connected
    // and the time to the next, and while it stops the time left of the stop's bound.
    #[tokio::test]
    async fn an_overview_answers_the_process_its_connection_and_its_stop() {
        let context = context(None);
        let overview = overview_in(&context).await;
        assert_eq!(overview.runner.runner_id, context.runner_id);
        assert_eq!(overview.runner.runner_id, "rnr_01k0z6r1w8f4jy2m7q9v3x5abd");
        assert_eq!(
            overview.runner.version,
            crate::telemetry::TEST_SERVICE_VERSION
        );
        assert_eq!(overview.runner.boot_id, "rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
        assert_eq!(
            overview.runner.process_state,
            OpenWord::Known(ProcessState::Running)
        );
        assert_eq!(overview.runner.stop_time_left_milliseconds, None);
        assert_eq!(
            overview.connection.state,
            OpenWord::Known(ConnectionState::Connecting)
        );
        assert_eq!(overview.connection.attempts_since_connected, None);
        assert_eq!(overview.connection.next_attempt_in_milliseconds, None);
        assert_eq!(
            overview.current_credential_id.as_deref(),
            Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abc")
        );
        assert_eq!(overview.pending_credential_id, None);
        assert_eq!(overview.assignment_counts, OverviewCounts::default());
        assert!(overview.assignments.is_empty());
        assert_eq!(overview.assignments_omitted, 0);
        assert_eq!(overview.unacknowledged_reports, 0);

        let delay = Duration::from_secs(7);
        context
            .status
            .backing_off(ConnectionFailure::Network, Duration::from_secs(3));
        context
            .status
            .backing_off(ConnectionFailure::CloudUnavailable, delay);
        let connection = overview_in(&context).await.connection;
        assert_eq!(
            connection.state,
            OpenWord::Known(ConnectionState::BackingOff)
        );
        assert_eq!(
            connection.last_failure,
            Some(OpenWord::Known(ConnectionFailure::CloudUnavailable))
        );
        assert_eq!(connection.attempts_since_connected, Some(2));
        let next = connection
            .next_attempt_in_milliseconds
            .expect("a backing-off connection gives the time to its next attempt");
        assert!(next <= u64::try_from(delay.as_millis()).unwrap(), "{next}");

        context.status.connected(None);
        let connection = overview_in(&context).await.connection;
        assert_eq!(
            connection.state,
            OpenWord::Known(ConnectionState::Connected)
        );
        assert_eq!(connection.attempts_since_connected, None);
        assert_eq!(connection.next_attempt_in_milliseconds, None);

        context.status.stopping();
        let runner = overview_in(&context).await.runner;
        assert_eq!(
            runner.process_state,
            OpenWord::Known(ProcessState::Stopping)
        );
        let left = runner
            .stop_time_left_milliseconds
            .expect("a stopping process gives the time left of its stop");
        assert!(left <= 315_000, "{left}");
    }

    // REQ-RUNNERENROLL-135: `overview` is asked for by name in a closed request, and asking
    // for it leaves the closed `status` answer as it was.
    #[tokio::test]
    async fn an_overview_request_is_closed_and_leaves_status_as_it_was() {
        let context = context(None);
        assert_eq!(
            decode_response(
                &exchange_in(
                    context.clone(),
                    b"{\"schemaVersion\":1,\"operation\":\"overview\",\"assignmentId\":\"asn_01k0z6r1w8f4jy2m7q9v3x5abc\"}\n",
                )
                .await
            )
            .unwrap(),
            Response::Error(ControlError::InvalidRequest)
        );
        let status = || async {
            let Response::Status(status) =
                decode_response(&exchange_in(context.clone(), STATUS_REQUEST).await).unwrap()
            else {
                panic!("status request did not return a snapshot");
            };
            StatusSnapshot {
                uptime_milliseconds: 0,
                ..status
            }
        };
        let before = status().await;
        overview_in(&context).await;
        assert_eq!(status().await, before);
    }

    /// `event` without its time, once its time is RFC 3339.
    fn untimed(
        event: &crate::control_protocol::OverviewEvent,
    ) -> crate::control_protocol::OverviewEvent {
        assert!(
            time::OffsetDateTime::parse(&event.at, &time::format_description::well_known::Rfc3339)
                .is_ok(),
            "{event:?}"
        );
        crate::control_protocol::OverviewEvent {
            at: String::new(),
            ..event.clone()
        }
    }

    /// An untimed event of `kind`, once, with no facts.
    fn untimed_event(kind: RunnerEventKind) -> crate::control_protocol::OverviewEvent {
        super::super::history::listed_event(Some(String::new()), kind, 1).unwrap()
    }

    // REQ-RUNNERENROLL-138: the overview lists what the live status was told as the runner's
    // events, newest first, a repeated failure once with its count and its last delay and a
    // second stop not at all; `status` answers as before.
    #[tokio::test]
    async fn an_overview_lists_the_runners_events_newest_first() {
        let context = context(None);
        let last_delay = Duration::from_millis(4_200);
        for delay in [Duration::from_secs(1), Duration::from_secs(2), last_delay] {
            context
                .status
                .backing_off(ConnectionFailure::Network, delay);
        }
        context.status.connected(None);
        context
            .status
            .promoted("rrc_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned());
        context.status.stopping();
        context.status.stopping();
        let events: Vec<_> = overview_in(&context)
            .await
            .events
            .iter()
            .map(untimed)
            .collect();
        assert_eq!(
            events,
            [
                untimed_event(RunnerEventKind::Stopping),
                crate::control_protocol::OverviewEvent {
                    credential_id: Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned()),
                    ..untimed_event(RunnerEventKind::CredentialReloaded)
                },
                untimed_event(RunnerEventKind::Connected),
                crate::control_protocol::OverviewEvent {
                    count: 3,
                    failure: Some(OpenWord::Known(ConnectionFailure::Network)),
                    next_attempt_in_milliseconds: Some(milliseconds(last_delay)),
                    ..untimed_event(RunnerEventKind::ConnectionFailed)
                },
            ]
        );

        context.status.authentication_failed();
        let events = overview_in(&context).await.events;
        assert_eq!(events.len(), 5);
        assert_eq!(
            untimed(&events[0]),
            crate::control_protocol::OverviewEvent {
                failure: Some(OpenWord::Known(ConnectionFailure::Authentication)),
                ..untimed_event(RunnerEventKind::ConnectionFailed)
            }
        );
        assert!(matches!(
            decode_response(&exchange_in(context.clone(), STATUS_REQUEST).await).unwrap(),
            Response::Status(_)
        ));
    }

    /// The protocol fixture's offer of a published tree.
    fn tree_assignment_offer() -> AssignmentOffer {
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
        AssignmentOffer {
            effect_id,
            assignment_id,
            run_id,
            project_id,
            attempt_id,
            attempt_number,
            execution_spec: *execution_spec,
            continuation: None,
        }
    }

    // REQ-RUNNERENROLL-136, INV-RUNNER-45: a run with a command in flight does not hold the
    // overview off; it is answered within the snapshot bound and lists the run.
    #[tokio::test]
    async fn an_overview_is_answered_while_a_run_has_a_command_in_flight() {
        let context = context(None);
        let offer = tree_assignment_offer();
        let activity = RunActivity::new();
        {
            let mut manager = context.assignments.lock().unwrap();
            install_running_renewal_fixture(&mut manager, offer.clone());
            install_run_activity(&mut manager, &offer.assignment_id, Arc::clone(&activity));
        }
        let in_flight = tokio::task::spawn_blocking(move || activity.start_command())
            .await
            .unwrap();

        let started = useful_support::monotonic_now();
        let overview = overview_in(&context).await;
        assert!(useful_support::elapsed(started) <= TEST_TIMEOUTS.snapshot);
        let [entry] = overview.assignments.as_slice() else {
            panic!("the overview lists the run alone: {overview:?}");
        };
        assert_eq!(entry.assignment_id, offer.assignment_id);
        assert_eq!(entry.run_id, offer.run_id);
        assert_eq!(entry.phase, OpenWord::Known(AssignmentPhase::Running));
        assert_eq!(entry.activity, Some(OpenWord::Known(HeldRunState::Working)));
        assert_eq!(overview.assignment_counts.running, 1);
        drop(in_flight);
    }

    /// An `assignment` request for `assignment_id` naming the child `run` and `step`.
    fn assignment_request(
        assignment_id: &str,
        run: Vec<OverviewRunElement>,
        step: Option<&str>,
    ) -> Vec<u8> {
        encode_assignment_request(&AssignmentQuery {
            assignment_id: assignment_id.to_owned(),
            run,
            step: step.map(str::to_owned),
        })
        .unwrap()
    }

    /// The assignment `context` answers `request`.
    async fn assignment_in(context: &ControlContext, request: &[u8]) -> AssignmentDetail {
        let response = exchange_in(context.clone(), request).await;
        let Ok(AssignmentAnswer::Assignment(detail)) = decode_assignment_response(&response) else {
            panic!("assignment request did not return an assignment: {response:?}");
        };
        *detail
    }

    /// `entry` without the time it has been held, which the fixture's monotonic clock moves
    /// between two reads.
    fn untimed_entry(mut entry: OverviewAssignment) -> OverviewAssignment {
        entry.held_milliseconds = 0;
        entry
    }

    /// A two-step `cmd` workflow: `plan`, and `build`, which depends on it.
    const TWO_STEP_WORKFLOW: &str = "schemaVersion: 1\nsteps:\n  plan:\n    kind: cmd\n    command:\n      argv: [\"scripts/plan\", \"--all\"]\n  build:\n    kind: cmd\n    dependsOn: [plan]\n    command:\n      argv: [\"true\"]\n";

    /// The run directory of `TWO_STEP_WORKFLOW` at `build.yaml`, admitted under `root` with a
    /// run record, as a tree run's is: both steps pending, and its lock released.
    fn pending_run_directory(root: &Path) -> PathBuf {
        let source = root.join("source");
        let execution = root.join("execution");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&execution).unwrap();
        fs::write(source.join("build.yaml"), TWO_STEP_WORKFLOW).unwrap();
        let workflow =
            useful_execution::resolve_workflow_file(&source, &source.join("build.yaml")).unwrap();
        let admitted = useful_execution::admit_workflow(
            workflow,
            useful_execution::ResolvedInputs::default(),
            useful_execution::ExecutionContext::new(
                execution,
                useful_execution::default_execution_policy_limits(1),
                useful_execution::EnvironmentSnapshot::default(),
                useful_execution::CancellationPolicy::new(
                    useful_execution::CancellationSource::new(),
                    Duration::from_secs(1),
                ),
            ),
        )
        .unwrap();
        let record = useful_execution::WorkflowRunRecord::new(
            None,
            &format!("sha256:{}", "0".repeat(64)),
            "build.yaml",
            "tester",
        );
        let directory = root.join("run");
        // The test and the reader share a process: closing `run.lock` in the reader would
        // release a lock the test still held.
        drop(
            useful_execution::InitialLocalRun::create_with_record(
                &directory,
                &admitted,
                Some(&record),
            )
            .unwrap(),
        );
        directory
    }

    /// A context holding the protocol fixture's tree offer as a running assignment whose
    /// activity published `directory` and `log`.
    fn running_context(
        directory: Option<PathBuf>,
        log: Option<PathBuf>,
    ) -> (ControlContext, AssignmentOffer, Arc<RunActivity>) {
        let context = context(None);
        let offer = tree_assignment_offer();
        let activity = RunActivity::new();
        if let Some(log) = log {
            activity.publish_log(log);
        }
        activity.publish_directory(directory);
        {
            let mut manager = context.assignments.lock().unwrap();
            install_running_renewal_fixture(&mut manager, offer.clone());
            install_run_activity(&mut manager, &offer.assignment_id, Arc::clone(&activity));
        }
        (context, offer, activity)
    }

    // REQ-RUNNERENROLL-144, REQ-RUNNERENROLL-63, REQ-RUNNERENROLL-82: an `assignment` request
    // answers the assignment's overview entry and the run its published run directory holds,
    // each step with what it runs; an assignment the runner does not hold is answered
    // `assignment_not_held`; a run directory that cannot be read, or a child path that names
    // no child run, leaves the run out of an answer that is still given; and a request with a
    // member it does not admit is `invalid_request`.
    #[tokio::test]
    async fn an_assignment_answers_its_entry_and_the_run_its_directory_holds() {
        let root = tempfile::tempdir().unwrap();
        let directory = pending_run_directory(root.path());
        let (context, offer, _activity) = running_context(Some(directory), None);
        let request = assignment_request(&offer.assignment_id, Vec::new(), None);

        let detail = assignment_in(&context, &request).await;
        let overview = overview_in(&context).await;
        let [listed] = overview.assignments.as_slice() else {
            panic!("the overview lists the run alone: {overview:?}");
        };
        assert_eq!(untimed_entry(detail.entry), untimed_entry(listed.clone()));
        let run = detail.run.expect("the run directory is read");
        assert_eq!(
            (
                run.workflow_path.as_str(),
                run.state.as_str(),
                run.path.len()
            ),
            ("build.yaml", "running", 0)
        );
        let steps: Vec<_> = run
            .steps
            .iter()
            .map(|step| {
                (
                    step.id.as_str(),
                    step.kind.as_str(),
                    step.state.as_str(),
                    step.command.as_ref().map(|command| command.argv.clone()),
                    step.agent.is_some() || step.workflow.is_some(),
                )
            })
            .collect();
        assert_eq!(
            steps,
            [
                (
                    "plan",
                    "cmd",
                    "pending",
                    Some(vec!["scripts/plan".to_owned(), "--all".to_owned()]),
                    false
                ),
                (
                    "build",
                    "cmd",
                    "pending",
                    Some(vec!["true".to_owned()]),
                    false
                ),
            ]
        );
        assert_eq!(run.steps[1].needs, ["plan"]);
        assert_eq!(run.steps_omitted, 0);
        // The activity published no run log.
        assert_eq!(detail.log, None);

        // A child path that names no child run leaves the run out.
        let missing_child = assignment_request(
            &offer.assignment_id,
            vec![OverviewRunElement {
                step: "build".to_owned(),
                item: None,
                ordinal: 1,
            }],
            None,
        );
        let detail = assignment_in(&context, &missing_child).await;
        assert_eq!(detail.entry.assignment_id, offer.assignment_id);
        assert_eq!(detail.run, None);

        // An assignment the runner does not hold.
        assert_eq!(
            exchange_in(
                context.clone(),
                &assignment_request("asn_01k0z6r1w8f4jy2m7q9v3x5zzz", Vec::new(), None),
            )
            .await,
            include_bytes!(
                "../../../../tests/fixtures/runner-control/v1/valid/assignment-not-held-response.json"
            )
        );

        // A request with a member it does not admit.
        let mut refused: serde_json::Value = serde_json::from_slice(&request).unwrap();
        refused["runId"] = offer.run_id.clone().into();
        let mut refused = serde_json::to_vec(&refused).unwrap();
        refused.push(b'\n');
        assert_eq!(
            decode_assignment_response(&exchange_in(context.clone(), &refused).await),
            Ok(AssignmentAnswer::Refused(ControlError::InvalidRequest))
        );

        // A published run directory that does not exist leaves the run out.
        let (context, offer, _activity) = running_context(Some(root.path().join("absent")), None);
        let detail = assignment_in(
            &context,
            &assignment_request(&offer.assignment_id, Vec::new(), None),
        )
        .await;
        assert_eq!(detail.entry.assignment_id, offer.assignment_id);
        assert_eq!((detail.run, detail.log), (None, None));
    }

    /// A context holding the protocol fixture's offer as a running assignment given the view
    /// of `TWO_STEP_WORKFLOW` whose step `plan` was observed `running`.
    async fn viewed_context() -> (ControlContext, AssignmentOffer) {
        let context = context(None);
        let offer = tree_assignment_offer();
        let view = fixture_run_view(TWO_STEP_WORKFLOW);
        let plan = ("plan", WorkflowNodeRole::Step);
        for (sequence, states) in [
            (1, (StepStateKind::Pending, StepStateKind::Starting)),
            (2, (StepStateKind::Starting, StepStateKind::Running)),
        ] {
            view.observe(ExecutionObservation::Transition(Box::new(
                step_execution_transition(sequence, plan, states, None, None, None),
            )))
            .await;
        }
        {
            let mut manager = context.assignments.lock().unwrap();
            install_running_renewal_fixture(&mut manager, offer.clone());
            install_run_view(&mut manager, &offer.assignment_id, view);
        }
        (context, offer)
    }

    /// A `step_records` log naming `step`, with no records.
    fn records_log(step: Option<&str>) -> AssignmentLog {
        AssignmentLog {
            source: AssignmentLogSource::StepRecords.into(),
            step: step.map(str::to_owned),
            invocation_id: None,
            stdout: None,
            stderr: None,
            earlier_invocations_omitted: 0,
            text: None,
            omitted_bytes: None,
            records: Vec::new(),
            omitted_records: 0,
        }
    }

    // REQ-RUNNERENROLL-144: a single-workflow assignment is answered from its run's view: the
    // entry lists its steps in flight, as the overview does, and the run has no path, one
    // attempt, and each step with its kind, its state and what it runs.
    // REQ-RUNNERENROLL-145: its log is always a `step_records` one, naming a step of the run it
    // was asked for, `source` alone with no step named; and a child run, which such a run has
    // none of, answers no run and that log.
    #[tokio::test]
    async fn a_single_workflow_assignment_is_answered_from_its_runs_view() {
        let (context, offer) = viewed_context().await;
        let running = vec![OverviewRunningStep {
            run: Vec::new(),
            run_attempt: 1,
            step: "plan".to_owned(),
            state: OpenWord::Known(LiveStepState::Running),
        }];

        let detail = assignment_in(
            &context,
            &assignment_request(&offer.assignment_id, Vec::new(), Some("plan")),
        )
        .await;
        assert_eq!(detail.entry.assignment_id, offer.assignment_id);
        assert_eq!(detail.entry.running_steps, running);
        let run = detail.run.expect("the view gives the run");
        assert_eq!(
            (
                run.path.len(),
                run.state.as_str(),
                run.attempts,
                run.attempt_number
            ),
            (0, "running", 1, 1)
        );
        let steps: Vec<_> = run
            .steps
            .iter()
            .map(|step| {
                (
                    step.id.as_str(),
                    step.kind.as_str(),
                    step.state.as_str(),
                    step.command.as_ref().map(|command| command.argv.clone()),
                )
            })
            .collect();
        assert_eq!(
            steps,
            [
                (
                    "plan",
                    "cmd",
                    "running",
                    Some(vec!["scripts/plan".to_owned(), "--all".to_owned()])
                ),
                ("build", "cmd", "pending", Some(vec!["true".to_owned()])),
            ]
        );
        assert_eq!(detail.log, Some(records_log(Some("plan"))));

        let response = exchange_in(context.clone(), OVERVIEW_REQUEST).await;
        let Ok(OverviewAnswer::Overview(overview)) = decode_overview_response(&response) else {
            panic!("overview request did not return an overview: {response:?}");
        };
        let [listed] = overview.assignments.as_slice() else {
            panic!("the overview lists the run alone: {overview:?}");
        };
        assert_eq!(listed.running_steps, running);
        let raw: serde_json::Value = serde_json::from_slice(&response).unwrap();
        let entries = raw["overview"]["assignments"].as_array().unwrap();
        assert!(entries.iter().all(|entry| entry.get("log").is_none()));

        let detail = assignment_in(
            &context,
            &assignment_request(&offer.assignment_id, Vec::new(), None),
        )
        .await;
        assert!(detail.run.is_some());
        assert_eq!(detail.log, Some(records_log(None)));

        let child = vec![OverviewRunElement {
            step: "build".to_owned(),
            item: None,
            ordinal: 1,
        }];
        let detail = assignment_in(
            &context,
            &assignment_request(&offer.assignment_id, child, Some("plan")),
        )
        .await;
        assert_eq!(detail.entry.assignment_id, offer.assignment_id);
        assert_eq!((detail.run, detail.log), (None, Some(records_log(None))));
    }

    /// A preparation that holds an offer in `preparing` until its sender is dropped.
    struct HeldPreparation(Mutex<std::sync::mpsc::Receiver<()>>);

    impl super::super::assignment::AssignmentRootPreparer for HeldPreparation {
        fn prepare(
            &self,
            _offer: &AssignmentOffer,
            _recorder: Option<Arc<crate::telemetry::Recorder>>,
        ) -> Result<
            crate::service::workspace::AssignmentRoot,
            crate::service::workspace::AssignmentRootCreationError,
        > {
            let _ = self.0.lock().unwrap().recv();
            Err(crate::service::workspace::AssignmentRootCreationError::Unavailable)
        }
    }

    // REQ-RUNNERENROLL-144: an assignment still being prepared is answered with its entry
    // alone.
    #[tokio::test]
    async fn an_assignment_being_prepared_answers_its_entry_alone() {
        let context = context(None);
        let offer = tree_assignment_offer();
        let (release, held) = std::sync::mpsc::channel();
        {
            let mut manager = context.assignments.lock().unwrap();
            crate::service::assignment::test_support::install_root_preparer(
                &mut manager,
                Arc::new(HeldPreparation(Mutex::new(held))),
            );
            manager.handle_offer(offer.clone()).unwrap();
        }

        let detail = assignment_in(
            &context,
            &assignment_request(&offer.assignment_id, Vec::new(), None),
        )
        .await;
        assert_eq!(detail.entry.assignment_id, offer.assignment_id);
        assert_eq!(
            detail.entry.phase,
            OpenWord::Known(AssignmentPhase::Preparing)
        );
        assert_eq!((detail.run, detail.log), (None, None));
        drop(release);
    }

    // REQ-RUNNERENROLL-145: with no step named, an assignment's log is the end of its run
    // log: the whole of a short one, and of a long one at most its window, begun after a
    // line feed, with the bytes before it counted; an overview gives no log.
    #[tokio::test]
    async fn an_assignment_gives_the_end_of_its_run_log_and_an_overview_none() {
        let root = tempfile::tempdir().unwrap();
        let log = root.path().join("tree-run.log");
        let short = "ran plan-marker\nran build\n{\"document\":1}\n";
        fs::write(&log, short).unwrap();
        let (context, offer, _activity) = running_context(None, Some(log.clone()));
        let request = assignment_request(&offer.assignment_id, Vec::new(), None);

        let detail = assignment_in(&context, &request).await;
        assert_eq!(detail.run, None);
        let given = detail.log.expect("the run log is given");
        assert_eq!(
            given.source,
            OpenWord::Known(crate::control_protocol::AssignmentLogSource::RunLog)
        );
        assert_eq!(
            (given.text.as_deref(), given.omitted_bytes),
            (Some(short), Some(0))
        );

        let overview = exchange_in(context.clone(), OVERVIEW_REQUEST).await;
        let value: serde_json::Value = serde_json::from_slice(&overview).unwrap();
        assert!(value["overview"]["assignments"][0].get("log").is_none());
        assert!(
            !String::from_utf8_lossy(&overview).contains("plan-marker"),
            "the overview holds the log's text"
        );

        let long: String = (0..40_000)
            .map(|line| format!("line {line:05}\n"))
            .collect();
        fs::write(&log, &long).unwrap();
        let given = assignment_in(&context, &request)
            .await
            .log
            .expect("the run log is given");
        let text = given.text.unwrap();
        let omitted = given.omitted_bytes.unwrap();
        assert!(text.len() <= 262_144);
        assert!(omitted > 0);
        assert_eq!(usize::try_from(omitted).unwrap() + text.len(), long.len());
        assert_eq!(
            long.as_bytes()[usize::try_from(omitted).unwrap() - 1],
            b'\n'
        );
        assert!(long.ends_with(&text));
    }

    // REQ-RUNNERENROLL-145: a named step that is still pending has no output to give, so the
    // answer carries its run beside the end of the run log.
    #[tokio::test]
    async fn a_pending_steps_assignment_gives_its_run_and_the_run_log() {
        let root = tempfile::tempdir().unwrap();
        let directory = pending_run_directory(root.path());
        let log = root.path().join("tree-run.log");
        fs::write(&log, "ran plan\n").unwrap();
        let (context, offer, _activity) = running_context(Some(directory), Some(log));

        let detail = assignment_in(
            &context,
            &assignment_request(&offer.assignment_id, Vec::new(), Some("plan")),
        )
        .await;
        assert_eq!(detail.run.map(|run| run.steps.len()), Some(2));
        let log = detail.log.expect("the run log is given");
        assert_eq!(
            log.source,
            OpenWord::Known(crate::control_protocol::AssignmentLogSource::RunLog)
        );
        assert_eq!(
            (log.step, log.text.as_deref(), log.omitted_bytes),
            (None, Some("ran plan\n"), Some(0))
        );
    }

    // REQ-RUNNERENROLL-145: a settled step's retained output is the answer's log: each
    // stream's recorded counts and its window as text from its first character boundary,
    // the bytes before it counted and bytes that are not UTF-8 given as U+FFFD, with the
    // earlier invocations counted; a settled step that retained none says so; and a read
    // that gave no output gives no step log, for the run log to stand in.
    #[test]
    fn a_settled_steps_retained_output_is_the_answers_log() {
        let retained = RunViewStepOutput::Retained(useful_execution::RunViewRetainedOutput {
            invocation_id: 7,
            // A window cut inside "é": the character's second byte begins it.
            stdout: RunViewStream {
                retained_bytes: 9_000,
                discarded_bytes: 12,
                window: b"\xA9 ready\xFF\n".to_vec(),
                omitted_bytes: 8_990,
            },
            // A file given whole begins at its first byte, whatever that byte is.
            stderr: RunViewStream {
                retained_bytes: 2,
                discarded_bytes: 0,
                window: b"\xA9!".to_vec(),
                omitted_bytes: 0,
            },
            earlier_invocations_omitted: 2,
        });
        let log = step_log(Some("plan".to_owned()), retained.clone()).unwrap();
        assert_eq!(
            log.source,
            OpenWord::Known(crate::control_protocol::AssignmentLogSource::Step)
        );
        assert_eq!(
            (
                log.step.as_deref(),
                log.invocation_id,
                log.earlier_invocations_omitted,
                &log.text,
                log.omitted_bytes,
            ),
            (Some("plan"), Some(7), 2, &None, None)
        );
        assert_eq!(
            log.stdout,
            Some(AssignmentLogStream {
                retained_bytes: 9_000,
                discarded_bytes: 12,
                omitted_bytes: 8_991,
                text: " ready\u{FFFD}\n".to_owned(),
            })
        );
        assert_eq!(
            log.stderr,
            Some(AssignmentLogStream {
                retained_bytes: 2,
                discarded_bytes: 0,
                omitted_bytes: 0,
                text: "\u{FFFD}!".to_owned(),
            })
        );

        let none = step_log(Some("plan".to_owned()), RunViewStepOutput::SettledWithNone).unwrap();
        assert_eq!(
            (
                none.step.as_deref(),
                none.invocation_id,
                &none.stdout,
                &none.stderr
            ),
            (Some("plan"), None, &None, &None)
        );
        assert_eq!(
            step_log(Some("plan".to_owned()), RunViewStepOutput::NotGiven),
            None
        );
        assert_eq!(step_log(None, retained), None);
    }

    // REQ-RUNNERENROLL-144: a run's wait is given as the overview gives one, its step by the
    // same rule: a fan-out element as its number, and the child path asked for echoed.
    #[test]
    fn a_read_runs_wait_is_given_as_the_overview_gives_one() {
        let path = vec![OverviewRunElement {
            step: "build".to_owned(),
            item: Some(2),
            ordinal: 1,
        }];
        let view = RunView {
            workflow_path: "build.yaml".to_owned(),
            state: "waiting".to_owned(),
            attempts: 1,
            outcome: None,
            attempt_number: 1,
            attempt_state: "waiting".to_owned(),
            wait: Some(useful_execution::RunViewWait {
                kind: "external".to_owned(),
                since: "2026-11-19T14:10:00Z".to_owned(),
                step: vec![
                    serde_json::json!("fix"),
                    serde_json::json!({"item": 2}),
                    serde_json::json!("queue"),
                ],
            }),
            steps: Vec::new(),
            steps_omitted: 3,
            output: RunViewStepOutput::NotGiven,
        };
        let (run, output) = assignment_run(view, path.clone());
        assert_eq!(output, RunViewStepOutput::NotGiven);
        assert_eq!((&run.path, run.steps_omitted), (&path, 3));
        assert_eq!(
            run.wait,
            Some(OverviewWait {
                kind: "external".to_owned(),
                since: Some("2026-11-19T14:10:00Z".to_owned()),
                step: vec![
                    crate::control_protocol::StepPathPart::Name("fix".to_owned()),
                    crate::control_protocol::StepPathPart::Number(2),
                    crate::control_protocol::StepPathPart::Name("queue".to_owned()),
                ],
            })
        );
    }

    /// An assignment answer of the overview fixture's first entry, `steps` copies of a step
    /// in its run, and `log`.
    fn sized_assignment(steps: usize, log: AssignmentLog) -> AssignmentDetail {
        let step = AssignmentStep {
            id: "build".to_owned(),
            role: "step".to_owned(),
            kind: "cmd".to_owned(),
            needs: Vec::new(),
            state: "succeeded".to_owned(),
            failure_policy: "required".to_owned(),
            started_at: None,
            duration_milliseconds: Some(4100),
            command: Some(AssignmentCommand {
                argv: vec!["scripts/build".to_owned()],
                cwd: None,
            }),
            agent: None,
            workflow: None,
            outputs: Vec::new(),
            child_runs: Vec::new(),
            child_runs_omitted: 0,
        };
        AssignmentDetail {
            entry: fixture_overview().assignments[0].clone(),
            run: Some(AssignmentRun {
                path: Vec::new(),
                workflow_path: "build.yaml".to_owned(),
                state: "running".to_owned(),
                attempts: 1,
                outcome: None,
                attempt_number: 1,
                attempt_state: "running".to_owned(),
                wait: None,
                steps: vec![step; steps],
                steps_omitted: 0,
            }),
            log: Some(log),
        }
    }

    fn run_log_of(text: &str, omitted_bytes: usize) -> AssignmentLog {
        AssignmentLog {
            source: crate::control_protocol::AssignmentLogSource::RunLog.into(),
            step: None,
            invocation_id: None,
            stdout: None,
            stderr: None,
            earlier_invocations_omitted: 0,
            text: Some(text.to_owned()),
            omitted_bytes: Some(u64::try_from(omitted_bytes).unwrap()),
            records: Vec::new(),
            omitted_records: 0,
        }
    }

    fn step_log_of(stdout: &str, stderr: &str) -> AssignmentLog {
        let stream = |text: &str| AssignmentLogStream {
            retained_bytes: u64::try_from(text.len()).unwrap(),
            discarded_bytes: 0,
            omitted_bytes: 0,
            text: text.to_owned(),
        };
        AssignmentLog {
            source: crate::control_protocol::AssignmentLogSource::Step.into(),
            step: Some("build".to_owned()),
            invocation_id: Some(1),
            stdout: Some(stream(stdout)),
            stderr: Some(stream(stderr)),
            earlier_invocations_omitted: 0,
            text: None,
            omitted_bytes: None,
            records: Vec::new(),
            omitted_records: 0,
        }
    }

    fn fitted(detail: AssignmentDetail, limit: usize) -> AssignmentDetail {
        let bytes = fit_assignment(detail, limit).unwrap();
        assert!(bytes.len() <= limit);
        let Ok(AssignmentAnswer::Assignment(detail)) = decode_assignment_response(&bytes) else {
            panic!("the fitted answer is an assignment");
        };
        *detail
    }

    // REQ-RUNNERENROLL-136: an assignment answer past its byte bound gives up the start of its
    // log first, the longer text halved at a character boundary and the bytes counted, then
    // its last steps, each counted, and then its run, never its entry.
    #[test]
    fn an_assignment_past_its_bound_gives_up_its_log_then_its_last_steps() {
        let text = "é".repeat(2_000);
        let detail = sized_assignment(20, run_log_of(&text, 0));
        let whole = encode_assignment_response(&detail).unwrap();
        assert_eq!(fitted(detail.clone(), whole.len()), detail);

        // The log alone is shortened while the steps fit.
        let emptied = run_log_of("", text.len());
        let without_text = encode_assignment_response(&sized_assignment(20, emptied.clone()))
            .unwrap()
            .len();
        let cut = fitted(detail.clone(), without_text + 100);
        assert_eq!(cut.run, detail.run);
        let log = cut.log.unwrap();
        let (kept, omitted) = (log.text.unwrap(), log.omitted_bytes.unwrap());
        assert!(!kept.is_empty() && kept.len() < text.len());
        assert!(text.ends_with(&kept));
        assert_eq!(usize::try_from(omitted).unwrap() + kept.len(), text.len());

        // Once the log is empty the last steps go.
        let cut = fitted(detail.clone(), without_text - 1);
        let log = cut.log.unwrap();
        assert_eq!(
            (log.text.as_deref(), log.omitted_bytes),
            (Some(""), Some(u64::try_from(text.len()).unwrap()))
        );
        let run = cut.run.unwrap();
        assert!(run.steps.len() < 20);
        assert_eq!(
            run.steps_omitted,
            20 - u64::try_from(run.steps.len()).unwrap()
        );
        assert_eq!(cut.entry, detail.entry);

        // Then the run, and never the entry.
        let entry_alone = encode_assignment_response(&AssignmentDetail {
            run: None,
            log: Some(emptied),
            ..detail.clone()
        })
        .unwrap()
        .len();
        let cut = fitted(detail.clone(), entry_alone);
        assert_eq!(cut.run, None);
        assert_eq!(cut.entry, detail.entry);
        assert_eq!(
            fit_assignment(detail, entry_alone - 1),
            Err(ProtocolFailure::Oversized)
        );

        // Of a step's two streams the longer is halved first.
        let detail = sized_assignment(1, step_log_of(&"o".repeat(1_000), "short"));
        let whole = encode_assignment_response(&detail).unwrap().len();
        let log = fitted(detail, whole - 1).log.unwrap();
        let (stdout, stderr) = (log.stdout.unwrap(), log.stderr.unwrap());
        assert_eq!((stdout.text.len(), stdout.omitted_bytes), (500, 500));
        assert_eq!((stderr.text.as_str(), stderr.omitted_bytes), ("short", 0));

        // Two full windows of control bytes, each written six times its size, pass the real
        // bound, and are cut to fit it.
        let window = "\u{1}".repeat(262_144);
        let detail = sized_assignment(1, step_log_of(&window, &window));
        assert!(matches!(
            encode_assignment_response(&detail),
            Err(ProtocolFailure::Oversized)
        ));
        let cut = fitted(detail.clone(), OVERVIEW_RESPONSE_LIMIT);
        assert_eq!(cut.run, detail.run);
        let log = cut.log.unwrap();
        for stream in [log.stdout.unwrap(), log.stderr.unwrap()] {
            assert!(window.ends_with(&stream.text));
            assert_eq!(
                usize::try_from(stream.omitted_bytes).unwrap() + stream.text.len(),
                window.len()
            );
        }
    }

    // REQ-RUNNERENROLL-136: an answer past its bound with many records gives up the older half
    // of them at a time, counting them and their bytes, until none is left, then its last
    // steps, and never its entry.
    #[test]
    fn an_assignment_past_its_bound_gives_up_its_older_records_then_its_last_steps() {
        let records: Vec<_> = (0..64)
            .map(|index| AssignmentLogRecord {
                source: if index % 2 == 0 { "stdout" } else { "stderr" }.to_owned(),
                text: format!("{index:04}{}", "x".repeat(96)),
                continuation: false,
            })
            .collect();
        let log = AssignmentLog {
            records: records.clone(),
            ..records_log(Some("build"))
        };
        let detail = sized_assignment(20, log);
        let without_records = encode_assignment_response(&sized_assignment(
            20,
            AssignmentLog {
                omitted_records: 64,
                omitted_bytes: Some(6_400),
                ..records_log(Some("build"))
            },
        ))
        .unwrap()
        .len();

        // The older records go while the steps fit, each counted with its bytes.
        let cut = fitted(detail.clone(), without_records + 1_000);
        assert_eq!(cut.run, detail.run);
        let log = cut.log.unwrap();
        assert!(!log.records.is_empty() && log.records.len() < records.len());
        assert!(records.ends_with(&log.records));
        let left_out = records.len() - log.records.len();
        assert_eq!(log.omitted_records, u64::try_from(left_out).unwrap());
        assert_eq!(
            log.omitted_bytes,
            Some(u64::try_from(left_out * 100).unwrap())
        );

        // Once none is left the last steps go, and the entry stays.
        let cut = fitted(detail.clone(), without_records - 1);
        let log = cut.log.unwrap();
        assert_eq!(
            (log.records.len(), log.omitted_records, log.omitted_bytes),
            (0, 64, Some(6_400))
        );
        let run = cut.run.unwrap();
        assert!(run.steps.len() < 20);
        assert_eq!(
            run.steps_omitted,
            20 - u64::try_from(run.steps.len()).unwrap()
        );
        assert_eq!(cut.entry, detail.entry);
    }

    // INV-RUNNER-45, REQ-RUNNERENROLL-136: with every place for a run directory read taken,
    // an `assignment` request is closed unanswered within the snapshot bound while an
    // `overview` is answered; and a run's command in flight does not hold the read off.
    #[tokio::test]
    async fn an_assignment_read_waits_on_no_run_and_keeps_to_its_places() {
        let root = tempfile::tempdir().unwrap();
        let directory = pending_run_directory(root.path());
        let (context, offer, activity) = running_context(Some(directory), None);
        let request = assignment_request(&offer.assignment_id, Vec::new(), None);

        let places = Arc::clone(&context.run_reads)
            .acquire_many_owned(u32::try_from(RUN_READ_PLACES).unwrap())
            .await
            .unwrap();
        let started = useful_support::monotonic_now();
        assert_eq!(exchange_in(context.clone(), &request).await, b"");
        assert!(useful_support::elapsed(started) <= TEST_TIMEOUTS.snapshot + TEST_TIMEOUTS.io);
        assert_eq!(overview_in(&context).await.assignments.len(), 1);
        drop(places);

        let in_flight = tokio::task::spawn_blocking(move || activity.start_command())
            .await
            .unwrap();
        let started = useful_support::monotonic_now();
        let detail = assignment_in(&context, &request).await;
        assert!(useful_support::elapsed(started) <= TEST_TIMEOUTS.snapshot);
        assert_eq!(
            detail.entry.activity,
            Some(OpenWord::Known(HeldRunState::Working))
        );
        assert!(detail.run.is_some());
        drop(in_flight);
    }

    // REQ-RUNNERENROLL-136: an overview past its byte bound is cut from its last entry, each
    // entry left out counted, and one whose runner alone passes the bound is not written.
    #[test]
    fn an_overview_past_its_bound_is_cut_from_its_last_entry() {
        // The events and the recent assignments are cut before any entry; this cut has none.
        let mut overview = Overview {
            events: Vec::new(),
            recent_assignments: Vec::new(),
            ..fixture_overview()
        };
        let mut third = overview.assignments[0].clone();
        third.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abz".to_owned();
        overview.assignments.push(third);
        let omitted = overview.assignments_omitted;
        let whole = encode_overview_response(&overview).unwrap();
        assert_eq!(
            fit_overview(overview.clone(), OVERVIEW_RESPONSE_LIMIT).unwrap(),
            whole
        );

        let cut = fit_overview(overview.clone(), whole.len() - 1).unwrap();
        assert!(cut.len() < whole.len());
        let Ok(OverviewAnswer::Overview(cut)) = decode_overview_response(&cut) else {
            panic!("the cut overview is an overview");
        };
        assert_eq!(cut.assignments, overview.assignments[..2]);
        assert_eq!(cut.assignments_omitted, omitted + 1);

        let runner_alone = encode_overview_response(&Overview {
            assignments: Vec::new(),
            assignments_omitted: omitted + 3,
            ..overview.clone()
        })
        .unwrap();
        assert_eq!(
            fit_overview(overview.clone(), runner_alone.len()).unwrap(),
            runner_alone
        );
        assert_eq!(
            fit_overview(overview, runner_alone.len() - 1),
            Err(ProtocolFailure::Oversized)
        );
    }

    /// The overview of `valid/overview-response.json`.
    fn fixture_overview() -> Overview {
        let Ok(OverviewAnswer::Overview(overview)) = decode_overview_response(include_bytes!(
            "../../../../tests/fixtures/runner-control/v1/valid/overview-response.json"
        )) else {
            panic!("the fixture is an overview");
        };
        *overview
    }

    // REQ-RUNNERENROLL-136: an overview past its byte bound leaves out its events from the
    // last, each counted, before any recent assignment or entry.
    #[test]
    fn an_overview_past_its_bound_is_cut_from_its_last_event_first() {
        let overview = fixture_overview();
        let events = overview.events.len();
        assert!(events > 1);
        let whole = encode_overview_response(&overview).unwrap();
        let Ok(OverviewAnswer::Overview(cut)) =
            decode_overview_response(&fit_overview(overview.clone(), whole.len() - 1).unwrap())
        else {
            panic!("the cut overview is an overview");
        };
        assert_eq!(cut.events, overview.events[..events - 1]);
        assert_eq!(cut.events_omitted, overview.events_omitted + 1);
        assert_eq!(cut.recent_assignments, overview.recent_assignments);
        assert_eq!(cut.assignments, overview.assignments);

        let without_events = encode_overview_response(&Overview {
            events: Vec::new(),
            events_omitted: overview.events_omitted + u64::try_from(events).unwrap(),
            ..overview.clone()
        })
        .unwrap();
        assert_eq!(
            fit_overview(overview.clone(), without_events.len()).unwrap(),
            without_events
        );
        let Ok(OverviewAnswer::Overview(cut)) = decode_overview_response(
            &fit_overview(overview.clone(), without_events.len() - 1).unwrap(),
        ) else {
            panic!("the cut overview is an overview");
        };
        assert_eq!(cut.events, []);
        assert_eq!(
            cut.recent_assignments,
            overview.recent_assignments[..overview.recent_assignments.len() - 1]
        );
        assert_eq!(
            cut.recent_assignments_omitted,
            overview.recent_assignments_omitted + 1
        );
        assert_eq!(cut.assignments, overview.assignments);
    }

    // REQ-RUNNERENROLL-136, REQ-RUNNERENROLL-137: an overview past its byte bound leaves out
    // its recent assignments from the last, each counted, before it leaves out any entry.
    #[test]
    fn an_overview_past_its_bound_is_cut_from_its_last_recent_assignment_first() {
        // The events are cut before any recent assignment; this cut has none.
        let overview = Overview {
            events: Vec::new(),
            ..fixture_overview()
        };
        assert_eq!(overview.recent_assignments.len(), 3);
        let (omitted, recent_omitted) = (
            overview.assignments_omitted,
            overview.recent_assignments_omitted,
        );
        let whole = encode_overview_response(&overview).unwrap();
        let Ok(OverviewAnswer::Overview(cut)) =
            decode_overview_response(&fit_overview(overview.clone(), whole.len() - 1).unwrap())
        else {
            panic!("the cut overview is an overview");
        };
        assert_eq!(cut.recent_assignments, overview.recent_assignments[..2]);
        assert_eq!(cut.recent_assignments_omitted, recent_omitted + 1);
        assert_eq!(cut.assignments, overview.assignments);
        assert_eq!(cut.assignments_omitted, omitted);

        let without_recent = encode_overview_response(&Overview {
            recent_assignments: Vec::new(),
            recent_assignments_omitted: recent_omitted + 3,
            ..overview.clone()
        })
        .unwrap();
        assert_eq!(
            fit_overview(overview.clone(), without_recent.len()).unwrap(),
            without_recent
        );
        let Ok(OverviewAnswer::Overview(cut)) = decode_overview_response(
            &fit_overview(overview.clone(), without_recent.len() - 1).unwrap(),
        ) else {
            panic!("the cut overview is an overview");
        };
        assert_eq!(cut.recent_assignments, []);
        assert_eq!(cut.recent_assignments_omitted, recent_omitted + 3);
        assert_eq!(cut.assignments, overview.assignments[..1]);
        assert_eq!(cut.assignments_omitted, omitted + 1);
    }

    // REQ-RUNNERENROLL-137: the overview gives the recent assignments the manager answered, in
    // its order.
    #[test]
    fn an_overview_gives_the_recent_assignments_in_the_order_the_manager_answers_them() {
        let status = LiveStatus::new(
            "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            "rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            None,
        );
        let recent = fixture_overview().recent_assignments;
        assert!(recent.len() > 1);
        let overview = status.overview(
            "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
            crate::telemetry::TEST_SERVICE_VERSION.to_owned(),
            AssignmentOverview {
                counts: OverviewCounts::default(),
                assignments: Vec::new(),
                assignments_omitted: 0,
                unacknowledged_reports: 0,
                recent_assignments: recent.clone(),
            },
        );
        assert_eq!(overview.recent_assignments, recent);
        assert_eq!(overview.recent_assignments_omitted, 0);
    }

    // REQ-RUNNERENROLL-133, INV-RUNNER-49: the snapshot and the overview give the reading
    // the monitor's handle holds, and neither gives one while the handle holds none or the
    // status was handed no handle.
    #[test]
    fn the_status_answers_give_the_published_disk_reading_and_none_without_one() {
        let answers = |status: &LiveStatus| {
            let snapshot = status.snapshot(AssignmentCounts::default(), Vec::new());
            let overview = status.overview(
                "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
                crate::telemetry::TEST_SERVICE_VERSION.to_owned(),
                AssignmentOverview {
                    counts: OverviewCounts::default(),
                    assignments: Vec::new(),
                    assignments_omitted: 0,
                    unacknowledged_reports: 0,
                    recent_assignments: Vec::new(),
                },
            );
            (snapshot.disk, overview.disk)
        };
        let status = || {
            LiveStatus::new(
                "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                "rrc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                None,
            )
        };

        let reading = super::super::disk::DiskReading {
            path: PathBuf::from("/var/lib/useful/runner/work"),
            available_bytes: 6_186_599,
            total_bytes: 423_054_278_656,
            minimum_free_bytes: 5_368_709_120,
            low: true,
            read_at: time::OffsetDateTime::parse(
                "2026-11-19T14:00:30Z",
                &time::format_description::well_known::Rfc3339,
            )
            .unwrap(),
        };
        let held = status().with_disk(super::super::disk::DiskReadingHandle::holding(reading));
        let (snapshot, overview) = answers(&held);
        assert_eq!(
            snapshot.as_deref(),
            Some(&crate::control_protocol::StatusDisk {
                path: "/var/lib/useful/runner/work".to_owned(),
                available_bytes: 6_186_599,
                total_bytes: 423_054_278_656,
                minimum_free_bytes: 5_368_709_120,
                low: true,
                read_at: "2026-11-19T14:00:30Z".to_owned(),
            })
        );
        assert_eq!(
            overview,
            Some(crate::control_protocol::OverviewDisk {
                path: "/var/lib/useful/runner/work".to_owned(),
                available_bytes: 6_186_599,
                total_bytes: 423_054_278_656,
                minimum_free_bytes: 5_368_709_120,
                low: true,
                read_at: "2026-11-19T14:00:30Z".to_owned(),
            })
        );

        let empty = status().with_disk(super::super::disk::DiskReadingHandle::new());
        assert_eq!(answers(&empty), (None, None));
        assert_eq!(answers(&status()), (None, None));
    }
}
