//! The `um` commands a tree run supervises (REQ-RUNNERENROLL-91): `um workflow
//! definition publish` into the run-scoped home, `um workflow run create` and `um
//! workflow run resume` under the assignment's lease, `um workflow run cancel` when
//! the assignment is cancelled, and `um workflow run event` for each trigger delivery, with
//! `um workflow run stop` and `lift` once the offer's deliveries are applied
//! (REQ-TRIGGER-136), and the `um workflow run` control command each Cloud control names
//! (REQ-RUNNERENROLL-90).
//! Each runs with the daemon's environment whole, with the agent logins offered to the
//! assignment (REQ-AGENTLOGIN-27, REQ-AGENTLOGIN-34), and `UM_HOME` set to the run-scoped
//! home; the runtime's own admission applies the root's passthrough to each step
//! (REQ-WFEXEC-22), so no step sees a runner-private name.
//!
//! Each runs in its own process group, killed when the runner abandons the command, and its
//! stderr, with the stdout the runner reads back, goes to the run's log, which keeps at most
//! `MAXIMUM_LOG_BYTES` and then ends with one `LOG_TRUNCATED` line. Every command but
//! `create`, `resume` and `retry`, which run the workflow, is short: it is abandoned once
//! `SHORT_COMMAND_BOUND` passes, an `event` or a control command also at the assignment's
//! cancellation, and the publication at the preparation's cancellation or deadline.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use rustix::process::{Pid, Signal, kill_process_group};
use tokio::io::AsyncReadExt as _;
use tokio::process::ChildStderr;

use super::super::Sleeper;
use super::super::run_inputs::PreparationDeadline;
use serde_json::Value;
use um_execution::{
    CONTROL_ANSWER, CONTROL_APPROVE, CONTROL_HAND_BACK, CONTROL_LIFT, CONTROL_RETRY,
    CancellationReason, CancellationSource, CaptureCancellation, ControlEvent, EnvironmentSnapshot,
    wait_identity,
};
use um_runner_protocol::{
    RunControlAction, RunControlAnswer, RunControlOutcome, TriggerDelivery, TriggerDeliveryAction,
    TriggerDeliveryApplication, TriggerDeliveryOutcome, TriggerIssueObservation,
};
use um_support::is_diagnostic_slug;

/// The longest a short `um` command (the publication, an `event`, a control command, the
/// start's stop and lift, or `cancel`) may run before the runner abandons it and kills its
/// process group. Each records one thing and returns, so a minute is ample.
pub(super) const SHORT_COMMAND_BOUND: Duration = Duration::from_secs(60);
/// The most command output the run's log keeps.
pub(super) const MAXIMUM_LOG_BYTES: u64 = 64 * 1024 * 1024;
/// The line that ends a log that reached `MAXIMUM_LOG_BYTES`; later output is dropped.
pub(super) const LOG_TRUNCATED: &[u8] =
    b"\n[tree-run.log reached 64 MiB; later output is dropped]\n";
/// How long a command's stderr is still copied to the log after the command ends, for a
/// descendant that holds it open.
const LOG_DRAIN: Duration = Duration::from_secs(1);
/// The most of a command's stderr copied to the log in one write.
const LOG_CHUNK_BYTES: usize = 64 * 1024;
/// The most of a supervised command's stdout read back for its last document.
const MAXIMUM_CAPTURED_DOCUMENT_BYTES: u64 = 16 * 1024 * 1024;
const PUBLISH_OUTPUT_LIMIT: u64 = 1024 * 1024;
const RUN_DIRECTORY_POLL: Duration = Duration::from_millis(50);
/// The code an application reports when `um workflow run event` could not be run or
/// answered with no document it could report.
const EVENT_COMMAND_FAILED: &str = "event_command_failed";
/// The reason the start's own stop is recorded with when the runner puts it back.
const START_STOP_REASON: &str = "The start trigger's stopBefore";
/// The reason a Cloud `stop` that gives none is recorded with; the local command needs one.
const CLOUD_STOP_REASON: &str = "Stopped by a Cloud control";
/// The code a control's application reports when its command could not be run or answered
/// with no document it could report.
const CONTROL_COMMAND_FAILED: &str = "control_command_failed";
/// The cause a tree run ends with when the start's own stop or the runner's lift was not
/// recorded, and the code the run's log names beside its class.
pub(in crate::service) const PENDING_STOP_UNLIFTED: &str = "tree_run_pending_stop_unlifted";
/// The outcome a `retry` that printed no outcome reports once its control is recorded.
const RECORDED: &str = "recorded";
/// The most of a control command's stdout read back for its last document.
const CONTROL_OUTPUT_LIMIT: u64 = 1024 * 1024;
/// The longest `actionOutcome` the protocol carries, in characters.
const MAXIMUM_ACTION_OUTCOME_CHARACTERS: usize = 64;
/// The longest provider value an observation reports, as the protocol bounds it.
const MAXIMUM_OBSERVED_ID_BYTES: usize = 255;
const MAXIMUM_OBSERVED_TIME_BYTES: usize = 64;

/// A `um` command line in the run-scoped home, its output appended to the run's log, each
/// short one abandoned once `bound` passes on `sleeper`.
#[derive(Clone)]
pub(in crate::service) struct UmCommand {
    pub(in crate::service) executable: PathBuf,
    pub(in crate::service) environment: EnvironmentSnapshot,
    pub(in crate::service) home: PathBuf,
    pub(in crate::service) log: RunLog,
    pub(in crate::service) sleeper: Arc<dyn Sleeper>,
    pub(in crate::service) bound: Duration,
}

impl UmCommand {
    /// Spawns `arguments` in its own process group, stdout to `stdout` and stderr copied to
    /// the log. The group is killed if the child is dropped before it is reaped.
    fn spawn(
        &self,
        arguments: impl IntoIterator<Item = impl AsRef<OsStr>>,
        stdout: Stdio,
    ) -> io::Result<(SupervisedChild, LogPump)> {
        // The log exists once a command has run, whether or not it wrote anything.
        drop(self.log.open()?);
        let mut command = tokio::process::Command::new(&self.executable);
        command
            .args(arguments)
            .env_clear()
            .envs(self.environment.variables())
            .env("UM_HOME", &self.home)
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let pump = LogPump::start(child.stderr.take(), self.log.clone());
        let group = child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .and_then(Pid::from_raw);
        Ok((SupervisedChild { child, group }, pump))
    }
}

/// The run's log, `tree-run.log`: each command's stderr as it writes it and the stdout the
/// runner reads back, kept up to `MAXIMUM_LOG_BYTES` and then ended with one `LOG_TRUNCATED`
/// line, after which output is dropped.
#[derive(Clone)]
pub(in crate::service) struct RunLog {
    path: PathBuf,
    /// Serializes appends, so commands running beside each other end the log once.
    gate: Arc<Mutex<()>>,
    /// Set once the log has ended, so later output is dropped without opening it.
    ended: Arc<AtomicBool>,
}

impl RunLog {
    pub(in crate::service) fn new(path: PathBuf) -> Self {
        Self {
            path,
            gate: Arc::default(),
            ended: Arc::default(),
        }
    }

    fn open(&self) -> io::Result<File> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&self.path)
    }

    fn has_ended(&self) -> bool {
        self.ended.load(Ordering::Acquire)
    }

    /// Appends `bytes` while the log is under its cap, ending it with `LOG_TRUNCATED` at the
    /// first byte past the cap; on the calling blocking worker.
    fn append(&self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() || self.has_ended() {
            return Ok(());
        }
        let _gate = self.gate.lock().unwrap_or_else(PoisonError::into_inner);
        let mut file = self.open()?;
        let length = file.metadata()?.len();
        // Past the cap, the log already ends with its truncation line.
        if length > MAXIMUM_LOG_BYTES {
            self.ended.store(true, Ordering::Release);
            return Ok(());
        }
        let room = usize::try_from(MAXIMUM_LOG_BYTES - length).unwrap_or(usize::MAX);
        match bytes.get(..room) {
            Some(kept) if kept.len() < bytes.len() => {
                file.write_all(kept)?;
                file.write_all(LOG_TRUNCATED)?;
                self.ended.store(true, Ordering::Release);
                Ok(())
            }
            _ => file.write_all(bytes),
        }
    }
}

/// The task copying a command's stderr into the run's log until it closes, reading on past
/// the cap so the command never blocks on a full pipe; aborted when dropped.
struct LogPump(tokio::task::JoinHandle<()>);

impl LogPump {
    fn start(stderr: Option<ChildStderr>, log: RunLog) -> Self {
        Self(tokio::spawn(async move {
            let Some(mut stderr) = stderr else {
                return;
            };
            let mut buffer = vec![0; LOG_CHUNK_BYTES];
            loop {
                let read = match stderr.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => read,
                };
                if log.has_ended() {
                    continue;
                }
                let (chunk, log) = (buffer[..read].to_vec(), log.clone());
                // A log that cannot be written loses this output; the command goes on.
                let _ = tokio::task::spawn_blocking(move || log.append(&chunk)).await;
            }
        }))
    }

    /// Waits for the copy to reach the end of what the ended command wrote, at most
    /// `LOG_DRAIN`: a descendant can hold its stderr open past it.
    async fn drain(mut self, sleeper: &dyn Sleeper) {
        tokio::select! {
            _ = &mut self.0 => {}
            () = sleeper.sleep(LOG_DRAIN) => {}
        }
    }
}

impl Drop for LogPump {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Where a short command's stdout goes: a documents file, or a pipe of which at most this
/// many bytes are read back.
enum ShortOutput {
    File(File),
    Captured(u64),
}

/// How a short command ended.
enum ShortEnd {
    /// It exited with `status`, having printed `captured` to a captured stdout.
    Exited {
        status: ExitStatus,
        captured: Vec<u8>,
    },
    /// Its bound passed first; its process group was killed.
    TimedOut,
    /// The cancellation it ran under came first; its process group was killed.
    Abandoned,
}

/// Runs one short `um` command line in its own process group until it exits, `bound`
/// passes or `abandon` completes; in the last two cases its group is killed.
async fn run_short(
    um: &UmCommand,
    arguments: Vec<OsString>,
    output: ShortOutput,
    bound: Duration,
    abandon: impl Future<Output = ()>,
) -> io::Result<ShortEnd> {
    let (stdout, limit) = match output {
        ShortOutput::File(file) => (Stdio::from(file), None),
        ShortOutput::Captured(limit) => (Stdio::piped(), Some(limit)),
    };
    let (mut child, pump) = um.spawn(&arguments, stdout)?;
    let exited = async {
        let mut captured = Vec::new();
        // The pipe closes once the limit is read, so a command printing more cannot block
        // on it.
        if let (Some(limit), Some(stdout)) = (limit, child.child.stdout.take()) {
            stdout.take(limit).read_to_end(&mut captured).await?;
        }
        let status = child.child.wait().await?;
        Ok::<_, io::Error>(ShortEnd::Exited { status, captured })
    };
    let end = tokio::select! {
        biased;
        exited = exited => exited?,
        () = abandon => ShortEnd::Abandoned,
        () = um.sleeper.sleep(bound) => ShortEnd::TimedOut,
    };
    if matches!(end, ShortEnd::Exited { .. }) {
        pump.drain(um.sleeper.as_ref()).await;
    } else {
        child.kill_group();
    }
    Ok(end)
}

/// Completes once `cancellation` is requested, for any reason.
async fn cancelled(cancellation: &CancellationSource) {
    cancellation.wait_for_cancellation().await;
}

/// What `um workflow definition publish --json` reports.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::service) struct Published {
    /// The definition the run-scoped home created for the tree.
    pub(in crate::service) definition: String,
    /// The revision the local publish recorded.
    pub(in crate::service) revision: u64,
    /// The tree digest the local publish computed, `sha256:<hex>`.
    pub(in crate::service) digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum PublishFailure {
    /// The `um` executable could not be started or its output read.
    Launch,
    /// The command refused the tree or printed no publication.
    Refused,
    /// The command outlived its bound or the preparation's deadline, or the preparation was
    /// cancelled; its process group was killed.
    Abandoned,
}

/// Publishes the downloaded tree with the spec's bindings snapshot into the run-scoped
/// home (REQ-WFDEF-22), on the calling blocking worker, abandoning it at the short-command
/// bound, the preparation's deadline or its cancellation.
pub(in crate::service) fn publish(
    um: &UmCommand,
    tree: &Path,
    bindings: &Path,
    cancellation: &CaptureCancellation,
    deadline: PreparationDeadline,
) -> Result<Published, PublishFailure> {
    let bound = deadline
        .remaining()
        .ok_or(PublishFailure::Abandoned)?
        .min(um.bound);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| PublishFailure::Launch)?;
    let arguments = vec![
        OsString::from("workflow"),
        "definition".into(),
        "publish".into(),
        tree.into(),
        "--bindings".into(),
        bindings.into(),
        "--json".into(),
    ];
    let end = runtime
        .block_on(run_short(
            um,
            arguments,
            ShortOutput::Captured(PUBLISH_OUTPUT_LIMIT),
            bound,
            cancellation.cancelled(),
        ))
        .map_err(|_| PublishFailure::Launch)?;
    let ShortEnd::Exited {
        status,
        captured: stdout,
    } = end
    else {
        return Err(PublishFailure::Abandoned);
    };
    if !status.success() {
        return Err(PublishFailure::Refused);
    }
    let document: Value = serde_json::from_slice(&stdout).map_err(|_| PublishFailure::Refused)?;
    match (
        document["definition"]["id"].as_str(),
        document["revision"]["n"].as_u64(),
        document["revision"]["tree"].as_str(),
    ) {
        (Some(definition), Some(revision), Some(digest)) => Ok(Published {
            definition: definition.to_owned(),
            revision,
            digest: digest.to_owned(),
        }),
        _ => Err(PublishFailure::Refused),
    }
}

/// How a supervised `create` or `resume` ended.
#[derive(Debug)]
pub(in crate::service) struct CommandExit {
    /// The last document the command's `--json` printed, when it printed one.
    pub(in crate::service) document: Option<Value>,
    /// The cancellation the runner applied to the run, if any.
    pub(in crate::service) cancellation: Option<CancellationReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum CommandFailure {
    Launch,
    Wait,
}

impl CommandFailure {
    pub(in crate::service) const fn cause(self) -> &'static str {
        match self {
            Self::Launch => "tree_run_launch_failed",
            Self::Wait => "tree_run_wait_failed",
        }
    }
}

/// A supervised process, its group killed if the supervision is dropped first, as when
/// lease supervision abandons the attempt at its containment deadline.
struct SupervisedChild {
    child: tokio::process::Child,
    group: Option<Pid>,
}

impl SupervisedChild {
    fn kill_group(&mut self) {
        // The leader is unreaped while `child` has an id, so the group id is still its.
        if self.child.id().is_some()
            && let Some(group) = self.group
        {
            let _ = kill_process_group(group, Signal::KILL);
        }
    }
}

impl Drop for SupervisedChild {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            self.kill_group();
        }
    }
}

/// `um workflow run resume RUN --json`: continues a resolved wait, an abandoned attempt
/// or an unchained `continued` edge, and reports a held wait unchanged (REQ-CONTINUE-74
/// to REQ-CONTINUE-78).
pub(in crate::service) fn resume_arguments(run_directory: &Path) -> Vec<OsString> {
    resume_from_arguments(run_directory, &[])
}

/// `um workflow run resume RUN --from STEP... --json`, one `--from` per step in order:
/// starts the next attempt of a finished run from the named steps and inherits the rest
/// (REQ-CONTINUE-1); with no step, `resume_arguments`.
pub(in crate::service) fn resume_from_arguments(
    run_directory: &Path,
    steps: &[String],
) -> Vec<OsString> {
    let mut arguments: Vec<OsString> = vec![
        "workflow".into(),
        "run".into(),
        "resume".into(),
        run_directory.into(),
    ];
    for step in steps {
        arguments.extend(["--from".into(), step.into()]);
    }
    arguments.push("--json".into());
    arguments
}

/// Whether the runner stops a run for `reason` without ending its record: its shutdown
/// and lease loss interrupt the attempt and leave the record as it stands, for a later
/// attempt to continue (REQ-RUNNERENROLL-73, REQ-RUNNERENROLL-89), while a cancellation
/// of the assignment ends the run with `um workflow run cancel`.
pub(in crate::service) const fn leaves_record(reason: CancellationReason) -> bool {
    matches!(
        reason,
        CancellationReason::RunnerShutdown | CancellationReason::ExecutionLeaseExpired
    )
}

/// Runs a `um workflow run create` or `resume` command line in its own process group
/// until it exits, its stdout in `documents`. A requested cancellation runs `um workflow
/// run cancel` for the run, which signals the process (REQ-LOCALRUN-186); a force abort,
/// or a cancellation the process has not honored within `grace`, kills its group, after
/// which `cancel` ends the abandoned record. The runner's shutdown and lease loss kill the
/// group and leave the record as the process left it (`leaves_record`).
pub(in crate::service) async fn supervise(
    um: &UmCommand,
    arguments: Vec<OsString>,
    runs: PathBuf,
    documents: &Path,
    cancellation: &CancellationSource,
    grace: Duration,
) -> Result<CommandExit, CommandFailure> {
    // The documents go to a file, not a pipe: a descendant that inherited a pipe would
    // hold it open past the process's exit.
    let stdout = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(documents)
        .map_err(|_| CommandFailure::Launch)?;
    let (mut child, pump) = um
        .spawn(&arguments, Stdio::from(stdout))
        .map_err(|_| CommandFailure::Launch)?;
    let sleeper = &um.sleeper;

    let mut applied = None;
    let mut canceller = None;
    let mut kill_at = None;
    let mut killed = false;
    let status = loop {
        let grace_remaining = kill_at
            .map(|deadline: std::time::Instant| deadline.saturating_duration_since(sleeper.now()));
        tokio::select! {
            biased;
            status = child.child.wait() => break status,
            () = cancellation.wait_for_force_abort(), if !killed => {
                applied = Some(CancellationReason::ForceAbort);
                child.kill_group();
                killed = true;
            }
            reason = cancellation.wait_for_cancellation(), if applied.is_none() => {
                applied = Some(reason);
                if leaves_record(reason) {
                    child.kill_group();
                    killed = true;
                } else {
                    kill_at = Some(sleeper.now() + grace);
                    canceller = Some(tokio::spawn(cancel_when_created(um.clone(), runs.clone())));
                }
            }
            () = sleep_or_pending(sleeper.as_ref(), grace_remaining), if !killed => {
                child.kill_group();
                killed = true;
            }
        }
    };
    let status = status.map_err(|_| CommandFailure::Wait);
    pump.drain(sleeper.as_ref()).await;
    if let Some(canceller) = canceller {
        // The process is gone: a `cancel` still waiting for its run directory waits for
        // nothing, and one that signalled has done its part; aborting it kills its group.
        canceller.abort();
        let _ = canceller.await;
    }
    if applied.is_some_and(|reason| !leaves_record(reason)) {
        // A process that exited before honoring the cancellation, or was killed, leaves its
        // record `running`; `cancel` ends that abandoned record `canceled` and refuses an
        // exited one.
        let _ = cancel_run(um, &runs).await;
    }
    let documents = documents.to_owned();
    let log = um.log.clone();
    let document = tokio::task::spawn_blocking(move || last_document(&documents, &log))
        .await
        .ok()
        .flatten();
    status?;
    Ok(CommandExit {
        document,
        cancellation: applied,
    })
}

/// Appends the command's stdout to the log and returns the last JSON document it printed.
fn last_document(documents: &Path, log: &RunLog) -> Option<Value> {
    let mut captured = Vec::new();
    File::open(documents)
        .ok()?
        .take(MAXIMUM_CAPTURED_DOCUMENT_BYTES)
        .read_to_end(&mut captured)
        .ok()?;
    logged_last_document(&captured, log)
}

/// Appends `captured` stdout to the log and returns the last JSON document it holds.
fn logged_last_document(captured: &[u8], log: &RunLog) -> Option<Value> {
    // A log that cannot be written loses this output; the document is still read.
    let _ = log.append(captured);
    serde_json::Deserializer::from_slice(captured)
        .into_iter::<Value>()
        .map_while(Result::ok)
        .last()
}

/// The only entry of `directory`, `None` unless it holds exactly one: the run-scoped
/// home's only run once `create` made it, or its only definition.
pub(in crate::service) fn only_entry(directory: &Path) -> Option<PathBuf> {
    let mut entries = fs::read_dir(directory).ok()?.filter_map(Result::ok);
    let only = entries.next()?;
    entries.next().is_none().then(|| only.path())
}

async fn sleep_or_pending(sleeper: &dyn Sleeper, duration: Option<Duration>) {
    match duration {
        Some(duration) => sleeper.sleep(duration).await,
        None => std::future::pending().await,
    }
}

async fn cancel_when_created(um: UmCommand, runs: PathBuf) {
    loop {
        let found = {
            let runs = runs.clone();
            tokio::task::spawn_blocking(move || only_entry(&runs))
                .await
                .ok()
                .flatten()
        };
        if found.is_some() {
            let _ = cancel_run(&um, &runs).await;
            return;
        }
        um.sleeper.sleep(RUN_DIRECTORY_POLL).await;
    }
}

/// Runs `um workflow run cancel RUN --json` for the home's run, output to the log. It is the
/// cancellation's own command, so only its bound abandons it.
pub(in crate::service) async fn cancel_run(um: &UmCommand, runs: &Path) -> io::Result<()> {
    let directory = {
        let runs = runs.to_owned();
        tokio::task::spawn_blocking(move || only_entry(&runs))
            .await
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?
    };
    let arguments = vec![
        OsString::from("workflow"),
        "run".into(),
        "cancel".into(),
        directory.into(),
        "--json".into(),
    ];
    let end = run_short(
        um,
        arguments,
        ShortOutput::Captured(CONTROL_OUTPUT_LIMIT),
        um.bound,
        std::future::pending(),
    )
    .await?;
    if let ShortEnd::Exited { captured, .. } = end {
        let log = um.log.clone();
        tokio::task::spawn_blocking(move || log.append(&captured))
            .await
            .map_err(io::Error::other)??;
    }
    Ok(())
}

/// Applies `deliveries` in order to the run at `run_directory`, each with `um workflow run
/// event RUN --event FILE --claim FILE --json` over its event and claim written under the
/// home's `deliveries/<deliveryId>/` (REQ-TRIGGER-136). The command's `applied` document
/// gives the action and its `refused` one the code, each with the fresh read it reports; a
/// command that cannot be launched, prints neither or outlives its bound is reported refused
/// `event_command_failed`, the run's log naming its `Unanswered` class. The assignment's
/// `cancellation` abandons the command running and those after it, which are reported as
/// nothing.
pub(in crate::service) async fn apply_deliveries(
    um: &UmCommand,
    run_directory: PathBuf,
    deliveries: Vec<TriggerDelivery>,
    cancellation: &CancellationSource,
) -> Vec<TriggerDeliveryApplication> {
    let mut applications = Vec::with_capacity(deliveries.len());
    for delivery in deliveries {
        let document = match run_event(um, &run_directory, &delivery, cancellation).await {
            Answered::Abandoned => break,
            Answered::Ended { document, .. } => document,
        };
        let (outcome, observation) = match document.as_ref().ok().and_then(application) {
            Some(reported) => reported,
            None => {
                log_failure(&um.log, EVENT_COMMAND_FAILED, unanswered(&document)).await;
                let code = EVENT_COMMAND_FAILED.to_owned();
                (TriggerDeliveryOutcome::Refused { code }, None)
            }
        };
        applications.push(TriggerDeliveryApplication {
            delivery_id: delivery.delivery_id,
            outcome,
            observation,
        });
    }
    applications
}

/// Why a short command left no document to report: the closed class the run's log names
/// beside the code its application is refused with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Unanswered {
    /// It could not be launched, or what it printed could not be read back, failing with
    /// an I/O error of this kind.
    Launch(io::ErrorKind),
    /// It outlived its bound; its process group was killed.
    Timeout,
    /// It exited and printed no document the runner could report.
    NoDocument,
}

impl Unanswered {
    const fn class(self) -> &'static str {
        match self {
            Self::Launch(_) => "launch_error",
            Self::Timeout => "timeout",
            Self::NoDocument => "no_document",
        }
    }
}

/// The class of a command that ended with `document`, which left nothing it could report: a
/// document it printed was not one.
fn unanswered(document: &Result<Value, Unanswered>) -> Unanswered {
    document
        .as_ref()
        .err()
        .copied()
        .unwrap_or(Unanswered::NoDocument)
}

/// The log's fields for this class: `class=CLASS`, and for a launch error ` kind=KIND`, the
/// kind of the I/O error it failed with.
impl fmt::Display for Unanswered {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "class={}", self.class())?;
        match self {
            Self::Launch(kind) => write!(formatter, " kind={kind:?}"),
            Self::Timeout | Self::NoDocument => Ok(()),
        }
    }
}

/// Appends the line naming why a command failed with `code`, `[tree-run: CODE FIELDS]`,
/// `fields` its class as `Unanswered` writes it, to the run's log on a blocking worker;
/// never the command's arguments or output, which the log already holds.
async fn log_failure(log: &RunLog, code: &str, fields: impl fmt::Display) {
    let (log, line) = (log.clone(), format!("\n[tree-run: {code} {fields}]\n"));
    // A log that cannot be written loses this line; the application is still reported.
    let _ = tokio::task::spawn_blocking(move || log.append(line.as_bytes())).await;
}

/// What a short command left to report.
enum Answered {
    /// It exited or outlived its bound: `success` when it exited 0, and the last JSON
    /// document it printed or why there is none.
    Ended {
        success: bool,
        document: Result<Value, Unanswered>,
    },
    /// The assignment's cancellation abandoned it.
    Abandoned,
}

impl Answered {
    /// A command that could not be launched or whose output could not be read back, failing
    /// with an I/O error of `kind`.
    const fn launch(kind: io::ErrorKind) -> Self {
        Self::Ended {
            success: false,
            document: Err(Unanswered::Launch(kind)),
        }
    }

    /// How a command that ended as `end`, its last document `document`, is reported.
    fn of(end: &ShortEnd, document: Option<Value>) -> Self {
        let (success, document) = match end {
            ShortEnd::Exited { status, .. } => {
                (status.success(), document.ok_or(Unanswered::NoDocument))
            }
            ShortEnd::TimedOut => (false, Err(Unanswered::Timeout)),
            ShortEnd::Abandoned => return Self::Abandoned,
        };
        Self::Ended { success, document }
    }
}

/// Runs `um workflow run event` for `delivery`, `Answered::launch` with the I/O error's kind
/// when its files could not be written, it could not be launched or its output could not be
/// read back.
async fn run_event(
    um: &UmCommand,
    run_directory: &Path,
    delivery: &TriggerDelivery,
    cancellation: &CancellationSource,
) -> Answered {
    event_command(um, run_directory, delivery, cancellation)
        .await
        .unwrap_or_else(|error| Answered::launch(error.kind()))
}

async fn event_command(
    um: &UmCommand,
    run_directory: &Path,
    delivery: &TriggerDelivery,
    cancellation: &CancellationSource,
) -> io::Result<Answered> {
    let directory = um.home.join("deliveries").join(&delivery.delivery_id);
    let (event, claim, output) = (
        directory.join("event.json"),
        directory.join("claim.json"),
        directory.join("event-output.json"),
    );
    let files = [
        (event.clone(), serde_json::to_vec(&delivery.event)?),
        (claim.clone(), serde_json::to_vec(&delivery.claim)?),
    ];
    let documents = output.clone();
    let stdout = tokio::task::spawn_blocking(move || {
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&directory)?;
        files
            .iter()
            .try_for_each(|(path, bytes)| write_synced(path, bytes))?;
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&documents)
    })
    .await
    .map_err(io::Error::other)??;
    let arguments = vec![
        OsString::from("workflow"),
        "run".into(),
        "event".into(),
        run_directory.into(),
        "--event".into(),
        event.into(),
        "--claim".into(),
        claim.into(),
        "--json".into(),
    ];
    let end = run_short(
        um,
        arguments,
        ShortOutput::File(stdout),
        um.bound,
        cancelled(cancellation),
    )
    .await?;
    let log = um.log.clone();
    let document = tokio::task::spawn_blocking(move || last_document(&output, &log))
        .await
        .map_err(io::Error::other)?;
    Ok(Answered::of(&end, document))
}

/// The outcome and observation `document`, the event command's last JSON document, reports;
/// `None` when it reports neither an action nor a refusal. An `applied` one reports the
/// action its `actionOutcome` says was taken (REQ-LOCALRUN-208): `canceled` a cancel, and
/// `withdrawn`, or `recorded` for a live run's withdrawal that its process ends `withdrawn`
/// (REQ-LOCALRUN-186), a withdrawal; never the `action` alone.
fn application(
    document: &Value,
) -> Option<(TriggerDeliveryOutcome, Option<TriggerIssueObservation>)> {
    let outcome = match document["outcome"].as_str()? {
        "applied" => TriggerDeliveryOutcome::Applied(match document["actionOutcome"].as_str()? {
            "canceled" => TriggerDeliveryAction::Cancel,
            "withdrawn" | RECORDED => TriggerDeliveryAction::Withdraw,
            _ => return None,
        }),
        "refused" => document["diagnostics"][0]["code"]
            .as_str()
            .filter(|code| is_diagnostic_slug(code))
            .map(|code| TriggerDeliveryOutcome::Refused {
                code: code.to_owned(),
            })?,
        _ => return None,
    };
    Some((outcome, observation(&document["observation"])))
}

/// The document's `{team, state, updatedAt}`, when it has the shape and bounds the protocol
/// carries.
fn observation(value: &Value) -> Option<TriggerIssueObservation> {
    let text = |value: &Value, limit: usize| {
        value
            .as_str()
            .filter(|text| !text.is_empty() && text.len() <= limit)
            .map(str::to_owned)
    };
    let nullable = |value: &Value, limit: usize| match value {
        Value::Null => Some(None),
        value => text(value, limit).map(Some),
    };
    value.as_object()?;
    Some(TriggerIssueObservation {
        team: nullable(&value["team"], MAXIMUM_OBSERVED_ID_BYTES)?,
        state: text(&value["state"], MAXIMUM_OBSERVED_ID_BYTES)?,
        updated_at: nullable(&value["updatedAt"], MAXIMUM_OBSERVED_TIME_BYTES)?,
    })
}

fn write_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// `um workflow run stop RUN --before STEP --reason TEXT --json`: puts back the start's own
/// stop once the runner lifts the one it created the run with. True when the stop was
/// recorded; false when it was not, as `start_control` logs, or the command was abandoned.
pub(in crate::service) async fn stop_before(
    um: &UmCommand,
    run_directory: &Path,
    step: &str,
    cancellation: &CancellationSource,
) -> bool {
    let arguments = stop_arguments(run_directory, step, START_STOP_REASON);
    start_control(um, arguments, cancellation).await
}

/// `um workflow run lift RUN --before STEP --json`: lifts the stop before the first step the
/// runner created the run with. True when the lift was recorded; false when it was not, as
/// `start_control` logs, or the command was abandoned.
pub(in crate::service) async fn lift(
    um: &UmCommand,
    run_directory: &Path,
    step: &str,
    cancellation: &CancellationSource,
) -> bool {
    start_control(um, lift_arguments(run_directory, Some(step)), cancellation).await
}

/// Runs the start's own stop or the runner's lift, true when it exited 0. One that ended
/// otherwise and was not abandoned has the run's log name `PENDING_STOP_UNLIFTED` with its
/// class: its `Unanswered` class, or `exit_status` when it exited unsuccessfully having
/// printed a document.
async fn start_control(
    um: &UmCommand,
    arguments: Vec<OsString>,
    cancellation: &CancellationSource,
) -> bool {
    let document = match control(um, arguments, cancellation).await {
        Answered::Ended { success: true, .. } => return true,
        Answered::Abandoned => return false,
        Answered::Ended { document, .. } => document,
    };
    match document {
        Err(class) => log_failure(&um.log, PENDING_STOP_UNLIFTED, class).await,
        Ok(_) => log_failure(&um.log, PENDING_STOP_UNLIFTED, "class=exit_status").await,
    }
    false
}

/// Applies a Cloud control's `action` to the run at `run_directory` with the `um workflow
/// run` command it names (REQ-RUNNERENROLL-90): `answer --question ID --text TEXT`, once
/// per answer in order and ending at the first that is not applied, `approve`, `hand-back
/// --notes TEXT`, `lift [--before STEP]` or `stop --before STEP --reason TEXT`, each reported
/// as `control_outcome` says. A `retry` continues the run, so the resume loop supervises it
/// instead (`retry_arguments`). `None` when the assignment's `cancellation` abandoned a
/// command, which leaves nothing to report.
pub(in crate::service) async fn apply_control(
    um: &UmCommand,
    run_directory: &Path,
    action: &RunControlAction,
    cancellation: &CancellationSource,
) -> Option<RunControlOutcome> {
    let mut outcome = command_failed();
    for (index, arguments) in control_arguments(run_directory, action)
        .into_iter()
        .enumerate()
    {
        let document = match control(um, arguments, cancellation).await {
            Answered::Abandoned => return None,
            Answered::Ended { document, .. } => document,
        };
        let recorded = Recorded::of(action, index);
        outcome = control_outcome(&um.log, document, run_directory, recorded).await;
        if !matches!(outcome, RunControlOutcome::Applied { .. }) {
            break;
        }
    }
    Some(outcome)
}

/// The control a command records for the wait its Cloud control names, as the run's
/// controls hold it: its event, the wait's identity and, for an answer, its question and
/// text.
struct Recorded {
    event: &'static str,
    wait: String,
    answer: Option<RunControlAnswer>,
}

impl Recorded {
    /// The control the command for `action`'s `index`-th line records; `None` for a control
    /// that names no wait.
    fn of(action: &RunControlAction, index: usize) -> Option<Self> {
        let (event, wait, answer) = match action {
            RunControlAction::Answer { wait, answers } => {
                (CONTROL_ANSWER, wait, answers.get(index))
            }
            RunControlAction::Approve { wait } => (CONTROL_APPROVE, wait, None),
            RunControlAction::HandBack { wait, .. } => (CONTROL_HAND_BACK, wait, None),
            RunControlAction::Lift {
                wait: Some(wait), ..
            } => (CONTROL_LIFT, wait, None),
            RunControlAction::Retry { wait } => (CONTROL_RETRY, wait, None),
            RunControlAction::Lift { wait: None, .. } | RunControlAction::Stop { .. } => {
                return None;
            }
        };
        Some(Self {
            event,
            wait: wait_identity(&wait.kind, &wait.since),
            answer: answer.cloned(),
        })
    }

    /// A `retry` of the wait whose identity is `wait`.
    const fn retry(wait: String) -> Self {
        Self {
            event: CONTROL_RETRY,
            wait,
            answer: None,
        }
    }

    fn holds(&self, control: &ControlEvent) -> bool {
        let text = |name: &str| {
            control
                .payload
                .as_ref()
                .and_then(|payload| payload.get(name))
                .and_then(Value::as_str)
        };
        control.event == self.event
            && control.wait.as_deref() == Some(self.wait.as_str())
            && self.answer.as_ref().is_none_or(|answer| {
                text("question") == Some(answer.question.as_str())
                    && text("text") == Some(answer.text.as_str())
            })
    }

    /// Whether the controls of the run at `run_directory`, read on a blocking worker, hold
    /// this control.
    async fn held_by(self, run_directory: &Path) -> bool {
        let run_directory = run_directory.to_owned();
        tokio::task::spawn_blocking(move || {
            um_execution::read_controls(&run_directory)
                .is_ok_and(|controls| controls.iter().any(|control| self.holds(control)))
        })
        .await
        .unwrap_or(false)
    }
}

/// The command lines `action` runs, in order; none for `retry`.
fn control_arguments(run_directory: &Path, action: &RunControlAction) -> Vec<Vec<OsString>> {
    let command = |name: &str, options: Vec<OsString>| {
        let mut arguments: Vec<OsString> = vec![
            "workflow".into(),
            "run".into(),
            name.into(),
            run_directory.into(),
        ];
        arguments.extend(options);
        arguments.push("--json".into());
        arguments
    };
    match action {
        RunControlAction::Answer { answers, .. } => answers
            .iter()
            .map(|answer| {
                command(
                    "answer",
                    vec![
                        option("--question", &answer.question),
                        option("--text", &answer.text),
                    ],
                )
            })
            .collect(),
        RunControlAction::Approve { .. } => vec![command("approve", Vec::new())],
        RunControlAction::HandBack { notes, .. } => {
            vec![command("hand-back", vec![option("--notes", notes)])]
        }
        RunControlAction::Lift { before, .. } => {
            vec![lift_arguments(run_directory, before.as_deref())]
        }
        RunControlAction::Stop { before, reason } => vec![stop_arguments(
            run_directory,
            before,
            reason.as_deref().unwrap_or(CLOUD_STOP_REASON),
        )],
        RunControlAction::Retry { .. } => Vec::new(),
    }
}

/// One `--name=value` argument. A control's text is a person's (an answer, hand-back notes, a
/// reason), so it never travels as an argument of its own, where text that begins with a
/// hyphen would be read as another option and the command refused before it ran.
fn option(name: &str, value: &str) -> OsString {
    format!("{name}={value}").into()
}

/// `um workflow run retry RUN --json`: on the run's `repair` wait, records the retry and
/// continues the run as `resume` does (REQ-LOCALRUN-184).
pub(in crate::service) fn retry_arguments(run_directory: &Path) -> Vec<OsString> {
    vec![
        "workflow".into(),
        "run".into(),
        "retry".into(),
        run_directory.into(),
        "--json".into(),
    ]
}

fn stop_arguments(run_directory: &Path, step: &str, reason: &str) -> Vec<OsString> {
    vec![
        "workflow".into(),
        "run".into(),
        "stop".into(),
        run_directory.into(),
        option("--before", step),
        option("--reason", reason),
        "--json".into(),
    ]
}

fn lift_arguments(run_directory: &Path, step: Option<&str>) -> Vec<OsString> {
    let mut arguments: Vec<OsString> = vec![
        "workflow".into(),
        "run".into(),
        "lift".into(),
        run_directory.into(),
    ];
    if let Some(step) = step {
        arguments.push(option("--before", step));
    }
    arguments.push("--json".into());
    arguments
}

/// The application a control command's last JSON document reports: `refused` with its first
/// diagnostic's code, `applied` with any other `outcome` the protocol carries (1 to 64
/// characters), and `None` for one that reports neither.
fn reported(document: &Value) -> Option<RunControlOutcome> {
    match document["outcome"].as_str()? {
        "refused" => document["diagnostics"][0]["code"]
            .as_str()
            .filter(|code| is_diagnostic_slug(code))
            .map(|code| RunControlOutcome::Refused {
                code: code.to_owned(),
            }),
        outcome
            if !outcome.is_empty()
                && outcome.chars().count() <= MAXIMUM_ACTION_OUTCOME_CHARACTERS =>
        {
            Some(RunControlOutcome::Applied {
                action_outcome: outcome.to_owned(),
            })
        }
        _ => None,
    }
}

/// The application of a control command that ended with `document` on the run at
/// `run_directory` (REQ-RUNNERENROLL-90): as `reported` reads a document it printed; for a
/// command that launched and printed no `outcome` or outlived its bound, `applied`
/// `recorded` once the run's controls hold `recorded`, the control it records for the wait
/// it names; and otherwise `refused` `control_command_failed`, the run's log naming the
/// failure's class.
async fn control_outcome(
    log: &RunLog,
    document: Result<Value, Unanswered>,
    run_directory: &Path,
    recorded: Option<Recorded>,
) -> RunControlOutcome {
    if let Some(outcome) = document.as_ref().ok().and_then(reported) {
        return outcome;
    }
    let printed_outcome = document
        .as_ref()
        .is_ok_and(|document| document["outcome"].is_string());
    if !printed_outcome
        && !matches!(document, Err(Unanswered::Launch(_)))
        && let Some(recorded) = recorded
        && recorded.held_by(run_directory).await
    {
        return RunControlOutcome::Applied {
            action_outcome: RECORDED.to_owned(),
        };
    }
    log_failure(log, CONTROL_COMMAND_FAILED, unanswered(&document)).await;
    command_failed()
}

/// The application of a supervised `retry` that ended with `document` on the run at
/// `run_directory`, as `control_outcome` reads it for the `control.retry` naming `wait`, the
/// identity of the wait it resolves.
pub(in crate::service) async fn retry_outcome(
    um: &UmCommand,
    document: Option<Value>,
    run_directory: &Path,
    wait: String,
) -> RunControlOutcome {
    let document = document.ok_or(Unanswered::NoDocument);
    control_outcome(
        &um.log,
        document,
        run_directory,
        Some(Recorded::retry(wait)),
    )
    .await
}

fn command_failed() -> RunControlOutcome {
    RunControlOutcome::Refused {
        code: CONTROL_COMMAND_FAILED.to_owned(),
    }
}

/// Runs one short control command, its stdout read back, at most `CONTROL_OUTPUT_LIMIT`, and
/// appended to the log: `Answered::launch` with the I/O error's kind when it could not be
/// launched or its output could not be read back.
async fn control(
    um: &UmCommand,
    arguments: Vec<OsString>,
    cancellation: &CancellationSource,
) -> Answered {
    let mut end = match run_short(
        um,
        arguments,
        ShortOutput::Captured(CONTROL_OUTPUT_LIMIT),
        um.bound,
        cancelled(cancellation),
    )
    .await
    {
        Ok(end) => end,
        Err(error) => return Answered::launch(error.kind()),
    };
    let captured = match &mut end {
        ShortEnd::Exited { captured, .. } => std::mem::take(captured),
        ShortEnd::TimedOut | ShortEnd::Abandoned => Vec::new(),
    };
    let log = um.log.clone();
    match tokio::task::spawn_blocking(move || logged_last_document(&captured, &log)).await {
        Ok(document) => Answered::of(&end, document),
        // The worker reading it back did not finish; there is no I/O error to name.
        Err(_) => Answered::launch(io::ErrorKind::Other),
    }
}
