//! What the runner viewer draws: plain data its source maps a runner's overview into
//! (REQ-RUNNERENROLL-140). Every text is drawn as received after it is made visible, and
//! every word this release does not know keeps the neutral mark.

use std::time::Duration;

use time::OffsetDateTime;

/// A mark the viewer draws beside a word, never in place of it (REQ-LOCALRUN-6).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerWatchMark {
    /// Working, or connected: `●`.
    Active,
    /// Waiting, or trying again: `◌`.
    Pending,
    /// Done as asked: `✓`.
    Success,
    /// Failed or refused: `✗`.
    Failure,
    /// Any other state, and every word this release does not know: `·`.
    Neutral,
}

/// One overview of a serving runner, as the viewer draws it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchOverview {
    pub runner_id: String,
    pub boot_id: String,
    pub version: String,
    pub uptime: Duration,
    /// The time left of the stop's bound, only while the runner is stopping.
    pub stop_time_left: Option<Duration>,
    pub connection: RunnerWatchConnection,
    /// The runner's reports Cloud has not yet acknowledged.
    pub reports_waiting: u64,
    pub current_credential_id: Option<String>,
    pub pending_credential_id: Option<String>,
    pub logins: Vec<RunnerWatchLogin>,
    pub counts: RunnerWatchCounts,
    pub assignments: Vec<RunnerWatchAssignment>,
    /// The assignments the answer itself left out.
    pub assignments_omitted: u64,
    /// The ended assignments, newest first.
    pub recent: Vec<RunnerWatchRecent>,
    /// The ended assignments the answer itself left out.
    pub recent_omitted: u64,
    /// The runner's own events, newest first.
    pub events: Vec<RunnerWatchEvent>,
    /// The events the answer itself left out.
    pub events_omitted: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchConnection {
    pub mark: RunnerWatchMark,
    /// The connection's state word as received.
    pub state: String,
    /// Whether the runner is connected now, so that `last_connected_at` is its start.
    pub connected: bool,
    pub last_connected_at: Option<OffsetDateTime>,
    /// The last failure's word as received.
    pub last_failure: Option<String>,
    /// The failed attempts since the last connection, while backing off.
    pub attempts: Option<u64>,
    /// The time until the next attempt, while backing off.
    pub next_attempt_in: Option<Duration>,
}

/// One provider's agent login.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchLogin {
    pub provider: String,
    pub ready: bool,
    /// Why a login that is not ready is not, as a code or state word.
    pub reason: Option<String>,
}

/// The assignments the runner holds, by phase.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RunnerWatchCounts {
    pub preparing: u64,
    pub accepted: u64,
    pub running: u64,
    pub waiting: u64,
    pub finishing: u64,
    pub reporting: u64,
}

/// What names a run on the screen: its subject when it has one, or else its run ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerWatchTitle {
    Subject(String),
    RunId(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchAssignment {
    pub assignment_id: String,
    pub run_id: String,
    pub attempt_number: u64,
    /// Whether this attempt continues an earlier one.
    pub continues_earlier_attempt: bool,
    /// The run's page in Cloud, when the runner was given one.
    pub run_url: Option<String>,
    pub title: RunnerWatchTitle,
    pub workflow_path: String,
    /// The phase word as received.
    pub phase: String,
    pub mark: RunnerWatchMark,
    /// The time since the runner took the offer.
    pub held: Duration,
    pub lease: Option<RunnerWatchLease>,
    pub detail: RunnerWatchAssignmentDetail,
}

/// The time Cloud granted a running assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchLease {
    /// The lease's health word as received.
    pub health: String,
    pub time_left: Duration,
}

/// What an assignment's second line tells: what its run waits for, or the steps running.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerWatchAssignmentDetail {
    None,
    Wait(RunnerWatchWait),
    Steps {
        steps: Vec<RunnerWatchStep>,
        /// The running steps the answer itself left out.
        omitted: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchWait {
    /// The wait's kind as received.
    pub kind: String,
    /// The waiting step's path, outermost first.
    pub step: Vec<String>,
    pub since: Option<OffsetDateTime>,
}

/// A running step named by its run's path and its own ID, outermost first.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchStep {
    pub path: Vec<String>,
}

/// One element of a child run's path from its assignment's root run: the `workflow` step
/// that started it, a fan-out's element and the child's ordinal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchRunElement {
    pub step: String,
    pub item: Option<u64>,
    pub ordinal: u64,
}

/// One run of an assignment as a runner read it: the root, or the child run a path names.
/// Every word is the engine's as received; the host reads it (REQ-RUNNERENROLL-135).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchRun {
    /// The child run's path from the root, outermost first; empty for the root.
    pub path: Vec<RunnerWatchRunElement>,
    pub workflow_path: String,
    /// The run record's state word.
    pub state: String,
    /// The run record's outcome, once it has one.
    pub outcome: Option<String>,
    /// The newest attempt's number, the run's count of attempts and the newest attempt's
    /// state word.
    pub attempt_number: u64,
    pub attempts: u64,
    pub attempt_state: String,
    /// What the run waits for, while it waits.
    pub wait: Option<RunnerWatchWait>,
    /// The newest attempt's steps then finalizers, in presentation order.
    pub steps: Vec<RunnerWatchRunStep>,
    /// The steps the answer itself left out.
    pub steps_omitted: u64,
}

/// One step or finalizer of a run a runner read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchRunStep {
    pub id: String,
    /// The role word, `step` or `finalizer`, as received.
    pub role: String,
    /// The kind word, `cmd`, `agent` or `workflow`, as received.
    pub kind: String,
    /// The state word as `state.json` spells it, as received.
    pub state: String,
    /// The failure-policy word as received.
    pub failure_policy: String,
    pub direct_dependencies: Vec<String>,
    pub started_at: Option<OffsetDateTime>,
    /// The time since a step in flight started, or to a settled step's last finish.
    pub duration: Option<Duration>,
    pub work: RunnerWatchRunWork,
    /// Each declared output and whether the attempt committed it.
    pub outputs: Vec<RunnerWatchRunOutput>,
    /// A `workflow` step's child runs, the last of them, in the engine's order.
    pub child_runs: Vec<RunnerWatchChildRun>,
    /// The earlier child runs the answer itself left out.
    pub child_runs_omitted: u64,
}

/// What a step runs, as the workflow declares it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerWatchRunWork {
    Command {
        argv: Vec<String>,
        cwd: Option<String>,
    },
    Agent {
        profile: String,
        /// The harness word as received.
        harness: String,
        model: Option<String>,
        effort: Option<String>,
    },
    Workflow {
        file: String,
    },
    /// The answer named none of the three.
    Unknown,
}

/// A declared output of a step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchRunOutput {
    pub name: String,
    /// The output's kind word as received.
    pub kind: String,
    pub committed: bool,
}

/// A child run of a `workflow` step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchChildRun {
    pub ordinal: u64,
    pub item: Option<u64>,
    /// The child run record's state word as received.
    pub state: String,
    /// Its outcome once it has exited.
    pub outcome: Option<String>,
}

/// What an assignment read asks a runner for (REQ-RUNNERENROLL-144): one assignment, the
/// child run its path names, the root when it is empty, and the step whose output is wanted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchAssignmentQuery {
    pub assignment_id: String,
    pub path: Vec<RunnerWatchRunElement>,
    pub step: Option<String>,
}

/// How a runner answered an assignment read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerWatchAssignmentAnswer {
    /// The runner holds the assignment, or still reports it.
    Held(Box<RunnerWatchHeldAssignment>),
    /// The runner holds it no longer.
    NotHeld,
}

/// An assignment a runner holds: its entry, the run the read named when the runner could
/// read it, and a step's output or the run log when there is one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchHeldAssignment {
    pub entry: RunnerWatchAssignment,
    pub run: Option<RunnerWatchRun>,
    pub log: Option<RunnerWatchLog>,
}

/// A step's retained output, the end of the assignment's run log, or a step's records
/// (REQ-RUNNERENROLL-145).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchLog {
    pub source: RunnerWatchLogSource,
    /// The streams in the order given; a step's output with none retained none, and a
    /// step's records none.
    pub streams: Vec<RunnerWatchLogStream>,
    /// A step's records in the order given; every other log none.
    pub records: Vec<RunnerWatchLogRecord>,
}

/// What a log is.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerWatchLogSource {
    /// The retained output of a settled step's newest invocation that retained any.
    Step {
        step: String,
        invocation: Option<u64>,
        /// The step's earlier invocations that retained output, not shown.
        earlier_invocations_omitted: u64,
    },
    /// The end of the assignment's run log.
    RunLog,
    /// A step's newest records, of a run that has no run log.
    StepRecords {
        /// The step the records are of; none when the answer named none, and then the run
        /// has no run log and no records.
        step: Option<String>,
        /// The step's older records not shown.
        omitted_records: u64,
        /// The bytes of the step's older records not shown.
        omitted_bytes: u64,
    },
    /// A source word this release does not know, with whatever text the log carried.
    Unknown(String),
}

/// One stream of a log: its text, the bytes of its file before the text and, for a step's
/// stream, its name and its retained and discarded counts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchLogStream {
    /// `stdout` or `stderr`; none for the run log.
    pub name: Option<String>,
    pub retained_bytes: Option<u64>,
    pub discarded_bytes: Option<u64>,
    /// The bytes left out before `text`.
    pub omitted_bytes: u64,
    pub text: String,
}

/// One record of a step's output: its source's word as received (`stdout`, `stderr` or an
/// agent record's word), its text and whether it is a piece a split record was cut into.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchLogRecord {
    pub source: String,
    pub text: String,
    pub continuation: bool,
}

/// An assignment the runner holds no longer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchRecent {
    pub assignment_id: String,
    pub title: RunnerWatchTitle,
    pub workflow_path: String,
    /// The result word as received.
    pub result: String,
    pub succeeded: bool,
    /// The chosen outcome, the failure code or the interruption cause.
    pub detail: Option<String>,
    pub duration: Duration,
    pub ended_at: OffsetDateTime,
}

/// One of the runner's own events.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerWatchEvent {
    pub at: OffsetDateTime,
    /// What happened, worded by the source.
    pub text: String,
    /// How many times in a row it happened.
    pub count: u64,
    pub mark: RunnerWatchMark,
}

/// How a runner answered an operation the viewer sent it (REQ-RUNNERENROLL-142): each
/// variant carries the short facts to show, worded by the source, which the viewer lays out
/// whole.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunnerWatchOutcome {
    /// The runner did what was asked.
    Done(Vec<String>),
    /// The runner refused it, or did not answer.
    Refused(Vec<String>),
}

impl RunnerWatchOutcome {
    pub(super) fn facts(&self) -> &[String] {
        match self {
            Self::Done(facts) | Self::Refused(facts) => facts,
        }
    }
}
