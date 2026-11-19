//! The tree run's resume loop (REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-99): one assignment,
//! lease and claim carry the run through every engine attempt. After `useful workflow run
//! create` and each later `useful` process exits, the loop reads `run.json`: `exited` ends
//! the loop; `waiting` reads `wait.json` and, for a wait only a control resolves (rule
//! source `um`), runs `useful workflow run resume` once a Cloud control is applied, and for an
//! external condition runs it on entry and then at an interval, longer while the condition
//! is unavailable; `running` with no process left (an abandoned attempt or an unchained
//! `continued` edge) runs `resume` once. A cancellation of a waiting run is `useful workflow
//! run cancel`; the runner's shutdown or lease loss ends the loop with the record as it
//! stands. Between and during the processes the loop reports each new root position as
//! `tree_progress`, numbered from 1 apart from the attempt's transitions, with the step
//! observations of the run and its child runs that are new or changed since it last
//! reported them (REQ-RUNNERENROLL-88), and flushes those before the record's exit ends the loop. A
//! report that carries a new root position must be queued or the attempt aborts; one for
//! steps alone that cannot be queued is offered again with the next read and costs the run
//! nothing. Once
//! the loop has ended on an exited record, after the last `useful` process and so after the
//! outcome hook, it reads the run's `mirror/` writes onto that record for the terminal
//! report, the number the run's `mirror/` held when the runner took it marking the leading
//! writes as earlier attempts', which request no release (REQ-RUNNERENROLL-99): none for a
//! created run, and for an adopted run the count read before its offer's deliveries are
//! applied and before its first command. When the last document carries no settled engine
//! result, it also reads the result the run's current attempt recorded in `state.json`. For
//! an exit with a workflow outcome that the runner reports from the record, it also reads the
//! root's text exports from the result the current attempt published (`exports`,
//! REQ-RUNNERENROLL-131), which `records::recorded_exit` carries with the record's outcome.
//!
//! A `create` the runtime refuses made no run: it ends the loop as a
//! `ClassifiedEnd::Refused` naming the root workflow's first step, never as a missing
//! record. An adopted run (REQ-RUNNERENROLL-89) creates nothing: with no named step the loop
//! starts at the record the earlier attempt left, and with named steps its first command
//! is `useful workflow run resume RUN --from STEP...`, whose refusal ends the loop as a
//! `ClassifiedEnd::Refused` rather than as a failure. A cancellation that reaches that first
//! command before it starts an engine attempt, the record still reading `exited` as the
//! earlier attempt left it, ends the loop as a `ClassifiedEnd::Unstarted`: the attempt is
//! cancelled or interrupted and reports nothing of that record. The adopted start reads the
//! assignment's cancellation before it runs anything: one already requested applies none of
//! the offer's pending deliveries, and one requested by the time they are applied launches no
//! first command, the loop ending `Unstarted` over an `exited` record and ending a standing
//! run as one no process holds.
//!
//! Each trigger delivery for the assignment is applied once with `useful workflow run event`
//! and its application reported (REQ-TRIGGER-136): while a process runs, beside it, so a
//! cancel reaches the live run; while the run waits, at once, the wait staying in place
//! unless the application ended or moved the run. The offer's pending deliveries are
//! applied before the run's first step: on an edge-scheduled root after `create`, which
//! stopped the run before it, a run they leave standing having the start's own stop put
//! back, the runner's lifted and being resumed; and before an adopted run's first command,
//! which does not run when they ended the run. On a root that is not edge-scheduled, which
//! the runtime holds before nowhere, they are applied once `create` returns, with no stop of
//! the runner's.
//!
//! Each Cloud control for the assignment is applied once and its application reported
//! (REQ-RUNNERENROLL-90). One that names a wait (`answer`, `approve`, `hand_back`, `retry`, and
//! `lift` when it names one) is applied while the run waits and no process holds it, only when
//! the run's current wait is that wait, and is otherwise skipped `wait_not_current` with
//! nothing run; an applied one is followed by `resume`, except `retry`, which continues the run
//! itself under the loop's supervision. One that names none (`stop`, `withdraw`, and `lift`
//! without a wait) is applied as soon as it arrives, beside a running process as a delivery
//! is. An applied `withdraw` is followed by no `resume` (REQ-RUNNERENROLL-123): the loop reads
//! the record, which ends it on an `exited` record the command left; a withdrawal recorded for
//! a process that then left the run waiting, as an evaluation of a held wait does, is finished
//! with one more `withdraw`, reported no further.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::FutureExt as _;
use futures_util::future::FusedFuture as _;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use useful_execution::{CancellationReason, CancellationSource, ConditionError, wait_identity};
use useful_runner_protocol::{
    RunControl, RunControlAction, RunControlApplication, RunControlOutcome, RunControlSkip,
    RunControlWait, TreeProgressWait, TreeRootProgress, TreeStepObservation, TreeStepState,
    TriggerDelivery, TriggerDeliveryApplication,
};

use super::command::{self, CommandExit, UsefulCommand};
use super::exports::{EXPORT_READER_LOST, read_exports};
use super::records::{
    ClassifiedEnd, EngineResult, MIRROR_READER_LOST, RecordState, RefusedCommand, TreeConclusion,
    TreeRecord, TreeWait, conclude, conclude_classified, mirror_read_failure, read_record,
    read_recorded_result, read_wait,
};
use super::steps::{self, StepKey, StepReader};
use super::{PendingStop, TreeRun, TreeStart};
use crate::service::assignment::ExecutionReport;

/// The most step observations one progress report carries.
const MAXIMUM_REPORT_STEPS: usize = 64;

/// The loop's intervals: between evaluations of a held external wait, between
/// evaluations of one whose condition could not be read, and between reads of the
/// record while a `useful` process runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) struct ResumeIntervals {
    pub(in crate::service) held: Duration,
    pub(in crate::service) unavailable: Duration,
    pub(in crate::service) record: Duration,
}

impl Default for ResumeIntervals {
    fn default() -> Self {
        Self {
            held: Duration::from_secs(30),
            unavailable: Duration::from_secs(120),
            record: Duration::from_secs(1),
        }
    }
}

/// The record the loop ended on, the last command's document, the cancellation the
/// runner applied, for a run whose end the runner classifies itself, how it ended, the
/// engine's result the run directory recorded for an exited run whose document carries
/// none, the class of a failure to read the exited run's `mirror/`, and the class of the
/// first of its text exports that could not be listed.
#[derive(Debug)]
pub(in crate::service) struct TreeEnd {
    pub(in crate::service) record: Option<TreeRecord>,
    pub(in crate::service) document: Option<Value>,
    pub(in crate::service) cancellation: Option<CancellationReason>,
    pub(in crate::service) classified: Option<ClassifiedEnd>,
    pub(in crate::service) recorded: Option<EngineResult>,
    pub(in crate::service) mirror_failure: Option<&'static str>,
    pub(in crate::service) export_failure: Option<&'static str>,
}

impl TreeEnd {
    /// The loop's end on `record`, as the command that last ran left it.
    fn after(record: Option<TreeRecord>, exit: CommandExit) -> Self {
        Self::of(record, exit.document, exit.cancellation)
    }

    /// The loop's end on `record` with the last command's `document` and the applied
    /// `cancellation`, neither classified nor yet read beyond the record.
    fn of(
        record: Option<TreeRecord>,
        document: Option<Value>,
        cancellation: Option<CancellationReason>,
    ) -> Self {
        Self {
            record,
            document,
            cancellation,
            classified: None,
            recorded: None,
            mirror_failure: None,
            export_failure: None,
        }
    }

    /// The settled engine result the last command's document carries.
    fn documented(&self) -> Option<EngineResult> {
        self.document.as_ref().and_then(EngineResult::from_document)
    }

    /// The attempt's report: `records::conclude_classified` for an end the runner
    /// classified, otherwise `records::conclude` over the engine's result the document
    /// carries or, when it carries none, the one the run directory recorded.
    pub(in crate::service) fn conclusion(&self) -> TreeConclusion {
        match &self.classified {
            Some(classified) => conclude_classified(classified),
            None => {
                let documented = self.documented();
                conclude(
                    self.record.as_ref(),
                    documented.as_ref().or(self.recorded.as_ref()),
                    self.cancellation,
                )
            }
        }
    }

    /// The end with what the run at `directory` recorded beside its exited record, read on
    /// one blocking worker: its mirror writes on the record, the first `earlier_writes` of
    /// them marked an earlier attempt's; for an unclassified end whose outcome is not the
    /// engine's cancellation, the root's text exports on the record (REQ-RUNNERENROLL-131)
    /// and the class of the first that could not be listed; and, for such an end whose
    /// document carries no settled result, the engine's result its current attempt
    /// recorded. Only a write past the earlier ones can request release
    /// (REQ-RUNNERENROLL-99): when their count could not be read every write is marked
    /// earlier, and a `mirror/` that cannot be read now reports no writes, so the report
    /// requests no release on an outcome it cannot confirm as the attempt's own. The end
    /// keeps the class of either mirror failure for the run event, the later read's when
    /// both failed, and a worker that did not return loses both reads.
    async fn with_exit_records(
        mut self,
        directory: PathBuf,
        earlier_writes: Result<usize, &'static str>,
    ) -> Self {
        if let Err(failure) = earlier_writes {
            self.mirror_failure = Some(failure);
        }
        let wants_result = self.classified.is_none() && self.documented().is_none();
        let Some(record) = self
            .record
            .as_mut()
            .filter(|record| record.state == RecordState::Exited)
        else {
            return self;
        };
        let wants_result = wants_result && !record.reads_cancellation();
        let wants_exports = self.classified.is_none() && !record.reads_cancellation();
        match tokio::task::spawn_blocking(move || {
            (
                useful_execution::read_mirror_writes(&directory),
                wants_result
                    .then(|| read_recorded_result(&directory))
                    .flatten(),
                wants_exports.then(|| read_exports(&directory)),
            )
        })
        .await
        {
            Ok((writes, recorded, exports)) => {
                match writes {
                    Ok(writes) => {
                        record.earlier_writes = earlier_writes.unwrap_or(writes.len());
                        record.mirror = writes;
                    }
                    Err(error) => self.mirror_failure = Some(mirror_read_failure(&error)),
                }
                self.recorded = recorded;
                if let Some(exports) = exports {
                    record.exports = exports.exports;
                    self.export_failure = exports.failure;
                }
            }
            Err(_) => {
                self.mirror_failure = Some(MIRROR_READER_LOST);
                if wants_exports {
                    self.export_failure = Some(EXPORT_READER_LOST);
                }
            }
        }
        self
    }
}

/// Why the loop ended without a record to conclude from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum DriveFailure {
    /// A command or record failed; the attempt aborts with this cause.
    Cause(&'static str),
    /// A report of a new root position or of a delivery's or control's application could
    /// not be queued; the enqueue recorded its cause.
    ReportUnqueued,
}

/// Queues one report of the run, a root progress report or a delivery's or control's
/// application; false when it could not be queued, which the loop survives only for a
/// progress report for steps alone.
pub(in crate::service) type ReportSink<'a> = &'a (dyn Fn(ExecutionReport) -> bool + Sync);

/// What the last `resume` of a still-waiting run said about its wait.
enum Answer {
    /// `outcome` `waiting`: the wait holds (REQ-CONTINUE-74).
    Held,
    /// Refused with `wait_condition_unavailable`: the condition could not be read.
    Unavailable,
    /// Anything else leaves the wait unresolved with no reason to ask again.
    Refused,
}

fn answer(document: Option<&Value>) -> Answer {
    let Some(document) = document else {
        return Answer::Refused;
    };
    if document["outcome"] == "waiting" {
        return Answer::Held;
    }
    let unavailable = document["outcome"] == "rejected"
        && document["diagnostics"]
            .as_array()
            .is_some_and(|diagnostics| {
                diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic["code"] == ConditionError::UNAVAILABLE)
            });
    if unavailable {
        Answer::Unavailable
    } else {
        Answer::Refused
    }
}

/// What the loop waits for before it next runs `resume` on a waiting run.
enum Pause {
    Now,
    For(Duration),
    Control,
}

/// How a pause ended.
enum Woken {
    /// The pause elapsed, or a control was applied for `resume` to act on.
    Due,
    /// A delivery's application ended the run or moved it off the wait, or a withdrawal was
    /// applied: the record is read again, and no `resume` runs on the control's account.
    Changed,
    /// A `retry` control ran the run's next command, which exited so.
    Ran(CommandExit),
    Cancelled(CancellationReason),
}

/// A root observation: the record's attempt count, position and wait.
type RootObservation = (u64, Vec<Value>, Option<TreeProgressWait>);

/// The progress reports sent so far: the last root observation, so an unchanged one is
/// not sent again, the state each step observation was last reported in, so only a new
/// or changed one is, and the last sequence.
struct Progress<'a> {
    sink: ReportSink<'a>,
    sequence: u64,
    last: Option<RootObservation>,
    reported: HashMap<StepKey, TreeStepState>,
}

impl Progress<'_> {
    /// Reports the record's attempt, position and wait when they differ from the last
    /// report, and the observations among `steps` that are new or changed, at most 64 a
    /// report. A report for steps alone restates the last root observation, or, before
    /// any, the record's attempt and position with no wait. A waiting record whose wait
    /// record is not yet written reports nothing, and an exited one reports only steps.
    /// A report for steps alone that the sink refuses is not a failure
    /// (REQ-RUNNERENROLL-88): its observations and those after it stay unreported, under
    /// the same sequence, for the next read to offer again. Once a report that carries the
    /// root as news is queued, the run's activity reads waiting exactly when it carries a
    /// wait.
    fn observe(
        &mut self,
        run: &TreeRun,
        record: &TreeRecord,
        wait: Option<&TreeWait>,
        steps: Vec<TreeStepObservation>,
    ) -> Result<(), DriveFailure> {
        let observed = match (record.state, wait) {
            (RecordState::Waiting, None) => return Ok(()),
            (RecordState::Exited, _) => None,
            (RecordState::Running, _) => Some((record.attempts, record.position.clone(), None)),
            (RecordState::Waiting, Some(wait)) => Some((
                record.attempts,
                record.position.clone(),
                Some(TreeProgressWait {
                    kind: wait.kind.clone(),
                    since: wait.since.clone(),
                    step: wait.step.clone(),
                    questions: wait.questions.clone(),
                    failed_child: wait.failed_child.clone(),
                }),
            )),
        };
        let root = match observed {
            Some(observed) if self.last.as_ref() != Some(&observed) => Some(observed),
            _ => None,
        };
        let mut changed = steps
            .into_iter()
            .map(|observation| (steps::key(&observation), observation))
            .filter(|(key, observation)| self.reported.get(key) != Some(&observation.state))
            .collect::<Vec<_>>();
        if root.is_none() && changed.is_empty() {
            return Ok(());
        }
        // Only the first report of a changed root carries it as news.
        let mut steps_only = root.is_none();
        let root = root
            .or_else(|| self.last.clone())
            .unwrap_or_else(|| (record.attempts, record.position.clone(), None));
        let reported_at = run
            .sleeper
            .utc_now()
            .format(&Rfc3339)
            .map_err(|_| DriveFailure::Cause("tree_run_progress_timestamp_unavailable"))?;
        loop {
            let rest = changed.split_off(changed.len().min(MAXIMUM_REPORT_STEPS));
            let sequence = self
                .sequence
                .checked_add(1)
                .ok_or(DriveFailure::Cause("tree_run_progress_sequence_exhausted"))?;
            let (keys, steps): (Vec<_>, Vec<_>) = changed.into_iter().unzip();
            let states = steps
                .iter()
                .map(|observation| observation.state)
                .collect::<Vec<_>>();
            let (attempt, position, wait) = root.clone();
            if !(self.sink)(ExecutionReport::TreeProgress {
                progress_sequence: sequence,
                progress: TreeRootProgress {
                    attempt,
                    position,
                    wait,
                    reported_at: reported_at.clone(),
                    steps,
                },
                steps_only,
            }) {
                // Step reporting changes no run's result: observations that could not be
                // queued alone are offered again with the next read. A report that carries
                // a new root is the run's own.
                return if steps_only {
                    Ok(())
                } else {
                    Err(DriveFailure::ReportUnqueued)
                };
            }
            if !steps_only {
                // The run's activity follows the report that carries its root as news: a
                // run is waiting exactly when it was last reported waiting (INV-RUNNER-15).
                run.activity().reported(root.2.is_some());
            }
            self.sequence = sequence;
            self.last = Some(root.clone());
            self.reported.extend(keys.into_iter().zip(states));
            if rest.is_empty() {
                return Ok(());
            }
            changed = rest;
            steps_only = true;
        }
    }
}

/// One read of the run directory: its record, its wait record while it waits, and the
/// step observations of the run and its child runs.
struct Observation {
    record: TreeRecord,
    wait: Option<TreeWait>,
    steps: Vec<TreeStepObservation>,
}

struct Driver<'a> {
    run: &'a TreeRun,
    progress: Progress<'a>,
    directory: Option<PathBuf>,
    /// How many writes the run's `mirror/` held when the runner took the run, which are
    /// earlier attempts' (REQ-RUNNERENROLL-99): none for a run `create` made, and for an
    /// adopted run the count read before anything is applied to it, or the class of the
    /// failure to read it.
    earlier_writes: Result<usize, &'static str>,
    steps: Arc<Mutex<StepReader>>,
    /// The deliveries and controls taken for application, by id, so a redelivered one is
    /// applied once.
    applied: BTreeSet<String>,
    /// The controls taken and not yet applied, in arrival order.
    controls: VecDeque<RunControl>,
    /// Whether a withdrawal was applied `recorded`, left for the process holding the run to
    /// honor, and the record has not been seen waiting since (REQ-RUNNERENROLL-123).
    withdrawal_recorded: bool,
}

/// Whether a command's document is the runtime's refusal, and its first diagnostic's
/// code.
fn refusal(document: Option<&Value>) -> Option<Option<String>> {
    let document = document.filter(|document| document["outcome"] == "rejected")?;
    Some(
        document["diagnostics"][0]["code"]
            .as_str()
            .map(str::to_owned),
    )
}

/// Runs the tree from `create`, or from the adopted record, to its end; see the module
/// documentation.
pub(super) async fn drive(run: &TreeRun, sink: ReportSink<'_>) -> Result<TreeEnd, DriveFailure> {
    let mut driver = Driver {
        run,
        progress: Progress {
            sink,
            sequence: 0,
            last: None,
            reported: HashMap::new(),
        },
        directory: None,
        earlier_writes: Ok(0),
        steps: Arc::default(),
        applied: BTreeSet::new(),
        controls: VecDeque::new(),
        withdrawal_recorded: false,
    };
    let end = drive_to_end(&mut driver).await?;
    Ok(match driver.directory {
        Some(directory) => {
            end.with_exit_records(directory, driver.earlier_writes)
                .await
        }
        None => end,
    })
}

/// Runs `create`, or the adopted run's first command, then `resume` as the record asks,
/// until the loop ends.
async fn drive_to_end(driver: &mut Driver<'_>) -> Result<TreeEnd, DriveFailure> {
    let run = driver.run;
    // The attempt count of an adopted exited record its first command resumes from named
    // steps: a record still exited on that count is the earlier attempt's, not this one's.
    let mut adopted_exit: Option<u64> = None;
    let mut exit = match &run.start {
        TreeStart::Create {
            arguments, first, ..
        } => {
            let mut exit = driver.command(arguments.clone()).await?;
            // A refused `create` made no run, so there is no record to read.
            if exit.cancellation.is_none()
                && let Some(code) = refusal(exit.document.as_ref())
            {
                return Ok(TreeEnd {
                    classified: Some(ClassifiedEnd::Refused {
                        command: RefusedCommand::Create,
                        step: first.clone(),
                        code,
                    }),
                    ..TreeEnd::after(None, exit)
                });
            }
            driver.directory = match exit
                .document
                .as_ref()
                .and_then(|document| document["runDirectory"].as_str())
            {
                Some(reported) => Some(PathBuf::from(reported)),
                None => locate(&run.runs).await,
            };
            if let Some(directory) = driver.directory.clone()
                && exit.cancellation.is_none()
            {
                exit = driver.apply_pending(&directory).await?.unwrap_or(exit);
            }
            exit
        }
        TreeStart::Adopt(steps) => {
            driver.directory = locate(&run.runs).await;
            let ended = match driver.directory.clone() {
                Some(directory) => {
                    // The writes the run already holds are earlier attempts', counted before
                    // a delivery or command of this attempt can add one.
                    driver.earlier_writes = count_mirror_writes(directory.clone()).await;
                    driver.apply_adopted_pending(&directory).await?
                }
                None => false,
            };
            match (driver.directory.clone(), steps.first()) {
                (Some(directory), Some(first)) if !ended => {
                    let adopted_record = observe_record(directory.clone())
                        .await
                        .map(|(record, _)| record);
                    adopted_exit = adopted_record
                        .as_ref()
                        .filter(|record| record.state == RecordState::Exited)
                        .map(|record| record.attempts);
                    // A cancellation already requested launches no resume: over an exited
                    // record the loop ends as on a resume it reached before it started an
                    // attempt, and a standing run is ended as one no process holds.
                    let exit = match (
                        run.cancellation.source().cancellation_reason(),
                        adopted_record,
                    ) {
                        (Some(reason), Some(record)) if record.state != RecordState::Exited => {
                            return driver.cancelled(reason, &directory, record).await;
                        }
                        (Some(reason), _) => CommandExit {
                            document: None,
                            cancellation: Some(reason),
                        },
                        (None, _) => {
                            driver
                                .command(command::resume_from_arguments(&directory, steps))
                                .await?
                        }
                    };
                    if exit.cancellation.is_none()
                        && let Some(code) = refusal(exit.document.as_ref())
                    {
                        // The loop never reads a refused run; its steps are reported here.
                        if let Some(observed) = driver.observe(directory).await {
                            driver.progress.observe(
                                run,
                                &observed.record,
                                observed.wait.as_ref(),
                                observed.steps,
                            )?;
                        }
                        return Ok(TreeEnd {
                            classified: Some(ClassifiedEnd::Refused {
                                command: RefusedCommand::Resume,
                                step: first.clone(),
                                code,
                            }),
                            ..TreeEnd::after(None, exit)
                        });
                    }
                    exit
                }
                // With no named step the loop starts at the record the earlier attempt left.
                _ => CommandExit {
                    document: None,
                    cancellation: None,
                },
            }
        }
    };
    let mut entered_wait: Option<String> = None;
    let mut resumed_running: Option<u64> = None;
    loop {
        let observed = match &driver.directory {
            Some(directory) => driver.observe(directory.clone()).await,
            None => None,
        };
        let (Some(directory), Some(observed)) = (driver.directory.clone(), observed) else {
            return Ok(TreeEnd::after(None, exit));
        };
        let Observation {
            record,
            wait,
            steps,
        } = observed;
        // A process the cancellation reached ends the loop, settled or not.
        if let Some(cancellation) = exit.cancellation {
            driver.flush(&record, steps, exit.cancellation)?;
            // A first resume from named steps that left the adopted record exited on the
            // same attempt started nothing: the record is the earlier attempt's.
            let classified = (record.state == RecordState::Exited
                && adopted_exit == Some(record.attempts))
            .then_some(ClassifiedEnd::Unstarted { cancellation });
            return Ok(TreeEnd {
                classified,
                ..TreeEnd::after(Some(record), exit)
            });
        }
        let pause = match record.state {
            RecordState::Exited => {
                // A first resume from named steps that was not refused and left the
                // adopted record exited on the same attempt started nothing.
                if adopted_exit == Some(record.attempts) {
                    return Err(DriveFailure::Cause("tree_run_resume_stalled"));
                }
                driver.flush(&record, steps, None)?;
                // An adopted record that already exited has no document: the end-of-drive
                // read takes its result from the run directory.
                return Ok(TreeEnd::after(Some(record), exit));
            }
            RecordState::Running => {
                // A resume that left the record running on the same attempt started
                // nothing; asking again would only repeat it.
                if resumed_running == Some(record.attempts) {
                    return Err(DriveFailure::Cause("tree_run_resume_stalled"));
                }
                driver.progress.observe(run, &record, None, steps)?;
                resumed_running = Some(record.attempts);
                entered_wait = None;
                Pause::Now
            }
            RecordState::Waiting => {
                // A withdrawal recorded for a process that ended without honoring it, as an
                // evaluation of a held wait does (REQ-LOCALRUN-212), is finished on the waiting
                // run, unreported, and the record read again (REQ-RUNNERENROLL-123).
                if std::mem::take(&mut driver.withdrawal_recorded) {
                    command::finish_withdrawal(&run.useful, &directory, run.cancellation.source())
                        .await;
                    continue;
                }
                let Some(wait) = wait else {
                    return Err(DriveFailure::Cause("tree_run_wait_unavailable"));
                };
                driver.progress.observe(run, &record, Some(&wait), steps)?;
                resumed_running = None;
                let identity = wait.identity();
                let entered = entered_wait.as_deref() != Some(identity.as_str());
                entered_wait = Some(identity);
                match (wait.awaits_control(), entered) {
                    (true, true) => Pause::Control,
                    (false, true) => Pause::Now,
                    (awaits_control, false) => match answer(exit.document.as_ref()) {
                        Answer::Refused => {
                            return Err(DriveFailure::Cause("tree_run_resume_refused"));
                        }
                        _ if awaits_control => Pause::Control,
                        Answer::Held => Pause::For(run.intervals.held),
                        Answer::Unavailable => Pause::For(run.intervals.unavailable),
                    },
                }
            }
        };
        match driver
            .pause(pause, &directory, entered_wait.as_deref())
            .await?
        {
            Woken::Due => {}
            Woken::Changed => continue,
            Woken::Ran(ran) => {
                exit = ran;
                continue;
            }
            Woken::Cancelled(reason) => {
                return driver.cancelled(reason, &directory, record).await;
            }
        }
        exit = driver
            .command(command::resume_arguments(&directory))
            .await?;
    }
}

impl Driver<'_> {
    /// Supervises one `useful` command, reading the record at the record interval while it
    /// runs so a new attempt, stage or step state is reported as it starts, and applying
    /// each delivery, and each control that names no wait, that arrives once the run
    /// directory exists, beside the process.
    async fn command(
        &mut self,
        arguments: Vec<std::ffi::OsString>,
    ) -> Result<CommandExit, DriveFailure> {
        let run = self.run;
        let supervised = command::supervise(
            &run.useful,
            arguments,
            run.runs.clone(),
            &run.documents,
            run.cancellation.source(),
            run.cancellation.grace(),
        );
        tokio::pin!(supervised);
        // Completes at once with nothing applied; each batch of deliveries and controls
        // replaces it.
        let cancellation = run.cancellation.source();
        let applying = apply_beside(
            &run.useful,
            cancellation,
            PathBuf::new(),
            Vec::new(),
            Vec::new(),
        )
        .fuse();
        tokio::pin!(applying);
        loop {
            tokio::select! {
                biased;
                exit = &mut supervised => {
                    if !applying.is_terminated() {
                        let applied = (&mut applying).await;
                        self.report_beside(applied)?;
                    }
                    return exit.map_err(|failure| DriveFailure::Cause(failure.cause()));
                }
                applied = &mut applying, if !applying.is_terminated() => {
                    self.report_beside(applied)?;
                }
                () = run.deliveries.arrived(), if applying.is_terminated() => {}
                () = run.controls.arrived(), if applying.is_terminated() => {}
                () = run.sleeper.sleep(run.intervals.record) => {
                    if self.directory.is_none() {
                        self.directory = locate(&run.runs).await;
                    }
                    if let Some(directory) = self.directory.clone()
                        && let Some(observed) = self.observe(directory).await
                    {
                        self.progress.observe(
                            run,
                            &observed.record,
                            observed.wait.as_ref(),
                            observed.steps,
                        )?;
                    }
                }
            }
            if applying.is_terminated()
                && let Some(directory) = self.directory.clone()
            {
                let deliveries = self.take_deliveries();
                let controls = self.take_unwaited_controls();
                if !deliveries.is_empty() || !controls.is_empty() {
                    applying.set(
                        apply_beside(&run.useful, cancellation, directory, deliveries, controls)
                            .fuse(),
                    );
                }
            }
        }
    }

    /// Waits as `pause` says, applying each delivery and control that arrives meanwhile to
    /// the run at `directory`: `Changed` once a delivery's application leaves the run off
    /// `wait`, the identity of the wait it holds, `Due`, `Ran` or, for a `withdraw`, `Changed`
    /// once a control is applied (`apply_control`), and `Cancelled` when the assignment's
    /// cancellation comes first.
    /// `Control` waits for a control alone. `Now` waits for nothing; the command it precedes
    /// applies any delivery, and any control that names no wait.
    async fn pause(
        &mut self,
        pause: Pause,
        directory: &Path,
        wait: Option<&str>,
    ) -> Result<Woken, DriveFailure> {
        let run = self.run;
        let cancellation = run.cancellation.source();
        if matches!(pause, Pause::Now) {
            return Ok(cancellation
                .cancellation_reason()
                .map_or(Woken::Due, Woken::Cancelled));
        }
        let elapsed = async move {
            match pause {
                Pause::Now => {}
                Pause::For(duration) => run.sleeper.sleep(duration).await,
                Pause::Control => std::future::pending().await,
            }
        };
        tokio::pin!(elapsed);
        loop {
            if let Some(reason) = cancellation.cancellation_reason() {
                return Ok(Woken::Cancelled(reason));
            }
            let deliveries = self.take_deliveries();
            if !deliveries.is_empty() {
                let applications = command::apply_deliveries(
                    &run.useful,
                    directory.to_owned(),
                    deliveries,
                    cancellation,
                )
                .await;
                self.report_applications(applications)?;
                let held =
                    observe_record(directory.to_owned())
                        .await
                        .is_some_and(|(record, held)| {
                            record.state == RecordState::Waiting
                                && held.map(|held| held.identity()).as_deref() == wait
                        });
                if !held {
                    return Ok(Woken::Changed);
                }
                continue;
            }
            self.queue_controls();
            if let Some(control) = self.controls.pop_front() {
                if let Some(woken) = self.apply_control(control, directory).await? {
                    return Ok(woken);
                }
                continue;
            }
            tokio::select! {
                biased;
                reason = cancellation.wait_for_cancellation() => return Ok(Woken::Cancelled(reason)),
                () = &mut elapsed => return Ok(Woken::Due),
                () = run.deliveries.arrived() => {}
                () = run.controls.arrived() => {}
            }
        }
    }

    /// Applies the offer's pending deliveries to the run `create` made. On an edge-scheduled
    /// root `create` stopped the run before its first step, and a run they leave standing has
    /// the start's own stop put back when it names a later step, the runner's stop lifted
    /// and is resumed; that `resume`'s exit is returned. On any other root they are applied
    /// as the run stands, and the loop goes on from its record.
    async fn apply_pending(
        &mut self,
        directory: &Path,
    ) -> Result<Option<CommandExit>, DriveFailure> {
        let run = self.run;
        let TreeStart::Create {
            first,
            pending_stop: Some(pending_stop),
            ..
        } = &run.start
        else {
            return Ok(None);
        };
        let cancellation = run.cancellation.source();
        let pending = self.take_pending();
        let applications =
            command::apply_deliveries(&run.useful, directory.to_owned(), pending, cancellation)
                .await;
        self.report_applications(applications)?;
        let PendingStop::Held { start } = pending_stop else {
            return Ok(None);
        };
        if !standing(directory).await || start.as_ref() == Some(first) {
            return Ok(None);
        }
        // A stop or lift the assignment's cancellation abandoned leaves the run stopped for
        // the loop to end as the cancellation says.
        let recorded = |recorded: bool| match recorded {
            true => Ok(true),
            false if cancellation.is_cancelled() => Ok(false),
            false => Err(DriveFailure::Cause(command::PENDING_STOP_UNLIFTED)),
        };
        if let Some(start) = start
            && !recorded(command::stop_before(&run.useful, directory, start, cancellation).await)?
        {
            return Ok(None);
        }
        if !recorded(command::lift(&run.useful, directory, first, cancellation).await)? {
            return Ok(None);
        }
        self.command(command::resume_arguments(directory))
            .await
            .map(Some)
    }

    /// Applies the offer's pending deliveries to the adopted run at `directory` before its
    /// first command, as `create`'s stop lets them precede a created run's first step; true
    /// when they ended a run that was standing, which then resumes no step. A cancellation
    /// already requested applies none of them, so they stay open for the run's next attempt.
    async fn apply_adopted_pending(&mut self, directory: &Path) -> Result<bool, DriveFailure> {
        if self.run.cancellation.source().is_cancelled() {
            return Ok(false);
        }
        let pending = self.take_pending();
        if pending.is_empty() {
            return Ok(false);
        }
        let was_standing = standing(directory).await;
        let applications = command::apply_deliveries(
            &self.run.useful,
            directory.to_owned(),
            pending,
            self.run.cancellation.source(),
        )
        .await;
        self.report_applications(applications)?;
        Ok(was_standing && !standing(directory).await)
    }

    /// The offer's pending deliveries not yet taken, in the offer's order.
    fn take_pending(&mut self) -> Vec<TriggerDelivery> {
        self.run
            .pending
            .iter()
            .filter(|delivery| self.applied.insert(delivery.delivery_id.clone()))
            .cloned()
            .collect()
    }

    /// The deliveries queued for the assignment and not yet taken, in arrival order.
    fn take_deliveries(&mut self) -> Vec<TriggerDelivery> {
        self.run
            .deliveries
            .take()
            .into_iter()
            .filter(|delivery| self.applied.insert(delivery.delivery_id.clone()))
            .collect()
    }

    /// Moves the controls queued for the assignment, each id once, behind those taken
    /// before.
    fn queue_controls(&mut self) {
        let taken = self.run.controls.take();
        self.controls.extend(
            taken
                .into_iter()
                .filter(|control| self.applied.insert(control.control_id.clone())),
        );
    }

    /// The controls taken that name no wait, which apply beside a running process; those
    /// that name one stay for the next pause.
    fn take_unwaited_controls(&mut self) -> Vec<RunControl> {
        self.queue_controls();
        let (unwaited, waited): (Vec<_>, Vec<_>) = std::mem::take(&mut self.controls)
            .into_iter()
            .partition(|control| named_wait(&control.action).is_none());
        self.controls = waited.into();
        unwaited
    }

    /// Applies `control` to the run at `directory`, which no process holds, and reports its
    /// application (REQ-RUNNERENROLL-90). A control naming a wait the run does not currently
    /// wait on is skipped `wait_not_current` and runs nothing. A `retry` runs as the loop's
    /// next command, `Ran` with its exit once it applied or was cancelled; a `withdraw` that
    /// applied is `Changed`, for the loop to read the record it left, and is followed by no
    /// `resume` (REQ-RUNNERENROLL-123); any other control that applied is `Due`, for `resume`
    /// to act on it; and one that did not, or that the assignment's cancellation abandoned
    /// unreported, leaves the run waiting as it was, `None`.
    async fn apply_control(
        &mut self,
        control: RunControl,
        directory: &Path,
    ) -> Result<Option<Woken>, DriveFailure> {
        let wait = named_wait(&control.action).map(|wait| wait_identity(&wait.kind, &wait.since));
        if let Some(wait) = &wait {
            let current = observe_record(directory.to_owned())
                .await
                .filter(|(record, _)| record.state == RecordState::Waiting)
                .and_then(|(_, held)| held)
                .map(|held| held.identity());
            if current.as_deref() != Some(wait.as_str()) {
                self.report_control(
                    control.control_id,
                    RunControlOutcome::Skipped(RunControlSkip::WaitNotCurrent),
                )?;
                return Ok(None);
            }
        }
        if let (RunControlAction::Retry { .. }, Some(wait)) = (&control.action, wait) {
            let exit = self.command(command::retry_arguments(directory)).await?;
            let outcome =
                command::retry_outcome(&self.run.useful, exit.document.clone(), directory, wait)
                    .await;
            let applied = matches!(outcome, RunControlOutcome::Applied { .. });
            self.report_control(control.control_id, outcome)?;
            return Ok((applied || exit.cancellation.is_some()).then_some(Woken::Ran(exit)));
        }
        let Some(outcome) = command::apply_control(
            &self.run.useful,
            directory,
            &control.action,
            self.run.cancellation.source(),
        )
        .await
        else {
            return Ok(None);
        };
        let applied = matches!(outcome, RunControlOutcome::Applied { .. });
        self.withdrawal_recorded |= recorded_withdrawal(&control.action, &outcome);
        self.report_control(control.control_id, outcome)?;
        Ok(applied.then_some(match control.action {
            RunControlAction::Withdraw => Woken::Changed,
            _ => Woken::Due,
        }))
    }

    /// Queues the applications made beside a running process, noting a withdrawal applied
    /// `recorded` among them.
    fn report_beside(&mut self, applied: AppliedBeside) -> Result<(), DriveFailure> {
        self.withdrawal_recorded |= applied.withdrawal_recorded;
        self.report(applied.reports)
    }

    fn report_control(
        &self,
        control_id: String,
        outcome: RunControlOutcome,
    ) -> Result<(), DriveFailure> {
        self.report(vec![ExecutionReport::RunControlApplied {
            application: RunControlApplication {
                control_id,
                outcome,
            },
        }])
    }

    fn report_applications(
        &self,
        applications: Vec<TriggerDeliveryApplication>,
    ) -> Result<(), DriveFailure> {
        self.report(
            applications
                .into_iter()
                .map(|application| ExecutionReport::TriggerDeliveryApplied { application })
                .collect(),
        )
    }

    /// Queues each application report in order.
    fn report(&self, reports: Vec<ExecutionReport>) -> Result<(), DriveFailure> {
        for report in reports {
            if !(self.progress.sink)(report) {
                return Err(DriveFailure::ReportUnqueued);
            }
        }
        Ok(())
    }

    /// Ends a run no process holds: the runner's shutdown and lease loss leave the record
    /// as it stands (`command::leaves_record`), and any other cancellation runs `useful
    /// workflow run cancel`, which ends a waiting or abandoned record `canceled`
    /// (REQ-LOCALRUN-186).
    async fn cancelled(
        &mut self,
        reason: CancellationReason,
        directory: &Path,
        record: TreeRecord,
    ) -> Result<TreeEnd, DriveFailure> {
        if command::leaves_record(reason) {
            return Ok(TreeEnd::of(Some(record), None, Some(reason)));
        }
        let _ = command::cancel_run(&self.run.useful, &self.run.runs).await;
        let record = match self.observe(directory.to_owned()).await {
            Some(observed) => {
                self.flush(&observed.record, observed.steps, Some(reason))?;
                Some(observed.record)
            }
            None => None,
        };
        Ok(TreeEnd::of(record, None, Some(reason)))
    }

    /// Before the loop ends on an exited record, reports the step observations that
    /// changed since the last report, so they precede the terminal report; after lease
    /// loss the assignment is no longer active and nothing is sent.
    fn flush(
        &mut self,
        record: &TreeRecord,
        steps: Vec<TreeStepObservation>,
        cancellation: Option<CancellationReason>,
    ) -> Result<(), DriveFailure> {
        if record.state != RecordState::Exited
            || cancellation == Some(CancellationReason::ExecutionLeaseExpired)
        {
            return Ok(());
        }
        self.progress.observe(self.run, record, None, steps)
    }

    /// Reads the run directory on a blocking worker: its record, its wait record while
    /// it waits, and its steps and its child runs' through the driver's step reader.
    async fn observe(&self, directory: PathBuf) -> Option<Observation> {
        let reader = Arc::clone(&self.steps);
        tokio::task::spawn_blocking(move || {
            let (record, wait) = read_state(&directory)?;
            let steps = reader
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .scan(&directory);
            Some(Observation {
                record,
                wait,
                steps,
            })
        })
        .await
        .ok()
        .flatten()
    }
}

/// The wait `action` names, which must be the run's current wait for it to apply.
fn named_wait(action: &RunControlAction) -> Option<&RunControlWait> {
    match action {
        RunControlAction::Answer { wait, .. }
        | RunControlAction::Approve { wait }
        | RunControlAction::HandBack { wait, .. }
        | RunControlAction::Retry { wait } => Some(wait),
        RunControlAction::Lift { wait, .. } => wait.as_ref(),
        RunControlAction::Stop { .. } | RunControlAction::Withdraw => None,
    }
}

/// Whether `outcome` applied a withdrawal `action` by recording it for the process that holds
/// the run, which ends the run `withdrawn` only if that process honors it.
fn recorded_withdrawal(action: &RunControlAction, outcome: &RunControlOutcome) -> bool {
    matches!(action, RunControlAction::Withdraw)
        && matches!(
            outcome,
            RunControlOutcome::Applied { action_outcome } if action_outcome == command::RECORDED
        )
}

/// The applications `apply_beside` made, in order, and whether a withdrawal among them was
/// applied `recorded`.
struct AppliedBeside {
    reports: Vec<ExecutionReport>,
    withdrawal_recorded: bool,
}

/// Applies `deliveries`, then `controls`, which name no wait, to the run at `directory`
/// beside a running process, returning their applications in that order; none for those the
/// assignment's `cancellation` abandoned.
async fn apply_beside(
    useful: &UsefulCommand,
    cancellation: &CancellationSource,
    directory: PathBuf,
    deliveries: Vec<TriggerDelivery>,
    controls: Vec<RunControl>,
) -> AppliedBeside {
    let mut reports =
        command::apply_deliveries(useful, directory.clone(), deliveries, cancellation)
            .await
            .into_iter()
            .map(|application| ExecutionReport::TriggerDeliveryApplied { application })
            .collect::<Vec<_>>();
    let mut withdrawal_recorded = false;
    for control in controls {
        let Some(outcome) =
            command::apply_control(useful, &directory, &control.action, cancellation).await
        else {
            break;
        };
        withdrawal_recorded |= recorded_withdrawal(&control.action, &outcome);
        reports.push(ExecutionReport::RunControlApplied {
            application: RunControlApplication {
                control_id: control.control_id,
                outcome,
            },
        });
    }
    AppliedBeside {
        reports,
        withdrawal_recorded,
    }
}

/// The home's only run directory, once `create` has made it.
async fn locate(runs: &Path) -> Option<PathBuf> {
    let runs = runs.to_owned();
    tokio::task::spawn_blocking(move || command::only_entry(&runs))
        .await
        .ok()
        .flatten()
}

/// How many writes the run's `mirror/` at `directory` holds, read on a blocking worker, or
/// the class of the failure to read it.
async fn count_mirror_writes(directory: PathBuf) -> Result<usize, &'static str> {
    match tokio::task::spawn_blocking(move || useful_execution::read_mirror_writes(&directory))
        .await
    {
        Ok(Ok(writes)) => Ok(writes.len()),
        Ok(Err(error)) => Err(mirror_read_failure(&error)),
        Err(_) => Err(MIRROR_READER_LOST),
    }
}

/// The run's record and, while it waits, its wait record.
fn read_state(directory: &Path) -> Option<(TreeRecord, Option<TreeWait>)> {
    let record = read_record(directory)?;
    let wait = (record.state == RecordState::Waiting)
        .then(|| read_wait(directory))
        .flatten();
    Some((record, wait))
}

/// Reads the run's record and, while it waits, its wait record, on a blocking worker.
async fn observe_record(directory: PathBuf) -> Option<(TreeRecord, Option<TreeWait>)> {
    tokio::task::spawn_blocking(move || read_state(&directory))
        .await
        .ok()
        .flatten()
}

/// Whether the run at `directory` stands: its record reads and does not read `exited`.
async fn standing(directory: &Path) -> bool {
    observe_record(directory.to_owned())
        .await
        .is_some_and(|(record, _)| record.state != RecordState::Exited)
}
