//! The runner viewer's host: it draws a serving runner's overview, asks its source for a
//! new one once a second off the thread that draws and reads keys, and keeps the last one
//! it was given while the runner does not answer (REQ-RUNNERENROLL-140, REQ-RUNNERENROLL-141,
//! REQ-RUNNERENROLL-143). Two keys send the runner an operation, also off that thread, and
//! the screen shows its outcome (REQ-RUNNERENROLL-142). Enter opens an assignment, which the
//! host asks for once a second and at once when the run or the step wanted changes, one read
//! at a time (REQ-RUNNERENROLL-146).

use std::fmt;

use super::archived::{
    ArchivedTerminalExitRequest, ArchivedTerminalHostExit, ViewerInteraction, ViewerTask,
    fail_archived_terminal, open_viewer, restore_archived_terminal, viewer_input,
};
use super::*;
use crate::workflow::run_timing::{ObservationClock, ObservationTime};

mod assignment;
mod interaction;
mod log_document;
mod run_steps;
#[cfg(test)]
mod tests;
mod view;
mod widgets;

use self::assignment::AssignmentScreen;
use self::interaction::{RunnerWatchInteraction, RunnerWatchSurface};
pub use self::view::{
    RunnerWatchAssignment, RunnerWatchAssignmentAnswer, RunnerWatchAssignmentDetail,
    RunnerWatchAssignmentQuery, RunnerWatchChildRun, RunnerWatchConnection, RunnerWatchCounts,
    RunnerWatchDisk, RunnerWatchEvent, RunnerWatchHeldAssignment, RunnerWatchLease, RunnerWatchLog,
    RunnerWatchLogRecord, RunnerWatchLogSource, RunnerWatchLogStream, RunnerWatchLogin,
    RunnerWatchMark, RunnerWatchOutcome, RunnerWatchOverview, RunnerWatchRecent, RunnerWatchRun,
    RunnerWatchRunElement, RunnerWatchRunOutput, RunnerWatchRunStep, RunnerWatchRunWork,
    RunnerWatchStep, RunnerWatchTitle, RunnerWatchWait,
};
use self::widgets::render_runner_watch;

/// How long the host waits after one request of a kind starts before it starts the next.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Why the newest request did not answer, as the last-known banner tells it
/// (REQ-RUNNERENROLL-141).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerWatchSourceError {
    /// The runner did not answer.
    NotReachable,
    /// Its answer could not be read.
    Unreadable,
    /// It is of a release that does not answer the request.
    EarlierRelease,
}

impl fmt::Display for RunnerWatchSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotReachable => "the runner did not answer",
            Self::Unreadable => "the runner's answer could not be read",
            Self::EarlierRelease => "the runner is of a release that does not answer this request",
        })
    }
}

impl std::error::Error for RunnerWatchSourceError {}

/// What the viewer reads a runner through. The host calls it only inside
/// `tokio::task::spawn_blocking` and never waits for it before leaving.
pub trait RunnerWatchSource: Send + Sync + 'static {
    /// Asks the runner for its overview, blocking until it answers or fails.
    fn overview(&self) -> Result<RunnerWatchOverview, RunnerWatchSourceError>;

    /// Asks the runner for one assignment, the run and the step `query` names, blocking
    /// until it answers or fails (REQ-RUNNERENROLL-144).
    fn assignment(
        &self,
        query: &RunnerWatchAssignmentQuery,
    ) -> Result<RunnerWatchAssignmentAnswer, RunnerWatchSourceError>;

    /// Asks the runner to reload its credential, blocking until it answers or fails.
    fn reload_credential(&self) -> RunnerWatchOutcome;

    /// Asks the runner to set up its agent logins, blocking until it answers or fails.
    fn set_up_agent_logins(&self) -> RunnerWatchOutcome;
}

/// An operation a key sends the runner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RunnerWatchOperation {
    ReloadCredential,
    SetUpAgentLogins,
}

impl RunnerWatchOperation {
    fn send<Source: RunnerWatchSource>(self, source: &Source) -> RunnerWatchOutcome {
        match self {
            Self::ReloadCredential => source.reload_credential(),
            Self::SetUpAgentLogins => source.set_up_agent_logins(),
        }
    }
}

/// The operation notice: an operation in flight since a moment, or the outcome of the last
/// one until it is dismissed or another starts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RunnerWatchNotice {
    InFlight {
        operation: RunnerWatchOperation,
        since: std::time::Instant,
    },
    Answered {
        operation: RunnerWatchOperation,
        outcome: RunnerWatchOutcome,
    },
}

pub struct RunnerWatchHost {
    task: ViewerTask,
}

impl RunnerWatchHost {
    /// Opens the viewer over the terminal with the overview the caller already has, so the
    /// first frame needs no request.
    pub fn start<Source, Clock>(
        source: Source,
        first: RunnerWatchOverview,
        clock: Clock,
        color: bool,
    ) -> Result<Self, PresentationFailure>
    where
        Source: RunnerWatchSource,
        Clock: ObservationClock,
    {
        Self::start_with_boundary(source, first, clock, color, SystemTerminalBoundary::new())
    }

    fn start_with_boundary<Source, Clock, Boundary>(
        source: Source,
        first: RunnerWatchOverview,
        clock: Clock,
        color: bool,
        boundary: Boundary,
    ) -> Result<Self, PresentationFailure>
    where
        Source: RunnerWatchSource,
        Clock: ObservationClock,
        Boundary: RunnerWatchBoundary,
    {
        let opened = clock.sample();
        let screen = RunnerWatchScreen::new(first, opened);
        let mut terminal = RestoringTerminal::new(boundary);
        let interaction = open_viewer(&mut terminal, |boundary, area| {
            let mut interaction = RunnerWatchInteraction::new(area);
            boundary.draw_runner_watch(&screen, &mut interaction, color)?;
            Ok(interaction)
        })?;
        let watch = RunnerWatchLoop {
            terminal,
            source: Arc::new(source),
            clock,
            screen,
            interaction,
            color,
            last_poll: opened.monotonic,
            last_assignment_poll: opened.monotonic,
            last_asked: None,
            last_drawn: opened.monotonic,
        };
        Ok(Self {
            task: ViewerTask::spawn(|exit| watch.run(exit)),
        })
    }

    pub fn exit_request(&self) -> ArchivedTerminalExitRequest {
        self.task.exit_request()
    }

    pub async fn wait(self) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
        self.task.wait().await
    }
}

/// What the viewer shows: the last overview answered, the open assignment as last
/// answered, why the newest request of either kind failed when it did, and when the runner
/// was last seen to restart.
pub(super) struct RunnerWatchScreen {
    pub(super) overview: RunnerWatchOverview,
    /// The open assignment, read by this host's own modules.
    assignment: Option<AssignmentScreen>,
    pub(super) failure: Option<RunnerWatchSourceError>,
    pub(super) last_answer: ObservationTime,
    pub(super) now: ObservationTime,
    pub(super) restarted_at: Option<time::OffsetDateTime>,
    pub(super) notice: Option<RunnerWatchNotice>,
}

impl RunnerWatchScreen {
    pub(super) fn new(overview: RunnerWatchOverview, now: ObservationTime) -> Self {
        Self {
            overview,
            assignment: None,
            failure: None,
            last_answer: now,
            now,
            restarted_at: None,
            notice: None,
        }
    }

    pub(super) fn operation_in_flight(&self) -> bool {
        matches!(self.notice, Some(RunnerWatchNotice::InFlight { .. }))
    }

    /// Takes a request's result at `now`: an answer replaces what is shown and clears the
    /// failure, and a failure keeps it. Answers whether the runner restarted.
    fn answer(
        &mut self,
        result: Result<RunnerWatchOverview, RunnerWatchSourceError>,
        now: ObservationTime,
    ) -> bool {
        self.now = now;
        match result {
            Ok(overview) => {
                let restarted = overview.boot_id != self.overview.boot_id;
                if restarted {
                    self.restarted_at = Some(now.utc);
                }
                self.overview = overview;
                self.failure = None;
                self.last_answer = now;
                restarted
            }
            Err(failure) => {
                self.failure = Some(failure);
                false
            }
        }
    }

    pub(super) fn since_last_answer(&self) -> Duration {
        self.now
            .monotonic
            .saturating_duration_since(self.last_answer.monotonic)
    }
}

impl ViewerInteraction for RunnerWatchInteraction {
    fn set_terminal_area(&mut self, area: Rect) {
        self.terminal_area = area;
    }

    fn help_visible(&self) -> bool {
        self.help_visible
    }
}

trait RunnerWatchBoundary: TerminalBoundary {
    fn draw_runner_watch(
        &mut self,
        screen: &RunnerWatchScreen,
        interaction: &mut RunnerWatchInteraction,
        color: bool,
    ) -> io::Result<()>;
}

impl RunnerWatchBoundary for SystemTerminalBoundary {
    fn draw_runner_watch(
        &mut self,
        screen: &RunnerWatchScreen,
        interaction: &mut RunnerWatchInteraction,
        color: bool,
    ) -> io::Result<()> {
        self.surface_mut()?
            .draw_runner_watch(screen, interaction, color)
    }
}

impl TerminalSurface {
    fn draw_runner_watch(
        &mut self,
        screen: &RunnerWatchScreen,
        interaction: &mut RunnerWatchInteraction,
        color: bool,
    ) -> io::Result<()> {
        self.terminal
            .draw(|frame| render_runner_watch(frame, screen, interaction, color))?;
        Ok(())
    }
}

type OverviewRequest = tokio::task::JoinHandle<Result<RunnerWatchOverview, RunnerWatchSourceError>>;
type OperationRequest = tokio::task::JoinHandle<RunnerWatchOutcome>;
/// An assignment read's answer with the query it was started with.
type AssignmentRead = (
    RunnerWatchAssignmentQuery,
    Result<RunnerWatchAssignmentAnswer, RunnerWatchSourceError>,
);
type AssignmentRequest = tokio::task::JoinHandle<AssignmentRead>;

struct RunnerWatchLoop<Boundary: TerminalBoundary, Source, Clock> {
    terminal: RestoringTerminal<Boundary>,
    source: Arc<Source>,
    clock: Clock,
    screen: RunnerWatchScreen,
    interaction: RunnerWatchInteraction,
    color: bool,
    last_poll: std::time::Instant,
    /// When the last assignment read started, and what it asked.
    last_assignment_poll: std::time::Instant,
    last_asked: Option<RunnerWatchAssignmentQuery>,
    last_drawn: std::time::Instant,
}

impl<Boundary, Source, Clock> RunnerWatchLoop<Boundary, Source, Clock>
where
    Boundary: RunnerWatchBoundary,
    Source: RunnerWatchSource,
    Clock: ObservationClock,
{
    async fn run(
        mut self,
        mut requested_exit: tokio::sync::mpsc::UnboundedReceiver<ArchivedTerminalHostExit>,
    ) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
        let mut redraw = redraw_interval();
        redraw.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The requests and the operation in flight are never awaited on the way out: the
        // caller's runtime abandons their workers.
        let mut request: Option<OverviewRequest> = None;
        let mut operation: Option<OperationRequest> = None;
        let mut assignment: Option<AssignmentRequest> = None;
        loop {
            let changed = tokio::select! {
                biased;
                exit = requested_exit.recv() => {
                    return restore_archived_terminal(
                        &mut self.terminal,
                        exit.unwrap_or(ArchivedTerminalHostExit::Terminated),
                    );
                }
                event = self.terminal.boundary.next_event() => {
                    let screen = &mut self.screen;
                    if let Some(ending) = viewer_input(
                        &mut self.terminal,
                        event,
                        &mut self.interaction,
                        |interaction, event| interaction.handle_key(event, screen),
                    ) {
                        return ending;
                    }
                    // While an operation is in flight either key starts nothing.
                    if let Some(asked) = self.interaction.requested.take()
                        && operation.is_none()
                    {
                        operation = Some(self.start_operation(asked));
                    }
                    true
                }
                answer = in_flight(&mut request), if request.is_some() => {
                    request = None;
                    self.answer(answer);
                    true
                }
                answered = in_flight(&mut operation), if operation.is_some() => {
                    operation = None;
                    self.operation_answered(answered);
                    true
                }
                read = in_flight(&mut assignment), if assignment.is_some() => {
                    assignment = None;
                    self.assignment_answered(read);
                    true
                }
                _ = redraw.tick() => self.tick(&mut request, &mut assignment),
            };
            if changed
                && let Err(error) = self.terminal.boundary.draw_runner_watch(
                    &self.screen,
                    &mut self.interaction,
                    self.color,
                )
            {
                return fail_archived_terminal(
                    &mut self.terminal,
                    PresentationFailureOperation::TerminalDraw,
                    &error,
                );
            }
            if changed {
                self.last_drawn = self.screen.now.monotonic;
            }
        }
    }

    /// Starts an overview request when none is in flight and one interval has passed since
    /// the last one started. While an assignment is open, starts its read when none is in
    /// flight and either one interval has passed since the last one started or the query
    /// wanted differs from the last one asked. Answers whether the frame's running time
    /// needs drawing.
    fn tick(
        &mut self,
        request: &mut Option<OverviewRequest>,
        assignment: &mut Option<AssignmentRequest>,
    ) -> bool {
        let now = self.clock.sample();
        self.screen.now = now;
        if request.is_none()
            && now.monotonic.saturating_duration_since(self.last_poll) >= POLL_INTERVAL
        {
            self.last_poll = now.monotonic;
            let source = Arc::clone(&self.source);
            *request = Some(tokio::task::spawn_blocking(move || source.overview()));
        }
        match self.wanted_query() {
            None => self.last_asked = None,
            Some(wanted)
                if assignment.is_none()
                    && (self.last_asked.as_ref() != Some(&wanted)
                        || now
                            .monotonic
                            .saturating_duration_since(self.last_assignment_poll)
                            >= POLL_INTERVAL) =>
            {
                self.last_assignment_poll = now.monotonic;
                self.last_asked = Some(wanted.clone());
                let source = Arc::clone(&self.source);
                *assignment = Some(tokio::task::spawn_blocking(move || {
                    let answer = source.assignment(&wanted);
                    (wanted, answer)
                }));
            }
            Some(_) => {}
        }
        (self.screen.failure.is_some() || self.screen.operation_in_flight())
            && now.monotonic.saturating_duration_since(self.last_drawn) >= POLL_INTERVAL
    }

    /// The assignment read the open surface wants, or none while the overview is shown or
    /// after the runner said it holds the assignment no longer.
    fn wanted_query(&self) -> Option<RunnerWatchAssignmentQuery> {
        let RunnerWatchSurface::Assignment(open) = &self.interaction.surface else {
            return None;
        };
        open.query(self.screen.assignment.as_ref()?)
    }

    /// Takes an assignment read's answer against the query it answered: a failure sets the
    /// banner and changes nothing else (REQ-RUNNERENROLL-141).
    fn assignment_answered(&mut self, read: Result<AssignmentRead, tokio::task::JoinError>) {
        let now = self.clock.sample();
        self.screen.now = now;
        // A worker that panicked or was cancelled left nothing that can be read.
        let (query, answer) = match read {
            Ok((query, Ok(answer))) => (query, answer),
            Ok((_, Err(failure))) => {
                self.screen.failure = Some(failure);
                return;
            }
            Err(_) => {
                self.screen.failure = Some(RunnerWatchSourceError::Unreadable);
                return;
            }
        };
        self.screen.failure = None;
        self.screen.last_answer = now;
        if let (RunnerWatchSurface::Assignment(open), Some(shown)) =
            (&mut self.interaction.surface, &mut self.screen.assignment)
        {
            shown.take(open, query, answer);
        }
    }

    /// Shows `operation` in flight and sends it off the thread that draws and reads keys.
    fn start_operation(&mut self, operation: RunnerWatchOperation) -> OperationRequest {
        let now = self.clock.sample();
        self.screen.now = now;
        self.screen.notice = Some(RunnerWatchNotice::InFlight {
            operation,
            since: now.monotonic,
        });
        let source = Arc::clone(&self.source);
        tokio::task::spawn_blocking(move || operation.send(&*source))
    }

    fn operation_answered(&mut self, answered: Result<RunnerWatchOutcome, tokio::task::JoinError>) {
        if let Some(RunnerWatchNotice::InFlight { operation, .. }) = self.screen.notice {
            // A worker that panicked or was cancelled left no outcome but that.
            let outcome = answered.unwrap_or_else(|_| {
                RunnerWatchOutcome::Refused(vec!["the request ended without an answer".to_owned()])
            });
            self.screen.notice = Some(RunnerWatchNotice::Answered { operation, outcome });
        }
    }

    fn answer(
        &mut self,
        answer: Result<Result<RunnerWatchOverview, RunnerWatchSourceError>, tokio::task::JoinError>,
    ) {
        // A worker that panicked or was cancelled left nothing that can be read.
        let result = answer.unwrap_or(Err(RunnerWatchSourceError::Unreadable));
        let selected = self
            .screen
            .overview
            .assignments
            .get(self.interaction.selected)
            .map(|assignment| assignment.assignment_id.clone());
        let restarted = self.screen.answer(result, self.clock.sample());
        if restarted {
            self.interaction.selected = 0;
            self.interaction.top = 0;
        } else if let Some(position) = selected.and_then(|selected| {
            self.screen
                .overview
                .assignments
                .iter()
                .position(|assignment| assignment.assignment_id == selected)
        }) {
            self.interaction.selected = position;
        }
    }
}

/// Waits for the worker in flight, or forever when there is none.
async fn in_flight<Answer>(
    request: &mut Option<tokio::task::JoinHandle<Answer>>,
) -> Result<Answer, tokio::task::JoinError> {
    match request {
        Some(request) => request.await,
        None => std::future::pending().await,
    }
}
