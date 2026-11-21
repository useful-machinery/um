//! `useful runner watch`: the live process's overview in a full-screen terminal interface
//! (REQ-RUNNERENROLL-139). This leaf checks the terminal, asks the serving process once and
//! opens the viewer's host over a source that reads the control socket; the mapping below is
//! where the protocol's words become the screen's.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::super::workflow::{
    execute_with_abandonable_runtime, first_view_signal, observe_workflow_signals,
    terminal_failure, viewer_exit_code,
};
use super::super::{CommandFailure, CommandResult};
use crate::exit_code::OutcomeClass;
use useful_execution::{
    ColorChoice, PresentationConfig, PresentationMode, RequestedPresentationMode,
    RunnerWatchAssignment, RunnerWatchAssignmentAnswer, RunnerWatchAssignmentDetail,
    RunnerWatchAssignmentQuery, RunnerWatchChildRun, RunnerWatchConnection, RunnerWatchCounts,
    RunnerWatchDisk, RunnerWatchEvent, RunnerWatchHeldAssignment, RunnerWatchHost,
    RunnerWatchLease, RunnerWatchLog, RunnerWatchLogRecord, RunnerWatchLogSource,
    RunnerWatchLogStream, RunnerWatchLogin, RunnerWatchMark, RunnerWatchOutcome,
    RunnerWatchOverview, RunnerWatchRecent, RunnerWatchRun, RunnerWatchRunElement,
    RunnerWatchRunOutput, RunnerWatchRunStep, RunnerWatchRunWork, RunnerWatchSource,
    RunnerWatchSourceError, RunnerWatchStep, RunnerWatchTitle, RunnerWatchWait,
    SystemObservationClock, TerminalCapabilities, format_bytes,
};
use useful_runner::{
    AgentLoginSetupResult, AgentLoginState, AgentLoginUse, AssignmentAnswer, AssignmentDetail,
    AssignmentLog, AssignmentLogSource, AssignmentLogStream, AssignmentPhase, AssignmentQuery,
    AssignmentRun, AssignmentStep, ConnectionState, ControlError, LiveStepState, OpenWord,
    Operation, Overview, OverviewAnswer, OverviewAssignment, OverviewDisk, OverviewEvent,
    OverviewProviderReadiness, OverviewRecentAssignment, OverviewRunElement, OverviewRunningStep,
    OverviewWait, ProviderReadiness, RequestFailure, Response, RunResult, RunnerEventKind,
    StepPathPart,
};

pub(super) const ABOUT: &str = "Watch live Runner Serve activity";

#[derive(Debug, Args)]
pub(super) struct Command {
    /// Read the control socket path from the closed runner operator
    /// configuration; the runner home under the useful home is the default, read
    /// without creating anything.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
}

impl Command {
    pub(super) fn execute(self) -> CommandResult {
        // REQ-RUNNERENROLL-139: the terminal is checked before anything is read or asked.
        let presentation = PresentationConfig {
            requested_mode: RequestedPresentationMode::Automatic,
            color: ColorChoice::Auto,
            capabilities: TerminalCapabilities::detect(),
            standard_input_reserved: false,
        };
        if presentation.mode() != PresentationMode::Tui {
            return Err(CommandFailure::new(anyhow!(
                "runner watch needs a terminal on standard input and output; run `useful runner status` to print the runner's state once"
            )));
        }
        let socket_path = super::configured_control_socket_path(self.config.as_deref())?;
        execute_with_abandonable_runtime(
            "runner",
            "watch",
            watch(socket_path, presentation.color_enabled()),
        )
    }
}

async fn watch(socket_path: PathBuf, color: bool) -> CommandResult {
    let (mut interrupt, mut terminate) = observe_workflow_signals("runner", "watch")?;
    let source = ControlSocketSource { socket_path };
    let first_path = source.socket_path.clone();
    let mut first =
        tokio::task::spawn_blocking(move || useful_runner::request_overview(&first_path));
    let answer = tokio::select! {
        biased;
        exit = first_view_signal(&mut interrupt, &mut terminate) => {
            return Ok(viewer_exit_code(exit));
        }
        answer = &mut first => answer.context("ask Runner Serve for its overview")?,
    };
    let first = first_view(answer)?;

    let host = RunnerWatchHost::start(source, first, SystemObservationClock, color)
        .map_err(anyhow::Error::new)
        .context("start runner watch terminal")?;
    let request = host.exit_request();
    let mut waiting = Box::pin(host.wait());
    let result = tokio::select! {
        biased;
        exit = first_view_signal(&mut interrupt, &mut terminate) => {
            request.request(exit);
            waiting.await
        }
        result = &mut waiting => result,
    };
    match result {
        Ok(exit) => Ok(viewer_exit_code(exit)),
        Err(failure) => Err(terminal_failure("runner watch", &failure).into()),
    }
}

/// The first answer, which decides whether the viewer opens (REQ-RUNNERENROLL-139): a
/// runner that does not answer is unreachable, one of an earlier release says so, and any
/// other answer that is not an overview is a protocol failure.
fn first_view(
    answer: Result<OverviewAnswer, RequestFailure>,
) -> Result<RunnerWatchOverview, CommandFailure> {
    match answer {
        Ok(OverviewAnswer::Overview(overview)) => view(*overview).ok_or_else(|| {
            CommandFailure::for_outcome(
                anyhow!("Runner Serve answered an overview whose times cannot be read"),
                OutcomeClass::Protocol,
            )
        }),
        // Only a serving process older than this release refuses the well-formed request.
        Ok(OverviewAnswer::Refused(ControlError::InvalidRequest)) => {
            Err(CommandFailure::new(anyhow!(
                "Runner Serve does not answer `useful runner watch` (invalid_request); restart the running `useful runner serve` on this release"
            )))
        }
        Ok(OverviewAnswer::Refused(error)) => Err(CommandFailure::for_outcome(
            anyhow!("Runner Serve refused the overview ({})", known_word(error)),
            OutcomeClass::Protocol,
        )),
        Err(error) => {
            let outcome = match error {
                RequestFailure::NotReachable => OutcomeClass::Unreachable,
                RequestFailure::Protocol(_) => OutcomeClass::Protocol,
            };
            Err(CommandFailure::for_outcome(
                anyhow::Error::new(error),
                outcome,
            ))
        }
    }
}

/// The viewer's source: the control socket of the serving process. Each call blocks on
/// the socket, and the host makes it only off the thread that draws.
struct ControlSocketSource {
    socket_path: PathBuf,
}

impl RunnerWatchSource for ControlSocketSource {
    fn overview(&self) -> Result<RunnerWatchOverview, RunnerWatchSourceError> {
        overview_view(useful_runner::request_overview(&self.socket_path))
    }

    fn assignment(
        &self,
        query: &RunnerWatchAssignmentQuery,
    ) -> Result<RunnerWatchAssignmentAnswer, RunnerWatchSourceError> {
        assignment_view(useful_runner::request_assignment(
            &self.socket_path,
            &assignment_query(query),
        ))
    }

    fn reload_credential(&self) -> RunnerWatchOutcome {
        reload_outcome(useful_runner::request(
            &self.socket_path,
            Operation::ReloadCredential,
        ))
    }

    fn set_up_agent_logins(&self) -> RunnerWatchOutcome {
        setup_outcome(useful_runner::request(
            &self.socket_path,
            Operation::SyncAgentLogins,
        ))
    }
}

/// A later answer, as the banner tells the three failures apart (REQ-RUNNERENROLL-141).
fn overview_view(
    answer: Result<OverviewAnswer, RequestFailure>,
) -> Result<RunnerWatchOverview, RunnerWatchSourceError> {
    match answer {
        Ok(OverviewAnswer::Overview(overview)) => {
            view(*overview).ok_or(RunnerWatchSourceError::Unreadable)
        }
        Ok(OverviewAnswer::Refused(ControlError::InvalidRequest)) => {
            Err(RunnerWatchSourceError::EarlierRelease)
        }
        Ok(OverviewAnswer::Refused(_)) | Err(RequestFailure::Protocol(_)) => {
            Err(RunnerWatchSourceError::Unreadable)
        }
        Err(RequestFailure::NotReachable) => Err(RunnerWatchSourceError::NotReachable),
    }
}

/// The view's query as the protocol's (REQ-RUNNERENROLL-144).
fn assignment_query(query: &RunnerWatchAssignmentQuery) -> AssignmentQuery {
    AssignmentQuery {
        assignment_id: query.assignment_id.clone(),
        run: query
            .path
            .iter()
            .map(|element| OverviewRunElement {
                step: element.step.clone(),
                item: element.item,
                ordinal: element.ordinal,
            })
            .collect(),
        step: query.step.clone(),
    }
}

/// An assignment read's answer, its failures told apart as the overview's are
/// (REQ-RUNNERENROLL-141, REQ-RUNNERENROLL-144).
fn assignment_view(
    answer: Result<AssignmentAnswer, RequestFailure>,
) -> Result<RunnerWatchAssignmentAnswer, RunnerWatchSourceError> {
    match answer {
        Ok(AssignmentAnswer::Assignment(detail)) => Ok(RunnerWatchAssignmentAnswer::Held(
            Box::new(held_assignment(*detail)),
        )),
        Ok(AssignmentAnswer::NotHeld) => Ok(RunnerWatchAssignmentAnswer::NotHeld),
        Ok(AssignmentAnswer::Refused(ControlError::InvalidRequest)) => {
            Err(RunnerWatchSourceError::EarlierRelease)
        }
        Ok(AssignmentAnswer::Refused(_)) | Err(RequestFailure::Protocol(_)) => {
            Err(RunnerWatchSourceError::Unreadable)
        }
        Err(RequestFailure::NotReachable) => Err(RunnerWatchSourceError::NotReachable),
    }
}

/// An assignment as the viewer draws it: the entry as the overview's, and the run's and
/// the log's engine words passed on as received (REQ-RUNNERENROLL-135).
fn held_assignment(detail: AssignmentDetail) -> RunnerWatchHeldAssignment {
    let AssignmentDetail { entry, run, log } = detail;
    RunnerWatchHeldAssignment {
        entry: assignment(entry),
        run: run.map(assignment_run),
        log: log.map(assignment_log),
    }
}

fn assignment_run(run: AssignmentRun) -> RunnerWatchRun {
    RunnerWatchRun {
        path: run
            .path
            .into_iter()
            .map(|element| RunnerWatchRunElement {
                step: element.step,
                item: element.item,
                ordinal: element.ordinal,
            })
            .collect(),
        workflow_path: run.workflow_path,
        state: run.state,
        outcome: run.outcome,
        attempt_number: run.attempt_number,
        attempts: run.attempts,
        attempt_state: run.attempt_state,
        wait: run.wait.map(wait),
        steps: run.steps.into_iter().map(assignment_step).collect(),
        steps_omitted: run.steps_omitted,
    }
}

/// A step, running what its one work member says, or nothing known without one.
fn assignment_step(step: AssignmentStep) -> RunnerWatchRunStep {
    let work = match (step.command, step.agent, step.workflow) {
        (Some(command), _, _) => RunnerWatchRunWork::Command {
            argv: command.argv,
            cwd: command.cwd,
        },
        (None, Some(agent), _) => RunnerWatchRunWork::Agent {
            profile: agent.profile,
            harness: agent.harness,
            model: agent.model,
            effort: agent.effort,
        },
        (None, None, Some(workflow)) => RunnerWatchRunWork::Workflow {
            file: workflow.file,
        },
        (None, None, None) => RunnerWatchRunWork::Unknown,
    };
    RunnerWatchRunStep {
        id: step.id,
        role: step.role,
        kind: step.kind,
        state: step.state,
        failure_policy: step.failure_policy,
        direct_dependencies: step.needs,
        // The reader did not validate it: one that does not parse is left out.
        started_at: step.started_at.as_deref().and_then(utc),
        duration: step.duration_milliseconds.map(Duration::from_millis),
        work,
        outputs: step
            .outputs
            .into_iter()
            .map(|output| RunnerWatchRunOutput {
                name: output.name,
                kind: output.kind,
                committed: output.committed,
            })
            .collect(),
        child_runs: step
            .child_runs
            .into_iter()
            .map(|child| RunnerWatchChildRun {
                ordinal: child.ordinal,
                item: child.item,
                state: child.state,
                outcome: child.outcome,
            })
            .collect(),
        child_runs_omitted: step.child_runs_omitted,
    }
}

/// A log by its source: a step's output with the streams it has, the run log with its one
/// unnamed stream, a step's records with the counts of the older ones left out and no
/// streams, or, under a source this release does not know, whatever text it carries.
fn assignment_log(log: AssignmentLog) -> RunnerWatchLog {
    let AssignmentLog {
        source,
        step,
        invocation_id,
        stdout,
        stderr,
        earlier_invocations_omitted,
        text,
        omitted_bytes,
        records,
        omitted_records,
    } = log;
    let source = match source {
        OpenWord::Known(AssignmentLogSource::Step) => RunnerWatchLogSource::Step {
            step: step.unwrap_or_default(),
            invocation: invocation_id,
            earlier_invocations_omitted,
        },
        OpenWord::Known(AssignmentLogSource::RunLog) => RunnerWatchLogSource::RunLog,
        OpenWord::Known(AssignmentLogSource::StepRecords) => {
            return RunnerWatchLog {
                source: RunnerWatchLogSource::StepRecords {
                    step,
                    omitted_records,
                    omitted_bytes: omitted_bytes.unwrap_or(0),
                },
                streams: Vec::new(),
                records: records
                    .into_iter()
                    .map(|record| RunnerWatchLogRecord {
                        source: record.source,
                        text: record.text,
                        continuation: record.continuation,
                    })
                    .collect(),
            };
        }
        OpenWord::Unknown(word) => RunnerWatchLogSource::Unknown(word),
    };
    let named = |name: &str, stream: AssignmentLogStream| RunnerWatchLogStream {
        name: Some(name.to_owned()),
        retained_bytes: Some(stream.retained_bytes),
        discarded_bytes: Some(stream.discarded_bytes),
        omitted_bytes: stream.omitted_bytes,
        text: stream.text,
    };
    let mut streams = Vec::new();
    streams.extend(stdout.map(|stream| named("stdout", stream)));
    streams.extend(stderr.map(|stream| named("stderr", stream)));
    streams.extend(text.map(|text| RunnerWatchLogStream {
        name: None,
        retained_bytes: None,
        discarded_bytes: None,
        omitted_bytes: omitted_bytes.unwrap_or(0),
        text,
    }));
    RunnerWatchLog {
        source,
        streams,
        records: Vec::new(),
    }
}

/// How a credential reload was answered (REQ-RUNNERENROLL-142).
fn reload_outcome(answer: Result<Response, RequestFailure>) -> RunnerWatchOutcome {
    match answer {
        Ok(Response::Reloaded { credential_id }) => {
            RunnerWatchOutcome::Done(vec![format!("credential now {credential_id}")])
        }
        answer => refusal(answer),
    }
}

/// How an agent-login setup was answered: each provider's readiness, and each check that
/// gave no answer with its cause (REQ-RUNNERENROLL-142, REQ-GRANT-41).
fn setup_outcome(answer: Result<Response, RequestFailure>) -> RunnerWatchOutcome {
    match answer {
        Ok(Response::AgentLoginsSynced(synced)) => {
            let mut facts = synced
                .readiness
                .providers
                .iter()
                .map(|entry| login_fact(&known_login(entry)))
                .collect::<Vec<_>>();
            facts.extend(synced.unanswered_checks.iter().map(|check| {
                format!(
                    "{} check gave no answer ({})",
                    check.provider.as_str(),
                    check.cause.as_str()
                )
            }));
            if facts.is_empty() {
                facts.push("no provider is authorized for the runner".to_owned());
            }
            RunnerWatchOutcome::Done(facts)
        }
        answer => refusal(answer),
    }
}

/// An operation that was not done: the closed error in words with its code, a runner that
/// did not answer, or an answer that does not fit what was asked.
fn refusal(answer: Result<Response, RequestFailure>) -> RunnerWatchOutcome {
    RunnerWatchOutcome::Refused(match answer {
        Ok(Response::Error(error)) => control_refusal(error),
        Err(RequestFailure::NotReachable) => vec!["the runner did not answer".to_owned()],
        Ok(_) | Err(RequestFailure::Protocol(_)) => {
            vec!["the runner's answer could not be read".to_owned()]
        }
    })
}

/// A closed error (REQ-RUNNERENROLL-82) as short facts, the first ending in its code.
fn control_refusal(error: ControlError) -> Vec<String> {
    let code = known_word(error);
    let (what, remedy) = match error {
        ControlError::InvalidRequest => (
            "the runner's release does not offer this",
            Some("restart `useful runner serve` on this release"),
        ),
        ControlError::UnsupportedVersion => ("the runner does not read this request", None),
        ControlError::RunnerBusy => (
            "a run the runner holds is working",
            Some("retry once every run waits or has ended"),
        ),
        ControlError::NoPendingCredential => ("no pending credential to reload", None),
        ControlError::PendingRegistrationMismatch => ("another runner's credential", None),
        ControlError::PendingAuthenticationFailed => ("Cloud refused the credential", None),
        ControlError::PendingProtocolFailed => ("Cloud's answer could not be read", None),
        ControlError::PendingConnectionFailed => ("the runner could not reach Cloud", None),
        ControlError::StateUpdateFailed => ("the runner could not update its state", None),
    };
    let mut facts = vec![format!("{what} ({code})")];
    facts.extend(remedy.map(str::to_owned));
    facts
}

/// The overview as the viewer draws it (REQ-RUNNERENROLL-140): every word passed on as
/// received (REQ-RUNNERENROLL-135). `None` when a time the reader validated does not parse.
fn view(overview: Overview) -> Option<RunnerWatchOverview> {
    let Overview {
        runner,
        disk,
        connection,
        current_credential_id,
        pending_credential_id,
        agent_logins,
        assignment_counts: counts,
        unacknowledged_reports,
        assignments,
        assignments_omitted,
        recent_assignments,
        recent_assignments_omitted,
        events,
        events_omitted,
    } = overview;
    let connected = matches!(
        connection.state,
        OpenWord::Known(ConnectionState::Connected)
    );
    let last_connected_at = match connection.last_connected_at.as_deref() {
        Some(at) => Some(utc(at)?),
        None => None,
    };
    Some(RunnerWatchOverview {
        runner_id: runner.runner_id,
        boot_id: runner.boot_id,
        version: runner.version,
        uptime: Duration::from_millis(runner.uptime_milliseconds),
        disk: disk.as_ref().map(disk_view),
        stop_time_left: runner
            .stop_time_left_milliseconds
            .map(Duration::from_millis),
        connection: RunnerWatchConnection {
            mark: connection_mark(&connection.state),
            state: word(&connection.state),
            connected,
            last_connected_at,
            last_failure: connection.last_failure.as_ref().map(word),
            attempts: connection.attempts_since_connected,
            next_attempt_in: connection
                .next_attempt_in_milliseconds
                .map(Duration::from_millis),
        },
        reports_waiting: unacknowledged_reports,
        current_credential_id,
        pending_credential_id,
        logins: agent_logins
            .map(|logins| logins.providers.iter().map(login).collect())
            .unwrap_or_default(),
        counts: RunnerWatchCounts {
            preparing: counts.preparing,
            accepted: counts.accepted,
            running: counts.running,
            waiting: counts.waiting,
            finishing: counts.finishing,
            reporting: counts.reporting,
        },
        assignments: assignments.into_iter().map(assignment).collect(),
        assignments_omitted,
        recent: recent_assignments
            .into_iter()
            .map(recent)
            .collect::<Option<_>>()?,
        recent_omitted: recent_assignments_omitted,
        events: events.iter().map(event).collect::<Option<_>>()?,
        events_omitted,
    })
}

/// The disk reading's sizes and state; its path and time are the status command's to give.
fn disk_view(disk: &OverviewDisk) -> RunnerWatchDisk {
    RunnerWatchDisk {
        available_bytes: disk.available_bytes,
        total_bytes: disk.total_bytes,
        minimum_free_bytes: disk.minimum_free_bytes,
        low: disk.low,
    }
}

fn connection_mark(state: &OpenWord<ConnectionState>) -> RunnerWatchMark {
    match state {
        OpenWord::Known(ConnectionState::Connected) => RunnerWatchMark::Active,
        OpenWord::Known(ConnectionState::Connecting | ConnectionState::BackingOff) => {
            RunnerWatchMark::Pending
        }
        OpenWord::Known(
            ConnectionState::AuthenticationFailed | ConnectionState::ProtocolFailed,
        ) => RunnerWatchMark::Failure,
        OpenWord::Known(ConnectionState::Stopping) | OpenWord::Unknown(_) => {
            RunnerWatchMark::Neutral
        }
    }
}

fn login(entry: &OverviewProviderReadiness) -> RunnerWatchLogin {
    match entry {
        OverviewProviderReadiness::Known(entry) => known_login(entry),
        // An entry this release cannot read keeps its two words.
        OverviewProviderReadiness::Unknown { provider, state } => {
            let ready = state == AgentLoginState::Ready.as_str();
            RunnerWatchLogin {
                provider: provider.clone(),
                ready,
                reason: (!ready).then(|| state.clone()),
            }
        }
    }
}

/// A login is ready when its state is `ready` and its last use was not refused; the reason
/// of one that is not is its failure code, or else its state word.
fn known_login(entry: &ProviderReadiness) -> RunnerWatchLogin {
    let ready =
        entry.state == AgentLoginState::Ready && entry.last_use != Some(AgentLoginUse::Refused);
    RunnerWatchLogin {
        provider: entry.provider.as_str().to_owned(),
        ready,
        reason: (!ready).then(|| {
            entry
                .failure_code()
                .unwrap_or_else(|| entry.state.as_str())
                .to_owned()
        }),
    }
}

fn login_fact(login: &RunnerWatchLogin) -> String {
    match &login.reason {
        Some(reason) if !login.ready => format!("{} not ready ({reason})", login.provider),
        _ => format!("{} ready", login.provider),
    }
}

fn assignment(entry: OverviewAssignment) -> RunnerWatchAssignment {
    let mark = match entry.phase {
        OpenWord::Known(AssignmentPhase::Running) => RunnerWatchMark::Active,
        OpenWord::Known(AssignmentPhase::Waiting) => RunnerWatchMark::Pending,
        _ => RunnerWatchMark::Neutral,
    };
    let detail = match entry.wait {
        Some(found) => RunnerWatchAssignmentDetail::Wait(wait(found)),
        None if !entry.running_steps.is_empty() || entry.running_steps_omitted > 0 => {
            RunnerWatchAssignmentDetail::Steps {
                steps: entry.running_steps.iter().map(running_step).collect(),
                omitted: entry.running_steps_omitted,
            }
        }
        None => RunnerWatchAssignmentDetail::None,
    };
    RunnerWatchAssignment {
        assignment_id: entry.assignment_id,
        title: title(entry.subject, entry.run_id.clone()),
        run_id: entry.run_id,
        attempt_number: entry.attempt_number,
        continues_earlier_attempt: entry.continues_earlier_attempt,
        run_url: entry.run_url,
        workflow_path: entry.workflow_path,
        phase: word(&entry.phase),
        mark,
        held: Duration::from_millis(entry.held_milliseconds),
        lease: entry.lease.map(|lease| RunnerWatchLease {
            health: word(&lease.health),
            time_left: Duration::from_millis(lease.time_left_milliseconds),
        }),
        detail,
    }
}

fn wait(wait: OverviewWait) -> RunnerWatchWait {
    RunnerWatchWait {
        kind: wait.kind,
        step: step_path(&wait.step),
        // A time the reader has not validated: one that does not parse is left out.
        since: wait.since.as_deref().and_then(utc),
    }
}

fn title(subject: Option<String>, run_id: String) -> RunnerWatchTitle {
    subject.map_or(RunnerWatchTitle::RunId(run_id), RunnerWatchTitle::Subject)
}

/// A step path's parts, an element number joined to the step before it in brackets.
fn step_path(parts: &[StepPathPart]) -> Vec<String> {
    let mut path: Vec<String> = Vec::with_capacity(parts.len());
    for part in parts {
        match (part, path.last_mut()) {
            (StepPathPart::Name(name), _) => path.push(name.clone()),
            (StepPathPart::Number(number), Some(last)) => last.push_str(&format!("[{number}]")),
            (StepPathPart::Number(number), None) => path.push(format!("[{number}]")),
        }
    }
    path
}

/// A running step's path: its run's elements, each with its item in brackets where it has
/// one, and the step, with its state in parentheses unless it is `running`.
fn running_step(step: &OverviewRunningStep) -> RunnerWatchStep {
    let mut path = step
        .run
        .iter()
        .map(|element| match element.item {
            Some(item) => format!("{}[{item}]", element.step),
            None => element.step.clone(),
        })
        .collect::<Vec<_>>();
    path.push(match &step.state {
        OpenWord::Known(LiveStepState::Running) => step.step.clone(),
        state => format!("{} ({})", step.step, word(state)),
    });
    RunnerWatchStep { path }
}

fn recent(entry: OverviewRecentAssignment) -> Option<RunnerWatchRecent> {
    Some(RunnerWatchRecent {
        assignment_id: entry.assignment_id,
        ended_at: utc(&entry.ended_at)?,
        succeeded: matches!(entry.result, OpenWord::Known(RunResult::Succeeded)),
        result: word(&entry.result),
        detail: entry
            .chosen_outcome
            .or(entry.failure_code)
            .or(entry.interruption_cause),
        duration: Duration::from_millis(entry.held_milliseconds),
        title: title(entry.subject, entry.run_id),
        workflow_path: entry.workflow_path,
    })
}

fn event(entry: &OverviewEvent) -> Option<RunnerWatchEvent> {
    let (text, mark) = event_text(entry);
    Some(RunnerWatchEvent {
        at: utc(&entry.at)?,
        text,
        count: entry.count,
        mark,
    })
}

/// One event in words, naming its facts and keeping each code as the code
/// (REQ-RUNNERENROLL-138); a kind this release does not know is its own word.
fn event_text(entry: &OverviewEvent) -> (String, RunnerWatchMark) {
    let run = |what: &str| {
        entry
            .run_id
            .as_deref()
            .map_or_else(|| what.to_owned(), |run_id| format!("{what}, {run_id}"))
    };
    let kind = match &entry.kind {
        OpenWord::Known(kind) => *kind,
        OpenWord::Unknown(word) => return (word.clone(), RunnerWatchMark::Neutral),
    };
    match kind {
        RunnerEventKind::Connected => ("connected".to_owned(), RunnerWatchMark::Success),
        RunnerEventKind::ConnectionFailed => {
            let mut text = "connection failed".to_owned();
            if let Some(failure) = &entry.failure {
                text.push_str(&format!(" ({})", word(failure)));
            }
            if let Some(next) = entry.next_attempt_in_milliseconds {
                text.push_str(&format!(", next attempt in {}", seconds(next)));
            }
            (text, RunnerWatchMark::Failure)
        }
        RunnerEventKind::CredentialReloaded => (
            entry.credential_id.as_deref().map_or_else(
                || "credential reloaded".to_owned(),
                |id| format!("credential reloaded, now {id}"),
            ),
            RunnerWatchMark::Success,
        ),
        RunnerEventKind::CredentialReloadFailed => (
            entry.error.as_ref().map_or_else(
                || "credential reload failed".to_owned(),
                |error| format!("credential reload failed ({})", word(error)),
            ),
            RunnerWatchMark::Failure,
        ),
        RunnerEventKind::AgentLoginsSetUp => {
            let (mut text, mark) = match &entry.setup_result {
                Some(OpenWord::Known(AgentLoginSetupResult::Finished)) => {
                    ("agent logins set up".to_owned(), RunnerWatchMark::Success)
                }
                Some(OpenWord::Known(AgentLoginSetupResult::RunnerBusy)) => (
                    "agent-login setup refused (runner_busy)".to_owned(),
                    RunnerWatchMark::Failure,
                ),
                Some(OpenWord::Known(AgentLoginSetupResult::Failed)) => (
                    "agent-login setup failed".to_owned(),
                    RunnerWatchMark::Failure,
                ),
                Some(OpenWord::Unknown(result)) => (
                    format!("agent-login setup {result}"),
                    RunnerWatchMark::Neutral,
                ),
                None => ("agent-login setup".to_owned(), RunnerWatchMark::Neutral),
            };
            if let Some(cause) = &entry.setup_cause {
                text.push_str(&format!(", asked by {}", word(cause)));
            }
            (text, mark)
        }
        RunnerEventKind::LeaseLost => (run("granted time lost"), RunnerWatchMark::Failure),
        RunnerEventKind::RunContinued => (run("run continued"), RunnerWatchMark::Neutral),
        RunnerEventKind::WorkspacesFound => (
            match entry.workspaces {
                Some(1) => "found 1 workspace of an earlier process".to_owned(),
                Some(count) => format!("found {count} workspaces of an earlier process"),
                None => "found workspaces of an earlier process".to_owned(),
            },
            RunnerWatchMark::Neutral,
        ),
        RunnerEventKind::ObservationRefused => (
            "report refused, queue full".to_owned(),
            RunnerWatchMark::Failure,
        ),
        RunnerEventKind::TelemetryDropped => (
            "telemetry record dropped".to_owned(),
            RunnerWatchMark::Failure,
        ),
        RunnerEventKind::DiskSpaceLow => {
            let mut text = "disk low".to_owned();
            if let Some(available) = entry.available_bytes {
                text.push_str(&format!(", {} free", format_bytes(available)));
            }
            if let Some(minimum) = entry.minimum_free_bytes {
                text.push_str(&format!(", minimum {}", format_bytes(minimum)));
            }
            (text, RunnerWatchMark::Failure)
        }
        RunnerEventKind::DiskSpaceRecovered => (
            entry.available_bytes.map_or_else(
                || "disk recovered".to_owned(),
                |available| format!("disk recovered, {} free", format_bytes(available)),
            ),
            RunnerWatchMark::Success,
        ),
        RunnerEventKind::DiskLowOfferDeclined => {
            (run("run declined, disk low"), RunnerWatchMark::Failure)
        }
        RunnerEventKind::Stopping => ("stopping".to_owned(), RunnerWatchMark::Neutral),
    }
}

/// A short wait in whole seconds, rounded up, or minutes past one.
fn seconds(milliseconds: u64) -> String {
    let seconds = milliseconds.div_ceil(1_000);
    if seconds < 60 {
        format!("{seconds}s")
    } else {
        format!("{}m", seconds / 60)
    }
}

fn utc(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

/// A vocabulary word as received: the known word as the wire spells it, or the string.
fn word<T: Serialize>(word: &OpenWord<T>) -> String {
    match word {
        OpenWord::Known(known) => known_word(known),
        OpenWord::Unknown(word) => word.clone(),
    }
}

fn known_word(value: impl Serialize) -> String {
    super::status::enum_json(value).unwrap_or_else(|_| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    use useful_runner::{
        AgentLoginCheckCause, AgentLoginProvider, AgentLoginReadiness, ProtocolFailure,
        SyncedAgentLogins, UnansweredCheck,
    };

    fn fixture_document() -> serde_json::Value {
        fixture_named("overview-response.json")
    }

    fn fixture_named(name: &str) -> serde_json::Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/runner-control/v1/valid")
            .join(name);
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    fn held_view(document: &serde_json::Value) -> RunnerWatchHeldAssignment {
        let detail: AssignmentDetail =
            serde_json::from_value(document["assignment"].clone()).unwrap();
        match assignment_view(Ok(AssignmentAnswer::Assignment(Box::new(detail)))) {
            Ok(RunnerWatchAssignmentAnswer::Held(held)) => *held,
            other => panic!("expected a held answer: {other:?}"),
        }
    }

    // REQ-RUNNERENROLL-144, REQ-RUNNERENROLL-135: an answer's entry, run and log map as
    // received, each engine word passed on as the word.
    #[test]
    fn the_fixture_assignment_maps_to_its_entry_run_and_step_output() {
        let document = fixture_named("assignment-response.json");
        let held = held_view(&document);
        let entry = &document["assignment"]["entry"];

        assert_eq!(held.entry.run_id, entry["runId"].as_str().unwrap());
        assert_eq!(held.entry.attempt_number, 2);
        assert!(held.entry.continues_earlier_attempt);
        assert_eq!(held.entry.run_url.as_deref(), entry["runUrl"].as_str());

        let run = held.run.expect("the fixture's run");
        let words = run
            .steps
            .iter()
            .map(|step| (step.id.as_str(), step.kind.as_str(), step.state.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            words,
            [
                ("plan", "cmd", "succeeded"),
                ("implement", "workflow", "running"),
                ("review", "agent", "pending"),
            ]
        );
        assert_eq!(
            run.steps[0].work,
            RunnerWatchRunWork::Command {
                argv: vec!["scripts/plan".to_owned(), "--all".to_owned()],
                cwd: Some("tools".to_owned()),
            }
        );
        assert_eq!(
            run.steps[1].work,
            RunnerWatchRunWork::Workflow {
                file: "implement.yaml".to_owned(),
            }
        );
        assert_eq!(
            run.steps[1].child_runs.last(),
            Some(&RunnerWatchChildRun {
                ordinal: 1,
                item: Some(1),
                state: "running".to_owned(),
                outcome: None,
            })
        );
        assert_eq!(run.steps[1].child_runs_omitted, 2);
        assert_eq!(
            run.steps[2].work,
            RunnerWatchRunWork::Agent {
                profile: "reviewer".to_owned(),
                harness: "claude_code".to_owned(),
                model: Some("claude-opus-4".to_owned()),
                effort: Some("high".to_owned()),
            }
        );

        let log = held.log.expect("the fixture's log");
        assert_eq!(
            log.source,
            RunnerWatchLogSource::Step {
                step: "plan".to_owned(),
                invocation: Some(3),
                earlier_invocations_omitted: 1,
            }
        );
        assert_eq!(
            log.streams.first(),
            Some(&RunnerWatchLogStream {
                name: Some("stdout".to_owned()),
                retained_bytes: Some(9000),
                discarded_bytes: Some(12),
                omitted_bytes: 8989,
                text: "plan ready\n".to_owned(),
            })
        );
        assert_eq!(log.streams.len(), 2);
    }

    // REQ-RUNNERENROLL-144: the run log is one unnamed stream with the bytes left out, and
    // an entry alone has neither run nor log.
    #[test]
    fn a_run_log_and_an_entry_alone_map_as_given() {
        let run_log = held_view(&fixture_named("assignment-run-log-response.json"));
        assert_eq!(run_log.run, None);
        assert_eq!(
            run_log.log,
            Some(RunnerWatchLog {
                source: RunnerWatchLogSource::RunLog,
                streams: vec![RunnerWatchLogStream {
                    name: None,
                    retained_bytes: None,
                    discarded_bytes: None,
                    omitted_bytes: 4096,
                    text: "resumed the run\nwaiting for the queue\n".to_owned(),
                }],
                records: Vec::new(),
            })
        );

        let entry = held_view(&fixture_named("assignment-entry-response.json"));
        assert_eq!((entry.run, entry.log), (None, None));
    }

    // REQ-RUNNERENROLL-146: a step's records map to the view's records log with the step,
    // the records in order and both counts, and no streams.
    #[test]
    fn a_step_records_log_maps_with_its_step_records_and_counts() {
        let held = held_view(&fixture_named("assignment-step-records-response.json"));
        let record = |source: &str, text: &str, continuation| RunnerWatchLogRecord {
            source: source.to_owned(),
            text: text.to_owned(),
            continuation,
        };
        assert_eq!(
            held.log,
            Some(RunnerWatchLog {
                source: RunnerWatchLogSource::StepRecords {
                    step: Some("build".to_owned()),
                    omitted_records: 12,
                    omitted_bytes: 3400,
                },
                streams: Vec::new(),
                records: vec![
                    record("stdout", "compiling core", false),
                    record("stderr", "warning: unused import", false),
                    record("stdout", " in core/src/lib.rs", true),
                ],
            })
        );
    }

    // REQ-RUNNERENROLL-146: a records log of `source` alone, of a run that has no run log,
    // maps to a records log with no step and no records.
    #[test]
    fn a_step_records_log_of_its_source_alone_maps_with_no_step() {
        let held = held_view(&fixture_named("assignment-step-records-none-response.json"));
        assert_eq!(
            held.log,
            Some(RunnerWatchLog {
                source: RunnerWatchLogSource::StepRecords {
                    step: None,
                    omitted_records: 0,
                    omitted_bytes: 0,
                },
                streams: Vec::new(),
                records: Vec::new(),
            })
        );
    }

    // REQ-RUNNERENROLL-141, REQ-RUNNERENROLL-144: not held, an earlier release, an
    // unreadable answer and no answer are told apart.
    #[test]
    fn assignment_failures_map_to_the_three_errors() {
        assert_eq!(
            assignment_view(Ok(AssignmentAnswer::NotHeld)),
            Ok(RunnerWatchAssignmentAnswer::NotHeld)
        );
        assert_eq!(
            assignment_view(Ok(AssignmentAnswer::Refused(ControlError::InvalidRequest))),
            Err(RunnerWatchSourceError::EarlierRelease)
        );
        assert_eq!(
            assignment_view(Err(RequestFailure::Protocol(ProtocolFailure::Invalid))),
            Err(RunnerWatchSourceError::Unreadable)
        );
        assert_eq!(
            assignment_view(Err(RequestFailure::NotReachable)),
            Err(RunnerWatchSourceError::NotReachable)
        );
    }

    // REQ-RUNNERENROLL-135: a state and a kind this release does not know arrive as the
    // words, a step with no work member runs nothing known, and a start that does not parse
    // is left out.
    #[test]
    fn a_step_this_release_does_not_know_keeps_its_words() {
        let mut document = fixture_named("assignment-response.json");
        let step = &mut document["assignment"]["run"]["steps"][0];
        step["state"] = "paused_by_moon".into();
        step["kind"] = "script".into();
        step["startedAt"] = "not a time".into();
        step.as_object_mut().unwrap().remove("command");
        let run = held_view(&document).run.unwrap();

        let step = &run.steps[0];
        assert_eq!(
            (step.state.as_str(), step.kind.as_str()),
            ("paused_by_moon", "script")
        );
        assert_eq!(step.work, RunnerWatchRunWork::Unknown);
        assert_eq!(step.started_at, None);
    }

    // REQ-RUNNERENROLL-144: the view's query is the protocol's, member for member.
    #[test]
    fn a_query_maps_to_the_protocol_query() {
        let query = RunnerWatchAssignmentQuery {
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            path: vec![
                RunnerWatchRunElement {
                    step: "build".to_owned(),
                    item: Some(1),
                    ordinal: 2,
                },
                RunnerWatchRunElement {
                    step: "fix".to_owned(),
                    item: None,
                    ordinal: 1,
                },
            ],
            step: Some("implement".to_owned()),
        };
        assert_eq!(
            assignment_query(&query),
            AssignmentQuery {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                run: vec![
                    OverviewRunElement {
                        step: "build".to_owned(),
                        item: Some(1),
                        ordinal: 2,
                    },
                    OverviewRunElement {
                        step: "fix".to_owned(),
                        item: None,
                        ordinal: 1,
                    },
                ],
                step: Some("implement".to_owned()),
            }
        );
    }

    fn overview_of(document: &serde_json::Value) -> Overview {
        serde_json::from_value(document["overview"].clone()).unwrap()
    }

    fn mapped(document: &serde_json::Value) -> RunnerWatchOverview {
        view(overview_of(document)).unwrap()
    }

    fn facts(outcome: &RunnerWatchOutcome) -> &[String] {
        match outcome {
            RunnerWatchOutcome::Done(facts) | RunnerWatchOutcome::Refused(facts) => facts,
        }
    }

    // REQ-RUNNERENROLL-140: one entry per assignment, titled by its subject or else its run
    // ID, with a waiting run's wait, a running run's step path and an event of each kind in
    // words naming its facts.
    #[test]
    fn the_fixture_overview_maps_to_what_the_viewer_draws() {
        let view = mapped(&fixture_document());

        assert_eq!(view.boot_id, "rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
        assert_eq!(view.version, "0.83.0");
        assert_eq!(view.reports_waiting, 2);
        assert_eq!(view.connection.state, "backing_off");
        assert_eq!(view.connection.mark, RunnerWatchMark::Pending);
        assert_eq!(view.connection.last_failure.as_deref(), Some("network"));
        assert_eq!(view.connection.attempts, Some(3));
        assert_eq!(view.stop_time_left, Some(Duration::from_secs(298)));

        let [working, waiting] = view.assignments.as_slice() else {
            panic!("expected two assignments: {:?}", view.assignments);
        };
        assert_eq!(
            working.title,
            RunnerWatchTitle::RunId("run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned())
        );
        assert_eq!(working.mark, RunnerWatchMark::Active);
        assert_eq!(
            working.lease,
            Some(RunnerWatchLease {
                health: "ok".to_owned(),
                time_left: Duration::from_secs(42),
            })
        );
        assert_eq!(
            working.detail,
            RunnerWatchAssignmentDetail::Steps {
                steps: vec![RunnerWatchStep {
                    path: vec!["build[0]".to_owned(), "implement".to_owned()],
                }],
                omitted: 2,
            }
        );
        assert_eq!(
            waiting.title,
            RunnerWatchTitle::Subject("AUTO-751".to_owned())
        );
        assert_eq!(waiting.mark, RunnerWatchMark::Pending);
        assert_eq!(waiting.phase, "waiting");
        assert_eq!(
            waiting.detail,
            RunnerWatchAssignmentDetail::Wait(RunnerWatchWait {
                kind: "external".to_owned(),
                step: vec!["review[0]".to_owned(), "queue".to_owned()],
                since: Some(utc("2026-11-19T14:10:00Z").unwrap()),
            })
        );
        assert_eq!(view.assignments_omitted, 1);

        let details = view
            .recent
            .iter()
            .map(|entry| {
                (
                    entry.result.as_str(),
                    entry.succeeded,
                    entry.detail.as_deref(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            details,
            [
                ("succeeded", true, Some("merged")),
                ("failed", false, Some("step_failed")),
                ("interrupted", false, Some("execution_lease_expired")),
            ]
        );

        assert_eq!(
            view.logins,
            [
                RunnerWatchLogin {
                    provider: "codex".to_owned(),
                    ready: true,
                    reason: None,
                },
                RunnerWatchLogin {
                    provider: "github".to_owned(),
                    ready: false,
                    reason: Some("not_delivered".to_owned()),
                },
            ]
        );

        let document = fixture_document();
        let kinds = document["overview"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["kind"].as_str().unwrap().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(kinds.len(), 11, "the fixture holds an event of each kind");
        assert_eq!(view.events.len(), 11);
        assert!(view.events.iter().all(|event| !event.text.is_empty()));
        let text_of = |count: u64, fact: &str| {
            view.events
                .iter()
                .any(|event| event.count == count && event.text.contains(fact))
        };
        assert!(text_of(3, "network"));
        assert!(text_of(1, "rrc_01k0z6r1w8f4jy2m7q9v3x5abd"));
        assert!(text_of(1, "no_pending_credential"));
        assert!(text_of(12, "schedule"));
        assert!(text_of(1, "run_01k0z6r1w8f4jy2m7q9v3x5abg"));
        assert!(text_of(1, "run_01k0z6r1w8f4jy2m7q9v3x5abd"));
        assert!(text_of(1, "2 workspaces"));
        assert_eq!(view.events_omitted, 2);
    }

    // REQ-RUNNERENROLL-138: the disk's three events are drawn in words naming their sizes
    // or their run, the turn to low and the declined offer as failures and the recovery as
    // a success.
    #[test]
    fn the_disk_events_map_to_words_naming_their_sizes() {
        let view = mapped(&fixture_named("overview-disk-events-response.json"));
        let [recovered, declined, low] = view.events.as_slice() else {
            panic!("expected three events: {:?}", view.events);
        };
        assert_eq!(declined.mark, RunnerWatchMark::Failure);
        assert!(
            declined.text.contains("run_01k0z6r1w8f4jy2m7q9v3x5abg"),
            "{declined:?}"
        );
        assert_eq!(recovered.mark, RunnerWatchMark::Success);
        assert!(
            recovered.text.starts_with("disk recovered"),
            "{recovered:?}"
        );
        assert!(recovered.text.contains("8.0 GiB"), "{recovered:?}");
        assert_eq!(low.mark, RunnerWatchMark::Failure);
        assert!(low.text.starts_with("disk low"), "{low:?}");
        for size in ["1.0 GiB", "5.0 GiB"] {
            assert!(low.text.contains(size), "{size}: {low:?}");
        }
    }

    // REQ-RUNNERENROLL-140: the overview's disk reading maps with its sizes, minimum and low
    // state, the member this release does not know left behind.
    #[test]
    fn the_disk_reading_maps_with_its_sizes_minimum_and_state() {
        let view = mapped(&fixture_named("overview-disk-response.json"));
        assert_eq!(
            view.disk,
            Some(RunnerWatchDisk {
                available_bytes: 6_186_599,
                total_bytes: 423_054_278_656,
                minimum_free_bytes: 5_368_709_120,
                low: true,
            })
        );
        assert_eq!(mapped(&fixture_document()).disk, None);
    }

    // REQ-RUNNERENROLL-135: a phase, a result and an event kind this release does not know
    // arrive as the word received, and a provider entry it cannot read keeps its two words.
    #[test]
    fn words_this_release_does_not_know_arrive_as_received() {
        let mut document = fixture_document();
        let overview = &mut document["overview"];
        overview["assignments"][0]["phase"] = "thawing".into();
        overview["recentAssignments"][0]["result"] = "vanished".into();
        overview["events"][0]["kind"] = "eclipsed".into();
        overview["agentLogins"]["providers"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"provider": "gemini", "state": "paused"}));
        let view = mapped(&document);

        assert_eq!(view.assignments[0].phase, "thawing");
        assert_eq!(view.assignments[0].mark, RunnerWatchMark::Neutral);
        assert_eq!(view.recent[0].result, "vanished");
        assert!(!view.recent[0].succeeded);
        assert_eq!(view.events[0].text, "eclipsed");
        assert_eq!(view.events[0].mark, RunnerWatchMark::Neutral);
        assert_eq!(
            view.logins.last(),
            Some(&RunnerWatchLogin {
                provider: "gemini".to_owned(),
                ready: false,
                reason: Some("paused".to_owned()),
            })
        );
    }

    // REQ-RUNNERENROLL-142: each operation's answer becomes an outcome that names what the
    // runner did or the code it refused with.
    #[test]
    fn operation_answers_map_to_outcomes_that_name_their_facts() {
        let reloaded = reload_outcome(Ok(Response::Reloaded {
            credential_id: "rrc_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
        }));
        assert!(matches!(reloaded, RunnerWatchOutcome::Done(_)));
        assert!(
            facts(&reloaded)
                .iter()
                .any(|fact| fact.contains("rrc_01k0z6r1w8f4jy2m7q9v3x5abd"))
        );

        let readiness: AgentLoginReadiness = serde_json::from_value(serde_json::json!({
            "updatedAt": "2026-11-19T14:00:00Z",
            "providers": [{"provider": "codex", "state": "install_failed"}]
        }))
        .unwrap();
        let synced = setup_outcome(Ok(Response::AgentLoginsSynced(SyncedAgentLogins {
            readiness,
            unanswered_checks: vec![UnansweredCheck {
                provider: AgentLoginProvider::ClaudeCode,
                cause: AgentLoginCheckCause::TimedOut,
            }],
        })));
        assert!(matches!(synced, RunnerWatchOutcome::Done(_)));
        assert!(
            facts(&synced)
                .iter()
                .any(|fact| fact.contains("codex") && fact.contains("install_failed"))
        );
        assert!(
            facts(&synced)
                .iter()
                .any(|fact| fact.contains("claude_code") && fact.contains("timed_out"))
        );

        for (outcome, code) in [
            (
                setup_outcome(Ok(Response::Error(ControlError::RunnerBusy))),
                "runner_busy",
            ),
            (
                reload_outcome(Ok(Response::Error(ControlError::NoPendingCredential))),
                "no_pending_credential",
            ),
        ] {
            assert!(matches!(outcome, RunnerWatchOutcome::Refused(_)));
            assert!(facts(&outcome).iter().any(|fact| fact.contains(code)));
        }

        let unreachable = reload_outcome(Err(RequestFailure::NotReachable));
        assert!(matches!(unreachable, RunnerWatchOutcome::Refused(ref facts) if !facts.is_empty()));
    }

    // REQ-RUNNERENROLL-142: every closed error is a fact the viewer can draw whole on its
    // minimum width, which it would otherwise count rather than show.
    #[test]
    fn every_closed_error_fits_the_notice_whole() {
        for error in [
            ControlError::InvalidRequest,
            ControlError::UnsupportedVersion,
            ControlError::RunnerBusy,
            ControlError::NoPendingCredential,
            ControlError::PendingRegistrationMismatch,
            ControlError::PendingAuthenticationFailed,
            ControlError::PendingProtocolFailed,
            ControlError::PendingConnectionFailed,
            ControlError::StateUpdateFailed,
        ] {
            let facts = control_refusal(error);
            assert!(facts[0].ends_with(&format!("({})", known_word(error))));
            for fact in &facts {
                assert!(
                    fact.chars().count() <= 60,
                    "{fact:?} is wider than 60 columns"
                );
            }
        }
    }
}
