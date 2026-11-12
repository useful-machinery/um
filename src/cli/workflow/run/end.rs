//! `um workflow run withdraw`, `cancel` and `release` (REQ-LOCALRUN-186, REQ-LOCALRUN-187).

use std::path::Path;
use std::time::Duration;

use anyhow::anyhow;
use clap::Args;
use rustix::process::{Pid, Signal, kill_process};
use um_execution::{
    CANCELED_OUTCOME, CONTROL_CANCEL, CONTROL_RELEASE, CONTROL_WITHDRAW, LocalContinuationOpen,
    Mirror, PendingLocalContinuation, RetryIneligibilityReason, RunState, WITHDRAWN_OUTCOME,
    WaitInspection, WaitRecord, acquire_local_continuation, lock_holder, read_controls,
};

use super::control::{Acted, ControlFailure, ControlTarget, Refusal};

pub(super) const WITHDRAW_ABOUT: &str = "End a run without continuing it";
pub(super) const CANCEL_ABOUT: &str = "End a run, signalling the process that runs it";
pub(super) const RELEASE_ABOUT: &str = "Record the release of an exited run";
const WITHDRAW_COMMAND: &str = "um workflow run withdraw";
const CANCEL_COMMAND: &str = "um workflow run cancel";
const RELEASE_COMMAND: &str = "um workflow run release";

/// How long `cancel` and `interrupt` wait for the process they signalled to release the run
/// (REQ-LOCALRUN-186, REQ-LOCALRUN-202): `RELEASE_POLLS` probes `RELEASE_POLL` apart, 60
/// seconds at the production cadence. A `test-fixtures` build lets an end-to-end test
/// shorten the cadence, and the wait with it (`um_support::poll_interval`).
const RELEASE_POLL: Duration = Duration::from_millis(200);
const RELEASE_POLLS: u32 = 300;

/// The arguments every leaf here takes.
#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[command(flatten)]
    output: super::control::JsonOutput,
}

impl Command {
    pub(super) fn withdraw(self) -> super::super::super::CommandResult {
        super::control::execute(WITHDRAW_COMMAND, &self.run, self.output.json, |target| {
            match &target.inspection {
                WaitInspection::Locked(_) => Err(super::control::run_locked().into()),
                WaitInspection::Held { wait }
                | WaitInspection::Satisfied { wait, .. }
                | WaitInspection::RuleInvalid { wait, .. }
                | WaitInspection::ConditionUnavailable { wait, .. } => {
                    end_waiting(target, wait, CONTROL_WITHDRAW, WITHDRAWN_OUTCOME)
                }
                WaitInspection::NoWait {
                    state: Some(RunState::Exited),
                    ..
                } => Err(run_exited().into()),
                WaitInspection::NoWait { .. } | WaitInspection::Continued { .. } => {
                    end_abandoned(target, CONTROL_WITHDRAW, WITHDRAWN_OUTCOME)
                }
            }
        })
    }

    pub(super) fn cancel(self) -> super::super::super::CommandResult {
        super::control::execute(
            CANCEL_COMMAND,
            &self.run,
            self.output.json,
            |target| match &target.inspection {
                WaitInspection::Locked(_) => cancel_live(target),
                WaitInspection::Held { wait }
                | WaitInspection::Satisfied { wait, .. }
                | WaitInspection::RuleInvalid { wait, .. }
                | WaitInspection::ConditionUnavailable { wait, .. } => {
                    end_waiting(target, wait, CONTROL_CANCEL, CANCELED_OUTCOME)
                }
                WaitInspection::NoWait {
                    state: Some(RunState::Exited),
                    ..
                } => Err(run_exited().into()),
                WaitInspection::NoWait { .. } | WaitInspection::Continued { .. } => {
                    end_abandoned(target, CONTROL_CANCEL, CANCELED_OUTCOME)
                }
            },
        )
    }

    pub(super) fn release(self) -> super::super::super::CommandResult {
        super::control::execute(RELEASE_COMMAND, &self.run, self.output.json, |target| {
            if !matches!(
                target.inspection,
                WaitInspection::NoWait {
                    state: Some(RunState::Exited),
                    ..
                }
            ) {
                return Err(Refusal::new(
                    "run_live",
                    "The run has not exited; only an exited run is released.",
                )
                .into());
            }
            if read_controls(&target.run_directory)?
                .iter()
                .any(|control| control.event == CONTROL_RELEASE)
            {
                return Err(Refusal::new("run_released", "The run is already released.").into());
            }
            let control = target.record(CONTROL_RELEASE, None, None)?;
            Ok(Acted::recorded(control, None))
        })
    }
}

pub(super) fn run_exited() -> Refusal {
    Refusal::new("run_exited", "The run has exited.")
}

/// The run's continuation lock, or `run_locked` when a process took the run since it was
/// inspected.
fn acquire(run_directory: &Path) -> Result<PendingLocalContinuation, ControlFailure> {
    match acquire_local_continuation(run_directory)? {
        LocalContinuationOpen::Acquired(pending) => Ok(*pending),
        LocalContinuationOpen::Rejected(rejection)
            if rejection.reason() == RetryIneligibilityReason::RunLocked =>
        {
            Err(super::control::run_locked().into())
        }
        LocalContinuationOpen::Rejected(rejection) => {
            Err(anyhow!("the run cannot be ended: {}", rejection.reason().as_str()).into())
        }
    }
}

/// Ends a waiting run with `outcome` (REQ-CONTINUE-75): under the run lock, once the run
/// still waits on `wait` and the mirror's write has its credentials (REQ-LOCALRUN-204),
/// appends `event` naming it and marks the record `exited`.
fn end_waiting(
    target: &ControlTarget,
    wait: &WaitRecord,
    event: &str,
    outcome: &'static str,
) -> Result<Acted, ControlFailure> {
    let pending = acquire(&target.run_directory)?;
    if !matches!(pending.wait(), Ok(Some(current)) if current.id() == wait.id()) {
        pending.settle_abandoned()?;
        return Err(Refusal::new(
            "wait_changed",
            "The run's wait changed while the command inspected it; run the command again.",
        )
        .into());
    }
    let mirror = mirror(target, &pending);
    outcome_credentials(mirror.as_ref(), outcome)?;
    let control = target.record(event, Some(wait.id()), None)?;
    pending.end_waiting(outcome)?;
    write_outcome(mirror.as_ref(), outcome);
    Ok(Acted {
        control,
        wait: None,
        outcome,
    })
}

/// Ends a run whose record reads `running` but whose process is gone with `outcome`
/// (REQ-LOCALRUN-186): under the run lock, once the mirror's write has its credentials
/// (REQ-LOCALRUN-204), appends `event` naming no wait, marks the record `exited` and settles
/// the abandoned attempt (REQ-CONTINUE-2), creating no attempt.
fn end_abandoned(
    target: &ControlTarget,
    event: &str,
    outcome: &'static str,
) -> Result<Acted, ControlFailure> {
    let pending = acquire(&target.run_directory)?;
    let mirror = mirror(target, &pending);
    outcome_credentials(mirror.as_ref(), outcome)?;
    let control = target.record(event, None, None)?;
    pending.update_run_record(RunState::Exited, target.record.attempts, Some(outcome))?;
    pending.settle_abandoned()?;
    write_outcome(mirror.as_ref(), outcome);
    Ok(Acted {
        control,
        wait: None,
        outcome,
    })
}

/// Refuses `mirror_credentials_unavailable` when the run's mirror would write `onOutcome` for
/// `outcome` without a key (REQ-LOCALRUN-204). The caller drops its continuation unsettled on
/// the refusal, which releases the lock and leaves the run's record, wait, controls and
/// `mirror/` as the command found them.
fn outcome_credentials(mirror: Option<&Mirror>, outcome: &str) -> Result<(), ControlFailure> {
    super::mirror::outcome_credentials(mirror, outcome)
        .map_err(|failure| Refusal::new(failure.code, super::mirror::CREDENTIALS_REFUSAL).into())
}

/// The run's mirror, read before the continuation that holds its root workflow is consumed.
fn mirror(target: &ControlTarget, pending: &PendingLocalContinuation) -> Option<Mirror> {
    let (workflow, inputs) = pending.root_specification();
    super::mirror::run_mirror(
        &target.record,
        &workflow.definition,
        inputs,
        &target.run_directory,
    )
}

/// The root's `onOutcome` for `outcome`, once the record reads `exited` (REQ-LOCALRUN-186,
/// REQ-LOCALRUN-191); its result never changes the command's.
fn write_outcome(mirror: Option<&Mirror>, outcome: &'static str) {
    if let Some(mirror) = mirror {
        super::mirror::perform(
            mirror,
            Box::new(move |mirror, target| mirror.outcome(target, outcome)),
        );
    }
}

/// Cancels a run a live process holds: signals that process to terminate, whose own
/// cancellation marks the record `exited` `canceled` (REQ-LOCALRUN-175), and waits for it
/// to release the run; `cancel_pending` when it has not within the wait.
fn cancel_live(target: &ControlTarget) -> Result<Acted, ControlFailure> {
    let control = target.record(CONTROL_CANCEL, None, None)?;
    if let Some(pid) = lock_holder(&target.run_directory)?
        && !signal_and_await_release(&target.run_directory, pid, Signal::TERM)?
    {
        return Err(Refusal::new(
            "cancel_pending",
            "The run's process has not released the run; the cancel stays recorded.",
        )
        .into());
    }
    match super::mirror::read_run_record(&target.run_directory) {
        Some(record) if record.state == RunState::Exited => Ok(Acted {
            control,
            wait: None,
            outcome: CANCELED_OUTCOME,
        }),
        _ => Err(Refusal::new(
            "cancel_pending",
            "The run's process released the run without marking it exited; the cancel stays recorded.",
        )
        .into()),
    }
}

/// Sends `signal` to `pid`, the process that holds the run at `run_directory`, and waits for
/// it to release the run (REQ-LOCALRUN-186, REQ-LOCALRUN-202): false when it still holds the
/// run after the wait. A process that exited before the signal has released the run.
pub(super) fn signal_and_await_release(
    run_directory: &Path,
    pid: Pid,
    signal: Signal,
) -> Result<bool, ControlFailure> {
    match kill_process(pid, signal) {
        Ok(()) | Err(rustix::io::Errno::SRCH) => {}
        Err(error) => {
            return Err(anyhow::Error::new(error)
                .context("signal the run's process")
                .into());
        }
    }
    let poll = um_support::poll_interval(RELEASE_POLL);
    let deadline = um_support::monotonic_now() + poll * RELEASE_POLLS;
    loop {
        if lock_holder(run_directory)? != Some(pid) {
            return Ok(true);
        }
        if um_support::monotonic_now() >= deadline {
            return Ok(false);
        }
        um_support::sleep(poll);
    }
}
