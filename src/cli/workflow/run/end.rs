//! `um workflow run withdraw`, `cancel` and `release` (REQ-LOCALRUN-186, REQ-LOCALRUN-187).

use std::path::Path;
use std::time::Duration;

use anyhow::anyhow;
use clap::Args;
use rustix::process::{Signal, kill_process};
use um_execution::{
    CANCELED_OUTCOME, CONTROL_CANCEL, CONTROL_RELEASE, CONTROL_WITHDRAW, LocalContinuationOpen,
    Mirror, PendingLocalContinuation, RetryIneligibilityReason, RunState, WITHDRAWN_OUTCOME,
    WaitInspection, WaitRecord, acquire_local_continuation, inspect_wait, lock_holder,
    read_controls,
};

use super::control::{Acted, ControlFailure, ControlTarget, Refusal};

pub(super) const WITHDRAW_ABOUT: &str = "End a run without continuing it";
pub(super) const CANCEL_ABOUT: &str = "End a run, signalling the process that runs it";
pub(super) const RELEASE_ABOUT: &str = "Record the release of an exited run";
const WITHDRAW_COMMAND: &str = "um workflow run withdraw";
const CANCEL_COMMAND: &str = "um workflow run cancel";
const RELEASE_COMMAND: &str = "um workflow run release";

/// How long `cancel` waits for a signalled process to release the run (REQ-LOCALRUN-186).
const CANCEL_WAIT: Duration = Duration::from_secs(60);
const CANCEL_POLL: Duration = Duration::from_millis(200);

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

fn run_exited() -> Refusal {
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
/// still waits on `wait`, appends `event` naming it and marks the record `exited`.
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
    let control = target.record(event, Some(wait.id()), None)?;
    let mirror = mirror(target, &pending);
    pending.end_waiting(outcome)?;
    write_outcome(mirror.as_ref(), outcome);
    Ok(Acted {
        control,
        wait: None,
        outcome,
    })
}

/// Ends a run whose record reads `running` but whose process is gone with `outcome`
/// (REQ-LOCALRUN-186): under the run lock, appends `event` naming no wait, marks the record
/// `exited` and settles the abandoned attempt (REQ-CONTINUE-2), creating no attempt.
fn end_abandoned(
    target: &ControlTarget,
    event: &str,
    outcome: &'static str,
) -> Result<Acted, ControlFailure> {
    let pending = acquire(&target.run_directory)?;
    let control = target.record(event, None, None)?;
    let mirror = mirror(target, &pending);
    pending.update_run_record(RunState::Exited, target.record.attempts, Some(outcome))?;
    pending.settle_abandoned()?;
    write_outcome(mirror.as_ref(), outcome);
    Ok(Acted {
        control,
        wait: None,
        outcome,
    })
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
/// to release the run; `cancel_pending` when it has not within `CANCEL_WAIT`.
fn cancel_live(target: &ControlTarget) -> Result<Acted, ControlFailure> {
    let control = target.record(CONTROL_CANCEL, None, None)?;
    if let Some(pid) = lock_holder(&target.run_directory)? {
        match kill_process(pid, Signal::TERM) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => {}
            Err(error) => {
                return Err(anyhow::Error::new(error)
                    .context("signal the run's process")
                    .into());
            }
        }
    }
    let deadline = um_support::monotonic_now() + CANCEL_WAIT;
    loop {
        let probe = inspect_wait(&target.run_directory, &um_execution::NoConditions)?;
        if !matches!(probe.inspection, WaitInspection::Locked(_)) {
            break;
        }
        if um_support::monotonic_now() >= deadline {
            return Err(Refusal::new(
                "cancel_pending",
                "The run's process has not released the run; the cancel stays recorded.",
            )
            .into());
        }
        um_support::sleep(CANCEL_POLL);
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
