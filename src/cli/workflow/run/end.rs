//! `um workflow run withdraw`, `cancel` and `release` (REQ-LOCALRUN-186, REQ-LOCALRUN-187);
//! `release --cloud ORGANIZATION` releases a Cloud run's subject claim instead
//! (REQ-CLOUDDEF-19).

use std::path::Path;
use std::time::Duration;

use anyhow::anyhow;
use clap::Args;
use rustix::process::{Pid, Signal, kill_process};
use um_execution::{
    CANCELED_OUTCOME, CONTROL_CANCEL, CONTROL_RELEASE, CONTROL_WITHDRAW, LocalContinuationOpen,
    Mirror, PendingLocalContinuation, RetryIneligibilityReason, RunState, WITHDRAWN_OUTCOME,
    WaitInspection, WaitRecord, acquire_local_continuation, await_run_lock_gate, inspect_wait,
    lock_holder, read_controls,
};

use super::control::{Acted, ControlFailure, ControlTarget, Refusal};

pub(super) const WITHDRAW_ABOUT: &str = "End a run without continuing it";
pub(super) const CANCEL_ABOUT: &str = "End a run, signalling the process that runs it";
pub(super) const RELEASE_ABOUT: &str =
    "Record the release of an exited run, or release a Cloud run's claim with --cloud";
pub(super) const RELEASE_AFTER_HELP: &str = "With --cloud ORGANIZATION, RUN is a Cloud run ID: the command asks Cloud, as an organization owner, to release the subject claim of that exited run under one request key, sent again if the response is lost. It reads and writes no local run.";
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

/// `release`'s arguments: the shared ones and the Cloud selector, which `withdraw` and
/// `cancel` do not take.
#[derive(Debug, Args)]
pub(super) struct Release {
    #[command(flatten)]
    command: Command,

    #[command(flatten)]
    cloud: super::CloudSelector,
}

impl Release {
    pub(super) fn execute(mut self) -> super::super::super::CommandResult {
        match self.cloud.organization.take() {
            Some(organization) => super::cloud::CloudRun::new(
                self.command.run,
                organization,
                self.cloud,
                self.command.output.json,
            )?
            .release(),
            None => self.command.release(),
        }
    }
}

impl Command {
    pub(super) fn withdraw(self) -> super::super::super::CommandResult {
        super::control::execute(WITHDRAW_COMMAND, &self.run, self.output.json, withdraw_run)
    }

    pub(super) fn cancel(self) -> super::super::super::CommandResult {
        super::control::execute(CANCEL_COMMAND, &self.run, self.output.json, cancel_run)
    }

    fn release(self) -> super::super::super::CommandResult {
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

/// Withdraws the run `target` opened, as `um workflow run withdraw` does (REQ-LOCALRUN-186);
/// `um workflow run event` applies a control trigger's `withdraw` through it
/// (REQ-LOCALRUN-208). A run a live process holds goes to `withdraw_live`, and so does a
/// `running` one found without a lock holder, which may be a process between the attempts it
/// chains (REQ-LOCALRUN-214); anything else is ended as `end_unheld` does.
pub(super) fn withdraw_run(target: &ControlTarget) -> Result<Acted, ControlFailure> {
    match &target.inspection {
        WaitInspection::Locked(_)
        | WaitInspection::Continued { .. }
        | WaitInspection::NoWait {
            state: Some(RunState::Running),
            ..
        } => withdraw_live(target),
        _ => end_unheld(target, CONTROL_WITHDRAW, WITHDRAWN_OUTCOME),
    }
}

/// Cancels the run `target` opened, as `um workflow run cancel` does (REQ-LOCALRUN-186);
/// `um workflow run event` applies a control trigger's `cancel` through it
/// (REQ-LOCALRUN-208): a live one through `cancel_live`, and otherwise as `end_unheld` does.
pub(super) fn cancel_run(target: &ControlTarget) -> Result<Acted, ControlFailure> {
    match &target.inspection {
        WaitInspection::Locked(_) => cancel_live(target),
        _ => end_unheld(target, CONTROL_CANCEL, CANCELED_OUTCOME),
    }
}

/// Ends the run with `event` and `outcome` as `target` found it: a waiting one at its wait,
/// and one whose process is gone as abandoned, each appending `event` only as it ends the
/// run; a run a process holds is `run_locked` and an exited run `run_exited`, with nothing
/// appended.
fn end_unheld(
    target: &ControlTarget,
    event: &str,
    outcome: &'static str,
) -> Result<Acted, ControlFailure> {
    match &target.inspection {
        WaitInspection::Locked(_) => Err(super::control::run_locked().into()),
        WaitInspection::Held { wait }
        | WaitInspection::Satisfied { wait, .. }
        | WaitInspection::RuleInvalid { wait, .. }
        | WaitInspection::ConditionUnavailable { wait, .. } => {
            end_waiting(target, wait, event, outcome)
        }
        WaitInspection::NoWait {
            state: Some(RunState::Exited),
            ..
        } => Err(run_exited().into()),
        WaitInspection::NoWait { .. } | WaitInspection::Continued { .. } => {
            end_abandoned(target, event, outcome)
        }
    }
}

pub(super) fn run_exited() -> Refusal {
    Refusal::new("run_exited", "The run has exited.")
}

/// The run's continuation lock, or `run_locked` when a process took the run since it was
/// inspected.
fn acquire(run_directory: &Path) -> Result<PendingLocalContinuation, ControlFailure> {
    // A test seam between the inspection and the run lock; nothing outside `test-fixtures`.
    await_run_lock_gate()?;
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
/// appends `event` naming the wait and marks the record `exited`.
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
/// (REQ-LOCALRUN-204), appends `event` naming no wait and ends the run through
/// `PendingLocalContinuation::end_running`, which marks the record `exited` and settles the
/// abandoned attempt (REQ-CONTINUE-2) or, for a run interrupted between chained attempts, spends
/// the withdrawals recorded for it (REQ-LOCALRUN-211), creating no attempt. It takes the
/// controls lock itself, so this command holds none here.
fn end_abandoned(
    target: &ControlTarget,
    event: &str,
    outcome: &'static str,
) -> Result<Acted, ControlFailure> {
    let pending = acquire(&target.run_directory)?;
    let mirror = mirror(target, &pending);
    outcome_credentials(mirror.as_ref(), outcome)?;
    let control = target.record(event, None, None)?;
    pending.end_running(outcome)?;
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

/// Withdraws a run a live process holds (REQ-LOCALRUN-186, INV-ENGINE-39): under the run's
/// controls lock, which that process takes for its last read of the withdrawals and keeps
/// until it releases the run, and which a process that chains attempts keeps from the release
/// of one until it holds the run for the next (REQ-LOCALRUN-214), the command opens the run
/// again. When a lock holds the run, whether or not the system reports its holder's process,
/// it appends `control.withdraw` naming no wait, so the holder honors it before its next
/// `workflow` step starts or, when it reaches none, as its attempt settles (REQ-WFEXEC-73);
/// `withdraw_pending` when one it has not honored is already recorded. A run no lock holds
/// has been released for the last time, or its process is gone, so the command appends
/// nothing there and ends the run as it finds it again (`end_unheld`).
fn withdraw_live(target: &ControlTarget) -> Result<Acted, ControlFailure> {
    // A test seam between the inspection and the controls lock; nothing outside
    // `test-fixtures`.
    await_run_lock_gate()?;
    let probe = inspect_wait(&target.run_directory, &um_execution::NoConditions)?;
    let controls = probe.lock_controls()?;
    let current = super::control::open(&target.run_directory)?;
    if !matches!(current.inspection, WaitInspection::Locked(_)) {
        drop(controls);
        return end_unheld(&current, CONTROL_WITHDRAW, WITHDRAWN_OUTCOME);
    }
    if probe.withdrawal_pending()? {
        return Err(super::control::withdraw_pending().into());
    }
    let control = current.record(CONTROL_WITHDRAW, None, None)?;
    drop(controls);
    Ok(Acted::recorded(control, None))
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
