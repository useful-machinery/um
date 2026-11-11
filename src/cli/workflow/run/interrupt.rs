//! `um workflow run interrupt` (REQ-LOCALRUN-202).

use clap::Args;
use rustix::process::{Pid, Signal};
use serde_json::{Map, Value};
use um_execution::{CONTROL_INTERRUPT, RunState, WaitInspection, latest_child_run, lock_holder};

use super::control::{Acted, ControlFailure, ControlTarget, Refusal};

pub(super) const ABOUT: &str = "Interrupt a running run so that retry continues its stage";
const COMMAND: &str = "um workflow run interrupt";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[arg(long, value_name = "TEXT", help = "Why the run is interrupted")]
    reason: String,

    #[command(flatten)]
    output: super::control::JsonOutput,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        super::control::execute(COMMAND, &self.run, self.output.json, |target| {
            self.interrupt(target)
        })
    }

    /// Records `control.interrupt` on a run a live process holds and wakes that process,
    /// which interrupts the run's attempt for it, then reports the wait the run stopped on.
    fn interrupt(&self, target: &ControlTarget) -> Result<Acted, ControlFailure> {
        match &target.inspection {
            WaitInspection::Locked(_) => {}
            WaitInspection::NoWait {
                state: Some(RunState::Exited),
                ..
            } => return Err(super::end::run_exited().into()),
            WaitInspection::Held { .. }
            | WaitInspection::Satisfied { .. }
            | WaitInspection::RuleInvalid { .. }
            | WaitInspection::ConditionUnavailable { .. } => {
                return Err(Refusal::new(
                    "run_not_running",
                    "The run waits; only a running run is interrupted.",
                )
                .into());
            }
            WaitInspection::NoWait { .. } | WaitInspection::Continued { .. } => {
                return Err(run_not_live().into());
            }
        }
        let pid = lock_holder(&target.run_directory)?.ok_or_else(run_not_live)?;
        live_child(target, pid)?;
        let payload = Map::from_iter([("reason".to_owned(), Value::String(self.reason.clone()))]);
        let control = target.record(CONTROL_INTERRUPT, None, Some(payload))?;
        // SIGURG carries no request of its own and is ignored by default, so a process that
        // does not read the journal when woken is undisturbed and the interrupt stays pending.
        if !super::end::signal_and_await_release(&target.run_directory, pid, Signal::URG)? {
            return Err(Refusal::new(
                "interrupt_pending",
                "The run's process has not released the run; the interrupt stays recorded and no later attempt acts on it.",
            )
            .into());
        }
        Ok(Acted {
            control,
            wait: super::control::current_wait(&target.run_directory)?,
            outcome: "interrupted",
        })
    }
}

fn run_not_live() -> Refusal {
    Refusal::new(
        "run_not_live",
        "No live process holds the run; resume or cancel it.",
    )
}

/// Refuses `child_not_live` a run positioned at a `workflow` step whose latest child is not
/// `running` under `pid`, the process that holds the run: the run is between stages. A run
/// with no position (a run started by path, or a flat workflow) has no child to check. The
/// child may still end before the signal arrives, which the process's own interruption rules
/// then decide.
fn live_child(target: &ControlTarget, pid: Pid) -> Result<(), ControlFailure> {
    let Some(step) = target.record.position.first() else {
        return Ok(());
    };
    let child = match step.as_str() {
        Some(step) => latest_child_run(&target.run_directory, step)?,
        None => None,
    };
    match child {
        Some((child, RunState::Running, _)) if lock_holder(&child)? == Some(pid) => Ok(()),
        _ => Err(Refusal::new(
            "child_not_live",
            "The run is between stages; stop it before a step with `um workflow run stop --before STEP` instead.",
        )
        .into()),
    }
}
