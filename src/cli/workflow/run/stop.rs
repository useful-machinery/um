//! `um workflow run stop` and `um workflow run lift` (REQ-LOCALRUN-185); with `--cloud
//! ORGANIZATION`, the control is carried to the Cloud run's runner instead (REQ-LOCALRUN-210).

use std::collections::BTreeSet;

use clap::Args;
use serde_json::{Map, Value};
use um_execution::{CONTROL_LIFT, CONTROL_STOP, WaitKind, read_controls, stop_in_force};

use super::control::{Acted, ControlFailure, ControlTarget, Refusal};

pub(super) const STOP_ABOUT: &str = "Park a run before a step";
pub(super) const LIFT_ABOUT: &str = "Lift a stop so the run may continue past it";
const STOP_COMMAND: &str = "um workflow run stop";
const LIFT_COMMAND: &str = "um workflow run lift";

#[derive(Debug, Args)]
pub(super) struct Stop {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[arg(long, value_name = "STEP", help = "Step the run waits before")]
    before: String,

    #[arg(long, value_name = "TEXT", help = "Why the run is parked")]
    reason: String,

    #[command(flatten)]
    output: super::control::JsonOutput,

    #[command(flatten)]
    cloud: super::CloudSelector,
}

#[derive(Debug, Args)]
pub(super) struct Lift {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[arg(
        long,
        value_name = "STEP",
        help = "Step whose stop to lift; required when several stops are in force"
    )]
    before: Option<String>,

    #[command(flatten)]
    output: super::control::JsonOutput,

    #[command(flatten)]
    cloud: super::CloudSelector,
}

impl Stop {
    pub(super) fn execute(mut self) -> super::super::super::CommandResult {
        if let Some(organization) = self.cloud.organization.take() {
            return super::cloud::CloudRun::new(
                self.run,
                organization,
                self.cloud,
                self.output.json,
            )?
            .control(super::cloud::Control::Stop {
                before: self.before,
                reason: self.reason,
            });
        }
        super::control::execute(STOP_COMMAND, &self.run, self.output.json, |target| {
            self.stop(target)
        })
    }

    fn stop(&self, target: &ControlTarget) -> Result<Acted, ControlFailure> {
        if super::control::exited(target) {
            return Err(Refusal::new("run_exited", "The run has exited.").into());
        }
        if !target.step_ids().contains(&self.before.as_str()) {
            return Err(
                Refusal::new("step_unknown", "The run's workflow has no such step.").into(),
            );
        }
        let controls = read_controls(&target.run_directory)?;
        if stop_in_force(&controls, &self.before).is_some() {
            return Err(Refusal::new(
                "stop_in_force",
                "A stop is already in force before this step.",
            )
            .into());
        }
        let payload = Map::from_iter([
            ("before".to_owned(), Value::String(self.before.clone())),
            ("reason".to_owned(), Value::String(self.reason.clone())),
        ]);
        let control = target.record(CONTROL_STOP, None, Some(payload))?;
        Ok(Acted::recorded(control, target.wait().cloned()))
    }
}

impl Lift {
    pub(super) fn execute(mut self) -> super::super::super::CommandResult {
        if let Some(organization) = self.cloud.organization.take() {
            return super::cloud::CloudRun::new(
                self.run,
                organization,
                self.cloud,
                self.output.json,
            )?
            .control(super::cloud::Control::Lift {
                before: self.before,
            });
        }
        super::control::execute(LIFT_COMMAND, &self.run, self.output.json, |target| {
            self.lift(target)
        })
    }

    fn lift(&self, target: &ControlTarget) -> Result<Acted, ControlFailure> {
        let controls = read_controls(&target.run_directory)?;
        let in_force = controls
            .iter()
            .filter(|control| control.event == CONTROL_STOP)
            .filter_map(|control| control.payload.as_ref()?.get("before")?.as_str())
            .filter(|step| stop_in_force(&controls, step).is_some())
            .collect::<BTreeSet<_>>();
        let step = match (&self.before, in_force.len()) {
            (Some(step), _) if in_force.contains(step.as_str()) => step.as_str(),
            (None, 1) => in_force.first().copied().unwrap_or_default(),
            (None, 2..) => {
                return Err(Refusal::new(
                    "stop_ambiguous",
                    "Several stops are in force; name the one to lift with --before STEP.",
                )
                .into());
            }
            _ => {
                return Err(Refusal::new(
                    "stop_not_in_force",
                    "No stop is in force for the run or the named step.",
                )
                .into());
            }
        };
        let Some(stop) = stop_in_force(&controls, step) else {
            return Err(
                Refusal::new("stop_not_in_force", "No stop is in force for the step.").into(),
            );
        };
        // The run's `stop` wait for this step, when it is in one, is the wait the lift acts on.
        let wait = target
            .wait()
            .filter(|wait| wait.kind == WaitKind::Stop && wait.then == step)
            .map(um_execution::WaitRecord::id);
        let payload = Map::from_iter([
            ("before".to_owned(), Value::String(step.to_owned())),
            ("stop".to_owned(), Value::String(stop.at.clone())),
        ]);
        let control = target.record(CONTROL_LIFT, wait, Some(payload))?;
        Ok(Acted::recorded(control, target.wait().cloned()))
    }
}
