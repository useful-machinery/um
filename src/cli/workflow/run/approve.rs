//! `um workflow run approve` and `um workflow run hand-back` (REQ-LOCALRUN-183).

use clap::Args;
use serde_json::{Map, Value};
use um_execution::{CONTROL_APPROVE, CONTROL_HAND_BACK, WaitKind};

use super::control::{Acted, ControlFailure, ControlTarget};

pub(super) const APPROVE_ABOUT: &str = "Approve a run waiting for approval";
pub(super) const HAND_BACK_ABOUT: &str = "Hand a run waiting for approval back with notes";
const APPROVE_COMMAND: &str = "um workflow run approve";
const HAND_BACK_COMMAND: &str = "um workflow run hand-back";

#[derive(Debug, Args)]
pub(super) struct Approve {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[command(flatten)]
    output: super::control::JsonOutput,
}

#[derive(Debug, Args)]
pub(super) struct HandBack {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[arg(
        long,
        value_name = "TEXT",
        help = "What the step the run returns to should change"
    )]
    notes: String,

    #[command(flatten)]
    output: super::control::JsonOutput,
}

impl Approve {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        super::control::execute(APPROVE_COMMAND, &self.run, self.output.json, |target| {
            resolve_approval(target, CONTROL_APPROVE, None)
        })
    }
}

impl HandBack {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        super::control::execute(HAND_BACK_COMMAND, &self.run, self.output.json, |target| {
            let payload = Map::from_iter([("notes".to_owned(), Value::String(self.notes))]);
            resolve_approval(target, CONTROL_HAND_BACK, Some(payload))
        })
    }
}

/// Appends `event` naming the run's `approval` wait.
fn resolve_approval(
    target: &ControlTarget,
    event: &str,
    payload: Option<Map<String, Value>>,
) -> Result<Acted, ControlFailure> {
    let wait = target.wait_to_resolve(WaitKind::Approval)?;
    let control = target.record(event, Some(wait.id()), payload)?;
    Ok(Acted::recorded(control, Some(wait.clone())))
}
