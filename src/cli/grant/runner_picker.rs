//! The list `um grant authorize` and `um grant deauthorize` given no `RUNNER` show on a
//! terminal to choose the runner from (REQ-GRANT-82, REQ-GRANT-83, INV-CLI-16). It is the only
//! source that names the prompt crate (INV-CLI-3), which draws on standard error, so that
//! standard output carries only the result, and leaves the terminal as it found it before it
//! returns.

use std::collections::BTreeSet;
use std::io::{self, Write};

use anyhow::Context;
use inquire::{InquireError, Select};

use crate::exit_code::ExitCode;

use super::runner_choice::Runner;

/// What was chosen from a list that offers all the runners as one choice: one runner, or all.
pub(super) enum Picked {
    One(Runner),
    All(Vec<Runner>),
}

/// Shows `runners`, in the order given, under `message` and returns the one chosen. Leaving
/// the list without a choice (Esc) reports that nothing was sent and exits 2; an interrupt
/// (Ctrl-C) ends the command with its status. Any other failure carries its cause.
pub(super) fn pick(
    message: &str,
    runners: Vec<Runner>,
) -> anyhow::Result<Result<Runner, ExitCode>> {
    match select(message, lines(&runners))? {
        Ok(index) => nth(runners, index).map(Ok),
        Err(exit) => Ok(Err(exit)),
    }
}

/// Shows `runners` as [`pick`] does, and, when there is more than one, a last entry that
/// stands for all of them (REQ-GRANT-83), so that Enter alone still takes the first runner.
pub(super) fn pick_one_or_all(
    message: &str,
    runners: Vec<Runner>,
) -> anyhow::Result<Result<Picked, ExitCode>> {
    if runners.len() < 2 {
        return Ok(pick(message, runners)?.map(Picked::One));
    }
    let mut lines = lines(&runners);
    lines.push(format!("All {} runners", runners.len()));
    match select(message, lines)? {
        Ok(index) if index == runners.len() => Ok(Ok(Picked::All(runners))),
        Ok(index) => nth(runners, index).map(|runner| Ok(Picked::One(runner))),
        Err(exit) => Ok(Err(exit)),
    }
}

/// Shows `lines` under `message` and returns the index of the one chosen, or the exit status
/// of leaving the list without a choice.
fn select(message: &str, lines: Vec<String>) -> anyhow::Result<Result<usize, ExitCode>> {
    match Select::new(message, lines).raw_prompt() {
        Ok(chosen) => Ok(Ok(chosen.index)),
        Err(InquireError::OperationCanceled) => {
            writeln!(io::stderr().lock(), "error: no runner chosen, nothing sent")?;
            Ok(Err(ExitCode::UsageError))
        }
        Err(InquireError::OperationInterrupted) => Ok(Err(ExitCode::Interrupted)),
        Err(error) => Err(error).context("show the runners to choose from"),
    }
}

fn nth(runners: Vec<Runner>, index: usize) -> anyhow::Result<Runner> {
    runners
        .into_iter()
        .nth(index)
        .context("read the runner chosen from the list")
}

/// The runners as the list shows them, each with its organization's ID when they are of more
/// than one organization.
fn lines(runners: &[Runner]) -> Vec<String> {
    let organizations = runners
        .iter()
        .map(|runner| runner.organization_id.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    runners
        .iter()
        .map(|runner| runner.line(organizations > 1))
        .collect()
}
