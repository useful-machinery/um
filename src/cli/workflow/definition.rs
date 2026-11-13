mod cloud;
mod enabled;
mod list;
mod publish;
mod report;
mod show;
mod validate;

use anyhow::Context;
use clap::{Args, Subcommand};

use crate::exit_code::ExitCode;
use um_execution::DefinitionError;

pub(super) const ABOUT: &str = "Work with local workflow definitions and their trees";
const COMMAND_PATH: &[&str] = &["workflow", "definition"];
const ID_HELP: &str = "Workflow definition id, as list prints it";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<DefinitionCommand>,
}

#[derive(Debug, Subcommand)]
enum DefinitionCommand {
    #[command(about = cloud::ABOUT)]
    Cloud(cloud::Command),
    #[command(about = enabled::DISABLE_ABOUT)]
    Disable(enabled::Command),
    #[command(about = enabled::ENABLE_ABOUT)]
    Enable(enabled::Command),
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = publish::ABOUT)]
    Publish(publish::Command),
    #[command(about = show::ABOUT)]
    Show(show::Command),
    #[command(about = validate::ABOUT)]
    Validate(validate::Command),
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        match self.command {
            None => super::super::print_help(COMMAND_PATH),
            Some(DefinitionCommand::Cloud(command)) => command.execute(),
            Some(DefinitionCommand::Disable(command)) => command.execute(false),
            Some(DefinitionCommand::Enable(command)) => command.execute(true),
            Some(DefinitionCommand::List(command)) => command.execute(),
            Some(DefinitionCommand::Publish(command)) => command.execute(),
            Some(DefinitionCommand::Show(command)) => command.execute(),
            Some(DefinitionCommand::Validate(command)) => command.execute(),
        }
    }
}

/// Writes the outcome of a definition command over the home: `write` with the result and
/// whether `--json` was given, exit 0, or the refusal headed by `refused` (REQ-WFDEF-27),
/// exit 1.
fn finish<T>(
    command: &'static str,
    refused: &str,
    result: Result<T, DefinitionError>,
    json: bool,
    write: impl FnOnce(&T, bool) -> anyhow::Result<()>,
) -> super::super::CommandResult {
    let (written, exit_code) = match result {
        Ok(value) => (write(&value, json), ExitCode::Success),
        Err(error) => (
            report::write_refused(command, refused, &error, json),
            ExitCode::GeneralFailure,
        ),
    };
    written.with_context(|| format!("write {command} result"))?;
    Ok(exit_code)
}

/// `yes` or `no` for a human report field.
fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
