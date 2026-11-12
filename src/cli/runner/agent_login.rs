use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use um_runner::{AgentLoginReadiness, ControlError, Operation, RequestFailure, Response};

use super::super::{CommandFailure, CommandResult, JsonArgs, RunnerJson};

pub(super) const ABOUT: &str = "Set up agent logins on the live runner";
const COMMAND_PATH: &[&str] = &["runner", "agent-login"];

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<AgentLoginCommand>,
}

#[derive(Debug, Subcommand)]
enum AgentLoginCommand {
    #[command(about = "Ask Runner Serve to set up agent logins now")]
    Sync(SyncCommand),
}

#[derive(Debug, Args)]
struct SyncCommand {
    /// Read the control socket path from the closed runner operator configuration.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,

    #[command(flatten)]
    output: JsonArgs<RunnerJson>,
}

impl Command {
    pub(super) fn execute(self) -> CommandResult {
        match self.command {
            None => super::super::print_help(COMMAND_PATH),
            Some(AgentLoginCommand::Sync(command)) => command.execute(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SyncOutput<'a> {
    schema_version: u8,
    outcome: &'static str,
    agent_logins: &'a AgentLoginReadiness,
}

impl SyncCommand {
    // REQ-AGENTLOGIN-31: the live runner runs the setup pass; this command
    // contacts no Cloud deployment itself.
    fn execute(self) -> CommandResult {
        let config_path = super::operator_config_path(&self.config)?;
        let socket_path = um_runner::load_control_socket_path(&config_path).with_context(|| {
            format!(
                "load runner operator configuration {}",
                config_path.display()
            )
        })?;
        let readiness = match um_runner::request(&socket_path, Operation::SyncAgentLogins) {
            Ok(Response::AgentLoginsSynced(readiness)) => readiness,
            Ok(Response::Error(error)) => return Err(control_failure(error)),
            Ok(Response::Status(_) | Response::Reloaded { .. }) => {
                return Err(CommandFailure::for_outcome(
                    anyhow!("Runner Serve control protocol is invalid"),
                    OutcomeClass::Protocol,
                ));
            }
            Err(error) => {
                let outcome = match error {
                    RequestFailure::NotReachable => OutcomeClass::Unreachable,
                    RequestFailure::Protocol(_) => OutcomeClass::Protocol,
                };
                return Err(CommandFailure::for_outcome(
                    anyhow::Error::new(error),
                    outcome,
                ));
            }
        };
        let mut stdout = io::stdout().lock();
        if self.output.json {
            serde_json::to_writer_pretty(
                &mut stdout,
                &SyncOutput {
                    schema_version: 1,
                    outcome: "synced",
                    agent_logins: &readiness,
                },
            )
            .context("write agent-login readiness")?;
            writeln!(stdout).context("write agent-login readiness")?;
        } else {
            write_readiness(&mut stdout, &readiness).context("write agent-login readiness")?;
        }
        Ok(
            if readiness
                .providers
                .iter()
                .any(|entry| entry.state.fails_readiness())
            {
                ExitCode::GeneralFailure
            } else {
                ExitCode::Success
            },
        )
    }
}

fn control_failure(error: ControlError) -> CommandFailure {
    match error {
        ControlError::RunnerBusy => CommandFailure::with_exit_code(
            anyhow!(
                "Runner Serve holds an assignment and did not finish agent-login setup (runner_busy); retry when it is idle"
            ),
            ExitCode::Unavailable,
        ),
        // Only a serving process older than this release refuses the
        // well-formed request this command sends.
        ControlError::InvalidRequest => CommandFailure::new(anyhow!(
            "Runner Serve does not offer agent-login setup (invalid_request); restart the running `um runner serve` on this release"
        )),
        error => CommandFailure::new(anyhow!(
            "Runner Serve could not set up agent logins ({})",
            super::status::enum_json(error).unwrap_or_else(|_| "unknown".to_owned())
        )),
    }
}

/// Writes one line per provider: its readiness state and, for a login the
/// runner installed, the generation.
pub(super) fn write_readiness(
    output: &mut impl Write,
    readiness: &AgentLoginReadiness,
) -> io::Result<()> {
    for entry in &readiness.providers {
        let label = format!("{} login:", super::status::enum_json(entry.provider)?);
        let state = super::status::enum_json(entry.state)?;
        match entry.generation {
            Some(generation) => {
                writeln!(output, "{label:<19}{state} (generation {generation})")?;
            }
            None => writeln!(output, "{label:<19}{state}")?,
        }
    }
    Ok(())
}
