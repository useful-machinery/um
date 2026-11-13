use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use um_runner::{
    AgentLoginReadiness, ControlError, Operation, ProviderReadiness, RequestFailure, Response,
};
use um_support::{CredentialStatus, NextAction};

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
    agent_logins: ReadinessView<'a>,
}

/// The readiness the control socket returned, each provider with its
/// availability and next action (REQ-AGENTLOGIN-43); the control socket
/// carries only the facts.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadinessView<'a> {
    updated_at: &'a str,
    providers: Vec<ProviderView<'a>>,
}

#[derive(Serialize)]
struct ProviderView<'a> {
    #[serde(flatten)]
    readiness: &'a ProviderReadiness,
    #[serde(flatten)]
    status: CredentialStatus,
}

impl<'a> ReadinessView<'a> {
    fn of(readiness: &'a AgentLoginReadiness) -> Self {
        Self {
            updated_at: &readiness.updated_at,
            providers: readiness
                .providers
                .iter()
                .map(|entry| ProviderView {
                    readiness: entry,
                    status: entry.credential_status(),
                })
                .collect(),
        }
    }
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
                    agent_logins: ReadinessView::of(&readiness),
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
                .any(ProviderReadiness::fails_readiness)
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

/// Writes one line per provider: its readiness state, for a login the runner
/// installed the generation, and its availability with the next action and
/// the step it names (REQ-AGENTLOGIN-43).
pub(super) fn write_readiness(
    output: &mut impl Write,
    readiness: &AgentLoginReadiness,
) -> io::Result<()> {
    for entry in &readiness.providers {
        let label = format!("{} login:", super::status::enum_json(entry.provider)?);
        let state = super::status::enum_json(entry.state)?;
        let generation = entry
            .generation
            .map(|generation| format!(" (generation {generation})"))
            .unwrap_or_default();
        let status = entry.credential_status();
        let step = runner_step(status, entry.agent_login_id.as_deref());
        writeln!(
            output,
            "{label:<19}{state}{generation}, {}",
            status.text(step.as_deref())
        )?;
    }
    Ok(())
}

/// The command or step a runner's next action names. Reconnecting a login
/// and restoring its owner's membership are the agent login's owner's to do,
/// not the runner operator's, so those name the agent login.
pub(super) fn runner_step(
    status: CredentialStatus,
    agent_login_id: Option<&str>,
) -> Option<String> {
    let agent_login = agent_login_id.map_or_else(
        || "the agent login".to_owned(),
        |id| format!("agent login {id}"),
    );
    match status.next_action {
        NextAction::Reconnect => Some(format!(
            "the owner of {agent_login} stores a new sign-in with `um auth agent-login reconnect {}`; only they can",
            agent_login_id.unwrap_or("AGENT_LOGIN")
        )),
        NextAction::RestoreMembership => Some(format!(
            "the owner of {agent_login} must be an active member of the organization again; it is theirs to settle, not the runner's"
        )),
        NextAction::SyncRunner => {
            Some("set agent logins up again with `um runner agent-login sync`".to_owned())
        }
        NextAction::CheckTarget => Some(
            "the deployment does not serve this provider for the runner's operating system"
                .to_owned(),
        ),
        NextAction::ResolveServiceLogin => Some(
            "the service's own login is in use; remove it from the service environment to use the agent login"
                .to_owned(),
        ),
        NextAction::None
        | NextAction::WaitForRunner
        | NextAction::AuthorizeRunner
        | NextAction::Connect => None,
    }
}
