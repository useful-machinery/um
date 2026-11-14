use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use um_runner::{
    AgentLoginProvider, AgentLoginReadiness, ControlError, Operation, ProviderReadiness,
    RequestFailure, Response,
};
use um_support::{CredentialStatus, NextAction};

use super::super::{CommandFailure, CommandResult, JsonArgs, RunnerJson};

pub(super) const ABOUT: &str = "Set up agent logins on the live runner";
pub(super) const GRANT_ABOUT: &str = "Set up grants on the live runner";
pub(super) const GRANT_SYNC_ABOUT: &str = "Ask Runner Serve to set up grants now";

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
    /// Read the control socket path from the closed runner operator
    /// configuration; the runner home under the um home is the default, read
    /// without creating anything.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    #[command(flatten)]
    output: JsonArgs<RunnerJson>,
}

impl Command {
    /// Runs the family under `path`, the spelling the invocation used:
    /// `runner grant` or its earlier, hidden spelling (REQ-GRANT-41,
    /// REQ-GRANT-77).
    pub(super) fn execute(self, path: &[&str]) -> CommandResult {
        match self.command {
            None => super::super::print_help(path),
            Some(AgentLoginCommand::Sync(command)) => command.execute(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SyncOutput<'a, Check: Serialize> {
    schema_version: u8,
    outcome: &'static str,
    agent_logins: ReadinessView<'a>,
    /// Each login whose check in the setup gave no answer, with why, as
    /// the control socket names them (REQ-GRANT-41); absent when there is
    /// none.
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    unanswered_checks: &'a [Check],
}

/// The readiness the control socket returned, each provider with its
/// availability and next action (REQ-GRANT-53); the control socket
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
    // REQ-GRANT-41: the live runner runs the setup, checks included; this
    // command contacts no Cloud deployment itself.
    fn execute(self) -> CommandResult {
        let socket_path = super::configured_control_socket_path(self.config.as_deref())?;
        let synced = match um_runner::request(&socket_path, Operation::SyncAgentLogins) {
            Ok(Response::AgentLoginsSynced(synced)) => synced,
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
        let readiness = &synced.readiness;
        let mut stdout = io::stdout().lock();
        if self.output.json {
            serde_json::to_writer_pretty(
                &mut stdout,
                &SyncOutput {
                    schema_version: 1,
                    outcome: "synced",
                    agent_logins: ReadinessView::of(readiness),
                    unanswered_checks: &synced.unanswered_checks,
                },
            )
            .context("write agent-login readiness")?;
            writeln!(stdout).context("write agent-login readiness")?;
        } else {
            write_readiness(&mut stdout, readiness)
                .and_then(|()| {
                    write_unanswered_checks(
                        &mut stdout,
                        synced
                            .unanswered_checks
                            .iter()
                            .map(|check| (check.provider, check.cause)),
                    )
                })
                .context("write agent-login readiness")?;
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
/// installed or a token it was issued the generation, its availability with
/// the next action and the step it names (REQ-GRANT-53, REQ-GRANT-88) and,
/// for a login with one, its last use and when, the time left out where the
/// serving process states none (REQ-GRANT-41).
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
        let step = if entry.provider.is_brokered() {
            grant_step(status, entry.provider)
        } else {
            let login = Login {
                provider: &super::status::enum_json(entry.provider)?,
                id: entry.agent_login_id.as_deref(),
            };
            runner_step(status, Some(login))
        };
        let last_use = match entry.last_use {
            Some(last_use) => {
                let last_use = super::status::enum_json(last_use)?;
                match entry.last_use_at.as_deref() {
                    Some(at) => format!(" · last use: {last_use} at {at}"),
                    None => format!(" · last use: {last_use}"),
                }
            }
            None => String::new(),
        };
        writeln!(
            output,
            "{label:<19}{state}{generation}, {}{last_use}",
            status.text(step.as_deref())
        )?;
    }
    Ok(())
}

/// Writes, after the readiness lines, one line per login whose check gave
/// no answer, naming its provider and the cause (REQ-GRANT-41). A check
/// with no answer says nothing of the login, so it changes no exit status.
fn write_unanswered_checks<Provider: Serialize, Cause: Serialize>(
    output: &mut impl Write,
    checks: impl IntoIterator<Item = (Provider, Cause)>,
) -> io::Result<()> {
    for (provider, cause) in checks {
        let label = format!("{} check:", super::status::enum_json(provider)?);
        writeln!(
            output,
            "{label:<19}no answer ({})",
            super::status::enum_json(cause)?
        )?;
    }
    Ok(())
}

/// The step an OAuth provider's next action names (REQ-GRANT-88). Its entry
/// is one assignment's outcome, which no runner command changes: the next
/// assignment the runner admits asks for a token again, and only the
/// grant's owner can consent again.
fn grant_step(status: CredentialStatus, provider: AgentLoginProvider) -> Option<String> {
    let provider = provider.grant_provider().unwrap_or(provider.as_str());
    match status.next_action {
        NextAction::SyncRunner => {
            Some("the next assignment the runner admits asks for a token again".to_owned())
        }
        NextAction::CheckTarget => Some(
            "the deployment issues no tokens for this provider; the next assignment the runner admits asks again"
                .to_owned(),
        ),
        NextAction::Reconnect => Some(format!(
            "the owner of the grant consents again with `um grant create {provider} --replace`; only they can"
        )),
        _ => runner_step(status, None),
    }
}

/// The login a runner's next action is about: its provider, and the agent
/// login the runner holds for it when the runner names one.
pub(super) struct Login<'a> {
    pub(super) provider: &'a str,
    pub(super) id: Option<&'a str>,
}

/// The command or step a runner's next action names, each command a
/// `um grant` or `um runner grant` one (REQ-GRANT-77). Reconnecting a login
/// and restoring its owner's membership are the agent login's owner's to do,
/// not the runner operator's, so those name the agent login.
pub(super) fn runner_step(status: CredentialStatus, login: Option<Login<'_>>) -> Option<String> {
    let id = login.as_ref().and_then(|login| login.id);
    let agent_login = id.map_or_else(
        || "the agent login".to_owned(),
        |id| format!("agent login {id}"),
    );
    match status.next_action {
        NextAction::Reconnect => Some(format!(
            "the owner of {agent_login} stores a new sign-in with `um grant create {} --replace`; only they can",
            login.as_ref().map_or("PROVIDER", |login| login.provider)
        )),
        NextAction::RestoreMembership => Some(format!(
            "the owner of {agent_login} must be an active member of the organization again; it is theirs to settle, not the runner's"
        )),
        NextAction::SyncRunner => {
            Some("set grants up again with `um runner grant sync`".to_owned())
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
