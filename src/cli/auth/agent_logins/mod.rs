mod authorize;
mod codex_login;
mod deauthorize;
mod import;
mod list;
mod output;
mod revoke;
mod show;

use clap::{Args, Subcommand};

use crate::exit_code::ExitCode;
use um_api::{AgentLoginApi, AgentLoginFailure};
use um_human_auth::Deployment;

pub(super) const ABOUT: &str = "Manage your coding agent logins";

#[derive(Debug)]
pub(super) struct AgentLoginJson;

impl super::super::JsonOutputKind for AgentLoginJson {
    const OUTPUT: super::super::JsonOutput = super::super::JsonOutput::Family("agent login");
}

// The agent-login API admits only active humans, so these leaves take no service API key.
type Options = super::super::CommonArgs<AgentLoginJson, super::super::NoAuthenticationArgs>;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<AgentLoginCommand>,
}

#[derive(Debug, Subcommand)]
enum AgentLoginCommand {
    #[command(about = authorize::ABOUT)]
    Authorize(authorize::Command),
    #[command(about = deauthorize::ABOUT)]
    Deauthorize(deauthorize::Command),
    #[command(about = import::ABOUT)]
    Import(import::Command),
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = revoke::ABOUT)]
    Revoke(revoke::Command),
    #[command(about = show::ABOUT)]
    Show(show::Command),
}

/// The agent login and the runner an authorization joins. The runner is named by ID: the
/// member's API has no organization in its path to resolve a runner name in.
#[derive(Debug, Args)]
struct RunnerTarget {
    #[arg(value_name = "AGENT_LOGIN", help = "Agent login ID")]
    agent_login_id: String,

    #[arg(
        long = "runner",
        value_name = "RUNNER_ID",
        value_parser = parse_runner_id,
        help = "Runner ID, as um runner list prints it"
    )]
    runner_id: String,
}

fn parse_runner_id(value: &str) -> Result<String, String> {
    if um_support::valid_typed_id(value, "rnr_") {
        Ok(value.to_owned())
    } else {
        Err(
            "runner must be an exact rnr_ identifier; um runner list ORGANIZATION prints it"
                .to_owned(),
        )
    }
}

/// The label rule of REQ-AGENTLOGIN-1, checked before anything is read, signed in to or sent:
/// 1 to 64 characters, none a control character. Cloud remains the judge of the rest.
fn parse_label(value: &str) -> Result<String, String> {
    if (1..=64).contains(&value.chars().count()) && !value.chars().any(char::is_control) {
        Ok(value.to_owned())
    } else {
        Err("label must be 1 to 64 printable characters".to_owned())
    }
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::super::execute_deployment_command(
            self.command,
            &["auth", "agent-login"],
            "configure Useful Machinery agent login access",
            |command, deployment| match command {
                AgentLoginCommand::Authorize(command) => {
                    command.run(deployment).map_err(Into::into)
                }
                AgentLoginCommand::Deauthorize(command) => {
                    command.run(deployment).map_err(Into::into)
                }
                AgentLoginCommand::Import(command) => command.run(deployment),
                AgentLoginCommand::List(command) => command.run(deployment).map_err(Into::into),
                AgentLoginCommand::Revoke(command) => command.run(deployment).map_err(Into::into),
                AgentLoginCommand::Show(command) => command.run(deployment).map_err(Into::into),
            },
        )
    }
}

impl super::super::HumanSessionApi for AgentLoginApi {
    type Failure = AgentLoginFailure;
    type BuildError = um_api::AgentLoginApiError;

    fn build(
        api_url: &str,
        token: &str,
        policy: um_api::HttpTransportPolicy,
    ) -> Result<Self, Self::BuildError> {
        Self::new(api_url, token, policy)
    }

    fn credential_rejected(failure: &AgentLoginFailure) -> bool {
        failure.credential_rejected()
    }

    fn unauthenticated() -> AgentLoginFailure {
        AgentLoginFailure::Unauthenticated
    }

    fn unreachable(category: um_api::UnreachableCategory) -> AgentLoginFailure {
        AgentLoginFailure::Unreachable { category }
    }
}

/// Runs `f` against the agent-login API with the signed-in human session, refreshing a
/// rejected session once.
fn with_api<T>(
    deployment: &Deployment,
    options: &Options,
    f: impl FnMut(&AgentLoginApi) -> Result<T, AgentLoginFailure>,
) -> anyhow::Result<Result<T, AgentLoginFailure>> {
    super::super::with_human_session_api(deployment, options.http.transport_policy(), f)
}

/// Runs one leaf's operation and reports its result through `write`, or its failure.
fn run_leaf<T>(
    deployment: &Deployment,
    options: &Options,
    operation: impl FnMut(&AgentLoginApi) -> Result<T, AgentLoginFailure>,
    write: impl FnOnce(&str, &T, bool) -> anyhow::Result<ExitCode>,
) -> anyhow::Result<ExitCode> {
    let result = with_api(deployment, options, operation)?;
    let deployment = deployment.fingerprint().api_url();
    match result {
        Ok(value) => write(deployment, &value, options.json),
        Err(failure) => output::failure(deployment, &failure, options.json),
    }
}
