mod create;
mod list;
mod output;
mod revoke;
mod uses;

use clap::{Args, Subcommand};

use um_api::{GrantApi, GrantFailure};
use um_human_auth::Deployment;

pub(super) const ABOUT: &str = "Manage your provider grants";

// The grant API admits only active humans, so these leaves take no service API key.
type Options =
    super::super::CommonArgs<super::super::GrantJson, super::super::NoAuthenticationArgs>;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<GrantCommand>,
}

#[derive(Debug, Subcommand)]
enum GrantCommand {
    #[command(about = create::ABOUT)]
    Create(create::Command),
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = revoke::ABOUT)]
    Revoke(revoke::Command),
    #[command(about = uses::ABOUT)]
    Uses(uses::Command),
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::super::execute_deployment_command(
            self.command,
            &["auth", "grant"],
            "configure Useful Machinery provider grant access",
            |command, deployment| match command {
                GrantCommand::Create(command) => command.run(deployment),
                GrantCommand::List(command) => command.run(deployment).map_err(Into::into),
                GrantCommand::Revoke(command) => command.run(deployment).map_err(Into::into),
                GrantCommand::Uses(command) => command.run(deployment).map_err(Into::into),
            },
        )
    }
}

/// Runs `f` against the grant API with the signed-in human session, refreshing a rejected
/// session once.
fn with_api<T, F: super::super::JsonOutputKind>(
    deployment: &Deployment,
    options: &super::super::CommonArgs<F, super::super::NoAuthenticationArgs>,
    mut f: impl FnMut(&GrantApi) -> Result<T, GrantFailure>,
) -> anyhow::Result<Result<T, GrantFailure>> {
    let policy = options.http.transport_policy();
    let client = super::super::human_session_client(policy)?;
    super::super::execute_required_api_operation_retrying_result(
        &client,
        deployment,
        |token| {
            let api = GrantApi::new(deployment.fingerprint().api_url(), token, policy)
                .map_err(|error| anyhow::anyhow!(error))?;
            Ok(f(&api))
        },
        |result| {
            result
                .as_ref()
                .is_err_and(GrantFailure::credential_rejected)
        },
        || GrantFailure::Unauthenticated,
        |category| GrantFailure::Unreachable { category },
        "acquire human session",
    )
}
