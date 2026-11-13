mod create;
mod list;
mod output;
mod revoke;
mod uses;

use clap::{Args, Subcommand};

use um_api::{GrantApi, GrantFailure};

// `um auth credential status` reports a failed read of the grants as `um auth grant` does.
pub(in crate::cli::auth) use output::failure;
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

impl super::super::HumanSessionApi for GrantApi {
    type Failure = GrantFailure;
    type BuildError = um_api::GrantApiError;

    fn build(
        api_url: &str,
        token: &str,
        policy: um_api::HttpTransportPolicy,
    ) -> Result<Self, Self::BuildError> {
        Self::new(api_url, token, policy)
    }

    fn credential_rejected(failure: &GrantFailure) -> bool {
        failure.credential_rejected()
    }

    fn unauthenticated() -> GrantFailure {
        GrantFailure::Unauthenticated
    }

    fn unreachable(category: um_api::UnreachableCategory) -> GrantFailure {
        GrantFailure::Unreachable { category }
    }
}

/// Runs `f` against the grant API with the signed-in human session, refreshing a rejected
/// session once.
fn with_api<T, F: super::super::JsonOutputKind>(
    deployment: &Deployment,
    options: &super::super::CommonArgs<F, super::super::NoAuthenticationArgs>,
    f: impl FnMut(&GrantApi) -> Result<T, GrantFailure>,
) -> anyhow::Result<Result<T, GrantFailure>> {
    super::super::with_human_session_api(deployment, options.http.transport_policy(), f)
}
