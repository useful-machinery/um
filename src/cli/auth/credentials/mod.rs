//! `um auth credential`: the member's provider grants and agent logins, each with its
//! availability and next action (REQ-AGENTLOGIN-45). The group holds no state of its own: it
//! reads the grant and agent-login lists and maps what Cloud returns through `projection`.

mod output;
pub(super) mod projection;
mod status;

use clap::{Args, Subcommand};

pub(super) const ABOUT: &str = "See whether your credentials can be used";

#[derive(Debug)]
struct CredentialJson;

impl super::super::JsonOutputKind for CredentialJson {
    const OUTPUT: super::super::JsonOutput = super::super::JsonOutput::Family("credential status");
}

// Both lists it reads admit only active humans, so the leaf takes no service API key.
type Options = super::super::CommonArgs<CredentialJson, super::super::NoAuthenticationArgs>;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<CredentialCommand>,
}

#[derive(Debug, Subcommand)]
enum CredentialCommand {
    #[command(about = status::ABOUT)]
    Status(status::Command),
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        super::super::execute_deployment_command(
            self.command,
            &["auth", "credential"],
            "read Useful Machinery credential status",
            |command, deployment| match command {
                CredentialCommand::Status(command) => command.run(deployment).map_err(Into::into),
            },
        )
    }
}
