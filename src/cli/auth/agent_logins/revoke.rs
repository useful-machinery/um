use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_api::AgentLoginFailure;
use um_human_auth::Deployment;

use super::{Options, output, run_leaf};

pub(super) const ABOUT: &str = "Revoke one of your coding agent logins";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "AGENT_LOGIN", help = "Agent login ID")]
    agent_login_id: String,

    #[command(flatten)]
    confirmation: super::super::super::ConfirmationArgs,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = um_support::generate_idempotency_key()
            .context("generate agent login revocation request key")?;
        run_leaf(
            deployment,
            &self.options,
            |api| {
                // A lost response may have revoked the agent login. Replay only this key, once.
                match api.revoke(&self.agent_login_id, &key) {
                    Err(AgentLoginFailure::Unreachable { .. }) => {
                        api.revoke(&self.agent_login_id, &key)
                    }
                    result => result,
                }
            },
            |deployment, (), json| output::write_revoke(deployment, &self.agent_login_id, json),
        )
    }
}
