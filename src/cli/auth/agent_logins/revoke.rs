use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_api::AgentLoginFailure;
use um_human_auth::Deployment;

use crate::cli::grant::confirmation::NATIVE_REVOKE;

use super::{Confirmation, Options, Stated, Statement, output, run_leaf};

pub(super) const ABOUT: &str = "Revoke one of your coding agent logins";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "AGENT_LOGIN", help = "Agent login ID")]
    agent_login_id: String,

    #[command(flatten)]
    confirmation: Confirmation<Self>,

    #[command(flatten)]
    options: Options,
}

/// The stop Cloud requests of each runner's run (REQ-GRANT-48).
impl Stated for Command {
    const COMMAND: &'static str = "um grant revoke";
    type Usage = crate::cli::grant::revoke::Command;
    const STATEMENT: Statement = Statement::Fixed(NATIVE_REVOKE);
    const ACTION: &'static str = "to revoke the agent login";
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        super::state(
            format_args!("Revoking agent login {}:", self.agent_login_id),
            NATIVE_REVOKE,
        )?;
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
                }?;
                // A revoked agent login is still read back (REQ-GRANT-13). The read only
                // reports each runner's removal and stop; its failure leaves the revocation done.
                Ok(api.get(&self.agent_login_id).ok())
            },
            |deployment, after, json| {
                output::write_revoke(deployment, &self.agent_login_id, after.as_ref(), json)
            },
        )
    }
}
