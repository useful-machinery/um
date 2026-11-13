use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_api::AgentLoginFailure;
use um_human_auth::Deployment;

use super::{Confirmation, Options, RunnerTarget, Stated, output, run_leaf};

pub(super) const ABOUT: &str = "End a runner's authorization for one of your coding agent logins";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    target: RunnerTarget,

    #[command(flatten)]
    confirmation: Confirmation<Self>,

    #[command(flatten)]
    options: Options,
}

/// The stop Cloud requests of the runner's run (REQ-AGENTLOGIN-38).
impl Stated for Command {
    const NAME: &'static str = "deauthorize";
    const STATEMENT: &'static [&'static str] =
        &["the run the runner is serving will be asked to stop, whichever member started it"];
    const ACTION: &'static str = "to end the runner's authorization";
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let RunnerTarget {
            agent_login_id,
            runner_id,
        } = &self.target;
        super::state::<Self>(format_args!(
            "Ending runner {runner_id}'s authorization for agent login {agent_login_id}:"
        ))?;
        let key = um_support::generate_idempotency_key()
            .context("generate agent login deauthorization request key")?;
        run_leaf(
            deployment,
            &self.options,
            |api| {
                // The agent login lists only its authorizations in force (REQ-AGENTLOGIN-3).
                let authorization_id = api
                    .get(agent_login_id)?
                    .authorizations
                    .into_iter()
                    .find(|authorization| authorization.runner_id == *runner_id)
                    .ok_or(AgentLoginFailure::AuthorizationNotFound)?
                    .id;
                // A lost response may have ended the authorization. Replay only this key, once.
                match api.deauthorize(agent_login_id, &authorization_id, &key) {
                    Err(AgentLoginFailure::Unreachable { .. }) => {
                        api.deauthorize(agent_login_id, &authorization_id, &key)
                    }
                    result => result,
                }?;
                // The read only reports the runner's removal and stop; its failure leaves the
                // authorization ended.
                Ok((authorization_id, api.get(agent_login_id).ok()))
            },
            |deployment, (authorization_id, after), json| {
                output::write_deauthorize(
                    deployment,
                    agent_login_id,
                    authorization_id,
                    runner_id,
                    after.as_ref(),
                    json,
                )
            },
        )
    }
}
