use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_api::AgentLoginFailure;
use um_human_auth::Deployment;

use super::{Options, RunnerTarget, output, run_leaf};

pub(super) const ABOUT: &str = "End a runner's authorization for one of your coding agent logins";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    target: RunnerTarget,

    #[command(flatten)]
    confirmation: super::super::super::ConfirmationArgs,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let RunnerTarget {
            agent_login_id,
            runner_id,
        } = &self.target;
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
                }
                .map(|()| authorization_id)
            },
            |deployment, authorization_id, json| {
                output::write_deauthorize(
                    deployment,
                    agent_login_id,
                    authorization_id,
                    runner_id,
                    json,
                )
            },
        )
    }
}
