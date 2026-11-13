use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_api::AgentLoginFailure;
use um_human_auth::Deployment;

use crate::cli::grant::confirmation::NATIVE_AUTHORIZE;

use super::{Confirmation, Options, RunnerTarget, Stated, Statement, output, run_leaf};

pub(super) const ABOUT: &str = "Authorize a runner to receive one of your coding agent logins";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    target: RunnerTarget,

    #[command(flatten)]
    confirmation: Confirmation<Self>,

    #[command(flatten)]
    options: Options,
}

/// Who can use the agent login once a runner holds it (REQ-GRANT-35).
impl Stated for Command {
    const COMMAND: &'static str = "um grant authorize";
    type Usage = crate::cli::grant::authorize::Command;
    const STATEMENT: Statement = Statement::Fixed(NATIVE_AUTHORIZE);
    const ACTION: &'static str = "to authorize the runner";
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let RunnerTarget {
            agent_login_id,
            runner_id,
        } = &self.target;
        super::state(
            format_args!("Authorizing runner {runner_id} for agent login {agent_login_id}:"),
            NATIVE_AUTHORIZE,
        )?;
        let key = um_support::generate_idempotency_key()
            .context("generate agent login authorization request key")?;
        run_leaf(
            deployment,
            &self.options,
            |api| {
                // A lost response may have authorized the runner. Replay only this key, once.
                match api.authorize(agent_login_id, runner_id, &key) {
                    Err(AgentLoginFailure::Unreachable { .. }) => {
                        api.authorize(agent_login_id, runner_id, &key)
                    }
                    result => result,
                }
            },
            |deployment, authorization, json| {
                output::write_authorize(deployment, agent_login_id, authorization, json)
            },
        )
    }
}
