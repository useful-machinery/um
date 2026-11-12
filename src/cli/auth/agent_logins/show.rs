use clap::Args;

use crate::exit_code::ExitCode;
use um_human_auth::Deployment;

use super::{Options, output, run_leaf};

pub(super) const ABOUT: &str = "Show one of your coding agent logins";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "AGENT_LOGIN", help = "Agent login ID")]
    agent_login_id: String,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        run_leaf(
            deployment,
            &self.options,
            |api| api.get(&self.agent_login_id),
            |deployment, login, json| output::write_agent_login(deployment, "shown", login, json),
        )
    }
}
