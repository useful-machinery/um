use clap::Args;

use crate::exit_code::ExitCode;
use um_human_auth::Deployment;

use super::{Options, output, run_leaf};

pub(super) const ABOUT: &str = "List your coding agent logins";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    pagination: super::super::super::PaginationArgs,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let limit = self.pagination.limit.map(i32::from);
        let cursor = self.pagination.cursor.as_deref();
        run_leaf(
            deployment,
            &self.options,
            |api| api.list(limit, cursor),
            output::write_list,
        )
    }
}
