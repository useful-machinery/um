use clap::Args;

use crate::exit_code::ExitCode;
use um_human_auth::Deployment;

use super::{Options, output, with_api};

pub(super) const ABOUT: &str = "List your provider grants";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    pagination: super::super::super::PaginationArgs,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = with_api(deployment, &self.options, |api| {
            api.list(
                self.pagination.limit.map(i32::from),
                self.pagination.cursor.as_deref(),
            )
        })?;
        let deployment = deployment.fingerprint().api_url();
        match result {
            Ok(page) => output::write_list(deployment, &page, self.options.json),
            Err(failure) => output::failure(deployment, &failure, self.options.json),
        }
    }
}
