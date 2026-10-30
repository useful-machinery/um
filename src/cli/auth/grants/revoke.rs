use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_api::GrantFailure;
use um_human_auth::Deployment;

use super::{Options, output, with_api};

pub(super) const ABOUT: &str = "Revoke a provider grant";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "GRANT", help = "Provider grant ID")]
    grant_id: String,

    #[command(flatten)]
    confirmation: super::super::super::ConfirmationArgs,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = um_support::generate_idempotency_key()
            .context("generate provider grant revocation request key")?;
        let result = with_api(deployment, &self.options, |api| {
            // A lost response may have revoked the grant. Replay only this key, once.
            match api.revoke(&self.grant_id, &key) {
                Err(GrantFailure::Unreachable { .. }) => api.revoke(&self.grant_id, &key),
                result => result,
            }
        })?;
        let deployment = deployment.fingerprint().api_url();
        match result {
            Ok(()) => output::write_revoke(deployment, &self.grant_id, self.options.json),
            Err(failure) => output::failure(deployment, &failure, self.options.json),
        }
    }
}
