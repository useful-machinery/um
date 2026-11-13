use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_api::MemberGrantKind;
use um_human_auth::Deployment;

use super::confirmation::{Confirmation, NATIVE_REVOKE, Stated, Statement};
use super::{GRANT_HELP, GrantArg, Options, act_on, output, replayed};

pub(super) const ABOUT: &str = "Revoke one of your grants everywhere";

#[derive(Debug, Args)]
pub(in crate::cli) struct Command {
    #[arg(value_name = "GRANT", value_parser = super::parse_grant, help = GRANT_HELP)]
    grant: GrantArg,

    #[command(flatten)]
    confirmation: Confirmation<Self>,

    #[command(flatten)]
    options: Options,
}

/// What revoking does (REQ-GRANT-68): an OAuth grant is erased and revoked at the provider,
/// and a native grant's runners' runs are asked to stop (REQ-GRANT-48).
impl Stated for Command {
    const COMMAND: &'static str = "um grant revoke";
    type Usage = Self;
    const STATEMENT: Statement = Statement::ByKind {
        oauth: &[
            "the grant is erased, and revoked at the provider where the provider offers it; no \
             run can be issued a token from it again",
        ],
        native: NATIVE_REVOKE,
    };
    const ACTION: &'static str = "to revoke the grant";
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = um_support::generate_idempotency_key()
            .context("generate grant revocation request key")?;
        act_on::<Self, _>(
            deployment,
            &self.options,
            &self.grant,
            |grant| format!("Revoking grant {}:", grant.id),
            |api, grant| {
                replayed(|| api.revoke(&grant.id, &key))?;
                // A revoked native grant is still read back (REQ-GRANT-13). The read only
                // reports each runner's removal and stop; its failure leaves the revocation
                // done. An OAuth grant's runners kept no copy.
                Ok((grant.kind == MemberGrantKind::GrantKindNative)
                    .then(|| api.get(&grant.id).ok()))
            },
            |api_url, grant, after, json| {
                output::write_revoke(api_url, grant, after.as_ref().map(Option::as_ref), json)
            },
            |id| format!("um grant revoke {id} --yes"),
        )
    }
}
