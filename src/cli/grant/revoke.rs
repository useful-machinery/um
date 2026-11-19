use anyhow::Context as _;
use clap::Args;

use crate::cli::CommandResult;
use crate::cli::integration::Reach;
use useful_api::MemberGrantKind;
use useful_human_auth::Deployment;

use super::confirmation::{Confirmation, NATIVE_REVOKE, Stated, Statement};
use super::{GrantArg, INTEGRATION_HELP, LeafRun, Options, Subject, act_on, output, replayed};

pub(super) const ABOUT: &str = "Revoke one of your grants everywhere";

/// What revoking a token does, whoever's it is (REQ-INTEGRATION-20).
const TOKEN_REVOKE: &[&str] =
    &["the token is erased from Cloud; it stays valid at the service until it is ended there"];

#[derive(Debug, Args)]
pub(in crate::cli) struct Command {
    #[arg(value_name = "INTEGRATION", value_parser = super::parse_grant, help = INTEGRATION_HELP)]
    grant: GrantArg,

    #[command(flatten)]
    confirmation: Confirmation<Self>,

    #[command(flatten)]
    options: Options,
}

/// What revoking does (REQ-GRANT-68): an OAuth grant is erased and revoked at the provider,
/// a native grant's runners' runs are asked to stop (REQ-GRANT-48), and a token is erased from
/// Cloud alone (REQ-INTEGRATION-20).
impl Stated for Command {
    const COMMAND: &'static str = "useful integration revoke";
    type Usage = Self;
    const STATEMENT: Statement = Statement::ByKind {
        oauth: &[
            "the grant is erased, and revoked at the provider where the provider offers it; no \
             run can be issued a token from it again",
        ],
        native: NATIVE_REVOKE,
        token: TOKEN_REVOKE,
        org_token: TOKEN_REVOKE,
    };
    const ACTION: &'static str = "to revoke the grant";
}

impl Command {
    /// Revokes what `INTEGRATION` names under `reach`: a grant, or, under `useful integration`,
    /// a token, the member's or with `--org` the organization's (REQ-INTEGRATION-20).
    pub(super) fn run(self, deployment: &Deployment, reach: &Reach) -> CommandResult {
        let key = useful_support::generate_idempotency_key()
            .context("generate grant revocation request key")?;
        let context = LeafRun {
            deployment,
            options: &self.options,
            reach,
        };
        act_on::<Self, _>(
            context,
            &self.grant,
            |named| format!("Revoking {named}:"),
            |api, subject| match subject {
                Subject::Grant(grant) => {
                    replayed(|| api.revoke(&grant.id, &key))?;
                    // A revoked native grant is still read back (REQ-GRANT-13). The read only
                    // reports each runner's removal and stop; its failure leaves the revocation
                    // done. An OAuth grant's runners kept no copy.
                    Ok((grant.kind == MemberGrantKind::GrantKindNative)
                        .then(|| api.get(&grant.id).ok()))
                }
                // A token's runners are issued it no more; nothing is read after.
                Subject::Token(token) => {
                    replayed(|| api.revoke_integration_token(reach.owner(), &token.id, &key))?;
                    Ok(None)
                }
            },
            |api_url, subject, after, json| match subject {
                Subject::Grant(grant) => {
                    output::write_revoke(api_url, grant, after.as_ref().map(Option::as_ref), json)
                }
                Subject::Token(token) => reach.write_revoked(api_url, token, json),
            },
            |id| format!("useful integration revoke {id} --yes"),
        )
    }
}
