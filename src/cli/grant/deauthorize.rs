use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_api::{MemberGrantFailure, MemberGrantKind};
use um_human_auth::Deployment;

use super::authorization::{Holder, ended_authorization_views, write_deauthorized};
use super::confirmation::{Confirmation, NATIVE_DEAUTHORIZE, Stated, Statement};
use super::{GRANT_HELP, GrantArg, Options, RUNNER_HELP, act_on, replayed};

pub(super) const ABOUT: &str = "End a runner's authorization for one of your grants";

#[derive(Debug, Args)]
pub(in crate::cli) struct Command {
    #[arg(value_name = "GRANT", value_parser = super::parse_grant, help = GRANT_HELP)]
    grant: GrantArg,

    #[arg(value_name = "RUNNER", value_parser = super::parse_runner_id, help = RUNNER_HELP)]
    runner_id: String,

    #[command(flatten)]
    confirmation: Confirmation<Self>,

    #[command(flatten)]
    options: Options,
}

/// What ending the authorization does: for a native grant, the stop Cloud requests of the
/// runner's run (REQ-GRANT-48); for an OAuth grant, no further token, and no stop
/// (REQ-GRANT-56).
impl Stated for Command {
    const COMMAND: &'static str = "um grant deauthorize";
    type Usage = Self;
    const STATEMENT: Statement = Statement::ByKind {
        oauth: &[
            "no run on the runner can be issued a token from the grant again; a token \
                  already issued expires on its own lifetime",
        ],
        native: NATIVE_DEAUTHORIZE,
    };
    const ACTION: &'static str = "to end the runner's authorization";
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let runner_id = &self.runner_id;
        let key = um_support::generate_idempotency_key()
            .context("generate grant deauthorization request key")?;
        act_on::<Self, _>(
            deployment,
            &self.options,
            &self.grant,
            |grant| {
                format!(
                    "Ending runner {runner_id}'s authorization for grant {}:",
                    grant.id
                )
            },
            |api, grant| {
                // The grant lists only its authorizations in force (REQ-GRANT-58).
                let authorization_id = grant
                    .authorizations
                    .iter()
                    .find(|authorization| authorization.runner_id == *runner_id)
                    .ok_or(MemberGrantFailure::AuthorizationNotFound)?
                    .id
                    .clone();
                replayed(|| api.deauthorize(&grant.id, &authorization_id, &key))?;
                // A native grant's runner kept a copy; the read only reports its removal and
                // stop, and its failure leaves the authorization ended.
                let after = (grant.kind == MemberGrantKind::GrantKindNative)
                    .then(|| api.get(&grant.id).ok())
                    .flatten();
                Ok((authorization_id, after))
            },
            |api_url, grant, (authorization_id, after), json| {
                let ended = after
                    .as_ref()
                    .and_then(|after| after.ended_authorizations.as_deref())
                    .map(ended_authorization_views)
                    .into_iter()
                    .flatten()
                    .find(|ended| ended.authorization.id == authorization_id);
                write_deauthorized(
                    api_url,
                    Holder::Grant(&grant.id),
                    &authorization_id,
                    runner_id,
                    ended,
                    json,
                )
            },
            |id| format!("um grant deauthorize {id} {runner_id} --yes"),
        )
    }
}
