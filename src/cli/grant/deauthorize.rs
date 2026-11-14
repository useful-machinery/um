use anyhow::Context;
use clap::{ArgMatches, Args};

use crate::exit_code::ExitCode;
use um_api::{MemberGrant, MemberGrantApi, MemberGrantFailure, MemberGrantKind};
use um_human_auth::Deployment;

use super::authorization::{Holder, ended_authorization_views, write_deauthorized};
use super::confirmation::{Confirmation, NATIVE_DEAUTHORIZE, Stated, Statement};
use super::runner_choice::Chosen;
use super::{
    GRANT_HELP, GrantArg, Options, RUNNER_HELP, RunnerArgs, grant_and_runner, replayed,
    runner_choice, runner_outcome, state_then_act, state_then_act_on_each,
};

pub(super) const ABOUT: &str = "End a runner's authorization for one of your grants";

/// `um grant deauthorize GRANT [RUNNER]`; given no `RUNNER` it offers the runners the grant is
/// authorized for instead (REQ-GRANT-81).
#[derive(Debug, Args)]
pub(in crate::cli) struct Command {
    // First, since reading an argument takes it out of the matches, and the confirmation asks
    // them whether a runner was named.
    #[command(flatten)]
    confirmation: Confirmation<Self>,

    #[arg(value_name = "GRANT", value_parser = super::parse_grant, help = GRANT_HELP)]
    grant: GrantArg,

    #[arg(value_name = "RUNNER", value_parser = super::parse_runner_id, help = RUNNER_HELP)]
    runner_id: Option<String>,

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

    fn chooses_runner(matches: &ArgMatches) -> bool {
        // The ID clap gives the field `runner_id`.
        !matches.try_contains_id("runner_id").unwrap_or(true)
    }
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let options = &self.options;
        let runner = RunnerArgs {
            id: self.runner_id.as_deref(),
            yes: self.confirmation.yes,
        };
        let (grant, chosen) = match grant_and_runner::<Self>(
            deployment,
            options,
            &self.grant,
            &runner,
            |grant| runner_choice::to_deauthorize(deployment, options, grant),
            |grant| format!("whose authorization for grant {} to end", grant.id),
            |_| true,
        )? {
            Ok(acting) => acting,
            Err(exit) => return Ok(exit),
        };
        let runner = match chosen {
            Chosen::One(runner) => runner,
            Chosen::All(all) => {
                return state_then_act_on_each::<Self, _>(
                    deployment,
                    options,
                    &grant,
                    &format!(
                        "Ending the authorization of each of {} runners for grant {}:",
                        all.runners.len(),
                        grant.id
                    ),
                    &all,
                    |api, grant, runner_id, key| {
                        runner_outcome(deauthorize(api, grant, runner_id, key), |failure| {
                            matches!(failure, MemberGrantFailure::AuthorizationNotFound)
                        })
                    },
                    |api_url, grant, runner_id, ended| {
                        write(api_url, grant, runner_id, ended, false)
                    },
                );
            }
        };
        let runner_id = runner.id.as_str();
        let key = um_support::generate_idempotency_key()
            .context("generate grant deauthorization request key")?;
        state_then_act::<Self, _>(
            deployment,
            options,
            &grant,
            &format!(
                "Ending runner {}'s authorization for grant {}:",
                runner.named, grant.id
            ),
            runner.asks,
            |api, grant| deauthorize(api, grant, runner_id, &key),
            |api_url, grant, ended, json| write(api_url, grant, runner_id, ended, json),
        )
    }
}

/// The ended authorization's ID and, for a native grant, the grant read after it ended, or
/// `None` when that read failed.
type Ended = (String, Option<MemberGrant>);

/// Ends `runner_id`'s authorization of `grant` under `key`, `authorization_not_found` when the
/// runner holds none in force; for a native grant, then reads the grant once more for the
/// ended authorization's removal state and stop.
fn deauthorize(
    api: &MemberGrantApi,
    grant: &MemberGrant,
    runner_id: &str,
    key: &str,
) -> Result<Ended, MemberGrantFailure> {
    // The grant lists only its authorizations in force (REQ-GRANT-58).
    let authorization_id = grant
        .authorizations
        .iter()
        .find(|authorization| authorization.runner_id == runner_id)
        .ok_or(MemberGrantFailure::AuthorizationNotFound)?
        .id
        .clone();
    replayed(|| api.deauthorize(&grant.id, &authorization_id, key))?;
    // A native grant's runner kept a copy; the read only reports its removal and stop, and its
    // failure leaves the authorization ended.
    let after = (grant.kind == MemberGrantKind::GrantKindNative)
        .then(|| api.get(&grant.id).ok())
        .flatten();
    Ok((authorization_id, after))
}

/// Reports `runner_id`'s ended authorization of `grant`.
fn write(
    api_url: &str,
    grant: &MemberGrant,
    runner_id: &str,
    (authorization_id, after): Ended,
    json: bool,
) -> anyhow::Result<ExitCode> {
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
}
