use anyhow::Context as _;
use clap::{ArgMatches, Args};

use crate::cli::CommandResult;
use crate::cli::integration::Reach;
use useful_api::{MemberGrant, MemberGrantApi, MemberGrantFailure, MemberGrantKind};
use useful_human_auth::Deployment;

use super::authorization::{ended_authorization_views, write_deauthorized};
use super::confirmation::{Confirmation, NATIVE_DEAUTHORIZE, Stated, Statement};
use super::runner_choice::Chosen;
use super::{
    GrantArg, INTEGRATION_HELP, LeafRun, Options, RUNNER_HELP, RunnerArgs, Subject, replayed,
    runner_choice, runner_outcome, state_then_act, state_then_act_on_each, subject_and_runner,
};

pub(super) const ABOUT: &str = "End a runner's authorization for one of your grants";

/// What ending a token's authorization does, whoever's it is (REQ-INTEGRATION-20).
const TOKEN_DEAUTHORIZE: &[&str] = &[
    "the runner is issued the token no more; a running assignment loses it at its next refresh, \
     within five minutes",
];

/// `useful integration deauthorize INTEGRATION [RUNNER]`; given no `RUNNER` it offers the runners
/// the grant or token is authorized for instead (REQ-GRANT-81, REQ-INTEGRATION-20).
#[derive(Debug, Args)]
pub(in crate::cli) struct Command {
    // First, since reading an argument takes it out of the matches, and the confirmation asks
    // them whether a runner was named.
    #[command(flatten)]
    confirmation: Confirmation<Self>,

    #[arg(value_name = "INTEGRATION", value_parser = super::parse_grant, help = INTEGRATION_HELP)]
    grant: GrantArg,

    #[arg(value_name = "RUNNER", value_parser = super::parse_runner_id, help = RUNNER_HELP)]
    runner_id: Option<String>,

    #[command(flatten)]
    options: Options,
}

/// What ending the authorization does: for a native grant, the stop Cloud requests of the
/// runner's run (REQ-GRANT-48); for an OAuth grant, no further token, and no stop
/// (REQ-GRANT-56); for a token, no further delivery of it (REQ-INTEGRATION-20).
impl Stated for Command {
    const COMMAND: &'static str = "useful integration deauthorize";
    type Usage = Self;
    const STATEMENT: Statement = Statement::ByKind {
        oauth: &[
            "no run on the runner can be issued a token from the grant again; a token \
                  already issued expires on its own lifetime",
        ],
        native: NATIVE_DEAUTHORIZE,
        token: TOKEN_DEAUTHORIZE,
        org_token: TOKEN_DEAUTHORIZE,
    };
    const ACTION: &'static str = "to end the runner's authorization";

    fn chooses_runner(matches: &ArgMatches) -> bool {
        // The ID clap gives the field `runner_id`.
        !matches.try_contains_id("runner_id").unwrap_or(true)
    }
}

impl Command {
    /// Ends a runner's authorization of what `INTEGRATION` names under `reach`: a grant, or,
    /// under `useful integration`, a token, the member's or with `--org` the organization's
    /// (REQ-INTEGRATION-20).
    pub(super) fn run(self, deployment: &Deployment, reach: &Reach) -> CommandResult {
        let context = LeafRun {
            deployment,
            options: &self.options,
            reach,
        };
        let options = &self.options;
        let runner = RunnerArgs {
            id: self.runner_id.as_deref(),
            yes: self.confirmation.yes,
        };
        let (subject, chosen) = match subject_and_runner::<Self>(
            context,
            &self.grant,
            &runner,
            |subject| runner_choice::to_deauthorize(deployment, options, subject),
            |subject| format!("whose authorization for {} to end", subject.named()),
            |_| true,
        )? {
            Ok(acting) => acting,
            Err(exit) => return Ok(exit),
        };
        let runner = match chosen {
            Chosen::One(runner) => runner,
            Chosen::All(all) => {
                return Ok(state_then_act_on_each::<Self, _>(
                    context,
                    &subject,
                    &format!(
                        "Ending the authorization of each of {} runners for {}:",
                        all.runners.len(),
                        subject.named()
                    ),
                    &all,
                    |api, subject, runner_id, key| {
                        runner_outcome(
                            deauthorize(api, reach, subject, runner_id, key),
                            |failure| {
                                matches!(
                                    failure,
                                    MemberGrantFailure::AuthorizationNotFound
                                        | MemberGrantFailure::IntegrationTokenAuthorizationNotFound
                                )
                            },
                        )
                    },
                    |api_url, subject, runner_id, ended| {
                        write(api_url, subject, runner_id, ended, false)
                    },
                )?);
            }
        };
        let runner_id = runner.id.as_str();
        let key = useful_support::generate_idempotency_key()
            .context("generate grant deauthorization request key")?;
        Ok(state_then_act::<Self, _>(
            context,
            &subject,
            &format!(
                "Ending runner {}'s authorization for {}:",
                runner.named,
                subject.named()
            ),
            runner.asks,
            |api, subject| deauthorize(api, reach, subject, runner_id, &key),
            |api_url, subject, ended, json| write(api_url, subject, runner_id, ended, json),
        )?)
    }
}

/// The ended authorization's ID and, for a native grant, the grant read after it ended, or
/// `None` when that read failed or there was none to read.
type Ended = (String, Option<MemberGrant>);

/// Ends `runner_id`'s authorization of `subject` under `key`, the refusal that the runner holds
/// none in force when the subject lists none for it; for a native grant, then reads the grant
/// once more for the ended authorization's removal state and stop.
fn deauthorize(
    api: &MemberGrantApi,
    reach: &Reach,
    subject: &Subject,
    runner_id: &str,
    key: &str,
) -> Result<Ended, MemberGrantFailure> {
    let authorization_id = subject
        .in_force()
        .iter()
        .find(|authorization| authorization.runner_id == runner_id)
        .map(|authorization| authorization.id.to_owned());
    match subject {
        Subject::Grant(grant) => {
            let authorization_id =
                authorization_id.ok_or(MemberGrantFailure::AuthorizationNotFound)?;
            replayed(|| api.deauthorize(&grant.id, &authorization_id, key))?;
            // A native grant's runner kept a copy; the read only reports its removal and stop,
            // and its failure leaves the authorization ended.
            let after = (grant.kind == MemberGrantKind::GrantKindNative)
                .then(|| api.get(&grant.id).ok())
                .flatten();
            Ok((authorization_id, after))
        }
        Subject::Token(token) => {
            let authorization_id = authorization_id
                .ok_or(MemberGrantFailure::IntegrationTokenAuthorizationNotFound)?;
            replayed(|| {
                api.deauthorize_integration_token(reach.owner(), &token.id, &authorization_id, key)
            })?;
            // A token's runner reports nothing back, so nothing is read after.
            Ok((authorization_id, None))
        }
    }
}

/// Reports `runner_id`'s ended authorization of `subject`.
fn write(
    api_url: &str,
    subject: &Subject,
    runner_id: &str,
    (authorization_id, after): Ended,
    json: bool,
) -> anyhow::Result<crate::exit_code::ExitCode> {
    let ended = after
        .as_ref()
        .and_then(|after| after.ended_authorizations.as_deref())
        .map(ended_authorization_views)
        .into_iter()
        .flatten()
        .find(|ended| ended.authorization.id == authorization_id);
    write_deauthorized(
        api_url,
        subject.holder(),
        &authorization_id,
        runner_id,
        ended,
        json,
    )
}
