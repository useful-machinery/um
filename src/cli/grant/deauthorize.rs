use anyhow::Context as _;
use clap::{ArgMatches, Args};

use crate::cli::CommandResult;
use crate::cli::integration::{ConnectionAuthorizations, Reach, RouteFailure};
use crate::exit_code::ExitCode;
use useful_api::{
    ConnectionAuthorizationRefusal, MemberGrant, MemberGrantApi, MemberGrantFailure,
    MemberGrantKind,
};
use useful_human_auth::Deployment;

use super::authorization::{Holder, ended_authorization_views, write_deauthorized};
use super::confirmation::{self, Confirmation, NATIVE_DEAUTHORIZE, Stated, Statement};
use super::runner_choice::{Acting, Chosen};
use super::{
    GrantArg, INTEGRATION_HELP, LeafRun, Options, RUNNER_HELP, RunnerArgs, Subject, replayed,
    runner_choice, runner_outcome, state_then_act, state_then_act_on_each, subject_and_runner,
    with_api,
};

pub(super) const ABOUT: &str = "End a runner's authorization for one of your grants";

/// What ending a token's authorization does, whoever's it is (REQ-INTEGRATION-20).
const TOKEN_DEAUTHORIZE: &[&str] = &[
    "the runner is issued the token no more; a running assignment loses it at its next refresh, \
     within five minutes",
];

/// What ending a runner's authorization for an organization's connection in one project does
/// (REQ-INTEGRATION-29, REQ-INTEGRATION-33): the permission ends, and no run is touched, since
/// Cloud delivers an organization's connection to no run today (REQ-INTEGRATION-24).
const CONNECTION_DEAUTHORIZE: &[&str] = &[
    "the runner's permission to use the connection for the project's app runs ends",
    "no run is asked to stop and none loses anything: Cloud delivers an organization's \
     connection to no run today",
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

    /// With `--project-id` the command ends a runner's authorization for an organization's
    /// connection, so its refusal states that end's lines alone (REQ-INTEGRATION-33). Under
    /// `useful grant`, which has no `project_id` to read, there is no project.
    fn refusal_lines(matches: &ArgMatches) -> Vec<String> {
        if matches.try_contains_id("project_id").unwrap_or(false) {
            CONNECTION_DEAUTHORIZE
                .iter()
                .map(|&line| line.to_owned())
                .collect()
        } else {
            Self::STATEMENT.every_line()
        }
    }
}

impl Command {
    /// Ends a runner's authorization of what `INTEGRATION` names under `reach`: a grant, or,
    /// under `useful integration`, a token, the member's or with `--org` the organization's
    /// (REQ-INTEGRATION-20). With `--project-id` it ends the runner's authorization for the
    /// organization's connection at the provider `INTEGRATION` names in that project instead,
    /// and looks no grant or token up (REQ-INTEGRATION-33).
    pub(super) fn run(self, deployment: &Deployment, reach: &Reach) -> CommandResult {
        let context = LeafRun {
            deployment,
            options: &self.options,
            reach,
        };
        if let Some(authorizations) =
            reach.connection_authorizations(self.grant.as_str(), Self::COMMAND)
        {
            return Ok(self.end_connection_authorization(context, &authorizations)?);
        }
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

    /// Ends the runner's authorization for the organization's connection `authorizations`
    /// names, in its project (REQ-INTEGRATION-29, REQ-INTEGRATION-33): finds it among the
    /// organization's authorizations in force for the project and the provider, of the runner
    /// `RUNNER` names or, given none, of one chosen among the runners they name (REQ-GRANT-81,
    /// REQ-GRANT-82), and of the connection `--connection-id` names; none is reported, with
    /// nothing more sent. Then it states that the permission ends, asks where the runner was
    /// chosen without `--yes`, and ends it under one key made before it is sent, once more under
    /// it when the answer was lost. No connection of the organization is read: the authorization
    /// names its own.
    fn end_connection_authorization(
        &self,
        context: LeafRun<'_>,
        authorizations: &ConnectionAuthorizations<'_>,
    ) -> anyhow::Result<ExitCode> {
        let LeafRun {
            deployment,
            options,
            ..
        } = context;
        let api_url = deployment.fingerprint().api_url();
        let json = options.json;
        let runner_id = self.runner_id.as_deref();
        let report = |failure: &RouteFailure| authorizations.report(api_url, failure);
        let not_found = || {
            report(&RouteFailure::Failed(
                MemberGrantFailure::ConnectionAuthorizationRefused(
                    ConnectionAuthorizationRefusal::AuthorizationNotFound,
                ),
            ))
            .write(api_url, json)
        };
        let read = with_api(deployment, options, |api| {
            authorizations.to_end(api, runner_id)
        })?;
        let in_force = match RouteFailure::answered(read) {
            Ok(in_force) if in_force.is_empty() => return not_found(),
            Ok(in_force) => in_force,
            Err(failure) => return report(&failure).write(api_url, json),
        };
        let runner = match runner_id {
            Some(runner_id) => Acting::named(runner_id),
            None => {
                let offer =
                    runner_choice::to_deauthorize_connection(deployment, options, &in_force)?;
                // The connection every authorization offered names, which the command line
                // names only where `--connection-id` did.
                let connection = in_force
                    .first()
                    .map(|first| first.connection_id.as_str())
                    .filter(|first| {
                        in_force
                            .iter()
                            .all(|authorization| authorization.connection_id == *first)
                    });
                let named = connection.filter(|_| authorizations.names_connection());
                match runner_choice::choose(
                    deployment,
                    json,
                    self.confirmation.yes,
                    offer,
                    connection.map(Holder::Connection),
                    &format!(
                        "whose authorization for the {} connection to end",
                        authorizations.provider()
                    ),
                    |runner_id| authorizations.command_line(runner_id, named),
                )? {
                    Ok(runner) => runner,
                    Err(exit) => return Ok(exit),
                }
            }
        };
        // Cloud holds at most one in force for the project, the runner and the provider.
        let Some(authorization) = in_force
            .into_iter()
            .find(|authorization| authorization.runner_id == runner.id)
        else {
            return not_found();
        };
        confirmation::state(
            format_args!(
                "{}",
                authorizations.ending_heading(&runner.named, &authorization)
            ),
            CONNECTION_DEAUTHORIZE,
        )?;
        if let Err(exit) = confirmation::go_on(runner.asks)? {
            return Ok(exit);
        }
        let key = useful_support::generate_idempotency_key()
            .context("generate connection deauthorization request key")?;
        let ended = with_api(deployment, options, |api| {
            authorizations.end(api, &authorization.id, &key)
        })?;
        match RouteFailure::answered(ended) {
            Ok(()) => authorizations.write_deauthorized(api_url, &authorization, json),
            Err(failure) => report(&failure).write(api_url, json),
        }
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
