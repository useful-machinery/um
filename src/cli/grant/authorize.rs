use anyhow::Context;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{ArgMatches, Args, FromArgMatches};

use crate::exit_code::ExitCode;
use um_api::{
    MemberGrant, MemberGrantAuthorization, MemberGrantFailure, MemberGrantKind, MemberGrantProvider,
};
use um_human_auth::Deployment;

use super::authorization::{AuthorizationView, Holder, write_authorized};
use super::confirmation::{self, Confirmation, NATIVE_AUTHORIZE, Stated, Statement};
use super::runner_choice::{self, Acting, Chosen};
use super::{
    GRANT_HELP, GrantArg, Options, RUNNER_HELP, RunnerArgs, each_under_its_key, every_grant,
    grant_and_runner, output, replayed, runner_outcome, state_then_act, state_then_act_on_each,
    unrevoked,
};

pub(super) const ABOUT: &str = "Authorize a runner to use one of your grants, or all of them";

/// The remedy for a Codex grant authorized for another runner (REQ-GRANT-66), which `um grant
/// authorize` and the earlier `um auth agent-login authorize` both report: a Codex grant serves
/// one runner, and another sign-in cannot make a second grant for another runner, since a held
/// grant is refused or replaced in place (REQ-GRANT-73).
pub(in crate::cli) const MOVE_CODEX_GRANT: &str = "A Codex grant serves one runner at a time. To move it to this runner, end its authorization for the other, which `um grant list GRANT` names, then authorize this one:\n  um grant deauthorize GRANT RUNNER --yes";

/// The leaf's arguments as clap reads them. `--all` takes the place of `GRANT`, so after it the
/// first positional is the runner: clap fills positionals in order, whatever they hold, so the
/// first takes either and `Command` sorts them out.
#[derive(Debug, Args)]
#[group(id = "target", args = ["grant", "all"], required = true, multiple = true)]
struct Arguments {
    // First, since reading an argument takes it out of the matches, and the confirmation asks
    // them whether a runner was named.
    #[command(flatten)]
    confirmation: Confirmation<Command>,

    #[arg(value_name = "GRANT", value_parser = parse_first, help = GRANT_HELP)]
    grant: Option<First>,

    #[arg(
        long,
        help = "Authorize the runner for every grant of yours that can serve it, in place of GRANT"
    )]
    all: bool,

    #[arg(value_name = "RUNNER", value_parser = super::parse_runner_id, help = RUNNER_HELP)]
    runner_id: Option<String>,

    #[command(flatten)]
    options: Options,
}

/// The first positional: a grant, or after `--all` the runner.
#[derive(Clone, Debug)]
enum First {
    Grant(GrantArg),
    Runner(String),
}

fn parse_first(value: &str) -> Result<First, String> {
    super::parse_runner_id(value)
        .map(First::Runner)
        .or_else(|_| super::parse_grant(value).map(First::Grant))
        .map_err(|grant| {
            format!("{grant}; after --all, name the runner by its exact rnr_ identifier")
        })
}

/// What the leaf authorizes the runner for: one grant, or every grant of the member's.
#[derive(Debug)]
enum Target {
    Grant(GrantArg),
    All,
}

/// `um grant authorize GRANT [RUNNER]` or `um grant authorize --all [RUNNER]`; given no
/// `RUNNER` it offers the runners instead (REQ-GRANT-81).
#[derive(Debug)]
pub(in crate::cli) struct Command {
    target: Target,
    runner_id: Option<String>,
    arguments: Arguments,
}

/// The target and runner `matches` name, or the usage error of a command line that names
/// both a grant and `--all`, a runner in place of `GRANT` without `--all`, or a second runner
/// after `--all`.
fn target(matches: &ArgMatches) -> Result<(Target, Option<String>), clap::Error> {
    let first = matches.try_get_one::<First>("grant").ok().flatten();
    let all = matches
        .try_get_one::<bool>("all")
        .ok()
        .flatten()
        .is_some_and(|all| *all);
    let runner_id = matches.try_get_one::<String>("runner_id").ok().flatten();
    let refused = |kind, context: &[(ContextKind, ContextValue)]| {
        let mut error = confirmation::usage_error::<Command>(kind);
        for (kind, value) in context {
            error.insert(*kind, value.clone());
        }
        error
    };
    let text = |value: &str| ContextValue::String(value.to_owned());
    match (first, all, runner_id) {
        (Some(First::Grant(grant)), false, runner_id) => {
            Ok((Target::Grant(grant.clone()), runner_id.cloned()))
        }
        (None, true, runner_id) => Ok((Target::All, runner_id.cloned())),
        (Some(First::Runner(runner_id)), true, None) => Ok((Target::All, Some(runner_id.clone()))),
        (Some(First::Grant(_)), true, _) => Err(refused(
            ErrorKind::ArgumentConflict,
            &[
                (ContextKind::InvalidArg, text("--all")),
                (ContextKind::PriorArg, text("[GRANT]")),
            ],
        )),
        (Some(First::Runner(_)), true, Some(runner_id)) => Err(refused(
            ErrorKind::UnknownArgument,
            &[(ContextKind::InvalidArg, text(runner_id))],
        )),
        (Some(First::Runner(runner_id)), false, _) => Err(refused(
            ErrorKind::ValueValidation,
            &[
                (ContextKind::InvalidArg, text("[GRANT]")),
                (ContextKind::InvalidValue, text(runner_id)),
                (
                    ContextKind::Suggested,
                    ContextValue::StyledStrs(vec![
                        "name the grant before the runner, or put --all in its place".into(),
                    ]),
                ),
            ],
        )),
        // The group requires GRANT or --all, so clap refuses a command line with neither
        // before this reads it.
        (None, false, _) => Err(refused(
            ErrorKind::MissingRequiredArgument,
            &[(
                ContextKind::InvalidArg,
                ContextValue::Strings(vec!["<GRANT|--all>".to_owned()]),
            )],
        )),
    }
}

impl Args for Command {
    fn augment_args(command: clap::Command) -> clap::Command {
        Arguments::augment_args(command)
    }

    fn augment_args_for_update(command: clap::Command) -> clap::Command {
        Arguments::augment_args_for_update(command)
    }
}

impl FromArgMatches for Command {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        let (target, runner_id) = target(matches)?;
        Ok(Self {
            target,
            runner_id,
            arguments: Arguments::from_arg_matches(matches)?,
        })
    }

    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), clap::Error> {
        (self.target, self.runner_id) = target(matches)?;
        self.arguments.update_from_arg_matches(matches)
    }
}

/// Who can use the grant once the runner is authorized for it (REQ-GRANT-66): an OAuth
/// grant's tokens are issued only to the runs its owner started (REQ-GRANT-10), and every step
/// of such a run can read the token, while a native grant's login is the runner's to every run
/// and step (REQ-GRANT-35).
impl Stated for Command {
    const COMMAND: &'static str = "um grant authorize";
    type Usage = Self;
    const STATEMENT: Statement = Statement::ByKind {
        oauth: &[
            "every run on the runner that you start can be issued tokens from the grant",
            "every step of such a run can read the token the runner is issued for it",
        ],
        native: NATIVE_AUTHORIZE,
    };
    const ACTION: &'static str = "to authorize the runner";

    fn chooses_runner(matches: &ArgMatches) -> bool {
        target(matches).is_ok_and(|(_, runner_id)| runner_id.is_none())
    }
}

/// What `--all` did with one grant.
pub(super) enum Outcome {
    Authorized(MemberGrantAuthorization),
    /// The runner was authorized for the grant already, and is left as it is.
    AlreadyAuthorized,
    /// A Codex grant authorized for another runner, which it serves in place of this one.
    AuthorizedElsewhere,
    /// A native grant of an organization other than the runner's, which cannot serve it.
    OtherOrganization,
    Refused(MemberGrantFailure),
}

impl Outcome {
    /// Whether Cloud found the runner where the grant can serve it, so that another native
    /// grant's `grant-runner-not-found` means another organization.
    const fn found_runner(&self) -> bool {
        matches!(
            self,
            Self::Authorized(_)
                | Self::AlreadyAuthorized
                | Self::AuthorizedElsewhere
                | Self::Refused(MemberGrantFailure::RunnerHoldsLogin)
        )
    }
}

/// A refusal of one runner that leaves `authorize GRANT` with all runners chosen to try the
/// others (REQ-GRANT-83); any other failure ends it.
const fn refuses_one_runner(failure: &MemberGrantFailure) -> bool {
    matches!(
        failure,
        MemberGrantFailure::RunnerNotFound
            | MemberGrantFailure::RunnerHoldsLogin
            | MemberGrantFailure::AlreadyAuthorized
    )
}

/// A refusal of one grant that leaves `--all` to try the others; any other failure ends it.
const fn refuses_one_grant(failure: &MemberGrantFailure) -> bool {
    matches!(
        failure,
        MemberGrantFailure::ProviderUnavailable
            | MemberGrantFailure::GrantNotFound
            | MemberGrantFailure::RunnerNotFound
            | MemberGrantFailure::RunnerHoldsLogin
    )
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let options = &self.arguments.options;
        let yes = self.arguments.confirmation.yes;
        let grant = match (&self.target, &self.runner_id) {
            (Target::All, Some(runner_id)) => {
                return self.run_all(deployment, &Acting::named(runner_id));
            }
            (Target::All, None) => {
                let offer = runner_choice::to_authorize_all(deployment, options)?;
                return match runner_choice::choose(
                    deployment,
                    options.json,
                    yes,
                    offer,
                    None,
                    "to authorize for every grant of yours that can serve it",
                    |runner_id| format!("{} --all {runner_id} --yes", Self::COMMAND),
                )? {
                    Ok(runner) => self.run_all(deployment, &runner),
                    Err(exit) => Ok(exit),
                };
            }
            (Target::Grant(grant), _) => grant,
        };
        let runner = RunnerArgs {
            id: self.runner_id.as_deref(),
            yes,
        };
        let (grant, chosen) = match grant_and_runner::<Self>(
            deployment,
            options,
            grant,
            &runner,
            |grant| runner_choice::to_authorize(deployment, options, grant),
            |grant| format!("to authorize for grant {}", grant.id),
            // A Codex grant serves one runner at a time (REQ-GRANT-59).
            |grant| grant.provider != MemberGrantProvider::GrantProviderCodex,
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
                        "Authorizing each of {} runners for grant {}:",
                        all.runners.len(),
                        grant.id
                    ),
                    &all,
                    |api, grant, runner_id, key| {
                        runner_outcome(authorize(api, grant, runner_id, key), refuses_one_runner)
                    },
                    |api_url, grant, _, authorization| write(api_url, grant, &authorization, false),
                );
            }
        };
        let key = um_support::generate_idempotency_key()
            .context("generate grant authorization request key")?;
        state_then_act::<Self, _>(
            deployment,
            options,
            &grant,
            &format!(
                "Authorizing runner {} for grant {}:",
                runner.named, grant.id
            ),
            runner.asks,
            |api, grant| authorize(api, grant, &runner.id, &key),
            |api_url, grant, authorization, json| write(api_url, grant, &authorization, json),
        )
    }

    /// Authorizes the runner for every unrevoked grant of the member that can serve it: each
    /// tried in turn, since only Cloud knows which organization the runner is in. A runner
    /// chosen from the list without `--yes` is asked about after the statement.
    fn run_all(&self, deployment: &Deployment, runner: &Acting) -> anyhow::Result<ExitCode> {
        let json = self.arguments.options.json;
        let api_url = deployment.fingerprint().api_url();
        let runner_id = runner.id.as_str();
        confirmation::state(
            format_args!(
                "Authorizing runner {} for every grant of yours that can serve it:",
                runner.named
            ),
            &Self::STATEMENT.every_line(),
        )?;
        if let Err(exit) = confirmation::go_on(runner.asks)? {
            return Ok(exit);
        }
        let grants = match super::with_api(deployment, &self.arguments.options, |api| {
            Ok(every_grant(api)?
                .into_iter()
                .filter(unrevoked)
                .collect::<Vec<_>>())
        })? {
            Ok(grants) if grants.is_empty() => {
                return output::failure(api_url, &MemberGrantFailure::GrantNotFound, json);
            }
            Ok(grants) => grants,
            Err(failure) => return output::failure(api_url, &failure, json),
        };
        // A failure other than a refusal of one grant ends the command at that grant, keeping
        // the outcomes of the grants before it to report.
        let (mut outcomes, ended) = each_under_its_key(
            deployment,
            &self.arguments.options,
            &grants,
            |api, grant, key| authorize_one(api, grant, runner_id, key),
        )?;
        let found_runner = outcomes.iter().any(Outcome::found_runner);
        match &ended {
            Some(failure) if outcomes.is_empty() => {
                return output::failure(api_url, failure, json);
            }
            // No grant found the runner: it is in none of the member's organizations, or none
            // of the member's grants can serve it.
            None if !found_runner => {
                return output::failure(api_url, &MemberGrantFailure::RunnerNotFound, json);
            }
            _ => {}
        }
        if found_runner {
            for (grant, outcome) in grants.iter().zip(&mut outcomes) {
                if grant.kind == MemberGrantKind::GrantKindNative
                    && matches!(
                        outcome,
                        Outcome::Refused(MemberGrantFailure::RunnerNotFound)
                    )
                {
                    *outcome = Outcome::OtherOrganization;
                }
            }
        }
        output::write_authorize_all(api_url, runner_id, &grants, &outcomes, ended.as_ref(), json)
    }
}

/// Authorizes `runner_id` for `grant` under `key`, once more when the answer was lost.
fn authorize(
    api: &um_api::MemberGrantApi,
    grant: &MemberGrant,
    runner_id: &str,
    key: &str,
) -> Result<MemberGrantAuthorization, MemberGrantFailure> {
    replayed(|| api.authorize(&grant.id, runner_id, key))
}

/// Reports the authorization of a runner for `grant`.
fn write(
    api_url: &str,
    grant: &MemberGrant,
    authorization: &MemberGrantAuthorization,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_authorized(
        api_url,
        Holder::Grant(&grant.id),
        AuthorizationView::made(authorization),
        json,
    )
}

/// Authorizes `runner_id` for `grant` unless it already is, reporting a refusal of this one
/// grant as its outcome.
fn authorize_one(
    api: &um_api::MemberGrantApi,
    grant: &MemberGrant,
    runner_id: &str,
    key: &str,
) -> Result<Outcome, MemberGrantFailure> {
    if grant
        .authorizations
        .iter()
        .any(|authorization| authorization.runner_id == runner_id)
    {
        return Ok(Outcome::AlreadyAuthorized);
    }
    match authorize(api, grant, runner_id, key) {
        Ok(authorization) => Ok(Outcome::Authorized(authorization)),
        // The grant does not list the runner, so for a native grant the authorization Cloud
        // holds is another runner's; an OAuth grant's is this runner's, made since the list.
        Err(MemberGrantFailure::AlreadyAuthorized) => Ok(match grant.kind {
            MemberGrantKind::GrantKindNative => Outcome::AuthorizedElsewhere,
            MemberGrantKind::GrantKindOAuth => Outcome::AlreadyAuthorized,
        }),
        Err(failure) if refuses_one_grant(&failure) => Ok(Outcome::Refused(failure)),
        Err(failure) => Err(failure),
    }
}
