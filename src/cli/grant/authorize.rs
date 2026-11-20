use anyhow::Context as _;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{ArgMatches, Args, FromArgMatches};

use crate::cli::CommandResult;
use crate::cli::integration::{ConnectionAuthorizations, Reach, RouteFailure};
use crate::exit_code::ExitCode;
use useful_api::{
    IntegrationTokenAuthorization, IntegrationTokenSide, MemberGrantApi, MemberGrantAuthorization,
    MemberGrantFailure, MemberGrantKind, MemberGrantProvider,
};
use useful_human_auth::Deployment;

use super::authorization::{AuthorizationView, Holder, write_authorized};
use super::confirmation::{self, Confirmation, Kind, NATIVE_AUTHORIZE, Stated, Statement};
use super::runner_choice::{self, Acting, Chosen};
use super::{
    GrantArg, INTEGRATION_HELP, LeafRun, Options, RUNNER_HELP, RunnerArgs, Subject,
    each_under_its_key, every_grant, output, replayed, report_under, runner_outcome,
    state_then_act, state_then_act_on_each, subject_and_runner, unrevoked, with_api,
};

pub(super) const ABOUT: &str = "Authorize a runner to use one of your grants, or all of them";

/// The remedy for a Codex grant authorized for another runner (REQ-GRANT-66), which `useful
/// integration authorize` and the earlier `useful auth agent-login authorize` both report: a Codex
/// grant serves one runner, and another sign-in cannot make a second grant for another runner,
/// since a held grant is refused or replaced in place (REQ-GRANT-73).
pub(in crate::cli) const MOVE_CODEX_GRANT: &str = "A Codex grant serves one runner at a time. To move it to this runner, end its authorization for the other, which `useful integration list INTEGRATION` names, then authorize this one:\n  useful integration deauthorize INTEGRATION RUNNER --yes";

/// What authorizing a runner for an organization's connection in one project permits
/// (REQ-INTEGRATION-33): it is a permission, which the authorization records and Cloud acts on
/// for no run today (REQ-INTEGRATION-24).
const CONNECTION_AUTHORIZE: &[&str] = &[
    "the permission covers every app run of the project on the runner, whoever or whatever \
     caused the run",
    "the permission reaches whatever the provider grants the connection; no authorization \
     narrows that",
    "Cloud delivers an organization's connection to no run today: the authorization records a \
     permission and delivers nothing",
];

/// The leaf's arguments as clap reads them. `--all` takes the place of `INTEGRATION`, so after it
/// the first positional is the runner: clap fills positionals in order, whatever they hold, so the
/// first takes either and `Command` sorts them out.
#[derive(Debug, Args)]
#[group(id = "target", args = ["grant", "all"], required = true, multiple = true)]
struct Arguments {
    // First, since reading an argument takes it out of the matches, and the confirmation asks
    // them whether a runner was named.
    #[command(flatten)]
    confirmation: Confirmation<Command>,

    #[arg(value_name = "INTEGRATION", value_parser = parse_first, help = INTEGRATION_HELP)]
    grant: Option<First>,

    #[arg(
        long,
        help = "Authorize the runner for every grant of yours that can serve it, in place of INTEGRATION"
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

/// `useful integration authorize INTEGRATION [RUNNER]` or `useful integration authorize --all
/// [RUNNER]`; given no `RUNNER` it offers the runners instead (REQ-GRANT-81).
#[derive(Debug)]
pub(in crate::cli) struct Command {
    target: Target,
    runner_id: Option<String>,
    arguments: Arguments,
}

/// The target and runner `matches` name, or the usage error of a command line that names
/// both a grant and `--all`, a runner in place of `INTEGRATION` without `--all`, or a second runner
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
                (ContextKind::PriorArg, text("[INTEGRATION]")),
            ],
        )),
        (Some(First::Runner(_)), true, Some(runner_id)) => Err(refused(
            ErrorKind::UnknownArgument,
            &[(ContextKind::InvalidArg, text(runner_id))],
        )),
        (Some(First::Runner(runner_id)), false, _) => Err(refused(
            ErrorKind::ValueValidation,
            &[
                (ContextKind::InvalidArg, text("[INTEGRATION]")),
                (ContextKind::InvalidValue, text(runner_id)),
                (
                    ContextKind::Suggested,
                    ContextValue::StyledStrs(vec![
                        "name the grant before the runner, or put --all in its place".into(),
                    ]),
                ),
            ],
        )),
        // The group requires INTEGRATION or --all, so clap refuses a command line with neither
        // before this reads it.
        (None, false, _) => Err(refused(
            ErrorKind::MissingRequiredArgument,
            &[(
                ContextKind::InvalidArg,
                ContextValue::Strings(vec!["<INTEGRATION|--all>".to_owned()]),
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

/// Who can use the grant or token once the runner is authorized for it (REQ-GRANT-66,
/// REQ-INTEGRATION-20): an OAuth grant's tokens are issued only to the runs its owner started
/// (REQ-GRANT-10), and every step of such a run can read the token, while a native grant's login
/// is the runner's to every run and step (REQ-GRANT-35). A member's token is issued to the runs
/// whose actor is the member and whose workflow declares the service as the member's, an
/// organization's token to every run of the organization whose workflow declares it as `org`.
impl Stated for Command {
    const COMMAND: &'static str = "useful integration authorize";
    type Usage = Self;
    const STATEMENT: Statement = Statement::ByKind {
        oauth: &[
            "every run on the runner that you start can be issued tokens from the grant",
            "every step of such a run can read the token the runner is issued for it",
        ],
        native: NATIVE_AUTHORIZE,
        token: &[
            "every run on the runner whose actor is you and whose workflow declares the service \
             as yours can be issued the token",
            "every step of such a run can read your token",
        ],
        org_token: &[
            "every run of the organization on the runner whose workflow declares the service as \
             org can be issued the token, whoever caused the run",
            "every step of such a run can read the organization's token",
        ],
    };
    const ACTION: &'static str = "to authorize the runner";

    fn chooses_runner(matches: &ArgMatches) -> bool {
        target(matches).is_ok_and(|(_, runner_id)| runner_id.is_none())
    }

    /// With `--project-id` the command authorizes a runner for an organization's connection, so
    /// its refusal states that authorization's lines alone (REQ-INTEGRATION-33). Under
    /// `useful grant`, which has no `project_id` to read, there is no project.
    fn refusal_lines(matches: &ArgMatches) -> Vec<String> {
        if matches.try_contains_id("project_id").unwrap_or(false) {
            CONNECTION_AUTHORIZE
                .iter()
                .map(|&line| line.to_owned())
                .collect()
        } else {
            Self::STATEMENT.every_line()
        }
    }
}

/// An authorization `authorize` made: of a grant, or of a token (REQ-INTEGRATION-20).
pub(super) enum Made {
    Grant(MemberGrantAuthorization),
    Token(IntegrationTokenAuthorization),
}

impl Made {
    pub(super) fn view(&self) -> AuthorizationView<'_> {
        match self {
            Self::Grant(authorization) => AuthorizationView::made(authorization),
            Self::Token(authorization) => AuthorizationView::of_token(authorization),
        }
    }
}

/// What `--all` did with one grant or token.
pub(super) enum Outcome {
    Authorized(Made),
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

/// A refusal of one runner that leaves `authorize INTEGRATION` with all runners chosen to try
/// the others (REQ-GRANT-83); any other failure ends it.
const fn refuses_one_runner(failure: &MemberGrantFailure) -> bool {
    matches!(
        failure,
        MemberGrantFailure::RunnerNotFound
            | MemberGrantFailure::RunnerHoldsLogin
            | MemberGrantFailure::AlreadyAuthorized
            | MemberGrantFailure::IntegrationTokenRunnerNotFound
            | MemberGrantFailure::IntegrationTokenAlreadyAuthorized
    )
}

/// A refusal of one grant or token that leaves `--all` to try the others; any other failure, a
/// refusal of the member by an organization among them, ends it.
const fn refuses_one_subject(failure: &MemberGrantFailure) -> bool {
    matches!(
        failure,
        MemberGrantFailure::ProviderUnavailable
            | MemberGrantFailure::GrantNotFound
            | MemberGrantFailure::RunnerNotFound
            | MemberGrantFailure::RunnerHoldsLogin
            | MemberGrantFailure::IntegrationTokenNotFound
            | MemberGrantFailure::IntegrationTokenRunnerNotFound
    )
}

impl Command {
    /// Authorizes the runner for what `INTEGRATION` names under `reach`, a grant or, under
    /// `useful integration`, a token, the member's or with `--org` the organization's; or with
    /// `--all` for every one that can serve it (REQ-GRANT-66, REQ-INTEGRATION-20). With
    /// `--project-id` it authorizes the runner for the organization's connection at the
    /// provider `INTEGRATION` names in that project instead, and looks no grant or token up
    /// (REQ-INTEGRATION-33).
    pub(super) fn run(self, deployment: &Deployment, reach: &Reach) -> CommandResult {
        let options = &self.arguments.options;
        let context = LeafRun {
            deployment,
            options,
            reach,
        };
        let yes = self.arguments.confirmation.yes;
        let grant = match (&self.target, &self.runner_id) {
            (Target::All, Some(runner_id)) => {
                return Ok(self.run_all(context, &Acting::named(runner_id))?);
            }
            (Target::All, None) => {
                let offer = runner_choice::to_authorize_all(deployment, options, reach.org())?;
                let org = reach
                    .org()
                    .map_or_else(String::new, |org| format!(" --org {org}"));
                return match runner_choice::choose(
                    deployment,
                    options.json,
                    yes,
                    offer,
                    None,
                    &format!("to authorize for {}", all_of(reach)),
                    |runner_id| format!("{} --all {runner_id}{org} --yes", Self::COMMAND),
                )? {
                    Ok(runner) => Ok(self.run_all(context, &runner)?),
                    Err(exit) => Ok(exit),
                };
            }
            (Target::Grant(grant), _) => {
                if let Some(authorizations) =
                    reach.connection_authorizations(grant.as_str(), Self::COMMAND)
                {
                    return Ok(self.authorize_connection(context, &authorizations)?);
                }
                grant
            }
        };
        let runner = RunnerArgs {
            id: self.runner_id.as_deref(),
            yes,
        };
        let (subject, chosen) = match subject_and_runner::<Self>(
            context,
            grant,
            &runner,
            |subject| runner_choice::to_authorize(deployment, options, subject),
            |subject| format!("to authorize for {}", subject.named()),
            // A Codex grant serves one runner at a time (REQ-GRANT-59).
            |subject| subject.provider() != MemberGrantProvider::GrantProviderCodex.as_str(),
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
                        "Authorizing each of {} runners for {}:",
                        all.runners.len(),
                        subject.named()
                    ),
                    &all,
                    |api, subject, runner_id, key| {
                        runner_outcome(
                            authorize(api, reach, subject, runner_id, key),
                            refuses_one_runner,
                        )
                    },
                    |api_url, subject, _, made| write(api_url, subject, &made, false),
                )?);
            }
        };
        let key = useful_support::generate_idempotency_key()
            .context("generate grant authorization request key")?;
        Ok(state_then_act::<Self, _>(
            context,
            &subject,
            &format!(
                "Authorizing runner {} for {}:",
                runner.named,
                subject.named()
            ),
            runner.asks,
            |api, subject| authorize(api, reach, subject, &runner.id, &key),
            |api_url, subject, made, json| write(api_url, subject, &made, json),
        )?)
    }

    /// Authorizes the runner for the organization's connection `authorizations` names, in its
    /// project (REQ-INTEGRATION-33): reads the organization's connections at the provider and
    /// takes the one it acts on, then the runner `RUNNER` names or, given none, one chosen among
    /// the organization's runners that hold no authorization in force for the project and the
    /// provider (REQ-GRANT-81, REQ-GRANT-82); states what the authorization permits, asks where
    /// the runner was chosen without `--yes`, and sends the authorization under one key made
    /// before the session is opened, once more under it when the answer was lost.
    fn authorize_connection(
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
        let connection = match authorizations.connection(
            deployment,
            options.http.allow_insecure_http,
            runner_id,
            json,
        )? {
            Ok(connection) => connection,
            Err(exit) => return Ok(exit),
        };
        let runner = match runner_id {
            Some(runner_id) => Acting::named(runner_id),
            None => {
                let read = with_api(deployment, options, |api| {
                    authorizations.in_force(api, None)
                })?;
                let in_force: Vec<_> = match RouteFailure::answered(read) {
                    Ok(in_force) => in_force
                        .into_iter()
                        .map(|authorization| authorization.runner_id)
                        .collect(),
                    Err(failure) => {
                        return authorizations
                            .report(api_url, &failure)
                            .write(api_url, json);
                    }
                };
                let offer = runner_choice::to_authorize_connection(
                    deployment,
                    options,
                    authorizations.organization(),
                    &in_force,
                )?;
                // The command line names the connection only where `--connection-id` did.
                let named = authorizations
                    .names_connection()
                    .then_some(connection.id.as_str());
                match runner_choice::choose(
                    deployment,
                    json,
                    self.arguments.confirmation.yes,
                    offer,
                    Some(Holder::Connection(&connection.id)),
                    &format!(
                        "to authorize for the {} connection {}",
                        authorizations.provider(),
                        connection.id
                    ),
                    |runner_id| authorizations.command_line(runner_id, named),
                )? {
                    Ok(runner) => runner,
                    Err(exit) => return Ok(exit),
                }
            }
        };
        confirmation::state(
            format_args!("{}", authorizations.heading(&runner.named, &connection)),
            CONNECTION_AUTHORIZE,
        )?;
        if let Err(exit) = confirmation::go_on(runner.asks)? {
            return Ok(exit);
        }
        let key = useful_support::generate_idempotency_key()
            .context("generate connection authorization request key")?;
        let made = with_api(deployment, options, |api| {
            authorizations.authorize(api, &connection.id, &runner.id, &key)
        })?;
        match RouteFailure::answered(made) {
            Ok(authorization) => authorizations.write_authorized(api_url, &authorization, json),
            Err(failure) => authorizations
                .report(api_url, &failure)
                .write(api_url, json),
        }
    }

    /// Authorizes the runner for every unrevoked grant of the member and, under
    /// `useful integration`, every token of theirs, or with `--org` for every token of that
    /// organization and nothing of the member's (REQ-INTEGRATION-20): each tried in turn, since
    /// only Cloud knows which organization the runner is in. A runner chosen from the list
    /// without `--yes` is asked about after the statement.
    fn run_all(&self, context: LeafRun<'_>, runner: &Acting) -> anyhow::Result<ExitCode> {
        let LeafRun {
            deployment,
            options,
            reach,
        } = context;
        let json = options.json;
        let api_url = deployment.fingerprint().api_url();
        let runner_id = runner.id.as_str();
        let kinds: &[Kind] = match (reach.org(), reach.tokens()) {
            (Some(_), _) => &[Kind::Token(IntegrationTokenSide::IntegrationTokenSideOrg)],
            (None, true) => &[
                Kind::GRANTS[0],
                Kind::GRANTS[1],
                Kind::Token(IntegrationTokenSide::IntegrationTokenSideMember),
            ],
            (None, false) => &Kind::GRANTS,
        };
        confirmation::state(
            format_args!("Authorizing runner {} for {}:", runner.named, all_of(reach)),
            &Self::STATEMENT.lines_of(kinds),
        )?;
        if let Err(exit) = confirmation::go_on(runner.asks)? {
            return Ok(exit);
        }
        let read = super::with_api(deployment, options, |api| {
            let mut subjects = Vec::new();
            if reach.org().is_none() {
                subjects.extend(
                    every_grant(api)?
                        .into_iter()
                        .filter(unrevoked)
                        .map(|grant| Subject::Grant(Box::new(grant))),
                );
            }
            subjects.extend(
                reach
                    .every_token(api)?
                    .into_iter()
                    .map(|token| Subject::Token(Box::new(token))),
            );
            Ok(subjects)
        })?;
        let report = |subject: Option<&Subject>, failure: &MemberGrantFailure| {
            let service = subject.map_or("SERVICE", Subject::provider);
            report_under(api_url, reach, service, failure)
        };
        let subjects = match read {
            Ok(subjects) if subjects.is_empty() => {
                return reach.report_nothing_held(api_url).write(api_url, json);
            }
            Ok(subjects) => subjects,
            Err(failure) => return report(None, &failure).write(api_url, json),
        };
        // A failure other than a refusal of one subject ends the command at that subject,
        // keeping the outcomes of those before it to report.
        let (mut outcomes, ended) =
            each_under_its_key(deployment, options, &subjects, |api, subject, key| {
                authorize_one(api, reach, subject, runner_id, key)
            })?;
        let found_runner = outcomes.iter().any(Outcome::found_runner);
        match &ended {
            Some(failure) if outcomes.is_empty() => {
                return report(subjects.first(), failure).write(api_url, json);
            }
            // Nothing found the runner: it is in none of the organizations the subjects serve.
            None if !found_runner => {
                let failure = if subjects
                    .iter()
                    .all(|subject| matches!(subject, Subject::Token(_)))
                {
                    MemberGrantFailure::IntegrationTokenRunnerNotFound
                } else {
                    MemberGrantFailure::RunnerNotFound
                };
                return report(None, &failure).write(api_url, json);
            }
            _ => {}
        }
        if found_runner {
            for (subject, outcome) in subjects.iter().zip(&mut outcomes) {
                if let Subject::Grant(grant) = subject
                    && grant.kind == MemberGrantKind::GrantKindNative
                    && matches!(
                        outcome,
                        Outcome::Refused(MemberGrantFailure::RunnerNotFound)
                    )
                {
                    *outcome = Outcome::OtherOrganization;
                }
            }
        }
        let ended = ended.map(|failure| report(subjects.get(outcomes.len()), &failure));
        output::write_authorize_all(
            api_url,
            runner_id,
            &subjects,
            &outcomes,
            ended,
            reach.tokens(),
            json,
        )
    }
}

/// What `authorize --all` authorizes the runner for under `reach`, as its statement and its list
/// of runners name it.
fn all_of(reach: &Reach) -> String {
    match (reach.org(), reach.tokens()) {
        (Some(org), _) => format!("every token of the organization {org}"),
        (None, true) => "every grant and token of yours that can serve it".to_owned(),
        (None, false) => "every grant of yours that can serve it".to_owned(),
    }
}

/// Authorizes `runner_id` for `subject` under `key`, once more when the answer was lost: a token
/// through the routes of the owner `reach` names.
fn authorize(
    api: &MemberGrantApi,
    reach: &Reach,
    subject: &Subject,
    runner_id: &str,
    key: &str,
) -> Result<Made, MemberGrantFailure> {
    match subject {
        Subject::Grant(grant) => {
            replayed(|| api.authorize(&grant.id, runner_id, key)).map(Made::Grant)
        }
        Subject::Token(token) => {
            replayed(|| api.authorize_integration_token(reach.owner(), &token.id, runner_id, key))
                .map(Made::Token)
        }
    }
}

/// Reports the authorization of a runner for `subject`.
fn write(api_url: &str, subject: &Subject, made: &Made, json: bool) -> anyhow::Result<ExitCode> {
    write_authorized(api_url, subject.holder(), made.view(), json)
}

/// Authorizes `runner_id` for `subject` unless it already is, reporting a refusal of this one
/// grant or token as its outcome.
fn authorize_one(
    api: &MemberGrantApi,
    reach: &Reach,
    subject: &Subject,
    runner_id: &str,
    key: &str,
) -> Result<Outcome, MemberGrantFailure> {
    if subject
        .in_force()
        .iter()
        .any(|authorization| authorization.runner_id == runner_id)
    {
        return Ok(Outcome::AlreadyAuthorized);
    }
    match (authorize(api, reach, subject, runner_id, key), subject) {
        (Ok(made), _) => Ok(Outcome::Authorized(made)),
        // The grant does not list the runner, so for a native grant the authorization Cloud
        // holds is another runner's; an OAuth grant's is this runner's, made since the list.
        (Err(MemberGrantFailure::AlreadyAuthorized), Subject::Grant(grant)) => {
            Ok(match grant.kind {
                MemberGrantKind::GrantKindNative => Outcome::AuthorizedElsewhere,
                MemberGrantKind::GrantKindOAuth => Outcome::AlreadyAuthorized,
            })
        }
        // A token is authorized for any number of runners: this one, since the list.
        (Err(MemberGrantFailure::IntegrationTokenAlreadyAuthorized), _) => {
            Ok(Outcome::AlreadyAuthorized)
        }
        (Err(failure), _) if refuses_one_subject(&failure) => Ok(Outcome::Refused(failure)),
        (Err(failure), _) => Err(failure),
    }
}
