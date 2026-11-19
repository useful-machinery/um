//! The leaves `useful integration` runs, and `useful grant` under its earlier, hidden name
//! (REQ-INTEGRATION-13, REQ-INTEGRATION-15): what Useful Machinery may do as the member at other
//! providers, a grant of either kind over the grants resource (REQ-GRANT-64 to REQ-GRANT-74). The
//! resource admits only active humans, so every leaf takes a human session only. Every command
//! they name to a member is a `useful integration` or `useful runner integration` one, whatever
//! spelling ran.

pub(super) mod alias;
pub(super) mod authorization;
pub(super) mod authorize;
mod browser;
mod claude_setup_token;
pub(super) mod codex_login;
pub(super) mod confirmation;
pub(super) mod consent;
pub(crate) mod controlling_terminal;
pub(super) mod create;
pub(super) mod deauthorize;
pub(super) mod list;
pub(super) mod material;
pub(super) mod organization;
pub(super) mod output;
mod program;
pub(super) mod projection;
pub(super) mod replacement;
pub(super) mod revoke;
mod runner_choice;
mod runner_picker;

use std::collections::BTreeSet;
use std::io::{self, Write};

use anyhow::Context;
use clap::{Args, Subcommand};

use crate::exit_code::ExitCode;
use useful_api::{
    IntegrationToken, IntegrationTokenSide, MEMBER_GRANT_PAGE, MemberGrant, MemberGrantApi,
    MemberGrantFailure, MemberGrantKind, MemberGrantProvider, MemberGrantState,
};
use useful_human_auth::Deployment;

use super::integration::Reach;
use super::{ApiFailureReport, CommandFailure, CommandResult};
use authorization::Holder;
use confirmation::Stated;

pub(super) use create::Flow;

pub(super) const ABOUT: &str = "Manage what Useful Machinery may do as you at other providers";

// The grants resource admits only active humans, so these leaves take no service API key.
type Options = super::CommonArgs<super::GrantJson, super::NoAuthenticationArgs>;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<GrantCommand>,
}

#[derive(Debug, Subcommand)]
enum GrantCommand {
    #[command(about = authorize::ABOUT)]
    Authorize(authorize::Command),
    #[command(about = create::ABOUT)]
    Create(create::Command),
    #[command(about = deauthorize::ABOUT)]
    Deauthorize(deauthorize::Command),
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = revoke::ABOUT)]
    Revoke(revoke::Command),
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        execute(self.command, &["grant"], |command| match command {
            GrantCommand::Authorize(command) => Leaf::Authorize(command, Reach::GRANTS),
            GrantCommand::Create(command) => Leaf::Create(command, Reach::GRANTS),
            GrantCommand::Deauthorize(command) => Leaf::Deauthorize(command, Reach::GRANTS),
            GrantCommand::List(command) => Leaf::List(command, Reach::GRANTS),
            GrantCommand::Revoke(command) => Leaf::Revoke(command, Reach::GRANTS),
        })
    }
}

/// A leaf of `useful grant` or of `useful integration`, which hold the same leaves
/// (REQ-INTEGRATION-13, REQ-INTEGRATION-15). Each carries what only `useful integration` hands a
/// leaf: whether token services are in reach, and the organization `--org` names, and for the
/// list the side it states of each grant (REQ-INTEGRATION-16 to REQ-INTEGRATION-20).
pub(in crate::cli) enum Leaf {
    Authorize(authorize::Command, Reach),
    Create(create::Command, Reach),
    Deauthorize(deauthorize::Command, Reach),
    List(list::Command, Reach),
    Revoke(revoke::Command, Reach),
}

/// Runs the leaf `command` names under the family at `path`, or prints that family's help
/// when it names none.
pub(super) fn execute<C>(
    command: Option<C>,
    path: &[&str],
    leaf: impl FnOnce(C) -> Leaf,
) -> super::CommandResult {
    super::execute_deployment_command(
        command,
        path,
        "configure Useful Machinery grant access",
        |command, deployment| match leaf(command) {
            Leaf::Authorize(command, reach) => command.run(deployment, &reach),
            Leaf::Create(command, reach) => command.run(deployment, &reach),
            Leaf::Deauthorize(command, reach) => command.run(deployment, &reach),
            Leaf::List(command, reach) => command.run(deployment, &reach).map_err(Into::into),
            Leaf::Revoke(command, reach) => command.run(deployment, &reach),
        },
    )
}

impl super::HumanSessionApi for MemberGrantApi {
    type Failure = MemberGrantFailure;
    type BuildError = useful_api::MemberGrantApiError;

    fn build(
        api_url: &str,
        token: &str,
        policy: useful_api::HttpTransportPolicy,
    ) -> Result<Self, Self::BuildError> {
        Self::new(api_url, token, policy)
    }

    fn credential_rejected(failure: &MemberGrantFailure) -> bool {
        failure.credential_rejected()
    }

    fn unauthenticated() -> MemberGrantFailure {
        MemberGrantFailure::Unauthenticated
    }

    fn unreachable(category: useful_api::UnreachableCategory) -> MemberGrantFailure {
        MemberGrantFailure::Unreachable { category }
    }
}

/// Runs `f` against the grants resource with the signed-in human session, refreshing a
/// rejected session once.
fn with_api<T, F: super::JsonOutputKind>(
    deployment: &Deployment,
    options: &super::CommonArgs<F, super::NoAuthenticationArgs>,
    f: impl FnMut(&MemberGrantApi) -> Result<T, MemberGrantFailure>,
) -> anyhow::Result<Result<T, MemberGrantFailure>> {
    super::with_human_session_api(deployment, options.http.transport_policy(), f)
}

const INTEGRATION_HELP: &str = "Provider you hold one grant of, or a grant's ID";
const RUNNER_HELP: &str =
    "Runner ID, as useful runner list prints it; omit to choose from the runners offered";

/// A runner as a command names it: by ID, since the member's API has no organization in its
/// path to resolve a runner name in (REQ-GRANT-65).
pub(super) fn parse_runner_id(value: &str) -> Result<String, String> {
    if useful_support::valid_typed_id(value, "rnr_") {
        Ok(value.to_owned())
    } else {
        Err(
            "runner must be an exact rnr_ identifier; useful runner list ORGANIZATION prints it"
                .to_owned(),
        )
    }
}

/// A grant as a command names it (REQ-GRANT-65): its ID, `pgr_` for an OAuth grant and `agl_`
/// for a native one, or the wire name of a provider the member holds one grant of, which is
/// any provider id or `claude_code` (REQ-GRANT-96).
#[derive(Clone, Debug)]
enum GrantArg {
    Id(String),
    Provider(String),
}

impl GrantArg {
    /// The ID or provider name as the command line gave it.
    fn as_str(&self) -> &str {
        match self {
            Self::Id(name) | Self::Provider(name) => name,
        }
    }
}

fn parse_grant(value: &str) -> Result<GrantArg, String> {
    if useful_support::valid_typed_id(value, "pgr_")
        || useful_support::valid_typed_id(value, "agl_")
    {
        return Ok(GrantArg::Id(value.to_owned()));
    }
    if useful_api::valid_provider_id(value)
        || value == MemberGrantProvider::GrantProviderClaudeCode.as_str()
    {
        return Ok(GrantArg::Provider(value.to_owned()));
    }
    Err(
        "grant must be an exact pgr_ or agl_ identifier, as useful integration list prints it, or a \
         provider name: claude_code, or a lowercase letter followed by at most 62 lowercase \
         letters, digits, dots and hyphens, such as github"
            .to_owned(),
    )
}

/// The grant a `GrantArg` names, or the IDs of the several unrevoked grants of the provider it
/// names, which a command refuses to choose between.
enum Resolved {
    Grant(Box<MemberGrant>),
    Ambiguous { provider: String, ids: Vec<String> },
}

/// Reads the grant `grant` names: by its ID, or among the member's unrevoked grants of the
/// provider it names, `grant_not_found` when there is none.
fn resolve(api: &MemberGrantApi, grant: &GrantArg) -> Result<Resolved, MemberGrantFailure> {
    let provider = match grant {
        GrantArg::Id(id) => return api.get(id).map(|grant| Resolved::Grant(Box::new(grant))),
        GrantArg::Provider(provider) => provider.as_str(),
    };
    let mut held: Vec<_> = every_grant(api)?
        .into_iter()
        .filter(|grant| unrevoked(grant) && grant.provider.as_str() == provider)
        .collect();
    match held.len() {
        0 => Err(MemberGrantFailure::GrantNotFound),
        1 => Ok(Resolved::Grant(Box::new(held.remove(0)))),
        _ => Ok(Resolved::Ambiguous {
            provider: provider.to_owned(),
            ids: held.into_iter().map(|grant| grant.id).collect(),
        }),
    }
}

/// What a name finds under the reach a leaf was handed: a grant, or the several unrevoked grants
/// of the provider it names, as [`resolve`] answers; or, where token services are in reach, a
/// token, the member's or with `--org` the organization's.
enum Found {
    Grant(Resolved),
    Token(Box<IntegrationToken>),
}

/// Finds what `name` names under `reach` (REQ-INTEGRATION-19). Without `--org` it is looked up
/// among the member's grants first, so a name that names a grant sends what it sent under
/// `useful grant`, and a provider name that names none among the member's tokens where `reach`
/// puts them in reach; with `--org` among that organization's tokens alone, and no grant. Not
/// found is the failure: `grant_not_found` as before for a name the member holds nothing of,
/// and `token_not_found` for a service the organization holds no token of, which a grant's ID
/// never names.
fn lookup(
    api: &MemberGrantApi,
    name: &GrantArg,
    reach: &Reach,
) -> Result<Found, MemberGrantFailure> {
    let token = |service| {
        reach
            .token_named(api, service)
            .map(|token| token.map(|token| Found::Token(Box::new(token))))
    };
    if reach.org().is_some() {
        let found = match name {
            GrantArg::Provider(service) => token(service)?,
            GrantArg::Id(_) => None,
        };
        return found.ok_or(MemberGrantFailure::IntegrationTokenNotFound);
    }
    match (resolve(api, name), name) {
        (Err(MemberGrantFailure::GrantNotFound), GrantArg::Provider(service)) if reach.tokens() => {
            token(service)?.ok_or(MemberGrantFailure::GrantNotFound)
        }
        (resolved, _) => resolved.map(Found::Grant),
    }
}

/// The signed-in member's unrevoked grants of both kinds, read as `useful integration list` reads
/// them with the human session alone; with `organization`, an ID or a slug, only that
/// organization's among the coding agents' grants (REQ-CREDREADY-22).
pub(super) fn held(
    deployment: &Deployment,
    policy: useful_api::HttpTransportPolicy,
    organization: Option<&str>,
) -> anyhow::Result<Result<Vec<MemberGrant>, MemberGrantFailure>> {
    super::with_human_session_api(deployment, policy, |api: &MemberGrantApi| {
        let organization = organization
            .map(|named| organization::named(api, named))
            .transpose()?;
        Ok(every_grant(api)?
            .into_iter()
            .filter(|grant| {
                unrevoked(grant)
                    && (grant.kind == MemberGrantKind::GrantKindOAuth
                        || organization.as_ref().is_none_or(|organization| {
                            grant.organization_id.as_deref() == Some(organization.id.as_str())
                        }))
            })
            .collect())
    })
}

fn unrevoked(grant: &MemberGrant) -> bool {
    grant.state != MemberGrantState::GrantStateRevoked
}

/// Every grant of the member, of both kinds, OAuth grants first.
fn every_grant(api: &MemberGrantApi) -> Result<Vec<MemberGrant>, MemberGrantFailure> {
    let invalid = MemberGrantFailure::InvalidResponse {
        credential_rejected: false,
    };
    every_page(invalid, |cursor| {
        api.list(None, Some(MEMBER_GRANT_PAGE), cursor)
            .map(|page| (page.items, page.next_cursor))
    })
}

/// Reads a list to its last page, following each page's next cursor; `read` lists the page at
/// a cursor as its items and the next cursor. A next cursor already followed would page
/// forever, and is refused as `invalid`. The grant commands, `useful auth credentials status` and
/// `useful runner doctor --readiness` all page through this one loop.
pub(super) fn every_page<T, E>(
    invalid: E,
    mut read: impl FnMut(Option<&str>) -> Result<(Vec<T>, Option<String>), E>,
) -> Result<Vec<T>, E> {
    let mut items = Vec::new();
    let mut seen = BTreeSet::new();
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) = read(cursor.as_deref())?;
        items.extend(page);
        let Some(next) = next else {
            return Ok(items);
        };
        if !seen.insert(next.clone()) {
            return Err(invalid);
        }
        cursor = Some(next);
    }
}

/// Sends a mutation, and once more under the same key when its answer was lost: the first may
/// have taken effect. A grant's and an integration token's mutations are sent this way.
pub(in crate::cli) fn replayed<T>(
    mut send: impl FnMut() -> Result<T, MemberGrantFailure>,
) -> Result<T, MemberGrantFailure> {
    match send() {
        Err(MemberGrantFailure::Unreachable { .. }) => send(),
        result => result,
    }
}

/// What a mutating leaf acts on (REQ-INTEGRATION-20): a grant of either kind, or, under
/// `useful integration`, a token service's token, the member's or with `--org` the
/// organization's. The helpers below read of it only its ID, the kind or side its statement
/// depends on, its authorizations in force and the organizations whose runners it may serve.
pub(super) enum Subject {
    Grant(Box<MemberGrant>),
    Token(Box<IntegrationToken>),
}

/// One authorization in force of a subject: its ID, the runner's and the runner's organization's.
pub(super) struct InForce<'a> {
    pub(super) id: &'a str,
    pub(super) runner_id: &'a str,
    pub(super) organization_id: &'a str,
}

impl Subject {
    pub(super) fn id(&self) -> &str {
        match self {
            Self::Grant(grant) => &grant.id,
            Self::Token(token) => &token.id,
        }
    }

    /// The provider of the grant, or the token service of the token.
    pub(super) fn provider(&self) -> &str {
        match self {
            Self::Grant(grant) => grant.provider.as_str(),
            Self::Token(token) => &token.provider,
        }
    }

    /// What the statement of acting on it depends on.
    pub(super) const fn kind(&self) -> confirmation::Kind {
        match self {
            Self::Grant(grant) => confirmation::Kind::Grant(grant.kind),
            Self::Token(token) => confirmation::Kind::Token(token.side),
        }
    }

    /// What a result names the authorization's holder by.
    pub(super) fn holder(&self) -> Holder<'_> {
        match self {
            Self::Grant(grant) => Holder::Grant(&grant.id),
            Self::Token(token) => Holder::IntegrationToken(&token.id),
        }
    }

    /// Its authorizations in force: a grant lists only those (REQ-GRANT-58), and so does a token.
    pub(super) fn in_force(&self) -> Vec<InForce<'_>> {
        match self {
            Self::Grant(grant) => grant
                .authorizations
                .iter()
                .map(|authorization| InForce {
                    id: &authorization.id,
                    runner_id: &authorization.runner_id,
                    organization_id: &authorization.organization_id,
                })
                .collect(),
            Self::Token(token) => token
                .authorizations
                .iter()
                .map(|authorization| InForce {
                    id: &authorization.id,
                    runner_id: &authorization.runner_id,
                    organization_id: &authorization.organization_id,
                })
                .collect(),
        }
    }

    /// The organizations whose runners alone it may serve: a native grant's and an
    /// organization's token's; `None` for those of every organization the member is an active
    /// member of, which an OAuth grant and the member's token serve.
    pub(super) fn serving(&self) -> Option<BTreeSet<String>> {
        match self {
            Self::Grant(grant) => (grant.kind == MemberGrantKind::GrantKindNative)
                .then(|| grant.organization_id.iter().cloned().collect()),
            Self::Token(token) => (token.side == IntegrationTokenSide::IntegrationTokenSideOrg)
                .then(|| token.organization_id.iter().cloned().collect()),
        }
    }

    /// How a statement's heading names it: a grant by its ID, a token by its service and ID.
    fn named(&self) -> String {
        match self {
            Self::Grant(grant) => format!("grant {}", grant.id),
            Self::Token(token) => format!("{} token {}", token.provider, token.id),
        }
    }

    /// How a command line names it: a grant by its ID, a token by its service, with the
    /// organization `--org` names for an organization's.
    fn argument(&self, reach: &Reach) -> String {
        match (self, reach.org()) {
            (Self::Grant(grant), _) => grant.id.clone(),
            (Self::Token(token), None) => token.provider.clone(),
            (Self::Token(token), Some(org)) => format!("{} --org {org}", token.provider),
        }
    }
}

/// What a mutating leaf runs with: the deployment, the leaf's options and what its dispatcher
/// handed it.
#[derive(Clone, Copy)]
pub(super) struct LeafRun<'a> {
    pub(super) deployment: &'a Deployment,
    pub(super) options: &'a Options,
    pub(super) reach: &'a Reach,
}

/// `failure` of a leaf acting on what `service` names, as the member reads it: with `--org` as a
/// failure on the organization's token, whose remedy names the organization
/// (REQ-INTEGRATION-17); otherwise as a grant's, so that `useful grant` reports what it did.
fn report_under(
    deployment: &str,
    reach: &Reach,
    service: &str,
    failure: &MemberGrantFailure,
) -> ApiFailureReport {
    match reach.org() {
        Some(_) => reach.report(deployment, failure, service),
        None => output::report(deployment, failure),
    }
}

/// Refuses `--org` for `name` before anything is read, exit 2 (REQ-INTEGRATION-17,
/// REQ-INTEGRATION-21): a coding agent's name or a grant's ID names a grant, which is the
/// member's own, and `github`, `gitlab.com` and `linear` name what an organization holds as a
/// connection, which acts for Cloud and is authorized for no runner; `example` is the leaf's
/// command line for it without `--org`.
fn refuse_org_for_a_grant(
    reach: &Reach,
    name: &GrantArg,
    example: impl FnOnce(&str) -> String,
) -> Result<(), CommandFailure> {
    if reach.org().is_none() {
        return Ok(());
    }
    let native = match name {
        GrantArg::Provider(provider) => match MemberGrantProvider::from_wire(provider) {
            Some(provider) => provider.kind() == MemberGrantKind::GrantKindNative,
            None => return Ok(()),
        },
        GrantArg::Id(id) => id.starts_with("agl_"),
    };
    Err(reach.org_refused(name.as_str(), native, &example(name.as_str())))
}

/// Reads what `name` names under `reach`, or reports why the leaf has nothing to act on: a
/// provider name that names several grants is refused, naming them with `example`, the leaf's
/// command line for one grant ID, and a failure is reported with its remedy. With `--org`, a name
/// the organization holds no token of is read for among the providers the deployment serves: an
/// OAuth provider takes no `--org` and is refused with exit 2, and any other is reported as no
/// token of the organization's (REQ-INTEGRATION-17).
fn read_subject(
    context: LeafRun<'_>,
    name: &GrantArg,
    example: impl Fn(&str) -> String,
) -> Result<Result<Subject, ExitCode>, CommandFailure> {
    let LeafRun {
        deployment,
        options,
        reach,
    } = context;
    let api_url = deployment.fingerprint().api_url();
    let json = options.json;
    let found = with_api(deployment, options, |api| match lookup(api, name, reach) {
        Err(MemberGrantFailure::IntegrationTokenNotFound) if reach.org().is_some() => {
            let served = api.served_integrations()?;
            let provider = served.is_some_and(|served| {
                served
                    .providers
                    .iter()
                    .any(|provider| provider.name == name.as_str())
            });
            // None: a provider the deployment serves, which takes no `--org`.
            if provider {
                Ok(None)
            } else {
                Err(MemberGrantFailure::IntegrationTokenNotFound)
            }
        }
        found => found.map(Some),
    })?;
    Ok(match found {
        Ok(Some(Found::Grant(Resolved::Grant(grant)))) => Ok(Subject::Grant(grant)),
        Ok(Some(Found::Grant(Resolved::Ambiguous { provider, ids }))) => Err(
            output::write_ambiguous(api_url, &provider, &ids, example, json)?,
        ),
        Ok(Some(Found::Token(token))) => Ok(Subject::Token(token)),
        Ok(None) => {
            return Err(reach.org_refused(name.as_str(), false, &example(name.as_str())));
        }
        Err(failure) => {
            Err(report_under(api_url, reach, name.as_str(), &failure).write(api_url, json)?)
        }
    })
}

/// Runs the mutating leaf `C` on what `name` names under `reach`: reads it, states on standard
/// error what acting on a subject of its kind does, then acts and reports the result, or the
/// failure. A provider name that names several grants is refused, naming them, before
/// anything is stated or sent, and so is `--org` for a grant's provider.
fn act_on<C: Stated, T>(
    context: LeafRun<'_>,
    name: &GrantArg,
    heading: impl FnOnce(&str) -> String,
    act: impl FnMut(&MemberGrantApi, &Subject) -> Result<T, MemberGrantFailure>,
    write: impl FnOnce(&str, &Subject, T, bool) -> anyhow::Result<ExitCode>,
    // The leaf's command line for one grant ID, which an ambiguous provider name is refused
    // with.
    example: impl Fn(&str) -> String,
) -> CommandResult {
    refuse_org_for_a_grant(context.reach, name, &example)?;
    let subject = match read_subject(context, name, example)? {
        Ok(subject) => subject,
        Err(exit) => return Ok(exit),
    };
    Ok(state_then_act::<C, _>(
        context,
        &subject,
        &heading(&subject.named()),
        false,
        act,
        write,
    )?)
}

/// States on standard error under `heading` what the leaf `C` does to `subject`, asks whether to
/// go on when `asks` (REQ-GRANT-82), then acts and reports the result, or the failure.
fn state_then_act<C: Stated, T>(
    context: LeafRun<'_>,
    subject: &Subject,
    heading: &str,
    asks: bool,
    mut act: impl FnMut(&MemberGrantApi, &Subject) -> Result<T, MemberGrantFailure>,
    write: impl FnOnce(&str, &Subject, T, bool) -> anyhow::Result<ExitCode>,
) -> anyhow::Result<ExitCode> {
    let LeafRun {
        deployment,
        options,
        reach,
    } = context;
    let api_url = deployment.fingerprint().api_url();
    let json = options.json;
    confirmation::state(format_args!("{heading}"), C::STATEMENT.of(subject.kind()))?;
    if let Err(exit) = confirmation::go_on(asks)? {
        return Ok(exit);
    }
    match with_api(deployment, options, |api| act(api, subject))? {
        Ok(value) => write(api_url, subject, value, json),
        Err(failure) => {
            report_under(api_url, reach, subject.provider(), &failure).write(api_url, json)
        }
    }
}

/// States on standard error under `heading` what the leaf `C` does to `subject` on each of the
/// runners `all` holds, asks whether to go on when it asks, then acts on each in turn with
/// `act` and reports each outcome with `write`, or the refusal (REQ-GRANT-83). `act` answers
/// a refusal of that one runner as its outcome, and any other failure ends the command at that
/// runner, after the outcomes before it. The command exits 1 when any runner was refused.
fn state_then_act_on_each<C: Stated, T>(
    context: LeafRun<'_>,
    subject: &Subject,
    heading: &str,
    all: &runner_choice::All,
    mut act: impl FnMut(&MemberGrantApi, &Subject, &str, &str) -> RunnerOutcome<T>,
    mut write: impl FnMut(&str, &Subject, &str, T) -> anyhow::Result<ExitCode>,
) -> anyhow::Result<ExitCode> {
    let LeafRun {
        deployment,
        options,
        reach,
    } = context;
    let api_url = deployment.fingerprint().api_url();
    let report =
        |failure: &MemberGrantFailure| report_under(api_url, reach, subject.provider(), failure);
    confirmation::state(format_args!("{heading}"), C::STATEMENT.of(subject.kind()))?;
    if let Err(exit) = confirmation::go_on(all.asks)? {
        return Ok(exit);
    }
    let (outcomes, ended) =
        each_under_its_key(deployment, options, &all.runners, |api, runner, key| {
            act(api, subject, &runner.id, key)
        })?;
    if let Some(failure) = &ended
        && outcomes.is_empty()
    {
        return report(failure).write(api_url, options.json);
    }
    let reached = outcomes.len();
    let mut refused = false;
    let mut written = false;
    for (runner, outcome) in all.runners.iter().zip(outcomes) {
        match outcome {
            Ok(value) => {
                if written {
                    writeln!(io::stdout().lock())?;
                }
                write(api_url, subject, &runner.id, value)?;
                written = true;
            }
            Err(failure) => {
                output::write_runner_failure(&runner.named, &report(&failure))?;
                refused = true;
            }
        }
    }
    if let Some(failure) = &ended {
        let named = all
            .runners
            .get(reached)
            .map_or("", |runner| runner.named.as_str());
        output::write_runner_failure(named, &report(failure))?;
    }
    Ok(if refused || ended.is_some() {
        ExitCode::GeneralFailure
    } else {
        ExitCode::Success
    })
}

/// Acts on each of `items` in turn with `act`, each under a request key of its own: the keys
/// are generated before anything is sent and kept for the replay of a lost answer and of a
/// refreshed session. Answers the outcomes in order and, when a failure ended the command, that
/// failure, met at the item after the last outcome. `authorize --all` ranges over grants this
/// way (REQ-GRANT-66) and a leaf with all runners chosen over runners (REQ-GRANT-83).
fn each_under_its_key<I, T>(
    deployment: &Deployment,
    options: &Options,
    items: &[I],
    mut act: impl FnMut(&MemberGrantApi, &I, &str) -> Result<T, MemberGrantFailure>,
) -> anyhow::Result<(Vec<T>, Option<MemberGrantFailure>)> {
    let keys = items
        .iter()
        .map(|_| useful_support::generate_idempotency_key())
        .collect::<Result<Vec<_>, _>>()
        .context("generate a request key for each request")?;
    let mut outcomes = Vec::with_capacity(keys.len());
    let ended = with_api(deployment, options, |api| {
        // A refreshed session replays every item with its key, so the outcomes restart.
        outcomes.clear();
        for (item, key) in items.iter().zip(&keys) {
            outcomes.push(act(api, item, key)?);
        }
        Ok(())
    })?
    .err();
    Ok((outcomes, ended))
}

/// One runner's outcome among all a leaf acts on: what acting yields, or the refusal of that
/// runner; the outer error is a failure that ends the command.
type RunnerOutcome<T> = Result<Result<T, MemberGrantFailure>, MemberGrantFailure>;

/// `result` as one runner's outcome: a failure `refuses_runner` names is that runner's refusal,
/// and any other ends the command.
fn runner_outcome<T>(
    result: Result<T, MemberGrantFailure>,
    refuses_runner: impl FnOnce(&MemberGrantFailure) -> bool,
) -> RunnerOutcome<T> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(failure) if refuses_runner(&failure) => Ok(Err(failure)),
        Err(failure) => Err(failure),
    }
}

/// What the command line of `authorize GRANT` or `deauthorize GRANT` names of the runner: its
/// ID, or none, and `--yes`.
struct RunnerArgs<'a> {
    id: Option<&'a str>,
    yes: bool,
}

/// Reads what `name` names under `reach` and the runner the leaf `C` acts on: the one `runner`
/// names, or, given none, one chosen among those `offered` reads for the subject (REQ-GRANT-81,
/// REQ-GRANT-82), whose refusal or list names the runner `purpose` asks for, or, where `all`
/// says the subject can take them, all of them chosen as one (REQ-GRANT-83). A provider name
/// that names several grants is refused, naming them, before any runner is read, and so is
/// `--org` for a grant's provider.
fn subject_and_runner<C: Stated>(
    context: LeafRun<'_>,
    name: &GrantArg,
    runner: &RunnerArgs<'_>,
    offered: impl FnOnce(&Subject) -> anyhow::Result<runner_choice::Offer>,
    purpose: impl FnOnce(&Subject) -> String,
    all: impl FnOnce(&Subject) -> bool,
) -> Result<Result<(Subject, runner_choice::Chosen), ExitCode>, CommandFailure> {
    let example = |argument: &str| match runner.id {
        Some(runner_id) => format!("{} {argument} {runner_id} --yes", C::COMMAND),
        None => format!("{} {argument}", C::COMMAND),
    };
    let LeafRun {
        deployment,
        options,
        reach,
    } = context;
    refuse_org_for_a_grant(reach, name, example)?;
    let subject = match read_subject(context, name, example)? {
        Ok(subject) => subject,
        Err(exit) => return Ok(Err(exit)),
    };
    if let Some(runner_id) = runner.id {
        return Ok(Ok((
            subject,
            runner_choice::Chosen::One(runner_choice::Acting::named(runner_id)),
        )));
    }
    let offer = offered(&subject)?;
    let purpose = purpose(&subject);
    let argument = subject.argument(reach);
    let example = |runner_id: &str| format!("{} {argument} {runner_id} --yes", C::COMMAND);
    let (json, yes, holder) = (options.json, runner.yes, Some(subject.holder()));
    let chosen = if all(&subject) {
        runner_choice::choose_one_or_all(deployment, json, yes, offer, holder, &purpose, example)?
    } else {
        runner_choice::choose(deployment, json, yes, offer, holder, &purpose, example)?
            .map(runner_choice::Chosen::One)
    };
    Ok(chosen.map(|chosen| (subject, chosen)))
}

#[cfg(test)]
mod tests {
    use super::every_page;

    #[derive(Debug, PartialEq)]
    enum Failure {
        RepeatedCursor,
        Looped,
    }

    // REQ-GRANT-64: the command reads every page, and a server whose cursors come back round
    // to one already followed ends the read with a failure rather than paging forever.
    #[test]
    fn a_cursor_seen_before_ends_the_read_with_a_failure() {
        let mut reads = Vec::new();
        let result = every_page(Failure::RepeatedCursor, |cursor| {
            reads.push(cursor.map(str::to_owned));
            let next = match cursor {
                None | Some("B") => "A",
                Some(_) => "B",
            };
            if reads.len() > 4 {
                return Err(Failure::Looped);
            }
            Ok((vec![reads.len()], Some(next.to_owned())))
        });

        assert_eq!(result, Err(Failure::RepeatedCursor));
        assert_eq!(
            reads,
            [None, Some("A".to_owned()), Some("B".to_owned())],
            "the read stops at the page that names A again"
        );
    }
}
