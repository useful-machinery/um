//! `um grant`: what Useful Machinery may do as the member at other providers, a grant of
//! either kind over the grants resource (REQ-GRANT-64 to REQ-GRANT-74). The resource admits only active humans, so every leaf takes a human session
//! only.

pub(super) mod alias;
pub(super) mod authorization;
pub(super) mod authorize;
mod browser;
mod claude_setup_token;
pub(super) mod codex_login;
pub(super) mod confirmation;
pub(super) mod consent;
pub(crate) mod controlling_terminal;
mod create;
pub(super) mod deauthorize;
mod list;
pub(super) mod material;
mod organization;
pub(super) mod output;
mod program;
pub(super) mod projection;
mod replacement;
pub(super) mod revoke;
mod runner_choice;
mod runner_picker;

use std::collections::BTreeSet;
use std::io::{self, Write};

use anyhow::Context;
use clap::{Args, Subcommand};

use crate::exit_code::ExitCode;
use um_api::{
    MEMBER_GRANT_PAGE, MemberGrant, MemberGrantApi, MemberGrantFailure, MemberGrantKind,
    MemberGrantState,
};
use um_human_auth::Deployment;

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
        super::execute_deployment_command(
            self.command,
            &["grant"],
            "configure Useful Machinery grant access",
            |command, deployment| match command {
                GrantCommand::Authorize(command) => command.run(deployment).map_err(Into::into),
                GrantCommand::Create(command) => command.run(deployment),
                GrantCommand::Deauthorize(command) => command.run(deployment).map_err(Into::into),
                GrantCommand::List(command) => command.run(deployment).map_err(Into::into),
                GrantCommand::Revoke(command) => command.run(deployment).map_err(Into::into),
            },
        )
    }
}

impl super::HumanSessionApi for MemberGrantApi {
    type Failure = MemberGrantFailure;
    type BuildError = um_api::MemberGrantApiError;

    fn build(
        api_url: &str,
        token: &str,
        policy: um_api::HttpTransportPolicy,
    ) -> Result<Self, Self::BuildError> {
        Self::new(api_url, token, policy)
    }

    fn credential_rejected(failure: &MemberGrantFailure) -> bool {
        failure.credential_rejected()
    }

    fn unauthenticated() -> MemberGrantFailure {
        MemberGrantFailure::Unauthenticated
    }

    fn unreachable(category: um_api::UnreachableCategory) -> MemberGrantFailure {
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

const GRANT_HELP: &str = "Grant ID, or the provider of your one grant of it";
const RUNNER_HELP: &str =
    "Runner ID, as um runner list prints it; omit to choose from the runners offered";

/// A runner as a command names it: by ID, since the member's API has no organization in its
/// path to resolve a runner name in (REQ-GRANT-65).
pub(super) fn parse_runner_id(value: &str) -> Result<String, String> {
    if um_support::valid_typed_id(value, "rnr_") {
        Ok(value.to_owned())
    } else {
        Err(
            "runner must be an exact rnr_ identifier; um runner list ORGANIZATION prints it"
                .to_owned(),
        )
    }
}

/// A grant as a command names it (REQ-GRANT-65): its ID, `pgr_` for an OAuth grant and `agl_`
/// for a native one, or the wire name of a provider the member holds one grant of.
#[derive(Clone, Debug)]
enum GrantArg {
    Id(String),
    Provider(&'static str),
}

fn parse_grant(value: &str) -> Result<GrantArg, String> {
    if um_support::valid_typed_id(value, "pgr_") || um_support::valid_typed_id(value, "agl_") {
        return Ok(GrantArg::Id(value.to_owned()));
    }
    projection::PROVIDERS
        .iter()
        .map(|provider| provider.name())
        .find(|name| *name == value)
        .map(GrantArg::Provider)
        .ok_or_else(|| {
            "grant must be an exact pgr_ or agl_ identifier, as um grant list prints it, or one of \
             github, gitlab.com, linear, claude_code and codex"
                .to_owned()
        })
}

/// The grant a `GrantArg` names, or the IDs of the several unrevoked grants of the provider it
/// names, which a command refuses to choose between.
enum Resolved {
    Grant(Box<MemberGrant>),
    Ambiguous {
        provider: &'static str,
        ids: Vec<String>,
    },
}

/// Reads the grant `grant` names: by its ID, or among the member's unrevoked grants of the
/// provider it names, `grant_not_found` when there is none.
fn resolve(api: &MemberGrantApi, grant: &GrantArg) -> Result<Resolved, MemberGrantFailure> {
    let provider = match grant {
        GrantArg::Id(id) => return api.get(id).map(|grant| Resolved::Grant(Box::new(grant))),
        GrantArg::Provider(provider) => *provider,
    };
    let mut held: Vec<_> = every_grant(api)?
        .into_iter()
        .filter(|grant| unrevoked(grant) && grant.provider.as_str() == provider)
        .collect();
    match held.len() {
        0 => Err(MemberGrantFailure::GrantNotFound),
        1 => Ok(Resolved::Grant(Box::new(held.remove(0)))),
        _ => Ok(Resolved::Ambiguous {
            provider,
            ids: held.into_iter().map(|grant| grant.id).collect(),
        }),
    }
}

/// The signed-in member's unrevoked grants of both kinds, read as `um grant list` reads them
/// with the human session alone; with `organization`, an ID or a slug, only that
/// organization's among the coding agents' grants (REQ-CREDREADY-22).
pub(super) fn held(
    deployment: &Deployment,
    policy: um_api::HttpTransportPolicy,
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
/// forever, and is refused as `invalid`. The grant commands, `um auth credentials status` and
/// `um runner doctor --readiness` all page through this one loop.
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
/// have taken effect.
fn replayed<T>(
    mut send: impl FnMut() -> Result<T, MemberGrantFailure>,
) -> Result<T, MemberGrantFailure> {
    match send() {
        Err(MemberGrantFailure::Unreachable { .. }) => send(),
        result => result,
    }
}

/// Reads the grant `grant` names, or reports why the leaf has none: a provider name that names
/// several grants is refused, naming them with `example`, the leaf's command line for one grant
/// ID, and a failure is reported with its remedy.
fn read_grant(
    deployment: &Deployment,
    options: &Options,
    grant: &GrantArg,
    example: impl FnOnce(&str) -> String,
) -> anyhow::Result<Result<Box<MemberGrant>, ExitCode>> {
    let api_url = deployment.fingerprint().api_url();
    let json = options.json;
    Ok(
        match with_api(deployment, options, |api| resolve(api, grant))? {
            Ok(Resolved::Grant(grant)) => Ok(grant),
            Ok(Resolved::Ambiguous { provider, ids }) => Err(output::write_ambiguous(
                api_url, provider, &ids, example, json,
            )?),
            Err(failure) => Err(output::failure(api_url, &failure, json)?),
        },
    )
}

/// Runs the mutating leaf `C` on the grant `grant` names: reads the grant, states on standard
/// error what acting on a grant of its kind does, then acts and reports the result, or the
/// failure. A provider name that names several grants is refused, naming them, before
/// anything is stated or sent.
fn act_on<C: Stated, T>(
    deployment: &Deployment,
    options: &Options,
    grant: &GrantArg,
    heading: impl FnOnce(&MemberGrant) -> String,
    act: impl FnMut(&MemberGrantApi, &MemberGrant) -> Result<T, MemberGrantFailure>,
    write: impl FnOnce(&str, &MemberGrant, T, bool) -> anyhow::Result<ExitCode>,
    // The leaf's command line for one grant ID, which an ambiguous provider name is refused
    // with.
    example: impl FnOnce(&str) -> String,
) -> anyhow::Result<ExitCode> {
    let grant = match read_grant(deployment, options, grant, example)? {
        Ok(grant) => grant,
        Err(exit) => return Ok(exit),
    };
    state_then_act::<C, _>(
        deployment,
        options,
        &grant,
        &heading(&grant),
        false,
        act,
        write,
    )
}

/// States on standard error under `heading` what the leaf `C` does to `grant`, asks whether to
/// go on when `asks` (REQ-GRANT-82), then acts and reports the result, or the failure.
fn state_then_act<C: Stated, T>(
    deployment: &Deployment,
    options: &Options,
    grant: &MemberGrant,
    heading: &str,
    asks: bool,
    mut act: impl FnMut(&MemberGrantApi, &MemberGrant) -> Result<T, MemberGrantFailure>,
    write: impl FnOnce(&str, &MemberGrant, T, bool) -> anyhow::Result<ExitCode>,
) -> anyhow::Result<ExitCode> {
    let api_url = deployment.fingerprint().api_url();
    let json = options.json;
    confirmation::state(format_args!("{heading}"), C::STATEMENT.of(grant.kind))?;
    if let Err(exit) = confirmation::go_on(asks)? {
        return Ok(exit);
    }
    match with_api(deployment, options, |api| act(api, grant))? {
        Ok(value) => write(api_url, grant, value, json),
        Err(failure) => output::failure(api_url, &failure, json),
    }
}

/// States on standard error under `heading` what the leaf `C` does to `grant` on each of the
/// runners `all` holds, asks whether to go on when it asks, then acts on each in turn with
/// `act` and reports each outcome with `write`, or the refusal (REQ-GRANT-83). `act` answers
/// a refusal of that one runner as its outcome, and any other failure ends the command at that
/// runner, after the outcomes before it. The command exits 1 when any runner was refused.
fn state_then_act_on_each<C: Stated, T>(
    deployment: &Deployment,
    options: &Options,
    grant: &MemberGrant,
    heading: &str,
    all: &runner_choice::All,
    mut act: impl FnMut(&MemberGrantApi, &MemberGrant, &str, &str) -> RunnerOutcome<T>,
    mut write: impl FnMut(&str, &MemberGrant, &str, T) -> anyhow::Result<ExitCode>,
) -> anyhow::Result<ExitCode> {
    let api_url = deployment.fingerprint().api_url();
    confirmation::state(format_args!("{heading}"), C::STATEMENT.of(grant.kind))?;
    if let Err(exit) = confirmation::go_on(all.asks)? {
        return Ok(exit);
    }
    let (outcomes, ended) =
        each_under_its_key(deployment, options, &all.runners, |api, runner, key| {
            act(api, grant, &runner.id, key)
        })?;
    if let Some(failure) = &ended
        && outcomes.is_empty()
    {
        return output::failure(api_url, failure, options.json);
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
                write(api_url, grant, &runner.id, value)?;
                written = true;
            }
            Err(failure) => {
                output::write_runner_failure(api_url, &runner.named, &failure)?;
                refused = true;
            }
        }
    }
    if let Some(failure) = &ended {
        let named = all
            .runners
            .get(reached)
            .map_or("", |runner| runner.named.as_str());
        output::write_runner_failure(api_url, named, failure)?;
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
        .map(|_| um_support::generate_idempotency_key())
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

/// Reads the grant `grant` names and the runner the leaf `C` acts on: the one `runner` names,
/// or, given none, one chosen among those `offered` reads for the grant (REQ-GRANT-81,
/// REQ-GRANT-82), whose refusal or list names the runner `purpose` asks for, or, where `all`
/// says the grant can take them, all of them chosen as one (REQ-GRANT-83). A provider name
/// that names several grants is refused, naming them, before any runner is read.
fn grant_and_runner<C: Stated>(
    deployment: &Deployment,
    options: &Options,
    grant: &GrantArg,
    runner: &RunnerArgs<'_>,
    offered: impl FnOnce(&MemberGrant) -> anyhow::Result<runner_choice::Offer>,
    purpose: impl FnOnce(&MemberGrant) -> String,
    all: impl FnOnce(&MemberGrant) -> bool,
) -> anyhow::Result<Result<(Box<MemberGrant>, runner_choice::Chosen), ExitCode>> {
    let grant = match read_grant(deployment, options, grant, |id| match runner.id {
        Some(runner_id) => format!("{} {id} {runner_id} --yes", C::COMMAND),
        None => format!("{} {id}", C::COMMAND),
    })? {
        Ok(grant) => grant,
        Err(exit) => return Ok(Err(exit)),
    };
    if let Some(runner_id) = runner.id {
        return Ok(Ok((
            grant,
            runner_choice::Chosen::One(runner_choice::Acting::named(runner_id)),
        )));
    }
    let offer = offered(&grant)?;
    let purpose = purpose(&grant);
    let example = |runner_id: &str| format!("{} {} {runner_id} --yes", C::COMMAND, grant.id);
    let (json, yes, grant_id) = (options.json, runner.yes, Some(grant.id.as_str()));
    let chosen = if all(&grant) {
        runner_choice::choose_one_or_all(deployment, json, yes, offer, grant_id, &purpose, example)?
    } else {
        runner_choice::choose(deployment, json, yes, offer, grant_id, &purpose, example)?
            .map(runner_choice::Chosen::One)
    };
    Ok(chosen.map(|chosen| (grant, chosen)))
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
