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

use std::collections::BTreeSet;

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
const RUNNER_HELP: &str = "Runner ID, as um runner list prints it";

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

/// Runs the mutating leaf `C` on the grant `grant` names: reads the grant, states on standard
/// error what acting on a grant of its kind does, then acts and reports the result, or the
/// failure. A provider name that names several grants is refused, naming them, before
/// anything is stated or sent.
fn act_on<C: Stated, T>(
    deployment: &Deployment,
    options: &Options,
    grant: &GrantArg,
    heading: impl FnOnce(&MemberGrant) -> String,
    mut act: impl FnMut(&MemberGrantApi, &MemberGrant) -> Result<T, MemberGrantFailure>,
    write: impl FnOnce(&str, &MemberGrant, T, bool) -> anyhow::Result<ExitCode>,
    // The leaf's command line for one grant ID, which an ambiguous provider name is refused
    // with.
    example: impl FnOnce(&str) -> String,
) -> anyhow::Result<ExitCode> {
    let api_url = deployment.fingerprint().api_url();
    let json = options.json;
    let grant = match with_api(deployment, options, |api| resolve(api, grant))? {
        Ok(Resolved::Grant(grant)) => grant,
        Ok(Resolved::Ambiguous { provider, ids }) => {
            return output::write_ambiguous(api_url, provider, &ids, example, json);
        }
        Err(failure) => return output::failure(api_url, &failure, json),
    };
    confirmation::state(
        format_args!("{}", heading(&grant)),
        C::STATEMENT.of(grant.kind),
    )?;
    match with_api(deployment, options, |api| act(api, &grant))? {
        Ok(value) => write(api_url, &grant, value, json),
        Err(failure) => output::failure(api_url, &failure, json),
    }
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
