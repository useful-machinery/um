use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_api::{MemberGrant, MemberGrantAuthorization, MemberGrantFailure, MemberGrantKind};
use um_human_auth::Deployment;

use super::authorization::{AuthorizationView, Holder, write_authorized};
use super::confirmation::{self, Confirmation, NATIVE_AUTHORIZE, Stated, Statement};
use super::{
    GRANT_HELP, GrantArg, Options, RUNNER_HELP, act_on, every_grant, output, replayed, unrevoked,
};

pub(super) const ABOUT: &str = "Authorize a runner to use one of your grants, or all of them";

/// The remedy for a Codex grant authorized for another runner (REQ-GRANT-66), which `um grant
/// authorize` and the earlier `um auth agent-login authorize` both report: a Codex grant serves
/// one runner, and another sign-in cannot make a second grant for another runner, since a held
/// grant is refused or replaced in place (REQ-GRANT-73).
pub(in crate::cli) const MOVE_CODEX_GRANT: &str = "A Codex grant serves one runner at a time. To move it to this runner, end its authorization for the other, which `um grant list GRANT` names, then authorize this one:\n  um grant deauthorize GRANT RUNNER --yes";

// `--all` takes the place of `GRANT`, so `RUNNER` is the one positional it is given.
#[derive(Debug, Args)]
#[command(allow_missing_positional = true)]
#[group(id = "target", args = ["grant", "all"], required = true, multiple = false)]
pub(in crate::cli) struct Command {
    #[arg(value_name = "GRANT", value_parser = super::parse_grant, help = GRANT_HELP)]
    grant: Option<GrantArg>,

    #[arg(
        long,
        help = "Authorize the runner for every grant of yours that can serve it, in place of GRANT"
    )]
    all: bool,

    #[arg(value_name = "RUNNER", value_parser = super::parse_runner_id, help = RUNNER_HELP)]
    runner_id: String,

    #[command(flatten)]
    confirmation: Confirmation<Self>,

    #[command(flatten)]
    options: Options,
}

/// Who can use the grant once the runner is authorized for it (REQ-GRANT-66): an OAuth
/// grant's tokens are issued only to the runs its owner started (REQ-GRANT-10), while a native
/// grant's login is the runner's to every run and step (REQ-GRANT-35).
impl Stated for Command {
    const COMMAND: &'static str = "um grant authorize";
    type Usage = Self;
    const STATEMENT: Statement = Statement::ByKind {
        oauth: &["every run on the runner that you start can be issued tokens from the grant"],
        native: NATIVE_AUTHORIZE,
    };
    const ACTION: &'static str = "to authorize the runner";
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
        let Some(grant) = &self.grant else {
            return self.run_all(deployment);
        };
        let runner_id = &self.runner_id;
        let key = um_support::generate_idempotency_key()
            .context("generate grant authorization request key")?;
        act_on::<Self, _>(
            deployment,
            &self.options,
            grant,
            |grant| format!("Authorizing runner {runner_id} for grant {}:", grant.id),
            |api, grant| replayed(|| api.authorize(&grant.id, runner_id, &key)),
            |api_url, grant, authorization, json| {
                write_authorized(
                    api_url,
                    Holder::Grant(&grant.id),
                    AuthorizationView::made(&authorization),
                    json,
                )
            },
            |id| format!("um grant authorize {id} {runner_id} --yes"),
        )
    }

    /// Authorizes the runner for every unrevoked grant of the member that can serve it: each
    /// tried in turn, since only Cloud knows which organization the runner is in.
    fn run_all(&self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let runner_id = &self.runner_id;
        let json = self.options.json;
        let api_url = deployment.fingerprint().api_url();
        confirmation::state(
            format_args!(
                "Authorizing runner {runner_id} for every grant of yours that can serve it:"
            ),
            &Self::STATEMENT.every_line(),
        )?;
        let grants = match super::with_api(deployment, &self.options, |api| {
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
        // One key for each grant's request, generated before anything is sent and kept for
        // the replay of a lost answer and of a refreshed session.
        let keys = grants
            .iter()
            .map(|_| um_support::generate_idempotency_key())
            .collect::<Result<Vec<_>, _>>()
            .context("generate grant authorization request keys")?;
        // A failure other than a refusal of one grant ends the command at that grant, keeping
        // the outcomes of the grants before it to report.
        let mut outcomes = Vec::with_capacity(grants.len());
        let ended = super::with_api(deployment, &self.options, |api| {
            // A refreshed session replays every grant with its key, so the outcomes restart.
            outcomes.clear();
            for (grant, key) in grants.iter().zip(&keys) {
                outcomes.push(authorize_one(api, grant, runner_id, key)?);
            }
            Ok(())
        })?
        .err();
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
    match replayed(|| api.authorize(&grant.id, runner_id, key)) {
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
