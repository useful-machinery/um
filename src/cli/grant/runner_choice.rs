//! What `um grant authorize` and `um grant deauthorize` offer when given no `RUNNER`
//! (REQ-GRANT-81): the runners they read, in the order they offer them, and the outcome when
//! there is none to offer; and the runner they act on, named by `RUNNER` or chosen from the
//! list where the command can prompt (REQ-GRANT-82, INV-CLI-16), or all of them chosen as
//! one (REQ-GRANT-83).

use std::collections::BTreeSet;

use anyhow::Context;

use crate::exit_code::ExitCode;
use um_api::{
    MemberGrant, MemberGrantFailure, MemberGrantKind, MemberGrantProvider, RunnerFailure,
    RunnerRegistration,
};
use um_human_auth::Deployment;

use super::super::PrincipalAuthenticationArgs;
use super::runner_picker::{self, Picked};
use super::{Options, confirmation, every_page, organization, output, with_api};

// The largest page the runner registration list serves.
const PAGE: u16 = 200;

/// A runner a command offers: its registration, or, for an authorization whose runner the
/// command found no registration of, its ID and organization alone.
pub(super) struct Runner {
    pub(super) id: String,
    pub(super) organization_id: String,
    pub(super) registration: Option<Registration>,
}

/// What a runner's registration says of it, each state by its wire name.
pub(super) struct Registration {
    pub(super) name: String,
    pub(super) pool: String,
    /// `online` or `offline`.
    pub(super) connectivity: String,
    /// `enabled`, `draining` or `disabled`.
    pub(super) mode: String,
}

impl Runner {
    fn registered(registration: RunnerRegistration) -> anyhow::Result<Self> {
        // Each state by its wire name, as the runner registrations carry it.
        let state = |name: Option<String>| name.context("runner state is not a contracted string");
        Ok(Self {
            registration: Some(Registration {
                connectivity: state(um_support::wire_name(&registration.connectivity.state))?,
                mode: state(um_support::wire_name(&registration.administration.mode))?,
                name: registration.name,
                pool: registration.runner_pool.name,
            }),
            id: registration.id,
            organization_id: registration.organization_id,
        })
    }

    /// The runner as a command names it among those it offers: its name, pool, `online` or
    /// `offline`, its mode when that is not `enabled`, its ID, and with `organization` its
    /// organization's ID. A runner with no registration is named by its ID alone.
    pub(super) fn line(&self, organization: bool) -> String {
        let registration = self.registration.as_ref();
        let mode = registration
            .map(|registration| registration.mode.as_str())
            .filter(|mode| *mode != "enabled");
        [
            registration.map(|registration| registration.name.as_str()),
            registration.map(|registration| registration.pool.as_str()),
            registration.map(|registration| registration.connectivity.as_str()),
            mode,
            Some(self.id.as_str()),
            organization.then_some(self.organization_id.as_str()),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" · ")
    }

    /// The runner as a statement or an outcome names it: by its name and ID, or by its ID
    /// alone when it has no registration.
    fn named(&self) -> String {
        match &self.registration {
            Some(registration) => format!("{} ({})", registration.name, self.id),
            None => self.id.clone(),
        }
    }

    /// Whether the runner is online and enabled, which a command offers first.
    fn ready(&self) -> bool {
        self.registration.as_ref().is_some_and(|registration| {
            registration.connectivity == "online" && registration.mode == "enabled"
        })
    }
}

/// Why a command has no runner to offer.
#[derive(Clone, Copy)]
pub(super) enum Nothing {
    /// The organizations it reads hold no runner.
    RunnerNotFound,
    /// The grant is authorized for every runner of those organizations.
    AlreadyAuthorized,
    /// A Codex grant, which serves one runner at a time (REQ-GRANT-59), has an authorization in
    /// force.
    AuthorizedElsewhere,
    /// The grant is authorized for no runner.
    AuthorizationNotFound,
}

/// What a command offers: the runners in order, why there is none, or the failure of a read.
pub(super) enum Offer {
    Runners(Vec<Runner>),
    Nothing(Nothing),
    GrantFailure(MemberGrantFailure),
    RunnerFailure(RunnerFailure),
}

/// The runners `authorize GRANT` offers: those of the grant's organization for a native grant,
/// of every organization the member is an active member of for an OAuth grant, less those the
/// grant is authorized for already. A Codex grant with an authorization in force is offered
/// none, and nothing is read.
pub(super) fn to_authorize(
    deployment: &Deployment,
    options: &Options,
    grant: &MemberGrant,
) -> anyhow::Result<Offer> {
    if grant.provider == MemberGrantProvider::GrantProviderCodex && !grant.authorizations.is_empty()
    {
        return Ok(Offer::Nothing(Nothing::AuthorizedElsewhere));
    }
    let organizations = match grant.kind {
        MemberGrantKind::GrantKindNative => grant.organization_id.iter().cloned().collect(),
        MemberGrantKind::GrantKindOAuth => match active_organizations(deployment, options)? {
            Ok(organizations) => organizations,
            Err(failure) => return Ok(Offer::GrantFailure(failure)),
        },
    };
    Ok(match registered(deployment, options, &organizations)? {
        Err(failure) => Offer::RunnerFailure(failure),
        Ok(runners) if runners.is_empty() => Offer::Nothing(Nothing::RunnerNotFound),
        Ok(runners) => offered(
            runners
                .into_iter()
                .filter(|runner| {
                    !grant
                        .authorizations
                        .iter()
                        .any(|authorization| authorization.runner_id == runner.id)
                })
                .collect(),
            Nothing::AlreadyAuthorized,
        ),
    })
}

/// The runners `authorize --all` offers: those of every organization the member is an active
/// member of.
pub(super) fn to_authorize_all(
    deployment: &Deployment,
    options: &Options,
) -> anyhow::Result<Offer> {
    let organizations = match active_organizations(deployment, options)? {
        Ok(organizations) => organizations,
        Err(failure) => return Ok(Offer::GrantFailure(failure)),
    };
    Ok(match registered(deployment, options, &organizations)? {
        Err(failure) => Offer::RunnerFailure(failure),
        Ok(runners) => offered(runners, Nothing::RunnerNotFound),
    })
}

/// The runners `deauthorize` offers: those the grant is authorized for, each with the
/// registration read from its authorization's organization, or by its ID alone when there is
/// none. A grant authorized for no runner is offered none, and nothing is read.
pub(super) fn to_deauthorize(
    deployment: &Deployment,
    options: &Options,
    grant: &MemberGrant,
) -> anyhow::Result<Offer> {
    if grant.authorizations.is_empty() {
        return Ok(Offer::Nothing(Nothing::AuthorizationNotFound));
    }
    let organizations = grant
        .authorizations
        .iter()
        .map(|authorization| authorization.organization_id.clone())
        .collect();
    let mut registered = match registered(deployment, options, &organizations)? {
        Ok(registered) => registered,
        Err(failure) => return Ok(Offer::RunnerFailure(failure)),
    };
    let runners = grant
        .authorizations
        .iter()
        .map(|authorization| {
            match registered
                .iter()
                .position(|runner| runner.id == authorization.runner_id)
            {
                Some(index) => registered.swap_remove(index),
                None => Runner {
                    id: authorization.runner_id.clone(),
                    organization_id: authorization.organization_id.clone(),
                    registration: None,
                },
            }
        })
        .collect();
    Ok(offered(runners, Nothing::AuthorizationNotFound))
}

/// The runner a leaf acts on: the one `RUNNER` names, or one chosen from the list.
pub(super) struct Acting {
    pub(super) id: String,
    /// The runner as the statement names it: by its ID, or, chosen from the list, by its name
    /// and ID.
    pub(super) named: String,
    /// Whether the leaf asks before it acts: a runner chosen from the list without `--yes`.
    pub(super) asks: bool,
}

impl Acting {
    /// The runner `RUNNER` names, which the leaf acts on without asking: `--yes` was required
    /// to name it.
    pub(super) fn named(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            named: id.to_owned(),
            asks: false,
        }
    }

    fn chosen(runner: Runner, yes: bool) -> Self {
        Self {
            named: runner.named(),
            id: runner.id,
            asks: !yes,
        }
    }
}

/// A runner a leaf acts on among all those it offered: its ID, and how its outcome names it.
pub(super) struct Each {
    pub(super) id: String,
    pub(super) named: String,
}

/// Every runner a leaf offered, in the order offered, chosen as one (REQ-GRANT-83).
pub(super) struct All {
    pub(super) runners: Vec<Each>,
    /// Whether the leaf asks before it acts, as for one runner chosen.
    pub(super) asks: bool,
}

/// What a leaf that offers all its runners as one choice acts on (REQ-GRANT-83).
pub(super) enum Chosen {
    One(Acting),
    All(All),
}

/// The runner a command given no `RUNNER` acts on (REQ-GRANT-81, REQ-GRANT-82): where it can
/// prompt, standard input and standard error both terminals and no `--json`, the one chosen
/// from the list of `offer`'s runners, which shows `purpose`; otherwise, or with no runner to
/// offer, the refusal [`refuse`] reports, and the command exits with its status.
pub(super) fn choose(
    deployment: &Deployment,
    json: bool,
    yes: bool,
    offer: Offer,
    grant_id: Option<&str>,
    purpose: &str,
    example: impl FnOnce(&str) -> String,
) -> anyhow::Result<Result<Acting, ExitCode>> {
    let pick = |message: &str, runners| {
        Ok(runner_picker::pick(message, runners)?.map(|runner| Acting::chosen(runner, yes)))
    };
    choose_with(deployment, json, offer, grant_id, purpose, example, pick)
}

/// What a command given no `RUNNER` acts on as [`choose`] says, where the list also offers
/// every runner as one choice when it shows more than one (REQ-GRANT-83).
pub(super) fn choose_one_or_all(
    deployment: &Deployment,
    json: bool,
    yes: bool,
    offer: Offer,
    grant_id: Option<&str>,
    purpose: &str,
    example: impl FnOnce(&str) -> String,
) -> anyhow::Result<Result<Chosen, ExitCode>> {
    let pick = |message: &str, runners| {
        Ok(
            runner_picker::pick_one_or_all(message, runners)?.map(|picked| match picked {
                Picked::One(runner) => Chosen::One(Acting::chosen(runner, yes)),
                Picked::All(runners) => Chosen::All(All {
                    runners: runners
                        .into_iter()
                        .map(|runner| Each {
                            named: runner.named(),
                            id: runner.id,
                        })
                        .collect(),
                    asks: !yes,
                }),
            }),
        )
    };
    choose_with(deployment, json, offer, grant_id, purpose, example, pick)
}

/// Where the command can prompt, what `pick` yields from the list of `offer`'s runners, which
/// shows `purpose`; otherwise, or with no runner to offer, the refusal [`refuse`] reports.
fn choose_with<T>(
    deployment: &Deployment,
    json: bool,
    offer: Offer,
    grant_id: Option<&str>,
    purpose: &str,
    example: impl FnOnce(&str) -> String,
    pick: impl FnOnce(&str, Vec<Runner>) -> anyhow::Result<Result<T, ExitCode>>,
) -> anyhow::Result<Result<T, ExitCode>> {
    match offer {
        Offer::Runners(runners) if !json && confirmation::can_ask() => {
            pick(&format!("Runner {purpose}:"), runners)
        }
        offer => refuse(deployment, json, offer, grant_id, purpose, example).map(Err),
    }
}

/// Refuses a command given no `RUNNER` (REQ-GRANT-81): with exit 2, naming each runner offered
/// under a diagnostic that names the runner `purpose` asks for and ending with the command line
/// `example` makes for one runner ID, or with the outcome of having none to offer or of a
/// failed read.
fn refuse(
    deployment: &Deployment,
    json: bool,
    offer: Offer,
    grant_id: Option<&str>,
    purpose: &str,
    example: impl FnOnce(&str) -> String,
) -> anyhow::Result<ExitCode> {
    let api_url = deployment.fingerprint().api_url();
    match offer {
        Offer::Runners(runners) => {
            let example = example(runners.first().map_or("RUNNER", |runner| &runner.id));
            let diagnostic = format!("name the runner {purpose}");
            output::write_runner_required(api_url, grant_id, &runners, &diagnostic, &example, json)
        }
        Offer::Nothing(nothing) => output::write_nothing_offered(api_url, nothing, json),
        Offer::GrantFailure(failure) => output::failure(api_url, &failure, json),
        Offer::RunnerFailure(failure) => super::super::runner::write_failure(
            deployment,
            &failure,
            &PrincipalAuthenticationArgs::default(),
            json,
        ),
    }
}

/// `runners` in the order a command offers them, or `nothing` when there are none: the runners
/// online and enabled first, then the rest, each group by name, then ID.
fn offered(mut runners: Vec<Runner>, nothing: Nothing) -> Offer {
    if runners.is_empty() {
        return Offer::Nothing(nothing);
    }
    let key = |runner: &Runner| {
        let name = runner
            .registration
            .as_ref()
            .map(|registration| registration.name.clone());
        (!runner.ready(), name.is_none(), name, runner.id.clone())
    };
    runners.sort_by_cached_key(key);
    Offer::Runners(runners)
}

/// The organizations the member is an active member of, by ID.
fn active_organizations(
    deployment: &Deployment,
    options: &Options,
) -> anyhow::Result<Result<BTreeSet<String>, MemberGrantFailure>> {
    with_api(deployment, options, |api| {
        let organizations = organization::active(api)?;
        Ok(organizations.into_iter().map(|(id, _)| id).collect())
    })
}

/// Every runner registered in `organizations`, each read to its last page with the member's
/// human session (REQ-RUNNERPOOL-38).
fn registered(
    deployment: &Deployment,
    options: &Options,
    organizations: &BTreeSet<String>,
) -> anyhow::Result<Result<Vec<Runner>, RunnerFailure>> {
    let read = super::super::runner::with_api(
        deployment,
        options.http.transport_policy(),
        &PrincipalAuthenticationArgs::default(),
        |api| {
            let mut registrations = Vec::new();
            for organization in organizations {
                registrations.extend(every_page(RunnerFailure::Protocol, |cursor| {
                    api.list_registrations(organization, Some(PAGE), cursor)
                        .map(|page| (page.items, page.next_cursor))
                })?);
            }
            Ok(registrations)
        },
    )?;
    match read {
        Ok(registrations) => registrations
            .into_iter()
            .map(Runner::registered)
            .collect::<anyhow::Result<_>>()
            .map(Ok),
        Err(failure) => Ok(Err(failure)),
    }
}
