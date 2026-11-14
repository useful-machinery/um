use std::collections::BTreeMap;
use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use super::super::ApiFailureReport;
use super::authorization::{
    AuthorizationView, EndedAuthorizationView, ended_authorization_views, write_revoked_runners,
    write_session_note,
};
use super::authorize::{MOVE_CODEX_GRANT, Outcome};
use super::projection::{MemberStatus, PROVIDERS, grant_authorization_status, grant_subject};
use super::runner_choice::{Nothing, Runner};
use crate::exit_code::{ExitCode, OutcomeClass};
use um_api::{MemberGrant, MemberGrantFailure, MemberGrantKind};

/// The names the member's other reads give a grant's account: each linked identity's, and
/// each organization's slug, by ID.
pub(super) struct Accounts {
    pub(super) identities: BTreeMap<String, String>,
    pub(super) organizations: BTreeMap<String, String>,
}

/// Whose account a grant acts as: an OAuth grant's linked identity, or the organization a
/// native grant was imported for, which is all Cloud knows of a coding agent's account.
#[derive(Serialize)]
struct AccountView<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
}

impl<'a> AccountView<'a> {
    fn new(grant: &'a MemberGrant, accounts: &'a Accounts) -> Self {
        let (kind, id, names) = match grant.kind {
            MemberGrantKind::GrantKindOAuth => (
                "identity",
                grant.identity_id.as_deref(),
                &accounts.identities,
            ),
            MemberGrantKind::GrantKindNative => (
                "organization",
                grant.organization_id.as_deref(),
                &accounts.organizations,
            ),
        };
        // The client checks that a grant carries the account its kind names.
        let id = id.unwrap_or_default();
        Self {
            kind,
            id,
            name: names.get(id).map(String::as_str),
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        match self.name {
            Some(name) => writeln!(output, "account: {name} ({} {})", self.kind, self.id),
            None => writeln!(output, "account: {} {}", self.kind, self.id),
        }
    }
}

/// A grant as `um grant list` presents it (REQ-GRANT-64): the model's named fields only, enums
/// as their wire strings, so nothing a response adds beyond the model reaches output
/// (REQ-GRANT-4, REQ-GRANT-14). The members after `status` are those `list GRANT` adds.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GrantView<'a> {
    id: &'a str,
    provider: &'static str,
    kind: &'static str,
    account: AccountView<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<&'a str>,
    state: &'static str,
    generation: i64,
    created_at: &'a str,
    authorizations: Vec<AuthorizationView<'a>>,
    // The grant's own availability and next action when no runner is authorized for it.
    #[serde(flatten)]
    status: Option<MemberStatus>,
    #[serde(flatten)]
    detail: Option<DetailView<'a>>,
}

/// What `um grant list GRANT` adds for each kind: an OAuth grant's scopes, last use and
/// expiries; a native grant's record, import and ended authorizations.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DetailView<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    scopes: Option<&'a [String]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_used_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_expires_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_expires_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_schema_version: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key_version: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    imported_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revoked_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ended_authorizations: Option<Vec<EndedAuthorizationView<'a>>>,
}

impl<'a> DetailView<'a> {
    fn new(grant: &'a MemberGrant) -> Self {
        Self {
            scopes: grant.scopes.as_deref(),
            last_used_at: grant.last_used_at.as_deref(),
            access_expires_at: grant.access_expires_at.as_deref(),
            refresh_expires_at: grant.refresh_expires_at.as_deref(),
            record_schema_version: grant.record_schema_version,
            key_version: grant.key_version,
            imported_at: grant.imported_at.as_deref(),
            revoked_at: grant.revoked_at.as_deref(),
            ended_authorizations: grant
                .ended_authorizations
                .as_deref()
                .map(ended_authorization_views),
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        match self.scopes {
            Some([]) => writeln!(output, "scopes: none")?,
            Some(scopes) => writeln!(output, "scopes: {}", scopes.join(", "))?,
            None => {}
        }
        for (key, value) in [
            ("last used", self.last_used_at),
            ("access expires", self.access_expires_at),
            ("refresh expires", self.refresh_expires_at),
            ("imported", self.imported_at),
            ("revoked", self.revoked_at),
        ] {
            if let Some(value) = value {
                writeln!(output, "{key}: {value}")?;
            }
        }
        Ok(())
    }
}

impl<'a> GrantView<'a> {
    /// The grant with its authorizations in force; with `detail`, what `list GRANT` adds.
    fn new(grant: &'a MemberGrant, accounts: &'a Accounts, detail: bool) -> Self {
        let subject = grant_subject(grant);
        // A native grant's exchange with each runner is part of its detail; an OAuth grant
        // has none.
        let exchange = detail && grant.kind == MemberGrantKind::GrantKindNative;
        Self {
            id: &grant.id,
            provider: grant.provider.as_str(),
            kind: grant.kind.as_str(),
            account: AccountView::new(grant, accounts),
            label: grant.label.as_deref(),
            state: grant.state.as_str(),
            generation: grant.generation,
            created_at: &grant.created_at,
            authorizations: grant
                .authorizations
                .iter()
                .map(|authorization| {
                    AuthorizationView::of_grant(
                        authorization,
                        MemberStatus::new(
                            grant_authorization_status(grant, Some(authorization)),
                            subject,
                        ),
                        exchange,
                    )
                })
                .collect(),
            status: grant
                .authorizations
                .is_empty()
                .then(|| MemberStatus::new(grant_authorization_status(grant, None), subject)),
            detail: detail.then(|| DetailView::new(grant)),
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "grant: {}", self.id)?;
        writeln!(output, "provider: {}", self.provider)?;
        writeln!(output, "kind: {}", self.kind)?;
        self.account.write_human(output)?;
        if let Some(label) = self.label {
            writeln!(output, "label: {label}")?;
        }
        writeln!(output, "state: {}", self.state)?;
        writeln!(output, "generation: {}", self.generation)?;
        writeln!(output, "created: {}", self.created_at)?;
        if let Some(detail) = &self.detail {
            detail.write_human(output)?;
        }
        if self.authorizations.is_empty() {
            writeln!(output, "authorizations: none")?;
        } else {
            writeln!(output, "authorizations: {}", self.authorizations.len())?;
        }
        for authorization in &self.authorizations {
            authorization.write_human(output)?;
        }
        if let Some(status) = &self.status {
            status.write_human(output, "")?;
        }
        let ended = self
            .detail
            .as_ref()
            .and_then(|detail| detail.ended_authorizations.as_deref())
            .unwrap_or_default();
        if !ended.is_empty() {
            writeln!(output, "ended authorizations: {}", ended.len())?;
        }
        for authorization in ended {
            authorization.write_human(output)?;
        }
        Ok(())
    }
}

/// A provider the member holds no unrevoked grant of, with the status that names how to make
/// one.
#[derive(Serialize)]
struct NotConnectedView {
    provider: &'static str,
    #[serde(flatten)]
    status: MemberStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    grants: &'a [GrantView<'a>],
    not_connected: &'a [NotConnectedView],
}

/// Writes the member's unrevoked grants, then each provider they hold none of
/// (REQ-GRANT-64); it exits 0 whatever they report.
pub(super) fn write_list(
    deployment: &str,
    grants: &[MemberGrant],
    accounts: &Accounts,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let views: Vec<_> = grants
        .iter()
        .map(|grant| GrantView::new(grant, accounts, false))
        .collect();
    let not_connected: Vec<_> = PROVIDERS
        .iter()
        .filter(|provider| {
            !grants
                .iter()
                .any(|grant| grant.provider.as_str() == provider.name())
        })
        .map(|provider| NotConnectedView {
            provider: provider.name(),
            status: provider.not_connected(),
        })
        .collect();
    if json {
        write_json(&ListResult {
            schema_version: 1,
            deployment,
            outcome: "listed",
            grants: &views,
            not_connected: &not_connected,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Grants listed.")?;
    writeln!(stdout, "deployment: {deployment}")?;
    writeln!(stdout, "count: {}", views.len())?;
    for view in &views {
        writeln!(stdout, "\n── grant ──")?;
        view.write_human(&mut stdout)?;
    }
    if !not_connected.is_empty() {
        writeln!(stdout, "\n── not connected ──")?;
    }
    for provider in &not_connected {
        writeln!(stdout, "{}", provider.provider)?;
        provider.status.write_human(&mut stdout, "  ")?;
    }
    stdout.flush()?;
    Ok(ExitCode::Success)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GrantResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    grant: GrantView<'a>,
}

/// Writes one grant in detail (REQ-GRANT-64).
pub(super) fn write_grant(
    deployment: &str,
    grant: &MemberGrant,
    accounts: &Accounts,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let view = GrantView::new(grant, accounts, true);
    if json {
        write_json(&GrantResult {
            schema_version: 1,
            deployment,
            outcome: "shown",
            grant: view,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Grant shown.")?;
    view.write_human(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AmbiguousResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    provider: &'a str,
    grant_ids: &'a [String],
}

/// Refuses a provider name that names several unrevoked grants, naming them (REQ-GRANT-65):
/// a command does not choose between them.
pub(super) fn write_ambiguous(
    deployment: &str,
    provider: &str,
    ids: &[String],
    example: impl FnOnce(&str) -> String,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&AmbiguousResult {
            schema_version: 1,
            deployment,
            outcome: "grant_ambiguous",
            provider,
            grant_ids: ids,
        })?;
    } else {
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "error: you hold {} grants of {provider}: {}\n\nName one by its ID, for example:\n  {}",
            ids.len(),
            ids.join(", "),
            example(ids.first().map_or("GRANT", String::as_str)),
        )?;
    }
    Ok(ExitCode::UsageError)
}

/// A runner a command given no `RUNNER` offers (REQ-GRANT-81); a runner it found no
/// registration of is named by its ID alone.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OfferedRunnerView<'a> {
    id: &'a str,
    name: Option<&'a str>,
    pool: Option<&'a str>,
    connectivity: Option<&'a str>,
    mode: Option<&'a str>,
    organization_id: &'a str,
}

impl<'a> OfferedRunnerView<'a> {
    fn new(runner: &'a Runner) -> Self {
        let registration = runner.registration.as_ref();
        Self {
            id: &runner.id,
            name: registration.map(|registration| registration.name.as_str()),
            pool: registration.map(|registration| registration.pool.as_str()),
            connectivity: registration.map(|registration| registration.connectivity.as_str()),
            mode: registration.map(|registration| registration.mode.as_str()),
            organization_id: &runner.organization_id,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunnerRequiredResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    grant_id: Option<&'a str>,
    runners: Vec<OfferedRunnerView<'a>>,
}

/// Refuses a command given no `RUNNER` where it cannot prompt as a missing argument is refused
/// (REQ-GRANT-81): exit 2, naming each runner offered by its name and ID under `diagnostic`,
/// then the command line `example` that names one; under `--json`, one document with outcome
/// `runner_required`.
pub(super) fn write_runner_required(
    deployment: &str,
    grant_id: Option<&str>,
    runners: &[Runner],
    diagnostic: &str,
    example: &str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&RunnerRequiredResult {
            schema_version: 1,
            deployment,
            outcome: "runner_required",
            grant_id,
            runners: runners.iter().map(OfferedRunnerView::new).collect(),
        })?;
    } else {
        let mut stderr = io::stderr().lock();
        writeln!(stderr, "error: {diagnostic}, one of:")?;
        for runner in runners {
            writeln!(stderr, "  {}", runner.line(false))?;
        }
        writeln!(stderr, "\nName one by its ID, for example:\n  {example}")?;
    }
    Ok(ExitCode::UsageError)
}

/// Reports that a command given no `RUNNER` has none to offer (REQ-GRANT-81), with exit 1 and
/// a remedy.
pub(super) fn write_nothing_offered(
    deployment: &str,
    nothing: Nothing,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let refused = |outcome, summary: &str, remedy: &str| {
        ApiFailureReport::new(outcome, summary, remedy, OutcomeClass::GeneralFailure)
    };
    match nothing {
        Nothing::RunnerNotFound => refused(
            "runner_not_found",
            "error: no runner found in the organizations the command can act in",
            "Register a runner in the organization, then run the command again. List an organization's runners:\n  um runner list ORGANIZATION",
        ),
        Nothing::AlreadyAuthorized => refused(
            "already_authorized",
            "error: this grant is already authorized for every runner it can serve",
            "Show the grant's authorizations:\n  um grant list GRANT",
        ),
        Nothing::AuthorizedElsewhere => authorized_elsewhere(),
        Nothing::AuthorizationNotFound => refused(
            "authorization_not_found",
            "error: this grant is authorized for no runner",
            "Authorize a runner for it first:\n  um grant authorize GRANT",
        ),
    }
    .write(deployment, json)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RevokeResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    grant_id: &'a str,
    provider: &'static str,
    kind: &'static str,
    // A native grant's, absent when the read after the revocation failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    ended_authorizations: Option<Vec<EndedAuthorizationView<'a>>>,
}

/// Reports a revocation (REQ-GRANT-68). For a native grant, `after` is the grant read after
/// it, `Some(None)` when that read failed, which leaves the revocation done: each runner the
/// grant was authorized for with its removal state and stop, and what neither covers.
pub(super) fn write_revoke(
    deployment: &str,
    grant: &MemberGrant,
    after: Option<Option<&MemberGrant>>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let ended = after
        .flatten()
        .and_then(|after| after.ended_authorizations.as_deref())
        .map(ended_authorization_views);
    if json {
        write_json(&RevokeResult {
            schema_version: 1,
            deployment,
            outcome: "revoked",
            grant_id: &grant.id,
            provider: grant.provider.as_str(),
            kind: grant.kind.as_str(),
            ended_authorizations: ended,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Grant revoked.")?;
    writeln!(stdout, "grant: {}", grant.id)?;
    writeln!(stdout, "provider: {}", grant.provider.as_str())?;
    writeln!(stdout, "kind: {}", grant.kind.as_str())?;
    writeln!(stdout, "deployment: {deployment}")?;
    if let Some(after) = after {
        match ended.as_deref() {
            Some(ended) if !ended.is_empty() => write_revoked_runners(&mut stdout, ended)?,
            None if after.is_none() => writeln!(
                stdout,
                "\nTo see which runners have confirmed removing it:\n  um grant list {}",
                grant.id
            )?,
            _ => {}
        }
        write_session_note(&mut stdout)?;
    }
    stdout.flush()?;
    Ok(ExitCode::Success)
}

/// One grant's outcome under `authorize --all`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizedGrantView<'a> {
    grant_id: &'a str,
    provider: &'static str,
    kind: &'static str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    authorization: Option<AuthorizationView<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizeAllResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    runner_id: &'a str,
    grants: &'a [AuthorizedGrantView<'a>],
}

/// Reports what `authorize --all` did with each grant (REQ-GRANT-66), and each refusal with
/// its remedy. It exits 1 when any grant was refused for a reason other than serving another
/// organization or being authorized for the runner already, a Codex grant authorized for
/// another runner among them, or when a failure `ended` the command: then
/// `outcomes` are those of the grants before the one it ended at, which follow them in
/// `grants`, and the failure is reported after them.
pub(super) fn write_authorize_all(
    deployment: &str,
    runner_id: &str,
    grants: &[MemberGrant],
    outcomes: &[Outcome],
    ended: Option<&MemberGrantFailure>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let refusals: Vec<_> = grants
        .iter()
        .zip(outcomes)
        .filter_map(|(grant, outcome)| match outcome {
            Outcome::AuthorizedElsewhere => Some((grant, authorized_elsewhere())),
            Outcome::Refused(failure) => Some((grant, report(deployment, failure))),
            _ => None,
        })
        .collect();
    let views: Vec<_> = grants
        .iter()
        .zip(outcomes)
        .map(|(grant, outcome)| AuthorizedGrantView {
            grant_id: &grant.id,
            provider: grant.provider.as_str(),
            kind: grant.kind.as_str(),
            outcome: match outcome {
                Outcome::Authorized(_) => "authorized",
                Outcome::AlreadyAuthorized => "already_authorized",
                Outcome::AuthorizedElsewhere => authorized_elsewhere().outcome,
                Outcome::OtherOrganization => "other_organization",
                Outcome::Refused(failure) => report(deployment, failure).outcome,
            },
            authorization: match outcome {
                Outcome::Authorized(authorization) => Some(AuthorizationView::made(authorization)),
                _ => None,
            },
        })
        .collect();
    let ended = ended.map(|failure| (grants.get(outcomes.len()), report(deployment, failure)));
    let (outcome, exit) = if refusals.is_empty() && ended.is_none() {
        ("authorized", ExitCode::Success)
    } else {
        ("refused", ExitCode::GeneralFailure)
    };
    if json {
        write_json(&AuthorizeAllResult {
            schema_version: 1,
            deployment,
            outcome,
            runner_id,
            grants: &views,
        })?;
        if let Some((_, report)) = &ended {
            // The command's exit status is 1 whatever the failure's own would be.
            report.write(deployment, json)?;
        }
        return Ok(exit);
    }
    let mut stdout = io::stdout().lock();
    if ended.is_some() {
        writeln!(
            stdout,
            "Runner not authorized for every grant: a failure ended the command."
        )?;
    } else if refusals.is_empty() {
        writeln!(stdout, "✓ Runner authorized for your grants.")?;
    } else {
        writeln!(
            stdout,
            "Runner not authorized for every grant: some refused it."
        )?;
    }
    writeln!(stdout, "runner: {runner_id}")?;
    writeln!(stdout, "deployment: {deployment}")?;
    writeln!(stdout)?;
    for (view, outcome) in views.iter().zip(outcomes) {
        write!(
            stdout,
            "{} {}: {}",
            view.grant_id, view.provider, view.outcome
        )?;
        match outcome {
            Outcome::Authorized(authorization) => {
                writeln!(stdout, " (authorization {})", authorization.id)?;
            }
            Outcome::OtherOrganization => {
                writeln!(
                    stdout,
                    " (a grant of an organization other than the runner's)"
                )?;
            }
            Outcome::AlreadyAuthorized | Outcome::AuthorizedElsewhere | Outcome::Refused(_) => {
                writeln!(stdout)?
            }
        }
    }
    stdout.flush()?;
    let mut stderr = io::stderr().lock();
    for (grant, report) in &refusals {
        write_grant_failure(&mut stderr, Some(*grant), report)?;
    }
    if let Some((grant, report)) = &ended {
        write_grant_failure(&mut stderr, *grant, report)?;
    }
    Ok(exit)
}

/// `authorize --all`'s refusal of a Codex grant that serves another runner (REQ-GRANT-66),
/// which Cloud answers as it answers a grant authorized for this runner already.
fn authorized_elsewhere() -> ApiFailureReport {
    ApiFailureReport::new(
        "authorized_elsewhere",
        "error: this Codex grant is authorized for another runner",
        MOVE_CODEX_GRANT,
        OutcomeClass::GeneralFailure,
    )
}

/// Writes a failure of `authorize --all` with the grant it met, then its remedy.
fn write_grant_failure(
    output: &mut impl Write,
    grant: Option<&MemberGrant>,
    report: &ApiFailureReport,
) -> io::Result<()> {
    writeln!(output, "{}", report.summary)?;
    if let Some(grant) = grant {
        writeln!(output, "grant: {} ({})", grant.id, grant.provider.as_str())?;
    }
    writeln!(output, "\n{}\n", report.remedy)
}

/// Reports, with its remedy, the refusal of the runner `named` among all a command acts on
/// (REQ-GRANT-83), or the failure that ended the command at it.
pub(super) fn write_runner_failure(
    deployment: &str,
    named: &str,
    failure: &MemberGrantFailure,
) -> io::Result<()> {
    let report = report(deployment, failure);
    let mut stderr = io::stderr().lock();
    writeln!(stderr, "{}", report.summary)?;
    writeln!(stderr, "runner: {named}")?;
    writeln!(stderr, "\n{}\n", report.remedy)
}

/// A grant operation's failure as the member reads it: each refusal of REQ-GRANT-65 with exit
/// status 1 and its remedy.
pub(super) fn report(deployment: &str, failure: &MemberGrantFailure) -> ApiFailureReport {
    const SUBJECT: &str = "grant";
    let refused = |outcome, summary: &str, remedy: &str| {
        ApiFailureReport::new(outcome, summary, remedy, OutcomeClass::GeneralFailure)
    };
    match failure {
        MemberGrantFailure::Unauthenticated => ApiFailureReport::unauthenticated(SUBJECT),
        MemberGrantFailure::Forbidden => ApiFailureReport::forbidden(SUBJECT),
        MemberGrantFailure::InvalidInput => refused(
            "invalid_input",
            "error: grant request rejected by the deployment",
            "Check the grant and runner IDs.",
        ),
        MemberGrantFailure::IdempotencyConflict => ApiFailureReport::idempotency_conflict(SUBJECT),
        MemberGrantFailure::ProviderUnavailable => refused(
            "provider_unavailable",
            "error: this deployment does not serve grants of that provider",
            "Ask the deployment operator which providers it serves grants for.",
        ),
        MemberGrantFailure::GrantNotFound => refused(
            "grant_not_found",
            "error: grant not found among your unrevoked grants",
            "List your grants:\n  um grant list",
        ),
        MemberGrantFailure::OrganizationNotFound => refused(
            "organization_not_found",
            "error: organization not found among your active memberships",
            "List your organizations:\n  um organization list",
        ),
        MemberGrantFailure::MaterialInvalid => refused(
            "material_invalid",
            "error: the deployment did not accept the login",
            "Import a Codex auth.json, or the token claude setup-token printed, unchanged.",
        ),
        MemberGrantFailure::LimitReached => refused(
            "limit_reached",
            "error: you hold the most grants of coding agents this organization allows",
            "Revoke one you no longer use:\n  um grant revoke GRANT --yes",
        ),
        MemberGrantFailure::RunnerNotFound => refused(
            "runner_not_found",
            "error: runner not found in an organization this grant can serve",
            "List the organization's runners and use a runner ID:\n  um runner list ORGANIZATION",
        ),
        MemberGrantFailure::RunnerHoldsLogin => refused(
            "runner_holds_login",
            "error: that runner already holds a login of this coding agent",
            "The owner of the login that runner holds must end its authorization first:\n  um grant deauthorize GRANT RUNNER --yes",
        ),
        MemberGrantFailure::AlreadyAuthorized => refused(
            "already_authorized",
            "error: this grant is already authorized for that runner, or, for a Codex grant, for another runner",
            MOVE_CODEX_GRANT,
        ),
        MemberGrantFailure::AuthorizationNotFound => refused(
            "authorization_not_found",
            "error: that runner holds no authorization of this grant",
            "Show the grant's authorizations:\n  um grant list GRANT",
        ),
        MemberGrantFailure::RateLimited { retry_after } => {
            ApiFailureReport::rate_limited(SUBJECT, *retry_after)
        }
        MemberGrantFailure::Unreachable { category } => {
            ApiFailureReport::unreachable(deployment, *category)
        }
        MemberGrantFailure::InvalidResponse { .. } => ApiFailureReport::invalid_response(SUBJECT),
    }
}

/// Reports a grant operation's failure with the remedy a member can act on; `um runner doctor
/// --grants` ends with it too (REQ-CREDREADY-22).
pub(in crate::cli) fn failure(
    deployment: &str,
    failure: &MemberGrantFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    report(deployment, failure).write(deployment, json)
}

/// The one event `um grant create` prints for a coding agent's login: the grant it imported
/// or replaced, or the refusal that stored nothing.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ImportEvent<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    grant: Option<GrantView<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<u64>,
}

impl<'a> ImportEvent<'a> {
    const fn new(deployment: &'a str, outcome: &'static str) -> Self {
        Self {
            schema_version: 1,
            event: "import_result",
            deployment,
            outcome,
            grant: None,
            category: None,
            retry_after: None,
        }
    }
}

/// Writes the native grant `um grant create` imported, or stored as the next generation of
/// the grant it `replaced` (REQ-GRANT-70, REQ-GRANT-73), naming its organization by `slug` when
/// that is known.
pub(super) fn write_imported(
    deployment: &str,
    grant: &MemberGrant,
    slug: Option<&str>,
    replaced: bool,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let accounts = Accounts {
        identities: BTreeMap::new(),
        organizations: grant
            .organization_id
            .iter()
            .zip(slug)
            .map(|(id, slug)| (id.clone(), slug.to_owned()))
            .collect(),
    };
    let view = GrantView::new(grant, &accounts, false);
    let outcome = if replaced { "replaced" } else { "imported" };
    if json {
        write_json_line(&ImportEvent {
            grant: Some(view),
            ..ImportEvent::new(deployment, outcome)
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Grant {outcome}.")?;
    view.write_human(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

/// Reports a refusal that stored no coding agent's login: as the command's one JSON event, or
/// on standard error with its remedy.
pub(super) fn write_import_refusal(
    deployment: &str,
    report: &ApiFailureReport,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json_line(&ImportEvent {
            category: report.category,
            retry_after: report.retry_after,
            ..ImportEvent::new(deployment, report.outcome)
        })?;
    } else {
        let mut stderr = io::stderr().lock();
        writeln!(stderr, "{}\n\n{}", report.summary, report.remedy)?;
    }
    Ok(report.class.exit_code())
}

/// The outcome of a coding agent `um grant create` without `PROVIDER` passes over because no
/// organization was resolved for its grant (REQ-GRANT-75).
pub(super) const ORGANIZATION_REQUIRED: &str = "organization_required";

/// The outcome of a provider `um grant create --workflow` passes over because every grant the
/// member holds of it needs reconnecting and its replacement was not settled (REQ-GRANT-79).
pub(super) const RECONNECT_REQUIRED: &str = "reconnect_required";

/// Reports a coding agent passed over for want of an organization: as its `import_result`
/// event, or on standard error with the `refusal` that says why and names `--organization`
/// (REQ-GRANT-75).
pub(super) fn write_organization_required(
    deployment: &str,
    refusal: &str,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        return write_json_line(&ImportEvent::new(deployment, ORGANIZATION_REQUIRED));
    }
    let mut stderr = io::stderr().lock();
    writeln!(stderr, "error: {refusal}")?;
    stderr.flush()?;
    Ok(())
}

/// Names on standard error the provider whose sign-in `um grant create` runs next, so the
/// member knows whose prompt follows (REQ-GRANT-75), and in a report parts it from the one
/// `after` another provider's sign-in printed.
pub(super) fn write_provider_heading(
    provider: &str,
    after: bool,
    json: bool,
) -> anyhow::Result<()> {
    if after && !json {
        let mut stdout = io::stdout().lock();
        writeln!(stdout)?;
        stdout.flush()?;
    }
    let mut stderr = io::stderr().lock();
    writeln!(stderr, "── {provider} ──")?;
    stderr.flush()?;
    Ok(())
}

/// One provider's entry in the summary of `um grant create` without a provider.
#[derive(Serialize)]
struct CreateOutcome {
    provider: &'static str,
    outcome: &'static str,
}

/// The event that ends `um grant create` without a provider: how the command ended and one
/// outcome per provider it signed in to, or the refusal that started none.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateSummary<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    outcome: &'static str,
    providers: Vec<CreateOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<u64>,
}

impl<'a> CreateSummary<'a> {
    const fn new(deployment: &'a str, outcome: &'static str) -> Self {
        Self {
            schema_version: 1,
            event: "summary",
            deployment,
            outcome,
            providers: Vec::new(),
            category: None,
            retry_after: None,
        }
    }
}

/// The providers `um grant create` without `PROVIDER` chose among, and the command that ran
/// the choice, as its report names them.
#[derive(Clone, Copy)]
pub(in crate::cli) enum Scope {
    /// Every provider (REQ-GRANT-75).
    Every,
    /// Those the `--workflow` files call for (REQ-GRANT-76).
    Workflows,
    /// Those the `--workflow` files call for, from `um runner doctor --connect`
    /// (REQ-CREDREADY-24).
    Doctor,
}

impl Scope {
    const fn all_held(self) -> &'static str {
        match self {
            Self::Every => "✓ You already hold a grant of each provider.",
            Self::Workflows | Self::Doctor => {
                "✓ You already hold a grant of each provider the workflows call for."
            }
        }
    }

    const fn again(self) -> &'static str {
        match self {
            Self::Every => "um grant create",
            Self::Workflows => "um grant create --workflow FILE",
            Self::Doctor => "um runner doctor --workflow FILE --connect",
        }
    }

    pub(super) const fn organization(self) -> &'static str {
        match self {
            Self::Every => "um grant create --organization ORGANIZATION",
            Self::Workflows => "um grant create --workflow FILE --organization ORGANIZATION",
            Self::Doctor => {
                "um runner doctor --workflow FILE --connect --organization-id ORGANIZATION"
            }
        }
    }
}

/// What the member does about a coding agent whose sign-in failed, besides what its own
/// failure printed: the agent's program missing from `PATH` is the failure no retry mends.
fn native_remedy(provider: &str) -> Option<&'static str> {
    match provider {
        "codex" => Some(
            "When codex is not on your PATH, install Codex so that it is, or import an auth.json:\n  um grant create codex --from-file PATH",
        ),
        "claude_code" => Some(
            "When claude is not on your PATH, install Claude Code so that it is, or import the token claude setup-token printed:\n  um grant create claude_code --token-file PATH",
        ),
        _ => None,
    }
}

/// Prints one outcome per provider `um grant create` signed in to or passed over, after their
/// own output, as the last JSON event or a report on standard output; `code` is the command's
/// exit status (REQ-GRANT-75). The report's verdict claims each grant made only when every
/// provider ended with one, created or `replaced`, and its remedies name what each other
/// outcome present leaves to do, `um grant create PROVIDER --replace` for each provider passed
/// over as `reconnect_required` (REQ-GRANT-79).
pub(super) fn write_create_summary(
    deployment: &str,
    outcomes: &[(&'static str, &'static str)],
    code: ExitCode,
    scope: Scope,
    json: bool,
) -> anyhow::Result<()> {
    let outcome = match code {
        _ if outcomes.is_empty() => "nothing_to_create",
        ExitCode::Success => "completed",
        ExitCode::Interrupted => "interrupted",
        ExitCode::Terminated => "terminated",
        _ => "failed",
    };
    if json {
        return write_json_line(&CreateSummary {
            providers: outcomes
                .iter()
                .map(|&(provider, outcome)| CreateOutcome { provider, outcome })
                .collect(),
            ..CreateSummary::new(deployment, outcome)
        });
    }
    let mut stdout = io::stdout().lock();
    if outcome == "nothing_to_create" {
        writeln!(stdout, "{}", scope.all_held())?;
        writeln!(stdout, "deployment: {deployment}")?;
        return stdout.flush().map_err(Into::into);
    }
    let with = |wanted: &str| -> Vec<&'static str> {
        outcomes
            .iter()
            .filter(|(_, outcome)| *outcome == wanted)
            .map(|(provider, _)| *provider)
            .collect()
    };
    let started = with("started");
    let unavailable = with("provider_unavailable");
    let organization = with(ORGANIZATION_REQUIRED);
    let reconnect = with(RECONNECT_REQUIRED);
    let failed = with("failed");
    let made = |kinds: &[&str]| outcomes.iter().all(|(_, outcome)| kinds.contains(outcome));
    let verdict = match outcome {
        "completed" if made(&["created"]) => "✓ Signed in to each provider you lacked.",
        "completed" if made(&["created", "replaced"]) => {
            "✓ Signed in to each provider you lacked or reconnected."
        }
        "completed" => "! Not every provider you lacked has a grant yet.",
        "interrupted" | "terminated" => "✗ Stopped before each sign-in ended.",
        _ if failed.is_empty() => "✗ No grant made: each provider was passed over.",
        _ => "✗ Not every sign-in ended with a grant.",
    };
    writeln!(stdout, "\n{verdict}")?;
    for (provider, outcome) in outcomes {
        writeln!(stdout, "{provider}: {outcome}")?;
    }
    writeln!(stdout, "deployment: {deployment}")?;
    if !started.is_empty() {
        writeln!(
            stdout,
            "\nStarted, not yet approved: {}. Approve each at the URL printed for it to make its grant, then see your grants:\n  um grant list",
            started.join(", ")
        )?;
    }
    if !unavailable.is_empty() {
        writeln!(
            stdout,
            "\nThis deployment does not serve grants of {}. Ask the deployment operator which providers it serves grants for.",
            unavailable.join(", ")
        )?;
    }
    if !organization.is_empty() {
        writeln!(
            stdout,
            "\nNo organization was resolved for the grants of {}. Name the organization:\n  {}",
            organization.join(", "),
            scope.organization()
        )?;
    }
    if !reconnect.is_empty() {
        writeln!(
            stdout,
            "\nYour grants of {} need reconnecting. Replace each with a new sign-in:",
            reconnect.join(", ")
        )?;
        for provider in &reconnect {
            writeln!(stdout, "  um grant create {provider} --replace")?;
        }
    }
    for remedy in failed.iter().filter_map(|provider| native_remedy(provider)) {
        writeln!(stdout, "\n{remedy}")?;
    }
    match outcome {
        "failed" if !failed.is_empty() => writeln!(
            stdout,
            "\nFix what each failed sign-in reported, then run the command again to sign in to each provider you still lack:\n  {}",
            scope.again()
        )?,
        "interrupted" | "terminated" => writeln!(
            stdout,
            "\nRun the command again to sign in to each provider you still lack:\n  {}",
            scope.again()
        )?,
        _ => {}
    }
    stdout.flush()?;
    Ok(())
}

/// Reports a failure that ended `um grant create` without a provider before any sign-in: as
/// its summary event, or on standard error with its remedy.
pub(super) fn write_create_refusal(
    deployment: &str,
    report: &ApiFailureReport,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json_line(&CreateSummary {
            category: report.category,
            retry_after: report.retry_after,
            ..CreateSummary::new(deployment, report.outcome)
        })?;
    } else {
        let mut stderr = io::stderr().lock();
        writeln!(stderr, "{}\n\n{}", report.summary, report.remedy)?;
    }
    Ok(report.class.exit_code())
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::write_pretty_json(value).context("write JSON grant result")
}

fn write_json_line(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::write_json_line(value).context("write JSON grant event")
}
