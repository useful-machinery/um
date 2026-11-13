use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use super::super::super::ApiFailureReport;
use super::super::credentials::projection::{
    MemberStatus, Subject, agent_login_provider, authorization_status,
};
use crate::exit_code::{ExitCode, OutcomeClass};
use um_api::{
    AgentLogin, AgentLoginAuthorization, AgentLoginFailure, AgentLoginList, AgentLoginReportState,
    AgentLoginState,
};

/// An agent login as the CLI presents it: the model's named fields only, enums as their wire
/// strings. Nothing a response adds beyond the model reaches output (REQ-AGENTLOGIN-4).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AgentLoginView<'a> {
    id: &'a str,
    organization_id: &'a str,
    provider: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<&'a str>,
    state: &'static str,
    generation: i64,
    record_schema_version: i32,
    key_version: i32,
    created_at: &'a str,
    imported_at: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    revoked_at: Option<&'a str>,
    authorizations: Vec<AuthorizationView<'a>>,
    // Absent from a Cloud that does not list ended authorizations, and then not printed.
    #[serde(skip_serializing_if = "Option::is_none")]
    ended_authorizations: Option<Vec<EndedAuthorizationView<'a>>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizationView<'a> {
    id: &'a str,
    runner_id: &'a str,
    created_at: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    delivery: Option<DeliveryView<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    report: Option<ReportView<'a>>,
    // An authorization in force's availability and next action (REQ-AGENTLOGIN-43); an ended
    // one, and the one `authorize` just made, carry none.
    #[serde(flatten)]
    status: Option<MemberStatus>,
}

/// The runner's last authorization of the agent login, ended, with whether that runner has
/// confirmed removing the copy it kept (REQ-AGENTLOGIN-37) and the stop requested of the run it
/// was serving (REQ-AGENTLOGIN-38).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::cli::auth) struct EndedAuthorizationView<'a> {
    #[serde(flatten)]
    authorization: AuthorizationView<'a>,
    ended_at: &'a str,
    removal: RemovalView<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop: Option<StopView<'a>>,
}

/// The graceful cancellation Cloud requested of the run the runner was serving
/// (REQ-AGENTLOGIN-38).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StopView<'a> {
    run_id: &'a str,
    cancellation_request_id: &'a str,
    requested_at: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RemovalView<'a> {
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    confirmed_at: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryView<'a> {
    generation: i64,
    delivered_at: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReportView<'a> {
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_use: Option<&'static str>,
    reported_at: &'a str,
}

/// The fields an authorization in force and an ended one share, read alike from either
/// generated model.
macro_rules! authorization_view {
    ($authorization:expr) => {{
        let authorization = $authorization;
        AuthorizationView {
            id: &authorization.id,
            runner_id: &authorization.runner_id,
            created_at: &authorization.created_at,
            delivery: authorization
                .delivery
                .as_deref()
                .map(|delivery| DeliveryView {
                    generation: delivery.generation,
                    delivered_at: &delivery.delivered_at,
                }),
            report: authorization.report.as_deref().map(|report| ReportView {
                state: report_state(report.state),
                generation: report.generation,
                last_use: report.last_use.map(|last_use| last_use.as_str()),
                reported_at: &report.reported_at,
            }),
            status: None,
        }
    }};
}

/// The agent login's ended authorizations, `None` when Cloud does not list them.
pub(in crate::cli::auth) fn ended_authorizations(
    login: &AgentLogin,
) -> Option<Vec<EndedAuthorizationView<'_>>> {
    let ended = login.ended_authorizations.as_ref()?;
    Some(
        ended
            .iter()
            .map(|ended| EndedAuthorizationView {
                authorization: authorization_view!(ended),
                ended_at: &ended.ended_at,
                removal: RemovalView {
                    state: ended.removal.state.as_str(),
                    confirmed_at: ended.removal.confirmed_at.as_deref(),
                },
                stop: ended.stop.as_deref().map(|stop| StopView {
                    run_id: &stop.run_id,
                    cancellation_request_id: &stop.cancellation_request_id,
                    requested_at: &stop.requested_at,
                }),
            })
            .collect(),
    )
}

impl<'a> AgentLoginView<'a> {
    pub(super) fn new(login: &'a AgentLogin) -> Self {
        Self {
            id: &login.id,
            organization_id: &login.organization_id,
            provider: agent_login_provider(login.provider),
            label: login.label.as_deref(),
            state: match login.state {
                AgentLoginState::AgentLoginStateActive => "active",
                AgentLoginState::AgentLoginStateRevoked => "revoked",
            },
            generation: login.generation,
            record_schema_version: login.record_schema_version,
            key_version: login.key_version,
            created_at: &login.created_at,
            imported_at: &login.imported_at,
            revoked_at: login.revoked_at.as_deref(),
            authorizations: login
                .authorizations
                .iter()
                .map(|authorization| AuthorizationView::in_force(login, authorization))
                .collect(),
            ended_authorizations: ended_authorizations(login),
        }
    }

    pub(super) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "agent login: {}", self.id)?;
        writeln!(output, "organization: {}", self.organization_id)?;
        writeln!(output, "provider: {}", self.provider)?;
        if let Some(label) = self.label {
            writeln!(output, "label: {label}")?;
        }
        writeln!(output, "state: {}", self.state)?;
        writeln!(output, "generation: {}", self.generation)?;
        writeln!(output, "imported: {}", self.imported_at)?;
        if let Some(revoked_at) = self.revoked_at {
            writeln!(output, "revoked: {revoked_at}")?;
        }
        if self.authorizations.is_empty() {
            writeln!(output, "authorizations: none")?;
        } else {
            writeln!(output, "authorizations: {}", self.authorizations.len())?;
        }
        for authorization in &self.authorizations {
            authorization.write_human(output)?;
        }
        let ended = self.ended_authorizations.as_deref().unwrap_or_default();
        if !ended.is_empty() {
            writeln!(output, "ended authorizations: {}", ended.len())?;
        }
        for authorization in ended {
            authorization.write_human(output)?;
        }
        Ok(())
    }
}

impl EndedAuthorizationView<'_> {
    pub(in crate::cli::auth) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        self.authorization.write_heading(output)?;
        writeln!(output, "    ended: {}", self.ended_at)?;
        self.authorization.write_exchange(output)?;
        self.write_outcome(output, "    ")
    }

    /// The runner's removal state and the stop requested there, if any.
    fn write_outcome(&self, output: &mut impl Write, indent: &str) -> io::Result<()> {
        self.removal.write_human(output, indent)?;
        match &self.stop {
            Some(stop) => stop.write_human(output, indent),
            None => Ok(()),
        }
    }
}

impl StopView<'_> {
    fn write_human(&self, output: &mut impl Write, indent: &str) -> io::Result<()> {
        writeln!(
            output,
            "{indent}stop: run {} asked to stop at {} (cancellation request {})",
            self.run_id, self.requested_at, self.cancellation_request_id
        )
    }
}

impl RemovalView<'_> {
    fn write_human(&self, output: &mut impl Write, indent: &str) -> io::Result<()> {
        match self.confirmed_at {
            Some(confirmed_at) => {
                writeln!(output, "{indent}removal: {} at {confirmed_at}", self.state)
            }
            None => writeln!(output, "{indent}removal: {}", self.state),
        }
    }
}

impl<'a> AuthorizationView<'a> {
    fn new(authorization: &'a AgentLoginAuthorization) -> Self {
        authorization_view!(authorization)
    }

    /// An authorization in force of `login`, with its availability and next action.
    fn in_force(login: &'a AgentLogin, authorization: &'a AgentLoginAuthorization) -> Self {
        Self {
            status: Some(MemberStatus::new(
                authorization_status(login, Some(authorization)),
                Subject::AgentLogin { id: &login.id },
            )),
            ..Self::new(authorization)
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        self.write_heading(output)?;
        self.write_exchange(output)?;
        match &self.status {
            Some(status) => status.write_human(output, "    "),
            None => Ok(()),
        }
    }

    fn write_heading(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "  authorization: {}", self.id)?;
        writeln!(output, "    runner: {}", self.runner_id)?;
        writeln!(output, "    authorized: {}", self.created_at)
    }

    /// What was last delivered to the runner and what it last reported.
    fn write_exchange(&self, output: &mut impl Write) -> io::Result<()> {
        match &self.delivery {
            Some(delivery) => writeln!(
                output,
                "    delivered: generation {} at {}",
                delivery.generation, delivery.delivered_at
            )?,
            None => writeln!(output, "    delivered: never")?,
        }
        match &self.report {
            Some(ReportView {
                state,
                generation: Some(generation),
                reported_at,
                ..
            }) => writeln!(
                output,
                "    reported: {state}, generation {generation} at {reported_at}"
            )?,
            Some(ReportView {
                state,
                generation: None,
                reported_at,
                ..
            }) => writeln!(output, "    reported: {state} at {reported_at}")?,
            None => writeln!(output, "    reported: never")?,
        }
        match self.report.as_ref().and_then(|report| report.last_use) {
            Some(last_use) => writeln!(output, "    last use: {last_use}"),
            None => Ok(()),
        }
    }
}

const fn report_state(state: AgentLoginReportState) -> &'static str {
    match state {
        AgentLoginReportState::AgentLoginReportStateReady => "ready",
        AgentLoginReportState::AgentLoginReportStateNotAuthorized => "not_authorized",
        AgentLoginReportState::AgentLoginReportStateRevoked => "revoked",
        AgentLoginReportState::AgentLoginReportStateForeignLogin => "foreign_login",
        AgentLoginReportState::AgentLoginReportStateUnsupportedTarget => "unsupported_target",
        AgentLoginReportState::AgentLoginReportStateStaleGeneration => "stale_generation",
        AgentLoginReportState::AgentLoginReportStateCloudUnavailable => "cloud_unavailable",
        AgentLoginReportState::AgentLoginReportStateInstallFailed => "install_failed",
    }
}

pub(super) fn write_list(
    deployment: &str,
    page: &AgentLoginList,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let items: Vec<_> = page.items.iter().map(AgentLoginView::new).collect();
    let next_cursor = page.next_cursor.as_deref();
    if json {
        write_json(&super::super::super::CloudListResult {
            schema_version: 1,
            deployment,
            outcome: "listed",
            items: &items,
            next_cursor,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Agent logins listed.")?;
    writeln!(stdout, "deployment: {deployment}")?;
    writeln!(stdout, "count: {}", items.len())?;
    for item in &items {
        writeln!(stdout, "\n── agent login ──")?;
        item.write_human(&mut stdout)?;
    }
    if let Some(next_cursor) = next_cursor {
        writeln!(stdout, "\nnext cursor: {next_cursor}")?;
    }
    stdout.flush()?;
    Ok(ExitCode::Success)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentLoginResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    agent_login: AgentLoginView<'a>,
}

/// Writes one agent login under `outcome` (`shown`, or a mutation's outcome).
pub(super) fn write_agent_login(
    deployment: &str,
    outcome: &'static str,
    login: &AgentLogin,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_agent_login_then(deployment, outcome, login, json, |_| Ok(()))
}

/// Writes the agent login at the generation `reconnect` stored, and when the runners
/// authorized for it take that generation (REQ-AGENTLOGIN-44).
pub(super) fn write_reconnected(
    deployment: &str,
    login: &AgentLogin,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_agent_login_then(deployment, "reconnected", login, json, |output| {
        writeln!(
            output,
            "\nEach runner authorized for this agent login installs generation {} at its next setup.",
            login.generation
        )
    })
}

/// Writes one agent login under `outcome`, the text form followed by `note`.
fn write_agent_login_then(
    deployment: &str,
    outcome: &'static str,
    login: &AgentLogin,
    json: bool,
    note: impl FnOnce(&mut io::StdoutLock<'_>) -> io::Result<()>,
) -> anyhow::Result<ExitCode> {
    let view = AgentLoginView::new(login);
    if json {
        write_json(&AgentLoginResult {
            schema_version: 1,
            deployment,
            outcome,
            agent_login: view,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Agent login {outcome}.")?;
    view.write_human(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    note(&mut stdout)?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RevokeResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    agent_login_id: &'a str,
    // Absent when the read after the revocation failed or did not list them.
    #[serde(skip_serializing_if = "Option::is_none")]
    ended_authorizations: Option<Vec<EndedAuthorizationView<'a>>>,
}

/// Reports a revocation with what the agent login read after it, `after`, says each runner has
/// confirmed; `None` when that read failed, which leaves the revocation done.
pub(super) fn write_revoke(
    deployment: &str,
    agent_login_id: &str,
    after: Option<&AgentLogin>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let ended = after.and_then(ended_authorizations);
    if json {
        write_json(&RevokeResult {
            schema_version: 1,
            deployment,
            outcome: "revoked",
            agent_login_id,
            ended_authorizations: ended,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Agent login revoked.")?;
    writeln!(stdout, "agent login: {agent_login_id}")?;
    writeln!(stdout, "deployment: {deployment}")?;
    match ended.as_deref() {
        Some(ended) if !ended.is_empty() => {
            writeln!(stdout, "\nrunners: {}", ended.len())?;
            for authorization in ended {
                writeln!(
                    stdout,
                    "  runner: {}",
                    authorization.authorization.runner_id
                )?;
                authorization.write_outcome(&mut stdout, "    ")?;
            }
            write_removal_note(&mut stdout)?;
            if ended
                .iter()
                .any(|authorization| authorization.stop.is_some())
            {
                write_stop_note(&mut stdout)?;
            }
        }
        Some(_) => {}
        None if after.is_none() => writeln!(
            stdout,
            "\nTo see which runners have confirmed removing it:\n  um auth agent-login show {agent_login_id}"
        )?,
        None => {}
    }
    writeln!(
        stdout,
        "\nThis does not end the session at the provider. To end it, sign out of the coding agent there."
    )?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

/// What a runner's removal state says, and no more (REQ-AGENTLOGIN-37).
fn write_removal_note(output: &mut impl Write) -> io::Result<()> {
    writeln!(
        output,
        "\nA runner whose removal is confirmed has removed the copy it kept; one still pending has not \
         yet confirmed. A confirmation covers only that copy: a step that ran there while the login \
         was installed could have copied it."
    )
}

/// What a stop is, and how to follow it with a command the CLI has (REQ-AGENTLOGIN-38).
fn write_stop_note(output: &mut impl Write) -> io::Result<()> {
    writeln!(
        output,
        "\nEach run asked to stop was sent a graceful cancellation request, whichever member started \
         it. To wait for a run's cancellation to settle:\n  um run cancel ORGANIZATION RUN --wait"
    )
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizeResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    agent_login_id: &'a str,
    authorization: AuthorizationView<'a>,
}

pub(super) fn write_authorize(
    deployment: &str,
    agent_login_id: &str,
    authorization: &AgentLoginAuthorization,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let authorization = AuthorizationView::new(authorization);
    if json {
        write_json(&AuthorizeResult {
            schema_version: 1,
            deployment,
            outcome: "authorized",
            agent_login_id,
            authorization,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Runner authorized.")?;
    writeln!(stdout, "agent login: {agent_login_id}")?;
    authorization.write_human(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeauthorizeResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    agent_login_id: &'a str,
    authorization_id: &'a str,
    runner_id: &'a str,
    // Absent when the read after the change failed or did not list the ended authorization.
    #[serde(skip_serializing_if = "Option::is_none")]
    removal: Option<RemovalView<'a>>,
    // Absent as well when no stop was requested there (REQ-AGENTLOGIN-38).
    #[serde(skip_serializing_if = "Option::is_none")]
    stop: Option<StopView<'a>>,
}

/// Reports an ended authorization with its removal state and stop as the agent login read
/// after the change, `after`, gives them; `None` when that read failed, which leaves the change
/// done.
pub(super) fn write_deauthorize(
    deployment: &str,
    agent_login_id: &str,
    authorization_id: &str,
    runner_id: &str,
    after: Option<&AgentLogin>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let ended = after
        .and_then(ended_authorizations)
        .into_iter()
        .flatten()
        .find(|ended| ended.authorization.id == authorization_id);
    let (removal, stop) = ended.map_or((None, None), |ended| (Some(ended.removal), ended.stop));
    if json {
        write_json(&DeauthorizeResult {
            schema_version: 1,
            deployment,
            outcome: "deauthorized",
            agent_login_id,
            authorization_id,
            runner_id,
            removal,
            stop,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Runner authorization ended.")?;
    writeln!(stdout, "agent login: {agent_login_id}")?;
    writeln!(stdout, "authorization: {authorization_id}")?;
    writeln!(stdout, "runner: {runner_id}")?;
    if let Some(removal) = &removal {
        removal.write_human(&mut stdout, "")?;
    }
    if let Some(stop) = &stop {
        stop.write_human(&mut stdout, "")?;
    }
    writeln!(stdout, "deployment: {deployment}")?;
    if removal.is_some() {
        write_removal_note(&mut stdout)?;
    }
    if stop.is_some() {
        write_stop_note(&mut stdout)?;
    }
    stdout.flush()?;
    Ok(ExitCode::Success)
}

/// An agent-login operation's failure as the member reads it.
fn report(deployment: &str, failure: &AgentLoginFailure) -> ApiFailureReport {
    const SUBJECT: &str = "agent login";
    // Every refusal of REQ-AGENTLOGIN-21 exits 1 with its remedy.
    let refused = |outcome, summary: &str, remedy: &str| {
        ApiFailureReport::new(outcome, summary, remedy, OutcomeClass::GeneralFailure)
    };
    match failure {
        AgentLoginFailure::Unauthenticated => ApiFailureReport::unauthenticated(SUBJECT),
        AgentLoginFailure::Forbidden => ApiFailureReport::forbidden(SUBJECT),
        AgentLoginFailure::InvalidInput => refused(
            "invalid_input",
            "error: agent login request rejected by the deployment",
            "Check the agent login ID and the --label, --limit and --cursor values.",
        ),
        AgentLoginFailure::IdempotencyConflict => ApiFailureReport::idempotency_conflict(SUBJECT),
        AgentLoginFailure::ProviderUnavailable => refused(
            "provider_unavailable",
            "error: this deployment does not serve agent logins for that coding agent",
            "Ask the deployment operator which coding agents it serves agent logins for.",
        ),
        AgentLoginFailure::AgentLoginNotFound => refused(
            "agent_login_not_found",
            "error: agent login not found",
            "List your agent logins:\n  um auth agent-login list",
        ),
        AgentLoginFailure::OrganizationNotFound => refused(
            "organization_not_found",
            "error: organization not found among your active memberships",
            "List your organizations:\n  um organization list",
        ),
        AgentLoginFailure::MaterialInvalid => refused(
            "material_invalid",
            "error: the deployment did not accept the agent login",
            "Import a Codex auth.json, or the token claude setup-token printed, unchanged.",
        ),
        AgentLoginFailure::LimitReached => refused(
            "limit_reached",
            "error: you hold the most agent logins this organization allows",
            "Revoke one you no longer use:\n  um auth agent-login revoke AGENT_LOGIN --yes",
        ),
        AgentLoginFailure::RunnerNotFound => refused(
            "runner_not_found",
            "error: runner not found in the agent login's organization",
            "List the organization's runners and use a runner ID:\n  um runner list ORGANIZATION",
        ),
        AgentLoginFailure::RunnerHoldsLogin => refused(
            "runner_holds_login",
            "error: that runner already holds an agent login for this coding agent",
            "The owner of the login the runner holds must end that authorization first:\n  um auth agent-login deauthorize AGENT_LOGIN --runner RUNNER_ID --yes",
        ),
        AgentLoginFailure::LoginAlreadyAuthorized => refused(
            "login_already_authorized",
            "error: this Codex login is already authorized for a runner",
            "A Codex login serves one runner. Import another login, which makes another agent login, for another runner:\n  um auth agent-login import ORGANIZATION codex --login",
        ),
        AgentLoginFailure::AuthorizationNotFound => refused(
            "authorization_not_found",
            "error: that runner holds no authorization of this agent login",
            "Show the agent login's authorizations:\n  um auth agent-login show AGENT_LOGIN",
        ),
        AgentLoginFailure::RateLimited { retry_after } => {
            ApiFailureReport::rate_limited(SUBJECT, *retry_after)
        }
        AgentLoginFailure::Unreachable { category } => {
            ApiFailureReport::unreachable(deployment, *category)
        }
        AgentLoginFailure::InvalidResponse { .. } => ApiFailureReport::invalid_response(SUBJECT),
    }
}

/// Reports an agent-login operation's failure with the remedy a member can act on.
pub(in crate::cli::auth) fn failure(
    deployment: &str,
    failure: &AgentLoginFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    report(deployment, failure).write(deployment, json)
}

/// Reports a `--login` sign-in that made no login to import (REQ-AGENTLOGIN-24): nothing was
/// sent to the deployment.
pub(super) fn provider_login_failed(
    deployment: &str,
    failure: &super::codex_login::Failure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    ApiFailureReport::new(
        "provider_login_failed",
        format!("error: sign in to Codex: {failure}"),
        failure.remedy(),
        OutcomeClass::GeneralFailure,
    )
    .write(deployment, json)
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::super::write_pretty_json(value).context("write JSON agent login result")
}
