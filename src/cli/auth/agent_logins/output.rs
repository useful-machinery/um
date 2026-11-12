use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use super::super::super::ApiFailureReport;
use crate::exit_code::{ExitCode, OutcomeClass};
use um_api::{
    AgentLogin, AgentLoginAuthorization, AgentLoginFailure, AgentLoginList, AgentLoginProvider,
    AgentLoginReportState, AgentLoginState,
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
    reported_at: &'a str,
}

impl<'a> AgentLoginView<'a> {
    pub(super) fn new(login: &'a AgentLogin) -> Self {
        Self {
            id: &login.id,
            organization_id: &login.organization_id,
            provider: match login.provider {
                AgentLoginProvider::AgentLoginProviderClaudeCode => "claude_code",
                AgentLoginProvider::AgentLoginProviderCodex => "codex",
            },
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
                .map(AuthorizationView::new)
                .collect(),
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
            return writeln!(output, "authorizations: none");
        }
        writeln!(output, "authorizations: {}", self.authorizations.len())?;
        for authorization in &self.authorizations {
            authorization.write_human(output)?;
        }
        Ok(())
    }
}

impl<'a> AuthorizationView<'a> {
    fn new(authorization: &'a AgentLoginAuthorization) -> Self {
        Self {
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
                reported_at: &report.reported_at,
            }),
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "  authorization: {}", self.id)?;
        writeln!(output, "    runner: {}", self.runner_id)?;
        writeln!(output, "    authorized: {}", self.created_at)?;
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
            }) => writeln!(
                output,
                "    reported: {state}, generation {generation} at {reported_at}"
            ),
            Some(ReportView {
                state,
                generation: None,
                reported_at,
            }) => writeln!(output, "    reported: {state} at {reported_at}"),
            None => writeln!(output, "    reported: never"),
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
}

pub(super) fn write_revoke(
    deployment: &str,
    agent_login_id: &str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&RevokeResult {
            schema_version: 1,
            deployment,
            outcome: "revoked",
            agent_login_id,
        })?;
    } else {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "✓ Agent login revoked.")?;
        writeln!(stdout, "agent login: {agent_login_id}")?;
        writeln!(stdout, "deployment: {deployment}")?;
        writeln!(
            stdout,
            "\nThis does not end the session at the provider. To end it, sign out of the coding agent there."
        )?;
    }
    Ok(ExitCode::Success)
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
}

pub(super) fn write_deauthorize(
    deployment: &str,
    agent_login_id: &str,
    authorization_id: &str,
    runner_id: &str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&DeauthorizeResult {
            schema_version: 1,
            deployment,
            outcome: "deauthorized",
            agent_login_id,
            authorization_id,
            runner_id,
        })?;
    } else {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "✓ Runner authorization ended.")?;
        writeln!(stdout, "agent login: {agent_login_id}")?;
        writeln!(stdout, "authorization: {authorization_id}")?;
        writeln!(stdout, "runner: {runner_id}")?;
        writeln!(stdout, "deployment: {deployment}")?;
    }
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
pub(super) fn failure(
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
