use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use super::super::super::ApiFailureReport;
use crate::cli::grant::authorization::{
    AuthorizationView, Holder, write_authorized, write_deauthorized, write_revoked_runners,
    write_session_note,
};
use crate::cli::grant::authorize::MOVE_CODEX_GRANT;
// The earlier `um auth credential status` lists an agent login's ended authorizations as `show`
// does.
pub(in crate::cli::auth) use crate::cli::grant::authorization::{
    EndedAuthorizationView, ended_authorizations,
};
use crate::cli::grant::projection::agent_login_provider;
use crate::exit_code::{ExitCode, OutcomeClass};
use um_api::{
    AgentLogin, AgentLoginAuthorization, AgentLoginFailure, AgentLoginList, AgentLoginState,
};

/// An agent login as the CLI presents it: the model's named fields only, enums as their wire
/// strings. Nothing a response adds beyond the model reaches output (REQ-GRANT-14).
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
/// authorized for it take that generation (REQ-GRANT-54).
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
        Some(ended) if !ended.is_empty() => write_revoked_runners(&mut stdout, ended)?,
        Some(_) => {}
        None if after.is_none() => writeln!(
            stdout,
            "\nTo see which runners have confirmed removing it:\n  um grant list {agent_login_id}"
        )?,
        None => {}
    }
    write_session_note(&mut stdout)?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

pub(super) fn write_authorize(
    deployment: &str,
    agent_login_id: &str,
    authorization: &AgentLoginAuthorization,
    json: bool,
) -> anyhow::Result<ExitCode> {
    write_authorized(
        deployment,
        Holder::AgentLogin(agent_login_id),
        AuthorizationView::new(authorization),
        json,
    )
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
    write_deauthorized(
        deployment,
        Holder::AgentLogin(agent_login_id),
        authorization_id,
        runner_id,
        ended,
        json,
    )
}

/// An agent-login operation's failure as the member reads it.
fn report(deployment: &str, failure: &AgentLoginFailure) -> ApiFailureReport {
    const SUBJECT: &str = "agent login";
    // Every refusal of REQ-GRANT-31 exits 1 with its remedy.
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
            "List your grants:\n  um grant list",
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
            "Revoke one you no longer use:\n  um grant revoke GRANT --yes",
        ),
        AgentLoginFailure::RunnerNotFound => refused(
            "runner_not_found",
            "error: runner not found in the agent login's organization",
            "List the organization's runners and use a runner ID:\n  um runner list ORGANIZATION",
        ),
        AgentLoginFailure::RunnerHoldsLogin => refused(
            "runner_holds_login",
            "error: that runner already holds an agent login for this coding agent",
            "The owner of the login the runner holds must end that authorization first:\n  um grant deauthorize GRANT RUNNER --yes",
        ),
        AgentLoginFailure::LoginAlreadyAuthorized => refused(
            "login_already_authorized",
            "error: this Codex login is already authorized for a runner",
            MOVE_CODEX_GRANT,
        ),
        AgentLoginFailure::AuthorizationNotFound => refused(
            "authorization_not_found",
            "error: that runner holds no authorization of this agent login",
            "Show the grant's authorizations:\n  um grant list GRANT",
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

/// Reports a `--login` sign-in that made no login to import (REQ-GRANT-34): nothing was
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
