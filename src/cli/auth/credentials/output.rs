use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::exit_code::ExitCode;
use um_api::{AgentLogin, AgentLoginAuthorization, AgentLoginState, Grant, GrantState};

use super::super::agent_logins::{EndedAuthorizationView, ended_authorizations};
use super::projection::{
    MemberStatus, NOT_CONNECTED, Subject, agent_login_provider, authorization_status,
    grant_provider, grant_status,
};

/// The providers a workflow can use a credential of, grant providers first, in the order the
/// rows are printed (REQ-AGENTLOGIN-45).
const PROVIDERS: [Provider; 5] = [
    Provider::Grant("github"),
    Provider::Grant("gitlab.com"),
    Provider::Grant("linear"),
    Provider::CodingAgent("claude_code"),
    Provider::CodingAgent("codex"),
];

#[derive(Clone, Copy)]
enum Provider {
    Grant(&'static str),
    CodingAgent(&'static str),
}

/// One credential a workflow can use as the member, with its status: a provider grant, an
/// agent login's authorization in force or the agent login when it has none, or a provider the
/// member has nothing unrevoked for.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CredentialView<'a> {
    provider: &'static str,
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identity_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runner_id: Option<&'a str>,
    #[serde(flatten)]
    status: MemberStatus,
}

impl<'a> CredentialView<'a> {
    fn grant(grant: &'a Grant) -> Self {
        let provider = grant_provider(grant);
        Self {
            identity_id: Some(&grant.identity_id),
            ..Self::bare(
                provider,
                "grant",
                Some(&grant.id),
                MemberStatus::new(grant_status(grant), Subject::Grant { provider }),
            )
        }
    }

    /// The agent login's row for one authorization in force, or its one row when it has none
    /// (`authorization` is `None`).
    fn agent_login(
        login: &'a AgentLogin,
        authorization: Option<&'a AgentLoginAuthorization>,
    ) -> Self {
        Self {
            label: login.label.as_deref(),
            organization_id: Some(&login.organization_id),
            generation: Some(login.generation),
            runner_id: authorization.map(|authorization| authorization.runner_id.as_str()),
            ..Self::bare(
                agent_login_provider(login.provider),
                "agent_login",
                Some(&login.id),
                MemberStatus::new(
                    authorization_status(login, authorization),
                    Subject::AgentLogin { id: &login.id },
                ),
            )
        }
    }

    fn not_connected(provider: Provider) -> Self {
        let (name, subject) = match provider {
            Provider::Grant(name) => (name, Subject::Grant { provider: name }),
            Provider::CodingAgent(name) => (name, Subject::CodingAgent { provider: name }),
        };
        Self::bare(
            name,
            "none",
            None,
            MemberStatus::new(NOT_CONNECTED, subject),
        )
    }

    const fn bare(
        provider: &'static str,
        kind: &'static str,
        id: Option<&'a str>,
        status: MemberStatus,
    ) -> Self {
        Self {
            provider,
            kind,
            id,
            identity_id: None,
            label: None,
            organization_id: None,
            generation: None,
            runner_id: None,
            status,
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        write!(output, "{}: ", self.provider)?;
        match (self.kind, self.id) {
            ("grant", Some(id)) => write!(output, "grant {id}")?,
            ("agent_login", Some(id)) => write!(output, "agent login {id}")?,
            _ => write!(output, "nothing connected")?,
        }
        if let Some(label) = self.label {
            write!(output, " ({label})")?;
        }
        writeln!(output)?;
        if let Some(identity_id) = self.identity_id {
            writeln!(output, "  identity: {identity_id}")?;
        }
        if let Some(organization_id) = self.organization_id {
            writeln!(output, "  organization: {organization_id}")?;
        }
        if let Some(generation) = self.generation {
            writeln!(output, "  generation: {generation}")?;
        }
        match (self.kind, self.runner_id) {
            (_, Some(runner_id)) => writeln!(output, "  runner: {runner_id}")?,
            ("agent_login", None) => writeln!(output, "  runner: none authorized")?,
            _ => {}
        }
        self.status.write_human(output, "  ")
    }
}

/// An ended authorization of one of the member's agent logins, revoked ones included, with its
/// removal state and stop as `um auth agent-login show` prints it (REQ-AGENTLOGIN-45).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EndedView<'a> {
    agent_login_id: &'a str,
    provider: &'static str,
    #[serde(flatten)]
    authorization: EndedAuthorizationView<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    credentials: &'a [CredentialView<'a>],
    ended_authorizations: &'a [EndedView<'a>],
}

/// The member's credentials: for each provider in turn its unrevoked grants, or the rows of its
/// unrevoked agent logins, or one `not_connected` row.
fn credentials<'a>(grants: &'a [Grant], logins: &'a [AgentLogin]) -> Vec<CredentialView<'a>> {
    let mut rows = Vec::new();
    for provider in PROVIDERS {
        let start = rows.len();
        match provider {
            Provider::Grant(name) => rows.extend(
                grants
                    .iter()
                    .filter(|grant| {
                        grant.state != GrantState::ProviderGrantStateRevoked
                            && grant_provider(grant) == name
                    })
                    .map(CredentialView::grant),
            ),
            Provider::CodingAgent(name) => {
                for login in logins.iter().filter(|login| {
                    login.state == AgentLoginState::AgentLoginStateActive
                        && agent_login_provider(login.provider) == name
                }) {
                    if login.authorizations.is_empty() {
                        rows.push(CredentialView::agent_login(login, None));
                    }
                    rows.extend(login.authorizations.iter().map(|authorization| {
                        CredentialView::agent_login(login, Some(authorization))
                    }));
                }
            }
        }
        if rows.len() == start {
            rows.push(CredentialView::not_connected(provider));
        }
    }
    rows
}

fn ended(logins: &[AgentLogin]) -> Vec<EndedView<'_>> {
    logins
        .iter()
        .flat_map(|login| {
            ended_authorizations(login)
                .unwrap_or_default()
                .into_iter()
                .map(|authorization| EndedView {
                    agent_login_id: &login.id,
                    provider: agent_login_provider(login.provider),
                    authorization,
                })
        })
        .collect()
}

/// Writes the member's credentials with their status; it exits 0 whatever they report.
pub(super) fn write_status(
    deployment: &str,
    grants: &[Grant],
    logins: &[AgentLogin],
    json: bool,
) -> anyhow::Result<ExitCode> {
    let credentials = credentials(grants, logins);
    let ended = ended(logins);
    if json {
        super::super::super::write_pretty_json(&StatusResult {
            schema_version: 1,
            deployment,
            outcome: "reported",
            credentials: &credentials,
            ended_authorizations: &ended,
        })
        .context("write JSON credential status")?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Credential status reported.")?;
    writeln!(stdout, "deployment: {deployment}")?;
    for credential in &credentials {
        writeln!(stdout)?;
        credential.write_human(&mut stdout)?;
    }
    if !ended.is_empty() {
        writeln!(stdout, "\nended authorizations: {}", ended.len())?;
    }
    for ended in &ended {
        writeln!(
            stdout,
            "{}: agent login {}",
            ended.provider, ended.agent_login_id
        )?;
        ended.authorization.write_human(&mut stdout)?;
    }
    stdout.flush()?;
    Ok(ExitCode::Success)
}
