//! A runner's authorization of a grant as the member reads it: what was last delivered to the
//! runner and what it reported (REQ-GRANT-13), its availability and next action
//! (REQ-GRANT-53), and, once it ended, whether the runner has confirmed removing its copy
//! (REQ-GRANT-47) and the stop requested of the run it was serving (REQ-GRANT-48).
//! `um grant list` prints it for a grant of either kind, and the earlier commands it replaces
//! for an agent login.

use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::exit_code::ExitCode;

use um_api::{
    AgentLogin, AgentLoginAuthorization, AgentLoginReportState, GrantEndedAuthorization,
    MemberGrantAuthorization,
};

use super::projection::{MemberStatus, Subject, agent_login_provider, authorization_status};

/// A runner's authorization of a grant or agent login, in force or ended: the model's named
/// fields only, enums as their wire strings. Nothing a response adds beyond the model reaches
/// output (REQ-GRANT-4, REQ-GRANT-14).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::cli) struct AuthorizationView<'a> {
    pub(in crate::cli) id: &'a str,
    pub(in crate::cli) runner_id: &'a str,
    // The runner's organization, which a grant's authorization names.
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_id: Option<&'a str>,
    created_at: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    delivery: Option<DeliveryView<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    report: Option<ReportView<'a>>,
    // An authorization in force's availability and next action (REQ-GRANT-53); an ended
    // one, and the one `authorize` just made, carry none.
    #[serde(flatten)]
    status: Option<MemberStatus>,
    // Whether the text form prints what was delivered and reported: an OAuth grant's
    // authorization has no exchange, and `um grant list` leaves it to `list GRANT`.
    #[serde(skip)]
    exchange: bool,
}

/// The runner's last authorization of the agent login, ended, with whether that runner has
/// confirmed removing the copy it kept (REQ-GRANT-47) and the stop requested of the run it
/// was serving (REQ-GRANT-48).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::cli) struct EndedAuthorizationView<'a> {
    #[serde(flatten)]
    pub(in crate::cli) authorization: AuthorizationView<'a>,
    ended_at: &'a str,
    pub(in crate::cli) removal: RemovalView<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::cli) stop: Option<StopView<'a>>,
}

/// The graceful cancellation Cloud requested of the run the runner was serving
/// (REQ-GRANT-48).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::cli) struct StopView<'a> {
    run_id: &'a str,
    cancellation_request_id: &'a str,
    requested_at: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::cli) struct RemovalView<'a> {
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
            organization_id: None,
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
            exchange: true,
        }
    }};
}

/// The agent login's ended authorizations, `None` when Cloud does not list them.
pub(in crate::cli) fn ended_authorizations(
    login: &AgentLogin,
) -> Option<Vec<EndedAuthorizationView<'_>>> {
    login
        .ended_authorizations
        .as_deref()
        .map(ended_authorization_views)
}

/// A native grant's ended authorizations, as the agent login's are read.
pub(in crate::cli) fn ended_authorization_views(
    ended: &[GrantEndedAuthorization],
) -> Vec<EndedAuthorizationView<'_>> {
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
        .collect()
}

impl EndedAuthorizationView<'_> {
    pub(in crate::cli) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        self.authorization.write_heading(output)?;
        writeln!(output, "    ended: {}", self.ended_at)?;
        self.authorization.write_exchange(output)?;
        self.write_outcome(output, "    ")
    }

    /// The runner's removal state and the stop requested there, if any.
    pub(in crate::cli) fn write_outcome(
        &self,
        output: &mut impl Write,
        indent: &str,
    ) -> io::Result<()> {
        self.removal.write_human(output, indent)?;
        match &self.stop {
            Some(stop) => stop.write_human(output, indent),
            None => Ok(()),
        }
    }
}

impl StopView<'_> {
    pub(in crate::cli) fn write_human(
        &self,
        output: &mut impl Write,
        indent: &str,
    ) -> io::Result<()> {
        writeln!(
            output,
            "{indent}stop: run {} asked to stop at {} (cancellation request {})",
            self.run_id, self.requested_at, self.cancellation_request_id
        )
    }
}

impl RemovalView<'_> {
    pub(in crate::cli) fn write_human(
        &self,
        output: &mut impl Write,
        indent: &str,
    ) -> io::Result<()> {
        match self.confirmed_at {
            Some(confirmed_at) => {
                writeln!(output, "{indent}removal: {} at {confirmed_at}", self.state)
            }
            None => writeln!(output, "{indent}removal: {}", self.state),
        }
    }
}

impl<'a> AuthorizationView<'a> {
    pub(in crate::cli) fn new(authorization: &'a AgentLoginAuthorization) -> Self {
        authorization_view!(authorization)
    }

    /// An authorization in force of a grant, with the runner's organization and the
    /// authorization's availability and next action; `exchange` keeps what was delivered to the
    /// runner and what it reported.
    pub(in crate::cli) fn of_grant(
        authorization: &'a MemberGrantAuthorization,
        status: MemberStatus,
        exchange: bool,
    ) -> Self {
        let view = authorization_view!(authorization);
        Self {
            organization_id: Some(&authorization.organization_id),
            delivery: view.delivery.filter(|_| exchange),
            report: view.report.filter(|_| exchange),
            status: Some(status),
            exchange,
            ..view
        }
    }

    /// The authorization of a grant `um grant authorize` just made, with the runner's
    /// organization: nothing has been delivered or reported under it yet.
    pub(in crate::cli) fn made(authorization: &'a MemberGrantAuthorization) -> Self {
        Self {
            organization_id: Some(&authorization.organization_id),
            exchange: false,
            ..authorization_view!(authorization)
        }
    }

    /// An authorization in force of `login`, with its availability and next action.
    pub(in crate::cli) fn in_force(
        login: &'a AgentLogin,
        authorization: &'a AgentLoginAuthorization,
    ) -> Self {
        Self {
            status: Some(MemberStatus::new(
                authorization_status(login, Some(authorization)),
                Subject::AgentLogin {
                    id: &login.id,
                    provider: agent_login_provider(login.provider),
                },
            )),
            ..Self::new(authorization)
        }
    }

    pub(in crate::cli) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        self.write_heading(output)?;
        if self.exchange {
            self.write_exchange(output)?;
        }
        match &self.status {
            Some(status) => status.write_human(output, "    "),
            None => Ok(()),
        }
    }

    fn write_heading(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "  authorization: {}", self.id)?;
        writeln!(output, "    runner: {}", self.runner_id)?;
        if let Some(organization_id) = self.organization_id {
            writeln!(output, "    organization: {organization_id}")?;
        }
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

/// What a runner's removal state says, and no more (REQ-GRANT-47).
pub(in crate::cli) fn write_removal_note(output: &mut impl Write) -> io::Result<()> {
    writeln!(
        output,
        "\nA runner whose removal is confirmed has removed the copy it kept; one still pending has not \
         yet confirmed. A confirmation covers only that copy: a step that ran there while the login \
         was installed could have copied it."
    )
}

/// What a stop is, and how to follow it with a command the CLI has (REQ-GRANT-48).
pub(in crate::cli) fn write_stop_note(output: &mut impl Write) -> io::Result<()> {
    writeln!(
        output,
        "\nEach run asked to stop was sent a graceful cancellation request, whichever member started \
         it. To wait for a run's cancellation to settle:\n  um run cancel ORGANIZATION RUN --wait"
    )
}

/// Each runner a revoked native grant was authorized for, with its removal state and the stop
/// requested there (REQ-GRANT-30, REQ-GRANT-68), then what those say.
pub(in crate::cli) fn write_revoked_runners(
    output: &mut impl Write,
    ended: &[EndedAuthorizationView<'_>],
) -> io::Result<()> {
    writeln!(output, "\nrunners: {}", ended.len())?;
    for authorization in ended {
        writeln!(
            output,
            "  runner: {}",
            authorization.authorization.runner_id
        )?;
        authorization.write_outcome(output, "    ")?;
    }
    write_removal_note(output)?;
    if ended
        .iter()
        .any(|authorization| authorization.stop.is_some())
    {
        write_stop_note(output)?;
    }
    Ok(())
}

/// That revoking a native grant in Cloud leaves its session at the provider (REQ-GRANT-23).
pub(in crate::cli) fn write_session_note(output: &mut impl Write) -> io::Result<()> {
    writeln!(
        output,
        "\nThis does not end the session at the provider. To end it, sign out of the coding agent there."
    )
}

/// What a runner's authorization was made or ended on: a grant, or, for the earlier agent-login
/// commands, an agent login, each under the member its result has always named.
#[derive(Clone, Copy, Serialize)]
pub(in crate::cli) enum Holder<'a> {
    #[serde(rename = "agentLoginId")]
    AgentLogin(&'a str),
    #[serde(rename = "grantId")]
    Grant(&'a str),
}

impl Holder<'_> {
    fn write_human(self, output: &mut impl Write) -> io::Result<()> {
        match self {
            Self::AgentLogin(id) => writeln!(output, "agent login: {id}"),
            Self::Grant(id) => writeln!(output, "grant: {id}"),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizeResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(flatten)]
    holder: Holder<'a>,
    authorization: AuthorizationView<'a>,
}

/// Reports the authorization `authorize` made (REQ-GRANT-35, REQ-GRANT-66).
pub(in crate::cli) fn write_authorized(
    deployment: &str,
    holder: Holder<'_>,
    authorization: AuthorizationView<'_>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&AuthorizeResult {
            schema_version: 1,
            deployment,
            outcome: "authorized",
            holder,
            authorization,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Runner authorized.")?;
    holder.write_human(&mut stdout)?;
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
    #[serde(flatten)]
    holder: Holder<'a>,
    authorization_id: &'a str,
    runner_id: &'a str,
    // Absent when the read after the change failed or did not list the ended authorization,
    // and for an OAuth grant, whose runner kept no copy.
    #[serde(skip_serializing_if = "Option::is_none")]
    removal: Option<RemovalView<'a>>,
    // Absent as well when no stop was requested there (REQ-GRANT-48).
    #[serde(skip_serializing_if = "Option::is_none")]
    stop: Option<StopView<'a>>,
}

/// Reports an ended authorization with its removal state and stop, as the read after the
/// change gives them in `ended`: `None` when that read failed, which leaves the change done,
/// or when there was none to make (REQ-GRANT-35, REQ-GRANT-67).
pub(in crate::cli) fn write_deauthorized(
    deployment: &str,
    holder: Holder<'_>,
    authorization_id: &str,
    runner_id: &str,
    ended: Option<EndedAuthorizationView<'_>>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let (removal, stop) = ended.map_or((None, None), |ended| (Some(ended.removal), ended.stop));
    if json {
        write_json(&DeauthorizeResult {
            schema_version: 1,
            deployment,
            outcome: "deauthorized",
            holder,
            authorization_id,
            runner_id,
            removal,
            stop,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Runner authorization ended.")?;
    holder.write_human(&mut stdout)?;
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

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::write_pretty_json(value).context("write JSON authorization result")
}
