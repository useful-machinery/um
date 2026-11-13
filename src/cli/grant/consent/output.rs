//! What a consent prints (REQ-GRANT-1, REQ-GRANT-4): the session the member has to approve
//! and how it settled, as text or as newline-delimited JSON events, and the grant it made.
//! Nothing a response adds beyond the model reaches output.

use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use super::super::super::ApiFailureReport;
use super::super::projection::grant_provider;
use super::Front;
use crate::exit_code::{ExitCode, OutcomeClass};
use um_api::{
    Grant, GrantFailure, GrantProvider, GrantSession, GrantSessionReason, GrantSessionStatus,
    GrantState,
};

/// A grant as the CLI presents it: the model's named fields only, enums as their wire
/// strings. Nothing a response adds beyond the model reaches output (REQ-GRANT-4).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::cli) struct GrantView<'a> {
    id: &'a str,
    identity_id: &'a str,
    provider: &'static str,
    scopes: &'a [String],
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_used_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_expires_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_expires_at: Option<&'a str>,
    created_at: &'a str,
}

impl<'a> GrantView<'a> {
    pub(in crate::cli) fn new(grant: &'a Grant) -> Self {
        Self {
            id: &grant.id,
            identity_id: &grant.identity_id,
            provider: grant_provider(grant),
            scopes: &grant.scopes,
            state: match grant.state {
                GrantState::ProviderGrantStateActive => "active",
                GrantState::ProviderGrantStateReconnectRequired => "reconnect_required",
                GrantState::ProviderGrantStateRevoked => "revoked",
            },
            last_used_at: grant.last_used_at.as_deref(),
            access_expires_at: grant.access_expires_at.as_deref(),
            refresh_expires_at: grant.refresh_expires_at.as_deref(),
            created_at: &grant.created_at,
        }
    }

    pub(in crate::cli) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "grant: {}", self.id)?;
        writeln!(output, "identity: {}", self.identity_id)?;
        writeln!(output, "provider: {}", self.provider)?;
        if self.scopes.is_empty() {
            writeln!(output, "scopes: none")?;
        } else {
            writeln!(output, "scopes: {}", self.scopes.join(", "))?;
        }
        writeln!(output, "state: {}", self.state)?;
        for (key, value) in [
            ("last used", self.last_used_at),
            ("access expires", self.access_expires_at),
            ("refresh expires", self.refresh_expires_at),
        ] {
            if let Some(value) = value {
                writeln!(output, "{key}: {value}")?;
            }
        }
        writeln!(output, "created: {}", self.created_at)
    }
}

/// A grant operation's failure as the member reads it.
pub(in crate::cli) fn report(deployment: &str, failure: &GrantFailure) -> ApiFailureReport {
    const SUBJECT: &str = "provider grant";
    match failure {
        GrantFailure::Unauthenticated => ApiFailureReport::unauthenticated(SUBJECT),
        GrantFailure::Forbidden => ApiFailureReport::forbidden(SUBJECT),
        GrantFailure::InvalidInput => ApiFailureReport::new(
            "invalid_input",
            "error: provider grant request rejected by the deployment",
            "Check the grant ID, --limit and --cursor values.",
            OutcomeClass::GeneralFailure,
        ),
        GrantFailure::NotFound => ApiFailureReport::new(
            "not_found",
            "error: provider grant not found",
            "List your grants:\n  um grant list",
            OutcomeClass::GeneralFailure,
        ),
        GrantFailure::IdempotencyConflict => ApiFailureReport::idempotency_conflict(SUBJECT),
        GrantFailure::ConsentInProgress => ApiFailureReport::new(
            "consent_in_progress",
            "error: a consent for this provider is already in progress",
            "Finish that consent or wait for it to expire, then list your grants:\n  um grant list",
            OutcomeClass::GeneralFailure,
        ),
        GrantFailure::ProviderUnavailable => ApiFailureReport::new(
            "provider_unavailable",
            "error: this deployment does not serve grants for that provider",
            "Ask the deployment operator which providers it serves grants for.",
            OutcomeClass::Unreachable,
        ),
        GrantFailure::RateLimited { retry_after } => {
            ApiFailureReport::rate_limited(SUBJECT, *retry_after)
        }
        GrantFailure::Unreachable { category } => {
            ApiFailureReport::unreachable(deployment, *category)
        }
        GrantFailure::InvalidResponse { .. } => ApiFailureReport::invalid_response(SUBJECT),
    }
}

/// The provider a consent asks: as a start request names it, as its wire name and as its name
/// in prose.
#[derive(Clone, Copy)]
pub(in crate::cli) struct ConsentProvider {
    pub(in crate::cli) request: GrantProvider,
    pub(in crate::cli) name: &'static str,
    pub(in crate::cli) title: &'static str,
}

impl ConsentProvider {
    fn of_session(provider: um_api::provider_grant_session::Provider) -> Self {
        use um_api::provider_grant_session::Provider;
        match provider {
            Provider::ProviderGrantSessionProviderGithub => Self::GITHUB,
            Provider::ProviderGrantSessionProviderGitlabCom => Self::GITLAB_COM,
            Provider::ProviderGrantSessionProviderLinear => Self::LINEAR,
        }
    }

    pub(in crate::cli) const GITHUB: Self = Self {
        request: GrantProvider::CreateGrantSessionRequestProviderGithub,
        name: "github",
        title: "GitHub",
    };
    pub(in crate::cli) const GITLAB_COM: Self = Self {
        request: GrantProvider::CreateGrantSessionRequestProviderGitlabCom,
        name: "gitlab.com",
        title: "GitLab.com",
    };
    pub(in crate::cli) const LINEAR: Self = Self {
        request: GrantProvider::CreateGrantSessionRequestProviderLinear,
        name: "linear",
        title: "Linear",
    };

    fn remedy(self, front: Front) -> String {
        format!("Run {} {} again.", front.create, self.name)
    }
}

/// One line of a consent's JSON output: the consent it started, or its result.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsentEvent<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    authorization_url: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    grant: Option<&'a GrantView<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<u64>,
}

impl<'a> ConsentEvent<'a> {
    const fn result(deployment: &'a str, outcome: &'static str) -> Self {
        Self {
            schema_version: 1,
            event: "consent_result",
            deployment,
            outcome,
            session_id: None,
            provider: None,
            authorization_url: None,
            expires_at: None,
            reason: None,
            grant: None,
            category: None,
            retry_after: None,
        }
    }
}

/// A consent session as the CLI presents it: its named fields only, so nothing a response
/// adds beyond the model reaches output (REQ-GRANT-4).
pub(super) struct SessionView<'a> {
    id: &'a str,
    front: Front,
    provider: ConsentProvider,
    status: GrantSessionStatus,
    authorization_url: Option<&'a str>,
    expires_at: &'a str,
    reason: Option<GrantSessionReason>,
    grant: Option<GrantView<'a>>,
}

impl<'a> SessionView<'a> {
    pub(super) fn new(session: &'a GrantSession, front: Front) -> Self {
        Self {
            id: &session.id,
            front,
            provider: ConsentProvider::of_session(session.provider),
            status: session.status,
            authorization_url: session.authorization_url.as_deref(),
            expires_at: &session.expires_at,
            reason: session.reason_code,
            grant: session.result_grant.as_deref().map(GrantView::new),
        }
    }

    pub(super) fn is_pending(&self) -> bool {
        self.status == GrantSessionStatus::ProviderGrantSessionStatusPending
    }

    /// Where the member approves a pending consent.
    pub(super) const fn authorization_url(&self) -> Option<&'a str> {
        self.authorization_url
    }

    /// Prints the consent the member has to approve and where to approve it.
    pub(super) fn write_pending(&self, deployment: &str, json: bool) -> anyhow::Result<ExitCode> {
        if json {
            write_json_line(&ConsentEvent {
                event: "consent_required",
                session_id: Some(self.id),
                provider: Some(self.provider.name),
                authorization_url: self.authorization_url,
                expires_at: Some(self.expires_at),
                ..ConsentEvent::result(deployment, "pending")
            })?;
            return Ok(ExitCode::Success);
        }
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "! Provider grant consent awaits your approval.")?;
        self.write_fields(&mut stdout, deployment)?;
        if let Some(url) = self.authorization_url {
            writeln!(
                stdout,
                "\nOpen this URL to approve access as your {} user:\n  {url}",
                self.provider.title
            )?;
        }
        stdout.flush()?;
        Ok(ExitCode::Success)
    }

    /// Prints how the consent settled; only a completed consent succeeds.
    pub(super) fn write_result(
        &self,
        deployment: &str,
        follows_pending: bool,
        json: bool,
    ) -> anyhow::Result<ExitCode> {
        let (outcome, verdict) = match self.status {
            GrantSessionStatus::ProviderGrantSessionStatusPending => {
                return self.write_pending(deployment, json);
            }
            GrantSessionStatus::ProviderGrantSessionStatusCompleted => ("completed", None),
            GrantSessionStatus::ProviderGrantSessionStatusFailed => ("failed", Some("failed")),
            GrantSessionStatus::ProviderGrantSessionStatusExpired => ("expired", Some("expired")),
            GrantSessionStatus::ProviderGrantSessionStatusRecoveryRequired => {
                ("recovery_required", Some("needs recovery"))
            }
        };
        let code = if verdict.is_some() {
            ExitCode::GeneralFailure
        } else {
            ExitCode::Success
        };
        if json {
            write_json_line(&ConsentEvent {
                session_id: Some(self.id),
                reason: self.reason.map(reason_code),
                grant: self.grant.as_ref(),
                ..ConsentEvent::result(deployment, outcome)
            })?;
            return Ok(code);
        }
        let mut stdout = io::stdout().lock();
        if follows_pending {
            writeln!(stdout)?;
        }
        match (verdict, &self.grant) {
            (None, Some(grant)) => {
                writeln!(stdout, "✓ Provider grant created.")?;
                grant.write_human(&mut stdout)?;
                writeln!(stdout, "deployment: {deployment}")?;
            }
            (None, None) => {
                writeln!(stdout, "✓ Provider grant consent completed.")?;
                self.write_fields(&mut stdout, deployment)?;
            }
            (Some(verdict), _) => {
                match self.reason {
                    Some(reason) => writeln!(
                        stdout,
                        "✗ Provider grant consent {verdict}: {}.",
                        self.reason_words(reason)
                    )?,
                    None => writeln!(stdout, "✗ Provider grant consent {verdict}.")?,
                }
                self.write_fields(&mut stdout, deployment)?;
                writeln!(stdout, "\n{}", self.provider.remedy(self.front))?;
            }
        }
        stdout.flush()?;
        Ok(code)
    }

    fn write_fields(&self, output: &mut impl Write, deployment: &str) -> io::Result<()> {
        writeln!(output, "session: {}", self.id)?;
        writeln!(output, "provider: {}", self.provider.name)?;
        if self.is_pending() {
            writeln!(output, "expires: {}", self.expires_at)?;
        }
        if let Some(reason) = self.reason {
            writeln!(output, "reason: {}", reason_code(reason))?;
        }
        writeln!(output, "deployment: {deployment}")
    }

    fn reason_words(&self, reason: GrantSessionReason) -> String {
        let provider = self.provider.title;
        match reason {
            GrantSessionReason::ProviderGrantSessionReasonCodeConsentDenied => {
                format!("access was denied at {provider}")
            }
            GrantSessionReason::ProviderGrantSessionReasonCodeSessionExpired => {
                "the consent expired before it was approved".to_owned()
            }
            GrantSessionReason::ProviderGrantSessionReasonCodeOwnerAuthorityLost => {
                "your account can no longer complete it".to_owned()
            }
            GrantSessionReason::ProviderGrantSessionReasonCodeSubjectMismatch => {
                format!("the {provider} user you approved as does not match the identity")
            }
            GrantSessionReason::ProviderGrantSessionReasonCodeExchangeFailed => {
                format!("{provider} did not issue the grant")
            }
            GrantSessionReason::ProviderGrantSessionReasonCodeExchangeAmbiguous => {
                format!("the answer from {provider} could not be confirmed")
            }
            GrantSessionReason::ProviderGrantSessionReasonCodeLinkLimitReached => {
                "your account has reached its limit of linked identities".to_owned()
            }
        }
    }
}

const fn reason_code(reason: GrantSessionReason) -> &'static str {
    match reason {
        GrantSessionReason::ProviderGrantSessionReasonCodeConsentDenied => "consent_denied",
        GrantSessionReason::ProviderGrantSessionReasonCodeSessionExpired => "session_expired",
        GrantSessionReason::ProviderGrantSessionReasonCodeOwnerAuthorityLost => {
            "owner_authority_lost"
        }
        GrantSessionReason::ProviderGrantSessionReasonCodeSubjectMismatch => "subject_mismatch",
        GrantSessionReason::ProviderGrantSessionReasonCodeExchangeFailed => "exchange_failed",
        GrantSessionReason::ProviderGrantSessionReasonCodeExchangeAmbiguous => "exchange_ambiguous",
        GrantSessionReason::ProviderGrantSessionReasonCodeLinkLimitReached => "link_limit_reached",
    }
}

/// Reports a consent's start or poll that failed, naming the session once it is known.
pub(super) fn write_consent_failure(
    deployment: &str,
    front: Front,
    provider: ConsentProvider,
    session_id: Option<&str>,
    failure: &GrantFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let mut report = report(deployment, failure);
    match (failure, session_id) {
        (GrantFailure::NotFound, None) => {
            report.summary = "error: linked identity not found".to_owned();
            "List your linked identities:\n  um auth identity list".clone_into(&mut report.remedy);
        }
        (GrantFailure::NotFound, Some(_)) => {
            report.summary = "error: provider grant consent not found".to_owned();
            report.remedy = provider.remedy(front);
        }
        (GrantFailure::InvalidInput, _) => front.check_input.clone_into(&mut report.remedy),
        (GrantFailure::ConsentInProgress, _) => {
            report.remedy = format!(
                "Finish that consent or wait for it to expire, then list your grants:\n  {}",
                front.list
            );
        }
        (GrantFailure::ProviderUnavailable, _) => {
            report.summary = format!(
                "error: this deployment does not serve grants for {} yet",
                provider.title
            );
        }
        _ => {}
    }
    write_refusal(deployment, session_id, &report, json)
}

/// Reports a refusal as a consent's result: on standard output as its JSON event, or on
/// standard error with its remedy.
pub(in crate::cli) fn write_refusal(
    deployment: &str,
    session_id: Option<&str>,
    report: &ApiFailureReport,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json_line(&ConsentEvent {
            session_id,
            category: report.category,
            retry_after: report.retry_after,
            ..ConsentEvent::result(deployment, report.outcome)
        })?;
    } else {
        let mut stderr = io::stderr().lock();
        writeln!(stderr, "{}", report.summary)?;
        if let Some(id) = session_id {
            writeln!(stderr, "session: {id}")?;
        }
        writeln!(stderr, "\n{}", report.remedy)?;
    }
    Ok(report.class.exit_code())
}

/// Reports a wait that stopped before the consent settled; the consent itself continues.
pub(super) fn write_consent_stopped(
    deployment: &str,
    front: Front,
    session_id: Option<&str>,
    outcome: &'static str,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        return write_json_line(&ConsentEvent {
            session_id,
            ..ConsentEvent::result(deployment, outcome)
        });
    }
    let mut stderr = io::stderr().lock();
    writeln!(
        stderr,
        "error: provider grant consent {}",
        outcome.replace('_', " ")
    )?;
    if let Some(id) = session_id {
        writeln!(stderr, "session: {id}")?;
    }
    writeln!(
        stderr,
        "\nList your grants before starting another consent:\n  {}",
        front.list
    )?;
    Ok(())
}

fn write_json_line(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::super::write_json_line(value).context("write JSON provider grant event")
}
