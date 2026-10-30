use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::exit_code::{ExitCode, OutcomeClass};
use um_api::{
    Grant, GrantFailure, GrantList, GrantSession, GrantSessionReason, GrantSessionStatus,
    GrantState, GrantUse, GrantUseList,
};

type Stdout = io::StdoutLock<'static>;

/// A grant as the CLI presents it: the model's named fields only, enums as their wire
/// strings. Nothing a response adds beyond the model reaches output (REQ-GRANT-4).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GrantView<'a> {
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
    pub(super) fn new(grant: &'a Grant) -> Self {
        use um_api::provider_grant::Provider;
        Self {
            id: &grant.id,
            identity_id: &grant.identity_id,
            provider: match grant.provider {
                Provider::ProviderGrantProviderGithub => "github",
                Provider::ProviderGrantProviderGitlabCom => "gitlab.com",
                Provider::ProviderGrantProviderLinear => "linear",
            },
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

    pub(super) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UseView<'a> {
    id: &'a str,
    grant_id: &'a str,
    run_id: &'a str,
    assignment_id: &'a str,
    organization_id: &'a str,
    provider: &'static str,
    issued_at: &'a str,
}

impl<'a> UseView<'a> {
    fn new(value: &'a GrantUse) -> Self {
        use um_api::provider_grant_use::Provider;
        Self {
            id: &value.id,
            grant_id: &value.grant_id,
            run_id: &value.run_id,
            assignment_id: &value.assignment_id,
            organization_id: &value.organization_id,
            provider: match value.provider {
                Provider::ProviderGrantUseProviderGithub => "github",
                Provider::ProviderGrantUseProviderGitlabCom => "gitlab.com",
                Provider::ProviderGrantUseProviderLinear => "linear",
            },
            issued_at: &value.issued_at,
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "use: {}", self.id)?;
        writeln!(output, "run: {}", self.run_id)?;
        writeln!(output, "assignment: {}", self.assignment_id)?;
        writeln!(output, "organization: {}", self.organization_id)?;
        writeln!(output, "provider: {}", self.provider)?;
        writeln!(output, "issued: {}", self.issued_at)
    }
}

pub(super) fn write_list(
    deployment: &str,
    page: &GrantList,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let items: Vec<_> = page.items.iter().map(GrantView::new).collect();
    write_page(
        deployment,
        &items,
        page.next_cursor.as_deref(),
        json,
        |output| writeln!(output, "✓ Provider grants listed."),
        "grant",
        GrantView::write_human,
    )
}

pub(super) fn write_uses(
    deployment: &str,
    grant_id: &str,
    page: &GrantUseList,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let items: Vec<_> = page.items.iter().map(UseView::new).collect();
    write_page(
        deployment,
        &items,
        page.next_cursor.as_deref(),
        json,
        |output| {
            writeln!(output, "✓ Provider grant uses listed.")?;
            writeln!(output, "grant: {grant_id}")
        },
        "use",
        UseView::write_human,
    )
}

fn write_page<T: Serialize>(
    deployment: &str,
    items: &[T],
    next_cursor: Option<&str>,
    json: bool,
    heading: impl FnOnce(&mut Stdout) -> io::Result<()>,
    section: &str,
    item: impl Fn(&T, &mut Stdout) -> io::Result<()>,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&super::super::super::CloudListResult {
            schema_version: 1,
            deployment,
            outcome: "listed",
            items,
            next_cursor,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    heading(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    writeln!(stdout, "count: {}", items.len())?;
    for value in items {
        writeln!(stdout, "\n── {section} ──")?;
        item(value, &mut stdout)?;
    }
    if let Some(next_cursor) = next_cursor {
        writeln!(stdout, "\nnext cursor: {next_cursor}")?;
    }
    stdout.flush()?;
    Ok(ExitCode::Success)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RevokeResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    grant_id: &'a str,
}

pub(super) fn write_revoke(
    deployment: &str,
    grant_id: &str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&RevokeResult {
            schema_version: 1,
            deployment,
            outcome: "revoked",
            grant_id,
        })?;
    } else {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "✓ Provider grant revoked.")?;
        writeln!(stdout, "grant: {grant_id}")?;
        writeln!(stdout, "deployment: {deployment}")?;
    }
    Ok(ExitCode::Success)
}

/// A grant operation's failure as the member reads it: the closed outcome, the facts a
/// script needs, and the human summary and remedy.
struct FailureReport {
    outcome: &'static str,
    category: Option<&'static str>,
    retry_after: Option<u64>,
    summary: String,
    remedy: String,
    class: OutcomeClass,
}

impl FailureReport {
    fn new(deployment: &str, failure: &GrantFailure) -> Self {
        let report = |outcome, summary: &str, remedy: &str, class| Self {
            outcome,
            category: None,
            retry_after: None,
            summary: summary.to_owned(),
            remedy: remedy.to_owned(),
            class,
        };
        match failure {
            GrantFailure::Unauthenticated => report(
                "unauthenticated",
                "! You must sign in before managing provider grants.",
                "Run:\n  um auth login",
                OutcomeClass::Unauthenticated,
            ),
            GrantFailure::Forbidden => report(
                "forbidden",
                "! Your account is not permitted to manage provider grants.",
                "Sign in as an active member:\n  um auth login",
                OutcomeClass::Forbidden,
            ),
            GrantFailure::InvalidInput => report(
                "invalid_input",
                "error: provider grant request rejected by the deployment",
                "Check the grant ID, --limit and --cursor values.",
                OutcomeClass::GeneralFailure,
            ),
            GrantFailure::NotFound => report(
                "not_found",
                "error: provider grant not found",
                "List your provider grants:\n  um auth grant list",
                OutcomeClass::GeneralFailure,
            ),
            GrantFailure::IdempotencyConflict => report(
                "idempotency_conflict",
                "error: provider grant request key already used for another request",
                "Run the command again; it sends a new request key.",
                OutcomeClass::GeneralFailure,
            ),
            GrantFailure::ProviderUnavailable => report(
                "provider_unavailable",
                "error: this deployment does not serve grants for that provider",
                "Ask the deployment operator which providers it serves grants for.",
                OutcomeClass::Unreachable,
            ),
            GrantFailure::RateLimited { retry_after } => Self {
                retry_after: *retry_after,
                ..report(
                    "rate_limited",
                    &match retry_after {
                        Some(seconds) => format!(
                            "error: provider grant requests rate limited\nretry after: {seconds}s"
                        ),
                        None => "error: provider grant requests rate limited".to_owned(),
                    },
                    "Wait, then run the command again.",
                    OutcomeClass::RateLimited,
                )
            },
            GrantFailure::Unreachable { category } => Self {
                category: Some(category.as_str()),
                ..report(
                    "unreachable",
                    &format!(
                        "error: Useful Machinery deployment unreachable: {deployment}\ncategory: {}",
                        category.as_str()
                    ),
                    "Check access to the deployment, then run the command again.",
                    super::super::super::unreachable_outcome_class(*category),
                )
            },
            GrantFailure::InvalidResponse { .. } => report(
                "invalid_response",
                "error: invalid provider grant response from the deployment",
                "Use a CLI version supported by this deployment.",
                OutcomeClass::Protocol,
            ),
        }
    }
}

/// Reports a grant operation's failure with the remedy a member can act on.
pub(super) fn failure(
    deployment: &str,
    failure: &GrantFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let report = FailureReport::new(deployment, failure);
    if json {
        write_json(&super::super::super::ApiFailureResult::with_retry_after(
            deployment,
            report.outcome,
            report.category,
            report.retry_after,
        ))?;
    } else {
        writeln!(
            io::stderr().lock(),
            "{}\n\n{}",
            report.summary,
            report.remedy
        )?;
    }
    Ok(report.class.exit_code())
}

/// The provider a consent asks, as its wire name and its name in prose.
#[derive(Clone, Copy)]
pub(super) struct ConsentProvider {
    pub(super) name: &'static str,
    pub(super) title: &'static str,
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

    pub(super) const GITHUB: Self = Self {
        name: "github",
        title: "GitHub",
    };
    pub(super) const GITLAB_COM: Self = Self {
        name: "gitlab.com",
        title: "GitLab.com",
    };
    pub(super) const LINEAR: Self = Self {
        name: "linear",
        title: "Linear",
    };

    fn remedy(self) -> String {
        format!("Run um auth grant create {} again.", self.name)
    }
}

/// One line of `um auth grant create` JSON output: the consent it started, or its result.
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
    provider: ConsentProvider,
    status: GrantSessionStatus,
    authorization_url: Option<&'a str>,
    expires_at: &'a str,
    reason: Option<GrantSessionReason>,
    grant: Option<GrantView<'a>>,
}

impl<'a> SessionView<'a> {
    pub(super) fn new(session: &'a GrantSession) -> Self {
        Self {
            id: &session.id,
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
                writeln!(stdout, "\n{}", self.provider.remedy())?;
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
    provider: ConsentProvider,
    session_id: Option<&str>,
    failure: &GrantFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let mut report = FailureReport::new(deployment, failure);
    match (failure, session_id) {
        (GrantFailure::NotFound, None) => {
            report.summary = "error: linked identity not found".to_owned();
            "List your linked identities:\n  um auth identity list".clone_into(&mut report.remedy);
        }
        (GrantFailure::NotFound, Some(_)) => {
            report.summary = "error: provider grant consent not found".to_owned();
            report.remedy = provider.remedy();
        }
        (GrantFailure::InvalidInput, _) => {
            "Check the provider and --identity-id values.".clone_into(&mut report.remedy);
        }
        (GrantFailure::ProviderUnavailable, _) => {
            report.summary = format!(
                "error: this deployment does not serve grants for {} yet",
                provider.title
            );
        }
        _ => {}
    }
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
        "\nList your provider grants before starting another consent:\n  um auth grant list"
    )?;
    Ok(())
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::super::write_pretty_json(value).context("write JSON provider grant result")
}

fn write_json_line(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::super::write_json_line(value).context("write JSON provider grant event")
}
