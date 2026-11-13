//! The member's view of a credential (REQ-AGENTLOGIN-43): what Cloud's API returns for an agent
//! login's authorization and for a provider grant, mapped to one availability and one next
//! action, with the command or step that next action names. `um auth credential status` and
//! `um auth agent-login list` and `show` print it; Cloud's API carries the facts only.

use std::io::{self, Write};

use serde::Serialize;

use um_api::{
    AgentLogin, AgentLoginAuthorization, AgentLoginProvider, AgentLoginReportState,
    AgentLoginState, Grant, GrantState, provider_grant,
};
use um_support::{Availability, CredentialStatus, NextAction};

/// The status of an agent login's authorization in force, or of the agent login when it has
/// none (`authorization` is `None`), as its owner reads it.
pub(in crate::cli::auth) fn authorization_status(
    login: &AgentLogin,
    authorization: Option<&AgentLoginAuthorization>,
) -> CredentialStatus {
    let (availability, next_action) = match (login.state, authorization) {
        (AgentLoginState::AgentLoginStateRevoked, _) => {
            (Availability::NotConnected, NextAction::None)
        }
        (AgentLoginState::AgentLoginStateActive, None) => {
            (Availability::NotConnected, NextAction::AuthorizeRunner)
        }
        (AgentLoginState::AgentLoginStateActive, Some(authorization)) => {
            match authorization.report.as_deref() {
                // Cloud holds what the runner has not reported installing.
                None => (Availability::Pending, NextAction::WaitForRunner),
                Some(report) => match report.state {
                    AgentLoginReportState::AgentLoginReportStateReady
                        if report.generation == Some(login.generation) =>
                    {
                        match report.last_use.map(|last_use| last_use.as_str()) {
                            None => (Availability::Unverified, NextAction::None),
                            Some("refused") => {
                                (Availability::ReconnectRequired, NextAction::Reconnect)
                            }
                            Some(_) => (Availability::Available, NextAction::None),
                        }
                    }
                    AgentLoginReportState::AgentLoginReportStateReady
                    | AgentLoginReportState::AgentLoginReportStateNotAuthorized
                    | AgentLoginReportState::AgentLoginReportStateRevoked => {
                        (Availability::Pending, NextAction::WaitForRunner)
                    }
                    AgentLoginReportState::AgentLoginReportStateInstallFailed
                    | AgentLoginReportState::AgentLoginReportStateStaleGeneration
                    | AgentLoginReportState::AgentLoginReportStateCloudUnavailable => {
                        (Availability::DeliveryFailed, NextAction::SyncRunner)
                    }
                    AgentLoginReportState::AgentLoginReportStateUnsupportedTarget => {
                        (Availability::DeliveryFailed, NextAction::CheckTarget)
                    }
                    AgentLoginReportState::AgentLoginReportStateForeignLogin => {
                        (Availability::NotDelivered, NextAction::ResolveServiceLogin)
                    }
                },
            }
        }
    };
    CredentialStatus {
        availability,
        next_action,
    }
}

/// The status of a provider grant, by its state (REQ-GRANT-3).
pub(super) const fn grant_status(grant: &Grant) -> CredentialStatus {
    let (availability, next_action) = match grant.state {
        GrantState::ProviderGrantStateActive => (Availability::Available, NextAction::None),
        GrantState::ProviderGrantStateReconnectRequired => {
            (Availability::ReconnectRequired, NextAction::Reconnect)
        }
        GrantState::ProviderGrantStateRevoked => (Availability::NotConnected, NextAction::Connect),
    };
    CredentialStatus {
        availability,
        next_action,
    }
}

/// The status of a provider the member has nothing unrevoked for.
pub(super) const NOT_CONNECTED: CredentialStatus = CredentialStatus {
    availability: Availability::NotConnected,
    next_action: NextAction::Connect,
};

/// What a credential is, as far as its next action's step names it.
#[derive(Clone, Copy)]
pub(in crate::cli::auth) enum Subject<'a> {
    /// A provider grant, or a grant provider the member has none for, by its provider.
    Grant { provider: &'a str },
    /// An agent login, by its ID.
    AgentLogin { id: &'a str },
    /// A coding agent the member has no agent login for.
    CodingAgent { provider: &'a str },
}

/// A grant provider's name on the wire.
pub(in crate::cli::auth) const fn grant_provider(grant: &Grant) -> &'static str {
    match grant.provider {
        provider_grant::Provider::ProviderGrantProviderGithub => "github",
        provider_grant::Provider::ProviderGrantProviderGitlabCom => "gitlab.com",
        provider_grant::Provider::ProviderGrantProviderLinear => "linear",
    }
}

/// A coding agent's name on the wire.
pub(in crate::cli::auth) const fn agent_login_provider(
    provider: AgentLoginProvider,
) -> &'static str {
    match provider {
        AgentLoginProvider::AgentLoginProviderClaudeCode => "claude_code",
        AgentLoginProvider::AgentLoginProviderCodex => "codex",
    }
}

/// A credential's status with the command or step its next action names, which only the text
/// form prints (REQ-AGENTLOGIN-43).
#[derive(Serialize)]
pub(in crate::cli::auth) struct MemberStatus {
    #[serde(flatten)]
    status: CredentialStatus,
    #[serde(skip)]
    step: Option<String>,
}

impl MemberStatus {
    pub(in crate::cli::auth) fn new(status: CredentialStatus, subject: Subject<'_>) -> Self {
        Self {
            status,
            step: member_step(status.next_action, subject),
        }
    }

    /// The availability and the next action, a line each.
    pub(in crate::cli::auth) fn write_human(
        &self,
        output: &mut impl Write,
        indent: &str,
    ) -> io::Result<()> {
        writeln!(
            output,
            "{indent}availability: {}",
            self.status.availability.as_str()
        )?;
        let next_action = self.status.next_action.as_str();
        match &self.step {
            Some(step) => writeln!(output, "{indent}next action: {next_action}: {step}"),
            None => writeln!(output, "{indent}next action: {next_action}"),
        }
    }
}

/// The command or step a member's next action names, each a command the CLI has. Waiting for a
/// runner names none.
fn member_step(next_action: NextAction, subject: Subject<'_>) -> Option<String> {
    match (next_action, subject) {
        (NextAction::Reconnect, Subject::AgentLogin { id }) => Some(format!(
            "store a new sign-in with `um auth agent-login reconnect {id}`"
        )),
        (NextAction::Reconnect, Subject::Grant { provider }) => Some(format!(
            "consent again with `um auth grant create {provider}`"
        )),
        (NextAction::Connect, Subject::Grant { provider }) => Some(format!(
            "consent with `um auth grant create {provider}`"
        )),
        (NextAction::Connect, Subject::CodingAgent { provider }) => Some(format!(
            "import a login with `um auth agent-login import ORGANIZATION {provider} {}`",
            if provider == "codex" {
                "--login"
            } else {
                "--token-file PATH"
            }
        )),
        (NextAction::AuthorizeRunner, Subject::AgentLogin { id }) => Some(format!(
            "authorize a runner with `um auth agent-login authorize {id} --runner RUNNER_ID --yes`"
        )),
        (NextAction::SyncRunner, _) => Some(
            "on the runner's host, set agent logins up again with `um runner agent-login sync --config PATH`"
                .to_owned(),
        ),
        (NextAction::CheckTarget, _) => Some(
            "the deployment does not serve this coding agent for the runner's operating system"
                .to_owned(),
        ),
        (NextAction::ResolveServiceLogin, _) => Some(
            "the runner's service uses its own login; remove it from the service environment to use the agent login"
                .to_owned(),
        ),
        (NextAction::RestoreMembership, _) => Some(
            "the agent login's owner must be an active member of the organization again".to_owned(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use um_api::{AgentLogin, Grant};
    use um_support::{Availability, CredentialStatus, NextAction};

    use super::{authorization_status, grant_status};

    const fn status(availability: Availability, next_action: NextAction) -> CredentialStatus {
        CredentialStatus {
            availability,
            next_action,
        }
    }

    // An agent login at generation 3 whose one authorization in force carries `report`.
    fn login(state: &str, report: Option<serde_json::Value>) -> AgentLogin {
        let mut authorization = serde_json::json!({
            "id": "aga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "createdAt": "2026-11-01T09:05:00Z"
        });
        if let Some(report) = report {
            authorization["report"] = report;
        }
        serde_json::from_value(serde_json::json!({
            "id": "agl_01k0z6r1w8f4jy2m7q9v3x5abc",
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "provider": "codex",
            "state": state,
            "generation": 3,
            "recordSchemaVersion": 1,
            "keyVersion": 1,
            "createdAt": "2026-11-01T09:00:00Z",
            "importedAt": "2026-11-02T10:00:00Z",
            "authorizations": [authorization]
        }))
        .unwrap()
    }

    fn report(state: &str, generation: Option<i64>, last_use: Option<&str>) -> serde_json::Value {
        let mut report = serde_json::json!({
            "state": state,
            "reportedAt": "2026-11-02T10:02:00Z"
        });
        if let Some(generation) = generation {
            report["generation"] = generation.into();
        }
        if let Some(last_use) = last_use {
            report["lastUse"] = last_use.into();
        }
        report
    }

    // REQ-AGENTLOGIN-43: the owner's view of an authorization and of an agent login.
    #[test]
    fn an_authorization_maps_cloud_facts_to_the_owners_column() {
        use Availability::{
            Available, DeliveryFailed, NotConnected, NotDelivered, Pending, ReconnectRequired,
            Unverified,
        };
        use NextAction::{
            AuthorizeRunner, CheckTarget, Reconnect, ResolveServiceLogin, SyncRunner, WaitForRunner,
        };
        let cases = [
            (None, status(Pending, WaitForRunner)),
            (
                Some(report("ready", Some(2), Some("accepted"))),
                status(Pending, WaitForRunner),
            ),
            (
                Some(report("ready", None, None)),
                status(Pending, WaitForRunner),
            ),
            (
                Some(report("not_authorized", None, None)),
                status(Pending, WaitForRunner),
            ),
            (
                Some(report("revoked", None, None)),
                status(Pending, WaitForRunner),
            ),
            (
                Some(report("ready", Some(3), Some("accepted"))),
                status(Available, NextAction::None),
            ),
            (
                Some(report("ready", Some(3), None)),
                status(Unverified, NextAction::None),
            ),
            (
                Some(report("ready", Some(3), Some("refused"))),
                status(ReconnectRequired, Reconnect),
            ),
            (
                Some(report("install_failed", Some(2), None)),
                status(DeliveryFailed, SyncRunner),
            ),
            (
                Some(report("stale_generation", Some(2), None)),
                status(DeliveryFailed, SyncRunner),
            ),
            (
                Some(report("cloud_unavailable", None, None)),
                status(DeliveryFailed, SyncRunner),
            ),
            (
                Some(report("unsupported_target", None, None)),
                status(DeliveryFailed, CheckTarget),
            ),
            (
                Some(report("foreign_login", None, None)),
                status(NotDelivered, ResolveServiceLogin),
            ),
        ];
        for (report, expected) in cases {
            let login = login("active", report.clone());
            assert_eq!(
                authorization_status(&login, login.authorizations.first()),
                expected,
                "{report:?}"
            );
        }

        let active = login("active", None);
        assert_eq!(
            authorization_status(&active, None),
            status(NotConnected, AuthorizeRunner)
        );
        let revoked = login("revoked", None);
        assert_eq!(
            authorization_status(&revoked, None),
            status(NotConnected, NextAction::None)
        );
    }

    // REQ-AGENTLOGIN-43: the grant column.
    #[test]
    fn a_grant_maps_its_state_to_the_grant_column() {
        for (state, expected) in [
            ("active", status(Availability::Available, NextAction::None)),
            (
                "reconnect_required",
                status(Availability::ReconnectRequired, NextAction::Reconnect),
            ),
            (
                "revoked",
                status(Availability::NotConnected, NextAction::Connect),
            ),
        ] {
            let grant: Grant = serde_json::from_value(serde_json::json!({
                "id": "grt_01k0z6r1w8f4jy2m7q9v3x5abc",
                "identityId": "idn_01k0z6r1w8f4jy2m7q9v3x5abc",
                "provider": "github",
                "scopes": [],
                "state": state,
                "createdAt": "2026-11-01T09:00:00Z"
            }))
            .unwrap();
            assert_eq!(grant_status(&grant), expected, "{state}");
        }
    }
}
