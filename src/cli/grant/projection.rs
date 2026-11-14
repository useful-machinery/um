//! The member's view of a credential (REQ-GRANT-53): what Cloud's API returns for a grant's
//! authorization, for an agent login's and for a provider grant, mapped to one availability
//! and one next action, with the command or step that next action names. `um grant list` and
//! the earlier commands it replaces print it; Cloud's API carries the facts only.

use std::io::{self, Write};

use serde::Serialize;

use um_api::{
    AgentLogin, AgentLoginAuthorization, AgentLoginProvider, AgentLoginReportState,
    AgentLoginState, Grant, GrantReport, GrantState, MemberGrant, MemberGrantAuthorization,
    MemberGrantKind, MemberGrantState, provider_grant,
};
use um_runner::CredentialKind;
use um_support::{Availability, CredentialStatus, NextAction};

/// The status of an agent login's authorization in force, or of the agent login when it has
/// none (`authorization` is `None`), as its owner reads it.
pub(in crate::cli) fn authorization_status(
    login: &AgentLogin,
    authorization: Option<&AgentLoginAuthorization>,
) -> CredentialStatus {
    native_status(
        login.state == AgentLoginState::AgentLoginStateActive,
        login.generation,
        authorization.map(|authorization| authorization.report.as_deref()),
    )
}

/// The status of a grant's authorization in force, or of the grant when it has none
/// (`authorization` is `None`), as its owner reads it. An OAuth grant is `available` on each
/// runner it is authorized for while it is `active`; Cloud issues its tokens per run.
pub(in crate::cli) fn grant_authorization_status(
    grant: &MemberGrant,
    authorization: Option<&MemberGrantAuthorization>,
) -> CredentialStatus {
    let active = grant.state == MemberGrantState::GrantStateActive;
    if grant.kind == MemberGrantKind::GrantKindNative {
        return native_status(
            active,
            grant.generation,
            authorization.map(|authorization| authorization.report.as_deref()),
        );
    }
    let (availability, next_action) = match (grant.state, authorization) {
        (MemberGrantState::GrantStateRevoked, _) => (Availability::NotConnected, NextAction::None),
        (MemberGrantState::GrantStateReconnectRequired, _) => {
            (Availability::ReconnectRequired, NextAction::Reconnect)
        }
        (MemberGrantState::GrantStateActive, None) => {
            (Availability::NotConnected, NextAction::AuthorizeRunner)
        }
        (MemberGrantState::GrantStateActive, Some(_)) => {
            (Availability::Available, NextAction::None)
        }
    };
    CredentialStatus {
        availability,
        next_action,
    }
}

/// What a grant is, as far as its next action's step names it: a grant of either kind by its
/// ID and provider.
pub(in crate::cli) fn grant_subject(grant: &MemberGrant) -> Subject<'_> {
    let provider = grant.provider.as_str();
    match grant.kind {
        MemberGrantKind::GrantKindOAuth => Subject::OAuthGrant {
            id: &grant.id,
            provider,
        },
        MemberGrantKind::GrantKindNative => Subject::AgentLogin {
            id: &grant.id,
            provider,
        },
    }
}

/// The status of a native grant's authorization in force, given its runner's report if any
/// (`authorization` is `Some(report)`), or of the grant when it has none (`None`).
fn native_status(
    active: bool,
    generation: i64,
    authorization: Option<Option<&GrantReport>>,
) -> CredentialStatus {
    let (availability, next_action) = match (active, authorization) {
        (false, _) => (Availability::NotConnected, NextAction::None),
        (true, None) => (Availability::NotConnected, NextAction::AuthorizeRunner),
        (true, Some(report)) => {
            match report {
                // Cloud holds what the runner has not reported installing.
                None => (Availability::Pending, NextAction::WaitForRunner),
                Some(report) => match report.state {
                    AgentLoginReportState::AgentLoginReportStateReady
                        if report.generation == Some(generation) =>
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
pub(in crate::cli) const fn grant_status(grant: &Grant) -> CredentialStatus {
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
const NOT_CONNECTED: CredentialStatus = CredentialStatus {
    availability: Availability::NotConnected,
    next_action: NextAction::Connect,
};

/// The providers a workflow can use a credential of, grant providers first, in the order the
/// rows are printed (REQ-GRANT-55, REQ-GRANT-64).
pub(in crate::cli) const PROVIDERS: [Provider; 5] = [
    Provider::Grant("github"),
    Provider::Grant("gitlab.com"),
    Provider::Grant("linear"),
    Provider::CodingAgent("claude_code"),
    Provider::CodingAgent("codex"),
];

/// A provider by its wire name: one an OAuth grant is made for, or a coding agent.
#[derive(Clone, Copy)]
pub(in crate::cli) enum Provider {
    Grant(&'static str),
    CodingAgent(&'static str),
}

impl Provider {
    /// The provider whose grant a kind a workflow calls for names; `gitlab` is GitLab.com
    /// (REQ-GRANT-76, REQ-CREDREADY-22).
    pub(in crate::cli) const fn of_kind(kind: CredentialKind) -> Self {
        match kind {
            CredentialKind::Github => Self::Grant("github"),
            CredentialKind::Gitlab => Self::Grant("gitlab.com"),
            CredentialKind::Linear => Self::Grant("linear"),
            CredentialKind::ClaudeCode => Self::CodingAgent("claude_code"),
            CredentialKind::Codex => Self::CodingAgent("codex"),
        }
    }

    pub(in crate::cli) const fn name(self) -> &'static str {
        match self {
            Self::Grant(name) | Self::CodingAgent(name) => name,
        }
    }

    /// The status of this provider when the member has nothing unrevoked for it.
    pub(in crate::cli) fn not_connected(self) -> MemberStatus {
        let subject = match self {
            Self::Grant(provider) => Subject::Grant { provider },
            Self::CodingAgent(provider) => Subject::CodingAgent { provider },
        };
        MemberStatus::new(NOT_CONNECTED, subject)
    }
}

/// What a credential is, as far as its next action's step names it.
#[derive(Clone, Copy)]
pub(in crate::cli) enum Subject<'a> {
    /// A provider grant, or a grant provider the member has none for, by its provider.
    Grant { provider: &'a str },
    /// An OAuth grant of the member's, by its ID, which names it whatever else they hold of
    /// its provider (REQ-GRANT-65), and its provider.
    OAuthGrant { id: &'a str, provider: &'a str },
    /// An agent login, a native grant, by its ID and its coding agent.
    AgentLogin { id: &'a str, provider: &'a str },
    /// A coding agent the member has no agent login for.
    CodingAgent { provider: &'a str },
}

/// A grant provider's name on the wire.
pub(in crate::cli) const fn grant_provider(grant: &Grant) -> &'static str {
    match grant.provider {
        provider_grant::Provider::ProviderGrantProviderGithub => "github",
        provider_grant::Provider::ProviderGrantProviderGitlabCom => "gitlab.com",
        provider_grant::Provider::ProviderGrantProviderLinear => "linear",
    }
}

/// A coding agent's name on the wire.
pub(in crate::cli) const fn agent_login_provider(provider: AgentLoginProvider) -> &'static str {
    match provider {
        AgentLoginProvider::AgentLoginProviderClaudeCode => "claude_code",
        AgentLoginProvider::AgentLoginProviderCodex => "codex",
    }
}

/// A credential's status with the command or step its next action names, which only the text
/// form prints (REQ-GRANT-53).
#[derive(Serialize)]
pub(in crate::cli) struct MemberStatus {
    #[serde(flatten)]
    status: CredentialStatus,
    #[serde(skip)]
    step: Option<String>,
}

impl MemberStatus {
    pub(in crate::cli) fn new(status: CredentialStatus, subject: Subject<'_>) -> Self {
        Self {
            status,
            step: member_step(status.next_action, subject),
        }
    }

    /// The availability and the next action, a line each.
    pub(in crate::cli) fn write_human(
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

/// The command or step a member's next action names, each a `um grant` or `um runner grant`
/// command (REQ-GRANT-77). Waiting for a runner names none.
fn member_step(next_action: NextAction, subject: Subject<'_>) -> Option<String> {
    match (next_action, subject) {
        (NextAction::Reconnect, Subject::AgentLogin { provider, .. }) => Some(format!(
            "store a new sign-in with `um grant create {provider} --replace`"
        )),
        (
            NextAction::Reconnect,
            Subject::Grant { provider } | Subject::OAuthGrant { provider, .. },
        ) => Some(format!(
            "consent again with `um grant create {provider} --replace`"
        )),
        (NextAction::Connect, Subject::Grant { provider }) => {
            Some(format!("consent with `um grant create {provider}`"))
        }
        (NextAction::Connect, Subject::CodingAgent { provider }) => {
            Some(format!("sign in with `um grant create {provider}`"))
        }
        (
            NextAction::AuthorizeRunner,
            Subject::AgentLogin { id, .. } | Subject::OAuthGrant { id, .. },
        ) => Some(format!(
            "authorize a runner with `um grant authorize {id} RUNNER --yes`"
        )),
        (NextAction::SyncRunner, _) => Some(
            "on the runner's host, set grants up again with `um runner grant sync`"
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
    use um_api::{AgentLogin, Grant, MemberGrant};
    use um_support::{Availability, CredentialStatus, NextAction};

    use super::{authorization_status, grant_authorization_status, grant_status};

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

    // REQ-GRANT-53: the owner's view of an authorization and of an agent login.
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

    // REQ-GRANT-53: the grant column.
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

    // REQ-GRANT-53
    // REQ-GRANT-64
    #[test]
    fn a_grants_authorization_maps_its_kind_and_state_to_the_owners_column() {
        let authorization = serde_json::json!({
            "id": "pga_01k0z6r1w8f4jy2m7q9v3x5abc",
            "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "createdAt": "2026-11-01T09:05:00Z",
            "report": {"state": "ready", "generation": 1, "reportedAt": "2026-11-01T09:06:00Z"}
        });
        let grant = |kind: &str, state: &str| -> MemberGrant {
            serde_json::from_value(serde_json::json!({
                "id": "pgr_01k0z6r1w8f4jy2m7q9v3x5abc",
                "kind": kind,
                "provider": if kind == "oauth" { "github" } else { "codex" },
                "state": state,
                "generation": 1,
                "createdAt": "2026-11-01T09:00:00Z",
                "authorizations": [authorization.clone()]
            }))
            .unwrap()
        };
        use Availability::{Available, NotConnected, ReconnectRequired, Unverified};
        use NextAction::{AuthorizeRunner, Reconnect};
        for (kind, state, authorized, expected) in [
            ("oauth", "active", true, status(Available, NextAction::None)),
            (
                "oauth",
                "active",
                false,
                status(NotConnected, AuthorizeRunner),
            ),
            (
                "oauth",
                "reconnect_required",
                true,
                status(ReconnectRequired, Reconnect),
            ),
            (
                "oauth",
                "reconnect_required",
                false,
                status(ReconnectRequired, Reconnect),
            ),
            (
                "oauth",
                "revoked",
                false,
                status(NotConnected, NextAction::None),
            ),
            // A native grant reads its runner's report, as an agent login does.
            (
                "native",
                "active",
                true,
                status(Unverified, NextAction::None),
            ),
            (
                "native",
                "active",
                false,
                status(NotConnected, AuthorizeRunner),
            ),
            (
                "native",
                "revoked",
                false,
                status(NotConnected, NextAction::None),
            ),
        ] {
            let grant = grant(kind, state);
            let authorization = grant.authorizations.first().filter(|_| authorized);
            assert_eq!(
                grant_authorization_status(&grant, authorization),
                expected,
                "{kind} {state} {authorized}"
            );
        }
    }
}
