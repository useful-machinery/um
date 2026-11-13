//! The grants read of `um runner doctor --workflow --grants` (REQ-CREDREADY-22,
//! REQ-CREDREADY-23): the signed-in member's own unrevoked grants, each requirement shown
//! beside what the member holds of its provider, with the availability and next action of the
//! member's projection (REQ-GRANT-53) on each runner a grant is authorized for, or on the one
//! `--runner-id` names. It lives in the command because `um-runner` reaches neither the API nor
//! the human session, and it changes no requirement's state or code (REQ-CREDREADY-18).

use std::io::{self, Write};

use serde::Serialize;

use um_api::{HttpTransportPolicy, MemberGrant, MemberGrantFailure, MemberGrantKind};
use um_human_auth::Deployment;
use um_runner::CredentialKind;

use super::super::super::grant;
use super::super::super::grant::projection::{
    MemberStatus, Provider, grant_authorization_status, grant_subject,
};

/// What `--grants` asks: the organization whose coding agent grants alone are kept, the runner
/// each grant's status is reported on, and how to reach Cloud.
pub(super) struct Request<'a> {
    pub(super) organization: Option<&'a str>,
    pub(super) runner: Option<&'a str>,
    pub(super) policy: HttpTransportPolicy,
}

/// The member's unrevoked grants, as read for one invocation, and the runner `--runner-id`
/// named.
pub(super) struct Held<'a> {
    grants: Vec<MemberGrant>,
    runner: Option<&'a str>,
}

impl<'a> Request<'a> {
    /// Reads the member's grants with the human session, or the failure that ends the command
    /// as it ends `um grant list` (`grant::output::failure`), with no report.
    pub(super) fn read(
        &self,
        deployment: &Deployment,
    ) -> anyhow::Result<Result<Held<'a>, MemberGrantFailure>> {
        Ok(
            grant::held(deployment, self.policy, self.organization)?.map(|grants| Held {
                grants,
                runner: self.runner,
            }),
        )
    }
}

impl Held<'_> {
    /// What the member holds of the provider `kind` names.
    pub(super) fn of(&self, kind: CredentialKind) -> ProviderView<'_> {
        let provider = Provider::of_kind(kind);
        let grants: Vec<_> = self
            .grants
            .iter()
            .filter(|grant| grant.provider.as_str() == provider.name())
            .map(|grant| GrantView::new(grant, self.runner))
            .collect();
        ProviderView {
            provider: provider.name(),
            status: grants.is_empty().then(|| provider.not_connected()),
            grants,
        }
    }
}

/// What the member holds of one provider: each unrevoked grant, or, holding none, the status
/// that names how to make one.
#[derive(Serialize)]
pub(super) struct ProviderView<'a> {
    provider: &'static str,
    grants: Vec<GrantView<'a>>,
    #[serde(flatten)]
    status: Option<MemberStatus>,
}

/// A grant as the doctor shows it: the fields of `um grant list`'s view that say which grant
/// it is and whose account, and its status on each runner reported (REQ-GRANT-64).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GrantView<'a> {
    id: &'a str,
    state: &'static str,
    generation: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identity_id: Option<&'a str>,
    authorizations: Vec<RunnerView<'a>>,
    // The grant's own availability and next action when no authorization is reported.
    #[serde(flatten)]
    status: Option<MemberStatus>,
}

/// A grant's status on one runner it is authorized for.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunnerView<'a> {
    runner_id: &'a str,
    #[serde(flatten)]
    status: MemberStatus,
}

impl<'a> GrantView<'a> {
    /// The grant's authorizations in force, only that of `runner` when one is named, and the
    /// grant's own status when none is reported (REQ-CREDREADY-23).
    fn new(grant: &'a MemberGrant, runner: Option<&str>) -> Self {
        let subject = grant_subject(grant);
        let authorizations: Vec<_> = grant
            .authorizations
            .iter()
            .filter(|authorization| runner.is_none_or(|runner| authorization.runner_id == runner))
            .map(|authorization| RunnerView {
                runner_id: &authorization.runner_id,
                status: MemberStatus::new(
                    grant_authorization_status(grant, Some(authorization)),
                    subject,
                ),
            })
            .collect();
        let (organization_id, identity_id) = match grant.kind {
            MemberGrantKind::GrantKindOAuth => (None, grant.identity_id.as_deref()),
            MemberGrantKind::GrantKindNative => (grant.organization_id.as_deref(), None),
        };
        Self {
            id: &grant.id,
            state: grant.state.as_str(),
            generation: grant.generation,
            organization_id,
            identity_id,
            status: authorizations
                .is_empty()
                .then(|| MemberStatus::new(grant_authorization_status(grant, None), subject)),
            authorizations,
        }
    }
}

impl ProviderView<'_> {
    /// The grants beneath a requirement, each next action with the command it names.
    pub(super) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "  grants of {}:", self.provider)?;
        if let Some(status) = &self.status {
            writeln!(output, "    none held")?;
            status.write_human(output, "    ")?;
        }
        for grant in &self.grants {
            write!(
                output,
                "    grant {} · {} · generation {}",
                grant.id, grant.state, grant.generation
            )?;
            match (grant.identity_id, grant.organization_id) {
                (Some(id), _) => writeln!(output, " · identity {id}")?,
                (None, Some(id)) => writeln!(output, " · organization {id}")?,
                (None, None) => writeln!(output)?,
            }
            for authorization in &grant.authorizations {
                writeln!(output, "      runner {}", authorization.runner_id)?;
                authorization.status.write_human(output, "        ")?;
            }
            if let Some(status) = &grant.status {
                status.write_human(output, "      ")?;
            }
        }
        Ok(())
    }
}
