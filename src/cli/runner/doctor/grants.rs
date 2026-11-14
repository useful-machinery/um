//! The grants read of `um runner doctor --workflow --grants` (REQ-CREDREADY-22,
//! REQ-CREDREADY-23): the signed-in member's own unrevoked grants, each requirement shown
//! beside what the member holds of its provider, with the availability and next action of the
//! member's projection (REQ-GRANT-53) on each runner a grant is authorized for, or on the one
//! `--runner-id` names. It lives in the command because `um-runner` reaches neither the API nor
//! the human session, and it changes no requirement's state or code (REQ-CREDREADY-18) unless
//! `--require-grants` asks for a grant the named runner can use (REQ-CREDREADY-26).

use std::io::{self, Write};

use serde::Serialize;

use um_api::{HttpTransportPolicy, MemberGrant, MemberGrantFailure, MemberGrantKind};
use um_human_auth::Deployment;
use um_runner::CredentialKind;
use um_support::Availability;

use super::super::super::grant;
use super::super::super::grant::projection::{
    MemberStatus, Provider, grant_authorization_status, grant_subject,
};

/// What `--grants` asks: the organization whose coding agent grants alone are kept, the runner
/// each grant's status is reported on, whether a grant that runner cannot use fails the report,
/// and how to reach Cloud.
pub(super) struct Request<'a> {
    pub(super) organization: Option<&'a str>,
    pub(super) runner: Option<&'a str>,
    pub(super) require: bool,
    pub(super) policy: HttpTransportPolicy,
}

/// The member's unrevoked grants, as read for one invocation, the runner `--runner-id` named
/// and whether `--require-grants` asks for a grant it can use.
pub(super) struct Held<'a> {
    grants: Vec<MemberGrant>,
    runner: Option<&'a str>,
    require: bool,
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
                require: self.require,
            }),
        )
    }
}

impl Held<'_> {
    /// The member's grants of `provider`.
    fn of_provider(&self, provider: Provider) -> impl Iterator<Item = &MemberGrant> {
        self.grants
            .iter()
            .filter(move |grant| grant.provider.as_str() == provider.name())
    }

    /// What the member holds of the provider `kind` names.
    pub(super) fn of(&self, kind: CredentialKind) -> ProviderView<'_> {
        let provider = Provider::of_kind(kind);
        let grants: Vec<_> = self
            .of_provider(provider)
            .map(|grant| GrantView::new(grant, self.runner))
            .collect();
        ProviderView {
            provider: provider.name(),
            status: grants.is_empty().then(|| provider.not_connected()),
            grants,
        }
    }

    /// Why the requirement of `kind` fails when `--require-grants` asks for a grant of its
    /// provider that the named runner can use, or `None` when it does not ask or the member
    /// holds one (REQ-CREDREADY-26). A grant is usable there when its availability is
    /// `available`, `unverified` or `pending`: a runner sets a login up for each assignment it
    /// admits before offering anything (REQ-GRANT-49). Of several grants the one nearest to
    /// usable is reported, one authorized for the runner before one that is not.
    pub(super) fn shortfall(&self, kind: CredentialKind) -> Option<Shortfall> {
        let runner = self.runner.filter(|_| self.require)?;
        let provider = Provider::of_kind(kind);
        let nearest = self
            .of_provider(provider)
            .map(|grant| {
                let authorization = grant
                    .authorizations
                    .iter()
                    .find(|authorization| authorization.runner_id == runner);
                let status = grant_authorization_status(grant, authorization);
                (grant, authorization.is_some(), status)
            })
            .min_by_key(|(_, authorized, status)| (!usable(status.availability), !authorized));
        let Some((grant, _, status)) = nearest else {
            return Some(Shortfall::new(
                "grant_not_connected",
                Failing::Missing,
                &provider.not_connected(),
            ));
        };
        let (code, failing) = match status.availability {
            Availability::Available | Availability::Unverified | Availability::Pending => {
                return None;
            }
            Availability::NotConnected => ("grant_not_authorized", Failing::Missing),
            Availability::ReconnectRequired => {
                ("grant_reconnect_required", Failing::ReconnectRequired)
            }
            Availability::DeliveryFailed
            | Availability::InsufficientPermission
            | Availability::NotDelivered => ("grant_not_ready", Failing::Missing),
        };
        Some(Shortfall::new(
            code,
            failing,
            &MemberStatus::new(status, grant_subject(grant)),
        ))
    }
}

/// Whether a grant with `availability` on a runner is in place for a run's step there.
const fn usable(availability: Availability) -> bool {
    matches!(
        availability,
        Availability::Available | Availability::Unverified | Availability::Pending
    )
}

/// The failing state a grant gives a requirement (REQ-CREDREADY-17, REQ-CREDREADY-26).
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Failing {
    Missing,
    ReconnectRequired,
}

impl Failing {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::ReconnectRequired => "reconnect_required",
        }
    }
}

/// A requirement `--require-grants` fails: its state, its code and the next action of the
/// grant status that fails it, with the command it names.
pub(super) struct Shortfall {
    pub(super) code: &'static str,
    pub(super) state: Failing,
    pub(super) next_action: String,
}

impl Shortfall {
    fn new(code: &'static str, state: Failing, status: &MemberStatus) -> Self {
        Self {
            code,
            state,
            next_action: status.next_action_text(),
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
