//! The credential status vocabulary (REQ-AGENTLOGIN-43): the one availability
//! and the one next action every surface reports for a credential. Each side
//! maps its own facts to it: `um-runner` a provider's readiness, the `um`
//! binary what Cloud's API returns for an agent login's authorization and for
//! a provider grant. The vocabulary holds no provider and no state of either.

use serde::{Deserialize, Serialize};

/// Whether a credential can be used now (REQ-AGENTLOGIN-43).
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    /// In place and accepted at its last use, or a grant that is `active`.
    Available,
    /// In place and not yet used at this generation.
    Unverified,
    /// Refused at its last use, or a grant that needs a new consent.
    ReconnectRequired,
    /// Cloud holds what the runner has not reported installing.
    Pending,
    /// The runner could not install what Cloud authorized.
    DeliveryFailed,
    /// Selected, and refused by Cloud because the login's owner is not an
    /// active member.
    InsufficientPermission,
    /// Not delivered to the runner: a service login of its own is in use, or
    /// the runner is issued no token for the provider.
    NotDelivered,
    /// Nothing selected: no authorization in force, a revoked credential, or
    /// none.
    NotConnected,
}

impl Availability {
    /// Every availability, in the order REQ-AGENTLOGIN-43 states them.
    pub const ALL: [Self; 8] = [
        Self::Available,
        Self::Unverified,
        Self::ReconnectRequired,
        Self::Pending,
        Self::DeliveryFailed,
        Self::InsufficientPermission,
        Self::NotDelivered,
        Self::NotConnected,
    ];

    /// The availability's name on the wire.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unverified => "unverified",
            Self::ReconnectRequired => "reconnect_required",
            Self::Pending => "pending",
            Self::DeliveryFailed => "delivery_failed",
            Self::InsufficientPermission => "insufficient_permission",
            Self::NotDelivered => "not_delivered",
            Self::NotConnected => "not_connected",
        }
    }

    /// The next action this availability fixes, where it fixes one; `None`
    /// where the next action depends on the facts it was mapped from:
    /// `delivery_failed` (`sync_runner` or `check_target`), `not_delivered`
    /// (`none` or `resolve_service_login`) and `not_connected` (`none`,
    /// `authorize_runner` or `connect`).
    pub const fn fixed_next_action(self) -> Option<NextAction> {
        match self {
            Self::Available | Self::Unverified => Some(NextAction::None),
            Self::ReconnectRequired => Some(NextAction::Reconnect),
            Self::Pending => Some(NextAction::WaitForRunner),
            Self::InsufficientPermission => Some(NextAction::RestoreMembership),
            Self::DeliveryFailed | Self::NotDelivered | Self::NotConnected => None,
        }
    }
}

/// What to do about a credential's availability (REQ-AGENTLOGIN-43).
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NextAction {
    /// Nothing.
    None,
    /// Store a new sign-in, or consent again for a grant.
    Reconnect,
    /// Wait for the runner to install what Cloud holds and report it.
    WaitForRunner,
    /// Ask the runner to set up its agent logins again.
    SyncRunner,
    /// Check that the deployment serves the provider for the runner's
    /// operating system.
    CheckTarget,
    /// Make the login's owner an active member again.
    RestoreMembership,
    /// Remove the service's own login, or keep using it.
    ResolveServiceLogin,
    /// Authorize a runner for an agent login that has none in force.
    AuthorizeRunner,
    /// Connect a provider the member has nothing unrevoked for.
    Connect,
}

impl NextAction {
    /// Every next action, in the order REQ-AGENTLOGIN-43 states them.
    pub const ALL: [Self; 9] = [
        Self::None,
        Self::Reconnect,
        Self::WaitForRunner,
        Self::SyncRunner,
        Self::CheckTarget,
        Self::RestoreMembership,
        Self::ResolveServiceLogin,
        Self::AuthorizeRunner,
        Self::Connect,
    ];

    /// The next action's name on the wire.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Reconnect => "reconnect",
            Self::WaitForRunner => "wait_for_runner",
            Self::SyncRunner => "sync_runner",
            Self::CheckTarget => "check_target",
            Self::RestoreMembership => "restore_membership",
            Self::ResolveServiceLogin => "resolve_service_login",
            Self::AuthorizeRunner => "authorize_runner",
            Self::Connect => "connect",
        }
    }
}

/// A credential's availability with its next action, serialized as
/// `{availability, nextAction}`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CredentialStatus {
    pub availability: Availability,
    pub next_action: NextAction,
}

impl CredentialStatus {
    /// The status as one line of text: the availability and, unless it is
    /// `none`, the next action with the command or step it names.
    pub fn text(self, step: Option<&str>) -> String {
        let availability = self.availability.as_str();
        match (self.next_action, step) {
            (NextAction::None, _) => format!("availability {availability}"),
            (next_action, None) => {
                format!(
                    "availability {availability}, next action {}",
                    next_action.as_str()
                )
            }
            (next_action, Some(step)) => format!(
                "availability {availability}, next action {}: {step}",
                next_action.as_str()
            ),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    reason = "credential status unit tests use Rust assertions"
)]
mod tests {
    use super::{Availability, NextAction};

    // REQ-AGENTLOGIN-43
    #[test]
    fn wire_names_are_the_serialized_names() {
        for availability in Availability::ALL {
            assert_eq!(
                crate::wire_name(availability).as_deref(),
                Some(availability.as_str())
            );
        }
        for next_action in NextAction::ALL {
            assert_eq!(
                crate::wire_name(next_action).as_deref(),
                Some(next_action.as_str())
            );
        }
    }
}
