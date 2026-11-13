//! Settling, before any sign-in starts, whether `um grant create` may replace a grant the
//! member already holds (REQ-GRANT-73): asked on a terminal, refused without one unless
//! `--replace` is given, and for a native provider refused when several are held in the
//! organization. With `--workflow`, a grant that needs reconnecting is settled the same way,
//! and one the member keeps is passed over rather than refused (REQ-GRANT-79).

use std::io::{self, BufRead, IsTerminal, Write};

use anyhow::anyhow;

use crate::cli::CommandFailure;
use crate::exit_code::ExitCode;
use um_api::{
    MEMBER_GRANT_PAGE, MemberGrant, MemberGrantApi, MemberGrantFailure, MemberGrantProvider,
    MemberGrantState,
};
use um_support::Availability;

use super::projection;

/// The member's grants of `provider` that a new sign-in would replace: those of its kind that
/// are `active` or `reconnect_required`, and for a native provider those of the organization
/// `organization_id` names. A revoked grant is not held.
pub(super) fn held(
    api: &MemberGrantApi,
    provider: MemberGrantProvider,
    organization_id: Option<&str>,
) -> Result<Vec<MemberGrant>, MemberGrantFailure> {
    let invalid = MemberGrantFailure::InvalidResponse {
        credential_rejected: false,
    };
    let grants = super::every_page(invalid, |cursor| {
        api.list(Some(provider.kind()), Some(MEMBER_GRANT_PAGE), cursor)
            .map(|page| (page.items, page.next_cursor))
    })?;
    Ok(grants
        .into_iter()
        .filter(|grant| holds(grant, provider, organization_id))
        .collect())
}

/// Whether `grant` is one the member holds of `provider`: `active` or `reconnect_required`,
/// and for `organization_id` when that is named. A revoked grant is not held.
pub(super) fn holds(
    grant: &MemberGrant,
    provider: MemberGrantProvider,
    organization_id: Option<&str>,
) -> bool {
    grant.provider == provider
        && organization_id.is_none_or(|id| grant.organization_id.as_deref() == Some(id))
        && matches!(
            grant.state,
            MemberGrantState::GrantStateActive | MemberGrantState::GrantStateReconnectRequired
        )
}

/// Whether `grant`, one the member holds, needs reconnecting (REQ-GRANT-79): its state is
/// `reconnect_required`, or its status on a runner it is authorized for is (REQ-GRANT-53).
pub(super) fn needs_reconnecting(grant: &MemberGrant) -> bool {
    grant.state == MemberGrantState::GrantStateReconnectRequired
        || grant.authorizations.iter().any(|authorization| {
            matches!(
                projection::grant_authorization_status(grant, Some(authorization)).availability,
                Availability::ReconnectRequired
            )
        })
}

/// Proceeds when nothing is held or `replace` is given; otherwise asks on a terminal, where
/// standard input and standard error both are one, and refuses with exit 2 without one or on
/// any answer but yes.
pub(super) fn settle(
    provider: MemberGrantProvider,
    held: &[MemberGrant],
    replace: bool,
) -> Result<(), CommandFailure> {
    if held.is_empty() {
        return Ok(());
    }
    let provider = provider.as_str();
    let ids = ids(held);
    let refusal = match answer(
        || format!("You already hold a {provider} grant: {ids}"),
        replace,
    )? {
        Some(true) => return Ok(()),
        Some(false) => format!("you kept your {provider} grant {ids}"),
        None => format!("you already hold a {provider} grant: {ids}"),
    };
    Err(CommandFailure::with_exit_code(
        anyhow!(
            "{refusal}\n\nTo replace it with a new sign-in, run the command again with --replace."
        ),
        ExitCode::UsageError,
    ))
}

/// Whether to replace the grants of `provider` the member holds, every one of which needs
/// reconnecting (REQ-GRANT-79): settled as [`settle`] settles a replacement, asked on a
/// terminal and taken without one only with `replace`, but kept rather than refused.
pub(super) fn reconnects(
    provider: MemberGrantProvider,
    held: &[MemberGrant],
    replace: bool,
) -> anyhow::Result<bool> {
    let provider = provider.as_str();
    let ids = ids(held);
    Ok(answer(
        || format!("Your {provider} grant needs reconnecting: {ids}"),
        replace,
    )? == Some(true))
}

/// The IDs of `held`, as a question or a refusal names them.
fn ids(held: &[MemberGrant]) -> String {
    held.iter()
        .map(|grant| grant.id.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Yes when `replace` is given; otherwise the member's answer to `question` on a terminal,
/// where standard input and standard error both are one, and none without one.
fn answer(question: impl FnOnce() -> String, replace: bool) -> anyhow::Result<Option<bool>> {
    if replace {
        return Ok(Some(true));
    }
    if !(io::stdin().is_terminal() && io::stderr().is_terminal()) {
        return Ok(None);
    }
    ask(&question()).map(Some)
}

/// The native grant a new sign-in replaces, once `settle` allows it, or none when nothing is
/// held. Several held grants are refused with exit 2, naming them, since the command cannot
/// choose between them.
pub(super) fn native_target(
    provider: MemberGrantProvider,
    mut held: Vec<MemberGrant>,
    replace: bool,
) -> Result<Option<MemberGrant>, CommandFailure> {
    if held.len() > 1 {
        let ids = ids(&held);
        return Err(CommandFailure::with_exit_code(
            anyhow!(
                "you hold several {} grants for this organization: {ids}\n\n\
                 Revoke the ones you no longer use, then run the command again:\n  \
                 um grant revoke GRANT --yes",
                provider.as_str()
            ),
            ExitCode::UsageError,
        ));
    }
    settle(provider, &held, replace)?;
    Ok(held.pop())
}

/// Asks on standard error whether to replace the held grant `question` names and reads one
/// line of answer.
fn ask(question: &str) -> anyhow::Result<bool> {
    let mut stderr = io::stderr().lock();
    write!(stderr, "{question}\nReplace it with a new sign-in? [y/N] ")?;
    stderr.flush()?;
    let mut answer = String::new();
    io::stdin().lock().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}
