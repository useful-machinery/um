//! Settling, before any sign-in starts, whether `um grant create` may replace a grant the
//! member already holds (REQ-GRANT-73): asked on a terminal, refused without one unless
//! `--replace` is given, and for a native provider refused when several are held in the
//! organization.

use std::io::{self, BufRead, IsTerminal, Write};

use anyhow::anyhow;

use crate::cli::CommandFailure;
use crate::exit_code::ExitCode;
use um_api::{
    MEMBER_GRANT_PAGE, MemberGrant, MemberGrantApi, MemberGrantFailure, MemberGrantProvider,
    MemberGrantState,
};

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

/// Proceeds when nothing is held or `replace` is given; otherwise asks on a terminal, where
/// standard input and standard error both are one, and refuses with exit 2 without one or on
/// any answer but yes.
pub(super) fn settle(
    provider: MemberGrantProvider,
    held: &[MemberGrant],
    replace: bool,
) -> Result<(), CommandFailure> {
    if held.is_empty() || replace {
        return Ok(());
    }
    let provider = provider.as_str();
    let ids = held
        .iter()
        .map(|grant| grant.id.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let terminal = io::stdin().is_terminal() && io::stderr().is_terminal();
    let refusal = if terminal {
        if ask(provider, &ids)? {
            return Ok(());
        }
        format!("you kept your {provider} grant {ids}")
    } else {
        format!("you already hold a {provider} grant: {ids}")
    };
    Err(CommandFailure::with_exit_code(
        anyhow!(
            "{refusal}\n\nTo replace it with a new sign-in, run the command again with --replace."
        ),
        ExitCode::UsageError,
    ))
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
        let ids = held
            .iter()
            .map(|grant| grant.id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
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

/// Asks on standard error whether to replace the held grant and reads one line of answer.
fn ask(provider: &str, ids: &str) -> anyhow::Result<bool> {
    let mut stderr = io::stderr().lock();
    write!(
        stderr,
        "You already hold a {provider} grant: {ids}\nReplace it with a new sign-in? [y/N] "
    )?;
    stderr.flush()?;
    let mut answer = String::new();
    io::stdin().lock().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}
