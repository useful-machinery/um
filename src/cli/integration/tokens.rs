//! Reading an owner's integration tokens: the one a name names, for `connect` and for a leaf
//! given a token service's name, every token of one owner, which `authorize --all` ranges over,
//! and every token `useful integration list` prints, the member's and their organizations', with
//! who connected an organization's token named from its directory (REQ-INTEGRATION-18 to
//! REQ-INTEGRATION-20). The list's memberships are read here, once, for the tokens and for the
//! connections that follow them (REQ-INTEGRATION-22).

use std::collections::BTreeMap;
use std::io::{self, Write};

use serde::{Serialize, Serializer};

use super::output::{self, TokenView};
use crate::cli::grant::{self, organization};
use crate::exit_code::ExitCode;
use useful_api::{
    ActiveMembership, IntegrationToken, IntegrationTokenOwner, MEMBER_GRANT_PAGE, MemberGrantApi,
    MemberGrantFailure,
};

/// Every unrevoked token of `owner`, read to the last page.
pub(super) fn every(
    api: &MemberGrantApi,
    owner: IntegrationTokenOwner<'_>,
) -> Result<Vec<IntegrationToken>, MemberGrantFailure> {
    grant::every_page(invalid(), |cursor| {
        api.integration_tokens(owner, Some(MEMBER_GRANT_PAGE), cursor)
            .map(|page| (page.items, page.next_cursor))
    })
}

/// The owner's unrevoked token of `provider`, read from every page of the owner's tokens.
pub(super) fn held(
    api: &MemberGrantApi,
    owner: IntegrationTokenOwner<'_>,
    provider: &str,
) -> Result<Option<IntegrationToken>, MemberGrantFailure> {
    Ok(every(api, owner)?
        .into_iter()
        .find(|token| token.provider == provider))
}

/// Whether the deployment serves a token service, which is when a token list is read unasked:
/// one that serves none, or predates the served read, is sent what the grant commands send.
pub(super) fn served(api: &MemberGrantApi) -> Result<bool, MemberGrantFailure> {
    Ok(api
        .served_integrations()?
        .is_some_and(|served| !served.token_services.is_empty()))
}

/// A token as a leaf prints it: the record, the slug of its organization where a read gave one,
/// and the name the organization's directory gives who connected it.
pub(in crate::cli) struct ListedToken {
    token: IntegrationToken,
    slug: Option<String>,
    connected_by_name: Option<String>,
}

impl ListedToken {
    /// The member's own token, which they connected: no directory names them.
    fn member(token: IntegrationToken) -> Self {
        Self {
            token,
            slug: None,
            connected_by_name: None,
        }
    }

    fn view(&self) -> TokenView<'_> {
        TokenView::new(
            &self.token,
            self.slug.as_deref(),
            self.connected_by_name.as_deref(),
        )
    }

    /// Reports the token `useful integration list INTEGRATION` names, as the one document or a
    /// report (REQ-INTEGRATION-19).
    pub(in crate::cli) fn write_shown(
        &self,
        deployment: &str,
        json: bool,
    ) -> anyhow::Result<ExitCode> {
        output::write_shown(deployment, self.view(), json)
    }
}

/// The tokens `useful integration list` read, in the order it prints them.
#[derive(Default)]
pub(in crate::cli) struct Tokens(Vec<ListedToken>);

impl Tokens {
    /// How many tokens were read.
    pub(super) fn count(&self) -> usize {
        self.0.len()
    }

    /// Writes each token's section of a human report.
    pub(super) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        for token in &self.0 {
            writeln!(output, "\n── token ──")?;
            token.view().write_human(output)?;
        }
        Ok(())
    }
}

impl Serialize for Tokens {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(ListedToken::view))
    }
}

/// The tokens `useful integration list` prints, the member's and then those of each
/// organization they are an active member of in the order the membership read names them, and
/// that read's memberships, which are read whatever the deployment serves. Where `served` is
/// false the deployment serves no token service and no token list is read.
pub(super) fn every_side(
    api: &MemberGrantApi,
    served: bool,
) -> Result<(Tokens, Vec<ActiveMembership>), MemberGrantFailure> {
    if !served {
        return Ok((Tokens::default(), organization::memberships(api)?));
    }
    let mut listed: Vec<_> = every(api, IntegrationTokenOwner::Member)?
        .into_iter()
        .map(ListedToken::member)
        .collect();
    let memberships = organization::memberships(api)?;
    for membership in &memberships {
        listed.extend(of_organization(
            api,
            &membership.organization_id,
            Some(&membership.slug),
        )?);
    }
    Ok((Tokens(listed), memberships))
}

/// The tokens of the organization `reference` names, an ID or a slug, which `list --org`
/// prints alone.
pub(super) fn organization_side(
    api: &MemberGrantApi,
    reference: &str,
) -> Result<Tokens, MemberGrantFailure> {
    of_organization(api, reference, Some(reference)).map(Tokens)
}

/// `token` as a leaf shows it: an organization's with who connected it named from the
/// directory of the organization `org` names, and the member's as it is.
pub(super) fn shown(
    api: &MemberGrantApi,
    token: &IntegrationToken,
    org: Option<&str>,
) -> ListedToken {
    match org {
        None => ListedToken::member(token.clone()),
        Some(org) => {
            let names = directory(api, org);
            named(token.clone(), Some(org), &names)
        }
    }
}

fn of_organization(
    api: &MemberGrantApi,
    reference: &str,
    slug: Option<&str>,
) -> Result<Vec<ListedToken>, MemberGrantFailure> {
    let tokens = every(api, IntegrationTokenOwner::Organization(reference))?;
    // The directory is read only for an organization that holds a token.
    let names = if tokens.is_empty() {
        BTreeMap::new()
    } else {
        directory(api, reference)
    };
    Ok(tokens
        .into_iter()
        .map(|token| named(token, slug, &names))
        .collect())
}

fn named(
    token: IntegrationToken,
    slug: Option<&str>,
    names: &BTreeMap<String, String>,
) -> ListedToken {
    ListedToken {
        connected_by_name: names.get(&token.connected_by).cloned(),
        slug: slug.map(str::to_owned),
        token,
    }
}

/// The display name the directory of the organization `reference` names gives each principal,
/// by ID, read to the last page. A read that fails names nobody, so each principal is shown by
/// ID.
fn directory(api: &MemberGrantApi, reference: &str) -> BTreeMap<String, String> {
    grant::every_page(invalid(), |cursor| {
        api.organization_directory(reference, Some(MEMBER_GRANT_PAGE), cursor)
    })
    .map(|entries| {
        entries
            .into_iter()
            .filter_map(|(principal, name)| Some((principal, name?)))
            .collect()
    })
    .unwrap_or_default()
}

const fn invalid() -> MemberGrantFailure {
    MemberGrantFailure::InvalidResponse {
        credential_rejected: false,
    }
}
