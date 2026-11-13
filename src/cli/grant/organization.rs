//! The one organization a native grant `um grant create` makes is for (REQ-GRANT-72): the one
//! `--organization` names, or else the member's only active membership.

use anyhow::anyhow;

use um_api::{MemberGrantApi, MemberGrantFailure};

// The largest page the membership list serves.
const PAGE: i32 = 200;

/// An organization of the member's, by ID, with its slug when a read named it.
pub(super) struct Organization {
    pub(super) id: String,
    pub(super) slug: Option<String>,
}

/// Resolves the organization: an ID as given, a slug to the ID of the organization it names
/// as the earlier agent-login import resolves one, and without `named` the one organization the
/// member is an active member of. The inner error refuses the command, naming the option and
/// the member's organizations, when there is not exactly one: `providers` names the grants it
/// is for and `example` the command line that names one.
pub(super) fn resolve(
    api: &MemberGrantApi,
    named: Option<&str>,
    providers: &str,
    example: &str,
) -> Result<Result<Organization, anyhow::Error>, MemberGrantFailure> {
    if let Some(named) = named {
        return self::named(api, named).map(Ok);
    }
    let invalid = MemberGrantFailure::InvalidResponse {
        credential_rejected: false,
    };
    let mut organizations = super::every_page(invalid, |cursor| {
        api.active_organizations(Some(PAGE), cursor)
    })?;
    if organizations.len() == 1 {
        let (id, slug) = organizations.remove(0);
        return Ok(Ok(Organization {
            id,
            slug: Some(slug),
        }));
    }
    Ok(Err(if organizations.is_empty() {
        anyhow!(
            "a {providers} grant is made for one organization, and you are an active member of none\n\n\
             Join an organization, then name it:\n  {example}"
        )
    } else {
        let slugs = organizations
            .iter()
            .map(|(_, slug)| slug.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        anyhow!(
            "a {providers} grant is made for one organization, and you are a member of several: \
             {slugs}\n\nName the one it is for with --organization:\n  {example}"
        )
    }))
}

/// The organization `named` names: an ID as given, a slug to the ID of the organization it
/// names among the member's.
pub(super) fn named(api: &MemberGrantApi, named: &str) -> Result<Organization, MemberGrantFailure> {
    if um_support::valid_typed_id(named, "org_") {
        return Ok(Organization {
            id: named.to_owned(),
            slug: None,
        });
    }
    api.organization_id(named).map(|id| Organization {
        id,
        slug: Some(named.to_owned()),
    })
}
