use std::collections::BTreeMap;
use std::io::{self, Write as _};

use clap::Args;

use crate::exit_code::ExitCode;
use um_api::{
    CommonIdentityFailure, GrantApi, GrantFailure, GrantUseList, HttpTransportPolicy,
    IdentityApiError, ListIdentitiesOutcome, MemberGrant, MemberGrantApi, MemberGrantFailure,
    MemberGrantKind, list_identities,
};
use um_human_auth::Deployment;

use super::output::{self, Accounts};
use super::{GRANT_HELP, GrantArg, Options, Resolved, every_grant, every_page, resolve, unrevoked};

pub(super) const ABOUT: &str = "List your grants with where each may run, or show one";

// The largest page the identity list serves.
const IDENTITY_PAGE: u16 = 200;

// The tokens issued from an OAuth grant that its detail shows unless `--uses` names another
// number (REQ-GRANT-64).
const USES_SHOWN: u16 = 20;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "GRANT", value_parser = super::parse_grant, help = GRANT_HELP)]
    grant: Option<GrantArg>,

    #[arg(
        long = "uses",
        value_name = "LIMIT",
        requires = "grant",
        value_parser = clap::value_parser!(u16).range(1..=200),
        help = "Show the newest LIMIT tokens issued from an OAuth grant (1-200) [default: 20]"
    )]
    uses: Option<u16>,

    #[command(flatten)]
    options: Options,
}

/// What the leaf read: every grant of the member, or the one `GRANT` names.
enum Read {
    Every(Vec<MemberGrant>),
    One(Resolved),
}

impl Read {
    fn grants(&self) -> &[MemberGrant] {
        match self {
            Self::Every(grants) => grants,
            Self::One(Resolved::Grant(grant)) => std::slice::from_ref(grant),
            Self::One(Resolved::Ambiguous { .. }) => &[],
        }
    }
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let json = self.options.json;
        let read = super::with_api(deployment, &self.options, |api| {
            let read = match &self.grant {
                None => {
                    let mut grants: Vec<_> =
                        every_grant(api)?.into_iter().filter(unrevoked).collect();
                    // Cloud lists OAuth grants first; the order is kept whatever it sends.
                    grants.sort_by_key(|grant| grant.kind != MemberGrantKind::GrantKindOAuth);
                    Read::Every(grants)
                }
                Some(grant) => Read::One(resolve(api, grant)?),
            };
            let organizations = organization_slugs(api, read.grants());
            Ok((read, organizations))
        })?;
        let api_url = deployment.fingerprint().api_url();
        let (read, organizations) = match read {
            Ok(read) => read,
            Err(failure) => return output::failure(api_url, &failure, json),
        };
        let uses = match &read {
            Read::One(Resolved::Grant(grant)) if grant.kind == MemberGrantKind::GrantKindOAuth => {
                let limit = self.uses.unwrap_or(USES_SHOWN);
                match read_uses(deployment, &self.options, &grant.id, limit)? {
                    Ok(page) => Some(page),
                    Err(failure) => return output::failure(api_url, &failure, json),
                }
            }
            // A native grant issues no token, so Cloud records no use of it (REQ-GRANT-62).
            _ => None,
        };
        let identities = if read
            .grants()
            .iter()
            .any(|grant| grant.kind == MemberGrantKind::GrantKindOAuth)
        {
            identity_names(deployment, self.options.http.transport_policy())?
        } else {
            BTreeMap::new()
        };
        let accounts = Accounts {
            identities,
            organizations,
        };
        match &read {
            Read::Every(grants) => output::write_list(api_url, grants, &accounts, json),
            Read::One(Resolved::Grant(grant)) => output::write_grant(
                api_url,
                grant,
                &accounts,
                uses.as_ref().map(|page| page.items.as_slice()),
                json,
            ),
            Read::One(Resolved::Ambiguous { provider, ids }) => output::write_ambiguous(
                api_url,
                provider,
                ids,
                |id| format!("um grant list {id}"),
                json,
            ),
        }
    }
}

/// The newest `limit` tokens issued from the OAuth grant `grant_id`, newest first: one page,
/// since the uses grow with every assignment (REQ-GRANT-8). A failed read is reported as a
/// failed read of the grant.
fn read_uses(
    deployment: &Deployment,
    options: &Options,
    grant_id: &str,
    limit: u16,
) -> anyhow::Result<Result<GrantUseList, MemberGrantFailure>> {
    let page = super::super::with_human_session_api(
        deployment,
        options.http.transport_policy(),
        |api: &GrantApi| api.uses(grant_id, Some(i32::from(limit)), None),
    )?;
    Ok(page.map_err(|failure| match failure {
        GrantFailure::Unauthenticated => MemberGrantFailure::Unauthenticated,
        GrantFailure::Forbidden => MemberGrantFailure::Forbidden,
        GrantFailure::InvalidInput => MemberGrantFailure::InvalidInput,
        GrantFailure::NotFound => MemberGrantFailure::GrantNotFound,
        GrantFailure::IdempotencyConflict => MemberGrantFailure::IdempotencyConflict,
        GrantFailure::ProviderUnavailable => MemberGrantFailure::ProviderUnavailable,
        GrantFailure::RateLimited { retry_after } => {
            MemberGrantFailure::RateLimited { retry_after }
        }
        GrantFailure::Unreachable { category } => MemberGrantFailure::Unreachable { category },
        GrantFailure::InvalidResponse {
            credential_rejected,
        } => MemberGrantFailure::InvalidResponse {
            credential_rejected,
        },
        // The uses route starts no consent, so this answer is not one it defines.
        GrantFailure::ConsentInProgress => MemberGrantFailure::InvalidResponse {
            credential_rejected: false,
        },
    }))
}

/// The slug of each organization a native grant of `grants` is for, where the member's
/// organization read resolves it; the grant's account is printed by ID otherwise.
fn organization_slugs(api: &MemberGrantApi, grants: &[MemberGrant]) -> BTreeMap<String, String> {
    let mut slugs = BTreeMap::new();
    for organization_id in grants
        .iter()
        .filter(|grant| grant.kind == MemberGrantKind::GrantKindNative)
        .filter_map(|grant| grant.organization_id.as_deref())
    {
        if slugs.contains_key(organization_id) {
            continue;
        }
        if let Ok(slug) = api.organization_slug(organization_id) {
            slugs.insert(organization_id.to_owned(), slug);
        }
    }
    slugs
}

/// Each of the member's linked identities by ID, named as `um auth identity list` shows it: by
/// the email the identity asserted, or else by its subject. A failed read names none, so each
/// OAuth grant's account is then printed by ID, and says so on standard error.
fn identity_names(
    deployment: &Deployment,
    policy: HttpTransportPolicy,
) -> anyhow::Result<BTreeMap<String, String>> {
    let identities = every_page(IdentityReadFailure::RepeatedCursor, |cursor| {
        match super::super::execute_with_human_credential(
            deployment,
            policy,
            "prepare identity networking",
            "contact identity API at",
            |client, api_url, access_token| {
                list_identities(client, api_url, access_token, Some(IDENTITY_PAGE), cursor)
            },
        ) {
            Ok(ListIdentitiesOutcome::Listed(page)) => Ok((page.items, page.next_cursor)),
            Ok(ListIdentitiesOutcome::Common(failure)) => {
                Err(IdentityReadFailure::Refused(failure))
            }
            Err(error) => Err(IdentityReadFailure::Request(error)),
        }
    });
    let identities = match identities {
        Ok(identities) => identities,
        Err(failure) => {
            writeln!(
                io::stderr().lock(),
                "warning: {}\n\nEach OAuth grant's account is shown by its identity ID instead.",
                failure.diagnostic(deployment.fingerprint().api_url()),
            )?;
            Vec::new()
        }
    };
    Ok(identities
        .into_iter()
        .map(|identity| {
            let name = identity.asserted_email.unwrap_or(identity.subject);
            (identity.id, name)
        })
        .collect())
}

/// Why the member's linked identities could not be read.
enum IdentityReadFailure {
    /// The request did not complete: no session, no client, or no answer the API defines.
    Request(anyhow::Error),
    /// The identity API answered with a failure.
    Refused(CommonIdentityFailure),
    /// A page named a next cursor already followed.
    RepeatedCursor,
}

impl IdentityReadFailure {
    /// The diagnostic line, without its `warning:` prefix.
    fn diagnostic(&self, api_url: &str) -> String {
        let reason = match self {
            Self::Request(error) => return format!("read linked identities: {error:#}"),
            Self::Refused(CommonIdentityFailure::Unreachable(category)) => {
                return format!(
                    "read linked identities: contact {api_url}: {}",
                    category.as_str()
                );
            }
            Self::Refused(CommonIdentityFailure::Unauthenticated) => "session rejected",
            Self::Refused(CommonIdentityFailure::Forbidden) => "forbidden",
            Self::Refused(CommonIdentityFailure::InvalidInput) => "request rejected",
            Self::RepeatedCursor => "response repeats a page cursor",
        };
        format!("read linked identities from {api_url}: {reason}")
    }
}

impl super::super::HumanCredentialOutcome for ListIdentitiesOutcome {
    type Error = IdentityApiError;

    fn unauthenticated() -> Self {
        Self::Common(CommonIdentityFailure::Unauthenticated)
    }

    fn unreachable(category: um_api::UnreachableCategory) -> Self {
        Self::Common(CommonIdentityFailure::Unreachable(category))
    }

    fn is_unauthenticated(&self) -> bool {
        matches!(self, Self::Common(CommonIdentityFailure::Unauthenticated))
    }

    fn credential_rejected(error: &IdentityApiError) -> bool {
        error.credential_rejected()
    }
}
