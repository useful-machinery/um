//! An organization's connections as `useful integration` reads them: all of them, as `list`
//! shows them (REQ-INTEGRATION-22), its GitHub App installations, its Linear connections and its
//! GitLab connections, and one provider's, as `authorize` given `--project-id` chooses the one it
//! authorizes a runner for (REQ-INTEGRATION-33). Each provider's are read with the client and the
//! session of the `useful connection` leaf that lists them and stated in the word that leaf
//! prints for its state. Only named fields are kept, never a record whole. The list passes over a
//! read Cloud refuses or answers that it cannot serve; the one provider's read passes over
//! nothing.

use serde_json::Value;

use useful_api::{
    GitHubFailure, GitHubInstallation, GitLabFailure, LinearConnection, LinearFailure,
    MemberGrantFailure, UnreachableCategory,
};
use useful_human_auth::Deployment;

use super::super::{CommonArgs, HttpOptions, PrincipalAuthenticationArgs, grant};

// The largest pages the Linear and the GitLab connection lists serve.
const LINEAR_PAGE: i32 = 100;
const GITLAB_PAGE: i32 = 200;

const INVALID: MemberGrantFailure = MemberGrantFailure::InvalidResponse {
    credential_rejected: false,
};

/// A provider at which an organization holds connections this family reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::cli) enum Provider {
    /// GitHub App installations, which Cloud lists to an owner alone.
    GitHub,
    Linear,
    GitLab,
}

impl Provider {
    /// The provider `name` names: `github`, `linear` or `gitlab.com`.
    pub(in crate::cli) fn named(name: &str) -> Option<Self> {
        [Self::GitHub, Self::Linear, Self::GitLab]
            .into_iter()
            .find(|provider| provider.name() == name)
    }

    /// The provider's wire name.
    pub(in crate::cli) const fn name(self) -> &'static str {
        match self {
            Self::GitHub => "github",
            Self::Linear => "linear",
            Self::GitLab => "gitlab.com",
        }
    }
}

/// One connection of an organization, by its named fields.
pub(in crate::cli) struct OrganizationConnection {
    /// The installation's or the connection's ID: `ghi_`, `lcn_` or `glc_`.
    pub(in crate::cli) id: String,
    /// `github`, `linear` or `gitlab.com`.
    pub(in crate::cli) service: &'static str,
    /// The organization's ID, where the record states it, as a Linear or GitLab one does.
    pub(in crate::cli) organization_id: Option<String>,
    /// The GitHub account's login, the Linear workspace's name or the GitLab bot user's username,
    /// where the record names one.
    pub(in crate::cli) account: Option<String>,
    /// The instance a GitLab connection is at.
    pub(in crate::cli) instance_url: Option<String>,
    /// The state, in the word `useful connection` prints for it.
    pub(in crate::cli) state: String,
}

/// The connections of the organization `organization` names, by ID or slug, under the human
/// session: its GitHub App installations where `installations`, which Cloud serves an owner
/// alone, then its Linear and its GitLab connections, each read to its last page. A read Cloud
/// refuses (`403`, `404`) or answers that it cannot serve (any `5xx`, as a deployment without
/// that provider configured answers) is passed over; any other failure is the inner error, as
/// the nearest failure of a member's read.
pub(in crate::cli) fn of_organization(
    deployment: &Deployment,
    organization: &str,
    installations: bool,
    allow_insecure_http: bool,
) -> anyhow::Result<Result<Vec<OrganizationConnection>, MemberGrantFailure>> {
    let providers = installations
        .then_some(Provider::GitHub)
        .into_iter()
        .chain([Provider::Linear, Provider::GitLab]);
    let mut connections = Vec::new();
    for provider in providers {
        match read(deployment, organization, provider, allow_insecure_http)? {
            Ok(items) => connections.extend(items),
            Err(Judged {
                failure,
                passed_over: false,
            }) => return Ok(Err(failure)),
            Err(Judged { .. }) => {}
        }
    }
    Ok(Ok(connections))
}

/// The connections of the organization `organization` names at `provider` alone, read as
/// [`of_organization`] reads them but passing nothing over: `403` is `Forbidden`, `404`
/// `OrganizationNotFound` and a `5xx` the unreachable or provider-unavailable failure nearest it.
pub(in crate::cli) fn at_provider(
    deployment: &Deployment,
    organization: &str,
    provider: Provider,
    allow_insecure_http: bool,
) -> anyhow::Result<Result<Vec<OrganizationConnection>, MemberGrantFailure>> {
    Ok(
        read(deployment, organization, provider, allow_insecure_http)?
            .map_err(|judged| judged.failure),
    )
}

/// A failed read as the nearest failure of a member's read, and whether the list passes it over:
/// Cloud refused it or answered that it cannot serve it.
struct Judged {
    failure: MemberGrantFailure,
    passed_over: bool,
}

impl Judged {
    /// A failure the list passes over: Cloud refused the read or could not serve it.
    const fn refused(failure: MemberGrantFailure) -> Self {
        Self {
            failure,
            passed_over: true,
        }
    }

    /// A failure that fails the list.
    const fn failed(failure: MemberGrantFailure) -> Self {
        Self {
            failure,
            passed_over: false,
        }
    }
}

/// The connections of `organization` at `provider`, read to the last page under a session of
/// that provider's client, or the read's failure judged.
fn read(
    deployment: &Deployment,
    organization: &str,
    provider: Provider,
    allow_insecure_http: bool,
) -> anyhow::Result<Result<Vec<OrganizationConnection>, Judged>> {
    let http = || HttpOptions {
        allow_insecure_http,
    };
    // Each client's own options, which differ in their JSON output alone and print nothing here.
    let options = || CommonArgs::new(false, PrincipalAuthenticationArgs::default(), http());
    let gitlab_options = || CommonArgs::new(false, PrincipalAuthenticationArgs::default(), http());
    Ok(match provider {
        Provider::GitHub => super::github::with_api(
            deployment,
            http().transport_policy(),
            &PrincipalAuthenticationArgs::default(),
            |api| api.list_installations(organization),
        )?
        .map(|items| items.iter().map(installation).collect())
        .map_err(github_failure),
        Provider::Linear => super::with_api(deployment, &options(), |api| {
            grant::every_page(linear_invalid(), |cursor| {
                api.list(organization, Some(LINEAR_PAGE), cursor)
                    .map(|page| (page.items, page.next_cursor))
            })
        })?
        .map(|items| items.iter().map(linear_connection).collect())
        .map_err(linear_failure),
        Provider::GitLab => super::gitlab::with_options(deployment, &gitlab_options(), |api| {
            grant::every_page(GitLabFailure::Protocol, |cursor| {
                let page = api.list(organization, Some(GITLAB_PAGE), cursor)?;
                let items = page["items"]
                    .as_array()
                    .ok_or(GitLabFailure::Protocol)?
                    .iter()
                    .map(|item| gitlab_connection(item).ok_or(GitLabFailure::Protocol))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((items, page["nextCursor"].as_str().map(str::to_owned)))
            })
        })?
        .map_err(gitlab_failure),
    })
}

fn installation(installation: &GitHubInstallation) -> OrganizationConnection {
    OrganizationConnection {
        id: installation.id.clone(),
        service: "github",
        organization_id: None,
        account: Some(installation.provider_account_login.clone()),
        instance_url: None,
        state: super::github::output::installation_state(installation.state).to_owned(),
    }
}

fn linear_connection(connection: &LinearConnection) -> OrganizationConnection {
    OrganizationConnection {
        id: connection.id.clone(),
        service: "linear",
        organization_id: Some(connection.organization_id.clone()),
        account: connection.workspace_name.clone(),
        instance_url: None,
        state: super::status(&connection.status).to_owned(),
    }
}

/// A GitLab connection's named fields, read from the document the client answers, whose state
/// `useful connection gitlab` prints as it is; none where the document lacks one it requires.
fn gitlab_connection(item: &Value) -> Option<OrganizationConnection> {
    let text = |value: &Value| value.as_str().map(str::to_owned);
    Some(OrganizationConnection {
        id: text(&item["id"])?,
        service: "gitlab.com",
        organization_id: text(&item["organizationId"]),
        account: text(&item["providerUser"]["username"]),
        instance_url: text(&item["instanceUrl"]),
        state: text(&item["state"])?,
    })
}

/// A failure of the installations read: one the list passes over where Cloud refused it or could
/// not serve it, as the nearest failure of a member's read.
fn github_failure(failure: GitHubFailure) -> Judged {
    match failure {
        GitHubFailure::Forbidden => Judged::refused(MemberGrantFailure::Forbidden),
        GitHubFailure::NotFound => Judged::refused(MemberGrantFailure::OrganizationNotFound),
        GitHubFailure::Unreachable(UnreachableCategory::Server) => {
            Judged::refused(MemberGrantFailure::Unreachable {
                category: UnreachableCategory::Server,
            })
        }
        GitHubFailure::Unauthenticated => Judged::failed(MemberGrantFailure::Unauthenticated),
        GitHubFailure::Unreachable(category) => {
            Judged::failed(MemberGrantFailure::Unreachable { category })
        }
        GitHubFailure::Protocol {
            credential_rejected,
        } => Judged::failed(MemberGrantFailure::InvalidResponse {
            credential_rejected,
        }),
        // A list read defines neither answer.
        GitHubFailure::InvalidInput | GitHubFailure::SourceConnectionConflict => {
            Judged::failed(INVALID)
        }
    }
}

/// A failure of the Linear read, judged as [`github_failure`] judges one.
fn linear_failure(failure: LinearFailure) -> Judged {
    match failure {
        LinearFailure::Forbidden => Judged::refused(MemberGrantFailure::Forbidden),
        LinearFailure::NotFound => Judged::refused(MemberGrantFailure::OrganizationNotFound),
        LinearFailure::Unreachable {
            category: UnreachableCategory::Server,
        } => Judged::refused(MemberGrantFailure::Unreachable {
            category: UnreachableCategory::Server,
        }),
        LinearFailure::Unauthenticated => Judged::failed(MemberGrantFailure::Unauthenticated),
        LinearFailure::Unreachable { category } => {
            Judged::failed(MemberGrantFailure::Unreachable { category })
        }
        LinearFailure::RateLimited { retry_after } => {
            Judged::failed(MemberGrantFailure::RateLimited { retry_after })
        }
        LinearFailure::InvalidResponse {
            credential_rejected,
        } => Judged::failed(MemberGrantFailure::InvalidResponse {
            credential_rejected,
        }),
        // A list read defines neither answer.
        LinearFailure::InvalidInput { .. } | LinearFailure::Conflict => Judged::failed(INVALID),
    }
}

/// A failure of the GitLab read, judged as [`github_failure`] judges one; a deployment without
/// GitLab answers `ServiceUnavailable` or `ProviderUnavailable`.
fn gitlab_failure(failure: GitLabFailure) -> Judged {
    match failure {
        GitLabFailure::Forbidden => Judged::refused(MemberGrantFailure::Forbidden),
        GitLabFailure::NotFound => Judged::refused(MemberGrantFailure::OrganizationNotFound),
        GitLabFailure::Unreachable(UnreachableCategory::Server) => {
            Judged::refused(MemberGrantFailure::Unreachable {
                category: UnreachableCategory::Server,
            })
        }
        GitLabFailure::ServiceUnavailable | GitLabFailure::ProviderUnavailable => {
            Judged::refused(MemberGrantFailure::ProviderUnavailable)
        }
        GitLabFailure::Unauthenticated => Judged::failed(MemberGrantFailure::Unauthenticated),
        GitLabFailure::Unreachable(category) => {
            Judged::failed(MemberGrantFailure::Unreachable { category })
        }
        // A list read defines none of these answers.
        GitLabFailure::InvalidInput
        | GitLabFailure::Conflict
        | GitLabFailure::CandidateUnavailable
        | GitLabFailure::IdentityMismatch
        | GitLabFailure::Protocol => Judged::failed(INVALID),
    }
}

const fn linear_invalid() -> LinearFailure {
    LinearFailure::InvalidResponse {
        credential_rejected: false,
    }
}
