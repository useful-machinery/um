//! An organization's connections as `useful integration list` shows them (REQ-INTEGRATION-22):
//! its GitHub App installations, its Linear connections and its GitLab connections, each read
//! with the client and the session of the `useful connection` leaf that lists them and stated in
//! the word that leaf prints for its state. Only named fields are kept, never a record whole, and
//! a read Cloud refuses or answers that it cannot serve is passed over.

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
    let http = || HttpOptions {
        allow_insecure_http,
    };
    let mut connections = Vec::new();
    if installations {
        let read = super::github::with_api(
            deployment,
            http().transport_policy(),
            &PrincipalAuthenticationArgs::default(),
            |api| api.list_installations(organization),
        )?;
        match kept(read, github_failure) {
            Ok(items) => connections.extend(items.iter().map(installation)),
            Err(failure) => return Ok(Err(failure)),
        }
    }
    let options = CommonArgs::new(false, PrincipalAuthenticationArgs::default(), http());
    let read = super::with_api(deployment, &options, |api| {
        grant::every_page(linear_invalid(), |cursor| {
            api.list(organization, Some(LINEAR_PAGE), cursor)
                .map(|page| (page.items, page.next_cursor))
        })
    })?;
    match kept(read, linear_failure) {
        Ok(items) => connections.extend(items.iter().map(linear_connection)),
        Err(failure) => return Ok(Err(failure)),
    }
    let options = CommonArgs::new(false, PrincipalAuthenticationArgs::default(), http());
    let read = super::gitlab::with_options(deployment, &options, |api| {
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
    })?;
    match kept(read, gitlab_failure) {
        Ok(items) => connections.extend(items),
        Err(failure) => return Ok(Err(failure)),
    }
    Ok(Ok(connections))
}

/// What a read gave: its items, none where `judged` passes its failure over, or the failure
/// `judged` names.
fn kept<T, F>(
    read: Result<Vec<T>, F>,
    judged: impl FnOnce(F) -> Option<MemberGrantFailure>,
) -> Result<Vec<T>, MemberGrantFailure> {
    match read {
        Ok(items) => Ok(items),
        Err(failure) => judged(failure).map_or_else(|| Ok(Vec::new()), Err),
    }
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

/// A failure of the installations read: passed over where Cloud refused it or could not serve
/// it, else the member's read failure nearest it.
fn github_failure(failure: GitHubFailure) -> Option<MemberGrantFailure> {
    match failure {
        GitHubFailure::Forbidden
        | GitHubFailure::NotFound
        | GitHubFailure::Unreachable(UnreachableCategory::Server) => None,
        GitHubFailure::Unauthenticated => Some(MemberGrantFailure::Unauthenticated),
        GitHubFailure::Unreachable(category) => Some(MemberGrantFailure::Unreachable { category }),
        GitHubFailure::Protocol {
            credential_rejected,
        } => Some(MemberGrantFailure::InvalidResponse {
            credential_rejected,
        }),
        // A list read defines neither answer.
        GitHubFailure::InvalidInput | GitHubFailure::SourceConnectionConflict => Some(INVALID),
    }
}

/// A failure of the Linear read, judged as [`github_failure`] judges one.
fn linear_failure(failure: LinearFailure) -> Option<MemberGrantFailure> {
    match failure {
        LinearFailure::Forbidden
        | LinearFailure::NotFound
        | LinearFailure::Unreachable {
            category: UnreachableCategory::Server,
        } => None,
        LinearFailure::Unauthenticated => Some(MemberGrantFailure::Unauthenticated),
        LinearFailure::Unreachable { category } => {
            Some(MemberGrantFailure::Unreachable { category })
        }
        LinearFailure::RateLimited { retry_after } => {
            Some(MemberGrantFailure::RateLimited { retry_after })
        }
        LinearFailure::InvalidResponse {
            credential_rejected,
        } => Some(MemberGrantFailure::InvalidResponse {
            credential_rejected,
        }),
        // A list read defines neither answer.
        LinearFailure::InvalidInput { .. } | LinearFailure::Conflict => Some(INVALID),
    }
}

/// A failure of the GitLab read, judged as [`github_failure`] judges one; a deployment without
/// GitLab answers `ServiceUnavailable` or `ProviderUnavailable`.
fn gitlab_failure(failure: GitLabFailure) -> Option<MemberGrantFailure> {
    match failure {
        GitLabFailure::Forbidden
        | GitLabFailure::NotFound
        | GitLabFailure::Unreachable(UnreachableCategory::Server)
        | GitLabFailure::ServiceUnavailable
        | GitLabFailure::ProviderUnavailable => None,
        GitLabFailure::Unauthenticated => Some(MemberGrantFailure::Unauthenticated),
        GitLabFailure::Unreachable(category) => Some(MemberGrantFailure::Unreachable { category }),
        // A list read defines none of these answers.
        GitLabFailure::InvalidInput
        | GitLabFailure::Conflict
        | GitLabFailure::CandidateUnavailable
        | GitLabFailure::IdentityMismatch
        | GitLabFailure::Protocol => Some(INVALID),
    }
}

const fn linear_invalid() -> LinearFailure {
    LinearFailure::InvalidResponse {
        credential_rejected: false,
    }
}
