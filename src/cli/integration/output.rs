//! How `useful integration` prints a token service's token: the record's view, which every leaf
//! that shows a token prints, the report of a token connected, replaced or revoked, the list of
//! one organization's tokens and connections and the report of one token, and a refusal with its
//! remedy (REQ-INTEGRATION-16 to REQ-INTEGRATION-20), the view of an organization's connection
//! the list prints with its authorizations (REQ-INTEGRATION-22, REQ-INTEGRATION-34), and a
//! runner's authorization for an organization's connection in one project with its scope, the
//! report of one made or ended and the refusals of making or ending one (REQ-INTEGRATION-32,
//! REQ-INTEGRATION-33). Only the record's named fields are printed, so nothing an answer adds
//! beyond the model, a token least of all, reaches output.

use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use super::authorizations::RouteFailure;
use super::connections::Listing;
use crate::cli::ApiFailureReport;
use crate::cli::connection::organization::{OrganizationConnection, Provider};
use crate::cli::grant;
use crate::exit_code::{ExitCode, OutcomeClass};
use useful_api::{
    ConnectionAuthorization, ConnectionAuthorizationRefusal, ConnectionAuthorizationScope,
    ConnectionAuthorizationState, GitHubRepositorySelection, IntegrationToken,
    IntegrationTokenAuthorization, IntegrationTokenSide, MemberGrantFailure, ServedTokenService,
};
use useful_execution::visible_text;

/// What a token service is called for people: the name the deployment gives it, else its id.
pub(super) fn title(service: &ServedTokenService) -> &str {
    service.display_name.as_deref().unwrap_or(&service.name)
}

/// A token's record as a leaf prints it: its ID, service, side and organization, version, who
/// connected it and when, and the runners it is authorized for.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TokenView<'a> {
    id: &'a str,
    service: &'a str,
    side: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization: Option<OrganizationView<'a>>,
    version: i64,
    connected_by: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    connected_by_name: Option<&'a str>,
    connected_at: &'a str,
    created_at: &'a str,
    authorizations: Vec<TokenAuthorizationView<'a>>,
}

/// The organization whose token or connection it is: its ID, which a token always states, and
/// its slug where the command was given one.
#[derive(Serialize)]
struct OrganizationView<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slug: Option<&'a str>,
}

impl<'a> OrganizationView<'a> {
    /// The view of the organization `id` names, with `slug` where it says more than the ID.
    fn new(id: Option<&'a str>, slug: Option<&'a str>) -> Self {
        Self {
            id,
            slug: slug.filter(|slug| Some(*slug) != id),
        }
    }
}

/// Writes the line that states a token's or a connection's side and, for an organization's, the
/// organization, whose slug is another party's text and is printed visibly.
fn write_side(
    output: &mut impl Write,
    side: &str,
    organization: Option<&OrganizationView<'_>>,
) -> io::Result<()> {
    match organization.map(|organization| (organization.id, organization.slug)) {
        Some((Some(id), Some(slug))) => {
            writeln!(output, "side: {side} · {} ({id})", visible_text(slug))
        }
        Some((Some(id), None)) => writeln!(output, "side: {side} · {id}"),
        Some((None, Some(slug))) => writeln!(output, "side: {side} · {}", visible_text(slug)),
        Some((None, None)) | None => writeln!(output, "side: {side}"),
    }
}

/// A runner a token is authorized for.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TokenAuthorizationView<'a> {
    id: &'a str,
    runner_id: &'a str,
    runner_name: &'a str,
    organization_id: &'a str,
    created_at: &'a str,
}

impl<'a> TokenAuthorizationView<'a> {
    fn new(authorization: &'a IntegrationTokenAuthorization) -> Self {
        Self {
            id: &authorization.id,
            runner_id: &authorization.runner_id,
            runner_name: &authorization.runner_name,
            organization_id: &authorization.organization_id,
            created_at: &authorization.created_at,
        }
    }
}

impl<'a> TokenView<'a> {
    /// The view of `token`; for an organization's token `slug` is its slug where that is known,
    /// and `connected_by_name` the name the organization's directory gives who connected it.
    pub(super) fn new(
        token: &'a IntegrationToken,
        slug: Option<&'a str>,
        connected_by_name: Option<&'a str>,
    ) -> Self {
        let organization = match token.side {
            IntegrationTokenSide::IntegrationTokenSideMember => None,
            IntegrationTokenSide::IntegrationTokenSideOrg => token
                .organization_id
                .as_deref()
                .map(|id| OrganizationView::new(Some(id), slug)),
        };
        Self {
            id: &token.id,
            service: &token.provider,
            side: match token.side {
                IntegrationTokenSide::IntegrationTokenSideMember => "member",
                IntegrationTokenSide::IntegrationTokenSideOrg => "org",
            },
            organization,
            version: token.version,
            connected_by: &token.connected_by,
            connected_by_name,
            connected_at: &token.connected_at,
            created_at: &token.created_at,
            authorizations: token
                .authorizations
                .iter()
                .map(TokenAuthorizationView::new)
                .collect(),
        }
    }

    /// Writes the line that states the token's side and, for an organization's, the
    /// organization.
    fn write_side(&self, output: &mut impl Write) -> io::Result<()> {
        write_side(output, self.side, self.organization.as_ref())
    }

    /// Writes the view's lines of a human report. What Cloud names, a slug, a person and a
    /// runner, is another party's text and is printed visibly; the member's own token is said to
    /// be theirs.
    pub(super) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "token: {}", self.id)?;
        writeln!(output, "service: {}", self.service)?;
        self.write_side(output)?;
        writeln!(output, "version: {}", self.version)?;
        let by = self.connected_by;
        match (self.organization.is_some(), self.connected_by_name) {
            (false, _) => writeln!(output, "connected by: you ({by})")?,
            (true, Some(name)) => writeln!(output, "connected by: {} ({by})", visible_text(name))?,
            (true, None) => writeln!(output, "connected by: {by}")?,
        }
        writeln!(output, "connected at: {}", self.connected_at)?;
        if self.authorizations.is_empty() {
            writeln!(output, "runners: none")?;
        }
        for authorization in &self.authorizations {
            writeln!(
                output,
                "runner: {} ({})",
                visible_text(authorization.runner_name),
                authorization.runner_id
            )?;
        }
        Ok(())
    }
}

/// An organization's connection as `useful integration list` prints it (REQ-INTEGRATION-22):
/// its ID, service and side, its organization, the account and instance the record names, its
/// state in the word `useful connection` prints, and, where the organization's were read, the
/// runners' authorizations in force for it (REQ-INTEGRATION-34). Never a credential: the
/// connection reads carry none, and only these named fields are printed.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConnectionView<'a> {
    id: &'a str,
    service: &'a str,
    side: &'static str,
    organization: OrganizationView<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    account: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    instance_url: Option<&'a str>,
    state: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    authorizations: Option<Vec<ConnectionAuthorizationView<'a>>>,
}

impl<'a> ConnectionView<'a> {
    /// The view of `connection`, an organization's, of the organization `organization_id` and
    /// `slug` name, with `authorizations` where the organization's were read and answered.
    pub(super) fn new(
        connection: &'a OrganizationConnection,
        organization_id: Option<&'a str>,
        slug: Option<&'a str>,
        authorizations: Option<&'a [ConnectionAuthorization]>,
    ) -> Self {
        Self {
            id: &connection.id,
            service: connection.service,
            side: "org",
            organization: OrganizationView::new(organization_id, slug),
            account: connection.account.as_deref(),
            instance_url: connection.instance_url.as_deref(),
            state: &connection.state,
            authorizations: authorizations.map(|authorizations| {
                authorizations
                    .iter()
                    .map(ConnectionAuthorizationView::new)
                    .collect()
            }),
        }
    }

    /// Writes the view's lines of a human report, each authorization's after the state; nothing
    /// of one where there is none. What Cloud names, an account and an instance, is another
    /// party's text and is printed visibly.
    pub(super) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "connection: {}", self.id)?;
        writeln!(output, "service: {}", self.service)?;
        write_side(output, self.side, Some(&self.organization))?;
        if let Some(account) = self.account {
            writeln!(output, "account: {}", visible_text(account))?;
        }
        if let Some(instance_url) = self.instance_url {
            writeln!(output, "instance: {}", visible_text(instance_url))?;
        }
        writeln!(output, "state: {}", visible_text(self.state))?;
        for authorization in self.authorizations.iter().flatten() {
            authorization.write_human(output)?;
        }
        Ok(())
    }
}

/// A runner's authorization for an organization's connection in one project, as a leaf prints
/// it (REQ-INTEGRATION-32, REQ-INTEGRATION-33): its ID and provider, its project, runner and
/// connection, who made it and when, and its scope. Never a credential: an authorization holds
/// none, and only these named fields are printed.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConnectionAuthorizationView<'a> {
    #[serde(flatten)]
    named: NamedAuthorizationView<'a>,
    authorized_by: &'a str,
    created_at: &'a str,
    scope: ScopeView<'a>,
}

/// What names a runner's authorization for an organization's connection: its ID and provider,
/// its project, runner and connection, which the report of one ended prints alone.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NamedAuthorizationView<'a> {
    id: &'a str,
    provider: &'a str,
    project_id: &'a str,
    project_name: &'a str,
    runner_id: &'a str,
    runner_name: &'a str,
    connection_id: &'a str,
    connection_account: &'a str,
}

/// What the provider grants the authorization's connection (REQ-INTEGRATION-32): its state,
/// and for a `current` GitHub scope the installation's repository selection and permissions.
#[derive(Serialize)]
struct ScopeView<'a> {
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    github: Option<GitHubScopeView<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GitHubScopeView<'a> {
    repository_selection: &'static str,
    permissions: Vec<PermissionView<'a>>,
}

/// A permission the installation grants, its name and access as GitHub reports them.
#[derive(Serialize)]
struct PermissionView<'a> {
    name: &'a str,
    access: &'a str,
}

impl<'a> ScopeView<'a> {
    fn new(scope: &'a ConnectionAuthorizationScope) -> Self {
        let current = scope.state == ConnectionAuthorizationState::Current;
        Self {
            state: match scope.state {
                ConnectionAuthorizationState::Current => "current",
                ConnectionAuthorizationState::Unavailable => "unavailable",
            },
            github: scope
                .github
                .as_deref()
                .filter(|_| current)
                .map(|github| GitHubScopeView {
                    repository_selection: match github.repository_selection {
                        GitHubRepositorySelection::GitHubInstallationScopeRepositorySelectionAll => {
                            "all"
                        }
                        GitHubRepositorySelection::GitHubInstallationScopeRepositorySelectionSelected => {
                            "selected"
                        }
                    },
                    permissions: github
                        .permissions
                        .iter()
                        .map(|permission| PermissionView {
                            name: &permission.name,
                            access: &permission.access,
                        })
                        .collect(),
                }),
        }
    }

    /// Writes the scope's lines of a human report: the repository selection and one line for
    /// each permission, which are GitHub's text and printed visibly; that the provider could not
    /// be read; or, for a `current` scope this release knows no member of, `current` alone.
    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        match (&self.github, self.state) {
            (Some(github), _) => {
                writeln!(
                    output,
                    "scope: {} repositories",
                    github.repository_selection
                )?;
                for permission in &github.permissions {
                    writeln!(
                        output,
                        "permission: {} · {}",
                        visible_text(permission.name),
                        visible_text(permission.access)
                    )?;
                }
                Ok(())
            }
            (None, "unavailable") => {
                writeln!(output, "scope: unavailable, the provider could not be read")
            }
            (None, state) => writeln!(output, "scope: {state}"),
        }
    }
}

impl<'a> ConnectionAuthorizationView<'a> {
    pub(super) fn new(authorization: &'a ConnectionAuthorization) -> Self {
        Self {
            named: NamedAuthorizationView::new(authorization),
            authorized_by: &authorization.authorized_by,
            created_at: &authorization.created_at,
            scope: ScopeView::new(&authorization.scope),
        }
    }

    /// Writes the view's lines of a human report: what names the authorization, then who made
    /// it and when, and its scope.
    pub(super) fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        self.named.write_human(output)?;
        writeln!(output, "authorized by: {}", self.authorized_by)?;
        writeln!(output, "authorized at: {}", self.created_at)?;
        self.scope.write_human(output)
    }
}

impl<'a> NamedAuthorizationView<'a> {
    fn new(authorization: &'a ConnectionAuthorization) -> Self {
        Self {
            id: &authorization.id,
            provider: &authorization.provider,
            project_id: &authorization.project_id,
            project_name: &authorization.project_name,
            runner_id: &authorization.runner_id,
            runner_name: &authorization.runner_name,
            connection_id: &authorization.connection_id,
            connection_account: &authorization.connection_account,
        }
    }

    /// Writes the view's lines of a human report. What Cloud names, a project, a runner and an
    /// account, is another party's text and is printed visibly, each with its ID.
    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "authorization: {}", self.id)?;
        writeln!(output, "provider: {}", self.provider)?;
        writeln!(
            output,
            "project: {} ({})",
            visible_text(self.project_name),
            self.project_id
        )?;
        writeln!(
            output,
            "runner: {} ({})",
            visible_text(self.runner_name),
            self.runner_id
        )?;
        writeln!(
            output,
            "connection: {} ({})",
            visible_text(self.connection_account),
            self.connection_id
        )
    }
}

/// The document of an authorization made or ended, whose view is `V`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionAuthorizationResult<'a, V> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    authorization: V,
}

/// Reports the authorization `useful integration authorize` given `--project-id` made
/// (REQ-INTEGRATION-33).
pub(super) fn write_connection_authorized(
    deployment: &str,
    authorization: &ConnectionAuthorization,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let view = ConnectionAuthorizationView::new(authorization);
    write_connection_outcome(
        deployment,
        ("authorized", "✓ Runner authorized."),
        &view,
        |output| view.write_human(output),
        json,
    )
}

/// Reports the authorization `useful integration deauthorize` given `--project-id` ended
/// (REQ-INTEGRATION-33): what names it, without who made it, when or its scope.
pub(super) fn write_connection_deauthorized(
    deployment: &str,
    authorization: &ConnectionAuthorization,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let view = NamedAuthorizationView::new(authorization);
    write_connection_outcome(
        deployment,
        ("deauthorized", "✓ Runner authorization ended."),
        &view,
        |output| view.write_human(output),
        json,
    )
}

/// Reports an authorization made or ended under `outcome`, the document's outcome and the
/// report's first line: under `--json` one document carrying `view`, else the report, whose
/// lines of the authorization `write_human` writes, then the deployment.
fn write_connection_outcome(
    deployment: &str,
    (outcome, done): (&'static str, &str),
    view: &impl Serialize,
    write_human: impl FnOnce(&mut io::StdoutLock<'static>) -> io::Result<()>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&ConnectionAuthorizationResult {
            schema_version: 1,
            deployment,
            outcome,
            authorization: view,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{done}")?;
    write_human(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

/// A connection a leaf names among those it can act on: its ID and account.
#[derive(Serialize)]
struct NamedConnectionView<'a> {
    id: &'a str,
    account: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionRequiredResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    connections: Vec<NamedConnectionView<'a>>,
}

/// Refuses a leaf given no `--connection-id` where the organization holds several `active`
/// connections at `provider` (REQ-INTEGRATION-33): exit 2, naming each by its ID and account,
/// then the command line `example` that names one; under `--json`, one document with outcome
/// `connection_required`.
pub(super) fn write_connection_required(
    deployment: &str,
    provider: Provider,
    connections: &[OrganizationConnection],
    example: &str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&ConnectionRequiredResult {
            schema_version: 1,
            deployment,
            outcome: "connection_required",
            connections: connections
                .iter()
                .map(|connection| NamedConnectionView {
                    id: &connection.id,
                    account: connection.account.as_deref(),
                })
                .collect(),
        })?;
    } else {
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "error: name the {} connection to authorize, one of:",
            provider.name()
        )?;
        for connection in connections {
            match connection.account.as_deref() {
                Some(account) => {
                    writeln!(stderr, "  {} · {}", connection.id, visible_text(account))?;
                }
                None => writeln!(stderr, "  {}", connection.id)?,
            }
        }
        writeln!(stderr, "\nName one by its ID, for example:\n  {example}")?;
    }
    Ok(ExitCode::UsageError)
}

/// The command that connects the organization `org` at `provider`: `connect --org` for GitHub
/// and Linear, and the first step of a GitLab connection, which `connect --org` refuses.
fn connecting(org: &str, provider: Provider) -> String {
    match provider {
        Provider::GitHub | Provider::Linear => {
            format!("useful integration connect {} --org {org}", provider.name())
        }
        Provider::GitLab => format!("useful connection gitlab create {org} INSTANCE_URL"),
    }
}

/// The refusal of a leaf that finds no connection of the organization `org` at `provider` to
/// authorize a runner for, having sent nothing more (REQ-INTEGRATION-33): none `active`, with
/// the command that connects one, or none by the ID `--connection-id` named, with the list of
/// the organization's connections.
pub(super) fn no_connection(
    org: &str,
    provider: Provider,
    connection_id: Option<&str>,
) -> ApiFailureReport {
    let name = provider.name();
    let (summary, remedy) = match connection_id {
        None => (
            format!("error: the organization {org} holds no active {name} connection"),
            format!(
                "Connect the organization at {name}:\n  {}",
                connecting(org, provider)
            ),
        ),
        Some(id) => (
            format!("error: the organization {org} holds no {name} connection {id}"),
            format!("List the organization's connections:\n  useful integration list --org {org}"),
        ),
    };
    ApiFailureReport::new(
        "connection_not_found",
        summary,
        remedy,
        OutcomeClass::GeneralFailure,
    )
}

/// A failure of a leaf acting on a connection authorization of the organization `org` at
/// `provider` (REQ-INTEGRATION-33): an owner's refusal, each refusal only those routes answer
/// worded with the organization and the provider, and a deployment that does not serve them;
/// any other as a grant's.
pub(super) fn connection_authorization_report(
    deployment: &str,
    failure: &RouteFailure,
    org: &str,
    provider: Provider,
) -> ApiFailureReport {
    use ConnectionAuthorizationRefusal as R;
    let name = provider.name();
    let list = format!("useful integration list --org {org}");
    let refused = |outcome, summary: String, remedy: String| {
        ApiFailureReport::new(outcome, summary, remedy, OutcomeClass::GeneralFailure)
    };
    let refusal = match failure {
        RouteFailure::NotServed => {
            return refused(
                "connection_authorizations_not_served",
                "error: this deployment does not serve connection authorizations".to_owned(),
                "Ask the deployment's operator when it will; until then no command authorizes a \
                 runner for an organization's connection there."
                    .to_owned(),
            );
        }
        RouteFailure::Failed(MemberGrantFailure::Forbidden) => {
            return ApiFailureReport::new(
                "forbidden",
                format!(
                    "! Only an owner of the organization {org} can authorize a runner for its \
                     connections or end that authorization."
                ),
                format!(
                    "Ask an owner of {org}. List the organization's members:\n  useful \
                     organization member list {org}"
                ),
                OutcomeClass::Forbidden,
            );
        }
        RouteFailure::Failed(MemberGrantFailure::ConnectionAuthorizationRefused(refusal)) => {
            *refusal
        }
        RouteFailure::Failed(failure) => return grant::output::report(deployment, failure),
    };
    let connect = format!(
        "Connect the organization at {name}:\n  {}\nor list its connections:\n  {list}",
        connecting(org, provider)
    );
    match refusal {
        R::ProjectNotFound => refused(
            "project_not_found",
            format!("error: project not found in the organization {org}"),
            format!(
                "List the organization's projects and use a project ID:\n  useful project list \
                 {org}"
            ),
        ),
        R::RunnerNotFound => refused(
            "runner_not_found",
            format!("error: runner not found in the organization {org}"),
            format!(
                "List the organization's runners and use a runner ID:\n  useful runner list {org}"
            ),
        ),
        R::ConnectionNotFound => refused(
            "connection_not_found",
            format!("error: {name} connection not found in the organization {org}"),
            connect,
        ),
        R::ConnectionInactive => refused(
            "connection_inactive",
            format!("error: that {name} connection of the organization {org} is not active"),
            connect,
        ),
        R::ProviderIneligible => refused(
            "provider_ineligible",
            format!(
                "error: this deployment cannot authorize a runner for an organization's {name} \
                 connection"
            ),
            format!("List the organization's integrations:\n  {list}"),
        ),
        R::AlreadyAuthorized => refused(
            "already_authorized",
            format!(
                "error: the runner is already authorized for a {name} connection of the \
                 organization {org} in that project"
            ),
            format!(
                "List the organization's integrations:\n  {list}\nor end the runner's \
                 authorization in that project:\n  useful integration deauthorize {name} RUNNER \
                 --org {org} --project-id PROJECT --yes"
            ),
        ),
        R::AuthorizationNotFound => refused(
            "authorization_not_found",
            format!("error: connection authorization not found in the organization {org}"),
            format!("List the organization's integrations:\n  {list}"),
        ),
    }
}

/// The one document `useful integration connect` prints for a token service: the token it
/// connected or replaced, or the refusal that stored nothing.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TokenResult<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<TokenView<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<u64>,
}

impl<'a> TokenResult<'a> {
    const fn new(deployment: &'a str, outcome: &'static str) -> Self {
        Self {
            schema_version: 1,
            event: "token_result",
            deployment,
            outcome,
            token: None,
            category: None,
            retry_after: None,
        }
    }
}

/// Reports the token `useful integration connect` stored, as the next version of the one held
/// where it `replaced` it, for the organization `org` names where one was named
/// (REQ-INTEGRATION-16, REQ-INTEGRATION-18).
pub(super) fn write_connected(
    deployment: &str,
    token: &IntegrationToken,
    org: Option<&str>,
    replaced: bool,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let view = TokenView::new(token, org, None);
    let outcome = if replaced { "replaced" } else { "connected" };
    if json {
        write_json_line(&TokenResult {
            token: Some(view),
            ..TokenResult::new(deployment, outcome)
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Token {outcome}.")?;
    view.write_human(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    if replaced {
        writeln!(
            stdout,
            "\nA runner authorized for this token takes the new version at its next request, \
             within five minutes on a running assignment."
        )?;
    }
    stdout.flush()?;
    Ok(ExitCode::Success)
}

/// Reports the one token `useful integration list INTEGRATION` names (REQ-INTEGRATION-19).
pub(super) fn write_shown(
    deployment: &str,
    view: TokenView<'_>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&TokenResult {
            token: Some(view),
            ..TokenResult::new(deployment, "shown")
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Token shown.")?;
    view.write_human(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

/// The document `useful integration list --org` prints: that organization's tokens and
/// connections alone.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TokenListResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    tokens: &'a super::Tokens,
    connections: &'a super::connections::Connections,
}

/// Reports one organization's tokens and connections, which `useful integration list --org`
/// prints with no grant (REQ-INTEGRATION-19, REQ-INTEGRATION-22); it exits 0 whatever they are.
/// The report's count is the tokens'.
pub(super) fn write_organization_side(
    deployment: &str,
    listing: &Listing,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&TokenListResult {
            schema_version: 1,
            deployment,
            outcome: "listed",
            tokens: listing.tokens(),
            connections: listing.connections(),
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Tokens listed.")?;
    writeln!(stdout, "deployment: {deployment}")?;
    writeln!(stdout, "count: {}", listing.tokens().count())?;
    listing.write_human(&mut stdout)?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

/// Reports a refusal that stored no token: as the command's one document, or on standard error
/// with its remedy.
pub(super) fn write_refusal(
    deployment: &str,
    report: &ApiFailureReport,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json_line(&TokenResult {
            category: report.category,
            retry_after: report.retry_after,
            ..TokenResult::new(deployment, report.outcome)
        })?;
    } else {
        writeln!(
            io::stderr().lock(),
            "{}\n\n{}",
            report.summary,
            report.remedy
        )?;
    }
    Ok(report.class.exit_code())
}

/// A failure of an operation on the token of `service`, the member's or the organization `org`
/// names, as the member reads it: an owner's refusal, a token stored meanwhile with the remedy
/// that names this token, and a service the organization holds no token of
/// (REQ-INTEGRATION-17 to REQ-INTEGRATION-19), and any other as a grant's.
pub(super) fn report(
    deployment: &str,
    failure: &MemberGrantFailure,
    service: &str,
    org: Option<&str>,
) -> ApiFailureReport {
    let on_org = org.map_or_else(String::new, |org| format!(" --org {org}"));
    match (failure, org) {
        (MemberGrantFailure::Forbidden, Some(org)) => ApiFailureReport::new(
            "forbidden",
            format!("! Only an owner of the organization {org} can manage its tokens."),
            format!(
                "Ask an owner of {org} to connect it, or connect your own:\n  \
                 useful integration connect {service}"
            ),
            OutcomeClass::Forbidden,
        ),
        (MemberGrantFailure::Forbidden, None) => ApiFailureReport::forbidden("integration"),
        (MemberGrantFailure::IntegrationTokenNotFound, Some(org)) => ApiFailureReport::new(
            "token_not_found",
            format!("error: the organization {org} holds no {service} token"),
            format!(
                "A token is named by its service. List the organization's tokens:\n  \
                 useful integration list --org {org}"
            ),
            OutcomeClass::GeneralFailure,
        ),
        (MemberGrantFailure::IntegrationTokenExists, _) => ApiFailureReport::new(
            "token_exists",
            format!("error: a {service} token was connected while this command ran"),
            format!("Replace it:\n  useful integration connect {service}{on_org} --replace"),
            OutcomeClass::GeneralFailure,
        ),
        _ => grant::output::report(deployment, failure),
    }
}

/// The refusal of `authorize --all --org` where the organization `org` holds no token
/// (REQ-INTEGRATION-20).
pub(super) fn no_token(org: &str) -> ApiFailureReport {
    ApiFailureReport::new(
        "token_not_found",
        format!("error: the organization {org} holds no token"),
        format!("Connect one for it first:\n  useful integration connect SERVICE --org {org}"),
        OutcomeClass::GeneralFailure,
    )
}

/// The document `useful integration revoke` prints for a token.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RevokedResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    integration_token_id: &'a str,
    service: &'a str,
    side: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization: Option<OrganizationView<'a>>,
}

/// Reports `token`, the member's or the one of the organization `org` names, which `revoke`
/// erased from Cloud; it stays valid at the service (REQ-INTEGRATION-20).
pub(super) fn write_revoked(
    deployment: &str,
    token: &IntegrationToken,
    org: Option<&str>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let view = TokenView::new(token, org, None);
    if json {
        write_json(&RevokedResult {
            schema_version: 1,
            deployment,
            outcome: "revoked",
            integration_token_id: view.id,
            service: view.service,
            side: view.side,
            organization: view.organization,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "✓ Token revoked.")?;
    writeln!(stdout, "token: {}", view.id)?;
    writeln!(stdout, "service: {}", view.service)?;
    view.write_side(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    writeln!(
        stdout,
        "\nThis does not end the token at the service. To end it, revoke it there."
    )?;
    stdout.flush()?;
    Ok(ExitCode::Success)
}

/// Writes a document of `useful integration list`, indented as the list's documents are.
fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::write_pretty_json(value).context("write JSON integration token list")
}

fn write_json_line(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::write_json_line(value).context("write JSON integration token event")
}
