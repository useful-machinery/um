//! `useful integration`: the commands that connect a service and say where it may be used
//! (REQ-INTEGRATION-13). For an OAuth provider or a coding agent each leaf is the `useful grant`
//! leaf of the same purpose, the same struct run through the same dispatch, so its arguments,
//! requests, output and exit statuses are that leaf's; `list` differs in stating each grant's
//! side and listing tokens, and each leaf, wrapped in a type of this family, takes `--org`
//! besides. What a token service's token alone needs is under `integration/`, reached through
//! the [`Reach`] this dispatcher hands a leaf (REQ-INTEGRATION-16 to REQ-INTEGRATION-20), and
//! `connect --org` connects the organization at GitHub or Linear through the `useful connection`
//! family's own leaves, reached the same way (REQ-INTEGRATION-21), as `list` reads the
//! organizations' connections through that family's reads (REQ-INTEGRATION-22).
//! `authorize` given `--org` and `--project-id` authorizes a runner for one of the
//! organization's connections in that project, and `deauthorize` given them ends that, each
//! acting on it through `integration/` as well (REQ-INTEGRATION-33). `useful grant` is its
//! earlier, hidden name (REQ-INTEGRATION-15).

mod authorizations;
mod connect;
mod connections;
mod output;
mod tokens;

use std::path::Path;

use clap::{Args, Subcommand};

use super::grant::{self, Flow, Leaf, authorize, create, deauthorize, list, revoke};
use super::{ApiFailureReport, CommandFailure, OrganizationArg, ProjectArg, connection};
use crate::exit_code::ExitCode;
use useful_api::{
    IntegrationToken, IntegrationTokenOwner, MemberGrantApi, MemberGrantFailure, ServedTokenService,
};
use useful_human_auth::Deployment;

pub(super) use authorizations::{ConnectionAuthorizations, RouteFailure};
pub(super) use connections::{Connections, Listing};
pub(super) use tokens::{ListedToken, Tokens};

pub(super) const ABOUT: &str = "Manage the services you connect to Useful Machinery";

/// The help of `--org` on every leaf that takes it.
const ORG_HELP: &str =
    "Act on this organization's side in place of yours, by organization ID or exact slug";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<IntegrationCommand>,
}

// Kept a type of its own, though its leaves are the grant family's, so that this family alone
// can gain what the earlier spelling does not.
#[derive(Debug, Subcommand)]
enum IntegrationCommand {
    #[command(about = "Authorize a runner to use one of your integrations, or all of them")]
    Authorize(OrgProject<authorize::Command>),
    #[command(about = "Connect a service through its provider's own sign-in or with its token")]
    Connect(Connect),
    #[command(about = "End a runner's authorization for one of your integrations")]
    Deauthorize(OrgProject<deauthorize::Command>),
    #[command(about = "List your integrations with where each may run, or show one")]
    List(Org<list::Command>),
    #[command(about = "Revoke one of your integrations everywhere")]
    Revoke(Org<revoke::Command>),
}

/// `useful integration connect`: the arguments of `useful grant create` and `--org`, so that no
/// argument of the leaf can carry a token (REQ-INTEGRATION-16).
#[derive(Debug, Args)]
struct Connect {
    #[command(flatten)]
    command: create::Command,

    #[arg(
        long = "org",
        value_name = OrganizationArg::VALUE_NAME,
        requires = "provider",
        help = ORG_HELP
    )]
    org: Option<OrganizationArg>,
}

/// `list` and `revoke` of `useful integration`: the arguments of the `useful grant` leaf `C` and
/// `--org`, with which `list` keeps one organization's side alone and `revoke` acts on the
/// organization's token (REQ-INTEGRATION-17, REQ-INTEGRATION-19).
#[derive(Debug, Args)]
struct Org<C: Args> {
    #[command(flatten)]
    command: C,

    #[arg(long = "org", value_name = OrganizationArg::VALUE_NAME, help = ORG_HELP)]
    org: Option<OrganizationArg>,
}

/// `authorize` and `deauthorize` of `useful integration`: the arguments of the `useful grant`
/// leaf `C`, `--org` as [`Org`] takes it, with which the leaf acts on the organization's token,
/// and `--project-id` and `--connection-id`, with which it acts on a runner's authorization for
/// one of the organization's connections in that project in place of a grant or a token
/// (REQ-INTEGRATION-17, REQ-INTEGRATION-33). `--project-id` requires `--org` and conflicts with
/// what [`ProjectLeaf::CONFLICTS`] names of `C`, and `--connection-id` requires `--project-id`.
#[derive(Debug, Args)]
struct OrgProject<C: ProjectLeaf> {
    // First: derive reads the fields in order and takes each value out of the matches, and the
    // leaf's confirmation reads `project_id` from them to state what it refuses.
    #[command(flatten)]
    command: C,

    #[arg(long = "org", value_name = OrganizationArg::VALUE_NAME, help = ORG_HELP)]
    org: Option<OrganizationArg>,

    #[arg(
        long,
        value_name = ProjectArg::VALUE_NAME,
        help = ProjectArg::HELP,
        requires = "org",
        conflicts_with_all = C::CONFLICTS
    )]
    project_id: Option<ProjectArg>,

    #[arg(
        long,
        value_name = "CONNECTION",
        help = C::CONNECTION_HELP,
        requires = "project_id"
    )]
    connection_id: Option<String>,
}

/// A `useful grant` leaf that `useful integration` gives `--project-id` and `--connection-id`.
trait ProjectLeaf: Args {
    /// The leaf's arguments that `--project-id` conflicts with.
    const CONFLICTS: &'static [&'static str];
    /// The leaf's help of `--connection-id`: what the leaf does with the connection it names and
    /// without one.
    const CONNECTION_HELP: &'static str;
}

impl ProjectLeaf for authorize::Command {
    // `--all` ranges over the grants or the organization's tokens, which name no project.
    const CONFLICTS: &'static [&'static str] = &["all"];
    const CONNECTION_HELP: &'static str = "Organization connection ID, as useful integration list prints it; omit to take the one active connection";
}

impl ProjectLeaf for deauthorize::Command {
    // `deauthorize` has no `--all`.
    const CONFLICTS: &'static [&'static str] = &[];
    // The authorization in force names its own connection, so the option only keeps one.
    const CONNECTION_HELP: &'static str = "Organization connection ID, as useful integration list prints it; omit to end the authorization whichever connection it names";
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        grant::execute(self.command, &["integration"], |command| match command {
            IntegrationCommand::Authorize(OrgProject {
                command,
                org,
                project_id,
                connection_id,
            }) => Leaf::Authorize(
                command,
                Reach::integration(Named::Authorize, org).in_project(project_id, connection_id),
            ),
            IntegrationCommand::Connect(Connect { command, org }) => {
                Leaf::Create(command, Reach::integration(Named::Connect, org))
            }
            IntegrationCommand::Deauthorize(OrgProject {
                command,
                org,
                project_id,
                connection_id,
            }) => Leaf::Deauthorize(
                command,
                Reach::integration(Named::Deauthorize, org).in_project(project_id, connection_id),
            ),
            IntegrationCommand::List(Org { command, org }) => {
                Leaf::List(command, Reach::integration(Named::List, org))
            }
            IntegrationCommand::Revoke(Org { command, org }) => {
                Leaf::Revoke(command, Reach::integration(Named::Revoke, org))
            }
        })
    }
}

/// What only `useful integration` hands a leaf: whether token services are in reach, the
/// organization `--org` names, by ID or slug, whose side the leaf acts on in place of the
/// member's (REQ-INTEGRATION-16, REQ-INTEGRATION-17), and for `authorize` and `deauthorize` the
/// project `--project-id` names and the connection `--connection-id` names (REQ-INTEGRATION-33).
/// `useful grant` hands [`Reach::GRANTS`], which reaches grants alone, so its leaves send and
/// print what they did (REQ-INTEGRATION-15). A leaf reaches the token and connection
/// authorization code through this, never directly.
#[derive(Debug)]
pub(in crate::cli) struct Reach {
    tokens: bool,
    org: Option<OrganizationArg>,
    /// The project of the connection authorization the leaf acts on; none on a leaf that takes
    /// no `--project-id` and under `useful grant`.
    project: Option<ProjectArg>,
    /// The connection `--connection-id` names, which only a project's leaf takes.
    connection: Option<String>,
    /// The leaf of `useful integration` handed this, whose refusal of `--org` it words; none
    /// under `useful grant`, which takes no `--org`.
    leaf: Option<Named>,
}

/// A leaf of `useful integration`, as a refusal of `--org` speaks for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Named {
    Authorize,
    Connect,
    Deauthorize,
    List,
    Revoke,
}

/// The provider at which `useful integration connect --org` connects the organization
/// (REQ-INTEGRATION-21).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::cli) enum OrganizationSignIn {
    /// The GitHub App setup `useful connection github setup begin` begins.
    GitHub,
    /// The Linear authorization `useful connection linear authorization create` starts.
    Linear,
}

impl Reach {
    /// What `useful grant` hands its leaves: grants alone.
    pub(in crate::cli) const GRANTS: Self = Self {
        tokens: false,
        org: None,
        project: None,
        connection: None,
        leaf: None,
    };

    /// What `useful integration` hands its leaf `leaf` given `org`.
    const fn integration(leaf: Named, org: Option<OrganizationArg>) -> Self {
        Self {
            tokens: true,
            org,
            project: None,
            connection: None,
            leaf: Some(leaf),
        }
    }

    /// This, for a leaf given the project `--project-id` names and the connection
    /// `--connection-id` names.
    fn in_project(self, project: Option<ProjectArg>, connection: Option<String>) -> Self {
        Self {
            project,
            connection,
            ..self
        }
    }

    /// Whether a token service's token is in reach of the leaf.
    pub(in crate::cli) const fn tokens(&self) -> bool {
        self.tokens
    }

    /// The organization `--org` names, by ID or slug.
    pub(in crate::cli) fn org(&self) -> Option<&str> {
        self.org.as_deref()
    }

    /// The project `--project-id` names, with which the leaf acts on a connection authorization
    /// (REQ-INTEGRATION-33).
    pub(in crate::cli) fn project(&self) -> Option<&str> {
        self.project.as_deref()
    }

    /// The organization's connection authorizations the leaf at `command` acts on for `name`
    /// (REQ-INTEGRATION-33): where `--org` and `--project-id` were given and `name` is `github`,
    /// `gitlab.com` or `linear`, those of that organization at that provider in that project,
    /// with the connection `--connection-id` names; otherwise none, and the leaf finds a grant or
    /// a token, or refuses `--project-id` with [`Reach::refuse_project`].
    pub(in crate::cli) fn connection_authorizations(
        &self,
        name: &str,
        command: &'static str,
    ) -> Option<ConnectionAuthorizations<'_>> {
        Some(ConnectionAuthorizations::new(
            command,
            self.org()?,
            self.project()?,
            connection::organization::Provider::named(name)?,
            self.connection.as_deref(),
        ))
    }

    /// The side `list` states of each grant, which only `useful integration list` states
    /// (REQ-INTEGRATION-13): a grant is always the member's own.
    pub(in crate::cli) fn side(&self) -> Option<&'static str> {
        self.tokens.then_some(grant::output::MEMBER_SIDE)
    }

    /// Whose tokens the leaf acts on: the organization's `--org` names, else the member's.
    pub(in crate::cli) fn owner(&self) -> IntegrationTokenOwner<'_> {
        self.org().map_or(
            IntegrationTokenOwner::Member,
            IntegrationTokenOwner::Organization,
        )
    }

    /// The token of `service` a leaf given that name acts on (REQ-INTEGRATION-19): with `--org`
    /// the organization's, read outright; else the member's, read only once the served read
    /// names a token service, so that against a deployment that serves none no token list is
    /// read. Nothing is read where token services are out of reach.
    pub(in crate::cli) fn token_named(
        &self,
        api: &MemberGrantApi,
        service: &str,
    ) -> Result<Option<IntegrationToken>, MemberGrantFailure> {
        if !self.tokens || (self.org.is_none() && !tokens::served(api)?) {
            return Ok(None);
        }
        tokens::held(api, self.owner(), service)
    }

    /// The tokens `authorize --all` ranges over (REQ-INTEGRATION-20): none where token services
    /// are out of reach; with `--org` that organization's, read outright; else the member's,
    /// read only once the served read names a token service.
    pub(in crate::cli) fn every_token(
        &self,
        api: &MemberGrantApi,
    ) -> Result<Vec<IntegrationToken>, MemberGrantFailure> {
        if !self.tokens || (self.org.is_none() && !tokens::served(api)?) {
            return Ok(Vec::new());
        }
        tokens::every(api, self.owner())
    }

    /// The tokens `list` prints, or none where token services are out of reach, which leaves
    /// the list's document without its `tokens` and `connections` (REQ-INTEGRATION-15,
    /// REQ-INTEGRATION-19). With `--org` they are that organization's; else the member's and each
    /// of their organizations', read only where `served` says the served read named a token
    /// service, and the memberships naming those organizations are read whatever is served.
    pub(in crate::cli) fn tokens_listed(
        &self,
        api: &MemberGrantApi,
        served: bool,
    ) -> Result<Option<Listing>, MemberGrantFailure> {
        if !self.tokens {
            return Ok(None);
        }
        match self.org() {
            Some(org) => Listing::organization_side(api, org),
            None => Listing::every_side(api, served),
        }
        .map(Some)
    }

    /// `listing`, which [`Reach::tokens_listed`] read, with the connections of its
    /// organizations, each read under the member's session through the leaves' reads of the
    /// `useful connection` family, with the flow's `--allow-insecure-http`: with `--org` that
    /// organization's three reads by the reference as given, else each membership's
    /// organization's, the installations only where the member is its owner
    /// (REQ-INTEGRATION-22); then, where it shows a connection of an organization and the member
    /// is its owner or `--org` names it, the organization's connection authorizations, each under
    /// the connection it names (REQ-INTEGRATION-34). A read Cloud refuses or cannot serve is
    /// passed over; any other failure is the inner error. Nothing is read where token services
    /// are out of reach.
    pub(in crate::cli) fn connections_listed(
        &self,
        deployment: &Deployment,
        allow_insecure_http: bool,
        listing: Option<Listing>,
    ) -> anyhow::Result<Result<Option<Listing>, MemberGrantFailure>> {
        match listing.filter(|_| self.tokens) {
            Some(listing) => Ok(listing
                .with_connections(deployment, allow_insecure_http)?
                .map(Some)),
            None => Ok(Ok(None)),
        }
    }

    /// `token`, which [`Reach::token_named`] found, as a leaf shows it: an organization's with
    /// who connected it named from the organization's directory, whose failed read names nobody.
    pub(in crate::cli) fn shown(
        &self,
        api: &MemberGrantApi,
        token: &IntegrationToken,
    ) -> ListedToken {
        tokens::shown(api, token, self.org())
    }

    /// A failure of an operation on the token of `service` as the member reads it, with the
    /// remedy that names the organization `--org` names (REQ-INTEGRATION-17,
    /// REQ-INTEGRATION-19).
    pub(in crate::cli) fn report(
        &self,
        deployment: &str,
        failure: &MemberGrantFailure,
        service: &str,
    ) -> ApiFailureReport {
        output::report(deployment, failure, service, self.org())
    }

    /// The refusal of `authorize --all` that finds nothing to authorize: with `--org` an
    /// organization that holds no token (REQ-INTEGRATION-20), else a member who holds no grant
    /// or token, reported as a grant not found, as `useful grant` reports it.
    pub(in crate::cli) fn report_nothing_held(&self, deployment: &str) -> ApiFailureReport {
        match self.org() {
            Some(org) => output::no_token(org),
            None => grant::output::report(deployment, &MemberGrantFailure::GrantNotFound),
        }
    }

    /// Reports `token`, which `revoke` erased from Cloud (REQ-INTEGRATION-20).
    pub(in crate::cli) fn write_revoked(
        &self,
        deployment: &str,
        token: &IntegrationToken,
        json: bool,
    ) -> anyhow::Result<ExitCode> {
        output::write_revoked(deployment, token, self.org(), json)
    }

    /// Connects the token of `service`, a token service the deployment serves, for the member or
    /// for the organization `--org` names, read from `from_file`, standard input or a prompt that
    /// does not echo (REQ-INTEGRATION-16 to REQ-INTEGRATION-18). The options a token service does
    /// not take are refused before this is called.
    pub(in crate::cli) fn connect_token(
        &self,
        flow: &Flow<'_>,
        deployment: &Deployment,
        service: &ServedTokenService,
        from_file: Option<&Path>,
    ) -> super::CommandResult {
        connect::run(flow, deployment, service, from_file, self.org())
    }

    /// Connects the organization `--org` names at `provider` under the member's session, through
    /// the leaf of the `useful connection` family that does it, with the flow's `--json`,
    /// `--allow-insecure-http`, `--no-wait` and `--timeout`: its requests, its output and its exit
    /// status are that leaf's (REQ-INTEGRATION-21). The options of the member's sign-in are
    /// refused before this is called.
    pub(in crate::cli) fn connect_organization(
        &self,
        flow: &Flow<'_>,
        deployment: &Deployment,
        provider: OrganizationSignIn,
    ) -> super::CommandResult {
        let organization = self
            .org
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--org names no organization to connect"))?;
        match provider {
            OrganizationSignIn::GitHub => Ok(connection::github::begin_setup(
                deployment,
                organization,
                flow.json,
                flow.allow_insecure_http,
            )?),
            OrganizationSignIn::Linear => connection::authorize_workspace(
                deployment.clone(),
                organization,
                flow.json,
                flow.allow_insecure_http,
                flow.no_wait,
                flow.timeout,
            ),
        }
    }

    /// The refusal of `--org` for `name`, a provider or a grant's ID, exit 2, before anything is
    /// sent (REQ-INTEGRATION-17, REQ-INTEGRATION-21). For `github`, `gitlab.com` and `linear` an
    /// organization's connection is made and removed by `useful connection` and is authorized for
    /// a runner only by a connection authorization (REQ-INTEGRATION-26): `connect` names the
    /// steps of a GitLab connection, `authorize` and `deauthorize` the `--project-id` form that
    /// authorizes a runner for it in one project or ends that (REQ-INTEGRATION-33), `revoke`,
    /// which removes no connection, the command that removes one, and any other leaf says that a
    /// connection acts for Cloud. Anything else names a grant, the member's own: a coding
    /// agent's, whose login serves the organization its `--organization` named, where `native`,
    /// and otherwise an OAuth provider's. `example` is the leaf's command line for it without
    /// `--org`.
    pub(in crate::cli) fn org_refused(
        &self,
        name: &str,
        native: bool,
        example: &str,
    ) -> CommandFailure {
        let remedy = match (name, self.leaf) {
            ("gitlab.com", Some(Named::Connect)) => {
                "An organization's GitLab.com connection is made in several steps. Begin with:\n  \
                 useful connection gitlab create ORGANIZATION INSTANCE_URL"
                    .to_owned()
            }
            ("github" | "gitlab.com" | "linear", Some(Named::Revoke)) => format!(
                "Revoke removes no organization's connection. Remove the {name} connection \
                 with:\n  {}",
                removal(name)
            ),
            ("github" | "gitlab.com" | "linear", Some(Named::Authorize)) => format!(
                "Authorize a runner for an organization's {name} connection in one project:\n  \
                 useful integration authorize {name} RUNNER --org ORGANIZATION --project-id \
                 PROJECT --yes\nor name your own {name} grant without --org:\n  {example}"
            ),
            ("github" | "gitlab.com" | "linear", Some(Named::Deauthorize)) => format!(
                "End a runner's authorization for an organization's {name} connection in one \
                 project:\n  useful integration deauthorize {name} RUNNER --org ORGANIZATION \
                 --project-id PROJECT --yes\nor name your own {name} grant without --org:\n  \
                 {example}"
            ),
            ("github" | "gitlab.com" | "linear", _) => format!(
                "An organization's {name} connection acts for Cloud. Name your own {name} grant \
                 without --org:\n  {example}"
            ),
            _ if native => format!(
                "A {name} grant is yours, for the organization --organization names:\n  {example}"
            ),
            _ => format!("A {name} grant is yours alone:\n  {example}"),
        };
        CommandFailure::with_exit_code(
            anyhow::anyhow!("--org does not apply to {name}\n\n{remedy}"),
            ExitCode::UsageError,
        )
    }

    /// The refusal of `--project-id` for `name`, a provider name that is not a coding agent's,
    /// exit 2, before anything is sent (REQ-INTEGRATION-33): `--project-id` names the project of
    /// an organization's connection at `github`, `gitlab.com` or `linear`, which go on, and a
    /// runner's authorization for an organization's token is made or ended without it.
    pub(in crate::cli) fn refuse_project(&self, name: &str) -> Result<(), CommandFailure> {
        if connection::organization::Provider::named(name).is_some() {
            return Ok(());
        }
        let (act, leaf) = match self.leaf {
            Some(Named::Deauthorize) => ("End a runner's authorization", "deauthorize"),
            _ => ("Authorize a runner", "authorize"),
        };
        Err(CommandFailure::with_exit_code(
            anyhow::anyhow!(
                "--project-id does not apply to {name}\n\n--project-id names the project of an \
                 organization's connection at github, gitlab.com or linear. {act} for an \
                 organization's token without it:\n  useful integration {leaf} {name} RUNNER \
                 --org ORGANIZATION --yes"
            ),
            ExitCode::UsageError,
        ))
    }
}

/// The `useful connection` command that removes an organization's connection at `name`, one of
/// `github`, `gitlab.com` and `linear`, as its `--help` spells it.
fn removal(name: &str) -> &'static str {
    match name {
        "github" => "useful connection github installation remove ORGANIZATION INSTALLATION --yes",
        "linear" => "useful connection linear remove ORGANIZATION CONNECTION --yes",
        _ => {
            "useful connection gitlab remove ORGANIZATION CONNECTION --expected-version VERSION \
             --yes"
        }
    }
}
