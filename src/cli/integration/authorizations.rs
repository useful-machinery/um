//! What `useful integration authorize` and `deauthorize` given `--org` and `--project-id` act on
//! (REQ-INTEGRATION-33): the organization's connection at one provider, read through the
//! connection family and chosen, the organization's connection authorizations in force for the
//! project, read to offer a runner or to find the one to end, and the authorization made or
//! ended, each through the member grant client's connection authorization routes
//! (REQ-INTEGRATION-27 to REQ-INTEGRATION-29). A generic
//! `not-found` from one of those routes is resolved by one read of the organization, since a
//! deployment that routes none of them answers it as it answers an organization the member
//! cannot read. `useful integration list` reads every authorization in force of an organization
//! to print under its connections (REQ-INTEGRATION-34), and passes that generic `not-found` over
//! with no such read.

use useful_api::{
    ConnectionAuthorization, ConnectionAuthorizationFilter, ConnectionAuthorizationRequest,
    MemberGrantApi, MemberGrantFailure, UnreachableCategory,
};
use useful_execution::visible_text;
use useful_human_auth::Deployment;

use super::output;
use crate::cli::connection::organization::{self, OrganizationConnection, Provider};
use crate::cli::grant;
use crate::cli::{ApiFailureReport, HttpOptions};
use crate::exit_code::ExitCode;

// The largest page the connection authorization list serves.
const PAGE: i32 = 200;

/// Every connection authorization in force of the organization `organization` names, by ID or
/// slug, oldest first, read to the last page under the member's session with
/// `allow_insecure_http`, which `useful integration list` prints under the connections they name
/// (REQ-INTEGRATION-34). `None` where Cloud refused the read, as it refuses a member who is not an
/// owner, answered the generic `not-found`, as a deployment that does not route it does, or
/// answered that it cannot serve it: the list passes those over. Any other failure is the inner
/// error.
pub(in crate::cli) fn of_organization(
    deployment: &Deployment,
    organization: &str,
    allow_insecure_http: bool,
) -> anyhow::Result<Result<Option<Vec<ConnectionAuthorization>>, MemberGrantFailure>> {
    let policy = HttpOptions {
        allow_insecure_http,
    }
    .transport_policy();
    let read = crate::cli::with_human_session_api(deployment, policy, |api: &MemberGrantApi| {
        grant::every_page(
            MemberGrantFailure::InvalidResponse {
                credential_rejected: false,
            },
            |cursor| {
                api.connection_authorizations(
                    organization,
                    ConnectionAuthorizationFilter::default(),
                    Some(PAGE),
                    cursor,
                )
                .map(|page| (page.items, page.next_cursor))
            },
        )
    })?;
    Ok(match read {
        Ok(authorizations) => Ok(Some(authorizations)),
        Err(
            MemberGrantFailure::Forbidden
            | MemberGrantFailure::OrganizationNotFound
            | MemberGrantFailure::Unreachable {
                category: UnreachableCategory::Server,
            },
        ) => Ok(None),
        Err(failure) => Err(failure),
    })
}

/// The state word of a connection a runner can be authorized for.
const ACTIVE: &str = "active";

/// The organization's connection authorizations at one provider in one project, which a leaf of
/// `useful integration` given `--org` and `--project-id` acts on: the organization by the
/// reference `--org` gave, the project, the provider and the connection `--connection-id`
/// names, and the command line of the leaf that names them.
pub(in crate::cli) struct ConnectionAuthorizations<'a> {
    command: &'static str,
    organization: &'a str,
    project_id: &'a str,
    provider: Provider,
    connection_id: Option<&'a str>,
}

/// A failure of an operation on a connection authorization route, as the command reports it.
pub(in crate::cli) enum RouteFailure {
    /// The failure the member grant client answered.
    Failed(MemberGrantFailure),
    /// The deployment routes no connection authorization: a route answered the generic
    /// `not-found` while the organization answers its read.
    NotServed,
}

impl RouteFailure {
    /// What `read`, an operation on a connection authorization route run under the member's
    /// session, answered: its value, or the failure of the route or of the session.
    pub(in crate::cli) fn answered<T>(
        read: Result<Result<T, Self>, MemberGrantFailure>,
    ) -> Result<T, Self> {
        read.map_err(Self::Failed).and_then(|answered| answered)
    }
}

impl<'a> ConnectionAuthorizations<'a> {
    pub(super) const fn new(
        command: &'static str,
        organization: &'a str,
        project_id: &'a str,
        provider: Provider,
        connection_id: Option<&'a str>,
    ) -> Self {
        Self {
            command,
            organization,
            project_id,
            provider,
            connection_id,
        }
    }

    /// The organization, by the reference `--org` gave.
    pub(in crate::cli) const fn organization(&self) -> &'a str {
        self.organization
    }

    /// The provider's wire name.
    pub(in crate::cli) const fn provider(&self) -> &'static str {
        self.provider.name()
    }

    /// Whether `--connection-id` named the connection.
    pub(in crate::cli) const fn names_connection(&self) -> bool {
        self.connection_id.is_some()
    }

    /// The leaf's command line for `runner`, with `--org`, `--project-id` and, where one is
    /// named, `--connection-id`.
    pub(in crate::cli) fn command_line(&self, runner: &str, connection_id: Option<&str>) -> String {
        let connection =
            connection_id.map_or_else(String::new, |id| format!(" --connection-id {id}"));
        format!(
            "{} {} {runner} --org {} --project-id {}{connection} --yes",
            self.command,
            self.provider(),
            self.organization,
            self.project_id
        )
    }

    /// The connection `runner_id`, where the command line names one, is authorized for, read
    /// strictly at the provider through the connection family with the member's session and
    /// `allow_insecure_http`: the one `--connection-id` names among every connection the read
    /// lists, whatever its state, else the one whose state is `active`. Otherwise the command
    /// ends here, having reported it: several `active` and none named, exit 2, naming each by ID
    /// and account with the command line that names one; none, or none the read lists by the
    /// ID named, exit 1 (`connection_not_found`); or the read's failure.
    pub(in crate::cli) fn connection(
        &self,
        deployment: &Deployment,
        allow_insecure_http: bool,
        runner_id: Option<&str>,
        json: bool,
    ) -> anyhow::Result<Result<OrganizationConnection, ExitCode>> {
        let api_url = deployment.fingerprint().api_url();
        let read = organization::at_provider(
            deployment,
            self.organization,
            self.provider,
            allow_insecure_http,
        )?;
        let connections = match read {
            Ok(connections) => connections,
            Err(failure) => {
                return Ok(Err(self
                    .report(api_url, &RouteFailure::Failed(failure))
                    .write(api_url, json)?));
            }
        };
        let not_found = || {
            output::no_connection(self.organization, self.provider, self.connection_id)
                .write(api_url, json)
                .map(Err)
        };
        if let Some(named) = self.connection_id {
            return match connections
                .into_iter()
                .find(|connection| connection.id == named)
            {
                Some(connection) => Ok(Ok(connection)),
                None => not_found(),
            };
        }
        let mut active: Vec<_> = connections
            .into_iter()
            .filter(|connection| connection.state == ACTIVE)
            .collect();
        match active.len() {
            0 => not_found(),
            1 => Ok(Ok(active.remove(0))),
            _ => {
                let example = self.command_line(
                    runner_id.unwrap_or("RUNNER"),
                    active.first().map(|connection| connection.id.as_str()),
                );
                output::write_connection_required(api_url, self.provider, &active, &example, json)
                    .map(Err)
            }
        }
    }

    /// The organization's authorizations in force for the project and the provider, of
    /// `runner_id` alone where one is given, read to the last page (REQ-INTEGRATION-28): those
    /// that hold the project and the provider for a runner, whichever connection they name.
    pub(in crate::cli) fn in_force(
        &self,
        api: &MemberGrantApi,
        runner_id: Option<&str>,
    ) -> Result<Result<Vec<ConnectionAuthorization>, RouteFailure>, MemberGrantFailure> {
        self.listed(api, runner_id, None)
    }

    /// The authorizations [`ConnectionAuthorizations::in_force`] reads, of the connection
    /// `--connection-id` names alone where it names one: those `deauthorize` can end.
    pub(in crate::cli) fn to_end(
        &self,
        api: &MemberGrantApi,
        runner_id: Option<&str>,
    ) -> Result<Result<Vec<ConnectionAuthorization>, RouteFailure>, MemberGrantFailure> {
        self.listed(api, runner_id, self.connection_id)
    }

    /// The organization's authorizations in force for the project and the provider, of
    /// `runner_id` and `connection_id` alone where each is given, read to the last page.
    fn listed(
        &self,
        api: &MemberGrantApi,
        runner_id: Option<&str>,
        connection_id: Option<&str>,
    ) -> Result<Result<Vec<ConnectionAuthorization>, RouteFailure>, MemberGrantFailure> {
        let filter = ConnectionAuthorizationFilter {
            project_id: Some(self.project_id),
            runner_id,
            provider: Some(self.provider()),
            connection_id,
        };
        let invalid = MemberGrantFailure::InvalidResponse {
            credential_rejected: false,
        };
        let read = grant::every_page(invalid, |cursor| {
            api.connection_authorizations(self.organization, filter, Some(PAGE), cursor)
                .map(|page| (page.items, page.next_cursor))
        });
        self.resolved(api, read)
    }

    /// Authorizes `runner_id` for `connection_id` in the project under `key`, once more under
    /// the same key when the answer was lost (REQ-INTEGRATION-27).
    pub(in crate::cli) fn authorize(
        &self,
        api: &MemberGrantApi,
        connection_id: &str,
        runner_id: &str,
        key: &str,
    ) -> Result<Result<ConnectionAuthorization, RouteFailure>, MemberGrantFailure> {
        let request = ConnectionAuthorizationRequest {
            project_id: self.project_id,
            runner_id,
            provider: self.provider(),
            connection_id,
        };
        let made = grant::replayed(|| api.authorize_connection(self.organization, &request, key));
        self.resolved(api, made)
    }

    /// Ends the authorization `authorization_id` under `key`, once more under the same key when
    /// the answer was lost (REQ-INTEGRATION-29).
    pub(in crate::cli) fn end(
        &self,
        api: &MemberGrantApi,
        authorization_id: &str,
        key: &str,
    ) -> Result<Result<(), RouteFailure>, MemberGrantFailure> {
        let ended = grant::replayed(|| {
            api.end_connection_authorization(self.organization, authorization_id, key)
        });
        self.resolved(api, ended)
    }

    /// `answer`, an answer of a connection authorization route, with a generic `not-found`
    /// resolved by one read of the organization: one that answers means the deployment does not
    /// serve the route, and one that does not is not found. Any other failure, and a failure of
    /// that read, is the outer error, so that a rejected session is refreshed.
    fn resolved<T>(
        &self,
        api: &MemberGrantApi,
        answer: Result<T, MemberGrantFailure>,
    ) -> Result<Result<T, RouteFailure>, MemberGrantFailure> {
        match answer {
            Ok(value) => Ok(Ok(value)),
            Err(MemberGrantFailure::OrganizationNotFound) => {
                Ok(Err(if api.organization_answers(self.organization)? {
                    RouteFailure::NotServed
                } else {
                    RouteFailure::Failed(MemberGrantFailure::OrganizationNotFound)
                }))
            }
            Err(failure) => Err(failure),
        }
    }

    /// `failure` as the command reports it, worded with the organization and the provider.
    pub(in crate::cli) fn report(
        &self,
        deployment: &str,
        failure: &RouteFailure,
    ) -> ApiFailureReport {
        output::connection_authorization_report(
            deployment,
            failure,
            self.organization,
            self.provider,
        )
    }

    /// Reports the authorization made (REQ-INTEGRATION-32, REQ-INTEGRATION-33).
    pub(in crate::cli) fn write_authorized(
        &self,
        deployment: &str,
        authorization: &ConnectionAuthorization,
        json: bool,
    ) -> anyhow::Result<ExitCode> {
        output::write_connection_authorized(deployment, authorization, json)
    }

    /// Reports the authorization ended (REQ-INTEGRATION-33).
    pub(in crate::cli) fn write_deauthorized(
        &self,
        deployment: &str,
        authorization: &ConnectionAuthorization,
        json: bool,
    ) -> anyhow::Result<ExitCode> {
        output::write_connection_deauthorized(deployment, authorization, json)
    }

    /// The heading of the statement of authorizing the runner `runner`, as the statement names
    /// it, for `connection`: the runner, the connection by ID and account, whose account is
    /// another party's text, and the project.
    pub(in crate::cli) fn heading(
        &self,
        runner: &str,
        connection: &OrganizationConnection,
    ) -> String {
        format!(
            "Authorizing runner {runner} for {}:",
            self.connection_named(&connection.id, connection.account.as_deref())
        )
    }

    /// The heading of the statement of ending the runner `runner`'s authorization
    /// `authorization`, named as [`ConnectionAuthorizations::heading`] names it.
    pub(in crate::cli) fn ending_heading(
        &self,
        runner: &str,
        authorization: &ConnectionAuthorization,
    ) -> String {
        format!(
            "Ending runner {runner}'s authorization for {}:",
            self.connection_named(
                &authorization.connection_id,
                Some(&authorization.connection_account)
            )
        )
    }

    /// The connection `connection_id` on the account `account`, which is another party's text,
    /// with the organization and the project, as a statement's heading names it.
    fn connection_named(&self, connection_id: &str, account: Option<&str>) -> String {
        let account = account.map_or_else(String::new, |account| {
            format!(" ({})", visible_text(account))
        });
        format!(
            "the {} connection {connection_id}{account} of {} in project {}",
            self.provider(),
            self.organization,
            self.project_id
        )
    }
}
