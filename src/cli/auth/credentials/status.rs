use clap::Args;

use crate::exit_code::ExitCode;
use useful_api::{
    AgentLogin, AgentLoginApi, AgentLoginFailure, Grant, GrantApi, GrantFailure, MemberGrantApi,
    MemberGrantFailure,
};
use useful_human_auth::Deployment;

use super::super::super::grant::every_page;
use super::super::super::{OrganizationArg, with_human_session_api};
use super::super::{agent_logins, grants};
use super::{Options, output};

pub(super) const ABOUT: &str =
    "Show the availability and next action of your provider grants and agent logins";

// The largest page either list serves.
const PAGE: i32 = 200;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(long, value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: Option<OrganizationArg>,

    #[command(flatten)]
    options: Options,
}

impl Command {
    /// Reads the member's agent logins, then their grants, then the providers the deployment
    /// serves, to name the other OAuth providers they hold nothing of (REQ-GRANT-55,
    /// REQ-GRANT-96). A deployment that predates the served read names none; a read that fails
    /// otherwise is reported as a failed grant read is.
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let policy = self.options.http.transport_policy();
        let json = self.options.json;
        let logins = with_human_session_api(deployment, policy, |api| self.agent_logins(api))?;
        let api_url = deployment.fingerprint().api_url();
        let logins = match logins {
            Ok(logins) => logins,
            Err(failure) => return agent_logins::failure(api_url, &failure, json),
        };
        let grants = match with_human_session_api(deployment, policy, read_grants)? {
            Ok(grants) => grants,
            Err(failure) => return grants::failure(api_url, &failure, json),
        };
        let served = with_human_session_api(deployment, policy, MemberGrantApi::served_providers)?;
        match served {
            Ok(served) => {
                output::write_status(api_url, &grants, &logins, &served.unwrap_or_default(), json)
            }
            Err(failure) => grants::failure(api_url, &grant_failure(failure), json),
        }
    }

    /// Every agent login of the member, or of the organization `--organization` names.
    fn agent_logins(&self, api: &AgentLoginApi) -> Result<Vec<AgentLogin>, AgentLoginFailure> {
        let organization_id = match self.organization.as_deref() {
            None => None,
            Some(id) if useful_support::valid_typed_id(id, "org_") => Some(id.to_owned()),
            Some(slug) => Some(api.organization_id(slug)?),
        };
        let invalid = AgentLoginFailure::InvalidResponse {
            credential_rejected: false,
        };
        let mut logins = every_page(invalid, |cursor| {
            api.list(Some(PAGE), cursor)
                .map(|page| (page.items, page.next_cursor))
        })?;
        if let Some(organization_id) = organization_id {
            logins.retain(|login| login.organization_id == organization_id);
        }
        Ok(logins)
    }
}

/// A failed read of the providers the deployment serves as the failure of a grant read, which
/// the command reports it as.
fn grant_failure(failure: MemberGrantFailure) -> GrantFailure {
    match failure {
        MemberGrantFailure::Unauthenticated => GrantFailure::Unauthenticated,
        MemberGrantFailure::Forbidden => GrantFailure::Forbidden,
        MemberGrantFailure::InvalidInput => GrantFailure::InvalidInput,
        MemberGrantFailure::IdempotencyConflict => GrantFailure::IdempotencyConflict,
        MemberGrantFailure::ProviderUnavailable => GrantFailure::ProviderUnavailable,
        MemberGrantFailure::GrantNotFound => GrantFailure::NotFound,
        MemberGrantFailure::RateLimited { retry_after } => {
            GrantFailure::RateLimited { retry_after }
        }
        MemberGrantFailure::Unreachable { category } => GrantFailure::Unreachable { category },
        MemberGrantFailure::InvalidResponse {
            credential_rejected,
        } => GrantFailure::InvalidResponse {
            credential_rejected,
        },
        // The served read names no organization, material, runner, authorization, integration
        // token or connection authorization, so these answers are not ones it defines.
        MemberGrantFailure::OrganizationNotFound
        | MemberGrantFailure::MaterialInvalid
        | MemberGrantFailure::LimitReached
        | MemberGrantFailure::RunnerNotFound
        | MemberGrantFailure::RunnerHoldsLogin
        | MemberGrantFailure::AlreadyAuthorized
        | MemberGrantFailure::AuthorizationNotFound
        | MemberGrantFailure::IntegrationTokenInvalid
        | MemberGrantFailure::IntegrationTokenExists
        | MemberGrantFailure::IntegrationTokenNotFound
        | MemberGrantFailure::IntegrationTokenRunnerNotFound
        | MemberGrantFailure::IntegrationTokenAlreadyAuthorized
        | MemberGrantFailure::IntegrationTokenAuthorizationNotFound
        | MemberGrantFailure::ConnectionAuthorizationRefused(_) => GrantFailure::InvalidResponse {
            credential_rejected: false,
        },
    }
}

/// Every provider grant of the member.
fn read_grants(api: &GrantApi) -> Result<Vec<Grant>, GrantFailure> {
    let invalid = GrantFailure::InvalidResponse {
        credential_rejected: false,
    };
    every_page(invalid, |cursor| {
        api.list(Some(PAGE), cursor)
            .map(|page| (page.items, page.next_cursor))
    })
}
