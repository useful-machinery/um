use clap::Args;

use crate::exit_code::ExitCode;
use um_api::{AgentLogin, AgentLoginApi, AgentLoginFailure, Grant, GrantApi, GrantFailure};
use um_human_auth::Deployment;

use super::super::super::{OrganizationArg, with_human_session_api};
use super::super::{agent_logins, grants};
use super::{Options, output};

pub(super) const ABOUT: &str =
    "Show the availability and next action of your provider grants and agent logins";

// The largest page either list serves.
const PAGE: i32 = 200;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(
        long,
        value_name = OrganizationArg::VALUE_NAME,
        help = "Keep only this organization's agent logins, by organization ID or exact slug"
    )]
    organization: Option<OrganizationArg>,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let policy = self.options.http.transport_policy();
        let json = self.options.json;
        let logins = with_human_session_api(deployment, policy, |api| self.agent_logins(api))?;
        let api_url = deployment.fingerprint().api_url();
        let logins = match logins {
            Ok(logins) => logins,
            Err(failure) => return agent_logins::failure(api_url, &failure, json),
        };
        let grants = with_human_session_api(deployment, policy, read_grants)?;
        match grants {
            Ok(grants) => output::write_status(api_url, &grants, &logins, json),
            Err(failure) => grants::failure(api_url, &failure, json),
        }
    }

    /// Every agent login of the member, or of the organization `--organization` names.
    fn agent_logins(&self, api: &AgentLoginApi) -> Result<Vec<AgentLogin>, AgentLoginFailure> {
        let organization_id = match self.organization.as_deref() {
            None => None,
            Some(id) if um_support::valid_typed_id(id, "org_") => Some(id.to_owned()),
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

/// Reads a list to its last page. A page whose next cursor is the one it was read with would
/// never end the list, and is refused as `invalid`.
fn every_page<T, E>(
    invalid: E,
    mut read: impl FnMut(Option<&str>) -> Result<(Vec<T>, Option<String>), E>,
) -> Result<Vec<T>, E> {
    let mut items = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) = read(cursor.as_deref())?;
        items.extend(page);
        match next {
            None => return Ok(items),
            Some(next) if cursor.as_deref() == Some(next.as_str()) => return Err(invalid),
            Some(next) => cursor = Some(next),
        }
    }
}
