//! `reconnect AGENT_LOGIN`: `import --agent-login` found by ID (REQ-GRANT-54). The agent
//! login it reads gives the provider and organization, and the sign-in is then taken and stored
//! exactly as `import` takes and stores it.

use clap::Args;

use um_human_auth::Deployment;

use super::super::super::CommandResult;
use super::import::{self, Provider, Source};
use super::{Options, output, with_api};

pub(super) const ABOUT: &str = "Store a new sign-in as a coding agent login's next generation";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "AGENT_LOGIN", help = "Agent login ID")]
    agent_login_id: String,

    #[command(flatten)]
    source: Source,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> CommandResult {
        let read = with_api(deployment, &self.options, |api| {
            import::replaceable(api, &self.agent_login_id)
        })?;
        let login = match read {
            Ok(login) => login,
            Err(failure) => {
                return output::failure(
                    deployment.fingerprint().api_url(),
                    &failure,
                    self.options.json,
                )
                .map_err(Into::into);
            }
        };
        // The read established the session and the target: nothing is left to check.
        let material = match self.source.take(
            Provider::of(login.provider),
            deployment,
            self.options.json,
            || Ok(None),
        )? {
            Ok(material) => material,
            Err(refused) => return Ok(refused),
        };
        import::store(
            deployment,
            &self.options,
            |api, key| api.replace(&self.agent_login_id, &material, key),
            output::write_reconnected,
        )
    }
}
