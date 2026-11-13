use clap::{Args, ValueEnum};

use crate::cli::grant::consent::{Consent, ConsentProvider, Front, Options};
use um_human_auth::Deployment;

pub(super) const ABOUT: &str = "Create a provider grant through the provider's consent";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(
        value_name = "PROVIDER",
        hide_possible_values = true,
        help = "Provider to ask for a grant: github, gitlab.com or linear"
    )]
    provider: GrantProviderArg,

    #[arg(
        long,
        value_name = "IDENTITY",
        value_parser = parse_identity_id,
        help = "Linked identity ID to bind the grant to; omit to let the consent find or create the link"
    )]
    identity_id: Option<String>,

    #[command(flatten)]
    wait: super::super::super::WaitTimeoutArgs,

    #[arg(
        long,
        conflicts_with = "timeout",
        help = "Return after starting consent instead of waiting for the callback"
    )]
    no_wait: bool,

    #[command(flatten)]
    options: Options,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum GrantProviderArg {
    Github,
    #[value(name = "gitlab.com")]
    GitlabCom,
    Linear,
}

impl GrantProviderArg {
    const fn consent(self) -> ConsentProvider {
        match self {
            Self::Github => ConsentProvider::GITHUB,
            Self::GitlabCom => ConsentProvider::GITLAB_COM,
            Self::Linear => ConsentProvider::LINEAR,
        }
    }
}

fn parse_identity_id(value: &str) -> Result<String, String> {
    if um_support::valid_typed_id(value, "idn_") {
        Ok(value.to_owned())
    } else {
        Err("must be an exact linked identity ID".to_owned())
    }
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> super::super::super::CommandResult {
        // This earlier command never opens a browser: it prints the URL only.
        Consent {
            front: Front::AUTH,
            provider: self.provider.consent(),
            identity_id: self.identity_id,
            open: false,
            no_wait: self.no_wait,
            timeout: self.wait.timeout,
            options: self.options,
        }
        .run(deployment)
    }
}
