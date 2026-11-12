use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use clap::{ArgGroup, Args, ValueEnum};
use zeroize::Zeroizing;

use crate::exit_code::ExitCode;
use um_api::{
    AgentLoginApi, AgentLoginFailure, AgentLoginImport, AgentLoginImportProvider,
    AgentLoginProvider, AgentLoginState,
};
use um_human_auth::Deployment;

use super::super::super::secret_file::{FileMode, SecretInput, trim_line_ending};
use super::super::super::{CommandFailure, CommandResult, OrganizationArg};
use super::{Options, codex_login, output, run_leaf, with_api};

pub(super) const ABOUT: &str = "Import a coding agent login";

// The bounds Cloud enforces on material (REQ-AGENTLOGIN-2).
const CODEX_LOGIN: SecretInput = SecretInput {
    name: "Codex login",
    max_bytes: 32 * 1024,
    max_text: "32 KiB",
};
const MAX_CLAUDE_CODE_TOKEN: usize = 4096;
const CLAUDE_CODE_TOKEN: SecretInput = SecretInput {
    name: "Claude Code token",
    // One trailing LF or CRLF is trimmed after reading.
    max_bytes: MAX_CLAUDE_CODE_TOKEN as u64 + 2,
    max_text: "4 KiB",
};

/// The coding agent a login signs in, spelled as a workflow's harness kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Provider {
    #[value(name = "claude_code")]
    ClaudeCode,
    Codex,
}

impl Provider {
    const fn of(provider: AgentLoginProvider) -> Self {
        match provider {
            AgentLoginProvider::AgentLoginProviderClaudeCode => Self::ClaudeCode,
            AgentLoginProvider::AgentLoginProviderCodex => Self::Codex,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::ClaudeCode => "claude_code",
            Self::Codex => "codex",
        }
    }

    const fn wire(self) -> AgentLoginImportProvider {
        match self {
            Self::ClaudeCode => AgentLoginImportProvider::ImportAgentLoginRequestProviderClaudeCode,
            Self::Codex => AgentLoginImportProvider::ImportAgentLoginRequestProviderCodex,
        }
    }
}

#[derive(Debug, Args)]
#[command(group = ArgGroup::new("source").required(true).args(["from_file", "token_file", "login"]))]
pub(super) struct Command {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = "PROVIDER", help = "Coding agent the login signs in")]
    provider: Provider,

    #[arg(
        long,
        value_name = "PATH",
        help = "Read a Codex auth.json from an owned mode-0600 file, or - for standard input"
    )]
    from_file: Option<PathBuf>,

    #[arg(
        long,
        help = "Run codex login in a private temporary Codex home and import the login it makes"
    )]
    login: bool,

    #[arg(
        long,
        value_name = "PATH",
        help = "Read the token claude setup-token printed from an owned mode-0600 file, or - for standard input"
    )]
    token_file: Option<PathBuf>,

    #[arg(
        long,
        value_name = "LABEL",
        value_parser = super::parse_label,
        conflicts_with = "agent_login",
        help = "Name the new agent login"
    )]
    label: Option<String>,

    #[arg(
        long,
        value_name = "AGENT_LOGIN",
        help = "Store the login as the next generation of this agent login ID"
    )]
    agent_login: Option<String>,

    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> CommandResult {
        let material = if self.login && matches!(self.provider, Provider::Codex) {
            // A refused session or target must not leave a new provider session behind.
            if let Some(refused) = self.check_session_and_target(deployment)? {
                return Ok(refused);
            }
            match codex_login::sign_in(read_codex_login)? {
                Ok(material) => material,
                Err(failure) => {
                    return output::provider_login_failed(
                        deployment.fingerprint().api_url(),
                        &failure,
                        self.options.json,
                    )
                    .map_err(Into::into);
                }
            }
        } else {
            let material = self
                .read_material()
                .map_err(|error| CommandFailure::with_exit_code(error, ExitCode::UsageError))?;
            if let Some(refused) = self.check_session_and_target(deployment)? {
                return Ok(refused);
            }
            material
        };
        let key = um_support::generate_idempotency_key()
            .context("generate agent login import request key")?;
        let import = AgentLoginImport {
            organization: &self.organization,
            provider: self.provider.wire(),
            label: self.label.as_deref(),
            material: &material,
        };
        run_leaf(
            deployment,
            &self.options,
            |api| {
                let send = || match &self.agent_login {
                    Some(agent_login_id) => api.replace(agent_login_id, &material, &key),
                    None => api.import(&import, &key),
                };
                // A lost response may have stored the login. Replay only this key, once.
                match send() {
                    Err(AgentLoginFailure::Unreachable { .. }) => send(),
                    result => result,
                }
            },
            |deployment, login, json| {
                output::write_agent_login(deployment, "imported", login, json)
            },
        )
        .map_err(Into::into)
    }

    /// Establishes the human session and, with `--agent-login`, checks the agent login it
    /// names (REQ-AGENTLOGIN-24), returning the exit status of a reported refusal.
    fn check_session_and_target(
        &self,
        deployment: &Deployment,
    ) -> Result<Option<ExitCode>, CommandFailure> {
        let checked = with_api(deployment, &self.options, |api| match &self.agent_login {
            Some(agent_login_id) => self.contradiction(api, agent_login_id),
            None => Ok(None),
        })?;
        match checked {
            Ok(None) => Ok(None),
            Ok(Some(contradiction)) => Err(CommandFailure::with_exit_code(
                contradiction,
                ExitCode::UsageError,
            )),
            Err(failure) => output::failure(
                deployment.fingerprint().api_url(),
                &failure,
                self.options.json,
            )
            .map(Some)
            .map_err(Into::into),
        }
    }

    /// Reads the agent login `--agent-login` names and states how it contradicts the
    /// PROVIDER and ORGANIZATION arguments, if it does, so that no material is sent to a
    /// login of another provider or organization (REQ-AGENTLOGIN-22). A revoked agent login
    /// takes no generation (REQ-AGENTLOGIN-6), so it is reported as Cloud would report the
    /// replacement, before anything is signed in to or sent. An organization slug is
    /// resolved only when the provider agrees.
    fn contradiction(
        &self,
        api: &AgentLoginApi,
        agent_login_id: &str,
    ) -> Result<Option<anyhow::Error>, AgentLoginFailure> {
        let login = api.get(agent_login_id)?;
        if login.state == AgentLoginState::AgentLoginStateRevoked {
            return Err(AgentLoginFailure::AgentLoginNotFound);
        }
        let provider = Provider::of(login.provider);
        if provider != self.provider {
            return Ok(Some(anyhow!(
                "--agent-login {agent_login_id} is a {} login, not {}\n\n\
                 Name its provider, or omit --agent-login to import a new agent login.",
                provider.name(),
                self.provider.name(),
            )));
        }
        let organization: &str = &self.organization;
        let named = if um_support::valid_typed_id(organization, "org_") {
            organization.to_owned()
        } else {
            api.organization_id(organization)?
        };
        if named != login.organization_id {
            return Ok(Some(anyhow!(
                "--agent-login {agent_login_id} belongs to organization {}, not {organization}\n\n\
                 Name its organization, or omit --agent-login to import a new agent login.",
                login.organization_id,
            )));
        }
        Ok(None)
    }

    /// Reads the material from the file or stream the provider takes, before any network use.
    fn read_material(&self) -> anyhow::Result<Zeroizing<Vec<u8>>> {
        match (self.provider, &self.from_file, &self.token_file) {
            (Provider::Codex, Some(path), None) => read_codex_login(path, FileMode::Private),
            (Provider::ClaudeCode, None, Some(path)) => read_claude_code_token(path),
            (Provider::Codex, _, _) => {
                bail!("a Codex login is read with --from-file or made with --login")
            }
            (Provider::ClaudeCode, _, _) => {
                bail!("a Claude Code token is read with --token-file")
            }
        }
    }
}

/// Reads a Codex `auth.json` to send byte for byte. Only its shape is checked: one JSON
/// object, as Cloud requires; its fields are never read. `mode` is checked for a file the
/// member names, not for one `codex login` wrote in the command's private home.
fn read_codex_login(path: &Path, mode: FileMode) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    let bytes = CODEX_LOGIN.read_with(path, mode)?;
    let object = bytes.iter().find(|byte| !byte.is_ascii_whitespace()) == Some(&b'{');
    // The parser's error can quote the input, so only the fact of failure is kept.
    if !object || serde_json::from_slice::<serde::de::IgnoredAny>(&bytes).is_err() {
        bail!("Codex login must be the JSON object of a Codex auth.json");
    }
    Ok(bytes)
}

/// Reads the token `claude setup-token` printed: one line of printable characters without
/// spaces.
fn read_claude_code_token(path: &Path) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    let mut bytes = CLAUDE_CODE_TOKEN.read(path)?;
    trim_line_ending(&mut bytes);
    if bytes.is_empty()
        || bytes.len() > MAX_CLAUDE_CODE_TOKEN
        || !bytes.iter().all(|byte| (b'!'..=b'~').contains(byte))
    {
        bail!(
            "Claude Code token must be one line of 1 to 4096 printable characters without spaces"
        );
    }
    Ok(bytes)
}
