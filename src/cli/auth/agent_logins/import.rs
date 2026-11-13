use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use clap::{Args, ValueEnum};
use zeroize::Zeroizing;

use crate::exit_code::ExitCode;
use um_api::{
    AgentLogin, AgentLoginApi, AgentLoginFailure, AgentLoginImport, AgentLoginImportProvider,
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
pub(super) enum Provider {
    #[value(name = "claude_code")]
    ClaudeCode,
    Codex,
}

impl Provider {
    pub(super) const fn of(provider: AgentLoginProvider) -> Self {
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

/// Where the sign-in comes from, for `import` and `reconnect` alike: exactly one source.
#[derive(Debug, Args)]
#[group(id = "source", required = true, multiple = false)]
pub(super) struct Source {
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
}

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = "PROVIDER", help = "Coding agent the login signs in")]
    provider: Provider,

    #[command(flatten)]
    source: Source,

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
        let material =
            match self
                .source
                .take(self.provider, deployment, self.options.json, || {
                    self.check_session_and_target(deployment)
                })? {
                Ok(material) => material,
                Err(refused) => return Ok(refused),
            };
        let import = AgentLoginImport {
            organization: &self.organization,
            provider: self.provider.wire(),
            label: self.label.as_deref(),
            material: &material,
        };
        store(
            deployment,
            &self.options,
            |api, key| match &self.agent_login {
                Some(agent_login_id) => api.replace(agent_login_id, &material, key),
                None => api.import(&import, key),
            },
            |deployment, login, json| {
                output::write_agent_login(deployment, "imported", login, json)
            },
        )
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
    /// login of another provider or organization (REQ-AGENTLOGIN-22). An organization slug is
    /// resolved only when the provider agrees.
    fn contradiction(
        &self,
        api: &AgentLoginApi,
        agent_login_id: &str,
    ) -> Result<Option<anyhow::Error>, AgentLoginFailure> {
        let login = replaceable(api, agent_login_id)?;
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
}

impl Source {
    /// Takes the sign-in `provider` accepts from this source (REQ-AGENTLOGIN-22 to
    /// REQ-AGENTLOGIN-24): reads the file or stream before `check`, refusing with exit 2 a source
    /// the provider does not take and an unsafe or malformed one, or, for `--login`, runs
    /// `codex login` after it, so that a refused session or target leaves no provider session
    /// behind. Returns the exit status `check` or a failed sign-in reported in place of the
    /// material.
    pub(super) fn take(
        &self,
        provider: Provider,
        deployment: &Deployment,
        json: bool,
        check: impl FnOnce() -> Result<Option<ExitCode>, CommandFailure>,
    ) -> Result<Result<Zeroizing<Vec<u8>>, ExitCode>, CommandFailure> {
        if self.login && matches!(provider, Provider::Codex) {
            if let Some(refused) = check()? {
                return Ok(Err(refused));
            }
            return match codex_login::sign_in(read_codex_login)? {
                Ok(material) => Ok(Ok(material)),
                Err(failure) => output::provider_login_failed(
                    deployment.fingerprint().api_url(),
                    &failure,
                    json,
                )
                .map(Err)
                .map_err(Into::into),
            };
        }
        let material = self
            .read(provider)
            .map_err(|error| CommandFailure::with_exit_code(error, ExitCode::UsageError))?;
        Ok(match check()? {
            Some(refused) => Err(refused),
            None => Ok(material),
        })
    }

    /// Reads the material from the file or stream the provider takes, before any network use.
    fn read(&self, provider: Provider) -> anyhow::Result<Zeroizing<Vec<u8>>> {
        match (provider, &self.from_file, &self.token_file) {
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

/// Reads the agent login a new generation is stored for. A revoked agent login takes no
/// generation (REQ-AGENTLOGIN-6), so it is reported as Cloud would report the replacement,
/// before anything is signed in to or sent.
pub(super) fn replaceable(
    api: &AgentLoginApi,
    agent_login_id: &str,
) -> Result<AgentLogin, AgentLoginFailure> {
    let login = api.get(agent_login_id)?;
    if login.state == AgentLoginState::AgentLoginStateRevoked {
        return Err(AgentLoginFailure::AgentLoginNotFound);
    }
    Ok(login)
}

/// Stores the material through `send` under one request key, replaying that key once when the
/// response is lost, which may have stored it, and prints the agent login through `write`.
pub(super) fn store(
    deployment: &Deployment,
    options: &Options,
    send: impl Fn(&AgentLoginApi, &str) -> Result<AgentLogin, AgentLoginFailure>,
    write: impl FnOnce(&str, &AgentLogin, bool) -> anyhow::Result<ExitCode>,
) -> CommandResult {
    let key = um_support::generate_idempotency_key().context("generate agent login request key")?;
    run_leaf(
        deployment,
        options,
        |api| match send(api, &key) {
            Err(AgentLoginFailure::Unreachable { .. }) => send(api, &key),
            result => result,
        },
        write,
    )
    .map_err(Into::into)
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
