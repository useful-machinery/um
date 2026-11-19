//! Agent-login setup for the serving runner: asking Cloud which agent logins
//! the runner may hold, fetching one delivery at a time, keeping it in the
//! runner's own agent-login directory, recording each provider's readiness,
//! and offering what it keeps to each assignment it admits, once it has set
//! them up for that admission (REQ-GRANT-49), noting how the provider
//! answered each use of what it offered (REQ-GRANT-52), and checking a
//! login it holds with its provider (REQ-GRANT-90). Each admission also
//! asks Cloud for the grant tokens of its own assignment and keeps them
//! until the assignment ends (REQ-GRANT-86). Where each provider's
//! credential comes from, the host or a grant, is settled by one rule,
//! `credential_source`, which the readiness document, the offer and what
//! the runner reports all take (REQ-GRANT-92, INV-RUNNER-31).

pub(crate) mod check;
pub(crate) mod claude_code;
pub(crate) mod cloud;
pub(crate) mod codex;
pub(crate) mod directory;
pub(crate) mod grant_token;
pub(crate) mod readiness;
pub(crate) mod setup;

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use useful_execution::{CLAUDE_CODE_LOGIN_VARIABLE, CODEX_LOGIN_VARIABLE, EnvironmentSnapshot};

use crate::control_protocol::{
    AgentLoginCheckCause, AgentLoginProvider, AgentLoginState, CredentialSource,
};
use crate::telemetry;
use directory::LoginDirectory;

/// A token the runner keeps in a file of its own, a Claude Code login or a
/// grant access token, is one line of at most this many printable
/// characters (REQ-GRANT-12, REQ-GRANT-86).
pub(crate) const TOKEN_LIMIT: usize = 4096;

/// Whether `bytes` are one such token: one line of 1 to `TOKEN_LIMIT`
/// printable characters without spaces, the form Cloud accepts and an
/// environment variable can carry.
pub(crate) fn valid_token(bytes: &[u8]) -> bool {
    (1..=TOKEN_LIMIT).contains(&bytes.len()) && bytes.iter().all(u8::is_ascii_graphic)
}

/// The serving process's environment as far as standing back for the
/// service's own credentials needs it. Setup takes it as a value so that the
/// decision rests on the service's environment, which only the serving
/// process has. It keeps the name of every variable the service carries,
/// never a value, so that whether the host provides an OAuth provider can be
/// answered for any name, one first seen in Cloud's list of an assignment's
/// providers included (REQ-GRANT-87); whether the service carries
/// `CLAUDE_CODE_OAUTH_TOKEN` or `CODEX_HOME`; and where its `HOME` and
/// `CLAUDE_CONFIG_DIR` are, so that a login file under them can be looked
/// for.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ServiceEnvironment {
    home: Option<OsString>,
    claude_config_dir: Option<OsString>,
    claude_code_oauth_token: bool,
    codex_home: bool,
    /// The name of every variable the service carries.
    names: BTreeSet<OsString>,
}

impl ServiceEnvironment {
    /// The environment of a service whose variables are `variables`, for
    /// example `std::env::vars_os()`: the name of each, and the values of
    /// `HOME` and `CLAUDE_CONFIG_DIR` alone.
    pub(crate) fn from_variables<I, Name, Value>(variables: I) -> Self
    where
        I: IntoIterator<Item = (Name, Value)>,
        Name: Into<OsString>,
        Value: Into<OsString>,
    {
        let mut environment = Self::default();
        for (name, value) in variables {
            let name = name.into();
            if name == "HOME" {
                environment.home = Some(value.into());
            } else if name == "CLAUDE_CONFIG_DIR" {
                environment.claude_config_dir = Some(value.into());
            }
            environment.names.insert(name);
        }
        environment.claude_code_oauth_token = environment.carries(CLAUDE_CODE_LOGIN_VARIABLE);
        environment.codex_home = environment.carries(CODEX_LOGIN_VARIABLE);
        environment
    }

    /// The environment of the service whose process environment is
    /// `process`.
    pub(crate) fn of(process: &EnvironmentSnapshot) -> Self {
        Self::from_variables(
            process
                .variables()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone())),
        )
    }

    /// Whether the service carries the variable `name`, whatever its value.
    pub(crate) fn carries(&self, name: &str) -> bool {
        self.names.contains(OsStr::new(name))
    }

    /// Whether the host provides the OAuth provider: the service carries
    /// either of its two names, so the runner asks for and offers no grant
    /// token of it, and both names stay as the service has them
    /// (REQ-GRANT-87).
    pub(crate) fn provides(&self, provider: AgentLoginProvider) -> bool {
        grant_token::OFFERED_VARIABLES
            .iter()
            .any(|&(offered, variable, file_variable)| {
                offered == provider && (self.carries(variable) || self.carries(file_variable))
            })
    }

    /// `relative` under the service's `HOME`; none while `HOME` is unset or
    /// not absolute.
    fn under_home(&self, relative: &str) -> Option<PathBuf> {
        self.home
            .as_ref()
            .map(PathBuf::from)
            .filter(|home| home.is_absolute())
            .map(|home| home.join(relative))
    }
}

/// Whether a file of the service's own login is present at `path`. It is
/// only looked at, never opened or followed, and one the runner cannot tell
/// is absent counts as present (REQ-GRANT-45, REQ-GRANT-51).
fn login_file_present(path: &Path) -> bool {
    match fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(error) => error.kind() != io::ErrorKind::NotFound,
    }
}

/// Why a delivery was not kept.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstallRefusal {
    /// The delivery is older than the generation the runner installed.
    StaleGeneration,
    /// The runner's directory or the file could not be written, or the
    /// material is not a login of the provider's form.
    InstallFailed(StepFailure),
}

impl InstallRefusal {
    pub(crate) const fn state(self) -> AgentLoginState {
        match self {
            Self::StaleGeneration => AgentLoginState::StaleGeneration,
            Self::InstallFailed(_) => AgentLoginState::InstallFailed,
        }
    }

    /// The step that failed, for a refusal that is a failure.
    pub(crate) const fn failure(self) -> Option<StepFailure> {
        match self {
            Self::StaleGeneration => None,
            Self::InstallFailed(failure) => Some(failure),
        }
    }

    /// The refusal of an install whose `stage` failed with `error`.
    pub(crate) fn failed(stage: FailureStage, error: &io::Error) -> Self {
        Self::InstallFailed(StepFailure {
            stage,
            cause: FailureCause::Io(error.kind()),
        })
    }
}

/// A step of agent-login setup for one provider that failed, as the
/// `runner.agent_login_setup` record names it (REQ-TELEMETRY-45): closed
/// values only, never a path, a message or material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StepFailure {
    pub(crate) stage: FailureStage,
    pub(crate) cause: FailureCause,
}

/// Where a provider's setup failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureStage {
    /// Asking Cloud for the runner's status.
    Status,
    /// Asking Cloud for a delivery, or keeping material that is not a login
    /// of the provider's form.
    Delivery,
    /// Making the runner's directory or its Codex home.
    Directory,
    /// Writing a file in it.
    Write,
    /// Removing the provider's file from it.
    Remove,
    /// Checking the provider's installed login with its provider, which
    /// gave no answer (REQ-GRANT-90).
    Check,
}

impl FailureStage {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Delivery => "delivery",
            Self::Directory => "directory",
            Self::Write => "write",
            Self::Remove => "remove",
            Self::Check => "check",
        }
    }
}

/// Why a step failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureCause {
    /// An operating-system call failed with this kind.
    Io(io::ErrorKind),
    /// Cloud answered with a status the request does not expect.
    HttpStatus(u16),
    /// Cloud gave no answer: the connection failed or timed out.
    Transport,
    /// Cloud's answer, or the material it delivered, is not one the runner
    /// may trust.
    InvalidResponse,
    /// A check of the login gave no answer for this reason.
    Check(AgentLoginCheckCause),
}

impl FailureCause {
    /// The closed class the record carries; an HTTP status's code is a
    /// separate attribute.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Io(kind) => telemetry::io_error_class(kind),
            Self::HttpStatus(_) => "http_status",
            Self::Transport => "transport",
            Self::InvalidResponse => "invalid_response",
            Self::Check(cause) => cause.as_str(),
        }
    }
}

/// Where `provider`'s credential comes from for an assignment the runner
/// admits now (REQ-GRANT-92, INV-RUNNER-31): `host` for an OAuth provider
/// the service environment `service` provides (REQ-GRANT-87) and for a
/// coding agent the service has a login of its own of (REQ-GRANT-51),
/// whatever Cloud answered; `grant` for an OAuth provider whose token
/// `grant_tokens` holds and can read back, and for a coding agent whose
/// login the runner keeps in `directory` and the offer therefore adds; none
/// otherwise. It reads what the offer reads, through the same functions, so
/// the source recorded, reported and offered never disagree.
pub(crate) fn credential_source(
    provider: AgentLoginProvider,
    service: &ServiceEnvironment,
    directory: Option<&LoginDirectory>,
    grant_tokens: Option<&grant_token::AssignmentGrantTokens>,
) -> Option<CredentialSource> {
    let (host, grant) = match provider {
        AgentLoginProvider::Codex => (
            codex::foreign_login(service),
            directory.is_some_and(|directory| codex::offered_home(service, directory).is_some()),
        ),
        AgentLoginProvider::ClaudeCode => (
            claude_code::foreign_login(service),
            directory
                .is_some_and(|directory| claude_code::offered_token(service, directory).is_some()),
        ),
        AgentLoginProvider::Github | AgentLoginProvider::Gitlab | AgentLoginProvider::Linear => (
            service.provides(provider),
            grant_tokens.is_some_and(|tokens| tokens.holds(provider)),
        ),
    };
    if host {
        Some(CredentialSource::Host)
    } else if grant {
        Some(CredentialSource::Grant)
    } else {
        None
    }
}

/// A provider's credential source as an admission settled it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProviderSource {
    pub(crate) provider: AgentLoginProvider,
    pub(crate) source: CredentialSource,
}

/// What an admission is offered: the environment its steps draw from
/// (REQ-GRANT-51, INV-RUNNER-17), and where each provider's credential in it
/// comes from (REQ-GRANT-92).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Offer {
    /// The process environment, with each agent login the runner offers
    /// added as the variable its coding agent reads.
    pub(crate) environment: EnvironmentSnapshot,
    /// Each variable added to `environment` whose login the readiness
    /// document records as installed, with that install: the uses an
    /// admission notes (REQ-GRANT-52).
    pub(crate) offered: Vec<OfferedLogin>,
    /// Each provider that has a source, in `AgentLoginProvider::ALL`'s
    /// order, as `credential_source` settles it from the process
    /// environment, the directory and the grant tokens the offer is built
    /// from (INV-RUNNER-31).
    pub(crate) sources: Vec<ProviderSource>,
}

/// A login the runner added to an admission's environment, as the
/// readiness document records its install.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OfferedLogin {
    /// The variable its coding agent reads.
    pub(crate) variable: &'static str,
    pub(crate) provider: AgentLoginProvider,
    pub(crate) agent_login_id: String,
    pub(crate) generation: u64,
}

/// What the admission is offered with the agent logins the runner keeps in
/// `directory` (`with_agent_logins`), and each provider's source as
/// `credential_source` settles it from the same environment and directory
/// with no grant token held; an admission is offered its grant tokens
/// through `offer_with_grant_tokens`.
pub(crate) fn offer(process: &EnvironmentSnapshot, directory: Option<&LoginDirectory>) -> Offer {
    let service = ServiceEnvironment::of(process);
    let (environment, offered) = with_agent_logins(process, &service, directory);
    Offer {
        environment,
        offered,
        sources: sources(&service, directory, None),
    }
}

/// What an assignment's admission is offered: `offer`'s, with the grant
/// tokens it holds, `grant_tokens`, and their files offered as well
/// (`grant_token::offer`, REQ-GRANT-87), and each provider's source settled
/// with them (REQ-GRANT-92, INV-RUNNER-31). The admission holds no token of
/// a provider the host provides, whose names stay as the process
/// environment has them, and nothing is withheld. No `OfferedLogin` records
/// a grant token, whose uses are not noted.
pub(crate) fn offer_with_grant_tokens(
    process: &EnvironmentSnapshot,
    directory: Option<&LoginDirectory>,
    grant_tokens: Option<&grant_token::AssignmentGrantTokens>,
) -> Offer {
    let offered = offer(process, directory);
    Offer {
        environment: grant_token::offer(&offered.environment, grant_tokens),
        sources: sources(&ServiceEnvironment::of(process), directory, grant_tokens),
        ..offered
    }
}

/// `process`, the environment of the service `service` describes, with each
/// agent login the runner keeps in `directory` added as the variable its
/// coding agent reads (`CODEX_HOME`, `CLAUDE_CODE_OAUTH_TOKEN`) where the
/// service has no login of that agent's own (REQ-GRANT-51), and each added
/// variable whose login the readiness document records as installed. Where
/// the runner keeps none either, nothing is added and nothing withheld: the
/// variable is as the process environment has it. A variable the process
/// environment carries, a grant token name included (REQ-GRANT-87,
/// REQ-WORKSPACE-18), is never replaced or removed, and the process
/// environment itself is never changed. A missing or unreadable readiness
/// document records no install of what is added. Which steps receive a
/// variable is the workflow's `environmentPassthrough` and its declared
/// integrations (REQ-WFEXEC-71, REQ-WFEXEC-78).
fn with_agent_logins(
    process: &EnvironmentSnapshot,
    service: &ServiceEnvironment,
    directory: Option<&LoginDirectory>,
) -> (EnvironmentSnapshot, Vec<OfferedLogin>) {
    let Some(directory) = directory else {
        return (process.clone(), Vec::new());
    };
    let document = recorded_readiness(directory);
    let recorded = |provider| {
        document
            .as_ref()
            .and_then(|document| document.login(provider))
    };
    let mut added = Vec::new();
    let mut offered = Vec::new();
    for provider in AgentLoginProvider::INSTALLABLE {
        let (variable, login) = match provider {
            AgentLoginProvider::Codex => (
                CODEX_LOGIN_VARIABLE,
                codex::offered_home(service, directory),
            ),
            AgentLoginProvider::ClaudeCode => (
                CLAUDE_CODE_LOGIN_VARIABLE,
                claude_code::offered_token(service, directory),
            ),
            AgentLoginProvider::Github
            | AgentLoginProvider::Gitlab
            | AgentLoginProvider::Linear => {
                continue;
            }
        };
        let Some(login) = login else {
            continue;
        };
        added.push(login);
        offered.extend(
            recorded(provider)
                .and_then(readiness::LoginReadiness::installation)
                .map(|installation| OfferedLogin {
                    variable,
                    provider,
                    agent_login_id: installation.agent_login_id.clone(),
                    generation: installation.generation,
                }),
        );
    }
    let environment = if added.is_empty() {
        process.clone()
    } else {
        EnvironmentSnapshot::new(
            process
                .variables()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .chain(added),
        )
    };
    (environment, offered)
}

/// Each provider's source, in `AgentLoginProvider::ALL`'s order, leaving out
/// those with none (`credential_source`).
fn sources(
    service: &ServiceEnvironment,
    directory: Option<&LoginDirectory>,
    grant_tokens: Option<&grant_token::AssignmentGrantTokens>,
) -> Vec<ProviderSource> {
    AgentLoginProvider::ALL
        .into_iter()
        .filter_map(|provider| {
            credential_source(provider, service, directory, grant_tokens)
                .map(|source| ProviderSource { provider, source })
        })
        .collect()
}

/// What an admission reports to Cloud of its `sources` (REQ-GRANT-93): each
/// provider that has one, an OAuth grant by the ID of the grant its token in
/// `grant_tokens` was issued from, and a coding agent's grant by the agent
/// login the offer's `offered` records for it, absent when the readiness
/// document records no install. An OAuth grant whose token's grant is not
/// known is left out, since Cloud records no such entry; `offer` gives it a
/// source only for a token it holds.
pub(crate) fn reported_sources(
    sources: &[ProviderSource],
    offered: &[OfferedLogin],
    grant_tokens: Option<&grant_token::AssignmentGrantTokens>,
) -> Vec<cloud::ReportedSource> {
    sources
        .iter()
        .filter_map(|&ProviderSource { provider, source }| {
            let (grant_id, agent_login_id) = match source {
                CredentialSource::Host => (None, None),
                CredentialSource::Grant if provider.is_brokered() => {
                    (Some(grant_tokens?.grant_id(provider)?.to_owned()), None)
                }
                CredentialSource::Grant => (
                    None,
                    offered
                        .iter()
                        .find(|login| login.provider == provider)
                        .map(|login| login.agent_login_id.clone()),
                ),
            };
            Some(cloud::ReportedSource {
                provider,
                source,
                grant_id,
                agent_login_id,
            })
        })
        .collect()
}

/// The readiness document beside `directory`; none when there is none or it
/// cannot be read, since the pass that read it has recorded that failure.
fn recorded_readiness(directory: &LoginDirectory) -> Option<readiness::ReadinessDocument> {
    // The directory and the document are both beside the runner state.
    readiness::ReadinessDocument::load(&readiness::readiness_path(directory.path()))
        .ok()
        .flatten()
}

/// The agent-login side of the control protocol's provider: which of them
/// this build of the runner can install, and in what form.
impl AgentLoginProvider {
    /// Every provider this build can install, in a stable order.
    pub(crate) const INSTALLABLE: [Self; 2] = [Self::Codex, Self::ClaudeCode];

    /// The installable provider `name` names on the wire; `None` for a
    /// brokered provider or one this build does not know.
    pub(crate) fn installable(name: &str) -> Option<Self> {
        Self::named(name).filter(|provider| !provider.is_brokered())
    }

    /// The record schema versions of the provider's material this build can
    /// install; none for a brokered provider.
    pub(crate) const fn record_schema_versions(self) -> &'static [u32] {
        match self {
            Self::Codex | Self::ClaudeCode => &[1],
            Self::Github | Self::Gitlab | Self::Linear => &[],
        }
    }
}

/// The runner host's operating system as Cloud lists it for a provider.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperatingSystem {
    Linux,
    Macos,
}

impl OperatingSystem {
    /// The operating system this build runs on; the runner is built only for
    /// Linux and macOS.
    pub(crate) const fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::Macos
        } else {
            Self::Linux
        }
    }
}
