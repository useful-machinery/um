//! Agent-login setup for the serving runner: asking Cloud which agent logins
//! the runner may hold, fetching one delivery at a time, keeping it in the
//! runner's own agent-login directory, recording each provider's readiness,
//! and offering what it keeps to each assignment it admits, once it has set
//! them up for that admission (REQ-AGENTLOGIN-39), noting how the provider
//! answered each use of what it offered (REQ-AGENTLOGIN-42).

pub(crate) mod claude_code;
pub(crate) mod cloud;
pub(crate) mod codex;
pub(crate) mod directory;
pub(crate) mod readiness;
pub(crate) mod setup;

use std::ffi::{OsStr, OsString};
use std::io;

use serde::{Deserialize, Serialize};
use um_execution::{CLAUDE_CODE_LOGIN_VARIABLE, CODEX_LOGIN_VARIABLE, EnvironmentSnapshot};

use crate::control_protocol::{AgentLoginProvider, AgentLoginState};
use crate::telemetry;
use directory::LoginDirectory;

/// The serving process's environment as far as standing back for the
/// service's own logins needs it. Setup takes it as a value so that the
/// decision rests on the service's environment, which only the serving
/// process has. It keeps whether the service carries
/// `CLAUDE_CODE_OAUTH_TOKEN` or `CODEX_HOME`, never their values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ServiceEnvironment {
    home: Option<OsString>,
    claude_config_dir: Option<OsString>,
    claude_code_oauth_token: bool,
    codex_home: bool,
}

impl ServiceEnvironment {
    /// Reads `HOME`, `CLAUDE_CONFIG_DIR` and whether
    /// `CLAUDE_CODE_OAUTH_TOKEN` and `CODEX_HOME` are set through `lookup`,
    /// for example `std::env::var_os`.
    pub(crate) fn from_lookup(lookup: impl Fn(&str) -> Option<OsString>) -> Self {
        Self {
            home: lookup("HOME"),
            claude_config_dir: lookup("CLAUDE_CONFIG_DIR"),
            claude_code_oauth_token: lookup(CLAUDE_CODE_LOGIN_VARIABLE).is_some(),
            codex_home: lookup(CODEX_LOGIN_VARIABLE).is_some(),
        }
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
}

impl FailureStage {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Delivery => "delivery",
            Self::Directory => "directory",
            Self::Write => "write",
            Self::Remove => "remove",
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
        }
    }
}

/// What an admission is offered: the environment its steps draw from, and
/// the login variables withheld from it (REQ-AGENTLOGIN-41, INV-RUNNER-17).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Offer {
    /// The process environment, with each agent login the runner offers
    /// added as the variable its coding agent reads.
    pub(crate) environment: EnvironmentSnapshot,
    /// The variable of each coding agent whose provider is selected, which
    /// the runner has no login of to offer and the service has none of its
    /// own for: an agent step that would have received it fails rather than
    /// start on the service's own login (INV-ENGINE-35).
    pub(crate) withheld: Vec<&'static str>,
    /// Each variable added to `environment` whose login the readiness
    /// document records as installed, with that install: the uses an
    /// admission notes (REQ-AGENTLOGIN-42).
    pub(crate) offered: Vec<OfferedLogin>,
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
/// `directory`: `process`, with each login added as the variable its coding
/// agent reads (`CODEX_HOME`, `CLAUDE_CODE_OAUTH_TOKEN`) where the service
/// has no login of that agent's own, and the variable withheld instead where
/// the runner keeps none of a selected provider's login (REQ-AGENTLOGIN-41).
/// A variable the process environment carries is never replaced or
/// withheld, and the process environment itself is never changed. A missing
/// or unreadable readiness document selects nothing, and records no install
/// of what is added. Which steps receive a variable is the workflow's
/// `environmentPassthrough` (REQ-WFEXEC-71).
pub(crate) fn offer(process: &EnvironmentSnapshot, directory: Option<&LoginDirectory>) -> Offer {
    let Some(directory) = directory else {
        return Offer {
            environment: process.clone(),
            withheld: Vec::new(),
            offered: Vec::new(),
        };
    };
    let service = ServiceEnvironment::from_lookup(|name| {
        process.variable(OsStr::new(name)).map(OsStr::to_os_string)
    });
    let document = recorded_readiness(directory);
    let recorded = |provider| {
        document
            .as_ref()
            .and_then(|document| document.login(provider))
    };
    let mut added = Vec::new();
    let mut offered = Vec::new();
    let mut withheld = Vec::new();
    for provider in AgentLoginProvider::INSTALLABLE {
        let (variable, login, foreign_login): (_, _, fn(&ServiceEnvironment) -> bool) =
            match provider {
                AgentLoginProvider::Codex => (
                    CODEX_LOGIN_VARIABLE,
                    codex::offered_home(&service, directory),
                    codex::foreign_login,
                ),
                AgentLoginProvider::ClaudeCode => (
                    CLAUDE_CODE_LOGIN_VARIABLE,
                    claude_code::offered_token(&service, directory),
                    claude_code::foreign_login,
                ),
                AgentLoginProvider::Github
                | AgentLoginProvider::Gitlab
                | AgentLoginProvider::Linear => continue,
            };
        match login {
            Some(login) => {
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
            None if recorded(provider).is_some_and(|login| login.selected)
                && !foreign_login(&service) =>
            {
                withheld.push(variable);
            }
            None => {}
        }
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
    Offer {
        environment,
        withheld,
        offered,
    }
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
