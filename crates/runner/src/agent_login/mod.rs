//! Agent-login setup for the serving runner: asking Cloud which agent logins
//! the runner may hold, fetching one delivery at a time, keeping it in the
//! runner's own agent-login directory, recording each provider's readiness,
//! and offering what it keeps to the assignments it admits.

pub(crate) mod claude_code;
pub(crate) mod cloud;
pub(crate) mod codex;
pub(crate) mod directory;
pub(crate) mod readiness;
pub(crate) mod setup;

use std::ffi::{OsStr, OsString};
use std::io;

use serde::{Deserialize, Serialize};
use um_execution::EnvironmentSnapshot;

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
            claude_code_oauth_token: lookup(claude_code::TOKEN_VARIABLE).is_some(),
            codex_home: lookup(codex::HOME_VARIABLE).is_some(),
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

/// The environment an assignment's steps draw from: `process`, with each
/// agent login the runner keeps in `directory` added as the variable its
/// coding agent reads (`CODEX_HOME`, `CLAUDE_CODE_OAUTH_TOKEN`) where the
/// service has no login of that agent's own. A variable the process
/// environment carries is never replaced, and the process environment itself
/// is never changed. Which steps receive a variable is the workflow's
/// `environmentPassthrough` (REQ-WFEXEC-71).
pub(crate) fn offer(
    process: &EnvironmentSnapshot,
    directory: Option<&LoginDirectory>,
) -> EnvironmentSnapshot {
    let Some(directory) = directory else {
        return process.clone();
    };
    let service = ServiceEnvironment::from_lookup(|name| {
        process.variable(OsStr::new(name)).map(OsStr::to_os_string)
    });
    let offered = [
        codex::offered_home(&service, directory),
        claude_code::offered_token(&service, directory),
    ];
    if offered.iter().all(Option::is_none) {
        return process.clone();
    }
    EnvironmentSnapshot::new(
        process
            .variables()
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .chain(offered.into_iter().flatten()),
    )
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
