//! The runner's check of a login it holds (REQ-GRANT-90): the coding
//! agent's own program, run once through the execution component's login
//! check (INV-HARNESS-18) with the environment an admission would be offered
//! for that login. The check writes nothing; setup notes what it found as
//! the login's last use and a pass records it (INV-RUNNER-29).

use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use um_execution::{
    CLAUDE_CODE_LOGIN_VARIABLE, CODEX_LOGIN_VARIABLE, CoordinatorClock, EnvironmentSnapshot,
    HarnessLoginCheck, HarnessLoginCheckCause, MINIMUM_CANCELLATION_GRACE,
    ValidatedClaudeCodeInstallation, ValidatedCodexInstallation, check_claude_code_login,
    check_codex_login,
};

use super::directory::LoginDirectory;
use super::readiness::{Installation, UseOutcome};
use super::{ServiceEnvironment, claude_code, codex};
use crate::control_protocol::{AgentLoginCheckCause, AgentLoginProvider};

/// How long a check waits for the program to end its turn (REQ-GRANT-90).
pub(crate) const CHECK_BOUND: Duration = Duration::from_secs(60);

/// The longest a check takes: its bound, and the grace a program stopped at
/// the bound has to exit before it is forced.
pub(crate) const CHECK_ALLOWANCE: Duration = CHECK_BOUND.saturating_add(MINIMUM_CANCELLATION_GRACE);

/// What a check of a login found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LoginCheckOutcome {
    /// The provider accepted or refused the login: its last use.
    Answered(UseOutcome),
    /// The check gave no answer, which says nothing of the login.
    NoAnswer(AgentLoginCheckCause),
}

/// Checks a login the runner holds with its provider.
pub(crate) trait LoginCheck: Send + Sync {
    /// Checks `login`, the provider's install the readiness document
    /// records, and blocks until the check ends.
    fn check(&self, provider: AgentLoginProvider, login: &Installation) -> LoginCheckOutcome;
}

/// The serving runner's check: the `claude` and `codex` installations it
/// validated when it started, run with the process environment and the
/// login's variable as the offer sets it from the runner's directory
/// (REQ-GRANT-36, REQ-GRANT-44). A provider whose program it has none of
/// answers `program_unavailable`.
pub(crate) struct ProgramLoginCheck<Clock> {
    claude_code: Option<ValidatedClaudeCodeInstallation>,
    codex: Option<ValidatedCodexInstallation>,
    process: EnvironmentSnapshot,
    directory: LoginDirectory,
    client_version: Arc<str>,
    clock: Clock,
}

impl<Clock: CoordinatorClock> ProgramLoginCheck<Clock> {
    /// The check of the runner whose protected state is at
    /// `runner_state_path`, from the process's own environment;
    /// `client_version` is the version the Codex adapter introduces itself
    /// with.
    pub(crate) fn new(
        claude_code: Option<ValidatedClaudeCodeInstallation>,
        codex: Option<ValidatedCodexInstallation>,
        runner_state_path: &Path,
        client_version: &str,
        clock: Clock,
    ) -> Self {
        Self {
            claude_code,
            codex,
            process: EnvironmentSnapshot::new(std::env::vars_os()),
            directory: LoginDirectory::beside(runner_state_path),
            client_version: Arc::from(client_version),
            clock,
        }
    }

    /// The process environment with `variable` as the offer sets it, or
    /// without it where the offer would set none: the check never falls
    /// back to a login of the host's own.
    fn offered(&self, variable: &str, login: Option<(OsString, OsString)>) -> EnvironmentSnapshot {
        EnvironmentSnapshot::new(
            self.process
                .variables()
                .iter()
                .filter(|(name, _)| name.as_os_str() != OsStr::new(variable))
                .map(|(name, value)| (name.clone(), value.clone()))
                .chain(login),
        )
    }

    fn service(&self) -> ServiceEnvironment {
        ServiceEnvironment::from_lookup(|name| {
            self.process
                .variable(OsStr::new(name))
                .map(OsStr::to_os_string)
        })
    }
}

impl<Clock: CoordinatorClock> LoginCheck for ProgramLoginCheck<Clock> {
    // The offer reads the login from the runner's directory, where the pass
    // that recorded `_login` installed it.
    fn check(&self, provider: AgentLoginProvider, _login: &Installation) -> LoginCheckOutcome {
        // The check's spawned work needs a runtime, and the setup's thread
        // is not one of the service's workers.
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return LoginCheckOutcome::NoAnswer(AgentLoginCheckCause::StartFailed);
        };
        let service = self.service();
        let found = match provider {
            AgentLoginProvider::ClaudeCode => {
                let Some(installation) = &self.claude_code else {
                    return LoginCheckOutcome::NoAnswer(AgentLoginCheckCause::ProgramUnavailable);
                };
                let environment = self.offered(
                    CLAUDE_CODE_LOGIN_VARIABLE,
                    claude_code::offered_token(&service, &self.directory),
                );
                runtime.block_on(check_claude_code_login(
                    installation,
                    environment,
                    CHECK_BOUND,
                    self.clock.clone(),
                ))
            }
            // The runner's Codex home itself, so a login Codex refreshes
            // during the check is kept (REQ-GRANT-36).
            AgentLoginProvider::Codex => {
                let Some(installation) = &self.codex else {
                    return LoginCheckOutcome::NoAnswer(AgentLoginCheckCause::ProgramUnavailable);
                };
                let environment = self.offered(
                    CODEX_LOGIN_VARIABLE,
                    codex::offered_home(&service, &self.directory),
                );
                runtime.block_on(check_codex_login(
                    installation,
                    environment,
                    CHECK_BOUND,
                    self.clock.clone(),
                    &self.client_version,
                ))
            }
            AgentLoginProvider::Github
            | AgentLoginProvider::Gitlab
            | AgentLoginProvider::Linear => {
                return LoginCheckOutcome::NoAnswer(AgentLoginCheckCause::ProgramUnavailable);
            }
        };
        outcome(found)
    }
}

/// What the execution component's check found, as the runner records it.
fn outcome(found: HarnessLoginCheck) -> LoginCheckOutcome {
    match found {
        HarnessLoginCheck::Accepted => LoginCheckOutcome::Answered(UseOutcome::Accepted),
        HarnessLoginCheck::Refused => LoginCheckOutcome::Answered(UseOutcome::Refused),
        HarnessLoginCheck::NoAnswer(cause) => LoginCheckOutcome::NoAnswer(match cause {
            HarnessLoginCheckCause::Preparation { .. }
            | HarnessLoginCheckCause::StartFailed { .. } => AgentLoginCheckCause::StartFailed,
            HarnessLoginCheckCause::TimedOut => AgentLoginCheckCause::TimedOut,
            HarnessLoginCheckCause::HarnessError { .. }
            | HarnessLoginCheckCause::HarnessFailed { .. } => AgentLoginCheckCause::HarnessError,
        }),
    }
}
