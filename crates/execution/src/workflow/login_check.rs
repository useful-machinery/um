//! The login check (INV-HARNESS-18): one invocation of a coding agent's adapter outside any
//! workflow, which asks the provider through the agent's own program whether the login the
//! caller passes is accepted. The runner's agent-login setup calls it.

use std::ffi::OsStr;
use std::io;
use std::num::NonZeroU64;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rustix::fs::{AtFlags, Mode, fchmod, mkdirat, unlinkat};
use rustix::io::Errno;

use super::admission::{
    CLAUDE_CODE_LOGIN_VARIABLE, CODEX_LOGIN_VARIABLE, CancellationReason, EnvironmentSnapshot,
    HarnessLoginOutcome, claude_code_stream_json_v1_limits, codex_app_server_v1_limits,
};
use super::agent::dispatch::production_agent_dispatcher;
use super::agent::{
    AGENT_LOGIN_UNAVAILABLE, AdmittedAgentAdapter, AgentAdapter, AgentCompatibilityProfile,
    AgentFailureCause, AgentHarnessSetupStage, AgentInvocation, AgentInvocationIdentity,
    AgentInvocationLimits, AgentInvocationStaging, AgentOutcome, AgentProcessContext, AgentPrompt,
    AgentValueMode, IntoNativeHarness, NoopAgentObservationSink, WorkflowRunId,
    agent_start_channel,
};
use super::agent_diagnostics::AgentDiagnosticSessionStore;
use super::cancellation::MINIMUM_CANCELLATION_GRACE;
use super::claude_code::{
    ClaudeCodeConfig, LOGIN_CHECK_EFFORT as CLAUDE_CODE_LOGIN_CHECK_EFFORT,
    LOGIN_CHECK_MODEL as CLAUDE_CODE_LOGIN_CHECK_MODEL,
};
use super::codex::{
    CodexConfig, LOGIN_CHECK_EFFORT as CODEX_LOGIN_CHECK_EFFORT,
    LOGIN_CHECK_MODEL as CODEX_LOGIN_CHECK_MODEL,
};
use super::coordinator::CoordinatorClock;
use super::diagnostic::StepDiagnosticLog;
use super::execution_root::AdmittedExecutionRoot;
use super::observation::NoopExecutionObserver;
use super::private_staging::open_directory_path;
use super::process_group::ProcessGuardRegistry;
use super::runtime::{ActionId, TransitionSequence};
use crate::claude_code::ValidatedClaudeCodeInstallation;
use crate::codex::ValidatedCodexInstallation;
use crate::owned_tree::remove_open_tree_at;

const CHECK_DIRECTORY_PREFIX: &str = "um-login-check-";
const WORKING_DIRECTORY: &str = "work";
const STAGING_DIRECTORY: &str = "staging";
const CHECK_IDENTITY: &str = "login-check";
/// The fixed prompt: a one-word answer and nothing else. The answer is not read; only how
/// the harness ends the turn is.
const CHECK_SYSTEM_PROMPT: &str = "This turn only checks that the login works.";
const CHECK_MESSAGE: &str = "Reply with the single word OK and nothing else.";
/// The bound on what the adapter may capture from the program's diagnostic stream; the
/// check discards it with the log.
const CHECK_DIAGNOSTIC_BYTES: NonZeroU64 = match NonZeroU64::new(64 * 1024) {
    Some(bytes) => bytes,
    None => NonZeroU64::MIN,
};
/// How long a program stopped at the bound may take to exit before it is forced, as a
/// step's cancellation grace allows at its shortest.
const FORCE_GRACE: Duration = MINIMUM_CANCELLATION_GRACE;

/// What a login check found (INV-HARNESS-18): the provider accepted or refused the login,
/// or the check gave no answer, which says nothing of the login.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessLoginCheck {
    /// The turn completed.
    Accepted,
    /// The turn failed with the harness error `unauthorized` (REQ-AGENT-45).
    Refused,
    /// Anything else, with why.
    NoAnswer(HarnessLoginCheckCause),
}

/// Why a login check gave no answer. It names a stage and an error kind, an adapter stage
/// or a harness error class, and never carries provider text, standard error or a path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessLoginCheckCause {
    /// The check could not prepare the launch; no program was started.
    Preparation {
        stage: HarnessLoginCheckStage,
        error: io::ErrorKind,
    },
    /// The adapter could not start the program, or the harness's own setup failed before
    /// the turn began: the adapter's name for the step that failed.
    StartFailed { stage: &'static str },
    /// No end within the bound; the program was stopped.
    TimedOut,
    /// The turn ended with this harness error class (REQ-AGENT-45), never `unauthorized`.
    HarnessError { class: &'static str },
    /// The invocation failed without a harness error class: the closed name of the failure.
    HarnessFailed { failure: &'static str },
}

/// The part of a login check's preparation that failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessLoginCheckStage {
    /// The environment does not carry the harness's login variable, or carries it empty.
    LoginVariable,
    /// The check's own directory under the system temporary directory.
    Directory,
    /// The diagnostic session the adapter writes into the check's directory.
    Diagnostics,
    /// The adapter itself.
    Adapter,
}

/// Checks the Claude Code login that `environment` carries in `CLAUDE_CODE_OAUTH_TOKEN`
/// (INV-HARNESS-18). It runs the installation once through its adapter with a fixed
/// prompt, in an empty directory of its own that it removes. At `bound` after the
/// invocation begins it stops the program, allowing it the shortest step cancellation
/// grace to exit before forcing it, and answers `TimedOut`.
pub async fn check_claude_code_login<Clock>(
    installation: &ValidatedClaudeCodeInstallation,
    environment: EnvironmentSnapshot,
    bound: Duration,
    clock: Clock,
) -> HarnessLoginCheck
where
    Clock: CoordinatorClock,
{
    let adapter = AdmittedAgentAdapter::new(
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
        installation.executable().to_owned(),
        Arc::from(installation.version().as_str()),
        ClaudeCodeConfig {
            model: CLAUDE_CODE_LOGIN_CHECK_MODEL.to_owned(),
            effort: CLAUDE_CODE_LOGIN_CHECK_EFFORT,
        },
    );
    let launch = CheckLaunch {
        adapter,
        limits: claude_code_stream_json_v1_limits(),
        login_variable: CLAUDE_CODE_LOGIN_VARIABLE,
        environment,
    };
    check_login(launch, bound, clock, "").await
}

/// Checks the Codex login whose home `environment` carries in `CODEX_HOME`
/// (INV-HARNESS-18), as `check_claude_code_login` does; `client_version` is the version
/// the adapter introduces itself with to the App Server.
pub async fn check_codex_login<Clock>(
    installation: &ValidatedCodexInstallation,
    environment: EnvironmentSnapshot,
    bound: Duration,
    clock: Clock,
    client_version: &str,
) -> HarnessLoginCheck
where
    Clock: CoordinatorClock,
{
    let adapter = AdmittedAgentAdapter::new(
        AgentCompatibilityProfile::CodexAppServerV1,
        installation.executable().to_owned(),
        Arc::from(installation.version().as_str()),
        CodexConfig {
            model: CODEX_LOGIN_CHECK_MODEL.to_owned(),
            effort: CODEX_LOGIN_CHECK_EFFORT.to_owned(),
        },
    );
    let launch = CheckLaunch {
        adapter,
        limits: codex_app_server_v1_limits(),
        login_variable: CODEX_LOGIN_VARIABLE,
        environment,
    };
    check_login(launch, bound, clock, client_version).await
}

struct CheckLaunch<Configuration, Limits> {
    adapter: AdmittedAgentAdapter<Configuration>,
    limits: AgentInvocationLimits<Limits>,
    login_variable: &'static str,
    environment: EnvironmentSnapshot,
}

async fn check_login<Clock, Configuration, Limits>(
    launch: CheckLaunch<Configuration, Limits>,
    bound: Duration,
    clock: Clock,
    client_version: &str,
) -> HarnessLoginCheck
where
    Clock: CoordinatorClock,
    Configuration: IntoNativeHarness<Limits> + Send + 'static,
    Limits: Send + 'static,
{
    // Without its variable the program would fall back to a login of the host's own.
    match launch
        .environment
        .variable(OsStr::new(launch.login_variable))
    {
        None => {
            return no_answer_preparing(
                HarnessLoginCheckStage::LoginVariable,
                io::ErrorKind::NotFound,
            );
        }
        Some(value) if value.is_empty() => {
            return no_answer_preparing(
                HarnessLoginCheckStage::LoginVariable,
                io::ErrorKind::InvalidInput,
            );
        }
        Some(_) => {}
    }
    let dispatcher = match production_agent_dispatcher(
        StepDiagnosticLog::default(),
        CHECK_DIAGNOSTIC_BYTES,
        clock.clone(),
        NoopExecutionObserver,
        client_version,
    ) {
        Ok(dispatcher) => dispatcher,
        Err(error) => return no_answer_preparing(HarnessLoginCheckStage::Adapter, error.kind()),
    };
    let prepared = tokio::task::spawn_blocking(move || {
        let directory = CheckDirectory::create()?;
        match directory.invocation(launch) {
            Ok(invocation) => Ok((directory, invocation)),
            Err(cause) => {
                directory.remove();
                Err(cause)
            }
        }
    })
    .await;
    let (directory, invocation) = match prepared {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(cause)) => return HarnessLoginCheck::NoAnswer(cause),
        Err(_) => {
            return no_answer_preparing(HarnessLoginCheckStage::Directory, io::ErrorKind::Other);
        }
    };
    let outcome = invoke_within(&dispatcher, invocation, bound, clock).await;
    // The directory goes whatever the outcome; a failed removal cannot change the answer.
    let _ = tokio::task::spawn_blocking(move || directory.remove()).await;
    judge(&outcome)
}

/// Invokes the adapter once. At `bound` the invocation is cancelled and its program
/// interrupted, and after the force grace forced, as a step's cancellation is.
async fn invoke_within<Dispatcher, Clock>(
    dispatcher: &Dispatcher,
    invocation: AgentInvocation,
    bound: Duration,
    clock: Clock,
) -> AgentOutcome
where
    Dispatcher: AgentAdapter,
    Clock: CoordinatorClock,
{
    let cancellation = invocation.cancellation().clone();
    let process = invocation.process_control().clone();
    let (started, _start) = agent_start_channel();
    let mut deadlines = clock.clone();
    let deadline = deadlines.now() + bound;
    let invocation = dispatcher.invoke(invocation, started);
    tokio::pin!(invocation);
    tokio::select! {
        biased;
        () = clock.wait_until(deadline) => {}
        outcome = &mut invocation => return outcome,
    }
    let _ = cancellation.request_cancellation(CancellationReason::TerminationRequest);
    process.interrupt();
    let forced = deadlines.now() + FORCE_GRACE;
    tokio::select! {
        biased;
        () = clock.wait_until(forced) => {}
        outcome = &mut invocation => return outcome,
    }
    process.force();
    invocation.await
}

/// The step runtime's judgment of an invocation (INV-ENGINE-36), with a cause for the rest.
fn judge(outcome: &AgentOutcome) -> HarnessLoginCheck {
    match outcome.harness_login_outcome() {
        Some(HarnessLoginOutcome::Accepted) => HarnessLoginCheck::Accepted,
        Some(HarnessLoginOutcome::Refused) => HarnessLoginCheck::Refused,
        None => HarnessLoginCheck::NoAnswer(match outcome {
            AgentOutcome::Failed(failure) => failure_cause(failure.cause()),
            // Only the bound cancels the check's invocation, and a completed one is accepted.
            AgentOutcome::Cancelled { .. } | AgentOutcome::Completed(_) => {
                HarnessLoginCheckCause::TimedOut
            }
        }),
    }
}

fn failure_cause(cause: &AgentFailureCause) -> HarnessLoginCheckCause {
    let failed = |failure| HarnessLoginCheckCause::HarnessFailed { failure };
    match cause {
        AgentFailureCause::HarnessStartFailed { stage, .. } => {
            HarnessLoginCheckCause::StartFailed { stage }
        }
        AgentFailureCause::AgentLoginUnavailable => HarnessLoginCheckCause::StartFailed {
            stage: AGENT_LOGIN_UNAVAILABLE,
        },
        AgentFailureCause::HarnessSetupFailed { stage }
        | AgentFailureCause::HarnessSetupRejected { stage, .. } => {
            HarnessLoginCheckCause::StartFailed {
                stage: setup_stage(*stage),
            }
        }
        AgentFailureCause::HarnessFailed { detail } => {
            match detail
                .diagnostic()
                .and_then(|diagnostic| diagnostic.harness_error)
            {
                Some(class) => HarnessLoginCheckCause::HarnessError { class },
                None => failed(detail.slug()),
            }
        }
        AgentFailureCause::HarnessInputTooLarge { .. } => failed("harness_input_too_large"),
        AgentFailureCause::HarnessProtocolFailed => failed("harness_protocol_failed"),
        AgentFailureCause::MissingResponse => failed("missing_response"),
        AgentFailureCause::MissingResult => failed("missing_result"),
        AgentFailureCause::ResultValidationLimitExceeded { .. } => {
            failed("result_validation_limit_exceeded")
        }
        AgentFailureCause::CapturedValueTooLarge => failed("captured_value_too_large"),
        AgentFailureCause::ResultSettlementFailed => failed("result_settlement_failed"),
    }
}

const fn setup_stage(stage: AgentHarnessSetupStage) -> &'static str {
    match stage {
        AgentHarnessSetupStage::ExecutableLaunch => "executable_launch",
        AgentHarnessSetupStage::Initialization => "initialization",
        AgentHarnessSetupStage::EffectiveConfiguration => "effective_configuration",
        AgentHarnessSetupStage::ThreadStart => "thread_start",
        AgentHarnessSetupStage::TurnStart => "turn_start",
        AgentHarnessSetupStage::StartAcknowledgement => "start_acknowledgement",
    }
}

fn no_answer_preparing(stage: HarnessLoginCheckStage, error: io::ErrorKind) -> HarnessLoginCheck {
    HarnessLoginCheck::NoAnswer(HarnessLoginCheckCause::Preparation { stage, error })
}

fn preparing(stage: HarnessLoginCheckStage) -> impl Fn(Errno) -> HarnessLoginCheckCause {
    move |errno| HarnessLoginCheckCause::Preparation {
        stage,
        error: io::Error::from_raw_os_error(errno.raw_os_error()).kind(),
    }
}

/// The check's own directory: a new `0700` directory under the system temporary directory
/// holding the empty working directory, the adapter's staging and its diagnostic session.
struct CheckDirectory {
    parent: OwnedFd,
    name: String,
    directory: OwnedFd,
    path: PathBuf,
}

impl CheckDirectory {
    fn create() -> Result<Self, HarnessLoginCheckCause> {
        let failure = preparing(HarnessLoginCheckStage::Directory);
        // Canonical, as the adapters require of the staging they are given.
        let temporary = std::fs::canonicalize(std::env::temp_dir()).map_err(|error| {
            HarnessLoginCheckCause::Preparation {
                stage: HarnessLoginCheckStage::Directory,
                error: error.kind(),
            }
        })?;
        let parent = open_directory_path(&temporary).map_err(&failure)?;
        let name = format!(
            "{CHECK_DIRECTORY_PREFIX}{}",
            ulid::Ulid::generate().to_string().to_ascii_lowercase()
        );
        let path = temporary.join(&name);
        mkdirat(&parent, name.as_str(), Mode::RWXU).map_err(&failure)?;
        let directory = match open_directory_path(&path) {
            Ok(directory) => directory,
            Err(errno) => {
                let _ = unlinkat(&parent, name.as_str(), AtFlags::REMOVEDIR);
                return Err(failure(errno));
            }
        };
        let created = Self {
            parent,
            name,
            directory,
            path,
        };
        let prepared = fchmod(&created.directory, Mode::RWXU).and_then(|()| {
            [WORKING_DIRECTORY, STAGING_DIRECTORY]
                .into_iter()
                .try_for_each(|name| mkdirat(&created.directory, name, Mode::RWXU))
        });
        match prepared {
            Ok(()) => Ok(created),
            Err(errno) => {
                created.remove();
                Err(failure(errno))
            }
        }
    }

    #[cfg(test)]
    fn working_directory_path(&self) -> PathBuf {
        self.path.join(WORKING_DIRECTORY)
    }

    fn invocation<Configuration, Limits>(
        &self,
        launch: CheckLaunch<Configuration, Limits>,
    ) -> Result<AgentInvocation, HarnessLoginCheckCause>
    where
        Configuration: IntoNativeHarness<Limits>,
    {
        let unavailable = |stage| HarnessLoginCheckCause::Preparation {
            stage,
            error: io::ErrorKind::Other,
        };
        let working_directory = AdmittedExecutionRoot::admit(&self.path)
            .ok()
            .and_then(|root| root.select_working_directory(Some(WORKING_DIRECTORY)).ok())
            .ok_or_else(|| unavailable(HarnessLoginCheckStage::Directory))?;
        let identity = AgentInvocationIdentity::new(
            WorkflowRunId::from(Arc::from(CHECK_IDENTITY)),
            Arc::from(CHECK_IDENTITY),
            ActionId {
                transition_sequence: TransitionSequence::default(),
            },
        );
        let diagnostic_session =
            AgentDiagnosticSessionStore::create_transient(&self.directory, &self.path)
                .and_then(|sessions| {
                    sessions.allocate(
                        &identity,
                        launch.adapter.profile(),
                        launch.adapter.version(),
                    )
                })
                .map_err(|_| unavailable(HarnessLoginCheckStage::Diagnostics))?;
        Ok(AgentInvocation::new(
            identity,
            launch.adapter,
            AgentProcessContext::new(working_directory, launch.environment),
            AgentInvocationStaging::new(self.path.join(STAGING_DIRECTORY)),
            diagnostic_session,
            AgentPrompt::new(Arc::from(CHECK_SYSTEM_PROMPT), Arc::from(CHECK_MESSAGE)),
            Arc::from([]),
            AgentValueMode::None,
            launch.limits,
            super::admission::CancellationSource::new(),
            ProcessGuardRegistry::default(),
            NoopAgentObservationSink,
        ))
    }

    /// Removes the directory and everything in it without following a link out of it.
    fn remove(self) {
        if let Err(error) = remove_open_tree_at(&self.parent, &self.name, &self.directory) {
            tracing::warn!(event = "login_check_directory_not_removed", error = ?error);
        }
    }
}

#[cfg(test)]
mod tests;
