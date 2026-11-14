use std::ffi::{OsStr, OsString};
use std::io::{self, IsTerminal, Write};
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{self, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use anyhow::Context;
use clap::{ArgGroup, Args};
use nix::sys::signal::{SigSet, SigmaskHow, Signal};
use rustix::termios::{LocalModes, OptionalActions, QueueSelector, Termios};
use zeroize::Zeroizing;

use super::super::{HttpOptions, OrganizationArg, PoolArg, PrincipalAuthenticationArgs};
use crate::exit_code::ExitCode;
use um_api::{HttpTransportPolicy, RunnerApi, RunnerFailure};
use um_execution::{
    ClaudeCodeInstallationFailure, CodexInstallationFailure, PiInstallationFailure,
    ValidatedClaudeCodeInstallation, ValidatedCodexInstallation, ValidatedPiInstallation,
    discover_and_validate_claude_code_installation, discover_and_validate_codex_installation,
    discover_and_validate_pi_installation, human_duration,
};
use um_human_auth::Deployment;
use um_runner::{
    ActivationArtifact, AmbiguousCause, Config, EnrollmentError, EnrollmentResponse,
    JoinKeyArtifact, JoinOutcome, JoinRefusal, JoinRetry, RecordedOptions, RegisteredRunner,
    RegistrationAuthority, RegistrationConflict, RegistrationConflictKind, RegistrationError,
    RegistrationOutcome, RegistrationStage, RegistrationTarget,
};
use um_support::generate_idempotency_key;

pub(super) const ABOUT: &str = "Serve run assignments";

const JOIN_KEY_VARIABLE: &str = "UM_RUNNER_JOIN_KEY";

// One registration mode at most: a signed-in mode excludes the other and a
// join key, needs --organization, and is the only use of --organization and
// --allow-insecure-http. UM_RUNNER_JOIN_KEY is refused by `join_key_source`.
#[derive(Debug, Args)]
#[command(
    group(
        ArgGroup::new("signed_in_mode")
            .args(["personal", "pool"])
            .requires("organization")
            .conflicts_with_all(["join_key_file", "join_key"])
    ),
    group(
        ArgGroup::new("signed_in_options")
            .args(["organization", "allow_insecure_http"])
            .multiple(true)
            .requires("signed_in_mode")
    )
)]
pub(super) struct Command {
    /// Read the closed runner operator configuration and enrolled state; the
    /// runner home under the um home is the default, created where it is missing.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Join the key's pool from a protected file or explicit stdin when the
    /// host holds no enrolled state; UM_RUNNER_JOIN_KEY may hold the key instead.
    #[arg(long, value_name = "PATH|-")]
    join_key_file: Option<PathBuf>,

    /// Join the key's pool with a join key pasted at a prompt that does not
    /// echo it when the host holds no enrolled state; needs a terminal.
    #[arg(long)]
    join_key: bool,

    /// Register the host as a runner in the signed-in account's personal
    /// pool of --organization when it holds no enrolled state.
    #[arg(long)]
    personal: bool,

    #[arg(long = "pool-id", value_name = PoolArg::VALUE_NAME, help = PoolArg::HELP)]
    pool: Option<PoolArg>,

    #[arg(long, value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: Option<OrganizationArg>,

    #[command(flatten)]
    http: HttpOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let registration = signed_in_registration(
            self.personal,
            self.pool,
            self.organization,
            self.http.transport_policy(),
        )?;
        let join_key = join_key_source(
            self.join_key_file.as_deref(),
            self.join_key,
            std::env::var_os(JOIN_KEY_VARIABLE),
            registration.as_ref().map(SignedInRegistration::option),
        )?;
        // Harness discovery runs before the operator configuration is judged,
        // so a start with an invalid configuration still reports what it found.
        let discover = || {
            discover_harness_installations_with(
                discover_and_validate_pi_installation,
                discover_and_validate_claude_code_installation,
                discover_and_validate_codex_installation,
            )
        };
        // The runner home is prepared once the join key's source and the
        // registration mode are known, so a key given more than one way or
        // beside a registration mode writes nothing, and before the host's
        // state is judged. The image a key in the variable is handed to prepares it
        // again and finds nothing to do.
        let config_path = match super::initialized_operator_config_path(self.config.as_deref()) {
            Ok(path) => path,
            Err(error) => {
                discover();
                return Err(error.into());
            }
        };
        // A host whose state file is present holds enrolled state: it serves
        // with it and never reads its join key or a sign-in, so a key that is
        // gone or unreadable does not stop it, and a configuration or state
        // that does not load is the start's failure. A signed-in mode is then
        // confirmed against the host's registration record, sending nothing.
        // The journal its own completed join left is removed before it
        // serves, as the second copy of the credential secret it is.
        let configuration_failure = |error| {
            discover();
            config_load_error(&config_path, error)
        };
        let enrolled = Config::holds_enrolled_state(&config_path);
        let (config, (pi_installation, claude_code_installation, codex_installation)) =
            match (enrolled, join_key, registration) {
                (Err(error), _, _) => return Err(configuration_failure(error).into()),
                // A host with no state file, no key and no registration mode
                // has nothing to enroll with: what can be checked of its
                // configuration before a join keeps its own cause, and
                // otherwise it is not enrolled.
                (Ok(false), None, None) => {
                    if let Err(error) = Config::validate_before_join(&config_path) {
                        return Err(configuration_failure(error).into());
                    }
                    discover();
                    return Err(not_enrolled(&config_path, self.config.is_some()).into());
                }
                // An enrolled host serves with its state when the registration
                // mode it is given, if any, is the one its registration record
                // holds.
                (Ok(true), join_key, registration) => {
                    match (Config::load(&config_path), join_key) {
                        (Err(error), _) => return Err(configuration_failure(error).into()),
                        (Ok(_), Some(JoinKeySource::Variable(_))) => {
                            return Err(restart_without_join_key_variable().into());
                        }
                        (Ok(config), _) => {
                            if let Some(registration) = registration {
                                registration.confirm(&config_path)?;
                            }
                            um_runner::remove_completed_join_journal(&config_path)
                                .context("remove the journal of the runner's completed join")?;
                            (config, discover())
                        }
                    }
                }
                // A host with no state file checks what it can of its
                // configuration before it reads the key, in both images of a
                // key given in the variable, so a host that cannot serve
                // stages, sends and writes nothing.
                (Ok(false), Some(join_key), _) => {
                    if let Err(error) = Config::validate_before_join(&config_path) {
                        return Err(configuration_failure(error).into());
                    }
                    let key = join_key.read()?;
                    let installations = discover();
                    join_unless_enrolled(&config_path, key)?;
                    (load_config(&config_path)?, installations)
                }
                // A signed-in registration checks the host the same way before
                // it reads the sign-in, so a host that cannot serve registers
                // nothing; the human session ends before the runner starts.
                (Ok(false), None, Some(registration)) => {
                    if let Err(error) = Config::validate_before_join(&config_path) {
                        return Err(configuration_failure(error).into());
                    }
                    let installations = discover();
                    match super::super::execute_deployment_leaf(
                        registration,
                        &[super::NAME, "serve"],
                        "configure Useful Machinery runner registration",
                        |registration, deployment| registration.register(&config_path, deployment),
                    )? {
                        ExitCode::Success => {}
                        exit_code => return Ok(exit_code),
                    }
                    (load_config(&config_path)?, installations)
                }
            };
        let config = configure_harness_installations(
            config,
            pi_installation,
            claude_code_installation,
            codex_installation,
        );
        match um_runner::run(config, crate::build_info::VERSION) {
            Ok(()) => Ok(ExitCode::Success),
            Err(error) => {
                let exit_code = service_exit_code(error.requires_operator_recovery());
                Err(super::super::CommandFailure::with_exit_code(
                    anyhow::Error::new(error).context("serve enrolled runner assignments"),
                    exit_code,
                ))
            }
        }
    }
}

/// A start on a host that holds no enrolled state and was given neither a
/// join key nor a registration mode (REQ-RUNNERENROLL-30). The commands it names repeat `--config` when the
/// start was given it.
fn not_enrolled(config_path: &Path, explicit_config: bool) -> anyhow::Error {
    let config = if explicit_config {
        format!(" --config {}", config_path.display())
    } else {
        String::new()
    };
    anyhow::anyhow!(
        "start runner with operator configuration {}: host is not enrolled\n\nEnroll the host with an activation an owner issued for it:\n  um runner enroll --activation-file <PATH>{config}\nOr start the runner with a join key a pool issued:\n  um runner serve --join-key-file <PATH>{config}",
        config_path.display()
    )
}

fn load_config(config_path: &Path) -> anyhow::Result<Config> {
    Config::load(config_path).map_err(|error| config_load_error(config_path, error))
}

fn config_load_error(config_path: &Path, error: um_runner::ConfigError) -> anyhow::Error {
    anyhow::Error::new(error).context(format!(
        "load runner operator configuration {}",
        config_path.display()
    ))
}

/// Where the join key would be read from; nothing is read until the host is
/// found to hold no enrolled state.
enum JoinKeySource {
    File(PathBuf),
    Variable(OsString),
    Prompt,
}

/// Where the join key would be read from, given `registration`, the
/// signed-in mode's option when one was given: a key in any form beside a
/// signed-in mode, or in two forms, is a usage error. Clap refuses a mode
/// beside `--join-key-file` or `--join-key`; the variable is refused here.
fn join_key_source(
    file: Option<&Path>,
    prompt: bool,
    variable: Option<OsString>,
    registration: Option<&'static str>,
) -> Result<Option<JoinKeySource>, super::super::CommandFailure> {
    match (file, prompt, variable, registration) {
        (None, false, None, _) => Ok(None),
        (Some(path), false, None, None) => Ok(Some(JoinKeySource::File(path.to_owned()))),
        (None, false, Some(text), None) => Ok(Some(JoinKeySource::Variable(text))),
        (None, true, None, None) => Ok(Some(JoinKeySource::Prompt)),
        (file, prompt, variable, registration) => {
            let given = [
                registration,
                file.map(|_| "--join-key-file"),
                prompt.then_some("--join-key"),
                variable.map(|_| JOIN_KEY_VARIABLE),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" and ");
            let failure = match registration {
                None => anyhow::anyhow!(
                    "read join key: {given} are set together\n\nGive the join key in only one of --join-key-file PATH|-, --join-key and {JOIN_KEY_VARIABLE}."
                ),
                Some(_) => anyhow::anyhow!(
                    "select runner registration: {given} are set together\n\nStart the runner with a join key or with --personal or --pool-id, not both. Unset {JOIN_KEY_VARIABLE} to register with the signed-in account."
                ),
            };
            Err(super::super::CommandFailure::with_exit_code(
                failure,
                ExitCode::UsageError,
            ))
        }
    }
}

/// A signed-in registration mode with its organization, as given.
struct SignedInRegistration {
    organization: OrganizationArg,
    target: RegistrationTarget,
    transport_policy: HttpTransportPolicy,
}

/// The signed-in registration mode the arguments name, if any. Clap's
/// groups make `--organization` present exactly when a mode is.
fn signed_in_registration(
    personal: bool,
    pool: Option<PoolArg>,
    organization: Option<OrganizationArg>,
    transport_policy: HttpTransportPolicy,
) -> Result<Option<SignedInRegistration>, super::super::CommandFailure> {
    let target = match (personal, pool) {
        (true, None) => RegistrationTarget::Personal,
        (false, Some(pool)) => RegistrationTarget::Pool(pool.into_string()),
        (false, None) => return Ok(None),
        (true, Some(_)) => {
            return Err(registration_usage_error(
                "--personal and --pool-id are set together",
            ));
        }
    };
    let organization = organization.ok_or_else(|| {
        registration_usage_error("--personal and --pool-id need --organization ORGANIZATION")
    })?;
    Ok(Some(SignedInRegistration {
        organization,
        target,
        transport_policy,
    }))
}

fn registration_usage_error(diagnostic: &str) -> super::super::CommandFailure {
    super::super::CommandFailure::with_exit_code(
        anyhow::anyhow!(
            "select runner registration: {diagnostic}\n\nGive one of --personal and --pool-id POOL, with --organization ORGANIZATION."
        ),
        ExitCode::UsageError,
    )
}

impl SignedInRegistration {
    /// The option that selected this mode.
    fn option(&self) -> &'static str {
        match self.target {
            RegistrationTarget::Personal => "--personal",
            RegistrationTarget::Pool(_) => "--pool-id",
        }
    }

    /// Confirms the mode against an enrolled host's registration record,
    /// completing a record whose runner the state holds; it reads no sign-in
    /// and sends nothing.
    fn confirm(&self, config_path: &Path) -> anyhow::Result<()> {
        match um_runner::confirm_registration(config_path, &self.organization, &self.target) {
            Ok(()) => Ok(()),
            Err(RegistrationError::Conflict(conflict)) => {
                Err(registration_conflict(CONFIRM_REGISTRATION, &conflict))
            }
            Err(error) => Err(anyhow::Error::new(error).context(CONFIRM_REGISTRATION)),
        }
    }

    /// Registers, enrolls and records the host's runner as the signed-in
    /// person, and reports it; the authority, and with it the human session,
    /// is dropped before this returns.
    fn register(self, config_path: &Path, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let mut authority = SignedInAuthority {
            deployment,
            transport_policy: self.transport_policy,
            authentication: PrincipalAuthenticationArgs::default(),
        };
        let outcome = um_runner::register_unless_enrolled(
            config_path,
            &self.organization,
            &self.target,
            &mut authority,
        );
        drop(authority);
        match outcome {
            Ok(RegistrationOutcome::AlreadyEnrolled) => Ok(ExitCode::Success),
            Ok(RegistrationOutcome::Registered(response)) => {
                write_enrolled(&mut io::stderr().lock(), "✓ Runner registered.", &response)?;
                Ok(ExitCode::Success)
            }
            Err(error) => registration_failure(deployment, error),
        }
    }
}

/// Cloud's side of `serve`'s registration: the human API as the signed-in
/// person, through the session `cloud::with_api` acquires and refreshes for
/// each request. Only `SignedInRegistration::register` holds one.
struct SignedInAuthority<'a> {
    deployment: &'a Deployment,
    transport_policy: HttpTransportPolicy,
    authentication: PrincipalAuthenticationArgs,
}

/// Why the signed-in authority answered nothing.
#[derive(Debug)]
enum AuthorityFailure {
    /// The API's closed outcome, a missing or rejected sign-in included.
    Api(RunnerFailure),
    /// The request could not be prepared or the session not read.
    Local(anyhow::Error),
}

impl SignedInAuthority<'_> {
    fn call<T>(
        &self,
        operation: impl FnMut(&RunnerApi) -> Result<T, RunnerFailure>,
    ) -> Result<T, AuthorityFailure> {
        match super::cloud::with_api(
            self.deployment,
            self.transport_policy,
            &self.authentication,
            operation,
        ) {
            Ok(result) => result.map_err(AuthorityFailure::Api),
            Err(error) => Err(AuthorityFailure::Local(error)),
        }
    }
}

impl RegistrationAuthority for SignedInAuthority<'_> {
    type Failure = AuthorityFailure;

    fn api_url(&self) -> &str {
        self.deployment.fingerprint().api_url()
    }

    fn principal_id(&mut self) -> Result<String, AuthorityFailure> {
        self.call(RunnerApi::current_principal_id)
    }

    fn pool_id(&mut self, organization: &str, pool: &str) -> Result<String, AuthorityFailure> {
        let Ok(pool) = pool.parse::<PoolArg>();
        self.call(|api| pool.resolve_id(api, organization))
    }

    // `--personal` names neither a pool nor a member, so Cloud registers the
    // runner in the caller's personal pool; the service chooses the name.
    fn register(
        &mut self,
        organization: &str,
        pool_id: Option<&str>,
        idempotency_key: &str,
    ) -> Result<RegisteredRunner, AuthorityFailure> {
        self.call(|api| api.create_registration(organization, idempotency_key, pool_id, None, None))
            .map(|registration| RegisteredRunner {
                runner_id: registration.id,
                organization_id: registration.organization_id,
                pool_id: registration.runner_pool.id,
            })
    }

    // A fresh key each time: a replayed issuance returns no secret.
    fn issue_activation(
        &mut self,
        organization: &str,
        runner_id: &str,
    ) -> Result<ActivationArtifact, AuthorityFailure> {
        let idempotency_key = generate_idempotency_key()
            .context("generate activation request identity")
            .map_err(AuthorityFailure::Local)?;
        let issuance =
            self.call(|api| api.create_activation(organization, runner_id, &idempotency_key))?;
        // A fresh issuance without its secret does not match the contract.
        super::host_activation_artifact(&issuance)
            .ok_or(AuthorityFailure::Api(RunnerFailure::Protocol))
    }

    fn is_definite_refusal(&self, failure: &AuthorityFailure) -> bool {
        matches!(
            failure,
            AuthorityFailure::Api(
                RunnerFailure::Forbidden
                    | RunnerFailure::NotFound
                    | RunnerFailure::InvalidInput
                    | RunnerFailure::PoolPersonalUnavailable
                    | RunnerFailure::PersonalPoolNotFound
                    | RunnerFailure::QuantityLimitReached
            )
        )
    }
}

const REGISTER_RUNNER: &str = "register runner with the signed-in account";
const CONFIRM_REGISTRATION: &str = "confirm the runner's registration";

fn registration_failure(
    deployment: &Deployment,
    error: RegistrationError<AuthorityFailure>,
) -> anyhow::Result<ExitCode> {
    let progress = registration_progress(&error);
    match error {
        RegistrationError::Authority {
            failure: AuthorityFailure::Api(failure),
            ..
        } => super::cloud::write_registration_failure(
            deployment.fingerprint().api_url(),
            &failure,
            &progress,
        ),
        RegistrationError::Authority {
            stage,
            failure: AuthorityFailure::Local(error),
            ..
        } => Err(anyhow::anyhow!(
            "{REGISTER_RUNNER}: {stage}: {error:#}\n\n{progress}"
        )),
        error @ RegistrationError::Host { .. } => {
            let error = anyhow::Error::new(error);
            Err(anyhow::anyhow!(
                "{REGISTER_RUNNER}: {error:#}\n\n{progress}"
            ))
        }
        RegistrationError::Conflict(conflict) => {
            Err(registration_conflict(REGISTER_RUNNER, &conflict))
        }
    }
}

/// What a failed registration leaves and how to go on: nothing registered,
/// a kept record a start with the same options resumes, or the registered
/// runner that start completes.
fn registration_progress<F>(error: &RegistrationError<F>) -> String {
    match error {
        RegistrationError::Authority {
            runner_id: Some(runner_id),
            ..
        }
        | RegistrationError::Host {
            runner_id: Some(runner_id),
            ..
        } => format!(
            "Runner {runner_id} was registered. Start the runner again with the same options to complete it."
        ),
        // A record that does not read is named by the failure itself.
        RegistrationError::Host {
            stage: RegistrationStage::ReadRecord,
            ..
        } => "Check the pool for a runner registered from this host before removing the registration record.".to_owned(),
        // Before the record is read the start knows nothing of an earlier
        // start's registration, so it says only what it did itself.
        RegistrationError::Host {
            stage: RegistrationStage::LockState | RegistrationStage::ReadState,
            ..
        } => "This start sent nothing to Cloud.".to_owned(),
        RegistrationError::Authority {
            record: Some(record),
            ..
        }
        | RegistrationError::Host {
            record: Some(record),
            ..
        } => format!(
            "The registration is kept in {}. Start the runner again with the same options to resume it.",
            record.display()
        ),
        RegistrationError::Authority { .. } | RegistrationError::Host { .. } => {
            NO_RUNNER_REGISTERED.to_owned()
        }
        RegistrationError::Conflict(_) => String::new(),
    }
}

/// A conflict as the diagnostic `operation` stopped at, its `code:` line and
/// the way forward.
fn registration_conflict(operation: &str, conflict: &RegistrationConflict) -> anyhow::Error {
    let path = conflict.path.display();
    let runner = conflict.runner_id.as_deref().map_or_else(
        || "its runner".to_owned(),
        |runner_id| format!("runner {runner_id}"),
    );
    let remedy = match (conflict.kind, &conflict.recorded) {
        (RegistrationConflictKind::Target, Some(recorded)) => format!(
            "Start the runner with the options its registration was made with:\n  {}\nA host that holds its runner state also starts without --personal and --pool-id. An owner moves a runner to another pool with um runner move.",
            recorded_options(recorded)
        ),
        (RegistrationConflictKind::Target, None) => format!(
            "The host was enrolled with an activation or a join key. Start {runner} without --personal and --pool-id. An owner moves a runner to another pool with um runner move."
        ),
        (RegistrationConflictKind::Unresolved, Some(recorded)) => format!(
            "Finish the registration by starting the runner with its options, signed in to the same deployment and account within six days of when it began:\n  {}\nOtherwise check the pool for a runner registered from this host, then remove {path}.",
            recorded_options(recorded)
        ),
        (RegistrationConflictKind::Unresolved, None) => format!(
            "Finish the staged enrollment with um runner enroll --resume, or the staged join by starting the runner with its join key. Otherwise check the pool for a runner registered from this host, then remove {path}."
        ),
        (RegistrationConflictKind::StateMissing, _) => format!(
            "Restore the runner state file of {runner}. Otherwise remove {path}, and delete {runner} in Cloud if it is no longer wanted."
        ),
    };
    anyhow::anyhow!(
        "{operation}: {conflict}\ncode: {}\n\n{remedy}",
        conflict.kind.code()
    )
}

/// The registration options a record was made with, as a start gives them.
fn recorded_options(recorded: &RecordedOptions) -> String {
    let organization = &recorded.organization;
    match &recorded.target {
        RegistrationTarget::Personal => format!("--personal --organization {organization}"),
        RegistrationTarget::Pool(pool) => {
            format!("--pool-id {pool} --organization {organization}")
        }
    }
}

impl JoinKeySource {
    fn read(self) -> Result<JoinKeyArtifact, super::super::CommandFailure> {
        match self {
            Self::File(path) => Ok(JoinKeyArtifact::read_file(&path)
                .with_context(|| format!("read join key {}", path.display()))?),
            Self::Variable(text) => Err(hand_join_key_to_new_image(&text).into()),
            Self::Prompt => prompt_for_join_key(),
        }
    }
}

/// The longest line the prompt reads, a join key artifact's bound.
const PROMPTED_LINE_LIMIT: usize = 4096;

// A prompted key is read from the terminal on standard input with echo off and
// held only in memory; it was never in the environment, so no `exec` follows.
// A start that has no terminal fails rather than waiting for input.
fn prompt_for_join_key() -> Result<JoinKeyArtifact, super::super::CommandFailure> {
    let stdin = io::stdin();
    if !stdin.is_terminal() {
        return Err(super::super::CommandFailure::with_exit_code(
            anyhow::anyhow!(
                "read join key: --join-key needs a terminal on standard input\n\nGive the join key of a start without a terminal with --join-key-file PATH|- or {JOIN_KEY_VARIABLE}."
            ),
            ExitCode::UsageError,
        ));
    }
    let mut stderr = io::stderr();
    stderr
        .write_all(b"Paste the join key (input is hidden): ")
        .and_then(|()| stderr.flush())
        .context("write the join key prompt")?;
    let line = read_line_without_echo(stdin.as_fd()).context("read join key from the terminal");
    // The newline the terminal did not echo.
    let _ = stderr.write_all(b"\n");
    let line = line?;
    let key = line.strip_suffix(b"\n").unwrap_or(&line);
    let key = key.strip_suffix(b"\r").unwrap_or(key);
    Ok(JoinKeyArtifact::parse(key).context("read join key from the terminal")?)
}

/// Reads one line of at most [`PROMPTED_LINE_LIMIT`] bytes from the terminal
/// `input` with echo off, in canonical mode, and restores its settings however
/// the read ends (see [`QuietTerminal`]). The rest of a longer line is
/// discarded so that it never reaches the next program to read the terminal.
fn read_line_without_echo(input: BorrowedFd<'_>) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    let quiet = QuietTerminal::enter(input)?;
    let mut line = Zeroizing::new(vec![0; PROMPTED_LINE_LIMIT + 1]);
    let mut filled = 0;
    while filled < line.len() && !line[..filled].ends_with(b"\n") {
        match rustix::io::read(input, &mut line[filled..]) {
            Ok(0) => break,
            Ok(count) => filled += count,
            Err(rustix::io::Errno::INTR) => {}
            Err(error) => return Err(anyhow::Error::new(error).context("read the terminal")),
        }
    }
    if filled == line.len() && !line.ends_with(b"\n") {
        rustix::termios::tcflush(input, QueueSelector::IFlush)
            .context("discard the rest of the line")?;
    }
    drop(quiet);
    line.truncate(filled);
    Ok(line)
}

/// The signals whose default action would end the process during the prompt,
/// and [`QUIET_TERMINAL_WAKE`], which ends the wait for them.
const QUIET_TERMINAL_SIGNALS: [Signal; 5] = [
    Signal::SIGINT,
    Signal::SIGTERM,
    Signal::SIGHUP,
    Signal::SIGQUIT,
    QUIET_TERMINAL_WAKE,
];
const QUIET_TERMINAL_WAKE: Signal = Signal::SIGUSR1;

/// A terminal with echo turned off, restored when this is dropped. A guard's
/// `Drop` does not run when a signal's default action ends the process, so
/// while it lives the calling thread, which is the process's only thread at
/// the prompt, blocks [`QUIET_TERMINAL_SIGNALS`] and a thread waits for them:
/// it restores the saved settings first and then lets the signal take its
/// default action. Dropping it restores the settings, wakes and joins that
/// thread, and unblocks the signals, so one that arrived meanwhile takes its
/// default action then.
struct QuietTerminal<'a> {
    input: BorrowedFd<'a>,
    saved: Termios,
    previous_mask: SigSet,
    done: Arc<AtomicBool>,
    waiter: Option<JoinHandle<()>>,
}

impl<'a> QuietTerminal<'a> {
    fn enter(input: BorrowedFd<'a>) -> anyhow::Result<Self> {
        let saved = rustix::termios::tcgetattr(input).context("read the terminal's settings")?;
        let signals = QUIET_TERMINAL_SIGNALS.into_iter().collect::<SigSet>();
        let previous_mask = signals
            .thread_swap_mask(SigmaskHow::SIG_BLOCK)
            .context("block signals during the prompt")?;
        let done = Arc::new(AtomicBool::new(false));
        let mut quiet = Self {
            input,
            saved: saved.clone(),
            previous_mask,
            done: Arc::clone(&done),
            waiter: None,
        };
        // The waiter inherits the blocked signals, so it alone takes them.
        quiet.waiter = Some(
            std::thread::Builder::new()
                .name("join-key-prompt-signals".to_owned())
                .spawn(move || restore_terminal_on_signal(signals, &saved, &done))
                .context("watch signals during the prompt")?,
        );
        let mut silent = quiet.saved.clone();
        silent.local_modes.remove(LocalModes::ECHO);
        rustix::termios::tcsetattr(input, OptionalActions::Now, &silent)
            .context("turn off the terminal's echo")?;
        Ok(quiet)
    }
}

impl Drop for QuietTerminal<'_> {
    fn drop(&mut self) {
        let _ = rustix::termios::tcsetattr(self.input, OptionalActions::Now, &self.saved);
        if let Some(waiter) = self.waiter.take() {
            self.done.store(true, Ordering::Release);
            // Blocked in both threads, the signal stays pending until the
            // waiter takes it; a waiter that has already returned takes none.
            if !waiter.is_finished()
                && nix::sys::signal::kill(nix::unistd::Pid::this(), QUIET_TERMINAL_WAKE).is_ok()
            {
                let _ = waiter.join();
            }
        }
        let _ = self.previous_mask.thread_set_mask();
    }
}

fn restore_terminal_on_signal(signals: SigSet, saved: &Termios, done: &AtomicBool) {
    let Ok(signal) = signals.wait() else {
        return;
    };
    if signal == QUIET_TERMINAL_WAKE && done.load(Ordering::Acquire) {
        return;
    }
    let _ = rustix::termios::tcsetattr(io::stdin(), OptionalActions::Now, saved);
    let _ = rustix::io::write(io::stderr(), b"\n");
    // Unblocked in this thread, the signal raised again takes its default
    // action, which ends the process; exit as it would, should it not.
    let _ = SigSet::from(signal).thread_unblock();
    let _ = nix::sys::signal::raise(signal);
    let exit_code = if signal == Signal::SIGINT {
        ExitCode::Interrupted
    } else {
        ExitCode::Terminated
    };
    process::exit(i32::from(exit_code.as_u8()));
}

// A key in UM_RUNNER_JOIN_KEY never stays in this process's environment block,
// which `/proc/<pid>/environ` and `ps eww` read. The command validates it and
// replaces itself with the same `um runner serve` started without the variable
// and with the key on a pipe as `--join-key-file -`; the new image has no
// variable, so it reads the pipe and does not hand the key on again.
//
// Returns only when the handoff fails: a successful `exec` does not return.
fn hand_join_key_to_new_image(text: &OsStr) -> anyhow::Error {
    match stage_join_key_handoff(text) {
        Ok(mut command) => anyhow::Error::new(command.exec())
            .context("start um runner serve with the join key on standard input"),
        Err(error) => error,
    }
}

// An enrolled host does not read UM_RUNNER_JOIN_KEY, but the variable still
// leaves its environment block: the command replaces itself with the same
// `um runner serve` started without it. Returns only when the restart fails.
fn restart_without_join_key_variable() -> anyhow::Error {
    match std::env::current_exe().context("locate the um executable") {
        Ok(executable) => anyhow::Error::new(
            join_key_handoff_command(&executable, std::env::args_os(), None).exec(),
        )
        .context(format!(
            "restart um runner serve without {JOIN_KEY_VARIABLE}"
        )),
        Err(error) => error,
    }
}

fn stage_join_key_handoff(text: &OsStr) -> anyhow::Result<process::Command> {
    let key = text.as_bytes();
    JoinKeyArtifact::parse(key)
        .with_context(|| format!("read join key from {JOIN_KEY_VARIABLE}"))?;
    // The parsed key is at most 4096 bytes, within any pipe's capacity, so the
    // write completes before the new image starts reading.
    let (reader, mut writer) = io::pipe().context("open the join key pipe")?;
    writer
        .write_all(key)
        .context("write the join key to its pipe")?;
    drop(writer);
    let executable = std::env::current_exe().context("locate the um executable")?;
    Ok(join_key_handoff_command(
        &executable,
        std::env::args_os(),
        Some(Stdio::from(reader)),
    ))
}

// The same `um runner serve` without UM_RUNNER_JOIN_KEY, given the key on
// standard input as `--join-key-file -` when there is one to hand on.
fn join_key_handoff_command(
    executable: &Path,
    arguments: impl IntoIterator<Item = OsString>,
    key: Option<Stdio>,
) -> process::Command {
    let mut arguments = arguments.into_iter();
    let mut command = process::Command::new(executable);
    if let Some(name) = arguments.next() {
        command.arg0(name);
    }
    command.args(arguments).env_remove(JOIN_KEY_VARIABLE);
    if let Some(key) = key {
        command.args(["--join-key-file", "-"]).stdin(key);
    }
    command
}

fn join_unless_enrolled(
    config_path: &Path,
    join_key: JoinKeyArtifact,
) -> Result<(), super::super::CommandFailure> {
    let join_key_id = join_key.join_key_id().to_owned();
    // The report is commentary: a failed write to stderr does not stop the join.
    let report_retry = |retry| {
        let _ = write_join_retry(&mut io::stderr().lock(), &join_key_id, retry);
    };
    match um_runner::join_unless_enrolled(config_path, join_key, report_retry) {
        Ok(JoinOutcome::AlreadyEnrolled) => Ok(()),
        Ok(JoinOutcome::Joined(response)) => {
            write_enrolled(&mut io::stderr().lock(), "✓ Runner joined.", &response)?;
            Ok(())
        }
        Err(error) => Err(join_failure(&join_key_id, error).into()),
    }
}

/// Said of a refused join only when no earlier send of it can have reached
/// Cloud: a resent join may have registered a runner Cloud has since deleted.
const NO_RUNNER_REGISTERED: &str = "No runner was registered.";

fn join_failure(join_key_id: &str, error: EnrollmentError) -> anyhow::Error {
    match error {
        error @ EnrollmentError::JoinRefused { refusal, resent } => {
            let registration = if resent {
                "Cloud holds no runner for this host's join: none was registered, or the one registered was deleted."
            } else {
                NO_RUNNER_REGISTERED
            };
            anyhow::anyhow!(
                "join runner pool with join key {join_key_id}: {error}\ncode: {}\n\n{registration} Start the runner with a join key Cloud accepts.",
                refusal.code()
            )
        }
        // Cloud rejected the token this start gave, so starting again with the
        // same key, however it is given, resends a join it rejects again.
        EnrollmentError::StagedJoinUnauthorized {
            ref journal,
            ref join_key_id,
        } => anyhow::anyhow!(
            "join runner pool with join key {join_key_id}: {error}\ncode: {}\n\nCloud rejected the token this start gave for join key {join_key_id}, so the same join key will be rejected again however it is given. Start um runner serve with join key {join_key_id} as it was issued, which completes the staged join. Otherwise look in the pool for a runner that joined from this host and delete it if there is one, then remove {} and start the runner with a join key Cloud accepts.",
            JoinRefusal::Unauthorized.code(),
            journal.display()
        ),
        EnrollmentError::RegistrationConflict(conflict) => registration_conflict(
            &format!("join runner pool with join key {join_key_id}"),
            &conflict,
        ),
        error => anyhow::Error::new(error)
            .context(format!("join runner pool with join key {join_key_id}")),
    }
}

fn write_enrolled(
    output: &mut impl Write,
    heading: &str,
    response: &EnrollmentResponse,
) -> anyhow::Result<()> {
    writeln!(output, "{heading}\n")?;
    writeln!(output, "runner:     {}", response.runner_id())?;
    writeln!(output, "name:       {}", response.runner_name())?;
    writeln!(output, "pool:       {}", response.pool_name())?;
    writeln!(output, "credential: {}", response.credential_id())?;
    Ok(())
}

fn write_join_retry(
    output: &mut impl Write,
    join_key_id: &str,
    retry: JoinRetry,
) -> io::Result<()> {
    let status = match retry.cause {
        AmbiguousCause::CloudUnavailable(status) => format!(" (HTTP {status})"),
        AmbiguousCause::Network | AmbiguousCause::RateLimited => String::new(),
    };
    writeln!(
        output,
        "join runner pool with join key {join_key_id}: no definite answer from Cloud{status}; sending the same join again in {}",
        human_duration(retry.delay)
    )?;
    writeln!(output, "cause: {}", retry.cause.code())
}

const fn service_exit_code(requires_operator_recovery: bool) -> ExitCode {
    if requires_operator_recovery {
        ExitCode::RunnerRecoveryRequired
    } else {
        ExitCode::GeneralFailure
    }
}

fn discover_harness_installations_with<PiDiscovery, ClaudeCodeDiscovery, CodexDiscovery>(
    discover_pi: PiDiscovery,
    discover_claude_code: ClaudeCodeDiscovery,
    discover_codex: CodexDiscovery,
) -> (
    Option<ValidatedPiInstallation>,
    Option<ValidatedClaudeCodeInstallation>,
    Option<ValidatedCodexInstallation>,
)
where
    PiDiscovery: FnOnce() -> Result<ValidatedPiInstallation, PiInstallationFailure>,
    ClaudeCodeDiscovery:
        FnOnce() -> Result<ValidatedClaudeCodeInstallation, ClaudeCodeInstallationFailure>,
    CodexDiscovery: FnOnce() -> Result<ValidatedCodexInstallation, CodexInstallationFailure>,
{
    (
        discover_pi().ok(),
        discover_claude_code().ok(),
        discover_codex().ok(),
    )
}

fn configure_harness_installations(
    config: Config,
    pi_installation: Option<ValidatedPiInstallation>,
    claude_code_installation: Option<ValidatedClaudeCodeInstallation>,
    codex_installation: Option<ValidatedCodexInstallation>,
) -> Config {
    let config = match pi_installation {
        Some(installation) => config.with_pi_installation(installation),
        None => config,
    };
    let config = match claude_code_installation {
        Some(installation) => config.with_claude_code_installation(installation),
        None => config,
    };
    match codex_installation {
        Some(installation) => config.with_codex_installation(installation),
        None => config,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::path::PathBuf;
    use std::time::Duration;

    use super::*;
    use um_execution::ClaudeCodeIncompatibility;

    // REQ-RUNNERENROLL-32
    #[test]
    fn startup_retains_each_available_harness_snapshot_independently() {
        for (pi_available, claude_code_available, codex_available) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, false, true),
            (true, true, false),
            (true, false, true),
            (false, true, true),
            (true, true, true),
        ] {
            let pi_calls = Cell::new(0);
            let claude_code_calls = Cell::new(0);
            let codex_calls = Cell::new(0);
            let pi = ValidatedPiInstallation::fixture(PathBuf::from("/validated/pi"));
            let claude_code =
                ValidatedClaudeCodeInstallation::fixture(PathBuf::from("/validated/claude"));
            let codex = ValidatedCodexInstallation::fixture(PathBuf::from("/validated/codex"));
            let work_root = tempfile::tempdir().unwrap();
            let config = Config::fixture_for_command_tests(
                "ws://127.0.0.1:8081/v1/runner/connect",
                work_root.path(),
            )
            .unwrap();

            let (pi_installation, claude_code_installation, codex_installation) =
                discover_harness_installations_with(
                    || {
                        pi_calls.set(pi_calls.get() + 1);
                        pi_available
                            .then(|| pi.clone())
                            .ok_or(PiInstallationFailure::Missing)
                    },
                    || {
                        claude_code_calls.set(claude_code_calls.get() + 1);
                        claude_code_available
                            .then(|| claude_code.clone())
                            .ok_or(ClaudeCodeInstallationFailure::Missing)
                    },
                    || {
                        codex_calls.set(codex_calls.get() + 1);
                        codex_available
                            .then(|| codex.clone())
                            .ok_or(CodexInstallationFailure::Missing)
                    },
                );
            let config = configure_harness_installations(
                config,
                pi_installation,
                claude_code_installation,
                codex_installation,
            );

            assert_eq!(pi_calls.get(), 1);
            assert_eq!(claude_code_calls.get(), 1);
            assert_eq!(codex_calls.get(), 1);
            assert_eq!(config.pi_installation(), pi_available.then_some(&pi));
            assert_eq!(
                config.claude_code_installation(),
                claude_code_available.then_some(&claude_code),
            );
            assert_eq!(
                config.codex_installation(),
                codex_available.then_some(&codex),
            );
        }
    }

    // REQ-RUNNERENROLL-38
    #[test]
    fn recovery_requirement_selects_the_supervisor_stopping_exit_code() {
        assert_eq!(service_exit_code(true), ExitCode::RunnerRecoveryRequired);
        assert_eq!(service_exit_code(false), ExitCode::GeneralFailure);
    }

    // REQ-RUNNERENROLL-32
    #[test]
    fn incompatible_claude_code_and_missing_codex_do_not_remove_compatible_pi() {
        let pi = ValidatedPiInstallation::fixture(PathBuf::from("/validated/pi"));
        let work_root = tempfile::tempdir().unwrap();
        let config = Config::fixture_for_command_tests(
            "ws://127.0.0.1:8081/v1/runner/connect",
            work_root.path(),
        )
        .unwrap();

        let (pi_installation, claude_code_installation, codex_installation) =
            discover_harness_installations_with(
                || Ok(pi.clone()),
                || {
                    Err(ClaudeCodeInstallationFailure::Unsupported(
                        ClaudeCodeIncompatibility::Version("2.1.221".to_owned()),
                    ))
                },
                || Err(CodexInstallationFailure::Missing),
            );
        let config = configure_harness_installations(
            config,
            pi_installation,
            claude_code_installation,
            codex_installation,
        );

        assert_eq!(config.pi_installation(), Some(&pi));
        assert_eq!(config.claude_code_installation(), None);
        assert_eq!(config.codex_installation(), None);
    }

    // REQ-RUNNERENROLL-113
    #[test]
    fn a_join_retry_report_names_the_key_its_cause_and_the_delay_handed_over() {
        let join_key_id = "rjk_01k0z6r1w8f4jy2m7q9v3x5abc";
        let mut output = Vec::new();

        write_join_retry(
            &mut output,
            join_key_id,
            JoinRetry {
                cause: AmbiguousCause::RateLimited,
                delay: Duration::from_millis(1500),
            },
        )
        .unwrap();

        let output = String::from_utf8(output).unwrap();
        assert!(
            output.lines().any(|line| line == "cause: rate_limited"),
            "{output}"
        );
        assert!(output.contains(join_key_id), "{output}");
        assert!(output.contains("1.5s"), "{output}");

        // A delay under a tenth of a second is still the delay handed over.
        let mut output = Vec::new();
        write_join_retry(
            &mut output,
            join_key_id,
            JoinRetry {
                cause: AmbiguousCause::Network,
                delay: Duration::from_millis(40),
            },
        )
        .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("40ms"), "{output}");
    }

    // REQ-RUNNERENROLL-97
    #[test]
    fn a_refused_join_claims_no_registration_only_when_no_earlier_send_can_have_registered() {
        const JOIN_KEY_ID: &str = "rjk_01k0z6r1w8f4jy2m7q9v3x5abc";
        for refusal in [
            JoinRefusal::Unauthorized,
            JoinRefusal::Gone,
            JoinRefusal::Conflict,
        ] {
            for resent in [false, true] {
                let failure = join_failure(
                    JOIN_KEY_ID,
                    EnrollmentError::JoinRefused { refusal, resent },
                )
                .to_string();

                assert!(
                    failure
                        .lines()
                        .any(|line| line == format!("code: {}", refusal.code())),
                    "{failure}"
                );
                assert_eq!(failure.contains(NO_RUNNER_REGISTERED), !resent, "{failure}");
            }
        }

        let journal = PathBuf::from("/var/lib/um/runner-state.join.json");
        let failure = join_failure(
            JOIN_KEY_ID,
            EnrollmentError::StagedJoinUnauthorized {
                journal: journal.clone(),
                join_key_id: JOIN_KEY_ID.to_owned(),
            },
        )
        .to_string();
        assert!(
            failure
                .lines()
                .any(|line| line == format!("code: {}", JoinRefusal::Unauthorized.code())),
            "{failure}"
        );
        assert!(
            failure.contains(&journal.display().to_string()),
            "{failure}"
        );
        assert!(!failure.contains(NO_RUNNER_REGISTERED), "{failure}");
    }

    // REQ-RUNNERENROLL-116: a refused registration says that no runner was
    // registered; a start that stopped before it read the host's
    // registration record cannot know that of an earlier start.
    #[test]
    fn a_stopped_registration_claims_no_runner_only_once_it_has_read_the_record() {
        let refused = RegistrationError::Authority {
            stage: RegistrationStage::Register,
            failure: (),
            runner_id: None,
            record: None,
        };
        assert_eq!(registration_progress(&refused), NO_RUNNER_REGISTERED);

        for (stage, error) in [
            (RegistrationStage::LockState, EnrollmentError::StateLock),
            (RegistrationStage::ReadState, EnrollmentError::InvalidState),
        ] {
            let unread = RegistrationError::<()>::Host {
                stage,
                error,
                runner_id: None,
                record: None,
            };

            let progress = registration_progress(&unread);

            assert!(!progress.contains(NO_RUNNER_REGISTERED), "{progress}");
        }
    }

    // REQ-RUNNERENROLL-97
    #[test]
    fn a_join_key_variable_is_handed_to_a_new_image_without_it_on_standard_input() {
        let (reader, _writer) = io::pipe().unwrap();
        let command = join_key_handoff_command(
            Path::new("/installed/um"),
            ["um", "runner", "serve", "--config", "/etc/um/runner.toml"].map(OsString::from),
            Some(Stdio::from(reader)),
        );

        assert_eq!(command.get_program(), "/installed/um");
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            [
                "runner",
                "serve",
                "--config",
                "/etc/um/runner.toml",
                "--join-key-file",
                "-",
            ],
        );
        assert_eq!(
            command.get_envs().collect::<Vec<_>>(),
            [(std::ffi::OsStr::new(JOIN_KEY_VARIABLE), None)],
        );
    }

    // REQ-RUNNERENROLL-97
    #[test]
    fn an_enrolled_host_restarts_without_the_join_key_variable_and_hands_on_no_key() {
        let command = join_key_handoff_command(
            Path::new("/installed/um"),
            ["um", "runner", "serve", "--config", "/etc/um/runner.toml"].map(OsString::from),
            None,
        );

        assert_eq!(command.get_program(), "/installed/um");
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["runner", "serve", "--config", "/etc/um/runner.toml"],
        );
        assert_eq!(
            command.get_envs().collect::<Vec<_>>(),
            [(std::ffi::OsStr::new(JOIN_KEY_VARIABLE), None)],
        );
    }
}
