use std::ffi::{OsStr, OsString};
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{self, Stdio};

use anyhow::Context;
use clap::Args;

use crate::exit_code::ExitCode;
use um_execution::{
    ClaudeCodeInstallationFailure, CodexInstallationFailure, PiInstallationFailure,
    ValidatedClaudeCodeInstallation, ValidatedCodexInstallation, ValidatedPiInstallation,
    discover_and_validate_claude_code_installation, discover_and_validate_codex_installation,
    discover_and_validate_pi_installation,
};
use um_runner::{
    Config, EnrollmentError, EnrollmentResponse, JoinKeyArtifact, JoinOutcome, JoinRefusal,
};

pub(super) const ABOUT: &str = "Serve run assignments";

const JOIN_KEY_VARIABLE: &str = "UM_RUNNER_JOIN_KEY";

#[derive(Debug, Args)]
pub(super) struct Command {
    /// Read the closed runner operator configuration and enrolled state; the
    /// runner home under the um home is the default, created where it is missing.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Join the key's pool from a protected file or explicit stdin when the
    /// host holds no enrolled state; UM_RUNNER_JOIN_KEY may hold the key instead.
    #[arg(long, value_name = "PATH|-")]
    join_key_file: Option<PathBuf>,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let join_key = join_key_source(
            self.join_key_file.as_deref(),
            std::env::var_os(JOIN_KEY_VARIABLE),
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
        // The runner home is prepared once the join key's source is known, so
        // a key given both ways writes nothing, and before the host's state is
        // judged. The image a key in the variable is handed to prepares it
        // again and finds nothing to do.
        let config_path = match super::initialized_operator_config_path(self.config.as_deref()) {
            Ok(path) => path,
            Err(error) => {
                discover();
                return Err(error.into());
            }
        };
        // A host whose state file is present holds enrolled state: it serves
        // with it and never reads its join key, so a key that is gone or
        // unreadable does not stop it, and a configuration or state that does
        // not load is the start's failure. The journal its own completed join
        // left is removed first, as the second copy of the credential secret it is.
        let configuration_failure = |error| {
            discover();
            config_load_error(&config_path, error)
        };
        let (config, (pi_installation, claude_code_installation, codex_installation)) =
            match (Config::holds_enrolled_state(&config_path), join_key) {
                (Err(error), _) => return Err(configuration_failure(error).into()),
                // A host with no state file and no key has nothing to join
                // with: what can be checked of its configuration before a join
                // keeps its own cause, and otherwise it is not enrolled.
                (Ok(false), None) => {
                    if let Err(error) = Config::validate_before_join(&config_path) {
                        return Err(configuration_failure(error).into());
                    }
                    discover();
                    return Err(not_enrolled(&config_path, self.config.is_some()).into());
                }
                (Ok(true), join_key) => match (Config::load(&config_path), join_key) {
                    (Err(error), _) => return Err(configuration_failure(error).into()),
                    (Ok(_), Some(JoinKeySource::Variable(_))) => {
                        return Err(restart_without_join_key_variable().into());
                    }
                    (Ok(config), _) => {
                        um_runner::remove_completed_join_journal(&config_path)
                            .context("remove the journal of the runner's completed join")?;
                        (config, discover())
                    }
                },
                // A host with no state file checks what it can of its
                // configuration before it reads the key, in both images of a
                // key given in the variable, so a host that cannot serve
                // stages, sends and writes nothing.
                (Ok(false), Some(join_key)) => {
                    if let Err(error) = Config::validate_before_join(&config_path) {
                        return Err(configuration_failure(error).into());
                    }
                    let key = join_key.read()?;
                    let installations = discover();
                    join_unless_enrolled(&config_path, key)?;
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

/// A start on a host that holds no enrolled state and was given no join key
/// (REQ-RUNNERENROLL-30). The commands it names repeat `--config` when the
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
}

fn join_key_source(
    file: Option<&Path>,
    variable: Option<OsString>,
) -> Result<Option<JoinKeySource>, super::super::CommandFailure> {
    match (file, variable) {
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(super::super::CommandFailure::with_exit_code(
            anyhow::anyhow!(
                "read join key: --join-key-file and {JOIN_KEY_VARIABLE} are both set\n\nGive the join key in one of them."
            ),
            ExitCode::UsageError,
        )),
        (Some(path), None) => Ok(Some(JoinKeySource::File(path.to_owned()))),
        (None, Some(text)) => Ok(Some(JoinKeySource::Variable(text))),
    }
}

impl JoinKeySource {
    fn read(self) -> Result<JoinKeyArtifact, super::super::CommandFailure> {
        match self {
            Self::File(path) => Ok(JoinKeyArtifact::read_file(&path)
                .with_context(|| format!("read join key {}", path.display()))?),
            Self::Variable(text) => Err(hand_join_key_to_new_image(&text).into()),
        }
    }
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
    match um_runner::join_unless_enrolled(config_path, join_key) {
        Ok(JoinOutcome::AlreadyEnrolled) => Ok(()),
        Ok(JoinOutcome::Joined(response)) => {
            write_join(&mut io::stderr().lock(), &response)?;
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
        // same key file or variable resends a join it rejects again.
        EnrollmentError::StagedJoinUnauthorized {
            ref journal,
            ref join_key_id,
        } => anyhow::anyhow!(
            "join runner pool with join key {join_key_id}: {error}\ncode: {}\n\nCloud rejected the token this start gave for join key {join_key_id}, so the same join key file or {JOIN_KEY_VARIABLE} value will be rejected again. Start um runner serve with join key {join_key_id} as it was issued, which completes the staged join. Otherwise look in the pool for a runner that joined from this host and delete it if there is one, then remove {} and start the runner with a join key Cloud accepts.",
            JoinRefusal::Unauthorized.code(),
            journal.display()
        ),
        error => anyhow::Error::new(error)
            .context(format!("join runner pool with join key {join_key_id}")),
    }
}

fn write_join(output: &mut impl Write, response: &EnrollmentResponse) -> anyhow::Result<()> {
    writeln!(output, "✓ Runner joined.\n")?;
    writeln!(output, "runner:     {}", response.runner_id())?;
    writeln!(output, "name:       {}", response.runner_name())?;
    writeln!(output, "pool:       {}", response.pool_name())?;
    writeln!(output, "credential: {}", response.credential_id())?;
    Ok(())
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
