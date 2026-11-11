use std::env;
use std::ffi::{OsStr, OsString};
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use rustix::io::Errno;
use rustix::process::{Pid, Signal, WaitId, WaitIdOptions, kill_process_group, waitid};

const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const TEST_WORKER_EXECUTABLE: &str = "UM_TEST_INTERNAL_WORKER_EXECUTABLE";

/// Set when the running executable asks, at startup, whether it was launched in an internal
/// worker role: that executable dispatches the worker roles and may be its own worker.
static DISPATCHES_WORKER_ROLES: AtomicBool = AtomicBool::new(false);

/// Records that the running executable dispatches the internal worker roles. Only the two
/// `internal_worker_requested` predicates call this; a test never does, because the record is
/// process-wide and would make the whole harness its own worker (INV-TESTING-20).
pub(crate) fn record_internal_worker_dispatch() {
    DISPATCHES_WORKER_ROLES.store(true, Ordering::Relaxed);
}

/// The executable launched in the child-guard and result-validation worker roles. A shipped
/// build is its own worker. A test build resolves it by INV-TESTING-20, see
/// [`test_build_internal_worker_executable`].
pub(crate) fn internal_worker_executable() -> io::Result<PathBuf> {
    if cfg!(any(test, feature = "test-fixtures")) {
        return test_build_internal_worker_executable(
            env::var_os(TEST_WORKER_EXECUTABLE),
            dispatches_internal_worker_roles(),
            &env::current_exe()?,
        );
    }
    env::current_exe()
}

/// The worker a test build resolves when no worker executable is supplied.
#[cfg(feature = "test-fixtures")]
pub(crate) fn unsupplied_internal_worker_executable() -> io::Result<PathBuf> {
    test_build_internal_worker_executable(
        None,
        dispatches_internal_worker_roles(),
        &env::current_exe()?,
    )
}

/// Whether the running executable has asked for a worker role. No harness has, the execution
/// crate's own included, because no test calls the predicates that record it.
fn dispatches_internal_worker_roles() -> bool {
    DISPATCHES_WORKER_ROLES.load(Ordering::Relaxed)
}

/// A test build takes a supplied worker when it is a file. Unsupplied, an executable that
/// dispatches the worker roles at startup (the `um` binary or the `internal-worker` example)
/// is its own worker; any other executable is a test harness, which would run its suite in a
/// worker role and launch itself again, so it takes the example beside it (INV-TESTING-20).
fn test_build_internal_worker_executable(
    supplied: Option<OsString>,
    dispatches_worker_roles: bool,
    executable: &Path,
) -> io::Result<PathBuf> {
    if let Some(path) = supplied {
        let path = PathBuf::from(path);
        return path.is_file().then_some(path).ok_or_else(|| {
            io::Error::other("supplied test internal-worker executable unavailable")
        });
    }
    if dispatches_worker_roles {
        return Ok(executable.to_path_buf());
    }
    test_build_worker_example(executable)
}

/// The `internal-worker` example beside a test harness, `<profile>/examples/internal-worker`
/// for a harness at `<profile>/deps/<crate>-<hash>`, which a plain `cargo test -p um-execution`
/// builds and no other package's test command does. A `--all-targets` build compiles the
/// example as a test harness instead, so such a run builds the ordinary example and names it in
/// `UM_TEST_INTERNAL_WORKER_EXECUTABLE`, as `cli/scripts/check-suite` does.
fn test_build_worker_example(test_executable: &Path) -> io::Result<PathBuf> {
    let example = test_executable
        .parent()
        .and_then(Path::parent)
        .map(|profile| profile.join("examples").join("internal-worker"));
    match example {
        Some(example) if example.is_file() => Ok(example),
        _ => Err(io::Error::other(
            "test internal-worker executable not supplied: set \
             UM_TEST_INTERNAL_WORKER_EXECUTABLE or build the um-execution internal-worker example",
        )),
    }
}

pub struct ManagedProcessGroup {
    child: Child,
    process_group: Option<Pid>,
    reaped: bool,
}

impl ManagedProcessGroup {
    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        command.process_group(0);
        let mut child = command.spawn()?;
        let Some(process_group) = i32::try_from(child.id()).ok().and_then(Pid::from_raw) else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::other("spawned process ID is out of range"));
        };
        Ok(Self {
            process_group: Some(process_group),
            child,
            reaped: false,
        })
    }

    pub(crate) fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if self.reaped {
            return self.child.try_wait();
        }
        let Some(process_group) = self.process_group else {
            let status = self.child.try_wait()?;
            self.reaped |= status.is_some();
            return Ok(status);
        };
        let exited = leader_has_exited(process_group, true)?;
        if !exited {
            return Ok(None);
        }

        // Keep the exited leader waitable while terminating its group. Reaping
        // first would free the numeric group identity for reuse before SIGKILL.
        self.terminate_process_group();
        let status = self.child.wait()?;
        self.reaped = true;
        Ok(Some(status))
    }

    #[cfg(test)]
    pub(crate) fn wait(&mut self) -> io::Result<ExitStatus> {
        if !self.reaped
            && let Some(process_group) = self.process_group
        {
            let _ = leader_has_exited(process_group, false)?;
            self.terminate_process_group();
        }
        let status = self.child.wait()?;
        self.reaped = true;
        Ok(status)
    }

    pub(crate) fn terminate_process_group(&mut self) {
        if let Some(process_group) = self.process_group.take() {
            let _ = kill_process_group(process_group, Signal::KILL);
        }
    }

    pub(crate) fn terminate(&mut self) {
        self.terminate_process_group();
        if !self.reaped {
            let _ = self.child.kill();
            let _ = self.child.wait();
            self.reaped = true;
        }
    }
}

impl Drop for ManagedProcessGroup {
    fn drop(&mut self) {
        self.terminate();
    }
}

fn leader_has_exited(process: Pid, nohang: bool) -> io::Result<bool> {
    let mut options = WaitIdOptions::EXITED | WaitIdOptions::NOWAIT;
    if nohang {
        options |= WaitIdOptions::NOHANG;
    }
    loop {
        match waitid(WaitId::Pid(process), options) {
            Ok(status) => return Ok(status.is_some()),
            Err(Errno::INTR) => {}
            Err(error) => {
                return Err(io::Error::from_raw_os_error(error.raw_os_error()));
            }
        }
    }
}

pub trait CommandRunner: Send + Sync {
    fn run(&self, command: CommandRequest<'_>) -> Result<CommandOutput, CommandProbeError>;
}

#[derive(Clone, Copy)]
pub struct CommandRequest<'a> {
    pub program: &'a Path,
    pub args: &'a [&'a str],
    pub timeout: Duration,
    pub maximum_stdout_bytes: usize,
    pub clear_environment: bool,
    pub environment: &'a [(&'a OsStr, &'a OsStr)],
    pub current_directory: Option<&'a Path>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandOutput {
    pub success: bool,
    pub stdout: Vec<u8>,
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandProbeError {
    CommandNotFound,
    Spawn,
    Timeout,
    Wait,
    PipeRead,
}

pub struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn run(&self, command: CommandRequest<'_>) -> Result<CommandOutput, CommandProbeError> {
        let executable = resolve_program(command.program)?;
        let mut process = Command::new(executable);
        process
            .args(command.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if command.clear_environment {
            process.env_clear();
        }
        process.envs(command.environment.iter().copied());
        if let Some(current_directory) = command.current_directory {
            process.current_dir(current_directory);
        }
        let mut child =
            ManagedProcessGroup::spawn(&mut process).map_err(|error| match error.kind() {
                io::ErrorKind::NotFound => CommandProbeError::CommandNotFound,
                _ => CommandProbeError::Spawn,
            })?;

        let child_process = child.child_mut();
        let (Some(stdout), Some(stderr)) =
            (child_process.stdout.take(), child_process.stderr.take())
        else {
            child.terminate();
            return Err(CommandProbeError::PipeRead);
        };
        let stdout_thread =
            thread::spawn(move || drain_stdout(stdout, command.maximum_stdout_bytes));
        let stderr_thread = thread::spawn(move || drain(stderr));
        let started = um_support::monotonic_now();

        let success = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status.success(),
                Ok(None) if um_support::elapsed(started) >= command.timeout => {
                    child.terminate();
                    let _ = join_readers(stdout_thread, stderr_thread);
                    return Err(CommandProbeError::Timeout);
                }
                Ok(None) => um_support::sleep(WAIT_POLL_INTERVAL),
                Err(_) => {
                    child.terminate();
                    let _ = join_readers(stdout_thread, stderr_thread);
                    return Err(CommandProbeError::Wait);
                }
            }
        };

        child.terminate_process_group();
        let (stdout, truncated) = join_readers(stdout_thread, stderr_thread)?;
        Ok(CommandOutput {
            success,
            stdout,
            truncated,
        })
    }
}

fn resolve_program(program: &Path) -> Result<PathBuf, CommandProbeError> {
    if !matches!(
        program.components().collect::<Vec<_>>().as_slice(),
        [Component::Normal(_)]
    ) {
        return Ok(program.to_owned());
    }

    // Resolve bare names ourselves so command classification does not depend
    // on platform-specific spawn-time PATH lookup.
    let search_path = env::var_os("PATH").ok_or(CommandProbeError::CommandNotFound)?;
    let mut inaccessible_candidate = false;
    for directory in env::split_paths(&search_path) {
        let candidate = directory.join(program);
        match candidate.metadata() {
            Ok(metadata) if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 => {
                return Ok(candidate);
            }
            Ok(_) => inaccessible_candidate = true,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) => {}
            Err(_) => inaccessible_candidate = true,
        }
    }

    if inaccessible_candidate {
        Err(CommandProbeError::Spawn)
    } else {
        Err(CommandProbeError::CommandNotFound)
    }
}

fn join_readers(
    stdout_thread: thread::JoinHandle<io::Result<(Vec<u8>, bool)>>,
    stderr_thread: thread::JoinHandle<io::Result<()>>,
) -> Result<(Vec<u8>, bool), CommandProbeError> {
    let stdout = stdout_thread
        .join()
        .map_err(|_| CommandProbeError::PipeRead)?
        .map_err(|_| CommandProbeError::PipeRead)?;
    stderr_thread
        .join()
        .map_err(|_| CommandProbeError::PipeRead)?
        .map_err(|_| CommandProbeError::PipeRead)?;
    Ok(stdout)
}

fn drain_stdout(mut reader: impl Read, maximum_bytes: usize) -> io::Result<(Vec<u8>, bool)> {
    let mut retained = Vec::with_capacity(maximum_bytes);
    let mut buffer = [0_u8; 4096];
    let mut truncated = false;

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok((retained, truncated));
        }

        let available = maximum_bytes.saturating_sub(retained.len());
        let retained_bytes = available.min(read);
        retained.extend_from_slice(&buffer[..retained_bytes]);
        truncated |= retained_bytes < read;
    }
}

fn drain(mut reader: impl Read) -> io::Result<()> {
    let mut buffer = [0_u8; 4096];
    while reader.read(&mut buffer)? != 0 {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use nix::sys::stat::Mode;
    use nix::unistd::mkfifo;

    use super::{
        CommandProbeError, CommandRequest, CommandRunner, SystemCommandRunner,
        test_build_internal_worker_executable,
    };

    const FIXTURE_STDOUT_LIMIT: usize = 8 * 1024;

    /// A target directory with a test harness at `<profile>/deps/um-0123abcd`, a supplied
    /// worker file, and, when `example_is_file`, the `internal-worker` example beside it.
    struct WorkerLayout {
        _target: tempfile::TempDir,
        harness: PathBuf,
        example: PathBuf,
        supplied: PathBuf,
    }

    fn worker_layout(example_is_file: bool) -> WorkerLayout {
        let target = tempfile::tempdir().unwrap();
        let profile = target.path().join("debug");
        std::fs::create_dir_all(profile.join("deps")).unwrap();
        std::fs::create_dir_all(profile.join("examples")).unwrap();
        let harness = profile.join("deps").join("um-0123abcd");
        std::fs::write(&harness, b"").unwrap();
        let example = profile.join("examples").join("internal-worker");
        if example_is_file {
            std::fs::write(&example, b"").unwrap();
        } else {
            std::fs::create_dir_all(&example).unwrap();
        }
        let supplied = target.path().join("supplied-worker");
        std::fs::write(&supplied, b"").unwrap();
        WorkerLayout {
            _target: target,
            harness,
            example,
            supplied,
        }
    }

    // INV-TESTING-20
    #[test]
    fn an_unsupplied_test_harness_takes_the_worker_example_and_never_itself() {
        let layout = worker_layout(true);
        let worker = test_build_internal_worker_executable(None, false, &layout.harness).unwrap();
        assert_eq!(worker, layout.example);
        assert_ne!(worker, layout.harness);

        let layout = worker_layout(false);
        for harness in [layout.harness.as_path(), Path::new("/um-0123abcd")] {
            let error = test_build_internal_worker_executable(None, false, harness).unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::Other);
            assert_eq!(error.raw_os_error(), None);
        }
    }

    // INV-TESTING-20
    #[test]
    fn an_unsupplied_executable_that_dispatches_the_worker_roles_is_its_own_worker() {
        let layout = worker_layout(true);
        assert_eq!(
            test_build_internal_worker_executable(None, true, &layout.harness).unwrap(),
            layout.harness
        );
    }

    // INV-TESTING-20
    #[test]
    fn a_supplied_worker_file_wins_and_a_supplied_non_file_is_refused() {
        let layout = worker_layout(true);
        for dispatches in [false, true] {
            assert_eq!(
                test_build_internal_worker_executable(
                    Some(layout.supplied.clone().into_os_string()),
                    dispatches,
                    &layout.harness,
                )
                .unwrap(),
                layout.supplied
            );
            let error = test_build_internal_worker_executable(
                Some(layout.example.with_file_name("absent").into_os_string()),
                dispatches,
                &layout.harness,
            )
            .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::Other);
            assert_eq!(error.raw_os_error(), None);
        }
    }

    #[cfg(unix)]
    #[test]
    fn timed_out_command_terminates_pipe_holding_descendants() {
        let runner = SystemCommandRunner;
        let temporary = tempfile::tempdir().unwrap();
        let blocker = temporary.path().join("blocker");
        mkfifo(&blocker, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();

        let result = runner.run(CommandRequest {
            program: Path::new("/bin/sh"),
            args: &[
                "-c",
                "(IFS= read -r unexpected < \"$BLOCKER\") & IFS= read -r unexpected < \"$BLOCKER\"",
            ],
            timeout: Duration::from_millis(50),
            maximum_stdout_bytes: FIXTURE_STDOUT_LIMIT,
            clear_environment: false,
            environment: &[(OsStr::new("BLOCKER"), blocker.as_os_str())],
            current_directory: None,
        });

        assert_eq!(result, Err(CommandProbeError::Timeout));
    }

    #[cfg(unix)]
    #[test]
    fn excessive_standard_output_is_drained_and_truncated_at_the_callers_limit() {
        let runner = SystemCommandRunner;
        let output = runner
            .run(CommandRequest {
                program: Path::new("/bin/sh"),
                args: &[
                    "-c",
                    "i=0; while [ \"$i\" -le 8192 ]; do printf x; i=$((i + 1)); done",
                ],
                timeout: Duration::from_secs(1),
                maximum_stdout_bytes: FIXTURE_STDOUT_LIMIT,
                clear_environment: false,
                environment: &[],
                current_directory: None,
            })
            .unwrap();

        assert!(output.success);
        assert_eq!(output.stdout.len(), FIXTURE_STDOUT_LIMIT);
        assert!(output.truncated);
    }
}
