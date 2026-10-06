use super::admission::{AdmittedWorkflow, ExecutionContext, ResolvedInputs, admit_workflow};
use super::resolution;
use super::runtime::{
    self, ConditionOutput, ExecutionSeed, ExecutionStart, InitialCancellation, Reduction,
    RuntimeDefinition,
};
use std::fmt;
use std::path::Path;

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Arc, Barrier};
use std::time::Duration;

pub(super) fn write_process_fixture_signal(variable: &str, value: &[u8]) {
    fs::write(std::env::var_os(variable).unwrap(), value).unwrap();
}

pub(super) fn write_process_fixture_id(variable: &str) {
    let process = format!("{}\n", std::process::id());
    write_process_fixture_signal(variable, process.as_bytes());
}

pub(super) fn process_fixture_interrupt_receiver() -> std::sync::mpsc::Receiver<()> {
    let (interrupt, interrupted) = std::sync::mpsc::sync_channel(1);
    process_fixture_interrupt_handler(move || {
        let _ = interrupt.try_send(());
    });
    interrupted
}

pub(super) fn process_fixture_interrupt_handler(handler: impl FnOnce() + Send + 'static) {
    let (ready, registered) = std::sync::mpsc::sync_channel(0);
    let _ = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let mut interrupt =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
            ready.send(()).unwrap();
            let _ = interrupt.recv().await;
            handler();
        });
    });
    registered.recv().unwrap();
}

pub(super) fn process_fixture_output(descriptor: u8) -> fs::File {
    fs::OpenOptions::new()
        .write(true)
        .open(format!("/dev/fd/{descriptor}"))
        .unwrap()
}

pub(super) fn spawn_process_fixture(test: &str) -> std::thread::JoinHandle<()> {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--ignored", "--test-threads=1"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    std::thread::spawn(move || {
        let _ = child.wait();
    })
}

pub(super) async fn run_with_stalled_child_guard(
    test: &str,
    worker_pid_variable: &str,
) -> ExitStatus {
    let current_executable = std::env::current_exe().unwrap();
    let isolated = tempfile::tempdir().unwrap();
    let pid_path = isolated.path().join("stalled-worker.pid");
    let guard_executable = isolated
        .path()
        .join(format!("um{}", std::env::consts::EXE_SUFFIX));
    fs::write(
        &guard_executable,
        "#!/bin/sh\nroot=${0%/*}\nprintf '%s\\n' \"$$\" > \"$root/stalled-worker.pid\"\nIFS= read -r _\n",
    )
    .unwrap();
    fs::set_permissions(&guard_executable, fs::Permissions::from_mode(0o755)).unwrap();

    tokio::process::Command::new(current_executable)
        .args(["--exact", test, "--ignored"])
        .env(worker_pid_variable, pid_path)
        .env("UM_TEST_INTERNAL_WORKER_EXECUTABLE", guard_executable)
        .kill_on_drop(true)
        .status()
        .await
        .unwrap()
}

pub(super) async fn wait_for_stalled_child_guard(worker_pid_variable: &str) -> i32 {
    let pid_path = std::path::PathBuf::from(std::env::var_os(worker_pid_variable).unwrap());
    tokio::task::spawn_blocking(move || {
        let started = um_support::monotonic_now();
        loop {
            if let Ok(pid) = fs::read_to_string(&pid_path)
                && let Ok(pid) = pid.trim().parse::<i32>()
            {
                return pid;
            }
            assert!(um_support::elapsed(started) < Duration::from_secs(5));
            um_support::sleep(Duration::from_millis(10));
        }
    })
    .await
    .unwrap()
}

#[derive(Clone)]
pub(super) struct SynchronousGate {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl SynchronousGate {
    pub(super) fn new() -> Self {
        Self {
            reached: Arc::new(Barrier::new(2)),
            resume: Arc::new(Barrier::new(2)),
        }
    }

    pub(super) fn wait_until_reached(&self) {
        self.reached.wait();
    }

    pub(super) fn resume(&self) {
        self.resume.wait();
    }

    pub(super) fn block_until_resumed(&self) {
        self.reached.wait();
        self.resume.wait();
    }
}

impl fmt::Debug for SynchronousGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SynchronousGate")
            .finish_non_exhaustive()
    }
}

/// Builds reducer state through the production initializer; tests customize the
/// definition and seed, never individual RuntimeState fields.
pub(crate) struct ReductionBuilder<Output, Deadline> {
    definition: RuntimeDefinition,
    seed: ExecutionSeed<Output>,
    initial_cancellation: Option<InitialCancellation<Deadline>>,
}

impl<Output, Deadline> ReductionBuilder<Output, Deadline> {
    pub(crate) fn new(definition: RuntimeDefinition) -> Self {
        Self {
            definition,
            seed: ExecutionSeed::empty(),
            initial_cancellation: None,
        }
    }

    pub(crate) fn seeded(mut self, seed: ExecutionSeed<Output>) -> Self {
        self.seed = seed;
        self
    }

    pub(crate) fn initially_cancelled(
        mut self,
        cancellation: InitialCancellation<Deadline>,
    ) -> Self {
        self.initial_cancellation = Some(cancellation);
        self
    }

    pub(crate) fn build<Provisional, Cause>(self) -> Reduction<Provisional, Cause, Output, Deadline>
    where
        Cause: Clone,
        Output: Clone + ConditionOutput,
        Deadline: Clone,
    {
        runtime::initialize_seeded_definition(
            ExecutionStart {
                definition: self.definition,
                initial_cancellation: self.initial_cancellation,
            },
            self.seed,
        )
    }
}

/// Owns the source-writing and resolution boundary of an admitted test workflow.
/// The caller owns the temporary directory and execution policy for its scenario.
pub(crate) struct AdmittedFixture<'a> {
    source_root: &'a Path,
    source: &'a str,
    files: &'a [(&'a str, &'a [u8])],
}

impl<'a> AdmittedFixture<'a> {
    pub(crate) fn new(source_root: &'a Path, source: &'a str) -> Self {
        Self {
            source_root,
            source,
            files: &[],
        }
    }

    pub(crate) fn with_files(mut self, files: &'a [(&'a str, &'a [u8])]) -> Self {
        self.files = files;
        self
    }

    pub(crate) fn admit(
        self,
        imports: ResolvedInputs,
        context: ExecutionContext,
    ) -> AdmittedWorkflow {
        fs::create_dir_all(self.source_root).unwrap();
        fs::write(self.source_root.join("workflow.yaml"), self.source).unwrap();
        for (path, bytes) in self.files {
            let path = self.source_root.join(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, bytes).unwrap();
        }
        admit_workflow(
            resolution::resolve(self.source_root, Path::new("workflow.yaml")).unwrap(),
            imports,
            context,
        )
        .unwrap()
    }
}

// All clock, observation and validation fixtures live under this shared module.
mod fixtures;
pub(crate) use fixtures::{
    ChannelObservationSink, ControlledClock, InlineValidationWorker, PendingClock,
    RecordingObservationSink,
};
pub(crate) use fixtures::{
    blocking_validation, claude_validation, codex_clock, pi_blocking_validation, pi_clock, pi_sink,
    pi_validation, step_clock, validation_fixtures,
};
