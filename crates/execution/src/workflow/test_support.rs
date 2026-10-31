use super::admission::{AdmittedWorkflow, ExecutionContext, ResolvedInputs, admit_workflow};
use super::resolution;
use super::runtime::{
    self, ConditionOutput, ExecutionSeed, ExecutionStart, InitialCancellation, Reduction,
    RuntimeDefinition,
};
use std::fmt;
use std::future::Future;
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
                identity: None,
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

/// The ceiling on the factor `host_scaled_watchdog` applies to a case's idle-host base.
pub(crate) const MAX_WATCHDOG_SCALE: u32 = 4;

/// One reading of the host's one-minute load average and the cores it is shared across.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HostLoad {
    pub(crate) one_minute: f64,
    pub(crate) cores: usize,
}

/// Reads `/proc/loadavg` on Linux and `sysctl -n vm.loadavg` elsewhere; `None` when the
/// load is unreadable.
pub(crate) fn host_load() -> Option<HostLoad> {
    let reading = if cfg!(target_os = "linux") {
        fs::read_to_string("/proc/loadavg").ok()?
    } else {
        let output = Command::new("sysctl")
            .args(["-n", "vm.loadavg"])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        String::from_utf8(output.stdout).ok()?
    };
    let one_minute = reading
        .split_whitespace()
        .find(|field| *field != "{")?
        .parse::<f64>()
        .ok()
        .filter(|load| load.is_finite() && *load >= 0.0)?;
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    Some(HostLoad { one_minute, cores })
}

/// `⌈one-minute load ÷ cores⌉` clamped to `1..=MAX_WATCHDOG_SCALE`; 1 with no reading.
pub(crate) fn watchdog_scale(load: Option<&HostLoad>) -> u32 {
    let Some(load) = load else {
        return 1;
    };
    let cores = f64::from(u32::try_from(load.cores.max(1)).unwrap_or(u32::MAX));
    let per_core = load.one_minute / cores;
    (1..MAX_WATCHDOG_SCALE)
        .find(|factor| per_core <= f64::from(*factor))
        .unwrap_or(MAX_WATCHDOG_SCALE)
}

/// The `repo-check` steps a schedule shows started and not finished, other than
/// `own_step`, in start order.
pub(crate) fn repo_check_concurrent_steps(schedule: &str, own_step: Option<&str>) -> Vec<String> {
    let mut running = Vec::<&str>::new();
    for line in schedule.lines() {
        let mut fields = line.split('\t');
        match (fields.next(), fields.next()) {
            (Some("started"), Some(key)) if !running.contains(&key) => running.push(key),
            (Some("finished"), Some(key)) => running.retain(|started| *started != key),
            _ => {}
        }
    }
    running
        .into_iter()
        .filter(|key| Some(*key) != own_step)
        .map(str::to_owned)
        .collect()
}

/// What an expired `host_scaled_watchdog` knows: the case, how long it ran, its bound as
/// base and factor, the load reading, the `repo-check` schedule and own step, and the
/// suite's own `diagnosis` of where the case stood, when it has one.
pub(crate) struct WatchdogExpiry<'a> {
    pub(crate) case: &'a str,
    pub(crate) elapsed: Duration,
    pub(crate) base: Duration,
    pub(crate) scale: u32,
    pub(crate) load: Option<&'a HostLoad>,
    pub(crate) schedule: Option<&'a str>,
    pub(crate) own_step: Option<&'a str>,
    pub(crate) diagnosis: Option<&'a str>,
}

/// The one-line panic message of an expired `host_scaled_watchdog`.
pub(crate) fn watchdog_expiry_message(expiry: &WatchdogExpiry<'_>) -> String {
    let WatchdogExpiry {
        case,
        elapsed,
        base,
        scale,
        load,
        schedule,
        own_step,
        diagnosis,
    } = *expiry;
    let reading = load.map_or_else(
        || "unknown".to_owned(),
        |load| format!("{:.2} over {} cores", load.one_minute, load.cores),
    );
    let concurrent = schedule.map_or_else(
        || "not under repo-check".to_owned(),
        |schedule| {
            let steps = repo_check_concurrent_steps(schedule, own_step);
            if steps.is_empty() {
                "none".to_owned()
            } else {
                steps.join(", ")
            }
        },
    );
    let diagnosis = diagnosis.map_or_else(String::new, |diagnosis| format!("; {diagnosis}"));
    format!(
        "{case} watchdog expired after {elapsed:.1?}: bound {:?} is {base:?} x {scale} for host load {reading}; repo-check also running: {concurrent}{diagnosis}",
        base * scale
    )
}

/// Bounds a harness test case against hangs: the case's idle-host `base` multiplied by
/// the host-load factor, re-evaluated when the bound expires, never more than
/// `MAX_WATCHDOG_SCALE` times the base. Expiry panics with `watchdog_expiry_message`.
///
/// A plain function, not an `async fn`, as are the `with_watchdog` wrappers the suites put in
/// front of it. An `async fn` keeps its `future` argument in its own state, and an unoptimized
/// build keeps every move of that state in the frame that moves it, so a case body that
/// embedded two adapter `invoke` futures (about 100 KiB unoptimized) cost about 2 MiB across
/// the `#[tokio::test]` wrapper and the test's own async frame and overflowed libtest's
/// default thread stack before its fixture ran. Pinning the body on the heap before any
/// wrapper's state exists leaves those frames holding a pointer.
pub(crate) fn host_scaled_watchdog<Output>(
    case: &str,
    base: Duration,
    future: impl Future<Output = Output>,
) -> impl Future<Output = Output> {
    host_scaled_watchdog_with_diagnosis(case, base, || None, future)
}

/// `host_scaled_watchdog` whose expiry message ends with the suite's own `diagnosis`,
/// taken at the moment of expiry (the last transition a scenario observed, for one).
#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
pub(crate) fn host_scaled_watchdog_with_diagnosis<Output>(
    case: &str,
    base: Duration,
    diagnosis: impl FnOnce() -> Option<String>,
    future: impl Future<Output = Output>,
) -> impl Future<Output = Output> {
    // Heap-pinned before the returned future exists (see `host_scaled_watchdog`), so neither
    // this loop's state nor any wrapper's holds the body.
    let mut future = Box::pin(future);
    async move {
        let started = std::time::Instant::now();
        let mut scale = watchdog_scale(host_load().as_ref());
        loop {
            let bound = base * scale;
            if let Ok(output) =
                tokio::time::timeout(bound.saturating_sub(started.elapsed()), future.as_mut()).await
            {
                return output;
            }
            let load = host_load();
            let rescaled = watchdog_scale(load.as_ref());
            if rescaled > scale {
                scale = rescaled;
                continue;
            }
            let schedule = std::env::var_os("UM_REPO_CHECK_SCHEDULE")
                .and_then(|path| fs::read_to_string(path).ok());
            let own_step =
                std::env::var_os("UM_REPO_CHECK_STEP").and_then(|step| step.into_string().ok());
            let diagnosis = diagnosis();
            panic!(
                "{}",
                watchdog_expiry_message(&WatchdogExpiry {
                    case,
                    elapsed: started.elapsed(),
                    base,
                    scale,
                    load: load.as_ref(),
                    schedule: schedule.as_deref(),
                    own_step: own_step.as_deref(),
                    diagnosis: diagnosis.as_deref(),
                })
            );
        }
    }
}

// INV-TESTING-18
mod host_scaled_watchdog_tests {
    use super::{
        HostLoad, MAX_WATCHDOG_SCALE, WatchdogExpiry, host_scaled_watchdog,
        host_scaled_watchdog_with_diagnosis, repo_check_concurrent_steps, watchdog_expiry_message,
        watchdog_scale,
    };
    use std::time::Duration;

    const OWN_STEP: &str = "codex-app-server-v1-conformance-x86_64-linux";
    const SCHEDULE: &str = "started\tcli\nstarted\tcodex-app-server-v1-conformance-x86_64-linux\nstarted\tgateway\nfinished\tgateway\t0\n";

    fn load(one_minute: f64, cores: usize) -> HostLoad {
        HostLoad { one_minute, cores }
    }

    #[test]
    fn watchdog_scale_is_the_rounded_up_load_per_core_within_the_ceiling() {
        assert_eq!(watchdog_scale(None), 1);
        assert_eq!(watchdog_scale(Some(&load(3.0, 4))), 1);
        assert_eq!(watchdog_scale(Some(&load(8.0, 4))), 2);
        assert_eq!(watchdog_scale(Some(&load(9.5, 4))), 3);
        assert_eq!(watchdog_scale(Some(&load(37.0, 4))), MAX_WATCHDOG_SCALE);
        assert_eq!(MAX_WATCHDOG_SCALE, 4);
    }

    #[test]
    fn repo_check_concurrent_steps_are_the_unfinished_other_steps_in_start_order() {
        assert_eq!(
            repo_check_concurrent_steps(SCHEDULE, Some(OWN_STEP)),
            vec!["cli".to_owned()]
        );
    }

    #[test]
    fn watchdog_expiry_message_names_the_bound_load_and_concurrent_steps() {
        let message = watchdog_expiry_message(&WatchdogExpiry {
            case: "probe case",
            elapsed: Duration::from_millis(30_250),
            base: Duration::from_secs(10),
            scale: 3,
            load: Some(&load(9.5, 4)),
            schedule: Some(SCHEDULE),
            own_step: Some(OWN_STEP),
            diagnosis: Some("last observed transition: step plan Running -> Succeeded"),
        });
        for part in [
            "probe case watchdog expired after 30.2",
            "bound 30s is 10s x 3",
            "host load 9.50 over 4 cores",
            "repo-check also running: cli",
            "; last observed transition: step plan Running -> Succeeded",
        ] {
            assert!(message.contains(part), "{part:?} missing from {message:?}");
        }
        assert!(!message.contains('\n'), "{message:?}");

        let unscheduled = watchdog_expiry_message(&WatchdogExpiry {
            case: "probe case",
            elapsed: Duration::from_secs(10),
            base: Duration::from_secs(10),
            scale: 1,
            load: None,
            schedule: None,
            own_step: None,
            diagnosis: None,
        });
        assert!(
            unscheduled.ends_with("not under repo-check"),
            "{unscheduled:?}"
        );
        for part in [
            "host load unknown",
            "repo-check also running: not under repo-check",
        ] {
            assert!(
                unscheduled.contains(part),
                "{part:?} missing from {unscheduled:?}"
            );
        }
    }

    #[tokio::test]
    async fn host_scaled_watchdog_expiry_panics_with_the_expiry_message() {
        let expired = tokio::spawn(host_scaled_watchdog(
            "probe",
            Duration::from_millis(1),
            std::future::pending::<()>(),
        ))
        .await
        .unwrap_err();
        let message = expired.into_panic().downcast::<String>().unwrap();
        assert!(
            message.starts_with("probe watchdog expired after"),
            "{message:?}"
        );
    }

    #[tokio::test]
    async fn host_scaled_watchdog_expiry_carries_the_diagnosis_taken_at_expiry() {
        let expired = tokio::spawn(host_scaled_watchdog_with_diagnosis(
            "probe",
            Duration::from_millis(1),
            || Some("last observed transition: none".to_owned()),
            std::future::pending::<()>(),
        ))
        .await
        .unwrap_err();
        let message = expired.into_panic().downcast::<String>().unwrap();
        assert!(
            message.starts_with("probe watchdog expired after")
                && message.ends_with("; last observed transition: none"),
            "{message:?}"
        );
    }
}
