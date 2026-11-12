//! The host-scaled watchdog that bounds a test's real-time waits (INV-TESTING-18): a case's
//! idle-host base multiplied by the host's load factor, re-evaluated when the bound expires.

use std::fmt;
use std::fs;
use std::future::Future;
use std::process::{Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

/// The ceiling on the factor `host_scaled_watchdog` applies to a case's idle-host base.
pub const MAX_WATCHDOG_SCALE: u32 = 4;

/// One reading of the host's one-minute load average and the cores it is shared across.
#[derive(Clone, Debug, PartialEq)]
pub struct HostLoad {
    pub one_minute: f64,
    pub cores: usize,
}

/// Reads `/proc/loadavg` on Linux and `sysctl -n vm.loadavg` elsewhere; `None` when the
/// load is unreadable.
pub fn host_load() -> Option<HostLoad> {
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
pub fn watchdog_scale(load: Option<&HostLoad>) -> u32 {
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
fn repo_check_concurrent_steps(schedule: &str, own_step: Option<&str>) -> Vec<String> {
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
struct WatchdogExpiry<'a> {
    case: &'a str,
    elapsed: Duration,
    base: Duration,
    scale: u32,
    load: Option<&'a HostLoad>,
    schedule: Option<&'a str>,
    own_step: Option<&'a str>,
    diagnosis: Option<&'a str>,
}

/// The one-line panic message of an expired `host_scaled_watchdog`.
fn watchdog_expiry_message(expiry: &WatchdogExpiry<'_>) -> String {
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

/// The idle-host base of a fixture wait: one wait at a fixture's process, socket or channel
/// boundary on an idle host, which `host_scaled_recv` and `host_scaled_duration` scale by
/// the host's load.
pub const FIXTURE_WAIT_BASE: Duration = Duration::from_secs(30);

/// An expired host-scaled bound. Its `Display` and `Debug` are both the one-line
/// `watchdog_expiry_message`, so a call site's `expect` prints the diagnosis bare.
pub struct WatchdogExpired {
    message: String,
}

impl fmt::Display for WatchdogExpired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl fmt::Debug for WatchdogExpired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for WatchdogExpired {}

/// Why `host_scaled_recv` returned no value. `Debug` is the `Display` text, as for
/// `WatchdogExpired`.
pub enum WatchdogRecvError {
    /// The sender stayed connected and silent past the host-scaled bound.
    Expired(WatchdogExpired),
    /// Every sender was dropped before a value arrived; reported at once, not at the bound.
    Disconnected { case: String },
}

impl fmt::Display for WatchdogRecvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expired(expired) => fmt::Display::fmt(expired, formatter),
            Self::Disconnected { case } => {
                write!(
                    formatter,
                    "{case} sender disconnected before a value arrived"
                )
            }
        }
    }
}

impl fmt::Debug for WatchdogRecvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for WatchdogRecvError {}

/// One host-scaled bound in progress: started at `started`, currently `base × scale`, with
/// the load reading that last set or confirmed the factor.
struct HostScaledBound {
    base: Duration,
    started: Instant,
    scale: u32,
    load: Option<HostLoad>,
}

impl HostScaledBound {
    #[expect(
        clippy::disallowed_methods,
        reason = "real time is used only as an anti-hang watchdog, never as success evidence"
    )]
    fn start(base: Duration) -> Self {
        let load = host_load();
        Self {
            base,
            started: Instant::now(),
            scale: watchdog_scale(load.as_ref()),
            load,
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "real time is used only as an anti-hang watchdog, never as success evidence"
    )]
    fn remaining(&self) -> Duration {
        (self.base * self.scale).saturating_sub(self.started.elapsed())
    }

    /// Called when the bound ran out: reads the load again and extends the bound, measured
    /// from the start, only if the factor rose.
    fn extend(&mut self) -> bool {
        self.load = host_load();
        let rescaled = watchdog_scale(self.load.as_ref());
        let extended = rescaled > self.scale;
        if extended {
            self.scale = rescaled;
        }
        extended
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "real time is used only as an anti-hang watchdog, never as success evidence"
    )]
    fn expired(&self, case: &str, diagnosis: Option<&str>) -> WatchdogExpired {
        let schedule = std::env::var_os("UM_REPO_CHECK_SCHEDULE")
            .and_then(|path| fs::read_to_string(path).ok());
        let own_step =
            std::env::var_os("UM_REPO_CHECK_STEP").and_then(|step| step.into_string().ok());
        WatchdogExpired {
            message: watchdog_expiry_message(&WatchdogExpiry {
                case,
                elapsed: self.started.elapsed(),
                base: self.base,
                scale: self.scale,
                load: self.load.as_ref(),
                schedule: schedule.as_deref(),
                own_step: own_step.as_deref(),
                diagnosis,
            }),
        }
    }
}

/// `base` multiplied by the host-load factor read once, for a bound the kernel enforces
/// (a socket's read or write timeout).
pub fn host_scaled_duration(base: Duration) -> Duration {
    base * watchdog_scale(host_load().as_ref())
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
pub fn host_scaled_watchdog<Output>(
    case: impl Into<String>,
    base: Duration,
    future: impl Future<Output = Output>,
) -> impl Future<Output = Output> {
    host_scaled_watchdog_with_diagnosis(case, base, || None, future)
}

/// `host_scaled_watchdog` whose expiry message ends with the suite's own `diagnosis`,
/// taken at the moment of expiry (the last transition a scenario observed, for one).
#[allow(
    clippy::panic,
    reason = "an expired watchdog fails the invoking test case with its diagnosis"
)]
pub fn host_scaled_watchdog_with_diagnosis<Output>(
    case: impl Into<String>,
    base: Duration,
    diagnosis: impl FnOnce() -> Option<String>,
    future: impl Future<Output = Output>,
) -> impl Future<Output = Output> {
    let bounded = host_scaled_bound(case.into(), base, diagnosis, future);
    async move {
        match bounded.await {
            Ok(output) => output,
            Err(expired) => panic!("{expired}"),
        }
    }
}

/// `host_scaled_watchdog` for a wait that reports its expiry rather than panicking: the
/// output, or the expiry as an error whose text is `watchdog_expiry_message`.
pub fn host_scaled_timeout<Output>(
    case: impl Into<String>,
    base: Duration,
    future: impl Future<Output = Output>,
) -> impl Future<Output = Result<Output, WatchdogExpired>> {
    host_scaled_bound(case.into(), base, || None, future)
}

/// The loop behind every async form. It owns `case`, so a caller can build the name and
/// hand over the returned future without lending it.
#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
fn host_scaled_bound<Output>(
    case: String,
    base: Duration,
    diagnosis: impl FnOnce() -> Option<String>,
    future: impl Future<Output = Output>,
) -> impl Future<Output = Result<Output, WatchdogExpired>> {
    // Heap-pinned before the returned future exists (see `host_scaled_watchdog`), so neither
    // this loop's state nor any wrapper's holds the body.
    let mut future = Box::pin(future);
    async move {
        let mut bound = HostScaledBound::start(base);
        loop {
            if let Ok(output) = tokio::time::timeout(bound.remaining(), future.as_mut()).await {
                return Ok(output);
            }
            if !bound.extend() {
                return Err(bound.expired(&case, diagnosis().as_deref()));
            }
        }
    }
}

/// A blocking channel receive under the host-scaled bound of `host_scaled_watchdog`: the
/// value, the expiry once `base` × the load factor has passed with the sender connected
/// and silent, or `Disconnected` as soon as every sender is gone.
#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
pub fn host_scaled_recv<T>(
    case: &str,
    base: Duration,
    receiver: &Receiver<T>,
) -> Result<T, WatchdogRecvError> {
    let mut bound = HostScaledBound::start(base);
    loop {
        match receiver.recv_timeout(bound.remaining()) {
            Ok(value) => return Ok(value),
            Err(RecvTimeoutError::Disconnected) => {
                return Err(WatchdogRecvError::Disconnected {
                    case: case.to_owned(),
                });
            }
            Err(RecvTimeoutError::Timeout) => {
                if !bound.extend() {
                    return Err(WatchdogRecvError::Expired(bound.expired(case, None)));
                }
            }
        }
    }
}

// INV-TESTING-18
#[cfg(test)]
#[allow(
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "watchdog unit tests use Rust assertions and panic shortcuts"
)]
mod host_scaled_watchdog_tests {
    use super::{
        HostLoad, MAX_WATCHDOG_SCALE, WatchdogExpiry, WatchdogRecvError, host_scaled_recv,
        host_scaled_timeout, host_scaled_watchdog, host_scaled_watchdog_with_diagnosis,
        repo_check_concurrent_steps, watchdog_expiry_message, watchdog_scale,
    };
    use std::sync::mpsc;
    use std::time::Duration;

    const OWN_STEP: &str = "codex-app-server-v1-conformance-x86_64-linux";
    const SCHEDULE: &str = "started\tcli\nstarted\tcodex-app-server-v1-conformance-x86_64-linux\nstarted\tgateway\nfinished\tgateway\t0\n";
    const EXPIRY: &str = "probe watchdog expired after";
    // Long enough that a wait which ran out its bound would hang the test, not pass it.
    const UNREACHED: Duration = Duration::from_secs(3600);

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

    #[tokio::test]
    async fn host_scaled_timeout_returns_the_expiry_as_an_error_or_the_ready_output() {
        let expired = host_scaled_timeout(
            "probe",
            Duration::from_millis(1),
            std::future::pending::<()>(),
        )
        .await
        .unwrap_err();
        for text in [format!("{expired:?}"), expired.to_string()] {
            assert!(text.starts_with(EXPIRY), "{text:?}");
        }

        let ready =
            host_scaled_timeout("probe", Duration::from_millis(1), std::future::ready(7)).await;
        assert_eq!(ready.unwrap(), 7);
    }

    #[test]
    fn host_scaled_recv_expires_while_the_sender_is_silent() {
        let (_sender, receiver) = mpsc::channel::<u32>();
        let expired = host_scaled_recv("probe", Duration::from_millis(1), &receiver).unwrap_err();
        assert!(
            matches!(expired, WatchdogRecvError::Expired(_)),
            "{expired:?}"
        );
        for text in [format!("{expired:?}"), expired.to_string()] {
            assert!(text.starts_with(EXPIRY), "{text:?}");
        }
    }

    #[test]
    fn host_scaled_recv_gives_a_sent_value_and_a_disconnect_without_waiting_out_the_bound() {
        let (sender, receiver) = mpsc::channel();
        sender.send(7).unwrap();
        assert_eq!(host_scaled_recv("probe", UNREACHED, &receiver).unwrap(), 7);

        drop(sender);
        let disconnected = host_scaled_recv("probe", UNREACHED, &receiver).unwrap_err();
        assert!(
            matches!(disconnected, WatchdogRecvError::Disconnected { .. }),
            "{disconnected:?}"
        );
    }
}
