//! The serving runner's disk monitor (INV-RUNNER-49): it reads the work root's filesystem
//! when the service starts and again `DISK_READ_INTERVAL` after each read ends, each read on
//! a thread of its own, and publishes the reading in a handle every reader copies from, the
//! start gate of a single-workflow assignment's engine among them (REQ-RUNNERENROLL-155).
//! Where a reading changes whether the disk is low (REQ-RUNNERENROLL-152) it writes one
//! `runner.disk_space` record (REQ-TELEMETRY-55) and records the runner's event
//! (REQ-RUNNERENROLL-138).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use opentelemetry::KeyValue;
use time::OffsetDateTime;
use useful_execution::{DiskSpace, StartGateReading, StepStartGate, read_disk_space};

use super::Sleeper;
use super::history::{EventLog, LoggedEvent};
use crate::telemetry::{self, Recorder};

/// How long the monitor waits after a read ends before it reads again.
const DISK_READ_INTERVAL: Duration = Duration::from_secs(30);

/// The name of the thread each read runs on.
const DISK_READ_THREAD: &str = "useful-disk-read";

/// What reads a filesystem's space: the system in production, a script in tests.
pub(super) trait DiskReader: Send + Sync {
    fn read(&self, path: &Path) -> io::Result<DiskSpace>;
}

/// Reads the filesystem through `statvfs`.
pub(super) struct SystemDiskReader;

impl DiskReader for SystemDiskReader {
    fn read(&self, path: &Path) -> io::Result<DiskSpace> {
        read_disk_space(path)
    }
}

/// One reading of the work root's disk as the monitor judged it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DiskReading {
    /// The path read: the work root.
    pub(super) path: PathBuf,
    pub(super) available_bytes: u64,
    pub(super) total_bytes: u64,
    pub(super) minimum_free_bytes: u64,
    /// Whether `available_bytes` is below `minimum_free_bytes` (`DiskSpace::is_low`).
    pub(super) low: bool,
    /// When the read ended, by the monitor's sleeper's clock.
    pub(super) read_at: OffsetDateTime,
}

/// The newest reading the monitor published, none before its first read succeeds. Its lock
/// is taken to copy the reading or to swap it, and for nothing else: nothing is read,
/// awaited or locked inside it (INV-RUNNER-49).
#[derive(Clone, Debug, Default)]
pub(super) struct DiskReadingHandle {
    reading: Arc<Mutex<Option<DiskReading>>>,
}

impl DiskReadingHandle {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// A handle already holding `reading`, as though the monitor had published it.
    #[cfg(test)]
    pub(super) fn holding(reading: DiskReading) -> Self {
        let handle = Self::new();
        handle.replace(reading);
        handle
    }

    /// Publishes `reading` in this handle, as the monitor would.
    #[cfg(test)]
    pub(super) fn publish(&self, reading: DiskReading) {
        self.replace(reading);
    }

    /// A copy of the newest published reading.
    pub(super) fn published(&self) -> Option<DiskReading> {
        self.lock().clone()
    }

    /// Publishes `reading`, answering the one it replaces.
    fn replace(&self, reading: DiskReading) -> Option<DiskReading> {
        self.lock().replace(reading)
    }

    fn lock(&self) -> MutexGuard<'_, Option<DiskReading>> {
        self.reading.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The start gate a single-workflow assignment's engine consults (REQ-RUNNERENROLL-155,
/// INV-ENGINE-46): it copies the published reading and reads no file, holding a step only
/// while that reading is low. A handle holding no reading never holds one.
impl StepStartGate for DiskReadingHandle {
    fn read(&self) -> StartGateReading {
        match self.published() {
            Some(reading) if reading.low => StartGateReading::DiskLow {
                available_bytes: reading.available_bytes,
                minimum_free_bytes: reading.minimum_free_bytes,
            },
            _ => StartGateReading::Open,
        }
    }
}

/// What the monitor reads with and sleeps on; a service without it starts no monitor.
pub(super) struct DiskMonitorDependencies {
    pub(super) reader: Arc<dyn DiskReader>,
    pub(super) sleeper: Arc<dyn Sleeper>,
}

/// The running monitor; dropping it stops it. A read in flight then finishes on its own
/// thread and its answer is dropped.
pub(super) struct DiskMonitor {
    task: tokio::task::JoinHandle<()>,
}

impl DiskMonitor {
    /// Starts reading `path`'s disk against `minimum_free_bytes`, publishing in `handle`
    /// and recording each change of state in `events` and through `recorder`.
    pub(super) fn start(
        dependencies: DiskMonitorDependencies,
        handle: DiskReadingHandle,
        path: PathBuf,
        minimum_free_bytes: u64,
        events: Arc<EventLog>,
        recorder: Arc<Recorder>,
    ) -> Self {
        let monitor = Monitor {
            reader: dependencies.reader,
            sleeper: dependencies.sleeper,
            handle,
            path,
            minimum_free_bytes,
            events,
            recorder,
        };
        Self {
            task: tokio::spawn(monitor.run()),
        }
    }
}

impl Drop for DiskMonitor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Monitor {
    reader: Arc<dyn DiskReader>,
    sleeper: Arc<dyn Sleeper>,
    handle: DiskReadingHandle,
    path: PathBuf,
    minimum_free_bytes: u64,
    events: Arc<EventLog>,
    recorder: Arc<Recorder>,
}

impl Monitor {
    /// Reads, publishes what it read, sleeps and reads again, until it is aborted. One read
    /// is in flight at a time, since the loop awaits it before it sleeps.
    async fn run(self) {
        loop {
            if let Some(space) = read_on_own_thread(&self.reader, &self.path).await {
                self.publish(space);
            }
            self.sleeper.sleep(DISK_READ_INTERVAL).await;
        }
    }

    /// Publishes `space` as the newest reading and, after the handle's lock is released,
    /// records the change of state it makes, if any.
    fn publish(&self, space: DiskSpace) {
        let reading = DiskReading {
            path: self.path.clone(),
            available_bytes: space.available_bytes,
            total_bytes: space.total_bytes,
            minimum_free_bytes: self.minimum_free_bytes,
            low: space.is_low(self.minimum_free_bytes),
            read_at: self.sleeper.utc_now(),
        };
        let was_low = self
            .handle
            .replace(reading.clone())
            .is_some_and(|previous| previous.low);
        match (was_low, reading.low) {
            (false, true) => {
                self.record(&reading, "low");
                self.events.record_at(
                    reading.read_at,
                    LoggedEvent::DiskSpaceLow {
                        available_bytes: reading.available_bytes,
                        minimum_free_bytes: reading.minimum_free_bytes,
                    },
                );
            }
            (true, false) => {
                self.record(&reading, "ok");
                self.events.record_at(
                    reading.read_at,
                    LoggedEvent::DiskSpaceRecovered {
                        available_bytes: reading.available_bytes,
                    },
                );
            }
            (false, false) | (true, true) => {}
        }
    }

    fn record(&self, reading: &DiskReading, state: &'static str) {
        self.recorder.record(
            "runner.disk_space",
            [
                KeyValue::new(telemetry::attribute::DISK_STATE, state),
                KeyValue::new(
                    telemetry::attribute::DISK_AVAILABLE_BYTES,
                    telemetry::integer(reading.available_bytes),
                ),
                KeyValue::new(
                    telemetry::attribute::DISK_TOTAL_BYTES,
                    telemetry::integer(reading.total_bytes),
                ),
                KeyValue::new(
                    telemetry::attribute::DISK_MINIMUM_FREE_BYTES,
                    telemetry::integer(reading.minimum_free_bytes),
                ),
            ],
        );
    }
}

/// Reads `path` on a named thread of its own, answering through a one-shot channel, so a
/// read that never returns holds neither the runtime nor its blocking pool, which the
/// service's end waits for. A thread that cannot start, an answer that never comes and a
/// failed read are each no reading.
async fn read_on_own_thread(reader: &Arc<dyn DiskReader>, path: &Path) -> Option<DiskSpace> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let reader = Arc::clone(reader);
    let path = path.to_owned();
    std::thread::Builder::new()
        .name(DISK_READ_THREAD.to_owned())
        .spawn(move || {
            let _ = sender.send(reader.read(&path));
        })
        .ok()?;
    receiver.await.ok()?.ok()
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::{self, UnboundedReceiver};

    use super::*;
    use crate::control_protocol::{OpenWord, OverviewEvent, RunnerEventKind};
    use crate::service::{SleepRelease, controlled_sleeper, with_watchdog};
    use crate::telemetry::{TestCapture, test_recorder};

    const GIB: u64 = 1024 * 1024 * 1024;
    const MINIMUM: u64 = 5 * GIB;
    const TOTAL: u64 = 100 * GIB;

    /// A reader the test answers: each read tells the test it was called, with the path,
    /// and waits on its thread until the test sends the answer.
    struct AnsweredReader {
        calls: mpsc::UnboundedSender<PathBuf>,
        answers: Mutex<std::sync::mpsc::Receiver<io::Result<DiskSpace>>>,
    }

    impl DiskReader for AnsweredReader {
        fn read(&self, path: &Path) -> io::Result<DiskSpace> {
            let answers = self.answers.lock().unwrap();
            let _ = self.calls.send(path.to_owned());
            answers
                .recv()
                .unwrap_or_else(|_| Err(io::Error::other("the test ended")))
        }
    }

    struct Harness {
        _monitor: DiskMonitor,
        handle: DiskReadingHandle,
        calls: UnboundedReceiver<PathBuf>,
        answers: std::sync::mpsc::Sender<io::Result<DiskSpace>>,
        sleeps: UnboundedReceiver<(Duration, SleepRelease)>,
        events: Arc<EventLog>,
        capture: TestCapture,
        work_root: PathBuf,
        origin: OffsetDateTime,
    }

    impl Harness {
        fn start(minimum_free_bytes: u64) -> Self {
            let (calls_sender, calls) = mpsc::unbounded_channel();
            let (answers, answers_receiver) = std::sync::mpsc::channel();
            let (sleeper, sleeps) = controlled_sleeper();
            let origin = sleeper.utc_now();
            let (recorder, capture) = test_recorder("rbt_01k0z6r1w8f4jy2m7q9v3x5abc");
            let events = Arc::new(EventLog::new());
            let handle = DiskReadingHandle::new();
            let work_root = PathBuf::from("/var/lib/useful/runner/work");
            let monitor = DiskMonitor::start(
                DiskMonitorDependencies {
                    reader: Arc::new(AnsweredReader {
                        calls: calls_sender,
                        answers: Mutex::new(answers_receiver),
                    }),
                    sleeper,
                },
                handle.clone(),
                work_root.clone(),
                minimum_free_bytes,
                Arc::clone(&events),
                recorder,
            );
            Self {
                _monitor: monitor,
                handle,
                calls,
                answers,
                sleeps,
                events,
                capture,
                work_root,
                origin,
            }
        }

        /// Waits for the monitor to read, and asserts it read the work root.
        async fn read_called(&mut self) {
            let path = with_watchdog(self.calls.recv()).await.unwrap().unwrap();
            assert_eq!(path, self.work_root);
        }

        /// Answers the read in flight with `answer` and waits for the sleep the monitor
        /// then asks for, 30 seconds.
        async fn answer(&mut self, answer: io::Result<DiskSpace>) -> SleepRelease {
            self.answers.send(answer).unwrap();
            let (duration, release) = with_watchdog(self.sleeps.recv()).await.unwrap().unwrap();
            assert_eq!(duration, Duration::from_secs(30));
            release
        }

        /// Asserts the monitor has neither read again nor asked for another sleep.
        fn assert_waiting(&mut self) {
            assert!(self.calls.try_recv().is_err(), "the monitor read again");
            assert!(self.sleeps.try_recv().is_err(), "the monitor slept again");
        }

        /// The published reading, copied on a thread of its own and bounded, so a copy
        /// that waited on anything would fail the test rather than hang it.
        async fn published(&self) -> Option<DiskReading> {
            let handle = self.handle.clone();
            with_watchdog(tokio::task::spawn_blocking(move || handle.published()))
                .await
                .unwrap()
                .unwrap()
        }

        fn reading(&self, available_bytes: u64, minimum: u64, second: i64) -> DiskReading {
            DiskReading {
                path: self.work_root.clone(),
                available_bytes,
                total_bytes: TOTAL,
                minimum_free_bytes: minimum,
                low: available_bytes < minimum,
                read_at: self.origin + time::Duration::seconds(second),
            }
        }

        /// The `runner.disk_space` records written so far, as (state, available, total,
        /// minimum).
        fn records(&self) -> Vec<(String, u64, u64, u64)> {
            let number = |record: &serde_json::Map<String, serde_json::Value>, key: &str| {
                record[key]
                    .as_u64()
                    .unwrap_or_else(|| panic!("{key}: {record:?}"))
            };
            self.capture
                .records()
                .into_iter()
                .filter(|record| record["event.name"] == "runner.disk_space")
                .map(|record| {
                    (
                        record["um.disk.state"].as_str().unwrap().to_owned(),
                        number(&record, "um.disk.available_bytes"),
                        number(&record, "um.disk.total_bytes"),
                        number(&record, "um.disk.minimum_free_bytes"),
                    )
                })
                .collect()
        }

        /// The events recorded so far, newest first, as (kind, available, minimum).
        fn events(&self) -> Vec<(OpenWord<RunnerEventKind>, Option<u64>, Option<u64>)> {
            self.events
                .newest_first()
                .into_iter()
                .map(|event: OverviewEvent| {
                    assert_eq!(event.count, 1, "{event:?}");
                    (event.kind, event.available_bytes, event.minimum_free_bytes)
                })
                .collect()
        }
    }

    fn space(available_bytes: u64) -> io::Result<DiskSpace> {
        Ok(DiskSpace {
            available_bytes,
            total_bytes: TOTAL,
        })
    }

    // REQ-RUNNERENROLL-152, REQ-TELEMETRY-55, REQ-RUNNERENROLL-138: the monitor reads at
    // once and again only once each 30-second sleep is released; each reading is published,
    // a failed read keeps the one before it, and only the turns to low and back write a
    // record and an event.
    #[tokio::test]
    async fn the_monitor_publishes_each_reading_and_records_each_change_of_state() {
        let mut harness = Harness::start(MINIMUM);
        harness.read_called().await;
        assert!(
            harness.sleeps.try_recv().is_err(),
            "slept before the first read"
        );
        let mut release = harness.answer(space(10 * GIB)).await;
        assert_eq!(
            harness.published().await,
            Some(harness.reading(10 * GIB, MINIMUM, 0))
        );
        assert_eq!(harness.records(), []);
        assert_eq!(harness.events(), []);

        for (second, answer, published) in [
            (30, space(GIB), harness.reading(GIB, MINIMUM, 30)),
            (60, space(GIB), harness.reading(GIB, MINIMUM, 60)),
            (
                90,
                Err(io::Error::other("unreadable")),
                harness.reading(GIB, MINIMUM, 60),
            ),
            (120, space(8 * GIB), harness.reading(8 * GIB, MINIMUM, 120)),
        ] {
            harness.assert_waiting();
            release.release();
            harness.read_called().await;
            release = harness.answer(answer).await;
            assert_eq!(harness.published().await, Some(published), "{second}");
            if second == 30 {
                assert_eq!(harness.records(), [("low".to_owned(), GIB, TOTAL, MINIMUM)]);
            }
        }

        assert_eq!(
            harness.records(),
            [
                ("low".to_owned(), GIB, TOTAL, MINIMUM),
                ("ok".to_owned(), 8 * GIB, TOTAL, MINIMUM),
            ]
        );
        assert_eq!(
            harness.events(),
            [
                (
                    OpenWord::Known(RunnerEventKind::DiskSpaceRecovered),
                    Some(8 * GIB),
                    None
                ),
                (
                    OpenWord::Known(RunnerEventKind::DiskSpaceLow),
                    Some(GIB),
                    Some(MINIMUM)
                ),
            ]
        );
    }

    // REQ-RUNNERENROLL-152, REQ-TELEMETRY-55, REQ-RUNNERENROLL-138: a first reading below
    // the minimum writes the `low` record and event at once.
    #[tokio::test]
    async fn a_first_reading_below_the_minimum_is_recorded_at_once() {
        let mut harness = Harness::start(MINIMUM);
        harness.read_called().await;
        let _release = harness.answer(space(GIB)).await;
        assert_eq!(
            harness.published().await,
            Some(harness.reading(GIB, MINIMUM, 0))
        );
        assert_eq!(harness.records(), [("low".to_owned(), GIB, TOTAL, MINIMUM)]);
        assert_eq!(
            harness.events(),
            [(
                OpenWord::Known(RunnerEventKind::DiskSpaceLow),
                Some(GIB),
                Some(MINIMUM)
            )]
        );
    }

    // REQ-RUNNERENROLL-152: with a minimum of 0 a disk with nothing available is not low.
    #[tokio::test]
    async fn a_minimum_of_zero_is_never_low() {
        let mut harness = Harness::start(0);
        harness.read_called().await;
        let _release = harness.answer(space(0)).await;
        let published = harness.published().await.unwrap();
        assert!(!published.low, "{published:?}");
        assert_eq!(published, harness.reading(0, 0, 0));
        assert_eq!(harness.records(), []);
        assert_eq!(harness.events(), []);
    }

    // INV-RUNNER-49: while a read is in flight the published reading is the one before it,
    // copied at once, and the monitor neither reads again nor sleeps until the read returns.
    #[tokio::test]
    async fn a_read_in_flight_leaves_the_reading_before_it_published() {
        let mut harness = Harness::start(MINIMUM);
        harness.read_called().await;
        let release = harness.answer(space(10 * GIB)).await;
        release.release();
        harness.read_called().await;

        assert_eq!(
            harness.published().await,
            Some(harness.reading(10 * GIB, MINIMUM, 0))
        );
        harness.assert_waiting();

        let _release = harness.answer(space(9 * GIB)).await;
        assert_eq!(
            harness.published().await,
            Some(harness.reading(9 * GIB, MINIMUM, 30))
        );
        harness.assert_waiting();
    }

    // REQ-RUNNERENROLL-155: the handle a single-workflow assignment's engine consults as its
    // start gate answers from the published reading alone: low while the reading is low,
    // with its available bytes and minimum, and open when it is not low or none is held.
    #[test]
    fn the_handle_as_a_start_gate_holds_only_while_the_published_reading_is_low() {
        let reading = |available_bytes: u64| DiskReading {
            path: PathBuf::from("/var/lib/useful/runner/work"),
            available_bytes,
            total_bytes: TOTAL,
            minimum_free_bytes: MINIMUM,
            low: available_bytes < MINIMUM,
            read_at: OffsetDateTime::UNIX_EPOCH,
        };
        let handle = DiskReadingHandle::new();
        let gate: Arc<dyn StepStartGate> = Arc::new(handle.clone());
        assert_eq!(gate.read(), StartGateReading::Open);

        handle.publish(reading(GIB));
        assert_eq!(
            gate.read(),
            StartGateReading::DiskLow {
                available_bytes: GIB,
                minimum_free_bytes: MINIMUM,
            }
        );

        handle.publish(reading(10 * GIB));
        assert_eq!(gate.read(), StartGateReading::Open);
    }
}
