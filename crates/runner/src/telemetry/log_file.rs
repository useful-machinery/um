//! The runner's log file, `runner.log` in the `logs` directory beside its
//! state (REQ-TELEMETRY-52), and the rotated files `runner.log.<n>` it is
//! kept within its limits by (REQ-TELEMETRY-53). Only the log output's
//! thread opens, writes, renames or removes them (INV-TELEMETRY-18), so no
//! file operation here runs on the runner's executor. A log the runner
//! cannot use is reported once for each kind of failure and never stops it
//! (REQ-TELEMETRY-54).

use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, OpenOptions, Permissions};
use std::io;
use std::os::unix::fs::{FileExt as _, MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use opentelemetry::KeyValue;
use time::{Duration, OffsetDateTime};

use super::attribute::ERROR_TYPE;
use super::{FramedWriter, RecordClock, RecordSink, Recorder, io_error_class};
use crate::agent_login::directory::ensure_private_directory;

const FILE_NAME: &str = "runner.log";
const FILE_MODE: u32 = 0o600;

const DIAGNOSTIC_NAME: &str = "runner.telemetry_log";
const DIAGNOSTIC_CLASSIFICATION_FIELD: &str = "diagnostic.classification";

/// A kind of failure of the log, each reported at most once a process
/// (REQ-TELEMETRY-54).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LogFailure {
    Open,
    Write,
    Rotation,
    Retention,
}

impl LogFailure {
    const fn classification(self) -> &'static str {
        match self {
            Self::Open => "open_failed",
            Self::Write => "write_failed",
            Self::Rotation => "rotation_failed",
            Self::Retention => "retention_failed",
        }
    }

    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

/// Reports the log's failures as `runner.telemetry_log` records through the
/// recorder, so they reach standard error and, while it takes writes, the
/// log. Each kind is reported once a process, with only its classification
/// and the closed class of its I/O error, never the error's text or a path
/// (REQ-TELEMETRY-54, REQ-TELEMETRY-13). The recorder owns the log's queue,
/// whose thread holds this reporter, so the reporter holds the recorder
/// weakly; a failure reported before `attach` is held until then.
#[derive(Clone, Default)]
pub(super) struct LogReporter {
    state: Arc<Mutex<ReporterState>>,
}

#[derive(Default)]
struct ReporterState {
    recorder: Option<Weak<Recorder>>,
    reported: u8,
    held: Vec<(LogFailure, io::ErrorKind)>,
}

impl LogReporter {
    /// Reports `failure`, whose I/O error was of `kind`, unless a failure of
    /// its kind was reported before.
    pub(super) fn report(&self, failure: LogFailure, kind: io::ErrorKind) {
        let recorder = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let bit = failure.bit();
            if state.reported & bit != 0 {
                return;
            }
            state.reported |= bit;
            match &state.recorder {
                Some(recorder) => recorder.upgrade(),
                None => {
                    state.held.push((failure, kind));
                    return;
                }
            }
        };
        if let Some(recorder) = recorder {
            record_failure(&recorder, failure, kind);
        }
    }

    /// Makes `recorder` the one failures are reported through, reporting
    /// those held until now.
    pub(super) fn attach(&self, recorder: &Arc<Recorder>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.recorder = Some(Arc::downgrade(recorder));
        for (failure, kind) in state.held.drain(..) {
            record_failure(recorder, failure, kind);
        }
    }
}

fn record_failure(recorder: &Recorder, failure: LogFailure, kind: io::ErrorKind) {
    recorder.record(
        DIAGNOSTIC_NAME,
        [
            KeyValue::new(DIAGNOSTIC_CLASSIFICATION_FIELD, failure.classification()),
            KeyValue::new(ERROR_TYPE, io_error_class(kind)),
        ],
    );
}

/// The log output's sink: a record it fails to write is reported as
/// `write_failed` and answered failed, so the queue's thread releases the
/// record from the log alone (REQ-TELEMETRY-14).
pub(super) struct ReportedWrites<S> {
    sink: S,
    reporter: LogReporter,
}

impl<S> ReportedWrites<S> {
    pub(super) fn new(sink: S, reporter: LogReporter) -> Self {
        Self { sink, reporter }
    }
}

impl<S: RecordSink> RecordSink for ReportedWrites<S> {
    fn write_record(&mut self, record: &[u8]) -> io::Result<()> {
        self.sink.write_record(record).inspect_err(|error| {
            self.reporter.report(LogFailure::Write, error.kind());
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sink.flush()
    }
}

/// The step of opening the log that failed.
#[derive(Clone, Copy, Debug)]
pub(super) enum OpenStage {
    /// Making `logs` a private directory of the runner's user.
    Directory,
    /// Opening `runner.log` in it for append.
    File,
}

/// Why the log did not open: the stage that failed and its error.
#[derive(Debug)]
pub(super) struct OpenError {
    stage: OpenStage,
    source: io::Error,
}

impl OpenError {
    pub(super) fn kind(&self) -> io::ErrorKind {
        self.source.kind()
    }
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let stage = match self.stage {
            OpenStage::Directory => "making the runner log directory",
            OpenStage::File => "opening the runner log",
        };
        write!(formatter, "{stage}: {}", self.source)
    }
}

impl std::error::Error for OpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// The bounds a log is kept within (REQ-TELEMETRY-53).
#[derive(Clone, Copy, Debug)]
pub(super) struct LogLimits {
    pub(super) file_bytes: u64,
    pub(super) rotated_files: u64,
    pub(super) rotated_age: Duration,
}

/// The production bounds: 32 MiB a file, seven rotated files, each kept
/// while its last record is at most 30 days old.
pub(super) const RUNNER_LOG_LIMITS: LogLimits = LogLimits {
    file_bytes: 32 * 1024 * 1024,
    rotated_files: 7,
    rotated_age: Duration::days(30),
};

/// How long, at most, records are written between two applications of the
/// limits; a connected runner writes a record about twice a minute, so the
/// limits need no timer of their own.
const LIMITS_INTERVAL: Duration = Duration::hours(1);

/// The open `runner.log`, appended to one record at a time with one
/// `write_all` on the bare file: no user-space buffer and no `fsync`. It is
/// rotated before a record that would take it past the file limit, and the
/// rotated files are kept within the count and age limits (REQ-TELEMETRY-53).
pub(super) struct LogFile {
    directory: PathBuf,
    limits: LogLimits,
    clock: RecordClock,
    output: FramedWriter<File>,
    // The bytes in the file `output` writes: its length at open and what
    // was written since.
    size: u64,
    limits_applied_at: OffsetDateTime,
    reporter: LogReporter,
}

/// The path of the log file the runner keeps in `directory`.
pub(super) fn path_in(directory: &Path) -> PathBuf {
    directory.join(FILE_NAME)
}

fn rotated_path(directory: &Path, number: u64) -> PathBuf {
    directory.join(format!("{FILE_NAME}.{number}"))
}

// rotated_number answers the number of a rotated file, a name of the exact
// form `runner.log.<decimal>`, and `None` for any other name. A number too
// large to hold is above every kept count.
fn rotated_number(name: &OsStr) -> Option<u64> {
    let digits = name.to_str()?.strip_prefix(FILE_NAME)?.strip_prefix('.')?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(digits.parse().unwrap_or(u64::MAX))
}

// open_active opens `runner.log` in `directory` for append, creating it
// `0600`, never truncating it and never following a link, and answers it
// framed after its earlier bytes with its length.
fn open_active(directory: &Path) -> io::Result<(FramedWriter<File>, u64)> {
    let file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .mode(FILE_MODE)
        // The standard library adds `O_CLOEXEC`. A FIFO at the path would
        // otherwise hold the open, and the recorder waiting on it, until a
        // reader came; non-blocking changes nothing for a regular file.
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path_in(directory))?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "runner log is not a regular file",
        ));
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    if metadata.mode() & 0o7777 != FILE_MODE {
        file.set_permissions(Permissions::from_mode(FILE_MODE))?;
    }
    let unterminated = match metadata.len().checked_sub(1) {
        Some(last) => {
            let mut byte = [0_u8];
            file.read_exact_at(&mut byte, last)?;
            byte[0] != b'\n'
        }
        None => false,
    };
    Ok((FramedWriter::resuming(file, unterminated), metadata.len()))
}

// is_present answers whether `path` names anything, without following a
// link.
fn is_present(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

// remove_rotated removes a rotated file another process may already have
// removed.
fn remove_rotated(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

impl LogFile {
    /// Makes `directory` a private directory of the runner's user, opens
    /// `runner.log` in it for append and applies `limits` to the rotated
    /// files there. A last line left unfinished by an earlier process is
    /// ended before the first record written here. Only the open can fail
    /// it: rotated files that cannot be removed are left as they are, and
    /// that and every later failure is told to `reporter`.
    pub(super) fn open(
        directory: &Path,
        limits: LogLimits,
        clock: RecordClock,
        reporter: LogReporter,
    ) -> Result<Self, OpenError> {
        ensure_private_directory(directory).map_err(|source| OpenError {
            stage: OpenStage::Directory,
            source,
        })?;
        let (output, size) = open_active(directory).map_err(|source| OpenError {
            stage: OpenStage::File,
            source,
        })?;
        let now = clock();
        let mut log = Self {
            directory: directory.to_owned(),
            limits,
            clock,
            output,
            size,
            limits_applied_at: now,
            reporter,
        };
        log.keep_limits(now);
        Ok(log)
    }

    // keep_limits applies the limits at `now`, reporting a rotated file that
    // could not be removed; it is tried again when the limits are next
    // applied.
    fn keep_limits(&mut self, now: OffsetDateTime) {
        if let Err(error) = self.apply_limits(now) {
            self.reporter.report(LogFailure::Retention, error.kind());
        }
    }

    // rotate makes `runner.log` the newest rotated file and opens a new one.
    // Each `runner.log.<n>` below the first free number, or below the kept
    // count when none is free, becomes `runner.log.<n+1>`, from the highest
    // down, the file at the kept count being removed first when no number is
    // free. Every step renames onto a free name, so a rotation that stops
    // part way loses no file and the next one fills the gap it left. A
    // `runner.log` that is gone is not renamed, only opened anew. A failure
    // leaves `output` writing the file it had open.
    fn rotate(&mut self) -> io::Result<()> {
        let active = path_in(&self.directory);
        if is_present(&active)? {
            let kept = self.limits.rotated_files;
            let top = match self.first_free_number()? {
                Some(free) => free,
                None => {
                    if kept > 0 {
                        remove_rotated(&rotated_path(&self.directory, kept))?;
                    }
                    kept
                }
            };
            for number in (1..top).rev() {
                fs::rename(
                    rotated_path(&self.directory, number),
                    rotated_path(&self.directory, number.saturating_add(1)),
                )?;
            }
            fs::rename(&active, rotated_path(&self.directory, 1))?;
        }
        let (output, size) = open_active(&self.directory)?;
        self.output = output;
        self.size = size;
        Ok(())
    }

    // first_free_number answers the lowest number up to the kept count that
    // names no file, `None` when every one does.
    fn first_free_number(&self) -> io::Result<Option<u64>> {
        for number in 1..=self.limits.rotated_files {
            if !is_present(&rotated_path(&self.directory, number))? {
                return Ok(Some(number));
            }
        }
        Ok(None)
    }

    // apply_limits removes each rotated file whose number is above the kept
    // count and then each whose last record, its modification time, is older
    // than the kept age at `now`. Nothing but a rotated file is a candidate.
    // A file that cannot be removed is left and the rest are still tried; the
    // first failure is answered.
    fn apply_limits(&mut self, now: OffsetDateTime) -> io::Result<()> {
        self.limits_applied_at = now;
        let mut first_failure = None;
        for entry in fs::read_dir(&self.directory)? {
            let removed = entry.and_then(|entry| {
                let Some(number) = rotated_number(&entry.file_name()) else {
                    return Ok(());
                };
                let expired = number > self.limits.rotated_files || {
                    let modified = OffsetDateTime::from(entry.metadata()?.modified()?);
                    now - modified > self.limits.rotated_age
                };
                if expired {
                    remove_rotated(&entry.path())?;
                }
                Ok(())
            });
            if let Err(error) = removed {
                first_failure.get_or_insert(error);
            }
        }
        first_failure.map_or(Ok(()), Err)
    }
}

impl RecordSink for LogFile {
    // write_record first rotates the file when it holds records and this one
    // would take it past the file limit, so no record is split across files
    // and a record larger than the limit still lands whole; and it applies
    // the limits after a rotation or when they were last applied an hour or
    // more ago. A rotation or removal that fails leaves the files as they
    // are, and is reported: the record is appended to the file open now and
    // the next write or application of the limits tries again. A rotation
    // whose renames succeeded but whose new `runner.log` did not open goes
    // on writing the file that is now `runner.log.1`.
    fn write_record(&mut self, record: &[u8]) -> io::Result<()> {
        let now = (self.clock)();
        let framed = u64::try_from(self.output.framed_len(record)).unwrap_or(u64::MAX);
        if self.size > 0 && self.size.saturating_add(framed) > self.limits.file_bytes {
            if let Err(error) = self.rotate() {
                self.reporter.report(LogFailure::Rotation, error.kind());
            }
            self.keep_limits(now);
        } else if (now - self.limits_applied_at).abs() >= LIMITS_INTERVAL {
            self.keep_limits(now);
        }
        let framed = u64::try_from(self.output.framed_len(record)).unwrap_or(u64::MAX);
        // A failed write may have left part of the record; counting all of
        // it can only bring the next rotation forward.
        self.size = self.size.saturating_add(framed);
        self.output.write_record(record)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _, symlink};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::SystemTime;

    use time::{Duration, OffsetDateTime};

    use super::{LogFile, LogLimits, LogReporter, RecordSink as _, path_in};
    use crate::telemetry::{RecordClock, io_error_class, test_recorder};

    fn mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).expect("inspect path").mode() & 0o7777
    }

    fn written_at() -> OffsetDateTime {
        OffsetDateTime::parse(
            "2026-11-19T04:54:24.993Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("parse the clock's time")
    }

    // A clock a test moves: every read answers the time last set.
    fn settable_clock(at: OffsetDateTime) -> (RecordClock, Arc<Mutex<OffsetDateTime>>) {
        let now = Arc::new(Mutex::new(at));
        let read = Arc::clone(&now);
        let clock: RecordClock = Arc::new(move || *read.lock().expect("read the clock"));
        (clock, now)
    }

    fn limits(file_bytes: u64, rotated_files: u64) -> LogLimits {
        LogLimits {
            file_bytes,
            rotated_files,
            rotated_age: Duration::days(30),
        }
    }

    // A log whose rotated files are never old enough to be removed by age:
    // the files a test writes are modified long after the clock's time.
    fn open_sized(directory: &Path, file_bytes: u64, rotated_files: u64) -> LogFile {
        let (clock, _) = settable_clock(OffsetDateTime::UNIX_EPOCH);
        LogFile::open(
            directory,
            limits(file_bytes, rotated_files),
            clock,
            LogReporter::default(),
        )
        .expect("open the log")
    }

    fn private_logs(root: &Path) -> PathBuf {
        let directory = root.join("logs");
        fs::create_dir(&directory).expect("create logs");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).expect("chmod logs");
        directory
    }

    fn rotated(directory: &Path, number: u64) -> PathBuf {
        directory.join(format!("runner.log.{number}"))
    }

    fn write_modified(path: &Path, contents: &[u8], modified: OffsetDateTime) {
        fs::write(path, contents).expect("write the file");
        File::options()
            .append(true)
            .open(path)
            .expect("open the file")
            .set_modified(SystemTime::from(modified))
            .expect("set the file's modification time");
    }

    fn names(directory: &Path) -> Vec<String> {
        let mut names = fs::read_dir(directory)
            .expect("list logs")
            .map(|entry| {
                entry
                    .expect("read an entry")
                    .file_name()
                    .into_string()
                    .expect("a UTF-8 name")
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn record(index: usize, padding: usize) -> Vec<u8> {
        format!(
            "{{\"event.name\":\"runner.record_{index}\",\"pad\":\"{}\"}}\n",
            "x".repeat(padding)
        )
        .into_bytes()
    }

    // REQ-TELEMETRY-53
    #[test]
    fn records_past_the_file_limit_rotate_whole_into_numbered_files() {
        let root = tempfile::tempdir().expect("temporary directory");
        let directory = root.path().join("logs");
        let mut log = open_sized(&directory, 120, 16);

        // Varied lengths, one of them alone larger than the limit.
        let records = [10, 30, 5, 60, 200, 0, 45, 20, 70, 15]
            .into_iter()
            .enumerate()
            .map(|(index, padding)| record(index, padding))
            .collect::<Vec<_>>();
        for record in &records {
            log.write_record(record).expect("write a record");
        }

        let mut files = names(&directory)
            .into_iter()
            .filter_map(|name| {
                name.strip_prefix("runner.log.")
                    .map(|number| number.parse::<u64>().expect("a rotated file's number"))
            })
            .collect::<Vec<_>>();
        files.sort_unstable();
        assert!(files.len() >= 3, "rotated files: {files:?}");
        assert_eq!(files, (1..=files.len() as u64).collect::<Vec<_>>());
        let first = fs::read(rotated(&directory, files.len() as u64)).expect("oldest file");
        assert!(first.starts_with(&records[0]));
        let mut read_back = Vec::new();
        for number in files.iter().rev() {
            let bytes = fs::read(rotated(&directory, *number)).expect("read a rotated file");
            let lines = bytes
                .split_inclusive(|byte| *byte == b'\n')
                .collect::<Vec<_>>();
            assert!(lines.iter().all(|line| records.iter().any(|r| r == line)));
            assert!(
                bytes.len() <= 120 || lines.len() == 1,
                "file {number} overfull"
            );
            read_back.extend_from_slice(&bytes);
        }
        read_back.extend_from_slice(&fs::read(path_in(&directory)).expect("read runner.log"));
        assert_eq!(read_back, records.concat());
    }

    // REQ-TELEMETRY-53
    #[test]
    fn a_rotation_past_the_kept_number_removes_the_highest_and_shifts_the_rest() {
        let root = tempfile::tempdir().expect("temporary directory");
        let directory = private_logs(root.path());
        fs::write(path_in(&directory), b"active\n").expect("write runner.log");
        for (number, contents) in [(1, "one\n"), (2, "two\n"), (3, "three\n"), (12, "twelve\n")] {
            fs::write(rotated(&directory, number), contents).expect("write a rotated file");
        }

        let mut log = open_sized(&directory, 16, 3);
        assert!(
            !rotated(&directory, 12).exists(),
            "a number above the kept count stays"
        );
        log.write_record(b"{\"event.name\":\"runner.next\"}\n")
            .expect("write a record");

        assert_eq!(
            names(&directory),
            ["runner.log", "runner.log.1", "runner.log.2", "runner.log.3"]
        );
        assert_eq!(fs::read(rotated(&directory, 1)).expect(".1"), b"active\n");
        assert_eq!(fs::read(rotated(&directory, 2)).expect(".2"), b"one\n");
        assert_eq!(fs::read(rotated(&directory, 3)).expect(".3"), b"two\n");
        assert_eq!(
            fs::read(path_in(&directory)).expect("runner.log"),
            b"{\"event.name\":\"runner.next\"}\n"
        );
        assert_eq!(mode(&path_in(&directory)), 0o600);
    }

    // REQ-TELEMETRY-53
    #[test]
    fn rotated_files_past_the_age_are_removed_at_open_and_hourly() {
        let root = tempfile::tempdir().expect("temporary directory");
        let directory = private_logs(root.path());
        let now = written_at();
        write_modified(
            &rotated(&directory, 1),
            b"recent\n",
            now - Duration::days(29),
        );
        write_modified(
            &rotated(&directory, 2),
            b"stale\n",
            now - Duration::days(31),
        );
        let (clock, set) = settable_clock(now);

        let mut log = LogFile::open(
            &directory,
            limits(1 << 20, 7),
            clock,
            LogReporter::default(),
        )
        .expect("open the log");
        assert_eq!(names(&directory), ["runner.log", "runner.log.1"]);

        write_modified(
            &rotated(&directory, 3),
            b"stale\n",
            now - Duration::days(31),
        );
        *set.lock().expect("set the clock") = now + Duration::minutes(59);
        log.write_record(b"{\"event.name\":\"runner.soon\"}\n")
            .expect("write a record");
        assert!(
            rotated(&directory, 3).exists(),
            "the limits applied within the hour"
        );

        *set.lock().expect("set the clock") = now + Duration::minutes(61);
        log.write_record(b"{\"event.name\":\"runner.later\"}\n")
            .expect("write a record");
        assert_eq!(names(&directory), ["runner.log", "runner.log.1"]);
        assert_eq!(fs::read(rotated(&directory, 1)).expect(".1"), b"recent\n");
    }

    // REQ-TELEMETRY-53
    #[test]
    fn nothing_but_rotated_files_is_removed() {
        let root = tempfile::tempdir().expect("temporary directory");
        let directory = private_logs(root.path());
        let now = written_at();
        let old = now - Duration::days(400);
        let others = [
            "notes.txt",
            "runner.log.",
            "runner.log.1a",
            "runner.log.old",
            "runner.log.-1",
            "runner.log.+2",
            "other.log.1",
            "runner.log.1.gz",
        ];
        for name in others {
            write_modified(&directory.join(name), b"kept\n", old);
        }
        write_modified(&path_in(&directory), b"active\n", old);
        let (clock, set) = settable_clock(now);

        let mut log = LogFile::open(
            &directory,
            limits(1 << 20, 0),
            clock,
            LogReporter::default(),
        )
        .expect("open the log");
        *set.lock().expect("set the clock") = now + Duration::hours(2);
        log.write_record(b"{\"event.name\":\"runner.next\"}\n")
            .expect("write a record");

        let mut expected = others
            .iter()
            .map(|name| (*name).to_owned())
            .chain(["runner.log".to_owned()])
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(names(&directory), expected);
        assert!(
            fs::read(path_in(&directory))
                .expect("runner.log")
                .starts_with(b"active\n")
        );
    }

    // REQ-TELEMETRY-54
    #[test]
    fn a_failed_rotation_is_reported_once_and_records_stay_in_runner_log() {
        let root = tempfile::tempdir().expect("temporary directory");
        let directory = private_logs(root.path());
        // The one kept rotated file is a directory, which a rotation cannot
        // remove to make room, so no rotation completes.
        fs::create_dir(rotated(&directory, 1)).expect("create runner.log.1");
        fs::write(rotated(&directory, 1).join("kept"), b"kept\n").expect("fill runner.log.1");
        let unremovable = root.path().join("unremovable");
        fs::create_dir(&unremovable).expect("create a directory");
        let class = io_error_class(
            fs::remove_file(&unremovable)
                .expect_err("a directory is not removed as a file")
                .kind(),
        );
        let (recorder, capture) = test_recorder("rbt_fixture");
        let reporter = LogReporter::default();
        reporter.attach(&recorder);
        let (clock, _) = settable_clock(OffsetDateTime::UNIX_EPOCH);
        let mut log =
            LogFile::open(&directory, limits(64, 1), clock, reporter).expect("open the log");

        let records = (0..4).map(|index| record(index, 40)).collect::<Vec<_>>();
        for record in &records {
            log.write_record(record).expect("write a record");
        }

        let reports = capture
            .records()
            .into_iter()
            .filter(|record| record["event.name"] == "runner.telemetry_log")
            .collect::<Vec<_>>();
        assert_eq!(reports.len(), 1, "{reports:?}");
        assert_eq!(reports[0]["diagnostic.classification"], "rotation_failed");
        assert_eq!(reports[0]["error.type"], class);
        assert_eq!(names(&directory), ["runner.log", "runner.log.1"]);
        assert_eq!(
            fs::read(path_in(&directory)).expect("read runner.log"),
            records.concat()
        );
    }

    // REQ-TELEMETRY-52
    #[test]
    fn opening_creates_a_private_directory_and_file() {
        let root = tempfile::tempdir().expect("temporary directory");
        let directory = root.path().join("logs");

        let mut log = open_sized(&directory, 1 << 20, 7);
        log.write_record(b"{\"event.name\":\"runner.first\"}\n")
            .expect("write a record");

        assert!(fs::symlink_metadata(&directory).expect("logs").is_dir());
        assert_eq!(mode(&directory), 0o700);
        assert_eq!(mode(&path_in(&directory)), 0o600);
        assert_eq!(
            fs::read(path_in(&directory)).expect("read the log"),
            b"{\"event.name\":\"runner.first\"}\n"
        );
    }

    // REQ-TELEMETRY-52
    #[test]
    fn opening_keeps_earlier_lines_and_ends_an_unfinished_one() {
        let root = tempfile::tempdir().expect("temporary directory");
        let directory = root.path().join("logs");
        fs::create_dir(&directory).expect("create logs");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).expect("chmod logs");
        let earlier = b"{\"event.name\":\"runner.earlier\"}\n{\"event.name\":\"runner.cut";
        fs::write(path_in(&directory), earlier).expect("write earlier lines");

        let mut log = open_sized(&directory, 1 << 20, 7);
        log.write_record(b"{\"event.name\":\"runner.next\"}\n")
            .expect("write a record");

        let mut expected = earlier.to_vec();
        expected.extend_from_slice(b"\n{\"event.name\":\"runner.next\"}\n");
        assert_eq!(
            fs::read(path_in(&directory)).expect("read the log"),
            expected
        );
    }

    // REQ-TELEMETRY-52, INV-TELEMETRY-18
    #[test]
    fn a_linked_directory_or_file_is_not_followed() {
        let root = tempfile::tempdir().expect("temporary directory");
        let target = root.path().join("elsewhere");
        fs::create_dir(&target).expect("create the link's target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).expect("chmod target");
        let linked_directory = root.path().join("linked").join("logs");
        fs::create_dir(linked_directory.parent().expect("parent")).expect("create parent");
        symlink(&target, &linked_directory).expect("link logs");

        assert!(
            LogFile::open(
                &linked_directory,
                limits(1 << 20, 7),
                settable_clock(written_at()).0,
                LogReporter::default(),
            )
            .is_err()
        );
        assert_eq!(mode(&target), 0o755);
        assert!(fs::read_dir(&target).expect("list target").next().is_none());

        let directory = root.path().join("logs");
        fs::create_dir(&directory).expect("create logs");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).expect("chmod logs");
        let target_file = root.path().join("elsewhere.log");
        fs::write(&target_file, b"kept").expect("write the link's target");
        fs::set_permissions(&target_file, fs::Permissions::from_mode(0o644)).expect("chmod");
        symlink(&target_file, path_in(&directory)).expect("link runner.log");

        assert!(
            LogFile::open(
                &directory,
                limits(1 << 20, 7),
                settable_clock(written_at()).0,
                LogReporter::default(),
            )
            .is_err()
        );
        assert_eq!(fs::read(&target_file).expect("read target"), b"kept");
        assert_eq!(mode(&target_file), 0o644);
        assert!(
            fs::symlink_metadata(path_in(&directory))
                .expect("the link")
                .file_type()
                .is_symlink()
        );
    }
}
