//! The disk rule (REQ-RUNNERENROLL-152): how much space a filesystem has, read in one
//! `statvfs`, when that is low against a minimum, and how a size is written for a person.
//! The serving runner's monitor, `useful runner doctor`'s check and the engine's gate all
//! judge a disk through these, so a reading means the same thing everywhere it is read.
//!
//! It also holds the gate a `useful workflow run` process builds from the runner's internal
//! variable (INV-ENGINE-46): the runner writes the variable through `RunnerDiskGate` and the
//! attempt reads it back through the same type, so the two ends cannot drift.

use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io;
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use crate::{StartGateReading, StepStartGate};

/// The minimum free space a runner keeps when its operator configuration names none:
/// 5 GiB.
pub const DEFAULT_MINIMUM_FREE_DISK_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// The largest minimum an operator configuration may name, the largest integer a JSON
/// reader holds exactly: 2^53 - 1.
pub const MAXIMUM_MINIMUM_FREE_DISK_BYTES: u64 = 9_007_199_254_740_991;

/// The space available to the reading user and the size of one filesystem, in bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiskSpace {
    pub available_bytes: u64,
    pub total_bytes: u64,
}

impl DiskSpace {
    /// Whether the available space is below `minimum`; with a minimum of 0 never.
    pub fn is_low(self, minimum: u64) -> bool {
        self.available_bytes < minimum
    }
}

/// The filesystem `path` is on, in one `statvfs`: the blocks available to an unprivileged
/// user and the filesystem's size, each in fragment-size units.
pub fn read_disk_space(path: &Path) -> io::Result<DiskSpace> {
    let statistics = rustix::fs::statvfs(path).map_err(io::Error::from)?;
    Ok(DiskSpace {
        available_bytes: statistics.f_bavail.saturating_mul(statistics.f_frsize),
        total_bytes: statistics.f_blocks.saturating_mul(statistics.f_frsize),
    })
}

/// `bytes` for a person in binary units: below 1 KiB the count and ` B`; otherwise the
/// largest unit the size reaches, floored to one decimal below 10 of it and to a whole
/// number from 10, so a size just under a minimum never reads as that minimum.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut unit_bytes: u64 = 1;
    let mut unit = None;
    for name in UNITS {
        let Some(next) = unit_bytes.checked_mul(1024).filter(|next| bytes >= *next) else {
            break;
        };
        unit_bytes = next;
        unit = Some(name);
    }
    let Some(unit) = unit else {
        return format!("{bytes} B");
    };
    let whole = bytes / unit_bytes;
    if whole >= 10 {
        return format!("{whole} {unit}");
    }
    let tenths = (bytes % unit_bytes).saturating_mul(10) / unit_bytes;
    format!("{whole}.{tenths} {unit}")
}

/// The variable through which the runner names the disk a tree run's `useful workflow run`
/// processes hold their steps by (INV-ENGINE-46). It is engine-reserved: like every
/// `USEFUL_*` name it never reaches a step, an agent or a child (INV-ENGINE-43).
pub const RUNNER_DISK_GATE_ENVIRONMENT: &str = "USEFUL_RUNNER_DISK_GATE";

/// The disk a run's steps are held by and the minimum free space they wait for, as the
/// runner writes them in `RUNNER_DISK_GATE_ENVIRONMENT`: the decimal minimum, one `:`, then
/// the absolute path, which may itself hold `:`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerDiskGate {
    path: PathBuf,
    minimum_free_bytes: u64,
}

/// Why a `RunnerDiskGate` was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerDiskGateError {
    /// The value holds no `:` between the minimum and the path.
    MissingSeparator,
    /// The minimum is empty or not a decimal number.
    MinimumNotDecimal,
    /// The minimum is above `MAXIMUM_MINIMUM_FREE_DISK_BYTES`.
    MinimumAboveMaximum,
    /// The path is not absolute.
    RelativePath,
}

impl fmt::Display for RunnerDiskGateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingSeparator => "the value is not <minimum>:<absolute path>",
            Self::MinimumNotDecimal => "the minimum is not a decimal number of bytes",
            Self::MinimumAboveMaximum => "the minimum is above the largest allowed",
            Self::RelativePath => "the path is not absolute",
        })
    }
}

impl Error for RunnerDiskGateError {}

impl RunnerDiskGate {
    /// The gate over `path` with `minimum_free_bytes`; refused when the path is not absolute
    /// or the minimum is above `MAXIMUM_MINIMUM_FREE_DISK_BYTES`, so every gate built is one
    /// `from_environment_value` reads back.
    pub fn new(path: PathBuf, minimum_free_bytes: u64) -> Result<Self, RunnerDiskGateError> {
        if minimum_free_bytes > MAXIMUM_MINIMUM_FREE_DISK_BYTES {
            return Err(RunnerDiskGateError::MinimumAboveMaximum);
        }
        if !path.is_absolute() {
            return Err(RunnerDiskGateError::RelativePath);
        }
        Ok(Self {
            path,
            minimum_free_bytes,
        })
    }

    /// The gate a `RUNNER_DISK_GATE_ENVIRONMENT` value names. The minimum is read as bytes up
    /// to the first `:`, so the path after it may hold one.
    pub fn from_environment_value(value: &OsStr) -> Result<Self, RunnerDiskGateError> {
        let bytes = value.as_bytes();
        let separator = bytes
            .iter()
            .position(|byte| *byte == b':')
            .ok_or(RunnerDiskGateError::MissingSeparator)?;
        let (minimum, path) = (&bytes[..separator], &bytes[separator + 1..]);
        if minimum.is_empty() || !minimum.iter().all(u8::is_ascii_digit) {
            return Err(RunnerDiskGateError::MinimumNotDecimal);
        }
        // Only ASCII digits remain, so a failed parse is an overflow.
        let minimum = std::str::from_utf8(minimum)
            .ok()
            .and_then(|minimum| minimum.parse::<u64>().ok())
            .ok_or(RunnerDiskGateError::MinimumAboveMaximum)?;
        Self::new(PathBuf::from(OsStr::from_bytes(path)), minimum)
    }

    /// The `RUNNER_DISK_GATE_ENVIRONMENT` value naming this gate.
    pub fn environment_value(&self) -> OsString {
        let mut value = format!("{}:", self.minimum_free_bytes).into_bytes();
        value.extend_from_slice(self.path.as_os_str().as_bytes());
        OsString::from_vec(value)
    }

    /// The path whose filesystem is read.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The free space below which a step is held.
    pub fn minimum_free_bytes(&self) -> u64 {
        self.minimum_free_bytes
    }

    /// The start gate that reads this disk.
    pub fn start_gate(self) -> RunnerDiskStartGate {
        RunnerDiskStartGate::with_reader(self, Box::new(read_disk_space))
    }
}

/// What reads a filesystem's space: `read_disk_space` in production, a script in tests.
type DiskReader = Box<dyn Fn(&Path) -> io::Result<DiskSpace> + Send + Sync>;

/// The start gate over a `RunnerDiskGate` (INV-ENGINE-46): each read judges the disk by
/// `DiskSpace::is_low`. A failed read keeps the last good reading, and with none the gate is
/// open, so a disk that cannot be read never holds a run.
pub struct RunnerDiskStartGate {
    gate: RunnerDiskGate,
    reader: DiskReader,
    last: Mutex<Option<DiskSpace>>,
}

impl RunnerDiskStartGate {
    fn with_reader(gate: RunnerDiskGate, reader: DiskReader) -> Self {
        Self {
            gate,
            reader,
            last: Mutex::new(None),
        }
    }
}

impl StepStartGate for RunnerDiskStartGate {
    fn read(&self) -> StartGateReading {
        let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
        match (self.reader)(&self.gate.path) {
            Ok(space) => *last = Some(space),
            Err(error) => {
                tracing::warn!(event = "disk_gate_read_failed", kind = ?error.kind());
            }
        }
        match *last {
            Some(space) if space.is_low(self.gate.minimum_free_bytes) => {
                StartGateReading::DiskLow {
                    available_bytes: space.available_bytes,
                    minimum_free_bytes: self.gate.minimum_free_bytes,
                }
            }
            _ => StartGateReading::Open,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // REQ-RUNNERENROLL-152: the disk is low while the available space is below the minimum,
    // and a minimum of 0 is never reached from below.
    #[test]
    fn a_disk_is_low_only_below_its_minimum() {
        let space = DiskSpace {
            available_bytes: 4,
            total_bytes: 10,
        };
        assert!(space.is_low(5));
        assert!(!space.is_low(4));
        let full = DiskSpace {
            available_bytes: 0,
            total_bytes: 10,
        };
        assert!(!full.is_low(0));
        assert!(full.is_low(1));
    }

    // REQ-RUNNERENROLL-152: a reading gives the filesystem's size and no more available
    // than that; a path that does not exist is not read.
    #[test]
    fn a_directory_reads_its_filesystem_and_a_missing_path_does_not() {
        let directory = tempfile::tempdir().unwrap();
        let space = read_disk_space(directory.path()).unwrap();
        assert!(space.total_bytes > 0, "{space:?}");
        assert!(space.available_bytes <= space.total_bytes, "{space:?}");

        let missing = read_disk_space(&directory.path().join("missing")).unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::NotFound);
    }

    // REQ-RUNNERENROLL-152: sizes are written in binary units, floored, with one decimal
    // below 10, so a size a byte under a unit's boundary never reads as the boundary.
    #[test]
    fn sizes_are_floored_binary_units() {
        for (bytes, written) in [
            (0, "0 B"),
            (1023, "1023 B"),
            (1024, "1.0 KiB"),
            (6_186_598, "5.8 MiB"),
            (6_186_599, "5.9 MiB"),
            (10 * 1024 * 1024 - 1, "9.9 MiB"),
            (DEFAULT_MINIMUM_FREE_DISK_BYTES - 1, "4.9 GiB"),
            (DEFAULT_MINIMUM_FREE_DISK_BYTES, "5.0 GiB"),
            (423_054_278_656, "394 GiB"),
            (u64::MAX, "15 EiB"),
        ] {
            assert_eq!(format_bytes(bytes), written, "{bytes}");
        }
    }

    // INV-ENGINE-46: a value is the decimal minimum, the first `:`, then the absolute path,
    // which may hold `:`, and it round-trips.
    #[test]
    fn the_runner_variable_names_a_minimum_and_an_absolute_path() {
        let read = |value: &str| RunnerDiskGate::from_environment_value(OsStr::new(value));
        let gate = read("5368709120:/work/a:b").unwrap();
        assert_eq!(gate.path(), Path::new("/work/a:b"));
        assert_eq!(gate.minimum_free_bytes(), DEFAULT_MINIMUM_FREE_DISK_BYTES);
        assert_eq!(
            gate.environment_value(),
            OsString::from("5368709120:/work/a:b")
        );

        for (path, minimum) in [
            ("/", 0),
            ("/srv/runner:work", MAXIMUM_MINIMUM_FREE_DISK_BYTES),
            ("/srv/work", 1),
        ] {
            let gate = RunnerDiskGate::new(PathBuf::from(path), minimum).unwrap();
            assert_eq!(
                RunnerDiskGate::from_environment_value(&gate.environment_value()).unwrap(),
                gate
            );
        }
        let not_utf8 = OsString::from_vec(b"7:/srv/\xff".to_vec());
        let gate = RunnerDiskGate::from_environment_value(&not_utf8).unwrap();
        assert_eq!(gate.environment_value(), not_utf8);
    }

    // INV-ENGINE-46: a value that is empty, has no decimal minimum within the maximum or names
    // a relative path is refused, and so is a gate built from either.
    #[test]
    fn a_malformed_runner_variable_is_refused() {
        let read = |value: &str| RunnerDiskGate::from_environment_value(OsStr::new(value));
        for (value, error) in [
            ("", RunnerDiskGateError::MissingSeparator),
            ("/srv/work", RunnerDiskGateError::MissingSeparator),
            (":/srv/work", RunnerDiskGateError::MinimumNotDecimal),
            ("+5:/srv/work", RunnerDiskGateError::MinimumNotDecimal),
            ("-1:/srv/work", RunnerDiskGateError::MinimumNotDecimal),
            ("5 :/srv/work", RunnerDiskGateError::MinimumNotDecimal),
            ("0x10:/srv/work", RunnerDiskGateError::MinimumNotDecimal),
            (
                "9007199254740992:/srv/work",
                RunnerDiskGateError::MinimumAboveMaximum,
            ),
            (
                "99999999999999999999999:/srv/work",
                RunnerDiskGateError::MinimumAboveMaximum,
            ),
            ("5:", RunnerDiskGateError::RelativePath),
            ("5:srv/work", RunnerDiskGateError::RelativePath),
        ] {
            assert_eq!(read(value), Err(error), "{value:?}");
        }
        assert_eq!(
            RunnerDiskGate::new(PathBuf::from("work"), 0),
            Err(RunnerDiskGateError::RelativePath)
        );
        assert_eq!(
            RunnerDiskGate::new(PathBuf::from("/work"), MAXIMUM_MINIMUM_FREE_DISK_BYTES + 1),
            Err(RunnerDiskGateError::MinimumAboveMaximum)
        );
    }

    // INV-ENGINE-46: the gate holds below the minimum with the reading's figures and is open
    // at or above it; a failed read keeps the last good answer, and with none is open.
    #[test]
    fn the_disk_gate_judges_each_reading_and_keeps_the_last_good_one() {
        let answers = std::sync::Arc::new(Mutex::new(std::collections::VecDeque::<
            io::Result<DiskSpace>,
        >::new()));
        let paths = std::sync::Arc::new(Mutex::new(Vec::<PathBuf>::new()));
        let gate = RunnerDiskStartGate::with_reader(
            RunnerDiskGate::new(PathBuf::from("/srv/work:root"), 100).unwrap(),
            Box::new({
                let answers = std::sync::Arc::clone(&answers);
                let paths = std::sync::Arc::clone(&paths);
                move |path: &Path| {
                    paths.lock().unwrap().push(path.to_owned());
                    answers.lock().unwrap().pop_front().unwrap()
                }
            }),
        );
        let space = |available_bytes| {
            Ok(DiskSpace {
                available_bytes,
                total_bytes: 1000,
            })
        };
        let failed = || Err(io::Error::other("statvfs failed"));
        answers.lock().unwrap().extend([
            failed(),
            space(99),
            failed(),
            space(100),
            failed(),
            space(1000),
        ]);
        let low = StartGateReading::DiskLow {
            available_bytes: 99,
            minimum_free_bytes: 100,
        };

        assert_eq!(gate.read(), StartGateReading::Open);
        assert_eq!(gate.read(), low);
        assert_eq!(gate.read(), low);
        assert_eq!(gate.read(), StartGateReading::Open);
        assert_eq!(gate.read(), StartGateReading::Open);
        assert_eq!(gate.read(), StartGateReading::Open);
        let paths = paths.lock().unwrap();
        assert_eq!(paths.len(), 6);
        assert!(paths.iter().all(|path| path == Path::new("/srv/work:root")));
    }
}
