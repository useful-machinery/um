use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use useful_execution::{DiskSpace, format_bytes};

use super::agent_logins::ConfigUnavailable;
use super::{CheckDescriptor, DoctorCheck, OperatorConfigSource, Outcome, Status};
use crate::telemetry::io_error_class;

/// How the check reads a disk: `useful_execution::read_disk_space` outside its tests.
type DiskReader = fn(&Path) -> io::Result<DiskSpace>;

/// `environment.disk-space` (REQ-RUNNERENROLL-153): reads the disk the configured work
/// root is on by the rule the serving runner uses (REQ-RUNNERENROLL-152) and judges it
/// against the configured minimum. It asks neither Cloud nor a serving runner, reads no
/// runner state and creates nothing.
pub(super) struct DiskSpaceCheck {
    config: Option<OperatorConfigSource>,
    read: DiskReader,
}

impl DiskSpaceCheck {
    pub(super) fn new(config: Option<OperatorConfigSource>) -> Self {
        Self::with_reader(config, useful_execution::read_disk_space)
    }

    fn with_reader(config: Option<OperatorConfigSource>, read: DiskReader) -> Self {
        Self { config, read }
    }
}

impl DoctorCheck for DiskSpaceCheck {
    fn descriptor(&self) -> CheckDescriptor {
        CheckDescriptor {
            id: "environment.disk-space",
            title: "Disk space",
            default: false,
        }
    }

    fn run(&self) -> Outcome {
        let Some(config) = &self.config else {
            return Outcome::fail(
                "config_required",
                "This check reads the runner's work root; set USEFUL_HOME or HOME to locate the runner home, or name the runner operator configuration with --config PATH.",
                BTreeMap::new(),
            );
        };
        let (work_root, minimum) = match config.disk_space_settings() {
            Ok(settings) => settings,
            Err(ConfigUnavailable::Absent) => {
                return Outcome::fail(
                    "config_required",
                    "The runner home holds no runner operator configuration; name one with --config PATH.",
                    BTreeMap::new(),
                );
            }
            Err(ConfigUnavailable::Invalid) => {
                return Outcome::fail(
                    "config_invalid",
                    "The runner operator configuration is invalid.",
                    BTreeMap::new(),
                );
            }
        };
        let space = match (self.read)(&work_root) {
            Ok(space) => space,
            // The class of the failure, never the path.
            Err(error) => {
                return Outcome::fail(
                    "disk_space_unreadable",
                    "The disk the runner's work root is on cannot be read.",
                    BTreeMap::from([("cause".to_owned(), io_error_class(error.kind()).to_owned())]),
                );
            }
        };
        space_outcome(space, minimum)
    }
}

/// The check's outcome for a reading of the work root's disk against `minimum`.
fn space_outcome(space: DiskSpace, minimum: u64) -> Outcome {
    let details = BTreeMap::from([
        (
            "available_bytes".to_owned(),
            space.available_bytes.to_string(),
        ),
        ("total_bytes".to_owned(), space.total_bytes.to_string()),
        ("minimum_free_bytes".to_owned(), minimum.to_string()),
    ]);
    let available = format_bytes(space.available_bytes);
    let total = format_bytes(space.total_bytes);
    let minimum_text = format_bytes(minimum);
    if space.is_low(minimum) {
        Outcome {
            status: Status::Fail,
            code: "disk_space_low",
            message: format!(
                "The work root's disk has {available} free of {total}, below the minimum of {minimum_text}."
            ),
            details,
        }
    } else {
        Outcome {
            status: Status::Pass,
            code: "ok",
            message: format!(
                "The work root's disk has {available} free of {total} (minimum {minimum_text})."
            ),
            details,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::io;
    use std::path::{Path, PathBuf};

    use useful_execution::DiskSpace;

    use super::DiskSpaceCheck;
    use crate::doctor::{DoctorCheck as _, OperatorConfigSource, Outcome, Status};
    use crate::enrollment::RunnerHome;

    const AVAILABLE: u64 = 5_368_709_120;
    const TOTAL: u64 = 423_054_278_656;

    thread_local! {
        /// The path the test's reader was last asked for.
        static READ_PATH: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }

    fn remember(path: &Path) {
        READ_PATH.with(|read| *read.borrow_mut() = Some(path.to_owned()));
    }

    fn read_path() -> Option<PathBuf> {
        READ_PATH.with(|read| read.borrow().clone())
    }

    fn reads_available(path: &Path) -> io::Result<DiskSpace> {
        remember(path);
        Ok(DiskSpace {
            available_bytes: AVAILABLE,
            total_bytes: TOTAL,
        })
    }

    fn reads_a_byte_less(path: &Path) -> io::Result<DiskSpace> {
        remember(path);
        Ok(DiskSpace {
            available_bytes: AVAILABLE - 1,
            total_bytes: TOTAL,
        })
    }

    fn reads_nothing(path: &Path) -> io::Result<DiskSpace> {
        remember(path);
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    }

    /// Writes an operator configuration at `path` whose work root is `work_root` and whose
    /// minimum is `minimum`.
    fn write_config(path: &Path, work_root: &Path, minimum: u64) {
        let directory = path.parent().unwrap();
        std::fs::create_dir_all(directory).unwrap();
        std::fs::write(
            path,
            serde_json::to_vec(&serde_json::json!({
                "schemaVersion": 1,
                "deploymentMode": "development",
                "runnerStatePath": directory.join("state.json"),
                "controlSocketPath": directory.join("control.sock"),
                "workRoot": work_root,
                "minimumFreeDiskBytes": minimum,
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn run(config: Option<OperatorConfigSource>, read: super::DiskReader) -> Outcome {
        DiskSpaceCheck::with_reader(config, read).run()
    }

    fn sizes(outcome: &Outcome) -> [Option<&str>; 3] {
        ["available_bytes", "total_bytes", "minimum_free_bytes"]
            .map(|key| outcome.details.get(key).map(String::as_str))
    }

    // REQ-RUNNERENROLL-153
    #[test]
    fn the_check_passes_at_the_minimum_and_fails_a_byte_below_it() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("runner.json");
        let work_root = root.path().join("work");
        write_config(&config, &work_root, AVAILABLE);
        let source = Some(OperatorConfigSource::File(config));

        let outcome = run(source.clone(), reads_available);
        assert_eq!((outcome.status, outcome.code), (Status::Pass, "ok"));
        assert_eq!(read_path().as_deref(), Some(work_root.as_path()));
        assert_eq!(
            sizes(&outcome),
            [Some("5368709120"), Some("423054278656"), Some("5368709120")],
        );

        let outcome = run(source, reads_a_byte_less);
        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "disk_space_low")
        );
        assert_eq!(
            sizes(&outcome),
            [Some("5368709119"), Some("423054278656"), Some("5368709120")],
        );
    }

    // REQ-RUNNERENROLL-153
    #[test]
    fn the_check_reads_the_runner_homes_configuration() {
        let root = tempfile::tempdir().unwrap();
        let home = RunnerHome::new(&root.path().join("useful")).unwrap();
        let work_root = root.path().join("work");
        write_config(&home.config_path(), &work_root, AVAILABLE + 1);

        let outcome = run(Some(OperatorConfigSource::Home(home)), reads_available);
        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "disk_space_low")
        );
        assert_eq!(read_path().as_deref(), Some(work_root.as_path()));
    }

    // REQ-RUNNERENROLL-153
    #[test]
    fn an_unreadable_disk_fails_with_its_class_and_no_path() {
        let root = tempfile::tempdir().unwrap();
        let config = root.path().join("runner.json");
        write_config(&config, &root.path().join("work"), 0);

        let outcome = run(Some(OperatorConfigSource::File(config)), reads_nothing);
        assert_eq!(
            (outcome.status, outcome.code),
            (Status::Fail, "disk_space_unreadable")
        );
        assert_eq!(
            outcome.details,
            [("cause".to_owned(), "permission_denied".to_owned())].into(),
        );
    }

    // REQ-RUNNERENROLL-153
    #[test]
    fn the_check_fails_without_a_configuration_it_can_load() {
        let root = tempfile::tempdir().unwrap();
        let empty_home = RunnerHome::new(&root.path().join("useful")).unwrap();
        let unloadable = root.path().join("runner.json");
        std::fs::write(&unloadable, b"{").unwrap();

        for (config, code) in [
            (None, "config_required"),
            (
                Some(OperatorConfigSource::Home(empty_home)),
                "config_required",
            ),
            (
                Some(OperatorConfigSource::File(unloadable)),
                "config_invalid",
            ),
        ] {
            READ_PATH.with(|read| read.take());
            let outcome = run(config, reads_available);
            assert_eq!((outcome.status, outcome.code), (Status::Fail, code));
            // The disk is not read without a work root, and nothing is created.
            assert_eq!(read_path(), None, "{code}");
        }
        assert!(!root.path().join("useful").exists());
    }
}
