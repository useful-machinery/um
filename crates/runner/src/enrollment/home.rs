//! The runner home: the directory `runner/` under the `um` home, which holds the
//! operator configuration, the enrolled state, the control socket and the work
//! root a `um runner` command uses when it is given no `--config`
//! (REQ-RUNNERENROLL-109).

use std::error::Error;
use std::fmt;
use std::fs::{self, DirBuilder, Permissions};
use std::io::{self, Write as _};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use rustix::fs::{CWD, RenameFlags};

use super::{
    DIRECTORY_MODE, DeploymentMode, EnrollmentError, OperatorConfig, TEMPORARY_SEQUENCE,
    create_new_private_file, ensure_private_directory, json_document, load_control_socket_path,
    load_operator_config, sync_directory_io,
};

const RUNNER_DIRECTORY: &str = "runner";
const CONFIG_FILE: &str = "config.json";
const STATE_FILE: &str = "state.json";
const CONTROL_SOCKET_FILE: &str = "control.sock";
const WORK_DIRECTORY: &str = "work";
/// The generated configuration's temporary files, `<prefix>.<pid>.<sequence>`
/// in `runner/`; distinct from the state's, so neither writer mistakes the
/// other's file for its own.
const CONFIG_TEMPORARY_PREFIX: &str = ".runner-config.tmp";
const SCHEMA_VERSION: u8 = 1;

/// The runner home of one `um` home. Its paths are absolute, made so from the
/// working directory without resolving a link; the directories need not exist.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnerHome {
    um_home: PathBuf,
}

impl RunnerHome {
    /// The runner home under the `um` home `um_home`.
    pub fn new(um_home: &Path) -> Result<Self, RunnerHomeError> {
        let absolute = std::path::absolute(um_home).map_err(|source| RunnerHomeError::Resolve {
            path: um_home.to_owned(),
            source,
        })?;
        Ok(Self { um_home: absolute })
    }

    /// `runner/` under the `um` home.
    pub fn directory(&self) -> PathBuf {
        self.um_home.join(RUNNER_DIRECTORY)
    }

    /// `runner/config.json`, the operator configuration.
    pub fn config_path(&self) -> PathBuf {
        self.directory().join(CONFIG_FILE)
    }

    /// `runner/state.json`, the enrolled state of the generated configuration.
    pub fn state_path(&self) -> PathBuf {
        self.directory().join(STATE_FILE)
    }

    /// `runner/control.sock`, the control socket of the generated configuration.
    pub fn control_socket_path(&self) -> PathBuf {
        self.directory().join(CONTROL_SOCKET_FILE)
    }

    /// `runner/work`, the work root of the generated configuration.
    pub fn work_root(&self) -> PathBuf {
        self.directory().join(WORK_DIRECTORY)
    }

    /// Creates what the home lacks and answers the path of its operator
    /// configuration, loaded and checked (REQ-RUNNERENROLL-110,
    /// REQ-RUNNERENROLL-111). The `um` home, `runner/` and `runner/work/` are
    /// created `0700` when absent, and a generated configuration is published
    /// at `runner/config.json` when nothing is there. Nothing that exists is
    /// replaced, rewritten or re-moded (INV-RUNNER-25): an existing `runner/`
    /// must already be a private directory of the effective user, an existing
    /// work root is left to `um runner serve`'s own checks, and an existing
    /// configuration is used as written. No state, journal, lock or socket is
    /// written.
    pub fn initialize(&self) -> Result<PathBuf, RunnerHomeError> {
        DirBuilder::new()
            .recursive(true)
            .mode(DIRECTORY_MODE)
            .create(&self.um_home)
            .map_err(|source| RunnerHomeError::CreateUmHome {
                path: self.um_home.clone(),
                source,
            })?;
        let directory = self.directory();
        create_private_directory_if_absent(&directory).map_err(|source| {
            RunnerHomeError::CreateDirectory {
                path: directory.clone(),
                source,
            }
        })?;
        ensure_private_directory(&directory).map_err(|source| {
            RunnerHomeError::UnsafeDirectory {
                path: directory.clone(),
                source,
            }
        })?;
        let work_root = self.work_root();
        create_private_directory_if_absent(&work_root).map_err(|source| {
            RunnerHomeError::CreateDirectory {
                path: work_root,
                source,
            }
        })?;
        let config_path = self.config_path();
        if self.existing_configuration()?.is_none() {
            self.publish_configuration(&directory, &config_path)?;
        }
        let config = load_operator_config(&config_path).map_err(|source| {
            RunnerHomeError::InvalidConfiguration {
                path: config_path.clone(),
                source,
            }
        })?;
        self.refuse_state_conflict(&config_path, config.runner_state_path)?;
        Ok(config_path)
    }

    /// The path of the home's operator configuration when anything is there,
    /// `None` when nothing is, judged without following a link. Creates
    /// nothing.
    pub fn existing_configuration(&self) -> Result<Option<PathBuf>, RunnerHomeError> {
        let path = self.config_path();
        match fs::symlink_metadata(&path) {
            Ok(_) => Ok(Some(path)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(RunnerHomeError::InspectConfiguration { path, source }),
        }
    }

    /// The control socket a command that only reads the home asks: the one
    /// its configuration names, or `runner/control.sock` when it holds none.
    /// Creates nothing (REQ-RUNNERENROLL-112).
    pub fn configured_control_socket_path(&self) -> Result<PathBuf, RunnerHomeError> {
        match self.existing_configuration()? {
            Some(path) => load_control_socket_path(&path)
                .map_err(|source| RunnerHomeError::InvalidConfiguration { path, source }),
            None => Ok(self.control_socket_path()),
        }
    }

    /// Writes the generated configuration to a private temporary file in
    /// `runner/` and renames it to `config_path` only if nothing is there; a
    /// start that finds one there has lost the race to another start, whose
    /// configuration it then reads. The temporary file is removed on every
    /// path.
    fn publish_configuration(
        &self,
        directory: &Path,
        config_path: &Path,
    ) -> Result<(), RunnerHomeError> {
        let failure = |stage| {
            move |source| RunnerHomeError::PublishConfiguration {
                path: config_path.to_owned(),
                stage,
                source,
            }
        };
        let document = OperatorConfig {
            schema_version: SCHEMA_VERSION,
            deployment_mode: DeploymentMode::Production,
            runner_state_path: self.state_path(),
            control_socket_path: self.control_socket_path(),
            work_root: self.work_root(),
        };
        let bytes = json_document(&document)
            .map_err(io::Error::from)
            .map_err(failure(PublicationStage::Encode))?;
        let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary = directory.join(format!(
            "{CONFIG_TEMPORARY_PREFIX}.{}.{sequence}",
            std::process::id()
        ));
        let mut file =
            create_new_private_file(&temporary).map_err(failure(PublicationStage::Create))?;
        let written = file
            .write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(failure(PublicationStage::Write));
        drop(file);
        let published = written.and_then(|()| {
            match rustix::fs::renameat_with(
                CWD,
                &temporary,
                CWD,
                config_path,
                RenameFlags::NOREPLACE,
            ) {
                Ok(()) => Ok(true),
                Err(rustix::io::Errno::EXIST) => Ok(false),
                Err(errno) => Err(failure(PublicationStage::Rename)(errno.into())),
            }
        });
        if !matches!(published, Ok(true)) {
            let _ = fs::remove_file(&temporary);
        }
        published?;
        sync_directory_io(directory).map_err(failure(PublicationStage::SyncDirectory))
    }

    /// Refuses a configuration whose `runnerStatePath` is not the home's
    /// `runner/state.json` while anything is there, as in a home that was
    /// moved or copied: a start would otherwise find no state where the
    /// configuration points and enroll the host a second time. The two are
    /// compared after resolving links, never by spelling.
    fn refuse_state_conflict(
        &self,
        config_path: &Path,
        configured: PathBuf,
    ) -> Result<(), RunnerHomeError> {
        let home_state = self.state_path();
        match fs::symlink_metadata(&home_state) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(RunnerHomeError::InspectState {
                    path: home_state,
                    source,
                });
            }
        }
        match (resolved(&configured), resolved(&home_state)) {
            (Some(configured), Some(home)) if configured == home => Ok(()),
            _ => Err(RunnerHomeError::StateConflict {
                config: config_path.to_owned(),
                configured,
                home: home_state,
            }),
        }
    }
}

/// Creates `path` with mode `0700` when nothing is there and leaves whatever
/// is there as it is: a start that finds it created by another start
/// concurrently goes on.
fn create_private_directory_if_absent(path: &Path) -> io::Result<()> {
    match DirBuilder::new().mode(DIRECTORY_MODE).create(path) {
        Ok(()) => fs::set_permissions(path, Permissions::from_mode(DIRECTORY_MODE)),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

/// `path` with its parent's links resolved, the file name kept as it is.
fn resolved(path: &Path) -> Option<PathBuf> {
    let name = path.file_name()?;
    let parent = fs::canonicalize(path.parent()?).ok()?;
    Some(parent.join(name))
}

/// The step at which publishing the generated configuration failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationStage {
    Encode,
    Create,
    Write,
    Rename,
    SyncDirectory,
}

impl fmt::Display for PublicationStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Encode => "encode runner operator configuration",
            Self::Create => "create temporary runner operator configuration for",
            Self::Write => "write temporary runner operator configuration for",
            Self::Rename => "publish runner operator configuration",
            Self::SyncDirectory => "sync runner home after publishing",
        })
    }
}

/// Why the runner home cannot be used as it stands. Each variant names the
/// path it concerns; a failed I/O call keeps its error.
#[derive(Debug)]
pub enum RunnerHomeError {
    /// The `um` home cannot be made absolute.
    Resolve { path: PathBuf, source: io::Error },
    /// The `um` home cannot be created.
    CreateUmHome { path: PathBuf, source: io::Error },
    /// `runner/` or `runner/work/` cannot be created.
    CreateDirectory { path: PathBuf, source: io::Error },
    /// `runner/` is not a directory of the effective user with mode `0700`.
    UnsafeDirectory {
        path: PathBuf,
        source: EnrollmentError,
    },
    /// `runner/config.json` cannot be inspected.
    InspectConfiguration { path: PathBuf, source: io::Error },
    /// The generated configuration cannot be published.
    PublishConfiguration {
        path: PathBuf,
        stage: PublicationStage,
        source: io::Error,
    },
    /// `runner/config.json` is not an operator configuration.
    InvalidConfiguration {
        path: PathBuf,
        source: EnrollmentError,
    },
    /// `runner/state.json` cannot be inspected.
    InspectState { path: PathBuf, source: io::Error },
    /// The configuration at `config` names the state `configured` while a
    /// state exists at the home's `home`.
    StateConflict {
        config: PathBuf,
        configured: PathBuf,
        home: PathBuf,
    },
}

impl fmt::Display for RunnerHomeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolve { path, source } => {
                write!(formatter, "resolve um home {}: {source}", path.display())
            }
            Self::CreateUmHome { path, source } => {
                write!(formatter, "create um home {}: {source}", path.display())
            }
            Self::CreateDirectory { path, source } => write!(
                formatter,
                "create runner home directory {}: {source}",
                path.display()
            ),
            Self::UnsafeDirectory { path, source } => {
                write!(formatter, "use runner home {}: {source}", path.display())
            }
            Self::InspectConfiguration { path, source } => write!(
                formatter,
                "inspect runner operator configuration {}: {source}",
                path.display()
            ),
            Self::PublishConfiguration {
                path,
                stage,
                source,
            } => write!(formatter, "{stage} {}: {source}", path.display()),
            Self::InvalidConfiguration { path, source } => write!(
                formatter,
                "load runner operator configuration {}: {source}",
                path.display()
            ),
            Self::InspectState { path, source } => write!(
                formatter,
                "inspect runner state {}: {source}",
                path.display()
            ),
            Self::StateConflict {
                config,
                configured,
                home,
            } => write!(
                formatter,
                "runner operator configuration {} names runner state {} while runner state exists at {}",
                config.display(),
                configured.display(),
                home.display()
            ),
        }
    }
}

impl Error for RunnerHomeError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::os::unix::fs::{MetadataExt, symlink};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::super::tests::{OPERATOR_CONFIG_SCHEMA, assert_json_matches_schema};
    use super::*;

    /// One entry of a directory tree as initialization could change it.
    #[derive(Debug, Eq, PartialEq)]
    struct Entry {
        path: PathBuf,
        mode: u32,
        inode: u64,
        content: Content,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum Content {
        Directory,
        File(Vec<u8>),
        Link(PathBuf),
    }

    /// Every name, mode, inode and byte under `root`, `root` included, links
    /// not followed.
    fn snapshot(root: &Path) -> Vec<Entry> {
        let mut entries = Vec::new();
        let mut pending = vec![root.to_owned()];
        while let Some(path) = pending.pop() {
            let metadata = fs::symlink_metadata(&path).unwrap();
            let content = if metadata.file_type().is_dir() {
                let mut children = fs::read_dir(&path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>();
                children.sort();
                pending.extend(children);
                Content::Directory
            } else if metadata.file_type().is_symlink() {
                Content::Link(fs::read_link(&path).unwrap())
            } else {
                Content::File(fs::read(&path).unwrap())
            };
            entries.push(Entry {
                path,
                mode: metadata.mode(),
                inode: metadata.ino(),
                content,
            });
        }
        entries
    }

    fn mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().mode() & 0o7777
    }

    fn names(directory: &Path) -> BTreeSet<String> {
        fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    fn layout_names() -> BTreeSet<String> {
        [CONFIG_FILE, WORK_DIRECTORY].map(str::to_owned).into()
    }

    /// `path` spelled relative to the working directory.
    fn relative_to_working_directory(path: &Path) -> PathBuf {
        let working_directory = std::env::current_dir().unwrap();
        let mut relative = PathBuf::new();
        for _ in working_directory.components().skip(1) {
            relative.push("..");
        }
        relative.join(path.strip_prefix("/").unwrap())
    }

    fn create_private_directory(path: &Path) {
        fs::create_dir_all(path).unwrap();
        fs::set_permissions(path, Permissions::from_mode(0o700)).unwrap();
    }

    fn write_private_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, Permissions::from_mode(0o600)).unwrap();
    }

    /// A `development` configuration naming `state` and the home's own socket
    /// and work root.
    fn write_development_config(home: &RunnerHome, state: &Path) {
        write_private_file(
            &home.config_path(),
            &serde_json::to_vec_pretty(&serde_json::json!({
                "schemaVersion": 1,
                "deploymentMode": "development",
                "runnerStatePath": state,
                "controlSocketPath": home.control_socket_path(),
                "workRoot": home.work_root()
            }))
            .unwrap(),
        );
    }

    /// A home whose `um` home, `runner/` and `runner/work/` exist and are
    /// private.
    fn prepared_home(temporary: &tempfile::TempDir) -> RunnerHome {
        let home = RunnerHome::new(&temporary.path().join("um")).unwrap();
        for directory in [
            temporary.path().join("um"),
            home.directory(),
            home.work_root(),
        ] {
            create_private_directory(&directory);
        }
        home
    }

    // REQ-RUNNERENROLL-110
    #[test]
    fn initializing_an_empty_um_home_creates_a_private_layout_and_a_generated_configuration() {
        let temporary = tempfile::tempdir().unwrap();
        let relative = relative_to_working_directory(&temporary.path().join("relative"));
        assert!(relative.is_relative());
        for um_home in [temporary.path().join("absolute"), relative] {
            let home = RunnerHome::new(&um_home).unwrap();

            let config_path = home.initialize().unwrap();

            let absolute = std::path::absolute(&um_home).unwrap();
            assert_eq!(home.directory(), absolute.join("runner"));
            assert_eq!(config_path, home.config_path());
            for directory in [absolute.clone(), home.directory(), home.work_root()] {
                assert!(fs::symlink_metadata(&directory).unwrap().is_dir());
                assert_eq!(mode(&directory), 0o700, "{}", directory.display());
            }
            assert!(fs::symlink_metadata(&config_path).unwrap().is_file());
            assert_eq!(mode(&config_path), 0o600);
            assert_json_matches_schema(
                "runner-operator-config-v1",
                OPERATOR_CONFIG_SCHEMA,
                &fs::read(&config_path).unwrap(),
            );
            let config = load_operator_config(&config_path).unwrap();
            assert_eq!(config.schema_version, 1);
            assert_eq!(config.deployment_mode, DeploymentMode::Production);
            assert_eq!(config.runner_state_path, home.state_path());
            assert_eq!(config.control_socket_path, home.control_socket_path());
            assert_eq!(config.work_root, home.work_root());
            for path in [
                &config.runner_state_path,
                &config.control_socket_path,
                &config.work_root,
            ] {
                assert!(path.is_absolute(), "{}", path.display());
            }
            // No state, journal, lock, socket or temporary file.
            assert_eq!(names(&home.directory()), layout_names());
            assert!(names(&home.work_root()).is_empty());
        }
    }

    // INV-RUNNER-25, REQ-RUNNERENROLL-110
    #[test]
    fn initialization_keeps_every_name_mode_inode_and_byte_that_exists() {
        let temporary = tempfile::tempdir().unwrap();
        let home = RunnerHome::new(&temporary.path().join("um")).unwrap();
        home.initialize().unwrap();
        let before = snapshot(temporary.path());

        assert_eq!(home.initialize().unwrap(), home.config_path());
        assert_eq!(snapshot(temporary.path()), before);

        // A home a runner already uses: its own configuration, state, lock,
        // journal and work, and directories whose modes initialization would
        // not have chosen.
        let temporary = tempfile::tempdir().unwrap();
        let home = prepared_home(&temporary);
        fs::set_permissions(temporary.path().join("um"), Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(home.work_root(), Permissions::from_mode(0o750)).unwrap();
        write_development_config(&home, &home.state_path());
        write_private_file(&home.state_path(), b"{\"state\": \"kept\"}\n");
        write_private_file(&home.directory().join(".runner-state.lock"), b"");
        write_private_file(&home.directory().join(".runner-enrollment.json"), b"{}");
        write_private_file(&home.work_root().join("retained.json"), b"retained");
        let before = snapshot(temporary.path());

        assert_eq!(home.initialize().unwrap(), home.config_path());
        assert_eq!(snapshot(temporary.path()), before);
    }

    // REQ-RUNNERENROLL-110, INV-RUNNER-25
    #[test]
    fn concurrent_initializations_of_one_home_all_read_the_one_published_configuration() {
        const STARTS: usize = 8;
        let temporary = tempfile::tempdir().unwrap();
        let home = RunnerHome::new(&temporary.path().join("um")).unwrap();
        let barrier = Arc::new(Barrier::new(STARTS));

        let starts = (0..STARTS)
            .map(|_| {
                let home = home.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    home.initialize()
                })
            })
            .collect::<Vec<_>>();

        for start in starts {
            assert_eq!(start.join().unwrap().unwrap(), home.config_path());
        }
        assert_eq!(names(&home.directory()), layout_names());
        assert_eq!(
            load_operator_config(&home.config_path())
                .unwrap()
                .runner_state_path,
            home.state_path()
        );
    }

    // REQ-RUNNERENROLL-111
    #[test]
    fn a_runner_directory_that_is_not_private_is_refused_and_left_as_it_is() {
        type Prepare = fn(&Path);
        let cases: [(&str, Prepare); 3] = [
            ("regular file", |runner| {
                write_private_file(runner, b"not a directory");
            }),
            ("link to a directory", |runner| {
                let target = runner.with_file_name("elsewhere");
                create_private_directory(&target);
                symlink(&target, runner).unwrap();
            }),
            ("directory of mode 0755", |runner| {
                fs::create_dir(runner).unwrap();
                fs::set_permissions(runner, Permissions::from_mode(0o755)).unwrap();
            }),
        ];
        for (case, prepare) in cases {
            let temporary = tempfile::tempdir().unwrap();
            let home = RunnerHome::new(&temporary.path().join("um")).unwrap();
            create_private_directory(&temporary.path().join("um"));
            prepare(&home.directory());
            let before = snapshot(temporary.path());

            let error = home.initialize().unwrap_err();

            assert!(
                matches!(&error, RunnerHomeError::UnsafeDirectory { path, .. } if *path == home.directory()),
                "{case}: {error:?}"
            );
            assert!(
                error
                    .to_string()
                    .contains(&home.directory().display().to_string()),
                "{case}: {error}"
            );
            assert_eq!(snapshot(temporary.path()), before, "{case}");
        }
    }

    // REQ-RUNNERENROLL-111
    #[test]
    fn a_configuration_that_is_not_an_operator_configuration_is_refused_and_kept() {
        type Prepare = fn(&RunnerHome);
        let cases: [(&str, Prepare); 3] = [
            ("link", |home| {
                let target = home.directory().join("elsewhere.json");
                write_development_config(home, &home.state_path());
                fs::rename(home.config_path(), &target).unwrap();
                symlink(&target, home.config_path()).unwrap();
            }),
            ("directory", |home| {
                create_private_directory(&home.config_path());
            }),
            ("malformed document", |home| {
                write_private_file(&home.config_path(), b"{\"schemaVersion\": 1");
            }),
        ];
        for (case, prepare) in cases {
            let temporary = tempfile::tempdir().unwrap();
            let home = prepared_home(&temporary);
            prepare(&home);
            let before = snapshot(temporary.path());

            let error = home.initialize().unwrap_err();

            assert!(
                matches!(
                    &error,
                    RunnerHomeError::InvalidConfiguration {
                        path,
                        source: EnrollmentError::InvalidConfig,
                    } if *path == home.config_path()
                ),
                "{case}: {error:?}"
            );
            assert_eq!(snapshot(temporary.path()), before, "{case}");
        }
    }

    // REQ-RUNNERENROLL-111
    #[test]
    fn a_configuration_naming_another_state_while_the_homes_state_exists_is_a_conflict() {
        let temporary = tempfile::tempdir().unwrap();
        let home = prepared_home(&temporary);
        let elsewhere = temporary.path().join("elsewhere").join("state.json");
        write_development_config(&home, &elsewhere);
        write_private_file(&home.state_path(), b"{}");
        let before = snapshot(temporary.path());

        let error = home.initialize().unwrap_err();

        assert!(
            matches!(
                &error,
                RunnerHomeError::StateConflict { config, configured, home: state }
                    if *config == home.config_path()
                        && *configured == elsewhere
                        && *state == home.state_path()
            ),
            "{error:?}"
        );
        let message = error.to_string();
        for path in [&elsewhere, &home.state_path()] {
            assert!(message.contains(&path.display().to_string()), "{message}");
        }
        assert_eq!(snapshot(temporary.path()), before);

        // With nothing at the home's state path the configuration is used as
        // written.
        fs::remove_file(home.state_path()).unwrap();
        assert_eq!(home.initialize().unwrap(), home.config_path());

        // The home's own state, named through a link to the home, is no
        // conflict.
        let alias = temporary.path().join("alias");
        symlink(temporary.path().join("um"), &alias).unwrap();
        write_development_config(&home, &alias.join("runner").join("state.json"));
        write_private_file(&home.state_path(), b"{}");
        assert_eq!(home.initialize().unwrap(), home.config_path());
    }

    // REQ-RUNNERENROLL-109, REQ-RUNNERENROLL-112
    #[test]
    fn reading_a_home_answers_its_configured_socket_or_its_layouts_and_creates_nothing() {
        let temporary = tempfile::tempdir().unwrap();
        let home = RunnerHome::new(&temporary.path().join("um")).unwrap();

        assert!(home.existing_configuration().unwrap().is_none());
        assert_eq!(
            home.configured_control_socket_path().unwrap(),
            home.control_socket_path()
        );
        assert!(names(temporary.path()).is_empty());

        let home = prepared_home(&temporary);
        let socket = temporary.path().join("elsewhere.sock");
        write_private_file(
            &home.config_path(),
            &serde_json::to_vec(&serde_json::json!({
                "schemaVersion": 1,
                "deploymentMode": "production",
                "runnerStatePath": home.state_path(),
                "controlSocketPath": socket,
                "workRoot": home.work_root()
            }))
            .unwrap(),
        );
        assert_eq!(
            home.existing_configuration().unwrap(),
            Some(home.config_path())
        );
        assert_eq!(home.configured_control_socket_path().unwrap(), socket);
    }
}
