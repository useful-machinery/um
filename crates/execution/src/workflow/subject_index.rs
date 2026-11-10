//! A definition's subject index (REQ-WFDEF-28): `workflows/<id>/subjects/<subject>` under the
//! `um` home names the run that holds the definition's slot for that subject. `claim` takes
//! the slot under an exclusive lock on `subjects/` and keeps the lock until the claim is
//! dropped, so the caller creates the claimant's run directory while no other claim can read
//! the slot; `SubjectClaim::take` and `name` split the claim so a successor's directory is
//! published under the lock before the slot names it (INV-ENGINE-26). The slot's state is decided from the holder's `run.json` and its controls alone
//! (INV-ENGINE-24): busy while the record reads `running` or `waiting`, settled once it reads
//! `exited` until a `control.release` is appended, and free when no file exists, when the
//! named run's directory is gone, or after the release.

use std::fmt;
use std::io;
use std::os::fd::OwnedFd;
use std::path::PathBuf;

use rustix::fs::{AtFlags, FlockOperation, Mode, RenameFlags, flock, mkdirat, statat};
use rustix::io::Errno;

use super::definition_store::WorkflowHome;
use super::local_run::{
    LocalRunDirectoryError, PRIVATE_DIRECTORY, file_error, home_definitions, is_run_id,
    open_directory_at, open_run_root, path_error, read_regular_file, read_run_record,
    sync_directory, write_through_private,
};
use super::private_staging::open_directory_path;
use super::run_record::{RunState, WorkflowRunRecord};
use super::tree_copy::file_locator;
use super::wait::{CONTROL_RELEASE, read_controls};

const SUBJECTS_DIRECTORY: &str = "subjects";
const SLOT_TEMPORARY_FILE: &str = "slot.tmp";
const MAXIMUM_SUBJECT_LENGTH: usize = 128;

/// Whether `value` has the subject form `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`
/// (REQ-WFDEF-28), so it is its slot's file name as it stands.
pub fn is_subject(value: &str) -> bool {
    let mut bytes = value.bytes();
    value.len() <= MAXIMUM_SUBJECT_LENGTH
        && bytes
            .next()
            .is_some_and(|first| first.is_ascii_alphanumeric())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// The run a slot file names, as the index reads it: its canonical directory, its record and
/// whether its controls hold a `control.release`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotHolder {
    pub run: String,
    pub directory: PathBuf,
    pub record: WorkflowRunRecord,
    pub released: bool,
}

impl SlotHolder {
    /// The refusal a claim that does not name this holder as its predecessor meets, or
    /// `None` when the holder leaves the slot free.
    pub fn refusal(&self) -> Option<SubjectError> {
        let run = self.run.clone();
        match self.record.state {
            RunState::Running | RunState::Waiting => Some(SubjectError::Busy { run }),
            RunState::Exited if !self.released => Some(SubjectError::Settled { run }),
            RunState::Exited => None,
        }
    }
}

impl SlotHolder {
    /// Every definition's holder of the slot for `subject` under `home`, in definition-id
    /// order, read without a lock as `slot` reads one (REQ-LOCALRUN-180): the runs the
    /// subject addresses.
    pub fn of_subject(home: &WorkflowHome, subject: &str) -> Result<Vec<Self>, SubjectError> {
        if !is_subject(subject) {
            return Err(SubjectError::Invalid);
        }
        let mut holders = Vec::new();
        for definition in home_definitions(home)? {
            holders.extend(slot(home, &definition, subject)?);
        }
        Ok(holders)
    }
}

/// A claim that took a definition's slot. The lock on `subjects/` is held until it drops.
#[derive(Debug)]
pub struct SubjectClaim {
    /// The open `subjects/`, which holds the lock.
    subjects: OwnedFd,
    subject: String,
}

impl SubjectClaim {
    /// Locks definition `definition`'s `subjects/` and decides the slot for `subject` as
    /// `check` decides it for a claimant naming `predecessor`, writing nothing: the holder
    /// stays the holder until `name` writes the slot, so a caller that dies between the two
    /// leaves the slot as it found it. The lock is held until the claim drops.
    pub fn take(
        home: &WorkflowHome,
        definition: &str,
        subject: &str,
        predecessor: Option<&str>,
    ) -> Result<Self, SubjectError> {
        if !is_subject(subject) {
            return Err(SubjectError::Invalid);
        }
        let directory = home.definition_directory(definition);
        let definition_root = open_directory_path(&directory)
            .map_err(|source| path_error(directory, "open directory", source))?;
        let subjects = create_private_directory(&definition_root, SUBJECTS_DIRECTORY)?;
        flock(&subjects, FlockOperation::LockExclusive)
            .map_err(|source| file_error(&definition_root, SUBJECTS_DIRECTORY, "lock", source))?;
        if let Some(holder) = read_slot(home, definition, &subjects, subject)?
            && let Some(refusal) = refused(&holder, predecessor)
        {
            return Err(refusal);
        }
        Ok(Self {
            subjects,
            subject: subject.to_owned(),
        })
    }

    /// Writes the slot naming `run`: `<subject>` is replaced with `run` and a newline, written
    /// whole under `subjects/.private/` and renamed into place. The caller names a run whose
    /// directory exists.
    pub fn name(&self, run: &str) -> Result<(), SubjectError> {
        if !is_run_id(run) {
            return Err(LocalRunDirectoryError::InvalidPath.into());
        }
        let private = create_private_directory(&self.subjects, PRIVATE_DIRECTORY)?;
        write_through_private(
            &private,
            SLOT_TEMPORARY_FILE,
            &self.subjects,
            &self.subject,
            format!("{run}\n").as_bytes(),
            Mode::RUSR | Mode::WUSR,
            RenameFlags::empty(),
        )?;
        sync_directory(&self.subjects)?;
        Ok(())
    }
}

/// Why a subject's slot was not read or taken.
#[derive(Debug)]
pub enum SubjectError {
    /// The value is outside the subject form.
    Invalid,
    /// The holder's record reads `running` or `waiting`.
    Busy { run: String },
    /// The holder's record reads `exited` and its controls hold no `control.release`.
    Settled { run: String },
    /// The index or the holder's run directory could not be read or written.
    Unavailable(LocalRunDirectoryError),
}

impl SubjectError {
    /// The refusal code `um workflow run create` reports (REQ-LOCALRUN-173), `None` for a
    /// failure of the store.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Invalid => Some("subject_invalid"),
            Self::Busy { .. } => Some("subject_busy"),
            Self::Settled { .. } => Some("subject_settled"),
            Self::Unavailable(_) => None,
        }
    }

    /// The id of the run that holds the slot.
    pub fn run(&self) -> Option<&str> {
        match self {
            Self::Busy { run } | Self::Settled { run } => Some(run),
            Self::Invalid | Self::Unavailable(_) => None,
        }
    }
}

impl fmt::Display for SubjectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid => formatter.write_str(
                "the subject is not 1 to 128 letters, digits, '.', '_' or '-' starting with a letter or digit",
            ),
            Self::Busy { run } => write!(formatter, "run {run} holds the subject and is live"),
            Self::Settled { run } => write!(
                formatter,
                "run {run} holds the subject and has exited without a release; release it with um workflow run release {run}"
            ),
            Self::Unavailable(error) => write!(formatter, "read the subject index: {error}"),
        }
    }
}

impl std::error::Error for SubjectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unavailable(error) => Some(error),
            _ => None,
        }
    }
}

impl From<LocalRunDirectoryError> for SubjectError {
    fn from(error: LocalRunDirectoryError) -> Self {
        Self::Unavailable(error)
    }
}

/// The run that holds definition `definition`'s slot for `subject`, read without the lock;
/// `None` when no slot file exists or the run it names is gone from the definition's
/// `runs/`.
pub fn slot(
    home: &WorkflowHome,
    definition: &str,
    subject: &str,
) -> Result<Option<SlotHolder>, SubjectError> {
    if !is_subject(subject) {
        return Err(SubjectError::Invalid);
    }
    let subjects = home
        .definition_directory(definition)
        .join(SUBJECTS_DIRECTORY);
    let subjects = match open_directory_path(&subjects) {
        Ok(subjects) => subjects,
        Err(Errno::NOENT) => return Ok(None),
        Err(source) => return Err(path_error(subjects, "open directory", source).into()),
    };
    Ok(read_slot(home, definition, &subjects, subject)?)
}

/// `Ok` when a claim for `subject` that names `predecessor` would take the slot now, else the
/// refusal; the slot is read without the lock, so `claim` decides again.
pub fn check(
    home: &WorkflowHome,
    definition: &str,
    subject: &str,
    predecessor: Option<&str>,
) -> Result<(), SubjectError> {
    match slot(home, definition, subject)? {
        Some(holder) => refused(&holder, predecessor).map_or(Ok(()), Err),
        None => Ok(()),
    }
}

/// Takes definition `definition`'s slot for `subject` for the run `run` (REQ-WFDEF-28):
/// `subjects/` is created owner-only on first use and locked exclusively, the slot is decided
/// as `check` decides it (`SubjectClaim::take`), and `<subject>` is replaced with `run` and a
/// newline, written whole under `subjects/.private/` and renamed into place
/// (`SubjectClaim::name`). The lock is held until the returned claim drops. A claimant whose
/// `predecessor` names the holder takes the slot in any state.
pub fn claim(
    home: &WorkflowHome,
    definition: &str,
    subject: &str,
    run: &str,
    predecessor: Option<&str>,
) -> Result<SubjectClaim, SubjectError> {
    if !is_run_id(run) {
        return Err(LocalRunDirectoryError::InvalidPath.into());
    }
    let claim = SubjectClaim::take(home, definition, subject, predecessor)?;
    claim.name(run)?;
    Ok(claim)
}

fn refused(holder: &SlotHolder, predecessor: Option<&str>) -> Option<SubjectError> {
    if predecessor == Some(holder.run.as_str()) {
        return None;
    }
    holder.refusal()
}

/// The holder the slot file `subject` under the open `subjects/` names.
fn read_slot(
    home: &WorkflowHome,
    definition: &str,
    subjects: &OwnedFd,
    subject: &str,
) -> Result<Option<SlotHolder>, LocalRunDirectoryError> {
    match statat(subjects, subject, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(_) => {}
        Err(Errno::NOENT) => return Ok(None),
        Err(source) => return Err(file_error(subjects, subject, "stat", source)),
    }
    let bytes = read_regular_file(subjects, subject)?;
    let run = std::str::from_utf8(&bytes)
        .ok()
        .and_then(|text| text.strip_suffix('\n'))
        .filter(|run| is_run_id(run))
        .ok_or_else(|| LocalRunDirectoryError::StateFile {
            path: file_locator(subjects, subject),
            operation: "validate",
            source: Box::new(LocalRunDirectoryError::StateInvalid),
        })?;
    let directory = home.runs_directory(definition).join(run);
    match std::fs::symlink_metadata(&directory) {
        Ok(_) => {}
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(path_error(directory, "inspect run directory", source)),
    }
    let (directory, root) = open_run_root(&directory)?;
    let record = read_run_record(&root)?;
    let released = record.state == RunState::Exited
        && read_controls(&directory)?
            .iter()
            .any(|control| control.event == CONTROL_RELEASE);
    Ok(Some(SlotHolder {
        run: run.to_owned(),
        directory,
        record,
        released,
    }))
}

/// Creates `name` under `parent` owner-only unless it exists, and opens it.
fn create_private_directory(
    parent: &OwnedFd,
    name: &str,
) -> Result<OwnedFd, LocalRunDirectoryError> {
    match mkdirat(parent, name, Mode::RWXU) {
        Ok(()) | Err(Errno::EXIST) => open_directory_at(parent, name),
        Err(source) => Err(file_error(parent, name, "create directory", source)),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::Path;

    use super::*;
    use crate::workflow::wait::{ControlEvent, append_control};

    const TREE: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    const DEFINITION: &str = "def_01jzzzzzzzzzzzzzzzzzzzzzzz";
    const OTHER_DEFINITION: &str = "def_01jyyyyyyyyyyyyyyyyyyyyyyy";

    /// A home holding two definition directories, as publishing leaves them.
    struct Fixture {
        _temporary: tempfile::TempDir,
        home: WorkflowHome,
    }

    impl Fixture {
        fn new() -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let root = temporary.path().to_owned();
            let home = WorkflowHome::from_lookup(|name| {
                (name == "UM_HOME").then(|| root.clone().into_os_string())
            })
            .unwrap();
            for definition in [DEFINITION, OTHER_DEFINITION] {
                fs::create_dir_all(home.runs_directory(definition)).unwrap();
            }
            Self {
                _temporary: temporary,
                home,
            }
        }

        /// A run of `definition` under its `runs/` whose record reads `state`.
        fn run(&self, definition: &str, state: RunState) -> String {
            let mut record = WorkflowRunRecord::new(
                Some((definition.to_owned(), 1)),
                TREE,
                "workflow.yaml",
                "operator",
            );
            record.subject = Some("AUTO-1".to_owned());
            record.state = state;
            let directory = self.home.runs_directory(definition).join(&record.id);
            fs::create_dir_all(directory.join(PRIVATE_DIRECTORY)).unwrap();
            fs::write(
                directory.join("run.json"),
                serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
            record.id
        }

        fn set_state(&self, definition: &str, run: &str, state: RunState) {
            let path = self
                .home
                .runs_directory(definition)
                .join(run)
                .join("run.json");
            let mut record: WorkflowRunRecord =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            record.state = state;
            fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
        }

        fn release(&self, definition: &str, run: &str) {
            let release = ControlEvent::new(CONTROL_RELEASE, run, "operator", None, None).unwrap();
            append_control(&self.home.runs_directory(definition).join(run), &release).unwrap();
        }

        fn claim(
            &self,
            definition: &str,
            run: &str,
            predecessor: Option<&str>,
        ) -> Result<(), SubjectError> {
            claim(&self.home, definition, "AUTO-1", run, predecessor).map(drop)
        }

        fn slot_file(&self, definition: &str) -> PathBuf {
            self.home
                .definition_directory(definition)
                .join(SUBJECTS_DIRECTORY)
                .join("AUTO-1")
        }

        fn holder(&self, definition: &str) -> String {
            fs::read_to_string(self.slot_file(definition)).unwrap()
        }
    }

    // REQ-WFDEF-28
    #[test]
    fn a_first_claim_writes_the_slot_owner_only() {
        let fixture = Fixture::new();
        let first = fixture.run(DEFINITION, RunState::Running);
        assert!(slot(&fixture.home, DEFINITION, "AUTO-1").unwrap().is_none());

        fixture.claim(DEFINITION, &first, None).unwrap();

        assert_eq!(fixture.holder(DEFINITION), format!("{first}\n"));
        let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&fixture.slot_file(DEFINITION)), 0o600);
        assert_eq!(mode(fixture.slot_file(DEFINITION).parent().unwrap()), 0o700);
        let holder = slot(&fixture.home, DEFINITION, "AUTO-1").unwrap().unwrap();
        assert_eq!(holder.run, first);
        assert_eq!(holder.record.id, first);
    }

    // REQ-WFDEF-28, INV-ENGINE-24: busy while the holder is live, settled once it exits,
    // free after a release, decided from the holder's record and controls alone.
    #[test]
    fn the_slot_follows_the_holders_record_and_controls() {
        let fixture = Fixture::new();
        let first = fixture.run(DEFINITION, RunState::Running);
        fixture.claim(DEFINITION, &first, None).unwrap();
        let second = fixture.run(DEFINITION, RunState::Running);

        for state in [RunState::Running, RunState::Waiting] {
            fixture.set_state(DEFINITION, &first, state);
            let refusal = fixture.claim(DEFINITION, &second, None).unwrap_err();
            assert!(
                matches!(&refusal, SubjectError::Busy { run } if *run == first),
                "{refusal:?}"
            );
            assert_eq!(refusal.code(), Some("subject_busy"));
            assert!(check(&fixture.home, DEFINITION, "AUTO-1", None).is_err());
        }

        fixture.set_state(DEFINITION, &first, RunState::Exited);
        let refusal = fixture.claim(DEFINITION, &second, None).unwrap_err();
        assert!(
            matches!(&refusal, SubjectError::Settled { run } if *run == first),
            "{refusal:?}"
        );
        assert_eq!(refusal.code(), Some("subject_settled"));
        assert_eq!(fixture.holder(DEFINITION), format!("{first}\n"));

        fixture.release(DEFINITION, &first);
        check(&fixture.home, DEFINITION, "AUTO-1", None).unwrap();
        fixture.claim(DEFINITION, &second, None).unwrap();
        assert_eq!(fixture.holder(DEFINITION), format!("{second}\n"));
    }

    // REQ-LOCALRUN-196, INV-ENGINE-26: a taken claim holds the lock and writes nothing until
    // it names the successor, so a successor whose creation dies after the take leaves the
    // predecessor as the holder and the subject busy, never a slot naming a missing run.
    #[test]
    fn a_taken_claim_names_the_slot_only_once_asked() {
        let fixture = Fixture::new();
        let first = fixture.run(DEFINITION, RunState::Waiting);
        fixture.claim(DEFINITION, &first, None).unwrap();
        let successor = fixture.run(DEFINITION, RunState::Running);

        let taken = SubjectClaim::take(&fixture.home, DEFINITION, "AUTO-1", Some(&first)).unwrap();
        assert_eq!(fixture.holder(DEFINITION), format!("{first}\n"));
        drop(taken);
        assert_eq!(fixture.holder(DEFINITION), format!("{first}\n"));
        let refusal = fixture.claim(DEFINITION, &successor, None).unwrap_err();
        assert!(matches!(&refusal, SubjectError::Busy { run } if *run == first));

        let taken = SubjectClaim::take(&fixture.home, DEFINITION, "AUTO-1", Some(&first)).unwrap();
        taken.name(&successor).unwrap();
        assert_eq!(fixture.holder(DEFINITION), format!("{successor}\n"));
        drop(taken);
        let holder = slot(&fixture.home, DEFINITION, "AUTO-1").unwrap().unwrap();
        assert_eq!(holder.run, successor);
        assert!(matches!(
            SubjectClaim::take(&fixture.home, DEFINITION, "AUTO-1", Some(&first)),
            Err(SubjectError::Busy { .. })
        ));
    }

    // REQ-WFDEF-28: a successor takes the slot over from its live predecessor.
    #[test]
    fn a_claimant_naming_the_holder_as_predecessor_takes_the_slot() {
        let fixture = Fixture::new();
        let first = fixture.run(DEFINITION, RunState::Waiting);
        fixture.claim(DEFINITION, &first, None).unwrap();
        let successor = fixture.run(DEFINITION, RunState::Running);

        check(&fixture.home, DEFINITION, "AUTO-1", Some(&first)).unwrap();
        fixture.claim(DEFINITION, &successor, Some(&first)).unwrap();
        assert_eq!(fixture.holder(DEFINITION), format!("{successor}\n"));
    }

    // REQ-WFDEF-28
    #[test]
    fn a_holder_whose_directory_is_gone_leaves_the_slot_free() {
        let fixture = Fixture::new();
        let first = fixture.run(DEFINITION, RunState::Running);
        fixture.claim(DEFINITION, &first, None).unwrap();
        fs::remove_dir_all(fixture.home.runs_directory(DEFINITION).join(&first)).unwrap();

        assert!(slot(&fixture.home, DEFINITION, "AUTO-1").unwrap().is_none());
        let second = fixture.run(DEFINITION, RunState::Running);
        fixture.claim(DEFINITION, &second, None).unwrap();
        assert_eq!(fixture.holder(DEFINITION), format!("{second}\n"));
    }

    // REQ-WFDEF-28
    #[test]
    fn a_value_outside_the_subject_form_is_invalid_and_writes_nothing() {
        let fixture = Fixture::new();
        let run = fixture.run(DEFINITION, RunState::Running);
        for subject in ["AUTO 1", "", "-AUTO-1", ".private", "a/b", &"A".repeat(129)] {
            let refusal = claim(&fixture.home, DEFINITION, subject, &run, None).unwrap_err();
            assert!(matches!(refusal, SubjectError::Invalid), "{subject}");
            assert_eq!(refusal.code(), Some("subject_invalid"));
        }
        assert!(
            !fixture
                .home
                .definition_directory(DEFINITION)
                .join(SUBJECTS_DIRECTORY)
                .exists()
        );
        assert!(is_subject(&"A".repeat(128)));
        assert!(is_subject("AUTO-1.retry_2"));
    }

    // REQ-LOCALRUN-180: a subject's holders are read from every definition's slot under the
    // home, in definition order, without a slot whose run is gone.
    #[test]
    fn a_subjects_holders_span_the_homes_definitions() {
        let fixture = Fixture::new();
        let runs = |subject| {
            SlotHolder::of_subject(&fixture.home, subject)
                .unwrap()
                .into_iter()
                .map(|holder| holder.run)
                .collect::<Vec<_>>()
        };
        assert!(runs("AUTO-1").is_empty());

        let first = fixture.run(DEFINITION, RunState::Exited);
        fixture.claim(DEFINITION, &first, None).unwrap();
        assert_eq!(runs("AUTO-1"), std::slice::from_ref(&first));

        let other = fixture.run(OTHER_DEFINITION, RunState::Waiting);
        fixture.claim(OTHER_DEFINITION, &other, None).unwrap();
        assert_eq!(runs("AUTO-1"), [other.clone(), first.clone()]);
        let holders = SlotHolder::of_subject(&fixture.home, "AUTO-1").unwrap();
        assert_eq!(
            holders[0].record.definition.as_deref(),
            Some(OTHER_DEFINITION)
        );
        assert_eq!(holders[0].record.state, RunState::Waiting);
        assert!(runs("AUTO-2").is_empty());

        fs::remove_dir_all(fixture.home.runs_directory(DEFINITION).join(&first)).unwrap();
        assert_eq!(runs("AUTO-1"), [other]);
        assert!(matches!(
            SlotHolder::of_subject(&fixture.home, "AUTO 1"),
            Err(SubjectError::Invalid)
        ));
    }

    // REQ-WFDEF-28: definitions share nothing.
    #[test]
    fn another_definitions_slot_for_the_subject_is_unaffected() {
        let fixture = Fixture::new();
        let first = fixture.run(DEFINITION, RunState::Running);
        fixture.claim(DEFINITION, &first, None).unwrap();

        let other = fixture.run(OTHER_DEFINITION, RunState::Running);
        fixture.claim(OTHER_DEFINITION, &other, None).unwrap();

        assert_eq!(fixture.holder(DEFINITION), format!("{first}\n"));
        assert_eq!(fixture.holder(OTHER_DEFINITION), format!("{other}\n"));
    }
}
