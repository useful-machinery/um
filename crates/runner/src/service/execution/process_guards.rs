use super::*;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use rustix::fs::{AtFlags, FileType, Mode, OFlags, fstat, openat, renameat, statat, unlinkat};

use crate::service::recovery::{RecoveryHost, SystemRecoveryHost};
use serde::{Deserialize, Serialize};

const GUARDS_FILE: &str = "process-guards-v1.json";
const RECOVERY_GUARDS_FILE: &str = "process-guards-v2.json";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(in crate::service) struct HostBoot {
    pub(super) host_binding: String,
    pub(super) os_boot_id: String,
}

impl HostBoot {
    pub(in crate::service) fn inspect(
        host_binding: &str,
        host: &dyn RecoveryHost,
    ) -> Result<Self, ProcessGuardStoreError> {
        let os_boot_id = host.os_boot_id().map_err(|_| ProcessGuardStoreError)?;
        let os_boot_id = os_boot_id.trim_end_matches(['\r', '\n']).to_owned();
        if host_binding.len() != 32
            || !host_binding.bytes().all(|c| c.is_ascii_hexdigit())
            || !canonical_uuid(&os_boot_id)
        {
            return Err(ProcessGuardStoreError);
        }
        Ok(Self {
            host_binding: host_binding.to_owned(),
            os_boot_id,
        })
    }
}

fn canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == b'-'
            } else {
                c.is_ascii_digit() || (b'a'..=b'f').contains(&c)
            }
        })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredJournal {
    schema_version: u8,
    records: Vec<StoredGuardV2>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredGuardV2 {
    guard_id: String,
    host_binding: String,
    os_boot_id: String,
    process_group_id: i32,
    leader_start_identity: String,
    lifecycle: StoredLifecycle,
}

const MAX_GUARDS_BYTES: u64 = 1024 * 1024;
const NOFOLLOW: i32 = rustix::fs::OFlags::NOFOLLOW.bits().cast_signed();

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredGuard {
    id: String,
    group: i32,
    leader_start_identity: String,
    lifecycle: StoredLifecycle,
    #[serde(skip)]
    os_boot_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredLifecycle {
    Prepared,
    Released,
    Quiesced,
}

impl From<GuardLifecycle> for StoredLifecycle {
    fn from(value: GuardLifecycle) -> Self {
        match value {
            GuardLifecycle::Prepared => Self::Prepared,
            GuardLifecycle::Released => Self::Released,
            GuardLifecycle::Quiesced => Self::Quiesced,
        }
    }
}

impl From<StoredLifecycle> for GuardLifecycle {
    fn from(value: StoredLifecycle) -> Self {
        match value {
            StoredLifecycle::Prepared => Self::Prepared,
            StoredLifecycle::Released => Self::Released,
            StoredLifecycle::Quiesced => Self::Quiesced,
        }
    }
}

// Records are committed before a child is allowed to leave its launch guard.
// The parent directory is runner-private, not the execution workspace. A failed
// registration prevents release; later write failures keep ownership visible.
struct GuardJournal {
    directory: File,
    private: PathBuf,
    binding: Option<HostBoot>,
    host: Arc<dyn RecoveryHost>,
}

impl GuardJournal {
    fn current_boot(&self) -> Option<Result<HostBoot, ProcessGuardStoreError>> {
        self.binding
            .as_ref()
            .map(|binding| HostBoot::inspect(&binding.host_binding, self.host.as_ref()))
    }

    fn open(private: &Path, host: Arc<dyn RecoveryHost>) -> Result<Self, ProcessGuardStoreError> {
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::DIRECTORY.bits().cast_signed() | NOFOLLOW)
            .open(private)
            .map_err(|_| ProcessGuardStoreError)?;
        let metadata = directory.metadata().map_err(|_| ProcessGuardStoreError)?;
        if !metadata.is_dir()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.permissions().mode() & 0o7777 != 0o700
        {
            return Err(ProcessGuardStoreError);
        }
        let named = fs::symlink_metadata(private).map_err(|_| ProcessGuardStoreError)?;
        if named.dev() != metadata.dev() || named.ino() != metadata.ino() {
            return Err(ProcessGuardStoreError);
        }
        Ok(Self {
            directory,
            private: private.to_owned(),
            binding: None,
            host,
        })
    }

    fn binding_matches(&self) -> Result<(), ProcessGuardStoreError> {
        let held = self
            .directory
            .metadata()
            .map_err(|_| ProcessGuardStoreError)?;
        let named = fs::symlink_metadata(&self.private).map_err(|_| ProcessGuardStoreError)?;
        if held.dev() != named.dev() || held.ino() != named.ino() || !named.is_dir() {
            return Err(ProcessGuardStoreError);
        }
        Ok(())
    }

    fn create(
        private: &Path,
        binding: Option<HostBoot>,
        host: Arc<dyn RecoveryHost>,
    ) -> Result<Self, ProcessGuardStoreError> {
        let mut journal = Self::open(private, host)?;
        journal.binding = binding;
        journal.binding_matches()?;
        let other = if journal.binding.is_some() {
            GUARDS_FILE
        } else {
            RECOVERY_GUARDS_FILE
        };
        if !matches!(
            statat(&journal.directory, other, AtFlags::SYMLINK_NOFOLLOW),
            Err(rustix::io::Errno::NOENT)
        ) {
            return Err(ProcessGuardStoreError);
        }
        let name = journal.name();
        match statat(&journal.directory, name, AtFlags::SYMLINK_NOFOLLOW) {
            Err(error) if error == rustix::io::Errno::NOENT => {}
            _ => return Err(ProcessGuardStoreError),
        }
        // Do not replace a file that appears between the absence check and
        // creation. A partial initial write after a crash is an invalid journal,
        // not evidence that it is safe to start an ordinary new owner.
        let mut file = File::from(
            openat(
                &journal.directory,
                name,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            )
            .map_err(|_| ProcessGuardStoreError)?,
        );
        let initial = if journal.binding.is_some() {
            b"{\"schemaVersion\":2,\"records\":[]}".as_slice()
        } else {
            b"[]".as_slice()
        };
        file.write_all(initial)
            .map_err(|_| ProcessGuardStoreError)?;
        file.sync_all().map_err(|_| ProcessGuardStoreError)?;
        journal
            .directory
            .sync_all()
            .map_err(|_| ProcessGuardStoreError)?;
        journal.binding_matches()?;
        Ok(journal)
    }

    fn name(&self) -> &'static str {
        if self.binding.is_some() {
            RECOVERY_GUARDS_FILE
        } else {
            GUARDS_FILE
        }
    }

    fn persist(
        &self,
        records: &BTreeMap<String, GuardRecord>,
    ) -> Result<(), ProcessGuardStoreError> {
        // A missing/corrupt prior image cannot be repaired by a new writer.
        self.read()?;
        let values = records
            .iter()
            .map(|(id, record)| StoredGuard {
                id: id.clone(),
                group: record.identity.process_group_id(),
                leader_start_identity: record.identity.leader_start_identity().to_owned(),
                lifecycle: record.lifecycle.into(),
                os_boot_id: record.os_boot_id.clone(),
            })
            .collect::<Vec<_>>();
        let contents = if let Some(binding) = &self.binding {
            let records = records
                .iter()
                .map(|(guard_id, record)| {
                    Ok(StoredGuardV2 {
                        guard_id: guard_id.clone(),
                        host_binding: binding.host_binding.clone(),
                        os_boot_id: record.os_boot_id.clone().ok_or(ProcessGuardStoreError)?,
                        process_group_id: record.identity.process_group_id(),
                        leader_start_identity: record.identity.leader_start_identity().to_owned(),
                        lifecycle: record.lifecycle.into(),
                    })
                })
                .collect::<Result<Vec<_>, ProcessGuardStoreError>>()?;
            serde_json::to_vec(&StoredJournal {
                schema_version: 2,
                records,
            })
        } else {
            serde_json::to_vec(&values)
        }
        .map_err(|_| ProcessGuardStoreError)?;
        if u64::try_from(contents.len()).map_err(|_| ProcessGuardStoreError)? > MAX_GUARDS_BYTES {
            return Err(ProcessGuardStoreError);
        }
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| ProcessGuardStoreError)?;
        self.binding_matches()?;
        let temporary = format!(
            ".process-guards-{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(nonce)
        );
        let mut file = File::from(
            openat(
                &self.directory,
                temporary.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            )
            .map_err(|_| ProcessGuardStoreError)?,
        );
        let written = (|| {
            file.write_all(&contents)
                .map_err(|_| ProcessGuardStoreError)?;
            file.sync_all().map_err(|_| ProcessGuardStoreError)?;
            // All mutations stay on the authenticated directory handle, even if
            // its pathname or an ancestor is rebound while a write is in flight.
            self.binding_matches()?;
            renameat(
                &self.directory,
                temporary.as_str(),
                &self.directory,
                self.name(),
            )
            .map_err(|_| ProcessGuardStoreError)?;
            self.directory
                .sync_all()
                .map_err(|_| ProcessGuardStoreError)?;
            self.binding_matches()
        })();
        if written.is_err() {
            let _ = unlinkat(&self.directory, temporary.as_str(), AtFlags::empty());
        }
        written
    }

    fn read(&self) -> Result<BTreeMap<String, GuardRecord>, ProcessGuardStoreError> {
        self.binding_matches()?;
        let other = if self.binding.is_some() {
            GUARDS_FILE
        } else {
            RECOVERY_GUARDS_FILE
        };
        if !matches!(
            statat(&self.directory, other, AtFlags::SYMLINK_NOFOLLOW),
            Err(rustix::io::Errno::NOENT)
        ) {
            return Err(ProcessGuardStoreError);
        }
        let file = File::from(
            openat(
                &self.directory,
                self.name(),
                OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| ProcessGuardStoreError)?,
        );
        let metadata = fstat(&file).map_err(|_| ProcessGuardStoreError)?;
        let named = statat(&self.directory, self.name(), AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| ProcessGuardStoreError)?;
        if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile
            || metadata.st_uid != rustix::process::geteuid().as_raw()
            || i64::from(metadata.st_mode) & 0o7777 != 0o600
            || metadata.st_nlink != 1
            || metadata.st_dev != named.st_dev
            || metadata.st_ino != named.st_ino
        {
            return Err(ProcessGuardStoreError);
        }
        let mut bytes = Vec::new();
        file.take(MAX_GUARDS_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ProcessGuardStoreError)?;
        if u64::try_from(bytes.len()).map_err(|_| ProcessGuardStoreError)? > MAX_GUARDS_BYTES {
            return Err(ProcessGuardStoreError);
        }
        let values: Vec<StoredGuard> = if let Some(binding) = &self.binding {
            let journal: StoredJournal =
                serde_json::from_slice(&bytes).map_err(|_| ProcessGuardStoreError)?;
            if journal.schema_version != 2 {
                return Err(ProcessGuardStoreError);
            }
            journal
                .records
                .into_iter()
                .map(|record| {
                    if record.host_binding != binding.host_binding
                        || !canonical_uuid(&record.os_boot_id)
                    {
                        return Err(ProcessGuardStoreError);
                    }
                    Ok(StoredGuard {
                        id: record.guard_id,
                        group: record.process_group_id,
                        leader_start_identity: record.leader_start_identity,
                        lifecycle: record.lifecycle,
                        os_boot_id: Some(record.os_boot_id),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            serde_json::from_slice(&bytes).map_err(|_| ProcessGuardStoreError)?
        };
        let mut records = BTreeMap::new();
        for value in values {
            let group =
                rustix::process::Pid::from_raw(value.group).ok_or(ProcessGuardStoreError)?;
            let identity = AuthenticatedProcessGroup::new(group, value.leader_start_identity)
                .ok_or(ProcessGuardStoreError)?;
            let id = value.id;
            if id.is_empty()
                || records
                    .insert(
                        id,
                        GuardRecord {
                            identity,
                            lifecycle: value.lifecycle.into(),
                            os_boot_id: value.os_boot_id,
                        },
                    )
                    .is_some()
            {
                return Err(ProcessGuardStoreError);
            }
        }
        let current = statat(&self.directory, self.name(), AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|_| ProcessGuardStoreError)?;
        if metadata.st_dev != current.st_dev || metadata.st_ino != current.st_ino {
            return Err(ProcessGuardStoreError);
        }
        self.binding_matches()?;
        Ok(records)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GuardLifecycle {
    Prepared,
    Released,
    Quiesced,
}

#[derive(Clone)]
pub(super) struct GuardRecord {
    identity: AuthenticatedProcessGroup,
    lifecycle: GuardLifecycle,
    os_boot_id: Option<String>,
}

pub(super) trait GuardProcessControl: Send + Sync {
    fn observe(&self, identity: &AuthenticatedProcessGroup) -> ProcessIdentityObservation;
    fn terminate(&self, identity: &AuthenticatedProcessGroup) -> AuthenticatedSignalResult;
}

pub(super) struct SystemGuardProcessControl;

impl GuardProcessControl for SystemGuardProcessControl {
    fn observe(&self, identity: &AuthenticatedProcessGroup) -> ProcessIdentityObservation {
        SystemProcessIdentityInspector.observe(identity)
    }

    fn terminate(&self, identity: &AuthenticatedProcessGroup) -> AuthenticatedSignalResult {
        terminate_authenticated_process_group(identity)
    }
}

pub(super) struct ProcessGuardState {
    next_id: u64,
    control: Arc<dyn GuardProcessControl>,
    records: BTreeMap<String, GuardRecord>,
    journal: Option<Arc<GuardJournal>>,
    journal_failed: bool,
    forced_containment_started: bool,
    #[cfg(test)]
    quiescence_fixture: Option<Arc<std::sync::atomic::AtomicBool>>,
}

#[derive(Clone)]
pub(in crate::service) struct AssignmentProcessGuards {
    state: Arc<Mutex<ProcessGuardState>>,
    // Serializes journal transitions without blocking containment-visible state.
    write_gate: Arc<Mutex<()>>,
}

impl AssignmentProcessGuards {
    pub(in crate::service) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ProcessGuardState {
                next_id: 1,
                control: Arc::new(SystemGuardProcessControl),
                records: BTreeMap::new(),
                journal: None,
                journal_failed: false,
                forced_containment_started: false,
                #[cfg(test)]
                quiescence_fixture: None,
            })),
            write_gate: Arc::new(Mutex::new(())),
        }
    }

    pub(in crate::service) fn durable(private: &Path) -> Result<Self, ProcessGuardStoreError> {
        GuardJournal::create(private, None, Arc::new(SystemRecoveryHost))?;
        Self::recover(private)
    }

    pub(in crate::service) fn durable_recovery(
        private: &Path,
        binding: HostBoot,
        host: Arc<dyn RecoveryHost>,
    ) -> Result<Self, ProcessGuardStoreError> {
        GuardJournal::create(private, Some(binding.clone()), Arc::clone(&host))?;
        Self::recover_with(private, Some(binding), host)
    }

    // Call only after authenticating the retaining assignment's exact workspace
    // and private directory. Never recover a prior owner's guards for a new root.
    pub(in crate::service) fn recover(private: &Path) -> Result<Self, ProcessGuardStoreError> {
        let host: Arc<dyn RecoveryHost> = Arc::new(SystemRecoveryHost);
        let journal = GuardJournal::open(private, Arc::clone(&host))?;
        if statat(
            &journal.directory,
            RECOVERY_GUARDS_FILE,
            AtFlags::SYMLINK_NOFOLLOW,
        )
        .is_ok()
        {
            let binding = host.host_binding().map_err(|_| ProcessGuardStoreError)?;
            return Self::recover_with(
                private,
                Some(HostBoot::inspect(
                    binding.trim_end_matches(['\r', '\n']),
                    host.as_ref(),
                )?),
                host,
            );
        }
        Self::recover_with(private, None, host)
    }

    pub(in crate::service) fn recover_recovery(
        private: &Path,
        binding: HostBoot,
        host: Arc<dyn RecoveryHost>,
    ) -> Result<Self, ProcessGuardStoreError> {
        Self::recover_with(private, Some(binding), host)
    }

    fn recover_with(
        private: &Path,
        binding: Option<HostBoot>,
        host: Arc<dyn RecoveryHost>,
    ) -> Result<Self, ProcessGuardStoreError> {
        let mut journal = GuardJournal::open(private, host)?;
        journal.binding = binding;
        let records = journal.read()?;
        let mut sequences = std::collections::BTreeSet::new();
        let mut highest = 0_u64;
        for id in records.keys() {
            let (prefix, sequence_text) = id.rsplit_once(':').ok_or(ProcessGuardStoreError)?;
            let (step, action_text) = prefix.rsplit_once(':').ok_or(ProcessGuardStoreError)?;
            let action = action_text
                .parse::<u64>()
                .map_err(|_| ProcessGuardStoreError)?;
            let sequence = sequence_text
                .parse::<u64>()
                .map_err(|_| ProcessGuardStoreError)?;
            if step.is_empty()
                || action.to_string() != action_text
                || sequence == 0
                || sequence.to_string() != sequence_text
                || !sequences.insert(sequence)
            {
                return Err(ProcessGuardStoreError);
            }
            highest = highest.max(sequence);
        }
        let next_id = highest.checked_add(1).ok_or(ProcessGuardStoreError)?;
        let guards = Self::new();
        {
            let mut state = guards.lock();
            state.records = records;
            state.next_id = next_id;
            state.journal = Some(Arc::new(journal));
        }
        Ok(guards)
    }

    pub(in crate::service) fn recovery_journal(&self) -> bool {
        self.lock()
            .journal
            .as_ref()
            .is_some_and(|journal| journal.binding.is_some())
    }

    pub(in crate::service) fn registry(&self, guarded: bool) -> ProcessGuardRegistry {
        if guarded {
            let store: Arc<dyn DurableProcessGuardStore> = Arc::new(self.clone());
            ProcessGuardRegistry::durable(store)
        } else {
            ProcessGuardRegistry::default()
        }
    }

    pub(super) fn begin_forced_containment(&self) {
        let (identities, control) = {
            let mut state = self.lock();
            state.forced_containment_started = true;
            let current = state
                .journal
                .as_ref()
                .and_then(|journal| journal.current_boot())
                .and_then(Result::ok);
            let identities = state
                .records
                .values()
                .filter(|record| {
                    record.lifecycle != GuardLifecycle::Quiesced
                        && record.os_boot_id.as_ref().is_none_or(|boot| {
                            current
                                .as_ref()
                                .is_some_and(|identity| boot == &identity.os_boot_id)
                        })
                })
                .map(|record| record.identity.clone())
                .collect::<Vec<_>>();
            (identities, Arc::clone(&state.control))
        };
        for identity in identities {
            let _ = control.terminate(&identity);
        }
    }

    // Observe each registered identity once. The decision and the identities used in
    // the report must describe the same observation, not two racing inspections.
    pub(super) fn quiescence_snapshot(&self) -> (bool, Vec<String>) {
        let state = self.lock();
        let current = state
            .journal
            .as_ref()
            .and_then(|journal| journal.current_boot());
        let boot_unknown = current.as_ref().is_some_and(Result::is_err);
        let surviving = state
            .records
            .iter()
            .filter(|(_, record)| {
                record.lifecycle != GuardLifecycle::Quiesced
                    && record.os_boot_id.as_ref().is_none_or(|boot| {
                        current
                            .as_ref()
                            .and_then(|result| result.as_ref().ok())
                            .is_some_and(|identity| boot == &identity.os_boot_id)
                    })
                    && !matches!(
                        state.control.observe(&record.identity),
                        ProcessIdentityObservation::Absent
                    )
            })
            .map(|(id, record)| {
                format!("{id} ({:?})", record.identity)
                    .chars()
                    .take(512)
                    .collect::<String>()
            })
            .take(255)
            .collect::<Vec<_>>();
        #[cfg(test)]
        if let Some(quiescent) = &state.quiescence_fixture {
            return (quiescent.load(Ordering::Acquire), surviving);
        }
        (
            surviving.is_empty() && !state.journal_failed && !boot_unknown,
            surviving,
        )
    }

    pub(in crate::service) fn is_quiescent(&self) -> bool {
        self.quiescence_snapshot().0
    }

    #[cfg(test)]
    pub(super) fn use_control(&self, control: Arc<dyn GuardProcessControl>) {
        self.lock().control = control;
    }

    #[cfg(test)]
    pub(in crate::service) fn use_quiescence_fixture(
        &self,
        quiescent: Arc<std::sync::atomic::AtomicBool>,
    ) {
        self.lock().quiescence_fixture = Some(quiescent);
    }

    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, ProcessGuardState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(super) fn forced_containment_started(&self) -> bool {
        self.lock().forced_containment_started
    }
}

/// Proof for every guard recorded by the retaining assignment. A released or
/// quiesced journal entry is still inspected: settlement is not proof that its
/// process group stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) struct RetainedQuiescence {
    pub(in crate::service) recorded: u64,
    pub(in crate::service) terminated: u64,
    pub(in crate::service) absent: u64,
}

impl AssignmentProcessGuards {
    /// Requires authentication of the retaining assignment and its private
    /// directory by the workspace owner. Inspect *all* groups before signaling
    /// any of them so corrupt or unclassifiable ownership never causes a signal.
    /// The caller runs this blocking inspection on the preparation worker.
    pub(in crate::service) fn quiesce_retained_chain(
        guards: &[Self],
    ) -> Result<RetainedQuiescence, ProcessGuardStoreError> {
        if guards.is_empty() {
            return Err(ProcessGuardStoreError);
        }
        let mut identities = Vec::new();
        for guard in guards {
            let state = guard.lock();
            if state.journal_failed || state.journal.is_none() {
                return Err(ProcessGuardStoreError);
            }
            let control = Arc::clone(&state.control);
            let current_boot = state
                .journal
                .as_ref()
                .and_then(|journal| journal.current_boot())
                .transpose()?;
            identities.extend(state.records.values().map(|record| {
                (
                    record.identity.clone(),
                    Arc::clone(&control),
                    record
                        .os_boot_id
                        .as_ref()
                        .zip(current_boot.as_ref())
                        .map(|(recorded, current)| recorded != &current.os_boot_id)
                        .unwrap_or(false),
                )
            }));
        }
        let recorded = u64::try_from(identities.len()).map_err(|_| ProcessGuardStoreError)?;
        let mut exact = Vec::new();
        let mut absent = 0_u64;
        for (identity, control, old_boot) in identities {
            if old_boot {
                absent += 1;
                continue;
            }
            match control.observe(&identity) {
                ProcessIdentityObservation::Exact { .. } => exact.push((identity, control)),
                ProcessIdentityObservation::Absent => absent += 1,
                ProcessIdentityObservation::Unavailable => return Err(ProcessGuardStoreError),
            }
        }
        let mut terminated = 0_u64;
        for (identity, control) in &exact {
            match control.terminate(identity) {
                AuthenticatedSignalResult::Signalled => terminated += 1,
                AuthenticatedSignalResult::Absent => absent += 1,
                AuthenticatedSignalResult::Unavailable => return Err(ProcessGuardStoreError),
            }
        }
        // Authentication is repeated by the signal implementation immediately
        // before sending; it never signals a numeric PGID on an uncertain match.
        for _ in 0..80 {
            let mut remaining = false;
            for (identity, control) in &exact {
                match control.observe(identity) {
                    ProcessIdentityObservation::Absent => {}
                    ProcessIdentityObservation::Exact { .. } => remaining = true,
                    ProcessIdentityObservation::Unavailable => return Err(ProcessGuardStoreError),
                }
            }
            if !remaining {
                return Ok(RetainedQuiescence {
                    recorded,
                    terminated,
                    absent,
                });
            }
            um_support::sleep(std::time::Duration::from_millis(25));
        }
        Err(ProcessGuardStoreError)
    }

    fn persist(
        &self,
        updated: &BTreeMap<String, GuardRecord>,
    ) -> Result<(), ProcessGuardStoreError> {
        let journal = self.lock().journal.clone();
        if let Some(journal) = journal
            && let Err(error) = journal.persist(updated)
        {
            // Rename may succeed before sync fails. Never overwrite uncertain evidence.
            self.lock().journal_failed = true;
            return Err(error);
        }
        Ok(())
    }

    fn serialize(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl DurableProcessGuardStore for AssignmentProcessGuards {
    fn register(
        &self,
        step: &str,
        action_id: u64,
        identity: &AuthenticatedProcessGroup,
    ) -> Result<String, ProcessGuardStoreError> {
        let _write = self.serialize();
        let mut state = self.lock();
        if state.journal_failed
            || state.forced_containment_started
            || state.records.values().any(|record| {
                record.lifecycle != GuardLifecycle::Quiesced && record.identity == *identity
            })
        {
            return Err(ProcessGuardStoreError);
        }
        let id = format!("{step}:{action_id}:{}", state.next_id);
        state.next_id = state.next_id.checked_add(1).ok_or(ProcessGuardStoreError)?;
        // A stalled write cannot hide a pending launch from forced containment.
        let os_boot_id = state
            .journal
            .as_ref()
            .and_then(|journal| journal.binding.as_ref())
            .map(|binding| binding.os_boot_id.clone());
        state.records.insert(
            id.clone(),
            GuardRecord {
                identity: identity.clone(),
                lifecycle: GuardLifecycle::Prepared,
                os_boot_id,
            },
        );
        let updated = state.records.clone();
        drop(state);
        self.persist(&updated)?;
        if self.lock().forced_containment_started {
            return Err(ProcessGuardStoreError);
        }
        Ok(id)
    }

    fn mark_released(&self, guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        let _write = self.serialize();
        let state = self.lock();
        if state.forced_containment_started || state.journal_failed {
            return Err(ProcessGuardStoreError);
        }
        let mut updated = state.records.clone();
        let record = updated.get_mut(guard_id).ok_or(ProcessGuardStoreError)?;
        match record.lifecycle {
            GuardLifecycle::Prepared => record.lifecycle = GuardLifecycle::Released,
            GuardLifecycle::Released => return Ok(()),
            GuardLifecycle::Quiesced => return Err(ProcessGuardStoreError),
        }
        drop(state);
        self.persist(&updated)?;
        let mut state = self.lock();
        state.records = updated;
        if state.forced_containment_started {
            return Err(ProcessGuardStoreError);
        }
        Ok(())
    }

    fn mark_quiesced(&self, guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        let _write = self.serialize();
        let state = self.lock();
        if state.journal_failed {
            return Err(ProcessGuardStoreError);
        }
        let mut updated = state.records.clone();
        let record = updated.get_mut(guard_id).ok_or(ProcessGuardStoreError)?;
        if record.lifecycle == GuardLifecycle::Quiesced {
            return Ok(());
        }
        record.lifecycle = GuardLifecycle::Quiesced;
        drop(state);
        self.persist(&updated)?;
        self.lock().records = updated;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingControl(AtomicUsize);

    impl GuardProcessControl for CountingControl {
        fn observe(&self, _: &AuthenticatedProcessGroup) -> ProcessIdentityObservation {
            ProcessIdentityObservation::Exact {
                leader: um_execution::LeaderState::Running,
            }
        }

        fn terminate(&self, _: &AuthenticatedProcessGroup) -> AuthenticatedSignalResult {
            self.0.fetch_add(1, Ordering::SeqCst);
            AuthenticatedSignalResult::Signalled
        }
    }

    fn private_directory() -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::tempdir().expect("isolated guard directory");
        let private = temp.path().join("private");
        fs::create_dir(&private).expect("private directory");
        fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).expect("private mode");
        (temp, private)
    }

    fn identity() -> AuthenticatedProcessGroup {
        AuthenticatedProcessGroup::new(
            rustix::process::Pid::from_raw(12345).expect("fixture group"),
            "boot-scoped-start-identity".to_owned(),
        )
        .expect("authenticated group")
    }

    struct QuiescenceControl {
        unknown: bool,
        signals: AtomicUsize,
        alive: std::sync::atomic::AtomicBool,
    }

    impl GuardProcessControl for QuiescenceControl {
        fn observe(&self, _: &AuthenticatedProcessGroup) -> ProcessIdentityObservation {
            if self.unknown {
                ProcessIdentityObservation::Unavailable
            } else if self.alive.load(Ordering::SeqCst) {
                ProcessIdentityObservation::Exact {
                    leader: um_execution::LeaderState::Running,
                }
            } else {
                ProcessIdentityObservation::Absent
            }
        }

        fn terminate(&self, _: &AuthenticatedProcessGroup) -> AuthenticatedSignalResult {
            self.signals.fetch_add(1, Ordering::SeqCst);
            self.alive.store(false, Ordering::SeqCst);
            AuthenticatedSignalResult::Signalled
        }
    }

    #[test]
    fn retained_recovery_proves_each_group_before_a_claim_can_be_published() {
        let (_temp, private) = private_directory();
        let original = AssignmentProcessGuards::durable(&private).expect("durable guards");
        original
            .register("step", 1, &identity())
            .expect("retain identity");
        drop(original);
        let recovered = AssignmentProcessGuards::recover(&private).expect("reopen guards");
        let unknown = Arc::new(QuiescenceControl {
            unknown: true,
            signals: AtomicUsize::new(0),
            alive: std::sync::atomic::AtomicBool::new(true),
        });
        recovered.use_control(unknown.clone());
        assert!(
            AssignmentProcessGuards::quiesce_retained_chain(std::slice::from_ref(&recovered))
                .is_err()
        );
        assert_eq!(unknown.signals.load(Ordering::SeqCst), 0);
        let exact = Arc::new(QuiescenceControl {
            unknown: false,
            signals: AtomicUsize::new(0),
            alive: std::sync::atomic::AtomicBool::new(true),
        });
        recovered.use_control(exact.clone());
        assert_eq!(
            AssignmentProcessGuards::quiesce_retained_chain(&[recovered])
                .expect("all groups absent"),
            RetainedQuiescence {
                recorded: 1,
                terminated: 1,
                absent: 0
            }
        );
        assert_eq!(exact.signals.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unclassifiable_earlier_owner_prevents_all_signals_in_a_chain() {
        let (_latest, latest_private) = private_directory();
        let (_earlier, earlier_private) = private_directory();
        let latest = AssignmentProcessGuards::durable(&latest_private).expect("latest journal");
        latest
            .register("step", 1, &identity())
            .expect("latest group");
        let earlier = AssignmentProcessGuards::durable(&earlier_private).expect("earlier journal");
        earlier
            .register("step", 1, &identity())
            .expect("earlier group");
        let exact = Arc::new(QuiescenceControl {
            unknown: false,
            signals: AtomicUsize::new(0),
            alive: std::sync::atomic::AtomicBool::new(true),
        });
        let unknown = Arc::new(QuiescenceControl {
            unknown: true,
            signals: AtomicUsize::new(0),
            alive: std::sync::atomic::AtomicBool::new(true),
        });
        latest.use_control(exact.clone());
        earlier.use_control(unknown.clone());
        assert!(AssignmentProcessGuards::quiesce_retained_chain(&[latest, earlier]).is_err());
        assert_eq!(exact.signals.load(Ordering::SeqCst), 0);
        assert_eq!(unknown.signals.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn containment_reaches_registered_groups_while_journal_writer_is_stalled() {
        let (_temp, private) = private_directory();
        let guards = AssignmentProcessGuards::durable(&private).expect("durable guards");
        let control = Arc::new(CountingControl(AtomicUsize::new(0)));
        guards.use_control(control.clone());
        guards.register("node", 3, &identity()).expect("register");
        // The writer holds only the serialization gate, never the identity lock.
        let stalled_writer = guards.serialize();
        let (sent, received) = tokio::sync::oneshot::channel();
        let force = guards.clone();
        let thread = std::thread::spawn(move || {
            force.begin_forced_containment();
            sent.send(()).expect("containment acknowledgement");
        });
        // The timeout is only an anti-hang bound; the acknowledgement, not elapsed
        // time, establishes that containment completed while the gate was held.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");
        let result = runtime.block_on(crate::service::test_support::with_watchdog(received));
        drop(stalled_writer);
        thread.join().expect("containment thread");
        result
            .expect("containment must not wait for journal serialization")
            .expect("containment acknowledgement");
        assert_eq!(control.0.load(Ordering::SeqCst), 1);
        assert!(guards.register("next", 4, &identity()).is_err());
    }

    #[test]
    fn journal_rebinding_never_writes_into_replacement_directory() {
        let (temp, private) = private_directory();
        let (_other_temp, other_private) = private_directory();
        let guards = AssignmentProcessGuards::durable(&private).expect("durable guards");
        let original = temp.path().join("original");
        fs::rename(&private, &original).expect("move private directory");
        std::os::unix::fs::symlink(&other_private, &private).expect("rebind private directory");
        assert!(guards.register("node", 3, &identity()).is_err());
        assert!(!other_private.join(GUARDS_FILE).exists());
        assert!(
            GuardJournal::open(&original, Arc::new(SystemRecoveryHost))
                .expect("original handle")
                .read()
                .expect("original journal")
                .is_empty()
        );
    }

    #[test]
    fn retained_guard_recovery_preserves_identity_lifecycle_and_sequence() {
        let (_temp, private) = private_directory();
        let guards = AssignmentProcessGuards::durable(&private).expect("create journal");
        let id = guards.register("node", 3, &identity()).expect("register");
        guards.mark_released(&id).expect("release");
        drop(guards);
        let recovered = AssignmentProcessGuards::recover(&private).expect("reopen journal");
        assert_eq!(recovered.lock().records[&id].identity, identity());
        assert_eq!(
            recovered.lock().records[&id].lifecycle,
            GuardLifecycle::Released
        );
        let next_identity = AuthenticatedProcessGroup::new(
            rustix::process::Pid::from_raw(12346).expect("fixture group"),
            "another-start-identity".to_owned(),
        )
        .expect("authenticated group");
        let next = recovered
            .register("next", 4, &next_identity)
            .expect("continue sequence");
        assert_eq!(next, "next:4:2");
        let control = Arc::new(CountingControl(AtomicUsize::new(0)));
        recovered.use_control(control.clone());
        recovered.begin_forced_containment();
        assert_eq!(control.0.load(Ordering::SeqCst), 2);
        assert!(recovered.register("next", 4, &identity()).is_err());
        assert!(!recovered.is_quiescent());
    }

    #[test]
    fn corrupt_or_rebound_retained_journal_never_recovers_as_empty() {
        let (temp, private) = private_directory();
        AssignmentProcessGuards::durable(&private).expect("create journal");
        let journal = private.join(GUARDS_FILE);
        for contents in [
            b"not-json".as_slice(),
            br#"[{"id":"node:3:1","group":12345,"leader_start_identity":"boot-scoped-start-identity","lifecycle":"prepared"},{"id":"next:4:1","group":12345,"leader_start_identity":"boot-scoped-start-identity","lifecycle":"released"}]"#,
        ] {
            fs::write(&journal, contents).expect("corrupt journal");
            assert!(AssignmentProcessGuards::recover(&private).is_err());
        }
        fs::remove_file(&journal).expect("remove corrupt journal");
        std::os::unix::fs::symlink(temp.path().join("outside"), &journal)
            .expect("substitute journal");
        assert!(AssignmentProcessGuards::recover(&private).is_err());
    }

    #[test]
    fn recovery_journal_never_signals_a_recycled_group_after_os_reboot() {
        use crate::service::recovery::host_fixture::fixture_host;

        let (_temp, private) = private_directory();
        let binding = HostBoot::inspect("a".repeat(32).as_str(), fixture_host().as_ref())
            .expect("boot identity");
        let guards =
            AssignmentProcessGuards::durable_recovery(&private, binding.clone(), fixture_host())
                .expect("create");
        let id = guards
            .register("node", 3, &identity())
            .expect("committed guard");
        guards.mark_released(&id).expect("released");
        drop(guards);
        let journal_path = private.join(RECOVERY_GUARDS_FILE);
        let mut bytes: serde_json::Value =
            serde_json::from_slice(&fs::read(&journal_path).expect("journal")).expect("json");
        let replacement = if binding.os_boot_id.starts_with('0') {
            "1"
        } else {
            "0"
        };
        let older = format!("{replacement}{}", &binding.os_boot_id[1..]);
        bytes["records"][0]["osBootId"] = serde_json::json!(older);
        fs::write(&journal_path, serde_json::to_vec(&bytes).expect("encode"))
            .expect("fixture boot transition");
        let recovered =
            AssignmentProcessGuards::recover_recovery(&private, binding.clone(), fixture_host())
                .expect("reopen");
        let control = Arc::new(QuiescenceControl {
            unknown: true,
            signals: AtomicUsize::new(0),
            alive: std::sync::atomic::AtomicBool::new(true),
        });
        recovered.use_control(control.clone());
        assert_eq!(
            AssignmentProcessGuards::quiesce_retained_chain(&[recovered])
                .expect("older boot absent"),
            RetainedQuiescence {
                recorded: 1,
                terminated: 0,
                absent: 1
            }
        );
        assert_eq!(control.signals.load(Ordering::SeqCst), 0);
        bytes["schemaVersion"] = serde_json::json!(3);
        fs::write(&journal_path, serde_json::to_vec(&bytes).expect("encode"))
            .expect("unknown revision");
        assert!(
            AssignmentProcessGuards::recover_recovery(&private, binding, fixture_host()).is_err()
        );
    }

    #[test]
    fn process_identity_is_committed_before_guard_release_and_never_lost_on_write_failure() {
        let (_temp, private) = private_directory();
        let guards = AssignmentProcessGuards::durable(&private).expect("create durable guards");
        let identity = identity();
        let id = guards.register("node", 3, &identity).expect("register");
        let journal = guards.lock();
        let record = journal
            .journal
            .as_ref()
            .expect("durable journal")
            .read()
            .expect("read committed group");
        assert_eq!(record[&id].identity, identity);
        assert_eq!(record[&id].lifecycle, GuardLifecycle::Prepared);
        drop(journal);
        guards.mark_released(&id).expect("release launch guard");
        let journal = guards.lock();
        assert_eq!(
            journal
                .journal
                .as_ref()
                .expect("journal")
                .read()
                .expect("released image")[&id]
                .lifecycle,
            GuardLifecycle::Released
        );
        drop(journal);
        let path = private.join(GUARDS_FILE);
        fs::remove_file(&path).expect("inject journal write failure");
        fs::create_dir(&path).expect("block atomic replacement");
        assert!(guards.mark_quiesced(&id).is_err());
        assert!(!guards.is_quiescent());
        assert!(guards.register("other", 4, &identity).is_err());
        assert_eq!(
            guards.lock().records[&id].lifecycle,
            GuardLifecycle::Released
        );
    }
}
