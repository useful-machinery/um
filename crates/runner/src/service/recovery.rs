//! Local-only Recovery V1 records. Nothing in this module grants Cloud authority or
//! advertises recovery. All entry points require the caller's exclusive work-root lease.
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

use base64::Engine;
use rustix::fs::{AtFlags, FileType, Mode, OFlags, fstat, openat, statat};
use serde::{Deserialize, Serialize};

use super::workspace::{AssignmentRoot, WorkRootAuthority};

const ROOT: &str = ".um-recovery-root-v1.json";
const MANIFEST: &str = ".um-recovery-manifest-v1.json";
const LIMIT: u64 = 1024 * 1024;
const MAX_GENERATION: u64 = 9_007_199_254_740_991;

#[derive(Debug)]
pub(super) enum RecoveryError {
    Identity(std::io::Error),
    Record(&'static str),
    Commit(std::io::Error),
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identity(cause) => write!(f, "recovery identity unavailable: {cause}"),
            Self::Record(stage) => write!(f, "recovery record invalid at {stage}"),
            Self::Commit(cause) => write!(f, "recovery record commit failed: {cause}"),
        }
    }
}
impl std::error::Error for RecoveryError {}

impl RecoveryError {
    pub(super) fn diagnostic(&self) -> String {
        let stage = match self {
            Self::Identity(_) => "host_identity".to_owned(),
            Self::Commit(_) => "record_commit".to_owned(),
            Self::Record(stage) => stage.to_ascii_lowercase().replace([' ', '/'], "_"),
        };
        let mut diagnostic = serde_json::json!({"errorType": self.error_type(), "stage": stage});
        if let Self::Identity(error) | Self::Commit(error) = self {
            let cause = match error.kind() {
                std::io::ErrorKind::NotFound => "not_found",
                std::io::ErrorKind::PermissionDenied => "permission_denied",
                std::io::ErrorKind::StorageFull => "storage_full",
                std::io::ErrorKind::Unsupported => "unsupported",
                std::io::ErrorKind::InvalidData => "invalid_data",
                _ => "io_error",
            };
            diagnostic["transportError"] = serde_json::json!(cause);
        }
        diagnostic.to_string()
    }

    pub(super) fn error_type(&self) -> &'static str {
        match self {
            Self::Identity(_) => "local_recovery_identity_unavailable",
            Self::Record(_) => "local_recovery_record_invalid",
            Self::Commit(_) => "local_recovery_commit_failed",
        }
    }
}

fn invalid(stage: &'static str) -> RecoveryError {
    RecoveryError::Record(stage)
}

pub(super) trait RecoveryHost: Send + Sync {
    fn host_binding(&self) -> Result<String, RecoveryError>;
    fn filesystem_id(&self, directory: &File) -> Result<String, RecoveryError>;
    fn os_boot_id(&self) -> std::io::Result<String>;
}

pub(super) struct SystemRecoveryHost;

impl RecoveryHost for SystemRecoveryHost {
    fn host_binding(&self) -> Result<String, RecoveryError> {
        fs::read_to_string("/etc/machine-id").map_err(RecoveryError::Identity)
    }

    fn filesystem_id(&self, directory: &File) -> Result<String, RecoveryError> {
        inspect_filesystem_id(directory)
    }

    fn os_boot_id(&self) -> std::io::Result<String> {
        fs::read_to_string("/proc/sys/kernel/random/boot_id")
    }
}

// A reboot changes boot_id but not machine-id, filesystem ID or the original
// directory's inode. In particular st_dev is deliberately not persisted: its
// number can change on remount. A copy on the same filesystem gets a new inode;
// a copy on another filesystem has a different filesystem ID. Unknown host or
// filesystem identity cannot be used for a recovery claim.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct RootBinding {
    pub(super) host_binding: String,
    filesystem_id: String,
    directory_inode: u64,
    location: PathBuf,
}

fn no_symlink_path(path: &Path) -> Result<(), RecoveryError> {
    if !path.is_absolute() {
        return Err(invalid("location"));
    }
    let mut cursor = PathBuf::new();
    for part in path.components() {
        match part {
            Component::RootDir | Component::Normal(_) => cursor.push(part),
            _ => return Err(invalid("location")),
        }
        if !fs::symlink_metadata(&cursor)
            .map_err(RecoveryError::Identity)?
            .is_dir()
        {
            return Err(invalid("location"));
        }
    }
    Ok(())
}

// libc keeps the fsid_t words private. Normalize their documented Linux
// Debug representation at this one host boundary instead of persisting that
// representation as a release-dependent durable identity. A changed upstream
// representation fails closed until a qualified reader supports it.
#[cfg(target_os = "linux")]
fn stable_linux_filesystem_id(debug: &str) -> Result<String, RecoveryError> {
    let words = debug
        .strip_prefix("fsid_t { __val: [")
        .and_then(|tail| tail.strip_suffix("] }"))
        .ok_or_else(|| invalid("filesystem identity"))?;
    let (a, b) = words
        .split_once(", ")
        .ok_or_else(|| invalid("filesystem identity"))?;
    let a: i32 = a.parse().map_err(|_| invalid("filesystem identity"))?;
    let b: i32 = b.parse().map_err(|_| invalid("filesystem identity"))?;
    Ok(format!("{a:08x}:{b:08x}"))
}

#[cfg(target_os = "linux")]
fn inspect_filesystem_id(directory: &File) -> Result<String, RecoveryError> {
    let fs_info = nix::sys::statfs::fstatfs(directory).map_err(|error| {
        RecoveryError::Identity(std::io::Error::from_raw_os_error(error as i32))
    })?;
    // Recovery identity is qualified only on Linux ext4. Other hosts retain
    // ordinary boot-local execution without advertising recovery.
    if fs_info.filesystem_type() != nix::sys::statfs::EXT4_SUPER_MAGIC {
        return Err(invalid("filesystem type"));
    }
    stable_linux_filesystem_id(format!("{:?}", fs_info.filesystem_id()).as_str())
}

#[cfg(not(target_os = "linux"))]
fn inspect_filesystem_id(_directory: &File) -> Result<String, RecoveryError> {
    Err(invalid("unsupported recovery host"))
}

impl RootBinding {
    pub(super) fn inspect(
        path: &Path,
        directory: &File,
        host: &dyn RecoveryHost,
    ) -> Result<Self, RecoveryError> {
        no_symlink_path(path)?;
        let host_binding = host.host_binding()?;
        let host_binding = host_binding.trim_end_matches(['\r', '\n']);
        if host_binding.len() != 32
            || host_binding.bytes().all(|byte| byte == b'0')
            || !host_binding.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(invalid("host binding"));
        }
        let metadata = directory.metadata().map_err(RecoveryError::Identity)?;
        let named = fs::symlink_metadata(path).map_err(RecoveryError::Identity)?;
        if metadata.ino() != named.ino() || metadata.dev() != named.dev() {
            return Err(invalid("root directory"));
        }
        let filesystem_id = host.filesystem_id(directory)?;
        Ok(Self {
            host_binding: host_binding.to_owned(),
            filesystem_id,
            directory_inode: metadata.ino(),
            location: path.to_owned(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CatalogEntry {
    workspace_id: String,
    origin_boot_id: String,
    origin_assignment_id: String,
    origin_run_id: String,
    origin_attempt_id: String,
    execution_root: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RootRecord {
    schema_version: u8,
    runner_id: String,
    work_root_id: String,
    installation_id: String,
    binding: RootBinding,
    catalog: Vec<CatalogEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct Claim {
    pub(super) claim_generation: u64,
    pub(super) assignment_id: String,
    pub(super) attempt_id: String,
    pub(super) runner_boot_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct Link {
    pub(super) boot_id: String,
    pub(super) assignment_id: String,
    pub(super) run_id: String,
    pub(super) attempt_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct Manifest {
    schema_version: u8,
    pub(super) runner_id: String,
    pub(super) work_root_id: String,
    pub(super) workspace_id: String,
    pub(super) binding: RootBinding,
    pub(super) execution_root: PathBuf,
    pub(super) lineage: Vec<Link>, // oldest first, including the origin
    pub(super) claim: Option<Claim>,
    pub(super) last_claim_generation: u64,
}

impl Manifest {
    fn validate(&self, root: &RootRecord, entry: &CatalogEntry) -> Result<(), RecoveryError> {
        let Some(origin) = self.lineage.first() else {
            return Err(invalid("lineage"));
        };
        if self.schema_version != 1
            || self.runner_id != root.runner_id
            || self.work_root_id != root.work_root_id
            || self.workspace_id != entry.workspace_id
            || self.binding != root.binding
            || self.execution_root != entry.execution_root
            || self.lineage.len() > 256
            || self.last_claim_generation > MAX_GENERATION
            || origin.boot_id != entry.origin_boot_id
            || origin.assignment_id != entry.origin_assignment_id
            || origin.run_id != entry.origin_run_id
            || origin.attempt_id != entry.origin_attempt_id
        {
            return Err(invalid("manifest binding"));
        }
        let mut seen = BTreeSet::new();
        for link in &self.lineage {
            if !um_runner_protocol::valid_boot_id(&link.boot_id)
                || !um_runner_protocol::valid_assignment_id(&link.assignment_id)
                || !um_runner_protocol::valid_run_id(&link.run_id)
                || !um_runner_protocol::valid_attempt_id(&link.attempt_id)
                || link.run_id != origin.run_id
                || !seen.insert(link.assignment_id.as_str())
            {
                return Err(invalid("lineage"));
            }
        }
        if let Some(claim) = &self.claim
            && (claim.claim_generation == 0
                || claim.claim_generation != self.last_claim_generation
                || !um_runner_protocol::valid_assignment_id(&claim.assignment_id)
                || !um_runner_protocol::valid_attempt_id(&claim.attempt_id)
                || !um_runner_protocol::valid_boot_id(&claim.runner_boot_id))
        {
            return Err(invalid("claim"));
        }
        Ok(())
    }
}

fn read_record<T: serde::de::DeserializeOwned>(dir: &File, name: &str) -> Result<T, RecoveryError> {
    let file = File::from(
        openat(
            dir,
            name,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| invalid("record open"))?,
    );
    let held = fstat(&file).map_err(|_| invalid("record stat"))?;
    let named = statat(dir, name, AtFlags::SYMLINK_NOFOLLOW).map_err(|_| invalid("record stat"))?;
    if FileType::from_raw_mode(held.st_mode) != FileType::RegularFile
        || held.st_uid != rustix::process::geteuid().as_raw()
        || held.st_mode & 0o7777 != 0o600
        || held.st_nlink != 1
        || held.st_dev != named.st_dev
        || held.st_ino != named.st_ino
        || held.st_size < 0
        || u64::try_from(held.st_size).map_err(|_| invalid("record size"))? > LIMIT
    {
        return Err(invalid("record ownership"));
    }
    let mut bytes = Vec::new();
    file.take(LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(RecoveryError::Identity)?;
    if bytes.len() as u64 > LIMIT {
        return Err(invalid("record size"));
    }
    let record = serde_json::from_slice(&bytes).map_err(|_| invalid("record schema"))?;
    let current =
        statat(dir, name, AtFlags::SYMLINK_NOFOLLOW).map_err(|_| invalid("record replaced"))?;
    if current.st_dev != held.st_dev || current.st_ino != held.st_ino {
        return Err(invalid("record replaced"));
    }
    Ok(record)
}

fn persist<T: Serialize>(
    dir: &File,
    name: &str,
    record: &T,
    replace: bool,
) -> Result<(), RecoveryError> {
    let bytes = serde_json::to_vec(record).map_err(|_| invalid("encode"))?;
    if bytes.len() as u64 > LIMIT {
        return Err(invalid("record size"));
    }
    let mut nonce = [0_u8; 16];
    getrandom::fill(&mut nonce).map_err(|_| invalid("nonce"))?;
    let temporary = format!(
        ".um-recovery-{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(nonce)
    );
    let mut file = File::from(
        openat(
            dir,
            temporary.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        )
        .map_err(|_| invalid("staging"))?,
    );
    let result = (|| {
        file.write_all(&bytes).map_err(RecoveryError::Commit)?;
        file.sync_all().map_err(RecoveryError::Commit)?;
        if replace {
            let existing: serde_json::Value = read_record(dir, name)?;
            if !existing.is_object() {
                return Err(invalid("prior record"));
            }
        } else if !matches!(
            statat(dir, name, AtFlags::SYMLINK_NOFOLLOW),
            Err(rustix::io::Errno::NOENT)
        ) {
            return Err(invalid("record already exists"));
        }
        rustix::fs::renameat_with(
            dir,
            temporary.as_str(),
            dir,
            name,
            if replace {
                rustix::fs::RenameFlags::empty()
            } else {
                rustix::fs::RenameFlags::NOREPLACE
            },
        )
        .map_err(|_| invalid("rename"))?;
        dir.sync_all().map_err(RecoveryError::Commit)
    })();
    if result.is_err() {
        let _ = rustix::fs::unlinkat(dir, temporary.as_str(), AtFlags::empty());
    }
    result
}

fn public_id(prefix: &str) -> String {
    format!(
        "{prefix}{}",
        ulid::Ulid::generate().to_string().to_ascii_lowercase()
    )
}

/// The authority's directory lock remains held throughout this store's lifetime.
/// A root without a V1 record may start new V1 work; historical directories are
/// never enumerated into the catalog or assigned identities.
#[derive(Clone)]
pub(super) struct Origin {
    boot_id: String,
    pub(super) assignment_id: String,
    run_id: String,
    attempt_id: String,
    execution_root: PathBuf,
}

impl Origin {
    pub(super) fn from_assignment(assignment: &AssignmentRoot, boot_id: &str) -> Self {
        let record = &assignment.attempt_record;
        Self {
            boot_id: boot_id.to_owned(),
            assignment_id: record.assignment_id.clone(),
            run_id: record.run_id.clone(),
            attempt_id: record.attempt_id.clone(),
            execution_root: assignment.execution.clone(),
        }
    }
}

pub(super) struct RecoveryStore {
    authority: WorkRootAuthority,
    runner_id: String,
    boot_id: String,
    binding: RootBinding,
    host: std::sync::Arc<dyn RecoveryHost>,
    writes: std::sync::Mutex<bool>, // poisoned after an uncertain commit
    cleanup_gate: std::sync::Arc<std::sync::Mutex<()>>,
}

impl RecoveryStore {
    pub(super) fn host(&self) -> std::sync::Arc<dyn RecoveryHost> {
        std::sync::Arc::clone(&self.host)
    }

    pub(super) fn host_boot(&self) -> Result<super::execution::HostBoot, RecoveryError> {
        self.root()?;
        super::execution::HostBoot::inspect(&self.binding.host_binding, self.host.as_ref())
            .map_err(|_| invalid("OS boot identity"))
    }

    pub(super) fn open(
        authority: WorkRootAuthority,
        runner_id: &str,
        boot_id: &str,
        cleanup_gate: std::sync::Arc<std::sync::Mutex<()>>,
        host: std::sync::Arc<dyn RecoveryHost>,
    ) -> Result<Self, RecoveryError> {
        authority.validate().map_err(|_| invalid("root lease"))?;
        let binding = RootBinding::inspect(
            authority.work_root(),
            &authority.shared.directory_lock,
            host.as_ref(),
        )?;
        if !valid_id(runner_id, "rnr_") || !um_runner_protocol::valid_boot_id(boot_id) {
            return Err(invalid("runner identity"));
        }
        let store = Self {
            authority,
            runner_id: runner_id.to_owned(),
            boot_id: boot_id.to_owned(),
            binding,
            host,
            writes: std::sync::Mutex::new(false),
            cleanup_gate,
        };
        let dir = &store.authority.shared.directory_lock;
        match statat(dir, ROOT, AtFlags::SYMLINK_NOFOLLOW) {
            Err(rustix::io::Errno::NOENT) => {
                if orphan_manifest(store.authority.work_root())? {
                    return Err(invalid("missing catalog"));
                }
                // A torn catalog cannot be healed by minting new IDs. The first
                // record for a root is created only when no V1 record is present.
                let record = RootRecord {
                    schema_version: 1,
                    runner_id: runner_id.to_owned(),
                    work_root_id: public_id("rwr_"),
                    installation_id: public_id("rin_"),
                    binding: store.binding.clone(),
                    catalog: Vec::new(),
                };
                persist(dir, ROOT, &record, false)?;
            }
            Ok(_) => {
                store.root()?;
            }
            Err(_) => return Err(invalid("root record")),
        }
        Ok(store)
    }

    fn root(&self) -> Result<RootRecord, RecoveryError> {
        self.authority
            .validate()
            .map_err(|_| invalid("root lease"))?;
        let root: RootRecord = read_record(&self.authority.shared.directory_lock, ROOT)?;
        if root.schema_version != 1
            || root.runner_id != self.runner_id
            || root.binding != self.binding
            || !valid_id(&root.runner_id, "rnr_")
            || !valid_id(&root.work_root_id, "rwr_")
            || !valid_id(&root.installation_id, "rin_")
            || root.catalog.len() > 4096
        {
            return Err(invalid("root binding"));
        }
        let mut ids = BTreeSet::new();
        let mut locations = BTreeSet::new();
        for entry in &root.catalog {
            if !valid_id(&entry.workspace_id, "rws_")
                || !um_runner_protocol::valid_boot_id(&entry.origin_boot_id)
                || !um_runner_protocol::valid_assignment_id(&entry.origin_assignment_id)
                || !um_runner_protocol::valid_run_id(&entry.origin_run_id)
                || !um_runner_protocol::valid_attempt_id(&entry.origin_attempt_id)
                || entry.execution_root
                    != self
                        .authority
                        .work_root()
                        .join(&entry.origin_boot_id)
                        .join(&entry.origin_assignment_id)
                        .join("workspace")
                || !ids.insert(&entry.workspace_id)
                || !locations.insert(&entry.execution_root)
            {
                return Err(invalid("catalog"));
            }
        }
        Ok(root)
    }

    // The origin assignment directory is absent beneath its still-present
    // boot directory, which only release of that assignment produces.
    fn origin_assignment_absent(&self, entry: &CatalogEntry) -> bool {
        let root = &self.authority.shared.directory_lock;
        let Ok(boot) = open_private_directory(root, &entry.origin_boot_id) else {
            return false;
        };
        matches!(
            statat(
                &boot,
                entry.origin_assignment_id.as_str(),
                AtFlags::SYMLINK_NOFOLLOW
            ),
            Err(rustix::io::Errno::NOENT)
        )
    }

    fn private(&self, entry: &CatalogEntry) -> Result<File, RecoveryError> {
        let root = &self.authority.shared.directory_lock;
        let boot = open_private_directory(root, &entry.origin_boot_id)?;
        let assignment = open_private_directory(&boot, &entry.origin_assignment_id)?;
        open_private_directory(&assignment, "private")
    }

    /// Retires the catalog entry of an ordinary origin whose assignment tree
    /// was just released. The caller holds the cleanup gate (the cleanup
    /// engine's serialized lock), so this never takes it again. A crash
    /// between release and retirement leaves an entry whose origin is absent,
    /// which may_clean_tree already tolerates and the next release retires.
    pub(super) fn retire_released_origin(
        &self,
        boot_id: &str,
        assignment_id: &str,
    ) -> Result<(), RecoveryError> {
        let mut writes = self.writes.lock().map_err(|_| invalid("write lock"))?;
        if *writes {
            return Err(invalid("uncertain commit"));
        }
        let mut root = self.root()?;
        let before = root.catalog.len();
        root.catalog.retain(|entry| {
            !(entry.origin_boot_id == boot_id
                && entry.origin_assignment_id == assignment_id
                && self.origin_assignment_absent(entry))
        });
        if root.catalog.len() == before {
            return Ok(());
        }
        if let Err(error) = persist(&self.authority.shared.directory_lock, ROOT, &root, true) {
            *writes = true;
            return Err(error);
        }
        Ok(())
    }

    /// Catalog first, then manifest: a crash between them reports an unavailable
    /// cataloged anchor instead of forgetting a known identity. No registration
    /// acknowledgement exists yet, so incomplete records confer no guarantee.
    pub(super) fn register(&self, origin: Origin) -> Result<String, RecoveryError> {
        let _cleanup = self
            .cleanup_gate
            .lock()
            .map_err(|_| invalid("cleanup lock"))?;
        let mut writes = self.writes.lock().map_err(|_| invalid("write lock"))?;
        if *writes {
            return Err(invalid("uncertain commit"));
        }
        if origin.boot_id != self.boot_id {
            return Err(invalid("origin boot"));
        }
        let mut root = self.root()?;
        if root.catalog.len() >= 4096 {
            return Err(invalid("catalog limit"));
        }
        let entry = CatalogEntry {
            workspace_id: public_id("rws_"),
            origin_boot_id: origin.boot_id,
            origin_assignment_id: origin.assignment_id,
            origin_run_id: origin.run_id,
            origin_attempt_id: origin.attempt_id,
            execution_root: origin.execution_root,
        };
        if root.catalog.iter().any(|old| {
            old.origin_assignment_id == entry.origin_assignment_id
                || old.execution_root == entry.execution_root
        }) {
            return Err(invalid("existing anchor"));
        }
        let private = self
            .authority
            .authenticate_recovery_link(&Link {
                boot_id: entry.origin_boot_id.clone(),
                assignment_id: entry.origin_assignment_id.clone(),
                run_id: entry.origin_run_id.clone(),
                attempt_id: entry.origin_attempt_id.clone(),
            })
            .map_err(|_| invalid("origin directory"))?;
        super::execution::AssignmentProcessGuards::recover_recovery(
            &private,
            self.host_boot()?,
            self.host(),
        )
        .map_err(|_| invalid("origin journal"))?;
        let private = self.private(&entry)?;
        let workspace_id = entry.workspace_id.clone();
        root.catalog.push(entry.clone());
        if let Err(error) = persist(&self.authority.shared.directory_lock, ROOT, &root, true) {
            *writes = true;
            return Err(error);
        }
        let manifest = Manifest {
            schema_version: 1,
            runner_id: root.runner_id.clone(),
            work_root_id: root.work_root_id.clone(),
            workspace_id: workspace_id.clone(),
            binding: root.binding.clone(),
            execution_root: entry.execution_root.clone(),
            lineage: vec![Link {
                boot_id: entry.origin_boot_id.clone(),
                assignment_id: entry.origin_assignment_id.clone(),
                run_id: entry.origin_run_id.clone(),
                attempt_id: entry.origin_attempt_id.clone(),
            }],
            claim: None,
            last_claim_generation: 0,
        };
        manifest.validate(&root, &entry)?;
        if let Err(error) = persist(&private, MANIFEST, &manifest, false) {
            *writes = true;
            return Err(error);
        }
        Ok(workspace_id)
    }

    pub(super) fn initial_inventory(
        &self,
        workspace_id: &str,
        origin: &Origin,
    ) -> Result<serde_json::Value, RecoveryError> {
        let root = self.root()?;
        let entry = root
            .catalog
            .iter()
            .find(|entry| {
                entry.workspace_id == workspace_id
                    && entry.origin_boot_id == origin.boot_id
                    && entry.origin_assignment_id == origin.assignment_id
                    && entry.origin_attempt_id == origin.attempt_id
                    && entry.origin_run_id == origin.run_id
                    && entry.execution_root == origin.execution_root
            })
            .ok_or_else(|| invalid("origin binding"))?;
        let private = self.private(entry)?;
        let manifest: Manifest = read_record(&private, MANIFEST)?;
        manifest.validate(&root, entry)?;
        if manifest.claim.is_some() || manifest.last_claim_generation != 0 {
            return Err(invalid("origin claim"));
        }
        Ok(serde_json::json!({
            "schemaVersion": 1, "workRootId": root.work_root_id,
            "workspaceId": workspace_id, "runId": origin.run_id,
            "priorAttemptId": origin.attempt_id,
            "priorAssignmentId": origin.assignment_id,
            "originRunnerBootId": origin.boot_id,
            "originAssignmentId": origin.assignment_id,
            "originAttemptId": origin.attempt_id,
            "executionRoot": origin.execution_root,
            "observedClaim": null, "lastClaimGeneration": 0,
            "presence": "present", "recoveryReason": null,
        }))
    }

    pub(super) fn may_clean_tree(&self, path: &Path) -> bool {
        let Ok(root) = self.root() else {
            return false;
        };
        root.catalog.iter().all(|entry| {
            let manifest = self.manifest(&entry.workspace_id);
            // An ordinary origin's catalog entry outlives its released
            // assignment. Only that proven absence stops blocking unrelated
            // cleanup; every other unreadable manifest stays fail-closed.
            if manifest.is_err() && self.origin_assignment_absent(entry) {
                return true;
            }
            manifest.is_ok_and(|manifest| {
                manifest.claim.is_none()
                    || !manifest.lineage.iter().any(|link| {
                        let assignment = self
                            .authority
                            .work_root()
                            .join(&link.boot_id)
                            .join(&link.assignment_id);
                        path == assignment || path == assignment.join("workspace")
                    })
            })
        })
    }

    pub(super) fn boot_has_unresolved_anchors(&self, boot_id: &str) -> bool {
        let Ok(root) = self.root() else {
            return true;
        };
        root.catalog.iter().any(|entry| {
            !self.manifest(&entry.workspace_id).is_ok_and(|manifest| {
                !((manifest.claim.is_some()
                    && manifest.lineage.iter().any(|link| link.boot_id == boot_id))
                    || entry.origin_boot_id == boot_id)
            })
        })
    }

    /// Snapshot only cataloged identities. A bad anchor remains visible as an
    /// unavailable observation; a bad catalog aborts the entire scan.
    pub(super) fn discovery(
        &self,
        mut on_unavailable: impl FnMut(&RecoveryError),
    ) -> Result<(String, Vec<serde_json::Value>), RecoveryError> {
        let root = self.root()?;
        let mut observations = Vec::with_capacity(root.catalog.len());
        for entry in &root.catalog {
            let verified = self.manifest(&entry.workspace_id).and_then(|manifest| {
                self.private_by_attempt(&manifest)?;
                let prior = manifest
                    .lineage
                    .last()
                    .cloned()
                    .ok_or_else(|| invalid("lineage"))?;
                Ok((manifest, prior))
            });
            let (prior, claim, generation, presence, reason) = match verified {
                Ok((manifest, prior)) => (
                    prior,
                    serde_json::json!(manifest.claim),
                    serde_json::json!(manifest.last_claim_generation),
                    "present",
                    serde_json::Value::Null,
                ),
                Err(error) => {
                    let reason = if self.origin_assignment_absent(entry) {
                        "workspace_not_found"
                    } else {
                        "recovery_record_invalid"
                    };
                    // The catalog's origin is the only trustworthy identity when
                    // the manifest or inherited process journal cannot be read.
                    let prior = Link {
                        boot_id: entry.origin_boot_id.clone(),
                        assignment_id: entry.origin_assignment_id.clone(),
                        run_id: entry.origin_run_id.clone(),
                        attempt_id: entry.origin_attempt_id.clone(),
                    };
                    on_unavailable(&error); // only classified safe facts reach local telemetry
                    (
                        prior,
                        serde_json::Value::Null,
                        serde_json::Value::Null,
                        "unavailable",
                        serde_json::json!(reason),
                    )
                }
            };
            observations.push(serde_json::json!({
                "schemaVersion": 1, "discovery": true, "workRootId": root.work_root_id,
                "workspaceId": entry.workspace_id, "runId": entry.origin_run_id,
                "priorAttemptId": prior.attempt_id, "priorAssignmentId": prior.assignment_id,
                "originRunnerBootId": entry.origin_boot_id,
                "originAssignmentId": entry.origin_assignment_id,
                "originAttemptId": entry.origin_attempt_id,
                "executionRoot": entry.execution_root, "observedClaim": claim,
                "lastClaimGeneration": generation, "presence": presence,
                "recoveryReason": reason
            }));
        }
        Ok((root.work_root_id, observations))
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(super) fn catalog(&self) -> Result<Vec<String>, RecoveryError> {
        Ok(self
            .root()?
            .catalog
            .into_iter()
            .map(|entry| entry.workspace_id)
            .collect())
    }

    pub(super) fn manifest(&self, workspace_id: &str) -> Result<Manifest, RecoveryError> {
        let root = self.root()?;
        let entry = root
            .catalog
            .iter()
            .find(|entry| entry.workspace_id == workspace_id)
            .ok_or_else(|| invalid("unknown workspace"))?;
        let manifest: Manifest = read_record(&self.private(entry)?, MANIFEST)?;
        manifest.validate(&root, entry)?;
        self.authenticated_chain(&manifest)?;
        Ok(manifest)
    }

    pub(super) fn private_by_attempt(
        &self,
        manifest: &Manifest,
    ) -> Result<std::collections::BTreeMap<String, PathBuf>, RecoveryError> {
        // Revalidate at point of use; lineage cannot override an origin path.
        let checked = self.manifest(&manifest.workspace_id)?;
        if &checked != manifest {
            return Err(invalid("lineage changed"));
        }
        let mut result = std::collections::BTreeMap::new();
        for link in &manifest.lineage {
            let private = self
                .authority
                .authenticate_recovery_link(link)
                .map_err(|_| invalid("lineage directory"))?;
            if result.insert(link.attempt_id.clone(), private).is_some() {
                return Err(invalid("duplicate attempt"));
            }
        }
        Ok(result)
    }

    fn authenticated_chain(
        &self,
        manifest: &Manifest,
    ) -> Result<Vec<super::execution::AssignmentProcessGuards>, RecoveryError> {
        let binding = self.host_boot()?;
        manifest
            .lineage
            .iter()
            .map(|link| {
                let private = self
                    .authority
                    .authenticate_recovery_link(link)
                    .map_err(|_| invalid("lineage directory"))?;
                super::execution::AssignmentProcessGuards::recover_recovery(
                    &private,
                    binding.clone(),
                    self.host(),
                )
                .map_err(|_| invalid("process journal"))
            })
            .collect()
    }

    /// A fixture grant is deliberately NOT a Cloud grant. It tests the complete
    /// local transition and must never be called by the offer/transport path.
    #[cfg(all(test, target_os = "linux"))]
    pub(super) fn claim_fixture(
        &self,
        workspace_id: &str,
        grant: &FixtureGrant,
        fenced_predecessor: bool,
    ) -> Result<Manifest, RecoveryError> {
        let _cleanup = self
            .cleanup_gate
            .lock()
            .map_err(|_| invalid("cleanup lock"))?;
        let mut writes = self.writes.lock().map_err(|_| invalid("write lock"))?;
        if *writes {
            return Err(invalid("uncertain commit"));
        }
        let root = self.root()?;
        let entry = root
            .catalog
            .iter()
            .find(|entry| entry.workspace_id == workspace_id)
            .ok_or_else(|| invalid("unknown workspace"))?;
        let private = self.private(entry)?;
        let mut manifest: Manifest = read_record(&private, MANIFEST)?;
        manifest.validate(&root, entry)?;
        let predecessor = manifest
            .claim
            .as_ref()
            .map(|claim| claim.assignment_id.as_str())
            .or_else(|| {
                manifest
                    .lineage
                    .last()
                    .map(|link| link.assignment_id.as_str())
            })
            .ok_or_else(|| invalid("predecessor"))?;
        if grant.runner_id != root.runner_id
            || grant.work_root_id != root.work_root_id
            || grant.workspace_id != entry.workspace_id
            || grant.execution_root != entry.execution_root
            || grant.origin_boot_id != entry.origin_boot_id
            || grant.origin_assignment_id != entry.origin_assignment_id
            || grant.origin_run_id != entry.origin_run_id
            || grant.origin_attempt_id != entry.origin_attempt_id
            || grant.claim.runner_boot_id != self.boot_id
            || grant.claim.claim_generation == 0
            || grant.claim.claim_generation > MAX_GENERATION
        {
            return Err(invalid("grant"));
        }
        if grant.claim.claim_generation == manifest.last_claim_generation
            && manifest.claim.as_ref() == Some(&grant.claim)
        {
            return Ok(manifest);
        }
        if grant.fenced_predecessor_assignment_id != predecessor {
            return Err(invalid("predecessor grant"));
        }
        if let Some(holder) = &manifest.claim
            && !manifest.lineage.iter().any(|link| {
                link.assignment_id == holder.assignment_id
                    && link.attempt_id == holder.attempt_id
                    && link.boot_id == holder.runner_boot_id
            })
        {
            return Err(invalid("unrecorded prior holder"));
        }
        if grant.claim.claim_generation <= manifest.last_claim_generation
            || manifest.last_claim_generation == MAX_GENERATION
        {
            return Err(invalid("stale or exhausted generation"));
        }
        if !fenced_predecessor {
            return Err(invalid("predecessor Cloud fencing"));
        }
        let guards = self.authenticated_chain(&manifest)?;
        super::execution::AssignmentProcessGuards::quiesce_retained_chain(&guards)
            .map_err(|_| invalid("predecessor writers"))?;
        let next = Link {
            boot_id: grant.claim.runner_boot_id.clone(),
            assignment_id: grant.claim.assignment_id.clone(),
            run_id: entry.origin_run_id.clone(),
            attempt_id: grant.claim.attempt_id.clone(),
        };
        let target = self
            .authority
            .authenticate_recovery_link(&next)
            .map_err(|_| invalid("claim holder directory"))?;
        super::execution::AssignmentProcessGuards::recover_recovery(
            &target,
            self.host_boot()?,
            self.host(),
        )
        .map_err(|_| invalid("claim holder journal"))?;
        if manifest.lineage.len() >= 256 || manifest.lineage.contains(&next) {
            return Err(invalid("claim lineage"));
        }
        manifest.lineage.push(next);
        manifest.last_claim_generation = grant.claim.claim_generation;
        manifest.claim = Some(grant.claim.clone());
        manifest.validate(&root, entry)?;
        if let Err(error) = persist(&private, MANIFEST, &manifest, true) {
            *writes = true;
            return Err(error);
        }
        Ok(manifest)
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(super) fn release_fixture_claim(
        &self,
        workspace_id: &str,
        holder: &Claim,
        fenced: bool,
    ) -> Result<Manifest, RecoveryError> {
        let _cleanup = self
            .cleanup_gate
            .lock()
            .map_err(|_| invalid("cleanup lock"))?;
        let mut writes = self.writes.lock().map_err(|_| invalid("write lock"))?;
        if *writes || !fenced {
            return Err(invalid("release authority"));
        }
        let root = self.root()?;
        let entry = root
            .catalog
            .iter()
            .find(|entry| entry.workspace_id == workspace_id)
            .ok_or_else(|| invalid("unknown workspace"))?;
        let private = self.private(entry)?;
        let mut manifest: Manifest = read_record(&private, MANIFEST)?;
        manifest.validate(&root, entry)?;
        if manifest.last_claim_generation != holder.claim_generation
            || manifest.claim.as_ref() != Some(holder)
        {
            return Err(invalid("claim holder"));
        }
        let guards = self.authenticated_chain(&manifest)?;
        super::execution::AssignmentProcessGuards::quiesce_retained_chain(&guards)
            .map_err(|_| invalid("claim writers"))?;
        manifest.claim = None;
        if let Err(error) = persist(&private, MANIFEST, &manifest, true) {
            *writes = true;
            return Err(error);
        }
        Ok(manifest)
    }
}

#[cfg(all(test, target_os = "linux"))]
#[derive(Clone)]
pub(super) struct FixtureGrant {
    pub(super) runner_id: String,
    pub(super) work_root_id: String,
    pub(super) workspace_id: String,
    pub(super) execution_root: PathBuf,
    pub(super) origin_boot_id: String,
    pub(super) origin_assignment_id: String,
    pub(super) origin_run_id: String,
    pub(super) origin_attempt_id: String,
    pub(super) fenced_predecessor_assignment_id: String,
    pub(super) claim: Claim,
}

fn orphan_manifest(root: &Path) -> Result<bool, RecoveryError> {
    let boots = fs::read_dir(root).map_err(RecoveryError::Identity)?;
    for (index, boot) in boots.enumerate() {
        if index >= 4096 {
            return Err(invalid("boot scan limit"));
        }
        let boot = boot.map_err(RecoveryError::Identity)?;
        let Some(name) = boot.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !um_runner_protocol::valid_boot_id(&name) {
            continue;
        }
        if !boot.file_type().map_err(RecoveryError::Identity)?.is_dir() {
            return Err(invalid("boot directory"));
        }
        for (index, assignment) in fs::read_dir(boot.path())
            .map_err(RecoveryError::Identity)?
            .enumerate()
        {
            if index >= 4096 {
                return Err(invalid("assignment scan limit"));
            }
            let assignment = assignment.map_err(RecoveryError::Identity)?;
            let Some(name) = assignment.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !um_runner_protocol::valid_assignment_id(&name) {
                continue;
            }
            if !assignment
                .file_type()
                .map_err(RecoveryError::Identity)?
                .is_dir()
            {
                return Err(invalid("assignment directory"));
            }
            let private = assignment.path().join("private");
            match fs::symlink_metadata(&private) {
                Ok(metadata) if !metadata.is_dir() => return Err(invalid("private directory")),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(RecoveryError::Identity(error)),
            }
            for name in [MANIFEST, "process-guards-v2.json"] {
                match fs::symlink_metadata(private.join(name)) {
                    Ok(_) => return Ok(true),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(RecoveryError::Identity(error)),
                }
            }
        }
    }
    Ok(false)
}

fn valid_id(value: &str, prefix: &str) -> bool {
    let Some(suffix) = value.strip_prefix(prefix) else {
        return false;
    };
    suffix.len() == 26
        && suffix
            .bytes()
            .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
}

pub(super) fn claim_absent_for_cleanup(assignment: &impl std::os::fd::AsFd) -> bool {
    let private = match statat(assignment, "private", AtFlags::SYMLINK_NOFOLLOW) {
        Err(rustix::io::Errno::NOENT) => return true,
        Err(_) => return false,
        Ok(_) => match open_private_directory(assignment, "private") {
            Ok(private) => private,
            Err(_) => return false,
        },
    };
    match statat(&private, MANIFEST, AtFlags::SYMLINK_NOFOLLOW) {
        Err(rustix::io::Errno::NOENT) => true,
        Ok(_) => read_record::<Manifest>(&private, MANIFEST)
            .is_ok_and(|record| record.schema_version == 1 && record.claim.is_none()),
        Err(_) => false,
    }
}

fn open_private_directory(
    parent: &impl std::os::fd::AsFd,
    name: &str,
) -> Result<File, RecoveryError> {
    let file = File::from(
        openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| invalid("directory open"))?,
    );
    let held = fstat(&file).map_err(|_| invalid("directory stat"))?;
    let named =
        statat(parent, name, AtFlags::SYMLINK_NOFOLLOW).map_err(|_| invalid("directory stat"))?;
    if FileType::from_raw_mode(held.st_mode) != FileType::Directory
        || held.st_uid != rustix::process::geteuid().as_raw()
        || held.st_mode & 0o7777 != 0o700
        || held.st_dev != named.st_dev
        || held.st_ino != named.st_ino
    {
        return Err(invalid("directory identity"));
    }
    Ok(file)
}

#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(test)]
pub(super) mod host_fixture;
