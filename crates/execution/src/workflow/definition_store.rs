//! Workflow definitions under the `um` home (REQ-WFDEF-17 to REQ-WFDEF-22): the home's
//! location, the `definition.json` records and their `revisions/<n>/bindings.json`
//! snapshots under `workflows/<id>/`, and the read-only tree copies under
//! `trees/sha256/<hex>/`. Publishing holds an exclusive lock on the definition's
//! `publish.lock`, reads the record under it, writes the snapshot, then the tree copy when
//! none is present, then the record, which is replaced whole by a rename. A definition is
//! created under a dot-named `workflows/.staging-<id>/` that readers skip and renamed to
//! `workflows/<id>/` once its record is written.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use ring::digest::{SHA256, digest};
use rustix::fs::{Mode, OFlags, RenameFlags, renameat_with};
use rustix::io::Errno;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::canonical_json;
use super::document::{Binding, BindingKind};
use super::schema_common::{
    DIGEST_PREFIX, digest_hex, is_identifier, is_prefixed_ulid, lowercase_hex, utc_timestamp,
};
use super::tree::ValidatedTree;
use super::tree_copy::{self, FileOperationError, TreeCopyError, write_tree_copy};
use crate::owned_tree::remove_open_tree_at;

const UM_HOME_VARIABLE: &str = "UM_HOME";
const HOME_VARIABLE: &str = "HOME";
const DEFAULT_HOME_DIRECTORY: &str = ".um";
const WORKFLOWS_DIRECTORY: &str = "workflows";
const TREES_DIRECTORY: &str = "trees";
const SHA256_DIRECTORY: &str = "sha256";
const REVISIONS_DIRECTORY: &str = "revisions";
const RUNS_DIRECTORY: &str = "runs";
const TREE_ROOT_FILE: &str = "workflow.yaml";
const DEFINITION_FILE: &str = "definition.json";
const DEFINITION_TEMPORARY_FILE: &str = "definition.json.tmp";
const DEFINITION_STAGING_PREFIX: &str = ".staging-";
const PUBLISH_LOCK_FILE: &str = "publish.lock";
const BINDINGS_FILE: &str = "bindings.json";
const DIRECTORY_MODE: u32 = 0o700;
const RECORD_FILE_MODE: u32 = 0o600;
const SNAPSHOT_FILE_MODE: u32 = 0o444;
const DEFINITION_ID_PREFIX: &str = "def_";
const SCHEMA_VERSION: u64 = 1;
const UNKNOWN_PUBLISHER: &str = "unknown";
/// A record grows by one revision per publish; this bounds a read, not a store.
const MAXIMUM_RECORD_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_BINDINGS_BYTES: u64 = 1024 * 1024;

/// The `um` home: `$UM_HOME` when set and not empty, otherwise `$HOME/.um` (REQ-WFDEF-17).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowHome {
    root: PathBuf,
}

impl WorkflowHome {
    pub fn from_environment() -> Result<Self, DefinitionError> {
        Self::from_lookup(|name| std::env::var_os(name))
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<OsString>) -> Result<Self, DefinitionError> {
        if let Some(root) = lookup(UM_HOME_VARIABLE).filter(|value| !value.is_empty()) {
            return Ok(Self {
                root: PathBuf::from(root),
            });
        }
        lookup(HOME_VARIABLE)
            .filter(|value| !value.is_empty())
            .map(|home| Self {
                root: PathBuf::from(home).join(DEFAULT_HOME_DIRECTORY),
            })
            .ok_or(DefinitionError::HomeUnavailable)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn workflows_directory(&self) -> PathBuf {
        self.root.join(WORKFLOWS_DIRECTORY)
    }

    pub fn definition_directory(&self, id: &str) -> PathBuf {
        self.workflows_directory().join(id)
    }

    /// The copy of the tree whose digest is `sha256:<hex>`, or `None` for another form.
    pub fn tree_directory(&self, digest: &str) -> Option<PathBuf> {
        digest_hex(digest).map(|hex| self.trees_directory().join(hex))
    }

    /// The home's copy of `revision`'s tree for a run of definition `id`
    /// (REQ-LOCALRUN-173): `tree_not_retained` when the copy holds no `workflow.yaml`. The
    /// copy's contents are the caller's to validate.
    pub fn retained_tree(
        &self,
        id: &str,
        revision: &RevisionRecord,
    ) -> Result<PathBuf, DefinitionError> {
        let not_retained = |path: PathBuf| DefinitionError::TreeNotRetained {
            id: id.to_owned(),
            revision: revision.n,
            path,
        };
        let Some(copy) = self.tree_directory(&revision.tree) else {
            return Err(not_retained(self.trees_directory()));
        };
        match fs::symlink_metadata(copy.join(TREE_ROOT_FILE)) {
            Ok(_) => Ok(copy),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Err(not_retained(copy)),
            Err(source) => Err(unavailable(&copy, "inspect tree copy", source)),
        }
    }

    /// `workflows/<id>/runs/`, which holds the definition's runs (REQ-LOCALRUN-173).
    pub fn runs_directory(&self, id: &str) -> PathBuf {
        self.definition_directory(id).join(RUNS_DIRECTORY)
    }

    /// Creates `workflows/<id>/runs/` readable by its owner alone unless it exists; the
    /// definition's directory must already exist.
    pub fn create_runs_directory(&self, id: &str) -> Result<PathBuf, DefinitionError> {
        let runs = self.runs_directory(id);
        match DirBuilder::new().mode(DIRECTORY_MODE).create(&runs) {
            Ok(()) => Ok(runs),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(runs),
            Err(source) => Err(unavailable(&runs, "create directory", source)),
        }
    }

    fn trees_directory(&self) -> PathBuf {
        self.root.join(TREES_DIRECTORY).join(SHA256_DIRECTORY)
    }
}

/// `definition.json`, exactly `contracts/jsonschema/workflow-definition-v1.schema.json`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DefinitionRecord {
    schema_version: u64,
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub current: u64,
    /// Newest first.
    pub revisions: Vec<RevisionRecord>,
    /// Required on read; written as `null` when absent.
    #[serde(deserialize_with = "Option::deserialize")]
    pub cloud_id: Option<String>,
}

impl DefinitionRecord {
    pub fn current_revision(&self) -> Option<&RevisionRecord> {
        self.revision(self.current)
    }

    /// Revision `n`, when the record lists it.
    pub fn revision(&self, n: u64) -> Option<&RevisionRecord> {
        self.revisions.iter().find(|revision| revision.n == n)
    }

    /// The record's schema shape, its id under `directory_name`, and a listed `current`.
    fn is_valid(&self, directory_name: &str) -> bool {
        self.schema_version == SCHEMA_VERSION
            && is_definition_id(&self.id)
            && self.id == directory_name
            && is_identifier(&self.name)
            && self.current >= 1
            && !self.revisions.is_empty()
            && self.revisions.iter().all(RevisionRecord::is_valid)
            && self.current_revision().is_some()
            && self.cloud_id.as_deref().is_none_or(is_definition_id)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RevisionRecord {
    pub n: u64,
    /// The tree digest, `sha256:<hex>`.
    pub tree: String,
    /// The `sha256:` digest of the snapshot file's bytes.
    pub bindings: String,
    pub published_at: String,
    pub by: String,
}

impl RevisionRecord {
    fn is_valid(&self) -> bool {
        self.n >= 1
            && digest_hex(&self.tree).is_some()
            && digest_hex(&self.bindings).is_some()
            && OffsetDateTime::parse(&self.published_at, &Rfc3339).is_ok()
            && !self.by.is_empty()
    }
}

/// A revision's `bindings.json` (REQ-WFDEF-3): every declared binding with its kind and value.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct BindingsSnapshot(BTreeMap<String, BoundBinding>);

impl BindingsSnapshot {
    pub fn bindings(&self) -> &BTreeMap<String, BoundBinding> {
        &self.0
    }

    /// The snapshot file's bytes: keys in bytewise order, no insignificant whitespace.
    fn canonical_bytes(&self) -> Result<Vec<u8>, io::Error> {
        let value = serde_json::to_value(self).map_err(io::Error::other)?;
        let mut bytes = Vec::new();
        canonical_json::to_writer(&mut bytes, &value).map_err(io::Error::other)?;
        Ok(bytes)
    }

    fn parse(bytes: &[u8]) -> Option<Self> {
        let snapshot = serde_json::from_slice::<Self>(bytes).ok()?;
        snapshot
            .0
            .iter()
            .all(|(key, bound)| is_identifier(key) && !bound.value.is_empty())
            .then_some(snapshot)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BoundBinding {
    pub kind: BindingKind,
    pub value: String,
}

/// A `--bindings` file (REQ-WFDEF-20): a JSON object keyed by binding key whose members
/// are the value, or an object of `value` with an optional `kind`. Values are checked
/// against the root's declarations by [`resolve_bindings`].
#[derive(Clone, Eq, PartialEq)]
pub struct BindingsFile {
    members: BTreeMap<String, FileMember>,
}

#[derive(Clone, Eq, PartialEq)]
struct FileMember {
    kind: Option<String>,
    value: Value,
}

impl fmt::Debug for BindingsFile {
    /// The keys, never the values.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BindingsFile")
            .field("keys", &self.members.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl BindingsFile {
    /// Reads and parses `path`; a failure names the path and never the contents.
    pub fn read(path: &Path) -> Result<Self, DefinitionError> {
        let bytes = read_bounded(path, MAXIMUM_BINDINGS_BYTES).map_err(|source| {
            DefinitionError::BindingsFileInvalid {
                path: Some(path.to_owned()),
                key: None,
                reason: "unreadable",
                source: Some(source),
            }
        })?;
        Self::parse(&bytes).map_err(|error| match error {
            DefinitionError::BindingsFileInvalid {
                key,
                reason,
                source,
                ..
            } => DefinitionError::BindingsFileInvalid {
                path: Some(path.to_owned()),
                key,
                reason,
                source,
            },
            other => other,
        })
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, DefinitionError> {
        let invalid = |key: Option<&str>, reason| DefinitionError::BindingsFileInvalid {
            path: None,
            key: key.map(str::to_owned),
            reason,
            source: None,
        };
        let Ok(Value::Object(object)) = serde_json::from_slice::<Value>(bytes) else {
            return Err(invalid(None, "not a JSON object"));
        };
        let mut members = BTreeMap::new();
        for (key, member) in object {
            let member = match member {
                Value::Object(mut fields) => {
                    if fields
                        .keys()
                        .any(|field| field != "kind" && field != "value")
                    {
                        return Err(invalid(
                            Some(&key),
                            "a member field other than kind and value",
                        ));
                    }
                    let kind = match fields.remove("kind") {
                        None => None,
                        Some(Value::String(kind)) => Some(kind),
                        Some(_) => {
                            return Err(invalid(Some(&key), "a member kind that is not a string"));
                        }
                    };
                    let Some(value) = fields.remove("value") else {
                        return Err(invalid(Some(&key), "a member object without a value"));
                    };
                    FileMember { kind, value }
                }
                value => FileMember { kind: None, value },
            };
            members.insert(key, member);
        }
        Ok(Self { members })
    }
}

/// The snapshot of the `declared` bindings (REQ-WFDEF-20, REQ-WFDEF-21): from `file` when
/// given, otherwise from the `carried` snapshot with keys no longer declared dropped; a
/// declared binding neither supplies takes its default.
pub(crate) fn resolve_bindings(
    declared: &BTreeMap<String, Binding>,
    file: Option<&BindingsFile>,
    carried: Option<&BindingsSnapshot>,
) -> Result<BindingsSnapshot, DefinitionError> {
    let mut supplied = BTreeMap::<&str, &str>::new();
    if let Some(file) = file {
        for (key, member) in &file.members {
            let binding = declared
                .get(key)
                .ok_or_else(|| DefinitionError::BindingUndeclared { key: key.clone() })?;
            if member
                .kind
                .as_deref()
                .is_some_and(|kind| kind != binding.kind.as_str())
            {
                return Err(DefinitionError::BindingKindMismatch { key: key.clone() });
            }
            let value = member
                .value
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| DefinitionError::BindingValueInvalid { key: key.clone() })?;
            supplied.insert(key, value);
        }
    } else if let Some(carried) = carried {
        for (key, bound) in &carried.0 {
            let Some(binding) = declared.get(key) else {
                continue;
            };
            if bound.kind != binding.kind {
                return Err(DefinitionError::BindingKindMismatch { key: key.clone() });
            }
            if bound.value.is_empty() {
                return Err(DefinitionError::BindingValueInvalid { key: key.clone() });
            }
            supplied.insert(key, &bound.value);
        }
    }
    let mut snapshot = BTreeMap::new();
    for (key, binding) in declared {
        let value = match supplied.get(key.as_str()) {
            Some(value) => (*value).to_owned(),
            None => binding
                .default
                .clone()
                .ok_or_else(|| DefinitionError::BindingMissing { key: key.clone() })?,
        };
        if value.is_empty() {
            return Err(DefinitionError::BindingValueInvalid { key: key.clone() });
        }
        snapshot.insert(
            key.clone(),
            BoundBinding {
                kind: binding.kind,
                value,
            },
        );
    }
    Ok(BindingsSnapshot(snapshot))
}

/// Which record a publish adds its revision to (REQ-WFDEF-19).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublishTarget {
    /// The one record whose `name` is the root's, or a new one when none has it.
    Resolve,
    /// The record with this id, renamed to the root's `name`.
    To(String),
    /// A new record even when the name is taken.
    New,
}

/// A record read from the home with its `workflows/<id>/` directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredDefinition {
    pub record: DefinitionRecord,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Published {
    /// The record as written, the new revision first.
    pub record: DefinitionRecord,
    pub revision: RevisionRecord,
    pub snapshot: BindingsSnapshot,
    pub created: bool,
    /// The definition's `workflows/<id>/` directory.
    pub path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnabledChange {
    pub definition: StoredDefinition,
    pub changed: bool,
}

/// Records `tree` as a new revision of the target definition (REQ-WFDEF-19 to
/// REQ-WFDEF-22). `by` is the publisher's login name, `unknown` when empty. Publishes to
/// one definition are serialized by its `publish.lock`, held from the record read through
/// the record rename; one that finds it held is `publish_conflict`. A refusal writes
/// nothing; a failure after the revision directory is made removes what this publish
/// wrote. A definition this publish creates is written under `workflows/.staging-<id>/`,
/// which readers skip, and renamed to `workflows/<id>/` once its record exists, so what a
/// stopped publish left, a `revisions/<n>` or a staging directory, is skipped, never removed.
pub fn publish(
    home: &WorkflowHome,
    tree: &ValidatedTree,
    target: PublishTarget,
    bindings: Option<&BindingsFile>,
    by: &str,
) -> Result<Published, DefinitionError> {
    let name = tree.name.clone().ok_or(DefinitionError::NameMissing)?;
    let resolved = match target {
        PublishTarget::To(id) => Some(read_definition(home, &id)?),
        PublishTarget::New => None,
        PublishTarget::Resolve => resolve_by_name(home, &name)?,
    };
    // Resolution reads every record unlocked; the target's own record is read again under
    // its lock, so a publish that finished in between is extended, never overwritten. A
    // definition this publish creates is reachable by no other until it is renamed into
    // place with its record.
    let (existing, _lock) = match resolved {
        Some(StoredDefinition { record, path }) => {
            let lock = PublishLock::acquire(&path, &record.id)?;
            (Some(read_stored(path, &record.id)?), Some(lock))
        }
        None => (None, None),
    };
    let snapshot = match &existing {
        Some(existing) => {
            let carried = match bindings {
                Some(_) => None,
                None => Some(read_current_bindings(existing)?),
            };
            resolve_bindings(&tree.bindings, bindings, carried.as_ref())?
        }
        None => {
            if bindings.is_none()
                && tree
                    .bindings
                    .values()
                    .any(|binding| binding.default.is_none())
            {
                return Err(DefinitionError::BindingsRequired);
            }
            resolve_bindings(&tree.bindings, bindings, None)?
        }
    };
    let snapshot_bytes = snapshot
        .canonical_bytes()
        .map_err(|source| unavailable(home.root(), "encode bindings snapshot", source))?;
    let published_at = utc_timestamp(um_support::utc_now()).map_err(|source| {
        unavailable(home.root(), "format publish time", io::Error::other(source))
    })?;

    create_private_directory(home.root())?;
    create_private_directory(&home.workflows_directory())?;
    let (StoredDefinition { mut record, path }, created) = match existing {
        Some(existing) => (existing, false),
        None => (create_definition(home, name.clone())?, true),
    };
    let n = next_ordinal(&record, &path)?;
    let revision = RevisionRecord {
        n,
        tree: format!("{DIGEST_PREFIX}{}", tree.digest.value),
        bindings: format!(
            "{DIGEST_PREFIX}{}",
            lowercase_hex(digest(&SHA256, &snapshot_bytes).as_ref())
        ),
        published_at,
        by: if by.is_empty() { UNKNOWN_PUBLISHER } else { by }.to_owned(),
    };
    record.name = name;
    record.revisions.insert(0, revision.clone());
    record.current = n;

    let placed = write_revision(home, tree, &path, &record, n, &snapshot_bytes).and_then(|()| {
        if created {
            place_definition(home, &record.id)
        } else {
            Ok(path.clone())
        }
    });
    let path = match placed {
        Ok(path) => path,
        Err(error) => {
            // A definition this publish created is still under its staging name, reached by
            // nothing; an existing one keeps its record and loses only the revision
            // directory this publish made.
            let residue = match (&error, created) {
                (_, true) => Some(path.clone()),
                (DefinitionError::PublishConflict { .. }, false) => None,
                (_, false) => Some(revision_directory(&path, n)),
            };
            if let Some(residue) = residue {
                let _ = fs::remove_dir_all(residue);
            }
            return Err(error);
        }
    };
    Ok(Published {
        record,
        revision,
        snapshot,
        created,
        path,
    })
}

/// The record `id` (REQ-WFDEF-27: `definition_id_invalid`, `definition_not_found` or
/// `definition_record_invalid`).
pub fn read_definition(home: &WorkflowHome, id: &str) -> Result<StoredDefinition, DefinitionError> {
    if !is_definition_id(id) {
        return Err(DefinitionError::IdInvalid { id: id.to_owned() });
    }
    let path = home.definition_directory(id);
    match fs::symlink_metadata(&path) {
        Ok(_) => read_stored(path, id),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Err(DefinitionError::NotFound { id: id.to_owned() })
        }
        Err(source) => Err(unavailable(&path, "inspect definition directory", source)),
    }
}

/// Every record under the home, sorted by name then id; none when the home or its
/// `workflows/` is absent. A dot-named entry, such as the `.staging-<id>` a stopped first
/// publish left, is not a definition and is skipped. One unreadable record stops the listing.
pub fn list_definitions(home: &WorkflowHome) -> Result<Vec<StoredDefinition>, DefinitionError> {
    let workflows = home.workflows_directory();
    let entries = match fs::read_dir(&workflows) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(unavailable(&workflows, "list definitions", source)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| unavailable(&workflows, "list definitions", source))?;
        let name = entry.file_name();
        if name.as_encoded_bytes().first() != Some(&b'.') {
            names.push(name);
        }
    }
    names.sort();
    let mut definitions = names
        .into_iter()
        .map(|name| {
            let path = workflows.join(&name);
            match name.to_str() {
                Some(id) => read_stored(path.clone(), id),
                None => Err(DefinitionError::RecordInvalid {
                    path: path.join(DEFINITION_FILE),
                    reason: "directory name is not an id",
                    source: None,
                }),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    definitions.sort_by(|left, right| {
        (&left.record.name, &left.record.id).cmp(&(&right.record.name, &right.record.id))
    });
    Ok(definitions)
}

/// The current revision's snapshot, read back and checked against its recorded digest.
pub fn read_current_bindings(
    definition: &StoredDefinition,
) -> Result<BindingsSnapshot, DefinitionError> {
    read_bindings(
        definition,
        definition.record.current,
        "current revision not listed",
    )
}

/// Revision `revision`'s snapshot of definition `id`, as `read_current_bindings` reads the
/// current one: the revision a run pinned, whatever was published since.
pub fn read_revision_bindings(
    home: &WorkflowHome,
    id: &str,
    revision: u64,
) -> Result<BindingsSnapshot, DefinitionError> {
    read_bindings(&read_definition(home, id)?, revision, "revision not listed")
}

fn read_bindings(
    definition: &StoredDefinition,
    n: u64,
    unlisted: &'static str,
) -> Result<BindingsSnapshot, DefinitionError> {
    let path = revision_directory(&definition.path, n).join(BINDINGS_FILE);
    let invalid = |reason, source| DefinitionError::RecordInvalid {
        path: path.clone(),
        reason,
        source,
    };
    let revision = definition
        .record
        .revision(n)
        .ok_or_else(|| invalid(unlisted, None))?;
    let bytes = read_bounded(&path, MAXIMUM_BINDINGS_BYTES)
        .map_err(|source| invalid("bindings snapshot unreadable", Some(source)))?;
    if digest_hex(&revision.bindings)
        != Some(lowercase_hex(digest(&SHA256, &bytes).as_ref()).as_str())
    {
        return Err(invalid("bindings snapshot does not match its digest", None));
    }
    BindingsSnapshot::parse(&bytes)
        .ok_or_else(|| invalid("bindings snapshot is not of the snapshot shape", None))
}

/// Sets the record's `enabled`, replacing it whole only when it changes (REQ-WFDEF-24).
pub fn set_enabled(
    home: &WorkflowHome,
    id: &str,
    enabled: bool,
) -> Result<EnabledChange, DefinitionError> {
    let mut definition = read_definition(home, id)?;
    let changed = definition.record.enabled != enabled;
    if changed {
        definition.record.enabled = enabled;
        write_record(&definition.path, &definition.record)?;
    }
    Ok(EnabledChange {
        definition,
        changed,
    })
}

/// A refused or failed definition command, with the id, ids, binding key or path it
/// concerns and never the contents of a tree file, bindings file or record (REQ-WFDEF-27).
#[derive(Debug)]
pub enum DefinitionError {
    HomeUnavailable,
    NameMissing,
    IdInvalid {
        id: String,
    },
    NotFound {
        id: String,
    },
    NameAmbiguous {
        name: String,
        ids: Vec<String>,
    },
    /// `reason` names the failed stage; `source` is the read failure when there was one.
    RecordInvalid {
        path: PathBuf,
        reason: &'static str,
        source: Option<io::Error>,
    },
    BindingsFileInvalid {
        path: Option<PathBuf>,
        key: Option<String>,
        reason: &'static str,
        source: Option<io::Error>,
    },
    BindingsRequired,
    BindingUndeclared {
        key: String,
    },
    BindingKindMismatch {
        key: String,
    },
    BindingValueInvalid {
        key: String,
    },
    BindingMissing {
        key: String,
    },
    /// Another publish holds the definition's lock, or made `revisions/<n>` without it.
    PublishConflict {
        id: String,
    },
    /// The home holds no copy of the tree of revision `revision` of `id`; `path` is where
    /// it belongs.
    TreeNotRetained {
        id: String,
        revision: u64,
        path: PathBuf,
    },
    /// The home's copy at `path` no longer has the digest revision `revision` of `id`
    /// recorded.
    TreeRetainedModified {
        id: String,
        revision: u64,
        path: PathBuf,
    },
    /// The home could not be read or written.
    StoreUnavailable {
        path: PathBuf,
        operation: &'static str,
        source: io::Error,
    },
}

impl DefinitionError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::HomeUnavailable => "home_unavailable",
            Self::NameMissing => "definition_name_missing",
            Self::IdInvalid { .. } => "definition_id_invalid",
            Self::NotFound { .. } => "definition_not_found",
            Self::NameAmbiguous { .. } => "definition_name_ambiguous",
            Self::RecordInvalid { .. } => "definition_record_invalid",
            Self::BindingsFileInvalid { .. } => "bindings_file_invalid",
            Self::BindingsRequired => "bindings_required",
            Self::BindingUndeclared { .. } => "binding_undeclared",
            Self::BindingKindMismatch { .. } => "binding_kind_mismatch",
            Self::BindingValueInvalid { .. } => "binding_value_invalid",
            Self::BindingMissing { .. } => "binding_missing",
            Self::PublishConflict { .. } => "publish_conflict",
            Self::TreeNotRetained { .. } => "tree_not_retained",
            Self::TreeRetainedModified { .. } => "tree_retained_modified",
            Self::StoreUnavailable { .. } => "definition_store_unavailable",
        }
    }

    /// The definition id the error concerns.
    pub fn id(&self) -> Option<&str> {
        match self {
            Self::IdInvalid { id }
            | Self::NotFound { id }
            | Self::PublishConflict { id, .. }
            | Self::TreeNotRetained { id, .. }
            | Self::TreeRetainedModified { id, .. } => Some(id),
            _ => None,
        }
    }

    /// Every matching id of `definition_name_ambiguous`, sorted; otherwise empty.
    pub fn ids(&self) -> &[String] {
        match self {
            Self::NameAmbiguous { ids, .. } => ids,
            _ => &[],
        }
    }

    /// The root's `name` of `definition_name_ambiguous`.
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::NameAmbiguous { name, .. } => Some(name),
            _ => None,
        }
    }

    /// The binding key the error concerns.
    pub fn key(&self) -> Option<&str> {
        match self {
            Self::BindingUndeclared { key }
            | Self::BindingKindMismatch { key }
            | Self::BindingValueInvalid { key }
            | Self::BindingMissing { key } => Some(key),
            Self::BindingsFileInvalid { key, .. } => key.as_deref(),
            _ => None,
        }
    }

    /// The file or directory the error concerns.
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::RecordInvalid { path, .. }
            | Self::StoreUnavailable { path, .. }
            | Self::TreeNotRetained { path, .. }
            | Self::TreeRetainedModified { path, .. } => Some(path),
            Self::BindingsFileInvalid { path, .. } => path.as_deref(),
            _ => None,
        }
    }

    /// The failed stage of `definition_record_invalid` or `bindings_file_invalid`.
    pub fn reason(&self) -> Option<&'static str> {
        match self {
            Self::RecordInvalid { reason, .. } | Self::BindingsFileInvalid { reason, .. } => {
                Some(reason)
            }
            _ => None,
        }
    }

    /// The revision ordinal of `tree_not_retained` or `tree_retained_modified`.
    pub fn revision(&self) -> Option<u64> {
        match self {
            Self::TreeNotRetained { revision, .. }
            | Self::TreeRetainedModified { revision, .. } => Some(*revision),
            _ => None,
        }
    }
}

impl fmt::Display for DefinitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HomeUnavailable => formatter.write_str("neither UM_HOME nor HOME is set"),
            Self::NameMissing => formatter.write_str("the root workflow declares no name"),
            Self::IdInvalid { .. } => formatter.write_str("the id is not a definition id"),
            Self::NotFound { id } => write!(formatter, "no definition has the id {id}"),
            Self::NameAmbiguous { name, ids } => write!(
                formatter,
                "definitions {} share the name {name}",
                ids.join(", ")
            ),
            Self::RecordInvalid { path, reason, .. } => write!(
                formatter,
                "{} is not a valid definition record: {reason}",
                path.display()
            ),
            Self::BindingsFileInvalid {
                path, key, reason, ..
            } => {
                formatter.write_str("the bindings file")?;
                if let Some(path) = path {
                    write!(formatter, " {}", path.display())?;
                }
                formatter.write_str(" is not a JSON object of bindings")?;
                if let Some(key) = key {
                    write!(formatter, " at {key}")?;
                }
                write!(formatter, ": {reason}")
            }
            Self::BindingsRequired => formatter
                .write_str("the root declares a binding without a default; pass a bindings file"),
            Self::BindingUndeclared { key } => {
                write!(formatter, "the root declares no binding {key}")
            }
            Self::BindingKindMismatch { key } => {
                write!(
                    formatter,
                    "the kind given for {key} is not the declared kind"
                )
            }
            Self::BindingValueInvalid { key } => {
                write!(formatter, "the value of {key} is not a non-empty string")
            }
            Self::BindingMissing { key } => write!(formatter, "no value is bound for {key}"),
            Self::PublishConflict { id } => {
                write!(formatter, "another publish of {id} is in progress")
            }
            Self::TreeNotRetained { id, revision, .. } => write!(
                formatter,
                "the home holds no copy of the tree of revision {revision} of {id}; publish the tree again"
            ),
            Self::TreeRetainedModified { id, revision, .. } => write!(
                formatter,
                "the home's copy of the tree of revision {revision} of {id} no longer has its digest; remove the copy and publish the tree again"
            ),
            Self::StoreUnavailable {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "could not {operation} {}: {source}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for DefinitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StoreUnavailable { source, .. } => Some(source),
            Self::RecordInvalid { source, .. } | Self::BindingsFileInvalid { source, .. } => source
                .as_ref()
                .map(|source| source as &(dyn std::error::Error + 'static)),
            _ => None,
        }
    }
}

fn unavailable(path: &Path, operation: &'static str, source: io::Error) -> DefinitionError {
    DefinitionError::StoreUnavailable {
        path: path.to_owned(),
        operation,
        source,
    }
}

impl From<FileOperationError> for DefinitionError {
    fn from(error: FileOperationError) -> Self {
        Self::StoreUnavailable {
            path: error.path,
            operation: error.operation,
            source: error.source,
        }
    }
}

fn is_definition_id(value: &str) -> bool {
    is_prefixed_ulid(value, DEFINITION_ID_PREFIX)
}

fn mint_definition_id() -> String {
    format!(
        "{DEFINITION_ID_PREFIX}{}",
        ulid::Ulid::generate().to_string().to_ascii_lowercase()
    )
}

fn revision_directory(definition: &Path, n: u64) -> PathBuf {
    definition.join(REVISIONS_DIRECTORY).join(n.to_string())
}

/// The dot-named sibling of `workflows/<id>/` a definition is written under until its record
/// exists; `list_definitions` skips it.
fn staging_name(id: &str) -> String {
    format!("{DEFINITION_STAGING_PREFIX}{id}")
}

/// The exclusive `flock` on `workflows/<id>/publish.lock` that serializes publishes to one
/// definition, held while the value lives; the OS releases a stopped publish's lock with
/// its descriptor, so a crash leaves nothing to clear.
struct PublishLock {
    _file: File,
}

impl PublishLock {
    /// Takes the lock without waiting; `publish_conflict` when another publish holds it.
    fn acquire(definition: &Path, id: &str) -> Result<Self, DefinitionError> {
        let path = definition.join(PUBLISH_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(RECORD_FILE_MODE)
            .open(&path)
            .map_err(|source| unavailable(&path, "open publish lock", source))?;
        match fs4::FileExt::try_lock(&file) {
            Ok(()) => Ok(Self { _file: file }),
            Err(fs4::TryLockError::WouldBlock) => {
                Err(DefinitionError::PublishConflict { id: id.to_owned() })
            }
            Err(fs4::TryLockError::Error(source)) => {
                Err(unavailable(&path, "lock publish lock", source))
            }
        }
    }
}

/// The next revision ordinal: one more than the greater of the highest recorded ordinal and
/// the highest decimal entry under `revisions/`, so a directory a publish left when it
/// stopped before writing the record never takes the ordinal the next publish chooses.
/// Under the lock the listing shows only such leftovers.
fn next_ordinal(record: &DefinitionRecord, definition: &Path) -> Result<u64, DefinitionError> {
    let recorded = record
        .revisions
        .iter()
        .map(|revision| revision.n)
        .max()
        .unwrap_or(0);
    let revisions = definition.join(REVISIONS_DIRECTORY);
    let mut highest = recorded;
    match fs::read_dir(&revisions) {
        Ok(entries) => {
            for entry in entries {
                let entry =
                    entry.map_err(|source| unavailable(&revisions, "list revisions", source))?;
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if !name.is_empty()
                    && name.bytes().all(|byte| byte.is_ascii_digit())
                    && let Ok(n) = name.parse::<u64>()
                {
                    highest = highest.max(n);
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => return Err(unavailable(&revisions, "list revisions", source)),
    }
    highest
        .checked_add(1)
        .ok_or_else(|| DefinitionError::RecordInvalid {
            path: definition.join(DEFINITION_FILE),
            reason: "revision ordinals exhausted",
            source: None,
        })
}

fn resolve_by_name(
    home: &WorkflowHome,
    name: &str,
) -> Result<Option<StoredDefinition>, DefinitionError> {
    let mut matching = list_definitions(home)?
        .into_iter()
        .filter(|definition| definition.record.name == name)
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        let mut ids = matching
            .into_iter()
            .map(|definition| definition.record.id)
            .collect::<Vec<_>>();
        ids.sort();
        return Err(DefinitionError::NameAmbiguous {
            name: name.to_owned(),
            ids,
        });
    }
    Ok(matching.pop())
}

fn read_stored(path: PathBuf, id: &str) -> Result<StoredDefinition, DefinitionError> {
    let file = path.join(DEFINITION_FILE);
    let invalid = |reason, source| DefinitionError::RecordInvalid {
        path: file.clone(),
        reason,
        source,
    };
    let bytes = read_bounded(&file, MAXIMUM_RECORD_BYTES)
        .map_err(|source| invalid("unreadable", Some(source)))?;
    // The parser's message can quote the record, so only the stage is kept.
    let record = serde_json::from_slice::<DefinitionRecord>(&bytes)
        .map_err(|_| invalid("not of the record shape", None))?;
    if !record.is_valid(id) {
        return Err(invalid("fails the record's constraints", None));
    }
    Ok(StoredDefinition { record, path })
}

fn read_bounded(path: &Path, maximum_bytes: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(maximum_bytes + 1)
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).map_or(true, |length| length > maximum_bytes) {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    Ok(bytes)
}

/// Creates `path` and any missing parent readable by its owner alone; an existing
/// directory is kept as it is.
fn create_private_directory(path: &Path) -> Result<(), DefinitionError> {
    DirBuilder::new()
        .recursive(true)
        .mode(DIRECTORY_MODE)
        .create(path)
        .map_err(|source| unavailable(path, "create directory", source))
}

/// A fresh `workflows/.staging-<id>/`, the directory `place_definition` renames to
/// `workflows/<id>/` once the record is written, and the record a first revision completes;
/// a taken id is minted again once.
fn create_definition(
    home: &WorkflowHome,
    name: String,
) -> Result<StoredDefinition, DefinitionError> {
    let mut retried = false;
    loop {
        let id = mint_definition_id();
        let path = home.workflows_directory().join(staging_name(&id));
        match DirBuilder::new().mode(DIRECTORY_MODE).create(&path) {
            Ok(()) => {
                return Ok(StoredDefinition {
                    record: DefinitionRecord {
                        schema_version: SCHEMA_VERSION,
                        id,
                        name,
                        enabled: true,
                        current: 0,
                        revisions: Vec::new(),
                        cloud_id: None,
                    },
                    path,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && !retried => {
                retried = true;
            }
            Err(source) => return Err(unavailable(&path, "create directory", source)),
        }
    }
}

/// Renames a created definition's `workflows/.staging-<id>/` to `workflows/<id>/` once its
/// record is written: the rename makes the definition reachable, and a staging directory a
/// stopped publish left is skipped by every reader and never removed. A taken `workflows/<id>/`
/// is `definition_store_unavailable`, leaving the staging directory to the caller.
fn place_definition(home: &WorkflowHome, id: &str) -> Result<PathBuf, DefinitionError> {
    let workflows = home.workflows_directory();
    let placed = workflows.join(id);
    let parent = rustix::fs::open(
        &workflows,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|source| unavailable(&workflows, "open directory", source.into()))?;
    renameat_with(
        &parent,
        staging_name(id),
        &parent,
        id,
        RenameFlags::NOREPLACE,
    )
    .map_err(|source| unavailable(&placed, "place definition", source.into()))?;
    // The rename is the commit; a failed directory sync cannot revoke it.
    let _ = tree_copy::sync_directory(&parent);
    Ok(placed)
}

/// Writes revision `n`'s snapshot under a directory only this publish creates, the tree
/// copy when none is present, then the record. The caller holds the definition's lock, so a
/// `revisions/<n>` already there was made by a writer without it.
fn write_revision(
    home: &WorkflowHome,
    tree: &ValidatedTree,
    definition: &Path,
    record: &DefinitionRecord,
    n: u64,
    snapshot: &[u8],
) -> Result<(), DefinitionError> {
    create_private_directory(&definition.join(REVISIONS_DIRECTORY))?;
    let revision = revision_directory(definition, n);
    match DirBuilder::new().mode(DIRECTORY_MODE).create(&revision) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(DefinitionError::PublishConflict {
                id: record.id.clone(),
            });
        }
        Err(source) => return Err(unavailable(&revision, "create directory", source)),
    }
    let snapshot_path = revision.join(BINDINGS_FILE);
    write_new_file(&snapshot_path, snapshot, SNAPSHOT_FILE_MODE)
        .map_err(|source| unavailable(&snapshot_path, "write bindings snapshot", source))?;
    write_tree(home, tree)?;
    write_record(definition, record)
}

fn write_new_file(path: &Path, bytes: &[u8], mode: u32) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Replaces `definition.json` whole: pretty JSON and a newline, written to a sibling
/// `.tmp` and renamed over the record.
fn write_record(definition: &Path, record: &DefinitionRecord) -> Result<(), DefinitionError> {
    let temporary = definition.join(DEFINITION_TEMPORARY_FILE);
    let mut bytes = serde_json::to_vec_pretty(record)
        .map_err(|source| unavailable(&temporary, "encode definition record", source.into()))?;
    bytes.push(b'\n');
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(RECORD_FILE_MODE)
        .open(&temporary)
        .map_err(|source| unavailable(&temporary, "create definition record", source))?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| unavailable(&temporary, "write definition record", source))?;
    fs::rename(&temporary, definition.join(DEFINITION_FILE))
        .map_err(|source| unavailable(&temporary, "replace definition record", source))?;
    // The rename is the commit; a failed directory sync cannot revoke it.
    let _ = File::open(definition).and_then(|directory| directory.sync_all());
    Ok(())
}

/// Copies `tree` to `trees/sha256/<hex>/` through a `.staging-<hex>-<ulid>` sibling unless
/// a copy is already there, which is kept as found.
fn write_tree(home: &WorkflowHome, tree: &ValidatedTree) -> Result<(), DefinitionError> {
    let trees = home.trees_directory();
    create_private_directory(&trees)?;
    let hex = tree.digest.value.as_str();
    match fs::symlink_metadata(trees.join(hex)) {
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(source) => return Err(unavailable(&trees.join(hex), "inspect tree copy", source)),
    }
    let parent = rustix::fs::open(
        &trees,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|source| unavailable(&trees, "open directory", source.into()))?;
    let staging_name = format!(
        ".staging-{hex}-{}",
        ulid::Ulid::generate().to_string().to_ascii_lowercase()
    );
    tree_copy::mkdir(&parent, &staging_name)?;
    let staging = tree_copy::open_directory_at(&parent, &staging_name)?;
    let written = write_tree_copy(
        &staging,
        tree.files.iter().map(|(path, file)| (path.as_str(), file)),
        &tree.programs,
    );
    let renamed = match written {
        Ok(()) => renameat_with(&parent, &staging_name, &parent, hex, RenameFlags::NOREPLACE),
        Err(error) => {
            remove_staging(&parent, &staging_name, &staging);
            return Err(match error {
                TreeCopyError::Layout => unavailable(
                    &trees.join(&staging_name),
                    "lay out tree copy",
                    io::Error::from(io::ErrorKind::InvalidData),
                ),
                TreeCopyError::File(error) => error.into(),
            });
        }
    };
    match renamed {
        Ok(()) => {
            let _ = tree_copy::sync_directory(&parent);
            Ok(())
        }
        // Another publish placed the copy while this one was walking; keep that one.
        Err(Errno::EXIST | Errno::NOTEMPTY) => {
            remove_staging(&parent, &staging_name, &staging);
            Ok(())
        }
        Err(source) => {
            remove_staging(&parent, &staging_name, &staging);
            Err(unavailable(
                &trees.join(hex),
                "place tree copy",
                source.into(),
            ))
        }
    }
}

fn remove_staging(parent: &OwnedFd, name: &str, staging: &OwnedFd) {
    let _ = remove_open_tree_at(parent, name, staging);
}

#[cfg(test)]
mod tests;
