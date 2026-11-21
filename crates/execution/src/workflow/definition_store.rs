//! Workflow definitions under the `useful` home (REQ-WFDEF-17 to REQ-WFDEF-22): the home's
//! location, the `definition.json` records and their `revisions/<n>/bindings.json`
//! snapshots under `workflows/<id>/`, each with the optional `resolution.json` of provider
//! ids beside it that the Cloud runner writes and publishing never does (REQ-WFDEF-32), and
//! the read-only tree copies under `trees/sha256/<hex>/`. Publishing holds an exclusive lock on the definition's
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
use super::document::{
    Binding, BindingKey, BindingKind, BindingValue, Integration, IntegrationBinding,
    IntegrationProvider,
};
use super::schema_common::{
    DIGEST_PREFIX, digest_hex, is_identifier, is_prefixed_ulid, lowercase_hex, utc_timestamp,
};
use super::tree::ValidatedTree;
use super::tree_copy::{self, FileOperationError, TreeCopyError, write_tree_copy};
use crate::owned_tree::remove_open_tree_at;

const USEFUL_HOME_VARIABLE: &str = "USEFUL_HOME";
const HOME_VARIABLE: &str = "HOME";
const DEFAULT_HOME_DIRECTORY: &str = ".useful";
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
const RESOLUTION_FILE: &str = "resolution.json";
const DIRECTORY_MODE: u32 = 0o700;
const RECORD_FILE_MODE: u32 = 0o600;
const SNAPSHOT_FILE_MODE: u32 = 0o444;
const DEFINITION_ID_PREFIX: &str = "def_";
const SCHEMA_VERSION: u64 = 1;
const UNKNOWN_PUBLISHER: &str = "unknown";
/// A record grows by one revision per publish; this bounds a read, not a store.
const MAXIMUM_RECORD_BYTES: u64 = 16 * 1024 * 1024;
const MAXIMUM_BINDINGS_BYTES: u64 = 1024 * 1024;

/// The `useful` home: `$USEFUL_HOME` when set and not empty, otherwise `$HOME/.useful` (REQ-WFDEF-17).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowHome {
    root: PathBuf,
}

impl WorkflowHome {
    pub fn from_environment() -> Result<Self, DefinitionError> {
        Self::from_lookup(|name| std::env::var_os(name))
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<OsString>) -> Result<Self, DefinitionError> {
        if let Some(root) = lookup(USEFUL_HOME_VARIABLE).filter(|value| !value.is_empty()) {
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

    /// Revision `revision`'s resolution record of definition `id` (REQ-WFDEF-32): the
    /// provider id resolved for each binding it names, by the reference key
    /// [`BindingsSnapshot::text_values_by_reference`] answers values by, or `None` when the revision has
    /// no `resolution.json`. A revision the record does not list, or a record that cannot be
    /// read or is not of the record's shape, is `definition_record_invalid`.
    pub fn revision_resolution(
        &self,
        id: &str,
        revision: u64,
    ) -> Result<Option<BTreeMap<String, String>>, DefinitionError> {
        let definition = read_definition(self, id)?;
        let path = revision_directory(&definition.path, revision).join(RESOLUTION_FILE);
        let invalid = |reason, source| DefinitionError::RecordInvalid {
            path: path.clone(),
            reason,
            source,
        };
        if definition.record.revision(revision).is_none() {
            return Err(invalid("revision not listed", None));
        }
        let bytes = match read_bounded(&path, MAXIMUM_BINDINGS_BYTES) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(invalid("resolution record unreadable", Some(source))),
        };
        resolved_ids(&bytes)
            .map(Some)
            .ok_or_else(|| invalid("resolution record is not of the record's shape", None))
    }
}

/// The ids of a `resolution.json` by the key a reference names each binding by: a flat key's
/// entry under the key, and the `linear` integration's under `linear.<name>` from the member
/// `linear`, an object from name to entry, told from the flat key `linear`'s own entry by
/// whether its `id` is a string. No other member nests (REQ-WFDEF-32).
fn resolved_ids(bytes: &[u8]) -> Option<BTreeMap<String, String>> {
    let linear = IntegrationProvider::Linear;
    let mut ids = BTreeMap::new();
    for (key, member) in serde_json::from_slice::<BTreeMap<String, Value>>(bytes).ok()? {
        if key == linear.as_str() && !member.get("id").is_some_and(Value::is_string) {
            for (name, resolved) in
                serde_json::from_value::<BTreeMap<String, Value>>(member).ok()?
            {
                let id = ResolvedBinding::id(resolved).filter(|_| is_identifier(&name))?;
                ids.insert(BindingAt::under(linear, &name).reference_key(), id);
            }
        } else {
            let id = ResolvedBinding::id(member).filter(|_| is_identifier(&key))?;
            ids.insert(key, id);
        }
    }
    Some(ids)
}

/// One entry of a revision's `resolution.json` (REQ-WFDEF-32): the provider's id for the
/// bound value and the name it had when Cloud resolved it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolvedBinding {
    id: String,
    name: String,
}

impl ResolvedBinding {
    /// The id of `entry` when it is an entry of non-empty strings.
    fn id(entry: Value) -> Option<String> {
        serde_json::from_value::<Self>(entry)
            .ok()
            .filter(|resolved| !resolved.id.is_empty() && !resolved.name.is_empty())
            .map(|resolved| resolved.id)
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

/// A revision's `bindings.json` (REQ-WFDEF-3): every declared binding with its kind and value,
/// the flat block's by key and each integration's under its id, but a `definition` binding
/// given none (REQ-WFDEF-40). Only an integration that declares bindings has a group, so the
/// snapshot of a root that declares no integration binding is the flat block alone.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BindingsSnapshot {
    flat: BTreeMap<String, BoundBinding>,
    integrations: BTreeMap<IntegrationProvider, BTreeMap<String, BoundIntegrationBinding>>,
}

/// One member of the snapshot object: a flat binding, or an integration's group.
#[derive(Serialize)]
#[serde(untagged)]
enum SnapshotMember<'a> {
    Bound(&'a BoundBinding),
    Group(&'a BTreeMap<String, BoundIntegrationBinding>),
}

impl Serialize for BindingsSnapshot {
    /// One JSON object: a flat key to its binding, an integration's id to its group.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.flat
            .iter()
            .map(|(key, bound)| (key.as_str(), SnapshotMember::Bound(bound)))
            .chain(
                self.integrations
                    .iter()
                    .map(|(provider, group)| (provider.as_str(), SnapshotMember::Group(group))),
            )
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer)
    }
}

impl BindingsSnapshot {
    /// The flat block's bindings.
    pub fn bindings(&self) -> &BTreeMap<String, BoundBinding> {
        &self.flat
    }

    /// Each integration's bindings by name, only for an integration that declares bindings.
    pub fn integration_bindings(
        &self,
    ) -> &BTreeMap<IntegrationProvider, BTreeMap<String, BoundIntegrationBinding>> {
        &self.integrations
    }

    /// Every text value by the key a reference names its binding by after `$` (REQ-WFDEF-37):
    /// a flat key as it is and an integration binding as `<id>.<name>`, the keys
    /// [`WorkflowHome::revision_resolution`] answers ids by. A `members` list is no text value.
    pub fn text_values_by_reference(&self) -> BTreeMap<String, String> {
        self.members()
            .filter_map(|(at, _, value)| Some((at.reference_key(), value.as_text()?.to_owned())))
            .collect()
    }

    /// Every binding with where it is declared, its kind as text and its value.
    fn members(&self) -> impl Iterator<Item = (BindingAt<'_>, &str, BindingValue)> {
        let flat = self.flat.iter().map(|(key, bound)| {
            (
                BindingAt::flat(key),
                bound.kind.as_str(),
                bound.value.clone(),
            )
        });
        let grouped = self.integrations.iter().flat_map(|(provider, group)| {
            group.iter().map(|(name, bound)| {
                (
                    BindingAt::under(*provider, name),
                    bound.kind.as_str(),
                    BindingValue::Text(bound.value.clone()),
                )
            })
        });
        flat.chain(grouped)
    }

    /// The snapshot file's bytes: keys in bytewise order, no insignificant whitespace.
    fn canonical_bytes(&self) -> Result<Vec<u8>, io::Error> {
        let value = serde_json::to_value(self).map_err(io::Error::other)?;
        let mut bytes = Vec::new();
        canonical_json::to_writer(&mut bytes, &value).map_err(io::Error::other)?;
        Ok(bytes)
    }

    /// A member whose `kind` is a string is a flat binding, any other an integration's group
    /// keyed by the provider's id: names that are identifiers to objects of a non-empty
    /// `kind` and a non-empty string `value`.
    fn parse(bytes: &[u8]) -> Option<Self> {
        let object = serde_json::from_slice::<BTreeMap<String, Value>>(bytes).ok()?;
        let mut snapshot = Self::default();
        for (key, member) in object {
            if member.get("kind").is_some_and(Value::is_string) {
                let bound = serde_json::from_value::<BoundBinding>(member).ok()?;
                if !is_identifier(&key) || !bound.value.fits(bound.kind) {
                    return None;
                }
                snapshot.flat.insert(key, bound);
                continue;
            }
            let provider = serde_json::from_value(Value::String(key)).ok()?;
            let group =
                serde_json::from_value::<BTreeMap<String, BoundIntegrationBinding>>(member).ok()?;
            let valid = !group.is_empty()
                && group.iter().all(|(name, bound)| {
                    is_identifier(name) && !bound.kind.is_empty() && !bound.value.is_empty()
                });
            if !valid {
                return None;
            }
            snapshot.integrations.insert(provider, group);
        }
        Some(snapshot)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BoundBinding {
    pub kind: BindingKind,
    /// A string, or for `members` a list kept as a list in the snapshot and its digest.
    pub value: BindingValue,
}

/// An integration binding in the snapshot: its provider-relative kind as the root declares it
/// and its value, a non-empty string.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BoundIntegrationBinding {
    pub kind: String,
    pub value: String,
}

/// A `--bindings` file (REQ-WFDEF-20): a JSON object whose members name the bindings a
/// publish sets, flat bindings by key, each the value, an object of `value` with an optional
/// `kind`, or `null` for the declared default, and, for an integration that declares
/// bindings, an object under its id of such members by name. Which a member is, and whether
/// it fits, is judged against the root's declarations by [`NamedBindings::judge`].
#[derive(Clone, Eq, PartialEq)]
pub struct BindingsFile {
    /// The file it was read from, which `bindings_file_invalid` names; absent when parsed.
    path: Option<PathBuf>,
    members: BTreeMap<String, Value>,
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
                integration: None,
                key: None,
                reason: "unreadable",
                source: Some(source),
            }
        })?;
        Self::from_bytes(&bytes, Some(path))
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, DefinitionError> {
        Self::from_bytes(bytes, None)
    }

    /// The file of `members`, as one already decoded, such as Cloud's answered snapshot;
    /// like a parsed one it has no path, so a `bindings_file_invalid` about it names none.
    pub fn from_object(members: serde_json::Map<String, Value>) -> Self {
        Self {
            path: None,
            members: members.into_iter().collect(),
        }
    }

    fn from_bytes(bytes: &[u8], path: Option<&Path>) -> Result<Self, DefinitionError> {
        let mut file = Self {
            path: path.map(Path::to_owned),
            members: BTreeMap::new(),
        };
        let Ok(Value::Object(members)) = serde_json::from_slice::<Value>(bytes) else {
            return Err(file.invalid(None, None, "not a JSON object"));
        };
        file.members = members.into_iter().collect();
        Ok(file)
    }

    fn invalid(
        &self,
        integration: Option<IntegrationProvider>,
        key: Option<&str>,
        reason: &'static str,
    ) -> DefinitionError {
        DefinitionError::BindingsFileInvalid {
            path: self.path.clone(),
            integration,
            key: key.map(str::to_owned),
            reason,
            source: None,
        }
    }

    /// What the member at `at`, flat or under an integration, names its declared binding for:
    /// its default when the member is `null`, else its value, the bare value or an object of
    /// `value` and an optional string `kind`, which must be the declared kind.
    fn judge(
        &self,
        at: BindingAt<'_>,
        member: &Value,
        declared: Declared<'_>,
    ) -> Result<Named, DefinitionError> {
        let (kind, value) = match member {
            Value::Null => return Ok(Named::Default),
            Value::Object(fields) => {
                if fields
                    .keys()
                    .any(|field| field != "kind" && field != "value")
                {
                    return Err(self.invalid(
                        at.integration,
                        Some(at.key),
                        "a member field other than kind and value",
                    ));
                }
                let kind = match fields.get("kind") {
                    None => None,
                    Some(Value::String(kind)) => Some(kind.as_str()),
                    Some(_) => {
                        return Err(self.invalid(
                            at.integration,
                            Some(at.key),
                            "a member kind that is not a string",
                        ));
                    }
                };
                let value = fields.get("value").ok_or_else(|| {
                    self.invalid(
                        at.integration,
                        Some(at.key),
                        "a member object without a value",
                    )
                })?;
                (kind, value)
            }
            value => (None, value),
        };
        declared
            .admit(at, kind, BindingValue::deserialize(value).ok())
            .map(Named::Value)
    }
}

/// What an invocation names a binding for (REQ-WFDEF-20, REQ-WFDEF-21).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Named {
    /// This value, which fits the declared kind.
    Value(BindingValue),
    /// The declared default.
    Default,
}

/// A binding the command line names (REQ-WFDEF-41): `--binding KEY=VALUE`, with the `VALUE`,
/// or `--binding-default KEY`, without one. `KEY` is a flat binding's key or an integration
/// binding's `<integration>.<name>`.
#[derive(Clone, Eq, PartialEq)]
pub struct BindingOption {
    pub key: String,
    pub value: Option<String>,
}

impl fmt::Debug for BindingOption {
    /// The key and whether a value was given, never the value.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BindingOption")
            .field("key", &self.key)
            .field("default", &self.value.is_none())
            .finish()
    }
}

impl BindingOption {
    /// Where the binding `KEY` names is declared: under an integration when the text before
    /// its last `.` is an integration that declares bindings, else in the flat block, where a
    /// dotted key is never declared.
    fn at(&self, declarations: Declarations<'_>) -> BindingAt<'_> {
        match BindingKey::split(&self.key) {
            BindingKey::Integration { integration, name } => declarations
                .integration(integration)
                .map_or(BindingAt::flat(&self.key), |provider| {
                    BindingAt::under(provider, name)
                }),
            BindingKey::Flat(key) => BindingAt::flat(key),
        }
    }

    /// What the option names its declared binding at `at` for: its default without a
    /// `VALUE`, else the `VALUE` fitted to the declared kind, a flat `members` binding's
    /// split at `,` into ids kept as written, none when it is empty.
    fn judge(&self, at: BindingAt<'_>, declared: Declared<'_>) -> Result<Named, DefinitionError> {
        let Some(value) = &self.value else {
            return Ok(Named::Default);
        };
        let value = match declared {
            Declared::Flat(binding) if binding.kind == BindingKind::Members => {
                BindingValue::List(if value.is_empty() {
                    Vec::new()
                } else {
                    value.split(',').map(str::to_owned).collect()
                })
            }
            Declared::Flat(_) | Declared::Integration(_) => BindingValue::Text(value.clone()),
        };
        declared.admit(at, None, Some(value)).map(Named::Value)
    }
}

/// The bindings an invocation names (REQ-WFDEF-20, REQ-WFDEF-41), judged against the root
/// before the home is touched: the flat block's by key and each integration's under its id
/// by name. A binding it does not name takes what REQ-WFDEF-21 gives it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NamedBindings {
    flat: BTreeMap<String, Named>,
    integrations: BTreeMap<IntegrationProvider, BTreeMap<String, Named>>,
}

impl NamedBindings {
    /// What `file` and then `options` name, each judged against `tree`'s declarations. In
    /// the file, a declared flat key is a flat member, the id of an integration that declares
    /// bindings an object of that integration's members by name, and any other key
    /// `binding_undeclared`. The whole file is judged first; each option, in the order given,
    /// then names its binding in place of the file's member for it. Without either nothing is
    /// named.
    pub fn judge(
        tree: &ValidatedTree,
        file: Option<&BindingsFile>,
        options: &[BindingOption],
    ) -> Result<Self, DefinitionError> {
        let declarations = Declarations {
            flat: &tree.bindings,
            integrations: &tree.required_integrations,
        };
        let mut named = Self::default();
        let members = file
            .iter()
            .flat_map(|file| file.members.iter().map(move |member| (file, member)));
        for (file, (key, member)) in members {
            let at = BindingAt::flat(key);
            let members = match declarations.integration(key) {
                Some(provider) if declarations.get(at).is_none() => {
                    let Value::Object(group) = member else {
                        return Err(file.invalid(
                            Some(provider),
                            None,
                            "an integration member that is not an object",
                        ));
                    };
                    group
                        .iter()
                        .map(|(name, member)| (BindingAt::under(provider, name), member))
                        .collect()
                }
                _ => vec![(at, member)],
            };
            for (at, member) in members {
                let declared = declarations.get(at).ok_or_else(|| at.undeclared())?;
                named.insert(at, file.judge(at, member, declared)?);
            }
        }
        for option in options {
            let at = option.at(declarations);
            let declared = declarations.get(at).ok_or_else(|| at.undeclared())?;
            named.insert(at, option.judge(at, declared)?);
        }
        Ok(named)
    }

    /// Whether the invocation names no binding.
    pub fn is_empty(&self) -> bool {
        self.flat.is_empty() && self.integrations.is_empty()
    }

    /// The flat bindings named, by key.
    pub fn bindings(&self) -> &BTreeMap<String, Named> {
        &self.flat
    }

    /// The integration bindings named, by integration and name.
    pub fn integration_bindings(&self) -> &BTreeMap<IntegrationProvider, BTreeMap<String, Named>> {
        &self.integrations
    }

    fn insert(&mut self, at: BindingAt<'_>, named: Named) {
        let names = match at.integration {
            None => &mut self.flat,
            Some(provider) => self.integrations.entry(provider).or_default(),
        };
        names.insert(at.key.to_owned(), named);
    }

    fn get(&self, at: BindingAt<'_>) -> Option<&Named> {
        match at.integration {
            None => self.flat.get(at.key),
            Some(provider) => self.integrations.get(&provider)?.get(at.key),
        }
    }
}

/// Where a binding is declared: the flat block, or an integration's bindings.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BindingAt<'a> {
    integration: Option<IntegrationProvider>,
    key: &'a str,
}

impl<'a> BindingAt<'a> {
    fn flat(key: &'a str) -> Self {
        Self {
            integration: None,
            key,
        }
    }

    fn under(integration: IntegrationProvider, key: &'a str) -> Self {
        Self {
            integration: Some(integration),
            key,
        }
    }

    /// The key a reference names the binding by after `$`, joined by [`BindingKey`].
    fn reference_key(self) -> String {
        match self.integration {
            None => BindingKey::Flat(self.key),
            Some(integration) => BindingKey::Integration {
                integration: integration.as_str(),
                name: self.key,
            },
        }
        .to_string()
    }

    fn undeclared(self) -> DefinitionError {
        DefinitionError::BindingUndeclared {
            integration: self.integration,
            key: self.key.to_owned(),
        }
    }

    fn kind_mismatch(self) -> DefinitionError {
        DefinitionError::BindingKindMismatch {
            integration: self.integration,
            key: self.key.to_owned(),
        }
    }

    fn value_invalid(self) -> DefinitionError {
        DefinitionError::BindingValueInvalid {
            integration: self.integration,
            key: self.key.to_owned(),
        }
    }

    fn missing(self) -> DefinitionError {
        DefinitionError::BindingMissing {
            integration: self.integration,
            key: self.key.to_owned(),
        }
    }
}

/// A declared binding as resolution judges a value for it.
#[derive(Clone, Copy)]
enum Declared<'a> {
    Flat(&'a Binding),
    Integration(&'a IntegrationBinding),
}

impl<'a> Declared<'a> {
    fn kind(self) -> &'a str {
        match self {
            Self::Flat(binding) => binding.kind.as_str(),
            Self::Integration(binding) => &binding.kind,
        }
    }

    /// Whether the binding may have no value: a `definition` binding without one has no
    /// entry in the snapshot (REQ-WFDEF-40).
    fn may_be_unbound(self) -> bool {
        matches!(self, Self::Flat(binding) if binding.kind == BindingKind::Definition)
    }

    fn default(self) -> Option<BindingValue> {
        match self {
            Self::Flat(binding) => binding.default.clone(),
            Self::Integration(binding) => binding.default.clone().map(BindingValue::Text),
        }
    }

    /// `value` for the binding at `at` when the `kind` given with it, if any, is the declared
    /// kind and it fits that kind: for an integration binding, a non-empty string.
    fn admit(
        self,
        at: BindingAt<'_>,
        kind: Option<&str>,
        value: Option<BindingValue>,
    ) -> Result<BindingValue, DefinitionError> {
        if kind.is_some_and(|kind| kind != self.kind()) {
            return Err(at.kind_mismatch());
        }
        value
            .filter(|value| match self {
                Self::Flat(binding) => value.fits(binding.kind),
                Self::Integration(_) => value.as_text().is_some_and(|text| !text.is_empty()),
            })
            .ok_or_else(|| at.value_invalid())
    }
}

/// The root's declared bindings: the flat block and each integration's.
#[derive(Clone, Copy)]
struct Declarations<'a> {
    flat: &'a BTreeMap<String, Binding>,
    integrations: &'a BTreeMap<IntegrationProvider, Integration>,
}

impl<'a> Declarations<'a> {
    fn get(self, at: BindingAt<'_>) -> Option<Declared<'a>> {
        match at.integration {
            None => self.flat.get(at.key).map(Declared::Flat),
            Some(provider) => self
                .integrations
                .get(&provider)?
                .bindings
                .get(at.key)
                .map(Declared::Integration),
        }
    }

    /// The integration whose id is `key`, when it declares bindings.
    fn integration(self, key: &str) -> Option<IntegrationProvider> {
        self.integrations
            .iter()
            .find(|(provider, integration)| {
                provider.as_str() == key && !integration.bindings.is_empty()
            })
            .map(|(provider, _)| *provider)
    }

    /// Every declared binding: the flat block in key order, then the integrations in id
    /// order and each one's names in bytewise order.
    fn all(self) -> impl Iterator<Item = (BindingAt<'a>, Declared<'a>)> {
        let flat = self
            .flat
            .iter()
            .map(|(key, binding)| (BindingAt::flat(key), Declared::Flat(binding)));
        let grouped = self
            .integrations
            .iter()
            .flat_map(|(provider, integration)| {
                integration.bindings.iter().map(|(name, binding)| {
                    (
                        BindingAt::under(*provider, name),
                        Declared::Integration(binding),
                    )
                })
            });
        flat.chain(grouped)
    }
}

/// The snapshot of the bindings the root declares, flat (`declared`) and under its
/// `integrations` (REQ-WFDEF-21). Each takes the value `named` gives it, its default when
/// `named` names it for its default, else the value the `current` snapshot holds for it with
/// the kind declared now, else its default; a `definition` binding left without one has no
/// entry (REQ-WFDEF-40). A current binding the root no longer declares, or declares with
/// another kind, is not kept. A value is recorded as written and never resolved.
pub(crate) fn resolve_bindings(
    declared: &BTreeMap<String, Binding>,
    integrations: &BTreeMap<IntegrationProvider, Integration>,
    named: &NamedBindings,
    current: Option<&BindingsSnapshot>,
) -> Result<BindingsSnapshot, DefinitionError> {
    let declarations = Declarations {
        flat: declared,
        integrations,
    };
    let mut kept = BTreeMap::<BindingAt<'_>, BindingValue>::new();
    if let Some(current) = current {
        for (at, kind, value) in current.members() {
            if declarations
                .get(at)
                .is_some_and(|declared| declared.kind() == kind)
            {
                kept.insert(at, value);
            }
        }
    }
    let mut snapshot = BindingsSnapshot::default();
    for (at, declared) in declarations.all() {
        let value = match named.get(at) {
            Some(Named::Value(value)) => Some(value.clone()),
            Some(Named::Default) => declared.default(),
            None => kept.remove(&at).or_else(|| declared.default()),
        };
        let Some(value) = value else {
            if declared.may_be_unbound() {
                continue;
            }
            return Err(at.missing());
        };
        let value = declared.admit(at, None, Some(value))?;
        match (declared, at.integration, value) {
            (Declared::Flat(binding), _, value) => {
                snapshot.flat.insert(
                    at.key.to_owned(),
                    BoundBinding {
                        kind: binding.kind,
                        value,
                    },
                );
            }
            (Declared::Integration(binding), Some(provider), BindingValue::Text(value)) => {
                snapshot.integrations.entry(provider).or_default().insert(
                    at.key.to_owned(),
                    BoundIntegrationBinding {
                        kind: binding.kind.clone(),
                        value,
                    },
                );
            }
            (Declared::Integration(_), _, _) => return Err(at.value_invalid()),
        }
    }
    Ok(snapshot)
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

/// Where a publish of a root records its revision (REQ-WFDEF-19): the root's `name` and the
/// record the target resolved to, none when the publish creates one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishPlace {
    name: String,
    resolved: Option<StoredDefinition>,
}

impl PublishPlace {
    /// The record as resolution read it, unlocked; none for a new record.
    pub fn record(&self) -> Option<&DefinitionRecord> {
        self.resolved.as_ref().map(|resolved| &resolved.record)
    }
}

/// The first half of a publish of `tree`: refuses a root without `name`
/// (`definition_name_missing`) and resolves `target` (REQ-WFDEF-19), reading the home without a
/// lock. `To` reads that record (`definition_id_invalid`, `definition_not_found`), `Resolve`
/// the one record of the root's name (`definition_name_ambiguous` for more than one) and
/// `New` none.
pub fn resolve_publish(
    home: &WorkflowHome,
    tree: &ValidatedTree,
    target: PublishTarget,
) -> Result<PublishPlace, DefinitionError> {
    let name = tree.name.clone().ok_or(DefinitionError::NameMissing)?;
    let resolved = match target {
        PublishTarget::To(id) => Some(read_definition(home, &id)?),
        PublishTarget::New => None,
        PublishTarget::Resolve => resolve_by_name(home, &name)?,
    };
    Ok(PublishPlace { name, resolved })
}

/// Records `tree` as a new revision of the target definition (REQ-WFDEF-19 to
/// REQ-WFDEF-22) with the bindings `named` sets and, for an existing definition, the rest
/// of its current snapshot that the root still declares with the same kind (REQ-WFDEF-21).
/// `by` is the publisher's login name, `unknown` when empty. Publishes to
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
    named: &NamedBindings,
    by: &str,
) -> Result<Published, DefinitionError> {
    let place = resolve_publish(home, tree, target)?;
    write_publication(home, tree, place, None, by, |existing| match existing {
        Some(existing) => resolve_bindings(
            &tree.bindings,
            &tree.required_integrations,
            named,
            Some(&read_current_bindings(existing)?),
        ),
        None => {
            let declarations = Declarations {
                flat: &tree.bindings,
                integrations: &tree.required_integrations,
            };
            if named.is_empty()
                && declarations
                    .all()
                    .any(|(_, declared)| declared.default().is_none() && !declared.may_be_unbound())
            {
                return Err(DefinitionError::BindingsRequired);
            }
            resolve_bindings(&tree.bindings, &tree.required_integrations, named, None)
        }
    })
}

/// Records `tree` at `place` as a Cloud publication (REQ-WFDEF-19, REQ-WFDEF-22,
/// REQ-CLOUDDEF-14): the revision's snapshot is `recorded`, the snapshot Cloud answered, and
/// the record's `cloudId` is `cloud_id`, set in the same record write, on a record this
/// creates too. The current snapshot is never read. `recorded` is judged as a bindings file
/// is (`binding_undeclared`, `binding_kind_mismatch`, `binding_value_invalid`) and must give
/// every declared binding a value, a `definition` binding included (`binding_missing`); a
/// `cloud_id` that is not a `def_` id is `definition_id_invalid`. The lock, the record read
/// again under it and the writes are `publish`'s, so a record extended since `place` was
/// resolved is extended and a held lock is `publish_conflict`.
pub fn record_publication(
    home: &WorkflowHome,
    tree: &ValidatedTree,
    place: PublishPlace,
    recorded: &BindingsFile,
    cloud_id: &str,
    by: &str,
) -> Result<Published, DefinitionError> {
    check_definition_id(cloud_id)?;
    write_publication(home, tree, place, Some(cloud_id), by, |_| {
        let named = NamedBindings::judge(tree, Some(recorded), &[])?;
        let declarations = Declarations {
            flat: &tree.bindings,
            integrations: &tree.required_integrations,
        };
        if let Some((at, _)) = declarations
            .all()
            .find(|(at, _)| !matches!(named.get(*at), Some(Named::Value(_))))
        {
            return Err(at.missing());
        }
        resolve_bindings(&tree.bindings, &tree.required_integrations, &named, None)
    })
}

/// The rest of a publish to `place`: takes the target's lock and reads its record again under
/// it, makes the revision's snapshot with `snapshot` from that record (none for a new one),
/// then writes the snapshot, the tree copy and the record, with `cloudId` set to `cloud_id`
/// when given.
fn write_publication(
    home: &WorkflowHome,
    tree: &ValidatedTree,
    place: PublishPlace,
    cloud_id: Option<&str>,
    by: &str,
    snapshot: impl FnOnce(Option<&StoredDefinition>) -> Result<BindingsSnapshot, DefinitionError>,
) -> Result<Published, DefinitionError> {
    let PublishPlace { name, resolved } = place;
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
    let snapshot = snapshot(existing.as_ref())?;
    let snapshot_bytes = snapshot
        .canonical_bytes()
        .map_err(|source| unavailable(home.root(), "encode bindings snapshot", source))?;
    let published_at = utc_timestamp(useful_support::utc_now()).map_err(|source| {
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
    if let Some(cloud_id) = cloud_id {
        record.cloud_id = Some(cloud_id.to_owned());
    }
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
    check_definition_id(id)?;
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

/// A refused or failed definition command, with the id, ids, binding key (and its integration,
/// for an integration binding) or path it concerns and never the contents of a tree file, bindings file or record (REQ-WFDEF-27).
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
    /// `integration` is the integration whose member of the file is refused, `key` the
    /// binding's name under it or the flat key.
    BindingsFileInvalid {
        path: Option<PathBuf>,
        integration: Option<IntegrationProvider>,
        key: Option<String>,
        reason: &'static str,
        source: Option<io::Error>,
    },
    BindingsRequired,
    /// In each binding refusal `integration` is absent for a flat binding and `key` is the
    /// flat key or the binding's name under the integration.
    BindingUndeclared {
        integration: Option<IntegrationProvider>,
        key: String,
    },
    BindingKindMismatch {
        integration: Option<IntegrationProvider>,
        key: String,
    },
    BindingValueInvalid {
        integration: Option<IntegrationProvider>,
        key: String,
    },
    BindingMissing {
        integration: Option<IntegrationProvider>,
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

    /// The binding the error concerns: the flat key, or the name under its integration.
    pub fn key(&self) -> Option<&str> {
        match self {
            Self::BindingUndeclared { key, .. }
            | Self::BindingKindMismatch { key, .. }
            | Self::BindingValueInvalid { key, .. }
            | Self::BindingMissing { key, .. } => Some(key),
            Self::BindingsFileInvalid { key, .. } => key.as_deref(),
            _ => None,
        }
    }

    /// The id of the integration whose binding, or member of the bindings file, the error
    /// concerns; absent for a flat binding.
    pub fn integration(&self) -> Option<&'static str> {
        match self {
            Self::BindingUndeclared { integration, .. }
            | Self::BindingKindMismatch { integration, .. }
            | Self::BindingValueInvalid { integration, .. }
            | Self::BindingMissing { integration, .. }
            | Self::BindingsFileInvalid { integration, .. } => {
                integration.map(IntegrationProvider::as_str)
            }
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
            Self::HomeUnavailable => formatter.write_str("neither USEFUL_HOME nor HOME is set"),
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
                path,
                integration,
                key,
                reason,
                ..
            } => {
                formatter.write_str("the bindings file")?;
                if let Some(path) = path {
                    write!(formatter, " {}", path.display())?;
                }
                formatter.write_str(" is not a JSON object of bindings")?;
                match (integration, key) {
                    (_, Some(key)) => write!(formatter, " at {}", named(*integration, key))?,
                    (Some(integration), None) => write!(formatter, " at {}", integration.as_str())?,
                    (None, None) => {}
                }
                write!(formatter, ": {reason}")
            }
            Self::BindingsRequired => formatter
                .write_str("the root declares a binding without a default and the publish names no binding; name its value in a bindings file"),
            Self::BindingUndeclared { integration, key } => {
                write!(
                    formatter,
                    "the root declares no binding {}",
                    named(*integration, key)
                )
            }
            Self::BindingKindMismatch { integration, key } => {
                write!(
                    formatter,
                    "the kind given for {} is not the declared kind",
                    named(*integration, key)
                )
            }
            Self::BindingValueInvalid { integration, key } => {
                write!(
                    formatter,
                    "the value of {} does not fit its declared kind",
                    named(*integration, key)
                )
            }
            Self::BindingMissing { integration, key } => write!(
                formatter,
                "no value is bound for {}",
                named(*integration, key)
            ),
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

/// A binding as a refusal names it: by its reference key, the flat key or `<id>.<name>`.
fn named(integration: Option<IntegrationProvider>, key: &str) -> String {
    BindingAt { integration, key }.reference_key()
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

/// Refuses `id` with `definition_id_invalid` unless it is `def_` and 26 characters
/// (REQ-WFDEF-27). Local and Cloud definition ids share the shape, so the check cannot tell
/// one from the other: `publish --cloud-to` checks a Cloud id with it (REQ-CLOUDDEF-20).
pub fn check_definition_id(id: &str) -> Result<(), DefinitionError> {
    if is_definition_id(id) {
        Ok(())
    } else {
        Err(DefinitionError::IdInvalid { id: id.to_owned() })
    }
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
