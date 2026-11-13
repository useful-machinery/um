use std::ffi::OsString;
use std::fs::{self, Permissions};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use regex::Regex;
use ring::digest::{SHA256, digest};
use serde_json::{Value, json};
use tempfile::TempDir;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::*;
use crate::workflow::canonical_json;
use crate::workflow::tree::validate_workflow_tree;

const ROOT_WORKFLOW: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/workflow/v1/valid/root-workflow.yaml"
));
const DESIGN_WORKFLOW: &str = r#"schemaVersion: 1
inputs:
  ticket:
    kind: text
steps:
  design:
    kind: cmd
    command:
      argv: ["design.py"]
"#;
const PLAIN_ROOT: &str = r#"schemaVersion: 1
name: plain
steps:
  prepare:
    kind: cmd
    command:
      argv: ["true"]
"#;
const MEMBER_ROOT: &str = r#"schemaVersion: 1
name: members
bindings:
  includedOwners: {kind: members, default: []}
  nightlyOwner: {kind: member}
steps:
  prepare:
    kind: cmd
    command:
      argv: ["true"]
"#;
const ID_PATTERN: &str = "^def_[0-7][0-9a-hjkmnp-tv-z]{25}$";
const DIGEST_PATTERN: &str = "^sha256:[0-9a-f]{64}$";

struct Tree {
    _temporary: TempDir,
    root: PathBuf,
}

impl Tree {
    fn engineering() -> Self {
        Self::with(ROOT_WORKFLOW)
    }

    fn with(root_workflow: &str) -> Self {
        let temporary = tempfile::tempdir().expect("temporary tree should be created");
        let root = temporary.path().join("engineering");
        let tree = Self {
            _temporary: temporary,
            root,
        };
        tree.write("workflow.yaml", root_workflow);
        tree.write("design.yaml", DESIGN_WORKFLOW);
        tree.write("scripts/design.py", "print('design')\n");
        tree.write("policy/engineering.json", "{\"note\": \"policy\"}\n");
        tree.write("tests/test_x.py", "def test_x():\n    pass\n");
        tree
    }

    fn plain() -> Self {
        let temporary = tempfile::tempdir().expect("temporary tree should be created");
        let root = temporary.path().join("plain");
        let tree = Self {
            _temporary: temporary,
            root,
        };
        tree.write("workflow.yaml", PLAIN_ROOT);
        tree
    }

    fn write(&self, relative: &str, contents: &str) {
        let path = self.root.join(relative);
        fs::create_dir_all(path.parent().expect("tree file should have a parent"))
            .expect("tree directory should be created");
        fs::write(path, contents).expect("tree file should be written");
    }

    fn validated(&self) -> ValidatedTree {
        validate_workflow_tree(&self.root)
            .unwrap_or_else(|diagnostics| panic!("tree should be valid: {diagnostics:?}"))
    }
}

/// A temporary `um` home; the read-only tree copies are made writable again before the
/// directory is removed.
struct Home {
    temporary: TempDir,
    home: WorkflowHome,
}

impl Home {
    fn new() -> Self {
        let temporary = tempfile::tempdir().expect("temporary home should be created");
        let root = temporary.path().join("um-home");
        let home = WorkflowHome::from_lookup(|name| {
            (name == "UM_HOME").then(|| root.clone().into_os_string())
        })
        .expect("home should resolve");
        Self { temporary, home }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.home.root().join(relative)
    }

    fn definition_bytes(&self, id: &str) -> Vec<u8> {
        fs::read(self.home.definition_directory(id).join("definition.json"))
            .expect("definition record should be readable")
    }

    fn snapshot_bytes(&self, id: &str, n: u64) -> Vec<u8> {
        fs::read(
            self.home
                .definition_directory(id)
                .join(format!("revisions/{n}/bindings.json")),
        )
        .expect("bindings snapshot should be readable")
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        restore_writable(self.temporary.path());
    }
}

fn restore_writable(path: &Path) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    if metadata.is_dir() {
        let _ = fs::set_permissions(path, Permissions::from_mode(0o700));
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                restore_writable(&entry.path());
            }
        }
    }
}

fn bindings(value: Value) -> BindingsFile {
    BindingsFile::parse(&serde_json::to_vec(&value).expect("bindings should encode"))
        .expect("bindings file should parse")
}

fn engineering_bindings() -> BindingsFile {
    bindings(json!({
        "connection": "local",
        "linearTeam": "AUTO",
        "repository": "useful-machinery/platform",
    }))
}

fn publish_engineering(home: &Home, target: PublishTarget) -> Published {
    publish(
        &home.home,
        &Tree::engineering().validated(),
        target,
        Some(&engineering_bindings()),
        "tester",
    )
    .expect("publish should succeed")
}

fn schema_validator(file: &str) -> jsonschema::Validator {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../schemas")
        .join(file);
    let schema = serde_json::from_slice::<Value>(&fs::read(path).expect("schema should be read"))
        .expect("schema should be JSON");
    jsonschema::draft202012::new(&schema).expect("schema should compile")
}

fn assert_records_match_schemas(home: &Home, id: &str, n: u64) {
    let definition =
        serde_json::from_slice::<Value>(&home.definition_bytes(id)).expect("record should be JSON");
    assert!(
        schema_validator("workflow-definition-v1.schema.json").is_valid(&definition),
        "{definition}"
    );
    let snapshot = serde_json::from_slice::<Value>(&home.snapshot_bytes(id, n))
        .expect("snapshot should be JSON");
    assert!(
        schema_validator("workflow-definition-bindings-v1.schema.json").is_valid(&snapshot),
        "{snapshot}"
    );
}

fn lookup<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<OsString> + 'a {
    move |name| {
        pairs
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| OsString::from(value))
    }
}

// REQ-WFDEF-17
#[test]
fn home_is_um_home_when_set_and_not_empty_else_home_dot_um() {
    let home = WorkflowHome::from_lookup(lookup(&[("UM_HOME", "/srv/um"), ("HOME", "/h")]))
        .expect("UM_HOME should resolve");
    assert_eq!(home.root(), Path::new("/srv/um"));
    assert_eq!(home.workflows_directory(), Path::new("/srv/um/workflows"));
    assert_eq!(
        home.definition_directory("def_01jabcdefghjkmnpqrstvwxyz0"),
        Path::new("/srv/um/workflows/def_01jabcdefghjkmnpqrstvwxyz0")
    );
    let hex = "a".repeat(64);
    assert_eq!(
        home.tree_directory(&format!("sha256:{hex}")),
        Some(PathBuf::from(format!("/srv/um/trees/sha256/{hex}")))
    );

    for pairs in [
        &[("HOME", "/h")][..],
        &[("UM_HOME", ""), ("HOME", "/h")][..],
    ] {
        let home = WorkflowHome::from_lookup(lookup(pairs)).expect("HOME should resolve");
        assert_eq!(home.root(), Path::new("/h/.um"));
    }

    for pairs in [&[][..], &[("UM_HOME", ""), ("HOME", "")][..]] {
        let error = WorkflowHome::from_lookup(lookup(pairs)).expect_err("no home should resolve");
        assert_eq!(error.code(), "home_unavailable");
    }
}

// REQ-WFDEF-17
#[test]
fn published_tree_is_a_read_only_copy_under_its_digest_without_excluded_files() {
    let home = Home::new();
    let published = publish_engineering(&home, PublishTarget::Resolve);
    let hex = published
        .revision
        .tree
        .strip_prefix("sha256:")
        .expect("tree digest should be sha256");
    let copy = home.path(&format!("trees/sha256/{hex}"));
    assert_eq!(
        home.home.tree_directory(&published.revision.tree),
        Some(copy.clone())
    );
    for (file, mode) in [
        ("workflow.yaml", 0o444),
        ("design.yaml", 0o444),
        ("scripts/design.py", 0o444),
        ("policy/engineering.json", 0o444),
    ] {
        let metadata = fs::metadata(copy.join(file)).expect("kept file should be copied");
        assert_eq!(metadata.permissions().mode() & 0o777, mode, "{file}");
    }
    for directory in ["", "scripts", "policy"] {
        let metadata = fs::metadata(copy.join(directory)).expect("directory should exist");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o555, "{directory}");
    }
    assert!(!copy.join("tests").exists());
    let entries = fs::read_dir(home.path("trees/sha256"))
        .expect("trees directory should be listed")
        .count();
    assert_eq!(entries, 1, "no staging copy should remain");
    for directory in [
        home.home.root().to_owned(),
        home.home.workflows_directory(),
        published.path.clone(),
    ] {
        let metadata = fs::metadata(&directory).expect("home directory should exist");
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            0o700,
            "{directory:?}"
        );
    }
}

// REQ-WFDEF-19
#[test]
fn first_publish_mints_an_id_and_a_second_publish_of_the_name_adds_to_it() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    assert!(first.created);
    assert!(Regex::new(ID_PATTERN).unwrap().is_match(&first.record.id));
    assert_eq!(first.record.name, "engineering");
    assert_eq!(first.path, home.home.definition_directory(&first.record.id));

    let tree = Tree::engineering().validated();
    let second = publish(&home.home, &tree, PublishTarget::Resolve, None, "tester")
        .expect("second publish should succeed");
    assert!(!second.created);
    assert_eq!(second.record.id, first.record.id);
    assert_eq!(second.revision.n, 2);
    assert_eq!(
        list_definitions(&home.home)
            .expect("definitions should list")
            .len(),
        1
    );
}

// REQ-WFDEF-19
#[test]
fn new_beside_a_name_mints_a_second_and_resolution_then_refuses_naming_both() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let second = publish_engineering(&home, PublishTarget::New);
    assert!(second.created);
    assert_ne!(second.record.id, first.record.id);
    assert_eq!(second.record.name, "engineering");

    let error = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        Some(&engineering_bindings()),
        "tester",
    )
    .expect_err("an ambiguous name should refuse");
    assert_eq!(error.code(), "definition_name_ambiguous");
    let mut expected = vec![first.record.id.clone(), second.record.id.clone()];
    expected.sort();
    assert_eq!(error.ids(), expected.as_slice());

    let to = publish_engineering(&home, PublishTarget::To(first.record.id.clone()));
    assert_eq!(to.record.id, first.record.id);
    assert_eq!(to.revision.n, 2);
}

// REQ-WFDEF-19
#[test]
fn to_an_unknown_id_is_not_found_and_to_a_differently_named_record_renames_it() {
    let home = Home::new();
    let unknown = "def_01jabcdefghjkmnpqrstvwxyz0";
    let error = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::To(unknown.to_owned()),
        Some(&engineering_bindings()),
        "tester",
    )
    .expect_err("an unknown id should refuse");
    assert_eq!(error.code(), "definition_not_found");
    assert_eq!(error.id(), Some(unknown));
    assert!(!home.path("workflows").exists());

    let error = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::To("engineering".to_owned()),
        Some(&engineering_bindings()),
        "tester",
    )
    .expect_err("a name is not an id");
    assert_eq!(error.code(), "definition_id_invalid");

    let first = publish_engineering(&home, PublishTarget::Resolve);
    let renamed_root = ROOT_WORKFLOW.replacen("name: engineering", "name: lifecycle", 1);
    let renamed = publish(
        &home.home,
        &Tree::with(&renamed_root).validated(),
        PublishTarget::To(first.record.id.clone()),
        None,
        "tester",
    )
    .expect("publish to the id should succeed");
    assert_eq!(renamed.record.id, first.record.id);
    assert_eq!(renamed.record.name, "lifecycle");
    let read = read_definition(&home.home, &first.record.id).expect("record should read");
    assert_eq!(read.record.name, "lifecycle");
}

// REQ-WFDEF-18
#[test]
fn a_root_without_a_name_is_refused() {
    let home = Home::new();
    let tree = Tree::with(&ROOT_WORKFLOW.replacen("name: engineering\n", "", 1));
    let error = publish(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        Some(&engineering_bindings()),
        "tester",
    )
    .expect_err("a nameless root should refuse");
    assert_eq!(error.code(), "definition_name_missing");
    assert!(!home.path("workflows").exists());
}

// REQ-WFDEF-20
#[test]
fn bindings_file_fills_defaults_into_a_snapshot_of_every_declared_binding() {
    let home = Home::new();
    let published = publish_engineering(&home, PublishTarget::Resolve);
    let snapshot = serde_json::from_slice::<Value>(&home.snapshot_bytes(&published.record.id, 1))
        .expect("snapshot should be JSON");
    assert_eq!(
        snapshot,
        json!({
            "backlog": {"kind": "linear.state", "value": "Backlog"},
            "canceled": {"kind": "linear.state", "value": "Canceled"},
            "connection": {"kind": "connection", "value": "local"},
            "done": {"kind": "linear.state", "value": "Done"},
            "linearTeam": {"kind": "linear.team", "value": "AUTO"},
            "ownedState": {"kind": "linear.state", "value": "Engineering"},
            "repository": {"kind": "github.repository", "value": "useful-machinery/platform"},
        })
    );
    assert_eq!(published.snapshot.bindings().len(), 7);
    assert_records_match_schemas(&home, &published.record.id, 1);
}

// REQ-WFDEF-20
#[test]
fn bindings_file_members_are_checked_against_the_declared_bindings() {
    let declared = Tree::engineering().validated().bindings;
    let resolve = |value: Value| resolve_bindings(&declared, Some(&bindings(value)), None);

    let snapshot = resolve(json!({
        "connection": {"kind": "connection", "value": "local"},
        "linearTeam": {"value": "AUTO"},
        "repository": "useful-machinery/platform",
        "done": {"kind": "linear.state", "value": "Shipped"},
    }))
    .expect("the object form should be accepted");
    assert_eq!(snapshot.bindings()["done"].value.as_text(), Some("Shipped"));
    assert_eq!(
        snapshot.bindings()["linearTeam"].kind,
        BindingKind::LinearTeam
    );

    for (value, code, key) in [
        (
            json!({"connection": {"kind": "linear.team", "value": "local"}, "linearTeam": "AUTO", "repository": "r"}),
            "binding_kind_mismatch",
            "connection",
        ),
        (
            json!({"connection": "local", "linearTeam": "AUTO", "repository": "r", "extra": "x"}),
            "binding_undeclared",
            "extra",
        ),
        (
            json!({"connection": "local", "linearTeam": "", "repository": "r"}),
            "binding_value_invalid",
            "linearTeam",
        ),
        (
            json!({"connection": "local", "linearTeam": {"value": 7}, "repository": "r"}),
            "binding_value_invalid",
            "linearTeam",
        ),
        (
            json!({"connection": "local", "linearTeam": "AUTO"}),
            "binding_missing",
            "repository",
        ),
    ] {
        let error = resolve(value).expect_err("the file should refuse");
        assert_eq!((error.code(), error.key()), (code, Some(key)));
    }

    for bytes in [
        &b"[]"[..],
        b"\"x\"",
        b"{",
        b"",
        b"{\"connection\": {\"value\": \"x\", \"other\": 1}}",
    ] {
        let error = BindingsFile::parse(bytes).expect_err("a non-object file should refuse");
        assert_eq!(error.code(), "bindings_file_invalid");
    }
}

// REQ-WFDEF-3, REQ-WFDEF-20, REQ-WFDEF-29
#[test]
fn member_and_members_bindings_publish_typed_values_kept_as_written() {
    let home = Home::new();
    let tree = Tree::with(MEMBER_ROOT).validated();
    let publish_with = |file: Option<Value>| {
        publish(
            &home.home,
            &tree,
            PublishTarget::Resolve,
            file.map(bindings).as_ref(),
            "tester",
        )
        .expect("publish should succeed")
    };

    let first = publish_with(Some(json!({"nightlyOwner": "prn_x"})));
    let id = first.record.id.clone();
    let snapshot = serde_json::from_slice::<Value>(&home.snapshot_bytes(&id, 1))
        .expect("snapshot should be JSON");
    assert_eq!(
        snapshot,
        json!({
            "includedOwners": {"kind": "members", "value": []},
            "nightlyOwner": {"kind": "member", "value": "prn_x"},
        })
    );
    assert_records_match_schemas(&home, &id, 1);

    let second = publish_with(Some(json!({"nightlyOwner": "prn_x"})));
    assert_eq!(
        second.record.revisions[0].bindings,
        first.record.revisions[0].bindings
    );

    let third = publish_with(Some(
        json!({"nightlyOwner": "prn_x", "includedOwners": ["prn_a"]}),
    ));
    assert_eq!(
        third.snapshot.bindings()["includedOwners"].value,
        BindingValue::List(vec!["prn_a".to_owned()])
    );
    assert_ne!(
        third.record.revisions[0].bindings,
        first.record.revisions[0].bindings
    );
    assert_records_match_schemas(&home, &id, 3);

    // Carried forward without a file, the list is read back as a list.
    let fourth = publish_with(None);
    assert_eq!(home.snapshot_bytes(&id, 4), home.snapshot_bytes(&id, 3));
    assert_eq!(fourth.snapshot, third.snapshot);

    for (value, key) in [
        (
            json!({"nightlyOwner": "prn_x", "includedOwners": "prn_a"}),
            "includedOwners",
        ),
        (json!({"nightlyOwner": ["prn_x"]}), "nightlyOwner"),
        (
            json!({"nightlyOwner": "prn_x", "includedOwners": [""]}),
            "includedOwners",
        ),
        (
            json!({"nightlyOwner": "prn_x", "includedOwners": {"value": [7]}}),
            "includedOwners",
        ),
    ] {
        let error = resolve_bindings(&tree.bindings, Some(&bindings(value)), None)
            .expect_err("a value that does not fit its kind should refuse");
        assert_eq!(
            (error.code(), error.key()),
            ("binding_value_invalid", Some(key))
        );
    }

    let empty_member_default =
        Tree::with(&MEMBER_ROOT.replace("default: []", "default: [\"\"]")).validated();
    let error = resolve_bindings(
        &empty_member_default.bindings,
        Some(&bindings(json!({"nightlyOwner": "prn_x"}))),
        None,
    )
    .expect_err("an empty member in a default should refuse");
    assert_eq!(
        (error.code(), error.key()),
        ("binding_value_invalid", Some("includedOwners"))
    );
}

// REQ-WFDEF-21
#[test]
fn a_first_publish_with_a_required_binding_needs_a_file_and_a_root_without_one_does_not() {
    let home = Home::new();
    let error = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect_err("a required binding without a file should refuse");
    assert_eq!(error.code(), "bindings_required");
    assert!(!home.path("workflows").exists());

    let published = publish(
        &home.home,
        &Tree::plain().validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect("a root without required bindings should publish");
    assert!(published.snapshot.bindings().is_empty());
    assert_eq!(home.snapshot_bytes(&published.record.id, 1), b"{}");
    assert_records_match_schemas(&home, &published.record.id, 1);
}

// REQ-WFDEF-21
#[test]
fn a_later_publish_carries_the_snapshot_forward_and_drops_undeclared_keys() {
    let home = Home::new();
    let first = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        Some(&bindings(json!({
            "connection": "local",
            "linearTeam": "AUTO",
            "repository": "useful-machinery/platform",
            "done": "Shipped",
        }))),
        "tester",
    )
    .expect("first publish should succeed");

    let without_repository =
        ROOT_WORKFLOW.replacen("  repository:      {kind: github.repository}\n", "", 1);
    let second = publish(
        &home.home,
        &Tree::with(&without_repository).validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect("a carried publish should succeed");
    assert_eq!(second.record.id, first.record.id);
    let snapshot = second.snapshot.bindings();
    assert!(!snapshot.contains_key("repository"));
    assert_eq!(snapshot.len(), 6);
    assert_eq!(snapshot["done"].value.as_text(), Some("Shipped"));
    assert_eq!(snapshot["linearTeam"].value.as_text(), Some("AUTO"));

    let with_new_binding = without_repository.replacen(
        "  done:",
        "  reviewer:        {kind: connection}\n  done:",
        1,
    );
    let error = publish(
        &home.home,
        &Tree::with(&with_new_binding).validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect_err("a newly required binding should refuse");
    assert_eq!(
        (error.code(), error.key()),
        ("binding_missing", Some("reviewer"))
    );
}

// REQ-WFDEF-22
#[test]
fn each_publish_records_a_new_first_revision_with_digests_of_tree_and_snapshot() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let tree = Tree::engineering();
    let second = publish(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        None,
        "op",
    )
    .expect("second publish should succeed");

    let record = read_definition(&home.home, &first.record.id)
        .expect("record should read")
        .record;
    assert_eq!(record, second.record);
    assert_eq!(record.current, 2);
    assert!(record.enabled);
    assert_eq!(record.cloud_id, None);
    assert_eq!(
        record.revisions.iter().map(|r| r.n).collect::<Vec<_>>(),
        vec![2, 1]
    );
    let revision = &record.revisions[0];
    assert_eq!(revision, &second.revision);
    assert_eq!(revision.by, "op");
    assert_eq!(record.revisions[1].by, "tester");
    assert!(revision.published_at.ends_with('Z'));
    OffsetDateTime::parse(&revision.published_at, &Rfc3339).expect("publishedAt is RFC 3339");
    assert_eq!(
        revision.tree,
        format!("sha256:{}", tree.validated().digest.value)
    );
    assert!(
        Regex::new(DIGEST_PATTERN)
            .unwrap()
            .is_match(&revision.bindings)
    );

    let bytes = home.snapshot_bytes(&record.id, 2);
    let mut canonical = Vec::new();
    canonical_json::to_writer(
        &mut canonical,
        &serde_json::from_slice(&bytes).expect("snapshot should be JSON"),
    )
    .expect("snapshot should re-encode");
    assert_eq!(bytes, canonical);
    assert_eq!(
        revision.bindings,
        format!("sha256:{}", lowercase_hex(digest(&SHA256, &bytes).as_ref()))
    );
    assert_eq!(revision.bindings, record.revisions[1].bindings);
    assert_eq!(bytes, home.snapshot_bytes(&record.id, 1));

    let raw = serde_json::from_slice::<Value>(&home.definition_bytes(&record.id))
        .expect("record should be JSON");
    assert_eq!(raw["cloudId"], Value::Null);
    assert_eq!(raw["schemaVersion"], 1);
    assert_records_match_schemas(&home, &record.id, 2);
}

// REQ-WFDEF-22
#[test]
fn republishing_the_same_or_an_older_tree_is_a_new_ordinal_with_its_digest() {
    let home = Home::new();
    let tree = Tree::engineering();
    let original = publish(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        Some(&engineering_bindings()),
        "tester",
    )
    .expect("first publish should succeed");
    let same = publish(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect("same tree should publish");
    assert_eq!(same.revision.n, 2);
    assert_eq!(same.revision.tree, original.revision.tree);

    tree.write("scripts/design.py", "print('changed')\n");
    let changed = publish(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect("changed tree should publish");
    assert_eq!(changed.revision.n, 3);
    assert_ne!(changed.revision.tree, original.revision.tree);

    tree.write("scripts/design.py", "print('design')\n");
    let older = publish(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect("older tree should publish");
    assert_eq!(older.revision.n, 4);
    assert_eq!(older.revision.tree, original.revision.tree);
    assert_eq!(older.record.current, 4);
    assert_eq!(older.record.revisions.len(), 4);
    let copies = fs::read_dir(home.path("trees/sha256"))
        .expect("trees should list")
        .count();
    assert_eq!(copies, 2);
}

// REQ-WFDEF-22
#[test]
fn a_revision_directory_a_crashed_publish_left_is_skipped_by_the_next_ordinal() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    // A publish that crashed after making `revisions/2` and its snapshot, before the record.
    let leftover = first.path.join("revisions/2");
    fs::create_dir(&leftover).expect("leftover directory should be made");
    fs::write(leftover.join("bindings.json"), b"{}").expect("leftover snapshot should write");
    fs::create_dir(first.path.join("revisions/not-a-number"))
        .expect("stray directory should be made");

    let second = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect("a leftover revision directory should not block the next publish");
    assert_eq!(second.revision.n, 3);
    assert_eq!(second.record.current, 3);
    assert_eq!(
        second
            .record
            .revisions
            .iter()
            .map(|r| r.n)
            .collect::<Vec<_>>(),
        vec![3, 1]
    );
    assert_eq!(
        fs::read(leftover.join("bindings.json")).expect("leftover should remain"),
        b"{}"
    );
    assert_eq!(
        home.snapshot_bytes(&first.record.id, 3),
        home.snapshot_bytes(&first.record.id, 1)
    );
}

// REQ-WFDEF-22, REQ-WFDEF-25
#[test]
fn a_first_publish_that_stopped_before_its_record_blocks_neither_listing_nor_a_later_publish() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    // A first publish that stopped after its snapshot and before its record: the definition
    // is still under its staging name, with a partial record and no `definition.json`.
    let stopped = mint_definition_id();
    let staging = home.path(&format!("workflows/.staging-{stopped}"));
    fs::create_dir_all(staging.join("revisions/1")).expect("staging revision should be made");
    fs::write(staging.join("revisions/1/bindings.json"), b"{}")
        .expect("staging snapshot should write");
    fs::write(staging.join("definition.json.tmp"), b"{").expect("partial record should write");

    let listed =
        list_definitions(&home.home).expect("a stopped first publish should not stop the listing");
    assert_eq!(
        listed
            .iter()
            .map(|definition| definition.record.id.as_str())
            .collect::<Vec<_>>(),
        vec![first.record.id.as_str()]
    );
    let error = read_definition(&home.home, &stopped).expect_err("the id was never placed");
    assert_eq!(error.code(), "definition_not_found");

    let second = publish(
        &home.home,
        &Tree::plain().validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect("a stopped first publish should not block a later publish by name");
    assert!(second.created);
    assert_eq!(
        second.path,
        home.home.definition_directory(&second.record.id)
    );
    assert!(second.path.join("definition.json").is_file());
    assert!(second.path.join("revisions/1/bindings.json").is_file());
    assert!(
        !home
            .path(&format!("workflows/.staging-{}", second.record.id))
            .exists(),
        "a placed definition leaves no staging directory"
    );
    // The leftover is neither recorded nor removed.
    assert!(staging.join("revisions/1/bindings.json").is_file());
    assert!(!home.path(&format!("workflows/{stopped}")).exists());
    let listed = list_definitions(&home.home).expect("listing should go on");
    assert_eq!(
        listed
            .iter()
            .map(|definition| definition.record.id.as_str())
            .collect::<Vec<_>>(),
        vec![first.record.id.as_str(), second.record.id.as_str()]
    );
}

// REQ-WFDEF-22
#[test]
fn a_publish_while_another_holds_the_definition_lock_is_a_publish_conflict_and_changes_nothing() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let before = home.definition_bytes(&first.record.id);
    // Another publish holds the lock: it is between its record read and its record rename.
    let other = PublishLock::acquire(&first.path, &first.record.id).expect("lock should be free");
    assert_eq!(
        fs::metadata(first.path.join("publish.lock"))
            .expect("a publish to an existing definition makes the lock file")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    for target in [
        PublishTarget::Resolve,
        PublishTarget::To(first.record.id.clone()),
    ] {
        let error = publish(
            &home.home,
            &Tree::engineering().validated(),
            target,
            None,
            "tester",
        )
        .expect_err("a publish while another holds the lock should refuse");
        assert_eq!(error.code(), "publish_conflict");
        assert_eq!(error.id(), Some(first.record.id.as_str()));
    }
    assert_eq!(home.definition_bytes(&first.record.id), before);
    assert!(!first.path.join("revisions/2").exists());

    drop(other);
    let second = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect("the lock released, the next publish records its revision");
    assert_eq!(second.revision.n, 2);
    assert_eq!(
        second
            .record
            .revisions
            .iter()
            .map(|r| r.n)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(
        read_definition(&home.home, &first.record.id)
            .expect("record should read")
            .record,
        second.record
    );
}

// REQ-WFDEF-22
#[test]
fn a_revision_directory_taken_by_a_writer_without_the_lock_is_a_publish_conflict() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let before = home.definition_bytes(&first.record.id);
    // A writer that did not hold the lock made `revisions/2` after this one chose the ordinal.
    fs::create_dir(first.path.join("revisions/2")).expect("revision directory should be made");
    let mut record = first.record.clone();
    record.revisions.insert(
        0,
        RevisionRecord {
            n: 2,
            ..first.revision.clone()
        },
    );
    record.current = 2;

    let error = write_revision(
        &home.home,
        &Tree::engineering().validated(),
        &first.path,
        &record,
        2,
        b"{}",
    )
    .expect_err("a taken ordinal should refuse");
    assert_eq!(error.code(), "publish_conflict");
    assert_eq!(error.id(), Some(first.record.id.as_str()));
    assert_eq!(home.definition_bytes(&first.record.id), before);
}

// REQ-WFDEF-27
#[test]
fn an_unreadable_record_stops_resolution_and_listing_naming_its_path() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let record = first.path.join("definition.json");
    let mut value = serde_json::from_slice::<Value>(&home.definition_bytes(&first.record.id))
        .expect("record should be JSON");
    value["current"] = json!(9);
    fs::write(&record, serde_json::to_vec(&value).unwrap()).expect("record should be replaced");

    let error = list_definitions(&home.home).expect_err("an invalid record should refuse");
    assert_eq!(error.code(), "definition_record_invalid");
    assert_eq!(error.path(), Some(record.as_path()));
    let error = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect_err("an invalid record should stop resolution");
    assert_eq!(error.code(), "definition_record_invalid");

    fs::write(&record, b"{}").expect("record should be replaced");
    let error = read_definition(&home.home, &first.record.id).expect_err("{} is not a record");
    assert_eq!(error.code(), "definition_record_invalid");
}

// REQ-WFDEF-3, REQ-WFDEF-22
#[test]
fn a_revision_s_snapshot_reads_back_after_a_later_publish_changed_the_bindings() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let id = first.record.id.clone();
    let second = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        Some(&bindings(json!({
            "connection": "local",
            "linearTeam": "AUTO",
            "repository": "useful-machinery/platform",
            "done": "Shipped",
        }))),
        "tester",
    )
    .expect("second publish should succeed");
    assert_eq!(second.record.current, 2);

    let one = read_revision_bindings(&home.home, &id, 1).expect("revision 1 should read");
    assert_eq!(one, first.snapshot);
    assert_eq!(one.bindings()["done"].value.as_text(), Some("Done"));
    let two = read_revision_bindings(&home.home, &id, 2).expect("revision 2 should read");
    assert_eq!(two.bindings()["done"].value.as_text(), Some("Shipped"));

    let unlisted = read_revision_bindings(&home.home, &id, 3)
        .expect_err("a revision the record does not list should refuse");
    assert_eq!(unlisted.code(), "definition_record_invalid");

    let snapshot = home
        .home
        .definition_directory(&id)
        .join("revisions/1/bindings.json");
    let tampered = String::from_utf8(home.snapshot_bytes(&id, 1))
        .expect("snapshot should be UTF-8")
        .replace("\"Done\"", "\"Shipped\"");
    restore_writable(&home.home.definition_directory(&id));
    fs::set_permissions(&snapshot, Permissions::from_mode(0o600))
        .expect("snapshot should become writable");
    fs::write(&snapshot, tampered).expect("snapshot should be replaced");
    let error = read_revision_bindings(&home.home, &id, 1)
        .expect_err("a snapshot unlike its digest should refuse");
    assert_eq!(error.code(), "definition_record_invalid");
    assert_eq!(error.path(), Some(snapshot.as_path()));
}

// REQ-WFDEF-32, REQ-WFDEF-7: publish never writes a revision's resolution record, so a
// revision without one reads none; one beside `bindings.json` reads back as the resolved id
// by binding key, and one not of the record's shape is `definition_record_invalid` at its
// path.
#[test]
fn a_revision_s_resolution_record_is_optional_and_read_by_binding_key() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let id = first.record.id.clone();
    let revision = home.home.definition_directory(&id).join("revisions/1");
    assert!(!revision.join("resolution.json").exists());
    assert_eq!(
        home.home
            .revision_resolution(&id, 1)
            .expect("a revision without a record should read"),
        None
    );

    let record = revision.join("resolution.json");
    fs::write(
        &record,
        json!({
            "linearTeam": {"id": "team-1", "name": "AUTO"},
            "done": {"id": "state-done", "name": "Done"},
        })
        .to_string(),
    )
    .expect("resolution record should be written");
    let resolved = home
        .home
        .revision_resolution(&id, 1)
        .expect("the record should read")
        .expect("the record should be present");
    assert_eq!(
        resolved,
        BTreeMap::from([
            ("done".to_owned(), "state-done".to_owned()),
            ("linearTeam".to_owned(), "team-1".to_owned()),
        ])
    );

    for invalid in [
        json!([]),
        json!({"done": "state-done"}),
        json!({"done": {"id": "state-done"}}),
        json!({"done": {"id": "", "name": "Done"}}),
        json!({"done": {"id": "state-done", "name": "Done", "extra": 1}}),
        json!({"not a key": {"id": "state-done", "name": "Done"}}),
    ] {
        fs::write(&record, invalid.to_string()).expect("resolution record should be replaced");
        let error = home
            .home
            .revision_resolution(&id, 1)
            .expect_err("a record not of the shape should refuse");
        assert_eq!(error.code(), "definition_record_invalid", "{invalid}");
        assert_eq!(error.path(), Some(record.as_path()), "{invalid}");
    }

    let unlisted = home
        .home
        .revision_resolution(&id, 2)
        .expect_err("a revision the record does not list should refuse");
    assert_eq!(unlisted.code(), "definition_record_invalid");

    // A later publish writes its own revision without a record and leaves revision 1's.
    publish_engineering(&home, PublishTarget::Resolve);
    assert_eq!(home.home.revision_resolution(&id, 2).unwrap(), None);
    assert!(record.exists());
}

const CLOUD_ID: &str = "def_01k0z6r1w8f4jy2m7q9v3x5abc";
const OTHER_CLOUD_ID: &str = "def_01k0z6r1w8f4jy2m7q9v3x5abd";

// REQ-CLOUDDEF-14, REQ-WFDEF-7, REQ-WFDEF-22
#[test]
fn set_cloud_id_replaces_the_record_whole_and_a_later_publish_keeps_it() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let id = first.record.id.clone();
    assert_eq!(first.record.cloud_id, None);

    let set = home
        .home
        .set_cloud_id(&id, CLOUD_ID)
        .expect("cloud id should be recorded");
    let mut expected = first.record.clone();
    expected.cloud_id = Some(CLOUD_ID.to_owned());
    assert_eq!(set.record, expected);
    assert_eq!(
        read_definition(&home.home, &id)
            .expect("record should read")
            .record,
        expected
    );
    assert_records_match_schemas(&home, &id, 1);

    let second = publish(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        None,
        "tester",
    )
    .expect("a later publish should succeed");
    assert_eq!(second.record.cloud_id.as_deref(), Some(CLOUD_ID));

    let replaced = home
        .home
        .set_cloud_id(&id, OTHER_CLOUD_ID)
        .expect("another cloud id should replace the first");
    assert_eq!(replaced.record.cloud_id.as_deref(), Some(OTHER_CLOUD_ID));
    assert_eq!(replaced.record.revisions, second.record.revisions);
}

// REQ-CLOUDDEF-14, REQ-WFDEF-27
#[test]
fn set_cloud_id_refuses_an_invalid_id_or_a_held_lock_and_changes_nothing() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let id = first.record.id.clone();
    let before = home.definition_bytes(&id);

    let invalid = home
        .home
        .set_cloud_id(&id, "def_not-a-cloud-id")
        .expect_err("a cloud id that is not a def_ id should refuse");
    assert_eq!(invalid.code(), "definition_id_invalid");
    let unknown = home
        .home
        .set_cloud_id(OTHER_CLOUD_ID, CLOUD_ID)
        .expect_err("an unknown local id should refuse");
    assert_eq!(unknown.code(), "definition_not_found");

    let other = PublishLock::acquire(&first.path, &id).expect("lock should be free");
    let held = home
        .home
        .set_cloud_id(&id, CLOUD_ID)
        .expect_err("a held publish lock should refuse");
    assert_eq!(held.code(), "publish_conflict");
    assert_eq!(home.definition_bytes(&id), before);
    drop(other);
}

// REQ-CLOUDDEF-20, REQ-WFDEF-27
#[test]
fn check_definition_id_accepts_only_the_definition_id_shape() {
    for valid in [CLOUD_ID, OTHER_CLOUD_ID, "def_7zzzzzzzzzzzzzzzzzzzzzzzzz"] {
        check_definition_id(valid).expect("a definition id should pass");
    }
    for invalid in [
        "",
        "def_not-a-cloud-id",
        "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
        "def_81k0z6r1w8f4jy2m7q9v3x5abc",
        "def_01k0z6r1w8f4jy2m7q9v3x5ab",
        "def_01k0z6r1w8f4jy2m7q9v3x5abcd",
        "def_01K0Z6R1W8F4JY2M7Q9V3X5ABC",
        "def_01k0z6r1w8f4jy2m7q9v3x5abu",
    ] {
        let error = check_definition_id(invalid).expect_err("a malformed id should refuse");
        assert_eq!(error.code(), "definition_id_invalid", "{invalid}");
        assert_eq!(error.id(), Some(invalid), "{invalid}");
    }
}
