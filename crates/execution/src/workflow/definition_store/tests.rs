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
const NESTED_ROOT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/workflow/v1/valid/root-integration-bindings.yaml"
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
const DEFINITION_ROOT: &str = r#"schemaVersion: 1
name: repair
bindings:
  connection: {kind: connection}
  verify: {kind: definition}
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

/// A temporary `useful` home; the read-only tree copies are made writable again before the
/// directory is removed.
struct Home {
    temporary: TempDir,
    home: WorkflowHome,
}

impl Home {
    fn new() -> Self {
        let temporary = tempfile::tempdir().expect("temporary home should be created");
        let root = temporary.path().join("useful-home");
        let home = WorkflowHome::from_lookup(|name| {
            (name == "USEFUL_HOME").then(|| root.clone().into_os_string())
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

/// Publishes `tree` with what `file` and the command line's `options` name for it, judged as
/// the command judges them; without either nothing is named.
fn publish_named(
    home: &WorkflowHome,
    tree: &ValidatedTree,
    target: PublishTarget,
    file: Option<&BindingsFile>,
    options: &[BindingOption],
    by: &str,
) -> Result<Published, DefinitionError> {
    publish(
        home,
        tree,
        target,
        &NamedBindings::judge(tree, file, options)?,
        by,
    )
}

/// The snapshot a first publish of `tree` records from the file `value` and `options`.
fn resolve_file(
    tree: &ValidatedTree,
    value: Value,
    options: &[BindingOption],
) -> Result<BindingsSnapshot, DefinitionError> {
    resolve_bindings(
        &tree.bindings,
        &tree.required_integrations,
        &NamedBindings::judge(tree, Some(&bindings(value)), options)?,
        None,
    )
}

/// `--binding KEY=VALUE`.
fn binding(key: &str, value: &str) -> BindingOption {
    BindingOption {
        key: key.to_owned(),
        value: Some(value.to_owned()),
    }
}

fn engineering_bindings() -> BindingsFile {
    bindings(json!({
        "connection": "local",
        "linearTeam": "AUTO",
        "repository": "useful-machinery/platform",
    }))
}

fn publish_engineering(home: &Home, target: PublishTarget) -> Published {
    publish_named(
        &home.home,
        &Tree::engineering().validated(),
        target,
        Some(&engineering_bindings()),
        &[],
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
    assert!(
        schema_validator("workflow-definition-bindings-v2.schema.json").is_valid(&snapshot),
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
fn home_is_useful_home_when_set_and_not_empty_else_home_dot_useful() {
    let home = WorkflowHome::from_lookup(lookup(&[("USEFUL_HOME", "/srv/useful"), ("HOME", "/h")]))
        .expect("USEFUL_HOME should resolve");
    assert_eq!(home.root(), Path::new("/srv/useful"));
    assert_eq!(
        home.workflows_directory(),
        Path::new("/srv/useful/workflows")
    );
    assert_eq!(
        home.definition_directory("def_01jabcdefghjkmnpqrstvwxyz0"),
        Path::new("/srv/useful/workflows/def_01jabcdefghjkmnpqrstvwxyz0")
    );
    let hex = "a".repeat(64);
    assert_eq!(
        home.tree_directory(&format!("sha256:{hex}")),
        Some(PathBuf::from(format!("/srv/useful/trees/sha256/{hex}")))
    );

    for pairs in [
        &[("HOME", "/h")][..],
        &[("USEFUL_HOME", ""), ("HOME", "/h")][..],
    ] {
        let home = WorkflowHome::from_lookup(lookup(pairs)).expect("HOME should resolve");
        assert_eq!(home.root(), Path::new("/h/.useful"));
    }

    for pairs in [&[][..], &[("USEFUL_HOME", ""), ("HOME", "")][..]] {
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
    let second = publish_named(
        &home.home,
        &tree,
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
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

    let error = publish_named(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        Some(&engineering_bindings()),
        &[],
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
    let error = publish_named(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::To(unknown.to_owned()),
        Some(&engineering_bindings()),
        &[],
        "tester",
    )
    .expect_err("an unknown id should refuse");
    assert_eq!(error.code(), "definition_not_found");
    assert_eq!(error.id(), Some(unknown));
    assert!(!home.path("workflows").exists());

    let error = publish_named(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::To("engineering".to_owned()),
        Some(&engineering_bindings()),
        &[],
        "tester",
    )
    .expect_err("a name is not an id");
    assert_eq!(error.code(), "definition_id_invalid");

    let first = publish_engineering(&home, PublishTarget::Resolve);
    let renamed_root = ROOT_WORKFLOW.replacen("name: engineering", "name: lifecycle", 1);
    let renamed = publish_named(
        &home.home,
        &Tree::with(&renamed_root).validated(),
        PublishTarget::To(first.record.id.clone()),
        None,
        &[],
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
    let error = publish_named(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        Some(&engineering_bindings()),
        &[],
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
    let tree = Tree::engineering().validated();
    let resolve = |value: Value| resolve_file(&tree, value, &[]);

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
        (
            json!({"connection": {"value": "x", "other": 1}, "linearTeam": "AUTO", "repository": "r"}),
            "bindings_file_invalid",
            "connection",
        ),
    ] {
        let error = resolve(value).expect_err("the file should refuse");
        assert_eq!((error.code(), error.key()), (code, Some(key)));
    }

    for bytes in [&b"[]"[..], b"\"x\"", b"{", b""] {
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
        publish_named(
            &home.home,
            &tree,
            PublishTarget::Resolve,
            file.map(bindings).as_ref(),
            &[],
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
        let error = resolve_file(&tree, value, &[])
            .expect_err("a value that does not fit its kind should refuse");
        assert_eq!(
            (error.code(), error.key()),
            ("binding_value_invalid", Some(key))
        );
    }

    let empty_member_default =
        Tree::with(&MEMBER_ROOT.replace("default: []", "default: [\"\"]")).validated();
    let error = resolve_file(&empty_member_default, json!({"nightlyOwner": "prn_x"}), &[])
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
    let error = publish_named(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
    .expect_err("a required binding without a file should refuse");
    assert_eq!(error.code(), "bindings_required");
    assert!(!home.path("workflows").exists());

    let published = publish_named(
        &home.home,
        &Tree::plain().validated(),
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
    .expect("a root without required bindings should publish");
    assert!(published.snapshot.bindings().is_empty());
    assert_eq!(home.snapshot_bytes(&published.record.id, 1), b"{}");
    assert_records_match_schemas(&home, &published.record.id, 1);
}

// REQ-WFDEF-40, REQ-WFDEF-3, REQ-WFDEF-20
#[test]
fn a_definition_binding_given_no_value_has_no_entry_and_a_given_one_is_recorded_as_written() {
    let tree = Tree::with(DEFINITION_ROOT).validated();
    let resolve = |value: Value| resolve_file(&tree, value, &[]);

    let home = Home::new();
    let published = publish_named(
        &home.home,
        &tree,
        PublishTarget::Resolve,
        Some(&bindings(json!({"connection": "local"}))),
        &[],
        "tester",
    )
    .expect("a definition binding without a value should publish");
    let snapshot = serde_json::from_slice::<Value>(&home.snapshot_bytes(&published.record.id, 1))
        .expect("snapshot should be JSON");
    assert_eq!(
        snapshot,
        json!({"connection": {"kind": "connection", "value": "local"}})
    );
    assert_records_match_schemas(&home, &published.record.id, 1);

    // Recorded as written and resolved nowhere, a definition id or not.
    for value in ["def_01hzy3v8k2m4n6p8q0r2s4t6v8", "the verify workflow"] {
        let published = publish_named(
            &home.home,
            &tree,
            PublishTarget::New,
            Some(&bindings(json!({
                "connection": "local",
                "verify": {"kind": "definition", "value": value},
            }))),
            &[],
            "tester",
        )
        .expect("a definition value should publish");
        let snapshot =
            serde_json::from_slice::<Value>(&home.snapshot_bytes(&published.record.id, 1))
                .expect("snapshot should be JSON");
        assert_eq!(
            snapshot["verify"],
            json!({"kind": "definition", "value": value})
        );
        assert_records_match_schemas(&home, &published.record.id, 1);
    }

    for value in [json!(""), json!(["def_01hzy3v8k2m4n6p8q0r2s4t6v8"])] {
        let error = resolve(json!({"connection": "local", "verify": value}))
            .expect_err("a definition value that is not a non-empty string should refuse");
        assert_eq!(
            (error.code(), error.key()),
            ("binding_value_invalid", Some("verify"))
        );
    }

    let error = resolve(json!({"verify": "def_01hzy3v8k2m4n6p8q0r2s4t6v8"}))
        .expect_err("the connection omitted should still refuse");
    assert_eq!(
        (error.code(), error.key()),
        ("binding_missing", Some("connection"))
    );

    let with_default = Tree::with(&DEFINITION_ROOT.replace(
        "verify: {kind: definition}",
        "verify: {kind: definition, default: def_01hzy3v8k2m4n6p8q0r2s4t6v8}",
    ))
    .validated();
    let snapshot = resolve_file(&with_default, json!({"connection": "local"}), &[])
        .expect("a definition default should be recorded");
    assert_eq!(
        snapshot.bindings()["verify"],
        BoundBinding {
            kind: BindingKind::Definition,
            value: BindingValue::Text("def_01hzy3v8k2m4n6p8q0r2s4t6v8".to_owned()),
        }
    );
}

// REQ-WFDEF-40, REQ-WFDEF-21
#[test]
fn a_definition_binding_needs_no_file_and_a_later_publish_carries_what_was_recorded() {
    let home = Home::new();
    let only_definition =
        Tree::with(&DEFINITION_ROOT.replace("  connection: {kind: connection}\n", "")).validated();
    let published = publish_named(
        &home.home,
        &only_definition,
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
    .expect("a root whose only required binding is a definition should publish without a file");
    assert!(published.snapshot.bindings().is_empty());
    assert_eq!(home.snapshot_bytes(&published.record.id, 1), b"{}");

    let tree = Tree::with(DEFINITION_ROOT).validated();
    let publish_new = |file: Value| {
        publish_named(
            &home.home,
            &tree,
            PublishTarget::New,
            Some(&bindings(file)),
            &[],
            "tester",
        )
        .expect("first publish should succeed")
    };
    let publish_carried = |id: &str| {
        publish_named(
            &home.home,
            &tree,
            PublishTarget::To(id.to_owned()),
            None,
            &[],
            "tester",
        )
        .expect("a carried publish should succeed")
    };

    let given =
        publish_new(json!({"connection": "local", "verify": "def_01hzy3v8k2m4n6p8q0r2s4t6v8"}));
    let carried = publish_carried(&given.record.id);
    assert_eq!(carried.snapshot, given.snapshot);
    assert_eq!(
        carried.snapshot.bindings()["verify"].value.as_text(),
        Some("def_01hzy3v8k2m4n6p8q0r2s4t6v8")
    );

    let absent = publish_new(json!({"connection": "local"}));
    let carried = publish_carried(&absent.record.id);
    assert_eq!(carried.snapshot, absent.snapshot);
    assert!(!carried.snapshot.bindings().contains_key("verify"));
}

// REQ-WFDEF-21
#[test]
fn a_later_publish_carries_the_snapshot_forward_and_drops_undeclared_keys() {
    let home = Home::new();
    let first = publish_named(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        Some(&bindings(json!({
            "connection": "local",
            "linearTeam": "AUTO",
            "repository": "useful-machinery/platform",
            "done": "Shipped",
        }))),
        &[],
        "tester",
    )
    .expect("first publish should succeed");

    let without_repository =
        ROOT_WORKFLOW.replacen("  repository:      {kind: github.repository}\n", "", 1);
    let second = publish_named(
        &home.home,
        &Tree::with(&without_repository).validated(),
        PublishTarget::Resolve,
        None,
        &[],
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
    let error = publish_named(
        &home.home,
        &Tree::with(&with_new_binding).validated(),
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
    .expect_err("a newly required binding should refuse");
    assert_eq!(
        (error.code(), error.key()),
        ("binding_missing", Some("reviewer"))
    );
}

// REQ-WFDEF-20, REQ-WFDEF-21
#[test]
fn a_later_publish_sets_what_its_file_names_and_keeps_the_rest() {
    let home = Home::new();
    let tree = Tree::engineering().validated();
    let publish_file = |value: Value| {
        publish_named(
            &home.home,
            &tree,
            PublishTarget::Resolve,
            Some(&bindings(value)),
            &[],
            "tester",
        )
    };
    let first = publish_file(json!({
        "connection": "local",
        "linearTeam": "AUTO",
        "repository": "useful-machinery/platform",
        "done": "Shipped",
    }))
    .expect("first publish should succeed");
    let id = first.record.id.clone();

    let second = publish_file(json!({"linearTeam": "ENG"}))
        .expect("a file naming one binding should publish");
    let mut expected = first.snapshot.bindings().clone();
    expected.insert(
        "linearTeam".to_owned(),
        BoundBinding {
            kind: BindingKind::LinearTeam,
            value: BindingValue::Text("ENG".to_owned()),
        },
    );
    assert_eq!(second.snapshot.bindings(), &expected);
    assert_eq!(expected["done"].value.as_text(), Some("Shipped"));

    let third = publish_file(json!({"done": null})).expect("a null member should publish");
    let snapshot = third.snapshot.bindings();
    assert_eq!(snapshot["done"].value.as_text(), Some("Done"));
    assert_eq!(snapshot["linearTeam"].value.as_text(), Some("ENG"));
    assert_eq!(snapshot.len(), 7);
    assert_records_match_schemas(&home, &id, 3);

    let record = home.definition_bytes(&id);
    let error = publish_file(json!({"repository": null}))
        .expect_err("a binding named for a default it lacks should refuse");
    assert_eq!(
        (error.code(), error.key()),
        ("binding_missing", Some("repository"))
    );
    assert_eq!(home.definition_bytes(&id), record);

    let error = publish_file(json!({"connection": {"value": null}}))
        .expect_err("a null value in a member object should refuse");
    assert_eq!(
        (error.code(), error.key()),
        ("binding_value_invalid", Some("connection"))
    );
    assert_eq!(home.definition_bytes(&id), record);
}

// REQ-WFDEF-20, REQ-WFDEF-21
#[test]
fn a_later_publish_sets_what_its_file_names_under_an_integration_and_keeps_the_rest() {
    let home = Home::new();
    let tree = Tree::with(NESTED_ROOT).validated();
    let publish_file = |value: Value| {
        publish_named(
            &home.home,
            &tree,
            PublishTarget::Resolve,
            Some(&bindings(value)),
            &[],
            "tester",
        )
    };
    publish_file(json!({
        "linear": {"team": "AUTO", "done": "Shipped"},
        "github": {"repository": "useful-machinery/platform"},
    }))
    .expect("first publish should succeed");

    let second = publish_file(json!({"linear": {"team": "ENG"}}))
        .expect("a file naming one integration binding should publish");
    let groups = second.snapshot.integration_bindings();
    let linear = &groups[&IntegrationProvider::Linear];
    assert_eq!(linear["team"].value, "ENG");
    assert_eq!(linear["done"].value, "Shipped");
    assert_eq!(linear.len(), 5);
    assert_eq!(
        groups[&IntegrationProvider::Github]["repository"].value,
        "useful-machinery/platform"
    );

    let third = publish_file(json!({"linear": {"done": null}}))
        .expect("a null integration member should publish");
    let linear = &third.snapshot.integration_bindings()[&IntegrationProvider::Linear];
    assert_eq!(linear["done"].value, "Done");
    assert_eq!(linear["team"].value, "ENG");

    for (value, code, key) in [
        (
            json!({"linear": {"team": null}}),
            "binding_missing",
            Some("team"),
        ),
        (json!({"linear": null}), "bindings_file_invalid", None),
    ] {
        let error = publish_file(value).expect_err("the file should refuse");
        assert_eq!(
            (error.code(), error.integration(), error.key()),
            (code, Some("linear"), key)
        );
    }
}

// REQ-WFDEF-21
#[test]
fn a_later_publish_gives_a_binding_whose_declared_kind_changed_its_default() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let publish_root = |root: &str| {
        publish_named(
            &home.home,
            &Tree::with(root).validated(),
            PublishTarget::Resolve,
            None,
            &[],
            "tester",
        )
    };

    let retyped = ROOT_WORKFLOW.replacen(
        "repository:      {kind: github.repository}",
        "repository:      {kind: linear.team}",
        1,
    );
    let error = publish_root(&retyped).expect_err("a value of another kind should not be kept");
    assert_eq!(
        (error.code(), error.key()),
        ("binding_missing", Some("repository"))
    );

    let with_default = ROOT_WORKFLOW.replacen(
        "repository:      {kind: github.repository}",
        "repository:      {kind: linear.team, default: \"OPS\"}",
        1,
    );
    let published = publish_root(&with_default).expect("the default should be recorded");
    assert_eq!(published.record.id, first.record.id);
    assert_eq!(
        published.snapshot.bindings()["repository"],
        BoundBinding {
            kind: BindingKind::LinearTeam,
            value: BindingValue::Text("OPS".to_owned()),
        }
    );
    assert_eq!(
        published.snapshot.bindings()["linearTeam"].value.as_text(),
        Some("AUTO")
    );
}

// REQ-WFDEF-20, REQ-WFDEF-21
#[test]
fn a_first_publish_naming_nothing_is_bindings_required_and_one_naming_part_needs_the_rest() {
    let home = Home::new();
    let tree = Tree::engineering().validated();
    for (value, code, key) in [
        (json!({}), "bindings_required", None),
        (
            json!({"done": "Shipped"}),
            "binding_missing",
            Some("connection"),
        ),
    ] {
        let error = publish_named(
            &home.home,
            &tree,
            PublishTarget::Resolve,
            Some(&bindings(value)),
            &[],
            "tester",
        )
        .expect_err("the first publish should refuse");
        assert_eq!((error.code(), error.key()), (code, key));
    }
    assert!(!home.path("workflows").exists());
}

// REQ-WFDEF-21, REQ-WFDEF-40
#[test]
fn a_definition_binding_named_for_its_missing_default_has_no_entry_and_stays_absent() {
    let home = Home::new();
    let tree = Tree::with(DEFINITION_ROOT).validated();
    let publish_file = |file: Option<Value>| {
        publish_named(
            &home.home,
            &tree,
            PublishTarget::Resolve,
            file.map(bindings).as_ref(),
            &[],
            "tester",
        )
        .expect("publish should succeed")
    };
    let first = publish_file(Some(
        json!({"connection": "local", "verify": "def_01hzy3v8k2m4n6p8q0r2s4t6v8"}),
    ));
    assert!(first.snapshot.bindings().contains_key("verify"));

    let cleared = publish_file(Some(json!({"verify": null})));
    assert!(!cleared.snapshot.bindings().contains_key("verify"));
    assert_eq!(
        cleared.snapshot.bindings()["connection"].value.as_text(),
        Some("local")
    );

    let carried = publish_file(None);
    assert_eq!(carried.snapshot, cleared.snapshot);
}

fn nested_bindings() -> BindingsFile {
    bindings(json!({
        "linear": {"team": "AUTO"},
        "github": {"repository": {"kind": "repository", "value": "useful-machinery/platform"}},
    }))
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", lowercase_hex(digest(&SHA256, bytes).as_ref()))
}

// REQ-WFDEF-3, REQ-WFDEF-20, REQ-WFDEF-22
#[test]
fn integration_bindings_publish_under_their_integration_in_a_canonical_snapshot() {
    let home = Home::new();
    let tree = Tree::with(NESTED_ROOT).validated();
    let published = publish_named(
        &home.home,
        &tree,
        PublishTarget::Resolve,
        Some(&nested_bindings()),
        &[],
        "tester",
    )
    .expect("publish should succeed");
    let id = published.record.id.clone();
    let bytes = home.snapshot_bytes(&id, 1);
    let snapshot = serde_json::from_slice::<Value>(&bytes).expect("snapshot should be JSON");
    assert_eq!(
        snapshot,
        json!({
            "github": {"repository": {"kind": "repository", "value": "useful-machinery/platform"}},
            "linear": {
                "backlog": {"kind": "state", "value": "Backlog"},
                "canceled": {"kind": "state", "value": "Canceled"},
                "done": {"kind": "state", "value": "Done"},
                "ownedState": {"kind": "state", "value": "Engineering"},
                "team": {"kind": "team", "value": "AUTO"},
            },
        })
    );
    let mut canonical = Vec::new();
    canonical_json::to_writer(&mut canonical, &snapshot).expect("snapshot should re-encode");
    assert_eq!(bytes, canonical);
    assert_eq!(published.revision.bindings, sha256(&bytes));
    assert!(schema_validator("workflow-definition-bindings-v2.schema.json").is_valid(&snapshot));
    assert!(!schema_validator("workflow-definition-bindings-v1.schema.json").is_valid(&snapshot));
    assert_eq!(
        read_revision_bindings(&home.home, &id, 1).expect("the snapshot should read back"),
        published.snapshot
    );

    // The snapshot is itself a bindings file.
    let again = publish_named(
        &home.home,
        &tree,
        PublishTarget::New,
        Some(&BindingsFile::parse(&bytes).expect("the snapshot should parse as a file")),
        &[],
        "tester",
    )
    .expect("publishing with the snapshot as the file should succeed");
    assert_ne!(again.record.id, id);
    assert_eq!(again.revision.bindings, published.revision.bindings);
}

// REQ-WFDEF-20
#[test]
fn integration_binding_members_are_checked_against_their_integration_s_declarations() {
    let tree = Tree::with(NESTED_ROOT).validated();
    let resolve = |value: Value| resolve_file(&tree, value, &[]);
    let with_linear = |linear: Value| json!({"linear": linear, "github": {"repository": "r"}});
    for (value, code, integration, key) in [
        (
            with_linear(json!({"tem": "AUTO"})),
            "binding_undeclared",
            Some("linear"),
            Some("tem"),
        ),
        (
            with_linear(json!({"team": {"kind": "state", "value": "AUTO"}})),
            "binding_kind_mismatch",
            Some("linear"),
            Some("team"),
        ),
        (
            with_linear(json!({"team": ""})),
            "binding_value_invalid",
            Some("linear"),
            Some("team"),
        ),
        (
            with_linear(json!({"team": ["AUTO"]})),
            "binding_value_invalid",
            Some("linear"),
            Some("team"),
        ),
        (
            with_linear(json!({"team": {"value": 7}})),
            "binding_value_invalid",
            Some("linear"),
            Some("team"),
        ),
        (
            json!({"linear": {"team": "AUTO"}}),
            "binding_missing",
            Some("github"),
            Some("repository"),
        ),
        (
            json!({"linear": {"team": "AUTO"}, "github": {"repository": "r"}, "team": "AUTO"}),
            "binding_undeclared",
            None,
            Some("team"),
        ),
        (
            json!({"linear": "AUTO", "github": {"repository": "r"}}),
            "bindings_file_invalid",
            Some("linear"),
            None,
        ),
        (
            with_linear(json!({"team": {"value": "AUTO", "other": 1}})),
            "bindings_file_invalid",
            Some("linear"),
            Some("team"),
        ),
    ] {
        let error = resolve(value).expect_err("the file should refuse");
        assert_eq!(
            (error.code(), error.integration(), error.key()),
            (code, integration, key)
        );
    }

    let engineering = Tree::engineering().validated();
    let error = resolve_file(
        &engineering,
        json!({
            "connection": "local",
            "linearTeam": "AUTO",
            "repository": "r",
            "linear": {"team": "AUTO"},
        }),
        &[],
    )
    .expect_err("an integration the root does not declare bindings for should refuse");
    assert_eq!(
        (error.code(), error.integration(), error.key()),
        ("binding_undeclared", None, Some("linear"))
    );
}

// REQ-WFDEF-41
#[test]
fn binding_options_are_judged_against_the_root_as_file_members_are() {
    let nested = Tree::with(NESTED_ROOT).validated();
    let refusal = |tree: &ValidatedTree, value: Value, options: &[BindingOption]| {
        let error = resolve_file(tree, value, options).expect_err("the options should refuse");
        (
            error.code(),
            error.integration(),
            error.key().map(str::to_owned),
        )
    };
    let complete = || json!({"linear": {"team": "AUTO"}, "github": {"repository": "r"}});

    // A dotted `KEY` names an integration's binding only for an integration that declares
    // bindings; any other is a flat key, and no flat key holds a `.`.
    assert_eq!(
        refusal(&nested, complete(), &[binding("buildkite.x", "v")]),
        ("binding_undeclared", None, Some("buildkite.x".to_owned()))
    );
    assert_eq!(
        refusal(&nested, complete(), &[binding("linear.tem", "v")]),
        ("binding_undeclared", Some("linear"), Some("tem".to_owned()))
    );
    assert_eq!(
        refusal(&nested, complete(), &[binding("linear.team", "")]),
        (
            "binding_value_invalid",
            Some("linear"),
            Some("team".to_owned())
        )
    );
    let engineering = Tree::engineering().validated();
    let engineering_file = || {
        json!({
            "connection": "local",
            "linearTeam": "AUTO",
            "repository": "r",
        })
    };
    assert_eq!(
        refusal(
            &engineering,
            engineering_file(),
            &[binding("linear.team", "AUTO")]
        ),
        ("binding_undeclared", None, Some("linear.team".to_owned()))
    );

    // An option names its binding in place of the file's member for it.
    let snapshot = resolve_file(
        &nested,
        json!({"linear": {"team": "AUTO", "done": "Shipped"}, "github": {"repository": "r"}}),
        &[
            binding("linear.team", "ENG"),
            BindingOption {
                key: "linear.done".to_owned(),
                value: None,
            },
        ],
    )
    .expect("the options should resolve");
    let values = snapshot.text_values_by_reference();
    assert_eq!(values["linear.team"], "ENG");
    assert_eq!(values["linear.done"], "Done");
    assert_eq!(values["github.repository"], "r");
    let snapshot = resolve_file(
        &engineering,
        engineering_file(),
        &[binding("linearTeam", "ENG")],
    )
    .expect("a flat option should resolve");
    assert_eq!(
        snapshot.bindings()["linearTeam"].value.as_text(),
        Some("ENG")
    );

    // The file is judged whole, whatever the options name.
    let mut undeclared_member = complete();
    undeclared_member["linear"]["tem"] = json!("x");
    assert_eq!(
        refusal(&nested, undeclared_member, &[binding("linear.team", "ENG")]),
        ("binding_undeclared", Some("linear"), Some("tem".to_owned()))
    );
    let mut invalid_member = engineering_file();
    invalid_member["linearTeam"] = json!("");
    assert_eq!(
        refusal(
            &engineering,
            invalid_member,
            &[binding("linearTeam", "ENG")]
        ),
        ("binding_value_invalid", None, Some("linearTeam".to_owned()))
    );
}

// REQ-WFDEF-20
#[test]
fn a_half_migrated_root_takes_flat_members_and_an_integration_s_from_one_file() {
    let half_migrated = ROOT_WORKFLOW.replacen(
        "\n\nbindings:",
        "\n  integrations:\n    linear:\n      bindings:\n        team: {kind: team}\n\nbindings:",
        1,
    );
    let home = Home::new();
    let published = publish_named(
        &home.home,
        &Tree::with(&half_migrated).validated(),
        PublishTarget::Resolve,
        Some(&bindings(json!({
            "connection": "local",
            "linearTeam": "AUTO",
            "repository": "useful-machinery/platform",
            "linear": {"team": "AUTO"},
        }))),
        &[],
        "tester",
    )
    .expect("publish should succeed");
    let snapshot = serde_json::from_slice::<Value>(&home.snapshot_bytes(&published.record.id, 1))
        .expect("snapshot should be JSON");
    assert_eq!(
        snapshot,
        json!({
            "backlog": {"kind": "linear.state", "value": "Backlog"},
            "canceled": {"kind": "linear.state", "value": "Canceled"},
            "connection": {"kind": "connection", "value": "local"},
            "done": {"kind": "linear.state", "value": "Done"},
            "linear": {"team": {"kind": "team", "value": "AUTO"}},
            "linearTeam": {"kind": "linear.team", "value": "AUTO"},
            "ownedState": {"kind": "linear.state", "value": "Engineering"},
            "repository": {"kind": "github.repository", "value": "useful-machinery/platform"},
        })
    );
    assert!(schema_validator("workflow-definition-bindings-v2.schema.json").is_valid(&snapshot));
}

// REQ-WFDEF-21
#[test]
fn integration_bindings_are_required_carried_dropped_and_checked_by_integration_and_name() {
    let home = Home::new();
    let publish_nested = |root: &str, target: PublishTarget, file: Option<&BindingsFile>| {
        publish_named(
            &home.home,
            &Tree::with(root).validated(),
            target,
            file,
            &[],
            "tester",
        )
    };
    let error = publish_nested(NESTED_ROOT, PublishTarget::Resolve, None)
        .expect_err("a first publish without a file should refuse");
    assert_eq!(error.code(), "bindings_required");
    assert!(!home.path("workflows").exists());

    let first = publish_nested(
        NESTED_ROOT,
        PublishTarget::Resolve,
        Some(&nested_bindings()),
    )
    .expect("first publish should succeed");
    let carried = publish_nested(NESTED_ROOT, PublishTarget::Resolve, None)
        .expect("a carried publish should succeed");
    assert_eq!(carried.record.id, first.record.id);
    assert_eq!(carried.revision.bindings, first.revision.bindings);
    assert_eq!(carried.snapshot, first.snapshot);

    let done_as_team = NESTED_ROOT
        .replacen("done:       {kind: state,", "done:       {kind: team, ", 1)
        .replacen("{state: $linear.done}", "{state: $linear.backlog}", 1);
    let retyped = publish_nested(&done_as_team, PublishTarget::Resolve, None)
        .expect("a current value of another kind should give way to the default");
    assert_eq!(
        retyped.snapshot.integration_bindings()[&IntegrationProvider::Linear]["done"],
        BoundIntegrationBinding {
            kind: "team".to_owned(),
            value: "Done".to_owned(),
        }
    );

    let without_done = NESTED_ROOT
        .replacen(
            "        done:       {kind: state, default: \"Done\"}\n",
            "",
            1,
        )
        .replacen("      landed:    {state: $linear.done}\n", "", 1);
    let dropped = publish_nested(&without_done, PublishTarget::Resolve, None)
        .expect("a carried publish should drop what the root no longer declares");
    let linear = &dropped.snapshot.integration_bindings()[&IntegrationProvider::Linear];
    assert!(!linear.contains_key("done"));
    assert_eq!(linear.len(), 4);
    assert_eq!(linear["team"].value, "AUTO");

    let other = Home::new();
    let engineering = publish_engineering(&other, PublishTarget::Resolve);
    let error = publish_named(
        &other.home,
        &Tree::with(NESTED_ROOT).validated(),
        PublishTarget::To(engineering.record.id),
        None,
        &[],
        "tester",
    )
    .expect_err("flat values carry no integration binding");
    assert_eq!(
        (error.code(), error.integration(), error.key()),
        ("binding_missing", Some("github"), Some("repository"))
    );
}

// REQ-WFDEF-22
#[test]
fn each_publish_records_a_new_first_revision_with_digests_of_tree_and_snapshot() {
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let tree = Tree::engineering();
    let second = publish_named(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        None,
        &[],
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
    let original = publish_named(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        Some(&engineering_bindings()),
        &[],
        "tester",
    )
    .expect("first publish should succeed");
    let same = publish_named(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
    .expect("same tree should publish");
    assert_eq!(same.revision.n, 2);
    assert_eq!(same.revision.tree, original.revision.tree);

    tree.write("scripts/design.py", "print('changed')\n");
    let changed = publish_named(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
    .expect("changed tree should publish");
    assert_eq!(changed.revision.n, 3);
    assert_ne!(changed.revision.tree, original.revision.tree);

    tree.write("scripts/design.py", "print('design')\n");
    let older = publish_named(
        &home.home,
        &tree.validated(),
        PublishTarget::Resolve,
        None,
        &[],
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

    let second = publish_named(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        None,
        &[],
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

    let second = publish_named(
        &home.home,
        &Tree::plain().validated(),
        PublishTarget::Resolve,
        None,
        &[],
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
        let error = publish_named(
            &home.home,
            &Tree::engineering().validated(),
            target,
            None,
            &[],
            "tester",
        )
        .expect_err("a publish while another holds the lock should refuse");
        assert_eq!(error.code(), "publish_conflict");
        assert_eq!(error.id(), Some(first.record.id.as_str()));
    }
    assert_eq!(home.definition_bytes(&first.record.id), before);
    assert!(!first.path.join("revisions/2").exists());

    drop(other);
    let second = publish_named(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        None,
        &[],
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
    let error = publish_named(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        None,
        &[],
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
    let second = publish_named(
        &home.home,
        &Tree::engineering().validated(),
        PublishTarget::Resolve,
        Some(&bindings(json!({
            "connection": "local",
            "linearTeam": "AUTO",
            "repository": "useful-machinery/platform",
            "done": "Shipped",
        }))),
        &[],
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

// REQ-WFDEF-32: the record's `linear` member holds the Linear integration's bindings by name,
// read by reference key `linear.<name>` beside the flat keys; a `linear` member that is
// itself `{id, name}` is the flat key `linear`'s entry, and no other member nests.
#[test]
fn a_resolution_record_reads_the_linear_integration_s_bindings_by_reference_key() {
    let home = Home::new();
    let id = publish_engineering(&home, PublishTarget::Resolve).record.id;
    let record = home
        .home
        .definition_directory(&id)
        .join("revisions/1/resolution.json");
    let read = |value: Value| {
        fs::write(&record, value.to_string()).expect("resolution record should be written");
        home.home.revision_resolution(&id, 1)
    };
    let resolved = |value: Value| {
        read(value)
            .expect("the record should read")
            .expect("the record should be present")
    };

    assert_eq!(
        resolved(json!({
            "linear": {
                "team": {"id": "team-1", "name": "AUTO"},
                "done": {"id": "state-done", "name": "Done"},
            },
            "extra": {"id": "x-1", "name": "X"},
        })),
        BTreeMap::from([
            ("extra".to_owned(), "x-1".to_owned()),
            ("linear.done".to_owned(), "state-done".to_owned()),
            ("linear.team".to_owned(), "team-1".to_owned()),
        ])
    );
    assert_eq!(
        resolved(json!({"linear": {"id": "c-1", "name": "Workspace"}})),
        BTreeMap::from([("linear".to_owned(), "c-1".to_owned())])
    );

    for invalid in [
        json!({"linear": {"done": "state-done"}}),
        json!({"linear": {"done": {"id": "", "name": "Done"}}}),
        json!({"linear": {"not a key": {"id": "s", "name": "S"}}}),
        json!({"github": {"repository": {"id": "r", "name": "R"}}}),
    ] {
        let error = read(invalid.clone()).expect_err("a record not of the shape should refuse");
        assert_eq!(error.code(), "definition_record_invalid", "{invalid}");
        assert_eq!(error.path(), Some(record.as_path()), "{invalid}");
    }
}

// REQ-WFDEF-3: a snapshot answers each text value by the key a reference names after `$`,
// an integration binding as `<id>.<name>` and a flat binding by its key; a `members` list is
// no text value.
#[test]
fn a_snapshot_answers_its_text_values_by_reference_key() {
    let home = Home::new();
    let nested = publish_named(
        &home.home,
        &Tree::with(NESTED_ROOT).validated(),
        PublishTarget::Resolve,
        Some(&nested_bindings()),
        &[],
        "tester",
    )
    .expect("publish should succeed");
    let values = nested.snapshot.text_values_by_reference();
    assert_eq!(values.get("linear.team").map(String::as_str), Some("AUTO"));
    assert_eq!(values.get("linear.done").map(String::as_str), Some("Done"));
    assert_eq!(
        values.get("github.repository").map(String::as_str),
        Some("useful-machinery/platform")
    );
    assert!(!values.contains_key("team"), "{values:?}");

    let half_migrated = MEMBER_ROOT.replacen(
        "\nbindings:",
        "\nrequires:\n  integrations:\n    linear:\n      bindings:\n        team: {kind: team}\nbindings:",
        1,
    );
    let half = publish_named(
        &home.home,
        &Tree::with(&half_migrated).validated(),
        PublishTarget::New,
        Some(&bindings(json!({
            "nightlyOwner": "prn_x",
            "includedOwners": ["prn_a"],
            "linear": {"team": "AUTO"},
        }))),
        &[],
        "tester",
    )
    .expect("publish should succeed");
    assert_eq!(
        half.snapshot.text_values_by_reference(),
        BTreeMap::from([
            ("linear.team".to_owned(), "AUTO".to_owned()),
            ("nightlyOwner".to_owned(), "prn_x".to_owned()),
        ])
    );
}

const CLOUD_ID: &str = "def_01k0z6r1w8f4jy2m7q9v3x5abc";
const OTHER_CLOUD_ID: &str = "def_01k0z6r1w8f4jy2m7q9v3x5abd";

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

/// `value`'s members; `value` is a JSON object.
fn object(value: Value) -> serde_json::Map<String, Value> {
    let Value::Object(members) = value else {
        panic!("an answer should be an object");
    };
    members
}

/// The engineering root's snapshot as Cloud answers it: one `{kind, value}` per declared
/// binding, `done` `Shipped`.
fn engineering_answer() -> serde_json::Map<String, Value> {
    object(json!({
        "backlog": {"kind": "linear.state", "value": "Backlog"},
        "canceled": {"kind": "linear.state", "value": "Canceled"},
        "connection": {"kind": "connection", "value": "local"},
        "done": {"kind": "linear.state", "value": "Shipped"},
        "linearTeam": {"kind": "linear.team", "value": "AUTO"},
        "ownedState": {"kind": "linear.state", "value": "Engineering"},
        "repository": {"kind": "github.repository", "value": "useful-machinery/platform"},
    }))
}

/// Whether the home's `workflows/` is absent or holds no entry.
fn holds_no_definition(home: &Home) -> bool {
    fs::read_dir(home.path("workflows")).map_or(true, |mut entries| entries.next().is_none())
}

/// Records `answer` for `root` with `cloud_id`, expecting a refusal both to a new record on a
/// fresh home, which leaves `workflows/` without an entry, and to an existing record published
/// locally from `local`, which keeps its `definition.json` and gains no revision; answers the
/// fresh home's refusal, the existing record's being the same.
fn refused_publication(
    root: &str,
    local: &BindingsFile,
    answer: &BindingsFile,
    cloud_id: &str,
) -> DefinitionError {
    let tree = Tree::with(root).validated();
    let fresh = Home::new();
    let place = resolve_publish(&fresh.home, &tree, PublishTarget::Resolve)
        .expect("a fresh home should resolve a new place");
    let error = record_publication(&fresh.home, &tree, place, answer, cloud_id, "tester")
        .expect_err("the publication should refuse on a fresh home");
    assert!(holds_no_definition(&fresh), "{error}");

    let existing = Home::new();
    let first = publish_named(
        &existing.home,
        &tree,
        PublishTarget::Resolve,
        Some(local),
        &[],
        "tester",
    )
    .expect("a local publish should succeed");
    let before = existing.definition_bytes(&first.record.id);
    let place = resolve_publish(&existing.home, &tree, PublishTarget::Resolve)
        .expect("the record should resolve");
    let again = record_publication(&existing.home, &tree, place, answer, cloud_id, "tester")
        .expect_err("the publication should refuse on an existing record");
    assert_eq!(
        (again.code(), again.integration(), again.key()),
        (error.code(), error.integration(), error.key())
    );
    assert_eq!(existing.definition_bytes(&first.record.id), before);
    assert!(!first.path.join("revisions/2").exists());
    error
}

// REQ-WFDEF-19, REQ-WFDEF-22, REQ-CLOUDDEF-14
#[test]
fn a_cloud_publication_records_the_snapshot_it_is_handed_with_its_cloud_id() {
    let tree = Tree::engineering().validated();

    // A fresh home: the place has no record, and the publication creates one with the id.
    let home = Home::new();
    let place = resolve_publish(&home.home, &tree, PublishTarget::Resolve)
        .expect("a fresh home should resolve a new place");
    assert_eq!(place.record(), None);
    let created = record_publication(
        &home.home,
        &tree,
        place,
        &BindingsFile::from_object(engineering_answer()),
        CLOUD_ID,
        "tester",
    )
    .expect("the answered snapshot should be recorded");
    assert!(created.created);
    assert_eq!(created.revision.n, 1);
    assert_eq!(created.record.cloud_id.as_deref(), Some(CLOUD_ID));
    let id = created.record.id.clone();
    let local = Home::new();
    let published = publish_named(
        &local.home,
        &tree,
        PublishTarget::Resolve,
        Some(&bindings(json!({
            "connection": "local",
            "done": "Shipped",
            "linearTeam": "AUTO",
            "repository": "useful-machinery/platform",
        }))),
        &[],
        "tester",
    )
    .expect("a local publish of the same values should succeed");
    assert_eq!(
        home.snapshot_bytes(&id, 1),
        local.snapshot_bytes(&published.record.id, 1)
    );
    assert_eq!(
        read_definition(&home.home, &id)
            .expect("record should read")
            .record
            .cloud_id
            .as_deref(),
        Some(CLOUD_ID)
    );
    assert_records_match_schemas(&home, &id, 1);

    // An existing record: the answer is recorded whole and the record gains the id.
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let id = first.record.id.clone();
    assert_eq!(first.record.cloud_id, None);
    let place = resolve_publish(&home.home, &tree, PublishTarget::To(id.clone()))
        .expect("the record should resolve by id");
    assert_eq!(
        place.record().map(|record| record.id.as_str()),
        Some(id.as_str())
    );
    let mut answer = engineering_answer();
    answer.insert(
        "linearTeam".to_owned(),
        json!({"kind": "linear.team", "value": "OPS"}),
    );
    let answer = BindingsFile::from_object(answer);
    let second = record_publication(&home.home, &tree, place, &answer, CLOUD_ID, "tester")
        .expect("the answered snapshot should be recorded");
    assert!(!second.created);
    assert_eq!(second.revision.n, 2);
    assert_eq!(second.record.cloud_id.as_deref(), Some(CLOUD_ID));
    assert_eq!(
        second.snapshot,
        resolve_bindings(
            &tree.bindings,
            &tree.required_integrations,
            &NamedBindings::judge(&tree, Some(&answer), &[]).expect("the answer should judge"),
            None,
        )
        .expect("the answer should resolve alone")
    );
    assert_eq!(
        second.snapshot.bindings()["linearTeam"].value.as_text(),
        Some("OPS")
    );
    assert_eq!(
        second.snapshot.bindings()["done"].value.as_text(),
        Some("Shipped")
    );
    assert_records_match_schemas(&home, &id, 2);
    let third = publish_named(
        &home.home,
        &tree,
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
    .expect("a later local publish should succeed");
    assert_eq!(third.record.cloud_id.as_deref(), Some(CLOUD_ID));
    assert_eq!(third.snapshot, second.snapshot);
    assert_eq!(home.snapshot_bytes(&id, 3), home.snapshot_bytes(&id, 2));

    // It never reads the current snapshot: one unlike its digest refuses a local publish only.
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let id = first.record.id.clone();
    let snapshot = first.path.join("revisions/1/bindings.json");
    restore_writable(&first.path);
    fs::set_permissions(&snapshot, Permissions::from_mode(0o600))
        .expect("snapshot should become writable");
    fs::write(&snapshot, b"{}").expect("snapshot should be replaced");
    let error = publish_named(
        &home.home,
        &tree,
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
    .expect_err("a snapshot unlike its digest should refuse a local publish");
    assert_eq!(error.code(), "definition_record_invalid");
    let place = resolve_publish(&home.home, &tree, PublishTarget::Resolve)
        .expect("the record should resolve");
    let recorded = record_publication(
        &home.home,
        &tree,
        place,
        &BindingsFile::from_object(engineering_answer()),
        CLOUD_ID,
        "tester",
    )
    .expect("a publication should not read the current snapshot");
    assert_eq!(recorded.record.id, id);
    assert_eq!(recorded.revision.n, 2);

    // A record extended between resolving the place and recording is extended.
    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let place = resolve_publish(&home.home, &tree, PublishTarget::Resolve)
        .expect("the record should resolve");
    publish_named(
        &home.home,
        &tree,
        PublishTarget::Resolve,
        None,
        &[],
        "tester",
    )
    .expect("a local publish in between should succeed");
    let extended = record_publication(
        &home.home,
        &tree,
        place,
        &BindingsFile::from_object(engineering_answer()),
        CLOUD_ID,
        "tester",
    )
    .expect("the publication should extend the record read again");
    assert_eq!(extended.record.id, first.record.id);
    assert_eq!(extended.revision.n, 3);
    assert_eq!(
        extended
            .record
            .revisions
            .iter()
            .map(|revision| revision.n)
            .collect::<Vec<_>>(),
        vec![3, 2, 1]
    );

    // Refusals write nothing.
    let refused = |changes: Value| {
        let mut answer = engineering_answer();
        for (key, value) in object(changes) {
            if value.is_null() {
                answer.remove(&key);
            } else {
                answer.insert(key, value);
            }
        }
        refused_publication(
            ROOT_WORKFLOW,
            &engineering_bindings(),
            &BindingsFile::from_object(answer),
            CLOUD_ID,
        )
    };
    let error = refused(json!({"unknown": {"kind": "connection", "value": "local"}}));
    assert_eq!(
        (error.code(), error.key()),
        ("binding_undeclared", Some("unknown"))
    );
    let error = refused(json!({"done": {"kind": "linear.team", "value": "AUTO"}}));
    assert_eq!(
        (error.code(), error.key()),
        ("binding_kind_mismatch", Some("done"))
    );
    let error = refused(json!({"done": []}));
    assert_eq!(
        (error.code(), error.key()),
        ("binding_value_invalid", Some("done"))
    );
    let error = refused(json!({"repository": null}));
    assert_eq!(
        (error.code(), error.integration(), error.key()),
        ("binding_missing", None, Some("repository"))
    );
    let error = refused_publication(
        NESTED_ROOT,
        &nested_bindings(),
        &BindingsFile::from_object(object(json!({
            "github": {"repository": {"kind": "repository", "value": "useful-machinery/platform"}},
            "linear": {
                "backlog": {"kind": "state", "value": "Backlog"},
                "canceled": {"kind": "state", "value": "Canceled"},
                "done": {"kind": "state", "value": "Done"},
                "ownedState": {"kind": "state", "value": "Engineering"},
            },
        }))),
        CLOUD_ID,
    );
    assert_eq!(
        (error.code(), error.integration(), error.key()),
        ("binding_missing", Some("linear"), Some("team"))
    );
    let error = refused_publication(
        DEFINITION_ROOT,
        &bindings(json!({"connection": "local"})),
        &BindingsFile::from_object(object(json!({
            "connection": {"kind": "connection", "value": "local"},
        }))),
        CLOUD_ID,
    );
    assert_eq!(
        (error.code(), error.key()),
        ("binding_missing", Some("verify"))
    );
    let error = refused_publication(
        ROOT_WORKFLOW,
        &engineering_bindings(),
        &BindingsFile::from_object(engineering_answer()),
        "def_not-a-cloud-id",
    );
    assert_eq!(error.code(), "definition_id_invalid");

    let home = Home::new();
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let before = home.definition_bytes(&first.record.id);
    let place = resolve_publish(&home.home, &tree, PublishTarget::Resolve)
        .expect("the record should resolve");
    let other = PublishLock::acquire(&first.path, &first.record.id).expect("lock should be free");
    let error = record_publication(
        &home.home,
        &tree,
        place,
        &BindingsFile::from_object(engineering_answer()),
        CLOUD_ID,
        "tester",
    )
    .expect_err("a publication while another holds the lock should refuse");
    assert_eq!(error.code(), "publish_conflict");
    assert_eq!(error.id(), Some(first.record.id.as_str()));
    assert_eq!(home.definition_bytes(&first.record.id), before);
    assert!(!first.path.join("revisions/2").exists());
    drop(other);

    // Resolving the place refuses as a publish does.
    let home = Home::new();
    let nameless = Tree::with(&ROOT_WORKFLOW.replacen("name: engineering\n", "", 1)).validated();
    let error = resolve_publish(&home.home, &nameless, PublishTarget::Resolve)
        .expect_err("a nameless root should refuse");
    assert_eq!(error.code(), "definition_name_missing");
    let unknown = "def_01jabcdefghjkmnpqrstvwxyz0";
    let error = resolve_publish(&home.home, &tree, PublishTarget::To(unknown.to_owned()))
        .expect_err("an unknown id should refuse");
    assert_eq!(
        (error.code(), error.id()),
        ("definition_not_found", Some(unknown))
    );
    let first = publish_engineering(&home, PublishTarget::Resolve);
    let second = publish_engineering(&home, PublishTarget::New);
    let error = resolve_publish(&home.home, &tree, PublishTarget::Resolve)
        .expect_err("an ambiguous name should refuse");
    assert_eq!(error.code(), "definition_name_ambiguous");
    let mut expected = vec![first.record.id, second.record.id];
    expected.sort();
    assert_eq!(error.ids(), expected.as_slice());
}
