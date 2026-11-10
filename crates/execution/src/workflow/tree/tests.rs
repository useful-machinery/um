use std::fs::{self, File, FileTimes, Permissions};
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::time::{Duration, SystemTime};

use ring::digest::{SHA256, digest};
use tempfile::TempDir;

use super::*;

const ROOT_WORKFLOW: &str = r#"schemaVersion: 1
exclude: [tests/, "**/__pycache__/"]
steps:
  prepare:
    kind: cmd
    command:
      argv: ["true"]
"#;

fn write(root: &Path, relative: &str, bytes: &[u8]) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().expect("tree file should have a parent"))
        .expect("tree directory should be created");
    fs::write(path, bytes).expect("tree file should be written");
}

fn engineering_like_tree() -> TempDir {
    let temporary = tempfile::tempdir().expect("temporary tree should be created");
    let root = temporary.path();
    write(root, "workflow.yaml", ROOT_WORKFLOW.as_bytes());
    write(root, "scripts/design.py", b"print('design')\n");
    write(root, "prompts/design.md", b"Design the change.\n");
    write(
        root,
        "tests/test_design.py",
        b"def test_design():\n    pass\n",
    );
    temporary
}

fn tree_digest(root: &Path) -> String {
    validate_workflow_tree(root)
        .unwrap_or_else(|diagnostics| panic!("tree should be valid: {diagnostics:?}"))
        .digest
        .value
}

// REQ-WFDEF-11
#[test]
fn tree_digest_depends_on_paths_and_bytes_not_modes_or_times() {
    let first = engineering_like_tree();
    let second = engineering_like_tree();
    let script = second.path().join("scripts/design.py");
    fs::set_permissions(&script, Permissions::from_mode(0o755)).expect("script mode should change");
    File::options()
        .write(true)
        .open(&script)
        .and_then(|file| {
            file.set_times(
                FileTimes::new().set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(86_400)),
            )
        })
        .expect("script time should change");

    let digest = tree_digest(first.path());
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(digest, tree_digest(second.path()));

    write(second.path(), "scripts/design.py", b"print('desigN')\n");
    assert_ne!(digest, tree_digest(second.path()));
}

// REQ-WFDEF-12
#[test]
fn excluded_files_do_not_contribute_to_the_tree_digest() {
    let tree = engineering_like_tree();
    let digest = tree_digest(tree.path());

    write(tree.path(), "tests/test_design.py", b"changed\n");
    write(tree.path(), "tests/nested/test_more.py", b"more\n");
    write(tree.path(), "scripts/__pycache__/x.pyc", b"\x00compiled");
    assert_eq!(digest, tree_digest(tree.path()));

    let validated = validate_workflow_tree(tree.path()).expect("tree should be valid");
    assert_eq!(validated.workflow_files, ["workflow.yaml"]);
}

// REQ-WFDEF-11: the framing Gateway reproduces.
#[test]
fn tree_digest_is_the_documented_framing_over_sorted_paths() {
    let temporary = tempfile::tempdir().expect("temporary tree should be created");
    let root = temporary.path();
    let workflow = b"schemaVersion: 1\nsteps:\n  prepare:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    write(root, "workflow.yaml", workflow);
    write(root, "c.txt", b"third\n");
    write(root, "a/b.txt", b"");

    let mut framed = b"um.workflow.tree.v1\0".to_vec();
    framed.extend_from_slice(&3_u64.to_be_bytes());
    for (path, content) in [
        ("a/b.txt", &b""[..]),
        ("c.txt", &b"third\n"[..]),
        ("workflow.yaml", &workflow[..]),
    ] {
        framed.extend_from_slice(&(path.len() as u64).to_be_bytes());
        framed.extend_from_slice(path.as_bytes());
        framed.extend_from_slice(&(content.len() as u64).to_be_bytes());
        framed.extend_from_slice(content);
    }
    let expected = digest(&SHA256, &framed)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    let validated = validate_workflow_tree(root).expect("tree should be valid");
    assert_eq!(validated.digest.algorithm.as_str(), "sha256");
    assert_eq!(validated.digest.value, expected);
}

// REQ-WFDEF-12
#[test]
fn exclude_patterns_follow_the_documented_grammar() {
    let matches = |pattern: &str, path: &str| {
        ExcludePattern::parse(pattern)
            .expect("pattern should be valid")
            .excludes_file(path)
    };
    assert!(matches("tests/", "tests/test_x.py"));
    assert!(matches("tests/", "a/tests/deep/test_x.py"));
    assert!(!matches("tests/", "tests.py"));
    assert!(matches("**/__pycache__/", "__pycache__/x.pyc"));
    assert!(matches("**/__pycache__/", "scripts/a/__pycache__/x.pyc"));
    assert!(matches("*.pyc", "scripts/x.pyc"));
    assert!(!matches("*.pyc", "scripts/x.py"));
    assert!(matches("scripts/*.tmp", "scripts/a.tmp"));
    assert!(!matches("scripts/*.tmp", "other/scripts/a.tmp"));
    assert!(matches("docs/**/draft.md", "docs/draft.md"));
    assert!(matches("docs/**/draft.md", "docs/a/b/draft.md"));

    for invalid in ["/tests/", "../x", "a/../b", "!keep.txt"] {
        assert!(ExcludePattern::parse(invalid).is_none(), "{invalid}");
    }
}

// REQ-WFDEF-12
#[test]
fn an_exclude_that_matches_the_root_or_a_bad_pattern_is_rejected() {
    let temporary = tempfile::tempdir().expect("temporary tree should be created");
    write(
        temporary.path(),
        "workflow.yaml",
        ROOT_WORKFLOW
            .replace(r#"[tests/, "**/__pycache__/"]"#, r#"["*.yaml", "!keep"]"#)
            .as_bytes(),
    );
    let diagnostics = validate_workflow_tree(temporary.path()).expect_err("tree should be invalid");
    let codes = diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code)
        .collect::<Vec<_>>();
    assert_eq!(codes, ["exclude_pattern_invalid", "tree_root_excluded"]);
}

const PATH_ARGV_STAGE: &str = r#"schemaVersion: 1
agentProfiles:
  designer:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  design:
    kind: cmd
    command:
      argv: ["./scripts/design.py"]
  review:
    kind: agent
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv: ["../repair"]
    agent:
      profile: designer
      systemPrompt: prompts/design.md
      message:
        text:
          - file: prompts/design.md
finalizers:
  cleanup:
    kind: cmd
    when: [succeeded]
    command:
      argv: ["/usr/bin/true"]
"#;

// REQ-WFDEF-16
// REQ-WFEXEC-54
#[test]
fn a_path_argv_anywhere_in_the_tree_is_command_program_is_path() {
    let tree = engineering_like_tree();
    write(
        tree.path(),
        "workflow.yaml",
        ROOT_WORKFLOW
            .replace(r#"argv: ["true"]"#, r#"argv: ["design.py"]"#)
            .as_bytes(),
    );
    write(tree.path(), "design.yaml", PATH_ARGV_STAGE.as_bytes());

    let diagnostics = validate_workflow_tree(tree.path()).expect_err("tree should be invalid");
    let mut found = diagnostics
        .iter()
        .map(|diagnostic| {
            (
                diagnostic.code,
                diagnostic.location.kind,
                diagnostic.location.path.as_deref(),
                diagnostic.location.step.as_deref(),
                diagnostic.location.key,
            )
        })
        .collect::<Vec<_>>();
    found.sort();
    let path_argv = |step, key| {
        (
            "command_program_is_path",
            "workflow",
            Some("design.yaml"),
            Some(step),
            Some(key),
        )
    };
    assert_eq!(
        found,
        [
            path_argv("cleanup", "command.argv"),
            path_argv("design", "command.argv"),
            path_argv("review", "recovery.handler.command.argv"),
        ]
    );
}

// REQ-WFDEF-16: the walk keeps each kept file's bytes and owner execute bit for the run copy.
#[test]
fn a_valid_tree_keeps_its_files_with_their_executable_bit() {
    let tree = engineering_like_tree();
    fs::set_permissions(
        tree.path().join("scripts/design.py"),
        Permissions::from_mode(0o744),
    )
    .expect("script mode should change");
    fs::set_permissions(
        tree.path().join("prompts/design.md"),
        Permissions::from_mode(0o655),
    )
    .expect("prompt mode should change");

    let validated = validate_workflow_tree(tree.path()).expect("tree should be valid");
    let kept = validated
        .files
        .iter()
        .map(|(path, file)| (path.as_str(), &*file.bytes, file.executable))
        .collect::<Vec<_>>();
    assert_eq!(
        kept,
        [
            ("prompts/design.md", &b"Design the change.\n"[..], false),
            ("scripts/design.py", &b"print('design')\n"[..], true),
            ("workflow.yaml", ROOT_WORKFLOW.as_bytes(), false),
        ]
    );
}
