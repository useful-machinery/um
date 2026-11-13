use std::fs::{self, File, FileTimes, Permissions};
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::time::{Duration, SystemTime};

use ring::digest::{SHA256, digest};
use tempfile::TempDir;

use super::*;
use crate::workflow::document::{BindingValue, OwnerDeclaration, OwnerSource, StopBefore};

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

const MEMBER_BINDINGS: &str = r#"bindings:
  includedOwners: {kind: members, default: []}
  reviewers: {kind: members, default: ["prn_a", "prn_b"]}
  nightlyOwner: {kind: member}
  backupOwner: {kind: member, default: "prn_c"}
"#;

// REQ-WFDEF-29: `member` and `members` bindings validate with their defaults as declared,
// and each kind's default has its own type.
#[test]
fn member_kinds_validate_with_typed_defaults() {
    let tree = engineering_like_tree();
    write(
        tree.path(),
        "workflow.yaml",
        format!("{ROOT_WORKFLOW}{MEMBER_BINDINGS}").as_bytes(),
    );
    let validated = validate_workflow_tree(tree.path())
        .unwrap_or_else(|diagnostics| panic!("tree should be valid: {diagnostics:?}"));
    let list = |items: &[&str]| {
        Some(BindingValue::List(
            items.iter().map(|item| (*item).to_owned()).collect(),
        ))
    };
    assert_eq!(
        validated.bindings,
        BTreeMap::from([
            (
                "backupOwner".to_owned(),
                Binding {
                    kind: BindingKind::Member,
                    default: Some(BindingValue::Text("prn_c".to_owned())),
                },
            ),
            (
                "includedOwners".to_owned(),
                Binding {
                    kind: BindingKind::Members,
                    default: list(&[]),
                },
            ),
            (
                "nightlyOwner".to_owned(),
                Binding {
                    kind: BindingKind::Member,
                    default: None,
                },
            ),
            (
                "reviewers".to_owned(),
                Binding {
                    kind: BindingKind::Members,
                    default: list(&["prn_a", "prn_b"]),
                },
            ),
        ])
    );

    for (from, to) in [
        ("default: []", "default: \"prn_a\""),
        ("default: \"prn_c\"", "default: [\"prn_c\"]"),
    ] {
        write(
            tree.path(),
            "workflow.yaml",
            format!("{ROOT_WORKFLOW}{}", MEMBER_BINDINGS.replace(from, to)).as_bytes(),
        );
        validate_workflow_tree(tree.path())
            .expect_err("a default of the other kind's type should be rejected");
    }
}

const TRIGGER_ROOT: &str = r#"schemaVersion: 1
inputs:
  ticket:
    kind: text
steps:
  prepare:
    kind: cmd
    command:
      argv: ["true"]
bindings:
  connection: {kind: connection}
  linearTeam: {kind: linear.team}
  includedOwners: {kind: members, default: []}
  excludedOwners: {kind: members, default: []}
  nightlyOwner: {kind: member}
triggers:
  start:
    on:
      source: $connection
      event: issue.state_entered
      where: "event.team == $linearTeam and event.issue.blocked == false"
    subject: event.issue.identifier
    stopBefore: {labels: event.issue.labels, prefix: "hold:"}
    owner: {from: [event.issue.assignee, event.issue.creator], only: $includedOwners, except: $excludedOwners}
    inputs:
      ticket: event.issue.identifier
  nightly:
    on: {schedule: "0 6 * * *", timezone: "UTC"}
    owner: $nightlyOwner
    inputs:
      ticket: "nightly"
"#;

fn trigger_tree(root_workflow: &str) -> TempDir {
    let tree = engineering_like_tree();
    write(tree.path(), "workflow.yaml", root_workflow.as_bytes());
    tree
}

/// A diagnostic's code with the trigger, key and binding it names.
type TriggerRejection = (
    &'static str,
    Option<String>,
    Option<&'static str>,
    Option<String>,
);

/// The diagnostics of the tree whose root is `root_workflow`, which must be invalid.
fn trigger_rejections(root_workflow: &str) -> Vec<TriggerRejection> {
    let tree = trigger_tree(root_workflow);
    let diagnostics = validate_workflow_tree(tree.path()).expect_err("tree should be invalid");
    diagnostics
        .into_iter()
        .map(|diagnostic| {
            (
                diagnostic.code,
                diagnostic.location.trigger,
                diagnostic.location.key,
                diagnostic.location.binding,
            )
        })
        .collect()
}

fn single_trigger_rejection(from: &str, to: &str) -> (&'static str, Option<&'static str>) {
    assert!(TRIGGER_ROOT.contains(from), "{from}");
    let rejections = trigger_rejections(&TRIGGER_ROOT.replacen(from, to, 1));
    assert_eq!(rejections.len(), 1, "{rejections:?}");
    let (code, _, key, _) = &rejections[0];
    (code, *key)
}

// REQ-WFDEF-10, REQ-WFDEF-14, REQ-WFDEF-30, REQ-WFDEF-31: a boolean operand, `stopBefore`, a
// filtered `owner` and a schedule trigger validate, and the tree keeps them, each rule's
// `where` and each start trigger's `subject` and `inputs` as written for the report.
#[test]
fn start_trigger_declarations_and_a_schedule_trigger_validate() {
    let tree = trigger_tree(TRIGGER_ROOT);
    let validated = validate_workflow_tree(tree.path())
        .unwrap_or_else(|diagnostics| panic!("tree should be valid: {diagnostics:?}"));

    let start = &validated.triggers["start"];
    assert_eq!(start.kind, TreeTriggerKind::Start);
    assert_eq!(start.source.as_deref(), Some("$connection"));
    assert_eq!(start.event.as_deref(), Some("issue.state_entered"));
    assert_eq!(
        start.condition.as_deref(),
        Some("event.team == $linearTeam and event.issue.blocked == false")
    );
    assert_eq!(start.schedule, None);
    assert_eq!(start.subject.as_deref(), Some("event.issue.identifier"));
    assert_eq!(
        start.inputs,
        BTreeMap::from([("ticket".to_owned(), "event.issue.identifier".to_owned())])
    );
    assert_eq!(
        start.stop_before,
        Some(StopBefore {
            labels: "event.issue.labels".to_owned(),
            prefix: "hold:".to_owned(),
        })
    );
    assert_eq!(
        start.owner,
        Some(OwnerDeclaration {
            from: OwnerSource::Events(vec![
                "event.issue.assignee".to_owned(),
                "event.issue.creator".to_owned(),
            ]),
            only: Some("$includedOwners".to_owned()),
            except: Some("$excludedOwners".to_owned()),
        })
    );

    let nightly = &validated.triggers["nightly"];
    assert_eq!(nightly.kind, TreeTriggerKind::Schedule);
    assert_eq!(nightly.kind.as_str(), "schedule");
    assert_eq!(
        (nightly.source.as_deref(), nightly.event.as_deref()),
        (None, None)
    );
    assert_eq!(nightly.schedule.as_deref(), Some("0 6 * * *"));
    assert_eq!(nightly.timezone.as_deref(), Some("UTC"));
    assert_eq!(nightly.condition, None);
    assert_eq!(nightly.subject, None);
    assert_eq!(
        nightly.inputs,
        BTreeMap::from([("ticket".to_owned(), "nightly".to_owned())])
    );
    assert_eq!(
        nightly.owner,
        Some(OwnerDeclaration {
            from: OwnerSource::Binding("$nightlyOwner".to_owned()),
            only: None,
            except: None,
        })
    );
}

// REQ-WFDEF-10: `true` and `false` are operands; a quoted literal is still refused.
#[test]
fn a_boolean_operand_passes_and_a_quoted_literal_does_not() {
    let (code, key) = single_trigger_rejection(
        "\"event.team == $linearTeam and event.issue.blocked == false\"",
        "'event.team == $linearTeam and event.issue.blocked == \"x\"'",
    );
    assert_eq!((code, key), ("literal_where_binding_expected", None));
}

/// A control trigger appended to `TRIGGER_ROOT`'s triggers.
const CONTROL_TRIGGER: &str = r#"  release:
    on:
      source: $connection
      event: issue.state_left
      where: "event.team == $linearTeam"
    control:
      run: bySubject
      action: {when: "event.to == $linearTeam", then: cancel, else: withdraw}
"#;

// REQ-WFDEF-10: a start `where` and a control `on.where` outside the grammar Cloud evaluates
// them with, and a control `when` the wait's evaluator cannot parse, are
// `trigger_rule_invalid`; a quoted literal and an undeclared `$key` keep their own codes.
#[test]
fn a_trigger_rule_outside_its_grammar_is_refused() {
    let start_where = "\"event.team == $linearTeam and event.issue.blocked == false\"";
    let control_where = "where: \"event.team == $linearTeam\"\n    control:";
    let control_when = "when: \"event.to == $linearTeam\"";
    let control_root = format!("{TRIGGER_ROOT}{CONTROL_TRIGGER}");
    let tree = trigger_tree(&control_root);
    validate_workflow_tree(tree.path())
        .unwrap_or_else(|diagnostics| panic!("tree should be valid: {diagnostics:?}"));
    let single = |root: &str, from: &str, to: &str| {
        assert!(root.contains(from), "{from}");
        let rejections = trigger_rejections(&root.replacen(from, to, 1));
        assert_eq!(rejections.len(), 1, "{to}: {rejections:?}");
        rejections.into_iter().next().unwrap()
    };
    let refused = |trigger: &str, key| {
        (
            "trigger_rule_invalid",
            Some(trigger.to_owned()),
            Some(key),
            None,
        )
    };

    for rule in [
        "event.team != $linearTeam",
        "event.team == $linearTeam or event.issue.blocked == false",
        "blocked",
        "event.team == $linearTeam == true",
        "event.issue..blocked == false",
    ] {
        assert_eq!(
            single(TRIGGER_ROOT, start_where, &format!("\"{rule}\"")),
            refused("start", "on.where"),
            "{rule}"
        );
        assert_eq!(
            single(
                &control_root,
                control_where,
                &format!("where: \"{rule}\"\n    control:")
            ),
            refused("release", "on.where"),
            "{rule}"
        );
    }
    for rule in [
        "event.to != $linearTeam",
        "event.to",
        "event.to == run.id",
        "event.to == $linearTeam-x",
    ] {
        assert_eq!(
            single(&control_root, control_when, &format!("when: \"{rule}\"")),
            refused("release", "control.action.when"),
            "{rule}"
        );
    }

    let (code, _, key, binding) = single(
        TRIGGER_ROOT,
        start_where,
        "\"event.team == $nobody and event.issue.blocked == false\"",
    );
    assert_eq!(
        (code, key, binding.as_deref()),
        ("binding_undeclared", None, Some("nobody"))
    );
    // Cloud reads a `$key` up to the next space or quote, so `$linearTeam-x` names no binding.
    let (code, _, key, binding) = single(
        TRIGGER_ROOT,
        start_where,
        "\"event.team == $linearTeam-x and event.issue.blocked == false\"",
    );
    assert_eq!(
        (code, key, binding.as_deref()),
        ("binding_undeclared", None, Some("linearTeam-x"))
    );
    let (code, _, key, _) = single(&control_root, control_when, "when: 'event.to != \"x\"'");
    assert_eq!((code, key), ("literal_where_binding_expected", None));
}

// REQ-WFDEF-10: the validator accepts exactly the `where` rules the shared corpus accepts,
// which the Gateway's `workflowtrigger.ParseRule` test reads too, so the two cannot drift.
#[test]
fn the_where_grammar_accepts_exactly_the_shared_corpus() {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/workflow/v1/trigger-rule-grammar.json"
    )))
    .unwrap();
    let rules = |key: &str| -> Vec<&str> {
        corpus[key]
            .as_array()
            .unwrap()
            .iter()
            .map(|rule| rule.as_str().unwrap())
            .collect()
    };
    let (accepted, rejected) = (rules("accepted"), rules("rejected"));
    assert!(!accepted.is_empty() && !rejected.is_empty());
    for rule in accepted {
        assert!(is_trigger_where(rule), "{rule:?} should be accepted");
    }
    for rule in rejected {
        assert!(!is_trigger_where(rule), "{rule:?} should be rejected");
    }
}

// REQ-WFDEF-10, REQ-WFDEF-30: `owner` names a `member` binding, and its filters `members`
// bindings beside an event owner only.
#[test]
fn owner_bindings_and_filters_are_checked() {
    let owner = "owner: {from: [event.issue.assignee, event.issue.creator], only: $includedOwners, except: $excludedOwners}";

    let rejections = trigger_rejections(&TRIGGER_ROOT.replacen(
        owner,
        "owner: {from: event.issue.assignee, only: $linearTeam}",
        1,
    ));
    assert_eq!(
        rejections,
        [(
            "binding_kind_mismatch",
            Some("start".to_owned()),
            Some("owner.only"),
            Some("linearTeam".to_owned()),
        )]
    );

    let rejections = trigger_rejections(&TRIGGER_ROOT.replacen(
        owner,
        "owner: {from: $nightlyOwner, only: $includedOwners}",
        1,
    ));
    assert_eq!(
        rejections,
        [(
            "owner_filter_invalid",
            Some("start".to_owned()),
            Some("owner.only"),
            Some("includedOwners".to_owned()),
        )]
    );

    let rejections = trigger_rejections(&TRIGGER_ROOT.replacen(owner, "owner: $linearTeam", 1));
    assert_eq!(
        rejections,
        [(
            "binding_kind_mismatch",
            Some("start".to_owned()),
            Some("owner"),
            Some("linearTeam".to_owned()),
        )]
    );

    let rejections = trigger_rejections(&TRIGGER_ROOT.replacen(owner, "owner: $nobody", 1));
    assert_eq!(
        rejections,
        [(
            "binding_undeclared",
            Some("start".to_owned()),
            Some("owner"),
            Some("nobody".to_owned()),
        )]
    );
}

// REQ-WFDEF-10, REQ-WFDEF-30
#[test]
fn stop_before_labels_must_be_an_event_path() {
    let (code, key) = single_trigger_rejection("labels: event.issue.labels", "labels: \"x\"");
    assert_eq!(
        (code, key),
        ("stop_before_labels_invalid", Some("stopBefore.labels"))
    );
}

// REQ-WFDEF-10, REQ-WFDEF-14, REQ-WFDEF-31: a schedule trigger reads no event, and its
// timetable has the cron and zone shape.
#[test]
fn a_schedule_trigger_reads_no_event_and_has_a_timetable_of_the_right_shape() {
    let schedule = "    on: {schedule: \"0 6 * * *\", timezone: \"UTC\"}\n";
    let with =
        |addition: &str| TRIGGER_ROOT.replacen(schedule, &format!("{schedule}{addition}"), 1);
    let nightly = |code, key| (code, Some("nightly".to_owned()), Some(key), None);

    assert_eq!(
        trigger_rejections(&with("    subject: event.x\n")),
        [nightly("schedule_trigger_invalid", "subject")]
    );
    assert_eq!(
        trigger_rejections(&with(
            "    stopBefore: {labels: event.issue.labels, prefix: \"hold:\"}\n"
        )),
        [nightly("schedule_trigger_invalid", "stopBefore")]
    );
    assert_eq!(
        trigger_rejections(&TRIGGER_ROOT.replacen(
            "ticket: \"nightly\"",
            "ticket: event.issue.identifier",
            1
        )),
        [nightly("schedule_trigger_invalid", "inputs")]
    );
    assert_eq!(
        trigger_rejections(&TRIGGER_ROOT.replacen(
            "owner: $nightlyOwner",
            "owner: event.issue.assignee",
            1
        )),
        [nightly("schedule_trigger_invalid", "owner")]
    );
    // A literal subject keeps one live run at a time; the tree keeps it for the report
    // (REQ-WFDEF-14).
    let tree = trigger_tree(&with("    subject: nightly\n"));
    let validated = validate_workflow_tree(tree.path())
        .unwrap_or_else(|diagnostics| panic!("tree should be valid: {diagnostics:?}"));
    assert_eq!(
        validated.triggers["nightly"].subject.as_deref(),
        Some("nightly")
    );

    let timing = |schedule: &str, timezone: &str| {
        TRIGGER_ROOT.replacen(
            "{schedule: \"0 6 * * *\", timezone: \"UTC\"}",
            &format!("{{schedule: \"{schedule}\", timezone: \"{timezone}\"}}"),
            1,
        )
    };
    for (schedule, timezone, key) in [
        ("0 6 * * * *", "UTC", "schedule"),
        ("0 6 * *", "UTC", "schedule"),
        ("0 6 ? * MON", "UTC", "schedule"),
        ("0 6 * * MONDAY", "UTC", "schedule"),
        ("0 6 * * *", "Mars", "timezone"),
        ("0 6 * * *", "Europe/", "timezone"),
    ] {
        assert_eq!(
            trigger_rejections(&timing(schedule, timezone)),
            [nightly("schedule_timing_invalid", key)],
            "{schedule} {timezone}"
        );
    }
    for (schedule, timezone) in [
        (
            "*/15 0-6,22 1 JAN-MAR mon",
            "America/Argentina/Buenos_Aires",
        ),
        ("0 6 * * *", "Etc/GMT+5"),
    ] {
        let tree = trigger_tree(&timing(schedule, timezone));
        validate_workflow_tree(tree.path())
            .unwrap_or_else(|diagnostics| panic!("{schedule} {timezone}: {diagnostics:?}"));
    }
}
