use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tempfile::TempDir;

use super::{CREDENTIALS_FILE_VARIABLE, run_with_env};

const ROOT_WORKFLOW: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/crates/execution/tests/fixtures/workflow/v1/valid/root-workflow.yaml"
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
const CONTENT_SENTINEL: &str = "unique-tree-content-sentinel";
const START_WHERE: &str = "event.team == $linearTeam and event.state == $ownedState";

pub(super) struct Tree {
    _temporary: TempDir,
    root: PathBuf,
}

impl Tree {
    pub(super) fn engineering() -> Self {
        Self::with(ROOT_WORKFLOW, DESIGN_WORKFLOW)
    }

    fn with(root_workflow: &str, design_workflow: &str) -> Self {
        let temporary = tempfile::tempdir().expect("temporary tree should be created");
        let root = temporary.path().join("engineering");
        let tree = Self {
            _temporary: temporary,
            root,
        };
        tree.write("workflow.yaml", root_workflow);
        tree.write("design.yaml", design_workflow);
        tree.write(
            "scripts/design.py",
            &format!("print('{CONTENT_SENTINEL}')\n"),
        );
        tree.write(
            "policy/engineering.json",
            &format!("{{\"note\": \"{CONTENT_SENTINEL}\"}}\n"),
        );
        tree.write("tests/test_x.py", "def test_x():\n    pass\n");
        tree
    }

    pub(super) fn write(&self, relative: &str, contents: &str) {
        let path = self.root.join(relative);
        fs::create_dir_all(path.parent().expect("tree file should have a parent"))
            .expect("tree directory should be created");
        fs::write(path, contents).expect("tree file should be written");
    }

    pub(super) fn path(&self) -> &str {
        self.root
            .to_str()
            .expect("temporary tree path should be UTF-8")
    }
}

fn validate(directory: &str, json: bool) -> std::process::Output {
    let mut args = vec!["workflow", "definition", "validate", directory];
    if json {
        args.push("--json");
    }
    run_offline(&args, &[])
}

/// Runs `um` with unusable credentials and a network sentinel that fails the test when the
/// command opens a connection; `environment` adds pairs such as `UM_HOME`.
fn run_offline(args: &[&str], environment: &[(&str, &str)]) -> std::process::Output {
    let listener = TcpListener::bind("127.0.0.1:0").expect("network sentinel should bind");
    listener
        .set_nonblocking(true)
        .expect("network sentinel should be nonblocking");
    let api_url = format!("http://{}/api", listener.local_addr().unwrap());
    let mut pairs = vec![
        (
            CREDENTIALS_FILE_VARIABLE,
            "/dev/null/workflow-definition-credentials.json",
        ),
        ("UM_API_URL", api_url.as_str()),
        ("UM_AUTH_ISSUER", "http://auth.workflow-definition.invalid/"),
        (
            "UM_AUTH_AUDIENCE",
            "https://api.workflow-definition.invalid",
        ),
        ("UM_AUTH_CLIENT_ID", "workflow-definition-client"),
    ];
    pairs.extend_from_slice(environment);
    let output = run_with_env(args, &pairs);
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "workflow definition commands must not open a network connection"
    );
    output
}

fn report(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("tree validation output should be JSON")
}

fn assert_no_content(output: &std::process::Output) {
    for stream in [&output.stdout, &output.stderr] {
        assert!(
            !String::from_utf8_lossy(stream).contains(CONTENT_SENTINEL),
            "tree validation must not print file contents"
        );
    }
}

fn rejection_codes(directory: &str) -> Vec<(String, Value)> {
    let output = validate(directory, true);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_no_content(&output);
    let report = report(&output);
    assert_eq!(report["outcome"], "invalid");
    assert_eq!(report["command"], "um workflow definition validate");
    report["diagnostics"]
        .as_array()
        .expect("diagnostics should be a list")
        .iter()
        .map(|diagnostic| {
            (
                diagnostic["code"]
                    .as_str()
                    .expect("diagnostic code should be text")
                    .to_owned(),
                diagnostic["location"].clone(),
            )
        })
        .collect()
}

fn single_rejection(root_workflow: &str, design_workflow: &str) -> (String, Value) {
    let tree = Tree::with(root_workflow, design_workflow);
    let mut codes = rejection_codes(tree.path());
    assert_eq!(codes.len(), 1, "{codes:?}");
    codes.remove(0)
}

// REQ-WFDEF-8, REQ-WFDEF-9, REQ-WFDEF-14, SC-WFDEF-1
#[test]
fn engineering_tree_validates_with_digest_files_triggers_and_bindings() {
    let tree = Tree::engineering();
    let output = validate(tree.path(), true);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_no_content(&output);
    let report = report(&output);

    assert_eq!(report["schemaVersion"], 1);
    assert_eq!(report["command"], "um workflow definition validate");
    assert_eq!(report["outcome"], "valid");
    assert!(report["tree"]["path"].is_string());
    assert_eq!(report["name"], "engineering");
    assert_eq!(report["version"], "1.0.0");
    assert_eq!(report["digest"]["algorithm"], "sha256");
    let digest = report["digest"]["value"]
        .as_str()
        .expect("digest value should be text");
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        report["workflowFiles"],
        json!(["workflow.yaml", "design.yaml"])
    );
    assert_eq!(report["programs"], json!(["scripts"]));
    assert_eq!(report["requiresTools"], json!(["git", "gh", "python3"]));
    assert_eq!(
        report["triggers"],
        json!({
            "start": {
                "kind": "start",
                "source": "$connection",
                "event": "issue.state_entered",
                "where": START_WHERE,
                "subject": "event.issue.identifier",
                "inputs": {"ticket": "event.issue.identifier", "baseBranch": "main"},
            },
            "release": {
                "kind": "control",
                "source": "$connection",
                "event": "issue.state_left",
                "where": START_WHERE,
            },
        })
    );
    assert_eq!(
        report["bindings"]["connection"],
        json!({"kind": "connection", "required": true})
    );
    assert_eq!(
        report["bindings"]["ownedState"],
        json!({"kind": "linear.state", "default": "Engineering", "required": false})
    );
    assert_eq!(
        report["bindings"]
            .as_object()
            .expect("bindings should be an object")
            .len(),
        7
    );

    // Moving excluded test files leaves the digest unchanged (REQ-WFDEF-12).
    fs::rename(
        tree.root.join("tests/test_x.py"),
        tree.root.join("tests/test_y.py"),
    )
    .expect("excluded file should move");
    assert_eq!(
        report_digest(&validate(tree.path(), true)),
        digest,
        "excluded files must not change the digest"
    );

    let human = validate(tree.path(), false);
    assert_eq!(human.status.code(), Some(0), "{human:?}");
    assert_no_content(&human);
    let stdout = String::from_utf8(human.stdout).expect("human output should be UTF-8");
    for expected in [
        "✓ Workflow tree is valid.",
        &format!("sha256:{digest}"),
        "workflow.yaml, design.yaml",
        "engineering",
        "1.0.0",
        "start",
        "release",
        "issue.state_entered",
        "(required)",
        "(default \"Engineering\")",
    ] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in {stdout}"
        );
    }
}

// REQ-WFDEF-14: `files` lists, in bytewise order, the paths the digest covers and none
// the root's `exclude` matches (`tests/`, `**/__pycache__/`).
#[test]
fn validate_json_reports_the_files_the_digest_covers() {
    let tree = Tree::engineering();
    tree.write("scripts/__pycache__/design.cpython-312.pyc", "cached\n");
    let output = validate(tree.path(), true);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        report(&output)["files"],
        json!([
            "design.yaml",
            "policy/engineering.json",
            "scripts/design.py",
            "workflow.yaml",
        ])
    );
}

fn report_digest(output: &std::process::Output) -> String {
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    report(output)["digest"]["value"]
        .as_str()
        .expect("digest value should be text")
        .to_owned()
}

// REQ-WFFORMAT-89
#[test]
fn a_root_with_a_workflow_step_validates_with_its_stage_file() {
    let steps = ROOT_WORKFLOW
        .find("steps:\n")
        .expect("the root fixture declares steps");
    let root = format!(
        "{}steps:\n  design:\n    kind: workflow\n    workflow: design.yaml\n    inputs:\n      ticket: {{ref: inputs.ticket}}\n    on:\n      succeeded: exit.landed\n      failed: exit.failed\noutcomes: [landed, failed]\n",
        &ROOT_WORKFLOW[..steps]
    );
    let tree = Tree::with(&root, DESIGN_WORKFLOW);
    let output = validate(tree.path(), true);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let report = report(&output);
    assert_eq!(report["outcome"], "valid");
    assert_eq!(
        report["workflowFiles"],
        json!(["workflow.yaml", "design.yaml"])
    );
}

// REQ-WFDEF-9
#[test]
fn root_only_block_in_a_stage_file_is_rejected() {
    let design = format!("{DESIGN_WORKFLOW}bindings:\n  team: {{kind: linear.team}}\n");
    let (code, location) = single_rejection(ROOT_WORKFLOW, &design);
    assert_eq!(code, "root_only_block_outside_root");
    assert_eq!(location["kind"], "workflow");
    assert_eq!(location["path"], "design.yaml");
    assert_eq!(location["key"], "bindings");
}

// REQ-WFDEF-9
#[test]
fn mirror_in_a_stage_file_is_rejected() {
    let design = format!("{DESIGN_WORKFLOW}mirror:\n  linear:\n    issue: inputs.ticket\n");
    let (code, location) = single_rejection(ROOT_WORKFLOW, &design);
    assert_eq!(code, "root_only_block_outside_root");
    assert_eq!(location["kind"], "workflow");
    assert_eq!(location["path"], "design.yaml");
    assert_eq!(location["key"], "mirror");
}

// REQ-WFFORMAT-101, REQ-WFDEF-10
#[test]
fn the_root_mirror_block_validates() {
    assert!(ROOT_WORKFLOW.contains("\nmirror:") && ROOT_WORKFLOW.contains("issue: inputs.ticket"));
    let tree = Tree::engineering();
    let output = validate(tree.path(), true);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(report(&output)["outcome"], "valid");
}

/// The one rejection of the engineering root with `from` replaced by `to` once.
fn mirror_rejection(from: &str, to: &str) -> (String, Value) {
    assert!(ROOT_WORKFLOW.contains(from), "{from:?}");
    single_rejection(&ROOT_WORKFLOW.replacen(from, to, 1), DESIGN_WORKFLOW)
}

fn assert_mirror_location(location: &Value, key: &str) {
    assert_eq!(location["kind"], "mirror", "{location}");
    assert_eq!(location["key"], key, "{location}");
}

fn assert_outcome_location(location: &Value, key: &str, outcome: &str) {
    assert_mirror_location(location, key);
    assert_eq!(location["outcome"], outcome, "{location}");
}

// REQ-WFFORMAT-82, REQ-WFFORMAT-101, REQ-WFDEF-10
#[test]
fn mirror_states_must_be_declared_linear_state_bindings() {
    let (code, location) = mirror_rejection("{state: $done}", "{state: Done}");
    assert_eq!(code, "invalid_workflow_structure");
    assert_eq!(location["kind"], "workflow");
    assert_eq!(location["path"], "workflow.yaml");

    let (code, location) = mirror_rejection("{state: $done}", "{state: $nope}");
    assert_eq!(code, "binding_undeclared");
    assert_outcome_location(&location, "onOutcome.state", "landed");
    assert_eq!(location["binding"], "nope");

    let (code, location) = mirror_rejection("{state: $done}", "{state: $linearTeam}");
    assert_eq!(code, "binding_kind_mismatch");
    assert_outcome_location(&location, "onOutcome.state", "landed");
    assert_eq!(location["binding"], "linearTeam");
}

// REQ-WFFORMAT-101, REQ-WFDEF-10
#[test]
fn mirror_guards_follow_the_guard_grammar() {
    const GUARD: &str = "unless: issue.state != $ownedState}";
    let (code, location) = mirror_rejection(GUARD, "unless: 'issue.state != \"Engineering\"'}");
    assert_eq!(code, "literal_where_binding_expected");
    assert_outcome_location(&location, "onOutcome.unless", "withdrawn");

    let (code, location) = mirror_rejection(GUARD, "unless: issue.state = $ownedState}");
    assert_eq!(code, "mirror_guard_invalid");
    assert_outcome_location(&location, "onOutcome.unless", "withdrawn");

    let (code, location) = mirror_rejection(GUARD, "unless: issue.state == $linearTeam}");
    assert_eq!(code, "binding_kind_mismatch");
    assert_outcome_location(&location, "onOutcome.unless", "withdrawn");
    assert_eq!(location["binding"], "linearTeam");
}

// REQ-WFFORMAT-101, REQ-WFDEF-10
#[test]
fn mirror_outcomes_must_be_outcomes_the_root_exits_with_other_than_superseded() {
    for outcome in ["superseded", "shipped"] {
        let (code, location) = mirror_rejection(
            "      landed:    {state: $done}\n",
            &format!("      landed:    {{state: $done}}\n      {outcome}: {{state: $backlog}}\n"),
        );
        assert_eq!(code, "mirror_outcome_undeclared");
        assert_outcome_location(&location, "onOutcome", outcome);
    }
}

// REQ-WFFORMAT-101, REQ-WFDEF-10
#[test]
fn mirror_templates_name_only_their_hooks_variables() {
    const START: &str = "Engineering started by {{operator}}. Run: {{run.url}}";
    for template in ["{{stage}}", "{{nope}}", "{{ operator }}"] {
        let (code, location) = mirror_rejection(START, template);
        assert_eq!(code, "mirror_variable_unknown", "{template}");
        assert_mirror_location(&location, "onStart.comment");
    }
    let (code, location) = mirror_rejection("{{wait.kind}}", "{{summary}}");
    assert_eq!(code, "mirror_variable_unknown");
    assert_mirror_location(&location, "onWait.comment");
}

// REQ-WFFORMAT-101, REQ-WFDEF-10: `operator`, `run.id` and `run.url` are every hook's.
#[test]
fn mirror_templates_name_the_operator_and_the_run_in_every_hook() {
    const EVERY_HOOK: &str = "{{operator}} {{run.id}} {{run.url}}";
    let mut root = ROOT_WORKFLOW.to_owned();
    for template in [
        "Engineering started by {{operator}}. Run: {{run.url}}",
        "{{stage}}: {{summary}}",
        "Waiting for {{wait.kind}}: {{wait.summary}}",
    ] {
        assert!(root.contains(template), "{template:?}");
        root = root.replacen(template, EVERY_HOOK, 1);
    }
    let tree = Tree::with(&root, DESIGN_WORKFLOW);
    let output = validate(tree.path(), true);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(report(&output)["outcome"], "valid");
}

// REQ-WFFORMAT-101, REQ-WFDEF-10
#[test]
fn the_mirror_issue_names_a_declared_text_input() {
    let (code, location) = mirror_rejection("issue: inputs.ticket", "issue: inputs.policy");
    assert_eq!(code, "mirror_issue_input_not_text");
    assert_mirror_location(&location, "issue");
    assert_eq!(location["input"], "policy");

    let (code, location) = mirror_rejection("issue: inputs.ticket", "issue: inputs.nothing");
    assert_eq!(code, "mirror_issue_input_undeclared");
    assert_mirror_location(&location, "issue");
    assert_eq!(location["input"], "nothing");
}

// REQ-WFDEF-9, REQ-WFFORMAT-100
#[test]
fn subject_in_a_stage_file_is_rejected() {
    let design = format!("{DESIGN_WORKFLOW}subject: ticket\n");
    let (code, location) = single_rejection(ROOT_WORKFLOW, &design);
    assert_eq!(code, "root_only_block_outside_root");
    assert_eq!(location["kind"], "workflow");
    assert_eq!(location["path"], "design.yaml");
    assert_eq!(location["key"], "subject");
}

// REQ-WFDEF-16
#[test]
fn a_command_program_named_by_path_is_rejected_with_its_file_and_step() {
    let design = DESIGN_WORKFLOW.replacen("design.py", "./scripts/design.py", 1);
    let (code, location) = single_rejection(ROOT_WORKFLOW, &design);
    assert_eq!(code, "command_program_is_path");
    assert_eq!(location["kind"], "workflow");
    assert_eq!(location["path"], "design.yaml");
    assert_eq!(location["step"], "design");
    assert_eq!(location["key"], "command.argv");
}

// REQ-WFDEF-10
#[test]
fn trigger_rules_must_name_declared_bindings_of_the_right_kind() {
    let literal = ROOT_WORKFLOW.replacen(
        &format!("\"{START_WHERE}\""),
        "'event.team == $linearTeam and event.state == \"Engineering\"'",
        1,
    );
    let (code, location) = single_rejection(&literal, DESIGN_WORKFLOW);
    assert_eq!(code, "literal_where_binding_expected");
    assert_eq!(location["kind"], "trigger");
    assert_eq!(location["trigger"], "start");

    let mismatched = ROOT_WORKFLOW.replacen("source: $connection", "source: $linearTeam", 1);
    let (code, location) = single_rejection(&mismatched, DESIGN_WORKFLOW);
    assert_eq!(code, "binding_kind_mismatch");
    assert_eq!(location["trigger"], "start");
    assert_eq!(location["binding"], "linearTeam");

    let undeclared = ROOT_WORKFLOW.replacen("$ownedState\"", "$nobody\"", 1);
    let (code, location) = single_rejection(&undeclared, DESIGN_WORKFLOW);
    assert_eq!(code, "binding_undeclared");
    assert_eq!(location["trigger"], "start");
    assert_eq!(location["binding"], "nobody");
}

// REQ-WFDEF-10: a trigger rule outside the grammar that evaluates it is refused, the start
// `where` against Cloud's and the control `when` against the wait's.
#[test]
fn trigger_rules_outside_their_grammar_are_refused() {
    let unequal = ROOT_WORKFLOW.replacen(
        &format!("\"{START_WHERE}\""),
        "\"event.team != $linearTeam and event.state == $ownedState\"",
        1,
    );
    let (code, location) = single_rejection(&unequal, DESIGN_WORKFLOW);
    assert_eq!(code, "trigger_rule_invalid");
    assert_eq!(location["kind"], "trigger");
    assert_eq!(location["trigger"], "start");
    assert_eq!(location["key"], "on.where");

    let disjunction = ROOT_WORKFLOW.replacen(
        "\"event.to == $canceled\"",
        "\"event.to == $canceled or event.to == $done\"",
        1,
    );
    let (code, location) = single_rejection(&disjunction, DESIGN_WORKFLOW);
    assert_eq!(code, "trigger_rule_invalid");
    assert_eq!(location["trigger"], "release");
    assert_eq!(location["key"], "control.action.when");
}

// REQ-WFDEF-10
#[test]
fn start_trigger_inputs_must_match_the_root_inputs() {
    let missing = ROOT_WORKFLOW.replacen("      ticket:     event.issue.identifier\n", "", 1);
    let (code, location) = single_rejection(&missing, DESIGN_WORKFLOW);
    assert_eq!(code, "trigger_input_missing");
    assert_eq!(location["trigger"], "start");
    assert_eq!(location["input"], "ticket");

    let undeclared = ROOT_WORKFLOW.replacen(
        "      baseBranch: \"main\"\n",
        "      baseBranch: \"main\"\n      extra: event.issue.title\n",
        1,
    );
    let (code, location) = single_rejection(&undeclared, DESIGN_WORKFLOW);
    assert_eq!(code, "trigger_input_undeclared");
    assert_eq!(location["input"], "extra");
}

// REQ-WFDEF-10
#[test]
fn programs_must_name_tree_directories() {
    let root = ROOT_WORKFLOW.replacen("programs: [scripts]", "programs: [bin]", 1);
    let (code, location) = single_rejection(&root, DESIGN_WORKFLOW);
    assert_eq!(code, "program_directory_missing");
    assert_eq!(location["kind"], "program");
    assert_eq!(location["program"], "bin");
}

// REQ-WFDEF-13
#[test]
fn tree_entries_must_be_regular_and_the_root_must_exist() {
    let tree = Tree::engineering();
    symlink(
        tree.root.join("policy/engineering.json"),
        tree.root.join("scripts/policy.json"),
    )
    .expect("symbolic link should be created");
    let codes = rejection_codes(tree.path());
    assert_eq!(codes.len(), 1, "{codes:?}");
    assert_eq!(codes[0].0, "tree_entry_not_regular");
    assert_eq!(codes[0].1["kind"], "tree");
    assert_eq!(codes[0].1["path"], "scripts/policy.json");

    let empty = tempfile::tempdir().expect("temporary directory should be created");
    let codes = rejection_codes(
        empty
            .path()
            .to_str()
            .expect("temporary directory should be UTF-8"),
    );
    assert_eq!(codes.len(), 1, "{codes:?}");
    assert_eq!(codes[0].0, "tree_root_missing");
}

// REQ-WFDEF-15, SC-WFDEF-1
#[test]
fn every_failure_in_a_tree_is_listed() {
    let root = ROOT_WORKFLOW.replacen(
        &format!("\"{START_WHERE}\""),
        "'event.team == $linearTeam and event.state == \"Engineering\"'",
        1,
    );
    let design = format!("{DESIGN_WORKFLOW}bindings:\n  team: {{kind: linear.team}}\n");
    let tree = Tree::with(&root, &design);
    let codes = rejection_codes(tree.path());
    let codes = codes
        .iter()
        .map(|(code, location)| (code.as_str(), location))
        .collect::<Vec<_>>();
    assert_eq!(codes.len(), 2, "{codes:?}");
    assert!(codes.iter().any(|(code, location)| {
        *code == "root_only_block_outside_root" && location["path"] == "design.yaml"
    }));
    assert!(codes.iter().any(|(code, location)| {
        *code == "literal_where_binding_expected" && location["trigger"] == "start"
    }));

    let human = validate(tree.path(), false);
    assert_eq!(human.status.code(), Some(1), "{human:?}");
    assert_no_content(&human);
    let stdout = String::from_utf8(human.stdout).expect("human output should be UTF-8");
    assert!(
        stdout.starts_with("✗ Workflow tree is invalid."),
        "{stdout}"
    );
    assert!(stdout.contains("root_only_block_outside_root"), "{stdout}");
    assert!(
        stdout.contains("literal_where_binding_expected"),
        "{stdout}"
    );
}

const PUBLISH_COMMAND: &str = "um workflow definition publish";

/// An empty `um` home in a temporary directory, passed to a command as `UM_HOME`.
pub(super) struct Home {
    _temporary: TempDir,
    root: PathBuf,
}

impl Home {
    pub(super) fn new() -> Self {
        let temporary = tempfile::tempdir().expect("temporary home should be created");
        let root = temporary.path().join("home");
        Self {
            _temporary: temporary,
            root,
        }
    }

    pub(super) fn path(&self) -> &str {
        self.root
            .to_str()
            .expect("temporary home path should be UTF-8")
    }

    /// Runs `um workflow definition ARGS` offline against this home.
    fn run(&self, args: &[&str]) -> std::process::Output {
        let mut full = vec!["workflow", "definition"];
        full.extend_from_slice(args);
        run_offline(&full, &[("UM_HOME", self.path())])
    }

    /// `um workflow definition publish DIR ARGS --json`, asserting it publishes; the report.
    pub(super) fn publish(&self, tree: &Tree, args: &[&str]) -> Value {
        let mut full = vec!["publish", tree.path()];
        full.extend_from_slice(args);
        let report = self.json(&full);
        assert_eq!(report["outcome"], "published", "{report}");
        report
    }

    /// `um workflow definition publish DIR ARGS --json`, asserting it is refused with `code`;
    /// the one diagnostic.
    fn refused(&self, tree: &Tree, args: &[&str], code: &str) -> Value {
        let mut full = vec!["publish", tree.path()];
        full.extend_from_slice(args);
        self.refused_command(&full, PUBLISH_COMMAND, code)
    }

    pub(super) fn definition_directory(&self, id: &str) -> PathBuf {
        self.root.join("workflows").join(id)
    }

    fn workflow_entries(&self) -> Vec<String> {
        match fs::read_dir(self.root.join("workflows")) {
            Ok(entries) => entries
                .map(|entry| {
                    entry
                        .expect("home entry should be readable")
                        .file_name()
                        .to_string_lossy()
                        .into_owned()
                })
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("home workflows should be readable: {error}"),
        }
    }

    /// No command here creates a run under the home or a definition (REQ-WFDEF-7).
    pub(super) fn assert_no_runs(&self) {
        assert!(!self.root.join("runs").exists());
        for id in self.workflow_entries() {
            assert!(!self.definition_directory(&id).join("runs").exists());
        }
    }
}

impl Tree {
    /// Writes `value` as a bindings file beside the tree, outside its digest.
    fn bindings_file(&self, file: &str, value: &Value) -> String {
        let path = self
            .root
            .parent()
            .expect("tree should have a parent")
            .join(file);
        fs::write(
            &path,
            serde_json::to_vec(value).expect("bindings should encode"),
        )
        .expect("bindings file should be written");
        path.to_str()
            .expect("temporary bindings path should be UTF-8")
            .to_owned()
    }

    pub(super) fn engineering_bindings(&self) -> String {
        self.bindings_file(
            "auto.json",
            &json!({
                "connection": "local",
                "linearTeam": "AUTO",
                "repository": "useful-machinery/platform",
            }),
        )
    }
}

fn is_definition_id(id: &str) -> bool {
    let Some(rest) = id.strip_prefix("def_") else {
        return false;
    };
    rest.len() == 26
        && rest.starts_with(|first: char| ('0'..='7').contains(&first))
        && rest.bytes().all(|byte| {
            byte.is_ascii_digit()
                || (byte.is_ascii_lowercase() && !matches!(byte, b'i' | b'l' | b'o' | b'u'))
        })
}

fn text<'a>(value: &'a Value, what: &str) -> &'a str {
    value
        .as_str()
        .unwrap_or_else(|| panic!("{what} should be text: {value}"))
}

pub(super) fn schema(file: &str) -> jsonschema::Validator {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join(file);
    let schema: Value = serde_json::from_slice(&fs::read(path).expect("schema should be readable"))
        .expect("schema should be JSON");
    jsonschema::draft202012::new(&schema).expect("schema should compile")
}

pub(super) fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("record should be readable"))
        .expect("record should be JSON")
}

// REQ-WFDEF-17, REQ-WFDEF-18, REQ-WFDEF-22, REQ-WFDEF-23, REQ-WFDEF-7
#[test]
fn publish_records_a_revision_with_its_tree_copy_and_carries_bindings_forward() {
    let home = Home::new();
    let tree = Tree::engineering();
    let digest = format!("sha256:{}", report_digest(&validate(tree.path(), true)));
    let bindings = tree.engineering_bindings();

    let first = home.publish(&tree, &["--bindings", &bindings]);
    assert_eq!(first["schemaVersion"], 1);
    assert_eq!(first["command"], PUBLISH_COMMAND);
    let id = text(&first["definition"]["id"], "definition id").to_owned();
    assert!(is_definition_id(&id), "{id}");
    assert_eq!(first["definition"]["name"], "engineering");
    assert_eq!(first["definition"]["enabled"], true);
    assert_eq!(first["definition"]["current"], 1);
    assert_eq!(first["definition"]["created"], true);
    assert_eq!(
        Path::new(text(&first["definition"]["path"], "definition path")),
        home.definition_directory(&id)
    );
    assert_eq!(first["revision"]["n"], 1);
    assert_eq!(first["revision"]["tree"], digest.as_str());
    let bindings_digest = text(&first["revision"]["bindings"], "bindings digest").to_owned();
    assert!(
        bindings_digest
            .strip_prefix("sha256:")
            .is_some_and(|hex| hex.len() == 64),
        "{bindings_digest}"
    );
    time::OffsetDateTime::parse(
        text(&first["revision"]["publishedAt"], "publish time"),
        &time::format_description::well_known::Rfc3339,
    )
    .expect("publish time should be RFC 3339");
    assert!(first["revision"]["by"].is_string());

    let definition = home.definition_directory(&id);
    let record = read_json(&definition.join("definition.json"));
    assert!(
        schema("workflow-definition-v1.schema.json").is_valid(&record),
        "{record}"
    );
    let snapshot = read_json(&definition.join("revisions/1/bindings.json"));
    assert!(
        schema("workflow-definition-bindings-v1.schema.json").is_valid(&snapshot),
        "{snapshot}"
    );
    let copy = home
        .root
        .join("trees/sha256")
        .join(digest.trim_start_matches("sha256:"));
    assert!(copy.join("scripts/design.py").is_file());
    assert!(!copy.join("tests").exists());

    let second = home.publish(&tree, &[]);
    assert_eq!(second["definition"]["id"], id.as_str());
    assert_eq!(second["definition"]["created"], false);
    assert_eq!(second["definition"]["current"], 2);
    assert_eq!(second["revision"]["n"], 2);
    assert_eq!(second["revision"]["tree"], digest.as_str());
    assert_eq!(second["revision"]["bindings"], bindings_digest.as_str());

    let human = home.run(&["publish", tree.path()]);
    assert_eq!(human.status.code(), Some(0), "{human:?}");
    assert_no_content(&human);
    let stdout = String::from_utf8(human.stdout).expect("human output should be UTF-8");
    for expected in [
        id.as_str(),
        "engineering",
        "3",
        digest.as_str(),
        bindings_digest.as_str(),
    ] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in {stdout}"
        );
    }
    assert_eq!(home.workflow_entries(), vec![id]);
    home.assert_no_runs();
}

// REQ-WFDEF-19, REQ-WFDEF-27, REQ-WFDEF-7
#[test]
fn publish_resolves_the_definition_by_name_or_refuses() {
    let home = Home::new();
    let unnamed = Tree::with(
        &ROOT_WORKFLOW.replacen("name: engineering\n", "", 1),
        DESIGN_WORKFLOW,
    );
    let bindings = unnamed.engineering_bindings();
    home.refused(
        &unnamed,
        &["--bindings", &bindings],
        "definition_name_missing",
    );
    assert!(home.workflow_entries().is_empty());

    let tree = Tree::engineering();
    let bindings = tree.engineering_bindings();
    let first = home.publish(&tree, &["--bindings", &bindings]);
    let first_id = text(&first["definition"]["id"], "definition id").to_owned();
    let canary = home.publish(&tree, &["--new", "--bindings", &bindings]);
    assert_eq!(canary["definition"]["created"], true);
    assert_eq!(canary["revision"]["n"], 1);
    let canary_id = text(&canary["definition"]["id"], "definition id").to_owned();
    assert_ne!(first_id, canary_id);

    let ambiguous = home.refused(&tree, &[], "definition_name_ambiguous");
    let mut both = vec![first_id.clone(), canary_id.clone()];
    both.sort();
    assert_eq!(ambiguous["definitions"], json!(both));

    let human = home.run(&["publish", tree.path()]);
    assert_eq!(human.status.code(), Some(1), "{human:?}");
    let stdout = String::from_utf8(human.stdout).expect("human output should be UTF-8");
    for expected in ["definition_name_ambiguous", &first_id, &canary_id] {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in {stdout}"
        );
    }

    let unknown = "def_01jzzzzzzzzzzzzzzzzzzzzzzz";
    let not_found = home.refused(&tree, &["--to", unknown], "definition_not_found");
    assert_eq!(not_found["definition"], unknown);

    let targeted = home.publish(&tree, &["--to", &first_id]);
    assert_eq!(targeted["definition"]["id"], first_id.as_str());
    assert_eq!(targeted["revision"]["n"], 2);

    let usage = home.run(&["publish", tree.path(), "--to", &first_id, "--new", "--json"]);
    assert_eq!(usage.status.code(), Some(2), "{usage:?}");
    assert!(usage.stdout.is_empty(), "{usage:?}");
    home.assert_no_runs();
}

// REQ-WFDEF-18, REQ-WFDEF-7
#[test]
fn publish_of_an_invalid_tree_reports_it_and_writes_nothing() {
    let home = Home::new();
    let design = format!("{DESIGN_WORKFLOW}bindings:\n  team: {{kind: linear.team}}\n");
    let tree = Tree::with(ROOT_WORKFLOW, &design);
    let bindings = tree.engineering_bindings();
    let output = home.run(&["publish", tree.path(), "--bindings", &bindings, "--json"]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_no_content(&output);
    let report = report(&output);
    assert_eq!(report["command"], PUBLISH_COMMAND);
    assert_eq!(report["outcome"], "invalid");
    assert!(report["tree"]["path"].is_string());
    assert!(
        report["diagnostics"]
            .as_array()
            .expect("diagnostics should be a list")
            .iter()
            .any(|diagnostic| diagnostic["code"] == "root_only_block_outside_root"),
        "{report}"
    );
    assert!(!home.root.join("workflows").exists());
    home.assert_no_runs();
}

// REQ-WFDEF-20, REQ-WFDEF-21, REQ-WFDEF-27, REQ-WFDEF-7
#[test]
fn publish_refuses_missing_or_unusable_bindings() {
    let home = Home::new();
    let tree = Tree::engineering();
    home.refused(&tree, &[], "bindings_required");

    let partial = tree.bindings_file(
        "partial.json",
        &json!({"connection": "local", "linearTeam": "AUTO"}),
    );
    let missing = home.refused(&tree, &["--bindings", &partial], "binding_missing");
    assert_eq!(missing["binding"], "repository");

    let absent = tree
        .root
        .parent()
        .expect("tree should have a parent")
        .join("absent.json");
    let absent = absent.to_str().expect("temporary path should be UTF-8");
    let unreadable = home.refused(&tree, &["--bindings", absent], "bindings_file_invalid");
    assert_eq!(unreadable["path"], absent);

    assert!(home.workflow_entries().is_empty());
    home.assert_no_runs();
}

const MEMBER_ROOT: &str = r#"schemaVersion: 1
name: owners
bindings:
  includedOwners: {kind: members, default: []}
  reviewers: {kind: members, default: ["prn_a", "prn_b"]}
  nightlyOwner: {kind: member}
steps:
  prepare:
    kind: cmd
    command:
      argv: ["true"]
"#;

// REQ-WFDEF-14, REQ-WFDEF-20, REQ-WFDEF-26, REQ-WFDEF-29: `member` and `members` bindings
// are reported with their defaults as declared and shown with their values as bound, a
// list kept a list.
#[test]
fn member_bindings_are_reported_and_shown_as_typed_values() {
    let tree = Tree::with(MEMBER_ROOT, DESIGN_WORKFLOW);
    let output = validate(tree.path(), true);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        report(&output)["bindings"],
        json!({
            "includedOwners": {"kind": "members", "default": [], "required": false},
            "reviewers": {"kind": "members", "default": ["prn_a", "prn_b"], "required": false},
            "nightlyOwner": {"kind": "member", "required": true},
        })
    );
    let human = validate(tree.path(), false);
    assert_eq!(human.status.code(), Some(0), "{human:?}");
    assert_contains_all(
        &String::from_utf8(human.stdout).expect("human output should be UTF-8"),
        &[
            "includedOwners: members (default [])",
            "reviewers: members (default [\"prn_a\", \"prn_b\"])",
            "nightlyOwner: member (required)",
        ],
    );

    let home = Home::new();
    let string_for_members = tree.bindings_file(
        "string.json",
        &json!({"nightlyOwner": "prn_x", "includedOwners": "prn_a"}),
    );
    let refused = home.refused(
        &tree,
        &["--bindings", &string_for_members],
        "binding_value_invalid",
    );
    assert_eq!(refused["binding"], "includedOwners");

    let owners = tree.bindings_file(
        "owners.json",
        &json!({"nightlyOwner": "prn_x", "includedOwners": ["prn_a"]}),
    );
    let published = home.publish(&tree, &["--bindings", &owners]);
    let id = text(&published["definition"]["id"], "definition id").to_owned();
    let shown = home.json(&["show", &id]);
    assert_eq!(
        shown["bindings"],
        json!({
            "includedOwners": {"kind": "members", "value": ["prn_a"]},
            "reviewers": {"kind": "members", "value": ["prn_a", "prn_b"]},
            "nightlyOwner": {"kind": "member", "value": "prn_x"},
        })
    );
    assert_contains_all(
        &home.human(&["show", &id], 0),
        &[
            "includedOwners: [\"prn_a\"] (members)",
            "nightlyOwner: prn_x (member)",
        ],
    );
}

/// The engineering root with a start policy on `start` and a `nightly` schedule trigger.
fn start_policy_root() -> String {
    let replacements = [
        (
            "  repository:      {kind: github.repository}\n",
            "  repository:      {kind: github.repository}\n  includedOwners:  {kind: members, default: []}\n  nightlyOwner:    {kind: member, default: \"prn_n\"}\n",
        ),
        (
            "and event.state == $ownedState\"}\n    subject:",
            "and event.state == $ownedState and event.issue.blocked == false\"}\n    stopBefore: {labels: event.issue.labels, prefix: \"hold:\"}\n    owner: {from: [event.issue.assignee, event.issue.creator], only: $includedOwners}\n    subject:",
        ),
        (
            "  release:",
            "  nightly:\n    on: {schedule: \"0 6 * * *\", timezone: \"UTC\"}\n    subject: \"nightly-sweep\"\n    owner: $nightlyOwner\n    inputs:\n      ticket: \"nightly\"\n  release:",
        ),
    ];
    replacements
        .into_iter()
        .fold(ROOT_WORKFLOW.to_owned(), |root, (from, to)| {
            assert!(root.contains(from), "{from}");
            root.replacen(from, to, 1)
        })
}

// REQ-WFDEF-10, REQ-WFDEF-14, REQ-WFDEF-30, REQ-WFDEF-31: the report prints each start
// trigger's `where`, `subject`, `inputs`, `stopBefore` and `owner` and names a schedule
// trigger by its kind and timetable; publishing the tree starts no run.
#[test]
fn start_policies_and_schedule_triggers_are_reported_and_start_nothing() {
    let tree = Tree::with(&start_policy_root(), DESIGN_WORKFLOW);
    let output = validate(tree.path(), true);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        report(&output)["triggers"],
        json!({
            "start": {
                "kind": "start",
                "source": "$connection",
                "event": "issue.state_entered",
                "where": format!("{START_WHERE} and event.issue.blocked == false"),
                "subject": "event.issue.identifier",
                "inputs": {"ticket": "event.issue.identifier", "baseBranch": "main"},
                "stopBefore": {"labels": "event.issue.labels", "prefix": "hold:"},
                "owner": {
                    "from": ["event.issue.assignee", "event.issue.creator"],
                    "only": "$includedOwners",
                },
            },
            "nightly": {
                "kind": "schedule",
                "schedule": "0 6 * * *",
                "timezone": "UTC",
                "subject": "nightly-sweep",
                "inputs": {"ticket": "nightly"},
                "owner": {"from": "$nightlyOwner"},
            },
            "release": {
                "kind": "control",
                "source": "$connection",
                "event": "issue.state_left",
                "where": START_WHERE,
            },
        })
    );
    let human = validate(tree.path(), false);
    assert_eq!(human.status.code(), Some(0), "{human:?}");
    assert_contains_all(
        &String::from_utf8(human.stdout).expect("human output should be UTF-8"),
        &[
            "nightly: schedule",
            "0 6 * * *",
            "hold:",
            "$includedOwners",
            "event.issue.blocked == false",
            "nightly-sweep",
            "event.issue.identifier",
        ],
    );

    let home = Home::new();
    home.publish(&tree, &["--bindings", &tree.engineering_bindings()]);
    home.assert_no_runs();
}

impl Home {
    /// `um workflow definition ARGS --json`, asserting it exits 0; the report.
    pub(super) fn json(&self, args: &[&str]) -> Value {
        let mut full = args.to_vec();
        full.push("--json");
        let output = self.run(&full);
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        assert_no_content(&output);
        report(&output)
    }

    /// `um workflow definition ARGS --json`, asserting it is refused with `code`; the one
    /// diagnostic.
    fn refused_command(&self, args: &[&str], command: &str, code: &str) -> Value {
        let mut full = args.to_vec();
        full.push("--json");
        let output = self.run(&full);
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert_no_content(&output);
        let report = report(&output);
        assert_eq!(report["schemaVersion"], 1);
        assert_eq!(report["command"], command);
        assert_eq!(report["outcome"], "refused", "{report}");
        let diagnostics = report["diagnostics"]
            .as_array()
            .expect("diagnostics should be a list");
        assert_eq!(diagnostics.len(), 1, "{report}");
        assert_eq!(diagnostics[0]["code"], code, "{report}");
        diagnostics[0].clone()
    }

    /// `um workflow definition ARGS` in the human form, asserting its exit status; stdout.
    fn human(&self, args: &[&str], status: i32) -> String {
        let output = self.run(args);
        assert_eq!(output.status.code(), Some(status), "{output:?}");
        assert_no_content(&output);
        String::from_utf8(output.stdout).expect("human output should be UTF-8")
    }

    /// The engineering definition and a `--new` canary of the same name; their ids.
    fn seed(&self, tree: &Tree) -> (String, String) {
        let bindings = tree.engineering_bindings();
        let first = self.publish(tree, &["--bindings", &bindings]);
        let canary = self.publish(tree, &["--new", "--bindings", &bindings]);
        (
            text(&first["definition"]["id"], "definition id").to_owned(),
            text(&canary["definition"]["id"], "definition id").to_owned(),
        )
    }
}

fn assert_contains_all(stdout: &str, expected: &[&str]) {
    for expected in expected {
        assert!(
            stdout.contains(expected),
            "missing {expected:?} in {stdout}"
        );
    }
}

const UNKNOWN_ID: &str = "def_01jzzzzzzzzzzzzzzzzzzzzzzz";

// REQ-WFDEF-24, REQ-WFDEF-27
#[test]
fn enable_and_disable_set_only_the_enabled_flag() {
    let home = Home::new();
    let tree = Tree::engineering();
    let (id, _) = home.seed(&tree);
    let record_path = home.definition_directory(&id).join("definition.json");
    let before = read_json(&record_path);
    assert_eq!(before["enabled"], true);

    let disabled = home.json(&["disable", &id]);
    assert_eq!(disabled["schemaVersion"], 1);
    assert_eq!(disabled["command"], "um workflow definition disable");
    assert_eq!(disabled["outcome"], "disabled");
    assert_eq!(disabled["definition"]["id"], id.as_str());
    assert_eq!(disabled["definition"]["name"], "engineering");
    assert_eq!(disabled["definition"]["enabled"], false);
    assert_eq!(disabled["definition"]["current"], 1);
    assert_eq!(disabled["definition"]["changed"], true);
    assert_eq!(
        Path::new(text(&disabled["definition"]["path"], "definition path")),
        home.definition_directory(&id)
    );
    let mut expected = before.clone();
    expected["enabled"] = json!(false);
    let after = read_json(&record_path);
    assert_eq!(after, expected);
    assert!(
        schema("workflow-definition-v1.schema.json").is_valid(&after),
        "{after}"
    );

    let again = home.json(&["disable", &id]);
    assert_eq!(again["outcome"], "disabled");
    assert_eq!(again["definition"]["enabled"], false);
    assert_eq!(again["definition"]["changed"], false);
    assert_eq!(read_json(&record_path), expected);

    let stdout = home.human(&["enable", &id], 0);
    assert_contains_all(&stdout, &[&id, "engineering", "enabled"]);
    assert_eq!(read_json(&record_path), before);
    let enabled = home.json(&["enable", &id]);
    assert_eq!(enabled["command"], "um workflow definition enable");
    assert_eq!(enabled["outcome"], "enabled");
    assert_eq!(enabled["definition"]["enabled"], true);
    assert_eq!(enabled["definition"]["changed"], false);

    let not_found = home.refused_command(
        &["disable", UNKNOWN_ID],
        "um workflow definition disable",
        "definition_not_found",
    );
    assert_eq!(not_found["definition"], UNKNOWN_ID);
    let invalid = home.refused_command(
        &["show", "engineering"],
        "um workflow definition show",
        "definition_id_invalid",
    );
    assert_eq!(invalid["definition"], "engineering");
    let stdout = home.human(&["enable", "engineering"], 1);
    assert_contains_all(&stdout, &["definition_id_invalid", "engineering"]);
    home.assert_no_runs();
}

// REQ-WFDEF-25
#[test]
fn list_prints_every_definition_sorted_by_name_then_id() {
    let home = Home::new();
    let empty = home.json(&["list"]);
    assert_eq!(empty["schemaVersion"], 1);
    assert_eq!(empty["command"], "um workflow definition list");
    assert_eq!(empty["definitions"], json!([]));
    assert_eq!(
        home.human(&["list"], 0).trim_end(),
        "No workflow definitions."
    );
    assert!(!home.root.exists(), "list must not create the home");

    let tree = Tree::engineering();
    let (first, canary) = home.seed(&tree);
    let second = home.publish(&tree, &["--to", &first]);
    let alpha = Tree::with(
        &ROOT_WORKFLOW.replacen("name: engineering\n", "name: alpha\n", 1),
        DESIGN_WORKFLOW,
    );
    let alpha_bindings = alpha.engineering_bindings();
    let alpha = home.publish(&alpha, &["--bindings", &alpha_bindings]);
    let alpha_id = text(&alpha["definition"]["id"], "definition id").to_owned();
    home.json(&["disable", &canary]);

    let listed = home.json(&["list"]);
    let definitions = listed["definitions"]
        .as_array()
        .expect("definitions should be a list");
    let mut engineering = [first.clone(), canary.clone()];
    engineering.sort();
    let order = definitions
        .iter()
        .map(|definition| text(&definition["id"], "definition id"))
        .collect::<Vec<_>>();
    assert_eq!(
        order,
        vec![alpha_id.as_str(), &engineering[0], &engineering[1]]
    );
    for definition in definitions {
        let id = text(&definition["id"], "definition id");
        assert_eq!(
            Path::new(text(&definition["path"], "definition path")),
            home.definition_directory(id)
        );
        let record = read_json(&home.definition_directory(id).join("definition.json"));
        assert_eq!(
            definition["publishedAt"],
            record["revisions"][0]["publishedAt"]
        );
    }
    let by_id = |id: &str| {
        definitions
            .iter()
            .find(|definition| definition["id"] == id)
            .expect("definition should be listed")
            .clone()
    };
    let listed_first = by_id(&first);
    assert_eq!(listed_first["name"], "engineering");
    assert_eq!(listed_first["enabled"], true);
    assert_eq!(listed_first["current"], 2);
    assert_eq!(listed_first["revisions"], 2);
    assert_eq!(
        listed_first["publishedAt"],
        second["revision"]["publishedAt"]
    );
    let listed_canary = by_id(&canary);
    assert_eq!(listed_canary["enabled"], false);
    assert_eq!(listed_canary["current"], 1);
    assert_eq!(listed_canary["revisions"], 1);
    assert_eq!(by_id(&alpha_id)["name"], "alpha");

    let stdout = home.human(&["list"], 0);
    assert_eq!(stdout.lines().count(), 3, "{stdout}");
    assert_contains_all(
        &stdout,
        &[&first, &canary, &alpha_id, "engineering", "alpha"],
    );
    home.assert_no_runs();
}

// REQ-WFDEF-26, SC-WFDEF-2
#[test]
fn show_prints_the_record_its_revisions_and_the_current_bindings() {
    let home = Home::new();
    let tree = Tree::engineering();
    let bindings = tree.engineering_bindings();
    let first = home.publish(&tree, &["--bindings", &bindings]);
    let id = text(&first["definition"]["id"], "definition id").to_owned();
    tree.write(
        "policy/engineering.json",
        &format!("{{\"note\": \"{CONTENT_SENTINEL} edited\"}}\n"),
    );
    let second = home.publish(&tree, &[]);
    assert_eq!(second["revision"]["n"], 2);
    assert_ne!(second["revision"]["tree"], first["revision"]["tree"]);

    let canary = home.publish(&tree, &["--new", "--bindings", &bindings]);
    let canary_id = text(&canary["definition"]["id"], "definition id").to_owned();
    home.refused(&tree, &[], "definition_name_ambiguous");
    let third = home.publish(&tree, &["--to", &id]);
    assert_eq!(third["revision"]["n"], 3);
    home.json(&["disable", &canary_id]);
    let listed = home.json(&["list"]);
    let enabled = listed["definitions"]
        .as_array()
        .expect("definitions should be a list")
        .iter()
        .map(|definition| (definition["id"].clone(), definition["enabled"].clone()))
        .collect::<Vec<_>>();
    assert_eq!(enabled.len(), 2);
    assert!(enabled.contains(&(json!(id), json!(true))));
    assert!(enabled.contains(&(json!(canary_id), json!(false))));

    // The first revision's tree copy is gone from the home; the others stay.
    let first_hex = text(&first["revision"]["tree"], "tree digest")
        .trim_start_matches("sha256:")
        .to_owned();
    let trees = home.root.join("trees/sha256");
    fs::rename(
        trees.join(&first_hex),
        trees.join(format!("gone-{first_hex}")),
    )
    .expect("tree copy should move aside");

    let shown = home.json(&["show", &id]);
    assert_eq!(shown["schemaVersion"], 1);
    assert_eq!(shown["command"], "um workflow definition show");
    assert_eq!(
        shown["definition"],
        json!({
            "id": id,
            "name": "engineering",
            "enabled": true,
            "current": 3,
            "cloudId": null,
            "path": home.definition_directory(&id).to_str().expect("home path should be UTF-8"),
        })
    );
    let revisions = shown["revisions"]
        .as_array()
        .expect("revisions should be a list");
    assert_eq!(
        revisions
            .iter()
            .map(|revision| revision["n"].clone())
            .collect::<Vec<_>>(),
        vec![json!(3), json!(2), json!(1)]
    );
    for (revision, published) in revisions.iter().zip([&third, &second, &first]) {
        for field in ["n", "tree", "bindings", "publishedAt", "by"] {
            assert_eq!(revision[field], published["revision"][field], "{field}");
        }
    }
    assert_eq!(revisions[0]["treeRetained"], true);
    assert_eq!(revisions[1]["treeRetained"], true);
    assert_eq!(revisions[2]["treeRetained"], false);
    assert_eq!(
        shown["bindings"],
        json!({
            "connection": {"kind": "connection", "value": "local"},
            "linearTeam": {"kind": "linear.team", "value": "AUTO"},
            "ownedState": {"kind": "linear.state", "value": "Engineering"},
            "backlog": {"kind": "linear.state", "value": "Backlog"},
            "canceled": {"kind": "linear.state", "value": "Canceled"},
            "done": {"kind": "linear.state", "value": "Done"},
            "repository": {"kind": "github.repository", "value": "useful-machinery/platform"},
        })
    );

    let stdout = home.human(&["show", &id], 0);
    assert_contains_all(
        &stdout,
        &[
            &id,
            "engineering",
            text(&third["revision"]["tree"], "tree digest"),
            text(&first["revision"]["tree"], "tree digest"),
            text(&third["revision"]["bindings"], "bindings digest"),
            "linearTeam",
            "linear.team",
            "AUTO",
            "useful-machinery/platform",
        ],
    );
    home.assert_no_runs();
}

// REQ-WFDEF-26, REQ-LOCALRUN-190: `show` lists the definition's live runs, a run behind the
// current revision drifted, and `No live runs.` for a definition without one.
#[test]
fn show_lists_the_live_runs_and_their_drift() {
    let published = super::workflow_run_control::PublishedWaitTree::with_subject();
    let home = &published.home;
    let id = published.definition.as_str();
    let (_, waiting) = published.waiting_for(id, "AUTO-1");
    let (_, exited) = published.waiting_for(id, "AUTO-2");
    let withdrawn = super::workflow_run_control::run_control(Some(home), "withdraw", &exited, &[]);
    assert_eq!(withdrawn.status.code(), Some(0), "{withdrawn:?}");
    let live = json!([{
        "id": waiting,
        "revision": 1,
        "subject": "AUTO-1",
        "state": "waiting",
        "drifted": false,
    }]);
    assert_eq!(home.json(&["show", id])["runs"], live);

    published.republish(&[]);
    let shown = home.json(&["show", id]);
    assert_eq!(shown["definition"]["current"], 2);
    let mut drifted = live;
    drifted[0]["drifted"] = json!(true);
    assert_eq!(shown["runs"], drifted, "{shown}");
    let stdout = home.human(&["show", id], 0);
    assert!(
        stdout
            .lines()
            .any(|line| line == format!("{waiting} · revision 1 · AUTO-1 · waiting · drifted")),
        "{stdout}"
    );
    assert!(!stdout.contains(&exited), "{stdout}");

    let canary = published.republish(&["--new"]);
    let canary = text(&canary["definition"]["id"], "definition id").to_owned();
    assert_eq!(home.json(&["show", &canary])["runs"], json!([]));
    let stdout = home.human(&["show", &canary], 0);
    assert!(
        stdout.lines().any(|line| line == "No live runs."),
        "{stdout}"
    );
}

// REQ-WFDEF-27
#[test]
fn list_and_show_refuse_a_record_not_of_the_published_shape() {
    let home = Home::new();
    let tree = Tree::engineering();
    let (id, _) = home.seed(&tree);
    let record_path = home.definition_directory(&id).join("definition.json");
    fs::write(&record_path, "{}").expect("record should be replaced");
    let record = record_path.to_str().expect("home path should be UTF-8");

    let listed = home.refused_command(
        &["list"],
        "um workflow definition list",
        "definition_record_invalid",
    );
    assert_eq!(listed["path"], record);
    let shown = home.refused_command(
        &["show", &id],
        "um workflow definition show",
        "definition_record_invalid",
    );
    assert_eq!(shown["path"], record);

    let stdout = home.human(&["list"], 1);
    assert_contains_all(&stdout, &["definition_record_invalid", record]);
    let stdout = home.human(&["show", &id], 1);
    assert_contains_all(&stdout, &["definition_record_invalid", record]);
}

const CLOUD_TOKEN: &str = "unique-workflow-definition-cloud-token-sentinel";
const CLOUD_ORGANIZATION: &str = "acme-research";
const CLOUD_PROJECT: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
const CLOUD_PUBLISHER: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5abc";
const CLOUD_TIME: &str = "2026-11-10T12:00:00Z";
/// A minted Cloud definition id is this and one more character.
const CLOUD_ID_STEM: &str = "def_01k0z6r1w8f4jy2m7q9v3x5ab";
const PROBLEMS: &str = "https://api.usefulmachinery.com/problems/";

/// One request the fake Gateway received, its path below the listener.
pub(super) struct GatewayRequest {
    pub(super) method: String,
    pub(super) path: String,
    headers: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
}

impl GatewayRequest {
    pub(super) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub(super) fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("request body should be JSON")
    }

    /// `METHOD path`, the path without its query.
    pub(super) fn route(&self) -> String {
        format!(
            "{} {}",
            self.method,
            self.path.split('?').next().unwrap_or_default()
        )
    }
}

/// What the fake Gateway holds between invocations: the file contents the organization
/// retains, the sealed tree digests, each definition, whether the next publication to an id
/// loses a race to another publisher or is forbidden, whether the reply to the next create
/// or publication is lost after it is applied, and each such reply by its `Idempotency-Key`.
#[derive(Default)]
struct GatewayState {
    retained: std::collections::BTreeSet<String>,
    sealed: std::collections::BTreeSet<String>,
    definitions: std::collections::BTreeMap<String, FakeDefinition>,
    lose_next_publication: bool,
    forbid_next_publication: bool,
    drop_next_publication_reply: bool,
    publications: std::collections::BTreeMap<String, Value>,
}

impl GatewayState {
    /// The reply to a create or publication keyed `key`: the one recorded under the key, or
    /// `publish`'s, recorded and, when the next reply is to be lost, not sent (no bytes).
    fn publication(
        &mut self,
        key: &str,
        keyed: impl Fn(&str, Value) -> Vec<u8>,
        publish: impl FnOnce(&mut Self) -> Result<Value, Vec<u8>>,
    ) -> Vec<u8> {
        if let Some(replayed) = self.publications.get(key) {
            return keyed("201 Created", replayed.clone());
        }
        let published = match publish(self) {
            Ok(published) => published,
            Err(refusal) => return refusal,
        };
        self.publications.insert(key.to_owned(), published.clone());
        if std::mem::take(&mut self.drop_next_publication_reply) {
            return Vec::new();
        }
        keyed("201 Created", published)
    }
}

/// One Cloud definition the fake Gateway holds: its current revision and settings.
#[derive(Clone, Default)]
struct FakeDefinition {
    current: i64,
    archived: bool,
    automatic_starts: bool,
    principal: Option<String>,
}

/// A loopback fake of the Gateway's tree and definition operations and of signed storage,
/// serving one `um` invocation at a time with a stored human session for `CLOUD_TOKEN`. A
/// tree upload reports missing the files under `scripts/` and `policy/` the organization
/// does not yet retain.
pub(super) struct FakeGateway {
    listener: TcpListener,
    api_url: String,
    _credentials: TempDir,
    credentials: String,
    state: std::sync::Arc<std::sync::Mutex<GatewayState>>,
}

impl FakeGateway {
    pub(super) fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("fake Gateway should bind");
        let api_url = format!("http://{}/api", listener.local_addr().unwrap());
        let directory = super::private_credential_directory();
        let credentials = directory.path().join("credentials.json");
        super::write_credential_fixture(
            &credentials,
            &api_url,
            CLOUD_TOKEN,
            "2999-01-01T00:00:00Z",
        );
        Self {
            listener,
            api_url,
            credentials: credentials.to_str().unwrap().to_owned(),
            _credentials: directory,
            state: std::sync::Arc::default(),
        }
    }

    /// Answers the next publication to an existing id with 409 `definition-revision-conflict`
    /// after another publisher's revision.
    pub(super) fn lose_next_publication(&self) {
        self.state.lock().unwrap().lose_next_publication = true;
    }

    /// Answers the next publication to an existing id with 403 `forbidden`, as Cloud answers
    /// a caller who is not an owner, and changes nothing.
    pub(super) fn forbid_next_publication(&self) {
        self.state.lock().unwrap().forbid_next_publication = true;
    }

    /// Applies the next create or publication but sends no reply to it.
    pub(super) fn drop_next_publication_reply(&self) {
        self.state.lock().unwrap().drop_next_publication_reply = true;
    }

    /// Holds definition `id` at revision `current`, active, disabled and with no principal.
    pub(super) fn define(&self, id: &str, current: i64) {
        self.state.lock().unwrap().definitions.insert(
            id.to_owned(),
            FakeDefinition {
                current,
                ..FakeDefinition::default()
            },
        );
    }

    /// Runs `um ARGS` against this Gateway and `home`; the output and the requests served.
    pub(super) fn run(
        &self,
        home: &Home,
        args: &[&str],
    ) -> (std::process::Output, Vec<GatewayRequest>) {
        let listener = self.listener.try_clone().unwrap();
        let address = listener.local_addr().unwrap();
        let state = std::sync::Arc::clone(&self.state);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = std::sync::Arc::clone(&stop);
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            loop {
                let (mut stream, _) = listener.accept().unwrap();
                if stopping.load(std::sync::atomic::Ordering::SeqCst) {
                    return requests;
                }
                let request = read_gateway_request(&stream);
                let response = respond(&mut state.lock().unwrap(), &request, &address.to_string());
                std::io::Write::write_all(&mut stream, &response).unwrap();
                requests.push(request);
            }
        });
        let mut environment =
            super::deployment_environment(&self.api_url, &self.credentials).to_vec();
        environment.push(("UM_HOME", home.path()));
        let output = run_with_env(args, &environment);
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(address);
        (output, server.join().unwrap())
    }
}

fn read_gateway_request(stream: &std::net::TcpStream) -> GatewayRequest {
    use std::io::{BufRead as _, Read as _};
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let path = parts.next().unwrap_or_default().to_owned();
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').expect("header should have a colon");
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
    }
    let length = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .map_or(0, |(_, value)| value.parse().unwrap());
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    GatewayRequest {
        method,
        path,
        headers,
        body,
    }
}

/// The 404 `not-found` problem the fake answers for anything it does not hold.
fn gateway_not_found() -> Vec<u8> {
    super::problem_http_response(
        "404 Not Found",
        json!({"type": format!("{PROBLEMS}not-found"), "title": "Not Found", "status": 404}),
    )
}

fn respond(state: &mut GatewayState, request: &GatewayRequest, address: &str) -> Vec<u8> {
    let organization = format!("/api/v1/organizations/{CLOUD_ORGANIZATION}");
    let definitions = format!("{organization}/projects/{CLOUD_PROJECT}/workflow-definitions");
    let route = request.route();
    let key = request
        .header("idempotency-key")
        .unwrap_or_default()
        .to_owned();
    let keyed = |status: &str, body: Value| {
        super::http_response_with_headers(
            status,
            Some("application/json"),
            &[("Idempotency-Key", &key), ("Location", "/v1/fixture")],
            &serde_json::to_vec(&body).unwrap(),
        )
    };
    if route == format!("POST {organization}/workflow-trees") {
        let manifest = request.json();
        let digest = text(&manifest["digest"], "manifest digest").to_owned();
        let sealed = state.sealed.contains(&digest);
        let capabilities =
            if sealed {
                Vec::new()
            } else {
                manifest["files"]
                .as_array()
                .expect("manifest files should be a list")
                .iter()
                .filter(|file| {
                    let path = text(&file["path"], "manifest path");
                    (path.starts_with("scripts/") || path.starts_with("policy/"))
                        && !state.retained.contains(text(&file["sha256"], "file sha256"))
                })
                .map(|file| {
                    let sha256 = text(&file["sha256"], "file sha256");
                    let raw = (0..sha256.len())
                        .step_by(2)
                        .map(|at| u8::from_str_radix(&sha256[at..at + 2], 16).unwrap())
                        .collect::<Vec<_>>();
                    json!({
                        "sha256": sha256,
                        "url": format!("http://{address}/storage/{sha256}?X-Amz-Signature=fixture"),
                        "requiredHeaders": {
                            "contentLength": file["sizeBytes"].to_string(),
                            "contentType": "application/octet-stream",
                            "ifNoneMatch": "*",
                            "xAmzChecksumSha256": base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD, raw),
                        },
                    })
                })
                .collect()
            };
        let expires = if capabilities.is_empty() {
            Value::Null
        } else {
            json!("2999-01-01T00:00:00Z")
        };
        return super::json_http_response(
            "200 OK",
            json!({
                "tree": gateway_tree(&digest, sealed),
                "capabilityExpiresAt": expires,
                "capabilities": capabilities,
            }),
        );
    }
    if let Some(sha256) = route.strip_prefix("PUT /storage/") {
        let observed = ring::digest::digest(&ring::digest::SHA256, &request.body);
        let hex = observed
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            hex, sha256,
            "an uploaded object should have its manifest digest"
        );
        state.retained.insert(hex);
        return super::http_response("200 OK", None, b"");
    }
    if let Some(digest) = route
        .strip_prefix(&format!("POST {organization}/workflow-trees/"))
        .and_then(|rest| rest.strip_suffix("/seal"))
    {
        let digest = digest.replace("%3A", ":").replace("%3a", ":");
        state.sealed.insert(digest.clone());
        return keyed("200 OK", gateway_tree(&digest, true));
    }
    if route == format!("POST {definitions}") {
        let body = request.json();
        return state.publication(&key, keyed, |state| {
            let id = format!(
                "{CLOUD_ID_STEM}{}",
                ['c', 'd', 'e', 'f', 'g', 'h'][state.definitions.len()]
            );
            state.definitions.insert(
                id.clone(),
                FakeDefinition {
                    current: 1,
                    ..FakeDefinition::default()
                },
            );
            Ok(gateway_publication(
                &id,
                &state.definitions[&id],
                &body["treeDigest"],
            ))
        });
    }
    if let Some(response) = respond_settings(state, request, &definitions, keyed) {
        return response;
    }
    if let Some((id, evaluation)) = route
        .strip_prefix(&format!("GET {definitions}/"))
        .and_then(|rest| rest.split_once("/evaluations"))
    {
        return super::json_http_response(
            "200 OK",
            match evaluation.strip_prefix('/') {
                Some(evaluation) => gateway_evaluation(id, evaluation),
                None => json!({
                    "items": [gateway_evaluation(id, CLOUD_EVALUATION)],
                    "nextCursor": "older-evaluations",
                }),
            },
        );
    }
    if let Some(id) = route.strip_prefix(&format!("GET {definitions}/")) {
        return match state.definitions.get(id) {
            Some(definition) => {
                super::json_http_response("200 OK", gateway_definition(id, definition))
            }
            None => gateway_not_found(),
        };
    }
    if let Some(id) = route
        .strip_prefix(&format!("POST {definitions}/"))
        .and_then(|rest| rest.strip_suffix("/revisions"))
    {
        let body = request.json();
        return state.publication(&key, keyed, |state| {
            let lose = std::mem::take(&mut state.lose_next_publication);
            let forbid = std::mem::take(&mut state.forbid_next_publication);
            let Some(definition) = state.definitions.get_mut(id) else {
                return Err(gateway_not_found());
            };
            if forbid {
                return Err(super::problem_http_response(
                    "403 Forbidden",
                    json!({"type": format!("{PROBLEMS}forbidden"), "title": "Forbidden", "status": 403}),
                ));
            }
            if lose {
                definition.current += 1;
            }
            if body["expectedCurrentRevision"] != json!(definition.current) {
                return Err(super::problem_http_response(
                    "409 Conflict",
                    json!({
                        "type": format!("{PROBLEMS}definition-revision-conflict"),
                        "title": "Definition revision conflict",
                        "status": 409,
                        "workflow": {"currentRevision": definition.current},
                    }),
                ));
            }
            definition.current += 1;
            Ok(gateway_publication(id, definition, &body["treeDigest"]))
        });
    }
    gateway_not_found()
}

fn gateway_tree(digest: &str, sealed: bool) -> Value {
    json!({
        "digest": digest,
        "state": if sealed { "sealed" } else { "uploading" },
        "sizeBytes": 512,
        "fileCount": 4,
        "name": if sealed { json!("engineering") } else { Value::Null },
        "createdAt": CLOUD_TIME,
        "sealedAt": if sealed { json!(CLOUD_TIME) } else { Value::Null },
    })
}

fn gateway_definition(id: &str, definition: &FakeDefinition) -> Value {
    json!({
        "id": id,
        "projectId": CLOUD_PROJECT,
        "name": "engineering",
        "state": if definition.archived { "archived" } else { "active" },
        "automaticStarts": definition.automatic_starts,
        "executionPrincipalId": definition.principal,
        "currentRevision": definition.current,
        "createdAt": CLOUD_TIME,
        "updatedAt": CLOUD_TIME,
    })
}

const CLOUD_EVALUATION: &str = "wev_01k0z6r1w8f4jy2m7q9v3x5abc";

/// A failed start evaluation `evaluation` of definition `id` with its stored reason.
fn gateway_evaluation(id: &str, evaluation: &str) -> Value {
    json!({
        "id": evaluation,
        "definitionId": id,
        "revision": 2,
        "triggerName": "start-on-todo",
        "connectionId": "lnc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "executionPrincipalId": CLOUD_PRINCIPAL,
        "eventName": "issue.state_entered",
        "event": {
            "issue": {"id": "issue-7", "identifier": "AUTO-7"},
            "issueUpdatedAt": CLOUD_TIME,
            "team": CLOUD_TEAM_ID,
            "state": "state-todo",
            "actor": null,
            "at": CLOUD_TIME,
        },
        "state": "failed",
        "reason": "owner_unresolved",
        "observations": null,
        "runId": null,
        "blockingRunId": null,
        "cycleNumber": 1,
        "attemptNumber": 2,
        "cycleStartedAt": CLOUD_TIME,
        "cycleDeadline": "2026-11-11T12:00:00Z",
        "nextAttemptAt": null,
        "acceptedAt": CLOUD_TIME,
        "expiresAt": "2026-12-10T12:00:00Z",
        "settledAt": CLOUD_TIME,
    })
}

fn gateway_publication(id: &str, definition: &FakeDefinition, tree_digest: &Value) -> Value {
    json!({
        "definition": gateway_definition(id, definition),
        "revision": {
            "definitionId": id,
            "number": definition.current,
            "treeDigest": tree_digest,
            "bindings": {},
            "resolution": {},
            "publishedAt": CLOUD_TIME,
            "publishedBy": CLOUD_PUBLISHER,
        },
    })
}

fn cloud_publish_args<'a>(tree: &'a Tree, bindings: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec![
        "workflow",
        "definition",
        "publish",
        tree.path(),
        "--bindings",
        bindings,
        "--cloud",
        CLOUD_ORGANIZATION,
        "--project",
        CLOUD_PROJECT,
        "--allow-insecure-http",
    ];
    args.extend_from_slice(extra);
    args
}

fn routes(requests: &[GatewayRequest]) -> Vec<String> {
    requests.iter().map(GatewayRequest::route).collect()
}

// REQ-CLOUDDEF-14, REQ-WFDEF-7, REQ-WFDEF-23
#[test]
fn publish_cloud_creates_then_publishes_to_the_recorded_cloud_definition() {
    use std::os::unix::fs::PermissionsExt as _;
    let home = Home::new();
    let tree = Tree::engineering();
    fs::set_permissions(
        Path::new(tree.path()).join("scripts/design.py"),
        fs::Permissions::from_mode(0o755),
    )
    .expect("script should become executable");
    let bindings = tree.engineering_bindings();
    let gateway = FakeGateway::new();
    let organization = format!("/api/v1/organizations/{CLOUD_ORGANIZATION}");
    let definitions = format!("{organization}/projects/{CLOUD_PROJECT}/workflow-definitions");

    let (output, requests) = gateway.run(&home, &cloud_publish_args(&tree, &bindings, &["--json"]));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_no_content(&output);
    super::assert_no_secret_output(&output, &[CLOUD_TOKEN]);
    let first = report(&output);
    assert_eq!(first["outcome"], "published", "{first}");
    let id = text(&first["definition"]["id"], "definition id").to_owned();
    let digest = text(&first["revision"]["tree"], "tree digest").to_owned();
    assert_eq!(first["revision"]["n"], 1);
    let cloud_id = text(&first["cloud"]["definition"], "cloud definition id").to_owned();
    assert!(
        cloud_id.starts_with(CLOUD_ID_STEM) && is_definition_id(&cloud_id),
        "{first}"
    );
    assert_eq!(first["cloud"]["created"], true);
    assert_eq!(first["cloud"]["revision"], 1);
    assert_eq!(first["cloud"]["treeDigest"], digest.as_str());
    let record_path = home.definition_directory(&id).join("definition.json");
    let record = read_json(&record_path);
    assert_eq!(record["cloudId"], cloud_id.as_str());
    assert!(
        schema("workflow-definition-v1.schema.json").is_valid(&record),
        "{record}"
    );

    let seal = format!(
        "POST {organization}/workflow-trees/{}/seal",
        digest.replace(':', "%3A")
    );
    let stored = |path: &str| {
        let bytes = fs::read(Path::new(tree.path()).join(path)).unwrap();
        let sha256 = ring::digest::digest(&ring::digest::SHA256, &bytes)
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        (bytes, sha256)
    };
    let (script, script_sha) = stored("scripts/design.py");
    let (policy, policy_sha) = stored("policy/engineering.json");
    assert_eq!(
        routes(&requests),
        vec![
            format!("POST {organization}/workflow-trees"),
            format!("PUT /storage/{policy_sha}"),
            format!("PUT /storage/{script_sha}"),
            seal,
            format!("POST {definitions}"),
        ]
    );
    let manifest = requests[0].json();
    assert_eq!(manifest["digest"], digest.as_str());
    let files = manifest["files"].as_array().expect("manifest files");
    assert_eq!(
        files
            .iter()
            .map(|file| (text(&file["path"], "path"), file["executable"].as_bool()))
            .collect::<Vec<_>>(),
        vec![
            ("design.yaml", Some(false)),
            ("policy/engineering.json", Some(false)),
            ("scripts/design.py", Some(true)),
            ("workflow.yaml", Some(false)),
        ]
    );
    let script_entry = &files[2];
    assert_eq!(script_entry["sha256"], script_sha.as_str());
    assert_eq!(script_entry["sizeBytes"], script.len());
    assert_eq!(requests[1].body, policy);
    assert_eq!(requests[2].body, script);
    assert_eq!(requests[1].header("if-none-match"), Some("*"));
    let key = requests[3]
        .header("idempotency-key")
        .expect("seal key")
        .to_owned();
    assert_eq!(requests[4].header("idempotency-key"), Some(key.as_str()));
    for request in [&requests[0], &requests[3], &requests[4]] {
        assert_eq!(
            request.header("authorization"),
            Some(format!("Bearer {CLOUD_TOKEN}").as_str())
        );
    }
    let created = requests[4].json();
    assert_eq!(created["treeDigest"], digest.as_str());
    assert_eq!(created["bindings"]["linearTeam"], "AUTO");
    assert_eq!(created["bindings"]["ownedState"], "Engineering");

    // A second publish reads the recorded Cloud definition's revision and publishes to it.
    let (output, requests) = gateway.run(&home, &cloud_publish_args(&tree, &bindings, &["--json"]));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let second = report(&output);
    assert_eq!(second["revision"]["n"], 2);
    assert_eq!(second["cloud"]["definition"], cloud_id.as_str());
    assert_eq!(second["cloud"]["created"], false);
    assert_eq!(second["cloud"]["revision"], 2);
    assert_eq!(
        routes(&requests),
        vec![
            format!("POST {organization}/workflow-trees"),
            format!("GET {definitions}/{cloud_id}"),
            format!("POST {definitions}/{cloud_id}/revisions"),
        ]
    );
    assert_eq!(requests[2].json()["expectedCurrentRevision"], 1);
    assert_eq!(requests[2].json()["treeDigest"], digest.as_str());
    assert_ne!(requests[2].header("idempotency-key"), Some(key.as_str()));
    assert_eq!(read_json(&record_path)["cloudId"], cloud_id.as_str());

    // A refusal leaves the local revision recorded and names the problem type.
    gateway.lose_next_publication();
    let (output, requests) = gateway.run(&home, &cloud_publish_args(&tree, &bindings, &[]));
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(requests.len(), 3);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("definition-revision-conflict"), "{stderr}");
    let record = read_json(&record_path);
    assert_eq!(record["current"], 3);
    assert_eq!(record["cloudId"], cloud_id.as_str());

    // `--cloud-new` creates another Cloud definition and records it.
    let (output, requests) = gateway.run(
        &home,
        &cloud_publish_args(&tree, &bindings, &["--cloud-new", "--json"]),
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let fourth = report(&output);
    let other = text(&fourth["cloud"]["definition"], "cloud definition id").to_owned();
    assert_ne!(other, cloud_id);
    assert_eq!(fourth["cloud"]["created"], true);
    assert_eq!(fourth["cloud"]["revision"], 1);
    assert_eq!(fourth["revision"]["n"], 4);
    assert_eq!(
        routes(&requests),
        vec![
            format!("POST {organization}/workflow-trees"),
            format!("POST {definitions}"),
        ]
    );
    assert_eq!(read_json(&record_path)["cloudId"], other.as_str());
    assert_eq!(home.workflow_entries(), vec![id]);
}

// REQ-CLOUDDEF-14
#[test]
fn publish_cloud_resends_a_lost_publication_once_and_publishes_executable_bits() {
    use std::os::unix::fs::PermissionsExt as _;
    let home = Home::new();
    let tree = Tree::engineering();
    let script = Path::new(tree.path()).join("scripts/design.py");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755))
        .expect("script should become executable");
    let bindings = tree.engineering_bindings();
    let gateway = FakeGateway::new();
    let definitions = format!(
        "/api/v1/organizations/{CLOUD_ORGANIZATION}/projects/{CLOUD_PROJECT}/workflow-definitions"
    );

    // The create's reply is lost: it is sent once more under the same key and creates one
    // definition, recorded as the local definition's `cloudId`.
    gateway.drop_next_publication_reply();
    let (output, requests) = gateway.run(&home, &cloud_publish_args(&tree, &bindings, &["--json"]));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let first = report(&output);
    assert_eq!(first["outcome"], "published", "{first}");
    let cloud_id = text(&first["cloud"]["definition"], "cloud definition id").to_owned();
    assert_eq!(first["cloud"]["created"], true);
    let digest = text(&first["revision"]["tree"], "tree digest").to_owned();
    let creates = requests
        .iter()
        .filter(|request| request.route() == format!("POST {definitions}"))
        .collect::<Vec<_>>();
    assert_eq!(creates.len(), 2, "{:?}", routes(&requests));
    let key = creates[0].header("idempotency-key").expect("create key");
    assert_eq!(creates[1].header("idempotency-key"), Some(key));
    assert_eq!(creates[0].body, creates[1].body);
    assert_eq!(
        creates[0].json()["executablePaths"],
        json!(["scripts/design.py"])
    );
    assert_eq!(
        gateway
            .state
            .lock()
            .unwrap()
            .definitions
            .keys()
            .collect::<Vec<_>>(),
        vec![&cloud_id]
    );
    let id = text(&first["definition"]["id"], "definition id").to_owned();
    let record_path = home.definition_directory(&id).join("definition.json");
    assert_eq!(read_json(&record_path)["cloudId"], cloud_id.as_str());

    // Clearing the last executable bit leaves the digest and publishes a new revision that
    // runs no file executable.
    fs::set_permissions(&script, fs::Permissions::from_mode(0o644))
        .expect("script should stop being executable");
    let (output, requests) = gateway.run(&home, &cloud_publish_args(&tree, &bindings, &["--json"]));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let second = report(&output);
    assert_eq!(second["revision"]["tree"], digest.as_str());
    assert_eq!(second["cloud"]["definition"], cloud_id.as_str());
    assert_eq!(second["cloud"]["revision"], 2);
    let published = requests
        .iter()
        .find(|request| request.route() == format!("POST {definitions}/{cloud_id}/revisions"))
        .expect("publication request")
        .json();
    assert_eq!(published["treeDigest"], digest.as_str());
    assert_eq!(published["executablePaths"], json!([]));
}

// REQ-CLOUDDEF-14, REQ-WFDEF-19
#[test]
fn publish_cloud_flags_without_their_companions_are_usage_errors_that_write_nothing() {
    let home = Home::new();
    let tree = Tree::engineering();
    for args in [
        vec!["publish", tree.path(), "--cloud", CLOUD_ORGANIZATION],
        vec!["publish", tree.path(), "--project", CLOUD_PROJECT],
        vec!["publish", tree.path(), "--cloud-new"],
        vec!["publish", tree.path(), "--cloud-to", CLOUD_TARGET],
        vec![
            "publish",
            tree.path(),
            "--cloud",
            CLOUD_ORGANIZATION,
            "--project",
            CLOUD_PROJECT,
            "--cloud-to",
            CLOUD_TARGET,
            "--cloud-new",
        ],
    ] {
        let output = home.run(&args);
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty());
    }
    assert!(home.workflow_entries().is_empty());
}

/// A Cloud definition id of the definition id shape that no fake Gateway mints.
const CLOUD_TARGET: &str = "def_01k0z6r1w8f4jy2m7q9v3x5abz";

/// The fake's definitions, each with its current revision.
fn gateway_definitions(gateway: &FakeGateway) -> Vec<(String, i64)> {
    gateway
        .state
        .lock()
        .unwrap()
        .definitions
        .iter()
        .map(|(id, definition)| (id.clone(), definition.current))
        .collect()
}

/// The record `report` published under `home`.
fn published_record(home: &Home, report: &Value) -> Value {
    let id = text(&report["definition"]["id"], "definition id");
    read_json(&home.definition_directory(id).join("definition.json"))
}

// REQ-CLOUDDEF-20, REQ-CLOUDDEF-14, REQ-WFDEF-19
#[test]
fn publish_cloud_to_publishes_a_fresh_home_s_revision_to_the_named_cloud_definition() {
    let (first_home, second_home) = (Home::new(), Home::new());
    let tree = Tree::engineering();
    let bindings = tree.engineering_bindings();
    let gateway = FakeGateway::new();
    let organization = format!("/api/v1/organizations/{CLOUD_ORGANIZATION}");
    let definitions = format!("{organization}/projects/{CLOUD_PROJECT}/workflow-definitions");

    // The first home creates the Cloud definition; a create names no target.
    let (output, _) = gateway.run(
        &first_home,
        &cloud_publish_args(&tree, &bindings, &["--json"]),
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let created = report(&output);
    let cloud_id = text(&created["cloud"]["definition"], "cloud definition id").to_owned();
    assert_eq!(created["cloud"]["created"], true);
    assert!(created["cloud"].get("target").is_none(), "{created}");

    // The second home, which has never published, finds the id with `cloud list`.
    let list = [
        "workflow",
        "definition",
        "cloud",
        "list",
        CLOUD_ORGANIZATION,
        CLOUD_PROJECT,
        "--json",
        "--allow-insecure-http",
    ];
    let (output, _) = gateway.run(&second_home, &list);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let listed = report(&output);
    assert_eq!(
        listed["definitions"][0]["id"],
        cloud_id.as_str(),
        "{listed}"
    );
    assert!(second_home.workflow_entries().is_empty());

    let (output, requests) = gateway.run(
        &second_home,
        &cloud_publish_args(&tree, &bindings, &["--cloud-to", &cloud_id, "--json"]),
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_no_content(&output);
    super::assert_no_secret_output(&output, &[CLOUD_TOKEN]);
    let targeted = report(&output);
    assert_eq!(targeted["outcome"], "published", "{targeted}");
    assert_eq!(targeted["definition"]["created"], true);
    assert_eq!(targeted["revision"]["n"], 1);
    assert_eq!(targeted["cloud"]["created"], false);
    assert_eq!(targeted["cloud"]["definition"], cloud_id.as_str());
    assert_eq!(targeted["cloud"]["target"], cloud_id.as_str());
    assert_eq!(targeted["cloud"]["revision"], 2);
    assert_eq!(
        routes(&requests),
        vec![
            format!("POST {organization}/workflow-trees"),
            format!("GET {definitions}/{cloud_id}"),
            format!("POST {definitions}/{cloud_id}/revisions"),
        ]
    );
    assert_eq!(requests[2].json()["expectedCurrentRevision"], 1);
    let record = published_record(&second_home, &targeted);
    assert_eq!(record["cloudId"], cloud_id.as_str());
    assert!(
        schema("workflow-definition-v1.schema.json").is_valid(&record),
        "{record}"
    );
    let (output, _) = gateway.run(&second_home, &list);
    assert_eq!(
        report(&output)["definitions"].as_array().map(Vec::len),
        Some(1)
    );

    // The recorded id is the target of the next publication from that home.
    let (output, requests) = gateway.run(
        &second_home,
        &cloud_publish_args(&tree, &bindings, &["--json"]),
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let recorded = report(&output);
    assert_eq!(recorded["definition"]["created"], false);
    assert_eq!(recorded["cloud"]["created"], false);
    assert_eq!(recorded["cloud"]["definition"], cloud_id.as_str());
    assert_eq!(recorded["cloud"]["target"], cloud_id.as_str());
    assert_eq!(recorded["cloud"]["revision"], 3);
    assert!(
        !routes(&requests).contains(&format!("POST {definitions}")),
        "{:?}",
        routes(&requests)
    );

    // A changed root name resolves a new local record and still updates the named definition.
    let renamed = Tree::with(
        &ROOT_WORKFLOW.replacen("name: engineering\n", "name: alpha\n", 1),
        DESIGN_WORKFLOW,
    );
    let renamed_bindings = renamed.engineering_bindings();
    let (output, requests) = gateway.run(
        &second_home,
        &cloud_publish_args(
            &renamed,
            &renamed_bindings,
            &["--cloud-to", &cloud_id, "--json"],
        ),
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let moved = report(&output);
    assert_eq!(moved["definition"]["created"], true);
    assert_eq!(moved["cloud"]["definition"], cloud_id.as_str());
    assert_eq!(moved["cloud"]["revision"], 4);
    // The new tree is uploaded and sealed, then published to the named id.
    let moved_routes = routes(&requests);
    assert!(
        moved_routes.ends_with(&[
            format!("GET {definitions}/{cloud_id}"),
            format!("POST {definitions}/{cloud_id}/revisions"),
        ]),
        "{moved_routes:?}"
    );
    assert!(
        !moved_routes.contains(&format!("POST {definitions}")),
        "{moved_routes:?}"
    );
    assert_eq!(second_home.workflow_entries().len(), 2);
    assert_eq!(
        published_record(&second_home, &moved)["cloudId"],
        cloud_id.as_str()
    );
    assert_eq!(gateway_definitions(&gateway), vec![(cloud_id, 4)]);
}

/// No request created a Cloud definition, in the addressed project or any other.
fn assert_no_definition_created(requests: &[GatewayRequest]) {
    let routes = routes(requests);
    assert!(
        !routes
            .iter()
            .any(|route| route.starts_with("POST ") && route.ends_with("/workflow-definitions")),
        "{routes:?}"
    );
}

// REQ-CLOUDDEF-20: a named target is published to whatever Cloud id the local record
// holds and replaces it once Cloud accepts; a refused target leaves the recorded id as it
// was and creates nothing.
#[test]
fn publish_cloud_to_replaces_the_recorded_cloud_definition_only_once_cloud_accepts() {
    let home = Home::new();
    let tree = Tree::engineering();
    let bindings = tree.engineering_bindings();
    let gateway = FakeGateway::new();
    let definitions = format!(
        "/api/v1/organizations/{CLOUD_ORGANIZATION}/projects/{CLOUD_PROJECT}/workflow-definitions"
    );

    // The home creates one Cloud definition and records its id.
    let (output, _) = gateway.run(&home, &cloud_publish_args(&tree, &bindings, &["--json"]));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let created = report(&output);
    let recorded = text(&created["cloud"]["definition"], "cloud definition id").to_owned();
    assert_eq!(
        published_record(&home, &created)["cloudId"],
        recorded.as_str()
    );

    // Naming another Cloud definition publishes there, not to the recorded one.
    let named = CLOUD_TARGET;
    assert_ne!(named, recorded);
    gateway.define(named, 5);
    let (output, requests) = gateway.run(
        &home,
        &cloud_publish_args(&tree, &bindings, &["--cloud-to", named, "--json"]),
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let moved = report(&output);
    assert_eq!(moved["cloud"]["created"], false, "{moved}");
    assert_eq!(moved["cloud"]["definition"], named);
    assert_eq!(moved["cloud"]["revision"], 6);
    let moved_routes = routes(&requests);
    assert!(
        moved_routes.ends_with(&[
            format!("GET {definitions}/{named}"),
            format!("POST {definitions}/{named}/revisions"),
        ]),
        "{moved_routes:?}"
    );
    assert!(
        !moved_routes.iter().any(|route| route.contains(&recorded)),
        "{moved_routes:?}"
    );
    assert_no_definition_created(&requests);
    assert_eq!(published_record(&home, &moved)["cloudId"], named);
    assert_eq!(
        gateway_definitions(&gateway),
        vec![(recorded.clone(), 1), (named.to_owned(), 6)]
    );

    // A target Cloud refuses leaves the record naming the definition it last published to.
    let absent = format!("{CLOUD_ID_STEM}y");
    let (output, requests) = gateway.run(
        &home,
        &cloud_publish_args(&tree, &bindings, &["--cloud-to", &absent, "--json"]),
    );
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let refused = report(&output);
    assert_eq!(refused["cloud"]["failure"], "refused", "{refused}");
    assert_eq!(refused["cloud"]["target"], absent.as_str());
    assert_no_definition_created(&requests);
    assert_eq!(published_record(&home, &refused)["cloudId"], named);
    assert_eq!(
        gateway_definitions(&gateway),
        vec![(recorded, 1), (named.to_owned(), 6)]
    );
}

// REQ-CLOUDDEF-20, REQ-CLOUDDEF-14, REQ-WFDEF-27
#[test]
fn publish_cloud_to_refusals_create_nothing_and_leave_the_record_unlinked() {
    let tree = Tree::engineering();
    let bindings = tree.engineering_bindings();
    let gateway = FakeGateway::new();
    let cloud_id = format!("{CLOUD_ID_STEM}c");
    gateway.define(&cloud_id, 2);
    // Publishes from a fresh home with `args`, asserting a Cloud refusal of `target` with
    // `status` and `problem`, no create in any project, and a local record still without a
    // `cloudId`.
    let refused = |args: &[&str], target: &str, status: u16, problem: &str| {
        let home = Home::new();
        let (output, requests) = gateway.run(&home, args);
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        let failed = report(&output);
        assert_eq!(failed["outcome"], "cloud_failed", "{failed}");
        assert_eq!(failed["cloud"]["failure"], "refused");
        assert_eq!(failed["cloud"]["status"], status);
        assert_eq!(failed["cloud"]["problem"], problem);
        assert_eq!(failed["cloud"]["target"], target);
        assert!(failed["cloud"].get("definition").is_none(), "{failed}");
        assert_no_definition_created(&requests);
        assert_eq!(published_record(&home, &failed)["cloudId"], Value::Null);
        failed
    };

    refused(
        &cloud_publish_args(&tree, &bindings, &["--cloud-to", CLOUD_TARGET, "--json"]),
        CLOUD_TARGET,
        404,
        "not-found",
    );
    assert_eq!(gateway_definitions(&gateway), vec![(cloud_id.clone(), 2)]);

    let other_project = [
        "workflow",
        "definition",
        "publish",
        tree.path(),
        "--bindings",
        &bindings,
        "--cloud",
        CLOUD_ORGANIZATION,
        "--project",
        "prj_01k0z6r1w8f4jy2m7q9v3x5xyz",
        "--cloud-to",
        &cloud_id,
        "--allow-insecure-http",
        "--json",
    ];
    refused(&other_project, &cloud_id, 404, "not-found");
    assert_eq!(gateway_definitions(&gateway), vec![(cloud_id.clone(), 2)]);

    gateway.forbid_next_publication();
    refused(
        &cloud_publish_args(&tree, &bindings, &["--cloud-to", &cloud_id, "--json"]),
        &cloud_id,
        403,
        "forbidden",
    );
    assert_eq!(gateway_definitions(&gateway), vec![(cloud_id.clone(), 2)]);

    // Another publisher's revision 3 lands first: the conflict carries it.
    gateway.lose_next_publication();
    let conflict = refused(
        &cloud_publish_args(&tree, &bindings, &["--cloud-to", &cloud_id, "--json"]),
        &cloud_id,
        409,
        "definition-revision-conflict",
    );
    assert_eq!(conflict["cloud"]["workflow"]["currentRevision"], 3);
    assert_eq!(gateway_definitions(&gateway), vec![(cloud_id.clone(), 3)]);

    // A target not of the definition id shape is refused before anything is written or sent.
    let home = Home::new();
    let (output, requests) = gateway.run(
        &home,
        &cloud_publish_args(
            &tree,
            &bindings,
            &["--cloud-to", "def_not-a-cloud-id", "--json"],
        ),
    );
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let invalid = report(&output);
    assert_eq!(invalid["outcome"], "refused", "{invalid}");
    assert_eq!(invalid["diagnostics"][0]["code"], "definition_id_invalid");
    assert!(requests.is_empty(), "{:?}", routes(&requests));
    assert!(home.workflow_entries().is_empty());
    assert_eq!(gateway_definitions(&gateway), vec![(cloud_id, 3)]);
}

const CLOUD_PRINCIPAL: &str = "prn_01k0z6r1w8f4jy2m7q9v3x5xyz";
const CLOUD_TEAM_ID: &str = "8f2c4c9e-team-automation";

/// The tree digest the fake records for revision `number`.
fn gateway_tree_digest(number: i64) -> String {
    format!("sha256:{number:0>64}")
}

/// Revision `number` of `id`, with one Linear team binding and its resolution.
fn gateway_revision(id: &str, number: i64) -> Value {
    json!({
        "definitionId": id,
        "number": number,
        "treeDigest": gateway_tree_digest(number),
        "bindings": {"linearTeam": {"kind": "linear.team", "value": "AUTO"}},
        "resolution": {"linearTeam": {
            "kind": "linear.team", "value": "AUTO", "id": CLOUD_TEAM_ID, "name": "Automation",
        }},
        "publishedAt": CLOUD_TIME,
        "publishedBy": CLOUD_PUBLISHER,
    })
}

/// The definition list, the revision reads and the setting changes; an enable without an
/// execution principal is refused 409 `definition-not-startable`.
fn respond_settings(
    state: &mut GatewayState,
    request: &GatewayRequest,
    definitions: &str,
    keyed: impl Fn(&str, Value) -> Vec<u8>,
) -> Option<Vec<u8>> {
    let route = request.route();
    if route == format!("GET {definitions}") {
        let items = state
            .definitions
            .iter()
            .map(|(id, definition)| gateway_definition(id, definition))
            .collect::<Vec<_>>();
        return Some(super::json_http_response("200 OK", json!({"items": items})));
    }
    let (method, rest) = route.split_once(' ')?;
    let (id, action) = rest
        .strip_prefix(definitions)?
        .strip_prefix('/')?
        .split_once('/')?;
    let definition = state.definitions.get_mut(id)?;
    match (method, action) {
        ("GET", "revisions") => {
            let items = (1..=definition.current)
                .rev()
                .map(|number| gateway_revision(id, number))
                .collect::<Vec<_>>();
            return Some(super::json_http_response("200 OK", json!({"items": items})));
        }
        ("GET", action) => {
            let number = action.strip_prefix("revisions/")?.parse().ok()?;
            return Some(super::json_http_response(
                "200 OK",
                gateway_revision(id, number),
            ));
        }
        ("POST", "enable") if definition.principal.is_none() => {
            return Some(super::problem_http_response(
                "409 Conflict",
                json!({
                    "type": format!("{PROBLEMS}definition-not-startable"),
                    "title": "Definition not startable",
                    "status": 409,
                    "workflow": {"startCheck": "execution-principal"},
                }),
            ));
        }
        ("POST", "enable") => definition.automatic_starts = true,
        ("POST", "disable") => definition.automatic_starts = false,
        ("POST", "archive") => {
            definition.archived = true;
            definition.automatic_starts = false;
        }
        ("POST", "unarchive") => definition.archived = false,
        ("PUT", "execution-principal") => {
            definition.principal =
                Some(text(&request.json()["principalId"], "principal id").to_owned());
        }
        _ => return None,
    }
    Some(keyed("200 OK", gateway_definition(id, definition)))
}

// REQ-CLOUDDEF-15
#[test]
fn cloud_group_lists_shows_and_administers_cloud_definitions() {
    let home = Home::new();
    let gateway = FakeGateway::new();
    let first = format!("{CLOUD_ID_STEM}c");
    let second = format!("{CLOUD_ID_STEM}d");
    gateway.define(&second, 1);
    gateway.define(&first, 2);
    let definitions = format!(
        "/api/v1/organizations/{CLOUD_ORGANIZATION}/projects/{CLOUD_PROJECT}/workflow-definitions"
    );
    let cloud = |args: &[&str]| {
        let mut all = vec!["workflow", "definition", "cloud"];
        all.extend_from_slice(args);
        all.push("--allow-insecure-http");
        let (output, requests) = gateway.run(&home, &all);
        super::assert_no_secret_output(&output, &[CLOUD_TOKEN]);
        for request in &requests {
            assert_eq!(
                request.header("authorization"),
                Some(format!("Bearer {CLOUD_TOKEN}").as_str())
            );
        }
        (output, requests)
    };
    let target = |leaf: &'static str, json: bool| {
        let mut args = vec![
            leaf,
            CLOUD_ORGANIZATION,
            first.as_str(),
            "--project-id",
            CLOUD_PROJECT,
        ];
        if json {
            args.push("--json");
        }
        args
    };

    let (output, requests) = cloud(&["list", CLOUD_ORGANIZATION, CLOUD_PROJECT, "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let listed = report(&output);
    assert_eq!(listed["outcome"], "listed", "{listed}");
    assert_eq!(listed["organizationRef"], CLOUD_ORGANIZATION);
    assert_eq!(listed["projectId"], CLOUD_PROJECT);
    let items = listed["definitions"].as_array().expect("definitions");
    assert_eq!(
        items
            .iter()
            .map(|item| (text(&item["id"], "id"), item["currentRevision"].as_i64()))
            .collect::<Vec<_>>(),
        vec![(first.as_str(), Some(2)), (second.as_str(), Some(1))]
    );
    assert_eq!(routes(&requests), vec![format!("GET {definitions}")]);

    let (output, requests) = cloud(&target("show", false));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_contains_all(
        &stdout,
        &[
            &first,
            &gateway_tree_digest(2),
            &gateway_tree_digest(1),
            "linearTeam",
            CLOUD_TEAM_ID,
            "Automation",
            CLOUD_PUBLISHER,
        ],
    );
    assert_eq!(
        routes(&requests),
        vec![
            format!("GET {definitions}/{first}"),
            format!("GET {definitions}/{first}/revisions/2"),
            format!("GET {definitions}/{first}/revisions"),
        ]
    );
    let (output, _) = cloud(&target("show", true));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let shown = report(&output);
    assert_eq!(shown["outcome"], "shown", "{shown}");
    assert_eq!(shown["definition"]["id"], first.as_str());
    assert_eq!(shown["revision"]["number"], 2);
    assert_eq!(shown["revision"]["treeDigest"], gateway_tree_digest(2));
    assert_eq!(
        shown["revision"]["resolution"]["linearTeam"]["id"],
        CLOUD_TEAM_ID
    );
    assert_eq!(
        shown["revisions"]
            .as_array()
            .expect("revisions")
            .iter()
            .map(|revision| revision["number"].as_i64())
            .collect::<Vec<_>>(),
        vec![Some(2), Some(1)]
    );

    // Enabling without an execution principal is refused with the problem type.
    let (output, requests) = cloud(&target("enable", false));
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_contains_all(
        &stderr,
        &["definition-not-startable", "execution-principal"],
    );
    assert_eq!(
        routes(&requests),
        vec![format!("POST {definitions}/{first}/enable")]
    );
    assert!(requests[0].header("idempotency-key").is_some());
    let (output, _) = cloud(&target("enable", true));
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let refused = report(&output);
    assert_eq!(refused["outcome"], "refused", "{refused}");
    assert_eq!(refused["status"], 409);
    assert_eq!(refused["problem"], "definition-not-startable");
    assert_eq!(refused["workflow"]["startCheck"], "execution-principal");

    let mut args = target("set-principal", true);
    args.insert(3, CLOUD_PRINCIPAL);
    let (output, requests) = cloud(&args);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let set = report(&output);
    assert_eq!(set["outcome"], "principal_set", "{set}");
    assert_eq!(set["definition"]["executionPrincipalId"], CLOUD_PRINCIPAL);
    assert_eq!(
        routes(&requests),
        vec![format!("PUT {definitions}/{first}/execution-principal")]
    );
    assert_eq!(requests[0].json(), json!({"principalId": CLOUD_PRINCIPAL}));
    assert!(requests[0].header("idempotency-key").is_some());

    let (output, requests) = cloud(&target("enable", true));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let enabled = report(&output);
    assert_eq!(enabled["outcome"], "enabled", "{enabled}");
    assert_eq!(enabled["definition"]["automaticStarts"], true);
    assert_eq!(
        routes(&requests),
        vec![format!("POST {definitions}/{first}/enable")]
    );

    for (leaf, state, automatic_starts) in [
        ("disable", "active", false),
        ("archive", "archived", false),
        ("unarchive", "active", false),
    ] {
        let (output, requests) = cloud(&target(leaf, true));
        assert_eq!(output.status.code(), Some(0), "{leaf}: {output:?}");
        let changed = report(&output);
        assert_eq!(changed["outcome"], format!("{leaf}d"), "{changed}");
        assert_eq!(changed["definition"]["state"], state);
        assert_eq!(changed["definition"]["automaticStarts"], automatic_starts);
        assert_eq!(
            routes(&requests),
            vec![format!("POST {definitions}/{first}/{leaf}")]
        );
    }
    assert!(home.workflow_entries().is_empty());
}

// REQ-CLOUDDEF-15: `evaluation list` sends its filters and page as typed and `evaluation
// show` reads one evaluation; both report each evaluation's state and stored reason.
#[test]
fn cloud_evaluations_are_listed_with_their_filters_and_shown_with_their_reason() {
    let home = Home::new();
    let gateway = FakeGateway::new();
    let definition = format!("{CLOUD_ID_STEM}c");
    gateway.define(&definition, 2);
    let evaluations = format!(
        "/api/v1/organizations/{CLOUD_ORGANIZATION}/projects/{CLOUD_PROJECT}/workflow-definitions/{definition}/evaluations"
    );
    let run_id = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
    let evaluation = |args: &[&str]| {
        let mut all = vec!["workflow", "definition", "cloud", "evaluation"];
        all.extend_from_slice(args);
        all.push("--allow-insecure-http");
        let (output, requests) = gateway.run(&home, &all);
        super::assert_no_secret_output(&output, &[CLOUD_TOKEN]);
        (output, requests)
    };
    let list = |extra: &[&str]| {
        let mut args = vec![
            "list",
            CLOUD_ORGANIZATION,
            definition.as_str(),
            "--project-id",
            CLOUD_PROJECT,
        ];
        args.extend_from_slice(extra);
        evaluation(&args)
    };

    let (output, requests) = list(&[
        "--state", "failed", "--run-id", run_id, "--limit", "5", "--cursor", "page-2", "--json",
    ]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(requests.len(), 1);
    let (path, query) = requests[0].path.split_once('?').expect("a query");
    assert_eq!(path, evaluations);
    let mut pairs = query.split('&').collect::<Vec<_>>();
    pairs.sort_unstable();
    assert_eq!(
        pairs,
        [
            "cursor=page-2",
            "limit=5",
            &format!("runId={run_id}"),
            "state=failed"
        ]
    );
    let listed = report(&output);
    assert_eq!(
        listed["command"],
        "um workflow definition cloud evaluation list"
    );
    assert_eq!(listed["outcome"], "listed", "{listed}");
    assert_eq!(listed["organizationRef"], CLOUD_ORGANIZATION);
    assert_eq!(listed["projectId"], CLOUD_PROJECT);
    assert_eq!(listed["definitionId"], definition.as_str());
    assert_eq!(listed["nextCursor"], "older-evaluations");
    let items = listed["evaluations"].as_array().expect("evaluations");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], gateway_evaluation(&definition, CLOUD_EVALUATION));

    let (output, requests) = list(&[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(requests[0].path, evaluations);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_contains_all(
        &stdout,
        &[
            CLOUD_EVALUATION,
            "failed",
            "owner_unresolved",
            "start-on-todo",
            "older-evaluations",
        ],
    );

    let show = |json: bool| {
        let mut args = vec![
            "show",
            CLOUD_ORGANIZATION,
            definition.as_str(),
            CLOUD_EVALUATION,
            "--project-id",
            CLOUD_PROJECT,
        ];
        if json {
            args.push("--json");
        }
        evaluation(&args)
    };
    let (output, requests) = show(true);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        routes(&requests),
        vec![format!("GET {evaluations}/{CLOUD_EVALUATION}")]
    );
    let shown = report(&output);
    assert_eq!(shown["outcome"], "shown", "{shown}");
    assert_eq!(
        shown["evaluation"],
        gateway_evaluation(&definition, CLOUD_EVALUATION)
    );
    let (output, _) = show(false);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_contains_all(
        &String::from_utf8_lossy(&output.stdout),
        &[
            CLOUD_EVALUATION,
            "failed",
            "owner_unresolved",
            "AUTO-7",
            CLOUD_PRINCIPAL,
        ],
    );
    assert!(home.workflow_entries().is_empty());
}
