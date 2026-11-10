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
            "start": {"kind": "start", "source": "$connection", "event": "issue.state_entered"},
            "release": {"kind": "control", "source": "$connection", "event": "issue.state_left"},
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
