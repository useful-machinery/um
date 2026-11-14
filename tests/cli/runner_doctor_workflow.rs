use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;

use serde_json::{Value, json};
use tempfile::TempDir;

use super::grant::{
    ENDED_RUNNER_ID, Git, IDENTITY_ID, LIST_FIRST_PAGE, NATIVE_GRANT_ID, OAUTH_GRANT_ID,
    ORGANIZATION_ID, REVOKED_OAUTH_GRANT_ID, RUNNER_ID, SENTINEL, native_grant_in_use, oauth_grant,
    page,
};
use super::{
    CREDENTIALS_FILE_VARIABLE, ScriptedServer, assert_no_secret_output,
    deployment_environment_with_issuer, fake_git, header_value, json_http_response,
    private_credential_directory, problem_http_response, run, run_with_env_from,
    write_credential_fixture_for_deployment,
};

// An edge-scheduled workflow holds only `workflow` steps (REQ-WFFORMAT-93), so the root
// file's agent and command nodes are finalizers.
const MAIN_WORKFLOW: &str = r#"schemaVersion: 1
environmentPassthrough: [LINEAR_API_KEY, MERGIFY_TOKEN, CARGO_TARGET_DIR, CLAUDE_CONFIG_DIR]
agentProfiles:
  reviewing:
    harness:
      kind: claude_code
      config:
        model: claude-opus-4-1
        effort: xhigh
steps:
  stage:
    kind: workflow
    workflow: stage.yaml
    on:
      succeeded: exit.succeeded
      failed: exit.failed
finalizers:
  review:
    kind: agent
    agent:
      profile: reviewing
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
  summarize:
    kind: agent
    after: [review]
    agent:
      profile: reviewing
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
  notify:
    kind: cmd
    command:
      argv: ["./notify"]
"#;

const STAGE_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: codex
      config:
        model: gpt-5.4
        effort: xhigh
  repairing:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  implement:
    kind: agent
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: repairing
        prompt: prompts/recovery.md
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
  check:
    kind: cmd
    dependsOn: [implement]
    command:
      argv: ["./check"]
finalizers:
  cleanup:
    kind: cmd
    command:
      argv: ["./cleanup"]
"#;

// A root workflow's blocks: required tools, bindings of every kind and a Linear mirror.
const ROOT_WORKFLOW: &str = r#"schemaVersion: 1
requires:
  tools: [git, gh, glab, python3]
bindings:
  connection: {kind: connection}
  linearTeam: {kind: linear.team}
  done: {kind: linear.state, default: Done}
  repository: {kind: github.repository}
mirror:
  linear:
    issue: inputs.ticket
    onOutcome:
      succeeded: {state: $done}
environmentPassthrough: [LINEAR_API_KEY]
inputs:
  ticket:
    kind: text
steps:
  build:
    kind: cmd
    command:
      argv: ["./build"]
"#;

// The scenario's root workflow: Linear and GitHub from its root blocks, two stage files.
const SCENARIO_ROOT_WORKFLOW: &str = r#"schemaVersion: 1
requires:
  tools: [git, gh]
bindings:
  linearTeam: {kind: linear.team}
  done: {kind: linear.state, default: Done}
  repository: {kind: github.repository}
mirror:
  linear:
    issue: inputs.ticket
    onOutcome:
      succeeded: {state: $done}
environmentPassthrough: [LINEAR_API_KEY, BUILDKITE_API_TOKEN]
inputs:
  ticket:
    kind: text
steps:
  design:
    kind: workflow
    workflow: design.yaml
    on:
      succeeded: build
      failed: exit.failed
  build:
    kind: workflow
    workflow: build.yaml
    on:
      succeeded: exit.succeeded
      failed: exit.failed
"#;

// A stage file of the scenario: a Claude Code agent step and a command step.
const SCENARIO_STAGE_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  writing:
    harness:
      kind: claude_code
      config:
        model: claude-opus-4-1
        effort: xhigh
steps:
  draft:
    kind: agent
    agent:
      profile: writing
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
  check:
    kind: cmd
    dependsOn: [draft]
    command:
      argv: ["./check"]
"#;

// An `origin` address carrying a credential, which no report may repeat.
const SECRET_ORIGIN: &str = "https://user:synthetic-secret@github.com/o/r.git";

struct WorkflowTree {
    _temporary: TempDir,
    root: PathBuf,
}

impl WorkflowTree {
    /// `main.yaml` running `stage.yaml`, with the prompts their agents read.
    fn new() -> Self {
        Self::with_files(&[
            ("main.yaml", MAIN_WORKFLOW),
            ("stage.yaml", STAGE_WORKFLOW),
            ("prompts/system.md", "System prompt."),
            ("prompts/message.md", "Message."),
            ("prompts/recovery.md", "Recovery prompt."),
        ])
    }

    /// A source root holding the given files by source-root-relative path.
    fn with_files(files: &[(&str, &str)]) -> Self {
        let temporary = tempfile::tempdir().expect("temporary source root should exist");
        let root = temporary.path().join("source");
        fs::create_dir_all(root.join("prompts")).expect("prompt directory should exist");
        for &(path, contents) in files {
            fs::write(root.join(path), contents).expect("workflow source should be written");
        }
        Self {
            _temporary: temporary,
            root,
        }
    }

    fn root_argument(&self) -> &str {
        self.root
            .to_str()
            .expect("temporary source root should be UTF-8")
    }

    fn run(&self, arguments: &[&str]) -> Output {
        run_in_source_root(&self.root, arguments)
    }
}

/// Runs `um` from the source root with the environment that proves no deployment or
/// credential file is loaded (REQ-RUNNERENROLL-45).
fn run_in_source_root(root: &Path, arguments: &[&str]) -> Output {
    run_with_env_from(
        arguments,
        &[
            ("UM_API_URL", "partial-override-is-ignored"),
            (CREDENTIALS_FILE_VARIABLE, "/dev/null/credentials.json"),
        ],
        Some(root),
    )
}

fn doctor(tree: &WorkflowTree, json: bool) -> Output {
    let mut arguments = vec![
        "runner",
        "doctor",
        "--workflow",
        "main.yaml",
        "--source-root",
        tree.root_argument(),
    ];
    if json {
        arguments.push("--json");
    }
    tree.run(&arguments)
}

fn step(id: &str) -> Value {
    json!({"kind": "step", "id": id})
}

fn finalizer(id: &str) -> Value {
    json!({"kind": "finalizer", "id": id})
}

fn recovery_handler(id: &str) -> Value {
    json!({"kind": "recovery_handler", "id": id})
}

// REQ-CREDREADY-1, REQ-CREDREADY-2, REQ-CREDREADY-3, REQ-CREDREADY-4, REQ-CREDREADY-5,
// REQ-CREDREADY-6, REQ-CREDREADY-7, REQ-CREDREADY-8, REQ-RUNNERENROLL-45
#[test]
fn runner_doctor_workflow_reports_tree_credentials_as_json() {
    let tree = WorkflowTree::new();

    let output = doctor(&tree, true);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("report should be UTF-8");
    assert!(!stdout.contains("CARGO_TARGET_DIR"));
    let report: Value = serde_json::from_str(&stdout).expect("report should be JSON");
    assert_eq!(report["schemaVersion"], 1);
    assert_eq!(report["command"], "um runner doctor");
    assert_eq!(report["deployment"], Value::Null);
    assert!(report.get("checks").is_none());
    assert_eq!(report["workflows"], json!(["main.yaml", "stage.yaml"]));

    let requirements = report["requirements"]
        .as_array()
        .expect("requirements should be an array");
    let kinds = requirements
        .iter()
        .map(|requirement| requirement["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(kinds, ["linear", "claude_code", "codex"]);
    for requirement in requirements {
        assert_eq!(requirement["state"], "unverified");
        let expected_code = if requirement["kind"] == "linear" {
            "not_checked"
        } else {
            "runner_unverified"
        };
        assert_eq!(requirement["code"], expected_code);
        assert!(
            !requirement["nextAction"]
                .as_str()
                .expect("next action should be text")
                .is_empty()
        );
    }
    assert_eq!(
        requirements[0]["origins"],
        json!([
            {"declaration": "forwarded_variable", "workflow": "main.yaml", "name": "LINEAR_API_KEY"},
        ])
    );
    assert_eq!(
        requirements[1]["origins"],
        json!([
            {"declaration": "agent_profile", "workflow": "main.yaml", "node": finalizer("review"), "name": "reviewing"},
            {"declaration": "agent_profile", "workflow": "main.yaml", "node": finalizer("summarize"), "name": "reviewing"},
            {"declaration": "forwarded_variable", "workflow": "main.yaml", "name": "CLAUDE_CONFIG_DIR"},
        ])
    );
    assert_eq!(
        requirements[2]["origins"],
        json!([
            {"declaration": "agent_profile", "workflow": "stage.yaml", "node": step("implement"), "name": "coding"},
        ])
    );

    assert_eq!(
        report["unknown"],
        json!([
            {"declaration": "command", "workflow": "main.yaml", "node": finalizer("notify")},
            {"declaration": "agent", "workflow": "main.yaml", "node": finalizer("review")},
            {"declaration": "agent", "workflow": "main.yaml", "node": finalizer("summarize")},
            {"declaration": "forwarded_variable", "workflow": "main.yaml", "name": "MERGIFY_TOKEN"},
            {"declaration": "command", "workflow": "stage.yaml", "node": step("check")},
            {"declaration": "command", "workflow": "stage.yaml", "node": finalizer("cleanup")},
            {"declaration": "agent", "workflow": "stage.yaml", "node": step("implement")},
            {"declaration": "agent", "workflow": "stage.yaml", "node": recovery_handler("implement")},
            {"declaration": "pi_harness", "workflow": "stage.yaml", "node": recovery_handler("implement")},
            {"declaration": "repository", "name": "origin"},
        ])
    );
    assert_eq!(
        report["summary"],
        json!({"missing": 0, "reconnectRequired": 0, "insufficientPermission": 0, "unverified": 3, "unknown": 10})
    );
}

// REQ-CREDREADY-7, REQ-CREDREADY-8
#[test]
fn runner_doctor_workflow_human_report_names_requirements_and_origins() {
    let tree = WorkflowTree::new();

    let output = doctor(&tree, false);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("report should be UTF-8");
    assert!(!stdout.contains("CARGO_TARGET_DIR"));
    let lines = stdout.lines().map(str::trim).collect::<Vec<_>>();
    for field in [
        "kind: linear",
        "kind: claude_code",
        "kind: codex",
        "state: unverified",
        "code: not_checked",
        "code: runner_unverified",
        "── summary ──",
        "unverified: 3",
        "unknown: 10",
    ] {
        assert!(lines.contains(&field), "{field} missing from:\n{stdout}");
    }
    assert!(!lines.contains(&"kind: github"));
    assert!(!lines.contains(&"kind: gitlab"));
    for (workflow, node) in [
        ("main.yaml", "finalizer review"),
        ("main.yaml", "finalizer summarize"),
        ("main.yaml", "CLAUDE_CONFIG_DIR"),
        ("main.yaml", "LINEAR_API_KEY"),
        ("stage.yaml", "step implement"),
    ] {
        assert!(
            lines.iter().any(|line| line.starts_with("needed by:")
                && line.contains(workflow)
                && line.contains(node)),
            "origin {workflow} {node} missing from:\n{stdout}"
        );
    }
    for entry in [
        "finalizer notify",
        "MERGIFY_TOKEN",
        "step check",
        "finalizer cleanup",
        "recovery handler implement",
    ] {
        assert!(
            stdout.contains(entry),
            "unknown {entry} missing from:\n{stdout}"
        );
    }
}

// REQ-CREDREADY-11, REQ-CREDREADY-3, REQ-CREDREADY-8
#[test]
fn runner_doctor_workflow_reads_root_bindings_mirror_and_tools() {
    let tree = WorkflowTree::with_files(&[("main.yaml", ROOT_WORKFLOW)]);

    let output = doctor(&tree, true);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let report: Value = serde_json::from_slice(&output.stdout).expect("report should be JSON");
    assert_eq!(report["workflows"], json!(["main.yaml"]));
    let requirements = report["requirements"]
        .as_array()
        .expect("requirements should be an array")
        .iter()
        .map(|requirement| (requirement["kind"].clone(), requirement["origins"].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        requirements,
        [
            (
                json!("github"),
                json!([
                    {"declaration": "binding", "workflow": "main.yaml", "name": "repository"},
                    {"declaration": "required_tool", "workflow": "main.yaml", "name": "gh"},
                ])
            ),
            (
                json!("gitlab"),
                json!([
                    {"declaration": "required_tool", "workflow": "main.yaml", "name": "glab"},
                ])
            ),
            (
                json!("linear"),
                json!([
                    {"declaration": "forwarded_variable", "workflow": "main.yaml", "name": "LINEAR_API_KEY"},
                    {"declaration": "binding", "workflow": "main.yaml", "name": "done"},
                    {"declaration": "binding", "workflow": "main.yaml", "name": "linearTeam"},
                    {"declaration": "mirror", "workflow": "main.yaml"},
                ])
            ),
        ]
    );
    assert_eq!(
        report["unknown"],
        json!([
            {"declaration": "command", "workflow": "main.yaml", "node": step("build")},
            {"declaration": "connection_binding", "workflow": "main.yaml", "name": "connection"},
            {"declaration": "repository", "name": "origin"},
        ])
    );
    assert_eq!(
        report["summary"],
        json!({"missing": 0, "reconnectRequired": 0, "insufficientPermission": 0, "unverified": 3, "unknown": 3})
    );

    let human = doctor(&tree, false);

    assert_eq!(human.status.code(), Some(0), "{human:?}");
    let stdout = String::from_utf8(human.stdout).expect("report should be UTF-8");
    let lines = stdout.lines().map(str::trim).collect::<Vec<_>>();
    for field in ["kind: github", "kind: gitlab", "kind: linear"] {
        assert!(lines.contains(&field), "{field} missing from:\n{stdout}");
    }
    for origin in ["repository", "gh", "glab", "done", "linearTeam", "mirror"] {
        assert!(
            lines.iter().any(|line| line.starts_with("needed by:")
                && line.contains("main.yaml")
                && line.split_whitespace().any(|word| word == origin)),
            "origin {origin} missing from:\n{stdout}"
        );
    }
    assert!(
        lines
            .iter()
            .any(|line| !line.starts_with("needed by:") && line.ends_with("connection")),
        "connection binding missing from:\n{stdout}"
    );
    for tool in ["git", "python3"] {
        assert!(
            !stdout.split_whitespace().any(|word| word == tool),
            "{tool} should not be reported:\n{stdout}"
        );
    }
}

// REQ-CREDREADY-2
#[test]
fn runner_doctor_workflow_rejects_a_child_that_does_not_resolve() {
    let tree = WorkflowTree::new();
    fs::write(
        tree.root.join("stage.yaml"),
        "schemaVersion: 1\nsteps:\n  broken:\n    kind: cmd\n",
    )
    .expect("broken child should be written");

    for json in [false, true] {
        let mut doctor_arguments = vec![
            "runner",
            "doctor",
            "--workflow",
            "main.yaml",
            "--source-root",
            tree.root_argument(),
        ];
        let mut validate_arguments = vec![
            "workflow",
            "validate",
            "--source-root",
            tree.root_argument(),
            "main.yaml",
        ];
        if json {
            doctor_arguments.push("--json");
            validate_arguments.push("--json");
        }

        let rejected = tree.run(&doctor_arguments);
        let validation = tree.run(&validate_arguments);

        assert_eq!(rejected.status.code(), Some(1), "{rejected:?}");
        assert_eq!(validation.status.code(), Some(1), "{validation:?}");
        assert!(!rejected.stdout.is_empty());
        assert_eq!(rejected.stdout, validation.stdout);
        assert_eq!(rejected.stderr, validation.stderr);
    }
}

// REQ-CREDREADY-1
#[test]
fn runner_doctor_workflow_refuses_conflicting_or_incomplete_options() {
    for arguments in [
        [
            "runner",
            "doctor",
            "--workflow",
            "main.yaml",
            "--source-root",
            ".",
            "--check",
            "environment.command.git",
        ]
        .as_slice(),
        [
            "runner",
            "doctor",
            "--workflow",
            "main.yaml",
            "--source-root",
            ".",
            "--list-checks",
        ]
        .as_slice(),
        ["runner", "doctor", "--source-root", "."].as_slice(),
    ] {
        let output = run(arguments);

        assert_eq!(output.status.code(), Some(2), "{arguments:?}");
        assert!(output.stdout.is_empty(), "{arguments:?}");
    }
}

// REQ-CREDREADY-1
#[test]
fn runner_doctor_workflow_bounds_its_files_by_their_git_repository() {
    let tree = WorkflowTree::new();
    fs::create_dir(tree.root.join(".git")).expect("Git marker should be created");
    let git = Git::new();
    let doctor = |options: &[&str]| {
        let mut arguments = vec!["runner", "doctor", "--workflow", "main.yaml"];
        arguments.extend_from_slice(options);
        arguments.push("--json");
        run_with_env_from(
            &arguments,
            &[
                ("UM_API_URL", "partial-override-is-ignored"),
                (CREDENTIALS_FILE_VARIABLE, "/dev/null/credentials.json"),
                ("PATH", git_path(&git.0)),
            ],
            Some(&tree.root),
        )
    };
    let bounded = doctor(&["--source-root", tree.root_argument()]);
    assert_eq!(bounded.status.code(), Some(0), "{bounded:?}");
    assert!(
        !git.invocations()
            .contains(&"rev-parse --show-toplevel".to_owned()),
        "Git was asked for a root --source-root names"
    );

    let found = doctor(&[]);

    assert_eq!(found.status.code(), Some(0), "{found:?}");
    assert!(found.stderr.is_empty(), "{found:?}");
    assert!(
        git.invocations()
            .contains(&"rev-parse --show-toplevel".to_owned()),
        "Git was not asked for the root"
    );
    let (bounded, found) = (json_report(&bounded), json_report(&found));
    for field in ["workflows", "requirements", "unknown"] {
        assert_eq!(found[field], bounded[field], "{field}");
    }

    fs::remove_dir(tree.root.join(".git")).expect("Git marker should be removed");
    let refused = doctor(&[]);

    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    assert!(refused.stdout.is_empty(), "{refused:?}");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("--source-root"),
        "{refused:?}"
    );
}

/// Runs the scenario's doctor from its source root with a Git whose `origin` remote is
/// `SECRET_ORIGIN`.
fn scenario_doctor(json: bool) -> Output {
    let tree = WorkflowTree::with_files(&[
        ("workflow.yaml", SCENARIO_ROOT_WORKFLOW),
        ("design.yaml", SCENARIO_STAGE_WORKFLOW),
        ("build.yaml", SCENARIO_STAGE_WORKFLOW),
        ("prompts/system.md", "System prompt."),
        ("prompts/message.md", "Message."),
    ]);
    let git = fake_git(&format!(
        "if [ \"$1 $2 $3\" = 'remote get-url origin' ]; then echo '{SECRET_ORIGIN}'; else exit 1; fi"
    ));
    let mut arguments = vec![
        "runner",
        "doctor",
        "--workflow",
        "workflow.yaml",
        "--source-root",
        ".",
    ];
    if json {
        arguments.push("--json");
    }
    run_with_env_from(
        &arguments,
        &[
            ("UM_API_URL", "partial-override-is-ignored"),
            (CREDENTIALS_FILE_VARIABLE, "/dev/null/credentials.json"),
            (
                "PATH",
                git.path().to_str().expect("fake Git path should be UTF-8"),
            ),
        ],
        Some(&tree.root),
    )
}

fn assert_no_origin_address(report: &str) {
    for fragment in ["synthetic-secret", "github.com", "o/r.git"] {
        assert!(
            !report.contains(fragment),
            "{fragment} leaked into:\n{report}"
        );
    }
}

// SC-CREDREADY-1, REQ-CREDREADY-12, REQ-CREDREADY-8
#[test]
fn runner_doctor_workflow_names_the_repository_provider_from_origin() {
    let output = scenario_doctor(true);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("report should be UTF-8");
    assert_no_origin_address(&stdout);
    let report: Value = serde_json::from_str(&stdout).expect("report should be JSON");
    assert_eq!(
        report["workflows"],
        json!(["build.yaml", "design.yaml", "workflow.yaml"])
    );
    let requirements = report["requirements"]
        .as_array()
        .expect("requirements should be an array");
    for requirement in requirements {
        assert_eq!(requirement["state"], "unverified");
        assert!(
            !requirement["nextAction"]
                .as_str()
                .expect("next action should be text")
                .is_empty()
        );
    }
    let requirements = requirements
        .iter()
        .map(|requirement| (requirement["kind"].clone(), requirement["origins"].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        requirements,
        [
            (
                json!("github"),
                json!([
                    {"declaration": "binding", "workflow": "workflow.yaml", "name": "repository"},
                    {"declaration": "required_tool", "workflow": "workflow.yaml", "name": "gh"},
                    {"declaration": "repository", "name": "origin"},
                ])
            ),
            (
                json!("linear"),
                json!([
                    {"declaration": "forwarded_variable", "workflow": "workflow.yaml", "name": "LINEAR_API_KEY"},
                    {"declaration": "binding", "workflow": "workflow.yaml", "name": "done"},
                    {"declaration": "binding", "workflow": "workflow.yaml", "name": "linearTeam"},
                    {"declaration": "mirror", "workflow": "workflow.yaml"},
                ])
            ),
            (
                json!("claude_code"),
                json!([
                    {"declaration": "agent_profile", "workflow": "build.yaml", "node": step("draft"), "name": "writing"},
                    {"declaration": "agent_profile", "workflow": "design.yaml", "node": step("draft"), "name": "writing"},
                ])
            ),
        ]
    );
    assert_eq!(
        report["unknown"],
        json!([
            {"declaration": "command", "workflow": "build.yaml", "node": step("check")},
            {"declaration": "agent", "workflow": "build.yaml", "node": step("draft")},
            {"declaration": "command", "workflow": "design.yaml", "node": step("check")},
            {"declaration": "agent", "workflow": "design.yaml", "node": step("draft")},
            {"declaration": "forwarded_variable", "workflow": "workflow.yaml", "name": "BUILDKITE_API_TOKEN"},
        ])
    );

    let human = scenario_doctor(false);

    assert_eq!(human.status.code(), Some(0), "{human:?}");
    assert!(human.stderr.is_empty(), "{human:?}");
    let stdout = String::from_utf8(human.stdout).expect("report should be UTF-8");
    assert_no_origin_address(&stdout);
    let lines = stdout.lines().map(str::trim).collect::<Vec<_>>();
    assert!(lines.contains(&"kind: github"), "{stdout}");
    for absent in ["kind: gitlab", "kind: codex"] {
        assert!(!lines.contains(&absent), "{absent} in:\n{stdout}");
    }
    assert!(
        lines.iter().any(|line| line.starts_with("needed by:")
            && !line.contains(".yaml")
            && line.split_whitespace().any(|word| word == "origin")),
        "repository origin missing from:\n{stdout}"
    );
}

// A tree that needs Linear, by a forwarded key, and Claude Code, by an agent step.
const LINEAR_WORKFLOW: &str = r#"schemaVersion: 1
environmentPassthrough: [LINEAR_API_KEY]
agentProfiles:
  reviewing:
    harness:
      kind: claude_code
      config:
        model: claude-opus-4-1
        effort: xhigh
steps:
  review:
    kind: agent
    agent:
      profile: reviewing
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
"#;

const SESSION_TOKEN: &str = "doctor-readiness-session-token";
const ORGANIZATION: &str = "acme";

/// A tree of one file, `main.yaml`, holding `workflow`.
fn single_file_tree(workflow: &str) -> WorkflowTree {
    WorkflowTree::with_files(&[
        ("main.yaml", workflow),
        ("prompts/system.md", "System prompt."),
        ("prompts/message.md", "Message."),
    ])
}

/// A Linear connection as Cloud lists it; it requires the `read` scope.
fn linear_connection(status: &str, granted_scopes: &[&str]) -> Value {
    json!({
        "id": "lcn_01k0z6r1w8f4jy2m7q9v3x5abc",
        "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
        "status": status,
        "workspaceId": "workspace-1",
        "workspaceName": "Research",
        "requiredScopes": ["read"],
        "grantedScopes": granted_scopes,
        "stateUpdatesConsented": false,
        "lifecycleGeneration": 1,
        "createdAt": "2026-09-05T12:00:00Z",
        "updatedAt": "2026-09-05T12:01:00Z"
    })
}

/// A GitLab connection as Cloud lists it, in `state` for `credential_reason`.
fn gitlab_connection(state: &str, credential_reason: Option<&str>) -> Value {
    json!({
        "id": "glc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
        "instanceUrl": "https://gitlab.com",
        "state": state,
        "credentialReason": credential_reason,
        "version": 3,
        "providerUser": null,
        "credential": null,
        "selectedRepositoryIds": [],
        "support": {"state": "unverified", "observedVersion": null},
        "createdAt": "2026-09-05T12:00:00Z",
        "updatedAt": "2026-09-05T12:01:00Z"
    })
}

/// A GitHub App installation as Cloud lists it.
fn github_installation(state: &str) -> Value {
    json!({
        "id": "ghi_01k0z6r1w8f4jy2m7q9v3x5abc",
        "providerInstallationId": "713",
        "providerAccountId": "829",
        "providerAccountLogin": "acme",
        "providerAccountType": "Organization",
        "state": state,
        "createdAt": "2026-09-05T12:00:00Z",
        "updatedAt": "2026-09-05T12:01:00Z"
    })
}

fn forbidden() -> Vec<u8> {
    problem_http_response(
        "403 Forbidden",
        json!({
            "type": "https://api.usefulmachinery.com/problems/forbidden",
            "title": "Forbidden",
            "status": 403
        }),
    )
}

pub(super) fn connection_page(items: &[Value], next_cursor: Option<&str>) -> Vec<u8> {
    let mut body = json!({"items": items});
    if let Some(cursor) = next_cursor {
        body["nextCursor"] = cursor.into();
    }
    json_http_response("200 OK", body)
}

/// A deployment answering with `responses` and, unless `signed_in` is false, a stored
/// session for it.
struct Deployment {
    server: ScriptedServer,
    _directory: TempDir,
    credential: PathBuf,
}

impl Deployment {
    fn new(responses: Vec<Vec<u8>>, signed_in: bool) -> Self {
        let server = ScriptedServer::respond(responses);
        let directory = private_credential_directory();
        let credential = directory.path().join("credentials.json");
        if signed_in {
            write_credential_fixture_for_deployment(
                &credential,
                &server.api_url,
                &server.issuer,
                SESSION_TOKEN,
                "2999-01-01T00:00:00Z",
            );
        }
        Self {
            server,
            _directory: directory,
            credential,
        }
    }

    fn signed_in(responses: Vec<Vec<u8>>) -> Self {
        Self::new(responses, true)
    }

    /// Runs the readiness check of `tree` against this deployment.
    fn doctor(&self, tree: &WorkflowTree, json: bool) -> Output {
        self.doctor_at(&self.server.api_url, tree, json)
    }

    fn doctor_at(&self, api_url: &str, tree: &WorkflowTree, json: bool) -> Output {
        self.doctor_with(api_url, tree, json, &[], &[])
    }

    /// Runs the readiness check with `options` after the readiness options and `environment`
    /// beside the deployment's.
    fn doctor_with(
        &self,
        api_url: &str,
        tree: &WorkflowTree,
        json: bool,
        options: &[&str],
        environment: &[(&str, &str)],
    ) -> Output {
        let readiness = ["--readiness", "--organization-id", ORGANIZATION];
        self.run_doctor(api_url, tree, json, &[&readiness, options], environment)
    }

    /// Runs the doctor of `tree` against the deployment at `api_url`, as `doctor_with` does,
    /// with `options` in order after the source root.
    fn run_doctor(
        &self,
        api_url: &str,
        tree: &WorkflowTree,
        json: bool,
        options: &[&[&str]],
        environment: &[(&str, &str)],
    ) -> Output {
        let mut arguments = vec![
            "runner",
            "doctor",
            "--workflow",
            "main.yaml",
            "--source-root",
            tree.root_argument(),
            "--allow-insecure-http",
        ];
        arguments.extend(options.iter().flat_map(|options| options.iter().copied()));
        if json {
            arguments.push("--json");
        }
        let mut deployment_environment = deployment_environment_with_issuer(
            api_url,
            &self.server.issuer,
            self.credential
                .to_str()
                .expect("credential path should be UTF-8"),
        )
        .to_vec();
        deployment_environment.extend_from_slice(environment);
        run_with_env_from(&arguments, &deployment_environment, Some(&tree.root))
    }

    /// Runs the grants read of `tree` against this deployment, with `options` after
    /// `--grants`.
    fn doctor_grants(&self, tree: &WorkflowTree, json: bool, options: &[&str]) -> Output {
        self.run_doctor(
            &self.server.api_url,
            tree,
            json,
            &[&["--grants"], options],
            &[],
        )
    }

    /// The requests the deployment saw, in order, each carrying the session's bearer token.
    fn requests(self) -> Vec<String> {
        let requests = self.server.finish();
        for request in &requests {
            assert_eq!(
                header_value(request, "authorization"),
                format!("Bearer {SESSION_TOKEN}")
            );
        }
        requests
    }

    /// The requests the deployment saw, each of which must be a Linear connection list for
    /// the organization carrying the session's bearer token.
    fn linear_list_requests(self) -> Vec<String> {
        let requests = self.requests();
        for request in &requests {
            assert!(request.starts_with(LINEAR_LIST), "{request}");
        }
        requests
    }
}

/// The request lines of the organization's connection lists, up to the query or the version.
pub(super) const LINEAR_LIST: &str = "GET /api/v1/organizations/acme/connections/linear?limit=100";
const GITLAB_LIST: &str = "GET /api/v1/organizations/acme/connections/gitlab?limit=100";
const GITHUB_LIST: &str = "GET /api/v1/organizations/acme/github/installations HTTP/1.1\r\n";

fn json_report(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("report should be JSON")
}

/// The requirement of `kind` in a JSON report.
pub(super) fn requirement<'a>(report: &'a Value, kind: &str) -> &'a Value {
    report["requirements"]
        .as_array()
        .expect("requirements should be an array")
        .iter()
        .find(|requirement| requirement["kind"] == kind)
        .unwrap_or_else(|| panic!("{kind} missing from {report}"))
}

fn next_action(requirement: &Value) -> &str {
    requirement["nextAction"]
        .as_str()
        .expect("next action should be text")
}

// REQ-CREDREADY-13, REQ-CREDREADY-16, REQ-CREDREADY-17
#[test]
fn runner_doctor_readiness_reports_a_ready_linear_connection_as_unverified() {
    let tree = single_file_tree(LINEAR_WORKFLOW);
    let deployment = Deployment::signed_in(vec![connection_page(
        &[linear_connection("active", &["read"])],
        None,
    )]);

    let output = deployment.doctor(&tree, true);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    assert_no_secret_output(&output, &[SESSION_TOKEN]);
    let report = json_report(&output);
    assert_eq!(report["deployment"], deployment.server.api_url.as_str());
    let linear = requirement(&report, "linear");
    assert_eq!(linear["state"], "unverified");
    assert_eq!(linear["code"], "runner_unverified");
    assert_eq!(linear["cloud"], "ready");
    let claude_code = requirement(&report, "claude_code");
    assert_eq!(claude_code["state"], "unverified");
    assert_eq!(claude_code["code"], "runner_unverified");
    assert!(claude_code.get("cloud").is_none(), "{claude_code}");
    assert_eq!(
        report["summary"],
        json!({"missing": 0, "reconnectRequired": 0, "insufficientPermission": 0, "unverified": 2, "unknown": 2})
    );
    // One request: the Linear list, and none for GitLab, GitHub or grants.
    assert_eq!(deployment.linear_list_requests().len(), 1);
}

// REQ-CREDREADY-15, REQ-CREDREADY-17
#[test]
fn runner_doctor_readiness_reports_a_linear_connection_that_is_not_ready() {
    for (connections, state, code) in [
        (vec![], "missing", "not_connected"),
        (
            vec![linear_connection("deleted", &["read"])],
            "missing",
            "not_connected",
        ),
        (
            vec![linear_connection("recovery_required", &["read"])],
            "reconnect_required",
            "connection_inactive",
        ),
        (
            vec![linear_connection("active", &[])],
            "insufficient_permission",
            "scope_missing",
        ),
    ] {
        let tree = single_file_tree(LINEAR_WORKFLOW);
        let deployment = Deployment::signed_in(vec![connection_page(&connections, None)]);

        let output = deployment.doctor(&tree, true);

        assert_eq!(output.status.code(), Some(1), "{code}: {output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
        let report = json_report(&output);
        let linear = requirement(&report, "linear");
        assert_eq!(linear["state"], state, "{linear}");
        assert_eq!(linear["code"], code, "{linear}");
        assert_eq!(linear["cloud"], state, "{linear}");
        assert!(
            next_action(linear).contains(&format!(
                "um connection linear authorization create {ORGANIZATION}"
            )),
            "{linear}"
        );
        assert_eq!(deployment.linear_list_requests().len(), 1);
    }
}

// REQ-CREDREADY-15, REQ-CREDREADY-8
#[test]
fn runner_doctor_readiness_human_report_marks_a_failing_requirement() {
    let tree = single_file_tree(LINEAR_WORKFLOW);
    let deployment = Deployment::signed_in(vec![connection_page(&[], None)]);

    let output = deployment.doctor(&tree, false);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("report should be UTF-8");
    let lines = stdout.lines().map(str::trim).collect::<Vec<_>>();
    for field in [
        "✗ Linear",
        "state: missing",
        "code: not_connected",
        "cloud: missing",
        "! Claude Code",
        "missing: 1",
    ] {
        assert!(lines.contains(&field), "{field} missing from:\n{stdout}");
    }
    // Only the provider requirement carries a `cloud` field.
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.starts_with("cloud:"))
            .count(),
        1,
        "{stdout}"
    );
    deployment.linear_list_requests();
}

// REQ-CREDREADY-13, REQ-CREDREADY-16
#[test]
fn runner_doctor_readiness_reads_every_page_of_linear_connections() {
    let tree = single_file_tree(LINEAR_WORKFLOW);
    let deployment = Deployment::signed_in(vec![
        connection_page(
            &[linear_connection("revoked", &["read"])],
            Some("opaque-second-page"),
        ),
        connection_page(&[linear_connection("active", &["read"])], None),
    ]);

    let output = deployment.doctor(&tree, true);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let report = json_report(&output);
    assert_eq!(requirement(&report, "linear")["cloud"], "ready");
    let requests = deployment.linear_list_requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1].starts_with(&format!(
            "GET /api/v1/organizations/{ORGANIZATION}/connections/linear?limit=100&cursor=opaque-second-page "
        )),
        "{}",
        requests[1]
    );
}

// REQ-CREDREADY-14
#[test]
fn runner_doctor_readiness_reports_a_refused_read_as_not_permitted() {
    let tree = single_file_tree(LINEAR_WORKFLOW);
    let deployment = Deployment::signed_in(vec![forbidden()]);

    let output = deployment.doctor(&tree, true);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let report = json_report(&output);
    let linear = requirement(&report, "linear");
    assert_eq!(linear["state"], "unverified");
    assert_eq!(linear["code"], "not_permitted");
    assert_eq!(linear["cloud"], "unverified");
    assert!(!next_action(linear).contains("um "), "{linear}");
    assert_eq!(deployment.linear_list_requests().len(), 1);
}

// REQ-CREDREADY-14
#[test]
fn runner_doctor_readiness_ends_without_a_report_when_cloud_cannot_answer() {
    let tree = single_file_tree(LINEAR_WORKFLOW);
    let signed_out = Deployment::new(Vec::new(), false);

    let unauthenticated = signed_out.doctor(&tree, true);

    assert_eq!(
        unauthenticated.status.code(),
        Some(3),
        "{unauthenticated:?}"
    );
    let failure = json_report(&unauthenticated);
    assert_eq!(failure["outcome"], "unauthenticated");
    assert!(failure.get("requirements").is_none(), "{failure}");
    let human = signed_out.doctor(&tree, false);
    assert_eq!(human.status.code(), Some(3), "{human:?}");
    assert!(human.stdout.is_empty(), "{human:?}");
    let stderr = String::from_utf8(human.stderr).expect("error should be UTF-8");
    assert!(
        stderr.lines().any(|line| line.trim() == "um auth login"),
        "{stderr}"
    );
    assert!(signed_out.server.finish().is_empty());

    // A listener that is gone: nothing answers at its address.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = closed
        .local_addr()
        .expect("listener should have an address");
    drop(closed);
    let unreachable_url = format!("http://{address}/api");
    let signed_in = Deployment::new(Vec::new(), false);
    write_credential_fixture_for_deployment(
        &signed_in.credential,
        &unreachable_url,
        &signed_in.server.issuer,
        SESSION_TOKEN,
        "2999-01-01T00:00:00Z",
    );

    let unreachable = signed_in.doctor_at(&unreachable_url, &tree, true);

    assert_eq!(unreachable.status.code(), Some(4), "{unreachable:?}");
    let failure = json_report(&unreachable);
    assert_eq!(failure["outcome"], "unreachable");
    assert!(failure.get("requirements").is_none(), "{failure}");
    assert!(signed_in.server.finish().is_empty());
}

// REQ-CREDREADY-13
#[test]
fn runner_doctor_readiness_asks_nothing_when_no_provider_is_needed() {
    let tree = single_file_tree(
        &LINEAR_WORKFLOW.replace("environmentPassthrough: [LINEAR_API_KEY]\n", ""),
    );

    let output = tree.run(&[
        "runner",
        "doctor",
        "--workflow",
        "main.yaml",
        "--source-root",
        tree.root_argument(),
        "--readiness",
        "--organization-id",
        ORGANIZATION,
        "--json",
    ]);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let report = json_report(&output);
    assert_eq!(report["deployment"], Value::Null);
    let kinds = report["requirements"]
        .as_array()
        .expect("requirements should be an array")
        .iter()
        .map(|requirement| requirement["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(kinds, ["claude_code"]);
}

// REQ-CREDREADY-13
#[test]
fn runner_doctor_readiness_refuses_incomplete_or_unrequested_options() {
    let tree = single_file_tree(LINEAR_WORKFLOW);
    let workflow = [
        "runner",
        "doctor",
        "--workflow",
        "main.yaml",
        "--source-root",
        tree.root_argument(),
    ];
    let with_workflow = |options: &[&'static str]| {
        let mut arguments = workflow.to_vec();
        arguments.extend_from_slice(options);
        arguments
    };
    for arguments in [
        with_workflow(&["--readiness"]),
        vec![
            "runner",
            "doctor",
            "--readiness",
            "--organization-id",
            ORGANIZATION,
        ],
        with_workflow(&["--organization-id", ORGANIZATION]),
        with_workflow(&["--service-api-key-file", "/dev/null/key"]),
        with_workflow(&["--allow-insecure-http"]),
        with_workflow(&["--project-id", PROJECT_ID]),
        vec!["runner", "doctor", "--allow-insecure-http"],
    ] {
        let output = tree.run(&arguments);

        assert_eq!(output.status.code(), Some(2), "{arguments:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{arguments:?}");
    }
}

// REQ-CREDREADY-7, REQ-RUNNERENROLL-45
#[test]
fn runner_doctor_workflow_without_readiness_names_it_and_asks_nothing() {
    let tree = single_file_tree(LINEAR_WORKFLOW);

    let output = doctor(&tree, true);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let report = json_report(&output);
    assert_eq!(report["deployment"], Value::Null);
    let linear = requirement(&report, "linear");
    assert_eq!(linear["state"], "unverified");
    assert_eq!(linear["code"], "not_checked");
    assert_eq!(linear["cloud"], "not_checked");
    assert!(next_action(linear).contains("--readiness"), "{linear}");
    assert!(
        next_action(linear).contains("um connection linear list"),
        "{linear}"
    );
    assert!(requirement(&report, "claude_code").get("cloud").is_none());
}

/// A tree that needs GitLab, by a forwarded key, and Claude Code.
fn gitlab_tree() -> WorkflowTree {
    single_file_tree(&LINEAR_WORKFLOW.replace("LINEAR_API_KEY", "GITLAB_TOKEN"))
}

/// A tree that needs GitHub, by a required tool, and Claude Code.
fn github_tree() -> WorkflowTree {
    single_file_tree(&LINEAR_WORKFLOW.replace(
        "environmentPassthrough: [LINEAR_API_KEY]",
        "requires: {tools: [gh]}",
    ))
}

fn rate_limited() -> Vec<u8> {
    problem_http_response(
        "429 Too Many Requests",
        json!({
            "type": "https://api.usefulmachinery.com/problems/rate-limit-exceeded",
            "title": "Too Many Requests",
            "status": 429
        }),
    )
}

// REQ-CREDREADY-14. The Linear read alone: the GitLab and GitHub clients of `um-api` know no
// rate-limited answer, and the public contract lists none for their lists.
#[test]
fn runner_doctor_readiness_ends_without_a_report_when_cloud_rate_limits_the_linear_read() {
    let tree = single_file_tree(LINEAR_WORKFLOW);
    let deployment = Deployment::signed_in(vec![rate_limited()]);

    let output = deployment.doctor(&tree, true);

    assert_eq!(output.status.code(), Some(4), "{output:?}");
    let failure = json_report(&output);
    assert_eq!(failure["outcome"], "rate_limited", "{failure}");
    assert!(failure.get("requirements").is_none(), "{failure}");
    assert_eq!(deployment.linear_list_requests().len(), 1);
}

/// Asserts that the readiness check failed on the requirement of `kind`, Cloud showing
/// `state` with `code`, and that the next action names `remedy` for the organization.
fn assert_fails_on(output: &Output, kind: &str, state: &str, code: &str, remedy: &str) {
    assert_eq!(output.status.code(), Some(1), "{code}: {output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let report = json_report(output);
    let requirement = requirement(&report, kind);
    assert_eq!(requirement["state"], state, "{requirement}");
    assert_eq!(requirement["code"], code, "{requirement}");
    assert_eq!(requirement["cloud"], state, "{requirement}");
    assert!(
        next_action(requirement).contains(&format!("{remedy} {ORGANIZATION}")),
        "{requirement}"
    );
}

// REQ-CREDREADY-19, REQ-CREDREADY-17
#[test]
fn runner_doctor_readiness_reports_a_gitlab_connection_that_is_not_ready() {
    for (connections, state, code, remedy) in [
        (
            vec![],
            "missing",
            "not_connected",
            "um connection gitlab create",
        ),
        (
            vec![gitlab_connection(
                "credential_required",
                Some("not-configured"),
            )],
            "missing",
            "not_connected",
            "um connection gitlab create",
        ),
        (
            vec![gitlab_connection("credential_required", Some("expired"))],
            "reconnect_required",
            "credential_expired",
            "um connection gitlab credential create",
        ),
        (
            vec![gitlab_connection(
                "credential_required",
                Some("credentials-rejected"),
            )],
            "reconnect_required",
            "connection_inactive",
            "um connection gitlab credential create",
        ),
        (
            vec![gitlab_connection("disconnected", None)],
            "reconnect_required",
            "connection_inactive",
            "um connection gitlab credential create",
        ),
    ] {
        let tree = gitlab_tree();
        let deployment = Deployment::signed_in(vec![connection_page(&connections, None)]);

        let output = deployment.doctor(&tree, true);

        assert_fails_on(&output, "gitlab", state, code, remedy);
        let requests = deployment.requests();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert!(requests[0].starts_with(GITLAB_LIST), "{}", requests[0]);
    }
}

// REQ-CREDREADY-19, REQ-CREDREADY-16
#[test]
fn runner_doctor_readiness_reports_an_active_gitlab_connection_as_unverified() {
    let tree = gitlab_tree();
    let deployment = Deployment::signed_in(vec![
        connection_page(
            &[gitlab_connection("disconnected", None)],
            Some("opaque-second-page"),
        ),
        connection_page(&[gitlab_connection("active", None)], None),
    ]);

    let output = deployment.doctor(&tree, true);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let report = json_report(&output);
    let gitlab = requirement(&report, "gitlab");
    assert_eq!(gitlab["state"], "unverified");
    assert_eq!(gitlab["code"], "runner_unverified");
    assert_eq!(gitlab["cloud"], "ready");
    let requests = deployment.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(
        requests[1].starts_with(&format!("{GITLAB_LIST}&cursor=opaque-second-page ")),
        "{}",
        requests[1]
    );
}

// REQ-CREDREADY-20, REQ-CREDREADY-16, REQ-CREDREADY-17
#[test]
fn runner_doctor_readiness_reports_github_installations() {
    for (installations, state, code) in [
        (vec![], "missing", "not_connected"),
        (
            vec![
                github_installation("revoked"),
                github_installation("disconnected"),
            ],
            "reconnect_required",
            "connection_inactive",
        ),
        (
            vec![
                github_installation("revoked"),
                github_installation("active"),
                github_installation("disconnected"),
            ],
            "unverified",
            "runner_unverified",
        ),
    ] {
        let tree = github_tree();
        let deployment = Deployment::signed_in(vec![connection_page(&installations, None)]);

        let output = deployment.doctor(&tree, true);

        if code == "runner_unverified" {
            assert_eq!(output.status.code(), Some(0), "{output:?}");
            let report = json_report(&output);
            let github = requirement(&report, "github");
            assert_eq!(github["state"], state);
            assert_eq!(github["code"], code);
            assert_eq!(github["cloud"], "ready");
        } else {
            assert_fails_on(
                &output,
                "github",
                state,
                code,
                "um connection github setup begin",
            );
        }
        let requests = deployment.requests();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert!(requests[0].starts_with(GITHUB_LIST), "{}", requests[0]);
    }
}

// REQ-CREDREADY-20, REQ-CREDREADY-14
#[test]
fn runner_doctor_readiness_reports_a_refused_github_read_as_not_permitted() {
    let tree = github_tree();
    let deployment = Deployment::signed_in(vec![forbidden()]);

    let output = deployment.doctor(&tree, true);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let report = json_report(&output);
    let github = requirement(&report, "github");
    assert_eq!(github["state"], "unverified");
    assert_eq!(github["code"], "not_permitted");
    assert_eq!(github["cloud"], "unverified");
    let requests = deployment.requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(requests[0].starts_with(GITHUB_LIST), "{}", requests[0]);
}

// SC-CREDREADY-2, REQ-CREDREADY-13, REQ-CREDREADY-19, REQ-CREDREADY-16, REQ-CREDREADY-17
#[test]
fn runner_doctor_readiness_finds_an_expired_gitlab_credential_beside_a_ready_linear() {
    let tree = single_file_tree(
        &LINEAR_WORKFLOW.replace("[LINEAR_API_KEY]", "[LINEAR_API_KEY, GITLAB_TOKEN]"),
    );
    // The reads are made in kind order: GitLab, then Linear.
    let deployment = Deployment::signed_in(vec![
        connection_page(
            &[gitlab_connection("credential_required", Some("expired"))],
            None,
        ),
        connection_page(&[linear_connection("active", &["read"])], None),
    ]);

    let output = deployment.doctor(&tree, true);

    assert_fails_on(
        &output,
        "gitlab",
        "reconnect_required",
        "credential_expired",
        "um connection gitlab credential create",
    );
    let report = json_report(&output);
    let linear = requirement(&report, "linear");
    assert_eq!(linear["state"], "unverified");
    assert_eq!(linear["cloud"], "ready");
    let claude_code = requirement(&report, "claude_code");
    assert_eq!(claude_code["code"], "runner_unverified");
    assert!(claude_code.get("cloud").is_none(), "{claude_code}");
    assert_eq!(
        report["summary"],
        json!({"missing": 0, "reconnectRequired": 1, "insufficientPermission": 0, "unverified": 2, "unknown": 2})
    );
    // Exactly the GitLab and Linear lists: nothing for GitHub.
    let requests = deployment.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(requests[0].starts_with(GITLAB_LIST), "{}", requests[0]);
    assert!(requests[1].starts_with(LINEAR_LIST), "{}", requests[1]);
}

const PROJECT_ID: &str = "prj_01k0z6r1w8f4jy2m7q9v3x5abc";
const PROJECT_READ: &str =
    "GET /api/v1/organizations/acme/projects/prj_01k0z6r1w8f4jy2m7q9v3x5abc HTTP/1.1\r\n";

/// A tree of one agent step on Claude Code, which needs no provider of its own.
fn claude_code_tree() -> WorkflowTree {
    single_file_tree(&LINEAR_WORKFLOW.replace("environmentPassthrough: [LINEAR_API_KEY]\n", ""))
}

/// A Git whose `origin` remote is at `gitlab.com` and which leaves a file named `invoked`
/// beside itself whenever it runs.
fn recording_gitlab_git() -> TempDir {
    fake_git(
        "echo invoked > \"${0%/*}/invoked\"\n\
         if [ \"$1 $2 $3\" = 'remote get-url origin' ]; then echo 'https://gitlab.com/g/p.git'; else exit 1; fi",
    )
}

fn git_path(git: &TempDir) -> &str {
    git.path().to_str().expect("fake Git path should be UTF-8")
}

/// A GitHub repository as a project carries it.
fn github_repository(availability: &str) -> Value {
    json!({
        "connectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "providerKind": "github",
        "installationBindingId": "ghi_01k0z6r1w8f4jy2m7q9v3x5abc",
        "providerRepositoryId": "123456789",
        "fullName": "acme/widget",
        "defaultBranch": "main",
        "availability": availability
    })
}

/// A GitLab repository as a project carries it, with the deployment's admission diagnostic.
fn gitlab_repository(availability: &str, admission_diagnostic: Option<&str>) -> Value {
    let mut repository = json!({
        "connectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "providerKind": "gitlab",
        "gitlabConnectionId": "glc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "instanceUrl": "https://gitlab.com",
        "providerRepositoryId": "123456789",
        "fullName": "acme/widget",
        "defaultBranch": "main",
        "availability": availability
    });
    if let Some(diagnostic) = admission_diagnostic {
        repository["admissionDiagnostic"] = diagnostic.into();
    }
    repository
}

/// The project `PROJECT_ID`, with a runner pool and `repository`.
fn project(repository: Option<Value>) -> Vec<u8> {
    let blockers = match &repository {
        None => json!(["repository_detached"]),
        Some(repository) if repository["availability"] == "unavailable" => {
            json!(["repository_unavailable"])
        }
        Some(_) => json!([]),
    };
    json_http_response(
        "200 OK",
        json!({
            "id": PROJECT_ID,
            "organizationId": "org_01k0z6r1w8f4jy2m7q9v3x5abc",
            "name": "widget",
            "runnerPool": {"id": "rpl_01k0z6r1w8f4jy2m7q9v3x5abc", "name": "builders"},
            "repository": repository,
            "executionReadiness": {
                "ready": blockers.as_array().is_some_and(Vec::is_empty),
                "blockers": blockers
            },
            "createdAt": "2026-09-05T12:00:00Z",
            "updatedAt": "2026-09-05T12:01:00Z"
        }),
    )
}

impl Deployment {
    /// Runs the readiness check of `tree` for the project `PROJECT_ID` with `git` on the path.
    fn doctor_for_project(&self, tree: &WorkflowTree, git: &TempDir) -> Output {
        self.doctor_with(
            &self.server.api_url,
            tree,
            true,
            &["--project-id", PROJECT_ID],
            &[("PATH", git_path(git))],
        )
    }
}

// REQ-CREDREADY-21, REQ-CREDREADY-12
#[test]
fn runner_doctor_readiness_takes_the_repository_provider_from_the_project() {
    let tree = claude_code_tree();
    let git = recording_gitlab_git();
    // Without a project the `origin` remote names GitLab, which shows the fake Git runs.
    let offline = run_with_env_from(
        &[
            "runner",
            "doctor",
            "--workflow",
            "main.yaml",
            "--source-root",
            tree.root_argument(),
            "--json",
        ],
        &[("PATH", git_path(&git))],
        Some(&tree.root),
    );
    assert_eq!(offline.status.code(), Some(0), "{offline:?}");
    requirement(&json_report(&offline), "gitlab");
    fs::remove_file(git.path().join("invoked")).expect("the fake Git should have run");
    let deployment = Deployment::signed_in(vec![
        project(Some(github_repository("active"))),
        connection_page(&[github_installation("active")], None),
    ]);

    let output = deployment.doctor_for_project(&tree, &git);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    assert!(!git.path().join("invoked").exists(), "Git ran");
    let report = json_report(&output);
    let github = requirement(&report, "github");
    assert_eq!(github["state"], "unverified");
    assert_eq!(github["code"], "runner_unverified");
    assert_eq!(github["cloud"], "ready");
    assert_eq!(
        github["origins"],
        json!([{"declaration": "repository", "name": "project"}])
    );
    let kinds = report["requirements"]
        .as_array()
        .expect("requirements should be an array")
        .iter()
        .map(|requirement| requirement["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(kinds, ["github", "claude_code"]);
    assert!(
        !report["unknown"]
            .as_array()
            .expect("unknown should be an array")
            .iter()
            .any(|entry| entry["declaration"] == "repository"),
        "{report}"
    );
    // The project first, since it can add a kind, then the GitHub installations.
    let requests = deployment.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(requests[0].starts_with(PROJECT_READ), "{}", requests[0]);
    assert!(requests[1].starts_with(GITHUB_LIST), "{}", requests[1]);
}

// REQ-CREDREADY-21, REQ-CREDREADY-17
#[test]
fn runner_doctor_readiness_reports_an_unavailable_project_repository() {
    // Whatever the installations say, an unavailable repository needs reconnecting.
    for installations in [
        connection_page(&[github_installation("active")], None),
        forbidden(),
    ] {
        let tree = claude_code_tree();
        let git = recording_gitlab_git();
        let deployment = Deployment::signed_in(vec![
            project(Some(github_repository("unavailable"))),
            installations,
        ]);

        let output = deployment.doctor_for_project(&tree, &git);

        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(output.stderr.is_empty(), "{output:?}");
        let report = json_report(&output);
        let github = requirement(&report, "github");
        assert_eq!(github["state"], "reconnect_required", "{github}");
        assert_eq!(github["code"], "repository_unavailable", "{github}");
        assert!(
            next_action(github).contains(&format!(
                "um project repository show {ORGANIZATION} {PROJECT_ID}"
            )),
            "{github}"
        );
        assert_eq!(deployment.requests().len(), 2);
    }

    // A closed GitLab admission is the deployment's, which no reconnection fixes.
    let tree = claude_code_tree();
    let git = recording_gitlab_git();
    let deployment = Deployment::signed_in(vec![
        project(Some(gitlab_repository(
            "unavailable",
            Some("gitlab_admission_closed"),
        ))),
        connection_page(&[gitlab_connection("active", None)], None),
    ]);

    let output = deployment.doctor_for_project(&tree, &git);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let report = json_report(&output);
    let gitlab = requirement(&report, "gitlab");
    assert_eq!(gitlab["state"], "unverified", "{gitlab}");
    assert_eq!(gitlab["code"], "provider_unavailable", "{gitlab}");
    assert_eq!(gitlab["cloud"], "unverified", "{gitlab}");
    assert!(
        next_action(gitlab).contains("um project repository show"),
        "{gitlab}"
    );
    let requests = deployment.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(requests[1].starts_with(GITLAB_LIST), "{}", requests[1]);
}

// REQ-CREDREADY-21
#[test]
fn runner_doctor_readiness_lists_a_project_without_a_repository_as_unknown() {
    let tree = claude_code_tree();
    let git = recording_gitlab_git();
    let deployment = Deployment::signed_in(vec![project(None)]);

    let output = deployment.doctor_for_project(&tree, &git);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(!git.path().join("invoked").exists(), "Git ran");
    let report = json_report(&output);
    let kinds = report["requirements"]
        .as_array()
        .expect("requirements should be an array")
        .iter()
        .map(|requirement| requirement["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(kinds, ["claude_code"]);
    let repositories = report["unknown"]
        .as_array()
        .expect("unknown should be an array")
        .iter()
        .filter(|entry| entry["declaration"] == "repository")
        .collect::<Vec<_>>();
    assert_eq!(
        repositories,
        [&json!({"declaration": "repository", "name": "project"})]
    );
    assert_eq!(report["deployment"], deployment.server.api_url.as_str());
    let requests = deployment.requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(requests[0].starts_with(PROJECT_READ), "{}", requests[0]);
}

// REQ-CREDREADY-21
#[test]
fn runner_doctor_readiness_ends_as_project_show_does_for_an_unknown_project() {
    let not_found = || {
        problem_http_response(
            "404 Not Found",
            json!({
                "type": "https://api.usefulmachinery.com/problems/not-found",
                "title": "Not Found",
                "status": 404
            }),
        )
    };
    let tree = claude_code_tree();
    let git = recording_gitlab_git();
    let deployment = Deployment::signed_in(vec![not_found()]);

    let output = deployment.doctor_for_project(&tree, &git);

    let shown = Deployment::signed_in(vec![not_found()]);
    let credential = shown
        .credential
        .to_str()
        .expect("credential path should be UTF-8");
    let show = run_with_env_from(
        &[
            "project",
            "show",
            ORGANIZATION,
            PROJECT_ID,
            "--allow-insecure-http",
            "--json",
        ],
        &deployment_environment_with_issuer(
            &shown.server.api_url,
            &shown.server.issuer,
            credential,
        ),
        None,
    );
    assert_eq!(show.status.code(), Some(1), "{show:?}");
    assert_eq!(output.status.code(), show.status.code(), "{output:?}");
    let without_deployment = |output: &Output| {
        let mut document = json_report(output);
        document
            .as_object_mut()
            .expect("failure should be an object")
            .remove("deployment");
        document
    };
    let failure = without_deployment(&output);
    assert_eq!(failure, without_deployment(&show));
    assert_eq!(failure["outcome"], "not_found");
    assert!(failure.get("requirements").is_none(), "{failure}");
    let requests = deployment.requests();
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert!(requests[0].starts_with(PROJECT_READ), "{}", requests[0]);
    shown.requests();
}

/// A tree that needs GitHub, by a required tool, Linear, by a forwarded key, and Codex, by an
/// agent step.
fn grants_tree() -> WorkflowTree {
    single_file_tree(
        &LINEAR_WORKFLOW
            .replace(
                "environmentPassthrough: [LINEAR_API_KEY]",
                "environmentPassthrough: [LINEAR_API_KEY]\nrequires: {tools: [gh]}",
            )
            .replace("kind: claude_code", "kind: codex")
            .replace("model: claude-opus-4-1", "model: gpt-5.4"),
    )
}

/// The member's grants list: an active GitHub grant authorized for `RUNNER_ID`, a Codex grant
/// that runner reported ready and accepted at its last use, and a revoked Linear grant.
fn held_grants() -> Vec<u8> {
    page(
        &[
            oauth_grant(OAUTH_GRANT_ID, "github", "active"),
            oauth_grant(REVOKED_OAUTH_GRANT_ID, "linear", "revoked"),
            native_grant_in_use(),
        ],
        None,
    )
}

/// The requests the deployment saw, each of which must be the first page of the member's
/// grants list with the session's bearer token.
fn assert_only_grants_lists(deployment: Deployment, count: usize) {
    let requests = deployment.requests();
    assert_eq!(requests.len(), count, "{requests:?}");
    for request in &requests {
        assert!(request.starts_with(LIST_FIRST_PAGE), "{request}");
    }
}

/// The one grant a requirement of `kind` reports.
pub(super) fn held<'a>(report: &'a Value, kind: &str) -> &'a Value {
    let grants = requirement(report, kind)["grant"]["grants"]
        .as_array()
        .unwrap_or_else(|| panic!("{kind} carries no grants in {report}"));
    assert_eq!(grants.len(), 1, "{kind}: {grants:?}");
    &grants[0]
}

// REQ-CREDREADY-22, REQ-CREDREADY-18
#[test]
fn runner_doctor_grants_shows_the_members_grants_beside_each_requirement() {
    let tree = grants_tree();
    let offline = json_report(&doctor(&tree, true));
    let deployment = Deployment::signed_in(vec![held_grants(), held_grants()]);

    let output = deployment.doctor_grants(&tree, true, &[]);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    assert_no_secret_output(&output, &[SENTINEL, SESSION_TOKEN]);
    let report = json_report(&output);
    assert_eq!(report["deployment"], deployment.server.api_url.as_str());
    // A grant is a facet: no requirement's state, code or next action, and not the summary,
    // changes (REQ-CREDREADY-18).
    for kind in ["github", "linear", "codex"] {
        for field in ["state", "code", "cloud", "nextAction"] {
            assert_eq!(
                requirement(&report, kind)[field],
                requirement(&offline, kind)[field],
                "{kind} {field}"
            );
        }
    }
    assert_eq!(report["summary"], offline["summary"]);
    for (kind, id, generation, account, account_id) in [
        ("github", OAUTH_GRANT_ID, 1, "identityId", IDENTITY_ID),
        (
            "codex",
            NATIVE_GRANT_ID,
            2,
            "organizationId",
            ORGANIZATION_ID,
        ),
    ] {
        assert_eq!(requirement(&report, kind)["grant"]["provider"], kind);
        let grant = held(&report, kind);
        assert_eq!(grant["id"], id, "{kind}");
        assert_eq!(grant["state"], "active", "{kind}");
        assert_eq!(grant["generation"], generation, "{kind}");
        assert_eq!(grant[account], account_id, "{kind}");
        assert_eq!(
            grant["authorizations"],
            json!([{"runnerId": RUNNER_ID, "availability": "available", "nextAction": "none"}]),
            "{kind}"
        );
        assert!(grant.get("availability").is_none(), "{grant}");
    }
    // The revoked Linear grant is not held.
    assert_eq!(
        requirement(&report, "linear")["grant"],
        json!({"provider": "linear", "grants": [], "availability": "not_connected", "nextAction": "connect"})
    );

    let human = deployment.doctor_grants(&tree, false, &[]);

    assert_eq!(human.status.code(), Some(0), "{human:?}");
    assert_no_secret_output(&human, &[SENTINEL, SESSION_TOKEN]);
    let stdout = String::from_utf8(human.stdout).expect("report should be UTF-8");
    for fragment in [
        OAUTH_GRANT_ID,
        NATIVE_GRANT_ID,
        RUNNER_ID,
        "um grant create linear",
        deployment.server.api_url.as_str(),
    ] {
        assert!(
            stdout.contains(fragment),
            "{fragment} missing from:\n{stdout}"
        );
    }
    assert_only_grants_lists(deployment, 2);
}

// REQ-CREDREADY-22
#[test]
fn runner_doctor_grants_keeps_the_named_organizations_coding_agent_grants() {
    let tree = grants_tree();
    let deployment = Deployment::signed_in(vec![held_grants()]);

    let output = deployment.doctor_grants(
        &tree,
        true,
        &["--organization-id", "org_01k0z6r1w8f4jy2m7q9v3x5abd"],
    );

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let report = json_report(&output);
    assert_eq!(held(&report, "github")["id"], OAUTH_GRANT_ID);
    assert_eq!(
        requirement(&report, "codex")["grant"],
        json!({"provider": "codex", "grants": [], "availability": "not_connected", "nextAction": "connect"})
    );
    // An organization named by ID is not read.
    assert_only_grants_lists(deployment, 1);
}

// REQ-CREDREADY-23
#[test]
fn runner_doctor_grants_reports_each_grant_on_the_named_runner() {
    let tree = grants_tree();
    let mut github = oauth_grant(OAUTH_GRANT_ID, "github", "active");
    github["authorizations"]
        .as_array_mut()
        .expect("authorizations should be an array")
        .push(json!({
            "id": "pga_01k0z6r1w8f4jy2m7q9v3x5abd",
            "runnerId": "rnr_01k0z6r1w8f4jy2m7q9v3x5abe",
            "organizationId": ORGANIZATION_ID,
            "createdAt": "2026-11-01T08:06:00Z"
        }));
    let grants = || page(&[github.clone(), native_grant_in_use()], None);
    let deployment = Deployment::signed_in(vec![grants(), grants()]);

    let named = json_report(&deployment.doctor_grants(&tree, true, &["--runner-id", RUNNER_ID]));
    // The runner whose authorization of the Codex grant ended holds neither grant.
    let other =
        json_report(&deployment.doctor_grants(&tree, true, &["--runner-id", ENDED_RUNNER_ID]));

    for kind in ["github", "codex"] {
        let grant = held(&named, kind);
        assert_eq!(
            grant["authorizations"],
            json!([{"runnerId": RUNNER_ID, "availability": "available", "nextAction": "none"}]),
            "{kind}"
        );
        assert!(grant.get("availability").is_none(), "{grant}");
        let grant = held(&other, kind);
        assert_eq!(grant["authorizations"], json!([]), "{kind}");
        assert_eq!(grant["availability"], "not_connected", "{kind}");
        assert_eq!(grant["nextAction"], "authorize_runner", "{kind}");
    }
    assert_only_grants_lists(deployment, 2);

    let report = |state: &str, last_use: Option<&str>| {
        let mut report =
            json!({"state": state, "generation": 2, "reportedAt": "2026-11-02T10:07:00Z"});
        if let Some(last_use) = last_use {
            report["lastUse"] = last_use.into();
        }
        report
    };
    for (runner_report, availability, next_action) in [
        (None, "pending", "wait_for_runner"),
        (
            Some(report("ready", Some("refused"))),
            "reconnect_required",
            "reconnect",
        ),
        (
            Some(report("install_failed", None)),
            "delivery_failed",
            "sync_runner",
        ),
    ] {
        let mut codex = native_grant_in_use();
        let authorization = codex["authorizations"][0]
            .as_object_mut()
            .expect("authorization should be an object");
        match &runner_report {
            None => authorization.remove("report"),
            Some(runner_report) => authorization.insert("report".to_owned(), runner_report.clone()),
        };
        let deployment = Deployment::signed_in(vec![page(&[codex], None)]);

        let output = deployment.doctor_grants(&tree, true, &["--runner-id", RUNNER_ID]);

        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let report = json_report(&output);
        assert_eq!(
            held(&report, "codex")["authorizations"],
            json!([{"runnerId": RUNNER_ID, "availability": availability, "nextAction": next_action}]),
            "{runner_report:?}"
        );
        assert_only_grants_lists(deployment, 1);
    }
}

// REQ-CREDREADY-13, REQ-CREDREADY-22, REQ-CREDREADY-23
#[test]
fn runner_doctor_grants_refuses_incomplete_or_unrequested_options() {
    let tree = grants_tree();
    let deployment = Deployment::signed_in(Vec::new());
    let service_key: &[&str] = &["--service-api-key-file", "/dev/null/key"];
    let readiness: &[&str] = &["--readiness", "--organization-id", ORGANIZATION];
    let cases: [&[&[&str]]; 5] = [
        &[&["--grants"], service_key],
        &[&["--grants"], readiness, service_key],
        // With --readiness, so that only the grants read --runner-id needs is missing.
        &[readiness, &["--runner-id", RUNNER_ID]],
        &[&["--grants", "--runner-id", "builder-one"]],
        &[&["--grants", "--project-id", PROJECT_ID]],
    ];
    for options in cases {
        let output = deployment.run_doctor(&deployment.server.api_url, &tree, true, options, &[]);

        assert_eq!(output.status.code(), Some(2), "{options:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{options:?}");
    }
    let output = run(&["runner", "doctor", "--grants"]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert!(deployment.server.finish().is_empty());
}

/// A tree that needs GitHub, by a required tool, Linear, by a forwarded key, and Claude Code,
/// by an agent step.
fn require_grants_tree() -> WorkflowTree {
    single_file_tree(&LINEAR_WORKFLOW.replace(
        "environmentPassthrough: [LINEAR_API_KEY]",
        "environmentPassthrough: [LINEAR_API_KEY]\nrequires: {tools: [gh]}",
    ))
}

const LINEAR_GRANT_ID: &str = "pgr_01k0z6r1w8f4jy2m7q9v3x5abe";

/// An active Linear grant authorized for `runner` alone.
fn linear_grant_for(runner: &str) -> Value {
    let mut linear = oauth_grant(LINEAR_GRANT_ID, "linear", "active");
    linear["authorizations"][0]["runnerId"] = runner.into();
    linear
}

/// A Claude Code grant authorized for `RUNNER_ID`, whose runner made `report`, or none.
fn claude_code_grant(report: Option<Value>) -> Value {
    let mut grant = native_grant_in_use();
    grant["provider"] = "claude_code".into();
    let authorization = grant["authorizations"][0]
        .as_object_mut()
        .expect("authorization should be an object");
    match report {
        None => authorization.remove("report"),
        Some(report) => authorization.insert("report".to_owned(), report),
    };
    grant
}

/// A second Claude Code grant, authorized for no runner.
fn unauthorized_claude_code_grant() -> Value {
    let mut grant = claude_code_grant(None);
    grant["id"] = "agl_01k0z6r1w8f4jy2m7q9v3x5abd".into();
    grant["authorizations"] = json!([]);
    grant
}

/// A runner's report of the Claude Code grant at its generation.
fn runner_report(state: &str, last_use: Option<&str>) -> Value {
    let mut report = json!({"state": state, "generation": 2, "reportedAt": "2026-11-02T10:07:00Z"});
    if let Some(last_use) = last_use {
        report["lastUse"] = last_use.into();
    }
    report
}

/// Asserts that the requirement of `kind` reads `state` with `code` and that its next action
/// is the grant status's `next_action`, with `command` when it names one.
fn assert_grant_fails(
    report: &Value,
    kind: &str,
    state: &str,
    code: &str,
    next_action: &str,
    command: &str,
) {
    let requirement = requirement(report, kind);
    assert_eq!(requirement["state"], state, "{kind}: {requirement}");
    assert_eq!(requirement["code"], code, "{kind}: {requirement}");
    let text = self::next_action(requirement);
    assert!(text.starts_with(next_action), "{kind}: {text}");
    assert!(text.contains(command), "{kind}: {text}");
}

// REQ-CREDREADY-26, REQ-CREDREADY-18, REQ-CREDREADY-17
#[test]
fn runner_doctor_require_grants_fails_on_a_grant_the_runner_cannot_use() {
    let tree = require_grants_tree();
    let offline = json_report(&doctor(&tree, true));
    let grants = || {
        page(
            &[
                oauth_grant(OAUTH_GRANT_ID, "github", "active"),
                linear_grant_for(ENDED_RUNNER_ID),
            ],
            None,
        )
    };
    let deployment = Deployment::signed_in(vec![grants(), grants(), grants()]);
    let require: &[&str] = &["--runner-id", RUNNER_ID, "--require-grants"];

    let output = deployment.doctor_grants(&tree, true, require);

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let report = json_report(&output);
    for field in ["state", "code", "cloud", "nextAction"] {
        assert_eq!(
            requirement(&report, "github")[field],
            requirement(&offline, "github")[field],
            "github {field}"
        );
    }
    assert_grant_fails(
        &report,
        "linear",
        "missing",
        "grant_not_authorized",
        "authorize_runner",
        &format!("um grant authorize {LINEAR_GRANT_ID}"),
    );
    assert_grant_fails(
        &report,
        "claude_code",
        "missing",
        "grant_not_connected",
        "connect",
        "um grant create claude_code",
    );
    // The grants beside each requirement are those `--grants` shows.
    assert_eq!(held(&report, "linear")["nextAction"], "authorize_runner");
    assert_eq!(report["summary"]["missing"], 2);
    assert_eq!(report["summary"]["unverified"], 1);

    let human = deployment.doctor_grants(&tree, false, require);

    assert_eq!(human.status.code(), Some(1), "{human:?}");
    let stdout = String::from_utf8(human.stdout).expect("report should be UTF-8");
    for fragment in ["grant_not_authorized", "grant_not_connected", "missing: 2"] {
        assert!(
            stdout.contains(fragment),
            "{fragment} missing from:\n{stdout}"
        );
    }

    // Without the option the report is that of `--grants` (REQ-CREDREADY-18).
    let unrequired = deployment.doctor_grants(&tree, true, &["--runner-id", RUNNER_ID]);

    assert_eq!(unrequired.status.code(), Some(0), "{unrequired:?}");
    let unrequired = json_report(&unrequired);
    for kind in ["github", "linear", "claude_code"] {
        for field in ["state", "code", "cloud", "nextAction"] {
            assert_eq!(
                requirement(&unrequired, kind)[field],
                requirement(&offline, kind)[field],
                "{kind} {field}"
            );
        }
    }
    assert_eq!(unrequired["summary"], offline["summary"]);
    assert_only_grants_lists(deployment, 3);
}

// REQ-CREDREADY-26
#[test]
fn runner_doctor_require_grants_fails_on_a_grant_that_needs_reconnecting_or_is_not_ready() {
    let tree = require_grants_tree();
    let github = || oauth_grant(OAUTH_GRANT_ID, "github", "active");
    let linear = || linear_grant_for(RUNNER_ID);
    let cases = [
        (
            vec![
                github(),
                oauth_grant(LINEAR_GRANT_ID, "linear", "reconnect_required"),
                claude_code_grant(Some(runner_report("ready", Some("accepted")))),
            ],
            "linear",
            "reconnect_required",
            "grant_reconnect_required",
            "reconnect",
            "um grant create linear --replace",
        ),
        (
            vec![
                github(),
                linear(),
                claude_code_grant(Some(runner_report("install_failed", None))),
            ],
            "claude_code",
            "missing",
            "grant_not_ready",
            "sync_runner",
            "um runner grant sync",
        ),
        // The runner's service has a login of its own, which a step would run on.
        (
            vec![
                github(),
                linear(),
                claude_code_grant(Some(runner_report("foreign_login", None))),
            ],
            "claude_code",
            "missing",
            "grant_not_ready",
            "resolve_service_login",
            "",
        ),
        // A grant authorized for the runner is reported before one that is not.
        (
            vec![
                github(),
                linear(),
                unauthorized_claude_code_grant(),
                claude_code_grant(Some(runner_report("install_failed", None))),
            ],
            "claude_code",
            "missing",
            "grant_not_ready",
            "sync_runner",
            "um runner grant sync",
        ),
    ];
    for (grants, kind, state, code, next_action, command) in cases {
        let deployment = Deployment::signed_in(vec![page(&grants, None)]);

        let output =
            deployment.doctor_grants(&tree, true, &["--runner-id", RUNNER_ID, "--require-grants"]);

        assert_eq!(output.status.code(), Some(1), "{kind} {code}: {output:?}");
        let report = json_report(&output);
        assert_grant_fails(&report, kind, state, code, next_action, command);
        for other in ["github", "linear", "claude_code"] {
            if other != kind {
                assert_eq!(
                    requirement(&report, other)["state"],
                    "unverified",
                    "{kind} {code}: {other}"
                );
            }
        }
        assert_only_grants_lists(deployment, 1);
    }
}

// REQ-CREDREADY-26
#[test]
fn runner_doctor_require_grants_passes_grants_the_runner_can_use() {
    let tree = require_grants_tree();
    let offline = json_report(&doctor(&tree, true));
    // Available, unverified at its generation and pending the runner's report.
    for claude_code in [
        claude_code_grant(Some(runner_report("ready", Some("accepted")))),
        claude_code_grant(Some(runner_report("ready", None))),
        claude_code_grant(None),
    ] {
        let grants = page(
            &[
                linear_grant_for(ENDED_RUNNER_ID),
                oauth_grant(OAUTH_GRANT_ID, "github", "active"),
                linear_grant_for(RUNNER_ID),
                claude_code,
            ],
            None,
        );
        let deployment = Deployment::signed_in(vec![grants]);

        let output =
            deployment.doctor_grants(&tree, true, &["--runner-id", RUNNER_ID, "--require-grants"]);

        assert_eq!(output.status.code(), Some(0), "{output:?}");
        let report = json_report(&output);
        for kind in ["github", "linear", "claude_code"] {
            for field in ["state", "code", "cloud", "nextAction"] {
                assert_eq!(
                    requirement(&report, kind)[field],
                    requirement(&offline, kind)[field],
                    "{kind} {field}"
                );
            }
        }
        assert_eq!(report["summary"], offline["summary"]);
        assert_only_grants_lists(deployment, 1);
    }
}

// REQ-CREDREADY-26, REQ-CREDREADY-17
#[test]
fn runner_doctor_require_grants_keeps_a_failing_readiness_code() {
    let tree = single_file_tree(LINEAR_WORKFLOW);
    let deployment = Deployment::signed_in(vec![
        connection_page(&[], None),
        page(&[linear_grant_for(ENDED_RUNNER_ID)], None),
    ]);

    // An organization named by ID is not read before the member's grants.
    let output = deployment.run_doctor(
        &deployment.server.api_url,
        &tree,
        true,
        &[
            &["--readiness", "--organization-id", ORGANIZATION_ID],
            &["--grants", "--runner-id", RUNNER_ID, "--require-grants"],
        ],
        &[],
    );

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let report = json_report(&output);
    let linear = requirement(&report, "linear");
    assert_eq!(linear["state"], "missing");
    assert_eq!(linear["code"], "not_connected");
    assert!(
        next_action(linear).contains("um connection linear authorization create"),
        "{linear}"
    );
    assert_eq!(
        requirement(&report, "claude_code")["code"],
        "grant_not_connected"
    );
}

// REQ-CREDREADY-26
#[test]
fn runner_doctor_require_grants_needs_the_grants_read_on_a_runner() {
    let tree = require_grants_tree();
    let deployment = Deployment::signed_in(Vec::new());
    let cases: [&[&str]; 3] = [
        &["--require-grants"],
        &["--grants", "--require-grants"],
        &["--connect", "--runner-id", RUNNER_ID, "--require-grants"],
    ];
    for options in cases {
        let output =
            deployment.run_doctor(&deployment.server.api_url, &tree, true, &[options], &[]);

        assert_eq!(output.status.code(), Some(2), "{options:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{options:?}");
    }
    assert!(deployment.server.finish().is_empty());
}

// REQ-CREDREADY-22
#[test]
fn runner_doctor_grants_ends_without_a_report_when_not_signed_in() {
    let tree = grants_tree();
    let signed_out = Deployment::new(Vec::new(), false);

    let output = signed_out.doctor_grants(&tree, true, &[]);

    assert_eq!(output.status.code(), Some(3), "{output:?}");
    let failure = json_report(&output);
    assert_eq!(failure["outcome"], "unauthenticated");
    assert!(failure.get("requirements").is_none(), "{failure}");
    let human = signed_out.doctor_grants(&tree, false, &[]);
    assert_eq!(human.status.code(), Some(3), "{human:?}");
    assert!(human.stdout.is_empty(), "{human:?}");
    assert!(signed_out.server.finish().is_empty());
}

// REQ-CREDREADY-22
#[test]
fn runner_doctor_grants_asks_nothing_when_no_kind_is_needed() {
    let tree = single_file_tree(
        r#"schemaVersion: 1
steps:
  build:
    kind: cmd
    command:
      argv: ["./build"]
"#,
    );
    let deployment = Deployment::signed_in(Vec::new());

    let output = deployment.doctor_grants(&tree, true, &[]);

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let report = json_report(&output);
    assert_eq!(report["requirements"], json!([]));
    assert_eq!(report["deployment"], Value::Null);
    assert!(deployment.server.finish().is_empty());
}

// REQ-CREDREADY-7
#[test]
fn runner_doctor_workflow_names_the_grant_commands_for_each_coding_agent() {
    let report = json_report(&doctor(&WorkflowTree::new(), true));

    for kind in ["claude_code", "codex"] {
        let next_action = next_action(requirement(&report, kind));
        assert!(
            next_action.contains(&format!("um grant create {kind}")),
            "{kind}: {next_action}"
        );
        assert!(
            next_action.contains("um grant authorize"),
            "{kind}: {next_action}"
        );
    }
}
