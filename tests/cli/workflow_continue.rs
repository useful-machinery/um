use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use super::workflow_run::{
    RunBundle, TreeBundle, git, initialize_git_repository, isolated_command, run_record,
    workflow_run_schema,
};
use super::workflow_run_control::run_control;
use super::workflow_run_list::shown_plain;

pub(super) const GIT_CONTINUATION_SOURCE: &str = r#"schemaVersion: 1
steps:
  capture:
    kind: cmd
    command: {argv: ["true"]}
    outputs:
      branch: {kind: git_branch, from: workspace}
  check:
    kind: cmd
    dependsOn: [capture]
    command: {argv: ["sh", "-c", "test \"$PHASE\" = continuation"]}
exports:
  branch: {ref: outputs.capture.branch}
"#;

fn continue_args(run: &Path, from: &[&str]) -> Vec<String> {
    let mut args = vec![
        "workflow".to_owned(),
        "run".to_owned(),
        "resume".to_owned(),
        run.to_string_lossy().into_owned(),
    ];
    for id in from {
        args.extend(["--from".to_owned(), (*id).to_owned()]);
    }
    args.push("--json".to_owned());
    args
}

// REQ-WFFORMAT-84, REQ-CONTINUE-9, REQ-CONTINUE-15: a child's admitted empty JSON
// survives the retained-record round trip without applying its object schema to null.
#[test]
fn continuation_preserves_an_omitted_schema_constrained_child_input() {
    let bundle = TreeBundle::new(
        "schemaVersion: 1\nsteps:\n  build:\n    kind: workflow\n    workflow: child.yaml\n    on:\n      succeeded: exit.succeeded\n      failed: exit.failed\n",
    );
    bundle.write_source(
        "findings.schema.json",
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
    );
    bundle.write_source(
        "child.yaml",
        r#"schemaVersion: 1
inputs:
  findings: {kind: json, optional: true, schema: findings.schema.json}
steps:
  prepare:
    kind: cmd
    command:
      argv: ["sh", "-c", "printf prepared > prepared.txt"]
    outputs:
      text: {kind: text, from: path, path: prepared.txt}
  gate:
    kind: cmd
    dependsOn: [prepare]
    inputs:
      prior: {ref: outputs.prepare.text}
      findings: {ref: inputs.findings}
    command:
      argv:
        - sh
        - -c
        - 'test -e ready && test "$(cat "$UM_STEP_INPUTS/values/findings")" = null && cp "$UM_STEP_INPUTS/values/prior" final.txt'
"#,
    );
    let run = bundle.result("optional-child");
    let initial = isolated_command(&bundle.args(&run)).output().unwrap();
    assert_eq!(initial.status.code(), Some(0), "{initial:?}");
    assert_eq!(run_record(&run)["outcome"], "failed");
    let child = run.join("children/build/000001");
    let manifest_path = child.join("workflow/manifest.json");
    let original_manifest = fs::read(&manifest_path).unwrap();
    fs::write(bundle.execution_root().join("prepared.txt"), "changed").unwrap();
    fs::write(bundle.execution_root().join("ready"), "").unwrap();

    let output = isolated_command(&continue_args(&child, &["gate"]))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("final.txt")).unwrap(),
        "prepared"
    );
    assert_eq!(fs::read(manifest_path).unwrap(), original_manifest);
    let result = read_json(&child.join("attempts/000002/result/result.json"));
    assert_eq!(result["outcome"], "succeeded", "{result}");
    assert_eq!(result["steps"][0]["state"], "inherited", "{result}");

    // An explicit null remains invalid during fresh admission.
    let explicit_run = bundle.result("explicit-null");
    let mut explicit = bundle.args_for(&explicit_run, "child.yaml");
    explicit.extend([
        "--input-json".to_owned(),
        "findings".to_owned(),
        "null".to_owned(),
    ]);
    let output = isolated_command(&explicit).output().unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let rejected: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rejected["outcome"], "rejected", "{rejected}");
    assert!(!explicit_run.join("state.json").exists());
}

// REQ-CONTINUE-9, REQ-CONTINUE-10: recovering an omitted empty value must not
// waive a replacement's changed schema, requiredness or default.
#[test]
fn continuation_rejects_incompatible_replacements_of_optional_json_inputs() {
    for (name, original_schema, explicit_null, replacement_declaration, replacement_schema) in [
        (
            "required",
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
            false,
            "kind: json, schema: findings.schema.json",
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
        ),
        (
            "new-default",
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
            false,
            "kind: json, default: {}, schema: findings.schema.json",
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
        ),
        (
            "changed-schema",
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
            false,
            "kind: json, optional: true, schema: findings.schema.json",
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}"#,
        ),
        (
            "explicit-null",
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":["object","null"]}"#,
            true,
            "kind: json, optional: true, schema: findings.schema.json",
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
        ),
    ] {
        let bundle = RunBundle::new(
            "schemaVersion: 1\ninputs:\n  findings: {kind: json, optional: true, schema: findings.schema.json}\nsteps:\n  fail:\n    kind: cmd\n    command: {argv: ['false']}\n",
        );
        let schema_path = bundle.source_root().join("findings.schema.json");
        fs::write(&schema_path, original_schema).unwrap();
        let run = bundle.result(name);
        let mut initial = bundle.args(&run);
        if explicit_null {
            initial.extend([
                "--input-json".to_owned(),
                "findings".to_owned(),
                "null".to_owned(),
            ]);
        }
        let output = isolated_command(&initial).output().unwrap();
        assert_eq!(output.status.code(), Some(1), "{name}: {output:?}");
        assert!(run.join("state.json").exists(), "{name}: {output:?}");

        fs::write(schema_path, replacement_schema).unwrap();
        let replacement = bundle.source_root().join("candidate.yaml");
        fs::write(
            &replacement,
            format!(
                "schemaVersion: 1\ninputs:\n  findings: {{{replacement_declaration}}}\nsteps:\n  fail:\n    kind: cmd\n    command: {{argv: ['false']}}\n"
            ),
        )
        .unwrap();
        let mut request = continue_args(&run, &["fail"]);
        request.extend([
            "--workflow".to_owned(),
            replacement.to_string_lossy().into_owned(),
        ]);
        let output = isolated_command(&request).output().unwrap();
        assert_eq!(output.status.code(), Some(1), "{name}: {output:?}");
        let rejected: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            rejected["diagnostics"].as_array().unwrap().iter().any(|d| {
                d["code"] == "continuation_inputs_unsatisfied"
                    && d["location"]["input"] == "findings"
            }),
            "{name}: {rejected}"
        );
        assert_eq!(
            read_json(&run.join("state.json"))["attempts"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
}

// REQ-CONTINUE-10, REQ-CONTINUE-13
#[test]
fn continuation_collects_all_missing_candidate_harness_profiles() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    fs::write(
        bundle.source_root().join("system.md"),
        "You are a test agent.",
    )
    .unwrap();
    fs::write(bundle.source_root().join("message.md"), "Act.").unwrap();
    let replacement = bundle.source_root().join("candidate.yaml");
    fs::write(
        &replacement,
        r#"schemaVersion: 1
agentProfiles:
  pi:
    harness:
      kind: pi
      config:
        model: fixture/model
        thinking: off
  codex:
    harness:
      kind: codex
      config:
        model: fixture/codex
        effort: high
steps:
  first:
    kind: cmd
    command: {argv: ["false"]}
    outputs:
      branch: {kind: git_branch, from: workspace}
  pi:
    kind: agent
    agent:
      profile: pi
      systemPrompt: system.md
      message:
        text:
          - file: message.md
  codex:
    kind: agent
    agent:
      profile: codex
      systemPrompt: system.md
      message:
        text:
          - file: message.md
"#,
    )
    .unwrap();
    let run = bundle.result("all-harnesses");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let mut request = continue_args(&run, &["first"]);
    request.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&request)
        .env("PATH", "/nonexistent")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let profiles = result["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["location"]["kind"] == "agent_harness")
        .map(|d| d["location"]["profile"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(profiles.len(), 2, "{result}");
    assert!(profiles.contains(&"PiJsonV1"));
    assert!(profiles.contains(&"CodexAppServerV1"));
    assert!(
        result["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|diagnostic| diagnostic["code"]
                .as_str()
                .is_some_and(|code| code.starts_with("git_"))),
        "{result}"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

// REQ-CONTINUE-9, REQ-CONTINUE-10
#[test]
fn continuation_reports_every_immutable_input_constraint_failure() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
inputs:
  first: {kind: json, schema: first.schema.json}
  second: {kind: json, schema: second.schema.json}
steps:
  fail:
    kind: cmd
    command: {argv: ["false"]}
"#,
    );
    for name in ["first.schema.json", "second.schema.json"] {
        fs::write(
            bundle.source_root().join(name),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
        )
        .unwrap();
    }
    let run = bundle.result("input-constraint-aggregation");
    let mut initial = bundle.args(&run);
    let position = initial.len() - 1;
    initial.splice(
        position..position,
        [
            "--input-json".to_owned(),
            "first".to_owned(),
            r#"{"value":1}"#.to_owned(),
            "--input-json".to_owned(),
            "second".to_owned(),
            r#"{"value":2}"#.to_owned(),
            "--json".to_owned(),
        ],
    );
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    for name in ["first.schema.json", "second.schema.json"] {
        fs::write(
            bundle.source_root().join(name),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}"#,
        )
        .unwrap();
    }
    fs::write(
        bundle.source_root().join("main.yaml"),
        r#"schemaVersion: 1
inputs:
  first: {kind: json, schema: first.schema.json}
  second: {kind: json, schema: second.schema.json}
  third: {kind: text}
steps:
  fail:
    kind: cmd
    command: {argv: ["false"]}
"#,
    )
    .unwrap();
    let mut request = continue_args(&run, &["fail"]);
    request.extend([
        "--workflow".to_owned(),
        bundle
            .source_root()
            .join("main.yaml")
            .to_string_lossy()
            .into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let diagnostics = result["diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 3, "{result}");
    assert!(
        diagnostics
            .iter()
            .all(|diagnostic| { diagnostic["code"] == "continuation_inputs_unsatisfied" })
    );
    assert_eq!(
        diagnostics
            .iter()
            .map(|diagnostic| diagnostic["location"]["input"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["first", "second", "third"]
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

// REQ-CONTINUE-27
#[test]
fn continuation_acquires_after_pre_rename_publication_crash() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  fail:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run = bundle.result("pre-rename-crash");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    fs::remove_dir_all(run.join("attempts/000001/result")).unwrap();
    let mut state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    state["attempts"][0]["result"] =
        serde_json::json!({"status": "not_published", "reason": "publication_pending"});
    let mut bytes = serde_json::to_vec_pretty(&state).unwrap();
    bytes.push(b'\n');
    fs::write(run.join("state.json"), bytes).unwrap();

    let replacement = bundle.source_root().join("invalid.yaml");
    fs::write(&replacement, "schemaVersion: 2\nsteps: {}\n").unwrap();
    let mut request = continue_args(&run, &["fail"]);
    request.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["phase"], "continuation");
    assert_eq!(
        result["diagnostics"][0]["code"],
        "continuation_definition_invalid"
    );
}

// REQ-CONTINUE-1, REQ-CONTINUE-27, REQ-CONTINUE-28
#[test]
fn replacement_resolution_failure_uses_the_continuation_diagnostic_vocabulary() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  fail:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run = bundle.result("invalid-replacement-definition");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("invalid.yaml");
    fs::write(&replacement, "schemaVersion: 2\nsteps: {}\n").unwrap();
    let mut request = continue_args(&run, &["fail"]);
    request.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(workflow_run_schema().is_valid(&result), "{result}");
    assert_eq!(result["command"], "um workflow run resume");
    assert_eq!(result["phase"], "continuation");
    assert_eq!(
        result["diagnostics"][0]["code"],
        "continuation_definition_invalid"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

// REQ-CONTINUE-10, REQ-CONTINUE-28
#[test]
fn invalid_selection_keeps_independent_immutable_input_diagnostics() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  fail:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run = bundle.result("invalid-selection-and-input");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("replacement.yaml");
    fs::write(
        &replacement,
        "schemaVersion: 1\ninputs:\n  required: {kind: text}\nsteps:\n  fail:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
    )
    .unwrap();
    let mut request = continue_args(&run, &["missing"]);
    request.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(workflow_run_schema().is_valid(&result), "{result}");
    assert_eq!(result["diagnostics"].as_array().unwrap().len(), 2);
    assert_eq!(
        result["diagnostics"][0]["code"],
        "continuation_definition_invalid"
    );
    assert_eq!(
        result["diagnostics"][1]["code"],
        "continuation_inputs_unsatisfied"
    );
    assert_eq!(result["diagnostics"][1]["location"]["input"], "required");
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

#[test]
fn changed_inherited_json_schema_does_not_revalidate_producer_evidence() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "printf '{\"value\":1}' > value.json"]}
    outputs:
      value: {kind: json, from: path, path: value.json, schema: value.schema.json}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["false"]}
"#,
    );
    fs::write(
        bundle.source_root().join("value.schema.json"),
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
    )
    .unwrap();
    let run = bundle.result("changed-json-schema");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    fs::write(
        bundle.source_root().join("value.schema.json"),
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}"#,
    )
    .unwrap();
    fs::write(
        bundle.source_root().join("main.yaml"),
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "printf '{\"value\":1}' > value.json"]}
    outputs:
      value: {kind: json, from: path, path: value.json, schema: value.schema.json}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["true"]}
"#,
    )
    .unwrap();
    let mut request = continue_args(&run, &["consumer"]);
    request.extend([
        "--workflow".to_owned(),
        bundle
            .source_root()
            .join("main.yaml")
            .to_string_lossy()
            .into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let attempt = &state["attempts"][1];
    assert_eq!(attempt["progress"]["steps"][0]["state"], "inherited");
    assert_eq!(
        attempt["continuation"]["inheritedSteps"][0]["definitionChanged"],
        true
    );
}

#[test]
fn chained_inheritance_rejects_a_referenced_output_without_retained_evidence() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n  middle:\n    kind: cmd\n    dependsOn: [producer]\n    command: {argv: [\"false\"]}\n",
    );
    let workflow_path = bundle.source_root().join("main.yaml");
    let run = bundle.result("missing-chained-output");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    fs::write(
        &workflow_path,
        "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      value: {kind: text, from: path, path: value.txt}\n  middle:\n    kind: cmd\n    dependsOn: [producer]\n    command: {argv: [\"false\"]}\n",
    )
    .unwrap();
    let mut second = continue_args(&run, &["middle"]);
    second.extend([
        "--workflow".to_owned(),
        workflow_path.to_string_lossy().into_owned(),
    ]);
    assert_eq!(
        isolated_command(&second).output().unwrap().status.code(),
        Some(1)
    );
    fs::write(
        &workflow_path,
        "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      value: {kind: text, from: path, path: value.txt}\n  consumer:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.producer.value}\n    command: {argv: [\"true\"]}\n",
    )
    .unwrap();
    let before = fs::read(run.join("state.json")).unwrap();
    let mut third = continue_args(&run, &["consumer"]);
    third.extend([
        "--workflow".to_owned(),
        workflow_path.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&third).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        result["diagnostics"][0]["code"],
        "continuation_references_unsatisfied"
    );
    assert_eq!(
        result["diagnostics"][0]["location"]["reference"],
        "outputs.producer.value"
    );
    assert_eq!(fs::read(run.join("state.json")).unwrap(), before);
    assert!(!run.join("attempts/000003").exists());
}

#[test]
fn chained_continuation_authenticates_the_original_producer_closure_before_claim() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  bootstrap:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run = bundle.result("historical-producer-closure");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("replacement.yaml");
    fs::write(
        bundle.source_root().join("value.schema.json"),
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
    )
    .unwrap();
    fs::write(
        &replacement,
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "printf '{\"value\":1}' > value.json"]}
    outputs:
      value: {kind: json, from: path, path: value.json, schema: value.schema.json}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["false"]}
"#,
    )
    .unwrap();
    let mut second = continue_args(&run, &["producer"]);
    second.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    assert_eq!(
        isolated_command(&second).output().unwrap().status.code(),
        Some(1)
    );
    let mut third = continue_args(&run, &["consumer"]);
    third.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    assert_eq!(
        isolated_command(&third).output().unwrap().status.code(),
        Some(1)
    );

    fs::remove_file(run.join("attempts/000002/workflow/manifest.json")).unwrap();
    let before = fs::read(run.join("state.json")).unwrap();
    let output = isolated_command(&third).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(fs::read(run.join("state.json")).unwrap(), before);
    assert!(!run.join("attempts/000004").exists());
}

// REQ-CONTINUE-8, REQ-CONTINUE-10
#[test]
fn continuation_accumulates_inheritance_and_executed_git_admission_failures() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "echo value > value.txt"]}
    outputs:
      value: {kind: text, from: path, path: value.txt}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["false"]}
"#,
    );
    fs::write(
        bundle.source_root().join("alternate.yaml"),
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "echo value > value.txt"]}
    outputs:
      value: {kind: file, from: path, path: value.txt, mediaType: text/plain}
  consumer:
    kind: cmd
    inputs:
      value: {ref: outputs.producer.value}
    command: {argv: ["true"]}
    outputs:
      branch: {kind: git_branch, from: workspace}
"#,
    )
    .unwrap();
    let run = bundle.result("admission-aggregation");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let mut request = continue_args(&run, &["consumer"]);
    request.extend([
        "--workflow".to_owned(),
        bundle
            .source_root()
            .join("alternate.yaml")
            .to_string_lossy()
            .into_owned(),
    ]);
    let output = isolated_command(&request).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let codes = result["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["code"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(
        codes.contains(&"continuation_references_unsatisfied"),
        "{codes:?}"
    );
    assert!(
        codes.iter().any(|code| code.starts_with("git_")),
        "{codes:?}"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["attempts"].as_array().unwrap().len(), 1);
}

// REQ-CONTINUE-27
#[test]
fn invalid_selection_does_not_claim_an_attempt() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: {argv: [\"false\"]}\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
    );
    let run = bundle.result("invalid-selection");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let before = fs::read(run.join("state.json")).unwrap();
    for selected in [
        &["missing"][..],
        &["cleanup"][..],
        &["work", "work"][..],
        &["zzz", "aaa"][..],
    ] {
        let output = isolated_command(&continue_args(&run, selected))
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let diagnostic: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(
            diagnostic["diagnostics"][0]["code"],
            "continuation_definition_invalid"
        );
        if selected.len() == 2 && selected[0] == "zzz" {
            assert_eq!(diagnostic["diagnostics"][0]["location"]["id"], "aaa");
            assert_eq!(diagnostic["diagnostics"][1]["location"]["id"], "zzz");
        }
        assert_eq!(fs::read(run.join("state.json")).unwrap(), before);
        assert!(!run.join("attempts/000002").exists());
    }
}

// REQ-CONTINUE-17
#[test]
fn inherited_zero_delta_git_keeps_original_producer_without_carrier() {
    let bundle = RunBundle::new(GIT_CONTINUATION_SOURCE);
    initialize_git_repository(bundle.execution_root());
    let run = bundle.result("inherited-zero-delta");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let continued = isolated_command(&continue_args(&run, &["check"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert!(
        continued.status.success(),
        "{} {}",
        String::from_utf8_lossy(&continued.stdout),
        String::from_utf8_lossy(&continued.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000002/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(result["exports"]["branch"]["provenance"], "inherited");
    assert_eq!(result["exports"]["branch"]["producer"]["attemptNumber"], 1);
    assert!(result["exports"]["branch"]["carrier"].is_null());
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    assert!(state["attempts"][1]["progress"]["steps"][0]["outputs"][0]["carrier"].is_null());
    let view = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "view".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(view.status.success());
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-view-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&view));
}

// REQ-CONTINUE-18
#[test]
fn continuation_git_capture_uses_initial_baseline_after_head_advances() {
    let bundle = RunBundle::new(GIT_CONTINUATION_SOURCE);
    initialize_git_repository(bundle.execution_root());
    let baseline = git(bundle.execution_root(), &["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    let run = bundle.result("git-baseline");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    let first = isolated_command(&initial)
        .env("PHASE", "initial")
        .output()
        .unwrap();
    assert_eq!(
        first.status.code(),
        Some(1),
        "{} {}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    fs::write(bundle.execution_root().join("tracked.txt"), "advanced\n").unwrap();
    git(bundle.execution_root(), &["add", "tracked.txt"]);
    git(
        bundle.execution_root(),
        &["commit", "--quiet", "-m", "advance"],
    );
    let head = git(bundle.execution_root(), &["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    assert_ne!(baseline, head);
    let resumed = isolated_command(&continue_args(&run, &["capture"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert!(
        resumed.status.success(),
        "{} {}",
        String::from_utf8_lossy(&resumed.stdout),
        String::from_utf8_lossy(&resumed.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000002/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(result["exports"]["branch"]["baseOid"], baseline);
    assert_eq!(result["exports"]["branch"]["headOid"], head);
    assert!(result["exports"]["branch"]["carrier"].is_object());
}

// REQ-CONTINUE-5, REQ-CONTINUE-14
#[test]
fn retry_after_failed_continuation_reruns_the_initial_definition_without_context() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  first:
    kind: cmd
    command: {argv: ["sh", "-c", "test -z \"${UM_CONTINUATION_CONTEXT:-}\"; echo first >> calls"]}
  second:
    kind: cmd
    dependsOn: [first]
    command: {argv: ["sh", "-c", "if test \"$PHASE\" = continued; then test -r \"$UM_CONTINUATION_CONTEXT\"; exit 75; fi; test -z \"${UM_CONTINUATION_CONTEXT:-}\"; test \"$PHASE\" = retry"]}
finalizers:
  cleanup:
    kind: cmd
    command: {argv: ["sh", "-c", "echo cleanup >> calls"]}
"#,
    );
    let run = bundle.result("retry-after-continuation");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("replacement.yaml");
    fs::write(
        &replacement,
        r#"schemaVersion: 1
steps:
  first:
    kind: cmd
    command: {argv: ["sh", "-c", "echo replacement >> calls"]}
  second:
    kind: cmd
    dependsOn: [first]
    command: {argv: ["sh", "-c", "test -r \"$UM_CONTINUATION_CONTEXT\"; exit 75"]}
finalizers:
  cleanup:
    kind: cmd
    command: {argv: ["sh", "-c", "echo replacement-cleanup >> calls"]}
"#,
    )
    .unwrap();
    let mut continuation = continue_args(&run, &["second"]);
    continuation.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    assert_eq!(
        isolated_command(&continuation)
            .env("PHASE", "continued")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let retry = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "retry".to_owned(),
        run.to_string_lossy().into_owned(),
        "--execution-root".to_owned(),
        bundle.execution_root().to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .env("PHASE", "retry")
    .output()
    .unwrap();
    assert!(
        retry.status.success(),
        "{} {}",
        String::from_utf8_lossy(&retry.stdout),
        String::from_utf8_lossy(&retry.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let last = &state["attempts"][2];
    assert_eq!(last["trigger"], "explicit_retry");
    assert!(last["continuation"].is_null());
    assert_eq!(last["progress"]["steps"][0]["state"], "succeeded");
    assert_eq!(last["progress"]["steps"][1]["state"], "succeeded");
    assert_eq!(last["finalization"]["finalizers"][0]["state"], "succeeded");
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("calls")).unwrap(),
        "first\ncleanup\nreplacement-cleanup\nfirst\ncleanup\n"
    );
}

// REQ-CONTINUE-16
#[test]
fn inherited_json_and_file_values_stage_from_original_producer() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  producer:
    kind: cmd
    command: {argv: ["sh", "-c", "echo '{\"ok\":true}' > data.json; echo artifact > file.txt"]}
    outputs:
      data: {kind: json, from: path, path: data.json, schema: data.schema.json}
      document: {kind: file, from: path, path: file.txt, mediaType: text/plain}
  consume:
    kind: cmd
    inputs:
      data: {ref: outputs.producer.data}
      document: {ref: outputs.producer.document}
    command:
      argv: ["sh", "-c", "test -r $UM_STEP_INPUTS/values/data; test -r $UM_STEP_INPUTS/values/document; test $PHASE = continuation"]
exports:
  retainedDocument: {ref: outputs.producer.document}
"#,
    );
    fs::write(bundle.source_root().join("data.schema.json"), r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["ok"],"properties":{"ok":{"const":true}}}"#).unwrap();
    let run = bundle.result("json-file");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let output = isolated_command(&continue_args(&run, &["consume"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    for descriptor in state["attempts"][1]["progress"]["steps"][0]["outputs"]
        .as_array()
        .unwrap()
    {
        assert_eq!(descriptor["producer"]["attemptNumber"], 1);
    }
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000002/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(
        result["exports"]["retainedDocument"]["provenance"],
        "inherited"
    );
    assert_eq!(
        result["exports"]["retainedDocument"]["producer"]["attemptNumber"],
        1
    );
}

// REQ-WFRECOVERY-55
#[test]
fn continuation_recovery_handler_reads_bound_context_and_starts_fresh() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  retained:
    kind: cmd
    command: {argv: ["true"]}
  repairable:
    kind: cmd
    dependsOn: [retained]
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv:
            - /bin/sh
            - -c
            - |
              set -eu
              if test "$PHASE" = resumed; then
                test -r "$UM_CONTINUATION_CONTEXT"
                : > repaired
              else
                test -z "${UM_CONTINUATION_CONTEXT:-}"
              fi
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"Retried after preparation.","reason":"Recheck current state."}' > "$UM_RECOVERY_RESULT"
    command:
      argv:
        - /bin/sh
        - -c
        - |
          set -eu
          if test "$PHASE" = resumed; then
            test -r "$UM_CONTINUATION_CONTEXT"
            test -f repaired
          else
            test -z "${UM_CONTINUATION_CONTEXT:-}"
            exit 75
          fi
"#,
    );
    let run = bundle.result("recovery-context");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let output = isolated_command(&continue_args(&run, &["repairable"]))
        .env("PHASE", "resumed")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let attempt = &state["attempts"][1];
    assert_eq!(attempt["progress"]["steps"][0]["state"], "inherited");
    assert_eq!(attempt["progress"]["steps"][1]["state"], "succeeded");
    assert_eq!(
        attempt["progress"]["steps"][1]["recovery"]["configuredRetries"],
        1
    );
}

// REQ-CONTINUE-11, REQ-CONTINUE-12, REQ-CONTINUE-23, REQ-CONTINUE-25
#[test]
fn replacement_definition_and_root_are_selected_independently() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  prior:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"echo original >> calls\"]}\n  target:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run = bundle.result("replacement");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial).output().unwrap().status.code(),
        Some(1)
    );
    let replacement = bundle.source_root().join("replacement.yaml");
    fs::write(&replacement, "schemaVersion: 1\nsteps:\n  prior:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"echo changed >> calls\"]}\n  target:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"echo target >> calls\"]}\n").unwrap();
    let normalized_replacement = fs::canonicalize(&replacement).unwrap();
    let new_root = bundle.result("other-root");
    fs::create_dir(&new_root).unwrap();
    let normalized_new_root = fs::canonicalize(&new_root).unwrap();
    let mut args = continue_args(&run, &["target"]);
    args.extend([
        "--workflow".to_owned(),
        replacement.to_string_lossy().into_owned(),
        "--execution-root".to_owned(),
        new_root.to_string_lossy().into_owned(),
    ]);
    let output = isolated_command(&args).output().unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let attempt = &state["attempts"][1];
    assert_eq!(
        attempt["continuation"]["definitionSource"]["kind"],
        "replaced"
    );
    assert_eq!(
        attempt["continuation"]["inheritedSteps"][0]["definitionChanged"],
        true
    );
    assert_eq!(attempt["continuation"]["workspace"]["modified"], "unknown");
    assert_eq!(
        attempt["continuation"]["workspace"]["executionRoot"],
        normalized_new_root.to_str().unwrap()
    );
    assert_eq!(
        attempt["continuation"]["request"]["definition"]["replaced"]["path"],
        normalized_replacement.to_str().unwrap()
    );
    assert!(run.join("attempts/000002/workflow/manifest.json").exists());
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("calls")).unwrap(),
        "original\n"
    );
    assert_eq!(
        fs::read_to_string(new_root.join("calls")).unwrap(),
        "target\n"
    );
    let status = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "status".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-status-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert!(
        jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&status)
    );
}

// REQ-WFFINALIZE-22
// REQ-CONTINUE-5, REQ-CONTINUE-15, REQ-CONTINUE-16, REQ-CONTINUE-17
#[test]
fn chained_continuation_keeps_original_producer_and_reruns_finalizers() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"echo producer >> calls; echo inherited > output.txt\"]}\n    outputs:\n      value: {kind: text, from: path, path: output.txt}\n  consumer:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.producer.value}\n    command: {argv: [\"sh\", \"-c\", \"echo consumer >> calls; test \\\"$PHASE\\\" = final\"]}\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"if test \\\"$PHASE\\\" != initial; then test -r \\\"$UM_CONTINUATION_CONTEXT\\\"; else test -z ${UM_CONTINUATION_CONTEXT:-}; fi; echo cleanup >> calls\"]}\nexports:\n  retained:\n    ref: outputs.producer.value\n",
    );
    let run = bundle.result("chain");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    assert_eq!(
        isolated_command(&initial)
            .env("PHASE", "initial")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    assert_eq!(
        isolated_command(&continue_args(&run, &["consumer"]))
            .env("PHASE", "again")
            .output()
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    let last = isolated_command(&continue_args(&run, &["consumer"]))
        .env("PHASE", "final")
        .output()
        .unwrap();
    assert!(
        last.status.success(),
        "{} {}",
        String::from_utf8_lossy(&last.stdout),
        String::from_utf8_lossy(&last.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let attempts = state["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts[2]["continuation"]["inheritedSteps"][0]["priorState"],
        "inherited"
    );
    assert_eq!(
        attempts[2]["progress"]["steps"][0]["detail"]["priorAttemptNumber"],
        2
    );
    assert_eq!(
        attempts[2]["progress"]["steps"][0]["outputs"][0]["producer"]["attemptNumber"],
        1
    );
    assert_eq!(
        attempts[2]["finalization"]["finalizers"][0]["state"],
        "succeeded"
    );
    let portable: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000003/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(portable["exports"]["retained"]["provenance"], "inherited");
    assert_eq!(
        portable["exports"]["retained"]["producer"]["attemptNumber"],
        1
    );
    assert!(run.join("attempts/000003/result/exports/0001").is_file());
    let view = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "view".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(view.status.success());
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-view-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&view));
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("calls")).unwrap(),
        "producer\nconsumer\ncleanup\nconsumer\ncleanup\nconsumer\ncleanup\n"
    );
    assert!(!run.join("attempts/000002/values/step/producer").exists());
    assert!(!run.join("attempts/000003/values/step/producer").exists());
}

// REQ-CONTINUE-1, REQ-CONTINUE-2, REQ-CONTINUE-3, REQ-CONTINUE-15, REQ-CONTINUE-19, REQ-CONTINUE-20, REQ-CONTINUE-21, REQ-CONTINUE-26, REQ-CONTINUE-28, REQ-CONTINUE-29, REQ-CONTINUE-31, REQ-LOCALRUN-127
#[test]
fn workflow_continue_inherits_prior_output_and_executes_only_downstream() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"printf first > first.txt; echo first >> calls; test -z ${UM_CONTINUATION_CONTEXT:-}\"]}\n    outputs:\n      value: {kind: text, from: path, path: first.txt}\n  second:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.first.value}\n    command: {argv: [\"sh\", \"-c\", \"echo second >> calls; if test \\\"$PHASE\\\" = continuation; then test -r \\\"$UM_CONTINUATION_CONTEXT\\\"; grep -q inheritedOutputs \\\"$UM_CONTINUATION_CONTEXT\\\"; else test -z ${UM_CONTINUATION_CONTEXT:-}; false; fi\"]}\n",
    );
    let run = bundle.result("continuation");
    let mut initial = bundle.args(&run);
    initial.insert(initial.len() - 1, "--json".to_owned());
    let first = isolated_command(&initial)
        .env("PHASE", "initial")
        .output()
        .unwrap();
    assert_eq!(
        first.status.code(),
        Some(1),
        "{} {}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    let output = isolated_command(&continue_args(&run, &["second"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("state.json")).unwrap()).unwrap();
    let continuation = &state["attempts"][1];
    assert_eq!(continuation["trigger"], "continuation");
    assert_eq!(
        continuation["continuation"]["reexecutedSteps"],
        serde_json::json!(["second"])
    );
    assert_eq!(continuation["progress"]["steps"][0]["state"], "inherited");
    assert_eq!(continuation["progress"]["steps"][1]["state"], "succeeded");
    let event: serde_json::Value =
        serde_json::from_slice(output.stderr.split(|byte| *byte == b'\n').next().unwrap()).unwrap();
    assert_eq!(event["event"], "continuation_partition");
    assert_eq!(event["continuation"], continuation["continuation"]);
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(workflow_run_schema().is_valid(&terminal), "{terminal}");
    assert_eq!(terminal["command"], "um workflow run resume");
    assert_eq!(
        terminal["result"]["continuation"],
        continuation["continuation"]
    );
    let result: serde_json::Value =
        serde_json::from_slice(&fs::read(run.join("attempts/000002/result/result.json")).unwrap())
            .unwrap();
    assert_eq!(result["continuation"], continuation["continuation"]);
    assert_eq!(result["steps"][0]["state"], "inherited");
    assert!(result["steps"][0]["startedAt"].is_null());
    assert!(result["steps"][0]["durationMilliseconds"].is_null());
    assert!(result["steps"][0]["commandOutput"].is_null());
    assert!(result["steps"][0]["invocations"].is_null());
    let context: serde_json::Value = serde_json::from_slice(
        &fs::read(run.join("attempts/000002/continuation-context.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(context["continuation"], continuation["continuation"]);
    assert_eq!(
        context["inheritedOutputs"]["first"][0]["producer"]["attemptNumber"],
        1
    );
    assert_eq!(
        continuation["progress"]["steps"][0]["outputs"][0]["producer"]["attemptNumber"],
        1
    );
    let status = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "status".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(
        status["state"]["attempts"][1]["continuation"],
        continuation["continuation"]
    );
    assert_eq!(
        status["workspaceModified"],
        continuation["continuation"]["workspace"]["modified"]
    );
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-status-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let status_validator = jsonschema::validator_for(&schema).unwrap();
    assert_eq!(
        status["state"]["attempts"][1]["continuation"]["workspace"]["preparation"],
        "ready"
    );
    assert!(status_validator.is_valid(&status));
    let mut historical_status = status.clone();
    historical_status["state"]["attempts"][1]["continuation"]["workspace"]
        .as_object_mut()
        .unwrap()
        .remove("preparation");
    assert!(status_validator.is_valid(&historical_status));
    let view = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "view".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(
        view.status.success(),
        "{} {}",
        String::from_utf8_lossy(&view.stdout),
        String::from_utf8_lossy(&view.stderr)
    );
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    assert_eq!(view["result"]["continuation"], continuation["continuation"]);
    let plain = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "view".to_owned(),
        run.to_string_lossy().into_owned(),
        "--plain".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(plain.status.success());
    let plain = String::from_utf8(plain.stdout).unwrap();
    assert!(plain.contains("inherited"));
    assert!(plain.contains("workspace modified"));
    let view_schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-view-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let validator = jsonschema::validator_for(&view_schema).unwrap();
    assert_eq!(
        view["result"]["continuation"]["workspace"]["preparation"],
        "ready"
    );
    assert!(
        validator.is_valid(&view),
        "{:?}",
        validator
            .iter_errors(&view)
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
    );
    let mut historical_view = view.clone();
    historical_view["result"]["continuation"]["workspace"]
        .as_object_mut()
        .unwrap()
        .remove("preparation");
    assert!(validator.is_valid(&historical_view));
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("calls")).unwrap(),
        "first\nsecond\nsecond\n"
    );
    let before = fs::read(run.join("state.json")).unwrap();
    let rejected = isolated_command(&continue_args(&run, &["second"]))
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(1));
    let diagnostic: serde_json::Value = serde_json::from_slice(&rejected.stdout).unwrap();
    assert!(workflow_run_schema().is_valid(&diagnostic), "{diagnostic}");
    assert_eq!(
        diagnostic["diagnostics"][0]["code"],
        "continuation_disposition_ineligible"
    );
    assert_eq!(fs::read(run.join("state.json")).unwrap(), before);
}

// REQ-AGENT-59
#[test]
fn continuation_applies_the_retained_profile_replacement_to_every_attempt() {
    // The replaced profile is unused, so no harness installation is required.
    let definition = |profiles: &str| {
        format!(
            "schemaVersion: 1\nenvironmentPassthrough: [PHASE]\n{profiles}steps:\n  execute:\n    kind: cmd\n    command: {{argv: [\"sh\", \"-c\", \"test \\\"$PHASE\\\" = continuation\"]}}\n"
        )
    };
    let declared = "agentProfiles:\n  unused:\n    harness:\n      kind: pi\n      config:\n        model: fixture/model\n        thinking: low\n";
    let bundle = RunBundle::new(&definition(declared));
    let document = bundle.initial_cwd().join("profiles.yaml");
    fs::write(
        &document,
        "schemaVersion: 1\nagentProfiles:\n  unused:\n    harness:\n      kind: claude_code\n      config:\n        model: fixture/claude\n        effort: low\n",
    )
    .unwrap();
    let run = bundle.result("replaced-continuation");
    let mut initial = bundle.args(&run);
    initial.splice(
        initial.len() - 1..initial.len() - 1,
        [
            "--agent-profiles".to_owned(),
            document.to_string_lossy().into_owned(),
            "--json".to_owned(),
        ],
    );
    let first = isolated_command(&initial)
        .env("PHASE", "initial")
        .output()
        .unwrap();
    assert_eq!(first.status.code(), Some(1));
    fs::remove_file(&document).unwrap();
    let replacement = |attempt: &str| -> serde_json::Value {
        let result: serde_json::Value = serde_json::from_slice(
            &fs::read(run.join(format!("attempts/{attempt}/result/result.json"))).unwrap(),
        )
        .unwrap();
        result["workflow"]["agentProfileReplacement"].clone()
    };
    let retained = replacement("000001");
    assert_eq!(
        retained["profiles"]["unused"]["harness"]["kind"],
        "claude_code"
    );
    let continue_with = |workflow: Option<&str>, phase: &str| {
        let mut request = continue_args(&run, &["execute"]);
        if let Some(workflow) = workflow {
            let path = bundle.source_root().join(workflow);
            request.extend(["--workflow".to_owned(), path.to_string_lossy().into_owned()]);
        }
        isolated_command(&request)
            .env("PHASE", phase)
            .output()
            .unwrap()
    };

    fs::write(bundle.source_root().join("undeclared.yaml"), definition("")).unwrap();
    let rejected = continue_with(Some("undeclared.yaml"), "continuation");
    assert_eq!(rejected.status.code(), Some(1));
    let rejection: serde_json::Value = serde_json::from_slice(&rejected.stdout).unwrap();
    assert_eq!(rejection["phase"], "continuation");
    assert_eq!(
        rejection["diagnostics"][0]["code"],
        "continuation_definition_invalid"
    );
    assert!(!run.join("attempts/000002").exists());

    fs::write(
        bundle.source_root().join("replacement.yaml"),
        definition(declared),
    )
    .unwrap();
    // A failed continuation can itself be continued; each attempt reapplies the replacement.
    for (workflow, phase, status, attempt) in [
        (None, "unset", Some(1), "000002"),
        (Some("replacement.yaml"), "continuation", Some(0), "000003"),
    ] {
        let output = continue_with(workflow, phase);
        assert_eq!(
            output.status.code(),
            status,
            "{attempt}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(replacement(attempt), retained, "{attempt}");
    }
}

// REQ-LOCALRUN-171, REQ-LOCALRUN-175, REQ-CONTINUE-1, REQ-CONTINUE-28
#[test]
fn a_continuation_of_a_tree_run_verifies_and_rebinds_the_copy() {
    let script = "#!/bin/sh\nset -eu\nprintf '%s' \"$UM_WORKFLOW_ROOT\" > workflow-root\ntest \"$PHASE\" = continuation\n";
    let bundle = TreeBundle::new(
        "schemaVersion: 1\nenvironmentPassthrough: [PHASE]\nprograms: [bin]\nsteps:\n  report:\n    kind: cmd\n    command:\n      argv: [\"report\"]\n",
    );
    bundle.write_program("bin/report", script);
    let destination = bundle.result("tree-continuation");
    let output = isolated_command(&bundle.args(&destination))
        .env("PHASE", "initial")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let run_directory = fs::canonicalize(&destination).unwrap();
    let record = run_record(&run_directory);
    assert_eq!(
        (&record["state"], &record["attempts"]),
        (&"exited".into(), &1.into())
    );
    let copy = run_directory.join("workflow/tree");
    let program = copy.join("bin/report");
    let workflow_root = bundle.execution_root().join("workflow-root");
    fs::remove_file(&workflow_root).unwrap();

    fs::set_permissions(&program, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(&program, "#!/bin/sh\nexit 0\n").unwrap();
    let rejected = isolated_command(&continue_args(&run_directory, &["report"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert_eq!(rejected.status.code(), Some(1));
    let rejection: serde_json::Value = serde_json::from_slice(&rejected.stdout).unwrap();
    assert!(workflow_run_schema().is_valid(&rejection), "{rejection}");
    assert_eq!(rejection["command"], "um workflow run resume");
    assert_eq!(rejection["phase"], "tree");
    assert_eq!(rejection["diagnostics"][0]["code"], "tree_copy_modified");
    assert!(!run_directory.join("attempts/000002").exists());

    fs::write(&program, script).unwrap();
    fs::set_permissions(&program, fs::Permissions::from_mode(0o555)).unwrap();
    let output = isolated_command(&continue_args(&run_directory, &["report"]))
        .env("PHASE", "continuation")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["attemptNumber"], 2);
    assert_eq!(
        fs::read_to_string(&workflow_root).unwrap(),
        copy.to_str().unwrap()
    );
    let resumed = run_record(&run_directory);
    assert_eq!(resumed["state"], "exited");
    assert_eq!(resumed["attempts"], 2);
    assert_eq!(resumed["id"], record["id"]);
}

/// A root whose `design` waits for answers when blocked and for a repair when it fails,
/// each continuing at `design` itself with the answers given so far, whose `review` waits
/// for an approval or a hand-back to `fix` (SC-CONTINUE-7, REQ-WFFORMAT-92), and whose
/// `merge`, once admitted to the queue, waits for the pull request `design` exported to
/// leave it (REQ-WFFORMAT-97, REQ-WFEXEC-65).
pub(super) const WAIT_TREE_ROOT: &str = "schemaVersion: 1
programs: [bin]
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    inputs:
      answers:
        ref: controls.answers
    on:
      planned: review
      blocked:
        wait: answer
        then: design
      failed:
        wait: repair
        then: design
  review:
    kind: workflow
    workflow: review.yaml
    on:
      approve:
        wait: approval
        then: merge
        onHandBack: fix
      failed: exit.failed
  fix:
    kind: workflow
    workflow: fix.yaml
    inputs:
      notes:
        ref: wait.notes
    on:
      built: merge
      failed: exit.failed
  merge:
    kind: workflow
    workflow: merge.yaml
    on:
      admitted:
        wait: external
        then: merge
        rule:
          source: github
          event: pull_request.queue_settled
          where: event.pullRequest == outputs.design.pullRequest
      landed: exit.landed
      failed: exit.failed
";

/// `decide NAME` records the step inputs it received as `seen-<input>` and reports the
/// execution root's `verdict-NAME`, first taking it from the head of `verdicts-NAME` when
/// that queue holds one, and the pull request `verdict-NAME-pullRequest` names, if any;
/// while the execution root holds `hold-NAME` it first writes `held-NAME` and sleeps until
/// `release-NAME` appears.
const WAIT_TREE_DECIDE: &str = "#!/bin/sh
set -eu
if [ -e \"hold-$1\" ]; then
  : > \"held-$1\"
  while [ ! -e \"release-$1\" ]; do sleep 0.1; done
fi
if [ -s \"verdicts-$1\" ]; then
  printf '%s' \"$(head -n 1 \"verdicts-$1\")\" > \"verdict-$1\"
  tail -n +2 \"verdicts-$1\" > \"verdicts-$1.tmp\"
  mv -f \"verdicts-$1.tmp\" \"verdicts-$1\"
fi
for input in answers notes findings; do
  if [ -e \"${UM_STEP_INPUTS:-}/values/$input\" ]; then
    cat \"$UM_STEP_INPUTS/values/$input\" > \"seen-$input.tmp\"
    mv -f \"seen-$input.tmp\" \"seen-$input\"
  fi
done
cat \"verdict-$1\" > \"verdict-$1.txt\"
if [ -e \"verdict-$1-pullRequest\" ]; then
  cat \"verdict-$1-pullRequest\" > \"pull-request-$1.txt\"
else
  : > \"pull-request-$1.txt\"
fi
";

/// The PI profile `design` declares and never uses, which a `control.retry` may replace.
const UNUSED_PROFILE: &str = "agentProfiles:
  unused:
    harness:
      kind: pi
      config:
        model: fixture/model
        thinking: low
";

/// The `questions` output `design` exports into its wait's payload: its kind and the file
/// in the execution root it is read from.
/// A JSON output also names its result schema, a file beside the child workflow.
struct Questions {
    kind: &'static str,
    path: &'static str,
    schema: Option<&'static str>,
}

const TEXT_QUESTIONS: Questions = Questions {
    kind: "text",
    path: "questions.txt",
    schema: None,
};

const JSON_QUESTIONS: Questions = Questions {
    kind: "json",
    path: "questions.json",
    schema: Some("questions.schema.json"),
};

fn wait_tree_child(
    name: &str,
    outcomes: &str,
    input: Option<(&str, &str)>,
    questions: &Questions,
) -> String {
    let (declared, mapped) = match input {
        Some((input, kind)) => (
            format!("inputs:\n  {input}:\n    kind: {kind}\n    optional: true\n"),
            format!("    inputs:\n      {input}:\n        ref: inputs.{input}\n"),
        ),
        None => (String::new(), String::new()),
    };
    // `design` also exports the questions its wait's payload carries and the pull request
    // `merge`'s external wait observes.
    let (profiles, questions) = if name == "design" {
        (
            UNUSED_PROFILE,
            format!(
                "      questions:\n        kind: {}\n        from: path\n        path: {}\n{}      pullRequest:\n        kind: text\n        from: path\n        path: pull-request-design.txt\nexports:\n  questions:\n    ref: outputs.decide.questions\n  pullRequest:\n    ref: outputs.decide.pullRequest\n",
                questions.kind,
                questions.path,
                questions
                    .schema
                    .map(|schema| format!("        schema: {schema}\n"))
                    .unwrap_or_default(),
            ),
        )
    } else {
        ("", String::new())
    };
    format!(
        "schemaVersion: 1\n{profiles}{declared}outcomes: [{outcomes}]\noutcome:\n  from: outputs.decide.verdict\n  onFailure: failed\nsteps:\n  decide:\n    kind: cmd\n{mapped}    command:\n      argv: [decide, {name}]\n    outputs:\n      verdict:\n        kind: text\n        from: path\n        path: verdict-{name}.txt\n{questions}"
    )
}

pub(super) fn wait_tree() -> TreeBundle {
    let bundle = wait_tree_asking(&TEXT_QUESTIONS);
    fs::write(
        bundle.execution_root().join(TEXT_QUESTIONS.path),
        "1. keep the cache?",
    )
    .unwrap();
    bundle
}

/// The wait tree whose `design` exports `questions` as JSON, the execution root's
/// `questions.json`.
pub(super) fn wait_tree_with_json_questions(questions: &serde_json::Value) -> TreeBundle {
    let bundle = wait_tree_asking(&JSON_QUESTIONS);
    fs::write(
        bundle.execution_root().join(JSON_QUESTIONS.path),
        serde_json::to_vec(questions).unwrap(),
    )
    .unwrap();
    bundle
}

fn wait_tree_asking(questions: &Questions) -> TreeBundle {
    let bundle = TreeBundle::new(WAIT_TREE_ROOT);
    for (name, outcomes, input) in [
        (
            "design",
            "planned, blocked, failed",
            Some(("answers", "json")),
        ),
        ("review", "approve, failed", None),
        ("fix", "built, failed", Some(("notes", "text"))),
        ("merge", "admitted, landed, failed", None),
    ] {
        bundle.write_source(
            &format!("{name}.yaml"),
            &wait_tree_child(name, outcomes, input, questions),
        );
    }
    if let Some(schema) = questions.schema {
        bundle.write_source(
            schema,
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array"}"#,
        );
    }
    bundle.write_program("bin/decide", WAIT_TREE_DECIDE);
    bundle
}

pub(super) fn decide(bundle: &TreeBundle, step: &str, verdict: &str) {
    fs::write(
        bundle.execution_root().join(format!("verdict-{step}")),
        verdict,
    )
    .unwrap();
}

pub(super) fn resume(run: &Path) -> std::process::Output {
    isolated_command(&continue_args(run, &[])).output().unwrap()
}

/// Appends `control` to the run as `controls/<NNNN>-<event>.json` (REQ-LOCALRUN-178).
fn append_control(run: &Path, control: &serde_json::Value) {
    let directory = run.join("controls");
    if !directory.exists() {
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let ordinal = fs::read_dir(&directory).unwrap().count() + 1;
    let event = control["event"]
        .as_str()
        .and_then(|event| event.strip_prefix("control."))
        .unwrap();
    fs::write(
        directory.join(format!("{ordinal:04}-{event}.json")),
        serde_json::to_vec(control).unwrap(),
    )
    .unwrap();
}

/// A control event for the run at `run`, resolving `wait` when it names one.
fn control(
    run: &Path,
    event: &str,
    wait: Option<&serde_json::Value>,
    payload: serde_json::Value,
) -> serde_json::Value {
    let mut control = serde_json::json!({
        "source": "um",
        "event": event,
        "run": run_record(run)["id"],
        "by": "operator",
        "at": "2026-11-07T12:00:00Z",
        "payload": payload,
    });
    if let Some(wait) = wait {
        control["wait"] = format!(
            "{}@{}",
            wait["kind"].as_str().unwrap(),
            wait["since"].as_str().unwrap()
        )
        .into();
    }
    control
}

pub(super) fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap_or_else(|_| panic!("{path:?}"))
}

pub(super) fn completed(output: &std::process::Output) -> serde_json::Value {
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let terminal: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(workflow_run_schema().is_valid(&terminal), "{terminal}");
    terminal
}

pub(super) fn partition_event(output: &std::process::Output) -> serde_json::Value {
    let event: serde_json::Value =
        serde_json::from_slice(output.stderr.split(|byte| *byte == b'\n').next().unwrap()).unwrap();
    assert_eq!(event["event"], "continuation_partition", "{event}");
    event
}

pub(super) fn attempt_count(run: &Path) -> usize {
    fs::read_dir(run.join("attempts")).unwrap().count()
}

/// Creates the wait tree's run with `design` deciding `verdict`, which waits.
pub(super) fn waiting_run(
    bundle: &TreeBundle,
    name: &str,
    verdict: &str,
) -> (std::path::PathBuf, serde_json::Value) {
    decide(bundle, "design", verdict);
    let destination = bundle.result(name);
    let terminal = completed(
        &isolated_command(&bundle.args(&destination))
            .output()
            .unwrap(),
    );
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    (fs::canonicalize(&destination).unwrap(), terminal)
}

// SC-CONTINUE-7, REQ-WFEXEC-61, REQ-WFEXEC-63, REQ-CONTINUE-74, REQ-CONTINUE-75,
// REQ-CONTINUE-77, REQ-LOCALRUN-127, REQ-LOCALRUN-178, REQ-CONTINUE-29
#[test]
fn an_answered_and_handed_back_run_continues_through_its_waits_to_landing() {
    let bundle = wait_tree();
    let (run, terminal) = waiting_run(&bundle, "answered", "blocked");
    let wait = &terminal["wait"];
    assert_eq!(wait["kind"], "answer", "{terminal}");
    assert_eq!(wait["then"], "design");
    assert_eq!(wait["step"], serde_json::json!(["design"]));
    assert_eq!(wait["payload"]["child"], "children/design/000001");
    assert_eq!(wait["payload"]["outcome"], "blocked");
    assert_eq!(wait["payload"]["questions"], "1. keep the cache?");
    let recorded = read_json(&run.join("wait.json"));
    assert!(
        super::workflow_definition::schema("workflow-run-wait-v1.schema.json").is_valid(&recorded),
        "{recorded}"
    );
    assert_eq!(&recorded, wait);
    assert_eq!(recorded, read_json(&run.join("attempts/000001/wait.json")));
    let record = run_record(&run);
    assert_eq!(record["state"], "waiting");
    assert_eq!(record["outcome"], serde_json::Value::Null);
    assert_eq!(record["attempts"], 1);
    let result = read_json(&run.join("attempts/000001/result/result.json"));
    assert_eq!(result["outcome"], "waiting");
    for step in ["review", "fix", "merge"] {
        let step = super::workflow_run::result_step(&terminal, step);
        assert_eq!(step["state"], "not_run", "{terminal}");
        assert_eq!(step["detail"]["code"], "not_reached", "{terminal}");
    }

    // REQ-CONTINUE-74: a held wait changes nothing and answers quickly.
    let before = fs::read(run.join("run.json")).unwrap();
    let started = um_support::monotonic_now();
    let held = completed(&resume(&run));
    assert!(
        um_support::monotonic_now().saturating_duration_since(started)
            < std::time::Duration::from_secs(2)
    );
    assert_eq!(held["outcome"], "waiting", "{held}");
    assert_eq!(held["attemptNumber"], 1);
    assert_eq!(&held["wait"], wait);
    assert_eq!(attempt_count(&run), 1);
    assert_eq!(fs::read(run.join("run.json")).unwrap(), before);

    // REQ-CONTINUE-75, REQ-WFEXEC-63, REQ-WFFORMAT-92: the answer continues from `design`
    // itself, whose second child receives the answers as JSON, and `review` enters an
    // approval wait.
    append_control(
        &run,
        &control(
            &run,
            "control.answer",
            Some(wait),
            serde_json::json!({
                "question": "1",
                "text": "keep it",
                "answers": {"1": "keep it"},
                "complete": true,
            }),
        ),
    );
    decide(&bundle, "design", "planned");
    decide(&bundle, "review", "approve");
    let output = resume(&run);
    let terminal = completed(&output);
    let event = partition_event(&output);
    assert_eq!(event["attemptNumber"], 2);
    assert_eq!(
        event["continuation"]["fromSteps"],
        serde_json::json!(["design"])
    );
    assert_eq!(
        event["continuation"]["reexecutedSteps"],
        serde_json::json!(["design", "review", "fix", "merge"])
    );
    assert_eq!(event["continuation"]["reason"], "resume");
    assert_eq!(
        read_json(&bundle.execution_root().join("seen-answers")),
        serde_json::json!({"1": "keep it"})
    );
    assert!(run.join("children/design/000002").is_dir());
    assert!(!run.join("children/design/000003").exists());
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    let approval = &terminal["wait"];
    assert_eq!(approval["kind"], "approval");
    assert_eq!(approval["then"], "merge");
    assert_eq!(approval["onHandBack"], "fix");
    assert_eq!(attempt_count(&run), 2);
    assert!(run.join("attempts/000001/wait.json").is_file());
    assert_eq!(read_json(&run.join("wait.json")), *approval);

    // REQ-CONTINUE-75, REQ-WFFORMAT-95: a hand-back continues from `fix` with the notes.
    append_control(
        &run,
        &control(
            &run,
            "control.hand_back",
            Some(approval),
            serde_json::json!({"notes": "tighten the test"}),
        ),
    );
    decide(&bundle, "fix", "built");
    decide(&bundle, "merge", "landed");
    let output = resume(&run);
    let terminal = completed(&output);
    let event = partition_event(&output);
    assert_eq!(event["attemptNumber"], 3);
    assert_eq!(
        event["continuation"]["fromSteps"],
        serde_json::json!(["fix"])
    );
    assert_eq!(
        event["continuation"]["inheritedSteps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|step| step["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["design", "review"]
    );
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("seen-notes")).unwrap(),
        "tighten the test"
    );
    assert!(run.join("children/fix/000001").is_dir());
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert!(terminal.get("wait").is_none(), "{terminal}");
    assert!(!run.join("wait.json").exists());
    let record = run_record(&run);
    assert_eq!(record["state"], "exited");
    assert_eq!(record["outcome"], "landed");
    assert_eq!(record["attempts"], 3);

    // REQ-CONTINUE-29: an exited run is not resumed without `--from`.
    let before = fs::read(run.join("run.json")).unwrap();
    let refused = resume(&run);
    assert_eq!(refused.status.code(), Some(1));
    let rejection: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert!(workflow_run_schema().is_valid(&rejection), "{rejection}");
    assert_eq!(
        rejection["diagnostics"][0]["code"],
        "continuation_disposition_ineligible"
    );
    assert_eq!(attempt_count(&run), 3);
    assert_eq!(fs::read(run.join("run.json")).unwrap(), before);
}

/// A root whose `build` step fans out over the parts `design` exports and then lands
/// through `merge` (REQ-WFFORMAT-98, REQ-WFFORMAT-99).
pub(super) const FAN_OUT_TREE_ROOT: &str = "schemaVersion: 1
programs: [bin]
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    on:
      planned: build
      failed: exit.failed
  build:
    kind: workflow
    workflow: build.yaml
    forEach: outputs.design.parts
    inputs:
      part:
        ref: item.name
      answers:
        ref: controls.answers
    on:
      built: merge
      blocked:
        wait: answer
        then: build
      failed: exit.failed
  merge:
    kind: workflow
    workflow: merge.yaml
    on:
      landed: exit.landed
      failed: exit.failed
";

/// `design` exports the execution root's `parts.json`; `merge` reports `verdict-merge.txt`.
const FAN_OUT_TREE_DESIGN: &str = "schemaVersion: 1
outcomes: [planned, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict-design.txt
      parts:
        kind: json
        from: path
        path: parts.json
        schema: parts.schema.json
exports:
  parts:
    ref: outputs.decide.parts
";

const FAN_OUT_TREE_BUILD: &str = "schemaVersion: 1
inputs:
  part:
    kind: text
  answers:
    kind: json
    optional: true
outcomes: [built, blocked, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    inputs:
      part:
        ref: inputs.part
      answers:
        ref: inputs.answers
    command:
      argv: [build-decide]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict-build.txt
";

const FAN_OUT_TREE_MERGE: &str = "schemaVersion: 1
outcomes: [landed, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict-merge.txt
";

/// `build-decide` reports the execution root's `verdict-build-<part>` for the part it was
/// given and records the answers it received, an omitted input reading `null`, as
/// `seen-answers-<part>`.
const FAN_OUT_TREE_DECIDE: &str = "#!/bin/sh
set -eu
part=$(cat \"$UM_STEP_INPUTS/values/part\")
answers=$(cat \"$UM_STEP_INPUTS/values/answers\")
if [ \"$answers\" != null ]; then
  printf '%s' \"$answers\" > \"seen-answers-$part\"
fi
cat \"verdict-build-$part\" > verdict-build.txt
";

/// The schema `design`'s `parts` output is checked against.
const PARTS_SCHEMA: &str =
    r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array"}"#;

/// The fan-out tree, its execution root deciding `design` `planned` with parts `a`, `b` and
/// `c`, whose builds decide `built`, `blocked` and `built`, and `merge` `landed`.
pub(super) fn fan_out_tree() -> TreeBundle {
    let bundle = TreeBundle::new(FAN_OUT_TREE_ROOT);
    bundle.write_source("design.yaml", FAN_OUT_TREE_DESIGN);
    bundle.write_source("build.yaml", FAN_OUT_TREE_BUILD);
    bundle.write_source("merge.yaml", FAN_OUT_TREE_MERGE);
    bundle.write_source("parts.schema.json", PARTS_SCHEMA);
    bundle.write_program("bin/build-decide", FAN_OUT_TREE_DECIDE);
    for (file, contents) in [
        ("verdict-design.txt", "planned"),
        ("parts.json", r#"[{"name":"a"},{"name":"b"},{"name":"c"}]"#),
        ("verdict-build-a", "built"),
        ("verdict-build-b", "blocked"),
        ("verdict-build-c", "built"),
        ("verdict-merge.txt", "landed"),
    ] {
        fs::write(bundle.execution_root().join(file), contents).unwrap();
    }
    bundle
}

// REQ-WFEXEC-66, REQ-WFEXEC-68, REQ-CONTINUE-75, REQ-LOCALRUN-177, REQ-WFDEF-4: a fan-out
// waits at the element whose child blocked, and the answered run resumes at that element,
// reruns only it with the answers and continues with the rest to landing.
#[test]
fn a_fan_out_waits_at_its_element_and_resumes_the_same_element() {
    let bundle = fan_out_tree();
    let root = bundle.execution_root();
    let destination = bundle.result("fan-out");
    let terminal = completed(
        &isolated_command(&bundle.args(&destination))
            .output()
            .unwrap(),
    );
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    let run = fs::canonicalize(&destination).unwrap();
    let wait = read_json(&run.join("wait.json"));
    assert_eq!(wait["kind"], "answer", "{wait}");
    assert_eq!(wait["step"], serde_json::json!(["build", {"item": 2}]));
    assert_eq!(wait["then"], "build");
    assert_eq!(wait["payload"]["outcome"], "blocked");
    assert_eq!(wait["payload"]["child"], "children/build/000002/000001");
    assert_eq!(
        run_record(&run)["position"],
        serde_json::json!(["build", {"item": 2}])
    );
    assert!(!run.join("children/build/000003").exists());

    let output = run_control(
        None,
        "answer",
        &run,
        &["--question", "1", "--text", "split"],
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    fs::write(root.join("verdict-build-b"), "built").unwrap();
    let terminal = completed(&resume(&run));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited");
    assert_eq!(record["outcome"], "landed");
    assert_eq!(record["position"], serde_json::json!([]));
    let ordinals = |element: &str| ordinals_of(&run.join("children/build").join(element));
    assert_eq!(ordinals("000001"), ["000001"]);
    assert_eq!(ordinals("000002"), ["000001", "000002"]);
    assert_eq!(ordinals("000003"), ["000001"]);
    // The answers reach the resumed element and the one after it; `a` did not run again.
    let answers = serde_json::json!({"1": "split"});
    assert_eq!(read_json(&root.join("seen-answers-b")), answers);
    assert_eq!(read_json(&root.join("seen-answers-c")), answers);
    assert!(!root.join("seen-answers-a").exists());

    let output = run_control(None, "show", &run, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let shown: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let children = shown["children"]
        .as_array()
        .unwrap()
        .iter()
        .map(|child| {
            (
                child["step"].as_str().unwrap().to_owned(),
                child["item"].clone(),
                child["ordinal"].as_u64().unwrap(),
                child["outcome"].clone(),
            )
        })
        .collect::<Vec<_>>();
    let child = |step: &str, item: serde_json::Value, ordinal, outcome: &str| {
        (step.to_owned(), item, ordinal, serde_json::json!(outcome))
    };
    assert_eq!(
        children,
        [
            child("build", 1.into(), 1, "built"),
            child("build", 2.into(), 1, "blocked"),
            child("build", 2.into(), 2, "built"),
            child("build", 3.into(), 1, "built"),
            child("design", serde_json::Value::Null, 1, "planned"),
            child("merge", serde_json::Value::Null, 1, "landed"),
        ],
        "{shown}"
    );
}

/// A root whose plain `build` step maps the `parts` json export of its `design` child and
/// waits for an answer when the build blocks.
const JSON_EXPORT_TREE_ROOT: &str = "schemaVersion: 1
programs: [bin]
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    on:
      planned: build
      failed: exit.failed
  build:
    kind: workflow
    workflow: build.yaml
    inputs:
      parts:
        ref: outputs.design.parts
    on:
      built: exit.landed
      blocked:
        wait: answer
        then: build
      failed: exit.failed
";

/// `build` copies its `parts` input to `seen-parts.json` and reports `verdict-build.txt`.
const JSON_EXPORT_TREE_BUILD: &str = "schemaVersion: 1
inputs:
  parts:
    kind: json
outcomes: [built, blocked, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    inputs:
      parts:
        ref: inputs.parts
    command:
      argv: [build-copy-parts]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict-build.txt
";

// REQ-CONTINUE-16, REQ-WFEXEC-60: a json export an inherited `workflow` step committed reaches
// the `workflow` step a resume re-executes, reloaded from the prior attempt.
#[test]
fn a_resumed_workflow_step_receives_an_inherited_childs_json_export() {
    let bundle = TreeBundle::new(JSON_EXPORT_TREE_ROOT);
    bundle.write_source("design.yaml", FAN_OUT_TREE_DESIGN);
    bundle.write_source("build.yaml", JSON_EXPORT_TREE_BUILD);
    bundle.write_source("parts.schema.json", PARTS_SCHEMA);
    bundle.write_program(
        "bin/build-copy-parts",
        "#!/bin/sh\nset -eu\ncp \"$UM_STEP_INPUTS/values/parts\" seen-parts.json\n",
    );
    let root = bundle.execution_root();
    let parts = serde_json::json!([{"name": "a"}, {"name": "b"}]);
    for (file, contents) in [
        ("verdict-design.txt", "planned".to_owned()),
        ("parts.json", parts.to_string()),
        ("verdict-build.txt", "blocked".to_owned()),
    ] {
        fs::write(root.join(file), contents).unwrap();
    }
    let destination = bundle.result("json-export");
    let terminal = completed(
        &isolated_command(&bundle.args(&destination))
            .output()
            .unwrap(),
    );
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    let run = fs::canonicalize(&destination).unwrap();
    fs::remove_file(root.join("seen-parts.json")).unwrap();

    let output = run_control(None, "answer", &run, &["--question", "1", "--text", "go"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    fs::write(root.join("verdict-build.txt"), "built").unwrap();
    let terminal = completed(&resume(&run));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(read_json(&root.join("seen-parts.json")), parts);
    assert_eq!(ordinals_of(&run.join("children/design")), ["000001"]);
    assert_eq!(
        ordinals_of(&run.join("children/build")),
        ["000001", "000002"]
    );
}

pub(super) fn ordinals_of(directory: &Path) -> Vec<String> {
    let mut names = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    names.sort();
    names
}

// REQ-CONTINUE-75, REQ-CONTINUE-77: a `control.retry` continues a repair wait with reason
// `retry` and resolves `then`'s child with its agent-profile replacement; a
// `control.withdraw` ends a run without another attempt.
#[test]
fn a_repair_wait_is_retried_with_replaced_profiles_or_withdrawn() {
    let bundle = wait_tree();
    let (run, terminal) = waiting_run(&bundle, "retried", "failed");
    let wait = terminal["wait"].clone();
    assert_eq!(wait["kind"], "repair", "{terminal}");
    let profiles = serde_json::json!({
        "unused": {
            "harness": {
                "kind": "claude_code",
                "config": {"model": "fixture/claude", "effort": "low"}
            }
        }
    });
    append_control(
        &run,
        &control(
            &run,
            "control.retry",
            Some(&wait),
            serde_json::json!({"agentProfiles": profiles}),
        ),
    );
    decide(&bundle, "design", "planned");
    decide(&bundle, "review", "approve");
    let output = resume(&run);
    let terminal = completed(&output);
    assert_eq!(partition_event(&output)["continuation"]["reason"], "retry");
    assert_eq!(terminal["result"]["continuation"]["reason"], "retry");
    let child = read_json(&run.join("children/design/000002/attempts/000001/result/result.json"));
    assert_eq!(
        child["workflow"]["agentProfileReplacement"]["profiles"]["unused"]["harness"]["kind"],
        "claude_code",
        "{child}"
    );
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");

    let (run, terminal) = waiting_run(&bundle, "withdrawn", "failed");
    append_control(
        &run,
        &control(
            &run,
            "control.withdraw",
            Some(&terminal["wait"]),
            serde_json::json!({}),
        ),
    );
    let withdrawn = completed(&resume(&run));
    assert_eq!(withdrawn["outcome"], "withdrawn", "{withdrawn}");
    assert_eq!(withdrawn["attemptNumber"], 1);
    let record = run_record(&run);
    assert_eq!(record["state"], "exited");
    assert_eq!(record["outcome"], "withdrawn");
    assert_eq!(record["attempts"], 1);
    assert_eq!(attempt_count(&run), 1);
    assert!(!run.join("wait.json").exists());
}

// REQ-WFEXEC-61, REQ-WFEXEC-62: a stop control in force before `review` makes the run wait
// `stop` there, and only a lift naming that stop continues it from `review`.
#[test]
fn a_stop_before_a_step_waits_until_it_is_lifted() {
    let bundle = wait_tree();
    let (run, terminal) = waiting_run(&bundle, "stopped", "blocked");
    let stop = control(
        &run,
        "control.stop",
        None,
        serde_json::json!({"before": "review"}),
    );
    append_control(&run, &stop);
    append_control(
        &run,
        &control(
            &run,
            "control.answer",
            Some(&terminal["wait"]),
            serde_json::json!({
                "question": "1",
                "text": "keep it",
                "answers": {"1": "keep it"},
                "complete": true,
            }),
        ),
    );
    decide(&bundle, "design", "planned");
    let terminal = completed(&resume(&run));
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    let wait = &terminal["wait"];
    assert_eq!(wait["kind"], "stop");
    assert_eq!(wait["step"], serde_json::json!(["review"]));
    assert_eq!(wait["then"], "review");
    assert_eq!(wait["payload"]["by"], "operator");
    assert_eq!(wait["payload"]["at"], stop["at"]);
    let review = super::workflow_run::result_step(&terminal, "review");
    assert_eq!(review["state"], "not_run", "{terminal}");
    assert_eq!(review["detail"]["code"], "not_reached", "{terminal}");
    assert!(!run.join("children/review").exists());
    assert_eq!(completed(&resume(&run))["outcome"], "waiting");

    // A lift naming an earlier stop for the same step leaves the wait held.
    append_control(
        &run,
        &control(
            &run,
            "control.lift",
            None,
            serde_json::json!({"before": "review", "stop": "2026-11-07T11:00:00Z"}),
        ),
    );
    let held = completed(&resume(&run));
    assert_eq!(held["outcome"], "waiting", "{held}");
    assert_eq!(&held["wait"], wait);
    assert_eq!(attempt_count(&run), 2);

    append_control(
        &run,
        &control(
            &run,
            "control.lift",
            None,
            serde_json::json!({"before": "review", "stop": stop["at"]}),
        ),
    );
    decide(&bundle, "review", "approve");
    let output = resume(&run);
    let terminal = completed(&output);
    assert_eq!(
        partition_event(&output)["continuation"]["fromSteps"],
        serde_json::json!(["review"])
    );
    assert!(run.join("children/review/000001").is_dir());
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");
}

/// A root whose `review` hands back to `fix` under a limit of one, after which it waits for a
/// repair at `review`, and whose `fix` builds back to `review` (REQ-WFEXEC-64,
/// REQ-WFFORMAT-92, REQ-WFFORMAT-96). `fix` reads the review's `findings`.
pub(super) const LOOP_TREE_ROOT: &str = "schemaVersion: 1
programs: [bin]
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    on:
      planned: review
      blocked: exit.failed
      failed: exit.failed
  review:
    kind: workflow
    workflow: review.yaml
    on:
      proceed: merge
      hand_back:
        then: fix
        limit: 1
        else:
          wait: repair
          then: review
      failed: exit.failed
  fix:
    kind: workflow
    workflow: fix.yaml
    inputs:
      findings:
        ref: outputs.review.findings
    on:
      built: review
      failed: exit.failed
  merge:
    kind: workflow
    workflow: merge.yaml
    on:
      admitted:
        wait: external
        then: merge
        rule:
          source: github
          event: pull_request.queue_settled
          where: event.pullRequest == outputs.design.pullRequest
      landed: exit.landed
      failed: exit.failed
";

/// The loop tree: the wait tree's children, `review` declaring `proceed, hand_back, failed`
/// and exporting the execution root's `findings.txt` as text `findings`, which `fix` reads.
pub(super) fn loop_tree() -> TreeBundle {
    loop_tree_with(
        LOOP_TREE_ROOT,
        "proceed, hand_back, failed",
        "admitted, landed, failed",
    )
}

/// The loop tree's children under `root`, `review` and `merge` declaring the outcomes given.
fn loop_tree_with(root: &str, review: &str, merge: &str) -> TreeBundle {
    let bundle = TreeBundle::new(root);
    for (name, outcomes, input) in [
        (
            "design",
            "planned, blocked, failed",
            Some(("answers", "json")),
        ),
        ("fix", "built, failed", Some(("findings", "text"))),
        ("merge", merge, None),
    ] {
        bundle.write_source(
            &format!("{name}.yaml"),
            &wait_tree_child(name, outcomes, input, &TEXT_QUESTIONS),
        );
    }
    bundle.write_source(
        "review.yaml",
        &(wait_tree_child("review", review, None, &TEXT_QUESTIONS)
            + "      findings:\n        kind: text\n        from: path\n        path: findings.txt\nexports:\n  findings:\n    ref: outputs.decide.findings\n"),
    );
    bundle.write_program("bin/decide", WAIT_TREE_DECIDE);
    let execution = bundle.execution_root();
    fs::write(execution.join(TEXT_QUESTIONS.path), "1. keep the cache?").unwrap();
    fs::write(execution.join("findings.txt"), "the cache is stale").unwrap();
    bundle
}

/// Queues `verdicts` for `step`, one per run of its `decide`, in order.
pub(super) fn decide_each(bundle: &TreeBundle, step: &str, verdicts: &[&str]) {
    let mut queue = verdicts.join("\n");
    queue.push('\n');
    fs::write(
        bundle.execution_root().join(format!("verdicts-{step}")),
        queue,
    )
    .unwrap();
}

/// The documents a chaining command writes with `--json`, one per attempt, each valid; the
/// command exits 0.
pub(super) fn chained(output: &std::process::Output) -> Vec<serde_json::Value> {
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let documents = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<serde_json::Value>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for document in &documents {
        assert!(workflow_run_schema().is_valid(document), "{document}");
    }
    documents
}

/// `um workflow run status RUN --json`, valid against its schema.
pub(super) fn status_document(run: &Path) -> serde_json::Value {
    let output = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "status".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let schema: serde_json::Value = serde_json::from_slice(
        &fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/schemas/workflow-status-result-v1.schema.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    assert!(
        validator.is_valid(&status),
        "{status}\n{:?}",
        validator.iter_errors(&status).collect::<Vec<_>>()
    );
    status
}

// REQ-WFEXEC-64, REQ-CONTINUE-77, REQ-LOCALRUN-126: a limited hand-back ends the attempt
// `continued` and the same command continues from `fix` with the edge counted; `fix`'s backward
// edge continues at `review` uncounted; the review proceeds and the run lands, and `--json`
// writes one document per chained attempt.
#[test]
fn a_limited_hand_back_and_a_backward_edge_chain_attempts_to_landing() {
    let bundle = loop_tree();
    decide(&bundle, "design", "planned");
    decide_each(&bundle, "review", &["hand_back", "proceed"]);
    decide(&bundle, "fix", "built");
    decide(&bundle, "merge", "landed");
    let destination = bundle.result("looped");
    let documents = chained(
        &isolated_command(&bundle.args(&destination))
            .output()
            .unwrap(),
    );
    let outcomes = documents
        .iter()
        .map(|document| document["outcome"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes, ["continued", "continued", "succeeded"]);
    let run = fs::canonicalize(&destination).unwrap();
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("seen-findings")).unwrap(),
        "the cache is stale"
    );

    let status = status_document(&run);
    let attempts = status["state"]["attempts"].as_array().unwrap();
    let states = attempts
        .iter()
        .map(|attempt| attempt["state"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(states, ["continued", "continued", "succeeded"], "{status}");
    assert_eq!(
        attempts[0]["takenEdge"],
        serde_json::json!({"step": "review", "outcome": "hand_back", "target": "fix", "counted": true})
    );
    assert_eq!(attempts[1]["continuation"]["reason"], "edge");
    assert_eq!(
        attempts[1]["continuation"]["edge"],
        serde_json::json!({"step": "review", "outcome": "hand_back", "target": "fix", "counted": true})
    );
    assert_eq!(
        attempts[1]["continuation"]["fromSteps"],
        serde_json::json!(["fix"])
    );
    assert_eq!(attempts[2]["continuation"]["reason"], "edge");
    assert_eq!(
        attempts[2]["continuation"]["edge"],
        serde_json::json!({"step": "fix", "outcome": "built", "target": "review", "counted": false})
    );
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "landed");
    assert_eq!(record["attempts"], 3);
    assert_eq!(record["edges"], serde_json::json!({"review.hand_back": 1}));
}

/// Runs the loop tree until its second hand-back exhausts the limit and the run waits for a
/// repair at `review`.
fn exhausted_loop(bundle: &TreeBundle, name: &str) -> (std::path::PathBuf, serde_json::Value) {
    decide(bundle, "design", "planned");
    decide_each(bundle, "review", &["hand_back", "hand_back"]);
    decide(bundle, "fix", "built");
    let destination = bundle.result(name);
    let documents = chained(
        &isolated_command(&bundle.args(&destination))
            .output()
            .unwrap(),
    );
    let outcomes = documents
        .iter()
        .map(|document| document["outcome"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes, ["continued", "continued", "waiting"]);
    let wait = documents[2]["wait"].clone();
    assert_eq!(wait["kind"], "repair", "{wait}");
    assert_eq!(wait["step"], serde_json::json!(["review"]));
    assert_eq!(wait["then"], "review");
    let run = fs::canonicalize(&destination).unwrap();
    let record = run_record(&run);
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["attempts"], 3);
    // The second hand-back took the `else`, uncounted.
    assert_eq!(record["edges"], serde_json::json!({"review.hand_back": 1}));
    (run, wait)
}

// REQ-WFEXEC-64, REQ-CONTINUE-75: once the limit is reached the hand-back takes its `else`,
// a repair wait at `review` that a `control.retry` continues from `review`.
#[test]
fn an_exhausted_limit_waits_for_a_repair_that_a_retry_continues() {
    let bundle = loop_tree();
    let (run, wait) = exhausted_loop(&bundle, "exhausted");
    append_control(
        &run,
        &control(&run, "control.retry", Some(&wait), serde_json::json!({})),
    );
    decide_each(&bundle, "review", &["proceed"]);
    decide(&bundle, "merge", "landed");
    let output = resume(&run);
    let documents = chained(&output);
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0]["outcome"], "succeeded", "{}", documents[0]);
    let partition = partition_event(&output);
    assert_eq!(partition["continuation"]["reason"], "retry");
    assert_eq!(
        partition["continuation"]["fromSteps"],
        serde_json::json!(["review"])
    );
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "landed");
    assert_eq!(record["edges"], serde_json::json!({"review.hand_back": 1}));
}

/// Removes `path` and everything under it, making each entry writable first.
fn remove_tree(path: &Path) {
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o700)).unwrap();
            remove_tree(&entry.path());
        } else {
            fs::remove_file(entry.path()).unwrap();
        }
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    fs::remove_dir(path).unwrap();
}

// REQ-CONTINUE-78: a run whose process ended after an attempt ended `continued` and before
// the next began is continued by `resume` from the recorded edge's target, uncounted.
#[test]
fn resume_continues_a_run_whose_latest_attempt_continued() {
    let bundle = loop_tree();
    let (run, _) = exhausted_loop(&bundle, "interrupted-chain");
    // Rewind the run to the moment attempt 2 (`fix` built, backward to `review`) ended.
    let mut state = read_json(&run.join("state.json"));
    state["attempts"].as_array_mut().unwrap().pop();
    state["currentAttemptNumber"] = 2.into();
    let mut bytes = serde_json::to_vec_pretty(&state).unwrap();
    bytes.push(b'\n');
    fs::write(run.join("state.json"), bytes).unwrap();
    remove_tree(&run.join("attempts/000003"));
    fs::remove_file(run.join("wait.json")).unwrap();
    let mut record = run_record(&run);
    record["state"] = "running".into();
    record["attempts"] = 2.into();
    let mut bytes = serde_json::to_vec_pretty(&record).unwrap();
    bytes.push(b'\n');
    let record_path = run.join("run.json");
    fs::set_permissions(&record_path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&record_path, bytes).unwrap();
    assert_eq!(
        status_document(&run)["state"]["attempts"][1]["state"],
        "continued"
    );

    decide_each(&bundle, "review", &["proceed"]);
    decide(&bundle, "merge", "landed");
    let output = resume(&run);
    let documents = chained(&output);
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0]["outcome"], "succeeded", "{}", documents[0]);
    assert_eq!(documents[0]["attemptNumber"], 3);
    let continuation = &partition_event(&output)["continuation"];
    assert_eq!(continuation["reason"], "edge");
    assert_eq!(continuation["fromSteps"], serde_json::json!(["review"]));
    assert_eq!(
        continuation["edge"],
        serde_json::json!({"step": "fix", "outcome": "built", "target": "review", "counted": false})
    );
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "landed");
    assert_eq!(record["attempts"], 3);
    assert_eq!(record["edges"], serde_json::json!({"review.hand_back": 1}));
}

/// The pull request `design` exports in the external-wait scenarios.
pub(super) const QUEUED_PULL_REQUEST: &str =
    "https://github.com/useful-machinery/platform/pull/3958";

/// What the queue stub answers for `QUEUED_PULL_REQUEST`.
#[derive(Clone, Copy, Debug)]
pub(super) enum QueueAnswer {
    /// GitHub: open; Mergify: queued (200).
    Queued,
    /// GitHub: merged.
    Merged,
    /// GitHub: HTTP 500.
    Failing,
}

/// A loopback stand-in for the GitHub and Mergify APIs the `github` /
/// `pull_request.queue_settled` condition reads (REQ-WFEXEC-65): it serves
/// `GET /repos/useful-machinery/platform/pulls/3958` and
/// `GET /v1/repos/useful-machinery/platform/merge-queue/pull/3958` as its answer says and
/// records each request's head. `command` points a `um` invocation at it with the token
/// `test`.
pub(super) struct QueueStub {
    base: String,
    answer: std::sync::Arc<std::sync::Mutex<QueueAnswer>>,
    requests: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl QueueStub {
    pub(super) fn start(answer: QueueAnswer) -> Self {
        use std::io::{BufRead as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let answer = std::sync::Arc::new(std::sync::Mutex::new(answer));
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (served_answer, served_requests) = (answer.clone(), requests.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                let mut head = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    head.push_str(&line);
                }
                let path = head.split(' ').nth(1).unwrap_or_default().to_owned();
                served_requests.lock().unwrap().push(head);
                let answer = *served_answer.lock().unwrap();
                let (status, body) = match (path.as_str(), answer) {
                    ("/repos/useful-machinery/platform/pulls/3958", QueueAnswer::Failing) => {
                        ("500 Internal Server Error", serde_json::json!({}))
                    }
                    ("/repos/useful-machinery/platform/pulls/3958", answer) => (
                        "200 OK",
                        serde_json::json!({
                            "merged": matches!(answer, QueueAnswer::Merged),
                            "head": {"sha": "4f2a"},
                        }),
                    ),
                    ("/v1/repos/useful-machinery/platform/merge-queue/pull/3958", _) => {
                        ("200 OK", serde_json::json!({"number": 3958}))
                    }
                    _ => ("404 Not Found", serde_json::json!({})),
                };
                let body = serde_json::to_vec(&body).unwrap();
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(&body);
            }
        });
        Self {
            base,
            answer,
            requests,
        }
    }

    pub(super) fn answer(&self, answer: QueueAnswer) {
        *self.answer.lock().unwrap() = answer;
    }

    /// The head of every request served so far, in order.
    pub(super) fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    /// `um` with `args`, its GitHub token `test` and both APIs at the stub.
    pub(super) fn command(&self, args: &[String]) -> std::process::Command {
        let mut command = isolated_command(args);
        command
            .env("GITHUB_TOKEN", "test")
            .env("UM_GITHUB_API_URL", &self.base)
            .env("UM_MERGIFY_API_URL", &self.base)
            .env("NO_PROXY", "127.0.0.1")
            .env("no_proxy", "127.0.0.1");
        command
    }
}

// REQ-WFEXEC-65, REQ-CONTINUE-74, REQ-CONTINUE-75: `merge` admitted to the queue waits for
// the pull request `design` exported; `resume` holds while it is queued, refuses a condition
// it cannot read, and continues at `merge` once the pull request is merged.
#[test]
fn an_external_wait_holds_while_queued_and_continues_once_merged() {
    let bundle = loop_tree();
    let stub = QueueStub::start(QueueAnswer::Queued);
    fs::write(
        bundle.execution_root().join("verdict-design-pullRequest"),
        QUEUED_PULL_REQUEST,
    )
    .unwrap();
    decide(&bundle, "design", "planned");
    decide(&bundle, "review", "proceed");
    decide(&bundle, "merge", "admitted");
    let destination = bundle.result("queued");
    let documents = chained(&stub.command(&bundle.args(&destination)).output().unwrap());
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0]["outcome"], "waiting", "{}", documents[0]);
    let run = fs::canonicalize(&destination).unwrap();
    let wait = read_json(&run.join("wait.json"));
    assert_eq!(wait["kind"], "external", "{wait}");
    assert_eq!(wait["step"], serde_json::json!(["merge"]));
    assert_eq!(wait["then"], "merge");
    assert_eq!(
        wait["rule"],
        serde_json::json!({
            "source": "github",
            "event": ["pull_request.queue_settled"],
            "where": format!("event.pullRequest == \"{QUEUED_PULL_REQUEST}\""),
        })
    );
    assert!(stub.requests().is_empty(), "{:?}", stub.requests());

    // Queued: the wait holds and nothing changes.
    let state = fs::read(run.join("state.json")).unwrap();
    let held = completed(&stub.command(&continue_args(&run, &[])).output().unwrap());
    assert_eq!(held["outcome"], "waiting", "{held}");
    assert_eq!(held["wait"], wait);
    assert_eq!(fs::read(run.join("state.json")).unwrap(), state);
    assert_eq!(attempt_count(&run), 1);
    let requests = stub.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(
        requests.iter().all(|request| request
            .lines()
            .any(|line| line.eq_ignore_ascii_case("authorization: bearer test"))),
        "{requests:?}"
    );

    // A condition the CLI cannot read is refused and changes nothing.
    stub.answer(QueueAnswer::Failing);
    let refused = stub.command(&continue_args(&run, &[])).output().unwrap();
    assert_eq!(refused.status.code(), Some(1), "{refused:?}");
    let refusal: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert!(workflow_run_schema().is_valid(&refusal), "{refusal}");
    assert_eq!(
        refusal["diagnostics"][0]["code"],
        "wait_condition_unavailable"
    );
    assert_eq!(fs::read(run.join("state.json")).unwrap(), state);
    assert_eq!(run_record(&run)["state"], "waiting");

    // Merged: `resume` continues at `merge`, which lands.
    stub.answer(QueueAnswer::Merged);
    decide(&bundle, "merge", "landed");
    let output = stub.command(&continue_args(&run, &[])).output().unwrap();
    let documents = chained(&output);
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0]["outcome"], "succeeded", "{}", documents[0]);
    assert_eq!(documents[0]["attemptNumber"], 2);
    let continuation = &partition_event(&output)["continuation"];
    assert_eq!(continuation["reason"], "resume");
    assert_eq!(continuation["fromSteps"], serde_json::json!(["merge"]));
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "landed");
    assert_eq!(record["attempts"], 2);
}

/// The engineering shape (SC-CONTINUE-8): the loop tree's root with `review` handing back to
/// `fix` under a limit of two and `merge` repairing itself under a limit of three, each limit's
/// `else` a repair wait, and `merge` admitted to the external wait for the queue.
const ENGINEERING_TREE_ROOT: &str = "schemaVersion: 1
programs: [bin]
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    on:
      planned: review
      blocked: exit.failed
      failed: exit.failed
  review:
    kind: workflow
    workflow: review.yaml
    on:
      proceed: merge
      approve:
        wait: approval
        then: merge
        onHandBack: fix
      hand_back:
        then: fix
        limit: 2
        else:
          wait: repair
          then: review
      failed: exit.failed
  fix:
    kind: workflow
    workflow: fix.yaml
    inputs:
      findings:
        ref: outputs.review.findings
    on:
      built: review
      failed: exit.failed
  merge:
    kind: workflow
    workflow: merge.yaml
    on:
      admitted:
        wait: external
        then: merge
        rule:
          source: github
          event: pull_request.queue_settled
          where: event.pullRequest == outputs.design.pullRequest
      landed: exit.landed
      repaired:
        then: merge
        limit: 3
        else:
          wait: repair
          then: merge
      dequeued:
        wait: repair
        then: merge
      failed: exit.failed
";

/// The outcome of every document a chaining command wrote, in order.
fn chained_outcomes(output: &std::process::Output) -> Vec<String> {
    chained(output)
        .iter()
        .map(|document| document["outcome"].as_str().unwrap().to_owned())
        .collect()
}

// SC-CONTINUE-8, REQ-WFEXEC-64, REQ-WFEXEC-65, REQ-CONTINUE-16: the review hands back to `fix`
// twice under its limit and the third time waits for a repair; the retried review proceeds,
// `merge` repairs itself twice and is admitted to the queue with the inherited `design`'s pull
// request, and the run lands once it merges, every attempt shown with its reason and edge.
#[test]
fn the_engineering_shape_loops_repairs_and_lands_through_the_queue() {
    let bundle = loop_tree_with(
        ENGINEERING_TREE_ROOT,
        "proceed, approve, hand_back, failed",
        "admitted, landed, repaired, dequeued, failed",
    );
    let stub = QueueStub::start(QueueAnswer::Queued);
    fs::write(
        bundle.execution_root().join("verdict-design-pullRequest"),
        QUEUED_PULL_REQUEST,
    )
    .unwrap();
    decide(&bundle, "design", "planned");
    decide_each(&bundle, "review", &["hand_back", "hand_back", "hand_back"]);
    decide(&bundle, "fix", "built");
    let destination = bundle.result("engineering");
    let outcomes = chained_outcomes(&stub.command(&bundle.args(&destination)).output().unwrap());
    // The limit is compared before the taking: two hand-backs reach `fix`, the third waits.
    assert_eq!(
        outcomes,
        [
            "continued",
            "continued",
            "continued",
            "continued",
            "waiting"
        ]
    );
    let run = fs::canonicalize(&destination).unwrap();
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("seen-findings")).unwrap(),
        "the cache is stale"
    );
    let wait = read_json(&run.join("wait.json"));
    assert_eq!(wait["kind"], "repair", "{wait}");
    assert_eq!(wait["step"], serde_json::json!(["review"]));
    assert_eq!(wait["then"], "review");
    let record = run_record(&run);
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["attempts"], 5);
    assert_eq!(record["edges"], serde_json::json!({"review.hand_back": 2}));

    // The retried review proceeds; `merge` repairs itself twice and is admitted.
    decide_each(&bundle, "review", &["proceed"]);
    decide_each(&bundle, "merge", &["repaired", "repaired", "admitted"]);
    let retry = stub
        .command(&[
            "workflow".to_owned(),
            "run".to_owned(),
            "retry".to_owned(),
            run.to_string_lossy().into_owned(),
            "--json".to_owned(),
        ])
        .output()
        .unwrap();
    assert_eq!(
        chained_outcomes(&retry),
        ["continued", "continued", "waiting"]
    );
    let wait = read_json(&run.join("wait.json"));
    assert_eq!(wait["kind"], "external", "{wait}");
    assert_eq!(wait["step"], serde_json::json!(["merge"]));
    assert_eq!(
        wait["rule"]["where"],
        format!("event.pullRequest == \"{QUEUED_PULL_REQUEST}\"")
    );
    let record = run_record(&run);
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["attempts"], 8);
    assert_eq!(
        record["edges"],
        serde_json::json!({"review.hand_back": 2, "merge.repaired": 2})
    );

    // Queued: `resume` holds and changes nothing.
    let state = fs::read(run.join("state.json")).unwrap();
    let held = completed(&stub.command(&continue_args(&run, &[])).output().unwrap());
    assert_eq!(held["outcome"], "waiting", "{held}");
    assert_eq!(fs::read(run.join("state.json")).unwrap(), state);

    // Merged: `resume` continues at `merge`, which lands.
    stub.answer(QueueAnswer::Merged);
    decide(&bundle, "merge", "landed");
    let output = stub.command(&continue_args(&run, &[])).output().unwrap();
    assert_eq!(chained_outcomes(&output), ["succeeded"]);
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "landed");
    assert_eq!(record["attempts"], 9);
    assert_eq!(
        record["edges"],
        serde_json::json!({"review.hand_back": 2, "merge.repaired": 2})
    );

    let output = run_control(None, "show", &run, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let shown: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(shown["record"], record);
    let edge = |step: &str, outcome: &str, target: &str, counted: bool| serde_json::json!({"step": step, "outcome": outcome, "target": target, "counted": counted});
    let hand_back = edge("review", "hand_back", "fix", true);
    let built = edge("fix", "built", "review", false);
    let repaired = edge("merge", "repaired", "merge", true);
    let none = serde_json::Value::Null;
    let expected = [
        (none.clone(), none.clone()),
        ("edge".into(), hand_back.clone()),
        ("edge".into(), built.clone()),
        ("edge".into(), hand_back),
        ("edge".into(), built),
        ("retry".into(), none.clone()),
        ("edge".into(), repaired.clone()),
        ("edge".into(), repaired),
        ("resume".into(), none),
    ];
    let attempts = shown["attempts"].as_array().unwrap();
    let actual = attempts
        .iter()
        .map(|attempt| (attempt["reason"].clone(), attempt["edge"].clone()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{shown}");
    assert!(
        shown_plain(&run)
            .lines()
            .any(|line| line == "edges: merge.repaired=2, review.hand_back=2"),
        "{}",
        shown_plain(&run)
    );
}
