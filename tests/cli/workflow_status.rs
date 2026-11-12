use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

#[cfg(target_os = "linux")]
use nix::errno::Errno;
#[cfg(target_os = "linux")]
use nix::sys::signal::killpg;
#[cfg(target_os = "linux")]
use nix::unistd::Pid as NixPid;
use rustix::process::{Pid, Signal, kill_process};

#[cfg(target_os = "linux")]
use super::poll_until;
use super::workflow_definition::{Home, Tree, read_json};
use super::workflow_run::{
    RunBundle, child_tree, child_tree_args, finalizer_signal_bundle, isolated_command, run,
    run_record, signal_bundle, spawn_held_child_tree, tree_finalizer_signal_bundle,
};
use super::workflow_view::workflow_view_schema;

fn successful_bundle() -> RunBundle {
    RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    )
}

fn completed_run(name: &str) -> (RunBundle, PathBuf) {
    let bundle = successful_bundle();
    let destination = bundle.result(name);
    let output = run(&bundle.args(&destination));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (bundle, destination)
}

fn status_args(run_directory: &Path, options: &[&str]) -> Vec<String> {
    let mut args = vec![
        "workflow".to_owned(),
        "run".to_owned(),
        "status".to_owned(),
        run_directory.to_string_lossy().into_owned(),
    ];
    args.extend(options.iter().map(|option| (*option).to_owned()));
    args
}

fn status(run_directory: &Path, options: &[&str]) -> std::process::Output {
    isolated_command(&status_args(run_directory, options))
        .output()
        .unwrap()
}

fn status_json(run_directory: &Path) -> (std::process::Output, serde_json::Value) {
    let output = status(run_directory, &["--json"]);
    let value = serde_json::from_slice(&output.stdout).unwrap();
    (output, value)
}

#[cfg(target_os = "linux")]
fn wait_for_process_group_quiescence(process_group: i32) {
    let process_group_path = Path::new("/proc").join(process_group.to_string());
    let process_group = NixPid::from_raw(process_group);
    let _final_observation = poll_until(
        "guarded process group quiescence",
        || {
            (
                killpg(process_group, None::<nix::sys::signal::Signal>),
                process_group_path.exists(),
            )
        },
        |(observation, leader_exists)| matches!(observation, Err(Errno::ESRCH)) && !leader_exists,
    );
}

fn read_state(run_directory: &Path) -> serde_json::Value {
    serde_json::from_slice(&fs::read(run_directory.join("state.json")).unwrap()).unwrap()
}

fn write_state(run_directory: &Path, state: &serde_json::Value) {
    let mut bytes = serde_json::to_vec_pretty(state).unwrap();
    bytes.push(b'\n');
    fs::write(run_directory.join("state.json"), bytes).unwrap();
}

pub(super) fn status_schema() -> jsonschema::Validator {
    let schema: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schemas/workflow-status-result-v1.schema.json"
    )))
    .unwrap();
    jsonschema::draft202012::new(&schema).unwrap()
}

// REQ-WFRECOVERY-46, REQ-LOCALRUN-145
#[test]
fn settled_command_recovery_status_retains_terminal_evidence() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  verify:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv:
            - /bin/sh
            - -c
            - |
              : > repaired
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"repaired","reason":"recheck unchanged target"}' > "$UM_RECOVERY_RESULT"
    command:
      argv: [/bin/sh, -c, 'test -f repaired || exit 75']
"#,
    );
    let run_directory = bundle.result("settled-command-recovery-status");
    let output = run(&bundle.args(&run_directory));
    assert!(
        output.status.success(),
        "workflow run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = status(&run_directory, &["--plain", "--color", "never"]);
    assert!(output.status.success());
    let output = String::from_utf8(output.stdout).unwrap();
    for evidence in [
        "target execution 2",
        "output owner target execution 2",
        "decision recheck",
        "latest target failure execution · command_exit · exit 75",
    ] {
        assert!(
            output.contains(evidence),
            "plain status omitted {evidence:?}:\n{output}"
        );
    }
}

#[test]
fn settled_exhaustion_status_uses_the_terminal_target_failure() {
    let bundle = RunBundle::new(
        r#"schemaVersion: 1
steps:
  verify:
    kind: cmd
    recovery:
      retries: 1
    command:
      argv:
        - /bin/sh
        - -c
        - |
          if test -f attempted; then
            exit 76
          fi
          : > attempted
          exit 75
"#,
    );
    let run_directory = bundle.result("settled-exhaustion-status");
    let output = run(&bundle.args(&run_directory));
    assert_eq!(
        output.status.code(),
        Some(1),
        "workflow run returned an unexpected status: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = status(&run_directory, &["--plain", "--color", "never"]);
    assert!(output.status.success());
    let output = String::from_utf8(output.stdout).unwrap();
    for evidence in [
        "target execution 2",
        "no output owner",
        "latest target failure execution · command_exit · exit 76",
    ] {
        assert!(
            output.contains(evidence),
            "plain status omitted {evidence:?}:\n{output}"
        );
    }
}

// REQ-WFFINALIZE-19, REQ-WFEXEC-50, REQ-LOCALRUN-146, REQ-LOCALRUN-175
#[test]
fn live_finalization_status_is_incomplete_role_aware_and_retry_ineligible() {
    // A closure run has no run record; a tree run's names no outcome while it is live.
    let closure = finalizer_signal_bundle();
    let closure_directory = closure.result("live-finalization");
    let mut closure_args = closure.args(&closure_directory);
    closure_args.insert(closure_args.len() - 1, "--json".to_owned());
    let tree = tree_finalizer_signal_bundle();
    let tree_directory = tree.result("live-finalization");
    let tree_args = tree.args(&tree_directory);
    for (args, run_directory, tree_run) in [
        (closure_args, closure_directory, false),
        (tree_args, tree_directory, true),
    ] {
        assert_live_finalization_status(&args, &run_directory, tree_run);
    }
}

fn assert_live_finalization_status(args: &[String], run_directory: &Path, tree_run: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let child = isolated_command(args)
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "signal-exit")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut ready = [0_u8; 1];
    control.read_exact(&mut ready).unwrap();
    assert_eq!(ready, [1]);

    let (output, result) = status_json(run_directory);
    assert!(output.status.success());
    assert!(status_schema().is_valid(&result), "{result}");
    if tree_run {
        assert_eq!(result["record"], run_record(run_directory));
        assert_eq!(result["record"]["state"], "running", "{result}");
        assert_eq!(result["record"]["outcome"], serde_json::Value::Null);
    } else {
        assert_eq!(result["record"], serde_json::Value::Null, "{result}");
    }
    assert_eq!(result["state"]["attempts"][0]["state"], "running");
    assert_eq!(
        result["state"]["attempts"][0]["finalization"]["complete"],
        false
    );
    assert_eq!(
        result["state"]["attempts"][0]["finalization"]["trigger"],
        "succeeded"
    );
    assert_eq!(
        result["state"]["attempts"][0]["finalization"]["finalizers"][0]["role"],
        "finalizer"
    );
    assert!(matches!(
        result["state"]["attempts"][0]["finalization"]["finalizers"][0]["state"].as_str(),
        Some("starting" | "running")
    ));
    assert_eq!(result["recovery"]["status"], "active");
    assert_eq!(
        result["retry"],
        serde_json::json!({"eligible": false, "reason": "run_locked"})
    );

    let owner = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(owner, Signal::INT).unwrap();
    assert_eq!(child.wait_with_output().unwrap().status.code(), Some(130));

    let (output, settled) = status_json(run_directory);
    assert!(output.status.success());
    assert!(status_schema().is_valid(&settled), "{settled}");
    if tree_run {
        assert_eq!(settled["record"], run_record(run_directory));
        assert_eq!(settled["record"]["state"], "exited", "{settled}");
        assert_eq!(settled["record"]["outcome"], "canceled");
    } else {
        assert_eq!(settled["record"], serde_json::Value::Null, "{settled}");
    }
    assert_eq!(settled["state"]["attempts"][0]["state"], "cancelled");
    assert_eq!(
        settled["state"]["attempts"][0]["finalization"]["complete"],
        true
    );
    assert_eq!(
        settled["state"]["attempts"][0]["finalization"]["cancellation"]["reason"],
        "user_request"
    );
    assert_eq!(settled["retry"]["eligible"], true);
}

#[test]
fn status_schema_rejects_active_ownership_with_eligible_retry() {
    let mut value: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/workflow-status/v1/valid/settled-interrupted.json"
    )))
    .unwrap();
    value["recovery"] = serde_json::json!({"status": "active"});

    assert!(
        !status_schema().is_valid(&value),
        "active ownership requires retry reason run_locked"
    );
}

#[test]
fn status_schema_bounds_result_publication_invariants() {
    let (_bundle, run_directory) = completed_run("status-schema-result-invariant");
    let (output, mut value) = status_json(&run_directory);
    assert!(output.status.success());
    value["state"]["attempts"][0]["result"] = serde_json::json!({
        "status": "publication_failed",
        "phase": "serialization",
        "resultInvariant": "export_values"
    });
    value["state"]["diagnostics"] = serde_json::json!([{
        "sequence": 1,
        "attemptNumber": 1,
        "code": "result_publication_failure"
    }]);
    assert!(status_schema().is_valid(&value));

    value["state"]["attempts"][0]["result"]["phase"] = serde_json::json!("rename");
    assert!(!status_schema().is_valid(&value));
    value["state"]["attempts"][0]["result"]["phase"] = serde_json::json!("serialization");
    value["state"]["attempts"][0]["result"]["resultInvariant"] =
        serde_json::json!("export_values:secret");
    assert!(!status_schema().is_valid(&value));

    let mut interrupted: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/workflow-status/v1/valid/settled-interrupted.json"
    )))
    .unwrap();
    interrupted["state"]["attempts"][0]["result"]["resultInvariant"] =
        serde_json::json!("export_values");
    assert!(!status_schema().is_valid(&interrupted));
    interrupted["state"]["attempts"][0]["result"] = serde_json::json!({
        "status": "not_published",
        "reason": "interrupted"
    });
    interrupted["state"]["diagnostics"] = serde_json::json!([{
        "sequence": 1,
        "attemptNumber": 1,
        "code": "result_publication_failure",
        "resultInvariant": "export_values"
    }]);
    assert!(!status_schema().is_valid(&interrupted));
}

// REQ-LOCALRUN-198
#[test]
fn status_schema_admits_a_closed_diagnostic_only_on_a_failed_node() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: {argv: [\"false\"]}\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: {argv: [\"false\"]}\n",
    );
    let run_directory = bundle.result("status-schema-failed-diagnostic");
    assert_eq!(run(&bundle.args(&run_directory)).status.code(), Some(1));
    let (output, failed) = status_json(&run_directory);
    assert!(output.status.success());
    let diagnostic = serde_json::json!({"harnessFailure": "model_error", "httpStatus": 429});
    for node in ["/progress/steps/0", "/finalization/finalizers/0"] {
        let pointer = format!("/state/attempts/0{node}");
        let mut value = failed.clone();
        let target = value.pointer_mut(&pointer).unwrap();
        assert_eq!(target["state"], "failed", "{pointer}");
        target["diagnostic"] = diagnostic.clone();
        assert!(status_schema().is_valid(&value), "{pointer}");
        for invalid in [serde_json::json!({}), serde_json::json!({"unknown": 1})] {
            value.pointer_mut(&pointer).unwrap()["diagnostic"] = invalid;
            assert!(!status_schema().is_valid(&value), "{pointer}");
        }
    }

    let (_bundle, run_directory) = completed_run("status-schema-succeeded-diagnostic");
    let (output, mut succeeded) = status_json(&run_directory);
    assert!(output.status.success());
    assert!(status_schema().is_valid(&succeeded));
    succeeded["state"]["attempts"][0]["progress"]["steps"][0]["diagnostic"] = diagnostic;
    assert!(!status_schema().is_valid(&succeeded));
}

// REQ-LOCALRUN-201
#[test]
fn status_json_records_when_a_step_that_ran_started() {
    let bundle = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: {argv: [\"false\"]}\n  after:\n    kind: cmd\n    dependsOn: [work]\n    command: {argv: [\"true\"]}\n",
    );
    let run_directory = bundle.result("status-step-started-at");
    assert_eq!(run(&bundle.args(&run_directory)).status.code(), Some(1));
    let (output, value) = status_json(&run_directory);
    assert!(output.status.success());
    assert!(status_schema().is_valid(&value), "{value}");
    let attempt = &value["state"]["attempts"][0];
    let utc = |field: &serde_json::Value| {
        let text = field.as_str().unwrap();
        assert!(text.ends_with('Z'), "{text}");
        time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).unwrap()
    };
    let steps = &attempt["progress"]["steps"];
    assert_eq!(steps[0]["id"], "work");
    assert_eq!(steps[0]["state"], "failed");
    let started = utc(&steps[0]["startedAt"]);
    assert!(utc(&attempt["createdAt"]) <= started);
    assert!(started <= utc(&attempt["settledAt"]));
    assert_eq!(steps[1]["id"], "after");
    assert_eq!(steps[1]["state"], "blocked");
    assert!(steps[1].get("startedAt").is_none(), "{}", steps[1]);

    // The schema admits a start on a node that left `pending` and on no pending node.
    let after = "/state/attempts/0/progress/steps/1";
    let mut blocked = value.clone();
    blocked.pointer_mut(after).unwrap()["startedAt"] = steps[0]["startedAt"].clone();
    assert!(status_schema().is_valid(&blocked), "{blocked}");
    let mut pending = value.clone();
    let node = pending.pointer_mut(after).unwrap().as_object_mut().unwrap();
    node.insert("state".to_owned(), "pending".into());
    node.remove("detail").unwrap();
    assert!(status_schema().is_valid(&pending), "{pending}");
    pending.pointer_mut(after).unwrap()["startedAt"] = steps[0]["startedAt"].clone();
    assert!(!status_schema().is_valid(&pending), "{pending}");
}

#[test]
fn closed_status_schema_accepts_positive_and_rejects_negative_fixtures() {
    let validator = status_schema();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/workflow-status/v1");
    for (category, valid) in [("valid", true), ("invalid", false)] {
        let mut fixtures = fs::read_dir(root.join(category))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        fixtures.sort_unstable_by_key(std::fs::DirEntry::file_name);
        assert!(!fixtures.is_empty());
        for fixture in fixtures {
            let value: serde_json::Value =
                serde_json::from_slice(&fs::read(fixture.path()).unwrap()).unwrap();
            assert_eq!(
                validator.is_valid(&value),
                valid,
                "unexpected schema result for {}",
                fixture.path().display()
            );
        }
    }
}

// REQ-LOCALRUN-149, REQ-LOCALRUN-150
#[test]
fn status_json_records_committed_pending_result_with_skipped_nodes() {
    let bundle = RunBundle::new(include_str!(
        "../fixtures/workflow-run/conditional-skip.yaml"
    ));
    let run_directory = bundle.result("skipped-pending-result");
    let output = run(&bundle.args(&run_directory));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut state = read_state(&run_directory);
    let published = state["attempts"][0]["result"].clone();
    state["attempts"][0]["result"] =
        serde_json::json!({"status": "not_published", "reason": "publication_pending"});
    write_state(&run_directory, &state);

    let (output, status) = status_json(&run_directory);
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(output.stderr.is_empty());
    assert!(status_schema().is_valid(&status));
    assert_eq!(
        read_state(&run_directory)["attempts"][0]["result"],
        published
    );
}

// REQ-LOCALRUN-150, REQ-LOCALRUN-165
#[test]
fn status_json_reports_invalid_committed_pending_result() {
    let (_bundle, run_directory) = completed_run("invalid-pending-result");
    let mut state = read_state(&run_directory);
    state["attempts"][0]["result"] =
        serde_json::json!({"status": "not_published", "reason": "publication_pending"});
    write_state(&run_directory, &state);
    fs::write(
        run_directory.join("attempts/000001/result/result.json"),
        b"invalid",
    )
    .unwrap();

    let (output, error) = status_json(&run_directory);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert!(status_schema().is_valid(&error));
    assert_eq!(error["outcome"], "error");
    assert_eq!(error["error"]["code"], "published_result_invalid");
    assert_eq!(
        error["runDirectory"],
        fs::canonicalize(&run_directory).unwrap().to_str().unwrap()
    );
    assert_eq!(read_state(&run_directory), state);
}

// REQ-CONTINUE-30, REQ-LOCALRUN-121, REQ-LOCALRUN-144, REQ-LOCALRUN-145, REQ-LOCALRUN-146, REQ-LOCALRUN-149
#[test]
fn status_json_and_plain_are_closed_read_only_snapshots() {
    let (_bundle, run_directory) = completed_run("settled");
    let run_before = fs::read(run_directory.join("local-run.json")).unwrap();
    let state_before = fs::read(run_directory.join("state.json")).unwrap();
    let lock_before = fs::metadata(run_directory.join("run.lock")).unwrap();

    let output = status(&run_directory, &["--json", "--color", "always"]);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(!output.stdout.contains(&0x1b));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(status_schema().is_valid(&result));
    assert_eq!(
        result
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        [
            "command",
            "continuation",
            "exitStatus",
            "outcome",
            "record",
            "recovery",
            "retry",
            "run",
            "runDirectory",
            "schemaVersion",
            "state",
            "wait",
            "workspaceModified",
        ]
    );
    assert_eq!(result["schemaVersion"], 1);
    assert_eq!(result["command"], "um workflow run status");
    assert_eq!(result["outcome"], "status");
    assert_eq!(result["exitStatus"], 0);
    assert_eq!(result["record"], serde_json::Value::Null);
    assert_eq!(result["wait"], serde_json::Value::Null);
    assert_eq!(result["recovery"], serde_json::json!({"status": "settled"}));
    assert_eq!(result["workspaceModified"], "unknown");
    assert_eq!(result["continuation"]["requestAdmission"], "not_evaluated");
    assert_eq!(
        result["retry"],
        serde_json::json!({
            "eligible": false,
            "reason": "latest_attempt_succeeded"
        })
    );
    let expected_run: serde_json::Value = serde_json::from_slice(&run_before).unwrap();
    let mut expected_state: serde_json::Value = serde_json::from_slice(&state_before).unwrap();
    for attempt in expected_state["attempts"].as_array_mut().unwrap() {
        attempt["forceAbort"] = serde_json::Value::Null;
    }
    assert_eq!(result["run"], expected_run);
    assert_eq!(result["state"], expected_state);
    assert!(result.get("private").is_none());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(".private"));

    let plain = status(&run_directory, &[]);
    assert!(plain.status.success());
    assert!(plain.stderr.is_empty());
    assert!(!plain.stdout.contains(&0x1b));
    assert!(!plain.stdout.is_empty());

    let colored = status(&run_directory, &["--plain", "--color", "always"]);
    assert!(colored.status.success());
    assert!(colored.stdout.contains(&0x1b));
    assert!(
        String::from_utf8_lossy(&colored.stdout).contains("\u{1b}[0m (attempts/000001/result)")
    );

    assert_eq!(
        fs::read(run_directory.join("local-run.json")).unwrap(),
        run_before
    );
    assert_eq!(
        fs::read(run_directory.join("state.json")).unwrap(),
        state_before
    );
    let lock_after = fs::metadata(run_directory.join("run.lock")).unwrap();
    assert_eq!(lock_after.len(), lock_before.len());
}

// REQ-LOCALRUN-147, REQ-LOCALRUN-149
#[test]
fn active_owner_has_retry_precedence_and_status_does_not_signal_work() {
    let bundle = signal_bundle();
    let run_directory = bundle.result("active");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let child = isolated_command(&bundle.args(&run_directory))
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "signal-exit")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut event = [0_u8; 1];
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [1]);

    let (output, result) = status_json(&run_directory);

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(result["recovery"], serde_json::json!({"status": "active"}));
    assert_eq!(
        result["retry"],
        serde_json::json!({"eligible": false, "reason": "run_locked"})
    );
    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(pid, Signal::INT).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [2]);
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(130));
}

#[test]
fn orphaned_guarded_command_is_reaped_and_retry_eligible() {
    let bundle = signal_bundle();
    let run_directory = bundle.result("guarded-owner-loss");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut child = isolated_command(&bundle.args(&run_directory))
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "wait")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut ready = [0_u8; 1];
    control.read_exact(&mut ready).unwrap();
    assert_eq!(ready, [1]);

    let state = read_state(&run_directory);
    assert_eq!(
        state["attempts"][0]["processGuards"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        state["attempts"][0]["progress"]["outstandingActions"][0]["kind"],
        "start_step"
    );

    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(pid, Signal::KILL).unwrap();
    assert!(!child.wait().unwrap().success());

    control
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut remaining = Vec::new();
    control.read_to_end(&mut remaining).unwrap();
    let (output, result) = status_json(&run_directory);

    assert!(output.status.success());
    assert_eq!(result["recovery"]["status"], "abandoned");
    assert_eq!(result["retry"], serde_json::json!({"eligible": true}));
}

#[cfg(target_os = "linux")]
#[test]
fn orphaned_guarded_cancellation_is_reaped_and_retry_eligible() {
    let bundle = signal_bundle();
    let run_directory = bundle.result("guarded-cancellation-owner-loss");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut child = isolated_command(&bundle.args(&run_directory))
        .env(
            "WORKFLOW_RUN_FIXTURE_SOCKET",
            listener.local_addr().unwrap().to_string(),
        )
        .env("WORKFLOW_RUN_FIXTURE_MODE", "signal-hold")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut control, _) = listener.accept().unwrap();
    let mut process_group = [0_u8; std::mem::size_of::<u32>()];
    control.read_exact(&mut process_group).unwrap();
    let process_group_raw = i32::try_from(u32::from_be_bytes(process_group)).unwrap();
    let mut event = [0_u8; 1];
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [1]);
    let owner = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    kill_process(owner, Signal::INT).unwrap();
    control.read_exact(&mut event).unwrap();
    assert_eq!(event, [2]);

    let state = read_state(&run_directory);
    assert_eq!(
        state["attempts"][0]["progress"]["outstandingActions"][0]["kind"],
        "cancel_step"
    );
    assert_eq!(
        state["attempts"][0]["processGuards"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    kill_process(owner, Signal::KILL).unwrap();
    assert!(!child.wait().unwrap().success());
    control
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut remaining = Vec::new();
    control.read_to_end(&mut remaining).unwrap();

    // Fixture EOF precedes the independent guard's reap boundary.
    wait_for_process_group_quiescence(process_group_raw);
    let (output, result) = status_json(&run_directory);
    assert!(output.status.success());
    assert_eq!(result["recovery"]["status"], "abandoned");
    assert_eq!(result["retry"], serde_json::json!({"eligible": true}));
}

// REQ-LOCALRUN-147, REQ-LOCALRUN-148
#[test]
fn status_distinguishes_interrupted_abandoned_and_unproven_ownership() {
    let (_bundle, run_directory) = completed_run("recoverable");
    let mut state = read_state(&run_directory);
    let attempt = &mut state["attempts"][0];
    attempt["state"] = serde_json::json!("interrupted");
    attempt["interruption"] = serde_json::json!({
        "cause": "execution_owner_lost",
        "executionMayHaveStarted": true,
        "cancellationRequested": false
    });
    attempt["result"] = serde_json::json!({"status": "not_published", "reason": "interrupted"});
    write_state(&run_directory, &state);

    let (output, interrupted) = status_json(&run_directory);
    assert!(output.status.success());
    assert_eq!(interrupted["state"]["attempts"][0]["state"], "interrupted");
    assert_eq!(interrupted["recovery"]["status"], "settled");
    assert_eq!(interrupted["retry"], serde_json::json!({"eligible": true}));

    let mut state = read_state(&run_directory);
    let attempt = &mut state["attempts"][0];
    attempt["state"] = serde_json::json!("running");
    attempt.as_object_mut().unwrap().remove("settledAt");
    attempt
        .as_object_mut()
        .unwrap()
        .remove("settlementSnapshot");
    attempt.as_object_mut().unwrap().remove("interruption");
    attempt["result"] =
        serde_json::json!({"status": "not_published", "reason": "attempt_nonterminal"});
    attempt["processGuards"] = serde_json::json!([]);
    write_state(&run_directory, &state);

    let (output, abandoned) = status_json(&run_directory);
    assert!(output.status.success());
    assert_eq!(abandoned["recovery"]["status"], "abandoned");
    assert_eq!(abandoned["retry"], serde_json::json!({"eligible": true}));

    let mut state = read_state(&run_directory);
    let attempt = &mut state["attempts"][0];
    attempt["processGuards"] = serde_json::json!([{
        "guardId": "11111111-1111-4111-8111-111111111111",
        "actionId": 1,
        "stepId": "complete",
        "nodeRole": "step",
        "state": "released",
        "executionHost": attempt["owner"]["executionHost"].clone(),
        "processGroupId": 1,
        "liveness": {
            "kind": "guard_handle_identity",
            "value": "retained-handle"
        }
    }]);
    write_state(&run_directory, &state);

    let (output, unproven) = status_json(&run_directory);
    assert!(output.status.success());
    assert!(status_schema().is_valid(&unproven));
    assert_eq!(unproven["recovery"]["status"], "ownership_unproven");
    assert_eq!(
        unproven["recovery"]["reason"],
        "process_identity_inspection_unavailable"
    );
    assert_eq!(
        unproven["recovery"]["guardIds"],
        serde_json::json!(["11111111-1111-4111-8111-111111111111"])
    );
    assert_eq!(
        unproven["retry"],
        serde_json::json!({"eligible": false, "reason": "ownership_unproven"})
    );
    let plain = status(&run_directory, &[]);
    assert!(plain.status.success());
    assert!(!plain.stdout.is_empty());
}

// REQ-LOCALRUN-151
#[test]
fn signals_interrupt_blocked_status_output_without_a_valid_object() {
    let (_bundle, run_directory) = completed_run("signal");
    let mut state = read_state(&run_directory);
    state["attempts"][0]
        .as_object_mut()
        .unwrap()
        .remove("definition");
    state["attempts"][0]["processGuards"] = serde_json::json!([]);
    state["attempts"][0]["progress"]["steps"] = serde_json::Value::Array(
        (0..20_000)
            .map(|index| {
                serde_json::json!({
                    "id": format!("step-{index:05}"),
                    "role": "step",
                    "failurePolicy": "required",
                    "state": "succeeded"
                })
            })
            .collect(),
    );
    write_state(&run_directory, &state);
    let state_size = fs::metadata(run_directory.join("state.json"))
        .unwrap()
        .len();
    assert!((1_048_576..4_194_304).contains(&state_size));

    for (signal, expected_status) in [(Signal::INT, 130), (Signal::TERM, 143)] {
        let mut child = isolated_command(&status_args(&run_directory, &["--json"]))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let mut bytes = vec![0_u8; 1];
        stdout.read_exact(&mut bytes).unwrap();

        let pid = Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
        kill_process(pid, signal).unwrap();
        let exit = child.wait().unwrap();
        stdout.read_to_end(&mut bytes).unwrap();
        let mut diagnostic = Vec::new();
        stderr.read_to_end(&mut diagnostic).unwrap();

        assert_eq!(exit.code(), Some(expected_status));
        assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_err());
        assert!(diagnostic.is_empty());
    }
}

// REQ-LOCALRUN-80
#[test]
fn status_usage_errors_return_two_without_a_status_object() {
    for args in [
        vec!["workflow", "run", "status", "--json"],
        vec!["workflow", "run", "status", "/tmp/run", "--plain", "--json"],
        vec![
            "workflow",
            "run",
            "status",
            "/tmp/run",
            "--execution-root",
            "/tmp/execution",
        ],
        vec!["workflow", "run", "status", "--run-dir", "/tmp/run"],
    ] {
        let args = args.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let output = isolated_command(&args).output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

// REQ-LOCALRUN-148, REQ-LOCALRUN-149, REQ-LOCALRUN-165
#[test]
fn invalid_run_directory_is_structured_and_missing_lock_is_not_created() {
    let (_bundle, run_directory) = completed_run("missing-lock");
    fs::remove_file(run_directory.join("run.lock")).unwrap();

    let (output, result) = status_json(&run_directory);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert!(status_schema().is_valid(&result));
    assert_eq!(result["outcome"], "error");
    assert_eq!(result["exitStatus"], 1);
    assert_eq!(result["error"]["code"], "run_directory_invalid");
    assert!(!run_directory.join("run.lock").exists());

    let plain = status(&run_directory, &[]);
    assert_eq!(plain.status.code(), Some(1));
    assert!(plain.stdout.is_empty());
    assert!(!plain.stderr.is_empty());
    assert!(!run_directory.join("run.lock").exists());

    let unavailable = status(&run_directory.join("absent"), &["--json"]);
    assert_eq!(unavailable.status.code(), Some(1));
    assert!(unavailable.stderr.is_empty());
    let unavailable: serde_json::Value = serde_json::from_slice(&unavailable.stdout).unwrap();
    assert_eq!(unavailable["error"]["code"], "run_directory_unavailable");
    assert!(unavailable.get("runDirectory").is_none());
}

// REQ-LOCALRUN-118, REQ-LOCALRUN-145, REQ-LOCALRUN-95
#[test]
fn plain_status_and_run_diagnostics_escape_host_run_directory_names() {
    // A doubled ESC abandons the first sequence, so the name keeps a visible
    // `\x1b` (REQ-LOCALRUN-115) and its override shows as `\u{202e}`
    // (REQ-LOCALRUN-116); raw, the second ESC would still colour the terminal.
    let hostile = "\u{1b}\u{1b}[31m-\u{202e}";
    let escaped = "\\x1b\\x1b[31m-\\u{202e}";
    let (bundle, run_directory) = completed_run(&format!("run-{hostile}"));

    let plain = status(&run_directory, &["--plain", "--color", "never"]);
    assert!(
        plain.status.success(),
        "{}",
        String::from_utf8_lossy(&plain.stderr)
    );
    assert!(!plain.stdout.contains(&0x1b));
    assert!(
        String::from_utf8(plain.stdout)
            .unwrap()
            .contains(&format!("run-{escaped}"))
    );

    let missing = status(&bundle.result(&format!("absent-{hostile}")), &[]);
    assert_eq!(missing.status.code(), Some(1));
    assert!(!missing.stderr.contains(&0x1b));
    assert!(
        String::from_utf8(missing.stderr)
            .unwrap()
            .contains(&format!("absent-{escaped}"))
    );

    let existing = run(&bundle.args(&run_directory));
    assert_eq!(existing.status.code(), Some(1));
    assert!(!existing.stderr.contains(&0x1b));
    assert!(
        String::from_utf8(existing.stderr)
            .unwrap()
            .contains(&format!("run-{escaped}"))
    );
}

// REQ-LOCALRUN-177, REQ-WFEXEC-50: a parent with `workflow` steps and each child read on
// their own, the steps reported as kind `workflow`.
#[test]
fn status_and_view_read_a_parent_and_its_child_runs() {
    let bundle = child_tree(super::workflow_run::HOLDING_CHILD_DECIDE);
    fs::write(bundle.execution_root().join("verdict-input"), "planned").unwrap();
    let destination = bundle.result("completed-parent");
    let output = run(&child_tree_args(&bundle, &destination));
    assert!(output.status.success(), "{output:?}");

    let (output, parent) = status_json(&destination);
    assert!(output.status.success(), "{output:?}");
    assert!(status_schema().is_valid(&parent), "{parent}");
    let view = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "view".to_owned(),
        destination.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(view.status.success(), "{view:?}");
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    assert!(workflow_view_schema().is_valid(&view), "{view}");
    let kinds = view["result"]["steps"]
        .as_array()
        .unwrap_or_else(|| panic!("{view}"))
        .iter()
        .map(|step| step["kind"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(kinds, ["workflow", "workflow"], "{view}");

    let child = destination.join("children/design/000001");
    let (output, result) = status_json(&child);
    assert!(output.status.success(), "{output:?}");
    assert!(status_schema().is_valid(&result), "{result}");
    assert_eq!(run_record(&child)["state"], "exited");
}

// REQ-LOCALRUN-177, REQ-LOCALRUN-147: a parent killed while its child runs leaves the
// child `running` with an abandoned attempt.
#[test]
fn a_child_whose_parent_was_killed_reports_an_abandoned_attempt() {
    let (_bundle, destination, mut parent) = spawn_held_child_tree("killed-parent");
    let pid = Pid::from_raw(i32::try_from(parent.id()).unwrap()).unwrap();
    kill_process(pid, Signal::KILL).unwrap();
    assert!(!parent.wait().unwrap().success());

    let child = destination.join("children/design/000001");
    assert_eq!(run_record(&child)["state"], "running");
    let (output, result) = super::poll_until(
        "the child's attempt is abandoned",
        || status_json(&child),
        |(_, result)| result["recovery"]["status"] == "abandoned",
    );
    assert!(output.status.success(), "{output:?}");
    assert!(status_schema().is_valid(&result), "{result}");
}

// REQ-LOCALRUN-179, REQ-LOCALRUN-146, REQ-LOCALRUN-145: a run whose attempt ended in a wait
// reports the attempt `waiting`, its wait record and an eligible continuation, and the
// archived view marks the outcome `waiting` and carries the attempt's wait record.
#[test]
fn status_and_view_report_a_waiting_run() {
    let bundle = child_tree(super::workflow_run::CHILD_TREE_DECIDE);
    bundle.write_source(
        "workflow.yaml",
        &super::workflow_run::CHILD_TREE_ROOT.replace(
            "      blocked: exit.failed\n",
            "      blocked:\n        wait: answer\n        then: build\n",
        ),
    );
    fs::write(bundle.execution_root().join("verdict-input"), "blocked").unwrap();
    let destination = bundle.result("waiting");
    let output = run(&child_tree_args(&bundle, &destination));
    assert!(output.status.success(), "{output:?}");

    let (output, snapshot) = status_json(&destination);
    assert!(output.status.success(), "{output:?}");
    assert!(status_schema().is_valid(&snapshot), "{snapshot}");
    let wait: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("wait.json")).unwrap()).unwrap();
    assert_eq!(snapshot["wait"], wait);
    assert_eq!(
        snapshot["state"]["attempts"][0]["state"], "waiting",
        "{snapshot}"
    );
    assert_eq!(snapshot["continuation"]["eligible"], true, "{snapshot}");
    assert_eq!(snapshot["record"]["state"], "waiting", "{snapshot}");

    let plain = status(&destination, &["--plain", "--color", "never"]);
    assert!(plain.status.success(), "{plain:?}");
    let plain = String::from_utf8(plain.stdout).unwrap();
    assert!(
        plain
            .lines()
            .any(|line| line.contains("answer") && line.contains("design")),
        "{plain}"
    );

    let view = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "view".to_owned(),
        destination.to_string_lossy().into_owned(),
        "--plain".to_owned(),
        "--color".to_owned(),
        "never".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(view.status.success(), "{view:?}");
    assert!(
        String::from_utf8(view.stdout).unwrap().contains("waiting"),
        "view marks the outcome waiting"
    );

    let view = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "view".to_owned(),
        destination.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ])
    .output()
    .unwrap();
    assert!(view.status.success(), "{view:?}");
    let view: serde_json::Value = serde_json::from_slice(&view.stdout).unwrap();
    assert!(workflow_view_schema().is_valid(&view), "{view}");
    assert_eq!(view["attemptState"], "waiting", "{view}");
    assert_eq!(view["result"]["outcome"], "waiting", "{view}");
    let attempt_wait: serde_json::Value =
        serde_json::from_slice(&fs::read(destination.join("attempts/000001/wait.json")).unwrap())
            .unwrap();
    assert_eq!(view["wait"], attempt_wait, "{view}");
}

const ADDRESSED_ROOT_WORKFLOW: &str = "schemaVersion: 1\nname: addressed\nprograms: [scripts]\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";

/// `um workflow run ARGS` against the `um` home `home`.
fn run_under_home(home: &Home, args: &[&str]) -> std::process::Output {
    let args = ["workflow", "run"]
        .iter()
        .chain(args)
        .map(|arg| (*arg).to_owned())
        .collect::<Vec<_>>();
    isolated_command(&args)
        .env("UM_HOME", home.path())
        .output()
        .unwrap()
}

// REQ-LOCALRUN-180, REQ-LOCALRUN-172
#[test]
fn status_addresses_a_run_under_the_home_by_its_id() {
    let tree = Tree::engineering();
    tree.write("workflow.yaml", ADDRESSED_ROOT_WORKFLOW);
    let home = Home::new();
    let report = home.publish(&tree, &[]);
    let definition = report["definition"]["id"].as_str().unwrap();
    let execution_root = tempfile::tempdir().unwrap();
    let created = run_under_home(
        &home,
        &[
            "create",
            definition,
            "--execution-root",
            execution_root.path().to_str().unwrap(),
            "--json",
        ],
    );
    assert!(created.status.success(), "{created:?}");
    let created: serde_json::Value = serde_json::from_slice(&created.stdout).unwrap();
    let run_directory = PathBuf::from(created["runDirectory"].as_str().unwrap());
    assert_eq!(
        run_directory.parent(),
        Some(
            home.definition_directory(definition)
                .join("runs")
                .canonicalize()
                .unwrap()
                .as_path()
        )
    );
    let id = read_json(&run_directory.join("run.json"))["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let by_id = run_under_home(&home, &["status", &id, "--json"]);
    assert_eq!(by_id.status.code(), Some(0), "{by_id:?}");
    let by_id: serde_json::Value = serde_json::from_slice(&by_id.stdout).unwrap();
    assert!(status_schema().is_valid(&by_id), "{by_id}");
    assert_eq!(by_id["command"], "um workflow run status");
    assert_eq!(by_id["runDirectory"], created["runDirectory"]);
    assert_eq!(by_id["record"]["id"], id.as_str());

    let unknown = "run_7zzzzzzzzzzzzzzzzzzzzzzzzz";
    let missing = run_under_home(&home, &["status", unknown, "--json"]);
    assert_eq!(missing.status.code(), Some(1), "{missing:?}");
    let missing: serde_json::Value = serde_json::from_slice(&missing.stdout).unwrap();
    assert_eq!(missing["command"], "um workflow run status");
    assert_eq!(missing["outcome"], "error");
    assert_eq!(missing["exitStatus"], 1);
    assert_eq!(missing["error"]["code"], "run_not_found", "{missing}");
    assert!(status_schema().is_valid(&missing), "{missing}");
    assert!(missing.get("runDirectory").is_none(), "{missing}");

    let plain = run_under_home(&home, &["status", unknown]);
    assert_eq!(plain.status.code(), Some(1), "{plain:?}");
    assert!(plain.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&plain.stderr).contains("run_not_found"),
        "{plain:?}"
    );
}
