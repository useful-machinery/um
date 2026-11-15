//! Nested workflow runs (REQ-WFEXEC-74): a root `workflow` step whose child declares
//! `workflow` steps of its own runs that child's stages and loops its edges inside the step.

use std::fs;
use std::path::Path;

use super::workflow_continue::{attempt_count, completed, ordinals_of, read_json};
use super::workflow_run::{TreeBundle, isolated_command, run_record};
use super::workflow_run_control::attempt_result;

/// The root of the nested tree: `outer` fans out over the `parts` input into the nested
/// workflow `part.yaml`, and `finish` follows it. The root admits both test variables.
pub(super) const NESTED_TREE_ROOT: &str = "schemaVersion: 1
programs: [bin]
environmentPassthrough: [NESTED_VISIBLE, NESTED_HIDDEN]
inputs:
  parts:
    kind: json
outcomes: [landed, failed]
steps:
  outer:
    kind: workflow
    workflow: part.yaml
    forEach: inputs.parts
    inputs:
      part:
        ref: item.name
    on:
      checked: finish
      failed: exit.failed
  finish:
    kind: workflow
    workflow: finish.yaml
    on:
      finished: exit.landed
      failed: exit.failed
";

/// The nested workflow: `work`, then `check`, whose first outcome hands back to `work` under
/// a limit of one and whose second exits. It lists only `NESTED_VISIBLE`.
pub(super) const NESTED_TREE_PART: &str = "schemaVersion: 1
environmentPassthrough: [NESTED_VISIBLE]
inputs:
  part:
    kind: text
outcomes: [checked, failed]
steps:
  work:
    kind: workflow
    workflow: work.yaml
    inputs:
      part:
        ref: inputs.part
    on:
      worked: check
      failed: exit.failed
  check:
    kind: workflow
    workflow: check.yaml
    inputs:
      part:
        ref: inputs.part
    on:
      again:
        then: work
        limit: 1
        else: exit.checked
      done: exit.checked
      failed: exit.failed
";

/// A stage that runs `stage NAME` for its `part` and routes on the verdict it writes. Every
/// stage lists both test variables, so the nested workflow's list alone narrows them.
fn nested_tree_stage(name: &str, outcomes: &str, part: bool) -> String {
    let (inputs, step_inputs) = if part {
        (
            "inputs:\n  part:\n    kind: text\n",
            "    inputs:\n      part:\n        ref: inputs.part\n",
        )
    } else {
        ("", "")
    };
    format!(
        "schemaVersion: 1
environmentPassthrough: [NESTED_VISIBLE, NESTED_HIDDEN]
{inputs}outcomes: [{outcomes}]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
{step_inputs}    command:
      argv: [stage, {name}]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict-{name}.txt
"
    )
}

/// `stage NAME` appends `NAME PART` to the execution root's `stages.log` and the values of
/// both test variables it sees to `seen-NAME-PART`; `check` decides `again` the first time it
/// runs for a part and `done` after that.
const NESTED_TREE_PROGRAM: &str = "#!/bin/sh
set -eu
stage=$1
part=-
if [ -f \"${UM_STEP_INPUTS:-}/values/part\" ]; then
  part=$(cat \"$UM_STEP_INPUTS/values/part\")
fi
printf '%s %s\\n' \"$stage\" \"$part\" >> stages.log
printf 'visible=%s hidden=%s\\n' \"${NESTED_VISIBLE-unset}\" \"${NESTED_HIDDEN-unset}\" >> \"seen-$stage-$part\"
case $stage in
  work) printf worked ;;
  check) if [ -e \"checked-$part\" ]; then printf done; else : > \"checked-$part\"; printf again; fi ;;
  finish) printf finished ;;
esac > \"verdict-$stage.txt\"
";

/// The nested tree: the root, `part.yaml` and the stages `work`, `check` and `finish`.
pub(super) fn nested_tree() -> TreeBundle {
    let bundle = TreeBundle::new(NESTED_TREE_ROOT);
    bundle.write_source("part.yaml", NESTED_TREE_PART);
    bundle.write_source(
        "work.yaml",
        &nested_tree_stage("work", "worked, failed", true),
    );
    bundle.write_source(
        "check.yaml",
        &nested_tree_stage("check", "again, done, failed", true),
    );
    bundle.write_source(
        "finish.yaml",
        &nested_tree_stage("finish", "finished, failed", false),
    );
    bundle.write_program("bin/stage", NESTED_TREE_PROGRAM);
    bundle
}

/// `um workflow run create` of the nested tree over parts `a` and `b`, with both test
/// variables set in the launching environment.
pub(super) fn create_nested(bundle: &TreeBundle, name: &str) -> std::process::Output {
    nested_command(bundle, name).output().unwrap()
}

/// The command `create_nested` runs, to spawn.
pub(super) fn nested_command(bundle: &TreeBundle, name: &str) -> std::process::Command {
    let mut command = isolated_command(&nested_args(bundle, name, &[]));
    command
        .env("NESTED_VISIBLE", "shown")
        .env("NESTED_HIDDEN", "secret");
    command
}

/// The arguments of `um workflow run create` of the nested tree over parts `a` and `b` into
/// the run directory `name`, with `extra` before the last.
fn nested_args(bundle: &TreeBundle, name: &str, extra: &[&str]) -> Vec<String> {
    let mut args = bundle.args(&bundle.result(name));
    let parts = r#"[{"name":"a"},{"name":"b"}]"#;
    args.splice(
        args.len() - 1..args.len() - 1,
        ["--input-json", "parts", parts]
            .iter()
            .chain(extra)
            .map(|arg| (*arg).to_owned()),
    );
    args
}

/// The nested run of `outer`'s element `item` under the root run `run`.
pub(super) fn nested_run(run: &Path, item: u64) -> std::path::PathBuf {
    run.join(format!("children/outer/{item:06}/000001"))
}

// REQ-WFEXEC-74, INV-ENGINE-40: each element's nested run runs its stages under its own
// directory, loops its limited edge in new attempts of the same run with its own count, and
// exits with the outcome the root routes on; the nested workflow's environment list narrows
// its stages'.
#[test]
fn a_nested_workflow_runs_its_stages_and_loops_its_edges_inside_the_parents_step() {
    let bundle = nested_tree();
    let output = create_nested(&bundle, "nested");
    let terminal = completed(&output);
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    let run = fs::canonicalize(bundle.result("nested")).unwrap();
    let root = run_record(&run);
    assert_eq!(root["state"], "exited", "{root}");
    assert_eq!(root["outcome"], "landed", "{root}");
    assert_eq!(root["edges"], serde_json::json!({}), "{root}");
    assert_eq!(attempt_count(&run), 1);
    assert_eq!(
        ordinals_of(&run.join("children/outer")),
        ["000001", "000002"]
    );

    for (item, part) in [(1, "a"), (2, "b")] {
        let nested = nested_run(&run, item);
        assert_eq!(ordinals_of(nested.parent().unwrap()), ["000001"]);
        let record = run_record(&nested);
        assert_eq!(record["state"], "exited", "{record}");
        assert_eq!(record["outcome"], "checked", "{record}");
        assert_eq!(
            record["parent"],
            serde_json::json!({"run": root["id"], "step": "outer", "item": item})
        );
        assert_eq!(
            record["edges"],
            serde_json::json!({"check.again": 1}),
            "{record}"
        );
        assert_eq!(record["attempts"], 2, "{record}");
        assert_eq!(attempt_count(&nested), 2);
        for stage in ["work", "check"] {
            let stages = nested.join("children").join(stage);
            assert_eq!(ordinals_of(&stages), ["000001", "000002"], "{part} {stage}");
            for ordinal in ["000001", "000002"] {
                let stage_record = read_json(&stages.join(ordinal).join("run.json"));
                assert_eq!(stage_record["parent"]["run"], record["id"]);
                assert_eq!(stage_record["parent"]["step"], stage);
                assert_eq!(stage_record["state"], "exited");
            }
            assert_eq!(
                fs::read_to_string(bundle.execution_root().join(format!("seen-{stage}-{part}")))
                    .unwrap(),
                "visible=shown hidden=unset\n".repeat(2),
                "{part} {stage}"
            );
        }
        assert!(!run.join("children/work").exists());
    }
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("stages.log")).unwrap(),
        "work a\ncheck a\nwork a\ncheck a\nwork b\ncheck b\nwork b\ncheck b\nfinish -\n"
    );
}

/// The nested workflow of the asking tree: `ask` waits for answers when it is blocked, and
/// `work` after it waits for a repair when it fails; each wait continues at its own step.
pub(super) const ASKING_TREE_PART: &str = "schemaVersion: 1
environmentPassthrough: [NESTED_VISIBLE]
inputs:
  part:
    kind: text
outcomes: [checked, failed]
steps:
  ask:
    kind: workflow
    workflow: ask.yaml
    inputs:
      part:
        ref: inputs.part
      answers:
        ref: controls.answers
    on:
      asked: work
      blocked:
        wait: answer
        then: ask
      failed: exit.failed
  work:
    kind: workflow
    workflow: work.yaml
    inputs:
      part:
        ref: inputs.part
    on:
      worked: exit.checked
      failed:
        wait: repair
        then: work
";

/// A stage of the asking tree that runs `nested-step NAME VERDICT` for its `part`. `ask`
/// also takes the `answers` its step maps from `controls.answers` and exports the questions
/// and summary its wait's payload carries; `work` first runs `prep`, which `decide` follows.
fn asking_tree_stage(name: &str, outcomes: &str, verdict: &str) -> String {
    let (answers, mapped, prep, exported) = match name {
        "ask" => (
            "  answers:\n    kind: json\n    optional: true\n",
            "      answers:\n        ref: inputs.answers\n",
            "",
            "      questions:\n        kind: text\n        from: path\n        path: questions.txt\n      summary:\n        kind: text\n        from: path\n        path: summary.txt\nexports:\n  questions:\n    ref: outputs.decide.questions\n  summary:\n    ref: outputs.decide.summary\n",
        ),
        "work" => (
            "",
            "",
            "  prep:\n    kind: cmd\n    inputs:\n      part:\n        ref: inputs.part\n    command:\n      argv: [nested-prep]\n  decide:\n    dependsOn: [prep]\n",
            "",
        ),
        _ => ("", "", "", ""),
    };
    let decide = if prep.is_empty() { "  decide:\n" } else { "" };
    format!(
        "schemaVersion: 1
inputs:
  part:
    kind: text
{answers}outcomes: [{outcomes}]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
{prep}{decide}    kind: cmd
    inputs:
      part:
        ref: inputs.part
{mapped}    command:
      argv: [nested-step, {name}, {verdict}]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict-{name}.txt
{exported}"
    )
}

/// `nested-step NAME VERDICT` appends `NAME PART` to the execution root's `stages.log` and
/// records the answers it was given, when there are any, as `answers-NAME-PART`; while the
/// execution root holds `hold-NAME-PART` it writes `held-NAME-PART` and sleeps until
/// `release-NAME-PART` appears; it fails while `fail-NAME-PART` exists, and otherwise reports
/// the verdict `verdict-NAME-PART` holds, else `VERDICT`, with a question and a summary about
/// the part.
const ASKING_TREE_PROGRAM: &str = "#!/bin/sh
set -eu
stage=$1
part=-
if [ -f \"${UM_STEP_INPUTS:-}/values/part\" ]; then
  part=$(cat \"$UM_STEP_INPUTS/values/part\")
fi
printf '%s %s\\n' \"$stage\" \"$part\" >> stages.log
if [ -f \"${UM_STEP_INPUTS:-}/values/answers\" ]; then
  answers=$(cat \"$UM_STEP_INPUTS/values/answers\")
  if [ \"$answers\" != null ]; then
    printf '%s' \"$answers\" > \"answers-$stage-$part\"
  fi
fi
if [ -e \"hold-$stage-$part\" ]; then
  : > \"held-$stage-$part\"
  while [ ! -e \"release-$stage-$part\" ]; do sleep 0.1; done
fi
if [ -e \"fail-$stage-$part\" ]; then
  exit 1
fi
if [ -e \"verdict-$stage-$part\" ]; then
  cat \"verdict-$stage-$part\" > \"verdict-$stage.txt\"
else
  printf '%s' \"$2\" > \"verdict-$stage.txt\"
fi
printf '1. which way for %s?' \"$part\" > questions.txt
printf '%s needs a decision' \"$part\" > summary.txt
";

/// `nested-prep` appends `prep PART` to the execution root's `preps.log`.
const ASKING_TREE_PREP: &str = "#!/bin/sh
set -eu
printf 'prep %s\\n' \"$(cat \"$UM_STEP_INPUTS/values/part\")\" >> preps.log
";

/// The nested tree whose nested workflow is `ASKING_TREE_PART`: `outer` fans out over parts
/// `a` and `b` into `ask` and then `work`, and `finish` follows.
pub(super) fn asking_tree() -> TreeBundle {
    let bundle = nested_tree();
    bundle.write_source("part.yaml", ASKING_TREE_PART);
    bundle.write_source(
        "ask.yaml",
        &asking_tree_stage("ask", "asked, blocked, failed", "asked"),
    );
    bundle.write_source(
        "work.yaml",
        &asking_tree_stage("work", "worked, failed", "worked"),
    );
    bundle.write_program("bin/nested-step", ASKING_TREE_PROGRAM);
    bundle.write_program("bin/nested-prep", ASKING_TREE_PREP);
    bundle
}

/// The rule an `answer` wait derives (REQ-WFEXEC-61).
fn answer_rule() -> serde_json::Value {
    serde_json::json!({
        "source": "um",
        "event": ["control.answer"],
        "where": "event.run == run.id and event.wait == wait.id and event.payload.complete == true",
    })
}

/// The root's published wait record, checked against the schema and its latest attempt's
/// copy, and the nested run's own record, which shares its kind and `since`.
fn carried_waits(run: &Path, item: u64) -> (serde_json::Value, serde_json::Value) {
    let wait = read_json(&run.join("wait.json"));
    assert!(
        super::workflow_definition::schema("workflow-run-wait-v1.schema.json").is_valid(&wait),
        "{wait}"
    );
    let latest = attempt_count(run);
    assert_eq!(
        wait,
        read_json(&run.join(format!("attempts/{latest:06}/wait.json")))
    );
    let nested = read_json(&nested_run(run, item).join("wait.json"));
    assert_eq!(nested["kind"], wait["kind"], "{nested}");
    assert_eq!(nested["since"], wait["since"], "{nested}");
    (wait, nested)
}

// REQ-WFEXEC-75, REQ-WFEXEC-68, REQ-WFDEF-4, REQ-WFDEF-5: a nested run's `answer` wait is
// entered in its own directory and carried by the root, whose record waits at the full
// path with the nested wait's kind, since and payload, `then` its own step and the rule the
// kind derives for it; the root's step ends `not_reached` and takes no edge.
#[test]
fn a_nested_runs_wait_is_carried_by_the_root_at_its_full_path() {
    let bundle = asking_tree();
    let execution = bundle.execution_root();
    fs::write(execution.join("verdict-ask-b"), "blocked").unwrap();
    let output = create_nested(&bundle, "asking");
    let terminal = completed(&output);
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    let run = fs::canonicalize(bundle.result("asking")).unwrap();

    let path = serde_json::json!(["outer", {"item": 2}, "ask"]);
    let record = run_record(&run);
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["outcome"], serde_json::Value::Null, "{record}");
    assert_eq!(record["position"], path, "{record}");
    assert_eq!(record["edges"], serde_json::json!({}), "{record}");

    let (wait, nested) = carried_waits(&run, 2);
    assert_eq!(terminal["wait"], wait, "{terminal}");
    assert_eq!(wait["kind"], "answer", "{wait}");
    assert_eq!(wait["step"], path, "{wait}");
    assert_eq!(wait["then"], "outer", "{wait}");
    assert_eq!(wait["rule"], answer_rule(), "{wait}");
    assert_eq!(
        wait["payload"]["questions"], "1. which way for b?",
        "{wait}"
    );
    assert_eq!(wait["payload"]["summary"], "b needs a decision", "{wait}");
    assert_eq!(
        wait["payload"]["child"], "children/outer/000002/000001/children/ask/000001",
        "{wait}"
    );
    assert!(
        run.join(wait["payload"]["child"].as_str().unwrap())
            .is_dir()
    );

    let nested_record = run_record(&nested_run(&run, 2));
    assert_eq!(nested_record["state"], "waiting", "{nested_record}");
    assert_eq!(nested_record["position"], serde_json::json!(["ask"]));
    assert_eq!(nested["step"], serde_json::json!(["ask"]), "{nested}");
    assert_eq!(nested["then"], "ask", "{nested}");
    assert_eq!(
        nested["payload"]["child"], "children/ask/000001",
        "{nested}"
    );
    assert_eq!(nested["payload"]["questions"], wait["payload"]["questions"]);
    assert_eq!(run_record(&nested_run(&run, 1))["state"], "exited");

    let outer = super::workflow_run::result_step(&terminal, "outer");
    assert_eq!(outer["state"], "not_run", "{terminal}");
    assert_eq!(outer["detail"]["code"], "not_reached", "{terminal}");
    let finish = super::workflow_run::result_step(&terminal, "finish");
    assert_eq!(finish["state"], "not_run", "{terminal}");
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "ask a\nwork a\nask b\n"
    );

    let status = super::workflow_continue::status_document(&run);
    assert_eq!(status["wait"], wait, "{status}");
    assert_eq!(status["record"]["state"], "waiting", "{status}");
}

/// A nested workflow for the asking tree whose `check` hands back to `ask` once under a
/// limit before it exits: `ask` waits for answers when it is blocked.
const LOOPING_ASKING_PART: &str = "schemaVersion: 1
environmentPassthrough: [NESTED_VISIBLE]
inputs:
  part:
    kind: text
outcomes: [checked, failed]
steps:
  ask:
    kind: workflow
    workflow: ask.yaml
    inputs:
      part:
        ref: inputs.part
      answers:
        ref: controls.answers
    on:
      asked: check
      blocked:
        wait: answer
        then: ask
      failed: exit.failed
  check:
    kind: workflow
    workflow: check.yaml
    inputs:
      part:
        ref: inputs.part
    on:
      again:
        then: ask
        limit: 1
        else: exit.checked
      done: exit.checked
      failed: exit.failed
";

/// The `show --json` entries of the stages `(step, ordinal, state, outcome)` of the nested
/// run at `nested`: a stage keeps a plain child's shape.
fn shown_stages(nested: &Path, stages: &[(&str, u64, &str, &str)]) -> serde_json::Value {
    stages
        .iter()
        .map(|(step, ordinal, state, outcome)| {
            let directory = nested.join(format!("children/{step}/{ordinal:06}"));
            serde_json::json!({
                "step": step,
                "item": null,
                "ordinal": ordinal,
                "id": run_record(&directory)["id"],
                "state": state,
                "outcome": outcome,
                "directory": directory.to_str().unwrap(),
                "progress": null,
            })
        })
        .collect()
}

// REQ-LOCALRUN-189: `show` lists each nested run's `edges` counts and its stages under it,
// and `show` and `status` print the position and the carried wait's step at full depth.
#[test]
fn show_lists_a_nested_runs_stages_and_edges_and_prints_full_paths() {
    let bundle = asking_tree();
    bundle.write_source("part.yaml", LOOPING_ASKING_PART);
    fs::write(bundle.execution_root().join("verdict-ask-b"), "blocked").unwrap();
    assert_eq!(
        completed(&create_nested(&bundle, "shown"))["outcome"],
        "waiting"
    );
    let run = fs::canonicalize(bundle.result("shown")).unwrap();
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("stages.log")).unwrap(),
        "ask a\ncheck a\nask a\ncheck a\nask b\n"
    );

    let output = super::workflow_run_control::run_control(None, "show", &run, &[]);
    let document = last_document(&output);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let path = serde_json::json!(["outer", {"item": 2}, "ask"]);
    assert_eq!(document["position"], "outer[2].ask", "{document}");
    assert_eq!(document["wait"]["step"], path, "{document}");
    let (first, second) = (nested_run(&run, 1), nested_run(&run, 2));
    assert_eq!(
        document["children"],
        serde_json::json!([
            {
                "step": "outer",
                "item": 1,
                "ordinal": 1,
                "id": run_record(&first)["id"],
                "state": "exited",
                "outcome": "checked",
                "directory": first.to_str().unwrap(),
                "progress": null,
                "edges": {"check.again": 1},
                "children": shown_stages(&first, &[
                    ("ask", 1, "exited", "asked"),
                    ("ask", 2, "exited", "asked"),
                    ("check", 1, "exited", "again"),
                    ("check", 2, "exited", "done"),
                ]),
            },
            {
                "step": "outer",
                "item": 2,
                "ordinal": 1,
                "id": run_record(&second)["id"],
                "state": "waiting",
                "outcome": null,
                "directory": second.to_str().unwrap(),
                "progress": null,
                "edges": {},
                "children": shown_stages(&second, &[("ask", 1, "exited", "blocked")]),
            },
        ]),
        "{document}"
    );

    let plain = super::workflow_run_list::shown_plain(&run);
    assert!(plain.contains("\nposition: outer[2].ask\n"), "{plain}");
    assert!(
        plain.contains("\nwait: answer at step outer[2].ask since "),
        "{plain}"
    );
    let children = plain
        .split("\nchildren:\n")
        .nth(1)
        .and_then(|rest| rest.split("\n\n").next())
        .unwrap();
    let rows = children
        .lines()
        .map(|line| {
            let indent = line.len() - line.trim_start().len();
            let mut fields = line.split_whitespace();
            (indent, fields.next().unwrap(), fields.next().unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            (2, "outer", "1"),
            (4, "edges:", "check.again=1"),
            (6, "ask", "-"),
            (6, "ask", "-"),
            (6, "check", "-"),
            (6, "check", "-"),
            (2, "outer", "2"),
            (4, "edges:", "-"),
            (6, "ask", "-"),
        ],
        "{plain}"
    );

    let status = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "status".to_owned(),
        run.to_string_lossy().into_owned(),
    ])
    .output()
    .unwrap();
    assert_eq!(status.status.code(), Some(0), "{status:?}");
    let status = String::from_utf8(status.stdout).unwrap();
    assert!(
        status.contains("\nwait: answer at step outer[2].ask since "),
        "{status}"
    );
}

// REQ-WFEXEC-75, REQ-WFEXEC-70, REQ-WFEXEC-68: `interrupt` while a nested stage runs fails
// the stage `interrupted`, the nested run enters the `repair` wait its failed edge names,
// and the root carries it with the interruption in its payload.
#[test]
fn an_interrupted_nested_stage_leaves_the_root_in_the_carried_repair_wait() {
    let bundle = asking_tree();
    let execution = bundle.execution_root().to_owned();
    fs::write(execution.join("hold-work-a"), "").unwrap();
    let destination = bundle.result("interrupted");
    let mut live = nested_command(&bundle, "interrupted")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    super::poll_until(
        "the nested stage's held step",
        || {
            execution.join("held-work-a").exists()
                && um_execution::lock_holder(&destination).is_ok_and(|holder| holder.is_some())
        },
        |held| *held,
    );
    let run = fs::canonicalize(&destination).unwrap();
    assert_eq!(
        run_record(&run)["position"],
        serde_json::json!(["outer", {"item": 1}, "work"])
    );

    let output =
        super::workflow_run_control::run_control(None, "interrupt", &run, &["--reason", "pausing"]);
    let document =
        super::workflow_run_control::recorded_as(&output, "interrupt", "interrupted", &run);
    let status = live.wait().unwrap();
    assert_eq!(status.code(), Some(143), "{status:?}");

    let path = serde_json::json!(["outer", {"item": 1}, "work"]);
    let record = run_record(&run);
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["position"], path, "{record}");
    let (wait, nested) = carried_waits(&run, 1);
    assert_eq!(document["wait"], wait, "{document}");
    assert_eq!(wait["kind"], "repair", "{wait}");
    assert_eq!(wait["step"], path, "{wait}");
    assert_eq!(wait["then"], "outer", "{wait}");
    assert_eq!(
        wait["payload"]["interrupted"]["reason"], "termination_request",
        "{wait}"
    );
    assert_eq!(
        wait["payload"]["interrupted"]["control"]["payload"],
        serde_json::json!({"reason": "pausing"}),
        "{wait}"
    );
    assert_eq!(
        wait["payload"]["child"], "children/outer/000001/000001/children/work/000001",
        "{wait}"
    );
    assert_eq!(nested["step"], serde_json::json!(["work"]), "{nested}");
    assert_eq!(nested["payload"], {
        let mut payload = wait["payload"].clone();
        payload["child"] = serde_json::json!("children/work/000001");
        payload
    });
    assert_eq!(run_record(&nested_run(&run, 1))["state"], "waiting");
    assert!(!run.join("children/outer/000002").exists());
}

/// The last JSON document `output` wrote to stdout.
fn last_document(output: &std::process::Output) -> serde_json::Value {
    serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<serde_json::Value>()
        .last()
        .and_then(Result::ok)
        .unwrap_or_else(|| {
            panic!(
                "stdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })
}

/// The asking tree's run whose element 2 waits for answers at `[outer, {item: 2}, ask]`,
/// with its one question answered.
fn answered_nested_run(bundle: &TreeBundle, name: &str) -> std::path::PathBuf {
    let execution = bundle.execution_root();
    fs::write(execution.join("verdict-ask-b"), "blocked").unwrap();
    assert_eq!(
        completed(&create_nested(bundle, name))["outcome"],
        "waiting"
    );
    let run = fs::canonicalize(bundle.result(name)).unwrap();
    let output = super::workflow_run_control::run_control(
        None,
        "answer",
        &run,
        &["--question", "1", "--text", "left"],
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    run
}

// REQ-WFEXEC-68, REQ-WFEXEC-75, REQ-CONTINUE-92: a nested workflow under a root step that
// fans nothing out waits at `[<step>, <nested>]`, its nested run the step's own child, and
// the answered wait continues that nested run in place with the answers.
#[test]
fn a_plain_steps_nested_run_waits_at_its_two_element_path_and_continues_in_place() {
    let bundle = asking_tree();
    bundle.write_source(
        "workflow.yaml",
        &NESTED_TREE_ROOT
            .replacen("  parts:\n    kind: json\n", "  part:\n    kind: text\n", 1)
            .replacen("    forEach: inputs.parts\n", "", 1)
            .replacen("        ref: item.name\n", "        ref: inputs.part\n", 1),
    );
    let execution = bundle.execution_root().to_owned();
    fs::write(execution.join("verdict-ask-b"), "blocked").unwrap();
    let mut args = bundle.args(&bundle.result("plain"));
    args.splice(
        args.len() - 1..args.len() - 1,
        ["--input-text", "part", "b"].map(str::to_owned),
    );
    let terminal = completed(&isolated_command(&args).output().unwrap());
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    let run = fs::canonicalize(bundle.result("plain")).unwrap();
    let nested = run.join("children/outer/000001");

    let path = serde_json::json!(["outer", "ask"]);
    assert_eq!(run_record(&run)["position"], path);
    let wait = read_json(&run.join("wait.json"));
    assert_eq!(wait["kind"], "answer", "{wait}");
    assert_eq!(wait["step"], path, "{wait}");
    assert_eq!(wait["then"], "outer", "{wait}");
    assert_eq!(
        wait["payload"]["child"], "children/outer/000001/children/ask/000001",
        "{wait}"
    );
    assert_eq!(read_json(&nested.join("wait.json"))["since"], wait["since"]);

    let output = super::workflow_run_control::run_control(
        None,
        "answer",
        &run,
        &["--question", "1", "--text", "left"],
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    fs::remove_file(execution.join("verdict-ask-b")).unwrap();
    let terminal = completed(&super::workflow_continue::resume(&run));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(ordinals_of(&run.join("children/outer")), ["000001"]);
    assert_eq!(attempt_count(&nested), 2);
    assert!(!nested.join("wait.json").exists());
    assert_eq!(
        read_json(&execution.join("answers-ask-b")),
        serde_json::json!({"1": "left"})
    );
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "ask b\nask b\nwork b\nfinish -\n"
    );
}

// SC-WFEXEC-6, REQ-CONTINUE-92, REQ-WFEXEC-75, REQ-WFEXEC-68, REQ-CONTINUE-81: the answered
// carried wait continues the same nested run in a new attempt from `ask`, which receives the
// answers, with element 1 and nothing before `ask` run again; `work` then fails into a
// carried repair wait, and the retry continues that failed stage in place, its succeeded
// `prep` inherited, until the root exits with its outcome.
#[test]
fn a_carried_wait_continues_its_nested_run_in_place() {
    let bundle = asking_tree();
    let execution = bundle.execution_root().to_owned();
    let run = answered_nested_run(&bundle, "walkthrough");
    let nested = nested_run(&run, 2);
    assert_eq!(attempt_count(&nested), 1);

    fs::remove_file(execution.join("verdict-ask-b")).unwrap();
    fs::write(execution.join("fail-work-b"), "").unwrap();
    let terminal = completed(&super::workflow_continue::resume(&run));
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");

    // The same nested run took a second attempt from `ask`, which read the answers.
    assert_eq!(ordinals_of(&run.join("children/outer/000002")), ["000001"]);
    assert_eq!(attempt_count(&nested), 2);
    let resumed = attempt_result(&nested, 2);
    assert_eq!(resumed["continuation"]["reason"], "resume", "{resumed}");
    assert_eq!(
        resumed["continuation"]["reexecutedSteps"],
        serde_json::json!(["ask", "work"]),
        "{resumed}"
    );
    assert_eq!(
        read_json(&execution.join("answers-ask-b")),
        serde_json::json!({"1": "left"})
    );
    assert!(!execution.join("answers-ask-a").exists());
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "ask a\nwork a\nask b\nask b\nwork b\n"
    );
    assert_eq!(
        ordinals_of(&nested.join("children/ask")),
        ["000001", "000002"]
    );

    // The answer wait is gone from both runs; the root now carries `work`'s repair wait.
    let path = serde_json::json!(["outer", {"item": 2}, "work"]);
    assert_eq!(run_record(&run)["position"], path);
    let (wait, nested_wait) = carried_waits(&run, 2);
    assert_eq!(wait["kind"], "repair", "{wait}");
    assert_eq!(wait["step"], path, "{wait}");
    assert_eq!(wait["then"], "outer", "{wait}");
    assert_eq!(
        wait["payload"]["child"], "children/outer/000002/000001/children/work/000001",
        "{wait}"
    );
    assert_eq!(nested_wait["step"], serde_json::json!(["work"]));

    // The retry continues the failed stage in place; `prep` is not run again.
    fs::remove_file(execution.join("fail-work-b")).unwrap();
    let output = super::workflow_run_control::run_control(None, "retry", &run, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let terminal = last_document(&output);
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "landed", "{record}");
    assert!(!run.join("wait.json").exists());
    assert!(!nested.join("wait.json").exists());
    assert_eq!(ordinals_of(&run.join("children/outer/000002")), ["000001"]);
    assert_eq!(ordinals_of(&run.join("children/outer/000001")), ["000001"]);
    assert_eq!(attempt_count(&nested), 3);
    let nested_record = run_record(&nested);
    assert_eq!(nested_record["state"], "exited", "{nested_record}");
    assert_eq!(nested_record["outcome"], "checked", "{nested_record}");
    let retried = attempt_result(&nested, 3);
    assert_eq!(retried["continuation"]["reason"], "retry", "{retried}");
    assert_eq!(
        retried["continuation"]["reexecutedSteps"],
        serde_json::json!(["work"]),
        "{retried}"
    );
    let stage = nested.join("children/work");
    assert_eq!(ordinals_of(&stage), ["000001"]);
    let stage = stage.join("000001");
    assert_eq!(attempt_count(&stage), 2);
    let stage_result = attempt_result(&stage, 2);
    assert_eq!(stage_result["continuation"]["reason"], "retry");
    assert_eq!(
        stage_result["continuation"]["reexecutedSteps"],
        serde_json::json!(["decide"]),
        "{stage_result}"
    );
    assert_eq!(
        fs::read_to_string(execution.join("preps.log")).unwrap(),
        "prep a\nprep b\n"
    );
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "ask a\nwork a\nask b\nask b\nwork b\nwork b\nfinish -\n"
    );
}

// REQ-CONTINUE-92, REQ-CONTINUE-82: a carried wait whose `payload.child` lies under another
// run than the waiting element's nested run is refused before the root's wait is consumed:
// no attempt begins, no stage runs and both runs keep waiting.
#[test]
fn a_carried_wait_naming_another_run_is_refused() {
    let bundle = asking_tree();
    let execution = bundle.execution_root().to_owned();
    let run = answered_nested_run(&bundle, "misnamed");
    let path = run.join("wait.json");
    let mut wait = read_json(&path);
    wait["payload"]["child"] = "children/outer/000001/000001/children/ask/000001".into();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o600);
    fs::set_permissions(&path, permissions).unwrap();
    fs::write(&path, serde_json::to_vec_pretty(&wait).unwrap()).unwrap();
    fs::remove_file(execution.join("verdict-ask-b")).unwrap();

    let output = super::workflow_continue::resume(&run);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let document = last_document(&output);
    assert_eq!(document["outcome"], "rejected", "{document}");
    assert_eq!(
        document["diagnostics"][0]["code"], "continuation_definition_invalid",
        "{document}"
    );
    assert_eq!(run_record(&run)["state"], "waiting");
    assert_eq!(read_json(&path), wait);
    assert_eq!(attempt_count(&run), 1);
    let nested = nested_run(&run, 2);
    assert_eq!(run_record(&nested)["state"], "waiting");
    assert!(nested.join("wait.json").exists());
    assert_eq!(attempt_count(&nested), 1);
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "ask a\nwork a\nask b\n"
    );
}

// REQ-CONTINUE-92, REQ-CONTINUE-75: withdrawing a root that carries a nested run's wait ends
// both runs `withdrawn` and removes both wait records, with no attempt begun.
#[test]
fn withdrawing_a_carried_wait_ends_the_nested_run_with_the_root() {
    let bundle = asking_tree();
    let run = answered_nested_run(&bundle, "withdrawn");
    assert_withdrawn_with_its_nested_run(&run, 2);
}

/// The rule of a `stop` wait the root carries from a nested run (REQ-WFEXEC-76): the lift
/// names the nested step through the stop's own `payload.before`, since `then` is the root's.
fn carried_stop_rule() -> serde_json::Value {
    serde_json::json!({
        "source": "um",
        "event": ["control.lift"],
        "where": "event.run == run.id and event.payload.before == wait.payload.payload.before and event.payload.stop == wait.payload.at",
    })
}

/// The nested tree's run `name`, created with a stop before the nested step `check`, parked
/// before element 1's `check`: its directory and terminal document.
fn stopped_before_check(
    bundle: &TreeBundle,
    name: &str,
) -> (std::path::PathBuf, serde_json::Value) {
    let args = nested_args(bundle, name, &["--stop-before", "check"]);
    let terminal = completed(&isolated_command(&args).output().unwrap());
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    (fs::canonicalize(bundle.result(name)).unwrap(), terminal)
}

// REQ-WFEXEC-76, REQ-LOCALRUN-185, REQ-WFEXEC-75, REQ-CONTINUE-92: `create --stop-before`
// naming a nested step parks the nested run before element 1's `check` and the root carries
// the `stop` wait at the full path; `stop` accepts the nested step's id and refuses an unknown
// one, and once `lift` resolves the wait, `resume` continues the same nested run at `check`,
// which starts again later in the run, and in element 2, without holding.
#[test]
fn a_stop_holds_a_nested_step_until_it_is_lifted() {
    let bundle = nested_tree();
    let execution = bundle.execution_root().to_owned();
    let (run, terminal) = stopped_before_check(&bundle, "stopped");

    let path = serde_json::json!(["outer", {"item": 1}, "check"]);
    let record = run_record(&run);
    assert_eq!(record["state"], "waiting", "{record}");
    // REQ-WFEXEC-68: the root's position is the carried wait's path, not the last stage's.
    assert_eq!(record["position"], path, "{record}");
    let (wait, nested) = carried_waits(&run, 1);
    assert_eq!(terminal["wait"], wait, "{terminal}");
    assert_eq!(wait["kind"], "stop", "{wait}");
    assert_eq!(wait["step"], path, "{wait}");
    assert_eq!(wait["then"], "outer", "{wait}");
    assert_eq!(wait["rule"], carried_stop_rule(), "{wait}");
    let stop = read_json(&run.join("controls/0001-stop.json"));
    assert_eq!(
        wait["payload"],
        serde_json::json!({"by": stop["by"], "at": stop["at"], "payload": {"before": "check"}}),
        "{wait}"
    );
    assert_eq!(nested["step"], serde_json::json!(["check"]), "{nested}");
    assert_eq!(nested["then"], "check", "{nested}");
    assert_eq!(nested["payload"], wait["payload"], "{nested}");
    assert_eq!(run_record(&nested_run(&run, 1))["state"], "waiting");
    assert!(!run.join("children/outer/000002").exists());
    assert!(!nested_run(&run, 1).join("children/check").exists());
    let outer = super::workflow_run::result_step(&terminal, "outer");
    assert_eq!(outer["state"], "not_run", "{terminal}");
    assert_eq!(outer["detail"]["code"], "not_reached", "{terminal}");
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "work a\n"
    );

    // A nested `workflow` step's id is one a stop may name; any other id is unknown.
    let control =
        |args: &[&str]| super::workflow_run_control::run_control(None, "stop", &run, args);
    super::workflow_run_control::refused(
        &control(&["--before", "check", "--reason", "again"]),
        "stop",
        "stop_in_force",
    );
    super::workflow_run_control::refused(
        &control(&["--before", "nothere", "--reason", "again"]),
        "stop",
        "step_unknown",
    );

    let output = super::workflow_run_control::run_control(None, "lift", &run, &[]);
    let lifted = super::workflow_run_control::recorded_as(&output, "lift", "recorded", &run);
    assert_eq!(
        lifted["control"]["wait"],
        format!("stop@{}", wait["since"].as_str().unwrap()),
        "{lifted}"
    );
    let lift = read_json(&run.join(lifted["control"]["path"].as_str().unwrap()));
    assert_eq!(
        lift["payload"],
        serde_json::json!({"before": "check", "stop": stop["at"]})
    );

    let terminal = completed(&super::workflow_continue::resume(&run));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "landed", "{record}");
    let nested = nested_run(&run, 1);
    for directory in [&run, &nested] {
        assert!(!directory.join("wait.json").exists());
    }
    assert_eq!(ordinals_of(&run.join("children/outer/000001")), ["000001"]);
    assert_eq!(run_record(&nested)["outcome"], "checked");
    assert_eq!(attempt_count(&nested), 3);
    let resumed = attempt_result(&nested, 2);
    assert_eq!(resumed["continuation"]["reason"], "resume", "{resumed}");
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "work a\ncheck a\nwork a\ncheck a\nwork b\ncheck b\nwork b\ncheck b\nfinish -\n"
    );

    // REQ-LOCALRUN-80: `--stop-before` naming no step of the tree's workflows is a usage error.
    let args = nested_args(&bundle, "stop-nowhere", &["--stop-before", "nothere"]);
    super::workflow_run::assert_usage_error(&args);
    assert!(!bundle.result("stop-nowhere").exists());
}

/// Withdrawing the root `run` carries a wait from element `item`'s nested run ends both runs
/// `withdrawn` and removes both wait records, with no attempt begun.
fn assert_withdrawn_with_its_nested_run(run: &Path, item: u64) {
    let output = super::workflow_run_control::run_control(None, "withdraw", run, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let nested = nested_run(run, item);
    let attempts = attempt_count(&nested);
    for directory in [run, &nested] {
        let record = run_record(directory);
        assert_eq!(record["state"], "exited", "{record}");
        assert_eq!(record["outcome"], "withdrawn", "{record}");
        assert!(!directory.join("wait.json").exists());
    }
    assert_eq!(attempt_count(run), 1);
    assert_eq!(attempt_count(&nested), attempts);
}

// REQ-WFEXEC-76, REQ-CONTINUE-75: withdrawing a root that carries a nested run's `stop` wait,
// whose payload names no child, ends the nested run at that element with the root.
#[test]
fn withdrawing_a_carried_stop_ends_the_nested_run_with_the_root() {
    let bundle = nested_tree();
    let (run, _) = stopped_before_check(&bundle, "stop-withdrawn");
    assert_withdrawn_with_its_nested_run(&run, 1);
}

/// The asking tree's root, published with a subject and a mirror that moves the issue to the
/// bound `Backlog` when the run is withdrawn.
fn mirrored_asking_root() -> String {
    let header = r#"schemaVersion: 1
name: nested
subject: ticket
bindings:
  backlog: {kind: linear.state, default: "Backlog"}
mirror:
  linear:
    issue: inputs.ticket
    onOutcome:
      withdrawn: {state: $backlog}
"#;
    NESTED_TREE_ROOT
        .replacen("schemaVersion: 1\n", header, 1)
        .replacen("inputs:\n", "inputs:\n  ticket:\n    kind: text\n", 1)
}

// REQ-WFEXEC-76, REQ-WFEXEC-73, REQ-LOCALRUN-186, REQ-LOCALRUN-191: a `withdraw` recorded
// while element 1's `ask` stage runs is honored once that stage has finished, before the
// nested step `work` starts: the nested run's attempt ends as a cancellation and the run
// `withdrawn`, the root's step ends `cancelled` with reason `user_request`, the root ends
// `withdrawn`, and the mirrored tree writes `onOutcome.withdrawn` once.
#[test]
fn a_withdrawal_is_honored_at_the_next_nested_step_boundary() {
    let bundle = asking_tree();
    bundle.write_source("workflow.yaml", &mirrored_asking_root());
    let home = super::workflow_definition::Home::new();
    let report = home.json(&["publish", bundle.source_root().to_str().unwrap()]);
    assert_eq!(report["outcome"], "published", "{report}");
    let definition = report["definition"]["id"].as_str().unwrap().to_owned();
    let stub = super::workflow_mirror::LinearStub::start();
    let execution = bundle.execution_root().to_owned();
    fs::write(execution.join("hold-ask-a"), "").unwrap();

    let command = |args: &[&str]| {
        let mut full = vec!["workflow".to_owned(), "run".to_owned()];
        full.extend(args.iter().map(|arg| (*arg).to_owned()));
        let mut command = isolated_command(&full);
        command.env("UM_HOME", home.path());
        stub.point(&mut command);
        command
    };
    let execution_root = execution.to_str().unwrap().to_owned();
    let process = command(&[
        "create",
        &definition,
        "--execution-root",
        &execution_root,
        "--input-text",
        "ticket",
        "AUTO-7",
        "--input-json",
        "parts",
        r#"[{"name":"a"},{"name":"b"}]"#,
        "--json",
    ])
    .stdin(std::process::Stdio::null())
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::null())
    .spawn()
    .unwrap();
    /// Lets the held stage go if the test ends early, so its process never sleeps on.
    struct Release(std::path::PathBuf);
    impl Drop for Release {
        fn drop(&mut self) {
            let _ = fs::write(&self.0, "");
        }
    }
    let release = Release(execution.join("release-ask-a"));
    super::poll_until(
        "the nested stage's held step",
        || execution.join("held-ask-a").exists(),
        |held| *held,
    );
    let withdrawn = super::poll_until(
        "the live run's withdraw",
        || command(&["withdraw", "AUTO-7", "--json"]).output().unwrap(),
        |output| output.status.code() == Some(0),
    );
    let document = last_document(&withdrawn);
    assert_eq!(document["outcome"], "recorded", "{document}");
    let run = std::path::PathBuf::from(document["runDirectory"].as_str().unwrap());

    drop(release);
    let output = process.wait_with_output().unwrap();
    let terminal = last_document(&output);
    assert_eq!(terminal["result"]["outcome"], "cancelled", "{terminal}");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "withdrawn", "{record}");
    assert!(!run.join("wait.json").exists());
    for id in ["outer", "finish"] {
        let step = super::workflow_run::result_step(&terminal, id);
        assert_eq!(step["state"], "cancelled", "{terminal}");
        assert_eq!(step["detail"]["code"], "user_request", "{terminal}");
    }

    // The stage finished; the nested run ended before `work` started and no element followed.
    let nested = nested_run(&run, 1);
    let nested_record = run_record(&nested);
    assert_eq!(nested_record["state"], "exited", "{nested_record}");
    assert_eq!(nested_record["outcome"], "withdrawn", "{nested_record}");
    assert_eq!(attempt_count(&nested), 1);
    let nested_result = attempt_result(&nested, 1);
    assert_eq!(nested_result["outcome"], "cancelled", "{nested_result}");
    assert_eq!(
        read_json(&nested.join("children/ask/000001/run.json"))["outcome"],
        "asked"
    );
    assert!(!nested.join("children/work").exists());
    assert!(!run.join("children/outer/000002").exists());
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "ask a\n"
    );

    let outcomes = super::workflow_mirror::mirror_writes(&run)
        .into_iter()
        .filter(|(name, _)| name.ends_with("-outcome.json"))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(outcomes[0].1["outcome"], "written", "{outcomes:?}");
    let updates = stub
        .bodies()
        .into_iter()
        .filter(|body| body["operationName"] == "UpdateIssueState")
        .count();
    assert_eq!(updates, 1);
}

/// The asking tree's run `name`, its root process killed while the nested stage `NAME-PART`
/// that `held` names holds, as the abandonment tests leave a run: its record `running` with
/// no lock holder. The hold is removed and the orphaned stage released, so a continuation
/// runs the stage through.
fn abandoned_while_held(bundle: &TreeBundle, name: &str, held: &str) -> std::path::PathBuf {
    let execution = bundle.execution_root().to_owned();
    fs::write(execution.join(format!("hold-{held}")), "").unwrap();
    let destination = bundle.result(name);
    let mut live = nested_command(bundle, name)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    super::poll_until(
        "the nested stage's held step",
        || {
            execution.join(format!("held-{held}")).exists()
                && um_execution::lock_holder(&destination).is_ok_and(|holder| holder.is_some())
        },
        |held| *held,
    );
    live.kill().unwrap();
    assert!(!live.wait().unwrap().success());
    fs::remove_file(execution.join(format!("hold-{held}"))).unwrap();
    fs::write(execution.join(format!("release-{held}")), "").unwrap();
    let run = fs::canonicalize(&destination).unwrap();
    assert_eq!(run_record(&run)["state"], "running");
    run
}

/// `um workflow run resume RUN --json` of the abandoned `run`, once the killed process's lock
/// is released and its orphaned step gone.
fn resumed_once_abandoned(run: &Path) -> std::process::Output {
    super::poll_until(
        "the abandoned run's resume",
        || super::workflow_continue::resume(run),
        |output| output.status.success(),
    )
}

// REQ-CONTINUE-91, REQ-CONTINUE-76, REQ-WFEXEC-68: resuming a root killed while element 1's
// nested stage `work` ran continues the same nested run in a new attempt from `work`, whose
// stage takes a new attempt in place; `ask` and element 1's nested run are not started again,
// and the root goes on to element 2 and exits with its outcome.
#[test]
fn resuming_an_abandoned_root_continues_its_running_nested_run_in_place() {
    let bundle = asking_tree();
    let execution = bundle.execution_root().to_owned();
    let run = abandoned_while_held(&bundle, "abandoned-stage", "work-a");
    assert_eq!(
        run_record(&run)["position"],
        serde_json::json!(["outer", {"item": 1}, "work"])
    );
    let nested = nested_run(&run, 1);
    assert_eq!(run_record(&nested)["state"], "running");

    let output = resumed_once_abandoned(&run);
    let continuation = &super::workflow_continue::partition_event(&output)["continuation"];
    assert_eq!(continuation["reason"], "resume", "{continuation}");
    assert_eq!(continuation["fromSteps"], serde_json::json!(["outer"]));
    let terminal = completed(&output);
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "landed", "{record}");
    assert_eq!(record["attempts"], 2, "{record}");

    // The same nested run took a second attempt from `work`, inheriting `ask`.
    assert_eq!(ordinals_of(&run.join("children/outer/000001")), ["000001"]);
    assert_eq!(attempt_count(&nested), 2);
    let nested_record = run_record(&nested);
    assert_eq!(nested_record["state"], "exited", "{nested_record}");
    assert_eq!(nested_record["outcome"], "checked", "{nested_record}");
    let resumed = attempt_result(&nested, 2);
    assert_eq!(resumed["continuation"]["reason"], "resume", "{resumed}");
    assert_eq!(
        resumed["continuation"]["reexecutedSteps"],
        serde_json::json!(["work"]),
        "{resumed}"
    );
    // Its stage `work` took a new attempt in its own directory.
    let stage = nested.join("children/work");
    assert_eq!(ordinals_of(&stage), ["000001"]);
    assert_eq!(attempt_count(&stage.join("000001")), 2);
    assert_eq!(ordinals_of(&nested.join("children/ask")), ["000001"]);
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "ask a\nwork a\nwork a\nask b\nwork b\nfinish -\n"
    );
}

// REQ-CONTINUE-91, REQ-WFEXEC-75, REQ-CONTINUE-92: a root abandoned once element 2's nested
// run has recorded its `answer` wait and before the root recorded its own enters the carried
// wait on `resume` and starts nothing; answered, it continues that nested run in place.
#[test]
fn resuming_an_abandoned_root_enters_the_wait_its_nested_run_recorded() {
    let bundle = asking_tree();
    let execution = bundle.execution_root().to_owned();
    fs::write(execution.join("verdict-ask-b"), "blocked").unwrap();
    let run = abandoned_while_held(&bundle, "abandoned-wait", "ask-b");
    let nested = nested_run(&run, 2);
    // The nested run, resumed on its own, records its wait as it would have before the root;
    // the root still reads `running` with `outer` running and no wait record.
    let waited = completed(&resumed_once_abandoned(&nested));
    assert_eq!(waited["outcome"], "waiting", "{waited}");
    assert_eq!(run_record(&nested)["state"], "waiting");
    assert_eq!(attempt_count(&nested), 2);
    assert_eq!(run_record(&run)["state"], "running");
    assert!(!run.join("wait.json").exists());
    let log = "ask a\nwork a\nask b\nask b\n";
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        log
    );

    let terminal = completed(&super::workflow_continue::resume(&run));
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    let path = serde_json::json!(["outer", {"item": 2}, "ask"]);
    let record = run_record(&run);
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["position"], path, "{record}");
    let (wait, nested_wait) = carried_waits(&run, 2);
    assert_eq!(terminal["wait"], wait, "{terminal}");
    assert_eq!(wait["kind"], "answer", "{wait}");
    assert_eq!(wait["step"], path, "{wait}");
    assert_eq!(wait["then"], "outer", "{wait}");
    assert_eq!(wait["rule"], answer_rule(), "{wait}");
    assert_eq!(
        wait["payload"]["child"], "children/outer/000002/000001/children/ask/000001",
        "{wait}"
    );
    assert_eq!(nested_wait["step"], serde_json::json!(["ask"]));
    // Nothing started: no stage, no nested attempt and no other element.
    assert_eq!(attempt_count(&run), 2);
    assert_eq!(attempt_count(&nested), 2);
    assert_eq!(ordinals_of(&run.join("children/outer/000002")), ["000001"]);
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        log
    );

    let output = super::workflow_run_control::run_control(
        None,
        "answer",
        &run,
        &["--question", "1", "--text", "left"],
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    fs::remove_file(execution.join("verdict-ask-b")).unwrap();
    let terminal = completed(&super::workflow_continue::resume(&run));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(attempt_count(&nested), 3);
    assert_eq!(
        read_json(&execution.join("answers-ask-b")),
        serde_json::json!({"1": "left"})
    );
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        format!("{log}ask b\nwork b\nfinish -\n")
    );
}

/// Publishes the asking tree's current source as the next revision of its definition.
fn publish_asking(
    bundle: &TreeBundle,
    home: &super::workflow_definition::Home,
) -> serde_json::Value {
    let report = home.json(&["publish", bundle.source_root().to_str().unwrap()]);
    assert_eq!(report["outcome"], "published", "{report}");
    report
}

/// A run of the published asking tree, by definition, whose element 2 `ask` ended `blocked`:
/// the root waits in the `answer` wait it carries at `[outer, {item: 2}, ask]`.
fn published_run_waiting_in_a_carried_answer() -> (
    TreeBundle,
    super::workflow_definition::Home,
    std::path::PathBuf,
) {
    let bundle = asking_tree();
    let named =
        NESTED_TREE_ROOT.replacen("schemaVersion: 1\n", "schemaVersion: 1\nname: asking\n", 1);
    bundle.write_source("workflow.yaml", &named);
    let home = super::workflow_definition::Home::new();
    let definition = publish_asking(&bundle, &home)["definition"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let execution = bundle.execution_root().to_owned();
    fs::write(execution.join("verdict-ask-b"), "blocked").unwrap();
    let mut create = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "create".to_owned(),
        definition,
        "--execution-root".to_owned(),
        execution.to_str().unwrap().to_owned(),
        "--input-json".to_owned(),
        "parts".to_owned(),
        r#"[{"name":"a"},{"name":"b"}]"#.to_owned(),
        "--json".to_owned(),
    ]);
    create.env("UM_HOME", home.path());
    let terminal = completed(&create.output().unwrap());
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    let predecessor = std::path::PathBuf::from(terminal["runDirectory"].as_str().unwrap());
    (bundle, home, predecessor)
}

// REQ-CONTINUE-91, REQ-CONTINUE-80, REQ-CONTINUE-92, REQ-WFEXEC-75: `upgrade` of a root
// waiting in a carried `answer` wait is refused while the current revision's nested workflow
// no longer defines the waiting nested step; once it does, the successor waits with the same
// `step` path, and answered, it starts a fresh nested run at that element whose `ask` receives
// the answers, with element 1 not run again.
#[test]
fn an_upgrade_carries_a_nested_wait_and_starts_its_nested_run_afresh() {
    let (bundle, home, predecessor) = published_run_waiting_in_a_carried_answer();
    let publish = || publish_asking(&bundle, &home);
    let execution = bundle.execution_root().to_owned();
    let predecessor_id = run_record(&predecessor)["id"].as_str().unwrap().to_owned();
    let path = serde_json::json!(["outer", {"item": 2}, "ask"]);
    let wait = fs::read(predecessor.join("wait.json")).unwrap();
    let upgrade =
        || super::workflow_run_control::run_control(Some(&home), "upgrade", &predecessor_id, &[]);

    // Revision 2's nested workflow names the waiting step `query`.
    bundle.write_source(
        "part.yaml",
        &ASKING_TREE_PART
            .replacen("  ask:\n", "  query:\n", 1)
            .replacen("then: ask\n", "then: query\n", 1),
    );
    assert_eq!(publish()["revision"]["n"], 2);
    let refusal = super::workflow_run::tree_rejection(&upgrade());
    assert_eq!(refusal["command"], "um workflow run upgrade", "{refusal}");
    assert_eq!(
        refusal["diagnostics"][0]["code"], "wait_target_unresolved",
        "{refusal}"
    );
    assert_eq!(refusal["diagnostics"].as_array().unwrap().len(), 1);
    assert_eq!(run_record(&predecessor)["state"], "waiting");

    // Revision 3 defines `ask` again.
    bundle.write_source("part.yaml", &format!("{ASKING_TREE_PART}# revision 3\n"));
    assert_eq!(publish()["revision"]["n"], 3);
    let document = completed(&upgrade());
    assert_eq!(document["outcome"], "upgraded", "{document}");
    let successor = std::path::PathBuf::from(document["runDirectory"].as_str().unwrap());
    let record = run_record(&successor);
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["revision"], 3, "{record}");
    assert_eq!(record["position"], path, "{record}");
    assert_eq!(fs::read(successor.join("wait.json")).unwrap(), wait);
    assert_eq!(read_json(&successor.join("wait.json"))["step"], path);
    assert_eq!(run_record(&predecessor)["outcome"], "superseded");

    let output = super::workflow_run_control::run_control(
        Some(&home),
        "answer",
        record["id"].as_str().unwrap(),
        &["--question", "1", "--text", "left"],
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    fs::remove_file(execution.join("verdict-ask-b")).unwrap();
    let terminal = completed(&super::workflow_continue::resume(&successor));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    let record = run_record(&successor);
    assert_eq!(record["outcome"], "landed", "{record}");
    // The successor's element 2 ran a fresh nested run from its entry; element 1 did not run.
    assert_eq!(ordinals_of(&successor.join("children/outer")), ["000002"]);
    let fresh = nested_run(&successor, 2);
    assert_eq!(attempt_count(&fresh), 1);
    assert_eq!(run_record(&fresh)["outcome"], "checked");
    assert_eq!(
        read_json(&execution.join("answers-ask-b")),
        serde_json::json!({"1": "left"})
    );
    assert!(!execution.join("answers-ask-a").exists());
    assert_eq!(
        fs::read_to_string(execution.join("stages.log")).unwrap(),
        "ask a\nwork a\nask b\nask b\nwork b\nfinish -\n"
    );
    assert_eq!(attempt_count(&nested_run(&predecessor, 2)), 1);
}

// REQ-CONTINUE-92, REQ-CONTINUE-80, REQ-CONTINUE-75: a successor waiting in a carried wait it
// imported holds no nested run, its predecessor's `payload.child` naming a directory it never
// had; withdrawing it ends it `withdrawn` and removes its wait record all the same.
#[test]
fn withdrawing_a_successor_ends_a_carried_wait_it_imported() {
    let (bundle, home, predecessor) = published_run_waiting_in_a_carried_answer();
    let predecessor_id = run_record(&predecessor)["id"].as_str().unwrap().to_owned();
    bundle.write_source("part.yaml", &format!("{ASKING_TREE_PART}# revision 2\n"));
    assert_eq!(publish_asking(&bundle, &home)["revision"]["n"], 2);
    let document = completed(&super::workflow_run_control::run_control(
        Some(&home),
        "upgrade",
        &predecessor_id,
        &[],
    ));
    assert_eq!(document["outcome"], "upgraded", "{document}");
    let successor = std::path::PathBuf::from(document["runDirectory"].as_str().unwrap());
    assert_eq!(run_record(&successor)["state"], "waiting");
    assert!(!successor.join("children").exists());

    let output = super::workflow_run_control::run_control(Some(&home), "withdraw", &successor, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let record = run_record(&successor);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "withdrawn", "{record}");
    assert!(!successor.join("wait.json").exists());
    assert_eq!(attempt_count(&nested_run(&predecessor, 2)), 1);
}
