use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};

use super::poll_until;
use rustix::process::{Pid, Signal, kill_process};

use super::workflow_continue::{
    FAN_OUT_TREE_ROOT, WAIT_TREE_ROOT, attempt_count, chained, completed, decide, decide_each,
    fan_out_tree, ordinals_of, partition_event, read_json, resume, status_document, wait_tree,
    wait_tree_with_json_questions, waiting_run,
};
use super::workflow_definition::Home;
use super::workflow_run::{
    RunBundle, TreeBundle, assert_usage_error, isolated_command, progress_position,
    progress_records, run_record, tree_rejection, workflow_run_schema,
};
use super::workflow_run_list::shown_plain;

/// The login name every control command in these tests runs as (REQ-LOCALRUN-181).
pub(super) const OPERATOR: &str = "control-operator";

/// `um workflow run LEAF RUN ARGS --json` as `OPERATOR`, with `UM_HOME` set when `home` is.
pub(super) fn run_control(
    home: Option<&Home>,
    leaf: &str,
    run: impl AsRef<std::ffi::OsStr>,
    args: &[&str],
) -> Output {
    control_command(home, leaf, run, args).output().unwrap()
}

/// The command `run_control` runs, to spawn.
fn control_command(
    home: Option<&Home>,
    leaf: &str,
    run: impl AsRef<std::ffi::OsStr>,
    args: &[&str],
) -> std::process::Command {
    let mut command = isolated_command(&["workflow".to_owned(), "run".to_owned(), leaf.to_owned()]);
    command
        .arg(run)
        .args(args)
        .arg("--json")
        .env("USER", OPERATOR);
    if let Some(home) = home {
        command.env("UM_HOME", home.path());
    }
    command
}

fn json(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        panic!(
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// The document of a control `leaf` that exited 0 with `outcome`, checked against the
/// facts every recorded control carries (REQ-LOCALRUN-181).
fn recorded_as(output: &Output, leaf: &str, outcome: &str, run: &Path) -> serde_json::Value {
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document = json(output);
    assert_eq!(document["schemaVersion"], 1, "{document}");
    assert_eq!(document["command"], format!("um workflow run {leaf}"));
    assert_eq!(document["outcome"], outcome, "{document}");
    assert_eq!(document["exitStatus"], 0);
    assert_eq!(document["runDirectory"], run.to_str().unwrap());
    assert_eq!(document["run"], run_record(run)["id"]);
    document
}

fn recorded(output: &Output, leaf: &str, run: &Path) -> serde_json::Value {
    recorded_as(output, leaf, "recorded", run)
}

/// The one diagnostic of a control `leaf` refused with `code`, exit 1.
pub(super) fn refused(output: &Output, leaf: &str, code: &str) -> serde_json::Value {
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let document = json(output);
    assert_eq!(document["schemaVersion"], 1, "{document}");
    assert_eq!(document["command"], format!("um workflow run {leaf}"));
    assert_eq!(document["outcome"], "refused", "{document}");
    assert_eq!(document["exitStatus"], 1);
    let diagnostics = document["diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 1, "{document}");
    assert_eq!(diagnostics[0]["code"], code, "{document}");
    assert!(diagnostics[0]["message"].is_string(), "{document}");
    document
}

/// The control file a recorded document names, checked against the published schema.
fn control_file(run: &Path, document: &serde_json::Value) -> serde_json::Value {
    let control = read_json(&run.join(document["control"]["path"].as_str().unwrap()));
    assert!(
        super::workflow_definition::schema("workflow-run-control-v1.schema.json")
            .is_valid(&control),
        "{control}"
    );
    assert_eq!(control["source"], "um");
    assert_eq!(control["event"], document["control"]["event"]);
    assert_eq!(control["run"], document["run"]);
    assert_eq!(control["by"], OPERATOR);
    assert_eq!(
        control.get("wait").unwrap_or(&serde_json::Value::Null),
        &document["control"]["wait"]
    );
    control
}

fn control_count(run: &Path) -> usize {
    fs::read_dir(run.join("controls")).map_or(0, Iterator::count)
}

fn wait_id(wait: &serde_json::Value) -> String {
    format!(
        "{}@{}",
        wait["kind"].as_str().unwrap(),
        wait["since"].as_str().unwrap()
    )
}

/// Answers the wait tree's text question, whose payload has no question list.
fn answer_the_one_question(run: &Path) {
    let output = run_control(None, "answer", run, &["--question", "1", "--text", "keep"]);
    let document = recorded(&output, "answer", run);
    assert_eq!(control_file(run, &document)["payload"]["complete"], true);
}

// REQ-LOCALRUN-181, REQ-LOCALRUN-182: answers accumulate per question and only the
// complete set resolves the wait.
#[test]
fn answers_accumulate_until_every_question_is_answered() {
    let questions = serde_json::json!([
        {"id": "q1", "text": "keep the cache?"},
        {"id": "q2", "text": "name?"},
    ]);
    let bundle = wait_tree_with_json_questions(&questions);
    let (run, terminal) = waiting_run(&bundle, "answered", "blocked");
    let wait = terminal["wait"].clone();
    assert_eq!(wait["kind"], "answer", "{terminal}");
    assert_eq!(wait["payload"]["questions"], questions);

    let output = run_control(None, "answer", &run, &["--question", "q1", "--text", "yes"]);
    let document = recorded(&output, "answer", &run);
    assert_eq!(document["control"]["ordinal"], 1);
    assert_eq!(document["control"]["event"], "control.answer");
    assert_eq!(document["control"]["path"], "controls/0001-answer.json");
    assert_eq!(document["control"]["wait"], wait_id(&wait));
    assert_eq!(document["wait"], wait);
    let control = control_file(&run, &document);
    assert_eq!(
        control["payload"],
        serde_json::json!({
            "question": "q1",
            "text": "yes",
            "answers": {"q1": "yes"},
            "complete": false,
        })
    );
    let held = completed(&resume(&run));
    assert_eq!(held["outcome"], "waiting", "{held}");
    assert_eq!(attempt_count(&run), 1);

    let unknown = run_control(None, "answer", &run, &["--question", "q3", "--text", "x"]);
    refused(&unknown, "answer", "question_unknown");
    assert_eq!(control_count(&run), 1);

    let output = run_control(None, "answer", &run, &["--question", "q2", "--text", "no"]);
    let document = recorded(&output, "answer", &run);
    assert_eq!(document["control"]["path"], "controls/0002-answer.json");
    let control = control_file(&run, &document);
    assert_eq!(
        control["payload"],
        serde_json::json!({
            "question": "q2",
            "text": "no",
            "answers": {"q1": "yes", "q2": "no"},
            "complete": true,
        })
    );
    // A recorded control resolves the wait: the person resumes the run instead.
    let resolved = run_control(
        None,
        "answer",
        &run,
        &["--question", "q1", "--text", "again"],
    );
    refused(&resolved, "answer", "wait_resolved");
    assert_eq!(control_count(&run), 2);

    decide(&bundle, "design", "planned");
    decide(&bundle, "review", "approve");
    let output = resume(&run);
    let terminal = completed(&output);
    assert_eq!(
        partition_event(&output)["continuation"]["fromSteps"],
        serde_json::json!(["design"])
    );
    assert_eq!(
        read_json(&bundle.execution_root().join("seen-answers")),
        serde_json::json!({"q1": "yes", "q2": "no"})
    );
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");
}

// REQ-LOCALRUN-183: `approve` continues at the approval's `then` and `hand-back` at its
// `onHandBack` with the notes.
#[test]
fn an_approval_wait_is_approved_or_handed_back() {
    let bundle = wait_tree();
    let (run, terminal) = waiting_run(&bundle, "handed-back", "blocked");
    refused(
        &run_control(None, "approve", &run, &[]),
        "approve",
        "wait_kind_mismatch",
    );
    assert_eq!(control_count(&run), 0);
    assert_eq!(terminal["wait"]["kind"], "answer");
    answer_the_one_question(&run);
    decide(&bundle, "design", "planned");
    decide(&bundle, "review", "approve");
    let approval = completed(&resume(&run))["wait"].clone();
    assert_eq!(approval["kind"], "approval", "{approval}");

    let output = run_control(None, "hand-back", &run, &["--notes", "tighten it"]);
    let document = recorded(&output, "hand-back", &run);
    assert_eq!(document["control"]["event"], "control.hand_back");
    assert_eq!(document["control"]["wait"], wait_id(&approval));
    assert_eq!(
        control_file(&run, &document)["payload"],
        serde_json::json!({"notes": "tighten it"})
    );
    decide(&bundle, "fix", "built");
    decide(&bundle, "merge", "landed");
    let output = resume(&run);
    let terminal = completed(&output);
    assert_eq!(
        partition_event(&output)["continuation"]["fromSteps"],
        serde_json::json!(["fix"])
    );
    assert_eq!(
        fs::read_to_string(bundle.execution_root().join("seen-notes")).unwrap(),
        "tighten it"
    );
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");

    let (run, terminal) = waiting_run(&bundle, "approved", "planned");
    let approval = &terminal["wait"];
    assert_eq!(approval["kind"], "approval", "{terminal}");
    let output = run_control(None, "approve", &run, &[]);
    let document = recorded(&output, "approve", &run);
    assert_eq!(document["control"]["event"], "control.approve");
    assert_eq!(document["control"]["wait"], wait_id(approval));
    assert!(control_file(&run, &document).get("payload").is_none());
    let output = resume(&run);
    let terminal = completed(&output);
    assert_eq!(
        partition_event(&output)["continuation"]["fromSteps"],
        serde_json::json!(["merge"])
    );
    assert_eq!(run_record(&run)["outcome"], "landed", "{terminal}");
}

// REQ-LOCALRUN-184: `retry` on a repair wait records the retry with the replacement and
// runs the continuation itself.
#[test]
fn a_repair_wait_is_retried_by_the_retry_command() {
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
    let replacement = bundle.result("replacement.json");
    fs::create_dir_all(replacement.parent().unwrap()).unwrap();
    fs::write(
        &replacement,
        serde_json::to_vec(&serde_json::json!({"schemaVersion": 1, "agentProfiles": profiles}))
            .unwrap(),
    )
    .unwrap();
    decide(&bundle, "design", "planned");
    decide(&bundle, "review", "approve");
    let output = run_control(
        None,
        "retry",
        &run,
        &["--agent-profiles", replacement.to_str().unwrap()],
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let last = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<serde_json::Value>()
        .map(Result::unwrap)
        .last()
        .unwrap();
    assert!(workflow_run_schema().is_valid(&last), "{last}");
    assert_eq!(last["command"], "um workflow run retry", "{last}");
    assert_eq!(last["result"]["continuation"]["reason"], "retry", "{last}");
    assert_eq!(last["wait"]["kind"], "approval", "{last}");
    assert_eq!(partition_event(&output)["continuation"]["reason"], "retry");
    let control = read_json(&run.join("controls/0001-retry.json"));
    assert_eq!(control["event"], "control.retry");
    assert_eq!(control["by"], OPERATOR);
    assert_eq!(control["wait"], wait_id(&wait));
    assert_eq!(control["payload"]["agentProfiles"], profiles);
    assert_eq!(attempt_count(&run), 2);
    let child = read_json(&run.join("children/design/000002/attempts/000001/result/result.json"));
    assert_eq!(
        child["workflow"]["agentProfileReplacement"]["profiles"]["unused"]["harness"]["kind"],
        "claude_code",
        "{child}"
    );

    let (run, _) = waiting_run(&bundle, "not-repair", "blocked");
    refused(
        &run_control(None, "retry", &run, &[]),
        "retry",
        "wait_kind_mismatch",
    );
    assert_eq!(control_count(&run), 0);
    assert_eq!(attempt_count(&run), 1);
}

// REQ-LOCALRUN-185: a stop parks the run before its step until `lift` names it, and
// `create --stop-before` sets one before the first step starts.
#[test]
fn a_stop_parks_a_run_until_it_is_lifted() {
    let bundle = wait_tree();
    let (run, terminal) = waiting_run(&bundle, "stopped", "blocked");
    let output = run_control(
        None,
        "stop",
        &run,
        &["--before", "review", "--reason", "freeze"],
    );
    let document = recorded(&output, "stop", &run);
    assert_eq!(document["control"]["event"], "control.stop");
    assert_eq!(document["control"]["wait"], serde_json::Value::Null);
    assert_eq!(document["wait"], terminal["wait"]);
    let stop = control_file(&run, &document);
    assert_eq!(
        stop["payload"],
        serde_json::json!({"before": "review", "reason": "freeze"})
    );
    refused(
        &run_control(None, "stop", &run, &["--before", "review", "--reason", "x"]),
        "stop",
        "stop_in_force",
    );
    refused(
        &run_control(
            None,
            "stop",
            &run,
            &["--before", "nowhere", "--reason", "x"],
        ),
        "stop",
        "step_unknown",
    );
    assert_eq!(control_count(&run), 1);

    answer_the_one_question(&run);
    decide(&bundle, "design", "planned");
    let parked = completed(&resume(&run));
    assert_eq!(parked["outcome"], "waiting", "{parked}");
    let wait = &parked["wait"];
    assert_eq!(wait["kind"], "stop", "{parked}");
    assert_eq!(wait["step"], serde_json::json!(["review"]));

    let output = run_control(None, "lift", &run, &[]);
    let document = recorded(&output, "lift", &run);
    assert_eq!(document["control"]["event"], "control.lift");
    assert_eq!(document["control"]["wait"], wait_id(wait));
    assert_eq!(
        control_file(&run, &document)["payload"],
        serde_json::json!({"before": "review", "stop": stop["at"]})
    );
    refused(
        &run_control(None, "lift", &run, &[]),
        "lift",
        "stop_not_in_force",
    );
    decide(&bundle, "review", "approve");
    let output = resume(&run);
    let terminal = completed(&output);
    assert_eq!(
        partition_event(&output)["continuation"]["fromSteps"],
        serde_json::json!(["review"])
    );
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");

    for (step, reason) in [("fix", "hold fix"), ("merge", "hold merge")] {
        recorded(
            &run_control(None, "stop", &run, &["--before", step, "--reason", reason]),
            "stop",
            &run,
        );
    }
    refused(
        &run_control(None, "lift", &run, &[]),
        "lift",
        "stop_ambiguous",
    );
    let lifted = recorded(
        &run_control(None, "lift", &run, &["--before", "merge"]),
        "lift",
        &run,
    );
    assert_eq!(lifted["control"]["wait"], serde_json::Value::Null);
    assert_eq!(control_file(&run, &lifted)["payload"]["before"], "merge");

    // `create --stop-before` parks the first attempt before `review`.
    decide(&bundle, "design", "planned");
    let destination = bundle.result("created-stopped");
    let mut args = bundle.args(&destination);
    args.splice(
        args.len() - 1..args.len() - 1,
        ["--stop-before".to_owned(), "review".to_owned()],
    );
    let terminal = completed(
        &isolated_command(&args)
            .env("USER", OPERATOR)
            .output()
            .unwrap(),
    );
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    assert_eq!(terminal["wait"]["kind"], "stop", "{terminal}");
    assert_eq!(terminal["wait"]["then"], "review");
    let created = fs::canonicalize(&destination).unwrap();
    let stop = read_json(&created.join("controls/0001-stop.json"));
    assert_eq!(stop["event"], "control.stop");
    assert_eq!(stop["by"], OPERATOR);
    assert_eq!(stop["payload"], serde_json::json!({"before": "review"}));
    assert!(stop.get("wait").is_none(), "{stop}");

    // REQ-LOCALRUN-80: a step the workflow does not have is a usage error.
    let destination = bundle.result("stop-nowhere");
    let mut args = bundle.args(&destination);
    args.splice(
        args.len() - 1..args.len() - 1,
        ["--stop-before".to_owned(), "nowhere".to_owned()],
    );
    assert_usage_error(&args);
    assert!(!destination.exists());
}

// REQ-LOCALRUN-186, REQ-LOCALRUN-187, REQ-LOCALRUN-185: `withdraw` and `cancel` end a
// waiting run themselves, and `release` records an exited run's release once.
#[test]
fn withdraw_cancel_and_release_end_and_release_runs() {
    let bundle = wait_tree();
    let (run, terminal) = waiting_run(&bundle, "withdrawn", "blocked");
    let output = run_control(None, "withdraw", &run, &[]);
    let document = recorded_as(&output, "withdraw", "withdrawn", &run);
    assert_eq!(document["control"]["event"], "control.withdraw");
    assert_eq!(document["control"]["wait"], wait_id(&terminal["wait"]));
    assert_eq!(document["wait"], serde_json::Value::Null);
    control_file(&run, &document);
    let record = run_record(&run);
    assert_eq!(record["state"], "exited");
    assert_eq!(record["outcome"], "withdrawn");
    assert_eq!(record["attempts"], 1);
    assert!(!run.join("wait.json").exists());
    assert_eq!(attempt_count(&run), 1);
    refused(
        &run_control(None, "withdraw", &run, &[]),
        "withdraw",
        "run_exited",
    );
    refused(
        &run_control(None, "stop", &run, &["--before", "review", "--reason", "x"]),
        "stop",
        "run_exited",
    );

    let (canceled, terminal) = waiting_run(&bundle, "canceled", "blocked");
    refused(
        &run_control(None, "release", &canceled, &[]),
        "release",
        "run_live",
    );
    let output = run_control(None, "cancel", &canceled, &[]);
    let document = recorded_as(&output, "cancel", "canceled", &canceled);
    assert_eq!(document["control"]["event"], "control.cancel");
    assert_eq!(document["control"]["wait"], wait_id(&terminal["wait"]));
    control_file(&canceled, &document);
    let record = run_record(&canceled);
    assert_eq!(record["state"], "exited");
    assert_eq!(record["outcome"], "canceled");
    assert!(!canceled.join("wait.json").exists());

    let output = run_control(None, "release", &run, &[]);
    let document = recorded(&output, "release", &run);
    assert_eq!(document["control"]["event"], "control.release");
    assert_eq!(document["control"]["wait"], serde_json::Value::Null);
    let release = control_file(&run, &document);
    assert!(release.get("payload").is_none(), "{release}");
    refused(
        &run_control(None, "release", &run, &[]),
        "release",
        "run_released",
    );
}

/// Kills the `um` process it holds if a test leaves it running, and lets a held step go.
pub(super) struct LiveRun {
    pub(super) process: Child,
    pub(super) release: PathBuf,
}

impl LiveRun {
    /// Sends `signal` to the `um` process alone and returns its output once it exits.
    pub(super) fn signal(&mut self, signal: Signal) -> Output {
        kill_process(self.pid(), signal).unwrap();
        self.output()
    }

    fn pid(&self) -> Pid {
        Pid::from_raw(i32::try_from(self.process.id()).unwrap()).unwrap()
    }

    /// The `um` process's output once it exits.
    fn output(&mut self) -> Output {
        use std::io::Read as _;
        let stderr = self.process.stderr.take().map(|mut pipe| {
            std::thread::spawn(move || {
                let mut bytes = Vec::new();
                pipe.read_to_end(&mut bytes).unwrap();
                bytes
            })
        });
        let mut stdout = Vec::new();
        if let Some(mut pipe) = self.process.stdout.take() {
            pipe.read_to_end(&mut stdout).unwrap();
        }
        Output {
            status: self.process.wait().unwrap(),
            stdout,
            stderr: stderr
                .map(|reader| reader.join().unwrap())
                .unwrap_or_default(),
        }
    }
}

impl Drop for LiveRun {
    fn drop(&mut self) {
        let _ = fs::write(&self.release, "");
        if self.process.try_wait().ok().flatten().is_none() {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }
}

// REQ-LOCALRUN-186: `cancel` signals the process that holds a live run, which cancels its
// step and marks the record `exited` `canceled`.
#[test]
fn cancel_ends_a_live_run() {
    let bundle = wait_tree();
    let execution = bundle.execution_root();
    fs::write(execution.join("hold-design"), "").unwrap();
    decide(&bundle, "design", "planned");
    let destination = bundle.result("live");
    let mut live = LiveRun {
        process: isolated_command(&bundle.args(&destination))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
        release: execution.join("release-design"),
    };
    poll_until(
        "the live run's held step",
        || {
            execution.join("held-design").exists()
                && um_execution::lock_holder(&destination).is_ok_and(|holder| holder.is_some())
        },
        |held| *held,
    );
    let run = fs::canonicalize(&destination).unwrap();

    let output = run_control(None, "cancel", &run, &[]);
    let document = recorded_as(&output, "cancel", "canceled", &run);
    assert_eq!(document["control"]["event"], "control.cancel");
    assert_eq!(document["control"]["wait"], serde_json::Value::Null);
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "canceled", "{record}");
    let exited = poll_until(
        "the live run's process exit",
        || live.process.try_wait().unwrap(),
        Option::is_some,
    );
    assert!(exited.is_some());
}

// REQ-LOCALRUN-186, REQ-CONTINUE-2: `cancel` of a `running` run whose process is gone settles
// the abandoned attempt and marks the record `exited` `canceled` without an attempt.
#[test]
fn cancel_ends_an_abandoned_run() {
    ends_an_abandoned_run("cancel", "canceled");
}

// REQ-LOCALRUN-186, REQ-CONTINUE-2: `withdraw` of a `running` run whose process is gone
// settles the abandoned attempt and marks the record `exited` `withdrawn` without an attempt.
#[test]
fn withdraw_ends_an_abandoned_run() {
    ends_an_abandoned_run("withdraw", "withdrawn");
}

/// Runs the wait tree until its `design` step holds, kills the `um` process, and ends the
/// abandoned run with `leaf`, which records `control.<leaf>` and exits the run `outcome`.
fn ends_an_abandoned_run(leaf: &str, outcome: &str) {
    let bundle = wait_tree();
    let execution = bundle.execution_root();
    fs::write(execution.join("hold-design"), "").unwrap();
    decide(&bundle, "design", "planned");
    let destination = bundle.result("abandoned");
    let mut live = LiveRun {
        process: isolated_command(&bundle.args(&destination))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
        release: execution.join("release-design"),
    };
    poll_until(
        "the run's held step",
        || {
            execution.join("held-design").exists()
                && um_execution::lock_holder(&destination).is_ok_and(|holder| holder.is_some())
        },
        |held| *held,
    );
    let run = fs::canonicalize(&destination).unwrap();
    live.process.kill().unwrap();
    live.process.wait().unwrap();
    assert_eq!(run_record(&run)["state"], "running");
    fs::write(&live.release, "").unwrap();

    // The orphaned step process ends first; until it does, the attempt's ownership is
    // unproven and the command fails without recording anything.
    let output = poll_until(
        &format!("the abandoned run's {leaf}"),
        || run_control(None, leaf, &run, &[]),
        |output| output.status.code() == Some(0),
    );
    let document = recorded_as(&output, leaf, outcome, &run);
    assert_eq!(document["control"]["event"], format!("control.{leaf}"));
    assert_eq!(document["control"]["wait"], serde_json::Value::Null);
    assert_eq!(
        document["control"]["path"],
        format!("controls/0001-{leaf}.json")
    );
    assert_eq!(document["wait"], serde_json::Value::Null);
    control_file(&run, &document);
    assert_eq!(control_count(&run), 1);
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], outcome, "{record}");
    assert_eq!(record["attempts"], 1, "{record}");
    assert_eq!(attempt_count(&run), 1);
    let shown = json(&run_control(None, "show", &run, &[]));
    assert_eq!(shown["attempts"][0]["state"], "interrupted", "{shown}");
    refused(&run_control(None, leaf, &run, &[]), leaf, "run_exited");
}

/// The first line of the wait tree's root as a definition publishes it, naming the tree.
const WAIT_TREE_HEADER: &str = "schemaVersion: 1\nname: waits\n";

/// The wait tree's header naming its `ticket` input as its `subject` (REQ-WFFORMAT-100).
const SUBJECT_HEADER: &str =
    "schemaVersion: 1\nname: waits\nsubject: ticket\ninputs:\n  ticket:\n    kind: text\n";

/// The wait tree root's `design` edge for `failed`, and the limited edge that replaces it.
const DESIGN_REPAIR_EDGE: &str = "      failed:\n        wait: repair\n        then: design\n";
const DESIGN_LIMITED_EDGE: &str =
    "      failed:\n        then: design\n        limit: 2\n        else: exit.failed\n";

/// The wait tree's root, its first line replaced by `header`.
fn wait_tree_root(header: &str) -> String {
    WAIT_TREE_ROOT.replacen("schemaVersion: 1\n", header, 1)
}

/// The fan-out tree root's `build` lines that map its parts over `forEach`.
const FAN_OUT_FOR_EACH: &str = "    forEach: outputs.design.parts\n";
const FAN_OUT_PART_INPUT: &str = "      part:\n        ref: item.name\n";

/// The fan-out tree's `root` named as a definition publishes it.
fn fan_out_root(root: &str) -> String {
    root.replacen("schemaVersion: 1\n", "schemaVersion: 1\nname: fanout\n", 1)
}

/// The wait tree published as a definition under a fresh `um` home (REQ-WFDEF-17), with
/// its execution root.
pub(super) struct PublishedWaitTree {
    pub(super) home: Home,
    pub(super) bundle: TreeBundle,
    pub(super) definition: String,
}

impl PublishedWaitTree {
    pub(super) fn new() -> Self {
        Self::publish(wait_tree())
    }

    /// The wait tree whose `design` asks `questions` as JSON, published as `new` does.
    pub(super) fn with_json_questions(questions: &serde_json::Value) -> Self {
        Self::publish(wait_tree_with_json_questions(questions))
    }

    /// The wait tree whose root names its `ticket` input as its `subject`, published as `new`
    /// does (REQ-WFFORMAT-100).
    pub(super) fn with_subject() -> Self {
        Self::publish_root(wait_tree(), &wait_tree_root(SUBJECT_HEADER))
    }

    /// The subject's wait tree whose `design` continues at `design` over a limited `failed`
    /// edge (REQ-WFEXEC-64) instead of waiting for a repair, published as `new` does.
    pub(super) fn with_subject_retrying_design() -> Self {
        let root = wait_tree_root(SUBJECT_HEADER);
        assert!(root.contains(DESIGN_REPAIR_EDGE), "{root}");
        Self::publish_root(
            wait_tree(),
            &root.replacen(DESIGN_REPAIR_EDGE, DESIGN_LIMITED_EDGE, 1),
        )
    }

    /// The fan-out tree, which names no `subject`, published as `new` does.
    pub(super) fn fan_out() -> Self {
        Self::publish_root(fan_out_tree(), &fan_out_root(FAN_OUT_TREE_ROOT))
    }

    fn publish(bundle: TreeBundle) -> Self {
        Self::publish_root(bundle, &wait_tree_root(WAIT_TREE_HEADER))
    }

    /// Publishes `bundle` with `root` as its `workflow.yaml`.
    fn publish_root(bundle: TreeBundle, root: &str) -> Self {
        bundle.write_source("workflow.yaml", root);
        let home = Home::new();
        let report = home.json(&["publish", bundle.source_root().to_str().unwrap()]);
        assert_eq!(report["outcome"], "published", "{report}");
        let definition = report["definition"]["id"].as_str().unwrap().to_owned();
        Self {
            home,
            bundle,
            definition,
        }
    }

    /// A run of the definition whose `design` decides `verdict`: its directory under the
    /// home, its id and its `--json` document.
    pub(super) fn run(&self, verdict: &str) -> (PathBuf, String, serde_json::Value) {
        decide(&self.bundle, "design", verdict);
        let terminal = completed(&self.create(&[]));
        let directory = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
        let id = run_record(&directory)["id"].as_str().unwrap().to_owned();
        (directory, id, terminal)
    }

    /// `um workflow run create` of the definition with `args` as `OPERATOR`.
    pub(super) fn create(&self, args: &[&str]) -> Output {
        self.create_in(&self.definition, args)
    }

    /// `um workflow run create` of `definition` under this home with `args` as `OPERATOR`.
    pub(super) fn create_in(&self, definition: &str, args: &[&str]) -> Output {
        isolated_command(&[
            "workflow".to_owned(),
            "run".to_owned(),
            "create".to_owned(),
            definition.to_owned(),
            "--execution-root".to_owned(),
            self.bundle.execution_root().to_str().unwrap().to_owned(),
            "--json".to_owned(),
        ])
        .args(args)
        .env("UM_HOME", self.home.path())
        .env("USER", OPERATOR)
        .output()
        .unwrap()
    }

    /// Publishes the tree again with `args`: a new revision of the definition, or with
    /// `--new` a second definition; the report.
    pub(super) fn republish(&self, args: &[&str]) -> serde_json::Value {
        let source = self.bundle.source_root();
        let report = self
            .home
            .json(&[&["publish", source.to_str().unwrap()], args].concat());
        assert_eq!(report["outcome"], "published", "{report}");
        report
    }

    /// A run of `definition` for `subject` whose `design` waits on an answer: its directory
    /// and id.
    pub(super) fn waiting_for(&self, definition: &str, subject: &str) -> (PathBuf, String) {
        decide(&self.bundle, "design", "blocked");
        let terminal = completed(&self.create_in(definition, &["--input-text", "ticket", subject]));
        assert_eq!(terminal["wait"]["kind"], "answer", "{terminal}");
        let directory = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
        let id = run_record(&directory)["id"].as_str().unwrap().to_owned();
        (directory, id)
    }
}

// REQ-LOCALRUN-180: a subject that names no path addresses the run its definition's slot
// holds, live and then exited; a subject two definitions' slots hold is `subject_ambiguous`
// naming both runs, and one no slot holds is `run_not_found`.
#[test]
fn run_commands_address_a_run_by_its_subject() {
    let published = PublishedWaitTree::with_subject();
    let home = Some(&published.home);
    let (run, id) = published.waiting_for(&published.definition, "AUTO-1");

    let shown = json(&run_control(home, "show", "AUTO-1", &[]));
    assert_eq!(shown["outcome"], "shown", "{shown}");
    assert_eq!(shown["runDirectory"], run.to_str().unwrap(), "{shown}");
    assert_eq!(shown["record"]["id"], id.as_str());
    assert_eq!(shown["record"]["state"], "waiting");
    refused(
        &run_control(home, "release", "AUTO-1", &[]),
        "release",
        "run_live",
    );
    recorded_as(
        &run_control(home, "withdraw", "AUTO-1", &[]),
        "withdraw",
        "withdrawn",
        &run,
    );
    let shown = json(&run_control(home, "show", "AUTO-1", &[]));
    assert_eq!(shown["record"]["state"], "exited", "{shown}");
    let released = recorded(
        &run_control(home, "release", "AUTO-1", &[]),
        "release",
        &run,
    );
    assert_eq!(released["control"]["event"], "control.release");

    // A second definition published from the same tree holds its own slot for the subject.
    let canary = published.republish(&["--new"]);
    let canary = canary["definition"]["id"].as_str().unwrap().to_owned();
    let (_, canary_run) = published.waiting_for(&canary, "AUTO-1");
    for leaf in ["show", "release"] {
        let refusal = refused(
            &run_control(home, leaf, "AUTO-1", &[]),
            leaf,
            "subject_ambiguous",
        );
        assert!(refusal.get("runDirectory").is_none(), "{refusal}");
        let message = refusal["diagnostics"][0]["message"].as_str().unwrap();
        assert!(
            message.contains(&id) && message.contains(&canary_run),
            "{refusal}"
        );
    }

    let unknown = refused(
        &run_control(home, "show", "AUTO-9", &[]),
        "show",
        "run_not_found",
    );
    assert!(unknown.get("runDirectory").is_none(), "{unknown}");
}

// REQ-LOCALRUN-173, REQ-TRIGGER-122: a second run for a subject whose run waits is refused
// `subject_busy` with the waiting run's id, and no run directory is created.
#[test]
fn a_second_run_for_a_waiting_subject_is_busy() {
    let published = PublishedWaitTree::with_subject();
    decide(&published.bundle, "design", "blocked");
    let ticket = ["--input-text", "ticket", "AUTO-1"];
    let first = completed(&published.create(&ticket));
    assert_eq!(first["wait"]["kind"], "answer", "{first}");
    let directory = PathBuf::from(first["runDirectory"].as_str().unwrap());
    let record = run_record(&directory);
    assert_eq!(record["subject"], "AUTO-1");
    assert_eq!(record["state"], "waiting");

    let rejection = tree_rejection(&published.create(&ticket));
    assert_eq!(rejection["phase"], "definition", "{rejection}");
    let diagnostics = rejection["diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 1, "{rejection}");
    assert_eq!(diagnostics[0]["code"], "subject_busy", "{rejection}");
    assert_eq!(diagnostics[0]["run"], record["id"], "{rejection}");
    assert_eq!(diagnostics[0]["definition"], published.definition.as_str());
    assert_eq!(
        fs::read_dir(directory.parent().unwrap())
            .unwrap()
            .filter(|entry| !entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with('.'))
            .count(),
        1
    );
}

// REQ-LOCALRUN-180, REQ-LOCALRUN-181: a control command addresses a run under the home by
// its id, and refuses an unknown id and a run without a run record.
#[test]
fn control_commands_address_a_run_by_its_id() {
    let published = PublishedWaitTree::new();
    let (run, id, terminal) = published.run("blocked");
    assert_eq!(terminal["wait"]["kind"], "answer", "{terminal}");
    let home = Some(&published.home);
    let output = run_control(home, "answer", &id, &["--question", "1", "--text", "yes"]);
    let document = recorded(&output, "answer", &run);
    assert_eq!(document["run"], id.as_str());
    assert_eq!(control_file(&run, &document)["payload"]["complete"], true);

    let unknown = run_control(home, "approve", "run_7zzzzzzzzzzzzzzzzzzzzzzzzz", &[]);
    let refusal = refused(&unknown, "approve", "run_not_found");
    assert!(refusal.get("runDirectory").is_none(), "{refusal}");

    // A run admitted from a workflow file's closure has no run record.
    let closure = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let destination = closure.result("closure");
    let created = isolated_command(&closure.args(&destination))
        .output()
        .unwrap();
    assert!(created.status.success(), "{created:?}");
    let output = run_control(
        None,
        "answer",
        &destination,
        &["--question", "1", "--text", "x"],
    );
    refused(&output, "answer", "run_record_missing");
    assert!(!destination.join("controls").exists());
}

// REQ-LOCALRUN-80: a control command missing a required option, and `retry` on a run that
// does not wait without `--execution-root`, are usage errors.
#[test]
fn control_command_usage_errors_exit_two() {
    let bundle = wait_tree();
    let (run, _) = waiting_run(&bundle, "usage", "blocked");
    let run = run.to_str().unwrap();
    for args in [
        vec!["answer", run, "--text", "x"],
        vec!["answer", run, "--question", "1"],
        vec!["hand-back", run],
        vec!["stop", run, "--before", "review"],
        vec!["stop", run, "--reason", "x"],
        vec!["approve", run, "--execution-root", run],
    ] {
        let args = ["workflow", "run"]
            .into_iter()
            .chain(args)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert_usage_error(&args);
    }
    assert!(!Path::new(run).join("controls").exists());

    let closure = RunBundle::new(
        "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"false\"]\n",
    );
    let destination = closure.result("failed");
    let created = isolated_command(&closure.args(&destination))
        .output()
        .unwrap();
    assert_eq!(created.status.code(), Some(1), "{created:?}");
    assert_usage_error(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "retry".to_owned(),
        destination.to_str().unwrap().to_owned(),
    ]);
}

/// A subject's waiting run restarted onto a changed revision (REQ-LOCALRUN-194): the wait
/// tree published with `subject: ticket`, a run for `AUTO-1` waiting on `answer` after
/// `design` on revision 1, revision 2 publishing a changed `review.yaml`, and `um workflow run
/// restart <predecessor id> --at review --json` run to its `approval` wait.
pub(super) struct RestartedSubject {
    pub(super) published: PublishedWaitTree,
    pub(super) predecessor: PathBuf,
    pub(super) predecessor_id: String,
    pub(super) successor: PathBuf,
    pub(super) successor_id: String,
    /// The restart's `--json` document, checked against the run-result schema.
    pub(super) terminal: serde_json::Value,
    /// The restart's `continuation_partition` event.
    pub(super) partition: serde_json::Value,
}

impl RestartedSubject {
    pub(super) fn new() -> Self {
        let published = PublishedWaitTree::with_subject();
        let (predecessor, predecessor_id) = published.waiting_for(&published.definition, "AUTO-1");
        let review = published.bundle.source_root().join("review.yaml");
        let changed = format!("{}# revision 2\n", fs::read_to_string(&review).unwrap());
        published.bundle.write_source("review.yaml", &changed);
        assert_eq!(published.republish(&[])["revision"]["n"], 2);
        decide(&published.bundle, "review", "approve");
        let output = run_control(
            Some(&published.home),
            "restart",
            &predecessor_id,
            &["--at", "review"],
        );
        let terminal = completed(&output);
        let partition = partition_event(&output);
        let successor = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
        let successor_id = run_record(&successor)["id"].as_str().unwrap().to_owned();
        Self {
            published,
            predecessor,
            predecessor_id,
            successor,
            successor_id,
            terminal,
            partition,
        }
    }
}

/// The bytes of a run's `run.json` and `state.json`, which a refused command leaves as they
/// are.
fn records(run: &Path) -> (Vec<u8>, Vec<u8>) {
    (
        fs::read(run.join("run.json")).unwrap(),
        fs::read(run.join("state.json")).unwrap(),
    )
}

/// The run directories under a definition's `runs/`, staging entries aside.
fn run_directories(runs: &Path) -> usize {
    fs::read_dir(runs)
        .unwrap()
        .filter(|entry| {
            !entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with('.')
        })
        .count()
}

/// A successor command `leaf` refused before anything was created: the document at `phase`
/// naming `um workflow run <leaf>`.
fn successor_command_refused(output: &Output, leaf: &str, phase: &str) -> serde_json::Value {
    let rejection = tree_rejection(output);
    assert_eq!(
        rejection["command"],
        format!("um workflow run {leaf}"),
        "{rejection}"
    );
    assert_eq!(rejection["phase"], phase, "{rejection}");
    rejection
}

/// A `restart` refused before anything was created: the document at `phase` naming the
/// command, with its first diagnostic's `code`.
fn restart_refused(output: &Output, phase: &str, code: &str) -> serde_json::Value {
    let rejection = successor_command_refused(output, "restart", phase);
    assert_eq!(rejection["diagnostics"][0]["code"], code, "{rejection}");
    rejection
}

/// An `upgrade` refused before anything was created: the document at `phase` whose
/// diagnostics carry exactly `codes`, in order.
fn upgrade_refused(output: &Output, phase: &str, codes: &[&str]) -> serde_json::Value {
    let rejection = successor_command_refused(output, "upgrade", phase);
    let found = rejection["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|diagnostic| diagnostic["code"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(found, codes, "{rejection}");
    rejection
}

// REQ-LOCALRUN-194, REQ-LOCALRUN-196, REQ-CONTINUE-79, REQ-WFDEF-28, SC-CONTINUE-9: a
// restart creates a successor on the current revision that inherits `design` and runs from
// `review` under revision 2, supersedes the waiting predecessor and moves the subject's
// slot; a refused restart leaves both runs as they were.
#[test]
fn a_restart_moves_a_waiting_subject_to_the_current_revision() {
    let restarted = RestartedSubject::new();
    let home = Some(&restarted.published.home);
    let (predecessor, successor) = (&restarted.predecessor, &restarted.successor);
    let (predecessor_id, successor_id) = (&restarted.predecessor_id, &restarted.successor_id);

    let terminal = &restarted.terminal;
    assert_eq!(terminal["command"], "um workflow run restart", "{terminal}");
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");
    assert_ne!(successor, predecessor);
    assert_eq!(successor.parent(), predecessor.parent());
    let continuation = &restarted.partition["continuation"];
    assert_eq!(continuation["reason"], "restart", "{continuation}");
    assert_eq!(continuation["fromSteps"], serde_json::json!(["review"]));
    assert_eq!(continuation["inheritedSteps"][0]["id"], "design");
    assert_eq!(
        continuation["inheritedSteps"][0]["definitionChanged"],
        false
    );
    let source = &continuation["definitionSource"];
    assert_eq!(source["kind"], "replaced", "{continuation}");
    assert_ne!(source["priorManifestDigest"], source["manifestDigest"]);

    let record = run_record(successor);
    assert_eq!(record["predecessor"], predecessor_id.as_str(), "{record}");
    assert_eq!(record["successor"], serde_json::Value::Null);
    assert_eq!(record["revision"], 2);
    assert_eq!(record["attempts"], 2);
    assert_eq!(record["subject"], "AUTO-1");
    assert_eq!(record["state"], "waiting");
    let state = read_json(&successor.join("state.json"));
    assert_eq!(
        state["attempts"][0]["imported"]["run"],
        predecessor_id.as_str()
    );
    assert_eq!(state["attempts"][1]["trigger"], "continuation", "{state}");
    assert!(state["attempts"][1].get("imported").is_none(), "{state}");

    let record = run_record(predecessor);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "superseded", "{record}");
    assert_eq!(record["successor"], successor_id.as_str(), "{record}");
    assert_eq!(record["attempts"], 1);
    assert!(!predecessor.join("wait.json").exists());
    assert_eq!(control_count(predecessor), 0);
    assert!(!predecessor.join("mirror").exists());
    let shown = json(&run_control(home, "show", "AUTO-1", &[]));
    assert_eq!(
        shown["runDirectory"],
        successor.to_str().unwrap(),
        "{shown}"
    );

    let before = (records(predecessor), records(successor));
    let unchanged = || {
        assert_eq!((records(predecessor), records(successor)), before);
        assert_eq!(run_directories(predecessor.parent().unwrap()), 2);
    };
    let refusal = restart_refused(
        &run_control(home, "restart", predecessor_id, &["--at", "review"]),
        "successor",
        "run_exited",
    );
    assert_eq!(refusal["diagnostics"].as_array().unwrap().len(), 1);
    assert_eq!(refusal["diagnostics"][0]["run"], predecessor_id.as_str());
    unchanged();
    restart_refused(
        &run_control(home, "restart", successor_id, &["--at", "nosuch"]),
        "continuation",
        "continuation_definition_invalid",
    );
    unchanged();
    assert_eq!(run_record(successor)["state"], "waiting");
    // REQ-LOCALRUN-80: `--at` is required.
    assert_usage_error(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "restart".to_owned(),
        successor.to_str().unwrap().to_owned(),
    ]);
    unchanged();

    // A live process holds the successor while it runs `merge`.
    let bundle = &restarted.published.bundle;
    let execution = bundle.execution_root();
    recorded(
        &run_control(home, "approve", successor_id, &[]),
        "approve",
        successor,
    );
    fs::write(execution.join("hold-merge"), "").unwrap();
    decide(bundle, "merge", "landed");
    let mut live = LiveRun {
        process: isolated_command(&[
            "workflow".to_owned(),
            "run".to_owned(),
            "resume".to_owned(),
            successor.to_str().unwrap().to_owned(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap(),
        release: execution.join("release-merge"),
    };
    poll_until(
        "the successor's held merge",
        || {
            execution.join("held-merge").exists()
                && um_execution::lock_holder(successor).is_ok_and(|holder| holder.is_some())
        },
        |held| *held,
    );
    let before = (records(predecessor), records(successor));
    let refusal = restart_refused(
        &run_control(home, "restart", successor_id, &["--at", "review"]),
        "successor",
        "run_locked",
    );
    assert_eq!(refusal["diagnostics"][0]["run"], successor_id.as_str());
    assert_eq!((records(predecessor), records(successor)), before);
    fs::write(&live.release, "").unwrap();
    assert!(live.process.wait().unwrap().success());
    assert_eq!(run_record(successor)["outcome"], "landed");
}

// REQ-LOCALRUN-196, INV-ENGINE-26: a move the command died in, the successor published and
// named in the slot while the predecessor still waits, is completed by the successor's next
// continuation; when the predecessor moved on without it, the successor is withdrawn, the slot
// given back and the command refused `successor_abandoned`.
#[test]
fn an_unfinished_move_is_completed_or_undone_by_the_successors_continuation() {
    let restarted = RestartedSubject::new();
    let home = Some(&restarted.published.home);
    let (predecessor, successor) = (&restarted.predecessor, &restarted.successor);
    let (predecessor_id, successor_id) = (&restarted.predecessor_id, &restarted.successor_id);
    let slot = PathBuf::from(restarted.published.home.path())
        .join("workflows")
        .join(&restarted.published.definition)
        .join("subjects")
        .join("AUTO-1");
    let holder = || fs::read_to_string(&slot).unwrap();
    assert_eq!(holder(), format!("{successor_id}\n"));

    // The predecessor as a death before `supersede` leaves it: waiting, naming no successor,
    // its root wait record in place, while the successor exists and the slot names it.
    let unsupersede = |state: &str| {
        let mut record = run_record(predecessor);
        record["state"] = serde_json::Value::String(state.to_owned());
        record["outcome"] = serde_json::Value::Null;
        record["successor"] = serde_json::Value::Null;
        fs::write(
            predecessor.join("run.json"),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
    };
    unsupersede("waiting");
    fs::copy(
        predecessor.join("attempts/000001/wait.json"),
        predecessor.join("wait.json"),
    )
    .unwrap();

    let waiting = completed(&run_control(home, "resume", successor_id, &[]));
    assert_eq!(waiting["outcome"], "waiting", "{waiting}");
    let record = run_record(predecessor);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "superseded", "{record}");
    assert_eq!(record["successor"], successor_id.as_str(), "{record}");
    assert!(!predecessor.join("wait.json").exists());
    assert_eq!(run_record(successor)["state"], "waiting");
    assert_eq!(holder(), format!("{successor_id}\n"));

    // The predecessor continued without the successor: the successor is abandoned.
    unsupersede("running");
    let before = records(predecessor);
    let refusal = tree_rejection(&run_control(home, "resume", successor_id, &[]));
    assert_eq!(refusal["command"], "um workflow run resume", "{refusal}");
    assert_eq!(refusal["phase"], "continuation", "{refusal}");
    assert_eq!(
        refusal["diagnostics"][0]["code"], "successor_abandoned",
        "{refusal}"
    );
    let record = run_record(successor);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "withdrawn", "{record}");
    assert_eq!(record["attempts"], 2, "{record}");
    assert!(!successor.join("wait.json").exists());
    assert_eq!(control_count(successor), 0);
    assert_eq!(holder(), format!("{predecessor_id}\n"));
    assert_eq!(records(predecessor), before);
    restart_refused(
        &run_control(home, "restart", successor_id, &["--at", "review"]),
        "successor",
        "run_exited",
    );
}

// REQ-CONTINUE-29, REQ-CONTINUE-30, REQ-LOCALRUN-129, REQ-LOCALRUN-196: a superseded
// predecessor keeps its last attempt `waiting`, yet `resume` with or without `--from` and
// `retry` refuse it with `continuation_disposition_ineligible` naming the successor, leaving
// it as it was, and `status` reports its continuation and retry ineligible with that code.
#[test]
fn a_superseded_predecessor_is_not_continued_or_retried() {
    let restarted = RestartedSubject::new();
    let home = Some(&restarted.published.home);
    let predecessor = &restarted.predecessor;
    let (predecessor_id, successor_id) = (&restarted.predecessor_id, &restarted.successor_id);
    let runs = predecessor.parent().unwrap();
    let before = records(predecessor);
    let directories = run_directories(runs);
    let unchanged = || {
        assert_eq!(records(predecessor), before);
        assert_eq!(run_directories(runs), directories);
    };
    let refused = |output: &Output, command: &str, phase: &str| {
        let rejection = tree_rejection(output);
        assert_eq!(rejection["command"], command, "{rejection}");
        assert_eq!(rejection["phase"], phase, "{rejection}");
        let diagnostic = &rejection["diagnostics"][0];
        assert_eq!(
            diagnostic["code"], "continuation_disposition_ineligible",
            "{rejection}"
        );
        rejection
    };
    let names_the_successor = |rejection: &serde_json::Value| {
        let message = rejection["diagnostics"][0]["message"].as_str().unwrap();
        assert!(message.contains(successor_id.as_str()), "{rejection}");
        assert!(message.contains("superseded"), "{rejection}");
    };

    let rejection = refused(
        &run_control(home, "resume", predecessor_id, &["--from", "review"]),
        "um workflow run resume",
        "continuation",
    );
    names_the_successor(&rejection);
    unchanged();

    let execution_root = tempfile::tempdir().unwrap();
    let rejection = refused(
        &run_control(
            home,
            "retry",
            predecessor_id,
            &["--execution-root", execution_root.path().to_str().unwrap()],
        ),
        "um workflow run retry",
        "retry",
    );
    names_the_successor(&rejection);
    unchanged();

    let rejection = refused(
        &run_control(home, "resume", predecessor_id, &[]),
        "um workflow run resume",
        "continuation",
    );
    names_the_successor(&rejection);
    unchanged();

    let output = run_control(home, "status", predecessor_id, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let status = json(&output);
    assert!(
        super::workflow_status::status_schema().is_valid(&status),
        "{status}"
    );
    let ineligible = serde_json::json!({
        "eligible": false,
        "reason": "continuation_disposition_ineligible",
    });
    assert_eq!(status["retry"], ineligible, "{status}");
    assert_eq!(
        status["continuation"]["eligible"], ineligible["eligible"],
        "{status}"
    );
    assert_eq!(
        status["continuation"]["reason"], ineligible["reason"],
        "{status}"
    );
    unchanged();
}

// REQ-WFEXEC-63, REQ-LOCALRUN-195: the answers a person gave on the predecessor reach a
// successor step that maps `controls.answers`, since the copied control still resolves the
// imported attempt's wait it resolved on the predecessor.
#[test]
fn a_restarted_successors_design_receives_the_predecessors_answers() {
    let published = PublishedWaitTree::with_subject();
    let home = Some(&published.home);
    let bundle = &published.bundle;
    let (predecessor, predecessor_id) = published.waiting_for(&published.definition, "AUTO-1");
    answer_the_one_question(&predecessor);
    decide(bundle, "design", "planned");
    decide(bundle, "review", "approve");
    let terminal = completed(&resume(&predecessor));
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");
    let seen = bundle.execution_root().join("seen-answers");
    let answers = read_json(&seen);
    assert!(answers.is_object(), "{answers}");
    fs::remove_file(&seen).unwrap();

    let review = bundle.source_root().join("review.yaml");
    let changed = format!("{}# revision 2\n", fs::read_to_string(&review).unwrap());
    bundle.write_source("review.yaml", &changed);
    assert_eq!(published.republish(&[])["revision"]["n"], 2);
    let output = run_control(home, "restart", &predecessor_id, &["--at", "design"]);
    let terminal = completed(&output);
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");
    let successor = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
    assert_ne!(successor, predecessor);
    assert_eq!(read_json(&seen), answers);
    assert_eq!(control_count(&successor), 1);
}

// REQ-LOCALRUN-188, REQ-LOCALRUN-189, SC-LOCALRUN-13: after a restart, `list` names each
// run's lineage after its outcome and `show` prints the predecessor and successor and marks
// the attempts the successor imported.
#[test]
fn list_and_show_print_a_restarted_runs_lineage() {
    let restarted = RestartedSubject::new();
    let home = &restarted.published.home;
    let (predecessor, successor) = (&restarted.predecessor, &restarted.successor);
    let (predecessor_id, successor_id) = (&restarted.predecessor_id, &restarted.successor_id);
    let list = |json: bool| {
        let mut args = vec!["workflow".to_owned(), "run".to_owned(), "list".to_owned()];
        if json {
            args.push("--json".to_owned());
        }
        let output = isolated_command(&args)
            .env("UM_HOME", home.path())
            .env("USER", OPERATOR)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(0), "{output:?}");
        output
    };

    let listed = json(&list(true));
    let runs = listed["runs"].as_array().unwrap();
    let entry = |id: &str| {
        runs.iter()
            .find(|run| run["id"] == id)
            .unwrap_or_else(|| panic!("{id} in {listed}"))
    };
    let old = entry(predecessor_id);
    assert_eq!(old["successor"], successor_id.as_str(), "{old}");
    assert_eq!(old["predecessor"], serde_json::Value::Null, "{old}");
    let new = entry(successor_id);
    assert_eq!(new["predecessor"], predecessor_id.as_str(), "{new}");
    assert_eq!(new["successor"], serde_json::Value::Null, "{new}");
    let plain = String::from_utf8(list(false).stdout).unwrap();
    let line = |id: &str| {
        plain
            .lines()
            .find(|line| line.starts_with(id))
            .unwrap_or_else(|| panic!("{id} in {plain}"))
            .to_owned()
    };
    let old = line(predecessor_id);
    assert!(
        old.contains(&format!(" · superseded · succ {successor_id}")),
        "{old}"
    );
    assert!(!old.contains(" · pred "), "{old}");
    let new = line(successor_id);
    assert!(new.contains(&format!(" · pred {predecessor_id}")), "{new}");
    assert!(!new.contains(" · succ "), "{new}");

    let shown = json(&run_control(Some(home), "show", successor_id, &[]));
    assert_eq!(
        shown["record"]["predecessor"],
        predecessor_id.as_str(),
        "{shown}"
    );
    let attempts = shown["attempts"].as_array().unwrap();
    assert_eq!(
        attempts[0]["imported"],
        serde_json::json!({"run": predecessor_id})
    );
    assert!(attempts[1].get("imported").is_none(), "{shown}");
    assert_eq!(attempts[1]["reason"], "restart", "{shown}");
    let plain = shown_plain(successor);
    let lines: Vec<&str> = plain.lines().collect();
    assert!(
        lines.contains(&format!("predecessor: {predecessor_id}").as_str()),
        "{plain}"
    );
    assert!(lines.contains(&"successor: -"), "{plain}");
    let first = lines
        .iter()
        .find(|line| line.trim_start().starts_with("1 "))
        .unwrap_or_else(|| panic!("attempt 1 in {plain}"));
    assert!(
        first.ends_with(&format!("imported from {predecessor_id}")),
        "{plain}"
    );
    let second = lines
        .iter()
        .find(|line| line.trim_start().starts_with("2 "))
        .unwrap_or_else(|| panic!("attempt 2 in {plain}"));
    assert!(!second.contains("imported from"), "{plain}");
    let plain = shown_plain(predecessor);
    assert!(
        plain
            .lines()
            .any(|line| line == format!("successor: {successor_id}")),
        "{plain}"
    );
}

// REQ-LOCALRUN-194, REQ-LOCALRUN-80: a restart carries the predecessor's inputs onto the
// current revision with each `--input-text` on top, retained in the successor's own
// `workflow/`; a revision that declares a new text input is refused at phase `continuation`
// until `--input-text` supplies it, and an `--input-text` naming the workflow's `subject` is a
// usage error, each leaving both the run and the home as they were.
#[test]
fn a_restart_sets_text_inputs_of_the_current_revision() {
    let published = PublishedWaitTree::with_subject();
    let home = Some(&published.home);
    let (predecessor, predecessor_id) = published.waiting_for(&published.definition, "AUTO-1");
    published.bundle.write_source(
        "workflow.yaml",
        &WAIT_TREE_ROOT.replacen(
            "schemaVersion: 1\n",
            "schemaVersion: 1\nname: waits\nsubject: ticket\ninputs:\n  ticket:\n    kind: text\n  note:\n    kind: text\n",
            1,
        ),
    );
    assert_eq!(published.republish(&[])["revision"]["n"], 2);
    decide(&published.bundle, "review", "approve");
    let runs = predecessor.parent().unwrap();
    let before = records(&predecessor);
    let unchanged = || {
        assert_eq!(records(&predecessor), before);
        assert_eq!(run_directories(runs), 1);
        assert_eq!(run_record(&predecessor)["state"], "waiting");
    };

    // Revision 2's `note` has no value in the predecessor's retained inputs.
    let refusal = restart_refused(
        &run_control(home, "restart", &predecessor_id, &["--at", "review"]),
        "continuation",
        "continuation_inputs_unsatisfied",
    );
    assert_eq!(refusal["diagnostics"][0]["location"]["input"], "note");
    unchanged();
    let subject_binding = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "restart".to_owned(),
        predecessor_id.clone(),
        "--at".to_owned(),
        "review".to_owned(),
        "--input-text".to_owned(),
        "ticket".to_owned(),
        "AUTO-2".to_owned(),
    ])
    .env("UM_HOME", published.home.path())
    .env("USER", OPERATOR)
    .output()
    .unwrap();
    assert_eq!(
        subject_binding.status.code(),
        Some(2),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&subject_binding.stdout),
        String::from_utf8_lossy(&subject_binding.stderr)
    );
    unchanged();

    let terminal = completed(&run_control(
        home,
        "restart",
        &predecessor_id,
        &["--at", "review", "--input-text", "note", "hello"],
    ));
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");
    let successor = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
    assert_eq!(run_record(&successor)["subject"], "AUTO-1");
    let manifest = read_json(&successor.join("workflow").join("manifest.json"));
    let retained = |name: &str| {
        let input = &manifest["inputs"][name];
        assert_eq!(input["kind"], "text", "{manifest}");
        fs::read_to_string(
            successor
                .join("workflow")
                .join(input["relativeFile"].as_str().unwrap()),
        )
        .unwrap()
    };
    assert_eq!(retained("note"), "hello");
    assert_eq!(retained("ticket"), "AUTO-1");
}

// REQ-LOCALRUN-197, REQ-LOCALRUN-196, REQ-CONTINUE-80, SC-LOCALRUN-14: an upgrade moves a
// subject's run waiting on `approval` to the current revision with its wait, position, edge
// counts and attempts carried and no attempt started; the approval recorded on the successor
// resumes it from `merge` under revision 2 with `design` and `review` inherited.
#[test]
fn an_upgrade_moves_a_waiting_subject_with_its_wait() {
    let published = PublishedWaitTree::with_subject_retrying_design();
    let home = Some(&published.home);
    let bundle = &published.bundle;
    decide_each(bundle, "design", &["failed", "planned"]);
    decide(bundle, "review", "approve");
    let documents = chained(&published.create(&["--input-text", "ticket", "AUTO-1"]));
    let outcomes = documents
        .iter()
        .map(|document| document["outcome"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes, ["continued", "waiting"]);
    let waiting = &documents[1];
    assert_eq!(waiting["wait"]["kind"], "approval", "{waiting}");
    let predecessor = PathBuf::from(waiting["runDirectory"].as_str().unwrap());
    let record = run_record(&predecessor);
    let predecessor_id = record["id"].as_str().unwrap().to_owned();
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(
        record["position"],
        serde_json::json!(["review"]),
        "{record}"
    );
    let edges = serde_json::json!({"design.failed": 1});
    assert_eq!(record["edges"], edges, "{record}");
    let attempts = record["attempts"].clone();
    let wait = fs::read(predecessor.join("wait.json")).unwrap();
    let runs = predecessor.parent().unwrap();
    assert_eq!(run_directories(runs), 1);

    let merge = bundle.source_root().join("merge.yaml");
    let changed = format!("{}# revision 2\n", fs::read_to_string(&merge).unwrap());
    bundle.write_source("merge.yaml", &changed);
    assert_eq!(published.republish(&[])["revision"]["n"], 2);
    let document = completed(&run_control(home, "upgrade", &predecessor_id, &[]));
    assert_eq!(document["command"], "um workflow run upgrade", "{document}");
    assert_eq!(document["outcome"], "upgraded", "{document}");
    assert_eq!(document["exitStatus"], 0);
    assert_eq!(document["predecessor"], predecessor_id.as_str());
    assert_eq!(
        document["wait"],
        serde_json::from_slice::<serde_json::Value>(&wait).unwrap()
    );
    let successor = PathBuf::from(document["runDirectory"].as_str().unwrap());
    assert_eq!(successor.parent(), Some(runs));
    let record = run_record(&successor);
    let successor_id = record["id"].as_str().unwrap().to_owned();
    assert_eq!(document["run"], successor_id.as_str());
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["revision"], 2, "{record}");
    assert_eq!(
        record["position"],
        serde_json::json!(["review"]),
        "{record}"
    );
    assert_eq!(record["edges"], edges, "{record}");
    assert_eq!(record["attempts"], attempts, "{record}");
    assert_eq!(record["predecessor"], predecessor_id.as_str(), "{record}");
    assert_eq!(record["subject"], "AUTO-1", "{record}");
    assert_eq!(fs::read(successor.join("wait.json")).unwrap(), wait);
    assert_eq!(
        attempt_count(&successor) as u64,
        attempts.as_u64().unwrap(),
        "the upgrade started no attempt"
    );
    let record = run_record(&predecessor);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "superseded", "{record}");
    assert_eq!(record["successor"], successor_id.as_str(), "{record}");
    assert!(!predecessor.join("wait.json").exists());
    assert_eq!(run_directories(runs), 2);
    let shown = json(&run_control(home, "show", "AUTO-1", &[]));
    assert_eq!(
        shown["runDirectory"],
        successor.to_str().unwrap(),
        "{shown}"
    );
    assert!(!predecessor.join("mirror").exists());
    assert!(!successor.join("mirror").exists());

    recorded(
        &run_control(home, "approve", &successor_id, &[]),
        "approve",
        &successor,
    );
    decide(bundle, "merge", "landed");
    let output = resume(&successor);
    let terminal = completed(&output);
    assert_eq!(terminal["command"], "um workflow run resume", "{terminal}");
    let continuation = &partition_event(&output)["continuation"];
    assert_eq!(continuation["reason"], "resume", "{continuation}");
    assert_eq!(continuation["fromSteps"], serde_json::json!(["merge"]));
    let source = &continuation["definitionSource"];
    assert_eq!(source["kind"], "replaced", "{continuation}");
    assert_ne!(source["priorManifestDigest"], source["manifestDigest"]);
    let inherited = continuation["inheritedSteps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|step| step["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(
        inherited.contains(&"design") && inherited.contains(&"review"),
        "{continuation}"
    );
    let record = run_record(&successor);
    assert_eq!(record["outcome"], "landed", "{record}");
    assert_eq!(record["revision"], 2, "{record}");
}

// REQ-LOCALRUN-197, REQ-LOCALRUN-80: an upgrade of a run waiting on `approval` is refused
// with every carried piece the current revision does not resolve, on a run already on the
// current revision and on a wait a recorded control resolves, each refusal leaving the run
// and the home as they were; an execution option is a usage error.
#[test]
fn an_upgrade_is_refused_with_every_miss_and_changes_nothing() {
    let published = PublishedWaitTree::new();
    let home = Some(&published.home);
    let bundle = &published.bundle;
    decide(bundle, "review", "approve");
    let (run, id, terminal) = published.run("planned");
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");
    let runs = run.parent().unwrap();
    let before = records(&run);
    let unchanged = || {
        assert_eq!(records(&run), before);
        assert_eq!(run_directories(runs), 1);
        assert_eq!(run_record(&run)["state"], "waiting");
    };
    let upgrade = || run_control(home, "upgrade", &id, &[]);

    let refusal = upgrade_refused(&upgrade(), "successor", &["revision_current"]);
    assert_eq!(refusal["diagnostics"][0]["run"], id.as_str(), "{refusal}");
    unchanged();

    // Revision 2 renames `merge`, the approval's `then`, and `design`'s outcome `planned`,
    // which `design` retained.
    let root = wait_tree_root(WAIT_TREE_HEADER)
        .replacen("      planned: review\n", "      ready: review\n", 1)
        .replace("  merge:\n", "  land:\n")
        .replace(": merge\n", ": land\n");
    bundle.write_source("workflow.yaml", &root);
    let design = fs::read_to_string(bundle.source_root().join("design.yaml")).unwrap();
    bundle.write_source(
        "design.yaml",
        &design.replacen("[planned, blocked, failed]", "[ready, blocked, failed]", 1),
    );
    assert_eq!(published.republish(&[])["revision"]["n"], 2);
    let refusal = upgrade_refused(
        &upgrade(),
        "successor",
        &["wait_target_unresolved", "outcome_unrouted"],
    );
    for diagnostic in refusal["diagnostics"].as_array().unwrap() {
        assert_eq!(diagnostic["run"], id.as_str(), "{refusal}");
    }
    assert_eq!(refusal["runDirectory"], run.to_str().unwrap(), "{refusal}");
    unchanged();

    for option in [
        &["--input-text", "note", "x"][..],
        &["--execution-root", "."][..],
    ] {
        let args = ["workflow", "run", "upgrade", run.to_str().unwrap()]
            .iter()
            .chain(option)
            .map(|arg| (*arg).to_owned())
            .collect::<Vec<_>>();
        assert_usage_error(&args);
    }
    unchanged();

    recorded(&run_control(home, "approve", &id, &[]), "approve", &run);
    let refusal = upgrade_refused(&upgrade(), "successor", &["wait_resolved"]);
    assert_eq!(refusal["diagnostics"][0]["run"], id.as_str(), "{refusal}");
    unchanged();
}

// REQ-LOCALRUN-197, REQ-LOCALRUN-195: an approval recorded on the predecessor after the
// upgrade inspected it and before it took the lock refuses the upgrade `wait_resolved` under
// the lock, instead of being copied to a successor it would not resolve; the predecessor is
// unchanged, and resuming it takes the approval.
#[test]
fn an_upgrade_refuses_a_wait_resolved_between_its_inspection_and_the_lock() {
    let published = PublishedWaitTree::new();
    let home = Some(&published.home);
    let bundle = &published.bundle;
    decide(bundle, "review", "approve");
    let (run, id, terminal) = published.run("planned");
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");
    let runs = run.parent().unwrap();
    let merge = bundle.source_root().join("merge.yaml");
    let changed = format!("{}# revision 2\n", fs::read_to_string(&merge).unwrap());
    bundle.write_source("merge.yaml", &changed);
    assert_eq!(published.republish(&[])["revision"]["n"], 2);
    let before = records(&run);

    let gate = bundle.result("upgrade-lock-gate");
    fs::create_dir_all(gate.parent().unwrap()).unwrap();
    let upgrade = control_command(home, "upgrade", &id, &[])
        .env("UM_TEST_RUN_LOCK_GATE", &gate)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    poll_until(
        "the upgrade's inspection",
        || gate.with_extension("inspected").exists(),
        |inspected| *inspected,
    );
    recorded(&run_control(home, "approve", &id, &[]), "approve", &run);
    let controls = || fs::read_dir(run.join("controls")).unwrap().count();
    assert_eq!(controls(), 1);
    fs::write(&gate, "").unwrap();
    let output = upgrade.wait_with_output().unwrap();

    let refusal = upgrade_refused(&output, "successor", &["wait_resolved"]);
    assert_eq!(refusal["diagnostics"][0]["run"], id.as_str(), "{refusal}");
    assert_eq!(refusal["runDirectory"], run.to_str().unwrap(), "{refusal}");
    assert_eq!(records(&run), before);
    assert_eq!(controls(), 1);
    assert_eq!(run_directories(runs), 1);
    assert_eq!(run_record(&run)["state"], "waiting");
    decide(bundle, "merge", "landed");
    let resumed = completed(&resume(&run));
    assert_eq!(resumed["outcome"], "succeeded", "{resumed}");
    let record = run_record(&run);
    assert_eq!(record["outcome"], "landed", "{record}");
    assert_eq!(record["revision"], 1, "{record}");
}

// REQ-LOCALRUN-197, REQ-CONTINUE-80, REQ-WFEXEC-68, REQ-LOCALRUN-195, REQ-LOCALRUN-177: an
// upgrade of a fan-out waiting at element 2 carries that position, and the answered successor
// resumes at element 2 under the current revision: element 1 does not run again, element 2
// and element 3 run as the successor's own children, and the predecessor's children stay as
// they were. A revision whose `build` no longer fans out is refused with the position and the
// items it cannot resolve.
#[test]
fn an_upgraded_fan_out_resumes_at_its_element() {
    let published = PublishedWaitTree::fan_out();
    let home = Some(&published.home);
    let bundle = &published.bundle;
    let root = bundle.execution_root();
    let element = serde_json::json!(["build", {"item": 2}]);
    let waiting_at_element = || {
        let terminal = completed(&published.create(&[]));
        assert_eq!(terminal["outcome"], "waiting", "{terminal}");
        let run = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
        let wait = read_json(&run.join("wait.json"));
        assert_eq!(wait["kind"], "answer", "{wait}");
        assert_eq!(wait["step"], element, "{wait}");
        let record = run_record(&run);
        assert_eq!(record["position"], element, "{record}");
        assert_eq!(record["subject"], serde_json::Value::Null, "{record}");
        let id = record["id"].as_str().unwrap().to_owned();
        (run, id)
    };
    let (predecessor, predecessor_id) = waiting_at_element();
    let runs = predecessor.parent().unwrap().to_owned();
    let elements = |run: &Path| ordinals_of(&run.join("children/build"));
    let ordinals =
        |run: &Path, element: &str| ordinals_of(&run.join("children/build").join(element));
    assert_eq!(elements(&predecessor), ["000001", "000002"]);
    let wait = fs::read(predecessor.join("wait.json")).unwrap();

    let merge = bundle.source_root().join("merge.yaml");
    let changed = format!("{}# revision 2\n", fs::read_to_string(&merge).unwrap());
    bundle.write_source("merge.yaml", &changed);
    assert_eq!(published.republish(&[])["revision"]["n"], 2);
    let document = completed(&run_control(home, "upgrade", &predecessor_id, &[]));
    assert_eq!(document["outcome"], "upgraded", "{document}");
    assert_eq!(document["predecessor"], predecessor_id.as_str());
    let successor = PathBuf::from(document["runDirectory"].as_str().unwrap());
    let record = run_record(&successor);
    let successor_id = record["id"].as_str().unwrap().to_owned();
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["revision"], 2, "{record}");
    assert_eq!(record["position"], element, "{record}");
    assert_eq!(fs::read(successor.join("wait.json")).unwrap(), wait);
    assert!(!successor.join("children").exists() || elements(&successor).is_empty());
    assert_eq!(run_record(&predecessor)["outcome"], "superseded");

    let output = run_control(
        home,
        "answer",
        &successor_id,
        &["--question", "1", "--text", "split"],
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    fs::write(root.join("verdict-build-b"), "built").unwrap();
    let output = resume(&successor);
    let terminal = completed(&output);
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    let continuation = &partition_event(&output)["continuation"];
    assert_eq!(continuation["reason"], "resume", "{continuation}");
    assert_eq!(continuation["fromSteps"], serde_json::json!(["build"]));
    let record = run_record(&successor);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "landed", "{record}");
    assert_eq!(record["revision"], 2, "{record}");
    // A successor's children start empty: element 1 did not run again, element 2 ran with the
    // answers and element 3 after it, while the predecessor keeps the children it ran.
    assert_eq!(elements(&successor), ["000002", "000003"]);
    assert_eq!(ordinals(&successor, "000002"), ["000001"]);
    assert_eq!(ordinals(&successor, "000003"), ["000001"]);
    assert_eq!(elements(&predecessor), ["000001", "000002"]);
    assert_eq!(ordinals(&predecessor, "000001"), ["000001"]);
    assert_eq!(ordinals(&predecessor, "000002"), ["000001"]);
    let answers = serde_json::json!({"1": "split"});
    assert_eq!(read_json(&root.join("seen-answers-b")), answers);
    assert_eq!(read_json(&root.join("seen-answers-c")), answers);
    assert!(!root.join("seen-answers-a").exists());

    // Revision 3's `build` is a plain step, so neither element 2 nor its items resolve; the
    // child's `part`, no longer mapped, becomes optional.
    fs::write(root.join("verdict-build-b"), "blocked").unwrap();
    let (second, second_id) = waiting_at_element();
    let plain = fan_out_root(FAN_OUT_TREE_ROOT);
    assert!(plain.contains(FAN_OUT_FOR_EACH) && plain.contains(FAN_OUT_PART_INPUT));
    bundle.write_source(
        "workflow.yaml",
        &plain
            .replacen(FAN_OUT_FOR_EACH, "", 1)
            .replacen(FAN_OUT_PART_INPUT, "", 1),
    );
    let build = fs::read_to_string(bundle.source_root().join("build.yaml")).unwrap();
    let part = "  part:\n    kind: text\n";
    assert!(build.contains(part), "{build}");
    bundle.write_source(
        "build.yaml",
        &build.replacen(part, "  part:\n    kind: text\n    optional: true\n", 1),
    );
    assert_eq!(published.republish(&[])["revision"]["n"], 3);
    let before = records(&second);
    let directories = run_directories(&runs);
    let refusal = upgrade_refused(
        &run_control(home, "upgrade", &second_id, &[]),
        "successor",
        &["position_unresolved", "items_unresolved"],
    );
    for diagnostic in refusal["diagnostics"].as_array().unwrap() {
        assert_eq!(diagnostic["run"], second_id.as_str(), "{refusal}");
    }
    assert_eq!(records(&second), before);
    assert_eq!(run_directories(&runs), directories);
    assert_eq!(run_record(&second)["state"], "waiting");
}

// REQ-LOCALRUN-199, REQ-LOCALRUN-194, REQ-WFEXEC-68: a restart at the fan-out a run waits
// inside at element 2 runs the step again from its first element and advises `upgrade` on
// standard error, its `--json` document unchanged on standard output; a restart at another
// step, a refused restart and a restart of a run waiting at the fan-out's first element write
// no advisory, and the refusal leaves the run waiting.
#[test]
fn a_restart_at_a_fan_out_waiting_past_its_first_element_advises_upgrade() {
    let published = PublishedWaitTree::fan_out();
    let home = Some(&published.home);
    let waits_at_element = |run: &Path, item: u64| {
        assert_eq!(run_record(run)["state"], "waiting");
        let wait = read_json(&run.join("wait.json"));
        assert_eq!(wait["kind"], "answer", "{wait}");
        assert_eq!(
            wait["step"],
            serde_json::json!(["build", {"item": item}]),
            "{wait}"
        );
    };
    let restarted_at = |output: &Output, item: u64| {
        let terminal = completed(output);
        assert_eq!(terminal["command"], "um workflow run restart", "{terminal}");
        assert_eq!(terminal["outcome"], "waiting", "{terminal}");
        let successor = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
        waits_at_element(&successor, item);
        (successor, String::from_utf8(output.stderr.clone()).unwrap())
    };
    let restarted = |output: &Output| restarted_at(output, 2);
    let advises = |stderr: &str| stderr.contains("um workflow run upgrade");
    let terminal = completed(&published.create(&[]));
    let predecessor = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
    waits_at_element(&predecessor, 2);
    let predecessor_id = run_record(&predecessor)["id"].as_str().unwrap().to_owned();

    let (successor, stderr) = restarted(&run_control(
        home,
        "restart",
        &predecessor_id,
        &["--at", "build"],
    ));
    assert_eq!(
        ordinals_of(&successor.join("children/build")),
        ["000001", "000002"]
    );
    let advisory = stderr
        .lines()
        .find(|line| advises(line))
        .unwrap_or_else(|| panic!("{stderr}"));
    assert!(advisory.contains("build"), "{advisory}");
    assert!(advisory.contains('2'), "{advisory}");
    assert_eq!(stderr.lines().filter(|line| advises(line)).count(), 1);

    let successor_id = run_record(&successor)["id"].as_str().unwrap().to_owned();
    let before = records(&successor);
    let refused = run_control(home, "restart", &successor_id, &["--at", "nosuch"]);
    restart_refused(&refused, "continuation", "continuation_definition_invalid");
    assert!(!advises(&String::from_utf8_lossy(&refused.stderr)));
    assert_eq!(records(&successor), before);
    waits_at_element(&successor, 2);

    let (third, stderr) = restarted(&run_control(
        home,
        "restart",
        &successor_id,
        &["--at", "design"],
    ));
    assert!(!advises(&stderr), "{stderr}");

    // A run waiting at the first element is where the restart starts anyway: once the first
    // part blocks, the restart that leaves element 2 advises and the one that leaves element 1
    // does not.
    let blocked_first = published.bundle.execution_root().join("verdict-build-a");
    fs::write(blocked_first, "blocked").unwrap();
    let restart_at_build = |run: &Path| {
        let id = run_record(run)["id"].as_str().unwrap().to_owned();
        restarted_at(&run_control(home, "restart", &id, &["--at", "build"]), 1)
    };
    let (fourth, stderr) = restart_at_build(&third);
    assert!(advises(&stderr), "{stderr}");
    let (_, stderr) = restart_at_build(&fourth);
    assert!(!advises(&stderr), "{stderr}");
}

/// A root whose `design` stage child plans and then routes, and whose physically failed
/// child waits for a repair at `design` itself (SC-CONTINUE-10). `design` maps the resolving
/// control's `notes`, and the root exports the child's `plan`.
const STAGE_TREE_ROOT: &str = "schemaVersion: 1
programs: [bin]
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    inputs:
      notes:
        ref: wait.notes
    on:
      planned: exit.landed
      failed:
        wait: repair
        then: design
exports:
  plan:
    ref: outputs.design.plan
";

/// The stage tree's root fanning `design` out over its `parts` input, one element per part,
/// each continuing to `merge` once it plans.
const STAGE_FAN_OUT_ROOT: &str = "schemaVersion: 1
programs: [bin]
inputs:
  parts:
    kind: json
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    forEach: inputs.parts
    inputs:
      part:
        ref: item.name
    on:
      planned: merge
      failed:
        wait: repair
        then: design
  merge:
    kind: workflow
    workflow: merge.yaml
    on:
      landed: exit.landed
      failed: exit.failed
";

/// The stage tree's root whose failed `design` waits for a repair that continues at an
/// earlier `setup` stage instead.
const STAGE_SETUP_ROOT: &str = "schemaVersion: 1
programs: [bin]
outcomes: [landed, failed]
steps:
  setup:
    kind: workflow
    workflow: merge.yaml
    on:
      landed: design
      failed: exit.failed
  design:
    kind: workflow
    workflow: design.yaml
    on:
      planned: exit.landed
      failed:
        wait: repair
        then: setup
";

/// The stage child: `read` and `plan` produce text outputs, `route` reads the plan and
/// succeeds only once the workspace's plan file reads `fenced`, `effect` runs after it, and
/// the `close` finalizer fails while `close-fails` exists. `findings` is an optional
/// object-schema JSON input no root maps, and `unused` a profile no step uses.
const STAGE_TREE_DESIGN: &str = "schemaVersion: 1
agentProfiles:
  unused:
    harness:
      kind: pi
      config:
        model: fixture/model
        thinking: low
inputs:
  part:
    kind: text
    optional: true
  notes:
    kind: text
    optional: true
  findings:
    kind: json
    optional: true
    schema: findings.schema.json
outcomes: [planned, failed]
outcome:
  from: outputs.route.verdict
  onFailure: failed
steps:
  read:
    kind: cmd
    command:
      argv: [stage, read]
    outputs:
      text:
        kind: text
        from: path
        path: read.txt
  plan:
    kind: cmd
    inputs:
      read:
        ref: outputs.read.text
    command:
      argv: [stage, plan]
    outputs:
      text:
        kind: text
        from: path
        path: plan.txt
  route:
    kind: cmd
    inputs:
      plan:
        ref: outputs.plan.text
      part:
        ref: inputs.part
      findings:
        ref: inputs.findings
    command:
      argv: [stage, route]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict.txt
  effect:
    kind: cmd
    dependsOn: [route]
    inputs:
      part:
        ref: inputs.part
    command:
      argv: [stage, effect]
finalizers:
  close:
    kind: cmd
    command:
      argv: [stage, close]
exports:
  plan:
    ref: outputs.plan.text
";

const STAGE_TREE_MERGE: &str = "schemaVersion: 1
outcomes: [landed, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    command:
      argv: [sh, -c, printf landed > verdict-merge.txt]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict-merge.txt
";

/// `stage STEP` counts its runs in `count-<STEP>`; `plan` and `route` record the value they
/// received as `seen-read[-<part>]` and `seen-plan[-<part>]`; `route` requires the omitted
/// `findings` to read `null` and `plan[-<part>].md` to read `fenced`; `effect` appends one
/// line to `effects`.
const STAGE_TREE_PROGRAM: &str = "#!/bin/sh
set -eu
count=$(($(cat \"count-$1\" 2>/dev/null || printf 0) + 1))
printf '%s' \"$count\" > \"count-$1\"
part=$(cat \"${UM_STEP_INPUTS:-/nonexistent}/values/part\" 2>/dev/null || true)
suffix=${part:+-$part}
case \"$1\" in
  read) printf 'read-%s' \"$count\" > read.txt ;;
  plan)
    cat \"$UM_STEP_INPUTS/values/read\" > \"seen-read$suffix\"
    printf 'plan-%s' \"$count\" > plan.txt ;;
  route)
    cat \"$UM_STEP_INPUTS/values/plan\" > \"seen-plan$suffix\"
    test \"$(cat \"$UM_STEP_INPUTS/values/findings\")\" = null
    grep -qx fenced \"plan$suffix.md\"
    printf planned > verdict.txt ;;
  effect) printf 'effect%s\\n' \"$suffix\" >> effects ;;
  close) test ! -e close-fails ;;
esac
";

fn stage_tree(root: &str) -> TreeBundle {
    let bundle = TreeBundle::new(root);
    bundle.write_source("design.yaml", STAGE_TREE_DESIGN);
    bundle.write_source("merge.yaml", STAGE_TREE_MERGE);
    bundle.write_source(
        "findings.schema.json",
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
    );
    bundle.write_program("bin/stage", STAGE_TREE_PROGRAM);
    bundle
}

/// How often each of `read`, `plan` and `route` ran in `root`.
fn stage_counts(root: &Path) -> [u32; 3] {
    ["read", "plan", "route"].map(|step| {
        fs::read_to_string(root.join(format!("count-{step}")))
            .map_or(0, |count| count.parse().unwrap())
    })
}

/// Creates the stage tree's run with `args`, which waits for a repair: its directory and wait.
fn repair_waiting_stage(
    bundle: &TreeBundle,
    name: &str,
    args: &[&str],
) -> (PathBuf, serde_json::Value) {
    let destination = bundle.result(name);
    let terminal = completed(
        &isolated_command(&bundle.args(&destination))
            .args(args)
            .output()
            .unwrap(),
    );
    assert_eq!(terminal["outcome"], "waiting", "{terminal}");
    assert_eq!(terminal["wait"]["kind"], "repair", "{terminal}");
    (
        fs::canonicalize(&destination).unwrap(),
        terminal["wait"].clone(),
    )
}

/// The last document a chaining `retry` or `resume` wrote; it exited 0.
fn last_document(output: &Output) -> serde_json::Value {
    chained(output).pop().unwrap()
}

fn attempt_result(run: &Path, attempt: u64) -> serde_json::Value {
    read_json(&run.join(format!("attempts/{attempt:06}/result/result.json")))
}

/// Each step's id and state in a published result.
fn step_states(result: &serde_json::Value) -> Vec<(String, String)> {
    result["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|step| {
            (
                step["id"].as_str().unwrap().to_owned(),
                step["state"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn states(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(id, state)| ((*id).to_owned(), (*state).to_owned()))
        .collect()
}

/// Every file under `directory`, by relative path, with its bytes.
fn file_tree(directory: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    let mut files = std::collections::BTreeMap::new();
    let mut pending = vec![directory.to_owned()];
    while let Some(next) = pending.pop() {
        for entry in fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if fs::symlink_metadata(&path).unwrap().is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name != "run.lock") {
                let relative = path.strip_prefix(directory).unwrap().to_owned();
                files.insert(relative, fs::read(&path).unwrap_or_default());
            }
        }
    }
    files
}

// SC-CONTINUE-10, REQ-CONTINUE-81, REQ-LOCALRUN-184, REQ-LOCALRUN-177, REQ-CONTINUE-9,
// REQ-CONTINUE-15, REQ-CONTINUE-16, INV-ENGINE-27: a repair retry continues the physically
// failed child in place: the same child's next attempts inherit `read` and `plan` from
// attempt 1, re-execute `route` and `effect` with reason `retry`, and the parent lands with
// the child's export; the profile replacement of the last retry resolves the child's
// re-executed attempt.
#[test]
fn a_repair_retry_continues_the_failed_child_without_repeating_its_completed_steps() {
    let bundle = stage_tree(STAGE_TREE_ROOT);
    let root = bundle.execution_root();
    fs::write(root.join("plan.md"), "unfenced").unwrap();
    let (run, wait) = repair_waiting_stage(&bundle, "repaired", &[]);
    assert_eq!(wait["payload"]["child"], "children/design/000001", "{wait}");
    let child = run.join("children/design/000001");
    let child_id = run_record(&child)["id"].clone();
    let first_attempt = file_tree(&child.join("attempts/000001"));
    assert_eq!(stage_counts(root), [1, 1, 1]);

    // Unrepaired, the retry continues the same child, whose route fails again.
    let waiting = last_document(&run_control(None, "retry", &run, &[]));
    assert_eq!(waiting["outcome"], "waiting", "{waiting}");
    assert_eq!(
        waiting["wait"]["payload"]["child"],
        "children/design/000001"
    );
    assert_eq!(stage_counts(root), [1, 1, 2]);
    assert_eq!(ordinals_of(&run.join("children/design")), ["000001"]);
    assert_eq!(attempt_count(&child), 2);

    // The operator repairs only the workspace; the workspace copy of `plan`'s file changes
    // too, but its retained output is what `route` reads.
    fs::write(root.join("plan.md"), "fenced").unwrap();
    fs::write(root.join("plan.txt"), "replaced").unwrap();
    let replacement = bundle.result("replacement.json");
    fs::create_dir_all(replacement.parent().unwrap()).unwrap();
    fs::write(
        &replacement,
        serde_json::to_vec(&serde_json::json!({
            "schemaVersion": 1,
            "agentProfiles": {"unused": {"harness": {
                "kind": "claude_code",
                "config": {"model": "fixture/claude", "effort": "low"}
            }}}
        }))
        .unwrap(),
    )
    .unwrap();
    let output = run_control(
        None,
        "retry",
        &run,
        &["--agent-profiles", replacement.to_str().unwrap()],
    );
    let terminal = last_document(&output);
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(terminal["result"]["continuation"]["reason"], "retry");
    let export = Path::new(terminal["resultDirectory"].as_str().unwrap()).join(
        terminal["result"]["exports"]["plan"]["path"]
            .as_str()
            .unwrap(),
    );
    assert_eq!(fs::read_to_string(export).unwrap(), "plan-1", "{terminal}");
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(attempt_count(&run), 3);

    assert_eq!(ordinals_of(&run.join("children/design")), ["000001"]);
    assert_eq!(run_record(&child)["id"], child_id);
    assert_eq!(run_record(&child)["outcome"], "planned");
    assert_eq!(attempt_count(&child), 3);
    for attempt in [2, 3] {
        let result = attempt_result(&child, attempt);
        assert_eq!(result["continuation"]["reason"], "retry", "{result}");
        assert_eq!(
            result["continuation"]["reexecutedSteps"],
            serde_json::json!(["route", "effect"]),
            "{result}"
        );
    }
    let last = attempt_result(&child, 3);
    assert_eq!(last["outcome"], "succeeded", "{last}");
    assert_eq!(
        step_states(&last),
        states(&[
            ("read", "inherited"),
            ("plan", "inherited"),
            ("route", "succeeded"),
            ("effect", "succeeded"),
        ])
    );
    assert_eq!(
        last["workflow"]["agentProfileReplacement"]["profiles"]["unused"]["harness"]["kind"],
        "claude_code",
        "{last}"
    );
    assert_eq!(stage_counts(root), [1, 1, 3]);
    assert_eq!(
        fs::read_to_string(root.join("seen-read")).unwrap(),
        "read-1"
    );
    assert_eq!(
        fs::read_to_string(root.join("seen-plan")).unwrap(),
        "plan-1"
    );
    assert_eq!(
        fs::read_to_string(root.join("effects")).unwrap(),
        "effect\n"
    );
    // The child's first attempt and its retained outputs are unchanged.
    assert_eq!(file_tree(&child.join("attempts/000001")), first_attempt);
}

// SC-CONTINUE-10, REQ-CONTINUE-81, REQ-WFEXEC-68: at a fan-out's second element the retry
// continues that element's child; the first element is not run again and the third runs once,
// after the repaired second plans.
#[test]
fn a_repair_retry_continues_the_failed_fan_out_element_before_the_next() {
    let bundle = stage_tree(STAGE_FAN_OUT_ROOT);
    let root = bundle.execution_root();
    for (part, plan) in [("a", "fenced"), ("b", "unfenced"), ("c", "fenced")] {
        fs::write(root.join(format!("plan-{part}.md")), plan).unwrap();
    }
    let (run, wait) = repair_waiting_stage(
        &bundle,
        "fan-out",
        &[
            "--input-json",
            "parts",
            r#"[{"name":"a"},{"name":"b"},{"name":"c"}]"#,
        ],
    );
    assert_eq!(wait["step"], serde_json::json!(["design", {"item": 2}]));
    assert_eq!(wait["payload"]["child"], "children/design/000002/000001");
    assert!(!run.join("children/design/000003").exists());
    assert_eq!(stage_counts(root), [2, 2, 2]);

    fs::write(root.join("plan-b.md"), "fenced").unwrap();
    let terminal = last_document(&run_control(None, "retry", &run, &[]));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(run_record(&run)["outcome"], "landed");
    let element = |n: &str| run.join("children/design").join(n);
    for n in ["000001", "000002", "000003"] {
        assert_eq!(ordinals_of(&element(n)), ["000001"], "element {n}");
    }
    assert_eq!(attempt_count(&element("000001").join("000001")), 1);
    assert_eq!(attempt_count(&element("000002").join("000001")), 2);
    assert_eq!(attempt_count(&element("000003").join("000001")), 1);
    assert_eq!(stage_counts(root), [3, 3, 4]);
    assert_eq!(
        fs::read_to_string(root.join("seen-plan-b")).unwrap(),
        "plan-2"
    );
    assert_eq!(
        fs::read_to_string(root.join("effects")).unwrap(),
        "effect-a\neffect-b\neffect-c\n"
    );
}

// REQ-LOCALRUN-200, REQ-WFEXEC-66: each fan-out element's child steps reach the parent's
// progress stream scoped by the element, the first element's before the second's.
#[test]
fn a_fan_out_childs_step_states_are_scoped_by_their_element() {
    let bundle = stage_tree(STAGE_FAN_OUT_ROOT);
    for part in ["a", "b"] {
        fs::write(
            bundle.execution_root().join(format!("plan-{part}.md")),
            "fenced",
        )
        .unwrap();
    }
    let destination = bundle.result("scoped");
    let output = isolated_command(&bundle.args(&destination))
        .args(["--input-json", "parts", r#"[{"name":"a"},{"name":"b"}]"#])
        .output()
        .unwrap();
    assert_eq!(completed(&output)["outcome"], "succeeded");
    let records = progress_records(&output.stderr);
    assert!(
        progress_position(&records, "design[1].read", "start")
            < progress_position(&records, "design[2].read", "start")
    );
}

/// What a refused repair continuation must leave as it was: the parent's records, its child
/// directories, the child's attempts, the counters and the workspace's files.
struct StageSnapshot {
    attempts: usize,
    children: Vec<String>,
    child: std::collections::BTreeMap<PathBuf, Vec<u8>>,
    workspace: std::collections::BTreeMap<PathBuf, Vec<u8>>,
}

impl StageSnapshot {
    fn take(run: &Path, root: &Path) -> Self {
        Self {
            attempts: attempt_count(run),
            children: ordinals_of(&run.join("children/design")),
            child: file_tree(&run.join("children/design/000001/attempts")),
            workspace: file_tree(root),
        }
    }

    fn assert_unchanged(&self, run: &Path, root: &Path, case: &str) {
        assert_eq!(attempt_count(run), self.attempts, "{case}");
        assert_eq!(
            ordinals_of(&run.join("children/design")),
            self.children,
            "{case}"
        );
        assert_eq!(
            file_tree(&run.join("children/design/000001/attempts")),
            self.child,
            "{case}"
        );
        assert_eq!(file_tree(root), self.workspace, "{case}");
        assert_eq!(run_record(run)["state"], "waiting", "{case}");
        assert!(run.join("wait.json").exists(), "{case}");
    }
}

/// Rewrites the JSON file at `path` with `change`, making it writable first.
fn rewrite_json(path: &Path, change: impl FnOnce(&mut serde_json::Value)) {
    let mut document = read_json(path);
    change(&mut document);
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
}

/// The retained carrier of the child's first `plan` output, the one value of its first
/// attempt that holds exactly `plan-1`.
fn plan_carrier(child: &Path) -> PathBuf {
    let attempt = child.join("attempts/000001");
    let carriers = file_tree(&attempt)
        .into_iter()
        .filter(|(path, bytes)| path.starts_with("values") && bytes == b"plan-1")
        .map(|(path, _)| attempt.join(path))
        .collect::<Vec<_>>();
    assert_eq!(carriers.len(), 1, "{carriers:?}");
    carriers.into_iter().next().unwrap()
}

// REQ-CONTINUE-82, REQ-LOCALRUN-184, INV-ENGINE-27: a retry whose wait names a child the run
// does not own for that step, or whose child's record, tree, retained outputs or lock do not
// match, is refused before the parent's wait is consumed: no child step runs, no attempt
// begins and no file changes, and the recorded retry control still resolves the wait for a
// later resume.
#[test]
fn a_repair_retry_refuses_an_unowned_or_altered_child() {
    type Alteration = fn(&Path, &Path);
    let cases: [(&str, Alteration, Option<&str>); 8] = [
        (
            "another ordinal",
            |run, _| {
                rewrite_json(&run.join("wait.json"), |wait| {
                    wait["payload"]["child"] = "children/design/000002".into();
                });
            },
            Some("continuation_definition_invalid"),
        ),
        (
            "an escaping locator",
            |run, _| {
                rewrite_json(&run.join("wait.json"), |wait| {
                    wait["payload"]["child"] = "children/design/../design/000001".into();
                });
            },
            Some("continuation_definition_invalid"),
        ),
        (
            "another parent step",
            |_, child| {
                rewrite_json(&child.join("run.json"), |record| {
                    record["parent"]["step"] = "review".into();
                });
            },
            Some("continuation_definition_invalid"),
        ),
        (
            "another child workflow",
            |_, child| {
                rewrite_json(&child.join("run.json"), |record| {
                    record["workflow"] = "merge.yaml".into();
                });
            },
            Some("continuation_definition_invalid"),
        ),
        (
            "a changed tree copy",
            |_, child| {
                let design = child.join("workflow/tree/design.yaml");
                fs::set_permissions(design.parent().unwrap(), fs::Permissions::from_mode(0o700))
                    .unwrap();
                fs::set_permissions(&design, fs::Permissions::from_mode(0o600)).unwrap();
                let mut bytes = fs::read(&design).unwrap();
                bytes.extend_from_slice(b"# changed\n");
                fs::write(&design, bytes).unwrap();
            },
            Some("tree_copy_modified"),
        ),
        (
            "a changed retained output",
            |_, child| {
                let carrier = plan_carrier(child);
                fs::set_permissions(&carrier, fs::Permissions::from_mode(0o600)).unwrap();
                fs::write(&carrier, "plan-9").unwrap();
            },
            None,
        ),
        (
            "a missing retained output",
            |_, child| {
                let carrier = plan_carrier(child);
                fs::set_permissions(carrier.parent().unwrap(), fs::Permissions::from_mode(0o700))
                    .unwrap();
                fs::remove_file(carrier).unwrap();
            },
            None,
        ),
        ("a live child lock", |_, _| {}, Some("run_locked")),
    ];
    for (case, alter, code) in cases {
        let bundle = stage_tree(STAGE_TREE_ROOT);
        let root = bundle.execution_root();
        fs::write(root.join("plan.md"), "unfenced").unwrap();
        let (run, _) = repair_waiting_stage(&bundle, "refused", &[]);
        let child = run.join("children/design/000001");
        fs::write(root.join("plan.md"), "fenced").unwrap();
        fs::write(root.join("sentinel"), case).unwrap();
        let held = (case == "a live child lock").then(|| hold_lock(&child));
        alter(&run, &child);
        let before = StageSnapshot::take(&run, root);

        let output = run_control(None, "retry", &run, &[]);
        assert_eq!(output.status.code(), Some(1), "{case}: {output:?}");
        if let Some(code) = code {
            let document = json(&output);
            assert_eq!(document["outcome"], "rejected", "{case}: {document}");
            assert_eq!(
                document["diagnostics"][0]["code"], code,
                "{case}: {document}"
            );
        }
        before.assert_unchanged(&run, root, case);
        assert_eq!(stage_counts(root), [1, 1, 1], "{case}");
        assert_eq!(control_count(&run), 1, "{case}");

        // Once the lock is released, the recorded control continues the same child.
        if held.is_some() {
            drop(held);
            let terminal = completed(&resume(&run));
            assert_eq!(terminal["outcome"], "succeeded", "{case}: {terminal}");
            assert_eq!(ordinals_of(&run.join("children/design")), ["000001"]);
            assert_eq!(attempt_count(&child), 2, "{case}");
            assert_eq!(stage_counts(root), [1, 1, 2], "{case}");
        }
    }
}

// REQ-CONTINUE-82, INV-ENGINE-27: the inputs the waiting step would map for the child must be
// the ones its run retained; a resolving control whose `notes` change them is refused before
// the parent's wait is consumed, and once the inputs are restored the recorded control
// continues the same child.
#[test]
fn a_repair_continuation_refuses_changed_mapped_inputs() {
    let bundle = stage_tree(STAGE_TREE_ROOT);
    let root = bundle.execution_root();
    fs::write(root.join("plan.md"), "unfenced").unwrap();
    let (run, wait) = repair_waiting_stage(&bundle, "changed-inputs", &[]);
    let child = run.join("children/design/000001");
    let mut control = serde_json::json!({
        "source": "um",
        "event": "control.retry",
        "run": run_record(&run)["id"],
        "by": OPERATOR,
        "at": "2026-11-07T12:00:00Z",
        "wait": wait_id(&wait),
        "payload": {"notes": "changed"},
    });
    assert_eq!(control_count(&run), 0);
    fs::create_dir(run.join("controls")).unwrap();
    fs::set_permissions(run.join("controls"), fs::Permissions::from_mode(0o700)).unwrap();
    let control_path = run.join("controls/0001-retry.json");
    fs::write(&control_path, serde_json::to_vec(&control).unwrap()).unwrap();
    fs::write(root.join("plan.md"), "fenced").unwrap();
    let before = StageSnapshot::take(&run, root);

    let output = resume(&run);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let refusal = json(&output);
    assert_eq!(refusal["outcome"], "rejected", "{refusal}");
    assert_eq!(
        refusal["diagnostics"][0]["code"], "continuation_inputs_unsatisfied",
        "{refusal}"
    );
    before.assert_unchanged(&run, root, "changed inputs");
    assert_eq!(stage_counts(root), [1, 1, 1]);
    assert_eq!(control_count(&run), 1);

    control["payload"] = serde_json::json!({});
    fs::write(&control_path, serde_json::to_vec(&control).unwrap()).unwrap();
    let terminal = completed(&resume(&run));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(ordinals_of(&run.join("children/design")), ["000001"]);
    assert_eq!(attempt_count(&child), 2);
    assert_eq!(stage_counts(root), [1, 1, 2]);
}

// REQ-CONTINUE-81, REQ-LOCALRUN-184: automatic continuation stays in the retained execution
// root; a same-path root the retained path no longer resolves to is refused, and an explicit
// other root starts the stage afresh there, leaving the original workspace intact.
#[test]
fn a_repair_retry_in_another_execution_root_starts_a_fresh_child() {
    let bundle = stage_tree(STAGE_TREE_ROOT);
    let root = bundle.execution_root().to_owned();
    fs::write(root.join("plan.md"), "unfenced").unwrap();
    let (run, _) = repair_waiting_stage(&bundle, "other-root", &[]);
    fs::write(root.join("sentinel"), "original").unwrap();
    let original = root.with_file_name("execution-original");
    let replacement = root.with_file_name("execution-replacement");
    fs::rename(&root, &original).unwrap();
    fs::create_dir(&replacement).unwrap();
    fs::write(replacement.join("plan.md"), "fenced").unwrap();
    std::os::unix::fs::symlink(&replacement, &root).unwrap();
    let workspace = file_tree(&original);

    let output = run_control(None, "retry", &run, &[]);
    let refusal = json(&output);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        refusal["diagnostics"][0]["code"], "continuation_definition_invalid",
        "{refusal}"
    );
    assert_eq!(attempt_count(&run), 1);
    assert_eq!(ordinals_of(&run.join("children/design")), ["000001"]);
    assert!(!replacement.join("count-read").exists());

    let mut args = continue_command(&run);
    args.extend([
        "--execution-root".to_owned(),
        replacement.to_string_lossy().into_owned(),
    ]);
    let terminal = completed(&isolated_command(&args).output().unwrap());
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(
        ordinals_of(&run.join("children/design")),
        ["000001", "000002"]
    );
    assert_eq!(stage_counts(&replacement), [1, 1, 1]);
    assert_eq!(file_tree(&original), workspace);
    assert_eq!(
        fs::read_to_string(original.join("sentinel")).unwrap(),
        "original"
    );
}

/// `um workflow run resume RUN --json`.
fn continue_command(run: &Path) -> Vec<String> {
    vec![
        "workflow".to_owned(),
        "run".to_owned(),
        "resume".to_owned(),
        run.to_string_lossy().into_owned(),
        "--json".to_owned(),
    ]
}

// REQ-CONTINUE-75, REQ-LOCALRUN-184: a repair wait that continues at another step runs that
// step and the failed stage afresh, as a new stage evaluation.
#[test]
fn a_repair_retry_targeting_another_step_runs_fresh_children() {
    let bundle = stage_tree(STAGE_SETUP_ROOT);
    let root = bundle.execution_root();
    fs::write(root.join("plan.md"), "unfenced").unwrap();
    let (run, wait) = repair_waiting_stage(&bundle, "setup", &[]);
    assert_eq!(wait["then"], "setup");
    fs::write(root.join("plan.md"), "fenced").unwrap();
    let terminal = last_document(&run_control(None, "retry", &run, &[]));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(
        ordinals_of(&run.join("children/setup")),
        ["000001", "000002"]
    );
    assert_eq!(
        ordinals_of(&run.join("children/design")),
        ["000001", "000002"]
    );
    assert_eq!(stage_counts(root), [2, 2, 2]);
}

// REQ-CONTINUE-81, REQ-CONTINUE-82: a child whose ordinary steps all succeeded and whose
// required finalizer failed leaves no unfinished step to continue from, so the retry is refused
// rather than starting the stage afresh or continuing nothing.
#[test]
fn a_repair_retry_refuses_a_child_that_failed_only_in_its_finalizer() {
    let bundle = stage_tree(STAGE_TREE_ROOT);
    let root = bundle.execution_root();
    fs::write(root.join("plan.md"), "fenced").unwrap();
    fs::write(root.join("close-fails"), "").unwrap();
    let (run, _) = repair_waiting_stage(&bundle, "finalizer", &[]);
    let before = StageSnapshot::take(&run, root);
    let output = run_control(None, "retry", &run, &[]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        json(&output)["diagnostics"][0]["code"],
        "continuation_definition_invalid"
    );
    before.assert_unchanged(&run, root, "finalizer");
    assert_eq!(
        fs::read_to_string(root.join("effects")).unwrap(),
        "effect\n"
    );
}

/// A root whose `build` stage child runs `prepare` and then the long-running `work`, and whose
/// failed child waits for a repair at `build` itself (SC-LOCALRUN-15, SC-CONTINUE-11).
const INTERRUPT_TREE_ROOT: &str = "schemaVersion: 1
programs: [bin]
outcomes: [landed, failed]
steps:
  build:
    kind: workflow
    workflow: build.yaml
    on:
      built: exit.landed
      failed:
        wait: repair
        then: build
";

const INTERRUPT_TREE_BUILD: &str = "schemaVersion: 1
outcomes: [built, failed]
outcome:
  from: outputs.work.verdict
  onFailure: failed
steps:
  prepare:
    kind: cmd
    command:
      argv: [build-step, prepare]
  work:
    kind: cmd
    dependsOn: [prepare]
    command:
      argv: [build-step, work]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict.txt
";

/// `build-step STEP` counts its runs in `count-<STEP>`; `prepare` writes the workspace file
/// `prepared`; `work`, while the execution root holds `hold-work`, writes `held-work` and
/// sleeps until `release-work` appears, then reports `built`.
const INTERRUPT_TREE_PROGRAM: &str = "#!/bin/sh
set -eu
count=$(($(cat \"count-$1\" 2>/dev/null || printf 0) + 1))
printf '%s' \"$count\" > \"count-$1\"
case \"$1\" in
  prepare) printf 'prepared-%s' \"$count\" > prepared ;;
  work)
    if [ -e hold-work ]; then
      : > held-work
      while [ ! -e release-work ]; do sleep 0.1; done
    fi
    printf built > verdict.txt ;;
esac
";

/// The interrupt tree, its `work` held.
fn interrupt_tree() -> TreeBundle {
    let bundle = TreeBundle::new(INTERRUPT_TREE_ROOT);
    bundle.write_source("build.yaml", INTERRUPT_TREE_BUILD);
    bundle.write_program("bin/build-step", INTERRUPT_TREE_PROGRAM);
    fs::write(bundle.execution_root().join("hold-work"), "").unwrap();
    bundle
}

/// How often `prepare` and `work` ran in `root`.
fn build_counts(root: &Path) -> [u32; 2] {
    ["prepare", "work"].map(|step| {
        fs::read_to_string(root.join(format!("count-{step}")))
            .map_or(0, |count| count.parse().unwrap())
    })
}

/// Starts the interrupt tree's run and returns its directory once its stage's `work` holds.
fn held_stage(bundle: &TreeBundle, name: &str) -> (PathBuf, LiveRun) {
    let execution = bundle.execution_root();
    let destination = bundle.result(name);
    let live = LiveRun {
        process: isolated_command(&bundle.args(&destination))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        release: execution.join("release-work"),
    };
    poll_until(
        "the stage's held step",
        || {
            execution.join("held-work").exists()
                && um_execution::lock_holder(&destination).is_ok_and(|holder| holder.is_some())
        },
        |held| *held,
    );
    (fs::canonicalize(&destination).unwrap(), live)
}

/// Starts a later attempt of the interrupt tree's `run` with `leaf`, once the earlier
/// attempt's process has ended, and returns it once its stage's `work` holds again.
fn held_again(root: &Path, leaf: &str, run: &Path, args: &[&str]) -> LiveRun {
    fs::remove_file(root.join("held-work")).unwrap();
    let mut later = LiveRun {
        process: control_command(None, leaf, run, args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        release: root.join("release-work"),
    };
    let held = poll_until(
        "the later attempt's held step",
        || root.join("held-work").exists() || later.process.try_wait().unwrap().is_some(),
        |held| *held,
    );
    assert!(
        held && root.join("held-work").exists(),
        "{:?}",
        later.output()
    );
    later
}

/// A published step of `result` by id.
fn result_step<'a>(result: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    result["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["id"] == id)
        .unwrap()
}

// SC-LOCALRUN-15, SC-CONTINUE-11, REQ-LOCALRUN-135, REQ-WFEXEC-69, REQ-WFEXEC-70, REQ-WFEXEC-40,
// REQ-CONTINUE-81, REQ-LOCALRUN-184: SIGINT or SIGTERM to the process of a staged run interrupts
// it: the stage's running step fails `interrupted`, the root waits for a repair at the stage
// with the interruption in its payload, no control is recorded and the command exits 130 or
// 143; `retry` continues the same child from the interrupted step, `prepare` inherited and the
// workspace kept, to the run's outcome.
#[test]
fn a_signal_interrupts_a_staged_run_and_retry_continues_its_stage() {
    for (signal, status, reason) in [
        (Signal::INT, 130, "user_request"),
        (Signal::TERM, 143, "termination_request"),
    ] {
        let bundle = interrupt_tree();
        let root = bundle.execution_root();
        let (run, mut live) = held_stage(&bundle, reason);
        let output = live.signal(signal);
        assert_eq!(output.status.code(), Some(status), "{reason}: {output:?}");
        let terminal = json(&output);
        assert_eq!(terminal["outcome"], "waiting", "{terminal}");
        assert_eq!(terminal["exitStatus"], status, "{terminal}");
        let record = run_record(&run);
        assert_eq!(record["state"], "waiting", "{record}");
        assert_eq!(record["outcome"], serde_json::Value::Null, "{record}");
        let wait = read_json(&run.join("wait.json"));
        assert_eq!(wait["kind"], "repair", "{wait}");
        assert_eq!(wait["step"], serde_json::json!(["build"]), "{wait}");
        assert_eq!(
            wait["payload"]["interrupted"],
            serde_json::json!({"reason": reason, "step": "work"}),
            "{wait}"
        );
        assert_eq!(wait["payload"]["child"], "children/build/000001", "{wait}");
        assert_eq!(control_count(&run), 0);

        let child = run.join("children/build/000001");
        let child_id = run_record(&child)["id"].clone();
        assert_eq!(
            status_document(&child)["state"]["attempts"][0]["state"],
            "workflow_failed"
        );
        let first = attempt_result(&child, 1);
        let work = result_step(&first, "work");
        assert_eq!(work["state"], "failed", "{first}");
        assert_eq!(work["detail"]["phase"], "execution", "{first}");
        assert_eq!(work["detail"]["code"], "interrupted", "{first}");
        assert_eq!(build_counts(root), [1, 1]);

        // SC-CONTINUE-11: the retry continues the same child at `work`.
        fs::remove_file(root.join("hold-work")).unwrap();
        let terminal = last_document(&run_control(None, "retry", &run, &[]));
        assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
        assert_eq!(run_record(&run)["outcome"], "landed");
        assert_eq!(ordinals_of(&run.join("children/build")), ["000001"]);
        assert_eq!(run_record(&child)["id"], child_id);
        assert_eq!(attempt_count(&child), 2);
        let second = attempt_result(&child, 2);
        assert_eq!(second["continuation"]["reason"], "retry", "{second}");
        assert_eq!(
            step_states(&second),
            states(&[("prepare", "inherited"), ("work", "succeeded")])
        );
        assert_eq!(build_counts(root), [1, 2]);
        assert_eq!(
            fs::read_to_string(root.join("prepared")).unwrap(),
            "prepared-1"
        );
    }
}

// REQ-LOCALRUN-135, REQ-WFEXEC-40: SIGINT or SIGTERM to a `restart` or an `upgrade` before its
// successor's run directory exists ends the command with 130 or 143 and creates no successor,
// so none is left `canceled`: the predecessor still waits, unchanged and holding its subject,
// and the same command later moves it. The command is held between its inspection and the
// predecessor's lock.
#[test]
fn a_signal_before_a_successor_exists_leaves_the_predecessor_waiting() {
    let restart: &[&str] = &["--at", "review"];
    for (leaf, args, signal, status) in [
        ("restart", restart, Signal::INT, 130),
        ("upgrade", &[], Signal::TERM, 143),
    ] {
        let published = PublishedWaitTree::with_subject();
        let home = Some(&published.home);
        let (predecessor, id) = published.waiting_for(&published.definition, "AUTO-1");
        let review = published.bundle.source_root().join("review.yaml");
        let changed = format!("{}# revision 2\n", fs::read_to_string(&review).unwrap());
        published.bundle.write_source("review.yaml", &changed);
        assert_eq!(published.republish(&[])["revision"]["n"], 2);
        let before = records(&predecessor);

        let gate = published.bundle.result("successor-lock-gate");
        fs::create_dir_all(gate.parent().unwrap()).unwrap();
        let mut command = LiveRun {
            process: control_command(home, leaf, &id, args)
                .env("UM_TEST_RUN_LOCK_GATE", &gate)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
            release: gate.clone(),
        };
        poll_until(
            "the command's inspection",
            || gate.with_extension("inspected").exists(),
            |inspected| *inspected,
        );
        kill_process(command.pid(), signal).unwrap();
        // The process is idle at the gate; give its observer time to take the signal.
        um_support::sleep(std::time::Duration::from_millis(500));
        fs::write(&gate, "").unwrap();
        let output = command.output();

        assert_eq!(output.status.code(), Some(status), "{leaf}: {output:?}");
        assert_eq!(run_directories(predecessor.parent().unwrap()), 1);
        assert_eq!(records(&predecessor), before);
        assert_eq!(run_record(&predecessor)["state"], "waiting");
        assert_eq!(control_count(&predecessor), 0);

        decide(&published.bundle, "review", "approve");
        let moved = run_control(home, leaf, &id, args);
        assert!(moved.status.success(), "{leaf}: {moved:?}");
        assert_eq!(run_record(&predecessor)["outcome"], "superseded");
    }
}

// REQ-LOCALRUN-186, REQ-LOCALRUN-135: `cancel` on a live staged run records the cancel its
// signal carries, so the run and its child end `canceled`; that recorded cancel does not make
// a signal to a later attempt of the run a cancel; a run an interruption left waiting for a
// repair is cancelled from its wait.
#[test]
fn cancel_still_cancels_a_live_staged_run_and_an_interrupted_one() {
    let bundle = interrupt_tree();
    let (run, mut live) = held_stage(&bundle, "canceled");
    let output = run_control(None, "cancel", &run, &[]);
    let document = recorded_as(&output, "cancel", "canceled", &run);
    assert_eq!(document["control"]["event"], "control.cancel");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "canceled", "{record}");
    let child = run_record(&run.join("children/build/000001"));
    assert_eq!(child["state"], "exited", "{child}");
    assert_eq!(child["outcome"], "canceled", "{child}");
    let exited = poll_until(
        "the live run's process exit",
        || live.process.try_wait().unwrap(),
        Option::is_some,
    );
    assert_eq!(exited.unwrap().code(), Some(143));

    // The cancel that ended that attempt is not a later attempt's: a signal to the process
    // of a retry of the run interrupts it, with the `control.cancel` still in its journal.
    let root = bundle.execution_root();
    let mut later = held_again(
        root,
        "retry",
        &run,
        &["--execution-root", root.to_str().unwrap()],
    );
    assert_eq!(later.signal(Signal::INT).status.code(), Some(130));
    let record = run_record(&run);
    assert_eq!(record["state"], "waiting", "{record}");
    assert_eq!(record["outcome"], serde_json::Value::Null, "{record}");
    let wait = read_json(&run.join("wait.json"));
    assert_eq!(wait["payload"]["interrupted"]["reason"], "user_request");
    assert_eq!(ordinals_of(&run.join("controls")), ["0001-cancel.json"]);

    let bundle = interrupt_tree();
    let (run, mut live) = held_stage(&bundle, "interrupted");
    assert_eq!(live.signal(Signal::INT).status.code(), Some(130));
    assert_eq!(run_record(&run)["state"], "waiting");
    let output = run_control(None, "cancel", &run, &[]);
    recorded_as(&output, "cancel", "canceled", &run);
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "canceled", "{record}");
}

// SC-LOCALRUN-16, REQ-LOCALRUN-202, REQ-WFEXEC-70, REQ-WFEXEC-40, REQ-CONTINUE-81,
// REQ-LOCALRUN-184: `interrupt` records `control.interrupt` and wakes the one process that
// holds the run, which interrupts its stage with `termination_request` and parks the root on
// its repair wait naming the control; another live run is untouched, and `retry` continues
// the same child from the interrupted step.
#[test]
fn interrupt_parks_one_live_run_and_retry_continues_its_stage() {
    let bundle = interrupt_tree();
    let root = bundle.execution_root();
    let (run, mut live) = held_stage(&bundle, "first");
    let other_bundle = interrupt_tree();
    let (other, other_live) = held_stage(&other_bundle, "second");

    let output = run_control(None, "interrupt", &run, &["--reason", "pausing"]);
    let document = recorded_as(&output, "interrupt", "interrupted", &run);
    assert_eq!(document["control"]["event"], "control.interrupt");
    assert_eq!(document["control"]["wait"], serde_json::Value::Null);
    let control = control_file(&run, &document);
    assert_eq!(control["payload"], serde_json::json!({"reason": "pausing"}));
    let wait = &document["wait"];
    assert_eq!(wait["kind"], "repair", "{document}");
    assert_eq!(wait["step"], serde_json::json!(["build"]), "{document}");
    assert_eq!(
        wait["payload"]["interrupted"],
        serde_json::json!({
            "reason": "termination_request",
            "step": "work",
            "control": {"by": OPERATOR, "at": control["at"], "payload": {"reason": "pausing"}},
        }),
        "{document}"
    );
    assert_eq!(&read_json(&run.join("wait.json")), wait);

    let interrupted = live.output();
    assert_eq!(interrupted.status.code(), Some(143), "{interrupted:?}");
    assert_eq!(json(&interrupted)["outcome"], "waiting");
    assert_eq!(run_record(&run)["state"], "waiting");
    let child = run.join("children/build/000001");
    let child_id = run_record(&child)["id"].clone();
    assert_eq!(control_count(&child), 0);

    // The other run's process still holds it, running, with nothing recorded on it.
    assert_eq!(run_record(&other)["state"], "running");
    assert_eq!(
        um_execution::lock_holder(&other).unwrap(),
        Some(other_live.pid())
    );
    assert_eq!(control_count(&other), 0);
    assert_eq!(control_count(&other.join("children/build/000001")), 0);
    drop(other_live);

    fs::remove_file(root.join("hold-work")).unwrap();
    let terminal = last_document(&run_control(None, "retry", &run, &[]));
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(ordinals_of(&run.join("children/build")), ["000001"]);
    assert_eq!(run_record(&child)["id"], child_id);
    let second = attempt_result(&child, 2);
    assert_eq!(
        step_states(&second),
        states(&[("prepare", "inherited"), ("work", "succeeded")])
    );
    assert_eq!(build_counts(root), [1, 2]);
}

/// Takes the `run.lock` of `run` in this process, as a live process would hold it; nothing
/// here may open that lock file again while the lock is held.
fn hold_lock(run: &Path) -> fs::File {
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(run.join("run.lock"))
        .unwrap();
    rustix::fs::fcntl_lock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    lock
}

/// `interrupt` on `run` refused with `code`, with no control written.
fn interrupt_refused(run: &Path, code: &str) {
    let before = control_count(run);
    let output = run_control(None, "interrupt", run, &["--reason", "pausing"]);
    refused(&output, "interrupt", code);
    assert_eq!(control_count(run), before, "{code}");
}

// REQ-LOCALRUN-202, REQ-LOCALRUN-181: `interrupt` refuses, before writing anything, an exited
// run, a waiting run of any kind, a `running` run whose process is gone, and a live root
// whose positioned stage has no running child.
#[test]
fn interrupt_refuses_a_run_it_cannot_interrupt() {
    let bundle = interrupt_tree();
    fs::remove_file(bundle.execution_root().join("hold-work")).unwrap();
    let destination = bundle.result("exited");
    completed(
        &isolated_command(&bundle.args(&destination))
            .output()
            .unwrap(),
    );
    interrupt_refused(&fs::canonicalize(&destination).unwrap(), "run_exited");

    let bundle = wait_tree();
    decide(&bundle, "review", "approve");
    for (name, verdict) in [
        ("answer", "blocked"),
        ("repair", "failed"),
        ("approval", "planned"),
    ] {
        let (run, terminal) = waiting_run(&bundle, name, verdict);
        assert_eq!(terminal["wait"]["kind"], name, "{terminal}");
        interrupt_refused(&run, "run_not_running");
    }
    let (answered, _) = waiting_run(&bundle, "answered", "blocked");
    answer_the_one_question(&answered);
    interrupt_refused(&answered, "run_not_running");

    let bundle = interrupt_tree();
    let (abandoned, mut live) = held_stage(&bundle, "abandoned");
    live.process.kill().unwrap();
    live.process.wait().unwrap();
    assert_eq!(run_record(&abandoned)["state"], "running");
    interrupt_refused(&abandoned, "run_not_live");

    // A live root whose position names `design`, whose latest child exited, or `review`,
    // which has no child.
    let bundle = wait_tree();
    let (run, _) = waiting_run(&bundle, "between-stages", "blocked");
    let held = hold_lock(&run);
    assert_eq!(run_record(&run)["position"], serde_json::json!(["design"]));
    interrupt_refused(&run, "child_not_live");
    rewrite_json(&run.join("run.json"), |record| {
        record["position"] = serde_json::json!(["review"]);
    });
    interrupt_refused(&run, "child_not_live");
    drop(held);
}

/// The test `LockHolder` launches.
const LOCK_HOLDER_FIXTURE: &str = "workflow_run_control::run_lock_holder_fixture";
const HELD_LOCKS_VARIABLE: &str = "UM_TEST_HELD_RUN_LOCKS";
const HELD_LOCKS_RELEASE_VARIABLE: &str = "UM_TEST_HELD_RUN_LOCKS_RELEASE";

/// A process that holds run locks and installs no signal handler, as a `um` that predates
/// the wake-up does, until it is dropped.
struct LockHolder {
    process: Child,
    release: PathBuf,
}

impl LockHolder {
    /// Holds the `run.lock` of each of `runs` until `release` exists.
    fn hold(runs: &[&Path], release: PathBuf) -> Self {
        let locks = std::env::join_paths(runs.iter().map(|run| run.join("run.lock"))).unwrap();
        let holder = Self {
            process: std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", LOCK_HOLDER_FIXTURE, "--nocapture"])
                .env(HELD_LOCKS_VARIABLE, locks)
                .env(HELD_LOCKS_RELEASE_VARIABLE, &release)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
            release,
        };
        let pid = holder.pid();
        poll_until(
            "the fixture's run locks",
            || {
                runs.iter()
                    .all(|run| um_execution::lock_holder(run).is_ok_and(|held| held == Some(pid)))
            },
            |held| *held,
        );
        holder
    }

    fn pid(&self) -> Pid {
        Pid::from_raw(i32::try_from(self.process.id()).unwrap()).unwrap()
    }
}

impl Drop for LockHolder {
    fn drop(&mut self) {
        let _ = fs::write(&self.release, "");
        let deadline = um_support::monotonic_now() + std::time::Duration::from_secs(10);
        while self.process.try_wait().ok().flatten().is_none() {
            if um_support::monotonic_now() >= deadline {
                let _ = self.process.kill();
                let _ = self.process.wait();
                return;
            }
            um_support::sleep(std::time::Duration::from_millis(10));
        }
    }
}

#[test]
#[ignore = "launched as a process that holds run locks"]
fn run_lock_holder_fixture() {
    let locks = std::env::var_os(HELD_LOCKS_VARIABLE).unwrap();
    let release = PathBuf::from(std::env::var_os(HELD_LOCKS_RELEASE_VARIABLE).unwrap());
    let held = std::env::split_paths(&locks)
        .map(|path| hold_lock(path.parent().unwrap()))
        .collect::<Vec<_>>();
    while !release.exists() {
        um_support::sleep(std::time::Duration::from_millis(10));
    }
    drop(held);
}

// REQ-LOCALRUN-202, REQ-WFDEF-6: a process that does not act on the wake-up leaves the
// interrupt `interrupt_pending` after the wait, recorded and the process alive; a later
// attempt of the run, woken, does not act on that earlier control.
#[test]
fn an_unanswered_interrupt_is_pending_and_no_later_attempt_acts_on_it() {
    let bundle = interrupt_tree();
    let root = bundle.execution_root();
    let (run, mut live) = held_stage(&bundle, "pending");
    let child = run.join("children/build/000001");
    live.process.kill().unwrap();
    live.process.wait().unwrap();
    let mut holder = LockHolder::hold(&[&run, &child], bundle.result("holder-release"));

    // A shortened polling cadence shortens the wait for the release with it.
    let output = control_command(None, "interrupt", &run, &["--reason", "pausing"])
        .env("UM_TEST_POLL_INTERVAL_MILLISECONDS", "10")
        .output()
        .unwrap();
    refused(&output, "interrupt", "interrupt_pending");
    let controls = ordinals_of(&run.join("controls"));
    assert_eq!(controls, ["0001-interrupt.json"]);
    assert!(
        holder.process.try_wait().unwrap().is_none(),
        "the holder exited"
    );
    assert_eq!(run_record(&run)["state"], "running");
    drop(holder);

    // The killed process's `work` was stopped with it (its guard's parent-death protection),
    // so a later attempt holds at `work` again.
    let mut later = held_again(root, "resume", &run, &[]);
    kill_process(later.pid(), Signal::URG).unwrap();
    um_support::sleep(std::time::Duration::from_millis(500));
    fs::write(&later.release, "").unwrap();
    let output = later.output();
    let terminal = chained(&output).pop().unwrap();
    assert_eq!(terminal["outcome"], "succeeded", "{terminal}");
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(ordinals_of(&run.join("controls")), ["0001-interrupt.json"]);
}
