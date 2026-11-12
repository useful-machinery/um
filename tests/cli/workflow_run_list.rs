use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};

use serde_json::{Value, json};

use super::poll_until;
use super::workflow_continue::{
    chained, completed, decide, decide_each, loop_tree, read_json, resume,
};
use super::workflow_definition::Home;
use super::workflow_run::{isolated_command, run_record};
use super::workflow_run_control::{LiveRun, OPERATOR, PublishedWaitTree, refused, run_control};

/// `um workflow run list ARGS` as `OPERATOR` against `home`.
fn list(home: &Home, args: &[&str]) -> Output {
    list_at(Path::new(home.path()), args)
}

/// `um workflow run list ARGS` as `OPERATOR` against the home at `home`, as that path spells it.
fn list_at(home: &Path, args: &[&str]) -> Output {
    isolated_command(
        &["workflow", "run", "list"]
            .into_iter()
            .chain(args.iter().copied())
            .map(str::to_owned)
            .collect::<Vec<_>>(),
    )
    .env("UM_HOME", home)
    .env("USER", OPERATOR)
    .output()
    .unwrap()
}

/// The `um workflow run list ARGS --json` document, checked.
fn document(home: &Home, args: &[&str]) -> Value {
    let output = list(home, &[args, &["--json"]].concat());
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["schemaVersion"], 1, "{document}");
    assert_eq!(document["command"], "um workflow run list");
    document
}

/// The runs `um workflow run list ARGS --json` lists.
fn listed(home: &Home, args: &[&str]) -> Vec<Value> {
    document(home, args)["runs"].as_array().unwrap().clone()
}

/// The ids `um workflow run list ARGS --json` lists and the exited runs it says it left out
/// (REQ-LOCALRUN-203).
fn selected(home: &Home, args: &[&str]) -> (Vec<String>, u64) {
    let document = document(home, args);
    let ids = ids(document["runs"].as_array().unwrap())
        .into_iter()
        .map(str::to_owned)
        .collect();
    (ids, document["exitedHidden"].as_u64().unwrap())
}

/// `um workflow run list ARGS`'s plain output, which exits 0.
fn plain(home: &Home, args: &[&str]) -> String {
    let output = list(home, args);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    String::from_utf8(output.stdout).unwrap()
}

fn ids(runs: &[Value]) -> Vec<&str> {
    runs.iter().map(|run| run["id"].as_str().unwrap()).collect()
}

/// The questions `design` asks, so one answer leaves the wait open (REQ-LOCALRUN-182).
fn two_questions() -> Value {
    json!([
        {"id": "q1", "text": "keep the cache?"},
        {"id": "q2", "text": "name?"},
    ])
}

// REQ-LOCALRUN-188, REQ-LOCALRUN-203: `list` lists the live runs under the home in id order
// with their record and wait and counts the exited runs it left out, `--all` lists every run,
// the filters select in memory, and an unreadable entry is named on stderr.
#[test]
fn list_shows_the_live_runs_or_with_all_every_run() {
    let published = PublishedWaitTree::with_json_questions(&two_questions());
    let home = Some(&published.home);
    decide(&published.bundle, "review", "approve");
    decide(&published.bundle, "merge", "landed");
    let (waiting, waiting_id, terminal) = published.run("blocked");
    let wait = terminal["wait"].clone();
    assert_eq!(wait["kind"], "answer", "{terminal}");

    let (landed, landed_id, terminal) = published.run("planned");
    assert_eq!(terminal["wait"]["kind"], "approval", "{terminal}");
    let approved = run_control(home, "approve", &landed_id, &[]);
    assert_eq!(approved.status.code(), Some(0), "{approved:?}");
    completed(&resume(&landed));
    assert_eq!(run_record(&landed)["outcome"], "landed");

    let (withdrawn, withdrawn_id, _) = published.run("blocked");
    let output = run_control(home, "withdraw", &withdrawn_id, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");

    // A run whose `design` holds its step reads `running` while its process lives.
    let execution = published.bundle.execution_root();
    fs::write(execution.join("hold-design"), "").unwrap();
    let mut live = LiveRun {
        process: published
            .create_command(&published.definition, &[])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
        release: execution.join("release-design"),
    };
    poll_until(
        "the running run's held step",
        || execution.join("held-design").exists(),
        |held| *held,
    );
    let runs_directory = PathBuf::from(published.home.path())
        .join("workflows")
        .join(&published.definition)
        .join("runs");
    let running_id = fs::read_dir(&runs_directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .find(|id| ![&waiting_id, &landed_id, &withdrawn_id].contains(&id))
        .unwrap();
    let running = fs::canonicalize(runs_directory.join(&running_id)).unwrap();
    assert_eq!(run_record(&running)["state"], "running");

    let mut live_ids = vec![waiting_id.clone(), running_id.clone()];
    live_ids.sort_unstable();
    let mut every = vec![
        waiting_id.clone(),
        landed_id.clone(),
        withdrawn_id.clone(),
        running_id.clone(),
    ];
    every.sort_unstable();
    let home = &published.home;
    assert_eq!(selected(home, &[]), (live_ids.clone(), 2));
    assert_eq!(selected(home, &["--all"]), (every.clone(), 0));

    let runs = listed(home, &["--all"]);
    for (run, directory) in [
        (&waiting_id, &waiting),
        (&landed_id, &landed),
        (&withdrawn_id, &withdrawn),
        (&running_id, &running),
    ] {
        let listed = runs.iter().find(|listed| listed["id"] == **run).unwrap();
        let record = run_record(directory);
        assert_eq!(listed["directory"], directory.to_str().unwrap());
        for (field, member) in [
            ("definition", "definition"),
            ("revision", "revision"),
            ("workflow", "workflow"),
            ("owner", "owner"),
            ("state", "state"),
            ("outcome", "outcome"),
            ("attempts", "attempts"),
            ("startedAt", "startedAt"),
        ] {
            assert_eq!(listed[field], record[member], "{field}: {listed}");
        }
        assert_eq!(listed["definition"], published.definition.as_str());
        assert_eq!(listed["owner"], OPERATOR);
    }
    // A home spelled through a symbolic link lists each run by the same resolved directory,
    // whatever `TMPDIR` the suite runs under.
    let alias = tempfile::tempdir().unwrap();
    let aliased = alias.path().join("home");
    symlink(home.path(), &aliased).unwrap();
    let output = list_at(&aliased, &["--all", "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let through_alias: Value = serde_json::from_slice(&output.stdout).unwrap();
    let directories = |runs: &[Value]| {
        runs.iter()
            .map(|run| (run["id"].clone(), run["directory"].clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        directories(through_alias["runs"].as_array().unwrap()),
        directories(&runs),
        "{through_alias}"
    );
    let entry = |id: &str| runs.iter().find(|run| run["id"] == id).unwrap();
    let waiting_entry = entry(&waiting_id);
    assert_eq!(waiting_entry["state"], "waiting");
    assert_eq!(waiting_entry["position"], "design");
    assert_eq!(
        waiting_entry["wait"],
        json!({"kind": "answer", "since": wait["since"], "step": ["design"]})
    );
    assert_eq!(entry(&running_id)["state"], "running");
    assert_eq!(entry(&running_id)["wait"], Value::Null);
    for other in [&landed_id, &withdrawn_id] {
        let entry = entry(other);
        assert_eq!(entry["state"], "exited", "{entry}");
        assert_eq!(entry["position"], Value::Null, "{entry}");
        assert_eq!(entry["wait"], Value::Null, "{entry}");
    }
    assert_eq!(entry(&landed_id)["outcome"], "landed");

    assert_eq!(
        selected(home, &["--waiting"]),
        (vec![waiting_id.clone()], 0)
    );
    for filter in [
        ["--owner", "me"],
        ["--owner", OPERATOR],
        ["--definition", published.definition.as_str()],
    ] {
        assert_eq!(selected(home, &filter), (live_ids.clone(), 2), "{filter:?}");
        assert_eq!(
            selected(home, &[&filter[..], &["--all"]].concat()),
            (every.clone(), 0),
            "{filter:?}"
        );
    }
    assert_eq!(selected(home, &["--owner", "nobody"]), (vec![], 0));
    assert_eq!(
        selected(home, &["--definition", "def_7zzzzzzzzzzzzzzzzzzzzzzzzz"]),
        (vec![], 0)
    );
    for flag in ["--waiting", "--drift"] {
        let output = list(home, &["--all", flag]);
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty(), "{output:?}");
    }

    let output = plain(home, &[]);
    let lines = output.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 4, "{output}");
    for (line, id) in lines.iter().zip(&live_ids) {
        assert!(line.starts_with(id.as_str()), "{output}");
    }
    assert_eq!(
        lines[2..],
        ["", "2 exited runs hidden. Pass --all to list every run."],
        "{output}"
    );
    let output = plain(home, &["--all"]);
    let lines = output.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 4, "{output}");
    for (line, id) in lines.iter().zip(&every) {
        assert!(line.starts_with(id.as_str()), "{output}");
    }

    // An entry under `runs/` without a run record is named on stderr and omitted.
    let stray = runs_directory.join("run_00000000000000000000000000");
    fs::create_dir(&stray).unwrap();
    let output = list(home, &["--all", "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        ids(document["runs"].as_array().unwrap()),
        every,
        "{document}"
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(&format!("! {}: run_record_missing", stray.display())),
        "{stderr}"
    );
    fs::remove_dir(&stray).unwrap();

    // With the live runs ended, the listing has no run and says how many it left out.
    let output = run_control(Some(home), "cancel", &running_id, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    poll_until(
        "the canceled run's process exit",
        || live.process.try_wait().unwrap(),
        Option::is_some,
    );
    let output = run_control(Some(home), "withdraw", &waiting_id, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(selected(home, &[]), (vec![], 4));
    assert_eq!(
        plain(home, &[]),
        "No live workflow runs.\n\n4 exited runs hidden. Pass --all to list every run.\n"
    );

    let empty = Home::new();
    assert_eq!(plain(&empty, &[]), "No live workflow runs.\n");
    assert_eq!(plain(&empty, &["--all"]), "No workflow runs.\n");
    assert_eq!(selected(&empty, &[]), (vec![], 0));
    assert_eq!(selected(&empty, &["--all"]), (vec![], 0));
}

// REQ-LOCALRUN-188, REQ-LOCALRUN-190, REQ-LOCALRUN-203: each run lists its subject, the one
// exited run is counted as hidden, and `--drift` keeps the live runs behind their
// definition's current revision, each with that revision.
#[test]
fn list_shows_the_subject_and_the_drifted_runs() {
    let published = PublishedWaitTree::with_subject();
    let home = &published.home;
    let (_, behind) = published.waiting_for(&published.definition, "AUTO-1");
    let (_, exited) = published.waiting_for(&published.definition, "AUTO-2");
    let output = run_control(Some(home), "withdraw", &exited, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");

    let runs = listed(home, &[]);
    let entry = runs.iter().find(|run| run["id"] == *behind).unwrap();
    assert_eq!(entry["subject"], "AUTO-1", "{entry}");
    assert!(entry.get("current").is_none(), "{entry}");
    let plain = list(home, &[]);
    assert_eq!(plain.status.code(), Some(0), "{plain:?}");
    let plain = String::from_utf8(plain.stdout).unwrap();
    let line = plain
        .lines()
        .find(|line| line.starts_with(&behind))
        .unwrap_or_else(|| panic!("{plain}"));
    assert!(
        line.starts_with(&format!(
            "{behind} · {}@1 · AUTO-1 · waiting",
            published.definition
        )),
        "{plain}"
    );
    assert_eq!(
        plain.lines().last(),
        Some("1 exited run hidden. Pass --all to list every run."),
        "{plain}"
    );
    assert!(listed(home, &["--drift"]).is_empty());

    let second = published.republish(&[]);
    assert_eq!(second["revision"]["n"], 2, "{second}");
    let (_, current) = published.waiting_for(&published.definition, "AUTO-3");
    assert_eq!(ids(&listed(home, &[])).len(), 2);
    assert_eq!(ids(&listed(home, &["--all"])).len(), 3);

    let drifted = listed(home, &["--drift"]);
    assert_eq!(ids(&drifted), [behind.as_str()], "{drifted:?}");
    assert_eq!(drifted[0]["revision"], 1);
    assert_eq!(drifted[0]["current"], 2);
    assert_eq!(drifted[0]["subject"], "AUTO-1");
    assert!(!ids(&drifted).contains(&current.as_str()));
    let plain = list(home, &["--drift"]);
    assert_eq!(plain.status.code(), Some(0), "{plain:?}");
    let plain = String::from_utf8(plain.stdout).unwrap();
    let lines = plain.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "{plain}");
    assert!(lines[0].starts_with(&behind), "{plain}");
    assert!(lines[0].ends_with(" · current 2"), "{plain}");

    // A definition whose record cannot be read is named on stderr and its runs omitted.
    let record = PathBuf::from(home.path())
        .join("workflows")
        .join(&published.definition)
        .join("definition.json");
    fs::write(&record, "{}").unwrap();
    let output = list(home, &["--drift", "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["runs"], json!([]), "{document}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(&format!(
            "! {}: definition_record_invalid",
            record.parent().unwrap().display()
        )),
        "{stderr}"
    );
}

/// Every path under `directory` with its size and modification time.
pub(super) fn snapshot(directory: &Path) -> Vec<(PathBuf, u64, std::time::SystemTime)> {
    let mut entries = Vec::new();
    let mut pending = vec![directory.to_owned()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            let metadata = fs::symlink_metadata(entry.path()).unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            }
            entries.push((entry.path(), metadata.len(), metadata.modified().unwrap()));
        }
    }
    entries.sort();
    entries
}

fn shown(home: &PublishedWaitTree, run: impl AsRef<std::ffi::OsStr>) -> Value {
    let output = run_control(Some(&home.home), "show", run, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["schemaVersion"], 1, "{document}");
    assert_eq!(document["command"], "um workflow run show");
    assert_eq!(document["outcome"], "shown");
    assert_eq!(document["exitStatus"], 0);
    document
}

// REQ-LOCALRUN-189: `show` prints one run's record, position, wait, attempts, children and
// controls without taking its lock or changing its directory.
#[test]
fn show_prints_a_run_with_its_children_and_controls() {
    let published = PublishedWaitTree::with_json_questions(&two_questions());
    let (run, id, terminal) = published.run("blocked");
    let wait = terminal["wait"].clone();

    let before = snapshot(&run);
    let document = shown(&published, &id);
    assert_eq!(document["controls"], json!([]));
    assert!(!run.join("controls").exists());
    assert_eq!(snapshot(&run), before);

    let home = Some(&published.home);
    let stop = run_control(
        home,
        "stop",
        &id,
        &["--before", "review", "--reason", "hold"],
    );
    assert_eq!(stop.status.code(), Some(0), "{stop:?}");
    let answer = run_control(home, "answer", &id, &["--question", "q1", "--text", "yes"]);
    assert_eq!(answer.status.code(), Some(0), "{answer:?}");

    let before = snapshot(&run);
    let document = shown(&published, &id);
    assert_eq!(snapshot(&run), before);
    assert_eq!(document["runDirectory"], run.to_str().unwrap());
    assert_eq!(document["record"], run_record(&run));
    // REQ-WFDEF-4: the record's position names the step whose child last started.
    assert_eq!(document["record"]["position"], json!(["design"]));
    assert_eq!(document["position"], "design");
    assert_eq!(document["wait"], wait);
    assert_eq!(
        document["wait"]["rule"],
        read_json(&run.join("wait.json"))["rule"]
    );
    assert_eq!(document["wait"]["payload"]["questions"], two_questions());
    let attempts = document["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 1, "{document}");
    assert_eq!(attempts[0]["number"], 1);
    assert_eq!(attempts[0]["trigger"], "initial");
    assert_eq!(attempts[0]["state"], "waiting");
    assert_eq!(attempts[0]["reason"], Value::Null);
    let child = run.join("children/design/000001");
    assert_eq!(
        document["children"],
        json!([{
            "step": "design",
            "item": null,
            "ordinal": 1,
            "id": run_record(&child)["id"],
            "state": "exited",
            "outcome": "blocked",
            "directory": child.to_str().unwrap(),
            "progress": null,
        }])
    );
    let controls = document["controls"].as_array().unwrap();
    assert_eq!(controls.len(), 2, "{document}");
    for (ordinal, (shown, file)) in controls
        .iter()
        .zip(["0001-stop.json", "0002-answer.json"])
        .enumerate()
    {
        let control = read_json(&run.join("controls").join(file));
        assert_eq!(shown["ordinal"], ordinal + 1);
        for field in ["event", "by", "at", "payload"] {
            assert_eq!(shown[field], control[field], "{field}: {shown}");
        }
        assert_eq!(
            shown["wait"],
            control.get("wait").cloned().unwrap_or(Value::Null)
        );
    }
    assert_eq!(controls[0]["event"], "control.stop");
    assert_eq!(controls[1]["event"], "control.answer");
    assert_eq!(controls[1]["payload"]["complete"], false);

    // The run's directory names the same run.
    assert_eq!(shown(&published, &run), document);

    let unknown = run_control(home, "show", "run_7zzzzzzzzzzzzzzzzzzzzzzzzz", &[]);
    let refusal = refused(&unknown, "show", "run_not_found");
    assert!(refusal.get("runDirectory").is_none(), "{refusal}");
}

/// `um workflow run show RUN` without `--json`, which exits 0.
pub(super) fn shown_plain(run: &Path) -> String {
    let output = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "show".to_owned(),
        run.to_string_lossy().into_owned(),
    ])
    .output()
    .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    String::from_utf8(output.stdout).unwrap()
}

// REQ-LOCALRUN-189: `show` prints the run's `edges` counts and, for an attempt a backward or
// limited edge started, the edge it took beside its `reason`.
#[test]
fn show_prints_the_edges_a_run_took() {
    let bundle = loop_tree();
    decide(&bundle, "design", "planned");
    decide_each(&bundle, "review", &["hand_back", "proceed"]);
    decide(&bundle, "fix", "built");
    decide(&bundle, "merge", "landed");
    let destination = bundle.result("shown-edges");
    chained(
        &isolated_command(&bundle.args(&destination))
            .output()
            .unwrap(),
    );
    let run = fs::canonicalize(&destination).unwrap();

    let output = run_control(None, "show", &run, &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["record"], run_record(&run));
    assert_eq!(document["record"]["edges"], json!({"review.hand_back": 1}));
    let attempts = document["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 3, "{document}");
    assert_eq!(attempts[0]["reason"], Value::Null);
    assert_eq!(attempts[0]["edge"], Value::Null);
    assert_eq!(attempts[1]["reason"], "edge");
    assert_eq!(
        attempts[1]["edge"],
        json!({"step": "review", "outcome": "hand_back", "target": "fix", "counted": true})
    );
    assert_eq!(attempts[2]["reason"], "edge");
    assert_eq!(
        attempts[2]["edge"],
        json!({"step": "fix", "outcome": "built", "target": "review", "counted": false})
    );

    let plain = shown_plain(&run);
    let lines = plain.lines().collect::<Vec<_>>();
    assert!(lines.contains(&"edges: review.hand_back=1"), "{plain}");
    let attempt = lines
        .iter()
        .find(|line| line.starts_with("  2  "))
        .unwrap_or_else(|| panic!("{plain}"));
    assert!(attempt.contains("review.hand_back -> fix"), "{plain}");

    // A run that took no edge prints none.
    let published = PublishedWaitTree::new();
    let (waiting, _, _) = published.run("blocked");
    assert!(shown_plain(&waiting).lines().any(|line| line == "edges: -"));
}
