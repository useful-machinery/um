//! The run mirror end to end (REQ-LOCALRUN-191, REQ-LOCALRUN-192, REQ-LOCALRUN-186): `um`
//! performs a root workflow's `mirror` writes against a loopback stand-in for Linear's GraphQL
//! API and records each under the run's `mirror/`.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Condvar, Mutex};

use serde_json::{Value, json};

use super::workflow_continue::{
    WAIT_TREE_ROOT, append_control, completed, control, decide, ordinals_of, wait_tree,
};
use super::workflow_definition::{Home, Tree, read_json};
use super::workflow_run::{TreeBundle, isolated_command, run_record};
use super::workflow_run_control::{LiveRun, refused, signalled_at_child_start_gate};
use super::workflow_run_create::Published;

/// The login name the mirrored runs are created by, which `{{operator}}` renders.
pub(super) const MIRROR_OPERATOR: &str = "mirror-operator";

/// The `updatedAt` the stub's issue reads with.
pub(super) const ISSUE_UPDATED_AT: &str = "2026-11-10T12:00:00.000Z";

/// The team states the stub's issue offers at first, by name and id.
const STATES: [(&str, &str); 4] = [
    ("Engineering", "state-engineering"),
    ("Backlog", "state-backlog"),
    ("Canceled", "state-canceled"),
    ("Done", "state-done"),
];

/// The engineering root's steps, replaced by one `design` stage whose exit decides the run's
/// outcome, so a run passes through `onStart`, `onStage` and `onOutcome`.
const DESIGN_STAGE_STEPS: &str = "steps:
  design:
    kind: workflow
    workflow: design.yaml
    inputs:
      ticket:
        ref: inputs.ticket
    on:
      succeeded: exit.landed
      failed: exit.failed
outcomes: [landed, failed]
";

/// The root-only header the wait tree is published with: a subject, the state bindings and a
/// mirror that comments when the run starts, when a stage exits and when it waits, and moves
/// the issue on `withdrawn` and `canceled` unless a person has already moved it out of
/// `Engineering`.
const MIRRORED_WAIT_HEADER: &str = r#"schemaVersion: 1
name: waits
subject: ticket
bindings:
  ownedState: {kind: linear.state, default: "Engineering"}
  backlog:    {kind: linear.state, default: "Backlog"}
  canceled:   {kind: linear.state, default: "Canceled"}
mirror:
  linear:
    issue: inputs.ticket
    onStart: {comment: "Engineering started by {{operator}}. Run {{run.id}}: `um workflow run show {{run.id}}`"}
    onStage: {comment: "{{stage}}: {{summary}}"}
    onWait: {comment: "Waiting for {{wait.kind}}: {{wait.summary}}"}
    onOutcome:
      withdrawn: {state: $backlog, unless: issue.state != $ownedState}
      canceled:  {state: $canceled, unless: issue.state != $ownedState}
inputs:
  ticket:
    kind: text
"#;

/// A loopback stand-in for Linear's GraphQL API (REQ-LOCALRUN-192): it serves
/// `POST /graphql` by the body's `operationName`, answering `IssueState` with the issue in
/// its current state, by the name and the id of the team state so named, its `updatedAt`
/// (`ISSUE_UPDATED_AT`), and its team, `team-auto` until it is moved, with the team's
/// states, `STATES` until one is renamed, and each mutation with `success` true, or with
/// HTTP 500 and a GraphQL error while it fails mutations. A mutation it is told to hold stays
/// in flight, unanswered, until it is released. It records every body in order, and the
/// `Authorization` value each request carried.
pub(super) struct LinearStub {
    base: String,
    state: Arc<Mutex<StubState>>,
    /// Notified when a held mutation is released.
    released: Arc<Condvar>,
}

struct StubState {
    issue_state: String,
    team: String,
    /// The team's states by name and id.
    states: Vec<(String, String)>,
    failing: bool,
    /// The text a mutation's body holds that keeps it in flight until released, and whether
    /// one is held.
    hold: Option<String>,
    held: bool,
    bodies: Vec<Value>,
    /// Each request's `Authorization` value, empty when it carried none, in order.
    authorizations: Vec<String>,
}

impl LinearStub {
    pub(super) fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(StubState {
            issue_state: "Engineering".to_owned(),
            team: "team-auto".to_owned(),
            states: STATES
                .iter()
                .map(|(name, id)| ((*name).to_owned(), (*id).to_owned()))
                .collect(),
            failing: false,
            hold: None,
            held: false,
            bodies: Vec::new(),
            authorizations: Vec::new(),
        }));
        let served = Arc::clone(&state);
        let released = Arc::new(Condvar::new());
        let awaited = Arc::clone(&released);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                // The thread is detached and holds the listener for the whole test, so a panic
                // here would release the address while the test's `um` processes still aim at it
                // (INV-TESTING-22): an unreadable connection is skipped and the next one served.
                let request = match um_test_support::try_read_request(&mut stream) {
                    Ok(request) => request,
                    Err(error) => {
                        eprintln!("LinearStub skipped an unreadable request: {error}");
                        continue;
                    }
                };
                let Some((path, authorization, body)) = parse_request(&request) else {
                    continue;
                };
                let mut serving = served.lock().unwrap();
                if serving.holds(&body) {
                    serving = awaited
                        .wait_while(serving, |state| state.hold.is_some())
                        .unwrap();
                }
                serving.authorizations.push(authorization);
                let (status, answer) = serving.answer(&path, body);
                drop(serving);
                let answer = serde_json::to_vec(&answer).unwrap();
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    answer.len()
                );
                let _ = stream.write_all(&answer);
            }
        });
        Self {
            base,
            state,
            released,
        }
    }

    /// The state name `IssueState` answers from now on.
    pub(super) fn issue_state(&self, state: &str) {
        state.clone_into(&mut self.state.lock().unwrap().issue_state);
    }

    /// The team id `IssueState` answers from now on, as when a person moves the issue to
    /// another team.
    pub(super) fn issue_team(&self, team: &str) {
        team.clone_into(&mut self.state.lock().unwrap().team);
    }

    /// Renames the team state whose id is `id` to `name`, as a person editing the team's
    /// workflow would.
    pub(super) fn rename_state(&self, id: &str, name: &str) {
        let mut state = self.state.lock().unwrap();
        let entry = state
            .states
            .iter_mut()
            .find(|(_, state_id)| state_id == id)
            .unwrap();
        name.clone_into(&mut entry.0);
    }

    /// Whether every mutation is answered HTTP 500 with a GraphQL error.
    pub(super) fn fail_mutations(&self, failing: bool) {
        self.state.lock().unwrap().failing = failing;
    }

    /// Keeps the next mutation whose body contains `text` in flight, unanswered, until
    /// `release_mutation`.
    pub(super) fn hold_mutation(&self, text: &str) {
        let mut state = self.state.lock().unwrap();
        state.hold = Some(text.to_owned());
        state.held = false;
    }

    /// Whether a mutation `hold_mutation` named is held in flight.
    pub(super) fn holding_mutation(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.held && state.hold.is_some()
    }

    /// Answers the held mutation, and holds no other.
    pub(super) fn release_mutation(&self) {
        self.state.lock().unwrap().hold = None;
        self.released.notify_all();
    }

    /// Every request body served so far, in order.
    pub(super) fn bodies(&self) -> Vec<Value> {
        self.state.lock().unwrap().bodies.clone()
    }

    /// The `Authorization` value of every request served so far, in order.
    pub(super) fn authorizations(&self) -> Vec<String> {
        self.state.lock().unwrap().authorizations.clone()
    }

    /// The `operationName` of every request served so far, in order.
    pub(super) fn operations(&self) -> Vec<String> {
        self.bodies()
            .iter()
            .map(|body| {
                body["operationName"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect()
    }

    /// Forgets the requests served so far.
    pub(super) fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.bodies.clear();
        state.authorizations.clear();
    }

    /// Points `command` at the stub with the key `test`, as `MIRROR_OPERATOR`.
    pub(super) fn point<'a>(&self, command: &'a mut Command) -> &'a mut Command {
        command
            .env("LINEAR_API_KEY", "test")
            .env("UM_LINEAR_API_URL", &self.base)
            .env("NO_PROXY", "127.0.0.1")
            .env("no_proxy", "127.0.0.1")
            .env("USER", MIRROR_OPERATOR)
    }
}

impl StubState {
    /// Whether `body` is a mutation the stub holds, marking it held.
    fn holds(&mut self, body: &Value) -> bool {
        let mutation = matches!(
            body["operationName"].as_str(),
            Some("CreateIssueComment" | "UpdateIssueState")
        );
        let holds = mutation
            && self
                .hold
                .as_ref()
                .is_some_and(|text| body.to_string().contains(text.as_str()));
        self.held |= holds;
        holds
    }

    fn answer(&mut self, path: &str, body: Value) -> (&'static str, Value) {
        let operation = body["operationName"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let identifier = body["variables"]["id"].clone();
        self.bodies.push(body);
        let mutation = |field: &str| {
            if self.failing {
                (
                    "500 Internal Server Error",
                    json!({"errors": [{"message": "stub refuses mutations"}]}),
                )
            } else {
                ("200 OK", json!({"data": {field: {"success": true}}}))
            }
        };
        let state_id = self
            .states
            .iter()
            .find(|(name, _)| *name == self.issue_state)
            .map_or("current", |(_, id)| id.as_str());
        match (path, operation.as_str()) {
            ("/graphql", "IssueState") => (
                "200 OK",
                json!({"data": {"issue": {
                    "id": "issue-1",
                    "identifier": identifier,
                    "state": {"id": state_id, "name": self.issue_state},
                    "updatedAt": ISSUE_UPDATED_AT,
                    "team": {"id": self.team, "states": {"nodes": self
                        .states
                        .iter()
                        .map(|(name, id)| json!({"id": id, "name": name}))
                        .collect::<Vec<_>>()}},
                }}}),
            ),
            ("/graphql", "CreateIssueComment") => mutation("commentCreate"),
            ("/graphql", "UpdateIssueState") => mutation("issueUpdate"),
            _ => ("404 Not Found", json!({})),
        }
    }
}

/// The framed request's path, `Authorization` value (empty without one) and JSON body (`Null`
/// when the body is not JSON), or `None` when the peer closed before a complete head or the
/// request line names no path.
fn parse_request(request: &[u8]) -> Option<(String, String, Value)> {
    let (head, body) = um_test_support::split_request(request)?;
    let head = String::from_utf8_lossy(head);
    let path = head.split(' ').nth(1)?.to_owned();
    let authorization = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.trim().to_owned())
        .unwrap_or_default();
    Some((
        path,
        authorization,
        serde_json::from_slice(body).unwrap_or(Value::Null),
    ))
}

/// The engineering tree whose root's steps are `DESIGN_STAGE_STEPS`, with or without its
/// `mirror` block, published.
fn engineering(mirrored: bool) -> Published {
    Published::publish(engineering_tree(mirrored))
}

/// The engineering tree whose root's steps are `DESIGN_STAGE_STEPS`, with or without its
/// `mirror` block.
fn engineering_tree(mirrored: bool) -> Tree {
    let tree = Tree::engineering();
    let root = fs::read_to_string(Path::new(tree.path()).join("workflow.yaml")).unwrap();
    let (header, _) = root.split_once("\nsteps:\n").unwrap();
    let header = if mirrored {
        header.to_owned()
    } else {
        let (before, after) = header.split_once("\nmirror:").unwrap();
        let (_, inputs) = after.split_once("\ninputs:").unwrap();
        format!("{before}\ninputs:{inputs}")
    };
    tree.write("workflow.yaml", &format!("{header}\n{DESIGN_STAGE_STEPS}"));
    tree
}

/// `um workflow run create` of the definition for `ticket`, its command adjusted by `adjust`:
/// the run directory.
fn create(
    published: &Published,
    ticket: &str,
    adjust: impl FnOnce(&mut Command) -> &mut Command,
) -> PathBuf {
    let terminal = completed(&adjust(&mut creation(published, ticket)).output().unwrap());
    PathBuf::from(terminal["runDirectory"].as_str().unwrap())
}

/// The `um workflow run create --json` command of the definition for `ticket`.
fn creation(published: &Published, ticket: &str) -> Command {
    let execution_root = published.execution_root();
    published.command(&[
        "create",
        &published.id,
        "--execution-root",
        execution_root.to_str().unwrap(),
        "--input-text",
        "ticket",
        ticket,
        "--json",
    ])
}

/// The run's mirror writes as `(file name, record)`, in ordinal order.
pub(super) fn mirror_writes(run: &Path) -> Vec<(String, Value)> {
    let Ok(entries) = fs::read_dir(run.join("mirror")) else {
        return Vec::new();
    };
    let mut writes = entries
        .map(|entry| entry.unwrap().path())
        .map(|path| {
            (
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                read_json(&path),
            )
        })
        .collect::<Vec<_>>();
    writes.sort_by(|left, right| left.0.cmp(&right.0));
    writes
}

/// Writes `resolution` as revision 1's `resolution.json` (REQ-WFDEF-32) of `definition` in
/// `home`, as the Cloud runner records the revision's resolution.
pub(super) fn write_resolution(home: &Home, definition: &str, resolution: &Value) {
    let path = Path::new(home.path())
        .join("workflows")
        .join(definition)
        .join("revisions/1/resolution.json");
    fs::write(path, resolution.to_string()).unwrap();
}

/// The resolution record of the engineering root's `linear.team` and `linear.state`
/// bindings, by `STATES`' ids, without the keys in `omit`.
pub(super) fn resolution(omit: &[&str]) -> Value {
    let mut record = json!({"linearTeam": {"id": "team-auto", "name": "AUTO"}});
    for (key, name) in [
        ("ownedState", "Engineering"),
        ("backlog", "Backlog"),
        ("canceled", "Canceled"),
        ("done", "Done"),
    ] {
        let id = STATES.iter().find(|(state, _)| *state == name).unwrap().1;
        record[key] = json!({"id": id, "name": name});
    }
    for key in omit {
        record.as_object_mut().unwrap().remove(*key);
    }
    record
}

/// The comment bodies the stub was sent, in order.
fn comments(stub: &LinearStub) -> Vec<String> {
    stub.bodies()
        .iter()
        .filter(|body| body["operationName"] == "CreateIssueComment")
        .map(|body| {
            assert_eq!(body["variables"]["input"]["issueId"], "issue-1", "{body}");
            body["variables"]["input"]["body"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

// REQ-LOCALRUN-191, REQ-LOCALRUN-192: a definition run of a mirrored root comments when it
// starts and when its stage exits, each comment keyed by the issue's id it read first, moves
// the issue to the bound `Done` on `landed`, and records each write under `mirror/`.
#[test]
fn a_mirrored_run_comments_at_start_and_stage_and_moves_the_issue_on_its_outcome() {
    let published = engineering(true);
    let stub = LinearStub::start();

    let run = create(&published, "AUTO-1", |command| stub.point(command));
    assert_eq!(run_record(&run)["outcome"], "landed");
    let run_url = url::Url::from_file_path(&run).unwrap().to_string();
    assert_eq!(
        comments(&stub),
        [
            format!("Engineering started by {MIRROR_OPERATOR}. Run: {run_url}"),
            "design: succeeded".to_owned(),
        ]
    );
    // Each hook runs against a fresh target, so each reads the issue once for its id.
    assert_eq!(
        stub.operations(),
        [
            "IssueState",
            "CreateIssueComment",
            "IssueState",
            "CreateIssueComment",
            "IssueState",
            "UpdateIssueState"
        ]
    );
    let update = &stub.bodies()[5];
    assert_eq!(
        update["variables"],
        json!({"id": "issue-1", "input": {"stateId": "state-done"}})
    );

    let writes = mirror_writes(&run);
    let names = writes
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["0001-start.json", "0002-stage.json", "0003-outcome.json"]
    );
    for (_, write) in &writes {
        assert_eq!(write["schemaVersion"], 1, "{write}");
        assert_eq!(write["issue"], "AUTO-1", "{write}");
        assert_eq!(write["outcome"], "written", "{write}");
    }
    assert_eq!(
        writes[1].1["action"],
        json!({"kind": "comment", "body": "design: succeeded"})
    );
    assert_eq!(
        writes[2].1["action"],
        json!({"kind": "state", "state": "Done", "binding": "done"})
    );
    // A write changes nothing the run records (INV-ENGINE-25).
    let record = run_record(&run);
    assert_eq!(record["state"], "exited");
    assert_eq!(record["outcome"], "landed");
}

// REQ-LOCALRUN-191, REQ-LOCALRUN-192: a write Linear refuses, or one without a key, is a
// failed record and never changes the run's outcome; without a key nothing is sent.
#[test]
fn a_failed_mirror_write_is_recorded_and_never_fails_the_run() {
    let published = engineering(true);
    let stub = LinearStub::start();

    stub.fail_mutations(true);
    let run = create(&published, "AUTO-2", |command| stub.point(command));
    assert_eq!(run_record(&run)["outcome"], "landed");
    let writes = mirror_writes(&run);
    assert_eq!(writes.len(), 3, "{writes:?}");
    for (_, write) in &writes {
        assert_eq!(write["outcome"], "failed", "{write}");
        assert_eq!(write["code"], "mirror_write_rejected", "{write}");
        let diagnostic = write["diagnostic"].as_str().unwrap();
        assert!(diagnostic.contains("stub refuses mutations"), "{write}");
        assert!(!diagnostic.contains("test"), "{write}");
    }

    stub.clear();
    let run = create(&published, "AUTO-3", |command| {
        stub.point(command).env_remove("LINEAR_API_KEY")
    });
    assert_eq!(run_record(&run)["outcome"], "landed");
    let writes = mirror_writes(&run);
    assert_eq!(writes.len(), 3, "{writes:?}");
    for (_, write) in &writes {
        assert_eq!(write["outcome"], "failed", "{write}");
        assert_eq!(write["code"], "mirror_credentials_unavailable", "{write}");
    }
    assert_eq!(stub.bodies(), Vec::<Value>::new());
}

/// The variable naming the file that holds the run's Linear grant token (REQ-LOCALRUN-192).
const TOKEN_FILE_VARIABLE: &str = "UM_LINEAR_ACCESS_TOKEN_FILE";

/// The variable holding the run's Linear grant token (REQ-LOCALRUN-192).
const TOKEN_VARIABLE: &str = "UM_LINEAR_ACCESS_TOKEN";

// REQ-LOCALRUN-192: the mirror writes with the first Linear credential the environment
// offers, read anew for each hook: the token in the file `UM_LINEAR_ACCESS_TOKEN_FILE` names,
// sent as `Bearer` before `UM_LINEAR_ACCESS_TOKEN` and `LINEAR_API_KEY`, and sent replaced by
// a hook after the file is replaced; else `UM_LINEAR_ACCESS_TOKEN` as `Bearer`, a missing
// file being skipped; else `LINEAR_API_KEY` bare.
#[test]
fn the_mirror_writes_with_the_first_linear_credential_the_environment_offers() {
    let published = engineering(true);
    let stub = LinearStub::start();
    let files = tempfile::tempdir().unwrap();
    let token_file = files.path().join("linear-token");
    fs::write(&token_file, "grant-first\n").unwrap();

    // The `onStart` comment is held in flight while the file is replaced, so the start hook
    // has sent the first token and every later hook reads the second.
    stub.hold_mutation("Engineering started by");
    let mut command = creation(&published, "AUTO-4");
    stub.point(&mut command)
        .env(TOKEN_FILE_VARIABLE, &token_file)
        .env(TOKEN_VARIABLE, "variable-token")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let process = command.spawn().unwrap();
    super::poll_until(
        "the held onStart write",
        || stub.holding_mutation(),
        |held| *held,
    );
    fs::write(&token_file, "grant-second\n").unwrap();
    stub.release_mutation();
    let terminal = completed(&process.wait_with_output().unwrap());
    let run = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(
        stub.authorizations(),
        [
            "Bearer grant-first",
            "Bearer grant-first",
            "Bearer grant-second",
            "Bearer grant-second",
            "Bearer grant-second",
            "Bearer grant-second",
        ]
    );
    for (_, write) in mirror_writes(&run) {
        assert_eq!(write["outcome"], "written", "{write}");
    }

    stub.clear();
    let run = create(&published, "AUTO-5", |command| {
        stub.point(command)
            .env(TOKEN_FILE_VARIABLE, files.path().join("missing"))
            .env(TOKEN_VARIABLE, "variable-token")
    });
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(stub.authorizations(), ["Bearer variable-token"; 6]);

    stub.clear();
    let run = create(&published, "AUTO-6", |command| stub.point(command));
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(stub.authorizations(), ["test"; 6]);
}

/// The `design` stage's program in `a_failed_state_commit_writes_no_outcome_to_the_mirror`,
/// run in the execution root: while `um-runs` there names the definition's runs directory, it
/// moves the `state.json` of the run that has not exited aside, so the root's next commit, the
/// step's settlement, finds none, and forgets `um-runs`. It always fails.
const STATE_MOVING_DESIGN: &str = r#"if [ -e um-runs ]; then for state in "$(cat um-runs)"/*/state.json; do case "$(cat "${state%state.json}run.json")" in *exited*) ;; *) mv "$state" root-state.json ;; esac; done; rm um-runs; fi; exit 1"#;

/// `contents` with its one `from` replaced by `to`.
fn replaced(contents: &str, from: &str, to: &str) -> String {
    assert_eq!(contents.matches(from).count(), 1, "{contents}");
    contents.replacen(from, to, 1)
}

// REQ-LOCALRUN-206, REQ-LOCALRUN-143: a mirrored run whose stage fails mirrors the failure
// outcome its record reads, but when the root's state commit fails the attempt is left
// unsettled, its record `running`, and nothing is written for `onOutcome`.
#[test]
fn a_failed_state_commit_writes_no_outcome_to_the_mirror() {
    let tree = engineering_tree(true);
    let on_outcome = "    onOutcome:\n";
    let root = fs::read_to_string(Path::new(tree.path()).join("workflow.yaml")).unwrap();
    tree.write(
        "workflow.yaml",
        &replaced(
            &root,
            on_outcome,
            &format!("{on_outcome}      failed:    {{state: $backlog}}\n"),
        ),
    );
    let design = fs::read_to_string(Path::new(tree.path()).join("design.yaml")).unwrap();
    tree.write(
        "design.yaml",
        &replaced(
            &design,
            r#"argv: ["design.py"]"#,
            &format!("argv: [\"sh\", \"-c\", '{STATE_MOVING_DESIGN}']"),
        ),
    );
    let published = Published::publish(tree);
    let stub = LinearStub::start();
    let execution_root = published.execution_root();
    let run = |ticket: &str, known: Option<&Path>| {
        let output = stub
            .point(&mut creation(&published, ticket))
            .output()
            .unwrap();
        let mut runs = fs::read_dir(published.runs_directory())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|run| Some(run.as_path()) != known)
            .collect::<Vec<_>>();
        assert_eq!(runs.len(), 1, "{runs:?}");
        (output, runs.pop().unwrap())
    };

    let (_, control) = run("AUTO-1", None);
    let record = run_record(&control);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "failed", "{record}");
    let writes = mirror_names(&control);
    assert!(
        writes.last().unwrap().ends_with("-outcome.json"),
        "{writes:?}"
    );
    assert!(stub.operations().contains(&"UpdateIssueState".to_owned()));

    stub.clear();
    fs::write(
        execution_root.join("um-runs"),
        published.runs_directory().to_str().unwrap(),
    )
    .unwrap();
    let (output, unsettled) = run("AUTO-2", Some(&control));
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(execution_root.join("root-state.json").is_file());
    let writes = mirror_names(&unsettled);
    assert_eq!(writes, ["0001-start.json", "0002-stage.json"]);
    let operations = stub.operations();
    assert!(
        !operations.contains(&"UpdateIssueState".to_owned()),
        "{operations:?}"
    );
    let record = run_record(&unsettled);
    assert_eq!(record["state"], "running", "{record}");
    assert_eq!(record["outcome"], Value::Null, "{record}");
}

// REQ-LOCALRUN-191: a run started by path and a run of a root without `mirror` write
// nothing and have no `mirror/`.
#[test]
fn a_path_run_and_an_unmirrored_root_write_nothing() {
    let published = engineering(true);
    let stub = LinearStub::start();
    let execution_root = published.execution_root();
    let run_dir = published.scratch.path().join("path-run");
    let mut command = published.command(&[
        "create",
        "--path",
        published.tree.path(),
        "--run-dir",
        run_dir.to_str().unwrap(),
        "--execution-root",
        execution_root.to_str().unwrap(),
        "--input-text",
        "ticket",
        "AUTO-1",
        "--json",
    ]);
    completed(&stub.point(&mut command).output().unwrap());
    assert_eq!(run_record(&run_dir)["outcome"], "landed");
    assert!(!run_dir.join("mirror").exists());

    let unmirrored = engineering(false);
    let run = create(&unmirrored, "AUTO-1", |command| stub.point(command));
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert!(!run.join("mirror").exists());
    assert_eq!(stub.bodies(), Vec::<Value>::new());

    // REQ-LOCALRUN-189: `show` lists no mirror write for a run without `mirror/`.
    let run = run.to_str().unwrap();
    assert_eq!(
        shown_mirror(&published.command(&["show", run]).output().unwrap()),
        ["mirror: none"]
    );
    let document = shown_document(
        &published
            .command(&["show", run, "--json"])
            .output()
            .unwrap(),
    );
    assert_eq!(document["mirror"], json!([]), "{document}");
}

// REQ-WFDEF-32, REQ-LOCALRUN-191, REQ-LOCALRUN-192: once a person renamed the `Done` state,
// a run of a revision without a resolution record finds no team state of the bound name,
// while one of a revision with the record moves the issue by the id it resolved; `run.url`
// is the run directory's `file://` URL, or the record's `external.url` when it has one.
#[test]
fn a_resolution_record_moves_the_issue_by_state_id_and_run_url_is_the_external_url() {
    let published = engineering(true);
    let stub = LinearStub::start();
    stub.rename_state("state-done", "Completed");

    let run = create(&published, "AUTO-1", |command| stub.point(command));
    assert_eq!(run_record(&run)["outcome"], "landed");
    let run_url = url::Url::from_file_path(&run).unwrap().to_string();
    assert_eq!(
        comments(&stub)[0],
        format!("Engineering started by {MIRROR_OPERATOR}. Run: {run_url}")
    );
    assert_eq!(stub.operations().last().unwrap(), "IssueState");
    let writes = mirror_writes(&run);
    let (name, write) = writes.last().unwrap();
    assert_eq!(name, "0003-outcome.json");
    assert_eq!(write["outcome"], "failed", "{write}");
    assert_eq!(write["code"], "mirror_state_unknown", "{write}");

    write_resolution(&published.home, &published.id, &resolution(&[]));
    stub.clear();
    let external = "https://usefulmachinery.test/runs/run_x";
    let run = create(&published, "AUTO-2", |command| {
        stub.point(command)
            .args(["--external", &format!("url={external}")])
    });
    assert_eq!(run_record(&run)["outcome"], "landed");
    assert_eq!(
        comments(&stub),
        [
            format!("Engineering started by {MIRROR_OPERATOR}. Run: {external}"),
            "design: succeeded".to_owned(),
        ]
    );
    assert_eq!(stub.operations()[4..], ["IssueState", "UpdateIssueState"]);
    assert_eq!(
        stub.bodies()[5]["variables"],
        json!({"id": "issue-1", "input": {"stateId": "state-done"}})
    );
    let writes = mirror_writes(&run);
    let (name, write) = writes.last().unwrap();
    assert_eq!(name, "0003-outcome.json");
    assert_eq!(write["outcome"], "written", "{write}");
    assert_eq!(
        write["action"],
        json!({"kind": "state", "state": "Done", "binding": "done"})
    );
}

// REQ-WFDEF-32, REQ-LOCALRUN-192: with a resolution record the guard
// `issue.state != $ownedState` compares state ids: it does not hold for an issue in the
// owned state a person renamed, and holds for one in another state now bearing the owned
// state's name; a bound key the record gives no id is `mirror_state_unknown` and sends
// nothing.
#[test]
fn with_a_resolution_record_the_guard_holds_by_state_id() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    write_resolution(
        &tree.home,
        &tree.definition,
        &resolution(&["linearTeam", "canceled", "done"]),
    );
    let stub = LinearStub::start();
    stub.rename_state("state-engineering", "In engineering");
    stub.issue_state("In engineering");

    let renamed = tree.waiting(&stub, "AUTO-1");
    stub.clear();
    let output = tree.run(&stub, &["withdraw", renamed.to_str().unwrap(), "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(stub.operations(), ["IssueState", "UpdateIssueState"]);
    assert_eq!(
        stub.bodies()[1]["variables"],
        json!({"id": "issue-1", "input": {"stateId": "state-backlog"}})
    );
    let writes = mirror_writes(&renamed);
    assert_eq!(writes.last().unwrap().1["outcome"], "written", "{writes:?}");

    stub.rename_state("state-backlog", "Engineering");
    stub.issue_state("Engineering");
    let elsewhere = tree.waiting(&stub, "AUTO-2");
    stub.clear();
    let output = tree.run(&stub, &["withdraw", elsewhere.to_str().unwrap(), "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(stub.operations(), ["IssueState"]);
    let writes = mirror_writes(&elsewhere);
    let (_, write) = writes.last().unwrap();
    assert_eq!(write["outcome"], "skipped", "{write}");
    assert_eq!(write["reason"], "guard_held", "{write}");

    let unresolved = tree.waiting(&stub, "AUTO-3");
    stub.clear();
    let output = tree.run(&stub, &["cancel", unresolved.to_str().unwrap(), "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(run_record(&unresolved)["outcome"], "canceled");
    assert_eq!(stub.bodies(), Vec::<Value>::new());
    let writes = mirror_writes(&unresolved);
    let (_, write) = writes.last().unwrap();
    assert_eq!(write["outcome"], "failed", "{write}");
    assert_eq!(write["code"], "mirror_state_unknown", "{write}");
    assert_eq!(
        write["action"],
        json!({"kind": "state", "state": "Canceled", "binding": "canceled"})
    );
}

/// `um workflow run show --json`'s document.
fn shown_document(output: &Output) -> Value {
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

/// The `mirror` section of `um workflow run show`'s plain output, which follows `controls`:
/// its lines from its heading to the end.
fn shown_mirror(output: &Output) -> Vec<String> {
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let lines = String::from_utf8(output.stdout.clone())
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let controls = lines.iter().position(|line| line == "controls:").unwrap();
    let mirror = lines
        .iter()
        .position(|line| line.starts_with("mirror:"))
        .unwrap();
    assert!(controls < mirror, "{lines:?}");
    lines[mirror..].to_vec()
}

/// The wait tree published with `MIRRORED_WAIT_HEADER`, its home and definition.
struct MirroredWaitTree {
    home: Home,
    bundle: TreeBundle,
    definition: String,
}

impl MirroredWaitTree {
    /// The wait tree published with `header` in place of its first line.
    fn publish(header: &str) -> Self {
        let bundle = wait_tree();
        bundle.write_source(
            "workflow.yaml",
            &WAIT_TREE_ROOT.replacen("schemaVersion: 1\n", header, 1),
        );
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

    /// `um workflow run ARGS` under the home, pointed at `stub`.
    fn run(&self, stub: &LinearStub, args: &[&str]) -> Output {
        self.run_adjusted(stub, args, |command| command)
    }

    /// `run` with its command, once pointed at `stub`, adjusted by `adjust`.
    fn run_adjusted(
        &self,
        stub: &LinearStub,
        args: &[&str],
        adjust: impl FnOnce(&mut Command) -> &mut Command,
    ) -> Output {
        let mut command = self.command(stub, args);
        adjust(&mut command).output().unwrap()
    }

    /// The command `run` runs, to spawn.
    fn command(&self, stub: &LinearStub, args: &[&str]) -> Command {
        let mut full = vec!["workflow".to_owned(), "run".to_owned()];
        full.extend(args.iter().map(|arg| (*arg).to_owned()));
        let mut command = isolated_command(&full);
        command.env("UM_HOME", self.home.path());
        stub.point(&mut command);
        command
    }

    /// A run for `ticket` whose `design` stage holds its step: the live process.
    fn held(&self, stub: &LinearStub, ticket: &str) -> LiveRun {
        let execution = self.bundle.execution_root();
        let _ = fs::remove_file(execution.join("held-design"));
        fs::write(execution.join("hold-design"), "").unwrap();
        decide(&self.bundle, "design", "planned");
        let mut full = [
            "workflow",
            "run",
            "create",
            &self.definition,
            "--execution-root",
        ]
        .map(str::to_owned)
        .to_vec();
        full.extend([
            execution.to_str().unwrap().to_owned(),
            "--input-text".to_owned(),
            "ticket".to_owned(),
            ticket.to_owned(),
            "--json".to_owned(),
        ]);
        let mut command = isolated_command(&full);
        command
            .env("UM_HOME", self.home.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let live = LiveRun {
            process: stub.point(&mut command).spawn().unwrap(),
            release: execution.join("release-design"),
        };
        super::poll_until(
            "the stage's held step",
            || execution.join("held-design").exists(),
            |held| *held,
        );
        live
    }

    /// A run for `ticket` whose `design` waits on an answer.
    fn waiting(&self, stub: &LinearStub, ticket: &str) -> PathBuf {
        self.waiting_after(stub, ticket, "blocked", "answer")
    }

    /// A run for `ticket` whose `design` decided `verdict` and waits on `kind`.
    fn waiting_after(&self, stub: &LinearStub, ticket: &str, verdict: &str, kind: &str) -> PathBuf {
        decide(&self.bundle, "design", verdict);
        let execution_root = self.bundle.execution_root().to_str().unwrap().to_owned();
        let terminal = completed(&self.run(
            stub,
            &[
                "create",
                &self.definition,
                "--execution-root",
                &execution_root,
                "--input-text",
                "ticket",
                ticket,
                "--json",
            ],
        ));
        assert_eq!(terminal["wait"]["kind"], kind, "{terminal}");
        PathBuf::from(terminal["runDirectory"].as_str().unwrap())
    }
}

// SC-LOCALRUN-12, REQ-LOCALRUN-191, REQ-LOCALRUN-192, REQ-LOCALRUN-186, REQ-LOCALRUN-189,
// REQ-WFFORMAT-101: a mirrored run comments when it starts, naming the run by its id and the
// command that shows it, when its stage exits and when it waits; `withdraw`
// skips the state write when a person already moved the issue out of the owned state,
// `cancel` moves an issue still in it to the bound `Canceled`, and `show` lists every write.
#[test]
fn a_tickets_lifecycle_is_mirrored_and_show_lists_every_write() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    let stub = LinearStub::start();

    let run = tree.waiting(&stub, "AUTO-1");
    let run_id = run_record(&run)["id"].as_str().unwrap().to_owned();
    let start = format!(
        "Engineering started by {MIRROR_OPERATOR}. Run {run_id}: `um workflow run show {run_id}`"
    );
    assert_eq!(
        comments(&stub),
        [
            start.as_str(),
            "design: blocked",
            "Waiting for answer: design"
        ]
    );
    let writes = mirror_writes(&run);
    let names = writes
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["0001-start.json", "0002-stage.json", "0003-wait.json"]
    );
    for (_, write) in &writes {
        assert_eq!(write["outcome"], "written", "{write}");
    }

    stub.clear();
    stub.issue_state("Backlog");
    let withdrawn = tree.run(&stub, &["withdraw", run.to_str().unwrap(), "--json"]);
    assert_eq!(withdrawn.status.code(), Some(0), "{withdrawn:?}");
    assert_eq!(run_record(&run)["outcome"], "withdrawn");
    assert_eq!(stub.operations(), ["IssueState"]);
    let writes = mirror_writes(&run);
    assert_eq!(writes.len(), 4, "{writes:?}");
    assert_eq!(writes[3].0, "0004-outcome.json");
    assert_eq!(writes[3].1["outcome"], "skipped");
    assert_eq!(writes[3].1["reason"], "guard_held");
    assert_eq!(
        writes[3].1["action"],
        json!({"kind": "state", "state": "Backlog", "binding": "backlog"})
    );

    // `show` lists each write in ordinal order after the controls, and its document carries
    // each record with its ordinal.
    assert_eq!(
        shown_mirror(&tree.run(&stub, &["show", "AUTO-1"])),
        [
            "mirror:".to_owned(),
            format!("  0001 · start · comment {start} · written"),
            "  0002 · stage · comment design: blocked · written".to_owned(),
            "  0003 · wait · comment Waiting for answer: design · written".to_owned(),
            "  0004 · outcome · state Backlog · skipped guard_held".to_owned(),
        ]
    );
    let document = shown_document(&tree.run(&stub, &["show", "AUTO-1", "--json"]));
    let expected = writes
        .iter()
        .zip(1_u64..)
        .map(|((_, write), ordinal)| {
            let mut write = write.clone();
            write["ordinal"] = json!(ordinal);
            write
        })
        .collect::<Vec<_>>();
    assert_eq!(document["mirror"], json!(expected), "{document}");

    stub.issue_state("Engineering");
    let canceled = tree.waiting(&stub, "AUTO-2");
    stub.clear();
    let output = tree.run(&stub, &["cancel", canceled.to_str().unwrap(), "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(run_record(&canceled)["outcome"], "canceled");
    assert_eq!(stub.operations(), ["IssueState", "UpdateIssueState"]);
    assert_eq!(
        stub.bodies()[1]["variables"],
        json!({"id": "issue-1", "input": {"stateId": "state-canceled"}})
    );
    let writes = mirror_writes(&canceled);
    let (name, write) = writes.last().unwrap();
    assert_eq!(name, "0004-outcome.json");
    assert_eq!(write["outcome"], "written", "{write}");
    assert_eq!(
        write["action"],
        json!({"kind": "state", "state": "Canceled", "binding": "canceled"})
    );
    assert_eq!(
        shown_mirror(&tree.run(&stub, &["show", "AUTO-2"]))
            .last()
            .unwrap(),
        "  0004 · outcome · state Canceled · written"
    );
}

// REQ-WFEXEC-73, REQ-LOCALRUN-191, REQ-LOCALRUN-186: a live run withdrawn while its stage runs
// ends `withdrawn` once the stage finishes, and its process fires `onOutcome.withdrawn` once,
// moving the issue still in the owned state to the bound `Backlog`.
#[test]
fn a_live_withdrawn_run_fires_its_withdrawn_outcome_once() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    let stub = LinearStub::start();
    let execution = tree.bundle.execution_root();
    fs::write(execution.join("hold-design"), "").unwrap();
    decide(&tree.bundle, "design", "planned");
    let execution_root = execution.to_str().unwrap().to_owned();
    let mut command = isolated_command(&[
        "workflow".to_owned(),
        "run".to_owned(),
        "create".to_owned(),
        tree.definition.clone(),
        "--execution-root".to_owned(),
        execution_root,
        "--input-text".to_owned(),
        "ticket".to_owned(),
        "AUTO-4".to_owned(),
        "--json".to_owned(),
    ]);
    command
        .env("UM_HOME", tree.home.path())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    /// Lets the held stage go if the test ends early, so its process never sleeps on.
    struct Release(PathBuf);
    impl Drop for Release {
        fn drop(&mut self) {
            let _ = fs::write(&self.0, "");
        }
    }
    let release = Release(execution.join("release-design"));
    let process = stub.point(&mut command).spawn().unwrap();
    super::poll_until(
        "the live run's held stage",
        || execution.join("held-design").exists(),
        |held| *held,
    );
    let withdrawn = super::poll_until(
        "the live run's withdraw",
        || tree.run(&stub, &["withdraw", "AUTO-4", "--json"]),
        |output| output.status.code() == Some(0),
    );
    let document: Value = serde_json::from_slice(&withdrawn.stdout).unwrap();
    assert_eq!(document["outcome"], "recorded", "{document}");
    let run = PathBuf::from(document["runDirectory"].as_str().unwrap());

    drop(release);
    let output = process.wait_with_output().unwrap();
    let terminal: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["result"]["outcome"], "cancelled", "{terminal}");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "withdrawn", "{record}");

    let outcomes = mirror_writes(&run)
        .into_iter()
        .filter(|(name, _)| name.ends_with("-outcome.json"))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(outcomes[0].1["outcome"], "written", "{outcomes:?}");
    assert_eq!(
        outcomes[0].1["action"],
        json!({"kind": "state", "state": "Backlog", "binding": "backlog"})
    );
    let updates = stub
        .bodies()
        .into_iter()
        .filter(|body| body["operationName"] == "UpdateIssueState")
        .collect::<Vec<_>>();
    assert_eq!(updates.len(), 1, "{updates:?}");
    assert_eq!(
        updates[0]["variables"],
        json!({"id": "issue-1", "input": {"stateId": "state-backlog"}})
    );
}

// SC-LOCALRUN-15, REQ-LOCALRUN-191, REQ-LOCALRUN-135, REQ-LOCALRUN-186, REQ-LOCALRUN-204:
// SIGINT to the process of a mirrored run interrupts its stage, whose repair wait's `onWait`
// comment begins with the interruption, and the run writes no `onOutcome`; a live run ended
// by `cancel`, even from a shell without the key, still writes `onOutcome.canceled`.
#[test]
fn an_interrupted_run_mirrors_its_wait_and_a_cancelled_one_its_outcome() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    let stub = LinearStub::start();

    let output = tree
        .held(&stub, "AUTO-1")
        .signal(rustix::process::Signal::INT);
    assert_eq!(output.status.code(), Some(130), "{output:?}");
    let terminal: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(terminal["wait"]["kind"], "repair", "{terminal}");
    let run = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
    assert_eq!(run_record(&run)["state"], "waiting");
    let writes = mirror_writes(&run);
    let waits = writes
        .iter()
        .filter(|(name, _)| name.ends_with("-wait.json"))
        .collect::<Vec<_>>();
    assert_eq!(waits.len(), 1, "{writes:?}");
    assert_eq!(waits[0].1["outcome"], "written", "{writes:?}");
    assert!(
        !writes
            .iter()
            .any(|(name, _)| name.ends_with("-outcome.json")),
        "{writes:?}"
    );
    assert!(
        comments(&stub)
            .last()
            .unwrap()
            .starts_with("Waiting for repair: interrupted"),
        "{:?}",
        comments(&stub)
    );
    assert!(!stub.operations().contains(&"UpdateIssueState".to_owned()));

    // A live run is cancelled from a shell without the key: the process that holds it writes
    // `onOutcome.canceled` with its own (REQ-LOCALRUN-204).
    stub.clear();
    let mut live = tree.held(&stub, "AUTO-2");
    let output = tree.run_adjusted(&stub, &["cancel", "AUTO-2", "--json"], without_key);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let exited = super::poll_until(
        "the cancelled run's process exit",
        || live.process.try_wait().unwrap(),
        Option::is_some,
    );
    assert!(exited.is_some());
    let shown: Value = serde_json::from_slice(&output.stdout).unwrap();
    let canceled = PathBuf::from(shown["runDirectory"].as_str().unwrap());
    assert_eq!(run_record(&canceled)["outcome"], "canceled");
    let writes = mirror_writes(&canceled);
    let (name, write) = writes.last().unwrap();
    assert!(name.ends_with("-outcome.json"), "{writes:?}");
    assert_eq!(write["outcome"], "written", "{write}");
    assert_eq!(
        write["action"],
        json!({"kind": "state", "state": "Canceled", "binding": "canceled"})
    );
}

// SC-CONTINUE-12, REQ-LOCALRUN-191, REQ-WFEXEC-72: a `retry` of a mirrored run's repair
// wait that SIGINT stops before its stage's child is prepared keeps that wait and writes no
// `onWait`, since the wait's was written when it was entered; nothing else is written either.
#[test]
fn a_kept_wait_writes_no_mirror_record() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    let stub = LinearStub::start();
    let output = tree
        .held(&stub, "AUTO-1")
        .signal(rustix::process::Signal::INT);
    assert_eq!(output.status.code(), Some(130), "{output:?}");
    let terminal: Value = serde_json::from_slice(&output.stdout).unwrap();
    let run = PathBuf::from(terminal["runDirectory"].as_str().unwrap());
    let written = mirror_names(&run);
    assert_eq!(
        written
            .iter()
            .filter(|name| name.ends_with("-wait.json"))
            .count(),
        1,
        "{written:?}"
    );

    stub.clear();
    let output = signalled_at_child_start_gate(
        tree.command(&stub, &["retry", run.to_str().unwrap(), "--json"]),
        &tree.bundle.result("child-start-gate"),
        rustix::process::Signal::INT,
        || {},
    );
    assert_eq!(output.status.code(), Some(130), "{output:?}");
    assert_eq!(run_record(&run)["state"], "waiting");
    assert_eq!(
        read_json(&run.join("attempts/000002/wait.json"))["kind"],
        "repair"
    );
    assert_eq!(mirror_names(&run), written);
    assert!(comments(&stub).is_empty(), "{:?}", comments(&stub));
}

/// `command` without `LINEAR_API_KEY`.
fn without_key(command: &mut Command) -> &mut Command {
    command.env_remove("LINEAR_API_KEY")
}

/// `command` with `LINEAR_API_KEY` set to the empty string.
fn with_empty_key(command: &mut Command) -> &mut Command {
    command.env("LINEAR_API_KEY", "")
}

/// The names of the run's mirror writes, in ordinal order.
fn mirror_names(run: &Path) -> Vec<String> {
    mirror_writes(run)
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// The names of the run's controls, in ordinal order; none before the first is appended.
fn control_names(run: &Path) -> Vec<String> {
    let controls = run.join("controls");
    if controls.exists() {
        ordinals_of(&controls)
    } else {
        Vec::new()
    }
}

/// Checks that `output` is `leaf` refused `mirror_credentials_unavailable` with a message
/// naming the three variables a Linear credential is read from (REQ-LOCALRUN-204).
fn refused_without_key(output: &Output, leaf: &str) {
    let document = refused(output, leaf, "mirror_credentials_unavailable");
    let message = document["diagnostics"][0]["message"].as_str().unwrap();
    for variable in [TOKEN_FILE_VARIABLE, TOKEN_VARIABLE, "LINEAR_API_KEY"] {
        assert!(message.contains(variable), "{document}");
    }
}

// SC-LOCALRUN-17, REQ-LOCALRUN-204, REQ-LOCALRUN-181, REQ-LOCALRUN-186: `withdraw` and
// `cancel` of a mirrored waiting run from a shell with no Linear credential, its
// `LINEAR_API_KEY` unset or empty and no grant token, are refused before anything changes and
// nothing is sent, and the same `cancel` with the key ends the run and moves its issue.
#[test]
fn ending_a_mirrored_waiting_run_without_the_key_is_refused_before_anything_changes() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    let stub = LinearStub::start();

    let run = tree.waiting(&stub, "AUTO-1");
    let record = fs::read(run.join("run.json")).unwrap();
    let wait = fs::read(run.join("wait.json")).unwrap();
    let writes = mirror_names(&run);
    stub.clear();
    let path = run.to_str().unwrap();
    for leaf in ["withdraw", "cancel"] {
        for adjust in [without_key, with_empty_key] {
            refused_without_key(
                &tree.run_adjusted(&stub, &[leaf, path, "--json"], adjust),
                leaf,
            );
            assert_eq!(control_names(&run), Vec::<String>::new());
            assert_eq!(fs::read(run.join("run.json")).unwrap(), record);
            assert_eq!(fs::read(run.join("wait.json")).unwrap(), wait);
            assert_eq!(mirror_names(&run), writes);
            assert_eq!(stub.bodies(), Vec::<Value>::new());
        }
    }

    let output = tree.run(&stub, &["cancel", path, "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "canceled", "{record}");
    assert_eq!(control_names(&run), ["0001-cancel.json"]);
    let writes = mirror_writes(&run);
    let (name, write) = writes.last().unwrap();
    assert_eq!(name, "0004-outcome.json");
    assert_eq!(write["outcome"], "written", "{write}");
}

// SC-LOCALRUN-17, REQ-LOCALRUN-204, REQ-LOCALRUN-192: the check before `withdraw` ends a
// mirrored waiting run is for any Linear credential: a token file that is missing and an
// empty `UM_LINEAR_ACCESS_TOKEN` are none, so it still refuses, and from a shell whose only
// credential is `UM_LINEAR_ACCESS_TOKEN` it proceeds and moves the issue with that token.
#[test]
fn ending_a_mirrored_waiting_run_with_only_the_linear_access_token_proceeds() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    let stub = LinearStub::start();
    let files = tempfile::tempdir().unwrap();

    let run = tree.waiting(&stub, "AUTO-1");
    stub.clear();
    let path = run.to_str().unwrap();
    refused_without_key(
        &tree.run_adjusted(&stub, &["withdraw", path, "--json"], |command| {
            without_key(command)
                .env(TOKEN_FILE_VARIABLE, files.path().join("missing"))
                .env(TOKEN_VARIABLE, "")
        }),
        "withdraw",
    );
    assert_eq!(control_names(&run), Vec::<String>::new());
    assert_eq!(stub.bodies(), Vec::<Value>::new());

    let output = tree.run_adjusted(&stub, &["withdraw", path, "--json"], |command| {
        without_key(command).env(TOKEN_VARIABLE, "variable-token")
    });
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(run_record(&run)["outcome"], "withdrawn");
    let writes = mirror_writes(&run);
    let (name, write) = writes.last().unwrap();
    assert_eq!(name, "0004-outcome.json");
    assert_eq!(write["outcome"], "written", "{write}");
    assert_eq!(stub.operations(), ["IssueState", "UpdateIssueState"]);
    assert_eq!(stub.authorizations(), ["Bearer variable-token"; 2]);
}

// REQ-LOCALRUN-204, REQ-LOCALRUN-186, REQ-CONTINUE-2: `cancel` of a mirrored `running` run
// whose process is gone, from a shell without the key, is refused and leaves the abandoned
// attempt unsettled, so the same `cancel` with the key ends the run.
#[test]
fn ending_a_mirrored_abandoned_run_without_the_key_is_refused_before_anything_changes() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    let stub = LinearStub::start();

    let mut live = tree.held(&stub, "AUTO-2");
    let shown = tree.run(&stub, &["show", "AUTO-2", "--json"]);
    assert_eq!(shown.status.code(), Some(0), "{shown:?}");
    let shown: Value = serde_json::from_slice(&shown.stdout).unwrap();
    let run = PathBuf::from(shown["runDirectory"].as_str().unwrap());
    live.process.kill().unwrap();
    live.process.wait().unwrap();
    assert_eq!(run_record(&run)["state"], "running");
    fs::write(&live.release, "").unwrap();

    // The orphaned step process ends first; until it does, the attempt's ownership is
    // unproven and the command fails without a document.
    let path = run.to_str().unwrap();
    let output = super::poll_until(
        "the abandoned run's refused cancel",
        || tree.run_adjusted(&stub, &["cancel", path, "--json"], without_key),
        |output| {
            serde_json::from_slice::<Value>(&output.stdout)
                .is_ok_and(|document| document["outcome"] != Value::Null)
        },
    );
    refused_without_key(&output, "cancel");
    assert_eq!(run_record(&run)["state"], "running");
    assert_eq!(control_names(&run), Vec::<String>::new());
    assert!(
        !mirror_names(&run)
            .iter()
            .any(|name| name.ends_with("-outcome.json")),
        "{:?}",
        mirror_names(&run)
    );

    let output = tree.run(&stub, &["cancel", path, "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "canceled", "{document}");
    assert_eq!(run_record(&run)["outcome"], "canceled");
}

/// The first attempt of the run's `state.json`.
fn first_attempt(run: &Path) -> Value {
    read_json(&run.join("state.json"))["attempts"][0].clone()
}

// REQ-LOCALRUN-204, REQ-LOCALRUN-186: `resume` of a mirrored `running` run whose process is
// gone after a withdrawal was recorded, from a shell without the key, is refused at phase
// `continuation` and leaves the abandoned attempt unsettled with the control kept; the same
// `resume` with the key withdraws the run and writes its `onOutcome.withdrawn` once.
#[test]
fn resuming_an_abandoned_run_whose_withdrawal_is_recorded_without_the_key_is_refused() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    let stub = LinearStub::start();

    let mut live = tree.held(&stub, "AUTO-5");
    let withdrawn = super::poll_until(
        "the live run's withdraw",
        || tree.run(&stub, &["withdraw", "AUTO-5", "--json"]),
        |output| output.status.code() == Some(0),
    );
    let document: Value = serde_json::from_slice(&withdrawn.stdout).unwrap();
    assert_eq!(document["outcome"], "recorded", "{document}");
    let run = PathBuf::from(document["runDirectory"].as_str().unwrap());
    live.process.kill().unwrap();
    live.process.wait().unwrap();
    assert_eq!(run_record(&run)["state"], "running");
    fs::write(&live.release, "").unwrap();
    let attempt = first_attempt(&run);
    assert_eq!(attempt.get("settledAt"), None, "{attempt}");
    stub.clear();

    // The orphaned step process ends first; until it does, the attempt's ownership is
    // unproven and the command fails without the refusal.
    let path = run.to_str().unwrap();
    let output = super::poll_until(
        "the abandoned run's refused resume",
        || tree.run_adjusted(&stub, &["resume", path, "--json"], without_key),
        |output| {
            output.status.code() == Some(0)
                || serde_json::from_slice::<Value>(&output.stdout).is_ok_and(|document| {
                    document["diagnostics"][0]["code"] == "mirror_credentials_unavailable"
                })
        },
    );
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "rejected", "{document}");
    assert_eq!(document["phase"], "continuation", "{document}");
    assert_eq!(
        document["diagnostics"].as_array().unwrap().len(),
        1,
        "{document}"
    );
    assert_eq!(run_record(&run)["state"], "running");
    assert_eq!(first_attempt(&run), attempt);
    assert_eq!(control_names(&run), ["0001-withdraw.json"]);
    assert!(
        !mirror_names(&run)
            .iter()
            .any(|name| name.ends_with("-outcome.json")),
        "{:?}",
        mirror_names(&run)
    );
    assert_eq!(stub.bodies(), Vec::<Value>::new());

    let output = tree.run(&stub, &["resume", path, "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "withdrawn", "{document}");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "withdrawn", "{record}");
    assert_eq!(record["attempts"], 1, "{record}");
    let attempt = first_attempt(&run);
    assert!(attempt["settledAt"].is_string(), "{attempt}");
    assert_eq!(control_names(&run), ["0001-withdraw.json"]);
    let outcomes = mirror_writes(&run)
        .into_iter()
        .filter(|(name, _)| name.ends_with("-outcome.json"))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(outcomes[0].1["outcome"], "written", "{outcomes:?}");
    assert_eq!(
        outcomes[0].1["action"],
        json!({"kind": "state", "state": "Backlog", "binding": "backlog"})
    );
}

// REQ-LOCALRUN-204: a mirror whose `onOutcome` does not map the outcome writes nothing, so
// `withdraw` without the key ends the run.
#[test]
fn a_mirror_that_does_not_map_the_outcome_needs_no_key() {
    let withdrawn = "      withdrawn: {state: $backlog, unless: issue.state != $ownedState}\n";
    assert!(MIRRORED_WAIT_HEADER.contains(withdrawn));
    let tree = MirroredWaitTree::publish(&MIRRORED_WAIT_HEADER.replacen(withdrawn, "", 1));
    let stub = LinearStub::start();

    let run = tree.waiting(&stub, "AUTO-3");
    let writes = mirror_names(&run);
    let output = tree.run_adjusted(
        &stub,
        &["withdraw", run.to_str().unwrap(), "--json"],
        without_key,
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "withdrawn", "{document}");
    assert_eq!(run_record(&run)["outcome"], "withdrawn");
    assert_eq!(mirror_names(&run), writes);
}

// REQ-LOCALRUN-204, REQ-CONTINUE-75: `resume` of a mirrored run whose repair wait a recorded
// `control.withdraw` resolves, from a shell without the key, is refused at phase
// `continuation` and keeps waiting with the control recorded; the same `resume` with the key
// withdraws the run and moves its issue.
#[test]
fn resuming_a_recorded_withdrawal_without_the_key_is_refused_and_keeps_the_run_waiting() {
    let tree = MirroredWaitTree::publish(MIRRORED_WAIT_HEADER);
    let stub = LinearStub::start();

    let run = tree.waiting_after(&stub, "AUTO-4", "failed", "repair");
    let wait = fs::read(run.join("wait.json")).unwrap();
    append_control(
        &run,
        &control(
            &run,
            "control.withdraw",
            Some(&read_json(&run.join("wait.json"))),
            json!({}),
        ),
    );
    stub.clear();
    let path = run.to_str().unwrap();

    let output = tree.run_adjusted(&stub, &["resume", path, "--json"], without_key);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "rejected", "{document}");
    assert_eq!(document["phase"], "continuation", "{document}");
    let diagnostics = document["diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 1, "{document}");
    assert_eq!(
        diagnostics[0]["code"], "mirror_credentials_unavailable",
        "{document}"
    );
    assert_eq!(run_record(&run)["state"], "waiting");
    assert_eq!(fs::read(run.join("wait.json")).unwrap(), wait);
    assert_eq!(control_names(&run), ["0001-withdraw.json"]);
    assert!(
        !mirror_names(&run)
            .iter()
            .any(|name| name.ends_with("-outcome.json")),
        "{:?}",
        mirror_names(&run)
    );
    assert_eq!(stub.bodies(), Vec::<Value>::new());

    let output = tree.run(&stub, &["resume", path, "--json"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["outcome"], "withdrawn", "{document}");
    let record = run_record(&run);
    assert_eq!(record["state"], "exited", "{record}");
    assert_eq!(record["outcome"], "withdrawn", "{record}");
    let writes = mirror_writes(&run);
    let (name, write) = writes.last().unwrap();
    assert!(name.ends_with("-outcome.json"), "{writes:?}");
    assert_eq!(write["outcome"], "written", "{write}");
    assert_eq!(
        write["action"],
        json!({"kind": "state", "state": "Backlog", "binding": "backlog"})
    );
}
