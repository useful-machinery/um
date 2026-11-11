//! The run mirror end to end (REQ-LOCALRUN-191, REQ-LOCALRUN-192, REQ-LOCALRUN-186): `um`
//! performs a root workflow's `mirror` writes against a loopback stand-in for Linear's GraphQL
//! API and records each under the run's `mirror/`.

use std::fs;
use std::io::{BufRead as _, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use super::workflow_continue::{WAIT_TREE_ROOT, completed, decide, wait_tree};
use super::workflow_definition::{Home, Tree, read_json};
use super::workflow_run::{TreeBundle, isolated_command, run_record};
use super::workflow_run_create::Published;

/// The login name the mirrored runs are created by, which `{{operator}}` renders.
pub(super) const MIRROR_OPERATOR: &str = "mirror-operator";

/// The team states the stub's issue offers, by name and id.
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
/// its current state and the team's `STATES`, and each mutation with `success` true, or with
/// HTTP 500 and a GraphQL error while it fails mutations. It records every body in order.
pub(super) struct LinearStub {
    base: String,
    state: Arc<Mutex<StubState>>,
}

struct StubState {
    issue_state: String,
    failing: bool,
    bodies: Vec<Value>,
}

impl LinearStub {
    pub(super) fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(StubState {
            issue_state: "Engineering".to_owned(),
            failing: false,
            bodies: Vec::new(),
        }));
        let served = Arc::clone(&state);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let Some((path, body)) = read_request(&stream) else {
                    continue;
                };
                let (status, answer) = served.lock().unwrap().answer(&path, body);
                let answer = serde_json::to_vec(&answer).unwrap();
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    answer.len()
                );
                let _ = stream.write_all(&answer);
            }
        });
        Self { base, state }
    }

    /// The state name `IssueState` answers from now on.
    pub(super) fn issue_state(&self, state: &str) {
        state.clone_into(&mut self.state.lock().unwrap().issue_state);
    }

    /// Whether every mutation is answered HTTP 500 with a GraphQL error.
    pub(super) fn fail_mutations(&self, failing: bool) {
        self.state.lock().unwrap().failing = failing;
    }

    /// Every request body served so far, in order.
    pub(super) fn bodies(&self) -> Vec<Value> {
        self.state.lock().unwrap().bodies.clone()
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
        self.state.lock().unwrap().bodies.clear();
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
        match (path, operation.as_str()) {
            ("/graphql", "IssueState") => (
                "200 OK",
                json!({"data": {"issue": {
                    "id": "issue-1",
                    "identifier": identifier,
                    "state": {"id": "current", "name": self.issue_state},
                    "team": {"states": {"nodes": STATES
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

/// The request's path and JSON body, read by its `Content-Length`.
fn read_request(stream: &std::net::TcpStream) -> Option<(String, Value)> {
    let mut reader = std::io::BufReader::new(stream.try_clone().ok()?);
    let mut head = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 || line == "\r\n" {
            break;
        }
        head.push_str(&line);
    }
    let path = head.split(' ').nth(1)?.to_owned();
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    Some((path, serde_json::from_slice(&body).unwrap_or(Value::Null)))
}

/// The engineering tree whose root's steps are `DESIGN_STAGE_STEPS`, with or without its
/// `mirror` block, published.
fn engineering(mirrored: bool) -> Published {
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
    Published::publish(tree)
}

/// `um workflow run create` of the definition for `ticket`, its command adjusted by `adjust`:
/// the run directory.
fn create(
    published: &Published,
    ticket: &str,
    adjust: impl FnOnce(&mut Command) -> &mut Command,
) -> PathBuf {
    let execution_root = published.execution_root();
    let mut command = published.command(&[
        "create",
        &published.id,
        "--execution-root",
        execution_root.to_str().unwrap(),
        "--input-text",
        "ticket",
        ticket,
        "--json",
    ]);
    let terminal = completed(&adjust(&mut command).output().unwrap());
    PathBuf::from(terminal["runDirectory"].as_str().unwrap())
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
    fn publish() -> Self {
        let bundle = wait_tree();
        bundle.write_source(
            "workflow.yaml",
            &WAIT_TREE_ROOT.replacen("schemaVersion: 1\n", MIRRORED_WAIT_HEADER, 1),
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
        let mut full = vec!["workflow".to_owned(), "run".to_owned()];
        full.extend(args.iter().map(|arg| (*arg).to_owned()));
        let mut command = isolated_command(&full);
        command.env("UM_HOME", self.home.path());
        stub.point(&mut command).output().unwrap()
    }

    /// A run for `ticket` whose `design` waits on an answer.
    fn waiting(&self, stub: &LinearStub, ticket: &str) -> PathBuf {
        decide(&self.bundle, "design", "blocked");
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
        assert_eq!(terminal["wait"]["kind"], "answer", "{terminal}");
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
    let tree = MirroredWaitTree::publish();
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
