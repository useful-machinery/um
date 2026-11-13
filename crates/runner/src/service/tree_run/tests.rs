//! The tree-run job against an injected `um` executable and a fake capability server.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ring::digest::{SHA256, digest};
use serde_json::{Value, json};
use url::Url;

use super::records::conclude;
use super::resume_loop::ResumeIntervals;
use super::{HttpTreeBroker, TransportResult, TreeConclusion, TreeRunDependencies};
use crate::service::assignment::test_support::{
    cleanup_failed, control_lease_clock, enqueue_transitions, finished_reports,
    keep_agent_logins_in, prepare_continued, prepare_offered, settled_observations, start_offered,
    start_offered_with, tree_run_manager, wait_for_reports,
};
use crate::service::assignment::{
    AssignmentCancel, AssignmentManager, AssignmentObservation, AssignmentOffer, AssignmentRelease,
    AssignmentRunControl, AssignmentTriggerDelivery, ExecutionReport, MAXIMUM_SERVICE_OBSERVATIONS,
    OBSERVATION_RESERVE_BASE,
};
use crate::service::config::{AssignmentConfig, Config, RepositoryUrlPolicy};
use um_execution::{
    CancellationReason, EnvironmentSnapshot, ResolvedAttachment, ResolvedFile, ResolvedInput,
    ResolvedInputs, ResolvedJsonInput,
};
use um_runner_protocol::{
    AssignmentDecline, CancellationMode, CloudFrame, ContinuationOffer, ExecutionSpecInvalidReason,
    RunControlApplication, RunControlOutcome, RunControlSkip, RunnerEnvelope, RunnerFrame,
    RunnerUnableReason, TreeExit, TriggerDelivery, TriggerDeliveryAction,
    TriggerDeliveryApplication, TriggerDeliveryOutcome, TriggerIssueObservation,
    WorkflowTreeFileV1RunnerProjection, decode_cloud_frame, encode_runner_frame,
};

const TREE_DIGEST: &str = "sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945";
const RUN_URL: &str = "https://usefulmachinery.com/acme/runs/run_01k0z6r1w8f4jy2m7q9v3x5abp";

/// The injected `um`: answers `definition publish --json` with revision 1 of
/// `$TREE_TEST_DIGEST`, making its revision directory as the local publish does, records
/// each command line under `$TREE_TEST_LOG`, and for `run create` copies the revision's
/// `resolution.json` there when one exists, copies the directory `$TREE_TEST_MIRROR`, when
/// set, as the run's `mirror/`, and leaves a `run.json` reading `$TREE_TEST_STATE` and
/// `$TREE_TEST_OUTCOME`, answering with `$TREE_TEST_RESULT` as the document's `result` when
/// it is set: for state `waiting`
/// beside a `wait.json` of kind `$TREE_TEST_WAIT_KIND` whose rule names
/// `$TREE_TEST_WAIT_SOURCE`, and for state `cancellable` a running record until `run
/// cancel` ends it `canceled`; for state `refused` it makes no run and answers with the
/// runtime's rejection of a root whose `requires.tools` names a missing tool. Each `run resume` appends its command line to
/// `resume.args`, leaves its environment in `resume.env` and answers with the next word of
/// `$TREE_TEST_RESUMES`, or with the word in `$TREE_TEST_LOG/answer` while that file
/// exists: `held` (the default) reports the wait unchanged, `unavailable`
/// refuses with `wait_condition_unavailable`, `refuse` refuses with
/// `continuation_nodes_unsatisfied`, `stall` starts nothing and prints nothing until `run
/// cancel` runs, and `continue` starts the record's next attempt at
/// `merge`, then lands the run once `$TREE_TEST_LOG/proceed` exists. `run cancel` ends a waiting
/// record `canceled`. A stage `$TREE_TEST_LOG/stages/<name>/` is copied into the run
/// directory: `create` before the first record is written, `exited` after `create` writes
/// an `exited` record, `resume<n>` by the n-th `resume` (after `continue` writes attempt 2)
/// and `landed` before the run lands.
/// `run event` copies its event and claim files to the log and answers by
/// `$TREE_TEST_EVENT`: `cancel` applies `cancel`, ending the run `canceled` and signalling
/// a `cancellable` process as the live run's cancellation does, `withdraw` applies
/// `withdraw` with the JSON `$TREE_TEST_ACTION_OUTCOME` as its `actionOutcome`, leaving the
/// record as it is, `not_later`
/// refuses `event_not_later` before any read, and `owned` refuses `issue_in_owned_state`
/// after one. `run stop` and `run lift` record their command lines; `run answer`, `approve`
/// and `hand-back` append theirs to `control.args` and answer `recorded`, or, with
/// `$TREE_TEST_CONTROL` `refuse`, refuse `question_unknown`, with `hang` hang until killed,
/// and with `record_hang` (`approve` and `hand-back`) record their control for the run's
/// wait under `controls/` and then hang until killed; `run retry` appends its command
/// line there too and then answers as `resume`. `event`, `stop`, `lift`, `answer`,
/// `approve`, `hand-back`, `retry` and `resume` (but not a retry's) each append their name
/// to `$TREE_TEST_LOG/order`. `run event` with `$TREE_TEST_EVENT` `hang` starts a `sleep`
/// beside it in its process group, writes the sleep's pid to `$TREE_TEST_LOG/hang.pid` and
/// waits for it. While `$TREE_TEST_LOG/noise` exists every command first writes that many
/// bytes to stderr. While `$TREE_TEST_LOG/unlaunchable` exists `run create` removes the
/// injected `um` itself, so no later command can be launched.
const UM_FIXTURE: &str = r#"#!/bin/sh
log="$TREE_TEST_LOG"
if [ -f "$log/noise" ]; then head -c "$(cat "$log/noise")" /dev/zero >&2; fi
stage() {
  if [ -d "$log/stages/$1" ]; then cp -R "$log/stages/$1/." "$run/"; fi
}
recorded() {
  printf '{"schemaVersion":1,"command":"um workflow run %s","outcome":"recorded","exitStatus":0,"runDirectory":"%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5loc","control":{"ordinal":1,"event":"control.%s","path":"controls/0001-%s.json","wait":null},"wait":null}\n' "$1" "$2" "$1" "$1"
}
retrying=
if [ "$1 $2 $3" = "workflow run retry" ]; then
  printf '%s\n' "$*" >> "$log/control.args"
  echo retry >> "$log/order"
  retrying=1
  set -- workflow run resume "$4" --json
fi
case "$1 $2 $3" in
"workflow definition publish")
  printf '%s\n' "$@" > "$log/publish.args"
  env > "$log/publish.env"
  if test -x "$4/bin/check.sh"; then echo executable; else echo plain; fi > "$log/check.mode"
  if test -x "$4/workflow.yaml"; then echo executable; else echo plain; fi > "$log/workflow.mode"
  cp "$6" "$log/bindings.json"
  mkdir -p -m 700 "$UM_HOME/workflows/def_01k0z6r1w8f4jy2m7q9v3x5loc/revisions/1"
  printf '{"definition":{"id":"def_01k0z6r1w8f4jy2m7q9v3x5loc"},"revision":{"n":1,"tree":"%s"}}\n' "$TREE_TEST_DIGEST"
  ;;
"workflow run create")
  printf '%s\n' "$@" > "$log/create.args"
  env > "$log/create.env"
  if [ -e "$log/unlaunchable" ]; then rm -f "$0"; fi
  if [ "$TREE_TEST_STATE" = refused ]; then
    printf '%s\n' '{"schemaVersion":1,"command":"um workflow run create","outcome":"rejected","exitStatus":1,"phase":"admission","workflow":{"path":"workflow.yaml"},"diagnostics":[{"code":"required_tool_unavailable","message":"Install the tool the root workflow'"'"'s `requires.tools` names, or put it on the launching `PATH`.","location":{"kind":"required_tool","tool":"um-test-no-such-tool"}}]}'
    exit 1
  fi
  resolution="$UM_HOME/workflows/def_01k0z6r1w8f4jy2m7q9v3x5loc/revisions/1/resolution.json"
  if test -e "$resolution"; then cp "$resolution" "$log/resolution.json"; fi
  run="$UM_HOME/workflows/def_01k0z6r1w8f4jy2m7q9v3x5loc/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc"
  mkdir -p "$run"
  if test -n "$TREE_TEST_MIRROR"; then cp -R "$TREE_TEST_MIRROR" "$run/mirror"; fi
  stage create
  case "$TREE_TEST_STATE" in
  cancellable)
    printf '{"state":"running","outcome":null,"attempts":1,"position":["build"]}\n' > "$run/run.json"
    n=0
    while [ ! -e "$log/cancelled" ] && [ "$n" -lt 400 ]; do sleep 0.05; n=$((n + 1)); done
    printf '{"state":"exited","outcome":"canceled","attempts":1,"position":[]}\n' > "$run/run.json"
    ;;
  waiting)
    printf '{"kind":"%s","since":"2026-07-23T00:00:05Z","step":["merge"],"then":"merge","rule":{"source":"%s","event":"pull_request.merged"},"payload":{}}\n' "$TREE_TEST_WAIT_KIND" "$TREE_TEST_WAIT_SOURCE" > "$run/wait.json"
    printf '{"state":"waiting","outcome":null,"attempts":1,"position":["merge"]}\n' > "$run/run.json"
    ;;
  exited)
    printf '{"state":"exited","outcome":%s,"attempts":1,"position":[]}\n' "$TREE_TEST_OUTCOME" > "$run/run.json"
    stage exited
    ;;
  *)
    printf '{"state":"%s","outcome":%s,"attempts":1,"position":["build"]}\n' "$TREE_TEST_STATE" "$TREE_TEST_OUTCOME" > "$run/run.json"
    ;;
  esac
  result="$TREE_TEST_RESULT"
  [ -n "$result" ] || result='{}'
  printf '{"runDirectory":"%s","result":%s}\n' "$run" "$result"
  ;;
"workflow run event")
  printf '%s\n' "$@" >> "$log/event.args"
  echo event >> "$log/order"
  cp "$6" "$log/event.json"
  cp "$8" "$log/claim.json"
  case "$TREE_TEST_EVENT" in
  cancel)
    : > "$log/cancelled"
    rm -f "$4/wait.json"
    printf '{"state":"exited","outcome":"canceled","attempts":1,"position":[]}\n' > "$4/run.json"
    printf '{"schemaVersion":1,"command":"um workflow run event","outcome":"applied","exitStatus":0,"runDirectory":"%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5loc","trigger":"departed","action":"cancel","actionOutcome":"canceled","control":{"ordinal":1,"event":"cancel","path":"controls/0001-cancel.json","wait":null},"observation":{"team":"team_8c1b","state":"state_canceled","updatedAt":"2026-07-23T00:00:07.512Z"}}\n' "$4"
    ;;
  withdraw)
    printf '{"schemaVersion":1,"command":"um workflow run event","outcome":"applied","exitStatus":0,"runDirectory":"%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5loc","trigger":"departed","action":"withdraw","actionOutcome":%s,"control":{"ordinal":1,"event":"withdraw","path":"controls/0001-withdraw.json","wait":null},"observation":{"team":"team_8c1b","state":"state_canceled","updatedAt":"2026-07-23T00:00:07.512Z"}}\n' "$4" "$TREE_TEST_ACTION_OUTCOME"
    ;;
  hang)
    sleep 60 &
    printf '%s\n' "$!" > "$log/hang.pid"
    wait
    ;;
  owned)
    printf '{"schemaVersion":1,"command":"um workflow run event","outcome":"refused","exitStatus":1,"runDirectory":"%s","diagnostics":[{"code":"issue_in_owned_state","message":"owned"}],"observation":{"team":"team_8c1b","state":"state_build","updatedAt":null}}\n' "$4"
    exit 1
    ;;
  *)
    printf '{"schemaVersion":1,"command":"um workflow run event","outcome":"refused","exitStatus":1,"runDirectory":"%s","diagnostics":[{"code":"event_not_later","message":"not later"}],"observation":null}\n' "$4"
    exit 1
    ;;
  esac
  ;;
"workflow run stop")
  printf '%s\n' "$@" > "$log/stop.args"
  echo stop >> "$log/order"
  recorded stop "$4"
  ;;
"workflow run lift")
  printf '%s\n' "$@" > "$log/lift.args"
  echo lift >> "$log/order"
  recorded lift "$4"
  ;;
"workflow run answer"|"workflow run approve"|"workflow run hand-back")
  printf '%s\n' "$*" >> "$log/control.args"
  echo "$3" >> "$log/order"
  if [ "$TREE_TEST_CONTROL" = refuse ]; then
    printf '{"schemaVersion":1,"command":"um workflow run %s","outcome":"refused","exitStatus":1,"runDirectory":"%s","diagnostics":[{"code":"question_unknown","message":"unknown"}]}\n' "$3" "$4"
    exit 1
  fi
  if [ "$TREE_TEST_CONTROL" = record_hang ]; then
    name=$(printf '%s' "$3" | tr - _)
    mkdir -p "$4/controls"
    printf '{"source":"um","event":"control.%s","run":"run_01k0z6r1w8f4jy2m7q9v3x5abp","by":"cloud","at":"2026-07-23T00:00:06Z","wait":"%s@2026-07-23T00:00:05Z"}\n' "$name" "$TREE_TEST_WAIT_KIND" > "$4/controls/0001-$name.json"
  fi
  case "$TREE_TEST_CONTROL" in
  hang|record_hang) sleep 60 & wait ;;
  esac
  recorded "$3" "$4"
  ;;
"workflow run resume")
  run="$4"
  [ -n "$retrying" ] || echo resume >> "$log/order"
  printf '%s\n' "$run" >> "$log/resume.log"
  printf '%s\n' "$*" >> "$log/resume.args"
  env > "$log/resume.env"
  n=$(wc -l < "$log/resume.log" | tr -d ' ')
  answer=$(printf '%s\n' $TREE_TEST_RESUMES | sed -n "${n}p")
  if [ -f "$log/answer" ]; then answer=$(cat "$log/answer"); fi
  case "$answer" in
  continue)
    rm -f "$run/wait.json"
    next=$(($(sed -n 's/.*"attempts":\([0-9]*\).*/\1/p' "$run/run.json") + 1))
    printf '{"state":"running","outcome":null,"attempts":%s,"position":["merge"]}\n' "$next" > "$run/run.json"
    stage "resume$n"
    i=0
    while [ ! -e "$log/proceed" ] && [ "$i" -lt 400 ]; do sleep 0.05; i=$((i + 1)); done
    stage landed
    printf '{"state":"exited","outcome":"landed","attempts":%s,"position":[]}\n' "$next" > "$run/run.json"
    printf '{"outcome":"succeeded","runDirectory":"%s","result":{}}\n' "$run"
    ;;
  refuse)
    printf '{"schemaVersion":1,"outcome":"rejected","exitStatus":1,"phase":"continuation","runDirectory":"%s","diagnostics":[{"code":"continuation_nodes_unsatisfied","message":"refused"}]}\n' "$run"
    exit 1
    ;;
  unavailable)
    stage "resume$n"
    printf '{"schemaVersion":1,"outcome":"rejected","exitStatus":1,"phase":"continuation","runDirectory":"%s","attemptNumber":1,"diagnostics":[{"code":"wait_condition_unavailable","message":"unavailable"}]}\n' "$run"
    exit 1
    ;;
  stall)
    i=0
    while [ ! -e "$log/cancelled" ] && [ "$i" -lt 400 ]; do sleep 0.05; i=$((i + 1)); done
    exit 1
    ;;
  *)
    stage "resume$n"
    printf '{"schemaVersion":1,"outcome":"waiting","exitStatus":0,"runDirectory":"%s","attemptNumber":1}\n' "$run"
    ;;
  esac
  ;;
"workflow run cancel")
  printf '%s\n' "$@" > "$log/cancel.args"
  : > "$log/cancelled"
  if grep -q '"state":"waiting"' "$4/run.json"; then
    rm -f "$4/wait.json"
    # Replaced whole, as the runtime writes it, so a `cancel` killed midway leaves it readable.
    printf '{"state":"exited","outcome":"canceled","attempts":1,"position":[]}\n' > "$4/run.json.next"
    mv "$4/run.json.next" "$4/run.json"
  fi
  ;;
esac
"#;

struct TreeFile {
    path: &'static str,
    bytes: &'static [u8],
    executable: bool,
}

const TREE_FILES: &[TreeFile] = &[
    TreeFile {
        path: "workflow.yaml",
        bytes: b"schemaVersion: 1\nname: build\nsteps:\n  build:\n    kind: cmd\n    command:\n      argv: [/bin/sh, -c, 'exit 0']\n  review:\n    kind: cmd\n    dependsOn: [build]\n    command:\n      argv: [/bin/sh, -c, 'exit 0']\n",
        executable: false,
    },
    TreeFile {
        path: "bin/check.sh",
        bytes: b"#!/bin/sh\nexit 0\n",
        executable: true,
    },
];

fn sha256_hex(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A Gateway stand-in serving the tree capability route and the signed file URLs.
fn capability_server() -> (Url, Arc<Mutex<Vec<Value>>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&requests);
    std::thread::spawn(move || {
        for connection in listener.incoming() {
            let Ok(mut connection) = connection else {
                return;
            };
            let mut reader = BufReader::new(connection.try_clone().unwrap());
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header == "\r\n" {
                    break;
                }
                if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let target = request_line
                .split(' ')
                .nth(1)
                .unwrap_or_default()
                .to_owned();
            let (content_type, answer) = if let Some(path) = target.strip_prefix("/files/") {
                let file = TREE_FILES.iter().find(|file| file.path == path).unwrap();
                ("application/octet-stream", file.bytes.to_vec())
            } else {
                let request: Value = serde_json::from_slice(&body).unwrap();
                let files = request["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|path| {
                        let file = TREE_FILES
                            .iter()
                            .find(|file| file.path == path.as_str().unwrap())
                            .unwrap();
                        json!({
                            "path": file.path,
                            "sha256": sha256_hex(file.bytes),
                            "size": file.bytes.len(),
                            "executable": file.executable,
                            "url": format!("http://{address}/files/{}", file.path),
                        })
                    })
                    .collect::<Vec<_>>();
                let answer = json!({
                    "schemaVersion": 1,
                    "digest": request["digest"],
                    "capabilityExpiresAt": "2099-01-01T00:00:00Z",
                    "files": files,
                });
                recorded.lock().unwrap().push(request);
                ("application/json", serde_json::to_vec(&answer).unwrap())
            };
            write!(
                connection,
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                answer.len()
            )
            .unwrap();
            connection.write_all(&answer).unwrap();
        }
    });
    (
        Url::parse(&format!("ws://{address}/v1/runner/connect")).unwrap(),
        requests,
    )
}

fn run_git(repository: &Path, arguments: &[&str]) -> String {
    let output = um_test_support::fixture_git_command("git")
        .current_dir(repository)
        .args(arguments)
        .output()
        .unwrap();
    assert!(output.status.success(), "fixture git command failed");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

/// The resume loop's intervals in the tests, in milliseconds.
const TEST_INTERVALS: ResumeIntervals = ResumeIntervals {
    held: Duration::from_millis(20),
    unavailable: Duration::from_millis(40),
    record: Duration::from_millis(10),
};

/// The protocol fixture's offer of a published tree.
fn tree_offer() -> CloudFrame {
    decode_cloud_frame(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-workflow-tree.json"
    )))
    .unwrap()
}

/// Writes the injected `um` to `directory`.
fn injected_um(directory: &Path) -> PathBuf {
    let um = directory.join("um");
    fs::write(&um, UM_FIXTURE).unwrap();
    fs::set_permissions(&um, fs::Permissions::from_mode(0o755)).unwrap();
    um
}

struct Fixture {
    _temporary: tempfile::TempDir,
    /// The repository every workspace checkout clones.
    source: PathBuf,
    manager: AssignmentManager,
    offer: AssignmentOffer,
    log: PathBuf,
    capability_requests: Arc<Mutex<Vec<Value>>>,
}

/// A manager whose next offer is a tree run of `TREE_FILES`, the injected `um`
/// reporting `published_digest` and leaving a record in `state` with `outcome`.
fn fixture(published_digest: &str, state: &str, outcome: &str) -> Fixture {
    fixture_with(published_digest, state, outcome, &[])
}

/// `fixture`, with `variables` added to the daemon's environment and the resume loop's
/// intervals in milliseconds.
fn fixture_with(
    published_digest: &str,
    state: &str,
    outcome: &str,
    variables: &[(&str, &str)],
) -> Fixture {
    fixture_recording(published_digest, state, outcome, variables, None)
}

/// `fixture_with`, the manager recording its telemetry to `recorder`.
fn fixture_recording(
    published_digest: &str,
    state: &str,
    outcome: &str,
    variables: &[(&str, &str)],
    recorder: Option<Arc<crate::telemetry::Recorder>>,
) -> Fixture {
    fixture_bounded(
        published_digest,
        state,
        outcome,
        variables,
        recorder,
        super::command::SHORT_COMMAND_BOUND,
    )
}

/// `fixture_recording`, each short `um` command abandoned once `bound` passes.
fn fixture_bounded(
    published_digest: &str,
    state: &str,
    outcome: &str,
    variables: &[(&str, &str)],
    recorder: Option<Arc<crate::telemetry::Recorder>>,
    bound: Duration,
) -> Fixture {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    let work = temporary.path().join("work");
    let log = temporary.path().join("log");
    for directory in [&source, &work, &log] {
        fs::create_dir(directory).unwrap();
    }
    fs::set_permissions(&work, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(source.join("README"), "workspace\n").unwrap();
    run_git(&source, &["init", "--quiet", "--object-format=sha1"]);
    run_git(&source, &["config", "user.name", "Tree Fixture"]);
    run_git(
        &source,
        &["config", "user.email", "fixture@usefulmachinery.invalid"],
    );
    run_git(&source, &["add", "."]);
    run_git(&source, &["commit", "--quiet", "-m", "fixture"]);
    let commit = run_git(&source, &["rev-parse", "HEAD"]);
    let um = injected_um(temporary.path());

    let (endpoint, capability_requests) = capability_server();
    let config = Config::new(
        "wss://gateway.example.test/v1/runner/connect",
        crate::credential::test_credential(),
        false,
        AssignmentConfig::new(&work).unwrap(),
        RepositoryUrlPolicy::with_file_repositories(true),
    )
    .unwrap();
    let broker = HttpTreeBroker::new(
        &endpoint,
        &crate::credential::test_credential(),
        "rbt_01k0z6r1w8f4jy2m7q9v3x5abe",
    )
    .unwrap();
    let environment = EnvironmentSnapshot::new(
        [
            ("PATH", std::env::var_os("PATH").unwrap_or_default()),
            ("TREE_TEST_LOG", log.clone().into_os_string()),
            ("TREE_TEST_DIGEST", published_digest.into()),
            ("TREE_TEST_STATE", state.into()),
            ("TREE_TEST_OUTCOME", outcome.into()),
            ("GITHUB_TOKEN", "daemon-github-token".into()),
        ]
        .into_iter()
        .chain(
            variables
                .iter()
                .map(|(name, value)| (*name, std::ffi::OsString::from(value))),
        ),
    );
    let manager = tree_run_manager(
        &config,
        &source,
        TreeRunDependencies::new(Some(Arc::new(broker)))
            .with_executable(um)
            .with_resume_intervals(TEST_INTERVALS)
            .with_command_bound(bound),
        environment,
        recorder,
    );

    let CloudFrame::AssignmentOffer {
        effect_id,
        assignment_id,
        run_id,
        project_id,
        attempt_id,
        attempt_number,
        execution_spec,
        ..
    } = tree_offer()
    else {
        panic!("expected an assignment offer");
    };
    let mut execution_spec = *execution_spec;
    execution_spec
        .workflow_definition_source
        .commit_oid
        .clone_from(&commit);
    execution_spec.primary_workspace_source.commit_oid = commit;
    let tree = execution_spec.workflow_tree.as_mut().unwrap();
    tree.digest = TREE_DIGEST.to_owned();
    tree.workflow_path = "workflow.yaml".to_owned();
    tree.stop_before = Some("review".to_owned());
    tree.files = TREE_FILES
        .iter()
        .map(|file| WorkflowTreeFileV1RunnerProjection {
            path: file.path.to_owned(),
            sha256: sha256_hex(file.bytes),
            size: file.bytes.len() as u64,
            executable: file.executable,
        })
        .collect();
    Fixture {
        _temporary: temporary,
        source,
        manager,
        offer: AssignmentOffer {
            effect_id,
            assignment_id,
            run_id,
            project_id,
            attempt_id,
            attempt_number,
            execution_spec,
            continuation: None,
        },
        log,
        capability_requests,
    }
}

fn logged_lines(log: &Path, name: &str) -> Vec<String> {
    fs::read_to_string(log.join(name))
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

async fn execute(fixture: &mut Fixture) -> Vec<ExecutionReport> {
    assert_eq!(
        prepare_offered(&mut fixture.manager, &fixture.offer).await,
        None
    );
    start_offered(&mut fixture.manager, &fixture.offer).spawn();
    finished_reports(&mut fixture.manager).await
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-99
#[tokio::test]
async fn published_tree_runs_once_and_reports_the_chosen_outcome() {
    let mut fixture = fixture(TREE_DIGEST, "exited", "\"planned\"");
    let reports = execute(&mut fixture).await;

    let requested = fixture.capability_requests.lock().unwrap().clone();
    assert_eq!(requested.len(), 1);
    assert_eq!(requested[0]["digest"], TREE_DIGEST);
    assert_eq!(
        requested[0]["files"],
        json!(["bin/check.sh", "workflow.yaml"])
    );
    assert_eq!(logged_lines(&fixture.log, "check.mode"), ["executable"]);
    assert_eq!(logged_lines(&fixture.log, "workflow.mode"), ["plain"]);

    let private = format!("/{}/private", fixture.offer.assignment_id);
    let publish = logged_lines(&fixture.log, "publish.args");
    assert_eq!(publish[..3], ["workflow", "definition", "publish"]);
    assert!(publish[3].ends_with(&format!("{private}/tree")));
    assert_eq!(publish[4], "--bindings");
    assert_eq!(publish[6], "--json");
    let bindings: Value =
        serde_json::from_slice(&fs::read(fixture.log.join("bindings.json")).unwrap()).unwrap();
    assert_eq!(
        bindings["linearTeam"],
        json!({"kind": "linear.team", "value": "ENG"})
    );
    let publish_env = logged_lines(&fixture.log, "publish.env");
    assert!(
        publish_env
            .iter()
            .any(|line| line.starts_with("UM_HOME=") && line.ends_with(&format!("{private}/home")))
    );

    let create = logged_lines(&fixture.log, "create.args");
    assert_eq!(
        create[..4],
        [
            "workflow",
            "run",
            "create",
            "def_01k0z6r1w8f4jy2m7q9v3x5loc"
        ]
    );
    let argument = |flag: &str| {
        create
            .iter()
            .position(|argument| argument == flag)
            .map(|index| create[index + 1].clone())
    };
    assert!(
        argument("--execution-root")
            .unwrap()
            .ends_with(&format!("/{}/workspace", fixture.offer.assignment_id))
    );
    assert!(create.contains(&"--owner=Ada Lovelace".to_owned()));
    assert_eq!(argument("--external"), Some(format!("url={RUN_URL}")));
    assert_eq!(argument("--stop-before").as_deref(), Some("review"));
    assert_eq!(create.last().map(String::as_str), Some("--json"));
    // The daemon's environment reaches the runtime whole; its own admission filters
    // each step (REQ-WFEXEC-22).
    let create_env = logged_lines(&fixture.log, "create.env");
    assert!(create_env.contains(&"GITHUB_TOKEN=daemon-github-token".to_owned()));
    assert!(create_env.iter().any(|line| line.starts_with("UM_HOME=")));

    let workflow_states = reports
        .iter()
        .filter_map(|report| match report {
            ExecutionReport::Transition { workflow_event, .. } => Some(workflow_event.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(matches!(reports.first(), Some(ExecutionReport::Started)));
    assert_eq!(workflow_states.len(), 1);
    assert_eq!(workflow_states[0]["eventType"], "workflow_state_changed");
    assert_eq!(workflow_states[0]["to"]["state"], "succeeded");
    let Some(ExecutionReport::TreeFinished {
        final_execution_event_sequence,
        outcome,
        exit,
        ..
    }) = reports.last()
    else {
        panic!("expected a tree finish: {reports:#?}");
    };
    assert_eq!(*final_execution_event_sequence, 1);
    assert_eq!(outcome["outcome"], "succeeded");
    assert_eq!(
        exit,
        &TreeExit {
            chosen_outcome: Some("planned".to_owned()),
            mirror: Vec::new(),
            claim_release: false,
        }
    );
}

// REQ-RUNNERENROLL-91, REQ-AGENTLOGIN-34
#[tokio::test]
async fn kept_agent_login_reaches_the_runtime_that_creates_the_run() {
    use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};

    const TOKEN: &str = "sk-ant-oat01-kept-claude-code-token";
    let mut fixture = fixture_with(TREE_DIGEST, "exited", "\"planned\"", &[]);
    let state = tempfile::tempdir().unwrap();
    let directory = crate::agent_login::directory::LoginDirectory::beside(
        &state.path().join("runner-state.json"),
    );
    fs::DirBuilder::new()
        .mode(0o700)
        .create(directory.path())
        .unwrap();
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.claude_code_token())
        .unwrap()
        .write_all(TOKEN.as_bytes())
        .unwrap();
    keep_agent_logins_in(&mut fixture.manager, directory);

    execute(&mut fixture).await;

    assert!(
        logged_lines(&fixture.log, "create.env")
            .contains(&format!("CLAUDE_CODE_OAUTH_TOKEN={TOKEN}"))
    );
}

// REQ-RUNNERENROLL-91
#[tokio::test]
async fn published_digest_other_than_the_spec_declines_the_assignment() {
    let other = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    let mut fixture = fixture(other, "exited", "\"planned\"");
    let decline = prepare_offered(&mut fixture.manager, &fixture.offer).await;
    assert_eq!(
        decline,
        Some(
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch
            )
            .diagnosed("workflow_tree_publication", "workflow_tree_digest_mismatch")
        )
    );
    assert!(!fixture.log.join("create.args").exists());
}

/// A mirror write as its run's `mirror/<NNNN>-<hook>.json` records it (REQ-LOCALRUN-193),
/// with `detail`'s members added.
fn mirror_write(hook: &str, action: Value, outcome: &str, detail: Value) -> Value {
    let mut write = json!({
        "schemaVersion": 1,
        "hook": hook,
        "at": "2026-07-23T00:00:05Z",
        "issue": "AUTO-143",
        "action": action,
        "outcome": outcome,
    });
    if let (Some(write), Value::Object(detail)) = (write.as_object_mut(), detail) {
        write.extend(detail);
    }
    write
}

fn comment_written() -> Value {
    mirror_write(
        "start",
        json!({"kind": "comment", "body": "Started."}),
        "written",
        json!({}),
    )
}

fn state_write(outcome: &str, detail: Value) -> Value {
    mirror_write(
        "outcome",
        json!({"kind": "state", "state": "Done", "binding": "doneState"}),
        outcome,
        detail,
    )
}

/// The `MirrorResult` a mirror write travels as: the write without `schemaVersion`.
fn mirror_result(mut write: Value) -> Value {
    write.as_object_mut().unwrap().remove("schemaVersion");
    write
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-99, REQ-WFDEF-32
#[tokio::test]
async fn resolution_is_written_before_create_and_the_finish_reports_the_mirror() {
    let mirror = tempfile::tempdir().unwrap();
    let writes = [comment_written(), state_write("written", json!({}))];
    for (name, write) in ["0001-start.json", "0002-outcome.json"].iter().zip(&writes) {
        fs::write(mirror.path().join(name), serde_json::to_vec(write).unwrap()).unwrap();
    }
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "exited",
        "\"landed\"",
        &[("TREE_TEST_MIRROR", mirror.path().to_str().unwrap())],
    );
    fixture
        .offer
        .execution_spec
        .workflow_tree
        .as_mut()
        .unwrap()
        .resolution = json!({
        "linearTeam": {"kind": "linear.team", "value": "ENG", "id": "team_8c1b", "name": "Engineering"},
        "doneState": {"kind": "linear.state", "value": "Done", "id": "state_d0e", "name": "Done"},
        "ownedState": {"kind": "linear.state", "value": "In Progress"},
        "reviewers": {"kind": "members", "value": ["prn_01k0z6r1w8f4jy2m7q9v3x5abs"]},
    });
    let reports = execute(&mut fixture).await;

    // `run create` found the record beside the revision's snapshot: the Linear keys Cloud
    // resolved to an id, each exactly `{id, name}`.
    let resolution: Value =
        serde_json::from_slice(&fs::read(fixture.log.join("resolution.json")).unwrap()).unwrap();
    assert_eq!(
        resolution,
        json!({
            "doneState": {"id": "state_d0e", "name": "Done"},
            "linearTeam": {"id": "team_8c1b", "name": "Engineering"},
        })
    );
    let Some(ExecutionReport::TreeFinished { exit, .. }) = reports.last() else {
        panic!("expected a tree finish: {reports:#?}");
    };
    assert_eq!(
        exit,
        &TreeExit {
            chosen_outcome: Some("landed".to_owned()),
            mirror: writes.into_iter().map(mirror_result).collect(),
            claim_release: true,
        }
    );
}

fn exited_with(writes: &[Value]) -> super::records::TreeRecord {
    super::records::TreeRecord {
        mirror: writes
            .iter()
            .map(|write| serde_json::from_value(write.clone()).unwrap())
            .collect(),
        ..exited("landed")
    }
}

/// `exit` on a tree run's finish, as the runner encodes it for Cloud.
fn encode_finish(exit: TreeExit) -> Result<Vec<u8>, um_runner_protocol::EncodeError> {
    encode_runner_frame(&RunnerFrame::ExecutionFinished {
        envelope: RunnerEnvelope {
            message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            sequence: 7,
            sent_at: "2026-07-23T00:00:06Z".to_owned(),
        },
        assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        final_execution_event_sequence: 1,
        outcome: TransportResult::Succeeded.outcome(),
        artifact_delivery: json!({
            "outcome": "failed",
            "phase": "preparation",
            "code": "delivery_internal_failure",
        }),
        diagnostic: None,
        tree_exit: Some(exit),
    })
}

// REQ-RUNNERENROLL-99: the runner asks for its claim's release only after its own confirmed
// departure, a written state move or a guard that held because the issue already left;
// a failed or uncertain write, or no outcome write, asks for none.
#[test]
fn claim_release_is_requested_only_after_a_confirmed_departure() {
    let failed = |code: &str| {
        state_write(
            "failed",
            json!({"code": code, "diagnostic": "The update was not confirmed."}),
        )
    };
    for (writes, release) in [
        (vec![], false),
        (vec![comment_written()], false),
        (
            vec![comment_written(), state_write("written", json!({}))],
            true,
        ),
        (
            vec![state_write("skipped", json!({"reason": "guard_held"}))],
            true,
        ),
        (vec![failed("mirror_write_rejected")], false),
        (vec![failed("mirror_unavailable")], false),
    ] {
        let TreeConclusion::Terminal { exit, .. } =
            conclude(Some(&exited_with(&writes)), None, None)
        else {
            panic!("an exited record has a result");
        };
        assert_eq!(exit.claim_release, release, "{writes:#?}");
        assert_eq!(
            exit.mirror,
            writes
                .iter()
                .cloned()
                .map(mirror_result)
                .collect::<Vec<_>>()
        );
        // Each item is a `MirrorResult` the protocol admits.
        encode_finish(exit).unwrap();
    }

    // A frame carries at most 1,024 results: the latest, whose outcome write still counts.
    let mut writes = vec![comment_written(); 1_100];
    writes.push(state_write("written", json!({})));
    let TreeConclusion::Terminal { exit, .. } = conclude(Some(&exited_with(&writes)), None, None)
    else {
        panic!("an exited record has a result");
    };
    assert_eq!(exit.mirror.len(), 1_024);
    assert_eq!(
        exit.mirror.last(),
        Some(&mirror_result(writes[1_100].clone()))
    );
    assert!(exit.claim_release);
    encode_finish(exit).unwrap();
}

/// The progress reports among `reports`: sequence, attempt, state and position.
fn progress(reports: &[ExecutionReport]) -> Vec<(u64, u64, &'static str, Value)> {
    reports
        .iter()
        .filter_map(|report| match report {
            ExecutionReport::TreeProgress {
                progress_sequence,
                progress,
                ..
            } => Some((
                *progress_sequence,
                progress.attempt,
                if progress.wait.is_some() {
                    "waiting"
                } else {
                    "running"
                },
                Value::Array(progress.position.clone()),
            )),
            _ => None,
        })
        .collect()
}

fn terminal_count(reports: &[ExecutionReport]) -> usize {
    reports.iter().filter(|report| report.is_terminal()).count()
}

fn resumes(log: &Path) -> usize {
    fs::read_to_string(log.join("resume.log")).map_or(0, |resumed| resumed.lines().count())
}

fn chosen_outcome(reports: &[ExecutionReport]) -> Option<String> {
    match reports.last() {
        Some(ExecutionReport::TreeFinished { exit, .. }) => exit.chosen_outcome.clone(),
        other => panic!("expected a tree finish: {other:#?}"),
    }
}

async fn start(fixture: &mut Fixture) {
    assert_eq!(
        prepare_offered(&mut fixture.manager, &fixture.offer).await,
        None
    );
    start_offered(&mut fixture.manager, &fixture.offer).spawn();
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-99
#[tokio::test]
async fn external_wait_is_evaluated_at_once_and_each_interval_until_it_continues() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            ("TREE_TEST_WAIT_KIND", "external"),
            ("TREE_TEST_WAIT_SOURCE", "github"),
            ("TREE_TEST_RESUMES", "held held continue"),
        ],
    );
    start(&mut fixture).await;
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, attempt, state, _)| *attempt == 2 && *state == "running")
    })
    .await;
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    let reports = finished_reports(&mut fixture.manager).await;

    assert_eq!(resumes(&fixture.log), 3);
    assert!(
        fs::read_to_string(fixture.log.join("resume.log"))
            .unwrap()
            .lines()
            .all(|run| run.ends_with("/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc"))
    );
    assert_eq!(
        progress(&reports),
        [
            (1, 1, "waiting", json!(["merge"])),
            (2, 2, "running", json!(["merge"])),
        ]
    );
    let Some(ExecutionReport::TreeProgress { progress, .. }) = reports
        .iter()
        .find(|report| matches!(report, ExecutionReport::TreeProgress { .. }))
    else {
        panic!("expected a progress report: {reports:#?}");
    };
    let wait = progress.wait.as_ref().unwrap();
    assert_eq!(
        (wait.kind.as_str(), wait.since.as_str(), &wait.step),
        ("external", "2026-07-23T00:00:05Z", &vec![json!("merge")])
    );
    assert!(progress.reported_at.ends_with('Z'));
    assert!(matches!(reports.first(), Some(ExecutionReport::Started)));
    assert_eq!(terminal_count(&reports), 1);
    let Some(ExecutionReport::TreeFinished { outcome, .. }) = reports.last() else {
        panic!("expected a tree finish: {reports:#?}");
    };
    assert_eq!(outcome["outcome"], "succeeded");
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

/// The `since` of the wait the injected `um` leaves a waiting run on.
const WAIT_SINCE: &str = "2026-07-23T00:00:05Z";
const CONTROL_ID: &str = "rct_01k0z6r1w8f4jy2m7q9v3x5acr";

/// The contract fixture's `run_control` effect for the fixture's assignment, as control `id`
/// carrying `control`, decoded as the connection decodes the frame.
fn run_control(fixture: &Fixture, id: &str, control: Value) -> AssignmentRunControl {
    let mut frame: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/valid/cloud-run-control-answer.json"
    )))
    .unwrap();
    let payload = &mut frame["payload"];
    payload["assignmentId"] = json!(fixture.offer.assignment_id);
    payload["runId"] = json!(fixture.offer.run_id);
    payload["attemptId"] = json!(fixture.offer.attempt_id);
    payload["controlId"] = json!(id);
    payload["control"] = control;
    let CloudFrame::RunControl {
        effect_id,
        assignment_id,
        run_id,
        attempt_id,
        control,
        ..
    } = decode_cloud_frame(&serde_json::to_vec(&frame).unwrap()).unwrap()
    else {
        panic!("expected a run control");
    };
    AssignmentRunControl {
        effect_id,
        assignment_id,
        run_id,
        attempt_id,
        control,
    }
}

/// Hands control `id` to the fixture's manager as the connection does a `run_control`.
fn send_control(fixture: &mut Fixture, id: &str, control: Value) {
    let control = run_control(fixture, id, control);
    fixture.manager.handle_run_control(control).unwrap();
}

/// A control of `kind` resolving the fixture's wait of `wait_kind`.
fn resolving(kind: &str, wait_kind: &str) -> Value {
    json!({"kind": kind, "wait": {"kind": wait_kind, "since": WAIT_SINCE}})
}

/// The control applications among `reports`, in order.
fn control_applications(reports: &[ExecutionReport]) -> Vec<RunControlApplication> {
    reports
        .iter()
        .filter_map(|report| match report {
            ExecutionReport::RunControlApplied { application } => Some(application.clone()),
            _ => None,
        })
        .collect()
}

fn applied(id: &str, action_outcome: &str) -> RunControlApplication {
    RunControlApplication {
        control_id: id.to_owned(),
        outcome: RunControlOutcome::Applied {
            action_outcome: action_outcome.to_owned(),
        },
    }
}

/// Cancels the fixture's assignment gracefully.
fn cancel_assignment(fixture: &mut Fixture) {
    fixture
        .manager
        .handle_cancel(AssignmentCancel {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
            assignment_id: fixture.offer.assignment_id.clone(),
            run_id: fixture.offer.run_id.clone(),
            attempt_id: fixture.offer.attempt_id.clone(),
            request_id: "cmd_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
            mode: CancellationMode::Graceful,
        })
        .unwrap();
}

/// A fixture whose run waits on a wait of `kind` that only a control resolves, the next
/// `resume` continuing it to `landed`.
fn control_wait_fixture(kind: &str, variables: &[(&str, &str)]) -> Fixture {
    control_wait_fixture_bounded(kind, variables, super::command::SHORT_COMMAND_BOUND)
}

/// `control_wait_fixture`, each short `um` command abandoned once `bound` passes.
fn control_wait_fixture_bounded(
    kind: &str,
    variables: &[(&str, &str)],
    bound: Duration,
) -> Fixture {
    let fixture = fixture_bounded(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            [
                ("TREE_TEST_WAIT_KIND", kind),
                ("TREE_TEST_WAIT_SOURCE", "um"),
                ("TREE_TEST_RESUMES", "continue"),
            ]
            .as_slice(),
            variables,
        ]
        .concat(),
        None,
        bound,
    );
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    fixture
}

async fn wait_for_waiting(fixture: &mut Fixture) {
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, _, state, _)| *state == "waiting")
    })
    .await;
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-90
#[tokio::test]
async fn control_for_the_current_wait_is_applied_once_and_the_run_resumes() {
    let mut fixture = control_wait_fixture("answer", &[]);
    start(&mut fixture).await;
    wait_for_waiting(&mut fixture).await;
    // Ten held-wait intervals pass without an evaluation.
    crate::service::Sleeper::sleep(&crate::service::TokioSleeper, Duration::from_millis(200)).await;
    assert_eq!(resumes(&fixture.log), 0);

    let answer = json!({
        "kind": "answer",
        "wait": {"kind": "answer", "since": WAIT_SINCE},
        "answers": [
            {"question": "scope", "text": "Only the API."},
            {"question": "2", "text": "- Yes."},
        ],
    });
    // A redelivered effect carries the same control, which is applied once.
    send_control(&mut fixture, CONTROL_ID, answer.clone());
    send_control(&mut fixture, CONTROL_ID, answer);
    let reports = finished_reports(&mut fixture.manager).await;

    let commands = logged_lines(&fixture.log, "control.args");
    assert_eq!(commands.len(), 2, "{commands:#?}");
    // Each value travels inside its option's own argument, so text that begins with a
    // hyphen is the option's value and never read as another option.
    for (command, answered) in commands.iter().zip([
        " --question=scope --text=Only the API. --json",
        " --question=2 --text=- Yes. --json",
    ]) {
        assert!(command.starts_with("workflow run answer "), "{command}");
        assert!(
            command.ends_with(&format!("/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc{answered}")),
            "{command}"
        );
    }
    assert_eq!(order(&fixture.log), ["answer", "answer", "resume"]);
    assert_eq!(
        control_applications(&reports),
        [applied(CONTROL_ID, "recorded")]
    );
    // The resumed run lands under the same assignment, its application reported first.
    let application = reports
        .iter()
        .position(|report| matches!(report, ExecutionReport::RunControlApplied { .. }));
    assert!(application < reports.iter().position(ExecutionReport::is_terminal));
    assert_eq!(resumes(&fixture.log), 1);
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

// REQ-RUNNERENROLL-90
#[tokio::test]
async fn control_naming_another_wait_is_skipped_and_runs_nothing() {
    let mut fixture = control_wait_fixture("approval", &[]);
    start(&mut fixture).await;
    wait_for_waiting(&mut fixture).await;
    send_control(
        &mut fixture,
        CONTROL_ID,
        json!({"kind": "approve", "wait": {"kind": "approval", "since": "2026-07-23T00:00:04Z"}}),
    );
    wait_for_reports(&mut fixture.manager, |reports| {
        !control_applications(reports).is_empty()
    })
    .await;
    cancel_assignment(&mut fixture);
    let reports = finished_reports(&mut fixture.manager).await;

    assert_eq!(
        control_applications(&reports),
        [RunControlApplication {
            control_id: CONTROL_ID.to_owned(),
            outcome: RunControlOutcome::Skipped(RunControlSkip::WaitNotCurrent),
        }]
    );
    assert!(!fixture.log.join("control.args").exists());
    assert_eq!(resumes(&fixture.log), 0);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

// REQ-RUNNERENROLL-90: a hand-back's notes are one argument with their option, so notes
// that open with a list hyphen reach `hand-back` as its notes.
#[tokio::test]
async fn hand_back_control_carries_notes_that_begin_with_a_hyphen() {
    let mut fixture = control_wait_fixture("approval", &[]);
    start(&mut fixture).await;
    wait_for_waiting(&mut fixture).await;
    let mut control = resolving("hand_back", "approval");
    control["notes"] = json!("- split the migration");
    send_control(&mut fixture, CONTROL_ID, control);
    let reports = finished_reports(&mut fixture.manager).await;

    let commands = logged_lines(&fixture.log, "control.args");
    assert_eq!(commands.len(), 1, "{commands:#?}");
    assert!(commands[0].starts_with("workflow run hand-back "));
    assert!(
        commands[0]
            .ends_with("/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc --notes=- split the migration --json"),
        "{}",
        commands[0]
    );
    assert_eq!(
        control_applications(&reports),
        [applied(CONTROL_ID, "recorded")]
    );
}

// REQ-RUNNERENROLL-90
#[tokio::test]
async fn refused_control_reports_the_runtime_code_and_the_run_keeps_waiting() {
    let mut fixture = control_wait_fixture("answer", &[("TREE_TEST_CONTROL", "refuse")]);
    start(&mut fixture).await;
    wait_for_waiting(&mut fixture).await;
    send_control(
        &mut fixture,
        CONTROL_ID,
        json!({
            "kind": "answer",
            "wait": {"kind": "answer", "since": WAIT_SINCE},
            "answers": [
                {"question": "unknown", "text": "Only the API."},
                {"question": "2", "text": "Yes."},
            ],
        }),
    );
    wait_for_reports(&mut fixture.manager, |reports| {
        !control_applications(reports).is_empty()
    })
    .await;
    // The wait stays in place, its later answers unsent and no evaluation run.
    crate::service::Sleeper::sleep(&crate::service::TokioSleeper, Duration::from_millis(100)).await;
    assert_eq!(resumes(&fixture.log), 0);
    assert_eq!(logged_lines(&fixture.log, "control.args").len(), 1);
    cancel_assignment(&mut fixture);
    let reports = finished_reports(&mut fixture.manager).await;

    assert_eq!(
        control_applications(&reports),
        [RunControlApplication {
            control_id: CONTROL_ID.to_owned(),
            outcome: RunControlOutcome::Refused {
                code: "question_unknown".to_owned(),
            },
        }]
    );
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

/// The bound the control tests that hang hold their control command to.
const HANGING_CONTROL_BOUND: Duration = Duration::from_secs(1);

/// The run's log, `tree-run.log`, beside the fixture's published tree.
fn run_log(fixture: &Fixture) -> String {
    let publish = logged_lines(&fixture.log, "publish.args");
    let private = Path::new(&publish[3]).parent().unwrap().to_owned();
    read_run_log(&private)
}

/// The run's log, `tree-run.log`, in the private directory `private`.
fn read_run_log(private: &Path) -> String {
    String::from_utf8_lossy(&fs::read(private.join("tree-run.log")).unwrap()).into_owned()
}

/// The lines of the run's log `log` that name the failure `code`.
fn failure_lines(log: &str, code: &str) -> Vec<String> {
    log.lines()
        .filter(|line| line.contains(code))
        .map(str::to_owned)
        .collect()
}

// REQ-RUNNERENROLL-90: a control command that recorded its control for the run's current
// wait and then outlived its bound is reported by the control it recorded, `applied`
// `recorded`, and the run resumes as after any applied control.
#[tokio::test]
async fn control_that_recorded_and_then_outlived_its_bound_is_applied_and_the_run_resumes() {
    let mut fixture = control_wait_fixture_bounded(
        "approval",
        &[("TREE_TEST_CONTROL", "record_hang")],
        HANGING_CONTROL_BOUND,
    );
    start(&mut fixture).await;
    wait_for_waiting(&mut fixture).await;
    send_control(&mut fixture, CONTROL_ID, resolving("approve", "approval"));
    let reports = finished_reports(&mut fixture.manager).await;

    assert_eq!(
        control_applications(&reports),
        [applied(CONTROL_ID, "recorded")]
    );
    assert_eq!(order(&fixture.log), ["approve", "resume"]);
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

// REQ-RUNNERENROLL-90: a control command that outlived its bound without recording its
// control is refused `control_command_failed`, the run's log naming the class `timeout`,
// and the run keeps waiting.
#[tokio::test]
async fn control_that_outlived_its_bound_unrecorded_is_refused_and_its_class_logged() {
    let mut fixture = control_wait_fixture_bounded(
        "approval",
        &[("TREE_TEST_CONTROL", "hang")],
        HANGING_CONTROL_BOUND,
    );
    start(&mut fixture).await;
    wait_for_waiting(&mut fixture).await;
    send_control(&mut fixture, CONTROL_ID, resolving("approve", "approval"));
    wait_for_reports(&mut fixture.manager, |reports| {
        !control_applications(reports).is_empty()
    })
    .await;
    let failed = failure_lines(&run_log(&fixture), "control_command_failed");
    assert_eq!(failed.len(), 1, "{failed:#?}");
    assert!(failed[0].contains("class=timeout"), "{}", failed[0]);
    // A command killed at its bound ended without an I/O error to name.
    assert!(!failed[0].contains("kind="), "{}", failed[0]);
    assert_eq!(resumes(&fixture.log), 0);
    cancel_assignment(&mut fixture);
    let reports = finished_reports(&mut fixture.manager).await;

    assert_eq!(
        control_applications(&reports),
        [RunControlApplication {
            control_id: CONTROL_ID.to_owned(),
            outcome: RunControlOutcome::Refused {
                code: "control_command_failed".to_owned(),
            },
        }]
    );
    assert_eq!(order(&fixture.log), ["approve"]);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

// REQ-RUNNERENROLL-90
#[tokio::test]
async fn retry_control_continues_the_run_itself() {
    let mut fixture = control_wait_fixture("repair", &[]);
    start(&mut fixture).await;
    wait_for_waiting(&mut fixture).await;
    send_control(&mut fixture, CONTROL_ID, resolving("retry", "repair"));
    let reports = finished_reports(&mut fixture.manager).await;

    let commands = logged_lines(&fixture.log, "control.args");
    assert_eq!(commands.len(), 1);
    assert!(commands[0].starts_with("workflow run retry "));
    assert!(commands[0].ends_with("/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc --json"));
    // The retry continued the run; no `resume` followed it.
    assert_eq!(order(&fixture.log), ["retry"]);
    assert_eq!(
        control_applications(&reports),
        [applied(CONTROL_ID, "succeeded")]
    );
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

// REQ-RUNNERENROLL-90
#[tokio::test]
async fn stop_control_while_a_process_runs_is_applied_beside_it() {
    let mut fixture = fixture(TREE_DIGEST, "cancellable", "null");
    start(&mut fixture).await;
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, _, state, _)| *state == "running")
    })
    .await;
    send_control(
        &mut fixture,
        CONTROL_ID,
        json!({"kind": "stop", "before": "review"}),
    );
    wait_for_reports(&mut fixture.manager, |reports| {
        !control_applications(reports).is_empty()
    })
    .await;
    // `create` still runs: nothing has cancelled it.
    assert!(!fixture.log.join("cancelled").exists());
    let stop = logged_lines(&fixture.log, "stop.args");
    assert_eq!(stop[..3], ["workflow", "run", "stop"]);
    assert_eq!(
        stop[4..],
        [
            "--before=review",
            "--reason=Stopped by a Cloud control",
            "--json"
        ]
    );
    cancel_assignment(&mut fixture);
    let reports = finished_reports(&mut fixture.manager).await;

    assert_eq!(
        control_applications(&reports),
        [applied(CONTROL_ID, "recorded")]
    );
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

// REQ-RUNNERENROLL-91
#[tokio::test]
async fn record_left_running_without_its_process_is_resumed_once() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "running",
        "null",
        &[("TREE_TEST_RESUMES", "continue")],
    );
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    let reports = execute(&mut fixture).await;

    assert_eq!(resumes(&fixture.log), 1);
    assert_eq!(
        progress(&reports).first(),
        Some(&(1, 1, "running", json!(["build"])))
    );
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

// REQ-RUNNERENROLL-91
#[tokio::test]
async fn unavailable_condition_leaves_the_wait_and_is_evaluated_again() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            ("TREE_TEST_WAIT_KIND", "external"),
            ("TREE_TEST_WAIT_SOURCE", "github"),
            ("TREE_TEST_RESUMES", "unavailable continue"),
        ],
    );
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    let reports = execute(&mut fixture).await;

    assert_eq!(resumes(&fixture.log), 2);
    assert_eq!(
        progress(&reports).first(),
        Some(&(1, 1, "waiting", json!(["merge"])))
    );
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-99
#[tokio::test]
async fn cancellation_during_a_wait_cancels_the_run() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            ("TREE_TEST_WAIT_KIND", "external"),
            ("TREE_TEST_WAIT_SOURCE", "github"),
        ],
    );
    start(&mut fixture).await;
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, _, state, _)| *state == "waiting")
    })
    .await;
    fixture
        .manager
        .handle_cancel(AssignmentCancel {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
            assignment_id: fixture.offer.assignment_id.clone(),
            run_id: fixture.offer.run_id.clone(),
            attempt_id: fixture.offer.attempt_id.clone(),
            request_id: "cmd_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
            mode: CancellationMode::Graceful,
        })
        .unwrap();
    let reports = finished_reports(&mut fixture.manager).await;

    let cancel = logged_lines(&fixture.log, "cancel.args");
    assert_eq!(cancel[..3], ["workflow", "run", "cancel"]);
    assert_eq!(terminal_count(&reports), 1);
    let Some(ExecutionReport::TreeFinished { outcome, .. }) = reports.last() else {
        panic!("expected a tree finish: {reports:#?}");
    };
    assert_eq!(outcome["outcome"], "cancelled");
    assert_eq!(outcome["reason"], "user_request");
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-99
#[tokio::test]
async fn lease_loss_kills_the_process_and_reports_no_outcome() {
    let mut fixture = fixture(TREE_DIGEST, "cancellable", "null");
    start(&mut fixture).await;
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, _, state, _)| *state == "running")
    })
    .await;
    fixture
        .manager
        .handle_release(AssignmentRelease {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abr".to_owned(),
            assignment_id: fixture.offer.assignment_id.clone(),
            run_id: fixture.offer.run_id.clone(),
            attempt_id: fixture.offer.attempt_id.clone(),
            reason: "execution_lease_expired".to_owned(),
        })
        .unwrap();
    let reports = finished_reports(&mut fixture.manager).await;

    assert!(!fixture.log.join("cancel.args").exists());
    // The killed process never ended its record.
    assert_eq!(local_record(&fixture.log)["state"], "running");
    assert_eq!(terminal_count(&reports), 1);
    assert!(
        matches!(
            reports.last(),
            Some(ExecutionReport::Interrupted { reason, terminal_outcome, .. })
                if reason == "execution_lease_expired"
                    && terminal_outcome["reason"] == "execution_lease_expired"
        ),
        "{reports:#?}"
    );
}

/// The `run.json` of the run `create` made in the run-scoped home.
fn local_record(log: &Path) -> Value {
    let home = logged_lines(log, "create.env")
        .into_iter()
        .find_map(|line| line.strip_prefix("UM_HOME=").map(PathBuf::from))
        .unwrap();
    serde_json::from_slice(
        &fs::read(home.join(
            "workflows/def_01k0z6r1w8f4jy2m7q9v3x5loc/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc/run.json",
        ))
        .unwrap(),
    )
    .unwrap()
}

/// Shuts the runner down once the run reads `state`, then asserts the shutdown ran no
/// `um workflow run cancel`, left the record reading `state` with no outcome, and reported
/// the attempt interrupted with no exit, so a continuation can adopt the run.
async fn shut_down_at(mut fixture: Fixture, state: &str) {
    start(&mut fixture).await;
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, _, reported, _)| *reported == state)
    })
    .await;
    fixture.manager.begin_shutdown().unwrap();
    let reports = finished_reports(&mut fixture.manager).await;

    // No `cancel`, so no `canceled` outcome and no `onOutcome.canceled` mirror write.
    assert!(!fixture.log.join("cancel.args").exists());
    let record = local_record(&fixture.log);
    assert_eq!(record["state"], state);
    assert_eq!(record["outcome"], Value::Null);
    assert_eq!(terminal_count(&reports), 1);
    assert!(
        matches!(
            reports.last(),
            Some(ExecutionReport::Interrupted { reason, terminal_outcome, .. })
                if reason == "graceful_shutdown"
                    && terminal_outcome["reason"] == "runner_shutdown"
        ),
        "{reports:#?}"
    );
}

// REQ-RUNNERENROLL-73, REQ-RUNNERENROLL-91
#[tokio::test]
async fn runner_shutdown_kills_the_running_stage_and_leaves_the_run_unexited() {
    // The `cancellable` process runs until `cancel` would end it, so a record still
    // `running` once the attempt finishes shows its group was killed.
    shut_down_at(fixture(TREE_DIGEST, "cancellable", "null"), "running").await;
}

// REQ-RUNNERENROLL-73, REQ-RUNNERENROLL-91
#[tokio::test]
async fn runner_shutdown_during_a_wait_leaves_the_run_waiting() {
    shut_down_at(
        fixture_with(
            TREE_DIGEST,
            "waiting",
            "null",
            &[
                ("TREE_TEST_WAIT_KIND", "external"),
                ("TREE_TEST_WAIT_SOURCE", "github"),
            ],
        ),
        "waiting",
    )
    .await;
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-99
#[tokio::test]
async fn assignment_cancellation_cancels_the_local_run() {
    let mut fixture = fixture(TREE_DIGEST, "cancellable", "null");
    assert_eq!(
        prepare_offered(&mut fixture.manager, &fixture.offer).await,
        None
    );
    start_offered(&mut fixture.manager, &fixture.offer).spawn();
    fixture
        .manager
        .handle_cancel(AssignmentCancel {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
            assignment_id: fixture.offer.assignment_id.clone(),
            run_id: fixture.offer.run_id.clone(),
            attempt_id: fixture.offer.attempt_id.clone(),
            request_id: "cmd_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
            mode: CancellationMode::Graceful,
        })
        .unwrap();
    let reports = finished_reports(&mut fixture.manager).await;

    let cancel = logged_lines(&fixture.log, "cancel.args");
    assert_eq!(cancel[..3], ["workflow", "run", "cancel"]);
    assert!(cancel[3].ends_with("/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc"));
    let Some(ExecutionReport::TreeFinished { outcome, exit, .. }) = reports.last() else {
        panic!("expected a tree finish: {reports:#?}");
    };
    assert_eq!(outcome["outcome"], "cancelled");
    assert_eq!(outcome["reason"], "user_request");
    assert_eq!(exit.chosen_outcome.as_deref(), Some("canceled"));
}

/// The contract fixture's delivery, as delivery `id`.
fn delivery(id: &str) -> TriggerDelivery {
    let CloudFrame::TriggerDelivery { delivery, .. } = decode_cloud_frame(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/valid/cloud-trigger-delivery.json"
    )))
    .unwrap() else {
        panic!("expected a trigger delivery");
    };
    TriggerDelivery {
        delivery_id: id.to_owned(),
        ..delivery
    }
}

/// Hands `delivery` to the fixture's manager as the connection does a `trigger_delivery`.
fn deliver(fixture: &mut Fixture, delivery: TriggerDelivery) {
    fixture
        .manager
        .handle_trigger_delivery(AssignmentTriggerDelivery {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5atd".to_owned(),
            assignment_id: fixture.offer.assignment_id.clone(),
            run_id: fixture.offer.run_id.clone(),
            attempt_id: fixture.offer.attempt_id.clone(),
            delivery,
        })
        .unwrap();
}

/// The applications among `reports`, in order.
fn applications(reports: &[ExecutionReport]) -> Vec<TriggerDeliveryApplication> {
    reports
        .iter()
        .filter_map(|report| match report {
            ExecutionReport::TriggerDeliveryApplied { application } => Some(application.clone()),
            _ => None,
        })
        .collect()
}

fn logged_json(log: &Path, name: &str) -> Value {
    serde_json::from_slice(&fs::read(log.join(name)).unwrap()).unwrap()
}

fn order(log: &Path) -> Vec<String> {
    fs::read_to_string(log.join("order"))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn canceled_observation() -> Option<TriggerIssueObservation> {
    Some(TriggerIssueObservation {
        team: Some("team_8c1b".to_owned()),
        state: "state_canceled".to_owned(),
        updated_at: Some("2026-07-23T00:00:07.512Z".to_owned()),
    })
}

// REQ-TRIGGER-136
#[tokio::test]
async fn delivery_during_an_external_wait_is_applied_once_and_its_action_reported() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            ("TREE_TEST_WAIT_KIND", "external"),
            ("TREE_TEST_WAIT_SOURCE", "github"),
            ("TREE_TEST_EVENT", "cancel"),
        ],
    );
    start(&mut fixture).await;
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, _, state, _)| *state == "waiting")
    })
    .await;
    let delivered = delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt");
    // A redelivered effect carries the same delivery, which is applied once.
    deliver(&mut fixture, delivered.clone());
    deliver(&mut fixture, delivered.clone());
    let reports = finished_reports(&mut fixture.manager).await;

    let event = logged_lines(&fixture.log, "event.args");
    assert_eq!(event[..3], ["workflow", "run", "event"]);
    assert!(event[3].ends_with("/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc"));
    assert_eq!(
        [&event[4], &event[6], &event[8]],
        ["--event", "--claim", "--json"]
    );
    assert_eq!(logged_json(&fixture.log, "event.json"), delivered.event);
    assert_eq!(logged_json(&fixture.log, "claim.json"), delivered.claim);
    assert_eq!(
        order(&fixture.log)
            .iter()
            .filter(|command| *command == "event")
            .count(),
        1
    );
    assert_eq!(
        applications(&reports),
        [TriggerDeliveryApplication {
            delivery_id: "wtd_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned(),
            outcome: TriggerDeliveryOutcome::Applied(TriggerDeliveryAction::Cancel),
            observation: canceled_observation(),
        }]
    );
    assert!(!fixture.log.join("cancel.args").exists());
    assert_eq!(terminal_count(&reports), 1);
    let Some(ExecutionReport::TreeFinished { outcome, .. }) = reports.last() else {
        panic!("expected a tree finish: {reports:#?}");
    };
    assert_eq!(outcome["outcome"], "cancelled");
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

// REQ-TRIGGER-136
#[tokio::test]
async fn delivery_while_a_process_runs_is_applied_beside_it() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "cancellable",
        "null",
        &[("TREE_TEST_EVENT", "cancel")],
    );
    start(&mut fixture).await;
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, _, state, _)| *state == "running")
    })
    .await;
    deliver(&mut fixture, delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt"));
    let reports = finished_reports(&mut fixture.manager).await;

    assert_eq!(order(&fixture.log), ["event"]);
    assert!(!fixture.log.join("cancel.args").exists());
    assert_eq!(
        applications(&reports),
        [TriggerDeliveryApplication {
            delivery_id: "wtd_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned(),
            outcome: TriggerDeliveryOutcome::Applied(TriggerDeliveryAction::Cancel),
            observation: canceled_observation(),
        }]
    );
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

// REQ-TRIGGER-136
#[tokio::test]
async fn refused_application_reports_its_code_and_the_run_keeps_waiting() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            ("TREE_TEST_WAIT_KIND", "external"),
            ("TREE_TEST_WAIT_SOURCE", "github"),
            ("TREE_TEST_EVENT", "not_later"),
        ],
    );
    start(&mut fixture).await;
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, _, state, _)| *state == "waiting")
    })
    .await;
    deliver(&mut fixture, delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt"));
    wait_for_reports(&mut fixture.manager, |reports| {
        !applications(reports).is_empty()
    })
    .await;
    // The wait is still held and evaluated at its interval.
    let resumed = resumes(&fixture.log);
    crate::service::Sleeper::sleep(&crate::service::TokioSleeper, Duration::from_millis(100)).await;
    assert!(resumes(&fixture.log) > resumed);

    fixture
        .manager
        .handle_cancel(AssignmentCancel {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
            assignment_id: fixture.offer.assignment_id.clone(),
            run_id: fixture.offer.run_id.clone(),
            attempt_id: fixture.offer.attempt_id.clone(),
            request_id: "cmd_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
            mode: CancellationMode::Graceful,
        })
        .unwrap();
    let reports = finished_reports(&mut fixture.manager).await;
    assert_eq!(
        applications(&reports),
        [TriggerDeliveryApplication {
            delivery_id: "wtd_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned(),
            outcome: TriggerDeliveryOutcome::Refused {
                code: "event_not_later".to_owned(),
            },
            observation: None,
        }]
    );
    assert_eq!(
        logged_lines(&fixture.log, "cancel.args")[..3],
        ["workflow", "run", "cancel"]
    );
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

// REQ-TRIGGER-136, REQ-LOCALRUN-208, REQ-LOCALRUN-186: an applied `withdraw` is reported
// from the event command's `actionOutcome`, `recorded` for a live run's withdrawal its process
// ends `withdrawn`; one whose `actionOutcome` names no withdrawal is `event_command_failed`.
#[tokio::test]
async fn applied_withdrawal_is_reported_from_its_action_outcome() {
    for (action_outcome, expected, observation) in [
        (
            r#""recorded""#,
            TriggerDeliveryOutcome::Applied(TriggerDeliveryAction::Withdraw),
            canceled_observation(),
        ),
        (
            r#""withdrawn""#,
            TriggerDeliveryOutcome::Applied(TriggerDeliveryAction::Withdraw),
            canceled_observation(),
        ),
        (
            "null",
            TriggerDeliveryOutcome::Refused {
                code: "event_command_failed".to_owned(),
            },
            None,
        ),
    ] {
        let mut fixture = fixture_with(
            TREE_DIGEST,
            "waiting",
            "null",
            &[
                ("TREE_TEST_WAIT_KIND", "external"),
                ("TREE_TEST_WAIT_SOURCE", "github"),
                ("TREE_TEST_EVENT", "withdraw"),
                ("TREE_TEST_ACTION_OUTCOME", action_outcome),
            ],
        );
        start(&mut fixture).await;
        wait_for_reports(&mut fixture.manager, |reports| {
            progress(reports)
                .iter()
                .any(|(_, _, state, _)| *state == "waiting")
        })
        .await;
        deliver(&mut fixture, delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt"));
        wait_for_reports(&mut fixture.manager, |reports| {
            !applications(reports).is_empty()
        })
        .await;
        fixture
            .manager
            .handle_cancel(AssignmentCancel {
                effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
                assignment_id: fixture.offer.assignment_id.clone(),
                run_id: fixture.offer.run_id.clone(),
                attempt_id: fixture.offer.attempt_id.clone(),
                request_id: "cmd_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned(),
                mode: CancellationMode::Graceful,
            })
            .unwrap();
        let reports = finished_reports(&mut fixture.manager).await;
        assert_eq!(
            applications(&reports),
            [TriggerDeliveryApplication {
                delivery_id: "wtd_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned(),
                outcome: expected,
                observation,
            }],
            "{action_outcome}"
        );
    }
}

/// A fixture whose offer carries `pending` deliveries, `create` leaving the run at the
/// stop before its first step and each delivery answered by `event`.
fn pending_fixture(event: &str, pending: Vec<TriggerDelivery>) -> Fixture {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            ("TREE_TEST_WAIT_KIND", "stop"),
            ("TREE_TEST_WAIT_SOURCE", "um"),
            ("TREE_TEST_EVENT", event),
            ("TREE_TEST_RESUMES", "continue"),
        ],
    );
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    fixture
        .offer
        .execution_spec
        .workflow_tree
        .as_mut()
        .unwrap()
        .pending_deliveries = pending;
    fixture
}

// REQ-TRIGGER-136
#[tokio::test]
async fn pending_deliveries_apply_before_the_first_step_and_a_surviving_run_is_lifted() {
    let mut fixture = pending_fixture(
        "owned",
        vec![
            delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt"),
            delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abx"),
        ],
    );
    let reports = execute(&mut fixture).await;

    // The stop before the first step is earlier than the start's `review`.
    let create = logged_lines(&fixture.log, "create.args");
    let stop = create
        .iter()
        .position(|argument| argument == "--stop-before")
        .unwrap();
    assert_eq!(create[stop + 1], "build");
    assert_eq!(
        create
            .iter()
            .filter(|argument| *argument == "--stop-before")
            .count(),
        1
    );
    assert_eq!(
        order(&fixture.log),
        ["event", "event", "stop", "lift", "resume"]
    );
    // The start's own stop is placed before the runner's is lifted.
    let stop = logged_lines(&fixture.log, "stop.args");
    assert_eq!(stop[..3], ["workflow", "run", "stop"]);
    assert!(stop[3].ends_with("/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc"));
    assert_eq!(stop[4], "--before=review");
    let lift = logged_lines(&fixture.log, "lift.args");
    assert_eq!(lift[..3], ["workflow", "run", "lift"]);
    assert_eq!(lift[4..], ["--before=build", "--json"]);
    let refused = |id: &str| TriggerDeliveryApplication {
        delivery_id: id.to_owned(),
        outcome: TriggerDeliveryOutcome::Refused {
            code: "issue_in_owned_state".to_owned(),
        },
        observation: Some(TriggerIssueObservation {
            team: Some("team_8c1b".to_owned()),
            state: "state_build".to_owned(),
            updated_at: None,
        }),
    };
    assert_eq!(
        applications(&reports),
        [
            refused("wtd_01k0z6r1w8f4jy2m7q9v3x5abt"),
            refused("wtd_01k0z6r1w8f4jy2m7q9v3x5abx"),
        ]
    );
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

// REQ-TRIGGER-136
#[tokio::test]
async fn pending_delivery_that_cancels_the_run_leaves_its_stop_in_place() {
    let mut fixture = pending_fixture("cancel", vec![delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt")]);
    let reports = execute(&mut fixture).await;

    assert_eq!(order(&fixture.log), ["event"]);
    assert_eq!(
        applications(&reports),
        [TriggerDeliveryApplication {
            delivery_id: "wtd_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned(),
            outcome: TriggerDeliveryOutcome::Applied(TriggerDeliveryAction::Cancel),
            observation: canceled_observation(),
        }]
    );
    let Some(ExecutionReport::TreeFinished { outcome, .. }) = reports.last() else {
        panic!("expected a tree finish: {reports:#?}");
    };
    assert_eq!(outcome["outcome"], "cancelled");
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

fn exited(outcome: &str) -> super::records::TreeRecord {
    super::records::TreeRecord {
        state: super::records::RecordState::Exited,
        outcome: Some(outcome.to_owned()),
        attempts: 1,
        position: Vec::new(),
        mirror: Vec::new(),
    }
}

// REQ-RUNNERENROLL-99
#[test]
fn record_outcomes_map_to_transport_results() {
    let issue = json!({
        "node": {"id": "build", "role": "step"},
        "state": "failed",
        "detail": {"phase": "execution", "code": "command_exit", "exitCode": 2},
    });
    let document = json!({"result": {"primaryIssue": issue}});
    let TreeConclusion::Terminal {
        transitions,
        result,
        exit,
    } = conclude(Some(&exited("failed")), Some(&document), None)
    else {
        panic!("a failed record has a result");
    };
    assert_eq!(
        result,
        TransportResult::Failed {
            primary_issue: issue.clone(),
            diagnostic: None,
        }
    );
    assert_eq!(result.outcome()["primaryIssue"], issue);
    assert_eq!(
        transitions
            .iter()
            .map(|event| (event["from"]["gate"].clone(), event["to"]["state"].clone()))
            .collect::<Vec<_>>(),
        [
            (json!("open"), json!("executing")),
            (json!("failure_stopped"), json!("failed")),
        ]
    );
    assert_eq!(exit.chosen_outcome.as_deref(), Some("failed"));

    for (outcome, applied, expected) in [
        (
            "canceled",
            Some(CancellationReason::UserRequest),
            CancellationReason::UserRequest,
        ),
        ("canceled", None, CancellationReason::UserRequest),
        ("withdrawn", None, CancellationReason::UserRequest),
        (
            "canceled",
            Some(CancellationReason::ForceAbort),
            CancellationReason::ForceAbort,
        ),
    ] {
        let TreeConclusion::Terminal {
            result,
            transitions,
            ..
        } = conclude(Some(&exited(outcome)), None, applied)
        else {
            panic!("a canceled record has a result");
        };
        assert_eq!(result, TransportResult::Cancelled { reason: expected });
        assert_eq!(result.outcome()["outcome"], "cancelled");
        assert_eq!(transitions.last().unwrap()["to"]["state"], "cancelled");
    }
    let TreeConclusion::Terminal { result, .. } = conclude(
        Some(&exited("canceled")),
        None,
        Some(CancellationReason::ExecutionLeaseExpired),
    ) else {
        panic!("a canceled record has a result");
    };
    assert_eq!(
        result,
        TransportResult::Interrupted {
            reason: CancellationReason::ExecutionLeaseExpired
        }
    );
    let TreeConclusion::Terminal { result, exit, .. } =
        conclude(Some(&exited("blocked")), None, None)
    else {
        panic!("a chosen outcome has a result");
    };
    assert_eq!(result, TransportResult::Succeeded);
    assert_eq!(exit.chosen_outcome.as_deref(), Some("blocked"));

    assert_eq!(
        conclude(Some(&exited("failed")), None, None),
        TreeConclusion::Unreportable("tree_run_failure_unclassified")
    );
    for state in [
        super::records::RecordState::Waiting,
        super::records::RecordState::Running,
    ] {
        let record = super::records::TreeRecord {
            state,
            outcome: None,
            attempts: 1,
            position: vec![json!("merge")],
            mirror: Vec::new(),
        };
        assert_eq!(
            conclude(Some(&record), None, Some(CancellationReason::UserRequest)),
            TreeConclusion::Unreportable("tree_run_record_unsettled")
        );
        let TreeConclusion::Terminal { result, exit, .. } = conclude(
            Some(&record),
            None,
            Some(CancellationReason::ExecutionLeaseExpired),
        ) else {
            panic!("lease loss interrupts an unsettled record");
        };
        assert_eq!(
            result,
            TransportResult::Interrupted {
                reason: CancellationReason::ExecutionLeaseExpired
            }
        );
        assert_eq!(exit.chosen_outcome, None);
    }
}

// REQ-RUNNERENROLL-73, REQ-RUNNERENROLL-91: a shutdown or lease loss that stops the run
// before `create` wrote its record is an interruption like any other, with no outcome; with
// no record and any other end there is no truthful result.
#[test]
fn interruption_before_the_run_record_exists_is_reported_interrupted() {
    for reason in [
        CancellationReason::RunnerShutdown,
        CancellationReason::ExecutionLeaseExpired,
    ] {
        let TreeConclusion::Terminal { result, exit, .. } = conclude(None, None, Some(reason))
        else {
            panic!("{reason:?} before the record exists interrupts the attempt");
        };
        assert_eq!(result, TransportResult::Interrupted { reason });
        assert_eq!(exit.chosen_outcome, None);
    }
    for cancellation in [None, Some(CancellationReason::UserRequest)] {
        assert_eq!(
            conclude(None, None, cancellation),
            TreeConclusion::Unreportable("tree_run_record_unavailable")
        );
    }
}

// REQ-RUNNERENROLL-91
#[test]
fn each_materialized_input_is_one_input_argument() {
    let temporary = tempfile::tempdir().unwrap();
    let directory = temporary.path().join("inputs");
    let inputs = ResolvedInputs::new(BTreeMap::from([
        (
            "ticket".to_owned(),
            ResolvedInput::Text(Arc::from("AUTO-1")),
        ),
        (
            "config".to_owned(),
            ResolvedInput::Json(
                ResolvedJsonInput::from_source(Arc::from(&b"{\"a\":1}"[..])).unwrap(),
            ),
        ),
        (
            "report".to_owned(),
            ResolvedInput::File(ResolvedFile::new(
                Arc::from("text/plain"),
                Arc::from(&b"r"[..]),
            )),
        ),
        (
            "shots".to_owned(),
            ResolvedInput::Attachments(Arc::from([ResolvedAttachment::new(
                Arc::from("image/png"),
                Arc::from(&b"png"[..]),
            )])),
        ),
        (
            "empty".to_owned(),
            ResolvedInput::Attachments(Arc::from([])),
        ),
    ]));
    let arguments = super::stage_inputs(&directory, &inputs)
        .unwrap()
        .into_iter()
        .map(|argument| argument.into_string().unwrap())
        .collect::<Vec<_>>();
    let file = |index: usize| fs::read(&arguments[index]).unwrap();
    assert_eq!(arguments[..2], ["--input-json-file", "config"]);
    assert_eq!(file(2), b"{\"a\":1}");
    assert_eq!(arguments[3..5], ["--input-attachments-empty", "empty"]);
    assert_eq!(arguments[5..8], ["--input-file", "report", "text/plain"]);
    assert_eq!(file(8), b"r");
    assert_eq!(
        arguments[9..12],
        ["--input-attachment", "shots", "image/png"]
    );
    assert_eq!(file(12), b"png");
    assert_eq!(arguments[13..15], ["--input-text-file", "ticket"]);
    assert_eq!(file(15), b"AUTO-1");
    assert_eq!(arguments.len(), 16);
}

/// Leaves `bytes` as `<run>/<file>` in the fixture's stage `name`, `run` relative to the
/// run directory.
fn stage(log: &Path, name: &str, run: &str, file: &str, bytes: &[u8]) {
    let directory = log.join("stages").join(name).join(run);
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join(file), bytes).unwrap();
}

/// Stages a `state.json` whose attempts, numbered as given, hold `steps`.
fn stage_state(log: &Path, name: &str, run: &str, attempts: &[(u64, Vec<Value>)]) {
    let attempts = attempts
        .iter()
        .map(|(number, steps)| {
            json!({
                "attemptNumber": number,
                "state": "running",
                "progress": { "steps": steps, "outstandingActions": [] },
            })
        })
        .collect::<Vec<_>>();
    let state = json!({ "schemaVersion": 1, "currentAttemptNumber": 1, "attempts": attempts });
    stage(
        log,
        name,
        run,
        "state.json",
        &serde_json::to_vec(&state).unwrap(),
    );
}

/// Stages a child run at `run`: the `run.json` that makes a `children/` entry a run
/// directory (REQ-LOCALRUN-177) and a `state.json` whose attempts hold `steps`.
fn stage_child_run(log: &Path, name: &str, run: &str, attempts: &[(u64, Vec<Value>)]) {
    stage(
        log,
        name,
        run,
        "run.json",
        b"{\"state\":\"running\",\"outcome\":null,\"attempts\":1,\"position\":[]}\n",
    );
    stage_state(log, name, run, attempts);
}

fn step(id: &str, state: &str) -> Value {
    json!({ "id": id, "role": "command", "state": state })
}

/// A step observation as the wire writes it.
fn observed(run: Value, attempt: u64, step: &str, state: &str) -> Value {
    json!({ "run": run, "runAttempt": attempt, "step": step, "state": state })
}

/// The step observations of each progress report among `reports`, by progress sequence.
fn step_frames(reports: &[ExecutionReport]) -> Vec<(u64, Vec<Value>)> {
    reports
        .iter()
        .filter_map(|report| match report {
            ExecutionReport::TreeProgress {
                progress_sequence,
                progress,
                ..
            } => Some((
                *progress_sequence,
                progress
                    .steps
                    .iter()
                    .map(|observation| {
                        let run = observation
                            .run
                            .iter()
                            .map(|element| {
                                let mut value =
                                    json!({ "step": element.step, "ordinal": element.ordinal });
                                if let Some(item) = element.item {
                                    value["item"] = json!(item);
                                }
                                value
                            })
                            .collect();
                        observed(
                            Value::Array(run),
                            observation.run_attempt,
                            &observation.step,
                            observation.state.as_str(),
                        )
                    })
                    .collect(),
            )),
            _ => None,
        })
        .collect()
}

fn reported_steps(reports: &[ExecutionReport]) -> Vec<Value> {
    step_frames(reports)
        .into_iter()
        .flat_map(|(_, steps)| steps)
        .collect()
}

// REQ-RUNNERENROLL-88
#[tokio::test]
async fn first_progress_reports_the_steps_of_the_run_and_its_child_runs() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "running",
        "null",
        &[("TREE_TEST_RESUMES", "continue")],
    );
    stage_state(
        &fixture.log,
        "create",
        "",
        &[(1, vec![step("design", "running"), step("build", "pending")])],
    );
    stage_child_run(
        &fixture.log,
        "create",
        "children/design/000001",
        &[(1, vec![step("plan", "succeeded"), step("write", "running")])],
    );
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    let reports = execute(&mut fixture).await;

    assert_eq!(
        progress(&reports).first(),
        Some(&(1, 1, "running", json!(["build"])))
    );
    let child = json!([{ "step": "design", "ordinal": 1 }]);
    let frames = step_frames(&reports);
    assert_eq!(
        frames[0],
        (
            1,
            vec![
                observed(json!([]), 1, "design", "running"),
                observed(json!([]), 1, "build", "pending"),
                observed(child.clone(), 1, "plan", "succeeded"),
                observed(child, 1, "write", "running"),
            ]
        )
    );
    // Nothing changed after the first report; later reports carry the root alone.
    assert!(frames[1..].iter().all(|(_, steps)| steps.is_empty()));
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

// REQ-RUNNERENROLL-88
#[tokio::test]
async fn later_progress_carries_only_changed_steps_under_their_engine_attempt() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            ("TREE_TEST_WAIT_KIND", "approval"),
            ("TREE_TEST_WAIT_SOURCE", "um"),
            ("TREE_TEST_RESUMES", "held held continue"),
        ],
    );
    let first = vec![step("design", "succeeded"), step("merge", "succeeded")];
    stage_state(
        &fixture.log,
        "create",
        "",
        &[(
            1,
            vec![step("design", "succeeded"), step("merge", "running")],
        )],
    );
    stage_state(&fixture.log, "resume1", "", &[(1, first.clone())]);
    // The second `resume` changes nothing; the third goes back to `merge` as attempt 2.
    stage_state(
        &fixture.log,
        "resume3",
        "",
        &[(1, first.clone()), (2, vec![step("merge", "running")])],
    );
    stage_state(
        &fixture.log,
        "landed",
        "",
        &[(1, first), (2, vec![step("merge", "succeeded")])],
    );
    start(&mut fixture).await;
    let approvals = [
        "rct_01k0z6r1w8f4jy2m7q9v3x5acr",
        "rct_01k0z6r1w8f4jy2m7q9v3x5acs",
        "rct_01k0z6r1w8f4jy2m7q9v3x5act",
    ];
    for (reported, approval) in (1..=2).zip(approvals) {
        wait_for_reports(&mut fixture.manager, |reports| {
            step_frames(reports).len() == reported
        })
        .await;
        send_control(&mut fixture, approval, resolving("approve", "approval"));
    }
    while resumes(&fixture.log) < 2 {
        crate::service::Sleeper::sleep(&crate::service::TokioSleeper, Duration::from_millis(10))
            .await;
    }
    send_control(&mut fixture, approvals[2], resolving("approve", "approval"));
    let merging = observed(json!([]), 2, "merge", "running");
    wait_for_reports(&mut fixture.manager, |reports| {
        reported_steps(reports).contains(&merging)
    })
    .await;
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    let reports = finished_reports(&mut fixture.manager).await;

    let roots = progress(&reports);
    let frames = step_frames(&reports);
    assert_eq!(
        roots[..2],
        [
            (1, 1, "waiting", json!(["merge"])),
            (2, 1, "waiting", json!(["merge"])),
        ]
    );
    assert_eq!(
        frames[1].1,
        [observed(json!([]), 1, "merge", "succeeded")],
        "a changed step alone restates the root"
    );
    // The unchanged second `resume` sent nothing: every later report is attempt 2's.
    assert!(
        roots[2..]
            .iter()
            .all(|(_, attempt, state, position)| *attempt == 2
                && *state == "running"
                && *position == json!(["merge"])),
        "{roots:#?}"
    );
    assert_eq!(
        roots
            .iter()
            .map(|(sequence, ..)| *sequence)
            .collect::<Vec<_>>(),
        (1..=roots.len() as u64).collect::<Vec<_>>()
    );
    assert_eq!(
        reported_steps(&reports),
        [
            observed(json!([]), 1, "design", "succeeded"),
            observed(json!([]), 1, "merge", "running"),
            observed(json!([]), 1, "merge", "succeeded"),
            merging,
            observed(json!([]), 2, "merge", "succeeded"),
        ]
    );
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

// REQ-RUNNERENROLL-88
#[tokio::test]
async fn run_ending_in_its_first_command_flushes_its_steps_split_at_64_a_report() {
    let mut fixture = fixture(TREE_DIGEST, "exited", "\"planned\"");
    let root = (0..70)
        .map(|index| step(&format!("step{index}"), "succeeded"))
        .collect::<Vec<_>>();
    stage_state(&fixture.log, "create", "", &[(1, root)]);
    stage(
        &fixture.log,
        "create",
        "children/implement/000002",
        "notes.txt",
        b"an element holds runs, not records\n",
    );
    stage_child_run(
        &fixture.log,
        "create",
        "children/implement/000002/000001",
        &[(1, vec![step("build", "failed")])],
    );
    let reports = execute(&mut fixture).await;

    assert_eq!(
        progress(&reports),
        [(1, 1, "running", json!([])), (2, 1, "running", json!([])),]
    );
    let frames = step_frames(&reports);
    assert_eq!(
        frames
            .iter()
            .map(|(_, steps)| steps.len())
            .collect::<Vec<_>>(),
        [64, 7]
    );
    let mut expected = (0..70)
        .map(|index| observed(json!([]), 1, &format!("step{index}"), "succeeded"))
        .collect::<Vec<_>>();
    expected.push(observed(
        json!([{ "step": "implement", "item": 2, "ordinal": 1 }]),
        1,
        "build",
        "failed",
    ));
    assert_eq!(reported_steps(&reports), expected);
    let last_progress = reports
        .iter()
        .rposition(|report| matches!(report, ExecutionReport::TreeProgress { .. }))
        .unwrap();
    assert_eq!(
        last_progress, 2,
        "the steps precede the transitions: {reports:#?}"
    );
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("planned"));
}

// REQ-RUNNERENROLL-88, REQ-LOCALRUN-177: the runner tells a child run from a fan-out
// element as the engine does, by `run.json`. An entry of `children/<step>/` that holds a
// `state.json` and no run record is an element, so the run beneath it is element 1's.
#[tokio::test]
async fn entry_without_a_run_record_is_a_fan_out_element_as_the_engine_reads_it() {
    let mut fixture = fixture(TREE_DIGEST, "exited", "\"planned\"");
    stage_state(
        &fixture.log,
        "create",
        "children/build/000001",
        &[(1, vec![step("stray", "running")])],
    );
    stage_child_run(
        &fixture.log,
        "create",
        "children/build/000001/000001",
        &[(1, vec![step("compile", "succeeded")])],
    );
    stage_child_run(
        &fixture.log,
        "create",
        "children/design/000001",
        &[(1, vec![step("plan", "succeeded")])],
    );
    let reports = execute(&mut fixture).await;

    let home = logged_lines(&fixture.log, "create.env")
        .into_iter()
        .find_map(|line| line.strip_prefix("UM_HOME=").map(PathBuf::from))
        .unwrap();
    let run =
        home.join("workflows/def_01k0z6r1w8f4jy2m7q9v3x5loc/runs/run_01k0z6r1w8f4jy2m7q9v3x5loc");
    let engine = um_execution::child_run_directories(&run)
        .unwrap()
        .into_iter()
        .map(|child| json!([{ "step": child.step, "item": child.item, "ordinal": child.ordinal }]))
        .collect::<Vec<_>>();
    let reported = reported_steps(&reports);
    assert_eq!(
        reported,
        [
            observed(
                json!([{ "step": "build", "item": 1, "ordinal": 1 }]),
                1,
                "compile",
                "succeeded"
            ),
            observed(
                json!([{ "step": "design", "ordinal": 1 }]),
                1,
                "plan",
                "succeeded"
            ),
        ]
    );
    assert_eq!(
        reported
            .iter()
            .map(|observation| {
                let mut run = observation["run"].clone();
                run[0]["item"] = observation["run"][0]
                    .get("item")
                    .cloned()
                    .unwrap_or(Value::Null);
                run
            })
            .collect::<Vec<_>>(),
        engine
    );
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("planned"));
}

// REQ-RUNNERENROLL-88
#[tokio::test]
async fn unreadable_state_records_report_nothing_and_leave_the_outcome() {
    let mut fixture = fixture(TREE_DIGEST, "exited", "\"planned\"");
    stage(
        &fixture.log,
        "create",
        "",
        "state.json",
        b"{\"attempts\": [",
    );
    stage(
        &fixture.log,
        "create",
        "children/design/000001",
        "run.json",
        b"{}\n",
    );
    stage_child_run(
        &fixture.log,
        "create",
        "children/design/000002",
        &[(
            1,
            vec![
                step("write", "succeeded"),
                step("Write", "succeeded"),
                step("review", "exploded"),
            ],
        )],
    );
    let reports = execute(&mut fixture).await;

    assert_eq!(
        step_frames(&reports),
        [(
            1,
            vec![observed(
                json!([{ "step": "design", "ordinal": 2 }]),
                1,
                "write",
                "succeeded"
            )]
        )]
    );
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("planned"));
}

// REQ-RUNNERENROLL-88: step observations the outbox has no room for cost the run nothing.
// It ends as its record reads, its own reports queued in the slots held back for them, and
// the refusal is on its run event rather than a fault.
#[tokio::test]
async fn step_report_the_outbox_refuses_leaves_the_result_and_is_recorded_on_the_run_event() {
    let (recorder, capture) = crate::telemetry::test_recorder("rbt_fixture");
    let mut fixture = fixture_recording(TREE_DIGEST, "exited", "\"planned\"", &[], Some(recorder));
    stage_state(
        &fixture.log,
        "create",
        "",
        &[(1, vec![step("build", "succeeded")])],
    );
    assert_eq!(
        prepare_offered(&mut fixture.manager, &fixture.offer).await,
        None
    );
    // Only the slots held back for a run's own reports are left.
    enqueue_transitions(
        &fixture.manager,
        (MAXIMUM_SERVICE_OBSERVATIONS - OBSERVATION_RESERVE_BASE) as u64,
    );
    start_offered(&mut fixture.manager, &fixture.offer).spawn();
    let reports = finished_reports(&mut fixture.manager).await;

    assert!(step_frames(&reports).is_empty());
    assert_eq!(terminal_count(&reports), 1);
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("planned"));
    let run = capture.event("runner.run");
    assert_eq!(run["um.run.result"], "succeeded");
    assert_eq!(
        run["um.run.step_report_refusal"],
        "start_observation_capacity_exceeded"
    );
    assert_eq!(run.get("um.executor_fault.reason"), None);
}

// REQ-RUNNERENROLL-99: a `mirror/` the runner cannot read, here beside a valid write, reports
// no writes and requests no release, and the run event records the read failure's class.
#[tokio::test]
async fn unreadable_mirror_reports_no_writes_and_is_recorded_on_the_run_event() {
    let departed = serde_json::to_vec(&state_write("written", json!({}))).unwrap();
    for (name, bytes) in [
        ("notes.txt", b"not a mirror write\n".to_vec()),
        ("0002-outcome.json", departed[..departed.len() / 2].to_vec()),
    ] {
        let mirror = tempfile::tempdir().unwrap();
        fs::write(
            mirror.path().join("0001-start.json"),
            serde_json::to_vec(&comment_written()).unwrap(),
        )
        .unwrap();
        fs::write(mirror.path().join(name), bytes).unwrap();
        let (recorder, capture) = crate::telemetry::test_recorder("rbt_fixture");
        let mut fixture = fixture_recording(
            TREE_DIGEST,
            "exited",
            "\"landed\"",
            &[("TREE_TEST_MIRROR", mirror.path().to_str().unwrap())],
            Some(recorder),
        );
        let reports = execute(&mut fixture).await;

        let Some(ExecutionReport::TreeFinished { exit, .. }) = reports.last() else {
            panic!("expected a tree finish for {name}: {reports:#?}");
        };
        assert_eq!(
            exit,
            &TreeExit {
                chosen_outcome: Some("landed".to_owned()),
                mirror: Vec::new(),
                claim_release: false,
            },
            "{name}"
        );
        let run = capture.event("runner.run");
        assert_eq!(run["um.run.result"], "succeeded", "{name}");
        assert_eq!(
            run["um.run.mirror_read_failure"], "mirror_write_invalid",
            "{name}"
        );
    }
}

/// Executes `fixture` and asserts that its run record could not be read: the attempt is
/// aborted `runner_internal_failure` with no finish, its run event naming the fault.
async fn assert_record_unreadable(
    fixture: &mut Fixture,
    capture: &crate::telemetry::TestCapture,
    case: &str,
) {
    let reports = execute(fixture).await;

    assert!(
        matches!(
            reports.last(),
            Some(ExecutionReport::Aborted { reason, .. }) if reason == "runner_internal_failure"
        ),
        "{case}: {reports:#?}"
    );
    assert!(
        !reports
            .iter()
            .any(|report| matches!(report, ExecutionReport::TreeFinished { .. })),
        "{case}"
    );
    let run = capture.event("runner.run");
    assert_eq!(
        run["um.executor_fault.reason"], "runner_internal_failure",
        "{case}"
    );
    assert_eq!(
        run["um.failure.cause_type"], "tree_run_record_unavailable",
        "{case}"
    );
}

// REQ-RUNNERENROLL-99: the record is read as the engine reads it, a regular file of bounded
// size. A `run.json` replaced by a symbolic link to a record outside the run directory is
// not followed: the run has no truthful result.
#[tokio::test]
async fn run_record_behind_a_symbolic_link_is_unreadable() {
    let (recorder, capture) = crate::telemetry::test_recorder("rbt_fixture");
    let mut fixture = fixture_recording(TREE_DIGEST, "exited", "\"planned\"", &[], Some(recorder));
    let outside = fixture.log.join("outside.json");
    let stage = fixture.log.join("stages/create");
    fs::create_dir_all(&stage).unwrap();
    std::os::unix::fs::symlink(&outside, stage.join("run.json")).unwrap();
    assert_record_unreadable(&mut fixture, &capture, "symbolic link").await;

    // `create` wrote its exited record through the link, where a following read finds it.
    let followed: Value = serde_json::from_slice(&fs::read(&outside).unwrap()).unwrap();
    assert_eq!(followed["state"], "exited");
}

// REQ-RUNNERENROLL-99: a `run.json` larger than the engine's bound on a durable record is not
// read whole, though it parses as an exited record.
#[tokio::test]
async fn run_record_larger_than_the_bound_is_unreadable() {
    let (recorder, capture) = crate::telemetry::test_recorder("rbt_fixture");
    let mut fixture = fixture_recording(TREE_DIGEST, "exited", "\"planned\"", &[], Some(recorder));
    let mut oversized =
        br#"{"state":"exited","outcome":"planned","attempts":1,"position":[]}"#.to_vec();
    let bound = usize::try_from(um_execution::MAXIMUM_DURABLE_RECORD_BYTES).unwrap();
    oversized.resize(bound + 1, b' ');
    stage(&fixture.log, "exited", "", "run.json", &oversized);
    assert_record_unreadable(&mut fixture, &capture, "oversized").await;
}

/// A report sink that refuses a report for steps alone while `refusing` is set, keeping
/// each report it was offered beside whether it queued it.
struct StepRefusingSink {
    refusing: std::sync::atomic::AtomicBool,
    offered: Mutex<Vec<(bool, ExecutionReport)>>,
}

impl StepRefusingSink {
    fn new() -> Self {
        Self {
            refusing: std::sync::atomic::AtomicBool::new(true),
            offered: Mutex::new(Vec::new()),
        }
    }

    fn offer(&self, report: ExecutionReport) -> bool {
        let queued =
            !(report.is_steps_only() && self.refusing.load(std::sync::atomic::Ordering::SeqCst));
        self.offered.lock().unwrap().push((queued, report));
        queued
    }

    /// The reports offered so far that were queued, or that were refused.
    fn reports(&self, queued: bool) -> Vec<ExecutionReport> {
        self.offered
            .lock()
            .unwrap()
            .iter()
            .filter(|(was_queued, _)| *was_queued == queued)
            .map(|(_, report)| report.clone())
            .collect()
    }
}

// REQ-RUNNERENROLL-88: a report for steps alone that could not be queued does not end the
// run. Its observations are offered again under the same sequence with the next read, and
// queued once there is room, each once.
#[tokio::test]
async fn refused_steps_only_report_is_offered_again_and_the_run_goes_on() {
    let adopted = adopted(record("running", None, 1, json!(["build"])), None);
    let run = adopt_run(&adopted, &[], "continue").unwrap();
    let sink = StepRefusingSink::new();
    let merging = observed(json!([]), 2, "merge", "running");
    let script = async {
        // The resumed attempt is running and its root reported before its step starts.
        while !progress(&sink.reports(true))
            .iter()
            .any(|(_, attempt, ..)| *attempt == 2)
        {
            pause().await;
        }
        let state = json!({
            "attempts": [{ "attemptNumber": 2, "progress": { "steps": [step("merge", "running")] } }],
        });
        let written = adopted.log.join("state.json");
        fs::write(&written, serde_json::to_vec(&state).unwrap()).unwrap();
        fs::rename(&written, adopted.run.join("state.json")).unwrap();
        while sink.reports(false).len() < 2 {
            pause().await;
        }
        sink.refusing
            .store(false, std::sync::atomic::Ordering::SeqCst);
        while !reported_steps(&sink.reports(true)).contains(&merging) {
            pause().await;
        }
        fs::write(adopted.log.join("proceed"), b"").unwrap();
    };
    let offer = |report| sink.offer(report);
    let driving = run.drive(&offer);
    tokio::pin!(driving);
    let end = tokio::select! {
        end = &mut driving => panic!("the run ended before its step was queued: {end:#?}"),
        () = script => driving.await,
    };

    succeeded_with(&end.unwrap(), "landed");
    let queued = sink.reports(true);
    assert_eq!(
        progress(&queued)
            .iter()
            .map(|(sequence, ..)| *sequence)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(reported_steps(&queued), std::slice::from_ref(&merging));
    for refused in step_frames(&sink.reports(false)) {
        assert_eq!(refused, (3, vec![merging.clone()]));
    }
}

// REQ-RUNNERENROLL-88: the steps flushed before a run's exit are no more owed than any
// other, so a run whose flush could not be queued still reports its result; a report that
// carries a new root position is the run's own, and one refused still ends the attempt.
#[tokio::test]
async fn refused_flush_leaves_the_result_and_a_refused_root_report_ends_the_attempt() {
    let failed = adopted_failure(&[]);
    let run = adopt_run(&failed, &[], "").unwrap();
    let sink = StepRefusingSink::new();
    let end = run.drive(&|report| sink.offer(report)).await.unwrap();
    assert!(matches!(
        end.conclusion(),
        TreeConclusion::Terminal {
            result: TransportResult::Failed { .. },
            ..
        }
    ));
    assert_eq!(
        step_frames(&sink.reports(false)),
        [(1, vec![observed(json!([]), 1, "build", "failed")])]
    );

    let running = adopted(record("running", None, 1, json!(["build"])), None);
    let run = adopt_run(&running, &[], "continue").unwrap();
    let end = run.drive(&|_| false).await;
    assert_eq!(end.err(), Some(super::DriveFailure::ReportUnqueued));
    assert_eq!(resumes(&running.log), 0);
}

const ADOPTED_DEFINITION: &str = "def_01k0z6r1w8f4jy2m7q9v3x5loc";
const ADOPTED_RUN: &str = "run_01k0z6r1w8f4jy2m7q9v3x5loc";

/// A run-scoped home as the assignment that created the run leaves it, and the private
/// directory of the assignment that adopts it.
struct Adopted {
    _temporary: tempfile::TempDir,
    /// The creating assignment's private directory, which holds the home.
    first: PathBuf,
    home: PathBuf,
    run: PathBuf,
    private: PathBuf,
    log: PathBuf,
    um: PathBuf,
}

/// A home whose one run directory holds `record` as its `run.json`, and `wait` as its
/// `wait.json` when given.
fn adopted(record: Value, wait: Option<Value>) -> Adopted {
    let temporary = tempfile::tempdir().unwrap();
    let first = temporary.path().join("first/private");
    let home = super::tree_home(&first);
    let run = home
        .join("workflows")
        .join(ADOPTED_DEFINITION)
        .join("runs")
        .join(ADOPTED_RUN);
    let private = temporary.path().join("second/private");
    let log = temporary.path().join("log");
    for directory in [&run, &private, &log] {
        fs::create_dir_all(directory).unwrap();
    }
    fs::write(run.join("run.json"), serde_json::to_vec(&record).unwrap()).unwrap();
    if let Some(wait) = wait {
        fs::write(run.join("wait.json"), serde_json::to_vec(&wait).unwrap()).unwrap();
    }
    let um = injected_um(temporary.path());
    Adopted {
        _temporary: temporary,
        first,
        home,
        run,
        private,
        log,
        um,
    }
}

fn record(state: &str, outcome: Option<&str>, attempts: u64, position: Value) -> Value {
    json!({ "state": state, "outcome": outcome, "attempts": attempts, "position": position })
}

/// The adopting assignment's tree run, its environment the adopting attempt's.
fn adopt_run(
    adopted: &Adopted,
    steps: &[&str],
    resumes: &str,
) -> Result<super::TreeRun, AssignmentDecline> {
    adopt_run_with(adopted, steps, resumes, "", &[])
}

/// The adopting assignment's tree run, its offer carrying the `pending` deliveries the
/// injected `um` answers by `event`.
fn adopt_run_with(
    adopted: &Adopted,
    steps: &[&str],
    resumes: &str,
    event: &str,
    pending: &[TriggerDelivery],
) -> Result<super::TreeRun, AssignmentDecline> {
    adopt_run_at(
        adopted,
        steps,
        resumes,
        event,
        pending,
        TreeRunDependencies::new(None).with_resume_intervals(TEST_INTERVALS),
    )
}

/// `adopt_run_with`, with `dependencies` but for the injected `um`.
fn adopt_run_at(
    adopted: &Adopted,
    steps: &[&str],
    resumes: &str,
    event: &str,
    pending: &[TriggerDelivery],
    dependencies: TreeRunDependencies,
) -> Result<super::TreeRun, AssignmentDecline> {
    let CloudFrame::AssignmentOffer {
        assignment_id,
        execution_spec,
        ..
    } = tree_offer()
    else {
        panic!("expected an assignment offer");
    };
    let environment = EnvironmentSnapshot::new([
        ("PATH", std::env::var_os("PATH").unwrap_or_default()),
        ("TREE_TEST_LOG", adopted.log.clone().into_os_string()),
        ("TREE_TEST_RESUMES", resumes.into()),
        ("TREE_TEST_EVENT", event.into()),
        ("GITHUB_TOKEN", "adopting-attempt-token".into()),
    ]);
    let steps = steps
        .iter()
        .map(|step| (*step).to_owned())
        .collect::<Vec<_>>();
    super::adopt(
        &dependencies.with_executable(adopted.um.clone()),
        &super::TreeAdoption {
            assignment_id: &assignment_id,
            home: &adopted.home,
            private: &adopted.private,
            environment: &environment,
            execution_spec: &execution_spec,
            sleeper: Arc::new(crate::service::TokioSleeper),
            steps: &steps,
            pending_deliveries: pending,
        },
    )
}

/// Drives the adopted run to its end and returns the end and each progress report; an
/// adopted run given no delivery reports nothing else.
async fn drive_adopted(
    adopted: &Adopted,
    steps: &[&str],
    resumes: &str,
) -> (
    super::TreeEnd,
    Vec<(u64, um_runner_protocol::TreeRootProgress)>,
) {
    let run = adopt_run(adopted, steps, resumes).unwrap();
    let reports = Mutex::new(Vec::new());
    let end = run
        .drive(&|report| {
            let ExecutionReport::TreeProgress {
                progress_sequence,
                progress,
                ..
            } = report
            else {
                panic!("an adopted run given no delivery reports only progress: {report:?}");
            };
            reports.lock().unwrap().push((progress_sequence, progress));
            true
        })
        .await
        .unwrap();
    (end, reports.into_inner().unwrap())
}

/// Every file under `root` with its contents.
fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.insert(path.clone(), fs::read(&path).unwrap());
            }
        }
    }
    files
}

fn succeeded_with(end: &super::TreeEnd, outcome: &str) {
    let TreeConclusion::Terminal { result, exit, .. } = end.conclusion() else {
        panic!("an adopted run that exited has a result: {end:#?}");
    };
    assert_eq!(result, TransportResult::Succeeded);
    assert_eq!(exit.chosen_outcome.as_deref(), Some(outcome));
}

// REQ-RUNNERENROLL-89
#[tokio::test]
async fn adopted_waiting_run_resumes_on_the_wait_without_creating_a_run() {
    let adopted = adopted(
        record("waiting", None, 2, json!(["merge"])),
        Some(json!({
            "kind": "external",
            "since": "2026-07-23T00:00:05Z",
            "step": ["merge"],
            "then": "merge",
            "rule": { "source": "github", "event": "pull_request.merged" },
            "payload": {},
        })),
    );
    fs::write(adopted.log.join("proceed"), b"").unwrap();
    let (end, reports) = drive_adopted(&adopted, &[], "held continue").await;

    assert!(!adopted.log.join("create.args").exists());
    // The wait is evaluated on entry, then again once the held interval passes.
    assert_eq!(resumes(&adopted.log), 2);
    let (sequence, first) = &reports[0];
    assert_eq!((*sequence, first.attempt), (1, 2));
    assert_eq!(
        first.wait.as_ref().map(|wait| wait.kind.as_str()),
        Some("external")
    );
    // The run ends on the attempt the wait's continuation started.
    assert_eq!(end.record.as_ref().map(|record| record.attempts), Some(3));
    succeeded_with(&end, "landed");
}

// REQ-RUNNERENROLL-89
#[tokio::test]
async fn adopted_running_run_without_its_process_is_resumed_once() {
    let adopted = adopted(record("running", None, 2, json!(["build"])), None);
    fs::write(adopted.log.join("proceed"), b"").unwrap();
    let (end, reports) = drive_adopted(&adopted, &[], "continue").await;

    assert_eq!(resumes(&adopted.log), 1);
    assert_eq!(
        logged_lines(&adopted.log, "resume.args"),
        [format!(
            "workflow run resume {} --json",
            adopted.run.display()
        )]
    );
    assert_eq!(reports[0].1.attempt, 2);
    succeeded_with(&end, "landed");
}

// REQ-RUNNERENROLL-89
#[tokio::test]
async fn adopted_exited_run_is_reported_as_it_reads_without_a_command() {
    let adopted = adopted(record("exited", Some("landed"), 1, json!([])), None);
    let home = files(&adopted.home);
    let (end, _) = drive_adopted(&adopted, &[], "").await;

    assert!(!adopted.log.join("create.args").exists());
    assert_eq!(resumes(&adopted.log), 0);
    succeeded_with(&end, "landed");
    let TreeConclusion::Terminal { exit, .. } = end.conclusion() else {
        panic!("an exited record has a result");
    };
    assert_eq!(
        exit,
        TreeExit {
            chosen_outcome: Some("landed".to_owned()),
            mirror: Vec::new(),
            claim_release: false,
        }
    );
    assert_eq!(files(&adopted.home), home);
}

// REQ-RUNNERENROLL-89
#[tokio::test]
async fn adopted_failed_record_reports_the_failure_its_attempt_recorded() {
    let adopted = adopted(record("exited", Some("failed"), 1, json!([])), None);
    let detail = json!({ "phase": "execution", "code": "command_exit", "exitCode": 2 });
    let state = json!({
        "schemaVersion": 1,
        "currentAttemptNumber": 1,
        "attempts": [{
            "attemptNumber": 1,
            "progress": { "steps": [
                { "id": "design", "role": "step", "state": "succeeded" },
                { "id": "build", "role": "step", "state": "failed", "detail": detail },
            ] },
        }],
    });
    fs::write(
        adopted.run.join("state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();
    let (end, _) = drive_adopted(&adopted, &[], "").await;

    assert_eq!(resumes(&adopted.log), 0);
    let TreeConclusion::Terminal { result, exit, .. } = end.conclusion() else {
        panic!("an adopted failed record has a result: {end:#?}");
    };
    let TransportResult::Failed {
        primary_issue,
        diagnostic,
    } = result
    else {
        panic!("a failed record fails the attempt");
    };
    assert_eq!(
        primary_issue,
        json!({ "node": { "id": "build", "role": "step" }, "state": "failed", "detail": detail })
    );
    assert_eq!(
        diagnostic,
        Some(json!({ "stage": "tree_run_adopted_failure" }))
    );
    assert_eq!(exit.chosen_outcome.as_deref(), Some("failed"));
    assert!(!exit.claim_release);
}

// REQ-RUNNERENROLL-89
#[tokio::test]
async fn adopted_run_resumes_from_the_named_steps_in_the_adopting_environment() {
    let adopted = adopted(record("exited", Some("failed"), 1, json!([])), None);
    fs::write(adopted.log.join("proceed"), b"").unwrap();
    let first = files(&adopted.first);
    let (end, _) = drive_adopted(&adopted, &["build", "review"], "continue").await;

    assert_eq!(
        logged_lines(&adopted.log, "resume.args"),
        [format!(
            "workflow run resume {} --from build --from review --json",
            adopted.run.display()
        )]
    );
    let environment = logged_lines(&adopted.log, "resume.env");
    assert!(environment.contains(&format!("UM_HOME={}", adopted.home.display())));
    assert!(environment.contains(&"GITHUB_TOKEN=adopting-attempt-token".to_owned()));
    succeeded_with(&end, "landed");
    // The adopting assignment keeps its own log and documents; under the adopted home only
    // the run's own `um` processes wrote.
    for own in ["tree-run.log", "tree-run.json"] {
        assert!(adopted.private.join(own).is_file(), "{own}");
    }
    let changed = files(&adopted.first)
        .into_iter()
        .filter(|(path, bytes)| first.get(path) != Some(bytes))
        .map(|(path, _)| path)
        .collect::<Vec<_>>();
    assert!(
        changed.iter().all(|path| path.starts_with(&adopted.run)),
        "{changed:#?}"
    );
}

// REQ-RUNNERENROLL-89
#[tokio::test]
async fn refused_first_resume_ends_the_attempt_failed_with_the_refusal_code() {
    let adopted = adopted(record("exited", Some("failed"), 1, json!([])), None);
    let (end, _) = drive_adopted(&adopted, &["build"], "refuse continue").await;

    assert_eq!(resumes(&adopted.log), 1);
    let TreeConclusion::Terminal {
        transitions,
        result,
        exit,
    } = end.conclusion()
    else {
        panic!("a refused first resume has a result: {end:#?}");
    };
    let TransportResult::Failed {
        primary_issue,
        diagnostic,
    } = &result
    else {
        panic!("a refused first resume fails the attempt");
    };
    assert_eq!(
        primary_issue["node"],
        json!({ "id": "build", "role": "step" })
    );
    assert_eq!(primary_issue["state"], "failed");
    assert_eq!(
        diagnostic,
        &Some(json!({
            "stage": "tree_run_continuation_refused",
            "declineCause": "continuation_nodes_unsatisfied",
        }))
    );
    assert_eq!(result.outcome()["outcome"], "failed");
    assert_eq!(transitions.last().unwrap()["to"]["state"], "failed");
    assert_eq!(
        exit,
        TreeExit {
            chosen_outcome: None,
            mirror: Vec::new(),
            claim_release: false,
        }
    );
}

// REQ-RUNNERENROLL-91: a `create` the runtime refuses, here with its rejection of a root whose
// `requires.tools` names a missing tool, fails the attempt with the runtime's code rather
// than aborting it; an owner whose name begins with `-` reaches `create` as its value.
#[tokio::test]
async fn refused_create_ends_the_attempt_failed_with_the_refusal_code() {
    let mut fixture = fixture(TREE_DIGEST, "refused", "null");
    fixture
        .offer
        .execution_spec
        .workflow_tree
        .as_mut()
        .unwrap()
        .owner = Some("-Ada".to_owned());

    let reports = execute(&mut fixture).await;

    assert!(
        logged_lines(&fixture.log, "create.args").contains(&"--owner=-Ada".to_owned()),
        "{:?}",
        logged_lines(&fixture.log, "create.args")
    );
    assert!(
        reports
            .iter()
            .all(|report| !matches!(report, ExecutionReport::Aborted { .. })),
        "{reports:#?}"
    );
    let payload = &finished_payload(&reports, &fixture.offer);
    assert_eq!(payload["outcome"]["outcome"], "failed");
    assert_eq!(
        payload["outcome"]["primaryIssue"],
        json!({
            "node": { "id": "build", "role": "step" },
            "state": "failed",
            "detail": { "phase": "start", "code": "step_unavailable" },
        })
    );
    assert_eq!(
        payload["diagnostic"],
        json!({
            "stage": "tree_run_create_refused",
            "declineCause": "required_tool_unavailable",
        })
    );
    assert!(payload.get("chosenOutcome").is_none());
    assert_eq!(payload["mirror"], json!([]));
    assert_eq!(payload["claimRelease"], json!(false));
}

/// The payload of the `execution_finished` frame the last of `reports` encodes for `offer`,
/// encoding validating it against the closed protocol schema.
fn finished_payload(reports: &[ExecutionReport], offer: &AssignmentOffer) -> Value {
    let Some(finished) = reports.last() else {
        panic!("expected a terminal report");
    };
    let frame = finished.runner_frame(
        um_runner_protocol::RunnerEnvelope {
            message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
            boot_id: BOOT.to_owned(),
            sequence: 1,
            sent_at: "2026-07-23T00:00:00Z".to_owned(),
        },
        offer.assignment_id.clone(),
        offer.attempt_id.clone(),
    );
    let encoded = um_runner_protocol::encode_runner_frame(&frame).unwrap();
    let mut frame: Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(frame["type"], "execution_finished");
    frame["payload"].take()
}

// REQ-RUNNERENROLL-88: a state record that could not be read is skipped only until it can
// be, its size and modification time unchanged or not.
#[test]
fn state_record_that_could_not_be_read_is_reported_once_it_can() {
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state.json");
    let attempts =
        json!([{ "attemptNumber": 1, "progress": { "steps": [step("build", "running")] } }]);
    fs::write(
        &state,
        serde_json::to_vec(&json!({ "attempts": attempts })).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o000)).unwrap();
    let mut reader = super::steps::StepReader::default();
    reader.scan(temporary.path());
    fs::set_permissions(&state, fs::Permissions::from_mode(0o600)).unwrap();

    let steps = reader
        .scan(temporary.path())
        .into_iter()
        .map(|observation| (observation.step, observation.state.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(steps, [("build".to_owned(), "running")]);
}

// REQ-RUNNERENROLL-88: a `state.json` that a step replaced with a symbolic link is not
// followed, as the engine does not follow it, so the file it points at reports no step.
#[test]
fn state_record_behind_a_symbolic_link_reports_no_step() {
    let temporary = tempfile::tempdir().unwrap();
    let run = temporary.path().join("run");
    fs::create_dir(&run).unwrap();
    let outside = temporary.path().join("outside.json");
    let attempts =
        json!([{ "attemptNumber": 1, "progress": { "steps": [step("build", "running")] } }]);
    fs::write(
        &outside,
        serde_json::to_vec(&json!({ "attempts": attempts })).unwrap(),
    )
    .unwrap();
    std::os::unix::fs::symlink(&outside, run.join("state.json")).unwrap();

    let mut reader = super::steps::StepReader::default();
    assert!(reader.scan(&run).is_empty());
}

// REQ-RUNNERENROLL-88: a refused first resume returns before the loop reads the record, and
// the adopted run's steps are still reported under the new attempt before its result.
#[tokio::test]
async fn refused_first_resume_reports_the_adopted_steps_before_its_result() {
    let adopted = adopted_failure(&[]);
    // No record interval elapses while the refused command runs.
    let intervals = ResumeIntervals {
        record: Duration::from_secs(3600),
        ..TEST_INTERVALS
    };
    let run = adopt_run_at(
        &adopted,
        &["build"],
        "refuse",
        "",
        &[],
        TreeRunDependencies::new(None).with_resume_intervals(intervals),
    )
    .unwrap();
    let reports = Mutex::new(Vec::new());
    let end = run
        .drive(&|report| {
            reports.lock().unwrap().push(report);
            true
        })
        .await
        .unwrap();

    assert!(matches!(
        end.conclusion(),
        TreeConclusion::Terminal {
            result: TransportResult::Failed { .. },
            ..
        }
    ));
    assert_eq!(
        step_frames(&reports.into_inner().unwrap()),
        [(1, vec![observed(json!([]), 1, "build", "failed")])]
    );
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-89: a first resume from named steps that is not
// refused and starts no engine attempt leaves the record as the earlier attempt wrote it;
// the adopting attempt is not reported as that record reads.
#[tokio::test]
async fn first_resume_that_starts_no_attempt_is_not_reported_as_the_adopted_record_reads() {
    let adopted = adopted(record("exited", Some("landed"), 1, json!([])), None);
    let run = adopt_run(&adopted, &["build"], "held").unwrap();
    let end = run.drive(&|_| true).await;

    assert_eq!(resumes(&adopted.log), 1);
    assert_eq!(
        end.err(),
        Some(super::DriveFailure::Cause("tree_run_resume_stalled"))
    );
}

/// One short wait of a test's poll for something another task or process does.
async fn pause() {
    crate::service::Sleeper::sleep(&crate::service::TokioSleeper, Duration::from_millis(5)).await;
}

/// Drives the adopted run from `build`, its first `resume --from` starting no engine attempt
/// before `reason`'s cancellation reaches it, and returns the loop's end.
async fn cancel_unstarted_resume(adopted: &Adopted, reason: CancellationReason) -> super::TreeEnd {
    let run = adopt_run(adopted, &["build"], "stall").unwrap();
    let resumed = adopted.log.join("resume.log");
    let cancel = async {
        // The line, not the file: the redirection creates the file before the line is written.
        while !fs::read_to_string(&resumed).is_ok_and(|resumed| resumed.ends_with('\n')) {
            pause().await;
        }
        assert!(run.cancellation().source().request_cancellation(reason));
    };
    let (end, ()) = tokio::join!(run.drive(&|_| true), cancel);
    end.unwrap()
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-89: a cancellation that reaches an adopted run's
// first `resume --from` before it starts an engine attempt ends the adopting attempt
// cancelled or interrupted by its own reason. The record still reads as the earlier attempt
// left it, so its outcome, mirror writes and release request are not this attempt's, and
// the retained run is left as it was.
#[tokio::test]
async fn cancelled_first_resume_that_started_no_attempt_reports_nothing_of_the_adopted_record() {
    let departed = [comment_written(), state_write("written", json!({}))];
    let cases = [
        (
            adopted(record("exited", Some("landed"), 1, json!([])), None),
            CancellationReason::UserRequest,
            TransportResult::Cancelled {
                reason: CancellationReason::UserRequest,
            },
        ),
        (
            adopted_failure(&departed),
            CancellationReason::RunnerShutdown,
            TransportResult::Interrupted {
                reason: CancellationReason::RunnerShutdown,
            },
        ),
        (
            adopted(record("exited", Some("canceled"), 1, json!([])), None),
            CancellationReason::ExecutionLeaseExpired,
            TransportResult::Interrupted {
                reason: CancellationReason::ExecutionLeaseExpired,
            },
        ),
    ];
    for (adopted, reason, expected) in cases {
        let retained = files(&adopted.run);
        let end = cancel_unstarted_resume(&adopted, reason).await;

        assert_eq!(resumes(&adopted.log), 1, "{reason:?}");
        let TreeConclusion::Terminal { result, exit, .. } = end.conclusion() else {
            panic!("a cancelled first resume has a result: {end:#?}");
        };
        assert_eq!(result, expected);
        assert_eq!(
            exit,
            TreeExit {
                chosen_outcome: None,
                mirror: Vec::new(),
                claim_release: false,
            },
            "{reason:?}"
        );
        assert_eq!(files(&adopted.run), retained, "{reason:?}");
    }
}

/// A home whose one run reads `exited` `failed`, its current attempt recording `build`
/// failed, beside `writes` as its `mirror/` files.
fn adopted_failure(writes: &[Value]) -> Adopted {
    let adopted = adopted(record("exited", Some("failed"), 1, json!([])), None);
    let state = json!({
        "schemaVersion": 1,
        "currentAttemptNumber": 1,
        "attempts": [{
            "attemptNumber": 1,
            "progress": { "steps": [{
                "id": "build",
                "role": "step",
                "state": "failed",
                "detail": { "phase": "execution", "code": "command_exit", "exitCode": 2 },
            }] },
        }],
    });
    fs::write(
        adopted.run.join("state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();
    let mirror = adopted.run.join("mirror");
    fs::create_dir(&mirror).unwrap();
    for (index, write) in writes.iter().enumerate() {
        fs::write(
            mirror.join(format!(
                "{:04}-{}.json",
                index + 1,
                write["hook"].as_str().unwrap()
            )),
            serde_json::to_vec(write).unwrap(),
        )
        .unwrap();
    }
    adopted
}

// REQ-RUNNERENROLL-89, REQ-RUNNERENROLL-99: an adopted run's exit, the one concluded from a
// record that already read `exited` included, reports its run's mirror writes and release
// request as a created run's does; a refused first resume changed nothing and requests none.
#[tokio::test]
async fn adopted_exit_reports_the_run_mirror_and_a_refused_resume_releases_nothing() {
    let writes = [comment_written(), state_write("written", json!({}))];
    let recorded = adopted_failure(&writes);
    let (end, _) = drive_adopted(&recorded, &[], "").await;
    let TreeConclusion::Terminal { exit, .. } = end.conclusion() else {
        panic!("an adopted failed record has a result: {end:#?}");
    };
    assert_eq!(
        exit,
        TreeExit {
            chosen_outcome: Some("failed".to_owned()),
            mirror: writes.iter().cloned().map(mirror_result).collect(),
            claim_release: true,
        }
    );

    let refused = adopted_failure(&writes);
    let (end, _) = drive_adopted(&refused, &["build"], "refuse").await;
    let TreeConclusion::Terminal { exit, .. } = end.conclusion() else {
        panic!("a refused first resume has a result: {end:#?}");
    };
    assert_eq!(
        exit,
        TreeExit {
            chosen_outcome: None,
            mirror: Vec::new(),
            claim_release: false,
        }
    );
}

/// Drives the adopted run its offer carrying `pending` to its end and returns the end and
/// each report.
async fn drive_adopted_with(
    adopted: &Adopted,
    steps: &[&str],
    event: &str,
    pending: &[TriggerDelivery],
) -> (super::TreeEnd, Vec<ExecutionReport>) {
    let run = adopt_run_with(adopted, steps, "continue", event, pending).unwrap();
    let reports = Mutex::new(Vec::new());
    let end = run
        .drive(&|report| {
            reports.lock().unwrap().push(report);
            true
        })
        .await
        .unwrap();
    (end, reports.into_inner().unwrap())
}

// REQ-RUNNERENROLL-89, REQ-TRIGGER-136: the deliveries a continuation offer carries are
// applied to the adopted run before its first command; one that ends the standing run
// leaves its named steps unresumed, and a refused one lets the run resume from them.
#[tokio::test]
async fn adopted_run_applies_its_offer_pending_deliveries_before_its_first_command() {
    let waiting = adopted(
        record("waiting", None, 1, json!(["merge"])),
        Some(json!({
            "kind": "stop",
            "since": "2026-07-23T00:00:05Z",
            "step": ["merge"],
            "then": "merge",
            "rule": { "source": "um", "event": "pull_request.merged" },
            "payload": {},
        })),
    );
    let (end, reports) = drive_adopted_with(
        &waiting,
        &["merge"],
        "cancel",
        &[delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt")],
    )
    .await;
    assert_eq!(order(&waiting.log), ["event"]);
    assert_eq!(
        applications(&reports),
        [TriggerDeliveryApplication {
            delivery_id: "wtd_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned(),
            outcome: TriggerDeliveryOutcome::Applied(TriggerDeliveryAction::Cancel),
            observation: canceled_observation(),
        }]
    );
    let TreeConclusion::Terminal { result, exit, .. } = end.conclusion() else {
        panic!("a cancelled adopted run has a result: {end:#?}");
    };
    assert_eq!(result.outcome()["outcome"], "cancelled");
    assert_eq!(exit.chosen_outcome.as_deref(), Some("canceled"));

    let failed = adopted_failure(&[]);
    fs::write(failed.log.join("proceed"), b"").unwrap();
    let (end, reports) = drive_adopted_with(
        &failed,
        &["build"],
        "owned",
        &[delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abx")],
    )
    .await;
    assert_eq!(order(&failed.log), ["event", "resume"]);
    assert_eq!(
        applications(&reports)
            .iter()
            .map(|application| &application.outcome)
            .collect::<Vec<_>>(),
        [&TriggerDeliveryOutcome::Refused {
            code: "issue_in_owned_state".to_owned(),
        }]
    );
    succeeded_with(&end, "landed");
}

// REQ-RUNNERENROLL-89
#[test]
fn home_without_exactly_one_run_refuses_adoption() {
    let unavailable = AssignmentDecline::RunnerUnable(
        um_runner_protocol::RunnerUnableReason::RetainedWorkspaceUnavailable,
    )
    .diagnosed("workflow_tree_adoption", "tree_run_directory_unavailable");

    let none = adopted(record("exited", Some("landed"), 1, json!([])), None);
    fs::remove_dir_all(&none.run).unwrap();
    assert_eq!(adopt_run(&none, &[], "").err(), Some(unavailable.clone()));

    let two = adopted(record("exited", Some("landed"), 1, json!([])), None);
    fs::create_dir(two.run.with_file_name("run_01k0z6r1w8f4jy2m7q9v3x5lod")).unwrap();
    assert_eq!(adopt_run(&two, &[], "").err(), Some(unavailable.clone()));

    let homeless = adopted(record("exited", Some("landed"), 1, json!([])), None);
    fs::remove_dir_all(&homeless.home).unwrap();
    assert_eq!(adopt_run(&homeless, &[], "").err(), Some(unavailable));
}

/// The boot of the tree run manager's work root.
const BOOT: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";

/// The primary issue the injected `um` reports for a run failing in `build`.
const BUILD_FAILED: &str = r#"{"primaryIssue":{"node":{"id":"build","role":"step"},"state":"failed","detail":{"phase":"execution","code":"command_exit","exitCode":2}}}"#;

/// A manager whose first attempt's run fails in `build` and is left retained, with
/// `variables` added to the daemon's environment.
fn failed_fixture(variables: &[(&str, &str)]) -> Fixture {
    let variables = [("TREE_TEST_RESULT", BUILD_FAILED)]
        .into_iter()
        .chain(variables.iter().copied())
        .collect::<Vec<_>>();
    fixture_with(TREE_DIGEST, "exited", "\"failed\"", &variables)
}

/// The continuation of `prior` as its next attempt on boot `boot`, from `steps`, of the
/// retained workspace `execution_root`, its ids ending in `suffix`.
fn continuation_of(
    prior: &AssignmentOffer,
    boot: &str,
    execution_root: &Path,
    steps: &[&str],
    suffix: &str,
) -> AssignmentOffer {
    let mut next = prior.clone();
    next.effect_id = format!("eff_01k0z6r1w8f4jy2m7q9v3x5{suffix}");
    next.assignment_id = format!("asn_01k0z6r1w8f4jy2m7q9v3x5{suffix}");
    next.attempt_id = format!("atm_01k0z6r1w8f4jy2m7q9v3x5{suffix}");
    next.execution_spec.execution_spec_id = format!("xsp_01k0z6r1w8f4jy2m7q9v3x5{suffix}");
    next.attempt_number = prior.attempt_number + 1;
    let definition = &prior.execution_spec.workflow_definition_source;
    next.continuation = Some(Box::new(ContinuationOffer {
        prior_assignment_id: prior.assignment_id.clone(),
        prior_attempt_id: prior.attempt_id.clone(),
        required_runner_boot_id: boot.to_owned(),
        execution_root: execution_root.to_str().unwrap().to_owned(),
        definition_source: definition.clone(),
        effective_capacity: prior.execution_spec.capacity.clone(),
        prior_manifest_digest: definition.workflow_source_closure_digest.clone(),
        request: json!({ "fromSteps": steps, "definition": "inherited" }),
        reexecuted_steps: steps.iter().map(|step| (*step).to_owned()).collect(),
        inherited_steps: Vec::new(),
        prior_settlement_snapshot: None,
    }));
    next
}

/// What a `workspace_retention_report` names: assignment, attempt and execution root.
fn retention_reports(observations: &[AssignmentObservation]) -> Vec<(String, String, String)> {
    observations
        .iter()
        .filter_map(|observation| match observation {
            AssignmentObservation::WorkspaceRetention {
                assignment_id,
                attempt_id,
                execution_root,
                state,
                ..
            } => {
                assert_eq!(state, "retained");
                Some((
                    assignment_id.clone(),
                    attempt_id.clone(),
                    execution_root.clone(),
                ))
            }
            _ => None,
        })
        .collect()
}

/// Acknowledges every queued retention report, as Cloud does once it records one.
fn acknowledge_retention(manager: &mut AssignmentManager) {
    for pending in manager.pending_observations(&std::collections::BTreeSet::new(), 100) {
        if matches!(
            pending.observation,
            AssignmentObservation::WorkspaceRetention { .. }
        ) {
            manager.acknowledge_observation(pending.id);
        }
    }
}

/// Runs the fixture's first attempt to its terminal report, acknowledges it and its
/// retention report, and returns the workspace the report names.
async fn retained_first_attempt(fixture: &mut Fixture) -> PathBuf {
    let reports = execute(fixture).await;
    assert!(
        matches!(reports.last(), Some(ExecutionReport::TreeFinished { outcome, .. })
            if outcome["outcome"] == "failed"),
        "{reports:#?}"
    );
    let retained = retention_reports(&settled_observations(&mut fixture.manager).await);
    let [(assignment, attempt, root)] = retained.as_slice() else {
        panic!("one retention report: {retained:#?}");
    };
    assert_eq!(
        (assignment, attempt),
        (&fixture.offer.assignment_id, &fixture.offer.attempt_id)
    );
    let root = PathBuf::from(root);
    acknowledge_retention(&mut fixture.manager);
    root
}

/// Prepares, starts and runs the continuation `offer` to its acknowledged terminal
/// report, its effects ending in `suffix`.
async fn execute_continued(fixture: &mut Fixture, offer: &AssignmentOffer) -> Vec<ExecutionReport> {
    let suffix = &offer.assignment_id[offer.assignment_id.len() - 3..];
    let (decline, ready) = prepare_continued(
        &mut fixture.manager,
        offer,
        &format!("eff_01k0z6r1w8f4jy2m7q9v3x5p{}", &suffix[1..]),
    )
    .await;
    assert_eq!(decline, None);
    assert!(
        ready,
        "the runner sends continuation_ready before it accepts"
    );
    start_offered_with(
        &mut fixture.manager,
        offer,
        [
            &format!("eff_01k0z6r1w8f4jy2m7q9v3x5s{}", &suffix[1..]),
            &format!("eff_01k0z6r1w8f4jy2m7q9v3x5t{}", &suffix[1..]),
        ],
    )
    .spawn();
    finished_reports(&mut fixture.manager).await
}

/// The directory of `assignment` under the manager's work root on `BOOT`.
fn assignment_directory(workspace: &Path, assignment: &str) -> PathBuf {
    workspace.parent().unwrap().with_file_name(assignment)
}

/// The run directory the first attempt created under its home.
fn created_run(workspace: &Path, first: &AssignmentOffer) -> PathBuf {
    assignment_directory(workspace, &first.assignment_id)
        .join("private/home/workflows")
        .join(ADOPTED_DEFINITION)
        .join("runs")
        .join(ADOPTED_RUN)
}

// REQ-RUNNERENROLL-89, REQ-CONTINUE-83
#[tokio::test]
async fn continuation_offer_adopts_the_retained_run_and_resumes_it_from_the_named_steps() {
    let mut fixture = failed_fixture(&[]);
    let workspace = retained_first_attempt(&mut fixture).await;
    let first = fixture.offer.clone();
    let run = created_run(&workspace, &first);
    fs::remove_file(fixture.log.join("create.args")).unwrap();
    // A checkout would clone this repository; adoption needs none.
    fs::remove_dir_all(&fixture.source).unwrap();
    fs::write(fixture.log.join("answer"), "continue").unwrap();
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    let next = continuation_of(&first, BOOT, &workspace, &["build"], "cn1");

    let reports = execute_continued(&mut fixture, &next).await;

    assert_eq!(fixture.capability_requests.lock().unwrap().len(), 1);
    assert!(!fixture.log.join("create.args").exists());
    assert_eq!(
        logged_lines(&fixture.log, "resume.args"),
        [format!(
            "workflow run resume {} --from build --json",
            run.display()
        )]
    );
    let home = assignment_directory(&workspace, &first.assignment_id).join("private/home");
    assert!(
        logged_lines(&fixture.log, "resume.env").contains(&format!("UM_HOME={}", home.display()))
    );
    assert!(
        !assignment_directory(&workspace, &next.assignment_id)
            .join("workspace")
            .exists(),
        "the continuation executes in the retained workspace, not a new one"
    );
    assert!(
        reports
            .iter()
            .all(|report| !matches!(report, ExecutionReport::Aborted { .. }))
    );
    let Some(ExecutionReport::TreeFinished { outcome, exit, .. }) = reports.last() else {
        panic!("expected a tree finish: {reports:#?}");
    };
    assert_eq!(outcome["outcome"], "succeeded");
    assert_eq!(exit.chosen_outcome.as_deref(), Some("landed"));

    // A succeeded continuation releases what it claimed as a path continuation does: the
    // retained workspace and its own directory go, and nothing is reported retained.
    let settled = settled_observations(&mut fixture.manager).await;
    assert_eq!(retention_reports(&settled), []);
    assert!(!cleanup_failed(&mut fixture.manager));
    assert!(!workspace.exists());
    assert!(!assignment_directory(&workspace, &next.assignment_id).exists());
}

// REQ-RUNNERENROLL-89
#[tokio::test]
async fn continuation_offer_for_another_boot_is_declined_as_any_continuation() {
    let mut fixture = failed_fixture(&[]);
    let workspace = retained_first_attempt(&mut fixture).await;
    let next = continuation_of(
        &fixture.offer,
        "rbt_01k0z6r1w8f4jy2m7q9v3x5zzz",
        &workspace,
        &["build"],
        "cn1",
    );

    let (decline, ready) = prepare_continued(
        &mut fixture.manager,
        &next,
        "eff_01k0z6r1w8f4jy2m7q9v3x5pn1",
    )
    .await;

    assert_eq!(
        decline,
        Some(AssignmentDecline::RunnerUnable(
            RunnerUnableReason::ExecutionEnvironmentUnavailable
        ))
    );
    assert!(!ready);
    assert!(!fixture.log.join("resume.args").exists());
}

// REQ-RUNNERENROLL-89
#[tokio::test]
async fn retained_chain_without_a_tree_home_is_declined_retained_workspace_unavailable() {
    let mut fixture = failed_fixture(&[]);
    let workspace = retained_first_attempt(&mut fixture).await;
    fs::remove_dir_all(
        assignment_directory(&workspace, &fixture.offer.assignment_id).join("private/home"),
    )
    .unwrap();
    let next = continuation_of(&fixture.offer, BOOT, &workspace, &["build"], "cn1");

    let (decline, _) = prepare_continued(
        &mut fixture.manager,
        &next,
        "eff_01k0z6r1w8f4jy2m7q9v3x5pn1",
    )
    .await;

    assert_eq!(
        decline,
        Some(
            AssignmentDecline::RunnerUnable(RunnerUnableReason::RetainedWorkspaceUnavailable)
                .diagnosed("workflow_tree_adoption", "tree_run_home_unavailable")
        )
    );
    assert!(!fixture.log.join("resume.args").exists());
}

// REQ-RUNNERENROLL-89
#[tokio::test]
async fn unproven_retained_workspace_is_declined_ownership_unproven() {
    let mut fixture = failed_fixture(&[]);
    let workspace = retained_first_attempt(&mut fixture).await;
    let elsewhere = workspace.with_file_name("elsewhere");
    let next = continuation_of(&fixture.offer, BOOT, &elsewhere, &["build"], "cn1");

    let (decline, ready) = prepare_continued(
        &mut fixture.manager,
        &next,
        "eff_01k0z6r1w8f4jy2m7q9v3x5pn1",
    )
    .await;

    assert_eq!(
        decline,
        Some(AssignmentDecline::RunnerUnable(
            RunnerUnableReason::OwnershipUnproven
        ))
    );
    assert!(!ready);
    assert!(workspace.join("README").is_file());
}

// REQ-RUNNERENROLL-89, REQ-CONTINUE-83
#[tokio::test]
async fn refused_first_resume_fails_the_attempt_and_leaves_the_run_continuable() {
    let mut fixture = failed_fixture(&[]);
    let workspace = retained_first_attempt(&mut fixture).await;
    let first = fixture.offer.clone();
    fs::write(fixture.log.join("answer"), "refuse").unwrap();
    let refused = continuation_of(&first, BOOT, &workspace, &["build"], "cn1");

    let reports = execute_continued(&mut fixture, &refused).await;

    assert!(
        reports
            .iter()
            .all(|report| !matches!(report, ExecutionReport::Aborted { .. })),
        "{reports:#?}"
    );
    let payload = &finished_payload(&reports, &refused);
    assert_eq!(payload["outcome"]["outcome"], "failed");
    assert_eq!(
        payload["outcome"]["primaryIssue"]["node"],
        json!({ "id": "build", "role": "step" })
    );
    assert_eq!(
        payload["diagnostic"],
        json!({
            "stage": "tree_run_continuation_refused",
            "declineCause": "continuation_nodes_unsatisfied",
        })
    );
    assert!(payload.get("chosenOutcome").is_none());
    assert_eq!(payload["claimRelease"], json!(false));
    // The refused attempt keeps the workspace, reported for its own attempt.
    assert_eq!(
        retention_reports(&settled_observations(&mut fixture.manager).await),
        [(
            refused.assignment_id.clone(),
            refused.attempt_id.clone(),
            workspace.to_str().unwrap().to_owned(),
        )]
    );
    assert!(!cleanup_failed(&mut fixture.manager));
    acknowledge_retention(&mut fixture.manager);

    // A second continuation, whose prior attempt is the refused one, finds the home the
    // first attempt created through the chain and adopts the run again.
    fs::write(fixture.log.join("answer"), "continue").unwrap();
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    let again = continuation_of(&refused, BOOT, &workspace, &["build"], "cn2");
    let reports = execute_continued(&mut fixture, &again).await;

    let run = created_run(&workspace, &first);
    assert_eq!(
        logged_lines(&fixture.log, "resume.args"),
        [
            format!("workflow run resume {} --from build --json", run.display()),
            format!("workflow run resume {} --from build --json", run.display()),
        ]
    );
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

// REQ-RUNNERENROLL-89, REQ-CONTINUE-83
#[tokio::test]
async fn lease_lost_while_waiting_reports_the_retained_workspace_and_continues_at_the_wait() {
    let mut fixture = fixture_with(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            ("TREE_TEST_WAIT_KIND", "external"),
            ("TREE_TEST_WAIT_SOURCE", "github"),
        ],
    );
    let mut timers = control_lease_clock(&mut fixture.manager);
    let (lost, mut connection) = tokio::sync::watch::channel(false);
    // Time passes for the lease only while the connection is lost; the runner's own
    // containment polls pass at once.
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            tokio::select! {
                request = timers.recv() => match request {
                    Some(request) => held.push(request),
                    None => return,
                },
                changed = connection.changed() => if changed.is_err() {
                    return;
                },
            }
            let passing = *connection.borrow();
            let (due, waiting): (Vec<_>, Vec<_>) = held
                .into_iter()
                .partition(|(duration, _)| passing || *duration <= Duration::from_secs(1));
            held = waiting;
            for (_, release) in due {
                release.release();
            }
        }
    });
    start(&mut fixture).await;
    wait_for_reports(&mut fixture.manager, |reports| {
        progress(reports)
            .iter()
            .any(|(_, _, state, _)| *state == "waiting")
    })
    .await;
    let workspace = PathBuf::from(
        logged_lines(&fixture.log, "create.args")
            .windows(2)
            .find(|pair| pair[0] == "--execution-root")
            .map(|pair| pair[1].clone())
            .unwrap(),
    );

    // The connection is lost and stays lost past the lease.
    fixture.manager.finish_transport();
    lost.send(true).unwrap();
    let settled = settled_observations(&mut fixture.manager).await;
    lost.send(false).unwrap();

    // Back on the same boot, the session replays the retention report.
    fixture.manager.finish_transport();
    let replayed = fixture
        .manager
        .pending_observations(&std::collections::BTreeSet::new(), 100)
        .into_iter()
        .map(|pending| pending.observation)
        .collect::<Vec<_>>();
    let expected = [(
        fixture.offer.assignment_id.clone(),
        fixture.offer.attempt_id.clone(),
        workspace.to_str().unwrap().to_owned(),
    )];
    assert_eq!(retention_reports(&settled), expected);
    assert_eq!(retention_reports(&replayed), expected);
    acknowledge_retention(&mut fixture.manager);

    // Continued with no step named, the run resumes at its wait.
    let first = fixture.offer.clone();
    let resumed = resumes(&fixture.log);
    fs::write(fixture.log.join("answer"), "continue").unwrap();
    fs::write(fixture.log.join("proceed"), b"").unwrap();
    let next = continuation_of(&first, BOOT, &workspace, &[], "cn1");
    let reports = execute_continued(&mut fixture, &next).await;

    assert_eq!(resumes(&fixture.log), resumed + 1);
    assert_eq!(
        logged_lines(&fixture.log, "resume.args").last(),
        Some(&format!(
            "workflow run resume {} --json",
            created_run(&workspace, &first).display()
        ))
    );
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("landed"));
}

/// The `sleep` the hanging `event` started in its process group, once it has started it.
async fn hanging_event(log: &Path) -> rustix::process::Pid {
    let recorded = log.join("hang.pid");
    loop {
        if let Ok(pid) = fs::read_to_string(&recorded)
            && pid.ends_with('\n')
        {
            return rustix::process::Pid::from_raw(pid.trim().parse().unwrap()).unwrap();
        }
        pause().await;
    }
}

/// Whether `process` has ended, allowing for its reaping.
async fn ended(process: rustix::process::Pid) -> bool {
    for _ in 0..400 {
        if rustix::process::test_kill_process(process).is_err() {
            return true;
        }
        pause().await;
    }
    false
}

/// A fixture whose run waits on an external wait, its `event` hanging.
fn hanging_event_fixture() -> Fixture {
    fixture_with(
        TREE_DIGEST,
        "waiting",
        "null",
        &[
            ("TREE_TEST_WAIT_KIND", "external"),
            ("TREE_TEST_WAIT_SOURCE", "github"),
            ("TREE_TEST_EVENT", "hang"),
        ],
    )
}

// REQ-RUNNERENROLL-91: an `event` still running when the assignment is cancelled is
// abandoned, its process group killed, and reported as nothing; the run is then cancelled.
#[tokio::test]
async fn hanging_event_is_abandoned_with_its_group_when_the_assignment_is_cancelled() {
    let mut fixture = hanging_event_fixture();
    start(&mut fixture).await;
    wait_for_waiting(&mut fixture).await;
    deliver(&mut fixture, delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt"));
    let hang = hanging_event(&fixture.log).await;
    cancel_assignment(&mut fixture);
    let reports = finished_reports(&mut fixture.manager).await;

    assert!(
        ended(hang).await,
        "the event's process group was not killed"
    );
    assert_eq!(applications(&reports), []);
    assert_eq!(
        logged_lines(&fixture.log, "cancel.args")[..3],
        ["workflow", "run", "cancel"]
    );
    let Some(ExecutionReport::TreeFinished { outcome, .. }) = reports.last() else {
        panic!("expected a tree finish: {reports:#?}");
    };
    assert_eq!(outcome["outcome"], "cancelled");
    assert_eq!(chosen_outcome(&reports).as_deref(), Some("canceled"));
}

// REQ-RUNNERENROLL-91: lease loss abandons a hanging `event` at once, so the attempt is
// reported interrupted rather than held to its containment deadline.
#[tokio::test]
async fn hanging_event_does_not_delay_lease_loss() {
    let mut fixture = hanging_event_fixture();
    start(&mut fixture).await;
    wait_for_waiting(&mut fixture).await;
    deliver(&mut fixture, delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt"));
    let hang = hanging_event(&fixture.log).await;
    fixture
        .manager
        .handle_release(AssignmentRelease {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abr".to_owned(),
            assignment_id: fixture.offer.assignment_id.clone(),
            run_id: fixture.offer.run_id.clone(),
            attempt_id: fixture.offer.attempt_id.clone(),
            reason: "execution_lease_expired".to_owned(),
        })
        .unwrap();
    let reports = finished_reports(&mut fixture.manager).await;

    assert!(
        ended(hang).await,
        "the event's process group was not killed"
    );
    assert!(!fixture.log.join("cancel.args").exists());
    assert_eq!(applications(&reports), []);
    assert!(
        matches!(
            reports.last(),
            Some(ExecutionReport::Interrupted { reason, .. }) if reason == "execution_lease_expired"
        ),
        "{reports:#?}"
    );
}

/// An adopted run waiting on an external wait, which its next `resume` lands.
fn adopted_external_wait() -> Adopted {
    let waiting = adopted(
        record("waiting", None, 1, json!(["merge"])),
        Some(json!({
            "kind": "external",
            "since": "2026-07-23T00:00:05Z",
            "step": ["merge"],
            "then": "merge",
            "rule": { "source": "github", "event": "pull_request.merged" },
            "payload": {},
        })),
    );
    fs::write(waiting.log.join("proceed"), b"").unwrap();
    waiting
}

// REQ-RUNNERENROLL-91, REQ-RUNNERENROLL-90: an `event` that outlives the short-command bound
// is abandoned, its process group killed, and reported `refused` `event_command_failed`, the
// run's log naming the class `timeout`; the run goes on.
#[tokio::test]
async fn hanging_event_is_abandoned_with_its_group_once_its_bound_passes() {
    let waiting = adopted_external_wait();
    let run = adopt_run_at(
        &waiting,
        &[],
        "continue",
        "hang",
        &[delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt")],
        TreeRunDependencies::new(None)
            .with_resume_intervals(TEST_INTERVALS)
            .with_command_bound(Duration::from_millis(300)),
    )
    .unwrap();
    let reports = Mutex::new(Vec::new());
    let end = crate::service::test_support::with_watchdog(run.drive(&|report| {
        reports.lock().unwrap().push(report);
        true
    }))
    .await
    .expect("the hanging event was not abandoned at its bound")
    .unwrap();

    assert!(
        ended(hanging_event(&waiting.log).await).await,
        "the event's process group was not killed"
    );
    assert_eq!(
        applications(&reports.into_inner().unwrap()),
        [TriggerDeliveryApplication {
            delivery_id: "wtd_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned(),
            outcome: TriggerDeliveryOutcome::Refused {
                code: "event_command_failed".to_owned(),
            },
            observation: None,
        }]
    );
    let failed = failure_lines(&read_run_log(&waiting.private), "event_command_failed");
    assert_eq!(failed.len(), 1, "{failed:#?}");
    assert!(failed[0].contains("class=timeout"), "{}", failed[0]);
    assert!(!failed[0].contains("kind="), "{}", failed[0]);
    succeeded_with(&end, "landed");
}

// REQ-RUNNERENROLL-90, REQ-TRIGGER-136: a command that cannot be launched is refused, the
// run's log naming the class `launch_error` and the I/O error's kind; the start's own stop
// or the runner's lift that cannot be launched leaves the pending stop unlifted, the log
// naming the same.
#[tokio::test]
async fn commands_that_cannot_be_launched_log_their_class_and_error_kind() {
    // With the start's own stop the runner puts it back first; without one it only lifts.
    for (start_stop, command) in [(Some("review"), "stop"), (None, "lift")] {
        let mut fixture =
            pending_fixture("owned", vec![delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt")]);
        let tree = fixture.offer.execution_spec.workflow_tree.as_mut().unwrap();
        tree.stop_before = start_stop.map(str::to_owned);
        fs::write(fixture.log.join("unlaunchable"), b"").unwrap();
        let reports = execute(&mut fixture).await;

        assert_eq!(
            applications(&reports),
            [TriggerDeliveryApplication {
                delivery_id: "wtd_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned(),
                outcome: TriggerDeliveryOutcome::Refused {
                    code: "event_command_failed".to_owned(),
                },
                observation: None,
            }],
            "{command}"
        );
        let log = run_log(&fixture);
        for code in ["event_command_failed", "tree_run_pending_stop_unlifted"] {
            let failed = failure_lines(&log, code);
            assert_eq!(failed.len(), 1, "{command}: {failed:#?}");
            assert!(
                failed[0].contains("class=launch_error"),
                "{command}: {}",
                failed[0]
            );
            assert!(
                failed[0].contains("kind=NotFound"),
                "{command}: {}",
                failed[0]
            );
        }
    }
}

// REQ-RUNNERENROLL-91: the run's log keeps at most its cap of command output and then ends
// with one truncation line, however much later commands write.
#[tokio::test]
async fn run_log_stops_growing_at_its_cap_with_one_truncation_line() {
    let waiting = adopted_external_wait();
    // The `event` and the `resume` after it each write 40 MiB, together past the cap.
    fs::write(waiting.log.join("noise"), (40 * 1024 * 1024).to_string()).unwrap();
    let run = adopt_run_with(
        &waiting,
        &[],
        "continue",
        "owned",
        &[delivery("wtd_01k0z6r1w8f4jy2m7q9v3x5abt")],
    )
    .unwrap();
    let end = run.drive(&|_| true).await.unwrap();
    succeeded_with(&end, "landed");

    let log = fs::read(waiting.private.join("tree-run.log")).unwrap();
    let cap = usize::try_from(super::command::MAXIMUM_LOG_BYTES).unwrap();
    assert_eq!(log.len(), cap + super::command::LOG_TRUNCATED.len());
    assert!(log.ends_with(super::command::LOG_TRUNCATED));
}
