use std::collections::VecDeque;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::net::TcpStream as StandardTcpStream;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use rustix::process::Pid;
use serde_json::json;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

use super::*;
use crate::credential::test_credential;
use crate::service::assignment::test_support::{active_step_count, align_fixture_capacity};
use crate::service::config::{AssignmentConfig, Config, RepositoryUrlPolicy};
use crate::service::lease_clock::{
    ControlledLeaseClock, LeaseTimerRelease, controlled_lease_clock,
};
use crate::service::source::{
    CommitAvailability, CredentialBrokerFailure, ProviderCredential, WorkflowGitRevocation,
    test_support::{fixture_source_broker, unavailable_source_broker},
};
use crate::service::test_support::{controlled_sleeper, fixture_lease_clock, with_watchdog};
use crate::service::workspace::{
    CleanupCancellation, CleanupSleeper, OwnedTree, TreeRemover, WorkRootHook, WorkspaceFilesystem,
};
use um_execution::{
    CODEX_APP_SERVER_V1_QUALIFICATION_VERSION, ValidatedClaudeCodeInstallation,
    ValidatedCodexInstallation, ValidatedPiInstallation, resolve,
};
use um_runner_protocol::{
    ArtifactRegistrationOutcome, ArtifactRegistrationResponse, ArtifactResultRegistrationOutcome,
    ArtifactResultRegistrationResponse, CloudFrame, ExecutionCapacityV1RunnerProjection,
    ExecutionLimitsV1RunnerProjection, PrimaryWorkspaceSourceV1RunnerProjection,
    WorkflowDefinitionSourceV1RunnerProjection, WorkflowSourceClosureDigestV1RunnerProjection,
    decode_cloud_frame,
};

const NOW: &str = "2026-07-23T00:00:00Z";
mod artifact_delivery_tests;
mod harness_executable_tests;
use harness_executable_tests::{manager_fixture_with_harnesses, manager_fixture_with_pi};
const COMMAND_FIXTURE_TEST_NAME: &str = "service::assignment::tests::command_fixture_process";
const FAILING_COMMAND_FIXTURE_TEST_NAME: &str =
    "service::assignment::tests::failing_command_fixture_process";
// UM_* variables are intentionally removed from admitted command environments.
const COMMAND_FIXTURE_SOCKET: &str = "WORKFLOW_ASSIGNMENT_COMMAND_FIXTURE_SOCKET";

#[test]
fn final_acknowledgement_grace_matches_runner_timing_contract() {
    let fixture: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/timing.json"
    )))
    .unwrap();
    let duration = |field| Duration::from_secs(fixture[field].as_u64().unwrap());
    let ping = duration("pingIntervalSeconds");
    let pong = duration("pongTimeoutSeconds");
    let presence = duration("presenceLeaseSeconds");

    assert_eq!(
        FINAL_ACKNOWLEDGEMENT_GRACE,
        duration("finalAcknowledgementGraceSeconds")
    );
    assert_eq!(FINAL_ACKNOWLEDGEMENT_GRACE, ping);
    assert_eq!(presence, pong + 2 * ping);
}

const SUCCESSFUL_PI: &str = r#"#!/bin/sh
set -eu
printf '%s\0' "$*" >> "${0%/*}/pi.calls"
assistant='{"role":"assistant","content":[{"type":"text","text":"value"}],"api":"test-api","provider":"test-provider","model":"test-model","usage":{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":2}'
printf '{"type":"session","version":3,"id":"00000000-0000-4000-8000-000000000099","timestamp":"2026-08-04T00:00:00Z","cwd":"%s"}\n' "$PWD"
printf '%s\n' '{"type":"agent_start"}' '{"type":"turn_start"}'
printf '{"type":"message_start","message":%s}\n' "$assistant"
printf '{"type":"message_end","message":%s}\n' "$assistant"
printf '{"type":"turn_end","message":%s,"toolResults":[]}\n' "$assistant"
printf '{"type":"agent_end","messages":[%s],"willRetry":false}\n' "$assistant"
printf '%s\n' '{"type":"agent_settled"}'
"#;
const PI_ONLY_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: fixture/pi
        thinking: high
steps:
  pi:
    kind: agent
    agent:
      profile: coding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
"#;
const CLAUDE_CODE_ONLY_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: claude_code
      config:
        model: fixture/claude
        effort: xhigh
steps:
  claude:
    kind: agent
    agent:
      profile: coding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
"#;
const CODEX_ONLY_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: codex
      config:
        model: gpt-5.4
        effort: high
steps:
  codex:
    kind: agent
    agent:
      profile: coding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
"#;
const ALL_HARNESS_WORKFLOW: &str = r#"schemaVersion: 1
agentProfiles:
  piCoding:
    harness:
      kind: pi
      config:
        model: fixture/pi
        thinking: high
  claudeCoding:
    harness:
      kind: claude_code
      config:
        model: fixture/claude
        effort: xhigh
  codexCoding:
    harness:
      kind: codex
      config:
        model: gpt-5.4
        effort: high
steps:
  pi:
    kind: agent
    agent:
      profile: piCoding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
  claude:
    kind: agent
    dependsOn: [pi]
    agent:
      profile: claudeCoding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
  codex:
    kind: agent
    dependsOn: [claude]
    agent:
      profile: codexCoding
      systemPrompt: system.md
      message:
        text: [{ file: system.md }]
"#;
const SUCCESSFUL_CODEX: &str = r#"#!/bin/sh
set -eu
printf '%s\0' "$*" >> "${0%/*}/codex.calls"
for argument in "$@"; do
  case "$argument" in
    sqlite_home=\"*\")
      CODEX_FIXTURE_SQLITE_HOME=${argument#sqlite_home=\"}
      CODEX_FIXTURE_SQLITE_HOME=${CODEX_FIXTURE_SQLITE_HOME%\"}
      export CODEX_FIXTURE_SQLITE_HOME
      ;;
  esac
done
exec "$CODEX_FIXTURE_HELPER" \
  --exact service::assignment::tests::codex_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null
"#;
const SUCCESSFUL_CLAUDE_CODE: &str = r#"#!/bin/sh
set -eu
printf '%s\0' "$*" >> "${0%/*}/claude.calls"
model=
session=
previous=
for argument in "$@"; do
  if [ "$previous" = --model ]; then model=$argument; fi
  if [ "$previous" = --session-id ]; then session=$argument; fi
  previous=$argument
done
while IFS= read -r _; do :; done
printf '{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.284"}\n' "$PWD" "$session" "$model"
if [ "${CLAUDE_FIXTURE_FAIL-}" = 1 ]; then exit 23; fi
printf '{"type":"stream_event","event":{"type":"message_start","message":{"id":"msg-runner","type":"message","role":"assistant","content":[],"model":"%s","usage":{"input_tokens":1,"output_tokens":0}}},"session_id":"%s","parent_tool_use_id":null}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"value"}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"assistant","message":{"id":"msg-runner","type":"message","role":"assistant","content":[{"type":"text","text":"value"}],"model":"%s"},"parent_tool_use_id":null,"session_id":"%s"}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_stop","index":0},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_stop"},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"result","subtype":"success","is_error":false,"terminal_reason":"completed","result":"value","session_id":"%s"}\n' "$session"
"#;

enum GatedRootPreparationOutcome {
    Create(Arc<WorkRootLease>),
    Unavailable,
    CleanupFailed,
}

struct GatedAssignmentRootPreparer {
    started: tokio::sync::mpsc::UnboundedSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    outcome: GatedRootPreparationOutcome,
}

impl AssignmentRootPreparer for GatedAssignmentRootPreparer {
    fn prepare(
        &self,
        offer: &AssignmentOffer,
        recorder: Option<Arc<crate::telemetry::Recorder>>,
    ) -> Result<AssignmentRoot, AssignmentRootCreationError> {
        let _ = self.started.send(());
        self.release
            .lock()
            .expect("assignment root preparation gate mutex poisoned")
            .recv()
            .map_err(|_| AssignmentRootCreationError::Unavailable)?;
        match &self.outcome {
            GatedRootPreparationOutcome::Create(work_root) => work_root
                .create_assignment_for_attempt(
                    &offer.assignment_id,
                    &offer.run_id,
                    &offer.attempt_id,
                    recorder,
                ),
            GatedRootPreparationOutcome::Unavailable => {
                Err(AssignmentRootCreationError::Unavailable)
            }
            GatedRootPreparationOutcome::CleanupFailed => {
                Err(AssignmentRootCreationError::CleanupFailed)
            }
        }
    }
}

struct BlockingSourceBroker {
    started: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
    stopped: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
    calls: AtomicUsize,
}

impl SourceCredentialBroker for BlockingSourceBroker {
    fn issue(
        &self,
        _assignment_id: &str,
        cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        while !cancellation.is_cancelled() {
            um_support::sleep(Duration::from_millis(5));
        }
        if let Some(stopped) = self.stopped.lock().unwrap().take() {
            let _ = stopped.send(());
        }
        Err(CredentialBrokerFailure::Fenced)
    }

    fn commit_availability(
        &self,
        _assignment_id: &str,
        _cancellation: &CaptureCancellation,
    ) -> Result<CommitAvailability, CredentialBrokerFailure> {
        Err(CredentialBrokerFailure::Fenced)
    }

    // Preparation-cancellation tests never grant execution-time source authority.
    fn issue_workflow_git(
        &self,
        _assignment_id: &str,
        _cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        Err(CredentialBrokerFailure::Fenced)
    }

    fn revoke_workflow_git(
        &self,
        _assignment_id: &str,
        _token: &[u8],
    ) -> Result<WorkflowGitRevocation, CredentialBrokerFailure> {
        Err(CredentialBrokerFailure::Fenced)
    }
}

struct BlockingSourceFixture {
    broker: Arc<BlockingSourceBroker>,
    started: std::sync::mpsc::Receiver<()>,
    stopped: std::sync::mpsc::Receiver<()>,
}

impl BlockingSourceFixture {
    fn new() -> Self {
        let (started_sender, started) = std::sync::mpsc::sync_channel(1);
        let (stopped_sender, stopped) = std::sync::mpsc::sync_channel(1);
        Self {
            broker: Arc::new(BlockingSourceBroker {
                started: Mutex::new(Some(started_sender)),
                stopped: Mutex::new(Some(stopped_sender)),
                calls: AtomicUsize::new(0),
            }),
            started,
            stopped,
        }
    }

    fn broker(&self) -> Arc<BlockingSourceBroker> {
        Arc::clone(&self.broker)
    }

    fn calls(&self) -> usize {
        self.broker.calls.load(Ordering::Relaxed)
    }

    fn assert_not_started(&self) {
        assert!(self.started.try_recv().is_err());
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "wall time only bounds the blocking source fixture's readiness message"
    )]
    fn wait_until_started(&self) {
        assert!(self.started.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "wall time only bounds the blocking source fixture's cancellation message"
    )]
    fn wait_until_stopped(&self) {
        assert!(self.stopped.recv_timeout(Duration::from_secs(1)).is_ok());
    }
}

// Manager tests need a bool-scripted remover coupled to their admission fixture;
// the filesystem module keeps its richer partial-removal script local.
struct CleanupRemover {
    outcomes: Mutex<VecDeque<bool>>,
    calls: AtomicUsize,
}

impl CleanupRemover {
    fn new(outcomes: impl IntoIterator<Item = bool>) -> Arc<Self> {
        Arc::new(Self {
            outcomes: Mutex::new(outcomes.into_iter().collect()),
            calls: AtomicUsize::new(0),
        })
    }
}

impl TreeRemover for CleanupRemover {
    fn remove_tree(&self, tree: &OwnedTree) -> io::Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.outcomes.lock().unwrap().pop_front().unwrap_or(true) {
            fs::remove_dir_all(tree.path())
        } else {
            Err(io::Error::other("injected cleanup failure"))
        }
    }
}

struct CleanupSleepRequest {
    duration: Duration,
    release: std::sync::mpsc::SyncSender<()>,
}

struct GatedCleanupSleeper {
    requests: tokio::sync::mpsc::UnboundedSender<CleanupSleepRequest>,
}

impl CleanupSleeper for GatedCleanupSleeper {
    fn sleep(&self, duration: Duration, cancellation: &CleanupCancellation) -> bool {
        let (release, released) = std::sync::mpsc::sync_channel(1);
        if self
            .requests
            .send(CleanupSleepRequest { duration, release })
            .is_err()
        {
            return false;
        }
        released.recv().is_ok() && !cancellation.is_cancelled()
    }
}

struct CleanupHook;

impl WorkRootHook for CleanupHook {
    fn before_child_enumeration(&self) {}
}

fn run_command_fixture() {
    let socket = std::env::var(COMMAND_FIXTURE_SOCKET).unwrap();
    let mut control = StandardTcpStream::connect(socket).unwrap();
    control.write_all(&[1]).unwrap();
    control.flush().unwrap();
    let mut release = [0_u8; 1];
    control.read_exact(&mut release).unwrap();
    assert_eq!(release, [1]);
}

// The runner owns this narrow cross-package fixture because its tests launch the
// already-running root test binary. Keep only the success and process-quiescence
// scenarios needed to verify runner integration; the execution crate owns the full
// Codex protocol fixture and conformance matrix.
fn write_codex_fixture_frame(output: &mut impl std::io::Write, value: Value) {
    serde_json::to_writer(&mut *output, &value).unwrap();
    output.write_all(b"\n").unwrap();
    output.flush().unwrap();
}

fn read_codex_fixture_frame(
    input: &mut impl std::io::BufRead,
    capture: &mut impl std::io::Write,
) -> Value {
    let mut line = String::new();
    assert!(input.read_line(&mut line).unwrap() > 0);
    capture.write_all(line.as_bytes()).unwrap();
    capture.flush().unwrap();
    serde_json::from_str(line.trim_end()).unwrap()
}

fn codex_fixture_thread(cwd: &str, version: &str) -> Value {
    json!({
        "id": "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e12",
        "sessionId": "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e12",
        "forkedFromId": null,
        "parentThreadId": null,
        "ephemeral": true,
        "path": null,
        "cliVersion": version,
        "turns": [],
        "cwd": cwd,
        "modelProvider": "loopback",
    })
}

#[expect(
    clippy::zombie_processes,
    reason = "the runner process guard force-terminates and reaps this deliberate descendant"
)]
fn materialize_runner_stubborn_descendant() {
    let descendant = std::process::Command::new("/bin/sh")
        // Use one stubborn process: a shell loop can orphan its sleep child
        // during group teardown and keep the group observable after the
        // recorded descendant PID is gone. Close the fixture's protocol fd.
        .args(["-c", "exec 3>&-; trap '' INT TERM; exec sleep 60"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    fs::write(
        std::env::var_os("CODEX_FIXTURE_DESCENDANT").unwrap(),
        format!("{}\n", descendant.id()),
    )
    .unwrap();
}

#[test]
#[ignore = "launched only as the runner's deterministic Codex process fixture"]
fn codex_process_fixture() {
    const THREAD_ID: &str = "018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e12";
    const TURN_ID: &str = "turn-fixture";

    let scenario = std::env::var("CODEX_FIXTURE_SCENARIO").unwrap();
    assert!(matches!(
        scenario.as_str(),
        "no-value" | "success-stubborn" | "failure-after-start-stubborn" | "cancellation-stubborn"
    ));
    let sqlite_home = PathBuf::from(std::env::var_os("CODEX_FIXTURE_SQLITE_HOME").unwrap());
    assert!(sqlite_home.is_absolute());
    fs::write(
        sqlite_home.join("state_5.sqlite"),
        b"transient fixture state\n",
    )
    .unwrap();
    fs::write(
        std::env::var_os("CODEX_FIXTURE_PROCESS").unwrap(),
        format!("{}\n", std::process::id()),
    )
    .unwrap();

    let mut capture =
        fs::File::create(std::env::var_os("CODEX_FIXTURE_REQUESTS").unwrap()).unwrap();
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let mut output = fs::OpenOptions::new()
        .write(true)
        .open("/dev/fd/3")
        .unwrap();
    let version = std::env::var("CODEX_FIXTURE_VERSION").unwrap();

    let initialize = read_codex_fixture_frame(&mut input, &mut capture);
    assert_eq!(initialize["id"], 1);
    write_codex_fixture_frame(
        &mut output,
        json!({"id": 1, "result": {
            "userAgent": format!("codex/{version}"),
            "codexHome": std::env::var("CODEX_HOME").unwrap(),
        }}),
    );
    assert_eq!(
        read_codex_fixture_frame(&mut input, &mut capture)["method"],
        "initialized"
    );
    let config = read_codex_fixture_frame(&mut input, &mut capture);
    assert_eq!(config["id"], 2);
    write_codex_fixture_frame(
        &mut output,
        json!({
            "method": "configWarning",
            "params": {"summary": "synthetic effective configuration warning"},
        }),
    );
    write_codex_fixture_frame(
        &mut output,
        json!({"id": 2, "result": {
            "config": {
                "developer_instructions": "native developer instructions",
                "sqlite_home": std::env::var("CODEX_FIXTURE_SQLITE_HOME").unwrap(),
                "model_provider": "loopback",
                "model_providers": {"loopback": {"wire_api": "responses"}},
                "projects": {"fixture-project": {"trust_level": "trusted"}},
                "hooks": {"enabled": true},
                "mcp_servers": {"native": {"required": true}},
                "skills": {"enabled": true},
            },
            "origins": {"developer_instructions": {"name": {"type": "user"}}},
            "layers": [{"name": {"type": "user"}}],
        }}),
    );

    let thread = read_codex_fixture_frame(&mut input, &mut capture);
    assert_eq!(thread["id"], 3);
    let cwd = thread["params"]["cwd"].as_str().unwrap();
    let thread = codex_fixture_thread(cwd, &version);
    write_codex_fixture_frame(
        &mut output,
        json!({"id": 3, "result": {
            "thread": thread,
            "model": "gpt-5.4",
            "modelProvider": "loopback",
            "cwd": cwd,
            "approvalPolicy": "never",
            "sandbox": {"type": "dangerFullAccess"},
        }}),
    );
    let turn = read_codex_fixture_frame(&mut input, &mut capture);
    assert_eq!(turn["id"], 4);
    write_codex_fixture_frame(
        &mut output,
        json!({"method": "thread/started", "params": {"thread": thread}}),
    );
    write_codex_fixture_frame(
        &mut output,
        json!({"id": 4, "result": {"turn": {
            "id": TURN_ID, "items": [], "status": "inProgress"
        }}}),
    );
    write_codex_fixture_frame(
        &mut output,
        json!({"method": "turn/started", "params": {
            "threadId": THREAD_ID,
            "turn": {"id": TURN_ID, "items": [], "status": "inProgress"},
        }}),
    );

    if scenario == "cancellation-stubborn" {
        fs::write(std::env::var_os("CODEX_FIXTURE_READY").unwrap(), b"ready\n").unwrap();
        let interrupt = read_codex_fixture_frame(&mut input, &mut capture);
        assert_eq!(interrupt["method"], "turn/interrupt");
        materialize_runner_stubborn_descendant();
        loop {
            std::thread::park();
        }
    }
    if scenario == "success-stubborn" || scenario == "failure-after-start-stubborn" {
        materialize_runner_stubborn_descendant();
    }
    if scenario == "failure-after-start-stubborn" {
        write_codex_fixture_frame(
            &mut output,
            json!({"method": "error", "params": {
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "error": {"message": "native execution diagnostic", "codexErrorInfo": "other"},
                "willRetry": false,
            }}),
        );
        write_codex_fixture_frame(
            &mut output,
            json!({"method": "turn/completed", "params": {
                "threadId": THREAD_ID,
                "turn": {
                    "id": TURN_ID,
                    "items": [],
                    "status": "failed",
                    "error": {"message": "terminal prose differs", "codexErrorInfo": "other"},
                },
            }}),
        );
    } else {
        let response = std::env::var("CODEX_FIXTURE_RESPONSE").unwrap();
        write_codex_fixture_frame(
            &mut output,
            json!({"method": "item/started", "params": {
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "item": {"id": "message-1", "type": "agentMessage", "text": "", "phase": null},
            }}),
        );
        write_codex_fixture_frame(
            &mut output,
            json!({"method": "item/agentMessage/delta", "params": {
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "itemId": "message-1",
                "delta": response,
            }}),
        );
        write_codex_fixture_frame(
            &mut output,
            json!({"method": "item/completed", "params": {
                "threadId": THREAD_ID,
                "turnId": TURN_ID,
                "item": {"id": "message-1", "type": "agentMessage", "text": response, "phase": "final_answer"},
            }}),
        );
        write_codex_fixture_frame(
            &mut output,
            json!({"method": "turn/completed", "params": {
                "threadId": THREAD_ID,
                "turn": {
                    "id": TURN_ID,
                    "items": [{"id": "message-1", "type": "agentMessage", "text": response, "phase": "final_answer"}],
                    "status": "completed",
                },
            }}),
        );
    }
    let mut trailing = Vec::new();
    input.read_to_end(&mut trailing).unwrap();
    assert!(trailing.is_empty());
}

// Keep commands alive until the test observes them. Bare `true` and `false`
// commands can exit before the direct-child test path captures their process identity.
#[test]
#[ignore = "launched only as the assignment workflow command fixture"]
fn command_fixture_process() {
    run_command_fixture();
}

#[test]
#[ignore = "launched only as the failing assignment workflow command fixture"]
fn failing_command_fixture_process() {
    run_command_fixture();
    panic!("intentional assignment command fixture failure");
}

fn policy() -> ExecutionLeasePolicy {
    ExecutionLeasePolicy {
        schema_version: 2,
        force_stop_and_reap_budget_milliseconds: 5000,
        terminal_report_delivery_budget_milliseconds: 5000,
        renewal_delivery_budget_milliseconds: 5000,
        lease_duration_milliseconds: 371_000,
        fencing_margin_milliseconds: 11_000,
    }
}

fn offer(suffix: &str) -> AssignmentOffer {
    AssignmentOffer {
        effect_id: format!("eff_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
        assignment_id: format!("asn_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
        run_id: format!("run_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
        project_id: "prj_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        attempt_id: format!("atm_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
        attempt_number: 1,
        execution_spec: ExecutionSpecV1RunnerProjection {
            execution_spec_id: format!("xsp_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
            schema_version: 1,
            execution_limits: ExecutionLimitsV1RunnerProjection {
                maximum_parallel_steps: 1,
                cancellation_grace_seconds: 1,
            },
            source_branch: "main".to_owned(),
            workflow_definition_source: production_workflow_definition_source(),
            primary_workspace_source: production_primary_workspace_source(),
            source_display_snapshot: None,
            capacity: production_capacity(),
            run_inputs: None,
        },
        continuation: None,
    }
}

#[test]
fn offer_replay_must_preserve_pinned_continuation_identity() {
    let original = offer("bc");
    let mut altered = original.clone();
    altered.continuation = Some(Box::new(ContinuationOffer {
        prior_assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5aaa".to_owned(),
        prior_attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5aaa".to_owned(),
        required_runner_boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5aaa".to_owned(),
        execution_root: "/retained/workspace".to_owned(),
        definition_source: altered.execution_spec.workflow_definition_source.clone(),
        effective_capacity: altered.execution_spec.capacity.clone(),
        prior_manifest_digest: altered
            .execution_spec
            .workflow_definition_source
            .workflow_source_closure_digest
            .clone(),
        request: serde_json::json!({"fromSteps":["build"]}),
        reexecuted_steps: vec!["build".to_owned()],
        inherited_steps: Vec::new(),
        prior_settlement_snapshot: None,
    }));
    assert!(!same_assignment(&original, &altered));
    let mut another_parent = altered.clone();
    another_parent
        .continuation
        .as_mut()
        .unwrap()
        .prior_attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5aab".to_owned();
    assert!(!same_assignment(&altered, &another_parent));
    let mut another_project = original.clone();
    another_project.project_id = "prj_01k0z6r1w8f4jy2m7q9v3x5aab".to_owned();
    assert!(!same_assignment(&original, &another_project));
}

fn prepare_for(manager: &AssignmentManager, offered: &AssignmentOffer) -> AssignmentPrepare {
    let preparation_expires_at = (manager.sleeper.utc_now() + time::Duration::minutes(15))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    AssignmentPrepare {
        effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5acz".to_owned(),
        assignment_id: offered.assignment_id.clone(),
        run_id: offered.run_id.clone(),
        attempt_id: offered.attempt_id.clone(),
        execution_spec_id: offered.execution_spec.execution_spec_id.clone(),
        preparation_expires_at,
    }
}

fn source_fixture_path(manager: &AssignmentManager) -> PathBuf {
    manager
        .work_root
        .boot_path()
        .parent()
        .and_then(Path::parent)
        .expect("fixture work root should have a temporary parent")
        .join("source")
}

fn align_offer_with_source_fixture(manager: &AssignmentManager, offered: &mut AssignmentOffer) {
    let source = source_fixture_path(manager);
    let commit_oid = run_fixture_git(&source, &["rev-parse", "HEAD"]);
    offered
        .execution_spec
        .workflow_definition_source
        .workflow_path = "workflow.yaml".to_owned();
    offered.execution_spec.workflow_definition_source.commit_oid = commit_oid.clone();
    offered.execution_spec.primary_workspace_source.commit_oid = commit_oid;
    if let Ok(workflow) = resolve(&source, Path::new("workflow.yaml")) {
        align_fixture_capacity(&mut offered.execution_spec, &workflow);
    }
}

fn has_offer_preparation(manager: &mut AssignmentManager) -> bool {
    manager
        .pending_observations(&BTreeSet::new(), 10)
        .iter()
        .any(|pending| matches!(pending.observation, AssignmentObservation::Preparing { .. }))
}

async fn wait_for_offer_preparation(
    manager: &mut AssignmentManager,
) -> PendingAssignmentObservation {
    with_watchdog(async {
        let notification = manager.notification();
        loop {
            let notified = notification.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(preparation) = manager
                .pending_observations(&BTreeSet::new(), 10)
                .into_iter()
                .find(|pending| {
                    matches!(pending.observation, AssignmentObservation::Preparing { .. })
                })
            {
                return preparation;
            }
            notified.await;
        }
    })
    .await
    .expect("offer preparation acknowledgement timed out")
}

async fn begin_preparation(manager: &mut AssignmentManager, offered: &AssignmentOffer) {
    let preparation_id = wait_for_offer_preparation(manager).await.id;
    manager.acknowledge_observation(preparation_id);
    manager
        .handle_prepare(prepare_for(manager, offered))
        .unwrap();
}

async fn prepare_current(manager: &mut AssignmentManager, offered: &AssignmentOffer) {
    begin_preparation(manager, offered).await;
    with_watchdog(wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Preparing(_)))
    }))
    .await
    .expect("source materialization did not complete");
    let progress_ids = manager
        .pending_observations(&BTreeSet::new(), 10)
        .into_iter()
        .filter(|pending| {
            matches!(
                pending.observation,
                AssignmentObservation::PreparationProgress { .. }
            )
        })
        .map(|pending| pending.id)
        .collect::<Vec<_>>();
    for id in progress_ids {
        manager.acknowledge_observation(id);
    }
}

async fn offer_then_prepare(manager: &mut AssignmentManager, offered: &AssignmentOffer) {
    let mut aligned = offered.clone();
    align_offer_with_source_fixture(manager, &mut aligned);
    manager.handle_offer(aligned.clone()).unwrap();
    prepare_current(manager, &aligned).await;
}

async fn settle_cleanup(manager: &mut AssignmentManager) {
    with_watchdog(wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Releasing(_)))
    }))
    .await
    .expect("assignment cleanup did not complete");
}

async fn wait_for_execution_finalization(manager: &mut AssignmentManager) {
    with_watchdog(wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Running(_)))
    }))
    .await
    .expect("execution workspace disposition did not complete");
}

async fn acknowledge_terminal_and_settle(manager: &mut AssignmentManager) {
    wait_for_execution_finalization(manager).await;
    let terminal_id = manager
        .pending_observations(&BTreeSet::new(), 100)
        .into_iter()
        .find(|entry| entry.observation.is_terminal())
        .expect("terminal observation")
        .id;
    manager.acknowledge_observation(terminal_id);
    settle_cleanup(manager).await;
}

fn release_current(manager: &mut AssignmentManager, offered: &AssignmentOffer, reason: &str) {
    manager
        .handle_release(release_for(offered, "br", reason))
        .unwrap();
}

fn production_capacity() -> ExecutionCapacityV1RunnerProjection {
    let source_closure_digest =
        production_workflow_definition_source().workflow_source_closure_digest;
    ExecutionCapacityV1RunnerProjection {
        execution_contract: "workflow_v1_cloud_inputs_artifacts@1".to_owned(),
        source_closure_digest,
        general_maximum_transitions: 8,
        selected_maximum_transitions: 7,
        maximum_invocations: 1,
        maximum_retained_bytes_per_invocation: 4_194_304,
        diagnostic_retention_bytes: 8_388_608,
        native_session_retention_bytes: 4_194_304,
        aggregate_retention_bytes: 12_582_912,
        condition_transition_count: 0,
        aggregate_condition_transition_bytes: 0,
        terminal_result_structure_bytes: 67_108_864,
        presentation_result_bytes: 0,
        portable_result_bytes: 202_027_692,
        encoded_outbox_bytes: 85_458_944,
    }
}

fn production_workflow_definition_source() -> WorkflowDefinitionSourceV1RunnerProjection {
    WorkflowDefinitionSourceV1RunnerProjection {
        repository_connection_id: "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        object_format: "sha1".to_owned(),
        commit_oid: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        workflow_path: "workflows/build.yaml".to_owned(),
        workflow_source_closure_digest: WorkflowSourceClosureDigestV1RunnerProjection {
            algorithm: "sha256".to_owned(),
            value: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
        },
    }
}

fn production_primary_workspace_source() -> PrimaryWorkspaceSourceV1RunnerProjection {
    PrimaryWorkspaceSourceV1RunnerProjection {
        kind: "connected_repository".to_owned(),
        provider_kind: "github".to_owned(),
        repository_connection_id: "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        object_format: "sha1".to_owned(),
        commit_oid: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        materialization_contract: "git_full_clone_v1".to_owned(),
    }
}

fn assert_offer_rejected_without_cleanup(
    manager: &mut AssignmentManager,
    offered: AssignmentOffer,
    expected: AssignmentDecline,
) {
    let assignment_id = offered.assignment_id.clone();
    manager.handle_offer(offered).unwrap();
    assert!(
        manager
            .pending_observations(&BTreeSet::new(), 100)
            .iter()
            .any(|pending| matches!(
                &pending.observation,
                AssignmentObservation::Decision(AssignmentDecision::Rejected {
                    assignment_id: rejected_assignment_id,
                    decline,
                    ..
                }) if rejected_assignment_id == &assignment_id && decline == &expected
            ))
    );
}

async fn assert_offer_declined(
    manager: &mut AssignmentManager,
    offered: AssignmentOffer,
    expected: AssignmentDecline,
) {
    manager.handle_offer(offered).unwrap();
    let pending = manager.pending_observations(&BTreeSet::new(), 1);
    assert!(
        !pending.is_empty(),
        "assignment decline should become observable"
    );
    assert!(matches!(
        &pending[0].observation,
        AssignmentObservation::Decision(AssignmentDecision::Rejected { decline, .. })
            if *decline == expected
    ));
    settle_cleanup(manager).await;
    assert!(manager.slot.is_none());
}

async fn assert_preparation_declined(
    manager: &mut AssignmentManager,
    mut offered: AssignmentOffer,
    expected: AssignmentDecline,
) {
    align_offer_with_source_fixture(manager, &mut offered);
    manager.handle_offer(offered.clone()).unwrap();
    prepare_current(manager, &offered).await;
    let pending = manager.pending_observations(&BTreeSet::new(), 10);
    assert!(pending.iter().any(|entry| matches!(
        &entry.observation,
        AssignmentObservation::Decision(AssignmentDecision::Rejected { decline, .. })
            if *decline == expected
    )));
    settle_cleanup(manager).await;
    assert!(manager.slot.is_none());
}

fn manager_fixture(workflow: &str) -> (tempfile::TempDir, AssignmentManager) {
    manager_fixture_with_harnesses(workflow, None, None, None)
}

fn run_fixture_git(repository: &Path, arguments: &[&str]) -> String {
    let output = um_test_support::fixture_git_command("git")
        .current_dir(repository)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture git command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn initialize_source_repository(source: &Path) {
    run_fixture_git(source, &["init", "--quiet", "--object-format=sha1"]);
    run_fixture_git(source, &["config", "user.name", "Scherzo Fixture"]);
    run_fixture_git(source, &["config", "user.email", "fixture@scherzo.invalid"]);
    run_fixture_git(source, &["add", "."]);
    run_fixture_git(source, &["commit", "--quiet", "-m", "fixture"]);
}

fn base_manager_config(workflow: &str) -> (tempfile::TempDir, PathBuf, Config) {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    let work = temporary.path().join("work");
    fs::create_dir(&source).unwrap();
    fs::create_dir(&work).unwrap();
    fs::set_permissions(&work, fs::Permissions::from_mode(0o700)).unwrap();
    // Fixture programs use named host inputs; declare them in the source that
    // the runner actually admits instead of bypassing the environment policy.
    let workflow = if workflow.contains("environmentPassthrough:") {
        workflow.to_owned()
    } else {
        workflow.replacen(
            "schemaVersion: 1\n",
            "schemaVersion: 1\nenvironmentPassthrough: [WORKFLOW_ASSIGNMENT_COMMAND_FIXTURE_SOCKET, CLAUDE_CONFIG_DIR, CLAUDE_FIXTURE_FAIL, CODEX_HOME, CODEX_FIXTURE_HELPER, CODEX_FIXTURE_ARGUMENTS, CODEX_FIXTURE_REQUESTS, CODEX_FIXTURE_PROCESS, CODEX_FIXTURE_READY, CODEX_FIXTURE_PROCEED, CODEX_FIXTURE_DESCENDANT, CODEX_FIXTURE_SCENARIO, CODEX_FIXTURE_VERSION, CODEX_FIXTURE_RESPONSE]\n",
            1,
        )
    };
    fs::write(source.join("workflow.yaml"), workflow).unwrap();
    fs::write(source.join("system.md"), "System.\n").unwrap();
    initialize_source_repository(&source);
    let assignment = AssignmentConfig::new(&work).unwrap();
    let config = Config::new(
        "wss://gateway.example.test/v1/runner/connect",
        test_credential(),
        false,
        assignment,
        RepositoryUrlPolicy::with_file_repositories(true),
    )
    .unwrap();
    (temporary, source, config)
}

fn manager_with_fixture_source(
    config: &Config,
    source: &Path,
    work_root: Arc<WorkRootLease>,
) -> AssignmentManager {
    let dependencies = AssignmentDependencies::new(
        work_root,
        Arc::new(crate::service::TokioSleeper),
        Some(fixture_source_broker(source)),
        None,
        Arc::from(crate::telemetry::TEST_SERVICE_VERSION),
        None,
        false,
    );
    AssignmentManager::new(config, fixture_lease_clock(), dependencies)
}

fn manager_fixture_with_cleanup(
    workflow: &str,
    remover: Arc<dyn TreeRemover>,
    cleanup_sleeper: Arc<dyn CleanupSleeper>,
) -> (tempfile::TempDir, AssignmentManager) {
    let (temporary, source, config) = base_manager_config(workflow);
    let boot_id = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned();
    let work_root = WorkRootLease::acquire_with(
        config.assignment().work_root(),
        &boot_id,
        WorkspaceFilesystem::injected(remover, cleanup_sleeper, Arc::new(CleanupHook)),
    )
    .unwrap();
    let mut manager = manager_with_fixture_source(&config, &source, work_root);
    manager.retain_lease_policy(&policy()).unwrap();
    (temporary, manager)
}

fn only_harness_call(path: &Path) -> String {
    let calls = fs::read(path).unwrap();
    assert_eq!(calls.iter().filter(|byte| **byte == 0).count(), 1);
    assert_eq!(calls.last(), Some(&0));
    String::from_utf8(calls[..calls.len() - 1].to_vec()).unwrap()
}

fn codex_requests(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn select_codex_scenario(manager: &mut AssignmentManager, scenario: &str) {
    let mut environment = manager.environment.variables().clone();
    environment.insert(
        OsString::from("CODEX_FIXTURE_SCENARIO"),
        OsString::from(scenario),
    );
    manager.environment = EnvironmentSnapshot::new(environment);
}

#[expect(
    clippy::disallowed_methods,
    reason = "the explicit fixture file is the OS-boundary readiness event; the delay only spaces polls"
)]
async fn wait_for_fixture_path(path: &Path) {
    with_watchdog(async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("Codex process fixture did not publish its readiness boundary");
}

fn fixture_pid(path: &Path) -> Pid {
    fs::read_to_string(path)
        .unwrap()
        .trim()
        .parse::<i32>()
        .ok()
        .and_then(Pid::from_raw)
        .expect("Codex fixture PID must be positive")
}

fn assert_codex_fixture_quiescent(temporary: &tempfile::TempDir) {
    for name in ["codex.process", "codex.descendant"] {
        let path = temporary.path().join(name);
        if path.exists() {
            assert!(
                rustix::process::test_kill_process(fixture_pid(&path)).is_err(),
                "{name} remained live after runner terminal reporting"
            );
        }
    }
}

fn start_for(offered: &AssignmentOffer) -> AssignmentStart {
    AssignmentStart {
        effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abh".to_owned(),
        assignment_id: offered.assignment_id.clone(),
        run_id: offered.run_id.clone(),
        attempt_id: offered.attempt_id.clone(),
        execution_spec_id: offered.execution_spec.execution_spec_id.clone(),
        lease: ExecutionLeaseGrant { sequence: 1 },
    }
}

fn start_from_civil_time(offered: &AssignmentOffer, sent_at: &str) -> AssignmentStart {
    let encoded = serde_json::to_vec(&json!({
        "protocolVersion": 1,
        "direction": "cloud_to_runner",
        "messageId": "cmsg_01k0z6r1w8f4jy2m7q9v3x5abc",
        "sentAt": sent_at,
        "type": "assignment_start",
        "payloadVersion": 1,
        "payload": {
            "effectId": "eff_01k0z6r1w8f4jy2m7q9v3x5abh",
            "assignmentId": offered.assignment_id,
            "runId": offered.run_id,
            "attemptId": offered.attempt_id,
            "executionSpecId": offered.execution_spec.execution_spec_id,
            "lease": { "leaseSequence": 1 }
        }
    }))
    .unwrap();
    let CloudFrame::AssignmentStart {
        effect_id,
        assignment_id,
        run_id,
        attempt_id,
        execution_spec_id,
        lease,
        ..
    } = decode_cloud_frame(&encoded).expect("civil-time assignment start must decode")
    else {
        panic!("fixture decoded as another Cloud frame");
    };
    AssignmentStart {
        effect_id,
        assignment_id,
        run_id,
        attempt_id,
        execution_spec_id,
        lease,
    }
}

fn cancel_for(offered: &AssignmentOffer, mode: CancellationMode, suffix: &str) -> AssignmentCancel {
    AssignmentCancel {
        effect_id: format!("eff_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
        assignment_id: offered.assignment_id.clone(),
        run_id: offered.run_id.clone(),
        attempt_id: offered.attempt_id.clone(),
        request_id: format!("cmd_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
        mode,
    }
}

fn release_for(offered: &AssignmentOffer, suffix: &str, reason: &str) -> AssignmentRelease {
    AssignmentRelease {
        effect_id: format!("eff_01k0z6r1w8f4jy2m7q9v3x5a{suffix}"),
        assignment_id: offered.assignment_id.clone(),
        run_id: offered.run_id.clone(),
        attempt_id: offered.attempt_id.clone(),
        reason: reason.to_owned(),
    }
}

fn cancellation_applications(
    manager: &mut AssignmentManager,
) -> Vec<(u64, AssignmentCancellationApplication)> {
    manager
        .pending_observations(&BTreeSet::new(), 100)
        .into_iter()
        .filter_map(|pending| match pending.observation {
            AssignmentObservation::CancellationApplied(application) => {
                Some((pending.id, application))
            }
            _ => None,
        })
        .collect()
}

fn pre_execution_cancellation_application(
    manager: &mut AssignmentManager,
    request_id: &str,
) -> (u64, AssignmentCancellationApplication) {
    let pending = manager.pending_observations(&BTreeSet::new(), 100);
    let (application_id, application) = pending
        .iter()
        .find_map(|pending| match &pending.observation {
            AssignmentObservation::CancellationApplied(application) => {
                Some((pending.id, application.clone()))
            }
            _ => None,
        })
        .expect("cancellation application");
    assert_eq!(application.request_id, request_id);
    assert_eq!(
        application.disposition,
        CancellationApplicationDisposition::PreExecutionStopped
    );
    let terminal = pending
        .iter()
        .find(|pending| pending.observation.is_terminal())
        .expect("pre-execution cancellation terminal");
    assert!(application_id < terminal.id);
    assert!(matches!(
        &terminal.observation,
        AssignmentObservation::Execution {
            report: ExecutionReport::AssignmentInterrupted { reason },
            ..
        } if reason == "user_request"
    ));
    (application_id, application)
}

fn assert_no_terminal_observation(manager: &mut AssignmentManager) {
    assert!(
        !manager
            .pending_observations(&BTreeSet::new(), 100)
            .iter()
            .any(|pending| pending.observation.is_terminal())
    );
}

fn gate_assignment_root_preparation_with_outcome(
    manager: &mut AssignmentManager,
    outcome: GatedRootPreparationOutcome,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (started, root_preparation_started) = tokio::sync::mpsc::unbounded_channel();
    let (release_root_preparation, released) = std::sync::mpsc::channel();
    manager.root_preparer = Arc::new(GatedAssignmentRootPreparer {
        started,
        release: Mutex::new(released),
        outcome,
    });
    (root_preparation_started, release_root_preparation)
}

fn gate_assignment_root_preparation(
    manager: &mut AssignmentManager,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    gate_assignment_root_preparation_with_outcome(
        manager,
        GatedRootPreparationOutcome::Create(Arc::clone(&manager.work_root)),
    )
}

fn gated_root_preparation_fixture() -> (
    tempfile::TempDir,
    AssignmentManager,
    AssignmentOffer,
    tokio::sync::mpsc::UnboundedReceiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    let (root_preparation_started, release_root_preparation) =
        gate_assignment_root_preparation(&mut manager);
    (
        temporary,
        manager,
        offered,
        root_preparation_started,
        release_root_preparation,
    )
}

async fn cancel_while_root_preparation_is_blocked(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
    root_preparation_started: &mut tokio::sync::mpsc::UnboundedReceiver<()>,
) -> AssignmentCancel {
    manager.handle_offer(offered.clone()).unwrap();
    root_preparation_started
        .recv()
        .await
        .expect("assignment root preparation did not start");
    let cancel = cancel_for(offered, CancellationMode::Graceful, "bm");
    manager.handle_cancel(cancel.clone()).unwrap();
    cancel
}

fn start_authorization_for(offered: &AssignmentOffer) -> AssignmentStartAuthorization {
    AssignmentStartAuthorization {
        effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned(),
        assignment_id: offered.assignment_id.clone(),
        run_id: offered.run_id.clone(),
        attempt_id: offered.attempt_id.clone(),
    }
}

fn renewal_for(offered: &AssignmentOffer) -> AssignmentRenewal {
    AssignmentRenewal {
        effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abj".to_owned(),
        assignment_id: offered.assignment_id.clone(),
        run_id: offered.run_id.clone(),
        attempt_id: offered.attempt_id.clone(),
        lease: ExecutionLeaseGrant { sequence: 2 },
    }
}

fn enqueue_finished(manager: &AssignmentManager, identity: &AssignmentIdentity) -> u64 {
    manager
        .outbox
        .enqueue(AssignmentObservation::Execution {
            assignment_id: identity.assignment_id.clone(),
            attempt_id: identity.attempt_id.clone(),
            report: ExecutionReport::Finished {
                diagnostic: None,
                final_execution_event_sequence: 1,
                outcome: json!({ "outcome": "succeeded", "forceAbort": null }),
                artifact_delivery: json!({
                    "outcome": "prepared",
                    "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
                }),
            },
        })
        .unwrap()
}

fn enqueue_completion(manager: &AssignmentManager, final_delivery_deadline: LeaseInstant) -> u64 {
    let identity = match &manager.slot {
        Some(LocalSlot::Running(running)) => running.identity.clone(),
        _ => panic!("fixture assignment must be running"),
    };
    let final_observation_id = enqueue_finished(manager, &identity);
    manager
        .event_sender
        .send(ManagerEvent::Finished {
            assignment_id: identity.assignment_id,
            final_observation_id: Some(final_observation_id),
            final_delivery_deadline: Some(final_delivery_deadline),
            lease_clock_failed: false,
            fenced: false,
            retained_root: None,
            quiescence: ProcessQuiescence::Proven,
            quiescence_failure: None,
            workspace_disposition: WorkspaceDisposition::Remove,
        })
        .unwrap();
    final_observation_id
}

fn execution_job(manager: &mut AssignmentManager, offered: &AssignmentOffer) -> ExecutionJob {
    let job = manager
        .handle_start(start_for(offered))
        .unwrap()
        .expect("valid start dispatches execution");
    manager
        .handle_start_authorized(start_authorization_for(offered))
        .expect("valid start authorization is accepted");
    job
}

fn spawn_execution(manager: &mut AssignmentManager, offered: &AssignmentOffer) {
    execution_job(manager, offered).spawn();
}

async fn cancellable_running_fixture() -> (
    tempfile::TempDir,
    AssignmentManager,
    AssignmentOffer,
    CancellationSource,
) {
    let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let _job = execution_job(&mut manager, &offered);
    let cancellation = match &manager.slot {
        Some(LocalSlot::Running(running)) => running.cancellation.clone(),
        _ => panic!("assignment must be running"),
    };
    (temporary, manager, offered, cancellation)
}

fn request_next_renewal(manager: &mut AssignmentManager, offered: &AssignmentOffer) {
    let causal_lease = match &manager.slot {
        Some(LocalSlot::Running(running)) => running.causal_lease.clone(),
        _ => panic!("assignment must remain running"),
    };
    causal_lease
        .request_renewal(
            1,
            &offered.assignment_id,
            &offered.attempt_id,
            &manager.lease_clock,
            &manager.outbox,
        )
        .unwrap();
}

async fn controlled_execution_job(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
) -> (
    ControlledLeaseClock,
    tokio::sync::mpsc::UnboundedReceiver<(Duration, LeaseTimerRelease)>,
    ExecutionJob,
) {
    let (lease_clock, control, waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    offer_then_prepare(manager, offered).await;
    let job = execution_job(manager, offered);
    (control, waits, job)
}

async fn controlled_running_fixture() -> (
    tempfile::TempDir,
    AssignmentManager,
    tokio::sync::mpsc::UnboundedReceiver<(Duration, LeaseTimerRelease)>,
    AssignmentOffer,
    PathBuf,
) {
    let workflow = "schemaVersion: 1\nsteps:\n  wait:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"sleep 60\"]\n";
    let (temporary, mut manager) = manager_fixture(workflow);
    let (lease_clock, _control, lease_waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let workspace = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => accepted.root.workspace.path(),
        _ => panic!("fixture assignment must be accepted"),
    };
    fs::write(
        workspace.join("lease-loss-sentinel"),
        b"retained lease bytes",
    )
    .unwrap();
    spawn_execution(&mut manager, &offered);
    (temporary, manager, lease_waits, offered, workspace)
}

async fn lease_wait_request(
    requests: &mut tokio::sync::mpsc::UnboundedReceiver<(Duration, LeaseTimerRelease)>,
    expected: Duration,
) -> LeaseTimerRelease {
    loop {
        let (duration, release) = requests
            .recv()
            .await
            .expect("controlled lease clock closed before the expected timer");
        if duration == expected {
            return release;
        }
    }
}

async fn wait_for_renewal_request(manager: &mut AssignmentManager) -> PendingAssignmentObservation {
    let notification = manager.notification();
    with_watchdog(async {
        loop {
            let notified = notification.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(requested) = manager
                .pending_observations(&BTreeSet::new(), 10)
                .into_iter()
                .find(|pending| {
                    matches!(
                        pending.observation,
                        AssignmentObservation::LeaseRenewalRequested { .. }
                    )
                })
            {
                break requested;
            }
            notified.await;
        }
    })
    .await
    .expect("runner did not request renewal")
}

async fn wait_for_manager_state(
    manager: &mut AssignmentManager,
    mut reached: impl FnMut(&mut AssignmentManager) -> bool,
) {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if reached(manager) {
            return;
        }
        notified.await;
    }
}

async fn assert_workflow_environment_unsupported(manager: &mut AssignmentManager) {
    let pending = manager.pending_observations(&BTreeSet::new(), 1);
    assert!(matches!(
        &pending[0].observation,
        AssignmentObservation::Decision(AssignmentDecision::Rejected {
            decline: AssignmentDecline::Diagnosed { decline, stage: "admission", cause: "admission_invalid" },
            ..
        }) if **decline == AssignmentDecline::RunnerUnable(RunnerUnableReason::WorkflowEnvironmentUnsupported)
    ));
    settle_cleanup(manager).await;
    assert!(manager.slot.is_none());
}

async fn execute_to_terminal(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
) -> Vec<ExecutionReport> {
    spawn_execution(manager, offered);
    with_watchdog(wait_for_terminal(manager))
        .await
        .expect("workflow did not finish")
}

async fn start_then_shut_down_and_wait<Ready>(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
    ready: Ready,
) -> Vec<ExecutionReport>
where
    Ready: std::future::Future<Output = ()>,
{
    spawn_execution(manager, offered);
    ready.await;
    manager.begin_shutdown().unwrap();
    with_watchdog(wait_for_terminal(manager))
        .await
        .expect("runner shutdown did not quiesce execution")
}

fn fail_pending_artifact_registrations(
    manager: &mut AssignmentManager,
    pending: &[PendingAssignmentObservation],
) -> bool {
    let registrations = pending
        .iter()
        .filter_map(|entry| match &entry.observation {
            AssignmentObservation::Artifact {
                delivery_id,
                request: ArtifactRequest::RegisterCarrier { .. },
            } => Some((entry.id, *delivery_id, false)),
            AssignmentObservation::Artifact {
                delivery_id,
                request: ArtifactRequest::RegisterResult { .. },
            } => Some((entry.id, *delivery_id, true)),
            _ => None,
        })
        .collect::<Vec<_>>();
    for (observation_id, delivery_id, is_result) in &registrations {
        let response = if *is_result {
            ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                outcome: ArtifactResultRegistrationOutcome::Failed {
                    code: "storage_quota_exceeded".to_owned(),
                },
            })
        } else {
            ArtifactCloudResponse::CarrierRegistration(ArtifactRegistrationResponse {
                request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                outcome: ArtifactRegistrationOutcome::Failed {
                    code: "storage_quota_exceeded".to_owned(),
                },
            })
        };
        manager
            .handle_artifact_response(*observation_id, *delivery_id, response)
            .expect("test Cloud must close artifact registration");
    }
    !registrations.is_empty()
}

async fn wait_for_carrier_registration(
    manager: &mut AssignmentManager,
) -> Vec<PendingAssignmentObservation> {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let pending = manager.pending_observations(&BTreeSet::new(), 100);
        if pending.iter().any(|entry| {
            matches!(
                entry.observation,
                AssignmentObservation::Artifact {
                    request: ArtifactRequest::RegisterCarrier { .. },
                    ..
                }
            )
        }) {
            return pending;
        }
        notified.await;
    }
}

async fn wait_for_terminal(manager: &mut AssignmentManager) -> Vec<ExecutionReport> {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        // The outbox uses notify_waiters, so register before checking its state.
        notified.as_mut().enable();
        let pending = manager.pending_observations(&BTreeSet::new(), 100);
        if fail_pending_artifact_registrations(manager, &pending) {
            continue;
        }
        if pending
            .iter()
            .any(|pending| pending.observation.is_terminal())
        {
            return pending
                .into_iter()
                .filter_map(|pending| match pending.observation {
                    AssignmentObservation::Execution { report, .. } => Some(report),
                    AssignmentObservation::Preparing { .. }
                    | AssignmentObservation::PreparationProgress { .. }
                    | AssignmentObservation::ContinuationReady { .. }
                    | AssignmentObservation::Decision(_)
                    | AssignmentObservation::CancellationApplied(_)
                    | AssignmentObservation::LeaseRenewalRequested { .. }
                    | AssignmentObservation::WorkspaceRetention { .. }
                    | AssignmentObservation::Artifact { .. } => None,
                })
                .collect();
        }
        notified.await;
    }
}

fn assert_only_workspace_retention(manager: &mut AssignmentManager) {
    let pending = manager.pending_observations(&BTreeSet::new(), 10);
    assert_eq!(pending.len(), 1);
    assert!(matches!(
        pending[0].observation,
        AssignmentObservation::WorkspaceRetention { .. }
    ));
}

fn assert_acknowledged_run(
    manager: &mut AssignmentManager,
    capture: &crate::telemetry::TestCapture,
    result: &str,
) {
    assert!(
        capture
            .events()
            .iter()
            .all(|event| event["event.name"] != "runner.run")
    );
    let final_id = manager
        .pending_observations(&BTreeSet::new(), 100)
        .into_iter()
        .find(|pending| pending.observation.is_terminal())
        .unwrap()
        .id;
    manager.acknowledge_observation(final_id);
    let runs: Vec<_> = capture
        .events()
        .into_iter()
        .filter(|event| event["event.name"] == "runner.run")
        .collect();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["um.run.result"], result);
}

fn command_fixture_arguments() -> Vec<String> {
    command_fixture_arguments_for(COMMAND_FIXTURE_TEST_NAME)
}

fn failing_command_fixture_arguments() -> Vec<String> {
    command_fixture_arguments_for(FAILING_COMMAND_FIXTURE_TEST_NAME)
}

fn command_fixture_arguments_for(test_name: &str) -> Vec<String> {
    std::iter::once(
        std::env::current_exe()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned(),
    )
    .chain(
        ["--ignored", "--exact", test_name, "--nocapture"]
            .into_iter()
            .map(str::to_owned),
    )
    .collect()
}

fn install_command_fixture_environment(manager: &mut AssignmentManager, address: String) {
    let mut variables = vec![(
        OsString::from(COMMAND_FIXTURE_SOCKET),
        OsString::from(address),
    )];
    if let Some(path) = manager.environment.variable(OsStr::new("PATH")) {
        variables.push((OsString::from("PATH"), path.to_os_string()));
    }
    manager.environment = EnvironmentSnapshot::new(variables);
}

async fn release_command_fixtures(listener: &TcpListener, count: usize) {
    for _ in 0..count {
        let (mut control, _) = listener.accept().await.unwrap();
        let mut ready = [0_u8; 1];
        control.read_exact(&mut ready).await.unwrap();
        assert_eq!(ready, [1]);
        control.write_all(&[1]).await.unwrap();
    }
}

async fn execute_fixture_to_terminal(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
    listener: &TcpListener,
    command_count: usize,
) -> Vec<ExecutionReport> {
    spawn_execution(manager, offered);
    match with_watchdog(async {
        tokio::join!(
            wait_for_terminal(manager),
            release_command_fixtures(listener, command_count),
        )
    })
    .await
    {
        Ok((reports, ())) => reports,
        Err(_) => panic!(
            "assignment command fixture did not finish; pending: {:#?}",
            manager.pending_observations(&BTreeSet::new(), 100)
        ),
    }
}

async fn execute_fixture_workflow(
    workflow: &str,
    pi_source: Option<&str>,
    command_count: usize,
) -> Vec<ExecutionReport> {
    let (_temporary, mut manager) = manager_fixture_with_pi(workflow, pi_source);
    offer_and_execute_with_command_fixtures(&mut manager, command_count).await
}

fn assert_succeeded(reports: &[ExecutionReport]) {
    assert!(
        matches!(
            reports.last(),
            Some(ExecutionReport::Finished { outcome, .. })
                if outcome == &json!({ "outcome": "succeeded", "forceAbort": null })
        ),
        "unexpected reports: {reports:#?}"
    );
}

async fn offer_and_execute(manager: &mut AssignmentManager) -> Vec<ExecutionReport> {
    let offered = offer("bg");
    offer_then_prepare(manager, &offered).await;
    execute_to_terminal(manager, &offered).await
}

async fn offer_and_execute_with_command_fixtures(
    manager: &mut AssignmentManager,
    command_count: usize,
) -> Vec<ExecutionReport> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    install_command_fixture_environment(manager, listener.local_addr().unwrap().to_string());
    let offered = offer("bg");
    offer_then_prepare(manager, &offered).await;
    execute_fixture_to_terminal(manager, &offered, &listener, command_count).await
}

fn runner_shutdown_outcome(reports: &[ExecutionReport]) -> &Value {
    let Some(ExecutionReport::Interrupted {
        reason,
        terminal_outcome,
        ..
    }) = reports.last()
    else {
        panic!("runner shutdown did not report an interrupted execution");
    };
    assert_eq!(reason, "graceful_shutdown");
    assert_eq!(terminal_outcome["reason"], "runner_shutdown");
    terminal_outcome
}

fn decoded_source_display_offer() -> AssignmentOffer {
    let CloudFrame::AssignmentOffer {
        effect_id,
        assignment_id,
        run_id,
        project_id,
        attempt_id,
        attempt_number,
        execution_spec,
        ..
    } = decode_cloud_frame(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-source-display.json"
    )))
    .unwrap()
    else {
        panic!("expected an assignment offer");
    };
    AssignmentOffer {
        effect_id,
        assignment_id,
        run_id,
        project_id,
        attempt_id,
        attempt_number,
        execution_spec: *execution_spec,
        continuation: None,
    }
}

#[test]
fn decoded_execution_spec_versions_retain_semantic_admission_guards() {
    for mut spec in [
        offer("bg").execution_spec,
        decoded_source_display_offer().execution_spec,
    ] {
        assert_eq!(validate_execution_spec(&spec), Ok(()));
        spec.execution_limits.maximum_parallel_steps = 0;
        assert_eq!(
            validate_execution_spec(&spec),
            Err(invalid_execution_limits())
        );
        spec.execution_limits.maximum_parallel_steps = 1;
        spec.primary_workspace_source.commit_oid = "0".repeat(40);
        assert_eq!(
            validate_execution_spec(&spec),
            Err(AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::InvalidSourceProjection,
            ))
        );
    }
    for version in [0, 3, u64::MAX] {
        let mut spec = decoded_source_display_offer().execution_spec;
        spec.schema_version = version;
        assert_eq!(
            validate_execution_spec(&spec),
            Err(AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::UnsupportedSchemaVersion,
            ))
        );
    }
}

#[tokio::test]
async fn malformed_run_input_projection_has_the_closed_immutable_decline() {
    let mut execution_spec = offer("bg").execution_spec;
    execution_spec.run_inputs = Some(um_runner_protocol::RunInputProjectionV1 {
        input_set_id: "ris_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        manifest_digest: um_runner_protocol::WorkflowSourceClosureDigestV1RunnerProjection {
            algorithm: "sha256".to_owned(),
            value: "A".repeat(64),
        },
    });
    assert_eq!(
        validate_execution_spec(&execution_spec),
        Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InvalidInputProjection,
        ))
    );
}

#[tokio::test]
async fn run_input_failures_keep_transient_local_and_immutable_families_distinct() {
    let cases = [
        (
            RunInputFailure::ServiceUnavailable,
            AssignmentDecline::RunnerUnable(RunnerUnableReason::InputServiceUnavailable),
        ),
        (
            RunInputFailure::EnvironmentUnavailable,
            AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable),
        ),
        (
            RunInputFailure::AssignmentFenced,
            AssignmentDecline::RunnerUnable(RunnerUnableReason::InputServiceUnavailable),
        ),
        (
            RunInputFailure::InvalidProjection,
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::InvalidInputProjection,
            ),
        ),
        (
            RunInputFailure::ManifestMismatch,
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::InputManifestMismatch,
            ),
        ),
        (
            RunInputFailure::ContentUnavailable,
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::InputContentUnavailable,
            ),
        ),
        (
            RunInputFailure::ContentMismatch,
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::InputContentMismatch,
            ),
        ),
        (
            RunInputFailure::TextInvalid,
            AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::InputTextInvalid),
        ),
        (
            RunInputFailure::JsonInvalid,
            AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::InputJsonInvalid),
        ),
    ];
    for (failure, expected) in cases {
        let cause = match failure {
            RunInputFailure::ServiceUnavailable => "input_service_unavailable",
            RunInputFailure::AssignmentFenced => "assignment_fenced",
            RunInputFailure::EnvironmentUnavailable => "execution_root_unavailable",
            _ => "admission_invalid",
        };
        assert_eq!(
            run_input_decline(failure),
            expected.diagnosed("input_materialization", cause)
        );
    }
}

#[test]
fn presentation_capacity_offer_is_bound_to_resolved_source() {
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      text: {kind: text, from: path, path: note.txt}\nexports:\n  note:\n    ref: outputs.work.text\n    presentation:\n      title: {ref: outputs.work.text}\n").unwrap();
    let workflow = resolve(source.path(), Path::new("workflow.yaml")).unwrap();
    let mut spec = offer("bg").execution_spec;
    align_fixture_capacity(&mut spec, &workflow);
    assert!(spec.capacity.presentation_result_bytes > 0);
    assert_eq!(validate_carried_capacity(&spec, &workflow), Ok(()));
    // Both changed fields still satisfy the closed arithmetic contract;
    // only recomputation against the resolved source detects this offer.
    spec.capacity.presentation_result_bytes += 1;
    spec.capacity.portable_result_bytes += 1;
    assert_eq!(
        validate_carried_capacity(&spec, &workflow),
        Err(capacity_binding_invalid())
    );
}

#[test]
fn replacement_capacity_is_checked_and_reserved_from_the_effective_definition() {
    let source = tempfile::tempdir().unwrap();
    fs::write(
        source.path().join("workflow.yaml"),
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
    )
    .unwrap();
    let initial = resolve(source.path(), Path::new("workflow.yaml")).unwrap();
    let mut offered = offer("bg");
    align_fixture_capacity(&mut offered.execution_spec, &initial);
    let initial_bytes = offered.execution_spec.capacity.encoded_outbox_bytes;
    fs::write(source.path().join("workflow.yaml"),
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n  replacement:\n    kind: cmd\n    command: {argv: [\"true\"]}\n").unwrap();
    let replacement = resolve(source.path(), Path::new("workflow.yaml")).unwrap();
    assert_eq!(
        validate_carried_capacity(&offered.execution_spec, &replacement),
        Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch
        ))
    );
    align_fixture_capacity(&mut offered.execution_spec, &replacement);
    assert!(offered.execution_spec.capacity.encoded_outbox_bytes > initial_bytes);
    assert_eq!(
        validate_carried_capacity(&offered.execution_spec, &replacement),
        Ok(())
    );
    let outbox = ObservationOutbox::new();
    let capacity = &offered.execution_spec.capacity;
    let transitions = usize::try_from(capacity.selected_maximum_transitions).unwrap();
    assert_eq!(
        outbox.reserve(transitions, capacity.encoded_outbox_bytes),
        Ok(transitions)
    );
    offered.execution_spec.capacity.encoded_outbox_bytes -= 1;
    assert_eq!(
        validate_carried_capacity(&offered.execution_spec, &replacement),
        Err(capacity_binding_invalid())
    );
}

#[test]
fn runner_admission_limits_match_shared_contract() {
    let contract: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../execution/tests/fixtures/workflow/v1/capacity-contract.json"
    )))
    .unwrap();
    let number = |path: &[&str]| -> u64 {
        path.iter()
            .fold(&contract, |value, key| &value[*key])
            .as_u64()
            .unwrap()
    };
    assert_eq!(
        MAXIMUM_CANCELLATION_GRACE.as_secs(),
        number(&["cancellationGraceSeconds", "maximum"])
    );
    assert_eq!(
        MAXIMUM_GENERAL_TRANSITIONS,
        number(&["transitionBounds", "general", "maximumWithFinalizers"])
    );
    assert_eq!(
        MAXIMUM_SELECTED_TRANSITIONS,
        number(&["transitionBounds", "selected", "maximumWithFinalizers"])
    );
    assert_eq!(
        MAXIMUM_INVOCATIONS,
        number(&["commonBounds", "maximumInvocations", "maximum"])
    );
    assert_eq!(
        MAXIMUM_RETAINED_BYTES_PER_INVOCATION,
        number(&[
            "commonBounds",
            "maximumRetainedBytesPerInvocation",
            "maximum"
        ])
    );
    assert_eq!(
        MAXIMUM_DIAGNOSTIC_RETENTION_BYTES,
        number(&["commonBounds", "diagnosticRetentionBytes", "maximum"])
    );
    assert_eq!(
        MAXIMUM_NATIVE_SESSION_RETENTION_BYTES,
        number(&["commonBounds", "nativeSessionRetentionBytes", "maximum"])
    );
    assert_eq!(
        MAXIMUM_AGGREGATE_RETENTION_BYTES,
        number(&["commonBounds", "aggregateRetentionBytes", "maximum"])
    );
}

#[tokio::test]
async fn execution_spec_accepts_maximum_cancellation_grace_and_rejects_above_it() {
    let mut execution_spec = offer("bg").execution_spec;
    execution_spec.execution_limits.cancellation_grace_seconds =
        MAXIMUM_CANCELLATION_GRACE.as_secs();
    assert_eq!(validate_execution_spec(&execution_spec), Ok(()));

    execution_spec.execution_limits.cancellation_grace_seconds += 1;
    assert_eq!(
        validate_execution_spec(&execution_spec),
        Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InvalidExecutionLimits
        ))
    );
}

#[tokio::test]
async fn preparation_authority_expires_on_the_injected_clock() {
    let (sleeper, mut requests) = controlled_sleeper();
    let expiration = (sleeper.utc_now() + time::Duration::hours(1))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    let deadline =
        PreparationDeadline::from_wire(&expiration, sleeper.utc_now(), sleeper.now()).unwrap();
    let advance = tokio::spawn({
        let sleeper = Arc::clone(&sleeper);
        async move { sleeper.sleep(Duration::from_secs(3_600)).await }
    });
    let (_, release) = requests.recv().await.unwrap();
    release.release();
    advance.await.unwrap();
    assert!(deadline.remaining_at(sleeper.now()).is_none());

    let cancellation = CaptureCancellation::default();
    assert_eq!(
        PreparationAuthority {
            deadline,
            cancellation: &cancellation,
            monotonic_now: sleeper.now(),
        }
        .ensure_current(),
        Err(AssignmentDecline::RunnerUnable(
            RunnerUnableReason::InputServiceUnavailable,
        ))
    );
}

#[tokio::test]
async fn prepared_event_after_absolute_deadline_cannot_retain_acceptance() {
    let workflow_source =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow_source);
    let mut offered = offer("bg");
    align_offer_with_source_fixture(&manager, &mut offered);
    manager.handle_offer(offered.clone()).unwrap();
    prepare_current(&mut manager, &offered).await;
    let acceptance = manager.pending_observations(&BTreeSet::new(), 1)[0].id;
    manager.acknowledge_observation(acceptance);
    let accepted = match manager.slot.take() {
        Some(LocalSlot::Accepted(accepted)) => *accepted,
        _ => panic!("fixture assignment must be accepted"),
    };
    manager.slot = Some(LocalSlot::Preparing(Box::new(PreparingAssignment {
        offer: offered.clone(),
        cancellation: CaptureCancellation::default(),
        root_preparation: None,
        root: None,
        prepare_effect_id: Some("eff_01k0z6r1w8f4jy2m7q9v3x5acz".to_owned()),
        preparation_event: None,
    })));
    manager
        .event_sender
        .send(ManagerEvent::Prepared {
            offer: Box::new(offered),
            prepare_effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5acz".to_owned(),
            deadline: PreparationDeadline::elapsed_for_test(),
            admission: Box::new(Ok(accepted)),
        })
        .unwrap();

    let pending = manager.pending_observations(&BTreeSet::new(), 100);
    assert!(!pending.iter().any(|entry| matches!(
        entry.observation,
        AssignmentObservation::Decision(AssignmentDecision::Accepted { .. })
    )));
    assert!(matches!(manager.slot, Some(LocalSlot::Releasing(_))));
    settle_cleanup(&mut manager).await;
    assert!(manager.slot.is_none());
}

#[tokio::test]
async fn retry_result_upload_preserves_the_cloud_attempt_number() {
    use base64::Engine as _;
    use um_runner_protocol::{
        ArtifactResultConfirmationOutcome, ArtifactResultConfirmationResponse,
        ArtifactUploadCapability,
    };

    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    manager.artifact_delivery = ArtifactDeliveryBroker::new(
        manager.outbox.clone(),
        Arc::clone(&manager.sleeper),
        true,
        None,
    );
    let mut offered = offer("bg");
    offered.attempt_number = 2;
    offer_then_prepare(&mut manager, &offered).await;
    let mut conflicting = offered.clone();
    conflicting.attempt_number = 1;
    assert_eq!(
        manager.handle_offer(conflicting),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );
    spawn_execution(&mut manager, &offered);
    let mut registration = None;
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        registration = manager
            .pending_observations(&BTreeSet::new(), 100)
            .into_iter()
            .find_map(|entry| match entry.observation {
                AssignmentObservation::Artifact {
                    delivery_id,
                    request:
                        ArtifactRequest::RegisterResult {
                            size_bytes, sha256, ..
                        },
                } => Some((delivery_id, size_bytes, sha256)),
                _ => None,
            });
        registration.is_some()
    }))
    .await
    .unwrap();
    let (delivery_id, size, sha256) = registration.unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let upload = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let body_start = loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&buffer[..read]);
            if let Some(index) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break index + 4;
            }
        };
        while request.len() - body_start < usize::try_from(size).unwrap() {
            let read = stream.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0);
            request.extend_from_slice(&buffer[..read]);
        }
        let result: Value = serde_json::from_slice(&request[body_start..]).unwrap();
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        result
    });
    let checksum = (0..sha256.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&sha256[index..index + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let artifact_set_id = "ats_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned();
    manager
        .artifact_delivery
        .handle_response(
            delivery_id,
            ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                outcome: ArtifactResultRegistrationOutcome::Succeeded {
                    artifact_set_id: artifact_set_id.clone(),
                    finalization_deadline: "2099-01-01T00:00:00Z".to_owned(),
                    upload_capability: ArtifactUploadCapability {
                        url: format!("http://{address}/result"),
                        content_length: size.to_string(),
                        content_type: "application/json".to_owned(),
                        if_none_match: "*".to_owned(),
                        checksum_sha256: base64::engine::general_purpose::STANDARD.encode(checksum),
                        expires_at: "2099-01-01T00:00:00Z".to_owned(),
                    },
                },
            }),
        )
        .unwrap();
    let result = with_watchdog(upload).await.unwrap().unwrap();
    assert_eq!(result["attemptNumber"], 2);
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager
            .pending_observations(&BTreeSet::new(), 100)
            .iter()
            .any(|entry| {
                matches!(
                    &entry.observation,
                    AssignmentObservation::Artifact {
                        request: ArtifactRequest::ConfirmResult { .. },
                        ..
                    }
                )
            })
    }))
    .await
    .unwrap();
    manager
        .artifact_delivery
        .handle_response(
            delivery_id,
            ArtifactCloudResponse::ResultConfirmation(ArtifactResultConfirmationResponse {
                request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
                outcome: ArtifactResultConfirmationOutcome::Confirmed { artifact_set_id },
            }),
        )
        .unwrap();
    let reports = with_watchdog(wait_for_terminal(&mut manager))
        .await
        .unwrap();
    assert_succeeded(&reports);
    acknowledge_terminal_and_settle(&mut manager).await;
}

#[tokio::test]
async fn broker_materialization_transitions_preparing_to_prepared_for_file_exports() {
    let workflow = "schemaVersion: 1\nsteps:\n  write:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      value:\n        kind: file\n        from: path\n        path: value.txt\n        mediaType: text/plain\nexports:\n  result:\n    ref: outputs.write.value\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let pending = manager.pending_observations(&BTreeSet::new(), 1);
    assert!(matches!(
        pending[0].observation,
        AssignmentObservation::Decision(AssignmentDecision::Accepted { .. })
    ));
}

#[tokio::test]
async fn command_failure_retains_git_dirty_untracked_ignored_and_build_bytes() {
    let workflow = "schemaVersion: 1\nsteps:\n  write:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"printf committed > tracked-output.txt; git add tracked-output.txt; git -c commit.gpgsign=false -c user.name=Fixture -c user.email=fixture@example.test commit --quiet -m 'retained fixture'; printf dirty-tracked > tracked-output.txt; printf '*.cache\\\\n' > .gitignore; printf untracked > untracked.txt; printf ignored > ignored.cache; mkdir build; printf build-output > build/result; exit 23\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let (workspace, private) = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => (
            accepted.root.workspace.path(),
            accepted.root.private.path().to_owned(),
        ),
        _ => panic!("fixture assignment must be accepted"),
    };

    let reports = execute_to_terminal(&mut manager, &offered).await;

    assert!(matches!(
        reports.last(),
        Some(ExecutionReport::Finished { outcome, .. })
            if outcome["outcome"] == "failed"
    ));
    assert!(workspace.join(".git/HEAD").exists());
    assert_eq!(
        run_fixture_git(&workspace, &["log", "-1", "--format=%s"]),
        "retained fixture"
    );
    assert_eq!(
        fs::read(workspace.join("tracked-output.txt")).unwrap(),
        b"dirty-tracked"
    );
    assert_eq!(
        fs::read(workspace.join("untracked.txt")).unwrap(),
        b"untracked"
    );
    assert_eq!(
        fs::read(workspace.join("ignored.cache")).unwrap(),
        b"ignored"
    );
    assert_eq!(
        run_fixture_git(&workspace, &["check-ignore", "ignored.cache"]),
        "ignored.cache"
    );
    assert_eq!(
        fs::read(workspace.join("build/result")).unwrap(),
        b"build-output"
    );
    assert!(private.read_dir().unwrap().next().is_some());
    acknowledge_terminal_and_settle(&mut manager).await;
    assert!(workspace.exists());
}

#[tokio::test]
async fn staged_carrier_delivery_reads_while_the_workspace_remains_available() {
    let workflow = "schemaVersion: 1\nsteps:\n  write:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"printf staged > value.txt\"]\n    outputs:\n      value:\n        kind: file\n        from: path\n        path: value.txt\n        mediaType: text/plain\nexports:\n  result:\n    ref: outputs.write.value\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let (workspace, private, helper) = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => {
            let private = accepted.root.private.path().to_owned();
            (
                accepted.root.workspace.path(),
                private.clone(),
                private.join("workflow-git-credential"),
            )
        }
        _ => panic!("fixture assignment must be accepted"),
    };
    spawn_execution(&mut manager, &offered);

    let pending = with_watchdog(wait_for_carrier_registration(&mut manager))
        .await
        .expect("carrier registration was not reached");
    assert_eq!(fs::read(workspace.join("value.txt")).unwrap(), b"staged");
    assert!(private.exists());
    assert!(helper.exists());
    assert!(!pending.iter().any(|entry| entry.observation.is_terminal()));
    assert!(fail_pending_artifact_registrations(&mut manager, &pending));

    let reports = with_watchdog(wait_for_terminal(&mut manager))
        .await
        .expect("terminal report was not selected");
    assert!(matches!(
        reports.last(),
        Some(ExecutionReport::Finished { .. })
    ));
    wait_for_execution_finalization(&mut manager).await;
    assert_eq!(fs::read(workspace.join("value.txt")).unwrap(), b"staged");
    assert!(private.exists());
    assert!(!helper.exists());
}

#[tokio::test]
async fn credential_teardown_failure_after_prepared_delivery_retains_and_admits_a_successor() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let workspace = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => accepted.root.workspace.path(),
        _ => panic!("fixture assignment must be accepted"),
    };
    let job = execution_job(&mut manager, &offered);
    drop(job);
    fs::write(workspace.join(".git/config.lock"), b"synthetic lock").unwrap();
    let deadline = manager
        .lease_clock
        .now()
        .unwrap()
        .checked_add(Duration::from_secs(5))
        .unwrap();
    let final_observation_id = enqueue_completion(&manager, deadline);

    wait_for_execution_finalization(&mut manager).await;
    let pending = manager.pending_observations(&BTreeSet::new(), 100);
    assert!(pending.iter().any(|entry| {
        entry.id == final_observation_id
            && matches!(
                &entry.observation,
                AssignmentObservation::Execution {
                    report: ExecutionReport::Finished {
                        outcome,
                        artifact_delivery,
                        ..
                    },
                    ..
                } if outcome["outcome"] == "succeeded"
                    && artifact_delivery["outcome"] == "prepared"
            )
    }));
    manager.acknowledge_observation(final_observation_id);
    settle_cleanup(&mut manager).await;
    assert!(workspace.join(".git/config.lock").exists());
    assert!(workspace.exists());

    let successor = offer("bh");
    manager.handle_offer(successor.clone()).unwrap();
    wait_for_offer_preparation(&mut manager).await;
    let successor_workspace = match &manager.slot {
        Some(LocalSlot::Preparing(preparing)) => preparing
            .root
            .as_ref()
            .expect("successor root")
            .workspace
            .path(),
        _ => panic!("successor must receive a fresh preparing slot"),
    };
    assert_ne!(successor_workspace, workspace);
    assert!(workspace.exists());
}

#[tokio::test]
async fn malformed_and_unsupported_source_projections_have_closed_declines() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let mut unsupported = offer("bg");
    unsupported
        .execution_spec
        .workflow_definition_source
        .object_format = "sha256".to_owned();
    assert_offer_declined(
        &mut manager,
        unsupported,
        AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::UnsupportedSourceObjectFormat,
        ),
    )
    .await;

    let (_temporary, mut manager) = manager_fixture(workflow);
    let mut malformed = offer("bh");
    malformed
        .execution_spec
        .workflow_definition_source
        .commit_oid = "not-an-oid".to_owned();
    assert_offer_declined(
        &mut manager,
        malformed,
        AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InvalidSourceProjection,
        ),
    )
    .await;

    let (_temporary, mut manager) = manager_fixture(workflow);
    let mut mismatched = offer("bj");
    mismatched
        .execution_spec
        .primary_workspace_source
        .commit_oid = "1123456789abcdef0123456789abcdef01234567".to_owned();
    assert_offer_declined(
        &mut manager,
        mismatched,
        AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InvalidSourceProjection,
        ),
    )
    .await;
}

#[tokio::test]
async fn release_during_root_preparation_retains_the_late_root() {
    let (_temporary, mut manager, offered, mut root_preparation_started, release_root_preparation) =
        gated_root_preparation_fixture();
    let root_path = manager.work_root.boot_path().join(&offered.assignment_id);

    manager.handle_offer(offered.clone()).unwrap();
    root_preparation_started
        .recv()
        .await
        .expect("assignment root preparation did not start");
    release_current(&mut manager, &offered, "stale_or_invalid_acceptance");

    assert!(matches!(manager.slot, Some(LocalSlot::Preparing(_))));
    assert!(!has_offer_preparation(&mut manager));

    release_root_preparation
        .send(())
        .expect("release assignment root preparation");
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        matches!(manager.slot, Some(LocalSlot::Releasing(_)))
    }))
    .await
    .expect("late assignment root did not reach cleanup");
    settle_cleanup(&mut manager).await;

    assert!(manager.slot.is_none());
    assert!(root_path.join("workspace").exists());
    assert!(!has_offer_preparation(&mut manager));
}

#[tokio::test]
async fn release_fences_source_preparation_and_exact_replay_cannot_reissue() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    let source = BlockingSourceFixture::new();
    manager.source_broker = Some(source.broker());
    let mut offered = offered;
    align_offer_with_source_fixture(&manager, &mut offered);

    manager.handle_offer(offered.clone()).unwrap();
    assert_eq!(source.calls(), 0);
    source.assert_not_started();
    begin_preparation(&mut manager, &offered).await;
    source.wait_until_started();
    assert!(matches!(manager.slot, Some(LocalSlot::Preparing(_))));
    release_current(&mut manager, &offered, "offer_expired");
    assert!(matches!(manager.slot, Some(LocalSlot::Preparing(_))));
    source.wait_until_stopped();
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Preparing(_)))
    }))
    .await
    .expect("cancelled source preparation did not reach cleanup");
    assert!(matches!(manager.slot, Some(LocalSlot::Releasing(_))));
    settle_cleanup(&mut manager).await;
    assert!(manager.slot.is_none());

    manager.handle_offer(offered).unwrap();
    assert_eq!(source.calls(), 1);
    assert_only_workspace_retention(&mut manager);
}

#[tokio::test]
async fn preparation_deadline_fences_blocked_source_materialization() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (sleeper, mut sleep_requests) = controlled_sleeper();
    manager.sleeper = sleeper;
    let source = BlockingSourceFixture::new();
    manager.source_broker = Some(source.broker());
    let mut offered = offer("bg");
    align_offer_with_source_fixture(&manager, &mut offered);

    manager.handle_offer(offered.clone()).unwrap();
    begin_preparation(&mut manager, &offered).await;
    source.wait_until_started();
    let (duration, deadline_release) = sleep_requests
        .recv()
        .await
        .expect("preparation fence should register its deadline sleep");
    assert_eq!(duration, Duration::from_secs(15 * 60));

    deadline_release.release();
    source.wait_until_stopped();
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Preparing(_)))
    }))
    .await
    .expect("expired source preparation did not reach cleanup");
    assert!(
        !manager
            .pending_observations(&BTreeSet::new(), 10)
            .iter()
            .any(|pending| matches!(
                pending.observation,
                AssignmentObservation::Decision(AssignmentDecision::Accepted { .. })
            ))
    );
    settle_cleanup(&mut manager).await;
    assert!(manager.slot.is_none());
}

#[tokio::test]
async fn source_materialization_declines_preserve_failure_provenance() {
    assert_eq!(
        materialization_decline(MaterializationFailure::CommitUnavailable),
        AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::SourceCommitUnavailable,
        )
        .diagnosed("source_materialization", "admission_invalid")
    );
    assert_eq!(
        materialization_decline(MaterializationFailure::RepositoryUnavailable),
        AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable)
            .diagnosed("source_materialization", "source_repository_unavailable")
    );
    assert_eq!(
        materialization_decline(MaterializationFailure::EnvironmentUnavailable),
        AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
            .diagnosed("source_materialization", "execution_root_unavailable")
    );
    for (failure, cause) in [
        (
            MaterializationFailure::ProviderUnavailable,
            "source_provider_unavailable",
        ),
        (
            MaterializationFailure::RepositoryUnavailable,
            "source_repository_unavailable",
        ),
        (
            MaterializationFailure::AssignmentFenced,
            "assignment_fenced",
        ),
    ] {
        assert_eq!(
            materialization_decline(failure),
            AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable)
                .diagnosed("source_materialization", cause)
        );
    }
}

#[test]
fn admission_git_context_causes_keep_distinct_safe_facts() {
    assert_eq!(
        admission_diagnostic(AdmissionFailureKind::GitContextUnavailable),
        ("admission", "git_context_unavailable")
    );
    assert_eq!(
        admission_diagnostic(AdmissionFailureKind::GitContextNotRepository),
        ("admission", "git_context_invalid")
    );
    assert_eq!(
        admission_diagnostic(AdmissionFailureKind::GitContextExecutionRootMismatch),
        ("execution_root", "execution_root_unavailable")
    );
}

#[tokio::test]
async fn source_credential_failure_declines_the_assignment() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    manager.source_broker = Some(unavailable_source_broker());

    assert_preparation_declined(
        &mut manager,
        offered,
        AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable)
            .diagnosed("source_materialization", "source_provider_unavailable"),
    )
    .await;
}

fn gated_cleanup_manager(
    outcomes: impl IntoIterator<Item = bool>,
) -> (
    tempfile::TempDir,
    AssignmentManager,
    Arc<CleanupRemover>,
    tokio::sync::mpsc::UnboundedReceiver<CleanupSleepRequest>,
) {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let remover = CleanupRemover::new(outcomes);
    let (requests, cleanup_requests) = tokio::sync::mpsc::unbounded_channel();
    let sleeper = Arc::new(GatedCleanupSleeper { requests });
    let (temporary, manager) = manager_fixture_with_cleanup(workflow, remover.clone(), sleeper);
    (temporary, manager, remover, cleanup_requests)
}

// Progress is gated by messages, not worker scheduling within a wall-clock budget.
// Awaiting also leaves the single-threaded test runtime free to drive manager tasks.
// Nextest's test-wide watchdog bounds a worker that never makes progress.
async fn release_all_cleanup_retries(
    manager: &mut AssignmentManager,
    requests: &mut tokio::sync::mpsc::UnboundedReceiver<CleanupSleepRequest>,
) {
    for expected in [100, 250, 500, 1_000, 2_000] {
        let request = tokio::select! {
            request = requests.recv() => request
                .expect("cleanup retry channel closed before exhaustion"),
            event = manager.events.recv() => match event {
                Some(ManagerEvent::CleanupFinished { result, .. }) => {
                    panic!("cleanup completed before exhaustion: {result:?}")
                }
                Some(_) => panic!("unexpected manager event before cleanup exhaustion"),
                None => panic!("manager event channel closed before cleanup exhaustion"),
            },
        };
        assert_eq!(request.duration, Duration::from_millis(expected));
        request.release.send(()).unwrap();
    }
}

#[test]
fn cancellation_effect_identity_fences_every_later_effect_kind() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    let cancel = cancel_for(&offered, CancellationMode::Graceful, "bm");
    manager.handle_cancel(cancel.clone()).unwrap();

    let mut conflicting_offer = offered.clone();
    conflicting_offer.effect_id = cancel.effect_id.clone();
    assert_eq!(
        manager.handle_offer(conflicting_offer),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );

    let mut conflicting_prepare = prepare_for(&manager, &offered);
    conflicting_prepare.effect_id = cancel.effect_id.clone();
    assert_eq!(
        manager.handle_prepare(conflicting_prepare),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );

    let mut conflicting_start = start_for(&offered);
    conflicting_start.effect_id = cancel.effect_id.clone();
    assert!(matches!(
        manager.handle_start(conflicting_start),
        Err(AssignmentManagerFailure::ConflictingOffer)
    ));

    let mut conflicting_authorization = start_authorization_for(&offered);
    conflicting_authorization.effect_id = cancel.effect_id.clone();
    assert_eq!(
        manager.handle_start_authorized(conflicting_authorization),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );

    let mut conflicting_renewal = renewal_for(&offered);
    conflicting_renewal.effect_id = cancel.effect_id.clone();
    assert!(matches!(
        manager.handle_renewal(conflicting_renewal),
        Err(AssignmentManagerFailure::ConflictingOffer)
    ));

    assert_eq!(
        manager.handle_release(AssignmentRelease {
            effect_id: cancel.effect_id.clone(),
            assignment_id: offered.assignment_id.clone(),
            run_id: offered.run_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            reason: "execution_lease_expired".to_owned(),
        }),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );
}

#[test]
fn retained_release_fences_replay_payload_and_later_cancellation() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    let release = release_for(&offered, "br", "execution_lease_expired");
    manager.handle_release(release.clone()).unwrap();
    manager.handle_release(release.clone()).unwrap();

    let mut changed_release = release.clone();
    changed_release.reason = "stale_or_invalid_acceptance".to_owned();
    assert_eq!(
        manager.handle_release(changed_release),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );

    let mut cancel = cancel_for(&offered, CancellationMode::Force, "bs");
    cancel.effect_id = release.effect_id;
    assert_eq!(
        manager.handle_cancel(cancel),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );
}

#[tokio::test]
async fn root_cleanup_failure_suppresses_pre_execution_cancellation_evidence() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    let (mut root_preparation_started, release_root_preparation) =
        gate_assignment_root_preparation_with_outcome(
            &mut manager,
            GatedRootPreparationOutcome::CleanupFailed,
        );

    let cancel = cancel_while_root_preparation_is_blocked(
        &mut manager,
        &offered,
        &mut root_preparation_started,
    )
    .await;
    release_root_preparation
        .send(())
        .expect("release assignment root preparation");
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        manager.cleanup_failed
    }))
    .await
    .expect("assignment root cleanup failure was not observed");

    assert!(manager.slot.is_none());
    assert!(manager.reporting.is_none());
    assert!(cancellation_applications(&mut manager).is_empty());
    assert_no_terminal_observation(&mut manager);
    let retained = manager
        .cancellations
        .iter()
        .find(|retained| retained.command.request_id == cancel.request_id)
        .expect("retained cancellation command");
    assert!(!retained.ready);
    assert!(retained.observation_id.is_none());

    assert_offer_rejected_without_cleanup(&mut manager, offer("bh"), environment_unavailable());
}

#[tokio::test]
async fn cancellation_before_continuation_ready_releases_the_claim_without_an_ack() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bp");
    let prepare = prepare_for(&manager, &offered);
    let deadline = PreparationDeadline::from_wire(
        &prepare.preparation_expires_at,
        manager.sleeper.utc_now(),
        manager.sleeper.now(),
    )
    .unwrap();
    let work_root = Arc::clone(&manager.work_root);
    let offer_for_claim = offered.clone();
    let (root, claim_path) = tokio::task::spawn_blocking(move || {
        let prior_assignment = "asn_01k0z6r1w8f4jy2m7q9v3x5abn";
        let prior_attempt = "atm_01k0z6r1w8f4jy2m7q9v3x5abn";
        let previous = work_root
            .create_assignment_for_attempt(
                prior_assignment,
                &offer_for_claim.run_id,
                prior_attempt,
                None,
            )
            .unwrap();
        AssignmentProcessGuards::durable(&previous.private).unwrap();
        let previous_path = previous.workspace.path();
        assert_eq!(
            previous
                .release_pending(
                    ProcessQuiescence::Proven,
                    WorkspaceDisposition::Retain(RetentionReason::Failed),
                )
                .wait(),
            CleanupResult::Retained
        );
        let mut root = work_root
            .create_assignment_for_attempt(
                &offer_for_claim.assignment_id,
                &offer_for_claim.run_id,
                &offer_for_claim.attempt_id,
                None,
            )
            .unwrap();
        work_root
            .claim_retained_for_attempt(
                &mut root,
                super::super::workspace::RetainedClaimRequest {
                    assignment_id: &offer_for_claim.assignment_id,
                    run_id: &offer_for_claim.run_id,
                    attempt_id: &offer_for_claim.attempt_id,
                    prior_assignment_id: prior_assignment,
                    prior_attempt_id: prior_attempt,
                    recorded_root: &previous_path,
                },
            )
            .unwrap();
        (
            root,
            previous_path
                .parent()
                .unwrap()
                .join(".scherzo-runner-serve-claim-v1"),
        )
    })
    .await
    .unwrap();
    assert!(claim_path.exists());

    let cancellation = CaptureCancellation::default();
    manager.slot = Some(LocalSlot::Preparing(Box::new(PreparingAssignment {
        offer: offered.clone(),
        cancellation: cancellation.clone(),
        root_preparation: None,
        root: None, // The preparation worker owns the claimed root until it reports.
        prepare_effect_id: Some(prepare.effect_id.clone()),
        preparation_event: None,
    })));
    let id = manager
        .outbox
        .enqueue(AssignmentObservation::ContinuationReady {
            assignment_id: offered.assignment_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            start_snapshot: json!({"algorithm": "git_worktree_sha256_v1", "unavailable": "git_unavailable"}),
            quiescence: json!({"groupsRecorded": 0, "groupsTerminated": 0, "groupsAbsent": 0, "provenAt": NOW}),
            modified: json!("unknown"),
        })
        .unwrap();
    let outbox = manager.outbox.clone();
    let sleeper = Arc::clone(&manager.sleeper);
    let sender = manager.event_sender.clone();
    let wake = manager.outbox.clone();
    let (waiting, waiting_rx) = tokio::sync::oneshot::channel();
    let worker_offer = offered.clone();
    let worker = tokio::spawn(async move {
        let wait =
            outbox.wait_for_continuation_ready(id, deadline, &cancellation, sleeper.as_ref());
        tokio::pin!(wait);
        assert!(
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(std::future::Future::poll(wait.as_mut(), cx).is_pending())
            })
            .await
        );
        let _ = waiting.send(());
        let decline = wait
            .await
            .expect_err("unacknowledged readiness must be cancelled");
        sender
            .send(ManagerEvent::Prepared {
                offer: Box::new(worker_offer),
                prepare_effect_id: prepare.effect_id,
                deadline,
                admission: Box::new(Err(Box::new((root, decline)))),
            })
            .unwrap();
        wake.wake();
    });
    with_watchdog(waiting_rx).await.unwrap().unwrap();
    manager.finish_transport(); // No ACK even on the disconnected transport.
    manager
        .handle_cancel(cancel_for(&offered, CancellationMode::Graceful, "bq"))
        .unwrap();
    with_watchdog(worker).await.unwrap().unwrap();
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        manager.slot.is_none()
    }))
    .await
    .unwrap();
    assert!(!claim_path.exists());
    assert!(!manager.cleanup_failed);
    assert_eq!(cancellation_applications(&mut manager).len(), 1);
}

#[tokio::test]
async fn cancellation_before_start_waits_for_preparation_containment_and_replays() {
    let (_temporary, mut manager, offered, mut root_preparation_started, release_root_preparation) =
        gated_root_preparation_fixture();

    let cancel = cancel_while_root_preparation_is_blocked(
        &mut manager,
        &offered,
        &mut root_preparation_started,
    )
    .await;
    assert!(manager.handle_start(start_for(&offered)).unwrap().is_none());
    assert!(cancellation_applications(&mut manager).is_empty());
    assert_no_terminal_observation(&mut manager);

    release_root_preparation
        .send(())
        .expect("release assignment root preparation");
    let contained = with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        manager.slot.is_none() && manager.reporting.is_some()
    }))
    .await;
    if contained.is_err() {
        let slot = match &manager.slot {
            Some(LocalSlot::Preparing(_)) => "preparing",
            Some(LocalSlot::Accepted(_)) => "accepted",
            Some(LocalSlot::Running(_)) => "running",
            Some(LocalSlot::Finishing(_)) => "finishing",
            Some(LocalSlot::Releasing(_)) => "releasing",
            None => "idle",
        };
        panic!(
            "cancelled preparation did not prove containment: slot={slot}, reporting={}, cleanup_failed={}, cancellations={:?}",
            manager.reporting.is_some(),
            manager.cleanup_failed,
            manager.cancellations
        );
    }

    let (application_id, application) =
        pre_execution_cancellation_application(&mut manager, &cancel.request_id);
    assert_eq!(application.effective_mode, CancellationMode::Graceful);

    let mut changed_identity = cancel_for(&offered, CancellationMode::Graceful, "br");
    changed_identity.run_id = "run_01k0z6r1w8f4jy2m7q9v3x5abz".to_owned();
    assert_eq!(
        manager.handle_cancel(changed_identity),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );

    manager.mark_observation_encoded(application_id);
    manager.finish_transport();
    let replay = cancellation_applications(&mut manager);
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].0, application_id);
    assert_eq!(replay[0].1, application);

    manager.handle_cancel(cancel.clone()).unwrap();
    assert_eq!(cancellation_applications(&mut manager).len(), 1);
    manager.acknowledge_observation(application_id);
    manager.handle_cancel(cancel).unwrap();
    assert_eq!(cancellation_applications(&mut manager).len(), 1);

    manager.handle_offer(offered.clone()).unwrap();
    assert!(manager.handle_start(start_for(&offered)).unwrap().is_none());
    assert!(manager.slot.is_none());
}

#[tokio::test]
async fn running_cancellation_is_sticky_and_exactly_fenced() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let _job = execution_job(&mut manager, &offered);

    let graceful = cancel_for(&offered, CancellationMode::Graceful, "bm");
    manager.handle_cancel(graceful.clone()).unwrap();
    assert!(matches!(
        &manager.slot,
        Some(LocalSlot::Running(running))
            if running.cancellation.cancellation_reason()
                == Some(CancellationReason::UserRequest)
    ));
    let force = cancel_for(&offered, CancellationMode::Force, "bn");
    manager.handle_cancel(force.clone()).unwrap();
    let superseded = cancel_for(&offered, CancellationMode::Graceful, "bp");
    manager.handle_cancel(superseded).unwrap();

    let applications = cancellation_applications(&mut manager);
    assert!(applications.iter().any(|(_, application)| {
        application.request_id == graceful.request_id
            && application.effective_mode == CancellationMode::Graceful
            && application.disposition == CancellationApplicationDisposition::OrdinaryCancelling
    }));
    let force_application = applications
        .iter()
        .find(|(_, application)| application.request_id == force.request_id)
        .expect("force cancellation application");
    assert_eq!(force_application.1.effective_mode, CancellationMode::Force);
    assert_eq!(
        force_application.1.disposition,
        CancellationApplicationDisposition::ForceCancelling
    );
    assert!(applications.iter().any(|(_, application)| {
        application.effective_mode == CancellationMode::Force
            && application.disposition == CancellationApplicationDisposition::Superseded
    }));

    manager.acknowledge_observation(force_application.0);
    manager.handle_cancel(force.clone()).unwrap();
    assert_eq!(
        cancellation_applications(&mut manager)
            .iter()
            .filter(|(_, application)| application.request_id == force.request_id)
            .count(),
        1
    );

    let mut changed = force.clone();
    changed.mode = CancellationMode::Graceful;
    assert_eq!(
        manager.handle_cancel(changed),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );
    let mut stale = cancel_for(&offered, CancellationMode::Graceful, "bq");
    stale.attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abz".to_owned();
    assert_eq!(
        manager.handle_cancel(stale),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );
}

async fn run_event_fixture(
    workflow: &str,
) -> (
    tempfile::TempDir,
    AssignmentManager,
    AssignmentOffer,
    ExecutionJob,
    crate::telemetry::TestCapture,
) {
    let (temporary, mut manager) = manager_fixture(workflow);
    let (recorder, capture) = crate::telemetry::test_recorder("rbt_fixture");
    manager.recorder = Some(recorder);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let job = execution_job(&mut manager, &offered);
    (temporary, manager, offered, job, capture)
}

#[tokio::test]
async fn run_event_ends_once_at_terminal_ack_or_fence() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager, offered, _job, capture) = run_event_fixture(workflow).await;
    assert!(
        capture
            .events()
            .iter()
            .all(|event| event["event.name"] != "runner.run")
    );

    let event = manager.run_events.get(&offered.assignment_id).unwrap();
    event.result("failed");
    event.set(KeyValue::new(
        crate::telemetry::attribute::FAILURE_CAUSE_TYPE,
        "occurrence_conflict",
    ));
    event.set(KeyValue::new(
        crate::telemetry::attribute::DIAGNOSTIC_STAGE,
        "harness_execution",
    ));
    let id = manager
        .outbox
        .enqueue(AssignmentObservation::Execution {
            assignment_id: offered.assignment_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            report: ExecutionReport::Aborted {
                last_execution_event_sequence: 0,
                reason: "runner_internal_failure".to_owned(),
            },
        })
        .unwrap();
    manager.acknowledge_observation(id);
    manager.acknowledge_observation(id);
    manager.retire_assignment_observations(&offered.assignment_id);
    let run_events = |capture: &crate::telemetry::TestCapture| {
        capture
            .events()
            .into_iter()
            .filter(|event| event["event.name"] == "runner.run")
            .collect::<Vec<_>>()
    };
    let runs = run_events(&capture);
    assert_eq!(runs.len(), 1);
    let run = &runs[0];
    assert_eq!(run["um.run.id"], offered.run_id);
    assert_eq!(run["um.assignment.id"], offered.assignment_id);
    assert_eq!(run["um.attempt.id"], offered.attempt_id);
    assert_eq!(run["um.runner.boot_id"], "rbt_fixture");
    assert_eq!(run["um.run.result"], "failed");
    assert_eq!(run["um.failure.cause_type"], "occurrence_conflict");
    assert_eq!(run["um.diagnostic.stage"], "harness_execution");
    let spans = capture.spans();
    let span = spans.iter().find(|span| span.name == "runner.run").unwrap();
    for key in [
        "um.run.id",
        "um.assignment.id",
        "um.attempt.id",
        "um.run.result",
        "um.failure.cause_type",
        "um.diagnostic.stage",
    ] {
        assert_eq!(
            span.attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == key)
                .unwrap()
                .value
                .to_string()
                .trim_matches('"'),
            run[key].as_str().unwrap()
        );
    }

    let (_temporary, mut manager, offered, _job, capture) = run_event_fixture(workflow).await;
    manager.retire_assignment_observations(&offered.assignment_id);
    manager.retire_assignment_observations(&offered.assignment_id);
    let runs = run_events(&capture);
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["um.run.result"], "fenced");
}

#[tokio::test]
async fn fenced_completion_keeps_its_disposition_without_a_report() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager, offered, _job, capture) = run_event_fixture(workflow).await;
    manager
        .event_sender
        .send(ManagerEvent::Finished {
            assignment_id: offered.assignment_id.clone(),
            final_observation_id: None,
            final_delivery_deadline: None,
            lease_clock_failed: false,
            fenced: true,
            retained_root: None,
            quiescence: ProcessQuiescence::Proven,
            quiescence_failure: None,
            workspace_disposition: WorkspaceDisposition::Retain(RetentionReason::Interrupted),
        })
        .unwrap();
    manager.drain_events();
    assert_eq!(capture.event("runner.run")["um.run.result"], "fenced");
    assert_eq!(
        capture
            .events()
            .iter()
            .filter(|event| event["event.name"] == "runner.run")
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_execution_activation_is_classified_without_error_text() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager, _offered, job, capture) = run_event_fixture(workflow).await;
    if let Some(LocalSlot::Running(running)) = &manager.slot {
        running.workflow_git.disable();
    }
    job.spawn();
    let reports = with_watchdog(wait_for_terminal(&mut manager))
        .await
        .unwrap();
    assert!(reports.iter().any(|report| matches!(report, ExecutionReport::Aborted { reason, .. } if reason == "execution_environment_lost")));
    assert_acknowledged_run(&mut manager, &capture, "aborted");
    let event = capture.event("runner.run");
    assert_eq!(
        event["um.failure.cause_type"],
        "workflow_git_activation_failed"
    );
    assert_eq!(event["um.diagnostic.stage"], "workflow_git_activation");
    assert!(
        !serde_json::to_string(&event)
            .unwrap()
            .contains(_temporary.path().to_str().unwrap())
    );
}

#[tokio::test]
async fn terminal_grace_clock_error_classifies_the_retained_run() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager, offered, _job, capture) = run_event_fixture(workflow).await;
    let (clock, control, _waits) = controlled_lease_clock();
    manager.lease_clock = clock;
    control.make_timer_unavailable();
    let deadline = manager.lease_clock.now().unwrap();
    assert_eq!(
        manager.start_final_grace(offered.assignment_id.clone(), 1, deadline, false),
        Err(LeaseClockError::TimerUnavailable)
    );
    assert!(
        capture
            .events()
            .iter()
            .all(|event| event["event.name"] != "runner.run")
    );
    drop(manager);
    let event = capture.event("runner.run");
    assert_eq!(event["um.run.result"], "aborted");
    assert_eq!(event["um.failure.cause_type"], "lease_timer_unavailable");
    assert_eq!(event["um.diagnostic.stage"], "terminal_acknowledgement");
}

#[tokio::test]
async fn terminal_grace_wait_failure_classifies_the_retained_run() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager, offered, _job, capture) = run_event_fixture(workflow).await;
    let (clock, control, mut waits) = controlled_lease_clock();
    manager.lease_clock = clock;
    let deadline = manager.lease_clock.now().unwrap();
    manager
        .start_final_grace(offered.assignment_id.clone(), 1, deadline, false)
        .unwrap();
    let (_, release) = waits.recv().await.unwrap();
    control.make_wait_unavailable();
    release.release();
    let failure = manager.events.recv().await.unwrap();
    assert!(
        matches!(&failure, ManagerEvent::LeaseClockFailed { assignment_id, error: LeaseClockError::TimerWaitFailed } if assignment_id == &offered.assignment_id)
    );
    manager.event_sender.send(failure).unwrap();
    manager.drain_events();
    assert!(manager.lease_clock_failed);
    drop(manager);
    let event = capture.event("runner.run");
    assert_eq!(event["um.failure.cause_type"], "lease_timer_wait_failed");
    assert_eq!(event["um.diagnostic.stage"], "terminal_acknowledgement");
}

#[tokio::test]
async fn authorized_execution_reports_one_run_after_acknowledgement() {
    for (command, expected) in [("true", "succeeded"), ("false", "failed")] {
        let workflow = format!(
            "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"{command}\"]\n"
        );
        let (_temporary, mut manager) = manager_fixture(&workflow);
        let (recorder, capture) = crate::telemetry::test_recorder("rbt_fixture");
        manager.recorder = Some(recorder);
        let offered = offer("bg");
        offer_then_prepare(&mut manager, &offered).await;
        spawn_execution(&mut manager, &offered);
        let reports = with_watchdog(wait_for_terminal(&mut manager))
            .await
            .unwrap();
        assert_eq!(
            reports.iter().filter(|report| report.is_terminal()).count(),
            1
        );
        assert_acknowledged_run(&mut manager, &capture, expected);
        if expected == "failed" {
            let run = capture.event("runner.run");
            assert!(run["um.failure.phase"].is_string());
            assert!(run["um.failure.code"].is_string());
        }
    }
}

#[tokio::test]
async fn cancellation_during_result_delivery_reports_execution_terminal() {
    let (_temporary, mut manager, offered, cancellation) = cancellable_running_fixture().await;
    let engine_terminal = match &manager.slot {
        Some(LocalSlot::Running(running)) => Arc::clone(&running.engine_terminal),
        _ => panic!("assignment must be running"),
    };
    engine_terminal.store(true, Ordering::Release);

    let cancel = cancel_for(&offered, CancellationMode::Force, "bm");
    manager.handle_cancel(cancel.clone()).unwrap();

    assert_eq!(cancellation.cancellation_reason(), None);
    assert!(
        cancellation_applications(&mut manager)
            .iter()
            .any(|(_, application)| {
                application.request_id == cancel.request_id
                    && application.disposition
                        == CancellationApplicationDisposition::ExecutionTerminal
            })
    );
}

#[tokio::test]
async fn cloud_graceful_cancellation_preserves_open_finalizers() {
    let (_temporary, mut manager, offered, cancellation) = cancellable_running_fixture().await;
    assert!(cancellation.fixture_begin_finalization_arm());
    assert!(cancellation.fixture_complete_finalization_arm());

    let cancel = cancel_for(&offered, CancellationMode::Graceful, "bm");
    manager.handle_cancel(cancel.clone()).unwrap();

    assert_eq!(cancellation.cancellation_reason(), None);
    assert!(!cancellation.finalization_cancellation_requested());
    assert!(
        cancellation_applications(&mut manager)
            .iter()
            .any(|(_, application)| {
                application.request_id == cancel.request_id
                    && application.disposition
                        == CancellationApplicationDisposition::FinalizersPreserved
            })
    );
}

#[tokio::test]
async fn causally_requested_renewal_survives_user_cancellation() {
    for (mode, suffix) in [
        (CancellationMode::Graceful, "bm"),
        (CancellationMode::Force, "bn"),
    ] {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let (lease_clock, control, _waits) = controlled_lease_clock();
        manager.lease_clock = lease_clock;
        let offered = offer("bg");
        offer_then_prepare(&mut manager, &offered).await;
        let job = execution_job(&mut manager, &offered);
        control.advance(Duration::from_secs(1));
        request_next_renewal(&mut manager, &offered);

        manager
            .handle_cancel(cancel_for(&offered, mode, suffix))
            .unwrap();
        let decision = manager
            .handle_renewal(renewal_for(&offered))
            .unwrap_or_else(|failure| panic!("{mode:?} renewal failed: {failure:?}"));

        assert_eq!(decision.disposition, RenewalDisposition::Applied);
        assert_eq!(job.authority_updates.borrow().sequence, 2);
        assert!(!job.authority_updates.borrow().revoked);
    }
}

#[tokio::test]
async fn user_cancellation_completion_waits_for_process_quiescence() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager, offered, job, capture) = run_event_fixture(workflow).await;
    manager
        .handle_cancel(cancel_for(&offered, CancellationMode::Graceful, "bm"))
        .unwrap();
    job.spawn();

    let reports = wait_for_terminal(&mut manager).await;
    assert!(reports.iter().any(|report| matches!(
        report,
        ExecutionReport::Finished { outcome, .. }
            if outcome["outcome"] == "cancelled" && outcome["reason"] == "user_request"
    )));
    assert_acknowledged_run(&mut manager, &capture, "cancelled");
}

#[tokio::test]
async fn runner_shutdown_remains_an_interruption_after_user_cancellation() {
    let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"sleep 60\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (recorder, capture) = crate::telemetry::test_recorder("rbt_fixture");
    manager.recorder = Some(recorder);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    spawn_execution(&mut manager, &offered);
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager
            .pending_observations(&BTreeSet::new(), 100)
            .iter()
            .any(|pending| {
                matches!(
                    &pending.observation,
                    AssignmentObservation::Execution {
                        report: ExecutionReport::Transition { workflow_event, .. },
                        ..
                    } if workflow_event["eventType"] == "step_state_changed"
                        && workflow_event["stepId"] == "check"
                        && workflow_event["to"] == "running"
                )
            })
    }))
    .await
    .expect("workflow did not enter ordinary execution");

    manager
        .handle_cancel(cancel_for(&offered, CancellationMode::Graceful, "bm"))
        .unwrap();
    manager.begin_shutdown().unwrap();

    let reports = wait_for_terminal(&mut manager).await;
    assert!(reports.iter().any(|report| matches!(
        report,
        ExecutionReport::Interrupted {
            reason,
            terminal_outcome,
            ..
        } if reason == "graceful_shutdown"
            && terminal_outcome["outcome"] == "cancelled"
            && terminal_outcome["reason"] == "user_request"
    )));
    assert_acknowledged_run(&mut manager, &capture, "interrupted");
}

#[tokio::test]
async fn failed_containment_suppresses_cancellation_completion_and_slot_reuse() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let mut job = execution_job(&mut manager, &offered);
    job.use_quiescence_fixture(Arc::new(AtomicBool::new(false)));
    let (clock, _control, mut waits) = controlled_lease_clock();
    job.use_containment_clock(clock);
    manager
        .handle_cancel(cancel_for(&offered, CancellationMode::Graceful, "bm"))
        .unwrap();
    job.spawn();
    with_watchdog(async {
        let notification = manager.notification();
        let mut released = 0;
        while released < 80 {
            let notified = notification.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let pending = manager.pending_observations(&BTreeSet::new(), 100);
            fail_pending_artifact_registrations(&mut manager, &pending);
            tokio::select! {
                Some((_, timer)) = waits.recv() => {
                    timer.release();
                    released += 1;
                }
                () = &mut notified => {}
            }
        }
    })
    .await
    .expect("failed containment did not complete bounded rechecks");

    with_watchdog(async {
        let notification = manager.notification();
        loop {
            let notified = notification.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            manager.drain_events();
            let pending = manager.pending_observations(&BTreeSet::new(), 100);
            fail_pending_artifact_registrations(&mut manager, &pending);
            if manager.cleanup_failed && manager.slot.is_none() {
                return;
            }
            notified.await;
        }
    })
    .await
    .expect("failed containment did not fence assignment admission");
    assert!(manager.reporting.is_none());
    assert_no_terminal_observation(&mut manager);

    assert_offer_rejected_without_cleanup(&mut manager, offer("bh"), environment_unavailable());
    assert!(manager.slot.is_none());
}

#[tokio::test]
async fn stale_completion_events_preserve_an_accepted_successor() {
    for grace_elapsed in [true, false] {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let predecessor = offer("bg");
        offer_then_prepare(&mut manager, &predecessor).await;
        let accepted = match manager.slot.take().unwrap() {
            LocalSlot::Accepted(accepted) => accepted,
            _ => panic!("predecessor must be accepted"),
        };
        let identity = accepted.identity;
        let final_observation_id = enqueue_finished(&manager, &identity);
        manager.slot = Some(LocalSlot::Finishing(Box::new(FinishingAssignment {
            identity: identity.clone(),
            final_observation_id,
            root: Some(accepted.root),
            workspace_disposition: WorkspaceDisposition::Remove,
        })));
        manager.acknowledge_observation(final_observation_id);
        settle_cleanup(&mut manager).await;
        let successor = offer("bh");
        offer_then_prepare(&mut manager, &successor).await;
        let event = if grace_elapsed {
            ManagerEvent::FinalGraceElapsed {
                assignment_id: identity.assignment_id,
                final_observation_id,
                continue_reporting: true,
            }
        } else {
            ManagerEvent::Finished {
                assignment_id: identity.assignment_id,
                final_observation_id: Some(final_observation_id),
                final_delivery_deadline: None,
                lease_clock_failed: false,
                fenced: false,
                retained_root: None,
                quiescence: ProcessQuiescence::Proven,
                quiescence_failure: None,
                workspace_disposition: WorkspaceDisposition::Remove,
            }
        };
        manager.event_sender.send(event).unwrap();
        manager.pending_observations(&BTreeSet::new(), 100);
        assert!(matches!(&manager.slot, Some(LocalSlot::Accepted(accepted))
                if accepted.identity.assignment_id == successor.assignment_id));
    }
}

#[tokio::test]
async fn cleanup_exhaustion_preserves_the_preselected_terminal_report() {
    let (_temporary, mut manager, remover, mut requests) = gated_cleanup_manager([false; 6]);
    let predecessor = offer("bg");
    let successor = offer("bh");
    offer_then_prepare(&mut manager, &predecessor).await;
    let acceptance = manager.pending_observations(&BTreeSet::new(), 1)[0].id;
    manager.acknowledge_observation(acceptance);
    let accepted = match manager.slot.take() {
        Some(LocalSlot::Accepted(accepted)) => accepted,
        _ => panic!("predecessor must be accepted"),
    };
    let identity = accepted.identity.clone();
    let root_path = accepted.root.execution.parent().unwrap().to_owned();
    let final_observation_id = enqueue_finished(&manager, &identity);
    manager.slot = Some(LocalSlot::Finishing(Box::new(FinishingAssignment {
        identity: identity.clone(),
        final_observation_id,
        root: Some(accepted.root),
        workspace_disposition: WorkspaceDisposition::Remove,
    })));
    manager
        .event_sender
        .send(ManagerEvent::FinalGraceElapsed {
            assignment_id: identity.assignment_id,
            final_observation_id,
            continue_reporting: true,
        })
        .unwrap();
    manager.pending_observations(&BTreeSet::new(), 10);
    manager.handle_offer(successor.clone()).unwrap();

    release_all_cleanup_retries(&mut manager, &mut requests).await;
    settle_cleanup(&mut manager).await;

    assert!(root_path.exists());
    assert_eq!(remover.calls.load(Ordering::Relaxed), 6);
    let pending = manager.pending_observations(&BTreeSet::new(), 10);
    assert!(pending.iter().any(|entry| matches!(
        &entry.observation,
        AssignmentObservation::Execution {
            report: ExecutionReport::Finished { outcome, .. },
            ..
        } if outcome == &json!({ "outcome": "succeeded", "forceAbort": null })
    )));
    assert!(pending.iter().any(|entry| matches!(
        &entry.observation,
        AssignmentObservation::Decision(AssignmentDecision::Rejected {
            assignment_id,
            decline: AssignmentDecline::RunnerUnable(
                RunnerUnableReason::ExecutionEnvironmentUnavailable
            ),
            ..
        }) if assignment_id == &successor.assignment_id
    )));
}

#[tokio::test]
async fn execution_spec_accepts_the_shared_parallelism_limit_and_declines_the_next_value() {
    let mut execution_spec = offer("bg").execution_spec;
    execution_spec.execution_limits.maximum_parallel_steps =
        u64::try_from(MAXIMUM_PARALLEL_STEPS).unwrap();
    assert_eq!(validate_execution_spec(&execution_spec), Ok(()));

    execution_spec.execution_limits.maximum_parallel_steps += 1;
    assert_eq!(
        validate_execution_spec(&execution_spec),
        Err(invalid_execution_limits())
    );
}

// These compact admission smoke tests intentionally share fixture setup.
#[tokio::test]
async fn command_workflows_do_not_require_an_agent_runtime() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    assert_eq!(active_step_count(&manager), Some(1));
}

#[tokio::test]
async fn managed_workflow_environment_excludes_runner_credentials_and_helpers() {
    let workflow = r#"schemaVersion: 1
environmentPassthrough: [RUNNER_VISIBLE]
steps:
  check:
    kind: cmd
    command:
      argv:
        - sh
        - -c
        - 'test "$RUNNER_VISIBLE" = retained && test -z "${RUNNER_HIDDEN+x}" && test -z "${GH_TOKEN+x}" && test -z "${GITHUB_TOKEN+x}" && test -z "${GIT_ASKPASS+x}" && test -z "${GIT_CONFIG_KEY_0+x}" && test -z "${GIT_CONFIG_VALUE_0+x}" && test -z "${GIT_SSH_COMMAND+x}" && test -z "${SSH_AUTH_SOCK+x}" && test -z "${SSH_AGENT_PID+x}" && test -z "${UM_SOURCE_TOKEN_FD+x}"'
"#;
    let (_temporary, mut manager) = manager_fixture(workflow);
    let mut variables = manager.environment.variables().clone();
    for (name, value) in [
        ("RUNNER_VISIBLE", "retained"),
        ("RUNNER_HIDDEN", "private"),
        ("GIT_ASKPASS", "/runner/private/askpass"),
        ("GIT_ASKPASS_REQUIRE", "force"),
        ("GIT_TERMINAL_PROMPT", "0"),
        ("GIT_CONFIG", "runner-private"),
        ("GIT_CONFIG_COUNT", "1"),
        ("GIT_CONFIG_PARAMETERS", "runner-private"),
        ("GIT_CONFIG_GLOBAL", "/runner/private/gitconfig"),
        ("GIT_CONFIG_NOSYSTEM", "1"),
        ("GIT_CONFIG_SYSTEM", "/runner/private/system-gitconfig"),
        ("GIT_CONFIG_KEY_0", "http.extraHeader"),
        ("GIT_CONFIG_VALUE_0", "authorization: runner-private"),
        ("GIT_SSH", "/runner/private/ssh"),
        ("GIT_SSH_COMMAND", "/runner/private/ssh --private"),
        ("SSH_ASKPASS", "/runner/private/ssh-askpass"),
        ("SSH_ASKPASS_REQUIRE", "force"),
        ("SSH_AUTH_SOCK", "/runner/private/agent.sock"),
        ("SSH_AGENT_PID", "4242"),
        ("GH_TOKEN", "runner-private"),
        ("GITHUB_TOKEN", "runner-private"),
        ("UM_SOURCE_TOKEN_FD", "9"),
    ] {
        variables.insert(OsString::from(name), OsString::from(value));
    }
    manager.environment = EnvironmentSnapshot::new(variables);
    let offered = offer("bg");

    offer_then_prepare(&mut manager, &offered).await;

    let execution = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => accepted.admitted.execution(),
        _ => panic!("assignment should be accepted"),
    };
    assert!(execution.root().join(".git").is_dir());
    let environment = execution.environment();
    assert_eq!(
        environment.variable(OsStr::new("RUNNER_VISIBLE")),
        Some(OsStr::new("retained"))
    );
    assert_eq!(
        environment.variable(OsStr::new("UM_SOURCE_BRANCH")),
        Some(OsStr::new("main"))
    );
    let commit = run_fixture_git(execution.root(), &["rev-parse", "HEAD"]);
    assert_eq!(
        environment.variable(OsStr::new("UM_SOURCE_COMMIT_OID")),
        Some(OsStr::new(&commit))
    );
    for name in [
        "RUNNER_HIDDEN",
        "GIT_ASKPASS",
        "GIT_ASKPASS_REQUIRE",
        "GIT_TERMINAL_PROMPT",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_SYSTEM",
        "GIT_CONFIG_KEY_0",
        "GIT_CONFIG_VALUE_0",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "SSH_ASKPASS",
        "SSH_ASKPASS_REQUIRE",
        "SSH_AUTH_SOCK",
        "SSH_AGENT_PID",
        "GH_TOKEN",
        "GITHUB_TOKEN",
        "UM_SOURCE_TOKEN_FD",
    ] {
        assert!(environment.variable(OsStr::new(name)).is_none(), "{name}");
    }

    assert_succeeded(&execute_to_terminal(&mut manager, &offered).await);
}

#[tokio::test]
async fn release_retires_an_unsent_acceptance_observation() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    assert_eq!(manager.pending_observations(&BTreeSet::new(), 10).len(), 1);

    manager
        .handle_release(release_for(&offered, "br", "stale_or_invalid_acceptance"))
        .unwrap();
    settle_cleanup(&mut manager).await;

    assert!(manager.slot.is_none());
    assert_only_workspace_retention(&mut manager);
}

#[tokio::test]
async fn accepted_phase_shutdown_omits_non_authoritative_finalization() {
    let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;

    manager.begin_shutdown().unwrap();

    assert!(matches!(manager.slot, Some(LocalSlot::Finishing(_))));
    let pending = manager.pending_observations(&BTreeSet::new(), 10);
    assert_eq!(
        pending.len(),
        2,
        "semantic acceptance must remain ahead of accepted-phase interruption"
    );
    assert!(matches!(
        &pending[0].observation,
        AssignmentObservation::Decision(AssignmentDecision::Accepted { .. })
    ));
    assert!(matches!(
        &pending[1].observation,
        AssignmentObservation::Execution {
            report: ExecutionReport::AssignmentInterrupted { reason },
            ..
        } if reason == "graceful_shutdown"
    ));
}

#[tokio::test]
async fn shutdown_starts_finishing_cleanup_before_the_five_second_reserve() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let accepted = match manager.slot.take() {
        Some(LocalSlot::Accepted(accepted)) => accepted,
        _ => panic!("assignment should be accepted"),
    };
    let root_path = accepted.root.execution.parent().unwrap().to_owned();
    let final_observation_id = enqueue_finished(&manager, &accepted.identity);
    manager.slot = Some(LocalSlot::Finishing(Box::new(FinishingAssignment {
        identity: accepted.identity,
        final_observation_id,
        root: Some(accepted.root),
        workspace_disposition: WorkspaceDisposition::Remove,
    })));
    let (lease_clock, _control, mut waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;

    manager.begin_shutdown().unwrap();
    let notification = manager.notification();
    let elapsed = notification.notified();
    tokio::pin!(elapsed);
    lease_wait_request(&mut waits, crate::service::SHUTDOWN_CLEANUP_START_TIMEOUT)
        .await
        .release();
    elapsed.await;
    manager.pending_observations(&BTreeSet::new(), 10);
    settle_cleanup(&mut manager).await;

    assert!(!root_path.exists());
    assert!(manager.shutdown_complete());
}

#[tokio::test]
async fn shutdown_requests_runner_cancellation_for_running_work() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let _job = execution_job(&mut manager, &offered);

    manager.begin_shutdown().unwrap();

    assert!(matches!(
        &manager.slot,
        Some(LocalSlot::Running(running))
            if running.cancellation.cancellation_reason()
                == Some(CancellationReason::RunnerShutdown)
    ));
    let (authority_active, authority) = match &manager.slot {
        Some(LocalSlot::Running(running)) => (
            running.workflow_git.is_active(),
            running.workflow_git.clone(),
        ),
        _ => panic!("assignment must remain running during graceful shutdown"),
    };
    let report = authority.teardown(ProcessQuiescence::Proven);
    assert!(report.local_state_destroyed);
    assert!(
        !authority_active,
        "runner shutdown must immediately fence workflow Git authority"
    );
}

#[tokio::test]
async fn shutdown_rearms_after_the_finalization_boundary_and_reports_summary() {
    let workflow = "schemaVersion: 1\nsteps:\n  wait:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"sleep 60\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let reports =
        start_then_shut_down_and_wait(&mut manager, &offered, std::future::ready(())).await;

    let terminal_outcome = runner_shutdown_outcome(&reports);
    assert_eq!(terminal_outcome["finalization"]["trigger"], "cancelled");
    assert_eq!(
        terminal_outcome["finalization"]["cancellation"]["reason"],
        "runner_shutdown"
    );
    assert_eq!(
        terminal_outcome["finalization"]["finalizers"][0]["id"],
        "cleanup"
    );
    assert_eq!(
        terminal_outcome["finalization"]["finalizers"][0]["state"],
        "cancelled"
    );
}

#[tokio::test]
async fn exact_sequence_only_start_dispatches_once() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let start = start_for(&offered);
    assert!(manager.handle_start(start.clone()).unwrap().is_some());
    assert!(manager.handle_start(start).unwrap().is_none());
}

async fn start_execution_waiting_for_authority(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
) -> WorkflowGitAuthority {
    let job = manager
        .handle_start(start_for(offered))
        .unwrap()
        .expect("valid start dispatches execution");
    let workflow_git = match &manager.slot {
        Some(LocalSlot::Running(running)) => running.workflow_git.clone(),
        _ => panic!("assignment must be waiting to run"),
    };
    job.spawn();
    with_watchdog(wait_for_manager_state(manager, |manager| {
        manager
            .pending_observations(&BTreeSet::new(), 100)
            .iter()
            .any(|entry| {
                matches!(
                    &entry.observation,
                    AssignmentObservation::Execution {
                        report: ExecutionReport::Started,
                        ..
                    }
                )
            })
    }))
    .await
    .expect("execution_started was not queued");
    assert!(!workflow_git.is_active());
    workflow_git
}

fn assert_no_executed_workflow_reports(manager: &mut AssignmentManager) {
    assert!(
        manager
            .pending_observations(&BTreeSet::new(), 100)
            .iter()
            .all(|entry| !matches!(
                &entry.observation,
                AssignmentObservation::Execution {
                    report: ExecutionReport::Transition { .. } | ExecutionReport::Finished { .. },
                    ..
                }
            ))
    );
}

#[tokio::test]
async fn delayed_start_authority_gates_first_workflow_git_fetch() {
    let workflow = "schemaVersion: 1\nsteps:\n  fetch:\n    kind: cmd\n    command:\n      argv: [\"git\", \"fetch\", \"--quiet\", \"origin\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    start_execution_waiting_for_authority(&mut manager, &offered).await;
    assert_no_executed_workflow_reports(&mut manager);
    assert!(
        manager
            .pending_observations(&BTreeSet::new(), 100)
            .iter()
            .all(|entry| !matches!(
                &entry.observation,
                AssignmentObservation::Execution {
                    report: ExecutionReport::Interrupted { .. },
                    ..
                }
            ))
    );

    let mut wrong = start_authorization_for(&offered);
    wrong.run_id = "run_01k0z6r1w8f4jy2m7q9v3x5azz".to_owned();
    assert_eq!(
        manager.handle_start_authorized(wrong),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );
    assert!(matches!(
        &manager.slot,
        Some(LocalSlot::Running(running)) if !*running.start_authority.borrow()
    ));

    let authorization = start_authorization_for(&offered);
    manager
        .handle_start_authorized(authorization.clone())
        .unwrap();
    manager.finish_transport();
    manager
        .handle_start_authorized(authorization.clone())
        .expect("an exact reconnect redelivery is idempotent");
    let mut duplicate = authorization;
    duplicate.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abz".to_owned();
    assert_eq!(
        manager.handle_start_authorized(duplicate),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );

    let reports = with_watchdog(wait_for_terminal(&mut manager))
        .await
        .expect("authorized workflow Git fetch did not finish");
    assert_succeeded(&reports);
    assert_eq!(
        reports
            .iter()
            .filter(|report| matches!(report, ExecutionReport::Started))
            .count(),
        1,
        "delayed authorization launched execution more than once"
    );
}

#[tokio::test]
async fn shutdown_while_waiting_for_start_authority_never_activates_workflow_git() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let workflow_git = start_execution_waiting_for_authority(&mut manager, &offered).await;

    manager.begin_shutdown().unwrap();
    wait_for_execution_finalization(&mut manager).await;
    assert!(!workflow_git.is_active());
    assert_no_executed_workflow_reports(&mut manager);
}

#[tokio::test]
async fn cancellation_during_start_authorization_reports_pre_execution_stop() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    start_execution_waiting_for_authority(&mut manager, &offered).await;

    let cancel = cancel_for(&offered, CancellationMode::Graceful, "bm");
    manager.handle_cancel(cancel.clone()).unwrap();
    assert!(cancellation_applications(&mut manager).is_empty());
    assert_no_terminal_observation(&mut manager);
    assert!(matches!(manager.slot, Some(LocalSlot::Running(_))));

    manager
        .handle_start_authorized(start_authorization_for(&offered))
        .unwrap();
    assert!(matches!(
        &manager.slot,
        Some(LocalSlot::Running(running)) if !*running.start_authority.borrow()
    ));

    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        manager.slot.is_none() && manager.reporting.is_some()
    }))
    .await
    .expect("pre-execution cancellation did not finish containment");

    pre_execution_cancellation_application(&mut manager, &cancel.request_id);
    assert_no_executed_workflow_reports(&mut manager);
}

fn authority_offsets(authority: &LeaseAuthority) -> Vec<Duration> {
    [
        authority.renewal_request,
        authority.cancellation_start,
        authority.force_stop_start,
        authority.force_stop_end,
        authority.local_expiry,
    ]
    .into_iter()
    .map(|boundary| boundary.checked_duration_since(authority.basis).unwrap())
    .collect()
}

#[test]
fn welcomed_policy_requires_two_complete_renewal_leads() {
    for (renewal_budget_ms, lease_duration_ms, valid) in [
        (5_000, 371_000, true),
        (5_000, 370_999, false),
        (40_000, 391_000, true),
        (40_000, 390_999, false),
    ] {
        let mut candidate = policy();
        candidate.renewal_delivery_budget_milliseconds = renewal_budget_ms;
        candidate.lease_duration_milliseconds = lease_duration_ms;
        assert_eq!(validate_lease_policy(&candidate).is_ok(), valid);
    }
}

#[test]
fn active_policy_uses_thirty_second_headroom_for_every_grace() {
    for grace_seconds in 1..=300 {
        let (clock, _control, _waits) = controlled_lease_clock();
        let basis = clock.now().unwrap();
        let authority =
            LeaseAuthority::derive(1, basis, &policy(), Duration::from_secs(grace_seconds))
                .unwrap();
        assert_eq!(
            authority
                .cancellation_start
                .checked_duration_since(authority.renewal_request)
                .unwrap(),
            Duration::from_secs(30),
        );
        assert!(
            authority
                .renewal_request
                .checked_duration_since(basis)
                .unwrap()
                >= Duration::from_secs(30)
        );
    }
}

#[test]
fn active_policy_boundaries_advance_during_suspend() {
    for (grace_seconds, expected_offsets) in [
        (
            1,
            [
                Duration::from_secs(329),
                Duration::from_secs(359),
                Duration::from_secs(360),
                Duration::from_secs(365),
                Duration::from_secs(371),
            ],
        ),
        (
            300,
            [
                Duration::from_secs(30),
                Duration::from_secs(60),
                Duration::from_secs(360),
                Duration::from_secs(365),
                Duration::from_secs(371),
            ],
        ),
    ] {
        let (clock, control, _waits) = controlled_lease_clock();
        let basis = clock.now().unwrap();
        let authority =
            LeaseAuthority::derive(1, basis, &policy(), Duration::from_secs(grace_seconds))
                .unwrap();
        assert_eq!(authority_offsets(&authority), expected_offsets);
        control.simulate_suspend(expected_offsets[0]);
        assert_eq!(clock.now().unwrap(), authority.renewal_request);
        assert_eq!(
            authority
                .cancellation_start
                .checked_duration_since(clock.now().unwrap())
                .unwrap(),
            Duration::from_secs(30),
        );
    }

    let (clock, _control, _waits) = controlled_lease_clock();
    let basis = clock.now().unwrap();
    let initial = LeaseAuthority::derive(1, basis, &policy(), Duration::from_secs(300)).unwrap();
    let renewed = LeaseAuthority::derive(
        2,
        initial.renewal_request,
        &policy(),
        Duration::from_secs(300),
    )
    .unwrap();
    assert_eq!(
        renewed
            .renewal_request
            .checked_duration_since(initial.renewal_request)
            .unwrap(),
        Duration::from_secs(30),
    );
    assert_eq!(
        Duration::from_secs(60).as_secs()
            / renewed
                .renewal_request
                .checked_duration_since(initial.renewal_request)
                .unwrap()
                .as_secs(),
        2,
    );
}

#[tokio::test]
async fn maximum_grace_renewal_delivery_boundary_is_fail_closed() {
    fn fixture(
        label: &str,
    ) -> (
        tempfile::TempDir,
        AssignmentManager,
        PathBuf,
        PathBuf,
        PathBuf,
        AssignmentOffer,
    ) {
        let placeholder = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (temporary, manager) = manager_fixture(placeholder);
        let marker = temporary.path().join(format!("{label}-invocations"));
        let started = temporary.path().join(format!("{label}-started"));
        let release = temporary.path().join(format!("{label}-release"));
        // Opening the invocation marker can expose an empty file before `printf` runs.
        // Publish a distinct readiness boundary only after the append has completed.
        let script = format!(
            "printf 'invoked\\n' >> {} && mkdir {}; while [ ! -e {} ]; do sleep 0.01; done",
            marker.display(),
            started.display(),
            release.display()
        );
        let argv = serde_json::to_string(&["sh", "-c", script.as_str()]).unwrap();
        fs::write(
                temporary.path().join("source/workflow.yaml"),
                format!(
                    "schemaVersion: 1\nsteps:\n  wait:\n    kind: cmd\n    command:\n      argv: {argv}\n"
                ),
            )
            .unwrap();
        let source = temporary.path().join("source");
        run_fixture_git(&source, &["add", "workflow.yaml"]);
        run_fixture_git(&source, &["commit", "--quiet", "-m", "update workflow"]);
        let mut offered = offer(label);
        offered
            .execution_spec
            .execution_limits
            .cancellation_grace_seconds = 300;
        (temporary, manager, marker, started, release, offered)
    }

    fn assert_one_invocation(marker: &Path) {
        assert_eq!(fs::read_to_string(marker).unwrap(), "invoked\n");
    }

    let (_temporary, mut manager, marker, started, release, offered) = fixture("bg");
    let (control, mut waits, job) = controlled_execution_job(&mut manager, &offered).await;
    job.spawn();
    wait_for_fixture_path(&started).await;
    assert_one_invocation(&marker);
    lease_wait_request(&mut waits, Duration::from_secs(30))
        .await
        .release();
    let _request = wait_for_renewal_request(&mut manager).await;
    let obsolete_cancellation_wait = lease_wait_request(&mut waits, Duration::from_secs(30)).await;
    control.advance(Duration::from_millis(29_999));
    let renewal = renewal_for(&offered);
    let decision = manager.handle_renewal(renewal.clone()).unwrap();
    assert_eq!(decision.disposition, RenewalDisposition::Applied);
    assert_eq!(decision.request_age_ms, Some(29_999));
    assert_eq!(decision.cancellation_headroom_ms, Some(1));
    assert_eq!(
        manager.handle_renewal(renewal).unwrap().disposition,
        RenewalDisposition::ReplayApplied,
    );
    control.advance(Duration::from_millis(2));
    obsolete_cancellation_wait.release();
    let renewed_request_wait = with_watchdog(lease_wait_request(&mut waits, Duration::ZERO))
        .await
        .expect("supervisor did not replace the obsolete cancellation timer");
    assert!(matches!(
        &manager.slot,
        Some(LocalSlot::Running(running))
            if running.current_grant.sequence == 2
                && running.cancellation.cancellation_reason().is_none()
    ));
    renewed_request_wait.release();
    assert_one_invocation(&marker);
    fs::write(release, b"complete").unwrap();
    let reports = with_watchdog(wait_for_terminal(&mut manager))
        .await
        .expect("renewed workflow did not finish");
    assert_succeeded(&reports);
    assert_one_invocation(&marker);

    let (
        _temporary,
        mut boundary_manager,
        boundary_marker,
        boundary_started,
        boundary_release,
        boundary_offer,
    ) = fixture("bh");
    let (boundary_control, mut boundary_waits, boundary_job) =
        controlled_execution_job(&mut boundary_manager, &boundary_offer).await;
    boundary_job.spawn();
    wait_for_fixture_path(&boundary_started).await;
    assert_one_invocation(&boundary_marker);
    lease_wait_request(&mut boundary_waits, Duration::from_secs(30))
        .await
        .release();
    let _request = wait_for_renewal_request(&mut boundary_manager).await;
    boundary_control.advance(Duration::from_millis(30_000));
    let late = boundary_manager
        .handle_renewal(renewal_for(&boundary_offer))
        .unwrap();
    assert_eq!(late.disposition, RenewalDisposition::CancellationStarted);
    assert_eq!(late.request_age_ms, Some(30_000));
    assert_eq!(late.cancellation_headroom_ms, Some(0));
    assert!(matches!(
        &boundary_manager.slot,
        Some(LocalSlot::Running(running))
            if running.current_grant.sequence == 1
                && running.cancellation.cancellation_reason()
                    == Some(CancellationReason::ExecutionLeaseExpired)
                && running.authority_updates.borrow().revoked
    ));
    fs::write(boundary_release, b"complete").unwrap();
    boundary_control.advance(Duration::from_secs(300));
    lease_wait_request(&mut boundary_waits, Duration::from_secs(30))
        .await
        .release();
    with_watchdog(wait_for_terminal(&mut boundary_manager))
        .await
        .expect("cancelled workflow did not finish");
    assert_one_invocation(&boundary_marker);
}

#[tokio::test]
async fn slow_renewal_is_applied_before_cancellation_and_reports_real_headroom() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (clock, control, _waits) = controlled_lease_clock();
    manager.lease_clock = clock;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let job = execution_job(&mut manager, &offered);
    let original = job.authority_updates.borrow().clone();
    control.advance(
        original
            .renewal_request
            .checked_duration_since(manager.lease_clock.now().unwrap())
            .unwrap(),
    );
    let Some(LocalSlot::Running(running)) = &manager.slot else {
        panic!("expected running assignment")
    };
    running
        .causal_lease
        .request_renewal(
            1,
            &offered.assignment_id,
            &offered.attempt_id,
            &manager.lease_clock,
            &manager.outbox,
        )
        .unwrap();
    control.advance(Duration::from_secs(20));
    let renewal = renewal_for(&offered);
    let decision = manager.handle_renewal(renewal.clone()).unwrap();
    assert_eq!(decision.disposition, RenewalDisposition::Applied);
    assert_eq!(decision.cancellation_headroom_ms, Some(10_000));
    assert_eq!(decision.request_age_ms, Some(20_000));
    assert_eq!(job.authority_updates.borrow().sequence, 2);
    assert_eq!(
        manager.handle_renewal(renewal).unwrap().disposition,
        RenewalDisposition::ReplayApplied
    );
    let (recorder, capture) = crate::telemetry::test_recorder("renewal-test");
    let event = recorder.start("runner.effect_acknowledgement", []);
    decision.record(&event);
    event.finish(TelemetryOutcome::Success);
    let event = capture.event("runner.effect_acknowledgement");
    assert_eq!(event["um.lease.disposition"], "applied");
    assert_eq!(event["um.lease.cancellation_headroom_ms"], 10_000);
    assert_eq!(event["um.lease.request_age_ms"], 20_000);
}

#[tokio::test]
async fn causal_acceptance_basis() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (lease_clock, awake, _waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let acceptance_basis = manager.decisions[0]
        .causal_lease
        .as_ref()
        .unwrap()
        .basis(1)
        .unwrap();

    awake.advance(Duration::from_secs(100));
    let start = start_for(&offered);
    let job = manager
        .handle_start(start.clone())
        .unwrap()
        .expect("delayed causal start retains authority");
    let authority = job.authority_updates.borrow().clone();
    assert_eq!(authority.basis, acceptance_basis);
    assert_eq!(
        authority_offsets(&authority),
        vec![
            Duration::from_secs(329),
            Duration::from_secs(359),
            Duration::from_secs(360),
            Duration::from_secs(365),
            Duration::from_secs(371),
        ]
    );
    assert!(manager.handle_start(start).unwrap().is_none());

    async fn remaining_after_advance(simulated_suspend: bool) -> Duration {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let (lease_clock, control, _waits) = controlled_lease_clock();
        manager.lease_clock = lease_clock;
        let offered = offer("bh");
        offer_then_prepare(&mut manager, &offered).await;
        let job = manager
            .handle_start(start_for(&offered))
            .unwrap()
            .expect("causal start has authority");
        if simulated_suspend {
            control.simulate_suspend(Duration::from_secs(120));
        } else {
            control.advance(Duration::from_secs(120));
        }
        job.authority_updates
            .borrow()
            .cancellation_start
            .checked_duration_since(manager.lease_clock.now().unwrap())
            .unwrap()
    }
    assert_eq!(
        remaining_after_advance(false).await,
        remaining_after_advance(true).await,
        "awake delay and simulated suspend must consume equal authority"
    );

    let (_temporary, mut boundary_manager) = manager_fixture(workflow);
    let (lease_clock, boundary, _waits) = controlled_lease_clock();
    boundary_manager.lease_clock = lease_clock;
    let boundary_offer = offer("bj");
    boundary_manager
        .handle_offer(boundary_offer.clone())
        .unwrap();
    wait_for_offer_preparation(&mut boundary_manager).await;
    boundary.advance(Duration::from_secs(359));
    assert!(
        boundary_manager
            .handle_start(start_for(&boundary_offer))
            .unwrap()
            .is_none(),
        "a start at cancellation must not produce an execution job"
    );
}

#[tokio::test]
async fn wrong_wall_time_after_transport_does_not_change_lease_authority() {
    async fn outcome(sent_at: &str) -> (Vec<Duration>, usize) {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let (lease_clock, _control, _waits) = controlled_lease_clock();
        manager.lease_clock = lease_clock;
        let offered = offer("bg");
        offer_then_prepare(&mut manager, &offered).await;
        let start = start_from_civil_time(&offered, sent_at);
        let mut invocations = 0;
        let job = manager.handle_start(start.clone()).unwrap();
        if job.is_some() {
            invocations += 1;
        }
        assert!(manager.handle_start(start).unwrap().is_none());
        (
            authority_offsets(&job.unwrap().authority_updates.borrow()),
            invocations,
        )
    }

    let past = outcome("1900-01-01T00:00:00Z").await;
    let future = outcome("9999-12-31T23:59:59Z").await;
    assert_eq!(past, future);
    assert_eq!(past.1, 1);
}

#[tokio::test]
async fn delayed_replayed_and_conflicting_grants() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (lease_clock, control, _waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let original_basis = manager.decisions[0]
        .causal_lease
        .as_ref()
        .unwrap()
        .basis(1)
        .unwrap();
    control.advance(Duration::from_secs(100));
    let start = start_for(&offered);
    let job = manager
        .handle_start(start.clone())
        .unwrap()
        .expect("delayed start invokes once");
    let original_authority = job.authority_updates.borrow().clone();
    assert_eq!(original_authority.basis, original_basis);
    assert!(manager.handle_start(start).unwrap().is_none());
    assert_eq!(*job.authority_updates.borrow(), original_authority);

    let mut stale = renewal_for(&offered);
    stale.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abm".to_owned();
    stale.lease.sequence = 1;
    manager.handle_renewal(stale).unwrap();
    let mut gap = renewal_for(&offered);
    gap.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abn".to_owned();
    gap.lease.sequence = 3;
    assert_eq!(
        manager.handle_renewal(gap),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );
    request_next_renewal(&mut manager, &offered);
    let renewal = renewal_for(&offered);
    manager.handle_renewal(renewal.clone()).unwrap();
    let renewed_authority = job.authority_updates.borrow().clone();
    assert_eq!(renewed_authority.sequence, 2);
    manager.handle_renewal(renewal.clone()).unwrap();
    assert_eq!(*job.authority_updates.borrow(), renewed_authority);
    let mut conflict = renewal;
    conflict.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abp".to_owned();
    assert_eq!(
        manager.handle_renewal(conflict),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );

    let remaining = renewed_authority
        .cancellation_start
        .checked_duration_since(manager.lease_clock.now().unwrap())
        .unwrap();
    control.advance(remaining);
    let mut post_stop = renewal_for(&offered);
    post_stop.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abq".to_owned();
    post_stop.lease.sequence = 3;
    manager.handle_renewal(post_stop).unwrap();
    assert_eq!(job.authority_updates.borrow().sequence, 2);
    assert!(job.authority_updates.borrow().revoked);

    let (_temporary, mut pre_basis) = manager_fixture(workflow);
    let (lease_clock, _control, _waits) = controlled_lease_clock();
    pre_basis.lease_clock = lease_clock;
    offer_then_prepare(&mut pre_basis, &offered).await;
    let pre_basis_job = pre_basis
        .handle_start(start_for(&offered))
        .unwrap()
        .expect("valid start dispatches execution");
    let rejected = renewal_for(&offered);
    pre_basis.handle_renewal(rejected.clone()).unwrap();
    pre_basis.finish_transport();
    request_next_renewal(&mut pre_basis, &offered);
    pre_basis.handle_renewal(rejected.clone()).unwrap();
    assert_eq!(pre_basis_job.authority_updates.borrow().sequence, 1);
    let mut conflicting_reuse = rejected;
    conflicting_reuse.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abv".to_owned();
    assert_eq!(
        pre_basis.handle_renewal(conflicting_reuse),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );

    let (_temporary, mut unsolicited) = manager_fixture(workflow);
    unsolicited.handle_renewal(renewal_for(&offered)).unwrap();
    assert!(unsolicited.slot.is_none());
}

#[tokio::test]
async fn pre_start_renewal_replay_never_gains_authority() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (lease_clock, control, _waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;

    let unsolicited = renewal_for(&offered);
    manager.handle_renewal(unsolicited.clone()).unwrap();

    let job = manager
        .handle_start(start_for(&offered))
        .unwrap()
        .expect("valid start dispatches execution");
    control.advance(Duration::from_secs(1));
    request_next_renewal(&mut manager, &offered);
    manager.handle_renewal(unsolicited).unwrap();

    assert_eq!(
        job.authority_updates.borrow().sequence,
        1,
        "a grant received before execution and its causal renewal basis must remain inert"
    );
}

#[tokio::test]
async fn same_boot_reconnect_retains_lease_basis() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (lease_clock, control, _waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    let mut offered = offer("bg");
    align_offer_with_source_fixture(&manager, &mut offered);
    manager.handle_offer(offered.clone()).unwrap();
    prepare_current(&mut manager, &offered).await;
    let causal_lease = manager.decisions[0].causal_lease.clone().unwrap();
    let acceptance_basis = causal_lease.basis(1).unwrap();
    manager.finish_transport();
    control.advance(Duration::from_secs(10));
    manager.handle_offer(offered.clone()).unwrap();
    assert_eq!(causal_lease.basis(1), Some(acceptance_basis));

    causal_lease
        .request_renewal(
            1,
            &offered.assignment_id,
            &offered.attempt_id,
            &manager.lease_clock,
            &manager.outbox,
        )
        .unwrap();
    let renewal_basis = causal_lease.basis(2).unwrap();
    manager.finish_transport();
    control.advance(Duration::from_secs(10));
    causal_lease
        .request_renewal(
            1,
            &offered.assignment_id,
            &offered.attempt_id,
            &manager.lease_clock,
            &manager.outbox,
        )
        .unwrap();
    assert_eq!(causal_lease.basis(2), Some(renewal_basis));

    let (_new_process_root, mut new_process) = manager_fixture(workflow);
    assert!(
        new_process
            .handle_start(start_for(&offered))
            .unwrap()
            .is_none()
    );
    assert!(new_process.decisions.is_empty());
}

#[tokio::test]
async fn lease_authority_arithmetic_failure_grants_no_execution() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (lease_clock, _control, _waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    manager
        .lease_policy
        .as_mut()
        .unwrap()
        .lease_duration_milliseconds = u64::MAX;

    assert!(matches!(
        manager.handle_start(start_for(&offered)),
        Err(AssignmentManagerFailure::LeaseClock)
    ));
    assert!(matches!(manager.slot, Some(LocalSlot::Accepted(_))));
}

#[tokio::test]
async fn lease_timer_failure_marks_runner_boot_unsuccessful_after_terminal_report() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    let (control, _waits, job) = controlled_execution_job(&mut manager, &offered).await;
    control.make_timer_unavailable();
    job.spawn();

    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager.lease_clock_has_failed()
    }))
    .await
    .expect("lease timer failure did not fail the runner boot");
    let pending = manager.pending_observations(&BTreeSet::new(), 100);
    assert!(
        pending.iter().any(|entry| matches!(
            &entry.observation,
            AssignmentObservation::Execution {
                report: ExecutionReport::Aborted { reason, .. },
                ..
            } if reason == "runner_internal_failure"
        )),
        "timer failure pending observations: {pending:#?}"
    );
    assert!(pending.iter().all(|entry| !matches!(
        &entry.observation,
        AssignmentObservation::Execution {
            report: ExecutionReport::Started
                | ExecutionReport::Transition { .. }
                | ExecutionReport::Finished { .. },
            ..
        }
    )));
}

#[tokio::test]
async fn accepted_runner_assignment_enables_stopped_spawn_registration() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    manager.guard_processes = true;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    assert!(matches!(
        &manager.slot,
        Some(LocalSlot::Accepted(accepted))
            if accepted.process_guards.registry(accepted.guard_processes).is_durable()
    ));
}

#[tokio::test]
async fn lease_terminal_delivery_uses_the_welcomed_budget_without_reporting() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (lease_clock, _control, mut lease_waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let job = execution_job(&mut manager, &offered);
    drop(job);
    let deadline = manager
        .lease_clock
        .now()
        .unwrap()
        .checked_add(Duration::from_secs(5))
        .unwrap();
    enqueue_completion(&manager, deadline);
    manager.pending_observations(&BTreeSet::new(), 100);

    lease_wait_request(&mut lease_waits, Duration::from_secs(5))
        .await
        .release();
    let notification = manager.notification();
    with_watchdog(async {
        loop {
            let notified = notification.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            manager.pending_observations(&BTreeSet::new(), 100);
            if manager.slot.is_none() {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("terminal delivery budget did not fence replay");
    assert!(manager.reporting.is_none());
    assert_eq!(manager.pending_observations(&BTreeSet::new(), 100), vec![]);
}

#[tokio::test]
async fn zero_terminal_delivery_budget_fences_the_report_at_selection() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let mut zero_delivery_policy = policy();
    zero_delivery_policy.terminal_report_delivery_budget_milliseconds = 0;
    manager.lease_policy = Some(zero_delivery_policy);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let job = execution_job(&mut manager, &offered);
    assert_eq!(
        job.authority_updates
            .borrow()
            .terminal_report_delivery_budget,
        Duration::ZERO
    );
    drop(job);
    let final_observation_id = enqueue_completion(&manager, manager.lease_clock.now().unwrap());

    let pending = manager.pending_observations(&BTreeSet::new(), 100);
    assert!(
        pending.iter().all(|entry| entry.id != final_observation_id),
        "an exclusive zero delivery budget must not leave the terminal report replayable"
    );
    assert!(manager.slot.is_none());
}

#[tokio::test]
async fn ordinary_terminal_delivery_uses_the_welcomed_budget() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (lease_clock, _control, mut lease_waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let acceptance = manager.pending_observations(&BTreeSet::new(), 1)[0].id;
    manager.acknowledge_observation(acceptance);
    spawn_execution(&mut manager, &offered);

    let (renewal_duration, _renewal_release) =
        with_watchdog(lease_waits.recv()).await.unwrap().unwrap();
    assert_eq!(renewal_duration, Duration::from_secs(329));
    let notification = manager.notification();
    with_watchdog(async {
        loop {
            let notified = notification.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let pending = manager.pending_observations(&BTreeSet::new(), 100);
            if fail_pending_artifact_registrations(&mut manager, &pending) {
                continue;
            }
            if pending.iter().any(|entry| entry.observation.is_terminal()) {
                manager.pending_observations(&BTreeSet::new(), 100);
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("workflow did not select a terminal report");
    wait_for_execution_finalization(&mut manager).await;

    let (artifact_duration, _artifact_release) =
        with_watchdog(lease_waits.recv()).await.unwrap().unwrap();
    assert_eq!(artifact_duration, Duration::from_secs(329));

    let (delivery_duration, _delivery_release) =
        with_watchdog(lease_waits.recv()).await.unwrap().unwrap();
    assert_eq!(delivery_duration, Duration::from_secs(5));
}

#[tokio::test]
async fn completion_without_a_terminal_report_fences_assignment_writes() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let job = execution_job(&mut manager, &offered);
    drop(job);
    manager
        .outbox
        .enqueue(AssignmentObservation::Execution {
            assignment_id: offered.assignment_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            report: ExecutionReport::Started,
        })
        .unwrap();
    manager
        .event_sender
        .send(ManagerEvent::Finished {
            assignment_id: offered.assignment_id,
            final_observation_id: None,
            final_delivery_deadline: None,
            lease_clock_failed: false,
            fenced: false,
            retained_root: None,
            quiescence: ProcessQuiescence::Proven,
            quiescence_failure: None,
            workspace_disposition: WorkspaceDisposition::Retain(RetentionReason::Failed),
        })
        .unwrap();

    assert_eq!(manager.pending_observations(&BTreeSet::new(), 100), vec![]);
    assert!(manager.slot.is_none());
}

#[tokio::test]
async fn expiry_release_revokes_running_authority_immediately() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let job = execution_job(&mut manager, &offered);

    manager
        .handle_release(release_for(&offered, "br", "execution_lease_expired"))
        .unwrap();

    assert!(job.authority_updates.borrow().revoked);
    assert!(matches!(
        &manager.slot,
        Some(LocalSlot::Running(running))
            if running.cancellation.cancellation_reason()
                == Some(CancellationReason::ExecutionLeaseExpired)
    ));
}

#[tokio::test]
async fn delayed_execution_job_does_not_start_at_cancellation_boundary() {
    let placeholder =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (temporary, mut manager) = manager_fixture(placeholder);
    let marker = temporary.path().join("launched");
    fs::write(
            temporary.path().join("source/workflow.yaml"),
            format!(
                "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"sh\", \"-c\", \"touch {}\"]\n",
                marker.display()
            ),
        )
        .unwrap();
    let source = temporary.path().join("source");
    run_fixture_git(&source, &["add", "workflow.yaml"]);
    run_fixture_git(&source, &["commit", "--quiet", "-m", "update workflow"]);
    let offered = offer("bg");
    let (control, _lease_waits, job) = controlled_execution_job(&mut manager, &offered).await;
    let acceptance = manager.pending_observations(&BTreeSet::new(), 1)[0].id;
    manager.acknowledge_observation(acceptance);

    control.advance(Duration::from_secs(359));
    job.spawn();

    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        let pending = manager.pending_observations(&BTreeSet::new(), 100);
        assert!(
            pending
                .iter()
                .all(|entry| !matches!(entry.observation, AssignmentObservation::Execution { .. })),
            "a delayed execution job must not publish assignment observations"
        );
        manager.slot.is_none()
    }))
    .await
    .expect("delayed execution job did not relinquish its assignment");
    assert!(
        !marker.exists(),
        "a delayed execution job launched its command"
    );
}

#[tokio::test]
async fn schedules_renewal_before_initial_lease_loss_cancellation() {
    let (_temporary, _manager, mut sleep_requests, _offered, _workspace) =
        controlled_running_fixture().await;

    let (duration, _release) = with_watchdog(sleep_requests.recv())
        .await
        .expect("runner did not schedule a lease timer")
        .expect("lease timer channel closed");

    // Cancellation starts after 359 seconds; target 30 seconds of renewal headroom.
    assert!(
        duration <= Duration::from_secs(329),
        "first lease timer was scheduled at {duration:?}"
    );
}

#[tokio::test]
async fn lease_loss_quiescence_attempts_artifact_delivery_before_terminal_report() {
    let (_temporary, mut manager, mut sleep_requests, _offered, workspace) =
        controlled_running_fixture().await;

    lease_wait_request(&mut sleep_requests, Duration::from_secs(329))
        .await
        .release();
    lease_wait_request(&mut sleep_requests, Duration::from_secs(30))
        .await
        .release();

    let notification = manager.notification();
    let report = with_watchdog(async {
        loop {
            let notified = notification.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let pending = manager.pending_observations(&BTreeSet::new(), 100);
            if let Some(artifact) = pending
                .iter()
                .find(|entry| entry.artifact_request().is_some())
            {
                manager
                    .handle_artifact_response(
                        artifact.id,
                        artifact.artifact_request().unwrap().0,
                        ArtifactCloudResponse::ResultRegistration(
                            ArtifactResultRegistrationResponse {
                                request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                                outcome: ArtifactResultRegistrationOutcome::Failed {
                                    code: "storage_quota_exceeded".to_owned(),
                                },
                            },
                        ),
                    )
                    .expect("lease-loss delivery registration must remain authoritative");
            }
            if let Some(report) = pending
                .into_iter()
                .find_map(|entry| match entry.observation {
                    AssignmentObservation::Execution { report, .. } if report.is_terminal() => {
                        Some(report)
                    }
                    _ => None,
                })
            {
                break report;
            }
            assert!(
                manager.slot.is_some(),
                "orderly lease-loss quiescence dropped the terminal artifact delivery"
            );
            notified.await;
        }
    })
    .await
    .expect("lease-loss delivery did not close");

    assert!(
        matches!(
            &report,
            ExecutionReport::Interrupted {
                reason,
                artifact_delivery,
                ..
            } if reason == "execution_lease_expired"
                && artifact_delivery == &json!({
                    "outcome": "failed",
                    "phase": "registration",
                    "code": "storage_quota_exceeded",
                })
        ),
        "unexpected lease-loss report: {report:?}"
    );
    assert_eq!(
        fs::read(workspace.join("lease-loss-sentinel")).unwrap(),
        b"retained lease bytes"
    );
}

#[tokio::test]
async fn renewal_at_cancellation_boundary_cannot_restore_authority() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let (lease_clock, control, _lease_waits) = controlled_lease_clock();
    manager.lease_clock = lease_clock;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let _job = execution_job(&mut manager, &offered);

    control.advance(Duration::from_secs(359));
    let decision = manager.handle_renewal(renewal_for(&offered)).unwrap();
    assert_eq!(
        decision.disposition,
        RenewalDisposition::CancellationStarted
    );
    assert_eq!(decision.cancellation_headroom_ms, Some(0));
    control.advance(Duration::from_secs(2));
    let late = manager.handle_renewal(renewal_for(&offered)).unwrap();
    assert_eq!(late.disposition, RenewalDisposition::CancellationStarted);
    assert_eq!(late.cancellation_headroom_ms, Some(-2000));

    let running = match &manager.slot {
        Some(LocalSlot::Running(running)) => running,
        _ => panic!("fixture assignment must remain represented while fencing"),
    };
    assert_eq!(
        running.current_grant.sequence, 1,
        "a renewal must not revive authority at the retained monotonic stop boundary"
    );
    assert_eq!(
        running.cancellation.cancellation_reason(),
        Some(CancellationReason::ExecutionLeaseExpired)
    );
    assert!(running.authority_updates.borrow().revoked);
}

#[tokio::test]
async fn exact_next_renewal_replaces_the_running_authority() {
    let (_temporary, mut manager, mut sleep_requests, offered, _workspace) =
        controlled_running_fixture().await;

    let (duration, release) = with_watchdog(sleep_requests.recv()).await.unwrap().unwrap();
    assert_eq!(duration, Duration::from_secs(329));
    release.release();
    let requested = wait_for_renewal_request(&mut manager).await;
    encode_runner_frame(&requested.observation.runner_frame(RunnerEnvelope {
        message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        sequence: 1,
        sent_at: NOW.to_owned(),
    }))
    .expect("renewal request satisfies the runner protocol");

    let renewal = renewal_for(&offered);
    manager.handle_renewal(renewal.clone()).unwrap();
    manager.handle_renewal(renewal).unwrap();
    let (duration, _release) = with_watchdog(async {
        loop {
            let request = sleep_requests.recv().await?;
            if request.0 != Duration::from_secs(30) {
                break Some(request);
            }
        }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(duration, Duration::from_secs(329));

    let mut gap = renewal_for(&offered);
    gap.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned();
    gap.lease.sequence = 4;
    assert_eq!(
        manager.handle_renewal(gap),
        Err(AssignmentManagerFailure::ConflictingOffer)
    );
}

#[tokio::test]
async fn executes_explicit_command_dag_and_reports_dense_transitions() {
    let fixture_argv = serde_json::to_string(&command_fixture_arguments()).unwrap();
    let workflow = format!(
        "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: {fixture_argv}\n  consume:\n    kind: cmd\n    dependsOn: [produce]\n    command:\n      argv: {fixture_argv}\n"
    );
    let reports = execute_fixture_workflow(&workflow, None, 2).await;
    assert!(
        matches!(reports.first(), Some(ExecutionReport::Started)),
        "unexpected reports: {reports:#?}"
    );
    assert_succeeded(&reports);
    let sequences: Vec<_> = reports
        .iter()
        .filter_map(|report| match report {
            ExecutionReport::Transition {
                execution_event_sequence,
                ..
            } => Some(*execution_event_sequence),
            _ => None,
        })
        .collect();
    assert_eq!(sequences, (1..=sequences.len() as u64).collect::<Vec<_>>());
    assert_eq!(
        reports
            .iter()
            .filter(|report| matches!(
                report,
                ExecutionReport::Transition { workflow_event, .. }
                    if workflow_event["eventType"] == "workflow_state_changed"
                        && workflow_event["to"]["state"] == "succeeded"
            ))
            .count(),
        1
    );
}

#[test]
fn invalid_source_and_input_broker_endpoints_fail_construction() {
    let invalid = url::Url::parse("https://localhost/v1/runner/connect").unwrap();
    let valid = url::Url::parse("wss://localhost/v1/runner/connect").unwrap();
    let credential = crate::credential::test_credential();
    let (recorder, _capture) = crate::telemetry::test_recorder("broker-startup");
    let policy = RepositoryUrlPolicy::production();
    let error = AssignmentDependencies::production_brokers(
        &invalid,
        &credential,
        "boot",
        policy,
        recorder.clone(),
        None,
    )
    .err()
    .expect("invalid source endpoint must fail startup");
    assert!(matches!(
        error,
        super::super::ServiceError::SourceBrokerConfiguration(_)
    ));
    assert!(!error.requires_operator_recovery());

    let source: Arc<dyn SourceCredentialBroker> =
        Arc::new(HttpSourceCredentialBroker::new(&valid, &credential, "boot", policy).unwrap());
    let error = AssignmentDependencies::production_brokers(
        &invalid,
        &credential,
        "boot",
        policy,
        recorder,
        Some(source),
    )
    .err()
    .expect("invalid input endpoint must fail startup");
    assert!(matches!(
        error,
        super::super::ServiceError::InputBrokerConfiguration(_)
    ));
    assert!(!error.requires_operator_recovery());
}

async fn assert_normal_success_and_reuse(
    manager: &mut AssignmentManager,
    reports: &[ExecutionReport],
) {
    assert!(
        matches!(reports.last(), Some(ExecutionReport::Finished { outcome, .. })
            if outcome["outcome"] == "succeeded" && outcome.get("quiescenceFailure").is_none())
    );
    acknowledge_terminal_and_settle(manager).await;
    assert!(!manager.cleanup_failed);
    let successor = offer("bh");
    manager
        .handle_offer(successor.clone())
        .expect("runner accepts successor");
    wait_for_offer_preparation(manager).await;
}

// A real unkillable same-user process cannot be safely left running by a test.
// Exercise the post-engine boundary with an injected identity inspector/signal
// adapter; the Codex fixture above covers actual child-group teardown.
async fn finish_with_stubborn_guard(
    kill_succeeds: bool,
    exits_on_last_wait: bool,
) -> (
    tempfile::TempDir,
    AssignmentManager,
    Arc<crate::service::execution::FixtureGuardProcessControl>,
    Vec<ExecutionReport>,
) {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (temporary, mut manager) = manager_fixture(workflow);
    manager.guard_processes = true;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let job = execution_job(&mut manager, &offered);
    let control = job.register_stubborn_fixture(kill_succeeds);
    let (clock, _clock_control, mut waits) = crate::service::lease_clock::controlled_lease_clock();
    job.finish_success_fixture(clock);
    with_watchdog(async {
        for index in 0..if kill_succeeds { 0 } else { 80 } {
            let (_, timer) = waits.recv().await.expect("containment wait was armed");
            if exits_on_last_wait && index == 79 {
                control.mark_absent();
            }
            timer.release();
        }
    })
    .await
    .expect("containment did not perform bounded rechecks");
    let reports = with_watchdog(wait_for_terminal(&mut manager))
        .await
        .expect("guard did not produce a terminal report");
    (temporary, manager, control, reports)
}

#[tokio::test]
async fn post_success_guard_containment_releases_workspace_and_slot() {
    let (_temporary, mut manager, control, reports) = finish_with_stubborn_guard(true, false).await;
    assert_eq!(control.kill_count.load(Ordering::Acquire), 1);
    assert_normal_success_and_reuse(&mut manager, &reports).await;
}

#[tokio::test]
async fn last_containment_wait_observes_departed_guard_before_fencing() {
    let (_temporary, mut manager, control, reports) = finish_with_stubborn_guard(false, true).await;
    assert_eq!(control.kill_count.load(Ordering::Acquire), 1);
    assert_normal_success_and_reuse(&mut manager, &reports).await;
}

#[tokio::test]
async fn uncontainable_guard_is_named_in_terminal_report_and_fences_offers() {
    let (_temporary, mut manager, control, reports) =
        finish_with_stubborn_guard(false, false).await;
    assert!(
        matches!(reports.last(), Some(ExecutionReport::Finished { outcome, .. })
            if outcome["quiescenceFailure"]["reason"] == "process_quiescence_failed"
                && outcome["quiescenceFailure"]["survivingGuards"].as_array()
                    .is_some_and(|guards| guards.iter().any(|guard| guard.as_str().is_some_and(|id| id.contains("stubborn-fixture")))))
    );
    wait_for_execution_finalization(&mut manager).await;
    assert_eq!(control.kill_count.load(Ordering::Acquire), 1);
    assert!(manager.cleanup_failed);
    let detail = manager.quiescence_failure();
    let (recorder, _capture) = crate::telemetry::test_recorder("quiescence-exit");
    let exit = crate::service::workspace_cleanup_failure(&recorder, detail);
    assert!(
        matches!(exit, crate::service::ServiceError::WorkspaceCleanupFailed(Some(ref guards))
            if guards.iter().any(|guard| guard.contains("stubborn-fixture")))
    );
    assert!(exit.requires_operator_recovery());
}

#[tokio::test]
async fn quiescence_failure_waits_for_terminal_ack_or_delivery_deadline() {
    for acknowledge in [true, false] {
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        let (clock, _control, mut waits) = controlled_lease_clock();
        manager.lease_clock = clock;
        let offered = offer("bg");
        offer_then_prepare(&mut manager, &offered).await;
        let acceptance = manager.pending_observations(&BTreeSet::new(), 1)[0].id;
        manager.acknowledge_observation(acceptance);
        let job = execution_job(&mut manager, &offered);
        drop(job);
        let identity = match &manager.slot {
            Some(LocalSlot::Running(running)) => running.identity.clone(),
            _ => panic!("fixture assignment must be running"),
        };
        let id = enqueue_finished(&manager, &identity);
        let deadline = manager
            .lease_clock
            .now()
            .unwrap()
            .checked_add(Duration::from_secs(5))
            .unwrap();
        manager
            .event_sender
            .send(ManagerEvent::Finished {
                assignment_id: identity.assignment_id.clone(),
                final_observation_id: Some(id),
                final_delivery_deadline: Some(deadline),
                lease_clock_failed: false,
                fenced: false,
                retained_root: None,
                quiescence: ProcessQuiescence::Failed,
                quiescence_failure: Some(vec!["guard-fixture".to_owned()]),
                workspace_disposition: WorkspaceDisposition::Retain(RetentionReason::Failed),
            })
            .unwrap();
        manager.pending_observations(&BTreeSet::new(), 100);
        assert!(manager.cleanup_failed);
        manager.mark_observation_encoded(id);
        assert!(!manager.cleanup_failure_ready_to_exit());
        // A send on a lost transport must remain replayable until Cloud acks it.
        manager.finish_transport();
        assert!(
            manager
                .pending_observations(&BTreeSet::new(), 100)
                .iter()
                .any(|entry| entry.id == id)
        );
        assert!(!manager.cleanup_failure_ready_to_exit());
        let timer = with_watchdog(waits.recv())
            .await
            .expect("terminal delivery timer was not armed")
            .expect("controlled lease clock closed")
            .1;
        if acknowledge {
            manager.mark_observation_encoded(id);
            manager.acknowledge_observation(id);
            assert!(manager.cleanup_failure_ready_to_exit());
        } else {
            timer.release();
            with_watchdog(wait_for_manager_state(&mut manager, |manager| {
                manager.cleanup_failure_ready_to_exit()
            }))
            .await
            .expect("terminal deadline did not release the fenced boot");
            assert!(!manager.outbox.contains(id));
        }
    }
}

#[tokio::test]
async fn runner_executes_recovery_and_reports_exact_summary_and_invocations() {
    let failing_argv = serde_json::to_string(&failing_command_fixture_arguments()).unwrap();
    let workflow = format!(
        "schemaVersion: 1\nsteps:\n  verify:\n    kind: cmd\n    recovery:\n      retries: 1\n    command:\n      argv: {failing_argv}\n"
    );
    let reports = execute_fixture_workflow(&workflow, None, 2).await;

    let evidence = reports
        .iter()
        .filter_map(|report| match report {
            ExecutionReport::Transition { workflow_event, .. } => {
                workflow_event.get("invocationEvidence")
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(evidence.len(), 2);
    assert!(evidence.iter().all(|value| value["role"] == "target"));
    assert!(reports.iter().any(|report| matches!(
        report,
        ExecutionReport::Transition { workflow_event, .. }
            if workflow_event["recoveryProgress"]["targetExecution"] == 2
    )));
    assert!(matches!(
        reports.last(),
        Some(ExecutionReport::Finished { outcome, .. })
            if outcome["outcome"] == "failed"
                && outcome["recoverySummaries"]["verify"]["schemaVersion"] == 1
                && outcome["recoverySummaries"]["verify"]["termination"]["kind"] == "exhausted"
                && outcome["recoverySummaries"]["verify"]["rounds"].as_array().map(Vec::len) == Some(1)
    ));
}

#[tokio::test]
async fn failed_command_with_agent_finalizer_retains_the_workflow_failure() {
    let failing_argv = serde_json::to_string(&failing_command_fixture_arguments()).unwrap();
    let workflow = format!(
        "schemaVersion: 1\nagentProfiles:\n  reporter:\n    harness:\n      kind: pi\n      config:\n        model: fixture/pi\n        thinking: medium\nsteps:\n  prepare:\n    kind: cmd\n    command:\n      argv: {failing_argv}\nfinalizers:\n  report:\n    kind: agent\n    when: [failed]\n    failurePolicy: advisory\n    agent:\n      profile: reporter\n      systemPrompt: system.md\n      message:\n        text: [{{file: system.md}}]\n"
    );
    let reports = execute_fixture_workflow(&workflow, Some(SUCCESSFUL_PI), 1).await;

    assert!(reports.iter().any(|report| matches!(
        report,
        ExecutionReport::Transition { workflow_event, .. }
            if workflow_event["stepId"] == "report"
                && workflow_event["role"] == "finalizer"
                && workflow_event["to"] == "succeeded"
                && workflow_event["invocationEvidence"]["role"] == "target"
                && workflow_event["invocationEvidence"]["targetExecution"] == 1
                && workflow_event["invocationEvidence"]["usage"]["inputTokens"] == 1
                && workflow_event["invocationEvidence"]["diagnosticReference"].is_string()
    )));
    assert!(
        matches!(
            reports.last(),
            Some(ExecutionReport::Finished { outcome, .. })
                if outcome["outcome"] == "failed"
                    && outcome["primaryIssue"]["node"]["id"] == "prepare"
                    && outcome["finalization"]["finalizers"][0]["state"] == "succeeded"
        ),
        "unexpected reports: {reports:#?}"
    );
}

#[tokio::test]
async fn outputless_finalizers_emit_roles_phase_and_authoritative_summary() {
    let successful_argv = serde_json::to_string(&command_fixture_arguments()).unwrap();
    let failing_argv = serde_json::to_string(&failing_command_fixture_arguments()).unwrap();
    let workflow = format!(
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: {successful_argv}\nfinalizers:\n  cleanup:\n    kind: cmd\n    inputs:\n      context:\n        ref: finalization.context\n    command:\n      argv: {successful_argv}\n  report:\n    kind: cmd\n    failurePolicy: advisory\n    command:\n      argv: {failing_argv}\n"
    );
    let reports = execute_fixture_workflow(&workflow, None, 3).await;

    assert!(reports.iter().any(|report| matches!(
        report,
        ExecutionReport::Transition { workflow_event, .. }
            if workflow_event["eventType"] == "workflow_state_changed"
                && workflow_event["to"]["state"] == "finalizing"
                && workflow_event["to"]["gate"] == "open"
    )));
    assert!(reports.iter().any(|report| matches!(
        report,
        ExecutionReport::Transition { workflow_event, .. }
            if workflow_event["eventType"] == "step_state_changed"
                && workflow_event["stepId"] == "cleanup"
                && workflow_event["role"] == "finalizer"
    )));
    assert!(matches!(
        reports.last(),
        Some(ExecutionReport::Finished { outcome, .. })
            if outcome["outcome"] == "succeeded"
                && outcome["finalization"]["trigger"] == "succeeded"
                && outcome["finalization"]["finalizers"].as_array().map(Vec::len) == Some(2)
                && outcome["finalization"]["issues"][0]["node"]["id"] == "report"
                && outcome["finalization"]["issues"][0]["impact"] == "advisory"
    ));
}

#[tokio::test]
async fn oversized_workflow_is_rejected_before_semantic_acceptance() {
    let mut workflow = String::from("schemaVersion: 1\nsteps:\n");
    for index in 0..257 {
        workflow.push_str(&format!(
            "  step{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
        ));
    }
    let (_temporary, mut manager) = manager_fixture(&workflow);
    assert_preparation_declined(
        &mut manager,
        offer("bg"),
        AssignmentDecline::ExecutionSpecInvalid(ExecutionSpecInvalidReason::WorkflowSourceInvalid)
            .diagnosed("source_materialization", "workflow_source_unavailable"),
    )
    .await;
}

#[tokio::test]
async fn runner_reservation_consumes_carried_capacity_without_node_arithmetic() {
    let frame_fixture: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/maximal-recovery-frame-size.json"
    )))
    .unwrap();
    assert_eq!(
        frame_fixture["runnerTerminalCapacityBytes"].as_u64(),
        Some(RUNNER_TERMINAL_FRAME_BYTES)
    );

    let outbox = ObservationOutbox::new();
    for _ in 0..32 {
        outbox
            .enqueue(AssignmentObservation::Execution {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                report: ExecutionReport::Started,
            })
            .unwrap();
    }
    let ordinary_maximum = 1_027;
    let finalizer_maximum = 1_030;
    assert_eq!(
        outbox.reserve(ordinary_maximum, 352_845_824),
        Ok(ordinary_maximum)
    );
    assert_eq!(
        outbox.reserve(finalizer_maximum, MAXIMUM_ENCODED_OUTBOX_BYTES),
        Ok(finalizer_maximum)
    );
    assert!(outbox.lock().entries.capacity() >= finalizer_maximum + OBSERVATION_RESERVE_BASE);
    let mut below_required = ObservationOutbox::new();
    below_required.maximum_encoded_bytes = MAXIMUM_ENCODED_OUTBOX_BYTES - 1;
    assert_eq!(
        below_required.reserve(finalizer_maximum, MAXIMUM_ENCODED_OUTBOX_BYTES),
        Err(environment_unavailable())
    );
    assert_eq!(outbox.lock().entries.len(), 32);
}

#[test]
fn oversized_observation_is_rejected_before_queueing() {
    let outbox = ObservationOutbox::new();
    // This limit fixture deliberately keeps a complete schema-valid transition; sharing
    // the failure-mapper test's richer construction would obscure the size boundary.
    assert_eq!(
        outbox.enqueue(AssignmentObservation::Execution {
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            report: ExecutionReport::Transition {
                diagnostic: None,
                execution_event_sequence: 1,
                workflow_event: json!({
                    "eventVersion": 1,
                    "eventType": "step_state_changed",
                    "transitionSequence": 1,
                    "stepId": "x".repeat(MAXIMUM_ORDINARY_FRAME_BYTES),
                    "failurePolicy": "required",
                    "from": "pending",
                    "to": "starting",
                }),
            },
        }),
        Err(OutboxFailure::Encoding)
    );
    assert_eq!(outbox.lock().entries.len(), 0);
}

#[test]
fn outbox_accepts_large_condition_evidence_transition() {
    let outbox = ObservationOutbox::new();
    let pointer = format!("/{}", "a".repeat(MAXIMUM_ORDINARY_FRAME_BYTES));
    let observation = AssignmentObservation::Execution {
        assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        report: ExecutionReport::Transition {
            diagnostic: None,
            execution_event_sequence: 1,
            workflow_event: json!({
                "eventVersion": 1,
                "eventType": "step_state_changed",
                "transitionSequence": 1,
                "stepId": "build",
                "role": "step",
                "failurePolicy": "required",
                "from": "pending",
                "to": "failed",
                "detail": {
                    "phase": "condition",
                    "code": "json_pointer_missing",
                    "ref": "outputs.plan.result",
                    "pointer": pointer,
                },
            }),
        },
    };

    assert!(outbox.enqueue(observation).is_ok());
    assert_eq!(outbox.lock().entries.len(), 1);
}

#[test]
fn outbox_retains_large_condition_failure_through_finalization() {
    let mut outbox = ObservationOutbox::new();
    // One conditional step and one finalizer: 14 transitions, 64 reserved
    // observations, a 4 MiB aggregate condition bound, and an 8 MiB terminal bound.
    // Use the carried byte reservation rather than the larger service-wide cap.
    outbox.maximum_encoded_bytes = (14 + 64 - 1 - 1) * 262_144 + 4_194_304 + 8_388_608;
    outbox.reserve(14, outbox.maximum_encoded_bytes).unwrap();
    // YAML's two-byte escape can expand to six JSON bytes. This pointer alone
    // consumes the entire 1 MiB workflow-document allowance; real definitions
    // necessarily leave room for their structure.
    let pointer = format!("/{}", "\u{0007}".repeat(524_288));
    let detail = json!({
        "phase": "condition", "code": "json_pointer_missing",
        "ref": "outputs.plan.result", "pointer": pointer,
    });
    let issue = json!({
        "node": {"id": "build", "role": "step"},
        "state": "failed", "detail": detail,
    });
    outbox
        .enqueue(AssignmentObservation::Execution {
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            report: ExecutionReport::Transition {
                diagnostic: None,
                execution_event_sequence: 1,
                workflow_event: json!({
                    "eventVersion": 1, "eventType": "step_state_changed",
                    "transitionSequence": 1, "stepId": "build", "role": "step",
                    "failurePolicy": "required", "from": "pending", "to": "failed",
                    "detail": detail,
                }),
            },
        })
        .unwrap();
    let cases: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/condition-workflow-transitions.json"
    )))
    .unwrap();
    for case in cases.as_array().unwrap() {
        if !matches!(
            case["name"].as_str(),
            Some("failure_stopped" | "finalizing" | "failed")
        ) {
            continue;
        }
        let event = serde_json::to_string(&case["workflowEvent"])
            .unwrap()
            .replace("\"/missing\"", &serde_json::to_string(&pointer).unwrap());
        outbox
            .enqueue(AssignmentObservation::Execution {
                assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                report: ExecutionReport::Transition {
                    diagnostic: None,
                    execution_event_sequence: 4,
                    workflow_event: serde_json::from_str(&event).unwrap(),
                },
            })
            .unwrap();
    }
    outbox.enqueue(AssignmentObservation::Execution {
        assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        report: ExecutionReport::Finished {
            diagnostic: None,
            final_execution_event_sequence: 5,
            outcome: json!({"outcome": "failed", "primaryIssue": issue, "forceAbort": null}),
            artifact_delivery: json!({"outcome": "prepared", "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc"}),
        },
    }).unwrap();
    let pending = outbox.pending(&BTreeSet::new(), 10);
    assert_eq!(pending.len(), 5);
    for observation in pending {
        let id = observation.id;
        assert!(outbox.acknowledge(id).is_some());
    }
    assert!(outbox.pending(&BTreeSet::new(), 10).is_empty());
}

fn assert_only_one_pending_terminal(observation: AssignmentObservation) {
    let outbox = ObservationOutbox::new();
    assert!(outbox.enqueue(observation.clone()).is_ok());
    assert_eq!(outbox.enqueue(observation), Err(OutboxFailure::Capacity));
}

#[test]
fn outbox_accepts_only_one_terminal_per_assignment() {
    let escaped = "\u{0001}".repeat(4_096);
    let rounds = (1..=2)
        .map(|number| {
            json!({
                "number": number,
                "failedExecution": {
                    "executionNumber": number,
                    "invocationId": number,
                    "failure": {
                        "phase": "execution",
                        "cause": { "code": "command_exit", "exitCode": 1 }
                    }
                },
                "handler": {
                    "kind": "cmd",
                    "invocationId": number + 2,
                    "outcome": "recheck",
                    "summary": escaped,
                    "reason": escaped,
                }
            })
        })
        .collect::<Vec<_>>();
    let large_report = ExecutionReport::Finished {
        diagnostic: None,
        final_execution_event_sequence: 1,
        outcome: json!({
            "outcome": "failed",
            "forceAbort": null,
            "primaryIssue": {
                "node": { "id": "verify", "role": "step" },
                "state": "failed",
                "detail": {
                    "phase": "execution",
                    "code": "command_exit",
                    "exitCode": 1,
                }
            },
            "recoverySummaries": {
                "verify": {
                    "schemaVersion": 1,
                    "configuredRetries": 2,
                    "handlerKind": "cmd",
                    "rounds": rounds,
                    "termination": { "kind": "exhausted", "executionNumber": 3 },
                }
            }
        }),
        artifact_delivery: json!({
            "outcome": "prepared",
            "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
        }),
    };
    let small_report = ExecutionReport::Finished {
        diagnostic: None,
        final_execution_event_sequence: 1,
        outcome: json!({"outcome": "succeeded", "forceAbort": null}),
        artifact_delivery: json!({
            "outcome": "prepared",
            "artifactSetId": "ats_01k0z6r1w8f4jy2m7q9v3x5abc",
        }),
    };

    for report in [large_report, small_report] {
        assert_only_one_pending_terminal(AssignmentObservation::Execution {
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            report,
        });
    }
}

#[tokio::test]
async fn successor_fences_leave_room_for_more_than_256_completed_assignments() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let alphabet = b"0123456789abcdefghjkmnpqrstvwxyz";
    let mut predecessor: Option<AssignmentIdentity> = None;

    // Build the completed-assignment retention state directly. Materializing the same
    // repository 256 times does not exercise successor fencing and makes this bounded
    // state-machine check depend on filesystem throughput.
    for index in 0..MAXIMUM_RETAINED_DECISIONS {
        let suffix = format!(
            "{}{}",
            char::from(alphabet[index / alphabet.len()]),
            char::from(alphabet[index % alphabet.len()])
        );
        if let Some(identity) = &predecessor {
            manager.retire_assignment_observations(&identity.assignment_id);
        }
        let offered = offer(&suffix);
        predecessor = Some(AssignmentIdentity::from_offer(&offered));
        let response = AssignmentDecision::Accepted {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5acz".to_owned(),
            assignment_id: offered.assignment_id.clone(),
            offered_execution_spec_id: offered.execution_spec.execution_spec_id.clone(),
        };
        manager.retain_decision(offered, response).unwrap();
        let acceptance_id = manager.pending_observations(&BTreeSet::new(), 1)[0].id;
        manager.mark_observation_encoded(acceptance_id);
        manager.finish_transport();
    }

    let predecessor = predecessor.unwrap();
    let final_observation_id = enqueue_finished(&manager, &predecessor);
    manager.mark_observation_encoded(final_observation_id);
    manager.finish_transport();
    manager.slot = Some(LocalSlot::Finishing(Box::new(FinishingAssignment {
        identity: predecessor,
        final_observation_id,
        root: None,
        workspace_disposition: WorkspaceDisposition::Remove,
    })));

    let successor = offer("80");
    assert_eq!(manager.handle_offer(successor), Ok(()));
    wait_for_offer_preparation(&mut manager).await;
}

#[tokio::test]
async fn transport_finish_drains_worker_events() {
    let (_temporary, mut manager) = manager_fixture("schemaVersion: 1\nsteps: {}\n");
    manager
        .event_sender
        .send(ManagerEvent::LeaseClockFailed {
            assignment_id: "unused".to_owned(),
            error: LeaseClockError::TimerWaitFailed,
        })
        .unwrap();
    manager.finish_transport();
    assert!(manager.lease_clock_failed);
}

#[tokio::test]
async fn deferred_offer_outbox_exhaustion_is_reported_in_both_cleanup_outcomes() {
    for result in [CleanupResult::Released, CleanupResult::Preempted] {
        let (_temporary, mut manager) = manager_fixture("schemaVersion: 1\nsteps: {}\n");
        let predecessor = offer("bg");
        let successor = offer("bh");
        manager.slot = Some(LocalSlot::Releasing(ReleasingAssignment {
            assignment_id: predecessor.assignment_id.clone(),
            after: ReleaseAfter::Idle,
            retention_report: None,
        }));
        manager.deferred_successor = Some(successor);
        // Both replay paths must enqueue a rejection even during shutdown.
        manager.shutting_down = true;
        manager.outbox.maximum_encoded_bytes = 0;
        manager
            .event_sender
            .send(ManagerEvent::CleanupFinished {
                assignment_id: predecessor.assignment_id,
                result,
            })
            .unwrap();
        assert_eq!(
            manager.take_deferred_offer_failure(),
            Some(AssignmentManagerFailure::DecisionCapacity)
        );
        assert!(manager.deferred_successor.is_none());
        assert!(manager.pending_observations(&BTreeSet::new(), 1).is_empty());
        assert_eq!(manager.take_deferred_offer_failure(), None);
    }
}

#[tokio::test]
async fn conflicting_deferred_successor_is_reported_after_cleanup() {
    let (_temporary, mut manager) = manager_fixture("schemaVersion: 1\nsteps: {}\n");
    let predecessor = offer("bg");
    let successor = offer("bh");
    let mut conflict = successor.clone();
    conflict.attempt_number += 1;
    manager
        .retain_decision(
            conflict.clone(),
            rejected(&conflict, AssignmentDecline::CapacityUnavailable),
        )
        .unwrap();
    manager.slot = Some(LocalSlot::Releasing(ReleasingAssignment {
        assignment_id: predecessor.assignment_id.clone(),
        after: ReleaseAfter::Idle,
        retention_report: None,
    }));
    manager.deferred_successor = Some(successor);
    manager
        .event_sender
        .send(ManagerEvent::CleanupFinished {
            assignment_id: predecessor.assignment_id,
            result: CleanupResult::Released,
        })
        .unwrap();
    assert_eq!(
        manager.take_deferred_offer_failure(),
        Some(AssignmentManagerFailure::ConflictingOffer)
    );
}

#[tokio::test]
async fn final_grace_acknowledgement_and_successor_fence_cleanup_state() {
    let workflow =
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
    let (_temporary, mut manager) = manager_fixture(workflow);
    let predecessor = offer("bg");
    offer_then_prepare(&mut manager, &predecessor).await;
    let identity = match manager.slot.take().unwrap() {
        LocalSlot::Accepted(accepted) => accepted.identity.clone(),
        _ => panic!("offer must be accepted"),
    };
    let send_grace = |manager: &AssignmentManager, final_observation_id| {
        manager
            .event_sender
            .send(ManagerEvent::FinalGraceElapsed {
                assignment_id: identity.assignment_id.clone(),
                final_observation_id,
                continue_reporting: true,
            })
            .unwrap();
    };
    let final_observation_id = enqueue_finished(&manager, &identity);
    manager.slot = Some(LocalSlot::Finishing(Box::new(FinishingAssignment {
        identity: identity.clone(),
        final_observation_id,
        root: None,
        workspace_disposition: WorkspaceDisposition::Remove,
    })));
    send_grace(&manager, final_observation_id);
    manager.pending_observations(&BTreeSet::new(), 100);
    assert!(manager.slot.is_none());
    assert_eq!(manager.reporting, Some(identity.clone()));
    manager.acknowledge_observation(final_observation_id);
    assert!(manager.reporting.is_none());

    let final_observation_id = enqueue_finished(&manager, &identity);
    manager.mark_observation_encoded(final_observation_id);
    manager.slot = Some(LocalSlot::Finishing(Box::new(FinishingAssignment {
        identity: identity.clone(),
        final_observation_id,
        root: None,
        workspace_disposition: WorkspaceDisposition::Remove,
    })));
    let successor = offer("bh");
    manager.handle_offer(successor.clone()).unwrap();
    assert!(manager.fenced_final_graces.contains(&FencedFinalGrace {
        assignment_id: identity.assignment_id.clone(),
        final_observation_id,
    }));
    wait_for_offer_preparation(&mut manager).await;
    send_grace(&manager, final_observation_id);
    manager.pending_observations(&BTreeSet::new(), 100);
    assert!(manager.fenced_final_graces.is_empty());
    assert!(manager.reporting.is_none());
    assert!(
        matches!(&manager.slot, Some(LocalSlot::Preparing(preparing))
            if preparing.offer.assignment_id == successor.assignment_id)
    );
    assert_eq!(manager.outbox.lock().entries.len(), 2);
    assert_eq!(manager.pending_observations(&BTreeSet::new(), 100).len(), 1);
    manager.finish_transport();
    assert_eq!(manager.outbox.lock().entries.len(), 1);
}

#[expect(
    clippy::disallowed_methods,
    reason = "wall time only bounds explicit root-worker and runtime-shutdown completion signals"
)]
#[test]
fn blocked_root_preparation_does_not_hold_tokio_runtime_shutdown() {
    let (started, mut root_preparation_started) = tokio::sync::mpsc::unbounded_channel();
    let (release_root_preparation, released) = std::sync::mpsc::channel();
    let (begin_shutdown, shutdown_requested) = std::sync::mpsc::sync_channel(1);
    let (finished, runtime_finished) = std::sync::mpsc::sync_channel(1);
    let service = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build fixture Tokio runtime");
        let guard = runtime.enter();
        let workflow = "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n";
        let (_temporary, mut manager) = manager_fixture(workflow);
        manager.root_preparer = Arc::new(GatedAssignmentRootPreparer {
            started,
            release: Mutex::new(released),
            outcome: GatedRootPreparationOutcome::Unavailable,
        });
        manager.handle_offer(offer("bg")).unwrap();
        shutdown_requested
            .recv()
            .expect("runtime shutdown request missing");
        manager.begin_shutdown().unwrap();
        assert!(
            manager.shutdown_complete(),
            "blocked root preparation must detach from graceful shutdown"
        );
        drop(manager);
        drop(guard);
        drop(runtime);
        let _ = finished.send(());
    });

    root_preparation_started
        .blocking_recv()
        .expect("assignment root preparation did not start");
    begin_shutdown.send(()).expect("begin runtime shutdown");
    runtime_finished
        .recv_timeout(Duration::from_secs(1))
        .expect("Tokio runtime waited for blocked assignment root preparation");
    release_root_preparation
        .send(())
        .expect("release assignment root preparation");
    service.join().expect("runtime fixture thread panicked");
}
