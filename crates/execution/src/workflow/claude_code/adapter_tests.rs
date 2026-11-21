use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::future::{Future, pending};
use std::io::{BufRead as _, Read as _, Write as _};
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use rustix::process::{Pid, getpgid};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use useful_test_support::mcp::McpServerProcess;
use useful_test_support::watchdog::{FIXTURE_WAIT_BASE, host_scaled_recv};

use super::adapter::{
    ClaudeCodeStreamJsonV1Adapter, MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES,
    NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC, QUIET_BOUND, QUIET_BOUND_FORCE_DELAY,
    native_project_slug, prepare_launch,
};
use super::result_tool::TOOL_NAME;
use super::test_support::{
    FixtureRelease, FixtureSignal, PendingClock, RecordingObservationSink, admitted_adapter,
    invocation_identity, native_resource_watchdog,
};
use super::*;
use crate::workflow::admission::{CancellationReason, CancellationSource, EnvironmentSnapshot};
use crate::workflow::agent::{
    AgentAdapter, AgentCompatibilityProfile, AgentDiagnosticLevel, AgentFailureCause,
    AgentHarnessFailureDetail, AgentInvocation, AgentInvocationLimits, AgentInvocationStaging,
    AgentLifecycleMilestone, AgentObservation, AgentOutcome, AgentProcessContext, AgentPrompt,
    AgentStartReceiver, AgentValueKind, AgentValueMode, CompletedAgentInvocation, PositiveDuration,
    RetainedJsonSchema, StagedAgentAttachment, agent_start_channel, failed_agent_outcome,
};
use crate::workflow::agent_diagnostics::AgentDiagnosticSessionStore;
use crate::workflow::agent_process_driver::PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL;
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::diagnostic::StepDiagnosticLog;
use crate::workflow::execution_root::AdmittedExecutionRoot;
use crate::workflow::observation::NoopExecutionObserver;
use crate::workflow::private_staging::open_directory_path;
use crate::workflow::process_group::{ProcessGuardRegistry, process_group_is_quiescent};
use crate::workflow::result_validation::{ResultValidationWorker, ValidationWorkerRequest};
pub(super) use crate::workflow::test_support::InlineValidationWorker;
use crate::workflow::test_support::claude_validation::{
    FailingValidationWorker, PendingValidation, PendingValidationWorker,
};
use crate::workflow::test_support::{
    fixture_progress_watchdog, process_fixture_interrupt_receiver, record_fixture_progress,
    record_invocation_end, spawn_process_fixture, write_process_fixture_id,
    write_process_fixture_signal,
};

const MODEL: &str = "scherzo-loopback";
const RESPONSE: &str = "driver response";
const FAKE_CLAUDE: &str = r#"#!/bin/sh
set -eu
for argument in "$@"; do
  printf '%s\0' "$argument"
done > "$CLAUDE_FIXTURE_ARGUMENTS"
head -n 1 > "$CLAUDE_FIXTURE_INPUT"
prompt=
model=
session=
previous=
for argument in "$@"; do
  case "$previous" in
    --append-system-prompt-file) prompt=$argument ;;
    --model) model=$argument ;;
    --session-id) session=$argument ;;
  esac
  previous=$argument
done
native_project=$CLAUDE_FIXTURE_NATIVE_PROJECT
printf '{malformed retained transcript' > "$native_project/$session.jsonl"
mkdir -p "$native_project/$session/subagents"
printf 'retained subagent activity\n' > "$native_project/$session/subagents/agent-fixture.jsonl"
cat "$prompt" > "$CLAUDE_FIXTURE_PROMPT"
printf '%s\n' "$PWD" > "$CLAUDE_FIXTURE_CWD"
{
  printf 'runner=%s\n' "$ONLY_RUNNER_VALUE"
  printf 'config=%s\n' "$CLAUDE_CONFIG_DIR"
  printf 'project_dir_name=%s\n' "${CLAUDE_CODE_PROJECT_DIR_NAME-unset}"
  printf 'updates=%s\n' "$DISABLE_UPDATES"
  printf 'traffic=%s\n' "$CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"
  printf 'marketplace=%s\n' "$CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL"
  printf 'memory=%s\n' "$CLAUDE_CODE_DISABLE_AUTO_MEMORY"
  printf 'git=%s\n' "$CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS"
  printf 'exit_after_stop=%s\n' "$CLAUDE_CODE_EXIT_AFTER_STOP_DELAY"
} > "$CLAUDE_FIXTURE_ENVIRONMENT"
printf 'fixture diagnostic\n' >&2
output=$(
printf '{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.321"}\n' "$PWD" "$session" "$model"
printf '{"type":"system","subtype":"status","status":"requesting","session_id":"%s"}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_start","message":{"id":"msg-driver","type":"message","role":"assistant","content":[],"model":"%s","usage":{"input_tokens":1,"output_tokens":0}}},"session_id":"%s","parent_tool_use_id":null}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"driver response"}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"assistant","message":{"id":"msg-driver","type":"message","role":"assistant","content":[{"type":"text","text":"driver response"}],"model":"%s"},"parent_tool_use_id":null,"session_id":"%s"}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_stop","index":0},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_stop"},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"result","subtype":"success","is_error":false,"terminal_reason":"completed","num_turns":1,"result":"driver response","session_id":"%s"}\n' "$session"
)
printf '%s\n' "$output"
"#;

/// A Claude Code that records its arguments, exports the `--mcp-config` file they name, and
/// hands over to the scripted process fixture, which writes its frames on fd 3.
const SCRIPTED_FAKE_CLAUDE: &str = r#"#!/bin/sh
for argument in "$@"; do
  printf '%s\0' "$argument"
done > "$CLAUDE_FIXTURE_ARGUMENTS"
previous=
for argument in "$@"; do
  if [ "$previous" = --mcp-config ]; then
    CLAUDE_FIXTURE_MCP_CONFIG=$argument
    export CLAUDE_FIXTURE_MCP_CONFIG
  fi
  previous=$argument
done
exec "$CLAUDE_FIXTURE_PROCESS_HELPER" \
  --exact workflow::claude_code::adapter_tests::scripted_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null 2>&1
"#;

const CANCELLATION_FAKE_CLAUDE: &str = r#"#!/bin/sh
exec "$CLAUDE_FIXTURE_PROCESS_HELPER" \
  --exact workflow::claude_code::adapter_tests::cancellation_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null 2>&1
"#;

const STUBBORN_DESCENDANT_FAKE_CLAUDE: &str = r#"#!/bin/sh
exec "$CLAUDE_FIXTURE_PROCESS_HELPER" \
  --exact workflow::claude_code::adapter_tests::stubborn_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null 2>&1
"#;

const STDOUT_BEFORE_INPUT_FAKE_CLAUDE: &str = r#"#!/bin/sh
exec "$CLAUDE_FIXTURE_PROCESS_HELPER" \
  --exact workflow::claude_code::adapter_tests::stdout_before_input_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null 2>&1
"#;

const BLOCKED_INPUT_FAKE_CLAUDE: &str = r#"#!/bin/sh
exec "$CLAUDE_FIXTURE_PROCESS_HELPER" \
  --exact workflow::claude_code::adapter_tests::blocked_input_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null 2>&1
"#;

const DELAYED_INPUT_FAKE_CLAUDE: &str = r#"#!/bin/sh
exec "$CLAUDE_FIXTURE_PROCESS_HELPER" \
  --exact workflow::claude_code::adapter_tests::delayed_input_process_fixture \
  --ignored --test-threads=1 \
  3>&1 >/dev/null 2>&1
"#;

type TestInvocation = AgentInvocation;

struct ProcessFixture {
    _temporary: tempfile::TempDir,
    attachment_directory: PathBuf,
    invocation: Option<TestInvocation>,
    observations: RecordingObservationSink,
    diagnostics: StepDiagnosticLog,
    arguments: PathBuf,
    input: PathBuf,
    prompt: PathBuf,
    cwd: PathBuf,
    environment: PathBuf,
    config: PathBuf,
    native_session: PathBuf,
    session_id: Arc<str>,
    process: PathBuf,
    report: PathBuf,
    result_endpoint: PathBuf,
    ready: FixtureSignal,
    descendant_ready: FixtureSignal,
    interrupted: FixtureSignal,
    /// The case's side of the `release` FIFO beside `ready` (INV-TESTING-29).
    release_channel: FixtureRelease,
    expected_cwd: PathBuf,
    diagnostic_directory: PathBuf,
}

struct AttachmentFixture<'a> {
    media_type: &'a str,
    bytes: &'a [u8],
    diagnostic_source_name: Option<&'a str>,
}

impl ProcessFixture {
    fn new(value_mode: AgentValueMode, maximum_response_bytes: u64) -> Self {
        Self::with_attachments(value_mode, maximum_response_bytes, &[])
    }

    fn with_attachments(
        value_mode: AgentValueMode,
        maximum_response_bytes: u64,
        attachment_fixtures: &[AttachmentFixture<'_>],
    ) -> Self {
        Self::build(
            value_mode,
            maximum_response_bytes,
            attachment_fixtures,
            FAKE_CLAUDE,
            None,
        )
    }

    /// A fake Claude Code that is the scripted process fixture, performing `actions` in order
    /// (see `scripted_process_fixture`).
    fn scripted(value_mode: AgentValueMode, actions: &[Value]) -> Self {
        Self::build(
            value_mode,
            1024,
            &[],
            SCRIPTED_FAKE_CLAUDE,
            Some(Value::Array(actions.to_vec())),
        )
    }

    /// Lets the fixture past its next wait for a release (`await_fixture_release`).
    fn release(&self) {
        self.release_channel.release();
    }

    fn build(
        value_mode: AgentValueMode,
        maximum_response_bytes: u64,
        attachment_fixtures: &[AttachmentFixture<'_>],
        script: &str,
        actions: Option<Value>,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let cwd = execution_root.join("worktree");
        let staging = temporary.path().join("staging");
        let attachment_directory = staging.join("attachments");
        let result_endpoint = staging.join("result-endpoint");
        let controls = temporary.path().join("controls");
        let config = temporary.path().join("runner-claude-config");
        let attempt = temporary.path().join("attempt");
        for directory in [
            &cwd,
            &staging,
            &attachment_directory,
            &result_endpoint,
            &controls,
            &config,
            &attempt,
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }
        // The result tool's socket and configuration live in the private endpoint directory.
        std::fs::set_permissions(&result_endpoint, std::fs::Permissions::from_mode(0o700)).unwrap();
        let executable = temporary.path().join("claude");
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();

        let arguments = controls.join("arguments");
        let input = controls.join("input");
        let prompt = controls.join("prompt");
        let captured_cwd = controls.join("cwd");
        let environment = controls.join("environment");
        let process = controls.join("process");
        let report = controls.join("report.json");
        let ready = FixtureSignal::create(controls.join("ready"));
        let descendant_ready = FixtureSignal::create(controls.join("descendant-ready"));
        let interrupted = FixtureSignal::create(controls.join("interrupted"));
        let release_channel = FixtureRelease::create(&controls.join("release"));
        let mut runner_environment = BTreeMap::from([
            (
                OsString::from("PATH"),
                std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/bin")),
            ),
            (
                OsString::from("ONLY_RUNNER_VALUE"),
                OsString::from("runner exact"),
            ),
            (
                OsString::from("CLAUDE_CONFIG_DIR"),
                config.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_CODE_PROJECT_DIR_NAME"),
                OsString::from("ambient-project-name"),
            ),
            (OsString::from("DISABLE_UPDATES"), OsString::from("0")),
            (
                OsString::from("CLAUDE_FIXTURE_ARGUMENTS"),
                arguments.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_INPUT"),
                input.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_PROMPT"),
                prompt.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_CWD"),
                captured_cwd.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_ENVIRONMENT"),
                environment.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_PROCESS_HELPER"),
                std::env::current_exe().unwrap().into_os_string(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_READY"),
                ready.path().as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_DESCENDANT_READY"),
                descendant_ready.path().as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_INTERRUPTED"),
                interrupted.path().as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_PROCESS"),
                process.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_FIXTURE_REPORT"),
                report.as_os_str().to_owned(),
            ),
        ]);
        if let Some(actions) = actions {
            runner_environment.insert(
                OsString::from("CLAUDE_FIXTURE_ACTIONS"),
                OsString::from(actions.to_string()),
            );
        }
        let attachments = attachment_fixtures
            .iter()
            .enumerate()
            .map(|(index, fixture)| {
                let path = attachment_directory.join(format!("{index:06}"));
                std::fs::write(&path, fixture.bytes).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
                StagedAgentAttachment::new(
                    path,
                    Arc::from(fixture.media_type),
                    fixture.diagnostic_source_name.map(Arc::from),
                )
            })
            .collect::<Vec<_>>();
        std::fs::set_permissions(
            &attachment_directory,
            std::fs::Permissions::from_mode(0o500),
        )
        .unwrap();

        let admitted_root = AdmittedExecutionRoot::admit(&execution_root).unwrap();
        let working_directory = admitted_root
            .select_working_directory(Some("worktree"))
            .unwrap();
        let expected_cwd = working_directory.protocol_path().unwrap();
        // Give the fake upstream writer the exact ambient directory so this fixture does
        // not duplicate Claude's bounded slug algorithm; that algorithm has its own test.
        runner_environment.insert(
            OsString::from("CLAUDE_FIXTURE_NATIVE_PROJECT"),
            config
                .join("projects")
                .join(native_project_slug(expected_cwd.to_str().unwrap()))
                .into_os_string(),
        );
        let observations = RecordingObservationSink::default();
        let diagnostics = StepDiagnosticLog::default();
        let identity = invocation_identity("run-claude-driver", "agent-step");
        let attempt_directory = open_directory_path(&attempt).unwrap();
        let diagnostic_session = AgentDiagnosticSessionStore::create(
            &attempt_directory,
            &attempt,
            Arc::from("00000000-0000-4000-8000-000000000001"),
            1,
        )
        .unwrap()
        .allocate(
            &identity,
            AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
            QUALIFICATION_VERSION,
        )
        .unwrap();
        let native_session = diagnostic_session
            .claude_code_native_session_directory()
            .unwrap()
            .to_owned();
        let session_id =
            Arc::<str>::from(diagnostic_session.claude_code_native_session_id().unwrap());
        runner_environment.insert(
            OsString::from("CLAUDE_FIXTURE_SESSION"),
            OsString::from(session_id.as_ref()),
        );
        let diagnostic_directory = diagnostic_session.directory().to_owned();
        let invocation = AgentInvocation::new(
            identity,
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(
                working_directory,
                EnvironmentSnapshot::new(runner_environment),
            ),
            AgentInvocationStaging::new(result_endpoint.clone()),
            diagnostic_session,
            AgentPrompt::new(
                Arc::from("exact system prompt\n"),
                Arc::from("exact message @literal\nsecond line"),
            ),
            Arc::from(attachments),
            value_mode,
            invocation_limits(maximum_response_bytes),
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        Self {
            _temporary: temporary,
            attachment_directory,
            invocation: Some(invocation),
            observations,
            diagnostics,
            arguments,
            input,
            prompt,
            cwd: captured_cwd,
            environment,
            config,
            native_session,
            session_id,
            process,
            report,
            result_endpoint,
            ready,
            descendant_ready,
            interrupted,
            release_channel,
            expected_cwd,
            diagnostic_directory,
        }
    }
}

impl Drop for ProcessFixture {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(
            &self.attachment_directory,
            std::fs::Permissions::from_mode(0o700),
        );
    }
}

// These small fixture limits intentionally differ from exact-binary conformance limits;
// keeping the complete admitted envelope visible makes boundary tests auditable.
fn invocation_limits(
    maximum_response_bytes: u64,
) -> AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits> {
    AgentInvocationLimits::new(
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroUsize::new(16).unwrap(),
        NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        NonZeroU64::new(maximum_response_bytes).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(512).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
    )
}

async fn run_fixture(fixture: ProcessFixture) -> (ProcessFixture, AgentOutcome) {
    let (fixture, outcome, started) = run_fixture_allowing_start_failure(fixture).await;
    assert!(started, "terminal outcome: {outcome:?}");
    (fixture, outcome)
}

async fn run_fixture_allowing_start_failure(
    mut fixture: ProcessFixture,
) -> (ProcessFixture, AgentOutcome, bool) {
    let invocation = fixture.invocation.take().unwrap();
    let (task, start, outcome) = start_process_fixture(invocation, fixture.diagnostics.clone());
    task.await.unwrap();
    (
        fixture,
        outcome.await.unwrap(),
        start.receive().await.is_ok(),
    )
}

fn start_process_fixture(
    invocation: TestInvocation,
    diagnostics: StepDiagnosticLog,
) -> (
    tokio::task::JoinHandle<()>,
    AgentStartReceiver,
    tokio::sync::oneshot::Receiver<AgentOutcome>,
) {
    start_adapter(
        invocation,
        diagnostics,
        PendingClock,
        InlineValidationWorker,
    )
}

fn assert_user_cancelled(outcome: AgentOutcome) {
    assert_eq!(
        outcome,
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest,
        }
    );
}

fn production_transcript_script(
    events: &[Value],
    malformed_tail: bool,
    exit_status: u8,
    session: &str,
) -> String {
    let mut output = String::new();
    for event in events {
        let event = serde_json::to_string(event).unwrap();
        assert!(!event.contains('\''));
        output.push_str(&format!("printf '%s\\n' '{event}'\n"));
    }
    if malformed_tail {
        output.push_str("printf 'not-json\\n'\n");
    }
    assert!(!session.contains('\''));
    format!(
        r#"#!/bin/sh
set -eu
model=
previous=
for argument in "$@"; do
  if [ "$previous" = --model ]; then model=$argument; fi
  previous=$argument
done
IFS= read -r _initial
session='{session}'
printf '{{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.321"}}\n' "$PWD" "$session" "$model"
{output}exit {exit_status}
"#
    )
}

fn retained_schema(schema: Value) -> RetainedJsonSchema {
    let bytes = Arc::<[u8]>::from(serde_json::to_vec(&schema).unwrap());
    RetainedJsonSchema::compile(bytes, Arc::new(schema)).unwrap()
}

pub(super) fn type_schema(root_type: &str) -> RetainedJsonSchema {
    retained_schema(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": root_type,
    }))
}

pub(super) fn positive_integer_schema() -> RetainedJsonSchema {
    retained_schema(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "integer",
        "minimum": 1,
    }))
}

pub(super) fn result_mode(schema: RetainedJsonSchema) -> AgentValueMode {
    AgentValueMode::Result {
        output: Arc::from("result"),
        schema,
    }
}

/// The deadlines a [`StagedClock`] lets expire.
#[derive(Clone, Default)]
struct ReleasedDeadlines {
    durations: BTreeSet<Duration>,
    /// Every deadline but the quiet bound's two.
    all_but_the_bound: bool,
}

impl ReleasedDeadlines {
    fn admit(&self, deadline: Duration) -> bool {
        self.durations.contains(&deadline)
            || (self.all_but_the_bound
                && deadline != QUIET_BOUND
                && deadline != QUIET_BOUND_FORCE_DELAY)
    }
}

/// A clock at zero whose deadlines expire only as the case releases their durations, and
/// which shows each quiet-bound wait still pending, so a case can tell a bound armed afresh
/// from the one it replaced.
#[derive(Clone)]
pub(super) struct StagedClock {
    registrations: mpsc::UnboundedSender<Duration>,
    released: watch::Receiver<ReleasedDeadlines>,
    pending_bounds: Arc<watch::Sender<BTreeSet<u64>>>,
    armed_bounds: Arc<AtomicU64>,
}

pub(super) struct StagedClockControl {
    registrations: mpsc::UnboundedReceiver<Duration>,
    released: watch::Sender<ReleasedDeadlines>,
    pending_bounds: watch::Receiver<BTreeSet<u64>>,
}

impl StagedClock {
    pub(super) fn new() -> (Self, StagedClockControl) {
        let (registrations, registered) = mpsc::unbounded_channel();
        let (released, release) = watch::channel(ReleasedDeadlines::default());
        let (pending_bounds, pending) = watch::channel(BTreeSet::new());
        (
            Self {
                registrations,
                released: release,
                pending_bounds: Arc::new(pending_bounds),
                armed_bounds: Arc::new(AtomicU64::new(0)),
            },
            StagedClockControl {
                registrations: registered,
                released,
                pending_bounds: pending,
            },
        )
    }
}

impl StagedClockControl {
    pub(super) fn release(&self, deadline: Duration) {
        self.released.send_modify(|released| {
            released.durations.insert(deadline);
        });
    }

    fn release_all_but_the_bound(&self) {
        self.released
            .send_modify(|released| released.all_but_the_bound = true);
    }

    /// Waits for the next registration of `deadline`.
    pub(super) async fn registered(&mut self, deadline: Duration) {
        while let Some(registered) = self.registrations.recv().await {
            if registered == deadline {
                record_fixture_progress(format!("staged clock registered {deadline:?}"));
                return;
            }
        }
        panic!("the clock was dropped before {deadline:?} was registered");
    }

    /// Waits until exactly one quiet-bound wait is pending and it was armed after `after`.
    async fn fresh_bound(&mut self, after: Option<u64>) -> u64 {
        let pending = self
            .pending_bounds
            .wait_for(|pending| pending.len() == 1 && pending.first().copied() > after)
            .await
            .unwrap();
        let armed = *pending.first().unwrap();
        record_fixture_progress(format!("staged clock armed quiet bound {armed}"));
        armed
    }
}

/// One quiet-bound wait while it is pending.
struct PendingBound {
    pending_bounds: Arc<watch::Sender<BTreeSet<u64>>>,
    armed: u64,
}

impl Drop for PendingBound {
    fn drop(&mut self) {
        self.pending_bounds.send_modify(|pending| {
            pending.remove(&self.armed);
        });
    }
}

impl CoordinatorClock for StagedClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        if deadline == PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL {
            // An OS process-group probe has no readiness channel: a spawned empty task is the
            // scheduling point between probes.
            let _ = tokio::spawn(async {}).await;
            return;
        }
        let _ = self.registrations.send(deadline);
        let _pending = (deadline == QUIET_BOUND).then(|| {
            let armed = self.armed_bounds.fetch_add(1, Ordering::SeqCst) + 1;
            self.pending_bounds.send_modify(|pending| {
                pending.insert(armed);
            });
            PendingBound {
                pending_bounds: Arc::clone(&self.pending_bounds),
                armed,
            }
        });
        let mut released = self.released.clone();
        let closed = released
            .wait_for(|released| released.admit(deadline))
            .await
            .is_err();
        if closed {
            pending::<()>().await;
        }
    }
}

/// A validation that never decides and reports that it started.
#[derive(Clone)]
struct ReportedPendingValidationWorker {
    pending: PendingValidationWorker,
    started: mpsc::UnboundedSender<()>,
}

impl ResultValidationWorker for ReportedPendingValidationWorker {
    type Running = PendingValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        let _ = self.started.send(());
        self.pending.start(request)
    }
}

fn start_adapter<Clock, Worker>(
    invocation: TestInvocation,
    diagnostics: StepDiagnosticLog,
    clock: Clock,
    worker: Worker,
) -> (
    tokio::task::JoinHandle<()>,
    AgentStartReceiver,
    tokio::sync::oneshot::Receiver<AgentOutcome>,
)
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    let adapter = ClaudeCodeStreamJsonV1Adapter::with_worker(
        diagnostics,
        NonZeroU64::new(1024).unwrap(),
        clock,
        NoopExecutionObserver,
        worker,
    );
    let (started, start) = agent_start_channel();
    let (terminal, outcome) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let outcome = adapter.invoke(invocation, started).await;
        record_invocation_end(&outcome);
        let _ = terminal.send(outcome);
    });
    (task, start, outcome)
}

/// Runs a scripted fixture to its outcome, and whether start was reported.
async fn run_scripted<Clock, Worker>(
    mut fixture: ProcessFixture,
    clock: Clock,
    worker: Worker,
) -> (ProcessFixture, AgentOutcome, bool)
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    let invocation = fixture.invocation.take().unwrap();
    let (task, start, outcome) =
        start_adapter(invocation, fixture.diagnostics.clone(), clock, worker);
    task.await.unwrap();
    (
        fixture,
        outcome.await.unwrap(),
        start.receive().await.is_ok(),
    )
}

fn relay() -> Value {
    json!({"relay": true})
}

fn init() -> Value {
    json!({"init": QUALIFICATION_VERSION})
}

fn submit(value: Value) -> Value {
    json!({"submit": {"submission": {"result": value}}})
}

fn frame(value: Value) -> Value {
    json!({"frame": value})
}

fn successful_result(text: &str) -> Value {
    frame(
        json!({"type": "result", "subtype": "success", "is_error": false,
        "terminal_reason": "completed", "num_turns": 2, "result": text}),
    )
}

/// What the scripted fixture reported, in action order: each submission's answer and each
/// `stdin` probe.
fn fixture_report(fixture: &ProcessFixture) -> Vec<Value> {
    serde_json::from_slice(&std::fs::read(&fixture.report).unwrap()).unwrap()
}

fn recorded_arguments(fixture: &ProcessFixture) -> Vec<OsString> {
    std::fs::read(&fixture.arguments)
        .unwrap()
        .split(|byte| *byte == 0)
        .filter(|argument| !argument.is_empty())
        .map(|argument| OsStr::from_bytes(argument).to_owned())
        .collect()
}

fn value_rejections(observations: &RecordingObservationSink) -> Vec<Arc<str>> {
    observations
        .snapshot()
        .into_iter()
        .filter_map(|observation| match observation.observation() {
            AgentObservation::ValueRejected {
                kind: AgentValueKind::Result,
                feedback,
            } => Some(Arc::clone(feedback)),
            _ => None,
        })
        .collect()
}

/// The warnings the adapter showed, but the transcript-capture one a fake that writes no
/// native transcript always earns.
/// Waits until the adapter has read a `result` frame.
pub(super) async fn wait_for_turn_completed(observations: &RecordingObservationSink) {
    observations
        .wait_for(|observation| {
            matches!(
                observation,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::TurnCompleted,
                }
            )
        })
        .await;
    record_fixture_progress("adapter read a result frame".to_owned());
}

pub(super) fn adapter_warnings(observations: &RecordingObservationSink) -> Vec<Arc<str>> {
    observations
        .snapshot()
        .into_iter()
        .filter_map(|observation| match observation.observation() {
            AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Warning,
                message,
            } if !message.starts_with(NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC) => {
                Some(Arc::clone(message))
            }
            _ => None,
        })
        .collect()
}

fn fixture_process(path: &Path) -> Pid {
    let process = std::fs::read_to_string(path)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    Pid::from_raw(process).unwrap()
}

fn open_fixture_protocol() -> std::fs::File {
    std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/fd/3")
        .unwrap()
}

fn write_fixture_initialization(
    protocol: &mut std::fs::File,
    session: &str,
    padding: Option<String>,
) {
    let mut initialization = json!({
        "type": "system",
        "subtype": "init",
        "cwd": std::env::current_dir().unwrap(),
        "session_id": session,
        "model": MODEL,
        "permissionMode": "bypassPermissions",
        "claude_code_version": QUALIFICATION_VERSION,
    });
    if let Some(padding) = padding {
        initialization["padding"] = Value::String(padding);
    }
    serde_json::to_writer(&mut *protocol, &initialization).unwrap();
    protocol.write_all(b"\n").unwrap();
}

fn write_fixture_init() {
    let mut initial = String::new();
    assert!(std::io::stdin().lock().read_line(&mut initial).unwrap() > 0);
    let mut protocol = open_fixture_protocol();
    write_fixture_initialization(
        &mut protocol,
        &std::env::var("CLAUDE_FIXTURE_SESSION").unwrap(),
        None,
    );
}

#[test]
#[ignore = "launched as the cooperative Claude Code cancellation process fixture"]
fn cancellation_process_fixture() {
    write_fixture_init();
    let interrupted = process_fixture_interrupt_receiver();
    write_process_fixture_id("CLAUDE_FIXTURE_ARGUMENTS");
    write_process_fixture_signal("CLAUDE_FIXTURE_READY", b"ready\n");
    interrupted.recv().unwrap();
    write_process_fixture_signal("CLAUDE_FIXTURE_INTERRUPTED", b"interrupted\n");
    std::process::exit(130);
}

#[test]
#[ignore = "launched as the stubborn Claude Code process fixture"]
fn stubborn_process_fixture() {
    write_fixture_init();
    let interrupted = process_fixture_interrupt_receiver();
    write_process_fixture_id("CLAUDE_FIXTURE_ARGUMENTS");
    let _descendant = spawn_process_fixture(
        "workflow::claude_code::adapter_tests::stubborn_descendant_process_fixture",
    );
    interrupted.recv().unwrap();
    write_process_fixture_signal("CLAUDE_FIXTURE_INTERRUPTED", b"interrupted\n");
    loop {
        std::thread::park();
    }
}

#[test]
#[ignore = "launched as the interrupt-resistant Claude Code descendant fixture"]
fn stubborn_descendant_process_fixture() {
    let _interrupt = process_fixture_interrupt_receiver();
    write_process_fixture_id("CLAUDE_FIXTURE_CWD");
    write_process_fixture_signal("CLAUDE_FIXTURE_DESCENDANT_READY", b"ready\n");
    loop {
        std::thread::park();
    }
}

#[test]
#[ignore = "launched as the stdout-before-input Claude Code process fixture"]
fn stdout_before_input_process_fixture() {
    let session = std::env::var("CLAUDE_FIXTURE_SESSION").unwrap();
    let mut protocol = open_fixture_protocol();
    write_fixture_initialization(&mut protocol, &session, Some("x".repeat(1024 * 1024)));
    protocol.flush().unwrap();

    let mut initial = String::new();
    assert!(std::io::stdin().lock().read_line(&mut initial).unwrap() > 0);
    serde_json::to_writer(
        &mut protocol,
        &json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "terminal_reason": "completed",
            "result": "completed after input",
            "session_id": session,
        }),
    )
    .unwrap();
    protocol.write_all(b"\n").unwrap();
}

#[test]
#[ignore = "launched as the blocked-input Claude Code process fixture"]
fn blocked_input_process_fixture() {
    write_process_fixture_id("CLAUDE_FIXTURE_ARGUMENTS");
    write_process_fixture_signal("CLAUDE_FIXTURE_READY", b"ready\n");
    loop {
        std::thread::park();
    }
}

#[test]
#[ignore = "launched as the delayed-input Claude Code process fixture"]
fn delayed_input_process_fixture() {
    write_process_fixture_signal("CLAUDE_FIXTURE_READY", b"ready\n");
    await_fixture_release();
    stdout_before_input_process_fixture();
}

fn write_fixture_frames(protocol: &mut std::fs::File, values: &[Value]) {
    let mut bytes = Vec::new();
    for value in values {
        serde_json::to_writer(&mut bytes, value).unwrap();
        bytes.push(b'\n');
    }
    protocol.write_all(&bytes).unwrap();
    protocol.flush().unwrap();
}

/// Each release is one line the case writes to the `release` FIFO beside the ready signal.
fn await_fixture_release() {
    let ready = PathBuf::from(std::env::var_os("CLAUDE_FIXTURE_READY").unwrap());
    read_fixture_release(&ready.with_file_name("release"));
}

/// Opens the `release` FIFO at `path` and takes one release from it, a byte at a time so that
/// a release queued behind it stays for the next wait (INV-TESTING-29). The case holds a writer
/// for its life, so the open returns at once and end of file means the case has gone away.
fn read_fixture_release(path: &Path) {
    let mut release = std::fs::File::open(path).unwrap();
    let mut byte = [0_u8; 1];
    loop {
        release
            .read_exact(&mut byte)
            .expect("the release FIFO ended before a release");
        if byte[0] == b'\n' {
            return;
        }
    }
}

/// Reads the prompt frame from stdin without buffering past it, so that what follows it is
/// still there for the `stdin` action to see.
fn read_prompt_frame() {
    let mut byte = [0_u8; 1];
    loop {
        let read = rustix::io::read(std::io::stdin(), &mut byte).unwrap();
        assert_eq!(read, 1, "stdin ended inside the prompt frame");
        if byte[0] == b'\n' {
            return;
        }
    }
}

/// `open` while stdin has neither ended nor carries a byte.
fn standard_input_state() -> &'static str {
    let standard_input = std::io::stdin();
    let mut probe = [rustix::event::PollFd::new(
        &standard_input,
        rustix::event::PollFlags::IN | rustix::event::PollFlags::HUP,
    )];
    let ready = rustix::event::poll(
        &mut probe,
        Some(&rustix::event::Timespec {
            tv_sec: 0,
            tv_nsec: 0,
        }),
    )
    .unwrap();
    if ready == 0 { "open" } else { "readable" }
}

/// The scripted fake Claude Code. It writes its process id, reads the prompt frame, and then
/// performs `CLAUDE_FIXTURE_ACTIONS` in order: `{"relay": _}` starts the relay the
/// `--mcp-config` file names and initializes it, as Claude Code does before `init`;
/// `{"init": version}` writes `system/init` naming `version`; `{"submit": arguments}` calls
/// the result tool and reports its answer; `{"frame": value}` writes a frame; `{"exit": code}`
/// exits; `"ready"` signals ready; `"release"` waits for the case's release; `"stdin"`
/// reports whether stdin is still `open`; `"catch_interrupt"` keeps SIGINT from ending it;
/// `"interrupted"` waits for SIGINT and signals it; `"park"` never returns. It then writes
/// its report and exits 0.
#[test]
#[ignore = "launched as the scripted Claude Code process fixture"]
fn scripted_process_fixture() {
    write_process_fixture_id("CLAUDE_FIXTURE_PROCESS");
    read_prompt_frame();
    let actions: Vec<Value> =
        serde_json::from_str(&std::env::var("CLAUDE_FIXTURE_ACTIONS").unwrap()).unwrap();
    let mut protocol = open_fixture_protocol();
    let mut relay = None;
    let mut interrupt = None;
    let mut report = Vec::new();
    for action in actions {
        match action.as_str() {
            Some("ready") => write_process_fixture_signal("CLAUDE_FIXTURE_READY", b"ready\n"),
            Some("release") => await_fixture_release(),
            Some("stdin") => report.push(json!(standard_input_state())),
            Some("catch_interrupt") => interrupt = Some(process_fixture_interrupt_receiver()),
            Some("interrupted") => {
                let interrupted: &std::sync::mpsc::Receiver<()> = interrupt.as_ref().unwrap();
                interrupted.recv().unwrap();
                write_process_fixture_signal("CLAUDE_FIXTURE_INTERRUPTED", b"interrupted\n");
            }
            Some("park") => loop {
                std::thread::park();
            },
            _ => {
                let (name, value) = action.as_object().unwrap().iter().next().unwrap();
                match name.as_str() {
                    "relay" => {
                        let configuration =
                            PathBuf::from(std::env::var_os("CLAUDE_FIXTURE_MCP_CONFIG").unwrap());
                        let mut server = McpServerProcess::start(&configuration).unwrap();
                        server.initialize().unwrap();
                        relay = Some(server);
                    }
                    "init" => write_fixture_frames(
                        &mut protocol,
                        &[json!({
                            "type": "system",
                            "subtype": "init",
                            "cwd": std::env::current_dir().unwrap(),
                            "session_id": std::env::var("CLAUDE_FIXTURE_SESSION").unwrap(),
                            "model": MODEL,
                            "permissionMode": "bypassPermissions",
                            "claude_code_version": value,
                        })],
                    ),
                    "submit" => {
                        let server: &mut McpServerProcess = relay.as_mut().unwrap();
                        report.push(match server.call_tool(TOOL_NAME, value.clone()) {
                            Ok(answer) => json!({"isError": answer.is_error, "text": answer.text}),
                            Err(error) => json!({"error": error.to_string()}),
                        });
                    }
                    "frame" => write_fixture_frames(&mut protocol, std::slice::from_ref(value)),
                    "exit" => std::process::exit(i32::try_from(value.as_i64().unwrap()).unwrap()),
                    _ => panic!("unknown fixture action {action}"),
                }
            }
        }
    }
    std::fs::write(
        std::env::var_os("CLAUDE_FIXTURE_REPORT").unwrap(),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
}

// Plain functions, not `async fn`: the body reaches the watchdog's heap pin before any
// wrapper state holds it. An expiry names the last fixture progress the case recorded.
fn with_watchdog<Output>(future: impl Future<Output = Output>) -> impl Future<Output = Output> {
    with_watchdog_limit(future, Duration::from_secs(10))
}

fn with_watchdog_limit<Output>(
    future: impl Future<Output = Output>,
    base: Duration,
) -> impl Future<Output = Output> {
    fixture_progress_watchdog("ClaudeCodeStreamJsonV1 fixture", base, future)
}

/// The panic message of a case `with_watchdog_limit` bounded for a millisecond.
async fn expired_watchdog_message(body: impl Future<Output = ()> + Send + 'static) -> String {
    expired_bound_message(with_watchdog_limit(body, Duration::from_millis(1))).await
}

/// The panic message of an already bounded future whose watchdog expires.
async fn expired_bound_message(bounded: impl Future<Output = ()> + Send + 'static) -> String {
    let expired = tokio::spawn(bounded).await.unwrap_err();
    *expired.into_panic().downcast::<String>().unwrap()
}

// INV-TESTING-18
#[tokio::test]
async fn fixture_watchdog_expiry_names_the_last_fixture_progress() {
    let message = expired_watchdog_message(async {
        record_fixture_progress("probe progress".to_owned());
        pending::<()>().await;
    })
    .await;
    assert!(
        message.starts_with("ClaudeCodeStreamJsonV1 fixture watchdog expired after")
            && message.ends_with("; last fixture progress: probe progress"),
        "{message:?}"
    );

    let message = expired_watchdog_message(pending::<()>()).await;
    assert!(
        message.ends_with("; last fixture progress: none"),
        "{message:?}"
    );

    // The suite's own waits record what they saw.
    let temporary = tempfile::tempdir().unwrap();
    let signal = FixtureSignal::create(temporary.path().join("probe"));
    std::fs::write(signal.path(), b"probe\n").unwrap();
    let message = expired_watchdog_message(async move {
        signal.receive().await;
        pending::<()>().await;
    })
    .await;
    assert!(
        message.contains("; last fixture progress: ") && !message.ends_with("none"),
        "{message:?}"
    );
}

// INV-TESTING-18
#[tokio::test]
async fn native_resource_watchdog_expiry_carries_the_mcp_observation() {
    let temporary = tempfile::tempdir().unwrap();
    let observation = temporary.path().join("native-mcp-observation.txt");
    std::fs::write(&observation, "started\n{\"method\":\"tools/call\"}\n").unwrap();
    let message = expired_bound_message(native_resource_watchdog(
        observation,
        Duration::from_millis(1),
        pending::<()>(),
    ))
    .await;
    assert!(
        message.starts_with("pinned Claude Code conformance watchdog expired after")
            && message.contains("; MCP observation: started\n{\"method\":\"tools/call\"}\n"),
        "{message:?}"
    );

    // An unreadable observation is diagnosed, never a panic that replaces the expiry.
    let message = expired_bound_message(native_resource_watchdog(
        temporary.path().join("absent"),
        Duration::from_millis(1),
        pending::<()>(),
    ))
    .await;
    assert!(
        message.starts_with("pinned Claude Code conformance watchdog expired after")
            && message.contains("; MCP observation: "),
        "{message:?}"
    );
}

#[test]
fn launch_plan_stages_exact_prompt_and_lf_delimited_input() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let plan = prepare_launch(fixture.invocation.as_ref().unwrap(), PendingClock).unwrap();
    assert_eq!(
        std::fs::read(plan.system_prompt_file()).unwrap(),
        b"exact system prompt\n"
    );
    assert_eq!(plan.input().last(), Some(&b'\n'));
    assert_eq!(
        serde_json::from_slice::<Value>(&plan.input()[..plan.input().len() - 1]).unwrap(),
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": "exact message @literal\nsecond line",
                }],
            },
        })
    );
    assert_eq!(plan.arguments()[0], OsStr::new("-p"));
    let session_argument = plan
        .arguments()
        .windows(2)
        .find(|arguments| arguments[0] == OsStr::new("--session-id"))
        .unwrap();
    assert_eq!(session_argument[1], OsStr::new(fixture.session_id.as_ref()));
}

// REQ-AGENT-17
// INV-HARNESS-9
#[test]
fn an_existing_native_session_entry_is_never_selected_or_replaced() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let ambient_project = fixture
        .config
        .join("projects")
        .join(native_project_slug(fixture.expected_cwd.to_str().unwrap()));
    std::fs::create_dir_all(&ambient_project).unwrap();
    let existing = ambient_project.join(format!("{}.jsonl", fixture.session_id));
    std::fs::write(&existing, b"existing conversation sentinel").unwrap();

    assert!(matches!(
        prepare_launch(fixture.invocation.as_ref().unwrap(), PendingClock),
        Err(AgentFailureCause::HarnessStartFailed { .. })
    ));
    assert_eq!(
        std::fs::read(existing).unwrap(),
        b"existing conversation sentinel"
    );
}

// REQ-GRANT-37
// INV-HARNESS-9
#[test]
fn the_bridge_removes_only_the_ambient_directories_it_created() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let projects = fixture.config.join("projects");
    let unrelated = projects.join("unrelated-project");
    std::fs::create_dir_all(&unrelated).unwrap();
    let ambient_project =
        projects.join(native_project_slug(fixture.expected_cwd.to_str().unwrap()));

    let plan = prepare_launch(fixture.invocation.as_ref().unwrap(), PendingClock).unwrap();
    assert!(ambient_project.is_dir());
    drop(plan);

    assert!(!ambient_project.exists());
    assert!(unrelated.is_dir());
    assert_eq!(std::fs::read_dir(&projects).unwrap().count(), 1);
}

#[test]
fn native_project_slug_matches_the_pinned_claude_code_storage_format() {
    assert_eq!(native_project_slug("/tmp/emoji-😀"), "-tmp-emoji---");
    let long = format!("/{}", "a".repeat(201));
    assert_eq!(
        native_project_slug(&long),
        format!("-{}-85qkr6", "a".repeat(199))
    );
}

// REQ-AGENT-18, REQ-AGENT-19
#[test]
fn attachment_transport_is_ordered_lossless_and_uses_only_sealed_identities() {
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"immutable source text").unwrap();
    let staged_source = std::fs::read(source.path()).unwrap();
    let fixtures = [
        AttachmentFixture {
            media_type: "text/plain",
            bytes: &staged_source,
            diagnostic_source_name: Some("../../caller-name.txt"),
        },
        AttachmentFixture {
            media_type: "Application/JSON; Charset=UTF-8",
            bytes: br#"{"a":1,"z":2}"#,
            diagnostic_source_name: Some("duplicate"),
        },
        AttachmentFixture {
            media_type: "IMAGE/PNG",
            bytes: &[0, 1, 2, 0xff],
            diagnostic_source_name: Some("duplicate"),
        },
        AttachmentFixture {
            media_type: "APPLICATION/PDF",
            bytes: b"%PDF-exact",
            diagnostic_source_name: Some("000999\n@escape"),
        },
        AttachmentFixture {
            media_type: "text/markdown",
            bytes: &[0xff],
            diagnostic_source_name: None,
        },
        AttachmentFixture {
            media_type: "application/octet-stream",
            bytes: &[0xde, 0xad],
            diagnostic_source_name: None,
        },
        AttachmentFixture {
            media_type: "text/plain; charset=utf-8",
            bytes: b"",
            diagnostic_source_name: None,
        },
        AttachmentFixture {
            media_type: "image/png; charset=binary",
            bytes: &[1, 2, 3],
            diagnostic_source_name: None,
        },
        AttachmentFixture {
            media_type: "application/pdf",
            bytes: b"",
            diagnostic_source_name: None,
        },
    ];
    let fixture = ProcessFixture::with_attachments(AgentValueMode::None, 1024, &fixtures);
    std::fs::write(source.path(), b"mutated after staging").unwrap();
    let invocation = fixture.invocation.as_ref().unwrap();
    let sealed_paths = invocation
        .attachments()
        .iter()
        .map(|attachment| attachment.path().to_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let plan = prepare_launch(invocation, PendingClock).unwrap();
    let frame = serde_json::from_slice::<Value>(&plan.input()[..plan.input().len() - 1]).unwrap();
    let content = frame["message"]["content"].as_array().unwrap();

    assert_eq!(content.len(), fixtures.len() + 1);
    assert_eq!(
        content[0],
        json!({
            "type": "text",
            "text": "exact message @literal\nsecond line",
        })
    );
    assert_eq!(
        content[1],
        json!({
            "type": "text",
            "text": "Scherzo attachment 000000 (text/plain) follows:\nimmutable source text",
        })
    );
    assert_eq!(
        content[2],
        json!({
            "type": "text",
            "text": "Scherzo attachment 000001 (Application/JSON; Charset=UTF-8) follows:\n{\"a\":1,\"z\":2}",
        })
    );
    assert_eq!(
        content[3],
        json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": "AAEC/w==",
            },
        })
    );
    assert_eq!(
        content[4],
        json!({
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": "JVBERi1leGFjdA==",
            },
        })
    );
    for (content_index, attachment_index) in [(5, 4), (6, 5), (8, 7)] {
        let media_type = fixtures[attachment_index].media_type;
        assert_eq!(
            content[content_index],
            json!({
                "type": "text",
                "text": format!(
                    "Scherzo attachment {attachment_index:06} has media type {media_type} and is available to runner tools at {}.",
                    sealed_paths[attachment_index]
                ),
            })
        );
    }
    assert_eq!(
        content[7],
        json!({
            "type": "text",
            "text": "Scherzo attachment 000006 (text/plain; charset=utf-8) follows:\n",
        })
    );
    assert_eq!(
        content[9],
        json!({
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": "",
            },
        })
    );
    let serialized = String::from_utf8(plan.input().to_vec()).unwrap();
    for diagnostic_name in ["../../caller-name.txt", "duplicate", "000999", "@escape"] {
        assert!(!serialized.contains(diagnostic_name));
    }
}

#[test]
fn unreadable_native_attachment_preserves_the_attachment_read_failure() {
    let fixture = ProcessFixture::with_attachments(
        AgentValueMode::None,
        1024,
        &[AttachmentFixture {
            media_type: "text/plain",
            bytes: b"permission denied",
            diagnostic_source_name: None,
        }],
    );
    let attachment_path = fixture.invocation.as_ref().unwrap().attachments()[0]
        .path()
        .to_owned();
    std::fs::set_permissions(&attachment_path, std::fs::Permissions::from_mode(0o000)).unwrap();

    let Err(AgentFailureCause::HarnessStartFailed { stage, error }) =
        prepare_launch(fixture.invocation.as_ref().unwrap(), PendingClock)
    else {
        panic!("unreadable attachment did not produce a typed launch failure");
    };
    assert_eq!(stage, "attachment read");
    assert!(!error.is_empty());
}

// REQ-AGENT-19
#[test]
fn native_attachment_above_the_inline_cap_uses_a_bounded_path_reference() {
    let oversized = vec![0x5a; MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES + 1];
    let fixture = ProcessFixture::with_attachments(
        AgentValueMode::None,
        1024,
        &[AttachmentFixture {
            media_type: "image/png",
            bytes: &oversized,
            diagnostic_source_name: None,
        }],
    );
    let invocation = fixture.invocation.as_ref().unwrap();
    let sealed_path = invocation.attachments()[0].path().to_str().unwrap();
    let plan = prepare_launch(invocation, PendingClock).unwrap();
    assert!(plan.input().len() <= MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES);

    let frame = serde_json::from_slice::<Value>(&plan.input()[..plan.input().len() - 1]).unwrap();
    let reference = &frame["message"]["content"][1];
    assert_eq!(reference["type"], "text");
    assert!(reference.get("source").is_none());
    assert!(reference["text"].as_str().unwrap().contains(sealed_path));
}

fn process_fixture_with_large_native_attachment(script: &str, fill: u8) -> ProcessFixture {
    let attachment = vec![fill; 1024 * 1024];
    let fixture = ProcessFixture::with_attachments(
        AgentValueMode::None,
        1024,
        &[AttachmentFixture {
            media_type: "image/png",
            bytes: &attachment,
            diagnostic_source_name: None,
        }],
    );
    std::fs::write(
        fixture.invocation.as_ref().unwrap().adapter().executable(),
        script,
    )
    .unwrap();
    fixture
}

#[tokio::test]
async fn initial_input_is_written_while_large_stdout_is_pumped() {
    with_watchdog(async {
        let fixture =
            process_fixture_with_large_native_attachment(STDOUT_BEFORE_INPUT_FAKE_CLAUDE, 0x42);
        let (_, outcome) = run_fixture(fixture).await;
        assert_eq!(
            outcome,
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
        );
    })
    .await;
}

// REQ-AGENT-62
#[tokio::test]
async fn slow_startup_does_not_expire_initial_input_delivery() {
    with_watchdog(async {
        let mut fixture =
            process_fixture_with_large_native_attachment(DELAYED_INPUT_FAKE_CLAUDE, 0x24);
        let invocation = fixture.invocation.take().unwrap();
        let (clock, control) = StagedClock::new();
        // Every deadline but the quiet bound has already expired. A large initial frame
        // must still wait for the gated reader, rather than fail before system/init.
        control.release_all_but_the_bound();
        let adapter = ClaudeCodeStreamJsonV1Adapter::with_worker(
            fixture.diagnostics.clone(),
            NonZeroU64::new(1024).unwrap(),
            clock,
            NoopExecutionObserver,
            InlineValidationWorker,
        );
        let (started, start) = agent_start_channel();
        let mut task = tokio::spawn(async move { adapter.invoke(invocation, started).await });
        tokio::select! {
            ready = fixture.ready.receive() => assert_eq!(ready, b"ready\n"),
            outcome = &mut task => panic!("initial input ended before the reader was released: {outcome:?}"),
        }
        fixture.release();
        assert_eq!(task.await.unwrap(), AgentOutcome::Completed(CompletedAgentInvocation::NoValue));
        assert!(start.receive().await.is_ok());
    })
    .await;
}

// REQ-AGENT-46, REQ-AGENT-62
#[tokio::test]
async fn cancellation_during_initial_input_quiesces_the_process_group() {
    with_watchdog(async {
        let mut fixture =
            process_fixture_with_large_native_attachment(BLOCKED_INPUT_FAKE_CLAUDE, 0x24);
        let invocation = fixture.invocation.take().unwrap();
        let cancellation = invocation.cancellation().clone();
        let adapter = ClaudeCodeStreamJsonV1Adapter::with_worker(
            fixture.diagnostics.clone(),
            NonZeroU64::new(1024).unwrap(),
            PendingClock,
            NoopExecutionObserver,
            InlineValidationWorker,
        );
        let (started, start) = agent_start_channel();
        let (terminal, outcome) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = terminal.send(adapter.invoke(invocation, started).await);
        });

        assert_eq!(fixture.ready.receive().await, b"ready\n");
        let process = fixture_process(&fixture.arguments);
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        task.await.unwrap();
        let outcome = outcome.await.unwrap();
        assert!(start.receive().await.is_err());
        assert_user_cancelled(outcome);
        assert!(process_group_is_quiescent(process));
    })
    .await;
}

// REQ-AGENT-29, REQ-AGENT-35, REQ-AGENT-84, REQ-AGENT-85
// INV-HARNESS-7, INV-HARNESS-23, INV-HARNESS-24
#[tokio::test]
async fn a_rejected_result_is_corrected_through_the_tool_and_the_last_accepted_one_is_the_value() {
    with_watchdog(async {
        let fixture = ProcessFixture::scripted(
            result_mode(positive_integer_schema()),
            &[
                relay(),
                init(),
                submit(json!(0)),
                submit(json!(3)),
                submit(json!(5)),
                json!("stdin"),
                successful_result("submitted"),
            ],
        );
        let (fixture, outcome, started) =
            run_scripted(fixture, PendingClock, InlineValidationWorker).await;
        assert!(started, "terminal outcome: {outcome:?}");
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("the last accepted submission must be the value: {outcome:?}");
        };
        assert_eq!(result.value(), &json!(5));

        let report = fixture_report(&fixture);
        assert_eq!(report.len(), 4, "{report:?}");
        // The rejection is the tool's error answer and the one observation of it.
        let rejections = value_rejections(&fixture.observations);
        assert_eq!(rejections.len(), 1);
        assert_eq!(report[0]["isError"], true);
        assert_eq!(report[0]["text"], rejections[0].as_ref());
        assert!(!rejections[0].is_empty());
        assert_eq!(report[1]["isError"], false);
        assert_eq!(report[2]["isError"], false);
        // Standard input held nothing after the prompt frame and had not ended.
        assert_eq!(report[3], "open");

        let arguments = recorded_arguments(&fixture);
        let [.., flag, configuration] = arguments.as_slice() else {
            panic!("too few arguments: {arguments:?}");
        };
        assert_eq!(flag, "--mcp-config");
        let configuration = Path::new(configuration);
        assert!(configuration.starts_with(&fixture.result_endpoint));
        // The configuration goes with the invocation.
        assert!(!configuration.exists());
    })
    .await;
}

// REQ-AGENT-87, REQ-AGENT-42
#[tokio::test]
async fn a_stream_the_reader_cannot_name_fails_nothing_and_leaves_no_rejection_record() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let session = fixture.session_id.to_string();
    let script = production_transcript_script(
        &[
            json!({"type": "mystery_frame", "payload": 1}),
            json!(["not", "an", "object"]),
            json!({"type": "system", "subtype": "init", "cwd": "/elsewhere",
                "session_id": "another-session", "model": "another-model",
                "permissionMode": "default", "claude_code_version": "0.0.1"}),
            json!({"type": "system", "subtype": "never_seen_before"}),
            json!({"type": "result", "subtype": "success", "is_error": false,
                "terminal_reason": "completed", "num_turns": 1, "result": "done",
                "session_id": session}),
        ],
        true,
        0,
        &session,
    );
    std::fs::write(
        fixture.invocation.as_ref().unwrap().adapter().executable(),
        script,
    )
    .unwrap();

    let (fixture, outcome) = run_fixture(fixture).await;
    assert_eq!(
        outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
    let observations = fixture.observations.snapshot();
    assert_eq!(
        observations
            .iter()
            .filter(|observation| matches!(
                observation.observation(),
                AgentObservation::UnrecognizedHarnessEvent { .. }
            ))
            .count(),
        3
    );
    assert!(!adapter_warnings(&fixture.observations).is_empty());
    assert!(fixture.diagnostic_directory.is_dir());
    assert!(
        !fixture
            .diagnostic_directory
            .join("protocol-rejection.json")
            .exists()
    );
}

/// Runs a scripted fixture that never ends by itself and is expected to fail at start, and
/// checks that its group is gone.
async fn assert_fails_at_start(fixture: ProcessFixture) {
    let (fixture, outcome, started) =
        run_scripted(fixture, PendingClock, InlineValidationWorker).await;
    assert!(!started, "terminal outcome: {outcome:?}");
    assert_eq!(
        outcome,
        failed_agent_outcome(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable"
        ))
    );
    assert!(process_group_is_quiescent(fixture_process(
        &fixture.process
    )));
}

// REQ-AGENT-89
#[tokio::test]
async fn an_init_without_the_result_tool_fails_at_start() {
    with_watchdog(assert_fails_at_start(ProcessFixture::scripted(
        result_mode(positive_integer_schema()),
        &[init(), json!("park")],
    )))
    .await;
}

// REQ-AGENT-44
// INV-HARNESS-4
#[tokio::test]
async fn an_init_naming_another_version_fails_at_start() {
    with_watchdog(assert_fails_at_start(ProcessFixture::scripted(
        result_mode(positive_integer_schema()),
        &[relay(), json!({"init": "2.1.320"}), json!("park")],
    )))
    .await;
}

// INV-TESTING-29
#[test]
fn a_release_neither_waits_for_the_fixture_nor_ends_its_next_wait() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("release");
    let release = FixtureRelease::create(&path);
    // Nothing reads the FIFO yet: both releases return, and both stay queued.
    release.release();
    release.release();
    // The fixture's side waits on its own thread, so a wait that never returns expires the
    // bound here; a wait that read end of file panics and drops `taken` unsent.
    let (taken, waited) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        for _ in 0..3 {
            read_fixture_release(&path);
            taken.send(()).unwrap();
        }
    });
    let wait_taken = || {
        host_scaled_recv("Claude Code fixture release", FIXTURE_WAIT_BASE, &waited).unwrap();
    };
    // Each queued release is one wait's,
    wait_taken();
    wait_taken();
    // and a wait that opens beside the earlier releases' writer takes the next line rather
    // than end of file.
    release.release();
    wait_taken();
    waiter.join().unwrap();
}

// REQ-AGENT-88
// INV-HARNESS-24
#[tokio::test]
async fn a_silent_claude_code_is_stopped_at_the_quiet_bound() {
    with_watchdog(async {
        // Silent after a frame and a submission, without a successful `result`: interrupted
        // at the bound, and how it then exits is not read.
        let mut fixture = ProcessFixture::scripted(
            result_mode(positive_integer_schema()),
            &[
                relay(),
                init(),
                json!("ready"),
                json!("release"),
                frame(json!({"type": "system", "subtype": "status", "status": "requesting"})),
                json!("ready"),
                json!("release"),
                submit(json!(7)),
                json!("catch_interrupt"),
                json!("ready"),
                json!("interrupted"),
                json!({"exit": 0}),
            ],
        );
        let (clock, mut control) = StagedClock::new();
        let (task, start, outcome) = start_adapter(
            fixture.invocation.take().unwrap(),
            fixture.diagnostics.clone(),
            clock,
            InlineValidationWorker,
        );
        control.registered(QUIET_BOUND).await;
        start.receive().await.unwrap();
        assert_eq!(fixture.ready.receive().await, b"ready\n");
        let mut bound = control.fresh_bound(None).await;
        // A stdout byte arms the bound afresh,
        fixture.release();
        assert_eq!(fixture.ready.receive().await, b"ready\n");
        bound = control.fresh_bound(Some(bound)).await;
        // and so does a request on the result channel.
        fixture.release();
        assert_eq!(fixture.ready.receive().await, b"ready\n");
        control.fresh_bound(Some(bound)).await;

        control.release(QUIET_BOUND);
        assert_eq!(fixture.interrupted.receive().await, b"interrupted\n");
        task.await.unwrap();
        assert_eq!(
            outcome.await.unwrap(),
            failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            })
        );
        assert_eq!(adapter_warnings(&fixture.observations).len(), 1);

        // A value accepted and a turn reported successful before the bound stand.
        let mut fixture = ProcessFixture::scripted(
            result_mode(positive_integer_schema()),
            &[
                relay(),
                init(),
                submit(json!(7)),
                successful_result("submitted"),
                json!("catch_interrupt"),
                json!("ready"),
                json!("interrupted"),
                json!({"exit": 130}),
            ],
        );
        let (clock, control) = StagedClock::new();
        let (task, _start, outcome) = start_adapter(
            fixture.invocation.take().unwrap(),
            fixture.diagnostics.clone(),
            clock,
            InlineValidationWorker,
        );
        assert_eq!(fixture.ready.receive().await, b"ready\n");
        // The bound is taken ahead of a stdout read, so the `result` must have been read.
        wait_for_turn_completed(&fixture.observations).await;
        control.release(QUIET_BOUND);
        task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) =
            outcome.await.unwrap()
        else {
            panic!("the accepted value must stand at the bound");
        };
        assert_eq!(result.value(), &json!(7));

        // A Claude Code that ignores the interrupt is forced 30 seconds later.
        let mut fixture = ProcessFixture::scripted(
            AgentValueMode::None,
            &[
                init(),
                json!("catch_interrupt"),
                json!("ready"),
                json!("interrupted"),
                json!("park"),
            ],
        );
        let (clock, mut control) = StagedClock::new();
        let (task, start, outcome) = start_adapter(
            fixture.invocation.take().unwrap(),
            fixture.diagnostics.clone(),
            clock,
            InlineValidationWorker,
        );
        assert_eq!(fixture.ready.receive().await, b"ready\n");
        // The bound is taken ahead of a stdout read, so the `init` must have been read.
        start.receive().await.unwrap();
        let process = fixture_process(&fixture.process);
        control.release(QUIET_BOUND);
        assert_eq!(fixture.interrupted.receive().await, b"interrupted\n");
        control.registered(QUIET_BOUND_FORCE_DELAY).await;
        assert!(!task.is_finished());
        control.release(QUIET_BOUND_FORCE_DELAY);
        task.await.unwrap();
        assert_eq!(
            outcome.await.unwrap(),
            failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            })
        );
        assert!(process_group_is_quiescent(process));
    })
    .await;
}

// REQ-AGENT-88
#[tokio::test]
async fn the_result_an_interrupt_at_the_quiet_bound_draws_decides_nothing() {
    with_watchdog(async {
        // 2.1.321 interrupted in a turn reports that turn aborted, as the pinned native case
        // `07` observed; the step is decided by what it held at the bound.
        let aborted = frame(json!({
            "type": "result", "subtype": "error_during_execution", "is_error": true,
            "terminal_reason": "aborted_streaming", "num_turns": 2,
        }));
        // Without a successful `result` before the bound the step fails, and with one the
        // accepted value stands.
        for (before_the_bound, value) in [
            (None, None),
            (Some(successful_result("submitted")), Some(json!(7))),
        ] {
            let mut script = vec![relay(), init(), submit(json!(7))];
            let turn_ends_before_the_bound = before_the_bound.is_some();
            script.extend(before_the_bound);
            script.extend([
                json!("catch_interrupt"),
                json!("ready"),
                json!("interrupted"),
                aborted.clone(),
                json!({"exit": 0}),
            ]);
            let mut fixture =
                ProcessFixture::scripted(result_mode(positive_integer_schema()), &script);
            let (clock, control) = StagedClock::new();
            let (task, _start, outcome) = start_adapter(
                fixture.invocation.take().unwrap(),
                fixture.diagnostics.clone(),
                clock,
                InlineValidationWorker,
            );
            assert_eq!(fixture.ready.receive().await, b"ready\n");
            // The bound is taken ahead of a stdout read, so the `result` must have been read.
            if turn_ends_before_the_bound {
                wait_for_turn_completed(&fixture.observations).await;
            }
            control.release(QUIET_BOUND);
            task.await.unwrap();
            let outcome = outcome.await.unwrap();
            match value {
                None => assert_eq!(
                    outcome,
                    failed_agent_outcome(AgentFailureCause::HarnessFailed {
                        detail: AgentHarnessFailureDetail::UnsuccessfulExit,
                    })
                ),
                Some(value) => {
                    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome
                    else {
                        panic!("the accepted value must stand at the bound, got {outcome:?}");
                    };
                    assert_eq!(result.value(), &value);
                }
            }
        }
    })
    .await;
}

// REQ-AGENT-32, REQ-AGENT-84
#[tokio::test]
async fn every_authoritatively_valid_json_root_is_returned_as_submitted() {
    let cases = [
        ("object", json!({"value": 1})),
        ("array", json!([1, 2])),
        ("string", json!("value")),
        ("number", json!(7)),
        ("boolean", json!(true)),
        ("null", Value::Null),
    ];
    for (root_type, candidate) in cases {
        let fixture = ProcessFixture::scripted(
            result_mode(type_schema(root_type)),
            &[
                relay(),
                init(),
                submit(candidate.clone()),
                successful_result("submitted"),
            ],
        );
        let (_, outcome, _) =
            with_watchdog(run_scripted(fixture, PendingClock, InlineValidationWorker)).await;
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("{root_type} root must complete through authoritative validation: {outcome:?}");
        };
        assert_eq!(result.value(), &candidate);
    }
}

// REQ-AGENT-30
#[tokio::test]
async fn an_oversized_submission_receives_bounded_feedback_before_a_valid_one() {
    with_watchdog(async {
        let fixture = ProcessFixture::scripted(
            result_mode(type_schema("string")),
            &[
                relay(),
                init(),
                submit(json!("x".repeat(1100))),
                submit(json!("ok")),
                successful_result("submitted"),
            ],
        );
        let (fixture, outcome, _) =
            run_scripted(fixture, PendingClock, InlineValidationWorker).await;
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("the valid submission must complete: {outcome:?}");
        };
        assert_eq!(result.value(), &json!("ok"));
        let report = fixture_report(&fixture);
        assert_eq!(report[0]["isError"], true);
        let feedback = report[0]["text"].as_str().unwrap();
        assert!(!feedback.is_empty() && feedback.len() <= 512);
        assert_eq!(report[1]["isError"], false);
    })
    .await;
}

// REQ-AGENT-31
#[tokio::test]
async fn a_step_without_an_accepted_submission_fails_with_missing_result() {
    with_watchdog(async {
        for actions in [
            vec![relay(), init(), successful_result("no submission")],
            vec![
                relay(),
                init(),
                submit(json!(0)),
                successful_result("rejected"),
            ],
        ] {
            let fixture =
                ProcessFixture::scripted(result_mode(positive_integer_schema()), &actions);
            let (_, outcome, _) = run_scripted(fixture, PendingClock, InlineValidationWorker).await;
            assert_eq!(
                outcome,
                failed_agent_outcome(AgentFailureCause::MissingResult)
            );
        }
    })
    .await;
}

// REQ-AGENT-34
#[tokio::test]
async fn validation_timeout_stops_its_worker_and_discards_the_submission() {
    with_watchdog(async {
        let stopped = Arc::new(AtomicBool::new(false));
        let quiesced = Arc::new(AtomicBool::new(false));
        let (validation_started, mut validating) = mpsc::unbounded_channel();
        let worker = ReportedPendingValidationWorker {
            pending: PendingValidationWorker {
                stopped: Arc::clone(&stopped),
                quiesced: Arc::clone(&quiesced),
            },
            started: validation_started,
        };
        let fixture = ProcessFixture::scripted(
            result_mode(type_schema("number")),
            &[
                relay(),
                init(),
                submit(json!(1)),
                successful_result("submitted"),
            ],
        );
        let (clock, control) = StagedClock::new();
        let task = tokio::spawn(run_scripted(fixture, clock, worker));
        validating.recv().await.unwrap();
        control.release(Duration::from_secs(1));
        let (_, outcome, _) = task.await.unwrap();
        assert_eq!(
            outcome,
            failed_agent_outcome(AgentFailureCause::ResultValidationLimitExceeded {
                deadline: PositiveDuration::new(Duration::from_secs(1)).unwrap(),
            })
        );
        assert!(stopped.load(Ordering::SeqCst));
        assert!(quiesced.load(Ordering::SeqCst));
    })
    .await;
}

#[tokio::test]
async fn cancellation_during_validation_stops_the_worker_and_quiesces_the_process_group() {
    with_watchdog(async {
        let stopped = Arc::new(AtomicBool::new(false));
        let quiesced = Arc::new(AtomicBool::new(false));
        let (validation_started, mut validating) = mpsc::unbounded_channel();
        let worker = ReportedPendingValidationWorker {
            pending: PendingValidationWorker {
                stopped: Arc::clone(&stopped),
                quiesced: Arc::clone(&quiesced),
            },
            started: validation_started,
        };
        let fixture = ProcessFixture::scripted(
            result_mode(type_schema("number")),
            &[relay(), init(), submit(json!(1))],
        );
        let cancellation = fixture.invocation.as_ref().unwrap().cancellation().clone();
        let process_path = fixture.process.clone();
        let task = tokio::spawn(run_scripted(fixture, PendingClock, worker));
        validating.recv().await.unwrap();
        let process = fixture_process(&process_path);

        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        let (_, outcome, _) = task.await.unwrap();
        assert_user_cancelled(outcome);
        assert!(stopped.load(Ordering::SeqCst));
        assert!(quiesced.load(Ordering::SeqCst));
        assert!(process_group_is_quiescent(process));
    })
    .await;
}

#[tokio::test]
async fn cancellation_after_an_accepted_result_forces_containment_quiescent() {
    with_watchdog(async {
        let mut fixture = ProcessFixture::scripted(
            result_mode(type_schema("number")),
            &[
                relay(),
                init(),
                submit(json!(1)),
                successful_result("submitted"),
                json!("catch_interrupt"),
                json!("ready"),
                json!("park"),
            ],
        );
        let invocation = fixture.invocation.take().unwrap();
        let cancellation = invocation.cancellation().clone();
        let process_control = invocation.process_control().clone();
        let (task, _start, outcome) = start_adapter(
            invocation,
            fixture.diagnostics.clone(),
            PendingClock,
            InlineValidationWorker,
        );
        assert_eq!(fixture.ready.receive().await, b"ready\n");
        let process = fixture_process(&fixture.process);
        assert!(
            !task.is_finished(),
            "an accepted result must not end the step while Claude Code runs"
        );

        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        process_control.interrupt();
        process_control.force();
        task.await.unwrap();
        assert_user_cancelled(outcome.await.unwrap());
        assert!(process_group_is_quiescent(process));
    })
    .await;
}

// REQ-AGENT-22, REQ-AGENT-23, REQ-AGENT-24
// INV-HARNESS-3
#[tokio::test]
async fn normal_driver_preserves_runner_configuration_and_returns_only_final_response() {
    let fixture = ProcessFixture::new(
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
        1024,
    );
    let (fixture, outcome) = run_fixture(fixture).await;
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("response driver must complete");
    };
    assert_eq!(response.as_str(), RESPONSE);

    let arguments = std::fs::read(&fixture.arguments).unwrap();
    let arguments = arguments
        .split(|byte| *byte == 0)
        .filter(|argument| !argument.is_empty())
        .map(OsStr::from_bytes)
        .collect::<Vec<_>>();
    assert_eq!(arguments.len(), 20);
    for rejected in [
        "--bare",
        "--continue",
        "--resume",
        "--fork-session",
        "--fallback-model",
        "--replay-user-messages",
        "--worktree",
        "--no-session-persistence",
    ] {
        assert!(!arguments.contains(&OsStr::new(rejected)));
    }
    assert_eq!(
        arguments[..19],
        [
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--forward-subagent-text",
            "--session-id",
            fixture.session_id.as_ref(),
            "--permission-mode",
            "bypassPermissions",
            "--setting-sources",
            "user,project,local",
            "--model",
            MODEL,
            "--effort",
            "xhigh",
            "--append-system-prompt-file",
        ]
        .map(OsStr::new)
    );
    assert_eq!(
        std::fs::read(&fixture.prompt).unwrap(),
        b"exact system prompt\n"
    );
    assert_eq!(
        std::fs::read_to_string(&fixture.cwd).unwrap(),
        format!("{}\n", fixture.expected_cwd.display())
    );
    assert_eq!(
        std::fs::read_to_string(&fixture.environment).unwrap(),
        format!(
            "runner=runner exact\nconfig={}\nproject_dir_name=unset\nupdates=1\ntraffic=1\nmarketplace=1\nmemory=1\ngit=1\nexit_after_stop=5000\n",
            fixture
                ._temporary
                .path()
                .join("runner-claude-config")
                .display()
        )
    );
    let input = std::fs::read(&fixture.input).unwrap();
    assert_eq!(input.last(), Some(&b'\n'));
    assert_eq!(input.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert_eq!(
        std::fs::read(fixture.native_session.join("transcript.jsonl")).unwrap(),
        b"{malformed retained transcript"
    );
    assert_eq!(
        std::fs::read(
            fixture
                .native_session
                .join("resources/subagents/agent-fixture.jsonl")
        )
        .unwrap(),
        b"retained subagent activity\n"
    );
    // The adapter created the ambient project path under a configuration
    // directory that had none, and removed it with its links (REQ-GRANT-37).
    assert!(!fixture.config.join("projects").exists());
    assert_eq!(std::fs::read_dir(&fixture.config).unwrap().count(), 0);

    let observations = fixture.observations.snapshot();
    assert_eq!(
        observations
            .iter()
            .filter(|observation| matches!(
                observation.observation(),
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::HarnessStarted
                }
            ))
            .count(),
        1
    );
    assert_eq!(
        observations
            .iter()
            .filter_map(|observation| match observation.observation() {
                AgentObservation::AssistantText { text } => Some(text.as_ref()),
                _ => None,
            })
            .collect::<String>(),
        RESPONSE
    );
    assert!(!observations.iter().any(|observation| matches!(
        observation.observation(),
        AgentObservation::UnrecognizedHarnessEvent { .. }
    )));
    let diagnostic = fixture.diagnostics.get("agent-step").unwrap();
    assert_eq!(diagnostic.standard_error().bytes(), b"fixture diagnostic\n");
    assert!(diagnostic.standard_error().fully_drained());
}

#[tokio::test]
async fn a_scripted_harness_that_writes_nowhere_emits_the_named_capture_diagnostic() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let session = fixture.session_id.to_string();
    let script = production_transcript_script(
        &[json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "terminal_reason": "completed",
            "result": "terminal convenience text",
            "session_id": session,
        })],
        false,
        0,
        &session,
    );
    std::fs::write(
        fixture.invocation.as_ref().unwrap().adapter().executable(),
        script,
    )
    .unwrap();

    let (fixture, outcome) = run_fixture(fixture).await;
    assert_eq!(
        outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
    assert!(!fixture.native_session.join("transcript.jsonl").exists());
    let observations = fixture.observations.snapshot();
    let quiescent = observations
        .iter()
        .position(|observation| {
            matches!(
                observation.observation(),
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::HarnessQuiescent,
                }
            )
        })
        .unwrap();
    let diagnostic = observations
        .iter()
        .position(|observation| {
            matches!(
                observation.observation(),
                AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Warning,
                    message,
                } if message
                    .strip_prefix(NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC)
                    .is_some_and(|detail| detail.starts_with(":"))
            )
        })
        .unwrap();
    assert!(diagnostic > quiescent);
}

// REQ-AGENT-39, REQ-AGENT-40
// INV-HARNESS-5
#[tokio::test]
async fn production_failure_matrix_uses_only_existing_typed_outcomes() {
    let successful = json!({"type": "result", "subtype": "success", "is_error": false,
        "terminal_reason": "completed", "num_turns": 1, "result": "terminal text"});
    let native_failure = json!({"type": "result", "subtype": "error_during_execution",
        "is_error": true, "terminal_reason": "error_during_execution", "num_turns": 1});
    let unsuccessful_exit = failed_agent_outcome(AgentFailureCause::HarnessFailed {
        detail: AgentHarnessFailureDetail::UnsuccessfulExit,
    });
    let cases = [
        // A malformed stream fails nothing.
        (
            vec![successful.clone()],
            true,
            0,
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
        ),
        (
            vec![native_failure],
            false,
            17,
            failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::ModelError,
            }),
        ),
        (vec![successful], false, 17, unsuccessful_exit.clone()),
        (Vec::new(), false, 0, unsuccessful_exit),
    ];
    for (events, malformed_tail, exit_status, expected) in cases {
        let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
        let session = fixture.session_id.to_string();
        let script = production_transcript_script(&events, malformed_tail, exit_status, &session);
        std::fs::write(
            fixture.invocation.as_ref().unwrap().adapter().executable(),
            script,
        )
        .unwrap();
        let (_, outcome, started) = run_fixture_allowing_start_failure(fixture).await;
        assert!(started, "fixture exit {exit_status}: {outcome:?}");
        assert_eq!(outcome, expected, "fixture exit {exit_status}");
    }

    let startup = ProcessFixture::new(AgentValueMode::None, 1024);
    std::fs::remove_file(startup.invocation.as_ref().unwrap().adapter().executable()).unwrap();
    let (_, startup_outcome, started) = run_fixture_allowing_start_failure(startup).await;
    assert!(!started);
    assert_eq!(
        startup_outcome,
        failed_agent_outcome(AgentFailureCause::start_failure(
            "claude process release",
            std::io::Error::from_raw_os_error(libc::ENOENT)
        ))
    );

    let validation = ProcessFixture::scripted(
        result_mode(type_schema("number")),
        &[
            relay(),
            init(),
            submit(json!(1)),
            successful_result("submitted"),
        ],
    );
    let (_, validation_outcome, _) = with_watchdog(run_scripted(
        validation,
        PendingClock,
        FailingValidationWorker,
    ))
    .await;
    assert_eq!(
        validation_outcome,
        failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed)
    );
}

#[tokio::test]
async fn cancellation_before_launch_starts_no_process_or_diagnostic_capture() {
    let fixture = ProcessFixture::new(
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
        1024,
    );
    let cancellation = fixture.invocation.as_ref().unwrap().cancellation().clone();
    assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
    let (fixture, outcome, started) = run_fixture_allowing_start_failure(fixture).await;

    assert_user_cancelled(outcome);
    assert!(!started);
    assert!(!fixture.arguments.exists());
    assert!(fixture.diagnostics.get("agent-step").is_none());
}

#[tokio::test]
async fn cancellation_during_native_work_drains_and_quiesces_before_reporting() {
    with_watchdog(async {
        let mut fixture = ProcessFixture::new(
            AgentValueMode::Response {
                output: Arc::from("response"),
            },
            1024,
        );
        std::fs::write(
            fixture.invocation.as_ref().unwrap().adapter().executable(),
            CANCELLATION_FAKE_CLAUDE,
        )
        .unwrap();
        let invocation = fixture.invocation.take().unwrap();
        let cancellation = invocation.cancellation().clone();
        let process_path = fixture.arguments.clone();
        let ready = fixture.ready.clone();
        let interrupted = fixture.interrupted.clone();
        let (task, start, outcome) = start_process_fixture(invocation, fixture.diagnostics.clone());

        start.receive().await.unwrap();
        assert_eq!(ready.receive().await, b"ready\n");
        let process = fixture_process(&process_path);
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        assert_eq!(interrupted.receive().await, b"interrupted\n");
        assert_user_cancelled(outcome.await.unwrap());
        task.await.unwrap();
        assert!(process_group_is_quiescent(process));
        assert!(
            fixture
                .diagnostics
                .get("agent-step")
                .unwrap()
                .standard_error()
                .fully_drained()
        );
    })
    .await;
}

// REQ-AGENT-46
// INV-HARNESS-8
#[tokio::test]
async fn forced_cancellation_removes_a_stubborn_in_group_descendant() {
    with_watchdog(async {
        let mut fixture = ProcessFixture::new(AgentValueMode::None, 1024);
        std::fs::write(
            fixture.invocation.as_ref().unwrap().adapter().executable(),
            STUBBORN_DESCENDANT_FAKE_CLAUDE,
        )
        .unwrap();
        let invocation = fixture.invocation.take().unwrap();
        let cancellation = invocation.cancellation().clone();
        let process_control = invocation.process_control().clone();
        let leader_path = fixture.arguments.clone();
        let descendant_path = fixture.cwd.clone();
        let descendant_ready = fixture.descendant_ready.clone();
        let interrupted = fixture.interrupted.clone();
        let (task, start, outcome) = start_process_fixture(invocation, fixture.diagnostics.clone());

        start.receive().await.unwrap();
        assert_eq!(descendant_ready.receive().await, b"ready\n");
        let leader = fixture_process(&leader_path);
        let descendant = fixture_process(&descendant_path);
        assert_eq!(getpgid(Some(leader)).unwrap(), leader);
        assert_eq!(getpgid(Some(descendant)).unwrap(), leader);

        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        process_control.interrupt();
        assert_eq!(interrupted.receive().await, b"interrupted\n");
        assert!(!process_group_is_quiescent(leader));
        assert_eq!(getpgid(Some(descendant)).unwrap(), leader);
        process_control.force();
        assert_user_cancelled(outcome.await.unwrap());
        task.await.unwrap();
        assert!(process_group_is_quiescent(leader));
    })
    .await;
}

#[tokio::test]
async fn no_value_and_over_limit_runs_report_one_existing_terminal_outcome() {
    let no_value = ProcessFixture::new(AgentValueMode::None, 1024);
    let (_, outcome) = run_fixture(no_value).await;
    assert_eq!(
        outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );

    let over_limit = ProcessFixture::new(
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
        3,
    );
    let (_, outcome) = run_fixture(over_limit).await;
    assert_eq!(
        outcome,
        failed_agent_outcome(AgentFailureCause::CapturedValueTooLarge)
    );
}
