use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::io::{BufRead as _, Write as _};
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nix::sys::stat::Mode;
use nix::unistd::mkfifo;
use rustix::process::{Pid, getpgid};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};

use super::adapter::{
    ClaudeCodeStreamJsonV1Adapter, MAXIMUM_INLINE_ATTACHMENT_FRAME_BYTES,
    NATIVE_TRANSCRIPT_CAPTURE_MISSING_DIAGNOSTIC, native_project_slug, prepare_launch,
};
use super::test_support::{
    FixtureSignal, PendingClock, RecordingObservationSink, admitted_adapter, invocation_identity,
};
use super::*;
use crate::workflow::admission::{CancellationReason, CancellationSource, EnvironmentSnapshot};
use crate::workflow::agent::{
    AgentAdapter, AgentCompatibilityProfile, AgentInvocation, AgentInvocationLimits,
    AgentInvocationStaging, AgentProcessContext, AgentPrompt, AgentStartReceiver, AgentValueMode,
    PositiveDuration, RetainedJsonSchema, StagedAgentAttachment, agent_start_channel,
};
use crate::workflow::agent_diagnostics::{AgentDiagnosticSession, AgentDiagnosticSessionStore};
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::diagnostic::StepDiagnosticLog;
use crate::workflow::execution_root::AdmittedExecutionRoot;
use crate::workflow::observation::NoopExecutionObserver;
use crate::workflow::private_staging::open_directory_path;
use crate::workflow::process_group::{ProcessGuardRegistry, process_group_is_quiescent};
use crate::workflow::result_validation::ResultValidationWorker;
use crate::workflow::test_support::{
    host_scaled_watchdog, process_fixture_interrupt_receiver, spawn_process_fixture,
    write_process_fixture_id, write_process_fixture_signal,
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
} > "$CLAUDE_FIXTURE_ENVIRONMENT"
printf 'fixture diagnostic\n' >&2
output=$(
printf '{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.310"}\n' "$PWD" "$session" "$model"
printf '{"type":"system","subtype":"status","status":"requesting","session_id":"%s"}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_start","message":{"id":"msg-driver","type":"message","role":"assistant","content":[],"model":"%s","usage":{"input_tokens":1,"output_tokens":0}}},"session_id":"%s","parent_tool_use_id":null}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"driver response"}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"assistant","message":{"id":"msg-driver","type":"message","role":"assistant","content":[{"type":"text","text":"driver response"}],"model":"%s"},"parent_tool_use_id":null,"session_id":"%s"}\n' "$model" "$session"
printf '{"type":"stream_event","event":{"type":"content_block_stop","index":0},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"stream_event","event":{"type":"message_stop"},"session_id":"%s","parent_tool_use_id":null}\n' "$session"
printf '{"type":"result","subtype":"success","is_error":false,"terminal_reason":"completed","result":"convenience duplicate","session_id":"%s"}\n' "$session"
)
printf '%s\n' "$output"
cat >> "$CLAUDE_FIXTURE_INPUT"
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

const BACKGROUND_SUBAGENT_FAKE_CLAUDE: &str = r#"#!/bin/sh
exec "$CLAUDE_FIXTURE_PROCESS_HELPER" \
  --exact workflow::claude_code::adapter_tests::background_subagent_response_process_fixture \
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
    ready: FixtureSignal,
    descendant_ready: FixtureSignal,
    interrupted: FixtureSignal,
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
        let executable = temporary.path().join("claude");
        std::fs::write(&executable, FAKE_CLAUDE).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();

        let arguments = controls.join("arguments");
        let input = controls.join("input");
        let prompt = controls.join("prompt");
        let captured_cwd = controls.join("cwd");
        let environment = controls.join("environment");
        let ready = FixtureSignal::create(controls.join("ready"));
        let descendant_ready = FixtureSignal::create(controls.join("descendant-ready"));
        let interrupted = FixtureSignal::create(controls.join("interrupted"));
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
        ]);
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
        // The normal-driver and result-driver fixtures intentionally construct distinct
        // production invocations so their stdin closure and correction scripts cannot mix.
        let invocation = AgentInvocation::new(
            identity,
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(
                working_directory,
                EnvironmentSnapshot::new(runner_environment),
            ),
            AgentInvocationStaging::new(result_endpoint),
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
            ready,
            descendant_ready,
            interrupted,
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
    let adapter = ClaudeCodeStreamJsonV1Adapter::with_worker(
        diagnostics,
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
    (task, start, outcome)
}

fn assert_user_cancelled(outcome: AgentOutcome) {
    assert_eq!(
        outcome,
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest,
        }
    );
}

fn assert_agent_failure(outcome: &AgentOutcome, cause: AgentFailureCause) {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected agent failure, got {outcome:?}");
    };
    assert_eq!(failure.cause(), &cause);
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
printf '{{"type":"system","subtype":"init","cwd":"%s","session_id":"%s","model":"%s","permissionMode":"bypassPermissions","claude_code_version":"2.1.310"}}\n' "$PWD" "$session" "$model"
{output}exit {exit_status}
"#
    )
}

async fn invoke_fixture_adapter<Adapter>(
    invocation: TestInvocation,
    adapter: Adapter,
) -> AgentOutcome
where
    Adapter: AgentAdapter,
{
    let (started, start) = agent_start_channel();
    let (terminal, outcome) = tokio::sync::oneshot::channel();
    let _ = terminal.send(adapter.invoke(invocation, started).await);
    let outcome = outcome.await.unwrap();
    assert_eq!(
        start.receive().await,
        Ok(()),
        "terminal outcome: {outcome:?}"
    );
    outcome
}

const RESULT_FAKE_CLAUDE: &str = r#"#!/bin/sh
set -eu
printf '%s\n' "$$" > "$CLAUDE_RESULT_PROCESS"
printf 'ready\n' > "$CLAUDE_RESULT_PROCESS_READY"
IFS= read -r initial
printf '%s\n' "$initial" > "$CLAUDE_RESULT_INPUT"
cat "$CLAUDE_RESULT_FIRST"
if [ "${CLAUDE_RESULT_READ_FEEDBACK:-0}" = 1 ]; then
  IFS= read -r feedback
  printf '%s\n' "$feedback" >> "$CLAUDE_RESULT_INPUT"
fi
if [ -s "$CLAUDE_RESULT_SECOND" ]; then
  cat "$CLAUDE_RESULT_SECOND"
fi
if [ "${CLAUDE_RESULT_MODE:-settle}" = exit-after-feedback ]; then
  exit 0
fi
cat >/dev/null
case "${CLAUDE_RESULT_MODE:-settle}" in
  settle | background-subagent) ;;
  delayed-exit)
    printf 'ready\n' > "$CLAUDE_RESULT_SETTLEMENT_READY"
    IFS= read -r released < "$CLAUDE_RESULT_SETTLEMENT_RELEASE"
    ;;
  post-work) cat "$CLAUDE_RESULT_POST_WORK" ;;
  epilogue)
    sed '$d' "$CLAUDE_RESULT_POST_WORK"
    printf 'ready\n' > "$CLAUDE_RESULT_SETTLEMENT_READY"
    IFS= read -r released < "$CLAUDE_RESULT_SETTLEMENT_RELEASE"
    tail -n 1 "$CLAUDE_RESULT_POST_WORK"
    ;;
  hang)
    trap '' INT TERM
    IFS= read -r unexpected < "$CLAUDE_RESULT_SETTLEMENT_RELEASE"
    exit 75
    ;;
  *) exit 74 ;;
esac
"#;

#[derive(Clone)]
struct ResultExchangeFixture {
    candidate: Option<Value>,
    structured_tool_count: usize,
    sibling_tool_use: bool,
}

impl ResultExchangeFixture {
    fn standalone(candidate: Value) -> Self {
        Self {
            candidate: Some(candidate),
            structured_tool_count: 1,
            sibling_tool_use: false,
        }
    }
}

#[derive(Clone, Copy)]
enum ResultFixtureMode {
    Settle,
    DelayedExit,
    ExitAfterFeedback,
    PostWork,
    Hang,
    /// The first exchange launches a backgrounded subagent before its candidate; after
    /// the feedback frame the agent ends a turn without a candidate, the subagent's
    /// notification arrives, and the notified turn submits the second candidate.
    BackgroundSubagent,
    /// The first exchange launches a backgrounded subagent that settles before the
    /// candidate; after acceptance the turn its notification announced (an epilogue)
    /// opens, holds until the case releases it, and ends with its `result`.
    Epilogue,
}

impl ResultFixtureMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Settle => "settle",
            Self::DelayedExit => "delayed-exit",
            Self::ExitAfterFeedback => "exit-after-feedback",
            Self::PostWork => "post-work",
            Self::Hang => "hang",
            Self::BackgroundSubagent => "background-subagent",
            Self::Epilogue => "epilogue",
        }
    }
}

struct ResultProcessFixture {
    _temporary: tempfile::TempDir,
    invocation: Option<TestInvocation>,
    observations: RecordingObservationSink,
    captured_input: PathBuf,
    process: PathBuf,
    process_ready: FixtureSignal,
    settlement_ready: FixtureSignal,
    settlement_release: PathBuf,
}

impl ResultProcessFixture {
    fn new(
        schema: RetainedJsonSchema,
        first: ResultExchangeFixture,
        second: Option<ResultExchangeFixture>,
        mode: ResultFixtureMode,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let cwd = execution_root.join("worktree");
        let staging = temporary.path().join("staging");
        let result_endpoint = staging.join("result-endpoint");
        let controls = temporary.path().join("controls");
        let home = temporary.path().join("home");
        let config = temporary.path().join("claude-config");
        for directory in [&cwd, &staging, &result_endpoint, &controls, &home, &config] {
            std::fs::create_dir_all(directory).unwrap();
        }
        let executable = temporary.path().join("claude");
        std::fs::write(&executable, RESULT_FAKE_CLAUDE).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();

        let admitted_root = AdmittedExecutionRoot::admit(&execution_root).unwrap();
        let working_directory = admitted_root
            .select_working_directory(Some("worktree"))
            .unwrap();
        let expected_cwd = working_directory.protocol_path().unwrap();
        let first_path = controls.join("first.jsonl");
        let second_path = controls.join("second.jsonl");
        let post_work_path = controls.join("post-work.jsonl");
        let mut first_values = result_exchange_values(&expected_cwd, 1, &first);
        let mut second_values = second.as_ref().map_or_else(Vec::new, |exchange| {
            result_exchange_values(&expected_cwd, 2, exchange)
        });
        if matches!(mode, ResultFixtureMode::BackgroundSubagent) {
            first_values.splice(1..1, backgrounded_subagent_launch(RESULT_FIXTURE_SESSION));
            let waiting = ResultExchangeFixture {
                candidate: None,
                structured_tool_count: 0,
                sibling_tool_use: false,
            };
            let mut paused = result_exchange_values(&expected_cwd, 2, &waiting);
            paused.push(backgrounded_subagent_notification(RESULT_FIXTURE_SESSION));
            second_values.splice(0..0, paused);
        }
        let mut post_work = Vec::new();
        if matches!(mode, ResultFixtureMode::Epilogue) {
            let mut launch = backgrounded_subagent_launch(RESULT_FIXTURE_SESSION);
            launch.push(backgrounded_subagent_notification(RESULT_FIXTURE_SESSION));
            first_values.splice(1..1, launch);
            let waiting = ResultExchangeFixture {
                candidate: None,
                structured_tool_count: 0,
                sibling_tool_use: false,
            };
            post_work = result_exchange_values(&expected_cwd, 2, &waiting);
            post_work.splice(
                1..1,
                fixture_stream_message(
                    RESULT_FIXTURE_SESSION,
                    "msg-epilogue",
                    json!({"type": "text", "text": "noted"}),
                    "end_turn",
                ),
            );
        }
        std::fs::write(&first_path, framed_result_values(&first_values)).unwrap();
        std::fs::write(&second_path, framed_result_values(&second_values)).unwrap();
        if post_work.is_empty() {
            post_work.push(json!({
                "type": "stream_event",
                "event": {
                    "type": "message_start",
                    "message": {
                        "id": "msg-post-acceptance",
                        "type": "message",
                        "role": "assistant",
                        "content": [],
                        "model": MODEL,
                        "usage": {"input_tokens": 1, "output_tokens": 0},
                    },
                },
                "session_id": RESULT_FIXTURE_SESSION,
                "parent_tool_use_id": null,
            }));
        }
        std::fs::write(&post_work_path, framed_result_values(&post_work)).unwrap();
        let captured_input = controls.join("input.jsonl");
        let process = controls.join("process.pid");
        let process_ready = FixtureSignal::create(controls.join("process.ready"));
        let settlement_ready = FixtureSignal::create(controls.join("settlement.ready"));
        let settlement_release = controls.join("settlement.release");
        mkfifo(&settlement_release, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let runner_environment = EnvironmentSnapshot::new([
            (
                OsString::from("PATH"),
                std::env::var_os("PATH").unwrap_or_else(|| OsString::from("/usr/bin:/bin")),
            ),
            (OsString::from("HOME"), home.into_os_string()),
            (OsString::from("CLAUDE_CONFIG_DIR"), config.into_os_string()),
            (
                OsString::from("CLAUDE_RESULT_INPUT"),
                captured_input.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_PROCESS"),
                process.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_PROCESS_READY"),
                process_ready.path().as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_SETTLEMENT_READY"),
                settlement_ready.path().as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_SETTLEMENT_RELEASE"),
                settlement_release.as_os_str().to_owned(),
            ),
            (
                OsString::from("CLAUDE_RESULT_FIRST"),
                first_path.into_os_string(),
            ),
            (
                OsString::from("CLAUDE_RESULT_SECOND"),
                second_path.into_os_string(),
            ),
            (
                OsString::from("CLAUDE_RESULT_POST_WORK"),
                post_work_path.into_os_string(),
            ),
            (
                OsString::from("CLAUDE_RESULT_READ_FEEDBACK"),
                OsString::from(
                    if second.is_some() || matches!(mode, ResultFixtureMode::ExitAfterFeedback) {
                        "1"
                    } else {
                        "0"
                    },
                ),
            ),
            (
                OsString::from("CLAUDE_RESULT_MODE"),
                OsString::from(mode.as_str()),
            ),
        ]);
        let observations = RecordingObservationSink::default();
        let invocation = AgentInvocation::new(
            invocation_identity("run-claude-result", "result-step"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, runner_environment),
            AgentInvocationStaging::new(result_endpoint),
            AgentDiagnosticSession::claude_code_fixture(
                temporary.path().join("diagnostics/session"),
            ),
            AgentPrompt::new(Arc::from("system"), Arc::from("produce one result")),
            Arc::from([]),
            AgentValueMode::Result {
                output: Arc::from("result"),
                schema,
            },
            invocation_limits(1024),
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        Self {
            _temporary: temporary,
            invocation: Some(invocation),
            observations,
            captured_input,
            process,
            process_ready,
            settlement_ready,
            settlement_release,
        }
    }
}

const RESULT_FIXTURE_SESSION: &str = "00000000-0000-4000-8000-000000000001";

fn result_exchange_values(
    expected_cwd: &std::path::Path,
    exchange: u64,
    fixture: &ResultExchangeFixture,
) -> Vec<Value> {
    let stream_event = |event| {
        json!({
            "type": "stream_event",
            "event": event,
            "session_id": RESULT_FIXTURE_SESSION,
            "parent_tool_use_id": null,
        })
    };
    let mut values = vec![json!({
        "type": "system",
        "subtype": "init",
        "cwd": expected_cwd,
        "session_id": RESULT_FIXTURE_SESSION,
        "model": MODEL,
        "permissionMode": "bypassPermissions",
        "claude_code_version": QUALIFICATION_VERSION,
    })];
    if let Some(candidate) = &fixture.candidate {
        values.push(stream_event(json!({
            "type": "message_start",
            "message": {
                "id": format!("msg-result-{exchange}"),
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": MODEL,
                "usage": {"input_tokens": 1, "output_tokens": 0},
            },
        })));
        for index in 0..fixture.structured_tool_count {
            let call_id = format!("tool-result-{exchange}-{index}");
            values.extend([
                stream_event(json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {
                        "type": "tool_use",
                        "id": call_id,
                        "name": "StructuredOutput",
                        "input": candidate,
                    },
                })),
                json!({
                    "type": "assistant",
                    "message": {
                        "id": format!("msg-result-{exchange}"),
                        "type": "message",
                        "role": "assistant",
                        "content": [{
                            "type": "tool_use",
                            "id": call_id,
                            "name": "StructuredOutput",
                            "input": candidate,
                        }],
                        "model": MODEL,
                    },
                    "parent_tool_use_id": null,
                    "session_id": RESULT_FIXTURE_SESSION,
                }),
                stream_event(json!({
                    "type": "content_block_stop",
                    "index": index,
                })),
            ]);
            values.push(json!({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": call_id,
                        "content": "structurally correlated acknowledgement",
                    }],
                },
                "parent_tool_use_id": null,
                "session_id": RESULT_FIXTURE_SESSION,
                "tool_use_result": "structurally correlated acknowledgement",
            }));
        }
        let next_index = fixture.structured_tool_count;
        // Thinking and text siblings are observational and leave a candidate standalone;
        // another tool call beside it is other native work and makes it ambiguous.
        if fixture.sibling_tool_use {
            values.extend([
                stream_event(json!({
                    "type": "content_block_start",
                    "index": next_index,
                    "content_block": {
                        "type": "tool_use",
                        "id": format!("tool-sibling-{exchange}"),
                        "name": "Bash",
                        "input": {"command": "true"},
                    },
                })),
                stream_event(json!({
                    "type": "content_block_stop",
                    "index": next_index,
                })),
            ]);
        }
        values.extend([
            stream_event(json!({
                "type": "message_delta",
                "delta": {"stop_reason": "tool_use"},
                "usage": {"output_tokens": 1},
            })),
            stream_event(json!({"type": "message_stop"})),
        ]);
    }
    let mut result = json!({
        "type": "result",
        "subtype": "success",
        "is_error": false,
        "terminal_reason": "completed",
        "result": "structured result convenience text",
        "session_id": RESULT_FIXTURE_SESSION,
    });
    if let Some(candidate) = &fixture.candidate {
        result["structured_output"] = candidate.clone();
    }
    values.push(result);
    values
}

fn framed_result_values(values: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        serde_json::to_writer(&mut bytes, value).unwrap();
        bytes.push(b'\n');
    }
    bytes
}

/// A main-thread assistant message streamed as Claude Code emits it.
fn fixture_stream_message(session: &str, id: &str, block: Value, stop_reason: &str) -> Vec<Value> {
    let stream_event = |event| {
        json!({
            "type": "stream_event",
            "event": event,
            "session_id": session,
            "parent_tool_use_id": null,
        })
    };
    let delta = match block["type"].as_str() {
        Some("text") => json!({"type": "text_delta", "text": block["text"]}),
        _ => json!({"type": "input_json_delta", "partial_json": "{}"}),
    };
    let mut start = block;
    if start["type"] == "text" {
        start["text"] = json!("");
    }
    vec![
        stream_event(json!({
            "type": "message_start",
            "message": {
                "id": id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": MODEL,
                "usage": {"input_tokens": 1, "output_tokens": 0},
            },
        })),
        stream_event(json!({"type": "content_block_start", "index": 0, "content_block": start})),
        stream_event(json!({"type": "content_block_delta", "index": 0, "delta": delta})),
        stream_event(json!({"type": "content_block_stop", "index": 0})),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
    ]
}

/// The main-thread fragment that launches a backgrounded subagent: the Agent call, the
/// backgrounded `task_started`, and the call's immediate tool result.
fn backgrounded_subagent_launch(session: &str) -> Vec<Value> {
    let mut values = fixture_stream_message(
        session,
        "msg-agent-call",
        json!({"type": "tool_use", "id": "agent-call", "name": "Agent", "input": {}}),
        "tool_use",
    );
    values.extend([
        json!({"type": "system", "subtype": "task_started", "task_id": "t1",
            "tool_use_id": "agent-call", "task_type": "local_agent",
            "description": "Review the diff", "subagent_type": "Explore",
            "is_backgrounded": true, "session_id": session}),
        json!({"type": "user", "session_id": session, "parent_tool_use_id": null,
            "message": {"role": "user", "content": [{"type": "tool_result",
                "tool_use_id": "agent-call", "content": "Async agent launched",
                "is_error": false}]}}),
    ]);
    values
}

fn backgrounded_subagent_notification(session: &str) -> Value {
    json!({"type": "system", "subtype": "task_notification", "task_id": "t1",
        "tool_use_id": "agent-call", "status": "completed", "summary": "No issues",
        "session_id": session})
}

fn retained_schema(schema: Value) -> RetainedJsonSchema {
    let bytes = Arc::<[u8]>::from(serde_json::to_vec(&schema).unwrap());
    RetainedJsonSchema::compile(bytes, Arc::new(schema)).unwrap()
}

fn type_schema(root_type: &str) -> RetainedJsonSchema {
    retained_schema(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": root_type,
    }))
}

use crate::workflow::test_support::ControlledClock;
pub(super) use crate::workflow::test_support::InlineValidationWorker;
use crate::workflow::test_support::claude_validation::{
    FailingValidationWorker, PendingValidationWorker,
};

fn controlled_clock() -> (
    ControlledClock,
    mpsc::UnboundedReceiver<Duration>,
    watch::Sender<bool>,
) {
    let (registrations, registered) = mpsc::unbounded_channel();
    let (release, released) = watch::channel(false);
    (
        ControlledClock {
            registrations,
            release: released,
        },
        registered,
        release,
    )
}

async fn run_result_fixture<Clock, Worker>(
    mut fixture: ResultProcessFixture,
    clock: Clock,
    worker: Worker,
) -> (ResultProcessFixture, AgentOutcome)
where
    Clock: CoordinatorClock,
    Worker: ResultValidationWorker,
{
    let invocation = fixture.invocation.take().unwrap();
    let adapter = ClaudeCodeStreamJsonV1Adapter::with_worker(
        StepDiagnosticLog::default(),
        NonZeroU64::new(1024).unwrap(),
        clock,
        NoopExecutionObserver,
        worker,
    );
    let outcome = invoke_fixture_adapter(invocation, adapter).await;
    (fixture, outcome)
}

fn fixture_process(path: &std::path::Path) -> Pid {
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
    protocol.write_all(&framed_result_values(values)).unwrap();
    protocol.flush().unwrap();
}

/// Each release is one line the test writes to the `release` FIFO beside the ready signal.
fn await_fixture_release() {
    let ready = PathBuf::from(std::env::var_os("CLAUDE_FIXTURE_READY").unwrap());
    let release = std::fs::File::open(ready.with_file_name("release")).unwrap();
    let mut line = String::new();
    assert!(
        std::io::BufReader::new(release)
            .read_line(&mut line)
            .unwrap()
            > 0
    );
}

#[test]
#[ignore = "launched as the backgrounded-subagent Claude Code response fixture"]
fn background_subagent_response_process_fixture() {
    let session = std::env::var("CLAUDE_FIXTURE_SESSION").unwrap();
    write_fixture_init();
    let mut protocol = open_fixture_protocol();
    let mut first_turn = backgrounded_subagent_launch(&session);
    first_turn.extend(fixture_stream_message(
        &session,
        "msg-waiting",
        json!({"type": "text", "text": "waiting"}),
        "end_turn",
    ));
    first_turn.push(
        json!({"type": "result", "subtype": "success", "is_error": false,
        "terminal_reason": "completed", "result": "waiting", "session_id": session}),
    );
    write_fixture_frames(&mut protocol, &first_turn);
    await_fixture_release();
    write_fixture_frames(
        &mut protocol,
        &[json!({"type": "assistant", "session_id": session,
            "parent_tool_use_id": "agent-call", "agent_id": "agent-1",
            "subagent_type": "Explore", "task_description": "Review the diff",
            "uuid": "00000000-0000-4000-8000-0000000000a1",
            "timestamp": "2026-10-23T00:00:00Z",
            "message": {"id": "msg-sub-1", "type": "message", "role": "assistant",
                "model": MODEL, "content": [{"type": "text", "text": "No issues"}]}})],
    );
    await_fixture_release();
    // The adapter has processed both chunks; a closed input is readable at once.
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
    write_process_fixture_signal(
        "CLAUDE_FIXTURE_READY",
        if ready == 0 {
            &b"open\n"[..]
        } else {
            &b"closed\n"[..]
        },
    );
    let mut final_turn = vec![backgrounded_subagent_notification(&session)];
    write_fixture_frames(&mut protocol, &final_turn);
    write_fixture_initialization(&mut protocol, &session, None);
    final_turn = fixture_stream_message(
        &session,
        "msg-final",
        json!({"type": "text", "text": "final"}),
        "end_turn",
    );
    final_turn.push(
        json!({"type": "result", "subtype": "success", "is_error": false,
        "terminal_reason": "completed", "result": "final", "session_id": session}),
    );
    write_fixture_frames(&mut protocol, &final_turn);
    // The step settles only when the adapter closes the input after the final result.
    let mut remaining = Vec::new();
    std::io::Read::read_to_end(&mut standard_input.lock(), &mut remaining).unwrap();
    assert!(remaining.is_empty());
}

async fn with_watchdog<Output>(future: impl Future<Output = Output>) -> Output {
    host_scaled_watchdog(
        "ClaudeCodeStreamJsonV1 fixture",
        Duration::from_secs(10),
        future,
    )
    .await
}

#[test]
fn launch_plan_stages_exact_prompt_and_lf_delimited_input() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let plan = prepare_launch(fixture.invocation.as_ref().unwrap()).unwrap();
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
    assert_eq!(session_argument[1], OsStr::new(plan.session_id()));
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
        prepare_launch(fixture.invocation.as_ref().unwrap()),
        Err(AgentFailureCause::HarnessStartFailed { .. })
    ));
    assert_eq!(
        std::fs::read(existing).unwrap(),
        b"existing conversation sentinel"
    );
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
    let plan = prepare_launch(invocation).unwrap();
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
        prepare_launch(fixture.invocation.as_ref().unwrap())
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
    let plan = prepare_launch(invocation).unwrap();
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
        let release_input = fixture.ready.path().with_file_name("release");
        mkfifo(&release_input, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let invocation = fixture.invocation.take().unwrap();
        let (clock, _registered, expire) = controlled_clock();
        // Every ordinary deadline is already expired. A large initial frame must
        // still wait for the gated reader, rather than fail before system/init.
        expire.send_replace(true);
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
        tokio::task::spawn_blocking(move || std::fs::write(release_input, b"release\n"))
            .await.unwrap().unwrap();
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

// REQ-AGENT-32
#[tokio::test]
async fn permissive_native_envelope_preserves_every_authoritatively_valid_json_root() {
    let cases = [
        ("object", json!({"value": 1})),
        ("array", json!([1, 2])),
        ("string", json!("value")),
        ("number", json!(7)),
        ("boolean", json!(true)),
        ("null", Value::Null),
    ];
    for (root_type, candidate) in cases {
        let fixture = ResultProcessFixture::new(
            type_schema(root_type),
            ResultExchangeFixture::standalone(json!({"result": candidate})),
            None,
            ResultFixtureMode::Settle,
        );
        let plan = prepare_launch(fixture.invocation.as_ref().unwrap()).unwrap();
        assert_eq!(
            plan.arguments()[plan.arguments().len() - 2..],
            [
                OsStr::new("--json-schema"),
                OsStr::new(RESULT_ENVELOPE_SCHEMA)
            ]
        );
        drop(plan);
        let (_, outcome) = run_result_fixture(fixture, PendingClock, InlineValidationWorker).await;
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("{root_type} root must complete through authoritative validation");
        };
        assert_eq!(result.value(), &candidate);
    }
}

// REQ-AGENT-29
#[tokio::test]
async fn rejected_candidate_is_corrected_in_the_same_process_and_conversation() {
    let schema = retained_schema(json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "integer",
        "minimum": 1,
    }));
    let fixture = ResultProcessFixture::new(
        schema,
        ResultExchangeFixture::standalone(json!({"result": -1})),
        Some(ResultExchangeFixture::standalone(json!({"result": 7}))),
        ResultFixtureMode::Settle,
    );
    let (fixture, outcome) =
        run_result_fixture(fixture, PendingClock, InlineValidationWorker).await;
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
        panic!("corrected candidate must complete");
    };
    assert_eq!(result.value(), &json!(7));

    let input = std::fs::read_to_string(&fixture.captured_input).unwrap();
    let frames = input
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(frames.len(), 2);
    let feedback = frames[1]["message"]["content"][0]["text"].as_str().unwrap();
    assert!(feedback.starts_with("Result rejected by the workflow schema:"));
    assert!(feedback.len() <= 512);
    let rejections = fixture
        .observations
        .snapshot()
        .into_iter()
        .filter(|observation| {
            matches!(
                observation.observation(),
                AgentObservation::ValueRejected {
                    kind: AgentValueKind::Result,
                    ..
                }
            )
        })
        .count();
    assert_eq!(rejections, 1);
}

// REQ-AGENT-30
#[tokio::test]
async fn oversized_candidate_receives_bounded_feedback_before_a_valid_correction() {
    let fixture = ResultProcessFixture::new(
        type_schema("string"),
        ResultExchangeFixture::standalone(json!({"result": "x".repeat(1100)})),
        Some(ResultExchangeFixture::standalone(json!({"result": "ok"}))),
        ResultFixtureMode::Settle,
    );
    let (fixture, outcome) =
        run_result_fixture(fixture, PendingClock, InlineValidationWorker).await;
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
        panic!("bounded correction must complete");
    };
    assert_eq!(result.value(), &json!("ok"));
    let input = std::fs::read_to_string(&fixture.captured_input).unwrap();
    let feedback = serde_json::from_str::<Value>(input.lines().nth(1).unwrap()).unwrap();
    let feedback = feedback["message"]["content"][0]["text"].as_str().unwrap();
    assert!(feedback.contains("1024-byte limit"));
    assert!(feedback.len() <= 512);
}

// REQ-AGENT-31
#[tokio::test]
async fn ambiguous_duplicate_and_missing_candidates_never_become_results() {
    let ambiguous = [
        ResultExchangeFixture {
            candidate: Some(json!({"result": 1})),
            structured_tool_count: 1,
            sibling_tool_use: true,
        },
        ResultExchangeFixture {
            candidate: Some(json!({"result": 1})),
            structured_tool_count: 2,
            sibling_tool_use: false,
        },
    ];
    for first in ambiguous {
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            first,
            None,
            ResultFixtureMode::ExitAfterFeedback,
        );
        let (fixture, outcome) =
            run_result_fixture(fixture, PendingClock, InlineValidationWorker).await;
        assert!(matches!(
            outcome,
            AgentOutcome::Failed(failure)
                if matches!(failure.cause(), AgentFailureCause::HarnessProtocolFailed)
        ));
        let input = std::fs::read_to_string(&fixture.captured_input).unwrap();
        assert_eq!(input.lines().count(), 2);
        assert!(input.contains("standalone structured result candidate"));
    }

    let missing = ResultProcessFixture::new(
        type_schema("number"),
        ResultExchangeFixture {
            candidate: None,
            structured_tool_count: 0,
            sibling_tool_use: false,
        },
        None,
        ResultFixtureMode::Settle,
    );
    let (_, outcome) = run_result_fixture(missing, PendingClock, InlineValidationWorker).await;
    assert_eq!(
        outcome,
        failed_agent_outcome(AgentFailureCause::MissingResult)
    );
}

// REQ-AGENT-34
#[tokio::test]
async fn validation_timeout_stops_its_worker_and_discards_the_candidate() {
    let stopped = Arc::new(AtomicBool::new(false));
    let quiesced = Arc::new(AtomicBool::new(false));
    let worker = PendingValidationWorker {
        stopped: Arc::clone(&stopped),
        quiesced: Arc::clone(&quiesced),
    };
    let (clock, mut registered, release) = controlled_clock();
    let fixture = ResultProcessFixture::new(
        type_schema("number"),
        ResultExchangeFixture::standalone(json!({"result": 1})),
        None,
        ResultFixtureMode::Settle,
    );
    let task = tokio::spawn(run_result_fixture(fixture, clock, worker));
    assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
    release.send_replace(true);
    let (_, outcome) = task.await.unwrap();
    assert_eq!(
        outcome,
        failed_agent_outcome(AgentFailureCause::ResultValidationLimitExceeded {
            deadline: PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        })
    );
    assert!(stopped.load(Ordering::SeqCst));
    assert!(quiesced.load(Ordering::SeqCst));
}

#[tokio::test]
async fn accepted_result_closes_input_and_remains_provisional_until_process_exit() {
    with_watchdog(async {
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::DelayedExit,
        );
        let ready = fixture.settlement_ready.clone();
        let release = fixture.settlement_release.clone();
        let captured_input = fixture.captured_input.clone();
        let task = tokio::spawn(run_result_fixture(
            fixture,
            PendingClock,
            InlineValidationWorker,
        ));

        assert_eq!(ready.receive().await, b"ready\n");
        assert_eq!(
            std::fs::read_to_string(captured_input)
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert!(
            !task.is_finished(),
            "an accepted candidate must remain provisional before native exit"
        );
        std::fs::write(release, b"release\n").unwrap();

        let (_, outcome) = task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("clean settlement must publish the accepted result");
        };
        assert_eq!(result.value(), &json!(1));
    })
    .await;
}

// REQ-AGENT-33
// INV-HARNESS-7
#[tokio::test]
async fn post_acceptance_work_and_failed_settlement_discard_the_provisional_result() {
    with_watchdog(async {
        let post_work = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::PostWork,
        );
        let (_, post_work_outcome) =
            run_result_fixture(post_work, PendingClock, InlineValidationWorker).await;
        assert_agent_failure(&post_work_outcome, AgentFailureCause::HarnessProtocolFailed);
        let AgentOutcome::Failed(post_work_failure) = post_work_outcome else {
            panic!("fixture outcome was not failed");
        };
        let rejection =
            serde_json::to_value(post_work_failure.protocol_rejection().unwrap()).unwrap();
        assert_eq!(
            rejection["detail"]["reason"],
            "terminal_drain_event_invalid"
        );

        let (clock, mut registered, release) = controlled_clock();
        let nonsettling = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::Hang,
        );
        let task = tokio::spawn(run_result_fixture(
            nonsettling,
            clock,
            InlineValidationWorker,
        ));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        release.send_replace(true);
        let (_, settlement_outcome) = task.await.unwrap();
        assert_eq!(
            settlement_outcome,
            failed_agent_outcome(AgentFailureCause::ResultSettlementFailed)
        );
    })
    .await;
}

// REQ-AGENT-76
// INV-HARNESS-7
#[tokio::test]
async fn an_announced_epilogue_suspends_the_settlement_grace_and_the_accepted_result_stands() {
    with_watchdog(async {
        let (clock, mut registered, _release) = controlled_clock();
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::Epilogue,
        );
        let ready = fixture.settlement_ready.clone();
        let release = fixture.settlement_release.clone();
        let observations = fixture.observations.clone();
        let task = tokio::spawn(run_result_fixture(fixture, clock, InlineValidationWorker));
        // The validation deadline, then the grace armed at acceptance.
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));

        assert_eq!(ready.receive().await, b"ready\n");
        observations
            .wait_for(|observation| {
                matches!(
                    observation,
                    AgentObservation::AssistantText { text } if text.as_ref() == "noted"
                )
            })
            .await;
        assert!(
            registered.try_recv().is_err(),
            "no settlement wait may run while the epilogue is open"
        );
        std::fs::write(release, b"release\n").unwrap();

        let (_, outcome) = task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("the accepted result must stand after the epilogue, got {outcome:?}");
        };
        assert_eq!(result.value(), &json!(1));
        assert_eq!(
            registered.try_recv().ok(),
            Some(Duration::from_secs(1)),
            "the grace restarts in full once the epilogue has settled"
        );
    })
    .await;
}

// REQ-AGENT-35
// INV-HARNESS-7
#[tokio::test]
async fn premature_structured_result_is_returned_as_feedback_until_background_work_settles() {
    with_watchdog(async {
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            Some(ResultExchangeFixture::standalone(json!({"result": 7}))),
            ResultFixtureMode::BackgroundSubagent,
        );
        let (fixture, outcome) =
            run_result_fixture(fixture, PendingClock, InlineValidationWorker).await;
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("the notified turn's candidate must complete, got {outcome:?}");
        };
        assert_eq!(result.value(), &json!(7));

        let rejections = fixture
            .observations
            .snapshot()
            .into_iter()
            .filter_map(|observation| match observation.observation() {
                AgentObservation::ValueRejected {
                    kind: AgentValueKind::Result,
                    feedback,
                } => Some(Arc::clone(feedback)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let [feedback] = rejections.as_slice() else {
            panic!("exactly one premature rejection, got {rejections:?}");
        };
        // The feedback names the kind of work that blocks the result and the stop route.
        assert!(feedback.starts_with("Result rejected:"), "{feedback}");
        assert!(feedback.contains("subagent"), "{feedback}");
        assert!(feedback.contains("TaskStop"), "{feedback}");
        let input = std::fs::read_to_string(&fixture.captured_input).unwrap();
        let frames = input
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(frames.len(), 2);
        assert_eq!(
            frames[1]["message"]["content"][0]["text"],
            Value::from(&**feedback)
        );
    })
    .await;
}

// REQ-AGENT-35
#[tokio::test]
async fn response_mode_closes_standard_input_only_when_the_invocation_settles() {
    with_watchdog(async {
        let mut fixture = ProcessFixture::new(
            AgentValueMode::Response {
                output: Arc::from("response"),
            },
            1024,
        );
        let invocation = fixture.invocation.take().unwrap();
        std::fs::write(
            invocation.adapter().executable(),
            BACKGROUND_SUBAGENT_FAKE_CLAUDE,
        )
        .unwrap();
        // Keep a reader open while the child drains stdout between phases. Opening
        // a write-only FIFO here could block the runtime that must drain that output.
        let release = FixtureSignal::create(fixture.ready.path().with_file_name("release"));
        let observations = fixture.observations.clone();
        let (task, start, outcome) = start_process_fixture(invocation, fixture.diagnostics.clone());

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
        std::fs::write(release.path(), b"release\n").unwrap();
        observations
            .wait_for(|observation| {
                matches!(
                    observation,
                    AgentObservation::AssistantText { text } if text.as_ref() == "No issues"
                )
            })
            .await;
        std::fs::write(release.path(), b"release\n").unwrap();
        assert_eq!(
            fixture.ready.receive().await,
            b"open\n",
            "the first result left a subagent outstanding, so the input must stay open"
        );

        task.await.unwrap();
        let outcome = outcome.await.unwrap();
        assert!(start.receive().await.is_ok());
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
            panic!("the wakeup turn must complete the response, got {outcome:?}");
        };
        assert_eq!(response.as_str(), "final");
    })
    .await;
}

#[tokio::test]
async fn cancellation_during_validation_stops_the_worker_and_quiesces_the_process_group() {
    with_watchdog(async {
        let stopped = Arc::new(AtomicBool::new(false));
        let quiesced = Arc::new(AtomicBool::new(false));
        let worker = PendingValidationWorker {
            stopped: Arc::clone(&stopped),
            quiesced: Arc::clone(&quiesced),
        };
        let (clock, mut registered, _release) = controlled_clock();
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::Settle,
        );
        let cancellation = fixture.invocation.as_ref().unwrap().cancellation().clone();
        let process_path = fixture.process.clone();
        let process_ready = fixture.process_ready.clone();
        let task = tokio::spawn(run_result_fixture(fixture, clock, worker));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        assert_eq!(process_ready.receive().await, b"ready\n");
        let process = fixture_process(&process_path);

        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        let (_, outcome) = task.await.unwrap();
        assert_user_cancelled(outcome);
        assert!(stopped.load(Ordering::SeqCst));
        assert!(quiesced.load(Ordering::SeqCst));
        assert!(process_group_is_quiescent(process));
    })
    .await;
}

#[tokio::test]
async fn cancellation_after_a_provisional_result_forces_containment_quiescent() {
    with_watchdog(async {
        let (clock, mut registered, _release) = controlled_clock();
        let fixture = ResultProcessFixture::new(
            type_schema("number"),
            ResultExchangeFixture::standalone(json!({"result": 1})),
            None,
            ResultFixtureMode::Hang,
        );
        let invocation = fixture.invocation.as_ref().unwrap();
        let cancellation = invocation.cancellation().clone();
        let process_control = invocation.process_control().clone();
        let process_path = fixture.process.clone();
        let process_ready = fixture.process_ready.clone();
        let task = tokio::spawn(run_result_fixture(fixture, clock, InlineValidationWorker));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        assert_eq!(registered.recv().await, Some(Duration::from_secs(1)));
        assert_eq!(process_ready.receive().await, b"ready\n");
        let process = fixture_process(&process_path);
        assert!(
            !task.is_finished(),
            "the accepted result must remain provisional during settlement"
        );

        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        process_control.interrupt();
        process_control.force();

        let (_, outcome) = task.await.unwrap();
        assert_user_cancelled(outcome);
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
            "runner=runner exact\nconfig={}\nproject_dir_name=unset\nupdates=1\ntraffic=1\nmarketplace=1\nmemory=1\ngit=1\n",
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
    let ambient_project = fixture
        .config
        .join("projects")
        .join(native_project_slug(fixture.expected_cwd.to_str().unwrap()));
    assert_eq!(std::fs::read_dir(ambient_project).unwrap().count(), 0);

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
    enum Transcript {
        Malformed,
        Success,
        NativeFailure,
    }

    let cases = [
        (
            AgentValueMode::None,
            Transcript::Malformed,
            0,
            AgentFailureCause::HarnessProtocolFailed,
        ),
        (
            AgentValueMode::Response {
                output: Arc::from("response"),
            },
            Transcript::Success,
            0,
            AgentFailureCause::MissingResponse,
        ),
        (
            AgentValueMode::None,
            Transcript::NativeFailure,
            17,
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::ModelError,
            },
        ),
        (
            AgentValueMode::None,
            Transcript::Success,
            17,
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            },
        ),
    ];
    for (value_mode, transcript, exit_status, cause) in cases {
        let fixture = ProcessFixture::new(value_mode, 1024);
        let session = fixture
            .invocation
            .as_ref()
            .unwrap()
            .diagnostic_session()
            .claude_code_native_session_id()
            .unwrap()
            .to_owned();
        let (events, malformed_tail) = match transcript {
            Transcript::Malformed => (Vec::new(), true),
            Transcript::Success => (
                vec![json!({
                    "type": "result",
                    "subtype": "success",
                    "is_error": false,
                    "terminal_reason": "completed",
                    "result": "terminal convenience text",
                    "session_id": session,
                })],
                false,
            ),
            Transcript::NativeFailure => (
                vec![json!({
                    "type": "result",
                    "subtype": "error_during_execution",
                    "is_error": true,
                    "terminal_reason": "error_during_execution",
                    "session_id": session,
                })],
                false,
            ),
        };
        let script =
            production_transcript_script(&events, malformed_tail, exit_status, session.as_str());
        std::fs::write(
            fixture.invocation.as_ref().unwrap().adapter().executable(),
            script,
        )
        .unwrap();
        let (_, outcome, started) = run_fixture_allowing_start_failure(fixture).await;
        assert!(started, "fixture exit {exit_status}: {outcome:?}");
        assert_agent_failure(&outcome, cause);
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

    let validation = ResultProcessFixture::new(
        type_schema("number"),
        ResultExchangeFixture::standalone(json!({"result": 1})),
        None,
        ResultFixtureMode::Settle,
    );
    let (_, validation_outcome) =
        run_result_fixture(validation, PendingClock, FailingValidationWorker).await;
    assert_eq!(
        validation_outcome,
        failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed)
    );
}

// REQ-AGENT-42, REQ-AGENT-71
#[tokio::test]
async fn parser_rejection_is_surfaced_and_retained_beside_claude_invocation_metadata() {
    let fixture = ProcessFixture::new(AgentValueMode::None, 1024);
    let session = fixture
        .invocation
        .as_ref()
        .unwrap()
        .diagnostic_session()
        .claude_code_native_session_id()
        .unwrap()
        .to_owned();
    let rejected = json!({
        "type": "stream_event",
        "event": {
            "type": "content_block_delta",
            "index": 7,
            "delta": {
                "type": "text_delta",
                "text": "SENTINEL_ASSISTANT_TEXT",
                "tool_input": "SENTINEL_TOOL_INPUT",
            },
        },
        "session_id": session,
        "parent_tool_use_id": "SENTINEL_PARENT_ID",
        "provider_payload": "SENTINEL_PROVIDER_PAYLOAD",
        "request_id": "SENTINEL_REQUEST_ID",
        "structured_output": "SENTINEL_STRUCTURED_OUTPUT",
        "tool_result": "SENTINEL_TOOL_RESULT",
    });
    std::fs::write(
        fixture.invocation.as_ref().unwrap().adapter().executable(),
        production_transcript_script(std::slice::from_ref(&rejected), false, 0, session.as_str()),
    )
    .unwrap();

    let (fixture, outcome, started) = run_fixture_allowing_start_failure(fixture).await;
    assert!(started);
    assert_agent_failure(&outcome, AgentFailureCause::HarnessProtocolFailed);
    let AgentOutcome::Failed(failure) = &outcome else {
        panic!("fixture outcome was not failed");
    };
    let surfaced = serde_json::to_value(failure.protocol_rejection().unwrap()).unwrap();
    assert_eq!(surfaced["profile"], "ClaudeCodeStreamJsonV1");
    assert_eq!(
        surfaced["detail"]["reason"],
        "active_stream_parent_mismatch"
    );
    assert_eq!(surfaced["detail"]["outerEvent"], "stream_event");
    assert_eq!(surfaced["detail"]["streamEvent"], "content_block_delta");
    assert_eq!(surfaced["detail"]["contentIndex"], 7);
    assert_eq!(surfaced["detail"]["contentBlock"], "text");

    let retained_bytes =
        std::fs::read(fixture.diagnostic_directory.join("protocol-rejection.json")).unwrap();
    let mut retained: Value = serde_json::from_slice(&retained_bytes).unwrap();
    assert!(retained_bytes.len() < 64 * 1024);
    // Only the file keeps the stream excerpt and the tracked work beside the surfaced
    // diagnostic (REQ-AGENT-71, REQ-AGENT-74).
    let members = retained.as_object_mut().unwrap();
    let rejected_line = members.remove("rejectedLine").unwrap();
    let preceding_lines = members.remove("precedingLines").unwrap();
    let tracked_work = members.remove("trackedWork").unwrap();
    assert_eq!(
        tracked_work,
        serde_json::to_value(failure.tracked_work().unwrap()).unwrap()
    );
    assert_eq!(retained, surfaced);
    assert_eq!(
        rejected_line["text"],
        serde_json::to_string(&rejected).unwrap()
    );
    let preceding_lines = preceding_lines.as_array().unwrap();
    let preceding: Value =
        serde_json::from_str(preceding_lines.last().unwrap()["text"].as_str().unwrap()).unwrap();
    assert_eq!(preceding["subtype"], "init");
    assert_eq!(preceding["session_id"], session.as_str());
    let metadata: Value = serde_json::from_slice(
        &std::fs::read(fixture.diagnostic_directory.join("metadata.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        metadata["localRunId"],
        "00000000-0000-4000-8000-000000000001"
    );
    assert_eq!(metadata["attemptNumber"], 1);
    assert_eq!(metadata["stepId"], "agent-step");
    assert!(metadata["invocationId"].is_u64());
    assert_eq!(metadata["profile"], "ClaudeCodeStreamJsonV1");
    assert_eq!(metadata["claudeCodeVersion"], QUALIFICATION_VERSION);
    assert_eq!(
        metadata["nativeSession"],
        json!({
            "relativeDirectory": "session",
            "formatVersion": 1
        })
    );
    assert!(metadata.get("nativeSessionPersistence").is_none());

    let serialized = surfaced.to_string();
    for sentinel in [
        "SENTINEL_ASSISTANT_TEXT",
        "SENTINEL_TOOL_INPUT",
        "SENTINEL_PARENT_ID",
        "SENTINEL_PROVIDER_PAYLOAD",
        "SENTINEL_REQUEST_ID",
        "SENTINEL_STRUCTURED_OUTPUT",
        "SENTINEL_TOOL_RESULT",
        session.as_str(),
    ] {
        assert!(!serialized.contains(sentinel));
    }
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
