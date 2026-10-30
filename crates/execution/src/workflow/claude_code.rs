#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClaudeCodeConfig {
    pub(crate) model: String,
    pub(crate) effort: ClaudeCodeEffort,
}

// Claude effort is deliberately independent of Pi thinking because the harness contracts differ.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub(crate) enum ClaudeCodeEffort {
    #[serde(rename = "low")]
    Low,
    #[serde(rename = "medium")]
    Medium,
    #[serde(rename = "high")]
    High,
    #[serde(rename = "xhigh")]
    XHigh,
    #[serde(rename = "max")]
    Max,
}

impl ClaudeCodeEffort {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }
}

// Keep this direct decoder local so Claude's closed configuration can evolve independently.
pub(crate) fn resolve_config(value: &Value) -> Option<ClaudeCodeConfig> {
    let config = serde_json::from_value::<ClaudeCodeConfig>(value.clone()).ok()?;
    if config.model.is_empty() {
        return None;
    }

    Some(ClaudeCodeConfig {
        model: config.model,
        effort: config.effort,
    })
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn resolves_only_the_closed_nonempty_model_and_effort_shape() {
        for (effort, expected) in [
            ("low", ClaudeCodeEffort::Low),
            ("medium", ClaudeCodeEffort::Medium),
            ("high", ClaudeCodeEffort::High),
            ("xhigh", ClaudeCodeEffort::XHigh),
            ("max", ClaudeCodeEffort::Max),
        ] {
            assert_eq!(
                resolve_config(&serde_json::json!({
                    "model": "claude-opus-4-1",
                    "effort": effort,
                })),
                Some(ClaudeCodeConfig {
                    model: "claude-opus-4-1".to_owned(),
                    effort: expected,
                })
            );
        }

        for invalid in [
            serde_json::json!({"effort": "high"}),
            serde_json::json!({"model": "", "effort": "high"}),
            serde_json::json!({"model": "claude-opus-4-1"}),
            serde_json::json!({"model": "claude-opus-4-1", "effort": "off"}),
            serde_json::json!({
                "model": "claude-opus-4-1",
                "effort": "high",
                "fallbackModel": "claude-sonnet-4",
            }),
        ] {
            assert_eq!(resolve_config(&invalid), None);
        }
    }
}

pub(crate) mod adapter;

use std::ffi::OsString;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::agent::{
    AgentDiagnosticLevel, AgentFailure, AgentFailureCause, AgentHarnessFailureDetail,
    AgentLifecycleMilestone, AgentObservation, AgentOutcome, AgentProtocolRejectionDiagnostic,
    AgentToolCallPhase, AgentValueKind, BoundedAgentResponse, CanonicalHarnessFailure,
    CapturedJson, CompletedAgentInvocation, HarnessFailureDiagnostic, failed_agent_outcome,
    tool_call_observation,
};
use super::agent_stream_excerpt::{RejectedStreamExcerpt, RetainedStreamLine, StreamTail};
use super::text_fit::ellipsize;
#[cfg(test)]
const QUALIFICATION_VERSION: &str =
    crate::claude_code::CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION;

const MAXIMUM_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const RETAINED_PRECEDING_LINES: usize = 8;
const MAXIMUM_PRECEDING_LINE_BYTES: usize = 1024;
const MAXIMUM_REJECTED_LINE_BYTES: usize = 8 * 1024;
const MAXIMUM_DIAGNOSTIC_SEQUENCE: u64 = i64::MAX as u64;
const PERMISSION_MODE: &str = "bypassPermissions";
const MAXIMUM_TRACKED_TASKS: usize = 128;
const MAXIMUM_OBSERVED_UNRECOGNIZED_KINDS: usize = 64;
const MAXIMUM_UNRECOGNIZED_KIND_TOKEN_BYTES: usize = 64;
// Display-width bounds on the harness-reported text a subagent diagnostic names.
const MAXIMUM_SUBAGENT_DESCRIPTION_WIDTH: usize = 200;
const MAXIMUM_SUBAGENT_TOKEN_WIDTH: usize = 64;
/// Entries of each tracked-work list `protocol-rejection.json` keeps (REQ-AGENT-74).
const MAXIMUM_RETAINED_TRACKED_WORK: usize = 16;
/// Width of a task, call or run identifier in the retained tracked work.
const MAXIMUM_TRACKED_WORK_ID_WIDTH: usize = 128;
const STRUCTURED_OUTPUT_TOOL_NAME: &str = "StructuredOutput";
pub(crate) const RESULT_ENVELOPE_SCHEMA: &str = r#"{"type":"object","required":["result"],"properties":{"result":{}},"additionalProperties":true}"#;

pub(crate) const FIXED_INVOCATION_ENVIRONMENT: [(&str, &str); 5] = [
    ("DISABLE_UPDATES", "1"),
    ("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ("CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL", "1"),
    ("CLAUDE_CODE_DISABLE_AUTO_MEMORY", "1"),
    ("CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS", "1"),
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClaudeCodeStreamJsonV1ProtocolRejection {
    reason: ClaudeCodeStreamJsonV1RejectionReason,
    stage: ClaudeCodeStreamJsonV1ProtocolStage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outer_event: Option<ClaudeCodeStreamJsonV1EventType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream_event: Option<ClaudeCodeStreamJsonV1StreamEventType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_block: Option<ClaudeCodeStreamJsonV1ContentBlockKind>,
    state: ClaudeCodeStreamJsonV1RejectionState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1ProtocolStage {
    FrameRead,
    FrameDecode,
    InitialInput,
    Initialization,
    ExchangeLifecycle,
    EventEnvelope,
    SessionCorrelation,
    StreamEventEnvelope,
    ActiveStreamParent,
    MessageStart,
    MessageTransition,
    ContentBlockStart,
    ContentBlockDelta,
    ContentBlockStop,
    AssistantEvent,
    UserEvent,
    SystemEvent,
    ToolProgressEvent,
    ResultCorrelation,
    TerminalDrain,
    EndOfStream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1RejectionReason {
    FrameTooLarge,
    FrameDecodeFailed,
    FrameNotObject,
    StandardInputWriteFailed,
    StandardInputWriteTimedOut,
    StandardInputCloseFailed,
    InitializationInvalid,
    ExchangeInitializationMissing,
    ExchangeLifecycleInvalid,
    EventEnvelopeInvalid,
    SessionCorrelationInvalid,
    StreamEventEnvelopeInvalid,
    ActiveStreamParentMismatch,
    MessageStartInvalid,
    MessageTransitionInvalid,
    ContentBlockStartInvalid,
    ContentBlockDeltaInvalid,
    ContentBlockStopInvalid,
    ContentBlockIndexMismatch,
    ContentBlockTypeTransitionInvalid,
    ContentBlockCorrelationInvalid,
    AssistantEventInvalid,
    UserEventInvalid,
    SystemEventInvalid,
    ToolProgressEventInvalid,
    // Pi uses some matching terminal labels, but each closed profile must own and evolve
    // its rejection taxonomy independently.
    ResultCorrelationInvalid,
    TerminalDrainEventInvalid,
    EndOfStreamInvariantInvalid,
    PartialFrameAtEndOfStream,
    RetainedStateLimitExceeded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1EventType {
    System,
    StreamEvent,
    Assistant,
    User,
    RateLimitEvent,
    ToolProgress,
    Result,
    Unrecognized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1StreamEventType {
    MessageStart,
    ContentBlockStart,
    ContentBlockDelta,
    ContentBlockStop,
    MessageDelta,
    MessageStop,
    Unrecognized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1ContentBlockKind {
    Text,
    Thinking,
    ToolUse,
    Unrecognized,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClaudeCodeStreamJsonV1RejectionState {
    initialized: bool,
    exchange_initialized: bool,
    exchange_active: bool,
    completed_exchanges: u64,
    active_message: ClaudeCodeStreamJsonV1ActiveMessageState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_content_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed_content_blocks: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    open_content_block: Option<ClaudeCodeStreamJsonV1OpenContentBlockState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_message_delta_seen: Option<bool>,
    final_main_message: bool,
    exchange_structured_output_candidates: u64,
    completed_result_exchange: ClaudeCodeStreamJsonV1CompletedResultExchangeState,
    result_decision_pending: bool,
    result_accepted: bool,
    native_failure: bool,
    retry_active: bool,
    // Records from runners before these facts existed omit them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_thread: Option<ClaudeCodeStreamJsonV1EventThread>,
    #[serde(default)]
    outstanding_background_commands: u64,
    #[serde(default)]
    outstanding_background_subagents: u64,
    #[serde(default)]
    scheduled_wakeup_pending: bool,
}

/// The thread the rejected frame belonged to, from its `parent_tool_use_id`: absent,
/// `null`, or a tool call id.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1EventThread {
    None,
    Main,
    Subagent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1ActiveMessageState {
    None,
    Main,
    Subagent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ClaudeCodeStreamJsonV1OpenContentBlockState {
    kind: ClaudeCodeStreamJsonV1ContentBlockKind,
    content_index: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClaudeCodeStreamJsonV1CompletedResultExchangeState {
    None,
    Candidate,
    AmbiguousCandidate,
    MissingCandidate,
    PrematureCandidate,
    NativeFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClaudeCodeStreamJsonV1RejectionContext {
    stage: ClaudeCodeStreamJsonV1ProtocolStage,
    outer_event: Option<ClaudeCodeStreamJsonV1EventType>,
    stream_event: Option<ClaudeCodeStreamJsonV1StreamEventType>,
    content_index: Option<u64>,
    content_block: Option<ClaudeCodeStreamJsonV1ContentBlockKind>,
    event_thread: Option<ClaudeCodeStreamJsonV1EventThread>,
}

impl Default for ClaudeCodeStreamJsonV1RejectionContext {
    fn default() -> Self {
        Self {
            stage: ClaudeCodeStreamJsonV1ProtocolStage::FrameRead,
            outer_event: None,
            stream_event: None,
            content_index: None,
            content_block: None,
            event_thread: None,
        }
    }
}

// Keep each closed profile's limits beside its native parser so future profile changes
// cannot silently alter another harness's admission contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaudeCodeStreamJsonV1ProtocolLimits {
    maximum_frame_bytes: NonZeroU64,
    /// Accepted frames kept, oldest first, as the lines before a rejected one.
    retained_preceding_lines: NonZeroUsize,
    maximum_preceding_line_bytes: NonZeroUsize,
    maximum_rejected_line_bytes: NonZeroUsize,
}

const fn nonzero_bound(bytes: usize) -> NonZeroUsize {
    match NonZeroUsize::new(bytes) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    }
}

impl ClaudeCodeStreamJsonV1ProtocolLimits {
    pub(crate) const fn profile() -> Self {
        let maximum_frame_bytes = match NonZeroU64::new(MAXIMUM_FRAME_BYTES) {
            Some(value) => value,
            None => NonZeroU64::MIN,
        };
        Self {
            maximum_frame_bytes,
            retained_preceding_lines: nonzero_bound(RETAINED_PRECEDING_LINES),
            maximum_preceding_line_bytes: nonzero_bound(MAXIMUM_PRECEDING_LINE_BYTES),
            maximum_rejected_line_bytes: nonzero_bound(MAXIMUM_REJECTED_LINE_BYTES),
        }
    }

    #[cfg(test)]
    const fn with_maximum_frame_bytes(maximum_frame_bytes: NonZeroU64) -> Self {
        Self {
            maximum_frame_bytes,
            ..Self::profile()
        }
    }

    #[cfg(test)]
    const fn with_excerpt_bounds(
        retained_preceding_lines: NonZeroUsize,
        maximum_preceding_line_bytes: NonZeroUsize,
        maximum_rejected_line_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            retained_preceding_lines,
            maximum_preceding_line_bytes,
            maximum_rejected_line_bytes,
            ..Self::profile()
        }
    }

    pub(crate) const fn maximum_frame_bytes(self) -> NonZeroU64 {
        self.maximum_frame_bytes
    }

    pub(crate) const fn retained_preceding_lines(self) -> NonZeroUsize {
        self.retained_preceding_lines
    }

    pub(crate) const fn maximum_preceding_line_bytes(self) -> NonZeroUsize {
        self.maximum_preceding_line_bytes
    }

    pub(crate) const fn maximum_rejected_line_bytes(self) -> NonZeroUsize {
        self.maximum_rejected_line_bytes
    }
}

pub(crate) fn normal_mode_arguments(
    model: &str,
    effort: &str,
    session_id: &str,
    system_prompt_file: &Path,
) -> Vec<OsString> {
    [
        OsString::from("-p"),
        OsString::from("--input-format"),
        OsString::from("stream-json"),
        OsString::from("--output-format"),
        OsString::from("stream-json"),
        OsString::from("--verbose"),
        OsString::from("--include-partial-messages"),
        OsString::from("--forward-subagent-text"),
        OsString::from("--session-id"),
        OsString::from(session_id),
        OsString::from("--permission-mode"),
        OsString::from(PERMISSION_MODE),
        OsString::from("--setting-sources"),
        OsString::from("user,project,local"),
        OsString::from("--model"),
        OsString::from(model),
        OsString::from("--effort"),
        OsString::from(effort),
        OsString::from("--append-system-prompt-file"),
        system_prompt_file.as_os_str().to_owned(),
    ]
    .into()
}

pub(crate) fn result_mode_arguments(
    model: &str,
    effort: &str,
    session_id: &str,
    system_prompt_file: &Path,
) -> Vec<OsString> {
    let mut arguments = normal_mode_arguments(model, effort, session_id, system_prompt_file);
    arguments.extend([
        OsString::from("--json-schema"),
        OsString::from(RESULT_ENVELOPE_SCHEMA),
    ]);
    arguments
}

pub(crate) fn user_content_frame(content: Vec<Value>) -> Result<Vec<u8>, serde_json::Error> {
    let mut frame = serde_json::to_vec(&json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": content,
        },
    }))?;
    frame.push(b'\n');
    Ok(frame)
}

pub(crate) fn initial_user_text_frame(message: &str) -> Result<Vec<u8>, serde_json::Error> {
    user_content_frame(vec![json!({
        "type": "text",
        "text": message,
    })])
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CompletedResultExchange {
    Candidate(Arc<Value>),
    AmbiguousCandidate,
    MissingCandidate,
    /// A structured result submitted while background work the step started still owes
    /// its notification. It is never committed; the adapter returns it as feedback.
    PrematureCandidate,
    NativeFailure,
}

/// The background commands and subagents the parser held when it refused a frame, and the
/// subagents an earlier wakeup `init` forgot, by identity: the wait the parser expected.
/// Retained beside the protocol rejection in `protocol-rejection.json` on the runner host
/// only (REQ-AGENT-74); never projected into a frame, a run or a published result
/// (REQ-DIAG-39). Each list keeps its newest `MAXIMUM_RETAINED_TRACKED_WORK` entries.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClaudeCodeStreamJsonV1TrackedWork {
    background_commands: Vec<ClaudeCodeStreamJsonV1TrackedCommand>,
    subagents: Vec<ClaudeCodeStreamJsonV1TrackedSubagent>,
    forgotten_subagents: Vec<String>,
    truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClaudeCodeStreamJsonV1TrackedCommand {
    task_id: String,
    tool_use_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<String>,
    backgrounded: bool,
    acknowledged: bool,
    notified: bool,
    monitor: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClaudeCodeStreamJsonV1TrackedSubagent {
    task_id: String,
    agent_call_id: String,
    tool_use_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<String>,
    backgrounded: bool,
    notified: bool,
}

fn tracked_work_id(id: &str) -> String {
    ellipsize(id, MAXIMUM_TRACKED_WORK_ID_WIDTH)
}

/// The newest `MAXIMUM_RETAINED_TRACKED_WORK` items of a ledger, and whether it was cut.
fn newest_tracked<T, U>(items: &[T], retain: impl Fn(&T) -> U) -> (Vec<U>, bool) {
    let skipped = items.len().saturating_sub(MAXIMUM_RETAINED_TRACKED_WORK);
    (items[skipped..].iter().map(retain).collect(), skipped > 0)
}

/// A main-thread call whose `local_bash` task the parser tracks: a Bash call, or a
/// `Monitor` call (`monitor`), whose task watches a command and whose result names it as
/// `taskId` rather than `backgroundTaskId`.
struct CommandCall {
    id: Arc<str>,
    monitor: bool,
}

struct BackgroundCommand {
    task_id: Arc<str>,
    tool_use_id: Arc<str>,
    run_id: Option<Arc<str>>,
    /// Started with `is_backgrounded: true`, or moved to the background later by a
    /// `task_updated` patch or by the tool result that names its `backgroundTaskId` (its
    /// `taskId`, for a monitor). Only a backgrounded command owes a `task_notification`
    /// before the stream may end.
    backgrounded: bool,
    acknowledged: bool,
    notified: bool,
    /// The task a `Monitor` call started: each line its command prints opens a turn that
    /// nothing on the stream announces, until its `task_notification` settles it.
    monitor: bool,
}

impl BackgroundCommand {
    fn awaits_notification(&self) -> bool {
        self.backgrounded && !self.notified
    }
}

/// A subagent the Agent tool started inside an active exchange. A foreground one is
/// observational: settling it is not a completion condition, and the exchange's end
/// forgets it. A backgrounded one outlives the exchange: it owes a `task_notification`
/// before the stream may end, and until then its messages and its own task events are
/// accepted in the terminal drain. A subagent is backgrounded when its `task_started`
/// says `is_backgrounded: true`, or later, when a `task_updated` patch moves it or the
/// Agent call's result reports an asynchronous launch; that result also tracks a subagent
/// whose start the harness never announced, by its `agentId`, with no run id (`None`),
/// so its notification and task entries match by task id and tool call alone. A message
/// the agent sends a finished backgrounded subagent resumes it as a new run under the
/// message's tool call; its messages keep naming the Agent call as their parent.
struct Subagent {
    task_id: Arc<str>,
    /// The Agent call that launched the subagent, which its messages name as
    /// `parent_tool_use_id` through every run.
    agent_call_id: Arc<str>,
    /// The tool call that started the current run: the Agent call, or the `SendMessage`
    /// call that resumed the subagent, which its `task_notification` names.
    tool_use_id: Arc<str>,
    run_id: Option<Arc<str>>,
    subagent_type: Option<Arc<str>>,
    description: Option<Arc<str>>,
    /// Whether a `task_started` announced the current run; false for a subagent the Agent
    /// call's asynchronous-launch result tracked alone, whose late start is adopted.
    announced: bool,
    backgrounded: bool,
    notified: bool,
    /// The main-thread `user` frames the parser had read when the current run's
    /// notification arrived, and whether a main-thread tool round of the same turn has taken
    /// a `user` frame since: Claude Code appends a queued notification to the user message
    /// of the main thread's next tool round, which stream-json does not echo, instead of
    /// opening a turn for it, while a turn's first request carries only the item it was
    /// opened for. A notification not delivered that way is still queued when the subagent
    /// is resumed, and its turn is carried past the resume (`carried_notifications`).
    notified_after_user_frames: u64,
    delivered: bool,
}

impl Subagent {
    fn awaits_notification(&self) -> bool {
        self.backgrounded && !self.notified
    }

    /// Moves the subagent's current run to the tool call that resumed it, which the
    /// resumed run's `task_notification` names, and reports the resume.
    fn resume(
        &mut self,
        tool_use_id: &str,
        run_id: Option<Arc<str>>,
        backgrounded: bool,
    ) -> String {
        self.tool_use_id = Arc::from(tool_use_id);
        self.run_id = run_id;
        self.announced = true;
        self.backgrounded = backgrounded;
        self.notified = false;
        self.message("resumed")
    }

    fn message(&self, event: &str) -> String {
        let mut message = String::from("subagent");
        for token in self.subagent_type.iter().map(AsRef::as_ref).chain([event]) {
            message.push(' ');
            message.push_str(token);
        }
        if let Some(description) = &self.description {
            message.push_str(" · ");
            message.push_str(description);
        }
        message
    }
}

pub(crate) struct ClaudeCodeStreamJsonV1Parser {
    expected_cwd: Arc<str>,
    expected_model: Arc<str>,
    expected_claude_code_version: Arc<str>,
    value_kind: AgentValueKind,
    maximum_response_bytes: NonZeroU64,
    limits: ClaudeCodeStreamJsonV1ProtocolLimits,
    frame: Vec<u8>,
    expected_session_id: Arc<str>,
    session_id: Option<Arc<str>>,
    exchange_initialized: bool,
    exchange_active: bool,
    completed_exchanges: u64,
    /// Main-thread Bash and `Monitor` calls awaiting their results (at most 128), whose
    /// `local_bash` task starts are tracked.
    command_calls: Vec<CommandCall>,
    background_commands: Vec<BackgroundCommand>,
    subagents: Vec<Subagent>,
    /// Backgrounded subagents a wakeup `init` forgot as outstanding work, kept by identity
    /// (newest last, at most `MAXIMUM_TRACKED_TASKS`) so that a later message to one
    /// resumes it under the Agent call its messages still name.
    forgotten_subagents: Vec<Subagent>,
    /// Task ids that `task_started owned_by_subagent: true` introduced (newest last, at most
    /// `MAXIMUM_TRACKED_TASKS`): a subagent's own work stays its work after the subagent
    /// settles, so a command it left running is silent wherever its events land.
    subagent_owned_tasks: Vec<Arc<str>>,
    /// Task ids whose `task_started` named a `task_type` the parser does not model, neither
    /// `local_bash` nor `local_agent` (newest last, at most `MAXIMUM_TRACKED_TASKS`): every
    /// later frame naming one is its activity, unrecognized and never a protocol failure,
    /// and nothing waits for it.
    unmodeled_tasks: Vec<Arc<str>>,
    /// Native `type`/`subtype` pairs already observed as unrecognized in this invocation.
    observed_unrecognized_kinds: Vec<(Box<str>, Option<Box<str>>)>,
    active_message: Option<StreamedMessage>,
    final_main_message: Option<CompletedMainMessage>,
    exchange_structured_output_candidates: u64,
    completed_result_exchange: Option<CompletedResultExchange>,
    result_decision_pending: bool,
    /// The pending decision is for a premature candidate, which may only be rejected.
    premature_candidate_pending: bool,
    /// A `command_lifecycle` `started` frame arrived in the drain while background work
    /// was outstanding: a wakeup the agent scheduled authorizes the next `init`.
    scheduled_wakeup_pending: bool,
    /// The current or last exchange began on a scheduled wakeup whose `completed` frame
    /// has not arrived yet.
    scheduled_wakeup_turn: bool,
    accepted_result: Option<CapturedJson>,
    /// The step's value has settled: the result was accepted in Result mode, or in
    /// Response and None modes a result left nothing the step waits on outstanding.
    /// A turn announced after that is an epilogue (REQ-AGENT-76).
    value_settled: bool,
    /// The open turn is an epilogue: its messages are activity and its result is no value.
    epilogue: bool,
    /// Main-thread messages completed in the open turn; a turn that ends with none and
    /// `num_turns: 0` is an empty turn.
    exchange_main_messages: u32,
    /// An empty turn consumed the announcement of a notified task without a model call;
    /// the announcement stands for the next `init`.
    announced_turn_pending: bool,
    /// Notifications of a subagent's earlier runs that its resume carried past, oldest first,
    /// each the count of main-thread `user` frames read when it arrived: each was still
    /// queued at Claude Code when the subagent ran again, and each still opens a turn, with
    /// a model call or, once a newer notification of the subagent queued behind it, without
    /// one (the empty turn), unless a main-thread tool round takes it first (a main-thread
    /// `user` frame past that count, then a main-thread message start), which drops it. An
    /// allowance for a drain `init`, never a duty: Claude Code may have appended one to a
    /// tool round the parser cannot see.
    carried_notifications: Vec<u64>,
    /// The adapter's correction prompt is on stdin and its turn has not ended: `begin_exchange`
    /// set it, and the turn's `result` clears it. An empty turn read inside the correction
    /// window consumes the exchange but not this, so the next drain `init` is still the
    /// correction turn's; a stream that ends with it set is invalid.
    correction_pending: bool,
    /// The carried notification whose turn an `init` in the correction window opened: Claude
    /// Code dequeued it ahead of the correction prompt, so its first request carried it and
    /// no tool round of the turn delivers it. Set aside from `carried_notifications` for the
    /// turn; its `result` with a model call returns it, the allowance that admits the
    /// correction turn, and an empty turn's `result` consumes it.
    correction_window_notification: Option<u64>,
    /// Main-thread `user` frames read so far, and the count when the open turn's `init`
    /// arrived, against which a notification's delivery to a tool round of the turn is told
    /// (`Subagent::delivered`).
    main_user_frames: u64,
    turn_main_user_frames: u64,
    /// A notified command or subagent announced the open turn, which an empty turn may
    /// consume; a first exchange, a wakeup turn or the adapter's correction turn is not
    /// announced, and ending it without a model call stays a protocol failure.
    announced_exchange: bool,
    native_failure: Option<AgentHarnessFailureDetail>,
    native_api_error: Option<HarnessFailureDiagnostic>,
    /// The newest reset time a usage window not `allowed` reported (REQ-AGENT-72).
    latest_reset: Option<u64>,
    retry_active: bool,
    observations: Vec<AgentObservation>,
    rejection_context: ClaudeCodeStreamJsonV1RejectionContext,
    rejection_state_snapshot: Option<ClaudeCodeStreamJsonV1RejectionState>,
    protocol_rejection: Option<AgentProtocolRejectionDiagnostic>,
    /// The most recent accepted frames, kept for the stream excerpt of a rejection.
    stream_tail: StreamTail,
    /// The frame, overrun buffer or open buffer the stream was rejected on.
    rejected_line: Option<RetainedStreamLine>,
    failure: Option<AgentFailureCause>,
}

impl ClaudeCodeStreamJsonV1Parser {
    pub(crate) fn profile(
        expected_cwd: Arc<str>,
        expected_model: Arc<str>,
        expected_session_id: Arc<str>,
        expected_claude_code_version: Arc<str>,
        value_kind: AgentValueKind,
        maximum_response_bytes: NonZeroU64,
    ) -> Self {
        Self::new(
            expected_cwd,
            expected_model,
            expected_session_id,
            expected_claude_code_version,
            value_kind,
            maximum_response_bytes,
            ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
        )
    }

    fn new(
        expected_cwd: Arc<str>,
        expected_model: Arc<str>,
        expected_session_id: Arc<str>,
        expected_claude_code_version: Arc<str>,
        value_kind: AgentValueKind,
        maximum_response_bytes: NonZeroU64,
        limits: ClaudeCodeStreamJsonV1ProtocolLimits,
    ) -> Self {
        Self {
            expected_cwd,
            expected_model,
            expected_claude_code_version,
            value_kind,
            maximum_response_bytes,
            stream_tail: StreamTail::new(
                limits.retained_preceding_lines(),
                limits.maximum_preceding_line_bytes(),
            ),
            rejected_line: None,
            limits,
            frame: Vec::new(),
            expected_session_id,
            session_id: None,
            exchange_initialized: false,
            exchange_active: true,
            completed_exchanges: 0,
            command_calls: Vec::new(),
            background_commands: Vec::new(),
            subagents: Vec::new(),
            forgotten_subagents: Vec::new(),
            subagent_owned_tasks: Vec::new(),
            unmodeled_tasks: Vec::new(),
            observed_unrecognized_kinds: Vec::new(),
            active_message: None,
            final_main_message: None,
            exchange_structured_output_candidates: 0,
            completed_result_exchange: None,
            result_decision_pending: false,
            premature_candidate_pending: false,
            scheduled_wakeup_pending: false,
            scheduled_wakeup_turn: false,
            accepted_result: None,
            value_settled: false,
            epilogue: false,
            exchange_main_messages: 0,
            announced_turn_pending: false,
            carried_notifications: Vec::new(),
            correction_pending: false,
            correction_window_notification: None,
            main_user_frames: 0,
            turn_main_user_frames: 0,
            announced_exchange: false,
            native_failure: None,
            native_api_error: None,
            latest_reset: None,
            retry_active: false,
            observations: Vec::new(),
            rejection_context: ClaudeCodeStreamJsonV1RejectionContext::default(),
            rejection_state_snapshot: None,
            protocol_rejection: None,
            failure: None,
        }
    }

    #[cfg(test)]
    fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    #[cfg(test)]
    const fn completed_exchanges(&self) -> u64 {
        self.completed_exchanges
    }

    pub(crate) fn take_completed_result_exchange(&mut self) -> Option<CompletedResultExchange> {
        self.completed_result_exchange.take()
    }

    pub(crate) fn reject_result_candidate(&mut self) -> Result<(), AgentFailureCause> {
        if self.value_kind != AgentValueKind::Result
            || !self.result_decision_pending
            || self.accepted_result.is_some()
        {
            return self.fail_protocol(
                ClaudeCodeStreamJsonV1RejectionReason::ResultCorrelationInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::ResultCorrelation,
            );
        }
        self.result_decision_pending = false;
        self.premature_candidate_pending = false;
        Ok(())
    }

    pub(crate) fn accept_result(&mut self, result: CapturedJson) -> Result<(), AgentFailureCause> {
        if self.value_kind != AgentValueKind::Result
            || !self.result_decision_pending
            || self.premature_candidate_pending
            || self.accepted_result.is_some()
            || self.exchange_active
            || self.exchange_initialized
        {
            return self.fail_protocol(
                ClaudeCodeStreamJsonV1RejectionReason::ResultCorrelationInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::ResultCorrelation,
            );
        }
        self.result_decision_pending = false;
        self.accepted_result = Some(result);
        self.value_settled = true;
        Ok(())
    }

    /// Begins the next serialized user exchange after the preceding result was rejected.
    /// The same process and session remain authoritative for the invocation. The correction
    /// prompt's turn is owed until a `result` ends it (`correction_pending`).
    pub(crate) fn begin_exchange(&mut self) -> Result<(), AgentFailureCause> {
        self.begin_turn()?;
        self.correction_pending = true;
        Ok(())
    }

    /// Begins a turn before the step's value has settled: the adapter's correction turn, or
    /// one a notification, a wakeup or a carried notification announced in the drain.
    fn begin_turn(&mut self) -> Result<(), AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        if self.session_id.is_none()
            || self.exchange_active
            || self.exchange_initialized
            || self.active_message.is_some()
            || self.completed_result_exchange.is_some()
            || self.result_decision_pending
            || self.accepted_result.is_some()
        {
            return self.fail_protocol(
                ClaudeCodeStreamJsonV1RejectionReason::ExchangeLifecycleInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::ExchangeLifecycle,
            );
        }
        self.exchange_active = true;
        self.final_main_message = None;
        self.reset_exchange_state();
        Ok(())
    }

    /// Begins a turn Claude Code opens after the step's value settled, announced or not.
    /// The final main-thread message stays the value's (REQ-AGENT-76).
    fn begin_epilogue(&mut self) {
        self.exchange_active = true;
        self.epilogue = true;
        self.reset_exchange_state();
    }

    fn reset_exchange_state(&mut self) {
        self.exchange_structured_output_candidates = 0;
        self.exchange_main_messages = 0;
        self.announced_exchange = false;
        self.native_failure = None;
        self.native_api_error = None;
    }

    /// Consumes arbitrary stdout chunks while retaining at most one bounded JSONL frame.
    // Pi and Claude intentionally retain independent native state machines; sharing
    // this byte loop would couple their profile-specific failure transitions.
    pub(crate) fn push_stdout(
        &mut self,
        bytes: &[u8],
        mut observe: impl FnMut(AgentObservation),
    ) -> Result<(), AgentFailureCause> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }

        for &byte in bytes {
            if byte == b'\n' {
                let frame = std::mem::take(&mut self.frame);
                if let Err(failure) = self.parse_frame(&frame) {
                    self.rejected_line = Some(self.retained_rejected_line(&frame));
                    self.invalidate_values();
                    self.observations.clear();
                    return Err(self.fail_stream(failure));
                }
                self.stream_tail.push(&frame);
                for observation in self.observations.drain(..) {
                    observe(observation);
                }
                continue;
            }

            let retained = u64::try_from(self.frame.len()).unwrap_or(u64::MAX);
            if retained >= self.limits.maximum_frame_bytes().get() {
                self.rejected_line = Some(self.retained_rejected_line(&self.frame));
                let failure = self.record_protocol_failure(
                    ClaudeCodeStreamJsonV1RejectionReason::FrameTooLarge,
                    ClaudeCodeStreamJsonV1ProtocolStage::FrameRead,
                );
                return Err(self.fail_stream(failure));
            }
            self.frame.push(byte);
        }
        Ok(())
    }

    pub(crate) fn finish(mut self, exit_success: bool) -> AgentOutcome {
        if self.failure.is_none() && !self.frame.is_empty() {
            self.rejected_line = Some(self.retained_rejected_line(&self.frame));
            self.prepare_rejection(ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream);
            self.record_rejection(
                ClaudeCodeStreamJsonV1RejectionReason::PartialFrameAtEndOfStream,
                ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream,
            );
            let failure = self.protocol_failure();
            self.fail_stream(failure);
            self.invalidate_values();
        }
        if let Some(failure) = self.failure.clone() {
            return AgentOutcome::Failed(self.agent_failure(failure));
        }
        if self.session_id.is_none() {
            return self.protocol_failure_outcome(
                AgentFailureCause::start_failure("launch preparation", "unavailable"),
                ClaudeCodeStreamJsonV1RejectionReason::EndOfStreamInvariantInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream,
            );
        }
        if self.exchange_active
            || self.exchange_initialized
            || self.active_message.is_some()
            || self.completed_result_exchange.is_some()
            || self.result_decision_pending
            || self.announced_turn_pending
            || self.correction_pending
            || self.completed_exchanges == 0
        {
            return self.end_of_stream_invariant_failure();
        }
        // A native terminal failure explains a stream that ends with background work still
        // running (INV-HARNESS-5).
        if let Some(detail) = self.native_failure {
            return failed(AgentFailureCause::HarnessFailed { detail });
        }
        if self.awaits_background_work() {
            return self.end_of_stream_invariant_failure();
        }
        if !exit_success {
            return failed(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            });
        }

        match self.value_kind {
            AgentValueKind::None => AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
            AgentValueKind::Response => {
                let Some(message) = self.final_main_message else {
                    return failed(AgentFailureCause::MissingResponse);
                };
                if message.text_blocks == 0 {
                    return failed(AgentFailureCause::MissingResponse);
                }
                AgentOutcome::Completed(CompletedAgentInvocation::Response(
                    BoundedAgentResponse::from_bounded(Arc::from(message.response)),
                ))
            }
            AgentValueKind::Result => self.accepted_result.map_or_else(
                || failed(AgentFailureCause::MissingResult),
                |result| AgentOutcome::Completed(CompletedAgentInvocation::Result(result)),
            ),
        }
    }

    fn end_of_stream_invariant_failure(&self) -> AgentOutcome {
        self.protocol_failure_outcome(
            AgentFailureCause::HarnessProtocolFailed,
            ClaudeCodeStreamJsonV1RejectionReason::EndOfStreamInvariantInvalid,
            ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream,
        )
    }

    // Native parser state stays profile-local even though both profiles snapshot before
    // decoding and install one fallback diagnostic for otherwise unclassified rejections.
    fn parse_frame(&mut self, frame: &[u8]) -> Result<(), AgentFailureCause> {
        self.prepare_rejection(ClaudeCodeStreamJsonV1ProtocolStage::FrameRead);
        let result = self.parse_frame_inner(frame);
        let parser_owned_rejection = result.as_ref().is_err_and(|cause| {
            matches!(
                cause,
                AgentFailureCause::HarnessStartFailed { .. }
                    | AgentFailureCause::HarnessProtocolFailed
            )
        });
        if parser_owned_rejection && self.protocol_rejection.is_none() {
            let stage = self.rejection_context.stage;
            self.record_rejection(default_rejection_reason(stage), stage);
        }
        self.rejection_state_snapshot = None;
        result
    }

    fn parse_frame_inner(&mut self, frame: &[u8]) -> Result<(), AgentFailureCause> {
        // This profile owns decoding failure classification because Claude's init boundary
        // differs from Pi's session-header and agent-start boundaries.
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::FrameDecode;
        let value =
            um_support::strict_json_from_slice(frame).map_err(|_| self.protocol_failure())?;
        let Some(object) = value.as_object() else {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::FrameNotObject,
                ClaudeCodeStreamJsonV1ProtocolStage::FrameDecode,
            );
        };

        self.rejection_context.outer_event = required_string(object, "type").map(claude_event_type);
        self.rejection_context.event_thread = match object.get("parent_tool_use_id") {
            None => Some(ClaudeCodeStreamJsonV1EventThread::None),
            Some(_) => parent_tool_use_id(object).map(|parent| match parent {
                None => ClaudeCodeStreamJsonV1EventThread::Main,
                Some(_) => ClaudeCodeStreamJsonV1EventThread::Subagent,
            }),
        };
        if self.session_id.is_none() {
            self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::Initialization;
            return self.parse_initialization(object);
        }

        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::EventEnvelope;
        let event_type = required_string(object, "type").ok_or_else(|| self.protocol_failure())?;
        if event_type == "stream_event" {
            self.record_stream_event_identity(object);
        }
        if event_type == "system" && required_string(object, "subtype") == Some("init") {
            self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::Initialization;
            // A Result-mode correction turn is begun by the adapter before its init arrives;
            // begin_exchange refuses this one while a decision is pending or a result is
            // accepted (INV-HARNESS-7). Once the value has settled, the turn is an epilogue
            // whatever announced it, or nothing did (REQ-AGENT-76): Claude Code may open a
            // turn for a notification the parser has already spent on another, as when two
            // settled runs of one subagent queue two notifications against its one
            // `notified` bit.
            if !self.exchange_active {
                let scheduled_wakeup = self.scheduled_wakeup_pending;
                let announced = self.notification_announces_a_turn();
                // After a native failure the refusal ends the stream with that failure
                // (`fail_stream`), whatever announced the turn.
                if self.native_failure.is_some()
                    || !(scheduled_wakeup
                        || announced
                        || self.correction_pending
                        || self.value_settled)
                {
                    return Err(self.protocol_failure());
                }
                if self.value_settled {
                    self.begin_epilogue();
                } else {
                    self.begin_turn()?;
                }
                self.announced_exchange = announced;
                // Claude Code keeps one queue of pending inputs, notifications and the
                // adapter's prompt alike, and opens one turn per item it dequeues: this
                // turn consumes one, oldest first. An empty turn's announcement stands for
                // the newer notification that superseded it, consumed with it; then a
                // notification a resume carried past; then the correction prompt whose
                // exchange an empty turn consumed; then the completed tasks and subagents
                // and the scheduled wakeup together, as one turn. Others remain owned by
                // the invocation and may notify during the next native exchange.
                if self.announced_turn_pending {
                    self.announced_turn_pending = false;
                    self.background_commands.retain(|task| !task.notified);
                    self.forget_notified_subagents();
                } else if !self.carried_notifications.is_empty() {
                    self.carried_notifications.remove(0);
                } else if self.correction_pending {
                    self.correction_pending = false;
                } else {
                    self.background_commands.retain(|task| !task.notified);
                    self.forget_notified_subagents();
                    self.scheduled_wakeup_pending = false;
                    self.scheduled_wakeup_turn |= scheduled_wakeup;
                }
            } else if !self.exchange_initialized && self.correction_pending {
                // The adapter has begun the correction turn, but a notification queued
                // ahead of its prompt opens its turn first, which this `init` may be: a
                // `result` without a model call then ends an empty turn, not the correction.
                // The notification Claude Code dequeued for this turn is the oldest carried
                // one; a tool round of the turn cannot deliver it again, so it is set aside
                // from the delivery inference until the turn ends.
                self.announced_exchange = self.notification_announces_a_turn();
                if !self.carried_notifications.is_empty() {
                    self.correction_window_notification =
                        Some(self.carried_notifications.remove(0));
                }
            }
            self.turn_main_user_frames = self.main_user_frames;
            return self.parse_initialization(object);
        }

        if !self.exchange_active {
            self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::TerminalDrain;
            return self.parse_terminal_drain(event_type, object, &value);
        }
        // The adapter has begun the correction turn (`begin_exchange`) and Claude Code has
        // not yet written its `init`. A subagent thread's frame or a task frame that lands
        // in that window is the turn's to read: a resumed run's first message or a stopped
        // subagent's interruption is the subagent's work (REQ-AGENT-64, REQ-AGENT-69), parsed
        // below as it would be after the `init`. The main thread cannot speak before its
        // `init`, so its frame stays the protocol failure.
        if !self.exchange_initialized
            && !correction_window_admits(event_type, self.rejection_context.event_thread)
        {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ExchangeInitializationMissing,
                ClaudeCodeStreamJsonV1ProtocolStage::ExchangeLifecycle,
            );
        }

        match event_type {
            "stream_event" => self.parse_stream_event(object, &value),
            "assistant" => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::AssistantEvent;
                self.parse_assistant(object, &value)
            }
            "user" => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::UserEvent;
                self.parse_user(object, &value)
            }
            "system" => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::SystemEvent;
                self.parse_system(object, &value)
            }
            "rate_limit_event" => self.parse_rate_limit_event(object),
            "tool_progress" => {
                self.rejection_context.stage =
                    ClaudeCodeStreamJsonV1ProtocolStage::ToolProgressEvent;
                self.parse_tool_progress(object)
            }
            "result" => {
                self.rejection_context.stage =
                    ClaudeCodeStreamJsonV1ProtocolStage::ResultCorrelation;
                self.parse_result(object)
            }
            "command_lifecycle" => {
                self.require_matching_session(object)?;
                self.parse_command_lifecycle(object, &value);
                Ok(())
            }
            _ => {
                self.require_matching_session(object)?;
                self.observe_unrecognized(&value);
                Ok(())
            }
        }
    }

    /// A wakeup the agent scheduled runs as a queued command: `started` precedes its
    /// `init` and `completed` follows its turn's `result`. Only a `started` frame arriving
    /// after the exchange ended while background work keeps the harness alive is a
    /// wakeup; any other lifecycle frame stays unrecognized.
    fn parse_command_lifecycle(&mut self, object: &Map<String, Value>, value: &Value) {
        match required_string(object, "state") {
            Some("started") if !self.exchange_active && self.awaits_background_work() => {
                if !self.scheduled_wakeup_pending {
                    self.scheduled_wakeup_pending = true;
                    self.observations.push(AgentObservation::Diagnostic {
                        level: AgentDiagnosticLevel::Information,
                        message: Arc::from("scheduled wakeup"),
                    });
                }
            }
            Some("completed") if self.scheduled_wakeup_turn => {
                self.scheduled_wakeup_turn = false;
            }
            _ => self.observe_unrecognized(value),
        }
    }

    fn parse_terminal_drain(
        &mut self,
        event_type: &str,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        // A backgrounded subagent keeps working after the exchange that started it; its
        // messages name its Agent call. A main-thread message stays invalid here.
        let subagent_message = parent_tool_use_id(object)
            .flatten()
            .is_some_and(|parent| self.awaits_subagent(parent));
        match event_type {
            "assistant" if subagent_message => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::AssistantEvent;
                self.parse_assistant(object, value)
            }
            "user" if subagent_message => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::UserEvent;
                self.parse_user(object, value)
            }
            "system"
                if matches!(
                    required_string(object, "subtype"),
                    Some("status" | "task_updated" | "task_notification" | "task_progress")
                ) =>
            {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::SystemEvent;
                self.parse_system(object, value)
            }
            "system"
                if required_string(object, "subtype") == Some("task_started")
                    && required_bool(object, "owned_by_subagent") == Some(true) =>
            {
                self.remember_subagent_work(object);
                Ok(())
            }
            "system" if required_string(object, "subtype") == Some("background_tasks_changed") => {
                if self.validate_drained_background_tasks(object)? {
                    self.observe_unrecognized(value);
                }
                Ok(())
            }
            // Claude Code resumes a finished subagent on its own to deliver the report of a
            // command the subagent left running: a `local_agent` start naming a settled
            // subagent, still tracked or forgotten, whose resumed run is outstanding work
            // again (REQ-AGENT-64).
            "system"
                if required_string(object, "subtype") == Some("task_started")
                    && required_string(object, "task_type") == Some("local_agent")
                    && required_nonempty_string(object, "task_id")
                        .is_some_and(|task_id| self.settled_subagent(task_id)) =>
            {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::SystemEvent;
                self.resume_settled_subagent(object)
            }
            "system"
                if required_string(object, "subtype") == Some("task_started")
                    && unmodeled_task_type(object) =>
            {
                self.remember_unmodeled_task(object, value);
                Ok(())
            }
            "system" if required_string(object, "subtype") == Some("task_started") => self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::TerminalDrainEventInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::TerminalDrain,
            ),
            "stream_event" | "assistant" | "user" | "tool_progress" | "result" => self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::TerminalDrainEventInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::TerminalDrain,
            ),
            "command_lifecycle" => {
                self.parse_command_lifecycle(object, value);
                Ok(())
            }
            _ => {
                self.observe_unrecognized(value);
                Ok(())
            }
        }
    }

    /// In the drain the harness's task list must name exactly the outstanding commands
    /// and subagents; an entry naming no tracked task is a subagent's own work while one
    /// is outstanding or when a subagent's start introduced it, and an entry naming a
    /// settled subagent, still tracked or forgotten, announces the resume that delivers
    /// its command's report. An entry naming a task of a kind the parser does not model is
    /// that task's activity; the result says whether the list named one, so that the
    /// caller records the frame as unrecognized.
    fn validate_drained_background_tasks(
        &self,
        object: &Map<String, Value>,
    ) -> Result<bool, AgentFailureCause> {
        let tasks = required_array(object, "tasks").ok_or_else(|| self.protocol_failure())?;
        let mut seen = Vec::new();
        let mut names_unmodeled_task = false;
        for entry in tasks {
            let entry = entry.as_object().ok_or_else(|| self.protocol_failure())?;
            let id = required_nonempty_string(entry, "task_id")
                .ok_or_else(|| self.protocol_failure())?;
            if self.is_subagent_work(id) || self.settled_subagent(id) {
                continue;
            }
            if self.is_unmodeled_task(id) || (unmodeled_task_type(entry) && !self.tracks_task(id)) {
                names_unmodeled_task = true;
                continue;
            }
            let run_id = required_nonempty_string(entry, "run_id");
            let outstanding = match required_string(entry, "task_type") {
                Some("local_bash") => self.background_commands.iter().any(|task| {
                    task.awaits_notification()
                        && task.task_id.as_ref() == id
                        && task.run_id.as_deref() == run_id
                }),
                Some("local_agent") => self.subagents.iter().any(|subagent| {
                    subagent.awaits_notification()
                        && subagent.task_id.as_ref() == id
                        && subagent
                            .run_id
                            .as_deref()
                            .is_none_or(|known| Some(known) == run_id)
                }),
                _ => false,
            };
            if !outstanding || seen.contains(&id) {
                return Err(self.protocol_failure());
            }
            seen.push(id);
        }
        let outstanding = self
            .background_commands
            .iter()
            .filter(|task| task.awaits_notification())
            .count()
            + self
                .subagents
                .iter()
                .filter(|subagent| subagent.awaits_notification())
                .count();
        if seen.len() != outstanding {
            return Err(self.protocol_failure());
        }
        Ok(names_unmodeled_task)
    }

    fn parse_initialization(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::Initialization;
        let session_id = required_string(object, "session_id");
        if !self.exchange_active
            || self.exchange_initialized
            || self.active_message.is_some()
            || required_string(object, "type") != Some("system")
            || required_string(object, "subtype") != Some("init")
            || required_string(object, "claude_code_version")
                != Some(self.expected_claude_code_version.as_ref())
            || required_string(object, "cwd") != Some(self.expected_cwd.as_ref())
            || required_string(object, "model") != Some(self.expected_model.as_ref())
            || required_string(object, "permissionMode") != Some(PERMISSION_MODE)
            || !session_id.is_some_and(|session_id| {
                session_id == self.expected_session_id.as_ref() && valid_session_id(session_id)
            })
        {
            return Err(self.protocol_failure());
        }

        let first_initialization = self.session_id.is_none();
        match (&self.session_id, session_id) {
            (None, Some(session_id)) => self.session_id = Some(Arc::from(session_id)),
            (Some(expected), Some(session_id)) if expected.as_ref() == session_id => {}
            _ => return Err(self.protocol_failure()),
        }
        self.exchange_initialized = true;
        if first_initialization {
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::SessionEstablished));
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::HarnessStarted));
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnStarted));
        Ok(())
    }

    fn parse_stream_event(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::StreamEventEnvelope;
        self.require_matching_session(object)?;
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::StreamEventEnvelope;
        let parent_tool_use_id =
            parent_tool_use_id(object).ok_or_else(|| self.protocol_failure())?;
        let event = required_object(object, "event").ok_or_else(|| self.protocol_failure())?;
        let event_type = required_string(event, "type").ok_or_else(|| self.protocol_failure())?;
        if event_type != "message_start" {
            self.require_active_stream_parent(parent_tool_use_id)?;
        }
        match event_type {
            "message_start" => {
                self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::MessageStart;
                self.stream_message_start(event, parent_tool_use_id)
            }
            "content_block_start" => self.stream_content_block_start(event, value),
            "content_block_delta" => self.stream_content_block_delta(event, value),
            "content_block_stop" => self.stream_content_block_stop(event),
            "message_delta" => {
                self.rejection_context.stage =
                    ClaudeCodeStreamJsonV1ProtocolStage::MessageTransition;
                self.stream_message_delta(event)
            }
            "message_stop" => {
                self.rejection_context.stage =
                    ClaudeCodeStreamJsonV1ProtocolStage::MessageTransition;
                self.stream_message_stop(event)
            }
            _ => {
                self.observe_unrecognized(value);
                Ok(())
            }
        }
    }

    fn record_stream_event_identity(&mut self, object: &Map<String, Value>) {
        let Some(event) = required_object(object, "event") else {
            return;
        };
        let Some(event_type) = required_string(event, "type") else {
            return;
        };
        self.rejection_context.stream_event = Some(claude_stream_event_type(event_type));
        if matches!(
            event_type,
            "content_block_start" | "content_block_delta" | "content_block_stop"
        ) {
            self.rejection_context.content_index =
                required_u64(event, "index").map(bounded_sequence);
        }
        self.rejection_context.content_block = match event_type {
            "content_block_start" => required_object(event, "content_block")
                .and_then(|block| required_string(block, "type"))
                .map(claude_content_block_kind),
            "content_block_delta" => required_object(event, "delta")
                .and_then(|delta| required_string(delta, "type"))
                .map(claude_delta_content_block_kind),
            _ => None,
        };
    }

    fn require_active_stream_parent(
        &mut self,
        parent_tool_use_id: Option<&str>,
    ) -> Result<(), AgentFailureCause> {
        let matches = self
            .active_message
            .as_ref()
            .is_some_and(|message| message.parent_tool_use_id.as_deref() == parent_tool_use_id);
        if !matches {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ActiveStreamParentMismatch,
                ClaudeCodeStreamJsonV1ProtocolStage::ActiveStreamParent,
            );
        }
        Ok(())
    }

    fn stream_message_start(
        &mut self,
        event: &Map<String, Value>,
        parent_tool_use_id: Option<&str>,
    ) -> Result<(), AgentFailureCause> {
        if self.active_message.is_some() {
            return Err(self.protocol_failure());
        }
        let message = required_object(event, "message").ok_or_else(|| self.protocol_failure())?;
        let id = required_nonempty_string(message, "id").ok_or_else(|| self.protocol_failure())?;
        let model =
            required_nonempty_string(message, "model").ok_or_else(|| self.protocol_failure())?;
        let usage = required_object(message, "usage").ok_or_else(|| self.protocol_failure())?;
        let input_tokens =
            required_u64(usage, "input_tokens").ok_or_else(|| self.protocol_failure())?;
        let output_tokens =
            required_u64(usage, "output_tokens").ok_or_else(|| self.protocol_failure())?;
        // A subagent (a message under a `parent_tool_use_id`) runs on whatever model the
        // harness's subagent facility chose for it; only the main thread must be the profile's.
        if required_string(message, "type") != Some("message")
            || required_string(message, "role") != Some("assistant")
            || (parent_tool_use_id.is_none() && model != self.expected_model.as_ref())
            || required_array(message, "content").is_none_or(|content| !content.is_empty())
        {
            return Err(self.protocol_failure());
        }
        self.complete_retry();
        if parent_tool_use_id.is_none() {
            // A tool round of this turn whose user message came after a subagent's
            // notification carries that notification to the model: Claude Code opens no turn
            // for it, whether the subagent still holds it or a resume has carried it. A
            // `user` frame of an earlier turn delivers nothing: the turn after it was opened
            // for one dequeued item, and its first request carries that item alone.
            let frames = self.main_user_frames;
            let turn_start = self.turn_main_user_frames;
            let delivered = |notified_after: u64| notified_after.max(turn_start) < frames;
            for subagent in &mut self.subagents {
                if subagent.notified && delivered(subagent.notified_after_user_frames) {
                    subagent.delivered = true;
                }
            }
            self.carried_notifications
                .retain(|&notified_after| !delivered(notified_after));
        }
        self.active_message = Some(StreamedMessage {
            id: Arc::from(id),
            parent_tool_use_id: parent_tool_use_id.map(Arc::from),
            next_index: 0,
            active_block: None,
            completed_blocks: 0,
            text_blocks: 0,
            tool_use_blocks: 0,
            structured_output_candidates: Vec::new(),
            structured_output_call_ids: Vec::new(),
            structured_output_acknowledgements: 0,
            successful_structured_output_acknowledgements: 0,
            response: String::new(),
            input_tokens,
            output_tokens,
            terminal_delta_seen: false,
        });
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageStarted));
        self.observations.push(AgentObservation::Model {
            name: Arc::from(model),
        });
        self.observations.push(AgentObservation::Usage {
            input_tokens,
            output_tokens,
        });
        Ok(())
    }

    fn stream_content_block_start(
        &mut self,
        event: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart;
        self.rejection_context.content_index = required_u64(event, "index").map(bounded_sequence);
        let index = required_u64(event, "index").ok_or_else(|| self.protocol_failure())?;
        let block =
            required_object(event, "content_block").ok_or_else(|| self.protocol_failure())?;
        let block_type = required_string(block, "type").ok_or_else(|| self.protocol_failure())?;
        self.rejection_context.content_block = Some(claude_content_block_kind(block_type));
        let Some(message) = self.active_message.as_ref() else {
            return Err(self.protocol_failure());
        };
        if message.active_block.is_some() || index != message.next_index {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ContentBlockIndexMismatch,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart,
            );
        }

        let maximum_correlation_bytes = self.limits.maximum_frame_bytes().get();
        let active_block = match block_type {
            "text" => {
                let text = required_string(block, "text").ok_or_else(|| self.protocol_failure())?;
                let mut streamed_text = String::new();
                if !bounded_append(&mut streamed_text, text, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart,
                    );
                }
                self.append_assistant_text(text)?;
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(text_blocks) = message.text_blocks.checked_add(1) else {
                    return Err(failure);
                };
                message.text_blocks = text_blocks;
                ActiveContentBlock::Text {
                    text: streamed_text,
                    nominal_seen: false,
                }
            }
            "thinking" => {
                let thinking =
                    required_string(block, "thinking").ok_or_else(|| self.protocol_failure())?;
                let mut streamed_thinking = String::new();
                if !bounded_append(&mut streamed_thinking, thinking, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart,
                    );
                }
                if !thinking.is_empty() {
                    self.observations.push(AgentObservation::Reasoning {
                        text: Arc::from(thinking),
                    });
                }
                ActiveContentBlock::Thinking {
                    thinking: streamed_thinking,
                    nominal_seen: false,
                }
            }
            "tool_use" => {
                let call_id =
                    required_nonempty_string(block, "id").ok_or_else(|| self.protocol_failure())?;
                let name = required_nonempty_string(block, "name")
                    .ok_or_else(|| self.protocol_failure())?;
                let Some(initial_input) = block.get("input").filter(|input| input.is_object())
                else {
                    return Err(self.protocol_failure());
                };
                // A subagent's `Monitor` is its own work, whose task starts are its own.
                let monitor = name == "Monitor" && message.parent_tool_use_id.is_none();
                if name == "Bash" || monitor {
                    if self.command_calls.len() >= 128
                        || self
                            .command_calls
                            .iter()
                            .any(|call| call.id.as_ref() == call_id)
                    {
                        return Err(self.protocol_failure());
                    }
                    self.command_calls.push(CommandCall {
                        id: Arc::from(call_id),
                        monitor,
                    });
                }
                self.observations.push(AgentObservation::ToolCall {
                    call_id: Arc::from(call_id),
                    name: Arc::from(name),
                    phase: AgentToolCallPhase::Started,
                });
                ActiveContentBlock::ToolUse {
                    call_id: Arc::from(call_id),
                    name: Arc::from(name),
                    initial_input: initial_input.clone(),
                    input_json: String::new(),
                    input_delta_seen: false,
                    nominal_seen: false,
                }
            }
            _ => {
                self.observe_unrecognized(value);
                ActiveContentBlock::Unknown {
                    initial_block: Value::Object(block.clone()),
                    delta_seen: false,
                    nominal_seen: false,
                }
            }
        };
        let Some(message) = self.active_message.as_mut() else {
            return Err(AgentFailureCause::HarnessProtocolFailed);
        };
        message.active_block = Some(active_block);
        Ok(())
    }

    fn stream_content_block_delta(
        &mut self,
        event: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta;
        self.rejection_context.content_index = required_u64(event, "index").map(bounded_sequence);
        let index = required_u64(event, "index").ok_or_else(|| self.protocol_failure())?;
        let delta = required_object(event, "delta").ok_or_else(|| self.protocol_failure())?;
        let delta_type = required_string(delta, "type").ok_or_else(|| self.protocol_failure())?;
        self.rejection_context.content_block = Some(claude_delta_content_block_kind(delta_type));
        let Some(message) = self.active_message.as_ref() else {
            return Err(self.protocol_failure());
        };
        if index != message.next_index {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ContentBlockIndexMismatch,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
            );
        }

        match (&message.active_block, delta_type) {
            (Some(ActiveContentBlock::Text { .. }), "text_delta") => {
                let text = required_string(delta, "text").ok_or_else(|| self.protocol_failure())?;
                let maximum_correlation_bytes = self.limits.maximum_frame_bytes().get();
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(ActiveContentBlock::Text { text: streamed, .. }) =
                    message.active_block.as_mut()
                else {
                    return Err(failure);
                };
                if !bounded_append(streamed, text, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
                    );
                }
                self.append_assistant_text(text)
            }
            (Some(ActiveContentBlock::Thinking { .. }), "thinking_delta") => {
                let thinking =
                    required_string(delta, "thinking").ok_or_else(|| self.protocol_failure())?;
                let maximum_correlation_bytes = self.limits.maximum_frame_bytes().get();
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(ActiveContentBlock::Thinking {
                    thinking: streamed, ..
                }) = message.active_block.as_mut()
                else {
                    return Err(failure);
                };
                if !bounded_append(streamed, thinking, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
                    );
                }
                if !thinking.is_empty() {
                    self.observations.push(AgentObservation::Reasoning {
                        text: Arc::from(thinking),
                    });
                }
                Ok(())
            }
            (Some(ActiveContentBlock::Thinking { .. }), "signature_delta") => {
                required_string(delta, "signature")
                    .map(|_| ())
                    .ok_or_else(|| self.protocol_failure())
            }
            (Some(ActiveContentBlock::ToolUse { call_id, name, .. }), "input_json_delta") => {
                let partial_json = required_string(delta, "partial_json")
                    .ok_or_else(|| self.protocol_failure())?;
                let call_id = Arc::clone(call_id);
                let name = Arc::clone(name);
                let maximum_correlation_bytes = self.limits.maximum_frame_bytes().get();
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(ActiveContentBlock::ToolUse {
                    input_json,
                    input_delta_seen,
                    ..
                }) = message.active_block.as_mut()
                else {
                    return Err(failure);
                };
                if !bounded_append(input_json, partial_json, maximum_correlation_bytes) {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
                    );
                }
                *input_delta_seen = true;
                self.observations.push(AgentObservation::ToolCall {
                    call_id,
                    name,
                    phase: AgentToolCallPhase::Updated,
                });
                Ok(())
            }
            (Some(ActiveContentBlock::Unknown { .. }), _) => {
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                let Some(ActiveContentBlock::Unknown { delta_seen, .. }) =
                    message.active_block.as_mut()
                else {
                    return Err(failure);
                };
                *delta_seen = true;
                self.observe_unrecognized(value);
                Ok(())
            }
            _ => self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ContentBlockTypeTransitionInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta,
            ),
        }
    }

    fn stream_content_block_stop(
        &mut self,
        event: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.rejection_context.stage = ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop;
        self.rejection_context.content_index = required_u64(event, "index").map(bounded_sequence);
        let index = required_u64(event, "index").ok_or_else(|| self.protocol_failure())?;
        let Some(message) = self.active_message.as_ref() else {
            return Err(self.protocol_failure());
        };
        if index != message.next_index {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::ContentBlockIndexMismatch,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop,
            );
        }
        if message.active_block.is_none() {
            return Err(self.protocol_failure());
        }
        if message.next_index == u64::MAX || message.completed_blocks == u64::MAX {
            return self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop,
            );
        }
        let block = {
            let failure = self.protocol_failure();
            let Some(message) = self.active_message.as_mut() else {
                return Err(failure.clone());
            };
            let Some(block) = message.active_block.take() else {
                return Err(failure);
            };
            message.next_index += 1;
            message.completed_blocks += 1;
            if matches!(block, ActiveContentBlock::ToolUse { .. }) {
                message.tool_use_blocks = message.tool_use_blocks.saturating_add(1);
            }
            block
        };
        if let ActiveContentBlock::ToolUse {
            call_id,
            name,
            initial_input,
            input_json,
            input_delta_seen,
            ..
        } = block
        {
            if name.as_ref() == STRUCTURED_OUTPUT_TOOL_NAME {
                let candidate = if input_delta_seen {
                    let Ok(candidate) = um_support::strict_json_from_str(&input_json) else {
                        return self.reject(
                            ClaudeCodeStreamJsonV1RejectionReason::ContentBlockCorrelationInvalid,
                            ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop,
                        );
                    };
                    candidate
                } else {
                    initial_input
                };
                let Some(next_candidate_count) =
                    self.exchange_structured_output_candidates.checked_add(1)
                else {
                    return self.reject(
                        ClaudeCodeStreamJsonV1RejectionReason::RetainedStateLimitExceeded,
                        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop,
                    );
                };
                let failure = self.protocol_failure();
                let Some(message) = self.active_message.as_mut() else {
                    return Err(failure);
                };
                message.structured_output_candidates.push(candidate);
                message
                    .structured_output_call_ids
                    .push(Arc::clone(&call_id));
                self.exchange_structured_output_candidates = next_candidate_count;
            }
            self.observations.push(AgentObservation::ToolCall {
                call_id,
                name,
                phase: AgentToolCallPhase::Completed,
            });
        }
        Ok(())
    }

    fn stream_message_delta(
        &mut self,
        event: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let delta = required_object(event, "delta").ok_or_else(|| self.protocol_failure())?;
        let output_tokens = event
            .get("usage")
            .map(|usage| {
                usage
                    .as_object()
                    .and_then(|usage| required_u64(usage, "output_tokens"))
                    .ok_or_else(|| self.protocol_failure())
            })
            .transpose()?;
        let Some(message) = self.active_message.as_mut() else {
            return Err(self.protocol_failure());
        };
        if message.active_block.is_some()
            || message.terminal_delta_seen
            || !delta.get("stop_reason").is_some_and(Value::is_string)
        {
            return Err(self.protocol_failure());
        }
        if let Some(output_tokens) = output_tokens {
            message.output_tokens = output_tokens;
            self.observations.push(AgentObservation::Usage {
                input_tokens: message.input_tokens,
                output_tokens,
            });
        }
        message.terminal_delta_seen = true;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageUpdated));
        Ok(())
    }

    fn stream_message_stop(
        &mut self,
        _event: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let Some(message) = self.active_message.take() else {
            return Err(self.protocol_failure());
        };
        if message.active_block.is_some() || !message.terminal_delta_seen {
            self.active_message = Some(message);
            return Err(self.protocol_failure());
        }
        if message.parent_tool_use_id.is_none() {
            self.exchange_main_messages = self.exchange_main_messages.saturating_add(1);
        }
        if message.parent_tool_use_id.is_none() && !self.epilogue {
            self.final_main_message = Some(CompletedMainMessage {
                text_blocks: message.text_blocks,
                tool_use_blocks: message.tool_use_blocks,
                structured_output_candidates: message.structured_output_candidates,
                structured_output_acknowledgements: message.structured_output_acknowledgements,
                successful_structured_output_acknowledgements: message
                    .successful_structured_output_acknowledgements,
                response: message.response,
            });
        }
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::MessageCompleted));
        Ok(())
    }

    fn append_assistant_text(&mut self, text: &str) -> Result<(), AgentFailureCause> {
        let Some(message) = self.active_message.as_mut() else {
            return Err(self.protocol_failure());
        };
        if message.parent_tool_use_id.is_none() && self.value_kind == AgentValueKind::Response {
            let current = u64::try_from(message.response.len()).unwrap_or(u64::MAX);
            let additional = u64::try_from(text.len()).unwrap_or(u64::MAX);
            if current
                .checked_add(additional)
                .is_none_or(|total| total > self.maximum_response_bytes.get())
            {
                return Err(AgentFailureCause::CapturedValueTooLarge);
            }
            message.response.push_str(text);
        }
        if !text.is_empty() {
            self.observations.push(AgentObservation::AssistantText {
                text: Arc::from(text),
            });
        }
        Ok(())
    }

    fn parse_assistant(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        let parent = parent_tool_use_id(object).ok_or_else(|| self.protocol_failure())?;
        let message = required_object(object, "message").ok_or_else(|| self.protocol_failure())?;
        let message_id =
            required_nonempty_string(message, "id").ok_or_else(|| self.protocol_failure())?;
        let content = required_array(message, "content").ok_or_else(|| self.protocol_failure())?;
        if required_string(message, "type") != Some("message")
            || required_string(message, "role") != Some("assistant")
        {
            return Err(self.protocol_failure());
        }
        if parent.is_none() && required_bool(object, "is_api_error_message") == Some(true) {
            let [block] = content else {
                return Err(self.protocol_failure());
            };
            let block = block.as_object().ok_or_else(|| self.protocol_failure())?;
            let diagnostic = required_nonempty_string(block, "text")
                .filter(|_| required_string(block, "type") == Some("text"))
                .ok_or_else(|| self.protocol_failure())?;
            if self.active_message.is_some() {
                return Err(self.protocol_failure());
            }
            // `request_id` is absent when no request reached the API (the login-less
            // `authentication_failed` answer); it is never required.
            let code =
                required_nonempty_string(object, "error").ok_or_else(|| self.protocol_failure())?;
            let http_status = object
                .get("api_error_status")
                .and_then(Value::as_u64)
                .and_then(|status| u16::try_from(status).ok());
            self.native_api_error = Some(
                HarnessFailureDiagnostic::from_code(code, http_status)
                    .with_limit_reset(self.latest_reset),
            );
            self.observations.push(AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Error,
                message: Arc::from(diagnostic),
            });
            return Ok(());
        }
        let model =
            required_nonempty_string(message, "model").ok_or_else(|| self.protocol_failure())?;
        if parent.is_none() && model != self.expected_model.as_ref() {
            return Err(self.protocol_failure());
        }

        if parent.is_none() {
            let failure = self.protocol_failure();
            let Some(active) = self.active_message.as_mut() else {
                return Err(failure);
            };
            let [nominal_block] = content else {
                return Err(failure);
            };
            if active.id.as_ref() != message_id
                || active.parent_tool_use_id.is_some()
                || active
                    .active_block
                    .as_mut()
                    .is_none_or(|block| !block.correlate_nominal(nominal_block))
            {
                return Err(failure);
            }
            return Ok(());
        }

        for block in content {
            let Some(block) = block.as_object() else {
                return Err(self.protocol_failure());
            };
            match required_string(block, "type") {
                Some("text") => {
                    let text =
                        required_string(block, "text").ok_or_else(|| self.protocol_failure())?;
                    if !text.is_empty() {
                        self.observations.push(AgentObservation::AssistantText {
                            text: Arc::from(text),
                        });
                    }
                }
                Some("thinking") => {
                    let thinking = required_string(block, "thinking")
                        .ok_or_else(|| self.protocol_failure())?;
                    if !thinking.is_empty() {
                        self.observations.push(AgentObservation::Reasoning {
                            text: Arc::from(thinking),
                        });
                    }
                }
                Some("tool_use") => {
                    let call_id = required_nonempty_string(block, "id")
                        .ok_or_else(|| self.protocol_failure())?;
                    let name = required_nonempty_string(block, "name")
                        .ok_or_else(|| self.protocol_failure())?;
                    self.observations.extend([
                        AgentObservation::ToolCall {
                            call_id: Arc::from(call_id),
                            name: Arc::from(name),
                            phase: AgentToolCallPhase::Started,
                        },
                        AgentObservation::ToolCall {
                            call_id: Arc::from(call_id),
                            name: Arc::from(name),
                            phase: AgentToolCallPhase::Completed,
                        },
                    ]);
                }
                _ => self.observe_unrecognized(value),
            }
        }
        Ok(())
    }

    fn parse_user(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        let parent = parent_tool_use_id(object).ok_or_else(|| self.protocol_failure())?;
        let message = required_object(object, "message").ok_or_else(|| self.protocol_failure())?;
        let content = required_array(message, "content").ok_or_else(|| self.protocol_failure())?;
        if required_string(message, "role") != Some("user") {
            return Err(self.protocol_failure());
        }
        if parent.is_none() {
            self.main_user_frames = self.main_user_frames.saturating_add(1);
        }
        for block in content {
            let Some(block) = block.as_object() else {
                return Err(self.protocol_failure());
            };
            if required_string(block, "type") != Some("tool_result") {
                if self.completed_exchanges > 0
                    && self.value_kind != AgentValueKind::Result
                    && parent.is_none()
                {
                    return Err(self.protocol_failure());
                }
                self.observe_unrecognized(value);
                continue;
            }
            let call_id = required_nonempty_string(block, "tool_use_id")
                .ok_or_else(|| self.protocol_failure())?;
            let result = block
                .get("content")
                .and_then(normalized_tool_result_content)
                .ok_or_else(|| self.protocol_failure())?;
            // A Bash call's result names its background task as `backgroundTaskId`; a
            // `Monitor` call's names its task as `taskId`, and the two acknowledge alike.
            let task_field = if self
                .command_calls
                .iter()
                .any(|call| call.monitor && call.id.as_ref() == call_id)
            {
                "taskId"
            } else {
                "backgroundTaskId"
            };
            let background_task_id = object
                .get("tool_use_result")
                .and_then(Value::as_object)
                .and_then(|result| required_nonempty_string(result, task_field));
            if parent.is_some() {
                // A subagent's tool result settles the subagent's own call; a task it
                // names is the subagent's work, never a command this invocation tracks.
            } else if let Some(task_id) = background_task_id {
                let Some(task) = self.background_commands.iter_mut().find(|task| {
                    task.task_id.as_ref() == task_id && task.tool_use_id.as_ref() == call_id
                }) else {
                    return Err(self.protocol_failure());
                };
                if task.acknowledged {
                    return Err(self.protocol_failure());
                }
                // A result naming the task is the harness's word that the command runs in
                // the background now, whether it started there or moved at its timeout.
                task.backgrounded = true;
                task.acknowledged = true;
            } else {
                // A result without a task is a foreground completion: the command's task,
                // if the harness announced one, is over.
                self.background_commands
                    .retain(|task| task.tool_use_id.as_ref() != call_id);
                // An Agent call's result reporting an asynchronous launch is the harness's
                // word that the subagent runs in the background, whether or not its
                // `task_started` said so, or was emitted at all.
                if let Some(launch) = object
                    .get("tool_use_result")
                    .and_then(Value::as_object)
                    .filter(|result| async_agent_launch(result))
                {
                    self.adopt_async_agent_launch(call_id, launch)?;
                }
            }
            let acknowledges_structured_output =
                self.active_message.as_ref().is_some_and(|message| {
                    message
                        .structured_output_call_ids
                        .iter()
                        .any(|expected| expected.as_ref() == call_id)
                });
            // `is_error` is optional on a native tool result and absent means success, as
            // in the Messages API block shape; a present value must still be a boolean.
            let is_error = match block.get("is_error") {
                None => false,
                Some(is_error) => is_error.as_bool().ok_or_else(|| self.protocol_failure())?,
            };
            if acknowledges_structured_output {
                let message = self
                    .active_message
                    .as_mut()
                    .ok_or(AgentFailureCause::HarnessProtocolFailed)?;
                message.structured_output_acknowledgements = message
                    .structured_output_acknowledgements
                    .checked_add(1)
                    .ok_or(AgentFailureCause::HarnessProtocolFailed)?;
                if !is_error {
                    message.successful_structured_output_acknowledgements = message
                        .successful_structured_output_acknowledgements
                        .checked_add(1)
                        .ok_or(AgentFailureCause::HarnessProtocolFailed)?;
                }
            }
            self.observations.push(AgentObservation::ToolResult {
                call_id: Arc::from(call_id),
                is_error,
                content: Arc::from(result),
            });
            // A foreground Bash result no longer needs correlation state. A
            // background result has already been paired with task_started above;
            // its task identity remains until notification/continuation.
            if let Some(index) = self
                .command_calls
                .iter()
                .position(|call| call.id.as_ref() == call_id)
            {
                self.command_calls.remove(index);
            }
        }
        Ok(())
    }

    fn parse_system(
        &mut self,
        object: &Map<String, Value>,
        value: &Value,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        match required_string(object, "subtype").ok_or_else(|| self.protocol_failure())? {
            "status" => {
                let status = required_nonempty_string(object, "status")
                    .ok_or_else(|| self.protocol_failure())?;
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Information,
                    message: Arc::from(status),
                });
            }
            "api_retry" => {
                if !self.exchange_active {
                    return Err(self.protocol_failure());
                }
                if !self.retry_active {
                    self.retry_active = true;
                    self.observations
                        .push(lifecycle(AgentLifecycleMilestone::RetryStarted));
                }
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Warning,
                    message: Arc::from(api_retry_message(object)),
                });
            }
            "compact_boundary" => self
                .observations
                .push(lifecycle(AgentLifecycleMilestone::CompactionCompleted)),
            "task_started" => {
                // A subagent's own tool runs belong to the subagent, not to this
                // invocation: they are never tracked and owe nothing.
                if required_bool(object, "owned_by_subagent") == Some(true) {
                    self.remember_subagent_work(object);
                    return Ok(());
                }
                if required_string(object, "task_type") == Some("local_agent") {
                    return self.start_subagent(object);
                }
                if unmodeled_task_type(object) {
                    self.remember_unmodeled_task(object, value);
                    return Ok(());
                }
                // Every Bash call starts a `local_bash` task, foreground or not, and so does
                // a `Monitor` call, backgrounded, for the command it watches. The harness
                // may start it as soon as the tool-use block closes, before the message's
                // own `message_delta`, so an active message is not a violation. A
                // foreground task is tracked so that the harness may move it to the
                // background later (at its foreground timeout) without the parser losing
                // the correlation; a start naming an unknown call is unrecognized unless
                // it claims the background, which is a protocol failure as before.
                let backgrounded = required_bool(object, "is_backgrounded") == Some(true);
                if !self.exchange_active {
                    return Err(self.protocol_failure());
                }
                let task_id = required_nonempty_string(object, "task_id")
                    .ok_or_else(|| self.protocol_failure())?;
                let tool_use_id = required_nonempty_string(object, "tool_use_id")
                    .ok_or_else(|| self.protocol_failure())?;
                let Some(monitor) = self
                    .command_calls
                    .iter()
                    .find(|call| call.id.as_ref() == tool_use_id)
                    .map(|call| call.monitor)
                else {
                    if backgrounded {
                        return Err(self.protocol_failure());
                    }
                    self.observe_unrecognized(value);
                    return Ok(());
                };
                if self.background_commands.len() >= MAXIMUM_TRACKED_TASKS
                    || self.background_commands.iter().any(|task| {
                        task.task_id.as_ref() == task_id || task.tool_use_id.as_ref() == tool_use_id
                    })
                    || self.tracks_subagent(task_id)
                {
                    return Err(self.protocol_failure());
                }
                self.background_commands.push(BackgroundCommand {
                    task_id: Arc::from(task_id),
                    tool_use_id: Arc::from(tool_use_id),
                    run_id: required_nonempty_string(object, "run_id").map(Arc::from),
                    backgrounded,
                    acknowledged: false,
                    notified: false,
                    monitor,
                });
            }
            "task_progress" => {
                if !required_nonempty_string(object, "task_id").is_some_and(|task_id| {
                    self.tracks_subagent(task_id) || self.is_subagent_work(task_id)
                }) {
                    self.observe_unrecognized(value);
                }
            }
            "background_tasks_changed" => {
                let membership_is_tracked = required_array(object, "tasks").is_some_and(|tasks| {
                    tasks.iter().all(|entry| {
                        entry
                            .as_object()
                            .and_then(|entry| required_nonempty_string(entry, "task_id"))
                            .is_some_and(|task_id| {
                                self.tracks_task(task_id) || self.is_subagent_work(task_id)
                            })
                    })
                });
                if !membership_is_tracked {
                    self.observe_unrecognized(value);
                }
            }
            "task_updated" | "task_notification" => {
                let task_id = required_nonempty_string(object, "task_id")
                    .ok_or_else(|| self.protocol_failure())?;
                if self.tracks_subagent(task_id) {
                    return self.settle_subagent(object, task_id);
                }
                let subagent_work = self.is_subagent_work(task_id);
                let Some(task) = self
                    .background_commands
                    .iter_mut()
                    .find(|task| task.task_id.as_ref() == task_id)
                else {
                    if subagent_work {
                        return Ok(());
                    }
                    if self.exchange_active || self.is_unmodeled_task(task_id) {
                        self.observe_unrecognized(value);
                        return Ok(());
                    }
                    return Err(self.protocol_failure());
                };
                if required_nonempty_string(object, "run_id") != task.run_id.as_deref() {
                    return Err(self.protocol_failure());
                }
                if required_string(object, "subtype") == Some("task_updated") {
                    // The harness moves a foreground command to the background when it
                    // outlives the foreground timeout; the patch is the only announcement.
                    if required_object(object, "patch")
                        .and_then(|patch| required_bool(patch, "is_backgrounded"))
                        == Some(true)
                    {
                        task.backgrounded = true;
                    }
                    return Ok(());
                }
                if required_nonempty_string(object, "tool_use_id")
                    != Some(task.tool_use_id.as_ref())
                    || required_nonempty_string(object, "status").is_none()
                {
                    return Err(self.protocol_failure());
                }
                if !task.backgrounded {
                    // A foreground command's completion report precedes its tool result
                    // and settles nothing: the result is what retires the call.
                    let task_id: Arc<str> = Arc::clone(&task.task_id);
                    self.background_commands
                        .retain(|task| task.task_id != task_id);
                    return Ok(());
                }
                // The status is the harness's classification of a background command's
                // end (`completed`, or a stop at the background limit); any end settles it.
                if !task.acknowledged || task.notified {
                    return Err(self.protocol_failure());
                }
                task.notified = true;
            }
            _ => self.observe_unrecognized(value),
        }
        Ok(())
    }

    /// Whether a notification stands that announces a drain `init`: an empty turn's
    /// announcement, a notification a resume carried past, a completed background command
    /// or subagent, or an outstanding monitor, each line of whose command opens a turn with
    /// nothing on the stream before its `init`.
    fn notification_announces_a_turn(&self) -> bool {
        self.announced_turn_pending
            || !self.carried_notifications.is_empty()
            || self
                .background_commands
                .iter()
                .any(|task| task.notified || (task.monitor && task.awaits_notification()))
            || self.subagents.iter().any(|subagent| subagent.notified)
    }

    /// A wakeup `init` forgets the notified subagents as outstanding work; their identity
    /// is kept, newest last, so that a later message to one resumes it (`start_subagent`).
    fn forget_notified_subagents(&mut self) {
        let (mut forgotten, outstanding): (Vec<_>, Vec<_>) = self
            .subagents
            .drain(..)
            .partition(|subagent| subagent.notified);
        self.subagents = outstanding;
        // The turn this `init` opens delivers their notifications: a later resume carries
        // nothing past.
        for subagent in &mut forgotten {
            subagent.delivered = true;
        }
        self.forgotten_subagents.extend(forgotten);
        keep_newest(&mut self.forgotten_subagents);
    }

    fn tracks_subagent(&self, task_id: &str) -> bool {
        self.subagents
            .iter()
            .any(|subagent| subagent.task_id.as_ref() == task_id)
    }

    fn tracks_task(&self, task_id: &str) -> bool {
        self.tracks_subagent(task_id)
            || self
                .background_commands
                .iter()
                .any(|task| task.task_id.as_ref() == task_id)
    }

    /// Whether the Agent call `tool_use_id` started a backgrounded subagent that has not
    /// yet settled, so that a message naming it as parent is that subagent's activity.
    fn awaits_subagent(&self, tool_use_id: &str) -> bool {
        self.subagents.iter().any(|subagent| {
            subagent.awaits_notification() && subagent.agent_call_id.as_ref() == tool_use_id
        })
    }

    /// Keeps the task id a `task_started owned_by_subagent: true` introduced, so that the
    /// subagent's own work stays its work after the subagent settles.
    fn remember_subagent_work(&mut self, object: &Map<String, Value>) {
        if let Some(task_id) = self.untracked_task_id(object) {
            remember_task_id(&mut self.subagent_owned_tasks, task_id);
        }
    }

    /// Records the start of a task of a kind the parser does not model as unrecognized and
    /// keeps its id, so that every later frame naming it is its activity (REQ-AGENT-38).
    fn remember_unmodeled_task(&mut self, object: &Map<String, Value>, value: &Value) {
        self.observe_unrecognized(value);
        if let Some(task_id) = self.untracked_task_id(object) {
            remember_task_id(&mut self.unmodeled_tasks, task_id);
        }
    }

    /// The `task_id` a task frame names, unless it names a tracked command or subagent.
    fn untracked_task_id<'a>(&self, object: &'a Map<String, Value>) -> Option<&'a str> {
        required_nonempty_string(object, "task_id").filter(|task_id| !self.tracks_task(task_id))
    }

    /// A task of a kind the parser does not model, which no tracked task shadows.
    fn is_unmodeled_task(&self, task_id: &str) -> bool {
        !self.tracks_task(task_id)
            && self
                .unmodeled_tasks
                .iter()
                .any(|known| known.as_ref() == task_id)
    }

    /// A task event naming no tracked task is a subagent's own work when a subagent's
    /// start introduced it, or while a backgrounded subagent is outstanding; the subagent,
    /// not this invocation, owns it.
    fn is_subagent_work(&self, task_id: &str) -> bool {
        !self.tracks_task(task_id)
            && (self
                .subagent_owned_tasks
                .iter()
                .any(|known| known.as_ref() == task_id)
                || self.subagents.iter().any(Subagent::awaits_notification))
    }

    fn forgotten_subagent(&self, task_id: &str) -> Option<usize> {
        self.forgotten_subagents
            .iter()
            .position(|subagent| subagent.task_id.as_ref() == task_id)
    }

    /// A subagent that has settled and may be resumed: a tracked one that is notified
    /// (no wakeup `init` has forgotten it yet) or a forgotten one.
    fn settled_subagent(&self, task_id: &str) -> bool {
        self.subagents
            .iter()
            .any(|subagent| subagent.task_id.as_ref() == task_id && subagent.notified)
            || self.forgotten_subagent(task_id).is_some()
    }

    pub(crate) const fn result_accepted(&self) -> bool {
        self.accepted_result.is_some()
    }

    /// Whether nothing is open or outstanding: no exchange or epilogue is under way and no
    /// background work owes a notification. The adapter's settlement grace runs only then
    /// (INV-HARNESS-7).
    pub(crate) fn settling(&self) -> bool {
        !self.exchange_active
            && !self.announced_turn_pending
            && !self.correction_pending
            && !self.awaits_background_work()
    }

    /// A turn a notified task announced, or an epilogue, that Claude Code opened and closed
    /// without a model call: `init`, then a `result` with `num_turns: 0` and no main-thread
    /// message (2.1.310 emits one for a backgrounded subagent's notification that its own
    /// resume superseded, with no `terminal_reason`). The turn decides and settles nothing.
    /// Before the value settled, the announcement it consumed stands for the next `init`,
    /// which delivers the notification (REQ-AGENT-69); read in the correction window it
    /// consumes the carried notification it was and the correction prompt stays owed. After
    /// the value settled no `init` needs one (REQ-AGENT-76), so a trailing empty turn leaves
    /// the parser `settling()` for the grace (INV-HARNESS-7) and `finish` clean. The same
    /// `result` in a turn nothing announced before settlement, the shape of a first prompt a
    /// hook blocked, never reaches here.
    fn complete_empty_turn(&mut self) -> Result<(), AgentFailureCause> {
        if self.correction_pending {
            // Read in the correction window, the turn was the carried notification's, which
            // it consumes here; the correction prompt stays owed (`correction_pending`) and
            // announces the next `init` itself, so no announcement is carried over.
            self.correction_window_notification = None;
        } else {
            self.announced_turn_pending = !self.value_settled;
        }
        self.epilogue = false;
        self.exchange_initialized = false;
        self.exchange_active = false;
        self.completed_exchanges = self
            .completed_exchanges
            .checked_add(1)
            .ok_or_else(|| self.protocol_failure())?;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnCompleted));
        Ok(())
    }

    /// Background commands and backgrounded subagents that still owe a notification.
    fn awaits_background_work(&self) -> bool {
        self.background_commands
            .iter()
            .any(BackgroundCommand::awaits_notification)
            || self.subagents.iter().any(Subagent::awaits_notification)
    }

    fn start_subagent(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !self.exchange_active {
            return Err(self.protocol_failure());
        }
        let task_id =
            required_nonempty_string(object, "task_id").ok_or_else(|| self.protocol_failure())?;
        let tool_use_id = required_nonempty_string(object, "tool_use_id")
            .ok_or_else(|| self.protocol_failure())?;
        let run_id = required_nonempty_string(object, "run_id").map(Arc::from);
        let backgrounded = required_bool(object, "is_backgrounded") == Some(true);
        let holds_tool_use = |subagent: &Subagent| subagent.tool_use_id.as_ref() == tool_use_id;
        if let Some(index) = self
            .subagents
            .iter()
            .position(|subagent| subagent.task_id.as_ref() == task_id)
        {
            // A message the agent sends a finished backgrounded subagent (`SendMessage`)
            // resumes it: the harness announces the resume as a `task_started` naming the
            // subagent's `task_id` under the message's `tool_use_id` and a new `run_id`, and
            // the resumed run owes a notification of its own. A message to a subagent still
            // running is queued for it and starts nothing, so a repeat of a `task_id` that
            // is not yet notified stays a protocol failure, as does one under a
            // `tool_use_id` another tracked subagent holds.
            let tracked = &mut self.subagents[index];
            if !tracked.announced && holds_tool_use(tracked) {
                // The start of a launch the Agent call's result already announced
                // (`adopt_async_agent_launch`): it names the run and the subagent.
                tracked.announced = true;
                tracked.run_id = run_id;
                tracked.backgrounded |= backgrounded;
                tracked.subagent_type = required_nonempty_string(object, "subagent_type")
                    .map(|kind| Arc::from(ellipsize(kind, MAXIMUM_SUBAGENT_TOKEN_WIDTH)));
                if let Some(description) = required_nonempty_string(object, "description") {
                    tracked.description = Some(Arc::from(ellipsize(
                        description,
                        MAXIMUM_SUBAGENT_DESCRIPTION_WIDTH,
                    )));
                }
                return Ok(());
            }
            return self.resume_subagent(index, object);
        }
        if let Some(index) = self.forgotten_subagent(task_id) {
            return self.resume_forgotten_subagent(index, object);
        }
        if self.subagents.len() >= MAXIMUM_TRACKED_TASKS
            || self.tracks_task(task_id)
            || self.subagents.iter().any(holds_tool_use)
        {
            return Err(self.protocol_failure());
        }
        let subagent = Subagent {
            task_id: Arc::from(task_id),
            agent_call_id: Arc::from(tool_use_id),
            tool_use_id: Arc::from(tool_use_id),
            run_id,
            subagent_type: required_nonempty_string(object, "subagent_type")
                .map(|kind| Arc::from(ellipsize(kind, MAXIMUM_SUBAGENT_TOKEN_WIDTH))),
            description: required_nonempty_string(object, "description")
                .map(|text| Arc::from(ellipsize(text, MAXIMUM_SUBAGENT_DESCRIPTION_WIDTH))),
            announced: true,
            backgrounded,
            notified: false,
            notified_after_user_frames: 0,
            delivered: false,
        };
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Information,
            message: Arc::from(subagent.message("started")),
        });
        self.subagents.push(subagent);
        Ok(())
    }

    /// Resumes the tracked subagent at `index`, which must have settled, under the tool
    /// call the start names (a `SendMessage` call, or any call no other tracked subagent
    /// holds when Claude Code resumes it on its own), and reports the resume; the resumed
    /// run owes a notification of its own.
    fn resume_subagent(
        &mut self,
        index: usize,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let tool_use_id = required_nonempty_string(object, "tool_use_id")
            .ok_or_else(|| self.protocol_failure())?;
        let other_holds_tool_use = self.subagents.iter().enumerate().any(|(other, subagent)| {
            other != index && subagent.tool_use_id.as_ref() == tool_use_id
        });
        if !self.subagents[index].notified || other_holds_tool_use {
            return Err(self.protocol_failure());
        }
        // The notification of the run that ended is still queued unless a main-thread tool
        // round took it: its turn is carried past the resume (REQ-AGENT-69).
        if !self.subagents[index].delivered {
            self.carried_notifications
                .push(self.subagents[index].notified_after_user_frames);
        }
        let message = self.subagents[index].resume(
            tool_use_id,
            required_nonempty_string(object, "run_id").map(Arc::from),
            required_bool(object, "is_backgrounded") == Some(true),
        );
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Information,
            message: Arc::from(message),
        });
        Ok(())
    }

    /// The same resume from a later turn, or the one Claude Code begins in the drain to
    /// deliver the report of a command the subagent left running: the `init` its
    /// notification authorized forgot the subagent as outstanding work, but its resumed
    /// run's messages still name the Agent call that launched it, so it is tracked again
    /// under that call.
    fn resume_forgotten_subagent(
        &mut self,
        index: usize,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if self.subagents.len() >= MAXIMUM_TRACKED_TASKS {
            return Err(self.protocol_failure());
        }
        let subagent = self.forgotten_subagents.remove(index);
        self.subagents.push(subagent);
        self.resume_subagent(self.subagents.len() - 1, object)
    }

    /// The drain's harness-initiated resume of a settled subagent (`settled_subagent`):
    /// in place while the subagent is still tracked, else from the forgotten list.
    fn resume_settled_subagent(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        let task_id =
            required_nonempty_string(object, "task_id").ok_or_else(|| self.protocol_failure())?;
        if let Some(index) = self
            .subagents
            .iter()
            .position(|subagent| subagent.task_id.as_ref() == task_id)
        {
            return self.resume_subagent(index, object);
        }
        let index = self
            .forgotten_subagent(task_id)
            .ok_or_else(|| self.protocol_failure())?;
        self.resume_forgotten_subagent(index, object)
    }

    /// The Agent call `call_id` returned `status: async_launched`: the subagent it started is
    /// background work from here on. The subagent its `task_started` tracked is promoted;
    /// when no start announced it, the result's `agentId` (the task id) tracks it under the
    /// call, with no run id, matched by task id and tool call alone.
    fn adopt_async_agent_launch(
        &mut self,
        call_id: &str,
        launch: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if let Some(subagent) = self
            .subagents
            .iter_mut()
            .find(|subagent| subagent.tool_use_id.as_ref() == call_id)
        {
            subagent.backgrounded = true;
            return Ok(());
        }
        let Some(task_id) = required_nonempty_string(launch, "agentId") else {
            return Ok(());
        };
        if self.subagents.len() >= MAXIMUM_TRACKED_TASKS || self.tracks_task(task_id) {
            return Err(self.protocol_failure());
        }
        let subagent = Subagent {
            task_id: Arc::from(task_id),
            agent_call_id: Arc::from(call_id),
            tool_use_id: Arc::from(call_id),
            run_id: None,
            subagent_type: None,
            description: required_nonempty_string(launch, "description")
                .map(|text| Arc::from(ellipsize(text, MAXIMUM_SUBAGENT_DESCRIPTION_WIDTH))),
            announced: false,
            backgrounded: true,
            notified: false,
            notified_after_user_frames: 0,
            delivered: false,
        };
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Information,
            message: Arc::from(subagent.message("started")),
        });
        self.subagents.push(subagent);
        Ok(())
    }

    /// `task_updated` for a tracked subagent is recognized silently, save a
    /// `patch.is_backgrounded: true` for the current run, which moves the subagent to the
    /// background as it does a command: Claude Code registers an `Agent` call made with
    /// `run_in_background` in the foreground at times and announces the move only so. Its
    /// `task_notification` names the status. A foreground subagent's notification retires
    /// it; a backgrounded one's must correlate exactly and settles what it owes.
    fn settle_subagent(
        &mut self,
        object: &Map<String, Value>,
        task_id: &str,
    ) -> Result<(), AgentFailureCause> {
        let Some(index) = self
            .subagents
            .iter()
            .position(|subagent| subagent.task_id.as_ref() == task_id)
        else {
            return Err(self.protocol_failure());
        };
        if required_string(object, "subtype") != Some("task_notification") {
            let moved = required_object(object, "patch")
                .and_then(|patch| required_bool(patch, "is_backgrounded"))
                == Some(true)
                && required_nonempty_string(object, "run_id")
                    == self.subagents[index].run_id.as_deref();
            if moved {
                self.subagents[index].backgrounded = true;
            }
            return Ok(());
        }
        let subagent = &self.subagents[index];
        let tool_use_id = required_nonempty_string(object, "tool_use_id");
        let status = required_nonempty_string(object, "status");
        let invalid = if subagent.backgrounded {
            subagent.notified
                || tool_use_id != Some(subagent.tool_use_id.as_ref())
                || subagent.run_id.as_deref().is_some_and(|run_id| {
                    required_nonempty_string(object, "run_id") != Some(run_id)
                })
                || status.is_none()
        } else {
            tool_use_id.is_some_and(|id| id != subagent.tool_use_id.as_ref())
        };
        if invalid {
            return Err(self.protocol_failure());
        }
        let status = status.map_or_else(
            || Arc::from("settled"),
            |status| Arc::from(ellipsize(status, MAXIMUM_SUBAGENT_TOKEN_WIDTH)),
        );
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Information,
            message: Arc::from(subagent.message(&status)),
        });
        if subagent.backgrounded {
            let subagent = &mut self.subagents[index];
            subagent.notified = true;
            subagent.notified_after_user_frames = self.main_user_frames;
            subagent.delivered = false;
        } else {
            self.subagents.remove(index);
        }
        Ok(())
    }

    fn parse_rate_limit_event(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        // Claude Code reports its usage windows on every turn; only a window that is
        // warning or rejecting is worth an operator's attention.
        let info = object.get("rate_limit_info").and_then(Value::as_object);
        let status = info.and_then(|info| required_nonempty_string(info, "status"));
        if status == Some("allowed") {
            return Ok(());
        }
        if let Some(reset) = info
            .and_then(|info| info.get("resetsAt"))
            .and_then(Value::as_u64)
        {
            self.latest_reset = Some(reset);
        }
        let window = info.and_then(|info| required_nonempty_string(info, "rateLimitType"));
        let message = match (status, window) {
            (Some(status), Some(window)) => format!("provider rate limit {status} ({window})"),
            (Some(status), None) => format!("provider rate limit {status}"),
            (None, _) => String::from("provider rate limit"),
        };
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Warning,
            message: Arc::from(message),
        });
        Ok(())
    }

    fn parse_tool_progress(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        self.require_matching_session(object)?;
        let call_id = required_nonempty_string(object, "tool_use_id")
            .ok_or_else(|| self.protocol_failure())?;
        let name =
            required_nonempty_string(object, "tool_name").ok_or_else(|| self.protocol_failure())?;
        // A heartbeat for a long-running call carries the call's id with a
        // `-heartbeat-<n>` suffix; the observation names the call itself.
        let call_id = if required_bool(object, "heartbeat") == Some(true) {
            heartbeat_call_id(call_id)
        } else {
            call_id
        };
        self.observations.push(tool_call_observation(
            call_id,
            name,
            AgentToolCallPhase::Updated,
        ));
        Ok(())
    }

    fn parse_result(&mut self, object: &Map<String, Value>) -> Result<(), AgentFailureCause> {
        if !self.exchange_initialized
            || self.active_message.is_some()
            || self.completed_result_exchange.is_some()
            || self.result_decision_pending
            || (self.accepted_result.is_some() && !self.epilogue)
        {
            return Err(self.protocol_failure());
        }
        self.require_matching_session(object)?;
        if (self.announced_exchange || self.epilogue)
            && self.exchange_main_messages == 0
            && object.get("num_turns").and_then(Value::as_u64) == Some(0)
            && required_bool(object, "is_error") == Some(false)
            && required_nonempty_string(object, "subtype") == Some("success")
        {
            return self.complete_empty_turn();
        }
        let epilogue = std::mem::take(&mut self.epilogue);
        let subtype =
            required_nonempty_string(object, "subtype").ok_or_else(|| self.protocol_failure())?;
        let is_error = required_bool(object, "is_error").ok_or_else(|| self.protocol_failure())?;
        let terminal_reason = required_nonempty_string(object, "terminal_reason")
            .ok_or_else(|| self.protocol_failure())?;
        let successful = !is_error;
        if successful {
            if subtype != "success"
                || terminal_reason != "completed"
                || required_string(object, "result").is_none()
            {
                return Err(self.protocol_failure());
            }
        } else {
            if terminal_reason == "completed" {
                return Err(self.protocol_failure());
            }
            // terminal_reason and subtype are harness-owned classifications; result text is not.
            let code = match terminal_reason {
                "rate_limit" | "rate_limited" => terminal_reason,
                "context_window_exceeded"
                | "prompt_too_long"
                | "usage_limit_exceeded"
                | "unauthorized"
                | "overloaded"
                | "bad_request"
                | "server_error"
                | "sandbox_error" => terminal_reason,
                _ => subtype,
            };
            let http_status = object
                .get("api_error_status")
                .and_then(Value::as_u64)
                .and_then(|status| u16::try_from(status).ok());
            let mut diagnostic = if terminal_reason == "api_error" {
                self.native_api_error
                    .unwrap_or_else(|| HarnessFailureDiagnostic::from_code(code, None))
            } else {
                HarnessFailureDiagnostic::from_code(code, None)
            }
            .with_limit_reset(self.latest_reset);
            diagnostic.http_status = http_status.or(diagnostic.http_status);
            if epilogue {
                // The value settled before this turn began: its failure is reported and
                // the value stands (REQ-AGENT-76).
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Warning,
                    message: Arc::from(epilogue_failure_message(diagnostic)),
                });
            } else {
                self.native_failure = Some(AgentHarnessFailureDetail::Classified {
                    canonical: CanonicalHarnessFailure::ModelError,
                    diagnostic,
                });
            }
        }

        let completes_harness = self.value_kind != AgentValueKind::Result;
        if epilogue {
            // An epilogue's result, structured output included, never replaces the value.
        } else if !completes_harness {
            let exchange = if !successful {
                Some(CompletedResultExchange::NativeFailure)
            } else if !self.awaits_background_work() {
                Some(self.extract_result_candidate(object))
            } else if object.contains_key("structured_output") {
                Some(CompletedResultExchange::PrematureCandidate)
            } else {
                // The agent ended its turn to wait for background work: the exchange
                // pauses with no decision until a notification or wakeup begins the next.
                None
            };
            self.result_decision_pending = matches!(
                exchange,
                Some(
                    CompletedResultExchange::Candidate(_)
                        | CompletedResultExchange::AmbiguousCandidate
                        | CompletedResultExchange::PrematureCandidate
                )
            );
            self.premature_candidate_pending =
                matches!(exchange, Some(CompletedResultExchange::PrematureCandidate));
            self.completed_result_exchange = exchange;
        } else if object.contains_key("structured_output") {
            return Err(self.protocol_failure());
        }

        self.complete_retry();
        self.exchange_initialized = false;
        self.exchange_active = false;
        self.correction_pending = false;
        // A turn with a model call in the correction window discharges the correction
        // prompt's duty, whichever prompt it answered; the notification it was opened for,
        // which it did not consume, stays the allowance that admits the correction turn.
        if let Some(notified_after) = self.correction_window_notification.take() {
            self.carried_notifications.insert(0, notified_after);
        }
        // A foreground subagent the exchange did not settle carries no completion duty; a
        // backgrounded one still owes its notification.
        self.subagents.retain(|subagent| subagent.backgrounded);
        self.completed_exchanges = self
            .completed_exchanges
            .checked_add(1)
            .ok_or_else(|| self.protocol_failure())?;
        self.observations
            .push(lifecycle(AgentLifecycleMilestone::TurnCompleted));
        if completes_harness
            && !self.value_settled
            && self.background_commands.iter().all(|task| task.notified)
            && self.subagents.iter().all(|subagent| subagent.notified)
        {
            self.value_settled = true;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::HarnessCompleted));
        }
        Ok(())
    }

    fn extract_result_candidate(&self, object: &Map<String, Value>) -> CompletedResultExchange {
        let Some(structured_output) = object.get("structured_output") else {
            return CompletedResultExchange::MissingCandidate;
        };
        let Some(envelope) = structured_output.as_object() else {
            return CompletedResultExchange::AmbiguousCandidate;
        };
        let Some(candidate) = envelope.get("result") else {
            return CompletedResultExchange::AmbiguousCandidate;
        };
        let Some(message) = self.final_main_message.as_ref() else {
            return CompletedResultExchange::AmbiguousCandidate;
        };
        // Thinking and text siblings are observational; a second tool call in the
        // candidate's message is other native work and makes the candidate ambiguous.
        if self.exchange_structured_output_candidates != 1
            || message.tool_use_blocks != 1
            || message.structured_output_candidates.len() != 1
            || message.structured_output_candidates.first() != Some(structured_output)
            || message.structured_output_acknowledgements != 1
            || message.successful_structured_output_acknowledgements != 1
        {
            return CompletedResultExchange::AmbiguousCandidate;
        }
        CompletedResultExchange::Candidate(Arc::new(candidate.clone()))
    }

    fn complete_retry(&mut self) {
        if self.retry_active {
            self.retry_active = false;
            self.observations
                .push(lifecycle(AgentLifecycleMilestone::RetryCompleted));
        }
    }

    fn require_matching_session(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), AgentFailureCause> {
        if required_string(object, "session_id")
            == self.session_id.as_ref().map(AsRef::<str>::as_ref)
        {
            Ok(())
        } else {
            self.reject(
                ClaudeCodeStreamJsonV1RejectionReason::SessionCorrelationInvalid,
                ClaudeCodeStreamJsonV1ProtocolStage::SessionCorrelation,
            )
        }
    }

    /// Observes an unrecognized event once per native `type`/`subtype` kind. Kinds that
    /// cannot be remembered, because a token is long or the set is full, are observed
    /// every time.
    fn observe_unrecognized(&mut self, value: &Value) {
        let kind = value.as_object().and_then(|object| {
            let event_type = required_string(object, "type")?;
            let subtype = required_string(object, "subtype");
            (event_type.len() <= MAXIMUM_UNRECOGNIZED_KIND_TOKEN_BYTES
                && subtype
                    .is_none_or(|subtype| subtype.len() <= MAXIMUM_UNRECOGNIZED_KIND_TOKEN_BYTES))
            .then_some((event_type, subtype))
        });
        if let Some((event_type, subtype)) = kind {
            if self
                .observed_unrecognized_kinds
                .iter()
                .any(|(seen_type, seen_subtype)| {
                    seen_type.as_ref() == event_type && seen_subtype.as_deref() == subtype
                })
            {
                return;
            }
            if self.observed_unrecognized_kinds.len() < MAXIMUM_OBSERVED_UNRECOGNIZED_KINDS {
                self.observed_unrecognized_kinds
                    .push((Box::from(event_type), subtype.map(Box::from)));
            }
        }
        self.observations
            .push(AgentObservation::UnrecognizedHarnessEvent {
                event: Arc::new(value.clone()),
            });
    }

    fn invalidate_values(&mut self) {
        self.active_message = None;
        self.final_main_message = None;
        self.completed_result_exchange = None;
        self.result_decision_pending = false;
        self.premature_candidate_pending = false;
        self.accepted_result = None;
    }

    /// Records `failure` as the stream's, unless a native failure already ended the
    /// invocation: every frame after a failed `result` is the drain of work that failure
    /// has doomed, so a frame refused there, the `init` of the turn Claude Code begins when
    /// the work finishes first among them, ends the stream with the native failure and no
    /// rejection record (INV-HARNESS-5, REQ-AGENT-75).
    fn fail_stream(&mut self, failure: AgentFailureCause) -> AgentFailureCause {
        let failure = match self.native_failure {
            Some(detail) => {
                self.protocol_rejection = None;
                self.rejected_line = None;
                AgentFailureCause::HarnessFailed { detail }
            }
            None => failure,
        };
        self.failure = Some(failure.clone());
        failure
    }

    // Claude initialization, rather than Pi's session-plus-agent-start sequence, owns the
    // start-to-protocol failure transition; keep that authority in this parser.
    fn protocol_failure(&self) -> AgentFailureCause {
        if self.session_id.is_some() {
            AgentFailureCause::HarnessProtocolFailed
        } else {
            AgentFailureCause::start_failure("launch preparation", "unavailable")
        }
    }

    fn fail_initial_input(
        &mut self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
    ) -> AgentFailureCause {
        let stage = ClaudeCodeStreamJsonV1ProtocolStage::InitialInput;
        self.prepare_rejection(stage);
        self.record_rejection(reason, stage);
        self.invalidate_values();
        let failure = AgentFailureCause::HarnessSetupFailed {
            stage: super::agent::AgentHarnessSetupStage::Initialization,
        };
        self.failure = Some(failure.clone());
        failure
    }

    fn agent_failure(&self, cause: AgentFailureCause) -> AgentFailure {
        match &self.protocol_rejection {
            Some(rejection) => AgentFailure::with_protocol_rejection(cause, rejection.clone())
                .with_stream_excerpt(self.stream_excerpt())
                .with_tracked_work(self.tracked_work()),
            None => AgentFailure::new(cause),
        }
    }

    fn retained_rejected_line(&self, line: &[u8]) -> RetainedStreamLine {
        RetainedStreamLine::retain(line, self.limits.maximum_rejected_line_bytes())
    }

    fn stream_excerpt(&self) -> RejectedStreamExcerpt {
        RejectedStreamExcerpt::new(self.rejected_line.clone(), self.stream_tail.lines())
    }

    /// The ledger as the parser holds it when it refuses: no refusing path mutates the
    /// ledger first, so this agrees with the counts the rejection state took before the
    /// frame.
    fn tracked_work(&self) -> ClaudeCodeStreamJsonV1TrackedWork {
        let (background_commands, commands_cut) =
            newest_tracked(&self.background_commands, |task| {
                ClaudeCodeStreamJsonV1TrackedCommand {
                    task_id: tracked_work_id(&task.task_id),
                    tool_use_id: tracked_work_id(&task.tool_use_id),
                    run_id: task.run_id.as_deref().map(tracked_work_id),
                    backgrounded: task.backgrounded,
                    acknowledged: task.acknowledged,
                    notified: task.notified,
                    monitor: task.monitor,
                }
            });
        let (subagents, subagents_cut) = newest_tracked(&self.subagents, |subagent| {
            ClaudeCodeStreamJsonV1TrackedSubagent {
                task_id: tracked_work_id(&subagent.task_id),
                agent_call_id: tracked_work_id(&subagent.agent_call_id),
                tool_use_id: tracked_work_id(&subagent.tool_use_id),
                run_id: subagent.run_id.as_deref().map(tracked_work_id),
                backgrounded: subagent.backgrounded,
                notified: subagent.notified,
            }
        });
        let (forgotten_subagents, forgotten_cut) =
            newest_tracked(&self.forgotten_subagents, |subagent| {
                tracked_work_id(&subagent.task_id)
            });
        ClaudeCodeStreamJsonV1TrackedWork {
            background_commands,
            subagents,
            forgotten_subagents,
            truncated: commands_cut || subagents_cut || forgotten_cut,
        }
    }

    fn prepare_rejection(&mut self, stage: ClaudeCodeStreamJsonV1ProtocolStage) {
        self.rejection_context = ClaudeCodeStreamJsonV1RejectionContext {
            stage,
            ..ClaudeCodeStreamJsonV1RejectionContext::default()
        };
        self.rejection_state_snapshot = Some(self.rejection_state());
    }

    fn reject<T>(
        &mut self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) -> Result<T, AgentFailureCause> {
        let cause = self.protocol_failure();
        self.record_rejection(reason, stage);
        Err(cause)
    }

    fn fail_protocol<T>(
        &mut self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) -> Result<T, AgentFailureCause> {
        Err(self.record_protocol_failure(reason, stage))
    }

    fn record_protocol_failure(
        &mut self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) -> AgentFailureCause {
        self.prepare_rejection(stage);
        let failure = self.protocol_failure();
        self.record_rejection(reason, stage);
        self.invalidate_values();
        self.failure = Some(failure.clone());
        failure
    }

    fn record_rejection(
        &mut self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) {
        self.rejection_context.stage = stage;
        if self.protocol_rejection.is_none() {
            self.protocol_rejection = Some(self.make_protocol_rejection(reason, stage));
        }
    }

    fn make_protocol_rejection(
        &self,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) -> AgentProtocolRejectionDiagnostic {
        AgentProtocolRejectionDiagnostic::claude_code_stream_json_v1(
            ClaudeCodeStreamJsonV1ProtocolRejection {
                reason,
                stage,
                outer_event: self.rejection_context.outer_event,
                stream_event: self.rejection_context.stream_event,
                content_index: self.rejection_context.content_index,
                content_block: self.rejection_context.content_block,
                state: ClaudeCodeStreamJsonV1RejectionState {
                    event_thread: self.rejection_context.event_thread,
                    ..self
                        .rejection_state_snapshot
                        .clone()
                        .unwrap_or_else(|| self.rejection_state())
                },
            },
        )
    }

    fn protocol_failure_outcome(
        &self,
        cause: AgentFailureCause,
        reason: ClaudeCodeStreamJsonV1RejectionReason,
        stage: ClaudeCodeStreamJsonV1ProtocolStage,
    ) -> AgentOutcome {
        AgentOutcome::Failed(
            AgentFailure::with_protocol_rejection(
                cause,
                AgentProtocolRejectionDiagnostic::claude_code_stream_json_v1(
                    ClaudeCodeStreamJsonV1ProtocolRejection {
                        reason,
                        stage,
                        outer_event: None,
                        stream_event: None,
                        content_index: None,
                        content_block: None,
                        state: self.rejection_state(),
                    },
                ),
            )
            .with_stream_excerpt(self.stream_excerpt())
            .with_tracked_work(self.tracked_work()),
        )
    }

    fn rejection_state(&self) -> ClaudeCodeStreamJsonV1RejectionState {
        let (
            active_message,
            next_content_index,
            completed_content_blocks,
            open_content_block,
            terminal_message_delta_seen,
        ) = match self.active_message.as_ref() {
            Some(message) => (
                if message.parent_tool_use_id.is_none() {
                    ClaudeCodeStreamJsonV1ActiveMessageState::Main
                } else {
                    ClaudeCodeStreamJsonV1ActiveMessageState::Subagent
                },
                Some(bounded_sequence(message.next_index)),
                Some(bounded_sequence(message.completed_blocks)),
                message.active_block.as_ref().map(|block| {
                    ClaudeCodeStreamJsonV1OpenContentBlockState {
                        kind: active_content_block_kind(block),
                        content_index: bounded_sequence(message.next_index),
                    }
                }),
                Some(message.terminal_delta_seen),
            ),
            None => (
                ClaudeCodeStreamJsonV1ActiveMessageState::None,
                None,
                None,
                None,
                None,
            ),
        };
        ClaudeCodeStreamJsonV1RejectionState {
            initialized: self.session_id.is_some(),
            exchange_initialized: self.exchange_initialized,
            exchange_active: self.exchange_active,
            completed_exchanges: bounded_sequence(self.completed_exchanges),
            active_message,
            next_content_index,
            completed_content_blocks,
            open_content_block,
            terminal_message_delta_seen,
            final_main_message: self.final_main_message.is_some(),
            exchange_structured_output_candidates: bounded_sequence(
                self.exchange_structured_output_candidates,
            ),
            completed_result_exchange: completed_result_exchange_state(
                self.completed_result_exchange.as_ref(),
            ),
            result_decision_pending: self.result_decision_pending,
            result_accepted: self.accepted_result.is_some(),
            native_failure: self.native_failure.is_some(),
            retry_active: self.retry_active,
            // The rejected frame's thread is known only once it is decoded.
            event_thread: None,
            outstanding_background_commands: bounded_count(
                self.background_commands
                    .iter()
                    .filter(|task| task.awaits_notification()),
            ),
            outstanding_background_subagents: bounded_count(
                self.subagents
                    .iter()
                    .filter(|subagent| subagent.awaits_notification()),
            ),
            scheduled_wakeup_pending: self.scheduled_wakeup_pending,
        }
    }
}

fn default_rejection_reason(
    stage: ClaudeCodeStreamJsonV1ProtocolStage,
) -> ClaudeCodeStreamJsonV1RejectionReason {
    match stage {
        ClaudeCodeStreamJsonV1ProtocolStage::FrameRead => {
            ClaudeCodeStreamJsonV1RejectionReason::FrameTooLarge
        }
        ClaudeCodeStreamJsonV1ProtocolStage::FrameDecode => {
            ClaudeCodeStreamJsonV1RejectionReason::FrameDecodeFailed
        }
        ClaudeCodeStreamJsonV1ProtocolStage::InitialInput => {
            ClaudeCodeStreamJsonV1RejectionReason::StandardInputWriteFailed
        }
        ClaudeCodeStreamJsonV1ProtocolStage::Initialization => {
            ClaudeCodeStreamJsonV1RejectionReason::InitializationInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ExchangeLifecycle => {
            ClaudeCodeStreamJsonV1RejectionReason::ExchangeLifecycleInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::EventEnvelope => {
            ClaudeCodeStreamJsonV1RejectionReason::EventEnvelopeInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::SessionCorrelation => {
            ClaudeCodeStreamJsonV1RejectionReason::SessionCorrelationInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::StreamEventEnvelope => {
            ClaudeCodeStreamJsonV1RejectionReason::StreamEventEnvelopeInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ActiveStreamParent => {
            ClaudeCodeStreamJsonV1RejectionReason::ActiveStreamParentMismatch
        }
        ClaudeCodeStreamJsonV1ProtocolStage::MessageStart => {
            ClaudeCodeStreamJsonV1RejectionReason::MessageStartInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::MessageTransition => {
            ClaudeCodeStreamJsonV1RejectionReason::MessageTransitionInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStart => {
            ClaudeCodeStreamJsonV1RejectionReason::ContentBlockStartInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockDelta => {
            ClaudeCodeStreamJsonV1RejectionReason::ContentBlockDeltaInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ContentBlockStop => {
            ClaudeCodeStreamJsonV1RejectionReason::ContentBlockStopInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::AssistantEvent => {
            ClaudeCodeStreamJsonV1RejectionReason::AssistantEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::UserEvent => {
            ClaudeCodeStreamJsonV1RejectionReason::UserEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::SystemEvent => {
            ClaudeCodeStreamJsonV1RejectionReason::SystemEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ToolProgressEvent => {
            ClaudeCodeStreamJsonV1RejectionReason::ToolProgressEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::ResultCorrelation => {
            ClaudeCodeStreamJsonV1RejectionReason::ResultCorrelationInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::TerminalDrain => {
            ClaudeCodeStreamJsonV1RejectionReason::TerminalDrainEventInvalid
        }
        ClaudeCodeStreamJsonV1ProtocolStage::EndOfStream => {
            ClaudeCodeStreamJsonV1RejectionReason::EndOfStreamInvariantInvalid
        }
    }
}

fn claude_event_type(event_type: &str) -> ClaudeCodeStreamJsonV1EventType {
    match event_type {
        "system" => ClaudeCodeStreamJsonV1EventType::System,
        "stream_event" => ClaudeCodeStreamJsonV1EventType::StreamEvent,
        "assistant" => ClaudeCodeStreamJsonV1EventType::Assistant,
        "user" => ClaudeCodeStreamJsonV1EventType::User,
        "rate_limit_event" => ClaudeCodeStreamJsonV1EventType::RateLimitEvent,
        "tool_progress" => ClaudeCodeStreamJsonV1EventType::ToolProgress,
        "result" => ClaudeCodeStreamJsonV1EventType::Result,
        _ => ClaudeCodeStreamJsonV1EventType::Unrecognized,
    }
}

fn claude_stream_event_type(event_type: &str) -> ClaudeCodeStreamJsonV1StreamEventType {
    match event_type {
        "message_start" => ClaudeCodeStreamJsonV1StreamEventType::MessageStart,
        "content_block_start" => ClaudeCodeStreamJsonV1StreamEventType::ContentBlockStart,
        "content_block_delta" => ClaudeCodeStreamJsonV1StreamEventType::ContentBlockDelta,
        "content_block_stop" => ClaudeCodeStreamJsonV1StreamEventType::ContentBlockStop,
        "message_delta" => ClaudeCodeStreamJsonV1StreamEventType::MessageDelta,
        "message_stop" => ClaudeCodeStreamJsonV1StreamEventType::MessageStop,
        _ => ClaudeCodeStreamJsonV1StreamEventType::Unrecognized,
    }
}

fn claude_content_block_kind(block_type: &str) -> ClaudeCodeStreamJsonV1ContentBlockKind {
    match block_type {
        "text" => ClaudeCodeStreamJsonV1ContentBlockKind::Text,
        "thinking" => ClaudeCodeStreamJsonV1ContentBlockKind::Thinking,
        "tool_use" => ClaudeCodeStreamJsonV1ContentBlockKind::ToolUse,
        _ => ClaudeCodeStreamJsonV1ContentBlockKind::Unrecognized,
    }
}

fn claude_delta_content_block_kind(delta_type: &str) -> ClaudeCodeStreamJsonV1ContentBlockKind {
    match delta_type {
        "text_delta" => ClaudeCodeStreamJsonV1ContentBlockKind::Text,
        "thinking_delta" | "signature_delta" => ClaudeCodeStreamJsonV1ContentBlockKind::Thinking,
        "input_json_delta" => ClaudeCodeStreamJsonV1ContentBlockKind::ToolUse,
        _ => ClaudeCodeStreamJsonV1ContentBlockKind::Unrecognized,
    }
}

fn active_content_block_kind(block: &ActiveContentBlock) -> ClaudeCodeStreamJsonV1ContentBlockKind {
    match block {
        ActiveContentBlock::Text { .. } => ClaudeCodeStreamJsonV1ContentBlockKind::Text,
        ActiveContentBlock::Thinking { .. } => ClaudeCodeStreamJsonV1ContentBlockKind::Thinking,
        ActiveContentBlock::ToolUse { .. } => ClaudeCodeStreamJsonV1ContentBlockKind::ToolUse,
        ActiveContentBlock::Unknown { .. } => ClaudeCodeStreamJsonV1ContentBlockKind::Unrecognized,
    }
}

fn completed_result_exchange_state(
    exchange: Option<&CompletedResultExchange>,
) -> ClaudeCodeStreamJsonV1CompletedResultExchangeState {
    match exchange {
        None => ClaudeCodeStreamJsonV1CompletedResultExchangeState::None,
        Some(CompletedResultExchange::Candidate(_)) => {
            ClaudeCodeStreamJsonV1CompletedResultExchangeState::Candidate
        }
        Some(CompletedResultExchange::AmbiguousCandidate) => {
            ClaudeCodeStreamJsonV1CompletedResultExchangeState::AmbiguousCandidate
        }
        Some(CompletedResultExchange::MissingCandidate) => {
            ClaudeCodeStreamJsonV1CompletedResultExchangeState::MissingCandidate
        }
        Some(CompletedResultExchange::PrematureCandidate) => {
            ClaudeCodeStreamJsonV1CompletedResultExchangeState::PrematureCandidate
        }
        Some(CompletedResultExchange::NativeFailure) => {
            ClaudeCodeStreamJsonV1CompletedResultExchangeState::NativeFailure
        }
    }
}

fn bounded_sequence(value: u64) -> u64 {
    value.min(MAXIMUM_DIAGNOSTIC_SEQUENCE)
}

fn bounded_count<T>(items: impl Iterator<Item = T>) -> u64 {
    bounded_sequence(u64::try_from(items.count()).unwrap_or(u64::MAX))
}

struct StreamedMessage {
    id: Arc<str>,
    parent_tool_use_id: Option<Arc<str>>,
    next_index: u64,
    active_block: Option<ActiveContentBlock>,
    completed_blocks: u64,
    text_blocks: u64,
    tool_use_blocks: u64,
    structured_output_candidates: Vec<Value>,
    structured_output_call_ids: Vec<Arc<str>>,
    structured_output_acknowledgements: u64,
    successful_structured_output_acknowledgements: u64,
    response: String,
    input_tokens: u64,
    output_tokens: u64,
    terminal_delta_seen: bool,
}

enum ActiveContentBlock {
    Text {
        text: String,
        nominal_seen: bool,
    },
    Thinking {
        thinking: String,
        nominal_seen: bool,
    },
    ToolUse {
        call_id: Arc<str>,
        name: Arc<str>,
        initial_input: Value,
        input_json: String,
        input_delta_seen: bool,
        nominal_seen: bool,
    },
    Unknown {
        initial_block: Value,
        delta_seen: bool,
        nominal_seen: bool,
    },
}

impl ActiveContentBlock {
    fn correlate_nominal(&mut self, value: &Value) -> bool {
        let Some(block) = value.as_object() else {
            return false;
        };
        match self {
            Self::Text { text, nominal_seen } => {
                let matches = !*nominal_seen
                    && required_string(block, "type") == Some("text")
                    && required_string(block, "text") == Some(text.as_str());
                *nominal_seen |= matches;
                matches
            }
            Self::Thinking {
                thinking,
                nominal_seen,
            } => {
                let matches = !*nominal_seen
                    && required_string(block, "type") == Some("thinking")
                    && required_string(block, "thinking") == Some(thinking.as_str());
                *nominal_seen |= matches;
                matches
            }
            Self::ToolUse {
                call_id,
                name,
                nominal_seen,
                ..
            } => {
                // The envelope restates the block with Claude Code's client-normalized
                // input (for example a Bash `command` without its leading `cd <cwd> && `),
                // so the streamed input is not compared. The call identity and name are
                // the correlation; the native tool input carries no Scherzo authority.
                let matches = !*nominal_seen
                    && required_string(block, "type") == Some("tool_use")
                    && required_nonempty_string(block, "id") == Some(call_id.as_ref())
                    && required_nonempty_string(block, "name") == Some(name.as_ref())
                    && block.get("input").is_some_and(Value::is_object);
                *nominal_seen |= matches;
                matches
            }
            Self::Unknown {
                initial_block,
                delta_seen,
                nominal_seen,
            } => {
                let matches = !*nominal_seen && !*delta_seen && initial_block == value;
                *nominal_seen |= matches;
                matches
            }
        }
    }
}

struct CompletedMainMessage {
    text_blocks: u64,
    tool_use_blocks: u64,
    structured_output_candidates: Vec<Value>,
    structured_output_acknowledgements: u64,
    successful_structured_output_acknowledgements: u64,
    response: String,
}

fn bounded_append(value: &mut String, addition: &str, maximum_bytes: u64) -> bool {
    let current = u64::try_from(value.len()).unwrap_or(u64::MAX);
    let additional = u64::try_from(addition.len()).unwrap_or(u64::MAX);
    if current
        .checked_add(additional)
        .is_none_or(|total| total > maximum_bytes)
    {
        return false;
    }
    value.push_str(addition);
    true
}

// A tool result is observational, so its text blocks are concatenated and the other
// block kinds Claude Code emits (`tool_reference` from ToolSearch, `image` from a screen
// capture) contribute nothing while remaining well formed.
fn normalized_tool_result_content(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Array(blocks) => {
            let mut result = String::new();
            for block in blocks {
                let block = block.as_object()?;
                if required_nonempty_string(block, "type")? == "text" {
                    result.push_str(required_string(block, "text")?);
                }
            }
            Some(result)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => None,
    }
}

/// Whether an Agent call's `tool_use_result` reports an asynchronous launch: Claude Code's
/// `status: "async_launched"` result, which also carries `isAsync: true` when present.
fn async_agent_launch(result: &Map<String, Value>) -> bool {
    required_string(result, "status") == Some("async_launched")
        || required_bool(result, "isAsync") == Some(true)
}

/// Whether a frame that lands between a rejected candidate's `result` and the correction
/// turn's `init` is read as the turn reads it: a subagent thread's message, tool result or
/// progress, or a `system` task frame. A main-thread message, stream event or `result` is not.
/// The thread is the one `push_stdout` records for the frame before dispatching it.
fn correction_window_admits(
    event_type: &str,
    thread: Option<ClaudeCodeStreamJsonV1EventThread>,
) -> bool {
    match event_type {
        "assistant" | "user" | "tool_progress" => {
            thread == Some(ClaudeCodeStreamJsonV1EventThread::Subagent)
        }
        "system" => true,
        _ => false,
    }
}

fn parent_tool_use_id(object: &Map<String, Value>) -> Option<Option<&str>> {
    match object.get("parent_tool_use_id")? {
        Value::Null => Some(None),
        Value::String(value) if !value.is_empty() => Some(Some(value)),
        _ => None,
    }
}

/// Renders the facts a Claude Code `api_retry` event reports, in the order
/// `api retry A/M · CLASS · HTTP S · next in D ms`, omitting absent facts.
fn api_retry_message(object: &Map<String, Value>) -> String {
    let counter = required_u64(object, "attempt")
        .zip(required_u64(object, "max_retries"))
        .map_or_else(
            || "api retry".to_owned(),
            |(attempt, maximum)| format!("api retry {attempt}/{maximum}"),
        );
    let class = required_string(object, "error").unwrap_or("provider retry");
    let mut parts = vec![counter, class.to_owned()];
    if let Some(status) = required_u64(object, "error_status") {
        parts.push(format!("HTTP {status}"));
    }
    if let Some(delay) = required_u64(object, "retry_delay_ms") {
        parts.push(format!("next in {delay} ms"));
    }
    parts.join(" · ")
}

/// The warning an epilogue's native failure yields: its closed classification only.
fn epilogue_failure_message(diagnostic: HarnessFailureDiagnostic) -> String {
    let class = diagnostic.harness_error.unwrap_or("other");
    match diagnostic.http_status {
        Some(status) => format!("turn after the settled value failed: {class} (HTTP {status})"),
        None => format!("turn after the settled value failed: {class}"),
    }
}

fn lifecycle(milestone: AgentLifecycleMilestone) -> AgentObservation {
    AgentObservation::Lifecycle { milestone }
}

fn failed(cause: AgentFailureCause) -> AgentOutcome {
    failed_agent_outcome(cause)
}

// These accessors and native identity checks stay profile-local because their callers
// assign different lifecycle authority and failure timing to superficially similar fields.
/// The call a `tool_progress` heartbeat belongs to: its `tool_use_id` is the call's id
/// followed by `-heartbeat-` and a counter. Any other shape is returned as given.
fn heartbeat_call_id(call_id: &str) -> &str {
    match call_id.rsplit_once("-heartbeat-") {
        Some((call, counter))
            if !call.is_empty()
                && !counter.is_empty()
                && counter.bytes().all(|b| b.is_ascii_digit()) =>
        {
            call
        }
        _ => call_id,
    }
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key)?.as_str()
}

fn required_nonempty_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    required_string(object, key).filter(|value| !value.is_empty())
}

fn required_bool(object: &Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key)?.as_bool()
}

/// Whether a task start or task-list entry names a `task_type` the parser does not model:
/// neither a shell command (`local_bash`, which a `Monitor` call's task also is) nor a
/// subagent (`local_agent`).
fn unmodeled_task_type(object: &Map<String, Value>) -> bool {
    !matches!(
        required_string(object, "task_type"),
        Some("local_bash" | "local_agent")
    )
}

/// Keeps `task_id` in `known`, newest last, at most `MAXIMUM_TRACKED_TASKS`.
fn remember_task_id(known: &mut Vec<Arc<str>>, task_id: &str) {
    if known.iter().any(|known| known.as_ref() == task_id) {
        return;
    }
    known.push(Arc::from(task_id));
    keep_newest(known);
}

/// Drops the oldest of `items`, newest last, beyond `MAXIMUM_TRACKED_TASKS`.
fn keep_newest<T>(items: &mut Vec<T>) {
    let excess = items.len().saturating_sub(MAXIMUM_TRACKED_TASKS);
    items.drain(..excess);
}

fn required_u64(object: &Map<String, Value>, key: &str) -> Option<u64> {
    object.get(key)?.as_u64()
}

fn required_object<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Option<&'a Map<String, Value>> {
    object.get(key)?.as_object()
}

fn required_array<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a [Value]> {
    object.get(key)?.as_array().map(Vec::as_slice)
}

fn valid_session_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes.get(index) == Some(&b'-'))
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
        && bytes
            .iter()
            .any(|byte| byte.is_ascii_hexdigit() && *byte != b'0')
}

#[cfg(test)]
mod adapter_tests;
#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod lifecycle_matrix;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
