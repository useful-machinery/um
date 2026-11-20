//! The reader of Claude Code's stream-json output (REQ-AGENT-87, INV-HARNESS-22). It takes
//! stdout bytes in chunks of any size and yields the activity it recognizes and the facts a
//! step is decided by (`outcome`); it has no failing path. It compares no session, model,
//! working directory or permission mode and keeps no account of turns or tasks: its only
//! state is the line being read, the facts, and small bounded tables it uses to label and
//! deduplicate what it shows, where a wrong guess shows a line twice or not at all.

use std::num::NonZeroU64;
use std::sync::Arc;

use serde_json::{Map, Value};

use super::ClaudeCodeStreamJsonV1ProtocolLimits;
use crate::workflow::agent::{
    AgentDiagnosticLevel, AgentHarnessFailureDetail, AgentLifecycleMilestone, AgentObservation,
    AgentToolCallPhase, AgentValueKind, CanonicalHarnessFailure, HarnessFailureDiagnostic,
};
use crate::workflow::text_fit::ellipsize;

/// Subagents remembered by task id for their names, newest kept.
const MAXIMUM_SUBAGENTS: usize = 128;
const MAXIMUM_SUBAGENT_DESCRIPTION_WIDTH: usize = 200;
const MAXIMUM_SUBAGENT_TOKEN_WIDTH: usize = 64;
const MAXIMUM_UNRECOGNIZED_KINDS: usize = 64;
const MAXIMUM_UNRECOGNIZED_KIND_TOKEN_BYTES: usize = 64;
const MAXIMUM_COMPACTION_ERROR_WIDTH: usize = 200;
/// Messages whose content stream events showed, newest kept, so that their `assistant`
/// envelopes are not shown again.
const MAXIMUM_STREAMED_MESSAGE_IDS: usize = 128;
/// Threads with a message streaming at once, and open tool-use blocks per message.
const MAXIMUM_OPEN_MESSAGES: usize = 16;
const MAXIMUM_OPEN_TOOL_BLOCKS: usize = 128;
/// The `terminal_reason`s a failed `result` is classified by ahead of its `subtype`.
const CLASSIFYING_TERMINAL_REASONS: [&str; 10] = [
    "rate_limit",
    "rate_limited",
    "context_window_exceeded",
    "prompt_too_long",
    "usage_limit_exceeded",
    "unauthorized",
    "overloaded",
    "bad_request",
    "server_error",
    "sandbox_error",
];

/// The first `system/init` the stream carried.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Initialization {
    /// The `claude_code_version` it names, if it names one.
    pub(super) claude_code_version: Option<Arc<str>>,
}

/// The last `result` read that reports a failure or the successful end of a turn; a `result`
/// closing a turn that never called the model (`num_turns: 0`) replaces nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DecidingResult {
    /// `is_error: true`, or a `terminal_reason` other than `completed`, classified
    /// (REQ-AGENT-45, REQ-AGENT-70, REQ-AGENT-72).
    Failed(AgentHarnessFailureDetail),
    Successful(ResultText),
}

/// The text a successful deciding `result` carries as its `result`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ResultText {
    /// The step returns no response, so the text was not kept.
    NotKept,
    Text(Arc<str>),
    /// The frame's `result` is not a string.
    Missing,
    /// The text exceeds the response limit; it was not kept (REQ-AGENT-28).
    OverLimit,
}

pub(super) struct StreamReader {
    value_kind: AgentValueKind,
    maximum_response_bytes: NonZeroU64,
    maximum_line_bytes: u64,
    line: Vec<u8>,
    /// The size of a line above the frame limit while its bytes are skipped.
    skipped_line_bytes: Option<u64>,
    observations: Vec<AgentObservation>,
    initialization: Option<Initialization>,
    deciding_result: Option<DecidingResult>,
    /// The class and status of the newest main-thread api-error message since the last
    /// `result`.
    api_error: Option<HarnessFailureDiagnostic>,
    /// The newest `resetsAt` a usage window not `allowed` reported (REQ-AGENT-72).
    latest_reset: Option<u64>,
    retry_open: bool,
    unrecognized_kinds: Vec<(Box<str>, Option<Box<str>>)>,
    subagents: Vec<Subagent>,
    open_messages: Vec<OpenMessage>,
    streamed_message_ids: Vec<Arc<str>>,
}

struct Subagent {
    task_id: Arc<str>,
    subagent_type: Option<Arc<str>>,
    description: Option<Arc<str>>,
}

impl Subagent {
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

/// A message streaming on one thread: the main thread (`None`) or a subagent's, by the
/// tool call its frames name as `parent_tool_use_id`.
struct OpenMessage {
    thread: Option<Arc<str>>,
    input_tokens: Option<u64>,
    tool_blocks: Vec<OpenToolBlock>,
}

struct OpenToolBlock {
    index: u64,
    call_id: Arc<str>,
    name: Arc<str>,
}

impl StreamReader {
    pub(super) fn new(value_kind: AgentValueKind, maximum_response_bytes: NonZeroU64) -> Self {
        Self {
            value_kind,
            maximum_response_bytes,
            maximum_line_bytes: ClaudeCodeStreamJsonV1ProtocolLimits::profile()
                .maximum_frame_bytes()
                .get(),
            line: Vec::new(),
            skipped_line_bytes: None,
            observations: Vec::new(),
            initialization: None,
            deciding_result: None,
            api_error: None,
            latest_reset: None,
            retry_open: false,
            unrecognized_kinds: Vec::new(),
            subagents: Vec::new(),
            open_messages: Vec::new(),
            streamed_message_ids: Vec::new(),
        }
    }

    /// The first `system/init` read, if one was.
    pub(super) fn initialization(&self) -> Option<&Initialization> {
        self.initialization.as_ref()
    }

    /// The deciding `result` read so far, if one was.
    pub(super) fn deciding_result(&self) -> Option<&DecidingResult> {
        self.deciding_result.as_ref()
    }

    /// Reads `bytes`, which may end inside a line, and gives `observe` what each line it
    /// completes shows.
    pub(super) fn push(&mut self, bytes: &[u8], mut observe: impl FnMut(AgentObservation)) {
        let mut pieces = bytes.split(|byte| *byte == b'\n').peekable();
        while let Some(piece) = pieces.next() {
            self.take_line_bytes(piece);
            if pieces.peek().is_some() {
                self.end_line();
                for observation in self.observations.drain(..) {
                    observe(observation);
                }
            }
        }
    }

    /// Ends the stream: bytes of a line it ended inside are dropped with a warning.
    pub(super) fn finish(&mut self, mut observe: impl FnMut(AgentObservation)) {
        let bytes = self
            .skipped_line_bytes
            .take()
            .unwrap_or_else(|| byte_count(self.line.len()));
        self.line = Vec::new();
        if bytes > 0 {
            observe(warning(dropped_line_message(bytes)));
        }
    }

    fn take_line_bytes(&mut self, piece: &[u8]) {
        let piece_bytes = byte_count(piece.len());
        if let Some(skipped) = self.skipped_line_bytes.as_mut() {
            *skipped = skipped.saturating_add(piece_bytes);
            return;
        }
        let total = byte_count(self.line.len()).saturating_add(piece_bytes);
        if total > self.maximum_line_bytes {
            self.skipped_line_bytes = Some(total);
            self.line = Vec::new();
        } else {
            self.line.extend_from_slice(piece);
        }
    }

    fn end_line(&mut self) {
        let line = std::mem::take(&mut self.line);
        if let Some(bytes) = self.skipped_line_bytes.take() {
            let message = format!(
                "dropped a harness output line of {bytes} bytes, above the {} byte frame limit",
                self.maximum_line_bytes
            );
            self.observations.push(warning(message));
            return;
        }
        if line.is_empty() {
            return;
        }
        match useful_support::strict_json_from_slice(&line) {
            Ok(value) => self.read_frame(&value),
            Err(_) => self
                .observations
                .push(warning(dropped_line_message(byte_count(line.len())))),
        }
    }

    fn read_frame(&mut self, value: &Value) {
        let Some(object) = value.as_object() else {
            self.observe_unrecognized(value);
            return;
        };
        match string(object, "type") {
            Some("system") => self.read_system(object, value),
            Some("stream_event") => self.read_stream_event(object, value),
            Some("assistant") => self.read_assistant(object, value),
            Some("user") => self.read_user(object, value),
            Some("rate_limit_event") => self.read_rate_limit_event(object),
            Some("tool_progress") => self.read_tool_progress(object, value),
            Some("result") => self.read_result(object, value),
            _ => self.observe_unrecognized(value),
        }
    }

    fn read_system(&mut self, object: &Map<String, Value>, value: &Value) {
        match string(object, "subtype") {
            Some("init") => {
                if self.initialization.is_none() {
                    self.initialization = Some(Initialization {
                        claude_code_version: string(object, "claude_code_version").map(Arc::from),
                    });
                    self.lifecycle(AgentLifecycleMilestone::SessionEstablished);
                    self.lifecycle(AgentLifecycleMilestone::HarnessStarted);
                }
                self.lifecycle(AgentLifecycleMilestone::TurnStarted);
            }
            Some("status") => self.read_status(object, value),
            Some("api_retry") => {
                if !self.retry_open {
                    self.retry_open = true;
                    self.lifecycle(AgentLifecycleMilestone::RetryStarted);
                }
                self.observations.push(warning(api_retry_message(object)));
            }
            Some("compact_boundary") => {
                self.lifecycle(AgentLifecycleMilestone::CompactionCompleted);
            }
            Some("task_started") => match string(object, "task_type") {
                Some("local_agent") => self.start_subagent(object),
                Some("local_bash") => {}
                _ => self.observe_unrecognized(value),
            },
            Some("task_notification") => self.settle_subagent(object),
            Some("task_progress" | "task_updated" | "background_tasks_changed") => {}
            _ => self.observe_unrecognized(value),
        }
    }

    /// A status is text or `null`, which clears it. Only beside `null` does Claude Code
    /// report how a compaction ended: `compact_result` `success`, whose `compact_boundary`
    /// follows, or `failed`, optionally with a text `compact_error` (REQ-AGENT-79).
    fn read_status(&mut self, object: &Map<String, Value>, value: &Value) {
        let compact_result = object.get("compact_result").map(Value::as_str);
        let compact_error = object.get("compact_error").map(Value::as_str);
        match (object.get("status"), compact_result, compact_error) {
            (Some(Value::String(status)), None, None) if !status.is_empty() => {
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Information,
                    message: Arc::from(status.as_str()),
                });
            }
            (Some(Value::Null), None | Some(Some("success")), None) => {}
            (Some(Value::Null), Some(Some("failed")), error @ (None | Some(Some(_)))) => {
                self.observations
                    .push(warning(compaction_failure_message(error.flatten())));
            }
            _ => self.observe_unrecognized(value),
        }
    }

    fn start_subagent(&mut self, object: &Map<String, Value>) {
        let subagent_type = nonempty_string(object, "subagent_type")
            .map(|kind| Arc::from(ellipsize(kind, MAXIMUM_SUBAGENT_TOKEN_WIDTH)));
        let description = nonempty_string(object, "description")
            .map(|text| Arc::from(ellipsize(text, MAXIMUM_SUBAGENT_DESCRIPTION_WIDTH)));
        let Some(task_id) = nonempty_string(object, "task_id") else {
            let unnamed = Subagent {
                task_id: Arc::from(""),
                subagent_type,
                description,
            };
            self.information(unnamed.message("started"));
            return;
        };
        let known = self
            .subagents
            .iter()
            .position(|subagent| subagent.task_id.as_ref() == task_id);
        let (subagent, event) = match known {
            Some(index) => {
                let mut subagent = self.subagents.remove(index);
                if subagent_type.is_some() {
                    subagent.subagent_type = subagent_type;
                }
                if description.is_some() {
                    subagent.description = description;
                }
                (subagent, "resumed")
            }
            None => (
                Subagent {
                    task_id: Arc::from(task_id),
                    subagent_type,
                    description,
                },
                "started",
            ),
        };
        self.information(subagent.message(event));
        self.subagents.push(subagent);
        keep_newest(&mut self.subagents, MAXIMUM_SUBAGENTS);
    }

    fn settle_subagent(&mut self, object: &Map<String, Value>) {
        let Some(subagent) = nonempty_string(object, "task_id").and_then(|task_id| {
            self.subagents
                .iter()
                .find(|subagent| subagent.task_id.as_ref() == task_id)
        }) else {
            return;
        };
        let status = nonempty_string(object, "status").map_or_else(
            || String::from("settled"),
            |status| ellipsize(status, MAXIMUM_SUBAGENT_TOKEN_WIDTH),
        );
        let message = subagent.message(&status);
        self.information(message);
    }

    fn read_stream_event(&mut self, object: &Map<String, Value>, value: &Value) {
        let parent = thread(object);
        let Some(event) = map(object, "event") else {
            self.observe_unrecognized(value);
            return;
        };
        let read = match string(event, "type") {
            Some("message_start") => self.stream_message_start(event, parent),
            Some("content_block_start") => self.stream_content_block_start(event, parent),
            Some("content_block_delta") => self.stream_content_block_delta(event, parent),
            Some("content_block_stop") => {
                if let Some(index) = unsigned(event, "index") {
                    self.close_tool_block(parent, index);
                }
                true
            }
            Some("message_delta") => {
                let output_tokens =
                    map(event, "usage").and_then(|usage| unsigned(usage, "output_tokens"));
                let input_tokens = self
                    .open_message(parent)
                    .and_then(|message| message.input_tokens);
                if let Some((input_tokens, output_tokens)) = input_tokens.zip(output_tokens) {
                    self.observations.push(AgentObservation::Usage {
                        input_tokens,
                        output_tokens,
                    });
                }
                self.lifecycle(AgentLifecycleMilestone::MessageUpdated);
                true
            }
            Some("message_stop") => {
                self.open_messages
                    .retain(|message| message.thread.as_deref() != parent);
                self.lifecycle(AgentLifecycleMilestone::MessageCompleted);
                true
            }
            _ => false,
        };
        if !read {
            self.observe_unrecognized(value);
        }
    }

    fn stream_message_start(&mut self, event: &Map<String, Value>, thread: Option<&str>) -> bool {
        let Some(message) = map(event, "message") else {
            return false;
        };
        self.complete_retry();
        if let Some(id) = nonempty_string(message, "id")
            && !self
                .streamed_message_ids
                .iter()
                .any(|known| known.as_ref() == id)
        {
            self.streamed_message_ids.push(Arc::from(id));
            keep_newest(&mut self.streamed_message_ids, MAXIMUM_STREAMED_MESSAGE_IDS);
        }
        let usage = map(message, "usage");
        let input_tokens = usage.and_then(|usage| unsigned(usage, "input_tokens"));
        let output_tokens = usage.and_then(|usage| unsigned(usage, "output_tokens"));
        self.open_messages
            .retain(|open| open.thread.as_deref() != thread);
        self.open_messages.push(OpenMessage {
            thread: thread.map(Arc::from),
            input_tokens,
            tool_blocks: Vec::new(),
        });
        keep_newest(&mut self.open_messages, MAXIMUM_OPEN_MESSAGES);
        self.lifecycle(AgentLifecycleMilestone::MessageStarted);
        if let Some(model) = nonempty_string(message, "model") {
            self.observations.push(AgentObservation::Model {
                name: Arc::from(model),
            });
        }
        if let Some((input_tokens, output_tokens)) = input_tokens.zip(output_tokens) {
            self.observations.push(AgentObservation::Usage {
                input_tokens,
                output_tokens,
            });
        }
        true
    }

    fn stream_content_block_start(
        &mut self,
        event: &Map<String, Value>,
        thread: Option<&str>,
    ) -> bool {
        let (Some(index), Some(block)) = (unsigned(event, "index"), map(event, "content_block"))
        else {
            return false;
        };
        match string(block, "type") {
            Some("text") => string(block, "text").is_some_and(|text| {
                self.assistant_text(text);
                true
            }),
            Some("thinking") => string(block, "thinking").is_some_and(|thinking| {
                self.reasoning(thinking);
                true
            }),
            Some("tool_use") => {
                let (Some(call_id), Some(name)) =
                    (nonempty_string(block, "id"), nonempty_string(block, "name"))
                else {
                    return false;
                };
                self.tool_call(call_id, name, AgentToolCallPhase::Started);
                let tool_block = OpenToolBlock {
                    index,
                    call_id: Arc::from(call_id),
                    name: Arc::from(name),
                };
                if let Some(message) = self.open_message_for(thread) {
                    message.tool_blocks.retain(|open| open.index != index);
                    message.tool_blocks.push(tool_block);
                    keep_newest(&mut message.tool_blocks, MAXIMUM_OPEN_TOOL_BLOCKS);
                }
                true
            }
            _ => false,
        }
    }

    fn stream_content_block_delta(
        &mut self,
        event: &Map<String, Value>,
        thread: Option<&str>,
    ) -> bool {
        let (Some(index), Some(delta)) = (unsigned(event, "index"), map(event, "delta")) else {
            return false;
        };
        match string(delta, "type") {
            Some("text_delta") => string(delta, "text").is_some_and(|text| {
                self.assistant_text(text);
                true
            }),
            Some("thinking_delta") => string(delta, "thinking").is_some_and(|thinking| {
                self.reasoning(thinking);
                true
            }),
            Some("signature_delta") => true,
            Some("input_json_delta") => {
                let Some((call_id, name)) = self.open_message(thread).and_then(|message| {
                    message
                        .tool_blocks
                        .iter()
                        .find(|block| block.index == index)
                        .map(|block| (Arc::clone(&block.call_id), Arc::clone(&block.name)))
                }) else {
                    return false;
                };
                self.observations.push(AgentObservation::ToolCall {
                    call_id,
                    name,
                    phase: AgentToolCallPhase::Updated,
                });
                true
            }
            _ => false,
        }
    }

    fn close_tool_block(&mut self, thread: Option<&str>, index: u64) {
        let Some(message) = self
            .open_messages
            .iter_mut()
            .find(|message| message.thread.as_deref() == thread)
        else {
            return;
        };
        let Some(position) = message
            .tool_blocks
            .iter()
            .position(|block| block.index == index)
        else {
            return;
        };
        let block = message.tool_blocks.remove(position);
        self.observations.push(AgentObservation::ToolCall {
            call_id: block.call_id,
            name: block.name,
            phase: AgentToolCallPhase::Completed,
        });
    }

    fn open_message(&self, thread: Option<&str>) -> Option<&OpenMessage> {
        self.open_messages
            .iter()
            .find(|message| message.thread.as_deref() == thread)
    }

    /// The thread's open message, opened when no `message_start` announced it.
    fn open_message_for(&mut self, thread: Option<&str>) -> Option<&mut OpenMessage> {
        if self.open_message(thread).is_none() {
            self.open_messages.push(OpenMessage {
                thread: thread.map(Arc::from),
                input_tokens: None,
                tool_blocks: Vec::new(),
            });
            keep_newest(&mut self.open_messages, MAXIMUM_OPEN_MESSAGES);
        }
        self.open_messages
            .iter_mut()
            .find(|message| message.thread.as_deref() == thread)
    }

    fn read_assistant(&mut self, object: &Map<String, Value>, value: &Value) {
        let Some(message) = map(object, "message") else {
            self.observe_unrecognized(value);
            return;
        };
        let content = array(message, "content");
        if thread(object).is_none() && boolean(object, "is_api_error_message") == Some(true) {
            // `request_id` is absent when no request reached the API (the login-less
            // `authentication_failed` answer); nothing of it is required.
            let http_status = object
                .get("api_error_status")
                .and_then(Value::as_u64)
                .and_then(|status| u16::try_from(status).ok());
            self.api_error = nonempty_string(object, "error")
                .map(|code| HarnessFailureDiagnostic::from_code(code, http_status));
            let text = content.and_then(|content| {
                content.iter().find_map(|block| {
                    let block = block.as_object()?;
                    (string(block, "type") == Some("text"))
                        .then(|| nonempty_string(block, "text"))
                        .flatten()
                })
            });
            if let Some(text) = text {
                self.observations.push(AgentObservation::Diagnostic {
                    level: AgentDiagnosticLevel::Error,
                    message: Arc::from(text),
                });
            }
            return;
        }
        if nonempty_string(message, "id").is_some_and(|id| {
            self.streamed_message_ids
                .iter()
                .any(|known| known.as_ref() == id)
        }) {
            return;
        }
        let Some(content) = content else {
            self.observe_unrecognized(value);
            return;
        };
        for block in content {
            if !self.read_assistant_block(block) {
                self.observe_unrecognized(value);
            }
        }
    }

    fn read_assistant_block(&mut self, block: &Value) -> bool {
        let Some(block) = block.as_object() else {
            return false;
        };
        match string(block, "type") {
            Some("text") => string(block, "text").is_some_and(|text| {
                self.assistant_text(text);
                true
            }),
            Some("thinking") => string(block, "thinking").is_some_and(|thinking| {
                self.reasoning(thinking);
                true
            }),
            Some("tool_use") => {
                let (Some(call_id), Some(name)) =
                    (nonempty_string(block, "id"), nonempty_string(block, "name"))
                else {
                    return false;
                };
                self.tool_call(call_id, name, AgentToolCallPhase::Started);
                self.tool_call(call_id, name, AgentToolCallPhase::Completed);
                true
            }
            _ => false,
        }
    }

    fn read_user(&mut self, object: &Map<String, Value>, value: &Value) {
        let Some(content) = map(object, "message").and_then(|message| array(message, "content"))
        else {
            self.observe_unrecognized(value);
            return;
        };
        for block in content {
            let result = block.as_object().and_then(|block| {
                if string(block, "type") != Some("tool_result") {
                    return None;
                }
                let call_id = nonempty_string(block, "tool_use_id")?;
                let content = block
                    .get("content")
                    .and_then(normalized_tool_result_content)?;
                // Absent means success, as in the Messages API block shape.
                let is_error = match block.get("is_error") {
                    None => false,
                    Some(is_error) => is_error.as_bool()?,
                };
                Some(AgentObservation::ToolResult {
                    call_id: Arc::from(call_id),
                    is_error,
                    content: Arc::from(content),
                })
            });
            match result {
                Some(observation) => self.observations.push(observation),
                None => self.observe_unrecognized(value),
            }
        }
    }

    /// Claude Code reports its usage windows on every turn; only a window that is warning
    /// or rejecting is worth an operator's attention, and its reset time is kept for a
    /// limit failure (REQ-AGENT-72).
    fn read_rate_limit_event(&mut self, object: &Map<String, Value>) {
        let info = map(object, "rate_limit_info");
        let status = info.and_then(|info| nonempty_string(info, "status"));
        if status == Some("allowed") {
            return;
        }
        if let Some(reset) = info.and_then(|info| unsigned(info, "resetsAt")) {
            self.latest_reset = Some(reset);
        }
        let window = info.and_then(|info| nonempty_string(info, "rateLimitType"));
        let message = match (status, window) {
            (Some(status), Some(window)) => format!("provider rate limit {status} ({window})"),
            (Some(status), None) => format!("provider rate limit {status}"),
            (None, _) => String::from("provider rate limit"),
        };
        self.observations.push(warning(message));
    }

    fn read_tool_progress(&mut self, object: &Map<String, Value>, value: &Value) {
        let (Some(call_id), Some(name)) = (
            nonempty_string(object, "tool_use_id"),
            nonempty_string(object, "tool_name"),
        ) else {
            self.observe_unrecognized(value);
            return;
        };
        // A heartbeat for a long-running call carries the call's id with a
        // `-heartbeat-<n>` suffix; the observation names the call itself.
        let call_id = if boolean(object, "heartbeat") == Some(true) {
            heartbeat_call_id(call_id)
        } else {
            call_id
        };
        self.tool_call(call_id, name, AgentToolCallPhase::Updated);
    }

    fn read_result(&mut self, object: &Map<String, Value>, value: &Value) {
        let Some(is_error) = boolean(object, "is_error") else {
            self.observe_unrecognized(value);
            return;
        };
        let api_error = self.api_error.take();
        let terminal_reason = string(object, "terminal_reason");
        if is_error || terminal_reason.is_some_and(|reason| reason != "completed") {
            self.deciding_result = Some(DecidingResult::Failed(self.classify_failure(
                object,
                terminal_reason,
                api_error,
            )));
        } else if unsigned(object, "num_turns") != Some(0) {
            self.deciding_result = Some(DecidingResult::Successful(self.result_text(object)));
        }
        self.complete_retry();
        self.lifecycle(AgentLifecycleMilestone::TurnCompleted);
    }

    /// The class of a failed `result` comes from its harness-owned fields alone, never its
    /// text: a known `terminal_reason`, else the `subtype`; an `api_error` takes the class
    /// of the api-error message read since the previous `result`.
    fn classify_failure(
        &self,
        object: &Map<String, Value>,
        terminal_reason: Option<&str>,
        api_error: Option<HarnessFailureDiagnostic>,
    ) -> AgentHarnessFailureDetail {
        let code = terminal_reason
            .filter(|reason| CLASSIFYING_TERMINAL_REASONS.contains(reason))
            .or_else(|| string(object, "subtype"))
            .unwrap_or_default();
        let http_status = object
            .get("api_error_status")
            .and_then(Value::as_u64)
            .and_then(|status| u16::try_from(status).ok());
        let mut diagnostic = match (terminal_reason, api_error) {
            (Some("api_error"), Some(api_error)) => api_error,
            _ => HarnessFailureDiagnostic::from_code(code, None),
        }
        .with_limit_reset(self.latest_reset);
        diagnostic.http_status = http_status.or(diagnostic.http_status);
        AgentHarnessFailureDetail::Classified {
            canonical: CanonicalHarnessFailure::ModelError,
            diagnostic,
        }
    }

    fn result_text(&self, object: &Map<String, Value>) -> ResultText {
        if self.value_kind != AgentValueKind::Response {
            return ResultText::NotKept;
        }
        match string(object, "result") {
            None => ResultText::Missing,
            Some(text) if byte_count(text.len()) > self.maximum_response_bytes.get() => {
                ResultText::OverLimit
            }
            Some(text) => ResultText::Text(Arc::from(text)),
        }
    }

    fn complete_retry(&mut self) {
        if self.retry_open {
            self.retry_open = false;
            self.lifecycle(AgentLifecycleMilestone::RetryCompleted);
        }
    }

    /// Observes an unrecognized event once per native `type`/`subtype` kind. Kinds that
    /// cannot be remembered, because a token is long, there is no `type`, or the set is
    /// full, are observed every time.
    fn observe_unrecognized(&mut self, value: &Value) {
        let kind = value.as_object().and_then(|object| {
            let event_type = string(object, "type")?;
            let subtype = string(object, "subtype");
            (event_type.len() <= MAXIMUM_UNRECOGNIZED_KIND_TOKEN_BYTES
                && subtype
                    .is_none_or(|subtype| subtype.len() <= MAXIMUM_UNRECOGNIZED_KIND_TOKEN_BYTES))
            .then_some((event_type, subtype))
        });
        if let Some((event_type, subtype)) = kind {
            if self
                .unrecognized_kinds
                .iter()
                .any(|(seen_type, seen_subtype)| {
                    seen_type.as_ref() == event_type && seen_subtype.as_deref() == subtype
                })
            {
                return;
            }
            if self.unrecognized_kinds.len() < MAXIMUM_UNRECOGNIZED_KINDS {
                self.unrecognized_kinds
                    .push((Box::from(event_type), subtype.map(Box::from)));
            }
        }
        self.observations
            .push(AgentObservation::UnrecognizedHarnessEvent {
                event: Arc::new(value.clone()),
            });
    }

    fn lifecycle(&mut self, milestone: AgentLifecycleMilestone) {
        self.observations
            .push(AgentObservation::Lifecycle { milestone });
    }

    fn information(&mut self, message: String) {
        self.observations.push(AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Information,
            message: Arc::from(message),
        });
    }

    fn assistant_text(&mut self, text: &str) {
        if !text.is_empty() {
            self.observations.push(AgentObservation::AssistantText {
                text: Arc::from(text),
            });
        }
    }

    fn reasoning(&mut self, thinking: &str) {
        if !thinking.is_empty() {
            self.observations.push(AgentObservation::Reasoning {
                text: Arc::from(thinking),
            });
        }
    }

    fn tool_call(&mut self, call_id: &str, name: &str, phase: AgentToolCallPhase) {
        self.observations.push(AgentObservation::ToolCall {
            call_id: Arc::from(call_id),
            name: Arc::from(name),
            phase,
        });
    }
}

fn warning(message: String) -> AgentObservation {
    AgentObservation::Diagnostic {
        level: AgentDiagnosticLevel::Warning,
        message: Arc::from(message),
    }
}

/// The warning for a line that is not JSON: its size, never its text.
fn dropped_line_message(bytes: u64) -> String {
    format!("dropped a harness output line of {bytes} bytes that is not JSON")
}

/// Renders the facts a Claude Code `api_retry` event reports, in the order
/// `api retry A/M · CLASS · HTTP S · next in D ms`, omitting absent facts.
fn api_retry_message(object: &Map<String, Value>) -> String {
    let counter = unsigned(object, "attempt")
        .zip(unsigned(object, "max_retries"))
        .map_or_else(
            || "api retry".to_owned(),
            |(attempt, maximum)| format!("api retry {attempt}/{maximum}"),
        );
    let class = string(object, "error").unwrap_or("provider retry");
    let mut parts = vec![counter, class.to_owned()];
    if let Some(status) = unsigned(object, "error_status") {
        parts.push(format!("HTTP {status}"));
    }
    if let Some(delay) = unsigned(object, "retry_delay_ms") {
        parts.push(format!("next in {delay} ms"));
    }
    parts.join(" · ")
}

/// The warning a compaction Claude Code reports as failed yields, carrying the
/// `compact_error` it names when there is one.
fn compaction_failure_message(error: Option<&str>) -> String {
    match error.filter(|error| !error.is_empty()) {
        Some(error) => format!(
            "context compaction failed: {}",
            ellipsize(error, MAXIMUM_COMPACTION_ERROR_WIDTH)
        ),
        None => String::from("context compaction failed"),
    }
}

/// The call a `tool_progress` heartbeat belongs to: its `tool_use_id` is the call's id
/// followed by `-heartbeat-` and a counter. Any other shape is returned as given.
fn heartbeat_call_id(call_id: &str) -> &str {
    match call_id.rsplit_once("-heartbeat-") {
        Some((call, counter))
            if !call.is_empty()
                && !counter.is_empty()
                && counter.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            call
        }
        _ => call_id,
    }
}

/// A tool result is activity, so its text blocks are concatenated and the other block
/// kinds Claude Code emits (`tool_reference` from ToolSearch, `image` from a screen
/// capture) contribute nothing.
fn normalized_tool_result_content(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Array(blocks) => {
            let mut result = String::new();
            for block in blocks {
                let block = block.as_object()?;
                if nonempty_string(block, "type")? == "text" {
                    result.push_str(string(block, "text")?);
                }
            }
            Some(result)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => None,
    }
}

/// The thread a frame belongs to: a subagent's, by the tool call it names as
/// `parent_tool_use_id`, or the main thread's (`None`).
fn thread(object: &Map<String, Value>) -> Option<&str> {
    nonempty_string(object, "parent_tool_use_id")
}

fn keep_newest<T>(items: &mut Vec<T>, maximum: usize) {
    let excess = items.len().saturating_sub(maximum);
    items.drain(..excess);
}

fn byte_count(length: usize) -> u64 {
    u64::try_from(length).unwrap_or(u64::MAX)
}

fn string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key)?.as_str()
}

fn nonempty_string<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    string(object, key).filter(|value| !value.is_empty())
}

fn boolean(object: &Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key)?.as_bool()
}

fn unsigned(object: &Map<String, Value>, key: &str) -> Option<u64> {
    object.get(key)?.as_u64()
}

fn map<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a Map<String, Value>> {
    object.get(key)?.as_object()
}

fn array<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a [Value]> {
    object.get(key)?.as_array().map(Vec::as_slice)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use serde_json::{Value, json};

    use super::*;
    use crate::claude_code::CLAUDE_CODE_STREAM_JSON_V1_QUALIFICATION_VERSION as QUALIFICATION_VERSION;
    use crate::workflow::agent::AgentFailureCause;

    const SESSION_ID: &str = "00000000-0000-4000-8000-000000000001";
    const RESPONSE_FIXTURE: &[u8] =
        include_bytes!("fixtures/response-duplicates-and-subagent.jsonl");
    const COMPACTION_COMPLETION: &[u8] = include_bytes!("fixtures/compaction-completion.jsonl");

    fn init() -> Value {
        json!({"type": "system", "subtype": "init", "session_id": SESSION_ID,
            "cwd": "/synthetic/project", "model": "scherzo-loopback",
            "permissionMode": "bypassPermissions", "claude_code_version": QUALIFICATION_VERSION})
    }

    fn system_event(subtype: &str, fields: Value) -> Value {
        let mut frame = json!({"type": "system", "subtype": subtype, "session_id": SESSION_ID});
        frame
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        frame
    }

    fn successful(text: &str) -> Value {
        json!({"type": "result", "subtype": "success", "is_error": false,
            "terminal_reason": "completed", "num_turns": 1, "result": text,
            "session_id": SESSION_ID})
    }

    fn stream_event(parent: Option<&str>, event: Value) -> Value {
        json!({"type": "stream_event", "event": event, "session_id": SESSION_ID,
            "parent_tool_use_id": parent})
    }

    fn rate_limit(info: Value) -> Value {
        json!({"type": "rate_limit_event", "rate_limit_info": info, "session_id": SESSION_ID})
    }

    fn api_error_message(code: &str, status: Option<u16>, text: &str) -> Value {
        json!({"type": "assistant", "message": {
            "id": "msg-api-error", "model": "<synthetic>", "role": "assistant",
            "type": "message", "content": [{"type": "text", "text": text}]},
            "parent_tool_use_id": null, "session_id": SESSION_ID,
            "error": code, "api_error_status": status, "is_api_error_message": true})
    }

    fn framed(values: &[Value]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            serde_json::to_writer(&mut bytes, value).unwrap();
            bytes.push(b'\n');
        }
        bytes
    }

    fn new_reader(kind: AgentValueKind) -> StreamReader {
        StreamReader::new(kind, NonZeroU64::new(1024).unwrap())
    }

    /// Reads `bytes` in one push and ends the stream.
    fn read(bytes: &[u8], kind: AgentValueKind) -> (Vec<AgentObservation>, StreamReader) {
        let mut reader = new_reader(kind);
        let mut observations = Vec::new();
        reader.push(bytes, |observation| observations.push(observation));
        reader.finish(|observation| observations.push(observation));
        (observations, reader)
    }

    fn diagnostics_at(
        observations: &[AgentObservation],
        wanted: AgentDiagnosticLevel,
    ) -> Vec<&str> {
        observations
            .iter()
            .filter_map(|observation| match observation {
                AgentObservation::Diagnostic { level, message } if *level == wanted => {
                    Some(message.as_ref())
                }
                _ => None,
            })
            .collect()
    }

    fn warnings(observations: &[AgentObservation]) -> Vec<&str> {
        diagnostics_at(observations, AgentDiagnosticLevel::Warning)
    }

    fn information(observations: &[AgentObservation]) -> Vec<&str> {
        diagnostics_at(observations, AgentDiagnosticLevel::Information)
    }

    fn unrecognized_events(observations: &[AgentObservation]) -> Vec<&Value> {
        observations
            .iter()
            .filter_map(|observation| match observation {
                AgentObservation::UnrecognizedHarnessEvent { event } => Some(event.as_ref()),
                _ => None,
            })
            .collect()
    }

    fn assistant_texts(observations: &[AgentObservation]) -> Vec<&str> {
        observations
            .iter()
            .filter_map(|observation| match observation {
                AgentObservation::AssistantText { text } => Some(text.as_ref()),
                _ => None,
            })
            .collect()
    }

    fn milestone_count(
        observations: &[AgentObservation],
        milestone: AgentLifecycleMilestone,
    ) -> usize {
        observations
            .iter()
            .filter(|observation| {
                matches!(observation, AgentObservation::Lifecycle { milestone: seen } if *seen == milestone)
            })
            .count()
    }

    /// The classified diagnostic of the deciding `result`, which must be failed.
    fn failure_diagnostic(reader: &StreamReader) -> HarnessFailureDiagnostic {
        let Some(DecidingResult::Failed(AgentHarnessFailureDetail::Classified {
            canonical: CanonicalHarnessFailure::ModelError,
            diagnostic,
        })) = reader.deciding_result()
        else {
            panic!(
                "expected a classified failed result, got {:?}",
                reader.deciding_result()
            );
        };
        *diagnostic
    }

    fn successful_text(reader: &StreamReader) -> &ResultText {
        let Some(DecidingResult::Successful(text)) = reader.deciding_result() else {
            panic!(
                "expected a successful result, got {:?}",
                reader.deciding_result()
            );
        };
        text
    }

    // REQ-AGENT-87, REQ-AGENT-38, INV-HARNESS-22
    #[test]
    fn nothing_claude_code_writes_fails_the_reader() {
        let limit = ClaudeCodeStreamJsonV1ProtocolLimits::profile()
            .maximum_frame_bytes()
            .get();
        let limit = usize::try_from(limit).unwrap();
        let unknown_stream_event =
            stream_event(None, json!({"type": "future_block_kind", "index": 0}));
        let unknown_task = system_event(
            "task_started",
            json!({"task_id": "w1", "task_type": "local_workflow", "description": "~~~~"}),
        );
        let lifecycle = json!({"type": "command_lifecycle", "state": "started",
            "session_id": SESSION_ID});
        let result_without_is_error =
            json!({"type": "result", "subtype": "success", "result": "~~~~"});
        let hook = system_event("hook_started", json!({"hook_id": "h1"}));
        let mut bytes = b"~~~~ sentinel, not JSON ~~~~\n".to_vec();
        // A JSON line exactly at the frame limit is read; one byte more is dropped unread.
        for length in [limit, limit + 1] {
            bytes.push(b'"');
            bytes.extend(std::iter::repeat_n(b'~', length - 2));
            bytes.extend(b"\"\n");
        }
        bytes.extend(framed(&[
            successful("before any init"),
            json!([1, 2]),
            json!({"subtype": "no type"}),
            hook.clone(),
            system_event("hook_started", json!({"hook_id": "h2"})),
            json!({"type": "future_additive_event", "session_id": SESSION_ID}),
            json!({"type": "future_additive_event", "session_id": SESSION_ID}),
            unknown_stream_event.clone(),
            stream_event(None, json!({"type": "future_block_kind", "index": 1})),
            unknown_task.clone(),
            lifecycle.clone(),
            lifecycle.clone(),
            result_without_is_error.clone(),
            init(),
            init(),
            // A subagent's frames with no start, under a call nothing announced.
            stream_event(
                Some("unknown-call"),
                json!({"type": "content_block_delta", "index": 3,
                    "delta": {"type": "text_delta", "text": "orphan text"}}),
            ),
            stream_event(
                Some("unknown-call"),
                json!({"type": "content_block_stop", "index": 3}),
            ),
            system_event(
                "task_notification",
                json!({"task_id": "never-started", "status": "completed"}),
            ),
            json!({"type": "result", "subtype": "error_during_execution", "is_error": true,
                "terminal_reason": "error_during_execution"}),
            init(),
            successful("second"),
            json!({"type": "result", "subtype": "success", "is_error": false,
                "num_turns": 0, "result": ""}),
        ]));
        bytes.extend(b"{\"type\": \"result\", \"is_error\": true, ~~~~");
        let (observations, reader) = read(&bytes, AgentValueKind::Response);

        let kinds = unrecognized_events(&observations)
            .into_iter()
            .map(|event| (event["type"].as_str(), event["subtype"].as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                // The line at the frame limit, a JSON string.
                (None, None),
                (None, None),
                (None, Some("no type")),
                (Some("system"), Some("hook_started")),
                (Some("future_additive_event"), None),
                (Some("stream_event"), None),
                (Some("system"), Some("task_started")),
                (Some("command_lifecycle"), None),
                (Some("result"), Some("success")),
            ]
        );
        let unrecognized = unrecognized_events(&observations);
        assert_eq!(unrecognized[3], &hook);
        assert_eq!(unrecognized[5], &unknown_stream_event);
        assert_eq!(unrecognized[6], &unknown_task);
        assert_eq!(unrecognized[7], &lifecycle);
        assert_eq!(unrecognized[8], &result_without_is_error);

        let warnings = warnings(&observations);
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        for warning in &warnings {
            assert!(
                !warning.contains('~') && !warning.contains("sentinel") && !warning.contains('{'),
                "a dropped line's warning carries none of it: {warning:?}"
            );
        }
        assert!(
            warnings[1].contains(&(limit + 1).to_string()),
            "{warnings:?}"
        );
        assert_eq!(assistant_texts(&observations), ["orphan text"]);
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::SessionEstablished),
            1
        );
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::TurnStarted),
            3
        );
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::TurnCompleted),
            4
        );
        assert_eq!(
            reader.initialization(),
            Some(&Initialization {
                claude_code_version: Some(Arc::from(QUALIFICATION_VERSION)),
            })
        );
        assert_eq!(
            successful_text(&reader),
            &ResultText::Text(Arc::from("second"))
        );

        // Through the adapter a line above the frame limit always spans reads: its bytes are
        // skipped across them, it is dropped once, and reading resumes at the next line.
        let mut long_reader = new_reader(AgentValueKind::Response);
        let mut long_observations = Vec::new();
        let over_limit = vec![b'~'; limit + 1];
        for piece in [over_limit.as_slice(), b"~~~~", b"~~\n"] {
            long_reader.push(piece, |observation| long_observations.push(observation));
        }
        long_reader.push(
            &framed(&[successful("after the long line")]),
            |observation| {
                long_observations.push(observation);
            },
        );
        long_reader.finish(|observation| long_observations.push(observation));
        let long_warnings = diagnostics_at(&long_observations, AgentDiagnosticLevel::Warning);
        assert_eq!(long_warnings.len(), 1, "{long_warnings:?}");
        assert!(
            long_warnings[0].contains(&(limit + 7).to_string()) && !long_warnings[0].contains('~'),
            "{long_warnings:?}"
        );
        assert_eq!(
            successful_text(&long_reader),
            &ResultText::Text(Arc::from("after the long line"))
        );

        // Recorded streams cut at every byte and read in 7-byte chunks: every push returns,
        // and what is shown and decided is what the complete lines read whole give, with the
        // cut line dropped with a warning.
        for (fixture, version, decided) in [
            (
                RESPONSE_FIXTURE,
                Some(QUALIFICATION_VERSION),
                Some(DecidingResult::Successful(ResultText::Text(Arc::from(
                    "wer",
                )))),
            ),
            (COMPACTION_COMPLETION, None, None),
        ] {
            let (whole, whole_reader) = read(fixture, AgentValueKind::Response);
            assert_eq!(unrecognized_events(&whole), Vec::<&Value>::new());
            assert_eq!(
                whole_reader
                    .initialization()
                    .map(|initialization| initialization.claude_code_version.as_deref()),
                version.map(Some)
            );
            assert_eq!(whole_reader.deciding_result(), decided.as_ref());
            for cut in 0..=fixture.len() {
                let prefix = &fixture[..cut];
                let complete = prefix
                    .iter()
                    .rposition(|byte| *byte == b'\n')
                    .map_or(&prefix[..0], |end| &prefix[..=end]);
                let (expected, expected_reader) = read(complete, AgentValueKind::Response);
                let mut chunked = new_reader(AgentValueKind::Response);
                let mut observations = Vec::new();
                for chunk in prefix.chunks(7) {
                    chunked.push(chunk, |observation| observations.push(observation));
                }
                chunked.finish(|observation| observations.push(observation));
                let (read_lines, trailing) = observations.split_at(expected.len());
                assert_eq!(read_lines, expected.as_slice(), "cut at {cut}");
                match trailing {
                    [] => assert_eq!(complete.len(), cut, "cut at {cut}"),
                    [warning] => {
                        let dropped = (cut - complete.len()).to_string();
                        assert!(
                            matches!(warning, AgentObservation::Diagnostic {
                                level: AgentDiagnosticLevel::Warning, message,
                            } if message.contains(&dropped) && !message.contains('{')),
                            "cut at {cut}: {warning:?}"
                        );
                    }
                    _ => panic!("cut at {cut}: {trailing:?}"),
                }
                assert_eq!(
                    chunked.initialization(),
                    expected_reader.initialization(),
                    "cut at {cut}"
                );
                assert_eq!(
                    chunked.deciding_result(),
                    expected_reader.deciding_result(),
                    "cut at {cut}"
                );
            }
        }
    }

    // REQ-AGENT-26, REQ-AGENT-37
    #[test]
    fn a_streamed_message_is_shown_once_and_its_envelope_is_not_shown_again() {
        let (observations, reader) = read(RESPONSE_FIXTURE, AgentValueKind::Response);
        // The subagent's message arrived only as its envelope, so the envelope shows it.
        assert_eq!(
            assistant_texts(&observations),
            ["earlier", "subagent", "final ", "answer"]
        );
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::MessageCompleted),
            2
        );
        assert_eq!(
            successful_text(&reader),
            &ResultText::Text(Arc::from("wer"))
        );
    }

    // REQ-AGENT-37
    #[test]
    fn api_retry_warning_names_the_reported_attempt_class_status_and_delay() {
        let (observations, _) = read(
            &framed(&[
                init(),
                system_event(
                    "api_retry",
                    json!({"attempt": 2, "max_retries": 10, "retry_delay_ms": 4000,
                        "error_status": 529, "error": "overloaded"}),
                ),
                system_event(
                    "api_retry",
                    json!({"attempt": 3, "max_retries": 10, "retry_delay_ms": 8000,
                        "error_status": null, "error": "unknown"}),
                ),
                system_event("api_retry", json!({"error": "rate_limit"})),
                successful("done"),
            ]),
            AgentValueKind::None,
        );
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::RetryStarted),
            1
        );
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::RetryCompleted),
            1
        );
        let warnings = warnings(&observations);
        let [overloaded, unknown, uncounted] = warnings.as_slice() else {
            panic!("each api_retry must yield one warning: {observations:?}");
        };
        for token in ["2/10", "overloaded", "HTTP 529", "4000"] {
            assert!(overloaded.contains(token), "{overloaded:?} lacks {token:?}");
        }
        assert!(unknown.contains("3/10") && unknown.contains("unknown"));
        assert!(!unknown.contains("HTTP"), "{unknown:?}");
        assert!(uncounted.contains("rate_limit"), "{uncounted:?}");
        assert!(!uncounted.contains('/'), "{uncounted:?}");
    }

    /// The frames of `COMPACTION_COMPLETION`, with its completion report and the
    /// `compact_boundary` after it replaced by `completion`.
    fn compaction_ending_with(completion: &Value) -> Vec<u8> {
        let frames = COMPACTION_COMPLETION
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<Value>(line).unwrap())
            .filter(|frame| frame["subtype"] != "compact_boundary")
            .map(|frame| {
                if frame.get("compact_result").is_some() {
                    completion.clone()
                } else {
                    frame
                }
            })
            .collect::<Vec<_>>();
        framed(&frames)
    }

    // REQ-AGENT-37, REQ-AGENT-79
    #[test]
    fn a_compaction_that_completes_mid_turn_is_activity() {
        let (observations, _) = read(COMPACTION_COMPLETION, AgentValueKind::Result);
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::CompactionCompleted),
            1
        );
        assert_eq!(warnings(&observations), Vec::<&str>::new());
        assert_eq!(unrecognized_events(&observations), Vec::<&Value>::new());
        assert_eq!(
            information(&observations),
            [
                "requesting",
                "compacting",
                "compacting",
                "compacting",
                "compacting",
                "requesting"
            ]
        );
    }

    // REQ-AGENT-79, REQ-AGENT-87
    #[test]
    fn a_failed_or_cleared_compaction_status_is_activity_and_any_other_status_is_unrecognized() {
        let marker = "compaction marker: the summary request was refused";
        let (failed, _) = read(
            &compaction_ending_with(&system_event(
                "status",
                json!({"status": null, "compact_result": "failed", "compact_error": marker}),
            )),
            AgentValueKind::Result,
        );
        let (cleared, _) = read(
            &compaction_ending_with(&system_event("status", json!({"status": null}))),
            AgentValueKind::Result,
        );
        for observations in [&failed, &cleared] {
            assert_eq!(
                milestone_count(observations, AgentLifecycleMilestone::CompactionCompleted),
                0
            );
            assert_eq!(unrecognized_events(observations), Vec::<&Value>::new());
        }
        let [warning] = warnings(&failed)[..] else {
            panic!("a failed compaction must yield one warning: {failed:?}");
        };
        assert!(warning.contains(marker), "{warning:?}");
        assert_eq!(warnings(&cleared), Vec::<&str>::new());

        for fields in [
            json!({}),
            json!({"status": 7}),
            json!({"status": ""}),
            json!({"status": "compacting", "compact_result": "success"}),
            json!({"status": null, "compact_result": "partial"}),
            json!({"status": null, "compact_result": 1}),
            json!({"status": null, "compact_error": "x"}),
            json!({"status": null, "compact_result": "success", "compact_error": "x"}),
            json!({"status": null, "compact_result": "failed", "compact_error": 5}),
        ] {
            let frame = system_event("status", fields);
            let (observations, _) = read(
                &framed(std::slice::from_ref(&frame)),
                AgentValueKind::Response,
            );
            assert_eq!(unrecognized_events(&observations), [&frame]);
            assert_eq!(observations.len(), 1, "{frame}");
        }
    }

    // REQ-AGENT-64, REQ-AGENT-73, REQ-AGENT-38
    #[test]
    fn subagents_are_named_at_start_resume_and_settlement_and_other_tasks_are_not_tracked() {
        let started = |fields: Value| {
            let mut frame = system_event(
                "task_started",
                json!({"task_id": "t1", "tool_use_id": "agent-call", "task_type": "local_agent",
                    "description": "Review the diff", "subagent_type": "Explore"}),
            );
            frame
                .as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            frame
        };
        let workflow_task = started(json!({"task_id": "w1", "task_type": "local_workflow"}));
        let (observations, _) = read(
            &framed(&[
                init(),
                started(json!({"is_backgrounded": true})),
                // A subagent's own command, its progress and the task list are not tracked.
                system_event(
                    "task_started",
                    json!({"task_id": "sub-task", "owned_by_subagent": true,
                        "task_type": "local_bash", "tool_use_id": "sub-bash"}),
                ),
                system_event("task_progress", json!({"task_id": "t1"})),
                system_event("task_updated", json!({"task_id": "t1", "patch": {}})),
                system_event(
                    "background_tasks_changed",
                    json!({"tasks": [{"task_id": "t1"}, {"task_id": "w1"}]}),
                ),
                system_event(
                    "task_notification",
                    json!({"task_id": "t1", "status": "completed",
                        "summary": "Found two issues"}),
                ),
                // A message to the finished subagent resumes it under another call.
                started(json!({"tool_use_id": "send-message", "description": null})),
                system_event("task_notification", json!({"task_id": "t1"})),
                system_event(
                    "task_notification",
                    json!({"task_id": "sub-task", "status": "completed"}),
                ),
                workflow_task.clone(),
                system_event("task_notification", json!({"task_id": "w1", "status": "x"})),
                successful("done"),
            ]),
            AgentValueKind::Response,
        );
        assert_eq!(
            information(&observations),
            [
                "subagent Explore started · Review the diff",
                "subagent Explore completed · Review the diff",
                "subagent Explore resumed · Review the diff",
                "subagent Explore settled · Review the diff",
            ]
        );
        assert_eq!(unrecognized_events(&observations), [&workflow_task]);
    }

    // REQ-AGENT-38
    #[test]
    fn reasoning_tools_results_metadata_and_unknown_events_are_normalized() {
        let main = |event: Value| stream_event(None, event);
        let (observations, _) = read(
            &framed(&[
                init(),
                main(json!({"type": "message_start", "message": {
                    "id": "msg-tools", "type": "message", "role": "assistant", "content": [],
                    "model": "scherzo-loopback",
                    "usage": {"input_tokens": 1, "output_tokens": 0}}})),
                main(json!({"type": "content_block_start", "index": 0,
                    "content_block": {"type": "thinking", "thinking": "plan"}})),
                main(json!({"type": "content_block_delta", "index": 0,
                    "delta": {"type": "thinking_delta", "thinking": " more"}})),
                main(json!({"type": "content_block_delta", "index": 0,
                    "delta": {"type": "signature_delta", "signature": "sig"}})),
                main(json!({"type": "content_block_stop", "index": 0})),
                main(json!({"type": "content_block_start", "index": 1,
                    "content_block": {"type": "tool_use", "id": "tool-1", "name": "Agent",
                        "input": {}}})),
                main(json!({"type": "content_block_delta", "index": 1,
                    "delta": {"type": "input_json_delta", "partial_json": "{}"}})),
                main(json!({"type": "content_block_stop", "index": 1})),
                main(
                    json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"},
                    "usage": {"output_tokens": 2}}),
                ),
                main(json!({"type": "message_stop"})),
                json!({"type": "user", "message": {"role": "user", "content": [{
                    "type": "tool_result", "tool_use_id": "tool-1",
                    "content": [{"type": "text", "text": "contents"}]}]},
                    "parent_tool_use_id": null, "session_id": SESSION_ID,
                    "tool_use_result": {"status": "completed", "agentId": "agent-fixture"}}),
                json!({"type": "tool_progress", "tool_use_id": "tool-1-heartbeat-3",
                    "tool_name": "Agent", "heartbeat": true, "session_id": SESSION_ID}),
                json!({"type": "future_additive_event", "session_id": SESSION_ID}),
                successful("done"),
            ]),
            AgentValueKind::Response,
        );
        let reasoning = observations
            .iter()
            .filter_map(|observation| match observation {
                AgentObservation::Reasoning { text } => Some(text.as_ref()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(reasoning, ["plan", " more"]);
        let tool_calls = observations
            .iter()
            .filter_map(|observation| match observation {
                AgentObservation::ToolCall {
                    call_id,
                    name,
                    phase,
                } => Some((call_id.as_ref(), name.as_ref(), *phase)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            tool_calls,
            [
                ("tool-1", "Agent", AgentToolCallPhase::Started),
                ("tool-1", "Agent", AgentToolCallPhase::Updated),
                ("tool-1", "Agent", AgentToolCallPhase::Completed),
                ("tool-1", "Agent", AgentToolCallPhase::Updated),
            ]
        );
        assert!(observations.contains(&AgentObservation::ToolResult {
            call_id: Arc::from("tool-1"),
            is_error: false,
            content: Arc::from("contents"),
        }));
        assert!(observations.contains(&AgentObservation::Model {
            name: Arc::from("scherzo-loopback"),
        }));
        let usage = observations
            .iter()
            .filter(|observation| matches!(observation, AgentObservation::Usage { .. }))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            usage,
            [
                AgentObservation::Usage {
                    input_tokens: 1,
                    output_tokens: 0
                },
                AgentObservation::Usage {
                    input_tokens: 1,
                    output_tokens: 2
                },
            ]
        );
        assert_eq!(unrecognized_events(&observations).len(), 1);
    }

    // REQ-AGENT-38
    #[test]
    fn tool_result_blocks_other_than_text_are_observed_without_content() {
        let user = |block: Value| {
            json!({"type": "user", "message": {"role": "user", "content": [block]},
                "parent_tool_use_id": null, "session_id": SESSION_ID})
        };
        let unreadable = user(json!({"type": "tool_result", "tool_use_id": "tool-bash",
            "content": [{"type": "text"}]}));
        let (observations, _) = read(
            &framed(&[
                user(
                    json!({"type": "tool_result", "tool_use_id": "tool-bash", "content": [
                        {"type": "tool_reference", "tool_name": "Bash"},
                        {"type": "text", "text": "README.md"},
                        {"type": "image", "source": {"type": "base64", "media_type": "image/png",
                            "data": "AA=="}},
                    ]}),
                ),
                unreadable.clone(),
            ]),
            AgentValueKind::Response,
        );
        assert_eq!(
            observations,
            [
                AgentObservation::ToolResult {
                    call_id: Arc::from("tool-bash"),
                    is_error: false,
                    content: Arc::from("README.md"),
                },
                AgentObservation::UnrecognizedHarnessEvent {
                    event: Arc::new(unreadable),
                },
            ]
        );
    }

    // REQ-AGENT-26
    #[test]
    fn a_subagent_message_names_its_own_model() {
        let subagent = |event: Value| stream_event(Some("tool-spawn-subagent"), event);
        let (observations, _) = read(
            &framed(&[
                init(),
                subagent(json!({"type": "message_start", "message": {
                    "id": "msg-subagent", "type": "message", "role": "assistant",
                    "content": [], "model": "builder-model",
                    "usage": {"input_tokens": 1, "output_tokens": 0}}})),
                subagent(json!({"type": "content_block_start", "index": 0,
                    "content_block": {"type": "text", "text": ""}})),
                subagent(json!({"type": "content_block_delta", "index": 0,
                    "delta": {"type": "text_delta", "text": "subagent"}})),
                subagent(json!({"type": "content_block_stop", "index": 0})),
                subagent(json!({"type": "message_stop"})),
                json!({"type": "assistant", "message": {"id": "msg-subagent",
                    "type": "message", "role": "assistant", "model": "builder-model",
                    "content": [{"type": "text", "text": "subagent"}]},
                    "parent_tool_use_id": "tool-spawn-subagent", "session_id": SESSION_ID}),
            ]),
            AgentValueKind::Response,
        );
        assert!(observations.contains(&AgentObservation::Model {
            name: Arc::from("builder-model"),
        }));
        assert_eq!(assistant_texts(&observations), ["subagent"]);
    }

    // REQ-AGENT-45, REQ-AGENT-70, REQ-AGENT-86
    #[test]
    fn structured_claude_terminal_reason_is_classified_without_result_text() {
        for (reason, expected, status) in [
            ("rate_limit", "rate_limited", None),
            ("context_window_exceeded", "context_window_exceeded", None),
            ("prompt_too_long", "context_window_exceeded", Some(400)),
            ("error_during_execution", "other", None),
        ] {
            let (_, reader) = read(
                &framed(&[
                    init(),
                    json!({"type": "result", "subtype": "error_during_execution",
                        "is_error": true, "terminal_reason": reason,
                        "api_error_status": status,
                        "result": "private sentinel /path token", "session_id": SESSION_ID}),
                ]),
                AgentValueKind::Response,
            );
            let diagnostic = failure_diagnostic(&reader);
            assert_eq!(diagnostic.harness_error, Some(expected));
            assert_eq!(diagnostic.http_status, status);
        }
        // A `terminal_reason` other than `completed` fails a `result` that says it succeeded,
        // and its text is no response (2.1.321's refused session).
        let (_, reader) = read(
            &framed(&[
                json!({"type": "result", "subtype": "success", "is_error": false,
                "terminal_reason": "turn_setup_failed", "num_turns": 0,
                "result": "Claude Code didn't start."}),
            ]),
            AgentValueKind::Response,
        );
        assert_eq!(failure_diagnostic(&reader).harness_error, Some("other"));
    }

    // REQ-AGENT-40, REQ-AGENT-45, REQ-DIAG-24, REQ-DIAG-36
    #[test]
    fn an_api_error_message_classifies_the_next_failed_result() {
        let api_error_result = |status: Option<u16>| {
            json!({"type": "result", "subtype": "success", "is_error": true,
                "terminal_reason": "api_error", "result": "private sentinel",
                "api_error_status": status, "session_id": SESSION_ID})
        };
        let (observations, reader) = read(
            &framed(&[
                init(),
                api_error_message("unknown", None, "API Error: controlled rejection"),
                api_error_result(None),
            ]),
            AgentValueKind::None,
        );
        assert_eq!(
            failure_diagnostic(&reader),
            HarnessFailureDiagnostic::from_code("other", None)
        );
        assert_eq!(
            diagnostics_at(&observations, AgentDiagnosticLevel::Error),
            ["API Error: controlled rejection"]
        );

        // Claude Code 2.1.306 answers a login-less first exchange with a `<synthetic>`
        // api-error message that carries no `request_id`.
        let (observations, reader) = read(
            &framed(&[
                init(),
                api_error_message(
                    "authentication_failed",
                    None,
                    "Not logged in · Please run /login",
                ),
                api_error_result(None),
            ]),
            AgentValueKind::None,
        );
        assert_eq!(
            failure_diagnostic(&reader),
            HarnessFailureDiagnostic::from_code("authentication_failed", None)
        );
        assert_eq!(
            diagnostics_at(&observations, AgentDiagnosticLevel::Error),
            ["Not logged in · Please run /login"]
        );

        for (code, expected, assistant_status, result_status, selected_status) in [
            (
                "rate_limit_error",
                "rate_limited",
                Some(429),
                None,
                Some(429),
            ),
            (
                "authentication_failed",
                "unauthorized",
                Some(401),
                Some(403),
                Some(403),
            ),
            ("invalid_request", "bad_request", None, Some(400), Some(400)),
        ] {
            let (_, reader) = read(
                &framed(&[
                    init(),
                    api_error_message(code, assistant_status, "private sentinel"),
                    api_error_result(result_status),
                ]),
                AgentValueKind::None,
            );
            let Some(DecidingResult::Failed(detail)) = reader.deciding_result().cloned() else {
                panic!("expected a failed result");
            };
            let diagnostic = failure_diagnostic(&reader);
            assert_eq!(diagnostic.harness_error, Some(expected));
            assert_eq!(diagnostic.http_status, selected_status);
            let failure =
                crate::workflow::agent::AgentFailure::new(AgentFailureCause::HarnessFailed {
                    detail,
                });
            let projected = crate::workflow::evidence::failure_detail(
                crate::workflow::evidence::FailurePhase::Execution,
                &crate::workflow::step_runtime::StepFailureCause::Execution(
                    crate::workflow::step_runtime::StepExecutionFailure::Agent(failure),
                ),
            )
            .unwrap()
            .runner_diagnostic()
            .unwrap();
            let mut exact = json!({"harnessError": expected, "harnessFailure": "model_error"});
            if let Some(status) = selected_status {
                exact["httpStatus"] = json!(status);
            }
            assert_eq!(projected, exact);
            assert!(!projected.to_string().contains("sentinel"));
        }

        // Any `result` clears the api-error message: the next failed one is classified by
        // its own fields.
        let (_, reader) = read(
            &framed(&[
                init(),
                api_error_message("rate_limit_error", Some(429), "private sentinel"),
                successful("recovered"),
                api_error_result(None),
            ]),
            AgentValueKind::None,
        );
        assert_eq!(
            failure_diagnostic(&reader),
            HarnessFailureDiagnostic::from_code("success", None)
        );
    }

    // REQ-AGENT-72
    #[test]
    fn rate_limit_events_warn_only_when_a_window_is_not_allowed() {
        let (observations, _) = read(
            &framed(&[
                init(),
                rate_limit(json!({"status": "allowed", "rateLimitType": "five_hour"})),
                rate_limit(json!({"status": "allowed_warning", "rateLimitType": "seven_day"})),
                rate_limit(json!({"status": "rejected"})),
                rate_limit(json!({})),
                successful("done"),
            ]),
            AgentValueKind::None,
        );
        assert_eq!(
            warnings(&observations),
            [
                "provider rate limit allowed_warning (seven_day)",
                "provider rate limit rejected",
                "provider rate limit",
            ]
        );
    }

    // REQ-AGENT-72
    #[test]
    fn rejected_window_reset_time_is_kept_only_for_a_limit_failure() {
        let api_error = |code: &str, status: u16| {
            [
                api_error_message(code, Some(status), "private sentinel"),
                json!({"type": "result", "subtype": "success", "is_error": true,
                    "terminal_reason": "api_error", "result": "private sentinel",
                    "session_id": SESSION_ID}),
            ]
        };
        let diagnostic = |frames: &[Value]| {
            let mut values = vec![init()];
            values.extend_from_slice(frames);
            let (_, reader) = read(&framed(&values), AgentValueKind::None);
            failure_diagnostic(&reader)
        };
        let rejected = rate_limit(json!({
            "status": "rejected", "rateLimitType": "seven_day", "resetsAt": 1_761_336_000,
        }));

        let [message, result] = api_error("rate_limit", 429);
        let limited = diagnostic(&[rejected.clone(), message, result]);
        assert_eq!(limited.harness_error, Some("rate_limited"));
        assert_eq!(limited.http_status, Some(429));
        assert_eq!(limited.resets_at, Some(1_761_336_000));

        // A later allowed report does not clear the kept reset; a structured result reason
        // carries it as the api-error message does.
        let structured = diagnostic(&[
            rejected.clone(),
            rate_limit(json!({"status": "allowed", "rateLimitType": "five_hour", "resetsAt": 1})),
            json!({"type": "result", "subtype": "error_during_execution", "is_error": true,
                "terminal_reason": "rate_limit", "session_id": SESSION_ID}),
        ]);
        assert_eq!(structured.harness_error, Some("rate_limited"));
        assert_eq!(structured.resets_at, Some(1_761_336_000));

        for window in [
            json!({"status": "allowed", "rateLimitType": "seven_day", "resetsAt": 1_761_336_000}),
            json!({"status": "rejected", "rateLimitType": "seven_day"}),
            json!({"status": "rejected", "rateLimitType": "seven_day", "resetsAt": -1}),
        ] {
            let [message, result] = api_error("rate_limit", 429);
            let unknown = diagnostic(&[rate_limit(window), message, result]);
            assert_eq!(
                unknown,
                HarnessFailureDiagnostic::from_code("rate_limit", Some(429))
            );
            assert_eq!(unknown.resets_at, None);
        }

        let [message, result] = api_error("authentication_failed", 401);
        let unauthorized = diagnostic(&[rejected, message, result]);
        assert_eq!(unauthorized.harness_error, Some("unauthorized"));
        assert_eq!(unauthorized.resets_at, None);
    }
}
