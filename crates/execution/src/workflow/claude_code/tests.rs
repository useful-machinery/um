use std::ffi::OsString;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;

use serde_json::{Value, json};

use super::*;

const SESSION_ID: &str = "00000000-0000-4000-8000-000000000001";
const CWD: &str = "/synthetic/project";
const MODEL: &str = "scherzo-loopback";
const RESPONSE_FIXTURE: &[u8] = include_bytes!("fixtures/response-duplicates-and-subagent.jsonl");
const CONTRADICTORY_INIT: &[u8] = include_bytes!("fixtures/contradictory-init.jsonl");
const MISSING_INIT: &[u8] = include_bytes!("fixtures/missing-init.jsonl");
const MALFORMED: &[u8] = include_bytes!("fixtures/malformed.jsonl");

fn init(version: &str, session_id: &str) -> Value {
    json!({
        "type": "system",
        "subtype": "init",
        "cwd": CWD,
        "session_id": session_id,
        "model": MODEL,
        "permissionMode": "bypassPermissions",
        "claude_code_version": version,
    })
}

fn status(session_id: &str) -> Value {
    json!({
        "type": "system",
        "subtype": "status",
        "status": "requesting",
        "session_id": session_id,
    })
}

fn result(session_id: &str) -> Value {
    json!({
        "type": "result",
        "subtype": "success",
        "is_error": false,
        "terminal_reason": "completed",
        "result": "observational convenience text",
        "session_id": session_id,
    })
}

fn framed(values: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        serde_json::to_writer(&mut bytes, value).unwrap();
        bytes.push(b'\n');
    }
    bytes
}

fn parser(kind: AgentValueKind, maximum_response_bytes: u64) -> ClaudeCodeStreamJsonV1Parser {
    parser_for_version(kind, maximum_response_bytes, QUALIFICATION_VERSION)
}

fn parser_for_version(
    kind: AgentValueKind,
    maximum_response_bytes: u64,
    version: &str,
) -> ClaudeCodeStreamJsonV1Parser {
    ClaudeCodeStreamJsonV1Parser::profile(
        Arc::from(CWD),
        Arc::from(MODEL),
        Arc::from(SESSION_ID),
        Arc::from(version),
        kind,
        NonZeroU64::new(maximum_response_bytes).unwrap(),
    )
}

fn replay(
    bytes: &[u8],
    kind: AgentValueKind,
    maximum_response_bytes: u64,
) -> (Vec<AgentObservation>, AgentOutcome) {
    let mut parser = parser(kind, maximum_response_bytes);
    let mut observations = Vec::new();
    let _ = parser.push_stdout(bytes, |observation| observations.push(observation));
    (observations, parser.finish(true))
}

fn stream_event(event: Value) -> Value {
    json!({
        "type": "stream_event",
        "event": event,
        "session_id": SESSION_ID,
        "parent_tool_use_id": null,
    })
}

fn message_start(id: &str) -> Value {
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
    }))
}

fn text_message(id: &str, deltas: &[&str], present: bool) -> Vec<Value> {
    let mut values = vec![message_start(id)];
    if present {
        values.push(stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""},
        })));
        values.extend(deltas.iter().map(|text| {
            stream_event(json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text},
            }))
        }));
        values.push(stream_event(json!({
            "type": "content_block_stop",
            "index": 0,
        })));
    }
    values.extend([
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
    ]);
    values
}

fn completed_text_transcript(deltas: &[&str], present: bool) -> Vec<u8> {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(text_message("msg-final", deltas, present));
    values.push(result(SESSION_ID));
    framed(&values)
}

fn assert_failed(outcome: AgentOutcome, cause: AgentFailureCause) {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected agent failure");
    };
    assert_eq!(failure.cause(), &cause);
}

fn protocol_rejection_value(outcome: &AgentOutcome) -> Value {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected agent failure, got {outcome:?}");
    };
    serde_json::to_value(
        failure
            .protocol_rejection()
            .expect("parser-owned failure must carry a rejection diagnostic"),
    )
    .unwrap()
}

// REQ-AGENT-16, REQ-AGENT-60
// INV-HARNESS-3
#[test]
fn normal_mode_arguments_and_input_frame_are_exact() {
    let arguments = normal_mode_arguments(
        "claude-profile-model",
        "xhigh",
        SESSION_ID,
        Path::new("/synthetic/private/system-prompt"),
    );
    assert_eq!(
        arguments,
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
            SESSION_ID,
            "--permission-mode",
            "bypassPermissions",
            "--setting-sources",
            "user,project,local",
            "--model",
            "claude-profile-model",
            "--effort",
            "xhigh",
            "--append-system-prompt-file",
            "/synthetic/private/system-prompt",
        ]
        .map(OsString::from)
    );

    let frame = initial_user_text_frame("exact\nuser text").unwrap();
    assert_eq!(frame.last(), Some(&b'\n'));
    assert_eq!(frame.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&frame[..frame.len() - 1]).unwrap(),
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": "exact\nuser text"}],
            },
        })
    );
}

#[test]
fn parser_requires_matching_initialization_and_serialized_exchange_results() {
    let mut parser = parser(AgentValueKind::None, 1024);
    let first = framed(&[
        init(QUALIFICATION_VERSION, SESSION_ID),
        status(SESSION_ID),
        result(SESSION_ID),
    ]);
    for byte in first {
        parser.push_stdout(&[byte], drop).unwrap();
    }

    parser.begin_exchange().unwrap();
    parser
        .push_stdout(
            &framed(&[init(QUALIFICATION_VERSION, SESSION_ID), result(SESSION_ID)]),
            drop,
        )
        .unwrap();
    assert_eq!(parser.session_id(), Some(SESSION_ID));
    assert_eq!(parser.completed_exchanges(), 2);
    assert_eq!(
        parser.finish(true),
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );
}

fn system_event(subtype: &str, fields: Value) -> Value {
    let mut frame = json!({"type": "system", "subtype": subtype, "session_id": SESSION_ID});
    frame
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    frame
}

fn api_retry(fields: Value) -> Value {
    system_event("api_retry", fields)
}

// REQ-AGENT-37
#[test]
fn api_retry_warning_names_the_reported_attempt_class_status_and_delay() {
    let mut parser = parser(AgentValueKind::None, 1024);
    let mut observations = Vec::new();
    parser
        .push_stdout(
            &framed(&[
                init(QUALIFICATION_VERSION, SESSION_ID),
                api_retry(
                    json!({"attempt": 2, "max_retries": 10, "retry_delay_ms": 4000,
                    "error_status": 529, "error": "overloaded"}),
                ),
                api_retry(
                    json!({"attempt": 3, "max_retries": 10, "retry_delay_ms": 8000,
                    "error_status": null, "error": "unknown"}),
                ),
                api_retry(json!({"error": "rate_limit"})),
            ]),
            |observation| observations.push(observation),
        )
        .unwrap();

    assert_eq!(
        milestone_count(&observations, AgentLifecycleMilestone::RetryStarted),
        1
    );
    let warnings = observations
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Warning,
                message,
            } => Some(message.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
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

/// One exchange whose assistant turn calls the Agent tool, with `lifecycle` between the
/// tool call and its result, ending in the final response `done`.
fn agent_tool_exchange(lifecycle: &[Value]) -> Vec<Value> {
    let mut values = vec![
        init(QUALIFICATION_VERSION, SESSION_ID),
        message_start("agent-turn"),
    ];
    values.extend([
        stream_event(json!({"type":"content_block_start","index":0,"content_block":{
            "type":"tool_use","id":"agent-call","name":"Agent","input":{}
        }})),
        stream_event(json!({"type":"content_block_delta","index":0,"delta":{
            "type":"input_json_delta","partial_json":"{}"
        }})),
        stream_event(json!({"type":"content_block_stop","index":0})),
        stream_event(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}})),
        stream_event(json!({"type":"message_stop"})),
    ]);
    values.extend_from_slice(lifecycle);
    values.push(
        json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
        "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"agent-call",
            "content":[{"type":"text","text":"subagent report"}],"is_error":false}]}}),
    );
    values.extend(text_message("final-answer", &["done"], true));
    values.push(result(SESSION_ID));
    values
}

fn subagent_started(fields: Value) -> Value {
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
}

fn information_diagnostics(observations: &[AgentObservation]) -> Vec<&str> {
    observations
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Information,
                message,
            } => Some(message.as_ref()),
            _ => None,
        })
        .collect()
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

fn completed_response(outcome: &AgentOutcome) -> &str {
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("the exchange must complete with a response, got {outcome:?}");
    };
    response.as_str()
}

// REQ-AGENT-64, REQ-AGENT-38
#[test]
fn foreground_subagent_lifecycle_is_activity_without_unrecognized_events() {
    let values = agent_tool_exchange(&[
        subagent_started(json!({})),
        // The subagent's own backgrounded Bash run belongs to it, not to the step.
        system_event(
            "task_started",
            json!({"task_id": "sub-task", "run_id": "sub-run", "owned_by_subagent": true,
                "tool_use_id": "sub-bash", "description": "Watch tests", "is_backgrounded": true,
                "parent_task_id": "t1", "task_type": "local_bash"}),
        ),
        system_event(
            "task_progress",
            json!({"task_id": "t1", "tool_use_id": "agent-call", "description": "Reading files",
                "last_tool_name": "Read", "usage": {"total_tokens": 10, "tool_uses": 1}}),
        ),
        system_event(
            "task_updated",
            json!({"task_id": "t1", "patch": {"status": "running"}}),
        ),
        system_event(
            "background_tasks_changed",
            json!({"tasks": [{"task_id": "t1", "task_type": "local_agent"}]}),
        ),
        system_event(
            "task_notification",
            json!({"task_id": "t1", "tool_use_id": "agent-call", "status": "completed",
                "summary": "Found two issues"}),
        ),
    ]);
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "done");
    let information = information_diagnostics(&observations);
    let [started, settled] = information.as_slice() else {
        panic!("start and settlement must each yield one diagnostic: {observations:?}");
    };
    assert!(started.contains("Explore") && started.contains("Review the diff"));
    assert!(settled.contains("Explore") && settled.contains("completed"));
    assert!(!settled.contains("Found two issues"), "{settled:?}");
    assert_eq!(unrecognized_events(&observations), Vec::<&Value>::new());

    // A subagent the exchange ends without settling is forgotten, not a protocol failure.
    let values = agent_tool_exchange(&[subagent_started(json!({"is_backgrounded": false}))]);
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "done");
    assert_eq!(information_diagnostics(&observations).len(), 1);

    // A second start for a tracked task is a protocol failure, as for background commands.
    let values = agent_tool_exchange(&[subagent_started(json!({})), subagent_started(json!({}))]);
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
}

// REQ-AGENT-64, REQ-AGENT-38
#[test]
fn other_task_kinds_stay_unrecognized() {
    let start = subagent_started(json!({"task_type": "local_workflow"}));
    let values = agent_tool_exchange(std::slice::from_ref(&start));
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "done");
    assert_eq!(unrecognized_events(&observations), [&start]);
    assert_eq!(information_diagnostics(&observations), Vec::<&str>::new());
}

/// A message of the backgrounded subagent the Agent call `agent-call` started.
fn subagent_message(kind: &str, message: Value) -> Value {
    json!({"type": kind, "session_id": SESSION_ID, "parent_tool_use_id": "agent-call",
        "agent_id": "agent-1", "subagent_type": "Explore", "task_description": "Review the diff",
        "uuid": "00000000-0000-4000-8000-0000000000a1", "timestamp": "2026-10-23T00:00:00Z",
        "message": message})
}

/// Sanitized from the sequence Claude Code 2.1.306 emits when the main thread ends its
/// turn while a subagent it started with `run_in_background` keeps working: the subagent's
/// messages, its own Bash run and its progress arrive after the first `result`; its
/// settlement and an empty task list precede a fresh `init` and the final main-thread turn.
fn backgrounded_subagent_transcript() -> Vec<Value> {
    let mut values = agent_tool_exchange(&[
        subagent_started(json!({"run_id": "agent-run", "is_backgrounded": true,
            "spawn_depth": 1, "prompt": "Review the diff"})),
        system_event(
            "background_tasks_changed",
            json!({"tasks": [{"task_id": "t1", "run_id": "agent-run", "task_type": "local_agent"}]}),
        ),
    ]);
    values.extend([
        subagent_message(
            "assistant",
            json!({"id": "msg-sub-1", "type": "message", "role": "assistant",
                "model": "subagent-model", "content": [{"type": "tool_use", "id": "sub-bash",
                "name": "Bash", "input": {"command": "ls"}}]}),
        ),
        system_event(
            "task_started",
            json!({"task_id": "sub-task", "run_id": "sub-run", "owned_by_subagent": true,
                "tool_use_id": "sub-bash", "description": "List files", "is_backgrounded": false,
                "parent_task_id": "t1", "task_type": "local_bash"}),
        ),
        system_event(
            "task_notification",
            json!({"task_id": "sub-task", "run_id": "sub-run", "tool_use_id": "sub-bash",
                "status": "completed", "output_file": "", "summary": "List files"}),
        ),
        system_event(
            "task_progress",
            json!({"task_id": "t1", "run_id": "agent-run", "tool_use_id": "agent-call",
                "description": "Running ls", "subagent_type": "Explore",
                "usage": {"total_tokens": 10, "tool_uses": 1}, "last_tool_name": "Bash"}),
        ),
        subagent_message(
            "user",
            json!({"role": "user", "content": [{"type": "tool_result", "tool_use_id": "sub-bash",
                "content": "README.md", "is_error": false}]}),
        ),
        subagent_message(
            "assistant",
            json!({"id": "msg-sub-2", "type": "message", "role": "assistant",
                "model": "subagent-model", "content": [{"type": "text", "text": "No issues"}]}),
        ),
        system_event(
            "task_updated",
            json!({"task_id": "t1", "run_id": "agent-run",
                "patch": {"status": "completed", "end_time": 1}}),
        ),
        system_event(
            "task_notification",
            json!({"task_id": "t1", "run_id": "agent-run", "tool_use_id": "agent-call",
                "status": "completed", "output_file": "/tmp/t1.output", "summary": "No issues"}),
        ),
        system_event("background_tasks_changed", json!({"tasks": []})),
        init(QUALIFICATION_VERSION, SESSION_ID),
    ]);
    values.extend(text_message("final-turn", &["final"], true));
    values.push(result(SESSION_ID));
    values
}

fn position_of(values: &[Value], predicate: impl Fn(&Value) -> bool) -> usize {
    values.iter().position(predicate).unwrap()
}

/// The index of the last `init`: the turn the drain wakes, before which a fixture splices the
/// drain it builds.
fn last_init(values: &[Value]) -> usize {
    values
        .iter()
        .rposition(|value| value["subtype"] == "init")
        .unwrap()
}

// REQ-AGENT-35, REQ-AGENT-64, REQ-AGENT-38
#[test]
fn backgrounded_subagent_activity_continues_after_the_exchange_and_its_notification_authorizes_the_wakeup()
 {
    let values = backgrounded_subagent_transcript();
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "final");
    let information = information_diagnostics(&observations);
    let [started, settled] = information.as_slice() else {
        panic!("start and settlement must each yield one diagnostic: {observations:?}");
    };
    assert!(started.contains("Explore") && started.contains("Review the diff"));
    assert!(settled.contains("Explore") && settled.contains("completed"));
    assert_eq!(unrecognized_events(&observations), Vec::<&Value>::new());
    assert!(tool_call_ids(&observations).contains(&String::from("sub-bash")));
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult { call_id, .. } if call_id.as_ref() == "sub-bash"
    )));
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::AssistantText { text } if text.as_ref() == "No issues"
    )));
}

/// One main-thread message calling `SendMessage` as `send-1`, addressed to the subagent
/// `t1`, followed by the task events Claude Code 2.1.310 emits when the addressee has
/// already finished: the subagent is resumed as a new run (`agent-run-2`) under the
/// message's tool call, announced before the call's own result returns.
fn send_message_to_finished_subagent() -> Vec<Value> {
    vec![
        message_start("send-turn"),
        stream_event(
            json!({"type":"content_block_start","index":0,"content_block":{
                "type":"tool_use","id":"send-1","name":"SendMessage","input":{}
            }}),
        ),
        stream_event(json!({"type":"content_block_delta","index":0,"delta":{
            "type":"input_json_delta","partial_json":"{\"to\":\"t1\",\"message\":\"Report once more\"}"
        }})),
        stream_event(json!({"type":"content_block_stop","index":0})),
        stream_event(
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}),
        ),
        stream_event(json!({"type":"message_stop"})),
        system_event(
            "background_tasks_changed",
            json!({"tasks": [{"task_id": "t1", "run_id": "agent-run-2", "task_type": "local_agent",
                "subagent_type": "Explore", "description": "Review the diff"}]}),
        ),
        subagent_started(json!({"run_id": "agent-run-2", "tool_use_id": "send-1",
            "is_backgrounded": true, "spawn_depth": 1, "prompt": "Report once more"})),
        json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"send-1",
                "content":[{"type":"text","text":"{\"success\":true,\"resumedAgentId\":\"t1\"}"}],
                "is_error":false}]}}),
    ]
}

/// The resumed run's activity and settlement: its message still names the Agent call as
/// its parent, while its `task_notification` names the resuming call and the new run.
fn resumed_subagent_settlement() -> Vec<Value> {
    vec![
        subagent_message(
            "assistant",
            json!({"id": "msg-sub-3", "type": "message", "role": "assistant",
                "model": "subagent-model", "content": [{"type": "text", "text": "Still no issues"}]}),
        ),
        system_event(
            "task_updated",
            json!({"task_id": "t1", "run_id": "agent-run-2",
                "patch": {"status": "completed", "end_time": 2}}),
        ),
        system_event(
            "task_notification",
            json!({"task_id": "t1", "run_id": "agent-run-2", "tool_use_id": "send-1",
                "status": "completed", "output_file": "/tmp/t1.output", "summary": "Still no issues"}),
        ),
        system_event("background_tasks_changed", json!({"tasks": []})),
    ]
}

/// Sanitized from the stream Claude Code 2.1.310 emits when the main thread, inside the
/// turn in which a backgrounded subagent's notification arrived, sends that subagent a
/// message: the resume's `task_started` repeats the subagent's `task_id`, and the resumed
/// run reports and notifies again after the main thread ends its turn to wait.
fn resumed_subagent_transcript() -> Vec<Value> {
    let base = backgrounded_subagent_transcript();
    let final_answer = position_of(&base, |value| {
        value["event"]["message"]["id"] == "final-answer"
    });
    let first_result = position_of(&base, |value| value["type"] == "result");
    let wakeup = last_init(&base);
    // The exchange that launched the subagent stays open through its settlement.
    let mut values = base[..final_answer].to_vec();
    values.extend_from_slice(&base[first_result + 1..wakeup]);
    values.extend(send_message_to_finished_subagent());
    values.extend(text_message("waiting-turn", &["waiting"], true));
    values.push(result(SESSION_ID));
    values.extend(resumed_subagent_settlement());
    values.extend_from_slice(&base[wakeup..]);
    values
}

/// The statuses the subagent `Review the diff` reported, in order.
fn subagent_statuses(observations: &[AgentObservation]) -> Vec<String> {
    test_support::subagent_statuses(observations, "Review the diff")
}

// REQ-AGENT-64, REQ-AGENT-35, REQ-AGENT-38
#[test]
fn messaging_a_finished_backgrounded_subagent_resumes_it_and_the_resumed_run_settles_the_step() {
    let values = resumed_subagent_transcript();
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "final");
    assert_eq!(
        subagent_statuses(&observations),
        ["started", "completed", "resumed", "completed"]
    );
    assert_eq!(unrecognized_events(&observations), Vec::<&Value>::new());
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::AssistantText { text } if text.as_ref() == "Still no issues"
    )));

    // The resumed run owes its own notification: a stream that ends without it, or a
    // wakeup it did not announce, fails as for any backgrounded subagent.
    let resumed_notification = position_of(&values, |value| {
        value["subtype"] == "task_notification" && value["run_id"] == "agent-run-2"
    });
    let (_, outcome) = replay(
        &framed(&values[..resumed_notification]),
        AgentValueKind::Response,
        1024,
    );
    assert_eq!(
        protocol_rejection_value(&outcome)["detail"]["reason"],
        "end_of_stream_invariant_invalid"
    );
}

// REQ-AGENT-69
#[test]
fn a_task_started_repeating_a_running_subagents_task_id_stays_a_protocol_failure() {
    let base = resumed_subagent_transcript();
    let first_notification = position_of(&base, |value| value["subtype"] == "task_notification");
    let resume = position_of(&base, |value| {
        value["subtype"] == "task_started" && value["run_id"] == "agent-run-2"
    });
    // The subagent has not notified when the repeat arrives.
    let mut running = base[..first_notification].to_vec();
    running.push(base[resume].clone());
    let (_, outcome) = replay(&framed(&running), AgentValueKind::Response, 1024);
    let rejection = protocol_rejection_value(&outcome);
    assert_eq!(rejection["detail"]["reason"], "system_event_invalid");
    assert_eq!(
        rejection["detail"]["state"]["outstandingBackgroundSubagents"],
        1
    );

    // A resume under a tool call another tracked subagent holds is a duplicate as before.
    let mut held = base.clone();
    held[resume]["tool_use_id"] = json!("agent-call");
    held.insert(
        resume,
        subagent_started(json!({"task_id": "t2", "tool_use_id": "agent-call-2",
            "run_id": "agent-run-3", "is_backgrounded": true})),
    );
    held[resume + 1]["tool_use_id"] = json!("agent-call-2");
    let (_, outcome) = replay(&framed(&held), AgentValueKind::Response, 1024);
    assert_eq!(
        protocol_rejection_value(&outcome)["detail"]["reason"],
        "system_event_invalid"
    );
}

/// The same message sent from the turn the subagent's notification woke: the wakeup's
/// `init` forgot the subagent as outstanding work, and the resumed run's messages still
/// name the Agent call that launched it.
fn resumed_after_wakeup_subagent_transcript() -> Vec<Value> {
    let base = backgrounded_subagent_transcript();
    let wakeup = last_init(&base);
    let mut values = base[..=wakeup].to_vec();
    values.extend(send_message_to_finished_subagent());
    values.extend(text_message("waiting-turn", &["waiting"], true));
    values.push(result(SESSION_ID));
    values.extend(resumed_subagent_settlement());
    values.extend_from_slice(&base[wakeup..]);
    values
}

// REQ-AGENT-64, REQ-AGENT-35
#[test]
fn messaging_a_finished_subagent_from_the_turn_its_notification_woke_resumes_it_too() {
    let values = resumed_after_wakeup_subagent_transcript();
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "final");
    assert_eq!(
        subagent_statuses(&observations),
        ["started", "completed", "resumed", "completed"]
    );
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::AssistantText { text } if text.as_ref() == "Still no issues"
    )));

    // The forgotten subagent authorizes no further wakeup: a second `init` before the
    // resumed run notifies is the protocol failure it always was.
    let resumed_notification = position_of(&values, |value| {
        value["subtype"] == "task_notification" && value["run_id"] == "agent-run-2"
    });
    let mut early_wakeup = values[..resumed_notification].to_vec();
    early_wakeup.push(init(QUALIFICATION_VERSION, SESSION_ID));
    let (_, outcome) = replay(&framed(&early_wakeup), AgentValueKind::Response, 1024);
    assert_eq!(
        protocol_rejection_value(&outcome)["detail"]["reason"],
        "initialization_invalid"
    );
}

/// The woken turn's message on 2.1.310 when the lead messages the finished subagent and
/// submits the value in one round, `[text, SendMessage, StructuredOutput]`: both calls run
/// inside the streamed message, the resume's frames and the `SendMessage` result ahead of the
/// structured output's acknowledgement, and the `result` carries the structured output while
/// the resumed run is outstanding (the capture UM-3111 records).
fn same_round_resume_and_submission() -> Vec<Value> {
    let submission = structured_output_exchange_after(&[
        json!({"type": "text", "text": "Messaging the subagent and submitting."}),
        json!({"type": "tool_use", "id": "send-1", "name": "SendMessage",
            "input": {"to": "t1", "message": "Report once more"}}),
    ]);
    let acknowledgement = position_of(&submission, |value| {
        value["message"]["content"][0]["tool_use_id"] == "tool-structured-output"
    });
    let resume = send_message_to_finished_subagent();
    let resume_frames = position_of(&resume, |value| {
        value["subtype"] == "background_tasks_changed"
    });
    let mut values = submission[1..acknowledgement].to_vec();
    values.extend_from_slice(&resume[resume_frames..]);
    values.extend_from_slice(&submission[acknowledgement..]);
    values
}

// REQ-AGENT-64, REQ-AGENT-69, REQ-AGENT-35 (UM-3111)
#[test]
fn a_subagents_frame_in_the_correction_window_is_its_work() {
    let base = backgrounded_subagent_transcript();
    let wakeup = last_init(&base);
    let mut values = base[..=wakeup].to_vec();
    values.extend(same_round_resume_and_submission());
    let settlement = resumed_subagent_settlement();
    let candidate = Some(CompletedResultExchange::Candidate(Arc::new(json!(7))));

    // The premature candidate is rejected and the adapter begins the correction turn; the
    // resumed run's first frame lands before that turn's `init` and is the subagent's work.
    let mut window = parser(AgentValueKind::Result, 1024);
    let mut observations = Vec::new();
    window
        .push_stdout(&framed(&values), |observation| {
            observations.push(observation)
        })
        .unwrap();
    assert_eq!(
        window.take_completed_result_exchange(),
        Some(CompletedResultExchange::PrematureCandidate)
    );
    window.reject_result_candidate().unwrap();
    window.begin_exchange().unwrap();
    window
        .push_stdout(&framed(&settlement[..1]), |observation| {
            observations.push(observation);
        })
        .unwrap();
    window
        .push_stdout(
            &framed(&text_turn("correction", "waiting")),
            |observation| {
                observations.push(observation);
            },
        )
        .unwrap();
    assert_eq!(window.take_completed_result_exchange(), None);
    window
        .push_stdout(&framed(&settlement[1..]), |observation| {
            observations.push(observation);
        })
        .unwrap();
    window
        .push_stdout(
            &framed(&structured_output_exchange_after(&[])),
            |observation| observations.push(observation),
        )
        .unwrap();
    assert_eq!(window.take_completed_result_exchange(), candidate);
    window
        .accept_result(CapturedJson::fixture(Arc::new(json!(7))))
        .unwrap();
    assert!(matches!(
        window.finish(true),
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));
    assert_eq!(
        subagent_statuses(&observations),
        ["started", "completed", "resumed", "completed"]
    );
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::AssistantText { text } if text.as_ref() == "Still no issues"
    )));
    // The resume's task list names the forgotten subagent, the one unrecognized frame.
    assert!(
        unrecognized_events(&observations)
            .iter()
            .all(|event| event["subtype"] == "background_tasks_changed")
    );

    // The main thread cannot speak before its `init`: its frame in the window stays the
    // protocol failure.
    let mut early = parser(AgentValueKind::Result, 1024);
    early.push_stdout(&framed(&values), drop).unwrap();
    early.take_completed_result_exchange();
    early.reject_result_candidate().unwrap();
    early.begin_exchange().unwrap();
    assert!(
        early
            .push_stdout(&framed(&[message_start("too-early")]), drop)
            .is_err()
    );
    let rejection = protocol_rejection_value(&early.finish(false));
    assert_eq!(
        rejection["detail"]["reason"],
        "exchange_initialization_missing"
    );
    assert_eq!(rejection["detail"]["state"]["eventThread"], "main");
    assert_eq!(rejection["detail"]["state"]["exchangeActive"], true);
}

// REQ-AGENT-69 (UM-3111)
#[test]
fn a_stopped_subagents_interruption_in_the_correction_window_is_its_work() {
    // The turn that launched the backgrounded subagent stops it on its next round beside the
    // answer and the value, `[text, TaskStop, StructuredOutput]`: the subagent settles
    // (`stopped`) before the stop's result, the candidate is ambiguous, and the subagent
    // thread's interruption lands after the `result`, in the correction window.
    let base = backgrounded_subagent_transcript();
    let launched = position_of(&base, |value| {
        value["message"]["content"][0]["tool_use_id"] == "agent-call"
    });
    let mut values = base[..=launched].to_vec();
    let stop = structured_output_exchange_after(&[
        json!({"type": "text", "text": "Stopping the subagent and submitting."}),
        json!({"type": "tool_use", "id": "stop-1", "name": "TaskStop",
            "input": {"task_id": "t1"}}),
    ]);
    let acknowledgement = position_of(&stop, |value| {
        value["message"]["content"][0]["tool_use_id"] == "tool-structured-output"
    });
    values.extend_from_slice(&stop[1..acknowledgement]);
    values.extend([
        system_event(
            "task_notification",
            json!({"task_id": "t1", "run_id": "agent-run", "tool_use_id": "agent-call",
                "status": "stopped", "output_file": "/tmp/t1.output", "summary": "Review the diff"}),
        ),
        json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"stop-1",
                "content":[{"type":"text","text":"{\"message\":\"Successfully stopped task: t1\"}"}],
                "is_error":false}]}}),
    ]);
    values.extend_from_slice(&stop[acknowledgement..]);
    let interruption = subagent_message(
        "user",
        json!({"role": "user", "content": [{"type": "text", "text": "[Request interrupted by user]"}]}),
    );

    let mut window = parser(AgentValueKind::Result, 1024);
    let mut observations = Vec::new();
    window
        .push_stdout(&framed(&values), |observation| {
            observations.push(observation)
        })
        .unwrap();
    assert_eq!(
        window.take_completed_result_exchange(),
        Some(CompletedResultExchange::AmbiguousCandidate)
    );
    window.reject_result_candidate().unwrap();
    window.begin_exchange().unwrap();
    window
        .push_stdout(&framed(&[interruption]), |observation| {
            observations.push(observation);
        })
        .unwrap();
    window
        .push_stdout(
            &framed(&structured_output_exchange_after(&[])),
            |observation| observations.push(observation),
        )
        .unwrap();
    assert!(matches!(
        window.take_completed_result_exchange(),
        Some(CompletedResultExchange::Candidate(candidate)) if *candidate == json!(7)
    ));
    window
        .accept_result(CapturedJson::fixture(Arc::new(json!(7))))
        .unwrap();
    assert!(matches!(
        window.finish(true),
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));
    assert_eq!(subagent_statuses(&observations), ["started", "stopped"]);
    // The interruption is the turn's unrecognized subagent frame, as when it wins the race.
    assert!(
        unrecognized_events(&observations)
            .iter()
            .any(|event| event["type"] == "user" && event["parent_tool_use_id"] == "agent-call")
    );
}

/// The backgrounded-subagent transcript with the launch registered in the foreground
/// (`is_backgrounded: false`), as Claude Code 2.1.310 may register an `Agent` call made with
/// `run_in_background`, and the move to the background announced only by `moved`: the
/// `task_updated` patch its schema names, or the `Agent` call's own result reporting an
/// asynchronous launch, which also stands alone when the harness announced no start.
fn registered_foreground_subagent_transcript(moved: SubagentMove) -> Vec<Value> {
    let mut values = backgrounded_subagent_transcript();
    let started = position_of(&values, |value| {
        value["subtype"] == "task_started" && value["task_id"] == "t1"
    });
    values[started]["is_backgrounded"] = json!(false);
    let tasks_changed = position_of(&values, |value| {
        value["subtype"] == "background_tasks_changed" && value["tasks"][0]["task_id"] == "t1"
    });
    let agent_result = position_of(&values, |value| {
        value["type"] == "user" && value["message"]["content"][0]["tool_use_id"] == "agent-call"
    });
    let async_launch = json!({
        "status": "async_launched", "isAsync": true, "agentId": "t1",
        "description": "Review the diff", "prompt": "Review the diff",
        "outputFile": "/tmp/t1.output",
    });
    match moved {
        SubagentMove::TaskUpdatedPatch => values.insert(
            tasks_changed + 1,
            system_event(
                "task_updated",
                json!({"task_id": "t1", "run_id": "agent-run",
                    "patch": {"is_backgrounded": true}}),
            ),
        ),
        SubagentMove::AsyncAgentResult => {
            values[agent_result]["tool_use_result"] = async_launch;
        }
        SubagentMove::AsyncAgentResultWithoutStart => {
            values[agent_result]["tool_use_result"] = async_launch;
            values.remove(tasks_changed);
            values.remove(started);
        }
    }
    values
}

#[derive(Clone, Copy, Debug)]
enum SubagentMove {
    TaskUpdatedPatch,
    AsyncAgentResult,
    /// No `task_started` at all: the result alone tracks the subagent, and its
    /// notification names the run.
    AsyncAgentResultWithoutStart,
}

// REQ-AGENT-35, REQ-AGENT-64
// UM-3077: a backgrounded builder registered in the foreground was forgotten by the
// exchange's `result`, so the Result-mode exchange produced `MissingCandidate`, the adapter
// closed stdin and the builder's notified turn began with an `init` nothing authorized.
#[test]
fn a_subagent_moved_to_the_background_after_its_start_is_background_work() {
    for moved in [
        SubagentMove::TaskUpdatedPatch,
        SubagentMove::AsyncAgentResult,
        SubagentMove::AsyncAgentResultWithoutStart,
    ] {
        let values = registered_foreground_subagent_transcript(moved);
        let first_result = position_of(&values, |value| value["type"] == "result");
        let notified_wakeup = last_init(&values);

        // Result mode: the exchange pauses on the moved subagent instead of reporting a
        // missing candidate, and the notified turn's candidate settles the step.
        let mut paused = parser(AgentValueKind::Result, 1024);
        paused
            .push_stdout(&framed(&values[..=first_result]), drop)
            .unwrap();
        assert_eq!(
            paused.take_completed_result_exchange(),
            None,
            "{moved:?}: the result must pause on the moved subagent"
        );
        paused
            .push_stdout(&framed(&values[first_result + 1..notified_wakeup]), drop)
            .unwrap();
        paused
            .push_stdout(&framed(&structured_output_exchange_after(&[])), drop)
            .unwrap();
        assert_eq!(
            paused.take_completed_result_exchange(),
            Some(CompletedResultExchange::Candidate(Arc::new(json!(7)))),
            "{moved:?}"
        );

        // Response mode: the drain accepts the subagent's activity and the notified turn
        // completes the step, with the start and settlement as diagnostics.
        let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
        assert_eq!(completed_response(&outcome), "final", "{moved:?}");
        let information = information_diagnostics(&observations);
        assert_eq!(information.len(), 2, "{moved:?}: {information:?}");
        assert!(information[0].contains("started") && information[1].contains("completed"));
        assert_eq!(
            unrecognized_events(&observations),
            Vec::<&Value>::new(),
            "{moved:?}"
        );
    }
}

// REQ-AGENT-74, REQ-AGENT-42
// UM-3077: the record of a refused `init` names the work the parser was waiting for.
#[test]
fn a_refused_init_records_the_tracked_work_the_parser_held() {
    // The first builder's notification wakes the lead, whose turn starts a backgrounded
    // command and a second backgrounded subagent and ends waiting; an `init` nothing
    // announced is then refused.
    let base = backgrounded_subagent_transcript();
    let notified_wakeup = last_init(&base);
    let mut values = base[..=notified_wakeup].to_vec();
    values.push(message_start("second-turn"));
    values.extend([
        stream_event(json!({"type":"content_block_start","index":0,"content_block":{
            "type":"tool_use","id":"bash-2","name":"Bash","input":{}
        }})),
        stream_event(json!({"type":"content_block_stop","index":0})),
        stream_event(json!({"type":"content_block_start","index":1,"content_block":{
            "type":"tool_use","id":"agent-2","name":"Agent","input":{}
        }})),
        stream_event(json!({"type":"content_block_stop","index":1})),
        stream_event(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}})),
        stream_event(json!({"type":"message_stop"})),
        bash_task_started("task-2", "bash-2", true),
        json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"bash-2",
                "content":[{"type":"text","text":"background"}],"is_error":false}]},
            "tool_use_result":{"backgroundTaskId":"task-2"}}),
        system_event(
            "task_started",
            json!({"task_id": "t2", "tool_use_id": "agent-2", "task_type": "local_agent",
                "run_id": "run-2", "is_backgrounded": true, "description": "Build slice 2",
                "subagent_type": "general-purpose"}),
        ),
        json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"agent-2",
                "content":[{"type":"text","text":"Async agent launched successfully."}],"is_error":false}]},
            "tool_use_result":{"status":"async_launched","agentId":"t2"}}),
    ]);
    values.extend(text_message("second-answer", &["waiting"], true));
    values.push(result(SESSION_ID));
    values.push(init(QUALIFICATION_VERSION, SESSION_ID));

    let (_, outcome) = replay(&framed(&values), AgentValueKind::Result, 1024);
    assert_eq!(
        protocol_rejection_value(&outcome)["detail"]["reason"],
        "initialization_invalid"
    );
    let AgentOutcome::Failed(failure) = &outcome else {
        panic!("the refused init must fail the invocation, got {outcome:?}");
    };
    let tracked = serde_json::to_value(failure.tracked_work().unwrap()).unwrap();
    assert_eq!(
        tracked,
        json!({
            "backgroundCommands": [{"taskId": "task-2", "toolUseId": "bash-2", "runId": "run-1",
                "backgrounded": true, "acknowledged": true, "notified": false, "monitor": false}],
            "subagents": [{"taskId": "t2", "agentCallId": "agent-2", "toolUseId": "agent-2",
                "runId": "run-2", "backgrounded": true, "notified": false}],
            "forgottenSubagents": ["t1"],
            "truncated": false,
        })
    );
}

// REQ-AGENT-74
// The record keeps the newest 16 entries of each list and says when a list was cut.
#[test]
fn the_recorded_tracked_work_keeps_the_newest_sixteen_of_each_list() {
    let base = backgrounded_subagent_transcript();
    let notified_wakeup = last_init(&base);
    let mut values = base[..=notified_wakeup].to_vec();
    values.push(message_start("second-turn"));
    let commands = 17;
    for index in 0..commands {
        values.extend([
            stream_event(
                json!({"type":"content_block_start","index":index,"content_block":{
                    "type":"tool_use","id":format!("bash-{index}"),"name":"Bash","input":{}
                }}),
            ),
            stream_event(json!({"type":"content_block_stop","index":index})),
        ]);
    }
    values.extend([
        stream_event(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}})),
        stream_event(json!({"type":"message_stop"})),
    ]);
    for index in 0..commands {
        let task_id = format!("task-{index}");
        let call_id = format!("bash-{index}");
        values.push(bash_task_started(&task_id, &call_id, true));
        values.push(bash_tool_result(&call_id, "background", Some(&task_id)));
    }
    values.extend(text_message("second-answer", &["waiting"], true));
    values.push(result(SESSION_ID));
    values.push(init(QUALIFICATION_VERSION, SESSION_ID));

    let (_, outcome) = replay(&framed(&values), AgentValueKind::Result, 1024);
    let AgentOutcome::Failed(failure) = &outcome else {
        panic!("the refused init must fail the invocation, got {outcome:?}");
    };
    let tracked = serde_json::to_value(failure.tracked_work().unwrap()).unwrap();
    let recorded = tracked["backgroundCommands"].as_array().unwrap();
    assert_eq!(recorded.len(), 16);
    assert_eq!(recorded[0]["taskId"], "task-1");
    assert_eq!(recorded[15]["taskId"], "task-16");
    assert_eq!(tracked["subagents"], json!([]));
    assert_eq!(tracked["truncated"], true);
}

/// A subagent's own Bash call moving to the background: its task start and the tool
/// result naming that task's `backgroundTaskId`, neither of which this invocation tracks.
fn subagent_backgrounded_bash_run() -> [Value; 2] {
    let mut tool_result = subagent_message(
        "user",
        json!({"role": "user", "content": [{"type": "tool_result", "tool_use_id": "sub-watch",
            "content": "Command running in background", "is_error": false}]}),
    );
    tool_result["tool_use_result"] = json!({"backgroundTaskId": "sub-watch-task"});
    [
        system_event(
            "task_started",
            json!({"task_id": "sub-watch-task", "run_id": "sub-watch-run",
                "owned_by_subagent": true, "tool_use_id": "sub-watch",
                "description": "Watch tests", "is_backgrounded": true,
                "parent_task_id": "t1", "task_type": "local_bash"}),
        ),
        tool_result,
    ]
}

// REQ-AGENT-64
#[test]
fn a_subagents_backgrounded_tool_result_is_its_own_work_in_the_exchange_and_the_drain() {
    let [started, tool_result] = subagent_backgrounded_bash_run();
    let foreground = agent_tool_exchange(&[
        subagent_started(json!({})),
        started.clone(),
        tool_result.clone(),
    ]);
    let (observations, outcome) = replay(&framed(&foreground), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "done");
    assert_eq!(unrecognized_events(&observations), Vec::<&Value>::new());

    let mut backgrounded = backgrounded_subagent_transcript();
    let first_subagent_message = position_of(&backgrounded, |value| value["type"] == "assistant");
    backgrounded.splice(
        first_subagent_message..first_subagent_message,
        [started, tool_result],
    );
    let (observations, outcome) = replay(&framed(&backgrounded), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "final");
    assert_eq!(unrecognized_events(&observations), Vec::<&Value>::new());
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult { call_id, .. } if call_id.as_ref() == "sub-watch"
    )));
}

// REQ-AGENT-69
#[test]
fn terminal_drain_refuses_work_no_outstanding_subagent_owns() {
    let base = backgrounded_subagent_transcript();
    let first_subagent_message = position_of(&base, |value| value["type"] == "assistant");

    let mut main_thread = base.clone();
    main_thread[first_subagent_message]["parent_tool_use_id"] = Value::Null;
    main_thread[first_subagent_message]["message"]["model"] = json!(MODEL);
    let mut unknown_parent = base.clone();
    unknown_parent[first_subagent_message]["parent_tool_use_id"] = json!("other-call");
    for transcript in [main_thread, unknown_parent] {
        let (_, outcome) = replay(&framed(&transcript), AgentValueKind::Response, 1024);
        let rejection = protocol_rejection_value(&outcome);
        assert_eq!(
            rejection["detail"]["reason"],
            "terminal_drain_event_invalid"
        );
        assert_eq!(rejection["detail"]["outerEvent"], "assistant");
    }

    // A turn the subagent's settlement did not announce, and a stream that ends with the
    // subagent still outstanding.
    let settlement = position_of(&base, |value| value["subtype"] == "task_updated");
    let mut unannounced = base[..settlement].to_vec();
    unannounced.push(init(QUALIFICATION_VERSION, SESSION_ID));
    unannounced.extend(text_message("final-turn", &["final"], true));
    unannounced.push(result(SESSION_ID));
    let (_, outcome) = replay(&framed(&unannounced), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);

    let (_, outcome) = replay(&framed(&base[..settlement]), AgentValueKind::Response, 1024);
    let rejection = protocol_rejection_value(&outcome);
    assert_eq!(
        rejection["detail"]["reason"],
        "end_of_stream_invariant_invalid"
    );

    // Once the subagent has settled, an unknown task's notification is a failure again.
    let wakeup = last_init(&base);
    let mut late = base[..wakeup].to_vec();
    late.push(system_event(
        "task_notification",
        json!({"task_id": "sub-task-2", "tool_use_id": "sub-bash-2", "status": "completed"}),
    ));
    let (_, outcome) = replay(&framed(&late), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);

    // A drain start naming a subagent neither tracked nor forgotten resumes nothing, and a
    // task list naming a command no subagent's start introduced names nothing owed.
    let unknown_start = subagent_started(json!({"task_id": "t9", "run_id": "agent-run-9",
        "tool_use_id": "agent-call-9", "is_backgrounded": true}));
    let unknown_command = system_event(
        "background_tasks_changed",
        json!({"tasks": [{"task_id": "other-task", "run_id": "other-run",
            "task_type": "local_bash", "description": "Watch tests"}]}),
    );
    for frame in [unknown_start, unknown_command] {
        let mut unowned = base[..wakeup].to_vec();
        unowned.push(frame);
        let (_, outcome) = replay(&framed(&unowned), AgentValueKind::Response, 1024);
        let rejection = protocol_rejection_value(&outcome);
        assert_eq!(
            rejection["detail"]["reason"],
            "terminal_drain_event_invalid"
        );
        assert_eq!(rejection["detail"]["outerEvent"], "system");
    }
}

fn command_lifecycle(state: &str) -> Value {
    json!({"type": "command_lifecycle", "command_uuid": "00000000-0000-4000-8000-0000000000c1",
        "state": state, "uuid": "00000000-0000-4000-8000-0000000000c2", "session_id": SESSION_ID})
}

fn text_turn(message_id: &str, text: &str) -> Vec<Value> {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(text_message(message_id, &[text], true));
    values.push(result(SESSION_ID));
    values
}

// REQ-AGENT-35, REQ-AGENT-69
#[test]
fn result_mode_pauses_on_outstanding_background_work_and_settles_on_the_wakeup_turn() {
    let base = backgrounded_subagent_transcript();
    let first_result = position_of(&base, |value| value["type"] == "result");
    let first_answer = position_of(&base, |value| {
        value["event"]["message"]["id"] == "final-answer"
    });
    let notified_wakeup = last_init(&base);
    let first_turn = framed(&base[..=first_result]);
    let drain = framed(&base[first_result + 1..notified_wakeup]);
    let submission = structured_output_exchange_after(&[]);
    let candidate = Some(CompletedResultExchange::Candidate(Arc::new(json!(7))));

    // The first turn ends without a candidate while the subagent works: the exchange
    // pauses with no decision, and the notification's turn submits the result.
    let mut paused = parser(AgentValueKind::Result, 1024);
    paused.push_stdout(&first_turn, drop).unwrap();
    assert_eq!(paused.take_completed_result_exchange(), None);
    paused.push_stdout(&drain, drop).unwrap();
    paused.push_stdout(&framed(&submission), drop).unwrap();
    assert_eq!(paused.take_completed_result_exchange(), candidate);
    paused
        .accept_result(CapturedJson::fixture(Arc::new(json!(7))))
        .unwrap();
    assert!(matches!(
        paused.finish(true),
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));

    // A candidate submitted while the subagent works is premature; rejecting it begins
    // the correction, whose turn without a candidate pauses again.
    let mut early = base[..first_answer].to_vec();
    early.extend_from_slice(&submission[1..]);
    let mut premature = parser(AgentValueKind::Result, 1024);
    premature.push_stdout(&framed(&early), drop).unwrap();
    assert_eq!(
        premature.take_completed_result_exchange(),
        Some(CompletedResultExchange::PrematureCandidate)
    );
    premature.reject_result_candidate().unwrap();
    premature.begin_exchange().unwrap();
    premature
        .push_stdout(&framed(&text_turn("correction", "waiting")), drop)
        .unwrap();
    assert_eq!(premature.take_completed_result_exchange(), None);
    premature.push_stdout(&drain, drop).unwrap();
    premature.push_stdout(&framed(&submission), drop).unwrap();
    assert_eq!(premature.take_completed_result_exchange(), candidate);

    // A wakeup the agent scheduled begins a turn while the subagent works; that turn
    // pauses too, and the wakeup's completion is recognized.
    let mut wakeup_turn = vec![command_lifecycle("started")];
    wakeup_turn.extend(text_turn("wakeup", "still waiting"));
    wakeup_turn.push(command_lifecycle("completed"));
    let mut woken = parser(AgentValueKind::Result, 1024);
    let mut observations = Vec::new();
    for bytes in [&first_turn, &framed(&wakeup_turn), &drain] {
        woken
            .push_stdout(bytes, |observation| observations.push(observation))
            .unwrap();
        assert_eq!(woken.take_completed_result_exchange(), None);
    }
    woken
        .push_stdout(&framed(&submission), |observation| {
            observations.push(observation);
        })
        .unwrap();
    assert_eq!(woken.take_completed_result_exchange(), candidate);
    assert_eq!(unrecognized_events(&observations), Vec::<&Value>::new());
    let information = information_diagnostics(&observations);
    assert_eq!(information.len(), 3, "{information:?}");
    assert!(information[1].contains("wakeup"));

    // With nothing outstanding a scheduled wakeup announces no turn: the late turn opens
    // after the value settled, so it is an epilogue and the settled response stands
    // (REQ-AGENT-76).
    let lifecycle = command_lifecycle("started");
    let mut idle = text_turn("only", "only");
    idle.push(lifecycle.clone());
    idle.extend(text_turn("unannounced", "late"));
    let (observations, outcome) = replay(&framed(&idle), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "only");
    assert_eq!(
        milestone_count(&observations, AgentLifecycleMilestone::TurnCompleted),
        2
    );
    assert_eq!(unrecognized_events(&observations), [&lifecycle]);

    // A native terminal failure explains a stream that ends with the subagent working.
    let mut failing = base[..first_result].to_vec();
    failing.push(
        json!({"type": "result", "subtype": "error_during_execution",
        "is_error": true, "terminal_reason": "error_during_execution", "session_id": SESSION_ID}),
    );
    let mut native = parser(AgentValueKind::Result, 1024);
    native.push_stdout(&framed(&failing), drop).unwrap();
    assert_eq!(
        native.take_completed_result_exchange(),
        Some(CompletedResultExchange::NativeFailure)
    );
    assert_failed(
        native.finish(true),
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::Classified {
                canonical: CanonicalHarnessFailure::ModelError,
                diagnostic: HarnessFailureDiagnostic::from_code("other", None),
            },
        },
    );
}

/// One exchange whose Agent call starts a backgrounded subagent that settles inside the
/// exchange, before the main thread's final message `done`, followed by the further turn
/// Claude Code 2.1.310 begins to deliver the queued `<task-notification>` once the
/// exchange has ended: an empty task list, an `init`, the text `noted` and a `result`.
/// The drain turn is `epilogue`; the frames before it are the first exchange.
fn subagent_settled_in_the_exchange_transcript(epilogue: &[Value]) -> Vec<Value> {
    let mut values = agent_tool_exchange(&[
        subagent_started(json!({"run_id": "agent-run", "is_backgrounded": true,
            "spawn_depth": 1, "prompt": "Review the diff"})),
        system_event(
            "task_updated",
            json!({"task_id": "t1", "run_id": "agent-run",
                "patch": {"status": "completed", "end_time": 1}}),
        ),
        system_event(
            "task_notification",
            json!({"task_id": "t1", "run_id": "agent-run", "tool_use_id": "agent-call",
                "status": "completed", "output_file": "/tmp/t1.output", "summary": "No issues"}),
        ),
    ]);
    values.push(system_event(
        "background_tasks_changed",
        json!({"tasks": []}),
    ));
    values.extend_from_slice(epilogue);
    values
}

fn epilogue_turn() -> Vec<Value> {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(text_message("epilogue", &["noted"], true));
    values.push(result(SESSION_ID));
    values
}

fn milestone_count(observations: &[AgentObservation], milestone: AgentLifecycleMilestone) -> usize {
    observations
        .iter()
        .filter(|observation| **observation == lifecycle(milestone))
        .count()
}

fn assert_epilogue_observed(observations: &[AgentObservation], turns: usize) {
    assert_eq!(unrecognized_events(observations), Vec::<&Value>::new());
    assert!(
        observations.iter().any(|observation| matches!(
            observation,
            AgentObservation::AssistantText { text } if text.as_ref() == "noted"
        )),
        "the epilogue's text must appear as activity: {observations:?}"
    );
    for milestone in [
        AgentLifecycleMilestone::TurnStarted,
        AgentLifecycleMilestone::TurnCompleted,
    ] {
        assert_eq!(
            milestone_count(observations, milestone),
            turns,
            "{milestone:?}"
        );
    }
}

// REQ-AGENT-76, REQ-AGENT-69, REQ-AGENT-35
#[test]
fn a_turn_a_settled_subagent_announced_after_the_value_settled_is_an_epilogue() {
    let transcript = subagent_settled_in_the_exchange_transcript(&epilogue_turn());
    for kind in [AgentValueKind::Response, AgentValueKind::None] {
        let (observations, outcome) = replay(&framed(&transcript), kind, 1024);
        match kind {
            AgentValueKind::Response => assert_eq!(completed_response(&outcome), "done"),
            _ => assert!(
                matches!(
                    outcome,
                    AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
                ),
                "{outcome:?}"
            ),
        }
        assert_epilogue_observed(&observations, 2);
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::HarnessCompleted),
            1
        );
    }

    // In Result mode the accepted candidate is the value; the epilogue's own submission
    // is no candidate, and the settlement grace waits while the epilogue is open.
    let final_answer = position_of(&transcript, |value| {
        value["event"]["message"]["id"] == "final-answer"
    });
    let mut first_turn = transcript[..final_answer].to_vec();
    first_turn.extend_from_slice(&structured_output_exchange_after(&[])[1..]);
    let mut epilogue = vec![system_event(
        "background_tasks_changed",
        json!({"tasks": []}),
    )];
    epilogue.push(init(QUALIFICATION_VERSION, SESSION_ID));
    epilogue.extend(text_message("epilogue", &["noted"], true));
    let resubmission = structured_output_exchange_with(&json!({"result": 8}), &[]);
    let mut observations = Vec::new();
    let mut parser = parser(AgentValueKind::Result, 1024);
    parser
        .push_stdout(&framed(&first_turn), |observation| {
            observations.push(observation);
        })
        .unwrap();
    assert_eq!(
        parser.take_completed_result_exchange(),
        Some(CompletedResultExchange::Candidate(Arc::new(json!(7))))
    );
    parser
        .accept_result(CapturedJson::fixture(Arc::new(json!(7))))
        .unwrap();
    assert!(parser.settling());
    parser
        .push_stdout(&framed(&epilogue), |observation| {
            observations.push(observation);
        })
        .unwrap();
    assert!(!parser.settling(), "an open epilogue suspends the grace");
    parser
        .push_stdout(&framed(&resubmission[1..]), |observation| {
            observations.push(observation);
        })
        .unwrap();
    assert_eq!(parser.take_completed_result_exchange(), None);
    assert!(parser.settling());
    assert_epilogue_observed(&observations, 2);
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = parser.finish(true)
    else {
        panic!("the accepted result must stand");
    };
    assert_eq!(result.value(), &json!(7));

    // A native failure the epilogue reports is a warning; the settled value stands.
    let mut failing = epilogue_turn();
    *failing.last_mut().unwrap() = json!({"type": "result", "subtype": "error_during_execution",
        "is_error": true, "terminal_reason": "rate_limit", "session_id": SESSION_ID});
    let transcript = subagent_settled_in_the_exchange_transcript(&failing);
    let (observations, outcome) = replay(&framed(&transcript), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "done");
    assert!(
        observations.iter().any(|observation| matches!(
            observation,
            AgentObservation::Diagnostic { level: AgentDiagnosticLevel::Warning, message }
                if message.contains("rate_limited")
        )),
        "{observations:?}"
    );
}

/// Sanitized from the stream Claude Code 2.1.310 emits when a backgrounded subagent starts
/// its own `run_in_background` Bash and closes its turn without awaiting it. The drain
/// before the wakeup `init` names only the leftover command in the task list. After the
/// main thread's final `result`, the leftover's completion is reported, Claude Code resumes
/// the finished subagent on its own to deliver that report (no `SendMessage` call; the
/// resume's `prompt` is the command's `<task-notification>`), the resumed run settles, and
/// a further main-thread turn (`noted`) begins and ends before the process exits.
fn backgrounded_subagent_leaving_its_command_running_transcript() -> Vec<Value> {
    let mut values = backgrounded_subagent_transcript();
    let closing_text = position_of(&values, |value| value["message"]["id"] == "msg-sub-2");
    values.splice(closing_text..closing_text, subagent_backgrounded_bash_run());
    let wakeup = last_init(&values);
    values[wakeup - 1] = system_event(
        "background_tasks_changed",
        json!({"tasks": [{"task_id": "sub-watch-task", "run_id": "sub-watch-run",
            "task_type": "local_bash", "description": "Watch tests", "parent_task_id": "t1"}]}),
    );
    values.extend(leftover_command_report());
    values.push(system_event(
        "background_tasks_changed",
        json!({"tasks": []}),
    ));
    values.extend(harness_resume_delivering_the_report("agent-run-2"));
    values.extend(epilogue_turn());
    values
}

/// The completion Claude Code reports for the command the subagent left running
/// (`sub-watch-task`, from `subagent_backgrounded_bash_run`): silent as the subagent's work.
fn leftover_command_report() -> [Value; 2] {
    [
        system_event(
            "task_updated",
            json!({"task_id": "sub-watch-task", "run_id": "sub-watch-run",
                "patch": {"status": "completed", "end_time": 2}}),
        ),
        system_event(
            "task_notification",
            json!({"task_id": "sub-watch-task", "run_id": "sub-watch-run",
                "tool_use_id": "sub-watch", "status": "completed",
                "output_file": "/tmp/sub-watch-task.output",
                "summary": "Background command \"Watch tests\" completed (exit code 0)"}),
        ),
    ]
}

/// The resume Claude Code 2.1.310 begins on its own, in the drain, to deliver the leftover
/// command's report to the finished subagent: the announcement naming the subagent with the
/// new `run_id`, the `task_started` whose `prompt` is the report (no `SendMessage` call),
/// the resumed run's text, and its settlement under the new run.
fn harness_resume_delivering_the_report(run_id: &str) -> Vec<Value> {
    vec![
        system_event(
            "background_tasks_changed",
            json!({"tasks": [{"task_id": "t1", "run_id": run_id, "task_type": "local_agent",
                "subagent_type": "Explore", "description": "Review the diff"}]}),
        ),
        subagent_started(json!({"run_id": run_id, "is_backgrounded": true,
            "spawn_depth": 1, "prompt": "<task-notification>\n<task-id>sub-watch-task</task-id>\n\
            <tool-use-id>sub-watch</tool-use-id>\n\
            <output-file>/tmp/sub-watch-task.output</output-file>\n<status>completed</status>\n\
            <summary>Background command \"Watch tests\" completed (exit code 0)</summary>\n\
            </task-notification>"})),
        subagent_message(
            "assistant",
            json!({"id": "msg-sub-report", "type": "message", "role": "assistant",
                "model": "subagent-model",
                "content": [{"type": "text", "text": "The tests passed"}]}),
        ),
        system_event(
            "task_updated",
            json!({"task_id": "t1", "run_id": run_id,
                "patch": {"status": "completed", "end_time": 3}}),
        ),
        system_event(
            "task_notification",
            json!({"task_id": "t1", "run_id": run_id, "tool_use_id": "agent-call",
                "status": "completed", "output_file": "/tmp/t1.output", "summary": "No issues",
                "usage": {"duration_ms": 210, "tool_uses": 1, "total_tokens": 7}}),
        ),
        system_event("background_tasks_changed", json!({"tasks": []})),
    ]
}

/// Sanitized from the stream Claude Code 2.1.310 emits when the main thread messages
/// (`SendMessage`) a finished backgrounded subagent that had left its own `run_in_background`
/// Bash running: the leftover's report arrives while the messaged run is outstanding, the
/// messaged run settles in the drain, and before any `init` Claude Code resumes the subagent
/// once more to deliver that report, announced by a `background_tasks_changed` naming a
/// subagent the parser still tracks as settled (no wakeup `init` has forgotten it). The
/// second resumed run's notification wakes the final turn.
fn messaged_subagent_resumed_again_for_its_commands_report_transcript() -> Vec<Value> {
    let mut drain = harness_resume_delivering_the_report("agent-run-3");
    drain.extend(empty_notification_turn());
    messaged_subagent_resumed_again_with_drain(drain)
}

/// As `messaged_subagent_resumed_again_for_its_commands_report_transcript`, with the two
/// turns Claude Code opens for the subagent's two queued notifications in the other order:
/// the notified turn first, then the empty turn for the superseded notification after the
/// notified turn's `result`. Inferred from the merge-queue build #3433 rejection record
/// (AUTO-54), whose every field fits this order; the capture is gone.
fn messaged_subagent_notified_before_its_superseded_notifications_empty_turn_transcript()
-> Vec<Value> {
    let mut values = messaged_subagent_resumed_again_with_drain(
        harness_resume_delivering_the_report("agent-run-3"),
    );
    values.extend(empty_notification_turn());
    values
}

/// `resumed_subagent_transcript` with the messaged subagent's leftover command and its
/// report, and `drain` spliced before the final `init`.
fn messaged_subagent_resumed_again_with_drain(drain: Vec<Value>) -> Vec<Value> {
    let mut values = resumed_subagent_transcript();
    let closing_text = position_of(&values, |value| value["message"]["id"] == "msg-sub-2");
    values.splice(closing_text..closing_text, subagent_backgrounded_bash_run());
    let send_result = position_of(&values, |value| value["type"] == "result");
    values.splice(send_result + 1..send_result + 1, leftover_command_report());
    let wakeup = last_init(&values);
    values.splice(wakeup..wakeup, drain);
    values
}

/// The turn Claude Code 2.1.310 opens for the messaged run's queued notification, which
/// its own resume of the subagent superseded, and closes without a model call: an `init`
/// and a `result` with `num_turns: 0`, an empty `result` text and no `terminal_reason`.
/// The announcement stands for the next `init`, which delivers the resumed run's.
fn empty_notification_turn() -> [Value; 2] {
    [
        init(QUALIFICATION_VERSION, SESSION_ID),
        json!({"type": "result", "subtype": "success", "is_error": false, "num_turns": 0,
            "stop_reason": null, "result": "", "duration_api_ms": 0, "session_id": SESSION_ID,
            "origin": {"kind": "task-notification", "producer": "session-task",
                "runId": "agent-run-2"}}),
    ]
}

// REQ-AGENT-64, REQ-AGENT-35, REQ-AGENT-69
#[test]
fn a_messaged_subagent_resumed_again_in_the_drain_for_its_commands_report_stays_its_work() {
    let values = messaged_subagent_resumed_again_for_its_commands_report_transcript();
    for kind in [AgentValueKind::Response, AgentValueKind::None] {
        let (observations, outcome) = replay(&framed(&values), kind, 1024);
        match kind {
            AgentValueKind::Response => assert_eq!(completed_response(&outcome), "final"),
            _ => assert!(
                matches!(
                    outcome,
                    AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
                ),
                "{outcome:?}"
            ),
        }
        assert_eq!(
            subagent_statuses(&observations),
            [
                "started",
                "completed",
                "resumed",
                "completed",
                "resumed",
                "completed"
            ]
        );
        assert_eq!(unrecognized_events(&observations), Vec::<&Value>::new());
        assert!(observations.iter().any(|observation| matches!(
            observation,
            AgentObservation::AssistantText { text } if text.as_ref() == "The tests passed"
        )));
        // The launching turn, the empty turn and the notified turn.
        for milestone in [
            AgentLifecycleMilestone::TurnStarted,
            AgentLifecycleMilestone::TurnCompleted,
        ] {
            assert_eq!(
                milestone_count(&observations, milestone),
                3,
                "{milestone:?}"
            );
        }
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::HarnessCompleted),
            1
        );
    }

    // The empty turn settles nothing: a stream that ends on it never delivered the
    // notification whose announcement it consumed.
    let final_wakeup = last_init(&values);
    let (_, outcome) = replay(
        &framed(&values[..final_wakeup]),
        AgentValueKind::Response,
        1024,
    );
    assert_eq!(
        protocol_rejection_value(&outcome)["detail"]["reason"],
        "end_of_stream_invariant_invalid"
    );

    // The second resumed run owes its notification as the first did: a wakeup before it
    // is unannounced.
    let second_notification = position_of(&values, |value| {
        value["subtype"] == "task_notification" && value["run_id"] == "agent-run-3"
    });
    let mut early_wakeup = values[..second_notification].to_vec();
    early_wakeup.push(init(QUALIFICATION_VERSION, SESSION_ID));
    let (_, outcome) = replay(&framed(&early_wakeup), AgentValueKind::Response, 1024);
    assert_eq!(
        protocol_rejection_value(&outcome)["detail"]["reason"],
        "initialization_invalid"
    );
}

// REQ-AGENT-76, REQ-AGENT-69, REQ-AGENT-64
#[test]
fn a_superseded_notifications_empty_turn_after_the_notified_turn_is_an_epilogue() {
    let values =
        messaged_subagent_notified_before_its_superseded_notifications_empty_turn_transcript();
    for kind in [AgentValueKind::Response, AgentValueKind::None] {
        let (observations, outcome) = replay(&framed(&values), kind, 1024);
        match kind {
            AgentValueKind::Response => assert_eq!(completed_response(&outcome), "final"),
            _ => assert!(
                matches!(
                    outcome,
                    AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
                ),
                "{outcome:?}"
            ),
        }
        assert_eq!(
            subagent_statuses(&observations),
            [
                "started",
                "completed",
                "resumed",
                "completed",
                "resumed",
                "completed"
            ]
        );
        assert_eq!(unrecognized_events(&observations), Vec::<&Value>::new());
        // The launching turn, the notified turn and the empty turn.
        for milestone in [
            AgentLifecycleMilestone::TurnStarted,
            AgentLifecycleMilestone::TurnCompleted,
        ] {
            assert_eq!(
                milestone_count(&observations, milestone),
                3,
                "{milestone:?}"
            );
        }
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::HarnessCompleted),
            1
        );
    }
}

// REQ-AGENT-76, REQ-AGENT-69, REQ-AGENT-64
#[test]
fn a_superseded_notifications_empty_turn_after_the_accepted_result_is_an_epilogue() {
    let values =
        messaged_subagent_notified_before_its_superseded_notifications_empty_turn_transcript();
    // The notified turn's `init`, the last before the empty turn's.
    let empty_turn = values.len() - empty_notification_turn().len();
    let notified_wakeup = last_init(&values[..empty_turn]);
    let mut notified_turn = values[..=notified_wakeup].to_vec();
    notified_turn.extend_from_slice(&structured_output_exchange_after(&[])[1..]);
    let mut parser = parser(AgentValueKind::Result, 1024);
    parser.push_stdout(&framed(&notified_turn), drop).unwrap();
    assert_eq!(
        parser.take_completed_result_exchange(),
        Some(CompletedResultExchange::Candidate(Arc::new(json!(7))))
    );
    parser
        .accept_result(CapturedJson::fixture(Arc::new(json!(7))))
        .unwrap();
    assert!(parser.settling());
    // The empty turn after settlement leaves no announcement standing: the grace runs on.
    parser
        .push_stdout(&framed(&empty_notification_turn()), drop)
        .unwrap();
    assert!(parser.settling());
    assert_eq!(parser.take_completed_result_exchange(), None);
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = parser.finish(true)
    else {
        panic!("the accepted result must stand");
    };
    assert_eq!(result.value(), &json!(7));
}

/// Captured from Claude Code 2.1.310 with a `UserPromptSubmit` hook that exits 2: the first
/// turn ends with no model call and a `result` of the empty turn's shape (`num_turns: 0`,
/// `is_error: false`, `subtype: success`, no `terminal_reason`) whose `result` text is the
/// hook's refusal. Nothing announced the turn, so it is no empty turn: the step fails as it
/// did before empty turns were drained (conformance `06d`).
// REQ-AGENT-69
#[test]
fn a_first_turn_a_hook_blocked_is_a_protocol_failure_not_an_empty_turn() {
    let values = [
        init(QUALIFICATION_VERSION, SESSION_ID),
        system_event(
            "informational",
            json!({"content": "UserPromptSubmit operation blocked by hook", "level": "warning",
                "prevent_continuation": true,
                "prompt_submit_outcome": {"kind": "blocked_by_hook", "prompt_kept": false}}),
        ),
        json!({"type": "result", "subtype": "success", "is_error": false, "num_turns": 0,
            "stop_reason": null, "duration_api_ms": 0, "session_id": SESSION_ID,
            "result": "UserPromptSubmit operation blocked by hook"}),
    ];
    for kind in [
        AgentValueKind::None,
        AgentValueKind::Response,
        AgentValueKind::Result,
    ] {
        let (observations, outcome) = replay(&framed(&values), kind, 1024);
        let rejection = protocol_rejection_value(&outcome);
        assert_eq!(
            rejection["detail"]["reason"], "result_correlation_invalid",
            "{kind:?}: {rejection}"
        );
        assert_eq!(rejection["detail"]["state"]["completedExchanges"], 0);
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::TurnCompleted),
            0,
            "{kind:?}: the blocked turn completes nothing"
        );
    }
}

fn assert_subagent_resumed_to_deliver_its_commands_report(observations: &[AgentObservation]) {
    assert_eq!(
        information_diagnostics(observations),
        [
            "subagent Explore started · Review the diff",
            "subagent Explore completed · Review the diff",
            "subagent Explore resumed · Review the diff",
            "subagent Explore completed · Review the diff",
        ]
    );
    assert_epilogue_observed(observations, 3);
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::AssistantText { text } if text.as_ref() == "The tests passed"
    )));
}

// REQ-AGENT-64, REQ-AGENT-73, REQ-AGENT-76
#[test]
fn a_backgrounded_subagents_command_outliving_it_stays_its_work_and_its_report_opens_an_epilogue() {
    let transcript = backgrounded_subagent_leaving_its_command_running_transcript();
    for kind in [AgentValueKind::Response, AgentValueKind::None] {
        let (observations, outcome) = replay(&framed(&transcript), kind, 1024);
        match kind {
            AgentValueKind::Response => assert_eq!(completed_response(&outcome), "final"),
            _ => assert!(
                matches!(
                    outcome,
                    AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
                ),
                "{outcome:?}"
            ),
        }
        assert_subagent_resumed_to_deliver_its_commands_report(&observations);
        assert_eq!(
            milestone_count(&observations, AgentLifecycleMilestone::HarnessCompleted),
            1
        );
    }

    // In Result mode the notified turn submits the value; the epilogue's submission is
    // no candidate.
    let first_result = position_of(&transcript, |value| value["type"] == "result");
    let after_first_result = &transcript[first_result + 1..];
    let wakeup =
        first_result + 1 + position_of(after_first_result, |value| value["subtype"] == "init");
    let notified_result =
        first_result + 1 + position_of(after_first_result, |value| value["type"] == "result");
    let epilogue = last_init(&transcript);
    let mut observations = Vec::new();
    let mut parser = parser(AgentValueKind::Result, 1024);
    for frames in [
        &transcript[..=first_result],
        &transcript[first_result + 1..wakeup],
    ] {
        parser
            .push_stdout(&framed(frames), |observation| {
                observations.push(observation)
            })
            .unwrap();
        assert_eq!(parser.take_completed_result_exchange(), None);
    }
    parser
        .push_stdout(
            &framed(&structured_output_exchange_after(&[])),
            |observation| {
                observations.push(observation);
            },
        )
        .unwrap();
    assert_eq!(
        parser.take_completed_result_exchange(),
        Some(CompletedResultExchange::Candidate(Arc::new(json!(7))))
    );
    parser
        .accept_result(CapturedJson::fixture(Arc::new(json!(7))))
        .unwrap();
    let mut tail = transcript[notified_result + 1..=epilogue].to_vec();
    tail.extend(text_message("epilogue", &["noted"], true));
    tail.extend_from_slice(&structured_output_exchange_with(&json!({"result": 8}), &[])[1..]);
    parser
        .push_stdout(&framed(&tail), |observation| observations.push(observation))
        .unwrap();
    assert_eq!(parser.take_completed_result_exchange(), None);
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = parser.finish(true)
    else {
        panic!("the accepted result must stand");
    };
    assert_eq!(result.value(), &json!(7));
    assert_subagent_resumed_to_deliver_its_commands_report(&observations);
}

// REQ-AGENT-75, REQ-AGENT-45
// INV-HARNESS-5
#[test]
fn a_turn_begun_after_a_native_failure_ends_the_stream_with_the_native_failure() {
    let base = backgrounded_subagent_transcript();
    let first_result = position_of(&base, |value| value["type"] == "result");
    let notified_init = last_init(&base);
    let mut failing = base[..first_result].to_vec();
    failing.push(
        json!({"type": "result", "subtype": "error_during_execution",
        "is_error": true, "terminal_reason": "rate_limit", "session_id": SESSION_ID}),
    );
    let rate_limited = AgentFailureCause::HarnessFailed {
        detail: AgentHarnessFailureDetail::Classified {
            canonical: CanonicalHarnessFailure::ModelError,
            diagnostic: HarnessFailureDiagnostic::from_code("rate_limit", None),
        },
    };

    // Result mode: the adapter takes the native failure and closes stdin, the subagent's
    // drain goes on, and the turn its notification begins ends the stream with the failure
    // the result reported, not with a rejection of the init and its record.
    let mut notified = parser(AgentValueKind::Result, 1024);
    notified.push_stdout(&framed(&failing), drop).unwrap();
    assert_eq!(
        notified.take_completed_result_exchange(),
        Some(CompletedResultExchange::NativeFailure)
    );
    notified
        .push_stdout(&framed(&base[first_result + 1..notified_init]), drop)
        .unwrap();
    assert_eq!(
        notified.push_stdout(&framed(&base[notified_init..=notified_init]), drop),
        Err(rate_limited.clone())
    );
    let AgentOutcome::Failed(failure) = notified.finish(false) else {
        panic!("expected the native failure");
    };
    assert_eq!(failure.cause(), &rate_limited);
    assert!(failure.protocol_rejection().is_none());

    // Response mode keeps stdin open after the failure; a main-thread message in the drain,
    // which the parser would otherwise refuse, ends the stream the same way.
    let mut unexplained = failing;
    unexplained.extend(text_message("after-failure", &["late"], true));
    let (_, outcome) = replay(&framed(&unexplained), AgentValueKind::Response, 1024);
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected the native failure");
    };
    assert_eq!(failure.cause(), &rate_limited);
    assert!(failure.protocol_rejection().is_none());
}

// REQ-AGENT-38
#[test]
fn each_unrecognized_event_kind_is_observed_once_per_invocation() {
    let hook = system_event("hook_started", json!({"hook_id": "h1"}));
    let values = agent_tool_exchange(&[
        hook.clone(),
        system_event("hook_started", json!({"hook_id": "h2"})),
        system_event("hook_response", json!({"hook_id": "h1"})),
        json!({"type": "future_additive_event", "session_id": SESSION_ID}),
        json!({"type": "future_additive_event", "session_id": SESSION_ID}),
    ]);
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "done");
    let kinds = unrecognized_events(&observations)
        .into_iter()
        .map(|event| (event["type"].as_str(), event["subtype"].as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            (Some("system"), Some("hook_started")),
            (Some("system"), Some("hook_response")),
            (Some("future_additive_event"), None),
        ]
    );
    assert_eq!(unrecognized_events(&observations)[0], &hook);
}

fn background_first_turn() -> Vec<Value> {
    let mut values = vec![
        init(QUALIFICATION_VERSION, SESSION_ID),
        message_start("tool-turn"),
    ];
    values.extend([
        stream_event(json!({"type":"content_block_start","index":0,"content_block":{
            "type":"tool_use","id":"bash-call","name":"Bash","input":{}
        }})),
        stream_event(json!({"type":"content_block_delta","index":0,"delta":{
            "type":"input_json_delta","partial_json":"{}"
        }})),
        stream_event(json!({"type":"content_block_stop","index":0})),
        stream_event(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}})),
        stream_event(json!({"type":"message_stop"})),
        json!({"type":"system","subtype":"task_started","session_id":SESSION_ID,
            "task_id":"task-1","task_type":"local_bash","tool_use_id":"bash-call","is_backgrounded":true}),
        json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"bash-call",
                "content":[{"type":"text","text":"background"}],"is_error":false}]},
            "tool_use_result":{"backgroundTaskId":"task-1"}}),
    ]);
    values.extend(text_message("first-answer", &["intermediate"], true));
    values.push(result(SESSION_ID));
    values
}

fn background_notification() -> Value {
    json!({"type":"system","subtype":"task_notification","session_id":SESSION_ID,
        "task_id":"task-1","tool_use_id":"bash-call","status":"completed"})
}

// REQ-AGENT-35
#[test]
fn correlated_background_continuation_keeps_only_the_last_response() {
    let mut values = background_first_turn();
    values.push(background_notification());
    values.push(init(QUALIFICATION_VERSION, SESSION_ID));
    values.extend(text_message("final-answer", &["final"], true));
    values.push(result(SESSION_ID));
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("correlated native continuation must settle: {outcome:?}");
    };
    assert_eq!(response.as_str(), "final");
}

#[test]
fn background_completion_during_foreground_work_does_not_require_another_init() {
    let mut values = background_first_turn();
    let notification = background_notification();
    // Native may report completion while the first exchange is still active.
    let final_message = values
        .iter()
        .position(|value| value["event"]["message"]["id"] == "first-answer")
        .unwrap();
    values.insert(final_message, notification);
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("settled background work must not require a wakeup: {outcome:?}");
    };
    assert_eq!(response.as_str(), "intermediate");
}

fn bash_call_message(message_id: &str, call_id: &str) -> Vec<Value> {
    tool_call_message(message_id, call_id, "Bash", &json!({}))
}

/// A main-thread message opening with one `tool_use` block named `name`, whose input
/// streams as one `input_json_delta`; the message's end is left to the caller.
fn tool_call_message(message_id: &str, call_id: &str, name: &str, input: &Value) -> Vec<Value> {
    vec![
        message_start(message_id),
        stream_event(
            json!({"type":"content_block_start","index":0,"content_block":{
                "type":"tool_use","id":call_id,"name":name,"input":{}
            }}),
        ),
        stream_event(json!({"type":"content_block_delta","index":0,"delta":{
            "type":"input_json_delta","partial_json":input.to_string()
        }})),
        stream_event(json!({"type":"content_block_stop","index":0})),
    ]
}

fn message_end(stop_reason: &str) -> Vec<Value> {
    vec![
        stream_event(
            json!({"type":"message_delta","delta":{"stop_reason":stop_reason},"usage":{"output_tokens":1}}),
        ),
        stream_event(json!({"type":"message_stop"})),
    ]
}

fn bash_task_started(task_id: &str, call_id: &str, backgrounded: bool) -> Value {
    json!({"type":"system","subtype":"task_started","session_id":SESSION_ID,
        "task_id":task_id,"run_id":"run-1","task_type":"local_bash","tool_use_id":call_id,
        "description":"long command","is_backgrounded":backgrounded})
}

fn bash_tool_result(call_id: &str, text: &str, background_task: Option<&str>) -> Value {
    let mut event = json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
        "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":call_id,
            "content":text,"is_error":false}]},
        "tool_use_result":{"stdout":"","stderr":"","interrupted":false}});
    if let Some(task_id) = background_task {
        event["tool_use_result"]["backgroundTaskId"] = json!(task_id);
        event["tool_use_result"]["timedOutAfterMs"] = json!(120000);
    }
    event
}

/// The `Monitor` call Claude Code 2.1.310 streams: a `tool_use` block whose input is the
/// command to watch and its description.
fn monitor_call_message(message_id: &str, call_id: &str) -> Vec<Value> {
    tool_call_message(
        message_id,
        call_id,
        "Monitor",
        &json!({"command": "./watch-the-build", "description": "watch the build"}),
    )
}

/// The `Monitor` call's result as 2.1.310 emits it: the acknowledgement names the task as
/// `taskId`, where a backgrounded Bash call's names it as `backgroundTaskId`.
fn monitor_tool_result(call_id: &str, task_id: &str) -> Value {
    json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
        "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":call_id,
            "content":format!("Monitor started (task {task_id}, expires in 5m unless the source \
                ends first). You will be notified on each event."),
            "is_error":false}]},
        "tool_use_result":{"taskId":task_id,"timeoutMs":300000,"persistent":false}})
}

fn tool_call_ids(observations: &[AgentObservation]) -> Vec<String> {
    observations
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::ToolCall { call_id, .. } => Some(call_id.to_string()),
            _ => None,
        })
        .collect()
}

// REQ-AGENT-35. The sequence Claude Code 2.1.302 emits when a foreground Bash call outlives
// its foreground timeout, recorded from the qualified binary on 2026-10-22: the task starts
// in the foreground, heartbeats name the call with a counter suffix, a `task_updated` patch
// moves it to the background, and the tool result names the task; the notification arrives
// after the exchange's result.
#[test]
fn foreground_bash_moved_to_the_background_at_its_timeout_settles() {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(bash_call_message("tool-turn", "bash-call"));
    values.extend(message_end("tool_use"));
    values.push(bash_task_started("task-1", "bash-call", false));
    for index in 0..4 {
        values.push(
            json!({"type":"tool_progress","session_id":SESSION_ID,"parent_tool_use_id":null,
            "tool_use_id":format!("bash-call-heartbeat-{index}"),"tool_name":"Bash",
            "elapsed_time_seconds":30 * (index + 1),"heartbeat":true}),
        );
    }
    values.push(json!({"type":"system","subtype":"background_tasks_changed","session_id":SESSION_ID,
        "tasks":[{"task_id":"task-1","run_id":"run-1","task_type":"local_bash","description":"long command"}]}));
    values.push(
        json!({"type":"system","subtype":"task_updated","session_id":SESSION_ID,
        "task_id":"task-1","run_id":"run-1","patch":{"is_backgrounded":true}}),
    );
    values.push(bash_tool_result(
        "bash-call",
        "moved to the background",
        Some("task-1"),
    ));
    values.extend(text_message("first-answer", &["intermediate"], true));
    values.push(result(SESSION_ID));
    values.push(
        json!({"type":"system","subtype":"task_updated","session_id":SESSION_ID,
        "task_id":"task-1","run_id":"run-1","patch":{"status":"completed","end_time":1}}),
    );
    values.push(
        json!({"type":"system","subtype":"task_notification","session_id":SESSION_ID,
        "task_id":"task-1","run_id":"run-1","tool_use_id":"bash-call","status":"completed",
        "output_file":"/tmp/task-1.output","summary":"Background command completed"}),
    );
    values.push(json!({"type":"system","subtype":"background_tasks_changed","session_id":SESSION_ID,"tasks":[]}));
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "intermediate");
    assert!(
        tool_call_ids(&observations)
            .iter()
            .all(|id| id == "bash-call"),
        "heartbeats must name the call: {:?}",
        tool_call_ids(&observations)
    );
    assert!(
        unrecognized_events(&observations)
            .iter()
            .all(|event| event["subtype"] == "background_tasks_changed"),
        "{:?}",
        unrecognized_events(&observations)
    );
}

// REQ-AGENT-35. The harness starts a tool as soon as its block closes; a backgrounded start
// may therefore precede the message's own end.
#[test]
fn backgrounded_bash_start_before_the_message_ends_is_accepted() {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(bash_call_message("tool-turn", "bash-call"));
    values.push(bash_task_started("task-1", "bash-call", true));
    values.extend(message_end("tool_use"));
    values.push(bash_tool_result(
        "bash-call",
        "running in the background",
        Some("task-1"),
    ));
    values.extend(text_message("first-answer", &["intermediate"], true));
    values.push(result(SESSION_ID));
    values.push(
        json!({"type":"system","subtype":"task_notification","session_id":SESSION_ID,
        "task_id":"task-1","run_id":"run-1","tool_use_id":"bash-call","status":"completed"}),
    );
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "intermediate");
}

// REQ-AGENT-35. A foreground command's `task_notification` precedes its tool result and
// settles nothing; the result retires the call and the stream may end without more.
#[test]
fn foreground_bash_notification_before_its_result_owes_nothing() {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(bash_call_message("tool-turn", "bash-call"));
    values.extend(message_end("tool_use"));
    values.push(bash_task_started("task-1", "bash-call", false));
    values.push(
        json!({"type":"system","subtype":"task_notification","session_id":SESSION_ID,
        "task_id":"task-1","run_id":"run-1","tool_use_id":"bash-call","status":"completed",
        "output_file":"","summary":"command completed"}),
    );
    values.push(bash_tool_result("bash-call", "finished", None));
    values.extend(text_message("answer", &["done"], true));
    values.push(result(SESSION_ID));
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "done");
}

// REQ-AGENT-35. A background command stopped at the harness's background limit still
// settles: the notification's status is the harness's classification, not a condition.
#[test]
fn background_bash_stopped_by_the_harness_still_settles() {
    let mut values = background_first_turn();
    values.push(
        json!({"type":"system","subtype":"task_notification","session_id":SESSION_ID,
        "task_id":"task-1","tool_use_id":"bash-call","status":"killed"}),
    );
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "intermediate");
}

// REQ-AGENT-35. A backgrounded start for a call the stream never made is still refused.
#[test]
fn backgrounded_start_for_an_unknown_call_is_refused() {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(bash_call_message("tool-turn", "bash-call"));
    values.extend(message_end("tool_use"));
    values.push(bash_task_started("task-1", "other-call", true));
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
}

/// A main-thread `Monitor` call as Claude Code 2.1.310 emits it, captured for UM-3104: the
/// task list names the monitor's `local_bash` task, which starts backgrounded under the
/// call; the result acknowledges it as `taskId`; the turn ends paused. An event the command
/// prints opens a turn that nothing on the stream announces. When the command ends, the
/// task settles, the list empties, and the notified turn ends with `final`.
fn monitor_transcript() -> Vec<Value> {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(monitor_call_message("monitor-turn", "monitor-call"));
    values.extend([
        system_event(
            "background_tasks_changed",
            json!({"tasks": [{"task_id": "monitor-task", "run_id": "run-1",
                "task_type": "local_bash", "description": "long command"}]}),
        ),
        bash_task_started("monitor-task", "monitor-call", true),
    ]);
    values.extend(message_end("tool_use"));
    values.push(monitor_tool_result("monitor-call", "monitor-task"));
    values.extend(text_message("first-answer", &["watching"], true));
    values.push(result(SESSION_ID));
    values.extend(text_turn("event-answer", "still watching"));
    values.extend([
        system_event(
            "task_updated",
            json!({"task_id": "monitor-task", "run_id": "run-1",
                "patch": {"status": "completed", "end_time": 1}}),
        ),
        system_event(
            "task_notification",
            json!({"task_id": "monitor-task", "run_id": "run-1", "tool_use_id": "monitor-call",
                "status": "completed", "output_file": "/tmp/monitor-task.output",
                "summary": "Monitor \"long command\" stream ended"}),
        ),
        system_event("background_tasks_changed", json!({"tasks": []})),
    ]);
    values.extend(text_turn("final-answer", "final"));
    values
}

// REQ-AGENT-35, REQ-AGENT-69
// UM-3104: 2.1.310 starts a `Monitor` call's task as a backgrounded `local_bash` task, which
// the parser refused as a backgrounded start under a call that was no Bash call.
#[test]
fn a_monitors_task_is_background_work_whose_events_open_turns() {
    let values = monitor_transcript();
    let result_at = |nth: usize| {
        values
            .iter()
            .enumerate()
            .filter(|(_, value)| value["type"] == "result")
            .nth(nth)
            .unwrap()
            .0
    };
    let (first_result, event_result) = (result_at(0), result_at(1));
    let notified_turn = last_init(&values);

    // Response mode: the monitor's event turn and its notified turn are accepted and the
    // last turn's text is the response. The only unrecognized frame is the task list that
    // names the monitor's task before its start.
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "final");
    let early_list = &values[position_of(&values, |value| {
        value["subtype"] == "background_tasks_changed"
    })];
    assert_eq!(unrecognized_events(&observations), [early_list]);

    // Result mode: neither the first turn nor the event turn completes an exchange while
    // the monitor runs, and the notified turn's candidate settles the step.
    let mut paused = parser(AgentValueKind::Result, 1024);
    paused
        .push_stdout(&framed(&values[..=first_result]), drop)
        .unwrap();
    assert_eq!(paused.take_completed_result_exchange(), None);
    paused
        .push_stdout(&framed(&values[first_result + 1..=event_result]), drop)
        .unwrap();
    assert_eq!(paused.take_completed_result_exchange(), None);
    paused
        .push_stdout(&framed(&values[event_result + 1..notified_turn]), drop)
        .unwrap();
    paused
        .push_stdout(&framed(&structured_output_exchange_after(&[])), drop)
        .unwrap();
    assert_eq!(
        paused.take_completed_result_exchange(),
        Some(CompletedResultExchange::Candidate(Arc::new(json!(7))))
    );
}

// REQ-AGENT-35
// UM-3104: a `Monitor` call's `taskId` acknowledges only the task tracked under that call,
// as a Bash call's `backgroundTaskId` does.
#[test]
fn a_monitors_result_naming_an_untracked_task_is_a_protocol_failure() {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(monitor_call_message("monitor-turn", "monitor-call"));
    values.push(bash_task_started("monitor-task", "monitor-call", true));
    values.extend(message_end("tool_use"));
    values.push(monitor_tool_result("monitor-call", "other-task"));
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(
        protocol_rejection_value(&outcome)["detail"]["reason"],
        "user_event_invalid"
    );
}

// REQ-AGENT-38
// UM-3104: a task of a kind the parser does not model (neither `local_bash` nor
// `local_agent`) is recorded as unrecognized wherever a frame names it; the drain refused its
// task list as `terminal_drain_event_invalid`.
#[test]
fn a_task_of_an_unmodeled_kind_is_activity_in_the_exchange_and_the_drain() {
    let task = json!({"task_id": "other-task", "run_id": "run-1"});
    let with = |fields: Value| {
        let mut frame = task.clone();
        frame
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        frame
    };
    let listed = system_event(
        "background_tasks_changed",
        json!({"tasks": [with(json!({"task_type": "local_other", "description": "long command"}))]}),
    );
    let mut acknowledgement = monitor_tool_result("monitor-call", "other-task");
    acknowledgement["tool_use_result"]
        .as_object_mut()
        .unwrap()
        .remove("taskId");
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend(monitor_call_message("monitor-turn", "monitor-call"));
    values.extend([
        listed.clone(),
        system_event(
            "task_started",
            with(
                json!({"task_type": "local_other", "tool_use_id": "monitor-call",
                "description": "long command", "is_backgrounded": true}),
            ),
        ),
    ]);
    values.extend(message_end("tool_use"));
    values.extend([
        acknowledgement,
        system_event(
            "task_updated",
            with(json!({"patch": {"status": "running"}})),
        ),
        system_event(
            "task_progress",
            with(json!({"description": "still running"})),
        ),
    ]);
    values.extend(text_message("first-answer", &["watching"], true));
    values.extend([
        result(SESSION_ID),
        listed,
        system_event(
            "task_updated",
            with(json!({"patch": {"status": "completed", "end_time": 1}})),
        ),
        system_event(
            "task_notification",
            with(json!({"tool_use_id": "monitor-call", "status": "completed",
                "output_file": "/tmp/other-task.output", "summary": "ended"})),
        ),
    ]);

    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_eq!(completed_response(&outcome), "watching");
    let kinds: Vec<_> = unrecognized_events(&observations)
        .into_iter()
        .map(|event| event["subtype"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        [
            "background_tasks_changed",
            "task_started",
            "task_updated",
            "task_progress",
            "task_notification",
        ]
    );
}

#[test]
fn separate_background_wakeups_preserve_other_outstanding_tasks() {
    let mut values = background_first_turn();
    let second_start = values
        .iter()
        .position(|value| value["event"]["message"]["id"] == "first-answer")
        .unwrap();
    let mut second = vec![message_start("second-bash")];
    second.extend([
        stream_event(json!({"type":"content_block_start","index":0,"content_block":{
            "type":"tool_use","id":"bash-2","name":"Bash","input":{}
        }})),
        stream_event(json!({"type":"content_block_stop","index":0})),
        stream_event(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}})),
        stream_event(json!({"type":"message_stop"})),
        json!({"type":"system","subtype":"task_started","session_id":SESSION_ID,
            "task_id":"task-2","task_type":"local_bash","tool_use_id":"bash-2","is_backgrounded":true}),
        json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"bash-2",
                "content":"background","is_error":false}]},
            "tool_use_result":{"backgroundTaskId":"task-2"}}),
    ]);
    values.splice(second_start..second_start, second);
    values.extend([
        background_notification(),
        json!({"type":"system","subtype":"background_tasks_changed","session_id":SESSION_ID,
            "tasks":[{"task_id":"task-2","task_type":"local_bash"}]}),
        init(QUALIFICATION_VERSION, SESSION_ID),
    ]);
    values.extend(text_message("second-answer", &["second"], true));
    values.extend([
        result(SESSION_ID),
        json!({"type":"system","subtype":"task_notification","session_id":SESSION_ID,
            "task_id":"task-2","tool_use_id":"bash-2","status":"completed"}),
        json!({"type":"system","subtype":"background_tasks_changed","session_id":SESSION_ID,
            "tasks":[]}),
        init(QUALIFICATION_VERSION, SESSION_ID),
    ]);
    values.extend(text_message("third-answer", &["third"], true));
    values.push(result(SESSION_ID));
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("independent task wakeups must settle: {outcome:?}");
    };
    assert_eq!(response.as_str(), "third");
    let mut contradictory = values;
    let snapshot = contradictory
        .iter_mut()
        .find(|value| value["subtype"] == "background_tasks_changed")
        .unwrap();
    snapshot["tasks"][0]["task_id"] = json!("unrelated");
    assert_failed(
        replay(&framed(&contradictory), AgentValueKind::Response, 1024).1,
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn structured_result_can_follow_settled_background_work_but_not_new_work() {
    let mut values = background_first_turn();
    let first_answer = values
        .iter()
        .position(|value| value["event"]["message"]["id"] == "first-answer")
        .unwrap();
    values.truncate(first_answer);
    values.push(background_notification());
    values.extend([
        message_start("structured"),
        stream_event(json!({"type":"content_block_start","index":0,"content_block":{
            "type":"tool_use","id":"structured-call","name":"StructuredOutput",
            "input":{"result":7}
        }})),
        stream_event(json!({"type":"content_block_stop","index":0})),
        json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"structured-call",
                "content":"ok","is_error":false}]}}),
        stream_event(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}})),
        stream_event(json!({"type":"message_stop"})),
    ]);
    let mut terminal = result(SESSION_ID);
    terminal["structured_output"] = json!({"result":7});
    values.push(terminal);
    let mut accepted = parser(AgentValueKind::Result, 1024);
    accepted.push_stdout(&framed(&values), drop).unwrap();
    assert_eq!(
        accepted.take_completed_result_exchange(),
        Some(CompletedResultExchange::Candidate(Arc::new(json!(7))))
    );
    accepted
        .accept_result(CapturedJson::fixture(Arc::new(json!(7))))
        .unwrap();
    assert!(matches!(
        accepted.finish(true),
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));

    // A result submitted before an already-owned task finishes is premature: it is
    // never accepted, and a turn the notification announces cannot race the undecided
    // candidate.
    let mut late = values.clone();
    late.remove(first_answer);
    let mut premature = parser(AgentValueKind::Result, 1024);
    premature.push_stdout(&framed(&late), drop).unwrap();
    assert_eq!(
        premature.take_completed_result_exchange(),
        Some(CompletedResultExchange::PrematureCandidate)
    );
    assert_eq!(
        premature.accept_result(CapturedJson::fixture(Arc::new(json!(7)))),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
    let mut racing = parser(AgentValueKind::Result, 1024);
    racing.push_stdout(&framed(&late), drop).unwrap();
    racing.take_completed_result_exchange();
    racing
        .push_stdout(&framed(&[background_notification()]), drop)
        .unwrap();
    assert_eq!(
        racing.push_stdout(&framed(&[init(QUALIFICATION_VERSION, SESSION_ID)]), drop),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );

    // A turn that ends without a candidate while the task runs pauses with no decision.
    let mut pending = background_first_turn();
    pending.truncate(first_answer);
    pending.push(result(SESSION_ID));
    let mut paused = parser(AgentValueKind::Result, 1024);
    paused.push_stdout(&framed(&pending), drop).unwrap();
    assert_eq!(paused.take_completed_result_exchange(), None);
    paused
        .push_stdout(&framed(&[background_notification()]), drop)
        .unwrap();
    assert_failed(paused.finish(true), AgentFailureCause::MissingResult);
}

#[test]
fn foreground_bash_calls_retire_after_their_tool_results() {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    for index in 0..129 {
        let call = format!("bash-{index}");
        values.extend([
            message_start(&format!("message-{index}")),
            stream_event(json!({"type":"content_block_start","index":0,"content_block":{
                "type":"tool_use","id":call,"name":"Bash","input":{}
            }})),
            stream_event(json!({"type":"content_block_stop","index":0})),
            stream_event(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}})),
            stream_event(json!({"type":"message_stop"})),
            json!({"type":"user","session_id":SESSION_ID,"parent_tool_use_id":null,
                "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":call,
                    "content":"ok","is_error":false}]}}),
        ]);
    }
    values.extend(text_message("last", &["done"], true));
    values.push(result(SESSION_ID));
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert!(
        matches!(
            outcome,
            AgentOutcome::Completed(CompletedAgentInvocation::Response(_))
        ),
        "{outcome:?}"
    );
}

#[test]
fn background_continuation_rejects_unrelated_and_unsettled_work() {
    let first = background_first_turn();
    for mutate in [
        ("task_started", "tool_use_id", "unrelated-call"),
        ("user", "backgroundTaskId", "unrelated-task"),
    ] {
        let mut changed = first.clone();
        if mutate.0 == "task_started" {
            let started = changed
                .iter_mut()
                .find(|value| value["subtype"] == "task_started")
                .unwrap();
            started[mutate.1] = json!(mutate.2);
        } else {
            let acknowledgement = changed
                .iter_mut()
                .find(|value| value["type"] == "user")
                .unwrap();
            acknowledgement["tool_use_result"][mutate.1] = json!(mutate.2);
        }
        assert_failed(
            replay(&framed(&changed), AgentValueKind::Response, 1024).1,
            AgentFailureCause::HarnessProtocolFailed,
        );
    }
    let mut premature = first.clone();
    let task_start = premature
        .iter()
        .position(|event| event["subtype"] == "task_started")
        .unwrap();
    premature.swap(task_start - 1, task_start);
    assert_failed(
        replay(&framed(&premature), AgentValueKind::Response, 1024).1,
        AgentFailureCause::HarnessProtocolFailed,
    );
    assert_failed(
        replay(&framed(&first), AgentValueKind::Response, 1024).1,
        AgentFailureCause::HarnessProtocolFailed,
    );
    for extra in [
        vec![init(QUALIFICATION_VERSION, SESSION_ID)],
        vec![
            json!({"type":"system","subtype":"task_notification","session_id":SESSION_ID,
            "task_id":"unrelated","tool_use_id":"bash-call","status":"completed"}),
        ],
        vec![background_notification(), background_notification()],
        vec![
            background_notification(),
            init(
                QUALIFICATION_VERSION,
                "00000000-0000-4000-8000-000000000002",
            ),
        ],
    ] {
        let mut values = first.clone();
        values.extend(extra);
        assert_failed(
            replay(&framed(&values), AgentValueKind::Response, 1024).1,
            AgentFailureCause::HarnessProtocolFailed,
        );
    }
    let mut failed_turn = first.clone();
    let terminal = failed_turn.last_mut().unwrap();
    terminal["subtype"] = json!("error_during_execution");
    terminal["is_error"] = json!(true);
    terminal["terminal_reason"] = json!("error_during_execution");
    failed_turn.push(background_notification());
    failed_turn.push(init(QUALIFICATION_VERSION, SESSION_ID));
    // The notified turn after a failed one ends the stream with the native failure, not
    // with a rejection of its init (REQ-AGENT-75).
    assert_failed(
        replay(&framed(&failed_turn), AgentValueKind::Response, 1024).1,
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::Classified {
                canonical: CanonicalHarnessFailure::ModelError,
                diagnostic: HarnessFailureDiagnostic::from_code("other", None),
            },
        },
    );
    let mut forged = first.clone();
    forged.push(background_notification());
    forged.push(init(QUALIFICATION_VERSION, SESSION_ID));
    forged.push(
        json!({"type":"user", "session_id":SESSION_ID, "parent_tool_use_id":null,
        "message":{"role":"user","content":[{"type":"text","text":"unsolicited"}]}}),
    );
    assert_failed(
        replay(&framed(&forged), AgentValueKind::Response, 1024).1,
        AgentFailureCause::HarnessProtocolFailed,
    );
    let mut accepted = parser(AgentValueKind::Result, 1024);
    accepted
        .push_stdout(
            &framed(&[init(QUALIFICATION_VERSION, SESSION_ID), result(SESSION_ID)]),
            drop,
        )
        .unwrap();
    assert_eq!(
        accepted
            .push_stdout(&framed(&[background_notification()]), drop)
            .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
}

// INV-HARNESS-4
#[test]
fn every_initialization_must_match_the_validated_installation_version() {
    let admitted_version = "2.1.235";
    let mut matching = parser_for_version(AgentValueKind::None, 1024, admitted_version);
    matching
        .push_stdout(
            &framed(&[init(admitted_version, SESSION_ID), result(SESSION_ID)]),
            drop,
        )
        .unwrap();
    assert_eq!(
        matching.finish(true),
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );

    let mut contradictory = parser_for_version(AgentValueKind::None, 1024, admitted_version);
    contradictory
        .push_stdout(
            &framed(&[init(admitted_version, SESSION_ID), result(SESSION_ID)]),
            drop,
        )
        .unwrap();
    contradictory.begin_exchange().unwrap();
    assert_eq!(
        contradictory
            .push_stdout(&framed(&[init(QUALIFICATION_VERSION, SESSION_ID,)]), drop,)
            .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
    assert_failed(
        contradictory.finish(true),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

// REQ-AGENT-26
#[test]
fn recorded_response_uses_only_final_main_thread_deltas_once() {
    let (observations, outcome) = replay(RESPONSE_FIXTURE, AgentValueKind::Response, 1024);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("recorded response must complete");
    };
    assert_eq!(response.as_str(), "final answer");
    let observed_text = observations
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::AssistantText { text } => Some(text.as_ref()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(observed_text, "earliersubagentfinal answer");
}

#[test]
fn contradictory_nominal_assistant_text_rejects_the_response() {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend([
        message_start("msg-contradictory"),
        stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""},
        })),
        stream_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "stream answer"},
        })),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-contradictory",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": "different nominal answer"}],
                "model": MODEL,
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
        }),
        stream_event(json!({"type": "content_block_stop", "index": 0})),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
        result(SESSION_ID),
    ]);

    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
}

/// One Bash exchange as Claude Code 2.1.296 emits it: the streamed `input_json_delta`
/// carries the model's input, the nominal envelope restates the block with the
/// client-normalized input and the additive 2.1.296 members, and the tool result carries
/// the additive `tool_result_meta`. The nominal block and the result block are supplied so
/// one transcript shape serves both the accepted and the contradictory cases.
fn bash_exchange_transcript(nominal_block: Value, tool_result_block: Value) -> Vec<Value> {
    let streamed_command = format!("cd {CWD} && ls");
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend([
        message_start("msg-bash"),
        stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "tool-bash", "name": "Bash", "input": {}},
        })),
        stream_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "input_json_delta",
                "partial_json": json!({"command": streamed_command, "description": "list"})
                    .to_string(),
            },
        })),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-bash",
                "type": "message",
                "role": "assistant",
                "content": [nominal_block],
                "model": MODEL,
                "container": null,
                "context_management": null,
                "stop_details": null,
            },
            "wire_tool_inputs": {
                "tool-bash": {"command": streamed_command, "description": "list"},
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
        }),
        stream_event(json!({"type": "content_block_stop", "index": 0})),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
        json!({
            "type": "user",
            "message": {"role": "user", "content": [tool_result_block]},
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
            "tool_use_result": {"stdout": "README.md", "stderr": "", "interrupted": false},
            "tool_result_meta": [{
                "id": "tool-bash",
                "permission_decision": {"decision": "allow", "source": "mode"},
            }],
        }),
    ]);
    values.extend(text_message("msg-final", &["done"], true));
    values.push(result(SESSION_ID));
    values
}

fn normalized_bash_nominal_block() -> Value {
    json!({
        "type": "tool_use",
        "id": "tool-bash",
        "name": "Bash",
        "input": {"command": "ls", "description": "list"},
        "caller": {"type": "direct"},
    })
}

fn bash_tool_result_block(is_error: Option<Value>) -> Value {
    let mut block = json!({
        "type": "tool_result",
        "tool_use_id": "tool-bash",
        "content": "README.md",
    });
    if let Some(is_error) = is_error {
        block["is_error"] = is_error;
    }
    block
}

#[test]
fn nominal_tool_use_correlates_by_identity_and_an_omitted_is_error_is_success() {
    let values = bash_exchange_transcript(
        normalized_bash_nominal_block(),
        bash_tool_result_block(None),
    );
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("a client-normalized nominal tool input must not fail the exchange");
    };
    assert_eq!(response.as_str(), "done");
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult { call_id, is_error: false, content }
            if call_id.as_ref() == "tool-bash" && content.as_ref() == "README.md"
    )));
}

#[test]
fn nominal_tool_use_identity_and_tool_result_shape_still_fail_closed() {
    let mut other_identity = normalized_bash_nominal_block();
    other_identity["id"] = json!("tool-other");
    let mut other_name = normalized_bash_nominal_block();
    other_name["name"] = json!("Write");
    let mut non_object_input = normalized_bash_nominal_block();
    non_object_input["input"] = json!("ls");
    for contradictory in [other_identity, other_name, non_object_input] {
        let values = bash_exchange_transcript(contradictory, bash_tool_result_block(None));
        let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
        assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
    }

    let values = bash_exchange_transcript(
        normalized_bash_nominal_block(),
        bash_tool_result_block(Some(json!("false"))),
    );
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
}

#[test]
fn stream_continuations_from_another_thread_reject_the_response() {
    let values = [
        init(QUALIFICATION_VERSION, SESSION_ID),
        message_start("msg-main"),
        json!({
            "type": "stream_event",
            "event": {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": "subagent"},
            },
            "session_id": SESSION_ID,
            "parent_tool_use_id": "tool-spawn-subagent",
        }),
        json!({
            "type": "stream_event",
            "event": {"type": "content_block_stop", "index": 0},
            "session_id": SESSION_ID,
            "parent_tool_use_id": "tool-spawn-subagent",
        }),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
        result(SESSION_ID),
    ];

    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
}

#[test]
fn malformed_and_contradictory_recorded_streams_never_accept_a_response() {
    for (bytes, expected) in [
        (
            MALFORMED,
            AgentFailureCause::start_failure("launch preparation", "unavailable"),
        ),
        (
            MISSING_INIT,
            AgentFailureCause::start_failure("launch preparation", "unavailable"),
        ),
        (CONTRADICTORY_INIT, AgentFailureCause::HarnessProtocolFailed),
    ] {
        let (_, outcome) = replay(bytes, AgentValueKind::Response, 1024);
        assert_failed(outcome, expected);
    }

    let (_, invalid_utf8) = replay(&[0xff, b'\n'], AgentValueKind::Response, 1024);
    assert_failed(
        invalid_utf8,
        AgentFailureCause::start_failure("launch preparation", "unavailable"),
    );

    let (_, non_object) = replay(b"[]\n", AgentValueKind::Response, 1024);
    assert_failed(
        non_object,
        AgentFailureCause::start_failure("launch preparation", "unavailable"),
    );
}

/// Validates a serialized rejection against the protocol schema the runner reports it in
/// and the `workflow view` result schema that mirrors it.
fn assert_rejection_matches_schemas(rejection: &Value) {
    for (schema, definition) in [
        (
            include_str!("../../../../runner-protocol/src/schema/runner-protocol-v1.schema.json"),
            "AgentProtocolRejectionDiagnostic",
        ),
        (
            include_str!("../../../../../schemas/workflow-view-result-v1.schema.json"),
            "ProtocolRejection",
        ),
    ] {
        let document: Value = serde_json::from_str(schema).unwrap();
        let schema = json!({
            "$schema": document["$schema"],
            "$defs": document["$defs"],
            "$ref": format!("#/$defs/{definition}"),
        });
        let validator = jsonschema::draft202012::new(&schema).unwrap();
        assert!(
            validator.is_valid(rejection),
            "{definition} rejected {rejection}"
        );
    }
}

// REQ-AGENT-42
#[test]
fn parser_rejections_report_stable_profile_owned_structural_context() {
    let mut malformed = parser(AgentValueKind::None, 1024);
    assert_eq!(
        malformed.push_stdout(b"not-json\n", drop),
        Err(AgentFailureCause::start_failure(
            "launch preparation",
            "unavailable"
        ))
    );
    let malformed = malformed.finish(false);
    assert_eq!(
        protocol_rejection_value(&malformed),
        json!({
            "schemaVersion": 1,
            "profile": "ClaudeCodeStreamJsonV1",
            "detail": {
                "reason": "frame_decode_failed",
                "stage": "frame_decode",
                "state": {
                    "initialized": false,
                    "exchangeInitialized": false,
                    "exchangeActive": true,
                    "completedExchanges": 0,
                    "activeMessage": "none",
                    "finalMainMessage": false,
                    "exchangeStructuredOutputCandidates": 0,
                    "completedResultExchange": "none",
                    "resultDecisionPending": false,
                    "resultAccepted": false,
                    "nativeFailure": false,
                    "retryActive": false,
                    "outstandingBackgroundCommands": 0,
                    "outstandingBackgroundSubagents": 0,
                    "scheduledWakeupPending": false
                }
            }
        })
    );
    assert_rejection_matches_schemas(&protocol_rejection_value(&malformed));

    let mut initialization = init(QUALIFICATION_VERSION, SESSION_ID);
    initialization["cwd"] = json!("/contradictory");
    let (_, invalid_initialization) =
        replay(&framed(&[initialization]), AgentValueKind::None, 1024);
    let initialization = protocol_rejection_value(&invalid_initialization);
    assert_eq!(initialization["detail"]["reason"], "initialization_invalid");
    assert_eq!(initialization["detail"]["stage"], "initialization");
    assert_eq!(initialization["detail"]["outerEvent"], "system");
    assert_eq!(initialization["detail"]["state"]["eventThread"], "none");

    let mut missing_exchange_init = parser(AgentValueKind::None, 1024);
    missing_exchange_init
        .push_stdout(
            &framed(&[init(QUALIFICATION_VERSION, SESSION_ID), result(SESSION_ID)]),
            drop,
        )
        .unwrap();
    missing_exchange_init.begin_exchange().unwrap();
    assert!(
        missing_exchange_init
            .push_stdout(&framed(&[message_start("msg-early")]), drop)
            .is_err()
    );
    let missing_exchange_init = protocol_rejection_value(&missing_exchange_init.finish(false));
    assert_eq!(
        missing_exchange_init["detail"]["reason"],
        "exchange_initialization_missing"
    );
    assert_eq!(
        missing_exchange_init["detail"]["stage"],
        "exchange_lifecycle"
    );
    assert_eq!(
        missing_exchange_init["detail"]["state"]["eventThread"],
        "main"
    );
    assert_rejection_matches_schemas(&missing_exchange_init);

    let wrong_parent = json!({
        "type": "stream_event",
        "event": {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": "SENTINEL_ASSISTANT_TEXT"},
        },
        "session_id": SESSION_ID,
        "parent_tool_use_id": "SENTINEL_PARENT_TOOL_ID",
    });
    let (_, wrong_parent) = replay(
        &framed(&[
            init(QUALIFICATION_VERSION, SESSION_ID),
            message_start("msg-main"),
            wrong_parent,
        ]),
        AgentValueKind::Response,
        1024,
    );
    let wrong_parent = protocol_rejection_value(&wrong_parent);
    assert_eq!(
        wrong_parent["detail"]["reason"],
        "active_stream_parent_mismatch"
    );
    assert_eq!(wrong_parent["detail"]["outerEvent"], "stream_event");
    assert_eq!(wrong_parent["detail"]["streamEvent"], "content_block_start");
    assert_eq!(wrong_parent["detail"]["contentIndex"], 0);
    assert_eq!(wrong_parent["detail"]["contentBlock"], "text");
    assert_eq!(wrong_parent["detail"]["state"]["activeMessage"], "main");
    assert_eq!(wrong_parent["detail"]["state"]["eventThread"], "subagent");

    let (_, message_transition) = replay(
        &framed(&[
            init(QUALIFICATION_VERSION, SESSION_ID),
            message_start("msg-incomplete"),
            stream_event(json!({"type": "message_stop"})),
        ]),
        AgentValueKind::Response,
        1024,
    );
    let message_transition = protocol_rejection_value(&message_transition);
    assert_eq!(
        message_transition["detail"]["reason"],
        "message_transition_invalid"
    );
    assert_eq!(message_transition["detail"]["streamEvent"], "message_stop");
    assert_eq!(
        message_transition["detail"]["state"]["activeMessage"],
        "main"
    );
    assert_eq!(message_transition["detail"]["state"]["eventThread"], "main");

    let mut rejected_delta = stream_event(json!({
        "type": "content_block_delta",
        "index": 1,
        "delta": {
            "type": "text_delta",
            "text": "SENTINEL_ASSISTANT_TEXT",
            "reasoning": "SENTINEL_REASONING",
            "tool_input": {"secret": "SENTINEL_TOOL_INPUT"},
        },
    }));
    rejected_delta["prompt"] = json!("SENTINEL_PROMPT");
    rejected_delta["tool_result"] = json!("SENTINEL_TOOL_RESULT");
    rejected_delta["structured_output"] = json!({"value": "SENTINEL_STRUCTURED_OUTPUT"});
    rejected_delta["provider_payload"] = json!("SENTINEL_PROVIDER_PAYLOAD");
    rejected_delta["request_id"] = json!("SENTINEL_REQUEST_ID");
    rejected_delta["native_session_id"] = json!("SENTINEL_NATIVE_SESSION_ID");
    rejected_delta["credential"] = json!("SENTINEL_CREDENTIAL");
    rejected_delta["oversized"] = json!("x".repeat(100_000));
    let (_, rejected_delta) = replay(
        &framed(&[
            init(QUALIFICATION_VERSION, SESSION_ID),
            message_start("msg-content"),
            stream_event(json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            })),
            rejected_delta,
        ]),
        AgentValueKind::Response,
        1024,
    );
    let rejected_delta = protocol_rejection_value(&rejected_delta);
    assert_eq!(
        rejected_delta["detail"]["reason"],
        "content_block_index_mismatch"
    );
    assert_eq!(rejected_delta["detail"]["stage"], "content_block_delta");
    assert_eq!(rejected_delta["detail"]["outerEvent"], "stream_event");
    assert_eq!(
        rejected_delta["detail"]["streamEvent"],
        "content_block_delta"
    );
    assert_eq!(rejected_delta["detail"]["contentIndex"], 1);
    assert_eq!(rejected_delta["detail"]["contentBlock"], "text");
    assert_eq!(
        rejected_delta["detail"]["state"]["openContentBlock"],
        json!({"kind": "text", "contentIndex": 0})
    );
    let serialized = rejected_delta.to_string();
    for sentinel in [
        "SENTINEL_ASSISTANT_TEXT",
        "SENTINEL_REASONING",
        "SENTINEL_TOOL_INPUT",
        "SENTINEL_PROMPT",
        "SENTINEL_TOOL_RESULT",
        "SENTINEL_STRUCTURED_OUTPUT",
        "SENTINEL_PROVIDER_PAYLOAD",
        "SENTINEL_REQUEST_ID",
        "SENTINEL_NATIVE_SESSION_ID",
        "SENTINEL_CREDENTIAL",
        SESSION_ID,
    ] {
        assert!(!serialized.contains(sentinel));
    }
    assert!(serialized.len() < 16 * 1024);
    assert_rejection_matches_schemas(&rejected_delta);

    let (_, result_correlation) = replay(
        &framed(&[
            init(QUALIFICATION_VERSION, SESSION_ID),
            message_start("msg-active"),
            result(SESSION_ID),
        ]),
        AgentValueKind::None,
        1024,
    );
    let result_correlation = protocol_rejection_value(&result_correlation);
    assert_eq!(
        result_correlation["detail"]["reason"],
        "result_correlation_invalid"
    );
    assert_eq!(result_correlation["detail"]["outerEvent"], "result");
    assert_eq!(
        result_correlation["detail"]["state"]["activeMessage"],
        "main"
    );

    let (_, terminal_drain) = replay(
        &framed(&[
            init(QUALIFICATION_VERSION, SESSION_ID),
            result(SESSION_ID),
            result(SESSION_ID),
        ]),
        AgentValueKind::None,
        1024,
    );
    let terminal_drain = protocol_rejection_value(&terminal_drain);
    assert_eq!(
        terminal_drain["detail"]["reason"],
        "terminal_drain_event_invalid"
    );
    assert_eq!(terminal_drain["detail"]["stage"], "terminal_drain");
    assert_eq!(terminal_drain["detail"]["outerEvent"], "result");
    assert_eq!(terminal_drain["detail"]["state"]["eventThread"], "none");

    // A subagent's message after the exchange ended, with no subagent the step waits on.
    let mut orphan_subagent = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    orphan_subagent.extend(text_message("msg-final", &["final"], true));
    orphan_subagent.push(result(SESSION_ID));
    orphan_subagent.push(subagent_message(
        "assistant",
        json!({"id": "msg-subagent", "type": "message", "role": "assistant",
            "model": MODEL, "content": [{"type": "text", "text": "SENTINEL_SUBAGENT_TEXT"}]}),
    ));
    let (_, orphan_subagent) = replay(&framed(&orphan_subagent), AgentValueKind::Response, 1024);
    let orphan_subagent = protocol_rejection_value(&orphan_subagent);
    assert_eq!(
        orphan_subagent["detail"]["reason"],
        "terminal_drain_event_invalid"
    );
    assert_eq!(orphan_subagent["detail"]["outerEvent"], "assistant");
    let state = &orphan_subagent["detail"]["state"];
    assert_eq!(state["eventThread"], "subagent");
    assert_eq!(state["outstandingBackgroundCommands"], 0);
    assert_eq!(state["outstandingBackgroundSubagents"], 0);
    assert_eq!(state["scheduledWakeupPending"], false);
    assert!(
        !orphan_subagent
            .to_string()
            .contains("SENTINEL_SUBAGENT_TEXT")
    );
    assert_rejection_matches_schemas(&orphan_subagent);

    // A main-thread message while a backgrounded command is still outstanding.
    let mut main_while_waiting = background_first_turn();
    main_while_waiting.push(message_start("msg-late"));
    let (_, main_while_waiting) =
        replay(&framed(&main_while_waiting), AgentValueKind::Response, 1024);
    let main_while_waiting = protocol_rejection_value(&main_while_waiting);
    assert_eq!(
        main_while_waiting["detail"]["reason"],
        "terminal_drain_event_invalid"
    );
    let state = &main_while_waiting["detail"]["state"];
    assert_eq!(state["eventThread"], "main");
    assert_eq!(state["outstandingBackgroundCommands"], 1);
    assert_eq!(state["outstandingBackgroundSubagents"], 0);
    assert_rejection_matches_schemas(&main_while_waiting);

    let (_, eof) = replay(
        &framed(&[init(QUALIFICATION_VERSION, SESSION_ID)]),
        AgentValueKind::None,
        1024,
    );
    let eof = protocol_rejection_value(&eof);
    assert_eq!(eof["detail"]["reason"], "end_of_stream_invariant_invalid");
    assert_eq!(eof["detail"]["stage"], "end_of_stream");
    assert!(eof["detail"].get("outerEvent").is_none());
    assert!(eof["detail"]["state"].get("eventThread").is_none());
    assert_rejection_matches_schemas(&eof);
}

// REQ-AGENT-42
#[test]
fn standard_input_write_rejections_match_the_published_schemas() {
    for (reason, expected) in [
        (
            ClaudeCodeStreamJsonV1RejectionReason::StandardInputWriteFailed,
            "standard_input_write_failed",
        ),
        (
            ClaudeCodeStreamJsonV1RejectionReason::StandardInputWriteTimedOut,
            "standard_input_write_timed_out",
        ),
    ] {
        let mut parser = parser(AgentValueKind::Response, 1024);
        parser.fail_initial_input(reason);
        let rejection = protocol_rejection_value(&parser.finish(true));
        assert_eq!(rejection["detail"]["reason"], expected);
        assert_eq!(rejection["detail"]["stage"], "initial_input");
        assert_rejection_matches_schemas(&rejection);
    }
}

#[test]
fn initialization_identity_and_exchange_boundaries_are_unambiguous() {
    let contradictory = [
        ("claude_code_version", json!("2.1.223")),
        ("cwd", json!("/other")),
        ("model", json!("other-model")),
        ("permissionMode", json!("default")),
        ("session_id", json!("00000000-0000-4000-8000-000000000002")),
    ];
    for (field, replacement) in contradictory {
        let mut initialization = init(QUALIFICATION_VERSION, SESSION_ID);
        initialization[field] = replacement;
        let (_, outcome) = replay(&framed(&[initialization]), AgentValueKind::Response, 1024);
        assert_failed(
            outcome,
            AgentFailureCause::start_failure("launch preparation", "unavailable"),
        );
    }

    let mut duplicate_result = parser(AgentValueKind::Response, 1024);
    duplicate_result
        .push_stdout(
            &framed(&[init(QUALIFICATION_VERSION, SESSION_ID), result(SESSION_ID)]),
            drop,
        )
        .unwrap();
    assert_eq!(
        duplicate_result
            .push_stdout(&framed(&[result(SESSION_ID)]), drop)
            .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
    assert_failed(
        duplicate_result.finish(true),
        AgentFailureCause::HarnessProtocolFailed,
    );

    let mut missing_exchange_init = parser(AgentValueKind::Response, 1024);
    missing_exchange_init
        .push_stdout(
            &framed(&[init(QUALIFICATION_VERSION, SESSION_ID), result(SESSION_ID)]),
            drop,
        )
        .unwrap();
    missing_exchange_init.begin_exchange().unwrap();
    assert_eq!(
        missing_exchange_init
            .push_stdout(&framed(&[message_start("msg-early")]), drop)
            .unwrap_err(),
        AgentFailureCause::HarnessProtocolFailed
    );
    assert_failed(
        missing_exchange_init.finish(true),
        AgentFailureCause::HarnessProtocolFailed,
    );
}

#[test]
fn frame_limit_is_enforced_while_reading_bytes() {
    let limits =
        ClaudeCodeStreamJsonV1ProtocolLimits::with_maximum_frame_bytes(NonZeroU64::new(8).unwrap());
    let make_parser = || {
        ClaudeCodeStreamJsonV1Parser::new(
            Arc::from(CWD),
            Arc::from(MODEL),
            Arc::from(SESSION_ID),
            Arc::from(QUALIFICATION_VERSION),
            AgentValueKind::None,
            NonZeroU64::new(1024).unwrap(),
            limits,
        )
    };
    let mut at_limit = make_parser();
    at_limit.push_stdout(b"{}      ", drop).unwrap();
    assert!(at_limit.push_stdout(b"\n", drop).is_err());
    let at_limit = protocol_rejection_value(&at_limit.finish(true));
    assert_ne!(at_limit["detail"]["reason"], "frame_too_large");

    let mut over_limit = make_parser();
    assert!(over_limit.push_stdout(b"{}      X", drop).is_err());
    let over_limit = over_limit.finish(true);
    assert_eq!(
        protocol_rejection_value(&over_limit)["detail"]["reason"],
        "frame_too_large"
    );
    // REQ-AGENT-71: the overrun buffer, without the byte that overran it, is the rejected line.
    assert_eq!(
        stream_excerpt_value(&over_limit),
        json!({"rejectedLine": retained_line(b"{}      ", 8192), "precedingLines": []})
    );
}

fn excerpt_parser(limits: ClaudeCodeStreamJsonV1ProtocolLimits) -> ClaudeCodeStreamJsonV1Parser {
    ClaudeCodeStreamJsonV1Parser::new(
        Arc::from(CWD),
        Arc::from(MODEL),
        Arc::from(SESSION_ID),
        Arc::from(QUALIFICATION_VERSION),
        AgentValueKind::None,
        NonZeroU64::new(1024).unwrap(),
        limits,
    )
}

fn stream_excerpt_value(outcome: &AgentOutcome) -> Value {
    let AgentOutcome::Failed(failure) = outcome else {
        panic!("expected agent failure, got {outcome:?}");
    };
    serde_json::to_value(
        failure
            .stream_excerpt()
            .expect("a protocol rejection must carry its stream excerpt"),
    )
    .unwrap()
}

/// The retained form of an ASCII `frame` at `maximum_bytes`.
fn retained_line(frame: &[u8], maximum_bytes: usize) -> Value {
    let text = std::str::from_utf8(frame).unwrap();
    json!({
        "text": &text[..text.len().min(maximum_bytes)],
        "bytes": frame.len(),
        "truncated": text.len() > maximum_bytes,
    })
}

// REQ-AGENT-42, REQ-AGENT-71
#[test]
fn rejection_retains_the_redacted_rejected_line_and_the_bounded_tail_before_it() {
    let bound = |bytes| NonZeroUsize::new(bytes).unwrap();
    let mut parser = excerpt_parser(ClaudeCodeStreamJsonV1ProtocolLimits::with_excerpt_bounds(
        bound(8),
        bound(64),
        bound(256),
    ));
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend((0..10).map(|index| {
        let mut frame = status(SESSION_ID);
        frame["status"] = json!(format!("{index}-requesting"));
        frame
    }));
    parser.push_stdout(&framed(&values), drop).unwrap();
    let rejected = format!(
        "not-json Authorization: Bearer sk-ant-sentinel-token {}",
        "x".repeat(300)
    );
    assert_eq!(
        parser.push_stdout(format!("{rejected}\n").as_bytes(), drop),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
    let outcome = parser.finish(true);

    let excerpt = stream_excerpt_value(&outcome);
    let rejected_line = &excerpt["rejectedLine"];
    let text = rejected_line["text"].as_str().unwrap();
    assert!(text.contains("[redacted]"), "{text}");
    assert!(!text.contains("sentinel"), "{text}");
    assert!(text.len() <= 256);
    assert_eq!(rejected_line["truncated"], true);
    assert_eq!(rejected_line["bytes"], rejected.len());
    let preceding = values[3..]
        .iter()
        .map(|value| retained_line(&serde_json::to_vec(value).unwrap(), 64))
        .collect::<Vec<_>>();
    assert_eq!(preceding.len(), 8);
    assert!(preceding.iter().all(|line| line["truncated"] == true));
    assert_eq!(excerpt["precedingLines"], Value::Array(preceding));

    let rejection = protocol_rejection_value(&outcome);
    assert_eq!(rejection["detail"]["reason"], "frame_decode_failed");
    let mut members = rejection.as_object().unwrap().keys().collect::<Vec<_>>();
    members.sort();
    assert_eq!(members, ["detail", "profile", "schemaVersion"]);
    assert!(!rejection.to_string().contains("requesting"));
    assert_rejection_matches_schemas(&rejection);
}

// REQ-AGENT-71
#[test]
fn excerpt_keeps_the_open_buffer_at_end_of_stream_and_an_accepted_stream_keeps_none() {
    let (_, accepted) = replay(
        &framed(&[init(QUALIFICATION_VERSION, SESSION_ID), result(SESSION_ID)]),
        AgentValueKind::None,
        1024,
    );
    assert_eq!(
        accepted,
        AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
    );

    let values = [init(QUALIFICATION_VERSION, SESSION_ID), status(SESSION_ID)];
    let mut transcript = framed(&values);
    let partial = serde_json::to_vec(&result(SESSION_ID)).unwrap();
    transcript.extend_from_slice(&partial);
    let (_, outcome) = replay(&transcript, AgentValueKind::None, 1024);
    assert_eq!(
        protocol_rejection_value(&outcome)["detail"]["reason"],
        "partial_frame_at_end_of_stream"
    );
    assert_eq!(
        stream_excerpt_value(&outcome),
        json!({
            "rejectedLine": retained_line(&partial, 8192),
            "precedingLines": values
                .iter()
                .map(|value| retained_line(&serde_json::to_vec(value).unwrap(), 1024))
                .collect::<Vec<_>>(),
        })
    );
}

#[test]
fn truncation_and_frame_overflow_fail_in_the_current_protocol_phase() {
    let mut transcript = framed(&[init(QUALIFICATION_VERSION, SESSION_ID), result(SESSION_ID)]);
    transcript.pop();
    let (_, outcome) = replay(&transcript, AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);

    let limits =
        ClaudeCodeStreamJsonV1ProtocolLimits::with_maximum_frame_bytes(NonZeroU64::new(8).unwrap());
    let mut oversized = ClaudeCodeStreamJsonV1Parser::new(
        Arc::from(CWD),
        Arc::from(MODEL),
        Arc::from(SESSION_ID),
        Arc::from(QUALIFICATION_VERSION),
        AgentValueKind::Response,
        NonZeroU64::new(1024).unwrap(),
        limits,
    );
    assert_eq!(
        oversized.push_stdout(b"123456789", drop).unwrap_err(),
        AgentFailureCause::start_failure("launch preparation", "unavailable")
    );
    assert_failed(
        oversized.finish(true),
        AgentFailureCause::start_failure("launch preparation", "unavailable"),
    );
}

// REQ-AGENT-27
#[test]
fn response_distinguishes_missing_text_from_a_present_empty_block() {
    let (_, missing) = replay(
        &completed_text_transcript(&[], false),
        AgentValueKind::Response,
        1024,
    );
    assert_failed(missing, AgentFailureCause::MissingResponse);

    let (_, empty) = replay(
        &completed_text_transcript(&[], true),
        AgentValueKind::Response,
        1024,
    );
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(empty)) = empty else {
        panic!("present empty block must complete");
    };
    assert_eq!(empty.as_str(), "");
}

// REQ-AGENT-28
#[test]
fn response_utf8_limit_succeeds_exactly_and_fails_before_first_excess_byte() {
    let exact = completed_text_transcript(&["A", "é", "🔥"], true);
    let (_, exact_outcome) = replay(&exact, AgentValueKind::Response, 7);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = exact_outcome
    else {
        panic!("exact-limit response must complete");
    };
    assert_eq!(response.as_str(), "Aé🔥");

    let over = completed_text_transcript(&["A", "é", "🔥", "B"], true);
    let (_, over_outcome) = replay(&over, AgentValueKind::Response, 7);
    assert_failed(over_outcome, AgentFailureCause::CapturedValueTooLarge);
}

// REQ-AGENT-38
#[test]
fn reasoning_tools_results_metadata_and_unknown_events_are_normalized() {
    let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
    values.extend([
        message_start("msg-tools"),
        stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "plan"},
        })),
        stream_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": " more"},
        })),
        stream_event(json!({"type": "content_block_stop", "index": 0})),
        stream_event(json!({
            "type": "content_block_start",
            "index": 1,
            "content_block": {"type": "tool_use", "id": "tool-1", "name": "Agent", "input": {}},
        })),
        stream_event(json!({
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": "{}"},
        })),
        stream_event(json!({"type": "content_block_stop", "index": 1})),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 2},
        })),
        stream_event(json!({"type": "message_stop"})),
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "content": [{"type": "text", "text": "contents"}],
                }],
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
            "tool_use_result": {
                "status": "completed",
                "agentId": "agent-fixture",
                "agentType": "general-purpose",
            },
        }),
        json!({"type": "future_additive_event", "session_id": SESSION_ID}),
    ]);
    values.extend(text_message("msg-final", &["done"], true));
    values.push(result(SESSION_ID));
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("normalized activity transcript must complete");
    };
    assert_eq!(response.as_str(), "done");
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Reasoning { text } if text.as_ref() == "plan"
    )));
    for phase in [
        AgentToolCallPhase::Started,
        AgentToolCallPhase::Updated,
        AgentToolCallPhase::Completed,
    ] {
        assert!(observations.iter().any(|observation| matches!(
            observation,
            AgentObservation::ToolCall { call_id, name, phase: observed }
                if call_id.as_ref() == "tool-1" && name.as_ref() == "Agent" && *observed == phase
        )));
    }
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult { call_id, is_error: false, content }
            if call_id.as_ref() == "tool-1" && content.as_ref() == "contents"
    )));
    assert!(observations
        .iter()
        .any(|observation| matches!(observation, AgentObservation::Model { name } if name.as_ref() == MODEL)));
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Usage {
            input_tokens: 1,
            output_tokens: 2
        }
    )));
    assert_eq!(
        observations
            .iter()
            .filter(|observation| matches!(
                observation,
                AgentObservation::UnrecognizedHarnessEvent { .. }
            ))
            .count(),
        1
    );
}

#[test]
fn duplicate_keys_in_streamed_result_candidate_fail_before_normalization() {
    let values = [
        init(QUALIFICATION_VERSION, SESSION_ID),
        message_start("msg-result"),
        stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "tool_use",
                "id": "tool-structured-output",
                "name": "StructuredOutput",
                "input": {},
            },
        })),
        stream_event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "input_json_delta",
                "partial_json":
                    "{\"result\":{\"decision\":\"recheck\",\"decision\":\"gave_up\"}}",
            },
        })),
        stream_event(json!({"type": "content_block_stop", "index": 0})),
    ];

    let mut parser = parser(AgentValueKind::Result, 1024);
    assert_eq!(
        parser.push_stdout(&framed(&values), drop),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );
    assert!(matches!(parser.finish(false), AgentOutcome::Failed(_)));
}

#[test]
fn result_candidate_requires_correlated_success_acknowledgement() {
    let envelope = json!({"result": 7});
    let call_id = "tool-structured-output";
    let mut terminal_result = result(SESSION_ID);
    terminal_result["structured_output"] = envelope.clone();
    let values = [
        init(QUALIFICATION_VERSION, SESSION_ID),
        message_start("msg-result"),
        stream_event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "tool_use",
                "id": call_id,
                "name": "StructuredOutput",
                "input": envelope,
            },
        })),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-result",
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": call_id,
                    "name": "StructuredOutput",
                    "input": {"result": 7},
                }],
                "model": MODEL,
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
        }),
        stream_event(json!({"type": "content_block_stop", "index": 0})),
        json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "is_error": true,
                    "content": "Structured output failed",
                }],
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
        }),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
        terminal_result,
    ];

    let mut parser = parser(AgentValueKind::Result, 1024);
    parser.push_stdout(&framed(&values), drop).unwrap();
    assert_eq!(
        parser.take_completed_result_exchange(),
        Some(CompletedResultExchange::AmbiguousCandidate),
        "a failed StructuredOutput acknowledgement must disqualify its candidate",
    );
}

/// One result-mode exchange whose StructuredOutput call follows the supplied sibling
/// blocks, each given as a complete `content_block_start` block, acknowledged
/// successfully and ended by a result carrying the same envelope.
fn structured_output_exchange_after(siblings: &[Value]) -> Vec<Value> {
    structured_output_exchange_with(&json!({"result": 7}), siblings)
}

/// As `structured_output_exchange_after`, submitting `envelope`.
fn structured_output_exchange_with(envelope: &Value, siblings: &[Value]) -> Vec<Value> {
    let envelope = envelope.clone();
    let call_id = "tool-structured-output";
    let mut terminal_result = result(SESSION_ID);
    terminal_result["structured_output"] = envelope.clone();
    let mut values = vec![
        init(QUALIFICATION_VERSION, SESSION_ID),
        message_start("msg-result"),
    ];
    for (index, sibling) in siblings.iter().enumerate() {
        values.extend([
            stream_event(json!({
                "type": "content_block_start",
                "index": index,
                "content_block": sibling,
            })),
            json!({
                "type": "assistant",
                "message": {
                    "id": "msg-result",
                    "type": "message",
                    "role": "assistant",
                    "content": [sibling],
                    "model": MODEL,
                },
                "parent_tool_use_id": null,
                "session_id": SESSION_ID,
            }),
            stream_event(json!({"type": "content_block_stop", "index": index})),
        ]);
    }
    let index = siblings.len();
    let candidate_block = json!({
        "type": "tool_use",
        "id": call_id,
        "name": "StructuredOutput",
        "input": envelope,
    });
    values.extend([
        stream_event(json!({
            "type": "content_block_start",
            "index": index,
            "content_block": candidate_block,
        })),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-result",
                "type": "message",
                "role": "assistant",
                "content": [candidate_block],
                "model": MODEL,
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
        }),
        stream_event(json!({"type": "content_block_stop", "index": index})),
        json!({
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
            "session_id": SESSION_ID,
            "tool_use_result": "structurally correlated acknowledgement",
        }),
        stream_event(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 1},
        })),
        stream_event(json!({"type": "message_stop"})),
        terminal_result,
    ]);
    values
}

// REQ-AGENT-30
#[test]
fn result_candidate_stays_standalone_beside_thinking_but_not_another_tool_call() {
    let values = structured_output_exchange_after(&[
        json!({"type": "thinking", "thinking": "deciding on the result", "signature": "sig"}),
        json!({"type": "text", "text": "Submitting the result."}),
    ]);
    let mut standalone = parser(AgentValueKind::Result, 1024);
    standalone.push_stdout(&framed(&values), drop).unwrap();
    assert!(
        matches!(
            standalone.take_completed_result_exchange(),
            Some(CompletedResultExchange::Candidate(candidate)) if *candidate == json!(7)
        ),
        "thinking and text beside the structured-output call are observations"
    );

    let values = structured_output_exchange_after(&[json!({
        "type": "tool_use",
        "id": "tool-sibling",
        "name": "Bash",
        "input": {"command": "true"},
    })]);
    let mut ambiguous = parser(AgentValueKind::Result, 1024);
    ambiguous.push_stdout(&framed(&values), drop).unwrap();
    assert_eq!(
        ambiguous.take_completed_result_exchange(),
        Some(CompletedResultExchange::AmbiguousCandidate),
        "another tool call beside the structured-output call is other native work",
    );
}

#[test]
fn tool_result_blocks_other_than_text_are_observed_without_content() {
    let values = bash_exchange_transcript(
        normalized_bash_nominal_block(),
        json!({
            "type": "tool_result",
            "tool_use_id": "tool-bash",
            "content": [
                {"type": "tool_reference", "tool_name": "Bash"},
                {"type": "text", "text": "README.md"},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AA=="}},
            ],
        }),
    );
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert!(matches!(
        outcome,
        AgentOutcome::Completed(CompletedAgentInvocation::Response(_))
    ));
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::ToolResult { call_id, is_error: false, content }
            if call_id.as_ref() == "tool-bash" && content.as_ref() == "README.md"
    )));

    let values = bash_exchange_transcript(
        normalized_bash_nominal_block(),
        json!({
            "type": "tool_result",
            "tool_use_id": "tool-bash",
            "content": [{"type": "text"}],
        }),
    );
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
}

// REQ-AGENT-45
#[test]
fn structured_claude_terminal_reason_is_classified_without_result_text() {
    for (reason, expected, status) in [
        ("rate_limit", "rate_limited", None),
        ("context_window_exceeded", "context_window_exceeded", None),
        ("prompt_too_long", "context_window_exceeded", Some(400)),
        ("error_during_execution", "other", None),
    ] {
        let frames = framed(&[
            init(QUALIFICATION_VERSION, SESSION_ID),
            json!({"type": "result", "subtype": "error_during_execution",
                "is_error": true, "terminal_reason": reason,
                "api_error_status": status,
                "result": "private sentinel /path token", "session_id": SESSION_ID}),
        ]);
        let (_, outcome) = replay(&frames, AgentValueKind::None, 1024);
        let AgentOutcome::Failed(failure) = outcome else {
            panic!("expected harness failure");
        };
        let crate::workflow::agent::AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::Classified { diagnostic, .. },
        } = failure.cause()
        else {
            panic!("expected structured harness failure");
        };
        assert_eq!(diagnostic.harness_error, Some(expected));
        assert_eq!(diagnostic.http_status, status);
    }
}

// REQ-AGENT-40, REQ-DIAG-36
#[test]
fn unsuccessful_native_result_and_exit_use_existing_typed_failures() {
    let native_error = framed(&[
        init(QUALIFICATION_VERSION, SESSION_ID),
        json!({
            "type": "result",
            "subtype": "error_during_execution",
            "is_error": true,
            "terminal_reason": "error_during_execution",
            "session_id": SESSION_ID,
        }),
    ]);
    let (_, outcome) = replay(&native_error, AgentValueKind::None, 1024);
    assert_failed(
        outcome,
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::Classified {
                canonical: CanonicalHarnessFailure::ModelError,
                diagnostic: HarnessFailureDiagnostic::from_code("other", None),
            },
        },
    );

    let api_error = framed(&[
        init(QUALIFICATION_VERSION, SESSION_ID),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-api-error",
                "model": "<synthetic>",
                "role": "assistant",
                "type": "message",
                "content": [{"type": "text", "text": "API Error: controlled rejection"}],
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
            "error": "unknown",
            "request_id": "req-controlled",
            "is_api_error_message": true,
        }),
        json!({
            "type": "result",
            "subtype": "success",
            "is_error": true,
            "terminal_reason": "api_error",
            "result": "API Error: controlled rejection",
            "session_id": SESSION_ID,
        }),
    ]);
    let (observations, outcome) = replay(&api_error, AgentValueKind::None, 1024);
    assert_failed(
        outcome,
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::Classified {
                canonical: CanonicalHarnessFailure::ModelError,
                diagnostic: HarnessFailureDiagnostic::from_code("other", None),
            },
        },
    );
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Error,
            message,
        } if message.as_ref() == "API Error: controlled rejection"
    )));

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
        let api_error = framed(&[
            init(QUALIFICATION_VERSION, SESSION_ID),
            json!({"type": "assistant", "message": {
                "id": "msg-api-error", "model": "<synthetic>", "role": "assistant",
                "type": "message", "content": [{"type": "text", "text": "private sentinel"}]},
                "parent_tool_use_id": null, "session_id": SESSION_ID,
                "error": code, "api_error_status": assistant_status,
                "request_id": "req-controlled", "is_api_error_message": true}),
            json!({"type": "result", "subtype": "success", "is_error": true,
                "terminal_reason": "api_error", "result": "private sentinel",
                "api_error_status": result_status, "session_id": SESSION_ID}),
        ]);
        let (_, outcome) = replay(&api_error, AgentValueKind::None, 1024);
        let AgentOutcome::Failed(failure) = outcome else {
            panic!("expected harness failure");
        };
        let AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::Classified { diagnostic, .. },
        } = failure.cause()
        else {
            panic!("expected structured harness error");
        };
        assert_eq!(diagnostic.harness_error, Some(expected));
        assert_eq!(diagnostic.http_status, selected_status);
        let detail = crate::workflow::evidence::failure_detail(
            crate::workflow::evidence::FailurePhase::Execution,
            &crate::workflow::step_runtime::StepFailureCause::Execution(
                crate::workflow::step_runtime::StepExecutionFailure::Agent(failure),
            ),
        )
        .unwrap();
        let projected = detail.runner_diagnostic().unwrap();
        let mut exact = json!({"harnessError": expected, "harnessFailure": "model_error"});
        if let Some(status) = selected_status {
            exact["httpStatus"] = json!(status);
        }
        assert_eq!(projected, exact);
        assert!(!projected.to_string().contains("sentinel"));
    }

    let mut parser = parser(AgentValueKind::None, 1024);
    parser
        .push_stdout(
            &framed(&[init(QUALIFICATION_VERSION, SESSION_ID), result(SESSION_ID)]),
            drop,
        )
        .unwrap();
    assert_failed(
        parser.finish(false),
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::UnsuccessfulExit,
        },
    );
}

// REQ-AGENT-40, REQ-AGENT-45, REQ-DIAG-24
#[test]
fn synthetic_api_error_without_request_id_is_classified() {
    // Claude Code 2.1.306 answers a login-less first exchange with a `<synthetic>` api-error
    // message that carries no `request_id`, since no request reached the API.
    let not_logged_in = framed(&[
        init(QUALIFICATION_VERSION, SESSION_ID),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-not-logged-in",
                "model": "<synthetic>",
                "role": "assistant",
                "type": "message",
                "content": [{"type": "text", "text": "Not logged in · Please run /login"}],
            },
            "parent_tool_use_id": null,
            "session_id": SESSION_ID,
            "error": "authentication_failed",
            "is_api_error_message": true,
        }),
        json!({
            "type": "result",
            "subtype": "success",
            "is_error": true,
            "terminal_reason": "api_error",
            "api_error_status": null,
            "result": "Not logged in · Please run /login",
            "session_id": SESSION_ID,
        }),
    ]);
    let (observations, outcome) = replay(&not_logged_in, AgentValueKind::None, 1024);
    assert_failed(
        outcome,
        AgentFailureCause::HarnessFailed {
            detail: AgentHarnessFailureDetail::Classified {
                canonical: CanonicalHarnessFailure::ModelError,
                diagnostic: HarnessFailureDiagnostic::from_code("authentication_failed", None),
            },
        },
    );
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Diagnostic {
            level: AgentDiagnosticLevel::Error,
            message,
        } if message.as_ref() == "Not logged in · Please run /login"
    )));
}

#[test]
fn rate_limit_events_warn_only_when_a_window_is_not_allowed() {
    fn rate_limit(info: Value) -> Value {
        json!({"type": "rate_limit_event", "rate_limit_info": info, "session_id": SESSION_ID})
    }
    let transcript = framed(&[
        init(QUALIFICATION_VERSION, SESSION_ID),
        rate_limit(json!({"status": "allowed", "rateLimitType": "five_hour"})),
        rate_limit(json!({"status": "allowed_warning", "rateLimitType": "seven_day"})),
        rate_limit(json!({"status": "rejected"})),
        rate_limit(json!({})),
        result(SESSION_ID),
    ]);
    let (observations, _) = replay(&transcript, AgentValueKind::None, 1024);
    let warnings: Vec<&str> = observations
        .iter()
        .filter_map(|observation| match observation {
            AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Warning,
                message,
            } => Some(message.as_ref()),
            _ => None,
        })
        .collect();
    assert_eq!(
        warnings,
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
    fn rate_limit(info: Value) -> Value {
        json!({"type": "rate_limit_event", "rate_limit_info": info, "session_id": SESSION_ID})
    }
    fn api_error(code: &str, status: u16) -> [Value; 2] {
        [
            json!({"type": "assistant", "message": {
                "id": "msg-api-error", "model": "<synthetic>", "role": "assistant",
                "type": "message", "content": [{"type": "text", "text": "private sentinel"}]},
                "parent_tool_use_id": null, "session_id": SESSION_ID,
                "error": code, "api_error_status": status,
                "request_id": "req-controlled", "is_api_error_message": true}),
            json!({"type": "result", "subtype": "success", "is_error": true,
                "terminal_reason": "api_error", "result": "private sentinel",
                "session_id": SESSION_ID}),
        ]
    }
    fn diagnostic(frames: &[Value]) -> HarnessFailureDiagnostic {
        let mut values = vec![init(QUALIFICATION_VERSION, SESSION_ID)];
        values.extend_from_slice(frames);
        let (_, outcome) = replay(&framed(&values), AgentValueKind::None, 1024);
        let AgentOutcome::Failed(failure) = outcome else {
            panic!("expected harness failure");
        };
        let AgentFailureCause::HarnessFailed {
            detail:
                AgentHarnessFailureDetail::Classified {
                    canonical: CanonicalHarnessFailure::ModelError,
                    diagnostic,
                },
        } = failure.cause()
        else {
            panic!("expected classified model error");
        };
        *diagnostic
    }
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
        assert_eq!(unknown.harness_error, Some("rate_limited"));
        assert_eq!(unknown.resets_at, None);
        assert_eq!(
            unknown,
            HarnessFailureDiagnostic::from_code("rate_limit", Some(429))
        );
    }

    let [message, result] = api_error("authentication_failed", 401);
    let unauthorized = diagnostic(&[rejected, message, result]);
    assert_eq!(unauthorized.harness_error, Some("unauthorized"));
    assert_eq!(unauthorized.resets_at, None);
}

// REQ-AGENT-26
#[test]
fn subagent_messages_may_run_on_another_model_while_the_main_thread_must_not() {
    let subagent = |event: Value| {
        json!({
            "type": "stream_event",
            "event": event,
            "session_id": SESSION_ID,
            "parent_tool_use_id": "tool-spawn-subagent",
        })
    };
    let mut values = vec![
        init(QUALIFICATION_VERSION, SESSION_ID),
        subagent(json!({
            "type": "message_start",
            "message": {
                "id": "msg-subagent",
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": "builder-model",
                "usage": {"input_tokens": 1, "output_tokens": 0},
            },
        })),
        subagent(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""},
        })),
        subagent(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "subagent"},
        })),
        subagent(json!({"type": "content_block_stop", "index": 0})),
        subagent(json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 1},
        })),
        subagent(json!({"type": "message_stop"})),
        json!({
            "type": "assistant",
            "message": {
                "id": "msg-subagent",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": "subagent"}],
                "model": "builder-model",
            },
            "parent_tool_use_id": "tool-spawn-subagent",
            "session_id": SESSION_ID,
        }),
    ];
    values.extend(text_message("msg-final", &["final answer"], true));
    values.push(result(SESSION_ID));
    let (observations, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
        panic!("a subagent on another model must not reject the response, got {outcome:?}");
    };
    assert_eq!(response.as_str(), "final answer");
    assert!(observations.iter().any(|observation| matches!(
        observation,
        AgentObservation::Model { name } if name.as_ref() == "builder-model"
    )));

    let mut main_start = message_start("msg-main");
    main_start["event"]["message"]["model"] = json!("builder-model");
    let values = [init(QUALIFICATION_VERSION, SESSION_ID), main_start];
    let (_, outcome) = replay(&framed(&values), AgentValueKind::Response, 1024);
    assert_failed(outcome, AgentFailureCause::HarnessProtocolFailed);
}
