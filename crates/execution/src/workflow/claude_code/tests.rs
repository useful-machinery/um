use std::ffi::OsString;
use std::num::NonZeroU64;
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

    // A result may be accepted before an already-owned task finishes. The
    // notification is lifecycle drain; a subsequent native turn is not.
    let mut late = values.clone();
    late.remove(first_answer);
    let mut accepted = parser(AgentValueKind::Result, 1024);
    accepted.push_stdout(&framed(&late), drop).unwrap();
    assert!(matches!(
        accepted.take_completed_result_exchange(),
        Some(CompletedResultExchange::Candidate(_))
    ));
    accepted
        .accept_result(CapturedJson::fixture(Arc::new(json!(7))))
        .unwrap();
    accepted
        .push_stdout(&framed(&[background_notification()]), drop)
        .unwrap();
    assert!(matches!(
        accepted.finish(true),
        AgentOutcome::Completed(CompletedAgentInvocation::Result(_))
    ));
    let mut forbidden = parser(AgentValueKind::Result, 1024);
    forbidden.push_stdout(&framed(&late), drop).unwrap();
    forbidden.take_completed_result_exchange();
    forbidden
        .accept_result(CapturedJson::fixture(Arc::new(json!(7))))
        .unwrap();
    forbidden
        .push_stdout(&framed(&[background_notification()]), drop)
        .unwrap();
    assert_eq!(
        forbidden.push_stdout(&framed(&[init(QUALIFICATION_VERSION, SESSION_ID)]), drop),
        Err(AgentFailureCause::HarnessProtocolFailed)
    );

    let mut pending = background_first_turn();
    pending.truncate(first_answer);
    pending.push(result(SESSION_ID));
    let mut parser = parser(AgentValueKind::Result, 1024);
    parser.push_stdout(&framed(&pending), drop).unwrap();
    assert_eq!(
        parser.take_completed_result_exchange(),
        Some(CompletedResultExchange::MissingCandidate)
    );
    parser
        .push_stdout(&framed(&[background_notification()]), drop)
        .unwrap();
    assert_failed(parser.finish(true), AgentFailureCause::MissingResult);
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
    assert_failed(
        replay(&framed(&failed_turn), AgentValueKind::Response, 1024).1,
        AgentFailureCause::HarnessProtocolFailed,
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
                    "retryActive": false
                }
            }
        })
    );

    let mut initialization = init(QUALIFICATION_VERSION, SESSION_ID);
    initialization["cwd"] = json!("/contradictory");
    let (_, invalid_initialization) =
        replay(&framed(&[initialization]), AgentValueKind::None, 1024);
    let initialization = protocol_rejection_value(&invalid_initialization);
    assert_eq!(initialization["detail"]["reason"], "initialization_invalid");
    assert_eq!(initialization["detail"]["stage"], "initialization");
    assert_eq!(initialization["detail"]["outerEvent"], "system");

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
            .push_stdout(&framed(&[status(SESSION_ID)]), drop)
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

    let (_, eof) = replay(
        &framed(&[init(QUALIFICATION_VERSION, SESSION_ID)]),
        AgentValueKind::None,
        1024,
    );
    let eof = protocol_rejection_value(&eof);
    assert_eq!(eof["detail"]["reason"], "end_of_stream_invariant_invalid");
    assert_eq!(eof["detail"]["stage"], "end_of_stream");
    assert!(eof["detail"].get("outerEvent").is_none());
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
            .push_stdout(&framed(&[status(SESSION_ID)]), drop)
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
    let over_limit = protocol_rejection_value(&over_limit.finish(true));
    assert_eq!(over_limit["detail"]["reason"], "frame_too_large");
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
    let envelope = json!({"result": 7});
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

// REQ-AGENT-40
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
        assert_eq!(projected["harnessError"], expected);
        assert_eq!(
            projected.get("httpStatus").cloned(),
            selected_status.map(|s| json!(s))
        );
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
