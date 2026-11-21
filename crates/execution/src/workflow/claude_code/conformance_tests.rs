use std::fs;
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use nix::sys::stat::Mode;
use nix::unistd::mkfifo;
use rustix::process::{Pid, getpgid};
#[cfg(target_os = "linux")]
use rustix::process::{PidfdFlags, Signal, pidfd_open, pidfd_send_signal};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use super::super::test_support::host_scaled_watchdog;
use super::adapter::{ClaudeCodeStreamJsonV1Adapter, QUIET_BOUND, QUIET_BOUND_FORCE_DELAY};
use super::adapter_tests::{
    StagedClock, adapter_warnings, positive_integer_schema, result_mode, type_schema,
    wait_for_turn_completed,
};
use super::outcome::{Ending, StepFacts, decide};
use super::reader::StreamReader;
use super::test_support::{
    BackgroundSubagent, FixtureSignal, LoopbackBlock, LoopbackProvider, MODEL, PendingClock,
    RESULT_TOOL, RecordingObservationSink, RunningProductionClaudeCode, RunningStartedClaudeCode,
    SyntheticClaudeCodeRoot, WATCHDOG, admitted_adapter, assert_backgrounded_subagent_activity,
    assistant_text, conformance_executable, conformance_limits, exclusive_conformance_executable,
    invocation_identity, native_resource_watchdog, next_request_or_outcome, submission,
    version_probe_environment,
};
use super::*;
use crate::workflow::admission::{CancellationReason, CancellationSource};
use crate::workflow::agent::{
    AgentAdapter, AgentFailureCause, AgentHarnessFailureDetail, AgentInvocation,
    AgentInvocationStaging, AgentLifecycleMilestone, AgentObservation, AgentOutcome,
    AgentProcessContext, AgentPrompt, AgentValueKind, AgentValueMode, CanonicalHarnessFailure,
    CompletedAgentInvocation, StagedAgentAttachment, agent_start_channel, failed_agent_outcome,
};
use crate::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::workflow::diagnostic::StepDiagnosticLog;
use crate::workflow::execution_root::AdmittedExecutionRoot;
use crate::workflow::observation::NoopExecutionObserver;
use crate::workflow::process_group::{ProcessGuardRegistry, process_group_is_quiescent};

const EFFORT: &str = "xhigh";
const RESPONSE: &str = "loopback complete";
const NATIVE_INSTRUCTION_MARKER: &str = "LIV2293_NATIVE_INSTRUCTION_MARKER";
const NATIVE_SKILL_MARKER: &str = "LIV2293_NATIVE_SKILL_MARKER";
const NATIVE_PLUGIN_MARKER: &str = "LIV2293_NATIVE_PLUGIN_MARKER";
const NATIVE_MCP_MARKER: &str = "LIV2293_NATIVE_MCP_MARKER";
const TESTING_SUBAGENT_PROMPT: &str = "Run the controlled test suite in the background.";
const TESTING_SUBAGENT_DESCRIPTION: &str = "Run the tests";
const DIRECT_SESSION_ID: &str = "00000000-0000-4000-8000-000000000001";
const FIRST_TEXT_BLOCK: &str = "first text block";
const SECOND_TEXT_BLOCK: &str = "second text block";
/// What the pinned release's `result` carries for a final message of several text blocks:
/// the last block's text alone (REQ-AGENT-26).
const SEVERAL_BLOCK_RESPONSE: &str = SECOND_TEXT_BLOCK;

async fn launch_response_lifecycle(
    executable: PathBuf,
    root: &SyntheticClaudeCodeRoot,
    provider: &LoopbackProvider,
) -> RunningStartedClaudeCode {
    launch_recorded_response_lifecycle(
        executable,
        root,
        provider,
        &RecordingObservationSink::default(),
    )
    .await
}

async fn launch_recorded_response_lifecycle(
    executable: PathBuf,
    root: &SyntheticClaudeCodeRoot,
    provider: &LoopbackProvider,
    observations: &RecordingObservationSink,
) -> RunningStartedClaudeCode {
    RunningProductionClaudeCode::launch(
        executable,
        root,
        provider,
        AgentValueMode::Response {
            output: Arc::from("response"),
        },
        observations.clone(),
    )
    .await_started()
    .await
}

async fn assert_user_cancelled(running: RunningStartedClaudeCode) {
    assert_eq!(
        running.finish().await,
        AgentOutcome::Cancelled {
            reason: CancellationReason::UserRequest,
        }
    );
}

async fn install_native_resource_plugin(
    executable: &Path,
    root: &SyntheticClaudeCodeRoot,
    provider: &LoopbackProvider,
    marketplace: &Path,
) {
    let mut add = Command::new(executable);
    add.args(["plugin", "marketplace", "add"])
        .arg(marketplace)
        .args(["--scope", "user"]);
    root.configure_command(&mut add, provider);
    let add = add.output().await.unwrap();
    assert!(
        add.status.success(),
        "failed to add native resource marketplace: stdout={} stderr={}",
        String::from_utf8_lossy(&add.stdout),
        String::from_utf8_lossy(&add.stderr),
    );

    let mut install = Command::new(executable);
    install.args([
        "plugin",
        "install",
        "scherzo-plugin@scherzo-marketplace",
        "--scope",
        "user",
    ]);
    root.configure_command(&mut install, provider);
    let install = install.output().await.unwrap();
    assert!(
        install.status.success(),
        "failed to install native resource plugin: stdout={} stderr={}",
        String::from_utf8_lossy(&install.stdout),
        String::from_utf8_lossy(&install.stderr),
    );
}

fn process_id(bytes: &[u8]) -> Pid {
    let raw = std::str::from_utf8(bytes).unwrap().trim().parse().unwrap();
    Pid::from_raw(raw).unwrap()
}

fn assert_retained_native_session(root: &SyntheticClaudeCodeRoot, expected: &[&str]) {
    let transcript = fs::read_to_string(root.retained_transcript()).unwrap();
    for fragment in expected {
        assert!(
            transcript.contains(fragment),
            "missing retained fragment: {fragment}"
        );
    }
    assert!(root.retained_resources().is_dir());
    let ambient_paths = root.ambient_session_paths(DIRECT_SESSION_ID);
    for ambient in &ambient_paths {
        assert!(
            !ambient.exists(),
            "ambient session residue: {}",
            ambient.display()
        );
    }
    // The adapter removes the ambient project directory it created once it is
    // empty, so one that remains holds an entry the harness left.
    let ambient_project = ambient_paths[0].parent().unwrap();
    assert!(
        !ambient_project.exists(),
        "ambient project history retained an unrelated entry"
    );
}

// REQ-AGENT-4
// INV-HARNESS-10
// INV-HARNESS-25
#[test]
#[ignore = "requires pinned harness"]
fn pinned_real_claude_code_00_qualification_anchor_is_exact() {
    let executable = conformance_executable();
    let temporary = tempfile::tempdir().unwrap();
    for (_, value) in version_probe_environment(temporary.path()) {
        fs::create_dir_all(value).unwrap();
    }
    let output = std::process::Command::new(executable)
        .arg("--version")
        .env_clear()
        .envs(version_probe_environment(temporary.path()))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        output.stdout,
        format!("{QUALIFICATION_VERSION} (Claude Code)\n").as_bytes()
    );
    println!(
        "qualified Claude Code version={} profile=ClaudeCodeStreamJsonV1 host={}-{}",
        QUALIFICATION_VERSION,
        std::env::consts::OS,
        std::env::consts::ARCH,
    );
}

// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_01_normal_mode_loopback_conforms_from_a_synthetic_root() {
    // Every exact-binary case deliberately owns a fresh watchdog, loopback provider, and
    // synthetic root; sharing those resources would let one native case contaminate another.
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    // The watchdog's diagnosis reads the MCP server's observation, so the root and its
    // fixture exist before the bound; the body borrows them.
    let root = SyntheticClaudeCodeRoot::new();
    let resources = root.install_native_resource_fixture(
        NATIVE_INSTRUCTION_MARKER,
        NATIVE_SKILL_MARKER,
        NATIVE_PLUGIN_MARKER,
        NATIVE_MCP_MARKER,
    );
    native_resource_watchdog(resources.mcp_observation.clone(), WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        install_native_resource_plugin(&executable, &root, &provider, &resources.marketplace).await;
        // A second prompt hook observes the same turn without relying on execution order.
        let concurrent_hook_observation = root.private().join("second-hook-observation");
        let concurrent_hook = root.private().join("second-hook.sh");
        fs::write(
            &concurrent_hook,
            format!("printf 'second hook complete\\n' > '{}'\n", concurrent_hook_observation.display()),
        ).unwrap();
        let settings_path = root.project().join(".claude/settings.json");
        let mut settings: Value = serde_json::from_slice(&fs::read(&settings_path).unwrap()).unwrap();
        settings["hooks"]["UserPromptSubmit"][0]["hooks"]
            .as_array_mut().unwrap().push(json!({
                "type": "command",
                "command": format!("{} {}", std::env::var("USEFUL_CLAUDE_CODE_CONFORMANCE_SHELL").unwrap(), concurrent_hook.display())
            }));
        fs::write(settings_path, serde_json::to_vec(&settings).unwrap()).unwrap();
        let message = "Complete the deterministic synthetic exchange.";

        let mut command = Command::new(executable);
        command
            .args(normal_mode_arguments(
                MODEL,
                EFFORT,
                DIRECT_SESSION_ID,
                root.system_prompt(),
            ))
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        root.configure_command(&mut command, &provider);
        // The unanswered elicitation is the behavior under test, not a readiness
        // signal; bound the native wait and assert its correlated result below.
        command.env("MCP_TOOL_TIMEOUT", "1000");
        // 2.1.321 keeps a skill's description in the listing only within a budget of 1% of
        // the context window, and its bundled skills' descriptions use it up, so the synthetic
        // project and plugin skills are listed by name only. The descriptions carry the
        // markers asserted below; raise the budget so the listing shows them.
        command.env("SLASH_COMMAND_TOOL_CHAR_BUDGET", "100000");
        let mut child = command.spawn().unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(&initial_user_text_frame(message).unwrap())
            .await
            .unwrap();
        stdin.shutdown().await.unwrap();
        drop(stdin);

        let request = provider.next_request().await;
        assert_eq!(request.path(), "/v1/messages?beta=true");
        assert!(request.used_placeholder_key());
        assert_eq!(request.body()["model"], MODEL);
        assert_eq!(request.body()["stream"], true);
        assert!(contains_string_fragment(
            request.body(),
            "Operate only on the deterministic synthetic conformance request."
        ));
        assert!(contains_exact_string(request.body(), message));
        for marker in [
            NATIVE_INSTRUCTION_MARKER,
            NATIVE_SKILL_MARKER,
            NATIVE_PLUGIN_MARKER,
        ] {
            assert!(
                contains_string_fragment(request.body(), marker),
                "native request omitted resource marker {marker}"
            );
        }
        assert_eq!(
            fs::read_to_string(&resources.hook_observation).unwrap(),
            resources.expected_hook_observation,
        );
        assert_eq!(fs::read(&concurrent_hook_observation).unwrap(), b"second hook complete\n");
        let mcp_observation = fs::read_to_string(&resources.mcp_observation).unwrap();
        assert!(mcp_observation.starts_with("started\n"), "{mcp_observation}");
        assert!(mcp_observation.contains("tools/list"), "{mcp_observation}");
        // Deferred tool selection precedes the MCP tool call. The server issues an
        // elicitation that the headless client cannot answer; the native timeout
        // must remain an ordinary tool error, not a protocol or session failure.
        request.release_tool_use("ToolSearch", json!({"query": "scherzo_native_resource"}));
        let request = provider.next_request().await;
        assert!(contains_string_fragment(request.body(), NATIVE_MCP_MARKER));
        request.release_tool_use("mcp__scherzo-resource__scherzo_native_resource", json!({}));
        // The cancellation is a notification Claude Code awaits no reply to, and its exit
        // stops the server; hold the last response until the server has recorded it.
        let request = provider.next_request().await;
        resources.mcp_cancelled.receive().await;
        request.release_text(RESPONSE);

        let output = child.wait_with_output().await.unwrap();
        let mcp_observation = fs::read_to_string(&resources.mcp_observation).unwrap();
        assert!(
            mcp_observation.contains("\"method\":\"tools/call\""),
            "{mcp_observation}"
        );
        assert!(
            mcp_observation.contains("\"method\":\"notifications/cancelled\""),
            "{mcp_observation}"
        );
        assert!(output.status.success());
        // The pinned release diagnoses the synthetic model on stderr. Its bytes are an
        // actionable process diagnostic, not protocol, and their wording is not a contract.
        assert!(!output.stderr.is_empty());
        assert!(!provider.has_pending_request());

        let mut reader = StreamReader::new(
            AgentValueKind::Response,
            NonZeroU64::new(1024).unwrap(),
        );
        let mut native_observations = Vec::new();
        for chunk in output.stdout.chunks(7) {
            reader.push(chunk, |observation| native_observations.push(observation));
        }
        reader.finish(|observation| native_observations.push(observation));
        let init: Value = serde_json::from_slice(
            output.stdout.split(|byte| *byte == b'\n').next().unwrap(),
        )
        .unwrap();
        assert!(init["tools"].as_array().unwrap().contains(&json!("Monitor")));
        assert!(init["plugins"]
            .as_array()
            .unwrap()
            .iter()
            .any(|plugin| plugin["name"] == "scherzo-plugin"));
        let mcp_call = native_observations
            .iter()
            .find_map(|observation| match observation {
                AgentObservation::ToolCall { call_id, name, .. }
                    if name.as_ref() == "mcp__scherzo-resource__scherzo_native_resource" =>
                {
                    Some(call_id)
                }
                _ => None,
            })
            .expect("native MCP call must be observed");
        assert!(native_observations.iter().any(|observation| matches!(
            observation,
            AgentObservation::ToolResult { call_id, is_error: true, .. } if call_id == mcp_call
        )));
        assert_eq!(
            reader
                .initialization()
                .and_then(|initialization| initialization.claude_code_version.as_deref()),
            Some(QUALIFICATION_VERSION)
        );
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            decide(StepFacts {
                cancellation: None,
                own_failure: None,
                started: true,
                value_kind: AgentValueKind::Response,
                deciding_result: reader.deciding_result().cloned(),
                accepted: None,
                ending: Ending::Exited { success: true },
            })
        else {
            panic!("exact binary response must complete through the reader and the rule");
        };
        assert_eq!(response.as_str(), RESPONSE);
        provider.shutdown().await;
    })
    .await;
}

// REQ-AGENT-26
// REQ-AGENT-35
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_02_production_driver_returns_one_normalized_response() {
    // Production-adapter cases each need independent native process and provider state;
    // sharing their synthetic roots would invalidate same-process correction evidence.
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code production-driver", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let admitted_root = AdmittedExecutionRoot::admit(root.project()).unwrap();
        let working_directory = admitted_root.select_working_directory(None).unwrap();
        let observations = RecordingObservationSink::default();
        let png_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl2xZcAAAAASUVORK5CYII=";
        let png = BASE64.decode(png_base64).unwrap();
        let attachment_specs: [(&str, &[u8]); 4] = [
            ("text/plain", b"native text attachment"),
            ("image/png", png.as_slice()),
            ("application/pdf", b"%PDF-native"),
            ("application/octet-stream", &[0xde, 0xad]),
        ];
        // Use the same sealed sibling directories as production staging: the
        // adapter rejects attachments outside the admitted attachment root.
        let attachment_root = root.private().join("attachments");
        let result_endpoint = root.private().join("result-endpoint");
        fs::create_dir(&attachment_root).unwrap();
        fs::create_dir(&result_endpoint).unwrap();
        let attachments = attachment_specs
            .iter()
            .enumerate()
            .map(|(index, (media_type, bytes))| {
                let path = attachment_root.join(format!("{index:06}"));
                fs::write(&path, bytes).unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
                StagedAgentAttachment::new(
                    path,
                    Arc::from(*media_type),
                    Some(Arc::from("caller-controlled-name")),
                )
            })
            .collect::<Vec<_>>();
        let unsupported_sealed_path = attachments[3].path().to_str().unwrap().to_owned();
        let limits = conformance_limits();
        let value_mode = AgentValueMode::Response {
            output: Arc::from("response"),
        };
        let invocation = AgentInvocation::new(
            invocation_identity("claude-code-conformance", "agent"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, root.environment_snapshot(&provider)),
            AgentInvocationStaging::new(result_endpoint),
            AgentDiagnosticSession::claude_code_fixture(
                root.private().join("diagnostics/session"),
            ),
            AgentPrompt::new(
                Arc::from(fs::read_to_string(root.system_prompt()).unwrap()),
                Arc::from("Complete through the production Scherzo adapter."),
            ),
            Arc::from(attachments),
            value_mode.clone(),
            limits,
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        let diagnostics = StepDiagnosticLog::default();
        let adapter = ClaudeCodeStreamJsonV1Adapter::new_default(
            diagnostics.clone(),
            NonZeroU64::new(1024).unwrap(),
            PendingClock,
            NoopExecutionObserver,
        )
        .unwrap();
        let (started, start) = agent_start_channel();
        let (terminal, outcome) = tokio::sync::oneshot::channel();
        let execution = tokio::spawn(async move {
            let _ = terminal.send(adapter.invoke(invocation, started).await);
        });

        start.receive().await.unwrap();
        let request = provider.next_request().await;
        assert_eq!(request.path(), "/v1/messages?beta=true");
        assert!(request.used_placeholder_key());
        assert_eq!(request.body()["model"], MODEL);
        assert!(contains_exact_string(
            request.body(),
            "Scherzo attachment 000000 (text/plain) follows:\nnative text attachment"
        ));
        assert!(contains_exact_value(
            request.body(),
            &json!({"type": "base64", "media_type": "image/png", "data": png_base64})
        ));
        assert!(contains_exact_value(
            request.body(),
            &json!({
                "type": "base64",
                "media_type": "application/pdf",
                "data": "JVBERi1uYXRpdmU="
            })
        ));
        // The pinned native release appends one LF to this separate user text block.
        // Check the whole block: substring matching could hide a merged instruction.
        let sealed_reference = format!(
            "Scherzo attachment 000003 has media type application/octet-stream and is available to runner tools at {unsupported_sealed_path}.\n"
        );
        assert_eq!(request.body()["messages"][0]["role"], "user");
        let user_content = request.body()["messages"][0]["content"]
            .as_array()
            .unwrap();
        assert!(user_content.iter().any(|block| {
            block["type"] == "text" && block["text"].as_str() == Some(sealed_reference.as_str())
        }));
        assert!(!contains_string_fragment(
            request.body(),
            "caller-controlled-name"
        ));
        // The final message carries several text blocks around a reasoning block.
        request.release_blocks(vec![
            LoopbackBlock::text(FIRST_TEXT_BLOCK),
            LoopbackBlock::thinking(&["between the text blocks"]),
            LoopbackBlock::text(SECOND_TEXT_BLOCK),
        ]);
        // The adapter holds standard input open and no adapter deadline fires on the pending
        // clock, so the step can end only by Claude Code's own idle exit after the turn
        // (REQ-AGENT-35).
        execution.await.unwrap();

        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            outcome.await.unwrap()
        else {
            panic!("production exact-binary adapter must return one response outcome");
        };
        assert_eq!(response.as_str(), SEVERAL_BLOCK_RESPONSE);
        assert_eq!(
            observations.concatenated_text(assistant_text),
            format!("{FIRST_TEXT_BLOCK}{SECOND_TEXT_BLOCK}")
        );
        let diagnostic = diagnostics.get("agent").unwrap();
        assert!(diagnostic.standard_output().bytes().is_empty());
        assert!(!diagnostic.standard_error().bytes().is_empty());
        assert_eq!(diagnostic.standard_error().truncation(), None);
        assert!(diagnostic.standard_error().fully_drained());
        // Thinking makes Claude Code interleave `system/thinking_tokens` estimates, which the
        // reader leaves unrecognized; nothing else in the exchange is.
        let unrecognized = observations
            .snapshot()
            .iter()
            .filter_map(|observation| match observation.observation() {
                AgentObservation::UnrecognizedHarnessEvent { event }
                    if event["subtype"] != "thinking_tokens" =>
                {
                    Some(Arc::clone(event))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(unrecognized.is_empty(), "{unrecognized:?}");
        assert!(!provider.has_pending_request());
        assert_retained_native_session(
            &root,
            &[
                "Complete through the production Scherzo adapter.",
                FIRST_TEXT_BLOCK,
                SECOND_TEXT_BLOCK,
            ],
        );
        provider.shutdown().await;
    })
    .await;
}

// SC-AGENT-3
// REQ-AGENT-84
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_03_result_tool_corrects_a_rejected_value_in_one_turn() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code result correction", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let observations = RecordingObservationSink::default();
        let running = RunningProductionClaudeCode::launch(
            executable,
            &root,
            &provider,
            result_mode(positive_integer_schema()),
            observations.clone(),
        )
        .await_started()
        .await;
        let mut terminal = running.terminal;

        let first = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(first.offers_result_tool());
        first.release_submission(json!(0));
        let correction = next_request_or_outcome(&mut provider, &mut terminal).await;
        let (rejected, feedback) = correction.tool_answer();
        assert!(rejected, "the rejected submission was answered as accepted");
        // The corrected value follows a reasoning block, as a model with extended thinking
        // submits it.
        correction.release_blocks(vec![
            LoopbackBlock::thinking(&["correcting the result"]),
            LoopbackBlock::tool_use(RESULT_TOOL, submission(json!(7))),
        ]);
        let accepted = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(
            !accepted.tool_answer().0,
            "the valid submission was rejected"
        );
        accepted.release_text("submitted");
        let outcome = terminal.await.unwrap();
        running.task.await.unwrap();

        assert_eq!(result_value(outcome), json!(7));
        let rejections = value_rejections(&observations);
        assert_eq!(rejections.len(), 1, "{rejections:?}");
        // Claude Code trims the answer's trailing newline.
        assert_eq!(feedback.trim_end(), rejections[0].trim_end());
        // One `system/init` for the whole exchange: the correction stays in the turn.
        assert_eq!(
            milestones(&observations, AgentLifecycleMilestone::SessionEstablished),
            1
        );
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &["Complete the controlled lifecycle exchange."]);
        provider.shutdown().await;
    })
    .await;
}

// SC-AGENT-7
// REQ-AGENT-35
// REQ-AGENT-64
// REQ-AGENT-85
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_03b_the_last_submission_after_a_backgrounded_subagent_stands() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog(
        "pinned Claude Code backgrounded-subagent result",
        WATCHDOG,
        async {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let subagent = BackgroundSubagent::new(&root);
            let observations = RecordingObservationSink::default();
            let running = RunningProductionClaudeCode::launch(
                executable,
                &root,
                &provider,
                result_mode(positive_integer_schema()),
                observations.clone(),
            )
            .await_started()
            .await;
            let mut terminal = running.terminal;

            // A is submitted while the subagent runs, and the agent ends its turn.
            subagent
                .start(&mut provider, &mut terminal)
                .await
                .release_submission(json!(3));
            let answered = next_request_or_outcome(&mut provider, &mut terminal).await;
            assert!(!answered.tool_answer().0, "A was rejected");
            answered.release_text("waiting for the subagent");
            wait_for_turn_completed(&observations).await;
            assert!(matches!(
                terminal.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));

            // The subagent's end begins a turn, in which B is submitted.
            subagent
                .finish(&mut provider, &mut terminal)
                .await
                .release_submission(json!(7));
            let answered = next_request_or_outcome(&mut provider, &mut terminal).await;
            assert!(!answered.tool_answer().0, "B was rejected");
            answered.release_text("final response");
            let outcome = terminal.await.unwrap();
            running.task.await.unwrap();

            assert_eq!(result_value(outcome), json!(7));
            assert_backgrounded_subagent_activity(&observations);
            assert!(!provider.has_pending_request());
            assert_retained_native_session(&root, &["waiting for the subagent", "final response"]);
            provider.shutdown().await;
        },
    )
    .await;
}

// REQ-AGENT-32
// REQ-AGENT-84
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_03c_a_string_and_a_null_come_back_as_written() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    // Two launches, each ending by the idle exit.
    host_scaled_watchdog("pinned Claude Code value fidelity", WATCHDOG * 2, async {
        for (schema, value) in [("string", json!("7")), ("null", Value::Null)] {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let running = RunningProductionClaudeCode::launch(
                executable.clone(),
                &root,
                &provider,
                result_mode(type_schema(schema)),
                RecordingObservationSink::default(),
            )
            .await_started()
            .await;
            let mut terminal = running.terminal;
            next_request_or_outcome(&mut provider, &mut terminal)
                .await
                .release_submission(value.clone());
            let answered = next_request_or_outcome(&mut provider, &mut terminal).await;
            assert!(
                !answered.tool_answer().0,
                "the {schema} submission was rejected"
            );
            answered.release_text("submitted");
            let outcome = terminal.await.unwrap();
            running.task.await.unwrap();
            assert_eq!(result_value(outcome), value, "{schema}");
            assert!(!provider.has_pending_request());
            provider.shutdown().await;
        }
    })
    .await;
}

// REQ-AGENT-35
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_03d_a_main_thread_background_command_begins_the_submitting_turn() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code background command", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let ready = FixtureSignal::create(root.private().join("background-ready"));
        let blocker = root.private().join("background-blocker");
        mkfifo(&blocker, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let operations = root.private().join("sandbox-operations");
        fs::create_dir_all(operations.join("empty")).unwrap();
        fs::write(operations.join("edit.txt"), b"before\n").unwrap();
        fs::write(operations.join("remove.txt"), b"remove me\n").unwrap();
        // The synthetic root's /usr/bin:/bin PATH has no coreutils on NixOS.
        // Supply only this command's pinned tools without changing fixture isolation.
        let tools_path = std::env::var("USEFUL_CLAUDE_CODE_CONFORMANCE_TOOLS_PATH")
            .expect("CLI Devenv must supply the native conformance tools");
        let observations = RecordingObservationSink::default();
        let running = RunningProductionClaudeCode::launch(
            executable,
            &root,
            &provider,
            result_mode(positive_integer_schema()),
            observations.clone(),
        )
        .await_started()
        .await;
        let mut terminal = running.terminal;
        next_request_or_outcome(&mut provider, &mut terminal).await.release_tool_use(
            "Bash",
            json!({
                "command": format!(
                    "export PATH='{tools_path}'; printf 'ready\\n' > '{}' && IFS= read -r done < '{}' && sed -i 's/before/after/' '{}/edit.txt' && rm '{}/remove.txt' && rmdir '{}/empty' && printf 'background finished\\n'",
                    ready.path().display(), blocker.display(), operations.display(), operations.display(), operations.display()
                ),
                "description": "Run a controlled background command",
                "run_in_background": true,
            }),
        );
        // The tool returns a task identity before its child is released, and the agent
        // ends its turn without a submission.
        let next = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert_eq!(ready.receive().await, b"ready\n");
        next.release_text("first response");
        wait_for_turn_completed(&observations).await;
        assert!(matches!(
            terminal.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        tokio::task::spawn_blocking(move || fs::write(blocker, b"done\n").unwrap())
            .await
            .unwrap();

        // The command's end begins the turn in which the agent submits.
        next_request_or_outcome(&mut provider, &mut terminal)
            .await
            .release_submission(json!(7));
        let answered = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(!answered.tool_answer().0, "the submission was rejected");
        answered.release_text("final response");
        let outcome = terminal.await.unwrap();
        running.task.await.unwrap();

        assert_eq!(result_value(outcome), json!(7));
        assert_eq!(fs::read(operations.join("edit.txt")).unwrap(), b"after\n");
        assert!(!operations.join("remove.txt").exists());
        assert!(!operations.join("empty").exists());
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &["first response", "final response"]);
        provider.shutdown().await;
    })
    .await;
}

// SC-AGENT-8
// REQ-AGENT-35
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_03e_a_subagents_own_background_command_is_waited_for() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog(
        "pinned Claude Code subagent background command",
        WATCHDOG,
        async {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let tests = BackgroundSubagent::named(&root, "subagent-tests");
            let observations = RecordingObservationSink::default();
            let running = RunningProductionClaudeCode::launch(
                executable,
                &root,
                &provider,
                result_mode(positive_integer_schema()),
                observations.clone(),
            )
            .await_started()
            .await;
            let mut terminal = running.terminal;

            // The agent launches a subagent in the background, which starts its tests as a
            // background command and ends. 2.1.321 stops a foreground subagent's background
            // command when the subagent ends, so only a backgrounded one outlives it.
            next_request_or_outcome(&mut provider, &mut terminal)
                .await
                .release_tool_use(
                    "Agent",
                    json!({
                        "description": TESTING_SUBAGENT_DESCRIPTION,
                        "prompt": TESTING_SUBAGENT_PROMPT,
                        "subagent_type": "general-purpose",
                        "run_in_background": true,
                    }),
                );
            // The agent's turn is held until the subagent has reported, so that its report
            // reaches the agent in a turn of its own.
            let mut held_turn = None;
            let mut command_started = false;
            let mut reported = false;
            while held_turn.is_none() || !reported {
                let request = next_request_or_outcome(&mut provider, &mut terminal).await;
                if !request.opens_with(TESTING_SUBAGENT_PROMPT) {
                    assert!(held_turn.is_none(), "the agent asked twice in its turn");
                    held_turn = Some(request);
                } else if command_started {
                    tests.await_ready().await;
                    request.release_text("the tests are running in the background");
                    reported = true;
                } else {
                    let mut command = tests.blocking_command();
                    command["run_in_background"] = json!(true);
                    request.release_tool_use("Bash", command);
                    command_started = true;
                }
            }
            held_turn.unwrap().release_text("waiting for the subagent");

            // The subagent's report begins a turn, which the agent ends without a submission;
            // Claude Code keeps running for the subagent's command.
            let woken = next_request_or_outcome(&mut provider, &mut terminal).await;
            assert!(!woken.opens_with(TESTING_SUBAGENT_PROMPT));
            woken.release_text("still waiting for the tests");

            // The command's end resumes the subagent, whose report wakes the agent.
            tests.release();
            let mut resumed = false;
            let woken = loop {
                let request = next_request_or_outcome(&mut provider, &mut terminal).await;
                if !request.opens_with(TESTING_SUBAGENT_PROMPT) {
                    break request;
                }
                request.release_text("the tests passed");
                resumed = true;
            };
            assert!(
                resumed,
                "the agent was woken before the subagent was resumed"
            );
            woken.release_submission(json!(7));
            let answered = next_request_or_outcome(&mut provider, &mut terminal).await;
            assert!(!answered.tool_answer().0, "the submission was rejected");
            answered.release_text("final response");
            let outcome = terminal.await.unwrap();
            running.task.await.unwrap();

            assert_eq!(result_value(outcome), json!(7));
            assert!(!provider.has_pending_request());
            assert_retained_native_session(
                &root,
                &["still waiting for the tests", "final response"],
            );
            provider.shutdown().await;
        },
    )
    .await;
}

// REQ-AGENT-31
// REQ-AGENT-84
// REQ-AGENT-86
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_03f_a_turn_without_a_submission_fails_missing_result() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code missing result", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let running = RunningProductionClaudeCode::launch(
            executable,
            &root,
            &provider,
            result_mode(positive_integer_schema()),
            RecordingObservationSink::default(),
        )
        .await_started()
        .await;
        let mut terminal = running.terminal;
        next_request_or_outcome(&mut provider, &mut terminal)
            .await
            .release_text(RESPONSE);
        // Nothing reminds the agent: the provider sees no further request before the exit.
        let outcome = tokio::select! {
            outcome = &mut terminal => outcome.unwrap(),
            request = provider.next_request() => {
                panic!("Claude Code asked again after a turn without a submission: {}", request.body())
            }
        };
        running.task.await.unwrap();
        assert_eq!(outcome, failed_agent_outcome(AgentFailureCause::MissingResult));
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &[RESPONSE]);
        provider.shutdown().await;
    })
    .await;
}

// REQ-AGENT-70
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_04_production_no_value_and_native_failure_are_typed() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    // Two launches, each ending by Claude Code's own exit.
    host_scaled_watchdog("pinned Claude Code no-value/failure", WATCHDOG * 2, async {
        let mut success_provider = LoopbackProvider::start().await;
        let success_root = SyntheticClaudeCodeRoot::new();
        let success = RunningProductionClaudeCode::launch(
            executable.clone(),
            &success_root,
            &success_provider,
            AgentValueMode::None,
            RecordingObservationSink::default(),
        )
        .await_started()
        .await;
        success_provider.next_request().await.release_text(RESPONSE);
        assert_eq!(
            success.finish().await,
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue)
        );
        assert!(!success_provider.has_pending_request());
        assert_retained_native_session(&success_root, &[RESPONSE]);
        success_provider.shutdown().await;

        let mut failure_provider = LoopbackProvider::start().await;
        let failure_root = SyntheticClaudeCodeRoot::new();
        let failure = RunningProductionClaudeCode::launch(
            executable,
            &failure_root,
            &failure_provider,
            AgentValueMode::None,
            RecordingObservationSink::default(),
        )
        .await_started()
        .await;
        failure_provider
            .next_request()
            .await
            .release_invalid_request();
        // Control every bounded native recovery attempt; the exact retry count
        // is not part of the profile's typed terminal-failure contract.
        let finish = failure.finish();
        tokio::pin!(finish);
        let mut attempts = 1;
        let outcome = loop {
            tokio::select! {
                outcome = &mut finish => break outcome,
                retry = failure_provider.next_request() => {
                    attempts += 1;
                    assert!(attempts <= 8, "native invalid-request recovery must be bounded");
                    assert!(retry.used_placeholder_key());
                    assert!(retry.body()["messages"].is_array());
                    retry.release_invalid_request();
                }
            }
        };
        let AgentOutcome::Failed(failure) = &outcome else {
            panic!("a native model error must fail the step, got {outcome:?}");
        };
        let AgentFailureCause::HarnessFailed {
            detail:
                AgentHarnessFailureDetail::Classified {
                    canonical: CanonicalHarnessFailure::ModelError,
                    diagnostic,
                },
        } = failure.cause()
        else {
            panic!("a native model error must be classified, got {outcome:?}");
        };
        // 2.1.321 names the class of a 400 `invalid_request_error` `unknown` in its api-error
        // message and the turn's `terminal_reason` `api_error`.
        assert_eq!(diagnostic.harness_error, Some("other"), "{outcome:?}");
        assert_eq!(diagnostic.http_status, Some(400), "{outcome:?}");
        assert_eq!(diagnostic.resets_at, None);
        assert!(!failure_provider.has_pending_request());
        assert_retained_native_session(
            &failure_root,
            &["Complete the controlled lifecycle exchange."],
        );
        failure_provider.shutdown().await;
    })
    .await;
}

// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_04b_provider_overload_and_stream_error_are_retried() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    // Two launches, each ending by Claude Code's own exit.
    host_scaled_watchdog("pinned Claude Code overload probe", WATCHDOG * 2, async {
        // A stream error after thinking, before the message ends, is retried by Claude
        // Code: the damaged exchange is no protocol failure, and the retry's answer is the
        // response.
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable.clone(), &root, &provider, &observations)
                .await;
        let mut terminal = running.terminal;
        next_request_or_outcome(&mut provider, &mut terminal)
            .await
            .release_overload_after_thinking();
        let retry = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(retry.used_placeholder_key());
        retry.release_text(RESPONSE);
        let outcome = terminal.await.unwrap();
        running.task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
            panic!("a retried stream error must produce the retry's response, got {outcome:?}");
        };
        assert_eq!(response.as_str(), RESPONSE);
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &[RESPONSE]);
        provider.shutdown().await;

        // An overload before stream initialization is retried the same way.
        let mut retry_provider = LoopbackProvider::start().await;
        let retry_root = SyntheticClaudeCodeRoot::new();
        let retry_observations = RecordingObservationSink::default();
        let retry_running = launch_recorded_response_lifecycle(
            executable,
            &retry_root,
            &retry_provider,
            &retry_observations,
        )
        .await;
        let mut terminal = retry_running.terminal;
        next_request_or_outcome(&mut retry_provider, &mut terminal)
            .await
            .release_overload();
        let request = next_request_or_outcome(&mut retry_provider, &mut terminal).await;
        assert!(request.used_placeholder_key());
        request.release_text(RESPONSE);
        let retry_outcome = terminal.await.unwrap();
        retry_running.task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = retry_outcome
        else {
            panic!("retry must produce one response, got {retry_outcome:?}");
        };
        assert_eq!(response.as_str(), RESPONSE);
        assert_eq!(
            retry_observations.concatenated_text(assistant_text),
            RESPONSE
        );
        assert!(!retry_provider.has_pending_request());
        assert_retained_native_session(&retry_root, &[RESPONSE]);
        retry_provider.shutdown().await;
    })
    .await;
}

// INV-HARNESS-8
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_05_cancels_a_blocked_provider_request() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog(
        "pinned Claude Code blocked-provider cancellation",
        WATCHDOG,
        async {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let running = launch_response_lifecycle(executable, &root, &provider).await;
            let blocked_request = provider.next_request().await;

            assert!(
                running
                    .cancellation
                    .request_cancellation(CancellationReason::UserRequest)
            );
            running.process_control.interrupt();
            let process_group = running.process_group.clone();
            assert_user_cancelled(running).await;
            assert!(process_group_is_quiescent(process_group.get()));
            drop(blocked_request);
            assert!(!provider.has_pending_request());
            assert_retained_native_session(&root, &["Complete the controlled lifecycle exchange."]);
            provider.shutdown().await;
        },
    )
    .await;
}

// INV-HARNESS-8
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06_cancels_a_stubborn_bash_descendant() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    #[cfg(target_os = "linux")]
    nix::sys::prctl::set_child_subreaper(true).unwrap();
    host_scaled_watchdog(
        "pinned Claude Code stubborn-child cancellation",
        WATCHDOG,
        async {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let child = FixtureSignal::create(root.private().join("stubborn-child.pid"));
            let blocker = root.private().join("stubborn-child.blocker");
            mkfifo(&blocker, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
            let running = launch_response_lifecycle(executable, &root, &provider).await;
            let command = format!(
                "trap '' INT TERM; printf '%s\\n' \"$$\" > '{}'; IFS= read -r unexpected < '{}'",
                child.path().display(),
                blocker.display()
            );
            provider.next_request().await.release_tool_use(
                "Bash",
                json!({
                    "command": command,
                    "description": "Run a controlled stubborn descendant",
                }),
            );
            let child = process_id(&child.receive().await);
            let child_group = getpgid(Some(child)).unwrap();

            assert!(
                running
                    .cancellation
                    .request_cancellation(CancellationReason::UserRequest)
            );
            running.process_control.interrupt();
            let process_group = running.process_group.clone();

            assert_user_cancelled(running).await;
            assert!(process_group_is_quiescent(process_group.get()));
            assert!(process_group_is_quiescent(child_group));
            assert!(!provider.has_pending_request());
            assert_retained_native_session(&root, &["Complete the controlled lifecycle exchange."]);
            provider.shutdown().await;
        },
    )
    .await;
}

// INV-HARNESS-8
// INV-HARNESS-25
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06b_headless_term_and_cont_quiesce_owned_work() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    nix::sys::prctl::set_child_subreaper(true).unwrap();
    host_scaled_watchdog("pinned Claude Code headless termination", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let child = FixtureSignal::create(root.private().join("term-child.pid"));
        let blocker = root.private().join("term-child.blocker");
        mkfifo(&blocker, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let native_payload = executable
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("libexec/claude-code/claude");
        let running = launch_response_lifecycle(executable, &root, &provider).await;
        provider.next_request().await.release_tool_use(
            "Bash",
            json!({
                "command": format!(
                    "printf '%s\\n' \"$$\" > '{}'; IFS= read -r unexpected < '{}'",
                    child.path().display(), blocker.display()
                ),
                "description": "Block owned work for a headless termination probe",
            }),
        );
        let child = process_id(&child.receive().await);
        let child_group = getpgid(Some(child)).unwrap();
        let status = fs::read_to_string(format!("/proc/{}/status", child.as_raw_pid())).unwrap();
        let parent: i32 = status
            .lines()
            .find_map(|line| line.strip_prefix("PPid:\t"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let cmdline = fs::read(format!("/proc/{parent}/cmdline")).unwrap();
        assert!(
            cmdline
                .split(|byte| *byte == 0)
                .any(|arg| arg == native_payload.as_os_str().as_encoded_bytes())
        );
        // Keep both signals bound to the same process even if TERM exits it before
        // CONT can be delivered. The owned child is ready before either signal.
        let native = pidfd_open(Pid::from_raw(parent).unwrap(), PidfdFlags::empty()).unwrap();
        assert_eq!(
            fs::read(format!("/proc/{parent}/cmdline")).unwrap(),
            cmdline
        );
        pidfd_send_signal(&native, Signal::TERM).unwrap();
        match pidfd_send_signal(&native, Signal::CONT) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => {}
            Err(error) => panic!("native SIGCONT failed: {error}"),
        }
        let process_group = running.process_group.clone();
        let outcome = running.finish().await;
        // Terminated in its turn, Claude Code reported no successful end of one.
        assert_eq!(
            outcome,
            failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            })
        );
        assert!(process_group_is_quiescent(process_group.get()));
        assert!(process_group_is_quiescent(child_group));
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &["Complete the controlled lifecycle exchange."]);
        provider.shutdown().await;
    })
    .await;
}

// REQ-AGENT-86
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06d_prompt_hook_deny_prevents_provider_request() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("native prompt-hook deny", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let hook = root.private().join("deny-prompt.sh");
        let observed = root.private().join("prompt-denied");
        fs::write(&hook, format!("printf denied > '{}'; exit 2\n", observed.display())).unwrap();
        let settings = root.project().join(".claude/settings.json");
        fs::create_dir_all(settings.parent().unwrap()).unwrap();
        fs::write(settings, serde_json::to_vec(&json!({
            "hooks": {"UserPromptSubmit": [{"hooks": [{"type": "command",
                "command": format!("{} {}", std::env::var("USEFUL_CLAUDE_CODE_CONFORMANCE_SHELL").unwrap(), hook.display())
            }]}]}
        })).unwrap()).unwrap();
        let running = RunningProductionClaudeCode::launch(
            executable, &root, &provider, AgentValueMode::None,
            RecordingObservationSink::default(),
        );
        let mut terminal = running.terminal;
        let outcome = tokio::select! {
            outcome = &mut terminal => outcome.unwrap(),
            _ = provider.next_request() => panic!("denied prompt reached provider"),
        };
        running.task.await.unwrap();
        // The blocked prompt closes a turn without calling the model, which decides
        // nothing, and Claude Code exits 0 by its idle exit.
        assert_eq!(
            outcome,
            failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            })
        );
        assert_eq!(fs::read(observed).unwrap(), b"denied");
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    }).await;
}

// REQ-AGENT-88
// INV-HARNESS-25
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_07_the_quiet_bound_interrupts_a_held_claude_code() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code quiet bound", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let observations = RecordingObservationSink::default();
        let (clock, mut control) = StagedClock::new();
        let running = RunningProductionClaudeCode::launch_on(
            executable,
            &root,
            &provider,
            AgentValueMode::None,
            observations.clone(),
            clock,
        )
        .await_started()
        .await;
        // Claude Code is held on a provider request nobody answers.
        let held = provider.next_request().await;
        control.release(QUIET_BOUND);
        // SIGINT ends it: the forced stop 30 seconds later is armed and never released.
        control.registered(QUIET_BOUND_FORCE_DELAY).await;
        let process_group = running.process_group.clone();
        let outcome = running.finish().await;
        assert_eq!(
            outcome,
            failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::UnsuccessfulExit,
            })
        );
        assert!(process_group_is_quiescent(process_group.get()));
        assert_eq!(adapter_warnings(&observations).len(), 1);
        drop(held);
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await;
}

fn result_value(outcome: AgentOutcome) -> Value {
    let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
        panic!("the step must return its result, got {outcome:?}");
    };
    result.value().clone()
}

fn value_rejections(observations: &RecordingObservationSink) -> Vec<Arc<str>> {
    observations
        .snapshot()
        .iter()
        .filter_map(|envelope| match envelope.observation() {
            AgentObservation::ValueRejected {
                kind: AgentValueKind::Result,
                feedback,
            } => Some(Arc::clone(feedback)),
            _ => None,
        })
        .collect()
}

fn milestones(
    observations: &RecordingObservationSink,
    milestone: AgentLifecycleMilestone,
) -> usize {
    observations
        .snapshot()
        .iter()
        .filter(|envelope| {
            matches!(envelope.observation(), AgentObservation::Lifecycle { milestone: observed } if *observed == milestone)
        })
        .count()
}

fn contains_exact_value(value: &Value, expected: &Value) -> bool {
    value == expected
        || match value {
            Value::Array(values) => values
                .iter()
                .any(|value| contains_exact_value(value, expected)),
            Value::Object(object) => object
                .values()
                .any(|value| contains_exact_value(value, expected)),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
        }
}

fn contains_string_fragment(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value.contains(expected),
        Value::Array(values) => values
            .iter()
            .any(|value| contains_string_fragment(value, expected)),
        Value::Object(object) => object
            .values()
            .any(|value| contains_string_fragment(value, expected)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn contains_exact_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values
            .iter()
            .any(|value| contains_exact_string(value, expected)),
        Value::Object(object) => object
            .values()
            .any(|value| contains_exact_string(value, expected)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}
