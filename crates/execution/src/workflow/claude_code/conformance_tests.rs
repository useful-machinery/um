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
use super::adapter::{ClaudeCodeStreamJsonV1Adapter, premature_candidate_feedback};
use super::lifecycle_matrix::{self, enforcement_nags, tool_names};
use super::test_support::{
    BACKGROUND_SUBAGENT_DESCRIPTION, BACKGROUND_SUBAGENT_PROMPT, BACKGROUND_SUBAGENT_RESPONSE,
    BackgroundSubagent, FixtureSignal, LoopbackBlock, LoopbackProvider, MODEL, PendingClock,
    RecordingObservationSink, RunningProductionClaudeCode, RunningStartedClaudeCode,
    SUBAGENT_OUTSTANDING, SyntheticClaudeCodeRoot, WATCHDOG, admitted_adapter,
    assert_backgrounded_subagent_activity, assistant_text, backgrounded_subagent_statuses,
    conformance_executable, conformance_limits, exclusive_conformance_executable,
    invocation_identity, launched_agent_id, next_request_or_outcome, version_probe_environment,
    wait_for_completed_turns,
};
use super::*;
use crate::workflow::admission::{CancellationReason, CancellationSource};
use crate::workflow::agent::{
    AgentAdapter, AgentInvocation, AgentInvocationStaging, AgentProcessContext, AgentPrompt,
    AgentValueMode, StagedAgentAttachment, agent_start_channel,
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
const DIRECT_SESSION_ID: &str = "00000000-0000-4000-8000-000000000001";

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

/// A Result-mode launch with the lifecycle matrix's schema, which any candidate is checked
/// against and a positive integer satisfies.
async fn launch_recorded_result_lifecycle(
    executable: PathBuf,
    root: &SyntheticClaudeCodeRoot,
    provider: &LoopbackProvider,
    observations: &RecordingObservationSink,
) -> RunningStartedClaudeCode {
    RunningProductionClaudeCode::launch(
        executable,
        root,
        provider,
        lifecycle_matrix::value_mode(lifecycle_matrix::Mode::Result),
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
    let ambient_project = ambient_paths[0].parent().unwrap();
    assert_eq!(
        fs::read_dir(ambient_project).unwrap().count(),
        0,
        "ambient project history retained an unrelated entry"
    );
}

// REQ-AGENT-4
// INV-HARNESS-10
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

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_01_normal_mode_loopback_conforms_from_a_synthetic_root() {
    // Every exact-binary case deliberately owns a fresh watchdog, loopback provider, and
    // synthetic root; sharing those resources would let one native case contaminate another.
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code conformance", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let resources = root.install_native_resource_fixture(
            NATIVE_INSTRUCTION_MARKER,
            NATIVE_SKILL_MARKER,
            NATIVE_PLUGIN_MARKER,
            NATIVE_MCP_MARKER,
        );
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
                "command": format!("{} {}", std::env::var("UM_CLAUDE_CODE_CONFORMANCE_SHELL").unwrap(), concurrent_hook.display())
            }));
        fs::write(settings_path, serde_json::to_vec(&settings).unwrap()).unwrap();
        let expected_cwd = fs::canonicalize(root.project()).unwrap();
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
        assert!(mcp_observation.starts_with("started\n"));
        assert!(mcp_observation.contains("tools/list"));
        // Deferred tool selection precedes the MCP tool call. The server issues an
        // elicitation that the headless client cannot answer; the native timeout
        // must remain an ordinary tool error, not a protocol or session failure.
        request.release_tool_use("ToolSearch", json!({"query": "scherzo_native_resource"}));
        let request = provider.next_request().await;
        assert!(contains_string_fragment(request.body(), NATIVE_MCP_MARKER));
        request.release_tool_use("mcp__scherzo-resource__scherzo_native_resource", json!({}));
        provider.next_request().await.release_text(RESPONSE);

        let output = child.wait_with_output().await.unwrap();
        let mcp_observation = fs::read_to_string(&resources.mcp_observation).unwrap();
        assert!(mcp_observation.contains("\"method\":\"tools/call\""));
        assert!(mcp_observation.contains("\"method\":\"notifications/cancelled\""));
        assert!(output.status.success());
        // The pinned release diagnoses the synthetic model on stderr. Its bytes are an
        // actionable process diagnostic, not protocol, and their wording is not a contract.
        assert!(!output.stderr.is_empty());
        assert!(!provider.has_pending_request());

        let mut parser = ClaudeCodeStreamJsonV1Parser::profile(
            Arc::from(expected_cwd.to_str().unwrap()),
            Arc::from(MODEL),
            Arc::from(DIRECT_SESSION_ID),
            Arc::from(QUALIFICATION_VERSION),
            crate::workflow::agent::AgentValueKind::Response,
            NonZeroU64::new(1024).unwrap(),
        );
        let mut native_observations = Vec::new();
        for chunk in output.stdout.chunks(7) {
            parser
                .push_stdout(chunk, |observation| native_observations.push(observation))
                .unwrap();
        }
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
        assert!(parser.session_id().is_some());
        assert_eq!(parser.completed_exchanges(), 1);
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            parser.finish(true)
        else {
            panic!("exact binary response must complete through the production parser");
        };
        assert_eq!(response.as_str(), RESPONSE);
        provider.shutdown().await;
    })
    .await;
}

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
        request.release_text(RESPONSE);
        execution.await.unwrap();

        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            outcome.await.unwrap()
        else {
            panic!("production exact-binary adapter must return one response outcome");
        };
        assert_eq!(response.as_str(), RESPONSE);
        assert_eq!(observations.concatenated_text(assistant_text), RESPONSE);
        let diagnostic = diagnostics.get("agent").unwrap();
        assert!(diagnostic.standard_output().bytes().is_empty());
        assert!(!diagnostic.standard_error().bytes().is_empty());
        assert_eq!(diagnostic.standard_error().truncation(), None);
        assert!(diagnostic.standard_error().fully_drained());
        assert!(!observations.snapshot().iter().any(|observation| matches!(
            observation.observation(),
            AgentObservation::UnrecognizedHarnessEvent { .. }
        )));
        assert!(!provider.has_pending_request());
        assert_retained_native_session(
            &root,
            &["Complete through the production Scherzo adapter.", RESPONSE],
        );
        provider.shutdown().await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_03_corrects_a_result_in_one_production_conversation() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code result-correction", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let admitted_root = AdmittedExecutionRoot::admit(root.project()).unwrap();
        let working_directory = admitted_root.select_working_directory(None).unwrap();
        let value_mode = lifecycle_matrix::value_mode(lifecycle_matrix::Mode::Result);
        let limits = conformance_limits();
        let observations = RecordingObservationSink::default();
        let invocation = AgentInvocation::new(
            invocation_identity("claude-code-result-conformance", "agent"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, root.environment_snapshot(&provider)),
            AgentInvocationStaging::new(root.private().to_owned()),
            AgentDiagnosticSession::claude_code_fixture(root.private().join("diagnostics/session")),
            AgentPrompt::new(
                Arc::from(fs::read_to_string(root.system_prompt()).unwrap()),
                Arc::from("Return the requested structured result."),
            ),
            Arc::from([]),
            value_mode.clone(),
            limits,
            CancellationSource::new(),
            ProcessGuardRegistry::default(),
            observations.clone(),
        );
        let adapter = ClaudeCodeStreamJsonV1Adapter::with_worker(
            StepDiagnosticLog::default(),
            NonZeroU64::new(1024).unwrap(),
            PendingClock,
            NoopExecutionObserver,
            super::adapter_tests::InlineValidationWorker,
        );
        let (started, start) = agent_start_channel();
        let (terminal, outcome) = tokio::sync::oneshot::channel();
        let mut execution = tokio::spawn(async move {
            let _ = terminal.send(adapter.invoke(invocation, started).await);
        });

        start.receive().await.unwrap();
        let first = provider.next_request().await;
        assert!(contains_exact_string(first.body(), "StructuredOutput"));
        first.release_structured_output(json!({"result": -1}));

        let second = tokio::select! {
            request = provider.next_request() => request,
            joined = &mut execution => {
                joined.unwrap();
                panic!("adapter ended before correction request: {:?}", outcome.await);
            }
        };
        assert!(contains_exact_value(second.body(), &json!({"result": -1})));
        assert!(contains_string_fragment(
            second.body(),
            "Result rejected by the workflow schema:"
        ));
        // The corrected candidate follows a thinking block, as a Claude model with extended
        // thinking submits it; reaching the accepted result proves a thinking sibling leaves
        // the candidate standalone on the exact binary.
        second.release_blocks(vec![
            LoopbackBlock::thinking(&["correcting the result "]),
            LoopbackBlock::tool_use(STRUCTURED_OUTPUT_TOOL_NAME, json!({"result": 7})),
        ]);
        execution.await.unwrap();

        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) =
            outcome.await.unwrap()
        else {
            panic!("exact binary corrected result must complete");
        };
        assert_eq!(result.value(), &json!(7));
        let observations = observations.snapshot();
        assert_eq!(
            observations
                .iter()
                .filter(|observation| matches!(
                    observation.observation(),
                    AgentObservation::ValueRejected {
                        kind: AgentValueKind::Result,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(observations.iter().any(|observation| matches!(
            observation.observation(),
            AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessQuiescent,
            }
        )));
        assert!(!provider.has_pending_request());
        assert_retained_native_session(
            &root,
            &[
                "Return the requested structured result.",
                "Result rejected by the workflow schema:",
            ],
        );
        provider.shutdown().await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_04_production_no_value_and_native_failure_are_typed() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code no-value/failure", WATCHDOG, async {
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
        assert_eq!(
            outcome,
            failed_agent_outcome(AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::ModelError,
            })
        );
        assert!(!failure_provider.has_pending_request());
        assert_retained_native_session(
            &failure_root,
            &["Complete the controlled lifecycle exchange."],
        );
        failure_provider.shutdown().await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_04b_provider_overload_retries_and_stream_error_fails_closed() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code overload probe", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;
        provider
            .next_request()
            .await
            .release_overload_after_thinking();
        let outcome = running.finish().await;
        let AgentOutcome::Failed(failure) = outcome else {
            panic!("damaged native exchange must fail closed, got {outcome:?}");
        };
        assert_eq!(failure.cause(), &AgentFailureCause::HarnessProtocolFailed);
        let rejection = serde_json::to_value(failure.protocol_rejection().unwrap()).unwrap();
        assert_eq!(rejection["detail"]["reason"], "message_transition_invalid");
        assert!(observations.concatenated_text(assistant_text).is_empty());
        assert!(!provider.has_pending_request());
        // Native failure before transcript creation is allowed, but the owned links
        // must never leave an ambient resumable conversation behind.
        let ambient_paths = root.ambient_session_paths(DIRECT_SESSION_ID);
        for ambient in &ambient_paths {
            assert!(!ambient.exists());
        }
        assert_eq!(
            fs::read_dir(ambient_paths[0].parent().unwrap())
                .unwrap()
                .count(),
            0
        );
        provider.shutdown().await;

        // An overload before stream initialization can be retried by Claude Code;
        // the adapter still accepts only the final completed exchange as response.
        let mut retry_provider = LoopbackProvider::start().await;
        let retry_root = SyntheticClaudeCodeRoot::new();
        let retry_observations = RecordingObservationSink::default();
        let retry_running = launch_recorded_response_lifecycle(
            conformance_executable(),
            &retry_root,
            &retry_provider,
            &retry_observations,
        )
        .await;
        retry_provider.next_request().await.release_overload();
        let terminal = retry_running.terminal;
        tokio::pin!(terminal);
        let retry_outcome = tokio::select! {
            request = retry_provider.next_request() => {
                assert!(request.used_placeholder_key());
                request.release_text(RESPONSE);
                terminal.await.unwrap()
            }
            outcome = &mut terminal => outcome.unwrap(),
        };
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

// SC-AGENT-7
// REQ-AGENT-75
// REQ-AGENT-70
// UM-3076: a native failure with a backgrounded subagent outstanding. In Result mode the
// lead launches a builder in the background and its own next request fails natively; the
// adapter closes stdin, the builder runs on, and the turn its notification begins ends the
// step with the classified model error, not with a rejection of that turn's init.
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_04c_native_failure_with_a_backgrounded_subagent_outstanding_is_the_model_error()
 {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog(
        "pinned Claude Code native-failure-with-subagent",
        WATCHDOG,
        async {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let subagent = BackgroundSubagent::new(&root);
            let observations = RecordingObservationSink::default();
            let running =
                launch_recorded_result_lifecycle(executable, &root, &provider, &observations).await;
            let mut terminal = running.terminal;

            // The main thread's request after the launch is rejected by the provider, as is
            // every bounded native retry (as in `04`), until the turn ends as a native failure.
            subagent
                .start(&mut provider, &mut terminal)
                .await
                .release_invalid_request();
            let mut attempts = 1;
            loop {
                tokio::select! {
                    request = next_request_or_outcome(&mut provider, &mut terminal) => {
                        assert!(!request.opens_with(BACKGROUND_SUBAGENT_PROMPT));
                        attempts += 1;
                        assert!(attempts <= 8, "native invalid-request recovery must be bounded");
                        request.release_invalid_request();
                    }
                    () = wait_for_completed_turns(&observations, 1) => break,
                }
            }
            // The failed turn ended the exchange, not the invocation: the subagent still runs.
            assert!(matches!(
                terminal.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));

            // Released, the subagent closes its own turn and its notification begins a main
            // turn, whose init ends the stream with the native failure. A main-thread request
            // Claude Code gets off before the group is forced is answered and never parsed.
            subagent.release().await;
            let outcome = loop {
                tokio::select! {
                    request = provider.next_request() => {
                        if request.opens_with(BACKGROUND_SUBAGENT_PROMPT) {
                            request.release_text(BACKGROUND_SUBAGENT_RESPONSE);
                        } else {
                            request.release_text("after the failure");
                        }
                    }
                    outcome = &mut terminal => break outcome.unwrap(),
                }
            };
            running.task.await.unwrap();
            assert_eq!(
                outcome,
                failed_agent_outcome(AgentFailureCause::HarnessFailed {
                    detail: AgentHarnessFailureDetail::ModelError,
                })
            );
            // The subagent's completion was observed after the failed turn: the drain went on
            // and the notified turn, not the end of the stream, met the failure.
            assert_backgrounded_subagent_activity(&observations);
            provider.shutdown().await;
        },
    )
    .await;
}

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
            assert_user_cancelled(running).await;
            drop(blocked_request);
            assert!(!provider.has_pending_request());
            assert_retained_native_session(&root, &["Complete the controlled lifecycle exchange."]);
            provider.shutdown().await;
        },
    )
    .await;
}

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
            let process_group = getpgid(Some(child)).unwrap();

            assert!(
                running
                    .cancellation
                    .request_cancellation(CancellationReason::UserRequest)
            );
            running.process_control.interrupt();

            assert_user_cancelled(running).await;
            assert!(process_group_is_quiescent(process_group));
            assert!(!provider.has_pending_request());
            assert_retained_native_session(&root, &["Complete the controlled lifecycle exchange."]);
            provider.shutdown().await;
        },
    )
    .await;
}

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
        let process_group = getpgid(Some(child)).unwrap();
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
        let outcome = running.finish().await;
        assert!(matches!(outcome, AgentOutcome::Failed(_)), "{outcome:?}");
        assert!(process_group_is_quiescent(process_group));
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &["Complete the controlled lifecycle exchange."]);
        provider.shutdown().await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_07_retains_forwarded_subagent_activity() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code subagent-retention", WATCHDOG, async {
        for subagent_type in ["general-purpose", "fork"] {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let running = launch_response_lifecycle(executable.clone(), &root, &provider).await;

            provider.next_request().await.release_tool_use(
                "Agent",
                json!({"description": "Run a controlled subagent",
                    "prompt": "Return the exact subagent retained marker.",
                    "subagent_type": subagent_type,
                    "run_in_background": false,
                }),
            );
            let mut terminal = running.terminal;
            tokio::select! {
                request = provider.next_request() => request.release_text("subagent retained marker"),
                outcome = &mut terminal => panic!("{subagent_type} stopped before child request: {outcome:?}"),
            }
            provider.next_request().await.release_text(RESPONSE);
            let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
                terminal.await.unwrap()
            else {
                panic!("{subagent_type} exchange must complete through the production adapter");
            };
            running.task.await.unwrap();
            assert_eq!(response.as_str(), RESPONSE);
            assert_retained_native_session(&root, &["subagent retained marker", RESPONSE]);
            let subagents = root.retained_resources().join("subagents");
            let retained = fs::read_dir(subagents)
                .unwrap()
                .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
                .collect::<Vec<_>>();
            assert!(!retained.is_empty());
            assert!(
                retained
                    .iter()
                    .any(|transcript| transcript.contains("subagent retained marker"))
            );
            assert!(!provider.has_pending_request());
            provider.shutdown().await;
        }
    })
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06e_cancel_pending_background_after_first_result() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    nix::sys::prctl::set_child_subreaper(true).unwrap();
    host_scaled_watchdog("pending background cancellation", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let ready = FixtureSignal::create(root.private().join("pending-child.pid"));
        let blocker = root.private().join("pending-child.blocker");
        mkfifo(&blocker, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;
        provider.next_request().await.release_tool_use(
            "Bash",
            json!({
                "command": format!("printf '%s\\n' \"$$\" > '{}'; IFS= read -r done < '{}'",
                    ready.path().display(), blocker.display()),
                "description": "Block a background command until cancellation",
                "run_in_background": true,
            }),
        );
        let child = process_id(&ready.receive().await);
        let process_group = getpgid(Some(child)).unwrap();
        provider
            .next_request()
            .await
            .release_text("not authoritative");
        observations
            .wait_for(|event| {
                matches!(
                    event,
                    AgentObservation::Lifecycle {
                        milestone: AgentLifecycleMilestone::TurnCompleted,
                    }
                )
            })
            .await;
        assert!(
            running
                .cancellation
                .request_cancellation(CancellationReason::UserRequest)
        );
        running.process_control.interrupt();
        assert_user_cancelled(running).await;
        assert!(process_group_is_quiescent(process_group));
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await;
}

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
                "command": format!("{} {}", std::env::var("UM_CLAUDE_CODE_CONFORMANCE_SHELL").unwrap(), hook.display())
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
        assert!(matches!(outcome, AgentOutcome::Failed(_)), "{outcome:?}");
        assert_eq!(fs::read(observed).unwrap(), b"denied");
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    }).await;
}

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06c_background_completion_continuation() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("background probe", WATCHDOG, async {
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
        let tools_path = std::env::var("UM_CLAUDE_CODE_CONFORMANCE_TOOLS_PATH")
            .expect("CLI Devenv must supply the native conformance tools");
        let observations = RecordingObservationSink::default();
        let mut running = launch_recorded_response_lifecycle(
            executable, &root, &provider, &observations,
        ).await;
        provider.next_request().await.release_tool_use(
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
        // The tool returns a task identity before its child is released.
        let next = provider.next_request().await;
        assert_eq!(ready.receive().await, b"ready\n");
        next.release_text("first response");
        observations.wait_for(|event| matches!(event, AgentObservation::Lifecycle {
            milestone: AgentLifecycleMilestone::TurnCompleted,
        })).await;
        assert!(matches!(running.terminal.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
        let release = tokio::task::spawn_blocking(move || {
            fs::write(blocker, b"done\n").unwrap();
        });
        release.await.unwrap();
        let mut terminal = running.terminal;
        let outcome = tokio::select! {
            request = provider.next_request() => {
                assert!(request.used_placeholder_key());
                assert!(request.body()["messages"].is_array());
                request.release_text("final response");
                terminal.await.unwrap()
            }
            outcome = &mut terminal => outcome.unwrap(),
        };
        running.task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
            panic!("background continuation must settle, got {outcome:?}");
        };
        assert_eq!(response.as_str(), "final response");
        assert_eq!(fs::read(operations.join("edit.txt")).unwrap(), b"after\n");
        assert!(!operations.join("remove.txt").exists());
        assert!(!operations.join("empty").exists());
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &["first response", "final response"]);
        let transcript = fs::read_to_string(root.retained_transcript()).unwrap();
        let records = transcript
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(!records.is_empty());
        provider.shutdown().await;
    }).await;
}

// SC-AGENT-7
// REQ-AGENT-35
// REQ-AGENT-64
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09_backgrounded_subagent_continues_the_invocation() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog(
        "pinned Claude Code backgrounded-subagent",
        WATCHDOG,
        async {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let subagent = BackgroundSubagent::new(&root);
            let observations = RecordingObservationSink::default();
            let running =
                launch_recorded_response_lifecycle(executable, &root, &provider, &observations)
                    .await;
            let mut terminal = running.terminal;

            subagent
                .start(&mut provider, &mut terminal)
                .await
                .release_text("first response");
            observations
                .wait_for(|event| {
                    matches!(
                        event,
                        AgentObservation::Lifecycle {
                            milestone: AgentLifecycleMilestone::TurnCompleted,
                        }
                    )
                })
                .await;
            assert!(matches!(
                terminal.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));

            subagent
                .finish(&mut provider, &mut terminal)
                .await
                .release_text("final response");
            let outcome = terminal.await.unwrap();
            running.task.await.unwrap();
            let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome
            else {
                panic!("the notified turn must settle the invocation, got {outcome:?}");
            };
            assert_eq!(response.as_str(), "final response");
            assert_backgrounded_subagent_activity(&observations);
            assert!(!provider.has_pending_request());
            assert_retained_native_session(&root, &["first response", "final response"]);
            provider.shutdown().await;
        },
    )
    .await;
}

// SC-AGENT-7
// REQ-AGENT-35
// INV-HARNESS-7
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09c_premature_result_waits_for_the_backgrounded_subagent() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code premature-result", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let subagent = BackgroundSubagent::new(&root);
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_result_lifecycle(executable, &root, &provider, &observations).await;
        let mut terminal = running.terminal;

        // A schema-valid candidate submitted while the subagent runs is still premature.
        subagent
            .start(&mut provider, &mut terminal)
            .await
            .release_structured_output(json!({"result": 1}));
        observations
            .wait_for(|event| {
                matches!(
                    event,
                    AgentObservation::ValueRejected {
                        kind: AgentValueKind::Result,
                        ..
                    }
                )
            })
            .await;
        let feedback = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(!feedback.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        assert!(contains_string_fragment(
            feedback.body(),
            &premature_candidate_feedback(SUBAGENT_OUTSTANDING)
        ));
        feedback.release_text("waiting for the subagent");
        // The agent ends the feedback turn to wait; any enforcement retry Claude Code makes
        // within that turn is answered the same way.
        loop {
            tokio::select! {
                request = next_request_or_outcome(&mut provider, &mut terminal) => {
                    assert!(!request.opens_with(BACKGROUND_SUBAGENT_PROMPT));
                    request.release_text("waiting for the subagent");
                }
                () = wait_for_completed_turns(&observations, 2) => break,
            }
        }
        assert!(matches!(
            terminal.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        subagent
            .finish(&mut provider, &mut terminal)
            .await
            .release_structured_output(json!({"result": 7}));
        let outcome = terminal.await.unwrap();
        running.task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("the notified turn's candidate must complete, got {outcome:?}");
        };
        assert_eq!(result.value(), &json!(7));
        let rejections = observations
            .snapshot()
            .into_iter()
            .filter_map(|envelope| match envelope.observation() {
                AgentObservation::ValueRejected { feedback, .. } => Some(Arc::clone(feedback)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rejections,
            [Arc::<str>::from(premature_candidate_feedback(
                SUBAGENT_OUTSTANDING
            ))]
        );
        assert_backgrounded_subagent_activity(&observations);
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await;
}

// SC-AGENT-7
// REQ-AGENT-35
// REQ-AGENT-64
// UM-3077: the UM-3024 build's sequence end to end. In Result mode the lead launches a
// backgrounded builder and submits early (feedback), answers the correction turn and the
// enforce nag with text and ends it waiting; the first builder's notification begins the
// turn in which the lead launches a second backgrounded builder and again ends with text
// through the nag; the second builder's notification begins the turn that submits. On the
// 0.70.0 release the second launch was recorded as foreground, the exchange's `result`
// forgot it, and the notified turn's `init` was refused as `initialization_invalid`.
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09f_a_builder_launched_in_a_notified_turn_after_a_refused_result_is_waited_for()
 {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code notified-turn launch", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let first = BackgroundSubagent::named(&root, "first");
        let second = BackgroundSubagent::named(&root, "second");
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_result_lifecycle(executable, &root, &provider, &observations).await;
        let mut terminal = running.terminal;

        // Exchange 1: the first builder is launched and a candidate is submitted early.
        first
            .start(&mut provider, &mut terminal)
            .await
            .release_structured_output(json!({"result": 1}));
        observations
            .wait_for(|event| {
                matches!(
                    event,
                    AgentObservation::ValueRejected {
                        kind: AgentValueKind::Result,
                        ..
                    }
                )
            })
            .await;
        // Exchange 2: the correction turn, answered with text through the enforce nag.
        let feedback = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(contains_string_fragment(
            feedback.body(),
            &premature_candidate_feedback(SUBAGENT_OUTSTANDING)
        ));
        feedback.release_text("waiting for the first builder");
        loop {
            tokio::select! {
                request = next_request_or_outcome(&mut provider, &mut terminal) => {
                    assert!(!request.opens_with(BACKGROUND_SUBAGENT_PROMPT));
                    request.release_text("waiting for the first builder");
                }
                () = wait_for_completed_turns(&observations, 2) => break,
            }
        }

        // Exchange 3, begun by the first builder's notification: the second builder is
        // launched and the turn ends with text through the nag.
        let notified = first.finish(&mut provider, &mut terminal).await;
        let main_thread = second
            .launch_from(notified, &mut provider, &mut terminal)
            .await;
        main_thread.release_text("waiting for the second builder");
        loop {
            tokio::select! {
                request = next_request_or_outcome(&mut provider, &mut terminal) => {
                    assert!(!request.opens_with(BACKGROUND_SUBAGENT_PROMPT));
                    request.release_text("waiting for the second builder");
                }
                () = wait_for_completed_turns(&observations, 3) => break,
            }
        }
        assert!(matches!(
            terminal.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        // Exchange 4, begun by the second builder's notification, submits the result.
        second
            .finish(&mut provider, &mut terminal)
            .await
            .release_structured_output(json!({"result": 7}));
        let outcome = terminal.await.unwrap();
        running.task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!("the second builder's notified turn must complete the step, got {outcome:?}");
        };
        assert_eq!(result.value(), &json!(7));
        let rejections = observations
            .snapshot()
            .into_iter()
            .filter_map(|envelope| match envelope.observation() {
                AgentObservation::ValueRejected { feedback, .. } => Some(Arc::clone(feedback)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rejections,
            [Arc::<str>::from(premature_candidate_feedback(
                SUBAGENT_OUTSTANDING
            ))]
        );
        assert_eq!(
            backgrounded_subagent_statuses(&observations),
            ["started", "completed", "started", "completed"]
        );
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await;
}

const RESUMED_SUBAGENT_RESPONSE: &str = "resumed subagent finished";

// REQ-AGENT-64
// REQ-AGENT-35
// UM-2879: the lead messages a backgrounded builder that has already finished, inside the
// turn in which its notification arrived, and Claude Code resumes the builder under the
// message's tool call. The resume used to be rejected as a duplicate `task_started`
// (`system_event_invalid`); it is a second run of the same subagent that notifies again.
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09d_messaging_a_finished_backgrounded_subagent_resumes_it() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code resumed-subagent", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let subagent = BackgroundSubagent::new(&root);
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;
        let mut terminal = running.terminal;

        // The main thread keeps its turn open in a foreground command while the subagent
        // finishes, so the subagent's notification lands inside the active exchange.
        let main_thread = subagent.start(&mut provider, &mut terminal).await;
        let agent_id = launched_agent_id(main_thread.body());
        let lead_ready = FixtureSignal::create(root.private().join("lead-ready"));
        let lead_blocker = root.private().join("lead-blocker");
        mkfifo(&lead_blocker, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        main_thread.release_tool_use(
            "Bash",
            json!({
                "command": format!(
                    "printf 'ready\\n' > '{}'; IFS= read -r done < '{}'; printf 'lead waited\\n'",
                    lead_ready.path().display(), lead_blocker.display()
                ),
                "description": "Wait while the subagent finishes",
            }),
        );
        assert_eq!(lead_ready.receive().await, b"ready\n");
        subagent.release().await;
        let request = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(request.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        request.release_text(BACKGROUND_SUBAGENT_RESPONSE);
        observations
            .wait_for(|event| {
                matches!(event, AgentObservation::Diagnostic { message, .. }
                    if message.contains(BACKGROUND_SUBAGENT_DESCRIPTION) && message.contains("completed"))
            })
            .await;
        tokio::task::spawn_blocking(move || fs::write(lead_blocker, b"done\n").unwrap())
            .await
            .unwrap();

        // The message is the second block of its message so that its tool-use id differs
        // from the Agent call's, as it does in production.
        let request = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(!request.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        request.release_blocks(vec![
            LoopbackBlock::text("messaging the finished subagent"),
            LoopbackBlock::tool_use(
                "SendMessage",
                json!({"to": agent_id, "summary": "Report once more",
                    "message": "Answer once more with the resumed marker."}),
            ),
        ]);

        // The main thread ends its turn to wait before the resumed subagent answers, so the
        // resumed run's message and notification arrive in the drain and the notification's
        // turn settles the invocation. Were the resumed run notified inside the turn, the
        // turn it opened would be an epilogue (REQ-AGENT-76) and the first turn's text the
        // response.
        let mut resumed = None;
        loop {
            tokio::select! {
                request = next_request_or_outcome(&mut provider, &mut terminal) => {
                    if request.opens_with(BACKGROUND_SUBAGENT_PROMPT) {
                        assert!(
                            resumed.replace(request).is_none(),
                            "the resumed subagent asked twice before answering"
                        );
                    } else {
                        request.release_text("waiting for the resumed subagent");
                    }
                }
                () = wait_for_completed_turns(&observations, 1) => break,
            }
        }
        let resumed = match resumed {
            Some(request) => request,
            None => next_request_or_outcome(&mut provider, &mut terminal).await,
        };
        assert!(resumed.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        resumed.release_text(RESUMED_SUBAGENT_RESPONSE);
        let final_turn = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(!final_turn.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        final_turn.release_text(RESPONSE);
        let outcome = terminal.await.unwrap();
        running.task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
            panic!("the resumed subagent's notification must settle the invocation, got {outcome:?}");
        };
        assert_eq!(response.as_str(), RESPONSE);
        assert_eq!(
            backgrounded_subagent_statuses(&observations),
            ["started", "completed", "resumed", "completed"]
        );
        assert!(observations.snapshot().iter().any(|envelope| matches!(
            envelope.observation(),
            AgentObservation::AssistantText { text } if text.as_ref() == RESUMED_SUBAGENT_RESPONSE
        )));
        assert!(!observations.snapshot().iter().any(|envelope| matches!(
            envelope.observation(),
            AgentObservation::UnrecognizedHarnessEvent { event } if event["subtype"] == "task_started"
        )));
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await;
}

// REQ-AGENT-64
// REQ-AGENT-35
// UM-2879: the same message sent from the turn the builder's notification woke. That
// turn's `init` forgot the subagent as outstanding work, and the resumed run's messages
// still name the Agent call that launched it, so the adapter must keep the identity it
// forgot for the resume to be accepted in the drain.
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09e_messaging_a_finished_subagent_from_its_wakeup_turn_resumes_it()
{
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code wakeup-resume", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let subagent = BackgroundSubagent::new(&root);
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;
        let mut terminal = running.terminal;

        // The main thread ends its turn to wait; the subagent's notification wakes it.
        let main_thread = subagent.start(&mut provider, &mut terminal).await;
        let agent_id = launched_agent_id(main_thread.body());
        main_thread.release_text("first response");
        observations
            .wait_for(|event| {
                matches!(
                    event,
                    AgentObservation::Lifecycle {
                        milestone: AgentLifecycleMilestone::TurnCompleted,
                    }
                )
            })
            .await;
        let wakeup = subagent.finish(&mut provider, &mut terminal).await;

        // From the wakeup turn, the message is the second block of its message so that
        // its tool-use id differs from the Agent call's, as it does in production.
        wakeup.release_blocks(vec![
            LoopbackBlock::text("messaging the finished subagent"),
            LoopbackBlock::tool_use(
                "SendMessage",
                json!({"to": agent_id, "summary": "Report once more",
                    "message": "Answer once more with the resumed marker."}),
            ),
        ]);

        // The main thread ends its turn before the resumed subagent answers, so the
        // resumed run's message and notification arrive in the drain and the notification's
        // turn settles the invocation.
        let mut resumed = None;
        loop {
            tokio::select! {
                request = next_request_or_outcome(&mut provider, &mut terminal) => {
                    if request.opens_with(BACKGROUND_SUBAGENT_PROMPT) {
                        assert!(
                            resumed.replace(request).is_none(),
                            "the resumed subagent asked twice before answering"
                        );
                    } else {
                        request.release_text("waiting for the resumed subagent");
                    }
                }
                () = wait_for_completed_turns(&observations, 2) => break,
            }
        }
        let resumed = match resumed {
            Some(request) => request,
            None => next_request_or_outcome(&mut provider, &mut terminal).await,
        };
        assert!(resumed.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        resumed.release_text(RESUMED_SUBAGENT_RESPONSE);
        let final_turn = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(!final_turn.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        final_turn.release_text(RESPONSE);
        let outcome = terminal.await.unwrap();
        running.task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
            panic!(
                "the resumed subagent's notification must settle the invocation, got {outcome:?}"
            );
        };
        assert_eq!(response.as_str(), RESPONSE);
        assert_eq!(
            backgrounded_subagent_statuses(&observations),
            ["started", "completed", "resumed", "completed"]
        );
        assert!(observations.snapshot().iter().any(|envelope| matches!(
            envelope.observation(),
            AgentObservation::AssistantText { text } if text.as_ref() == RESUMED_SUBAGENT_RESPONSE
        )));
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await;
}

// REQ-AGENT-35
// REQ-AGENT-64
// UM-2879: `09e`'s message in Result mode, from a lead that loads `SendMessage` before it
// calls it, as a model does that finds the tool among the deferred ones. 2.1.310 lists
// `SendMessage` as deferred, absent from the request's `tools[]`, and runs it unloaded
// (`09d`, `09e`); here the wakeup turn's first round is a `ToolSearch` that loads it, the
// next request's `tools[]` carries it, the message resumes the finished subagent, the lead
// ends the turn with text through the enforcement nag, and the value is submitted on the
// turn the resumed run's notification wakes.
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09g_a_lead_that_loads_send_message_before_messaging_a_finished_subagent_resumes_it()
 {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code loaded-SendMessage", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let subagent = BackgroundSubagent::new(&root);
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_result_lifecycle(executable, &root, &provider, &observations).await;
        let mut terminal = running.terminal;

        // The main thread ends its turn to wait, through the enforcement nag; the subagent,
        // blocked in its command, asks nothing meanwhile, and its notification wakes the lead.
        let main_thread = subagent.start(&mut provider, &mut terminal).await;
        let agent_id = launched_agent_id(main_thread.body());
        main_thread.release_text("waiting for the subagent");
        loop {
            tokio::select! {
                request = next_request_or_outcome(&mut provider, &mut terminal) => {
                    assert!(!request.opens_with(BACKGROUND_SUBAGENT_PROMPT));
                    request.release_text("waiting for the subagent");
                }
                () = wait_for_completed_turns(&observations, 1) => break,
            }
        }
        let wakeup = subagent.finish(&mut provider, &mut terminal).await;
        assert!(
            !tool_names(wakeup.body())
                .iter()
                .any(|name| name == "SendMessage"),
            "2.1.310 defers SendMessage: {:?}",
            tool_names(wakeup.body())
        );
        wakeup.release_blocks(vec![
            LoopbackBlock::text("loading SendMessage"),
            LoopbackBlock::tool_use(
                "ToolSearch",
                json!({"query": "select:SendMessage", "max_results": 1}),
            ),
        ]);
        let loaded = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(!loaded.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        assert!(
            tool_names(loaded.body())
                .iter()
                .any(|name| name == "SendMessage"),
            "the ToolSearch round did not load SendMessage: {:?}",
            tool_names(loaded.body())
        );
        // The nag reaches the provider merged into the user message that returns the turn's
        // tool results (the parser records its event type once), so the conversation's count
        // tells a new one.
        let nags_before = enforcement_nags(loaded.body());
        let mut nags = nags_before;
        loaded.release_blocks(vec![
            LoopbackBlock::text("messaging the finished subagent"),
            LoopbackBlock::tool_use(
                "SendMessage",
                json!({"to": agent_id, "summary": "Report once more",
                    "message": "Answer once more with the resumed marker."}),
            ),
        ]);

        // As in `09e`, the lead ends the turn before the resumed subagent answers, here
        // refusing the nag with text, so the resumed run's notification wakes the turn that
        // submits the value.
        let mut resumed = None;
        loop {
            tokio::select! {
                request = next_request_or_outcome(&mut provider, &mut terminal) => {
                    if request.opens_with(BACKGROUND_SUBAGENT_PROMPT) {
                        assert!(
                            resumed.replace(request).is_none(),
                            "the resumed subagent asked twice before answering"
                        );
                    } else {
                        nags = nags.max(enforcement_nags(request.body()));
                        request.release_text("waiting for the resumed subagent");
                    }
                }
                () = wait_for_completed_turns(&observations, 2) => break,
            }
        }
        assert!(
            nags > nags_before,
            "the messaging turn ended with text but drew no enforcement nag"
        );
        let resumed = match resumed {
            Some(request) => request,
            None => next_request_or_outcome(&mut provider, &mut terminal).await,
        };
        assert!(resumed.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        resumed.release_text(RESUMED_SUBAGENT_RESPONSE);
        let final_turn = next_request_or_outcome(&mut provider, &mut terminal).await;
        assert!(!final_turn.opens_with(BACKGROUND_SUBAGENT_PROMPT));
        final_turn.release_structured_output(json!({"result": 7}));
        let outcome = terminal.await.unwrap();
        running.task.await.unwrap();
        let AgentOutcome::Completed(CompletedAgentInvocation::Result(result)) = outcome else {
            panic!(
                "the resumed subagent's notification must begin the turn that settles, got \
                 {outcome:?}"
            );
        };
        assert_eq!(result.value(), &json!(7));
        assert_eq!(
            backgrounded_subagent_statuses(&observations),
            ["started", "completed", "resumed", "completed"]
        );
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await;
}

/// Ordered thinking segments deliberately carry multibyte, newline, tab, and trailing
/// whitespace so the case fails if Claude Code trims or normalizes reconstructed
/// thinking before restating it in its nominal assistant envelope.
const THINKING_SEGMENTS: [&str; 3] = [
    "Step one: \u{e9}\u{4e2d}\u{6587} ",
    "line\nbreak\ttab  ",
    "trailing space   ",
];

fn reasoning_text(observation: &AgentObservation) -> Option<&str> {
    match observation {
        AgentObservation::Reasoning { text } => Some(text.as_ref()),
        _ => None,
    }
}

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_08_correlates_a_nominal_thinking_envelope_before_text() {
    // Every exact-binary case deliberately owns a fresh watchdog, loopback provider, and
    // synthetic root; sharing those resources would let one native case contaminate another.
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("pinned Claude Code nominal-thinking", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;

        provider.next_request().await.release_blocks(vec![
            LoopbackBlock::thinking(&THINKING_SEGMENTS),
            LoopbackBlock::text(RESPONSE),
        ]);

        // Claude Code 2.1.302 emits a nominal `assistant` envelope restating the thinking
        // block. `ActiveContentBlock::correlate_nominal` requires that envelope to be
        // byte-equal to the reconstructed `thinking_delta` stream, so reaching a response
        // at all proves the equality invariant holds for native thinking.
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            running.finish().await
        else {
            panic!("a thinking block must not prevent an ordinary response outcome");
        };
        assert_eq!(response.as_str(), RESPONSE);

        // Thinking is observational only: it never contributes to the response, and the
        // response is exactly the text block that followed it.
        assert_eq!(
            observations.concatenated_text(reasoning_text),
            THINKING_SEGMENTS.concat()
        );
        assert_eq!(observations.concatenated_text(assistant_text), RESPONSE);
        // Thinking additionally makes Claude Code interleave `system` events with subtype
        // `thinking_tokens` between content-block events. They carry no authority, so they
        // must degrade to ordered unrecognized observations without breaking correlation.
        assert!(observations.snapshot().iter().any(|observation| matches!(
            observation.observation(),
            AgentObservation::UnrecognizedHarnessEvent { event }
                if event["type"] == "system" && event["subtype"] == "thinking_tokens"
        )));
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &[RESPONSE]);
        provider.shutdown().await;
    })
    .await;
}

/// Opaque payload of a native `redacted_thinking` block. The native API delivers this
/// block complete in its `content_block_start`, with no deltas.
const REDACTED_THINKING_DATA: &str = "EmwKAhgBEgy3va3scherzoredacted";

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09_every_block_kind_correlates_its_own_nominal_envelope() {
    // Fresh per-case native resources, as in every other exact-binary case.
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog(
        "pinned Claude Code block-kind correlation",
        WATCHDOG,
        async {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let observations = RecordingObservationSink::default();
            let running =
                launch_recorded_response_lifecycle(executable, &root, &provider, &observations)
                    .await;

            // One native message carrying every content-block kind. `redacted_thinking` is an
            // unrecognized kind, so it correlates through `ActiveContentBlock::Unknown`, whose
            // rule is stricter than the text and thinking rules: the nominal envelope must equal
            // the whole `content_block_start` block. A tool_use block additionally proves the
            // exchange survives block-kind interleaving and continues into a second message.
            provider.next_request().await.release_blocks(vec![
                LoopbackBlock::thinking(&["deciding to call a tool "]),
                LoopbackBlock::redacted_thinking(REDACTED_THINKING_DATA),
                LoopbackBlock::tool_use(
                    "Bash",
                    json!({"command": "echo probe-marker", "description": "controlled probe"}),
                ),
            ]);
            provider.next_request().await.release_text(RESPONSE);

            let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
                running.finish().await
            else {
                panic!("interleaved block kinds must still reach an ordinary response outcome");
            };
            assert_eq!(response.as_str(), RESPONSE);
            // Only the final message's text is the response; nothing from the tool-calling
            // message contributes to it.
            assert_eq!(observations.concatenated_text(assistant_text), RESPONSE);
            assert!(!provider.has_pending_request());
            assert_retained_native_session(&root, &[RESPONSE]);
            provider.shutdown().await;
        },
    )
    .await;
}

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09b_oversized_image_read_returns_a_bounded_tool_error() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog("oversized image refusal", WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        // Sparse file: the native Read tool must refuse it without reading 129 MB.
        let image = root.project().join("oversized.png");
        fs::File::create(&image)
            .unwrap()
            .set_len(129 * 1024 * 1024)
            .unwrap();
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;
        provider
            .next_request()
            .await
            .release_tool_use("Read", json!({"file_path": image.display().to_string()}));
        let next = provider.next_request().await;
        let tool_results = observations
            .snapshot()
            .iter()
            .filter_map(|entry| match entry.observation() {
                AgentObservation::ToolResult {
                    is_error, content, ..
                } => Some((*is_error, content.len())),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(tool_results.len(), 1, "{tool_results:?}");
        assert!(
            tool_results[0].0,
            "oversized image must return a tool error"
        );
        assert!(
            tool_results[0].1 < 4096,
            "tool error must not contain image bytes"
        );
        next.release_text(RESPONSE);
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
            running.finish().await
        else {
            panic!("image refusal must allow the production adapter to settle");
        };
        assert_eq!(response.as_str(), RESPONSE);
        assert_retained_native_session(&root, &[RESPONSE]);
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_10_tool_exchange_normalizes_inputs_and_omits_is_error() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    host_scaled_watchdog(
        "pinned Claude Code tool-exchange normalization",
        WATCHDOG,
        async {
            let mut provider = LoopbackProvider::start().await;
            let root = SyntheticClaudeCodeRoot::new();
            let observations = RecordingObservationSink::default();
            let running =
                launch_recorded_response_lifecycle(executable, &root, &provider, &observations)
                    .await;

            // A Write followed by a Bash call in one message reproduces the live workflow
            // exchange that drifted from the profile: Write's successful result carries no
            // `is_error`, and the model's Bash `command` begins with `cd <cwd> && `, which
            // Claude Code strips before restating the block in its nominal envelope. The
            // command reads the file with shell builtins only: the Bash tool's `PATH` under
            // the synthetic root is `/usr/bin:/bin`, which carries no coreutils on the NixOS
            // conformance runners.
            let probe = root.project().join("probe-marker.txt");
            const READ_PROBE: &str = "printf '%s\\n' \"$(<probe-marker.txt)\"";
            let streamed_command = format!("cd {} && {READ_PROBE}", root.project().display());
            provider.next_request().await.release_blocks(vec![
                LoopbackBlock::tool_use(
                    "Write",
                    json!({"file_path": probe.display().to_string(), "content": "probe-marker\n"}),
                ),
                LoopbackBlock::tool_use(
                    "Bash",
                    json!({"command": streamed_command, "description": "controlled probe"}),
                ),
            ]);
            provider.next_request().await.release_text(RESPONSE);

            let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) =
                running.finish().await
            else {
                panic!("a tool exchange with normalized inputs must reach an ordinary response");
            };
            assert_eq!(response.as_str(), RESPONSE);
            assert_eq!(fs::read_to_string(&probe).unwrap(), "probe-marker\n");

            // Both results are observed as successes; the Bash result is the written content.
            let results = observations
                .snapshot()
                .iter()
                .filter_map(|observation| match observation.observation() {
                    AgentObservation::ToolResult {
                        call_id,
                        is_error,
                        content,
                    } => Some((Arc::clone(call_id), *is_error, Arc::clone(content))),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(results.len(), 2, "{results:?}");
            assert!(
                results.iter().all(|(_, is_error, _)| !is_error),
                "{results:?}"
            );
            assert!(
                results
                    .iter()
                    .any(|(_, _, content)| content.trim() == "probe-marker")
            );

            // The retained native transcript proves the drift this case qualifies: the Bash
            // block Claude Code recorded no longer begins with the `cd <cwd> && ` prefix the
            // model streamed, and the Write result carries no `is_error` member at all.
            let transcript = fs::read_to_string(root.retained_transcript())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>();
            let bash_block = transcript
                .iter()
                .find_map(|message| find_content_block(message, "tool_use", "Bash"))
                .expect("retained transcript must carry the Bash tool_use block");
            assert_eq!(bash_block["input"]["command"], READ_PROBE);
            let write_result = transcript
                .iter()
                .find_map(|message| find_tool_result(message, "File created successfully"))
                .expect("retained transcript must carry the Write tool_result block");
            assert!(write_result.get("is_error").is_none(), "{write_result}");

            assert!(!provider.has_pending_request());
            assert_retained_native_session(&root, &[RESPONSE]);
            provider.shutdown().await;
        },
    )
    .await;
}

fn content_blocks(message: &Value) -> impl Iterator<Item = &Value> {
    message["message"]["content"]
        .as_array()
        .into_iter()
        .flatten()
}

fn find_content_block<'a>(message: &'a Value, block_type: &str, name: &str) -> Option<&'a Value> {
    content_blocks(message).find(|block| block["type"] == block_type && block["name"] == name)
}

fn find_tool_result<'a>(message: &'a Value, content_fragment: &str) -> Option<&'a Value> {
    content_blocks(message).find(|block| {
        block["type"] == "tool_result"
            && contains_string_fragment(&block["content"], content_fragment)
    })
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
