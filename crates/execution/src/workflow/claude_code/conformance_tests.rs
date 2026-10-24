use std::cell::Cell;
use std::fs;
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

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

use super::adapter::{ClaudeCodeStreamJsonV1Adapter, PREMATURE_CANDIDATE_FEEDBACK};
use super::test_support::{
    ControlledProviderRequest, FixtureSignal, LoopbackBlock, LoopbackProvider, PendingClock,
    RecordingObservationSink, SyntheticClaudeCodeRoot, admitted_adapter, invocation_identity,
    version_probe_environment,
};
use super::*;
use crate::workflow::admission::{CancellationReason, CancellationSource};
use crate::workflow::agent::{
    AgentAdapter, AgentInvocation, AgentInvocationLimits, AgentInvocationStaging,
    AgentProcessContext, AgentProcessControl, AgentPrompt, AgentStartReceiver, AgentValueMode,
    PositiveDuration, RetainedJsonSchema, StagedAgentAttachment, agent_start_channel,
};
use crate::workflow::agent_diagnostics::AgentDiagnosticSession;
use crate::workflow::diagnostic::StepDiagnosticLog;
use crate::workflow::execution_root::AdmittedExecutionRoot;
use crate::workflow::observation::NoopExecutionObserver;
use crate::workflow::process_group::{ProcessGuardRegistry, process_group_is_quiescent};

const MODEL: &str = "scherzo-loopback";
const EFFORT: &str = "xhigh";
const RESPONSE: &str = "loopback complete";
const NATIVE_INSTRUCTION_MARKER: &str = "LIV2293_NATIVE_INSTRUCTION_MARKER";
const NATIVE_SKILL_MARKER: &str = "LIV2293_NATIVE_SKILL_MARKER";
const NATIVE_PLUGIN_MARKER: &str = "LIV2293_NATIVE_PLUGIN_MARKER";
const NATIVE_MCP_MARKER: &str = "LIV2293_NATIVE_MCP_MARKER";
const DIRECT_SESSION_ID: &str = "00000000-0000-4000-8000-000000000001";
const WATCHDOG: Duration = Duration::from_secs(20);

static EXACT_BINARY_CONFORMANCE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn conformance_executable() -> PathBuf {
    std::env::var_os("UM_CLAUDE_CODE_CONFORMANCE_EXECUTABLE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            panic!(
                "UM_CLAUDE_CODE_CONFORMANCE_EXECUTABLE must name the pinned Claude Code executable"
            )
        })
}

async fn exclusive_conformance_executable() -> (PathBuf, tokio::sync::MutexGuard<'static, ()>) {
    let executable = conformance_executable();
    // Native startup is resource-intensive enough that concurrent qualification cases can
    // exhaust their independent anti-hang watchdogs before reaching controlled provider I/O.
    let exclusive = EXACT_BINARY_CONFORMANCE.lock().await;
    (executable, exclusive)
}

fn conformance_limits() -> AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits> {
    AgentInvocationLimits::new(
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(512).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        PositiveDuration::new(Duration::from_secs(1)).unwrap(),
        ClaudeCodeStreamJsonV1ProtocolLimits::profile(),
    )
}

struct RunningProductionClaudeCode {
    task: tokio::task::JoinHandle<()>,
    started: AgentStartReceiver,
    terminal: tokio::sync::oneshot::Receiver<AgentOutcome>,
    cancellation: CancellationSource,
    process_control: AgentProcessControl,
}

impl RunningProductionClaudeCode {
    fn launch(
        executable: PathBuf,
        root: &SyntheticClaudeCodeRoot,
        provider: &LoopbackProvider,
        value_mode: AgentValueMode,
        observations: RecordingObservationSink,
    ) -> Self {
        // Lifecycle cases deliberately build their own invocation instead of borrowing the
        // attachment case's resources, so native cancellation cannot contaminate another root.
        let admitted_root = AdmittedExecutionRoot::admit(root.project()).unwrap();
        let working_directory = admitted_root.select_working_directory(None).unwrap();
        let cancellation = CancellationSource::new();
        let invocation = AgentInvocation::new(
            invocation_identity("claude-code-lifecycle-conformance", "agent"),
            admitted_adapter(executable, MODEL),
            AgentProcessContext::new(working_directory, root.environment_snapshot(provider)),
            AgentInvocationStaging::new(root.private().to_owned()),
            AgentDiagnosticSession::claude_code_fixture(root.private().join("diagnostics/session")),
            AgentPrompt::new(
                Arc::from(fs::read_to_string(root.system_prompt()).unwrap()),
                Arc::from("Complete the controlled lifecycle exchange."),
            ),
            Arc::from([]),
            value_mode.clone(),
            conformance_limits(),
            cancellation.clone(),
            ProcessGuardRegistry::default(),
            observations,
        );
        let process_control = invocation.process_control().clone();
        let adapter = ClaudeCodeStreamJsonV1Adapter::new_default(
            StepDiagnosticLog::default(),
            NonZeroU64::new(1024).unwrap(),
            PendingClock,
            NoopExecutionObserver,
        )
        .unwrap();
        let (started_callback, started) = agent_start_channel();
        let (terminal_callback, terminal) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = terminal_callback.send(adapter.invoke(invocation, started_callback).await);
        });
        Self {
            task,
            started,
            terminal,
            cancellation,
            process_control,
        }
    }

    async fn await_started(self) -> RunningStartedClaudeCode {
        self.started.receive().await.unwrap();
        RunningStartedClaudeCode {
            task: self.task,
            terminal: self.terminal,
            cancellation: self.cancellation,
            process_control: self.process_control,
        }
    }
}

struct RunningStartedClaudeCode {
    task: tokio::task::JoinHandle<()>,
    terminal: tokio::sync::oneshot::Receiver<AgentOutcome>,
    cancellation: CancellationSource,
    process_control: AgentProcessControl,
}

impl RunningStartedClaudeCode {
    async fn finish(self) -> AgentOutcome {
        let outcome = self.terminal.await.unwrap();
        self.task.await.unwrap();
        outcome
    }
}

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

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_01_normal_mode_loopback_conforms_from_a_synthetic_root() {
    // Every exact-binary case deliberately owns a fresh watchdog, loopback provider, and
    // synthetic root; sharing those resources would let one native case contaminate another.
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
        // This release defers MCP descriptions until the native tool search
        // selects the tool. Exercise that path before the ordinary response.
        request.release_tool_use("ToolSearch", json!({"query": "scherzo_native_resource"}));
        let request = provider.next_request().await;
        assert!(contains_string_fragment(request.body(), NATIVE_MCP_MARKER));
        request.release_text(RESPONSE);

        let output = child.wait_with_output().await.unwrap();
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
        for chunk in output.stdout.chunks(7) {
            parser.push_stdout(chunk, drop).unwrap();
        }
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
    .await
    .expect("pinned Claude Code conformance watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_02_production_driver_returns_one_normalized_response() {
    // Production-adapter cases each need independent native process and provider state;
    // sharing their synthetic roots would invalidate same-process correction evidence.
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
    .await
    .expect("pinned Claude Code production-driver watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_03_corrects_a_result_in_one_production_conversation() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let admitted_root = AdmittedExecutionRoot::admit(root.project()).unwrap();
        let working_directory = admitted_root.select_working_directory(None).unwrap();
        let schema_document = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "integer",
            "minimum": 1,
        });
        let schema_bytes = Arc::<[u8]>::from(serde_json::to_vec(&schema_document).unwrap());
        let value_mode = AgentValueMode::Result {
            output: Arc::from("result"),
            schema: RetainedJsonSchema::compile(schema_bytes, Arc::new(schema_document)).unwrap(),
        };
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
    .await
    .expect("pinned Claude Code result-correction watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_04_production_no_value_and_native_failure_are_typed() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
    .await
    .expect("pinned Claude Code no-value/failure watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_04b_provider_overload_retries_and_stream_error_fails_closed() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
    .await
    .expect("pinned Claude Code overload probe watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_05_cancels_a_blocked_provider_request() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
    })
    .await
    .expect("pinned Claude Code blocked-provider cancellation watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06_cancels_a_stubborn_bash_descendant() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    #[cfg(target_os = "linux")]
    nix::sys::prctl::set_child_subreaper(true).unwrap();
    tokio::time::timeout(WATCHDOG, async {
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
    })
    .await
    .expect("pinned Claude Code stubborn-child cancellation watchdog expired");
}

#[cfg(target_os = "linux")]
#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06b_headless_term_and_cont_quiesce_owned_work() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    nix::sys::prctl::set_child_subreaper(true).unwrap();
    tokio::time::timeout(WATCHDOG, async {
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
    .await
    .expect("pinned Claude Code headless termination watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_07_retains_forwarded_subagent_activity() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
    .await
    .expect("pinned Claude Code subagent-retention watchdog expired");
}

#[cfg(target_os = "linux")]
#[expect(
    clippy::disallowed_methods,
    reason = "real time is only an anti-hang boundary for the native process"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06e_cancel_pending_background_after_first_result() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    nix::sys::prctl::set_child_subreaper(true).unwrap();
    tokio::time::timeout(WATCHDOG, async {
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
    .await
    .expect("pending background cancellation watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is only an anti-hang boundary for the native process"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06d_prompt_hook_deny_prevents_provider_request() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
    }).await.expect("native prompt-hook deny watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time bounds only a controlled external process probe"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_06c_background_completion_continuation() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
    }).await.expect("background probe watchdog expired");
}

const BACKGROUND_SUBAGENT_PROMPT: &str = "Run the controlled background subagent command.";
const BACKGROUND_SUBAGENT_DESCRIPTION: &str = "Run a backgrounded subagent";
const BACKGROUND_SUBAGENT_RESPONSE: &str = "background subagent finished";

/// A subagent the main thread starts with `run_in_background`, whose own Bash call blocks
/// on a fifo until the case releases it, so the main thread's turn ends while it runs.
struct BackgroundSubagent {
    ready: FixtureSignal,
    blocker: PathBuf,
}

impl BackgroundSubagent {
    fn new(root: &SyntheticClaudeCodeRoot) -> Self {
        let ready = FixtureSignal::create(root.private().join("subagent-ready"));
        let blocker = root.private().join("subagent-blocker");
        mkfifo(&blocker, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        Self { ready, blocker }
    }

    /// Serves the main thread's `Agent` call and the subagent's blocking command, returning
    /// the main thread's next request once the command is running. The subagent's request
    /// may arrive before or after the main thread's.
    async fn start(
        &self,
        provider: &mut LoopbackProvider,
        terminal: &mut tokio::sync::oneshot::Receiver<AgentOutcome>,
    ) -> ControlledProviderRequest {
        provider.next_request().await.release_tool_use(
            "Agent",
            json!({
                "description": BACKGROUND_SUBAGENT_DESCRIPTION,
                "prompt": BACKGROUND_SUBAGENT_PROMPT,
                "subagent_type": "general-purpose",
                "run_in_background": true,
            }),
        );
        let mut main_thread = None;
        let mut subagent_started = false;
        while main_thread.is_none() || !subagent_started {
            let request = next_request_or_outcome(provider, terminal).await;
            if request.opens_with(BACKGROUND_SUBAGENT_PROMPT) {
                assert!(
                    !subagent_started,
                    "the subagent asked again before its command ran"
                );
                request.release_tool_use(
                    "Bash",
                    json!({
                        "command": format!(
                            "printf 'ready\\n' > '{}'; IFS= read -r done < '{}'; printf 'subagent command finished\\n'",
                            self.ready.path().display(), self.blocker.display()
                        ),
                        "description": "Block the subagent until released",
                    }),
                );
                subagent_started = true;
            } else {
                assert!(
                    main_thread.is_none(),
                    "the main thread asked twice before the subagent ran"
                );
                main_thread = Some(request);
            }
        }
        assert_eq!(self.ready.receive().await, b"ready\n");
        main_thread.unwrap()
    }

    /// Releases the subagent's command, serves its closing text, and returns the request of
    /// the main-thread turn its notification starts.
    async fn finish(
        self,
        provider: &mut LoopbackProvider,
        terminal: &mut tokio::sync::oneshot::Receiver<AgentOutcome>,
    ) -> ControlledProviderRequest {
        let blocker = self.blocker;
        tokio::task::spawn_blocking(move || fs::write(blocker, b"done\n").unwrap())
            .await
            .unwrap();
        loop {
            let request = next_request_or_outcome(provider, terminal).await;
            if !request.opens_with(BACKGROUND_SUBAGENT_PROMPT) {
                return request;
            }
            request.release_text(BACKGROUND_SUBAGENT_RESPONSE);
        }
    }
}

async fn next_request_or_outcome(
    provider: &mut LoopbackProvider,
    terminal: &mut tokio::sync::oneshot::Receiver<AgentOutcome>,
) -> ControlledProviderRequest {
    tokio::select! {
        request = provider.next_request() => request,
        outcome = terminal => panic!("invocation ended while the case awaited a request: {outcome:?}"),
    }
}

fn completed_turns(observations: &RecordingObservationSink) -> usize {
    observations
        .snapshot()
        .iter()
        .filter(|envelope| {
            matches!(
                envelope.observation(),
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::TurnCompleted,
                }
            )
        })
        .count()
}

async fn wait_for_completed_turns(observations: &RecordingObservationSink, turns: usize) {
    while completed_turns(observations) < turns {
        // `wait_for` evaluates the record from its start on each observation, so an
        // evaluation past the length seen here is of a newer observation.
        let seen = observations.snapshot().len();
        let evaluated = Cell::new(0);
        observations
            .wait_for(|_| {
                evaluated.set(evaluated.get() + 1);
                evaluated.get() > seen
            })
            .await;
    }
}

/// The subagent's start and settlement are information diagnostics, and its own tool runs
/// are recognized rather than reported as unrecognized `task_started` events.
fn assert_backgrounded_subagent_activity(observations: &RecordingObservationSink) {
    let observations = observations.snapshot();
    let information = observations
        .iter()
        .filter_map(|envelope| match envelope.observation() {
            AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Information,
                message,
            } => Some(message.as_ref()),
            _ => None,
        })
        .filter(|message| message.contains(BACKGROUND_SUBAGENT_DESCRIPTION))
        .collect::<Vec<_>>();
    for status in ["started", "completed"] {
        assert!(
            information
                .iter()
                .any(|message| message.split_whitespace().any(|token| token == status)),
            "no subagent diagnostic names {status}: {information:?}"
        );
    }
    assert!(!observations.iter().any(|envelope| matches!(
        envelope.observation(),
        AgentObservation::UnrecognizedHarnessEvent { event } if event["subtype"] == "task_started"
    )));
}

// SC-AGENT-7
// REQ-AGENT-35
// REQ-AGENT-64
#[expect(
    clippy::disallowed_methods,
    reason = "real time is only an anti-hang boundary for the native process"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09_backgrounded_subagent_continues_the_invocation() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let subagent = BackgroundSubagent::new(&root);
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;
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
        let AgentOutcome::Completed(CompletedAgentInvocation::Response(response)) = outcome else {
            panic!("the notified turn must settle the invocation, got {outcome:?}");
        };
        assert_eq!(response.as_str(), "final response");
        assert_backgrounded_subagent_activity(&observations);
        assert!(!provider.has_pending_request());
        assert_retained_native_session(&root, &["first response", "final response"]);
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code backgrounded-subagent watchdog expired");
}

// SC-AGENT-7
// REQ-AGENT-35
// INV-HARNESS-7
#[expect(
    clippy::disallowed_methods,
    reason = "real time is only an anti-hang boundary for the native process"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09c_premature_result_waits_for_the_backgrounded_subagent() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let subagent = BackgroundSubagent::new(&root);
        let schema_document = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "integer",
            "minimum": 1,
        });
        let schema_bytes = Arc::<[u8]>::from(serde_json::to_vec(&schema_document).unwrap());
        let observations = RecordingObservationSink::default();
        let running = RunningProductionClaudeCode::launch(
            executable,
            &root,
            &provider,
            AgentValueMode::Result {
                output: Arc::from("result"),
                schema: RetainedJsonSchema::compile(schema_bytes, Arc::new(schema_document))
                    .unwrap(),
            },
            observations.clone(),
        )
        .await_started()
        .await;
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
            PREMATURE_CANDIDATE_FEEDBACK
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
        assert_eq!(rejections, [Arc::<str>::from(PREMATURE_CANDIDATE_FEEDBACK)]);
        assert_backgrounded_subagent_activity(&observations);
        assert!(!provider.has_pending_request());
        provider.shutdown().await;
    })
    .await
    .expect("pinned Claude Code premature-result watchdog expired");
}

/// Ordered thinking segments deliberately carry multibyte, newline, tab, and trailing
/// whitespace so the case fails if Claude Code trims or normalizes reconstructed
/// thinking before restating it in its nominal assistant envelope.
const THINKING_SEGMENTS: [&str; 3] = [
    "Step one: \u{e9}\u{4e2d}\u{6587} ",
    "line\nbreak\ttab  ",
    "trailing space   ",
];

fn assistant_text(observation: &AgentObservation) -> Option<&str> {
    match observation {
        AgentObservation::AssistantText { text } => Some(text.as_ref()),
        _ => None,
    }
}

fn reasoning_text(observation: &AgentObservation) -> Option<&str> {
    match observation {
        AgentObservation::Reasoning { text } => Some(text.as_ref()),
        _ => None,
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_08_correlates_a_nominal_thinking_envelope_before_text() {
    // Every exact-binary case deliberately owns a fresh watchdog, loopback provider, and
    // synthetic root; sharing those resources would let one native case contaminate another.
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
    .await
    .expect("pinned Claude Code nominal-thinking watchdog expired");
}

/// Opaque payload of a native `redacted_thinking` block. The native API delivers this
/// block complete in its `content_block_start`, with no deltas.
const REDACTED_THINKING_DATA: &str = "EmwKAhgBEgy3va3scherzoredacted";

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09_every_block_kind_correlates_its_own_nominal_envelope() {
    // Fresh per-case native resources, as in every other exact-binary case.
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;

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
    })
    .await
    .expect("pinned Claude Code block-kind correlation watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_09b_oversized_image_read_returns_a_bounded_tool_error() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
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
    .await
    .expect("oversized image refusal watchdog expired");
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is used only as an anti-hang watchdog, never as success evidence"
)]
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_10_tool_exchange_normalizes_inputs_and_omits_is_error() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    tokio::time::timeout(WATCHDOG, async {
        let mut provider = LoopbackProvider::start().await;
        let root = SyntheticClaudeCodeRoot::new();
        let observations = RecordingObservationSink::default();
        let running =
            launch_recorded_response_lifecycle(executable, &root, &provider, &observations).await;

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
    })
    .await
    .expect("pinned Claude Code tool-exchange normalization watchdog expired");
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
