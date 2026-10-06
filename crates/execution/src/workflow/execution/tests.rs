use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::{mpsc, watch};

use super::*;
use crate::claude_code::ValidatedClaudeCodeInstallation;
use crate::codex::ValidatedCodexInstallation;
use crate::pi::ValidatedPiInstallation;
use crate::workflow::admission::{
    CancellationPolicy, CancellationReason, CancellationSource, CaptureLimits, EnvironmentSnapshot,
    ExecutionContext, ExecutionPolicyLimits, InputLimits, ResolvedAttachment, ResolvedInput,
    ResolvedInputs, admit_local_workflow,
};
use crate::workflow::agent::scripted::{
    ScriptedAgentDispatcher, ScriptedAgentValue, scripted_agent_dispatcher,
};
use crate::workflow::agent::{
    AgentCompatibilityProfile, AgentFailureCause, AgentLifecycleMilestone, AgentObservation,
    AgentObservationEnvelope, AgentValueKind, WorkflowRunId,
};
use crate::workflow::agent_diagnostics::AgentDiagnosticSessionStore;
use crate::workflow::agent_input::AgentInputStaging;
use crate::workflow::artifact::{ArtifactReadFailure, ArtifactStaging};
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::diagnostic::StepDiagnosticLog;
use crate::workflow::evidence::{
    BlockedDetail, CancellationDetail, FailureCode, NonExecutionCode, NonExecutionDetail,
    Prerequisite,
};
use crate::workflow::input::InputStaging;
use crate::workflow::invocation_accounting::{InvocationAccountingLog, InvocationUsage};
use crate::workflow::observation::{
    CommandOutputSource, ExecutionObservation, ExecutionObserver, NoopExecutionObserver,
    TransitionObservation,
};
use crate::workflow::recovery::{
    RECOVERY_AGENT_INSTRUCTIONS, RECOVERY_CONTEXT_VARIABLE, RecoveryDecisionFailureKind,
    RecoveryHandlerFailure, read_recovery_context,
};
use crate::workflow::resolution;
use crate::workflow::runtime::{
    ExportValue, FailurePhase, RecoveryHandlerOutcome, StepState, StepStateKind, TransitionEvent,
};
use crate::workflow::step_runtime::{AgentExecution, StepFailureCause};
use crate::workflow::test_support::{
    AdmittedFixture,
    step_clock::{TestClock, TestInstant},
};

const FIXTURE_TEST_NAME: &str = "workflow::step_runtime::tests::command_fixture_process";
const STDIN_FIXTURE_TEST_NAME: &str = "workflow::execution::tests::command_stdin_fixture_process";
const FIXTURE_ARGUMENT: &str = "literal * $HOME; [not-a-glob]";
const TEST_WATCHDOG: Duration = Duration::from_secs(10);

type RecordedObservations = Arc<Mutex<Vec<ExecutionObservation<TestInstant>>>>;
type ObservationReceiver = mpsc::UnboundedReceiver<ExecutionObservation<TestInstant>>;

async fn execute_workflow<Clock, Observer, Dispatcher>(
    admitted: AdmittedWorkflow,
    artifacts: &ArtifactStaging,
    inputs: &InputStaging,
    diagnostics: &StepDiagnosticLog,
    agents: AgentExecution<Dispatcher>,
    clock: Clock,
    observer: Observer,
) -> Result<WorkflowExecutionResult<Clock::Instant>, CoordinationError>
where
    Clock: CoordinatorClock,
    Clock::Instant: Sync,
    Observer: ExecutionObserver<Clock::Instant>,
    Dispatcher: WorkflowAgentDispatcher<Clock::Instant, Observer>,
{
    super::execute_workflow(
        admitted,
        artifacts,
        inputs,
        diagnostics,
        agents,
        clock,
        NoopCommitPort,
        observer,
        crate::workflow::process_group::ProcessGuardRegistry::default(),
    )
    .await
}

#[derive(Clone)]
struct RecordingObserver {
    entries: RecordedObservations,
    notifications: mpsc::UnboundedSender<ExecutionObservation<TestInstant>>,
    terminal_gate: Option<TerminalGate>,
    step_success_gate: Option<TerminalGate>,
}

#[derive(Clone)]
struct TerminalGate {
    reached: mpsc::UnboundedSender<()>,
    release: watch::Receiver<bool>,
}

impl RecordingObserver {
    fn new() -> (Self, RecordedObservations, ObservationReceiver) {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let (notifications, observed) = mpsc::unbounded_channel();
        (
            Self {
                entries: Arc::clone(&entries),
                notifications,
                terminal_gate: None,
                step_success_gate: None,
            },
            entries,
            observed,
        )
    }

    fn with_terminal_gate() -> (
        Self,
        RecordedObservations,
        ObservationReceiver,
        mpsc::UnboundedReceiver<()>,
        watch::Sender<bool>,
    ) {
        let (mut observer, entries, observed) = Self::new();
        let (reached, terminal_reached) = mpsc::unbounded_channel();
        let (release, released) = watch::channel(false);
        observer.terminal_gate = Some(TerminalGate {
            reached,
            release: released,
        });
        (observer, entries, observed, terminal_reached, release)
    }

    fn with_step_success_gate() -> (
        Self,
        RecordedObservations,
        ObservationReceiver,
        mpsc::UnboundedReceiver<()>,
        watch::Sender<bool>,
    ) {
        let (mut observer, entries, observed) = Self::new();
        let (reached, success_reached) = mpsc::unbounded_channel();
        let (release, released) = watch::channel(false);
        observer.step_success_gate = Some(TerminalGate {
            reached,
            release: released,
        });
        (observer, entries, observed, success_reached, release)
    }
}

impl ExecutionObserver<TestInstant> for RecordingObserver {
    fn observe(
        &self,
        observation: ExecutionObservation<TestInstant>,
    ) -> impl Future<Output = ()> + Send {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(observation.clone());
        let _ = self.notifications.send(observation.clone());
        let gate = if is_terminal_cancellation(&observation) {
            self.terminal_gate.clone()
        } else if is_step_success(&observation) {
            self.step_success_gate.clone()
        } else {
            None
        };
        async move {
            let Some(mut gate) = gate else {
                return;
            };
            let _ = gate.reached.send(());
            while !*gate.release.borrow_and_update() {
                if gate.release.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

fn is_terminal_cancellation(observation: &ExecutionObservation<TestInstant>) -> bool {
    matches!(
        observation,
        ExecutionObservation::Transition(transition)
            if matches!(
                transition.as_ref(),
                TransitionObservation {
                    event: TransitionEvent::Workflow { to, .. },
                    ..
                } if matches!(to.as_ref(), WorkflowState::Cancelled { .. })
            )
    )
}

fn is_step_success(observation: &ExecutionObservation<TestInstant>) -> bool {
    matches!(
        observation,
        ExecutionObservation::Transition(transition)
            if matches!(
                transition.as_ref(),
                TransitionObservation {
                    event: TransitionEvent::Step {
                        to: StepStateKind::Succeeded,
                        ..
                    },
                    ..
                }
            )
    )
}

struct ExecutionFixture {
    _temporary: tempfile::TempDir,
    execution_root: PathBuf,
    source_root: PathBuf,
    admitted: AdmittedWorkflow,
    artifacts: ArtifactStaging,
    inputs: InputStaging,
    agent_inputs: AgentInputStaging,
    diagnostic_sessions: AgentDiagnosticSessionStore,
}

fn execution_fixture(
    source: &str,
    imports: ResolvedInputs,
    environment: EnvironmentSnapshot,
    cancellation: CancellationSource,
    parallelism: usize,
    log_bytes: u64,
) -> ExecutionFixture {
    execution_fixture_with_source_files(
        source,
        &[],
        imports,
        environment,
        cancellation,
        parallelism,
        log_bytes,
    )
}

fn execution_fixture_with_source_files(
    source: &str,
    source_files: &[(&str, &[u8])],
    imports: ResolvedInputs,
    environment: EnvironmentSnapshot,
    cancellation: CancellationSource,
    parallelism: usize,
    log_bytes: u64,
) -> ExecutionFixture {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("execution");
    let staging_root = temporary.path().join("staging");
    fs::create_dir(&execution_root).unwrap();
    fs::create_dir(&staging_root).unwrap();
    let admitted = AdmittedFixture::new(&source_root, source)
        .with_files(source_files)
        .admit(
            imports,
            ExecutionContext::new(
                execution_root.clone(),
                ExecutionPolicyLimits::new(
                    parallelism,
                    CaptureLimits::new(16, 1024 * 1024, 8 * 1024 * 1024),
                    InputLimits::new(16, 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
                    log_bytes,
                ),
                environment,
                CancellationPolicy::new(cancellation, Duration::from_secs(1)),
            )
            .with_pi_installation(ValidatedPiInstallation::fixture("/validated/pi".into()))
            .with_claude_code_installation(ValidatedClaudeCodeInstallation::fixture(
                "/validated/claude".into(),
            ))
            .with_codex_installation(ValidatedCodexInstallation::fixture(
                "/validated/codex".into(),
            )),
        );
    let artifacts = ArtifactStaging::create(admitted.execution(), &staging_root).unwrap();
    let inputs = InputStaging::create(admitted.execution(), &staging_root).unwrap();
    let agent_inputs = AgentInputStaging::create(admitted.execution(), &staging_root).unwrap();
    let attempt_directory = temporary.path().join("run/attempts/000001");
    fs::create_dir_all(&attempt_directory).unwrap();
    let attempt_handle: OwnedFd = fs::File::open(&attempt_directory).unwrap().into();
    let diagnostic_sessions = AgentDiagnosticSessionStore::create(
        &attempt_handle,
        &attempt_directory,
        Arc::from("00000000-0000-4000-8000-000000000001"),
        1,
    )
    .unwrap();
    ExecutionFixture {
        _temporary: temporary,
        execution_root,
        source_root,
        admitted,
        artifacts,
        inputs,
        agent_inputs,
        diagnostic_sessions,
    }
}

#[tokio::test]
#[ignore = "launched with a live adapter input by the closed-stdin regression test"]
async fn command_stdin_fixture_process() {
    let path = env::var_os("PATH").unwrap_or_else(|| OsString::from("/bin:/usr/bin"));
    let fixture = execution_fixture(
        &format!(
            "schemaVersion: 1\nsteps:\n  eof:\n    kind: cmd\n    command:\n      argv: {}\n",
            serde_json::to_string(&["sh", "-c", "if IFS= read -r unexpected; then exit 91; fi",])
                .unwrap(),
        ),
        ResolvedInputs::default(),
        EnvironmentSnapshot::new([("PATH", path)]),
        CancellationSource::new(),
        1,
        32,
    );
    let result = execute_workflow(
        fixture.admitted,
        &fixture.artifacts,
        &fixture.inputs,
        &StepDiagnosticLog::default(),
        AgentExecution::disabled(),
        TestClock,
        NoopExecutionObserver,
    )
    .await
    .unwrap();
    assert_eq!(result.outcome, RunOutcome::Succeeded);
}

fn agent_runtime(
    fixture: &ExecutionFixture,
    adapter: ScriptedAgentDispatcher,
) -> AgentExecution<ScriptedAgentDispatcher> {
    AgentExecution::enabled(
        WorkflowRunId::from(Arc::from("run-fixed")),
        fixture.agent_inputs.clone(),
        fixture.diagnostic_sessions.clone(),
        adapter,
    )
}

fn agent_runtime_with_accounting(
    fixture: &ExecutionFixture,
    adapter: ScriptedAgentDispatcher,
    accounting: InvocationAccountingLog,
) -> AgentExecution<ScriptedAgentDispatcher> {
    AgentExecution::enabled_with_accounting(
        WorkflowRunId::from(Arc::from("run-fixed")),
        fixture.agent_inputs.clone(),
        fixture.diagnostic_sessions.clone(),
        adapter,
        accounting,
    )
}

fn recovery_profile_source(profile: AgentCompatibilityProfile) -> &'static str {
    match profile {
        AgentCompatibilityProfile::PiJsonV1 => {
            r#"agentProfiles:
  recovery:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
"#
        }
        AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => {
            r#"agentProfiles:
  recovery:
    harness:
      kind: claude_code
      config:
        model: claude-opus-4-1
        effort: xhigh
"#
        }
        AgentCompatibilityProfile::CodexAppServerV1 => {
            r#"agentProfiles:
  recovery:
    harness:
      kind: codex
      config:
        model: gpt-5.4
        effort: xhigh
"#
        }
    }
}

fn assert_stream(
    entries: &[ExecutionObservation<TestInstant>],
    step: &str,
    source: CommandOutputSource,
    expected: &[u8],
) {
    assert_eq!(observed_stream(entries, step, source), expected);
}

fn assert_stream_contains(
    entries: &[ExecutionObservation<TestInstant>],
    step: &str,
    source: CommandOutputSource,
    expected: &[u8],
) {
    assert!(
        observed_stream(entries, step, source)
            .windows(expected.len())
            .any(|window| window == expected)
    );
}

fn observed_stream(
    entries: &[ExecutionObservation<TestInstant>],
    step: &str,
    source: CommandOutputSource,
) -> Vec<u8> {
    let observations = entries
        .iter()
        .filter_map(|entry| match entry {
            ExecutionObservation::CommandOutput(output)
                if output.step == step && output.source == source =>
            {
                Some(output)
            }
            ExecutionObservation::Transition(_)
            | ExecutionObservation::CommandOutput(_)
            | ExecutionObservation::CommandOutputClosed(_)
            | ExecutionObservation::Agent(_) => None,
        })
        .collect::<Vec<_>>();
    assert!(!observations.is_empty());
    assert!(
        observations
            .windows(2)
            .all(|pair| pair[0].sequence.get() + 1 == pair[1].sequence.get())
    );
    assert!(
        observations
            .iter()
            .all(|output| output.invocation == observations[0].invocation)
    );
    let closed = entries
        .iter()
        .filter_map(|entry| match entry {
            ExecutionObservation::CommandOutputClosed(closed)
                if closed.step == step && closed.source == source =>
            {
                Some(closed)
            }
            ExecutionObservation::Transition(_)
            | ExecutionObservation::CommandOutput(_)
            | ExecutionObservation::CommandOutputClosed(_)
            | ExecutionObservation::Agent(_) => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].invocation, observations[0].invocation);
    assert_eq!(
        closed[0].sequence.get(),
        observations.last().unwrap().sequence.get() + 1
    );
    observations
        .iter()
        .flat_map(|output| output.bytes.iter().copied())
        .collect()
}

fn fixture_arguments() -> Vec<String> {
    [
        "--ignored",
        "--exact",
        FIXTURE_TEST_NAME,
        "--nocapture",
        "--skip",
        FIXTURE_ARGUMENT,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn fixture_script(exit_code: i32, role: &str, writes_output: bool) -> String {
    let finish = if writes_output {
        "status=$?; if [ \"$status\" -eq 0 ]; then printf 'retained sibling' > retained.txt; fi; exit \"$status\""
    } else {
        "exit $?"
    };
    format!(
        "WORKFLOW_FIXTURE_EXIT_CODE={exit_code} WORKFLOW_FIXTURE_ROLE={role} \"$1\" \"$@\"; {finish}"
    )
}

fn command_argv(script: &str, executable: &Path, fixture_args: &[String]) -> String {
    serde_json::to_string(
        &std::iter::once("sh".to_owned())
            .chain(["-c".to_owned(), script.to_owned(), "fixture".to_owned()])
            .chain(std::iter::once(executable.to_string_lossy().into_owned()))
            .chain(fixture_args.iter().cloned())
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

fn fixture_environment(listener: &TcpListener) -> EnvironmentSnapshot {
    EnvironmentSnapshot::new([
        (
            OsString::from("WORKFLOW_FIXTURE_SOCKET"),
            OsString::from(listener.local_addr().unwrap().to_string()),
        ),
        (
            OsString::from("WORKFLOW_FIXTURE_EXIT_CODE"),
            OsString::from("0"),
        ),
        (
            OsString::from("PATH"),
            env::var_os("PATH").unwrap_or_else(|| OsString::from("/bin:/usr/bin")),
        ),
    ])
}

async fn accept_fixture(listener: &TcpListener) -> (String, TcpStream) {
    let (stream, _) = listener.accept().await.unwrap();
    let report = read_fixture_event(&stream).await;
    (report["role"].as_str().unwrap().to_owned(), stream)
}

async fn read_fixture_event(stream: &TcpStream) -> Value {
    let mut line = Vec::new();
    let mut buffer = [0_u8; 1];
    loop {
        stream.readable().await.unwrap();
        match stream.try_read(&mut buffer) {
            Ok(0) => panic!("fixture closed before reporting"),
            Ok(read) => {
                line.extend_from_slice(&buffer[..read]);
                if line.last() == Some(&b'\n') {
                    line.pop();
                    return serde_json::from_slice(&line).unwrap();
                }
            }
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure) => panic!("fixture read failed: {failure:?}"),
        }
    }
}

async fn release_fixture(stream: TcpStream) {
    loop {
        stream.writable().await.unwrap();
        match stream.try_write(&[1]) {
            Ok(1) => return,
            Ok(_) => {}
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure) => panic!("fixture release failed: {failure:?}"),
        }
    }
}

async fn wait_for_step_transition(
    observed: &mut mpsc::UnboundedReceiver<ExecutionObservation<TestInstant>>,
    expected_step: &str,
    expected_state: StepStateKind,
) {
    loop {
        if let Some(ExecutionObservation::Transition(transition)) = observed.recv().await
            && matches!(
                transition.as_ref(),
                TransitionObservation {
                    event: TransitionEvent::Step { step, to, .. },
                    ..
                } if step == expected_step && *to == expected_state
            )
        {
            return;
        }
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is allowed only as an anti-hang watchdog, not a behavior assertion"
)]
async fn with_watchdog<Output>(future: impl Future<Output = Output>) -> Output {
    match tokio::time::timeout(TEST_WATCHDOG, future).await {
        Ok(output) => output,
        Err(_) => panic!("workflow execution test watchdog expired"),
    }
}

const AGENT_PROFILE: &str = r#"agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
"#;

mod agent_lifecycle;
mod agent_recovery;
mod agent_results;
mod commands;
