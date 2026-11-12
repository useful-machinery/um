use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::TcpStream as StandardTcpStream;
use std::num::NonZeroUsize;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{self, Stdio};
use std::sync::{Arc, Condvar};
use std::task::Poll;
use std::thread::ThreadId;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command;
use tokio::sync::mpsc;

use super::*;
use crate::workflow::admission::{
    CancellationOperationId, CancellationPolicy, CancellationReason, CancellationSource,
    CaptureLimits, EnvironmentSnapshot, ExecutionContext, ExecutionPolicyLimits, InputLimits,
    ResolvedAttachment, ResolvedInput, ResolvedInputs, admit_workflow,
};
use crate::workflow::agent::{AgentProcessDirective, agent_process_control_channel};
use crate::workflow::artifact::{
    ArtifactStaging, CaptureBoundary, CaptureBoundaryKind, CaptureBoundaryObserver,
    CaptureDeclaration, CaptureFailure, CaptureFailureKind,
};
use crate::workflow::coordinator::{
    CommitPort, CommittedReduction, CoordinationError, Coordinator, CoordinatorClock,
    DriverOccurrenceTestAcknowledgement, OccurrenceReceiver, occurrence_channel,
};
use crate::workflow::diagnostic::{StepDiagnostic, StepDiagnosticLog};
use crate::workflow::evidence::FailureCode;
use crate::workflow::input::InputStaging;
use crate::workflow::process_group::ProcessGuardStoreError;
use crate::workflow::resolution;
use crate::workflow::runtime::{
    self, Action, ActiveStepInvocation, ExportValue, Occurrence, RequestedAction, StepState,
    TargetExecutionNumber, TransitionSequence, WorkflowState,
};
use crate::workflow::test_support::{
    SynchronousGate,
    step_clock::{AdvancingClock, ControlledClock, TestClock, TestInstant},
};
use crate::workflow::test_support::{
    process_fixture_interrupt_handler, run_with_stalled_child_guard, wait_for_stalled_child_guard,
};
use crate::workflow::value::CapturedValue;

const FIXTURE_TEST_NAME: &str = "workflow::step_runtime::tests::command_fixture_process";
const FIXTURE_SOCKET: &str = "WORKFLOW_FIXTURE_SOCKET";
const FIXTURE_EXIT_CODE: &str = "WORKFLOW_FIXTURE_EXIT_CODE";
const FIXTURE_MODE: &str = "WORKFLOW_FIXTURE_MODE";
const FIXTURE_OUTPUT_BYTES: &str = "WORKFLOW_FIXTURE_OUTPUT_BYTES";
const FIXTURE_ROLE: &str = "WORKFLOW_FIXTURE_ROLE";
const FIXTURE_MODE_INTERRUPTIBLE: &str = "interruptible-group";
const FIXTURE_MODE_STUBBORN: &str = "stubborn-group";
const FIXTURE_MODE_PARENT_EXITS: &str = "parent-exits";
const STALLED_GUARD_PID_PATH: &str = "WORKFLOW_STALLED_GUARD_PID_PATH";
const STALLED_GUARD_LAUNCH_FIXTURE: &str =
    "workflow::step_runtime::tests::cancelled_stalled_launch_cleans_worker_fixture";
const FIXTURE_PARENT: &str = "parent";
const FIXTURE_DESCENDANT: &str = "descendant";
const LITERAL_ARGUMENT: &str = "literal * $HOME; [not-a-glob]";
const TEST_WATCHDOG: Duration = Duration::from_secs(10);

fn text_inputs(value: impl Into<Arc<str>>) -> ResolvedInputs {
    ResolvedInputs::new(BTreeMap::from([(
        "request".to_owned(),
        ResolvedInput::Text(value.into()),
    )]))
}

fn text_and_attachments_inputs(
    value: impl Into<Arc<str>>,
    attachments: impl Into<Arc<[ResolvedAttachment]>>,
) -> ResolvedInputs {
    ResolvedInputs::new(BTreeMap::from([
        ("request".to_owned(), ResolvedInput::Text(value.into())),
        (
            "evidence".to_owned(),
            ResolvedInput::Attachments(attachments.into()),
        ),
    ]))
}

type TestOccurrence =
    Occurrence<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>;
type TestReceiver = OccurrenceReceiver<ProvisionalStepOutputs, StepFailureCause, CapturedValue>;
type TestRequestedAction =
    RequestedAction<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>;
type TestRuntimeState = runtime::RuntimeState<StepFailureCause, CapturedValue, TestInstant>;

#[derive(Debug, Deserialize, Serialize)]
struct FixtureReport {
    role: String,
    arguments: Vec<String>,
    current_directory: PathBuf,
    environment: BTreeMap<String, String>,
    process_group_leader: bool,
    process_group: i32,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct FixtureEvent {
    event: String,
}

enum FixtureCommand {
    Interrupted,
    Released,
}

#[test]
#[ignore = "launched only as the repository-owned workflow command fixture"]
fn command_fixture_process() {
    let socket = std::env::var(FIXTURE_SOCKET).unwrap();
    let exit_code = std::env::var(FIXTURE_EXIT_CODE)
        .unwrap()
        .parse::<i32>()
        .unwrap();
    let mode = std::env::var(FIXTURE_MODE).ok();
    let role = std::env::var(FIXTURE_ROLE).unwrap_or_else(|_| "single".to_owned());
    let mut descendant = if role == FIXTURE_PARENT {
        Some(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(fixture_arguments())
                .env(FIXTURE_ROLE, FIXTURE_DESCENDANT)
                .spawn()
                .unwrap(),
        )
    } else {
        None
    };

    let mut control = StandardTcpStream::connect(socket).unwrap();
    let commands = if mode.as_deref() == Some(FIXTURE_MODE_INTERRUPTIBLE) {
        let (commands, command) = std::sync::mpsc::channel();
        let interrupted_command = commands.clone();
        let mut interrupted = control.try_clone().unwrap();
        process_fixture_interrupt_handler(move || {
            interrupted
                .write_all(b"{\"event\":\"interrupted\"}\n")
                .unwrap();
            interrupted.flush().unwrap();
            interrupted_command
                .send(FixtureCommand::Interrupted)
                .unwrap();
        });
        Some((commands, command))
    } else {
        if mode.as_deref() == Some(FIXTURE_MODE_STUBBORN) {
            let mut interrupted = control.try_clone().unwrap();
            process_fixture_interrupt_handler(move || {
                interrupted
                    .write_all(b"{\"event\":\"interrupted\"}\n")
                    .unwrap();
                interrupted.flush().unwrap();
            });
        }
        None
    };

    let process_id = rustix::process::getpid();
    let process_group = rustix::process::getpgrp();
    let report = FixtureReport {
        role: role.clone(),
        arguments: std::env::args().skip(1).collect(),
        current_directory: std::env::current_dir().unwrap(),
        environment: std::env::vars().collect(),
        process_group_leader: process_id == process_group,
        process_group: process_group.as_raw_pid(),
    };
    serde_json::to_writer(&mut control, &report).unwrap();
    control.write_all(b"\n").unwrap();
    control.flush().unwrap();

    if let Ok(output_bytes) = std::env::var(FIXTURE_OUTPUT_BYTES) {
        let output_bytes = output_bytes.parse::<usize>().unwrap();
        let mut standard_output = std::io::stdout().lock();
        standard_output
            .write_all(&vec![b'o'; output_bytes])
            .unwrap();
        standard_output.flush().unwrap();
        let mut standard_error = std::io::stderr().lock();
        standard_error.write_all(&vec![b'e'; output_bytes]).unwrap();
        standard_error.flush().unwrap();
        control
            .write_all(b"{\"event\":\"output-written\"}\n")
            .unwrap();
        control.flush().unwrap();
    }

    if let Some((released_command, command)) = commands {
        let mut released = control.try_clone().unwrap();
        drop(std::thread::spawn(move || {
            let mut release = [0_u8; 1];
            released.read_exact(&mut release).unwrap();
            assert_eq!(release, [1]);
            released_command.send(FixtureCommand::Released).unwrap();
        }));
        match command.recv().unwrap() {
            FixtureCommand::Interrupted | FixtureCommand::Released => {}
        }
    } else if mode.as_deref() == Some(FIXTURE_MODE_PARENT_EXITS) && role == FIXTURE_DESCENDANT {
        let mut probe = [0_u8; 1];
        control.read_exact(&mut probe).unwrap();
        assert_eq!(probe, [2]);
        control.write_all(b"{\"event\":\"alive\"}\n").unwrap();
        control.flush().unwrap();
        let mut release = [0_u8; 1];
        control.read_exact(&mut release).unwrap();
        assert_eq!(release, [1]);
    } else {
        let mut release = [0_u8; 1];
        control.read_exact(&mut release).unwrap();
        assert_eq!(release, [1]);
    }
    if mode.as_deref() != Some(FIXTURE_MODE_PARENT_EXITS)
        && let Some(descendant) = descendant.as_mut()
    {
        assert!(descendant.wait().unwrap().success());
    }
    if exit_code != 0 {
        process::exit(exit_code);
    }
}

#[derive(Clone, Copy)]
enum ProgramForm {
    Bare,
    Relative,
    Absolute,
}

struct FixtureRun {
    report: FixtureReport,
    action: ActionId,
    terminal: TestOccurrence,
    diagnostic: StepDiagnostic,
}

struct PreparedFixtureCommand {
    _temporary: tempfile::TempDir,
    cwd: PathBuf,
    listener: TcpListener,
    admitted: AdmittedWorkflow,
}

struct TestArtifacts {
    _temporary: tempfile::TempDir,
    staging: ArtifactStaging,
    inputs: InputStaging,
}

enum StagingBindingMismatch {
    Artifact,
    Input,
}

struct CaptureBoundaryGate {
    reached: mpsc::UnboundedSender<CaptureBoundary>,
    permits: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl CaptureBoundaryObserver for CaptureBoundaryGate {
    fn reached(&self, boundary: CaptureBoundary) {
        self.reached.send(boundary).unwrap();
        self.permits.lock().unwrap().recv().unwrap();
    }
}

struct CaptureBoundaryControl {
    reached: mpsc::UnboundedReceiver<CaptureBoundary>,
    permits: std::sync::mpsc::Sender<()>,
}

impl CaptureBoundaryControl {
    async fn next(&mut self) -> CaptureBoundary {
        self.reached.recv().await.unwrap()
    }

    fn release(&self) {
        self.permits.send(()).unwrap();
    }
}

fn capture_boundary_gate() -> (Arc<dyn CaptureBoundaryObserver>, CaptureBoundaryControl) {
    let (reached, pending) = mpsc::unbounded_channel();
    let (permits, permission) = std::sync::mpsc::channel();
    (
        Arc::new(CaptureBoundaryGate {
            reached,
            permits: Mutex::new(permission),
        }),
        CaptureBoundaryControl {
            reached: pending,
            permits,
        },
    )
}

fn test_artifacts(admitted: &AdmittedWorkflow) -> TestArtifacts {
    let temporary = tempfile::tempdir().unwrap();
    let staging = ArtifactStaging::create(admitted.execution(), temporary.path()).unwrap();
    let inputs = InputStaging::create(admitted.execution(), temporary.path()).unwrap();
    TestArtifacts {
        _temporary: temporary,
        staging,
        inputs,
    }
}

struct PreparedGroupCommand {
    _temporary: tempfile::TempDir,
    listener: TcpListener,
    admitted: AdmittedWorkflow,
}
type WorkflowCommit = CommittedReduction<StepFailureCause, CapturedValue, TestInstant>;

struct RecordingCommitPort {
    commits: mpsc::UnboundedSender<WorkflowCommit>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GuardStoreOperation {
    Register,
    Release,
    Quiesce,
}

struct GuardStoreState {
    release_register: bool,
    operations: Vec<(GuardStoreOperation, ThreadId)>,
}

struct BlockingGuardStore {
    pause_at: GuardStoreOperation,
    state: Arc<(Mutex<GuardStoreState>, Condvar)>,
    register_started: mpsc::UnboundedSender<()>,
}

struct BlockingGuardStoreControl {
    state: Arc<(Mutex<GuardStoreState>, Condvar)>,
    register_started: mpsc::UnboundedReceiver<()>,
}

impl BlockingGuardStore {
    fn new() -> (Self, BlockingGuardStoreControl) {
        Self::pausing_at(GuardStoreOperation::Register)
    }

    fn pausing_at(pause_at: GuardStoreOperation) -> (Self, BlockingGuardStoreControl) {
        let state = Arc::new((
            Mutex::new(GuardStoreState {
                release_register: false,
                operations: Vec::new(),
            }),
            Condvar::new(),
        ));
        let (register_started, started) = mpsc::unbounded_channel();
        (
            Self {
                pause_at,
                state: Arc::clone(&state),
                register_started,
            },
            BlockingGuardStoreControl {
                state,
                register_started: started,
            },
        )
    }

    fn record(&self, operation: GuardStoreOperation) {
        let (state, release) = &*self.state;
        let mut state = state.lock().unwrap();
        state
            .operations
            .push((operation, std::thread::current().id()));
        if operation == self.pause_at {
            let _ = self.register_started.send(());
            while !state.release_register {
                state = release.wait(state).unwrap();
            }
        }
    }
}

impl crate::workflow::process_group::DurableProcessGuardStore for BlockingGuardStore {
    fn register(
        &self,
        _step: &str,
        _action_id: u64,
        _identity: &AuthenticatedProcessGroup,
    ) -> Result<String, ProcessGuardStoreError> {
        self.record(GuardStoreOperation::Register);
        Ok("guard-operation-fixture".to_owned())
    }

    fn mark_released(&self, _guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        self.record(GuardStoreOperation::Release);
        Ok(())
    }

    fn mark_quiesced(&self, _guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        self.record(GuardStoreOperation::Quiesce);
        Ok(())
    }
}

impl BlockingGuardStoreControl {
    async fn wait_until_register_started(&mut self) {
        self.register_started.recv().await.unwrap();
    }

    fn release_register(&self) {
        let (state, release) = &*self.state;
        state.lock().unwrap().release_register = true;
        release.notify_one();
    }

    fn operations(&self) -> Vec<(GuardStoreOperation, ThreadId)> {
        self.state.0.lock().unwrap().operations.clone()
    }
}

impl CommitPort<WorkflowCommit> for RecordingCommitPort {
    type Error = std::convert::Infallible;

    fn commit(&mut self, commit: WorkflowCommit) -> impl Future<Output = Result<(), Self::Error>> {
        let _ = self.commits.send(commit);
        std::future::ready(Ok(()))
    }
}

#[tokio::test]
async fn command_launch_and_durable_guard_writes_leave_the_executor_responsive() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_INTERRUPTIBLE).await;
        let action = start_actions(&admitted)["task"];
        let (store, mut control) = BlockingGuardStore::new();
        let registry = ProcessGuardRegistry::durable(Arc::new(store));
        let (clock, mut clock_control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let responsiveness_clock = clock.clone();
        let responsive_at = TestInstant(Duration::from_secs(17));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let runtime = StepRuntime::with_observer(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            StepDiagnosticLog::default(),
            sender,
            clock,
            NoopExecutionObserver,
            AgentExecution::disabled(),
            registry,
        );
        let executor_thread = std::thread::current().id();
        let execution =
            tokio::spawn(async move { runtime.execute_step("task".to_owned(), action).await });

        control.wait_until_register_started().await;
        let (responsive, response) = oneshot::channel();
        drop(tokio::spawn(async move {
            responsiveness_clock.wait_until(responsive_at).await;
            let _ = responsive.send(());
        }));
        assert_eq!(clock_control.next_deadline().await, responsive_at);
        clock_control.release();
        response.await.unwrap();
        control.release_register();

        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action,
            }
        );
        let processes = accept_group(&listener).await;
        release_group(&processes).await;
        assert!(matches!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepExecutionCompleted { action: completed, .. } if completed == action
        ));
        assert_eq!(execution.await.unwrap(), Ok(()));

        let operations = control.operations();
        assert_eq!(
            operations
                .iter()
                .map(|(operation, _)| *operation)
                .collect::<Vec<_>>(),
            vec![
                GuardStoreOperation::Register,
                GuardStoreOperation::Release,
                GuardStoreOperation::Quiesce,
            ]
        );
        assert!(
            operations
                .iter()
                .all(|(_, operation_thread)| *operation_thread != executor_thread)
        );
    })
    .await;
}

#[tokio::test]
async fn cancellation_during_a_stalled_launch_quiesces_before_the_controlled_deadline() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_INTERRUPTIBLE).await;
        let deadline = TestInstant(Duration::from_secs(31));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let (clock, control_clock) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (store, mut guard_control) = BlockingGuardStore::new();
        let registry = ProcessGuardRegistry::durable(Arc::new(store));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::with_observer(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            StepDiagnosticLog::default(),
            sender,
            clock,
            NoopExecutionObserver,
            AgentExecution::disabled(),
            registry,
        );

        runtime.release(start).await;
        guard_control.wait_until_register_started().await;
        runtime.release(cancel).await;
        guard_control.release_register();

        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );
        assert_eq!(control_clock.active_waiters(), 0);
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(
            guard_control
                .operations()
                .into_iter()
                .map(|(operation, _)| operation)
                .collect::<Vec<_>>(),
            vec![GuardStoreOperation::Register, GuardStoreOperation::Quiesce]
        );
        let listener = listener.into_std().unwrap();
        assert!(
            matches!(listener.accept(), Err(failure) if failure.kind() == io::ErrorKind::WouldBlock)
        );
    })
    .await;
}

#[tokio::test]
async fn cancelled_stalled_launch_does_not_report_quiescence_while_worker_is_live() {
    with_watchdog(async {
        let status =
            run_with_stalled_child_guard(STALLED_GUARD_LAUNCH_FIXTURE, STALLED_GUARD_PID_PATH)
                .await;
        assert!(status.success());
    })
    .await;
}

#[tokio::test]
#[ignore = "launched in an isolated test-binary tree by the stalled guard integration test"]
async fn cancelled_stalled_launch_cleans_worker_fixture() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener: _,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_INTERRUPTIBLE).await;
        let deadline = TestInstant(Duration::from_secs(31));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let start_action = start.id;
        let cancel_action = cancel.id;
        let (store, guard_control) = BlockingGuardStore::new();
        guard_control.release_register();
        let registry = ProcessGuardRegistry::durable(Arc::new(store));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::with_observer(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            StepDiagnosticLog::default(),
            sender,
            TestClock,
            NoopExecutionObserver,
            AgentExecution::disabled(),
            registry,
        );

        runtime.release(start).await;
        let worker_pid = wait_for_stalled_child_guard(STALLED_GUARD_PID_PATH).await;
        let worker = Pid::from_raw(worker_pid).unwrap();
        assert!(rustix::process::test_kill_process(worker).is_ok());
        runtime.release(cancel).await;
        assert!(runtime.cancellation_for(start_action).is_some());

        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );
        assert!(rustix::process::test_kill_process(worker).is_err());
    })
    .await;
}

// REQ-WFEXEC-19, REQ-WFEXEC-22
#[tokio::test]
async fn command_uses_contained_cwd_literal_argv_and_isolates_parent_environment() {
    with_watchdog(async {
        for form in [
            ProgramForm::Bare,
            ProgramForm::Relative,
            ProgramForm::Absolute,
        ] {
            let run = run_fixture_command(form, 0).await;
            assert_eq!(run.report.arguments, fixture_arguments());
            let expected_environment =
                ["EXPLICIT_VALUE", "PATH", FIXTURE_EXIT_CODE, FIXTURE_SOCKET];
            let observed_environment = run
                .report
                .environment
                .keys()
                // macOS may add this process-local Foundation setting after
                // Scherzo starts an otherwise environment-cleared child.
                .filter(|key| {
                    !cfg!(target_os = "macos") || key.as_str() != "__CF_USER_TEXT_ENCODING"
                })
                .map(String::as_str)
                .collect::<Vec<_>>();
            assert_eq!(observed_environment, expected_environment);
            assert_eq!(
                run.report.environment.get("EXPLICIT_VALUE"),
                Some(&"from-admission".to_owned())
            );
            assert!(run.report.process_group_leader);
            assert_eq!(
                run.terminal,
                Occurrence::StepExecutionCompleted {
                    step: "task".to_owned(),
                    action: run.action,
                    provisional: ProvisionalStepOutputs::command(),
                }
            );
        }
    })
    .await;
}

// REQ-WFEXEC-22, REQ-WFEXEC-25
#[tokio::test]
async fn concurrent_consumers_receive_private_inputs_and_reserved_environment() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let executable = std::env::current_exe().unwrap();
        let arguments = fixture_arguments();
        let argv = std::iter::once(executable.to_str().unwrap())
            .chain(arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let mut source = String::from(
            "schemaVersion: 1\ninputs:\n  request: {kind: text}\n  evidence: {kind: attachments}\nsteps:\n",
        );
        for step in ["alpha", "beta"] {
            source.push_str(&format!(
                "  {step}:\n    kind: cmd\n    inputs:\n      attachments:\n        ref: inputs.evidence\n      prompt:\n        ref: inputs.request\n    command:\n      argv: {}\n",
                serde_json::to_string(&argv).unwrap()
            ));
        }
        let environment =
            fixture_environment(&control_address, 0, &execution_root, None);
        let inputs = text_and_attachments_inputs(
            "shared prompt",
            [ResolvedAttachment::new(
                Arc::from("application/octet-stream"),
                Arc::from([0_u8, 0xff, 7]),
            )],
        );
        let admitted = admit_fixture_with_inputs(
            temporary.path(),
            &execution_root,
            &source,
            environment,
            FixtureExecution {
                limits: ExecutionPolicyLimits::new(
                    2,
                    CaptureLimits::new(16, 1024, 4096),
                    InputLimits::new(8, 1024, 4096, 4096),
                    1024,
                ),
                imports: inputs,
            },
        );
        let artifacts = test_artifacts(&admitted);
        let diagnostics = StepDiagnosticLog::default();
        let (commit_sender, _commits) = mpsc::unbounded_channel();
        let execution = execute_workflow(
            admitted,
            &artifacts.staging,
            &artifacts.inputs,
            &diagnostics,
            TestClock,
            RecordingCommitPort {
                commits: commit_sender,
            },
        );
        let commands = async {
            let (first_control, first_report) = accept_report(&listener).await;
            let (second_control, second_report) = accept_report(&listener).await;
            let expected_environment = [
                "EXPLICIT_VALUE",
                "PATH",
                "UM_STEP_INPUTS",
                FIXTURE_EXIT_CODE,
                FIXTURE_SOCKET,
            ];
            for report in [&first_report, &second_report] {
                let observed_environment = report
                    .environment
                    .keys()
                    // macOS may add this process-local Foundation setting after
                    // Scherzo starts an otherwise environment-cleared child.
                    .filter(|key| {
                        !cfg!(target_os = "macos") || key.as_str() != "__CF_USER_TEXT_ENCODING"
                    })
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                assert_eq!(observed_environment, expected_environment);
                assert!(!report.environment.contains_key("UM_INHERITED"));
            }
            let first_path = PathBuf::from(&first_report.environment["UM_STEP_INPUTS"]);
            let second_path = PathBuf::from(&second_report.environment["UM_STEP_INPUTS"]);
            assert_ne!(first_path, second_path);
            assert_eq!(artifacts.inputs.reservation_usage(), (2, 6, 32));
            assert_eq!(
                fs::read(first_path.join("values/prompt")).unwrap(),
                b"shared prompt"
            );
            assert_eq!(
                fs::read(second_path.join("values/prompt")).unwrap(),
                b"shared prompt"
            );
            fs::set_permissions(
                first_path.join("values/prompt"),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
            fs::write(first_path.join("values/prompt"), b"first changed").unwrap();
            assert_eq!(
                fs::read(second_path.join("values/prompt")).unwrap(),
                b"shared prompt"
            );
            release(first_control).await;
            release(second_control).await;
            [first_path, second_path]
        };

        let (result, paths) = tokio::join!(execution, commands);
        assert_eq!(result.unwrap().state.workflow, WorkflowState::Succeeded);
        assert!(paths.into_iter().all(|path| !path.exists()));
        assert_eq!(artifacts.inputs.reservation_usage(), (0, 0, 0));
    })
    .await;
}

// REQ-WFEXEC-25
#[tokio::test]
async fn concurrent_consumers_copy_one_committed_file_without_shared_mutation() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        fs::write(execution_root.join("report.bin"), b"captured file").unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let executable = std::env::current_exe().unwrap();
        let arguments = fixture_arguments();
        let argv = std::iter::once(executable.to_str().unwrap())
            .chain(arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        // Hold the producer at the fixture boundary so it cannot exit before
        // the direct test runtime authenticates its process-group identity.
        let mut source = format!(
            "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: {}\n    outputs:\n      report:\n        kind: file\n        from: path\n        path: report.bin\n        mediaType: application/fixture\n",
            serde_json::to_string(&argv).unwrap()
        );
        for step in ["alpha", "beta"] {
            source.push_str(&format!(
                "  {step}:\n    kind: cmd\n    dependsOn: [produce]\n    inputs:\n      artifact:\n        ref: outputs.produce.report\n    command:\n      argv: {}\n",
                serde_json::to_string(&argv).unwrap()
            ));
        }
        let admitted = admit_fixture(
            temporary.path(),
            &execution_root,
            &source,
            fixture_environment(&control_address, 0, &execution_root, None),
            2,
        );
        let artifacts = test_artifacts(&admitted);
        let diagnostics = StepDiagnosticLog::default();
        let (commit_sender, _commits) = mpsc::unbounded_channel();
        let execution = execute_workflow(
            admitted,
            &artifacts.staging,
            &artifacts.inputs,
            &diagnostics,
            TestClock,
            RecordingCommitPort {
                commits: commit_sender,
            },
        );
        let commands = async {
            let (producer_control, _) = accept_report(&listener).await;
            release(producer_control).await;
            let (first_control, first_report) = accept_report(&listener).await;
            let (second_control, second_report) = accept_report(&listener).await;
            let first_path = PathBuf::from(&first_report.environment["UM_STEP_INPUTS"]);
            let second_path = PathBuf::from(&second_report.environment["UM_STEP_INPUTS"]);
            assert_ne!(first_path, second_path);
            assert_eq!(artifacts.inputs.reservation_usage(), (2, 2, 26));
            let first_file = first_path.join("values/artifact");
            let second_file = second_path.join("values/artifact");
            assert_eq!(fs::read(&first_file).unwrap(), b"captured file");
            assert_eq!(fs::read(&second_file).unwrap(), b"captured file");
            fs::set_permissions(&first_file, fs::Permissions::from_mode(0o600)).unwrap();
            fs::write(&first_file, b"first changed").unwrap();
            assert_eq!(fs::read(&second_file).unwrap(), b"captured file");
            release(first_control).await;
            release(second_control).await;
            [first_path, second_path]
        };

        let (result, paths) = tokio::join!(execution, commands);
        let result = result.unwrap();
        assert_eq!(result.state.workflow, WorkflowState::Succeeded);
        assert!(paths.into_iter().all(|path| !path.exists()));
        let StepState::Succeeded { outputs } = &result.state.steps["produce"].state else {
            panic!("producer did not commit its file");
        };
        let captured = outputs["report"].as_file().unwrap();
        let mut captured_bytes = Vec::new();
        artifacts
            .staging
            .copy_to(captured.handle(), &mut captured_bytes)
            .unwrap();
        assert_eq!(captured_bytes, b"captured file");
        assert_eq!(fs::read(execution_root.join("report.bin")).unwrap(), b"captured file");
        assert_eq!(artifacts.inputs.reservation_usage(), (0, 0, 0));
    })
    .await;
}

#[tokio::test]
async fn workflow_execution_dispatches_start_actions_to_the_step_runtime() {
    with_watchdog(async {
        let PreparedFixtureCommand {
            _temporary,
            cwd,
            listener,
            admitted,
        } = prepare_fixture_command(ProgramForm::Absolute, 0).await;
        let (commit_sender, mut commit_receiver) = mpsc::unbounded_channel();
        let artifacts = test_artifacts(&admitted);
        let diagnostics = StepDiagnosticLog::default();
        let execution = execute_workflow(
            admitted,
            &artifacts.staging,
            &artifacts.inputs,
            &diagnostics,
            TestClock,
            RecordingCommitPort {
                commits: commit_sender,
            },
        );
        let command = async {
            let (control, report) = accept_report(&listener).await;
            assert_eq!(report.current_directory, fs::canonicalize(cwd).unwrap());
            release(control).await;
        };

        let (result, ()) = tokio::join!(execution, command);
        let result = result.unwrap();
        assert_eq!(result.state.workflow, WorkflowState::Succeeded);
        assert_eq!(result.last_occurrence_ordinal.get(), 3);
        let diagnostic = diagnostics.get("task").unwrap();
        assert!(diagnostic.standard_output().fully_drained());
        assert!(diagnostic.standard_error().fully_drained());

        let mut commits = Vec::new();
        while let Ok(commit) = commit_receiver.try_recv() {
            commits.push(commit);
        }
        assert_eq!(commits.len(), 3);
        let started_action = commits[0].state.steps["task"].current_action.unwrap();
        assert_eq!(commits[0].state.steps["task"].state, StepState::Starting);
        assert_eq!(
            commits[1].state.steps["task"],
            crate::workflow::runtime::StepRuntimeState {
                state: StepState::Running,
                current_action: Some(started_action),
                target_execution: Some(TargetExecutionNumber::FIRST),
                target_invocation: Some(started_action),
                active_invocation: Some(ActiveStepInvocation::Target {
                    execution_number: TargetExecutionNumber::FIRST,
                }),
                recovery: None,
                condition_passed: false,
                failure_kind: None,
            }
        );
        assert_eq!(
            commits[2].state.steps["task"].state,
            StepState::Succeeded {
                outputs: BTreeMap::new(),
            }
        );
    })
    .await;
}

#[tokio::test]
async fn transition_capacity_failure_quiesces_a_running_step() {
    with_watchdog(async {
        let PreparedFixtureCommand {
            _temporary,
            listener,
            mut admitted,
            ..
        } = prepare_fixture_command(ProgramForm::Absolute, 0).await;
        admitted.set_transition_ceiling(1);
        let artifacts = test_artifacts(&admitted);
        let diagnostics = StepDiagnosticLog::default();
        let (commit_sender, _commits) = mpsc::unbounded_channel();
        let execution = execute_workflow(
            admitted,
            &artifacts.staging,
            &artifacts.inputs,
            &diagnostics,
            TestClock,
            RecordingCommitPort {
                commits: commit_sender,
            },
        );
        tokio::pin!(execution);
        let result = tokio::select! {
            result = &mut execution => result,
            accepted = listener.accept() => {
                let (control, _peer) = accepted.unwrap();
                // The established connection is the fixture's readiness signal. Once the
                // transition failure begins quiescence, termination may preempt its report.
                let result = (&mut execution).await;
                drop(control);
                result
            }
        };
        assert_eq!(result, Err(CoordinationError::TransitionCapacityExceeded));
    })
    .await;
}

#[tokio::test]
async fn semantic_outputs_capture_cancellation_discards_stale_delivery() {
    let PreparedFixtureCommand {
        _temporary,
        cwd,
        admitted,
        ..
    } = prepare_fixture_command_with_output(ProgramForm::Absolute, 0).await;
    fs::write(cwd.join("report.txt"), b"uncommitted report").unwrap();
    let artifacts = test_artifacts(&admitted);
    let initialized = runtime::initialize::<
        ProvisionalStepOutputs,
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >(&admitted, None);
    let start = initialized
        .actions
        .iter()
        .find(|requested| matches!(&requested.action, Action::StartStep { step, .. } if step == "task"))
        .unwrap();
    let running =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &initialized.state,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start.id,
            },
        );
    let capture_requested =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &running.state,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action: start.id,
                provisional: ProvisionalStepOutputs::command(),
            },
        );
    let capture = capture_requested
        .actions
        .iter()
        .find(|requested| {
            matches!(&requested.action, Action::CaptureOutputs { step, .. } if step == "task")
        })
        .unwrap();
    let cancelled =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &capture_requested.state,
            Occurrence::CancellationOperationRequested {
                operation: CancellationOperationId::fixture(1),
                reason: CancellationReason::UserRequest,
                deadline: TestInstant(Duration::from_secs(1)),
            },
        );
    assert_eq!(
        cancelled.state.steps["task"].state,
        StepState::Cancelling {
            detail: super::super::evidence::CancellationDetail::new(
                CancellationReason::UserRequest,
            ),
        }
    );
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    let step_runtime = StepRuntime::new(
        admitted,
        artifacts.staging.clone(),
        artifacts.inputs.clone(),
        sender,
        TestClock,
    );

    step_runtime
        .capture_outputs("task".to_owned(), capture.id)
        .await
        .unwrap();
    let (completion, acknowledgement) = next_acknowledged_occurrence(&mut receiver).await;
    assert_eq!(artifacts.staging.staged_artifact_count(), 1);
    assert_eq!(artifacts.staging.budget_usage(), (0, 0));
    assert_eq!(artifacts.staging.reservation_usage(), (1, 18));
    let stale = runtime::reduce(&cancelled.state, completion);
    acknowledgement.resolve(stale.occurrence_accepted).await;

    assert!(stale.events.is_empty());
    assert_eq!(stale.state, cancelled.state);
    assert_eq!(artifacts.staging.staged_artifact_count(), 0);
    assert_eq!(artifacts.staging.budget_usage(), (0, 0));
    assert_eq!(artifacts.staging.reservation_usage(), (0, 0));
}

#[tokio::test]
async fn semantic_outputs_capture_cancellation_prevents_source_access() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        fs::write(execution_root.join("alpha.bin"), b"alpha").unwrap();
        fs::write(execution_root.join("beta.bin"), b"beta").unwrap();
        let source = r#"schemaVersion: 1
steps:
  alpha:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      alphaOutput:
        kind: file
        from: path
        path: alpha.bin
        mediaType: application/octet-stream
  beta:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      betaOutput:
        kind: file
        from: path
        path: beta.bin
        mediaType: application/octet-stream
"#;
        let admitted = admit_fixture(
            temporary.path(),
            &execution_root,
            source,
            EnvironmentSnapshot::default(),
            2,
        );
        let (capturing, captures) = capturing_actions(&admitted);
        let cancelled = runtime::reduce::<
            ProvisionalStepOutputs,
            StepFailureCause,
            CapturedValue,
            TestInstant,
        >(
            &capturing,
            Occurrence::CancellationOperationRequested {
                operation: CancellationOperationId::fixture(1),
                reason: CancellationReason::UserRequest,
                deadline: TestInstant(Duration::from_secs(1)),
            },
        );
        let cancellations = cancelled
            .actions
            .iter()
            .filter_map(|requested| match &requested.action {
                Action::CancelStep { step, .. } => Some((step.clone(), requested.clone())),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(8).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut driver = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            TestClock,
        );
        let (observer, mut boundaries) = capture_boundary_gate();
        driver.set_capture_observer(observer);

        driver.release(captures["alpha"].clone()).await;
        let first_boundary = boundaries.next().await;
        assert_eq!(first_boundary.output_identity.as_ref(), "alphaOutput");
        assert_eq!(first_boundary.kind, CaptureBoundaryKind::BeforeSourceOpen);
        driver.release(captures["beta"].clone()).await;
        for cancellation in cancellations.values() {
            driver.release(cancellation.clone()).await;
            driver.release(cancellation.clone()).await;
        }
        fs::remove_file(execution_root.join("beta.bin")).unwrap();
        assert!(receiver.try_recv().is_none());

        boundaries.release();
        let mut quiesced = BTreeMap::new();
        for _ in 0..2 {
            let Occurrence::StepQuiesced { step, action } = next_occurrence(&mut receiver).await
            else {
                panic!("cancelled capture reported a lifecycle completion");
            };
            quiesced.insert(step, action);
        }
        assert_eq!(quiesced["alpha"], cancellations["alpha"].id);
        assert_eq!(quiesced["beta"], cancellations["beta"].id);
        assert!(boundaries.reached.try_recv().is_err());
        assert_eq!(artifacts.staging.staged_artifact_count(), 0);
        assert_eq!(artifacts.staging.budget_usage(), (0, 0));
        assert_eq!(artifacts.staging.reservation_usage(), (0, 0));
        assert_eq!(driver.active_work_count(), 0);
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

#[tokio::test]
async fn semantic_outputs_capture_cancellation_during_copy_quiesces() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        fs::write(execution_root.join("report.bin"), b"one gated chunk").unwrap();
        let source = r#"schemaVersion: 1
steps:
  task:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      report:
        kind: file
        from: path
        path: report.bin
        mediaType: application/octet-stream
"#;
        let admitted = admit_fixture(
            temporary.path(),
            &execution_root,
            source,
            EnvironmentSnapshot::default(),
            1,
        );
        let (capturing, captures) = capturing_actions(&admitted);
        let cancelled = runtime::reduce::<
            ProvisionalStepOutputs,
            StepFailureCause,
            CapturedValue,
            TestInstant,
        >(
            &capturing,
            Occurrence::CancellationOperationRequested {
                operation: CancellationOperationId::fixture(1),
                reason: CancellationReason::UserRequest,
                deadline: TestInstant(Duration::from_secs(1)),
            },
        );
        let cancellation = cancelled.actions[0].clone();
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut driver = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            TestClock,
        );
        let (observer, mut boundaries) = capture_boundary_gate();
        driver.set_capture_observer(observer);
        driver.release(captures["task"].clone()).await;

        for expected in [
            CaptureBoundaryKind::BeforeSourceOpen,
            CaptureBoundaryKind::BeforeRead,
            CaptureBoundaryKind::BeforeWrite,
        ] {
            let boundary = boundaries.next().await;
            assert_eq!(boundary.output_identity.as_ref(), "report");
            assert_eq!(boundary.kind, expected);
            boundaries.release();
        }
        let after_first_write = boundaries.next().await;
        assert_eq!(after_first_write.kind, CaptureBoundaryKind::AfterWrite);
        assert_eq!(artifacts.staging.staged_artifact_count(), 1);

        driver.release(cancellation.clone()).await;
        driver.release(cancellation.clone()).await;
        let force_abort = RequestedAction {
            id: ActionId {
                transition_sequence: TransitionSequence(1_u64 << 63),
            },
            action: Action::ForceAbortStep {
                step: "task".to_owned(),
                active: ActiveStepInvocation::Target {
                    execution_number: TargetExecutionNumber::FIRST,
                },
                reason: CancellationReason::UserRequest,
                deadline: TestInstant(Duration::ZERO),
            },
        };
        driver.release(force_abort.clone()).await;
        driver.release(force_abort.clone()).await;
        assert!(receiver.try_recv().is_none());
        boundaries.release();

        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: force_abort.id,
            }
        );
        assert!(boundaries.reached.try_recv().is_err());
        assert_eq!(artifacts.staging.staged_artifact_count(), 0);
        assert_eq!(artifacts.staging.budget_usage(), (0, 0));
        assert_eq!(artifacts.staging.reservation_usage(), (0, 0));
        assert_eq!(driver.active_work_count(), 0);
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

// REQ-WFEXEC-46
#[tokio::test]
async fn cancel_first_rejects_a_ready_candidate_and_commit_first_retains_it() {
    with_watchdog(async {
        for cancel_first in [true, false] {
            let temporary = tempfile::tempdir().unwrap();
            let execution_root = temporary.path().join("execution");
            fs::create_dir(&execution_root).unwrap();
            fs::write(execution_root.join("report.bin"), b"candidate").unwrap();
            let source = r#"schemaVersion: 1
steps:
  task:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      report:
        kind: file
        from: path
        path: report.bin
        mediaType: application/octet-stream
exports:
  reportExport:
    ref: outputs.task.report
"#;
            let admitted = admit_fixture(
                temporary.path(),
                &execution_root,
                source,
                EnvironmentSnapshot::default(),
                1,
            );
            let (capturing, captures) = capturing_actions(&admitted);
            let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
            let artifacts = test_artifacts(&admitted);
            let mut driver = StepRuntime::new(
                admitted,
                artifacts.staging.clone(),
                artifacts.inputs.clone(),
                sender,
                TestClock,
            );
            driver.release(captures["task"].clone()).await;
            let (candidate, acknowledgement) = next_acknowledged_occurrence(&mut receiver).await;
            assert_eq!(artifacts.staging.budget_usage(), (0, 0));
            assert_eq!(artifacts.staging.reservation_usage(), (1, 9));

            if cancel_first {
                let cancelled = runtime::reduce::<
                    ProvisionalStepOutputs,
                    StepFailureCause,
                    CapturedValue,
                    TestInstant,
                >(
                    &capturing,
                    Occurrence::CancellationOperationRequested {
                        operation: CancellationOperationId::fixture(1),
                        reason: CancellationReason::UserRequest,
                        deadline: TestInstant(Duration::from_secs(1)),
                    },
                );
                let cancellation = cancelled.actions[0].clone();
                driver.release(cancellation.clone()).await;
                driver.release(cancellation.clone()).await;
                let stale = runtime::reduce(&cancelled.state, candidate);
                acknowledgement.resolve(stale.occurrence_accepted).await;
                assert!(!stale.occurrence_accepted);
                let quiesced = next_occurrence(&mut receiver).await;
                let terminal = runtime::reduce(&cancelled.state, quiesced);
                assert_eq!(
                    terminal.state.workflow,
                    WorkflowState::Cancelled {
                        reason: CancellationReason::UserRequest,
                    }
                );
                assert!(matches!(
                    terminal.state.exports.as_ref().unwrap()["reportExport"],
                    ExportValue::Unavailable { .. }
                ));
                assert_eq!(artifacts.staging.staged_artifact_count(), 0);
                assert_eq!(artifacts.staging.budget_usage(), (0, 0));
                assert_eq!(artifacts.staging.reservation_usage(), (0, 0));
                assert!(receiver.try_recv().is_none());
            } else {
                let committed = runtime::reduce(&capturing, candidate);
                acknowledgement.resolve(committed.occurrence_accepted).await;
                assert!(committed.occurrence_accepted);
                assert_eq!(committed.state.workflow, WorkflowState::Succeeded);
                let late_cancellation = runtime::reduce::<
                    ProvisionalStepOutputs,
                    StepFailureCause,
                    CapturedValue,
                    TestInstant,
                >(
                    &committed.state,
                    Occurrence::CancellationOperationRequested {
                        operation: CancellationOperationId::fixture(1),
                        reason: CancellationReason::UserRequest,
                        deadline: TestInstant(Duration::from_secs(1)),
                    },
                );
                assert!(!late_cancellation.occurrence_accepted);
                assert!(late_cancellation.actions.is_empty());
                assert_eq!(late_cancellation.state, committed.state);
                assert_eq!(artifacts.staging.staged_artifact_count(), 1);
                assert_eq!(artifacts.staging.budget_usage(), (1, 9));
                assert_eq!(artifacts.staging.reservation_usage(), (0, 0));
                let StepState::Succeeded { outputs } = &committed.state.steps["task"].state else {
                    panic!("accepted output candidate did not commit");
                };
                let mut bytes = Vec::new();
                artifacts
                    .staging
                    .copy_to(outputs["report"].as_file().unwrap().handle(), &mut bytes)
                    .unwrap();
                assert_eq!(bytes, b"candidate");
            }
            assert_eq!(driver.active_work_count(), 0);
        }
    })
    .await;
}

fn assert_interrupted(state: &StepState<CapturedValue>, phase: runtime::FailurePhase) {
    let StepState::Failed { detail } = state else {
        panic!("the interrupted step did not fail: {state:?}");
    };
    assert_eq!(
        (detail.phase, detail.code),
        (phase, FailureCode::Interrupted)
    );
}

// REQ-WFEXEC-69
#[tokio::test]
async fn a_command_that_exits_zero_after_an_interruption_fails_interrupted_without_outputs() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        fs::write(execution_root.join("report.bin"), b"candidate").unwrap();
        let source = r#"schemaVersion: 1
steps:
  task:
    kind: cmd
    command:
      argv: ["/bin/sh", "-c", "exit 0"]
    outputs:
      report:
        kind: file
        from: path
        path: report.bin
        mediaType: application/octet-stream
exports:
  reportExport:
    ref: outputs.task.report
"#;
        let admitted = admit_fixture(
            temporary.path(),
            &execution_root,
            source,
            EnvironmentSnapshot::default(),
            1,
        );
        let initialized = runtime::initialize::<
            ProvisionalStepOutputs,
            StepFailureCause,
            CapturedValue,
            TestInstant,
        >(&admitted, None);
        let start = initialized
            .actions
            .iter()
            .find(|requested| matches!(requested.action, Action::StartStep { .. }))
            .unwrap()
            .clone();
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut driver = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            TestClock,
        );
        driver.release(start.clone()).await;
        let started = runtime::reduce(&initialized.state, next_occurrence(&mut receiver).await);
        assert!(started.occurrence_accepted);
        let interrupted = runtime::reduce::<
            ProvisionalStepOutputs,
            StepFailureCause,
            CapturedValue,
            TestInstant,
        >(
            &started.state,
            Occurrence::InterruptionOperationRequested {
                operation: CancellationOperationId::fixture(1),
                reason: CancellationReason::UserRequest,
                deadline: TestInstant(Duration::from_secs(1)),
            },
        );
        let stop = interrupted
            .actions
            .iter()
            .find(|requested| matches!(requested.action, Action::CancelStep { .. }))
            .unwrap()
            .clone();

        // The command exits 0 only after the interruption was accepted.
        let (completed, acknowledgement) = next_acknowledged_occurrence(&mut receiver).await;
        assert!(matches!(
            completed,
            Occurrence::StepExecutionCompleted { .. }
        ));
        let late = runtime::reduce(&interrupted.state, completed);
        assert!(!late.occurrence_accepted);
        acknowledgement.resolve(late.occurrence_accepted).await;
        driver.release(stop).await;
        let terminal = runtime::reduce(&interrupted.state, next_occurrence(&mut receiver).await);

        assert_interrupted(
            &terminal.state.steps["task"].state,
            runtime::FailurePhase::Execution,
        );
        assert!(matches!(
            terminal.state.workflow,
            WorkflowState::Failed {
                later_cancellation: Some(CancellationReason::UserRequest),
                ..
            }
        ));
        assert!(matches!(
            terminal.state.exports.as_ref().unwrap()["reportExport"],
            ExportValue::Unavailable { .. }
        ));
        assert_eq!(artifacts.staging.staged_artifact_count(), 0);
        assert_eq!(driver.active_work_count(), 0);
    })
    .await;
}

// REQ-WFEXEC-69
#[tokio::test]
async fn an_agent_cancelled_under_an_interruption_fails_interrupted() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let staging_root = temporary.path().join("staging");
        fs::create_dir(&execution_root).unwrap();
        fs::create_dir(&staging_root).unwrap();
        let cancellation = CancellationSource::new();
        let admitted = crate::workflow::test_support::AdmittedFixture::new(
            &source_root,
            r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  active:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: text
        from: agent_response
  pending:
    kind: cmd
    dependsOn: [active]
    command:
      argv: ["/bin/true"]
"#,
        )
        .with_files(&[("prompt.md", b"remain active")])
        .admit(
            ResolvedInputs::default(),
            ExecutionContext::new(
                execution_root,
                ExecutionPolicyLimits::new(
                    1,
                    CaptureLimits::new(16, 1024 * 1024, 8 * 1024 * 1024),
                    InputLimits::new(16, 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
                    1024,
                ),
                EnvironmentSnapshot::default(),
                CancellationPolicy::new(cancellation.clone(), Duration::from_secs(1)),
            )
            .with_pi_installation(crate::pi::ValidatedPiInstallation::fixture(
                PathBuf::from("/validated/pi"),
            )),
        );
        let artifacts = test_artifacts(&admitted);
        let attempt_directory = temporary.path().join("run/attempts/000001");
        fs::create_dir_all(&attempt_directory).unwrap();
        let attempt_handle: std::os::fd::OwnedFd =
            fs::File::open(&attempt_directory).unwrap().into();
        let (adapter, mut control) = crate::workflow::agent::scripted::scripted_agent_dispatcher();
        let agents = AgentExecution::enabled(
            WorkflowRunId::from(Arc::from("run-fixed")),
            AgentInputStaging::create(admitted.execution(), &staging_root).unwrap(),
            AgentDiagnosticSessionStore::create(
                &attempt_handle,
                &attempt_directory,
                Arc::from("00000000-0000-4000-8000-000000000001"),
                1,
            )
            .unwrap(),
            adapter,
        );
        let (commits, _committed) = mpsc::unbounded_channel();
        let execution = tokio::spawn(async move {
            execute_workflow_observed(
                admitted,
                &artifacts.staging,
                &artifacts.inputs,
                &StepDiagnosticLog::default(),
                TestClock,
                RecordingCommitPort { commits },
                NoopExecutionObserver,
                agents,
                WorkflowExecutionStart::initial(ProcessGuardRegistry::default()),
            )
            .await
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        assert!(cancellation.request_interruption(CancellationReason::UserRequest));
        let result = execution.await.unwrap().unwrap();

        assert_interrupted(
            &result.state.steps["active"].state,
            runtime::FailurePhase::Execution,
        );
        assert_eq!(
            result.state.steps["pending"].state,
            StepState::Cancelled {
                detail: crate::workflow::evidence::CancellationDetail::new(
                    CancellationReason::UserRequest
                ),
            }
        );
        assert!(matches!(
            result.state.workflow,
            WorkflowState::Failed {
                later_cancellation: Some(CancellationReason::UserRequest),
                ..
            }
        ));
    })
    .await;
}

#[tokio::test]
async fn rejected_candidate_cleanup_failure_quarantines_staging_before_quiescence() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        fs::write(execution_root.join("report.bin"), b"candidate").unwrap();
        let source = r#"schemaVersion: 1
steps:
  task:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      report:
        kind: file
        from: path
        path: report.bin
        mediaType: application/octet-stream
"#;
        let admitted = admit_fixture(
            temporary.path(),
            &execution_root,
            source,
            EnvironmentSnapshot::default(),
            1,
        );
        let (capturing, captures) = capturing_actions(&admitted);
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut driver = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            TestClock,
        );
        driver.release(captures["task"].clone()).await;
        let (candidate, acknowledgement) = next_acknowledged_occurrence(&mut receiver).await;

        let staging_root = fs::read_dir(artifacts._temporary.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| fs::read_dir(path).is_ok_and(|mut entries| entries.next().is_some()))
            .unwrap();
        artifacts.staging.block_artifact_unlinks();
        let cancelled = runtime::reduce::<
            ProvisionalStepOutputs,
            StepFailureCause,
            CapturedValue,
            TestInstant,
        >(
            &capturing,
            Occurrence::CancellationOperationRequested {
                operation: CancellationOperationId::fixture(1),
                reason: CancellationReason::UserRequest,
                deadline: TestInstant(Duration::from_secs(1)),
            },
        );
        let cancellation = cancelled.actions[0].clone();
        driver.release(cancellation.clone()).await;
        let stale = runtime::reduce(&cancelled.state, candidate);
        acknowledgement.resolve(stale.occurrence_accepted).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancellation.id,
            }
        );

        assert_eq!(artifacts.staging.staged_artifact_count(), 1);
        assert_eq!(artifacts.staging.reservation_usage(), (0, 0));
        let retry = artifacts.staging.capture_files(&[CaptureDeclaration::file(
            "retry",
            Path::new("report.bin"),
            "application/octet-stream",
        )]);
        assert_eq!(
            retry.as_ref().err().map(CaptureFailure::kind),
            Some(CaptureFailureKind::StagingUnavailable),
            "capture quiesced without quarantining failed candidate cleanup"
        );
        drop(retry);

        artifacts.staging.release().unwrap();
        assert!(!staging_root.exists());
    })
    .await;
}

// INV-ENGINE-9
#[tokio::test]
async fn workflow_execution_rejects_artifact_staging_bound_to_another_execution() {
    assert_staging_binding_guard(StagingBindingMismatch::Artifact).await;
}

// INV-ENGINE-9
#[tokio::test]
async fn workflow_execution_rejects_input_staging_bound_to_another_execution() {
    assert_staging_binding_guard(StagingBindingMismatch::Input).await;
}

async fn assert_staging_binding_guard(mismatch: StagingBindingMismatch) {
    let temporary = tempfile::tempdir().unwrap();
    let admitted_root = temporary.path().join("admitted-execution");
    let other_root = temporary.path().join("other-execution");
    fs::create_dir(&admitted_root).unwrap();
    fs::create_dir(&other_root).unwrap();
    let command_marker = admitted_root.join("command-started");
    let source = workflow_source(&[(
        "task",
        None,
        &[
            "/bin/sh",
            "-c",
            r#"printf started > "$1""#,
            "staging-binding-guard",
            command_marker.to_str().unwrap(),
        ],
    )]);
    let admitted = admit_fixture(
        temporary.path(),
        &admitted_root,
        &source,
        EnvironmentSnapshot::default(),
        1,
    );
    let other_admitted = admit_fixture(
        temporary.path(),
        &other_root,
        &source,
        EnvironmentSnapshot::default(),
        1,
    );
    let matching = test_artifacts(&admitted);
    let other = test_artifacts(&other_admitted);
    let (artifacts, inputs, expected) = match mismatch {
        StagingBindingMismatch::Artifact => (
            &other.staging,
            &matching.inputs,
            CoordinationError::ArtifactStagingMismatch,
        ),
        StagingBindingMismatch::Input => (
            &matching.staging,
            &other.inputs,
            CoordinationError::InputStagingMismatch,
        ),
    };
    let diagnostics = StepDiagnosticLog::default();
    let (commit_sender, mut commits) = mpsc::unbounded_channel();

    let result = execute_workflow(
        admitted,
        artifacts,
        inputs,
        &diagnostics,
        TestClock,
        RecordingCommitPort {
            commits: commit_sender,
        },
    )
    .await;

    assert_eq!(result, Err(expected));
    assert!(commits.try_recv().is_err());
    assert!(!command_marker.exists());
}

#[tokio::test]
async fn workflow_execution_rejects_input_staging_with_a_different_live_limit() {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&execution_root).unwrap();
    let source = workflow_source(&[("task", None, &["/bin/true"])]);
    let admitted = admit_fixture(
        temporary.path(),
        &execution_root,
        &source,
        EnvironmentSnapshot::default(),
        1,
    );
    let mismatched_input_admission = admit_fixture_with_live_input_limit(
        temporary.path(),
        &execution_root,
        &source,
        EnvironmentSnapshot::default(),
        1,
        64 * 1024 * 1024 + 1,
    );
    let matching_staging = test_artifacts(&admitted);
    let mismatched_staging = test_artifacts(&mismatched_input_admission);
    let diagnostics = StepDiagnosticLog::default();
    let (commit_sender, mut commits) = mpsc::unbounded_channel();

    let result = execute_workflow(
        admitted,
        &matching_staging.staging,
        &mismatched_staging.inputs,
        &diagnostics,
        TestClock,
        RecordingCommitPort {
            commits: commit_sender,
        },
    )
    .await;

    assert_eq!(result, Err(CoordinationError::InputStagingMismatch));
    assert!(commits.try_recv().is_err());
}

// REQ-OUTPUTS-1
#[tokio::test]
async fn workflow_execution_captures_declared_outputs_before_success() {
    with_watchdog(async {
        let PreparedFixtureCommand {
            _temporary,
            cwd,
            listener,
            admitted,
        } = prepare_fixture_command_with_output(ProgramForm::Absolute, 0).await;
        fs::write(cwd.join("report.txt"), b"committed report").unwrap();
        let artifacts = test_artifacts(&admitted);
        let diagnostics = StepDiagnosticLog::default();
        let (commit_sender, _commits) = mpsc::unbounded_channel();
        let execution = execute_workflow(
            admitted,
            &artifacts.staging,
            &artifacts.inputs,
            &diagnostics,
            TestClock,
            RecordingCommitPort {
                commits: commit_sender,
            },
        );
        let command = async {
            let (control, _) = accept_report(&listener).await;
            release(control).await;
        };

        let (result, ()) = tokio::join!(execution, command);
        let result = result.unwrap();

        assert_eq!(result.state.workflow, WorkflowState::Succeeded);
        assert_eq!(result.last_occurrence_ordinal.get(), 4);
        let StepState::Succeeded { outputs } = &result.state.steps["task"].state else {
            panic!("output-producing command did not succeed");
        };
        let captured = outputs["report"].as_file().unwrap();
        assert_eq!(captured.output_identity(), "report");
        assert_eq!(captured.media_type(), "text/plain");
        assert_eq!(captured.size(), 16);
        let mut bytes = Vec::new();
        artifacts
            .staging
            .copy_to(captured.handle(), &mut bytes)
            .unwrap();
        assert_eq!(bytes, b"committed report");
    })
    .await;
}

// REQ-OUTPUTS-9, REQ-EXPORTS-2
// INV-ENGINE-12
#[tokio::test]
async fn multi_file_capture_emits_one_complete_typed_set_and_complete_exports() {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&execution_root).unwrap();
    fs::write(execution_root.join("alpha.txt"), b"alpha").unwrap();
    fs::write(execution_root.join("beta.json"), br#"{"beta":true}"#).unwrap();
    let source = r#"schemaVersion: 1
steps:
  task:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      alpha:
        kind: file
        from: path
        path: alpha.txt
        mediaType: text/plain
      beta:
        kind: file
        from: path
        path: beta.json
        mediaType: application/json
exports:
  alphaExport:
    ref: outputs.task.alpha
  betaExport:
    ref: outputs.task.beta
"#;
    let admitted = admit_fixture(
        temporary.path(),
        &execution_root,
        source,
        EnvironmentSnapshot::default(),
        1,
    );
    let initialized = runtime::initialize::<
        ProvisionalStepOutputs,
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >(&admitted, None);
    let start = initialized.actions[0].id;
    let running =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &initialized.state,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start,
            },
        );
    let capture_requested =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &running.state,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action: start,
                provisional: ProvisionalStepOutputs::command(),
            },
        );
    let capture = capture_requested
        .actions
        .iter()
        .find(|requested| matches!(requested.action, Action::CaptureOutputs { .. }))
        .unwrap();
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
    let artifacts = test_artifacts(&admitted);
    let runtime = StepRuntime::new(
        admitted,
        artifacts.staging.clone(),
        artifacts.inputs.clone(),
        sender,
        TestClock,
    );

    runtime
        .capture_outputs("task".to_owned(), capture.id)
        .await
        .unwrap();

    let (occurrence, acknowledgement) = next_acknowledged_occurrence(&mut receiver).await;
    let Occurrence::OutputsCaptured {
        step,
        action,
        outputs,
    } = &occurrence
    else {
        panic!("complete capture did not produce outputsCaptured");
    };
    assert_eq!(step, "task");
    assert_eq!(*action, capture.id);
    assert_eq!(
        outputs.keys().cloned().collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    assert_eq!(
        outputs["alpha"].as_file().unwrap().media_type(),
        "text/plain"
    );
    assert_eq!(
        outputs["beta"].as_file().unwrap().media_type(),
        "application/json"
    );
    assert_eq!(artifacts.staging.budget_usage(), (0, 0));
    assert_eq!(artifacts.staging.reservation_usage(), (2, 18));
    assert!(receiver.try_recv().is_none());

    let committed = runtime::reduce(&capture_requested.state, occurrence);
    acknowledgement.resolve(committed.occurrence_accepted).await;
    assert_eq!(artifacts.staging.budget_usage(), (2, 18));
    assert_eq!(artifacts.staging.reservation_usage(), (0, 0));
    assert_eq!(committed.state.workflow, WorkflowState::Succeeded);
    let finish = committed
        .actions
        .iter()
        .find(|requested| matches!(requested.action, Action::FinishRun { .. }))
        .unwrap();
    let Action::FinishRun { exports, .. } = &finish.action else {
        panic!("fixture action did not finish the run");
    };
    let ExportValue::Available { output: alpha } = &exports["alphaExport"] else {
        panic!("alpha export was unavailable");
    };
    let ExportValue::Available { output: beta } = &exports["betaExport"] else {
        panic!("beta export was unavailable");
    };
    assert_eq!(alpha.as_file().unwrap().media_type(), "text/plain");
    assert_eq!(beta.as_file().unwrap().media_type(), "application/json");
}

#[tokio::test]
async fn capture_action_release_completes_when_occurrence_channel_is_full() {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&execution_root).unwrap();
    fs::write(execution_root.join("report.txt"), b"report").unwrap();
    let source = r#"schemaVersion: 1
steps:
  task:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      report:
        kind: file
        from: path
        path: report.txt
        mediaType: text/plain
"#;
    let admitted = admit_fixture(
        temporary.path(),
        &execution_root,
        source,
        EnvironmentSnapshot::default(),
        1,
    );
    let initialized = runtime::initialize::<
        ProvisionalStepOutputs,
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >(&admitted, None);
    let start = initialized.actions[0].id;
    let running =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &initialized.state,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start,
            },
        );
    let capture_requested =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &running.state,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action: start,
                provisional: ProvisionalStepOutputs::command(),
            },
        );
    let capture = capture_requested.actions[0].clone();
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    sender
        .send(DriverOccurrence::step_started("queued".to_owned(), start))
        .await
        .unwrap();
    let artifacts = test_artifacts(&admitted);
    let mut driver = StepRuntime::new(
        admitted,
        artifacts.staging,
        artifacts.inputs.clone(),
        sender,
        TestClock,
    );

    with_watchdog(driver.release(capture)).await;

    assert!(matches!(
        receiver.recv().await.unwrap().into_runtime::<TestInstant>(),
        Occurrence::StepStarted { step, .. } if step == "queued"
    ));
    assert!(matches!(
        receiver.recv().await.unwrap().into_runtime::<TestInstant>(),
        Occurrence::OutputsCaptured { step, .. } if step == "task"
    ));
}

// REQ-OUTPUTS-2
// INV-ENGINE-12
#[tokio::test]
async fn failed_set_rolls_back_without_removing_a_prior_steps_export() {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&execution_root).unwrap();
    fs::write(execution_root.join("prior.bin"), b"12").unwrap();
    fs::write(execution_root.join("candidate.bin"), b"34").unwrap();
    let source = r#"schemaVersion: 1
steps:
  prior:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      report:
        kind: file
        from: path
        path: prior.bin
        mediaType: application/prior
  failing:
    kind: cmd
    dependsOn: [prior]
    command:
      argv: ["/bin/true"]
    outputs:
      candidate:
        kind: file
        from: path
        path: candidate.bin
        mediaType: application/candidate
      missing:
        kind: file
        from: path
        path: missing.bin
        mediaType: application/missing
exports:
  priorExport:
    ref: outputs.prior.report
  failedExport:
    ref: outputs.failing.candidate
"#;
    let admitted = admit_fixture_with_capture_limits(
        temporary.path(),
        &execution_root,
        source,
        EnvironmentSnapshot::default(),
        1,
        CaptureLimits::new(3, 4, 6),
    );
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
    let artifacts = test_artifacts(&admitted);
    let runtime = StepRuntime::new(
        admitted.clone(),
        artifacts.staging.clone(),
        artifacts.inputs.clone(),
        sender,
        TestClock,
    );
    let initialized = runtime::initialize::<
        ProvisionalStepOutputs,
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >(&admitted, None);
    let prior_start = initialized.actions[0].id;
    let prior_running =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &initialized.state,
            Occurrence::StepStarted {
                step: "prior".to_owned(),
                action: prior_start,
            },
        );
    let prior_capture_requested =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &prior_running.state,
            Occurrence::StepExecutionCompleted {
                step: "prior".to_owned(),
                action: prior_start,
                provisional: ProvisionalStepOutputs::command(),
            },
        );
    let prior_capture = prior_capture_requested.actions[0].id;
    runtime
        .capture_outputs("prior".to_owned(), prior_capture)
        .await
        .unwrap();
    let (prior_occurrence, prior_acknowledgement) =
        next_acknowledged_occurrence(&mut receiver).await;
    let prior_committed = runtime::reduce(&prior_capture_requested.state, prior_occurrence);
    prior_acknowledgement
        .resolve(prior_committed.occurrence_accepted)
        .await;
    let failing_start = prior_committed
        .actions
        .iter()
        .find(|requested| matches!(&requested.action, Action::StartStep { step, .. } if step == "failing"))
        .unwrap()
        .id;
    let failing_running =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &prior_committed.state,
            Occurrence::StepStarted {
                step: "failing".to_owned(),
                action: failing_start,
            },
        );
    let failing_capture_requested =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &failing_running.state,
            Occurrence::StepExecutionCompleted {
                step: "failing".to_owned(),
                action: failing_start,
                provisional: ProvisionalStepOutputs::command(),
            },
        );
    let failing_capture = failing_capture_requested.actions[0].id;

    runtime
        .capture_outputs("failing".to_owned(), failing_capture)
        .await
        .unwrap();

    let (failure_occurrence, failure_acknowledgement) =
        next_acknowledged_occurrence(&mut receiver).await;
    let Occurrence::OutputCaptureFailed { cause, .. } = &failure_occurrence else {
        panic!("failed set did not report outputCaptureFailed");
    };
    let StepFailureCause::OutputCapture(OutputCaptureFailure::Capture(capture_failure)) = cause
    else {
        panic!("failed set did not retain its typed capture cause");
    };
    assert_eq!(capture_failure.output_identity(), "missing");
    assert_eq!(artifacts.staging.staged_artifact_count(), 1);
    assert_eq!(artifacts.staging.budget_usage(), (1, 2));

    let terminal = runtime::reduce(&failing_capture_requested.state, failure_occurrence);
    failure_acknowledgement
        .resolve(terminal.occurrence_accepted)
        .await;
    assert!(matches!(
        terminal.state.workflow,
        WorkflowState::Failed { .. }
    ));
    let StepState::Succeeded { outputs } = &terminal.state.steps["prior"].state else {
        panic!("prior step lost its committed output");
    };
    let prior = outputs["report"].as_file().unwrap();
    let mut prior_bytes = Vec::new();
    artifacts
        .staging
        .copy_to(prior.handle(), &mut prior_bytes)
        .unwrap();
    assert_eq!(prior_bytes, b"12");
    let Action::FinishRun { exports, .. } = &terminal.actions.last().unwrap().action else {
        panic!("terminal failure did not finish the run");
    };
    assert!(matches!(
        exports["priorExport"],
        ExportValue::Available { .. }
    ));
    assert!(matches!(
        exports["failedExport"],
        ExportValue::Unavailable { .. }
    ));
}

// REQ-WFEXEC-56
// INV-ENGINE-17
#[tokio::test]
async fn outcome_source_outside_declared_outcomes_fails_capture() {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&execution_root).unwrap();
    fs::write(execution_root.join("verdict.txt"), b"shipped").unwrap();
    let source = r#"schemaVersion: 1
outcomes: [planned, blocked, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict.txt
"#;
    let admitted = admit_fixture(
        temporary.path(),
        &execution_root,
        source,
        EnvironmentSnapshot::default(),
        1,
    );
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
    let artifacts = test_artifacts(&admitted);
    let runtime = StepRuntime::new(
        admitted.clone(),
        artifacts.staging.clone(),
        artifacts.inputs.clone(),
        sender,
        TestClock,
    );
    let initialized = runtime::initialize::<
        ProvisionalStepOutputs,
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >(&admitted, None);
    let start = initialized.actions[0].id;
    let running =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &initialized.state,
            Occurrence::StepStarted {
                step: "decide".to_owned(),
                action: start,
            },
        );
    let capture_requested =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &running.state,
            Occurrence::StepExecutionCompleted {
                step: "decide".to_owned(),
                action: start,
                provisional: ProvisionalStepOutputs::command(),
            },
        );
    let capture = capture_requested.actions[0].id;

    runtime
        .capture_outputs("decide".to_owned(), capture)
        .await
        .unwrap();

    let (occurrence, acknowledgement) = next_acknowledged_occurrence(&mut receiver).await;
    let Occurrence::OutputCaptureFailed { step, cause, .. } = &occurrence else {
        panic!("an undeclared outcome did not report outputCaptureFailed");
    };
    assert_eq!(step, "decide");
    assert_eq!(
        cause,
        &StepFailureCause::OutputCapture(OutputCaptureFailure::OutcomeUndeclared {
            output: "verdict".to_owned(),
        })
    );
    let detail = crate::workflow::evidence::failure_detail(
        crate::workflow::evidence::FailurePhase::OutputCapture,
        cause,
    )
    .unwrap();
    assert_eq!(
        (detail.phase, detail.code, detail.output.as_deref()),
        (
            crate::workflow::evidence::FailurePhase::OutputCapture,
            FailureCode::OutcomeUndeclared,
            Some("verdict"),
        )
    );
    let terminal = runtime::reduce(&capture_requested.state, occurrence);
    acknowledgement.resolve(terminal.occurrence_accepted).await;
    assert!(matches!(
        terminal.state.workflow,
        WorkflowState::Failed { .. }
    ));
}

#[derive(Debug, Eq, PartialEq)]
struct CaptureBudgetTranscript {
    winner: String,
    winner_action_sequence: u64,
    winner_output: (String, u64, String, bool),
    loser: String,
    loser_action_sequence: u64,
    loser_failure: CaptureFailureKind,
    budget: (usize, u64),
    reservations: (usize, u64),
    staged_artifacts: usize,
}

async fn run_contended_capture_transcript() -> CaptureBudgetTranscript {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&execution_root).unwrap();
    // Make the later action's source available first; source readiness does not
    // participate in budget allocation.
    fs::write(execution_root.join("beta.bin"), b"bbb").unwrap();
    fs::write(execution_root.join("alpha.bin"), b"aaa").unwrap();
    let source = r#"schemaVersion: 1
steps:
  alpha:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      artifact:
        kind: file
        from: path
        path: alpha.bin
        mediaType: application/alpha
  beta:
    kind: cmd
    command:
      argv: ["/bin/true"]
    outputs:
      artifact:
        kind: file
        from: path
        path: beta.bin
        mediaType: application/beta
"#;
    let admitted = admit_fixture_with_capture_limits(
        temporary.path(),
        &execution_root,
        source,
        EnvironmentSnapshot::default(),
        2,
        CaptureLimits::new(2, 3, 3),
    );
    let initialized = runtime::initialize::<
        ProvisionalStepOutputs,
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >(&admitted, None);
    let starts = initialized
        .actions
        .iter()
        .map(|requested| match &requested.action {
            Action::StartStep { step, .. } => (step.clone(), requested.id),
            _ => panic!("fixture action did not start a step"),
        })
        .collect::<BTreeMap<_, _>>();
    let alpha_running =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &initialized.state,
            Occurrence::StepStarted {
                step: "alpha".to_owned(),
                action: starts["alpha"],
            },
        );
    let both_running =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &alpha_running.state,
            Occurrence::StepStarted {
                step: "beta".to_owned(),
                action: starts["beta"],
            },
        );
    let alpha_capture_requested =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &both_running.state,
            Occurrence::StepExecutionCompleted {
                step: "alpha".to_owned(),
                action: starts["alpha"],
                provisional: ProvisionalStepOutputs::command(),
            },
        );
    let alpha_capture = alpha_capture_requested.actions[0].clone();
    let beta_capture_requested =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &alpha_capture_requested.state,
            Occurrence::StepExecutionCompleted {
                step: "beta".to_owned(),
                action: starts["beta"],
                provisional: ProvisionalStepOutputs::command(),
            },
        );
    let beta_capture = beta_capture_requested.actions[0].clone();
    assert!(alpha_capture.id < beta_capture.id);

    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    sender
        .send(DriverOccurrence::step_started(
            "queued".to_owned(),
            starts["alpha"],
        ))
        .await
        .unwrap();
    let artifacts = test_artifacts(&admitted);
    let mut driver = StepRuntime::new(
        admitted,
        artifacts.staging.clone(),
        artifacts.inputs.clone(),
        sender,
        TestClock,
    );

    // Hold both result deliveries behind a full channel. Action release only queues
    // capture work, while the worker still stages and reserves in action order.
    driver.release(alpha_capture).await;
    driver.release(beta_capture).await;
    assert!(matches!(
        receiver.recv().await.unwrap().into_runtime::<TestInstant>(),
        Occurrence::StepStarted { step, .. } if step == "queued"
    ));

    let (winner, winner_acknowledgement) = next_acknowledged_occurrence(&mut receiver).await;
    assert_eq!(artifacts.staging.budget_usage(), (0, 0));
    let Occurrence::OutputsCaptured {
        step,
        action,
        outputs,
    } = &winner
    else {
        panic!("earlier capture action did not win the budget");
    };
    let output = outputs["artifact"].as_file().unwrap();
    let winner_output = (
        output.output_identity().to_owned(),
        output.size(),
        output.media_type().to_owned(),
        output.handle().opaque_id().starts_with("art_"),
    );
    let winner_step = step.clone();
    let winner_action = *action;
    let winner_reduction = runtime::reduce(&beta_capture_requested.state, winner);
    winner_acknowledgement
        .resolve(winner_reduction.occurrence_accepted)
        .await;

    let (loser, loser_acknowledgement) = next_acknowledged_occurrence(&mut receiver).await;
    let Occurrence::OutputCaptureFailed {
        step: loser_step,
        action: loser_action,
        cause,
    } = &loser
    else {
        panic!("later capture action did not report budget failure");
    };
    let StepFailureCause::OutputCapture(OutputCaptureFailure::Capture(failure)) = cause else {
        panic!("later capture action lost its typed budget failure");
    };
    let loser_step = loser_step.clone();
    let loser_action = *loser_action;
    let loser_failure = failure.kind();
    let loser_reduction = runtime::reduce(&winner_reduction.state, loser);
    loser_acknowledgement
        .resolve(loser_reduction.occurrence_accepted)
        .await;
    CaptureBudgetTranscript {
        winner: winner_step,
        winner_action_sequence: winner_action.transition_sequence.get(),
        winner_output,
        loser: loser_step,
        loser_action_sequence: loser_action.transition_sequence.get(),
        loser_failure,
        budget: artifacts.staging.budget_usage(),
        reservations: artifacts.staging.reservation_usage(),
        staged_artifacts: artifacts.staging.staged_artifact_count(),
    }
}

#[tokio::test]
async fn reducer_action_order_deterministically_allocates_contended_capture_budget() {
    let first = run_contended_capture_transcript().await;
    let second = run_contended_capture_transcript().await;

    assert_eq!(first, second);
    assert_eq!(
        first,
        CaptureBudgetTranscript {
            winner: "alpha".to_owned(),
            winner_action_sequence: 5,
            winner_output: (
                "artifact".to_owned(),
                3,
                "application/alpha".to_owned(),
                true
            ),
            loser: "beta".to_owned(),
            loser_action_sequence: 6,
            loser_failure: CaptureFailureKind::TotalSizeLimitExceeded,
            budget: (1, 3),
            reservations: (0, 0),
            staged_artifacts: 1,
        }
    );
}

// REQ-WFEXEC-20
#[tokio::test]
async fn bare_program_search_skips_a_candidate_inaccessible_to_the_caller() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let blocked_directory = execution_root.join("blocked-bin");
        let usable_directory = execution_root.join("usable-bin");
        fs::create_dir_all(&blocked_directory).unwrap();
        fs::create_dir(&usable_directory).unwrap();

        let current_executable = std::env::current_exe().unwrap();
        let blocked_executable = blocked_directory.join("workflow-command-fixture");
        fs::write(&blocked_executable, b"not executable by the test caller").unwrap();
        fs::set_permissions(&blocked_executable, fs::Permissions::from_mode(0o001)).unwrap();
        let usable_executable = usable_directory.join("workflow-command-fixture");
        install_fixture(&current_executable, &usable_executable);

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let search_path = std::env::join_paths([&blocked_directory, &usable_directory]).unwrap();
        let environment = EnvironmentSnapshot::new([
            (
                OsString::from(FIXTURE_SOCKET),
                OsString::from(&control_address),
            ),
            (OsString::from(FIXTURE_EXIT_CODE), OsString::from("0")),
            (OsString::from("PATH"), search_path),
        ]);
        let arguments = fixture_arguments();
        let argv = std::iter::once("workflow-command-fixture")
            .chain(arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let source = workflow_source(&[("task", None, argv.as_slice())]);
        let admitted = admit_fixture(temporary.path(), &execution_root, &source, environment, 1);
        let action = start_actions(&admitted)["task"];
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
        let artifacts = test_artifacts(&admitted);
        let runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            TestClock,
        );
        let execution =
            tokio::spawn(async move { runtime.execute_step("task".to_owned(), action).await });

        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action,
            }
        );
        let (control, _) = accept_report(&listener).await;
        release(control).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action,
                provisional: ProvisionalStepOutputs::command(),
            }
        );
        assert_eq!(execution.await.unwrap(), Ok(()));
    })
    .await;
}

// REQ-WFEXEC-9, REQ-WFEXEC-20
#[tokio::test]
async fn cwd_and_launch_failures_are_typed_start_occurrences() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let outside = temporary.path().join("outside");
        fs::create_dir(&execution_root).unwrap();
        fs::create_dir(&outside).unwrap();
        symlink(&outside, execution_root.join("escape")).unwrap();

        for (cwd, expected) in [
            (
                "missing",
                StepStartFailure::WorkingDirectory(WorkingDirectoryFailure::Unavailable),
            ),
            (
                "escape",
                StepStartFailure::WorkingDirectory(WorkingDirectoryFailure::EscapesExecutionRoot),
            ),
        ] {
            let source = workflow_source(&[("task", Some(cwd), &["./not-started"])]);
            let admitted = admit_fixture(
                temporary.path(),
                &execution_root,
                &source,
                EnvironmentSnapshot::default(),
                1,
            );
            assert_start_failure(admitted, "task", expected).await;
        }

        let source = workflow_source(&[("task", None, &["./missing-executable"])]);
        let admitted = admit_fixture(
            temporary.path(),
            &execution_root,
            &source,
            EnvironmentSnapshot::default(),
            1,
        );
        assert_start_failure(
            admitted,
            "task",
            StepStartFailure::CommandLaunch(CommandLaunchFailure::NotFound),
        )
        .await;
    })
    .await;
}

#[derive(Clone, Copy)]
enum RebindingCommand {
    Absolute,
    RelativePath,
    AbsolutePath,
}

// REQ-WFEXEC-48
#[tokio::test]
async fn execution_root_rebinding_at_the_prelaunch_boundary_fails_before_spawn() {
    assert_execution_root_rebinding_fails_before_spawn(RebindingCommand::Absolute).await;
}

#[tokio::test]
async fn execution_root_rebinding_during_relative_program_resolution_reports_root_failure() {
    assert_execution_root_rebinding_fails_before_spawn(RebindingCommand::RelativePath).await;
}

#[tokio::test]
async fn execution_root_rebinding_during_absolute_path_resolution_reports_root_failure() {
    assert_execution_root_rebinding_fails_before_spawn(RebindingCommand::AbsolutePath).await;
}

async fn assert_execution_root_rebinding_fails_before_spawn(command: RebindingCommand) {
    with_watchdog(async move {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        let moved_root = temporary.path().join("moved-execution");
        fs::create_dir(&execution_root).unwrap();
        let (argv, environment) = match command {
            RebindingCommand::Absolute => (
                vec![
                    "/bin/sh".to_owned(),
                    "-c".to_owned(),
                    "printf started > command-ran".to_owned(),
                ],
                EnvironmentSnapshot::default(),
            ),
            RebindingCommand::RelativePath | RebindingCommand::AbsolutePath => {
                let executable = execution_root.join("bin/root-bound-command");
                fs::create_dir_all(executable.parent().unwrap()).unwrap();
                fs::write(&executable, "#!/bin/sh\nprintf started > command-ran\n").unwrap();
                fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
                let search_path = if matches!(command, RebindingCommand::RelativePath) {
                    OsString::from("bin")
                } else {
                    executable.parent().unwrap().as_os_str().to_owned()
                };
                (
                    vec!["root-bound-command".to_owned()],
                    EnvironmentSnapshot::new([(OsString::from("PATH"), search_path)]),
                )
            }
        };
        let argv = argv.iter().map(String::as_str).collect::<Vec<_>>();
        let source = workflow_source(&[("task", None, &argv)]);
        let admitted = admit_fixture(temporary.path(), &execution_root, &source, environment, 1);
        let artifacts = test_artifacts(&admitted);
        let boundary = SynchronousGate::new();
        let launch_gate = boundary.clone();
        admitted
            .execution()
            .root_identity()
            .set_prelaunch_boundary(move || launch_gate.block_until_resumed());
        let action = start_actions(&admitted)["task"];
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
        let runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            TestClock,
        );
        let execution =
            tokio::spawn(async move { runtime.execute_step("task".to_owned(), action).await });

        let waiting = boundary.clone();
        tokio::task::spawn_blocking(move || waiting.wait_until_reached())
            .await
            .unwrap();
        fs::rename(&execution_root, &moved_root).unwrap();
        fs::create_dir(&execution_root).unwrap();
        boundary.resume();

        assert_eq!(execution.await.unwrap(), Ok(()));
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStartFailed {
                step: "task".to_owned(),
                action,
                cause: StepFailureCause::Start(StepStartFailure::WorkingDirectory(
                    WorkingDirectoryFailure::ExecutionRootRebound,
                )),
            }
        );
        assert!(!moved_root.join("command-ran").exists());
        assert!(!execution_root.join("command-ran").exists());
    })
    .await;
}

#[tokio::test]
async fn panicked_spawned_step_task_fails_the_named_step() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        let source = workflow_source(&[("task", None, &["/bin/true"])]);
        let admitted = admit_fixture(
            temporary.path(),
            &execution_root,
            &source,
            EnvironmentSnapshot::default(),
            1,
        );
        let artifacts = test_artifacts(&admitted);
        let (occurrences, receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
        let runtime = StepRuntime::new(
            admitted.clone(),
            artifacts.staging,
            artifacts.inputs,
            occurrences,
            TestClock,
        );
        runtime.inject_step_task_panic("task");
        let (commit_sender, _commits) = mpsc::unbounded_channel();
        let result = Coordinator::new(
            admitted,
            receiver,
            TestClock,
            RecordingCommitPort {
                commits: commit_sender,
            },
            runtime.clone(),
        )
        .run()
        .await
        .unwrap();
        runtime.shutdown().await;

        let WorkflowState::Failed { primary_issue, .. } = result.state.workflow else {
            panic!("panicked task did not fail the workflow");
        };
        assert_eq!(primary_issue.node.id, "task");
        let StepState::Failed { detail } = &result.state.steps["task"].state else {
            panic!("panicked task did not retain step failure evidence");
        };
        assert_eq!(detail.code, FailureCode::PreparationTaskUnavailable);
    })
    .await;
}

// REQ-WFEXEC-21
#[tokio::test]
async fn nonzero_exit_is_a_typed_execution_occurrence_with_the_start_action() {
    with_watchdog(async {
        let run = run_fixture_command(ProgramForm::Absolute, 23).await;
        assert_eq!(
            run.terminal,
            Occurrence::StepExecutionFailed {
                step: "task".to_owned(),
                action: run.action,
                cause: StepFailureCause::Execution(StepExecutionFailure::Command(
                    CommandExecutionFailure::UnsuccessfulExit { code: Some(23) },
                )),
            }
        );
        assert!(run.diagnostic.standard_output().fully_drained());
        assert!(run.diagnostic.standard_error().fully_drained());
    })
    .await;
}

#[tokio::test]
async fn input_views_cleanup_after_launch_and_execution_failures() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let executable = std::env::current_exe().unwrap();
        let arguments = fixture_arguments();
        let argv = std::iter::once(executable.to_str().unwrap())
            .chain(arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let source = input_command_source("task", argv.as_slice());
        let admitted = admit_fixture_with_inputs(
            temporary.path(),
            &execution_root,
            &source,
            fixture_environment(&control_address, 23, &execution_root, None),
            FixtureExecution {
                limits: ExecutionPolicyLimits::new(
                    1,
                    CaptureLimits::new(8, 1024, 4096),
                    InputLimits::new(4, 1024, 4096, 4096),
                    1024,
                ),
                imports: text_inputs("failure prompt"),
            },
        );
        let artifacts = test_artifacts(&admitted);
        let diagnostics = StepDiagnosticLog::default();
        let (commits, _) = mpsc::unbounded_channel();
        let execution = execute_workflow(
            admitted,
            &artifacts.staging,
            &artifacts.inputs,
            &diagnostics,
            TestClock,
            RecordingCommitPort { commits },
        );
        let command = async {
            let (control, report) = accept_report(&listener).await;
            let path = PathBuf::from(&report.environment["UM_STEP_INPUTS"]);
            release(control).await;
            path
        };
        let (result, execution_failure_path) = tokio::join!(execution, command);
        assert!(matches!(
            result.unwrap().state.workflow,
            WorkflowState::Failed { .. }
        ));
        assert!(!execution_failure_path.exists());
        assert_eq!(artifacts.inputs.reservation_usage(), (0, 0, 0));

        let launch_source = input_command_source("launch", &["./missing-executable"]);
        let launch_admitted = admit_fixture_with_inputs(
            temporary.path(),
            &execution_root,
            &launch_source,
            EnvironmentSnapshot::new([("UM_STEP_INPUTS", "caller-value")]),
            FixtureExecution {
                limits: ExecutionPolicyLimits::new(
                    1,
                    CaptureLimits::new(8, 1024, 4096),
                    InputLimits::new(4, 1024, 4096, 4096),
                    1024,
                ),
                imports: text_inputs("launch prompt"),
            },
        );
        let launch_artifacts = test_artifacts(&launch_admitted);
        let (commits, _) = mpsc::unbounded_channel();
        let result = execute_workflow(
            launch_admitted,
            &launch_artifacts.staging,
            &launch_artifacts.inputs,
            &StepDiagnosticLog::default(),
            TestClock,
            RecordingCommitPort { commits },
        )
        .await
        .unwrap();
        let WorkflowState::Failed { primary_issue, .. } = result.state.workflow else {
            panic!("launch failure did not fail the workflow");
        };
        assert!(matches!(
            primary_issue.detail,
            super::super::evidence::PrimaryIssueDetail::Failed(ref detail)
                if detail.code == super::super::evidence::FailureCode::CommandLaunchNotFound
        ));
        assert_eq!(launch_artifacts.inputs.reservation_usage(), (0, 0, 0));
    })
    .await;
}

#[tokio::test]
async fn input_staging_cleanup_is_deferred_to_caller_and_retryable() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let executable = std::env::current_exe().unwrap();
        let arguments = fixture_arguments();
        let argv = std::iter::once(executable.to_str().unwrap())
            .chain(arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let source = input_command_source("task", argv.as_slice());
        let admitted = admit_fixture_with_inputs(
            temporary.path(),
            &execution_root,
            &source,
            fixture_environment(&control_address, 0, &execution_root, None),
            FixtureExecution {
                limits: ExecutionPolicyLimits::new(
                    1,
                    CaptureLimits::new(8, 1024, 4096),
                    InputLimits::new(4, 1024, 4096, 4096),
                    1024,
                ),
                imports: text_inputs("retry prompt"),
            },
        );
        let artifacts = test_artifacts(&admitted);
        artifacts.inputs.block_cleanup();
        let diagnostics = StepDiagnosticLog::default();
        let (commits, _) = mpsc::unbounded_channel();
        let execution = execute_workflow(
            admitted,
            &artifacts.staging,
            &artifacts.inputs,
            &diagnostics,
            TestClock,
            RecordingCommitPort { commits },
        );
        let command = async {
            let (control, report) = accept_report(&listener).await;
            let path = PathBuf::from(&report.environment["UM_STEP_INPUTS"]);
            release(control).await;
            path
        };

        let (result, input_path) = tokio::join!(execution, command);

        assert!(result.is_ok());
        assert!(input_path.exists());
        assert_eq!(artifacts.inputs.reservation_usage(), (1, 1, 12));
        assert!(artifacts.inputs.release().is_err());

        artifacts.inputs.unblock_cleanup();
        artifacts.inputs.release().unwrap();

        assert!(!input_path.exists());
        assert_eq!(artifacts.inputs.reservation_usage(), (0, 0, 0));
        artifacts.inputs.release().unwrap();
    })
    .await;
}

#[tokio::test]
async fn unavailable_captured_input_fails_preparation_without_launching_command() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        fs::write(execution_root.join("report.bin"), b"foreign artifact").unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let executable = std::env::current_exe().unwrap();
        let arguments = fixture_arguments();
        let argv = std::iter::once(executable.to_str().unwrap())
            .chain(arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let source = format!(
            "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"unused\"]\n    outputs:\n      report:\n        kind: file\n        from: path\n        path: report.bin\n        mediaType: application/fixture\n  consume:\n    kind: cmd\n    dependsOn: [produce]\n    inputs:\n      artifact:\n        ref: outputs.produce.report\n    command:\n      argv: {}\n",
            serde_json::to_string(&argv).unwrap()
        );
        let admitted = admit_fixture(
            temporary.path(),
            &execution_root,
            &source,
            fixture_environment(&control_address, 0, &execution_root, None),
            1,
        );
        let artifacts = test_artifacts(&admitted);
        let foreign_parent = tempfile::tempdir().unwrap();
        let foreign_store =
            ArtifactStaging::create(admitted.execution(), foreign_parent.path()).unwrap();
        let mut foreign_outputs = foreign_store
            .capture_files(&[crate::workflow::artifact::CaptureDeclaration::file(
                "report",
                Path::new("report.bin"),
                "application/fixture",
            )])
            .unwrap();
        let foreign_value = CapturedValue::file(foreign_outputs.remove("report").unwrap());

        let initialized =
            runtime::initialize::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(&admitted, None);
        let producer_start = initialized.actions[0].id;
        let running = runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &initialized.state,
            Occurrence::StepStarted {
                step: "produce".to_owned(),
                action: producer_start,
            },
        );
        let capture_requested = runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &running.state,
            Occurrence::StepExecutionCompleted {
                step: "produce".to_owned(),
                action: producer_start,
                provisional: ProvisionalStepOutputs::command(),
            },
        );
        let capture_action = capture_requested.actions[0].id;
        let consumer_requested = runtime::reduce(
            &capture_requested.state,
            Occurrence::OutputsCaptured {
                step: "produce".to_owned(),
                action: capture_action,
                outputs: BTreeMap::from([("report".to_owned(), foreign_value)]),
            },
        );
        let consumer = consumer_requested
            .actions
            .into_iter()
            .find(|requested| {
                matches!(&requested.action, Action::StartStep { step, .. } if step == "consume")
            })
            .unwrap();
        let consumer_action = consumer.id;
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
        let mut driver = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            TestClock,
        );

        driver.release(consumer).await;

        let Occurrence::StepStartFailed { action, cause, .. } =
            next_occurrence(&mut receiver).await
        else {
            panic!("unavailable input did not fail command preparation");
        };
        assert_eq!(action, consumer_action);
        let StepFailureCause::Start(StepStartFailure::InputPreparation(failure)) = cause else {
            panic!("unavailable input lost its typed preparation cause");
        };
        assert_eq!(failure.input_identity(), Some("artifact"));
        assert_eq!(
            failure.kind(),
            crate::workflow::input::InputPreparationFailureKind::SourceUnavailable
        );
        assert_eq!(artifacts.inputs.reservation_usage(), (0, 0, 0));
        let listener = listener.into_std().unwrap();
        assert!(matches!(
            listener.accept(),
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock
        ));
    })
    .await;
}

#[tokio::test]
async fn agent_process_control_escalates_at_the_cancel_action_deadline() {
    with_watchdog(async {
        let PreparedFixtureCommand {
            _temporary,
            admitted,
            ..
        } = prepare_fixture_command(ProgramForm::Absolute, 0).await;
        let deadline = TestInstant(Duration::from_secs(41));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let Action::StartStep { step, .. } = start.action else {
            panic!("fixture did not produce a start action");
        };
        let (clock, mut deadline_control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, _receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime =
            StepRuntime::new(admitted, artifacts.staging, artifacts.inputs, sender, clock);
        let _cancellation = runtime.register_start(step, start.id).unwrap();
        assert!(matches!(
            runtime.with_work(|work| work.begin_launch(start.id)),
            BeginLaunch::Launch
        ));
        let (process_control, mut directives) = agent_process_control_channel();
        assert!(matches!(
            runtime.with_work(|work| work.record_agent_launch(start.id, process_control)),
            RecordLaunch::Running
        ));

        runtime.release(cancel).await;
        assert_eq!(
            directives.recv().await,
            Some(AgentProcessDirective::Interrupt)
        );
        assert_eq!(deadline_control.next_deadline().await, deadline);
        deadline_control.release();
        assert_eq!(directives.recv().await, Some(AgentProcessDirective::Force));
        runtime.with_work(|work| work.abandon(start.id));
    })
    .await;
}

#[tokio::test]
async fn prelaunch_cancellation_releases_inputs_before_reporting_quiescence() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        let source = input_command_source("task", &["/bin/true"]);
        let admitted = admit_fixture_with_inputs(
            temporary.path(),
            &execution_root,
            &source,
            EnvironmentSnapshot::default(),
            FixtureExecution {
                limits: ExecutionPolicyLimits::new(
                    1,
                    CaptureLimits::new(8, 1024, 4096),
                    InputLimits::new(4, 1024, 4096, 4096),
                    1024,
                ),
                imports: text_inputs("cancel before launch"),
            },
        );
        let deadline = TestInstant(Duration::from_secs(41));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let Action::StartStep { step, inputs, .. } = start.action else {
            panic!("fixture did not produce a start action");
        };
        let Action::CancelStep {
            step: cancel_step,
            deadline,
            ..
        } = cancel.action
        else {
            panic!("fixture did not produce a cancellation action");
        };
        let (sender, _receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
        sender
            .send(DriverOccurrence::step_started(
                "channel-occupant".to_owned(),
                start.id,
            ))
            .await
            .unwrap();
        let artifacts = test_artifacts(&admitted);
        let runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            TestClock,
        );
        let cancellation = runtime.register_start(step.clone(), start.id).unwrap();
        let CancellationRegistration::Active {
            wake,
            interrupt,
            agent_deadline,
        } = runtime.request_cancellation(
            cancel_step,
            cancel.id,
            CancellationReason::UserRequest,
            deadline,
        )
        else {
            panic!("prelaunch cancellation was not registered");
        };
        assert!(interrupt.is_none());
        assert!(agent_deadline.is_none());
        wake.unwrap().send(()).unwrap();

        let execution = runtime.execute_registered_step(step, start.id, inputs, cancellation);
        tokio::pin!(execution);
        std::future::poll_fn(|context| match execution.as_mut().poll(context) {
            Poll::Ready(result) => panic!("quiescence bypassed channel backpressure: {result:?}"),
            Poll::Pending if runtime.active_work_count() == 0 => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        })
        .await;

        assert_eq!(artifacts.inputs.reservation_usage(), (0, 0, 0));
    })
    .await;
}

#[tokio::test]
async fn input_view_cleanup_precedes_controlled_cancellation_quiescence() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let executable = std::env::current_exe().unwrap();
        let arguments = fixture_arguments();
        let argv = std::iter::once(executable.to_str().unwrap())
            .chain(arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let source = input_command_source("task", argv.as_slice());
        let environment = EnvironmentSnapshot::new([
            (
                OsString::from(FIXTURE_SOCKET),
                OsString::from(control_address),
            ),
            (OsString::from(FIXTURE_EXIT_CODE), OsString::from("0")),
            (
                OsString::from(FIXTURE_MODE),
                OsString::from(FIXTURE_MODE_INTERRUPTIBLE),
            ),
            (OsString::from(FIXTURE_ROLE), OsString::from(FIXTURE_PARENT)),
            (
                OsString::from("UM_INHERITED"),
                OsString::from("must-not-reach-command"),
            ),
        ]);
        let admitted = admit_fixture_with_inputs(
            temporary.path(),
            &execution_root,
            &source,
            environment,
            FixtureExecution {
                limits: ExecutionPolicyLimits::new(
                    1,
                    CaptureLimits::new(8, 1024, 4096),
                    InputLimits::new(4, 1024, 4096, 4096),
                    1024,
                ),
                imports: text_inputs("cancel prompt"),
            },
        );
        let deadline = TestInstant(Duration::from_secs(41));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let start_action = start.id;
        let cancel_action = cancel.id;
        let (clock, mut deadline_control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            clock,
        );

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;
        let parent_path = PathBuf::from(&processes[FIXTURE_PARENT].1.environment["UM_STEP_INPUTS"]);
        let descendant_path =
            PathBuf::from(&processes[FIXTURE_DESCENDANT].1.environment["UM_STEP_INPUTS"]);
        assert_eq!(parent_path, descendant_path);
        assert_eq!(artifacts.inputs.reservation_usage(), (1, 1, 13));

        runtime.release(cancel).await;
        assert_group_interrupted(&processes).await;
        assert_eq!(deadline_control.next_deadline().await, deadline);
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );
        assert!(!parent_path.exists());
        assert_eq!(artifacts.inputs.reservation_usage(), (0, 0, 0));
        assert_group_closed(&mut processes).await;
    })
    .await;
}

#[tokio::test]
async fn output_beyond_each_log_limit_is_drained_before_success() {
    with_watchdog(async {
        let PreparedFixtureCommand {
            _temporary,
            listener,
            admitted,
            ..
        } = prepare_fixture_command_with_log_output(256 * 1024, 37).await;
        let action = start_actions(&admitted)["task"];
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
        let artifacts = test_artifacts(&admitted);
        let diagnostics = StepDiagnosticLog::default();
        let runtime = StepRuntime::with_diagnostics(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            diagnostics.clone(),
            sender,
            TestClock,
        );
        let execution =
            tokio::spawn(async move { runtime.execute_step("task".to_owned(), action).await });

        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action,
            }
        );
        let (control, _) = accept_report(&listener).await;
        assert_eq!(next_fixture_event(&control).await.event, "output-written");
        release(control).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action,
                provisional: ProvisionalStepOutputs::command(),
            }
        );
        assert_eq!(execution.await.unwrap(), Ok(()));

        let diagnostic = diagnostics.get("task").unwrap();
        for stream in [diagnostic.standard_output(), diagnostic.standard_error()] {
            assert_eq!(stream.bytes().len(), 37);
            assert!(stream.truncation().is_some());
            assert!(stream.fully_drained());
        }
    })
    .await;
}

#[tokio::test]
async fn cancellation_during_diagnostic_join_waits_for_readers() {
    with_watchdog(async {
        let PreparedFixtureCommand {
            _temporary,
            admitted,
            ..
        } = prepare_fixture_command(ProgramForm::Absolute, 0).await;
        let deadline = TestInstant(Duration::from_secs(17));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
        let artifacts = test_artifacts(&admitted);
        let diagnostics = StepDiagnosticLog::default();
        let mut runtime = StepRuntime::with_diagnostics(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            diagnostics.clone(),
            sender,
            TestClock,
        );
        let cancellation = runtime.register_start("task".to_owned(), start.id).unwrap();
        drop(cancellation);

        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "__diagnostic_join_child__"])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.spawn().unwrap();
        let status = child.wait().await.unwrap();

        let (standard_output, standard_output_writer) = tokio::io::duplex(1);
        let (standard_error, standard_error_writer) = tokio::io::duplex(1);
        let pending = diagnostics.start_capture::<TestInstant, _, _, _>(
            "task".to_owned(),
            start.id,
            NonZeroU64::new(1).unwrap(),
            standard_output,
            standard_error,
            crate::workflow::observation::NoopExecutionObserver,
        );
        let mut launched = LaunchedStepBody::fixture(child, pending);
        let settlement_runtime = runtime.clone();
        let settlement = settlement_runtime.settle_launched(
            "task".to_owned(),
            start.id,
            &mut launched,
            Some(Ok(status)),
        );
        tokio::pin!(settlement);
        std::future::poll_fn(|context| {
            assert!(settlement.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;

        let occurrence = receiver.recv();
        tokio::pin!(occurrence);
        std::future::poll_fn(|context| {
            assert!(occurrence.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;

        runtime.release(cancel).await;
        std::future::poll_fn(|context| {
            assert!(settlement.as_mut().poll(context).is_pending());
            assert!(occurrence.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(
            diagnostics.get("task").is_none(),
            "diagnostics completed while both writers remained open"
        );

        drop(standard_output_writer);
        drop(standard_error_writer);
        assert_eq!(settlement.await, Ok(()));
        assert_eq!(
            occurrence.await.unwrap().into_runtime::<()>(),
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );
        let diagnostic = diagnostics.get("task").unwrap();
        assert!(diagnostic.standard_output().fully_drained());
        assert!(diagnostic.standard_error().fully_drained());
    })
    .await;
}

// REQ-WFEXEC-19
#[tokio::test]
async fn concurrent_commands_receive_distinct_process_groups() {
    with_watchdog(async {
        let temporary = tempfile::tempdir().unwrap();
        let execution_root = temporary.path().join("execution");
        fs::create_dir(&execution_root).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let control_address = listener.local_addr().unwrap().to_string();
        let executable = std::env::current_exe().unwrap();
        let executable = executable.to_str().unwrap();
        let fixture_arguments = fixture_arguments();
        let argv = std::iter::once(executable)
            .chain(fixture_arguments.iter().map(String::as_str))
            .collect::<Vec<_>>();
        let source = workflow_source(&[
            ("alpha", None, argv.as_slice()),
            ("beta", None, argv.as_slice()),
        ]);
        let environment = fixture_environment(&control_address, 0, execution_root.as_path(), None);
        let admitted = admit_fixture(temporary.path(), &execution_root, &source, environment, 2);
        let actions = start_actions(&admitted);
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            TestClock,
        );
        let alpha_runtime = runtime.clone();
        let alpha_action = actions["alpha"];
        let alpha = tokio::spawn(async move {
            alpha_runtime
                .execute_step("alpha".to_owned(), alpha_action)
                .await
        });
        let beta_action = actions["beta"];
        let beta =
            tokio::spawn(async move { runtime.execute_step("beta".to_owned(), beta_action).await });

        let mut started = BTreeMap::new();
        for _ in 0..2 {
            let Occurrence::StepStarted { step, action } = next_occurrence(&mut receiver).await
            else {
                panic!("command did not report its successful launch");
            };
            started.insert(step, action);
        }
        assert_eq!(started, actions);

        let (first_control, first_report) = accept_report(&listener).await;
        let (second_control, second_report) = accept_report(&listener).await;
        assert!(first_report.process_group_leader);
        assert!(second_report.process_group_leader);
        assert_ne!(first_report.process_group, second_report.process_group);
        release(first_control).await;
        release(second_control).await;

        let mut completed = BTreeMap::new();
        for _ in 0..2 {
            let Occurrence::StepExecutionCompleted {
                step,
                action,
                provisional,
            } = next_occurrence(&mut receiver).await
            else {
                panic!("command did not report zero-exit completion");
            };
            assert_eq!(provisional, ProvisionalStepOutputs::command());
            completed.insert(step, action);
        }
        assert_eq!(completed, actions);
        let (alpha_result, beta_result) = tokio::join!(alpha, beta);
        assert_eq!(alpha_result.unwrap(), Ok(()));
        assert_eq!(beta_result.unwrap(), Ok(()));
    })
    .await;
}

#[tokio::test]
async fn cancellation_before_launch_revokes_the_action_and_duplicate_delivery_is_inert() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_INTERRUPTIBLE).await;
        let deadline = TestInstant(Duration::from_secs(19));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let (clock, control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            clock,
        );

        runtime.release(start.clone()).await;
        runtime.release(cancel.clone()).await;
        runtime.release(cancel).await;
        runtime.release(start).await;

        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(receiver.try_recv().is_none());
        let listener = listener.into_std().unwrap();
        assert!(
            matches!(listener.accept(), Err(failure) if failure.kind() == io::ErrorKind::WouldBlock)
        );
    })
    .await;
}

#[tokio::test]
async fn cancellation_before_launch_returns_interrupts_the_released_group_and_reaps_once() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_INTERRUPTIBLE).await;
        let deadline = TestInstant(Duration::from_secs(23));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let start_action = start.id;
        let (clock, mut control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let diagnostics = StepDiagnosticLog::default();
        let (store, mut guard_control) =
            BlockingGuardStore::pausing_at(GuardStoreOperation::Release);
        let mut runtime = StepRuntime::with_observer(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            diagnostics.clone(),
            sender,
            clock,
            NoopExecutionObserver,
            AgentExecution::disabled(),
            ProcessGuardRegistry::durable(Arc::new(store)),
        );

        runtime.release(start).await;
        guard_control.wait_until_register_started().await;
        let mut processes = accept_group(&listener).await;

        // User code is ready, but the launch worker cannot return until the
        // durable release write completes. Cancellation must still be graceful.
        runtime.release(cancel.clone()).await;
        runtime.release(cancel).await;
        assert!(runtime.cancellation_for(start_action).is_some());
        guard_control.release_register();
        assert_group_interrupted(&processes).await;
        assert_eq!(control.next_deadline().await, deadline);
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );

        assert_group_closed(&mut processes).await;
        let diagnostic = diagnostics.get("task").unwrap();
        assert!(diagnostic.standard_output().fully_drained());
        assert!(diagnostic.standard_error().fully_drained());
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

#[tokio::test]
async fn sixty_second_local_grace_allows_clean_exit_after_thirty_seconds() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_STUBBORN).await;
        let deadline = TestInstant(Duration::from_secs(60));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let start_action = start.id;
        let (clock, mut control) = AdvancingClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            clock,
        );

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;

        runtime.release(cancel).await;
        assert_group_interrupted(&processes).await;
        assert_eq!(control.next_deadline().await, deadline);
        control.advance_to(TestInstant(Duration::from_secs(30)));
        assert_eq!(control.active_waiters(), 1);
        release_group(&processes).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );

        assert_group_closed(&mut processes).await;
        assert_eq!(control.active_waiters(), 0);
        assert_eq!(runtime.active_work_count(), 0);
    })
    .await;
}

// REQ-WFEXEC-47
#[tokio::test]
async fn admitted_deadline_forces_the_stubborn_process_group_without_wall_clock_waiting() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_STUBBORN).await;
        let deadline = TestInstant(Duration::from_secs(29));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let start_action = start.id;
        let (clock, mut control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            clock,
        );

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;

        runtime.release(cancel).await;
        assert_group_interrupted(&processes).await;
        assert_eq!(control.next_deadline().await, deadline);
        assert_eq!(control.active_waiters(), 1);
        control.release();
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );

        assert_group_closed(&mut processes).await;
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

#[tokio::test]
async fn force_abort_of_gracefully_cancelling_command_reports_the_force_action() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_STUBBORN).await;
        let graceful_deadline = TestInstant(Duration::from_secs(29));
        let (start, cancel) = running_cancellation_actions(&admitted, graceful_deadline);
        let start_action = start.id;
        let force_abort = RequestedAction {
            id: ActionId {
                transition_sequence: TransitionSequence((1_u64 << 63) | 1),
            },
            action: Action::ForceAbortStep {
                step: "task".to_owned(),
                active: ActiveStepInvocation::Target {
                    execution_number: TargetExecutionNumber::FIRST,
                },
                reason: CancellationReason::UserRequest,
                deadline: TestInstant(Duration::ZERO),
            },
        };
        let force_action = force_abort.id;
        let (clock, mut control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            clock,
        );

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;

        runtime.release(cancel).await;
        assert_group_interrupted(&processes).await;
        assert_eq!(control.next_deadline().await, graceful_deadline);
        runtime.release(force_abort.clone()).await;
        runtime.release(force_abort).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: force_action,
            }
        );

        assert_group_closed(&mut processes).await;
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

#[tokio::test]
async fn natural_exit_before_cancel_delivery_has_no_late_lifecycle_or_cleanup_work() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_INTERRUPTIBLE).await;
        let deadline = TestInstant(Duration::from_secs(31));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let start_action = start.id;
        let (clock, mut control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            clock,
        );

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;
        release_group(&processes).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action: start_action,
                provisional: ProvisionalStepOutputs::command(),
            }
        );

        runtime.release(cancel.clone()).await;
        runtime.release(cancel).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );

        assert_group_closed(&mut processes).await;
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(control.registrations.try_recv().is_err());
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

// REQ-WFEXEC-47
#[tokio::test]
async fn cancellation_after_the_owned_child_exits_still_terminates_its_descendant() {
    with_watchdog(async {
        let PreparedGroupCommand {
            _temporary,
            listener,
            admitted,
        } = prepare_group_command(FIXTURE_MODE_PARENT_EXITS).await;
        let deadline = TestInstant(Duration::from_secs(37));
        let (start, cancel) = running_cancellation_actions(&admitted, deadline);
        let cancel_action = cancel.id;
        let start_action = start.id;
        let (clock, control) = ControlledClock::new(TestInstant(Duration::ZERO));
        let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(4).unwrap());
        let artifacts = test_artifacts(&admitted);
        let mut runtime = StepRuntime::new(
            admitted,
            artifacts.staging.clone(),
            artifacts.inputs.clone(),
            sender,
            clock,
        );

        runtime.release(start).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start_action,
            }
        );
        let mut processes = accept_group(&listener).await;
        release_control(&processes[FIXTURE_PARENT].0).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepExecutionCompleted {
                step: "task".to_owned(),
                action: start_action,
                provisional: ProvisionalStepOutputs::command(),
            }
        );

        runtime.release(cancel).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "task".to_owned(),
                action: cancel_action,
            }
        );

        let descendant_was_alive = probe_fixture_alive(&processes[FIXTURE_DESCENDANT].0).await;
        if descendant_was_alive {
            release_control(&processes[FIXTURE_DESCENDANT].0).await;
        }
        assert_group_closed(&mut processes).await;

        assert!(
            !descendant_was_alive,
            "step quiesced while the command descendant could still perform work"
        );
        assert_eq!(runtime.active_work_count(), 0);
        assert_eq!(control.active_waiters(), 0);
        assert!(receiver.try_recv().is_none());
    })
    .await;
}

async fn run_fixture_command(form: ProgramForm, exit_code: i32) -> FixtureRun {
    let PreparedFixtureCommand {
        _temporary,
        cwd,
        listener,
        admitted,
    } = prepare_fixture_command(form, exit_code).await;
    let action = start_actions(&admitted)["task"];
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(2).unwrap());
    let artifacts = test_artifacts(&admitted);
    let diagnostics = StepDiagnosticLog::default();
    let runtime = StepRuntime::with_diagnostics(
        admitted,
        artifacts.staging.clone(),
        artifacts.inputs.clone(),
        diagnostics.clone(),
        sender,
        TestClock,
    );
    let execution =
        tokio::spawn(async move { runtime.execute_step("task".to_owned(), action).await });

    assert_eq!(
        next_occurrence(&mut receiver).await,
        Occurrence::StepStarted {
            step: "task".to_owned(),
            action,
        }
    );
    let (control, report) = accept_report(&listener).await;
    assert_eq!(report.current_directory, fs::canonicalize(cwd).unwrap());
    release(control).await;
    let terminal = next_occurrence(&mut receiver).await;
    assert_eq!(execution.await.unwrap(), Ok(()));
    FixtureRun {
        report,
        action,
        terminal,
        diagnostic: diagnostics.get("task").unwrap(),
    }
}

async fn prepare_fixture_command(form: ProgramForm, exit_code: i32) -> PreparedFixtureCommand {
    prepare_fixture_command_with_options(form, exit_code, false, None, 1024 * 1024).await
}

async fn prepare_fixture_command_with_output(
    form: ProgramForm,
    exit_code: i32,
) -> PreparedFixtureCommand {
    prepare_fixture_command_with_options(form, exit_code, true, None, 1024 * 1024).await
}

async fn prepare_fixture_command_with_log_output(
    output_bytes: usize,
    maximum_log_bytes: u64,
) -> PreparedFixtureCommand {
    prepare_fixture_command_with_options(
        ProgramForm::Absolute,
        0,
        false,
        Some(output_bytes),
        maximum_log_bytes,
    )
    .await
}

async fn prepare_fixture_command_with_options(
    form: ProgramForm,
    exit_code: i32,
    declare_output: bool,
    output_bytes: Option<usize>,
    maximum_log_bytes: u64,
) -> PreparedFixtureCommand {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    let cwd = execution_root.join("work");
    fs::create_dir_all(&cwd).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let control_address = listener.local_addr().unwrap().to_string();
    let current_executable = std::env::current_exe().unwrap();
    let (program, path_directory) = match form {
        ProgramForm::Bare => {
            let path_directory = execution_root.join("bin");
            fs::create_dir(&path_directory).unwrap();
            let installed = path_directory.join("workflow-command-fixture");
            install_fixture(&current_executable, &installed);
            ("workflow-command-fixture".to_owned(), path_directory)
        }
        ProgramForm::Relative => {
            let installed = cwd.join("workflow-command-fixture");
            install_fixture(&current_executable, &installed);
            (
                "./workflow-command-fixture".to_owned(),
                execution_root.clone(),
            )
        }
        ProgramForm::Absolute => (
            current_executable.to_str().unwrap().to_owned(),
            execution_root.clone(),
        ),
    };
    let arguments = fixture_arguments();
    let mut argv = vec![program];
    argv.extend(arguments);
    let argv_references = argv.iter().map(String::as_str).collect::<Vec<_>>();
    let mut source = workflow_source(&[("task", Some("work"), &argv_references)]);
    if declare_output {
        source.push_str(
            "    outputs:\n      report:\n        kind: file\n        from: path\n        path: work/report.txt\n        mediaType: text/plain\n",
        );
    }
    let environment =
        fixture_environment(&control_address, exit_code, &path_directory, output_bytes);
    let admitted = admit_fixture_with_log_limit(
        temporary.path(),
        &execution_root,
        &source,
        environment,
        1,
        maximum_log_bytes,
    );
    PreparedFixtureCommand {
        _temporary: temporary,
        cwd,
        listener,
        admitted,
    }
}

async fn prepare_group_command(mode: &str) -> PreparedGroupCommand {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&execution_root).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let control_address = listener.local_addr().unwrap().to_string();
    let executable = std::env::current_exe().unwrap();
    let executable = executable.to_str().unwrap();
    let arguments = fixture_arguments();
    let argv = std::iter::once(executable)
        .chain(arguments.iter().map(String::as_str))
        .collect::<Vec<_>>();
    let source = workflow_source(&[("task", None, argv.as_slice())]);
    let environment = EnvironmentSnapshot::new([
        (
            OsString::from(FIXTURE_SOCKET),
            OsString::from(control_address),
        ),
        (OsString::from(FIXTURE_EXIT_CODE), OsString::from("0")),
        (OsString::from(FIXTURE_MODE), OsString::from(mode)),
        (OsString::from(FIXTURE_ROLE), OsString::from(FIXTURE_PARENT)),
    ]);
    let admitted = admit_fixture(temporary.path(), &execution_root, &source, environment, 1);
    PreparedGroupCommand {
        _temporary: temporary,
        listener,
        admitted,
    }
}

fn capturing_actions(
    admitted: &AdmittedWorkflow,
) -> (TestRuntimeState, BTreeMap<String, TestRequestedAction>) {
    let initialized = runtime::initialize::<
        ProvisionalStepOutputs,
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >(admitted, None);
    let starts = initialized
        .actions
        .iter()
        .filter_map(|requested| match &requested.action {
            Action::StartStep { step, .. } => Some((step.clone(), requested.id)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut state = initialized.state;
    for (step, action) in &starts {
        state = runtime::reduce::<
            ProvisionalStepOutputs,
            StepFailureCause,
            CapturedValue,
            TestInstant,
        >(
            &state,
            Occurrence::StepStarted {
                step: step.clone(),
                action: *action,
            },
        )
        .state;
    }

    let mut captures = BTreeMap::new();
    for (step, action) in starts {
        let reduction = runtime::reduce::<
            ProvisionalStepOutputs,
            StepFailureCause,
            CapturedValue,
            TestInstant,
        >(
            &state,
            Occurrence::StepExecutionCompleted {
                step: step.clone(),
                action,
                provisional: ProvisionalStepOutputs::command(),
            },
        );
        let capture = reduction
            .actions
            .iter()
            .find(|requested| matches!(requested.action, Action::CaptureOutputs { .. }))
            .unwrap()
            .clone();
        captures.insert(step, capture);
        state = reduction.state;
    }
    (state, captures)
}

fn running_cancellation_actions(
    admitted: &AdmittedWorkflow,
    deadline: TestInstant,
) -> (TestRequestedAction, TestRequestedAction) {
    let initialized = runtime::initialize::<
        ProvisionalStepOutputs,
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >(admitted, None);
    let start = initialized
        .actions
        .iter()
        .find(|requested| matches!(requested.action, Action::StartStep { .. }))
        .unwrap()
        .clone();
    let started =
        runtime::reduce::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, TestInstant>(
            &initialized.state,
            Occurrence::StepStarted {
                step: "task".to_owned(),
                action: start.id,
            },
        );
    let cancelled = runtime::reduce(
        &started.state,
        Occurrence::CancellationOperationRequested {
            operation: CancellationOperationId::fixture(1),
            reason: CancellationReason::UserRequest,
            deadline,
        },
    );
    let cancel = cancelled
        .actions
        .into_iter()
        .find(|requested| matches!(requested.action, Action::CancelStep { .. }))
        .unwrap();
    (start, cancel)
}

async fn accept_group(listener: &TcpListener) -> BTreeMap<String, (TcpStream, FixtureReport)> {
    let mut processes = BTreeMap::new();
    for _ in 0..2 {
        let (stream, report) = accept_report(listener).await;
        assert!(
            processes
                .insert(report.role.clone(), (stream, report))
                .is_none()
        );
    }

    let parent = &processes[FIXTURE_PARENT].1;
    let descendant = &processes[FIXTURE_DESCENDANT].1;
    assert!(parent.process_group_leader);
    assert!(!descendant.process_group_leader);
    assert_eq!(parent.process_group, descendant.process_group);
    processes
}

async fn assert_group_interrupted(processes: &BTreeMap<String, (TcpStream, FixtureReport)>) {
    let parent = next_fixture_event(&processes[FIXTURE_PARENT].0);
    let descendant = next_fixture_event(&processes[FIXTURE_DESCENDANT].0);
    let (parent, descendant) = tokio::join!(parent, descendant);
    assert_eq!(parent.event, "interrupted");
    assert_eq!(descendant.event, "interrupted");
}

async fn release_group(processes: &BTreeMap<String, (TcpStream, FixtureReport)>) {
    release_control(&processes[FIXTURE_DESCENDANT].0).await;
    release_control(&processes[FIXTURE_PARENT].0).await;
}

async fn assert_group_closed(processes: &mut BTreeMap<String, (TcpStream, FixtureReport)>) {
    for (stream, _) in processes.values_mut() {
        await_fixture_eof(stream).await;
    }
}

fn fixture_arguments() -> Vec<String> {
    [
        "--ignored",
        "--exact",
        FIXTURE_TEST_NAME,
        "--nocapture",
        "--skip",
        LITERAL_ARGUMENT,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn fixture_environment(
    control_address: &str,
    exit_code: i32,
    path_directory: &Path,
    output_bytes: Option<usize>,
) -> EnvironmentSnapshot {
    let mut variables = vec![
        (
            OsString::from(FIXTURE_SOCKET),
            OsString::from(control_address),
        ),
        (
            OsString::from(FIXTURE_EXIT_CODE),
            OsString::from(exit_code.to_string()),
        ),
        (
            OsString::from("PATH"),
            path_directory.as_os_str().to_owned(),
        ),
        (
            OsString::from("EXPLICIT_VALUE"),
            OsString::from("from-admission"),
        ),
        (
            OsString::from("UM_INHERITED"),
            OsString::from("must-not-reach-command"),
        ),
    ];
    if let Some(output_bytes) = output_bytes {
        variables.push((
            OsString::from(FIXTURE_OUTPUT_BYTES),
            OsString::from(output_bytes.to_string()),
        ));
    }
    EnvironmentSnapshot::new(variables)
}

fn install_fixture(source: &Path, destination: &Path) {
    symlink(source, destination).unwrap();
}

fn input_command_source(step: &str, argv: &[&str]) -> String {
    format!(
        "schemaVersion: 1\ninputs:\n  request: {{kind: text}}\nsteps:\n  {step}:\n    kind: cmd\n    inputs:\n      prompt:\n        ref: inputs.request\n    command:\n      argv: {}\n",
        serde_json::to_string(argv).unwrap()
    )
}

fn workflow_source(steps: &[(&str, Option<&str>, &[&str])]) -> String {
    let mut source = String::from("schemaVersion: 1\nsteps:\n");
    for (step, cwd, argv) in steps {
        source.push_str(&format!("  {step}:\n    kind: cmd\n"));
        if let Some(cwd) = cwd {
            source.push_str(&format!(
                "    cwd: {}\n",
                serde_json::to_string(cwd).unwrap()
            ));
        }
        source.push_str(&format!(
            "    command:\n      argv: {}\n",
            serde_json::to_string(argv).unwrap()
        ));
    }
    source
}

fn admit_fixture(
    temporary_root: &Path,
    execution_root: &Path,
    source: &str,
    environment: EnvironmentSnapshot,
    maximum_parallel_steps: usize,
) -> AdmittedWorkflow {
    admit_fixture_with_limits(
        temporary_root,
        execution_root,
        source,
        environment,
        maximum_parallel_steps,
        CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
        1024 * 1024,
    )
}

fn admit_fixture_with_capture_limits(
    temporary_root: &Path,
    execution_root: &Path,
    source: &str,
    environment: EnvironmentSnapshot,
    maximum_parallel_steps: usize,
    capture_limits: CaptureLimits,
) -> AdmittedWorkflow {
    admit_fixture_with_limits(
        temporary_root,
        execution_root,
        source,
        environment,
        maximum_parallel_steps,
        capture_limits,
        1024 * 1024,
    )
}

fn admit_fixture_with_log_limit(
    temporary_root: &Path,
    execution_root: &Path,
    source: &str,
    environment: EnvironmentSnapshot,
    maximum_parallel_steps: usize,
    maximum_log_bytes: u64,
) -> AdmittedWorkflow {
    admit_fixture_with_limits(
        temporary_root,
        execution_root,
        source,
        environment,
        maximum_parallel_steps,
        CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
        maximum_log_bytes,
    )
}

fn admit_fixture_with_live_input_limit(
    temporary_root: &Path,
    execution_root: &Path,
    source: &str,
    environment: EnvironmentSnapshot,
    maximum_parallel_steps: usize,
    maximum_live_input_bytes: u64,
) -> AdmittedWorkflow {
    admit_fixture_with_inputs(
        temporary_root,
        execution_root,
        source,
        environment,
        FixtureExecution {
            limits: ExecutionPolicyLimits::new(
                maximum_parallel_steps,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(
                    1024,
                    1024 * 1024,
                    64 * 1024 * 1024,
                    maximum_live_input_bytes,
                ),
                1024 * 1024,
            ),
            imports: ResolvedInputs::default(),
        },
    )
}

fn admit_fixture_with_limits(
    temporary_root: &Path,
    execution_root: &Path,
    source: &str,
    environment: EnvironmentSnapshot,
    maximum_parallel_steps: usize,
    capture_limits: CaptureLimits,
    maximum_log_bytes: u64,
) -> AdmittedWorkflow {
    admit_fixture_with_inputs(
        temporary_root,
        execution_root,
        source,
        environment,
        FixtureExecution {
            limits: ExecutionPolicyLimits::new(
                maximum_parallel_steps,
                capture_limits,
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                maximum_log_bytes,
            ),
            imports: ResolvedInputs::default(),
        },
    )
}

struct FixtureExecution {
    limits: ExecutionPolicyLimits,
    imports: ResolvedInputs,
}

fn admit_fixture_with_inputs(
    temporary_root: &Path,
    execution_root: &Path,
    source: &str,
    environment: EnvironmentSnapshot,
    execution: FixtureExecution,
) -> AdmittedWorkflow {
    let source_root = temporary_root.join(format!(
        "source-{}",
        temporary_root.read_dir().unwrap().count()
    ));
    fs::create_dir(&source_root).unwrap();
    // Fixture processes need explicit passthrough in their workflow, just like production.
    let source = source.replacen(
        "schemaVersion: 1\n",
        "schemaVersion: 1\nenvironmentPassthrough: [EXPLICIT_VALUE, WORKFLOW_FIXTURE_SOCKET, WORKFLOW_FIXTURE_EXIT_CODE, WORKFLOW_FIXTURE_MODE, WORKFLOW_FIXTURE_OUTPUT_BYTES, WORKFLOW_FIXTURE_ROLE]\n",
        1,
    );
    fs::write(source_root.join("workflow.yaml"), source).unwrap();
    admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        execution.imports,
        ExecutionContext::new(
            execution_root.to_owned(),
            execution.limits,
            environment,
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap()
}

#[test]
fn absent_agent_response_remains_a_successful_empty_capture() {
    let temporary = tempfile::tempdir().unwrap();
    let execution_root = temporary.path().join("execution");
    fs::create_dir(&execution_root).unwrap();
    let source_root = temporary.path().join("source");
    fs::create_dir(&source_root).unwrap();
    fs::write(
        source_root.join("workflow.yaml"),
        r#"schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  respond:
    kind: agent
    agent:
      profile: coding
      systemPrompt: workflow.yaml
      message:
        text:
          - file: workflow.yaml
    outputs:
      response:
        kind: text
        from: agent_response
"#,
    )
    .unwrap();
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedInputs::default(),
        ExecutionContext::new(
            execution_root,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                1024 * 1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        )
        .with_pi_installation(crate::pi::ValidatedPiInstallation::fixture(PathBuf::from(
            "/validated/pi",
        ))),
    )
    .unwrap();
    let artifacts = test_artifacts(&admitted);
    let (occurrences, _receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    let worker = CaptureWorker {
        diagnostics: StepDiagnosticLog::default(),
        admitted,
        artifacts: artifacts.staging.clone(),
        occurrences,
        work: Arc::new(Mutex::new(CaptureWorkRegistry::new())),
    };
    let values = completed_agent_outputs(
        Some("response".to_owned()),
        CompletedAgentInvocation::NoResponse,
    )
    .unwrap();
    let value_names = values.keys().cloned().collect();
    let Ok(candidates) =
        worker.capture_outputs_blocking("respond", &value_names, &CaptureCancellation::default())
    else {
        panic!("an absent optional response must remain a successful empty capture");
    };

    assert!(candidates.outputs().is_empty());
}

#[test]
fn agent_finalization_context_is_an_engine_input_not_an_upstream_output() {
    let temporary = tempfile::tempdir().unwrap();
    fs::write(temporary.path().join("system.md"), "System.\n").unwrap();
    fs::write(temporary.path().join("message.md"), "Message.\n").unwrap();
    fs::write(
        temporary.path().join("workflow.yaml"),
        r#"schemaVersion: 1
agentProfiles:
  reporting:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  work:
    kind: cmd
    command:
      argv: ["true"]
finalizers:
  notify:
    kind: agent
    agent:
      profile: reporting
      systemPrompt: system.md
      message:
        text:
          - file: message.md
        attachments:
          - ref: finalization.context
"#,
    )
    .unwrap();
    let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
    let ValidatedStep::Agent(agent) = &workflow.definition.finalizers["notify"].body else {
        panic!("notify must resolve as an agent finalizer");
    };
    let context: Arc<[u8]> = Arc::from(br#"{"schemaVersion":1}"#.as_slice());
    let inputs = BTreeMap::from([(
        "finalization.context".to_owned(),
        ActionInput::FinalizationContext(context),
    )]);

    assert_eq!(
        resolve_agent_upstream_outputs(agent, &inputs),
        Ok(BTreeMap::new())
    );
}

fn start_actions(admitted: &AdmittedWorkflow) -> BTreeMap<String, ActionId> {
    runtime::initialize::<ProvisionalStepOutputs, StepFailureCause, CapturedValue, ()>(
        admitted, None,
    )
    .actions
    .into_iter()
    .filter_map(|requested| match requested.action {
        Action::StartStep { step, .. } => Some((step, requested.id)),
        Action::StartRecoveryHandler { .. }
        | Action::CaptureOutputs { .. }
        | Action::ReleaseStepResources { .. }
        | Action::CancelStep { .. }
        | Action::ForceAbortStep { .. }
        | Action::FinishRun { .. } => None,
    })
    .collect()
}

async fn assert_start_failure(admitted: AdmittedWorkflow, step: &str, expected: StepStartFailure) {
    let action = start_actions(&admitted)[step];
    let (sender, mut receiver) = occurrence_channel(NonZeroUsize::new(1).unwrap());
    let artifacts = test_artifacts(&admitted);
    let runtime = StepRuntime::new(
        admitted,
        artifacts.staging.clone(),
        artifacts.inputs.clone(),
        sender,
        TestClock,
    );
    assert_eq!(runtime.execute_step(step.to_owned(), action).await, Ok(()));
    assert_eq!(
        next_occurrence(&mut receiver).await,
        Occurrence::StepStartFailed {
            step: step.to_owned(),
            action,
            cause: StepFailureCause::Start(expected),
        }
    );
}

async fn next_occurrence(receiver: &mut TestReceiver) -> TestOccurrence {
    receiver.recv().await.unwrap().into_runtime()
}

async fn next_acknowledged_occurrence(
    receiver: &mut TestReceiver,
) -> (TestOccurrence, DriverOccurrenceTestAcknowledgement) {
    let (occurrence, acknowledgement) = receiver.recv_with_acknowledgement().await.unwrap();
    (occurrence.into_runtime(), acknowledgement)
}

async fn accept_report(listener: &TcpListener) -> (TcpStream, FixtureReport) {
    let (stream, _) = listener.accept().await.unwrap();
    let report = read_fixture_line(&stream).await;
    (stream, serde_json::from_slice(&report).unwrap())
}

async fn next_fixture_event(stream: &TcpStream) -> FixtureEvent {
    serde_json::from_slice(&read_fixture_line(stream).await).unwrap()
}

async fn read_fixture_line(stream: &TcpStream) -> Vec<u8> {
    try_read_fixture_line(stream)
        .await
        .unwrap_or_else(|| panic!("fixture closed before its control message"))
}

async fn try_read_fixture_line(stream: &TcpStream) -> Option<Vec<u8>> {
    let mut line = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        // A report and its next event can arrive in one TCP read. Inspect the
        // available bytes first so this read leaves later messages on the socket.
        let available = match stream.peek(&mut buffer).await {
            Ok(0) => return None,
            Ok(read) => read,
            Err(failure)
                if matches!(
                    failure.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                return None;
            }
            Err(failure) => panic!("fixture control peek failed: {failure:?}"),
        };
        let through_line = buffer[..available]
            .iter()
            .position(|&byte| byte == b'\n')
            .map_or(available, |index| index + 1);
        stream.readable().await.unwrap();
        match stream.try_read(&mut buffer[..through_line]) {
            Ok(0) => return None,
            Ok(read) => {
                line.extend_from_slice(&buffer[..read]);
                assert!(line.len() <= 16 * 1024);
                if line.last() == Some(&b'\n') {
                    line.pop();
                    return Some(line);
                }
            }
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure)
                if matches!(
                    failure.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                return None;
            }
            Err(failure) => panic!("fixture control read failed: {failure:?}"),
        }
    }
}

async fn probe_fixture_alive(stream: &TcpStream) -> bool {
    if !send_fixture_control(stream, 2).await {
        return false;
    }
    let Some(event) = try_read_fixture_line(stream).await else {
        return false;
    };
    assert_eq!(
        serde_json::from_slice::<FixtureEvent>(&event)
            .unwrap()
            .event,
        "alive"
    );
    true
}

async fn await_fixture_eof(stream: &TcpStream) {
    let mut buffer = [0_u8; 256];
    loop {
        stream.readable().await.unwrap();
        match stream.try_read(&mut buffer) {
            Ok(0) => return,
            Ok(_) => panic!("fixture sent an unexpected late control message"),
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure)
                if matches!(
                    failure.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                return;
            }
            Err(failure) => panic!("fixture closure read failed: {failure:?}"),
        }
    }
}

async fn release(stream: TcpStream) {
    release_control(&stream).await;
}

async fn release_control(stream: &TcpStream) {
    assert!(send_fixture_control(stream, 1).await);
}

async fn send_fixture_control(stream: &TcpStream, control: u8) -> bool {
    loop {
        stream.writable().await.unwrap();
        match stream.try_write(&[control]) {
            Ok(1) => return true,
            Ok(_) => {}
            Err(failure) if failure.kind() == io::ErrorKind::WouldBlock => {}
            Err(failure)
                if matches!(
                    failure.kind(),
                    io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                return false;
            }
            Err(failure) => panic!("fixture control write failed: {failure:?}"),
        }
    }
}

// REQ-WFRECOVERY-62
#[test]
fn recovery_handler_launch_refusal_keeps_its_cause() {
    assert_eq!(
        recovery_handler_launch_failure(StepStartFailure::CommandLaunch(
            CommandLaunchFailure::NotFound
        )),
        RecoveryHandlerFailure::CommandLaunch(CommandLaunchFailure::NotFound)
    );
    assert_eq!(
        recovery_handler_launch_failure(StepStartFailure::WorkingDirectory(
            WorkingDirectoryFailure::ExecutionRootRebound
        )),
        RecoveryHandlerFailure::WorkingDirectoryUnavailable
    );
    assert_eq!(
        recovery_handler_launch_failure(StepStartFailure::StepUnavailable),
        RecoveryHandlerFailure::CommandLaunch(CommandLaunchFailure::Other(
            CommandLaunchRefusal::new(CommandLaunchCause::LaunchTaskFailed)
        ))
    );
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is allowed only as an anti-hang watchdog, not a behavior assertion"
)]
fn with_watchdog<Output>(future: impl Future<Output = Output>) -> impl Future<Output = Output> {
    // Heap-pinned before the returned future exists, as `test_support::host_scaled_watchdog`
    // explains, so no wrapper state holds the body.
    let future = Box::pin(future);
    async move {
        match tokio::time::timeout(TEST_WATCHDOG, future).await {
            Ok(output) => output,
            Err(_) => panic!("workflow test watchdog expired"),
        }
    }
}

const CHILD_PARENT_WORKFLOW: &str = "schemaVersion: 1
inputs:
  ticket:
    kind: text
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    inputs:
      ticket:
        ref: inputs.ticket
    on:
      planned: exit.landed
      blocked: exit.failed
      failed: exit.failed
";

const CHILD_DESIGN_WORKFLOW: &str = "schemaVersion: 1
inputs:
  ticket:
    kind: text
outcomes: [planned, blocked, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict.txt
      plan:
        kind: text
        from: path
        path: plan.txt
exports:
  plan:
    ref: outputs.decide.plan
";

struct ChildFixture {
    _temporary: tempfile::TempDir,
    admitted: AdmittedWorkflow,
}

fn admit_child_fixture() -> ChildFixture {
    admit_child_parent(CHILD_PARENT_WORKFLOW)
}

/// `parent` admitted as the root, its `workflow` steps running `design.yaml`.
fn admit_child_parent(parent: &str) -> ChildFixture {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("root");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(source_root.join("workflow.yaml"), parent).unwrap();
    fs::write(source_root.join("design.yaml"), CHILD_DESIGN_WORKFLOW).unwrap();
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedInputs::new(BTreeMap::from([(
            "ticket".to_owned(),
            ResolvedInput::Text(Arc::from("AUTO-1")),
        )])),
        ExecutionContext::new(
            execution_root,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                1024 * 1024,
            ),
            EnvironmentSnapshot::new(Vec::<(OsString, OsString)>::new()),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();
    ChildFixture {
        _temporary: temporary,
        admitted,
    }
}

/// What a scripted child reports: its request, the cancellation reason it observed, and
/// whether it observed a force abort.
type ChildReport = (ChildRunRequest, Option<CancellationReason>, bool);

/// A child dispatcher answering its scripted endings in order, the last for every later
/// child; `started` reports the child's directory first, `await_cancellation` holds the
/// child until its source is cancelled, and `await_force_abort` then holds it until its
/// source is force-aborted.
#[derive(Clone)]
struct ScriptedChildDispatcher {
    outcomes: Arc<std::sync::Mutex<std::collections::VecDeque<ChildRunOutcome>>>,
    started: bool,
    await_cancellation: bool,
    /// The steps a stop control is in force for (REQ-WFEXEC-62).
    stopped: Vec<String>,
    await_force_abort: bool,
    requests: mpsc::UnboundedSender<ChildReport>,
}

impl ChildRunDispatcher for ScriptedChildDispatcher {
    async fn stop_before(&self, step: &str) -> bool {
        self.stopped.iter().any(|stopped| stopped == step)
    }

    async fn run(&self, request: ChildRunRequest, mut control: ChildRunControl) -> ChildRunOutcome {
        if self.started {
            control.report_started(PathBuf::from("children/design/000001"));
        }
        let observed = if self.await_cancellation {
            let _ = self.requests.send((request.clone(), None, false));
            Some(control.cancellation().wait_for_cancellation().await)
        } else {
            None
        };
        if self.await_force_abort {
            control.cancellation().wait_for_force_abort().await;
        }
        let _ = self
            .requests
            .send((request, observed, self.await_force_abort));
        let mut outcomes = self.outcomes.lock().unwrap();
        if outcomes.len() > 1 {
            outcomes.pop_front().unwrap()
        } else {
            outcomes.front().unwrap().clone()
        }
    }
}

fn scripted_child(
    outcome: ChildRunOutcome,
    started: bool,
) -> (
    ScriptedChildDispatcher,
    mpsc::UnboundedReceiver<ChildReport>,
) {
    scripted_children(vec![outcome], started)
}

fn scripted_children(
    outcomes: Vec<ChildRunOutcome>,
    started: bool,
) -> (
    ScriptedChildDispatcher,
    mpsc::UnboundedReceiver<ChildReport>,
) {
    let (requests, received) = mpsc::unbounded_channel();
    (
        ScriptedChildDispatcher {
            outcomes: Arc::new(std::sync::Mutex::new(outcomes.into())),
            started,
            await_cancellation: false,
            stopped: Vec::new(),
            await_force_abort: false,
            requests,
        },
        received,
    )
}

/// A parent whose two steps run one child file that takes the control values as optional
/// inputs (REQ-WFFORMAT-95).
const CONTROL_PARENT_WORKFLOW: &str = "schemaVersion: 1
outcomes: [landed]
steps:
  design:
    kind: workflow
    workflow: child.yaml
    inputs:
      notes:
        ref: wait.notes
      answers:
        ref: controls.answers
    on:
      done: fix
      failed: exit.landed
  fix:
    kind: workflow
    workflow: child.yaml
    inputs:
      notes:
        ref: wait.notes
      answers:
        ref: controls.answers
    on:
      done: exit.landed
      failed: exit.landed
";

const CONTROL_CHILD_WORKFLOW: &str = "schemaVersion: 1
inputs:
  notes:
    kind: text
    optional: true
  answers:
    kind: json
    optional: true
outcomes: [done, failed]
outcome:
  from: outputs.work.verdict
  onFailure: failed
steps:
  work:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict.txt
";

fn admit_control_fixture() -> ChildFixture {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("root");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(source_root.join("workflow.yaml"), CONTROL_PARENT_WORKFLOW).unwrap();
    fs::write(source_root.join("child.yaml"), CONTROL_CHILD_WORKFLOW).unwrap();
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedInputs::new(BTreeMap::new()),
        ExecutionContext::new(
            execution_root,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                1024 * 1024,
            ),
            EnvironmentSnapshot::new(Vec::<(OsString, OsString)>::new()),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();
    ChildFixture {
        _temporary: temporary,
        admitted,
    }
}

async fn execute_with_children(
    admitted: AdmittedWorkflow,
    children: ChildRuns<ScriptedChildDispatcher>,
) -> crate::workflow::coordinator::CoordinationResult<StepFailureCause, CapturedValue, TestInstant>
{
    let artifacts = test_artifacts(&admitted);
    let (commits, _committed) = mpsc::unbounded_channel();
    execute_workflow_observed_with_children(
        admitted,
        &artifacts.staging,
        &artifacts.inputs,
        &StepDiagnosticLog::default(),
        TestClock,
        RecordingCommitPort { commits },
        NoopExecutionObserver,
        AgentExecution::disabled(),
        children,
        WorkflowExecutionStart::initial(ProcessGuardRegistry::default()),
    )
    .await
    .unwrap()
}

fn assert_failed_with(
    result: &crate::workflow::coordinator::CoordinationResult<
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >,
    phase: crate::workflow::evidence::FailurePhase,
    code: FailureCode,
) {
    assert_step_failed_with(result, "design", phase, code);
}

fn assert_step_failed_with(
    result: &crate::workflow::coordinator::CoordinationResult<
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >,
    step: &str,
    phase: crate::workflow::evidence::FailurePhase,
    code: FailureCode,
) {
    let StepState::Failed { detail } = &result.state.steps[step].state else {
        panic!("{step} did not fail: {:?}", result.state.steps[step].state);
    };
    assert_eq!((detail.phase, detail.code), (phase, code));
    assert!(matches!(
        result.state.workflow,
        WorkflowState::Failed { .. }
    ));
}

// REQ-WFEXEC-60: a child that exits commits `outcome` and its available exports.
#[tokio::test]
async fn an_exited_child_commits_its_outcome_and_available_exports() {
    with_watchdog(async {
        let fixture = admit_child_fixture();
        let (dispatcher, mut requests) = scripted_child(
            ChildRunOutcome::Exited {
                outcome: "planned".to_owned(),
                exports: BTreeMap::from([(
                    "plan".to_owned(),
                    CapturedValue::text(Arc::from("plan: AUTO-1")),
                )]),
                interruption: None,
            },
            true,
        );
        let result = execute_with_children(
            fixture.admitted,
            ChildRuns::Enabled {
                dispatcher,
                controls: ControlContext::default(),
            },
        )
        .await;

        let (request, _, _) = requests.recv().await.unwrap();
        assert_eq!(request.workflow, "design.yaml");
        assert_eq!(
            request.inputs,
            BTreeMap::from([(
                "ticket".to_owned(),
                ChildInputValue::Text(Arc::from("AUTO-1"))
            )])
        );
        assert_eq!(result.state.workflow, WorkflowState::Succeeded);
        assert_eq!(
            result.state.steps["design"].state,
            StepState::Succeeded {
                outputs: BTreeMap::from([
                    (
                        "outcome".to_owned(),
                        CapturedValue::text(Arc::from("planned"))
                    ),
                    (
                        "plan".to_owned(),
                        CapturedValue::text(Arc::from("plan: AUTO-1"))
                    ),
                ]),
            }
        );
    })
    .await;
}

/// A parent whose `build` step fans out over `design`'s `parts` (REQ-WFFORMAT-98) and lands
/// through a later step; `{for_each}` is the step's `forEach` value.
const FAN_OUT_PARENT_WORKFLOW: &str = "schemaVersion: 1
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    on:
      planned: build
      failed: exit.failed
  build:
    kind: workflow
    workflow: build.yaml
    forEach: {for_each}
    inputs:
      part:
        ref: item.name
      plan:
        ref: outputs.design.plan
    on:
      built: land
      blocked:
        wait: answer
        then: build
      failed: exit.failed
  land:
    kind: workflow
    workflow: land.yaml
    on:
      landed: exit.landed
      failed: exit.failed
";

const FAN_OUT_DESIGN_WORKFLOW: &str = "schemaVersion: 1
outcomes: [planned, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict.txt
      parts:
        kind: json
        from: path
        path: parts.json
        schema: parts.schema.json
      plan:
        kind: text
        from: path
        path: plan.txt
exports:
  parts:
    ref: outputs.decide.parts
  plan:
    ref: outputs.decide.plan
";

/// `build.yaml`, whose `part` input is optional when `{optional}` declares it so.
const FAN_OUT_BUILD_WORKFLOW: &str = "schemaVersion: 1
inputs:
  part:
    kind: text
{optional}  plan:
    kind: text
outcomes: [built, blocked, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict.txt
";

const FAN_OUT_LAND_WORKFLOW: &str = "schemaVersion: 1
outcomes: [landed, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
steps:
  decide:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      verdict:
        kind: text
        from: path
        path: verdict.txt
";

fn admit_fan_out_fixture(for_each: &str, optional_part: bool) -> ChildFixture {
    admit_fan_out_parent(
        &FAN_OUT_PARENT_WORKFLOW.replace("{for_each}", for_each),
        optional_part,
    )
}

/// `parent` admitted as the root beside the fan-out fixture's child files.
fn admit_fan_out_parent(parent: &str, optional_part: bool) -> ChildFixture {
    let temporary = tempfile::tempdir().unwrap();
    let source_root = temporary.path().join("source");
    let execution_root = temporary.path().join("root");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&execution_root).unwrap();
    fs::write(source_root.join("workflow.yaml"), parent).unwrap();
    fs::write(source_root.join("design.yaml"), FAN_OUT_DESIGN_WORKFLOW).unwrap();
    fs::write(
        source_root.join("parts.schema.json"),
        r#"{"$schema":"https://json-schema.org/draft/2020-12/schema"}"#,
    )
    .unwrap();
    fs::write(
        source_root.join("build.yaml"),
        FAN_OUT_BUILD_WORKFLOW.replace(
            "{optional}",
            if optional_part {
                "    optional: true\n"
            } else {
                ""
            },
        ),
    )
    .unwrap();
    fs::write(source_root.join("land.yaml"), FAN_OUT_LAND_WORKFLOW).unwrap();
    let admitted = admit_workflow(
        resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedInputs::new(BTreeMap::new()),
        ExecutionContext::new(
            execution_root,
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024),
                InputLimits::new(1024, 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024),
                1024 * 1024,
            ),
            EnvironmentSnapshot::new(Vec::<(OsString, OsString)>::new()),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();
    ChildFixture {
        _temporary: temporary,
        admitted,
    }
}

fn exited(outcome: &str) -> ChildRunOutcome {
    ChildRunOutcome::Exited {
        outcome: outcome.to_owned(),
        exports: BTreeMap::new(),
        interruption: None,
    }
}

/// `design`'s ending: `planned` with `parts` and the plan text exported.
fn planned_with_parts(parts: serde_json::Value) -> ChildRunOutcome {
    ChildRunOutcome::Exited {
        outcome: "planned".to_owned(),
        exports: BTreeMap::from([
            (
                "parts".to_owned(),
                CapturedValue::json_fixture(Arc::new(parts)),
            ),
            (
                "plan".to_owned(),
                CapturedValue::text(Arc::from("one plan")),
            ),
        ]),
        interruption: None,
    }
}

/// Runs the fan-out parent with `endings` scripted after `design`'s, answering every
/// `build` request in order.
async fn run_fan_out(
    fixture: ChildFixture,
    parts: serde_json::Value,
    endings: &[&str],
    controls: ControlContext,
) -> (
    crate::workflow::coordinator::CoordinationResult<StepFailureCause, CapturedValue, TestInstant>,
    Vec<ChildRunRequest>,
) {
    let mut outcomes = vec![planned_with_parts(parts)];
    outcomes.extend(endings.iter().copied().map(exited));
    let (dispatcher, mut requests) = scripted_children(outcomes, true);
    let result = execute_with_children(
        fixture.admitted,
        ChildRuns::Enabled {
            dispatcher,
            controls,
        },
    )
    .await;
    let mut built = Vec::new();
    while let Ok((request, _, _)) = requests.try_recv() {
        if request.step == "build" {
            built.push(request);
        }
    }
    (result, built)
}

fn committed_outcome(
    result: &crate::workflow::coordinator::CoordinationResult<
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >,
    step: &str,
) -> Option<CapturedValue> {
    match &result.state.steps[step].state {
        StepState::Succeeded { outputs } => outputs.get("outcome").cloned(),
        _ => None,
    }
}

fn parts_abc() -> serde_json::Value {
    serde_json::json!([{"name": "a"}, {"name": "b"}, {"name": "c"}])
}

// REQ-WFEXEC-66, REQ-WFEXEC-67: a fan-out runs one child per element in order with `item`
// bound per element and every other input the same, goes on past an element whose outcome
// routes to a later step, and commits the last element's outcome once.
#[tokio::test]
async fn a_fan_out_runs_one_child_per_element_and_commits_the_last_outcome() {
    with_watchdog(async {
        let fixture = admit_fan_out_fixture("outputs.design.parts", false);
        let (result, built) = run_fan_out(
            fixture,
            parts_abc(),
            &["built", "built", "built", "landed"],
            ControlContext::default(),
        )
        .await;
        assert_eq!(
            built
                .iter()
                .map(|request| (request.item, request.inputs.clone()))
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
                .into_iter()
                .zip(1..)
                .map(|(part, item)| (
                    Some(item),
                    BTreeMap::from([
                        ("part".to_owned(), ChildInputValue::Text(Arc::from(part))),
                        (
                            "plan".to_owned(),
                            ChildInputValue::Text(Arc::from("one plan"))
                        ),
                    ])
                ))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            committed_outcome(&result, "build"),
            Some(CapturedValue::text(Arc::from("built")))
        );
        assert_eq!(
            committed_outcome(&result, "land"),
            Some(CapturedValue::text(Arc::from("landed")))
        );
        assert_eq!(result.state.exit_outcome.as_deref(), Some("landed"));
    })
    .await;
}

// REQ-WFEXEC-66: an element whose outcome routes to a wait or an exit ends the step there;
// the elements after it do not run.
#[tokio::test]
async fn a_fan_out_ends_at_an_element_that_waits_or_exits() {
    with_watchdog(async {
        let fixture = admit_fan_out_fixture("outputs.design.parts", false);
        let (result, built) = run_fan_out(
            fixture,
            parts_abc(),
            &["built", "blocked"],
            ControlContext::default(),
        )
        .await;
        assert_eq!(
            built.iter().map(|request| request.item).collect::<Vec<_>>(),
            [Some(1), Some(2)]
        );
        assert_eq!(
            committed_outcome(&result, "build"),
            Some(CapturedValue::text(Arc::from("blocked")))
        );
        let WorkflowState::Waiting { wait } = &result.state.workflow else {
            panic!("the run did not wait: {:?}", result.state.workflow);
        };
        assert_eq!(
            (wait.step(), wait.kind_name(), wait.then()),
            ("build", "answer", "build")
        );

        let fixture = admit_fan_out_fixture("outputs.design.parts", false);
        let (result, built) = run_fan_out(
            fixture,
            parts_abc(),
            &["built", "failed"],
            ControlContext::default(),
        )
        .await;
        assert_eq!(built.len(), 2);
        assert_eq!(
            committed_outcome(&result, "build"),
            Some(CapturedValue::text(Arc::from("failed")))
        );
        assert_eq!(result.state.exit_outcome.as_deref(), Some("failed"));
        assert!(matches!(
            result.state.steps["land"].state,
            StepState::NotRun { .. }
        ));
    })
    .await;
}

// REQ-WFEXEC-68: a continuation at the waiting step starts the fan-out at the element the
// wait recorded.
#[tokio::test]
async fn a_fan_out_resumes_at_the_element_its_wait_recorded() {
    with_watchdog(async {
        let fixture = admit_fan_out_fixture("outputs.design.parts", false);
        let (result, built) = run_fan_out(
            fixture,
            parts_abc(),
            &["built", "built", "landed"],
            ControlContext {
                resume_item: Some(("build".to_owned(), 2)),
                ..ControlContext::default()
            },
        )
        .await;
        assert_eq!(
            built
                .iter()
                .map(|request| (request.item, request.inputs["part"].clone()))
                .collect::<Vec<_>>(),
            [
                (Some(2), ChildInputValue::Text(Arc::from("b"))),
                (Some(3), ChildInputValue::Text(Arc::from("c"))),
            ]
        );
        assert_eq!(result.state.exit_outcome.as_deref(), Some("landed"));
    })
    .await;
}

// REQ-WFEXEC-66: an empty list fails the step at `start` unless `empty` names its outcome,
// which the step commits without running a child.
#[tokio::test]
async fn an_empty_fan_out_fails_at_start_or_commits_its_empty_outcome() {
    with_watchdog(async {
        let fixture = admit_fan_out_fixture("outputs.design.parts", false);
        let (result, built) = run_fan_out(
            fixture,
            serde_json::json!([]),
            &["built"],
            ControlContext::default(),
        )
        .await;
        assert!(built.is_empty());
        assert_step_failed_with(
            &result,
            "build",
            crate::workflow::evidence::FailurePhase::Start,
            FailureCode::InputsUnavailable,
        );

        let fixture = admit_fan_out_fixture("{items: outputs.design.parts, empty: built}", false);
        let (result, built) = run_fan_out(
            fixture,
            serde_json::json!([]),
            &["landed"],
            ControlContext::default(),
        )
        .await;
        assert!(built.is_empty());
        assert_eq!(
            result.state.steps["build"].state,
            StepState::Succeeded {
                outputs: BTreeMap::from([(
                    "outcome".to_owned(),
                    CapturedValue::text(Arc::from("built"))
                )]),
            }
        );
        assert_eq!(result.state.exit_outcome.as_deref(), Some("landed"));
    })
    .await;
}

// REQ-WFEXEC-67: a list that is not an array, or an element whose `item.<field>` is no
// string for a required `text` input, fails the step at `start`; an optional input goes
// without it.
#[tokio::test]
async fn fan_out_values_are_checked_when_the_step_starts() {
    with_watchdog(async {
        let fixture = admit_fan_out_fixture("outputs.design.parts", false);
        let (result, built) = run_fan_out(
            fixture,
            serde_json::json!({"name": "a"}),
            &["built"],
            ControlContext::default(),
        )
        .await;
        assert!(built.is_empty());
        assert_step_failed_with(
            &result,
            "build",
            crate::workflow::evidence::FailurePhase::Start,
            FailureCode::InputsUnavailable,
        );

        let fixture = admit_fan_out_fixture("outputs.design.parts", false);
        let (result, built) = run_fan_out(
            fixture,
            serde_json::json!([{"name": 7}]),
            &["built"],
            ControlContext::default(),
        )
        .await;
        assert!(built.is_empty());
        assert_step_failed_with(
            &result,
            "build",
            crate::workflow::evidence::FailurePhase::Start,
            FailureCode::InputsUnavailable,
        );

        let fixture = admit_fan_out_fixture("outputs.design.parts", true);
        let (result, built) = run_fan_out(
            fixture,
            serde_json::json!([{"name": 7}]),
            &["built", "landed"],
            ControlContext::default(),
        )
        .await;
        assert_eq!(
            built
                .iter()
                .map(|request| request.inputs.keys().cloned().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            [vec!["plan".to_owned()]]
        );
        assert_eq!(result.state.exit_outcome.as_deref(), Some("landed"));
    })
    .await;
}

// REQ-WFEXEC-60: a child that cannot be admitted fails the step at `start`.
#[tokio::test]
async fn a_rejected_child_fails_the_step_at_start() {
    with_watchdog(async {
        let fixture = admit_child_fixture();
        let (dispatcher, _requests) = scripted_child(
            ChildRunOutcome::Rejected {
                code: "resolution_failed".to_owned(),
            },
            false,
        );
        let result = execute_with_children(
            fixture.admitted,
            ChildRuns::Enabled {
                dispatcher,
                controls: ControlContext::default(),
            },
        )
        .await;
        assert_failed_with(
            &result,
            crate::workflow::evidence::FailurePhase::Start,
            FailureCode::ChildAdmissionRejected,
        );
    })
    .await;
}

// REQ-WFEXEC-60: a child that ends without a published result fails the step at
// `execution`.
#[tokio::test]
async fn an_interrupted_child_fails_the_step_at_execution() {
    with_watchdog(async {
        let fixture = admit_child_fixture();
        let (dispatcher, _requests) = scripted_child(ChildRunOutcome::Interrupted, true);
        let result = execute_with_children(
            fixture.admitted,
            ChildRuns::Enabled {
                dispatcher,
                controls: ControlContext::default(),
            },
        )
        .await;
        assert_failed_with(
            &result,
            crate::workflow::evidence::FailurePhase::Execution,
            FailureCode::ChildRunInterrupted,
        );
    })
    .await;
}

// REQ-WFEXEC-59: the runner runs no child, so its `workflow` step fails at `start`.
#[tokio::test]
async fn disabled_child_runs_fail_a_workflow_step_at_start() {
    with_watchdog(async {
        let fixture = admit_child_fixture();
        let result = execute_with_children(fixture.admitted, ChildRuns::Disabled).await;
        assert_failed_with(
            &result,
            crate::workflow::evidence::FailurePhase::Start,
            FailureCode::ChildAdmissionRejected,
        );
    })
    .await;
}

// REQ-WFEXEC-59: a cancellation of the parent reaches the child, and the step ends
// `cancelled`.
#[tokio::test]
async fn parent_cancellation_reaches_the_child_and_cancels_the_step() {
    with_watchdog(async {
        let fixture = admit_child_fixture();
        let parent = fixture.admitted.execution().cancellation().source().clone();
        let (mut dispatcher, mut requests) = scripted_child(ChildRunOutcome::Cancelled, true);
        dispatcher.await_cancellation = true;
        let execution = execute_with_children(
            fixture.admitted.clone(),
            ChildRuns::Enabled {
                dispatcher,
                controls: ControlContext::default(),
            },
        );
        let cancel = async {
            assert_eq!(requests.recv().await.unwrap().1, None);
            assert!(parent.request_cancellation(CancellationReason::UserRequest));
            requests.recv().await.unwrap().1
        };
        let (result, observed) = tokio::join!(execution, cancel);
        assert_eq!(observed, Some(CancellationReason::UserRequest));
        assert!(matches!(
            result.state.steps["design"].state,
            StepState::Cancelled { .. }
        ));
        assert!(matches!(
            result.state.workflow,
            WorkflowState::Cancelled { .. }
        ));
    })
    .await;
}

/// A root whose `design` and `build` steps run `design.yaml` (REQ-WFEXEC-70): `{planned}` is
/// `design`'s edge for its child's `planned` and `{failed}` its edge for `failed`.
const INTERRUPTED_PARENT_WORKFLOW: &str = "schemaVersion: 1
inputs:
  ticket:
    kind: text
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    inputs:
      ticket:
        ref: inputs.ticket
    on:
      planned: {planned}
      blocked: exit.failed
      failed:{failed}
  build:
    kind: workflow
    workflow: design.yaml
    inputs:
      ticket:
        ref: inputs.ticket
    on:
      planned: exit.landed
      blocked: exit.failed
      failed: exit.failed
";

const REPAIR_AT_DESIGN: &str = "\n        wait: repair\n        then: design";
const ANSWER_AT_DESIGN: &str = "\n        wait: answer\n        then: design";
const LIMITED_TO_DESIGN: &str =
    "\n        then: design\n        limit: 2\n        else: exit.failed";

fn interrupted_parent(planned: &str, failed: &str) -> ChildFixture {
    admit_child_parent(
        &INTERRUPTED_PARENT_WORKFLOW
            .replace("{planned}", planned)
            .replace("{failed}", failed),
    )
}

/// A child that published its failure outcome after an interruption failed its step `step`.
fn interrupted_exit(outcome: &str, step: &str) -> ChildRunOutcome {
    ChildRunOutcome::Exited {
        outcome: outcome.to_owned(),
        exports: BTreeMap::new(),
        interruption: Some(runtime::ChildInterruption {
            reason: CancellationReason::UserRequest,
            step: step.to_owned(),
        }),
    }
}

enum HeldChildEvent {
    Holding,
    Stopped { interrupted: bool },
}

/// What the interruption tests' children saw: every request in order, whether the held
/// child's stop was an interruption and a force abort reached it before it settled, and
/// whether the driver awaited the parent's force-stop deadline at all.
#[derive(Debug, Default)]
struct HeldChildObservation {
    requests: Vec<ChildRunRequest>,
    interrupted: Option<bool>,
    force_aborted: Option<bool>,
    deadline_awaited: bool,
}

/// A child dispatcher answering its scripted endings in order, the last for every later
/// child. The child at call `held`, counted from 0, holds until its source is stopped, then
/// until the test releases it, so the parent's force-stop deadline can pass meanwhile.
#[derive(Clone)]
struct HeldChildDispatcher {
    endings: Arc<std::sync::Mutex<std::collections::VecDeque<ChildRunOutcome>>>,
    held: usize,
    report_started: bool,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    events: mpsc::UnboundedSender<HeldChildEvent>,
    release: Arc<tokio::sync::Notify>,
    observation: Arc<std::sync::Mutex<HeldChildObservation>>,
}

impl ChildRunDispatcher for HeldChildDispatcher {
    async fn stop_before(&self, _step: &str) -> bool {
        false
    }

    async fn run(&self, request: ChildRunRequest, mut control: ChildRunControl) -> ChildRunOutcome {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.report_started {
            control.report_started(PathBuf::from(format!(
                "children/{}/{call:06}",
                request.step
            )));
        }
        self.observation.lock().unwrap().requests.push(request);
        if call == self.held {
            let source = control.cancellation().clone();
            let _ = self.events.send(HeldChildEvent::Holding);
            let _ = source.wait_for_cancellation().await;
            let _ = self.events.send(HeldChildEvent::Stopped {
                interrupted: source.interruption_requested(),
            });
            self.release.notified().await;
            let forced =
                futures_util::FutureExt::now_or_never(source.wait_for_force_abort()).is_some();
            self.observation.lock().unwrap().force_aborted = Some(forced);
        }
        let mut endings = self.endings.lock().unwrap();
        if endings.len() > 1 {
            endings.pop_front().unwrap()
        } else {
            endings.front().unwrap().clone()
        }
    }
}

/// Runs `fixture` with `endings` scripted, interrupts the run with `user_request` once the
/// child at call `held` runs and, when `running` names it, its step is `running`, passes the
/// parent's force-stop deadline while that child holds and then releases it
/// (REQ-WFEXEC-70). A child whose step is not named never reports that it started.
async fn interrupt_held_child(
    fixture: &ChildFixture,
    endings: Vec<ChildRunOutcome>,
    held: usize,
    running: Option<&str>,
) -> (
    crate::workflow::coordinator::CoordinationResult<StepFailureCause, CapturedValue, TestInstant>,
    HeldChildObservation,
) {
    interrupt_held_child_seeded(
        fixture,
        endings,
        held,
        running,
        runtime::ExecutionSeed::empty(),
    )
    .await
}

/// `interrupt_held_child` for an attempt started from `seed`.
async fn interrupt_held_child_seeded(
    fixture: &ChildFixture,
    endings: Vec<ChildRunOutcome>,
    held: usize,
    running: Option<&str>,
    seed: runtime::ExecutionSeed<CapturedValue>,
) -> (
    crate::workflow::coordinator::CoordinationResult<StepFailureCause, CapturedValue, TestInstant>,
    HeldChildObservation,
) {
    let parent = fixture.admitted.execution().cancellation().source().clone();
    let (events, mut received) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let observation = Arc::new(std::sync::Mutex::new(HeldChildObservation::default()));
    let dispatcher = HeldChildDispatcher {
        endings: Arc::new(std::sync::Mutex::new(endings.into())),
        held,
        report_started: running.is_some(),
        calls: Arc::default(),
        events,
        release: Arc::clone(&release),
        observation: Arc::clone(&observation),
    };
    let (clock, mut clock_control) = AdvancingClock::new(TestInstant(Duration::ZERO));
    let artifacts = test_artifacts(&fixture.admitted);
    let (commits, mut committed) = mpsc::unbounded_channel();
    let diagnostics = StepDiagnosticLog::default();
    let execution = execute_workflow_observed_with_children(
        fixture.admitted.clone(),
        &artifacts.staging,
        &artifacts.inputs,
        &diagnostics,
        clock,
        RecordingCommitPort { commits },
        NoopExecutionObserver,
        AgentExecution::disabled(),
        ChildRuns::Enabled {
            dispatcher,
            controls: ControlContext::default(),
        },
        WorkflowExecutionStart::seeded(ProcessGuardRegistry::default(), seed),
    );
    let interrupt = async {
        assert!(matches!(
            received.recv().await,
            Some(HeldChildEvent::Holding)
        ));
        if let Some(step) = running {
            while !matches!(
                committed.recv().await.unwrap().state.steps[step].state,
                StepState::Running
            ) {}
        }
        assert!(parent.request_interruption(CancellationReason::UserRequest));
        let Some(HeldChildEvent::Stopped { interrupted }) = received.recv().await else {
            panic!("the held child was not stopped");
        };
        // The parent's force-stop deadline passes while the child is still settling.
        clock_control.advance_to(TestInstant(Duration::from_secs(3_600)));
        release.notify_one();
        interrupted
    };
    let (result, interrupted) = tokio::join!(execution, interrupt);
    let mut observed = std::mem::take(&mut *observation.lock().unwrap());
    observed.interrupted = Some(interrupted);
    observed.deadline_awaited = clock_control.registrations.try_recv().is_ok();
    (result.unwrap(), observed)
}

fn not_reached_step(
    result: &crate::workflow::coordinator::CoordinationResult<
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >,
    step: &str,
) -> bool {
    matches!(
        &result.state.steps[step].state,
        StepState::NotRun { detail } if detail.code == crate::workflow::evidence::NonExecutionCode::NotReached
    )
}

// REQ-WFEXEC-70: an interruption reaches the running child as an interruption, never forced
// at the parent's deadline, and the child it failed routes the step to its `repair` wait,
// whose payload names the interruption.
#[tokio::test]
async fn an_interrupted_child_routes_its_step_to_the_repair_wait() {
    with_watchdog(async {
        let fixture = interrupted_parent("build", REPAIR_AT_DESIGN);
        let (result, observed) = interrupt_held_child(
            &fixture,
            vec![interrupted_exit("failed", "plan")],
            0,
            Some("design"),
        )
        .await;

        assert_eq!(observed.interrupted, Some(true));
        assert_eq!(observed.force_aborted, Some(false));
        assert!(!observed.deadline_awaited);
        assert_eq!(
            committed_outcome(&result, "design"),
            Some(CapturedValue::text(Arc::from("failed")))
        );
        let WorkflowState::Waiting { wait } = &result.state.workflow else {
            panic!("the run did not wait: {:?}", result.state.workflow);
        };
        assert_eq!(
            (wait.step(), wait.kind(), wait.then()),
            (
                "design",
                crate::workflow::document::WaitKind::Repair,
                "design"
            )
        );
        assert_eq!(
            wait.interrupted(),
            Some(&runtime::ChildInterruption {
                reason: CancellationReason::UserRequest,
                step: "plan".to_owned(),
            })
        );
        assert!(not_reached_step(&result, "build"));
        assert_eq!(observed.requests.len(), 1);

        let steps = result
            .state
            .steps
            .iter()
            .map(|(id, runtime)| (id.clone(), runtime.state.clone()))
            .collect::<BTreeMap<_, _>>();
        let payload =
            crate::workflow::wait::wait_payload(wait, &steps, Some("children/design/000000"), None);
        assert_eq!(payload["child"], "children/design/000000");
        assert_eq!(payload["outcome"], "failed");
        assert_eq!(
            payload["interrupted"],
            serde_json::json!({"reason": "user_request", "step": "plan"})
        );
        assert_eq!(
            payload["summary"]
                .as_str()
                .and_then(|summary| summary.split_whitespace().next()),
            Some("interrupted")
        );
    })
    .await;
}

// REQ-WFEXEC-70: an interrupted child whose failure edge is a step, an exit, a limited edge
// or an `answer` wait takes no edge: its step fails `interrupted` and the run fails.
#[tokio::test]
async fn an_interrupted_child_takes_no_edge_but_a_repair_wait() {
    with_watchdog(async {
        for failed in [
            " build",
            " exit.failed",
            LIMITED_TO_DESIGN,
            ANSWER_AT_DESIGN,
        ] {
            let fixture = interrupted_parent("build", failed);
            let (result, observed) = interrupt_held_child(
                &fixture,
                vec![interrupted_exit("failed", "plan")],
                0,
                Some("design"),
            )
            .await;

            assert_eq!(observed.interrupted, Some(true), "{failed}");
            assert_interrupted(
                &result.state.steps["design"].state,
                runtime::FailurePhase::Execution,
            );
            assert!(
                matches!(
                    &result.state.workflow,
                    WorkflowState::Failed {
                        primary_issue,
                        later_cancellation: Some(CancellationReason::UserRequest),
                    } if primary_issue.node.id == "design"
                ),
                "{failed}: {:?}",
                result.state.workflow
            );
            assert_eq!(result.state.exit_outcome, None, "{failed}");
            assert_eq!(result.state.wait, None, "{failed}");
            assert_eq!(result.state.continued, None, "{failed}");
            assert!(matches!(
                result.state.steps["build"].state,
                StepState::Cancelled { .. }
            ));
            assert_eq!(observed.requests.len(), 1, "{failed}");
        }
    })
    .await;
}

// REQ-WFEXEC-70, REQ-WFEXEC-64: a child that exits on its own under an interruption settles
// its step; an edge that would start a step ends the attempt `continued`, uncounted, with
// nothing started, and a wait or an exit edge is taken as without the interruption.
#[tokio::test]
async fn a_child_exiting_on_its_own_under_an_interruption_starts_no_step() {
    with_watchdog(async {
        let fixture = interrupted_parent("build", REPAIR_AT_DESIGN);
        let (result, observed) =
            interrupt_held_child(&fixture, vec![exited("planned")], 0, Some("design")).await;
        assert_eq!(
            committed_outcome(&result, "design"),
            Some(CapturedValue::text(Arc::from("planned")))
        );
        assert_eq!(
            result.state.workflow,
            WorkflowState::Continued {
                edge: runtime::TakenEdgeRecord {
                    step: "design".to_owned(),
                    outcome: "planned".to_owned(),
                    target: "build".to_owned(),
                    counted: false,
                },
            }
        );
        assert!(not_reached_step(&result, "build"));
        assert_eq!(observed.requests.len(), 1);

        let fixture = interrupted_parent("build", ANSWER_AT_DESIGN);
        let (result, _) =
            interrupt_held_child(&fixture, vec![exited("failed")], 0, Some("design")).await;
        let WorkflowState::Waiting { wait } = &result.state.workflow else {
            panic!("the run did not wait: {:?}", result.state.workflow);
        };
        assert_eq!(
            (wait.kind(), wait.interrupted()),
            (crate::workflow::document::WaitKind::Answer, None)
        );
        assert!(not_reached_step(&result, "build"));

        let fixture = interrupted_parent("build", ANSWER_AT_DESIGN);
        let (result, _) =
            interrupt_held_child(&fixture, vec![exited("blocked")], 0, Some("design")).await;
        assert_eq!(result.state.workflow, WorkflowState::Succeeded);
        assert_eq!(result.state.exit_outcome.as_deref(), Some("failed"));
        assert!(not_reached_step(&result, "build"));
    })
    .await;
}

// REQ-WFEXEC-70: a child that ends cancelled or without a result under an interruption
// fails its step `interrupted` at `execution`, and a step whose child had not started fails
// at `start`.
#[tokio::test]
async fn a_child_ending_without_an_outcome_under_an_interruption_fails_its_step() {
    with_watchdog(async {
        for ending in [ChildRunOutcome::Cancelled, ChildRunOutcome::Interrupted] {
            let fixture = interrupted_parent("build", REPAIR_AT_DESIGN);
            let (result, _) = interrupt_held_child(&fixture, vec![ending], 0, Some("design")).await;
            assert_interrupted(
                &result.state.steps["design"].state,
                runtime::FailurePhase::Execution,
            );
            assert!(matches!(
                result.state.workflow,
                WorkflowState::Failed { .. }
            ));
        }

        let fixture = interrupted_parent("build", REPAIR_AT_DESIGN);
        let (result, observed) =
            interrupt_held_child(&fixture, vec![interrupted_exit("failed", "plan")], 0, None).await;
        assert_eq!(observed.interrupted, Some(true));
        assert_interrupted(
            &result.state.steps["design"].state,
            runtime::FailurePhase::Start,
        );
        assert!(matches!(
            result.state.workflow,
            WorkflowState::Failed { .. }
        ));
        assert_eq!(result.state.wait, None);
    })
    .await;
}

// REQ-WFEXEC-72, REQ-WFEXEC-70: a continuation of a wait whose entry `workflow` step the
// interruption reaches before its child started keeps that wait, with the step `not_run` and
// no step failed.
#[tokio::test]
async fn an_interruption_before_a_continuations_child_starts_keeps_its_wait() {
    with_watchdog(async {
        let fixture = interrupted_parent("build", REPAIR_AT_DESIGN);
        let resumed = runtime::EnteredWait {
            step: "design".to_owned(),
            outcome: Some("failed".to_owned()),
            kind: crate::workflow::document::WaitKind::Repair,
            then: "design".to_owned(),
            on_hand_back: None,
            rule: None,
            interrupted: None,
            kept: None,
        };
        let seed = runtime::ExecutionSeed::empty()
            .with_entry("design")
            .with_resumed_wait(resumed.clone());
        let (result, observed) = interrupt_held_child_seeded(
            &fixture,
            vec![interrupted_exit("failed", "plan")],
            0,
            None,
            seed,
        )
        .await;

        assert_eq!(observed.interrupted, Some(true));
        let kept = runtime::EnteredWait {
            kept: Some(CancellationReason::UserRequest),
            ..resumed
        };
        assert_eq!(
            result.state.workflow,
            WorkflowState::Waiting { wait: kept.clone() }
        );
        assert_eq!(result.state.wait, Some(kept));
        assert!(not_reached_step(&result, "design"));
        assert!(not_reached_step(&result, "build"));
    })
    .await;
}

// REQ-WFEXEC-70, REQ-WFEXEC-66, REQ-WFEXEC-68: a fan-out interrupted at element 2 ends there
// on its `repair` wait and runs no element 3.
#[tokio::test]
async fn an_interrupted_fan_out_element_ends_the_step_on_its_repair_wait() {
    with_watchdog(async {
        let parent = FAN_OUT_PARENT_WORKFLOW
            .replace("{for_each}", "outputs.design.parts")
            .replace(
                "      failed: exit.failed\n  land:",
                "      failed:\n        wait: repair\n        then: build\n  land:",
            );
        let fixture = admit_fan_out_parent(&parent, false);
        let (result, observed) = interrupt_held_child(
            &fixture,
            vec![
                planned_with_parts(parts_abc()),
                exited("built"),
                interrupted_exit("failed", "plan"),
            ],
            2,
            Some("build"),
        )
        .await;

        assert_eq!(
            observed
                .requests
                .iter()
                .filter(|request| request.step == "build")
                .map(|request| request.item)
                .collect::<Vec<_>>(),
            [Some(1), Some(2)]
        );
        let WorkflowState::Waiting { wait } = &result.state.workflow else {
            panic!("the run did not wait: {:?}", result.state.workflow);
        };
        assert_eq!(
            (wait.step(), wait.kind()),
            ("build", crate::workflow::document::WaitKind::Repair)
        );
        assert!(wait.interrupted().is_some());
        assert!(not_reached_step(&result, "land"));
    })
    .await;
}

// REQ-WFEXEC-62: a stop control in force for a `workflow` step enters a `stop` wait at it
// instead of starting its child.
#[tokio::test]
async fn a_stop_in_force_enters_a_stop_wait_and_runs_no_child() {
    with_watchdog(async {
        let fixture = admit_child_fixture();
        let (mut dispatcher, mut requests) = scripted_child(
            ChildRunOutcome::Exited {
                outcome: "planned".to_owned(),
                exports: BTreeMap::new(),
                interruption: None,
            },
            true,
        );
        dispatcher.stopped = vec!["design".to_owned()];
        let result = execute_with_children(
            fixture.admitted,
            ChildRuns::Enabled {
                dispatcher,
                controls: ControlContext::default(),
            },
        )
        .await;

        let WorkflowState::Waiting { wait } = &result.state.workflow else {
            panic!("the run did not wait: {:?}", result.state.workflow);
        };
        assert_eq!(
            (wait.step(), wait.kind_name(), wait.then(), wait.outcome()),
            ("design", "stop", "design", None)
        );
        assert!(matches!(
            result.state.steps["design"].state,
            StepState::NotRun { .. }
        ));
        assert!(requests.try_recv().is_err());
    })
    .await;
}

/// A driver running `workflow` steps through `dispatcher`, with the `StartStep` the parent's
/// initial transition releases for `design`.
fn child_running_driver(
    admitted: &AdmittedWorkflow,
    artifacts: &TestArtifacts,
    dispatcher: ScriptedChildDispatcher,
) -> (
    StepRuntime<TestClock, NoopExecutionObserver, NoAgentDispatcher, ScriptedChildDispatcher>,
    TestReceiver,
    TestRequestedAction,
) {
    let start = runtime::initialize::<
        ProvisionalStepOutputs,
        StepFailureCause,
        CapturedValue,
        TestInstant,
    >(admitted, None)
    .actions
    .into_iter()
    .find(
        |requested| matches!(&requested.action, Action::StartStep { step, .. } if step == "design"),
    )
    .unwrap();
    let (sender, receiver) = occurrence_channel(NonZeroUsize::new(8).unwrap());
    let driver = StepRuntime::new(
        admitted.clone(),
        artifacts.staging.clone(),
        artifacts.inputs.clone(),
        sender,
        TestClock,
    )
    .with_children(ChildRuns::Enabled {
        dispatcher,
        controls: ControlContext::default(),
    });
    (driver, receiver, start)
}

fn design_cancellation(sequence: u64, force: bool) -> TestRequestedAction {
    let step = "design".to_owned();
    let active = ActiveStepInvocation::Target {
        execution_number: TargetExecutionNumber::FIRST,
    };
    let reason = CancellationReason::UserRequest;
    let deadline = TestInstant(Duration::ZERO);
    RequestedAction {
        id: ActionId {
            transition_sequence: TransitionSequence(sequence),
        },
        action: if force {
            Action::ForceAbortStep {
                step,
                active,
                reason,
                deadline,
            }
        } else {
            Action::CancelStep {
                step,
                active,
                reason,
                deadline,
            }
        },
    }
}

// REQ-WFEXEC-59: a force abort of the parent's step that follows its cancellation reaches
// the running child, and the driver settles the step's cancellation.
#[tokio::test]
async fn a_force_abort_after_cancellation_reaches_the_running_child() {
    with_watchdog(async {
        let fixture = admit_child_fixture();
        let artifacts = test_artifacts(&fixture.admitted);
        let (mut dispatcher, mut requests) = scripted_child(ChildRunOutcome::Cancelled, true);
        dispatcher.await_cancellation = true;
        dispatcher.await_force_abort = true;
        let (mut driver, mut receiver, start) =
            child_running_driver(&fixture.admitted, &artifacts, dispatcher);

        driver.release(start.clone()).await;
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepStarted {
                step: "design".to_owned(),
                action: start.id,
            }
        );
        assert_eq!(requests.recv().await.unwrap().1, None);
        driver
            .release(design_cancellation(1_u64 << 62, false))
            .await;
        let force_abort = design_cancellation(1_u64 << 63, true);
        driver.release(force_abort.clone()).await;

        let (_, observed, force_aborted) = requests.recv().await.unwrap();
        assert_eq!(observed, Some(CancellationReason::UserRequest));
        assert!(force_aborted);
        assert_eq!(
            next_occurrence(&mut receiver).await,
            Occurrence::StepQuiesced {
                step: "design".to_owned(),
                action: force_abort.id,
            }
        );
        assert_eq!(driver.active_work_count(), 0);
    })
    .await;
}

// REQ-WFEXEC-63: a mapped `wait.<field>` is the resolving control's field when the wait
// continues at the step and is omitted for any other step; `controls.answers` is the step's
// recorded answers.
#[tokio::test]
async fn control_values_reach_only_the_step_they_name() {
    with_watchdog(async {
        let fixture = admit_control_fixture();
        let (dispatcher, mut requests) = scripted_child(
            ChildRunOutcome::Exited {
                outcome: "done".to_owned(),
                exports: BTreeMap::new(),
                interruption: None,
            },
            true,
        );
        let answers = Arc::new(serde_json::json!({"1": "yes"}));
        let controls = ControlContext {
            wait: Some((
                "fix".to_owned(),
                BTreeMap::from([("notes".to_owned(), "keep it".to_owned())]),
            )),
            answers: BTreeMap::from([("design".to_owned(), Arc::clone(&answers))]),
            resume_item: None,
        };
        let result = execute_with_children(
            fixture.admitted,
            ChildRuns::Enabled {
                dispatcher,
                controls,
            },
        )
        .await;
        assert_eq!(result.state.workflow, WorkflowState::Succeeded);

        let (design, _, _) = requests.recv().await.unwrap();
        assert_eq!(design.step, "design");
        assert_eq!(
            design.inputs,
            BTreeMap::from([("answers".to_owned(), ChildInputValue::Json(answers))])
        );
        let (fix, _, _) = requests.recv().await.unwrap();
        assert_eq!(fix.step, "fix");
        assert_eq!(
            fix.inputs,
            BTreeMap::from([(
                "notes".to_owned(),
                ChildInputValue::Text(Arc::from("keep it"))
            )])
        );
    })
    .await;
}

// REQ-WFEXEC-59: quiescing the driver for a runner shutdown revokes the running child with
// the shutdown reason.
#[tokio::test]
async fn runner_shutdown_revocation_reaches_the_running_child() {
    with_watchdog(async {
        let fixture = admit_child_fixture();
        let artifacts = test_artifacts(&fixture.admitted);
        let (mut dispatcher, mut requests) = scripted_child(ChildRunOutcome::Cancelled, true);
        dispatcher.await_cancellation = true;
        let (mut driver, _receiver, start) =
            child_running_driver(&fixture.admitted, &artifacts, dispatcher);

        driver.release(start).await;
        assert_eq!(requests.recv().await.unwrap().1, None);
        driver.quiesce_active_work().await;

        assert_eq!(
            requests.recv().await.unwrap().1,
            Some(CancellationReason::RunnerShutdown)
        );
        assert_eq!(driver.active_work_count(), 0);
    })
    .await;
}
