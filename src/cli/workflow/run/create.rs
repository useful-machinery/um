use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read};
use std::ops::Add;
use std::os::fd::AsFd;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use clap::Args;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::unix::AsyncFd;

use crate::exit_code::{ExitCode, OutcomeClass};
#[cfg(test)]
use um_execution::admit_workflow;
use um_execution::{
    ActionId, AdmittedWorkflow, AgentExecution, AgentHarnessInstallationFailure, AgentInputStaging,
    ArtifactStaging, CancellationPolicy, CancellationReason, CancellationSource, ColorChoice,
    CommitFailure, CoordinationError, CoordinatorClock, DisplayDeadline, DurableDeadline,
    DurableInvocationStateV1, DurableInvocationV1, EnvironmentSnapshot, ExecutionContext,
    ExecutionObservation, ExecutionObserver, FailurePolicy, InitialLocalRun, InputStaging,
    InvocationAccountingLog, LocalAttemptOwner, LocalAttemptOwnershipReleased,
    LocalPublicationError, LocalPublicationPhase, MAXIMUM_PARALLEL_STEPS, ObservationClock,
    PresentationConfig, PresentationFailure, PresentationFailureOperation, PresentationMode,
    PublicationFailurePhaseV1, PublicationPresentation, RecoveryDiagnosticKindV1,
    RecoveryInvocationDiagnosticV1, RecoveryInvocationStateV1, RecoveryInvocationV1,
    RequestedPresentationMode, ResolvedAttachment, ResolvedFile, ResolvedInput, ResolvedInputs,
    ResolvedJsonInput, ResolvedWorkflow, RunOutcome, RunState, RunTimingObservation,
    RunTimingSnapshot, StepDiagnosticLog, SystemObservationClock, TerminalCapabilities,
    TerminalHostExit, TransitionSequence, TreeDiagnostic, UntestedHarnessAdvisory,
    ValidatedHarness, ValidatedRecoveryHandler, ValidatedStep, ValidatedTree,
    WorkflowExecutionResult, WorkflowExecutionStart, WorkflowNodeRole, WorkflowRunCancellation,
    WorkflowRunCleanupResult, WorkflowRunFinalization, WorkflowRunFinalizationCancellation,
    WorkflowRunId, WorkflowRunOutput, WorkflowRunPresentation, WorkflowRunPresentationResult,
    WorkflowRunPublicationResult, WorkflowRunRecord, WorkflowRunResult, WorkflowRunStep,
    WorkflowRunStepKind, WorkflowRunTerminalResultV1, WorkflowRunTiming, WorkflowRunViewModel,
    WorkflowStepTiming, WorkflowTerminalHost, admit_local_workflow, command_output_v1,
    default_execution_policy_limits, discover_and_validate_claude_code_installation,
    discover_and_validate_codex_installation, discover_and_validate_pi_installation,
    execute_workflow_with_children, is_workflow_tree, normalize_terminal_path,
    prepare_attempt_result_destination, production_agent_dispatcher,
    publish_prepared_workflow_result, resolve_workflow_file_with_agent_profiles,
    step_recovery_summary_v1, summary_disposition_matches, validate_workflow_tree,
    write_untested_harness_advisories,
};
use um_execution::{
    AgentProfileReplacementDocument, AgentProfileReplacementV1, ChildRunPresentation,
    MAXIMUM_AGENT_PROFILE_REPLACEMENT_BYTES,
};
use um_execution::{
    CONTROL_CANCEL, CONTROL_INTERRUPT, CONTROL_STOP, CapturedValue, ChildCommitFailure,
    ChildInputValue, ChildRunControl, ChildRunDispatcher, ChildRunOutcome, ChildRunParent,
    ChildRunRequest, ChildRuns, ControlEvent, ControlsLock, EnteredWait, ExportValue,
    LocalRetryBeginError, LocalRetryOpen, Mirror, RejectionDiagnostic, Scheduling, WaitKind,
    WaitRecord, acquire_local_retry, adopted_child_outcome, append_control, attempt_controls,
    await_child_start_gate, await_release_gate, await_settled_gate, position_path, read_controls,
    read_numbered_controls, stop_in_force, wait_payload,
};
use um_execution::{
    DefinitionError, SubjectClaim, SubjectError, WITHDRAWN_OUTCOME, WorkflowHome, check_subject,
    chosen_outcome, failure_outcome, is_subject, read_definition,
};

mod cloud;

pub(super) const ABOUT: &str = "Start a local workflow run, or with --cloud a Cloud run";
pub(super) const AFTER_HELP: &str = "Interactive mode:
  Automatic mode uses the terminal interface only when stdin and stdout are terminals,
  TERM is usable, and stdin is not reserved by a Text or JSON input. Resize keeps the
  interface active; undersized terminals show a resize notice without changing modes.
  Use Up/Down or j/k to select steps, Enter to inspect logs, ? for complete help,
  Ctrl-C to request cancellation, and q to leave only after publication and cleanup.
  After q, Useful Machinery restores the terminal and prints the standard plain summary.";

const MAXIMUM_INPUTS: usize = 256;
const MAXIMUM_TEXT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_ATTACHMENTS: usize = 256;
const MAXIMUM_ATTACHMENT_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const CANCELLATION_GRACE: Duration = Duration::from_secs(10);
const ROOT_WORKFLOW_FILE: &str = "workflow.yaml";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExecutionLeaf {
    Run,
    Retry,
    Resume,
    /// A successor's first own attempt (REQ-LOCALRUN-194).
    Restart,
    /// A successor carrying its predecessor's wait, which starts no attempt
    /// (REQ-LOCALRUN-197): it names the command on the refusals before the successor exists.
    Upgrade,
}

// `--execution-root` is required only of a local run, and `--cloud` needs `--project`
// (REQ-LOCALRUN-76, REQ-RUN-94).
#[derive(Debug, Args)]
#[command(
    mut_arg("execution_root", |arg| arg
        .required(false)
        .required_unless_present("cloud")
        .conflicts_with("cloud")),
    mut_arg("cloud", |arg| arg.requires("project"))
)]
pub(super) struct Command {
    #[arg(
        value_name = "ID",
        required_unless_present = "path",
        help = "Workflow definition id, as `um workflow definition list` prints it; with --cloud, a Cloud definition id"
    )]
    id: Option<String>,

    #[arg(
        long,
        value_name = "DIR",
        conflicts_with_all = ["id", "cloud"],
        requires = "run_dir",
        help = "Run the tree or workflow source directory at DIR instead of a definition"
    )]
    path: Option<PathBuf>,

    #[arg(
        long,
        value_name = "FILE",
        help = "Workflow file inside the tree, relative to its root; the root workflow when omitted"
    )]
    workflow: Option<PathBuf>,

    #[command(flatten)]
    execution: Option<super::LocalExecutionRoot>,

    #[arg(
        long,
        value_name = "PATH",
        conflicts_with_all = ["id", "cloud"],
        help = "Directory to create for this run (must not already exist)"
    )]
    run_dir: Option<PathBuf>,

    #[command(flatten)]
    inputs: super::super::super::NamedInputArgs,

    #[arg(
        long,
        value_name = "PATH",
        conflicts_with = "cloud",
        help = "Replace declared agent profiles with the closed replacement document at PATH"
    )]
    agent_profiles: Option<PathBuf>,

    #[arg(
        long,
        value_name = "COUNT",
        value_parser = parse_parallelism,
        help = "Maximum simultaneous workflow steps (1 to 256, or 1 to 64 with --cloud; 1 when omitted)"
    )]
    max_parallel: Option<usize>,

    #[arg(
        long,
        value_name = "STEP",
        conflicts_with = "cloud",
        help = "Park the run before STEP until the stop is lifted"
    )]
    stop_before: Option<String>,

    #[arg(
        long,
        value_name = "OWNER",
        value_parser = parse_owner,
        conflicts_with = "cloud",
        help = "Record OWNER as the run's owner in place of the login name"
    )]
    owner: Option<String>,

    #[arg(
        long,
        value_name = "NAME=VALUE",
        value_parser = parse_external,
        conflicts_with = "cloud",
        help = "Record VALUE as the run's external reference NAME (repeatable)"
    )]
    external: Vec<(String, String)>,

    #[command(flatten)]
    presentation: super::PresentationOptions,

    #[command(flatten)]
    cloud: super::CloudSelector,

    #[command(flatten)]
    cloud_creation: cloud::Options,
}

impl Command {
    pub(super) fn execute(mut self) -> super::super::super::CommandResult {
        if let Some(organization) = self.cloud.organization.take() {
            return self.create_in_cloud(organization);
        }
        if let Some(flag) = self.cloud_creation.given() {
            return Err(super::super::super::CommandFailure::with_exit_code(
                anyhow!("{flag} needs --cloud ORGANIZATION"),
                ExitCode::UsageError,
            ));
        }
        execute_with_runtime("start local workflow runtime", self.execute_async())
    }

    async fn execute_async(mut self) -> super::super::super::CommandResult {
        // The argument rules require it of a local run (REQ-LOCALRUN-76).
        let Some(super::LocalExecutionRoot { execution_root }) = self.execution.take() else {
            return Err(super::super::super::CommandFailure::with_exit_code(
                anyhow!("a local run needs --execution-root PATH"),
                ExitCode::UsageError,
            ));
        };
        let source = match (self.id.clone(), self.path.clone(), self.run_dir.clone()) {
            (Some(id), None, None) => RunSubject::Definition(id),
            (None, Some(root), Some(run_directory)) => RunSubject::Path {
                root,
                run_directory,
            },
            // The argument rules admit exactly the two forms (REQ-LOCALRUN-80).
            _ => {
                return Err(super::super::super::CommandFailure::with_exit_code(
                    anyhow!("name a definition id, or --path DIR with --run-dir PATH"),
                    ExitCode::UsageError,
                ));
            }
        };
        let external = external_references(&self.external).map_err(|error| {
            super::super::super::CommandFailure::with_exit_code(error, ExitCode::UsageError)
        })?;
        let relative_workflow = tree_workflow_file(self.workflow.as_deref()).map_err(|error| {
            super::super::super::CommandFailure::with_exit_code(error, ExitCode::UsageError)
        })?;
        let input_plan = self.input_plan().map_err(|error| {
            super::super::super::CommandFailure::with_exit_code(error, ExitCode::UsageError)
        })?;
        let presentation_config = self.presentation_config_with_input_plan(&input_plan);
        let cancellation = CancellationSource::new();
        let mut signal_task = AbortOnDrop(Some(start_signal_observation(
            cancellation.clone(),
            UnixSignals::new()?,
        )));

        let inputs = acquire_inputs(&input_plan, &cancellation)
            .await
            .map_err(|error| signalled_failure(error, cancellation.cancellation_reason()))?;
        let replacement = match &self.agent_profiles {
            Some(path) => Some(acquire_agent_profile_replacement(path, &cancellation)?),
            None => None,
        };
        let (source_root, tree, destination) = match source {
            RunSubject::Path {
                root,
                run_directory,
            } => {
                let tree_root = root.clone();
                match blocking_operation(move || local_workflow_tree(&tree_root)).await {
                    Ok(tree) => (root, tree, RunDestination::Path(run_directory)),
                    Err(BlockingOperationError::Operation(diagnostics)) => {
                        return rejection_output(presentation_config, |output| {
                            output.render_tree_rejection(None, &diagnostics)
                        });
                    }
                    Err(BlockingOperationError::WorkerUnavailable) => {
                        return Err(anyhow!("validate local workflow tree").into());
                    }
                }
            }
            RunSubject::Definition(id) => {
                match blocking_operation(move || definition_tree(&id)).await {
                    Ok(definition) => (
                        definition.copy,
                        Some(definition.tree),
                        RunDestination::Definition {
                            home: definition.home,
                            id: definition.id,
                            revision: definition.revision,
                        },
                    ),
                    Err(BlockingOperationError::Operation(failure)) => {
                        return rejection_exit(failure.render(WorkflowRunOutput::new(
                            presentation_config,
                            io::stdout(),
                            io::stderr(),
                        )));
                    }
                    Err(BlockingOperationError::WorkerUnavailable) => {
                        return Err(anyhow!("read workflow definition").into());
                    }
                }
            }
        };
        let workflow_file = source_root.join(&relative_workflow);
        // A file the tree does not name as a workflow file is refused before it is read.
        let selected = tree_relative_workflow(self.workflow.as_deref());
        if let Some(tree) = &tree
            && !tree.workflow_files.contains(&selected)
        {
            return rejection_output(presentation_config, |output| {
                output.render_workflow_not_in_tree(&selected)
            });
        }
        let tree_digest = tree.as_ref().map(tree_digest);
        let workflow = match blocking_operation(move || {
            resolve_workflow_file_with_agent_profiles(
                &source_root,
                &workflow_file,
                replacement.as_ref(),
            )
        })
        .await
        {
            Ok(workflow) => workflow,
            Err(BlockingOperationError::Operation(failure)) => {
                return rejection_output(presentation_config, |output| {
                    output.render_resolution_rejection(&failure)
                });
            }
            Err(BlockingOperationError::WorkerUnavailable) => {
                return Err(anyhow!("resolve local workflow definition").into());
            }
        };
        if let Some(tree) = &tree
            && !tree.workflow_files.contains(&workflow.source.workflow_path)
        {
            return rejection_output(presentation_config, |output| {
                output.render_workflow_not_in_tree(&workflow.source.workflow_path)
            });
        }
        // REQ-LOCALRUN-80, REQ-LOCALRUN-185: the stop names a step of the selected workflow
        // of a run that records itself, before the run directory exists.
        if let Some(step) = &self.stop_before {
            if !workflow.definition.steps.contains_key(step) {
                return Err(super::super::super::CommandFailure::with_exit_code(
                    anyhow!("--stop-before names no step of the selected workflow"),
                    ExitCode::UsageError,
                ));
            }
            if tree.is_none() {
                return Err(super::super::super::CommandFailure::with_exit_code(
                    anyhow!(
                        "--stop-before needs a run of a tree or a definition, which has a run record"
                    ),
                    ExitCode::UsageError,
                ));
            }
        }
        // REQ-LOCALRUN-209: the owner and external references land in the run record.
        if tree.is_none() && (self.owner.is_some() || !external.is_empty()) {
            return Err(super::super::super::CommandFailure::with_exit_code(
                anyhow!(
                    "--owner and --external need a run of a tree or a definition, which has a run record"
                ),
                ExitCode::UsageError,
            ));
        }
        let workflow_for_context = workflow.clone();
        let maximum_parallel_steps = self.max_parallel.unwrap_or(1);
        let context_cancellation = cancellation.clone();
        let context = match blocking_operation(move || {
            execution_context_for_workflow(
                &workflow_for_context,
                execution_root,
                maximum_parallel_steps,
                context_cancellation,
            )
        })
        .await
        {
            Ok(context) => context,
            Err(BlockingOperationError::Operation(failure)) => {
                return rejection_output(presentation_config, |output| {
                    output.render_agent_harness_installation_rejection(&workflow, &failure)
                });
            }
            Err(BlockingOperationError::WorkerUnavailable) => {
                return Err(anyhow!("prepare local workflow execution context").into());
            }
        };
        let context = match tree {
            Some(tree) => context.with_tree(tree),
            None => context,
        };
        let untested_advisories = context.untested_harness_advisories();
        let workflow_for_admission = workflow.clone();
        let admitted = match blocking_operation(move || {
            admit_local_workflow(workflow_for_admission, inputs, context)
        })
        .await
        {
            Ok(admitted) => admitted,
            Err(BlockingOperationError::Operation(failure)) => {
                return rejection_output(presentation_config, |output| {
                    output.render_admission_rejection(&workflow, &failure)
                });
            }
            Err(BlockingOperationError::WorkerUnavailable) => {
                return Err(anyhow!("admit local workflow").into());
            }
        };

        if workflow.source.source_root.to_str().is_none()
            || admitted.execution().root().to_str().is_none()
        {
            return Err(anyhow!(
                "prepare local workflow paths: an authoritative path is not valid UTF-8"
            )
            .into());
        }
        // REQ-LOCALRUN-173, REQ-LOCALRUN-175: a tree run's subject is the resolved value of
        // the input its file names as `subject`; a value outside the subject form, or a slot
        // another run of the definition holds, is refused before the run directory exists.
        let subject = tree_digest
            .as_ref()
            .and_then(|_| run_subject(&workflow, &admitted));
        if let Some(subject) = &subject
            && let Err(error) = destination.refuse_subject(subject)
        {
            return subject_rejection(presentation_config, destination.definition(), error);
        }
        write_untested_advisories(&untested_advisories);
        // REQ-LOCALRUN-175, REQ-LOCALRUN-209: a run admitted from a tree records itself in
        // `run.json`, owned by `--owner` or the login name.
        let record = tree_digest.map(|digest| {
            let mut record = WorkflowRunRecord::new(
                destination.definition(),
                &digest,
                &workflow.source.workflow_path,
                &self.owner.clone().unwrap_or_else(super::super::login_name),
            );
            record.subject.clone_from(&subject);
            record.external = external;
            record
        });
        let run_directory = destination.run_directory(record.as_ref())?;
        let started_record = record.clone();
        let definition = destination.definition();
        let creation = RunCreation {
            destination,
            subject,
            record,
            run_directory: run_directory.clone(),
            admitted: admitted.clone(),
            stop_before: self.stop_before.clone(),
        };
        let creation_cancellation = cancellation.clone();
        let journals = signal_task
            .0
            .as_ref()
            .map(|observation| observation.journals.clone())
            .unwrap_or_default();
        let created =
            tokio::task::spawn_blocking(move || creation.create(&creation_cancellation, &journals))
                .await
                .map_err(|error| CreationFailure::Other(anyhow::Error::new(error)))
                .and_then(|result| result);
        // REQ-LOCALRUN-135: a signal reads the created run's control journal.
        let (owned_run, _journal) = match created {
            Ok(created) => created,
            Err(CreationFailure::Subject(error)) => {
                return subject_rejection(presentation_config, definition, error);
            }
            Err(CreationFailure::Signalled(failure)) => return Err(failure),
            Err(CreationFailure::Other(error)) => {
                return Err(error
                    .context(format!(
                        "create workflow run {}",
                        normalize_terminal_path(&run_directory)
                    ))
                    .into());
            }
        };
        let admitted = bind_tree_copy(admitted, &owned_run.tree_copy_directory())?;
        // REQ-LOCALRUN-191: the created run's `onStart`, once its record and directory exist;
        // a resumed or retried attempt never writes it.
        if let Some(record) = &started_record {
            let mirror = super::mirror::run_mirror(
                record,
                &workflow.definition,
                admitted.inputs(),
                owned_run.run_directory(),
            )
            .map(Arc::new);
            super::mirror::perform_blocking(
                mirror.as_ref(),
                Box::new(|mirror, target| mirror.start(target)),
            )
            .await;
        }
        execute_owned_attempt(
            workflow,
            admitted,
            owned_run,
            cancellation,
            signal_task
                .0
                .take()
                .ok_or_else(|| anyhow!("local workflow signal observation unavailable"))?,
            presentation_config,
            ExecutionLeaf::Run,
        )
        .await
    }

    fn input_plan(&self) -> anyhow::Result<InputPlan> {
        plan_inputs(
            &self.inputs.input_text,
            &self.inputs.input_text_file,
            &self.inputs.input_json,
            &self.inputs.input_json_file,
            &self.inputs.input_file,
            &self.inputs.input_attachment,
            &self.inputs.input_attachments_empty,
        )
    }

    fn presentation_config_with_input_plan(&self, input_plan: &InputPlan) -> PresentationConfig {
        presentation_config_with(
            &self.presentation,
            input_plan.standard_input_reserved,
            TerminalCapabilities::detect(),
        )
    }

    #[cfg(test)]
    fn presentation_config_with(&self, capabilities: TerminalCapabilities) -> PresentationConfig {
        let standard_input_reserved = self
            .input_plan()
            .is_ok_and(|plan| plan.standard_input_reserved);
        presentation_config_with(&self.presentation, standard_input_reserved, capabilities)
    }
}

/// What `run create` runs: a definition's current revision under the home
/// (REQ-LOCALRUN-173) or a directory with no definition (REQ-LOCALRUN-174).
enum RunSubject {
    Definition(String),
    Path {
        root: PathBuf,
        run_directory: PathBuf,
    },
}

/// Where the run directory goes: `--run-dir`, or `workflows/<id>/runs/<run id>/` under the
/// home for a definition run.
enum RunDestination {
    Path(PathBuf),
    Definition {
        home: WorkflowHome,
        id: String,
        revision: u64,
    },
}

impl RunDestination {
    /// The definition and revision the run record names, none for a `--path` run.
    fn definition(&self) -> Option<(String, u64)> {
        match self {
            Self::Path(_) => None,
            Self::Definition { id, revision, .. } => Some((id.clone(), *revision)),
        }
    }

    fn run_directory(&self, record: Option<&WorkflowRunRecord>) -> anyhow::Result<PathBuf> {
        match (self, record) {
            (Self::Path(run_directory), _) => Ok(run_directory.clone()),
            (Self::Definition { home, id, .. }, Some(record)) => {
                Ok(home.runs_directory(id).join(&record.id))
            }
            (Self::Definition { .. }, None) => Err(anyhow!(
                "create workflow run: a definition run has no run record"
            )),
        }
    }

    /// Refuses `subject` when it is outside the subject form or, for a definition run, when
    /// the definition's slot for it is held (REQ-WFDEF-28), read without the index's lock.
    fn refuse_subject(&self, subject: &str) -> Result<(), SubjectError> {
        match self {
            Self::Path(_) if is_subject(subject) => Ok(()),
            Self::Path(_) => Err(SubjectError::Invalid),
            Self::Definition { home, id, .. } => check_subject(home, id, subject, None),
        }
    }

    /// Takes the definition's claim on the slot for `subject`, holding the index's lock until
    /// the claim drops and writing nothing until the claim names a run; a `--path` run takes
    /// no claim (REQ-LOCALRUN-173).
    fn take_subject_slot(&self, subject: &str) -> Result<Option<SubjectClaim>, SubjectError> {
        match self {
            Self::Path(_) => Ok(None),
            Self::Definition { home, id, .. } => {
                SubjectClaim::take(home, id, subject, None).map(Some)
            }
        }
    }

    /// Creates the definition's `runs/` before its run directory is created there.
    fn prepare(&self) -> anyhow::Result<()> {
        match self {
            Self::Path(_) => Ok(()),
            Self::Definition { home, id, .. } => home
                .create_runs_directory(id)
                .map(drop)
                .map_err(anyhow::Error::new),
        }
    }
}

/// The creation of an admitted run: its directory at `run_directory` under `destination`,
/// its record and subject slot, and the stop `--stop-before` names.
struct RunCreation {
    destination: RunDestination,
    subject: Option<String>,
    record: Option<WorkflowRunRecord>,
    run_directory: PathBuf,
    admitted: AdmittedWorkflow,
    stop_before: Option<String>,
}

impl RunCreation {
    /// Creates the run directory, holding the subject's claim under the index's lock and
    /// writing the slot only after the directory exists, and returns the run with its
    /// control journal watched in `journals`. A signal `cancellation` holds before the claim
    /// is taken or the directory is created refuses the creation, so no run exists and no
    /// slot is written. `journals` watches the run directory from before the first of those
    /// checks, so a signal that arrives after the last one, while the directory is made, is
    /// an interruption: the run it could not refuse never ends `canceled` (REQ-LOCALRUN-135).
    fn create(
        self,
        cancellation: &CancellationSource,
        journals: &ControlJournals,
    ) -> Result<(InitialLocalRun, WatchedJournal), CreationFailure> {
        let Self {
            destination,
            subject,
            record,
            run_directory,
            admitted,
            stop_before,
        } = self;
        // The directory does not exist yet, so this journal holds no control and makes a
        // signal an interruption; the created run's journal replaces it below.
        let creating = journals.watch(&run_directory);
        let refuse_when_signalled = || match cancellation.cancellation_reason() {
            None => Ok(()),
            reason => Err(CreationFailure::Signalled(signalled_before_creation(
                reason,
            ))),
        };
        refuse_when_signalled()?;
        destination.prepare().map_err(CreationFailure::Other)?;
        // REQ-WFDEF-28: the claim is taken under the index's lock, held until the run
        // directory exists and the slot names it, so of two creates for one subject one is
        // refused.
        let claim = match (&subject, &record) {
            (Some(subject), Some(_)) => destination
                .take_subject_slot(subject)
                .map_err(CreationFailure::Subject)?,
            _ => None,
        };
        // A refusal here drops the claim before it names a run, so no slot is written
        // (REQ-LOCALRUN-135, REQ-WFDEF-28).
        refuse_when_signalled()?;
        let owned_run =
            InitialLocalRun::create_with_record(&run_directory, &admitted, record.as_ref())
                .map_err(|error| CreationFailure::Other(anyhow::Error::new(error)))?;
        // The slot names the run only now that its directory exists.
        if let (Some(claim), Some(record)) = (&claim, &record) {
            claim.name(&record.id).map_err(|error| {
                CreationFailure::Other(
                    anyhow::Error::new(error)
                        .context(format!("name run {} in the subject's slot", record.id)),
                )
            })?;
        }
        drop(claim);
        // REQ-LOCALRUN-185: the stop is recorded before the first step starts.
        if let (Some(step), Some(record)) = (stop_before, &record) {
            let payload = serde_json::Map::from_iter([(
                "before".to_owned(),
                serde_json::Value::String(step),
            )]);
            let stop = ControlEvent::new(
                CONTROL_STOP,
                &record.id,
                &super::super::login_name(),
                None,
                Some(payload),
            )
            .map_err(|error| CreationFailure::Other(error.into()))?;
            append_control(owned_run.run_directory(), &stop)
                .map_err(|error| CreationFailure::Other(error.into()))?;
        }
        // A `control.cancel` appended from here on is new to this journal and cancels.
        let journal = journals.watch(owned_run.run_directory());
        drop(creating);
        Ok((owned_run, journal))
    }
}

/// The failure of a command a signal reached before the directory of the run it would
/// create existed, `create`'s run or a successor: no run is created (REQ-LOCALRUN-135).
pub(super) fn signalled_before_creation(
    reason: Option<CancellationReason>,
) -> super::super::super::CommandFailure {
    signalled_failure(
        anyhow!("workflow run not created: a signal arrived before its run directory existed"),
        reason,
    )
}

/// The failure of a `resume`, `retry` or `restart` a signal reached before its attempt began:
/// no attempt began and the run is as it was (REQ-LOCALRUN-135).
pub(super) fn signalled_before_attempt(
    reason: Option<CancellationReason>,
) -> super::super::super::CommandFailure {
    signalled_failure(
        anyhow!("workflow run attempt not begun: a signal arrived before the attempt began"),
        reason,
    )
}

/// The failure of a command a signal stopped before its run's attempt, by the signal's
/// `reason` (REQ-WFEXEC-40): 130 for `user_request`, 143 for `termination_request`, and a
/// general failure for no signal.
fn signalled_failure(
    error: anyhow::Error,
    reason: Option<CancellationReason>,
) -> super::super::super::CommandFailure {
    match reason {
        Some(CancellationReason::UserRequest) => {
            super::super::super::CommandFailure::for_outcome(error, OutcomeClass::Interrupted)
        }
        Some(CancellationReason::TerminationRequest) => {
            super::super::super::CommandFailure::for_outcome(error, OutcomeClass::Terminated)
        }
        _ => error.into(),
    }
}

/// Why the run directory of an admitted run was not created.
enum CreationFailure {
    /// The subject's slot was refused under the index's lock (REQ-WFDEF-28).
    Subject(SubjectError),
    /// A signal arrived before the run directory existed (REQ-LOCALRUN-135).
    Signalled(super::super::super::CommandFailure),
    Other(anyhow::Error),
}

/// The resolved value of the text input the workflow file names as its `subject`
/// (REQ-WFFORMAT-100), or `None` when it names none or the input has no value.
fn run_subject(workflow: &ResolvedWorkflow, admitted: &AdmittedWorkflow) -> Option<String> {
    let name = workflow.definition.subject.as_deref()?;
    match admitted.inputs().get(name)? {
        ResolvedInput::Text(value) => Some(value.to_string()),
        _ => None,
    }
}

/// A refusal of the run's subject at phase `definition` (REQ-LOCALRUN-173); a failure to
/// read or write the index is a command failure.
fn subject_rejection(
    config: PresentationConfig,
    definition: Option<(String, u64)>,
    error: SubjectError,
) -> super::super::super::CommandResult {
    let Some(code) = error.code() else {
        return Err(anyhow::Error::new(error).into());
    };
    let message = error.to_string();
    let (definition, revision) = definition.unzip();
    rejection_output(config, |output| {
        output.render_subject_rejection(
            code,
            &message,
            definition.as_deref(),
            revision,
            error.run(),
        )
    })
}

/// The current revision's tree of a definition, read from the home and checked against the
/// revision's digest.
pub(super) struct DefinitionTree {
    pub(super) home: WorkflowHome,
    pub(super) id: String,
    pub(super) revision: u64,
    pub(super) copy: PathBuf,
    pub(super) tree: ValidatedTree,
}

pub(super) enum DefinitionTreeFailure {
    /// Refused at phase `definition`.
    Definition(DefinitionError),
    /// The copy is not a valid tree, reported at phase `tree` as for a `--path` tree.
    Tree(Vec<TreeDiagnostic>),
}

impl DefinitionTreeFailure {
    /// The refusal at phase `definition` or `tree` (REQ-LOCALRUN-173).
    pub(super) fn render(
        &self,
        output: WorkflowRunOutput<io::Stdout, io::Stderr>,
    ) -> WorkflowRunPresentationResult {
        match self {
            Self::Definition(error) => output.render_definition_rejection(error),
            Self::Tree(diagnostics) => output.render_tree_rejection(None, diagnostics),
        }
    }
}

impl From<DefinitionError> for DefinitionTreeFailure {
    fn from(error: DefinitionError) -> Self {
        Self::Definition(error)
    }
}

/// Reads definition `id` and validates the home's copy of its current revision's tree,
/// enabled or not (REQ-LOCALRUN-173).
pub(super) fn definition_tree(id: &str) -> Result<DefinitionTree, DefinitionTreeFailure> {
    let home = WorkflowHome::from_environment()?;
    let definition = read_definition(&home, id)?;
    // `read_definition` refuses a record whose `current` names no revision.
    let Some(revision) = definition.record.current_revision() else {
        return Err(DefinitionError::RecordInvalid {
            path: definition.path.clone(),
            reason: "names no current revision",
            source: None,
        }
        .into());
    };
    let copy = home.retained_tree(id, revision)?;
    let tree = validate_workflow_tree(&copy).map_err(DefinitionTreeFailure::Tree)?;
    if tree_digest(&tree) != revision.tree {
        return Err(DefinitionError::TreeRetainedModified {
            id: id.to_owned(),
            revision: revision.n,
            path: copy,
        }
        .into());
    }
    Ok(DefinitionTree {
        id: id.to_owned(),
        revision: revision.n,
        home,
        copy,
        tree,
    })
}

/// A tree's digest as a run record and a definition revision name it, `<algorithm>:<hex>`.
pub(super) fn tree_digest(tree: &ValidatedTree) -> String {
    format!("{}:{}", tree.digest.algorithm.as_str(), tree.digest.value)
}

/// The workflow file a run executes, relative to the tree root or `--path` directory:
/// `--workflow`, or `workflow.yaml`. An absolute or `..`-bearing `--workflow` is refused
/// before any file is read (REQ-LOCALRUN-80).
fn tree_workflow_file(workflow: Option<&Path>) -> anyhow::Result<PathBuf> {
    let Some(workflow) = workflow else {
        return Ok(PathBuf::from(ROOT_WORKFLOW_FILE));
    };
    if workflow.as_os_str().is_empty()
        || workflow
            .components()
            .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
    {
        return Err(anyhow!(
            "--workflow {}: name a workflow file relative to the tree root, without '..'",
            normalize_terminal_path(workflow)
        ));
    }
    Ok(workflow.to_owned())
}

/// The tree-relative spelling of an admitted `--workflow`, `workflow.yaml` when omitted.
fn tree_relative_workflow(workflow: Option<&Path>) -> String {
    workflow.map_or_else(
        || ROOT_WORKFLOW_FILE.to_owned(),
        |workflow| {
            workflow
                .components()
                .filter_map(|component| match component {
                    Component::Normal(name) => Some(name.to_string_lossy()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("/")
        },
    )
}

pub(super) const TREE_COPY_PATH_FAILURE: &str = "prepare local workflow paths: the run directory of a tree run cannot contain ':', since its tree copy's programs lead PATH";

/// A source root with `workflow.yaml` directly under it is a tree, validated before the
/// selected workflow file resolves (REQ-LOCALRUN-169); any other source root is not.
fn local_workflow_tree(source_root: &Path) -> Result<Option<ValidatedTree>, Vec<TreeDiagnostic>> {
    if !is_workflow_tree(source_root) {
        return Ok(None);
    }
    validate_workflow_tree(source_root).map(Some)
}

/// Binds the run's tree copy (REQ-WFEXEC-53); a workflow admitted without a tree is
/// returned unchanged.
pub(super) fn bind_tree_copy(
    admitted: AdmittedWorkflow,
    copy_root: &Path,
) -> anyhow::Result<AdmittedWorkflow> {
    admitted
        .with_tree_copy(copy_root)
        .context(TREE_COPY_PATH_FAILURE)
}

pub(super) enum BlockingOperationError<Error> {
    Operation(Error),
    WorkerUnavailable,
}

pub(super) async fn blocking_operation<Value, Error, Operation>(
    operation: Operation,
) -> Result<Value, BlockingOperationError<Error>>
where
    Value: Send + 'static,
    Error: Send + 'static,
    Operation: FnOnce() -> Result<Value, Error> + Send + 'static,
{
    match tokio::task::spawn_blocking(operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(BlockingOperationError::Operation(error)),
        Err(_) => Err(BlockingOperationError::WorkerUnavailable),
    }
}

pub(super) fn execute_with_runtime(
    failure_context: &str,
    execution: impl Future<Output = super::super::super::CommandResult>,
) -> super::super::super::CommandResult {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .with_context(|| failure_context.to_owned())?;
    runtime.block_on(execution)
}

pub(super) fn presentation_config(presentation: &super::PresentationOptions) -> PresentationConfig {
    presentation_config_with(presentation, false, TerminalCapabilities::detect())
}

pub(super) fn presentation_config_with(
    presentation: &super::PresentationOptions,
    standard_input_reserved: bool,
    capabilities: TerminalCapabilities,
) -> PresentationConfig {
    PresentationConfig {
        requested_mode: if presentation.output.json {
            RequestedPresentationMode::Json
        } else if presentation.plain {
            RequestedPresentationMode::Plain
        } else {
            RequestedPresentationMode::Automatic
        },
        color: match presentation.color {
            super::ColorArgument::Auto => ColorChoice::Auto,
            super::ColorArgument::Always => ColorChoice::Always,
            super::ColorArgument::Never => ColorChoice::Never,
        },
        capabilities,
        standard_input_reserved,
    }
}

struct PreparedExecutionPresentation<Observer, Host> {
    observer: Observer,
    host: Host,
    timing: RunTimingObservation,
}

fn initialize_execution_presentation<Observer, Host, Clock>(
    clock: Clock,
    initialize: impl FnOnce() -> Result<
        PreparedExecutionPresentation<Observer, Host>,
        PresentationFailure,
    >,
) -> Result<PreparedExecutionPresentation<Observer, Host>, PresentationFailure>
where
    Clock: ObservationClock,
{
    let prepared = initialize()?;
    prepared.timing.mark_execution_started(clock.sample());
    Ok(prepared)
}

struct AbortOnDrop(Option<SignalObservation>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(observation) = self.0.take() {
            observation.abort();
        }
    }
}

struct AttemptTeardown {
    run: Option<LocalAttemptOwner>,
    /// The run's controls lock a failed attempt that ends the run takes for its spend
    /// (REQ-LOCALRUN-211), declared after `run` so it is released after the run lock.
    controls: Option<ControlsLock>,
    /// The run's controls lock an attempt that ended `continued` and chains took before it
    /// released the run lock, which the attempt loop carries until the next attempt's
    /// acquisition tried the run lock (REQ-LOCALRUN-214, INV-ENGINE-39).
    bridge: Option<ControlsLock>,
    /// The outcome the run record takes when the attempt ends without a published result
    /// (REQ-WFEXEC-57).
    failure_outcome: String,
    signal_task: Option<SignalObservation>,
    private_staging: Option<um_execution::AttemptPrivateStaging>,
    artifacts: Option<ArtifactStaging>,
    inputs: Option<InputStaging>,
    agents: Option<AgentInputStaging>,
    host: Option<ActiveRunHost>,
    execution_started: bool,
    armed: bool,
    /// The edge the attempt ended `continued` on, which the attempt loop continues from
    /// (REQ-WFEXEC-64).
    continued: Option<um_execution::TakenEdgeRecord>,
    /// The root attempt's mirror (REQ-LOCALRUN-191), which writes `onOutcome` for the
    /// failure outcome too.
    mirror: Option<Arc<Mirror>>,
}

impl AttemptTeardown {
    fn new(
        run: LocalAttemptOwner,
        failure_outcome: &str,
        signal_task: Option<SignalObservation>,
    ) -> Self {
        Self {
            run: Some(run),
            controls: None,
            bridge: None,
            failure_outcome: failure_outcome.to_owned(),
            signal_task,
            private_staging: None,
            artifacts: None,
            inputs: None,
            agents: None,
            host: None,
            execution_started: false,
            armed: true,
            continued: None,
            mirror: None,
        }
    }

    fn run(&self) -> anyhow::Result<&LocalAttemptOwner> {
        self.run
            .as_ref()
            .ok_or_else(|| anyhow!("local attempt ownership unavailable"))
    }

    fn host(&mut self) -> anyhow::Result<&mut ActiveRunHost> {
        self.host
            .as_mut()
            .ok_or_else(|| anyhow!("local attempt presentation unavailable"))
    }

    fn disarm(&mut self) {
        if let Some(signal_task) = self.signal_task.take() {
            signal_task.abort();
        }
        self.armed = false;
    }

    async fn teardown(&mut self) {
        if let Some(signal_task) = self.signal_task.take() {
            signal_task.abort();
        }
        if !self.execution_started
            && let Some(run) = self.run.as_ref()
        {
            settle_before_execution_failure(run).await;
        }
        let inputs = self.inputs.take();
        let agents = self.agents.take();
        let artifacts = self.artifacts.take();
        let private_staging = self.private_staging.take();
        let cleanup = blocking_operation(move || {
            let staging_failed = match (inputs.as_ref(), artifacts.as_ref()) {
                (Some(inputs), Some(artifacts)) => {
                    release_execution_staging(inputs, agents.as_ref(), artifacts)
                }
                (None, Some(artifacts)) => artifacts.release().is_err(),
                (Some(inputs), None) => inputs.release().is_err(),
                (None, None) => false,
            };
            let agent_failed = if inputs.is_none() || artifacts.is_none() {
                agents
                    .as_ref()
                    .is_some_and(|agents| agents.release().is_err())
            } else {
                false
            };
            Ok::<_, std::convert::Infallible>(
                staging_failed
                    | agent_failed
                    | private_staging.is_some_and(|staging| staging.release().is_err()),
            )
        })
        .await;
        if let Some(run) = self.run.take() {
            let cleanup_failed = !matches!(cleanup, Ok(false));
            let outcome = self.failure_outcome.clone();
            let mirror = self.mirror.take();
            if let Ok((run, controls)) = blocking_operation(move || {
                record_private_cleanup_failure(&run, cleanup_failed);
                // REQ-LOCALRUN-206: an attempt left unsettled leaves the record `running`,
                // the abandoned attempt a continuation settles, and performs no `onOutcome`.
                if !run.attempt_settled() {
                    return Ok((run, None));
                }
                // REQ-LOCALRUN-211: the attempt ends the run, so it spends every withdrawal
                // recorded before it under the controls lock, kept until the run lock is
                // released.
                let controls = run.lock_controls().ok();
                let _ = run.spend_withdrawals();
                let exited =
                    run.update_run_record(RunState::Exited, run.attempt_number(), Some(&outcome));
                // REQ-LOCALRUN-191: the failure outcome's `onOutcome`, once the record reads it.
                if let (Ok(()), Some(mirror)) = (exited, mirror) {
                    super::mirror::perform(
                        &mirror,
                        Box::new(move |mirror, target| mirror.outcome(target, &outcome)),
                    );
                }
                Ok::<_, std::convert::Infallible>((run, controls))
            })
            .await
            {
                self.run = Some(run);
                self.controls = controls;
            }
        }
        if let Some(host) = self.host.as_mut() {
            host.stop_terminal().await;
        }
        self.disarm();
    }
}

impl Drop for AttemptTeardown {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // A cancelled attempt cannot await its cleanup. Transfer the owned resources to
        // a task; the same teardown ordering applies as for an ordinary phase failure.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let mut pending = Self {
                run: self.run.take(),
                controls: self.controls.take(),
                bridge: None,
                failure_outcome: std::mem::take(&mut self.failure_outcome),
                signal_task: self.signal_task.take(),
                private_staging: self.private_staging.take(),
                artifacts: self.artifacts.take(),
                inputs: self.inputs.take(),
                agents: self.agents.take(),
                host: self.host.take(),
                execution_started: self.execution_started,
                armed: false,
                continued: None,
                mirror: self.mirror.take(),
            };
            runtime.spawn(async move { pending.teardown().await });
        } else if let Some(signal_task) = self.signal_task.take() {
            signal_task.abort();
            // The staging owners' Drop implementations are the last-resort cleanup
            // when no executor remains to drive an asynchronous terminal shutdown.
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AttemptCheckpoint {
    ArtifactsStaged,
    InputsStaged,
    BeforeHostStart,
    AfterHostStart,
}

struct AttemptSettings<Hooks> {
    presentation_config: PresentationConfig,
    leaf: ExecutionLeaf,
    hooks: Hooks,
    /// A child run's attempt (REQ-WFEXEC-59), with the presentation it has in its parent's
    /// progress stream (REQ-LOCALRUN-200): no presentation and no child runs of its own.
    child: Option<ChildPresentation>,
    /// The child run an abandoned parent's continuation adopts or retries in place
    /// (REQ-CONTINUE-76), or the child an independent repair completed that a repair retry
    /// adopts (REQ-CONTINUE-75).
    child_recovery: Option<ChildRecovery>,
    /// The control journals the command's signal observer reads, which the attempt's run
    /// directory and its children's join while they run (REQ-LOCALRUN-135).
    journals: ControlJournals,
}

trait AttemptHooks {
    fn checkpoint(&self, point: AttemptCheckpoint) -> anyhow::Result<()>;
    fn start_terminal(
        &self,
        view: WorkflowRunViewModel<SystemObservationClock>,
        cancellation: CancellationSource,
        color: bool,
    ) -> Result<WorkflowTerminalHost, PresentationFailure>;
    fn execution_failure(&mut self) -> Option<CoordinationError>;
}

impl<Check, Start, Fail> AttemptHooks for (Check, Start, Fail)
where
    Check: Fn(AttemptCheckpoint) -> anyhow::Result<()>,
    Start: Fn(
        WorkflowRunViewModel<SystemObservationClock>,
        CancellationSource,
        bool,
    ) -> Result<WorkflowTerminalHost, PresentationFailure>,
    Fail: FnMut() -> Option<CoordinationError>,
{
    fn checkpoint(&self, point: AttemptCheckpoint) -> anyhow::Result<()> {
        self.0(point)
    }

    fn start_terminal(
        &self,
        view: WorkflowRunViewModel<SystemObservationClock>,
        cancellation: CancellationSource,
        color: bool,
    ) -> Result<WorkflowTerminalHost, PresentationFailure> {
        self.1(view, cancellation, color)
    }

    fn execution_failure(&mut self) -> Option<CoordinationError> {
        self.2()
    }
}

pub(super) async fn execute_owned_attempt(
    workflow: ResolvedWorkflow,
    admitted: AdmittedWorkflow,
    owned_run: LocalAttemptOwner,
    cancellation: CancellationSource,
    signal_task: SignalObservation,
    presentation_config: PresentationConfig,
    leaf: ExecutionLeaf,
) -> super::super::super::CommandResult {
    execute_recovering_attempt(
        workflow,
        admitted,
        owned_run,
        cancellation,
        signal_task,
        (presentation_config, leaf),
        None,
    )
    .await
}

/// `execute_owned_attempt` for a continuation whose `child_recovery` names the child run
/// the abandoned attempt left behind (REQ-CONTINUE-76) or the child an independent repair
/// completed, which a repair retry adopts (REQ-CONTINUE-75).
pub(super) async fn execute_recovering_attempt(
    workflow: ResolvedWorkflow,
    admitted: AdmittedWorkflow,
    owned_run: LocalAttemptOwner,
    cancellation: CancellationSource,
    signal_task: SignalObservation,
    (presentation_config, leaf): (PresentationConfig, ExecutionLeaf),
    child_recovery: Option<ChildRecovery>,
) -> super::super::super::CommandResult {
    let journals = signal_task.journals.clone();
    execute_owned_attempt_with(
        workflow,
        admitted,
        owned_run,
        cancellation,
        signal_task,
        AttemptSettings {
            presentation_config,
            leaf,
            hooks: (|_| Ok(()), WorkflowTerminalHost::start, || None),
            child: None,
            child_recovery,
            journals,
        },
    )
    .await
}

/// Executes the owned attempt and, when it ends `continued`, continues the run from the edge's
/// target once its lock is released, so `create`, `retry` and `resume` all chain the attempts
/// a backward or limited edge starts (REQ-WFEXEC-64); the last attempt's result is the
/// command's.
async fn execute_owned_attempt_with(
    workflow: ResolvedWorkflow,
    admitted: AdmittedWorkflow,
    owned_run: LocalAttemptOwner,
    cancellation: CancellationSource,
    signal_task: SignalObservation,
    settings: AttemptSettings<impl AttemptHooks>,
) -> super::super::super::CommandResult {
    let run_directory = owned_run.run_directory().to_owned();
    let (config, leaf) = (settings.presentation_config.clone(), settings.leaf);
    let mut attempt = AttemptTeardown::new(
        owned_run,
        failure_outcome(&workflow.definition),
        Some(signal_task),
    );
    let result =
        execute_attempt_phases(&workflow, &admitted, &cancellation, &mut attempt, settings).await;
    // A terminal task can fail again while stopping after a readiness failure.
    // Report its diagnostic after teardown restores the terminal, rather than
    // losing it behind the earlier generic readiness error.
    let readiness_failure = !attempt.execution_started
        && result
            .as_ref()
            .err()
            .is_some_and(|error| error.downcast_ref::<PresentationFailure>().is_some());
    if result.is_err() {
        attempt.teardown().await;
        if readiness_failure
            && let Some(failure) = attempt
                .host
                .as_ref()
                .and_then(ActiveRunHost::terminal_failure)
        {
            return diagnose(failure);
        }
    }
    let result = result.unwrap_or_else(|error| Err(error.into()));
    match (attempt.continued.take(), &result) {
        // REQ-LOCALRUN-135: an interruption holds back the next attempt, leaving the run
        // `running` for `um workflow run resume` (REQ-CONTINUE-78).
        (Some(edge), Ok(ExitCode::Success))
            if !admitted
                .execution()
                .cancellation()
                .source()
                .interruption_requested() =>
        {
            let bridge = attempt.bridge.take();
            let signal_task = start_signal_observation(cancellation.clone(), UnixSignals::new()?);
            Box::pin(super::resume::continue_run(
                super::resume::Continuation {
                    run_directory,
                    from_steps: Vec::new(),
                    workflow: None,
                    execution_root: None,
                    leaf,
                    edge: Some(edge.into()),
                    bridge,
                },
                config,
                cancellation,
                signal_task,
            ))
            .await
        }
        _ => result,
    }
}

/// REQ-LOCALRUN-143, INV-ENGINE-33: a failed state commit as the command reports it. Its
/// diagnostic line reads the commit port's own error, which names the write, the file and
/// the operating system's message, or, when the port kept none, the failure's write and
/// error number; the class follows as a field and the remedy that resumes the run is last.
/// It has no source, so the shared renderer prints nothing after the remedy.
#[derive(Debug)]
struct StateCommitFailed {
    failure: CommitFailure,
    text: Option<String>,
    run_directory: PathBuf,
}

impl std::fmt::Display for StateCommitFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("record run state")?;
        if let Some(text) = &self.text {
            write!(formatter, ": {text}")?;
        } else {
            if let Some(operation) = self.failure.operation {
                write!(formatter, ": {operation}")?;
            }
            if let Some(os_error) = self.failure.os_error {
                write!(formatter, ": {}", io::Error::from_raw_os_error(os_error))?;
            }
        }
        write!(
            formatter,
            "\nclass: {}\n\nThe run is still running, and this attempt is not settled. \
             Correct the cause, then continue the run:\n  um workflow run resume {}",
            self.failure.class.as_str(),
            normalize_terminal_path(&self.run_directory),
        )
    }
}

impl Error for StateCommitFailed {}

async fn execute_attempt_phases(
    workflow: &ResolvedWorkflow,
    admitted: &AdmittedWorkflow,
    cancellation: &CancellationSource,
    attempt: &mut AttemptTeardown,
    settings: AttemptSettings<impl AttemptHooks>,
) -> anyhow::Result<super::super::super::CommandResult> {
    let AttemptSettings {
        presentation_config,
        leaf,
        mut hooks,
        child,
        child_recovery,
        journals,
    } = settings;
    let _journal = journals.watch(attempt.run()?.run_directory());
    let quiet = child.is_some();
    let run_directory = attempt
        .run()?
        .run_directory()
        .to_str()
        .ok_or_else(|| {
            anyhow!("prepare local workflow paths: an authoritative path is not valid UTF-8")
        })?
        .to_owned();
    // REQ-LOCALRUN-175: the run record names this attempt while it is live. A created
    // run's record already does; an attempt it chained into does not (REQ-WFEXEC-64).
    let attempt_number = attempt.run()?.attempt_number();
    if leaf != ExecutionLeaf::Run || attempt_number > 1 {
        let owned_run = attempt
            .run
            .take()
            .ok_or_else(|| anyhow!("local attempt ownership unavailable"))?;
        let (owned_run, recorded) = match blocking_operation(move || {
            let recorded = owned_run.update_run_record(RunState::Running, attempt_number, None);
            Ok::<_, std::convert::Infallible>((owned_run, recorded))
        })
        .await
        {
            Ok(recorded) => recorded,
            Err(BlockingOperationError::Operation(never)) => match never {},
            Err(BlockingOperationError::WorkerUnavailable) => {
                return Err(anyhow!("record the live local workflow attempt"));
            }
        };
        attempt.run = Some(owned_run);
        recorded.context("record the live local workflow attempt")?;
    }
    // REQ-LOCALRUN-191, INV-ENGINE-25: a root attempt's mirror; a child attempt mirrors nothing.
    if !quiet {
        let run = attempt.run()?;
        attempt.mirror = run
            .child_run_parent()
            .ok()
            .and_then(|parent| {
                super::mirror::run_mirror(
                    parent.record(),
                    &workflow.definition,
                    admitted.inputs(),
                    run.run_directory(),
                )
            })
            .map(Arc::new);
    }
    let result_directory = attempt.run()?.result_directory().to_owned();
    let private_directory = attempt.run()?.private_directory().to_owned();
    let result_parent = rustix::io::dup(attempt.run()?.attempt_directory_handle())?;
    let staging_parent = rustix::io::dup(attempt.run()?.private_directory_handle())?;
    let destination = tokio::task::spawn_blocking(move || {
        prepare_attempt_result_destination(
            &result_directory,
            &private_directory,
            &result_parent,
            &staging_parent,
        )
    })
    .await??;
    attempt.private_staging = Some(attempt.run()?.create_private_staging()?);
    let staging = attempt
        .private_staging
        .as_ref()
        .ok_or_else(|| anyhow!("prepare private local workflow staging"))?;
    let artifacts =
        ArtifactStaging::create_bound(admitted.execution(), staging.path(), staging.root_handle())?;
    attempt.artifacts = Some(artifacts.clone());
    hooks.checkpoint(AttemptCheckpoint::ArtifactsStaged)?;
    let inputs =
        InputStaging::create_bound(admitted.execution(), staging.path(), staging.root_handle())?;
    attempt.inputs = Some(inputs.clone());
    hooks.checkpoint(AttemptCheckpoint::InputsStaged)?;
    let agent_staging = if !admitted.requires_agent_runtime() {
        None
    } else {
        Some(
            AgentInputStaging::create(admitted.execution(), staging.path())
                .map_err(anyhow::Error::new)
                .context("prepare private local agent staging")?,
        )
    };
    attempt.agents = agent_staging.clone();
    hooks.checkpoint(AttemptCheckpoint::BeforeHostStart)?;

    let run_clock = SystemObservationClock;
    let run_for_output = attempt.run()?;
    let prepared = initialize_execution_presentation(run_clock, || {
        match (child, presentation_config.mode()) {
            (Some(child), _) => {
                let timing_observation = RunTimingObservation::new(run_clock.sample());
                Ok(PreparedExecutionPresentation {
                    observer: RunExecutionObserver::Quiet(TimingObserver::new(
                        child,
                        cancellation.clone(),
                        timing_observation.clone(),
                        run_clock,
                    )),
                    host: ActiveRunHost::Quiet(QuietRunHost::default()),
                    timing: timing_observation,
                })
            }
            (None, PresentationMode::Tui) => {
                let presentation_opened = run_clock.sample();
                let timing_observation = RunTimingObservation::new(presentation_opened);
                let view = WorkflowRunViewModel::new(
                    workflow,
                    admitted.execution().limits().maximum_parallel_steps().get(),
                    timing_observation.clone(),
                    run_clock,
                );
                let terminal = hooks.start_terminal(
                    view.clone(),
                    cancellation.clone(),
                    presentation_config.color_enabled(),
                )?;
                Ok(PreparedExecutionPresentation {
                    observer: RunExecutionObserver::Tui(view.clone()),
                    host: ActiveRunHost::Tui {
                        view,
                        terminal: Some(terminal),
                        failure: None,
                        config: Box::new(presentation_config),
                        leaf,
                    },
                    timing: timing_observation,
                })
            }
            (None, PresentationMode::Plain | PresentationMode::Json) => {
                let output = execution_output(presentation_config, run_for_output, leaf);
                let presentation = output.start_for_result(
                    workflow,
                    &run_directory,
                    admitted.execution().limits().maximum_parallel_steps().get(),
                    run_clock,
                )?;
                let timing_observation = RunTimingObservation::new(presentation.opened_at());
                let timing = TimingObserver::new(
                    presentation.clone(),
                    cancellation.clone(),
                    timing_observation.clone(),
                    run_clock,
                );
                Ok(PreparedExecutionPresentation {
                    observer: RunExecutionObserver::Standard(timing),
                    host: ActiveRunHost::Standard(presentation),
                    timing: timing_observation,
                })
            }
        }
    })?;
    let observer = prepared.observer;
    attempt.host = Some(prepared.host);
    attempt.host()?.await_ready().await?;
    attempt.host()?.activate_execution()?;
    hooks.checkpoint(AttemptCheckpoint::AfterHostStart)?;

    if let Some(ActiveRunHost::Standard(presentation)) = attempt.host.as_ref() {
        let mut failures = presentation.subscribe_failures();
        let output_cancellation = cancellation.clone();
        tokio::spawn(async move {
            loop {
                if failures.borrow_and_update().is_some() {
                    output_cancellation
                        .request_cancellation(CancellationReason::CallerOutputFailure);
                    break;
                }
                if failures.changed().await.is_err() {
                    break;
                }
            }
        });
    }
    let agent_diagnostic_sessions = if agent_staging.is_some() {
        Some(attempt.run()?.create_agent_diagnostic_sessions()?)
    } else {
        None
    };
    let diagnostics = StepDiagnosticLog::default();
    let accounting = InvocationAccountingLog::default();
    let agents = match (agent_staging.as_ref(), agent_diagnostic_sessions) {
        (Some(staging), Some(diagnostic_sessions)) => {
            let dispatcher = production_agent_dispatcher(
                diagnostics.clone(),
                admitted.execution().limits().maximum_step_log_bytes(),
                SystemExecutionClock,
                observer.clone(),
                crate::build_info::VERSION,
            )
            .map_err(|error| anyhow!("prepare local agent runtimes: {error:?}"))?;
            AgentExecution::enabled_with_accounting(
                WorkflowRunId::from(Arc::from(run_directory.as_str())),
                staging.clone(),
                diagnostic_sessions,
                dispatcher,
                accounting.clone(),
            )
        }
        (None, None) => AgentExecution::Disabled,
        _ => return Err(anyhow!("prepare local agent diagnostic retention")),
    };
    let seed = attempt
        .run()?
        .execution_seed(admitted.clone(), artifacts.clone())
        .await
        .map_err(anyhow::Error::new)
        .context("load retained workflow values")?;
    let (run_id, attempt_id) = attempt.run()?.finalization_identity()?;
    let execution_start =
        WorkflowExecutionStart::seeded(attempt.run()?.process_guard_registry(), seed)
            .with_finalization_identity(run_id, attempt_id);
    let commit_port =
        attempt
            .run()?
            .commit_port(diagnostics.clone(), accounting, artifacts.clone());
    // REQ-LOCALRUN-143: the text of the port's failed commit outlives the engine's port.
    let commit_failure_text = commit_port.failure_text();
    let dispatcher = if quiet || !has_workflow_steps(workflow) {
        None
    } else {
        // REQ-WFEXEC-63, REQ-CONTINUE-75: the values the resolved waits leave this attempt.
        let controls_directory = attempt.run()?.run_directory().to_owned();
        let controls = blocking_operation(move || {
            attempt_controls(&controls_directory, attempt_number).map_err(anyhow::Error::new)
        })
        .await
        .map_err(|error| match error {
            BlockingOperationError::Operation(error) => error,
            BlockingOperationError::WorkerUnavailable => anyhow!("read the run's controls"),
        })
        .context("read the run's controls")?;
        // REQ-WFEXEC-68: an abandoned fan-out resumes at the element of the child it left,
        // and a repair retry at the element of the repaired child it adopts (REQ-CONTINUE-75).
        let mut context = controls.controls;
        if let Some((step, Some(item))) = child_recovery.as_ref().map(ChildRecovery::target) {
            context.resume_item = Some((step.to_owned(), item));
        }
        Some((
            LocalChildRunDispatcher::new(
                attempt.run()?,
                admitted,
                controls.replacement,
                (child_recovery, attempt.mirror.clone()),
                journals.clone(),
                // REQ-LOCALRUN-200: a child's steps reach the parent's progress stream; the
                // terminal interface shows none of them.
                match attempt.host.as_ref() {
                    Some(ActiveRunHost::Standard(presentation)) => Some(presentation.clone()),
                    _ => None,
                },
                // REQ-LOCALRUN-207: a child's failed state commit refuses this attempt's
                // later commits.
                commit_port.child_failure_latch(),
            )?,
            context,
        ))
    };
    let children = match &dispatcher {
        None => ChildRuns::Disabled,
        Some((dispatcher, controls)) => ChildRuns::Enabled {
            dispatcher: dispatcher.clone(),
            controls: controls.clone(),
        },
    };
    attempt.execution_started = true;
    let execution = if let Some(error) = hooks.execution_failure() {
        Err(error)
    } else {
        execute_workflow_with_children(
            admitted.clone(),
            &artifacts,
            &inputs,
            &diagnostics,
            agents,
            children,
            SystemExecutionClock,
            commit_port,
            observer.clone(),
            execution_start,
        )
        .await
    };
    if let Some(signal_task) = attempt.signal_task.take() {
        signal_task.abort();
    }
    let execution = match execution {
        Ok(execution) => execution,
        Err(error) => {
            // REQ-LOCALRUN-143, REQ-LOCALRUN-205: a failed state commit is recorded, best
            // effort, and settles nothing; the error prints it and downcasts to it.
            if let CoordinationError::CommitFailed(failure) = error {
                let _ = attempt
                    .run()?
                    .record_state_persistence_failure_async(failure)
                    .await;
                return Err(anyhow::Error::new(StateCommitFailed {
                    failure,
                    text: commit_failure_text.get().cloned(),
                    run_directory: attempt.run()?.run_directory().to_owned(),
                }));
            }
            return Err(anyhow::Error::new(error).context("execute admitted local workflow"));
        }
    };
    let durable_invocations = attempt.run()?.durable_invocations()?;
    let observed_timing = observer.snapshot();
    let run_timing = observed_run_timing(&observed_timing)
        .ok_or_else(|| anyhow!("prepare authoritative local workflow terminal result"))?;
    // REQ-WFEXEC-73, REQ-LOCALRUN-186: a withdrawal recorded while the attempt ran that no
    // `workflow` step boundary honored, because the root has no `workflow` step, the active
    // stage ended the attempt in a wait or the attempt exited, ends the run `withdrawn` here
    // instead: no wait record and no `onWait`, the published result the attempt reached, and
    // `onOutcome.withdrawn` once. It is read again as the final record is written. A
    // continued attempt leaves it to the chained attempt's first boundary; a child run's
    // withdrawal is its parent's.
    let settles_withdrawable = !quiet
        && matches!(
            execution.outcome,
            RunOutcome::Waiting { .. } | RunOutcome::Succeeded | RunOutcome::Failed { .. }
        );
    // A test seam between the settlement and the first read; nothing outside `test-fixtures`.
    if !quiet {
        blocking_operation(await_settled_gate)
            .await
            .map_err(|error| match error {
                BlockingOperationError::Operation(error) => anyhow::Error::new(error),
                BlockingOperationError::WorkerUnavailable => anyhow!("await the settled gate"),
            })?;
    }
    let withdrawn = settles_withdrawable && attempt_withdrawal_pending(attempt).await?;
    // REQ-WFEXEC-61, REQ-LOCALRUN-178: an attempt that ends in a wait chooses no outcome
    // and writes its wait record before the run record reads `waiting`.
    let wait_record = match &execution.outcome {
        RunOutcome::Waiting { .. } if withdrawn => None,
        RunOutcome::Waiting { wait } => {
            // REQ-WFEXEC-72: an attempt that kept the wait it continued from writes back the
            // record `begin` removed, unchanged, and performs no `onWait` (REQ-LOCALRUN-191).
            let kept = wait.kept().is_some();
            let record = if kept {
                attempt
                    .run()?
                    .resumed_wait_record()
                    .cloned()
                    .ok_or_else(|| anyhow!("the kept wait's record is unavailable"))?
            } else {
                entered_wait_record(
                    wait,
                    admitted.inputs(),
                    &execution.steps,
                    attempt.run()?.run_directory(),
                    dispatcher.as_ref().map(|(dispatcher, _)| dispatcher),
                )?
            };
            let owned_run = attempt
                .run
                .take()
                .ok_or_else(|| anyhow!("local attempt ownership unavailable"))?;
            let written = record.clone();
            let (owned_run, recorded) = match blocking_operation(move || {
                let recorded = owned_run.record_wait(&written);
                Ok::<_, std::convert::Infallible>((owned_run, recorded))
            })
            .await
            {
                Ok(recorded) => recorded,
                Err(BlockingOperationError::Operation(never)) => match never {},
                Err(BlockingOperationError::WorkerUnavailable) => {
                    return Err(anyhow!("record the local workflow wait"));
                }
            };
            attempt.run = Some(owned_run);
            recorded.context("record the local workflow wait")?;
            // REQ-LOCALRUN-191: `onWait`, once the wait record is written, for a wait entered.
            if !kept {
                let waited = record.clone();
                super::mirror::perform_blocking(
                    attempt.mirror.as_ref(),
                    Box::new(move |mirror, target| mirror.wait(target, &waited)),
                )
                .await;
            }
            Some(record)
        }
        _ => None,
    };
    // REQ-WFEXEC-64: an attempt that took a backward or limited edge chooses no outcome
    // either, and the run record stays `running` for the attempt the loop chains into.
    if let RunOutcome::Continued { edge } = &execution.outcome {
        attempt.continued = Some(edge.clone());
    }
    let continued = attempt.continued.is_some();
    let outcome = if withdrawn {
        Some(WITHDRAWN_OUTCOME.to_owned())
    } else {
        (wait_record.is_none() && !continued)
            .then(|| chosen_outcome(&workflow.definition, &execution))
    };
    if let (Some(outcome), Some(ActiveRunHost::Quiet(host))) = (&outcome, attempt.host.as_mut()) {
        host.outcome = Some(child_run_outcome(outcome, &execution));
    }
    let run = build_run_result(
        workflow,
        admitted,
        execution,
        LocalRunEvidence {
            diagnostics: &diagnostics,
            durable_invocations: &durable_invocations,
            timing: observed_timing,
        },
        run_timing,
        attempt.run()?,
    )?;
    attempt.host()?.reconcile_and_mark_quiescent(&run)?;
    attempt.host()?.begin_publication();
    let publication_run = run.clone();
    let publication_artifacts = artifacts.clone();
    let published_wait = wait_record.clone();
    // REQ-WFEXEC-40, REQ-LOCALRUN-135: a `continued` attempt under an interruption starts no
    // next attempt and exits by the interruption's reason.
    let source = admitted.execution().cancellation().source();
    let held_back = source
        .interruption_requested()
        .then(|| source.cancellation_reason())
        .flatten()
        .filter(|_| continued);
    let chains = !quiet && continued && held_back.is_none();
    let owned_run = attempt
        .run
        .take()
        .ok_or_else(|| anyhow!("local attempt ownership unavailable"))?;
    let (owned_run, publication, state_publication) = complete_blocking_phase(
        blocking_operation(move || {
            let mut publication = publish_prepared_workflow_result(
                &destination,
                &publication_artifacts,
                &publication_run,
            );
            if let Ok(terminal) = &mut publication {
                match leaf {
                    ExecutionLeaf::Retry => terminal.mark_retry(),
                    ExecutionLeaf::Resume => terminal.mark_resume(),
                    ExecutionLeaf::Restart => terminal.mark_restart(),
                    ExecutionLeaf::Upgrade => terminal.mark_upgrade(),
                    ExecutionLeaf::Run => {}
                }
                if let Some(wait) = published_wait {
                    terminal.set_wait(wait);
                }
                if let Some(reason) = held_back {
                    terminal.hold_back_continuation(reason);
                }
            }
            let state_publication = match &publication {
                Ok(_) => owned_run.record_result_published(),
                // A committed rename is already visible. Do not seal a permanent
                // failure over it when only the post-rename sync failed: status,
                // retry and continue reconcile the pending publication durably.
                Err(error) if error.committed() => Ok(()),
                Err(error) => owned_run.record_result_publication_failed(
                    publication_failure_phase(error.phase()),
                    error.invariant(),
                ),
            };
            Ok::<_, std::convert::Infallible>((owned_run, publication, state_publication))
        })
        .await,
        attempt.host()?,
        "publish terminal local workflow result",
    )
    .await?;
    attempt.run = Some(owned_run);
    attempt.host()?.complete_publication(&publication);
    attempt.host()?.begin_cleanup();
    let private_staging = attempt
        .private_staging
        .take()
        .ok_or_else(|| anyhow!("private local workflow staging unavailable"))?;
    let owned_run = attempt
        .run
        .take()
        .ok_or_else(|| anyhow!("local attempt ownership unavailable"))?;
    let exit_mirror = attempt.mirror.take();
    let wait_written = wait_record.is_some();
    let (cleanup_failed, cleanup_state, released_ownership, bridge) = complete_blocking_phase(
        blocking_operation(move || {
            let execution_staging_failed =
                release_execution_staging(&inputs, agent_staging.as_ref(), &artifacts);
            let private_staging_failed = private_staging.release().is_err();
            let cleanup_failed = execution_staging_failed || private_staging_failed;
            let cleanup_state = if cleanup_failed {
                owned_run.record_private_cleanup_failure()
            } else {
                Ok(())
            };
            // REQ-WFEXEC-73: a withdrawal recorded while the attempt finished, after the first
            // read, is read again under the run lock as the final record is written. It ends
            // the run `withdrawn` in place of the outcome or wait the attempt chose: the wait
            // record it wrote is removed, and an `onWait` already written stays. Every attempt
            // that ends the run, a `canceled` one included, holds the run's controls lock from
            // before that read until the run lock is released, so a `withdraw` either appends
            // before it or finds the run released (REQ-LOCALRUN-186); a lock that cannot be
            // taken leaves the read unserialized, as unreadable controls withdraw nothing.
            // REQ-LOCALRUN-211: under that lock it spends every withdrawal recorded before it,
            // honored or not: its `settledAt` is taken again when one is later, so a retry of
            // the run is not withdrawn by it. A `canceled` attempt keeps its outcome.
            // REQ-LOCALRUN-214, INV-ENGINE-39: an attempt that ended `continued` and chains, one
            // no interruption holds back, takes the same lock before it releases the run and
            // hands it to the next attempt's acquisition, so a `withdraw` arriving between the
            // attempts waits for that acquisition and finds the run held.
            let ends_run = !quiet && !continued;
            let last_read = settles_withdrawable && !withdrawn;
            let controls = (ends_run || chains)
                .then(|| owned_run.lock_controls().ok())
                .flatten();
            let withdrawn_late = last_read && owned_run.withdrawal_pending().unwrap_or(false);
            let spent = if ends_run {
                // A test seam before the final record; nothing outside `test-fixtures`.
                owned_run
                    .spend_withdrawals()
                    .and_then(|()| await_release_gate())
            } else {
                Ok(())
            };
            let outcome = if withdrawn_late {
                Some(WITHDRAWN_OUTCOME.to_owned())
            } else {
                outcome
            };
            let record_state = match &outcome {
                Some(outcome) => {
                    owned_run.update_run_record(RunState::Exited, attempt_number, Some(outcome))
                }
                None if continued => {
                    owned_run.update_run_record(RunState::Running, attempt_number, None)
                }
                None => owned_run.update_run_record(RunState::Waiting, attempt_number, None),
            };
            let exited = record_state.is_ok();
            let record_state = match record_state {
                Ok(()) if withdrawn_late && wait_written => owned_run.remove_wait_record(),
                record_state => record_state,
            };
            let cleanup_state = cleanup_state.and(spent).and(record_state);
            let released_ownership = owned_run.release();
            let bridge = controls.filter(|_| chains);
            // REQ-LOCALRUN-191: the chosen outcome's `onOutcome`, once the record reads it and
            // the run is released, so a write never holds the run.
            if let (true, Some(outcome), Some(mirror)) = (exited, outcome, exit_mirror) {
                super::mirror::perform(
                    &mirror,
                    Box::new(move |mirror, target| mirror.outcome(target, &outcome)),
                );
            }
            Ok::<_, std::convert::Infallible>((
                cleanup_failed,
                cleanup_state,
                released_ownership,
                bridge,
            ))
        })
        .await,
        attempt.host()?,
        "release private local workflow staging",
    )
    .await?;
    attempt.bridge = bridge;
    attempt.host()?.complete_cleanup(cleanup_failed);
    let state_commit_failed = state_publication.is_err() || cleanup_state.is_err();
    attempt
        .host()?
        .mark_adapter_lifecycle_completed(released_ownership);
    attempt.disarm();
    Ok(attempt
        .host()?
        .finish(
            workflow,
            &run,
            &publication,
            cleanup_failed,
            state_commit_failed,
        )
        .await)
}

/// REQ-WFEXEC-73: whether a withdrawal recorded while the run is live is pending, read
/// through the attempt's owner under the run lock it holds. Controls that cannot be read
/// withdraw nothing.
async fn attempt_withdrawal_pending(attempt: &mut AttemptTeardown) -> anyhow::Result<bool> {
    let owned_run = attempt
        .run
        .take()
        .ok_or_else(|| anyhow!("local attempt ownership unavailable"))?;
    let (owned_run, pending) = match blocking_operation(move || {
        let pending = owned_run.withdrawal_pending().unwrap_or(false);
        Ok::<_, std::convert::Infallible>((owned_run, pending))
    })
    .await
    {
        Ok(read) => read,
        Err(BlockingOperationError::Operation(never)) => match never {},
        Err(BlockingOperationError::WorkerUnavailable) => {
            return Err(anyhow!("read the run's controls"));
        }
    };
    attempt.run = Some(owned_run);
    Ok(pending)
}

async fn complete_blocking_phase<Value>(
    completion: Result<Value, BlockingOperationError<std::convert::Infallible>>,
    host: &mut ActiveRunHost,
    failure_context: &str,
) -> anyhow::Result<Value> {
    match completion {
        Ok(value) => Ok(value),
        Err(BlockingOperationError::Operation(never)) => match never {},
        Err(BlockingOperationError::WorkerUnavailable) => {
            host.stop_terminal().await;
            Err(anyhow!(failure_context.to_owned()))
        }
    }
}

fn release_execution_staging(
    inputs: &InputStaging,
    agents: Option<&AgentInputStaging>,
    artifacts: &ArtifactStaging,
) -> bool {
    let input_failed = inputs.release().is_err();
    let agent_failed = agents.is_some_and(|staging| staging.release().is_err());
    let artifact_failed = artifacts.release().is_err();
    input_failed || agent_failed || artifact_failed
}

fn execution_output(
    config: PresentationConfig,
    owned_run: &LocalAttemptOwner,
    leaf: ExecutionLeaf,
) -> WorkflowRunOutput<io::Stdout, io::Stderr> {
    let output = WorkflowRunOutput::new(config, io::stdout(), io::stderr());
    match leaf {
        ExecutionLeaf::Run => output,
        ExecutionLeaf::Retry => output.for_retry(owned_run.run_directory()),
        ExecutionLeaf::Resume => output.for_resume(owned_run.run_directory()),
        ExecutionLeaf::Restart => output.for_restart(owned_run.run_directory()),
        ExecutionLeaf::Upgrade => output.for_upgrade(owned_run.run_directory()),
    }
}

fn rejection_output(
    config: PresentationConfig,
    render: impl FnOnce(WorkflowRunOutput<io::Stdout, io::Stderr>) -> WorkflowRunPresentationResult,
) -> super::super::super::CommandResult {
    rejection_exit(render(WorkflowRunOutput::new(
        config,
        io::stdout(),
        io::stderr(),
    )))
}

pub(super) fn rejection_exit(
    result: WorkflowRunPresentationResult,
) -> super::super::super::CommandResult {
    match result {
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: Some(diagnostic),
        } => Err(anyhow!(diagnostic).into()),
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: None,
        } => Ok(ExitCode::GeneralFailure),
        WorkflowRunPresentationResult::Failed(failure) => Err(anyhow::Error::new(failure).into()),
        WorkflowRunPresentationResult::PublicationFailed(error) => {
            Err(anyhow::Error::new(error).into())
        }
        WorkflowRunPresentationResult::Published { .. } => Ok(ExitCode::GeneralFailure),
    }
}

fn presentation_exit_code(
    result: WorkflowRunPresentationResult,
) -> super::super::super::CommandResult {
    match result {
        WorkflowRunPresentationResult::Published { outcome, .. } => Ok(outcome.into()),
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: Some(diagnostic),
        } => Err(anyhow!(diagnostic).into()),
        WorkflowRunPresentationResult::Rejected {
            human_diagnostic: None,
        } => Ok(ExitCode::GeneralFailure),
        WorkflowRunPresentationResult::PublicationFailed(error) => {
            Err(anyhow::Error::new(error).into())
        }
        WorkflowRunPresentationResult::Failed(failure) => Err(anyhow::Error::new(failure).into()),
    }
}

fn parse_parallelism(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| (1..=MAXIMUM_PARALLEL_STEPS).contains(value))
        .ok_or_else(|| format!("value must be between 1 and {MAXIMUM_PARALLEL_STEPS}"))
}

/// REQ-LOCALRUN-209: a run's owner is a non-empty name.
fn parse_owner(value: &str) -> Result<String, String> {
    if value.is_empty() {
        return Err("the owner must not be empty".to_owned());
    }
    Ok(value.to_owned())
}

/// REQ-LOCALRUN-209: `NAME=VALUE`, split at the first `=`, with `NAME` of the external
/// reference form.
fn parse_external(value: &str) -> Result<(String, String), String> {
    let (name, value) = value
        .split_once('=')
        .ok_or_else(|| "expected NAME=VALUE".to_owned())?;
    if !WorkflowRunRecord::is_external_name(name) {
        return Err(
            "NAME must be a lowercase letter followed by letters, digits or dots".to_owned(),
        );
    }
    Ok((name.to_owned(), value.to_owned()))
}

/// REQ-LOCALRUN-209: the `--external` references by name, each named at most once.
fn external_references(entries: &[(String, String)]) -> anyhow::Result<BTreeMap<String, String>> {
    let mut references = BTreeMap::new();
    for (name, value) in entries {
        if references.insert(name.clone(), value.clone()).is_some() {
            return Err(anyhow!("--external names `{name}` more than once"));
        }
    }
    Ok(references)
}

fn required_agent_harnesses(
    workflow: &ResolvedWorkflow,
) -> impl Iterator<Item = &ValidatedHarness> {
    let node_harnesses = workflow
        .definition
        .source_order
        .iter()
        .chain(&workflow.definition.finalizer_source_order)
        .filter_map(|step_name| {
            let step = workflow.definition.node(step_name)?;
            let ValidatedStep::Agent(step) = step else {
                return None;
            };
            Some(&step.agent.harness)
        });
    let recovery_harnesses = workflow
        .definition
        .source_order
        .iter()
        .filter_map(|step_name| {
            let handler = workflow
                .definition
                .recoveries
                .get(step_name)?
                .as_ref()?
                .handler
                .as_ref()?;
            let ValidatedRecoveryHandler::Agent { harness, .. } = handler else {
                return None;
            };
            Some(harness)
        });
    node_harnesses.chain(recovery_harnesses)
}

/// REQ-AGENT-66: called only after admission succeeds, so a rejection leaves stderr to the
/// rejection report, and before the attempt starts the terminal interface. The advisory is
/// nonblocking, so a failed write does not stop the run.
pub(super) fn write_untested_advisories(advisories: &[UntestedHarnessAdvisory]) {
    let _ = write_untested_harness_advisories(&mut io::stderr(), advisories);
}

pub(super) fn execution_context_for_workflow(
    workflow: &ResolvedWorkflow,
    root: PathBuf,
    maximum_parallel_steps: usize,
    cancellation: CancellationSource,
) -> Result<ExecutionContext, AgentHarnessInstallationFailure> {
    let (context, mut failures) = execution_context_with_profile_failures(
        workflow,
        root,
        maximum_parallel_steps,
        cancellation,
        false,
        EnvironmentSnapshot::new(env::vars_os()),
    );
    match failures.pop() {
        Some(failure) => Err(failure),
        None => Ok(context),
    }
}

/// The admission context of a continuation, every missing harness collected: the launching
/// environment is the host's, or for a child's repair continuation `environment`, its
/// parent's admitted environment (REQ-WFEXEC-59).
pub(super) fn continuation_execution_context(
    workflow: &ResolvedWorkflow,
    root: PathBuf,
    maximum_parallel_steps: usize,
    cancellation: CancellationSource,
    environment: Option<EnvironmentSnapshot>,
) -> (ExecutionContext, Vec<AgentHarnessInstallationFailure>) {
    execution_context_with_profile_failures(
        workflow,
        root,
        maximum_parallel_steps,
        cancellation,
        true,
        environment.unwrap_or_else(|| EnvironmentSnapshot::new(env::vars_os())),
    )
}

fn execution_context_with_profile_failures(
    workflow: &ResolvedWorkflow,
    root: PathBuf,
    maximum_parallel_steps: usize,
    cancellation: CancellationSource,
    collect_all: bool,
    environment: EnvironmentSnapshot,
) -> (ExecutionContext, Vec<AgentHarnessInstallationFailure>) {
    // REQ-WFEXEC-59, REQ-WFEXEC-65: an `external` wait admits on a condition this CLI
    // evaluates.
    let mut context = ExecutionContext::new(
        root,
        default_execution_policy_limits(maximum_parallel_steps),
        environment.clone(),
        CancellationPolicy::new(cancellation, CANCELLATION_GRACE),
    )
    .with_conditions(super::condition::Conditions::default().supported());
    if workflow.requires_git_capture() {
        context = context.with_local_git_capture();
    }

    let mut pi_validated = false;
    let mut claude_code_validated = false;
    let mut codex_validated = false;
    let mut failures = Vec::new();
    for harness in required_agent_harnesses(workflow) {
        let failure = match harness {
            ValidatedHarness::Pi(_) if !pi_validated => {
                pi_validated = true;
                match discover_and_validate_pi_installation() {
                    Ok(installation) => {
                        context = context.with_pi_installation(installation);
                        None
                    }
                    Err(error) => Some(AgentHarnessInstallationFailure::Pi(error)),
                }
            }
            ValidatedHarness::ClaudeCode(_) if !claude_code_validated => {
                claude_code_validated = true;
                match discover_and_validate_claude_code_installation() {
                    Ok(installation) => {
                        context = context.with_claude_code_installation(installation);
                        None
                    }
                    Err(error) => Some(AgentHarnessInstallationFailure::ClaudeCode(error)),
                }
            }
            ValidatedHarness::Codex(_) if !codex_validated => {
                codex_validated = true;
                match discover_and_validate_codex_installation() {
                    Ok(installation) => {
                        context = context.with_codex_installation(installation);
                        None
                    }
                    Err(error) => Some(AgentHarnessInstallationFailure::Codex(error)),
                }
            }
            ValidatedHarness::Pi(_)
            | ValidatedHarness::ClaudeCode(_)
            | ValidatedHarness::Codex(_) => None,
        };
        if let Some(failure) = failure {
            failures.push(failure);
            if !collect_all {
                break;
            }
        }
    }
    (context, failures)
}

/// A workflow with `workflow` steps is edge-scheduled (REQ-WFFORMAT-93), and only such a
/// workflow runs children.
fn has_workflow_steps(workflow: &ResolvedWorkflow) -> bool {
    matches!(workflow.definition.scheduling, Scheduling::Edges { .. })
}

/// How a child attempt that ran to its result ended, for the parent's step
/// (REQ-WFEXEC-60): a cancelled child is `Cancelled`; any other child exited with the
/// outcome it records and its available `text` and `json` exports, and with the
/// interruption that failed it when its primary issue is an `interrupted` step
/// (REQ-WFEXEC-70).
fn child_run_outcome(
    outcome: &str,
    execution: &WorkflowExecutionResult<ExecutionInstant>,
) -> ChildRunOutcome {
    if matches!(execution.outcome, RunOutcome::Cancelled { .. }) {
        return ChildRunOutcome::Cancelled;
    }
    ChildRunOutcome::Exited {
        outcome: outcome.to_owned(),
        exports: execution
            .exports
            .iter()
            .filter_map(|(name, export)| match export {
                ExportValue::Available {
                    output: value @ (CapturedValue::Text(_) | CapturedValue::Json(_)),
                } => Some((name.clone(), value.clone())),
                ExportValue::Available { .. } | ExportValue::Unavailable { .. } => None,
            })
            .collect(),
        interruption: execution.outcome.interruption(),
    }
}

/// The child run a parent's continuation takes over rather than starting another: from an
/// abandoned attempt (REQ-CONTINUE-76) one that exited is adopted, one left running gets a
/// new attempt in place, and an element whose child was never created runs afresh; a repair
/// retry adopts the child an independent repair completed after its wait began
/// (REQ-CONTINUE-75), and otherwise, for a repair wait's retry (REQ-CONTINUE-81), the failed
/// child the wait names continues with the partition its preflight admitted. `item` is the
/// fan-out element, at which the continuation resumes the step (REQ-WFEXEC-68).
pub(super) enum ChildRecovery {
    Fresh {
        step: String,
        item: Option<u64>,
    },
    Adopt {
        step: String,
        item: Option<u64>,
        directory: PathBuf,
    },
    Retry {
        step: String,
        item: Option<u64>,
        directory: PathBuf,
    },
    Continue {
        step: String,
        item: Option<u64>,
        continuation: Box<super::child_recovery::ChildContinuation>,
    },
}

impl ChildRecovery {
    fn target(&self) -> (&str, Option<u64>) {
        match self {
            Self::Fresh { step, item }
            | Self::Adopt { step, item, .. }
            | Self::Retry { step, item, .. }
            | Self::Continue { step, item, .. } => (step, *item),
        }
    }
}

/// The record of the wait an attempt ended on (REQ-WFEXEC-61, REQ-WFEXEC-62): the payload
/// from the waiting step's outputs and child directory or, for a stop, from the stop control
/// in force before the step, for an interrupted `repair` wait with the `control.interrupt`
/// the signal observer acted on (REQ-WFEXEC-70, REQ-WFDEF-6), and for an `external` wait its
/// rule's run-value operands resolved from the run's `inputs` and step outputs
/// (REQ-WFEXEC-65).
fn entered_wait_record(
    wait: &EnteredWait,
    inputs: &um_execution::ResolvedInputs,
    steps: &BTreeMap<String, um_execution::StepState<CapturedValue>>,
    run_directory: &Path,
    dispatcher: Option<&LocalChildRunDispatcher>,
) -> anyhow::Result<WaitRecord> {
    // REQ-WFDEF-6: an interrupted wait's payload names the `control.interrupt` whose
    // interruption the signal observer requested.
    let control = if wait.kind() == WaitKind::Stop {
        let controls = read_controls(run_directory).context("read the run's controls")?;
        stop_in_force(&controls, wait.step()).cloned()
    } else if wait.interrupted().is_some() {
        dispatcher.and_then(|dispatcher| dispatcher.parent.journals.interrupting_control())
    } else {
        None
    };
    // REQ-WFEXEC-61, REQ-WFEXEC-68: the waiting step's latest child and, for a fan-out, the
    // element it ran for.
    let (child, item) = dispatcher
        .and_then(|dispatcher| dispatcher.started_child(wait.step()))
        .map_or((None, None), |(child, item)| {
            (
                child
                    .strip_prefix(run_directory)
                    .ok()
                    .and_then(Path::to_str)
                    .map(str::to_owned),
                item,
            )
        });
    let payload = wait_payload(wait, steps, child.as_deref(), control.as_ref());
    WaitRecord::entered(wait, item, payload, um_support::utc_now(), inputs, steps)
        .map_err(anyhow::Error::new)
        .context("record the local workflow wait")
}

/// Runs a parent's `workflow` steps as child runs of files in the parent's pinned tree
/// copy, in this process and awaited (REQ-WFEXEC-59, REQ-LOCALRUN-177).
#[derive(Clone)]
struct LocalChildRunDispatcher {
    parent: Arc<LocalChildRunParent>,
}

struct LocalChildRunParent {
    run: ChildRunParent,
    run_directory: PathBuf,
    copy_root: PathBuf,
    execution_root: PathBuf,
    maximum_parallel_steps: usize,
    /// The parent's admitted environment, the launching environment of each child: its
    /// admission drops the engine's `UM_` names again and its `environmentPassthrough`
    /// selects among the names the root admitted.
    environment: EnvironmentSnapshot,
    /// The step a `control.retry` continues at and the agent-profile replacement its child
    /// resolves with (REQ-CONTINUE-75).
    replacement: Option<(String, AgentProfileReplacementDocument)>,
    /// Taken by the first child run of its step and element: an abandoned parent's child
    /// (REQ-CONTINUE-76) or the repaired child a repair retry adopts (REQ-CONTINUE-75).
    recovery: std::sync::Mutex<Option<ChildRecovery>>,
    /// The latest child run directory each step started and the fan-out element it ran
    /// for, for a wait's payload and step (REQ-WFEXEC-61).
    started: std::sync::Mutex<BTreeMap<String, (PathBuf, Option<u64>)>>,
    /// The parent's mirror, which writes `onStage` as each child exits (REQ-LOCALRUN-191).
    mirror: Option<Arc<Mirror>>,
    /// The journals the command's signal observer reads, which each child's run directory
    /// joins while its attempt runs (REQ-LOCALRUN-135).
    journals: ControlJournals,
    /// The parent's progress stream, which carries each child's step state records
    /// (REQ-LOCALRUN-200); none when the parent presents through the terminal interface.
    presentation: Option<SystemPresentation>,
    /// The parent's commit-port latch, set by a child whose own state commit failed so the
    /// parent commits nothing after it (REQ-LOCALRUN-207, INV-ENGINE-33).
    child_failure: Arc<OnceLock<ChildCommitFailure>>,
}

impl LocalChildRunDispatcher {
    fn new(
        run: &LocalAttemptOwner,
        admitted: &AdmittedWorkflow,
        replacement: Option<(String, serde_json::Value)>,
        (recovery, mirror): (Option<ChildRecovery>, Option<Arc<Mirror>>),
        journals: ControlJournals,
        presentation: Option<SystemPresentation>,
        child_failure: Arc<OnceLock<ChildCommitFailure>>,
    ) -> anyhow::Result<Self> {
        let parent = run
            .child_run_parent()
            .map_err(anyhow::Error::new)
            .context("prepare local child runs")?;
        let environment = admitted.execution().environment().clone();
        let replacement = replacement
            .map(|(step, profiles)| {
                control_replacement_document(profiles).map(|document| (step, document))
            })
            .transpose()
            .context("prepare the control's agent-profile replacement")?;
        Ok(Self {
            parent: Arc::new(LocalChildRunParent {
                run: parent,
                run_directory: run.run_directory().to_owned(),
                copy_root: run.tree_copy_directory(),
                execution_root: admitted.execution().root().to_owned(),
                maximum_parallel_steps: admitted
                    .execution()
                    .limits()
                    .maximum_parallel_steps()
                    .get(),
                environment,
                replacement,
                recovery: std::sync::Mutex::new(recovery),
                started: std::sync::Mutex::new(BTreeMap::new()),
                mirror,
                journals,
                presentation,
                child_failure,
            }),
        })
    }

    /// The latest child run directory `step` started in this attempt and its element.
    fn started_child(&self, step: &str) -> Option<(PathBuf, Option<u64>)> {
        self.parent
            .started
            .lock()
            .ok()
            .and_then(|started| started.get(step).cloned())
    }
}

/// The replacement document a `control.retry`'s `payload.agentProfiles` names (REQ-AGENT-59).
pub(super) fn control_replacement_document(
    profiles: serde_json::Value,
) -> serde_json::Result<AgentProfileReplacementDocument> {
    serde_json::to_vec(&serde_json::json!({
        "schemaVersion": 1,
        "agentProfiles": profiles,
    }))
    .map(AgentProfileReplacementDocument::new)
}

impl LocalChildRunParent {
    fn record_started(
        &self,
        request: &ChildRunRequest,
        directory: &Path,
        control: &mut ChildRunControl,
    ) {
        if let Ok(mut started) = self.started.lock() {
            started.insert(request.step.clone(), (directory.to_owned(), request.item));
        }
        control.report_started(directory.to_owned());
    }

    fn take_recovery(&self, step: &str, item: Option<u64>) -> Option<ChildRecovery> {
        let mut recovery = self.recovery.lock().ok()?;
        if recovery
            .as_ref()
            .is_some_and(|recovery| recovery.target() == (step, item))
        {
            recovery.take()
        } else {
            None
        }
    }
}

type ChildRunFuture = std::pin::Pin<Box<dyn Future<Output = ChildRunOutcome> + Send>>;

impl ChildRunDispatcher for LocalChildRunDispatcher {
    fn run(
        &self,
        request: ChildRunRequest,
        control: ChildRunControl,
    ) -> impl Future<Output = ChildRunOutcome> + Send {
        // A child attempt runs the same attempt phases as its parent; boxing its future
        // keeps the dispatcher's future type finite.
        let child: ChildRunFuture =
            Box::pin(run_local_child(Arc::clone(&self.parent), request, control));
        child
    }

    /// REQ-WFEXEC-62: a stop control in force for `step`. A controls directory that cannot
    /// be read stops nothing.
    async fn stop_before(&self, step: &str) -> bool {
        let run_directory = self.parent.run_directory.clone();
        let step = step.to_owned();
        blocking_operation(move || {
            read_controls(&run_directory).map(|controls| stop_in_force(&controls, &step).is_some())
        })
        .await
        .unwrap_or(false)
    }

    /// REQ-WFEXEC-73: a withdrawal recorded while the run is live, read under the run lock
    /// this attempt holds. Controls that cannot be read withdraw nothing.
    async fn withdrawal_pending(&self) -> bool {
        let parent = Arc::clone(&self.parent);
        blocking_operation(move || parent.run.withdrawal_pending())
            .await
            .unwrap_or(false)
    }
}

async fn run_local_child(
    parent: Arc<LocalChildRunParent>,
    request: ChildRunRequest,
    mut control: ChildRunControl,
) -> ChildRunOutcome {
    let cancellation = control.cancellation().clone();
    // A test seam before the child is prepared; nothing outside `test-fixtures`.
    if blocking_operation(await_child_start_gate).await.is_err() {
        return ChildRunOutcome::Rejected {
            code: "child_preparation_unavailable".to_owned(),
        };
    }
    let recovery = parent.take_recovery(&request.step, request.item);
    if let Some(ChildRecovery::Adopt { directory, .. }) = recovery {
        // REQ-CONTINUE-76: a child that exited settles the step from its record and
        // published exports without running again.
        let adopted = directory.clone();
        let outcome = blocking_operation(move || {
            Ok::<_, std::convert::Infallible>(adopted_child_outcome(&adopted))
        })
        .await
        .unwrap_or(ChildRunOutcome::Interrupted);
        parent.record_started(&request, &directory, &mut control);
        return outcome;
    }
    // REQ-WFEXEC-70, REQ-WFEXEC-72: a child whose preparation an interruption precedes is not
    // created and gets no attempt; the driver settles the step as an unstarted child, and the
    // dropped recovery releases what it held. An exited child is adopted above as before, and a
    // cancellation prepares the child as before.
    if cancellation.interruption_requested() {
        return ChildRunOutcome::Interrupted;
    }
    let preparation_parent = Arc::clone(&parent);
    let preparation_request = request.clone();
    let preparation_cancellation = cancellation.clone();
    let (prepared, leaf) = match recovery {
        Some(ChildRecovery::Retry { directory, .. }) => (
            blocking_operation(move || {
                prepare_child_retry(&preparation_parent, &directory, preparation_cancellation)
            })
            .await,
            ExecutionLeaf::Retry,
        ),
        // REQ-CONTINUE-81, INV-ENGINE-27: the failed child the repair wait named, whose
        // continuation its preflight admitted and still holds locked.
        Some(ChildRecovery::Continue { continuation, .. }) => (
            blocking_operation(move || {
                super::child_recovery::begin(
                    *continuation,
                    &preparation_request,
                    preparation_cancellation,
                )
            })
            .await,
            ExecutionLeaf::Retry,
        ),
        _ => (
            blocking_operation(move || {
                prepare_local_child(
                    &preparation_parent,
                    &preparation_request,
                    preparation_cancellation,
                )
            })
            .await,
            ExecutionLeaf::Run,
        ),
    };
    let (workflow, admitted, owned_run) = match prepared {
        Ok(prepared) => prepared,
        Err(BlockingOperationError::Operation(code)) => {
            return ChildRunOutcome::Rejected { code };
        }
        Err(BlockingOperationError::WorkerUnavailable) => {
            return ChildRunOutcome::Rejected {
                code: "child_preparation_unavailable".to_owned(),
            };
        }
    };
    parent.record_started(&request, owned_run.run_directory(), &mut control);
    let copy_root = owned_run.tree_copy_directory();
    let mut attempt = AttemptTeardown::new(owned_run, failure_outcome(&workflow.definition), None);
    let Ok(admitted) = bind_tree_copy(admitted, &copy_root) else {
        attempt.teardown().await;
        return ChildRunOutcome::Interrupted;
    };
    // REQ-LOCALRUN-200: a fresh, retried or continued child's steps reach the parent's
    // progress stream under the parent's step and element. A child file whose definition the
    // stream cannot describe stays silent rather than failing the child.
    let presentation = parent
        .presentation
        .as_ref()
        .and_then(|presentation| {
            presentation
                .child_scope(&workflow, &request.step, request.item)
                .ok()
        })
        .map_or(ChildPresentation::Silent, ChildPresentation::Forwarding);
    let result = execute_attempt_phases(
        &workflow,
        &admitted,
        &cancellation,
        &mut attempt,
        AttemptSettings {
            presentation_config: child_presentation_config(),
            leaf,
            hooks: (|_| Ok(()), WorkflowTerminalHost::start, || None),
            child: Some(presentation),
            child_recovery: None,
            journals: parent.journals.clone(),
        },
    )
    .await;
    if let Err(error) = result {
        // REQ-LOCALRUN-207, INV-ENGINE-33: a child whose state commit failed latches the
        // failure and its text on the parent's commit port before the parent's step can
        // settle from it.
        if let Some(StateCommitFailed { failure, text, .. }) =
            error.downcast_ref::<StateCommitFailed>()
        {
            let _ = parent.child_failure.set(ChildCommitFailure {
                failure: *failure,
                text: text.clone(),
            });
        }
        attempt.teardown().await;
        return ChildRunOutcome::Interrupted;
    }
    let outcome = match attempt.host.as_mut() {
        Some(ActiveRunHost::Quiet(host)) => host.outcome.take(),
        _ => None,
    }
    .unwrap_or(ChildRunOutcome::Interrupted);
    // REQ-LOCALRUN-191: the stage transition's `onStage`, before the parent routes it.
    if let ChildRunOutcome::Exited {
        outcome, exports, ..
    } = &outcome
    {
        let stage = match request.item {
            Some(item) => position_path(&[
                serde_json::Value::String(request.step.clone()),
                serde_json::json!({ "item": item }),
            ]),
            None => request.step.clone(),
        };
        let summary = match exports.get("summary") {
            Some(CapturedValue::Text(summary)) => Some(summary.as_ref().to_owned()),
            _ => None,
        };
        let outcome = outcome.clone();
        super::mirror::perform_blocking(
            parent.mirror.as_ref(),
            Box::new(move |mirror, target| {
                mirror.stage(target, &stage, &outcome, summary.as_deref())
            }),
        )
        .await;
    }
    outcome
}

/// The admission context of a child: the parent's execution root, limits and admitted
/// environment, and the parent's tree copy; a missing harness is the child's rejection code.
fn child_execution_context(
    parent: &LocalChildRunParent,
    workflow: &ResolvedWorkflow,
    tree: ValidatedTree,
    cancellation: CancellationSource,
) -> Result<ExecutionContext, String> {
    let (context, failures) = execution_context_with_profile_failures(
        workflow,
        parent.execution_root.clone(),
        parent.maximum_parallel_steps,
        cancellation,
        false,
        parent.environment.clone(),
    );
    if let Some(failure) = failures.first() {
        return Err(
            RejectionDiagnostic::from_agent_harness_installation(failure)
                .code
                .to_owned(),
        );
    }
    Ok(context.with_tree(tree))
}

fn child_admission_code(failure: &um_execution::AdmissionFailure) -> String {
    RejectionDiagnostic::from_admission(failure)
        .map_or("workflow_admission_invalid", |diagnostic| diagnostic.code)
        .to_owned()
}

/// Resolves the child file from the parent's tree copy, with the agent-profile replacement
/// a `control.retry` names for the step, admits it with the parent's execution root,
/// limits, tree and environment and the mapped inputs, and creates its run directory; a
/// refusal is the child's rejection code.
fn prepare_local_child(
    parent: &LocalChildRunParent,
    request: &ChildRunRequest,
    cancellation: CancellationSource,
) -> Result<(ResolvedWorkflow, AdmittedWorkflow, InitialLocalRun), String> {
    let replacement = parent
        .replacement
        .as_ref()
        .filter(|(step, _)| *step == request.step)
        .map(|(_, document)| document);
    let workflow = resolve_workflow_file_with_agent_profiles(
        &parent.copy_root,
        &parent.copy_root.join(&request.workflow),
        replacement,
    )
    .map_err(|failure| {
        RejectionDiagnostic::from_resolution(&failure)
            .code
            .to_owned()
    })?;
    let tree = validate_workflow_tree(&parent.copy_root).map_err(|diagnostics| {
        diagnostics
            .first()
            .map_or("tree_invalid", |diagnostic| diagnostic.code)
            .to_owned()
    })?;
    let inputs = child_request_inputs(request)?;
    let context = child_execution_context(parent, &workflow, tree, cancellation)?;
    let admitted = admit_local_workflow(workflow.clone(), inputs, context)
        .map_err(|failure| child_admission_code(&failure))?;
    // REQ-LOCALRUN-177: a fan-out element's child nests under its element.
    let record = WorkflowRunRecord::child_of(
        parent.run.record(),
        &request.step,
        &request.workflow,
        request.item,
    );
    let run = parent
        .run
        .create_child(&request.step, request.item, &admitted, &record)
        .map_err(|_| "child_run_directory_unavailable".to_owned())?;
    Ok((workflow, admitted, run))
}

/// A child request's mapped inputs as the child's resolved inputs; a JSON value that does not
/// round-trip is the child's `input_kind_mismatch`.
pub(super) fn child_request_inputs(request: &ChildRunRequest) -> Result<ResolvedInputs, String> {
    let mut inputs = BTreeMap::new();
    for (name, value) in &request.inputs {
        let input = match value {
            ChildInputValue::Text(text) => ResolvedInput::Text(Arc::clone(text)),
            ChildInputValue::Json(json) => ResolvedInput::Json(
                serde_json::to_vec(json.as_ref())
                    .ok()
                    .and_then(|source| ResolvedJsonInput::from_source(Arc::from(source)).ok())
                    .ok_or_else(|| "input_kind_mismatch".to_owned())?,
            ),
        };
        inputs.insert(name.clone(), input);
    }
    Ok(ResolvedInputs::new(inputs))
}

/// A new attempt of the child run at `directory` the parent's abandoned attempt left
/// running (REQ-CONTINUE-76), as `um workflow run retry` on it would start: its own retry
/// settles the abandoned attempt, and it runs its retained definition and inputs from its
/// unchanged tree copy under the parent's execution root, limits and environment.
fn prepare_child_retry(
    parent: &LocalChildRunParent,
    directory: &Path,
    cancellation: CancellationSource,
) -> Result<(ResolvedWorkflow, AdmittedWorkflow, LocalAttemptOwner), String> {
    let pending = match acquire_local_retry(directory) {
        Ok(LocalRetryOpen::Acquired(pending)) => *pending,
        Ok(LocalRetryOpen::Rejected(rejection)) => {
            return Err(rejection.reason().as_str().to_owned());
        }
        Err(_) => return Err("child_retry_unavailable".to_owned()),
    };
    let tree = match pending.verify_tree_copy() {
        Some(Ok(tree)) => tree,
        Some(Err(_)) => return Err("tree_copy_modified".to_owned()),
        None => return Err("workflow_step_requires_tree".to_owned()),
    };
    let (workflow, inputs, _) = pending.execution_specification();
    let (workflow, inputs) = (workflow.clone(), inputs.clone());
    let context = child_execution_context(parent, &workflow, tree, cancellation)?;
    let admitted = admit_local_workflow(workflow.clone(), inputs, context)
        .map_err(|failure| child_admission_code(&failure))?;
    let run = pending.begin(&admitted).map_err(|error| match error {
        LocalRetryBeginError::Rejected(rejection) => rejection.reason().as_str().to_owned(),
        LocalRetryBeginError::Operational(_) => "child_retry_unavailable".to_owned(),
    })?;
    Ok((workflow, admitted, run))
}

/// A child presents nothing; the configuration only satisfies the attempt's settings.
fn child_presentation_config() -> PresentationConfig {
    PresentationConfig {
        requested_mode: RequestedPresentationMode::Json,
        color: ColorChoice::Never,
        capabilities: TerminalCapabilities {
            stdin_is_terminal: false,
            stdout_is_terminal: false,
            stderr_is_terminal: false,
            stdout_width: None,
            stderr_width: None,
            term: None,
            no_color: None,
        },
        standard_input_reserved: true,
    }
}

#[derive(Debug)]
enum PlannedInput {
    TextInline(Arc<str>),
    TextFile(PathBuf),
    JsonInline(Arc<[u8]>),
    JsonFile(PathBuf),
    File(PlannedAttachment),
    Attachments(Vec<PlannedAttachment>),
}

#[derive(Debug)]
struct PlannedAttachment {
    media_type: Arc<str>,
    path: PathBuf,
}

#[derive(Debug)]
struct InputPlan {
    values: std::collections::BTreeMap<String, PlannedInput>,
    standard_input_reserved: bool,
}

fn plan_inputs(
    text_values: &[OsString],
    text_files: &[OsString],
    json_values: &[OsString],
    json_files: &[OsString],
    files: &[OsString],
    attachments: &[OsString],
    empty_attachments: &[String],
) -> anyhow::Result<InputPlan> {
    let mut values = std::collections::BTreeMap::new();
    for (name, text) in text_bindings(text_values)? {
        insert_scalar_input(&mut values, name, PlannedInput::TextInline(Arc::from(text)))?;
    }
    let text_file_bindings = text_files.chunks_exact(2);
    if !text_file_bindings.remainder().is_empty() {
        return Err(anyhow!("invalid --input-text-file binding"));
    }
    let mut standard_input_reserved = false;
    for binding in text_file_bindings {
        let name = input_argument(binding.first(), "input name")?;
        let path = PathBuf::from(binding.get(1).ok_or_else(|| anyhow!("missing Text path"))?);
        claim_standard_input(&path, &mut standard_input_reserved)?;
        insert_scalar_input(&mut values, name, PlannedInput::TextFile(path))?;
    }
    let inline = json_values.chunks_exact(2);
    if !inline.remainder().is_empty() {
        return Err(anyhow!("invalid --input-json binding"));
    }
    for binding in inline {
        let name = input_argument(binding.first(), "input name")?;
        let json = input_argument(binding.get(1), "JSON value")?;
        insert_scalar_input(
            &mut values,
            name,
            PlannedInput::JsonInline(Arc::from(json.as_bytes())),
        )?;
    }
    let json_file_bindings = json_files.chunks_exact(2);
    if !json_file_bindings.remainder().is_empty() {
        return Err(anyhow!("invalid --input-json-file binding"));
    }
    for binding in json_file_bindings {
        let name = input_argument(binding.first(), "input name")?;
        let path = PathBuf::from(binding.get(1).ok_or_else(|| anyhow!("missing JSON path"))?);
        claim_standard_input(&path, &mut standard_input_reserved)?;
        insert_scalar_input(&mut values, name, PlannedInput::JsonFile(path))?;
    }
    let scalar_files = files.chunks_exact(3);
    if !scalar_files.remainder().is_empty() {
        return Err(anyhow!("invalid --input-file binding"));
    }
    for binding in scalar_files {
        let name = input_argument(binding.first(), "input name")?;
        let media_type = input_argument(binding.get(1), "File media type")?;
        let path = PathBuf::from(binding.get(2).ok_or_else(|| anyhow!("missing File path"))?);
        insert_scalar_input(
            &mut values,
            name,
            PlannedInput::File(PlannedAttachment {
                media_type: Arc::from(media_type),
                path,
            }),
        )?;
    }
    let members = attachments.chunks_exact(3);
    if !members.remainder().is_empty() || members.len() > MAXIMUM_ATTACHMENTS {
        return Err(anyhow!("invalid named attachment bindings"));
    }
    for binding in members {
        let name = input_argument(binding.first(), "input name")?;
        let media_type = input_argument(binding.get(1), "attachment media type")?;
        let path = PathBuf::from(
            binding
                .get(2)
                .ok_or_else(|| anyhow!("missing attachment path"))?,
        );
        match values.entry(name.to_owned()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(PlannedInput::Attachments(vec![PlannedAttachment {
                    media_type: Arc::from(media_type),
                    path,
                }]));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let PlannedInput::Attachments(items) = entry.get_mut() else {
                    return Err(anyhow!("one input name has incompatible bindings"));
                };
                items.push(PlannedAttachment {
                    media_type: Arc::from(media_type),
                    path,
                });
            }
        }
    }
    for name in empty_attachments {
        validate_input_name(name)?;
        insert_scalar_input(&mut values, name, PlannedInput::Attachments(Vec::new()))?;
    }
    if values.len() > MAXIMUM_INPUTS {
        return Err(anyhow!("named input count exceeds {MAXIMUM_INPUTS}"));
    }
    Ok(InputPlan {
        values,
        standard_input_reserved,
    })
}

fn claim_standard_input(path: &Path, reserved: &mut bool) -> anyhow::Result<()> {
    if path != Path::new("-") {
        return Ok(());
    }
    if *reserved {
        return Err(anyhow!(
            "standard input may supply only one named Text or JSON input"
        ));
    }
    *reserved = true;
    Ok(())
}

/// The `--input-text NAME TEXT` pairs as given (REQ-LOCALRUN-80): an odd count, a name or
/// value that is not UTF-8 or a name outside the input-name form is a usage error.
pub(super) fn text_bindings(values: &[OsString]) -> anyhow::Result<Vec<(&str, &str)>> {
    let pairs = values.chunks_exact(2);
    if !pairs.remainder().is_empty() {
        return Err(anyhow!("invalid --input-text binding"));
    }
    pairs
        .map(|binding| {
            Ok((
                input_argument(binding.first(), "input name")?,
                input_argument(binding.get(1), "Text value")?,
            ))
        })
        .collect()
}

fn input_argument<'a>(value: Option<&'a OsString>, description: &str) -> anyhow::Result<&'a str> {
    let value = value
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("{description} is not valid UTF-8"))?;
    if description == "input name" {
        validate_input_name(value)?;
    }
    Ok(value)
}

fn validate_input_name(name: &str) -> anyhow::Result<()> {
    if !um_execution::is_input_name(name) {
        return Err(anyhow!("invalid Workflow V1 input name"));
    }
    Ok(())
}

fn insert_scalar_input(
    values: &mut std::collections::BTreeMap<String, PlannedInput>,
    name: &str,
    input: PlannedInput,
) -> anyhow::Result<()> {
    validate_input_name(name)?;
    if values.insert(name.to_owned(), input).is_some() {
        return Err(anyhow!(
            "one input name has duplicate or incompatible bindings"
        ));
    }
    Ok(())
}

async fn acquire_inputs(
    plan: &InputPlan,
    cancellation: &CancellationSource,
) -> anyhow::Result<ResolvedInputs> {
    let mut total_bytes = 0_u64;
    let mut values = std::collections::BTreeMap::new();
    for (name, input) in &plan.values {
        if cancellation.is_cancelled() {
            return Err(input_error(
                InputAcquisitionFailureKind::Interrupted,
                name,
                None,
            ));
        }
        let value = match input {
            PlannedInput::TextInline(text) => {
                account_input_bytes(&mut total_bytes, text.len() as u64, MAXIMUM_TEXT_BYTES)?;
                ResolvedInput::Text(Arc::clone(text))
            }
            PlannedInput::TextFile(path) => {
                let bytes = acquire_scalar_file(path, name, &mut total_bytes, cancellation).await?;
                let text = String::from_utf8(bytes).map(Arc::from).map_err(|_| {
                    input_error(InputAcquisitionFailureKind::InvalidUtf8, name, Some(path))
                })?;
                ResolvedInput::Text(text)
            }
            PlannedInput::JsonInline(source) => {
                account_input_bytes(
                    &mut total_bytes,
                    u64::try_from(source.len()).map_err(|_| input_bytes_error())?,
                    MAXIMUM_TEXT_BYTES,
                )?;
                ResolvedInput::Json(ResolvedJsonInput::from_source(Arc::clone(source)).map_err(
                    |_| input_error(InputAcquisitionFailureKind::InvalidJson, name, None),
                )?)
            }
            PlannedInput::JsonFile(path) => {
                let bytes = acquire_scalar_file(path, name, &mut total_bytes, cancellation).await?;
                ResolvedInput::Json(ResolvedJsonInput::from_source(Arc::from(bytes)).map_err(
                    |_| input_error(InputAcquisitionFailureKind::InvalidJson, name, Some(path)),
                )?)
            }
            PlannedInput::File(item) => {
                let file = open_regular_input(&item.path)
                    .map_err(|kind| input_error(kind, name, Some(&item.path)))?;
                let bytes = read_bounded(
                    file,
                    remaining_input_bytes(total_bytes, MAXIMUM_ATTACHMENT_BYTES),
                    cancellation,
                )
                .map_err(|kind| input_error(kind, name, Some(&item.path)))?;
                account_input_bytes(
                    &mut total_bytes,
                    u64::try_from(bytes.len()).map_err(|_| input_bytes_error())?,
                    MAXIMUM_ATTACHMENT_BYTES,
                )?;
                ResolvedInput::File(ResolvedFile::new(
                    Arc::clone(&item.media_type),
                    Arc::from(bytes),
                ))
            }
            PlannedInput::Attachments(items) => {
                let mut resolved = Vec::with_capacity(items.len());
                for item in items {
                    let file = open_regular_input(&item.path)
                        .map_err(|kind| input_error(kind, name, Some(&item.path)))?;
                    let bytes = read_bounded(
                        file,
                        remaining_input_bytes(total_bytes, MAXIMUM_ATTACHMENT_BYTES),
                        cancellation,
                    )
                    .map_err(|kind| input_error(kind, name, Some(&item.path)))?;
                    account_input_bytes(
                        &mut total_bytes,
                        u64::try_from(bytes.len()).map_err(|_| input_bytes_error())?,
                        MAXIMUM_ATTACHMENT_BYTES,
                    )?;
                    resolved.push(ResolvedAttachment::new(
                        Arc::clone(&item.media_type),
                        Arc::from(bytes),
                    ));
                }
                ResolvedInput::Attachments(Arc::from(resolved))
            }
        };
        values.insert(name.clone(), value);
    }
    Ok(ResolvedInputs::new(values))
}

async fn acquire_scalar_file(
    path: &Path,
    name: &str,
    total_bytes: &mut u64,
    cancellation: &CancellationSource,
) -> anyhow::Result<Vec<u8>> {
    let bytes = if path == Path::new("-") {
        read_stdin_bounded(
            remaining_input_bytes(*total_bytes, MAXIMUM_TEXT_BYTES),
            cancellation,
        )
        .await
        .map_err(|kind| input_error(kind, name, None))?
    } else {
        let file = open_regular_input(path).map_err(|kind| input_error(kind, name, Some(path)))?;
        read_bounded(
            file,
            remaining_input_bytes(*total_bytes, MAXIMUM_TEXT_BYTES),
            cancellation,
        )
        .map_err(|kind| input_error(kind, name, Some(path)))?
    };
    account_input_bytes(
        total_bytes,
        u64::try_from(bytes.len()).map_err(|_| input_bytes_error())?,
        MAXIMUM_TEXT_BYTES,
    )?;
    Ok(bytes)
}

fn remaining_input_bytes(total: u64, per_value_limit: u64) -> u64 {
    per_value_limit.min(MAXIMUM_TOTAL_INPUT_BYTES.saturating_sub(total))
}

fn account_input_bytes(total: &mut u64, size: u64, per_value_limit: u64) -> anyhow::Result<()> {
    if size > per_value_limit {
        return Err(input_bytes_error());
    }
    *total = total
        .checked_add(size)
        .filter(|total| *total <= MAXIMUM_TOTAL_INPUT_BYTES)
        .ok_or_else(input_bytes_error)?;
    Ok(())
}

fn input_error(
    kind: InputAcquisitionFailureKind,
    name: &str,
    path: Option<&Path>,
) -> anyhow::Error {
    let context = path.map_or_else(
        || format!("acquire local workflow input {name}"),
        |path| format!("acquire local workflow input {name} from {path:?}"),
    );
    anyhow::Error::new(kind).context(context)
}

fn input_bytes_error() -> anyhow::Error {
    anyhow!("acquire local workflow inputs: an input byte bound was exceeded")
}

/// Acquire the exact bytes of an agent profile replacement document under the Local
/// Workflow Run CLI section 3.6.3 rules. Failures name the cause but never the path.
pub(in crate::cli::workflow) fn acquire_agent_profile_replacement(
    path: &Path,
    cancellation: &CancellationSource,
) -> Result<AgentProfileReplacementDocument, super::super::super::CommandFailure> {
    let acquire = || -> Result<AgentProfileReplacementDocument, InputAcquisitionFailureKind> {
        let file = open_regular_input(path)?;
        let bytes = read_bounded(file, MAXIMUM_AGENT_PROFILE_REPLACEMENT_BYTES, cancellation)?;
        if std::str::from_utf8(&bytes).is_err() {
            return Err(InputAcquisitionFailureKind::InvalidUtf8);
        }
        Ok(AgentProfileReplacementDocument::new(bytes))
    };
    acquire().map_err(|kind| {
        let error = anyhow!("agent profile replacement document: {kind}");
        match kind {
            InputAcquisitionFailureKind::Interrupted => {
                signalled_failure(error, cancellation.cancellation_reason())
            }
            _ => error.into(),
        }
    })
}

fn open_regular_input(path: &Path) -> Result<File, InputAcquisitionFailureKind> {
    super::super::super::open_regular_file_nonblocking(path).map_err(|error| match error {
        super::super::super::OpenRegularFileError::NotRegular => {
            InputAcquisitionFailureKind::NotRegularFile
        }
        super::super::super::OpenRegularFileError::Open(_)
        | super::super::super::OpenRegularFileError::Metadata(_) => {
            InputAcquisitionFailureKind::Unavailable
        }
    })
}

fn read_bounded(
    mut reader: impl Read,
    maximum: u64,
    cancellation: &CancellationSource,
) -> Result<Vec<u8>, InputAcquisitionFailureKind> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if cancellation.is_cancelled() {
            return Err(InputAcquisitionFailureKind::Interrupted);
        }
        let remaining = maximum.saturating_sub(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let permitted = usize::try_from(remaining.saturating_add(1))
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        match reader.read(&mut buffer[..permitted]) {
            Ok(0) => return Ok(bytes),
            Ok(read) => {
                bytes.extend_from_slice(&buffer[..read]);
                if u64::try_from(bytes.len()).map_or(true, |length| length > maximum) {
                    return Err(InputAcquisitionFailureKind::TooLarge);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return Err(InputAcquisitionFailureKind::Read),
        }
    }
}

async fn read_stdin_bounded(
    maximum: u64,
    cancellation: &CancellationSource,
) -> Result<Vec<u8>, InputAcquisitionFailureKind> {
    read_stdin_from(&io::stdin(), maximum, cancellation).await
}

async fn read_stdin_from(
    standard_input: &impl AsFd,
    maximum: u64,
    cancellation: &CancellationSource,
) -> Result<Vec<u8>, InputAcquisitionFailureKind> {
    let input = rustix::io::dup(standard_input.as_fd())
        .map_err(|_| InputAcquisitionFailureKind::Unavailable)?;
    let original_flags =
        fcntl_getfl(standard_input).map_err(|_| InputAcquisitionFailureKind::Unavailable)?;
    fcntl_setfl(standard_input, original_flags | OFlags::NONBLOCK)
        .map_err(|_| InputAcquisitionFailureKind::Unavailable)?;
    let flags = StdinFlags {
        input: &standard_input,
        original_flags,
    };
    let input = File::from(input);
    let async_input = match AsyncFd::new(input) {
        Ok(input) => input,
        Err(_) => {
            flags
                .restore()
                .map_err(|_| InputAcquisitionFailureKind::Read)?;
            let input = rustix::io::dup(standard_input.as_fd())
                .map_err(|_| InputAcquisitionFailureKind::Unavailable)?;
            return read_bounded(File::from(input), maximum, cancellation);
        }
    };
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    let result = loop {
        if cancellation.is_cancelled() {
            break Err(InputAcquisitionFailureKind::Interrupted);
        }
        let remaining = maximum.saturating_sub(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let permitted = usize::try_from(remaining.saturating_add(1))
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let mut ready = tokio::select! {
            biased;
            _ = cancellation.wait_for_cancellation() => {
                break Err(InputAcquisitionFailureKind::Interrupted);
            }
            ready = async_input.readable() => match ready {
                Ok(ready) => ready,
                Err(_) => break Err(InputAcquisitionFailureKind::Read),
            }
        };
        match ready.try_io(|inner| inner.get_ref().read(&mut buffer[..permitted])) {
            Ok(Ok(0)) => break Ok(bytes),
            Ok(Ok(read)) => {
                bytes.extend_from_slice(&buffer[..read]);
                if u64::try_from(bytes.len()).map_or(true, |length| length > maximum) {
                    break Err(InputAcquisitionFailureKind::TooLarge);
                }
            }
            Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
            Ok(Err(_)) => break Err(InputAcquisitionFailureKind::Read),
            Err(_) => {}
        }
    };
    drop(async_input);
    flags
        .restore()
        .map_err(|_| InputAcquisitionFailureKind::Read)?;
    result
}

struct StdinFlags<'a, T: AsFd> {
    input: &'a T,
    original_flags: OFlags,
}

impl<T: AsFd> StdinFlags<'_, T> {
    fn restore(&self) -> rustix::io::Result<()> {
        fcntl_setfl(self.input, self.original_flags)
    }
}

impl<T: AsFd> Drop for StdinFlags<'_, T> {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

pub(super) struct UnixSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    wake_up: tokio::signal::unix::Signal,
}

impl UnixSignals {
    pub(super) fn new() -> anyhow::Result<Self> {
        Ok(Self {
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .context("install local workflow interrupt observation")?,
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("install local workflow termination observation")?,
            wake_up: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(
                libc::SIGURG,
            ))
            .context("install local workflow wake-up observation")?,
        })
    }
}

/// What the command's signal observer receives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SignalEvent {
    /// SIGINT (`UserRequest`) or SIGTERM (`TerminationRequest`).
    Signal(CancellationReason),
    /// SIGURG, which carries no request of its own: the observer reads the watched control
    /// journals for a new `control.interrupt` (REQ-WFDEF-6).
    WakeUp,
}

pub(super) trait SignalEvents: Send + 'static {
    fn next(&mut self) -> impl Future<Output = Option<SignalEvent>> + Send;
}

impl SignalEvents for UnixSignals {
    async fn next(&mut self) -> Option<SignalEvent> {
        tokio::select! {
            biased;
            signal = self.interrupt.recv() => {
                signal.map(|()| SignalEvent::Signal(CancellationReason::UserRequest))
            }
            signal = self.terminate.recv() => {
                signal.map(|()| SignalEvent::Signal(CancellationReason::TerminationRequest))
            }
            signal = self.wake_up.recv() => signal.map(|()| SignalEvent::WakeUp),
        }
    }
}

/// The run directories whose control journals the observer reads, a signal for a new
/// `control.cancel` (REQ-LOCALRUN-135) and a wake-up for a new `control.interrupt`
/// (REQ-WFDEF-6): the run the command owns once its directory is known, and each child run
/// an attempt of it is executing. Empty before the run directory exists. It also holds the
/// `control.interrupt` whose interruption the observer requested, for the wait payload.
#[derive(Clone, Default)]
pub(super) struct ControlJournals(Arc<std::sync::Mutex<JournalSlot>>);

#[derive(Default)]
struct JournalSlot {
    next_watch: u64,
    watched: Vec<Watched>,
    interrupt: Option<ControlEvent>,
}

/// One watched run directory and the highest control ordinal its journal held when it was
/// watched, `None` when the journal could not be read then, so that no control in it is new.
#[derive(Clone)]
struct Watched {
    id: u64,
    directory: PathBuf,
    taken: Option<u64>,
}

impl ControlJournals {
    /// Adds `directory` until the returned guard is dropped, noting the controls it already
    /// holds, which no signal and no wake-up acts on.
    fn watch(&self, directory: &Path) -> WatchedJournal {
        let taken = read_numbered_controls(directory)
            .ok()
            .map(|controls| controls.last().map_or(0, |(ordinal, _)| *ordinal));
        let id = self.0.lock().ok().map(|mut slot| {
            let id = slot.next_watch;
            slot.next_watch = slot.next_watch.wrapping_add(1);
            slot.watched.push(Watched {
                id,
                directory: directory.to_owned(),
                taken,
            });
            id
        });
        WatchedJournal {
            journals: self.clone(),
            id,
        }
    }

    fn watched(&self) -> Vec<Watched> {
        self.0
            .lock()
            .map(|slot| slot.watched.clone())
            .unwrap_or_default()
    }

    /// Requests the interruption `control` asks for (REQ-WFDEF-6) and, when it is accepted
    /// as an interruption, keeps `control` for the wait payload. Under the slot's lock, so
    /// that a wait entered on the interruption finds it.
    fn interrupt(&self, cancellation: &CancellationSource, control: ControlEvent) {
        let Ok(mut slot) = self.0.lock() else {
            return;
        };
        if cancellation.request_interruption(CancellationReason::TerminationRequest)
            && cancellation.interruption_requested()
        {
            slot.interrupt = Some(control);
        }
    }

    /// The `control.interrupt` whose interruption the observer requested, if any.
    fn interrupting_control(&self) -> Option<ControlEvent> {
        self.0.lock().ok().and_then(|slot| slot.interrupt.clone())
    }
}

/// A run directory `ControlJournals` holds while it lives.
pub(super) struct WatchedJournal {
    journals: ControlJournals,
    id: Option<u64>,
}

impl Drop for WatchedJournal {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.journals.0.lock()
            && let Some(index) = slot
                .watched
                .iter()
                .position(|watched| Some(watched.id) == self.id)
        {
            slot.watched.remove(index);
        }
    }
}

/// The command's signal observer and the control journals it reads.
pub(super) struct SignalObservation {
    task: tokio::task::JoinHandle<()>,
    journals: ControlJournals,
}

impl SignalObservation {
    pub(super) fn abort(&self) {
        self.task.abort();
    }

    /// Reads `directory`'s control journal on each signal until the guard is dropped.
    pub(super) fn watch(&self, directory: &Path) -> WatchedJournal {
        self.journals.watch(directory)
    }
}

/// What a signal asks of the run's cancellation source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignalRequest {
    /// No run exists yet, or the run's or its live child's journal holds a `control.cancel`
    /// recorded since this process took that run.
    Cancel,
    /// Any other signal to a process that owns a run (REQ-LOCALRUN-135, REQ-WFEXEC-69).
    Interrupt,
}

pub(super) fn start_signal_observation(
    cancellation: CancellationSource,
    mut signals: impl SignalEvents,
) -> SignalObservation {
    let journals = ControlJournals::default();
    let observed = journals.clone();
    let task = tokio::spawn(async move {
        while let Some(event) = signals.next().await {
            match event {
                SignalEvent::Signal(reason) => {
                    let request = signal_request(&observed).await;
                    if handle_observed_signal(&cancellation, reason, request) {
                        return;
                    }
                }
                // REQ-WFDEF-6: a wake-up acts only on the journal; a second one, or one that
                // finds no new `control.interrupt`, changes nothing.
                SignalEvent::WakeUp => {
                    if let Some(control) = recorded_interrupt(&observed).await {
                        observed.interrupt(&cancellation, control);
                    }
                }
            }
        }
    });
    SignalObservation { task, journals }
}

/// A cancel when no run directory is known yet or a watched journal holds a new
/// `control.cancel`, which `um workflow run cancel` appends before it signals
/// (REQ-LOCALRUN-186); otherwise an interruption (REQ-LOCALRUN-135).
async fn signal_request(journals: &ControlJournals) -> SignalRequest {
    let watched = journals.watched();
    if watched.is_empty() || recorded_since(watched, CONTROL_CANCEL).await.is_some() {
        SignalRequest::Cancel
    } else {
        SignalRequest::Interrupt
    }
}

/// The new `control.interrupt` a wake-up acts on (REQ-WFDEF-6).
async fn recorded_interrupt(journals: &ControlJournals) -> Option<ControlEvent> {
    recorded_since(journals.watched(), CONTROL_INTERRUPT).await
}

/// The first `event` control one of the `watched` journals holds above the ordinal it held
/// when it was watched. A control recorded before this process took the run was an earlier
/// attempt's, acted on or ignored then, and never cancels or interrupts this one
/// (REQ-LOCALRUN-135, REQ-WFDEF-6). A journal that cannot be read holds none.
async fn recorded_since(watched: Vec<Watched>, event: &'static str) -> Option<ControlEvent> {
    blocking_operation(move || {
        Ok::<_, std::convert::Infallible>(watched.into_iter().find_map(|watched| {
            let taken = watched.taken?;
            read_numbered_controls(&watched.directory)
                .ok()?
                .into_iter()
                .find(|(ordinal, control)| *ordinal > taken && control.event == event)
                .map(|(_, control)| control)
        }))
    })
    .await
    .unwrap_or(None)
}

/// Requests the stop `request` names; a signal that changes nothing then force-aborts a
/// cancelling finalization (REQ-WFFINALIZE-15), which ends the observation.
fn handle_observed_signal(
    cancellation: &CancellationSource,
    reason: CancellationReason,
    request: SignalRequest,
) -> bool {
    let requested = match request {
        SignalRequest::Cancel => cancellation.request_cancellation(reason),
        SignalRequest::Interrupt => cancellation.request_interruption(reason),
    };
    if requested {
        return false;
    }
    cancellation.finalization_cancellation_requested() && cancellation.request_force_abort()
}

#[derive(Clone, Copy, Debug)]
struct ExecutionInstant {
    monotonic: Instant,
    utc: OffsetDateTime,
}

impl Add<Duration> for ExecutionInstant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self {
            monotonic: self.monotonic + duration,
            utc: self.utc + duration,
        }
    }
}

impl DisplayDeadline for ExecutionInstant {
    fn deadline_utc(&self) -> OffsetDateTime {
        self.utc
    }
}

impl DurableDeadline for ExecutionInstant {
    fn deadline_utc(&self) -> OffsetDateTime {
        self.utc
    }
}

#[derive(Clone, Copy)]
struct SystemExecutionClock;

impl CoordinatorClock for SystemExecutionClock {
    type Instant = ExecutionInstant;

    fn now(&mut self) -> Self::Instant {
        let point = SystemObservationClock.sample();
        ExecutionInstant {
            monotonic: point.monotonic,
            utc: point.utc,
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "SystemExecutionClock is the workflow adapter boundary for deadline waits"
    )]
    fn wait_until(&self, deadline: Self::Instant) -> impl Future<Output = ()> + Send {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.monotonic))
    }
}

type SystemPresentation = WorkflowRunPresentation<io::Stdout, io::Stderr, SystemObservationClock>;

#[derive(Clone)]
enum RunExecutionObserver {
    Standard(TimingObserver<SystemPresentation, SystemObservationClock>),
    Tui(WorkflowRunViewModel<SystemObservationClock>),
    Quiet(TimingObserver<ChildPresentation, SystemObservationClock>),
}

impl RunExecutionObserver {
    fn snapshot(&self) -> RunTimingSnapshot {
        match self {
            Self::Standard(observer) => observer.snapshot(),
            Self::Tui(view) => view.timing_observation().snapshot(),
            Self::Quiet(observer) => observer.snapshot(),
        }
    }
}

/// The presentation of a child run (REQ-WFEXEC-59): silent under a parent that presents
/// through the terminal interface, otherwise forwarding the child's step state records into
/// the parent's progress stream (REQ-LOCALRUN-200, INV-ENGINE-28).
#[derive(Clone)]
enum ChildPresentation {
    Silent,
    Forwarding(ChildRunPresentation<io::Stdout, io::Stderr, SystemObservationClock>),
}

impl ExecutionObserver<ExecutionInstant> for ChildPresentation {
    fn observe(
        &self,
        observation: ExecutionObservation<ExecutionInstant>,
    ) -> impl Future<Output = ()> + Send {
        let forwarded = match self {
            Self::Silent => None,
            Self::Forwarding(presentation) => Some(presentation.observe(observation)),
        };
        async move {
            if let Some(forwarded) = forwarded {
                forwarded.await;
            }
        }
    }
}

/// A child never fails on its parent's stream by itself: the parent's failure watcher
/// cancels the parent with `caller_output_failure`, and that cancels the child
/// (REQ-LOCALRUN-137).
impl PresentationFailureState for ChildPresentation {
    fn presentation_failed(&self) -> bool {
        false
    }
}

/// A child run's host: it presents nothing and keeps how the child ended for the parent's
/// step, which a failed publication turns into an interrupted child (REQ-WFEXEC-60).
#[derive(Default)]
struct QuietRunHost {
    outcome: Option<ChildRunOutcome>,
}

impl ExecutionObserver<ExecutionInstant> for RunExecutionObserver {
    fn observe(
        &self,
        observation: ExecutionObservation<ExecutionInstant>,
    ) -> impl Future<Output = ()> + Send {
        let observer = self.clone();
        async move {
            match observer {
                Self::Standard(observer) => observer.observe(observation).await,
                Self::Tui(view) => view.observe(observation).await,
                Self::Quiet(observer) => observer.observe(observation).await,
            }
        }
    }
}

enum ActiveRunHost {
    Standard(SystemPresentation),
    Quiet(QuietRunHost),
    Tui {
        view: WorkflowRunViewModel<SystemObservationClock>,
        terminal: Option<WorkflowTerminalHost>,
        failure: Option<PresentationFailure>,
        config: Box<PresentationConfig>,
        leaf: ExecutionLeaf,
    },
}

impl ActiveRunHost {
    async fn await_ready(&mut self) -> Result<(), PresentationFailure> {
        match self {
            Self::Standard(presentation) => {
                // Let an immediately failing header reject the attempt before execution.
                // A stalled output consumer must not hold up the workflow indefinitely.
                let mut clock = SystemExecutionClock;
                let deadline = clock.now() + Duration::from_millis(100);
                tokio::select! {
                    () = presentation.flush_pending() => {}
                    () = clock.wait_until(deadline) => {}
                }
                presentation.failure().map_or(Ok(()), Err)
            }
            Self::Tui {
                terminal: Some(terminal),
                ..
            } => terminal.await_ready().await,
            Self::Tui { terminal: None, .. } => Err(PresentationFailure::operation(
                PresentationFailureOperation::TerminalTask,
            )),
            Self::Quiet(_) => Ok(()),
        }
    }

    fn activate_execution(&mut self) -> Result<(), PresentationFailure> {
        match self {
            Self::Standard(_) | Self::Quiet(_) => Ok(()),
            Self::Tui { terminal, .. } => terminal.as_mut().map_or_else(
                || {
                    Err(PresentationFailure::operation(
                        PresentationFailureOperation::TerminalTask,
                    ))
                },
                WorkflowTerminalHost::activate_execution,
            ),
        }
    }

    async fn stop_terminal(&mut self) {
        if let Self::Tui {
            terminal, failure, ..
        } = self
            && let Some(active) = terminal.take()
            && let Err(terminal_failure) = active.stop().await
            && failure.is_none()
        {
            *failure = Some(terminal_failure);
        }
    }

    fn terminal_failure(&self) -> Option<PresentationFailure> {
        match self {
            Self::Tui { failure, .. } => failure.clone(),
            Self::Standard(_) | Self::Quiet(_) => None,
        }
    }

    fn reconcile_and_mark_quiescent(&self, run: &WorkflowRunResult) -> anyhow::Result<()> {
        if let Self::Tui { view, .. } = self {
            view.reconcile_terminal_result(run)
                .map_err(|_| anyhow!("prepare authoritative local workflow terminal result"))?;
            view.mark_quiescent();
        }
        Ok(())
    }

    fn begin_publication(&self) {
        if let Self::Tui { view, .. } = self {
            view.begin_publication();
        }
    }

    fn complete_publication(
        &self,
        publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
    ) {
        if let Self::Tui { view, .. } = self {
            let result = match publication {
                Ok(terminal) => WorkflowRunPublicationResult::Succeeded {
                    result_directory: terminal.result_directory().to_owned(),
                },
                Err(error) => WorkflowRunPublicationResult::Failed(error.into()),
            };
            view.complete_publication(result);
        }
    }

    fn begin_cleanup(&self) {
        if let Self::Tui { view, .. } = self {
            view.begin_cleanup();
        }
    }

    fn complete_cleanup(&self, failed: bool) {
        if let Self::Tui { view, .. } = self {
            view.complete_cleanup(if failed {
                WorkflowRunCleanupResult::Failed
            } else {
                WorkflowRunCleanupResult::Succeeded
            });
        }
    }

    fn mark_adapter_lifecycle_completed(&self, _released_ownership: LocalAttemptOwnershipReleased) {
        if let Self::Tui { view, .. } = self {
            view.mark_adapter_lifecycle_completed();
        }
    }

    async fn finish(
        &mut self,
        workflow: &ResolvedWorkflow,
        run: &WorkflowRunResult,
        publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
        cleanup_failed: bool,
        state_commit_failed: bool,
    ) -> super::super::super::CommandResult {
        match self {
            Self::Quiet(host) => {
                if publication.is_err() {
                    host.outcome = None;
                }
                Ok(ExitCode::Success)
            }
            Self::Standard(presentation) => {
                if cleanup_failed || state_commit_failed {
                    if render_without_terminal_json(presentation, run, publication)
                        .await
                        .cannot_report_failure()
                    {
                        // A blocked presentation stream may also be stderr. Do not
                        // render a second, unbounded diagnostic on that stream.
                        return Ok(ExitCode::GeneralFailure);
                    }
                    return Err(if state_commit_failed {
                        state_commit_failure()
                    } else {
                        cleanup_failure(publication)
                    }
                    .into());
                }
                let presented = match publication {
                    Ok(terminal) => {
                        present_standard(
                            presentation,
                            run,
                            PublicationPresentation::Published(terminal),
                            true,
                        )
                        .await
                    }
                    Err(error) => {
                        present_standard(
                            presentation,
                            run,
                            PublicationPresentation::Failed(error),
                            true,
                        )
                        .await
                    }
                };
                match presented {
                    StandardPresentation::DrainTimedOut => Ok(ExitCode::GeneralFailure),
                    StandardPresentation::Completed(WorkflowRunPresentationResult::Failed(
                        failure,
                    )) if failure.error_kind == Some(io::ErrorKind::WouldBlock) => {
                        Ok(ExitCode::GeneralFailure)
                    }
                    StandardPresentation::Completed(presented) => presentation_exit_code(presented),
                }
            }
            Self::Tui {
                terminal,
                failure,
                config,
                leaf,
                ..
            } => {
                if let Some(active) = terminal.take() {
                    match active.wait().await {
                        Ok(TerminalHostExit::Quit) => {}
                        Ok(TerminalHostExit::Stopped) => {
                            if failure.is_none() {
                                *failure = Some(PresentationFailure {
                                    operation: PresentationFailureOperation::TerminalTask,
                                    error_kind: None,
                                    result_directory: None,
                                    panic_message: None,
                                });
                            }
                        }
                        Err(terminal_failure) => {
                            if failure.is_none() {
                                *failure = Some(terminal_failure);
                            }
                        }
                    }
                }
                if let Some(mut terminal_failure) = failure.clone() {
                    terminal_failure.result_directory = publication
                        .as_ref()
                        .ok()
                        .map(|terminal| terminal.result_directory().to_owned());
                    let mut error = anyhow::Error::new(terminal_failure);
                    if let Err(publication_error) = publication {
                        error = error.context(publication_error.to_string());
                    }
                    if state_commit_failed {
                        error = error.context("commit terminal local run state");
                    } else if cleanup_failed {
                        error = error.context(cleanup_failure_message(publication));
                    }
                    return Err(error.into());
                }

                let output =
                    WorkflowRunOutput::new(config.as_ref().clone(), io::stdout(), io::stderr());
                let output = match *leaf {
                    ExecutionLeaf::Run => output,
                    ExecutionLeaf::Retry => output.for_retry(&run.run_directory),
                    ExecutionLeaf::Resume => output.for_resume(&run.run_directory),
                    ExecutionLeaf::Restart => output.for_restart(&run.run_directory),
                    ExecutionLeaf::Upgrade => output.for_upgrade(&run.run_directory),
                };
                let presented = match publication {
                    Ok(terminal) => output.render_standard_summary(
                        workflow,
                        run,
                        PublicationPresentation::Published(terminal),
                    ),
                    Err(error) => output.render_standard_summary(
                        workflow,
                        run,
                        PublicationPresentation::Failed(error),
                    ),
                };
                if state_commit_failed {
                    Err(state_commit_failure().into())
                } else if cleanup_failed {
                    Err(cleanup_failure(publication).into())
                } else {
                    presentation_exit_code(presented)
                }
            }
        }
    }
}

enum StandardPresentation {
    Completed(WorkflowRunPresentationResult),
    DrainTimedOut,
}

impl StandardPresentation {
    fn cannot_report_failure(&self) -> bool {
        // An exhausted queue or timed-out drain may have stderr blocked too.
        matches!(self, Self::DrainTimedOut)
            || matches!(self, Self::Completed(WorkflowRunPresentationResult::Failed(failure))
                if failure.error_kind == Some(io::ErrorKind::WouldBlock))
    }
}

async fn present_standard(
    presentation: &SystemPresentation,
    run: &WorkflowRunResult,
    publication: PublicationPresentation<'_>,
    emit_terminal_json: bool,
) -> StandardPresentation {
    let output = async {
        if emit_terminal_json {
            presentation.finish(run, publication).await
        } else {
            presentation
                .finish_without_terminal_json(run, publication)
                .await
        }
    };
    if run.cancellation.is_none()
        && run.force_abort.is_none()
        && !run.finalization.as_ref().is_some_and(|finalization| {
            finalization.cancellation.is_some() || finalization.force_abort
        })
    {
        return StandardPresentation::Completed(output.await);
    }
    // The workflow clock adapter bounds the final output wait for cancellation
    // in either phase, including force-aborted finalizers;
    // the presentation thread may be stuck in an unread pipe indefinitely.
    let mut clock = SystemExecutionClock;
    let deadline = clock.now() + Duration::from_millis(100);
    tokio::select! {
        presented = output => StandardPresentation::Completed(presented),
        () = clock.wait_until(deadline) => StandardPresentation::DrainTimedOut,
    }
}

async fn render_without_terminal_json(
    presentation: &SystemPresentation,
    run: &WorkflowRunResult,
    publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
) -> StandardPresentation {
    let publication = match publication {
        Ok(terminal) => PublicationPresentation::Published(terminal),
        Err(error) => PublicationPresentation::Failed(error),
    };
    present_standard(presentation, run, publication, false).await
}

fn cleanup_failure(
    publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
) -> anyhow::Error {
    anyhow!(cleanup_failure_message(publication))
}

fn cleanup_failure_message(
    publication: &Result<WorkflowRunTerminalResultV1, LocalPublicationError>,
) -> String {
    publication.as_ref().map_or_else(
        |_| "release private workflow staging".to_owned(),
        |terminal| {
            format!(
                "release private workflow staging; result published at {}",
                terminal.result_directory()
            )
        },
    )
}

fn state_commit_failure() -> anyhow::Error {
    anyhow!("commit terminal local run state")
}

trait PresentationFailureState: Clone + Send + Sync + 'static {
    fn presentation_failed(&self) -> bool;
}

impl PresentationFailureState for SystemPresentation {
    fn presentation_failed(&self) -> bool {
        self.failure().is_some()
    }
}

#[derive(Clone)]
struct TimingObserver<Presentation, Clock> {
    presentation: Presentation,
    cancellation: CancellationSource,
    timing: RunTimingObservation,
    clock: Clock,
}

impl<Presentation, Clock> TimingObserver<Presentation, Clock>
where
    Clock: ObservationClock,
{
    fn new(
        presentation: Presentation,
        cancellation: CancellationSource,
        timing: RunTimingObservation,
        clock: Clock,
    ) -> Self {
        Self {
            presentation,
            cancellation,
            timing,
            clock,
        }
    }

    fn snapshot(&self) -> RunTimingSnapshot {
        self.timing.snapshot()
    }

    fn record(&self, observation: &ExecutionObservation<ExecutionInstant>) {
        self.timing.observe(observation, &self.clock);
    }
}

impl<Presentation, Clock> ExecutionObserver<ExecutionInstant>
    for TimingObserver<Presentation, Clock>
where
    Presentation: ExecutionObserver<ExecutionInstant> + PresentationFailureState,
    Clock: ObservationClock,
{
    fn observe(
        &self,
        observation: ExecutionObservation<ExecutionInstant>,
    ) -> impl Future<Output = ()> + Send {
        self.record(&observation);
        let presentation = self.presentation.clone();
        let cancellation = self.cancellation.clone();
        async move {
            presentation.observe(observation).await;
            if presentation.presentation_failed() {
                cancellation.request_cancellation(CancellationReason::CallerOutputFailure);
            }
        }
    }
}

fn observed_run_timing(timing: &RunTimingSnapshot) -> Option<WorkflowRunTiming> {
    let started = timing.execution_started?;
    let finished = timing.terminal?;
    Some(WorkflowRunTiming {
        started_at: started.utc,
        finished_at: finished.utc,
        duration: finished
            .monotonic
            .saturating_duration_since(started.monotonic),
    })
}

async fn settle_before_execution_failure(run: &InitialLocalRun) {
    let _ = run.record_executor_fault_before_execution_async().await;
}

fn record_private_cleanup_failure(run: &InitialLocalRun, cleanup_failed: bool) {
    if cleanup_failed {
        let _ = run.record_private_cleanup_failure();
    }
}

fn publication_failure_phase(phase: LocalPublicationPhase) -> PublicationFailurePhaseV1 {
    match phase {
        LocalPublicationPhase::ExportCopy => PublicationFailurePhaseV1::ExportCopy,
        LocalPublicationPhase::Serialization => PublicationFailurePhaseV1::Serialization,
        LocalPublicationPhase::Close => PublicationFailurePhaseV1::Close,
        LocalPublicationPhase::Verification => PublicationFailurePhaseV1::Verification,
        LocalPublicationPhase::TargetValidation
        | LocalPublicationPhase::Staging
        | LocalPublicationPhase::Commit => PublicationFailurePhaseV1::Rename,
    }
}

struct LocalRunEvidence<'a> {
    diagnostics: &'a StepDiagnosticLog,
    durable_invocations: &'a [DurableInvocationV1],
    timing: RunTimingSnapshot,
}

fn build_run_result(
    workflow: &ResolvedWorkflow,
    admitted: &AdmittedWorkflow,
    execution: WorkflowExecutionResult<ExecutionInstant>,
    evidence: LocalRunEvidence<'_>,
    run_timing: WorkflowRunTiming,
    local_run: &InitialLocalRun,
) -> anyhow::Result<WorkflowRunResult> {
    let diagnostics = evidence.diagnostics;
    let durable_invocations = evidence.durable_invocations;
    let continuation = local_run
        .continuation_record()
        .map_err(|_| invalid_terminal_result_error())?;
    let timing = &evidence.timing;
    // A walk an interrupted `workflow` step's child ended on its edge, a wait, a continuation
    // or an exit, after the interruption was accepted carries no cancellation (REQ-WFEXEC-70).
    let interrupted_walk = match &execution.outcome {
        RunOutcome::Waiting { .. } | RunOutcome::Continued { .. } => true,
        RunOutcome::Succeeded => admitted
            .execution()
            .cancellation()
            .source()
            .interruption_requested(),
        RunOutcome::Failed { .. } | RunOutcome::Cancelled { .. } => false,
    };
    let cancellation = match timing.cancellation {
        None => None,
        Some(_) if interrupted_walk => None,
        Some((reason, deadline)) => {
            let retained_by_outcome = match &execution.outcome {
                RunOutcome::Succeeded => false,
                RunOutcome::Failed {
                    later_cancellation, ..
                } => *later_cancellation == Some(reason),
                RunOutcome::Cancelled {
                    reason: outcome_reason,
                } => *outcome_reason == reason,
                RunOutcome::Waiting { .. } | RunOutcome::Continued { .. } => false,
            };
            if !retained_by_outcome {
                return Err(invalid_terminal_result_error());
            }
            Some(WorkflowRunCancellation {
                reason,
                force_stop_deadline: deadline,
            })
        }
    };
    let mut states = execution.steps;
    let mut recoveries = execution.recoveries;
    let mut steps = Vec::with_capacity(states.len());
    for id in &workflow.definition.presentation_order {
        let state = states
            .remove(id)
            .ok_or_else(invalid_terminal_result_error)?;
        // A step a stop kept from starting (REQ-WFEXEC-62) reached `starting` only to be
        // settled `not_run`, which records no timing.
        let timing = match timing.steps.get(id) {
            _ if matches!(state, um_execution::StepState::NotRun { .. }) => None,
            None => None,
            Some(timing) => {
                let finished = timing.finished.ok_or_else(invalid_terminal_result_error)?;
                Some(WorkflowStepTiming {
                    started_at: timing.started.utc,
                    duration: finished.saturating_duration_since(timing.started.monotonic),
                })
            }
        };
        let (kind, failure_policy) = match workflow.definition.steps.get(id) {
            Some(ValidatedStep::Command(command)) => {
                (WorkflowRunStepKind::Command, command.common.failure_policy)
            }
            Some(ValidatedStep::Agent(agent)) => {
                (WorkflowRunStepKind::Agent, agent.common.failure_policy)
            }
            Some(ValidatedStep::Workflow(workflow)) => (
                WorkflowRunStepKind::Workflow,
                workflow.common.failure_policy,
            ),
            None => return Err(invalid_terminal_result_error()),
        };
        let recovery_state = recoveries
            .remove(id)
            .ok_or_else(invalid_terminal_result_error)?;
        let recovery = step_recovery_summary_v1(recovery_state.as_ref())
            .map_err(|_| invalid_terminal_result_error())?;
        let invocations = if recovery.is_some()
            || kind == WorkflowRunStepKind::Agent
                && durable_invocations
                    .iter()
                    .any(|invocation| invocation.step_id == *id)
        {
            project_recovery_invocations(id, durable_invocations, diagnostics)?
        } else {
            Vec::new()
        };
        steps.push(WorkflowRunStep {
            id: id.clone(),
            role: WorkflowNodeRole::Step,
            kind,
            failure_policy,
            state,
            timing,
            command_output: (kind == WorkflowRunStepKind::Command)
                .then(|| diagnostics.get(id))
                .flatten(),
            recovery,
            invocations,
        });
    }
    let finalization = match (
        workflow.definition.finalizers.is_empty(),
        execution.finalization_summary,
    ) {
        (true, None) => None,
        (false, Some(summary)) => {
            let mut retained = summary
                .finalizers
                .into_iter()
                .map(|finalizer| (finalizer.finalizer.clone(), finalizer))
                .collect::<std::collections::BTreeMap<_, _>>();
            let mut finalizers = Vec::with_capacity(retained.len());
            for id in &workflow.definition.finalizer_presentation_order {
                let state = states
                    .remove(id)
                    .ok_or_else(invalid_terminal_result_error)?;
                let summarized = retained
                    .remove(id)
                    .ok_or_else(invalid_terminal_result_error)?;
                if summarized.failure_policy
                    != finalizer_failure_policy(workflow, id)
                        .ok_or_else(invalid_terminal_result_error)?
                    || !summary_disposition_matches(&summarized.disposition, &state)
                {
                    return Err(invalid_terminal_result_error());
                }
                let timing = match timing.steps.get(id) {
                    None => None,
                    Some(timing) => {
                        let finished = timing.finished.ok_or_else(invalid_terminal_result_error)?;
                        Some(WorkflowStepTiming {
                            started_at: timing.started.utc,
                            duration: finished.saturating_duration_since(timing.started.monotonic),
                        })
                    }
                };
                let (kind, failure_policy) = finalizer_kind_and_policy(workflow, id)
                    .ok_or_else(invalid_terminal_result_error)?;
                let recovery = recoveries
                    .remove(id)
                    .ok_or_else(invalid_terminal_result_error)?;
                if recovery.is_some() {
                    return Err(invalid_terminal_result_error());
                }
                finalizers.push(WorkflowRunStep {
                    id: id.clone(),
                    role: WorkflowNodeRole::Finalizer,
                    kind,
                    failure_policy,
                    state,
                    timing,
                    command_output: (kind == WorkflowRunStepKind::Command)
                        .then(|| diagnostics.get(id))
                        .flatten(),
                    recovery: None,
                    invocations: if kind == WorkflowRunStepKind::Agent
                        && durable_invocations
                            .iter()
                            .any(|invocation| invocation.step_id == *id)
                    {
                        project_recovery_invocations(id, durable_invocations, diagnostics)?
                    } else {
                        Vec::new()
                    },
                });
            }
            if !retained.is_empty() {
                return Err(invalid_terminal_result_error());
            }
            Some(WorkflowRunFinalization {
                trigger: summary.trigger,
                finalizers,
                cancellation: summary.cancellation.map(|cancellation| {
                    WorkflowRunFinalizationCancellation {
                        reason: cancellation.reason,
                        force_stop_deadline: cancellation.deadline.map(|deadline| deadline.utc),
                    }
                }),
                force_abort: summary.force_abort,
            })
        }
        (true, Some(_)) | (false, None) => return Err(invalid_terminal_result_error()),
    };
    if !states.is_empty() || !recoveries.is_empty() {
        return Err(invalid_terminal_result_error());
    }
    Ok(WorkflowRunResult {
        run_directory: local_run.run_directory().to_owned(),
        attempt_number: local_run.attempt_number(),
        continuation,
        output_producers: execution.output_producers.into_iter().fold(
            BTreeMap::new(),
            |mut producers, ((node, output), producer)| {
                producers.entry(node).or_default().insert(output, producer);
                producers
            },
        ),
        workflow_path: execution.provenance.workflow_path,
        source_root: execution.provenance.source_root,
        content_digest: execution.content_digest,
        agent_profile_replacement: workflow
            .agent_profile_replacement
            .as_ref()
            .map(AgentProfileReplacementV1::from_resolved),
        execution_root: admitted.execution().root().to_owned(),
        maximum_parallel_steps: admitted.execution().limits().maximum_parallel_steps(),
        maximum_retained_bytes_per_stream: admitted
            .execution()
            .limits()
            .maximum_step_log_bytes()
            .get(),
        cloud_capacity: None,
        maximum_result_bytes: workflow.capacity.requirements.portable_result_bytes,
        timing: run_timing,
        outcome: execution.outcome,
        cancellation,
        force_abort: execution.force_abort,
        steps,
        finalization,
        exports: execution.exports,
        export_sources: workflow.definition.exports.clone(),
        export_presentation: workflow.definition.export_presentation.clone(),
    })
}

fn project_recovery_invocations(
    step_id: &str,
    durable: &[DurableInvocationV1],
    diagnostics: &StepDiagnosticLog,
) -> anyhow::Result<Vec<RecoveryInvocationV1>> {
    let mut projected = Vec::new();
    for invocation in durable
        .iter()
        .filter(|invocation| invocation.step_id == step_id)
    {
        let finished_at = invocation
            .finished_at
            .as_deref()
            .ok_or_else(invalid_terminal_result_error)?;
        let started = OffsetDateTime::parse(&invocation.started_at, &Rfc3339)
            .map_err(|_| invalid_terminal_result_error())?;
        let finished = OffsetDateTime::parse(finished_at, &Rfc3339)
            .map_err(|_| invalid_terminal_result_error())?;
        let duration = (finished - started).whole_milliseconds();
        let duration_milliseconds =
            u64::try_from(duration).map_err(|_| invalid_terminal_result_error())?;
        let action = ActionId {
            transition_sequence: TransitionSequence(invocation.invocation_id),
        };
        let retained = diagnostics.get_invocation(step_id, action);
        let command_output = retained
            .as_ref()
            .map(command_output_v1)
            .transpose()
            .map_err(|_| invalid_terminal_result_error())?;
        if command_output.is_some() != !invocation.diagnostics.is_empty() {
            return Err(invalid_terminal_result_error());
        }
        let mut invocation_diagnostics = Vec::with_capacity(invocation.diagnostics.len());
        for diagnostic in &invocation.diagnostics {
            let output = command_output
                .as_ref()
                .ok_or_else(invalid_terminal_result_error)?;
            let stream = match diagnostic.kind {
                RecoveryDiagnosticKindV1::CommandStdout
                | RecoveryDiagnosticKindV1::AgentHarnessStdout => output.stdout.clone(),
                RecoveryDiagnosticKindV1::CommandStderr
                | RecoveryDiagnosticKindV1::AgentHarnessStderr => output.stderr.clone(),
            };
            if stream.retained_bytes != diagnostic.retained_bytes
                || stream.discarded_bytes != diagnostic.discarded_bytes
                || stream.truncated != diagnostic.truncated
                || stream.fully_drained != diagnostic.fully_drained
            {
                return Err(invalid_terminal_result_error());
            }
            invocation_diagnostics.push(RecoveryInvocationDiagnosticV1 {
                kind: diagnostic.kind,
                reference: diagnostic.reference.clone(),
                stream,
            });
        }
        projected.push(RecoveryInvocationV1 {
            invocation_id: invocation.invocation_id,
            role: invocation.role,
            target_execution: invocation.target_execution,
            recovery_round: invocation.recovery_round,
            state: match invocation.state {
                DurableInvocationStateV1::Settled => RecoveryInvocationStateV1::Settled,
                DurableInvocationStateV1::Cancelled => RecoveryInvocationStateV1::Cancelled,
                DurableInvocationStateV1::Active => {
                    return Err(invalid_terminal_result_error());
                }
            },
            started_at: invocation.started_at.clone(),
            finished_at: finished_at.to_owned(),
            duration_milliseconds,
            usage: invocation.usage,
            diagnostics: invocation_diagnostics,
            diagnostic_reference: invocation.diagnostic_reference.clone(),
        });
    }
    if projected.is_empty() {
        return Err(invalid_terminal_result_error());
    }
    projected.sort_by_key(|invocation| invocation.invocation_id);
    Ok(projected)
}

fn finalizer_kind_and_policy(
    workflow: &ResolvedWorkflow,
    id: &str,
) -> Option<(WorkflowRunStepKind, FailurePolicy)> {
    let finalizer = workflow.definition.finalizers.get(id)?;
    let (kind, policy) = match &finalizer.body {
        ValidatedStep::Command(command) => {
            (WorkflowRunStepKind::Command, command.common.failure_policy)
        }
        ValidatedStep::Agent(agent) => (WorkflowRunStepKind::Agent, agent.common.failure_policy),
        ValidatedStep::Workflow(workflow) => (
            WorkflowRunStepKind::Workflow,
            workflow.common.failure_policy,
        ),
    };
    Some((kind, policy))
}

fn finalizer_failure_policy(workflow: &ResolvedWorkflow, id: &str) -> Option<FailurePolicy> {
    finalizer_kind_and_policy(workflow, id).map(|(_, policy)| policy)
}

fn invalid_terminal_result_error() -> anyhow::Error {
    anyhow!("prepare authoritative local workflow terminal result")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InputAcquisitionFailureKind {
    Unavailable,
    NotRegularFile,
    Interrupted,
    Read,
    TooLarge,
    InvalidUtf8,
    InvalidJson,
}

impl std::fmt::Display for InputAcquisitionFailureKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "input is unavailable",
            Self::NotRegularFile => "input is not a regular file",
            Self::Interrupted => "input acquisition was interrupted",
            Self::Read => "input read unavailable",
            Self::TooLarge => "input exceeds the byte limit",
            Self::InvalidUtf8 => "input is not valid UTF-8",
            Self::InvalidJson => "input is not valid JSON",
        })
    }
}

impl Error for InputAcquisitionFailureKind {}

pub(super) fn diagnose(
    error: impl Error + Send + Sync + 'static,
) -> super::super::super::CommandResult {
    Err(anyhow::Error::new(error).into())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::ready;
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use std::process::{Command as ProcessCommand, Stdio};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};

    use nix::sys::stat::Mode;
    use nix::unistd::mkfifo;
    use rustix::fs::{FlockOperation, fcntl_lock};
    use time::format_description::well_known::Rfc3339;

    use super::*;
    use um_execution::{
        ObservationTime, ObservedExecutions, SchedulingGate, StepStateKind, TransitionEvent,
        TransitionObservation, TransitionSequence, WorkflowState, resolve,
    };

    #[derive(Clone)]
    struct ScriptedClock {
        points: Arc<Mutex<VecDeque<ObservationTime>>>,
    }

    impl ScriptedClock {
        fn new(points: impl IntoIterator<Item = ObservationTime>) -> Self {
            Self {
                points: Arc::new(Mutex::new(points.into_iter().collect())),
            }
        }
    }

    impl ObservationClock for ScriptedClock {
        fn sample(&self) -> ObservationTime {
            self.points.lock().unwrap().pop_front().unwrap()
        }
    }

    #[derive(Clone)]
    struct ControlledObservationClock {
        current: Arc<Mutex<ObservationTime>>,
    }

    impl ControlledObservationClock {
        fn new(current: ObservationTime) -> Self {
            Self {
                current: Arc::new(Mutex::new(current)),
            }
        }

        fn set(&self, current: ObservationTime) {
            *self.current.lock().unwrap() = current;
        }
    }

    impl ObservationClock for ControlledObservationClock {
        fn sample(&self) -> ObservationTime {
            *self.current.lock().unwrap()
        }
    }

    struct DelayedHeaderWriter {
        clock: ControlledObservationClock,
        completed_at: ObservationTime,
        flushed: Arc<AtomicBool>,
        entered: std::sync::mpsc::Sender<()>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl Write for DelayedHeaderWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            let _ = self.entered.send(());
            let (lock, wake) = &*self.gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
            self.clock.set(self.completed_at);
            self.flushed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RecordingPresentation {
        observations: Arc<AtomicUsize>,
        failed: bool,
    }

    impl PresentationFailureState for RecordingPresentation {
        fn presentation_failed(&self) -> bool {
            self.failed
        }
    }

    impl ExecutionObserver<ExecutionInstant> for RecordingPresentation {
        fn observe(
            &self,
            _observation: ExecutionObservation<ExecutionInstant>,
        ) -> impl Future<Output = ()> + Send {
            self.observations.fetch_add(1, Ordering::SeqCst);
            ready(())
        }
    }

    fn os_arguments(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn typed_diagnostic_keeps_its_source_chain() {
        #[derive(Debug)]
        struct StageFailure(io::Error);
        impl std::fmt::Display for StageFailure {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("stage failed")
            }
        }
        impl Error for StageFailure {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                Some(&self.0)
            }
        }
        let result = diagnose(StageFailure(io::Error::other("underlying cause")));
        let failure = result.err().unwrap();
        assert_eq!(failure.error().chain().count(), 2);
    }

    struct RestoringBoundary(Arc<AtomicBool>);

    impl um_execution::TerminalBoundary for RestoringBoundary {
        fn setup(&mut self) -> io::Result<um_execution::TerminalRect> {
            Ok(um_execution::TerminalRect::new(0, 0, 120, 24))
        }
        async fn next_event(&mut self) -> io::Result<um_execution::TerminalInputEvent> {
            std::future::pending().await
        }
        fn resize(&mut self) -> io::Result<um_execution::TerminalRect> {
            Ok(um_execution::TerminalRect::new(0, 0, 120, 24))
        }
        fn restore(&mut self) -> io::Result<()> {
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    impl um_execution::WorkflowTerminalBoundary for RestoringBoundary {
        fn draw_workflow(
            &mut self,
            _snapshot: &um_execution::WorkflowRunViewSnapshot,
            _interaction: &mut um_execution::HostInteraction,
            _color: bool,
        ) -> io::Result<()> {
            Ok(())
        }
    }

    fn private_staging_paths(root: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("workflow-")
            })
            .collect()
    }

    #[tokio::test]
    async fn attempt_failure_releases_staging_and_restores_active_terminal() {
        for (phase, target, start_host) in [
            (
                "after artifact staging",
                Some(AttemptCheckpoint::ArtifactsStaged),
                false,
            ),
            (
                "after input staging",
                Some(AttemptCheckpoint::InputsStaged),
                false,
            ),
            (
                "before host start",
                Some(AttemptCheckpoint::BeforeHostStart),
                false,
            ),
            (
                "after host start",
                Some(AttemptCheckpoint::AfterHostStart),
                true,
            ),
            ("execution failure", None, true),
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let source = temporary.path().join("source");
            let execution_root = temporary.path().join("execution");
            std::fs::create_dir(&source).unwrap();
            std::fs::create_dir(&execution_root).unwrap();
            std::fs::write(
                source.join("workflow.yaml"),
                "schemaVersion: 1\nsteps:\n  task:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
            ).unwrap();
            let workflow = resolve(&source, Path::new("workflow.yaml")).unwrap();
            let admitted = admit_workflow(
                workflow.clone(),
                ResolvedInputs::default(),
                execution_context_for_workflow(
                    &workflow,
                    execution_root,
                    1,
                    CancellationSource::new(),
                )
                .unwrap(),
            )
            .unwrap();
            let run = InitialLocalRun::create(&temporary.path().join("run"), &admitted).unwrap();
            let private_root = run.private_directory().to_owned();
            let reached = Arc::new(AtomicBool::new(false));
            let reached_checkpoint = reached.clone();
            let reached_execution = reached.clone();
            let private_at_checkpoint = private_root.clone();
            let private_root_for_execution = private_root.clone();
            let restored = Arc::new(AtomicBool::new(false));
            let restored_for_terminal = restored.clone();
            let result = execute_owned_attempt_with(
                workflow,
                admitted,
                run,
                CancellationSource::new(),
                SignalObservation {
                    task: tokio::spawn(std::future::pending()),
                    journals: ControlJournals::default(),
                },
                AttemptSettings {
                    presentation_config: PresentationConfig {
                        requested_mode: RequestedPresentationMode::Automatic,
                        color: ColorChoice::Never,
                        capabilities: TerminalCapabilities {
                            stdin_is_terminal: true,
                            stdout_is_terminal: true,
                            stderr_is_terminal: true,
                            stdout_width: Some(120),
                            stderr_width: Some(120),
                            term: Some("xterm".into()),
                            no_color: None,
                        },
                        standard_input_reserved: false,
                    },
                    leaf: ExecutionLeaf::Run,
                    child: None,
                    child_recovery: None,
                    journals: ControlJournals::default(),
                    hooks: (
                        move |point| {
                            if Some(point) == target {
                                assert_eq!(private_staging_paths(&private_at_checkpoint).len(), 1);
                                reached_checkpoint.store(true, Ordering::SeqCst);
                                anyhow::bail!("injected phase failure")
                            }
                            Ok(())
                        },
                        move |view, cancellation, color| {
                            WorkflowTerminalHost::start_with_boundary(
                                view,
                                cancellation,
                                color,
                                RestoringBoundary(restored_for_terminal.clone()),
                            )
                        },
                        || {
                            // Return an execution error after ownership has entered the
                            // execution phase, without depending on a real child process.
                            if target.is_none() {
                                assert_eq!(
                                    private_staging_paths(&private_root_for_execution).len(),
                                    1
                                );
                                reached_execution.store(true, Ordering::SeqCst);
                                Some(CoordinationError::ReducerStateUnavailable)
                            } else {
                                None
                            }
                        },
                    ),
                },
            )
            .await;
            assert!(reached.load(Ordering::SeqCst), "{phase} was not reached");
            assert!(result.is_err(), "{phase} must fail");
            assert!(private_staging_paths(&private_root).is_empty(), "{phase}");
            assert_eq!(restored.load(Ordering::SeqCst), start_host, "{phase}");
        }
    }

    // REQ-WFEXEC-69, REQ-WFEXEC-40, REQ-LOCALRUN-136
    #[tokio::test]
    async fn an_interruption_before_any_step_starts_publishes_the_failed_step_with_timing() {
        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&execution_root).unwrap();
        std::fs::write(
            source.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n  second:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
        )
        .unwrap();
        let workflow = resolve(&source, Path::new("workflow.yaml")).unwrap();
        let cancellation = CancellationSource::new();
        assert!(cancellation.request_interruption(CancellationReason::UserRequest));
        let admitted = admit_workflow(
            workflow.clone(),
            ResolvedInputs::default(),
            execution_context_for_workflow(&workflow, execution_root, 1, cancellation.clone())
                .unwrap(),
        )
        .unwrap();
        let run = InitialLocalRun::create(&temporary.path().join("run"), &admitted).unwrap();
        let result_directory = run.result_directory().to_owned();

        let result = execute_owned_attempt_with(
            workflow,
            admitted,
            run,
            cancellation,
            SignalObservation {
                task: tokio::spawn(std::future::pending()),
                journals: ControlJournals::default(),
            },
            AttemptSettings {
                presentation_config: PresentationConfig {
                    requested_mode: RequestedPresentationMode::Plain,
                    color: ColorChoice::Never,
                    capabilities: TerminalCapabilities {
                        stdin_is_terminal: false,
                        stdout_is_terminal: false,
                        stderr_is_terminal: false,
                        stdout_width: None,
                        stderr_width: None,
                        term: None,
                        no_color: None,
                    },
                    standard_input_reserved: false,
                },
                leaf: ExecutionLeaf::Run,
                child: None,
                child_recovery: None,
                journals: ControlJournals::default(),
                hooks: (
                    |_| Ok(()),
                    |view, cancellation, color| {
                        WorkflowTerminalHost::start_with_boundary(
                            view,
                            cancellation,
                            color,
                            RestoringBoundary(Arc::new(AtomicBool::new(false))),
                        )
                    },
                    || None,
                ),
            },
        )
        .await;

        let exit = match result {
            Ok(exit) => exit,
            Err(failure) => panic!("the interrupted attempt failed: {:#}", failure.error()),
        };
        assert_eq!(exit, ExitCode::Interrupted);
        let bytes = std::fs::read(result_directory.join("result.json")).unwrap();
        let published: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(published["outcome"], "failed");
        let steps = published["steps"].as_array().unwrap();
        let step = |id: &str| {
            steps
                .iter()
                .find(|step| step["id"] == id)
                .unwrap_or_else(|| panic!("{id} is missing from {published}"))
        };
        let first = step("first");
        assert_eq!(first["state"], "failed");
        assert_eq!(first["detail"]["phase"], "start");
        assert_eq!(first["detail"]["code"], "interrupted");
        assert!(first["startedAt"].is_string(), "{first}");
        assert!(first["durationMilliseconds"].is_u64(), "{first}");
        assert_eq!(step("second")["state"], "cancelled");
    }

    // REQ-LOCALRUN-205, REQ-LOCALRUN-206: a failed state commit is named in the command's
    // error and recorded as a `state_persistence_failure` diagnostic with its class and OS
    // error number, and the attempt it leaves unsettled leaves the run record `running`.
    // Any other execution failure leaves the record `running` and records nothing.
    #[tokio::test]
    async fn an_execution_failure_over_an_unsettled_attempt_leaves_the_run_running() {
        let commit_failure = um_execution::CommitFailure {
            class: um_execution::CommitFailureClass::StorageFull,
            operation: Some("write temporary state"),
            os_error: Some(28),
        };
        for (error, recorded) in [
            (CoordinationError::CommitFailed(commit_failure), true),
            (CoordinationError::ReducerStateUnavailable, false),
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let cancellation = CancellationSource::new();
            let (creation, _home, _run_directory) =
                subject_creation(temporary.path(), &cancellation);
            let admitted = creation.admitted.clone();
            let workflow =
                resolve(&temporary.path().join("source"), Path::new("workflow.yaml")).unwrap();
            let Ok((run, _journal)) = creation.create(&cancellation, &ControlJournals::default())
            else {
                panic!("the run was not created");
            };
            let run_directory = run.run_directory().to_owned();

            let result = execute_owned_attempt_with(
                workflow,
                admitted,
                run,
                cancellation,
                SignalObservation {
                    task: tokio::spawn(std::future::pending()),
                    journals: ControlJournals::default(),
                },
                AttemptSettings {
                    presentation_config: PresentationConfig {
                        requested_mode: RequestedPresentationMode::Plain,
                        color: ColorChoice::Never,
                        capabilities: TerminalCapabilities {
                            stdin_is_terminal: false,
                            stdout_is_terminal: false,
                            stderr_is_terminal: false,
                            stdout_width: None,
                            stderr_width: None,
                            term: None,
                            no_color: None,
                        },
                        standard_input_reserved: false,
                    },
                    leaf: ExecutionLeaf::Run,
                    child: None,
                    child_recovery: None,
                    journals: ControlJournals::default(),
                    hooks: (
                        |_| Ok(()),
                        |view, cancellation, color| {
                            WorkflowTerminalHost::start_with_boundary(
                                view,
                                cancellation,
                                color,
                                RestoringBoundary(Arc::new(AtomicBool::new(false))),
                            )
                        },
                        move || Some(error),
                    ),
                },
            )
            .await;

            let Err(failure) = result else {
                panic!("{error:?} must fail the command");
            };
            let state: serde_json::Value =
                serde_json::from_slice(&std::fs::read(run_directory.join("state.json")).unwrap())
                    .unwrap();
            let attempt = &state["attempts"][0];
            assert!(
                ["created", "running"].contains(&attempt["state"].as_str().unwrap()),
                "{error:?}: {attempt}"
            );
            let diagnostics = state["diagnostics"].as_array().unwrap();
            if recorded {
                // REQ-LOCALRUN-143: no port ran, so the diagnostic line names the write and
                // its error number from the failure; the class is a field and the remedy,
                // last, resumes the run.
                let rendered = format!("{:#}", failure.error());
                let lines = rendered.lines().collect::<Vec<_>>();
                assert!(lines.contains(&"class: storage_full"), "{rendered}");
                assert!(
                    lines[0].contains("write temporary state")
                        && lines[0].ends_with(&io::Error::from_raw_os_error(28).to_string()),
                    "{rendered}"
                );
                assert_eq!(
                    lines.last().map(|line| line.trim()),
                    Some(
                        format!(
                            "um workflow run resume {}",
                            normalize_terminal_path(&run_directory)
                        )
                        .as_str()
                    ),
                    "{rendered}"
                );
                assert_eq!(
                    failure
                        .error()
                        .downcast_ref::<StateCommitFailed>()
                        .map(|failed| failed.failure),
                    Some(commit_failure)
                );
                assert_eq!(
                    diagnostics.as_slice(),
                    [serde_json::json!({
                        "sequence": 1,
                        "attemptNumber": 1,
                        "code": "state_persistence_failure",
                        "cause": "storage_full",
                        "osError": 28,
                    })],
                );
            } else {
                assert!(diagnostics.is_empty(), "{error:?}: {diagnostics:?}");
            }
            let record: serde_json::Value =
                serde_json::from_slice(&std::fs::read(run_directory.join("run.json")).unwrap())
                    .unwrap();
            assert_eq!(record["state"], "running", "{error:?}: {record}");
            assert_eq!(record["outcome"], serde_json::Value::Null, "{error:?}");
        }
    }

    const SUBJECT_DEFINITION: &str = "def_01jz0000000000000000000000";
    const SUBJECT: &str = "AUTO-1";

    /// The creation of a one-step run of `SUBJECT` under `SUBJECT_DEFINITION` in a home
    /// below `root`, admitted under `cancellation`, with that home and the run directory.
    fn subject_creation(
        root: &Path,
        cancellation: &CancellationSource,
    ) -> (RunCreation, WorkflowHome, PathBuf) {
        let source = root.join("source");
        let execution_root = root.join("execution");
        std::fs::create_dir(&source).unwrap();
        std::fs::create_dir(&execution_root).unwrap();
        std::fs::write(
            source.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  only:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
        )
        .unwrap();
        let workflow = resolve(&source, Path::new("workflow.yaml")).unwrap();
        let admitted = admit_workflow(
            workflow.clone(),
            ResolvedInputs::default(),
            execution_context_for_workflow(&workflow, execution_root, 1, cancellation.clone())
                .unwrap(),
        )
        .unwrap();
        let home_root = root.join("home");
        let home = WorkflowHome::from_lookup(|name| {
            (name == "UM_HOME").then(|| home_root.clone().into_os_string())
        })
        .unwrap();
        std::fs::create_dir_all(home.definition_directory(SUBJECT_DEFINITION)).unwrap();
        let mut record = WorkflowRunRecord::new(
            Some((SUBJECT_DEFINITION.to_owned(), 1)),
            &format!("sha256:{}", "0".repeat(64)),
            "workflow.yaml",
            "tester",
        );
        record.subject = Some(SUBJECT.to_owned());
        let destination = RunDestination::Definition {
            home: home.clone(),
            id: SUBJECT_DEFINITION.to_owned(),
            revision: 1,
        };
        let run_directory = destination.run_directory(Some(&record)).unwrap();
        let creation = RunCreation {
            destination,
            subject: Some(SUBJECT.to_owned()),
            record: Some(record),
            run_directory: run_directory.clone(),
            admitted,
            stop_before: None,
        };
        (creation, home, run_directory)
    }

    /// The failure of a creation a signal refused, which left no run under the destination
    /// and the subject's slot free (REQ-LOCALRUN-135).
    fn refused_creation(
        created: Result<(InitialLocalRun, WatchedJournal), CreationFailure>,
        home: &WorkflowHome,
        run_directory: &Path,
    ) -> super::super::super::super::CommandFailure {
        let failure = match created {
            Err(CreationFailure::Signalled(failure)) => failure,
            Ok((run, _)) => panic!("created {}", run.run_directory().display()),
            Err(CreationFailure::Subject(error)) => panic!("{error}"),
            Err(CreationFailure::Other(error)) => panic!("{error:#}"),
        };
        assert!(!run_directory.exists());
        let runs = std::fs::read_dir(home.runs_directory(SUBJECT_DEFINITION))
            .map(|entries| entries.count())
            .unwrap_or(0);
        assert_eq!(runs, 0, "a run was left under the destination");
        assert_eq!(
            um_execution::subject_slot(home, SUBJECT_DEFINITION, SUBJECT).unwrap(),
            None
        );
        failure
    }

    // REQ-LOCALRUN-135, REQ-WFEXEC-40
    #[test]
    fn a_signal_before_the_run_directory_exists_creates_no_run() {
        for (reason, exit) in [
            (CancellationReason::UserRequest, ExitCode::Interrupted),
            (CancellationReason::TerminationRequest, ExitCode::Terminated),
        ] {
            let temporary = tempfile::tempdir().unwrap();
            // While no journal is watched, a signal requests a cancel (`signal_request`).
            let cancellation = CancellationSource::new();
            assert!(cancellation.request_cancellation(reason));
            let (creation, home, run_directory) = subject_creation(temporary.path(), &cancellation);

            let created = creation.create(&cancellation, &ControlJournals::default());

            let failure = refused_creation(created, &home, &run_directory);
            assert_eq!(failure.exit_code(), exit, "{reason:?}");
            assert!(
                !home
                    .definition_directory(SUBJECT_DEFINITION)
                    .join("subjects")
                    .join(SUBJECT)
                    .exists(),
                "{reason:?} took the subject slot"
            );
        }
    }

    // REQ-LOCALRUN-135, REQ-WFEXEC-40: from the moment `create` begins to make the run
    // directory a signal is an interruption, never a cancel, so a run the creation still
    // makes cannot end `canceled`; one the creation meets before the directory exists
    // refuses it and writes no subject slot. The creation is held at the subject index's
    // lock, as a concurrent create of the subject holds it.
    #[tokio::test]
    async fn a_signal_while_the_run_directory_is_created_interrupts() {
        let temporary = tempfile::tempdir().unwrap();
        let cancellation = CancellationSource::new();
        let (creation, home, run_directory) = subject_creation(temporary.path(), &cancellation);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer = start_signal_observation(cancellation.clone(), InjectedSignals(receiver));
        let index = SubjectClaim::take(&home, SUBJECT_DEFINITION, SUBJECT, None).unwrap();
        let (journals, creating_cancellation) = (observer.journals.clone(), cancellation.clone());
        let creating =
            tokio::task::spawn_blocking(move || creation.create(&creating_cancellation, &journals));
        let watched = observer.journals.clone();
        let watching = tokio::task::spawn_blocking(move || {
            let deadline = um_support::monotonic_now() + std::time::Duration::from_secs(10);
            while watched.watched().is_empty() && um_support::monotonic_now() < deadline {
                um_support::sleep(std::time::Duration::from_millis(5));
            }
            !watched.watched().is_empty()
        })
        .await
        .unwrap();

        sender
            .send(SignalEvent::Signal(CancellationReason::UserRequest))
            .unwrap();
        assert_eq!(
            cancellation.wait_for_cancellation().await,
            CancellationReason::UserRequest
        );
        drop(index);
        let created = creating.await.unwrap();

        assert!(watching, "the creation watched no control journal");
        assert!(cancellation.interruption_requested());
        let failure = refused_creation(created, &home, &run_directory);
        assert_eq!(failure.exit_code(), ExitCode::Interrupted);
        assert!(
            !home
                .definition_directory(SUBJECT_DEFINITION)
                .join("subjects")
                .join(SUBJECT)
                .exists(),
            "the refused creation wrote the subject slot"
        );
        assert!(observer.journals.watched().is_empty());
        drop(sender);
        observer.task.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_stdin_read_restores_shared_file_description_flags() {
        use std::future::Future as _;
        use std::os::unix::net::UnixStream;
        use std::task::{Context, Poll, Waker};

        let (input, _writer) = UnixStream::pair().unwrap();
        let original = fcntl_getfl(&input).unwrap();
        let cancellation = CancellationSource::new();
        let mut read = Box::pin(read_stdin_from(&input, 64, &cancellation));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(read.as_mut().poll(&mut context), Poll::Pending));
        assert!(fcntl_getfl(&input).unwrap().contains(OFlags::NONBLOCK));
        drop(read);
        assert_eq!(fcntl_getfl(&input).unwrap(), original);
    }

    // REQ-LOCALRUN-83
    #[test]
    fn named_input_planning_rejects_conflicts_before_source_io() {
        let missing = "/path/that/must/not/be/read";
        for (text, text_files, json, json_files, attachments, empty) in [
            (
                os_arguments(&["request", "inline"]),
                os_arguments(&["request", missing]),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            (
                Vec::new(),
                os_arguments(&["first", "-"]),
                Vec::new(),
                os_arguments(&["second", "-"]),
                Vec::new(),
                Vec::new(),
            ),
            (
                Vec::new(),
                Vec::new(),
                os_arguments(&["request", "null"]),
                os_arguments(&["request", missing]),
                Vec::new(),
                Vec::new(),
            ),
            (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                os_arguments(&["evidence", "text/plain", missing]),
                vec!["evidence".to_owned()],
            ),
        ] {
            assert!(
                plan_inputs(
                    &text,
                    &text_files,
                    &json,
                    &json_files,
                    &[],
                    &attachments,
                    &empty,
                )
                .is_err()
            );
        }
    }

    // REQ-LOCALRUN-88
    #[test]
    fn named_input_planning_enforces_inclusive_name_and_member_bounds() {
        let mut text = Vec::new();
        for index in 0..MAXIMUM_INPUTS {
            text.push(OsString::from(format!("input{index}")));
            text.push(OsString::from("value"));
        }
        assert!(plan_inputs(&text, &[], &[], &[], &[], &[], &[]).is_ok());
        text.push(OsString::from("oneOver"));
        text.push(OsString::from("value"));
        assert!(plan_inputs(&text, &[], &[], &[], &[], &[], &[]).is_err());

        let mut attachments = Vec::new();
        for index in 0..MAXIMUM_ATTACHMENTS {
            attachments.extend([
                OsString::from("evidence"),
                OsString::from("application/octet-stream"),
                OsString::from(format!("member-{index}")),
            ]);
        }
        assert!(plan_inputs(&[], &[], &[], &[], &[], &attachments, &[]).is_ok());
        attachments.extend([
            OsString::from("evidence"),
            OsString::from("application/octet-stream"),
            OsString::from("member-over"),
        ]);
        assert!(plan_inputs(&[], &[], &[], &[], &[], &attachments, &[]).is_err());
    }

    // REQ-LOCALRUN-82, REQ-LOCALRUN-84
    #[tokio::test]
    async fn named_input_acquisition_preserves_empty_values_and_member_order() {
        let temporary = tempfile::tempdir().unwrap();
        let first = temporary.path().join("first");
        let second = temporary.path().join("second");
        std::fs::write(&first, b"distinct-first").unwrap();
        std::fs::write(&second, b"second-value").unwrap();
        let file_value = temporary.path().join("payload");
        std::fs::write(&file_value, b"file-value").unwrap();
        let files = vec![
            OsString::from("payload"),
            OsString::from("application/octet-stream"),
            file_value.into_os_string(),
        ];
        let attachments = vec![
            OsString::from("evidence"),
            OsString::from("text/plain"),
            first.into_os_string(),
            OsString::from("evidence"),
            OsString::from("application/octet-stream"),
            second.into_os_string(),
        ];
        let plan = plan_inputs(
            &os_arguments(&["request", ""]),
            &[],
            &os_arguments(&["settings", "{ \"z\": null, \"n\": 1.2300 }"]),
            &[],
            &files,
            &attachments,
            &["emptyEvidence".to_owned()],
        )
        .unwrap();
        let inputs = acquire_inputs(&plan, &CancellationSource::new())
            .await
            .unwrap();
        assert!(matches!(
            inputs.get("request"),
            Some(ResolvedInput::Text(value)) if value.is_empty()
        ));
        let Some(ResolvedInput::Json(settings)) = inputs.get("settings") else {
            panic!("named JSON value is missing");
        };
        assert_eq!(settings.source(), b"{ \"z\": null, \"n\": 1.2300 }");
        assert_eq!(settings.canonical(), b"{\"n\":1.2300,\"z\":null}");
        assert!(settings.value()["z"].is_null());
        let Some(ResolvedInput::File(value)) = inputs.get("payload") else {
            panic!("named File value is missing");
        };
        assert_eq!(value.media_type(), "application/octet-stream");
        assert_eq!(value.bytes(), b"file-value");
        let Some(ResolvedInput::Attachments(values)) = inputs.get("evidence") else {
            panic!("named attachment collection is missing");
        };
        assert_eq!(values[0].bytes(), b"distinct-first");
        assert_eq!(values[1].bytes(), b"second-value");
        assert!(matches!(
            inputs.get("emptyEvidence"),
            Some(ResolvedInput::Attachments(values)) if values.is_empty()
        ));
    }

    // REQ-LOCALRUN-84
    #[tokio::test]
    async fn named_json_acquisition_rejects_duplicate_keys_without_a_fallback() {
        for source in ["{\"safe\":1,\"safe\":2}", "", "null true"] {
            let plan = plan_inputs(
                &[],
                &[],
                &os_arguments(&["request", source]),
                &[],
                &[],
                &[],
                &[],
            )
            .unwrap();
            assert!(
                acquire_inputs(&plan, &CancellationSource::new())
                    .await
                    .is_err()
            );
        }
    }

    // REQ-LOCALRUN-85
    #[test]
    fn named_input_paths_follow_regular_symlinks_and_reject_nonregular_files() {
        let temporary = tempfile::tempdir().unwrap();
        let regular = temporary.path().join("regular");
        let link = temporary.path().join("link");
        let fifo = temporary.path().join("fifo");
        std::fs::write(&regular, b"regular-bytes").unwrap();
        symlink(&regular, &link).unwrap();
        mkfifo(&fifo, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();

        let file = open_regular_input(&link).unwrap();
        assert_eq!(
            read_bounded(file, 32, &CancellationSource::new()).unwrap(),
            b"regular-bytes"
        );
        assert_eq!(
            open_regular_input(temporary.path()).unwrap_err(),
            InputAcquisitionFailureKind::NotRegularFile
        );
        assert_eq!(
            open_regular_input(&fifo).unwrap_err(),
            InputAcquisitionFailureKind::NotRegularFile
        );
    }

    // REQ-LOCALRUN-88
    #[tokio::test]
    async fn json_file_acquisition_accepts_exact_source_bound_and_rejects_one_over() {
        let temporary = tempfile::tempdir().unwrap();
        let exact_path = temporary.path().join("exact.json");
        let one_over_path = temporary.path().join("one-over.json");
        let mut exact = Vec::with_capacity(usize::try_from(MAXIMUM_TEXT_BYTES).unwrap());
        exact.push(b'"');
        exact.extend(std::iter::repeat_n(
            b'x',
            usize::try_from(MAXIMUM_TEXT_BYTES).unwrap() - 2,
        ));
        exact.push(b'"');
        let mut one_over = exact.clone();
        one_over.insert(one_over.len() - 1, b'y');
        std::fs::write(&exact_path, &exact).unwrap();
        std::fs::write(&one_over_path, &one_over).unwrap();

        let exact_plan = plan_inputs(
            &[],
            &[],
            &[],
            &[OsString::from("request"), exact_path.into_os_string()],
            &[],
            &[],
            &[],
        )
        .unwrap();
        let exact_inputs = acquire_inputs(&exact_plan, &CancellationSource::new())
            .await
            .unwrap();
        assert!(matches!(
            exact_inputs.get("request"),
            Some(ResolvedInput::Json(value)) if value.source().len() == exact.len()
        ));

        let one_over_plan = plan_inputs(
            &[],
            &[],
            &[],
            &[OsString::from("request"), one_over_path.into_os_string()],
            &[],
            &[],
            &[],
        )
        .unwrap();
        assert!(
            acquire_inputs(&one_over_plan, &CancellationSource::new())
                .await
                .is_err()
        );
    }

    // REQ-LOCALRUN-88
    #[test]
    fn input_reader_accepts_exact_limit_and_rejects_one_over() {
        let cancellation = CancellationSource::new();
        let exact = vec![b'x'; 17];
        assert_eq!(
            read_bounded(io::Cursor::new(exact.clone()), 17, &cancellation).unwrap(),
            exact
        );
        assert_eq!(
            read_bounded(io::Cursor::new(vec![b'y'; 18]), 17, &cancellation),
            Err(InputAcquisitionFailureKind::TooLarge)
        );

        let mut aggregate = MAXIMUM_TOTAL_INPUT_BYTES - 1;
        account_input_bytes(&mut aggregate, 1, MAXIMUM_ATTACHMENT_BYTES).unwrap();
        assert_eq!(aggregate, MAXIMUM_TOTAL_INPUT_BYTES);
        assert!(account_input_bytes(&mut aggregate, 1, MAXIMUM_ATTACHMENT_BYTES).is_err());
    }

    #[test]
    fn local_context_discovers_a_recovery_only_agent_harness() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        std::fs::create_dir(&source_root).unwrap();
        std::fs::write(source_root.join("recovery.md"), "Repair the target.\n").unwrap();
        std::fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nagentProfiles:\n  repair:\n    harness:\n      kind: pi\n      config: {model: openai/gpt-5, thinking: high}\nsteps:\n  check:\n    kind: cmd\n    recovery:\n      retries: 1\n      handler:\n        kind: agent\n        profile: repair\n        prompt: recovery.md\n    command: {argv: [\"true\"]}\n",
        )
        .unwrap();
        let workflow = resolve(&source_root, Path::new("workflow.yaml")).unwrap();

        let harnesses = required_agent_harnesses(&workflow).collect::<Vec<_>>();
        assert_eq!(harnesses.len(), 1);
        assert!(matches!(harnesses[0], ValidatedHarness::Pi(_)));
    }

    #[test]
    fn presentation_flags_forward_injected_terminal_capabilities() {
        let capabilities = TerminalCapabilities {
            stdin_is_terminal: true,
            stdout_is_terminal: true,
            stderr_is_terminal: false,
            stdout_width: Some(100),
            stderr_width: None,
            term: Some("xterm".into()),
            no_color: Some("1".into()),
        };
        let command = <Command as clap::FromArgMatches>::from_arg_matches(
            &<Command as Args>::augment_args(clap::Command::new("create")).get_matches_from([
                "create",
                "--path",
                "source",
                "--execution-root",
                "execution",
                "--run-dir",
                "run",
                "--max-parallel",
                "2",
                "--json",
                "--color",
                "always",
            ]),
        )
        .unwrap();

        assert_eq!(
            command.presentation_config_with(capabilities.clone()),
            PresentationConfig {
                requested_mode: RequestedPresentationMode::Json,
                color: ColorChoice::Always,
                capabilities,
                standard_input_reserved: false,
            }
        );
    }

    // REQ-WFFINALIZE-15, REQ-LOCALRUN-135
    #[test]
    fn repeated_signals_only_force_abort_a_cancelling_finalization() {
        for request in [SignalRequest::Cancel, SignalRequest::Interrupt] {
            let ordinary = CancellationSource::new();
            assert!(!handle_observed_signal(
                &ordinary,
                CancellationReason::UserRequest,
                request
            ));
            assert!(!handle_observed_signal(
                &ordinary,
                CancellationReason::TerminationRequest,
                request
            ));
            assert_eq!(
                ordinary.interruption_requested(),
                request == SignalRequest::Interrupt
            );
            assert!(ordinary.request_force_abort());

            let finalization = CancellationSource::new();
            assert!(finalization.fixture_begin_finalization_arm());
            assert!(finalization.fixture_complete_finalization_arm());
            assert!(!handle_observed_signal(
                &finalization,
                CancellationReason::UserRequest,
                request
            ));
            assert!(handle_observed_signal(
                &finalization,
                CancellationReason::TerminationRequest,
                request
            ));
            assert!(!finalization.interruption_requested());
            assert!(!finalization.request_force_abort());
        }
    }

    struct InjectedSignals(tokio::sync::mpsc::UnboundedReceiver<SignalEvent>);

    impl SignalEvents for InjectedSignals {
        async fn next(&mut self) -> Option<SignalEvent> {
            self.0.recv().await
        }
    }

    /// A directory shaped like a run root as far as its control journal needs.
    fn control_journal() -> tempfile::TempDir {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join(".private")).unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        root
    }

    fn record_control(root: &Path, event: &str, reason: &str) -> ControlEvent {
        let payload = serde_json::json!({"reason": reason, "before": "build"});
        let control = ControlEvent::new(
            event,
            "run_01jcf9a4r7t2m8k3v6x1z5n0pq",
            "saeed",
            None,
            payload.as_object().cloned(),
        )
        .unwrap();
        append_control(root, &control).unwrap();
        control
    }

    /// Delivers `events` to an observer of `cancellation` watching `root` once `before` was
    /// recorded there and `after` once it watches, and returns the control it reported for
    /// the wait payload after the observation ends.
    async fn observe_journal(
        cancellation: &CancellationSource,
        root: &Path,
        (before, after): (&[&str], &[&str]),
        events: &[SignalEvent],
    ) -> Option<ControlEvent> {
        for event in before {
            record_control(root, event, "earlier");
        }
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer = start_signal_observation(cancellation.clone(), InjectedSignals(receiver));
        let journal = observer.watch(root);
        for event in after {
            record_control(root, event, "pausing for a fix");
        }
        for event in events {
            sender.send(*event).unwrap();
        }
        drop(sender);
        let SignalObservation { task, journals } = observer;
        task.await.unwrap();
        drop(journal);
        journals.interrupting_control()
    }

    // REQ-WFDEF-6, REQ-LOCALRUN-135: a wake-up reads the journal and interrupts, with
    // `termination_request`, for a `control.interrupt` recorded since the process took the
    // run, which it reports for the wait payload; one that finds no such control, a second
    // one, and one while finalization is cancelling request nothing more. A signal still
    // reads `control.cancel` as a cancellation.
    #[tokio::test]
    async fn a_wake_up_interrupts_only_for_a_control_interrupt_recorded_since_the_run_was_taken() {
        const INTERRUPT: &str = "control.interrupt";
        let wake = [SignalEvent::WakeUp];
        for (before, after) in [(&[][..], &[][..]), (&[INTERRUPT][..], &[CONTROL_STOP][..])] {
            let cancellation = CancellationSource::new();
            let root = control_journal();
            let reported =
                observe_journal(&cancellation, root.path(), (before, after), &wake).await;
            assert_eq!(reported, None, "{before:?} then {after:?}");
            assert_eq!(cancellation.cancellation_reason(), None);
            assert!(!cancellation.interruption_requested());
        }

        let cancellation = CancellationSource::new();
        let root = control_journal();
        let reported = observe_journal(
            &cancellation,
            root.path(),
            (&[INTERRUPT], &[CONTROL_STOP, INTERRUPT]),
            &[SignalEvent::WakeUp, SignalEvent::WakeUp],
        )
        .await
        .expect("the interrupting control");
        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::TerminationRequest)
        );
        assert!(cancellation.interruption_requested());
        assert_eq!(
            reported,
            read_controls(root.path()).unwrap().pop().unwrap(),
            "the control recorded after the watch"
        );
        assert_eq!(reported.by, "saeed");
        assert_eq!(
            reported.payload.as_ref().unwrap()["reason"],
            "pausing for a fix"
        );
        assert!(cancellation.request_force_abort());

        // While finalization is cancelling, the interrupt cancels it and a second wake-up
        // forces nothing.
        let finalization = CancellationSource::new();
        assert!(finalization.fixture_begin_finalization_arm());
        assert!(finalization.fixture_complete_finalization_arm());
        let root = control_journal();
        let reported = observe_journal(
            &finalization,
            root.path(),
            (&[], &[INTERRUPT]),
            &[SignalEvent::WakeUp, SignalEvent::WakeUp],
        )
        .await;
        assert_eq!(reported, None);
        assert_eq!(
            finalization.cancellation_reason(),
            Some(CancellationReason::TerminationRequest)
        );
        assert!(finalization.request_force_abort());

        // SIGTERM with `control.cancel` recorded cancels (REQ-LOCALRUN-186).
        let cancellation = CancellationSource::new();
        let root = control_journal();
        let reported = observe_journal(
            &cancellation,
            root.path(),
            (&[], &[CONTROL_CANCEL]),
            &[SignalEvent::Signal(CancellationReason::TerminationRequest)],
        )
        .await;
        assert_eq!(reported, None);
        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::TerminationRequest)
        );
        assert!(!cancellation.interruption_requested());

        // REQ-LOCALRUN-135: a `control.cancel` recorded before the process took the run is
        // an earlier attempt's, so the signal interrupts.
        let cancellation = CancellationSource::new();
        let root = control_journal();
        let reported = observe_journal(
            &cancellation,
            root.path(),
            (&[CONTROL_CANCEL], &[]),
            &[SignalEvent::Signal(CancellationReason::TerminationRequest)],
        )
        .await;
        assert_eq!(reported, None);
        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::TerminationRequest)
        );
        assert!(cancellation.interruption_requested());
    }

    // REQ-LOCALRUN-135: before the run directory is known a signal cancels; once a watched
    // journal holds no `control.cancel`, it interrupts.
    #[tokio::test]
    async fn a_signal_interrupts_once_a_run_directory_is_watched() {
        let temporary = tempfile::tempdir().unwrap();
        for watched in [false, true] {
            let cancellation = CancellationSource::new();
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let observer =
                start_signal_observation(cancellation.clone(), InjectedSignals(receiver));
            let journal = watched.then(|| observer.watch(temporary.path()));
            sender
                .send(SignalEvent::Signal(CancellationReason::UserRequest))
                .unwrap();
            assert_eq!(
                cancellation.wait_for_cancellation().await,
                CancellationReason::UserRequest
            );
            assert_eq!(cancellation.interruption_requested(), watched);
            drop(journal);
            assert!(observer.journals.watched().is_empty());
            drop(sender);
            observer.task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn injected_signals_map_once_to_the_closed_cancellation_reason() {
        let cancellation = CancellationSource::new();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer = start_signal_observation(cancellation.clone(), InjectedSignals(receiver));
        sender
            .send(SignalEvent::Signal(CancellationReason::TerminationRequest))
            .unwrap();
        assert_eq!(
            cancellation.wait_for_cancellation().await,
            CancellationReason::TerminationRequest
        );
        assert!(!cancellation.request_cancellation(CancellationReason::UserRequest));
        drop(sender);
        observer.task.await.unwrap();

        let finalization = CancellationSource::new();
        assert!(finalization.fixture_begin_finalization_arm());
        assert!(finalization.fixture_complete_finalization_arm());
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer = start_signal_observation(finalization.clone(), InjectedSignals(receiver));
        sender
            .send(SignalEvent::Signal(CancellationReason::UserRequest))
            .unwrap();
        assert_eq!(
            finalization.wait_for_cancellation().await,
            CancellationReason::UserRequest
        );
        sender
            .send(SignalEvent::Signal(CancellationReason::TerminationRequest))
            .unwrap();
        observer.task.await.unwrap();
        assert!(!finalization.request_force_abort());
    }

    #[test]
    fn adapter_completion_follows_attempt_ownership_release() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let run_parent = temporary.path().join("runs");
        for directory in [&source_root, &execution_root, &run_parent] {
            std::fs::create_dir(directory).unwrap();
        }
        std::fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  task:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        let workflow = resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let admitted = admit_workflow(
            workflow.clone(),
            ResolvedInputs::default(),
            execution_context_for_workflow(&workflow, execution_root, 1, CancellationSource::new())
                .unwrap(),
        )
        .unwrap();
        let run_directory = run_parent.join("owned");
        let owned_run = InitialLocalRun::create(&run_directory, &admitted).unwrap();
        let lock_path = run_directory.join("run.lock");
        assert_run_lock_available(&lock_path, false);

        let clock = SystemObservationClock;
        let view = WorkflowRunViewModel::new(
            &workflow,
            1,
            RunTimingObservation::new(clock.sample()),
            clock,
        );
        let host = ActiveRunHost::Tui {
            view: view.clone(),
            terminal: None,
            failure: None,
            config: Box::new(PresentationConfig {
                requested_mode: RequestedPresentationMode::Automatic,
                color: ColorChoice::Never,
                capabilities: TerminalCapabilities {
                    stdin_is_terminal: true,
                    stdout_is_terminal: true,
                    stderr_is_terminal: true,
                    stdout_width: Some(80),
                    stderr_width: Some(80),
                    term: Some("xterm".into()),
                    no_color: None,
                },
                standard_input_reserved: false,
            }),
            leaf: ExecutionLeaf::Run,
        };
        assert!(!view.snapshot().quit_eligible);

        let released_ownership = owned_run.release();
        assert_run_lock_available(&lock_path, true);
        assert!(!view.snapshot().quit_eligible);

        host.mark_adapter_lifecycle_completed(released_ownership);
        assert!(view.snapshot().quit_eligible);
    }

    fn assert_run_lock_available(path: &Path, expected: bool) {
        let output = ProcessCommand::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "cli::workflow::run::create::tests::run_lock_probe_fixture",
            ])
            .env("UM_TEST_RUN_LOCK_PATH", path)
            .env(
                "UM_TEST_RUN_LOCK_AVAILABLE",
                if expected { "true" } else { "false" },
            )
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "run.lock probe failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "launched as a run.lock ownership probe"]
    fn run_lock_probe_fixture() {
        let path = std::env::var_os("UM_TEST_RUN_LOCK_PATH")
            .unwrap_or_else(|| panic!("UM_TEST_RUN_LOCK_PATH must be set"));
        let expected = std::env::var("UM_TEST_RUN_LOCK_AVAILABLE").unwrap() == "true";
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let available = fcntl_lock(&lock, FlockOperation::NonBlockingLockExclusive).is_ok();
        assert_eq!(available, expected);
    }

    // REQ-LOCALRUN-138
    #[tokio::test]
    async fn execution_handoff_does_not_wait_for_a_blocked_plain_header() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        std::fs::create_dir(&source_root).unwrap();
        std::fs::write(
            source_root.join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  step:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
        let workflow = resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let monotonic = um_support::monotonic_now();
        let opened = timing_point(monotonic, "2026-08-02T12:01:43.5Z", 0);
        let initialized = timing_point(monotonic, "2026-08-02T12:01:44Z", 500);
        let terminal = timing_point(monotonic, "2026-08-02T12:01:44.03Z", 530);
        let clock = ControlledObservationClock::new(opened);
        let flushed = Arc::new(AtomicBool::new(false));
        let (entered, waiting) = std::sync::mpsc::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));

        let prepared = initialize_execution_presentation(clock.clone(), || {
            let presentation = WorkflowRunOutput::new(
                PresentationConfig {
                    requested_mode: RequestedPresentationMode::Plain,
                    color: ColorChoice::Never,
                    capabilities: TerminalCapabilities {
                        stdin_is_terminal: false,
                        stdout_is_terminal: false,
                        stderr_is_terminal: false,
                        stdout_width: None,
                        stderr_width: None,
                        term: None,
                        no_color: None,
                    },
                    standard_input_reserved: false,
                },
                DelayedHeaderWriter {
                    clock: clock.clone(),
                    completed_at: initialized,
                    flushed: flushed.clone(),
                    entered,
                    gate: gate.clone(),
                },
                io::sink(),
            )
            .start_for_result(&workflow, "result", 1, clock.clone())?;
            let timing = RunTimingObservation::new(presentation.opened_at());
            Ok(PreparedExecutionPresentation {
                observer: presentation,
                host: (),
                timing,
            })
        })
        .unwrap();

        let wait = tokio::task::spawn_blocking(move || waiting.recv().unwrap());
        wait.await.unwrap();
        assert!(!flushed.load(Ordering::SeqCst));
        prepared.timing.record(&terminal_transition(), terminal);
        let timing = observed_run_timing(&prepared.timing.snapshot()).unwrap();
        assert_eq!(timing.started_at, opened.utc);
        assert_eq!(timing.duration, Duration::from_millis(530));
        let (lock, wake) = &*gate;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        prepared.observer.flush_pending().await;
        assert!(flushed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn timing_observer_excludes_presentation_opening_and_uses_terminal_transition() {
        let monotonic = um_support::monotonic_now();
        let opened = timing_point(monotonic, "2026-08-02T12:01:43.5Z", 0);
        let started = timing_point(monotonic, "2026-08-02T12:01:44Z", 500);
        let step_started = timing_point(monotonic, "2026-08-02T12:01:44.01Z", 510);
        let step_finished = timing_point(monotonic, "2026-08-02T12:01:44.02Z", 520);
        let terminal = timing_point(monotonic, "2026-08-02T12:01:44.03Z", 530);
        let observations = Arc::new(AtomicUsize::new(0));
        let cancellation = CancellationSource::new();
        let timing = RunTimingObservation::new(opened);
        timing.mark_execution_started(started);
        let observer = TimingObserver::new(
            RecordingPresentation {
                observations: observations.clone(),
                failed: false,
            },
            cancellation,
            timing,
            ScriptedClock::new([step_started, step_finished, terminal]),
        );

        observer
            .observe(step_transition(
                StepStateKind::Pending,
                StepStateKind::Starting,
            ))
            .await;
        observer
            .observe(step_transition(
                StepStateKind::CapturingOutputs,
                StepStateKind::Succeeded,
            ))
            .await;
        observer.observe(terminal_transition()).await;

        assert_eq!(observations.load(Ordering::SeqCst), 3);
        let timing = observer.snapshot();
        let step = timing.steps.get("step").unwrap();
        assert_eq!(step.started.utc, step_started.utc);
        assert_eq!(step.finished, Some(step_finished.monotonic));
        let run = observed_run_timing(&timing).unwrap();
        assert_eq!(run.started_at, started.utc);
        assert_eq!(run.finished_at, terminal.utc);
        assert_eq!(run.duration, Duration::from_millis(30));
    }

    #[tokio::test]
    async fn presentation_failure_requests_cancellation_without_replacing_a_signal() {
        let monotonic = um_support::monotonic_now();
        let cancellation = CancellationSource::new();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        let observed_at = timing_point(monotonic, "2026-08-02T12:01:44Z", 0);
        let timing = RunTimingObservation::new(observed_at);
        timing.mark_execution_started(observed_at);
        let observer = TimingObserver::new(
            RecordingPresentation {
                observations: Arc::new(AtomicUsize::new(0)),
                failed: true,
            },
            cancellation.clone(),
            timing,
            ScriptedClock::new([observed_at]),
        );

        observer
            .observe(step_transition(
                StepStateKind::Pending,
                StepStateKind::Starting,
            ))
            .await;

        assert_eq!(
            cancellation.cancellation_reason(),
            Some(CancellationReason::UserRequest)
        );
    }

    fn timing_point(monotonic: Instant, utc: &str, milliseconds: u64) -> ObservationTime {
        ObservationTime {
            utc: OffsetDateTime::parse(utc, &Rfc3339).unwrap(),
            monotonic: monotonic + Duration::from_millis(milliseconds),
        }
    }

    fn step_transition(
        from: StepStateKind,
        to: StepStateKind,
    ) -> ExecutionObservation<ExecutionInstant> {
        ExecutionObservation::Transition(Box::new(TransitionObservation {
            event: TransitionEvent::Step {
                sequence: TransitionSequence::default(),
                step: "step".to_owned(),
                role: WorkflowNodeRole::Step,
                failure_policy: FailurePolicy::Required,
                from,
                to,
            },
            step: None,
            executions: ObservedExecutions::default(),
        }))
    }

    fn terminal_transition() -> ExecutionObservation<ExecutionInstant> {
        ExecutionObservation::Transition(Box::new(TransitionObservation {
            event: TransitionEvent::Workflow {
                sequence: TransitionSequence::default(),
                from: WorkflowState::Executing {
                    gate: SchedulingGate::Open,
                },
                to: Box::new(WorkflowState::Succeeded),
            },
            step: None,
            executions: ObservedExecutions::default(),
        }))
    }
}
