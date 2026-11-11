//! What a command that moves a waiting definition run to a successor shares
//! (REQ-LOCALRUN-194 to REQ-LOCALRUN-197, INV-ENGINE-26): inspecting the predecessor without
//! its lock, reading the definition's current revision, the pre-check of the continuation
//! against it under the predecessor's lock, the take-create-name-supersede sequence and then
//! the hand-off to `continue_run` for `restart` or the `upgraded` report for `upgrade`. Nothing
//! is written before the pre-check passes, so every refusal leaves both runs as they were, and
//! `reconcile` completes or undoes a move a command died in (REQ-LOCALRUN-196).

use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::io::Write as _;

use anyhow::{Context, anyhow};
use serde::Serialize;
use um_execution::{
    AdmittedWorkflow, CancellationSource, InitialLocalRun, LocalContinuationOpen, NoConditions,
    PendingLocalContinuation, PresentationConfig, PresentationMode, ResolvedInput,
    RetryIneligibilityReason, RunState, SubjectClaim, SubjectError, SuccessorCarry, WaitInspection,
    WaitRecord, WorkflowHome, WorkflowRunOutput, WorkflowRunRecord, acquire_local_continuation,
    await_run_lock_gate, claim_subject, inspect_wait, normalize_terminal_path,
    normalize_terminal_scalar, read_local_run_status, resolve_workflow_file_with_agent_profiles,
    subject_slot,
};

use super::create::{
    BlockingOperationError, DefinitionTree, ExecutionLeaf, blocking_operation, definition_tree,
    rejection_exit, text_bindings, tree_digest,
};
use super::resume::{
    Continuation, ContinuationCandidate, Unadmitted, admit_continuation, continuation_output,
    continue_run,
};
use crate::exit_code::ExitCode;

type CommandResult = super::super::super::CommandResult;

/// What the command asks of the successor.
pub(super) enum SuccessorRequest {
    /// `restart`: the step the successor continues from, the `--input-text` values set on top
    /// of the projected inputs and its execution root (REQ-LOCALRUN-194).
    Restart {
        from: String,
        input_text: Vec<(String, String)>,
        execution_root: Option<PathBuf>,
    },
    /// `upgrade`: the successor carries the predecessor's wait and starts no attempt
    /// (REQ-LOCALRUN-197).
    Upgrade,
}

impl SuccessorRequest {
    /// The leaf whose command the request's documents name.
    fn leaf(&self) -> ExecutionLeaf {
        match self {
            Self::Restart { .. } => ExecutionLeaf::Restart,
            Self::Upgrade => ExecutionLeaf::Upgrade,
        }
    }
}

/// The `--input-text NAME VALUE` pairs as `create` reads them (`text_bindings`), a usage
/// error (REQ-LOCALRUN-80) for a name given twice as well.
pub(super) fn text_inputs(values: &[OsString]) -> Result<Vec<(String, String)>, anyhow::Error> {
    let mut inputs: Vec<(String, String)> = Vec::new();
    for (name, value) in text_bindings(values)? {
        if inputs.iter().any(|(given, _)| given == name) {
            return Err(anyhow!(
                "one input name has duplicate --input-text bindings"
            ));
        }
        inputs.push((name.to_owned(), value.to_owned()));
    }
    Ok(inputs)
}

/// Starts the successor of the waiting run at `requested` (REQ-LOCALRUN-194,
/// REQ-LOCALRUN-197): a restarted successor continues, and the command's documents are
/// `continue_run`'s once it exists; an upgraded one is reported `upgraded`. A refusal before
/// the successor exists is reported at phase `successor`, `definition`, `tree` or
/// `continuation`.
pub(super) async fn start(
    requested: PathBuf,
    request: SuccessorRequest,
    config: PresentationConfig,
) -> CommandResult {
    let cancellation = CancellationSource::new();
    let signal_task = super::create::start_signal_observation(
        cancellation.clone(),
        super::create::UnixSignals::new()?,
    );
    // REQ-LOCALRUN-135: a signal reads the predecessor's control journal from here on, so one
    // that arrives while the successor is made is an interruption, never a cancel; the
    // successor's own journal is watched once it continues.
    let journal = signal_task.watch(&requested);
    match create(requested, request, &config, &cancellation).await {
        Ok(Created::Continue {
            continuation,
            restarted_element,
        }) => {
            drop(journal);
            // REQ-LOCALRUN-199: written once the successor exists, so a refusal leaves stderr
            // to its report, and before the attempt; the advisory is nonblocking, so a failed
            // write does not stop the run.
            if let Some(restarted) = restarted_element {
                let _ = writeln!(io::stderr(), "{restarted}");
            }
            continue_run(continuation, config, cancellation, signal_task).await
        }
        Ok(Created::Upgraded(upgraded)) => {
            signal_task.abort();
            write_upgraded(&config, &upgraded)
        }
        Err(finished) => {
            signal_task.abort();
            finished
        }
    }
}

/// The successor once it exists: a restart's continuation, with the fan-out element it no
/// longer waits at, or an upgraded run to report.
enum Created {
    Continue {
        continuation: Continuation,
        restarted_element: Option<RestartedElement>,
    },
    Upgraded(Upgraded),
}

/// The fan-out step a restart names while its predecessor waits inside it at an element past
/// its first (REQ-LOCALRUN-199): the step runs again from element 1, and its display is the
/// advisory naming `upgrade`, which would have kept the wait at `element`.
struct RestartedElement {
    step: String,
    element: u64,
}

impl RestartedElement {
    /// The element of `from` the predecessor's `wait` holds, when `from` is the fan-out it
    /// waits inside at element 2 or later.
    fn of(wait: &WaitRecord, from: &str) -> Option<Self> {
        let (step, element) = wait.fan_out_element()?;
        (step == from && element >= 2).then(|| Self {
            step: step.to_owned(),
            element,
        })
    }
}

impl fmt::Display for RestartedElement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let step = normalize_terminal_scalar(self.step.as_bytes());
        let rerun = match self.element.saturating_sub(1) {
            1 => "element 1".to_owned(),
            last => format!("elements 1 to {last}"),
        };
        write!(
            formatter,
            "! Step {step} runs again from its first element, {rerun} included. To keep a waiting run at element {} instead, run um workflow run upgrade RUN and resolve its wait there.",
            self.element
        )
    }
}

/// A successor `upgrade` created waiting on its predecessor's wait (REQ-LOCALRUN-197).
struct Upgraded {
    run_directory: PathBuf,
    run: String,
    predecessor: String,
    wait: WaitRecord,
}

/// The `--json` document of `um workflow run upgrade` (REQ-LOCALRUN-197).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UpgradedDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    run_directory: &'a str,
    run: &'a str,
    predecessor: &'a str,
    wait: &'a WaitRecord,
}

/// Reports the upgraded successor and exits 0: with `--json` one document, otherwise one
/// line naming the successor, its predecessor and the carried wait.
fn write_upgraded(config: &PresentationConfig, upgraded: &Upgraded) -> CommandResult {
    if config.mode() == PresentationMode::Json {
        let run_directory = upgraded
            .run_directory
            .to_str()
            .ok_or_else(|| anyhow!("normalized run path is not UTF-8"))?;
        super::super::super::write_pretty_json(&UpgradedDocument {
            schema_version: 1,
            command: super::resume::command_name(ExecutionLeaf::Upgrade),
            outcome: "upgraded",
            exit_status: 0,
            run_directory,
            run: &upgraded.run,
            predecessor: &upgraded.predecessor,
            wait: &upgraded.wait,
        })
        .map_err(anyhow::Error::new)?;
    } else {
        let wait = &upgraded.wait;
        let mut stdout = io::stdout().lock();
        writeln!(
            stdout,
            "run {} upgraded · from {} · {} at step {} since {}",
            normalize_terminal_path(&upgraded.run_directory),
            normalize_terminal_scalar(upgraded.predecessor.as_bytes()),
            normalize_terminal_scalar(wait.kind_name().as_bytes()),
            normalize_terminal_scalar(wait.step_path().as_bytes()),
            normalize_terminal_scalar(wait.since.as_bytes()),
        )
        .map_err(anyhow::Error::new)?;
        stdout.flush().map_err(anyhow::Error::new)?;
    }
    Ok(ExitCode::Success)
}

/// The `wait_resolved` refusal (REQ-LOCALRUN-197), decided from the inspection before the lock
/// and again from the controls under it.
const WAIT_RESOLVED: (&str, &str) = (
    "wait_resolved",
    "A recorded control already resolves the run's wait; resume the run instead.",
);

/// The waiting run a successor supersedes, as inspected without its lock.
struct Predecessor {
    run_directory: PathBuf,
    record: WorkflowRunRecord,
    definition: String,
    /// The wait it holds, which must still be its wait under the lock.
    wait: WaitRecord,
    /// Whether a recorded control or observed event already resolves the wait.
    resolved: bool,
}

/// Why the predecessor does not admit a successor: a refusal at phase `successor`, or an
/// operational failure.
enum Unsuitable {
    Refused {
        run_directory: PathBuf,
        run: Option<String>,
        code: &'static str,
        message: &'static str,
    },
    Failed(anyhow::Error),
}

/// Reads the run at `requested` without its lock: it must have a run record naming a
/// definition, and wait.
fn inspect(requested: &Path) -> Result<Predecessor, Unsuitable> {
    let snapshot = read_local_run_status(requested).map_err(|error| {
        Unsuitable::Failed(anyhow!(
            "read workflow run {}: {}",
            normalize_terminal_path(requested),
            error.code.as_str()
        ))
    })?;
    let refused = |run: Option<&str>, code, message| Unsuitable::Refused {
        run_directory: snapshot.run_directory.clone(),
        run: run.map(str::to_owned),
        code,
        message,
    };
    let Some(record) = snapshot.record.clone() else {
        return Err(refused(
            None,
            "run_record_missing",
            "The run has no run record; only a waiting run of a definition has a successor.",
        ));
    };
    let record: WorkflowRunRecord = serde_json::from_value(record)
        .context("read the run's record")
        .map_err(Unsuitable::Failed)?;
    let run = Some(record.id.as_str());
    let Some(definition) = record.definition.clone() else {
        return Err(refused(
            run,
            "definition_missing",
            "The run was started by path; only a run of a definition has a successor.",
        ));
    };
    let probe = inspect_wait(&snapshot.run_directory, &NoConditions)
        .context("inspect the run's wait")
        .map_err(Unsuitable::Failed)?;
    let (wait, resolved) = match probe.inspection {
        WaitInspection::Satisfied { wait, .. } => (wait, true),
        WaitInspection::Held { wait }
        | WaitInspection::RuleInvalid { wait, .. }
        | WaitInspection::ConditionUnavailable { wait, .. } => (wait, false),
        WaitInspection::Locked(_) => {
            return Err(refused(
                run,
                "run_locked",
                "A live process holds the run; wait for it to finish or cancel it.",
            ));
        }
        WaitInspection::NoWait {
            state: Some(RunState::Exited),
            ..
        } => return Err(refused(run, "run_exited", "The run has exited.")),
        WaitInspection::NoWait { .. } | WaitInspection::Continued { .. } => {
            return Err(refused(
                run,
                "run_not_waiting",
                "The run does not wait; cancel or resume it instead.",
            ));
        }
    };
    Ok(Predecessor {
        run_directory: probe.run_directory,
        record,
        definition,
        wait,
        resolved,
    })
}

/// Everything up to the successor's first own attempt: the created successor, or the
/// command's finished result.
async fn create(
    requested: PathBuf,
    request: SuccessorRequest,
    config: &PresentationConfig,
    cancellation: &CancellationSource,
) -> Result<Created, CommandResult> {
    let leaf = request.leaf();
    let output = |run_directory: &Path| continuation_output(config.clone(), leaf, run_directory);
    let refuse = |run_directory: &Path, run: Option<&str>, code, message: &str| {
        rejection_exit(output(run_directory).render_successor_refusal(code, message, run))
    };
    let predecessor = match blocking_operation(move || inspect(&requested)).await {
        Ok(predecessor) => predecessor,
        Err(BlockingOperationError::Operation(Unsuitable::Refused {
            run_directory,
            run,
            code,
            message,
        })) => return Err(refuse(&run_directory, run.as_deref(), code, message)),
        Err(BlockingOperationError::Operation(Unsuitable::Failed(error))) => {
            return Err(Err(error.into()));
        }
        Err(BlockingOperationError::WorkerUnavailable) => {
            return Err(Err(anyhow!("inspect workflow run").into()));
        }
    };
    let run_directory = predecessor.run_directory.clone();
    let predecessor_id = predecessor.record.id.clone();
    let refuse_predecessor =
        |code, message: &str| refuse(&run_directory, Some(&predecessor_id), code, message);
    // REQ-LOCALRUN-196: a run that is itself a successor whose move did not finish is settled
    // before it gets a successor of its own.
    let lineage_directory = run_directory.clone();
    match blocking_operation(move || reconcile(&lineage_directory)).await {
        Ok(Lineage::Abandoned { .. }) => {
            return Err(refuse_predecessor(ABANDONED_CODE, ABANDONED_MESSAGE));
        }
        Ok(Lineage::PredecessorLocked) => {
            return Err(refuse_predecessor(
                PREDECESSOR_LOCKED_CODE,
                PREDECESSOR_LOCKED_MESSAGE,
            ));
        }
        Ok(Lineage::Settled | Lineage::Superseded) => {}
        Err(BlockingOperationError::Operation(error)) => return Err(Err(error.into())),
        Err(BlockingOperationError::WorkerUnavailable) => {
            return Err(Err(anyhow!("reconcile the run's predecessor").into()));
        }
    }

    // REQ-LOCALRUN-173: the current revision's tree, read and checked as `create` reads it.
    let definition_id = predecessor.definition.clone();
    let definition = match blocking_operation(move || definition_tree(&definition_id)).await {
        Ok(definition) => definition,
        Err(BlockingOperationError::Operation(failure)) => {
            return Err(rejection_exit(failure.render(output(&run_directory))));
        }
        Err(BlockingOperationError::WorkerUnavailable) => {
            return Err(Err(anyhow!("read workflow definition").into()));
        }
    };
    if matches!(request, SuccessorRequest::Upgrade) {
        // REQ-LOCALRUN-197: an upgrade moves a run to a newer revision, carrying a wait no
        // control resolves yet.
        if predecessor
            .record
            .revision
            .is_some_and(|revision| definition.revision <= revision)
        {
            return Err(refuse_predecessor(
                "revision_current",
                "The run is already on its definition's current revision; nothing to upgrade.",
            ));
        }
        if predecessor.resolved {
            return Err(refuse_predecessor(WAIT_RESOLVED.0, WAIT_RESOLVED.1));
        }
    }
    if !definition
        .tree
        .workflow_files
        .contains(&predecessor.record.workflow)
    {
        return Err(refuse_predecessor(
            "workflow_not_in_tree",
            "The run's workflow file is not a workflow file of the definition's current revision.",
        ));
    }

    let locked_directory = run_directory.clone();
    let pending = match tokio::task::spawn_blocking(move || {
        // A test seam between the inspection and the lock; nothing outside `test-fixtures`.
        await_run_lock_gate()?;
        acquire_local_continuation(&locked_directory)
    })
    .await
    {
        Ok(Ok(LocalContinuationOpen::Acquired(pending))) => *pending,
        Ok(Ok(LocalContinuationOpen::Rejected(rejection)))
            if rejection.reason() == RetryIneligibilityReason::RunLocked =>
        {
            return Err(refuse_predecessor(
                "run_locked",
                "A live process holds the run; wait for it to finish or cancel it.",
            ));
        }
        Ok(Ok(LocalContinuationOpen::Rejected(rejection))) => {
            return Err(rejection_exit(
                output(&run_directory).render_retry_rejection(&rejection),
            ));
        }
        Ok(Err(error)) => return Err(Err(anyhow::Error::new(error).into())),
        Err(error) => return Err(Err(anyhow::Error::new(error).into())),
    };
    // The inspection ran without the lock: the wait it found must still be the run's.
    if !matches!(pending.wait(), Ok(Some(current)) if current.id() == predecessor.wait.id()) {
        let result = refuse_predecessor(
            "wait_changed",
            "The run's wait changed while the command inspected it; run the command again.",
        );
        return Err(settled(pending, result).await);
    }
    match precheck(
        &pending,
        &predecessor,
        definition,
        request,
        cancellation,
        config,
    )
    .await
    {
        Ok(admission) => {
            // REQ-LOCALRUN-135: a signal before the successor's directory exists ends the
            // command and creates no run; the predecessor is settled and still waits.
            if let Some(reason) = cancellation.cancellation_reason() {
                let failure = super::create::signalled_before_creation(Some(reason));
                return Err(settled(pending, Err(failure)).await);
            }
            let created =
                tokio::task::spawn_blocking(move || create_and_supersede(admission, pending)).await;
            match created {
                Ok(Ok(continuation)) => Ok(continuation),
                Ok(Err(Creation::Subject(error))) => Err(subject_refusal(
                    output(&run_directory),
                    &predecessor.definition,
                    error,
                )),
                Ok(Err(Creation::Failed(error))) => Err(Err(error.into())),
                Err(error) => Err(Err(anyhow::Error::new(error)
                    .context("create the successor run")
                    .into())),
            }
        }
        Err(result) => Err(settled(pending, result).await),
    }
}

/// A successor admitted against the current revision, before anything is written.
struct Admission {
    home: WorkflowHome,
    definition: String,
    record: WorkflowRunRecord,
    run_directory: PathBuf,
    admitted: AdmittedWorkflow,
    next: Next,
}

/// What follows the successor's creation.
enum Next {
    /// `restart`: its first own attempt continues from `from` in `execution_root`.
    Continue {
        from: String,
        execution_root: PathBuf,
        restarted_element: Option<RestartedElement>,
    },
    /// `upgrade`: it carries `wait` and is reported.
    Report { wait: WaitRecord },
}

/// Admits the successor against the current revision's workflow under the predecessor's
/// lock. For `restart` (REQ-LOCALRUN-194, REQ-CONTINUE-79) the continuation from `STEP`:
/// `STEP` and its dependents re-executed, the rest inherited from the predecessor's attempts,
/// the predecessor's inputs projected with each `--input-text` on top. For `upgrade`
/// (REQ-LOCALRUN-197) the carried state resolved against the workflow, every miss listed, then
/// the continuations from the wait's `then` and `onHandBack`. A refusal is the command's
/// result, for the caller to return once the predecessor is settled.
async fn precheck(
    pending: &PendingLocalContinuation,
    predecessor: &Predecessor,
    definition: DefinitionTree,
    request: SuccessorRequest,
    cancellation: &CancellationSource,
    config: &PresentationConfig,
) -> Result<Admission, CommandResult> {
    let leaf = request.leaf();
    let DefinitionTree {
        home,
        id,
        revision,
        copy,
        tree,
    } = definition;
    let file = copy.join(&predecessor.record.workflow);
    // A successor inherits the run's retained replacement, as a continuation does.
    let replacement = pending
        .previous_definition()
        .agent_profile_replacement
        .as_ref()
        .map(|replacement| replacement.document.clone());
    let workflow = match blocking_operation(move || {
        resolve_workflow_file_with_agent_profiles(&copy, &file, replacement.as_ref())
    })
    .await
    {
        Ok(workflow) => workflow,
        Err(BlockingOperationError::Operation(failure)) => {
            return Err(rejection_exit(
                continuation_output(config.clone(), leaf, pending.run_directory())
                    .render_continuation_resolution_rejection(
                        pending.run_directory(),
                        pending.prior_attempt_number(),
                        &failure,
                    ),
            ));
        }
        Err(BlockingOperationError::WorkerUnavailable) => {
            return Err(Err(
                anyhow!("resolve the current revision's workflow").into()
            ));
        }
    };
    let mut inputs = pending.candidate_inputs(&workflow);
    let (starts, next) = match request {
        SuccessorRequest::Restart {
            from,
            input_text,
            execution_root,
        } => {
            // REQ-LOCALRUN-80: the successor keeps the predecessor's subject and slot.
            if let Some(subject) = &workflow.definition.subject
                && input_text.iter().any(|(name, _)| name == subject)
            {
                return Err(Err(super::super::super::CommandFailure::with_exit_code(
                    anyhow!("--input-text names the input the workflow names as its subject"),
                    ExitCode::UsageError,
                )));
            }
            for (name, value) in input_text {
                inputs.insert(name, ResolvedInput::Text(Arc::from(value)));
            }
            let execution_root =
                execution_root.unwrap_or_else(|| pending.prior_execution_root().to_owned());
            let restarted_element = RestartedElement::of(&predecessor.wait, &from);
            (
                vec![from.clone()],
                Next::Continue {
                    from,
                    execution_root,
                    restarted_element,
                },
            )
        }
        SuccessorRequest::Upgrade => {
            let wait = predecessor.wait.clone();
            // REQ-LOCALRUN-197: the inspection ran without the lock, so a control recorded
            // since is asked about here; copied to the successor it would resolve nothing
            // (REQ-LOCALRUN-195).
            let resolving = pending.resolving_control(&wait).map_err(|error| {
                Err(anyhow::Error::new(error)
                    .context("read the run's controls under its lock")
                    .into())
            })?;
            if resolving.is_some() {
                return Err(rejection_exit(
                    continuation_output(config.clone(), leaf, pending.run_directory())
                        .render_successor_refusal(
                            WAIT_RESOLVED.0,
                            WAIT_RESOLVED.1,
                            Some(&predecessor.record.id),
                        ),
                ));
            }
            let misses = pending.resolve_carry(&workflow, &inputs).map_err(|error| {
                Err(anyhow::Error::new(error)
                    .context("resolve the run's carried state against the current revision")
                    .into())
            })?;
            if !misses.is_empty() {
                return Err(rejection_exit(
                    continuation_output(config.clone(), leaf, pending.run_directory())
                        .render_successor_refusals(&misses, &predecessor.record.id),
                ));
            }
            let starts = std::iter::once(wait.then.clone())
                .chain(wait.on_hand_back.clone())
                .collect();
            (starts, Next::Report { wait })
        }
    };
    let execution_root = match &next {
        Next::Continue { execution_root, .. } => execution_root.clone(),
        Next::Report { .. } => pending.prior_execution_root().to_owned(),
    };
    let digest = tree_digest(&tree);
    // Each start is admitted on its own, as the continuation that resolves the wait would
    // be; the first admission is the successor's. Its own continuation admits it again and
    // prints the advisories.
    let mut admitted = None;
    for from in &starts {
        let candidate = ContinuationCandidate {
            workflow: &workflow,
            from: std::slice::from_ref(from),
            inputs: inputs.clone(),
            execution_root: execution_root.clone(),
            tree: Some(tree.clone()),
        };
        match admit_continuation(pending, candidate, cancellation, (config, leaf)).await {
            Ok((candidate, _)) => {
                admitted.get_or_insert(candidate);
            }
            Err(Unadmitted::Rejected(result)) => return Err(rejection_exit(result)),
            Err(Unadmitted::Failed(message)) => return Err(Err(anyhow!(message).into())),
        }
    }
    let admitted = admitted.ok_or_else(|| Err(anyhow!("admit the successor").into()))?;
    if let Err(error) = pending.validate_execution_root(&admitted) {
        return Err(super::create::diagnose(error));
    }
    let mut record = WorkflowRunRecord::new(
        Some((id.clone(), revision)),
        &digest,
        &predecessor.record.workflow,
        &super::super::login_name(),
    );
    record.subject.clone_from(&predecessor.record.subject);
    record.predecessor = Some(predecessor.record.id.clone());
    record.attempts = pending.prior_attempt_number();
    if let Next::Report { .. } = next {
        // REQ-LOCALRUN-197: the successor waits where the predecessor waits.
        record.state = RunState::Waiting;
        record.position.clone_from(&predecessor.record.position);
        record.edges.clone_from(&predecessor.record.edges);
        record.external.clone_from(&predecessor.record.external);
    }
    Ok(Admission {
        run_directory: home.runs_directory(&id).join(&record.id),
        home,
        definition: id,
        record,
        admitted,
        next,
    })
}

/// Why the successor was not created once admitted.
enum Creation {
    /// The slot was refused under the index's lock (REQ-WFDEF-28).
    Subject(SubjectError),
    Failed(anyhow::Error),
}

/// Takes the subject index's lock as a claimant naming the predecessor, creates the
/// successor, names it in the slot, supersedes the predecessor and releases the lock
/// (REQ-LOCALRUN-196, INV-ENGINE-26): the slot never names a run that does not exist, so a
/// death anywhere in the sequence leaves the subject busy, and `reconcile` finishes or undoes
/// the move on the successor's next continuation. A restarted successor's continuation from
/// `STEP` follows, and an upgraded one, carrying the wait, is reported (REQ-LOCALRUN-197). A
/// failure before the successor exists settles the predecessor, which keeps its slot.
fn create_and_supersede(
    admission: Admission,
    pending: PendingLocalContinuation,
) -> Result<Created, Creation> {
    let Admission {
        home,
        definition,
        record,
        run_directory,
        admitted,
        next,
    } = admission;
    let carry = match next {
        Next::Continue { .. } => SuccessorCarry::Continue,
        Next::Report { .. } => SuccessorCarry::Wait,
    };
    let predecessor = record.predecessor.clone().unwrap_or_default();
    if let Err(error) = home.create_runs_directory(&definition) {
        return Err(settle_before_creation(
            pending,
            Creation::Failed(anyhow::Error::new(error)),
        ));
    }
    // REQ-WFDEF-28: the claim names the predecessor, so it may take the slot the predecessor
    // holds; the index lock is held, and the slot still names the predecessor, until the
    // successor's directory exists.
    let claim = match &record.subject {
        Some(subject) => {
            match SubjectClaim::take(&home, &definition, subject, Some(&predecessor)) {
                Ok(claim) => Some(claim),
                Err(error) => {
                    return Err(settle_before_creation(pending, Creation::Subject(error)));
                }
            }
        }
        None => None,
    };
    let successor = match InitialLocalRun::create_successor(
        &run_directory,
        &admitted,
        &record,
        &pending,
        carry,
    ) {
        Ok(successor) => successor,
        Err(error) => {
            // Nothing names the successor: the predecessor keeps its slot and still waits.
            drop(claim);
            let failure = anyhow::Error::new(error).context(format!(
                "create successor run {}",
                normalize_terminal_path(&run_directory)
            ));
            return Err(settle_before_creation(pending, Creation::Failed(failure)));
        }
    };
    // REQ-LOCALRUN-196: the slot names the successor only now that its directory exists; a
    // failure from here on leaves both runs for `reconcile` to settle.
    if let Some(claim) = &claim
        && let Err(error) = claim.name(&record.id)
    {
        return Err(Creation::Failed(anyhow::Error::new(error).context(format!(
            "name successor run {} in the subject's slot; its next continuation completes the move",
            record.id
        ))));
    }
    pending.supersede(&record.id).map_err(|error| {
        Creation::Failed(anyhow::Error::new(error).context(format!(
            "supersede run {predecessor} by its successor {}; the successor's next continuation completes the move",
            record.id
        )))
    })?;
    drop(claim);
    Ok(match next {
        Next::Continue {
            from,
            execution_root,
            restarted_element,
        } => Created::Continue {
            continuation: Continuation {
                run_directory: successor,
                from_steps: vec![from],
                workflow: None,
                execution_root: Some(execution_root),
                leaf: ExecutionLeaf::Restart,
                edge: None,
            },
            restarted_element,
        },
        Next::Report { wait } => Created::Upgraded(Upgraded {
            run_directory: successor,
            run: record.id,
            predecessor,
            wait,
        }),
    })
}

pub(super) const ABANDONED_CODE: &str = "successor_abandoned";
pub(super) const ABANDONED_MESSAGE: &str = "The run's predecessor continued or exited without being superseded by this run, which is withdrawn; address the subject again.";
pub(super) const PREDECESSOR_LOCKED_CODE: &str = "predecessor_locked";
pub(super) const PREDECESSOR_LOCKED_MESSAGE: &str = "A live process holds the run's predecessor, which this run has not superseded yet; run the command again.";

/// What `reconcile` found of a run's predecessor (REQ-LOCALRUN-196).
pub(super) enum Lineage {
    /// The run has no predecessor, or its predecessor is gone or already names it as
    /// `successor`.
    Settled,
    /// The predecessor still waited without naming the run: it is superseded now and the
    /// subject's slot names the run.
    Superseded,
    /// The predecessor continued or exited without naming the run: the run is ended `exited`
    /// `withdrawn` without an attempt, at `attempt_number`, and the slot given back to the
    /// predecessor.
    Abandoned { attempt_number: u64 },
    /// A live process holds the predecessor; nothing changed.
    PredecessorLocked,
}

/// Completes or undoes a move to the successor at `run_directory` that the command performing
/// it did not finish (REQ-LOCALRUN-196, INV-ENGINE-26): the sequence names the slot after the
/// successor exists and supersedes the predecessor after that, so a death between leaves a
/// predecessor that still waits and a successor it does not name. Reads the records without a
/// lock first, so a run that is no successor, or one its predecessor names, costs two reads;
/// otherwise takes the successor's lock (a successor a live process holds is left to that
/// process) and the predecessor's, and settles the pair as `Lineage` states.
pub(super) fn reconcile(run_directory: &Path) -> Result<Lineage, anyhow::Error> {
    let Some(record) = record_of(run_directory) else {
        return Ok(Lineage::Settled);
    };
    let (Some(predecessor), Some(definition)) =
        (record.predecessor.clone(), record.definition.clone())
    else {
        return Ok(Lineage::Settled);
    };
    let Some(predecessor_directory) = run_directory
        .parent()
        .map(|runs| runs.join(&predecessor))
        .filter(|directory| directory.is_dir())
    else {
        return Ok(Lineage::Settled);
    };
    let Some(previous) = record_of(&predecessor_directory) else {
        return Ok(Lineage::Settled);
    };
    if previous.successor.as_deref() == Some(record.id.as_str()) {
        return Ok(Lineage::Settled);
    }
    let pending = match acquire_local_continuation(run_directory)
        .with_context(|| format!("lock successor run {}", record.id))?
    {
        LocalContinuationOpen::Acquired(pending) => *pending,
        LocalContinuationOpen::Rejected(_) => return Ok(Lineage::Settled),
    };
    // The predecessor is read again under the successor's lock: another continuation of the
    // successor may have settled the pair meanwhile.
    let Some(previous) = record_of(&predecessor_directory) else {
        return Ok(Lineage::Settled);
    };
    if previous.successor.as_deref() == Some(record.id.as_str()) {
        return Ok(Lineage::Settled);
    }
    let home = WorkflowHome::from_environment().context("resolve the um home")?;
    let slot_holder = |subject: &str| -> Result<Option<String>, anyhow::Error> {
        Ok(subject_slot(&home, &definition, subject)
            .map_err(anyhow::Error::new)?
            .map(|holder| holder.run))
    };
    if previous.state == RunState::Waiting {
        let superseding = match acquire_local_continuation(&predecessor_directory)
            .with_context(|| format!("lock predecessor run {predecessor}"))?
        {
            LocalContinuationOpen::Acquired(superseding) => *superseding,
            LocalContinuationOpen::Rejected(rejection)
                if rejection.reason() == RetryIneligibilityReason::RunLocked =>
            {
                return Ok(Lineage::PredecessorLocked);
            }
            LocalContinuationOpen::Rejected(rejection) => {
                return Err(anyhow!(
                    "predecessor run {predecessor} admits no continuation: {}",
                    rejection.reason().as_str()
                ));
            }
        };
        superseding.supersede(&record.id).with_context(|| {
            format!("supersede run {predecessor} by its successor {}", record.id)
        })?;
        if let Some(subject) = &record.subject
            && slot_holder(subject)?.as_deref() != Some(record.id.as_str())
        {
            claim_subject(&home, &definition, subject, &record.id, Some(&predecessor))
                .with_context(|| {
                    format!("name successor run {} in the subject's slot", record.id)
                })?;
        }
        drop(pending);
        return Ok(Lineage::Superseded);
    }
    let attempt_number = pending.prior_attempt_number();
    pending
        .abandon()
        .with_context(|| format!("withdraw abandoned successor run {}", record.id))?;
    if let Some(subject) = &record.subject
        && slot_holder(subject)?.as_deref() == Some(record.id.as_str())
    {
        claim_subject(&home, &definition, subject, &predecessor, Some(&record.id))
            .with_context(|| format!("give the subject's slot back to {predecessor}"))?;
    }
    Ok(Lineage::Abandoned { attempt_number })
}

/// The run record at `run_directory`, `None` when the run has none or cannot be read.
fn record_of(run_directory: &Path) -> Option<WorkflowRunRecord> {
    read_local_run_status(run_directory)
        .ok()
        .and_then(|snapshot| snapshot.record)
        .and_then(|record| serde_json::from_value(record).ok())
}

/// Settles the predecessor after `failure` and returns it, carrying a settlement failure too.
fn settle_before_creation(pending: PendingLocalContinuation, failure: Creation) -> Creation {
    match pending.settle_abandoned() {
        Ok(()) => failure,
        Err(error) => {
            let failure = match failure {
                Creation::Subject(subject) => anyhow::Error::new(subject),
                Creation::Failed(failure) => failure,
            };
            Creation::Failed(failure.context(format!("settle the predecessor: {error}")))
        }
    }
}

/// A slot the successor could not take, at phase `definition` (REQ-LOCALRUN-173); a failure
/// to read or write the index is a command failure.
fn subject_refusal(
    output: WorkflowRunOutput<io::Stdout, io::Stderr>,
    definition: &str,
    error: SubjectError,
) -> CommandResult {
    let Some(code) = error.code() else {
        return Err(anyhow::Error::new(error).into());
    };
    let message = error.to_string();
    rejection_exit(output.render_subject_rejection(
        code,
        &message,
        Some(definition),
        None,
        error.run(),
    ))
}

/// Settles the predecessor without an attempt and returns `result`.
async fn settled(pending: PendingLocalContinuation, result: CommandResult) -> CommandResult {
    tokio::task::spawn_blocking(move || pending.settle_abandoned())
        .await
        .map_err(anyhow::Error::new)?
        .map_err(anyhow::Error::new)?;
    result
}
