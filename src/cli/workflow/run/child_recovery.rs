//! The repair continuation of a parent's failed child (REQ-CONTINUE-81, REQ-CONTINUE-82,
//! INV-ENGINE-27): a `control.retry` that resolves a repair wait back to the waiting step
//! continues the child the wait names in its own run directory rather than starting the stage
//! afresh. `prepare` verifies and admits that continuation under the parent's and the child's
//! locks before the parent's wait is consumed; `begin` starts the child's next attempt once
//! the parent's dispatcher requests the step.
//!
//! A wait the root carries from a nested run (REQ-CONTINUE-92) is continued one level down:
//! `prepare_wait` verifies the waiting element's nested run under the root's lock and then its
//! own, admits its next attempt from the nested step that waited and, for a repair, prepares
//! that step's stage as `prepare` does with the nested run as the parent.
//!
//! An abandoned root's nested run (REQ-CONTINUE-91) is settled by its record:
//! `prepare_abandoned` carries the wait of one that reads `waiting` and admits the in-place
//! continuation of one that reads `running`, as `um workflow run resume` continues an
//! abandoned root.

use std::path::PathBuf;

use um_execution::{
    AdmittedWorkflow, AttemptControls, CancellationSource, ChildRunRequest,
    ContinuationDefinitionMode, ContinuationEdgeV1, ContinuationReasonV1, ControlContext,
    FailedAttemptRestart, LocalAttemptOwner, LocalContinuationOpen, LocalRetryRejection,
    LocalRunDirectoryError, PendingLocalContinuation, ResolutionFailure, ResolvedWorkflow,
    RunState, TreeCopyMismatch, ValidatedStep, ValidatedTree, WaitChild, WaitKind, WaitRecord,
    WorkflowRunPresentationResult, acquire_local_continuation, attempt_controls,
    completed_inputs_match, latest_attempt_failed, latest_child_run, resolve_workflow_file,
    resolve_workflow_file_with_agent_profiles, wait_child,
};

use super::create::{BlockingOperationError, ChildRecovery, ExecutionLeaf, blocking_operation};
use super::resume::{
    ContinuationCandidate, Unadmitted, admit_continuation_in, continuation_output,
};

/// The code every ownership or selection refusal of a repair continuation reports: automatic
/// selection found no child it may continue, so the operator names the steps (REQ-CONTINUE-27).
const REFUSAL_CODE: &str = "continuation_definition_invalid";
/// The code of a child whose mapped inputs are not its retained ones (REQ-CONTINUE-9).
const INPUTS_UNSATISFIED: &str = "continuation_inputs_unsatisfied";
const UNOWNED: &str = "The repair wait names no failed child this run holds for the waiting step and element; continue the run with --from STEP.";
const CHANGED: &str =
    "The failed child changed while the command inspected it; run the command again.";
const FINALIZER_ONLY: &str = "The failed child finished every ordinary step and only a finalizer failed; continue the child or the run with --from STEP.";
const INPUTS_CHANGED: &str = "The waiting step would give the failed child other inputs than its run retained; restore them or continue the run with --from STEP.";
const ROOT_CHANGED: &str = "The execution root no longer resolves to the directory the failed child ran in; name the root to use with --execution-root to run the stage afresh there.";
const NESTED_UNOWNED: &str = "The wait names no waiting nested run this run holds for the waiting step and element; continue the run with --from STEP.";
const NESTED_ROOT_CHANGED: &str = "The execution root no longer resolves to the directory the nested run ran in; name the root to use with --execution-root to run it afresh there.";
const NESTED_ABANDONED: &str = "The abandoned nested run left no single running step to continue from; continue the run with --from STEP.";
const NESTED_CHANGED: &str =
    "The abandoned nested run changed while the command inspected it; run the command again.";

/// A failed child's continuation, verified and admitted before its parent's wait was
/// consumed. It holds the child's run lock until `begin` starts the attempt or it is dropped.
pub(super) struct ChildContinuation {
    pending: PendingLocalContinuation,
    workflow: ResolvedWorkflow,
    admitted: AdmittedWorkflow,
    from: Vec<String>,
    mode: ContinuationDefinitionMode,
    /// `retry` for a failed stage (REQ-CONTINUE-81); for a nested run, `retry` or `resume` as
    /// the root's continuation (REQ-CONTINUE-92), or for an abandoned one `resume`, or `edge`
    /// from the edge its latest attempt took (REQ-CONTINUE-91).
    reason: ContinuationReasonV1,
    /// The edge an abandoned nested run's latest attempt ended `continued` on, uncounted
    /// (REQ-CONTINUE-78); none otherwise.
    edge: Option<ContinuationEdgeV1>,
}

impl ChildContinuation {
    pub(super) fn reason(&self) -> ContinuationReasonV1 {
        self.reason
    }
}

/// A parent's continuation of a repair wait by `control.retry` at `target`, admitted in
/// `admitted`'s execution root, which `explicit_root` says the command named.
pub(super) struct Repair<'a> {
    pub(super) parent: &'a PendingLocalContinuation,
    pub(super) admitted: &'a AdmittedWorkflow,
    pub(super) target: &'a str,
    pub(super) explicit_root: bool,
}

/// The root a nested parent belongs to and the nested run's path from it, whose controls are
/// the nested run's (REQ-WFEXEC-75).
struct NestedRoot {
    directory: PathBuf,
    prefix: Vec<serde_json::Value>,
}

/// Why a repair continuation was not prepared: its rendered refusal, an operational failure,
/// or a retained-record failure the caller diagnoses after settling the parent.
pub(super) enum Unprepared {
    Rejected(WorkflowRunPresentationResult),
    Failed(&'static str),
    Diagnosed(LocalRunDirectoryError),
}

/// What the parent's own records require of the child the wait names.
struct Expected {
    directory: PathBuf,
    parent_directory: PathBuf,
    /// For a nested parent, the root whose controls it reads.
    root: Option<NestedRoot>,
    next_attempt: u64,
    parent_run: String,
    step: String,
    item: Option<u64>,
    workflow: String,
    tree: String,
    tree_digest: Option<String>,
    definition: Option<String>,
    revision: Option<u64>,
    execution_root: PathBuf,
}

/// The child's continuation as the blocking preflight leaves it: a fresh stage, or the
/// locked child with its restart steps, verified tree and definition.
enum Opened {
    Fresh,
    Child(Box<Verified>),
}

struct Verified {
    pending: PendingLocalContinuation,
    controls: ControlContext,
    tree: ValidatedTree,
    from: Vec<String>,
    workflow: ResolvedWorkflow,
    mode: ContinuationDefinitionMode,
}

enum Refusal {
    Unowned(&'static str),
    Locked(LocalRetryRejection),
    TreeChanged {
        directory: PathBuf,
        workflow_path: String,
        mismatch: TreeCopyMismatch,
    },
    Unresolved {
        directory: PathBuf,
        attempt_number: u64,
        failure: ResolutionFailure,
    },
    Replacement,
    Directory(LocalRunDirectoryError),
}

/// The repair continuation of the child the parent's wait names (REQ-CONTINUE-81), or `None`
/// for a fresh stage: a successor's imported wait, another wait kind or step, a wait without a
/// child, an explicitly different execution root, or a child whose latest attempt did not
/// fail during execution, such as one that exited with a declared outcome. Every mismatch of
/// the wait's locator, the child's record, tree, history, retained evidence or execution root
/// is refused (REQ-CONTINUE-82), never answered with a fresh child.
async fn prepare(
    repair: Repair<'_>,
    cancellation: &CancellationSource,
    output: (&um_execution::PresentationConfig, ExecutionLeaf),
) -> Result<Option<ChildRecovery>, Unprepared> {
    prepare_stage(repair, None, cancellation, output).await
}

/// `prepare` for a parent that is the run itself or, with `root`, a nested run whose controls
/// are its root's.
async fn prepare_stage(
    repair: Repair<'_>,
    root: Option<NestedRoot>,
    cancellation: &CancellationSource,
    output: (&um_execution::PresentationConfig, ExecutionLeaf),
) -> Result<Option<ChildRecovery>, Unprepared> {
    let Repair {
        parent,
        admitted,
        target,
        explicit_root,
    } = repair;
    let refuse = |message: &'static str| {
        Unprepared::Rejected(
            continuation_output(output.0.clone(), output.1, parent.run_directory())
                .render_continuation_refusal(
                    parent.run_directory(),
                    parent.prior_attempt_number(),
                    REFUSAL_CODE,
                    message,
                ),
        )
    };
    // A successor's carried wait keeps its fresh-stage path (REQ-CONTINUE-80).
    if parent.imported_prior().is_some() {
        return Ok(None);
    }
    let wait = match parent.wait() {
        Ok(Some(wait)) => wait,
        Ok(None) => return Ok(None),
        Err(error) => return Err(Unprepared::Diagnosed(error)),
    };
    if wait.kind != WaitKind::Repair
        || wait.step.first().and_then(serde_json::Value::as_str) != Some(target)
    {
        return Ok(None);
    }
    let (locator, item) = match wait_child(&wait) {
        WaitChild::Absent => return Ok(None),
        WaitChild::Unowned | WaitChild::Carried { .. } => return Err(refuse(UNOWNED)),
        WaitChild::Owned { locator, item } => (locator, item),
    };
    // Automatic continuation stays in the root the failed stage ran in; a root named on the
    // command that resolves elsewhere starts the stage afresh there.
    let execution_root = admitted.execution().root().to_owned();
    if execution_root != parent.prior_execution_root() {
        return if explicit_root {
            Ok(None)
        } else {
            Err(refuse(ROOT_CHANGED))
        };
    }
    let Some(ValidatedStep::Workflow(step)) = admitted.workflow().definition.steps.get(target)
    else {
        return Ok(None);
    };
    let record = match parent.record() {
        Ok(Some(record)) => record,
        Ok(None) => return Err(refuse(UNOWNED)),
        Err(error) => return Err(Unprepared::Diagnosed(error)),
    };
    let expected = Expected {
        directory: parent.run_directory().join(locator),
        parent_directory: parent.run_directory().to_owned(),
        root,
        next_attempt: parent.prior_attempt_number().saturating_add(1),
        parent_run: record.id,
        step: target.to_owned(),
        item,
        workflow: step.workflow.clone(),
        tree: record.tree,
        tree_digest: parent.tree_copy().map(|(_, tree)| tree.digest().to_owned()),
        definition: record.definition,
        revision: record.revision,
        execution_root: execution_root.clone(),
    };
    let verified = match blocking_operation(move || open_failed_child(&expected)).await {
        Ok(Opened::Fresh) => return Ok(None),
        Ok(Opened::Child(verified)) => *verified,
        Err(BlockingOperationError::Operation(refusal)) => {
            return Err(rendered_refusal(*refusal, &refuse, output));
        }
        Err(BlockingOperationError::WorkerUnavailable) => {
            return Err(Unprepared::Failed("verify the failed child run"));
        }
    };
    let Verified {
        pending,
        mut controls,
        tree,
        from,
        workflow,
        mode,
    } = verified;
    // REQ-CONTINUE-82: the inputs the step will map for the child, from the run's admitted
    // inputs, the outputs it inherits and the resolving control, must be the child's
    // retained ones before the wait is consumed.
    let mapped = Mapped {
        parent,
        admitted,
        target,
        item,
        workflow: &step.workflow,
    };
    check_mapped_inputs(mapped, &pending, &mut controls, output, UNOWNED)?;
    // The partition, retained inputs and inherited outputs are admitted as for any
    // continuation, launched from the parent's admitted environment (REQ-WFEXEC-59).
    let candidate = ContinuationCandidate {
        inputs: pending.candidate_inputs(&workflow),
        workflow: &workflow,
        from: &from,
        execution_root,
        tree: Some(tree),
    };
    let environment = admitted.execution().environment().clone();
    let (child_admitted, _) =
        match admit_continuation_in(&pending, candidate, Some(environment), cancellation, output)
            .await
        {
            Ok(admitted) => admitted,
            Err(Unadmitted::Rejected(result)) => return Err(Unprepared::Rejected(result)),
            Err(Unadmitted::Failed(message)) => return Err(Unprepared::Failed(message)),
        };
    if let Err(error) = pending.validate_execution_root(&child_admitted) {
        return Err(Unprepared::Diagnosed(error));
    }
    Ok(Some(ChildRecovery::Continue {
        step: target.to_owned(),
        item,
        continuation: Box::new(ChildContinuation {
            pending,
            workflow,
            admitted: child_admitted,
            from,
            mode,
            reason: ContinuationReasonV1::Retry,
            edge: None,
        }),
    }))
}

/// The child recovery of a continuation that a resolved wait starts at `target` with `reason`
/// (REQ-CONTINUE-75): for a wait the root carries from a nested run (REQ-WFEXEC-75), the
/// continuation of that nested run in place (REQ-CONTINUE-92); for a `control.retry` of the
/// run's own repair wait, the failed child's (`prepare`); otherwise none.
pub(super) async fn prepare_wait(
    repair: Repair<'_>,
    reason: ContinuationReasonV1,
    cancellation: &CancellationSource,
    output: (&um_execution::PresentationConfig, ExecutionLeaf),
) -> Result<Option<ChildRecovery>, Unprepared> {
    let wait = match repair.parent.wait() {
        Ok(wait) => wait,
        Err(error) => return Err(Unprepared::Diagnosed(error)),
    };
    match wait {
        Some(wait) if wait.is_carried() => {
            prepare_nested(repair, wait, reason, cancellation, output).await
        }
        _ if reason == ContinuationReasonV1::Retry => prepare(repair, cancellation, output).await,
        _ => Ok(None),
    }
}

/// The nested run a verified carried wait continues: its run, locked, and the nested wait it
/// waits on.
struct VerifiedNested {
    pending: PendingLocalContinuation,
    wait: WaitRecord,
    tree: ValidatedTree,
}

/// The continuation of the nested run that entered the root's carried `wait` (REQ-CONTINUE-92),
/// or `None` for a fresh nested run: a successor's imported wait (REQ-CONTINUE-80), a target
/// that is not the waiting step's `workflow` step, or an explicitly different execution root.
/// Before the root's wait is consumed, under the root's lock and then the nested run's, the
/// wait's `payload.child` must lie under the waiting element's nested run, which must name the
/// root, the step and the element, keep its tree copy and wait on the root wait's kind and
/// `since` at the nested step; anything else is refused as REQ-CONTINUE-82 refuses a failed
/// child. The nested run's next attempt is admitted from the nested step that waited with
/// `reason`; for a repair, that step's stage is adopted, continued or started afresh as the
/// run's own would be, with the nested run as its parent and the root's resolving control.
async fn prepare_nested(
    repair: Repair<'_>,
    wait: WaitRecord,
    reason: ContinuationReasonV1,
    cancellation: &CancellationSource,
    output: (&um_execution::PresentationConfig, ExecutionLeaf),
) -> Result<Option<ChildRecovery>, Unprepared> {
    let Repair {
        parent,
        admitted,
        target,
        explicit_root,
    } = repair;
    let refuse = |message: &'static str| {
        Unprepared::Rejected(
            continuation_output(output.0.clone(), output.1, parent.run_directory())
                .render_continuation_refusal(
                    parent.run_directory(),
                    parent.prior_attempt_number(),
                    REFUSAL_CODE,
                    message,
                ),
        )
    };
    if parent.imported_prior().is_some()
        || wait.step.first().and_then(serde_json::Value::as_str) != Some(target)
    {
        return Ok(None);
    }
    let Some(ValidatedStep::Workflow(step)) = admitted.workflow().definition.steps.get(target)
    else {
        return Ok(None);
    };
    let execution_root = admitted.execution().root().to_owned();
    if execution_root != parent.prior_execution_root() {
        return if explicit_root {
            Ok(None)
        } else {
            Err(refuse(NESTED_ROOT_CHANGED))
        };
    }
    let (directory, item, nested_step, child) = match wait_child(&wait) {
        WaitChild::Carried {
            locator,
            item,
            step,
            child,
        } => (
            parent.run_directory().join(locator),
            item,
            step,
            Some(child),
        ),
        // A wait without a child, a `stop` entered before the nested step started, names its
        // nested run by the element it waits at (REQ-WFEXEC-76).
        WaitChild::Absent => match parent.nested_run_at_element(&wait) {
            Ok(Some((directory, item))) => {
                let Some(nested) = wait.step.last().and_then(serde_json::Value::as_str) else {
                    return Err(refuse(NESTED_UNOWNED));
                };
                (directory, item, nested.to_owned(), None)
            }
            Ok(None) => return Err(refuse(NESTED_UNOWNED)),
            Err(error) => return Err(Unprepared::Diagnosed(error)),
        },
        WaitChild::Owned { .. } | WaitChild::Unowned => return Err(refuse(NESTED_UNOWNED)),
    };
    let record = match parent.record() {
        Ok(Some(record)) => record,
        Ok(None) => return Err(refuse(NESTED_UNOWNED)),
        Err(error) => return Err(Unprepared::Diagnosed(error)),
    };
    let control = match parent.resolving_control(&wait) {
        Ok(control) => control,
        Err(error) => return Err(Unprepared::Diagnosed(error)),
    };
    let mut prefix = vec![serde_json::Value::String(target.to_owned())];
    prefix.extend(item.map(|item| serde_json::json!({ "item": item })));
    let expected = Expected {
        directory,
        parent_directory: parent.run_directory().to_owned(),
        root: None,
        next_attempt: parent.prior_attempt_number().saturating_add(1),
        parent_run: record.id,
        step: target.to_owned(),
        item,
        workflow: step.workflow.clone(),
        tree: record.tree,
        tree_digest: parent.tree_copy().map(|(_, tree)| tree.digest().to_owned()),
        definition: record.definition,
        revision: record.revision,
        execution_root: execution_root.clone(),
    };
    let carried = (wait.clone(), nested_step, child);
    let opened = blocking_operation(move || {
        let verified = open_waiting_nested(&expected, &carried)?;
        let controls = parent_controls(&expected).map_err(Refusal::Directory)?;
        Ok::<_, Box<Refusal>>((verified, controls, expected))
    })
    .await;
    let (verified, controls, expected) = match opened {
        Ok(opened) => opened,
        Err(BlockingOperationError::Operation(refusal)) => {
            return Err(rendered_refusal(*refusal, &refuse, output));
        }
        Err(BlockingOperationError::WorkerUnavailable) => {
            return Err(Unprepared::Failed("verify the waiting nested run"));
        }
    };
    let VerifiedNested {
        pending,
        wait: nested_wait,
        tree,
    } = *verified;
    let mut controls = controls.controls;
    let mapped = Mapped {
        parent,
        admitted,
        target,
        item,
        workflow: &step.workflow,
    };
    check_mapped_inputs(mapped, &pending, &mut controls, output, NESTED_UNOWNED)?;
    // The nested run's next attempt starts at the nested step that waited, under its own
    // definition and retained inputs, launched from the root's admitted environment.
    let workflow = pending.previous_definition().clone();
    let from = vec![nested_wait.then.clone()];
    let candidate = ContinuationCandidate {
        inputs: pending.candidate_inputs(&workflow),
        workflow: &workflow,
        from: &from,
        execution_root,
        tree: Some(tree),
    };
    let environment = admitted.execution().environment().clone();
    let (nested_admitted, _) =
        match admit_continuation_in(&pending, candidate, Some(environment), cancellation, output)
            .await
        {
            Ok(admitted) => admitted,
            Err(Unadmitted::Rejected(result)) => return Err(Unprepared::Rejected(result)),
            Err(Unadmitted::Failed(message)) => return Err(Unprepared::Failed(message)),
        };
    if let Err(error) = pending.validate_execution_root(&nested_admitted) {
        return Err(Unprepared::Diagnosed(error));
    }
    // REQ-CONTINUE-92: a repair retry treats the nested step's stage as the run's own repair
    // would, with the nested run as the parent and the control recorded on the root.
    let stage = if nested_wait.kind == WaitKind::Repair && reason == ContinuationReasonV1::Retry {
        let adopted =
            match super::resume::adopted_repair(&pending, &nested_wait.then, &nested_wait, control)
            {
                Ok(adopted) => adopted,
                Err(_) => return Err(Unprepared::Failed("inspect the repaired stage")),
            };
        match adopted {
            Some(adopted) => Some(adopted),
            None => {
                let stage = Repair {
                    parent: &pending,
                    admitted: &nested_admitted,
                    target: &nested_wait.then,
                    explicit_root,
                };
                let root = NestedRoot {
                    directory: expected.parent_directory.clone(),
                    prefix,
                };
                prepare_stage(stage, Some(root), cancellation, output).await?
            }
        }
    } else {
        None
    };
    Ok(Some(ChildRecovery::Nested {
        step: target.to_owned(),
        item,
        continuation: Box::new(ChildContinuation {
            pending,
            workflow,
            admitted: nested_admitted,
            from,
            mode: ContinuationDefinitionMode::Inherited,
            reason,
            edge: None,
        }),
        stage: stage.map(Box::new),
    }))
}

/// The child run an abandoned root's step left `running` or `waiting`, which the abandonment
/// path chose to retry in place (REQ-CONTINUE-76), settled by its record when it is a nested
/// run (REQ-CONTINUE-91): one that reads `waiting` is carried as the wait it recorded, starting
/// nothing (REQ-WFEXEC-75), and one that reads `running` is continued in place as `um workflow
/// run resume` continues an abandoned root, from the edge its latest attempt ended `continued`
/// on, uncounted (REQ-CONTINUE-78), or from its single running `workflow` step, whose latest
/// stage is adopted, retried in place or run afresh. Its own definition and retained inputs are
/// admitted with the root's admitted environment and execution root. A stage keeps its retry,
/// and any other recovery is returned as it is.
pub(super) async fn prepare_abandoned(
    recovery: ChildRecovery,
    parent: &PendingLocalContinuation,
    admitted: &AdmittedWorkflow,
    cancellation: &CancellationSource,
    output: (&um_execution::PresentationConfig, ExecutionLeaf),
) -> Result<Option<ChildRecovery>, Unprepared> {
    let ChildRecovery::Retry {
        step,
        item,
        directory,
    } = recovery
    else {
        return Ok(Some(recovery));
    };
    let refuse = |message: &'static str| {
        Unprepared::Rejected(
            continuation_output(output.0.clone(), output.1, parent.run_directory())
                .render_continuation_refusal(
                    parent.run_directory(),
                    parent.prior_attempt_number(),
                    REFUSAL_CODE,
                    message,
                ),
        )
    };
    // The step's child file in the run's tree copy says whether the child is a nested run,
    // read before any lock so a stage's retry is the one it always was.
    let child_file = match (
        parent.tree_copy(),
        admitted.workflow().definition.steps.get(&step),
    ) {
        (Some((copy_root, _)), Some(ValidatedStep::Workflow(child))) => {
            Some((copy_root.clone(), copy_root.join(&child.workflow)))
        }
        _ => None,
    };
    let opened = directory.clone();
    let abandoned = match blocking_operation(move || {
        let nested = child_file.is_some_and(|(copy_root, file)| {
            resolve_workflow_file(&copy_root, &file)
                .is_ok_and(|child| super::create::has_workflow_steps(&child))
        });
        if nested {
            open_abandoned_nested(&opened)
        } else {
            Ok(AbandonedChild::Stage)
        }
    })
    .await
    {
        Ok(abandoned) => abandoned,
        Err(BlockingOperationError::Operation(refusal)) => {
            return Err(rendered_refusal(*refusal, &refuse, output));
        }
        Err(BlockingOperationError::WorkerUnavailable) => {
            return Err(Unprepared::Failed("read the abandoned step's nested run"));
        }
    };
    let continued = match abandoned {
        AbandonedChild::Stage => {
            return Ok(Some(ChildRecovery::Retry {
                step,
                item,
                directory,
            }));
        }
        AbandonedChild::Waiting(wait) => {
            return Ok(Some(ChildRecovery::Waiting {
                step,
                item,
                directory,
                wait,
            }));
        }
        AbandonedChild::Running(continued) => *continued,
    };
    let AbandonedNested {
        pending,
        tree,
        from,
        reason,
        edge,
        stage,
    } = continued;
    let workflow = pending.previous_definition().clone();
    let candidate = ContinuationCandidate {
        inputs: pending.candidate_inputs(&workflow),
        workflow: &workflow,
        from: &from,
        execution_root: admitted.execution().root().to_owned(),
        tree: Some(tree),
    };
    let environment = admitted.execution().environment().clone();
    let (nested_admitted, _) =
        match admit_continuation_in(&pending, candidate, Some(environment), cancellation, output)
            .await
        {
            Ok(admitted) => admitted,
            Err(Unadmitted::Rejected(result)) => return Err(Unprepared::Rejected(result)),
            Err(Unadmitted::Failed(message)) => return Err(Unprepared::Failed(message)),
        };
    if let Err(error) = pending.validate_execution_root(&nested_admitted) {
        return Err(Unprepared::Diagnosed(error));
    }
    Ok(Some(ChildRecovery::Nested {
        step,
        item,
        continuation: Box::new(ChildContinuation {
            pending,
            workflow,
            admitted: nested_admitted,
            from,
            mode: ContinuationDefinitionMode::Inherited,
            reason,
            edge,
        }),
        stage: stage.map(Box::new),
    }))
}

/// What an abandoned root's child run is: a stage, or a nested run read under its lock.
enum AbandonedChild {
    /// A stage, which keeps its retry (REQ-CONTINUE-76).
    Stage,
    /// A nested run that reads `waiting` on this record.
    Waiting(Box<WaitRecord>),
    /// A nested run that reads `running`, held for its continuation.
    Running(Box<AbandonedNested>),
}

/// An abandoned nested run's continuation before admission: the run, locked, its verified tree
/// and where its next attempt starts, with the recovery of the stage that step left.
struct AbandonedNested {
    pending: PendingLocalContinuation,
    tree: ValidatedTree,
    from: Vec<String>,
    reason: ContinuationReasonV1,
    edge: Option<ContinuationEdgeV1>,
    stage: Option<ChildRecovery>,
}

/// Acquires the nested run at `directory` and reads, under its lock, how it stands. One that
/// reads `waiting` without a wait record, or `exited` since the lock-free read, is refused, as
/// is a `running` one whose abandoned attempt left no single running step.
fn open_abandoned_nested(directory: &std::path::Path) -> Result<AbandonedChild, Box<Refusal>> {
    let pending = match acquire_local_continuation(directory) {
        Ok(LocalContinuationOpen::Acquired(pending)) => *pending,
        Ok(LocalContinuationOpen::Rejected(rejection)) => {
            return Err(Box::new(Refusal::Locked(rejection)));
        }
        Err(error) => return Err(Box::new(Refusal::Directory(error))),
    };
    let record = pending
        .record()
        .map_err(Refusal::Directory)?
        .ok_or(Refusal::Unowned(NESTED_CHANGED))?;
    match record.state {
        RunState::Waiting => {
            let wait = pending
                .wait()
                .map_err(Refusal::Directory)?
                .ok_or(Refusal::Unowned(NESTED_CHANGED))?;
            return Ok(AbandonedChild::Waiting(Box::new(wait)));
        }
        RunState::Running => {}
        RunState::Exited => return Err(Box::new(Refusal::Unowned(NESTED_CHANGED))),
    }
    let (from, reason, edge) = match pending.continued_edge() {
        Some(edge) => (
            vec![edge.target.clone()],
            ContinuationReasonV1::Edge,
            Some(ContinuationEdgeV1 {
                counted: false,
                ..edge.clone()
            }),
        ),
        None => match pending.abandoned_steps().as_slice() {
            [step] => (vec![step.clone()], ContinuationReasonV1::Resume, None),
            _ => return Err(Box::new(Refusal::Unowned(NESTED_ABANDONED))),
        },
    };
    // REQ-CONTINUE-76 one level down: the running step's latest stage, from the nested run's
    // own `position`, is adopted when it exited and retried in place when it did not; none
    // runs afresh.
    let stage = match (&edge, from.as_slice()) {
        (None, [step]) => match latest_child_run(directory, step).map_err(Refusal::Directory)? {
            Some((directory, RunState::Exited, item)) => Some(ChildRecovery::Adopt {
                step: step.clone(),
                item,
                directory,
            }),
            Some((directory, RunState::Running | RunState::Waiting, item)) => {
                Some(ChildRecovery::Retry {
                    step: step.clone(),
                    item,
                    directory,
                })
            }
            None => None,
        },
        _ => None,
    };
    let tree = match pending.verify_tree_copy() {
        Some(Ok(tree)) => tree,
        Some(Err(mismatch)) => {
            return Err(Box::new(Refusal::TreeChanged {
                directory: directory.to_owned(),
                workflow_path: pending.previous_definition().source.workflow_path.clone(),
                mismatch,
            }));
        }
        None => return Err(Box::new(Refusal::Unowned(NESTED_CHANGED))),
    };
    Ok(AbandonedChild::Running(Box::new(AbandonedNested {
        pending,
        tree,
        from,
        reason,
        edge,
        stage,
    })))
}

/// Acquires the nested run `expected` names and verifies, under its lock, that it is the
/// root's own nested run of that step and element, in the same root and pinned tree, and that
/// it waits on `wait`'s kind and `since` at `nested` with the child the root's wait named.
fn open_waiting_nested(
    expected: &Expected,
    (wait, nested, child): &(WaitRecord, String, Option<PathBuf>),
) -> Result<Box<VerifiedNested>, Box<Refusal>> {
    let pending = match acquire_local_continuation(&expected.directory) {
        Ok(LocalContinuationOpen::Acquired(pending)) => *pending,
        Ok(LocalContinuationOpen::Rejected(rejection)) => {
            return Err(Box::new(Refusal::Locked(rejection)));
        }
        Err(error) => return Err(Box::new(Refusal::Directory(error))),
    };
    let record = owned_child(&pending, expected, NESTED_UNOWNED)?;
    let nested_wait = pending
        .wait()
        .map_err(Refusal::Directory)?
        .filter(|nested_wait| {
            record.state == RunState::Waiting
                && nested_wait.kind == wait.kind
                && nested_wait.since == wait.since
                && nested_wait.step == [serde_json::Value::String(nested.clone())]
                && child.as_ref().is_none_or(|child| {
                    wait_child(nested_wait)
                        == WaitChild::Owned {
                            locator: child.clone(),
                            item: None,
                        }
                })
        })
        .ok_or(Refusal::Unowned(NESTED_UNOWNED))?;
    if pending.prior_execution_root() != expected.execution_root {
        return Err(Box::new(Refusal::Unowned(NESTED_ROOT_CHANGED)));
    }
    let tree = match pending.verify_tree_copy() {
        Some(Ok(tree)) => tree,
        Some(Err(mismatch)) => {
            return Err(Box::new(Refusal::TreeChanged {
                directory: expected.directory.clone(),
                workflow_path: pending.previous_definition().source.workflow_path.clone(),
                mismatch,
            }));
        }
        None => return Err(Box::new(Refusal::Unowned(NESTED_UNOWNED))),
    };
    Ok(Box::new(VerifiedNested {
        pending,
        wait: nested_wait,
        tree,
    }))
}

/// A refusal the blocking preflight returned, rendered for the command: an ownership refusal
/// through `refuse`, a lock, tree or resolution refusal for the run it names.
fn rendered_refusal(
    refusal: Refusal,
    refuse: &dyn Fn(&'static str) -> Unprepared,
    output: (&um_execution::PresentationConfig, ExecutionLeaf),
) -> Unprepared {
    match refusal {
        Refusal::Unowned(message) => refuse(message),
        Refusal::Locked(rejection) => Unprepared::Rejected(
            continuation_output(output.0.clone(), output.1, rejection.run_directory())
                .render_retry_rejection(&rejection),
        ),
        Refusal::TreeChanged {
            directory,
            workflow_path,
            mismatch,
        } => Unprepared::Rejected(
            continuation_output(output.0.clone(), output.1, &directory)
                .render_tree_copy_rejection(&workflow_path, &mismatch),
        ),
        Refusal::Unresolved {
            directory,
            attempt_number,
            failure,
        } => Unprepared::Rejected(
            continuation_output(output.0.clone(), output.1, &directory)
                .render_continuation_resolution_rejection(&directory, attempt_number, &failure),
        ),
        Refusal::Replacement => {
            Unprepared::Failed("prepare the control's agent-profile replacement")
        }
        Refusal::Directory(error) => Unprepared::Diagnosed(error),
    }
}

/// The child a parent's step maps a request for: the parent, its admitted workflow, the step
/// `target` at element `item` and the child workflow it names.
struct Mapped<'a> {
    parent: &'a PendingLocalContinuation,
    admitted: &'a AdmittedWorkflow,
    target: &'a str,
    item: Option<u64>,
    workflow: &'a str,
}

/// REQ-CONTINUE-82: refuses a continuation of the child `pending` holds unless the request
/// the step would map for it under `controls`, at its element, is its own workflow with the
/// inputs its run retained; another workflow is refused with `unowned`.
fn check_mapped_inputs(
    mapped: Mapped<'_>,
    pending: &PendingLocalContinuation,
    controls: &mut ControlContext,
    output: (&um_execution::PresentationConfig, ExecutionLeaf),
    unowned: &'static str,
) -> Result<(), Unprepared> {
    let Mapped {
        parent,
        admitted,
        target,
        item,
        workflow,
    } = mapped;
    if let Some(item) = item {
        controls.resume_item = Some((target.to_owned(), item));
    }
    let inputs = match parent.child_request_inputs(admitted, target, item, controls) {
        Ok(inputs) => inputs,
        Err(error) => return Err(Unprepared::Diagnosed(error)),
    };
    let checked = match inputs {
        Some(inputs) => retained_request(
            pending,
            &ChildRunRequest {
                step: target.to_owned(),
                workflow: workflow.to_owned(),
                inputs,
                item,
            },
        ),
        None => Err(INPUTS_UNSATISFIED.to_owned()),
    };
    let Err(code) = checked else {
        return Ok(());
    };
    let message = if code == INPUTS_UNSATISFIED {
        INPUTS_CHANGED
    } else {
        unowned
    };
    let code = if code == INPUTS_UNSATISFIED {
        INPUTS_UNSATISFIED
    } else {
        REFUSAL_CODE
    };
    Err(Unprepared::Rejected(
        continuation_output(output.0.clone(), output.1, parent.run_directory())
            .render_continuation_refusal(
                parent.run_directory(),
                parent.prior_attempt_number(),
                code,
                message,
            ),
    ))
}

/// Reads the child `expected` names without its lock to choose between a fresh stage and a
/// continuation, then acquires it and verifies, under the lock, that it is the parent's own
/// failed child of that step and element, in the same root and pinned tree.
fn open_failed_child(expected: &Expected) -> Result<Opened, Box<Refusal>> {
    match latest_attempt_failed(&expected.directory) {
        Ok(true) => {}
        Ok(false) => return Ok(Opened::Fresh),
        Err(_) => return Err(Box::new(Refusal::Unowned(UNOWNED))),
    }
    let pending = match acquire_local_continuation(&expected.directory) {
        Ok(LocalContinuationOpen::Acquired(pending)) => *pending,
        Ok(LocalContinuationOpen::Rejected(rejection)) => {
            return Err(Box::new(Refusal::Locked(rejection)));
        }
        Err(error) => return Err(Box::new(Refusal::Directory(error))),
    };
    owned_child(&pending, expected, UNOWNED)?;
    let from = match pending.failed_attempt_restart() {
        FailedAttemptRestart::Steps(from) => from,
        FailedAttemptRestart::NotFailed => return Err(Box::new(Refusal::Unowned(CHANGED))),
        FailedAttemptRestart::FinalizerOnly => {
            return Err(Box::new(Refusal::Unowned(FINALIZER_ONLY)));
        }
    };
    if pending.prior_execution_root() != expected.execution_root {
        return Err(Box::new(Refusal::Unowned(ROOT_CHANGED)));
    }
    let tree = match pending.verify_tree_copy() {
        Some(Ok(tree)) => tree,
        Some(Err(mismatch)) => {
            return Err(Box::new(Refusal::TreeChanged {
                directory: expected.directory.clone(),
                workflow_path: pending.previous_definition().source.workflow_path.clone(),
                mismatch,
            }));
        }
        None => return Err(Box::new(Refusal::Unowned(UNOWNED))),
    };
    let controls = parent_controls(expected).map_err(Refusal::Directory)?;
    let (workflow, mode) = child_definition(&pending, expected, controls.replacement)?;
    Ok(Opened::Child(Box::new(Verified {
        pending,
        controls: controls.controls,
        tree,
        from,
        workflow,
        mode,
    })))
}

/// Refuses with `unowned` unless the run `pending` holds is the child `expected` names: in
/// that directory, naming the parent run, step and element, with the parent's child workflow,
/// pinned tree, definition and revision and the parent's tree copy (REQ-CONTINUE-82). It
/// returns the child's record.
fn owned_child(
    pending: &PendingLocalContinuation,
    expected: &Expected,
    unowned: &'static str,
) -> Result<um_execution::WorkflowRunRecord, Box<Refusal>> {
    if pending.run_directory() != expected.directory {
        return Err(Box::new(Refusal::Unowned(unowned)));
    }
    let record = pending
        .record()
        .map_err(Refusal::Directory)?
        .ok_or(Refusal::Unowned(unowned))?;
    let owned = record.parent.as_ref().is_some_and(|parent| {
        parent.run == expected.parent_run
            && parent.step == expected.step
            && parent.item == expected.item
    }) && record.workflow == expected.workflow
        && record.tree == expected.tree
        && record.definition == expected.definition
        && record.revision == expected.revision
        && pending.tree_copy().map(|(_, tree)| tree.digest()) == expected.tree_digest.as_deref();
    if owned {
        Ok(record)
    } else {
        Err(Box::new(Refusal::Unowned(unowned)))
    }
}

/// The controls the parent's next attempt starts with: its own or, for a nested parent, its
/// root's (REQ-WFEXEC-75).
fn parent_controls(expected: &Expected) -> Result<AttemptControls, LocalRunDirectoryError> {
    match &expected.root {
        None => attempt_controls(&expected.parent_directory, expected.next_attempt),
        Some(root) => AttemptControls::nested(
            &root.directory,
            &expected.parent_directory,
            expected.next_attempt,
            &root.prefix,
        ),
    }
}

/// The definition the child's continuation runs under: its prior attempt's, or that file
/// resolved again with the agent-profile replacement a `control.retry` names for the step
/// (REQ-AGENT-59), which a changed definition retains as the attempt's own.
fn child_definition(
    pending: &PendingLocalContinuation,
    expected: &Expected,
    replacement: Option<(String, serde_json::Value)>,
) -> Result<(ResolvedWorkflow, ContinuationDefinitionMode), Box<Refusal>> {
    let previous = pending.previous_definition();
    let replacement = replacement.filter(|(step, _)| *step == expected.step);
    let Some((_, profiles)) = replacement else {
        return Ok((previous.clone(), ContinuationDefinitionMode::Inherited));
    };
    let document =
        super::create::control_replacement_document(profiles).map_err(|_| Refusal::Replacement)?;
    let root = &previous.source.source_root;
    let resolved = resolve_workflow_file_with_agent_profiles(
        root,
        &root.join(&previous.source.workflow_path),
        Some(&document),
    )
    .map_err(|failure| Refusal::Unresolved {
        directory: expected.directory.clone(),
        attempt_number: pending.prior_attempt_number(),
        failure,
    })?;
    let replacement_digest = |workflow: &ResolvedWorkflow| {
        workflow
            .agent_profile_replacement
            .as_ref()
            .map(|replacement| replacement.digest.clone())
    };
    if resolved.content_digest == previous.content_digest
        && replacement_digest(&resolved) == replacement_digest(previous)
    {
        Ok((previous.clone(), ContinuationDefinitionMode::Inherited))
    } else {
        Ok((resolved, ContinuationDefinitionMode::Replaced))
    }
}

/// Whether `request` is for the child `pending` holds: its own workflow, and mapped inputs
/// that, completed as admission completes omitted optional inputs, are its retained inputs
/// (REQ-CONTINUE-82). A mismatch is the child's rejection code.
fn retained_request(
    pending: &PendingLocalContinuation,
    request: &ChildRunRequest,
) -> Result<(), String> {
    let recorded = pending
        .record()
        .ok()
        .flatten()
        .map(|record| record.workflow);
    if recorded.as_deref() != Some(request.workflow.as_str()) {
        return Err(REFUSAL_CODE.to_owned());
    }
    let supplied = super::create::child_request_inputs(request)?;
    let (initial, retained) = pending.root_specification();
    if completed_inputs_match(initial, supplied, retained) {
        Ok(())
    } else {
        Err(INPUTS_UNSATISFIED.to_owned())
    }
}

/// Begins the next attempt of the child `continuation` holds for the step's dispatched
/// `request`, with its reason, `retry` for a failed stage and `retry` or `resume` for a nested
/// run (REQ-CONTINUE-92), and its continuation context bound (INV-ENGINE-27). The
/// request must be for the child's own workflow with the mapped inputs its run retained
/// (REQ-CONTINUE-82); otherwise nothing is written and the refusal is the child's rejection
/// code. The attempt observes the step's `cancellation`.
pub(super) fn begin(
    continuation: ChildContinuation,
    request: &ChildRunRequest,
    cancellation: CancellationSource,
) -> Result<(ResolvedWorkflow, AdmittedWorkflow, LocalAttemptOwner), String> {
    let ChildContinuation {
        pending,
        workflow,
        admitted,
        from,
        mode,
        reason,
        edge,
    } = continuation;
    // The preflight checked the same request; under the lock held since, it is checked again.
    retained_request(&pending, request)?;
    let admitted = admitted.with_cancellation_source(cancellation);
    let owned = pending
        .begin(&admitted, from, mode, reason, edge)
        .map_err(|_| "child_continuation_unavailable".to_owned())?;
    let admitted = owned
        .bind_continuation_context(admitted)
        .map_err(|_| "child_continuation_unavailable".to_owned())?;
    Ok((workflow, admitted, owned))
}
