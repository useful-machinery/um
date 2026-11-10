//! The repair continuation of a parent's failed child (REQ-CONTINUE-81, REQ-CONTINUE-82,
//! INV-ENGINE-27): a `control.retry` that resolves a repair wait back to the waiting step
//! continues the child the wait names in its own run directory rather than starting the stage
//! afresh. `prepare` verifies and admits that continuation under the parent's and the child's
//! locks before the parent's wait is consumed; `begin` starts the child's next attempt once
//! the parent's dispatcher requests the step.

use std::path::PathBuf;

use um_execution::{
    AdmittedWorkflow, CancellationSource, ChildRunRequest, ContinuationDefinitionMode,
    ContinuationReasonV1, ControlContext, FailedAttemptRestart, LocalAttemptOwner,
    LocalContinuationOpen, LocalRetryRejection, LocalRunDirectoryError, PendingLocalContinuation,
    ResolutionFailure, ResolvedWorkflow, TreeCopyMismatch, ValidatedStep, ValidatedTree, WaitChild,
    WaitKind, WorkflowRunPresentationResult, acquire_local_continuation, attempt_controls,
    completed_inputs_match, latest_attempt_failed, resolve_workflow_file_with_agent_profiles,
    wait_child,
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

/// A failed child's continuation, verified and admitted before its parent's wait was
/// consumed. It holds the child's run lock until `begin` starts the attempt or it is dropped.
pub(super) struct ChildContinuation {
    pending: PendingLocalContinuation,
    workflow: ResolvedWorkflow,
    admitted: AdmittedWorkflow,
    from: Vec<String>,
    mode: ContinuationDefinitionMode,
}

/// A parent's continuation of a repair wait by `control.retry` at `target`, admitted in
/// `admitted`'s execution root, which `explicit_root` says the command named.
pub(super) struct Repair<'a> {
    pub(super) parent: &'a PendingLocalContinuation,
    pub(super) admitted: &'a AdmittedWorkflow,
    pub(super) target: &'a str,
    pub(super) explicit_root: bool,
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
pub(super) async fn prepare(
    repair: Repair<'_>,
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
        WaitChild::Unowned => return Err(refuse(UNOWNED)),
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
            return Err(match *refusal {
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
                        .render_continuation_resolution_rejection(
                            &directory,
                            attempt_number,
                            &failure,
                        ),
                ),
                Refusal::Replacement => {
                    Unprepared::Failed("prepare the control's agent-profile replacement")
                }
                Refusal::Directory(error) => Unprepared::Diagnosed(error),
            });
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
    if let Some(item) = item {
        controls.resume_item = Some((target.to_owned(), item));
    }
    let inputs = match parent.child_request_inputs(admitted, target, item, &controls) {
        Ok(inputs) => inputs,
        Err(error) => return Err(Unprepared::Diagnosed(error)),
    };
    let checked = match inputs {
        Some(inputs) => retained_request(
            &pending,
            &ChildRunRequest {
                step: target.to_owned(),
                workflow: step.workflow.clone(),
                inputs,
                item,
            },
        ),
        None => Err(INPUTS_UNSATISFIED.to_owned()),
    };
    if let Err(code) = checked {
        return Err(if code == INPUTS_UNSATISFIED {
            Unprepared::Rejected(
                continuation_output(output.0.clone(), output.1, parent.run_directory())
                    .render_continuation_refusal(
                        parent.run_directory(),
                        parent.prior_attempt_number(),
                        INPUTS_UNSATISFIED,
                        INPUTS_CHANGED,
                    ),
            )
        } else {
            refuse(UNOWNED)
        });
    }
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
        }),
    }))
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
    if pending.run_directory() != expected.directory {
        return Err(Box::new(Refusal::Unowned(UNOWNED)));
    }
    let record = pending
        .record()
        .map_err(Refusal::Directory)?
        .ok_or(Refusal::Unowned(UNOWNED))?;
    let owned = record.parent.as_ref().is_some_and(|parent| {
        parent.run == expected.parent_run
            && parent.step == expected.step
            && parent.item == expected.item
    }) && record.workflow == expected.workflow
        && record.tree == expected.tree
        && record.definition == expected.definition
        && record.revision == expected.revision
        && pending.tree_copy().map(|(_, tree)| tree.digest()) == expected.tree_digest.as_deref();
    if !owned {
        return Err(Box::new(Refusal::Unowned(UNOWNED)));
    }
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
    let controls = attempt_controls(&expected.parent_directory, expected.next_attempt)
        .map_err(Refusal::Directory)?;
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
/// `request`, with reason `retry` and its continuation context bound (INV-ENGINE-27). The
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
    } = continuation;
    // The preflight checked the same request; under the lock held since, it is checked again.
    retained_request(&pending, request)?;
    let admitted = admitted.with_cancellation_source(cancellation);
    let owned = pending
        .begin(&admitted, from, mode, ContinuationReasonV1::Retry, None)
        .map_err(|_| "child_continuation_unavailable".to_owned())?;
    let admitted = owned
        .bind_continuation_context(admitted)
        .map_err(|_| "child_continuation_unavailable".to_owned())?;
    Ok((workflow, admitted, owned))
}
