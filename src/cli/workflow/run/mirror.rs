//! The run mirror (REQ-LOCALRUN-191, INV-ENGINE-25): which runs write to Linear and through
//! what. A definition run of a root workflow that carries `mirror` gets a `Mirror` over its
//! issue input, its revision's bindings snapshot, its owner and its run directory; the hook
//! sites in `create.rs`, `end.rs` and `resume.rs` drive it against the Linear target, and a
//! write's result never changes the command's. A command that ends a run itself checks the
//! key first (`outcome_credentials`, REQ-LOCALRUN-204).

mod linear;

use std::path::Path;
use std::sync::Arc;

use um_execution::{
    LocalRunDirectoryError, Mirror, MirrorFailure, MirrorTarget, ResolvedInput, ResolvedInputs,
    ValidatedWorkflow, WorkflowHome, WorkflowRunRecord, read_revision_bindings,
};

use super::create::blocking_operation;

/// A hook performed against the target: one of `Mirror`'s methods with its arguments.
pub(super) type Hook =
    dyn FnOnce(&Mirror, &dyn MirrorTarget) -> Result<(), LocalRunDirectoryError> + Send;

/// The run's mirror, or `None` when it writes nothing: its root has no `mirror` block, it was
/// started by path (no definition or revision), or it is a child run. A missing or non-text
/// issue input, or a snapshot that cannot be read, still makes a `Mirror`, whose every hook
/// records the failure.
pub(super) fn mirror_for_run(
    home: Option<&WorkflowHome>,
    record: &WorkflowRunRecord,
    workflow: &ValidatedWorkflow,
    inputs: &ResolvedInputs,
    run_directory: &Path,
) -> Option<Mirror> {
    let block = workflow.root.as_ref()?.mirror.as_ref()?;
    let (Some(definition), Some(revision), None) =
        (&record.definition, record.revision, &record.parent)
    else {
        return None;
    };
    let issue = match inputs.get(&block.issue) {
        Some(ResolvedInput::Text(issue)) => Ok(issue.to_string()),
        _ => Err(MirrorFailure::issue_unavailable(format!(
            "The run has no text input `{}` naming the issue.",
            block.issue
        ))),
    };
    let bindings = home
        .ok_or_else(|| MirrorFailure::bindings_unavailable("The workflow home cannot be located."))
        .and_then(|home| {
            read_revision_bindings(home, definition, revision).map_err(|error| {
                MirrorFailure::bindings_unavailable(format!(
                    "The bindings snapshot of {definition} revision {revision} cannot be read: {}.",
                    error.code()
                ))
            })
        })
        .map(|snapshot| {
            snapshot
                .bindings()
                .iter()
                .map(|(key, bound)| (key.clone(), bound.value.clone()))
                .collect()
        });
    let (issue, bindings) = match issue {
        Ok(issue) => (issue, bindings),
        Err(failure) => (String::new(), Err(failure)),
    };
    let run_url = url::Url::from_file_path(run_directory).map_or_else(
        |()| format!("file://{}", run_directory.display()),
        String::from,
    );
    Some(Mirror::new(
        block.clone(),
        issue,
        bindings,
        record.owner.clone(),
        record.id.clone(),
        run_url,
        run_directory.to_owned(),
    ))
}

/// `mirror_for_run` with the home `WorkflowHome::from_environment` locates.
pub(super) fn run_mirror(
    record: &WorkflowRunRecord,
    workflow: &ValidatedWorkflow,
    inputs: &ResolvedInputs,
    run_directory: &Path,
) -> Option<Mirror> {
    let home = WorkflowHome::from_environment().ok();
    mirror_for_run(home.as_ref(), record, workflow, inputs, run_directory)
}

/// The run record `run.json` of the run at `run_directory`, for a command that holds the run
/// but not its record; `None` for a run without one or one that cannot be read.
pub(super) fn read_run_record(run_directory: &Path) -> Option<WorkflowRunRecord> {
    um_execution::read_local_run_status(run_directory)
        .ok()
        .and_then(|snapshot| snapshot.record)
        .and_then(|record| serde_json::from_value(record).ok())
}

/// The message a command refuses with when `outcome_credentials` fails (REQ-LOCALRUN-204).
pub(super) const CREDENTIALS_REFUSAL: &str = "LINEAR_API_KEY is unset or empty, so the run's \
    mirror cannot update its issue in Linear; the run is unchanged. Set LINEAR_API_KEY and \
    run the command again.";

/// The check a command makes before it ends a run itself with `outcome` and then performs
/// `onOutcome` (REQ-LOCALRUN-204): `mirror_credentials_unavailable` when the run's mirror maps
/// `outcome` and the environment's Linear target has no key, the failure that write would
/// record; nothing for a run without a mirror or an outcome the mirror does not map. It reads
/// the environment and sends nothing.
pub(super) fn outcome_credentials(
    mirror: Option<&Mirror>,
    outcome: &str,
) -> Result<(), MirrorFailure> {
    match mirror {
        Some(mirror) if mirror.maps_outcome(outcome) => {
            linear::LinearTarget::from_environment().credentials()
        }
        _ => Ok(()),
    }
}

/// Performs `hook` against Linear on this thread. A failed write is already a record; a record
/// that cannot be written is dropped too, since the mirror never changes the command's result
/// (INV-ENGINE-25).
pub(super) fn perform(mirror: &Mirror, hook: Box<Hook>) {
    let target = linear::LinearTarget::from_environment();
    let _ = hook(mirror, &target);
}

/// `perform` on a blocking thread, for the attempt's async phases; nothing without a mirror.
pub(super) async fn perform_blocking(mirror: Option<&Arc<Mirror>>, hook: Box<Hook>) {
    let Some(mirror) = mirror.cloned() else {
        return;
    };
    let _ = blocking_operation(move || {
        perform(&mirror, hook);
        Ok::<_, std::convert::Infallible>(())
    })
    .await;
}
