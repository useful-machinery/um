//! The run mirror (REQ-LOCALRUN-191, INV-ENGINE-25): which runs write to Linear and through
//! what. A definition run of a root workflow that carries `mirror` gets a `Mirror` over its
//! issue input, its revision's bindings snapshot and resolution record (REQ-WFDEF-32), its
//! owner and id, its `external.url` or run directory and its run directory; the hook
//! sites in `create.rs`, `end.rs` and `resume.rs` drive it against the Linear target, and a
//! write's result never changes the command's. A command that ends a run itself checks for a
//! Linear credential first (`outcome_credentials`, REQ-LOCALRUN-204).

pub(super) mod linear;

use std::path::Path;
use std::sync::Arc;

use zeroize::Zeroizing;

use um_execution::{
    DefinitionError, LocalRunDirectoryError, Mirror, MirrorFailure, MirrorTarget, ResolvedInput,
    ResolvedInputs, ValidatedWorkflow, WorkflowHome, WorkflowRunRecord, read_revision_bindings,
};

use super::create::blocking_operation;

/// A hook performed against the target: one of `Mirror`'s methods with its arguments.
pub(super) type Hook =
    dyn FnOnce(&Mirror, &dyn MirrorTarget) -> Result<(), LocalRunDirectoryError> + Send;

/// The run's mirror, or `None` when it writes nothing: its root has no `mirror` block, it was
/// started by path (no definition or revision), or it is a child run. A missing or non-text
/// issue input, or a snapshot or resolution record that cannot be read, still makes a
/// `Mirror`, whose every hook records the failure. A revision with a resolution record
/// makes a mirror that matches and writes states by id (REQ-LOCALRUN-192).
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
    let unreadable = |what: &str, error: DefinitionError| {
        MirrorFailure::bindings_unavailable(format!(
            "The {what} of {definition} revision {revision} cannot be read: {}.",
            error.code()
        ))
    };
    let read = home
        .ok_or_else(|| MirrorFailure::bindings_unavailable("The workflow home cannot be located."))
        .and_then(|home| {
            let snapshot = read_revision_bindings(home, definition, revision)
                .map_err(|error| unreadable("bindings snapshot", error))?;
            let resolved_ids = home
                .revision_resolution(definition, revision)
                .map_err(|error| unreadable("resolution record", error))?;
            Ok((snapshot, resolved_ids))
        });
    let (bindings, resolved_ids) = match (issue.as_ref(), read) {
        (Err(failure), _) => (Err(failure.clone()), None),
        (Ok(_), Err(failure)) => (Err(failure), None),
        (Ok(_), Ok((snapshot, resolved_ids))) => (
            Ok(snapshot
                .bindings()
                .iter()
                .filter_map(|(key, bound)| Some((key.clone(), bound.value.as_text()?.to_owned())))
                .collect()),
            resolved_ids,
        ),
    };
    // REQ-LOCALRUN-191: the record's `external.url`, which a Cloud runner sets to the run's
    // page, else the run directory.
    let run_url = record.external.get("url").cloned().unwrap_or_else(|| {
        url::Url::from_file_path(run_directory).map_or_else(
            |()| format!("file://{}", run_directory.display()),
            String::from,
        )
    });
    let mirror = Mirror::new(
        block.clone(),
        issue.unwrap_or_default(),
        bindings,
        record.owner.clone(),
        record.id.clone(),
        run_url,
        run_directory.to_owned(),
    );
    Some(match resolved_ids {
        Some(resolved_ids) => mirror.with_resolved_ids(resolved_ids),
        None => mirror,
    })
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
pub(super) const CREDENTIALS_REFUSAL: &str = "No Linear credential is set, so the run's mirror \
    cannot update its issue in Linear; the run is unchanged. Set UM_LINEAR_ACCESS_TOKEN_FILE, \
    UM_LINEAR_ACCESS_TOKEN or LINEAR_API_KEY and run the command again.";

/// The check a command makes before it ends a run itself with `outcome` and then performs
/// `onOutcome` (REQ-LOCALRUN-204): `mirror_credentials_unavailable` when the run's mirror maps
/// `outcome` and the environment's Linear target has no credential, the failure that write would
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

/// Performs `hook` against Linear on this thread, with the credential the environment holds now,
/// so each hook reads a token a runner replaced during the run (REQ-LOCALRUN-192). A failed
/// write is already a record; a record that cannot be written is dropped too, since the mirror
/// never changes the command's result (INV-ENGINE-25).
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

/// The token the environment offers through `file_variable` and then `variable`, read now: the
/// contents of the file `file_variable` names, else the value of `variable`, each trimmed and
/// taken only when it is not empty and holds no whitespace inside. A file that is missing or
/// unreadable, or a value that is not one token, is skipped for the next source and is no error
/// of its own; the caller names its own later sources. No token or path reaches a diagnostic.
pub(super) fn offered_token(file_variable: &str, variable: &str) -> Option<Zeroizing<String>> {
    first_token(
        std::env::var_os(file_variable).as_deref().map(Path::new),
        std::env::var(variable).ok(),
    )
}

/// `offered_token` over the file `file` names and the variable's `value`.
fn first_token(file: Option<&Path>, value: Option<String>) -> Option<Zeroizing<String>> {
    let one_token = |held: Zeroizing<String>| {
        let token = held.trim();
        (!token.is_empty() && !token.contains(char::is_whitespace))
            .then(|| Zeroizing::new(token.to_owned()))
    };
    file.filter(|file| !file.as_os_str().is_empty())
        .and_then(|file| std::fs::read_to_string(file).ok())
        .map(Zeroizing::new)
        .and_then(one_token)
        .or_else(|| value.map(Zeroizing::new).and_then(one_token))
}

#[cfg(test)]
mod tests {
    use super::*;

    // REQ-LOCALRUN-192: the file's trimmed token comes first; a file that is missing, empty or
    // not one token is skipped for the variable, which is taken by the same rule.
    #[test]
    fn the_offered_token_is_the_files_then_the_variables() {
        let files = tempfile::tempdir().unwrap();
        let file = |name: &str, contents: &str| {
            let path = files.path().join(name);
            std::fs::write(&path, contents).unwrap();
            path
        };
        let token = |file: Option<&Path>, value: Option<&str>| {
            first_token(file, value.map(str::to_owned)).map(|token| token.as_str().to_owned())
        };
        let held = file("held", " grant\n");
        assert_eq!(
            token(Some(&held), Some("variable")).as_deref(),
            Some("grant")
        );
        for skipped in [
            files.path().join("missing"),
            file("empty", ""),
            file("blank", " \n"),
            file("two", "grant other\n"),
        ] {
            assert_eq!(
                token(Some(&skipped), Some(" variable\n")).as_deref(),
                Some("variable"),
                "{}",
                skipped.display()
            );
            assert_eq!(token(Some(&skipped), None), None);
        }
        for value in [None, Some(""), Some("two tokens")] {
            assert_eq!(token(None, value), None, "{value:?}");
        }
    }
}
