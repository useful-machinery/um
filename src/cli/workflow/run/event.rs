//! `um workflow run event` (REQ-LOCALRUN-208): applies a delivered provider event to the
//! run's control trigger. The event acts only when its `issueUpdatedAt` is strictly later
//! than the claim's admission observation; the issue is then read fresh through the mirror's
//! Linear target, and the fresh state, never the event's own `to`, is `event.to` for the
//! control's `when`, read from the run's revision tree against the revision's resolution
//! record. `then` or `else` ends the run through `end.rs`'s `cancel` or `withdraw` path.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use um_execution::{
    ControlTrigger, DefinitionError, ResolvedWorkflow, WorkflowHome, WorkflowRunRecord,
    normalize_terminal_path, normalize_terminal_scalar, read_definition, read_revision_bindings,
    resolve_workflow_file,
};

use super::control::{Acted, ControlFailure, ControlTarget, RecordedControl, Refusal};
use super::mirror::linear::{IssueObservation, LinearTarget};
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Apply a delivered provider event to a run's control trigger";
const COMMAND: &str = "um workflow run event";
/// The most bytes the command reads of the event or the claim file.
const MAXIMUM_FILE_BYTES: u64 = 1024 * 1024;
const ROOT_WORKFLOW_FILE: &str = "workflow.yaml";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[arg(
        long,
        value_name = "FILE",
        help = "Delivered event: a JSON document with its event and payload"
    )]
    event: PathBuf,

    #[arg(
        long,
        value_name = "FILE",
        help = "The run's claim: a JSON document with its issue and admission observation"
    )]
    claim: PathBuf,

    #[command(flatten)]
    output: super::control::JsonOutput,
}

/// The shared event `{source, event, at, payload}` (REQ-TRIGGER-124); the command reads its
/// `event` and `payload`.
#[derive(Deserialize)]
struct DeliveredEvent {
    event: String,
    payload: Value,
}

/// The claim `{connection, issue, observation}`; the command reads its `issue`, the Linear
/// issue id, and the admission observation.
#[derive(Deserialize)]
struct Claim {
    issue: String,
    observation: ClaimObservation,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClaimObservation {
    team: String,
    state: String,
    updated_at: String,
}

/// The fresh read the command acted on, in the claim observation's shape.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Observation {
    team: Option<String>,
    state: String,
    updated_at: Option<String>,
}

/// The class of the cause of an input the command could not use, kept in the refusal's
/// diagnostic: the bytes could not be read, were not of their shape, or the revision's root
/// workflow did not resolve.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum Cause {
    Read,
    Parse,
    Resolve,
}

impl Cause {
    /// The class of a definition store failure: a record that was read but is not of its
    /// shape, or does not list the revision, is `parse`; any other failure is `read`.
    fn of(error: &DefinitionError) -> Self {
        match error {
            DefinitionError::RecordInvalid { source: None, .. }
            | DefinitionError::TreeRetainedModified { .. } => Self::Parse,
            _ => Self::Read,
        }
    }
}

/// Why the command did not apply the event: a refusal, with its cause's class when it
/// refused an input it could not use, or an operational failure.
enum EventFailure {
    Refused(Refusal, Option<Cause>),
    Failed(anyhow::Error),
}

impl From<Refusal> for EventFailure {
    fn from(refusal: Refusal) -> Self {
        Self::Refused(refusal, None)
    }
}

impl From<ControlFailure> for EventFailure {
    fn from(failure: ControlFailure) -> Self {
        match failure {
            ControlFailure::Refused(refusal) => Self::Refused(refusal, None),
            ControlFailure::Failed(error) => Self::Failed(error),
        }
    }
}

impl From<anyhow::Error> for EventFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Failed(error)
    }
}

/// What the command applied: the trigger, its action and what the action did.
struct Applied {
    trigger: String,
    action: String,
    acted: Acted,
    observation: Observation,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        let json = self.output.json;
        let run_directory = match self.run.resolve() {
            Ok(run_directory) => run_directory,
            Err(error) => {
                return write_refused(
                    None,
                    &Refusal::new(error.code, error.message),
                    None,
                    None,
                    json,
                );
            }
        };
        let target = match super::control::open(&run_directory) {
            Ok(target) => target,
            Err(refusal) => {
                return write_refused(Some(&run_directory), &refusal, None, None, json);
            }
        };
        let mut observation = None;
        match self.apply(&target, &mut observation) {
            Ok(applied) => write_applied(&target, &applied, json),
            Err(EventFailure::Refused(refusal, cause)) => write_refused(
                Some(&target.run_directory),
                &refusal,
                cause,
                observation.as_ref(),
                json,
            ),
            Err(EventFailure::Failed(error)) => Err(error
                .context(format!(
                    "{COMMAND} {}",
                    normalize_terminal_path(&target.run_directory)
                ))
                .into()),
        }
    }

    /// Applies the event to `target`, leaving the fresh read in `observed` once it is made.
    fn apply(
        &self,
        target: &ControlTarget,
        observed: &mut Option<Observation>,
    ) -> Result<Applied, EventFailure> {
        let event: DeliveredEvent = read_document(&self.event).map_err(|cause| {
            EventFailure::Refused(
                Refusal::new(
                    "event_invalid",
                    "The event file is not a JSON document with `event` and `payload`.",
                ),
                Some(cause),
            )
        })?;
        let claim: Claim = read_document(&self.claim).map_err(|cause| {
            EventFailure::Refused(
                Refusal::new(
                    "claim_invalid",
                    "The claim file is not a JSON document with `issue` and an `observation` of `team`, `state` and `updatedAt`.",
                ),
                Some(cause),
            )
        })?;
        if event.payload.pointer("/issue/id").and_then(Value::as_str) != Some(&claim.issue) {
            return Err(Refusal::new(
                "claim_mismatch",
                "The event's `issue.id` is not the claim's issue.",
            )
            .into());
        }
        if !strictly_later(
            event.payload.get("issueUpdatedAt"),
            &claim.observation.updated_at,
        ) {
            return Err(Refusal::new(
                "event_not_later",
                "The event's `issueUpdatedAt` is not strictly later than the claim's admission observation.",
            )
            .into());
        }
        let revision = Revision::read(&target.record)?;
        let trigger = ControlTrigger::find(&revision.workflow.definition, &event.event)
            .ok_or_else(|| {
                control_unavailable(format!(
                    "The run's revision has no control trigger for `{}`.",
                    event.event
                ))
            })?;

        let fresh = LinearTarget::from_environment()
            .observe(&claim.issue)
            .map_err(|failure| Refusal::new("issue_unavailable", failure.diagnostic))?;
        let IssueObservation {
            team,
            state,
            state_name,
            updated_at,
        } = fresh;
        let observation = observed.insert(Observation {
            team,
            state,
            updated_at,
        });
        if observation.team.as_deref() != Some(&claim.observation.team) {
            return Err(Refusal::new(
                "issue_unavailable",
                "The issue is no longer in the claim's team.",
            )
            .into());
        }
        if observation.state == claim.observation.state {
            return Err(Refusal::new(
                "issue_in_owned_state",
                "The issue is still in the state the claim owns it in.",
            )
            .into());
        }

        // REQ-WFDEF-32: with a resolution record, `event.to` and each binding are state ids;
        // without one, names as the bindings snapshot holds them.
        let to = if revision.resolution.is_some() {
            &observation.state
        } else {
            &state_name
        };
        let action = trigger
            .action(&json!({ "to": to }), |key| revision.bound(key))
            .map_err(|error| control_unavailable(error.to_string()))?;
        let acted = match action {
            "cancel" => super::end::cancel_run(target)?,
            "withdraw" => super::end::withdraw_run(target)?,
            other => {
                return Err(control_unavailable(format!(
                    "The control trigger's action `{other}` is not one this command applies."
                ))
                .into());
            }
        };
        Ok(Applied {
            trigger: trigger.name().to_owned(),
            action: action.to_owned(),
            acted,
            observation: observed
                .take()
                .ok_or_else(|| anyhow!("observation taken"))?,
        })
    }
}

/// The run's revision as the control trigger reads it: the root workflow of its tree and the
/// bound value of each binding, by resolved id or by name.
struct Revision {
    workflow: ResolvedWorkflow,
    resolution: Option<BTreeMap<String, String>>,
    names: BTreeMap<String, String>,
}

impl Revision {
    /// The revision `record` names, read from the um home: `control_unavailable` for a run
    /// with no definition or revision, or whose revision cannot be read, the latter with its
    /// cause's class.
    fn read(record: &WorkflowRunRecord) -> Result<Self, EventFailure> {
        let (Some(definition), Some(revision)) = (&record.definition, record.revision) else {
            return Err(
                control_unavailable("The run was not started from a definition revision.").into(),
            );
        };
        let unreadable = |what: &str, code: &str, cause: Cause| {
            EventFailure::Refused(
                control_unavailable(format!(
                    "The {what} of {definition} revision {revision} cannot be read: {code}."
                )),
                Some(cause),
            )
        };
        let stored_failure = |what: &'static str| {
            move |error: DefinitionError| unreadable(what, error.code(), Cause::of(&error))
        };
        let home = WorkflowHome::from_environment().map_err(stored_failure("um home"))?;
        let stored = read_definition(&home, definition).map_err(stored_failure("record"))?;
        let listed = stored
            .record
            .revision(revision)
            .ok_or_else(|| unreadable("record", "revision not listed", Cause::Parse))?;
        let copy = home
            .retained_tree(definition, listed)
            .map_err(stored_failure("tree"))?;
        let workflow = resolve_workflow_file(&copy, &copy.join(ROOT_WORKFLOW_FILE))
            .map_err(|_| unreadable("tree", "workflow.yaml does not resolve", Cause::Resolve))?;
        let resolution = home
            .revision_resolution(definition, revision)
            .map_err(stored_failure("resolution record"))?;
        let names = read_revision_bindings(&home, definition, revision)
            .map_err(stored_failure("bindings snapshot"))?
            .bindings()
            .iter()
            .filter_map(|(key, bound)| Some((key.clone(), bound.value.as_text()?.to_owned())))
            .collect();
        Ok(Self {
            workflow,
            resolution,
            names,
        })
    }

    /// Binding `key`'s resolved id, or without a resolution record its bound name.
    fn bound(&self, key: &str) -> Option<String> {
        self.resolution
            .as_ref()
            .unwrap_or(&self.names)
            .get(key)
            .cloned()
    }
}

fn control_unavailable(message: impl Into<String>) -> Refusal {
    Refusal::new("control_unavailable", message)
}

/// The JSON document at `path`, read up to `MAXIMUM_FILE_BYTES`, as `T`: `read` when the
/// file cannot be opened or read or is larger, and `parse` when it is not such a document.
fn read_document<T: DeserializeOwned>(path: &Path) -> Result<T, Cause> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| file.take(MAXIMUM_FILE_BYTES + 1).read_to_end(&mut bytes))
        .map_err(|_| Cause::Read)?;
    if u64::try_from(bytes.len()).map_or(true, |length| length > MAXIMUM_FILE_BYTES) {
        return Err(Cause::Read);
    }
    serde_json::from_slice(&bytes).map_err(|_| Cause::Parse)
}

/// Whether the event's `issueUpdatedAt` is an RFC 3339 time strictly later than the
/// admission observation's; a missing or unreadable time never is.
fn strictly_later(event: Option<&Value>, admitted: &str) -> bool {
    let parse = |text: &str| OffsetDateTime::parse(text, &Rfc3339).ok();
    matches!(
        (event.and_then(Value::as_str).and_then(parse), parse(admitted)),
        (Some(event), Some(admitted)) if event > admitted
    )
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AppliedDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    run_directory: &'a str,
    run: &'a str,
    trigger: &'a str,
    action: &'a str,
    action_outcome: &'static str,
    control: &'a RecordedControl,
    observation: &'a Observation,
}

fn write_applied(
    target: &ControlTarget,
    applied: &Applied,
    json: bool,
) -> super::super::super::CommandResult {
    if json {
        let run_directory = target
            .run_directory
            .to_str()
            .ok_or_else(|| anyhow!("normalized run path is not UTF-8"))?;
        super::super::super::write_pretty_json(&AppliedDocument {
            schema_version: 1,
            command: COMMAND,
            outcome: "applied",
            exit_status: ExitCode::Success.as_u8(),
            run_directory,
            run: &target.record.id,
            trigger: &applied.trigger,
            action: &applied.action,
            action_outcome: applied.acted.outcome,
            control: &applied.acted.control,
            observation: &applied.observation,
        })
        .with_context(|| format!("write {COMMAND} output"))?;
    } else {
        let mut stdout = io::stdout().lock();
        write!(
            stdout,
            "applied {} of trigger {} to {} as {}",
            applied.action,
            normalize_terminal_scalar(applied.trigger.as_bytes()),
            normalize_terminal_scalar(target.record.id.as_bytes()),
            applied.acted.control.path
        )
        .and_then(|()| match applied.acted.outcome {
            "recorded" => Ok(()),
            outcome => write!(stdout, " · run {outcome}"),
        })
        .and_then(|()| writeln!(stdout))
        .and_then(|()| stdout.flush())
        .with_context(|| format!("write {COMMAND} output"))?;
    }
    Ok(ExitCode::Success)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RefusedDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_directory: Option<&'a str>,
    diagnostics: [RefusalDiagnostic<'a>; 1],
    observation: Option<&'a Observation>,
}

#[derive(Serialize)]
struct RefusalDiagnostic<'a> {
    code: &'static str,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cause: Option<Cause>,
}

/// Prints `refusal` and exits 1: with `--json` the `refused` document, its diagnostic with
/// the class of the cause when one is known, with the fresh read once one was made, and
/// otherwise the control commands' stderr line.
fn write_refused(
    run_directory: Option<&Path>,
    refusal: &Refusal,
    cause: Option<Cause>,
    observation: Option<&Observation>,
    json: bool,
) -> super::super::super::CommandResult {
    if !json {
        return super::control::write_refusal(COMMAND, run_directory, refusal, false);
    }
    super::super::super::write_pretty_json(&RefusedDocument {
        schema_version: 1,
        command: COMMAND,
        outcome: "refused",
        exit_status: ExitCode::GeneralFailure.as_u8(),
        run_directory: run_directory.and_then(Path::to_str),
        diagnostics: [RefusalDiagnostic {
            code: refusal.code,
            message: &refusal.message,
            cause,
        }],
        observation,
    })
    .with_context(|| format!("write {COMMAND} output"))?;
    Ok(ExitCode::GeneralFailure)
}
