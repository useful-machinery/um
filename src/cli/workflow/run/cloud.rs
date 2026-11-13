//! `um workflow run show RUN --cloud ORGANIZATION`, `um workflow run release RUN --cloud
//! ORGANIZATION` (REQ-LOCALRUN-189, REQ-LOCALRUN-187) and the control leaves `answer`,
//! `approve`, `hand-back`, `lift`, `stop` and `retry` with `--cloud` (REQ-LOCALRUN-210): the
//! Cloud run `RUN` names, as typed, read with its identity, observations and trigger
//! deliveries (REQ-RUN-96, REQ-RUN-87 and REQ-RUN-88), its subject claim released by an owner
//! (REQ-CLOUDDEF-19), or a control for its current wait carried to its runner (REQ-RUN-91),
//! through the public API under the CLI's session handling. Nothing under the `um` home is
//! read or written.

use std::io::{self, Write};

use anyhow::{Context, anyhow};
use serde::Serialize;
use serde_json::Value;
use um_api::{
    HttpClient, Run, RunApi, RunClaimRelease, RunControl, RunControlAnswer, RunControlKind,
    RunControlRequest, RunControlSubmission, RunControlWait, RunFailure, RunRead, RunSubjectClaim,
    RunTriggerDelivery,
};
use um_human_auth::Deployment;

use super::super::super::{
    self as cli, CommandFailure, HttpOptions, OrganizationArg, PrincipalAuthenticationArgs,
};
use crate::exit_code::ExitCode;

const SHOW_COMMAND: &str = "um workflow run show";
const RELEASE_COMMAND: &str = "um workflow run release";
/// The most deliveries `show` reads, the API's page bound.
const SHOWN_DELIVERIES: u32 = 100;
/// The run fields a definition-backed run's read carries and a run by `workflowPath` may
/// leave out; `show` reports an absent one as null.
const OBSERVED_FIELDS: [&str; 6] = [
    "definition",
    "subjectClaim",
    "workflowState",
    "workflowOutcome",
    "workflowProgress",
    "exits",
];

/// The `after_help` paragraph of a control leaf that takes `--cloud` (REQ-LOCALRUN-210).
pub(super) const CONTROL_AFTER_HELP: &str = "With --cloud ORGANIZATION, RUN is a Cloud run ID: the command reads that run and asks Cloud, under one request key sent again if the response is lost, to carry the control for the run's current wait to the runner executing it, which applies it as this command applies it to a local run. Cloud's acceptance is not the runner's application. It reads and writes no local run.";

/// The control a leaf asks Cloud to carry to the run's runner (REQ-LOCALRUN-210), with the
/// arguments its local form takes.
pub(super) enum Control {
    Answer { question: String, text: String },
    Approve,
    HandBack { notes: String },
    Lift { before: Option<String> },
    Stop { before: String, reason: String },
    Retry,
}

/// Why a control leaf sent nothing: the run does not wait on the wait its control resolves.
enum Unsent {
    NotWaiting,
    KindMismatch,
}

impl Control {
    fn leaf(&self) -> &'static str {
        match self {
            Self::Answer { .. } => "answer",
            Self::Approve => "approve",
            Self::HandBack { .. } => "hand-back",
            Self::Lift { .. } => "lift",
            Self::Stop { .. } => "stop",
            Self::Retry => "retry",
        }
    }

    fn command(&self) -> &'static str {
        match self {
            Self::Answer { .. } => "um workflow run answer",
            Self::Approve => "um workflow run approve",
            Self::HandBack { .. } => "um workflow run hand-back",
            Self::Lift { .. } => "um workflow run lift",
            Self::Stop { .. } => "um workflow run stop",
            Self::Retry => "um workflow run retry",
        }
    }

    /// The kind of wait the control resolves, which the run must currently wait on; a `lift`
    /// names the run's `stop` wait when it waits on one, and a `stop` names none.
    fn resolves(&self) -> Option<&'static str> {
        match self {
            Self::Answer { .. } => Some("answer"),
            Self::Approve | Self::HandBack { .. } => Some("approval"),
            Self::Retry => Some("repair"),
            Self::Lift { .. } | Self::Stop { .. } => None,
        }
    }

    /// The request naming `wait`, or the reason none is sent.
    fn request(&self, wait: Option<RunControlWait>) -> Result<RunControlRequest, Unsent> {
        let wait = match self.resolves() {
            Some(kind) => match wait {
                None => return Err(Unsent::NotWaiting),
                Some(wait) if wire(wait.kind) != kind => return Err(Unsent::KindMismatch),
                wait => wait,
            },
            None if matches!(self, Self::Lift { .. }) => {
                wait.filter(|wait| wire(wait.kind) == "stop")
            }
            None => None,
        };
        let kind = match self {
            Self::Answer { .. } => RunControlKind::RunControlKindAnswer,
            Self::Approve => RunControlKind::RunControlKindApprove,
            Self::HandBack { .. } => RunControlKind::RunControlKindHandBack,
            Self::Lift { .. } => RunControlKind::RunControlKindLift,
            Self::Stop { .. } => RunControlKind::RunControlKindStop,
            Self::Retry => RunControlKind::RunControlKindRetry,
        };
        let mut request = RunControlRequest::new(kind);
        request.wait = wait.map(|wait| Some(Box::new(wait)));
        match self {
            Self::Answer { question, text } => {
                request.answers = Some(vec![RunControlAnswer::new(question.clone(), text.clone())]);
            }
            Self::HandBack { notes } => request.notes = Some(notes.clone()),
            Self::Lift { before } => request.before.clone_from(before),
            Self::Stop { before, reason } => {
                request.before = Some(before.clone());
                request.reason = Some(reason.clone());
            }
            Self::Approve | Self::Retry => {}
        }
        Ok(request)
    }
}

/// The run's current wait as its latest progress report names it, if it waits.
fn current_wait(read: RunRead) -> Option<RunControlWait> {
    let RunRead::Materialized(run) = read else {
        return None;
    };
    let progress = run.workflow_progress.flatten()?;
    progress.wait.as_deref().map(RunControlWait::from)
}

/// The Cloud run one leaf acts on and how it reports.
pub(super) struct CloudRun {
    organization: OrganizationArg,
    run_id: String,
    authentication: PrincipalAuthenticationArgs,
    http: HttpOptions,
    json: bool,
}

impl CloudRun {
    /// `RUN` as the Cloud run id in `organization`, through `selector`'s authentication and
    /// transport; a `RUN` that is not UTF-8 is a usage error.
    pub(super) fn new(
        run: super::ExistingLocalRun,
        organization: OrganizationArg,
        selector: super::CloudSelector,
        json: bool,
    ) -> Result<Self, CommandFailure> {
        let run_id = run.run_dir.into_os_string().into_string().map_err(|_| {
            CommandFailure::with_exit_code(
                anyhow!("the Cloud run id RUN must be valid UTF-8"),
                ExitCode::UsageError,
            )
        })?;
        Ok(Self {
            organization,
            run_id,
            authentication: selector.authentication,
            http: selector.http,
            json,
        })
    }

    /// Reads the run and, for a definition-backed run holding a claim, the newest page of
    /// its trigger deliveries.
    pub(super) fn show(self) -> cli::CommandResult {
        cli::execute_deployment_command(
            Some(self),
            &["workflow", "run", "show"],
            "configure Useful Machinery runs",
            |cloud, deployment| {
                let read = cloud.with_api(deployment, |api| {
                    let run = match api.get(&cloud.organization, &cloud.run_id)? {
                        RunRead::Materialized(run) => run,
                        RunRead::Pending(_) => return Ok(None),
                    };
                    let claimed = matches!(run.definition, Some(Some(_)))
                        && matches!(run.subject_claim, Some(Some(_)));
                    let deliveries = if claimed {
                        Some(api.list_trigger_deliveries(
                            &cloud.organization,
                            &cloud.run_id,
                            Some(SHOWN_DELIVERIES),
                            None,
                        )?)
                    } else {
                        None
                    };
                    let value = serde_json::to_value(&*run).map_err(|_| RunFailure::Protocol {
                        credential_rejected: false,
                    })?;
                    Ok(Some((run, value, deliveries)))
                })?;
                match read {
                    Ok(Some((run, value, deliveries))) => {
                        cloud.write_shown(&run, value, deliveries)
                    }
                    Ok(None) => cloud.write_pending(),
                    Err(failure) => cloud.write_failure(SHOW_COMMAND, &failure),
                }
            },
        )
    }

    /// Asks Cloud to release the run's current claim under one fresh key.
    pub(super) fn release(self) -> cli::CommandResult {
        let key = um_support::generate_idempotency_key()
            .context("generate Cloud claim release request identity")?;
        cli::execute_deployment_command(
            Some(self),
            &["workflow", "run", "release"],
            "configure Useful Machinery runs",
            |cloud, deployment| {
                let released = cloud.with_api(deployment, |api| {
                    api.release_claim(&cloud.organization, &cloud.run_id, &key)
                })?;
                match released {
                    Ok(RunClaimRelease::Released(claim)) => cloud.write_released(&claim),
                    Ok(RunClaimRelease::Live) => cloud.write_refusal(
                        RELEASE_COMMAND,
                        "claim_live",
                        "The run is live and still holds its claim; nothing was released.",
                        ExitCode::GeneralFailure,
                    ),
                    Ok(RunClaimRelease::Changed) => cloud.write_refusal(
                        RELEASE_COMMAND,
                        "claim_changed",
                        "The run's current claim changed; nothing was released.",
                        ExitCode::GeneralFailure,
                    ),
                    Ok(RunClaimRelease::IdempotencyConflict) => cloud.write_refusal(
                        RELEASE_COMMAND,
                        "idempotency_conflict",
                        "Cloud answered this request's key for another request; nothing was released.",
                        ExitCode::GeneralFailure,
                    ),
                    Err(failure) => cloud.write_failure(RELEASE_COMMAND, &failure),
                }
            },
        )
    }

    /// Reads the run for its current wait, unless the control is a `stop`, and asks Cloud to
    /// carry the control naming that wait to the run's runner under one fresh key.
    pub(super) fn control(self, control: Control) -> cli::CommandResult {
        let key = um_support::generate_idempotency_key()
            .context("generate Cloud run control request identity")?;
        let command = control.command();
        cli::execute_deployment_command(
            Some(self),
            &["workflow", "run", control.leaf()],
            "configure Useful Machinery runs",
            |cloud, deployment| {
                let sent = cloud.with_api(deployment, |api| {
                    let wait = if matches!(control, Control::Stop { .. }) {
                        None
                    } else {
                        current_wait(api.get(&cloud.organization, &cloud.run_id)?)
                    };
                    match control.request(wait) {
                        Ok(request) => api
                            .request_control(&cloud.organization, &cloud.run_id, &key, &request)
                            .map(Ok),
                        Err(unsent) => Ok(Err(unsent)),
                    }
                })?;
                match sent {
                    Ok(Ok(RunControlSubmission::Accepted(accepted))) => {
                        cloud.write_accepted(command, &accepted)
                    }
                    Ok(Ok(RunControlSubmission::WaitNotCurrent)) => cloud.write_refusal(
                        command,
                        "wait_not_current",
                        "The run's wait changed before Cloud accepted the control; nothing was sent to the runner.",
                        ExitCode::GeneralFailure,
                    ),
                    Ok(Ok(RunControlSubmission::RunNotLive)) => cloud.write_refusal(
                        command,
                        "run_not_live",
                        "No runner is executing the run's current attempt; nothing was sent.",
                        ExitCode::GeneralFailure,
                    ),
                    Ok(Ok(RunControlSubmission::TooLarge)) => cloud.write_refusal(
                        command,
                        "control_too_large",
                        "The control is larger than a runner accepts; nothing was sent.",
                        ExitCode::GeneralFailure,
                    ),
                    Ok(Ok(RunControlSubmission::IdempotencyConflict)) => cloud.write_refusal(
                        command,
                        "idempotency_conflict",
                        "Cloud answered this request's key for another request; nothing was sent.",
                        ExitCode::GeneralFailure,
                    ),
                    Ok(Err(Unsent::NotWaiting)) => cloud.write_refusal(
                        command,
                        "run_not_waiting",
                        "The run does not wait, so there is nothing to resolve; nothing was sent.",
                        ExitCode::GeneralFailure,
                    ),
                    Ok(Err(Unsent::KindMismatch)) => cloud.write_refusal(
                        command,
                        "wait_kind_mismatch",
                        "The run waits on a kind of wait this command does not resolve; nothing was sent.",
                        ExitCode::GeneralFailure,
                    ),
                    Err(failure) => cloud.write_failure(command, &failure),
                }
            },
        )
    }

    /// Runs `operation` against the deployment under the leaf's session, again with a
    /// refreshed session when the first is rejected.
    fn with_api<T>(
        &self,
        deployment: &Deployment,
        mut operation: impl FnMut(&RunApi<'_>) -> Result<T, RunFailure>,
    ) -> anyhow::Result<Result<T, RunFailure>> {
        let policy = self.http.transport_policy();
        let client = HttpClient::new(policy)
            .map_err(|error| anyhow!(error))
            .context("prepare Cloud run networking")?;
        cli::execute_selected_api_operation(
            cli::principal_api_context(
                &client,
                deployment,
                &self.authentication,
                "acquire human session for Cloud run operation",
            ),
            |token| {
                let api = RunApi::new(deployment.fingerprint().api_url(), token, policy, &client)
                    .map_err(|error| anyhow!(error))
                    .context("prepare Cloud run networking")?;
                Ok(operation(&api))
            },
            RunFailure::credential_rejected,
            || RunFailure::Unauthenticated,
            RunFailure::Unreachable,
        )
    }

    fn write_shown(
        &self,
        typed: &Run,
        mut run: Value,
        deliveries: Option<(Vec<RunTriggerDelivery>, Option<String>)>,
    ) -> cli::CommandResult {
        if let Some(fields) = run.as_object_mut() {
            for field in OBSERVED_FIELDS {
                fields.entry(field).or_insert(Value::Null);
            }
        }
        let (deliveries, more_deliveries) = match deliveries {
            Some((deliveries, cursor)) => (Some(deliveries), cursor.is_some()),
            None => (None, false),
        };
        let written = if self.json {
            cli::write_pretty_json(&ShowDocument {
                schema_version: 1,
                command: SHOW_COMMAND,
                outcome: "shown",
                exit_status: ExitCode::Success.as_u8(),
                organization_ref: &self.organization,
                run_id: &self.run_id,
                run: Some(&run),
                deliveries: deliveries.as_deref(),
                more_deliveries,
            })
        } else {
            write_plain(
                &self.organization,
                typed,
                &run,
                deliveries.as_deref(),
                more_deliveries,
            )
        };
        written.with_context(|| format!("write {SHOW_COMMAND} output"))?;
        Ok(ExitCode::Success)
    }

    /// A run whose creation Cloud has accepted but not yet materialized.
    fn write_pending(&self) -> cli::CommandResult {
        let written = if self.json {
            cli::write_pretty_json(&ShowDocument {
                schema_version: 1,
                command: SHOW_COMMAND,
                outcome: "pending",
                exit_status: ExitCode::Success.as_u8(),
                organization_ref: &self.organization,
                run_id: &self.run_id,
                run: None,
                deliveries: None,
                more_deliveries: false,
            })
        } else {
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "run: {}", text(&self.run_id))
                .and_then(|()| writeln!(stdout, "organization: {}", text(&self.organization)))
                .and_then(|()| writeln!(stdout, "state: creation pending"))
                .and_then(|()| stdout.flush())
        };
        written.with_context(|| format!("write {SHOW_COMMAND} output"))?;
        Ok(ExitCode::Success)
    }

    fn write_released(&self, claim: &RunSubjectClaim) -> cli::CommandResult {
        let written = if self.json {
            cli::write_pretty_json(&ReleasedDocument {
                schema_version: 1,
                command: RELEASE_COMMAND,
                outcome: "released",
                exit_status: ExitCode::Success.as_u8(),
                organization_ref: &self.organization,
                run_id: &self.run_id,
                claim,
            })
        } else {
            let mut stdout = io::stdout().lock();
            writeln!(
                stdout,
                "✓ Released claim {} of run {} for {} (cause {}).",
                text(&claim.claim_id),
                text(&self.run_id),
                text(&claim.subject),
                claim
                    .release_cause
                    .as_deref()
                    .map_or_else(|| "-".to_owned(), text)
            )
            .and_then(|()| stdout.flush())
        };
        written.with_context(|| format!("write {RELEASE_COMMAND} output"))?;
        Ok(ExitCode::Success)
    }

    fn write_accepted(&self, command: &'static str, control: &RunControl) -> cli::CommandResult {
        let written = if self.json {
            cli::write_pretty_json(&AcceptedDocument {
                schema_version: 1,
                command,
                outcome: "accepted",
                exit_status: ExitCode::Success.as_u8(),
                organization_ref: &self.organization,
                run_id: &self.run_id,
                control,
            })
        } else {
            let mut stdout = io::stdout().lock();
            writeln!(
                stdout,
                "✓ Cloud accepted {} control {} for run {} attempt {} (state {}); the runner executing the attempt applies it.",
                wire(control.kind),
                text(&control.id),
                text(&self.run_id),
                control.attempt_number,
                wire(control.state),
            )
            .and_then(|()| stdout.flush())
        };
        written.with_context(|| format!("write {command} output"))?;
        Ok(ExitCode::Success)
    }

    /// Reports `failure` as a refusal under [`failure_diagnostic`].
    fn write_failure(&self, command: &'static str, failure: &RunFailure) -> cli::CommandResult {
        let (code, message, exit) = failure_diagnostic(command, failure);
        self.write_refusal(command, code, message, exit)
    }

    fn write_refusal(
        &self,
        command: &'static str,
        code: &'static str,
        message: &'static str,
        exit: ExitCode,
    ) -> cli::CommandResult {
        let written = if self.json {
            cli::write_pretty_json(&RefusedDocument {
                schema_version: 1,
                command,
                outcome: "refused",
                exit_status: exit.as_u8(),
                organization_ref: &self.organization,
                run_id: &self.run_id,
                diagnostics: [Diagnostic { code, message }],
            })
        } else {
            let mut stderr = io::stderr().lock();
            writeln!(stderr, "{command} refused: {code}: {message}").and_then(|()| stderr.flush())
        };
        written.with_context(|| format!("write {command} output"))?;
        Ok(exit)
    }
}

/// The diagnostic code, message and exit status leaf `command` reports `failure` under: a
/// rejected session exits 3, Cloud being unreachable or rate limiting the request exits 4,
/// and anything else exits 1. Only `show` and `release` read the run's subject claim, so
/// only their not-found names it.
fn failure_diagnostic(
    command: &str,
    failure: &RunFailure,
) -> (&'static str, &'static str, ExitCode) {
    match failure {
        RunFailure::Unauthenticated => (
            "authentication_required",
            "Cloud rejected the session; sign in with um auth login and try again.",
            ExitCode::AuthenticationRequired,
        ),
        RunFailure::Forbidden => (
            "forbidden",
            "The principal may not do this to the run.",
            ExitCode::GeneralFailure,
        ),
        RunFailure::NotFound if matches!(command, SHOW_COMMAND | RELEASE_COMMAND) => (
            "not_found",
            "Cloud has no such run in this organization, or the run has no subject claim.",
            ExitCode::GeneralFailure,
        ),
        RunFailure::NotFound => (
            "not_found",
            "Cloud has no such run in this organization.",
            ExitCode::GeneralFailure,
        ),
        RunFailure::InvalidInput => (
            "invalid_input",
            "Cloud refused the request as invalid.",
            ExitCode::GeneralFailure,
        ),
        RunFailure::Conflict => (
            "conflict",
            "Cloud refused the request because of the run's state.",
            ExitCode::GeneralFailure,
        ),
        RunFailure::Unreachable(_)
        | RunFailure::RetryAfter(_)
        | RunFailure::RetryAmbiguousRateLimited => (
            "unavailable",
            "Cloud could not be reached or is limiting requests; try again.",
            ExitCode::Unavailable,
        ),
        _ if failure.credential_rejected() => (
            "authentication_required",
            "Cloud rejected the session; sign in with um auth login and try again.",
            ExitCode::AuthenticationRequired,
        ),
        _ => (
            "protocol_error",
            "Cloud answered with a response this CLI does not accept.",
            ExitCode::GeneralFailure,
        ),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShowDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    organization_ref: &'a str,
    run_id: &'a str,
    run: Option<&'a Value>,
    deliveries: Option<&'a [RunTriggerDelivery]>,
    more_deliveries: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleasedDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    organization_ref: &'a str,
    run_id: &'a str,
    claim: &'a RunSubjectClaim,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AcceptedDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    organization_ref: &'a str,
    run_id: &'a str,
    control: &'a RunControl,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RefusedDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    organization_ref: &'a str,
    run_id: &'a str,
    diagnostics: [Diagnostic; 1],
}

#[derive(Serialize)]
struct Diagnostic {
    code: &'static str,
    message: &'static str,
}

fn text(value: &str) -> String {
    um_execution::normalize_terminal_scalar(value.as_bytes())
}

/// The string at `key` of `object`, or `-` when it is absent, null or not a string.
fn field(object: &Value, key: &str) -> String {
    object[key].as_str().map_or_else(|| "-".to_owned(), text)
}

/// A position or wait step path as its elements joined by `.`, an item by its number.
fn path(elements: &Value) -> String {
    elements.as_array().map_or_else(
        || "-".to_owned(),
        |elements| {
            elements
                .iter()
                .map(|element| match element {
                    Value::String(step) => text(step),
                    other => other["item"]
                        .as_i64()
                        .map_or_else(|| "?".to_owned(), |item| item.to_string()),
                })
                .collect::<Vec<_>>()
                .join(".")
        },
    )
}

fn wire(value: impl Serialize) -> String {
    um_support::wire_name(value).unwrap_or_else(|| "-".to_owned())
}

/// The run as `show` prints it without `--json`: its identity, owner, route and what it
/// waits for, workspace, claim, workflow state and outcome, latest progress, exits with
/// their mirror results and its deliveries, acceptance apart from the runner's application.
fn write_plain(
    organization: &str,
    typed: &Run,
    run: &Value,
    deliveries: Option<&[RunTriggerDelivery]>,
    more_deliveries: bool,
) -> io::Result<()> {
    let mut out = io::stdout().lock();
    writeln!(out, "run: {}", field(run, "id"))?;
    writeln!(out, "organization: {}", text(organization))?;
    writeln!(out, "state: {}", field(run, "state"))?;
    cli::run::write_route(&mut out, typed)?;
    if let Some(observation) = typed.observation.as_deref() {
        cli::run::write_waiting(&mut out, observation)?;
    }
    let definition = &run["definition"];
    if definition.is_object() {
        writeln!(
            out,
            "definition: {} revision {}",
            field(definition, "definitionId"),
            definition["revision"]
        )?;
        writeln!(out, "tree: {}", field(definition, "treeDigest"))?;
        writeln!(out, "workflow: {}", field(definition, "workflowPath"))?;
    } else {
        writeln!(out, "definition: none")?;
        writeln!(
            out,
            "workflow: {}",
            field(&run["workflowDefinitionSource"], "workflowPath")
        )?;
    }
    let workspace = &run["primaryWorkspaceSource"];
    writeln!(
        out,
        "workspace: {} {} commit {}",
        field(workspace, "kind"),
        field(workspace, "providerKind"),
        field(workspace, "commitOid")
    )?;
    let claim = &run["subjectClaim"];
    if claim.is_object() {
        write!(
            out,
            "subject claim: {} · {} · {}",
            field(claim, "claimId"),
            field(claim, "subject"),
            field(claim, "state")
        )?;
        if let Some(cause) = claim["releaseCause"].as_str() {
            write!(out, " ({})", text(cause))?;
        }
        writeln!(out)?;
    } else {
        writeln!(out, "subject claim: none")?;
    }
    writeln!(out, "workflow state: {}", field(run, "workflowState"))?;
    writeln!(out, "workflow outcome: {}", field(run, "workflowOutcome"))?;
    write_progress(&mut out, &run["workflowProgress"])?;
    write_exits(&mut out, &run["exits"])?;
    write_deliveries(&mut out, deliveries, more_deliveries)?;
    out.flush()
}

fn write_progress(out: &mut impl Write, progress: &Value) -> io::Result<()> {
    if !progress.is_object() {
        return writeln!(out, "progress: none");
    }
    write!(
        out,
        "progress: attempt {} at {}",
        progress["attempt"],
        path(&progress["position"])
    )?;
    let wait = &progress["wait"];
    if wait.is_object() {
        write!(
            out,
            " · waiting {} at {} since {}",
            field(wait, "kind"),
            path(&wait["step"]),
            field(wait, "since")
        )?;
    } else {
        write!(out, " · running")?;
    }
    writeln!(out, " · reported {}", field(progress, "reportedAt"))
}

fn write_exits(out: &mut impl Write, exits: &Value) -> io::Result<()> {
    let Some(exits) = exits.as_array() else {
        return writeln!(out, "exits: none");
    };
    if exits.is_empty() {
        return writeln!(out, "exits: none");
    }
    writeln!(out, "exits:")?;
    for exit in exits {
        writeln!(
            out,
            "  attempt {} · outcome {} · claim release {}",
            exit["attemptNumber"],
            field(exit, "workflowOutcome"),
            if exit["claimRelease"] == true {
                "yes"
            } else {
                "no"
            }
        )?;
        match exit["mirror"].as_array() {
            None => writeln!(out, "    mirror: not attempted")?,
            Some(results) => {
                for result in results {
                    writeln!(out, "    mirror {}", mirror_text(result))?;
                }
            }
        }
    }
    Ok(())
}

/// A mirror result as `<hook> <issue> · comment <body>|state <state> · <outcome>`, with a
/// skip's reason, a failure's code and its diagnostic when present.
fn mirror_text(result: &Value) -> String {
    let action = &result["action"];
    let action = match action["kind"].as_str() {
        Some("comment") => format!("comment {}", field(action, "body")),
        Some("state") => format!("state {}", field(action, "state")),
        _ => "-".to_owned(),
    };
    let mut line = format!(
        "{} {} · {action} · {}",
        field(result, "hook"),
        field(result, "issue"),
        field(result, "outcome")
    );
    for key in ["reason", "code", "diagnostic"] {
        if let Some(value) = result[key].as_str() {
            line.push(' ');
            line.push_str(&text(value));
        }
    }
    line
}

fn write_deliveries(
    out: &mut impl Write,
    deliveries: Option<&[RunTriggerDelivery]>,
    more: bool,
) -> io::Result<()> {
    let Some(deliveries) = deliveries.filter(|deliveries| !deliveries.is_empty()) else {
        return writeln!(out, "deliveries: none");
    };
    writeln!(out, "deliveries:")?;
    for delivery in deliveries {
        let event = serde_json::to_value(&delivery.event).unwrap_or(Value::Null);
        write!(
            out,
            "  {} · {} revision {} · {} {} · claim {} · accepted {} · ",
            text(&delivery.id),
            text(&delivery.trigger_name),
            delivery.revision,
            wire(delivery.event_name),
            field(&event["issue"], "identifier"),
            text(&delivery.claim.claim_id),
            text(&delivery.accepted_at)
        )?;
        let state = wire(delivery.state);
        match (&delivery.action, &delivery.reason) {
            (Some(action), _) => write!(out, "{state} {}", wire(action))?,
            (None, Some(reason)) => write!(out, "{state} {}", text(reason))?,
            (None, None) => write!(out, "{state}")?,
        }
        if let Some(delivered) = &delivery.delivered_at {
            write!(out, " · delivered {}", text(delivered))?;
        }
        if let Some(settled) = &delivery.settled_at {
            write!(out, " · settled {}", text(settled))?;
        }
        writeln!(out)?;
    }
    if more {
        writeln!(out, "older deliveries: not shown")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use um_api::RunFailure;

    use super::{SHOW_COMMAND, failure_diagnostic};
    use crate::exit_code::ExitCode;

    // REQ-COMPAT-20, REQ-LOCALRUN-187, REQ-LOCALRUN-210: a refusal Cloud answers `409`
    // under a type this release does not name is reported as a conflict, exit 1.
    #[test]
    fn an_unnamed_conflict_is_reported_as_a_conflict() {
        let (code, _, exit) = failure_diagnostic(SHOW_COMMAND, &RunFailure::Conflict);
        assert_eq!((code, exit), ("conflict", ExitCode::GeneralFailure));
    }
}
