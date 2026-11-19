use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;
use serde_json::Value;
use useful_api::{
    ProjectReadinessBlocker, Publication, PublicationState, Run, RunArtifactDelivery,
    RunCancellationReceipt, RunHeldRunState, RunObservation, RunPublicationHandoffState,
};
use useful_execution::visible_text;

use crate::exit_code::ExitCode;

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct CloudSnapshot {
    pub run_id: Option<String>,
    pub run: Option<Box<Run>>,
    pub publication: Option<Box<Publication>>,
    pub cancellation_request: Option<Box<RunCancellationReceipt>>,
    pub replayed: Option<bool>,
}

impl CloudSnapshot {
    pub(super) fn for_run(run_id: impl Into<String>) -> Self {
        Self {
            run_id: Some(run_id.into()),
            ..Self::default()
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CloudResult<'a> {
    schema_version: u8,
    operation: &'static str,
    deployment: &'a str,
    organization_ref: &'a str,
    run_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_set_id: Option<&'a str>,
    outcome: &'static str,
    run: Option<&'a Run>,
    publication: Option<&'a Publication>,
    cancellation_request: Option<&'a RunCancellationReceipt>,
    replayed: Option<bool>,
    error: Option<CloudError<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CloudError<'a> {
    code: &'static str,
    idempotency_key: Option<&'a str>,
    requested_mode: Option<&'a str>,
    request_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    problem_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostic: Option<InputAdmissionDiagnostic<'a>>,
    /// The rejected response's `X-Request-ID`; `request_id` names a cancellation request.
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<&'a str>,
    /// A `project-not-ready` rejection's readiness blockers, by wire name.
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    blockers: &'a [ProjectReadinessBlocker],
}

#[derive(Serialize)]
struct InputAdmissionDiagnostic<'a> {
    code: &'a str,
    input: &'a str,
}

/// A reviewed create-admission rejection: only closed codes, slugs and validated ids,
/// never server prose.
pub(super) struct AdmissionProblem<'a> {
    pub code: &'static str,
    pub status: u16,
    pub problem_type: &'a str,
    pub diagnostic: Option<(&'a str, &'a str)>,
    pub correlation_id: Option<&'a str>,
    pub blockers: &'a [ProjectReadinessBlocker],
}

pub(super) fn write_admission_failure(
    deployment: &str,
    organization: &str,
    input_set_id: Option<&str>,
    key: Option<&str>,
    problem: AdmissionProblem<'_>,
) -> anyhow::Result<()> {
    let result = CloudResult {
        schema_version: 1,
        operation: "create",
        deployment,
        organization_ref: organization,
        run_id: None,
        input_set_id,
        outcome: "error",
        run: None,
        publication: None,
        cancellation_request: None,
        replayed: None,
        error: Some(CloudError {
            code: problem.code,
            idempotency_key: key,
            requested_mode: None,
            request_id: None,
            http_status: Some(problem.status),
            problem_type: Some(format!(
                "https://api.usefulmachinery.com/problems/{}",
                problem.problem_type
            )),
            diagnostic: problem
                .diagnostic
                .map(|(code, input)| InputAdmissionDiagnostic { code, input }),
            correlation_id: problem.correlation_id,
            blockers: problem.blockers,
        }),
    };
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &result)?;
    writeln!(stdout)?;
    Ok(())
}

pub(super) struct CloudOutput<'a> {
    pub operation: &'static str,
    pub deployment: &'a str,
    pub organization: &'a str,
    pub snapshot: &'a CloudSnapshot,
    pub json: bool,
}

pub(super) fn write_cloud(
    output: CloudOutput<'_>,
    outcome: &'static str,
    error_code: Option<&'static str>,
    key: Option<&str>,
    mode: Option<&str>,
    exit: ExitCode,
) -> anyhow::Result<ExitCode> {
    let CloudOutput {
        operation,
        deployment,
        organization,
        snapshot,
        json,
    } = output;
    if json {
        let result = CloudResult {
            schema_version: 1,
            operation,
            deployment,
            organization_ref: organization,
            run_id: snapshot.run_id.as_deref(),
            input_set_id: None,
            outcome,
            run: snapshot.run.as_deref(),
            publication: snapshot.publication.as_deref(),
            cancellation_request: snapshot.cancellation_request.as_deref(),
            replayed: snapshot.replayed,
            error: error_code.map(|code| CloudError {
                code,
                idempotency_key: key,
                requested_mode: mode,
                request_id: snapshot
                    .cancellation_request
                    .as_deref()
                    .map(|request| request.id.as_str()),
                http_status: None,
                problem_type: None,
                diagnostic: None,
                correlation_id: None,
                blockers: &[],
            }),
        };
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        serde_json::to_writer(&mut stdout, &result).context("serialize Cloud run result")?;
        writeln!(stdout).context("write Cloud run result")?;
    } else if let Some(code) = error_code {
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "error: Cloud run {operation}: {code}\n\nrun: {}\norganization: {organization}\nidempotency key: {}",
            snapshot.run_id.as_deref().unwrap_or("not established"),
            key.unwrap_or("none"),
        )?;
        if operation == "cancel" {
            write_cancel_context(&mut stderr, snapshot, mode)?;
        } else {
            if let Some(run) = snapshot.run.as_deref() {
                writeln!(
                    stderr,
                    "last observed run state: {}",
                    super::enum_text(&run.state)?
                )?;
                if let Some(handoff) = run.publication.as_deref() {
                    writeln!(
                        stderr,
                        "last observed handoff: {} · publication: {}",
                        super::enum_text(&handoff.state)?,
                        handoff.publication_id.as_deref().unwrap_or("none")
                    )?;
                }
            }
            if let Some(publication) = snapshot.publication.as_deref() {
                writeln!(
                    stderr,
                    "last observed publication: {} · state: {}",
                    publication.id,
                    super::enum_text(&publication.state)?
                )?;
            }
        }
        writeln!(
            stderr,
            "\n{}",
            if key.is_some() {
                "Inspect the run and reconcile this key with the same requested mode before retrying if acceptance is uncertain."
            } else {
                "Inspect the run and resolve the error before retrying."
            },
        )?;
    } else {
        if let Some(run) = snapshot.run.as_deref() {
            let publication_failed = run.publication.as_deref().is_some_and(|handoff| {
                handoff.state == useful_api::RunPublicationHandoffState::Failed
            }) || snapshot.publication.as_deref().is_some_and(
                |publication| publication.state == useful_api::PublicationState::Failed,
            );
            super::write_run_human(
                deployment,
                if publication_failed {
                    "✗ Automatic publication failed."
                } else {
                    "✓ Run observed."
                },
                run,
            )?;
        } else {
            writeln!(
                io::stdout().lock(),
                "✓ Run {outcome}.\n\nrun: {}\norganization: {organization}\ndeployment: {deployment}",
                snapshot.run_id.as_deref().unwrap_or("not established")
            )?;
        }
        if let Some(publication) = snapshot.publication.as_deref() {
            writeln!(io::stdout().lock(), "\nautomatic publication:")?;
            writeln!(io::stdout().lock(), "  publication: {}", publication.id)?;
            writeln!(
                io::stdout().lock(),
                "  export: {}",
                useful_execution::visible_text(&publication.export_name)
            )?;
            writeln!(
                io::stdout().lock(),
                "  state: {}",
                super::enum_text(&publication.state)?
            )?;
            if let Some(branch) = publication.branch.as_deref() {
                writeln!(
                    io::stdout().lock(),
                    "  branch: {}",
                    super::enum_text(&branch.disposition)?
                )?;
            }
            if let Some(pull_request) = publication.pull_request.as_ref().and_then(Option::as_deref)
            {
                writeln!(
                    io::stdout().lock(),
                    "  pull request: {} ({}, {})",
                    pull_request.number,
                    super::enum_text(&pull_request.disposition)?,
                    super::enum_text(&pull_request.state)?
                )?;
                let url = super::super::publication::redacted_human_url(&pull_request.url)?;
                writeln!(
                    io::stdout().lock(),
                    "  pull request url: {}",
                    url.escape_default()
                )?;
            }
            if let Some(mr) = publication
                .merge_request
                .as_ref()
                .and_then(Option::as_deref)
            {
                writeln!(io::stdout().lock(), "  merge request: !{}", mr.iid)?;
                let url = super::super::publication::redacted_human_url(&mr.url)?;
                writeln!(
                    io::stdout().lock(),
                    "  merge request url: {}",
                    url.escape_default()
                )?;
            }
            if let Some(outcome) = publication.outcome {
                writeln!(
                    io::stdout().lock(),
                    "  outcome: {}",
                    super::enum_text(&outcome)?
                )?;
            }
            if let Some(failure) = publication.failure.as_deref() {
                writeln!(
                    io::stdout().lock(),
                    "  failure: {}",
                    super::enum_text(&failure.code)?
                )?;
                writeln!(
                    io::stdout().lock(),
                    "  phase: {}",
                    super::enum_text(&failure.phase)?
                )?;
                writeln!(io::stdout().lock(), "  retryable: {}", failure.retryable)?;
                if let Some(diagnostic) = failure.diagnostic.as_deref() {
                    super::super::write_publication_diagnostic(
                        &mut io::stdout().lock(),
                        diagnostic,
                        "    ",
                    )?;
                }
            }
        }
        if snapshot
            .publication
            .as_deref()
            .is_some_and(|publication| publication.state == PublicationState::Failed)
        {
            writeln!(
                io::stdout().lock(),
                "\nInspect the failed attempt with useful publication show. After resolving the failure, review provider effects before creating another publication for this Run and export."
            )?;
        } else if snapshot
            .run
            .as_deref()
            .and_then(|run| run.publication.as_deref())
            .is_some_and(|handoff| handoff.state == RunPublicationHandoffState::Failed)
        {
            writeln!(
                io::stdout().lock(),
                "\nInspect the failed automatic publication handoff. After resolving the failure, check for an existing attempt before using useful publication create for this Run and export."
            )?;
        }
        if let Some(replayed) = snapshot.replayed {
            writeln!(
                io::stdout().lock(),
                "replayed: {}",
                if replayed { "yes" } else { "no" }
            )?;
        }
        if let Some(receipt) = snapshot.cancellation_request.as_deref() {
            let mode = if receipt.mode == useful_api::RunCancellationReceiptMode::Force {
                "force"
            } else {
                "graceful"
            };
            write_cancel_receipt(&mut io::stdout().lock(), snapshot, receipt, mode)?;
        }
    }
    Ok(exit)
}

fn write_cancel_receipt(
    out: &mut impl Write,
    snapshot: &CloudSnapshot,
    receipt: &RunCancellationReceipt,
    mode: &str,
) -> anyhow::Result<()> {
    writeln!(out, "request: {}", receipt.id)?;
    writeln!(out, "requested mode: {mode}")?;
    writeln!(out, "request state: {}", super::enum_text(&receipt.state)?)?;
    writeln!(
        out,
        "effective mode: {}",
        snapshot
            .run
            .as_deref()
            .and_then(|run| run.cancellation.as_deref())
            .map(|cancellation| super::enum_text(&cancellation.mode))
            .transpose()?
            .as_deref()
            .unwrap_or("not applied")
    )?;
    Ok(())
}

fn write_cancel_context(
    out: &mut impl Write,
    snapshot: &CloudSnapshot,
    mode: Option<&str>,
) -> anyhow::Result<()> {
    if let Some(receipt) = snapshot.cancellation_request.as_deref() {
        let requested_mode = mode.unwrap_or_else(|| {
            if receipt.mode == useful_api::RunCancellationReceiptMode::Force {
                "force"
            } else {
                "graceful"
            }
        });
        write_cancel_receipt(out, snapshot, receipt, requested_mode)?;
    } else {
        if let Some(mode) = mode {
            writeln!(out, "requested mode: {mode}")?;
        }
        writeln!(out, "request: not observed")?;
    }
    if let Some(run) = snapshot.run.as_deref() {
        writeln!(out, "run state: {}", super::enum_text(&run.state)?)?;
        if let Some(interruption) = run.interruption.as_deref() {
            writeln!(
                out,
                "interruption phase: {}",
                super::enum_text(&interruption.phase)?
            )?;
            writeln!(
                out,
                "interruption cause: {}",
                super::enum_text(&interruption.cause)?
            )?;
            if let Some(fault) = interruption.executor_fault.as_ref() {
                writeln!(out, "executor fault: {}", super::enum_text(fault)?)?;
            }
            if let Some(reason) = interruption.recovery_reason.as_ref() {
                writeln!(out, "recovery reason: {}", super::enum_text(reason)?)?;
            }
            writeln!(
                out,
                "stop confirmed: {}",
                if interruption.stop_confirmed {
                    "yes"
                } else {
                    "no"
                }
            )?;
        }
        if let Some(delivery) = run.artifact_delivery.as_deref() {
            match delivery {
                RunArtifactDelivery::RunArtifactDeliverySucceeded(delivery) => {
                    writeln!(
                        out,
                        "artifact delivery: succeeded ({})",
                        delivery.artifact_set_id
                    )?;
                }
                RunArtifactDelivery::RunArtifactDeliveryRegistrationFailed(delivery) => {
                    write_delivery_failure(out, &delivery.phase, &delivery.code)?;
                }
                RunArtifactDelivery::RunArtifactDeliveryUploadFailed(delivery) => {
                    write_delivery_failure(out, &delivery.phase, &delivery.code)?;
                }
                RunArtifactDelivery::RunArtifactDeliveryPreparationFailed(delivery) => {
                    write_delivery_failure(out, &delivery.phase, &delivery.code)?;
                }
            }
        }
    } else {
        writeln!(out, "run state: not observed")?;
    }
    Ok(())
}

fn write_delivery_failure(
    out: &mut impl Write,
    phase: &impl Serialize,
    code: &impl Serialize,
) -> anyhow::Result<()> {
    writeln!(
        out,
        "artifact delivery: failed ({}: {})",
        super::enum_text(phase)?,
        super::enum_text(code)?
    )?;
    Ok(())
}

/// The lines naming a Cloud run's owner, or that it has none, the rule that routed it and
/// the pools it may use (REQ-RUN-101). A field a Cloud does not send prints no line.
pub(crate) fn write_route(out: &mut impl Write, run: &Run) -> io::Result<()> {
    match &run.owner {
        Some(Some(owner)) => writeln!(out, "owner: {}", visible_text(owner))?,
        Some(None) => writeln!(out, "owner: none")?,
        None => {}
    }
    match &run.routing {
        Some(Some(routing)) => {
            writeln!(
                out,
                "routing: {} · policy {}",
                visible_text(&routing.rule),
                routing.policy_version
            )?;
            let pools = routing
                .pools
                .iter()
                .map(|pool| match pool.name.as_deref() {
                    Some(name) => format!("{} ({})", visible_text(name), visible_text(&pool.id)),
                    None => visible_text(&pool.id),
                })
                .collect::<Vec<_>>();
            if pools.is_empty() {
                writeln!(out, "pools: none")?;
            } else {
                writeln!(out, "pools: {}", pools.join(", "))?;
            }
        }
        Some(None) => writeln!(out, "routing: none")?,
        None => {}
    }
    Ok(())
}

/// One line for each provider of a Cloud run's credential sources, where the runner took
/// its credential: the host, a grant of a member or a grant Cloud recorded no member of
/// (REQ-RUN-107). A run that records none prints no line.
pub(super) fn write_credential_sources(out: &mut impl Write, run: &Run) -> anyhow::Result<()> {
    let Some(Some(sources)) = &run.credential_sources else {
        return Ok(());
    };
    for entry in &sources.sources {
        let provider = visible_text(&super::enum_text(&entry.provider)?);
        let source = super::enum_text(&entry.source)?;
        match entry.principal_id.as_deref() {
            Some(principal) if source == "grant" => writeln!(
                out,
                "credential {provider}: grant of {}",
                visible_text(principal)
            )?,
            _ => writeln!(out, "credential {provider}: {}", visible_text(&source))?,
        }
    }
    Ok(())
}

/// One line for each integration a Cloud run's workflow declares, in the API's order, saying
/// where its credential came from: the host, the member who connected it, the organization
/// or a member Cloud names no principal of, or that nothing provided one, with the token
/// version where there is one (REQ-RUN-109). A run without integration sources prints no
/// line; a source this release has no words for prints as given.
pub(super) fn write_integration_sources(out: &mut impl Write, run: &Run) -> anyhow::Result<()> {
    let Some(entries) = &run.integration_sources else {
        return Ok(());
    };
    for entry in entries {
        let integration = visible_text(&entry.integration);
        let source = entry.source.as_ref().map(super::enum_text).transpose()?;
        let acted_as = entry.acted_as.as_ref().map(super::enum_text).transpose()?;
        let what = match (source.as_deref(), entry.connected_by.as_deref()) {
            (None, _) => "not provided".to_owned(),
            (Some("host"), _) => "host".to_owned(),
            (Some(_), Some(principal)) => format!("connected by {}", visible_text(principal)),
            (Some(source), None) => match acted_as.as_deref() {
                Some("org") => "connected by the organization".to_owned(),
                Some("member") => "connected by a member".to_owned(),
                _ => visible_text(source),
            },
        };
        match entry.token_version {
            Some(version) => writeln!(out, "integration {integration}: {what}, token v{version}")?,
            None => writeln!(out, "integration {integration}: {what}")?,
        }
    }
    Ok(())
}

/// The line saying what a queued run waits for, its reason in words and an unrecognized
/// reason as given (REQ-RUN-100, REQ-RUN-101); nothing when it waits for nothing.
pub(crate) fn write_waiting(out: &mut impl Write, observation: &RunObservation) -> io::Result<()> {
    let Some(Some(waiting)) = &observation.waiting else {
        return Ok(());
    };
    let reason = match waiting.reason.as_str() {
        "owner_inactive" => "owner is not an active member".to_owned(),
        "no_runner" => "no runner in its pools".to_owned(),
        "no_fitting_runner" => "no runner in its pools fits the run".to_owned(),
        "runners_offline" => "runners offline".to_owned(),
        "runners_busy" => "runners busy".to_owned(),
        other => visible_text(other),
    };
    match waiting.since.as_deref() {
        Some(since) => writeln!(out, "waiting: {reason} since {}", visible_text(since)),
        None => writeln!(out, "waiting: {reason}"),
    }
}

/// The other runs the run's runner holds, in Cloud's order, each with whether it is working
/// or waiting (REQ-RUN-120); nothing when the runner holds no other run or Cloud does not say.
pub(crate) fn write_held_with(
    out: &mut impl Write,
    observation: &RunObservation,
) -> io::Result<()> {
    let Some(held) = observation
        .held_with
        .as_deref()
        .filter(|held| !held.is_empty())
    else {
        return Ok(());
    };
    writeln!(out, "runner also holds:")?;
    for run in held {
        let state = match run.state {
            RunHeldRunState::Working => "working",
            RunHeldRunState::Waiting => "waiting",
        };
        writeln!(out, "  {}: {state}", visible_text(&run.run_id))?;
    }
    Ok(())
}

/// The block saying why Cloud rejected a run, nothing when it did not: its reason, under it
/// each validation fact its runner diagnostic holds, in the order code, location, path and
/// step (REQ-DIAG-29, REQ-DIAG-42), and its recovery reason when it has one. It prints no
/// other diagnostic member and escapes every value: a fact is untrusted runner text.
pub(crate) fn write_rejection(out: &mut impl Write, run: &Run) -> io::Result<()> {
    let Some(rejection) = run.rejection.as_deref() else {
        return Ok(());
    };
    writeln!(out, "rejection:")?;
    writeln!(out, "  reason: {}", visible_text(&rejection.reason))?;
    if let Some(diagnostic) = rejection.diagnostic.as_deref() {
        for (label, fact) in [
            ("validation code", &diagnostic.validation_code),
            ("validation location", &diagnostic.validation_location),
            ("validation path", &diagnostic.validation_path),
            ("validation step", &diagnostic.validation_step),
        ] {
            if let Some(fact) = fact {
                writeln!(out, "  {label}: {}", visible_text(fact))?;
            }
        }
    }
    if let Some(reason) = rejection.recovery_reason {
        let reason = useful_support::wire_name(reason).ok_or_else(|| {
            io::Error::other("Cloud run recovery reason is not a contracted string")
        })?;
        writeln!(out, "  recovery reason: {}", visible_text(&reason))?;
    }
    Ok(())
}

/// A runner-reported scalar as the run commands print it, normalized for the terminal so a
/// control character prints as an escape.
pub(crate) fn reported_text(value: &str) -> String {
    useful_execution::normalize_terminal_scalar(value.as_bytes())
}

/// The string at `key` of `object` as `reported_text` prints it, or `-` when it is absent,
/// null or not a string.
pub(crate) fn reported_field(object: &Value, key: &str) -> String {
    object[key]
        .as_str()
        .map_or_else(|| "-".to_owned(), reported_text)
}

/// A position or wait step path as its elements joined by `.`, an item by its number.
pub(crate) fn step_path(elements: &Value) -> String {
    elements.as_array().map_or_else(
        || "-".to_owned(),
        |elements| {
            elements
                .iter()
                .map(|element| match element {
                    Value::String(step) => reported_text(step),
                    other => other["item"]
                        .as_i64()
                        .map_or_else(|| "?".to_owned(), |item| item.to_string()),
                })
                .collect::<Vec<_>>()
                .join(".")
        },
    )
}

/// A workflow wait's wire document as `<kind> at <step> since <since>`, the one rendering
/// that `useful workflow run show --cloud`, `useful run show`, `useful run list` and the
/// `--wait` observation line share (REQ-RUN-88, REQ-RUN-116, REQ-RUN-113).
pub(crate) fn wait_phrase(wait: &Value) -> String {
    format!(
        "{} at {} since {}",
        reported_field(wait, "kind"),
        step_path(&wait["step"]),
        reported_field(wait, "since")
    )
}

/// A typed wait, a run read's or a listed run's, as `wait_phrase` renders its wire document.
pub(super) fn typed_wait_phrase(wait: &impl Serialize) -> io::Result<String> {
    serde_json::to_value(wait)
        .map(|wait| wait_phrase(&wait))
        .map_err(io::Error::other)
}

/// What a run read says its workflow waits for: `None` unless its workflow state is
/// `waiting`, and then the wait's phrase, or `None` within when its progress carries no wait.
pub(super) fn read_wait(run: &Run) -> io::Result<Option<Option<String>>> {
    let state = run
        .workflow_state
        .flatten()
        .and_then(useful_support::wire_name);
    if state.as_deref() != Some("waiting") {
        return Ok(None);
    }
    run.workflow_progress
        .as_ref()
        .and_then(Option::as_deref)
        .and_then(|progress| progress.wait.as_deref())
        .map(typed_wait_phrase)
        .transpose()
        .map(Some)
}

/// The line naming a run's workflow state, nothing when its read carries none, and under it
/// while it waits the wait's kind, step and since (REQ-RUN-116).
pub(super) fn write_workflow_state(out: &mut impl Write, run: &Run) -> io::Result<()> {
    let Some(state) = run.workflow_state.flatten() else {
        return Ok(());
    };
    let state = useful_support::wire_name(state)
        .ok_or_else(|| io::Error::other("Cloud run workflow state is not a contracted string"))?;
    writeln!(out, "workflow state: {state}")?;
    if let Some(Some(wait)) = read_wait(run)? {
        writeln!(out, "workflow wait: {wait}")?;
    }
    Ok(())
}
