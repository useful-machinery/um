use std::io::{self, Write};

use clap::{Args, Subcommand};
use serde::Serialize;

use crate::exit_code::ExitCode;
use um_api::{RunFailure, RunLinearLifecycle, RunLinearLifecycleUpdate};
use um_execution::visible_text;
use um_human_auth::Deployment;

pub(super) const ABOUT: &str = "Manage a run's Linear state updates";
const NAME: &str = "linear";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<LinearLifecycleCommand>,
}

#[derive(Debug, Subcommand)]
enum LinearLifecycleCommand {
    #[command(about = "List a run's Linear state updates, newest first")]
    History(HistoryCommand),
    #[command(about = "Show a run's Linear issue binding and latest state update")]
    Show(ShowCommand),
}

#[derive(Debug, Args)]
struct ShowCommand {
    #[command(flatten)]
    run: super::RunReference,
    #[command(flatten)]
    options: super::RunOptions,
}

#[derive(Debug, Args)]
struct HistoryCommand {
    #[command(flatten)]
    run: super::RunReference,
    #[command(flatten)]
    page: super::super::PaginationArgs<100>,
    #[command(flatten)]
    options: super::RunOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let path = &[super::NAME, NAME];
        let context = "configure Useful Machinery run access";
        match self.command {
            None => super::super::print_help(path),
            Some(LinearLifecycleCommand::History(command)) => {
                super::super::execute_deployment_command(
                    Some(command),
                    path,
                    context,
                    |command, deployment| command.execute(deployment.clone()),
                )
            }
            Some(LinearLifecycleCommand::Show(command)) => {
                super::super::execute_deployment_command(
                    Some(command),
                    path,
                    context,
                    |command, deployment| command.execute(deployment.clone()),
                )
            }
        }
    }
}

/// The run a lifecycle command names, as every envelope and diagnostic repeats it.
struct Target<'a> {
    deployment: &'a str,
    organization: &'a str,
    run_id: &'a str,
}

impl ShowCommand {
    fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        super::super::execute_read_only_with_signals(
            "Cloud run Linear lifecycle show",
            move |control| {
                let result = super::with_api(
                    &deployment,
                    self.options.http.transport_policy(),
                    &self.options.authentication,
                    |api| api.get_linear_lifecycle(&self.run.organization, &self.run.run_id),
                )?;
                super::super::complete_read_only_output(control, || {
                    let target = Target {
                        deployment: deployment.fingerprint().api_url(),
                        organization: &self.run.organization,
                        run_id: &self.run.run_id,
                    };
                    write_read(
                        &target,
                        "show",
                        result.as_ref(),
                        (self.options.authentication.kind(), self.options.json),
                        |view| ShowPayload {
                            linear_lifecycle: view,
                        },
                        |view| {
                            let mut stdout = io::stdout().lock();
                            write_view(&mut stdout, view, "")?;
                            writeln!(stdout, "deployment: {}", target.deployment)?;
                            Ok(())
                        },
                    )
                    .map_err(Into::into)
                })
            },
        )
    }
}

impl HistoryCommand {
    fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        super::super::execute_read_only_with_signals(
            "Cloud run Linear lifecycle history",
            move |control| {
                let result = super::with_api(
                    &deployment,
                    self.options.http.transport_policy(),
                    &self.options.authentication,
                    |api| {
                        api.list_linear_lifecycle_updates(
                            &self.run.organization,
                            &self.run.run_id,
                            self.page.limit.map(u32::from),
                            self.page.cursor.as_deref(),
                        )
                    },
                )?;
                super::super::complete_read_only_output(control, || {
                    let target = Target {
                        deployment: deployment.fingerprint().api_url(),
                        organization: &self.run.organization,
                        run_id: &self.run.run_id,
                    };
                    write_read(
                        &target,
                        "history",
                        result.as_ref(),
                        (self.options.authentication.kind(), self.options.json),
                        |page| HistoryPayload { updates: page },
                        |page| write_history(&page.items, page.next_cursor.as_deref()),
                    )
                    .map_err(Into::into)
                })
            },
        )
    }
}

/// The JSON envelope every lifecycle command writes, with its operation's
/// members between the outcome and the error.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LinearLifecycleResult<'a, P> {
    schema_version: u8,
    operation: &'static str,
    deployment: &'a str,
    organization_ref: &'a str,
    run_id: &'a str,
    outcome: &'static str,
    #[serde(flatten)]
    payload: P,
    error: Option<LinearLifecycleError>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShowPayload<'a> {
    linear_lifecycle: Option<&'a RunLinearLifecycle>,
}

#[derive(Serialize)]
struct HistoryPayload<'a, P> {
    updates: Option<&'a P>,
}

#[derive(Serialize)]
struct LinearLifecycleError {
    code: &'static str,
}

type Outcome = (&'static str, Option<LinearLifecycleError>, ExitCode);

fn write_envelope(
    target: &Target<'_>,
    operation: &'static str,
    outcome: &'static str,
    payload: impl Serialize,
    error: Option<LinearLifecycleError>,
) -> anyhow::Result<()> {
    super::write_json(&LinearLifecycleResult {
        schema_version: 1,
        operation,
        deployment: target.deployment,
        organization_ref: target.organization,
        run_id: target.run_id,
        outcome,
        payload,
        error,
    })
}

/// Writes a read's envelope, or its value as `human` prints it, or its failure.
fn write_read<'v, V, P: Serialize>(
    target: &Target<'_>,
    operation: &'static str,
    result: Result<&'v V, &RunFailure>,
    (authentication, json): (super::super::PrincipalAuthenticationKind, bool),
    payload: impl FnOnce(Option<&'v V>) -> P,
    human: impl FnOnce(&V) -> anyhow::Result<()>,
) -> anyhow::Result<ExitCode> {
    let (outcome, error, exit) = match result {
        Ok(_) => ("found", None, ExitCode::Success),
        Err(failure) => failure_outcome(failure, authentication),
    };
    if json {
        write_envelope(target, operation, outcome, payload(result.ok()), error)?;
    } else if let Ok(value) = result {
        human(value)?;
    } else if let Some(error) = error {
        write_failure(target, operation, error.code, &[])?;
    }
    Ok(exit)
}

fn failure_outcome(
    failure: &RunFailure,
    authentication: super::super::PrincipalAuthenticationKind,
) -> Outcome {
    let (code, exit) = super::failure_code(failure, authentication);
    let outcome = match failure {
        RunFailure::NotFound => "not_found",
        RunFailure::Conflict => "conflict",
        _ => "error",
    };
    (outcome, Some(LinearLifecycleError { code }), exit)
}

fn write_failure(
    target: &Target<'_>,
    operation: &str,
    code: &str,
    details: &[(&str, &str)],
) -> anyhow::Result<()> {
    let mut stderr = io::stderr().lock();
    writeln!(
        stderr,
        "error: Cloud run Linear lifecycle {operation}: {code}\n\nrun: {}\norganization: {}",
        target.run_id, target.organization
    )?;
    for (label, value) in details {
        writeln!(stderr, "{label}: {value}")?;
    }
    Ok(())
}

/// Writes each update as `show` writes the latest one, newest first with a blank
/// line between updates, then the cursor of a page that is not the last.
fn write_history(
    updates: &[RunLinearLifecycleUpdate],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    for (index, update) in updates.iter().enumerate() {
        if index > 0 {
            writeln!(stdout)?;
        }
        write_update(&mut stdout, update, "")?;
    }
    if let Some(cursor) = next_cursor {
        if !updates.is_empty() {
            writeln!(stdout)?;
        }
        writeln!(stdout, "next cursor: {cursor}")?;
    }
    Ok(())
}

/// Writes one `key: value` line per present member, in the view's contract order.
pub(super) fn write_view(
    out: &mut impl Write,
    view: &RunLinearLifecycle,
    indent: &str,
) -> anyhow::Result<()> {
    writeln!(out, "{indent}trigger: {}", view.trigger_id)?;
    writeln!(
        out,
        "{indent}configuration version: {}",
        view.configuration_version
    )?;
    writeln!(out, "{indent}connection: {}", view.connection_id)?;
    writeln!(
        out,
        "{indent}workspace: {}",
        visible_text(&view.workspace_id)
    )?;
    writeln!(out, "{indent}issue: {}", visible_text(&view.issue_id))?;
    writeln!(
        out,
        "{indent}ownership state: {}",
        super::enum_text(&view.ownership_state)?
    )?;
    if let Some(attempt_id) = &view.attempt_id {
        writeln!(out, "{indent}attempt: {attempt_id}")?;
    }
    if let Some(attempt_number) = view.attempt_number {
        writeln!(out, "{indent}attempt number: {attempt_number}")?;
    }
    if let Some(event) = view.ownership_event.as_deref() {
        let node = match event.node.as_deref() {
            Some(node) => format!(", node {}", node_text(&node.scope, &node.role, &node.id)?),
            None => String::new(),
        };
        writeln!(
            out,
            "{indent}ownership event: {} ({}, sequence {}, occurred {}, attempt {} number {}{node})",
            visible_text(&event.event_id),
            super::enum_text(&event.event_type)?,
            event.sequence,
            event.occurred_at,
            event.attempt_id,
            event.attempt_number,
        )?;
    }
    if let Some(run_id) = &view.controlling_run_id {
        writeln!(out, "{indent}controlling run: {run_id}")?;
    }
    if let Some(code) = &view.reason_code {
        writeln!(out, "{indent}reason code: {}", super::enum_text(code)?)?;
    }
    if let Some(update) = view.latest_update.as_deref() {
        writeln!(out, "{indent}latest update:")?;
        write_update(out, update, &format!("{indent}  "))?;
    }
    Ok(())
}

/// Writes one `key: value` line per present member of an update.
fn write_update(
    out: &mut impl Write,
    update: &RunLinearLifecycleUpdate,
    indent: &str,
) -> anyhow::Result<()> {
    writeln!(out, "{indent}id: {}", update.id)?;
    writeln!(out, "{indent}attempt: {}", update.attempt_id)?;
    writeln!(out, "{indent}attempt number: {}", update.attempt_number)?;
    writeln!(out, "{indent}event: {}", visible_text(&update.event_id))?;
    writeln!(
        out,
        "{indent}event type: {}",
        super::enum_text(&update.event_type)?
    )?;
    writeln!(out, "{indent}event sequence: {}", update.event_sequence)?;
    writeln!(out, "{indent}event occurred: {}", update.event_occurred_at)?;
    if let Some(node) = update.node.as_deref() {
        writeln!(
            out,
            "{indent}node: {}",
            node_text(&node.scope, &node.role, &node.id)?
        )?;
    }
    writeln!(out, "{indent}created: {}", update.created_at)?;
    writeln!(out, "{indent}updated: {}", update.updated_at)?;
    writeln!(out, "{indent}state: {}", super::enum_text(&update.state)?)?;
    if let Some(code) = &update.reason_code {
        writeln!(out, "{indent}reason code: {}", super::enum_text(code)?)?;
    }
    writeln!(out, "{indent}cycle number: {}", update.cycle_number)?;
    writeln!(out, "{indent}cycle deadline: {}", update.cycle_deadline)?;
    writeln!(out, "{indent}request count: {}", update.request_count)?;
    writeln!(out, "{indent}retryable: {}", update.retryable)?;
    for (label, state_id) in [
        ("desired state", &update.desired_state_id),
        ("observed state", &update.observed_state_id),
        ("confirmed state", &update.confirmed_state_id),
    ] {
        if let Some(state_id) = state_id {
            writeln!(out, "{indent}{label}: {}", visible_text(state_id))?;
        }
    }
    if let Some(next_attempt_at) = &update.next_attempt_at {
        writeln!(out, "{indent}next attempt: {next_attempt_at}")?;
    }
    if let Some(blocking) = update.blocking_update.as_deref() {
        writeln!(
            out,
            "{indent}blocking update: {} (run {})",
            blocking.update_id, blocking.run_id
        )?;
    }
    if let Some(facts) = update.diagnostic.as_deref() {
        let facts = [
            facts
                .cause
                .as_deref()
                .map(|cause| format!("cause {}", visible_text(cause))),
            facts
                .provider_status
                .map(|status| format!("provider status {status}")),
            facts
                .transport_error
                .as_deref()
                .map(|error| format!("transport error {}", visible_text(error))),
            facts
                .retry_after
                .as_deref()
                .map(|at| format!("retry after {at}")),
        ];
        let facts: Vec<String> = facts.into_iter().flatten().collect();
        writeln!(out, "{indent}diagnostic: {}", facts.join(", "))?;
    }
    Ok(())
}

/// A step node as its role and slash-joined scope path.
fn node_text(scope: &[String], role: &impl Serialize, id: &str) -> anyhow::Result<String> {
    let path = scope.iter().map(String::as_str).chain([id]);
    Ok(format!(
        "{} {}",
        super::enum_text(role)?,
        visible_text(&path.collect::<Vec<_>>().join("/"))
    ))
}
