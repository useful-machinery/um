//! What the `useful workflow definition cloud` leaves print (REQ-CLOUDDEF-15): with `--json`, one
//! document naming the command, the organization and project, the `outcome` and the result,
//! or for a failure the failure name with a refusal's status, problem slug and `workflow`
//! safe facts or an invalid response's `diagnostic` (REQ-CLOUDDEF-28); without it, a plain
//! report on stdout or the failure on stderr with the same facts as labelled lines.

use std::collections::BTreeMap;
use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;
use serde_json::Value;

use super::super::publish::{
    InvalidResponseDiagnostic, cloud_failure, invalid_response_diagnostic,
};
use super::super::report::{display, display_members, write_json};
use crate::exit_code::ExitCode;
use useful_api::{
    WorkflowDefinition, WorkflowDefinitionFailure, WorkflowDefinitionRevision,
    WorkflowProblemDetail,
};

/// The problem slug Cloud refuses a delete of a definition that is still active with.
const DEFINITION_NOT_ARCHIVED: &str = "definition-not-archived";

/// The leaf a report is for and where it reached.
pub(super) struct Scope<'a> {
    pub(super) leaf: &'static str,
    pub(super) organization: &'a str,
    pub(super) project: &'a str,
    pub(super) json: bool,
}

impl Scope<'_> {
    fn command(&self) -> String {
        format!("useful workflow definition cloud {}", self.leaf)
    }

    pub(super) fn write<B: Serialize>(&self, outcome: &str, body: B) -> anyhow::Result<()> {
        write_json(
            &Envelope {
                schema_version: 1,
                command: self.command(),
                organization_ref: self.organization,
                project_id: self.project,
                outcome,
                body,
            },
            "workflow definition cloud result",
        )
    }
}

/// What `show` read: the definition, its current revision and its newest revisions, with
/// whether older ones remain unread.
pub(super) struct Shown {
    pub(super) definition: WorkflowDefinition,
    pub(super) current: WorkflowDefinitionRevision,
    pub(super) revisions: Vec<WorkflowDefinitionRevision>,
    pub(super) more: bool,
}

/// Writes `result` with `write`, exit 0, or its failure with the failure's exit status.
pub(super) fn finish<T>(
    scope: &Scope<'_>,
    result: Result<&T, &WorkflowDefinitionFailure>,
    write: impl FnOnce(&T) -> anyhow::Result<()>,
) -> super::super::super::super::CommandResult {
    finish_for(scope, None, result, write)
}

/// Writes the delete of `definition` as [`finish`] writes a result: `deleted`, or the failure,
/// a refusal `definition-not-archived` saying to archive the definition first.
pub(super) fn finish_delete(
    scope: &Scope<'_>,
    definition: &str,
    result: Result<&(), &WorkflowDefinitionFailure>,
) -> super::super::super::super::CommandResult {
    finish_for(scope, Some(definition), result, |()| {
        deleted(scope, definition)
    })
}

/// As [`finish`], for the leaf addressed to `definition` when it is given.
fn finish_for<T>(
    scope: &Scope<'_>,
    definition: Option<&str>,
    result: Result<&T, &WorkflowDefinitionFailure>,
    write: impl FnOnce(&T) -> anyhow::Result<()>,
) -> super::super::super::super::CommandResult {
    let (written, exit_code) = match result {
        Ok(value) => (write(value), ExitCode::Success),
        Err(failure) => (failed(scope, definition, failure), cloud_failure(failure).1),
    };
    written.with_context(|| format!("write {} result", scope.command()))?;
    Ok(exit_code)
}

pub(super) fn listed(
    scope: &Scope<'_>,
    definitions: &[WorkflowDefinition],
    next_cursor: Option<&str>,
) -> anyhow::Result<()> {
    if scope.json {
        return scope.write(
            "listed",
            ListedBody {
                definitions,
                next_cursor,
            },
        );
    }
    let mut stdout = io::stdout().lock();
    if definitions.is_empty() {
        writeln!(stdout, "No Cloud workflow definitions.")?;
    }
    for definition in definitions {
        writeln!(
            stdout,
            "{} · {} · {} · automatic starts {} · current {}",
            display(&definition.id),
            definition
                .name
                .as_deref()
                .map_or_else(|| "-".to_owned(), display),
            wire(definition.state),
            if definition.automatic_starts {
                "on"
            } else {
                "off"
            },
            definition.current_revision
        )?;
    }
    if let Some(cursor) = next_cursor {
        writeln!(stdout, "next cursor: {}", display(cursor))?;
    }
    Ok(())
}

pub(super) fn shown(scope: &Scope<'_>, shown: &Shown) -> anyhow::Result<()> {
    if scope.json {
        return scope.write(
            "shown",
            ShownBody {
                definition: &shown.definition,
                revision: &shown.current,
                revisions: &shown.revisions,
                more_revisions: shown.more,
            },
        );
    }
    let mut stdout = io::stdout().lock();
    write_definition(&mut stdout, &shown.definition)?;
    let current = &shown.current;
    writeln!(stdout, "tree: {}", display(&current.tree_digest))?;
    writeln!(stdout, "published at: {}", display(&current.published_at))?;
    writeln!(stdout, "by: {}", display(&current.published_by))?;
    writeln!(stdout, "── resolution ──")?;
    for (key, resolved) in current.resolution.iter().collect::<BTreeMap<_, _>>() {
        write!(
            stdout,
            "{}: {} ({})",
            display(key),
            binding_value(resolved.value.as_ref()),
            wire(resolved.kind)
        )?;
        end_resolved(
            &mut stdout,
            resolved.id.as_deref(),
            resolved.name.as_deref(),
        )?;
    }
    write_integration_resolution(&mut stdout, current)?;
    writeln!(stdout, "── revisions ──")?;
    for revision in &shown.revisions {
        writeln!(
            stdout,
            "{}{} · tree {} · published at {} · by {}",
            revision.number,
            if revision.number == shown.definition.current_revision {
                " (current)"
            } else {
                ""
            },
            display(&revision.tree_digest),
            display(&revision.published_at),
            display(&revision.published_by)
        )?;
    }
    if shown.more {
        writeln!(stdout, "older revisions: not shown")?;
    }
    Ok(())
}

/// Each integration's resolution in id order (REQ-CLOUDDEF-15): its account under its id, then
/// each binding as `<id>.<name>` in the line form of a flat one; nothing for a revision that
/// holds none.
fn write_integration_resolution(
    out: &mut impl Write,
    revision: &WorkflowDefinitionRevision,
) -> io::Result<()> {
    let Some(integrations) = &revision.integration_resolution else {
        return Ok(());
    };
    for (integration, resolution) in integrations.iter().collect::<BTreeMap<_, _>>() {
        if let Some(account) = &resolution.account {
            write!(out, "{}: account", display(integration))?;
            end_resolved(out, Some(&account.id), account.name.as_deref())?;
        }
        let bindings = resolution.bindings.iter().flatten();
        for (name, resolved) in bindings.collect::<BTreeMap<_, _>>() {
            write!(
                out,
                "{}.{}: {} ({})",
                display(integration),
                display(name),
                display(&resolved.value),
                wire(resolved.kind)
            )?;
            end_resolved(out, resolved.id.as_deref(), resolved.name.as_deref())?;
        }
    }
    Ok(())
}

/// Ends a resolution line with the resolved id and display name Cloud recorded, if any.
fn end_resolved(out: &mut impl Write, id: Option<&str>, name: Option<&str>) -> io::Result<()> {
    if let Some(id) = id {
        write!(out, " · id {}", display(id))?;
    }
    if let Some(name) = name {
        write!(out, " · name {}", display(name))?;
    }
    writeln!(out)
}

pub(super) fn changed(
    scope: &Scope<'_>,
    outcome: &'static str,
    definition: &WorkflowDefinition,
) -> anyhow::Result<()> {
    if scope.json {
        return scope.write(outcome, ChangedBody { definition });
    }
    let mut stdout = io::stdout().lock();
    writeln!(
        stdout,
        "✓ Cloud workflow definition {} {}.",
        display(&definition.id),
        outcome.replace('_', " ")
    )?;
    write_definition(&mut stdout, definition)?;
    Ok(())
}

/// The delete of `definition` (REQ-CLOUDDEF-40): with `--json` the group's document with its
/// `definitionId`, without it one line naming the id.
fn deleted(scope: &Scope<'_>, definition: &str) -> anyhow::Result<()> {
    if scope.json {
        return scope.write(
            "deleted",
            DeletedBody {
                definition_id: definition,
            },
        );
    }
    writeln!(
        io::stdout().lock(),
        "✓ Cloud workflow definition {} deleted.",
        display(definition)
    )?;
    Ok(())
}

fn write_definition(out: &mut impl Write, definition: &WorkflowDefinition) -> io::Result<()> {
    writeln!(out, "definition: {}", display(&definition.id))?;
    writeln!(
        out,
        "name: {}",
        definition
            .name
            .as_deref()
            .map_or_else(|| "none".to_owned(), display)
    )?;
    writeln!(out, "project: {}", display(&definition.project_id))?;
    writeln!(out, "state: {}", wire(definition.state))?;
    writeln!(
        out,
        "automatic starts: {}",
        super::super::yes_no(definition.automatic_starts)
    )?;
    writeln!(
        out,
        "execution principal: {}",
        definition
            .execution_principal_id
            .as_deref()
            .map_or_else(|| "none".to_owned(), display)
    )?;
    writeln!(out, "current: {}", definition.current_revision)
}

/// Writes `failure`; `definition` is the definition the leaf addressed, given for a delete,
/// whose refusal `definition-not-archived` ends with the archive to run first.
fn failed(
    scope: &Scope<'_>,
    definition: Option<&str>,
    failure: &WorkflowDefinitionFailure,
) -> anyhow::Result<()> {
    let name = cloud_failure(failure).0;
    let refusal = match failure {
        WorkflowDefinitionFailure::Refused(refusal) => Some(refusal),
        _ => None,
    };
    let diagnostic = invalid_response_diagnostic(failure);
    if scope.json {
        return scope.write(
            name,
            FailedBody {
                status: refusal.map(|refusal| refusal.status),
                problem: refusal.map(|refusal| refusal.problem.as_str()),
                workflow: refusal.and_then(|refusal| refusal.workflow.as_ref()),
                diagnostic,
            },
        );
    }
    let mut stderr = io::stderr().lock();
    let Some(refusal) = refusal else {
        writeln!(
            stderr,
            "error: Cloud workflow definition {} failed: {name}",
            scope.leaf
        )?;
        if let Some(diagnostic) = diagnostic {
            diagnostic.write(&mut stderr)?;
        }
        writeln!(
            stderr,
            "\nCheck your session and the deployment, then try again."
        )?;
        return Ok(());
    };
    writeln!(
        stderr,
        "error: Cloud workflow definition {} refused: {} (HTTP {})",
        scope.leaf, refusal.problem, refusal.status
    )?;
    if let Some(detail) = &refusal.workflow {
        if let Some(check) = detail.start_check {
            writeln!(stderr, "start check: {}", wire(check))?;
        }
        if let Some(trigger) = &detail.trigger_key {
            writeln!(stderr, "trigger: {}", display(trigger))?;
        }
        if let Some(integration) = &detail.integration {
            writeln!(stderr, "integration: {}", display(integration))?;
        }
        if let Some(binding) = &detail.binding_key {
            writeln!(stderr, "binding: {}", display(binding))?;
        }
        if let Some(current) = detail.current_revision {
            writeln!(stderr, "current revision: {current}")?;
        }
    }
    if let Some(definition) = definition
        && refusal.problem == DEFINITION_NOT_ARCHIVED
    {
        writeln!(
            stderr,
            "\nArchive the definition before deleting it:\n  useful workflow definition cloud archive {} {} --project-id {}",
            display(scope.organization),
            display(definition),
            display(scope.project)
        )?;
        return Ok(());
    }
    writeln!(
        stderr,
        "\nInspect the definition and your access before trying again."
    )?;
    Ok(())
}

/// A wire enumeration value by its published name.
pub(super) fn wire(value: impl Serialize) -> String {
    useful_support::wire_name(value).unwrap_or_default()
}

/// A bound value as `show` prints it: a string, or a `members` array as `["a", "b"]`.
fn binding_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => display(text),
        Some(Value::Array(items)) => {
            let members = items
                .iter()
                .map(|item| {
                    item.as_str()
                        .map_or_else(|| item.to_string(), str::to_owned)
                })
                .collect::<Vec<_>>();
            display_members(&members)
        }
        Some(other) => display(&other.to_string()),
        None => "none".to_owned(),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<'a, B: Serialize> {
    schema_version: u8,
    command: String,
    organization_ref: &'a str,
    project_id: &'a str,
    outcome: &'a str,
    #[serde(flatten)]
    body: B,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListedBody<'a> {
    definitions: &'a [WorkflowDefinition],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShownBody<'a> {
    definition: &'a WorkflowDefinition,
    revision: &'a WorkflowDefinitionRevision,
    revisions: &'a [WorkflowDefinitionRevision],
    more_revisions: bool,
}

#[derive(Serialize)]
struct ChangedBody<'a> {
    definition: &'a WorkflowDefinition,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeletedBody<'a> {
    definition_id: &'a str,
}

#[derive(Serialize)]
struct FailedBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    problem: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow: Option<&'a WorkflowProblemDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostic: Option<InvalidResponseDiagnostic>,
}
