//! What the `um workflow definition cloud` leaves print (REQ-CLOUDDEF-15): with `--json`, one
//! document naming the command, the organization and project, the `outcome` and the result,
//! or for a failure the failure name with a refusal's status, problem slug and `workflow`
//! safe facts; without it, a plain report on stdout or the failure on stderr.

use std::collections::BTreeMap;
use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;
use serde_json::Value;

use super::super::publish::cloud_failure;
use super::super::report::{display, display_members, write_json};
use crate::exit_code::ExitCode;
use um_api::{
    WorkflowDefinition, WorkflowDefinitionFailure, WorkflowDefinitionRevision,
    WorkflowProblemDetail,
};

/// The leaf a report is for and where it reached.
pub(super) struct Scope<'a> {
    pub(super) leaf: &'static str,
    pub(super) organization: &'a str,
    pub(super) project: &'a str,
    pub(super) json: bool,
}

impl Scope<'_> {
    fn command(&self) -> String {
        format!("um workflow definition cloud {}", self.leaf)
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
    let (written, exit_code) = match result {
        Ok(value) => (write(value), ExitCode::Success),
        Err(failure) => (failed(scope, failure), cloud_failure(failure).1),
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
        if let Some(id) = &resolved.id {
            write!(stdout, " · id {}", display(id))?;
        }
        if let Some(name) = &resolved.name {
            write!(stdout, " · name {}", display(name))?;
        }
        writeln!(stdout)?;
    }
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

fn failed(scope: &Scope<'_>, failure: &WorkflowDefinitionFailure) -> anyhow::Result<()> {
    let name = cloud_failure(failure).0;
    let refusal = match failure {
        WorkflowDefinitionFailure::Refused(refusal) => Some(refusal),
        _ => None,
    };
    if scope.json {
        return scope.write(
            name,
            FailedBody {
                status: refusal.map(|refusal| refusal.status),
                problem: refusal.map(|refusal| refusal.problem.as_str()),
                workflow: refusal.and_then(|refusal| refusal.workflow.as_ref()),
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
        if let Some(binding) = &detail.binding_key {
            writeln!(stderr, "binding: {}", display(binding))?;
        }
        if let Some(current) = detail.current_revision {
            writeln!(stderr, "current revision: {current}")?;
        }
    }
    writeln!(
        stderr,
        "\nInspect the definition and your access before trying again."
    )?;
    Ok(())
}

/// A wire enumeration value by its published name.
pub(super) fn wire(value: impl Serialize) -> String {
    um_support::wire_name(value).unwrap_or_default()
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
struct FailedBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    problem: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow: Option<&'a WorkflowProblemDetail>,
}
