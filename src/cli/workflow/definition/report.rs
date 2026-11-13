//! Reports shared by the `um workflow definition` leaves: an invalid tree exactly as
//! `validate` prints it (REQ-WFDEF-15) and a refused definition command (REQ-WFDEF-27).

use std::io::{self, Write};
use std::path::Path;

use anyhow::Context;
use serde::Serialize;

use um_execution::{
    DefinitionError, RejectionLocation, TreeDiagnostic, TreeLocation, normalize_terminal_scalar,
};

pub(super) fn display(value: &str) -> String {
    normalize_terminal_scalar(value.as_bytes())
}

/// A `members` value as `["a", "b"]`, each member normalized as [`display`] does.
pub(super) fn display_members(members: &[String]) -> String {
    let members = members
        .iter()
        .map(|member| format!("\"{}\"", display(member)))
        .collect::<Vec<_>>();
    format!("[{}]", members.join(", "))
}

pub(super) fn display_path(path: &Path) -> String {
    normalize_terminal_scalar(path.as_os_str().as_encoded_bytes())
}

pub(super) fn write_json(report: &impl Serialize, what: &'static str) -> anyhow::Result<()> {
    super::super::super::write_pretty_json(report).with_context(|| format!("write {what}"))
}

#[derive(Serialize)]
pub(super) struct JsonTree {
    pub(super) path: String,
}

impl JsonTree {
    pub(super) fn new(directory: &Path) -> Self {
        Self {
            path: directory.to_string_lossy().into_owned(),
        }
    }
}

/// `outcome` `invalid`: every failure found in the tree, as `validate --json` writes it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InvalidReport<'a> {
    schema_version: u8,
    command: &'static str,
    tree: JsonTree,
    outcome: &'static str,
    diagnostics: Vec<JsonDiagnostic<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonDiagnostic<'a> {
    code: &'static str,
    location: &'a TreeLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_location: Option<RejectionLocation<'a>>,
}

fn invalid_report<'a>(
    command: &'static str,
    directory: &Path,
    diagnostics: &'a [TreeDiagnostic],
) -> InvalidReport<'a> {
    InvalidReport {
        schema_version: 1,
        command,
        tree: JsonTree::new(directory),
        outcome: "invalid",
        diagnostics: diagnostics
            .iter()
            .map(|diagnostic| JsonDiagnostic {
                code: diagnostic.code,
                location: &diagnostic.location,
                workflow_location: diagnostic.workflow_location(),
            })
            .collect(),
    }
}

fn write_human_invalid(directory: &Path, diagnostics: &[TreeDiagnostic]) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✗ Workflow tree is invalid.")?;
    writeln!(stdout, "tree: {}", display_path(directory))?;
    for diagnostic in diagnostics {
        write!(
            stdout,
            "code: {}  location: {}",
            diagnostic.code, diagnostic.location
        )?;
        if let Some(location) = diagnostic.workflow_location() {
            write!(stdout, "; {location}")?;
        }
        writeln!(stdout)?;
    }
    writeln!(stdout, "No workflow steps were executed.")?;
    for remedy in diagnostics.iter().filter_map(TreeDiagnostic::remedy) {
        writeln!(stdout)?;
        writeln!(stdout, "{remedy}")?;
    }
    Ok(())
}

/// Writes the invalid-tree report of `command` in the form `json` selects.
pub(super) fn write_invalid(
    command: &'static str,
    directory: &Path,
    diagnostics: &[TreeDiagnostic],
    json: bool,
) -> anyhow::Result<()> {
    if json {
        write_json(
            &invalid_report(command, directory, diagnostics),
            "workflow tree validation result",
        )
    } else {
        write_human_invalid(directory, diagnostics)
    }
}

/// `outcome` `refused`: the one stable code and what it concerns, never file contents.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RefusedReport<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    diagnostics: [RefusalDiagnostic<'a>; 1],
}

#[derive(Serialize)]
struct RefusalDiagnostic<'a> {
    code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    definition: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    definitions: Option<&'a [String]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    binding: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
}

impl<'a> RefusalDiagnostic<'a> {
    fn new(error: &'a DefinitionError) -> Self {
        Self {
            code: error.code(),
            definition: error.id(),
            definitions: Some(error.ids()).filter(|ids| !ids.is_empty()),
            binding: error.key(),
            path: error.path().map(|path| path.to_string_lossy().into_owned()),
        }
    }
}

fn refused_report<'a>(command: &'static str, error: &'a DefinitionError) -> RefusedReport<'a> {
    RefusedReport {
        schema_version: 1,
        command,
        outcome: "refused",
        diagnostics: [RefusalDiagnostic::new(error)],
    }
}

/// `✗ <summary>` then `code: <code>` and what it concerns on one line.
fn write_human_refused(summary: &str, error: &DefinitionError) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✗ {summary}")?;
    write!(stdout, "code: {}", error.code())?;
    if let Some(id) = error.id() {
        write!(stdout, "  definition: {}", display(id))?;
    }
    if !error.ids().is_empty() {
        let ids = error.ids().iter().map(|id| display(id)).collect::<Vec<_>>();
        write!(stdout, "  definitions: {}", ids.join(", "))?;
    }
    if let Some(key) = error.key() {
        write!(stdout, "  binding: {}", display(key))?;
    }
    if let Some(path) = error.path() {
        write!(stdout, "  path: {}", display_path(path))?;
    }
    writeln!(stdout)?;
    Ok(())
}

/// Writes the refusal of `command` in the form `json` selects; `summary` heads the human
/// form, such as `Publish refused.`.
pub(super) fn write_refused(
    command: &'static str,
    summary: &str,
    error: &DefinitionError,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        write_json(
            &refused_report(command, error),
            "workflow definition refusal",
        )
    } else {
        write_human_refused(summary, error)
    }
}
