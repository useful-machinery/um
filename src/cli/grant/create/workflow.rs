//! The providers `um grant create --workflow` signs in to (REQ-GRANT-76): those a workflow tree
//! calls for, derived as `um runner doctor --workflow` derives them, and the plan printed before
//! any sign-in.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use serde::Serialize;

use crate::cli::CommandFailure;
use crate::cli::runner::doctor::workflow::{
    self as doctor, JsonDeclaration, describe, group_label,
};
use crate::exit_code::ExitCode;
use um_execution::{CommandProbeError, CommandRunner, SystemCommandRunner};
use um_runner::{
    CredentialRequirement, Declaration, RepositoryRootError, RepositorySource, WorkflowCredentials,
};

/// The credentials the named workflows and the workflows their `workflow` steps name need, or
/// `None` once the first file that does not resolve was rejected as `um workflow validate`
/// rejects it. `source_root` bounds every file; without it each file is bounded by the root of
/// the Git repository that holds it, the files of one repository are derived together under its
/// root, and the derivations are merged by kind. A file whose `git rev-parse --show-toplevel`
/// fails is refused with exit 2 before any file is derived. Each derivation reads its root's
/// `origin` remote (REQ-CREDREADY-12) through the runner that asks for the roots.
pub(super) fn derive(
    workflows: &[PathBuf],
    source_root: Option<&Path>,
    json: bool,
) -> Result<Option<WorkflowCredentials>, CommandFailure> {
    let runner: Arc<dyn CommandRunner> = Arc::new(SystemCommandRunner);
    let groups = match source_root {
        Some(root) => vec![(root.to_owned(), workflows.to_vec())],
        None => by_repository(runner.as_ref(), workflows)?,
    };
    let mut merged = WorkflowCredentials {
        workflows: Vec::new(),
        requirements: Vec::new(),
        unknown: Vec::new(),
    };
    for (root, files) in groups {
        let source = RepositorySource::OriginRemote(Arc::clone(&runner));
        let Some(credentials) = doctor::derive(&root, &files, source, json)? else {
            return Ok(None);
        };
        merge(&mut merged, credentials);
    }
    Ok(Some(merged))
}

/// The files grouped by the root of the Git repository that holds each, in the order first
/// named. A file whose directory does not exist is not in one: it is grouped under the working
/// directory, whose derivation rejects it as `um workflow validate` does.
fn by_repository(
    runner: &dyn CommandRunner,
    workflows: &[PathBuf],
) -> Result<Vec<(PathBuf, Vec<PathBuf>)>, CommandFailure> {
    let mut groups: Vec<(PathBuf, Vec<PathBuf>)> = Vec::new();
    for file in workflows {
        let directory = file
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let root = if directory.is_dir() {
            um_runner::repository_root(runner, directory)
                .map_err(|error| refusal(file, directory, error))?
        } else {
            PathBuf::from(".")
        };
        match groups.iter_mut().find(|(known, _)| *known == root) {
            Some((_, files)) => files.push(file.clone()),
            None => groups.push((root, vec![file.clone()])),
        }
    }
    Ok(groups)
}

/// Why `file` has no repository root, and `--source-root` as the remedy. A failed
/// `git rev-parse --show-toplevel` is named as that command failing in `directory`, since its
/// exit status does not say why, and refused with exit 2.
fn refusal(file: &Path, directory: &Path, error: RepositoryRootError) -> CommandFailure {
    let (cause, code) = match error {
        RepositoryRootError::Probe(error) => {
            (probe_failure(error).to_owned(), ExitCode::GeneralFailure)
        }
        RepositoryRootError::Failed => (
            format!(
                "`git rev-parse --show-toplevel` failed in {}",
                directory.display()
            ),
            ExitCode::UsageError,
        ),
        RepositoryRootError::NoRoot => (
            "git printed no repository root".to_owned(),
            ExitCode::GeneralFailure,
        ),
    };
    CommandFailure::with_exit_code(
        anyhow!(
            "find the Git repository of {}: {cause}\n\n\
             Name the directory that bounds the workflow files:\n  \
             um grant create --workflow FILE --source-root ROOT",
            file.display()
        ),
        code,
    )
}

const fn probe_failure(error: CommandProbeError) -> &'static str {
    match error {
        CommandProbeError::CommandNotFound => "git was not found on PATH",
        CommandProbeError::Spawn => "start git",
        CommandProbeError::Timeout => "git timed out",
        CommandProbeError::Wait | CommandProbeError::PipeRead => "read git's answer",
    }
}

/// Merges one repository's derivation into the others': its files after theirs, its origins
/// into the requirement of their kind, and the unknown entries they lack.
fn merge(into: &mut WorkflowCredentials, from: WorkflowCredentials) {
    into.workflows.extend(from.workflows);
    for requirement in from.requirements {
        match into
            .requirements
            .iter_mut()
            .find(|known| known.kind == requirement.kind)
        {
            Some(known) => add(&mut known.origins, requirement.origins),
            None => into.requirements.push(requirement),
        }
    }
    into.requirements
        .sort_by_key(|requirement| requirement.kind);
    add(&mut into.unknown, from.unknown);
}

/// Adds the declarations `into` lacks, keeping those of no workflow file last, as a report
/// lists them.
fn add<K: PartialEq>(into: &mut Vec<Declaration<K>>, from: Vec<Declaration<K>>) {
    for entry in from {
        if !into.contains(&entry) {
            into.push(entry);
        }
    }
    into.sort_by_key(|entry| entry.workflow.is_none());
}

/// A provider the tree calls for: whether it was selected for a sign-in or passed over as held,
/// and the requirement whose origins call for it.
pub(super) struct Planned<'a> {
    pub(super) provider: &'static str,
    pub(super) selected: bool,
    pub(super) requirement: &'a CredentialRequirement,
}

/// Prints, before any sign-in, the workflow files covered, each provider selected and each
/// passed over as held with the origins that call for it, and what the declarations cannot
/// show: as the first JSON event, or as a report on standard output (REQ-GRANT-76).
pub(super) fn write_plan(
    deployment: &str,
    credentials: &WorkflowCredentials,
    planned: &[Planned<'_>],
    json: bool,
) -> anyhow::Result<()> {
    if json {
        let providers = |selected: bool| {
            planned
                .iter()
                .filter(|entry| entry.selected == selected)
                .map(|entry| JsonProvider {
                    provider: entry.provider,
                    origins: entry
                        .requirement
                        .origins
                        .iter()
                        .map(JsonDeclaration::from_declaration)
                        .collect(),
                })
                .collect()
        };
        return crate::cli::write_json_line(&JsonPlan {
            schema_version: 1,
            event: "workflow_requirements",
            deployment,
            workflows: &credentials.workflows,
            selected: providers(true),
            held: providers(false),
            unknown: credentials
                .unknown
                .iter()
                .map(JsonDeclaration::from_declaration)
                .collect(),
        })
        .context("write JSON grant event");
    }
    let mut stdout = io::stdout().lock();
    write_human_plan_to(&mut stdout, credentials, planned)?;
    stdout.flush()?;
    Ok(())
}

fn write_human_plan_to(
    output: &mut impl Write,
    credentials: &WorkflowCredentials,
    planned: &[Planned<'_>],
) -> io::Result<()> {
    for workflow in &credentials.workflows {
        writeln!(output, "workflow: {workflow}")?;
    }
    writeln!(output)?;
    for entry in planned {
        let state = if entry.selected { "sign in" } else { "held" };
        writeln!(output, "{state}: {}", entry.provider)?;
        for origin in &entry.requirement.origins {
            write_declaration(output, "  needed by", origin)?;
        }
    }
    if !planned.is_empty() {
        writeln!(output)?;
    }
    if !credentials.unknown.is_empty() {
        writeln!(
            output,
            "These declarations may use credentials the workflows do not name, and no sign-in is run for them:"
        )?;
        for entry in &credentials.unknown {
            write_declaration(output, "unknown", entry)?;
        }
        writeln!(output)?;
    }
    Ok(())
}

/// `<key>: <workflow file> · <node> · <declaration> [<name>]`, as the doctor names a declaration.
fn write_declaration<K: um_runner::DeclarationKind + doctor::HumanLabel>(
    output: &mut impl Write,
    key: &str,
    entry: &Declaration<K>,
) -> io::Result<()> {
    writeln!(
        output,
        "{key}: {} · {}",
        group_label(entry),
        describe(entry)
    )
}

/// The first event of `um grant create --workflow --json`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonPlan<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    workflows: &'a [String],
    selected: Vec<JsonProvider<'a>>,
    held: Vec<JsonProvider<'a>>,
    unknown: Vec<JsonDeclaration<'a>>,
}

#[derive(Serialize)]
struct JsonProvider<'a> {
    provider: &'static str,
    origins: Vec<JsonDeclaration<'a>>,
}
