//! The roots that bound the files of `um runner doctor --workflow` and
//! `um grant create --workflow` (REQ-CREDREADY-1, REQ-GRANT-76): `--source-root`, or the root of
//! the Git repository that holds each file, with each root's derivation merged into one.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::anyhow;

use crate::cli::CommandFailure;
use crate::exit_code::ExitCode;
use um_execution::{CommandProbeError, CommandRunner, SystemCommandRunner};
use um_runner::{Declaration, RepositoryRootError, RepositorySource, WorkflowCredentials};

/// The named workflow files grouped by the root that bounds them, in the order first named.
pub(in crate::cli) struct Roots {
    /// The runner that asked Git for the roots, which also asks for each root's `origin` remote.
    runner: Arc<dyn CommandRunner>,
    groups: Vec<(PathBuf, Vec<PathBuf>)>,
}

impl Roots {
    /// `source_root` bounds every file; without it each file is bounded by the root of the Git
    /// repository that holds it. A file whose `git rev-parse --show-toplevel` fails is refused
    /// with exit 2, before any file is derived, naming `remedy`: the command line that names
    /// the boundary instead.
    pub(in crate::cli) fn find(
        workflows: &[PathBuf],
        source_root: Option<&Path>,
        remedy: &str,
    ) -> Result<Self, CommandFailure> {
        let runner: Arc<dyn CommandRunner> = Arc::new(SystemCommandRunner);
        let groups = match source_root {
            Some(root) => vec![(root.to_owned(), workflows.to_vec())],
            None => by_repository(runner.as_ref(), workflows, remedy)?,
        };
        Ok(Self { runner, groups })
    }

    /// Each root's `origin` remote, asked of Git through the runner that asked for the roots
    /// (REQ-CREDREADY-12).
    pub(in crate::cli) fn origin_remote(&self) -> RepositorySource {
        RepositorySource::OriginRemote(Arc::clone(&self.runner))
    }

    /// The credentials the named workflows and the workflows their `workflow` steps name need,
    /// or `None` once the first file that does not resolve was rejected as
    /// `um workflow validate` rejects it. The files of one root are derived together under it,
    /// each reading its repository's provider from `source`, and the derivations are merged by
    /// kind.
    pub(in crate::cli) fn derive(
        &self,
        source: &RepositorySource,
        json: bool,
    ) -> anyhow::Result<Option<WorkflowCredentials>> {
        let mut merged: Option<WorkflowCredentials> = None;
        for (root, files) in &self.groups {
            let Some(credentials) = super::workflow::derive(root, files, again(source), json)?
            else {
                return Ok(None);
            };
            match &mut merged {
                Some(into) => merge(into, credentials),
                None => merged = Some(credentials),
            }
        }
        Ok(Some(merged.unwrap_or(WorkflowCredentials {
            workflows: Vec::new(),
            requirements: Vec::new(),
            unknown: Vec::new(),
        })))
    }
}

/// `source` for one more root.
fn again(source: &RepositorySource) -> RepositorySource {
    match source {
        RepositorySource::OriginRemote(runner) => {
            RepositorySource::OriginRemote(Arc::clone(runner))
        }
        RepositorySource::Project(kind) => RepositorySource::Project(*kind),
    }
}

/// The files grouped by the root of the Git repository that holds each, in the order first
/// named. A file whose directory does not exist is not in one: it is grouped under the working
/// directory, whose derivation rejects it as `um workflow validate` does.
fn by_repository(
    runner: &dyn CommandRunner,
    workflows: &[PathBuf],
    remedy: &str,
) -> Result<Vec<(PathBuf, Vec<PathBuf>)>, CommandFailure> {
    let mut groups: Vec<(PathBuf, Vec<PathBuf>)> = Vec::new();
    for file in workflows {
        let directory = file
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let root = if directory.is_dir() {
            um_runner::repository_root(runner, directory)
                .map_err(|error| refusal(file, directory, error, remedy))?
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

/// Why `file` has no repository root, and `remedy` as the way out. A failed
/// `git rev-parse --show-toplevel` is named as that command failing in `directory`, since its
/// exit status does not say why, and refused with exit 2.
fn refusal(
    file: &Path,
    directory: &Path,
    error: RepositoryRootError,
    remedy: &str,
) -> CommandFailure {
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
             {remedy}",
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
