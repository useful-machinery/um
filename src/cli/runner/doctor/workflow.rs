//! The workflow mode of `um runner doctor` (REQ-CREDREADY-1 to REQ-CREDREADY-21): the
//! credentials a workflow tree declares, with what Cloud holds for the organization and a named
//! project when `--readiness` asks, printed as a checklist or a JSON document.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use serde::Serialize;

use crate::exit_code::ExitCode;
use um_execution::SystemCommandRunner;
use um_runner::{
    CredentialRequirement, Declaration, DeclarationKind, Node, NodeKind, OriginDeclaration,
    RepositorySource, UnknownDeclaration, WorkflowCredentials, read_workflow_credentials,
};

use super::readiness::{self, Readiness, Repository};

pub(super) fn execute(
    source_root: &Path,
    workflow_files: &[PathBuf],
    json: bool,
    readiness: Option<&Readiness<'_>>,
) -> super::super::super::CommandResult {
    // A named project, not the `origin` remote, names the repository's provider, so the project
    // is read before the derivation and Git is not asked (REQ-CREDREADY-21).
    if let Some(readiness) = readiness
        && let Some(project) = readiness.project
    {
        return super::super::super::execute_deployment_leaf(
            readiness,
            COMMAND_PATH,
            DEPLOYMENT_CONTEXT,
            |readiness, deployment| {
                let session = readiness::Session::new(deployment, readiness)?;
                let api_url = deployment.fingerprint().api_url();
                let repository = match session.read_project(project)? {
                    Ok(repository) => repository,
                    Err(ending) => {
                        return ending.write(api_url, readiness.authentication.kind(), json);
                    }
                };
                let source =
                    RepositorySource::Project(repository.map(|repository| repository.kind));
                let Some(credentials) = derive(source_root, workflow_files, source, json)? else {
                    return Ok(ExitCode::GeneralFailure);
                };
                check(&session, readiness, api_url, credentials, repository, json)
            },
        );
    }
    let source = RepositorySource::OriginRemote(Arc::new(SystemCommandRunner));
    let Some(credentials) = derive(source_root, workflow_files, source, json)? else {
        return Ok(ExitCode::GeneralFailure);
    };
    // Nothing is loaded and Cloud is not contacted unless a requirement has a read
    // (REQ-RUNNERENROLL-45, REQ-CREDREADY-13).
    let Some(readiness) = readiness.filter(|_| {
        credentials
            .requirements
            .iter()
            .any(|requirement| readiness::read_for(requirement.kind).is_some())
    }) else {
        let report = Report {
            credentials,
            organization: readiness.map(|readiness| readiness.organization),
            project: None,
            deployment: None,
        };
        return write_report(&report, json).map_err(Into::into);
    };
    super::super::super::execute_deployment_leaf(
        credentials,
        COMMAND_PATH,
        DEPLOYMENT_CONTEXT,
        |credentials, deployment| {
            let session = readiness::Session::new(deployment, readiness)?;
            let api_url = deployment.fingerprint().api_url();
            check(&session, readiness, api_url, credentials, None, json)
        },
    )
}

const COMMAND_PATH: &[&str] = &["runner", "doctor"];
const DEPLOYMENT_CONTEXT: &str = "configure the readiness check's Cloud access";

/// The credentials the tree needs, or `None` once the first file that does not resolve was
/// rejected as `um workflow validate` rejects it (REQ-CREDREADY-2).
pub(in crate::cli) fn derive(
    source_root: &Path,
    workflow_files: &[PathBuf],
    repository: RepositorySource,
    json: bool,
) -> anyhow::Result<Option<WorkflowCredentials>> {
    match read_workflow_credentials(source_root, workflow_files, repository) {
        Ok(credentials) => Ok(Some(credentials)),
        Err(failure) => {
            if json {
                crate::cli::workflow::validate::write_json_invalid(&failure)
            } else {
                crate::cli::workflow::validate::write_human_invalid(&failure)
            }
            .context("write workflow validation result")?;
            Ok(None)
        }
    }
}

/// Makes the read of each requirement that has one, in kind order, combines a named project's
/// repository with its kind's code, and writes the report; a failure that ends the command is
/// written instead (REQ-CREDREADY-14).
fn check(
    session: &readiness::Session<'_>,
    readiness: &Readiness<'_>,
    api_url: &str,
    mut credentials: WorkflowCredentials,
    repository: Option<Repository>,
    json: bool,
) -> anyhow::Result<ExitCode> {
    for requirement in &mut credentials.requirements {
        let Some(read) = readiness::read_for(requirement.kind) else {
            continue;
        };
        match read(session)? {
            Ok(code) => {
                requirement.code = repository
                    .filter(|repository| repository.kind == requirement.kind)
                    .map_or(code, |repository| repository.code(code));
            }
            Err(ending) => {
                return ending.write(api_url, readiness.authentication.kind(), json);
            }
        }
    }
    let report = Report {
        credentials,
        organization: Some(readiness.organization),
        project: readiness.project,
        deployment: Some(api_url),
    };
    write_report(&report, json)
}

/// The derivation's result, after any Cloud reads, and what the report names beside it.
struct Report<'a> {
    credentials: WorkflowCredentials,
    /// The organization `--readiness` named, for the next actions.
    organization: Option<&'a str>,
    /// The project `--project-id` named, for the next actions.
    project: Option<&'a str>,
    /// The deployment asked, when a read was made (REQ-CREDREADY-17).
    deployment: Option<&'a str>,
}

/// Writes the report; the command fails when a requirement does (REQ-CREDREADY-17).
fn write_report(report: &Report<'_>, json: bool) -> anyhow::Result<ExitCode> {
    if json {
        super::super::super::write_pretty_json(&JsonReport::from_report(report))
            .context("write JSON runner doctor report")?;
    } else {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        write_human_report_to(&mut stdout, report).context("write runner doctor report")?;
    }
    Ok(if report.credentials.fails() {
        ExitCode::GeneralFailure
    } else {
        ExitCode::Success
    })
}

fn write_human_report_to(output: &mut impl Write, report: &Report<'_>) -> io::Result<()> {
    let credentials = &report.credentials;
    writeln!(output, "Useful Machinery runner doctor")?;
    writeln!(output)?;
    for workflow in &credentials.workflows {
        writeln!(output, "workflow: {workflow}")?;
    }
    if let Some(deployment) = report.deployment {
        writeln!(output, "deployment: {deployment}")?;
    }
    writeln!(output)?;

    if credentials.requirements.is_empty() {
        writeln!(output, "✓ The workflows declare no credentials.")?;
        writeln!(output)?;
    }
    for requirement in &credentials.requirements {
        write_requirement(output, requirement, report)?;
    }

    if !credentials.unknown.is_empty() {
        writeln!(output, "── unknown ──")?;
        writeln!(
            output,
            "These declarations may use credentials the workflows do not name."
        )?;
        // The entries of no workflow file come last, under their own heading.
        let mut current_group = None;
        for entry in &credentials.unknown {
            let group = group_label(entry);
            if current_group != Some(group) {
                writeln!(output, "{group}")?;
                current_group = Some(group);
            }
            writeln!(output, "  {}", describe(entry))?;
        }
        writeln!(output)?;
    }

    let summary = credentials.summary();
    writeln!(output, "── summary ──")?;
    writeln!(output, "missing: {}", summary.missing)?;
    writeln!(output, "reconnect required: {}", summary.reconnect_required)?;
    writeln!(
        output,
        "insufficient permission: {}",
        summary.insufficient_permission
    )?;
    writeln!(output, "unverified: {}", summary.unverified)?;
    writeln!(output, "unknown: {}", summary.unknown)?;
    Ok(())
}

fn write_requirement(
    output: &mut impl Write,
    requirement: &CredentialRequirement,
    report: &Report<'_>,
) -> io::Result<()> {
    let marker = if requirement.fails() { '✗' } else { '!' };
    writeln!(output, "{marker} {}", requirement.kind.title())?;
    writeln!(output, "  kind: {}", requirement.kind.as_str())?;
    writeln!(output, "  state: {}", requirement.state())?;
    writeln!(output, "  code: {}", requirement.code.as_str())?;
    if let Some(cloud) = requirement.cloud() {
        writeln!(output, "  cloud: {cloud}")?;
    }
    for origin in &requirement.origins {
        writeln!(
            output,
            "  needed by: {} · {}",
            group_label(origin),
            describe(origin)
        )?;
    }
    writeln!(output)?;
    writeln!(
        output,
        "  {}",
        requirement.next_action(report.organization, report.project)
    )?;
    writeln!(output)
}

/// The human label of a declaration; the JSON document carries its stable identifier.
pub(in crate::cli) trait HumanLabel {
    fn label(self) -> &'static str;
}

impl HumanLabel for OriginDeclaration {
    fn label(self) -> &'static str {
        match self {
            Self::AgentProfile => "agent profile",
            Self::ForwardedVariable => "forwarded variable",
            Self::Binding => "binding",
            Self::Mirror => "Linear mirror",
            Self::RequiredTool => "required tool",
            Self::Repository => "repository",
        }
    }
}

impl HumanLabel for UnknownDeclaration {
    fn label(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Agent => "agent",
            Self::PiHarness => "Pi harness",
            Self::ForwardedVariable => "forwarded variable",
            Self::ConnectionBinding => "connection binding",
            Self::Repository => "repository",
        }
    }
}

/// The workflow file a declaration sits in, or the source root for the repository's.
pub(in crate::cli) fn group_label<K>(entry: &Declaration<K>) -> &str {
    entry.workflow.as_deref().unwrap_or("source root")
}

/// `<node> · <declaration> [<name>]`, without the workflow file.
pub(in crate::cli) fn describe<K: DeclarationKind + HumanLabel>(entry: &Declaration<K>) -> String {
    let mut parts = Vec::new();
    if let Some(node) = &entry.node {
        parts.push(format!("{} {}", node_label(node.kind), node.id));
    }
    parts.push(match &entry.name {
        Some(name) => format!("{} {name}", entry.declaration.label()),
        None => entry.declaration.label().to_owned(),
    });
    parts.join(" · ")
}

const fn node_label(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Step => "step",
        NodeKind::Finalizer => "finalizer",
        NodeKind::RecoveryHandler => "recovery handler",
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonReport<'a> {
    schema_version: u8,
    command: &'static str,
    deployment: Option<&'a str>,
    workflows: &'a [String],
    requirements: Vec<JsonRequirement<'a>>,
    unknown: Vec<JsonDeclaration<'a>>,
    summary: JsonSummary,
}

impl<'a> JsonReport<'a> {
    fn from_report(report: &'a Report<'a>) -> Self {
        let credentials = &report.credentials;
        let summary = credentials.summary();
        Self {
            schema_version: 1,
            command: super::COMMAND_NAME,
            deployment: report.deployment,
            workflows: &credentials.workflows,
            requirements: credentials
                .requirements
                .iter()
                .map(|requirement| JsonRequirement::from_requirement(requirement, report))
                .collect(),
            unknown: credentials
                .unknown
                .iter()
                .map(JsonDeclaration::from_declaration)
                .collect(),
            summary: JsonSummary {
                missing: summary.missing,
                reconnect_required: summary.reconnect_required,
                insufficient_permission: summary.insufficient_permission,
                unverified: summary.unverified,
                unknown: summary.unknown,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonRequirement<'a> {
    kind: &'static str,
    state: &'static str,
    code: &'static str,
    /// Present on the kinds Cloud holds: `github`, `gitlab` and `linear` (REQ-CREDREADY-16).
    #[serde(skip_serializing_if = "Option::is_none")]
    cloud: Option<&'static str>,
    next_action: String,
    origins: Vec<JsonDeclaration<'a>>,
}

impl<'a> JsonRequirement<'a> {
    fn from_requirement(requirement: &'a CredentialRequirement, report: &Report<'_>) -> Self {
        Self {
            kind: requirement.kind.as_str(),
            state: requirement.state(),
            code: requirement.code.as_str(),
            cloud: requirement.cloud(),
            next_action: requirement.next_action(report.organization, report.project),
            origins: requirement
                .origins
                .iter()
                .map(JsonDeclaration::from_declaration)
                .collect(),
        }
    }
}

#[derive(Serialize)]
pub(in crate::cli) struct JsonDeclaration<'a> {
    declaration: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node: Option<JsonNode<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
}

impl<'a> JsonDeclaration<'a> {
    pub(in crate::cli) fn from_declaration<K: DeclarationKind>(entry: &'a Declaration<K>) -> Self {
        Self {
            declaration: entry.declaration.as_str(),
            workflow: entry.workflow.as_deref(),
            node: entry.node.as_ref().map(JsonNode::from_node),
            name: entry.name.as_deref(),
        }
    }
}

#[derive(Serialize)]
struct JsonNode<'a> {
    kind: &'static str,
    id: &'a str,
}

impl<'a> JsonNode<'a> {
    fn from_node(node: &'a Node) -> Self {
        Self {
            kind: node.kind.as_str(),
            id: &node.id,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonSummary {
    missing: usize,
    reconnect_required: usize,
    insufficient_permission: usize,
    unverified: usize,
    unknown: usize,
}
