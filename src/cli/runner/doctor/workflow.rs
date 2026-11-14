//! The workflow mode of `um runner doctor` (REQ-CREDREADY-1 to REQ-CREDREADY-26): the
//! credentials a workflow tree declares, with what Cloud holds for the organization and a named
//! project when `--readiness` asks and what the member holds of each provider when `--grants`
//! asks, printed as a checklist or a JSON document. `--connect` first runs the grant flow of
//! `um grant create --workflow` over the same derivation (INV-CLI-15).

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::Serialize;

use crate::cli::CommandFailure;
use crate::exit_code::ExitCode;
use um_human_auth::Deployment;
use um_runner::{
    CredentialRequirement, CredentialSummary, Declaration, DeclarationKind, Node, NodeKind,
    OriginDeclaration, RepositorySource, UnknownDeclaration, WorkflowCredentials,
    read_workflow_credentials,
};

use super::super::super::grant::{Flow, output};
use super::grants::{self, Failing, Held, Shortfall};
use super::readiness::{self, Readiness, Repository};
use super::roots::Roots;

pub(super) fn execute(
    source_root: Option<&Path>,
    workflow_files: &[PathBuf],
    json: bool,
    readiness: Option<&Readiness<'_>>,
    grants: Option<&grants::Request<'_>>,
    connect: Option<&Flow<'_>>,
) -> super::super::super::CommandResult {
    // A file in no repository is refused before anything is loaded (REQ-CREDREADY-1).
    let roots = Roots::find(
        workflow_files,
        source_root,
        "um runner doctor --workflow FILE --source-root ROOT",
    )?;
    let format = Format::new(json, connect.is_some());
    // A named project, not the `origin` remote, names the repository's provider, so the project
    // is read before the derivation and Git is not asked for it (REQ-CREDREADY-21).
    if let Some(readiness) = readiness
        && let Some(project) = readiness.project
    {
        return super::super::super::execute_deployment_command(
            Some(readiness),
            COMMAND_PATH,
            DEPLOYMENT_CONTEXT,
            |readiness, deployment| {
                let session = readiness::Session::new(deployment, readiness)?;
                let api_url = deployment.fingerprint().api_url();
                let repository = match session.read_project(project)? {
                    Ok(repository) => repository,
                    Err(ending) => {
                        return ending
                            .write(api_url, readiness.authentication.kind(), json)
                            .map_err(Into::into);
                    }
                };
                let source =
                    RepositorySource::Project(repository.map(|repository| repository.kind));
                let Some(credentials) = roots.derive(&source, json)? else {
                    return Ok(ExitCode::GeneralFailure);
                };
                let flowed = match connected(connect, deployment, &credentials)? {
                    Ok(flowed) => flowed,
                    Err(ended) => return Ok(ended),
                };
                let checked = check(
                    deployment,
                    Some((&session, readiness)),
                    grants,
                    credentials,
                    repository,
                    format,
                )?;
                Ok(joined(flowed, checked))
            },
        );
    }
    let Some(credentials) = roots.derive(&roots.origin_remote(), json)? else {
        return Ok(ExitCode::GeneralFailure);
    };
    // Nothing is loaded and Cloud is not contacted unless a requirement has a read: a
    // connection read, or the grants read, which any requirement has, or `--connect` asks for
    // the grant flow, which takes a tree that calls for no kind to Cloud for nothing
    // (REQ-RUNNERENROLL-45, REQ-CREDREADY-13, REQ-CREDREADY-22, REQ-CREDREADY-24).
    let connections = readiness.is_some()
        && credentials
            .requirements
            .iter()
            .any(|requirement| readiness::read_for(requirement.kind).is_some());
    let reads = connections || (grants.is_some() && !credentials.requirements.is_empty());
    if connect.is_none() && !reads {
        return write_report(&unread(credentials, readiness), format).map_err(Into::into);
    }
    super::super::super::execute_deployment_command(
        Some(credentials),
        COMMAND_PATH,
        DEPLOYMENT_CONTEXT,
        |credentials, deployment| {
            let flowed = match connected(connect, deployment, &credentials)? {
                Ok(flowed) => flowed,
                Err(ended) => return Ok(ended),
            };
            let checked = if reads {
                let session = readiness
                    .map(|readiness| readiness::Session::new(deployment, readiness))
                    .transpose()?;
                let readiness = session.as_ref().zip(readiness);
                check(deployment, readiness, grants, credentials, None, format)?
            } else {
                write_report(&unread(credentials, readiness), format)?
            };
            Ok(joined(flowed, checked))
        },
    )
}

/// The report of a derivation no Cloud read was made for.
fn unread<'a>(
    credentials: WorkflowCredentials,
    readiness: Option<&'a Readiness<'a>>,
) -> Report<'a> {
    Report {
        credentials,
        organization: readiness.map(|readiness| readiness.organization),
        project: None,
        grants: None,
        deployment: None,
    }
}

/// Runs the grant flow over `credentials` when `--connect` asks for it (REQ-CREDREADY-24): its
/// status once it reached its summary and no signal stopped it, for the report that follows,
/// and otherwise, as the error, the status the command ends with at once, with no report
/// (REQ-CREDREADY-25). Without `--connect` nothing is run and nothing failed.
fn connected(
    connect: Option<&Flow<'_>>,
    deployment: &Deployment,
    credentials: &WorkflowCredentials,
) -> Result<Result<ExitCode, ExitCode>, CommandFailure> {
    let Some(flow) = connect else {
        return Ok(Ok(ExitCode::Success));
    };
    let ending = flow.run_workflows(deployment, credentials, output::Scope::Doctor)?;
    if !ending.summarized || ending.stopped() {
        return Ok(Err(ending.code));
    }
    // The report follows the summary as a section of its own.
    if !flow.json {
        writeln!(io::stdout()).context("write runner doctor report")?;
    }
    Ok(Ok(ending.code))
}

/// The command's status once the report is written: the report's when it failed, and otherwise
/// the flow's, which is 1 when a sign-in failed or every provider selected was passed over
/// (REQ-CREDREADY-17, REQ-CREDREADY-25).
fn joined(flowed: ExitCode, checked: ExitCode) -> ExitCode {
    if checked == ExitCode::Success {
        flowed
    } else {
        checked
    }
}

/// How the report is written.
#[derive(Clone, Copy)]
enum Format {
    Human,
    /// One JSON document.
    Json,
    /// One JSON line, the event `report`, after the grant flow's events (REQ-CREDREADY-25).
    Event,
}

impl Format {
    const fn new(json: bool, connect: bool) -> Self {
        match (json, connect) {
            (false, _) => Self::Human,
            (true, false) => Self::Json,
            (true, true) => Self::Event,
        }
    }

    const fn json(self) -> bool {
        !matches!(self, Self::Human)
    }
}

const COMMAND_PATH: &[&str] = &["runner", "doctor"];
const DEPLOYMENT_CONTEXT: &str = "configure the doctor's Cloud access";

/// The credentials the tree under `source_root` needs, or `None` once the first file that does
/// not resolve was rejected as `um workflow validate` rejects it (REQ-CREDREADY-2).
pub(super) fn derive(
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

/// With `readiness`, makes the connection read of each requirement that has one, in kind
/// order, and combines a named project's repository with its kind's code; with `grants`, then
/// reads the member's grants when a requirement exists; and writes the report. A failure that
/// ends the command is written instead (REQ-CREDREADY-14, REQ-CREDREADY-22).
fn check(
    deployment: &Deployment,
    readiness: Option<(&readiness::Session<'_>, &Readiness<'_>)>,
    grants: Option<&grants::Request<'_>>,
    mut credentials: WorkflowCredentials,
    repository: Option<Repository>,
    format: Format,
) -> anyhow::Result<ExitCode> {
    let api_url = deployment.fingerprint().api_url();
    let json = format.json();
    if let Some((session, readiness)) = readiness {
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
    }
    let held = match grants.filter(|_| !credentials.requirements.is_empty()) {
        Some(request) => match request.read(deployment)? {
            Ok(held) => Some(held),
            Err(failure) => return output::failure(api_url, &failure, json),
        },
        None => None,
    };
    let readiness = readiness.map(|(_, readiness)| readiness);
    let report = Report {
        credentials,
        organization: readiness.map(|readiness| readiness.organization),
        project: readiness.and_then(|readiness| readiness.project),
        grants: held,
        deployment: Some(api_url),
    };
    write_report(&report, format)
}

/// The derivation's result, after any Cloud reads, and what the report names beside it.
struct Report<'a> {
    credentials: WorkflowCredentials,
    /// The organization `--readiness` named, for the next actions.
    organization: Option<&'a str>,
    /// The project `--project-id` named, for the next actions.
    project: Option<&'a str>,
    /// The member's grants, when `--grants` read them (REQ-CREDREADY-22).
    grants: Option<Held<'a>>,
    /// The deployment asked, when a read was made (REQ-CREDREADY-17).
    deployment: Option<&'a str>,
}

impl Report<'_> {
    /// Why `--require-grants` fails `requirement` on the member's grants (REQ-CREDREADY-26). A
    /// requirement `--readiness` already fails keeps its state and code.
    fn shortfall(&self, requirement: &CredentialRequirement) -> Option<Shortfall> {
        if requirement.fails() {
            return None;
        }
        self.grants.as_ref()?.shortfall(requirement.kind)
    }

    /// The requirements by state, each one `--require-grants` fails counted in its failing
    /// state rather than as `unverified`.
    fn summary(&self) -> CredentialSummary {
        let mut summary = self.credentials.summary();
        for requirement in &self.credentials.requirements {
            if let Some(shortfall) = self.shortfall(requirement) {
                summary.unverified -= 1;
                match shortfall.state {
                    Failing::Missing => summary.missing += 1,
                    Failing::ReconnectRequired => summary.reconnect_required += 1,
                }
            }
        }
        summary
    }

    /// Whether a requirement reads `missing`, `reconnect_required` or `insufficient_permission`
    /// (REQ-CREDREADY-17, REQ-CREDREADY-26).
    fn fails(&self) -> bool {
        self.credentials
            .requirements
            .iter()
            .any(|requirement| requirement.fails() || self.shortfall(requirement).is_some())
    }
}

/// Writes the report; the command fails when a requirement does (REQ-CREDREADY-17).
fn write_report(report: &Report<'_>, format: Format) -> anyhow::Result<ExitCode> {
    match format {
        Format::Human => {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            write_human_report_to(&mut stdout, report).context("write runner doctor report")?;
        }
        Format::Json => {
            super::super::super::write_pretty_json(&JsonReport::from_report(report, None))
                .context("write JSON runner doctor report")?;
        }
        Format::Event => {
            super::super::super::write_json_line(&JsonReport::from_report(report, Some("report")))
                .context("write JSON runner doctor report")?;
        }
    }
    Ok(if report.fails() {
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

    let summary = report.summary();
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
    let shortfall = report.shortfall(requirement);
    let marker = if requirement.fails() || shortfall.is_some() {
        '✗'
    } else {
        '!'
    };
    writeln!(output, "{marker} {}", requirement.kind.title())?;
    writeln!(output, "  kind: {}", requirement.kind.as_str())?;
    writeln!(
        output,
        "  state: {}",
        state(requirement, shortfall.as_ref())
    )?;
    writeln!(output, "  code: {}", code(requirement, shortfall.as_ref()))?;
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
    writeln!(output, "  {}", next_action(requirement, shortfall, report))?;
    if let Some(held) = &report.grants {
        writeln!(output)?;
        held.of(requirement.kind).write_human(output)?;
    }
    writeln!(output)
}

/// A requirement's state, or the failing state `--require-grants` gives it (REQ-CREDREADY-26).
fn state(requirement: &CredentialRequirement, shortfall: Option<&Shortfall>) -> &'static str {
    shortfall.map_or_else(|| requirement.state(), |shortfall| shortfall.state.as_str())
}

/// A requirement's code, or the code `--require-grants` gives it (REQ-CREDREADY-26).
fn code(requirement: &CredentialRequirement, shortfall: Option<&Shortfall>) -> &'static str {
    shortfall.map_or_else(|| requirement.code.as_str(), |shortfall| shortfall.code)
}

/// A requirement's next action, or the next action of the grant status `--require-grants` fails
/// it on, with the command it names (REQ-CREDREADY-26).
fn next_action(
    requirement: &CredentialRequirement,
    shortfall: Option<Shortfall>,
    report: &Report<'_>,
) -> String {
    shortfall.map_or_else(
        || requirement.next_action(report.organization, report.project),
        |shortfall| shortfall.next_action,
    )
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
    /// `report`, when the report follows the grant flow's events (REQ-CREDREADY-25).
    #[serde(skip_serializing_if = "Option::is_none")]
    event: Option<&'static str>,
    command: &'static str,
    deployment: Option<&'a str>,
    workflows: &'a [String],
    requirements: Vec<JsonRequirement<'a>>,
    unknown: Vec<JsonDeclaration<'a>>,
    summary: JsonSummary,
}

impl<'a> JsonReport<'a> {
    fn from_report(report: &'a Report<'a>, event: Option<&'static str>) -> Self {
        let credentials = &report.credentials;
        let summary = report.summary();
        Self {
            schema_version: 1,
            event,
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
    /// What the member holds of the kind's provider, with `--grants` (REQ-CREDREADY-22).
    #[serde(skip_serializing_if = "Option::is_none")]
    grant: Option<grants::ProviderView<'a>>,
}

impl<'a> JsonRequirement<'a> {
    fn from_requirement(requirement: &'a CredentialRequirement, report: &'a Report<'a>) -> Self {
        let shortfall = report.shortfall(requirement);
        Self {
            kind: requirement.kind.as_str(),
            state: state(requirement, shortfall.as_ref()),
            code: code(requirement, shortfall.as_ref()),
            cloud: requirement.cloud(),
            next_action: next_action(requirement, shortfall, report),
            origins: requirement
                .origins
                .iter()
                .map(JsonDeclaration::from_declaration)
                .collect(),
            grant: report.grants.as_ref().map(|held| held.of(requirement.kind)),
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
