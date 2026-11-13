//! The credentials a workflow tree declares it needs, read offline from its definitions and
//! the source root's `origin` remote or, when the command names a project, the provider of the
//! project's repository (REQ-CREDREADY-1 to REQ-CREDREADY-12, REQ-CREDREADY-21). Nothing here
//! runs a node, starts a harness or reads the process environment; the one process started is
//! the bounded `git remote get-url origin` (INV-HARNESS-17). The derivation gives every requirement
//! its offline code; the command, which alone may ask Cloud, replaces a provider's code with
//! what the organization's connections show (REQ-CREDREADY-13), and the state, the `cloud`
//! value and the next action follow from the code.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use um_execution::{
    BindingKind, CommandRunner, ResolutionFailure, ResolvedWorkflow, RootBlocks, ValidatedHarness,
    ValidatedRecoveryHandler, ValidatedStep, resolve_workflow_file,
};

use super::git::origin_remote_host;

/// A provider or harness whose credential a workflow needs, in report order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CredentialKind {
    Github,
    Gitlab,
    Linear,
    ClaudeCode,
    Codex,
}

impl CredentialKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Github => "github",
            Self::Gitlab => "gitlab",
            Self::Linear => "linear",
            Self::ClaudeCode => "claude_code",
            Self::Codex => "codex",
        }
    }

    /// The kind's name in a sentence or a heading.
    pub const fn title(self) -> &'static str {
        match self {
            Self::Github => "GitHub",
            Self::Gitlab => "GitLab",
            Self::Linear => "Linear",
            Self::ClaudeCode => "Claude Code",
            Self::Codex => "Codex",
        }
    }

    /// Whether Cloud holds the organization's connections for this kind (REQ-CREDREADY-16);
    /// a harness is signed in on the runner host, which nothing reads.
    const fn held_by_cloud(self) -> bool {
        match self {
            Self::Github | Self::Gitlab | Self::Linear => true,
            Self::ClaudeCode | Self::Codex => false,
        }
    }

    /// The code a requirement carries when no readiness check has run (REQ-CREDREADY-7).
    const fn offline_code(self) -> ReadinessCode {
        if self.held_by_cloud() {
            ReadinessCode::NotChecked
        } else {
            ReadinessCode::RunnerUnverified
        }
    }
}

/// Why a requirement is in its state; the state, and for a kind Cloud holds the `cloud`
/// value, are derived from it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessCode {
    /// No organization connection was read for a provider.
    NotChecked,
    /// The credential reaches a runner only on the runner host or, for a provider, only for a
    /// run: a harness sign-in, or a connection Cloud holds ready (REQ-CREDREADY-16).
    RunnerUnverified,
    /// The organization has no connection of the kind other than deleted ones.
    NotConnected,
    /// The organization has a connection of the kind and none is active.
    ConnectionInactive,
    /// No active connection was granted every scope it requires.
    ScopeMissing,
    /// No connection of the kind is active and a connection's credential expired.
    CredentialExpired,
    /// Cloud refused the caller the read (REQ-CREDREADY-14).
    NotPermitted,
    /// The named project's repository is unavailable (REQ-CREDREADY-21).
    RepositoryUnavailable,
    /// The named project reports its repository provider's admission closed on the deployment,
    /// which no reconnection fixes (REQ-CREDREADY-21).
    ProviderUnavailable,
}

impl ReadinessCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotChecked => "not_checked",
            Self::RunnerUnverified => "runner_unverified",
            Self::NotConnected => "not_connected",
            Self::ConnectionInactive => "connection_inactive",
            Self::ScopeMissing => "scope_missing",
            Self::CredentialExpired => "credential_expired",
            Self::NotPermitted => "not_permitted",
            Self::RepositoryUnavailable => "repository_unavailable",
            Self::ProviderUnavailable => "provider_unavailable",
        }
    }

    pub const fn state(self) -> &'static str {
        match self {
            Self::NotChecked
            | Self::RunnerUnverified
            | Self::NotPermitted
            | Self::ProviderUnavailable => UNVERIFIED,
            Self::NotConnected => MISSING,
            Self::ConnectionInactive | Self::CredentialExpired | Self::RepositoryUnavailable => {
                RECONNECT_REQUIRED
            }
            Self::ScopeMissing => INSUFFICIENT_PERMISSION,
        }
    }

    /// Whether a requirement with this code fails a readiness check (REQ-CREDREADY-17).
    pub fn fails(self) -> bool {
        self.state() != UNVERIFIED
    }
}

const UNVERIFIED: &str = "unverified";
const MISSING: &str = "missing";
const RECONNECT_REQUIRED: &str = "reconnect_required";
const INSUFFICIENT_PERMISSION: &str = "insufficient_permission";

/// The kind of workflow node a declaration sits on; a recovery handler takes its step's id.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NodeKind {
    Step,
    Finalizer,
    RecoveryHandler,
}

impl NodeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Step => "step",
            Self::Finalizer => "finalizer",
            Self::RecoveryHandler => "recovery_handler",
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Node {
    pub kind: NodeKind,
    pub id: String,
}

/// A declaration that calls for a requirement.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OriginDeclaration {
    /// An agent node's profile selects a harness; `name` is the profile.
    AgentProfile,
    /// A root `environmentPassthrough` name; `name` is the variable.
    ForwardedVariable,
    /// A root binding of a provider's kind; `name` is the binding key.
    Binding,
    /// A root `mirror.linear` block.
    Mirror,
    /// A root `requires.tools` entry; `name` is the tool.
    RequiredTool,
    /// The source root's repository, by its `origin` remote's host or, when a project is named,
    /// by the project's repository; `name` is `origin` or `project`.
    Repository,
}

/// A declaration from which no credential need can be read.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum UnknownDeclaration {
    /// A command node, whose program may use a credential nothing declares.
    Command,
    /// An agent node, whose agent may use a credential nothing declares.
    Agent,
    /// An agent node on Pi, whose model provider the profile does not name.
    PiHarness,
    /// A forwarded variable that reads as a credential of no known kind.
    ForwardedVariable,
    /// A root binding of kind `connection`, which names no provider; `name` is the key.
    ConnectionBinding,
    /// The source root's repository, whose `origin` remote names no known host or could not
    /// be read, or the named project, which has no repository; `name` is `origin` or `project`.
    Repository,
}

pub trait DeclarationKind: Copy + Ord {
    fn as_str(self) -> &'static str;
}

impl DeclarationKind for OriginDeclaration {
    fn as_str(self) -> &'static str {
        match self {
            Self::AgentProfile => "agent_profile",
            Self::ForwardedVariable => "forwarded_variable",
            Self::Binding => "binding",
            Self::Mirror => "mirror",
            Self::RequiredTool => "required_tool",
            Self::Repository => "repository",
        }
    }
}

impl DeclarationKind for UnknownDeclaration {
    fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::Agent => "agent",
            Self::PiHarness => "pi_harness",
            Self::ForwardedVariable => "forwarded_variable",
            Self::ConnectionBinding => "connection_binding",
            Self::Repository => "repository",
        }
    }
}

/// One declaration, in a workflow file or, with no `workflow`, of the whole report. The field
/// order is the sort order: workflow path, then declaration, then node, then name; a report
/// lists the declarations of no workflow file after the others.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Declaration<K> {
    pub workflow: Option<String>,
    pub declaration: K,
    pub node: Option<Node>,
    pub name: Option<String>,
}

pub type CredentialOrigin = Declaration<OriginDeclaration>;
pub type UnknownCredential = Declaration<UnknownDeclaration>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialRequirement {
    pub kind: CredentialKind,
    pub code: ReadinessCode,
    pub origins: Vec<CredentialOrigin>,
}

impl CredentialRequirement {
    pub const fn state(&self) -> &'static str {
        self.code.state()
    }

    /// What Cloud showed of the organization's connections for a kind it holds, and `None` for
    /// a harness (REQ-CREDREADY-16): `not_checked` without a read, `ready` for a connection
    /// it holds ready, and otherwise the state, which is `unverified` for a refused read or a
    /// closed provider admission.
    pub const fn cloud(&self) -> Option<&'static str> {
        if !self.kind.held_by_cloud() {
            return None;
        }
        Some(match self.code {
            ReadinessCode::NotChecked => "not_checked",
            ReadinessCode::RunnerUnverified => "ready",
            code => code.state(),
        })
    }

    /// Whether the requirement fails a readiness check (REQ-CREDREADY-17).
    pub fn fails(&self) -> bool {
        self.code.fails()
    }

    /// What the operator can do about the requirement (REQ-CREDREADY-7, REQ-CREDREADY-14 to
    /// REQ-CREDREADY-16, REQ-CREDREADY-19 to REQ-CREDREADY-21), for `organization` and
    /// `project` when they were named. It names commands, never a credential.
    pub fn next_action(&self, organization: Option<&str>, project: Option<&str>) -> String {
        let organization = organization.unwrap_or("ORGANIZATION");
        let title = self.kind.title();
        let show_repository = || {
            format!(
                "um project repository show {organization} {}",
                project.unwrap_or("PROJECT")
            )
        };
        let missing = self.code == ReadinessCode::NotConnected;
        // The command that lists the kind's connections, and the remedy for one Cloud shows
        // missing, inactive or lacking a scope.
        let (list, remedy) = match self.kind {
            CredentialKind::Github => (
                "um connection github installation list",
                format!(
                    "Install the GitHub App for the organization with um connection github setup begin {organization}."
                ),
            ),
            CredentialKind::Gitlab if missing => (
                "um connection gitlab list",
                format!(
                    "Connect GitLab.com with um connection gitlab create {organization} https://gitlab.com, then give it a credential with um connection gitlab credential create."
                ),
            ),
            CredentialKind::Gitlab => (
                "um connection gitlab list",
                format!(
                    "Replace the GitLab connection's credential with um connection gitlab credential create {organization} CONNECTION; um connection gitlab list {organization} shows its ID."
                ),
            ),
            CredentialKind::Linear if missing => (
                "um connection linear list",
                format!(
                    "Connect a Linear workspace with um connection linear authorization create {organization}."
                ),
            ),
            CredentialKind::Linear => (
                "um connection linear list",
                format!(
                    "Reauthorize the Linear connection with um connection linear authorization create {organization} --connection-id CONNECTION; um connection linear list {organization} shows its ID."
                ),
            ),
            CredentialKind::ClaudeCode | CredentialKind::Codex => {
                return format!(
                    "Sign {title} in on the runner host as the user that runs um runner serve."
                );
            }
        };
        match self.code {
            ReadinessCode::NotChecked => format!(
                "Run this check with --readiness --organization-id {organization} to read the organization's {title} connections from Cloud, or list them with {list} {organization}."
            ),
            ReadinessCode::RunnerUnverified => format!(
                "Cloud holds a ready {title} connection and issues its credential to a runner only for a run."
            ),
            ReadinessCode::NotPermitted => format!(
                "This caller may not read the organization's {title} connections; an organization owner can run this check."
            ),
            ReadinessCode::RepositoryUnavailable => format!(
                "The project's {title} repository is unavailable; check its connection and selection with {}.",
                show_repository()
            ),
            ReadinessCode::ProviderUnavailable => format!(
                "The deployment has closed {title} run admission for the project's repository; {} shows it.",
                show_repository()
            ),
            ReadinessCode::NotConnected
            | ReadinessCode::ConnectionInactive
            | ReadinessCode::ScopeMissing
            | ReadinessCode::CredentialExpired => remedy,
        }
    }
}

/// The requirements counted by state, and the unknown entries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialSummary {
    pub missing: usize,
    pub reconnect_required: usize,
    pub insufficient_permission: usize,
    pub unverified: usize,
    pub unknown: usize,
}

/// The credentials a workflow tree needs: the covered files by source-root-relative path,
/// one requirement per kind in kind order, and what the declarations cannot show.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowCredentials {
    pub workflows: Vec<String>,
    pub requirements: Vec<CredentialRequirement>,
    pub unknown: Vec<UnknownCredential>,
}

impl WorkflowCredentials {
    pub fn summary(&self) -> CredentialSummary {
        let in_state = |state: &str| {
            self.requirements
                .iter()
                .filter(|requirement| requirement.state() == state)
                .count()
        };
        CredentialSummary {
            missing: in_state(MISSING),
            reconnect_required: in_state(RECONNECT_REQUIRED),
            insufficient_permission: in_state(INSUFFICIENT_PERMISSION),
            unverified: in_state(UNVERIFIED),
            unknown: self.unknown.len(),
        }
    }

    /// Whether any requirement fails a readiness check (REQ-CREDREADY-17).
    pub fn fails(&self) -> bool {
        self.requirements.iter().any(CredentialRequirement::fails)
    }
}

/// The recognition rules, kept in one table (REQ-CREDREADY-5, REQ-CREDREADY-11,
/// REQ-CREDREADY-12).
struct Recognition {
    /// A forwarded name holding one of these words reads as a credential.
    credential_words: &'static [&'static str],
    /// Whole forwarded names that read as a credential without such a word.
    credential_names: &'static [&'static str],
    /// The first word of a credential name and the kind it calls for.
    provider_words: &'static [(&'static str, CredentialKind)],
    /// A root `requires.tools` entry and the kind it calls for; other tools are ignored.
    required_tools: &'static [(&'static str, CredentialKind)],
    /// The host of the `origin` remote and the kind it calls for; other hosts are unknown.
    remote_hosts: &'static [(&'static str, CredentialKind)],
}

const RECOGNITION: Recognition = Recognition {
    credential_words: &["TOKEN", "KEY", "SECRET", "PASSWORD", "CREDENTIALS"],
    credential_names: &["CLAUDE_CONFIG_DIR", "CODEX_HOME"],
    provider_words: &[
        ("GITHUB", CredentialKind::Github),
        ("GH", CredentialKind::Github),
        ("GITLAB", CredentialKind::Gitlab),
        ("GLAB", CredentialKind::Gitlab),
        ("LINEAR", CredentialKind::Linear),
        ("ANTHROPIC", CredentialKind::ClaudeCode),
        ("CLAUDE", CredentialKind::ClaudeCode),
        ("OPENAI", CredentialKind::Codex),
        ("CODEX", CredentialKind::Codex),
    ],
    required_tools: &[
        ("gh", CredentialKind::Github),
        ("glab", CredentialKind::Gitlab),
    ],
    remote_hosts: &[
        ("github.com", CredentialKind::Github),
        ("gitlab.com", CredentialKind::Gitlab),
    ],
};

/// How a forwarded variable's name reads; only the name is read, never a value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ForwardedVariable {
    Ignored,
    Unknown,
    Calls(CredentialKind),
}

fn classify_forwarded_variable(name: &str) -> ForwardedVariable {
    let is_credential = RECOGNITION
        .credential_names
        .iter()
        .any(|credential| name.eq_ignore_ascii_case(credential))
        || name.split('_').any(|word| {
            RECOGNITION
                .credential_words
                .iter()
                .any(|credential| word.eq_ignore_ascii_case(credential))
        });
    if !is_credential {
        return ForwardedVariable::Ignored;
    }
    let first_word = name.split('_').next().unwrap_or_default();
    RECOGNITION
        .provider_words
        .iter()
        .find(|(word, _)| first_word.eq_ignore_ascii_case(word))
        .map_or(ForwardedVariable::Unknown, |(_, kind)| {
            ForwardedVariable::Calls(*kind)
        })
}

/// Where the derivation learns the provider of the source root's repository.
pub enum RepositorySource {
    /// The host of the source root's `origin` remote, asked of `git` through the runner
    /// (REQ-CREDREADY-12).
    OriginRemote(Arc<dyn CommandRunner>),
    /// The provider of the named project's repository, which the command read from Cloud, or
    /// `None` for a project with no repository; Git is not asked (REQ-CREDREADY-21).
    Project(Option<CredentialKind>),
}

/// Resolves each named workflow file and every file its `workflow` steps name, one level
/// deep, each once, and reads the credentials they declare, then names the repository's
/// provider from `repository`. The first file that does not resolve ends the read with its
/// resolution failure (REQ-CREDREADY-2), before Git is asked.
pub fn read_workflow_credentials(
    source_root: &Path,
    workflow_files: &[PathBuf],
    repository: RepositorySource,
) -> Result<WorkflowCredentials, ResolutionFailure> {
    let mut resolved = BTreeMap::new();
    for workflow_file in workflow_files {
        let workflow = resolve_workflow_file(source_root, workflow_file)?;
        let children = child_workflow_paths(&workflow);
        resolved.insert(workflow.source.workflow_path.clone(), workflow);
        for child in children {
            if resolved.contains_key(&child) {
                continue;
            }
            let child_workflow = resolve_workflow_file(source_root, &source_root.join(&child))?;
            resolved.insert(child_workflow.source.workflow_path.clone(), child_workflow);
        }
    }

    let mut reading = Reading::default();
    for (path, workflow) in &resolved {
        reading.read_workflow(path, workflow);
    }
    match repository {
        RepositorySource::OriginRemote(runner) => {
            let host = origin_remote_host(runner.as_ref(), source_root);
            reading.read_repository(remote_host_kind(host.as_deref()), ORIGIN_REMOTE);
        }
        RepositorySource::Project(kind) => reading.read_repository(kind, PROJECT),
    }
    Ok(reading.finish(resolved.into_keys().collect()))
}

fn child_workflow_paths(workflow: &ResolvedWorkflow) -> BTreeSet<String> {
    workflow
        .definition
        .steps
        .values()
        .filter_map(|step| match step {
            ValidatedStep::Workflow(step) => Some(step.workflow.clone()),
            ValidatedStep::Command(_) | ValidatedStep::Agent(_) => None,
        })
        .collect()
}

#[derive(Default)]
struct Reading {
    origins: BTreeMap<CredentialKind, BTreeSet<CredentialOrigin>>,
    unknown: BTreeSet<UnknownCredential>,
}

impl Reading {
    fn read_workflow(&mut self, path: &str, workflow: &ResolvedWorkflow) {
        let definition = &workflow.definition;
        for (id, step) in &definition.steps {
            self.read_node(path, NodeKind::Step, id, step);
        }
        for (id, finalizer) in &definition.finalizers {
            self.read_node(path, NodeKind::Finalizer, id, &finalizer.body);
        }
        for (id, recovery) in &definition.recoveries {
            let Some(handler) = recovery
                .as_ref()
                .and_then(|recovery| recovery.handler.as_ref())
            else {
                continue;
            };
            let node = Node {
                kind: NodeKind::RecoveryHandler,
                id: id.clone(),
            };
            match handler {
                ValidatedRecoveryHandler::Command { .. } => {
                    self.add_unknown(path, UnknownDeclaration::Command, Some(node), None);
                }
                ValidatedRecoveryHandler::Agent {
                    profile, harness, ..
                } => self.read_agent(path, node, profile, harness),
            }
        }
        for name in definition.environment_passthrough() {
            match classify_forwarded_variable(name) {
                ForwardedVariable::Ignored => {}
                ForwardedVariable::Unknown => self.add_unknown(
                    path,
                    UnknownDeclaration::ForwardedVariable,
                    None,
                    Some(name),
                ),
                ForwardedVariable::Calls(kind) => self.add_origin(
                    kind,
                    path,
                    OriginDeclaration::ForwardedVariable,
                    None,
                    Some(name),
                ),
            }
        }
        if let Some(root) = &definition.root {
            self.read_root_blocks(path, root);
        }
    }

    /// The root-only blocks, which only a tree's root file carries (REQ-CREDREADY-11).
    /// Triggers add nothing: a trigger's source is a binding already read.
    fn read_root_blocks(&mut self, path: &str, root: &RootBlocks) {
        for (key, binding) in &root.bindings {
            let kind = match binding.kind {
                BindingKind::LinearTeam | BindingKind::LinearState => CredentialKind::Linear,
                BindingKind::GithubRepository => CredentialKind::Github,
                BindingKind::Connection => {
                    self.add_unknown(path, UnknownDeclaration::ConnectionBinding, None, Some(key));
                    continue;
                }
                // A member names an organization principal, not a provider credential.
                BindingKind::Member | BindingKind::Members => continue,
            };
            self.add_origin(kind, path, OriginDeclaration::Binding, None, Some(key));
        }
        if root.mirror.is_some() {
            self.add_origin(
                CredentialKind::Linear,
                path,
                OriginDeclaration::Mirror,
                None,
                None,
            );
        }
        for tool in root.requires.iter().flat_map(|requires| &requires.tools) {
            if let Some((_, kind)) = RECOGNITION
                .required_tools
                .iter()
                .find(|(required, _)| required == tool)
            {
                self.add_origin(
                    *kind,
                    path,
                    OriginDeclaration::RequiredTool,
                    None,
                    Some(tool),
                );
            }
        }
    }

    /// The repository's provider as `name`, the remote or the project, names it; no provider
    /// is an unknown repository (REQ-CREDREADY-12, REQ-CREDREADY-21).
    fn read_repository(&mut self, kind: Option<CredentialKind>, name: &str) {
        match kind {
            Some(kind) => {
                self.origins
                    .entry(kind)
                    .or_default()
                    .insert(of_repository(OriginDeclaration::Repository, name));
            }
            None => {
                self.unknown
                    .insert(of_repository(UnknownDeclaration::Repository, name));
            }
        }
    }

    fn read_node(&mut self, path: &str, kind: NodeKind, id: &str, step: &ValidatedStep) {
        let node = Node {
            kind,
            id: id.to_owned(),
        };
        match step {
            ValidatedStep::Command(_) => {
                self.add_unknown(path, UnknownDeclaration::Command, Some(node), None);
            }
            ValidatedStep::Agent(step) => {
                self.read_agent(path, node, step.agent.profile_name(), &step.agent.harness);
            }
            // The child file is read as its own workflow.
            ValidatedStep::Workflow(_) => {}
        }
    }

    fn read_agent(&mut self, path: &str, node: Node, profile: &str, harness: &ValidatedHarness) {
        self.add_unknown(path, UnknownDeclaration::Agent, Some(node.clone()), None);
        let kind = match harness {
            ValidatedHarness::ClaudeCode(_) => CredentialKind::ClaudeCode,
            ValidatedHarness::Codex(_) => CredentialKind::Codex,
            ValidatedHarness::Pi(_) => {
                self.add_unknown(path, UnknownDeclaration::PiHarness, Some(node), None);
                return;
            }
        };
        self.add_origin(
            kind,
            path,
            OriginDeclaration::AgentProfile,
            Some(node),
            Some(profile),
        );
    }

    fn add_origin(
        &mut self,
        kind: CredentialKind,
        workflow: &str,
        declaration: OriginDeclaration,
        node: Option<Node>,
        name: Option<&str>,
    ) {
        self.origins.entry(kind).or_default().insert(declaration_at(
            workflow,
            declaration,
            node,
            name,
        ));
    }

    fn add_unknown(
        &mut self,
        workflow: &str,
        declaration: UnknownDeclaration,
        node: Option<Node>,
        name: Option<&str>,
    ) {
        self.unknown
            .insert(declaration_at(workflow, declaration, node, name));
    }

    fn finish(self, workflows: Vec<String>) -> WorkflowCredentials {
        WorkflowCredentials {
            workflows,
            requirements: self
                .origins
                .into_iter()
                .map(|(kind, origins)| CredentialRequirement {
                    kind,
                    code: kind.offline_code(),
                    origins: in_report_order(origins),
                })
                .collect(),
            unknown: in_report_order(self.unknown),
        }
    }
}

fn declaration_at<K>(
    workflow: &str,
    declaration: K,
    node: Option<Node>,
    name: Option<&str>,
) -> Declaration<K> {
    Declaration {
        workflow: Some(workflow.to_owned()),
        declaration,
        node,
        name: name.map(str::to_owned),
    }
}

/// The remote whose host names the repository's provider.
const ORIGIN_REMOTE: &str = "origin";
/// The name of the repository's declaration when a project names its provider.
const PROJECT: &str = "project";

/// The kind a remote's host calls for, by the host alone (REQ-CREDREADY-12); no host, or one
/// of no known provider, calls for none.
fn remote_host_kind(host: Option<&str>) -> Option<CredentialKind> {
    let host = host?;
    RECOGNITION
        .remote_hosts
        .iter()
        .find(|(known, _)| host.eq_ignore_ascii_case(known))
        .map(|(_, kind)| *kind)
}

/// A declaration of the source root's repository, which belongs to no workflow file.
fn of_repository<K>(declaration: K, name: &str) -> Declaration<K> {
    Declaration {
        workflow: None,
        declaration,
        node: None,
        name: Some(name.to_owned()),
    }
}

/// The declarations of each workflow file in sort order, then those of no workflow file.
fn in_report_order<K>(declarations: BTreeSet<Declaration<K>>) -> Vec<Declaration<K>> {
    let (mut in_files, of_report): (Vec<_>, Vec<_>) = declarations
        .into_iter()
        .partition(|declaration| declaration.workflow.is_some());
    in_files.extend(of_report);
    in_files
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use um_execution::{CommandOutput, CommandProbeError, CommandRequest, CommandRunner};

    use super::super::git::{MAXIMUM_STDOUT_BYTES, SYSTEM_TIMEOUT};
    use super::{
        CredentialKind, Declaration, ForwardedVariable, OriginDeclaration, RepositorySource,
        UnknownDeclaration, classify_forwarded_variable, read_workflow_credentials,
    };

    /// The parts of a probe request a test compares.
    #[derive(Debug, Eq, PartialEq)]
    struct ProbeRequest {
        program: PathBuf,
        args: Vec<String>,
        timeout: Duration,
        maximum_stdout_bytes: usize,
        clear_environment: bool,
        environment: usize,
        current_directory: Option<PathBuf>,
    }

    /// Answers every request with one result and records what it was asked to run.
    struct FakeGit {
        result: Result<CommandOutput, CommandProbeError>,
        requests: Mutex<Vec<ProbeRequest>>,
    }

    impl FakeGit {
        fn answering(result: Result<CommandOutput, CommandProbeError>) -> Arc<Self> {
            Arc::new(Self {
                result,
                requests: Mutex::new(Vec::new()),
            })
        }
    }

    impl CommandRunner for FakeGit {
        fn run(&self, command: CommandRequest<'_>) -> Result<CommandOutput, CommandProbeError> {
            self.requests.lock().unwrap().push(ProbeRequest {
                program: command.program.to_path_buf(),
                args: command.args.iter().map(|arg| (*arg).to_owned()).collect(),
                timeout: command.timeout,
                maximum_stdout_bytes: command.maximum_stdout_bytes,
                clear_environment: command.clear_environment,
                environment: command.environment.len(),
                current_directory: command.current_directory.map(Path::to_path_buf),
            });
            self.result.clone()
        }
    }

    /// A Git that is not installed: the repository is unknown.
    fn no_git() -> RepositorySource {
        RepositorySource::OriginRemote(FakeGit::answering(Err(CommandProbeError::CommandNotFound)))
    }

    fn printed(success: bool, stdout: &str, truncated: bool) -> CommandOutput {
        CommandOutput {
            success,
            stdout: stdout.as_bytes().to_vec(),
            truncated,
        }
    }

    const ROOT: &str = r#"schemaVersion: 1
requires:
  tools: [git, gh, glab, python3]
bindings:
  connection: {kind: connection}
  linearTeam: {kind: linear.team}
  done: {kind: linear.state, default: Done}
  repository: {kind: github.repository}
mirror:
  linear:
    issue: inputs.ticket
    onOutcome:
      succeeded: {state: $done}
environmentPassthrough: [LINEAR_API_KEY]
inputs:
  ticket:
    kind: text
steps:
  build:
    kind: cmd
    command:
      argv: ["./build"]
"#;

    const CHILD: &str = r#"schemaVersion: 1
environmentPassthrough: [OPENAI_API_KEY]
steps:
  build:
    kind: cmd
    command:
      argv: ["./build"]
"#;

    fn parent(environment: &str) -> String {
        format!(
            r#"schemaVersion: 1
environmentPassthrough: [{environment}]
steps:
  build:
    kind: workflow
    workflow: child.yaml
    on:
      succeeded: exit.succeeded
      failed: exit.failed
"#
        )
    }

    // REQ-CREDREADY-3
    #[test]
    fn a_file_reached_from_two_named_files_is_read_once() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        fs::write(root.join("first.yaml"), parent("LINEAR_API_KEY")).unwrap();
        fs::write(root.join("second.yaml"), parent("GITHUB_PUBLISH_TOKEN")).unwrap();
        fs::write(root.join("child.yaml"), CHILD).unwrap();

        let credentials = read_workflow_credentials(
            &root,
            &[
                root.join("first.yaml"),
                root.join("second.yaml"),
                root.join("child.yaml"),
            ],
            no_git(),
        )
        .unwrap();

        assert_eq!(
            credentials.workflows,
            ["child.yaml", "first.yaml", "second.yaml"]
        );
        let codex = credentials
            .requirements
            .iter()
            .filter(|requirement| requirement.kind == CredentialKind::Codex)
            .collect::<Vec<_>>();
        assert_eq!(codex.len(), 1);
        assert_eq!(codex[0].origins.len(), 1);
        assert_eq!(codex[0].origins[0].workflow.as_deref(), Some("child.yaml"));
        assert_eq!(
            credentials
                .unknown
                .iter()
                .filter(|entry| entry.workflow.as_deref() == Some("child.yaml"))
                .count(),
            1
        );
        assert_eq!(
            credentials
                .requirements
                .iter()
                .map(|requirement| requirement.kind)
                .collect::<Vec<_>>(),
            [
                CredentialKind::Github,
                CredentialKind::Linear,
                CredentialKind::Codex
            ]
        );
    }

    fn origin(
        declaration: OriginDeclaration,
        name: Option<&str>,
    ) -> Declaration<OriginDeclaration> {
        Declaration {
            workflow: Some("root.yaml".to_owned()),
            declaration,
            node: None,
            name: name.map(str::to_owned),
        }
    }

    // REQ-CREDREADY-11, REQ-CREDREADY-3
    #[test]
    fn a_root_workflow_calls_for_providers_by_bindings_mirror_and_tools() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        fs::write(root.join("root.yaml"), ROOT).unwrap();

        let credentials =
            read_workflow_credentials(&root, &[root.join("root.yaml")], no_git()).unwrap();

        let requirements = credentials
            .requirements
            .iter()
            .map(|requirement| (requirement.kind, requirement.origins.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            requirements,
            [
                (
                    CredentialKind::Github,
                    vec![
                        origin(OriginDeclaration::Binding, Some("repository")),
                        origin(OriginDeclaration::RequiredTool, Some("gh")),
                    ]
                ),
                (
                    CredentialKind::Gitlab,
                    vec![origin(OriginDeclaration::RequiredTool, Some("glab"))]
                ),
                (
                    CredentialKind::Linear,
                    vec![
                        origin(OriginDeclaration::ForwardedVariable, Some("LINEAR_API_KEY")),
                        origin(OriginDeclaration::Binding, Some("done")),
                        origin(OriginDeclaration::Binding, Some("linearTeam")),
                        origin(OriginDeclaration::Mirror, None),
                    ]
                ),
            ]
        );
        let unknown = credentials
            .unknown
            .iter()
            .map(|entry| (entry.declaration, entry.name.as_deref()))
            .collect::<Vec<_>>();
        assert_eq!(
            unknown,
            [
                (UnknownDeclaration::Command, None),
                (UnknownDeclaration::ConnectionBinding, Some("connection")),
                (UnknownDeclaration::Repository, Some("origin")),
            ]
        );
    }

    const COMMAND_ONLY: &str = r#"schemaVersion: 1
steps:
  build:
    kind: cmd
    command:
      argv: ["./build"]
"#;

    fn repository<K>(declaration: K) -> Declaration<K> {
        Declaration {
            workflow: None,
            declaration,
            node: None,
            name: Some("origin".to_owned()),
        }
    }

    // REQ-CREDREADY-12, INV-HARNESS-17
    #[test]
    fn the_origin_remote_names_the_repository_provider_by_host_alone() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        fs::write(root.join("main.yaml"), COMMAND_ONLY).unwrap();
        let secret_address = "https://user:synthetic-secret@github.com/o/r.git";

        for (address, result, provider) in [
            (
                "https://github.com/o/r.git",
                Ok(printed(true, "https://github.com/o/r.git\n", false)),
                Some(CredentialKind::Github),
            ),
            (
                "git@github.com:o/r.git",
                Ok(printed(true, "git@github.com:o/r.git\n", false)),
                Some(CredentialKind::Github),
            ),
            (
                "ssh://git@github.com/o/r",
                Ok(printed(true, "ssh://git@github.com/o/r\n", false)),
                Some(CredentialKind::Github),
            ),
            (
                secret_address,
                Ok(printed(true, &format!("{secret_address}\n"), false)),
                Some(CredentialKind::Github),
            ),
            (
                "https://gitlab.com/g/p.git",
                Ok(printed(true, "https://GitLab.com/g/p.git\n", false)),
                Some(CredentialKind::Gitlab),
            ),
            (
                "https://git.example.test/o/r.git",
                Ok(printed(true, "https://git.example.test/o/r.git\n", false)),
                None,
            ),
            (
                secret_address,
                Ok(printed(false, &format!("{secret_address}\n"), false)),
                None,
            ),
            (
                secret_address,
                Err(CommandProbeError::CommandNotFound),
                None,
            ),
            (secret_address, Err(CommandProbeError::Timeout), None),
            (
                secret_address,
                Ok(printed(true, secret_address, true)),
                None,
            ),
        ] {
            let git = FakeGit::answering(result);

            let credentials = read_workflow_credentials(
                &root,
                &[root.join("main.yaml")],
                RepositorySource::OriginRemote(git.clone()),
            )
            .unwrap();

            assert_eq!(
                *git.requests.lock().unwrap(),
                [ProbeRequest {
                    program: PathBuf::from("git"),
                    args: vec![
                        "remote".to_owned(),
                        "get-url".to_owned(),
                        "origin".to_owned()
                    ],
                    timeout: SYSTEM_TIMEOUT,
                    maximum_stdout_bytes: MAXIMUM_STDOUT_BYTES,
                    clear_environment: false,
                    environment: 0,
                    current_directory: Some(root.clone()),
                }],
                "{address}"
            );
            let requirements = credentials
                .requirements
                .iter()
                .map(|requirement| (requirement.kind, requirement.origins.clone()))
                .collect::<Vec<_>>();
            let repository_unknown = credentials
                .unknown
                .iter()
                .filter(|entry| entry.declaration == UnknownDeclaration::Repository)
                .cloned()
                .collect::<Vec<_>>();
            match provider {
                Some(kind) => {
                    assert_eq!(
                        requirements,
                        [(kind, vec![repository(OriginDeclaration::Repository)])],
                        "{address}"
                    );
                    assert!(repository_unknown.is_empty(), "{address}");
                }
                None => {
                    assert!(requirements.is_empty(), "{address}");
                    assert_eq!(
                        repository_unknown,
                        [repository(UnknownDeclaration::Repository)],
                        "{address}"
                    );
                    // Entries of no workflow file follow the per-file entries.
                    assert_eq!(
                        credentials.unknown.last(),
                        Some(&repository(UnknownDeclaration::Repository))
                    );
                }
            }
            let debug = format!("{credentials:?}");
            assert!(!debug.contains("synthetic-secret"), "{debug}");
            assert!(!debug.contains(address), "{debug}");
            assert!(!debug.contains("/o/r"), "{debug}");
        }
    }

    // REQ-CREDREADY-5
    #[test]
    fn forwarded_variables_read_by_the_recognition_table() {
        for (name, reading) in [
            (
                "GITHUB_PUBLISH_TOKEN_FILE",
                ForwardedVariable::Calls(CredentialKind::Github),
            ),
            (
                "gh_api_token",
                ForwardedVariable::Calls(CredentialKind::Github),
            ),
            (
                "GITLAB_TOKEN",
                ForwardedVariable::Calls(CredentialKind::Gitlab),
            ),
            (
                "LINEAR_API_KEY",
                ForwardedVariable::Calls(CredentialKind::Linear),
            ),
            (
                "ANTHROPIC_API_KEY",
                ForwardedVariable::Calls(CredentialKind::ClaudeCode),
            ),
            (
                "CLAUDE_CONFIG_DIR",
                ForwardedVariable::Calls(CredentialKind::ClaudeCode),
            ),
            (
                "OPENAI_API_KEY",
                ForwardedVariable::Calls(CredentialKind::Codex),
            ),
            (
                "CODEX_HOME",
                ForwardedVariable::Calls(CredentialKind::Codex),
            ),
            ("BUILDKITE_API_TOKEN", ForwardedVariable::Unknown),
            ("MERGIFY_TOKEN", ForwardedVariable::Unknown),
            ("LINEAR_PROXY_URL", ForwardedVariable::Ignored),
            ("PI_CODING_AGENT_DIR", ForwardedVariable::Ignored),
            ("CARGO_TARGET_DIR", ForwardedVariable::Ignored),
            ("KEYBOARD_LAYOUT", ForwardedVariable::Ignored),
        ] {
            assert_eq!(classify_forwarded_variable(name), reading, "{name}");
        }
    }
}
