//! Validation of a workflow tree: a directory with `workflow.yaml` at its root
//! (REQ-WFDEF-8 to REQ-WFDEF-16). Nothing here executes a step or acts on the root-only
//! blocks; it checks them, computes the tree digest and keeps the walked files.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ring::digest::{Context, SHA256};
use serde::Serialize;

use super::document::{Binding, BindingKind, RootBlocks, Trigger, TriggerControlAction};
use super::mirror::{MirrorBlock, MirrorGuard, MirrorGuardError, Template, hook_variables};
use super::presentation_feed::normalize_terminal_scalar;
use super::rejection::{RejectionDiagnostic, RejectionLocation, human_resolution_remedy};
use super::resolution::{
    ContentDigestAlgorithm, ResolutionFailure, ResolvedWorkflow, WorkflowContentDigest,
    resolve_workflow_file,
};
use super::schema_common::lowercase_hex;
use super::validated::{
    Scheduling, ValidatedEdgeTarget, ValidatedRecoveryHandler, ValidatedStep, ValidatedWorkflow,
    WorkflowValueType,
};

const ROOT_WORKFLOW: &str = "workflow.yaml";
const TREE_DIGEST_DOMAIN: &[u8] = b"um.workflow.tree.v1\0";
const MAX_TREE_BYTES: u64 = 64 * 1024 * 1024;

/// A tree that passed validation, with what `um workflow definition validate` reports.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedTree {
    /// The canonical tree root.
    pub root: PathBuf,
    pub digest: WorkflowContentDigest,
    pub name: Option<String>,
    pub version: Option<String>,
    /// `workflow.yaml` first, then the other workflow files in bytewise order.
    pub workflow_files: Vec<String>,
    pub programs: Vec<String>,
    pub required_tools: Vec<String>,
    pub triggers: BTreeMap<String, TreeTrigger>,
    pub bindings: BTreeMap<String, Binding>,
    /// Every file the walk kept, by tree-relative path, for the run copy.
    pub files: BTreeMap<String, TreeFile>,
}

/// One kept file of a tree: its bytes and whether its mode has the owner execute bit.
/// The digest reads only the bytes (REQ-WFDEF-11).
#[derive(Clone, Eq, PartialEq)]
pub struct TreeFile {
    pub bytes: Arc<[u8]>,
    pub executable: bool,
}

impl fmt::Debug for TreeFile {
    /// The length, never the contents (REQ-WFDEF-15).
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TreeFile")
            .field("length", &self.bytes.len())
            .field("executable", &self.executable)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeTrigger {
    pub kind: TreeTriggerKind,
    /// The binding reference as written, `$` and the binding key.
    pub source: String,
    pub event: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TreeTriggerKind {
    Start,
    Control,
}

impl TreeTriggerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Control => "control",
        }
    }
}

/// One failure in a tree, with a stable code and what it concerns (REQ-WFDEF-15).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeDiagnostic {
    pub code: &'static str,
    pub location: TreeLocation,
    resolution: Option<ResolutionFailure>,
}

impl TreeDiagnostic {
    fn new(code: &'static str, location: TreeLocation) -> Self {
        Self {
            code,
            location,
            resolution: None,
        }
    }

    fn resolution(path: &str, failure: ResolutionFailure) -> Self {
        Self {
            code: RejectionDiagnostic::from_resolution(&failure).code,
            location: TreeLocation::new("workflow").path(path),
            resolution: Some(failure),
        }
    }

    /// Where inside the workflow file a per-file resolution failure sits.
    pub fn workflow_location(&self) -> Option<RejectionLocation<'_>> {
        self.resolution
            .as_ref()
            .map(|failure| RejectionDiagnostic::from_resolution(failure).location)
    }

    /// The remedy for a per-file resolution failure or a command named by path.
    pub fn remedy(&self) -> Option<String> {
        match self.code {
            "command_program_is_path" => Some(
                "Name the program bare; the root's `programs` directories lead the step PATH."
                    .to_owned(),
            ),
            _ => self.resolution.as_ref().map(human_resolution_remedy),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct TreeLocation {
    /// `tree`, `workflow`, `trigger`, `mirror`, `program` or `exclude`.
    kind: &'static str,
    /// A tree-relative path.
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    /// The step, finalizer or recovered step a command belongs to.
    #[serde(skip_serializing_if = "Option::is_none")]
    step: Option<String>,
    /// A root-only key found outside the root, the key holding a command's `argv`, or the
    /// key inside `mirror.linear`, such as `onOutcome.unless`.
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    binding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<String>,
    /// The `mirror.linear.onOutcome` entry a mirror failure concerns.
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    program: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pattern: Option<String>,
}

impl TreeLocation {
    fn new(kind: &'static str) -> Self {
        Self {
            kind,
            ..Self::default()
        }
    }

    fn path(mut self, path: &str) -> Self {
        self.path = Some(path.to_owned());
        self
    }

    fn key(mut self, key: &'static str) -> Self {
        self.key = Some(key);
        self
    }

    fn step(mut self, step: &str) -> Self {
        self.step = Some(step.to_owned());
        self
    }

    fn trigger(name: &str) -> Self {
        Self {
            trigger: Some(name.to_owned()),
            ..Self::new("trigger")
        }
    }

    fn binding(mut self, key: &str) -> Self {
        self.binding = Some(key.to_owned());
        self
    }

    fn input(mut self, input: &str) -> Self {
        self.input = Some(input.to_owned());
        self
    }

    fn outcome(mut self, outcome: &str) -> Self {
        self.outcome = Some(outcome.to_owned());
        self
    }
}

impl fmt::Display for TreeLocation {
    /// The kind and what it names, then any detail: `trigger start (binding team)` or
    /// `workflow design.yaml (step design) (key command.argv)`.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind)?;
        let named = [&self.path, &self.trigger, &self.program, &self.pattern];
        for value in named.into_iter().flatten() {
            write!(
                formatter,
                " {}",
                normalize_terminal_scalar(value.as_bytes())
            )?;
        }
        let details = [
            ("step", self.step.as_deref()),
            ("key", self.key),
            ("binding", self.binding.as_deref()),
            ("input", self.input.as_deref()),
            ("outcome", self.outcome.as_deref()),
        ];
        for (name, value) in details {
            if let Some(value) = value {
                let value = normalize_terminal_scalar(value.as_bytes());
                write!(formatter, " ({name} {value})")?;
            }
        }
        Ok(())
    }
}

/// Whether `root` is a tree (REQ-WFDEF-8): a regular `workflow.yaml` sits directly under it.
/// `um workflow run create --path` admits such a directory from the tree (REQ-LOCALRUN-169).
pub fn is_workflow_tree(root: &Path) -> bool {
    fs::symlink_metadata(root.join(ROOT_WORKFLOW)).is_ok_and(|metadata| metadata.is_file())
}

/// Validates the tree rooted at `root`, collecting every failure; a root workflow that
/// does not resolve ends validation with its failure alone.
pub fn validate_workflow_tree(root: &Path) -> Result<ValidatedTree, Vec<TreeDiagnostic>> {
    let root_missing = || {
        vec![TreeDiagnostic::new(
            "tree_root_missing",
            TreeLocation::new("tree"),
        )]
    };
    let root = fs::canonicalize(root).map_err(|_| root_missing())?;
    if fs::symlink_metadata(root.join(ROOT_WORKFLOW)).is_err() {
        return Err(root_missing());
    }
    let root_workflow = resolve_workflow_file(&root, &root.join(ROOT_WORKFLOW))
        .map_err(|failure| vec![TreeDiagnostic::resolution(ROOT_WORKFLOW, failure)])?;
    let blocks = root_workflow.definition.root.clone().unwrap_or_default();

    let mut diagnostics = Vec::new();
    let excludes = parse_excludes(&blocks.exclude, &mut diagnostics);
    let mut walk = TreeWalk::default();
    walk.directory(&root, None, &excludes);
    diagnostics.append(&mut walk.diagnostics);

    let workflow_files = workflow_files(&walk.files);
    check_commands(ROOT_WORKFLOW, &root_workflow.definition, &mut diagnostics);
    for file in workflow_files.iter().skip(1) {
        match resolve_workflow_file(&root, &root.join(file)) {
            Ok(workflow) => {
                check_stage_file(file, &workflow, &mut diagnostics);
                check_commands(file, &workflow.definition, &mut diagnostics);
            }
            Err(failure) => diagnostics.push(TreeDiagnostic::resolution(file, failure)),
        }
    }
    check_triggers(&root_workflow, &blocks, &mut diagnostics);
    if let Some(mirror) = &blocks.mirror {
        check_mirror(&root_workflow, &blocks, mirror, &mut diagnostics);
    }
    for program in &blocks.programs {
        if !walk.directories.contains(program.trim_end_matches('/')) {
            diagnostics.push(TreeDiagnostic::new(
                "program_directory_missing",
                TreeLocation {
                    program: Some(program.clone()),
                    ..TreeLocation::new("program")
                },
            ));
        }
    }
    if !diagnostics.is_empty() {
        return Err(diagnostics);
    }

    let definition = root_workflow.definition;
    Ok(ValidatedTree {
        root,
        digest: digest_tree(&walk.files),
        name: definition.name,
        version: definition.version,
        workflow_files,
        required_tools: blocks
            .requires
            .map(|requires| requires.tools)
            .unwrap_or_default(),
        programs: blocks.programs,
        triggers: blocks
            .triggers
            .iter()
            .map(|(name, trigger)| (name.clone(), TreeTrigger::from(trigger)))
            .collect(),
        bindings: blocks.bindings,
        files: walk.files,
    })
}

impl From<&Trigger> for TreeTrigger {
    fn from(trigger: &Trigger) -> Self {
        let (kind, rule) = match trigger {
            Trigger::Start { on, .. } => (TreeTriggerKind::Start, on),
            Trigger::Control { on, .. } => (TreeTriggerKind::Control, on),
        };
        Self {
            kind,
            source: rule.source.clone(),
            event: rule.event.clone(),
        }
    }
}

/// `workflow.yaml` and every `.yaml` or `.yml` file directly under the root (REQ-WFDEF-8).
fn workflow_files(files: &BTreeMap<String, TreeFile>) -> Vec<String> {
    let mut workflow_files = vec![ROOT_WORKFLOW.to_owned()];
    workflow_files.extend(
        files
            .keys()
            .filter(|path| is_workflow_file_beside_root(path))
            .cloned(),
    );
    workflow_files
}

/// A `.yaml` or `.yml` file directly under the root other than `workflow.yaml`: a workflow
/// file of the tree (REQ-WFDEF-8) and the only file a `workflow` step may name
/// (REQ-WFFORMAT-89).
pub(crate) fn is_workflow_file_beside_root(path: &str) -> bool {
    !path.contains('/')
        && path != ROOT_WORKFLOW
        && (path.ends_with(".yaml") || path.ends_with(".yml"))
}

fn check_stage_file(
    file: &str,
    workflow: &ResolvedWorkflow,
    diagnostics: &mut Vec<TreeDiagnostic>,
) {
    let definition = &workflow.definition;
    let blocks = definition.root.as_ref();
    let present = [
        ("name", definition.name.is_some()),
        ("version", definition.version.is_some()),
        ("subject", definition.subject.is_some()),
        (
            "programs",
            blocks.is_some_and(|blocks| !blocks.programs.is_empty()),
        ),
        (
            "exclude",
            blocks.is_some_and(|blocks| !blocks.exclude.is_empty()),
        ),
        (
            "requires",
            blocks.is_some_and(|blocks| blocks.requires.is_some()),
        ),
        (
            "bindings",
            blocks.is_some_and(|blocks| !blocks.bindings.is_empty()),
        ),
        (
            "triggers",
            blocks.is_some_and(|blocks| !blocks.triggers.is_empty()),
        ),
        (
            "mirror",
            blocks.is_some_and(|blocks| blocks.mirror.is_some()),
        ),
    ];
    for (key, _) in present.into_iter().filter(|(_, present)| *present) {
        diagnostics.push(TreeDiagnostic::new(
            "root_only_block_outside_root",
            TreeLocation::new("workflow").path(file).key(key),
        ));
    }
}

/// A program in a tree is named bare (REQ-WFDEF-16): no `cmd` step, `cmd` finalizer or
/// command recovery handler may name its `argv[0]` by path.
fn check_commands(file: &str, workflow: &ValidatedWorkflow, diagnostics: &mut Vec<TreeDiagnostic>) {
    let finalizers = workflow
        .finalizers
        .iter()
        .map(|(name, finalizer)| (name, &finalizer.body));
    let commands = workflow
        .steps
        .iter()
        .chain(finalizers)
        .filter_map(|(name, step)| match step {
            ValidatedStep::Command(command) => {
                Some((name, "command.argv", command.argv.as_slice()))
            }
            ValidatedStep::Agent(_) | ValidatedStep::Workflow(_) => None,
        });
    let handlers = workflow.recoveries.iter().filter_map(|(name, recovery)| {
        match recovery.as_ref()?.handler.as_ref()? {
            ValidatedRecoveryHandler::Command { argv, .. } => {
                Some((name, "recovery.handler.command.argv", argv.as_slice()))
            }
            ValidatedRecoveryHandler::Agent { .. } => None,
        }
    });
    for (step, key, argv) in commands.chain(handlers) {
        if argv.first().is_some_and(|program| program.contains('/')) {
            diagnostics.push(TreeDiagnostic::new(
                "command_program_is_path",
                TreeLocation::new("workflow").path(file).step(step).key(key),
            ));
        }
    }
}

fn check_triggers(
    root_workflow: &ResolvedWorkflow,
    blocks: &RootBlocks,
    diagnostics: &mut Vec<TreeDiagnostic>,
) {
    let required_inputs = root_workflow.required_inputs();
    for (name, trigger) in &blocks.triggers {
        let (rule, when) = match trigger {
            Trigger::Start { on, .. } => (on, None),
            Trigger::Control { on, control } => (
                on,
                match control {
                    TriggerControlAction::Conditional { when, .. } => Some(when),
                    TriggerControlAction::Named(_) => None,
                },
            ),
        };
        let source = rule.source.strip_prefix('$').unwrap_or(&rule.source);
        match blocks.bindings.get(source) {
            None => diagnostics.push(TreeDiagnostic::new(
                "binding_undeclared",
                TreeLocation::trigger(name).binding(source),
            )),
            Some(binding) if binding.kind != BindingKind::Connection => {
                diagnostics.push(TreeDiagnostic::new(
                    "binding_kind_mismatch",
                    TreeLocation::trigger(name).binding(source),
                ));
            }
            Some(_) => {}
        }

        let expressions = [rule.condition.as_ref(), when].into_iter().flatten();
        let mut references = BTreeSet::new();
        let mut literal = false;
        for expression in expressions {
            literal |= expression.contains(['"', '\'']);
            references.extend(binding_references(expression));
        }
        if literal {
            diagnostics.push(TreeDiagnostic::new(
                "literal_where_binding_expected",
                TreeLocation::trigger(name),
            ));
        }
        for key in references
            .into_iter()
            .filter(|key| !blocks.bindings.contains_key(*key))
        {
            diagnostics.push(TreeDiagnostic::new(
                "binding_undeclared",
                TreeLocation::trigger(name).binding(key),
            ));
        }

        if let Trigger::Start { inputs, .. } = trigger {
            for input in inputs
                .keys()
                .filter(|input| !root_workflow.declared_inputs().contains_key(*input))
            {
                diagnostics.push(TreeDiagnostic::new(
                    "trigger_input_undeclared",
                    TreeLocation::trigger(name).input(input),
                ));
            }
            for input in required_inputs
                .keys()
                .filter(|input| !inputs.contains_key(*input))
            {
                diagnostics.push(TreeDiagnostic::new(
                    "trigger_input_missing",
                    TreeLocation::trigger(name).input(input),
                ));
            }
        }
    }
}

/// The root's `mirror` block (REQ-WFFORMAT-101): its issue input, outcome keys, states,
/// guards and template variables, each failure at the `mirror` location with its key.
fn check_mirror(
    root_workflow: &ResolvedWorkflow,
    blocks: &RootBlocks,
    mirror: &MirrorBlock,
    diagnostics: &mut Vec<TreeDiagnostic>,
) {
    let location = |key| TreeLocation::new("mirror").key(key);
    match root_workflow.declared_inputs().get(&mirror.issue) {
        None => diagnostics.push(TreeDiagnostic::new(
            "mirror_issue_input_undeclared",
            location("issue").input(&mirror.issue),
        )),
        Some(WorkflowValueType::Text) => {}
        Some(_) => diagnostics.push(TreeDiagnostic::new(
            "mirror_issue_input_not_text",
            location("issue").input(&mirror.issue),
        )),
    }

    for (hook, template) in mirror.templates() {
        let variables = hook_variables(hook);
        let resolves = Template::parse(template).is_ok_and(|template| {
            template
                .variables()
                .all(|variable| variables.contains(&variable))
        });
        if !resolves {
            diagnostics.push(TreeDiagnostic::new(
                "mirror_variable_unknown",
                location(hook.template_key()),
            ));
        }
    }

    let exits = root_exit_outcomes(&root_workflow.definition);
    for (outcome, state) in &mirror.on_outcome {
        let location = |key| location(key).outcome(outcome);
        if !exits.contains(outcome.as_str())
            && !MIRRORED_ENGINE_OUTCOMES.contains(&outcome.as_str())
        {
            diagnostics.push(TreeDiagnostic::new(
                "mirror_outcome_undeclared",
                location("onOutcome"),
            ));
        }
        check_state_binding(
            blocks,
            &state.state,
            location("onOutcome.state"),
            diagnostics,
        );
        let Some(unless) = &state.unless else {
            continue;
        };
        match MirrorGuard::parse(unless) {
            Ok(guard) => check_state_binding(
                blocks,
                &guard.binding,
                location("onOutcome.unless"),
                diagnostics,
            ),
            Err(error) => diagnostics.push(TreeDiagnostic::new(
                match error {
                    MirrorGuardError::QuotedLiteral => "literal_where_binding_expected",
                    MirrorGuardError::Invalid => "mirror_guard_invalid",
                },
                location("onOutcome.unless"),
            )),
        }
    }
}

/// The outcomes besides the root's own that a mirror may map: `failed` and the engine
/// outcomes a local run reaches. `superseded` is never mirrored (REQ-WFFORMAT-101).
const MIRRORED_ENGINE_OUTCOMES: [&str; 3] = ["failed", "withdrawn", "canceled"];

/// A mirror state names a declared `linear.state` binding.
fn check_state_binding(
    blocks: &RootBlocks,
    key: &str,
    location: TreeLocation,
    diagnostics: &mut Vec<TreeDiagnostic>,
) {
    let code = match blocks.bindings.get(key) {
        None => "binding_undeclared",
        Some(binding) if binding.kind != BindingKind::LinearState => "binding_kind_mismatch",
        Some(_) => return,
    };
    diagnostics.push(TreeDiagnostic::new(code, location.binding(key)));
}

/// The outcomes a run of the root can exit with: its declared outcomes, the exits its edges
/// reach when it is edge-scheduled, or the implicit `succeeded` and `failed`.
fn root_exit_outcomes(workflow: &ValidatedWorkflow) -> BTreeSet<&str> {
    fn exits<'a>(target: &'a ValidatedEdgeTarget, outcomes: &mut BTreeSet<&'a str>) {
        match target {
            ValidatedEdgeTarget::Exit(outcome) => {
                outcomes.insert(outcome);
            }
            ValidatedEdgeTarget::Limited { else_, .. } => exits(else_, outcomes),
            ValidatedEdgeTarget::Wait { then, .. } => exits(then, outcomes),
            ValidatedEdgeTarget::Step(_) => {}
        }
    }
    if let Some(outcome) = &workflow.outcome {
        return outcome.names.iter().map(String::as_str).collect();
    }
    let mut outcomes = BTreeSet::from(["failed"]);
    match workflow.scheduling {
        Scheduling::Dataflow => {
            outcomes.insert("succeeded");
        }
        Scheduling::Edges { .. } => {
            for step in workflow.steps.values() {
                if let ValidatedStep::Workflow(step) = step {
                    step.edges
                        .values()
                        .for_each(|target| exits(target, &mut outcomes));
                }
            }
        }
    }
    outcomes
}

/// Each `$` and the identifier characters that follow it, scanned lexically.
fn binding_references(expression: &str) -> impl Iterator<Item = &str> {
    expression.match_indices('$').map(|(index, _)| {
        let rest = &expression[index + 1..];
        let end = rest
            .find(|character: char| !character.is_ascii_alphanumeric())
            .unwrap_or(rest.len());
        &rest[..end]
    })
}

fn parse_excludes(
    patterns: &[String],
    diagnostics: &mut Vec<TreeDiagnostic>,
) -> Vec<ExcludePattern> {
    let mut excludes = Vec::new();
    for pattern in patterns {
        match ExcludePattern::parse(pattern) {
            Some(exclude) => excludes.push(exclude),
            None => diagnostics.push(TreeDiagnostic::new(
                "exclude_pattern_invalid",
                TreeLocation {
                    pattern: Some(pattern.clone()),
                    ..TreeLocation::new("exclude")
                },
            )),
        }
    }
    if excludes
        .iter()
        .any(|exclude| exclude.excludes_file(ROOT_WORKFLOW))
    {
        diagnostics.push(TreeDiagnostic::new(
            "tree_root_excluded",
            TreeLocation::new("exclude").path(ROOT_WORKFLOW),
        ));
    }
    excludes
}

/// One `exclude` pattern in the documented subset of gitignore (REQ-WFDEF-12).
#[derive(Debug)]
struct ExcludePattern {
    segments: Vec<String>,
    /// A trailing `/`: the pattern names a directory and everything under it.
    directory: bool,
}

impl ExcludePattern {
    fn parse(pattern: &str) -> Option<Self> {
        if pattern.starts_with('/') || pattern.contains('!') {
            return None;
        }
        let (body, directory) = match pattern.strip_suffix('/') {
            Some(body) => (body, true),
            None => (pattern, false),
        };
        let segments = body.split('/').map(str::to_owned).collect::<Vec<_>>();
        if body.is_empty() || segments.iter().any(|segment| segment == "..") {
            return None;
        }
        Some(Self {
            segments,
            directory,
        })
    }

    fn excludes_file(&self, path: &str) -> bool {
        let parts = path.split('/').collect::<Vec<_>>();
        if self.directory {
            (1..parts.len()).any(|length| self.matches(&parts[..length]))
        } else {
            self.matches(&parts)
        }
    }

    fn excludes_directory(&self, path: &str) -> bool {
        self.directory && self.matches(&path.split('/').collect::<Vec<_>>())
    }

    /// A pattern without `/` matches the last segment; otherwise the whole path.
    fn matches(&self, parts: &[&str]) -> bool {
        match (self.segments.as_slice(), parts.last()) {
            ([segment], Some(last)) => segment_matches(segment, last),
            _ => segments_match(&self.segments, parts),
        }
    }
}

fn segments_match(pattern: &[String], parts: &[&str]) -> bool {
    match pattern.split_first() {
        None => parts.is_empty(),
        Some((first, rest)) if first == "**" => {
            (0..=parts.len()).any(|skipped| segments_match(rest, &parts[skipped..]))
        }
        Some((first, rest)) => parts.split_first().is_some_and(|(part, remaining)| {
            segment_matches(first, part) && segments_match(rest, remaining)
        }),
    }
}

/// `*` matches any run of characters within one segment.
fn segment_matches(pattern: &str, text: &str) -> bool {
    let mut pieces = pattern.split('*');
    let first = pieces.next().unwrap_or_default();
    let Some(mut rest) = text.strip_prefix(first) else {
        return false;
    };
    let mut pieces = pieces.collect::<Vec<_>>();
    let Some(last) = pieces.pop() else {
        return rest.is_empty();
    };
    for piece in pieces {
        match rest.find(piece) {
            Some(index) => rest = &rest[index + piece.len()..],
            None => return false,
        }
    }
    rest.ends_with(last)
}

#[derive(Default)]
struct TreeWalk {
    files: BTreeMap<String, TreeFile>,
    directories: BTreeSet<String>,
    bytes: u64,
    too_large: bool,
    diagnostics: Vec<TreeDiagnostic>,
}

impl TreeWalk {
    fn directory(&mut self, directory: &Path, prefix: Option<&str>, excludes: &[ExcludePattern]) {
        let relative = |name: &str| {
            prefix.map_or_else(|| name.to_owned(), |prefix| format!("{prefix}/{name}"))
        };
        let entries = fs::read_dir(directory).and_then(|entries| {
            entries
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<Result<Vec<_>, _>>()
        });
        let Ok(mut names) = entries else {
            self.unreadable(prefix.unwrap_or("."));
            return;
        };
        names.sort();
        for name in names {
            let Some(utf8_name) = name.to_str() else {
                self.push("invalid_canonical_path", &relative(&name.to_string_lossy()));
                continue;
            };
            let path = relative(utf8_name);
            let host_path = directory.join(&name);
            let Ok(metadata) = fs::symlink_metadata(&host_path) else {
                self.unreadable(&path);
                continue;
            };
            if metadata.is_dir() {
                if !excludes
                    .iter()
                    .any(|exclude| exclude.excludes_directory(&path))
                {
                    self.directories.insert(path.clone());
                    self.directory(&host_path, Some(&path), excludes);
                }
            } else if !excludes.iter().any(|exclude| exclude.excludes_file(&path)) {
                if metadata.is_file() {
                    self.file(&host_path, path, &metadata);
                } else {
                    self.push("tree_entry_not_regular", &path);
                }
            }
        }
    }

    /// Reads a kept file and records whether its mode has the owner execute bit.
    fn file(&mut self, host_path: &Path, path: String, metadata: &fs::Metadata) {
        let executable = metadata.permissions().mode() & 0o100 != 0;
        self.bytes = self.bytes.saturating_add(metadata.len());
        if self.too_large || self.bytes > MAX_TREE_BYTES {
            if !self.too_large {
                self.too_large = true;
                self.diagnostics.push(TreeDiagnostic::new(
                    "tree_too_large",
                    TreeLocation::new("tree"),
                ));
            }
            let bytes = Arc::from(Vec::new());
            self.files.insert(path, TreeFile { bytes, executable });
            return;
        }
        match fs::read(host_path) {
            Ok(bytes) => {
                let bytes = Arc::from(bytes);
                self.files.insert(path, TreeFile { bytes, executable });
            }
            Err(_) => self.unreadable(&path),
        }
    }

    fn unreadable(&mut self, path: &str) {
        self.push("source_unavailable", path);
    }

    fn push(&mut self, code: &'static str, path: &str) {
        self.diagnostics.push(TreeDiagnostic::new(
            code,
            TreeLocation::new("tree").path(path),
        ));
    }
}

/// The tree digest (REQ-WFDEF-11): the content-closure framing under its own domain.
fn digest_tree(files: &BTreeMap<String, TreeFile>) -> WorkflowContentDigest {
    let mut context = Context::new(&SHA256);
    context.update(TREE_DIGEST_DOMAIN);
    context.update(&length_prefix(files.len()));
    for (path, file) in files {
        for bytes in [path.as_bytes(), &file.bytes] {
            context.update(&length_prefix(bytes.len()));
            context.update(bytes);
        }
    }
    WorkflowContentDigest {
        algorithm: ContentDigestAlgorithm::Sha256,
        value: lowercase_hex(context.finish().as_ref()),
    }
}

fn length_prefix(length: usize) -> [u8; 8] {
    // A tree is bounded at 64 MiB, so every count and length fits.
    u64::try_from(length).unwrap_or(u64::MAX).to_be_bytes()
}

#[cfg(test)]
mod tests;
