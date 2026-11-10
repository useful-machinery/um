use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use serde_json::Value;

use super::condition::{
    ConditionValueKind, JsonPointer, ResolvedOperand, ResolvedPredicate, ResolvedSelector,
};
use super::document::{
    Agent, AgentProfile, CommonNode, ConditionOperand, ConditionPredicate, EdgeTarget,
    ExportPresentation, FailurePolicy, FinalizerDefinition, ForEach, InputDeclaration, JsonDefault,
    MessageSource, NodeBody, Output, OutputReference, PresentationSource, RecoveryHandler,
    StepDefinition, StepRecovery, ValueReference, WaitKind, WaitRule, WorkflowDocument,
};
use super::evidence::{MAXIMUM_PREREQUISITES, Prerequisite};
use super::export_presentation::{Field, resolve_text};
use super::validated::{
    DeclaredInputs, ResolvedDirectPrerequisite, ResolvedOutputSource, ResolvedValueReference,
    ResolvedValueSource, Scheduling, ValidatedAgent, ValidatedAgentMessage, ValidatedAgentStep,
    ValidatedChildInput, ValidatedCommandStep, ValidatedCommonStep, ValidatedEdgeTarget,
    ValidatedExportPresentation, ValidatedFinalizer, ValidatedForEach, ValidatedHarness,
    ValidatedInputDefault, ValidatedMessageSource, ValidatedOutcome, ValidatedOutput,
    ValidatedPresentationField, ValidatedRecoveryHandler, ValidatedStep, ValidatedStepRecovery,
    ValidatedWorkflow, ValidatedWorkflowStep, WorkflowNode, WorkflowNodeRole, WorkflowValueType,
};
use super::wait::{Operand, parse_where};

const MAXIMUM_WORKFLOW_NODES: usize = 256;
const MAXIMUM_CONDITION_DEPTH: usize = 16;
const MAXIMUM_CONDITION_NODES: usize = 256;
const MAXIMUM_CONDITION_CHILDREN: usize = 64;
const MAXIMUM_WORKFLOW_CONDITION_NODES: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ValidationFailureKind {
    MissingDependency,
    SelfDependency,
    DependencyCycle,
    UnknownAgentProfile,
    UnknownReplacementProfile,
    UnknownInput,
    UnknownOutputStep,
    UnknownOutput,
    MessageTypeMismatch,
    TerminalOutputReference,
    ExcessAgentResponseOutput,
    ExcessAgentResultOutput,
    ConflictingAgentValueOutputs,
    ExcessWorkspaceOutput,
    DuplicateOutputPath,
    AdvisoryDataDependency,
    InvalidExportTarget,
    AdvisoryExportTarget,
    InvalidExportPresentation,
    InvalidExportPresentationTarget,
    InvalidOutcomeDeclaration,
    InvalidOutcomeSource,
    TooManyNodes,
    DuplicateNodeId,
    CrossPhaseOutputReference,
    InvalidFinalizerAfterTarget,
    IncompatibleFinalizerTriggers,
    FinalizerExportTrigger,
    TooManyPrerequisites,
    InvalidCondition,
    InvalidConditionReference,
    InvalidConditionType,
    InvalidJsonPointer,
    ConditionDepthExceeded,
    ConditionNodeLimitExceeded,
    ConditionChildLimitExceeded,
    WorkflowConditionNodeLimitExceeded,
    ChildWorkflowNotInTree,
    ChildWorkflowUnavailable,
    ChildWorkflowInvalid,
    ChildWorkflowNested,
    ChildWorkflowRootBlocks,
    ChildInputUndeclared,
    ChildInputMissing,
    ChildInputTypeMismatch,
    ChildInputUnsupported,
    ChildExportReserved,
    EdgeOutcomeUndeclared,
    EdgeOutcomeMissing,
    EdgeTargetUnknown,
    EdgeLimitTargetInvalid,
    EdgeExitUndeclared,
    EdgeSchedulingKind,
    StepUnreachable,
    WaitHandBackRequired,
    WaitHandBackUnsupported,
    WaitRuleRequired,
    WaitRuleUnsupported,
    WaitRuleInvalid,
    WaitRuleReferenceInvalid,
    ControlReferenceRequiresOptional,
    ForEachReferenceInvalid,
    ForEachEmptyUndeclared,
    ItemReferenceRequiresForEach,
    SubjectInputUndeclared,
    SubjectInputNotText,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidationLocation {
    Workflow { key: &'static str },
    WorkflowGraph,
    WorkflowNamespace,
    Step { step: String },
    AgentProfileReplacement { profile: String },
    AgentProfileReference { step: String },
    RecoveryAgentProfileReference { step: String },
    FinalizerAgentProfileReference { finalizer: String },
    StepDependency { step: String, index: usize },
    StepInput { step: String, input: String },
    MessageText { step: String, index: usize },
    MessageAttachment { step: String, index: usize },
    StepOutput { step: String, output: String },
    StepCondition { step: String },
    StepWorkflow { step: String },
    StepEdge { step: String, outcome: String },
    StepForEach { step: String },
    FinalizerAfter { finalizer: String, index: usize },
    FinalizerInput { finalizer: String, input: String },
    FinalizerMessageText { finalizer: String, index: usize },
    FinalizerMessageAttachment { finalizer: String, index: usize },
    FinalizerOutput { finalizer: String, output: String },
    FinalizerCondition { finalizer: String },
    Export { name: String },
    Outcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidationFailure {
    kind: ValidationFailureKind,
    location: ValidationLocation,
}

impl ValidationFailure {
    pub(crate) fn kind(&self) -> ValidationFailureKind {
        self.kind
    }

    pub(crate) fn location(&self) -> &ValidationLocation {
        &self.location
    }

    fn new(kind: ValidationFailureKind, location: ValidationLocation) -> Self {
        Self { kind, location }
    }
}

impl fmt::Display for ValidationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "workflow semantic failure: {:?}", self.kind)
    }
}

impl std::error::Error for ValidationFailure {}

/// What a parent's validation needs to know of a child workflow file (REQ-WFFORMAT-89 to
/// REQ-WFFORMAT-92); resolution builds one per file a `workflow` step names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChildWorkflowSummary {
    /// The declared outcomes, or the implicit `succeeded` and `failed`.
    pub(crate) outcomes: Vec<String>,
    pub(crate) exports: BTreeMap<String, WorkflowValueType>,
    /// Each declared input's kind and whether a run may omit it.
    pub(crate) inputs: BTreeMap<String, (WorkflowValueType, bool)>,
    pub(crate) has_workflow_steps: bool,
    pub(crate) has_root_blocks: bool,
    /// The canonical paths the child's resolution reached, its own file included; empty
    /// for a nested child, which is not resolved.
    pub(crate) paths: BTreeSet<String>,
}

impl ChildWorkflowSummary {
    pub(crate) fn of(child: &ValidatedWorkflow, paths: BTreeSet<String>) -> Self {
        Self {
            outcomes: child.outcome.as_ref().map_or_else(
                || vec!["succeeded".to_owned(), "failed".to_owned()],
                |outcome| outcome.names.clone(),
            ),
            exports: child
                .exports
                .iter()
                .map(|(name, source)| (name.clone(), source.value_type))
                .collect(),
            inputs: child
                .declared_inputs
                .iter()
                .map(|(name, value_type)| {
                    (
                        name.clone(),
                        (*value_type, child.input_may_be_omitted(name)),
                    )
                })
                .collect(),
            has_workflow_steps: child
                .steps
                .values()
                .any(|step| matches!(step, ValidatedStep::Workflow(_))),
            has_root_blocks: child.root.is_some()
                || child.name.is_some()
                || child.version.is_some()
                || child.subject.is_some(),
            paths,
        }
    }

    /// A child that itself declares a `workflow` step; nothing else of it is read, since
    /// it cannot be validated alone.
    pub(crate) fn nested() -> Self {
        Self {
            outcomes: Vec::new(),
            exports: BTreeMap::new(),
            inputs: BTreeMap::new(),
            has_workflow_steps: true,
            has_root_blocks: false,
            paths: BTreeSet::new(),
        }
    }
}

struct ValidatedGraph {
    direct_prerequisites: BTreeMap<String, Vec<ResolvedDirectPrerequisite>>,
    presentation_order: Vec<String>,
}

#[cfg(test)]
pub(crate) fn validate(document: WorkflowDocument) -> Result<ValidatedWorkflow, ValidationFailure> {
    validate_with_replacement(document, None)
}

/// Validate a single file: a `workflow` step in it has no child summary and fails
/// `child_workflow_unavailable`.
#[cfg(test)]
pub(crate) fn validate_with_replacement(
    document: WorkflowDocument,
    replacement: Option<&BTreeMap<String, AgentProfile>>,
) -> Result<ValidatedWorkflow, ValidationFailure> {
    validate_with_children(document, replacement, &BTreeMap::new())
}

/// Validate a document, replacing declared agent profiles as a whole by name.
///
/// Replacement happens before any step, finalizer, or recovery-handler reference is
/// pinned, so each reference to a replaced name pins the replacement. Decoding has
/// already typed every declared and replacement profile; unknown names are rejected in
/// ascending name order. `children` summarizes each child file a `workflow` step names,
/// keyed by the step's `workflow` path.
pub(crate) fn validate_with_children(
    mut document: WorkflowDocument,
    replacement: Option<&BTreeMap<String, AgentProfile>>,
    children: &BTreeMap<String, ChildWorkflowSummary>,
) -> Result<ValidatedWorkflow, ValidationFailure> {
    validate_node_namespace(&document)?;
    let scheduling = validate_scheduling(&document)?;
    document.derived_outputs = derive_child_outputs(&document, children);
    validate_workflow_condition_bounds(&document)?;
    let mut agent_profiles = document
        .agent_profiles
        .iter()
        .map(|(name, profile)| (name.clone(), profile.harness.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut replaced_agent_profiles = BTreeMap::new();
    for (name, profile) in replacement.into_iter().flatten() {
        if !agent_profiles.contains_key(name) {
            return Err(ValidationFailure::new(
                ValidationFailureKind::UnknownReplacementProfile,
                ValidationLocation::AgentProfileReplacement {
                    profile: name.clone(),
                },
            ));
        }
        agent_profiles.insert(name.clone(), profile.harness.clone());
        replaced_agent_profiles.insert(name.clone(), profile.harness.clone());
    }
    let step_graph = validate_step_graph(&document)?;
    let finalizer_graph = validate_finalizer_graph(&document)?;
    validate_output_rules(&document)?;

    let mut declared_inputs = document
        .inputs
        .iter()
        .map(|(name, declaration)| {
            let value_type = match declaration {
                InputDeclaration::Text { .. } => WorkflowValueType::Text,
                InputDeclaration::Json { .. } => WorkflowValueType::Json,
                InputDeclaration::File { .. } => WorkflowValueType::File,
                InputDeclaration::Attachments { .. } => WorkflowValueType::AttachmentCollection,
            };
            (name.clone(), value_type)
        })
        .collect::<DeclaredInputs>();
    validate_subject(document.subject.as_deref(), &declared_inputs)?;
    let optional_inputs = document
        .inputs
        .iter()
        .filter(|(_, declaration)| {
            matches!(
                declaration,
                InputDeclaration::Text { optional: true, .. }
                    | InputDeclaration::Json { optional: true, .. }
                    | InputDeclaration::Attachments { optional: true }
            )
        })
        .map(|(name, _)| name.clone())
        .collect();
    let input_defaults = document
        .inputs
        .iter()
        .filter_map(|(name, declaration)| {
            let default = match declaration {
                InputDeclaration::Text {
                    default: Some(text),
                    ..
                } => ValidatedInputDefault::Text(text.clone()),
                InputDeclaration::Json {
                    default: Some(JsonDefault::File(path)),
                    ..
                } => ValidatedInputDefault::JsonFile(path.clone()),
                InputDeclaration::Json {
                    default: Some(JsonDefault::Literal(value)),
                    ..
                } => ValidatedInputDefault::JsonLiteral(value.clone()),
                InputDeclaration::Text { default: None, .. }
                | InputDeclaration::Json { default: None, .. }
                | InputDeclaration::File { .. }
                | InputDeclaration::Attachments { .. } => return None,
            };
            Some((name.clone(), default))
        })
        .collect();
    let input_json_schema_paths = document
        .inputs
        .iter()
        .filter_map(|(name, declaration)| match declaration {
            InputDeclaration::Json {
                schema: Some(schema),
                ..
            } => Some((name.clone(), schema.clone())),
            InputDeclaration::Text { .. }
            | InputDeclaration::Json { schema: None, .. }
            | InputDeclaration::File { .. }
            | InputDeclaration::Attachments { .. } => None,
        })
        .collect();
    let input_file_media_types = document
        .inputs
        .iter()
        .filter_map(|(name, declaration)| match declaration {
            InputDeclaration::File {
                media_type: Some(media_type),
            } => Some((name.clone(), media_type.clone())),
            InputDeclaration::Text { .. }
            | InputDeclaration::Json { .. }
            | InputDeclaration::File { media_type: None }
            | InputDeclaration::Attachments { .. } => None,
        })
        .collect();
    let mut steps = BTreeMap::new();
    let mut recoveries = BTreeMap::new();
    // An edge-scheduled workflow is checked step by step in document order, each child
    // before the steps that read it.
    let step_names = match scheduling {
        Scheduling::Dataflow => document.steps.keys().collect::<Vec<_>>(),
        Scheduling::Edges { .. } => document.step_order.iter().collect(),
    };
    for step_name in step_names {
        let step = &document.steps[step_name];
        let body = match &step.body {
            NodeBody::Workflow(node) => ValidatedStep::Workflow(validate_workflow_step(
                step_name,
                node,
                &document,
                &mut declared_inputs,
                children,
            )?),
            NodeBody::Command(_) | NodeBody::Agent(_) => validate_body(
                step_name,
                WorkflowNodeRole::Step,
                &step.body,
                &step_graph.direct_prerequisites[step_name],
                &document,
                &agent_profiles,
                &mut declared_inputs,
            )?,
        };
        let recovery = validate_recovery(step_name, step.recovery.as_ref(), &agent_profiles)?;
        steps.insert(step_name.clone(), body);
        recoveries.insert(step_name.clone(), recovery);
    }

    let mut finalizers = BTreeMap::new();
    for (finalizer_name, finalizer) in &document.finalizers {
        let body = validate_body(
            finalizer_name,
            WorkflowNodeRole::Finalizer,
            &finalizer.body,
            &finalizer_graph.direct_prerequisites[finalizer_name],
            &document,
            &agent_profiles,
            &mut declared_inputs,
        )?;
        finalizers.insert(
            finalizer_name.clone(),
            ValidatedFinalizer {
                body,
                when: finalizer.when.clone(),
            },
        );
    }

    let exports = document
        .exports
        .iter()
        .map(|(name, reference)| {
            resolve_export(name, &reference.source, &document).map(|source| (name.clone(), source))
        })
        .collect::<Result<_, _>>()?;

    let export_presentation = document
        .exports
        .iter()
        .filter_map(|(name, export)| {
            export.presentation.as_ref().map(|presentation| {
                resolve_export_presentation(name, presentation, &document)
                    .map(|resolved| (name.clone(), resolved))
            })
        })
        .collect::<Result<_, _>>()?;

    let outcome = match scheduling {
        Scheduling::Dataflow => resolve_outcome(&document)?,
        // The exit edge a run reaches chooses its outcome (REQ-WFFORMAT-88).
        Scheduling::Edges { .. } => None,
    };
    validate_reachability(&document, &scheduling)?;

    Ok(ValidatedWorkflow {
        schema_version: document.schema_version,
        description: document.description,
        environment_passthrough: document.environment_passthrough,
        steps,
        recoveries,
        source_order: document.step_order,
        presentation_order: step_graph.presentation_order,
        finalizers,
        finalizer_source_order: document.finalizer_order,
        finalizer_presentation_order: finalizer_graph.presentation_order,
        exports,
        export_presentation,
        outcome,
        declared_inputs,
        optional_inputs,
        input_defaults,
        input_json_schema_paths,
        input_file_media_types,
        replaced_agent_profiles,
        name: document.name,
        version: document.version,
        subject: document.subject,
        root: document.root,
        scheduling,
    })
}

/// `subject` names a declared root input of kind `text` (REQ-WFFORMAT-100).
fn validate_subject(
    subject: Option<&str>,
    declared_inputs: &DeclaredInputs,
) -> Result<(), ValidationFailure> {
    let kind = match subject.map(|name| declared_inputs.get(name)) {
        None | Some(Some(WorkflowValueType::Text)) => return Ok(()),
        Some(None) => ValidationFailureKind::SubjectInputUndeclared,
        Some(Some(_)) => ValidationFailureKind::SubjectInputNotText,
    };
    Err(ValidationFailure::new(
        kind,
        ValidationLocation::Workflow { key: "subject" },
    ))
}

/// A workflow with a `workflow` step is edge-scheduled from its first step: every ordinary
/// step is a `workflow` step and the exit edges, not an `outcome` block, choose the
/// outcome (REQ-WFFORMAT-93, REQ-WFFORMAT-88).
fn validate_scheduling(document: &WorkflowDocument) -> Result<Scheduling, ValidationFailure> {
    let is_workflow = |step: &str| matches!(document.steps[step].body, NodeBody::Workflow(_));
    if !document.step_order.iter().any(|step| is_workflow(step)) {
        return Ok(Scheduling::Dataflow);
    }
    if let Some(step) = document.step_order.iter().find(|step| !is_workflow(step)) {
        return Err(ValidationFailure::new(
            ValidationFailureKind::EdgeSchedulingKind,
            ValidationLocation::Step { step: step.clone() },
        ));
    }
    if document.outcome.is_some() {
        return Err(ValidationFailure::new(
            ValidationFailureKind::InvalidOutcomeDeclaration,
            ValidationLocation::Outcome,
        ));
    }
    Ok(Scheduling::Edges {
        entry: document.step_order[0].clone(),
    })
}

/// The outputs of each `workflow` step whose child is summarized: the child's `text` and
/// `json` exports and the implicit `outcome` (REQ-WFFORMAT-91).
fn derive_child_outputs(
    document: &WorkflowDocument,
    children: &BTreeMap<String, ChildWorkflowSummary>,
) -> BTreeMap<String, BTreeMap<String, Output>> {
    document
        .steps
        .iter()
        .filter_map(|(step, definition)| {
            let NodeBody::Workflow(node) = &definition.body else {
                return None;
            };
            let child = children.get(&node.workflow)?;
            let mut outputs = child
                .exports
                .iter()
                .filter(|(_, value_type)| {
                    matches!(
                        value_type,
                        WorkflowValueType::Text | WorkflowValueType::Json
                    )
                })
                .map(|(name, value_type)| {
                    (
                        name.clone(),
                        Output::ChildExport {
                            value_type: *value_type,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            outputs.insert(CHILD_OUTCOME_OUTPUT.to_owned(), Output::ChildOutcome);
            Some((step.clone(), outputs))
        })
        .collect()
}

/// The implicit output that carries a child's recorded outcome (REQ-WFFORMAT-91).
pub(crate) const CHILD_OUTCOME_OUTPUT: &str = "outcome";

fn validate_workflow_step(
    step_name: &str,
    node: &super::document::WorkflowNode,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
    children: &BTreeMap<String, ChildWorkflowSummary>,
) -> Result<ValidatedWorkflowStep, ValidationFailure> {
    let step_failure = |kind| {
        ValidationFailure::new(
            kind,
            ValidationLocation::StepWorkflow {
                step: step_name.to_owned(),
            },
        )
    };
    let child = children
        .get(&node.workflow)
        .ok_or_else(|| step_failure(ValidationFailureKind::ChildWorkflowUnavailable))?;
    if child.has_workflow_steps {
        return Err(step_failure(ValidationFailureKind::ChildWorkflowNested));
    }
    if child.has_root_blocks {
        return Err(step_failure(ValidationFailureKind::ChildWorkflowRootBlocks));
    }
    let for_each = node
        .for_each
        .as_ref()
        .map(|for_each| validate_for_each(step_name, for_each, document, declared_inputs, child))
        .transpose()?;

    let mut inputs = BTreeMap::new();
    for (input, reference) in &node.inputs {
        let location = ValidationLocation::StepInput {
            step: step_name.to_owned(),
            input: input.clone(),
        };
        let failure = |kind| ValidationFailure::new(kind, location.clone());
        let Some(&(value_type, may_be_omitted)) = child.inputs.get(input) else {
            return Err(failure(ValidationFailureKind::ChildInputUndeclared));
        };
        if !matches!(
            value_type,
            WorkflowValueType::Text | WorkflowValueType::Json
        ) {
            return Err(failure(ValidationFailureKind::ChildInputUnsupported));
        }
        let mut reference = resolve_child_input_reference(
            step_name,
            reference,
            document,
            declared_inputs,
            location.clone(),
        )?;
        // An element takes the child input's kind; the step checks the value when it
        // starts (REQ-WFFORMAT-99).
        if matches!(reference.source, ResolvedValueSource::Item { .. }) {
            if for_each.is_none() {
                return Err(failure(ValidationFailureKind::ItemReferenceRequiresForEach));
            }
            reference.value_type = value_type;
        }
        if reference.value_type != value_type {
            return Err(failure(ValidationFailureKind::ChildInputTypeMismatch));
        }
        // A control value may be absent when the step starts (REQ-WFFORMAT-95).
        if matches!(
            reference.source,
            ResolvedValueSource::Wait(_) | ResolvedValueSource::ControlsAnswers
        ) && !may_be_omitted
        {
            return Err(failure(
                ValidationFailureKind::ControlReferenceRequiresOptional,
            ));
        }
        inputs.insert(
            input.clone(),
            ValidatedChildInput {
                reference,
                may_be_omitted,
            },
        );
    }
    if let Some((input, _)) = child
        .inputs
        .iter()
        .find(|(input, (_, may_be_omitted))| !may_be_omitted && !node.inputs.contains_key(*input))
    {
        return Err(ValidationFailure::new(
            ValidationFailureKind::ChildInputMissing,
            ValidationLocation::StepInput {
                step: step_name.to_owned(),
                input: input.clone(),
            },
        ));
    }

    if child.exports.contains_key(CHILD_OUTCOME_OUTPUT) {
        return Err(step_failure(ValidationFailureKind::ChildExportReserved));
    }

    let edges = validate_edges(step_name, node, child, document)?;
    let outputs = document
        .derived_outputs
        .get(step_name)
        .into_iter()
        .flatten()
        .map(|(name, output)| {
            (
                name.clone(),
                ValidatedOutput {
                    definition: output.clone(),
                    value_type: output_value_type(output),
                },
            )
        })
        .collect();
    Ok(ValidatedWorkflowStep {
        common: ValidatedCommonStep {
            failure_policy: FailurePolicy::Required,
            condition: None,
            condition_values: BTreeMap::new(),
            prerequisites: Vec::new(),
            evidence_prerequisites: Vec::new(),
            cwd: None,
            outputs,
        },
        workflow: node.workflow.clone(),
        child_paths: child.paths.clone(),
        inputs,
        edges,
        for_each,
    })
}

/// `forEach` names a `json` root input or a `json` export of an earlier `workflow` step,
/// and `empty` one of the child's outcomes (REQ-WFFORMAT-98).
fn validate_for_each(
    step_name: &str,
    for_each: &ForEach,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
    child: &ChildWorkflowSummary,
) -> Result<ValidatedForEach, ValidationFailure> {
    let location = ValidationLocation::StepForEach {
        step: step_name.to_owned(),
    };
    let failure = |kind| ValidationFailure::new(kind, location.clone());
    // Every failure to resolve the reference is the one closed answer.
    let items = resolve_child_input_reference(
        step_name,
        &for_each.items,
        document,
        declared_inputs,
        location.clone(),
    )
    .ok()
    .filter(|items| {
        matches!(
            items.source,
            ResolvedValueSource::Input(_) | ResolvedValueSource::Output(_)
        ) && items.value_type == WorkflowValueType::Json
    })
    .ok_or_else(|| failure(ValidationFailureKind::ForEachReferenceInvalid))?;
    if let Some(empty) = &for_each.empty
        && !child.outcomes.contains(empty)
    {
        return Err(failure(ValidationFailureKind::ForEachEmptyUndeclared));
    }
    Ok(ValidatedForEach {
        items,
        empty: for_each.empty.clone(),
    })
}

/// A mapped child input reads a root input, an output of an earlier step, or a control
/// value (REQ-WFFORMAT-95); it adds no dependency (REQ-WFFORMAT-90, REQ-WFFORMAT-93).
fn resolve_child_input_reference(
    step_name: &str,
    reference: &ValueReference,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
    location: ValidationLocation,
) -> Result<ResolvedValueReference, ValidationFailure> {
    if let ValueReference::Output(output) = reference {
        let failure = |kind| ValidationFailure::new(kind, location.clone());
        if document.finalizers.contains_key(&output.node) {
            return Err(failure(ValidationFailureKind::CrossPhaseOutputReference));
        }
        if output.node == step_name {
            return Err(failure(ValidationFailureKind::SelfDependency));
        }
        // A later step has not run when the walk first reaches this one.
        if step_index(document, &output.node) > step_index(document, step_name) {
            return Err(failure(ValidationFailureKind::UnknownOutputStep));
        }
    }
    resolve_value_reference(reference, document, declared_inputs, location)
}

fn step_index(document: &WorkflowDocument, step: &str) -> Option<usize> {
    document.step_order.iter().position(|id| id == step)
}

/// Every child outcome has exactly one edge, and each target is a step of the workflow
/// (later, the step itself or earlier), a parent outcome, a limited edge whose `then` is a
/// step and whose `else` is an unlimited target (REQ-WFFORMAT-96), or a wait whose `then`
/// is a step or an exit, whose `onHandBack`, present exactly for `approval`, is a step,
/// and whose `rule`, present exactly for `external`, is checked (REQ-WFFORMAT-92,
/// REQ-WFFORMAT-97).
fn validate_edges(
    step_name: &str,
    node: &super::document::WorkflowNode,
    child: &ChildWorkflowSummary,
    document: &WorkflowDocument,
) -> Result<BTreeMap<String, ValidatedEdgeTarget>, ValidationFailure> {
    let edge_failure = |kind, outcome: &str| {
        ValidationFailure::new(
            kind,
            ValidationLocation::StepEdge {
                step: step_name.to_owned(),
                outcome: outcome.to_owned(),
            },
        )
    };
    if let Some(outcome) = node.edges.keys().find(|key| !child.outcomes.contains(key)) {
        return Err(edge_failure(
            ValidationFailureKind::EdgeOutcomeUndeclared,
            outcome,
        ));
    }
    if let Some(outcome) = child
        .outcomes
        .iter()
        .find(|outcome| !node.edges.contains_key(*outcome))
    {
        return Err(edge_failure(
            ValidationFailureKind::EdgeOutcomeMissing,
            outcome,
        ));
    }
    let position = step_index(document, step_name);
    node.edges
        .iter()
        .map(|(outcome, target)| {
            validate_edge_target(target, position, document)
                .map(|target| (outcome.clone(), target))
                .map_err(|kind| edge_failure(kind, outcome))
        })
        .collect()
}

fn validate_edge_target(
    target: &EdgeTarget,
    position: Option<usize>,
    document: &WorkflowDocument,
) -> Result<ValidatedEdgeTarget, ValidationFailureKind> {
    match target {
        EdgeTarget::Step(step) => {
            validate_step_target(step, document).map(ValidatedEdgeTarget::Step)
        }
        EdgeTarget::Exit(outcome) => {
            let declared = if document.outcomes.is_empty() {
                matches!(outcome.as_str(), "succeeded" | "failed")
            } else {
                document.outcomes.contains(outcome)
            };
            declared
                .then(|| ValidatedEdgeTarget::Exit(outcome.clone()))
                .ok_or(ValidationFailureKind::EdgeExitUndeclared)
        }
        EdgeTarget::Limited { then, limit, else_ } => {
            // The schema admits only a step id or an exit as `then`, and no limited `else`.
            if then.starts_with("exit.") || matches!(else_.as_ref(), EdgeTarget::Limited { .. }) {
                return Err(ValidationFailureKind::EdgeLimitTargetInvalid);
            }
            Ok(ValidatedEdgeTarget::Limited {
                then: validate_step_target(then, document)?,
                limit: *limit,
                else_: Box::new(validate_edge_target(else_, position, document)?),
            })
        }
        EdgeTarget::Wait {
            kind,
            then,
            on_hand_back,
            rule,
        } => {
            match (kind, on_hand_back) {
                (WaitKind::Approval, None) => {
                    return Err(ValidationFailureKind::WaitHandBackRequired);
                }
                (WaitKind::Approval, Some(_)) | (_, None) => {}
                (_, Some(_)) => return Err(ValidationFailureKind::WaitHandBackUnsupported),
            }
            let rule = match (kind, rule) {
                (WaitKind::External, None) => return Err(ValidationFailureKind::WaitRuleRequired),
                (WaitKind::External, Some(rule)) => {
                    validate_wait_rule(rule, position, document)?;
                    Some(rule.clone())
                }
                (_, Some(_)) => return Err(ValidationFailureKind::WaitRuleUnsupported),
                (_, None) => None,
            };
            Ok(ValidatedEdgeTarget::Wait {
                kind: *kind,
                then: Box::new(validate_edge_target(then, position, document)?),
                on_hand_back: on_hand_back
                    .as_deref()
                    .map(|step| validate_step_target(step, document))
                    .transpose()?,
                rule,
            })
        }
    }
}

/// A step target names any step of the workflow: a later one starts in the attempt, the
/// step itself or an earlier one continues the run (REQ-WFFORMAT-92).
fn validate_step_target(
    step: &str,
    document: &WorkflowDocument,
) -> Result<String, ValidationFailureKind> {
    step_index(document, step)
        .map(|_| step.to_owned())
        .ok_or(ValidationFailureKind::EdgeTargetUnknown)
}

/// An `external` wait's authored rule (REQ-WFFORMAT-97): a provider other than `um`, at
/// least one event, a `where` in the matcher's grammar, and each `inputs.<name>` a declared
/// `text` input and each `outputs.<step>.<output>` a `text` export of a `workflow` step
/// earlier than the waiting one, the values the wait record resolves.
fn validate_wait_rule(
    rule: &WaitRule,
    position: Option<usize>,
    document: &WorkflowDocument,
) -> Result<(), ValidationFailureKind> {
    if rule.source.is_empty() || rule.source == "um" || rule.event.is_empty() {
        return Err(ValidationFailureKind::WaitRuleInvalid);
    }
    let conjuncts =
        parse_where(&rule.where_).map_err(|_| ValidationFailureKind::WaitRuleInvalid)?;
    let admitted = |operand: &Operand| match operand {
        Operand::Inputs(name) => matches!(
            document.inputs.get(name),
            Some(InputDeclaration::Text { .. })
        ),
        Operand::Outputs { step, output } => {
            step_index(document, step)
                .is_some_and(|index| position.is_some_and(|waiting| index < waiting))
                && matches!(
                    document
                        .derived_outputs
                        .get(step)
                        .and_then(|outputs| outputs.get(output)),
                    Some(Output::ChildExport {
                        value_type: WorkflowValueType::Text
                    })
                )
        }
        Operand::Event(_)
        | Operand::RunId
        | Operand::Wait(_)
        | Operand::Literal(_)
        | Operand::Bool(_) => true,
    };
    conjuncts
        .iter()
        .all(|(left, right)| admitted(left) && admitted(right))
        .then_some(())
        .ok_or(ValidationFailureKind::WaitRuleReferenceInvalid)
}

/// In an edge-scheduled workflow every step but the entry is the step target of some
/// other step's edge, through a wait's `then` or `onHandBack` or a limited edge's `then` or
/// `else` as well; an edge naming its own step reaches nothing new (REQ-WFFORMAT-92,
/// REQ-WFFORMAT-93, REQ-WFFORMAT-96).
fn validate_reachability(
    document: &WorkflowDocument,
    scheduling: &Scheduling,
) -> Result<(), ValidationFailure> {
    let Scheduling::Edges { entry } = scheduling else {
        return Ok(());
    };
    fn step_targets<'a>(target: &'a EdgeTarget, targeted: &mut BTreeSet<&'a str>) {
        match target {
            EdgeTarget::Step(step) => {
                targeted.insert(step);
            }
            EdgeTarget::Exit(_) => {}
            EdgeTarget::Limited { then, else_, .. } => {
                targeted.insert(then);
                step_targets(else_, targeted);
            }
            EdgeTarget::Wait {
                then, on_hand_back, ..
            } => {
                targeted.extend(on_hand_back.as_deref());
                step_targets(then, targeted);
            }
        }
    }
    let mut targeted = BTreeSet::new();
    for (id, step) in &document.steps {
        if let NodeBody::Workflow(node) = &step.body {
            let mut own = BTreeSet::new();
            for target in node.edges.values() {
                step_targets(target, &mut own);
            }
            own.remove(id.as_str());
            targeted.extend(own);
        }
    }
    match document
        .step_order
        .iter()
        .find(|step| *step != entry && !targeted.contains(step.as_str()))
    {
        Some(step) => Err(ValidationFailure::new(
            ValidationFailureKind::StepUnreachable,
            ValidationLocation::Step { step: step.clone() },
        )),
        None => Ok(()),
    }
}

fn validate_node_namespace(document: &WorkflowDocument) -> Result<(), ValidationFailure> {
    let total = document
        .steps
        .len()
        .checked_add(document.finalizers.len())
        .ok_or_else(|| namespace_failure(ValidationFailureKind::TooManyNodes))?;
    if total > MAXIMUM_WORKFLOW_NODES {
        return Err(namespace_failure(ValidationFailureKind::TooManyNodes));
    }
    if document
        .steps
        .keys()
        .any(|id| document.finalizers.contains_key(id))
    {
        return Err(namespace_failure(ValidationFailureKind::DuplicateNodeId));
    }
    Ok(())
}

fn namespace_failure(kind: ValidationFailureKind) -> ValidationFailure {
    ValidationFailure::new(kind, ValidationLocation::WorkflowNamespace)
}

fn validate_step_graph(document: &WorkflowDocument) -> Result<ValidatedGraph, ValidationFailure> {
    let direct_prerequisites = document
        .steps
        .iter()
        .map(|(step_name, step)| {
            resolve_step_prerequisites(step_name, step, document)
                .map(|prerequisites| (step_name.clone(), prerequisites))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    validate_graph(
        direct_prerequisites,
        &document.step_order,
        document.steps.len(),
    )
}

fn validate_finalizer_graph(
    document: &WorkflowDocument,
) -> Result<ValidatedGraph, ValidationFailure> {
    let direct_prerequisites = document
        .finalizers
        .iter()
        .map(|(finalizer_name, finalizer)| {
            resolve_finalizer_prerequisites(finalizer_name, finalizer, document)
                .map(|prerequisites| (finalizer_name.clone(), prerequisites))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    validate_graph(
        direct_prerequisites,
        &document.finalizer_order,
        document.finalizers.len(),
    )
}

fn validate_graph(
    direct_prerequisites: BTreeMap<String, Vec<ResolvedDirectPrerequisite>>,
    source_order: &[String],
    node_count: usize,
) -> Result<ValidatedGraph, ValidationFailure> {
    let source_indices = source_order
        .iter()
        .enumerate()
        .map(|(index, node)| (node.clone(), index))
        .collect::<BTreeMap<_, _>>();
    let mut dependents = BTreeMap::<String, Vec<String>>::new();
    let mut remaining_prerequisites = BTreeMap::<String, usize>::new();

    for (node_name, prerequisites) in &direct_prerequisites {
        for prerequisite in prerequisites {
            dependents
                .entry(prerequisite.producer.clone())
                .or_default()
                .push(node_name.clone());
        }
        remaining_prerequisites.insert(node_name.clone(), prerequisites.len());
    }

    let mut ready = remaining_prerequisites
        .iter()
        .filter(|(_, count)| **count == 0)
        .filter_map(|(name, _)| source_indices.get(name).map(|index| (*index, name.clone())))
        .collect::<BTreeSet<_>>();
    let mut presentation_order = Vec::with_capacity(node_count);

    while let Some((_, node_name)) = ready.pop_first() {
        presentation_order.push(node_name.clone());
        if let Some(node_dependents) = dependents.get(&node_name) {
            for dependent in node_dependents {
                if let Some(count) = remaining_prerequisites.get_mut(dependent) {
                    *count -= 1;
                    if *count == 0
                        && let Some(index) = source_indices.get(dependent)
                    {
                        ready.insert((*index, dependent.clone()));
                    }
                }
            }
        }
    }

    if presentation_order.len() != node_count {
        return Err(ValidationFailure::new(
            ValidationFailureKind::DependencyCycle,
            ValidationLocation::WorkflowGraph,
        ));
    }

    Ok(ValidatedGraph {
        direct_prerequisites,
        presentation_order,
    })
}

fn resolve_step_prerequisites(
    step_name: &str,
    step: &StepDefinition,
    document: &WorkflowDocument,
) -> Result<Vec<ResolvedDirectPrerequisite>, ValidationFailure> {
    let mut prerequisites = BTreeMap::<String, ResolvedDirectPrerequisite>::new();
    for (index, dependency) in step.control_dependencies.iter().enumerate() {
        insert_control_prerequisite(
            step_name,
            dependency,
            &mut prerequisites,
            document.steps.contains_key(dependency),
            ValidationFailureKind::MissingDependency,
            ValidationLocation::StepDependency {
                step: step_name.to_owned(),
                index,
            },
        )?;
    }
    infer_condition_prerequisites(
        step_name,
        WorkflowNodeRole::Step,
        common_node(&step.body).condition.as_ref(),
        document,
        &mut prerequisites,
    )?;
    infer_body_prerequisites(
        step_name,
        WorkflowNodeRole::Step,
        &step.body,
        document,
        &mut prerequisites,
    )?;
    Ok(prerequisites.into_values().collect())
}

fn resolve_finalizer_prerequisites(
    finalizer_name: &str,
    finalizer: &FinalizerDefinition,
    document: &WorkflowDocument,
) -> Result<Vec<ResolvedDirectPrerequisite>, ValidationFailure> {
    let mut prerequisites = BTreeMap::<String, ResolvedDirectPrerequisite>::new();
    for (index, dependency) in finalizer.after.iter().enumerate() {
        insert_control_prerequisite(
            finalizer_name,
            dependency,
            &mut prerequisites,
            document.finalizers.contains_key(dependency),
            ValidationFailureKind::InvalidFinalizerAfterTarget,
            ValidationLocation::FinalizerAfter {
                finalizer: finalizer_name.to_owned(),
                index,
            },
        )?;
    }
    infer_condition_prerequisites(
        finalizer_name,
        WorkflowNodeRole::Finalizer,
        common_node(&finalizer.body).condition.as_ref(),
        document,
        &mut prerequisites,
    )?;
    infer_body_prerequisites(
        finalizer_name,
        WorkflowNodeRole::Finalizer,
        &finalizer.body,
        document,
        &mut prerequisites,
    )?;
    Ok(prerequisites.into_values().collect())
}

fn insert_control_prerequisite(
    consumer: &str,
    dependency: &String,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
    target_exists: bool,
    missing_kind: ValidationFailureKind,
    location: ValidationLocation,
) -> Result<(), ValidationFailure> {
    let failure = |kind| ValidationFailure::new(kind, location.clone());
    if dependency == consumer {
        return Err(failure(ValidationFailureKind::SelfDependency));
    }
    if !target_exists {
        return Err(failure(missing_kind));
    }
    prerequisites.insert(
        dependency.clone(),
        ResolvedDirectPrerequisite {
            producer: dependency.clone(),
            control: true,
            disposition_control: false,
            data: false,
            condition_data: false,
        },
    );
    Ok(())
}

fn validate_workflow_condition_bounds(
    document: &WorkflowDocument,
) -> Result<(), ValidationFailure> {
    let mut total = 0_usize;
    for (name, role, condition) in document
        .steps
        .iter()
        .map(|(name, step)| {
            (
                name,
                WorkflowNodeRole::Step,
                common_node(&step.body).condition.as_ref(),
            )
        })
        .chain(document.finalizers.iter().map(|(name, finalizer)| {
            (
                name,
                WorkflowNodeRole::Finalizer,
                common_node(&finalizer.body).condition.as_ref(),
            )
        }))
    {
        let Some(condition) = condition else { continue };
        let location = condition_location(name, role);
        let count = condition_node_count(condition, 1, &location)?;
        total = total.checked_add(count).ok_or_else(|| {
            ValidationFailure::new(
                ValidationFailureKind::WorkflowConditionNodeLimitExceeded,
                ValidationLocation::WorkflowGraph,
            )
        })?;
        if total > MAXIMUM_WORKFLOW_CONDITION_NODES {
            return Err(ValidationFailure::new(
                ValidationFailureKind::WorkflowConditionNodeLimitExceeded,
                ValidationLocation::WorkflowGraph,
            ));
        }
    }
    Ok(())
}

fn condition_node_count(
    predicate: &ConditionPredicate,
    depth: usize,
    location: &ValidationLocation,
) -> Result<usize, ValidationFailure> {
    if depth > MAXIMUM_CONDITION_DEPTH {
        return Err(ValidationFailure::new(
            ValidationFailureKind::ConditionDepthExceeded,
            location.clone(),
        ));
    }
    let children: &[ConditionPredicate] = match predicate {
        ConditionPredicate::All(children) | ConditionPredicate::Any(children) => {
            if children.is_empty() || children.len() > MAXIMUM_CONDITION_CHILDREN {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::ConditionChildLimitExceeded,
                    location.clone(),
                ));
            }
            children
        }
        ConditionPredicate::Not(child) => std::slice::from_ref(child),
        ConditionPredicate::Equals(_)
        | ConditionPredicate::Exists(_)
        | ConditionPredicate::Disposition { .. } => &[],
    };
    let mut count = 1_usize;
    for child in children {
        count = count
            .checked_add(condition_node_count(child, depth + 1, location)?)
            .ok_or_else(|| {
                ValidationFailure::new(
                    ValidationFailureKind::ConditionNodeLimitExceeded,
                    location.clone(),
                )
            })?;
        if count > MAXIMUM_CONDITION_NODES {
            return Err(ValidationFailure::new(
                ValidationFailureKind::ConditionNodeLimitExceeded,
                location.clone(),
            ));
        }
    }
    Ok(count)
}

fn infer_condition_prerequisites(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    condition: Option<&ConditionPredicate>,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
) -> Result<(), ValidationFailure> {
    let Some(condition) = condition else {
        return Ok(());
    };
    match condition {
        ConditionPredicate::All(children) | ConditionPredicate::Any(children) => {
            for child in children {
                infer_condition_prerequisites(
                    consumer_name,
                    consumer_role,
                    Some(child),
                    document,
                    prerequisites,
                )?;
            }
        }
        ConditionPredicate::Not(child) => infer_condition_prerequisites(
            consumer_name,
            consumer_role,
            Some(child),
            document,
            prerequisites,
        )?,
        ConditionPredicate::Equals(operands) => {
            for operand in operands {
                if let ConditionOperand::Reference { reference, .. } = operand {
                    infer_condition_value_reference(
                        consumer_name,
                        consumer_role,
                        reference,
                        document,
                        prerequisites,
                    )?;
                }
            }
        }
        ConditionPredicate::Exists(selector) => infer_condition_value_reference(
            consumer_name,
            consumer_role,
            &selector.reference,
            document,
            prerequisites,
        )?,
        ConditionPredicate::Disposition { node, .. } => {
            let target_role = condition_target_role(
                document,
                node,
                condition_location(consumer_name, consumer_role),
            )?;
            if node == consumer_name && target_role == consumer_role
                || consumer_role == WorkflowNodeRole::Step && target_role != WorkflowNodeRole::Step
            {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::InvalidConditionReference,
                    condition_location(consumer_name, consumer_role),
                ));
            }
            if consumer_role == target_role {
                prerequisites
                    .entry(node.clone())
                    .and_modify(|prerequisite| prerequisite.disposition_control = true)
                    .or_insert_with(|| ResolvedDirectPrerequisite {
                        producer: node.clone(),
                        control: false,
                        disposition_control: true,
                        data: false,
                        condition_data: false,
                    });
            }
        }
    }
    Ok(())
}

fn infer_condition_value_reference(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    reference: &ValueReference,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
) -> Result<(), ValidationFailure> {
    let location = condition_location(consumer_name, consumer_role);
    match reference {
        ValueReference::Input { name }
            if matches!(
                document.inputs.get(name),
                Some(InputDeclaration::Text { .. } | InputDeclaration::Json { .. })
            ) =>
        {
            Ok(())
        }
        ValueReference::Input { .. } => Err(ValidationFailure::new(
            ValidationFailureKind::InvalidConditionReference,
            location,
        )),
        ValueReference::FinalizationContext if consumer_role == WorkflowNodeRole::Finalizer => {
            Ok(())
        }
        // The structural schema admits the control values and the element only in a
        // `workflow` step's inputs, never in a condition.
        ValueReference::FinalizationContext
        | ValueReference::Wait { .. }
        | ValueReference::ControlsAnswers
        | ValueReference::Item { .. } => Err(ValidationFailure::new(
            ValidationFailureKind::InvalidConditionReference,
            location,
        )),
        ValueReference::Output(reference) => {
            let (producer_role, producer_body, output) =
                declared_output(document, reference, location.clone())?;
            if !matches!(
                output_value_type(output),
                WorkflowValueType::Text | WorkflowValueType::Json
            ) || reference.node == consumer_name && producer_role == consumer_role
                || consumer_role == WorkflowNodeRole::Step
                    && producer_role == WorkflowNodeRole::Finalizer
            {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::InvalidConditionReference,
                    location,
                ));
            }
            validate_data_dependency_authority(
                document,
                consumer_name,
                &reference.node,
                consumer_role,
                producer_role,
                common_node(producer_body).failure_policy,
                &location,
            )?;
            if consumer_role == producer_role {
                mark_data_prerequisite(prerequisites, &reference.node, true);
            }
            Ok(())
        }
    }
}

fn condition_location(name: &str, role: WorkflowNodeRole) -> ValidationLocation {
    match role {
        WorkflowNodeRole::Step => ValidationLocation::StepCondition {
            step: name.to_owned(),
        },
        WorkflowNodeRole::Finalizer => ValidationLocation::FinalizerCondition {
            finalizer: name.to_owned(),
        },
    }
}

fn condition_target_role(
    document: &WorkflowDocument,
    node: &str,
    location: ValidationLocation,
) -> Result<WorkflowNodeRole, ValidationFailure> {
    if document.steps.contains_key(node) {
        Ok(WorkflowNodeRole::Step)
    } else if document.finalizers.contains_key(node) {
        Ok(WorkflowNodeRole::Finalizer)
    } else {
        Err(ValidationFailure::new(
            ValidationFailureKind::InvalidConditionReference,
            location,
        ))
    }
}

fn validate_data_dependency_authority(
    document: &WorkflowDocument,
    consumer_name: &str,
    producer_name: &str,
    consumer_role: WorkflowNodeRole,
    producer_role: WorkflowNodeRole,
    producer_policy: FailurePolicy,
    location: &ValidationLocation,
) -> Result<(), ValidationFailure> {
    if producer_policy == FailurePolicy::Advisory
        && node_common(document, consumer_name, consumer_role).failure_policy
            == FailurePolicy::Required
    {
        return Err(ValidationFailure::new(
            ValidationFailureKind::AdvisoryDataDependency,
            location.clone(),
        ));
    }
    validate_finalizer_trigger_compatibility(
        document,
        consumer_name,
        producer_name,
        consumer_role,
        producer_role,
        location,
    )
}

fn validate_finalizer_trigger_compatibility(
    document: &WorkflowDocument,
    consumer_name: &str,
    producer_name: &str,
    consumer_role: WorkflowNodeRole,
    producer_role: WorkflowNodeRole,
    location: &ValidationLocation,
) -> Result<(), ValidationFailure> {
    if consumer_role == WorkflowNodeRole::Finalizer
        && producer_role == WorkflowNodeRole::Finalizer
        && !document.finalizers[consumer_name]
            .when
            .is_subset(&document.finalizers[producer_name].when)
    {
        return Err(ValidationFailure::new(
            ValidationFailureKind::IncompatibleFinalizerTriggers,
            location.clone(),
        ));
    }
    Ok(())
}

fn infer_body_prerequisites(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    body: &NodeBody,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
) -> Result<(), ValidationFailure> {
    match body {
        NodeBody::Command(command) => {
            for (input, reference) in &command.inputs {
                infer_reference_prerequisite(
                    consumer_name,
                    consumer_role,
                    reference,
                    document,
                    prerequisites,
                    node_input_location(consumer_name, consumer_role, input),
                )?;
            }
        }
        NodeBody::Agent(agent) => {
            for (index, source) in agent.agent.message.text.iter().enumerate() {
                infer_message_prerequisite(
                    consumer_name,
                    consumer_role,
                    source,
                    document,
                    prerequisites,
                    node_message_location(consumer_name, consumer_role, index, false),
                )?;
            }
            for (index, source) in agent.agent.message.attachments.iter().enumerate() {
                infer_message_prerequisite(
                    consumer_name,
                    consumer_role,
                    source,
                    document,
                    prerequisites,
                    node_message_location(consumer_name, consumer_role, index, true),
                )?;
            }
        }
        // References supply values and never order an edge-scheduled step (REQ-WFFORMAT-93).
        NodeBody::Workflow(_) => {}
    }
    Ok(())
}

fn infer_message_prerequisite(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    source: &MessageSource,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
    location: ValidationLocation,
) -> Result<(), ValidationFailure> {
    if let MessageSource::Reference(reference) = source {
        infer_reference_prerequisite(
            consumer_name,
            consumer_role,
            reference,
            document,
            prerequisites,
            location,
        )?;
    }
    Ok(())
}

fn infer_reference_prerequisite(
    consumer_name: &str,
    consumer_role: WorkflowNodeRole,
    reference: &ValueReference,
    document: &WorkflowDocument,
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
    location: ValidationLocation,
) -> Result<(), ValidationFailure> {
    match reference {
        // The structural schema admits `finalization.context` only where a finalizer may
        // bind it and the control values only in a `workflow` step's inputs, and an engine
        // value adds no prerequisite.
        ValueReference::Input { .. }
        | ValueReference::FinalizationContext
        | ValueReference::Wait { .. }
        | ValueReference::ControlsAnswers
        | ValueReference::Item { .. } => return Ok(()),
        ValueReference::Output(reference) => {
            let (producer_role, producer_body, output) =
                declared_output(document, reference, location.clone())?;
            if matches!(output, Output::GitBranchWorkspace) {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::TerminalOutputReference,
                    location,
                ));
            }
            if reference.node == consumer_name && producer_role == consumer_role {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::SelfDependency,
                    location,
                ));
            }
            if consumer_role == WorkflowNodeRole::Step
                && producer_role == WorkflowNodeRole::Finalizer
            {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::CrossPhaseOutputReference,
                    location,
                ));
            }
            validate_data_dependency_authority(
                document,
                consumer_name,
                &reference.node,
                consumer_role,
                producer_role,
                common_node(producer_body).failure_policy,
                &location,
            )?;
            if consumer_role == producer_role {
                mark_data_prerequisite(prerequisites, &reference.node, false);
            }
        }
    }
    Ok(())
}

fn mark_data_prerequisite(
    prerequisites: &mut BTreeMap<String, ResolvedDirectPrerequisite>,
    producer: &str,
    condition_data: bool,
) {
    prerequisites
        .entry(producer.to_owned())
        .and_modify(|prerequisite| {
            prerequisite.data = true;
            prerequisite.condition_data |= condition_data;
        })
        .or_insert_with(|| ResolvedDirectPrerequisite {
            producer: producer.to_owned(),
            control: false,
            disposition_control: false,
            data: true,
            condition_data,
        });
}

fn validate_output_rules(document: &WorkflowDocument) -> Result<(), ValidationFailure> {
    for (name, step) in &document.steps {
        validate_node_outputs(name, WorkflowNodeRole::Step, &step.body)?;
    }
    for (name, finalizer) in &document.finalizers {
        validate_node_outputs(name, WorkflowNodeRole::Finalizer, &finalizer.body)?;
    }
    Ok(())
}

fn validate_node_outputs(
    name: &str,
    role: WorkflowNodeRole,
    body: &NodeBody,
) -> Result<(), ValidationFailure> {
    let common = common_node(body);
    let mut paths = BTreeSet::new();
    let mut workspace_count = 0;
    for (output_name, output) in &common.outputs {
        if let Some(path) = output_path(output)
            && !paths.insert(path)
        {
            return Err(output_failure(
                ValidationFailureKind::DuplicateOutputPath,
                name,
                role,
                output_name,
            ));
        }
        if matches!(output, Output::GitBranchWorkspace) {
            workspace_count += 1;
            if workspace_count > 1 {
                return Err(output_failure(
                    ValidationFailureKind::ExcessWorkspaceOutput,
                    name,
                    role,
                    output_name,
                ));
            }
        }
    }

    // The structural schema admits agent-only output sources only on agent nodes.
    if let NodeBody::Agent(agent) = body {
        let mut response_count = 0;
        let mut result_count = 0;
        for (output_name, output) in &agent.common.outputs {
            let failure_kind = match output {
                Output::TextAgentResponse => {
                    response_count += 1;
                    if response_count > 1 {
                        Some(ValidationFailureKind::ExcessAgentResponseOutput)
                    } else {
                        (result_count > 0)
                            .then_some(ValidationFailureKind::ConflictingAgentValueOutputs)
                    }
                }
                Output::JsonAgentResult { .. } => {
                    result_count += 1;
                    if result_count > 1 {
                        Some(ValidationFailureKind::ExcessAgentResultOutput)
                    } else {
                        (response_count > 0)
                            .then_some(ValidationFailureKind::ConflictingAgentValueOutputs)
                    }
                }
                Output::TextPath { .. }
                | Output::JsonPath { .. }
                | Output::FilePath { .. }
                | Output::GitBranchWorkspace
                | Output::ChildExport { .. }
                | Output::ChildOutcome => None,
            };
            if let Some(kind) = failure_kind {
                return Err(output_failure(kind, name, role, output_name));
            }
        }
    }
    Ok(())
}

fn output_path(output: &Output) -> Option<&str> {
    match output {
        Output::TextPath { path }
        | Output::JsonPath { path, .. }
        | Output::FilePath { path, .. } => Some(path),
        Output::TextAgentResponse
        | Output::JsonAgentResult { .. }
        | Output::GitBranchWorkspace
        | Output::ChildExport { .. }
        | Output::ChildOutcome => None,
    }
}

fn output_failure(
    kind: ValidationFailureKind,
    name: &str,
    role: WorkflowNodeRole,
    output: &str,
) -> ValidationFailure {
    ValidationFailure::new(kind, node_output_location(name, role, output))
}

fn validate_recovery(
    step_name: &str,
    recovery: Option<&StepRecovery>,
    agent_profiles: &BTreeMap<String, ValidatedHarness>,
) -> Result<Option<ValidatedStepRecovery>, ValidationFailure> {
    recovery
        .map(|recovery| {
            let handler = recovery
                .handler
                .as_ref()
                .map(|handler| match handler {
                    RecoveryHandler::Command { argv, cwd } => {
                        Ok(ValidatedRecoveryHandler::Command {
                            argv: argv.clone(),
                            cwd: cwd.clone(),
                        })
                    }
                    RecoveryHandler::Agent {
                        profile,
                        prompt,
                        cwd,
                    } => {
                        let harness = agent_profiles.get(profile).cloned().ok_or_else(|| {
                            ValidationFailure::new(
                                ValidationFailureKind::UnknownAgentProfile,
                                ValidationLocation::RecoveryAgentProfileReference {
                                    step: step_name.to_owned(),
                                },
                            )
                        })?;
                        Ok(ValidatedRecoveryHandler::Agent {
                            profile: profile.clone(),
                            prompt: prompt.clone(),
                            cwd: cwd.clone(),
                            harness,
                        })
                    }
                })
                .transpose()?;
            Ok(ValidatedStepRecovery {
                retries: recovery.retries,
                handler,
            })
        })
        .transpose()
}

fn resolve_condition_predicate(
    predicate: &ConditionPredicate,
    node_name: &str,
    role: WorkflowNodeRole,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
    condition_values: &mut BTreeMap<String, ResolvedValueSource>,
) -> Result<ResolvedPredicate, ValidationFailure> {
    let resolve_child =
        |child: &ConditionPredicate,
         declared_inputs: &mut DeclaredInputs,
         condition_values: &mut BTreeMap<String, ResolvedValueSource>| {
            resolve_condition_predicate(
                child,
                node_name,
                role,
                document,
                declared_inputs,
                condition_values,
            )
        };
    match predicate {
        ConditionPredicate::All(children) => Ok(ResolvedPredicate::All(
            children
                .iter()
                .map(|child| resolve_child(child, declared_inputs, condition_values))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        )),
        ConditionPredicate::Any(children) => Ok(ResolvedPredicate::Any(
            children
                .iter()
                .map(|child| resolve_child(child, declared_inputs, condition_values))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        )),
        ConditionPredicate::Not(child) => Ok(ResolvedPredicate::Not(Box::new(resolve_child(
            child,
            declared_inputs,
            condition_values,
        )?))),
        ConditionPredicate::Equals(operands) => resolve_condition_equals(
            operands,
            node_name,
            role,
            document,
            declared_inputs,
            condition_values,
        ),
        ConditionPredicate::Exists(selector) => {
            let (canonical, source, kind) = resolve_condition_reference(
                &selector.reference,
                node_name,
                role,
                document,
                declared_inputs,
            )?;
            if kind != ConditionValueKind::Json {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::InvalidConditionType,
                    condition_location(node_name, role),
                ));
            }
            let pointer =
                JsonPointer::parse(Arc::<str>::from(selector.pointer.as_str())).map_err(|_| {
                    ValidationFailure::new(
                        ValidationFailureKind::InvalidJsonPointer,
                        condition_location(node_name, role),
                    )
                })?;
            condition_values.insert(canonical.clone(), source);
            Ok(ResolvedPredicate::Exists(ResolvedSelector::new(
                canonical, pointer,
            )))
        }
        ConditionPredicate::Disposition { node, is } => {
            let target_role =
                condition_target_role(document, node, condition_location(node_name, role))?;
            Ok(ResolvedPredicate::Disposition {
                node: WorkflowNode {
                    id: node.clone(),
                    role: target_role,
                },
                is: *is,
            })
        }
    }
}

fn resolve_condition_equals(
    operands: &[ConditionOperand; 2],
    node_name: &str,
    role: WorkflowNodeRole,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
    condition_values: &mut BTreeMap<String, ResolvedValueSource>,
) -> Result<ResolvedPredicate, ValidationFailure> {
    struct ReferenceResolution {
        canonical: String,
        source: ResolvedValueSource,
        kind: ConditionValueKind,
        pointer: Option<JsonPointer>,
    }
    let mut references: [Option<ReferenceResolution>; 2] = [None, None];
    for (index, operand) in operands.iter().enumerate() {
        let ConditionOperand::Reference { reference, pointer } = operand else {
            continue;
        };
        let (canonical, source, kind) =
            resolve_condition_reference(reference, node_name, role, document, declared_inputs)?;
        if kind == ConditionValueKind::Text && pointer.is_some() {
            return Err(ValidationFailure::new(
                ValidationFailureKind::InvalidConditionType,
                condition_location(node_name, role),
            ));
        }
        let pointer = pointer
            .as_ref()
            .map(|pointer| JsonPointer::parse(Arc::<str>::from(pointer.as_str())))
            .transpose()
            .map_err(|_| {
                ValidationFailure::new(
                    ValidationFailureKind::InvalidJsonPointer,
                    condition_location(node_name, role),
                )
            })?;
        references[index] = Some(ReferenceResolution {
            canonical,
            source,
            kind,
            pointer,
        });
    }
    let kind = match (&references[0], &references[1]) {
        (None, None) => {
            return Err(ValidationFailure::new(
                ValidationFailureKind::InvalidCondition,
                condition_location(node_name, role),
            ));
        }
        (Some(left), Some(right)) if left.kind != right.kind => {
            return Err(ValidationFailure::new(
                ValidationFailureKind::InvalidConditionType,
                condition_location(node_name, role),
            ));
        }
        (Some(reference), _) | (_, Some(reference)) => reference.kind,
    };
    let mut resolved = Vec::with_capacity(2);
    for (index, operand) in operands.iter().enumerate() {
        match operand {
            ConditionOperand::Reference { .. } => {
                let reference = references[index].take().ok_or_else(|| {
                    ValidationFailure::new(
                        ValidationFailureKind::InvalidConditionReference,
                        condition_location(node_name, role),
                    )
                })?;
                condition_values.insert(reference.canonical.clone(), reference.source);
                resolved.push(match reference.kind {
                    ConditionValueKind::Text => {
                        ResolvedOperand::text_reference(reference.canonical)
                    }
                    ConditionValueKind::Json => {
                        ResolvedOperand::json_reference(reference.canonical, reference.pointer)
                    }
                });
            }
            ConditionOperand::Literal(value) => match kind {
                ConditionValueKind::Text => {
                    let Value::String(value) = value else {
                        return Err(ValidationFailure::new(
                            ValidationFailureKind::InvalidConditionType,
                            condition_location(node_name, role),
                        ));
                    };
                    resolved.push(ResolvedOperand::text_literal(Arc::<str>::from(
                        value.as_str(),
                    )));
                }
                ConditionValueKind::Json => {
                    resolved.push(ResolvedOperand::json_literal(Arc::new(value.clone())));
                }
            },
        }
    }
    let operands: [ResolvedOperand; 2] = resolved.try_into().map_err(|_| {
        ValidationFailure::new(
            ValidationFailureKind::InvalidCondition,
            condition_location(node_name, role),
        )
    })?;
    Ok(ResolvedPredicate::Equals(operands))
}

fn resolve_condition_reference(
    reference: &ValueReference,
    node_name: &str,
    role: WorkflowNodeRole,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
) -> Result<(String, ResolvedValueSource, ConditionValueKind), ValidationFailure> {
    let resolved = resolve_value_reference(
        reference,
        document,
        declared_inputs,
        condition_location(node_name, role),
    )?;
    let kind = match resolved.value_type {
        WorkflowValueType::Text => ConditionValueKind::Text,
        WorkflowValueType::Json => ConditionValueKind::Json,
        WorkflowValueType::AttachmentCollection
        | WorkflowValueType::File
        | WorkflowValueType::GitBranch => {
            return Err(ValidationFailure::new(
                ValidationFailureKind::InvalidConditionType,
                condition_location(node_name, role),
            ));
        }
    };
    let canonical = match &resolved.source {
        ResolvedValueSource::Input(name) => format!("inputs.{name}"),
        ResolvedValueSource::Output(output) => output.reference(),
        ResolvedValueSource::FinalizationContext => "finalization.context".to_owned(),
        ResolvedValueSource::Wait(field) => format!("wait.{field}"),
        ResolvedValueSource::ControlsAnswers => "controls.answers".to_owned(),
        ResolvedValueSource::Item { field: None } => "item".to_owned(),
        ResolvedValueSource::Item { field: Some(field) } => format!("item.{field}"),
    };
    Ok((canonical, resolved.source, kind))
}

fn validate_body(
    node_name: &str,
    role: WorkflowNodeRole,
    body: &NodeBody,
    prerequisites: &[ResolvedDirectPrerequisite],
    document: &WorkflowDocument,
    agent_profiles: &BTreeMap<String, ValidatedHarness>,
    declared_inputs: &mut DeclaredInputs,
) -> Result<ValidatedStep, ValidationFailure> {
    let mut condition_values = BTreeMap::new();
    let condition = common_node(body)
        .condition
        .as_ref()
        .map(|condition| {
            resolve_condition_predicate(
                condition,
                node_name,
                role,
                document,
                declared_inputs,
                &mut condition_values,
            )
        })
        .transpose()?;
    let evidence_prerequisites = evidence_prerequisites(body, prerequisites)?;
    match body {
        NodeBody::Command(command) => Ok(ValidatedStep::Command(ValidatedCommandStep {
            common: validate_common(
                &command.common,
                condition,
                condition_values,
                prerequisites,
                evidence_prerequisites,
            ),
            inputs: validate_command_inputs(
                node_name,
                role,
                &command.inputs,
                document,
                declared_inputs,
            )?,
            argv: command.argv.clone(),
        })),
        NodeBody::Agent(agent) => {
            let common = validate_common(
                &agent.common,
                condition,
                condition_values,
                prerequisites,
                evidence_prerequisites,
            );
            let harness = resolve_agent_profile(node_name, role, &agent.agent, agent_profiles)?;
            let validated_agent = validate_agent(
                node_name,
                role,
                &agent.agent,
                document,
                declared_inputs,
                harness,
            )?;
            Ok(ValidatedStep::Agent(ValidatedAgentStep {
                common,
                agent: validated_agent,
            }))
        }
        NodeBody::Workflow(_) => Err(ValidationFailure::new(
            ValidationFailureKind::EdgeSchedulingKind,
            ValidationLocation::Step {
                step: node_name.to_owned(),
            },
        )),
    }
}

fn evidence_prerequisites(
    body: &NodeBody,
    direct: &[ResolvedDirectPrerequisite],
) -> Result<Vec<Prerequisite>, ValidationFailure> {
    let mut descriptors = direct
        .iter()
        .filter(|prerequisite| prerequisite.control)
        .filter_map(|prerequisite| Prerequisite::control(prerequisite.producer.clone()).ok())
        .collect::<Vec<_>>();
    retain_condition_descriptors(common_node(body).condition.as_ref(), &mut descriptors);
    let mut retain_reference = |reference: &ValueReference| {
        if let ValueReference::Output(output) = reference
            && let Ok(descriptor) =
                Prerequisite::body(format!("outputs.{}.{}", output.node, output.output))
        {
            descriptors.push(descriptor);
        }
    };
    match body {
        NodeBody::Command(command) => {
            for reference in command.inputs.values() {
                retain_reference(reference);
            }
        }
        NodeBody::Agent(agent) => {
            for source in agent
                .agent
                .message
                .text
                .iter()
                .chain(&agent.agent.message.attachments)
            {
                if let MessageSource::Reference(reference) = source {
                    retain_reference(reference);
                }
            }
        }
        NodeBody::Workflow(_) => {}
    }
    descriptors.sort();
    descriptors.dedup();
    if descriptors.len() > MAXIMUM_PREREQUISITES {
        return Err(ValidationFailure::new(
            ValidationFailureKind::TooManyPrerequisites,
            ValidationLocation::WorkflowGraph,
        ));
    }
    Ok(descriptors)
}

fn retain_condition_descriptors(
    condition: Option<&ConditionPredicate>,
    descriptors: &mut Vec<Prerequisite>,
) {
    let Some(condition) = condition else { return };
    match condition {
        ConditionPredicate::All(children) | ConditionPredicate::Any(children) => {
            for child in children {
                retain_condition_descriptors(Some(child), descriptors);
            }
        }
        ConditionPredicate::Not(child) => {
            retain_condition_descriptors(Some(child), descriptors);
        }
        ConditionPredicate::Equals(operands) => {
            for operand in operands {
                if let ConditionOperand::Reference {
                    reference: ValueReference::Output(output),
                    ..
                } = operand
                    && let Ok(descriptor) = Prerequisite::condition(format!(
                        "outputs.{}.{}",
                        output.node, output.output
                    ))
                {
                    descriptors.push(descriptor);
                }
            }
        }
        ConditionPredicate::Exists(selector) => {
            if let ValueReference::Output(output) = &selector.reference
                && let Ok(descriptor) =
                    Prerequisite::condition(format!("outputs.{}.{}", output.node, output.output))
            {
                descriptors.push(descriptor);
            }
        }
        ConditionPredicate::Disposition { node, .. } => {
            if let Ok(descriptor) = Prerequisite::control(node.clone()) {
                descriptors.push(descriptor);
            }
        }
    }
}

fn validate_common(
    common: &CommonNode,
    condition: Option<ResolvedPredicate>,
    condition_values: BTreeMap<String, ResolvedValueSource>,
    prerequisites: &[ResolvedDirectPrerequisite],
    evidence_prerequisites: Vec<Prerequisite>,
) -> ValidatedCommonStep {
    let outputs = common
        .outputs
        .iter()
        .map(|(name, output)| {
            (
                name.clone(),
                ValidatedOutput {
                    definition: output.clone(),
                    value_type: output_value_type(output),
                },
            )
        })
        .collect();

    ValidatedCommonStep {
        failure_policy: common.failure_policy,
        condition,
        condition_values,
        prerequisites: prerequisites.to_vec(),
        evidence_prerequisites,
        cwd: common.cwd.clone(),
        outputs,
    }
}

fn validate_command_inputs(
    node_name: &str,
    role: WorkflowNodeRole,
    inputs: &BTreeMap<String, ValueReference>,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
) -> Result<BTreeMap<String, ResolvedValueReference>, ValidationFailure> {
    inputs
        .iter()
        .map(|(input_name, reference)| {
            resolve_value_reference(
                reference,
                document,
                declared_inputs,
                node_input_location(node_name, role, input_name),
            )
            .map(|input| (input_name.clone(), input))
        })
        .collect()
}

fn resolve_value_reference(
    reference: &ValueReference,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
    location: ValidationLocation,
) -> Result<ResolvedValueReference, ValidationFailure> {
    match reference {
        ValueReference::Input { name } => {
            let Some(value_type) = declared_inputs.get(name).copied() else {
                return Err(ValidationFailure::new(
                    ValidationFailureKind::UnknownInput,
                    location,
                ));
            };
            Ok(ResolvedValueReference {
                source: ResolvedValueSource::Input(name.clone()),
                value_type,
            })
        }
        ValueReference::Output(reference) => {
            let (role, _, output) = declared_output(document, reference, location)?;
            let value_type = output_value_type(output);
            Ok(ResolvedValueReference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    node: WorkflowNode {
                        id: reference.node.clone(),
                        role,
                    },
                    output: reference.output.clone(),
                    value_type,
                }),
                value_type,
            })
        }
        ValueReference::FinalizationContext => Ok(ResolvedValueReference {
            source: ResolvedValueSource::FinalizationContext,
            value_type: WorkflowValueType::Json,
        }),
        // Only a `workflow` step's inputs decode these (REQ-WFFORMAT-95).
        ValueReference::Wait { field } => Ok(ResolvedValueReference {
            source: ResolvedValueSource::Wait(field.clone()),
            value_type: WorkflowValueType::Text,
        }),
        ValueReference::ControlsAnswers => Ok(ResolvedValueReference {
            source: ResolvedValueSource::ControlsAnswers,
            value_type: WorkflowValueType::Json,
        }),
        // An element is any JSON value; the child input it maps onto gives it its kind
        // (REQ-WFFORMAT-99).
        ValueReference::Item { field } => Ok(ResolvedValueReference {
            source: ResolvedValueSource::Item {
                field: field.clone(),
            },
            value_type: WorkflowValueType::Json,
        }),
    }
}

fn declared_output<'a>(
    document: &'a WorkflowDocument,
    reference: &OutputReference,
    location: ValidationLocation,
) -> Result<(WorkflowNodeRole, &'a NodeBody, &'a Output), ValidationFailure> {
    let (role, body) = node_body(document, &reference.node).ok_or_else(|| {
        ValidationFailure::new(ValidationFailureKind::UnknownOutputStep, location.clone())
    })?;
    let output = node_outputs(document, &reference.node, body)
        .get(&reference.output)
        .ok_or_else(|| ValidationFailure::new(ValidationFailureKind::UnknownOutput, location))?;
    Ok((role, body, output))
}

fn resolve_agent_profile(
    node_name: &str,
    role: WorkflowNodeRole,
    agent: &Agent,
    profiles: &BTreeMap<String, ValidatedHarness>,
) -> Result<ValidatedHarness, ValidationFailure> {
    profiles.get(&agent.profile).cloned().ok_or_else(|| {
        let location = match role {
            WorkflowNodeRole::Step => ValidationLocation::AgentProfileReference {
                step: node_name.to_owned(),
            },
            WorkflowNodeRole::Finalizer => ValidationLocation::FinalizerAgentProfileReference {
                finalizer: node_name.to_owned(),
            },
        };
        ValidationFailure::new(ValidationFailureKind::UnknownAgentProfile, location)
    })
}

fn validate_agent(
    node_name: &str,
    role: WorkflowNodeRole,
    agent: &Agent,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
    harness: ValidatedHarness,
) -> Result<ValidatedAgent, ValidationFailure> {
    let text = agent
        .message
        .text
        .iter()
        .enumerate()
        .map(|(index, source)| {
            validate_message_source(
                node_name,
                role,
                index,
                source,
                document,
                declared_inputs,
                MessageDestination::Text,
            )
        })
        .collect::<Result<_, _>>()?;
    let attachments = agent
        .message
        .attachments
        .iter()
        .enumerate()
        .map(|(index, source)| {
            validate_message_source(
                node_name,
                role,
                index,
                source,
                document,
                declared_inputs,
                MessageDestination::Attachment,
            )
        })
        .collect::<Result<_, _>>()?;

    Ok(ValidatedAgent {
        profile: agent.profile.clone(),
        system_prompt: agent.system_prompt.clone(),
        message: ValidatedAgentMessage { text, attachments },
        harness,
    })
}

#[derive(Clone, Copy)]
enum MessageDestination {
    Text,
    Attachment,
}

fn validate_message_source(
    node_name: &str,
    role: WorkflowNodeRole,
    index: usize,
    source: &MessageSource,
    document: &WorkflowDocument,
    declared_inputs: &mut DeclaredInputs,
    destination: MessageDestination,
) -> Result<ValidatedMessageSource, ValidationFailure> {
    let attachment = matches!(destination, MessageDestination::Attachment);
    let location = node_message_location(node_name, role, index, attachment);

    let reference = match source {
        MessageSource::File { path } => {
            return Ok(ValidatedMessageSource::File { path: path.clone() });
        }
        MessageSource::Reference(reference) => reference,
    };
    let value = resolve_value_reference(reference, document, declared_inputs, location.clone())?;
    let compatible = match destination {
        MessageDestination::Text => value.value_type == WorkflowValueType::Text,
        MessageDestination::Attachment => matches!(
            value.value_type,
            WorkflowValueType::AttachmentCollection
                | WorkflowValueType::Json
                | WorkflowValueType::File
        ),
    };
    if !compatible {
        return Err(ValidationFailure::new(
            ValidationFailureKind::MessageTypeMismatch,
            location,
        ));
    }

    Ok(ValidatedMessageSource::Reference {
        source: value.source,
        value_type: value.value_type,
    })
}

fn resolve_export(
    name: &str,
    reference: &OutputReference,
    document: &WorkflowDocument,
) -> Result<ResolvedOutputSource, ValidationFailure> {
    let Some((role, body)) = node_body(document, &reference.node) else {
        return Err(invalid_export(name));
    };
    let Some(output) = node_outputs(document, &reference.node, body).get(&reference.output) else {
        return Err(invalid_export(name));
    };
    if common_node(body).failure_policy == FailurePolicy::Advisory {
        return Err(ValidationFailure::new(
            ValidationFailureKind::AdvisoryExportTarget,
            ValidationLocation::Export {
                name: name.to_owned(),
            },
        ));
    }
    if role == WorkflowNodeRole::Finalizer
        && !document.finalizers[&reference.node]
            .when
            .contains(&super::document::FinalizationTrigger::Succeeded)
    {
        return Err(ValidationFailure::new(
            ValidationFailureKind::FinalizerExportTrigger,
            ValidationLocation::Export {
                name: name.to_owned(),
            },
        ));
    }
    Ok(ResolvedOutputSource {
        node: WorkflowNode {
            id: reference.node.clone(),
            role,
        },
        output: reference.output.clone(),
        value_type: output_value_type(output),
    })
}

/// Resolve `outcomes` and `outcome` (REQ-WFFORMAT-87, REQ-WFFORMAT-88). The source must
/// be a `text` output of a required ordinary step without a `condition`, so that a succeeded
/// run always holds its value.
fn resolve_outcome(
    document: &WorkflowDocument,
) -> Result<Option<ValidatedOutcome>, ValidationFailure> {
    let failure = |kind| ValidationFailure::new(kind, ValidationLocation::Outcome);
    let declaration = match (&document.outcome, document.outcomes.is_empty()) {
        (None, true) => return Ok(None),
        (Some(declaration), false) => declaration,
        _ => return Err(failure(ValidationFailureKind::InvalidOutcomeDeclaration)),
    };
    if !document.outcomes.contains(&declaration.on_failure) {
        return Err(failure(ValidationFailureKind::InvalidOutcomeDeclaration));
    }
    let reference = &declaration.from;
    let source = match node_body(document, &reference.node) {
        Some((WorkflowNodeRole::Step, body)) => {
            let common = common_node(body);
            common
                .outputs
                .get(&reference.output)
                .filter(|output| {
                    common.failure_policy != FailurePolicy::Advisory
                        && common.condition.is_none()
                        && output_value_type(output) == WorkflowValueType::Text
                })
                .map(|_| ResolvedOutputSource {
                    node: WorkflowNode {
                        id: reference.node.clone(),
                        role: WorkflowNodeRole::Step,
                    },
                    output: reference.output.clone(),
                    value_type: WorkflowValueType::Text,
                })
        }
        Some((WorkflowNodeRole::Finalizer, _)) | None => None,
    }
    .ok_or_else(|| failure(ValidationFailureKind::InvalidOutcomeSource))?;
    Ok(Some(ValidatedOutcome {
        names: document.outcomes.clone(),
        source,
        on_failure: declaration.on_failure.clone(),
    }))
}

fn resolve_export_presentation(
    name: &str,
    presentation: &ExportPresentation,
    document: &WorkflowDocument,
) -> Result<ValidatedExportPresentation, ValidationFailure> {
    fn resolve_field(
        name: &str,
        source: &PresentationSource,
        field: Field,
        document: &WorkflowDocument,
    ) -> Result<ValidatedPresentationField, ValidationFailure> {
        let location = ValidationLocation::Export {
            name: name.to_owned(),
        };
        match source {
            PresentationSource::Literal(value) => resolve_text(field, value)
                .map(|trimmed| ValidatedPresentationField::Literal(trimmed.to_owned()))
                .map_err(|_| {
                    ValidationFailure::new(
                        ValidationFailureKind::InvalidExportPresentation,
                        location,
                    )
                }),
            PresentationSource::Reference(reference) => {
                let (role, body) = node_body(document, &reference.node).ok_or_else(|| {
                    ValidationFailure::new(
                        ValidationFailureKind::InvalidExportPresentationTarget,
                        location.clone(),
                    )
                })?;
                let output = node_outputs(document, &reference.node, body)
                    .get(&reference.output)
                    .ok_or_else(|| {
                        ValidationFailure::new(
                            ValidationFailureKind::InvalidExportPresentationTarget,
                            location.clone(),
                        )
                    })?;
                if output_value_type(output) != WorkflowValueType::Text
                    || role == WorkflowNodeRole::Finalizer
                        && !document.finalizers[&reference.node]
                            .when
                            .contains(&super::document::FinalizationTrigger::Succeeded)
                {
                    return Err(ValidationFailure::new(
                        ValidationFailureKind::InvalidExportPresentationTarget,
                        location,
                    ));
                }
                Ok(ValidatedPresentationField::Reference(
                    ResolvedOutputSource {
                        node: WorkflowNode {
                            id: reference.node.clone(),
                            role,
                        },
                        output: reference.output.clone(),
                        value_type: WorkflowValueType::Text,
                    },
                ))
            }
        }
    }
    Ok(ValidatedExportPresentation {
        title: presentation
            .title
            .as_ref()
            .map(|source| resolve_field(name, source, Field::Title, document))
            .transpose()?,
        description: presentation
            .description
            .as_ref()
            .map(|source| resolve_field(name, source, Field::Description, document))
            .transpose()?,
    })
}

fn invalid_export(name: &str) -> ValidationFailure {
    ValidationFailure::new(
        ValidationFailureKind::InvalidExportTarget,
        ValidationLocation::Export {
            name: name.to_owned(),
        },
    )
}

fn node_body<'a>(
    document: &'a WorkflowDocument,
    name: &str,
) -> Option<(WorkflowNodeRole, &'a NodeBody)> {
    document
        .steps
        .get(name)
        .map(|step| (WorkflowNodeRole::Step, &step.body))
        .or_else(|| {
            document
                .finalizers
                .get(name)
                .map(|finalizer| (WorkflowNodeRole::Finalizer, &finalizer.body))
        })
}

fn node_common<'a>(
    document: &'a WorkflowDocument,
    name: &str,
    role: WorkflowNodeRole,
) -> &'a CommonNode {
    match role {
        WorkflowNodeRole::Step => common_node(&document.steps[name].body),
        WorkflowNodeRole::Finalizer => common_node(&document.finalizers[name].body),
    }
}

/// A `workflow` step is required, unconditional and declares no outputs of its own.
static WORKFLOW_STEP_COMMON: CommonNode = CommonNode {
    failure_policy: FailurePolicy::Required,
    condition: None,
    cwd: None,
    outputs: BTreeMap::new(),
};

fn common_node(body: &NodeBody) -> &CommonNode {
    match body {
        NodeBody::Command(command) => &command.common,
        NodeBody::Agent(agent) => &agent.common,
        NodeBody::Workflow(_) => &WORKFLOW_STEP_COMMON,
    }
}

/// A node's declared outputs, or a `workflow` step's derived ones.
fn node_outputs<'a>(
    document: &'a WorkflowDocument,
    name: &str,
    body: &'a NodeBody,
) -> &'a BTreeMap<String, Output> {
    match body {
        NodeBody::Workflow(_) => document
            .derived_outputs
            .get(name)
            .unwrap_or(&WORKFLOW_STEP_COMMON.outputs),
        NodeBody::Command(_) | NodeBody::Agent(_) => &common_node(body).outputs,
    }
}

fn node_input_location(name: &str, role: WorkflowNodeRole, input: &str) -> ValidationLocation {
    match role {
        WorkflowNodeRole::Step => ValidationLocation::StepInput {
            step: name.to_owned(),
            input: input.to_owned(),
        },
        WorkflowNodeRole::Finalizer => ValidationLocation::FinalizerInput {
            finalizer: name.to_owned(),
            input: input.to_owned(),
        },
    }
}

fn node_message_location(
    name: &str,
    role: WorkflowNodeRole,
    index: usize,
    attachment: bool,
) -> ValidationLocation {
    match (role, attachment) {
        (WorkflowNodeRole::Step, false) => ValidationLocation::MessageText {
            step: name.to_owned(),
            index,
        },
        (WorkflowNodeRole::Step, true) => ValidationLocation::MessageAttachment {
            step: name.to_owned(),
            index,
        },
        (WorkflowNodeRole::Finalizer, false) => ValidationLocation::FinalizerMessageText {
            finalizer: name.to_owned(),
            index,
        },
        (WorkflowNodeRole::Finalizer, true) => ValidationLocation::FinalizerMessageAttachment {
            finalizer: name.to_owned(),
            index,
        },
    }
}

fn node_output_location(name: &str, role: WorkflowNodeRole, output: &str) -> ValidationLocation {
    match role {
        WorkflowNodeRole::Step => ValidationLocation::StepOutput {
            step: name.to_owned(),
            output: output.to_owned(),
        },
        WorkflowNodeRole::Finalizer => ValidationLocation::FinalizerOutput {
            finalizer: name.to_owned(),
            output: output.to_owned(),
        },
    }
}

fn output_value_type(output: &Output) -> WorkflowValueType {
    match output {
        Output::TextPath { .. } | Output::TextAgentResponse => WorkflowValueType::Text,
        Output::JsonPath { .. } | Output::JsonAgentResult { .. } => WorkflowValueType::Json,
        Output::FilePath { .. } => WorkflowValueType::File,
        Output::GitBranchWorkspace => WorkflowValueType::GitBranch,
        Output::ChildExport { value_type } => *value_type,
        Output::ChildOutcome => WorkflowValueType::Text,
    }
}

#[cfg(test)]
mod tests;
