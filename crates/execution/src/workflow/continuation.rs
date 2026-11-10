use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::admission::{ResolvedInput, ResolvedInputs};
use super::document::{FailurePolicy, Output};
use super::resolution::ResolvedWorkflow;
use super::validated::{
    ResolvedOutputSource, ResolvedValueSource, Scheduling, ValidatedEdgeTarget,
    ValidatedMessageSource, ValidatedStep, ValidatedWorkflow, WorkflowNode, WorkflowNodeRole,
    WorkflowValueType,
};
use super::wait::{Operand, parse_where};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Partition {
    pub(crate) reexecuted: Vec<String>,
    pub(crate) inherited: Vec<String>,
}

/// Resolve selections against the new definition, never against the prior attempt.
/// The validated presentation order is Kahn order with source-index tie breaking.
/// An edge-scheduled continuation names exactly one step and also re-executes every step in
/// `unfinished`, those the prior attempt left neither succeeded nor inherited, so the walk
/// restarts from the named step and reaches or leaves each of them `not_reached`
/// (REQ-CONTINUE-3); a dataflow caller passes an empty set.
pub(crate) fn partition(
    workflow: &ValidatedWorkflow,
    from: &[String],
    unfinished: &BTreeSet<String>,
) -> Result<Partition, Vec<String>> {
    let mut seen = BTreeSet::new();
    let invalid = from
        .iter()
        .filter(|id| !seen.insert((*id).clone()) || !workflow.steps.contains_key(*id))
        .cloned()
        .collect::<Vec<_>>();
    if from.is_empty() || !invalid.is_empty() {
        return Err(invalid);
    }
    let edges = matches!(workflow.scheduling, Scheduling::Edges { .. });
    if edges && from.len() > 1 {
        return Err(from[1..].to_vec());
    }

    let mut dependents: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for id in &workflow.presentation_order {
        let Some(step) = workflow.steps.get(id) else {
            return Err(vec![id.clone()]);
        };
        for prerequisite in &step.common().prerequisites {
            dependents
                .entry(&prerequisite.producer)
                .or_default()
                .push(id);
        }
        // An edge-scheduled step has no prerequisites; the later steps its edges can start,
        // a wait's hand-back step among them, are its dependents, so a resume re-executes
        // the walk from it (REQ-WFEXEC-58, REQ-WFFORMAT-92). An edge naming the step itself
        // or an earlier step adds none, so the producer a continued step reads stays
        // inherited (REQ-CONTINUE-3, REQ-WFEXEC-64).
        if let ValidatedStep::Workflow(step) = step {
            let mut targets = Vec::new();
            for target in step.edges.values() {
                edge_step_targets(target, &mut targets);
            }
            let position = |step: &str| workflow.source_order.iter().position(|id| id == step);
            let producer = position(id);
            dependents.entry(id).or_default().extend(
                targets
                    .into_iter()
                    .filter(|target| position(target) > producer),
            );
        }
    }
    let mut reexecuted = from.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut queue = from.iter().map(String::as_str).collect::<VecDeque<_>>();
    while let Some(id) = queue.pop_front() {
        for dependent in dependents.get(id).into_iter().flatten() {
            if reexecuted.insert(dependent) {
                queue.push_back(dependent);
            }
        }
    }
    if edges {
        reexecuted.extend(unfinished.iter().map(String::as_str));
    }
    let (reexecuted, inherited) = workflow
        .presentation_order
        .iter()
        .cloned()
        .partition(|id| reexecuted.contains(id.as_str()));
    Ok(Partition {
        reexecuted,
        inherited,
    })
}

/// The steps an edge can start: a step target, a wait's `then` and `onHandBack`, and a
/// limited edge's `then` and `else`; an exit starts none.
fn edge_step_targets<'a>(target: &'a ValidatedEdgeTarget, targets: &mut Vec<&'a str>) {
    match target {
        ValidatedEdgeTarget::Step(id) => targets.push(id),
        ValidatedEdgeTarget::Exit(_) => {}
        ValidatedEdgeTarget::Limited { then, else_, .. } => {
            targets.push(then);
            edge_step_targets(else_, targets);
        }
        ValidatedEdgeTarget::Wait {
            then, on_hand_back, ..
        } => {
            edge_step_targets(then, targets);
            targets.extend(on_hand_back.as_deref());
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriorState {
    Succeeded,
    Skipped,
    Inherited,
    Unsatisfied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionViolation {
    InvalidFrom {
        id: String,
    },
    Node {
        id: String,
        prior: Option<PriorState>,
    },
    RequiredSkipped {
        consumer: String,
        producer: String,
    },
    Reference {
        reference: String,
    },
}

/// Project immutable run values by the replacement declaration, without borrowing
/// undeclared values into the attempt. The caller must still admit the projected
/// values against every constraint of the resolved replacement definition.
pub(crate) fn project_inputs(
    workflow: &ValidatedWorkflow,
    prior: &ValidatedWorkflow,
    values: &ResolvedInputs,
) -> Result<ResolvedInputs, Vec<String>> {
    let (projected, unsatisfied) = project_input_candidates(workflow, prior, values);
    if unsatisfied.is_empty() {
        Ok(projected)
    } else {
        Err(unsatisfied)
    }
}

pub(crate) fn project_input_candidates(
    workflow: &ValidatedWorkflow,
    prior: &ValidatedWorkflow,
    values: &ResolvedInputs,
) -> (ResolvedInputs, Vec<String>) {
    let mut projected = BTreeMap::new();
    let mut unsatisfied = Vec::new();
    for (name, kind) in &workflow.declared_inputs {
        let retained = values.get(name);
        if retained.is_none() && workflow.input_may_be_omitted(name) {
            // Admission completes an optional or defaulted input the run does not have,
            // as Cloud's continuation planner leaves it to (REQ-CONTINUE-9).
            continue;
        }
        if prior.declared_inputs.get(name) != Some(kind)
            || !matches!(
                (kind, retained),
                (WorkflowValueType::Text, Some(ResolvedInput::Text(_)))
                    | (WorkflowValueType::Json, Some(ResolvedInput::Json(_)))
                    | (WorkflowValueType::File, Some(ResolvedInput::File(_)))
                    | (
                        WorkflowValueType::AttachmentCollection,
                        Some(ResolvedInput::Attachments(_))
                    )
            )
        {
            unsatisfied.push(name.clone());
        } else if let Some(value) = retained {
            projected.insert(name.clone(), value.clone());
        }
    }
    (ResolvedInputs::new(projected), unsatisfied)
}

/// Collect independent node and ordinary-output definition failures. Inputs and
/// finalizer-to-finalizer references belong to their own admission domains.
pub(crate) fn check_inheritance(
    workflow: &ValidatedWorkflow,
    previous: &ValidatedWorkflow,
    partition: &Partition,
    states: &BTreeMap<String, PriorState>,
    effectively_skipped: &BTreeSet<String>,
) -> Vec<AdmissionViolation> {
    let inherited = partition
        .inherited
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut violations = Vec::new();
    for id in &partition.inherited {
        let prior = states.get(id).copied();
        if !matches!(
            prior,
            Some(PriorState::Succeeded | PriorState::Skipped | PriorState::Inherited)
        ) {
            violations.push(AdmissionViolation::Node {
                id: id.clone(),
                prior,
            });
        }
    }
    let references = ordered_references(
        workflow,
        referenced_outputs(workflow, &partition.reexecuted),
    );
    for id in &partition.reexecuted {
        if let Some(step) = workflow.steps.get(id)
            && step.common().failure_policy == FailurePolicy::Required
        {
            let mut body_references = BTreeSet::new();
            check_body_references(step, &mut body_references);
            for producer in body_references
                .iter()
                .map(|source| &source.node.id)
                .collect::<BTreeSet<_>>()
            {
                if inherited.contains(producer.as_str()) && effectively_skipped.contains(producer) {
                    violations.push(AdmissionViolation::RequiredSkipped {
                        consumer: id.clone(),
                        producer: producer.clone(),
                    });
                }
            }
        }
    }
    for source in references {
        if source.node.role != WorkflowNodeRole::Step
            || !inherited.contains(source.node.id.as_str())
        {
            continue;
        }
        let expected = workflow
            .steps
            .get(&source.node.id)
            .and_then(|step| step.common().outputs.get(&source.output));
        let prior = previous
            .steps
            .get(&source.node.id)
            .and_then(|step| step.common().outputs.get(&source.output));
        if !matches!((expected, prior), (Some(expected), Some(prior)) if same_output_contract(&expected.definition, &prior.definition))
        {
            violations.push(AdmissionViolation::Reference {
                reference: source.reference(),
            });
        }
    }
    violations
}

fn ordered_references(
    workflow: &ValidatedWorkflow,
    references: BTreeSet<ResolvedOutputSource>,
) -> Vec<ResolvedOutputSource> {
    let indices = workflow
        .presentation_order
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut references = references.into_iter().collect::<Vec<_>>();
    references.sort_by_key(|source| {
        (
            indices
                .get(source.node.id.as_str())
                .copied()
                .unwrap_or(usize::MAX),
            source.reference(),
        )
    });
    references
}

pub(crate) fn sort_violations(violations: &mut [AdmissionViolation], workflow: &ValidatedWorkflow) {
    let indices = workflow
        .presentation_order
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    violations.sort_by_key(|violation| {
        let (reason, node, reference) = match violation {
            AdmissionViolation::InvalidFrom { id } => (0, id.as_str(), id.as_str()),
            AdmissionViolation::Node { id, .. } => (1, id.as_str(), ""),
            AdmissionViolation::RequiredSkipped { consumer, producer } => {
                (1, consumer.as_str(), producer.as_str())
            }
            AdmissionViolation::Reference { reference } => (
                2,
                reference.split('.').nth(1).unwrap_or(""),
                reference.as_str(),
            ),
        };
        (
            reason,
            indices.get(node).copied().unwrap_or(usize::MAX),
            reference.to_owned(),
        )
    });
}

pub(crate) fn referenced_outputs(
    workflow: &ValidatedWorkflow,
    reexecuted: &[String],
) -> BTreeSet<ResolvedOutputSource> {
    let mut references = BTreeSet::new();
    for id in reexecuted {
        if let Some(step) = workflow.steps.get(id) {
            check_step_references(step, &mut references);
        }
    }
    for finalizer in workflow.finalizers.values() {
        check_step_references(&finalizer.body, &mut references);
    }
    references.extend(workflow.exports.values().cloned());
    references
}

fn check_step_references(step: &ValidatedStep, references: &mut BTreeSet<ResolvedOutputSource>) {
    for source in step.common().condition_values.values() {
        add_reference(source, references);
    }
    check_body_references(step, references);
}

fn check_body_references(step: &ValidatedStep, references: &mut BTreeSet<ResolvedOutputSource>) {
    match step {
        ValidatedStep::Command(command) => {
            for reference in command.inputs.values() {
                add_reference(&reference.source, references);
            }
        }
        ValidatedStep::Workflow(workflow) => {
            for input in workflow.inputs.values() {
                add_reference(&input.reference.source, references);
            }
            if let Some(for_each) = &workflow.for_each {
                add_reference(&for_each.items.source, references);
            }
            for target in workflow.edges.values() {
                add_rule_references(target, references);
            }
        }
        ValidatedStep::Agent(agent) => {
            // Document validation and resolved-reference admission traverse distinct types.
            // Keeping these traversals separate makes the admission boundary explicit.
            for source in agent
                .agent
                .message
                .text
                .iter()
                .chain(&agent.agent.message.attachments)
            {
                if let ValidatedMessageSource::Reference { source, .. } = source {
                    add_reference(source, references);
                }
            }
        }
    }
}

/// The `outputs.<step>.<output>` operands of an `external` wait's rule, which the wait record
/// resolves to literals when the step enters it (REQ-WFEXEC-65), so an inherited producer's
/// value is loaded for the attempt.
fn add_rule_references(
    target: &ValidatedEdgeTarget,
    references: &mut BTreeSet<ResolvedOutputSource>,
) {
    match target {
        ValidatedEdgeTarget::Wait {
            rule: Some(rule), ..
        } => {
            for operand in parse_where(&rule.where_)
                .iter()
                .flatten()
                .flat_map(|(left, right)| [left, right])
            {
                if let Operand::Outputs { step, output } = operand {
                    references.insert(ResolvedOutputSource {
                        node: WorkflowNode {
                            id: step.clone(),
                            role: WorkflowNodeRole::Step,
                        },
                        output: output.clone(),
                        value_type: WorkflowValueType::Text,
                    });
                }
            }
        }
        ValidatedEdgeTarget::Limited { else_, .. } => add_rule_references(else_, references),
        ValidatedEdgeTarget::Wait { rule: None, .. }
        | ValidatedEdgeTarget::Step(_)
        | ValidatedEdgeTarget::Exit(_) => {}
    }
}

fn add_reference(source: &ResolvedValueSource, references: &mut BTreeSet<ResolvedOutputSource>) {
    if let ResolvedValueSource::Output(output) = source {
        references.insert(output.clone());
    }
}

/// Compare resolved node semantics and only the retained static bytes reachable
/// from this node. A change elsewhere in the closure does not change this node.
pub(crate) fn definition_changed(
    current: &ResolvedWorkflow,
    previous: &ResolvedWorkflow,
    id: &str,
) -> bool {
    let current_step = current.definition.steps.get(id);
    let previous_step = previous.definition.steps.get(id);
    if current_step != previous_step
        || current.definition.recoveries.get(id) != previous.definition.recoveries.get(id)
    {
        return true;
    }
    let Some(step) = current_step else {
        return true;
    };
    let mut paths = BTreeSet::new();
    let common = step.common();
    for output in common.outputs.values() {
        match &output.definition {
            Output::JsonPath { schema, .. } | Output::JsonAgentResult { schema } => {
                paths.insert(schema.as_str());
            }
            _ => {}
        }
    }
    if let Some(Some(super::validated::ValidatedStepRecovery {
        handler: Some(super::validated::ValidatedRecoveryHandler::Agent { prompt, .. }),
        ..
    })) = current.definition.recoveries.get(id)
    {
        paths.insert(prompt.as_str());
    }
    if let ValidatedStep::Workflow(workflow) = step {
        paths.extend(workflow.child_paths.iter().map(String::as_str));
    }
    if let ValidatedStep::Agent(agent) = step {
        paths.insert(agent.agent.system_prompt.as_str());
        for source in agent
            .agent
            .message
            .text
            .iter()
            .chain(&agent.agent.message.attachments)
        {
            if let ValidatedMessageSource::File { path } = source {
                paths.insert(path);
            }
        }
    }
    paths
        .into_iter()
        .any(|path| current.source_bytes(path) != previous.source_bytes(path))
}

fn same_output_contract(current: &Output, previous: &Output) -> bool {
    match (current, previous) {
        (
            Output::TextPath { .. } | Output::TextAgentResponse,
            Output::TextPath { .. } | Output::TextAgentResponse,
        )
        | (
            Output::JsonPath { .. } | Output::JsonAgentResult { .. },
            Output::JsonPath { .. } | Output::JsonAgentResult { .. },
        )
        | (Output::GitBranchWorkspace, Output::GitBranchWorkspace)
        | (Output::ChildOutcome, Output::ChildOutcome) => true,
        (Output::FilePath { media_type: a, .. }, Output::FilePath { media_type: b, .. }) => a == b,
        (Output::ChildExport { value_type: a }, Output::ChildExport { value_type: b }) => a == b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // REQ-CONTINUE-9, REQ-WFFORMAT-84
    #[test]
    fn an_optional_or_defaulted_input_the_run_lacks_is_left_to_admission_to_complete() {
        let validated = |inputs: &str| {
            let document = super::super::decode(
                format!(
                    "schemaVersion: 1\ninputs:\n{inputs}steps:\n  work:\n    kind: cmd\n    command: {{argv: [\"true\"]}}\n"
                )
                .as_bytes(),
            )
            .unwrap();
            super::super::validation::validate(document).unwrap()
        };
        let prior = validated("  ticket: {kind: text}\n");
        let values = ResolvedInputs::new(BTreeMap::from([(
            "ticket".to_owned(),
            ResolvedInput::Text(std::sync::Arc::from("AUTO-199")),
        )]));

        let widened = validated(
            "  ticket: {kind: text}\n  branch: {kind: text, optional: true}\n  baseBranch: {kind: text, default: main}\n",
        );
        let projected = project_inputs(&widened, &prior, &values).unwrap();
        assert!(projected.get("ticket").is_some());
        assert!(projected.get("branch").is_none());
        assert!(projected.get("baseBranch").is_none());

        let demanding = validated("  ticket: {kind: text}\n  extra: {kind: text}\n");
        assert_eq!(
            project_inputs(&demanding, &prior, &values).unwrap_err(),
            ["extra".to_owned()]
        );
    }

    // INV-ENGINE-11
    #[test]
    fn union_of_control_and_data_closures_uses_kahn_order_not_selection_order() {
        let document = super::super::decode(
            b"schemaVersion: 1\nsteps:\n  west:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      text: {kind: text, from: path, path: west.txt}\n  east:\n    kind: cmd\n    command: {argv: [\"true\"]}\n  join:\n    kind: cmd\n    dependsOn: [east]\n    inputs:\n      value: {ref: outputs.west.text}\n    command: {argv: [\"true\"]}\n  tail:\n    kind: cmd\n    dependsOn: [join]\n    command: {argv: [\"true\"]}\n  alone:\n    kind: cmd\n    command: {argv: [\"true\"]}\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
        )
        .unwrap();
        let workflow = super::super::validation::validate(document).unwrap();
        let from = ["east".to_owned(), "west".to_owned()];
        let selected = partition(&workflow, &from, &BTreeSet::new()).unwrap();
        assert_eq!(selected.reexecuted, ["west", "east", "join", "tail"]);
        assert_eq!(selected.inherited, ["alone"]);
        assert_eq!(
            partition(&workflow, &["cleanup".to_owned()], &BTreeSet::new()),
            Err(vec!["cleanup".to_owned()])
        );
        assert_eq!(
            partition(
                &workflow,
                &["east".to_owned(), "east".to_owned()],
                &BTreeSet::new()
            ),
            Err(vec!["east".to_owned()])
        );
    }

    // REQ-WFEXEC-58, REQ-WFFORMAT-92
    #[test]
    fn an_edge_scheduled_resume_reexecutes_the_steps_its_edges_reach() {
        let document = super::super::decode(
            b"schemaVersion: 1\noutcomes: [landed, failed]\nsteps:\n  design:\n    kind: workflow\n    workflow: design.yaml\n    on:\n      planned: build\n      blocked: exit.failed\n  build:\n    kind: workflow\n    workflow: build.yaml\n    on:\n      built:\n        wait: approval\n        then: review\n        onHandBack: fix\n      failed: exit.failed\n  review:\n    kind: workflow\n    workflow: review.yaml\n    on:\n      approve: exit.landed\n  fix:\n    kind: workflow\n    workflow: fix.yaml\n    on:\n      fixed: exit.landed\n",
        )
        .unwrap();
        let child = |outcomes: &[&str]| super::super::validation::ChildWorkflowSummary {
            outcomes: outcomes
                .iter()
                .map(|outcome| (*outcome).to_owned())
                .collect(),
            exports: BTreeMap::new(),
            inputs: BTreeMap::new(),
            has_workflow_steps: false,
            has_root_blocks: false,
            paths: BTreeSet::new(),
        };
        let children = BTreeMap::from([
            ("design.yaml".to_owned(), child(&["planned", "blocked"])),
            ("build.yaml".to_owned(), child(&["built", "failed"])),
            ("review.yaml".to_owned(), child(&["approve"])),
            ("fix.yaml".to_owned(), child(&["fixed"])),
        ]);
        let workflow =
            super::super::validation::validate_with_children(document, None, &children).unwrap();

        // A wait's `then` and its `onHandBack` are both dependents (REQ-WFFORMAT-92).
        let selected = partition(&workflow, &["build".to_owned()], &BTreeSet::new()).unwrap();
        assert_eq!(selected.reexecuted, ["build", "review", "fix"]);
        assert_eq!(selected.inherited, ["design"]);
        let selected = partition(&workflow, &["design".to_owned()], &BTreeSet::new()).unwrap();
        assert_eq!(selected.reexecuted, ["design", "build", "review", "fix"]);
        assert!(selected.inherited.is_empty());
        let selected = partition(&workflow, &["review".to_owned()], &BTreeSet::new()).unwrap();
        assert_eq!(selected.reexecuted, ["review"]);
        assert_eq!(selected.inherited, ["design", "build", "fix"]);
    }

    // REQ-CONTINUE-3: an edge-scheduled continuation names one step and also re-executes
    // every step the prior attempt left unfinished, so the walk restarts from the named step.
    #[test]
    fn an_edge_scheduled_continuation_reexecutes_the_unfinished_steps() {
        let document = super::super::decode(
            b"schemaVersion: 1\noutcomes: [landed, failed]\nsteps:\n  design:\n    kind: workflow\n    workflow: design.yaml\n    on:\n      planned: review\n      blocked: exit.failed\n  review:\n    kind: workflow\n    workflow: review.yaml\n    on:\n      approve:\n        wait: approval\n        then: merge\n        onHandBack: fix\n  fix:\n    kind: workflow\n    workflow: fix.yaml\n    on:\n      built: merge\n  merge:\n    kind: workflow\n    workflow: merge.yaml\n    on:\n      landed: exit.landed\n",
        )
        .unwrap();
        let child = |outcomes: &[&str]| super::super::validation::ChildWorkflowSummary {
            outcomes: outcomes
                .iter()
                .map(|outcome| (*outcome).to_owned())
                .collect(),
            exports: BTreeMap::new(),
            inputs: BTreeMap::new(),
            has_workflow_steps: false,
            has_root_blocks: false,
            paths: BTreeSet::new(),
        };
        let children = BTreeMap::from([
            ("design.yaml".to_owned(), child(&["planned", "blocked"])),
            ("review.yaml".to_owned(), child(&["approve"])),
            ("fix.yaml".to_owned(), child(&["built"])),
            ("merge.yaml".to_owned(), child(&["landed"])),
        ]);
        let workflow =
            super::super::validation::validate_with_children(document, None, &children).unwrap();
        // The prior attempt waited on `review`'s approval: `fix` and `merge` were not reached.
        let unfinished = BTreeSet::from(["fix".to_owned(), "merge".to_owned()]);

        let selected = partition(&workflow, &["merge".to_owned()], &unfinished).unwrap();
        assert_eq!(selected.reexecuted, ["fix", "merge"]);
        assert_eq!(selected.inherited, ["design", "review"]);
        assert_eq!(
            partition(
                &workflow,
                &["fix".to_owned(), "merge".to_owned()],
                &unfinished
            ),
            Err(vec!["merge".to_owned()])
        );
    }

    // REQ-CONTINUE-3, REQ-WFEXEC-64: a backward edge adds no dependent, so the producer a
    // continued step reads through an output reference stays inherited, while a limited
    // edge's forward `then` is one.
    #[test]
    fn an_edge_continuation_reexecutes_only_forward_targets() {
        let document = super::super::decode(
            b"schemaVersion: 1\noutcomes: [landed, failed]\nsteps:\n  design:\n    kind: workflow\n    workflow: design.yaml\n    on:\n      planned: review\n  review:\n    kind: workflow\n    workflow: review.yaml\n    on:\n      proceed: merge\n      hand_back:\n        then: fix\n        limit: 2\n        else:\n          wait: repair\n          then: review\n  fix:\n    kind: workflow\n    workflow: build.yaml\n    inputs:\n      findings: {ref: outputs.review.findings}\n    on:\n      built: review\n  merge:\n    kind: workflow\n    workflow: merge.yaml\n    on:\n      landed: exit.landed\n",
        )
        .unwrap();
        let child = |outcomes: &[&str], exports: &[&str], inputs: &[&str]| {
            super::super::validation::ChildWorkflowSummary {
                outcomes: outcomes
                    .iter()
                    .map(|outcome| (*outcome).to_owned())
                    .collect(),
                exports: exports
                    .iter()
                    .map(|export| ((*export).to_owned(), WorkflowValueType::Text))
                    .collect(),
                inputs: inputs
                    .iter()
                    .map(|input| ((*input).to_owned(), (WorkflowValueType::Text, true)))
                    .collect(),
                has_workflow_steps: false,
                has_root_blocks: false,
                paths: BTreeSet::new(),
            }
        };
        let children = BTreeMap::from([
            ("design.yaml".to_owned(), child(&["planned"], &[], &[])),
            (
                "review.yaml".to_owned(),
                child(&["proceed", "hand_back"], &["findings"], &[]),
            ),
            (
                "build.yaml".to_owned(),
                child(&["built"], &[], &["findings"]),
            ),
            ("merge.yaml".to_owned(), child(&["landed"], &[], &[])),
        ]);
        let workflow =
            super::super::validation::validate_with_children(document, None, &children).unwrap();

        // `review` handed back: `fix` and `merge` were not reached.
        let unfinished = BTreeSet::from(["fix".to_owned(), "merge".to_owned()]);
        let selected = partition(&workflow, &["fix".to_owned()], &unfinished).unwrap();
        assert_eq!(selected.reexecuted, ["fix", "merge"]);
        assert_eq!(selected.inherited, ["design", "review"]);

        // `fix` built and its backward edge continues at `review`.
        let unfinished = BTreeSet::from(["merge".to_owned()]);
        let selected = partition(&workflow, &["review".to_owned()], &unfinished).unwrap();
        assert_eq!(selected.reexecuted, ["review", "fix", "merge"]);
        assert_eq!(selected.inherited, ["design"]);
    }

    // REQ-WFEXEC-58, REQ-CONTINUE-3: a step or export that reads an inherited `workflow`
    // step's child export or `outcome` keeps its contract, so the step stays inherited.
    #[test]
    fn an_inherited_workflow_steps_child_exports_and_outcome_keep_their_contract() {
        let document = super::super::decode(
            b"schemaVersion: 1\noutcomes: [landed, failed]\nsteps:\n  design:\n    kind: workflow\n    workflow: design.yaml\n    on:\n      planned: build\n      blocked: exit.failed\n  build:\n    kind: workflow\n    workflow: build.yaml\n    inputs:\n      parts: {ref: outputs.design.parts}\n      verdict: {ref: outputs.design.outcome}\n    on:\n      built: exit.landed\nexports:\n  parts: {ref: outputs.design.parts}\n  verdict: {ref: outputs.design.outcome}\n",
        )
        .unwrap();
        let summary = super::super::validation::ChildWorkflowSummary {
            outcomes: vec!["planned".to_owned(), "blocked".to_owned()],
            exports: BTreeMap::from([("parts".to_owned(), WorkflowValueType::Json)]),
            inputs: BTreeMap::new(),
            has_workflow_steps: false,
            has_root_blocks: false,
            paths: BTreeSet::new(),
        };
        let children = BTreeMap::from([
            ("design.yaml".to_owned(), summary.clone()),
            (
                "build.yaml".to_owned(),
                super::super::validation::ChildWorkflowSummary {
                    outcomes: vec!["built".to_owned()],
                    exports: BTreeMap::new(),
                    inputs: BTreeMap::from([
                        ("parts".to_owned(), (WorkflowValueType::Json, false)),
                        ("verdict".to_owned(), (WorkflowValueType::Text, false)),
                    ]),
                    ..summary
                },
            ),
        ]);
        let workflow =
            super::super::validation::validate_with_children(document, None, &children).unwrap();
        let selected = partition(&workflow, &["build".to_owned()], &BTreeSet::new()).unwrap();
        assert_eq!(selected.inherited, ["design"]);
        let states = BTreeMap::from([("design".to_owned(), PriorState::Succeeded)]);
        assert!(
            check_inheritance(&workflow, &workflow, &selected, &states, &BTreeSet::new())
                .is_empty()
        );

        let mut previous = workflow.clone();
        if let Some(ValidatedStep::Workflow(design)) = previous.steps.get_mut("design") {
            design.common.outputs.get_mut("parts").unwrap().definition = Output::ChildExport {
                value_type: WorkflowValueType::Text,
            };
        }
        let violations =
            check_inheritance(&workflow, &previous, &selected, &states, &BTreeSet::new());
        assert!(!violations.is_empty());
        assert!(violations.iter().all(|violation| matches!(
            violation,
            AdmissionViolation::Reference { reference } if reference == "outputs.design.parts"
        )));
    }

    #[test]
    fn reference_failures_follow_presentation_order_not_lexical_ids() {
        let document = super::super::decode(b"schemaVersion: 1\nsteps:\n  zeta:\n    kind: cmd\n    outputs:\n      value: {kind: text, from: path, path: z.txt}\n    command: {argv: [\"true\"]}\n  alpha:\n    kind: cmd\n    outputs:\n      value: {kind: text, from: path, path: a.txt}\n    command: {argv: [\"true\"]}\n  consume:\n    kind: cmd\n    inputs:\n      z: {ref: outputs.zeta.value}\n      a: {ref: outputs.alpha.value}\n    command: {argv: [\"true\"]}\n").unwrap();
        let workflow = super::super::validation::validate(document).unwrap();
        let selected = partition(&workflow, &["consume".to_owned()], &BTreeSet::new()).unwrap();
        let mut previous = workflow.clone();
        for id in ["zeta", "alpha"] {
            if let Some(ValidatedStep::Command(command)) = previous.steps.get_mut(id) {
                command.common.outputs.clear();
            }
        }
        let states = BTreeMap::from([
            ("zeta".to_owned(), PriorState::Succeeded),
            ("alpha".to_owned(), PriorState::Succeeded),
        ]);
        assert_eq!(
            check_inheritance(&workflow, &previous, &selected, &states, &BTreeSet::new()),
            vec![
                AdmissionViolation::Reference {
                    reference: "outputs.zeta.value".to_owned()
                },
                AdmissionViolation::Reference {
                    reference: "outputs.alpha.value".to_owned()
                },
            ]
        );
    }

    // REQ-CONTINUE-12, REQ-WFEXEC-59: an inherited `workflow` step's definition covers the
    // bytes its child resolution reached, the child file and its schemas, and no others.
    #[test]
    fn definition_comparison_covers_the_bytes_a_workflow_steps_child_reached() {
        use std::fs;
        use std::path::Path;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::write(root.join("workflow.yaml"), "schemaVersion: 1\noutcomes: [landed, failed]\nsteps:\n  design:\n    kind: workflow\n    workflow: design.yaml\n    on:\n      succeeded: build\n      failed: exit.failed\n  build:\n    kind: workflow\n    workflow: build.yaml\n    on:\n      succeeded: exit.landed\n      failed: exit.failed\n").unwrap();
        let child = |name: &str, argv: &str| {
            format!(
                "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: {{argv: [\"{argv}\"]}}\n    outputs:\n      result: {{kind: json, from: path, path: result.json, schema: {name}.schema.json}}\n"
            )
        };
        let schema = |kind: &str| {
            format!(
                r#"{{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"{kind}"}}"#
            )
        };
        for name in ["design", "build"] {
            fs::write(root.join(format!("{name}.yaml")), child(name, "true")).unwrap();
            fs::write(root.join(format!("{name}.schema.json")), schema("object")).unwrap();
        }
        let resolve =
            || super::super::resolution::resolve(root, Path::new("workflow.yaml")).unwrap();
        let previous = resolve();

        let edits = [
            (
                "design.yaml",
                child("design", "false"),
                child("design", "true"),
            ),
            ("design.schema.json", schema("string"), schema("object")),
        ];
        for (file, edited, original) in edits {
            fs::write(root.join(file), edited).unwrap();
            let current = resolve();
            assert!(definition_changed(&current, &previous, "design"), "{file}");
            assert!(!definition_changed(&current, &previous, "build"), "{file}");
            fs::write(root.join(file), original).unwrap();
        }

        let current = resolve();
        assert!(!definition_changed(&current, &previous, "design"));
        assert!(!definition_changed(&current, &previous, "build"));
    }

    #[test]
    fn definition_comparison_tracks_only_node_reached_static_bytes() {
        use std::fs;
        use std::path::Path;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::write(root.join("flow.yaml"), "schemaVersion: 1\nsteps:\n  cached:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      result: {kind: json, from: path, path: result.json, schema: result.schema.json}\n  independent:\n    kind: cmd\n    command: {argv: [\"true\"]}\n").unwrap();
        fs::write(
            root.join("result.schema.json"),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
        )
        .unwrap();
        let previous = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        fs::write(
            root.join("result.schema.json"),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}"#,
        )
        .unwrap();
        let current = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        assert!(definition_changed(&current, &previous, "cached"));
        assert!(!definition_changed(&current, &previous, "independent"));
    }

    #[test]
    fn definition_comparison_includes_agent_and_recovery_prompt_bytes() {
        use std::fs;
        use std::path::Path;
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::write(root.join("system.md"), "initial system").unwrap();
        fs::write(root.join("message.md"), "initial message").unwrap();
        fs::write(root.join("repair.md"), "initial repair").unwrap();
        fs::write(root.join("flow.yaml"), "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config: {model: fixture/model, thinking: off}\nsteps:\n  cached:\n    kind: agent\n    recovery:\n      retries: 1\n      handler:\n        kind: agent\n        profile: coding\n        prompt: repair.md\n    agent:\n      profile: coding\n      systemPrompt: system.md\n      message:\n        text: [{file: message.md}]\n").unwrap();
        let previous = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        for (file, bytes) in [("system.md", "new system"), ("repair.md", "new repair")] {
            fs::write(root.join(file), bytes).unwrap();
            let current = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
            assert!(definition_changed(&current, &previous, "cached"), "{file}");
            fs::write(
                root.join(file),
                format!("initial {name}", name = file.trim_end_matches(".md")),
            )
            .unwrap();
        }
    }

    #[test]
    fn changed_retained_json_schema_is_recorded_without_rejecting_inheritance() {
        use std::{fs, path::Path};
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        fs::write(root.join("flow.yaml"), "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      value: {kind: json, from: path, path: value.json, schema: value.schema.json}\n  consumer:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.producer.value}\n    command: {argv: [\"true\"]}\n").unwrap();
        fs::write(
            root.join("value.schema.json"),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
        )
        .unwrap();
        let previous = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        fs::write(
            root.join("value.schema.json"),
            r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}"#,
        )
        .unwrap();
        let current = super::super::resolution::resolve(root, Path::new("flow.yaml")).unwrap();
        let selected = partition(
            &current.definition,
            &["consumer".to_owned()],
            &BTreeSet::new(),
        )
        .unwrap();
        let states = BTreeMap::from([("producer".to_owned(), PriorState::Succeeded)]);
        assert!(
            check_inheritance(
                &current.definition,
                &previous.definition,
                &selected,
                &states,
                &BTreeSet::new(),
            )
            .is_empty()
        );
        assert!(definition_changed(&current, &previous, "producer"));
    }

    #[test]
    fn condition_only_skipped_source_remains_admissible_across_inherited_chain() {
        let document = super::super::decode(b"schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      value: {kind: text, from: path, path: value.txt}\n  consumer:\n    kind: cmd\n    condition:\n      equals:\n        - ref: outputs.producer.value\n        - value: expected\n    command: {argv: [\"true\"]}\n").unwrap();
        let workflow = super::super::validation::validate(document).unwrap();
        let selected = partition(&workflow, &["consumer".to_owned()], &BTreeSet::new()).unwrap();
        let states = BTreeMap::from([("producer".to_owned(), PriorState::Inherited)]);
        let skipped = BTreeSet::from(["producer".to_owned()]);
        assert!(check_inheritance(&workflow, &workflow, &selected, &states, &skipped).is_empty());
    }

    #[test]
    fn admission_reports_inherited_node_and_reference_failures_independently() {
        let document = super::super::decode(
            b"schemaVersion: 1\nsteps:\n  cached:\n    kind: cmd\n    outputs:\n      value: {kind: text, from: path, path: value.txt}\n    command: {argv: [\"true\"]}\n  run:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.cached.value}\n    command: {argv: [\"true\"]}\n  stranded:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
        ).unwrap();
        let workflow = super::super::validation::validate(document).unwrap();
        let selected = partition(&workflow, &["run".to_owned()], &BTreeSet::new()).unwrap();
        let mut previous = workflow.clone();
        if let Some(ValidatedStep::Command(command)) = previous.steps.get_mut("cached") {
            command.common.outputs.clear();
        }
        let states = BTreeMap::from([
            ("cached".to_owned(), PriorState::Inherited),
            ("stranded".to_owned(), PriorState::Unsatisfied),
        ]);
        assert_eq!(
            check_inheritance(&workflow, &previous, &selected, &states, &BTreeSet::new()),
            vec![
                AdmissionViolation::Node {
                    id: "stranded".to_owned(),
                    prior: Some(PriorState::Unsatisfied)
                },
                AdmissionViolation::Reference {
                    reference: "outputs.cached.value".to_owned()
                },
            ]
        );
        let states = BTreeMap::from([
            ("cached".to_owned(), PriorState::Skipped),
            ("stranded".to_owned(), PriorState::Succeeded),
        ]);
        assert!(
            check_inheritance(
                &workflow,
                &workflow,
                &selected,
                &states,
                &BTreeSet::from(["cached".to_owned()]),
            )
            .contains(&AdmissionViolation::RequiredSkipped {
                consumer: "run".to_owned(),
                producer: "cached".to_owned()
            })
        );
    }
}
