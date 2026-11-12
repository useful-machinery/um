use std::collections::{BTreeMap, BTreeSet};

use super::super::claude_code::{ClaudeCodeConfig, ClaudeCodeEffort};
use super::super::condition::{ResolvedOperand, ResolvedPredicate};
use super::super::document::{
    FailurePolicy, FinalizationTrigger, NodeBody, Output, ValueReference, WaitKind, WaitRule,
};
use super::super::pi::{PiConfig, Thinking};
use super::super::validated::{
    ResolvedDirectPrerequisite, ResolvedOutputSource, ResolvedValueReference, ResolvedValueSource,
    Scheduling, ValidatedEdgeTarget, ValidatedForEach, ValidatedHarness, ValidatedMessageSource,
    ValidatedOutcome, ValidatedStep, ValidatedWorkflow, ValidatedWorkflowStep, WorkflowNode,
    WorkflowNodeRole, WorkflowValueType,
};
use super::super::{DecodeFailureKind, decode};
use super::{ChildWorkflowSummary, ValidationFailureKind, ValidationLocation};

fn validate_yaml(source: &str) -> Result<ValidatedWorkflow, super::ValidationFailure> {
    super::validate(decode(source.as_bytes()).unwrap())
}

fn assert_failure(source: &str, kind: ValidationFailureKind, location: ValidationLocation) {
    let failure = validate_yaml(source).unwrap_err();
    assert_eq!(failure.kind(), kind);
    assert_eq!(failure.location(), &location);
}

fn command_step(name: &str, dependencies: &str) -> String {
    format!("  {name}:\n    kind: cmd\n{dependencies}    command:\n      argv: [\"true\"]\n")
}

fn prerequisite_shape(prerequisites: &[ResolvedDirectPrerequisite]) -> Vec<(&str, bool, bool)> {
    prerequisites
        .iter()
        .map(|prerequisite| {
            (
                prerequisite.producer.as_str(),
                prerequisite.control,
                prerequisite.data,
            )
        })
        .collect()
}

// REQ-WFFORMAT-40
#[test]
fn failure_policy_defaults_and_typed_data_compatibility_are_resolved() {
    let source = "schemaVersion: 1
steps:
  analyze:
    kind: cmd
    failurePolicy: advisory
    command:
      argv: [\"true\"]
    outputs:
      report:
        kind: file
        from: path
        path: report.json
        mediaType: application/json
  summarize:
    kind: cmd
    failurePolicy: advisory
    dependsOn: [analyze]
    inputs:
      report:
        ref: outputs.analyze.report
    command:
      argv: [\"true\"]
  required:
    kind: cmd
    dependsOn: [analyze]
    command:
      argv: [\"true\"]
";
    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(analyze) = &workflow.steps["analyze"] else {
        panic!("analyze must be a command step");
    };
    let ValidatedStep::Command(summarize) = &workflow.steps["summarize"] else {
        panic!("summarize must be a command step");
    };
    let ValidatedStep::Command(required) = &workflow.steps["required"] else {
        panic!("required must be a command step");
    };
    assert_eq!(analyze.common.failure_policy, FailurePolicy::Advisory);
    assert_eq!(summarize.common.failure_policy, FailurePolicy::Advisory);
    assert_eq!(required.common.failure_policy, FailurePolicy::Required);
    assert_eq!(
        prerequisite_shape(&summarize.common.prerequisites),
        [("analyze", true, true)]
    );
    assert_eq!(
        prerequisite_shape(&required.common.prerequisites),
        [("analyze", true, false)]
    );

    let required_consumer = source.replace(
        "  summarize:\n    kind: cmd\n    failurePolicy: advisory\n",
        "  summarize:\n    kind: cmd\n",
    );
    assert_failure(
        &required_consumer,
        ValidationFailureKind::AdvisoryDataDependency,
        ValidationLocation::StepInput {
            step: "summarize".to_owned(),
            input: "report".to_owned(),
        },
    );
}

// REQ-WFFORMAT-66
#[test]
fn advisory_outputs_cannot_be_exported() {
    let source = "schemaVersion: 1
steps:
  analyze:
    kind: cmd
    failurePolicy: advisory
    command:
      argv: [\"true\"]
    outputs:
      report:
        kind: file
        from: path
        path: report.json
        mediaType: application/json
exports:
  report:
    ref: outputs.analyze.report
";
    assert_failure(
        source,
        ValidationFailureKind::AdvisoryExportTarget,
        ValidationLocation::Export {
            name: "report".to_owned(),
        },
    );
}

// REQ-WFFORMAT-38
#[test]
fn dependency_graph_failures_are_classified_at_owned_locations() {
    let missing = format!(
        "schemaVersion: 1\nsteps:\n{}",
        command_step("consumer", "    dependsOn: [missing]\n")
    );
    assert_failure(
        &missing,
        ValidationFailureKind::MissingDependency,
        ValidationLocation::StepDependency {
            step: "consumer".to_owned(),
            index: 0,
        },
    );

    let self_dependency = format!(
        "schemaVersion: 1\nsteps:\n{}",
        command_step("consumer", "    dependsOn: [consumer]\n")
    );
    assert_failure(
        &self_dependency,
        ValidationFailureKind::SelfDependency,
        ValidationLocation::StepDependency {
            step: "consumer".to_owned(),
            index: 0,
        },
    );

    let cycle = format!(
        "schemaVersion: 1\nsteps:\n{}{}{}",
        command_step("one", "    dependsOn: [three]\n"),
        command_step("two", "    dependsOn: [one]\n"),
        command_step("three", "    dependsOn: [two]\n")
    );
    assert_failure(
        &cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );

    let data_cycle = "schemaVersion: 1
steps:
  one:
    kind: cmd
    inputs:
      value:
        ref: outputs.two.value
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: one.txt
        mediaType: text/plain
  two:
    kind: cmd
    inputs:
      value:
        ref: outputs.one.value
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: two.txt
        mediaType: text/plain
";
    assert_failure(
        data_cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );

    let mixed_cycle = data_cycle.replace(
        "    inputs:\n      value:\n        ref: outputs.two.value\n",
        "    dependsOn: [two]\n",
    );
    assert_failure(
        &mixed_cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );
}

#[test]
fn branching_and_disconnected_dag_retains_every_edge_and_step() {
    let source = format!(
        "schemaVersion: 1\nsteps:\n{}{}{}{}{}",
        command_step("root", ""),
        command_step("left", "    dependsOn: [root]\n"),
        command_step("right", "    dependsOn: [root]\n"),
        command_step("join", "    dependsOn: [left, right]\n"),
        command_step("isolated", ""),
    );
    let workflow = validate_yaml(&source).unwrap();

    assert_eq!(workflow.steps.len(), 5);
    let ValidatedStep::Command(join) = &workflow.steps["join"] else {
        panic!("join must be a command step");
    };
    assert_eq!(
        prerequisite_shape(&join.common.prerequisites),
        [("left", true, false), ("right", true, false)]
    );
    for (dependency, dependent) in [
        ("root", "left"),
        ("root", "right"),
        ("left", "join"),
        ("right", "join"),
    ] {
        let dependency_position = workflow
            .presentation_order
            .iter()
            .position(|step| step == dependency)
            .unwrap();
        let dependent_position = workflow
            .presentation_order
            .iter()
            .position(|step| step == dependent)
            .unwrap();
        assert!(dependency_position < dependent_position);
    }
    assert!(workflow.presentation_order.contains(&"isolated".to_owned()));
}

#[test]
fn presentation_order_uses_source_index_for_each_kahn_ready_set() {
    let source = "schemaVersion: 1
steps:
  z:
    kind: cmd
    command:
      argv: [\"true\"]
  a:
    kind: cmd
    dependsOn: [z]
    command:
      argv: [\"true\"]
  m:
    kind: cmd
    command:
      argv: [\"true\"]
";

    let workflow = validate_yaml(source).unwrap();

    assert_eq!(workflow.source_order, ["z", "a", "m"]);
    assert_eq!(workflow.presentation_order, ["z", "a", "m"]);
}

// REQ-WFFORMAT-45, REQ-WFFORMAT-47, REQ-WFFORMAT-51
#[test]
fn agent_profiles_resolve_to_pinned_configs_and_missing_references_fail() {
    let source = "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
  unused:
    harness:
      kind: pi
      config:
        model: openai/gpt-4.1
        thinking: low
steps:
  agent:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
";
    let workflow = validate_yaml(source).unwrap();
    assert!(workflow.declared_inputs.is_empty());
    let ValidatedStep::Agent(agent) = &workflow.steps["agent"] else {
        panic!("agent must be an agent step");
    };
    assert_eq!(agent.agent.profile, "coding");
    assert_eq!(
        agent.agent.harness,
        ValidatedHarness::Pi(PiConfig {
            model: "openai/gpt-5".to_owned(),
            thinking: Thinking::XHigh,
        })
    );

    assert_failure(
        &source.replace("profile: coding", "profile: missing"),
        ValidationFailureKind::UnknownAgentProfile,
        ValidationLocation::AgentProfileReference {
            step: "agent".to_owned(),
        },
    );
}

// REQ-WFFORMAT-47, REQ-WFFORMAT-78
#[test]
fn claude_code_profiles_resolve_without_installation_or_model_lookup() {
    let source = "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: claude_code
      config:
        model: future-claude-model
        effort: xhigh
steps:
  agent:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Agent(agent) = &workflow.steps["agent"] else {
        panic!("agent must be an agent step");
    };
    assert_eq!(
        agent.agent.harness,
        ValidatedHarness::ClaudeCode(ClaudeCodeConfig {
            model: "future-claude-model".to_owned(),
            effort: ClaudeCodeEffort::XHigh,
        })
    );
}

// REQ-WFFORMAT-48, REQ-WFFORMAT-78
#[test]
fn codex_profiles_resolve_exact_nonempty_native_strings_without_lookup() {
    let source = "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: codex
      config:
        model: future-codex-model
        effort: future-native-effort
steps:
  agent:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Agent(agent) = &workflow.steps["agent"] else {
        panic!("agent must be an agent step");
    };
    assert_eq!(
        agent.agent.harness,
        ValidatedHarness::Codex(crate::workflow::codex::CodexConfig {
            model: "future-codex-model".to_owned(),
            effort: "future-native-effort".to_owned(),
        })
    );
}

// REQ-WFFORMAT-55
#[test]
fn output_bindings_require_declared_non_self_outputs() {
    let missing_step = "schemaVersion: 1
steps:
  consumer:
    kind: cmd
    inputs:
      value:
        ref: outputs.missing.value
    command:
      argv: [\"true\"]
";
    assert_failure(
        missing_step,
        ValidationFailureKind::UnknownOutputStep,
        ValidationLocation::StepInput {
            step: "consumer".to_owned(),
            input: "value".to_owned(),
        },
    );

    let missing_output = "schemaVersion: 1
steps:
  producer:
    kind: cmd
    command:
      argv: [\"true\"]
  consumer:
    kind: cmd
    inputs:
      value:
        ref: outputs.producer.missing
    command:
      argv: [\"true\"]
";
    assert_failure(
        missing_output,
        ValidationFailureKind::UnknownOutput,
        ValidationLocation::StepInput {
            step: "consumer".to_owned(),
            input: "value".to_owned(),
        },
    );

    let self_reference = "schemaVersion: 1
steps:
  consumer:
    kind: cmd
    inputs:
      value:
        ref: outputs.consumer.value
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: value.txt
        mediaType: text/plain
";
    assert_failure(
        self_reference,
        ValidationFailureKind::SelfDependency,
        ValidationLocation::StepInput {
            step: "consumer".to_owned(),
            input: "value".to_owned(),
        },
    );
}

// REQ-WFFORMAT-39
#[test]
fn command_output_references_form_normalized_direct_prerequisites() {
    let source = "schemaVersion: 1
steps:
  alpha:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: alpha.txt
        mediaType: text/plain
  beta:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: beta.txt
        mediaType: text/plain
  middle:
    kind: cmd
    dependsOn: [alpha]
    command:
      argv: [\"true\"]
  zeta:
    kind: cmd
    command:
      argv: [\"true\"]
  consumer:
    kind: cmd
    dependsOn: [zeta, middle, alpha]
    inputs:
      second:
        ref: outputs.beta.value
      first:
        ref: outputs.alpha.value
    command:
      argv: [\"true\"]
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be a command step");
    };
    assert_eq!(
        prerequisite_shape(&consumer.common.prerequisites),
        [
            ("alpha", true, true),
            ("beta", false, true),
            ("middle", true, false),
            ("zeta", true, false),
        ]
    );
    assert_eq!(consumer.inputs.len(), 2);
    assert_eq!(consumer.inputs["first"].value_type, WorkflowValueType::File);
    assert_eq!(
        workflow.presentation_order,
        ["alpha", "beta", "middle", "zeta", "consumer"]
    );
}

#[test]
fn transitive_output_reference_remains_a_direct_prerequisite() {
    let source = "schemaVersion: 1
steps:
  producer:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: value.txt
        mediaType: text/plain
  middle:
    kind: cmd
    dependsOn: [producer]
    command:
      argv: [\"true\"]
  consumer:
    kind: cmd
    dependsOn: [middle]
    inputs:
      value:
        ref: outputs.producer.value
    command:
      argv: [\"true\"]
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be a command step");
    };
    assert_eq!(
        prerequisite_shape(&consumer.common.prerequisites),
        [("middle", true, false), ("producer", false, true)]
    );
    assert_eq!(
        workflow.presentation_order,
        ["producer", "middle", "consumer"]
    );
}

// REQ-WFFORMAT-39
#[test]
fn workflow_inputs_and_exports_do_not_create_step_prerequisites() {
    let source = "schemaVersion: 1
inputs:
  request:
    kind: text
steps:
  consumer:
    kind: cmd
    inputs:
      prompt:
        ref: inputs.request
    command:
      argv: [\"true\"]
  producer:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      value:
        kind: file
        from: path
        path: value.txt
        mediaType: text/plain
exports:
  value:
    ref: outputs.producer.value
";

    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be a command step");
    };
    assert!(consumer.common.prerequisites.is_empty());
    assert_eq!(
        workflow.declared_inputs.get("request"),
        Some(&WorkflowValueType::Text)
    );
    assert_eq!(workflow.presentation_order, ["consumer", "producer"]);
}

// REQ-WFFORMAT-29
#[test]
fn undeclared_workflow_input_is_rejected() {
    let source = "schemaVersion: 1
steps:
  consumer:
    kind: cmd
    inputs:
      value:
        ref: inputs.unknown
    command:
      argv: [\"true\"]
";
    assert_failure(
        source,
        ValidationFailureKind::UnknownInput,
        ValidationLocation::StepInput {
            step: "consumer".to_owned(),
            input: "value".to_owned(),
        },
    );
}

// REQ-WFFORMAT-100
#[test]
fn subject_names_a_declared_text_input() {
    let workflow = |subject: &str| {
        format!(
            "schemaVersion: 1\nsubject: {subject}\ninputs:\n  ticket:\n    kind: text\n  \
             payload:\n    kind: json\nsteps:\n{}",
            command_step("prepare", "")
        )
    };
    let validated = validate_yaml(&workflow("ticket")).unwrap();
    assert_eq!(validated.subject.as_deref(), Some("ticket"));
    // Like `name`, `subject` keeps a file from being a `workflow` step's child
    // (REQ-WFFORMAT-89).
    assert!(ChildWorkflowSummary::of(&validated, BTreeSet::new()).has_root_blocks);
    let at_subject = ValidationLocation::Workflow { key: "subject" };
    assert_failure(
        &workflow("nothing"),
        ValidationFailureKind::SubjectInputUndeclared,
        at_subject.clone(),
    );
    assert_failure(
        &workflow("payload"),
        ValidationFailureKind::SubjectInputNotText,
        at_subject,
    );
}

fn typed_message_workflow(reference: &str, destination: &str) -> String {
    let message = match destination {
        "text" => format!("        text:\n          - ref: {reference}\n"),
        "attachment" => format!(
            "        text:\n          - file: prompts/message.md\n        attachments:\n          - ref: {reference}\n"
        ),
        _ => panic!("unknown test destination"),
    };
    format!(
        "schemaVersion: 1
inputs:
  request:
    kind: text
  evidence:
    kind: attachments
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  responseProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      response:
        kind: text
        from: agent_response
      file:
        kind: file
        from: path
        path: artifact.bin
        mediaType: application/octet-stream
  resultProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: schemas/result.json
  consumer:
    kind: agent
    dependsOn: [responseProducer, resultProducer]
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
{message}"
    )
}

// REQ-WFCOND-6, REQ-WFCOND-7, REQ-WFCOND-10
#[test]
fn named_text_conditions_are_phase_valid_and_non_text_inputs_are_rejected() {
    let source = "schemaVersion: 1
inputs:
  request: {kind: text}
  evidence: {kind: attachments}
  payload: {kind: file}
steps:
  ordinary:
    kind: cmd
    condition:
      equals:
        - ref: inputs.request
        - value: exact
    command: {argv: [\"true\"]}
finalizers:
  finish:
    kind: cmd
    condition:
      equals:
        - ref: inputs.request
        - value: exact
    command: {argv: [\"true\"]}
";
    let workflow = validate_yaml(source).unwrap();
    for (name, step) in [
        ("ordinary", &workflow.steps["ordinary"]),
        ("finish", &workflow.finalizers["finish"].body),
    ] {
        let common = match step {
            ValidatedStep::Command(step) => &step.common,
            ValidatedStep::Agent(_) | ValidatedStep::Workflow(_) => {
                panic!("{name} must be a command")
            }
        };
        assert_eq!(
            common.condition_values.get("inputs.request"),
            Some(&ResolvedValueSource::Input("request".to_owned()))
        );
    }
    for rejected_input in ["evidence", "payload"] {
        for (node, location) in [
            (
                "ordinary",
                ValidationLocation::StepCondition {
                    step: "ordinary".to_owned(),
                },
            ),
            (
                "finish",
                ValidationLocation::FinalizerCondition {
                    finalizer: "finish".to_owned(),
                },
            ),
        ] {
            let mut invalid = source.to_owned();
            let start = invalid.find(&format!("  {node}:")).unwrap();
            let relative = invalid[start..].find("ref: inputs.request").unwrap();
            let offset = start + relative;
            invalid.replace_range(
                offset..offset + "ref: inputs.request".len(),
                &format!("ref: inputs.{rejected_input}"),
            );
            assert_failure(
                &invalid,
                ValidationFailureKind::InvalidConditionReference,
                location,
            );
        }
    }
}

const CONDITION_WORKFLOW: &str = "schemaVersion: 1
inputs:
  request: {kind: text}
  payload: {kind: json}
steps:
  plan:
    kind: cmd
    command: {argv: [\"true\"]}
    outputs:
      note: {kind: text, from: path, path: note.txt}
  gate:
    kind: cmd
#gate
    command: {argv: [\"true\"]}
finalizers:
  finish:
    kind: cmd
#finish
    command: {argv: [\"true\"]}
  report:
    kind: cmd
    command: {argv: [\"true\"]}
";

const TEXT_EQUALS: &str = "{equals: [{ref: inputs.request}, {value: x}]}";

fn gate_condition(condition: &str) -> String {
    CONDITION_WORKFLOW
        .replace("#gate\n", &format!("    condition: {condition}\n"))
        .replace("#finish\n", "")
}

fn finish_condition(condition: &str) -> String {
    CONDITION_WORKFLOW
        .replace("#gate\n", "")
        .replace("#finish\n", &format!("    condition: {condition}\n"))
}

fn gate_location() -> ValidationLocation {
    ValidationLocation::StepCondition {
        step: "gate".to_owned(),
    }
}

fn assert_structurally_invalid(source: &str) {
    assert_eq!(
        decode(source.as_bytes()).unwrap_err().kind(),
        DecodeFailureKind::StructuralContract,
        "accepted {source}"
    );
}

fn resolved_condition<'a>(workflow: &'a ValidatedWorkflow, step: &str) -> &'a ResolvedPredicate {
    let ValidatedStep::Command(step) = &workflow.steps[step] else {
        panic!("{step} must be a command step");
    };
    step.common.condition.as_ref().unwrap()
}

// REQ-WFCOND-1
#[test]
fn a_condition_is_exactly_one_closed_predicate_object() {
    let leaf = TEXT_EQUALS;
    for accepted in [
        format!("{{all: [{leaf}]}}"),
        format!("{{any: [{leaf}]}}"),
        format!("{{not: {leaf}}}"),
        leaf.to_owned(),
        "{exists: {ref: inputs.payload, pointer: /ready}}".to_owned(),
        "{disposition: {node: plan, is: succeeded}}".to_owned(),
    ] {
        validate_yaml(&gate_condition(&accepted))
            .unwrap_or_else(|failure| panic!("rejected {accepted}: {failure:?}"));
    }
    for rejected in [
        format!("{{every: [{leaf}]}}"),
        format!("{{not: {leaf}, all: [{leaf}]}}"),
        "{disposition: {node: plan, is: succeeded, note: extra}}".to_owned(),
        "{equals: [{ref: inputs.request, label: extra}, {value: x}]}".to_owned(),
        "null".to_owned(),
        "true".to_owned(),
        "\"inputs.request == 'x'\"".to_owned(),
        format!("[{leaf}]"),
    ] {
        assert_structurally_invalid(&gate_condition(&rejected));
    }
}

// REQ-WFCOND-2
#[test]
fn compound_predicates_hold_bounded_ordered_children_and_fixed_arity() {
    let children = |count: usize| {
        (0..count)
            .map(|index| format!("{{equals: [{{ref: inputs.request}}, {{value: v{index}}}]}}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let expected_children = || {
        (0..64)
            .map(|index| {
                ResolvedPredicate::Equals([
                    ResolvedOperand::text_reference("inputs.request"),
                    ResolvedOperand::text_literal(format!("v{index}")),
                ])
            })
            .collect::<Vec<_>>()
    };
    for (key, expected) in [
        ("all", ResolvedPredicate::All(expected_children().into())),
        ("any", ResolvedPredicate::Any(expected_children().into())),
    ] {
        let workflow =
            validate_yaml(&gate_condition(&format!("{{{key}: [{}]}}", children(64)))).unwrap();
        assert_eq!(resolved_condition(&workflow, "gate"), &expected);
        for count in [0, 65] {
            assert_structurally_invalid(&gate_condition(&format!(
                "{{{key}: [{}]}}",
                children(count)
            )));
        }
    }
    let leaf = TEXT_EQUALS;
    for rejected in [
        format!("{{not: [{leaf}]}}"),
        format!("{{not: [{leaf}, {leaf}]}}"),
        "{equals: [{ref: inputs.request}]}".to_owned(),
        "{equals: [{ref: inputs.request}, {value: x}, {value: y}]}".to_owned(),
    ] {
        assert_structurally_invalid(&gate_condition(&rejected));
    }
}

// REQ-WFCOND-3
#[test]
fn operands_are_a_reference_or_any_json_literal_but_never_two_literals() {
    for literal in [
        "null",
        "true",
        "3",
        "text",
        "[1, {a: null}]",
        "{nested: {deep: [true]}}",
    ] {
        validate_yaml(&gate_condition(&format!(
            "{{equals: [{{ref: inputs.payload, pointer: /a}}, {{value: {literal}}}]}}"
        )))
        .unwrap_or_else(|failure| panic!("rejected literal {literal}: {failure:?}"));
    }
    for rejected in [
        "{equals: [{ref: inputs.request, value: x}, {value: x}]}",
        "{equals: [{pointer: /a}, {value: x}]}",
        "{equals: [{}, {value: x}]}",
    ] {
        assert_structurally_invalid(&gate_condition(rejected));
    }
    assert_failure(
        &gate_condition("{equals: [{value: x}, {value: x}]}"),
        ValidationFailureKind::InvalidCondition,
        gate_location(),
    );
}

// REQ-WFCOND-4
#[test]
fn exists_requires_a_pointer_into_a_json_value() {
    validate_yaml(&gate_condition(
        "{exists: {ref: inputs.payload, pointer: /ready}}",
    ))
    .unwrap();
    assert_structurally_invalid(&gate_condition("{exists: {ref: inputs.payload}}"));
    for text in ["inputs.request", "outputs.plan.note"] {
        assert_failure(
            &gate_condition(&format!("{{exists: {{ref: {text}, pointer: /ready}}}}")),
            ValidationFailureKind::InvalidConditionType,
            gate_location(),
        );
    }
}

// REQ-WFCOND-8
#[test]
fn condition_outputs_must_be_declared_and_steps_cannot_read_the_finalization_context() {
    assert_failure(
        &gate_condition("{equals: [{ref: outputs.missing.note}, {value: x}]}"),
        ValidationFailureKind::UnknownOutputStep,
        gate_location(),
    );
    assert_failure(
        &gate_condition("{equals: [{ref: outputs.plan.missing}, {value: x}]}"),
        ValidationFailureKind::UnknownOutput,
        gate_location(),
    );
    let context = "{exists: {ref: finalization.context, pointer: /trigger}}";
    assert_structurally_invalid(&gate_condition(context));
    validate_yaml(&finish_condition(context)).unwrap();
}

// REQ-WFCOND-9
#[test]
fn dispositions_name_another_node_and_steps_cannot_observe_finalizers() {
    let disposition = |node: &str| format!("{{disposition: {{node: {node}, is: succeeded}}}}");
    for node in ["missing", "gate", "finish"] {
        assert_failure(
            &gate_condition(&disposition(node)),
            ValidationFailureKind::InvalidConditionReference,
            gate_location(),
        );
    }
    for node in ["missing", "finish"] {
        assert_failure(
            &finish_condition(&disposition(node)),
            ValidationFailureKind::InvalidConditionReference,
            ValidationLocation::FinalizerCondition {
                finalizer: "finish".to_owned(),
            },
        );
    }
    for node in ["plan", "gate", "report"] {
        validate_yaml(&finish_condition(&disposition(node)))
            .unwrap_or_else(|failure| panic!("finalizer could not observe {node}: {failure:?}"));
    }
}

// REQ-WFCOND-11
#[test]
fn equals_never_mixes_text_and_json_kinds() {
    for accepted in [
        "{equals: [{ref: inputs.request}, {value: x}]}",
        "{equals: [{value: x}, {ref: outputs.plan.note}]}",
        "{equals: [{ref: inputs.request}, {ref: outputs.plan.note}]}",
        "{equals: [{ref: inputs.payload, pointer: /a}, {value: x}]}",
        "{equals: [{ref: inputs.payload}, {ref: inputs.payload, pointer: /a}]}",
    ] {
        validate_yaml(&gate_condition(accepted))
            .unwrap_or_else(|failure| panic!("rejected {accepted}: {failure:?}"));
    }
    for rejected in [
        "{equals: [{ref: inputs.request}, {value: 1}]}",
        "{equals: [{value: null}, {ref: outputs.plan.note}]}",
        "{equals: [{ref: inputs.request}, {ref: inputs.payload}]}",
        "{equals: [{ref: inputs.request, pointer: /a}, {value: x}]}",
    ] {
        assert_failure(
            &gate_condition(rejected),
            ValidationFailureKind::InvalidConditionType,
            gate_location(),
        );
    }
}

// REQ-WFCOND-14
#[test]
fn condition_depth_predicate_and_workflow_limits_count_predicates_not_operands() {
    let nested = |levels: usize| {
        let mut condition = TEXT_EQUALS.to_owned();
        for _ in 1..levels {
            condition = format!("{{not: {condition}}}");
        }
        condition
    };
    // One `any` holding 63 three-leaf `all` groups and one group of `last` leaves.
    let predicates = |last: usize| {
        let group = |leaves: usize| format!("{{all: [{}]}}", vec![TEXT_EQUALS; leaves].join(", "));
        let mut groups = vec![group(3); 63];
        groups.push(group(last));
        format!("{{any: [{}]}}", groups.join(", "))
    };
    validate_yaml(&gate_condition(&nested(16))).unwrap();
    assert_failure(
        &gate_condition(&nested(17)),
        ValidationFailureKind::ConditionDepthExceeded,
        gate_location(),
    );
    validate_yaml(&gate_condition(&predicates(2))).unwrap();
    assert_failure(
        &gate_condition(&predicates(3)),
        ValidationFailureKind::ConditionNodeLimitExceeded,
        gate_location(),
    );

    let workflow = |extra: bool| {
        let mut source = "schemaVersion: 1\ninputs:\n  request: {kind: text}\nsteps:\n".to_owned();
        for index in 0..16 {
            source.push_str(&format!(
                "  step{index}:\n    kind: cmd\n    condition: {}\n    command: {{argv: [\"true\"]}}\n",
                predicates(2)
            ));
        }
        if extra {
            source.push_str(&format!(
                "  extra:\n    kind: cmd\n    condition: {TEXT_EQUALS}\n    command: {{argv: [\"true\"]}}\n"
            ));
        }
        source
    };
    validate_yaml(&workflow(false)).unwrap();
    assert_failure(
        &workflow(true),
        ValidationFailureKind::WorkflowConditionNodeLimitExceeded,
        ValidationLocation::WorkflowGraph,
    );
}

// REQ-WFCOND-16
#[test]
fn every_condition_reference_is_a_prerequisite_even_past_a_short_circuit() {
    let source = "schemaVersion: 1
inputs:
  request: {kind: text}
steps:
  plan:
    kind: cmd
#plan
    command: {argv: [\"true\"]}
    outputs:
      note: {kind: text, from: path, path: note.txt}
  review:
    kind: cmd
#review
    command: {argv: [\"true\"]}
  gate:
    kind: cmd
    condition:
      any:
        - equals: [{ref: inputs.request}, {value: x}]
        - equals: [{ref: outputs.plan.note}, {value: y}]
        - disposition: {node: review, is: succeeded}
    command: {argv: [\"true\"]}
";
    let acyclic = source.replace("#plan\n", "").replace("#review\n", "");
    let workflow = validate_yaml(&acyclic).unwrap();
    let ValidatedStep::Command(gate) = &workflow.steps["gate"] else {
        panic!("gate must be a command step");
    };
    assert_eq!(
        gate.common
            .prerequisites
            .iter()
            .map(|prerequisite| (
                prerequisite.producer.as_str(),
                prerequisite.control,
                prerequisite.disposition_control,
                prerequisite.condition_data,
            ))
            .collect::<Vec<_>>(),
        [("plan", false, false, true), ("review", false, true, false)]
    );
    for producer in ["#plan\n", "#review\n"] {
        let cyclic = source
            .replace(producer, "    dependsOn: [gate]\n")
            .replace("#plan\n", "")
            .replace("#review\n", "");
        assert_failure(
            &cyclic,
            ValidationFailureKind::DependencyCycle,
            ValidationLocation::WorkflowGraph,
        );
    }
}

// REQ-WFFORMAT-35, REQ-WFFORMAT-49
#[test]
fn message_type_table_rejects_every_inverse_destination_without_conversion() {
    let cases = [
        ("inputs.request", "attachment"),
        ("inputs.evidence", "text"),
        ("outputs.responseProducer.response", "attachment"),
        ("outputs.resultProducer.result", "text"),
        ("outputs.responseProducer.file", "text"),
    ];

    for (reference, destination) in cases {
        let failure = validate_yaml(&typed_message_workflow(reference, destination)).unwrap_err();
        assert_eq!(
            failure.kind(),
            ValidationFailureKind::MessageTypeMismatch,
            "unexpected classification for {reference} in {destination}"
        );
        let expected_location = match destination {
            "text" => ValidationLocation::MessageText {
                step: "consumer".to_owned(),
                index: 0,
            },
            "attachment" => ValidationLocation::MessageAttachment {
                step: "consumer".to_owned(),
                index: 0,
            },
            _ => panic!("unknown test destination"),
        };
        assert_eq!(failure.location(), &expected_location);
    }
}

#[test]
fn validated_definition_preserves_explicit_consumption_and_effective_prerequisites() {
    let source = "schemaVersion: 1
description: Typed workflow.
inputs:
  request:
    kind: text
  evidence:
    kind: attachments
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  responseProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      response:
        kind: text
        from: agent_response
      file:
        kind: file
        from: path
        path: artifact.bin
        mediaType: application/octet-stream
      ignored:
        kind: file
        from: path
        path: ignored.bin
        mediaType: application/octet-stream
  resultProducer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: schemas/result.json
  consumer:
    kind: agent
    dependsOn: [responseProducer, resultProducer]
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - ref: inputs.request
          - ref: outputs.responseProducer.response
        attachments:
          - ref: inputs.evidence
          - ref: outputs.resultProducer.result
          - ref: outputs.responseProducer.file
exports:
  response:
    ref: outputs.responseProducer.response
  result:
    ref: outputs.resultProducer.result
  file:
    ref: outputs.responseProducer.file
";
    let workflow = validate_yaml(source).unwrap();

    assert_eq!(
        workflow.declared_inputs,
        std::collections::BTreeMap::from([
            (
                "evidence".to_owned(),
                WorkflowValueType::AttachmentCollection
            ),
            ("request".to_owned(), WorkflowValueType::Text),
        ])
    );
    let consumer_position = workflow
        .presentation_order
        .iter()
        .position(|step| step == "consumer")
        .unwrap();
    for producer in ["responseProducer", "resultProducer"] {
        let producer_position = workflow
            .presentation_order
            .iter()
            .position(|step| step == producer)
            .unwrap();
        assert!(producer_position < consumer_position);
    }
    let ValidatedStep::Agent(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be an agent step");
    };
    assert_eq!(
        prerequisite_shape(&consumer.common.prerequisites),
        [
            ("responseProducer", true, true),
            ("resultProducer", true, true),
        ]
    );
    assert_eq!(
        consumer.agent.message.text,
        [
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Input("request".to_owned()),
                value_type: WorkflowValueType::Text,
            },
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    node: WorkflowNode {
                        id: "responseProducer".to_owned(),
                        role: WorkflowNodeRole::Step,
                    },
                    output: "response".to_owned(),
                    value_type: WorkflowValueType::Text,
                }),
                value_type: WorkflowValueType::Text,
            },
        ]
    );
    assert_eq!(
        consumer.agent.message.attachments,
        [
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Input("evidence".to_owned()),
                value_type: WorkflowValueType::AttachmentCollection,
            },
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    node: WorkflowNode {
                        id: "resultProducer".to_owned(),
                        role: WorkflowNodeRole::Step,
                    },
                    output: "result".to_owned(),
                    value_type: WorkflowValueType::Json,
                }),
                value_type: WorkflowValueType::Json,
            },
            ValidatedMessageSource::Reference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    node: WorkflowNode {
                        id: "responseProducer".to_owned(),
                        role: WorkflowNodeRole::Step,
                    },
                    output: "file".to_owned(),
                    value_type: WorkflowValueType::File,
                }),
                value_type: WorkflowValueType::File,
            },
        ]
    );

    let ValidatedStep::Agent(response_producer) = &workflow.steps["responseProducer"] else {
        panic!("response producer must be an agent step");
    };
    assert_eq!(
        response_producer.common.outputs["response"].value_type,
        WorkflowValueType::Text
    );
    assert_eq!(
        response_producer.common.outputs["file"].value_type,
        WorkflowValueType::File
    );
    let ValidatedStep::Agent(result_producer) = &workflow.steps["resultProducer"] else {
        panic!("result producer must be an agent step");
    };
    assert_eq!(
        result_producer.common.outputs["result"].value_type,
        WorkflowValueType::Json
    );
    assert_eq!(
        response_producer.common.outputs["ignored"].value_type,
        WorkflowValueType::File
    );
    for (name, step, expected_type) in [
        ("response", "responseProducer", WorkflowValueType::Text),
        ("result", "resultProducer", WorkflowValueType::Json),
        ("file", "responseProducer", WorkflowValueType::File),
    ] {
        assert_eq!(workflow.exports[name].node.id, step);
        assert_eq!(workflow.exports[name].node.role, WorkflowNodeRole::Step);
        assert_eq!(workflow.exports[name].output, name);
        assert_eq!(workflow.exports[name].value_type, expected_type);
    }
}

// REQ-WFFORMAT-29, REQ-WFFORMAT-39, REQ-WFFORMAT-55
#[test]
fn direct_message_reference_failures_are_reported_at_the_message_location() {
    for (reference, kind) in [
        ("inputs.unknown", ValidationFailureKind::UnknownInput),
        (
            "outputs.missing.value",
            ValidationFailureKind::UnknownOutputStep,
        ),
        (
            "outputs.responseProducer.missing",
            ValidationFailureKind::UnknownOutput,
        ),
    ] {
        let failure = validate_yaml(&typed_message_workflow(reference, "text")).unwrap_err();
        assert_eq!(failure.kind(), kind);
        assert_eq!(
            failure.location(),
            &ValidationLocation::MessageText {
                step: "consumer".to_owned(),
                index: 0,
            }
        );
    }

    let data_only = typed_message_workflow("outputs.responseProducer.response", "text")
        .replace("    dependsOn: [responseProducer, resultProducer]\n", "");
    let workflow = validate_yaml(&data_only).unwrap();
    let ValidatedStep::Agent(consumer) = &workflow.steps["consumer"] else {
        panic!("consumer must be an agent step");
    };
    assert_eq!(
        prerequisite_shape(&consumer.common.prerequisites),
        [("responseProducer", false, true)]
    );
}

// REQ-WFFORMAT-43
#[test]
fn git_branch_is_exportable_but_rejected_from_every_downstream_binding() {
    let producer = "schemaVersion: 1
steps:
  produce:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      changes:
        kind: git_branch
        from: workspace
";
    let exported = format!("{producer}exports:\n  changes:\n    ref: outputs.produce.changes\n");
    let workflow = validate_yaml(&exported).unwrap();
    assert_eq!(
        workflow.exports["changes"].value_type,
        WorkflowValueType::GitBranch
    );

    let command = format!(
        "{producer}  consume:\n    kind: cmd\n    inputs:\n      changes:\n        ref: outputs.produce.changes\n    command:\n      argv: [\"true\"]\n"
    );
    assert_failure(
        &command,
        ValidationFailureKind::TerminalOutputReference,
        ValidationLocation::StepInput {
            step: "consume".to_owned(),
            input: "changes".to_owned(),
        },
    );

    for (message, location) in [
        (
            "        text: [{ ref: outputs.produce.changes }]",
            ValidationLocation::MessageText {
                step: "consume".to_owned(),
                index: 0,
            },
        ),
        (
            "        text: [{ file: system.md }]\n        attachments: [{ ref: outputs.produce.changes }]",
            ValidationLocation::MessageAttachment {
                step: "consume".to_owned(),
                index: 0,
            },
        ),
    ] {
        let agent = format!(
            "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: openai/gpt-5\n        thinking: high\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      changes:\n        kind: git_branch\n        from: workspace\n  consume:\n    kind: agent\n    agent:\n      profile: coding\n      systemPrompt: system.md\n      message:\n{message}\n"
        );
        assert_failure(
            &agent,
            ValidationFailureKind::TerminalOutputReference,
            location,
        );
    }
}

// REQ-WFFORMAT-56
#[test]
fn semantic_outputs_definition_rejects_duplicate_paths_and_excess_workspace_sources() {
    let duplicate_path = r#"schemaVersion: 1
steps:
  produce:
    kind: cmd
    command:
      argv: ["true"]
    outputs:
      first:
        kind: text
        from: path
        path: value
      second:
        kind: file
        from: path
        path: value
        mediaType: application/octet-stream
"#;
    assert_failure(
        duplicate_path,
        ValidationFailureKind::DuplicateOutputPath,
        ValidationLocation::StepOutput {
            step: "produce".to_owned(),
            output: "second".to_owned(),
        },
    );

    let excess_workspace = r#"schemaVersion: 1
steps:
  produce:
    kind: cmd
    command:
      argv: ["true"]
    outputs:
      first:
        kind: git_branch
        from: workspace
      second:
        kind: git_branch
        from: workspace
"#;
    assert_failure(
        excess_workspace,
        ValidationFailureKind::ExcessWorkspaceOutput,
        ValidationLocation::StepOutput {
            step: "produce".to_owned(),
            output: "second".to_owned(),
        },
    );

    let equal_paths_in_different_nodes = duplicate_path.replace(
        "      second:\n        kind: file\n        from: path\n        path: value\n        mediaType: application/octet-stream\n",
        "  consume:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      second:\n        kind: file\n        from: path\n        path: value\n        mediaType: application/octet-stream\n",
    );
    assert!(validate_yaml(&equal_paths_in_different_nodes).is_ok());
}

// REQ-WFFORMAT-56
#[test]
fn agent_output_cardinality_rules_are_enforced() {
    let two_responses = "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  agent:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
    outputs:
      first:
        kind: text
        from: agent_response
      second:
        kind: text
        from: agent_response
";
    assert_failure(
        two_responses,
        ValidationFailureKind::ExcessAgentResponseOutput,
        ValidationLocation::StepOutput {
            step: "agent".to_owned(),
            output: "second".to_owned(),
        },
    );

    let two_results = two_responses.replace(
        "kind: text\n        from: agent_response",
        "kind: json\n        from: agent_result\n        schema: schemas/result.json",
    );
    assert_failure(
        &two_results,
        ValidationFailureKind::ExcessAgentResultOutput,
        ValidationLocation::StepOutput {
            step: "agent".to_owned(),
            output: "second".to_owned(),
        },
    );

    let conflicting_values = two_responses.replacen(
        "kind: text\n        from: agent_response",
        "kind: json\n        from: agent_result\n        schema: schemas/result.json",
        1,
    );
    assert_failure(
        &conflicting_values,
        ValidationFailureKind::ConflictingAgentValueOutputs,
        ValidationLocation::StepOutput {
            step: "agent".to_owned(),
            output: "second".to_owned(),
        },
    );
}

// REQ-WFFORMAT-65
#[test]
fn exports_must_target_declared_outputs() {
    let base = "schemaVersion: 1
steps:
  command:
    kind: cmd
    command:
      argv: [\"true\"]
    outputs:
      report:
        kind: file
        from: path
        path: report.txt
        mediaType: text/plain
exports:
  public:
    ref: TARGET
";
    for target in ["outputs.missing.report", "outputs.command.missing"] {
        assert_failure(
            &base.replace("TARGET", target),
            ValidationFailureKind::InvalidExportTarget,
            ValidationLocation::Export {
                name: "public".to_owned(),
            },
        );
    }
}

const OUTCOME_WORKFLOW: &str = "schemaVersion: 1
inputs:
  request: {kind: text}
#outcomes
steps:
  decide:
    kind: cmd
    command: {argv: [\"true\"]}
    outputs:
      verdict: {kind: text, from: path, path: verdict.txt}
      structured: {kind: json, from: path, path: verdict.json, schema: verdict.schema.json}
      report: {kind: file, from: path, path: report.txt, mediaType: text/plain}
      changes: {kind: git_branch, from: workspace}
  advise:
    kind: cmd
    failurePolicy: advisory
    command: {argv: [\"true\"]}
    outputs:
      verdict: {kind: text, from: path, path: advice.txt}
  gated:
    kind: cmd
    condition:
      equals:
        - ref: inputs.request
        - value: exact
    command: {argv: [\"true\"]}
    outputs:
      verdict: {kind: text, from: path, path: gated.txt}
finalizers:
  report:
    kind: cmd
    command: {argv: [\"true\"]}
    outputs:
      verdict: {kind: text, from: path, path: final.txt}
";

fn outcome_workflow(declaration: &str) -> String {
    OUTCOME_WORKFLOW.replace("#outcomes\n", declaration)
}

fn declared_outcome(from: &str, on_failure: &str) -> String {
    outcome_workflow(&format!(
        "outcomes: [planned, failed]\noutcome: {{from: {from}, onFailure: {on_failure}}}\n"
    ))
}

// REQ-WFFORMAT-87, REQ-WFFORMAT-88
#[test]
fn outcomes_and_outcome_are_declared_together_with_a_member_failure_name() {
    for source in [
        outcome_workflow("outcome: {from: outputs.decide.verdict, onFailure: failed}\n"),
        outcome_workflow("outcomes: [planned, failed]\n"),
        declared_outcome("outputs.decide.verdict", "shipped"),
    ] {
        assert_failure(
            &source,
            ValidationFailureKind::InvalidOutcomeDeclaration,
            ValidationLocation::Outcome,
        );
    }

    let workflow = validate_yaml(&declared_outcome("outputs.decide.verdict", "failed")).unwrap();
    assert_eq!(
        workflow.outcome,
        Some(ValidatedOutcome {
            names: vec!["planned".to_owned(), "failed".to_owned()],
            source: ResolvedOutputSource {
                node: WorkflowNode {
                    id: "decide".to_owned(),
                    role: WorkflowNodeRole::Step,
                },
                output: "verdict".to_owned(),
                value_type: WorkflowValueType::Text,
            },
            on_failure: "failed".to_owned(),
        })
    );

    let undeclared = validate_yaml(&outcome_workflow("")).unwrap();
    assert_eq!(undeclared.outcome, None);
}

// REQ-WFFORMAT-88
#[test]
fn the_outcome_source_is_a_text_output_of_a_required_unconditional_step() {
    for from in [
        "outputs.missing.verdict",
        "outputs.decide.missing",
        "outputs.decide.structured",
        "outputs.decide.report",
        "outputs.decide.changes",
        "outputs.advise.verdict",
        "outputs.report.verdict",
        "outputs.gated.verdict",
    ] {
        let failure = validate_yaml(&declared_outcome(from, "failed")).unwrap_err();
        assert_eq!(
            (failure.kind(), failure.location()),
            (
                ValidationFailureKind::InvalidOutcomeSource,
                &ValidationLocation::Outcome
            ),
            "unexpected failure for {from}"
        );
    }
}

const EDGE_WORKFLOW: &str = "schemaVersion: 1
inputs:
  ticket:
    kind: text
  policy:
    kind: json
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    inputs:
      ticket:
        ref: inputs.ticket
    on:
      planned: build
      blocked: exit.failed
      failed: exit.failed
  build:
    kind: workflow
    workflow: build.yaml
    inputs:
      parts:
        ref: outputs.design.parts
    on:
      built: exit.landed
      failed: exit.failed
";

fn design_summary() -> ChildWorkflowSummary {
    ChildWorkflowSummary {
        outcomes: ["planned", "blocked", "failed"].map(str::to_owned).to_vec(),
        exports: [
            ("parts", WorkflowValueType::Json),
            ("plan", WorkflowValueType::Text),
            ("report", WorkflowValueType::File),
            ("branch", WorkflowValueType::GitBranch),
        ]
        .into_iter()
        .map(|(name, value_type)| (name.to_owned(), value_type))
        .collect(),
        inputs: [
            ("ticket", (WorkflowValueType::Text, false)),
            ("notes", (WorkflowValueType::Text, true)),
            ("answers", (WorkflowValueType::Json, true)),
            ("brief", (WorkflowValueType::File, true)),
            ("evidence", (WorkflowValueType::AttachmentCollection, true)),
        ]
        .into_iter()
        .map(|(name, input)| (name.to_owned(), input))
        .collect(),
        has_workflow_steps: false,
        has_root_blocks: false,
        paths: BTreeSet::new(),
    }
}

fn build_summary() -> ChildWorkflowSummary {
    ChildWorkflowSummary {
        outcomes: vec!["built".to_owned(), "failed".to_owned()],
        exports: BTreeMap::new(),
        inputs: [
            ("parts", (WorkflowValueType::Json, false)),
            ("part", (WorkflowValueType::Text, true)),
            ("answers", (WorkflowValueType::Json, true)),
        ]
        .into_iter()
        .map(|(name, input)| (name.to_owned(), input))
        .collect(),
        has_workflow_steps: false,
        has_root_blocks: false,
        paths: BTreeSet::new(),
    }
}

fn child_summaries() -> BTreeMap<String, ChildWorkflowSummary> {
    [
        ("design.yaml".to_owned(), design_summary()),
        ("build.yaml".to_owned(), build_summary()),
    ]
    .into()
}

fn validate_with_children(
    source: &str,
    children: &BTreeMap<String, ChildWorkflowSummary>,
) -> Result<ValidatedWorkflow, super::ValidationFailure> {
    super::validate_with_children(decode(source.as_bytes()).unwrap(), None, children)
}

fn assert_edge_failure(
    source: &str,
    children: &BTreeMap<String, ChildWorkflowSummary>,
    kind: ValidationFailureKind,
    location: ValidationLocation,
) {
    let failure = validate_with_children(source, children).unwrap_err();
    assert_eq!(
        (failure.kind(), failure.location()),
        (kind, &location),
        "unexpected failure for:\n{source}"
    );
}

fn edge_workflow(from: &str, to: &str) -> String {
    assert!(EDGE_WORKFLOW.contains(from), "{from}");
    EDGE_WORKFLOW.replacen(from, to, 1)
}

fn step_input(step: &str, input: &str) -> ValidationLocation {
    ValidationLocation::StepInput {
        step: step.to_owned(),
        input: input.to_owned(),
    }
}

fn step_edge(step: &str, outcome: &str) -> ValidationLocation {
    ValidationLocation::StepEdge {
        step: step.to_owned(),
        outcome: outcome.to_owned(),
    }
}

fn workflow_step<'a>(workflow: &'a ValidatedWorkflow, step: &str) -> &'a ValidatedWorkflowStep {
    let ValidatedStep::Workflow(step) = &workflow.steps[step] else {
        panic!("{step} validates as a workflow step");
    };
    step
}

// REQ-WFFORMAT-90
#[test]
fn workflow_step_inputs_map_the_child_declaration_with_its_kinds() {
    let workflow = validate_with_children(EDGE_WORKFLOW, &child_summaries()).unwrap();
    let design = workflow_step(&workflow, "design");
    assert_eq!(design.workflow, "design.yaml");
    assert_eq!(
        design.inputs.keys().collect::<Vec<_>>(),
        ["ticket"],
        "unmapped optional and defaulted child inputs are omitted"
    );
    assert_eq!(
        design.inputs["ticket"].reference.source,
        ResolvedValueSource::Input("ticket".to_owned())
    );
    assert!(!design.inputs["ticket"].may_be_omitted);
    let build = workflow_step(&workflow, "build");
    assert_eq!(
        build.inputs["parts"].reference.value_type,
        WorkflowValueType::Json
    );

    let ticket_mapping = "    inputs:\n      ticket:\n        ref: inputs.ticket\n";
    for (source, kind, location) in [
        (
            edge_workflow(
                ticket_mapping,
                &format!("{ticket_mapping}      extra:\n        ref: inputs.ticket\n"),
            ),
            ValidationFailureKind::ChildInputUndeclared,
            step_input("design", "extra"),
        ),
        (
            edge_workflow(ticket_mapping, ""),
            ValidationFailureKind::ChildInputMissing,
            step_input("design", "ticket"),
        ),
        (
            edge_workflow("ref: inputs.ticket", "ref: inputs.policy"),
            ValidationFailureKind::ChildInputTypeMismatch,
            step_input("design", "ticket"),
        ),
        (
            edge_workflow(
                ticket_mapping,
                &format!("{ticket_mapping}      brief:\n        ref: inputs.ticket\n"),
            ),
            ValidationFailureKind::ChildInputUnsupported,
            step_input("design", "brief"),
        ),
        (
            edge_workflow(
                ticket_mapping,
                &format!("{ticket_mapping}      evidence:\n        ref: inputs.policy\n"),
            ),
            ValidationFailureKind::ChildInputUnsupported,
            step_input("design", "evidence"),
        ),
    ] {
        assert_edge_failure(&source, &child_summaries(), kind, location);
    }
}

// REQ-WFFORMAT-91
#[test]
fn workflow_step_outputs_are_the_child_text_and_json_exports_and_its_outcome() {
    let workflow = validate_with_children(EDGE_WORKFLOW, &child_summaries()).unwrap();
    let outputs = &workflow_step(&workflow, "design").common.outputs;
    assert_eq!(
        outputs
            .iter()
            .map(|(name, output)| (name.as_str(), output.value_type))
            .collect::<Vec<_>>(),
        [
            ("outcome", WorkflowValueType::Text),
            ("parts", WorkflowValueType::Json),
            ("plan", WorkflowValueType::Text),
        ]
    );
    assert_eq!(outputs["outcome"].definition, Output::ChildOutcome);
    assert!(matches!(
        outputs["parts"].definition,
        Output::ChildExport { .. }
    ));
    let design = workflow_step(&workflow, "design");
    assert!(design.common.prerequisites.is_empty());
    assert!(design.common.evidence_prerequisites.is_empty());

    let mut reserved = child_summaries();
    reserved
        .get_mut("design.yaml")
        .unwrap()
        .exports
        .insert("outcome".to_owned(), WorkflowValueType::Text);
    assert_edge_failure(
        EDGE_WORKFLOW,
        &reserved,
        ValidationFailureKind::ChildExportReserved,
        ValidationLocation::StepWorkflow {
            step: "design".to_owned(),
        },
    );

    for unexported in ["report", "branch", "missing"] {
        assert_edge_failure(
            &edge_workflow(
                "ref: outputs.design.parts",
                &format!("ref: outputs.design.{unexported}"),
            ),
            &child_summaries(),
            ValidationFailureKind::UnknownOutput,
            step_input("build", "parts"),
        );
    }
}

// REQ-WFFORMAT-92
#[test]
fn workflow_step_edges_route_every_child_outcome_to_one_target() {
    for (source, kind, location) in [
        (
            edge_workflow(
                "      blocked: exit.failed\n",
                "      blocked: exit.failed\n      shipped: exit.failed\n",
            ),
            ValidationFailureKind::EdgeOutcomeUndeclared,
            step_edge("design", "shipped"),
        ),
        (
            edge_workflow("      blocked: exit.failed\n", ""),
            ValidationFailureKind::EdgeOutcomeMissing,
            step_edge("design", "blocked"),
        ),
        (
            edge_workflow("planned: build", "planned: review"),
            ValidationFailureKind::EdgeTargetUnknown,
            step_edge("design", "planned"),
        ),
        (
            edge_workflow("built: exit.landed", "built: exit.shipped"),
            ValidationFailureKind::EdgeExitUndeclared,
            step_edge("build", "built"),
        ),
        (
            edge_workflow("built: exit.landed", "built: exit.succeeded"),
            ValidationFailureKind::EdgeExitUndeclared,
            step_edge("build", "built"),
        ),
        (
            edge_workflow("outcomes: [landed, failed]\n", ""),
            ValidationFailureKind::EdgeExitUndeclared,
            step_edge("build", "built"),
        ),
        (
            edge_workflow(
                "      blocked: exit.failed\n",
                "      blocked:\n        wait: repair\n        then: review\n",
            ),
            ValidationFailureKind::EdgeTargetUnknown,
            step_edge("design", "blocked"),
        ),
        (
            edge_workflow(
                "      blocked: exit.failed\n",
                "      blocked:\n        wait: answer\n        then: exit.shipped\n",
            ),
            ValidationFailureKind::EdgeExitUndeclared,
            step_edge("design", "blocked"),
        ),
    ] {
        assert_edge_failure(&source, &child_summaries(), kind, location);
    }

    // A step target may name the step itself or an earlier step.
    for target in ["design", "build"] {
        let workflow = validate_with_children(
            &edge_workflow("built: exit.landed", &format!("built: {target}")),
            &child_summaries(),
        )
        .unwrap();
        assert_eq!(
            workflow_step(&workflow, "build").edges["built"],
            ValidatedEdgeTarget::Step(target.to_owned())
        );
    }

    let implicit = edge_workflow("outcomes: [landed, failed]\n", "")
        .replace("built: exit.landed", "built: exit.succeeded");
    let workflow = validate_with_children(&implicit, &child_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "build").edges["built"],
        ValidatedEdgeTarget::Exit("succeeded".to_owned())
    );

    let waiting = edge_workflow(
        "      blocked: exit.failed\n",
        "      blocked:\n        wait: repair\n        then: build\n",
    );
    let workflow = validate_with_children(&waiting, &child_summaries()).unwrap();
    let design = workflow_step(&workflow, "design");
    assert_eq!(
        design.edges["blocked"],
        ValidatedEdgeTarget::Wait {
            kind: WaitKind::Repair,
            then: Box::new(ValidatedEdgeTarget::Step("build".to_owned())),
            on_hand_back: None,
            rule: None,
        }
    );
    assert_eq!(
        design.edges["planned"],
        ValidatedEdgeTarget::Step("build".to_owned())
    );
}

// REQ-WFFORMAT-92
#[test]
fn an_approval_wait_names_a_hand_back_step_and_no_other_wait_does() {
    for (source, kind, location) in [
        (
            edge_workflow(
                "      blocked: exit.failed\n",
                "      blocked:\n        wait: approval\n        then: build\n",
            ),
            ValidationFailureKind::WaitHandBackRequired,
            step_edge("design", "blocked"),
        ),
        (
            edge_workflow(
                "      blocked: exit.failed\n",
                "      blocked:\n        wait: answer\n        then: build\n        onHandBack: build\n",
            ),
            ValidationFailureKind::WaitHandBackUnsupported,
            step_edge("design", "blocked"),
        ),
        (
            edge_workflow(
                "      blocked: exit.failed\n",
                "      blocked:\n        wait: approval\n        then: build\n        onHandBack: review\n",
            ),
            ValidationFailureKind::EdgeTargetUnknown,
            step_edge("design", "blocked"),
        ),
    ] {
        assert_edge_failure(&source, &child_summaries(), kind, location);
    }

    // A hand-back may continue at an earlier step.
    let earlier = edge_workflow(
        "built: exit.landed",
        "built:\n        wait: approval\n        then: exit.landed\n        onHandBack: design",
    );
    let workflow = validate_with_children(&earlier, &child_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "build").edges["built"],
        ValidatedEdgeTarget::Wait {
            kind: WaitKind::Approval,
            then: Box::new(ValidatedEdgeTarget::Exit("landed".to_owned())),
            on_hand_back: Some("design".to_owned()),
            rule: None,
        }
    );

    // `fix` is the target of no edge but the hand-back of `build`'s approval wait.
    let hand_back = edge_workflow(
        "built: exit.landed",
        "built:\n        wait: approval\n        then: exit.landed\n        onHandBack: fix",
    ) + "  fix:
    kind: workflow
    workflow: build.yaml
    inputs:
      parts:
        ref: outputs.design.parts
    on:
      built: exit.landed
      failed: exit.failed
";
    let workflow = validate_with_children(&hand_back, &child_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "build").edges["built"],
        ValidatedEdgeTarget::Wait {
            kind: WaitKind::Approval,
            then: Box::new(ValidatedEdgeTarget::Exit("landed".to_owned())),
            on_hand_back: Some("fix".to_owned()),
            rule: None,
        }
    );
}

// REQ-WFFORMAT-92
#[test]
fn a_wait_may_continue_at_the_waiting_step_or_an_earlier_one() {
    let repair_self = edge_workflow(
        "      built: exit.landed\n      failed: exit.failed\n",
        "      built: exit.landed\n      failed:\n        wait: repair\n        then: build\n",
    );
    let workflow = validate_with_children(&repair_self, &child_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "build").edges["failed"],
        ValidatedEdgeTarget::Wait {
            kind: WaitKind::Repair,
            then: Box::new(ValidatedEdgeTarget::Step("build".to_owned())),
            on_hand_back: None,
            rule: None,
        }
    );

    let answer_self = edge_workflow(
        "      blocked: exit.failed\n",
        "      blocked:\n        wait: answer\n        then: design\n",
    );
    validate_with_children(&answer_self, &child_summaries()).unwrap();

    let hand_back_self = edge_workflow(
        "built: exit.landed",
        "built:\n        wait: approval\n        then: exit.landed\n        onHandBack: build",
    );
    let workflow = validate_with_children(&hand_back_self, &child_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "build").edges["built"],
        ValidatedEdgeTarget::Wait {
            kind: WaitKind::Approval,
            then: Box::new(ValidatedEdgeTarget::Exit("landed".to_owned())),
            on_hand_back: Some("build".to_owned()),
            rule: None,
        }
    );

    let answer_earlier = edge_workflow(
        "      built: exit.landed\n",
        "      built:\n        wait: answer\n        then: design\n",
    );
    let workflow = validate_with_children(&answer_earlier, &child_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "build").edges["built"],
        ValidatedEdgeTarget::Wait {
            kind: WaitKind::Answer,
            then: Box::new(ValidatedEdgeTarget::Step("design".to_owned())),
            on_hand_back: None,
            rule: None,
        }
    );

    // A step whose only targeting edge is its own wait is not reached.
    assert_edge_failure(
        &edge_workflow(
            "      built: exit.landed\n      failed: exit.failed\n",
            "      built: exit.landed\n      failed:\n        wait: repair\n        then: build\n",
        )
        .replace("planned: build", "planned: exit.landed"),
        &child_summaries(),
        ValidationFailureKind::StepUnreachable,
        ValidationLocation::Step {
            step: "build".to_owned(),
        },
    );
}

// REQ-WFFORMAT-96
#[test]
fn a_limited_edge_continues_at_a_step_until_its_limit_and_then_takes_else() {
    let limited = |then: &str, else_: &str| {
        format!("planned:\n        then: {then}\n        limit: 2\n        else: {else_}")
    };
    for (source, kind) in [
        (
            edge_workflow("planned: build", &limited("exit.landed", "build")),
            ValidationFailureKind::EdgeLimitTargetInvalid,
        ),
        (
            edge_workflow("planned: build", &limited("build", "review")),
            ValidationFailureKind::EdgeTargetUnknown,
        ),
        (
            edge_workflow("planned: build", &limited("review", "build")),
            ValidationFailureKind::EdgeTargetUnknown,
        ),
        (
            edge_workflow("planned: build", &limited("build", "exit.shipped")),
            ValidationFailureKind::EdgeExitUndeclared,
        ),
    ] {
        assert_edge_failure(
            &source,
            &child_summaries(),
            kind,
            step_edge("design", "planned"),
        );
    }

    // `build` is reached only through the limited edge's `then`, or only through its `else`.
    let through_then = edge_workflow("planned: build", &limited("build", "exit.failed"));
    let workflow = validate_with_children(&through_then, &child_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "design").edges["planned"],
        ValidatedEdgeTarget::Limited {
            then: "build".to_owned(),
            limit: 2,
            else_: Box::new(ValidatedEdgeTarget::Exit("failed".to_owned())),
        }
    );
    let through_else = edge_workflow(
        "planned: build",
        "planned:\n        then: design\n        limit: 2\n        else:\n          wait: repair\n          then: build",
    );
    let workflow = validate_with_children(&through_else, &child_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "design").edges["planned"],
        ValidatedEdgeTarget::Limited {
            then: "design".to_owned(),
            limit: 2,
            else_: Box::new(ValidatedEdgeTarget::Wait {
                kind: WaitKind::Repair,
                then: Box::new(ValidatedEdgeTarget::Step("build".to_owned())),
                on_hand_back: None,
                rule: None,
            }),
        }
    );
}

const EXTERNAL_WAIT_WORKFLOW: &str = r#"schemaVersion: 1
inputs:
  ticket:
    kind: text
  policy:
    kind: json
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    inputs:
      ticket:
        ref: inputs.ticket
    on:
      planned: review
      blocked: exit.failed
      failed: exit.failed
  review:
    kind: workflow
    workflow: review.yaml
    on:
      proceed: merge
      failed: exit.failed
  merge:
    kind: workflow
    workflow: merge.yaml
    on:
      admitted:
        wait: external
        then: merge
        rule:
          source: github
          event: pull_request.queue_settled
          where: 'event.pullRequest == outputs.design.pullRequest and event.state == "merged"'
      landed: exit.landed
      failed: exit.failed
"#;

fn external_wait_summaries() -> BTreeMap<String, ChildWorkflowSummary> {
    let mut design = design_summary();
    design
        .exports
        .insert("pullRequest".to_owned(), WorkflowValueType::Text);
    let child = |outcomes: &[&str], exports: &[(&str, WorkflowValueType)]| ChildWorkflowSummary {
        outcomes: outcomes
            .iter()
            .map(|outcome| (*outcome).to_owned())
            .collect(),
        exports: exports
            .iter()
            .map(|(name, value_type)| ((*name).to_owned(), *value_type))
            .collect(),
        inputs: BTreeMap::new(),
        has_workflow_steps: false,
        has_root_blocks: false,
        paths: BTreeSet::new(),
    };
    [
        ("design.yaml".to_owned(), design),
        (
            "review.yaml".to_owned(),
            child(
                &["proceed", "failed"],
                &[("findings", WorkflowValueType::Json)],
            ),
        ),
        (
            "merge.yaml".to_owned(),
            child(
                &["admitted", "landed", "failed"],
                &[("pullRequest", WorkflowValueType::Text)],
            ),
        ),
    ]
    .into()
}

fn external_wait(from: &str, to: &str) -> String {
    assert!(EXTERNAL_WAIT_WORKFLOW.contains(from), "{from}");
    EXTERNAL_WAIT_WORKFLOW.replacen(from, to, 1)
}

// REQ-WFFORMAT-97
#[test]
fn an_external_wait_carries_a_rule_over_earlier_text_values() {
    let where_ = "outputs.design.pullRequest and event.state";
    for (source, kind) in [
        (
            external_wait(
                "        rule:\n          source: github\n          event: pull_request.queue_settled\n          where: 'event.pullRequest == outputs.design.pullRequest and event.state == \"merged\"'\n",
                "",
            ),
            ValidationFailureKind::WaitRuleRequired,
        ),
        (
            external_wait("wait: external", "wait: repair"),
            ValidationFailureKind::WaitRuleUnsupported,
        ),
        (
            external_wait("source: github", "source: um"),
            ValidationFailureKind::WaitRuleInvalid,
        ),
        (
            external_wait(
                "'event.pullRequest == outputs.design.pullRequest and event.state == \"merged\"'",
                "'event.x != run.id'",
            ),
            ValidationFailureKind::WaitRuleInvalid,
        ),
        (
            external_wait(where_, "outputs.review.findings and event.state"),
            ValidationFailureKind::WaitRuleReferenceInvalid,
        ),
        (
            external_wait(where_, "outputs.merge.pullRequest and event.state"),
            ValidationFailureKind::WaitRuleReferenceInvalid,
        ),
        (
            external_wait(where_, "outputs.design.branch and event.state"),
            ValidationFailureKind::WaitRuleReferenceInvalid,
        ),
        (
            external_wait(where_, "inputs.repository and event.state"),
            ValidationFailureKind::WaitRuleReferenceInvalid,
        ),
        (
            external_wait(where_, "inputs.policy and event.state"),
            ValidationFailureKind::WaitRuleReferenceInvalid,
        ),
    ] {
        assert_edge_failure(
            &source,
            &external_wait_summaries(),
            kind,
            step_edge("merge", "admitted"),
        );
    }

    let workflow =
        validate_with_children(EXTERNAL_WAIT_WORKFLOW, &external_wait_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "merge").edges["admitted"],
        ValidatedEdgeTarget::Wait {
            kind: WaitKind::External,
            then: Box::new(ValidatedEdgeTarget::Step("merge".to_owned())),
            on_hand_back: None,
            rule: Some(WaitRule {
                source: "github".to_owned(),
                event: vec!["pull_request.queue_settled".to_owned()],
                where_:
                    "event.pullRequest == outputs.design.pullRequest and event.state == \"merged\""
                        .to_owned(),
            }),
        }
    );
    validate_with_children(
        &external_wait(where_, "inputs.ticket and event.state"),
        &external_wait_summaries(),
    )
    .unwrap();
}

// REQ-WFFORMAT-95
#[test]
fn control_references_map_onto_optional_child_inputs_of_their_kind() {
    let ticket_mapping = "    inputs:\n      ticket:\n        ref: inputs.ticket\n";
    let mapped = |notes: &str, answers: &str| {
        edge_workflow(
            ticket_mapping,
            &format!(
                "{ticket_mapping}      notes:\n        ref: {notes}\n      answers:\n        ref: {answers}\n"
            ),
        )
    };
    let workflow = validate_with_children(
        &mapped("wait.notes", "controls.answers"),
        &child_summaries(),
    )
    .unwrap();
    let design = workflow_step(&workflow, "design");
    assert_eq!(
        (
            &design.inputs["notes"].reference.source,
            design.inputs["notes"].reference.value_type,
        ),
        (
            &ResolvedValueSource::Wait("notes".to_owned()),
            WorkflowValueType::Text
        )
    );
    assert_eq!(
        (
            &design.inputs["answers"].reference.source,
            design.inputs["answers"].reference.value_type,
        ),
        (
            &ResolvedValueSource::ControlsAnswers,
            WorkflowValueType::Json
        )
    );

    for input in ["notes", "answers"] {
        let mut required = child_summaries();
        required
            .get_mut("design.yaml")
            .unwrap()
            .inputs
            .get_mut(input)
            .unwrap()
            .1 = false;
        assert_edge_failure(
            &mapped("wait.notes", "controls.answers"),
            &required,
            ValidationFailureKind::ControlReferenceRequiresOptional,
            step_input("design", input),
        );
    }
    assert_edge_failure(
        &mapped("inputs.ticket", "wait.notes"),
        &child_summaries(),
        ValidationFailureKind::ChildInputTypeMismatch,
        step_input("design", "answers"),
    );
    assert_edge_failure(
        &mapped("controls.answers", "inputs.policy"),
        &child_summaries(),
        ValidationFailureKind::ChildInputTypeMismatch,
        step_input("design", "notes"),
    );
}

const BUILD_MAPPING: &str =
    "    workflow: build.yaml\n    inputs:\n      parts:\n        ref: outputs.design.parts\n";

/// `EDGE_WORKFLOW` whose `build` step carries `for_each` (omitted when empty) and maps
/// `part` and `answers` onto the references given.
fn fan_out_workflow(for_each: &str, part: &str, answers: &str) -> String {
    edge_workflow(
        BUILD_MAPPING,
        &format!(
            "    workflow: build.yaml\n{for_each}    inputs:\n      parts:\n        ref: outputs.design.parts\n      part:\n        ref: {part}\n      answers:\n        ref: {answers}\n"
        ),
    )
}

fn step_for_each(step: &str) -> ValidationLocation {
    ValidationLocation::StepForEach {
        step: step.to_owned(),
    }
}

// REQ-WFFORMAT-98, REQ-WFFORMAT-99
#[test]
fn for_each_iterates_a_json_value_and_maps_item_onto_child_inputs() {
    let over_parts = "    forEach: outputs.design.parts\n";
    let workflow = validate_with_children(
        &fan_out_workflow(over_parts, "item.name", "item"),
        &child_summaries(),
    )
    .unwrap();
    let build = workflow_step(&workflow, "build");
    assert_eq!(
        build.for_each,
        Some(ValidatedForEach {
            items: ResolvedValueReference {
                source: ResolvedValueSource::Output(ResolvedOutputSource {
                    node: WorkflowNode {
                        id: "design".to_owned(),
                        role: WorkflowNodeRole::Step,
                    },
                    output: "parts".to_owned(),
                    value_type: WorkflowValueType::Json,
                }),
                value_type: WorkflowValueType::Json,
            },
            empty: None,
        })
    );
    let input = |name: &str| {
        (
            build.inputs[name].reference.source.clone(),
            build.inputs[name].reference.value_type,
        )
    };
    assert_eq!(
        input("part"),
        (
            ResolvedValueSource::Item {
                field: Some("name".to_owned())
            },
            WorkflowValueType::Text
        )
    );
    assert_eq!(
        input("answers"),
        (
            ResolvedValueSource::Item { field: None },
            WorkflowValueType::Json
        )
    );

    let object = |empty: &str| {
        fan_out_workflow(
            &format!("    forEach:\n      items: outputs.design.parts\n      empty: {empty}\n"),
            "item.name",
            "item",
        )
    };
    let workflow = validate_with_children(&object("built"), &child_summaries()).unwrap();
    assert_eq!(
        workflow_step(&workflow, "build")
            .for_each
            .as_ref()
            .unwrap()
            .empty
            .as_deref(),
        Some("built")
    );
    assert_edge_failure(
        &object("shipped"),
        &child_summaries(),
        ValidationFailureKind::ForEachEmptyUndeclared,
        step_for_each("build"),
    );

    for items in [
        "outputs.design.plan",
        "inputs.ticket",
        "outputs.build.parts",
    ] {
        assert_edge_failure(
            &fan_out_workflow(&format!("    forEach: {items}\n"), "item.name", "item"),
            &child_summaries(),
            ValidationFailureKind::ForEachReferenceInvalid,
            step_for_each("build"),
        );
    }
    // The structural schema admits only `inputs.` and `outputs.` forms; the validator
    // refuses an engine value there all the same.
    for items in [
        ValueReference::Wait {
            field: "notes".to_owned(),
        },
        ValueReference::ControlsAnswers,
        ValueReference::Item { field: None },
        ValueReference::FinalizationContext,
    ] {
        let mut document =
            decode(fan_out_workflow(over_parts, "item.name", "item").as_bytes()).unwrap();
        let NodeBody::Workflow(node) = &mut document.steps.get_mut("build").unwrap().body else {
            panic!("build decodes as a workflow step");
        };
        node.for_each.as_mut().unwrap().items = items;
        let failure =
            super::validate_with_children(document, None, &child_summaries()).unwrap_err();
        assert_eq!(
            (failure.kind(), failure.location()),
            (
                ValidationFailureKind::ForEachReferenceInvalid,
                &step_for_each("build")
            )
        );
    }

    assert_edge_failure(
        &fan_out_workflow("", "item.name", "inputs.policy"),
        &child_summaries(),
        ValidationFailureKind::ItemReferenceRequiresForEach,
        step_input("build", "part"),
    );
}

// REQ-WFFORMAT-93
#[test]
fn a_workflow_with_workflow_steps_is_scheduled_by_its_edges() {
    let workflow = validate_with_children(EDGE_WORKFLOW, &child_summaries()).unwrap();
    assert_eq!(
        workflow.scheduling,
        Scheduling::Edges {
            entry: "design".to_owned()
        }
    );
    assert_eq!(workflow.presentation_order, ["design", "build"]);
    assert_eq!(workflow.outcome, None);
    let dataflow = validate_yaml(&format!(
        "schemaVersion: 1\nsteps:\n{}",
        command_step("only", "")
    ))
    .unwrap();
    assert_eq!(dataflow.scheduling, Scheduling::Dataflow);

    let with_command = format!("{EDGE_WORKFLOW}{}", command_step("tidy", ""));
    assert_edge_failure(
        &with_command,
        &child_summaries(),
        ValidationFailureKind::EdgeSchedulingKind,
        ValidationLocation::Step {
            step: "tidy".to_owned(),
        },
    );
    assert_edge_failure(
        &edge_workflow("planned: build", "planned: exit.landed"),
        &child_summaries(),
        ValidationFailureKind::StepUnreachable,
        ValidationLocation::Step {
            step: "build".to_owned(),
        },
    );
    // A step reached only through a wait's `then` is reachable.
    validate_with_children(
        &edge_workflow(
            "planned: build",
            "planned:\n        wait: repair\n        then: build",
        ),
        &child_summaries(),
    )
    .unwrap();
    assert_edge_failure(
        &edge_workflow(
            "outcomes: [landed, failed]\n",
            "outcomes: [landed, failed]\noutcome: {from: outputs.build.outcome, onFailure: failed}\n",
        ),
        &child_summaries(),
        ValidationFailureKind::InvalidOutcomeDeclaration,
        ValidationLocation::Outcome,
    );
}

// REQ-WFFORMAT-89
#[test]
fn a_child_workflow_is_a_readable_stage_without_workflow_steps_or_root_blocks() {
    let design = || ValidationLocation::StepWorkflow {
        step: "design".to_owned(),
    };
    let mut nested = child_summaries();
    nested.get_mut("design.yaml").unwrap().has_workflow_steps = true;
    assert_edge_failure(
        EDGE_WORKFLOW,
        &nested,
        ValidationFailureKind::ChildWorkflowNested,
        design(),
    );
    let mut rooted = child_summaries();
    rooted.get_mut("design.yaml").unwrap().has_root_blocks = true;
    assert_edge_failure(
        EDGE_WORKFLOW,
        &rooted,
        ValidationFailureKind::ChildWorkflowRootBlocks,
        design(),
    );
    let mut missing = child_summaries();
    missing.remove("design.yaml");
    assert_edge_failure(
        EDGE_WORKFLOW,
        &missing,
        ValidationFailureKind::ChildWorkflowUnavailable,
        design(),
    );
    assert_failure(
        EDGE_WORKFLOW,
        ValidationFailureKind::ChildWorkflowUnavailable,
        design(),
    );
}

#[test]
fn structurally_decoded_references_are_not_reparsed_during_validation() {
    let mut document =
        decode(b"schemaVersion: 1\nsteps: {command: {kind: cmd, command: {argv: [\"true\"]}}}\n")
            .unwrap();
    let NodeBody::Command(command) = &mut document.steps.get_mut("command").unwrap().body else {
        panic!("command must be a command step");
    };
    document.inputs.insert(
        "request".to_owned(),
        super::super::document::InputDeclaration::Text {
            optional: false,
            default: None,
        },
    );
    command.inputs.insert(
        "prompt".to_owned(),
        ValueReference::Input {
            name: "request".to_owned(),
        },
    );

    let validated = super::validate(document).unwrap();
    let ValidatedStep::Command(command) = &validated.steps["command"] else {
        panic!("command must be a command step");
    };
    assert_eq!(
        command.inputs["prompt"].source,
        ResolvedValueSource::Input("request".to_owned())
    );
}

// REQ-WFFORMAT-9, REQ-WFFORMAT-10
#[test]
fn combined_node_limit_and_shared_namespace_are_enforced_before_validation() {
    fn workflow_with_counts(step_count: usize, finalizer_count: usize) -> String {
        let mut source = String::from("schemaVersion: 1\nsteps:\n");
        for index in 0..step_count {
            source.push_str(&command_step(&format!("s{index}"), ""));
        }
        source.push_str("finalizers:\n");
        for index in 0..finalizer_count {
            source.push_str(&format!(
                "  f{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
            ));
        }
        source
    }

    let accepted = validate_yaml(&workflow_with_counts(128, 128)).unwrap();
    assert_eq!(accepted.steps.len() + accepted.finalizers.len(), 256);

    let too_many = validate_yaml(&workflow_with_counts(128, 129)).unwrap_err();
    assert_eq!(too_many.kind(), ValidationFailureKind::TooManyNodes);
    assert_eq!(too_many.location(), &ValidationLocation::WorkflowNamespace);

    assert_failure(
        "schemaVersion: 1
steps:
  shared:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  shared:
    kind: cmd
    command: { argv: [\"true\"] }
",
        ValidationFailureKind::DuplicateNodeId,
        ValidationLocation::WorkflowNamespace,
    );
}

// REQ-WFFINALIZE-17, REQ-WFEXEC-49
#[test]
fn semantic_outputs_ordinary_finalizer_parity() {
    let source = "schemaVersion: 1
steps:
  later:
    kind: cmd
    dependsOn: [producer]
    command: { argv: [\"true\"] }
  producer:
    kind: cmd
    command: { argv: [\"true\"] }
    outputs:
      value:
        kind: file
        from: path
        path: value.json
        mediaType: application/json
finalizers:
  report:
    kind: cmd
    after: [cleanup]
    when: [succeeded]
    inputs:
      result:
        ref: outputs.cleanup.result
      context:
        ref: finalization.context
    command: { argv: [\"true\"] }
  cleanup:
    kind: cmd
    inputs:
      value:
        ref: outputs.producer.value
    command: { argv: [\"true\"] }
    outputs:
      result:
        kind: file
        from: path
        path: result.json
        mediaType: application/json
exports:
  report:
    ref: outputs.report.result
";
    let source = source.replace(
        "    command: { argv: [\"true\"] }\n  cleanup:",
        "    command: { argv: [\"true\"] }\n    outputs:\n      result:\n        kind: file\n        from: path\n        path: report.json\n        mediaType: application/json\n  cleanup:",
    );
    let workflow = validate_yaml(&source).unwrap();

    assert_eq!(workflow.source_order, ["later", "producer"]);
    assert_eq!(workflow.presentation_order, ["producer", "later"]);
    assert_eq!(workflow.finalizer_source_order, ["report", "cleanup"]);
    assert_eq!(workflow.finalizer_presentation_order, ["cleanup", "report"]);
    let report = &workflow.finalizers["report"];
    assert_eq!(
        report.when,
        [FinalizationTrigger::Succeeded].into_iter().collect()
    );
    let ValidatedStep::Command(report) = &report.body else {
        panic!("report must be a command finalizer");
    };
    assert_eq!(
        prerequisite_shape(&report.common.prerequisites),
        [("cleanup", true, true)]
    );
    assert_eq!(
        report.inputs["context"],
        super::super::validated::ResolvedValueReference {
            source: ResolvedValueSource::FinalizationContext,
            value_type: WorkflowValueType::Json,
        }
    );
    let ValidatedStep::Command(cleanup) = &workflow.finalizers["cleanup"].body else {
        panic!("cleanup must be a command finalizer");
    };
    assert!(cleanup.common.prerequisites.is_empty());
    assert_eq!(
        workflow.exports["report"].node.role,
        WorkflowNodeRole::Finalizer
    );
    assert_eq!(workflow.exports["report"].node.id, "report");
}

// REQ-WFFORMAT-62
#[test]
fn finalizer_graph_rejects_cross_phase_edges_self_edges_duplicates_and_cycles() {
    let after_step = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  cleanup:
    kind: cmd
    after: [work]
    command: { argv: [\"true\"] }
";
    assert_failure(
        after_step,
        ValidationFailureKind::InvalidFinalizerAfterTarget,
        ValidationLocation::FinalizerAfter {
            finalizer: "cleanup".to_owned(),
            index: 0,
        },
    );

    let self_edge = after_step.replace("after: [work]", "after: [cleanup]");
    assert_failure(
        &self_edge,
        ValidationFailureKind::SelfDependency,
        ValidationLocation::FinalizerAfter {
            finalizer: "cleanup".to_owned(),
            index: 0,
        },
    );

    let control_cycle = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  one:
    kind: cmd
    after: [two]
    command: { argv: [\"true\"] }
  two:
    kind: cmd
    after: [one]
    command: { argv: [\"true\"] }
";
    assert_failure(
        control_cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );

    let data_cycle = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  one:
    kind: cmd
    inputs: { value: { ref: outputs.two.value } }
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: one.txt, mediaType: text/plain }
  two:
    kind: cmd
    inputs: { value: { ref: outputs.one.value } }
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: two.txt, mediaType: text/plain }
";
    assert_failure(
        data_cycle,
        ValidationFailureKind::DependencyCycle,
        ValidationLocation::WorkflowGraph,
    );
}

// REQ-WFFORMAT-64, REQ-WFFINALIZE-7
#[test]
fn output_references_obey_phase_trigger_and_failure_impact_rules() {
    let ordinary_reads_finalizer = "schemaVersion: 1
steps:
  work:
    kind: cmd
    inputs: { value: { ref: outputs.cleanup.value } }
    command: { argv: [\"true\"] }
finalizers:
  cleanup:
    kind: cmd
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: value.txt, mediaType: text/plain }
";
    assert_failure(
        ordinary_reads_finalizer,
        ValidationFailureKind::CrossPhaseOutputReference,
        ValidationLocation::StepInput {
            step: "work".to_owned(),
            input: "value".to_owned(),
        },
    );

    let incompatible_trigger = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  produce:
    kind: cmd
    when: [succeeded]
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: value.txt, mediaType: text/plain }
  consume:
    kind: cmd
    inputs: { value: { ref: outputs.produce.value } }
    command: { argv: [\"true\"] }
";
    assert_failure(
        incompatible_trigger,
        ValidationFailureKind::IncompatibleFinalizerTriggers,
        ValidationLocation::FinalizerInput {
            finalizer: "consume".to_owned(),
            input: "value".to_owned(),
        },
    );

    let advisory_ordinary = "schemaVersion: 1
steps:
  produce:
    kind: cmd
    failurePolicy: advisory
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: value.txt, mediaType: text/plain }
finalizers:
  consume:
    kind: cmd
    inputs: { value: { ref: outputs.produce.value } }
    command: { argv: [\"true\"] }
";
    assert_failure(
        advisory_ordinary,
        ValidationFailureKind::AdvisoryDataDependency,
        ValidationLocation::FinalizerInput {
            finalizer: "consume".to_owned(),
            input: "value".to_owned(),
        },
    );

    let advisory_finalizer = incompatible_trigger
        .replace("    when: [succeeded]\n", "    failurePolicy: advisory\n")
        .replace(
            "  consume:\n    kind: cmd\n",
            "  consume:\n    kind: cmd\n    when: [succeeded]\n",
        );
    assert_failure(
        &advisory_finalizer,
        ValidationFailureKind::AdvisoryDataDependency,
        ValidationLocation::FinalizerInput {
            finalizer: "consume".to_owned(),
            input: "value".to_owned(),
        },
    );

    let control_only = advisory_finalizer.replace(
        "    inputs: { value: { ref: outputs.produce.value } }\n",
        "    after: [produce]\n",
    );
    validate_yaml(&control_only).unwrap();
}

// REQ-WFFORMAT-66
#[test]
fn finalizer_exports_require_required_success_eligibility() {
    let base = "schemaVersion: 1
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  report:
    kind: cmd
    POLICY
    when: [TRIGGER]
    command: { argv: [\"true\"] }
    outputs:
      value: { kind: file, from: path, path: value.txt, mediaType: text/plain }
exports:
  value: { ref: outputs.report.value }
";
    assert_failure(
        &base
            .replace("POLICY", "failurePolicy: advisory")
            .replace("TRIGGER", "succeeded"),
        ValidationFailureKind::AdvisoryExportTarget,
        ValidationLocation::Export {
            name: "value".to_owned(),
        },
    );
    assert_failure(
        &base
            .replace("POLICY", "failurePolicy: required")
            .replace("TRIGGER", "failed"),
        ValidationFailureKind::FinalizerExportTrigger,
        ValidationLocation::Export {
            name: "value".to_owned(),
        },
    );
    let accepted = base
        .replace("POLICY", "failurePolicy: required")
        .replace("TRIGGER", "succeeded");
    assert_eq!(
        validate_yaml(&accepted).unwrap().exports["value"].node.role,
        WorkflowNodeRole::Finalizer
    );
}

// REQ-WFFORMAT-63
#[test]
fn finalization_context_is_json_in_every_declared_finalizer_position() {
    let source = "schemaVersion: 1
agentProfiles:
  reporting:
    harness:
      kind: pi
      config: { model: openai/gpt-5, thinking: high }
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
finalizers:
  command:
    kind: cmd
    inputs: { context: { ref: finalization.context } }
    command: { argv: [\"true\"] }
  agent:
    kind: agent
    agent:
      profile: reporting
      systemPrompt: system.md
      message:
        text: [{ file: message.md }]
        attachments: [{ ref: finalization.context }]
  gated:
    kind: cmd
    condition: { exists: { ref: finalization.context, pointer: /trigger } }
    command: { argv: [\"true\"] }
";
    let workflow = validate_yaml(source).unwrap();
    let ValidatedStep::Command(command) = &workflow.finalizers["command"].body else {
        panic!("command must remain a command finalizer");
    };
    assert_eq!(
        command.inputs["context"].value_type,
        WorkflowValueType::Json
    );
    assert_eq!(
        command.inputs["context"].source,
        ResolvedValueSource::FinalizationContext
    );
    let ValidatedStep::Agent(agent) = &workflow.finalizers["agent"].body else {
        panic!("agent must remain an agent finalizer");
    };
    assert_eq!(
        agent.agent.message.attachments[0],
        ValidatedMessageSource::Reference {
            source: ResolvedValueSource::FinalizationContext,
            value_type: WorkflowValueType::Json,
        }
    );

    let ValidatedStep::Command(gated) = &workflow.finalizers["gated"].body else {
        panic!("gated must remain a command finalizer");
    };
    assert!(matches!(
        gated.common.condition,
        Some(ResolvedPredicate::Exists(_))
    ));
}

// REQ-WFCOND-15
#[test]
fn workflow_node_evidence_definition_limit_applies_to_steps_and_finalizers() {
    for finalizer in [false, true] {
        for count in [1_024, 1_025] {
            let mut outputs = String::new();
            let mut inputs = String::new();
            for index in 0..count {
                let name = format!("value{index:04}");
                outputs.push_str(&format!(
                    "      {name}:\n        kind: text\n        from: path\n        path: {name}.txt\n"
                ));
                inputs.push_str(&format!(
                    "      {name}:\n        ref: outputs.producer.{name}\n"
                ));
            }
            let consumer = format!(
                "    kind: cmd\n    inputs:\n{inputs}    command:\n      argv: [\"true\"]\n"
            );
            let source = if finalizer {
                format!(
                    "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n{outputs}finalizers:\n  consume:\n    when: [succeeded]\n{consumer}"
                )
            } else {
                format!(
                    "schemaVersion: 1\nsteps:\n  producer:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n{outputs}  consume:\n{consumer}"
                )
            };
            let resolved = validate_yaml(&source);
            if count == 1_024 {
                let workflow = resolved.unwrap();
                let step = if finalizer {
                    &workflow.finalizers["consume"].body
                } else {
                    &workflow.steps["consume"]
                };
                let common = step.common();
                assert_eq!(common.evidence_prerequisites.len(), 1_024);
                assert!(common.evidence_prerequisites.iter().all(|descriptor| {
                    descriptor.kind() == super::super::evidence::PrerequisiteKind::Body
                }));
            } else {
                assert_eq!(
                    resolved.unwrap_err().kind(),
                    ValidationFailureKind::TooManyPrerequisites
                );
            }
        }
    }
}

fn replacement_profiles(source: &str) -> BTreeMap<String, super::super::document::AgentProfile> {
    super::super::profile_replacement::decode(source.as_bytes())
        .unwrap()
        .profiles
}

const REPLACEABLE_SOURCE: &str = "schemaVersion: 1
agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: xhigh
  review:
    harness:
      kind: pi
      config:
        model: openai/gpt-4.1
        thinking: low
steps:
  implement:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
  verify:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: coding
        prompt: prompts/repair.md
    command: {argv: [\"true\"]}
finalizers:
  summarize:
    kind: agent
    agent:
      profile: review
      systemPrompt: prompts/system.md
      message:
        text:
          - file: prompts/message.md
";

// REQ-WFFORMAT-52, REQ-AGENT-52, REQ-AGENT-53
#[test]
fn agent_profile_replacement_pins_every_reference_as_a_whole_without_translation() {
    let replacement = replacement_profiles(
        "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: claude_code\n      config:\n        model: example/claude\n        effort: low\n",
    );
    let workflow = super::validate_with_replacement(
        decode(REPLACEABLE_SOURCE.as_bytes()).unwrap(),
        Some(&replacement),
    )
    .unwrap();
    let expected = ValidatedHarness::ClaudeCode(ClaudeCodeConfig {
        model: "example/claude".to_owned(),
        effort: ClaudeCodeEffort::Low,
    });
    let ValidatedStep::Agent(step) = &workflow.steps["implement"] else {
        panic!("implement must be an agent step");
    };
    assert_eq!(step.agent.harness, expected);
    let Some(super::super::validated::ValidatedRecoveryHandler::Agent { harness, .. }) =
        &workflow.recoveries["verify"].as_ref().unwrap().handler
    else {
        panic!("verify must have an agent recovery handler");
    };
    assert_eq!(harness, &expected);
    let ValidatedStep::Agent(finalizer) = &workflow.finalizers["summarize"].body else {
        panic!("summarize must be an agent finalizer");
    };
    assert_eq!(
        finalizer.agent.harness,
        ValidatedHarness::Pi(PiConfig {
            model: "openai/gpt-4.1".to_owned(),
            thinking: Thinking::Low,
        })
    );
    assert_eq!(
        workflow.replaced_agent_profiles,
        BTreeMap::from([("coding".to_owned(), expected)])
    );

    let unreplaced = super::validate(decode(REPLACEABLE_SOURCE.as_bytes()).unwrap()).unwrap();
    assert!(unreplaced.replaced_agent_profiles.is_empty());

    // Replacing an unused declared profile is valid and has no runtime effect.
    let unused_only = super::validate_with_replacement(
        decode(REPLACEABLE_SOURCE.as_bytes()).unwrap(),
        Some(&replacement_profiles(
            "schemaVersion: 1\nagentProfiles:\n  review:\n    harness:\n      kind: codex\n      config:\n        model: example/codex\n        effort: medium\n",
        )),
    )
    .unwrap();
    assert_eq!(unused_only.steps, unreplaced.steps);
    assert_eq!(unused_only.replaced_agent_profiles.len(), 1);
}

// REQ-AGENT-54
#[test]
fn agent_profile_replacement_rejects_an_undeclared_name_at_the_replacement() {
    let failure = super::validate_with_replacement(
        decode(REPLACEABLE_SOURCE.as_bytes()).unwrap(),
        Some(&replacement_profiles(
            "schemaVersion: 1\nagentProfiles:\n  missing:\n    harness:\n      kind: pi\n      config:\n        model: openai/gpt-5\n        thinking: low\n",
        )),
    )
    .unwrap_err();
    assert_eq!(
        failure.kind(),
        ValidationFailureKind::UnknownReplacementProfile
    );
    assert_eq!(
        failure.location(),
        &ValidationLocation::AgentProfileReplacement {
            profile: "missing".to_owned(),
        }
    );
}

// REQ-WFFORMAT-7
#[test]
fn description_name_and_version_are_inert_and_a_workflow_declares_no_identifier() {
    let source = format!(
        "schemaVersion: 1\nsteps:\n{}{}",
        command_step("prepare", ""),
        command_step("publish", "    dependsOn: [prepare]\n")
    );
    let described = source.replacen(
        "schemaVersion: 1\n",
        "schemaVersion: 1\ndescription: \"Release train: run publish when prepare succeeds\"\n\
         name: release\nversion: \"2.1\"\n",
        1,
    );

    let plain = validate_yaml(&source).unwrap();
    let mut described = validate_yaml(&described).unwrap();
    assert!(described.description.is_some());
    assert_eq!(described.name.as_deref(), Some("release"));
    assert_eq!(described.version.as_deref(), Some("2.1"));
    described.description = None;
    described.name = None;
    described.version = None;
    assert_eq!(described, plain);

    for identifier in ["id: release", "workflowId: release"] {
        assert_structurally_invalid(&source.replacen(
            "schemaVersion: 1\n",
            &format!("schemaVersion: 1\n{identifier}\n"),
            1,
        ));
    }
}

// REQ-WFFORMAT-54
#[test]
fn output_kinds_require_their_source_schema_and_media_type_fields() {
    let output = |fields: &str| {
        format!(
            "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      value:\n{fields}"
        )
    };
    for complete in [
        "        kind: text\n        from: path\n        path: value.txt\n",
        "        kind: json\n        from: path\n        path: value.json\n        schema: value.schema.json\n",
        "        kind: file\n        from: path\n        path: value.bin\n        mediaType: application/octet-stream\n",
    ] {
        validate_yaml(&output(complete))
            .unwrap_or_else(|failure| panic!("rejected {complete}: {failure:?}"));
    }
    for incomplete in [
        "        kind: text\n        from: path\n",
        "        kind: json\n        from: path\n        path: value.json\n",
        "        kind: file\n        from: path\n        path: value.bin\n",
        &format!(
            "        kind: file\n        from: path\n        path: value.bin\n        mediaType: application/{}\n",
            "x".repeat(117)
        ),
    ] {
        assert_structurally_invalid(&output(incomplete));
    }
}

fn passthrough_workflow(root: &str, step: &str, handler: &str, finalizer: &str) -> String {
    format!(
        "schemaVersion: 1
{root}steps:
  check:
    kind: cmd
{step}    recovery:
      retries: 1
      handler:
        kind: cmd
{handler}        command:
          argv: [\"true\"]
    command:
      argv: [\"true\"]
finalizers:
  report:
    kind: cmd
{finalizer}    command:
      argv: [\"true\"]
"
    )
}

// REQ-WFFORMAT-102
#[test]
fn node_environment_passthrough_names_must_be_declared_at_the_root() {
    let root = "environmentPassthrough: [VISIBLE]\n";
    let node = "    environmentPassthrough: [VISIBLE]\n";
    let handler = "        environmentPassthrough: [VISIBLE]\n";
    let undeclared_node = "    environmentPassthrough: [VISIBLE, OTHER]\n";
    let undeclared_handler = "        environmentPassthrough: [OTHER]\n";
    validate_yaml(&passthrough_workflow(root, node, handler, node)).unwrap();

    assert_failure(
        &passthrough_workflow(root, undeclared_node, handler, node),
        ValidationFailureKind::EnvironmentPassthroughUndeclared,
        ValidationLocation::EnvironmentPassthrough {
            step: "check".to_owned(),
        },
    );
    assert_failure(
        &passthrough_workflow(root, node, handler, undeclared_node),
        ValidationFailureKind::EnvironmentPassthroughUndeclared,
        ValidationLocation::FinalizerEnvironmentPassthrough {
            finalizer: "report".to_owned(),
        },
    );
    assert_failure(
        &passthrough_workflow(root, node, undeclared_handler, node),
        ValidationFailureKind::EnvironmentPassthroughUndeclared,
        ValidationLocation::RecoveryEnvironmentPassthrough {
            step: "check".to_owned(),
        },
    );

    let empty = "    environmentPassthrough: []\n";
    validate_yaml(&passthrough_workflow("", empty, "", "")).unwrap();
    assert_failure(
        &passthrough_workflow("", node, "", ""),
        ValidationFailureKind::EnvironmentPassthroughUndeclared,
        ValidationLocation::EnvironmentPassthrough {
            step: "check".to_owned(),
        },
    );
}
