use std::fs;
use std::path::{Path, PathBuf};

use super::claude_code::{ClaudeCodeConfig, ClaudeCodeEffort};
use super::codex::CodexConfig;
use super::document::{
    FailurePolicy, FinalizationTrigger, MessageSource, NodeBody, Output, OutputReference,
    RecoveryHandler, ValueReference,
};
use super::pi::{PiConfig, Thinking};
use super::validated::ValidatedHarness;
use super::*;

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/workflow/v1")
}

fn canonical_valid_fixture() -> Vec<u8> {
    fs::read(fixture_root().join("valid/plan-implement-test.yaml")).unwrap()
}

// REQ-WFFORMAT-65, REQ-EXPORTS-6, REQ-EXPORTS-7, REQ-EXPORTS-9
#[test]
fn export_presentation_sources_validate_without_creating_dependencies() {
    // Source admission stays behind the canonical schema until the inert
    // resource checkpoint has qualified the proposed portable envelope.
    let source = serde_json::json!({
        "schemaVersion": 1,
        "steps": {
            "implement": {
                "kind": "cmd", "command": { "argv": ["true"] },
                "outputs": {
                    "branch": { "kind": "git_branch", "from": "workspace" },
                    "summary": { "kind": "text", "from": "path", "path": "summary.txt" },
                    "structured": { "kind": "json", "from": "path", "path": "summary.json", "schema": "schema.json" }
                }
            }
        },
        "finalizers": {
            "writeup": {
                "kind": "cmd", "command": { "argv": ["true"] },
                "when": ["succeeded"], "failurePolicy": "advisory",
                "outputs": { "title": { "kind": "text", "from": "path", "path": "title.txt" } }
            }
        },
        "exports": {
            "changes": {
                "ref": "outputs.implement.branch",
                "presentation": {
                    "title": { "ref": "outputs.writeup.title" },
                    "description": { "value": "  Result summary  " }
                }
            },
            "alias": {
                "ref": "outputs.implement.branch",
                "presentation": { "title": { "ref": "outputs.implement.summary" } }
            }
        }
    });
    let validate_source = |source: serde_json::Value| {
        let dto: schema::WorkflowDto = serde_json::from_value(source).unwrap();
        let definition = dto
            .into_document(vec!["implement".to_owned()], vec!["writeup".to_owned()])
            .unwrap();
        validation::validate(definition)
    };
    let validated = validate_source(source.clone()).unwrap();
    let validated::ValidatedStep::Command(implement) = &validated.steps["implement"] else {
        panic!("implementation is not a command");
    };
    assert!(implement.common.prerequisites.is_empty());
    assert_eq!(
        validated.export_presentation["changes"].description,
        Some(validated::ValidatedPresentationField::Literal(
            "Result summary".to_owned()
        ))
    );
    assert!(matches!(
        validated.export_presentation["changes"].title,
        Some(validated::ValidatedPresentationField::Reference(_))
    ));
    let capacity = capacity::resolve_workflow_capacity(
        &validated,
        resolution::WorkflowContentDigest {
            algorithm: resolution::ContentDigestAlgorithm::Sha256,
            value: "1".repeat(64),
        },
        0,
    )
    .unwrap();
    assert_eq!(capacity.requirements.presentation_result_bytes, 12_500);
    assert_eq!(capacity.requirements.portable_result_bytes, 202_040_192);
    let mut invalid = source.clone();
    invalid["finalizers"]["writeup"]["when"] = serde_json::json!(["failed"]);
    assert_eq!(
        validate_source(invalid).unwrap_err().kind(),
        validation::ValidationFailureKind::InvalidExportPresentationTarget
    );
    let mut invalid = source.clone();
    invalid["exports"]["changes"]["presentation"]["title"] =
        serde_json::json!({"ref":"outputs.implement.structured"});
    assert_eq!(
        validate_source(invalid).unwrap_err().kind(),
        validation::ValidationFailureKind::InvalidExportPresentationTarget
    );
    let mut invalid = source;
    invalid["exports"]["changes"]["presentation"]["description"] =
        serde_json::json!({"value":"  "});
    assert_eq!(
        validate_source(invalid).unwrap_err().kind(),
        validation::ValidationFailureKind::InvalidExportPresentation
    );
}

// REQ-WFFORMAT-54
#[test]
fn media_type_parameters_use_horizontal_separators_and_control_free_unicode_values() {
    for valid in [
        "application/octet-stream;version=1",
        "application/octet-stream ; version=1",
        "application/octet-stream\t;\tversion=1",
        "application/octet-stream ;\tversion=雪",
    ] {
        assert!(is_valid_media_type(valid), "valid media type: {valid:?}");
    }

    for control in (0..=0x1f).chain([0x7f]) {
        let control = char::from_u32(control).unwrap();
        let invalid = format!("application/octet-stream;version=one{control}two");
        assert!(
            !is_valid_media_type(&invalid),
            "media type with U+{:04X} in a parameter value accepted",
            u32::from(control)
        );
    }

    for invalid in [
        "application/octet-stream\n;version=1",
        "application/octet-stream\r;version=1",
        "application/octet-stream\u{000b};version=1",
        "application/octet-stream\u{000c};version=1",
    ] {
        assert!(
            !is_valid_media_type(invalid),
            "media type with a non-horizontal separator accepted: {invalid:?}"
        );
    }
}

// REQ-WFFORMAT-32
#[test]
fn environment_passthrough_rejects_reserved_invalid_and_duplicate_names() {
    for names in [
        "[UM_INTERNAL_CHILD_GUARD_WORKER]",
        "[GH_TOKEN]",
        "[GIT_CONFIG_KEY_0]",
        "[SSH_AUTH_SOCK]",
        "[BAD-NAME]",
        "[VISIBLE, VISIBLE]",
    ] {
        let source = format!(
            "schemaVersion: 1\nenvironmentPassthrough: {names}\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [/bin/true]\n"
        );
        assert_eq!(
            decode(source.as_bytes()).unwrap_err().kind(),
            DecodeFailureKind::StructuralContract,
            "accepted {names}"
        );
    }
    let source = b"schemaVersion: 1\nenvironmentPassthrough: [VISIBLE]\nsteps:\n  check:\n    kind: cmd\n    command:\n      argv: [/bin/true]\n";
    assert!(
        decode(source)
            .unwrap()
            .environment_passthrough
            .contains("VISIBLE")
    );
}

// REQ-WFFORMAT-1, REQ-WFFORMAT-34, REQ-WFFORMAT-44
#[test]
fn canonical_workflow_decodes_into_the_complete_execution_document() {
    let workflow = decode(&canonical_valid_fixture()).unwrap();

    assert_eq!(workflow.schema_version, 1);
    assert_eq!(
        workflow.description.as_deref(),
        Some("Plan, implement, test, and export a requested change.")
    );
    assert_eq!(workflow.agent_profiles.len(), 1);
    assert_eq!(workflow.steps.len(), 4);
    assert_eq!(
        workflow.step_order,
        ["prepare", "plan", "implement", "test"]
    );
    assert!(workflow.finalizers.is_empty());
    assert!(workflow.finalizer_order.is_empty());
    assert_eq!(workflow.exports.len(), 2);
    assert_eq!(
        workflow.agent_profiles["coding"].harness,
        ValidatedHarness::Pi(PiConfig {
            model: "openai/gpt-5".to_owned(),
            thinking: Thinking::High,
        })
    );

    let prepare = &workflow.steps["prepare"];
    let NodeBody::Command(prepare_body) = &prepare.body else {
        panic!("prepare must be a command step");
    };
    assert_eq!(prepare_body.argv, ["./scripts/prepare-workspace.sh"]);
    assert!(prepare.control_dependencies.is_empty());
    assert!(prepare_body.inputs.is_empty());
    assert!(prepare_body.common.outputs.is_empty());

    let plan = &workflow.steps["plan"];
    let NodeBody::Agent(plan_body) = &plan.body else {
        panic!("plan must be an agent step");
    };
    assert_eq!(plan.control_dependencies, ["prepare"]);
    assert_eq!(plan_body.agent.profile, "coding");
    assert_eq!(plan_body.agent.system_prompt, "prompts/plan-system.md");
    assert_eq!(
        plan_body.agent.message.text,
        [MessageSource::Reference(ValueReference::Input {
            name: "request".to_owned()
        })]
    );
    assert_eq!(
        plan_body.agent.message.attachments,
        [MessageSource::Reference(ValueReference::Input {
            name: "evidence".to_owned()
        })]
    );
    assert_eq!(
        plan_body.common.outputs["plan"],
        Output::JsonAgentResult {
            schema: "schemas/change-plan.schema.json".to_owned()
        }
    );
    assert_eq!(
        plan_body.common.outputs["artifact"],
        Output::FilePath {
            path: "artifacts/plan.txt".to_owned(),
            media_type: "text/plain".to_owned(),
        }
    );

    let implement = &workflow.steps["implement"];
    let NodeBody::Agent(implement_body) = &implement.body else {
        panic!("implement must be an agent step");
    };
    assert!(implement.control_dependencies.is_empty());
    assert_eq!(implement_body.agent.profile, "coding");
    assert_eq!(
        implement_body.agent.system_prompt,
        "prompts/implement-system.md"
    );
    assert_eq!(
        implement_body.agent.message.text,
        [
            MessageSource::File {
                path: "prompts/implement-message.md".to_owned()
            },
            MessageSource::Reference(ValueReference::Input {
                name: "request".to_owned()
            })
        ]
    );
    assert_eq!(
        implement_body.agent.message.attachments,
        [
            MessageSource::Reference(ValueReference::Input {
                name: "evidence".to_owned()
            }),
            MessageSource::Reference(ValueReference::Output(OutputReference {
                node: "plan".to_owned(),
                output: "plan".to_owned(),
            })),
            MessageSource::Reference(ValueReference::Output(OutputReference {
                node: "plan".to_owned(),
                output: "artifact".to_owned(),
            }))
        ]
    );
    assert_eq!(
        implement_body.common.outputs["response"],
        Output::TextAgentResponse
    );

    let test = &workflow.steps["test"];
    let NodeBody::Command(test_body) = &test.body else {
        panic!("test must be a command step");
    };
    assert!(test.control_dependencies.is_empty());
    assert_eq!(test_body.common.cwd.as_deref(), Some("packages/api"));
    assert_eq!(test_body.argv, ["./scripts/test.sh"]);
    assert_eq!(
        test_body.inputs["prompt"],
        ValueReference::Input {
            name: "request".to_owned(),
        }
    );
    assert_eq!(
        test_body.inputs["changeSummary"],
        ValueReference::Output(OutputReference {
            node: "implement".to_owned(),
            output: "response".to_owned(),
        })
    );
    assert_eq!(
        test_body.common.outputs["report"],
        Output::FilePath {
            path: "packages/api/artifacts/test-report.xml".to_owned(),
            media_type: "application/junit+xml".to_owned(),
        }
    );

    assert_eq!(
        workflow.exports["response"].source,
        OutputReference {
            node: "implement".to_owned(),
            output: "response".to_owned(),
        }
    );
    assert_eq!(
        workflow.exports["testReport"].source,
        OutputReference {
            node: "test".to_owned(),
            output: "report".to_owned(),
        }
    );
}

// REQ-WFFORMAT-41
#[test]
fn workflow_v1_decodes_the_three_closed_recovery_forms_only_on_ordinary_steps() {
    let fixture = fs::read(fixture_root().join("valid/recovery-forms.yaml")).unwrap();
    let workflow = decode(&fixture).unwrap();

    let handlerless = workflow.steps["handlerless"].recovery.as_ref().unwrap();
    assert_eq!(handlerless.retries, 1);
    assert_eq!(handlerless.handler, None);
    let command = workflow.steps["commandRepair"].recovery.as_ref().unwrap();
    assert_eq!(command.retries, 2);
    assert_eq!(
        command.handler,
        Some(RecoveryHandler::Command {
            argv: vec!["./repair".to_owned(), "--generated".to_owned()],
            cwd: Some("repair".to_owned()),
        })
    );
    let agent = workflow.steps["agentRepair"].recovery.as_ref().unwrap();
    assert_eq!(agent.retries, 10);
    assert_eq!(
        agent.handler,
        Some(RecoveryHandler::Agent {
            profile: "repair".to_owned(),
            prompt: "prompts/recovery.md".to_owned(),
            cwd: Some("repair".to_owned()),
        })
    );
    assert!(workflow.finalizers.is_empty());
}

// REQ-WFFORMAT-46
#[test]
fn workflow_v1_decodes_all_three_closed_agent_profiles() {
    let fixture = fs::read(fixture_root().join("valid/mixed-agent-harnesses.yaml")).unwrap();
    let workflow = decode(&fixture).unwrap();

    assert_eq!(workflow.agent_profiles.len(), 3);
    assert_eq!(
        workflow.agent_profiles["claudeCoding"].harness,
        ValidatedHarness::ClaudeCode(ClaudeCodeConfig {
            model: "claude-opus-4-1".to_owned(),
            effort: ClaudeCodeEffort::XHigh,
        })
    );
    assert_eq!(
        workflow.agent_profiles["piCoding"].harness,
        ValidatedHarness::Pi(PiConfig {
            model: "openai/gpt-5".to_owned(),
            thinking: Thinking::High,
        })
    );
    assert_eq!(
        workflow.agent_profiles["codexCoding"].harness,
        ValidatedHarness::Codex(CodexConfig {
            model: "gpt-5.4".to_owned(),
            effort: "xhigh".to_owned(),
        })
    );
}

// REQ-WFFORMAT-48
#[test]
fn workflow_v1_codex_shape_is_exact() {
    for config in [
        "effort: high",
        "model: gpt-5.4",
        "model: \"\"\n        effort: high",
        "model: gpt-5.4\n        effort: \"\"",
        "model: gpt-5.4\n        effort: high\n        modelProvider: openai",
        "model: gpt-5.4\n        effort: high\n        apiKey: forbidden",
    ] {
        let source = format!(
            "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: codex\n      config:\n        {config}\nsteps:\n  command:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
        );

        assert_eq!(
            decode(source.as_bytes()).unwrap_err().kind(),
            DecodeFailureKind::StructuralContract
        );
    }
}

// REQ-WFFORMAT-61
#[test]
fn finalizers_decode_with_independent_order_defaults_and_engine_context() {
    let source = br#"schemaVersion: 1
agentProfiles:
  reporting:
    harness:
      kind: pi
      config:
        model: openai/gpt-5
        thinking: high
steps:
  zStep:
    kind: cmd
    command:
      argv: ["true"]
  aStep:
    kind: cmd
    command:
      argv: ["true"]
finalizers:
  zCleanup:
    kind: cmd
    inputs:
      context:
        ref: finalization.context
    command:
      argv: ["true"]
  aReport:
    kind: agent
    failurePolicy: advisory
    after: [zCleanup]
    when: [failed]
    agent:
      profile: reporting
      systemPrompt: system.md
      message:
        text:
          - file: message.md
        attachments:
          - ref: finalization.context
"#;

    let workflow = decode(source).unwrap();
    assert_eq!(workflow.step_order, ["zStep", "aStep"]);
    assert_eq!(workflow.finalizer_order, ["zCleanup", "aReport"]);
    assert_eq!(
        workflow.finalizers["zCleanup"].when,
        FinalizationTrigger::all()
    );
    assert_eq!(workflow.finalizers["aReport"].after, ["zCleanup"]);
    assert_eq!(
        workflow.finalizers["aReport"].when,
        [FinalizationTrigger::Failed].into_iter().collect()
    );

    let NodeBody::Command(cleanup) = &workflow.finalizers["zCleanup"].body else {
        panic!("cleanup must be a command finalizer");
    };
    assert_eq!(
        cleanup.inputs["context"],
        ValueReference::FinalizationContext
    );
    assert_eq!(cleanup.common.failure_policy, FailurePolicy::Required);

    let NodeBody::Agent(report) = &workflow.finalizers["aReport"].body else {
        panic!("report must be an agent finalizer");
    };
    assert_eq!(report.common.failure_policy, FailurePolicy::Advisory);
    assert_eq!(
        report.agent.message.attachments,
        [MessageSource::Reference(
            ValueReference::FinalizationContext
        )]
    );
}

// REQ-WFFORMAT-2, REQ-WFFORMAT-8, REQ-WFFORMAT-11, REQ-WFFORMAT-34, REQ-WFFORMAT-36, REQ-WFFORMAT-37, REQ-WFFORMAT-41, REQ-WFFORMAT-42, REQ-WFFORMAT-45, REQ-WFFORMAT-46, REQ-WFFORMAT-53, REQ-WFFORMAT-54, REQ-WFFORMAT-61, REQ-WFRECOVERY-56, REQ-WFRECOVERY-57
#[test]
fn every_canonical_invalid_fixture_is_a_structural_failure() {
    let invalid_root = fixture_root().join("invalid");
    let fixtures = fs::read_dir(&invalid_root)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(!fixtures.is_empty());

    for fixture in fixtures {
        let path = fixture.path();
        let error = decode(&fs::read(&path).unwrap()).unwrap_err();
        assert_eq!(
            error.kind(),
            DecodeFailureKind::StructuralContract,
            "unexpected classification for {}",
            path.display()
        );
    }
}

// REQ-WFFORMAT-4, REQ-WFFORMAT-5, REQ-WFFORMAT-75, REQ-WFCOND-3
#[test]
fn every_canonical_forbidden_yaml_fixture_is_refused() {
    let forbidden_root = fixture_root().join("yaml-forbidden");
    let fixtures = fs::read_dir(&forbidden_root)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(!fixtures.is_empty());

    let misclassified = fixtures
        .into_iter()
        .map(|fixture| fixture.path())
        .filter_map(|path| {
            let kind = decode(&fs::read(&path).unwrap())
                .map(|_| ())
                .map_err(|error| error.kind());
            (kind != Err(DecodeFailureKind::ForbiddenYaml))
                .then(|| format!("{}: {kind:?}", path.display()))
        })
        .collect::<Vec<_>>();
    assert!(
        misclassified.is_empty(),
        "fixtures not refused as forbidden YAML: {misclassified:#?}"
    );
}

// REQ-WFFORMAT-63
#[test]
fn finalization_context_is_structurally_restricted_to_finalizer_inputs_and_attachments() {
    let agent_prefix = "schemaVersion: 1
agentProfiles:
  reporting:
    harness:
      kind: pi
      config: { model: openai/gpt-5, thinking: high }
steps:
  work:
    kind: cmd
    command: { argv: [\"true\"] }
";
    let invalid = [
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    inputs: { context: { ref: finalization.context } }\n    command: { argv: [\"true\"] }\n".to_owned(),
        format!(
            "{agent_prefix}  observer:\n    kind: agent\n    agent:\n      profile: reporting\n      systemPrompt: system.md\n      message:\n        text: [{{ file: message.md }}]\n        attachments: [{{ ref: finalization.context }}]\n"
        ),
        format!(
            "{agent_prefix}finalizers:\n  report:\n    kind: agent\n    agent:\n      profile: reporting\n      systemPrompt: system.md\n      message:\n        text: [{{ ref: finalization.context }}]\n"
        ),
        format!(
            "{agent_prefix}exports:\n  context: {{ ref: finalization.context }}\n"
        ),
    ];

    for source in invalid {
        assert_eq!(
            decode(source.as_bytes()).unwrap_err().kind(),
            DecodeFailureKind::StructuralContract
        );
    }
}

// REQ-WFFORMAT-56
#[test]
fn agent_only_output_sources_are_structurally_refused_on_command_nodes() {
    let agent_profile = "agentProfiles:
  reporting:
    harness:
      kind: pi
      config: { model: openai/gpt-5, thinking: high }
";
    let command_body = "    kind: cmd
    command: { argv: [\"true\"] }
";
    let agent_body = "    kind: agent
    agent:
      profile: reporting
      systemPrompt: system.md
      message:
        text: [{ file: message.md }]
";
    for output in [
        "{ kind: text, from: agent_response }",
        "{ kind: json, from: agent_result, schema: result.json }",
    ] {
        let node = |body: &str| format!("  node:\n{body}    outputs:\n      value: {output}\n");
        let ordinary =
            |body: &str| format!("schemaVersion: 1\n{agent_profile}steps:\n{}", node(body));
        let finalizer = |body: &str| {
            format!(
                "schemaVersion: 1\n{agent_profile}steps:\n  work:\n{command_body}finalizers:\n{}",
                node(body)
            )
        };
        for source in [ordinary(command_body), finalizer(command_body)] {
            assert_eq!(
                decode(source.as_bytes()).unwrap_err().kind(),
                DecodeFailureKind::StructuralContract,
                "{source}"
            );
        }
        for source in [ordinary(agent_body), finalizer(agent_body)] {
            assert!(decode(source.as_bytes()).is_ok(), "{source}");
        }
    }
}

// REQ-WFFORMAT-47, REQ-WFFORMAT-51
#[test]
fn unused_agent_profiles_still_use_the_pi_config_contract() {
    let source = b"schemaVersion: 1
agentProfiles:
  unused:
    harness:
      kind: pi
      config:
        model: ''
        thinking: high
steps:
  command:
    kind: cmd
    command:
      argv: [\"true\"]
";

    assert_eq!(
        decode(source).unwrap_err().kind(),
        DecodeFailureKind::StructuralContract
    );
}

// REQ-WFFORMAT-4, REQ-WFFORMAT-5, REQ-WFFORMAT-10
#[test]
fn forbidden_yaml_features_are_not_normalized() {
    let cases = [
        (
            "duplicate mapping key",
            "schemaVersion: 1\nschemaVersion: 1\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "duplicate step ID",
            "schemaVersion: 1\nsteps:\n  work: {kind: cmd, command: {argv: [true]}}\n  work: {kind: cmd, command: {argv: [true]}}\n",
        ),
        (
            "duplicate finalizer ID",
            "schemaVersion: 1\nsteps: {work: {kind: cmd, command: {argv: [true]}}}\nfinalizers:\n  cleanup: {kind: cmd, command: {argv: [true]}}\n  cleanup: {kind: cmd, command: {argv: [true]}}\n",
        ),
        (
            "non-string mapping key",
            "schemaVersion: 1\nsteps: {1: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "anchor",
            "schemaVersion: 1\ndescription: &description text\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "alias",
            "schemaVersion: 1\ndescription: *description\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "merge key",
            "schemaVersion: 1\nsteps:\n  prepare:\n    <<: {cwd: work}\n    kind: cmd\n    command: {argv: [true]}\n",
        ),
        (
            "custom tag",
            "schemaVersion: 1\ndescription: !text hello\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "multiple documents",
            "schemaVersion: 1\nsteps: {one: {kind: cmd, command: {argv: [true]}}}\n---\nschemaVersion: 1\nsteps: {two: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "YAML 1.1 directive",
            "%YAML 1.1\n---\nschemaVersion: 1\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "YAML 1.2 directive",
            "%YAML 1.2\n---\nschemaVersion: 1\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "unknown directive",
            "%FOO bar\n---\nschemaVersion: 1\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "non-JSON core float",
            "schemaVersion: 1\ndescription: .nan\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "plain float schemaVersion",
            "schemaVersion: 1.\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
        (
            "tagged float schemaVersion",
            "schemaVersion: !!float 1\nsteps: {prepare: {kind: cmd, command: {argv: [true]}}}\n",
        ),
    ];

    for (name, source) in cases {
        let error = decode(source.as_bytes()).unwrap_err();
        assert_eq!(
            error.kind(),
            DecodeFailureKind::ForbiddenYaml,
            "unexpected classification for {name}"
        );
    }
}

// REQ-WFFORMAT-3, REQ-WFFORMAT-5
#[test]
fn yaml_1_2_core_does_not_apply_legacy_scalar_coercions() {
    let workflow = decode(
        b"schemaVersion: 01\ndescription: yes\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
    )
    .unwrap();

    assert_eq!(workflow.schema_version, 1);
    assert_eq!(workflow.description.as_deref(), Some("yes"));
    let NodeBody::Command(prepare) = &workflow.steps["prepare"].body else {
        panic!("prepare must be a command step");
    };
    assert_eq!(prepare.argv, ["true"]);

    let workflow = decode(
        b"schemaVersion: 0x1\ndescription: +.nan\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
    )
    .unwrap();
    assert_eq!(workflow.description.as_deref(), Some("+.nan"));

    let workflow = decode(
        b"schemaVersion: 1\ndescription: +0x1\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
    )
    .unwrap();
    assert_eq!(workflow.description.as_deref(), Some("+0x1"));

    assert_eq!(
        decode(
            b"schemaVersion: 1\ndescription: 1.\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
        )
        .unwrap_err()
        .kind(),
        DecodeFailureKind::ForbiddenYaml
    );
}

// REQ-WFFORMAT-3, REQ-WFFORMAT-5
#[test]
fn float_schema_version_is_forbidden_yaml() {
    let error = decode(
        b"schemaVersion: 1.0000000000000001\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
    )
    .expect_err("a float schemaVersion must not be accepted");

    assert_eq!(error.kind(), DecodeFailureKind::ForbiddenYaml);
}

// REQ-WFFORMAT-4
#[test]
fn tag_directive_is_accepted() {
    let workflow = decode(
        b"%TAG !e! tag:example.com,2026:\n---\nschemaVersion: 1\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n",
    )
    .expect("a %TAG directive is allowed");

    assert_eq!(workflow.schema_version, 1);
    assert!(workflow.steps.contains_key("prepare"));
}

// REQ-WFFORMAT-75
#[test]
fn byte_order_mark_prefix_is_accepted() {
    let source = b"schemaVersion: 1\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n";
    let mut prefixed = b"\xEF\xBB\xBF".to_vec();
    prefixed.extend_from_slice(source);

    let workflow = decode(&prefixed).expect("a leading byte order mark is not content");

    assert_eq!(workflow, decode(source).unwrap());
}

// REQ-WFFORMAT-5
#[test]
fn explicit_yaml_core_tags_are_accepted() {
    let workflow = decode(
        b"schemaVersion: !!int 1\ndescription: !!str true\nsteps: !!map {prepare: !!map {kind: cmd, command: !!map {argv: !!seq [\"true\"]}}}\n",
    )
    .expect("standard YAML Core tags are not custom tags");

    assert_eq!(workflow.schema_version, 1);
    assert_eq!(workflow.description.as_deref(), Some("true"));
    let NodeBody::Command(prepare) = &workflow.steps["prepare"].body else {
        panic!("prepare must be a command step");
    };
    assert_eq!(prepare.argv, ["true"]);
}

// REQ-WFFORMAT-76
#[test]
fn workflow_file_size_and_nesting_limits_match_cloud_boundaries() {
    let source = "schemaVersion: 1\nsteps: {prepare: {kind: cmd, command: {argv: [\"true\"]}}}\n";
    let maximum = 1024 * 1024;
    let at_limit = format!("{source}#{}\n", "x".repeat(maximum - source.len() - 2));
    assert_eq!(at_limit.len(), maximum);
    decode(at_limit.as_bytes()).expect("a 1 MiB workflow file is accepted");
    assert_eq!(
        decode(format!("{at_limit}#").as_bytes())
            .unwrap_err()
            .kind(),
        DecodeFailureKind::WorkflowFileTooLarge
    );

    let nested = |depth: usize| {
        format!(
            "schemaVersion: 1\ndescription: {}{}\nsteps: {{prepare: {{kind: cmd, command: {{argv: [\"true\"]}}}}}}\n",
            "[".repeat(depth),
            "]".repeat(depth)
        )
    };
    assert_eq!(
        decode(nested(100).as_bytes()).unwrap_err().kind(),
        DecodeFailureKind::StructuralContract
    );
    for depth in [101, 300] {
        assert_eq!(
            decode(nested(depth).as_bytes()).unwrap_err().kind(),
            DecodeFailureKind::WorkflowNestingTooDeep,
            "{depth} nested flow sequences"
        );
    }
}

// REQ-WFFORMAT-6
#[test]
fn decode_failures_are_classified() {
    let failures = [
        decode(b"schemaVersion: [\n").unwrap_err(),
        decode(
            b"schemaVersion: 1\nsteps: {prepare: &step {kind: cmd, command: {argv: [true]}}}\n",
        )
        .unwrap_err(),
        decode(
            b"schemaVersion: 1\nsteps: {prepare: {kind: cmd, unknown: true, command: {argv: [true]}}}\n",
        )
        .unwrap_err(),
    ];

    assert_eq!(failures[0].kind(), DecodeFailureKind::MalformedYaml);
    assert_eq!(failures[1].kind(), DecodeFailureKind::ForbiddenYaml);
    assert_eq!(failures[2].kind(), DecodeFailureKind::StructuralContract);
    assert_eq!(
        decode(b"schemaVersion: \xff").unwrap_err().kind(),
        DecodeFailureKind::MalformedYaml
    );
}
