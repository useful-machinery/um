use super::*;

#[tokio::test]
async fn semantic_outputs_atomic_mixed_success() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  produce:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: text
        from: agent_response
      summary:
        kind: text
        from: path
        path: summary.txt
      data:
        kind: json
        from: path
        path: data.json
        schema: result.schema.json
      artifact:
        kind: file
        from: path
        path: agent.txt
        mediaType: text/plain
  consume:
    kind: cmd
    inputs:
      response:
        ref: outputs.produce.response
      summary:
        ref: outputs.produce.summary
      data:
        ref: outputs.produce.data
    command:
      argv: ["/bin/sh", "-c", "printf consumed > consumed.txt"]
    outputs:
      consumed:
        kind: file
        from: path
        path: consumed.txt
        mediaType: text/plain
exports:
  response:
    ref: outputs.produce.response
  summary:
    ref: outputs.produce.summary
  data:
    ref: outputs.produce.data
  artifact:
    ref: outputs.produce.artifact
  consumed:
    ref: outputs.consume.consumed
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[
                ("prompt.md", b"produce the declared outputs"),
                (
                    "result.schema.json",
                    br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
                ),
            ],
            ResolvedInputs::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            2,
            1024,
        );
        fs::write(fixture.execution_root.join("agent.txt"), b"agent artifact").unwrap();
        fs::write(
            fixture.execution_root.join("summary.txt"),
            b"\xef\xbb\xbfline one\r\nline two\n",
        )
        .unwrap();
        fs::write(
            fixture.execution_root.join("data.json"),
            br#"{ "z": 2, "a": 1 }"#,
        )
        .unwrap();
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let diagnostics = StepDiagnosticLog::default();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &diagnostics,
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        assert_eq!(started.identity().run().as_ref(), "run-fixed");
        assert_eq!(started.identity().step(), "produce");
        started
            .control()
            .observe(AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessStarted,
            })
            .await
            .unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Response(Arc::from("agent response")))
            .await
            .unwrap();
        started.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        let StepState::Succeeded { outputs } = &result.steps["produce"] else {
            panic!("agent producer did not succeed");
        };
        assert_eq!(outputs.len(), 4);
        assert!(matches!(
            &outputs["response"],
            CapturedValue::Text(value) if value.as_ref() == "agent response"
        ));
        assert!(matches!(
            &outputs["summary"],
            CapturedValue::Text(value)
                if value.carrier() == b"\xef\xbb\xbfline one\r\nline two\n"
        ));
        assert!(matches!(
            &outputs["data"],
            CapturedValue::Json(value) if value.carrier() == br#"{"a":1,"z":2}"#
        ));
        let ExportValue::Available { output } = &result.exports["consumed"] else {
            panic!("downstream command output was unavailable");
        };
        let mut consumed = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut consumed)
            .unwrap();
        assert_eq!(consumed, b"consumed");
        let ExportValue::Available { output } = &result.exports["artifact"] else {
            panic!("agent file output was unavailable");
        };
        let mut artifact = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut artifact)
            .unwrap();
        assert_eq!(artifact, b"agent artifact");
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn structured_agent_result_flows_only_through_its_explicit_command_binding() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  produce:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: result.schema.json
  consume:
    kind: cmd
    inputs:
      result:
        ref: outputs.produce.result
    command:
      argv: ["/bin/sh", "-c", "IFS= read -r value < \"$UM_STEP_INPUTS/values/result\" || true; printf '%s' \"$value\" > consumed.json"]
    outputs:
      consumed:
        kind: file
        from: path
        path: consumed.json
        mediaType: application/json
exports:
  result:
    ref: outputs.produce.result
  consumed:
    ref: outputs.consume.consumed
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[
                ("prompt.md", b"return a result"),
                (
                    "result.schema.json",
                    br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
                ),
            ],
            ResolvedInputs::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Result(Arc::new(json!({
                "z": 2,
                "a": 1
            }))))
            .await
            .unwrap();
        started.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        let ExportValue::Available { output } = &result.exports["result"] else {
            panic!("structured result was unavailable");
        };
        assert!(matches!(
            output,
            CapturedValue::Json(value) if value.as_ref() == &json!({"z": 2, "a": 1})
        ));
        let ExportValue::Available { output } = &result.exports["consumed"] else {
            panic!("result consumer was unavailable");
        };
        let mut consumed = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut consumed)
            .unwrap();
        assert_eq!(consumed, br#"{"a":1,"z":2}"#);
    })
    .await;
}

#[tokio::test]
async fn semantic_outputs_command_and_agent_path_matrix() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  aResponse:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: text
        from: agent_response
  bResult:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      result:
        kind: json
        from: agent_result
        schema: result.schema.json
  cPath:
    kind: cmd
    command:
      argv: ["/bin/sh", "-c", "true"]
    outputs:
      text:
        kind: text
        from: path
        path: path.txt
      json:
        kind: json
        from: path
        path: path.json
        schema: result.schema.json
      artifact:
        kind: file
        from: path
        path: upstream.txt
        mediaType: text/plain
  zConsumer:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - ref: outputs.aResponse.response
          - ref: outputs.cPath.text
        attachments:
          - ref: outputs.bResult.result
          - ref: outputs.cPath.json
          - ref: outputs.cPath.artifact
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[
                ("prompt.md", b"consume committed values"),
                (
                    "result.schema.json",
                    br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#,
                ),
            ],
            ResolvedInputs::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            3,
            1024,
        );
        fs::write(fixture.execution_root.join("upstream.txt"), b"file exact").unwrap();
        fs::write(
            fixture.execution_root.join("path.txt"),
            b"path text\r\nwith trailing newline\n",
        )
        .unwrap();
        fs::write(
            fixture.execution_root.join("path.json"),
            br#"{ "z": 2, "a": 1 }"#,
        )
        .unwrap();
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let mut execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let first = control.wait_until_started().await.unwrap();
        let second = control.wait_until_started().await.unwrap();
        let producers = BTreeMap::from([
            (first.identity().step().to_owned(), first.control().clone()),
            (second.identity().step().to_owned(), second.control().clone()),
        ]);
        assert_eq!(
            producers.keys().map(String::as_str).collect::<Vec<_>>(),
            ["aResponse", "bResult"]
        );
        for producer in producers.values() {
            producer.start().await.unwrap();
        }
        producers["aResponse"]
            .propose(ScriptedAgentValue::Response(Arc::from("response exact")))
            .await
            .unwrap();
        producers["bResult"]
            .propose(ScriptedAgentValue::Result(Arc::new(json!({
                "z": 2,
                "a": 1
            }))))
            .await
            .unwrap();
        producers["aResponse"].complete().await.unwrap();
        producers["bResult"].complete().await.unwrap();

        let consumer = tokio::select! {
            consumer = control.wait_until_started() => match consumer {
                Ok(consumer) => consumer,
                Err(failure) => panic!(
                    "adapter stopped before the dependent agent started ({failure:?}): {:?}",
                    (&mut execution).await
                ),
            },
            result = &mut execution => panic!(
                "workflow finished before the dependent agent started: {result:?}"
            ),
        };
        assert_eq!(consumer.identity().step(), "zConsumer");
        assert_eq!(
            consumer.message(),
            "response exact\n\npath text\r\nwith trailing newline\n"
        );
        assert_eq!(consumer.attachments().len(), 3);
        assert_eq!(consumer.attachments()[0].media_type(), "application/json");
        assert_eq!(
            fs::read(consumer.attachments()[0].path()).unwrap(),
            br#"{"a":1,"z":2}"#
        );
        assert_eq!(consumer.attachments()[1].media_type(), "application/json");
        assert_eq!(
            fs::read(consumer.attachments()[1].path()).unwrap(),
            br#"{"a":1,"z":2}"#
        );
        assert_eq!(consumer.attachments()[2].media_type(), "text/plain");
        assert_eq!(
            fs::read(consumer.attachments()[2].path()).unwrap(),
            b"file exact"
        );
        consumer.control().start().await.unwrap();
        consumer.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert!(matches!(
            result.steps["zConsumer"],
            StepState::Succeeded { .. }
        ));
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn semantic_outputs_atomic_mixed_failure() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  produce:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
    outputs:
      response:
        kind: text
        from: agent_response
      missing:
        kind: file
        from: path
        path: missing.txt
        mediaType: text/plain
  consume:
    kind: cmd
    inputs:
      response:
        ref: outputs.produce.response
    command:
      argv: ["/bin/true"]
exports:
  response:
    ref: outputs.produce.response
  missing:
    ref: outputs.produce.missing
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"produce output")],
            ResolvedInputs::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Response(Arc::from("must not commit")))
            .await
            .unwrap();
        started.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        let StepState::Failed { detail } = &result.steps["produce"] else {
            panic!("agent producer did not report the exact capture failure");
        };
        assert_eq!(detail.phase, FailurePhase::OutputCapture);
        assert_eq!(detail.code, FailureCode::OutputMissing);
        assert_eq!(detail.output.as_deref(), Some("missing"));
        assert_eq!(
            result.steps["consume"],
            StepState::Blocked {
                detail: BlockedDetail::new([
                    Prerequisite::body("outputs.produce.response").unwrap()
                ])
                .unwrap(),
            }
        );
        assert!(matches!(
            result.exports["response"],
            ExportValue::Unavailable { .. }
        ));
        assert!(matches!(
            result.exports["missing"],
            ExportValue::Unavailable { .. }
        ));
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
        assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
        assert_eq!(fixture.artifacts.budget_usage(), (0, 0));
        assert_eq!(fixture.artifacts.reservation_usage(), (0, 0));
    })
    .await;
}

#[derive(Debug, Eq, PartialEq)]
struct AgentEngineTranscript {
    observations: Vec<AgentObservationEnvelope>,
    terminal_transitions: Vec<StepStateKind>,
}

#[tokio::test]
async fn no_value_agent_observations_are_repeatable_and_never_become_outputs() {
    let first = run_no_value_agent_transcript().await;
    let second = run_no_value_agent_transcript().await;
    assert_eq!(first, second);
    assert_eq!(first.observations.len(), 2);
    assert_eq!(first.observations[0].sequence().get(), 1);
    assert_eq!(first.observations[1].sequence().get(), 2);
    assert_eq!(first.terminal_transitions, [StepStateKind::Succeeded]);
}

async fn run_no_value_agent_transcript() -> AgentEngineTranscript {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  observe:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
"#
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"observe only")],
            ResolvedInputs::default(),
            EnvironmentSnapshot::default(),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let (observer, entries, _observed) = RecordingObserver::new();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            async move {
                execute_workflow(
                    admitted,
                    &artifacts,
                    &inputs,
                    &StepDiagnosticLog::default(),
                    agents,
                    TestClock,
                    observer,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        started.control().start().await.unwrap();
        for observation in [
            AgentObservation::AssistantText {
                text: Arc::from("not an implicit response"),
            },
            AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::HarnessQuiescent,
            },
        ] {
            started.control().observe(observation).await.unwrap();
        }
        started.control().complete().await.unwrap();
        let result = execution.await.unwrap().unwrap();
        let StepState::Succeeded { outputs } = &result.steps["observe"] else {
            panic!("no-value agent did not succeed");
        };
        assert!(outputs.is_empty());
        assert!(result.exports.is_empty());
        let entries = entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let observations = entries
            .iter()
            .filter_map(|observation| match observation {
                ExecutionObservation::Agent(observation) => Some(observation.clone()),
                ExecutionObservation::Transition(_)
                | ExecutionObservation::CommandOutput(_)
                | ExecutionObservation::CommandOutputClosed(_) => None,
            })
            .collect();
        let terminal_transitions = entries
            .iter()
            .filter_map(|observation| match observation {
                ExecutionObservation::Transition(transition) => match transition.as_ref() {
                    TransitionObservation {
                        event: TransitionEvent::Step { step, to, .. },
                        ..
                    } if step == "observe"
                        && matches!(
                            to,
                            StepStateKind::Succeeded
                                | StepStateKind::Failed
                                | StepStateKind::Cancelled
                        ) =>
                    {
                        Some(*to)
                    }
                    _ => None,
                },
                ExecutionObservation::CommandOutput(_)
                | ExecutionObservation::CommandOutputClosed(_)
                | ExecutionObservation::Agent(_) => None,
            })
            .collect();
        AgentEngineTranscript {
            observations,
            terminal_transitions,
        }
    })
    .await
}
