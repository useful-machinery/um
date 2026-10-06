use super::*;

#[tokio::test]
async fn committed_agent_steps_release_staging_while_a_dependent_step_runs() {
    with_watchdog(async {
        for (first_outputs, response) in [
            ("", None),
            (
                r#"    outputs:
      response:
        kind: text
        from: agent_response
"#,
                Some("captured response"),
            ),
        ] {
            let source = format!(
                r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  first:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
{first_outputs}  second:
    kind: agent
    dependsOn: [first]
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
                &[("prompt.md", b"complete each step")],
                ResolvedInputs::default(),
                EnvironmentSnapshot::default(),
                CancellationSource::new(),
                1,
                1024,
            );
            let (adapter, mut control) = scripted_agent_dispatcher();
            let agents = agent_runtime(&fixture, adapter);
            let (observer, _entries, mut observed) = RecordingObserver::new();
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

            let first = control.wait_until_started().await.unwrap();
            assert_eq!(first.identity().step(), "first");
            let first_staging = first.result_endpoint_directory().to_owned();
            first.control().start().await.unwrap();
            if let Some(response) = response {
                first
                    .control()
                    .propose(ScriptedAgentValue::Response(Arc::from(response)))
                    .await
                    .unwrap();
            }
            first.control().complete().await.unwrap();

            let second = control.wait_until_started().await.unwrap();
            assert_eq!(second.identity().step(), "second");
            second.control().start().await.unwrap();
            wait_for_step_transition(&mut observed, "second", StepStateKind::Running).await;

            assert!(!first_staging.exists());
            assert_eq!(fixture.agent_inputs.active_view_count(), 1);
            assert!(!execution.is_finished());

            second.control().complete().await.unwrap();
            assert_eq!(
                execution.await.unwrap().unwrap().outcome,
                RunOutcome::Succeeded
            );
            assert_eq!(fixture.agent_inputs.active_view_count(), 0);
        }
    })
    .await;
}

#[tokio::test]
async fn committed_agent_completion_wins_a_later_cancellation_before_delivery_finishes() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  complete:
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
exports:
  response:
    ref: outputs.complete.response
"#
        );
        let cancellation = CancellationSource::new();
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"complete")],
            ResolvedInputs::default(),
            EnvironmentSnapshot::default(),
            cancellation.clone(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let (observer, _entries, _observed, mut success_reached, release_success) =
            RecordingObserver::with_step_success_gate();
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
        started
            .control()
            .propose(ScriptedAgentValue::Response(Arc::from("winner")))
            .await
            .unwrap();
        started.control().complete().await.unwrap();
        success_reached.recv().await.unwrap();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        assert!(!execution.is_finished());
        release_success.send(true).unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert!(matches!(
            result.exports["response"],
            ExportValue::Available {
                output: CapturedValue::Text(ref value)
            } if value.as_ref() == "winner"
        ));
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn agent_failure_stops_pending_command_but_keeps_active_agent_outputs() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  aFail:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
  bCommit:
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
      artifact:
        kind: file
        from: path
        path: retained.txt
        mediaType: text/plain
  cActive:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompt.md
      message:
        text:
          - file: prompt.md
  zStopped:
    kind: cmd
    dependsOn: [aFail]
    command:
      argv: ["/bin/true"]
exports:
  response:
    ref: outputs.bCommit.response
  artifact:
    ref: outputs.bCommit.artifact
"#
        );
        let cancellation = CancellationSource::new();
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"execute")],
            ResolvedInputs::default(),
            EnvironmentSnapshot::default(),
            cancellation.clone(),
            3,
            1024,
        );
        fs::write(fixture.execution_root.join("retained.txt"), b"retained").unwrap();
        let (adapter, mut control) = scripted_agent_dispatcher();
        let agents = agent_runtime(&fixture, adapter);
        let (observer, _entries, mut observed) = RecordingObserver::new();
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

        let first = control.wait_until_started().await.unwrap();
        let second = control.wait_until_started().await.unwrap();
        let third = control.wait_until_started().await.unwrap();
        let controls = BTreeMap::from([
            (first.identity().step().to_owned(), first.control().clone()),
            (
                second.identity().step().to_owned(),
                second.control().clone(),
            ),
            (third.identity().step().to_owned(), third.control().clone()),
        ]);
        assert_eq!(
            controls.keys().map(String::as_str).collect::<Vec<_>>(),
            ["aFail", "bCommit", "cActive"]
        );
        for control in controls.values() {
            control.start().await.unwrap();
        }
        controls["bCommit"]
            .propose(ScriptedAgentValue::Response(Arc::from("committed")))
            .await
            .unwrap();
        controls["bCommit"].complete().await.unwrap();
        wait_for_step_transition(&mut observed, "bCommit", StepStateKind::Succeeded).await;
        controls["aFail"]
            .fail(AgentFailureCause::HarnessFailed {
                detail: crate::workflow::agent::AgentHarnessFailureDetail::ModelError,
            })
            .await
            .unwrap();
        wait_for_step_transition(&mut observed, "aFail", StepStateKind::Failed).await;
        assert!(cancellation.request_cancellation(CancellationReason::RunnerShutdown));

        let result = execution.await.unwrap().unwrap();
        assert!(matches!(
            result.outcome,
            RunOutcome::Failed {
                later_cancellation: Some(CancellationReason::RunnerShutdown),
                ..
            }
        ));
        assert_eq!(
            result.steps["zStopped"],
            StepState::Blocked {
                detail: BlockedDetail::new([Prerequisite::control("aFail").unwrap()]).unwrap(),
            }
        );
        assert_eq!(
            result.steps["cActive"],
            StepState::Cancelled {
                detail: CancellationDetail::new(CancellationReason::RunnerShutdown),
            }
        );
        assert!(matches!(
            result.exports["response"],
            ExportValue::Available {
                output: CapturedValue::Text(_)
            }
        ));
        assert!(matches!(
            result.exports["artifact"],
            ExportValue::Available {
                output: CapturedValue::File(_)
            }
        ));
    })
    .await;
}

#[tokio::test]
async fn cancellation_discards_provisional_agent_value_and_waits_for_adapter_quiescence() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  active:
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
      artifact:
        kind: file
        from: path
        path: side-effect.txt
        mediaType: text/plain
  pending:
    kind: cmd
    dependsOn: [active]
    command:
      argv: ["/bin/true"]
exports:
  response:
    ref: outputs.active.response
  artifact:
    ref: outputs.active.artifact
"#
        );
        let cancellation = CancellationSource::new();
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("prompt.md", b"remain active")],
            ResolvedInputs::default(),
            EnvironmentSnapshot::default(),
            cancellation.clone(),
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
            .propose(ScriptedAgentValue::Response(Arc::from("provisional")))
            .await
            .unwrap();
        let side_effect = fixture.execution_root.join("side-effect.txt");
        fs::write(&side_effect, b"ordinary filesystem side effect").unwrap();
        let mut barrier = started.control().block().unwrap();
        barrier.wait_until_blocked().await.unwrap();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        assert!(!execution.is_finished());
        barrier.release().unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(
            result.outcome,
            RunOutcome::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
        assert_eq!(
            result.steps["active"],
            StepState::Cancelled {
                detail: CancellationDetail::new(CancellationReason::UserRequest),
            }
        );
        assert_eq!(
            result.steps["pending"],
            StepState::Cancelled {
                detail: CancellationDetail::new(CancellationReason::UserRequest),
            }
        );
        assert!(matches!(
            result.exports["response"],
            ExportValue::Unavailable { .. }
        ));
        assert!(matches!(
            result.exports["artifact"],
            ExportValue::Unavailable { .. }
        ));
        assert_eq!(
            fs::read(side_effect).unwrap(),
            b"ordinary filesystem side effect",
            "cancellation must not roll back ordinary filesystem writes"
        );
        assert_eq!(fixture.artifacts.staged_artifact_count(), 0);
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn harness_start_failure_is_a_start_failure() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{AGENT_PROFILE}steps:
  task:
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
            &[("prompt.md", b"start")],
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
        started
            .control()
            .fail(AgentFailureCause::start_failure(
                "launch preparation",
                "unavailable",
            ))
            .await
            .unwrap();

        let result = execution.await.unwrap().unwrap();
        assert!(
            matches!(
                &result.steps["task"],
                StepState::Failed { detail }
                    if detail.phase == FailurePhase::Start
                        && detail.code == FailureCode::HarnessStartFailed
            ),
            "a pre-start harness failure must not transition the step through running: {:?}",
            result.steps["task"]
        );
    })
    .await;
}
