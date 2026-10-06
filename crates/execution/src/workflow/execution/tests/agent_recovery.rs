use super::*;

#[tokio::test]
async fn omitted_recovery_handler_cwd_inherits_agent_target_cwd() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{}steps:
  repair:
    kind: cmd
    cwd: nested
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: recovery
        prompt: recovery.md
    command:
      argv: [/bin/sh, -c, "exit 75"]
"#,
            recovery_profile_source(AgentCompatibilityProfile::PiJsonV1)
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("recovery.md", b"Inspect the target working directory.")],
            ResolvedInputs::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationSource::new(),
            1,
            1024,
        );
        fs::create_dir(fixture.execution_root.join("nested")).unwrap();
        let (adapter, mut control) = scripted_agent_dispatcher();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn({
            let admitted = fixture.admitted.clone();
            let agents = agent_runtime(&fixture, adapter);
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
        assert_eq!(
            started.working_directory(),
            fs::canonicalize(fixture.execution_root.join("nested")).unwrap()
        );
        started.control().start().await.unwrap();
        started
            .control()
            .propose(ScriptedAgentValue::Result(Arc::new(json!({
                "schemaVersion": 1,
                "decision": "gave_up",
                "summary": "inspection complete",
                "reason": "the fixture only checks cwd inheritance"
            }))))
            .await
            .unwrap();
        started.control().complete().await.unwrap();
        assert!(matches!(
            execution.await.unwrap().unwrap().outcome,
            RunOutcome::Failed { .. }
        ));
    })
    .await;
}

#[tokio::test]
async fn all_profiles_use_one_fresh_authoritative_recovery_protocol() {
    with_watchdog(async {
        for profile in [
            AgentCompatibilityProfile::PiJsonV1,
            AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
            AgentCompatibilityProfile::CodexAppServerV1,
        ] {
            let source = format!(
                r#"schemaVersion: 1
{}steps:
  repair:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: recovery
        prompt: recovery.md
    command:
      argv: [/bin/sh, -c, "test -f repaired.marker"]
"#,
                recovery_profile_source(profile)
            );
            let fixture = execution_fixture_with_source_files(
                &source,
                &[(
                    "recovery.md",
                    b"Repair the generated workspace, then request a recheck.",
                )],
                ResolvedInputs::default(),
                EnvironmentSnapshot::new([
                    ("PATH", "/bin:/usr/bin"),
                    ("UM_INHERITED", "must-be-scrubbed"),
                ]),
                CancellationSource::new(),
                1,
                1024,
            );
            let (adapter, mut control) = scripted_agent_dispatcher();
            let accounting = InvocationAccountingLog::default();
            let agents = agent_runtime_with_accounting(&fixture, adapter, accounting.clone());
            let diagnostics = StepDiagnosticLog::default();
            let execution_diagnostics = diagnostics.clone();
            let artifacts = fixture.artifacts.clone();
            let inputs = fixture.inputs.clone();
            let (observer, observations, _observed) = RecordingObserver::new();
            let execution = tokio::spawn({
                let admitted = fixture.admitted.clone();
                async move {
                    execute_workflow(
                        admitted,
                        &artifacts,
                        &inputs,
                        &execution_diagnostics,
                        agents,
                        TestClock,
                        observer,
                    )
                    .await
                }
            });

            let started = control.wait_until_started().await.unwrap();
            assert_eq!(started.profile(), profile);
            assert_eq!(started.system_prompt(), RECOVERY_AGENT_INSTRUCTIONS);
            assert_eq!(
                started.message(),
                "Repair the generated workspace, then request a recheck."
            );
            assert_eq!(started.value_kind(), AgentValueKind::Result);
            assert!(started.attachments().is_empty());
            assert!(started.result_endpoint_directory().is_dir());
            assert!(started.diagnostic_directory().is_dir());
            let context_path = PathBuf::from(
                started
                    .environment()
                    .variable(std::ffi::OsStr::new(RECOVERY_CONTEXT_VARIABLE))
                    .unwrap(),
            );
            assert!(
                started
                    .environment()
                    .variable(std::ffi::OsStr::new("UM_INHERITED"))
                    .is_none()
            );
            let context = read_recovery_context(&fs::read(&context_path).unwrap()).unwrap();
            assert_eq!(context.target.id, "repair");
            assert_eq!(context.recovery_round, 1);
            assert_eq!(context.failed_execution.execution_number, 1);
            assert_eq!(context.failed_execution.invocation_id, 1);
            assert!(
                context
                    .diagnostics
                    .iter()
                    .all(|entry| entry.trust == "untrusted")
            );
            let handler_action = started.identity().invocation();
            let result_endpoint = started.result_endpoint_directory().to_owned();
            let diagnostic_directory = started.diagnostic_directory().to_owned();
            started.control().start().await.unwrap();
            started
                .control()
                .observe(AgentObservation::AssistantText {
                    text: Arc::from("I choose gave_up in prose, which is not authority."),
                })
                .await
                .unwrap();
            started
                .control()
                .observe(AgentObservation::Usage {
                    input_tokens: 7,
                    output_tokens: 3,
                })
                .await
                .unwrap();
            fs::write(
                fixture.execution_root.join("repaired.marker"),
                b"agent mutation",
            )
            .unwrap();
            started
                .control()
                .propose(ScriptedAgentValue::Result(Arc::new(json!({
                    "schemaVersion": 1,
                    "decision": "recheck",
                    "summary": "repaired workspace",
                    "reason": "run the unchanged target"
                }))))
                .await
                .unwrap();
            started.control().complete().await.unwrap();

            let result = execution.await.unwrap().unwrap();
            assert_eq!(result.outcome, RunOutcome::Succeeded);
            assert!(!context_path.exists());
            assert!(!result_endpoint.exists());
            assert!(diagnostic_directory.exists());
            assert_eq!(
                accounting.usage(handler_action),
                Some(InvocationUsage {
                    input_tokens: 7,
                    output_tokens: 3,
                })
            );
            let native = accounting.native_session(handler_action).unwrap();
            assert_eq!(native.profile, profile);
            assert_ne!(native.diagnostic_identity.as_ref(), "unavailable");
            match profile {
                AgentCompatibilityProfile::PiJsonV1
                | AgentCompatibilityProfile::ClaudeCodeStreamJsonV1 => {
                    assert!(native.native_session_identity.is_some());
                }
                AgentCompatibilityProfile::CodexAppServerV1 => {
                    assert!(native.native_session_identity.is_none());
                }
            }
            let observations = observations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(observations.iter().any(|observation| matches!(
                observation,
                ExecutionObservation::Agent(envelope)
                    if envelope.invocation() == handler_action
                        && matches!(envelope.observation(), AgentObservation::AssistantText { .. })
            )));
            assert_eq!(diagnostics.invocation_ids("repair").len(), 2);
        }
    })
    .await;
}

#[tokio::test]
async fn failed_agent_target_handler_and_recheck_use_three_fresh_accounted_invocations() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{}steps:
  repair:
    kind: agent
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: recovery
        prompt: recovery.md
    agent:
      profile: recovery
      systemPrompt: target.md
      message:
        text:
          - file: target.md
"#,
            recovery_profile_source(AgentCompatibilityProfile::PiJsonV1)
        );
        let fixture = execution_fixture_with_source_files(
            &source,
            &[
                ("target.md", b"Run the unchanged target protocol."),
                (
                    "recovery.md",
                    b"Repair the workspace and request a recheck.",
                ),
            ],
            ResolvedInputs::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationSource::new(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let accounting = InvocationAccountingLog::default();
        let agents = agent_runtime_with_accounting(&fixture, adapter, accounting.clone());
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

        let target_one = control.wait_until_started().await.unwrap();
        assert_eq!(target_one.value_kind(), AgentValueKind::None);
        assert_eq!(
            target_one.system_prompt(),
            "Run the unchanged target protocol."
        );
        let target_one_action = target_one.identity().invocation();
        let target_one_staging = target_one.result_endpoint_directory().to_owned();
        let target_one_diagnostics = target_one.diagnostic_directory().to_owned();
        target_one.control().start().await.unwrap();
        target_one
            .control()
            .observe(AgentObservation::Usage {
                input_tokens: 2,
                output_tokens: 1,
            })
            .await
            .unwrap();
        target_one
            .control()
            .fail(AgentFailureCause::HarnessProtocolFailed)
            .await
            .unwrap();

        let handler = control.wait_until_started().await.unwrap();
        assert!(!target_one_staging.exists());
        assert_eq!(handler.value_kind(), AgentValueKind::Result);
        assert_eq!(handler.system_prompt(), RECOVERY_AGENT_INSTRUCTIONS);
        let handler_action = handler.identity().invocation();
        let handler_staging = handler.result_endpoint_directory().to_owned();
        let handler_diagnostics = handler.diagnostic_directory().to_owned();
        handler.control().start().await.unwrap();
        handler
            .control()
            .observe(AgentObservation::Usage {
                input_tokens: 3,
                output_tokens: 2,
            })
            .await
            .unwrap();
        fs::write(
            fixture.execution_root.join("agent-repaired.marker"),
            b"visible",
        )
        .unwrap();
        handler
            .control()
            .propose(ScriptedAgentValue::Result(Arc::new(json!({
                "schemaVersion": 1,
                "decision": "recheck",
                "summary": "agent repaired workspace",
                "reason": "run the unchanged target"
            }))))
            .await
            .unwrap();
        handler.control().complete().await.unwrap();

        let target_two = control.wait_until_started().await.unwrap();
        assert!(!handler_staging.exists());
        assert_eq!(target_two.value_kind(), AgentValueKind::None);
        assert_eq!(
            target_two.system_prompt(),
            "Run the unchanged target protocol."
        );
        assert_eq!(target_two.message(), "Run the unchanged target protocol.");
        assert_eq!(
            fs::read(fixture.execution_root.join("agent-repaired.marker")).unwrap(),
            b"visible"
        );
        let target_two_action = target_two.identity().invocation();
        let target_two_diagnostics = target_two.diagnostic_directory().to_owned();
        assert_ne!(target_one_action, handler_action);
        assert_ne!(handler_action, target_two_action);
        assert_ne!(target_one_diagnostics, handler_diagnostics);
        assert_ne!(handler_diagnostics, target_two_diagnostics);
        target_two.control().start().await.unwrap();
        target_two
            .control()
            .observe(AgentObservation::Usage {
                input_tokens: 5,
                output_tokens: 4,
            })
            .await
            .unwrap();
        target_two.control().complete().await.unwrap();

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert_eq!(
            accounting.usage(target_one_action),
            Some(InvocationUsage {
                input_tokens: 2,
                output_tokens: 1,
            })
        );
        assert_eq!(
            accounting.usage(handler_action),
            Some(InvocationUsage {
                input_tokens: 3,
                output_tokens: 2,
            })
        );
        assert_eq!(
            accounting.usage(target_two_action),
            Some(InvocationUsage {
                input_tokens: 5,
                output_tokens: 4,
            })
        );
        assert_eq!(accounting.recorded_invocations().len(), 3);
    })
    .await;
}

#[tokio::test]
async fn cancellation_waits_for_recovery_agent_quiescence_and_rejects_late_decision() {
    with_watchdog(async {
        let source = format!(
            r#"schemaVersion: 1
{}steps:
  repair:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: agent
        profile: recovery
        prompt: recovery.md
    command:
      argv: [/bin/sh, -c, "exit 75"]
"#,
            recovery_profile_source(AgentCompatibilityProfile::PiJsonV1)
        );
        let cancellation = CancellationSource::new();
        let fixture = execution_fixture_with_source_files(
            &source,
            &[("recovery.md", b"Wait for operator control.")],
            ResolvedInputs::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            cancellation.clone(),
            1,
            1024,
        );
        let (adapter, mut control) = scripted_agent_dispatcher();
        let diagnostics = StepDiagnosticLog::default();
        let execution_diagnostics = diagnostics.clone();
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
                    &execution_diagnostics,
                    agents,
                    TestClock,
                    NoopExecutionObserver,
                )
                .await
            }
        });

        let started = control.wait_until_started().await.unwrap();
        let context_path = PathBuf::from(
            started
                .environment()
                .variable(std::ffi::OsStr::new(RECOVERY_CONTEXT_VARIABLE))
                .unwrap(),
        );
        let result_endpoint = started.result_endpoint_directory().to_owned();
        started.control().start().await.unwrap();
        let mut barrier = started.control().block().unwrap();
        barrier.wait_until_blocked().await.unwrap();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        assert!(
            !execution.is_finished(),
            "terminal cancellation must wait for the blocked adapter"
        );
        let late_control = started.control().clone();
        let late_decision = tokio::spawn(async move {
            late_control
                .propose(ScriptedAgentValue::Result(Arc::new(json!({
                    "schemaVersion": 1,
                    "decision": "recheck",
                    "summary": "too late",
                    "reason": "cancellation already owns authority"
                }))))
                .await
        });
        barrier.release().unwrap();
        assert!(late_decision.await.unwrap().is_err());

        let result = (&mut execution).await.unwrap().unwrap();
        assert_eq!(
            result.outcome,
            RunOutcome::Cancelled {
                reason: CancellationReason::UserRequest
            }
        );
        assert!(!context_path.exists());
        assert!(!result_endpoint.exists());
        assert_eq!(diagnostics.invocation_ids("repair").len(), 1);
    })
    .await;
}
