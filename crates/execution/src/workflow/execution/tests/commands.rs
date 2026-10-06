use super::*;

#[tokio::test]
async fn command_environment_requires_declared_passthrough() {
    for (declaration, expected) in [
        ("", b"unset".as_slice()),
        (
            "environmentPassthrough: [TEST_SENTINEL]\n",
            b"present".as_slice(),
        ),
    ] {
        let source = format!(
            "schemaVersion: 1\n{declaration}steps:\n  check:\n    kind: cmd\n    command:\n      argv: [/bin/sh, -c, 'printf %s \"${{TEST_SENTINEL-unset}}\" > observed.txt']\n"
        );
        let fixture = execution_fixture(
            &source,
            ResolvedInputs::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin"), ("TEST_SENTINEL", "present")]),
            CancellationSource::new(),
            1,
            1024,
        );
        let result = execute_workflow(
            fixture.admitted,
            &fixture.artifacts,
            &fixture.inputs,
            &StepDiagnosticLog::default(),
            AgentExecution::disabled(),
            TestClock,
            NoopExecutionObserver,
        )
        .await
        .unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert_eq!(
            fs::read(fixture.execution_root.join("observed.txt")).unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn source_neutral_command_handler_repairs_and_rechecks_with_private_authority() {
    with_watchdog(async {
        let source = r#"schemaVersion: 1
steps:
  repair:
    kind: cmd
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv:
            - /bin/sh
            - -c
            - |
              if IFS= read -r unexpected; then exit 91; fi
              test -r "$UM_RECOVERY_CONTEXT"
              test ! -w "$UM_RECOVERY_CONTEXT"
              /bin/grep -q '"schemaVersion": 1' "$UM_RECOVERY_CONTEXT"
              /bin/grep -q '"recoveryRound": 1' "$UM_RECOVERY_CONTEXT"
              /bin/grep -q '"executionNumber": 1' "$UM_RECOVERY_CONTEXT"
              /bin/grep -q '"command_stderr"' "$UM_RECOVERY_CONTEXT"
              test -z "${UM_INHERITED+x}"
              printf '%s\n%s\n' UM_RECOVERY_CONTEXT UM_RECOVERY_RESULT > recovery-environment.txt
              printf '%s\n%s\n' "$UM_RECOVERY_CONTEXT" "$UM_RECOVERY_RESULT" > recovery-private-paths.txt
              printf repaired > repaired.marker
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"repaired workspace","reason":"target should pass unchanged"}' > "$UM_RECOVERY_RESULT"
              printf 'handler ordinary output is diagnostic only'
    command:
      argv:
        - /bin/sh
        - -c
        - |
          printf 'target diagnostic' >&2
          if test -f repaired.marker; then
            printf 'terminal output' > artifact.txt
            exit 0
          fi
          printf 'provisional output' > artifact.txt
          exit 75
    outputs:
      artifact:
        kind: file
        from: path
        path: artifact.txt
        mediaType: text/plain
exports:
  artifact:
    ref: outputs.repair.artifact
"#;
        let fixture = execution_fixture(
            source,
            ResolvedInputs::default(),
            EnvironmentSnapshot::new([
                ("PATH", "/bin:/usr/bin"),
                ("EXPLICIT_VALUE", "retained"),
                ("UM_INHERITED", "must-be-scrubbed"),
            ]),
            CancellationSource::new(),
            1,
            1024,
        );
        let diagnostics = StepDiagnosticLog::default();
        let result = execute_workflow(
            fixture.admitted,
            &fixture.artifacts,
            &fixture.inputs,
            &diagnostics,
            AgentExecution::disabled(),
            TestClock,
            NoopExecutionObserver,
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, RunOutcome::Succeeded);
        let ExportValue::Available { output } = &result.exports["artifact"] else {
            panic!("the recovered target output must be available");
        };
        let mut bytes = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut bytes)
            .unwrap();
        assert_eq!(bytes, b"terminal output");
        assert_eq!(
            fs::read_to_string(fixture.execution_root.join("recovery-environment.txt")).unwrap(),
            "UM_RECOVERY_CONTEXT\nUM_RECOVERY_RESULT\n"
        );
        let private_paths = fs::read_to_string(
            fixture.execution_root.join("recovery-private-paths.txt"),
        )
        .unwrap();
        assert!(private_paths.lines().all(|path| !Path::new(path).exists()));
        assert_eq!(fs::read(fixture.execution_root.join("repaired.marker")).unwrap(), b"repaired");
        let invocations = diagnostics.invocation_ids("repair");
        assert_eq!(invocations.len(), 3);
        assert_eq!(invocations.iter().copied().collect::<std::collections::BTreeSet<_>>().len(), 3);
        let handler_diagnostic = diagnostics.get_invocation("repair", invocations[1]).unwrap();
        assert_eq!(
            handler_diagnostic.standard_output().bytes(),
            b"handler ordinary output is diagnostic only"
        );
        assert_eq!(
            diagnostics.get("repair").unwrap().standard_output().bytes(),
            b"",
            "handler diagnostics must not become ordinary step output"
        );
    })
    .await;
}

#[tokio::test]
async fn omitted_recovery_handler_cwd_inherits_command_target_cwd() {
    with_watchdog(async {
        let source = r#"schemaVersion: 1
steps:
  repair:
    kind: cmd
    cwd: nested
    recovery:
      retries: 1
      handler:
        kind: cmd
        command:
          argv:
            - /bin/sh
            - -c
            - |
              printf repaired > repaired.marker
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"repaired workspace","reason":"rerun the target"}' > "$UM_RECOVERY_RESULT"
    command:
      argv: [/bin/sh, -c, "test -f repaired.marker || exit 75"]
"#;
        let fixture = execution_fixture(
            source,
            ResolvedInputs::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationSource::new(),
            1,
            1024,
        );
        fs::create_dir(fixture.execution_root.join("nested")).unwrap();

        let result = execute_workflow(
            fixture.admitted,
            &fixture.artifacts,
            &fixture.inputs,
            &StepDiagnosticLog::default(),
            AgentExecution::disabled(),
            TestClock,
            NoopExecutionObserver,
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert_eq!(
            fs::read(fixture.execution_root.join("nested/repaired.marker")).unwrap(),
            b"repaired"
        );
    })
    .await;
}

#[tokio::test]
async fn semantic_outputs_recovery_reruns_complete_target() {
    with_watchdog(async {
        let source = r#"schemaVersion: 1
steps:
  repair:
    kind: cmd
    recovery:
      retries: 1
    command:
      argv:
        - /bin/sh
        - -c
        - |
          if test -f target-runs.txt; then
            printf 'captured only from execution 2' > artifact.txt
          fi
          printf x >> target-runs.txt
    outputs:
      artifact:
        kind: file
        from: path
        path: artifact.txt
        mediaType: text/plain
exports:
  artifact:
    ref: outputs.repair.artifact
"#;
        let fixture = execution_fixture(
            source,
            ResolvedInputs::default(),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationSource::new(),
            1,
            1024,
        );
        let (observer, observations, _observed) = RecordingObserver::new();
        let result = execute_workflow(
            fixture.admitted,
            &fixture.artifacts,
            &fixture.inputs,
            &StepDiagnosticLog::default(),
            AgentExecution::disabled(),
            TestClock,
            observer,
        )
        .await
        .unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        let invocations =
            observations
                .lock()
                .unwrap()
                .iter()
                .filter_map(|observation| match observation {
                    ExecutionObservation::Transition(transition) => match &transition.step {
                        Some(ObservedStepTransition::Recovery {
                            active:
                                crate::workflow::runtime::ActiveStepInvocation::Target {
                                    execution_number,
                                },
                            active_invocation_id,
                            ..
                        }) if execution_number.get() == 2 => Some(*active_invocation_id),
                        _ => None,
                    },
                    _ => None,
                })
                .collect::<Vec<_>>();
        assert!(
            invocations.len() >= 3,
            "observe start, running, and output capture"
        );
        assert!(
            invocations.iter().all(|id| *id == invocations[0]),
            "output capture must retain the recovered target's invocation identity"
        );
        assert_eq!(
            fs::read(fixture.execution_root.join("target-runs.txt")).unwrap(),
            b"xx"
        );
        let ExportValue::Available { output } = &result.exports["artifact"] else {
            panic!("execution 2 must own the terminal output");
        };
        let mut bytes = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut bytes)
            .unwrap();
        assert_eq!(bytes, b"captured only from execution 2");
        assert_eq!(fixture.artifacts.reservation_usage(), (0, 0));
        assert_eq!(fixture.inputs.reservation_usage(), (0, 0, 0));
        assert_eq!(fixture.agent_inputs.active_view_count(), 0);
    })
    .await;
}

#[tokio::test]
async fn command_handler_failures_stop_once_without_authorizing_recheck() {
    with_watchdog(async {
        let scenarios = [
            (
                "start",
                r#"command:
          argv: [/definitely-missing-recovery-handler]"#,
                RecoveryHandlerFailure::CommandLaunchFailed,
            ),
            (
                "execution",
                r#"command:
          argv: [/bin/sh, -c, "printf '%s' '{\"schemaVersion\":1,\"decision\":\"recheck\",\"summary\":\"looks valid\",\"reason\":\"but exit fails\"}' > \"$UM_RECOVERY_RESULT\"; exit 9"]"#,
                RecoveryHandlerFailure::CommandExitFailed { code: Some(9) },
            ),
            (
                "missing",
                r#"command:
          argv: [/bin/sh, -c, "true"]"#,
                RecoveryHandlerFailure::ResultMissing,
            ),
            (
                "validation",
                r#"command:
          argv: [/bin/sh, -c, "printf '{' > \"$UM_RECOVERY_RESULT\""]"#,
                RecoveryHandlerFailure::DecisionInvalid(
                    RecoveryDecisionFailureKind::InvalidJson,
                ),
            ),
            (
                "settlement",
                r#"command:
          argv:
            - /bin/sh
            - -c
            - |
              printf '%s' '{"schemaVersion":1,"decision":"recheck","summary":"valid","reason":"before settlement sabotage"}' > "$UM_RECOVERY_RESULT"
              root=${UM_RECOVERY_CONTEXT%/context/context.json}
              mv "$root" "$root-moved""#,
                RecoveryHandlerFailure::SettlementFailed,
            ),
        ];

        for (name, command, expected) in scenarios {
            let source = format!(
                "schemaVersion: 1\nsteps:\n  repair:\n    kind: cmd\n    recovery:\n      retries: 1\n      handler:\n        kind: cmd\n        {command}\n    command:\n      argv: [/bin/sh, -c, \"printf x >> target-count.txt; exit 75\"]\n"
            );
            let fixture = execution_fixture(
                &source,
                ResolvedInputs::default(),
                EnvironmentSnapshot::new([(
                    OsString::from("PATH"),
                    env::var_os("PATH").unwrap(),
                )]),
                CancellationSource::new(),
                1,
                1024,
            );
            let diagnostics = StepDiagnosticLog::default();
            let coordinated = crate::workflow::step_runtime::execute_workflow_observed(
                fixture.admitted,
                &fixture.artifacts,
                &fixture.inputs,
                &diagnostics,
                TestClock,
                NoopCommitPort,
                NoopExecutionObserver,
                AgentExecution::disabled(),
                crate::workflow::process_group::ProcessGuardRegistry::default(),
            )
            .await
            .unwrap_or_else(|failure| panic!("{name} scenario failed to coordinate: {failure:?}"));
            assert!(matches!(coordinated.state.workflow, WorkflowState::Failed { .. }));
            assert_eq!(
                fs::read(fixture.execution_root.join("target-count.txt")).unwrap(),
                b"x",
                "{name} handler failure must not authorize target execution 2"
            );
            let recovery = coordinated.state.steps["repair"].recovery.as_ref().unwrap();
            assert_eq!(recovery.rounds.len(), 1);
            let RecoveryHandlerOutcome::Failed {
                cause: StepFailureCause::RecoveryHandler(actual),
                ..
            } = &recovery.rounds[0].handler.as_ref().unwrap().outcome
            else {
                panic!("{name} did not retain one typed handler failure");
            };
            assert_eq!(actual, &expected, "{name} handler failure kind");
        }
    })
    .await;
}

#[tokio::test]
async fn configured_inactive_local_recovery_preserves_target_execution() {
    let source = "schemaVersion: 1\nsteps:\n  guarded:\n    kind: cmd\n    recovery:\n      retries: 1\n    command:\n      argv: [\"/bin/sh\", \"-c\", \": > target-started\"]\n";
    let fixture = execution_fixture(
        source,
        ResolvedInputs::default(),
        EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
        CancellationSource::new(),
        1,
        32,
    );
    let admitted = admit_local_workflow(
        resolution::resolve(&fixture.source_root, Path::new("workflow.yaml")).unwrap(),
        ResolvedInputs::default(),
        ExecutionContext::new(
            fixture.execution_root.clone(),
            ExecutionPolicyLimits::new(
                1,
                CaptureLimits::new(16, 1024 * 1024, 8 * 1024 * 1024),
                InputLimits::new(16, 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
                32,
            ),
            EnvironmentSnapshot::new([("PATH", "/bin:/usr/bin")]),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        ),
    )
    .unwrap();

    let result = execute_workflow(
        admitted,
        &fixture.artifacts,
        &fixture.inputs,
        &StepDiagnosticLog::default(),
        AgentExecution::disabled(),
        TestClock,
        NoopExecutionObserver,
    )
    .await;
    assert_eq!(result.unwrap().outcome, RunOutcome::Succeeded);
    assert!(fixture.execution_root.join("target-started").exists());
}

#[tokio::test]
async fn command_finalizer_receives_the_engine_context_after_ordinary_quiescence() {
    with_watchdog(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let executable = env::current_exe().unwrap();
        let fixture_args = fixture_arguments();
        let source = format!(
            "schemaVersion: 1\nenvironmentPassthrough: [WORKFLOW_FIXTURE_SOCKET]\nsteps:\n  work:\n    kind: cmd\n    command:\n      argv: {}\nfinalizers:\n  release:\n    kind: cmd\n    inputs:\n      context: {{ ref: finalization.context }}\n    command:\n      argv: {}\n",
            command_argv(
                &fixture_script(0, "work", false),
                &executable,
                &fixture_args
            ),
            command_argv(
                &fixture_script(0, "release", false),
                &executable,
                &fixture_args
            ),
        );
        let fixture = execution_fixture(
            &source,
            ResolvedInputs::default(),
            fixture_environment(&listener),
            CancellationSource::new(),
            1,
            32,
        );
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn(async move {
            execute_workflow(
                fixture.admitted,
                &artifacts,
                &inputs,
                &StepDiagnosticLog::default(),
                AgentExecution::disabled(),
                TestClock,
                NoopExecutionObserver,
            )
            .await
        });

        let (role, work) = accept_fixture(&listener).await;
        assert_eq!(role, "work");
        release_fixture(work).await;
        let (role, release) = accept_fixture(&listener).await;
        assert_eq!(role, "release");
        release_fixture(release).await;

        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert!(matches!(
            result.steps["release"],
            StepState::Succeeded { .. }
        ));
        let summary = result.finalization_summary.unwrap();
        assert_eq!(
            summary.trigger,
            crate::workflow::document::FinalizationTrigger::Succeeded
        );
        assert!(matches!(
            summary.finalizers[0].disposition,
            StepState::Succeeded { .. }
        ));
    })
    .await;
}

#[tokio::test]
async fn command_receives_eof_instead_of_the_adapters_live_input() {
    with_watchdog(async {
        let mut engine = Command::new(env::current_exe().unwrap());
        engine
            .args(["--ignored", "--exact", STDIN_FIXTURE_TEST_NAME])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut engine = engine.spawn().unwrap();
        let _live_adapter_input = engine.stdin.take().unwrap();
        assert!(engine.wait().await.unwrap().success());
    })
    .await;
}

#[tokio::test]
async fn admitted_producer_consumer_executes_with_inputs_observations_and_export() {
    with_watchdog(async {
        let path = env::var_os("PATH").unwrap_or_else(|| OsString::from("/bin:/usr/bin"));
        let producer_script = r#"set -eu
if IFS= read -r unexpected; then exit 91; fi
{
  printf '%s|' "$(cat "$UM_STEP_INPUTS/values/prompt")"
  cat "$UM_STEP_INPUTS/collections/attachments/000000"
  printf '|'
  cat "$UM_STEP_INPUTS/collections/attachments/000001"
} > produced.txt
printf producer-standard-output
printf producer-standard-error >&2
"#;
        let consumer_script = r#"set -eu
if IFS= read -r unexpected; then exit 92; fi
cat "$UM_STEP_INPUTS/values/artifact" > exported.txt
printf consumer-standard-output
printf consumer-standard-error >&2
"#;
        let source = format!(
            "schemaVersion: 1\ninputs:\n  request: {{kind: text}}\n  evidence: {{kind: attachments}}\nsteps:\n  produce:\n    kind: cmd\n    inputs:\n      prompt:\n        ref: inputs.request\n      attachments:\n        ref: inputs.evidence\n    command:\n      argv: {}\n    outputs:\n      produced:\n        kind: file\n        from: path\n        path: produced.txt\n        mediaType: text/plain\n  consume:\n    kind: cmd\n    inputs:\n      artifact:\n        ref: outputs.produce.produced\n    command:\n      argv: {}\n    outputs:\n      delivered:\n        kind: file\n        from: path\n        path: exported.txt\n        mediaType: text/plain\nexports:\n  result:\n    ref: outputs.consume.delivered\n",
            serde_json::to_string(&["sh", "-c", producer_script]).unwrap(),
            serde_json::to_string(&["sh", "-c", consumer_script]).unwrap(),
        );
        let fixture = execution_fixture(
            &source,
            ResolvedInputs::new(BTreeMap::from([
                (
                    "request".to_owned(),
                    ResolvedInput::Text(Arc::from("typed request")),
                ),
                (
                    "evidence".to_owned(),
                    ResolvedInput::Attachments(Arc::from([
                        ResolvedAttachment::new(Arc::from("text/plain"), Arc::from(*b"first")),
                        ResolvedAttachment::new(Arc::from("text/plain"), Arc::from(*b"second")),
                    ])),
                ),
            ])),
            EnvironmentSnapshot::new([("PATH", path)]),
            CancellationSource::new(),
            2,
            3,
        );
        let expected_provenance = fixture.admitted.workflow().source.clone();
        let expected_digest = fixture.admitted.workflow().content_digest.clone();
        fs::remove_dir_all(&fixture.source_root).unwrap();
        let diagnostics = StepDiagnosticLog::default();
        let (observer, entries, _observed) = RecordingObserver::new();

        let result = execute_workflow(
            fixture.admitted,
            &fixture.artifacts,
            &fixture.inputs,
            &diagnostics,
            AgentExecution::disabled(),
            TestClock,
            observer,
        )
        .await
        .unwrap();

        assert_eq!(result.outcome, RunOutcome::Succeeded);
        assert!(matches!(result.steps["produce"], StepState::Succeeded { .. }));
        assert!(matches!(result.steps["consume"], StepState::Succeeded { .. }));
        assert_eq!(result.provenance, expected_provenance);
        assert_eq!(result.content_digest, expected_digest);
        assert!(fixture.execution_root.exists());
        assert_eq!(fixture.inputs.active_view_count(), 0);
        assert_eq!(fixture.inputs.reservation_usage(), (0, 0, 0));

        let ExportValue::Available { output } = &result.exports["result"] else {
            panic!("exported file was unavailable");
        };
        let file = output.as_file().unwrap();
        let mut exported = Vec::new();
        fixture
            .artifacts
            .copy_to(file.handle(), &mut exported)
            .unwrap();
        assert_eq!(exported, b"typed request|first|second");

        let entries = entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_stream(&entries, "produce", CommandOutputSource::StandardOutput, b"producer-standard-output");
        assert_stream(&entries, "produce", CommandOutputSource::StandardError, b"producer-standard-error");
        assert_stream(&entries, "consume", CommandOutputSource::StandardOutput, b"consumer-standard-output");
        assert_stream(&entries, "consume", CommandOutputSource::StandardError, b"consumer-standard-error");
        assert!(entries.iter().any(|entry| matches!(entry, ExecutionObservation::Transition(_))));
        assert_eq!(diagnostics.get("produce").unwrap().standard_output().bytes(), b"pro");

        fixture.artifacts.release().unwrap();
        let mut unavailable = Vec::new();
        assert!(matches!(
            fixture.artifacts.copy_to(file.handle(), &mut unavailable),
            Err(ArtifactReadFailure::Unavailable | ArtifactReadFailure::UnknownHandle)
        ));
    })
    .await;
}

#[tokio::test]
async fn failure_stops_new_work_but_retains_the_successful_sibling_output() {
    with_watchdog(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let executable = env::current_exe().unwrap();
        let fixture_args = fixture_arguments();
        let fail_script = fixture_script(23, "fail", false);
        let sibling_script = fixture_script(0, "sibling", true);
        let queued_script = fixture_script(0, "queued", false);
        let source = format!(
            "schemaVersion: 1\nenvironmentPassthrough: [WORKFLOW_FIXTURE_SOCKET]\nsteps:\n  aFail:\n    kind: cmd\n    command:\n      argv: {}\n  bSibling:\n    kind: cmd\n    command:\n      argv: {}\n    outputs:\n      retained:\n        kind: file\n        from: path\n        path: retained.txt\n        mediaType: text/plain\n  cFailChild:\n    kind: cmd\n    dependsOn: [aFail]\n    command:\n      argv: {}\n  zQueued:\n    kind: cmd\n    command:\n      argv: {}\n  zzQueuedChild:\n    kind: cmd\n    dependsOn: [zQueued]\n    command:\n      argv: {}\nexports:\n  retained:\n    ref: outputs.bSibling.retained\n",
            command_argv(&fail_script, &executable, &fixture_args),
            command_argv(&sibling_script, &executable, &fixture_args),
            command_argv(&queued_script, &executable, &fixture_args),
            command_argv(&queued_script, &executable, &fixture_args),
            command_argv(&queued_script, &executable, &fixture_args),
        );
        let fixture = execution_fixture(
            &source,
            ResolvedInputs::default(),
            fixture_environment(&listener),
            CancellationSource::new(),
            2,
            1024,
        );
        let diagnostics = StepDiagnosticLog::default();
        let (observer, _entries, mut observed) = RecordingObserver::new();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn(async move {
            execute_workflow(
                fixture.admitted,
                &artifacts,
                &inputs,
                &diagnostics,
                AgentExecution::disabled(),
                TestClock,
                observer,
            )
            .await
        });

        let first = accept_fixture(&listener).await;
        let second = accept_fixture(&listener).await;
        let mut commands = BTreeMap::from([(first.0.clone(), first.1), (second.0.clone(), second.1)]);
        assert_eq!(commands.keys().cloned().collect::<Vec<_>>(), ["fail", "sibling"]);
        release_fixture(commands.remove("fail").unwrap()).await;
        wait_for_step_transition(&mut observed, "aFail", StepStateKind::Failed).await;
        release_fixture(commands.remove("sibling").unwrap()).await;

        let result = execution.await.unwrap().unwrap();
        assert!(matches!(
            &result.steps["aFail"],
            StepState::Failed { detail }
                if detail.phase == FailurePhase::Execution
                    && detail.code == FailureCode::CommandExit
                    && detail.exit_code == Some(23)
        ));
        assert_eq!(
            result.steps["cFailChild"],
            StepState::Blocked {
                detail: BlockedDetail::new([Prerequisite::control("aFail").unwrap()]).unwrap(),
            }
        );
        assert_eq!(
            result.steps["zQueued"],
            StepState::NotRun {
                detail: NonExecutionDetail::for_role(
                    crate::workflow::validated::WorkflowNodeRole::Step,
                    NonExecutionCode::FailureStop,
                )
                .unwrap(),
            }
        );
        assert_eq!(
            result.steps["zzQueuedChild"],
            StepState::Blocked {
                detail: BlockedDetail::new([Prerequisite::control("zQueued").unwrap()]).unwrap(),
            }
        );
        let ExportValue::Available { output } = &result.exports["retained"] else {
            panic!("successful sibling output was not retained");
        };
        let mut retained = Vec::new();
        fixture
            .artifacts
            .copy_to(output.as_file().unwrap().handle(), &mut retained)
            .unwrap();
        assert_eq!(retained, b"retained sibling");
    })
    .await;
}

#[tokio::test]
async fn controlled_cancellation_orders_events_and_waits_for_terminal_delivery() {
    with_watchdog(async {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let cancellation = CancellationSource::new();
        let source = format!(
            "schemaVersion: 1\nenvironmentPassthrough: [WORKFLOW_FIXTURE_SOCKET, WORKFLOW_FIXTURE_EXIT_CODE, WORKFLOW_FIXTURE_MODE, WORKFLOW_FIXTURE_OUTPUT_BYTES, WORKFLOW_FIXTURE_ROLE]\nsteps:\n  active:\n    kind: cmd\n    command:\n      argv: {}\n  pending:\n    kind: cmd\n    dependsOn: [active]\n    command:\n      argv: {}\n",
            serde_json::to_string(&std::iter::once(env::current_exe().unwrap().to_string_lossy().into_owned()).chain(fixture_arguments()).collect::<Vec<_>>()).unwrap(),
            serde_json::to_string(&["true"]).unwrap(),
        );
        let mut environment = fixture_environment(&listener).variables().clone();
        environment.insert(OsString::from("WORKFLOW_FIXTURE_MODE"), OsString::from("interruptible-group"));
        environment.insert(OsString::from("WORKFLOW_FIXTURE_OUTPUT_BYTES"), OsString::from("19"));
        environment.insert(OsString::from("WORKFLOW_FIXTURE_ROLE"), OsString::from("active"));
        let fixture = execution_fixture(
            &source,
            ResolvedInputs::default(),
            EnvironmentSnapshot::new(environment),
            cancellation.clone(),
            1,
            4,
        );
        let diagnostics = StepDiagnosticLog::default();
        let (observer, entries, _observed, mut terminal_reached, release_terminal) =
            RecordingObserver::with_terminal_gate();
        let artifacts = fixture.artifacts.clone();
        let inputs = fixture.inputs.clone();
        let execution = tokio::spawn(async move {
            execute_workflow(
                fixture.admitted,
                &artifacts,
                &inputs,
                &diagnostics,
                AgentExecution::disabled(),
                TestClock,
                observer,
            )
            .await
        });

        let (_role, command) = accept_fixture(&listener).await;
        assert_eq!(read_fixture_event(&command).await["event"], "output-written");
        assert!(cancellation.request_cancellation(CancellationReason::TerminationRequest));
        assert!(!cancellation.request_cancellation(CancellationReason::RunnerShutdown));
        assert_eq!(read_fixture_event(&command).await["event"], "interrupted");

        terminal_reached.recv().await.unwrap();
        assert!(!execution.is_finished());
        release_terminal.send(true).unwrap();
        let result = execution.await.unwrap().unwrap();
        assert_eq!(
            result.outcome,
            RunOutcome::Cancelled {
                reason: CancellationReason::TerminationRequest
            }
        );
        assert_eq!(
            result.steps["active"],
            StepState::Cancelled {
                detail: CancellationDetail::new(CancellationReason::TerminationRequest),
            }
        );
        assert_eq!(
            result.steps["pending"],
            StepState::Cancelled {
                detail: CancellationDetail::new(CancellationReason::TerminationRequest),
            }
        );

        let entries = entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let transitions = entries
            .iter()
            .filter_map(|entry| match entry {
                ExecutionObservation::Transition(transition) => Some(&transition.event),
                ExecutionObservation::CommandOutput(_)
                | ExecutionObservation::CommandOutputClosed(_)
                | ExecutionObservation::Agent(_) => None,
            })
            .collect::<Vec<_>>();
        let accepted = transitions.iter().position(|transition| matches!(
            transition,
            TransitionEvent::CancellationAccepted {
                reason: CancellationReason::TerminationRequest,
                deadline,
                ..
            } if *deadline == TestInstant(Duration::from_secs(1))
        )).unwrap();
        let derived = transitions.iter().position(|transition| matches!(
            transition,
            TransitionEvent::Step {
                step,
                to: StepStateKind::Cancelling,
                ..
            } if step == "active"
        )).unwrap();
        assert!(accepted < derived);
        assert_stream_contains(
            &entries,
            "active",
            CommandOutputSource::StandardOutput,
            &[b'o'; 19],
        );
        assert_stream_contains(
            &entries,
            "active",
            CommandOutputSource::StandardError,
            &[b'e'; 19],
        );
    })
    .await;
}

#[tokio::test]
async fn initial_cancellation_is_observed_without_starting_a_step() {
    with_watchdog(async {
        let cancellation = CancellationSource::new();
        assert!(cancellation.request_cancellation(CancellationReason::CallerOutputFailure));
        let fixture = execution_fixture(
            "schemaVersion: 1\nsteps:\n  never:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
            ResolvedInputs::default(),
            EnvironmentSnapshot::default(),
            cancellation,
            1,
            32,
        );
        let diagnostics = StepDiagnosticLog::default();
        let (observer, entries, _observed) = RecordingObserver::new();
        let result = execute_workflow(
            fixture.admitted,
            &fixture.artifacts,
            &fixture.inputs,
            &diagnostics,
            AgentExecution::disabled(),
            TestClock,
            observer,
        )
        .await
        .unwrap();
        assert_eq!(
            result.outcome,
            RunOutcome::Cancelled {
                reason: CancellationReason::CallerOutputFailure
            }
        );
        let transitions = entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter_map(|entry| match entry {
                ExecutionObservation::Transition(transition) => Some(transition.event.clone()),
                ExecutionObservation::CommandOutput(_)
                | ExecutionObservation::CommandOutputClosed(_)
                | ExecutionObservation::Agent(_) => None,
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            transitions.as_slice(),
            [
                TransitionEvent::CancellationAccepted {
                    reason: CancellationReason::CallerOutputFailure,
                    deadline,
                    ..
                },
                TransitionEvent::Step {
                    step,
                    from: StepStateKind::Pending,
                    to: StepStateKind::Cancelled,
                    ..
                },
                TransitionEvent::Workflow { to, .. }
            ] if *deadline == TestInstant(Duration::from_secs(1))
                && step == "never"
                && matches!(
                    to.as_ref(),
                    WorkflowState::Cancelled {
                        reason: CancellationReason::CallerOutputFailure
                    }
                )
        ));
    })
    .await;
}
