use super::*;
use crate::workflow::admission::{HarnessLoginObserver, HarnessLoginOutcome};

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

// REQ-WFEXEC-9
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

fn withheld_login_source(kind: &str, root: &str, step: &str) -> String {
    let config = match kind {
        "codex" => "        model: gpt-5.4\n        effort: xhigh\n",
        _ => "        model: claude-opus-4-1\n        effort: xhigh\n",
    };
    format!(
        "schemaVersion: 1\n{root}agentProfiles:\n  coding:\n    harness:\n      kind: {kind}\n      config:\n{config}steps:\n  task:\n    kind: agent\n{step}    agent:\n      profile: coding\n      systemPrompt: prompt.md\n      message:\n        text:\n          - file: prompt.md\n"
    )
}

// INV-ENGINE-35, REQ-DIAG-41
#[tokio::test]
async fn an_agent_step_whose_login_was_withheld_fails_before_its_harness_starts() {
    with_watchdog(async {
        for (kind, variable, step) in [
            ("codex", "CODEX_HOME", ""),
            (
                "codex",
                "CODEX_HOME",
                "    environmentPassthrough: [CODEX_HOME]\n",
            ),
            ("claude_code", "CLAUDE_CODE_OAUTH_TOKEN", ""),
        ] {
            let source = withheld_login_source(
                kind,
                &format!("environmentPassthrough: [{variable}]\n"),
                step,
            );
            let run = run_with_withheld_logins(&source, &[variable]).await;
            assert!(!run.harness_started, "{kind} {step:?}");
            let StepState::Failed { detail } = &run.result.steps["task"] else {
                panic!("{kind}: {:?}", run.result.steps["task"]);
            };
            assert_eq!(detail.phase, FailurePhase::Start, "{kind}");
            assert_eq!(detail.code, FailureCode::HarnessStartFailed, "{kind}");
            assert_eq!(
                detail.runner_diagnostic(),
                Some(json!({"launchFailure": "agent_login_unavailable"})),
                "{kind}"
            );
        }
    })
    .await;
}

// INV-ENGINE-35, REQ-DIAG-41
#[tokio::test]
async fn a_withheld_login_the_step_would_not_receive_leaves_the_step_to_start() {
    with_watchdog(async {
        for (kind, root, step, withheld) in [
            (
                "codex",
                "environmentPassthrough: [CODEX_HOME]\n",
                "",
                &[][..],
            ),
            (
                "codex",
                "environmentPassthrough: [CODEX_HOME]\n",
                "    environmentPassthrough: []\n",
                &["CODEX_HOME"][..],
            ),
            ("codex", "", "", &["CODEX_HOME"][..]),
            (
                "codex",
                "environmentPassthrough: [CODEX_HOME, CLAUDE_CODE_OAUTH_TOKEN]\n",
                "",
                &["CLAUDE_CODE_OAUTH_TOKEN"][..],
            ),
            (
                "claude_code",
                "environmentPassthrough: [CLAUDE_CODE_OAUTH_TOKEN]\n",
                "    environmentPassthrough: []\n",
                &["CLAUDE_CODE_OAUTH_TOKEN"][..],
            ),
        ] {
            let source = withheld_login_source(kind, root, step);
            let run = run_with_withheld_logins(&source, withheld).await;
            assert!(run.harness_started, "{kind} {root:?} {step:?} {withheld:?}");
            assert_eq!(run.result.outcome, RunOutcome::Succeeded, "{kind} {root:?}");
        }

        let command = run_with_withheld_logins(
            "schemaVersion: 1\nenvironmentPassthrough: [CODEX_HOME]\nsteps:\n  task:\n    kind: cmd\n    command:\n      argv: [/bin/sh, -c, 'true']\n",
            &["CODEX_HOME"],
        )
        .await;
        assert!(!command.harness_started);
        assert_eq!(command.result.outcome, RunOutcome::Succeeded);
    })
    .await;
}

// INV-ENGINE-35, REQ-DIAG-41
#[tokio::test]
async fn an_agent_finalizer_whose_login_was_withheld_is_refused_as_a_step_is() {
    with_watchdog(async {
        let source = "schemaVersion: 1\nenvironmentPassthrough: [CODEX_HOME]\nagentProfiles:\n  coding:\n    harness:\n      kind: codex\n      config:\n        model: gpt-5.4\n        effort: xhigh\nsteps:\n  work:\n    kind: cmd\n    command:\n      argv: [/bin/sh, -c, 'true']\nfinalizers:\n  release:\n    kind: agent\n    agent:\n      profile: coding\n      systemPrompt: prompt.md\n      message:\n        text:\n          - file: prompt.md\n";
        let run = run_with_withheld_logins(source, &["CODEX_HOME"]).await;
        assert!(!run.harness_started);
        let StepState::Failed { detail } = &run.result.steps["release"] else {
            panic!("{:?}", run.result.steps["release"]);
        };
        assert_eq!(detail.phase, FailurePhase::Start);
        assert_eq!(detail.code, FailureCode::HarnessStartFailed);
        assert_eq!(
            detail.runner_diagnostic(),
            Some(json!({"launchFailure": "agent_login_unavailable"}))
        );
    })
    .await;
}

/// Records each harness login the step runtime reports (INV-ENGINE-36).
#[derive(Default)]
struct RecordingLoginObserver {
    observed: Mutex<Vec<(String, HarnessLoginOutcome)>>,
}

impl HarnessLoginObserver for RecordingLoginObserver {
    fn observe(&self, variable: &str, outcome: HarnessLoginOutcome) {
        self.observed
            .lock()
            .unwrap()
            .push((variable.to_owned(), outcome));
    }
}

/// How the scripted harness ends the workflow's one agent invocation.
enum ScriptedEnd {
    Complete,
    Fail(AgentFailureCause),
    Cancel,
}

fn harness_error(class: &str) -> AgentFailureCause {
    use crate::workflow::agent::{
        AgentHarnessFailureDetail, CanonicalHarnessFailure, HarnessFailureDiagnostic,
    };
    AgentFailureCause::HarnessFailed {
        detail: AgentHarnessFailureDetail::Classified {
            canonical: CanonicalHarnessFailure::ModelError,
            diagnostic: HarnessFailureDiagnostic::from_code(class, Some(401)),
        },
    }
}

/// Runs `source` from `environment`, with a recording harness-login observer in the
/// execution context when `observed`, ends its one started agent invocation as `end` says,
/// and returns the run's outcome with what the observer was told.
async fn observed_harness_logins(
    source: &str,
    environment: &[(&str, &str)],
    observed: bool,
    end: ScriptedEnd,
) -> (RunOutcome, Vec<(String, HarnessLoginOutcome)>) {
    let observer = Arc::new(RecordingLoginObserver::default());
    let cancellation = CancellationSource::new();
    let fixture = execution_fixture_with_context(
        source,
        &[("prompt.md", b"start")],
        ResolvedInputs::default(),
        |execution_root| {
            let context = fixture_execution_context(
                execution_root,
                EnvironmentSnapshot::new(environment.iter().copied()),
                cancellation.clone(),
                1,
                1024,
            );
            if observed {
                context.with_harness_login_observer(observer.clone())
            } else {
                context
            }
        },
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
    match end {
        ScriptedEnd::Complete => started.control().complete().await.unwrap(),
        ScriptedEnd::Fail(cause) => started.control().fail(cause).await.unwrap(),
        ScriptedEnd::Cancel => {
            assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        }
    }
    let outcome = execution.await.unwrap().unwrap().outcome;
    let observed = observer.observed.lock().unwrap().clone();
    (outcome, observed)
}

const LOGIN_ENVIRONMENT: &[(&str, &str)] = &[
    ("PATH", "/bin:/usr/bin"),
    ("CODEX_HOME", "/logins/codex"),
    ("CLAUDE_CODE_OAUTH_TOKEN", "claude-login"),
];

// INV-ENGINE-36
#[tokio::test]
async fn a_codex_step_that_received_its_login_reports_only_acceptance_or_refusal() {
    with_watchdog(async {
        let source = withheld_login_source("codex", "environmentPassthrough: [CODEX_HOME]\n", "");
        let accepted =
            observed_harness_logins(&source, LOGIN_ENVIRONMENT, true, ScriptedEnd::Complete).await;
        assert_eq!(accepted.0, RunOutcome::Succeeded);
        assert_eq!(
            accepted.1,
            [("CODEX_HOME".to_owned(), HarnessLoginOutcome::Accepted)]
        );

        let refused = observed_harness_logins(
            &source,
            LOGIN_ENVIRONMENT,
            true,
            ScriptedEnd::Fail(harness_error("unauthorized")),
        )
        .await;
        assert!(matches!(refused.0, RunOutcome::Failed { .. }));
        assert_eq!(
            refused.1,
            [("CODEX_HOME".to_owned(), HarnessLoginOutcome::Refused)]
        );

        for (label, end) in [
            (
                "rate_limited",
                ScriptedEnd::Fail(harness_error("rate_limited")),
            ),
            (
                "harness_protocol_failed",
                ScriptedEnd::Fail(AgentFailureCause::HarnessProtocolFailed),
            ),
            ("cancelled", ScriptedEnd::Cancel),
        ] {
            let (outcome, observed) =
                observed_harness_logins(&source, LOGIN_ENVIRONMENT, true, end).await;
            assert_ne!(outcome, RunOutcome::Succeeded, "{label}");
            assert_eq!(observed, [], "{label}");
        }
    })
    .await;
}

// INV-ENGINE-36
#[tokio::test]
async fn a_step_whose_environment_omits_its_login_reports_nothing() {
    with_watchdog(async {
        for (root, step) in [
            ("", ""),
            (
                "environmentPassthrough: [CODEX_HOME]\n",
                "    environmentPassthrough: []\n",
            ),
        ] {
            let source = withheld_login_source("codex", root, step);
            for end in [
                ScriptedEnd::Complete,
                ScriptedEnd::Fail(harness_error("unauthorized")),
            ] {
                let (_, observed) =
                    observed_harness_logins(&source, LOGIN_ENVIRONMENT, true, end).await;
                assert_eq!(observed, [], "{root:?} {step:?}");
            }
        }
    })
    .await;
}

// INV-ENGINE-36
#[tokio::test]
async fn a_claude_code_step_reports_its_own_login_variable_only() {
    with_watchdog(async {
        let source = withheld_login_source(
            "claude_code",
            "environmentPassthrough: [CODEX_HOME, CLAUDE_CODE_OAUTH_TOKEN]\n",
            "",
        );
        let (_, accepted) =
            observed_harness_logins(&source, LOGIN_ENVIRONMENT, true, ScriptedEnd::Complete).await;
        assert_eq!(
            accepted,
            [(
                "CLAUDE_CODE_OAUTH_TOKEN".to_owned(),
                HarnessLoginOutcome::Accepted
            )]
        );
        let (_, refused) = observed_harness_logins(
            &source,
            LOGIN_ENVIRONMENT,
            true,
            ScriptedEnd::Fail(harness_error("authentication_error")),
        )
        .await;
        assert_eq!(
            refused,
            [(
                "CLAUDE_CODE_OAUTH_TOKEN".to_owned(),
                HarnessLoginOutcome::Refused
            )]
        );
    })
    .await;
}

// INV-ENGINE-36
#[tokio::test]
async fn a_context_without_a_login_observer_runs_its_agent_step_as_before() {
    with_watchdog(async {
        let source = withheld_login_source("codex", "environmentPassthrough: [CODEX_HOME]\n", "");
        let (outcome, observed) =
            observed_harness_logins(&source, LOGIN_ENVIRONMENT, false, ScriptedEnd::Complete).await;
        assert_eq!(outcome, RunOutcome::Succeeded);
        assert_eq!(observed, []);
    })
    .await;
}
